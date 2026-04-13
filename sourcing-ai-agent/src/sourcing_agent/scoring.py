from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

from .confidence_policy import DEFAULT_HIGH_THRESHOLD, DEFAULT_MEDIUM_THRESHOLD
from .domain import (
    Candidate,
    FACET_ALIAS_MAP,
    ROLE_BUCKET_ALIAS_MAP,
    JobRequest,
    candidate_profile_signal_text,
    candidate_searchable_text,
    derive_candidate_facets,
    derive_candidate_filter_facets,
    derive_candidate_role_bucket,
    normalize_requested_facet,
    normalize_requested_role_bucket,
    sanitize_candidate_notes,
)
from .query_signal_knowledge import lookup_scope_signal
from .request_normalization import resolve_request_intent_view


SEARCH_FIELDS = {
    "role": 5,
    "team": 4,
    "focus_areas": 4,
    "derived_facets": 4,
    "acquisition_signals": 4,
    "investment_involvement": 4,
    "education": 2,
    "work_history": 2,
    "notes": 3,
    "ethnicity_background": 1,
    "current_destination": 1,
}

ORGANIZATION_SEARCH_FIELDS = {
    "organization_scope": 4,
    "acquisition_scope": 4,
}

SUPPORTING_PROVENANCE_FIELDS = {
    "acquisition_signals",
    "acquisition_scope",
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

REQUEST_MEMBERSHIP_CATEGORIES = {
    "employee",
    "former_employee",
    "investor",
    "lead",
    "non_member",
}

KEYWORD_ALIAS_MAP = {
    "基础设施": ["infrastructure", "infra", "platform", "distributed systems", "systems"],
    "infra": ["infrastructure", "platform", "distributed systems"],
    "训练": ["training", "trainer", "pretraining", "pre-training"],
    "训练系统": ["training systems", "training infrastructure", "training platform"],
    "预训练": ["pretraining", "pre-training"],
    "pre-train": ["pretrain", "pre-training", "pretraining", "pre-trained", "pretrained", "model pretraining"],
    "pretrain": ["pre-train", "pre-training", "pretraining", "pre-trained", "pretrained", "model pretraining"],
    "pre-training": ["pre-train", "pretrain", "pretraining", "pre-trained", "pretrained", "model pretraining"],
    "pretraining": ["pre-train", "pretrain", "pre-training", "pre-trained", "pretrained", "model pretraining"],
    "post-train": ["posttrain", "post-training", "posttraining", "post-trained", "posttrained", "model posttraining"],
    "posttrain": ["post-train", "post-training", "posttraining", "post-trained", "posttrained", "model posttraining"],
    "post-training": ["post-train", "posttrain", "posttraining", "post-trained", "posttrained", "model posttraining"],
    "posttraining": ["post-train", "posttrain", "post-training", "post-trained", "posttrained", "model posttraining"],
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
    "多模态": ["multimodal", "vision-language", "vision language", "video generation", "image generation"],
    "veo": ["video generation", "text-to-video", "multimodal", "vision-language", "vision language"],
    "nano banana": ["multimodal", "vision-language", "image generation", "video generation"],
    "greater china experience": [
        "greater_china_region_experience",
        "greater china",
        "mainland china",
        "china",
        "中国大陆",
        "中国",
        "hong kong",
        "hong kong sar",
        "香港",
        "taiwan",
        "台灣",
        "台湾",
        "taipei",
        "hsinchu",
        "新竹",
        "singapore",
        "新加坡",
    ],
    "chinese bilingual outreach": [
        "mainland_china_experience_or_chinese_language",
        "greater_china_region_experience",
        "中文",
        "chinese",
        "mandarin",
        "cantonese",
        "bilingual",
        "双语",
        "simplified chinese",
        "traditional chinese",
    ],
    "greater_china_region_experience": [
        "greater china",
        "greater china experience",
        "mainland china",
        "hong kong",
        "macau",
        "taiwan",
        "singapore",
    ],
    "mainland_china_experience_or_chinese_language": [
        "mainland china",
        "mandarin",
        "cantonese",
        "chinese language",
        "putonghua",
        "guoyu",
        "中文",
    ],
}

OUTREACH_ONLY_RETRIEVAL_TERMS = {
    "greater china experience",
    "chinese bilingual outreach",
    "greater_china_region_experience",
    "mainland_china_experience_or_chinese_language",
    "mainland or chinese language",
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
    intent_view = resolve_request_intent_view(request)
    scored: list[ScoredCandidate] = []
    for candidate in candidates:
        item = score_candidate(
            candidate,
            request,
            criteria_patterns=criteria_patterns,
            confidence_policy=confidence_policy,
            semantic_hit=(semantic_hits or {}).get(candidate.candidate_id),
            intent_view=intent_view,
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
    *,
    intent_view: dict[str, Any] | None = None,
) -> ScoredCandidate | None:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    if not candidate_matches_structured_filters(candidate, request, intent_view=intent_view):
        return None

    membership_categories, requested_role_categories = _requested_category_filters(request, intent_view=intent_view)
    keywords = build_query_terms(request, criteria_patterns or [], intent_view=intent_view)
    organization_keywords = build_organization_terms(request, intent_view=intent_view)
    effective_employment_statuses = list(intent_view.get("employment_statuses") or [])

    score = 0.0
    matched_fields: list[dict[str, Any]] = []
    keyword_score, keyword_matches = _score_keyword_pool(
        candidate,
        keywords,
        _search_fields_for_request(request, intent_view=intent_view),
        criteria_patterns or [],
    )
    organization_score, organization_matches = _score_keyword_pool(
        candidate,
        organization_keywords,
        ORGANIZATION_SEARCH_FIELDS,
        criteria_patterns or [],
    )
    score += keyword_score + organization_score
    matched_fields.extend(keyword_matches)
    matched_fields.extend(organization_matches)

    if matched_fields:
        distinct_keywords = {str(item.get("keyword") or "").strip().lower() for item in matched_fields if str(item.get("keyword") or "").strip()}
        high_signal_field_hits = sum(
            1
            for item in matched_fields
            if str(item.get("field") or "") in {"organization", "role", "team", "focus_areas", "derived_facets"}
        )
        profile_signal_hits = sum(
            1
            for item in matched_fields
            if str(item.get("field") or "") in {"education", "work_history", "notes"}
        )
        score += min(len(distinct_keywords), 8) * 0.75
        score += min(high_signal_field_hits, 8) * 0.35
        score += min(profile_signal_hits, 6) * 0.25

    if membership_categories and candidate.category in membership_categories:
        score += 1.5
    if requested_role_categories and _candidate_matches_requested_role_categories(candidate, requested_role_categories):
        score += 1.5
    if effective_employment_statuses and candidate.employment_status in effective_employment_statuses:
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

    if (keywords or organization_keywords) and not matched_fields:
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
            candidate_searchable_text(candidate),
            " ".join(derive_candidate_facets(candidate)),
            derive_candidate_role_bucket(candidate),
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


def build_query_terms(
    request: JobRequest,
    criteria_patterns: list[dict[str, Any]] | None = None,
    *,
    intent_view: dict[str, Any] | None = None,
) -> list[str]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    natural_language_query = request.query or request.raw_user_request
    keywords = list(intent_view.get("keywords") or request.keywords or [])
    if not keywords:
        keywords = _extract_keywords_from_query(natural_language_query)
    expanded: list[str] = []
    for keyword in (
        keywords
        + list(intent_view.get("must_have_keywords") or request.must_have_keywords)
        + list(intent_view.get("must_have_facets") or request.must_have_facets)
        + list(intent_view.get("must_have_primary_role_buckets") or request.must_have_primary_role_buckets)
    ):
        if _is_outreach_only_retrieval_term(keyword):
            continue
        if _is_parent_scope_keyword(keyword, request, intent_view=intent_view):
            continue
        expanded.extend(_keyword_variants(keyword, criteria_patterns or []))
    return _dedupe(expanded)


def build_organization_terms(
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> list[str]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    return _dedupe(
        [
            str(item).strip()
            for item in list(intent_view.get("organization_keywords") or request.organization_keywords or [])
            if str(item).strip() and not _is_outreach_only_retrieval_term(str(item))
        ]
    )


def _is_parent_scope_keyword(
    keyword: str,
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> bool:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    normalized_keyword = _normalize(keyword)
    normalized_target = _normalize(str(intent_view.get("target_company") or request.target_company or ""))
    if not normalized_keyword or not normalized_target or normalized_target not in normalized_keyword:
        return False
    organization_terms = {
        _normalize(item)
        for item in list(intent_view.get("organization_keywords") or request.organization_keywords or [])
        if _normalize(str(item))
    }
    return normalized_keyword in organization_terms


def _search_fields_for_request(
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> dict[str, float]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    if (
        intent_view.get("must_have_primary_role_buckets")
        and str(intent_view.get("primary_role_bucket_mode") or "hard").strip().lower() == "hard"
    ):
        return {field_name: weight for field_name, weight in SEARCH_FIELDS.items() if field_name != "notes"}
    return SEARCH_FIELDS


def candidate_matches_structured_filters(
    candidate: Candidate,
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> bool:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    effective_target_company = str(intent_view.get("target_company") or request.target_company or "").strip()
    effective_employment_statuses = list(intent_view.get("employment_statuses") or request.employment_statuses or [])
    effective_must_have_facets = list(intent_view.get("must_have_facets") or request.must_have_facets or [])
    effective_role_buckets = list(
        intent_view.get("must_have_primary_role_buckets") or request.must_have_primary_role_buckets or []
    )
    primary_role_bucket_mode = str(intent_view.get("primary_role_bucket_mode") or "hard").strip().lower() or "hard"
    effective_organization_keywords = list(intent_view.get("organization_keywords") or request.organization_keywords or [])
    effective_must_have_keywords = list(intent_view.get("must_have_keywords") or request.must_have_keywords or [])
    if effective_target_company and _normalize(candidate.target_company) != _normalize(effective_target_company):
        return False
    membership_categories, requested_role_categories = _requested_category_filters(request, intent_view=intent_view)
    if membership_categories and candidate.category not in membership_categories:
        return False
    if effective_employment_statuses and candidate.employment_status not in effective_employment_statuses:
        return False

    searchable_blob = _normalize(_candidate_blob(candidate))
    candidate_facets = {_normalize(item) for item in derive_candidate_filter_facets(candidate)}
    if requested_role_categories and not _candidate_matches_requested_role_categories(candidate, requested_role_categories):
        return False
    if candidate.category == "investor" and _is_direct_investment_search(request, intent_view=intent_view):
        involvement_label = _investment_label(candidate.investment_involvement)
        if involvement_label == "no":
            return False

    if effective_must_have_facets and not all(_normalize(facet) in candidate_facets for facet in effective_must_have_facets):
        return False
    if effective_role_buckets and primary_role_bucket_mode == "hard":
        candidate_role_bucket = _normalize(derive_candidate_role_bucket(candidate))
        requested_primary_buckets = {_normalize(item) for item in effective_role_buckets}
        direct_role_blob = _normalize(" ".join([candidate.role, candidate.team]))
        # Role-bucket inference is heuristic and enrichment can surface extra
        # focus-area facets that should not override an explicit role title.
        # Keep the hard primary-bucket filter, but allow a mismatch when the
        # requested bucket is still directly supported by role/title text.
        if candidate_role_bucket not in requested_primary_buckets and not any(
            _role_text_supports_bucket(direct_role_blob, bucket) for bucket in requested_primary_buckets
        ):
            return False
    if effective_organization_keywords:
        org_blob = _normalize(_candidate_organization_scope_text(candidate))
        if not any(_normalize(token) in org_blob for token in effective_organization_keywords):
            return False
    must_have_keywords = [
        token
        for token in effective_must_have_keywords
        if not _is_outreach_only_retrieval_term(token)
    ]
    must_have_keyword_groups = [
        [variant for variant in _keyword_variants(token, []) if _normalize(variant)]
        for token in must_have_keywords
    ]
    if must_have_keyword_groups and not all(
        any(_normalize(variant) in searchable_blob for variant in variants)
        for variants in must_have_keyword_groups
    ):
        return False
    if request.exclude_keywords and any(_normalize(token) in searchable_blob for token in request.exclude_keywords):
        return False
    return True


def _requested_category_filters(
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> tuple[list[str], list[str]]:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    membership_categories: list[str] = []
    requested_role_categories: list[str] = []
    seen_membership: set[str] = set()
    seen_role_like: set[str] = set()
    for raw_category in list(intent_view.get("categories") or request.categories or []):
        normalized_category = _normalize(raw_category)
        if not normalized_category:
            continue
        if normalized_category in REQUEST_MEMBERSHIP_CATEGORIES:
            if normalized_category not in seen_membership:
                seen_membership.add(normalized_category)
                membership_categories.append(normalized_category)
            continue
        for role_like in (
            normalize_requested_role_bucket(raw_category),
            normalize_requested_facet(raw_category),
        ):
            normalized_role_like = _normalize(role_like)
            if not normalized_role_like or normalized_role_like in seen_role_like:
                continue
            seen_role_like.add(normalized_role_like)
            requested_role_categories.append(normalized_role_like)
    return membership_categories, requested_role_categories


def _role_text_supports_bucket(role_text: str, bucket: str) -> bool:
    normalized_role_text = _normalize(role_text)
    normalized_bucket = _normalize(bucket)
    if not normalized_role_text or not normalized_bucket:
        return False
    aliases = set(FACET_ALIAS_MAP.get(normalized_bucket, set())) | set(ROLE_BUCKET_ALIAS_MAP.get(normalized_bucket, set()))
    aliases.add(normalized_bucket)
    return any(_normalize(alias) in normalized_role_text for alias in aliases if _normalize(alias))


def _candidate_matches_requested_role_categories(candidate: Candidate, requested_role_categories: list[str]) -> bool:
    if not requested_role_categories:
        return True
    candidate_role_bucket = _normalize(derive_candidate_role_bucket(candidate))
    candidate_facets = {_normalize(item) for item in derive_candidate_filter_facets(candidate)}
    return any(role_like in candidate_facets or role_like == candidate_role_bucket for role_like in requested_role_categories)


def _is_direct_investment_search(
    request: JobRequest,
    *,
    intent_view: dict[str, Any] | None = None,
) -> bool:
    intent_view = dict(intent_view or resolve_request_intent_view(request))
    signal_text = " ".join(list(intent_view.get("keywords") or request.keywords or []) + [request.query])
    direct_terms = ["直接参与", "决策", "领投", "deal lead", "主导", "投资决策"]
    return any(term in signal_text for term in direct_terms)


def _is_outreach_only_retrieval_term(value: str) -> bool:
    normalized = _normalize(value)
    return normalized in OUTREACH_ONLY_RETRIEVAL_TERMS


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
    if field_name == "notes":
        return candidate_profile_signal_text(candidate, include_notes=True)
    if field_name == "acquisition_signals":
        return _candidate_acquisition_signal_text(candidate)
    if field_name == "organization_scope":
        return _candidate_organization_scope_text(candidate)
    if field_name == "acquisition_scope":
        return _candidate_acquisition_scope_text(candidate)
    return str(getattr(candidate, field_name) or "").strip()


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


def _score_keyword_pool(
    candidate: Candidate,
    keywords: list[str],
    field_weights: dict[str, float],
    criteria_patterns: list[dict[str, Any]],
) -> tuple[float, list[dict[str, Any]]]:
    score = 0.0
    matched_fields: list[dict[str, Any]] = []
    for keyword in keywords:
        variants = _keyword_variants(keyword, criteria_patterns or [])
        if not any(_normalize(item) for item in variants):
            continue
        primary_matches: list[dict[str, Any]] = []
        supporting_matches: list[dict[str, Any]] = []
        for field_name, weight in field_weights.items():
            value = _candidate_field_value(candidate, field_name)
            normalized_value = _normalize(value)
            if not normalized_value:
                continue
            matched_variant = next((item for item in variants if _normalize(item) in normalized_value), "")
            if not matched_variant:
                continue
            exact_match = _normalize(matched_variant) == _normalize(keyword)
            effective_weight = weight if exact_match else round(weight * 0.65, 2)
            match_record = {
                "keyword": keyword,
                "matched_on": matched_variant,
                "field": field_name,
                "value": _shorten(value),
                "weight": effective_weight,
                "match_type": "exact" if exact_match else "alias",
            }
            if field_name in SUPPORTING_PROVENANCE_FIELDS:
                supporting_matches.append(match_record)
            else:
                primary_matches.append(match_record)
        if primary_matches:
            matched_fields.extend(primary_matches)
            score += sum(float(item.get("weight") or 0.0) for item in primary_matches)
            for supporting in supporting_matches:
                supporting_weight = _supporting_match_weight(
                    keyword=keyword,
                    match_type=str(supporting.get("match_type") or ""),
                    matched_on=str(supporting.get("matched_on") or ""),
                    base_weight=float(supporting.get("weight") or 0.0),
                    has_primary_match=True,
                )
                if supporting_weight <= 0:
                    continue
                supporting["weight"] = supporting_weight
                matched_fields.append(supporting)
                score += supporting_weight
            score += 0.5
            continue
        for supporting in supporting_matches:
            supporting_weight = _supporting_match_weight(
                keyword=keyword,
                match_type=str(supporting.get("match_type") or ""),
                matched_on=str(supporting.get("matched_on") or ""),
                base_weight=float(supporting.get("weight") or 0.0),
                has_primary_match=False,
            )
            if supporting_weight <= 0:
                continue
            supporting["weight"] = supporting_weight
            matched_fields.append(supporting)
            score += supporting_weight
    return score, matched_fields


def _keyword_variants(keyword: str, criteria_patterns: list[dict[str, Any]]) -> list[str]:
    variants = [keyword]
    scope_signal = lookup_scope_signal(keyword)
    if scope_signal:
        variants.extend(
            str(item).strip()
            for item in (
                [scope_signal.get("canonical_label")]
                + list(scope_signal.get("aliases") or [])
                + list(scope_signal.get("keyword_labels") or [])
                + list(scope_signal.get("search_query_aliases") or [])
            )
            if str(item or "").strip()
        )
    else:
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


def _supporting_match_weight(
    *,
    keyword: str,
    match_type: str,
    matched_on: str,
    base_weight: float,
    has_primary_match: bool,
) -> float:
    if base_weight <= 0:
        return 0.0
    exact_match = str(match_type or "") == "exact" or _normalize(matched_on) == _normalize(keyword)
    if has_primary_match:
        multiplier = 0.2 if exact_match else 0.1
        return round(base_weight * multiplier, 2)
    if lookup_scope_signal(keyword):
        return 0.0
    multiplier = 0.12 if exact_match else 0.05
    return round(base_weight * multiplier, 2)


def _candidate_acquisition_signal_text(candidate: Candidate) -> str:
    metadata = dict(candidate.metadata or {})
    parts = [
        str(metadata.get("seed_query") or "").strip(),
        " | ".join(str(item).strip() for item in list(metadata.get("source_queries") or []) if str(item).strip()),
        " | ".join(str(item).strip() for item in list(metadata.get("keywords") or []) if str(item).strip()),
        sanitize_candidate_notes(candidate.notes),
    ]
    return " | ".join(part for part in parts if part)


def _candidate_organization_scope_text(candidate: Candidate) -> str:
    metadata = dict(candidate.metadata or {})
    parts = [
        candidate.organization,
        candidate.role,
        candidate.team,
        str(metadata.get("current_company") or "").strip(),
        " | ".join(str(item).strip() for item in list(metadata.get("scope_keywords") or []) if str(item).strip()),
    ]
    return " | ".join(part for part in parts if part)


def _candidate_acquisition_scope_text(candidate: Candidate) -> str:
    metadata = dict(candidate.metadata or {})
    parts = [
        str(metadata.get("seed_query") or "").strip(),
        " | ".join(str(item).strip() for item in list(metadata.get("scope_keywords") or []) if str(item).strip()),
        " | ".join(str(item).strip() for item in list(metadata.get("source_queries") or []) if str(item).strip()),
    ]
    return " | ".join(part for part in parts if part)


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
