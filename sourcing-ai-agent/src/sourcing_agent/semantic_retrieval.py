from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import math
import re
from typing import Any

from .domain import (
    Candidate,
    JobRequest,
    candidate_profile_signal_text,
    derive_candidate_facets,
    derive_candidate_role_bucket,
)
from .scoring import build_query_terms, candidate_matches_structured_filters
from .semantic_provider import SemanticProvider


SEMANTIC_FIELD_WEIGHTS = {
    "organization": 1.4,
    "role": 3.0,
    "team": 2.7,
    "focus_areas": 2.8,
    "derived_facets": 3.2,
    "investment_involvement": 2.0,
    "education": 1.1,
    "work_history": 1.6,
    "notes": 1.8,
    "ethnicity_background": 0.8,
    "current_destination": 0.8,
}


@dataclass(slots=True)
class SemanticHit:
    candidate_id: str
    semantic_score: float
    cosine_score: float
    overlap_terms: list[str]
    matched_fields: list[dict[str, Any]]
    explanation: str

    def to_record(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "semantic_score": self.semantic_score,
            "cosine_score": self.cosine_score,
            "overlap_terms": self.overlap_terms,
            "matched_fields": self.matched_fields,
            "explanation": self.explanation,
        }


@dataclass(slots=True)
class _SemanticDocument:
    candidate: Candidate
    token_weights: dict[str, float]
    term_fields: dict[str, list[str]]
    field_values: dict[str, str]


def rank_semantic_candidates(
    candidates: list[Candidate],
    request: JobRequest,
    *,
    criteria_patterns: list[dict[str, Any]] | None = None,
    semantic_fields: list[str] | None = None,
    limit: int = 15,
    semantic_provider: SemanticProvider | None = None,
) -> dict[str, dict[str, Any]]:
    if limit <= 0:
        return {}
    candidate_pool = [candidate for candidate in candidates if candidate_matches_structured_filters(candidate, request)]
    if not candidate_pool:
        return {}

    query_terms = _build_semantic_query_terms(request, criteria_patterns or [])
    if not query_terms:
        return {}

    fields = _semantic_fields_for_request(request, semantic_fields)
    documents = [_build_document(candidate, fields) for candidate in candidate_pool]
    if _should_use_remote_semantic_provider(request, semantic_provider):
        provider_hits = _rank_with_provider(query_terms, documents, semantic_provider, limit=limit)
        if provider_hits:
            return provider_hits
    query_vector, query_norm = _build_query_vector(query_terms, documents)
    if not query_vector or not query_norm:
        return {}

    hits: list[SemanticHit] = []
    for document in documents:
        hit = _score_document(document, query_vector, query_norm)
        if hit is not None:
            hits.append(hit)
    hits.sort(key=lambda item: (-item.semantic_score, -item.cosine_score, item.candidate_id))
    return {item.candidate_id: item.to_record() for item in hits[:limit]}


def _should_use_remote_semantic_provider(
    request: JobRequest,
    semantic_provider: SemanticProvider | None,
) -> bool:
    if semantic_provider is None:
        return False
    if semantic_provider.provider_name() == "local_sparse":
        return False
    execution_preferences = dict(request.execution_preferences or {})
    return bool(execution_preferences.get("allow_high_cost_sources"))


def _rank_with_provider(
    query_terms: list[str],
    documents: list[_SemanticDocument],
    semantic_provider: SemanticProvider,
    *,
    limit: int,
) -> dict[str, dict[str, Any]]:
    query_text = " ".join(query_terms).strip()
    if not query_text or not documents:
        return {}
    provider_limit = max(limit * 3, limit)
    local_hits = _rank_local_candidates(query_terms, documents, limit=provider_limit)
    if not local_hits:
        local_hits = [
            SemanticHit(
                candidate_id=document.candidate.candidate_id,
                semantic_score=0.0,
                cosine_score=0.0,
                overlap_terms=[],
                matched_fields=[],
                explanation="preselected for provider rerank",
            )
            for document in documents[:provider_limit]
        ]
    selected_documents: list[_SemanticDocument] = []
    document_lookup = {document.candidate.candidate_id: document for document in documents}
    for hit in local_hits:
        document = document_lookup.get(hit.candidate_id)
        if document is not None:
            selected_documents.append(document)
    doc_texts = [_document_text(document) for document in selected_documents]
    if not doc_texts:
        return {}

    try:
        query_vectors = semantic_provider.embed_texts([query_text] + doc_texts)
    except Exception:
        query_vectors = []
    embedding_scores: dict[str, float] = {}
    if len(query_vectors) == len(doc_texts) + 1:
        query_vector = query_vectors[0]
        for document, vector in zip(selected_documents, query_vectors[1:]):
            embedding_scores[document.candidate.candidate_id] = round(_vector_cosine(query_vector, vector), 4)

    try:
        reranked = semantic_provider.rerank(query_text, doc_texts, top_n=limit)
    except Exception:
        reranked = []
    if not reranked:
        return {}

    hits: list[SemanticHit] = []
    for item in reranked:
        index = int(item.get("index") or 0)
        if index >= len(selected_documents):
            continue
        document = selected_documents[index]
        score = float(item.get("relevance_score") or 0.0)
        overlap_terms = _top_overlap_terms(document, query_terms)
        field_name = (document.term_fields.get(overlap_terms[0]) or ["semantic_document"])[0] if overlap_terms else "semantic_document"
        matched_fields = [
            {
                "keyword": term,
                "matched_on": term,
                "field": (document.term_fields.get(term) or [field_name])[0],
                "value": _shorten(document.field_values.get((document.term_fields.get(term) or [field_name])[0], "")),
                "weight": round(score, 2),
                "match_kind": "semantic",
            }
            for term in overlap_terms[:4]
        ]
        explanation = (
            f"external semantic rerank {score:.2f}"
            + (
                f", embedding={embedding_scores[document.candidate.candidate_id]:.2f}"
                if document.candidate.candidate_id in embedding_scores
                else ""
            )
        )
        hits.append(
            SemanticHit(
                candidate_id=document.candidate.candidate_id,
                semantic_score=round(min(6.0, score * 6.0 + embedding_scores.get(document.candidate.candidate_id, 0.0) * 2.0), 2),
                cosine_score=embedding_scores.get(document.candidate.candidate_id, round(score, 4)),
                overlap_terms=overlap_terms,
                matched_fields=matched_fields,
                explanation=explanation,
            )
        )
    hits.sort(key=lambda item: (-item.semantic_score, -item.cosine_score, item.candidate_id))
    return {item.candidate_id: item.to_record() for item in hits[:limit]}


def _rank_local_candidates(query_terms: list[str], documents: list[_SemanticDocument], *, limit: int) -> list[SemanticHit]:
    query_vector, query_norm = _build_query_vector(query_terms, documents)
    if not query_vector or not query_norm:
        return []
    hits: list[SemanticHit] = []
    for document in documents:
        hit = _score_document(document, query_vector, query_norm)
        if hit is not None:
            hits.append(hit)
    hits.sort(key=lambda item: (-item.semantic_score, -item.cosine_score, item.candidate_id))
    return hits[:limit]


def _build_semantic_query_terms(request: JobRequest, criteria_patterns: list[dict[str, Any]]) -> list[str]:
    raw_terms = build_query_terms(request, criteria_patterns)
    if not raw_terms:
        raw_terms = [request.query or request.raw_user_request]
    expanded: list[str] = []
    for term in raw_terms:
        expanded.extend(_text_features(term))
    return _dedupe(expanded)


def _semantic_fields_for_request(request: JobRequest, semantic_fields: list[str] | None) -> list[str]:
    fields = list(semantic_fields or SEMANTIC_FIELD_WEIGHTS.keys())
    if request.must_have_primary_role_buckets:
        return [field_name for field_name in fields if field_name != "notes"]
    return fields


def _build_document(candidate: Candidate, semantic_fields: list[str]) -> _SemanticDocument:
    token_weights: dict[str, float] = {}
    term_fields: dict[str, list[str]] = {}
    field_values: dict[str, str] = {}
    for field_name in semantic_fields:
        value = _semantic_field_value(candidate, field_name)
        if not value:
            continue
        field_values[field_name] = value
        weight = float(SEMANTIC_FIELD_WEIGHTS.get(field_name, 1.0))
        for token in _text_features(value):
            token_weights[token] = round(token_weights.get(token, 0.0) + weight, 4)
            term_fields.setdefault(token, [])
            if field_name not in term_fields[token]:
                term_fields[token].append(field_name)
    return _SemanticDocument(
        candidate=candidate,
        token_weights=token_weights,
        term_fields=term_fields,
        field_values=field_values,
    )


def _build_query_vector(query_terms: list[str], documents: list[_SemanticDocument]) -> tuple[dict[str, float], float]:
    doc_count = max(len(documents), 1)
    df = Counter()
    for document in documents:
        for term in set(query_terms):
            if term in document.token_weights:
                df[term] += 1

    query_vector: dict[str, float] = {}
    for term in query_terms:
        idf = math.log((doc_count + 1) / (df.get(term, 0) + 1)) + 1.0
        query_vector[term] = round(query_vector.get(term, 0.0) + idf, 6)
    norm = math.sqrt(sum(value * value for value in query_vector.values()))
    return query_vector, norm


def _score_document(
    document: _SemanticDocument,
    query_vector: dict[str, float],
    query_norm: float,
) -> SemanticHit | None:
    doc_norm = 0.0
    dot = 0.0
    overlapping_terms: list[str] = []
    for term, query_weight in query_vector.items():
        doc_weight = float(document.token_weights.get(term) or 0.0)
        if not doc_weight:
            continue
        dot += query_weight * doc_weight
        doc_norm += doc_weight * doc_weight
        overlapping_terms.append(term)
    if not dot or not doc_norm or not query_norm:
        return None
    cosine = dot / (query_norm * math.sqrt(doc_norm))
    if cosine < 0.06:
        return None

    overlap_terms = _dedupe(overlapping_terms)[:6]
    matched_fields: list[dict[str, Any]] = []
    distinct_fields: set[str] = set()
    for term in overlap_terms[:4]:
        field_name = (document.term_fields.get(term) or ["semantic_document"])[0]
        distinct_fields.add(field_name)
        matched_fields.append(
            {
                "keyword": term,
                "matched_on": term,
                "field": field_name,
                "value": _shorten(document.field_values.get(field_name, "")),
                "weight": round(cosine, 2),
                "match_kind": "semantic",
            }
        )

    semantic_score = round(min(6.0, cosine * 8.0 + min(len(distinct_fields), 3) * 0.35), 2)
    explanation = (
        f"sparse-vector similarity {cosine:.2f} via "
        f"{', '.join(overlap_terms[:3]) or 'semantic overlap'}"
    )
    return SemanticHit(
        candidate_id=document.candidate.candidate_id,
        semantic_score=semantic_score,
        cosine_score=round(cosine, 4),
        overlap_terms=overlap_terms,
        matched_fields=matched_fields,
        explanation=explanation,
    )


def _document_text(document: _SemanticDocument) -> str:
    return " ".join(
        value
        for _, value in sorted(document.field_values.items())
        if value
    )


def _top_overlap_terms(document: _SemanticDocument, query_terms: list[str]) -> list[str]:
    ranked = []
    for term in _dedupe(query_terms):
        weight = float(document.token_weights.get(term) or 0.0)
        if weight:
            ranked.append((weight, term))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [term for _, term in ranked[:6]]


def _semantic_field_value(candidate: Candidate, field_name: str) -> str:
    if field_name == "derived_facets":
        return " | ".join([derive_candidate_role_bucket(candidate)] + derive_candidate_facets(candidate))
    if field_name == "notes":
        return candidate_profile_signal_text(candidate, include_notes=True)
    value = str(getattr(candidate, field_name, "") or "").strip()
    return value


def _text_features(value: str) -> list[str]:
    compact = " ".join(str(value or "").lower().replace("/", " ").replace("-", " ").split())
    if not compact:
        return []
    tokens = re.findall(r"[\u4e00-\u9fff]{2,}|[a-z0-9\+]{2,}", compact)
    features: list[str] = list(tokens)
    joined = "".join(tokens)
    if 4 <= len(joined) <= 32:
        features.append(joined)
    if 4 <= len(joined) <= 18:
        features.extend(joined[index : index + 3] for index in range(0, len(joined) - 2))
    return _dedupe(features)


def _shorten(value: str, limit: int = 120) -> str:
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _vector_cosine(left: list[float], right: list[float]) -> float:
    size = min(len(left), len(right))
    if size == 0:
        return 0.0
    left_norm = math.sqrt(sum(value * value for value in left[:size])) or 1.0
    right_norm = math.sqrt(sum(value * value for value in right[:size])) or 1.0
    dot = sum(left[index] * right[index] for index in range(size))
    return dot / (left_norm * right_norm)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in items:
        normalized = " ".join(str(item or "").split()).strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results
