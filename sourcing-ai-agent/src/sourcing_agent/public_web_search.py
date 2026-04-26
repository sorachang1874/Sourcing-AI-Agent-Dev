from __future__ import annotations

import json
import re
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from hashlib import sha1
from html import unescape
from pathlib import Path
from typing import Any, Protocol
from urllib import parse

from .asset_logger import AssetLogger
from .document_extraction import analyze_remote_document, empty_signal_bundle, merge_signal_bundle
from .domain import Candidate
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .search_provider import (
    BaseSearchProvider,
    SearchBatchFetchTask,
    SearchResponse,
    SearchResultItem,
)

DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES: tuple[str, ...] = (
    "profile_web_presence",
    "resume_and_documents",
    "technical_presence",
    "social_presence",
    "candidate_publication_presence",
    "scholar_profile_discovery",
    "contact_signals",
)

ENTRY_LINK_TYPES: tuple[str, ...] = (
    "personal_homepage",
    "resume_url",
    "github_url",
    "x_url",
    "substack_url",
    "scholar_url",
    "publication_url",
    "academic_profile",
    "company_page",
    "linkedin_url",
    "other",
)

MODEL_LINK_TYPE_ALIASES = {
    "scholar_profile": "scholar_url",
    "google_scholar": "scholar_url",
    "google_scholar_profile": "scholar_url",
    "github_profile": "github_url",
    "github_repository": "github_url",
    "github_repo": "github_url",
    "personal_url": "personal_homepage",
    "personal_website": "personal_homepage",
    "homepage": "personal_homepage",
    "website": "personal_homepage",
    "x_profile": "x_url",
    "twitter_profile": "x_url",
    "substack_profile": "substack_url",
    "paper": "publication_url",
    "publication": "publication_url",
    "academic_page": "academic_profile",
}

FETCHABLE_ENTRY_LINK_TYPES = {
    "personal_homepage",
    "resume_url",
    "github_url",
    "x_url",
    "substack_url",
    "scholar_url",
    "publication_url",
    "academic_profile",
}

FETCH_QUEUE_ENTRY_TYPE_PRIORITY = (
    "personal_homepage",
    "scholar_url",
    "github_url",
    "x_url",
    "substack_url",
    "resume_url",
    "academic_profile",
    "publication_url",
)
FETCH_QUEUE_ENTRY_TYPE_CAPS = {
    "personal_homepage": 2,
    "scholar_url": 2,
    "github_url": 1,
    "x_url": 1,
    "substack_url": 1,
    "resume_url": 1,
    "academic_profile": 1,
    "publication_url": 1,
}
HIGH_PRIORITY_DISCOVERED_FETCH_TYPES = {"resume_url"}
ADJUDICATION_ENTRY_LINK_LIMIT = 50
ADJUDICATION_ENTRY_TYPE_CAPS = {
    "personal_homepage": 6,
    "scholar_url": 8,
    "github_url": 8,
    "x_url": 8,
    "substack_url": 8,
    "resume_url": 5,
    "academic_profile": 4,
    "publication_url": 5,
    "linkedin_url": 2,
    "company_page": 2,
    "other": 2,
}
ADJUDICATION_ENTRY_TYPE_PRIORITY = (
    "personal_homepage",
    "scholar_url",
    "github_url",
    "x_url",
    "substack_url",
    "resume_url",
    "academic_profile",
    "publication_url",
    "linkedin_url",
    "company_page",
    "other",
)

PUBLICATION_DOMAINS = {
    "arxiv.org",
    "openreview.net",
    "semanticscholar.org",
    "www.semanticscholar.org",
    "aclanthology.org",
    "papers.nips.cc",
    "proceedings.mlr.press",
    "dl.acm.org",
    "ieeexplore.ieee.org",
    "researchgate.net",
    "www.researchgate.net",
}

LOW_VALUE_FETCH_DOMAINS = {
    "linkedin.com",
    "www.linkedin.com",
    "google.com",
    "www.google.com",
    "scholar.google.com",
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
    "facebook.com",
    "www.facebook.com",
    "rocketreach.co",
    "www.rocketreach.co",
    "zoominfo.com",
    "www.zoominfo.com",
    "apollo.io",
    "www.apollo.io",
    "signalhire.com",
    "www.signalhire.com",
    "contactout.com",
    "www.contactout.com",
    "clay.earth",
    "idcrawl.com",
    "www.idcrawl.com",
    "luma.com",
    "www.luma.com",
    "theorg.com",
    "www.theorg.com",
    "twstalker.com",
    "mobile.twstalker.com",
    "ww.twstalker.com",
}

GENERIC_EMAIL_LOCAL_PARTS = {
    "admin",
    "careers",
    "contact",
    "hello",
    "help",
    "info",
    "jobs",
    "media",
    "news",
    "noreply",
    "no-reply",
    "press",
    "privacy",
    "recruiting",
    "security",
    "support",
    "team",
}

ACADEMIC_EMAIL_DOMAIN_HINTS = (
    ".edu",
    ".ac.",
    ".edu.",
    ".edu",
    "mit.edu",
    "stanford.edu",
    "berkeley.edu",
    "cmu.edu",
    "harvard.edu",
    "ox.ac.uk",
    "cam.ac.uk",
)

SEARCH_NOISE_NEGATIVE_FILTER = (
    "-site:rocketreach.co "
    "-site:zoominfo.com "
    "-site:apollo.io "
    "-site:signalhire.com "
    "-site:contactout.com "
    "-site:idcrawl.com "
    "-site:linkedin.com "
    "-site:twstalker.com "
    "-site:luma.com "
    "-site:clay.earth "
    "-site:theorg.com "
    "-site:facebook.com"
)


class PublicWebModelClient(Protocol):
    def provider_name(self) -> str: ...

    def analyze_public_web_candidate_signals(self, payload: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class PublicWebExperimentOptions:
    source_families: tuple[str, ...] = DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES
    max_queries_per_candidate: int = 10
    max_results_per_query: int = 10
    max_entry_links_per_candidate: int = 40
    max_fetches_per_candidate: int = 5
    max_ai_evidence_documents: int = 8
    fetch_content: bool = True
    extract_contact_signals: bool = True
    ai_extraction: str = "auto"
    timeout_seconds: int = 30
    use_batch_search: bool = True
    batch_ready_poll_interval_seconds: float = 10.0
    max_batch_ready_polls: int = 18
    max_concurrent_fetches_per_candidate: int = 4
    max_concurrent_candidate_analyses: int = 2


@dataclass(frozen=True, slots=True)
class PublicWebCandidateContext:
    record_id: str
    candidate_id: str
    candidate_name: str
    current_company: str
    headline: str = ""
    linkedin_url: str = ""
    linkedin_url_key: str = ""
    primary_email: str = ""
    job_id: str = ""
    history_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PublicWebQuerySpec:
    query_id: str
    source_family: str
    query_text: str
    objective: str

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ClassifiedEntryLink:
    url: str
    normalized_url: str
    title: str
    snippet: str
    source_domain: str
    entry_type: str
    source_family: str
    score: float
    reasons: tuple[str, ...] = ()
    query_id: str = ""
    query_text: str = ""
    provider_name: str = ""
    result_rank: int = 0
    fetchable: bool = False
    identity_match_label: str = "unreviewed"
    identity_match_score: float = 0.0
    confidence_label: str = "medium"
    adjudication: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class EmailCandidateSignal:
    value: str
    normalized_value: str
    email_type: str
    confidence_label: str
    confidence_score: float
    publishable: bool
    promotion_status: str
    source_url: str
    source_domain: str
    source_family: str
    source_title: str = ""
    evidence_excerpt: str = ""
    suppression_reason: str = ""
    identity_match_label: str = "needs_ai_review"
    identity_match_score: float = 0.0
    adjudication: dict[str, Any] = field(default_factory=dict)

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CandidateSearchPlan:
    ordinal: int
    candidate: PublicWebCandidateContext
    candidate_dir: Path
    logger: AssetLogger
    queries: list[PublicWebQuerySpec]
    started_monotonic: float


@dataclass(slots=True)
class CandidateSearchOutcome:
    query_results: list[dict[str, Any]] = field(default_factory=list)
    raw_links: list[ClassifiedEntryLink] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    search_mode: str = "sequential"


@dataclass(slots=True)
class PublicWebFetchResult:
    fetch_index: int
    link: ClassifiedEntryLink
    document_record: dict[str, Any]
    signals: dict[str, Any] = field(default_factory=dict)
    email_candidates: list[EmailCandidateSignal] = field(default_factory=list)
    discovered_entry_links: list[ClassifiedEntryLink] = field(default_factory=list)
    error: str = ""


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def public_web_experiment_run_id(prefix: str = "public-web-exp") -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{stamp}"


def candidate_context_from_target_candidate(record: dict[str, Any]) -> PublicWebCandidateContext:
    metadata = _candidate_metadata_from_target_candidate(record)
    linkedin_url = str(record.get("linkedin_url") or "").strip()
    return PublicWebCandidateContext(
        record_id=str(record.get("id") or record.get("record_id") or "").strip(),
        candidate_id=str(record.get("candidate_id") or "").strip(),
        candidate_name=str(
            record.get("candidate_name") or record.get("display_name") or record.get("name") or ""
        ).strip(),
        current_company=str(
            record.get("current_company") or record.get("company") or metadata.get("target_company") or ""
        ).strip(),
        headline=str(record.get("headline") or "").strip(),
        linkedin_url=linkedin_url,
        linkedin_url_key=normalize_linkedin_profile_url_key(linkedin_url),
        primary_email=str(record.get("primary_email") or "").strip(),
        job_id=str(record.get("job_id") or "").strip(),
        history_id=str(record.get("history_id") or "").strip(),
        metadata=metadata,
    )


def candidate_from_context(context: PublicWebCandidateContext) -> Candidate:
    return Candidate(
        candidate_id=context.candidate_id or context.record_id or _short_hash(context.candidate_name, context.current_company),
        name_en=context.candidate_name,
        display_name=context.candidate_name,
        target_company=context.current_company,
        organization=context.current_company,
        role=context.headline,
        linkedin_url=context.linkedin_url,
        source_dataset="target_candidates_public_web_search",
        metadata={
            "target_candidate_record_id": context.record_id,
            "linkedin_url_key": context.linkedin_url_key,
            **context.metadata,
        },
    )


def plan_candidate_public_web_queries(
    candidate: PublicWebCandidateContext,
    *,
    source_families: tuple[str, ...] = DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES,
    max_queries: int = 8,
) -> list[PublicWebQuerySpec]:
    name = candidate.candidate_name.strip()
    company = candidate.current_company.strip()
    if not name:
        return []
    quoted_name = _quote_search_term(name)
    quoted_company = _quote_search_term(company) if company else ""
    family_order = [family for family in source_families if family in DEFAULT_TARGET_CANDIDATE_SOURCE_FAMILIES]
    planned: list[tuple[str, str, str]] = []

    def add(source_family: str, query: str, objective: str) -> None:
        if source_family in family_order and query.strip():
            planned.append((source_family, _normalize_query_text(query), objective))

    base_with_company = " ".join(item for item in [quoted_name, quoted_company] if item)
    add("profile_web_presence", base_with_company, "Find general public web presence and likely personal pages.")
    add(
        "profile_web_presence",
        f'{base_with_company} (homepage OR "personal website" OR "personal site") {SEARCH_NOISE_NEGATIVE_FILTER}',
        "Find likely personal homepages while avoiding contact directories.",
    )
    add(
        "profile_web_presence",
        f'{quoted_name} (homepage OR "personal website" OR "personal site") {SEARCH_NOISE_NEGATIVE_FILTER}',
        "Fallback personal homepage discovery without company constraint.",
    )
    add(
        "scholar_profile_discovery",
        f"{quoted_name} {quoted_company} site:scholar.google.com/citations" if quoted_company else f"{quoted_name} site:scholar.google.com/citations",
        "Find company-constrained Google Scholar profile pages.",
    )
    add(
        "technical_presence",
        f"{quoted_name} {quoted_company} site:github.com" if quoted_company else f"{quoted_name} site:github.com",
        "Find company-constrained GitHub profile and project presence.",
    )
    add("technical_presence", f"{quoted_name} site:github.com", "Fallback GitHub profile discovery without company constraint.")
    add(
        "scholar_profile_discovery",
        f"{quoted_name} site:scholar.google.com/citations",
        "Fallback Google Scholar profile discovery without company constraint.",
    )
    add(
        "social_presence",
        f"{quoted_name} {quoted_company} (site:x.com OR site:twitter.com)",
        "Find X/Twitter presence.",
    )
    add(
        "social_presence",
        f"{quoted_name} (site:x.com OR site:twitter.com)",
        "Fallback X/Twitter discovery without company constraint.",
    )
    add(
        "social_presence",
        f"{quoted_name} site:substack.com",
        "Find Substack presence.",
    )
    add(
        "social_presence",
        f"{base_with_company} site:substack.com",
        "Find company-context Substack presence.",
    )
    add(
        "candidate_publication_presence",
        f"{base_with_company} (arxiv OR OpenReview OR publication)",
        "Find publications and paper pages.",
    )
    add(
        "contact_signals",
        f"{base_with_company} academic email university {SEARCH_NOISE_NEGATIVE_FILTER}",
        "Low-priority fallback for public academic email evidence while avoiding contact directories.",
    )
    add(
        "resume_and_documents",
        f"{quoted_name} CV filetype:pdf {quoted_company} {SEARCH_NOISE_NEGATIVE_FILTER}",
        "Low-priority fallback for direct CV PDFs after higher-precision entry discovery queries.",
    )
    add(
        "contact_signals",
        f"{base_with_company} email contact {SEARCH_NOISE_NEGATIVE_FILTER}",
        "Low-priority fallback for public contact evidence while avoiding contact directories.",
    )
    add("candidate_publication_presence", f"{quoted_name} arXiv", "Find arXiv author or paper pages.")

    deduped: list[PublicWebQuerySpec] = []
    seen: set[str] = set()
    for source_family, query_text, objective in planned:
        key = _query_key(query_text)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(
            PublicWebQuerySpec(
                query_id=f"q{len(deduped) + 1:02d}",
                source_family=source_family,
                query_text=query_text,
                objective=objective,
            )
        )
        if len(deduped) >= max(1, int(max_queries or 1)):
            break
    return deduped


def classify_public_web_url(
    *,
    url: str,
    title: str = "",
    snippet: str = "",
    candidate: PublicWebCandidateContext | None = None,
    query: PublicWebQuerySpec | None = None,
    provider_name: str = "",
    result_rank: int = 0,
) -> ClassifiedEntryLink | None:
    normalized_url = normalize_public_web_url(url)
    if not normalized_url:
        return None
    parsed = parse.urlparse(normalized_url)
    domain = parsed.netloc.lower().removeprefix("www.")
    lower_url = normalized_url.lower()
    combined = " ".join([title, snippet, normalized_url]).lower()
    entry_type = "other"
    source_family = query.source_family if query else "profile_web_presence"
    score = 10.0
    reasons: list[str] = []

    if "linkedin.com/in/" in lower_url:
        entry_type = "linkedin_url"
        score += 20
        reasons.append("linkedin_profile_url")
    elif domain == "scholar.google.com" or "scholar.google." in domain:
        entry_type = "scholar_url"
        source_family = "scholar_profile_discovery"
        score += 85
        reasons.append("google_scholar_domain")
    elif domain == "github.com":
        entry_type = "github_url"
        source_family = "technical_presence"
        score += 78
        reasons.append("github_domain")
    elif domain in {"x.com", "twitter.com"}:
        if _looks_like_x_search_or_utility_url(parsed.path, parsed.query):
            entry_type = "other"
            score += 8
            reasons.append("x_twitter_search_or_utility_page")
        else:
            entry_type = "x_url"
            source_family = "social_presence"
            score += 62
            reasons.append("x_twitter_domain")
    elif domain.endswith("substack.com") or "substack" in domain:
        entry_type = "substack_url"
        source_family = "social_presence"
        score += 68
        reasons.append("substack_domain")
    elif _is_low_value_fetch_domain(domain):
        entry_type = "other"
        score += 12
        reasons.append("low_value_directory_or_aggregator")
    elif _looks_like_resume_url(lower_url, combined):
        entry_type = "resume_url"
        source_family = "resume_and_documents"
        score += 98
        reasons.append("resume_or_cv_indicator")
    elif domain in PUBLICATION_DOMAINS or any(token in lower_url for token in ["/paper", "/publication", "/abs/", "/pdf/"]):
        entry_type = "publication_url"
        source_family = "candidate_publication_presence"
        score += 66
        reasons.append("publication_surface")
    elif _looks_like_academic_profile(domain, lower_url, combined):
        entry_type = "academic_profile"
        source_family = "candidate_publication_presence"
        score += 72
        reasons.append("academic_profile_surface")
    elif _looks_like_personal_homepage(normalized_url, combined, candidate, title=title):
        entry_type = "personal_homepage"
        source_family = "profile_web_presence"
        score += 74
        reasons.append("personal_homepage_shape")
    elif candidate and _looks_like_company_page(domain, lower_url, combined, candidate):
        entry_type = "company_page"
        score += 45
        reasons.append("target_company_page")

    if candidate:
        candidate_name = candidate.candidate_name.lower()
        company = candidate.current_company.lower()
        if candidate_name and _name_tokens_match(candidate_name, combined):
            score += 18
            reasons.append("candidate_name_match")
        if company and company in combined:
            score += 8
            reasons.append("company_match")
    if result_rank:
        score += max(0, 8 - min(result_rank, 8))

    fetchable = entry_type in FETCHABLE_ENTRY_LINK_TYPES and (
        entry_type in {"scholar_url", "x_url"} or not _is_low_value_fetch_domain(domain)
    )
    return ClassifiedEntryLink(
        url=url,
        normalized_url=normalized_url,
        title=str(title or "").strip(),
        snippet=str(snippet or "").strip(),
        source_domain=domain,
        entry_type=entry_type,
        source_family=source_family,
        score=round(score, 2),
        reasons=tuple(reasons),
        query_id=query.query_id if query else "",
        query_text=query.query_text if query else "",
        provider_name=provider_name,
        result_rank=max(0, int(result_rank or 0)),
        fetchable=fetchable,
        confidence_label=confidence_label_from_score(min(score / 130.0, 0.95)),
    )


def rank_entry_links(links: list[ClassifiedEntryLink], *, limit: int = 20) -> list[ClassifiedEntryLink]:
    best_by_url: dict[str, ClassifiedEntryLink] = {}
    for link in links:
        key = normalize_public_web_url_key(link.normalized_url)
        if not key:
            continue
        current = best_by_url.get(key)
        if current is None or link.score > current.score:
            best_by_url[key] = link
    ranked = sorted(
        best_by_url.values(),
        key=lambda item: (
            item.entry_type == "other",
            -item.score,
            item.result_rank or 999,
            item.normalized_url,
        ),
    )
    return ranked[: max(1, int(limit or 1))]


def build_diversified_fetch_queue(
    ranked_links: list[ClassifiedEntryLink],
    *,
    max_fetches: int,
) -> list[ClassifiedEntryLink]:
    front_limit = max(0, int(max_fetches or 0))
    if front_limit <= 0:
        return []
    fetchable_links = [link for link in ranked_links if link.fetchable]
    if not fetchable_links:
        return []
    selected: list[ClassifiedEntryLink] = []
    selected_keys: set[str] = set()
    type_counts: dict[str, int] = {}

    def add(link: ClassifiedEntryLink, *, honor_cap: bool) -> bool:
        key = normalize_public_web_url_key(link.normalized_url)
        if not key or key in selected_keys:
            return False
        cap = FETCH_QUEUE_ENTRY_TYPE_CAPS.get(link.entry_type, 1)
        if honor_cap and type_counts.get(link.entry_type, 0) >= cap:
            return False
        selected.append(link)
        selected_keys.add(key)
        type_counts[link.entry_type] = type_counts.get(link.entry_type, 0) + 1
        return True

    available_types = {link.entry_type for link in fetchable_links}
    ordered_types = [
        entry_type for entry_type in FETCH_QUEUE_ENTRY_TYPE_PRIORITY if entry_type in available_types
    ] + sorted(available_types.difference(FETCH_QUEUE_ENTRY_TYPE_PRIORITY))

    for entry_type in ordered_types:
        if len(selected) >= front_limit:
            break
        match = next((link for link in fetchable_links if link.entry_type == entry_type), None)
        if match is not None:
            add(match, honor_cap=True)

    for entry_type in ordered_types:
        for link in fetchable_links:
            if len(selected) >= front_limit:
                break
            if link.entry_type == entry_type:
                add(link, honor_cap=True)
        if len(selected) >= front_limit:
            break
    for link in fetchable_links:
        if len(selected) >= front_limit:
            break
        add(link, honor_cap=False)

    remaining = [link for link in fetchable_links if normalize_public_web_url_key(link.normalized_url) not in selected_keys]
    return [*selected, *remaining]


def should_prioritize_discovered_fetch_link(link: ClassifiedEntryLink, *, source_url: str) -> bool:
    if link.entry_type in HIGH_PRIORITY_DISCOVERED_FETCH_TYPES:
        return True
    if link.entry_type != "personal_homepage":
        return False
    source_domain = parse.urlparse(normalize_public_web_url(source_url)).netloc.lower().removeprefix("www.")
    target_domain = parse.urlparse(normalize_public_web_url(link.normalized_url)).netloc.lower().removeprefix("www.")
    return bool(target_domain and target_domain != source_domain and "scholar.google." in source_domain)


def extract_email_candidate_signals(
    *,
    text: str,
    source_url: str,
    source_family: str,
    source_title: str = "",
    candidate: PublicWebCandidateContext | None = None,
) -> list[EmailCandidateSignal]:
    expanded_text = deobfuscate_email_text(text)
    emails = _dedupe_preserve_order(_EMAIL_RE.findall(expanded_text))
    publication_multi_email_context = source_family == "candidate_publication_presence" and len(emails) > 1
    signals: list[EmailCandidateSignal] = []
    domain = parse.urlparse(source_url).netloc.lower().removeprefix("www.")
    for email in emails:
        normalized_email = normalize_email(email)
        if not normalized_email:
            continue
        local_part = normalized_email.partition("@")[0]
        if _is_noise_email(normalized_email, raw_value=email) or _is_verified_domain_placeholder_email(
            normalized_email,
            expanded_text,
        ):
            continue
        email_type = infer_email_type(normalized_email, source_domain=domain, candidate=candidate)
        suppression_reason = ""
        publishable = True
        confidence_score = 0.45
        if local_part in GENERIC_EMAIL_LOCAL_PARTS:
            suppression_reason = "generic_inbox"
            publishable = False
            confidence_score = 0.18
        elif source_family in {"resume_and_documents", "profile_web_presence", "candidate_publication_presence"}:
            confidence_score = 0.75
        elif source_family == "contact_signals":
            confidence_score = 0.65
        if email_type == "academic":
            confidence_score += 0.08
        elif email_type == "personal":
            confidence_score += 0.05
        elif email_type == "generic":
            confidence_score -= 0.18
        if candidate and candidate.candidate_name:
            name_tokens = _person_name_tokens(candidate.candidate_name)
            if name_tokens and any(token in local_part for token in name_tokens):
                confidence_score += 0.08
            elif publication_multi_email_context:
                suppression_reason = suppression_reason or "coauthor_email_needs_ai_review"
                publishable = False
                confidence_score = min(confidence_score, 0.55)
        confidence_score = max(0.05, min(confidence_score, 0.95))
        confidence_label = confidence_label_from_score(confidence_score)
        if not suppression_reason and confidence_score >= 0.72:
            promotion_status = "promotion_recommended"
        else:
            promotion_status = "not_promoted"
        signals.append(
            EmailCandidateSignal(
                value=normalized_email,
                normalized_value=normalized_email,
                email_type=email_type,
                confidence_label=confidence_label,
                confidence_score=round(confidence_score, 2),
                publishable=publishable,
                promotion_status=promotion_status,
                source_url=source_url,
                source_domain=domain,
                source_family=source_family,
                source_title=source_title,
                evidence_excerpt=_email_excerpt(expanded_text, normalized_email),
                suppression_reason=suppression_reason,
            )
        )
    return _dedupe_email_signals(signals)


def adjudicate_public_web_signals(
    *,
    candidate: PublicWebCandidateContext,
    email_candidates: list[EmailCandidateSignal],
    entry_links: list[ClassifiedEntryLink],
    model_client: PublicWebModelClient | None,
    ai_extraction: str = "auto",
) -> tuple[list[EmailCandidateSignal], dict[str, Any]]:
    adjudication, result = run_public_web_candidate_adjudication(
        candidate=candidate,
        email_candidates=email_candidates,
        entry_links=entry_links,
        fetched_documents=[],
        model_client=model_client,
        ai_extraction=ai_extraction,
    )
    if result.get("status") != "completed":
        return email_candidates, result
    updated = apply_email_adjudication(email_candidates, adjudication)
    return updated, result


def adjudicate_public_web_candidate_evidence(
    *,
    candidate: PublicWebCandidateContext,
    email_candidates: list[EmailCandidateSignal],
    entry_links: list[ClassifiedEntryLink],
    fetched_documents: list[dict[str, Any]],
    model_client: PublicWebModelClient | None,
    ai_extraction: str = "auto",
    max_ai_evidence_documents: int = 8,
) -> tuple[list[EmailCandidateSignal], list[ClassifiedEntryLink], dict[str, Any]]:
    adjudication, result = run_public_web_candidate_adjudication(
        candidate=candidate,
        email_candidates=email_candidates,
        entry_links=entry_links,
        fetched_documents=fetched_documents,
        model_client=model_client,
        ai_extraction=ai_extraction,
        max_ai_evidence_documents=max_ai_evidence_documents,
    )
    if result.get("status") != "completed":
        return email_candidates, entry_links, result
    return (
        apply_email_adjudication(email_candidates, adjudication),
        apply_link_adjudication(entry_links, adjudication),
        result,
    )


def run_public_web_candidate_adjudication(
    *,
    candidate: PublicWebCandidateContext,
    email_candidates: list[EmailCandidateSignal],
    entry_links: list[ClassifiedEntryLink],
    fetched_documents: list[dict[str, Any]],
    model_client: PublicWebModelClient | None,
    ai_extraction: str = "auto",
    max_ai_evidence_documents: int = 8,
) -> tuple[dict[str, Any], dict[str, Any]]:
    mode = str(ai_extraction or "auto").strip().lower()
    if mode in {"0", "false", "off", "no", "disabled"}:
        return {}, {"status": "skipped", "reason": "ai_extraction_disabled"}
    if not email_candidates and not entry_links:
        return {}, {"status": "skipped", "reason": "no_signals"}
    if mode == "auto" and not email_candidates and not fetched_documents:
        return {}, {"status": "skipped", "reason": "auto_mode_entry_link_only"}
    if model_client is None or not hasattr(model_client, "analyze_public_web_candidate_signals"):
        return {}, {"status": "skipped", "reason": "model_client_unavailable"}
    if mode == "auto" and str(model_client.provider_name()).strip() in {"deterministic", "offline_model"}:
        return {}, {"status": "skipped", "reason": "auto_mode_non_live_model"}

    document_limit = max(1, int(max_ai_evidence_documents or 8))
    evidence_slices = [
        _compact_evidence_slice_for_adjudication(dict(item.get("evidence_slice") or {}))
        for item in fetched_documents[:document_limit]
        if isinstance(item, dict) and isinstance(item.get("evidence_slice"), dict)
    ]
    adjudication_entry_links = select_entry_links_for_adjudication(
        entry_links,
        limit=ADJUDICATION_ENTRY_LINK_LIMIT,
    )
    payload = {
        "candidate": build_candidate_adjudication_context(candidate),
        "email_candidates": [item.to_record() for item in email_candidates[:20]],
        "entry_links": [item.to_record() for item in adjudication_entry_links],
        "search_evidence": [_compact_search_result_for_adjudication(item) for item in adjudication_entry_links],
        "evidence_slices": evidence_slices,
        "fetched_documents": [
            {
                key: value
                for key, value in dict(item).items()
                if key
                in {
                    "source_url",
                    "final_url",
                    "entry_type",
                    "source_family",
                    "document_type",
                    "content_type",
                    "title",
                    "signals",
                    "analysis",
                    "email_candidates",
                    "evidence_slice_path",
                }
            }
            for item in fetched_documents[:document_limit]
            if isinstance(item, dict)
        ],
        "instructions": {
            "email_policy": (
                "Assess whether each email likely belongs to this candidate. Personal homepage, CV/resume, "
                "paper PDFs, and university profile evidence can be high confidence when identity matches. "
                "Generic inboxes and same-name collisions should be suppressed or marked needs_review. "
                "Suppress paper-title artifacts such as Learning@Scale.Conference and Control@Scale.Robotics. "
                "Handle grouped paper emails such as {barryz, lesli}@domain by judging each expanded address separately."
            ),
            "link_policy": (
                "Judge whether each homepage, GitHub, X/Twitter, Substack, Scholar, publication, resume, academic profile, "
                "company page, or LinkedIn result belongs to the target candidate. Mark ambiguous_identity when the page "
                "only shares a name or company but lacks ownership evidence. Use search_evidence for DataForSEO title/snippet "
                "context when a platform page cannot be fetched. URL shape matters: X/Twitter status/search/utility URLs, "
                "Substack posts/home feed/deep links, GitHub repositories/deep links, and non-citations Scholar URLs can be "
                "useful evidence but are not clean profile links."
            ),
            "evidence_policy": (
                "Use evidence_slices as the model-safe document evidence. They are source-aware excerpts from fetched pages "
                "or PDF front matter; raw HTML/PDF artifacts are audit inputs and are not included in this prompt."
            ),
        },
    }
    try:
        adjudication = model_client.analyze_public_web_candidate_signals(payload)
    except Exception as exc:  # pragma: no cover - live model failures are environment dependent
        return {}, {"status": "failed", "error": str(exc)[:300]}
    adjudication = sanitize_public_web_adjudication_for_payload(adjudication, payload)
    return (
        adjudication,
        {
            "status": "completed",
            "provider": model_client.provider_name(),
            "input_counts": {
                "email_candidates": len(email_candidates[:20]),
                "entry_links": len(adjudication_entry_links),
                "search_evidence": len(adjudication_entry_links),
                "fetched_documents": len(fetched_documents[:document_limit]),
                "evidence_slices": len(evidence_slices),
                "max_ai_evidence_documents": document_limit,
            },
            "result": adjudication,
        },
    )


def select_entry_links_for_adjudication(
    entry_links: list[ClassifiedEntryLink],
    *,
    limit: int = ADJUDICATION_ENTRY_LINK_LIMIT,
) -> list[ClassifiedEntryLink]:
    front_limit = max(1, int(limit or ADJUDICATION_ENTRY_LINK_LIMIT))
    deduped: list[ClassifiedEntryLink] = []
    seen: set[str] = set()
    for link in entry_links:
        key = normalize_public_web_url_key(link.normalized_url)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(link)
    selected: list[ClassifiedEntryLink] = []
    selected_keys: set[str] = set()
    type_counts: dict[str, int] = {}

    def add(link: ClassifiedEntryLink, *, honor_cap: bool) -> bool:
        if len(selected) >= front_limit:
            return False
        key = normalize_public_web_url_key(link.normalized_url)
        if not key or key in selected_keys:
            return False
        cap = ADJUDICATION_ENTRY_TYPE_CAPS.get(link.entry_type, 2)
        if honor_cap and type_counts.get(link.entry_type, 0) >= cap:
            return False
        selected.append(link)
        selected_keys.add(key)
        type_counts[link.entry_type] = type_counts.get(link.entry_type, 0) + 1
        return True

    available_types = {link.entry_type for link in deduped}
    ordered_types = [
        entry_type for entry_type in ADJUDICATION_ENTRY_TYPE_PRIORITY if entry_type in available_types
    ] + sorted(available_types.difference(ADJUDICATION_ENTRY_TYPE_PRIORITY))
    for entry_type in ordered_types:
        match = next((link for link in deduped if link.entry_type == entry_type), None)
        if match is not None:
            add(match, honor_cap=True)
    for entry_type in ordered_types:
        for link in deduped:
            if link.entry_type == entry_type:
                add(link, honor_cap=True)
            if len(selected) >= front_limit:
                break
        if len(selected) >= front_limit:
            break
    for link in deduped:
        if len(selected) >= front_limit:
            break
        add(link, honor_cap=False)
    return selected


def sanitize_public_web_adjudication_for_payload(
    adjudication: dict[str, Any] | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    normalized = dict(adjudication or {})
    allowed_emails = {
        normalize_email(str(item.get("normalized_value") or item.get("value") or item.get("email") or ""))
        for item in list(payload.get("email_candidates") or [])
        if isinstance(item, dict)
    }
    normalized["email_assessments"] = [
        item
        for item in list(normalized.get("email_assessments") or [])
        if isinstance(item, dict) and normalize_email(str(item.get("email") or item.get("value") or "")) in allowed_emails
    ]
    allowed_link_keys = {
        normalize_public_web_url_key(str(item.get("normalized_url") or item.get("url") or ""))
        for item in list(payload.get("entry_links") or [])
        if isinstance(item, dict)
    }
    link_assessments: list[dict[str, Any]] = []
    for item in list(normalized.get("link_assessments") or []):
        if not isinstance(item, dict):
            continue
        key = normalize_public_web_url_key(str(item.get("url") or ""))
        if not key or key not in allowed_link_keys:
            continue
        link_assessments.append(
            {
                **item,
                "signal_type": normalize_model_link_signal_type(item.get("signal_type"), fallback="other"),
            }
        )
    normalized["link_assessments"] = link_assessments
    return normalized


def _compact_evidence_slice_for_adjudication(slice_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_url": str(slice_payload.get("source_url") or "").strip(),
        "final_url": str(slice_payload.get("final_url") or "").strip(),
        "source_type": str(slice_payload.get("source_type") or "").strip(),
        "document_type": str(slice_payload.get("document_type") or "").strip(),
        "title": str(slice_payload.get("title") or "").strip(),
        "description": str(slice_payload.get("description") or "").strip(),
        "selected_text": str(slice_payload.get("selected_text") or "").strip()[:6000],
        "email_contexts": list(slice_payload.get("email_contexts") or [])[:8],
        "links": dict(slice_payload.get("links") or {}),
        "structured_signals": dict(slice_payload.get("structured_signals") or {}),
    }


def _compact_search_result_for_adjudication(link: ClassifiedEntryLink) -> dict[str, Any]:
    link_shape_warnings = public_web_link_shape_warnings(link.entry_type, link.normalized_url)
    return {
        "url": link.normalized_url,
        "title": link.title,
        "snippet": link.snippet,
        "source_domain": link.source_domain,
        "entry_type": link.entry_type,
        "source_family": link.source_family,
        "query_text": link.query_text,
        "provider_name": link.provider_name,
        "result_rank": link.result_rank,
        "score": link.score,
        "reasons": list(link.reasons),
        "fetchable": link.fetchable,
        "link_shape_warnings": link_shape_warnings,
        "clean_profile_link": not link_shape_warnings,
    }


def _looks_like_x_profile_url(parsed: Any) -> bool:
    host = parsed.netloc.lower().removeprefix("www.")
    if host not in {"x.com", "twitter.com"}:
        return False
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 1:
        return False
    handle = path_parts[0].lower()
    return handle not in {
        "about",
        "compose",
        "download",
        "explore",
        "hashtag",
        "home",
        "i",
        "intent",
        "messages",
        "notifications",
        "privacy",
        "search",
        "settings",
        "share",
        "tos",
    }


def _looks_like_substack_profile_url(parsed: Any) -> bool:
    host = parsed.netloc.lower().removeprefix("www.")
    path_parts = [part for part in parsed.path.split("/") if part]
    lowered_parts = [part.lower() for part in path_parts]
    if host.endswith(".substack.com") and host != "substack.com":
        if not lowered_parts:
            return True
        if len(lowered_parts) == 1 and lowered_parts[0] in {"about", "archive"}:
            return True
        if lowered_parts[0] == "people" and len(lowered_parts) <= 2:
            return True
        return False
    if host == "substack.com" and len(path_parts) == 1 and path_parts[0].startswith("@"):
        return True
    return False


def _looks_like_google_scholar_profile_url(parsed: Any) -> bool:
    host = parsed.netloc.lower().removeprefix("www.")
    if host != "scholar.google.com":
        return False
    if parsed.path.rstrip("/") != "/citations":
        return False
    return bool(parse.parse_qs(parsed.query).get("user"))


def _looks_like_github_profile_url(parsed: Any) -> bool:
    host = parsed.netloc.lower().removeprefix("www.")
    if host != "github.com":
        return False
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 1:
        return False
    account = path_parts[0].lower()
    return account not in {
        "about",
        "apps",
        "blog",
        "collections",
        "customer-stories",
        "enterprise",
        "events",
        "explore",
        "features",
        "marketplace",
        "new",
        "notifications",
        "orgs",
        "pricing",
        "pulls",
        "search",
        "settings",
        "sponsors",
        "topics",
        "trending",
    }


def public_web_link_shape_warnings(entry_type: str, url: str) -> list[str]:
    normalized_entry_type = str(entry_type or "").strip()
    normalized_url = normalize_public_web_url(url)
    if not normalized_url:
        return ["profile_link_missing_url"] if normalized_entry_type in ENTRY_LINK_TYPES else []
    warnings: list[str] = []
    parsed = parse.urlparse(normalized_url)
    if normalized_entry_type == "github_url" and not _looks_like_github_profile_url(parsed):
        warnings.append("github_repository_or_deep_link_not_profile")
    if normalized_entry_type == "x_url" and not _looks_like_x_profile_url(parsed):
        warnings.append("x_link_not_profile")
    if normalized_entry_type == "substack_url" and not _looks_like_substack_profile_url(parsed):
        warnings.append("substack_link_not_profile_or_publication")
    if normalized_entry_type == "scholar_url" and not _looks_like_google_scholar_profile_url(parsed):
        warnings.append("scholar_link_not_profile")
    return warnings


def is_clean_profile_link(entry_type: str, url: str) -> bool:
    return not public_web_link_shape_warnings(entry_type, url)


def build_candidate_adjudication_context(candidate: PublicWebCandidateContext) -> dict[str, Any]:
    metadata_candidate = dict(candidate.metadata.get("candidate") or {}) if isinstance(candidate.metadata, dict) else {}
    return {
        **candidate.to_record(),
        "profile_identity": {
            "linkedin_url": candidate.linkedin_url,
            "linkedin_url_key": candidate.linkedin_url_key,
            "primary_email": candidate.primary_email,
        },
        "known_profile_context": {
            "headline": candidate.headline,
            "current_company": candidate.current_company,
            "education": _first_nonempty(
                metadata_candidate.get("education"),
                metadata_candidate.get("education_summary"),
                metadata_candidate.get("education_lines"),
                candidate.metadata.get("education") if isinstance(candidate.metadata, dict) else "",
                candidate.metadata.get("education_lines") if isinstance(candidate.metadata, dict) else "",
            ),
            "work_history": _first_nonempty(
                metadata_candidate.get("work_history"),
                metadata_candidate.get("experience"),
                metadata_candidate.get("experience_lines"),
                candidate.metadata.get("work_history") if isinstance(candidate.metadata, dict) else "",
                candidate.metadata.get("experience_lines") if isinstance(candidate.metadata, dict) else "",
            ),
            "role": metadata_candidate.get("role") or metadata_candidate.get("title") or candidate.headline,
            "organization": metadata_candidate.get("organization") or candidate.current_company,
            "source_profile_url": metadata_candidate.get("profile_url")
            or metadata_candidate.get("linkedin_url")
            or candidate.linkedin_url,
        },
    }


def apply_email_adjudication(
    email_candidates: list[EmailCandidateSignal],
    adjudication: dict[str, Any],
) -> list[EmailCandidateSignal]:
    assessments = list(adjudication.get("email_assessments") or [])
    by_email: dict[str, dict[str, Any]] = {}
    for item in assessments:
        if not isinstance(item, dict):
            continue
        email = normalize_email(str(item.get("email") or item.get("value") or ""))
        if email:
            by_email[email] = item
    updated: list[EmailCandidateSignal] = []
    for signal in email_candidates:
        assessment = by_email.get(signal.normalized_value)
        if not assessment:
            updated.append(signal)
            continue
        confidence_label = _coerce_confidence_label(assessment.get("confidence_label"), signal.confidence_label)
        confidence_score = _coerce_confidence_score(assessment.get("confidence_score"), signal.confidence_score)
        publishable = bool(assessment.get("publishable", signal.publishable))
        suppression_reason = str(assessment.get("suppression_reason") or signal.suppression_reason or "").strip()
        promotion_status = str(assessment.get("promotion_status") or signal.promotion_status).strip()
        if promotion_status not in {"not_promoted", "promotion_recommended", "rejected", "suppressed"}:
            promotion_status = "promotion_recommended" if publishable and confidence_score >= 0.72 else "not_promoted"
        identity_match_label = str(
            assessment.get("identity_match_label") or signal.identity_match_label or "needs_review"
        ).strip()
        identity_match_score = _coerce_confidence_score(
            assessment.get("identity_match_score"),
            signal.identity_match_score,
        )
        if identity_match_label in {"ambiguous_identity", "not_same_person"} or (
            identity_match_label == "needs_review" and identity_match_score < 0.5
        ):
            publishable = False
            if promotion_status == "promotion_recommended":
                promotion_status = "not_promoted"
        if suppression_reason:
            publishable = False
            if promotion_status == "promotion_recommended":
                promotion_status = "suppressed"
        updated.append(
            EmailCandidateSignal(
                value=signal.value,
                normalized_value=signal.normalized_value,
                email_type=str(assessment.get("email_type") or signal.email_type or "unknown").strip(),
                confidence_label=confidence_label,
                confidence_score=confidence_score,
                publishable=publishable,
                promotion_status=promotion_status,
                source_url=signal.source_url,
                source_domain=signal.source_domain,
                source_family=signal.source_family,
                source_title=signal.source_title,
                evidence_excerpt=signal.evidence_excerpt,
                suppression_reason=suppression_reason,
                identity_match_label=identity_match_label,
                identity_match_score=identity_match_score,
                adjudication={key: value for key, value in assessment.items() if key not in {"email", "value"}},
            )
        )
    return updated


def apply_link_adjudication(
    entry_links: list[ClassifiedEntryLink],
    adjudication: dict[str, Any],
) -> list[ClassifiedEntryLink]:
    assessments = list(adjudication.get("link_assessments") or [])
    by_url: dict[str, dict[str, Any]] = {}
    for item in assessments:
        if not isinstance(item, dict):
            continue
        key = normalize_public_web_url_key(str(item.get("url") or ""))
        if key:
            by_url[key] = item
    updated: list[ClassifiedEntryLink] = []
    for link in entry_links:
        assessment = by_url.get(normalize_public_web_url_key(link.normalized_url))
        if not assessment:
            updated.append(link)
            continue
        entry_type = normalize_model_link_signal_type(assessment.get("signal_type"), fallback=link.entry_type)
        link_shape_warnings = public_web_link_shape_warnings(entry_type, link.normalized_url)
        assessment_metadata = {
            key: value
            for key, value in assessment.items()
            if key != "url"
        }
        assessment_metadata["link_shape_warnings"] = link_shape_warnings
        assessment_metadata["clean_profile_link"] = not link_shape_warnings
        updated.append(
            ClassifiedEntryLink(
                url=link.url,
                normalized_url=link.normalized_url,
                title=link.title,
                snippet=link.snippet,
                source_domain=link.source_domain,
                entry_type=entry_type,
                source_family=link.source_family,
                score=link.score,
                reasons=link.reasons,
                query_id=link.query_id,
                query_text=link.query_text,
                provider_name=link.provider_name,
                result_rank=link.result_rank,
                fetchable=link.fetchable,
                identity_match_label=str(
                    assessment.get("identity_match_label") or link.identity_match_label or "needs_review"
                ).strip(),
                identity_match_score=_coerce_confidence_score(
                    assessment.get("identity_match_score"),
                    link.identity_match_score,
                ),
                confidence_label=_coerce_confidence_label(
                    assessment.get("confidence_label"),
                    link.confidence_label,
                ),
                adjudication=assessment_metadata,
            )
        )
    return updated


def normalize_model_link_signal_type(value: Any, *, fallback: str = "other") -> str:
    normalized = str(value or "").strip().lower()
    if normalized in ENTRY_LINK_TYPES:
        return normalized
    if normalized in MODEL_LINK_TYPE_ALIASES:
        return MODEL_LINK_TYPE_ALIASES[normalized]
    fallback_normalized = str(fallback or "").strip().lower()
    return fallback_normalized if fallback_normalized in ENTRY_LINK_TYPES else "other"


def run_target_candidate_public_web_experiment(
    *,
    target_candidates: list[dict[str, Any]],
    search_provider: BaseSearchProvider,
    output_dir: str | Path,
    model_client: PublicWebModelClient | None = None,
    options: PublicWebExperimentOptions | None = None,
    run_id: str = "",
) -> dict[str, Any]:
    normalized_options = options or PublicWebExperimentOptions()
    normalized_run_id = str(run_id or "").strip() or public_web_experiment_run_id()
    experiment_dir = Path(output_dir).expanduser().resolve() / normalized_run_id
    experiment_dir.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(experiment_dir)
    started_at = utc_timestamp()
    candidate_records = [candidate_context_from_target_candidate(record) for record in target_candidates]
    manifest = {
        "run_id": normalized_run_id,
        "started_at": started_at,
        "status": "running",
        "candidate_count": len(candidate_records),
        "options": asdict(normalized_options),
        "raw_asset_policy": "Raw HTML/PDF/search payloads are analysis inputs and should not be included in export packages by default.",
        "candidates": [candidate.to_record() for candidate in candidate_records],
    }
    logger.write_json(
        "run_manifest.json",
        manifest,
        asset_type="public_web_experiment_manifest",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )

    plans = [
        prepare_candidate_search_plan(
            candidate=candidate,
            experiment_dir=experiment_dir,
            options=normalized_options,
            ordinal=index,
        )
        for index, candidate in enumerate(candidate_records, start=1)
    ]
    outcomes = execute_candidate_search_plans(
        plans=plans,
        search_provider=search_provider,
        root_logger=logger,
        options=normalized_options,
    )
    candidate_summaries = finalize_candidate_public_web_experiment_plans(
        plans=plans,
        outcomes=outcomes,
        model_client=model_client,
        options=normalized_options,
    )

    completed_at = utc_timestamp()
    summary = build_public_web_experiment_summary(
        run_id=normalized_run_id,
        started_at=started_at,
        completed_at=completed_at,
        candidate_results=candidate_summaries,
        experiment_dir=experiment_dir,
    )
    logger.write_json(
        "run_summary.json",
        summary,
        asset_type="public_web_experiment_summary",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    manifest["status"] = "completed"
    manifest["completed_at"] = completed_at
    logger.write_json(
        "run_manifest.json",
        manifest,
        asset_type="public_web_experiment_manifest",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    return summary


def finalize_candidate_public_web_experiment_plans(
    *,
    plans: list[CandidateSearchPlan],
    outcomes: dict[str, CandidateSearchOutcome],
    model_client: PublicWebModelClient | None,
    options: PublicWebExperimentOptions,
) -> list[dict[str, Any]]:
    if not plans:
        return []
    max_workers = min(
        len(plans),
        max(1, int(options.max_concurrent_candidate_analyses or 1)),
    )
    if max_workers <= 1:
        return [
            finalize_candidate_public_web_experiment(
                plan=plan,
                outcome=outcomes.get(plan.candidate.record_id) or CandidateSearchOutcome(),
                model_client=model_client,
                options=options,
            )
            for plan in plans
        ]
    summaries_by_ordinal: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="public-web-candidate") as executor:
        futures = {
            executor.submit(
                finalize_candidate_public_web_experiment,
                plan=plan,
                outcome=outcomes.get(plan.candidate.record_id) or CandidateSearchOutcome(),
                model_client=model_client,
                options=options,
            ): plan.ordinal
            for plan in plans
        }
        for future in as_completed(futures):
            summaries_by_ordinal[futures[future]] = future.result()
    return [summaries_by_ordinal[plan.ordinal] for plan in plans]


def prepare_candidate_search_plan(
    *,
    candidate: PublicWebCandidateContext,
    experiment_dir: Path,
    options: PublicWebExperimentOptions,
    ordinal: int,
) -> CandidateSearchPlan:
    safe_record_id = _safe_path_token(candidate.record_id or candidate.candidate_id or candidate.candidate_name)
    candidate_dir = experiment_dir / "candidates" / f"{ordinal:02d}_{safe_record_id}"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(candidate_dir)
    queries = plan_candidate_public_web_queries(
        candidate,
        source_families=options.source_families,
        max_queries=options.max_queries_per_candidate,
    )
    logger.write_json(
        "query_manifest.json",
        [query.to_record() for query in queries],
        asset_type="public_web_query_manifest",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    write_candidate_status(
        logger,
        candidate=candidate,
        status="queued",
        phase="search",
        query_count=len(queries),
        artifact_root=str(candidate_dir),
    )
    return CandidateSearchPlan(
        ordinal=ordinal,
        candidate=candidate,
        candidate_dir=candidate_dir,
        logger=logger,
        queries=queries,
        started_monotonic=time.monotonic(),
    )


def execute_candidate_search_plans(
    *,
    plans: list[CandidateSearchPlan],
    search_provider: BaseSearchProvider,
    root_logger: AssetLogger,
    options: PublicWebExperimentOptions,
) -> dict[str, CandidateSearchOutcome]:
    outcomes = {
        plan.candidate.record_id: CandidateSearchOutcome(search_mode="batch" if options.use_batch_search else "sequential")
        for plan in plans
    }
    for plan in plans:
        write_candidate_status(
            plan.logger,
            candidate=plan.candidate,
            status="searching",
            phase="search",
            query_count=len(plan.queries),
            artifact_root=str(plan.candidate_dir),
        )
    if options.use_batch_search:
        batch_result = execute_candidate_search_plans_batch(
            plans=plans,
            search_provider=search_provider,
            root_logger=root_logger,
            options=options,
            outcomes=outcomes,
        )
        if batch_result.get("used_batch"):
            return outcomes
    for plan in plans:
        outcome = outcomes[plan.candidate.record_id]
        outcome.search_mode = "sequential"
        execute_single_candidate_searches(
            plan=plan,
            search_provider=search_provider,
            options=options,
            outcome=outcome,
        )
    return outcomes


def execute_candidate_search_plans_batch(
    *,
    plans: list[CandidateSearchPlan],
    search_provider: BaseSearchProvider,
    root_logger: AssetLogger,
    options: PublicWebExperimentOptions,
    outcomes: dict[str, CandidateSearchOutcome],
) -> dict[str, Any]:
    query_specs: list[dict[str, Any]] = []
    task_index: dict[str, tuple[CandidateSearchPlan, PublicWebQuerySpec, int]] = {}
    for plan in plans:
        for query_index, query in enumerate(plan.queries, start=1):
            task_key = f"{plan.ordinal:02d}:{query.query_id}:{_short_hash(plan.candidate.record_id, query.query_text)}"
            query_specs.append(
                {
                    "task_key": task_key,
                    "query_text": query.query_text,
                    "max_results": max(1, int(options.max_results_per_query or 1)),
                    "metadata": {
                        "record_id": plan.candidate.record_id,
                        "candidate_id": plan.candidate.candidate_id,
                        "query_id": query.query_id,
                        "source_family": query.source_family,
                    },
                }
            )
            task_index[task_key] = (plan, query, query_index)
    if not query_specs:
        return {"used_batch": False, "reason": "no_queries"}
    try:
        submission = search_provider.submit_batch_queries(query_specs)
    except Exception as exc:
        root_logger.write_json(
            Path("search_batches") / "batch_submit_error.json",
            {"status": "failed", "error": str(exc), "query_count": len(query_specs)},
            asset_type="public_web_search_batch_error",
            source_kind="target_candidate_public_web_search",
            is_raw_asset=False,
            model_safe=True,
        )
        return {"used_batch": False, "reason": "submit_failed", "error": str(exc)}
    if submission is None:
        return {"used_batch": False, "reason": "provider_batch_unavailable"}
    write_search_execution_artifacts(
        root_logger,
        artifacts=submission.artifacts,
        prefix="submit",
    )
    root_logger.write_json(
        Path("search_batches") / "batch_submission_summary.json",
        {
            "provider_name": submission.provider_name,
            "message": submission.message,
            "task_count": len(submission.tasks),
            "submitted_at": utc_timestamp(),
        },
        asset_type="public_web_search_batch_submission_summary",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    pending_specs: dict[str, dict[str, Any]] = {}
    for task in submission.tasks:
        pending_specs[task.task_key] = {
            "task_key": task.task_key,
            "query_text": task.query_text,
            "checkpoint": dict(task.checkpoint or {}),
            "metadata": dict(task.metadata or {}),
        }
    fetched_count = 0
    poll_count = 0
    while pending_specs and poll_count < max(1, int(options.max_batch_ready_polls or 1)):
        poll_count += 1
        try:
            ready = search_provider.poll_ready_batch(list(pending_specs.values()))
        except Exception as exc:
            root_logger.write_json(
                Path("search_batches") / f"batch_ready_error_{poll_count:02d}.json",
                {"status": "failed", "error": str(exc), "pending_count": len(pending_specs), "poll_count": poll_count},
                asset_type="public_web_search_batch_error",
                source_kind="target_candidate_public_web_search",
                is_raw_asset=False,
                model_safe=True,
            )
            break
        if ready is None:
            break
        write_search_execution_artifacts(
            root_logger,
            artifacts=ready.artifacts,
            prefix=f"ready_{poll_count:02d}",
        )
        ready_specs: list[dict[str, Any]] = []
        for task in ready.tasks:
            pending = pending_specs.get(task.task_key)
            if pending is None:
                continue
            pending["checkpoint"] = dict(task.checkpoint or pending.get("checkpoint") or {})
            pending["task_id"] = task.task_id
            if bool(task.metadata.get("ready")) or str(task.checkpoint.get("status") or "") == "ready_cached":
                ready_specs.append(pending)
            else:
                pending_specs[task.task_key] = pending
        if ready_specs:
            fetched_tasks, fetch_artifacts, fetch_errors = fetch_ready_specs_isolated(
                search_provider=search_provider,
                ready_specs=ready_specs,
            )
            write_search_execution_artifacts(
                root_logger,
                artifacts=fetch_artifacts,
                prefix=f"fetch_{poll_count:02d}",
            )
            for task in fetched_tasks:
                task_key = str(task.task_key)
                pending_specs.pop(task_key, None)
                plan_query = task_index.get(task_key)
                if plan_query is None:
                    continue
                plan, query, query_index = plan_query
                outcome = outcomes[plan.candidate.record_id]
                if task.response is None:
                    outcome.errors.append(f"search_failed:{query.query_id}:empty_batch_response")
                    continue
                record_candidate_search_response(
                    plan=plan,
                    query=query,
                    query_index=query_index,
                    response=task.response,
                    outcome=outcome,
                    duration_seconds=0.0,
                    search_mode="batch_queue",
                )
                fetched_count += 1
            for task_key, error_text in fetch_errors.items():
                pending_specs.pop(task_key, None)
                plan_query = task_index.get(task_key)
                if plan_query is None:
                    continue
                plan, query, _query_index = plan_query
                outcomes[plan.candidate.record_id].errors.append(f"search_failed:{query.query_id}:{error_text[:200]}")
        if pending_specs and options.batch_ready_poll_interval_seconds > 0:
            time.sleep(float(options.batch_ready_poll_interval_seconds))
    for task_key, pending in pending_specs.items():
        plan_query = task_index.get(task_key)
        if plan_query is None:
            continue
        plan, query, _query_index = plan_query
        outcomes[plan.candidate.record_id].errors.append(
            f"search_timeout:{query.query_id}:batch task not ready after {poll_count} poll(s)"
        )
        outcomes[plan.candidate.record_id].query_results.append(
            {
                "query": query.to_record(),
                "status": "timeout",
                "search_mode": "batch_queue",
                "checkpoint": dict(pending.get("checkpoint") or {}),
            }
        )
    root_logger.write_json(
        Path("search_batches") / "batch_execution_summary.json",
        {
            "status": "completed",
            "provider_name": submission.provider_name,
            "submitted_task_count": len(submission.tasks),
            "fetched_task_count": fetched_count,
            "timed_out_task_count": len(pending_specs),
            "poll_count": poll_count,
            "completed_at": utc_timestamp(),
        },
        asset_type="public_web_search_batch_execution_summary",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    return {"used_batch": True, "fetched_task_count": fetched_count, "timed_out_task_count": len(pending_specs)}


def fetch_ready_specs_isolated(
    *,
    search_provider: BaseSearchProvider,
    ready_specs: list[dict[str, Any]],
) -> tuple[list[SearchBatchFetchTask], list[Any], dict[str, str]]:
    try:
        result = search_provider.fetch_ready_batch(ready_specs)
        if result is None:
            return [], [], {str(spec.get("task_key") or ""): "provider returned no batch fetch result" for spec in ready_specs}
        return list(result.tasks or []), list(result.artifacts or []), {}
    except Exception as exc:
        if len(ready_specs) <= 1:
            task_key = str((ready_specs[0] or {}).get("task_key") or "") if ready_specs else ""
            return [], [], {task_key: str(exc)}
    fetched_tasks: list[SearchBatchFetchTask] = []
    artifacts: list[Any] = []
    errors: dict[str, str] = {}
    for spec in ready_specs:
        task_key = str(spec.get("task_key") or "")
        try:
            result = search_provider.fetch_ready_batch([spec])
            if result is None:
                errors[task_key] = "provider returned no batch fetch result"
                continue
            fetched_tasks.extend(list(result.tasks or []))
            artifacts.extend(list(result.artifacts or []))
        except Exception as exc:
            errors[task_key] = str(exc)
    return fetched_tasks, artifacts, errors


def write_search_execution_artifacts(
    logger: AssetLogger,
    *,
    artifacts: list[Any],
    prefix: str,
) -> None:
    for index, artifact in enumerate(list(artifacts or []), start=1):
        logger.write_json(
            Path("search_batches") / f"{prefix}_{index:03d}_{_safe_path_token(str(getattr(artifact, 'label', 'artifact')))}.json",
            {
                "label": getattr(artifact, "label", ""),
                "payload": getattr(artifact, "payload", None),
                "raw_format": getattr(artifact, "raw_format", "json"),
                "content_type": getattr(artifact, "content_type", "application/json"),
                "metadata": getattr(artifact, "metadata", {}),
            },
            asset_type="public_web_search_batch_artifact",
            source_kind="target_candidate_public_web_search",
            is_raw_asset=True,
            model_safe=False,
        )


def write_candidate_status(
    logger: AssetLogger,
    *,
    candidate: PublicWebCandidateContext,
    status: str,
    phase: str,
    query_count: int,
    artifact_root: str,
    summary: dict[str, Any] | None = None,
) -> None:
    logger.write_json(
        "candidate_status.json",
        {
            "record_id": candidate.record_id,
            "candidate_id": candidate.candidate_id,
            "candidate_name": candidate.candidate_name,
            "current_company": candidate.current_company,
            "status": status,
            "phase": phase,
            "query_count": query_count,
            "artifact_root": artifact_root,
            "updated_at": utc_timestamp(),
            "summary": summary or {},
        },
        asset_type="public_web_candidate_status",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )


def execute_single_candidate_searches(
    *,
    plan: CandidateSearchPlan,
    search_provider: BaseSearchProvider,
    options: PublicWebExperimentOptions,
    outcome: CandidateSearchOutcome,
) -> None:
    for query_index, query in enumerate(plan.queries, start=1):
        search_started = time.monotonic()
        try:
            response = search_provider.search(
                query.query_text,
                max_results=max(1, int(options.max_results_per_query or 1)),
                timeout=options.timeout_seconds,
            )
        except Exception as exc:
            outcome.errors.append(f"search_failed:{query.query_id}:{str(exc)[:200]}")
            outcome.query_results.append({"query": query.to_record(), "status": "failed", "error": str(exc)})
            continue
        record_candidate_search_response(
            plan=plan,
            query=query,
            query_index=query_index,
            response=response,
            outcome=outcome,
            duration_seconds=round(time.monotonic() - search_started, 3),
            search_mode="sequential",
        )


def record_candidate_search_response(
    *,
    plan: CandidateSearchPlan,
    query: PublicWebQuerySpec,
    query_index: int,
    response: SearchResponse,
    outcome: CandidateSearchOutcome,
    duration_seconds: float,
    search_mode: str,
) -> None:
    raw_search_path = plan.logger.write_json(
        Path("search") / f"{query_index:02d}_{query.source_family}.json",
        _search_response_record(response),
        asset_type="public_web_search_response",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=True,
        model_safe=False,
        metadata={"query_id": query.query_id, "query_text": query.query_text, "search_mode": search_mode},
    )
    classified_for_query: list[dict[str, Any]] = []
    for rank, item in enumerate(response.results, start=1):
        classified = classify_public_web_url(
            url=item.url,
            title=item.title,
            snippet=item.snippet,
            candidate=plan.candidate,
            query=query,
            provider_name=response.provider_name,
            result_rank=rank,
        )
        if classified is None:
            continue
        outcome.raw_links.append(classified)
        classified_for_query.append(classified.to_record())
    outcome.query_results.append(
        {
            "query": query.to_record(),
            "status": "completed",
            "provider_name": response.provider_name,
            "raw_path": str(raw_search_path),
            "result_count": len(response.results),
            "classified_entry_links": classified_for_query,
            "duration_seconds": duration_seconds,
            "search_mode": search_mode,
        }
    )


def run_single_candidate_public_web_experiment(
    *,
    candidate: PublicWebCandidateContext,
    search_provider: BaseSearchProvider,
    model_client: PublicWebModelClient | None,
    root_logger: AssetLogger,
    experiment_dir: Path,
    options: PublicWebExperimentOptions,
    ordinal: int,
) -> dict[str, Any]:
    del root_logger
    plan = prepare_candidate_search_plan(
        candidate=candidate,
        experiment_dir=experiment_dir,
        options=options,
        ordinal=ordinal,
    )
    outcome = CandidateSearchOutcome(search_mode="sequential")
    execute_single_candidate_searches(
        plan=plan,
        search_provider=search_provider,
        options=options,
        outcome=outcome,
    )
    return finalize_candidate_public_web_experiment(
        plan=plan,
        outcome=outcome,
        model_client=model_client,
        options=options,
    )


def classify_entry_links_from_document_signals(
    *,
    signals: dict[str, Any],
    candidate: PublicWebCandidateContext,
    source_url: str,
) -> list[ClassifiedEntryLink]:
    source_url = normalize_public_web_url(source_url)
    specs = [
        ("resume_urls", "resume_and_documents", "CV/resume link discovered while fetching public web evidence."),
        ("github_urls", "technical_presence", "GitHub link discovered while fetching public web evidence."),
        ("x_urls", "social_presence", "X/Twitter link discovered while fetching public web evidence."),
        ("linkedin_urls", "profile_web_presence", "LinkedIn link discovered while fetching public web evidence."),
        ("personal_urls", "profile_web_presence", "Personal/homepage link discovered while fetching public web evidence."),
    ]
    discovered: list[ClassifiedEntryLink] = []
    for signal_key, source_family, objective in specs:
        query = PublicWebQuerySpec(
            query_id=f"doc:{signal_key}",
            source_family=source_family,
            query_text=source_url,
            objective=objective,
        )
        for value in list(signals.get(signal_key) or []):
            url = str(value or "").strip()
            if not url or url == source_url:
                continue
            classified = classify_public_web_url(
                url=url,
                title="",
                snippet=f"Discovered from {source_url}",
                candidate=candidate,
                query=query,
                provider_name="document_extraction",
                result_rank=0,
            )
            if classified is not None:
                discovered.append(classified)
    return rank_entry_links(discovered, limit=20)


def fetch_candidate_public_web_documents(
    *,
    ranked_links: list[ClassifiedEntryLink],
    candidate: PublicWebCandidateContext,
    candidate_dir: Path,
    logger: AssetLogger,
    model_client: PublicWebModelClient | None,
    options: PublicWebExperimentOptions,
) -> dict[str, Any]:
    max_fetches = max(0, int(options.max_fetches_per_candidate))
    if max_fetches <= 0:
        return {
            "fetched_documents": [],
            "gathered_signals": empty_signal_bundle(),
            "email_candidates": [],
            "discovered_entry_links": [],
            "errors": [],
        }
    fetch_queue = build_diversified_fetch_queue(ranked_links, max_fetches=max_fetches)
    fetched_url_keys: set[str] = set()
    known_entry_link_keys = {
        key for link in ranked_links if (key := normalize_public_web_url_key(link.normalized_url))
    }
    fetch_index = 0
    max_workers = min(
        max_fetches,
        max(1, int(options.max_concurrent_fetches_per_candidate or 1)),
    )
    fetched_by_index: dict[int, dict[str, Any]] = {}
    gathered_signals = empty_signal_bundle()
    email_candidates: list[EmailCandidateSignal] = []
    discovered_entry_links: list[ClassifiedEntryLink] = []
    errors: list[str] = []

    def submit_next(executor: ThreadPoolExecutor, in_flight: dict[Any, PublicWebFetchResult | tuple[int, ClassifiedEntryLink]]) -> None:
        nonlocal fetch_index
        while fetch_queue and fetch_index < max_fetches and len(in_flight) < max_workers:
            link = fetch_queue.pop(0)
            link_key = normalize_public_web_url_key(link.normalized_url)
            if not link_key or link_key in fetched_url_keys:
                continue
            fetched_url_keys.add(link_key)
            fetch_index += 1
            future = executor.submit(
                fetch_candidate_public_web_document,
                candidate=candidate,
                candidate_dir=candidate_dir,
                logger=logger,
                model_client=model_client,
                options=options,
                link=link,
                fetch_index=fetch_index,
            )
            in_flight[future] = (fetch_index, link)

    def consume_result(result: PublicWebFetchResult) -> None:
        fetched_by_index[result.fetch_index] = result.document_record
        if result.error:
            errors.append(f"fetch_failed:{result.link.normalized_url}:{result.error[:200]}")
            return
        merge_signal_bundle(gathered_signals, result.signals)
        email_candidates.extend(result.email_candidates)
        source_url = str(result.document_record.get("final_url") or result.link.normalized_url)
        for discovered_link in result.discovered_entry_links:
            discovered_key = normalize_public_web_url_key(discovered_link.normalized_url)
            if not discovered_key or discovered_key in known_entry_link_keys:
                continue
            known_entry_link_keys.add(discovered_key)
            discovered_entry_links.append(discovered_link)
            if discovered_link.fetchable and discovered_key not in fetched_url_keys:
                if should_prioritize_discovered_fetch_link(discovered_link, source_url=source_url):
                    fetch_queue.insert(0, discovered_link)
                else:
                    fetch_queue.append(discovered_link)

    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="public-web-fetch") as executor:
        in_flight: dict[Any, tuple[int, ClassifiedEntryLink]] = {}
        submit_next(executor, in_flight)
        while in_flight:
            completed, _pending = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
            for future in completed:
                _fetch_index, link = in_flight.pop(future)
                try:
                    consume_result(future.result())
                except Exception as exc:  # pragma: no cover - defensive wrapper around fetch worker
                    errors.append(f"fetch_failed:{link.normalized_url}:{str(exc)[:200]}")
                    fetched_by_index[_fetch_index] = {
                        "source_url": link.normalized_url,
                        "entry_type": link.entry_type,
                        "source_family": link.source_family,
                        "status": "failed",
                        "error": str(exc),
                    }
            submit_next(executor, in_flight)

    return {
        "fetched_documents": [fetched_by_index[index] for index in sorted(fetched_by_index)],
        "gathered_signals": gathered_signals,
        "email_candidates": email_candidates,
        "discovered_entry_links": discovered_entry_links,
        "errors": errors,
    }


def fetch_candidate_public_web_document(
    *,
    candidate: PublicWebCandidateContext,
    candidate_dir: Path,
    logger: AssetLogger,
    model_client: PublicWebModelClient | None,
    options: PublicWebExperimentOptions,
    link: ClassifiedEntryLink,
    fetch_index: int,
) -> PublicWebFetchResult:
    try:
        analyzed = analyze_remote_document(
            candidate=candidate_from_context(candidate),
            target_company=candidate.current_company,
            source_url=link.normalized_url,
            asset_dir=candidate_dir / "documents",
            asset_logger=logger,
            model_client=model_client,  # type: ignore[arg-type]
            source_kind="target_candidate_public_web_search",
            asset_prefix=f"doc_{fetch_index:02d}_{link.entry_type}",
            timeout=options.timeout_seconds,
            analyze_with_model=False,
            document_type_hint=link.entry_type,
        )
        document_record = {
            "source_url": analyzed.source_url,
            "final_url": analyzed.final_url,
            "entry_type": link.entry_type,
            "source_family": link.source_family,
            "document_type": analyzed.document_type,
            "content_type": analyzed.content_type,
            "raw_path": analyzed.raw_path,
            "extracted_text_path": analyzed.extracted_text_path,
            "analysis_input_path": analyzed.analysis_input_path,
            "analysis_path": analyzed.analysis_path,
            "evidence_slice_path": analyzed.evidence_slice_path,
            "title": analyzed.title,
            "signals": analyzed.signals,
            "analysis": analyzed.analysis,
            "evidence_slice": analyzed.evidence_slice,
        }
        extracted_emails: list[EmailCandidateSignal] = []
        if options.extract_contact_signals:
            text = str((analyzed.evidence_slice or {}).get("selected_text") or "").strip()
            if not text:
                text = _read_document_text_for_email_extraction(
                    raw_path=analyzed.raw_path,
                    extracted_text_path=analyzed.extracted_text_path,
                )
            extracted_emails = extract_email_candidate_signals(
                text=text,
                source_url=analyzed.final_url or link.normalized_url,
                source_family=link.source_family,
                source_title=analyzed.title or link.title,
                candidate=candidate,
            )
            document_record["email_candidates"] = [item.to_record() for item in extracted_emails]
        discovered_entry_links = classify_entry_links_from_document_signals(
            signals=analyzed.signals,
            candidate=candidate,
            source_url=analyzed.final_url or link.normalized_url,
        )
        return PublicWebFetchResult(
            fetch_index=fetch_index,
            link=link,
            document_record=document_record,
            signals=analyzed.signals,
            email_candidates=extracted_emails,
            discovered_entry_links=discovered_entry_links,
        )
    except Exception as exc:
        return PublicWebFetchResult(
            fetch_index=fetch_index,
            link=link,
            document_record={
                "source_url": link.normalized_url,
                "entry_type": link.entry_type,
                "source_family": link.source_family,
                "status": "failed",
                "error": str(exc),
            },
            error=str(exc),
        )


def finalize_candidate_public_web_experiment(
    *,
    plan: CandidateSearchPlan,
    outcome: CandidateSearchOutcome,
    model_client: PublicWebModelClient | None,
    options: PublicWebExperimentOptions,
) -> dict[str, Any]:
    candidate = plan.candidate
    logger = plan.logger
    candidate_dir = plan.candidate_dir
    write_candidate_status(
        logger,
        candidate=candidate,
        status="analyzing",
        phase="analysis",
        query_count=len(plan.queries),
        artifact_root=str(candidate_dir),
    )
    ranked_links = rank_entry_links(outcome.raw_links, limit=options.max_entry_links_per_candidate)
    logger.write_json(
        "entry_links.json",
        [link.to_record() for link in ranked_links],
        asset_type="public_web_entry_links",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )

    fetched_documents: list[dict[str, Any]] = []
    gathered_signals = empty_signal_bundle()
    email_candidates: list[EmailCandidateSignal] = []
    discovered_entry_links: list[ClassifiedEntryLink] = []
    if options.fetch_content:
        fetch_result = fetch_candidate_public_web_documents(
            ranked_links=ranked_links,
            candidate=candidate,
            candidate_dir=candidate_dir,
            logger=logger,
            model_client=model_client,
            options=options,
        )
        fetched_documents = fetch_result["fetched_documents"]
        gathered_signals = fetch_result["gathered_signals"]
        email_candidates = fetch_result["email_candidates"]
        discovered_entry_links = fetch_result["discovered_entry_links"]
        outcome.errors.extend(fetch_result["errors"])

    deduped_email_candidates = _dedupe_email_signals(email_candidates)
    if discovered_entry_links:
        ranked_links = rank_entry_links(
            [*ranked_links, *discovered_entry_links],
            limit=options.max_entry_links_per_candidate,
        )
        logger.write_json(
            "entry_links.json",
            [link.to_record() for link in ranked_links],
            asset_type="public_web_entry_links",
            source_kind="target_candidate_public_web_search",
            is_raw_asset=False,
            model_safe=True,
        )
    adjudicated_email_candidates, adjudicated_links, ai_result = adjudicate_public_web_candidate_evidence(
        candidate=candidate,
        email_candidates=deduped_email_candidates,
        entry_links=ranked_links,
        fetched_documents=fetched_documents,
        model_client=model_client,
        ai_extraction=options.ai_extraction,
        max_ai_evidence_documents=options.max_ai_evidence_documents,
    )
    signals_payload = {
        "candidate": candidate.to_record(),
        "entry_links": [link.to_record() for link in adjudicated_links],
        "fetched_documents": fetched_documents,
        "email_candidates": [item.to_record() for item in adjudicated_email_candidates],
        "gathered_signals": gathered_signals,
        "ai_adjudication": ai_result,
        "errors": outcome.errors,
    }
    logger.write_json(
        "signals.json",
        signals_payload,
        asset_type="public_web_candidate_signals",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    summary = {
        "record_id": candidate.record_id,
        "candidate_id": candidate.candidate_id,
        "candidate_name": candidate.candidate_name,
        "current_company": candidate.current_company,
        "linkedin_url_key": candidate.linkedin_url_key,
        "status": "completed" if not outcome.errors else "completed_with_errors",
        "search_mode": outcome.search_mode,
        "query_count": len(plan.queries),
        "entry_link_count": len(adjudicated_links),
        "fetchable_entry_link_count": len([link for link in adjudicated_links if link.fetchable]),
        "fetched_document_count": len([item for item in fetched_documents if not item.get("error")]),
        "email_candidate_count": len(adjudicated_email_candidates),
        "promotion_recommended_email_count": len(
            [item for item in adjudicated_email_candidates if item.promotion_status == "promotion_recommended"]
        ),
        "entry_link_type_counts": _count_by([link.entry_type for link in adjudicated_links]),
        "source_family_counts": _count_by([link.source_family for link in adjudicated_links]),
        "link_identity_counts": _count_by([link.identity_match_label for link in adjudicated_links]),
        "primary_links": _primary_links(adjudicated_links, gathered_signals),
        "artifact_root": str(candidate_dir),
        "duration_seconds": round(time.monotonic() - plan.started_monotonic, 3),
        "errors": outcome.errors,
    }
    logger.write_json(
        "candidate_summary.json",
        summary,
        asset_type="public_web_candidate_summary",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    logger.write_json(
        "search_results.json",
        outcome.query_results,
        asset_type="public_web_candidate_search_results",
        source_kind="target_candidate_public_web_search",
        is_raw_asset=False,
        model_safe=True,
    )
    write_candidate_status(
        logger,
        candidate=candidate,
        status=summary["status"],
        phase="completed",
        query_count=len(plan.queries),
        artifact_root=str(candidate_dir),
        summary=summary,
    )
    return summary


def build_public_web_experiment_summary(
    *,
    run_id: str,
    started_at: str,
    completed_at: str,
    candidate_results: list[dict[str, Any]],
    experiment_dir: Path,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "status": "completed",
        "started_at": started_at,
        "completed_at": completed_at,
        "artifact_root": str(experiment_dir),
        "candidate_count": len(candidate_results),
        "completed_count": len([item for item in candidate_results if str(item.get("status") or "").startswith("completed")]),
        "completed_with_errors_count": len(
            [item for item in candidate_results if str(item.get("status") or "") == "completed_with_errors"]
        ),
        "query_count": sum(int(item.get("query_count") or 0) for item in candidate_results),
        "entry_link_count": sum(int(item.get("entry_link_count") or 0) for item in candidate_results),
        "fetched_document_count": sum(int(item.get("fetched_document_count") or 0) for item in candidate_results),
        "email_candidate_count": sum(int(item.get("email_candidate_count") or 0) for item in candidate_results),
        "promotion_recommended_email_count": sum(
            int(item.get("promotion_recommended_email_count") or 0) for item in candidate_results
        ),
        "candidate_summaries": candidate_results,
    }


def normalize_public_web_url(url: str) -> str:
    raw = str(url or "").strip()
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = f"https:{raw}"
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", raw):
        raw = f"https://{raw}"
    parsed = parse.urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    fragmentless = parsed._replace(fragment="")
    return parse.urlunparse(fragmentless)


def normalize_public_web_url_key(url: str) -> str:
    normalized = normalize_public_web_url(url)
    if not normalized:
        return ""
    parsed = parse.urlparse(normalized)
    query_pairs = [
        (key, value)
        for key, value in parse.parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_") and key.lower() not in {"fbclid", "gclid"}
    ]
    cleaned = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower().removeprefix("www."),
        path=parsed.path.rstrip("/") or "/",
        query=parse.urlencode(query_pairs),
        fragment="",
    )
    return parse.urlunparse(cleaned)


def load_target_candidates_from_json(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        records = payload.get("target_candidates") or payload.get("candidates") or []
        return [dict(item) for item in list(records) if isinstance(item, dict)]
    return []


def _candidate_metadata_from_target_candidate(record: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(record.get("metadata") or {}) if isinstance(record.get("metadata"), dict) else {}
    candidate_context = {}
    if isinstance(record.get("candidate"), dict):
        candidate_context.update(dict(record.get("candidate") or {}))
    if isinstance(metadata.get("candidate"), dict):
        candidate_context.update(dict(metadata.get("candidate") or {}))

    for key in [
        "education",
        "education_summary",
        "education_lines",
        "work_history",
        "experience",
        "experience_lines",
        "role",
        "title",
        "organization",
        "profile_url",
        "linkedin_url",
        "current_company",
        "headline",
        "focus_areas",
        "team",
        "notes",
    ]:
        value = record.get(key)
        if value in (None, "", [], {}, ()):
            value = metadata.get(key)
        if value not in (None, "", [], {}, ()) and key not in candidate_context:
            candidate_context[key] = value
        if value not in (None, "", [], {}, ()) and key not in metadata:
            metadata[key] = value

    if candidate_context:
        metadata["candidate"] = candidate_context
    return metadata


def _search_response_record(response: SearchResponse) -> dict[str, Any]:
    return {
        "provider_name": response.provider_name,
        "query_text": response.query_text,
        "results": [item.to_record() for item in response.results],
        "raw_payload": response.raw_payload,
        "raw_format": response.raw_format,
        "final_url": response.final_url,
        "content_type": response.content_type,
        "metadata": response.metadata,
    }


def _quote_search_term(value: str) -> str:
    normalized = " ".join(str(value or "").split())
    if not normalized:
        return ""
    escaped = normalized.replace('"', "")
    return f'"{escaped}"'


def _normalize_query_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value not in (None, "", [], {}, ()):
            return value
    return ""


def _query_key(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower().replace("-", " ").replace("_", " "))


def _short_hash(*parts: str) -> str:
    return sha1("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:16]


def _safe_path_token(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "_", str(value or "").strip())[:80].strip("._")
    return cleaned or "candidate"


def _looks_like_resume_url(lower_url: str, combined: str) -> bool:
    return any(
        token in lower_url or token in combined
        for token in [
            "resume",
            "curriculum-vitae",
            "curriculum vitae",
            "/cv",
            " cv ",
            "cv.pdf",
            "vita.pdf",
        ]
    )


def _looks_like_academic_profile(domain: str, lower_url: str, combined: str) -> bool:
    academic_domain = ".edu" in domain or ".ac." in domain or domain.endswith(".edu")
    profile_hint = any(token in lower_url for token in ["/people", "/person", "/profile", "/faculty", "/~"])
    return academic_domain and (profile_hint or any(token in combined for token in ["professor", "student", "university"]))


def _looks_like_personal_homepage(
    normalized_url: str,
    combined: str,
    candidate: PublicWebCandidateContext | None,
    title: str = "",
) -> bool:
    parsed = parse.urlparse(normalized_url)
    domain = parsed.netloc.lower().removeprefix("www.")
    if _is_low_value_fetch_domain(domain) or domain in PUBLICATION_DOMAINS:
        return False
    path_parts = [part for part in parsed.path.strip("/").split("/") if part]
    if "~" in parsed.path:
        return True
    if candidate and candidate.candidate_name:
        name_tokens = _person_name_tokens(candidate.candidate_name)
        url_text = re.sub(r"[^a-z0-9]+", " ", f"{domain} {parsed.path}".lower())
        if name_tokens and sum(1 for token in name_tokens[:2] if token in url_text) >= min(2, len(name_tokens)):
            return True
        title_like_match = _name_tokens_match(candidate.candidate_name.lower(), str(title or "").lower())
        return bool(title_like_match and len(path_parts) <= 1 and _looks_like_individual_domain(domain, candidate))
    return len(path_parts) <= 1 and _looks_like_individual_domain(domain, candidate)


def _looks_like_company_page(domain: str, lower_url: str, combined: str, candidate: PublicWebCandidateContext) -> bool:
    company = str(candidate.current_company or "").strip().lower()
    if not company:
        return False
    company_tokens = [token for token in re.findall(r"[a-z0-9]+", company) if len(token) > 1]
    if not company_tokens:
        return False
    compact_domain = re.sub(r"[^a-z0-9]+", "", domain)
    compact_company = "".join(company_tokens)
    if compact_company and compact_company in compact_domain:
        return True
    if any(token in compact_domain for token in company_tokens if len(token) >= 5):
        return True
    del lower_url, combined
    return False


def _looks_like_individual_domain(domain: str, candidate: PublicWebCandidateContext | None) -> bool:
    if _is_low_value_fetch_domain(domain):
        return False
    generic_tokens = {
        "academy",
        "blog",
        "careers",
        "company",
        "docs",
        "events",
        "forum",
        "jobs",
        "labs",
        "news",
        "research",
        "support",
        "team",
        "www",
    }
    domain_tokens = [token for token in re.findall(r"[a-z0-9]+", domain) if token not in generic_tokens]
    if candidate:
        company_tokens = set(_person_name_tokens(candidate.current_company))
        if company_tokens and any(token in company_tokens for token in domain_tokens):
            return False
    return bool(domain_tokens and len(domain_tokens) <= 3)


def _is_low_value_fetch_domain(domain: str) -> bool:
    normalized = str(domain or "").strip().lower().removeprefix("www.")
    return any(normalized == root or normalized.endswith(f".{root}") for root in LOW_VALUE_FETCH_DOMAINS)


def _looks_like_x_search_or_utility_url(path: str, query: str) -> bool:
    normalized_path = "/" + str(path or "").strip().lower().lstrip("/")
    if normalized_path.startswith(("/search", "/intent", "/share", "/hashtag", "/i/")):
        return True
    query_pairs = {key.lower(): value for key, value in parse.parse_qsl(str(query or ""), keep_blank_values=True)}
    return "q" in query_pairs and normalized_path in {"", "/"}


def _name_tokens_match(name: str, text: str) -> bool:
    tokens = _person_name_tokens(name)
    if not tokens:
        return False
    normalized_text = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return all(token in normalized_text for token in tokens[:2])


def _person_name_tokens(name: str) -> list[str]:
    return [token for token in re.findall(r"[a-z0-9]+", str(name or "").lower()) if len(token) > 1][:4]


_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)


def deobfuscate_email_text(text: str) -> str:
    value = unescape(str(text or ""))
    replacements = [
        (r"\s*\[\s*at\s*\]\s*", "@"),
        (r"\s*\(\s*at\s*\)\s*", "@"),
        (r"\s+\bat\b\s+", "@"),
        (r"\s*\[\s*dot\s*\]\s*", "."),
        (r"\s*\(\s*dot\s*\)\s*", "."),
        (r"\s+\bdot\b\s+", "."),
    ]
    for pattern, replacement in replacements:
        value = re.sub(pattern, replacement, value, flags=re.IGNORECASE)
    return expand_group_email_patterns(value)


def expand_group_email_patterns(text: str) -> str:
    value = str(text or "")

    def _replace(match: re.Match[str]) -> str:
        local_parts = re.split(r"[,;/\s]+", match.group(1))
        domain = str(match.group(2) or "").strip()
        emails = []
        for local_part in local_parts:
            cleaned = str(local_part or "").strip().strip("'\"`")
            if cleaned and re.fullmatch(r"[A-Za-z0-9._%+\-]+", cleaned):
                emails.append(f"{cleaned}@{domain}")
        return " ".join(emails) if emails else match.group(0)

    return re.sub(
        r"\{\s*([^{}@]{1,160}?)\s*\}\s*@\s*([A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        _replace,
        value,
    )


def normalize_email(value: str) -> str:
    normalized = str(value or "").strip().strip(".,;:()[]{}<>\"'").lower()
    if not _EMAIL_RE.fullmatch(normalized):
        return ""
    return normalized


def infer_email_type(
    email: str,
    *,
    source_domain: str = "",
    candidate: PublicWebCandidateContext | None = None,
) -> str:
    local_part, _, domain = email.partition("@")
    del local_part
    domain = domain.lower()
    if any(hint in domain for hint in ACADEMIC_EMAIL_DOMAIN_HINTS):
        return "academic"
    if domain in {"gmail.com", "outlook.com", "hotmail.com", "icloud.com", "me.com", "proton.me", "protonmail.com"}:
        return "personal"
    if candidate and candidate.current_company:
        company_tokens = _person_name_tokens(candidate.current_company)
        if company_tokens and any(token in domain.replace("-", "").replace(".", "") for token in company_tokens):
            return "company"
    if source_domain and domain and (domain == source_domain or source_domain.endswith(domain) or domain.endswith(source_domain)):
        return "company"
    if domain:
        return "unknown"
    return "generic"


def confidence_label_from_score(score: float) -> str:
    if score >= 0.72:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def _is_noise_email(email: str, *, raw_value: str = "") -> bool:
    local_part, _, domain = email.partition("@")
    if not local_part or not domain:
        return True
    if domain.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")):
        return True
    if domain in {"example.com", "email.com", "domain.com"}:
        return True
    tld = domain.rsplit(".", 1)[-1]
    if tld in {"conference", "robotics", "workshop", "symposium"}:
        return True
    raw = str(raw_value or "").strip()
    if raw and re.search(r"[A-Z][A-Za-z]+@[A-Z][A-Za-z]+\.[A-Z][A-Za-z]+", raw):
        return True
    return False


def _is_verified_domain_placeholder_email(email: str, text: str) -> bool:
    local_part, _, domain = email.partition("@")
    if local_part != "email" or not domain:
        return False
    escaped_domain = re.escape(domain)
    return bool(
        re.search(
            rf"verified\s+email\s*@\s*{escaped_domain}|verified\s+email\s+at\s+{escaped_domain}",
            str(text or ""),
            flags=re.IGNORECASE,
        )
    )


def _email_excerpt(text: str, email: str, *, window: int = 180) -> str:
    lower_text = text.lower()
    index = lower_text.find(email.lower())
    if index < 0:
        return ""
    start = max(0, index - window)
    end = min(len(text), index + len(email) + window)
    return " ".join(text[start:end].split())[:500]


def _dedupe_email_signals(signals: list[EmailCandidateSignal]) -> list[EmailCandidateSignal]:
    best: dict[str, EmailCandidateSignal] = {}
    order: list[str] = []
    for signal in signals:
        key = signal.normalized_value
        if not key:
            continue
        if key not in best:
            order.append(key)
            best[key] = signal
            continue
        if signal.confidence_score > best[key].confidence_score:
            best[key] = signal
    return [best[key] for key in order]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            deduped.append(normalized)
    return deduped


def _read_document_text_for_email_extraction(*, raw_path: str, extracted_text_path: str = "") -> str:
    if str(extracted_text_path or "").strip():
        return _safe_read_text(extracted_text_path)
    return _safe_read_text(raw_path)


def _safe_read_text(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _coerce_confidence_label(value: Any, fallback: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"high", "medium", "low"}:
        return normalized
    return fallback


def _coerce_confidence_score(value: Any, fallback: float) -> float:
    try:
        return round(max(0.0, min(float(value), 1.0)), 2)
    except (TypeError, ValueError):
        return round(float(fallback or 0.0), 2)


def _count_by(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "").strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _primary_links(links: list[ClassifiedEntryLink], signals: dict[str, Any]) -> dict[str, str]:
    primary: dict[str, str] = {}
    promotable_identity_labels = {"confirmed", "likely_same_person"}
    for entry_type in ENTRY_LINK_TYPES:
        if entry_type == "other":
            continue
        match = next(
            (
                link.normalized_url
                for link in links
                if link.entry_type == entry_type
                and link.identity_match_label in promotable_identity_labels
                and _is_primary_link_candidate(link)
            ),
            "",
        )
        if match:
            primary[entry_type] = match
    del signals
    return primary


def _is_primary_link_candidate(link: ClassifiedEntryLink) -> bool:
    return is_clean_profile_link(link.entry_type, link.normalized_url)


def build_sample_target_candidate_records(limit: int = 10) -> list[dict[str, Any]]:
    names = [
        ("Mira Murati", "OpenAI", "Former CTO"),
        ("Ilya Sutskever", "Safe Superintelligence", "Co-Founder"),
        ("Noam Brown", "OpenAI", "Research Scientist"),
        ("Lilian Weng", "OpenAI", "Research"),
        ("Sholto Douglas", "Anthropic", "Research"),
        ("Chelsea Finn", "Stanford University", "Assistant Professor"),
        ("Percy Liang", "Stanford University", "Professor"),
        ("Pieter Abbeel", "UC Berkeley", "Professor"),
        ("Andrej Karpathy", "Eureka Labs", "Founder"),
        ("Jim Fan", "NVIDIA", "Research Scientist"),
    ]
    records: list[dict[str, Any]] = []
    for index, (name, company, headline) in enumerate(names[: max(1, int(limit or 1))], start=1):
        records.append(
            {
                "id": f"sample-public-web::{index:02d}",
                "candidate_id": f"sample-public-web-{index:02d}",
                "candidate_name": name,
                "current_company": company,
                "headline": headline,
                "linkedin_url": "",
                "metadata": {"source": "public_web_experiment_sample"},
            }
        )
    return records


def _search_item_from_record(record: dict[str, Any]) -> SearchResultItem:
    return SearchResultItem(
        title=str(record.get("title") or ""),
        url=str(record.get("url") or ""),
        snippet=str(record.get("snippet") or record.get("description") or ""),
        metadata=dict(record.get("metadata") or {}),
    )
