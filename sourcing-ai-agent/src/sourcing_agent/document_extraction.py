from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from html import unescape
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib import parse

from .asset_logger import AssetLogger
from .asset_policy import DEFAULT_MODEL_CONTEXT_CHAR_LIMIT, RAW_ASSET_POLICY_SUMMARY
from .domain import Candidate
from .web_fetch import fetch_binary_url, fetch_text_url

if TYPE_CHECKING:
    from .model_provider import ModelClient


_VENDOR_DIR = Path(__file__).resolve().parents[1] / "_vendor"
_LOCAL_VENDOR_DIR = Path(__file__).resolve().parents[2] / "runtime" / "vendor" / "python"
for candidate_dir in (_LOCAL_VENDOR_DIR, _VENDOR_DIR):
    if candidate_dir.exists():
        vendor_path = str(candidate_dir)
        if vendor_path not in sys.path:
            sys.path.insert(0, vendor_path)

try:  # pragma: no cover - exercised in live/runtime paths
    from pypdf import PdfReader
except Exception:  # pragma: no cover - graceful fallback when vendored dep is unavailable
    PdfReader = None

try:  # pragma: no cover - exercised in live/runtime paths
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:  # pragma: no cover - graceful fallback when optional dep is unavailable
    pdfminer_extract_text = None


STRUCTURED_SIGNAL_KEYS = ("education_signals", "work_history_signals", "affiliation_signals")
DEFAULT_EVIDENCE_SLICE_MAX_CHARS = 4200
SOURCE_EVIDENCE_CHAR_LIMITS = {
    "github_profile": 2600,
    "google_scholar_profile": 2600,
    "x_profile": 1800,
    "substack_profile": 2400,
    "publication_pdf": 3600,
    "resume_or_cv": 4200,
    "academic_profile": 3600,
    "personal_homepage": 5600,
    "web_page": 3000,
}
PUBLICATION_DOCUMENT_DOMAINS = {
    "arxiv.org",
    "openreview.net",
    "semanticscholar.org",
    "aclanthology.org",
    "papers.nips.cc",
    "proceedings.mlr.press",
    "dl.acm.org",
    "ieeexplore.ieee.org",
    "researchgate.net",
}
_EVIDENCE_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b", re.IGNORECASE)


@dataclass(slots=True)
class AnalyzedDocumentAsset:
    source_url: str
    final_url: str
    content_type: str
    document_type: str
    raw_path: str
    extracted_text_path: str
    analysis_input_path: str
    analysis_path: str
    signals: dict[str, Any]
    analysis: dict[str, Any]
    title: str = ""
    description: str = ""
    evidence_slice_path: str = ""
    evidence_slice: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PdfExtractionResult:
    text: str
    method: str
    attempts: list[dict[str, Any]]


def empty_signal_bundle() -> dict[str, Any]:
    return {
        "linkedin_urls": [],
        "x_urls": [],
        "github_urls": [],
        "personal_urls": [],
        "resume_urls": [],
        "descriptions": [],
        "validated_summaries": [],
        "role_signals": [],
        "analysis_notes": [],
        "education_signals": [],
        "work_history_signals": [],
        "affiliation_signals": [],
        "document_types": [],
        "research_interests": [],
        "scholar_verified_domains": [],
        "scholar_citation_metrics": [],
        "scholar_publications": [],
        "scholar_coauthors": [],
    }


def extract_page_signals(html_text: str, base_url: str) -> dict[str, Any]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    title = " ".join(unescape(title_match.group(1)).split()) if title_match else ""
    meta_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        flags=re.IGNORECASE,
    )
    if not meta_match:
        meta_match = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            html_text,
            flags=re.IGNORECASE,
        )
    description = " ".join(unescape(meta_match.group(1)).split()) if meta_match else ""
    links = re.findall(r'href=["\']([^"\']+)["\']', html_text, flags=re.IGNORECASE)
    absolute_links = [_normalize_url(parse.urljoin(base_url, link)) for link in links]
    linkedin_urls = _dedupe([link for link in absolute_links if "linkedin.com/in/" in link])
    x_urls = _dedupe([link for link in absolute_links if "x.com/" in link or "twitter.com/" in link])
    github_urls = _dedupe([link for link in absolute_links if "github.com/" in link and _is_meaningful_github_url(link)])
    resume_urls = _dedupe(
        [
            link
            for link in absolute_links
            if link.endswith(".pdf") or any(token in link.lower() for token in ["/cv", "/resume", "resume", "curriculum-vitae"])
        ]
    )
    personal_urls = _dedupe(
        [
            link
            for link in absolute_links
            if link.startswith("http")
            and "linkedin.com" not in link
            and "x.com" not in link
            and "twitter.com" not in link
            and "github.com" not in link
            and "duckduckgo.com" not in link
            and _is_meaningful_personal_url(link)
        ]
    )
    descriptions = _dedupe([item for item in [title, description] if item])
    bundle = empty_signal_bundle()
    bundle.update(
        {
            "linkedin_urls": linkedin_urls,
            "x_urls": x_urls,
            "github_urls": github_urls,
            "personal_urls": personal_urls,
            "resume_urls": resume_urls,
            "descriptions": descriptions,
        }
    )
    if _is_google_scholar_profile_url(base_url):
        scholar_signals = extract_google_scholar_profile_signals(html_text, base_url)
        bundle["personal_urls"] = list(scholar_signals.get("personal_urls") or [])
        merge_signal_bundle(
            bundle,
            {key: value for key, value in scholar_signals.items() if key != "personal_urls"},
        )
    return bundle


def extract_google_scholar_profile_signals(html_text: str, base_url: str) -> dict[str, Any]:
    bundle = empty_signal_bundle()
    profile_block_match = re.search(
        r'<div id="gsc_prf_i">(.*?)</div></div></div><div id="gsc_prf_t_wrp"',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    profile_block = profile_block_match.group(1) if profile_block_match else html_text
    homepage_match = re.search(
        r'id=["\']gsc_prf_ivh["\'][^>]*>.*?<a[^>]+href=["\']([^"\']+)["\'][^>]*>\s*Homepage\s*</a>',
        profile_block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if homepage_match:
        homepage_url = parse.urljoin(base_url, unescape(homepage_match.group(1)))
        if _is_meaningful_personal_url(homepage_url):
            bundle["personal_urls"].append(homepage_url)
    verified_domain_match = re.search(
        r"Verified\s+email\s+at\s+([A-Za-z0-9.\-]+\.[A-Za-z]{2,})",
        profile_block,
        flags=re.IGNORECASE,
    )
    if verified_domain_match:
        bundle["scholar_verified_domains"].append(verified_domain_match.group(1).lower())
    organization_match = re.search(
        r'<div class=["\']gsc_prf_il["\']>\s*<a[^>]*class=["\']gsc_prf_ila["\'][^>]*>(.*?)</a>\s*</div>',
        profile_block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if organization_match:
        organization = _strip_html_text(organization_match.group(1))
        if organization:
            bundle["affiliation_signals"].append(
                {
                    "organization": organization,
                    "relation": "scholar_profile_affiliation",
                    "evidence": "Google Scholar profile affiliation",
                    "source_url": base_url,
                }
            )
    interests = [
        _strip_html_text(item)
        for item in re.findall(
            r'class=["\']gsc_prf_inta[^"\']*["\'][^>]*>(.*?)</a>',
            profile_block,
            flags=re.IGNORECASE | re.DOTALL,
        )
    ]
    bundle["research_interests"] = _dedupe([item for item in interests if item])
    metrics = extract_google_scholar_citation_metrics(html_text)
    if metrics:
        bundle["scholar_citation_metrics"].append(metrics)
    bundle["scholar_publications"] = extract_google_scholar_publications(html_text, base_url)[:10]
    bundle["scholar_coauthors"] = extract_google_scholar_coauthors(html_text)[:10]
    merge_signal_bundle(bundle, {"role_signals": bundle["research_interests"]})
    return bundle


def extract_google_scholar_citation_metrics(html_text: str) -> dict[str, str]:
    metrics: dict[str, str] = {}
    for row in re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.IGNORECASE | re.DOTALL):
        cells = [_strip_html_text(cell) for cell in re.findall(r"<td[^>]*>(.*?)</td>", row, flags=re.IGNORECASE | re.DOTALL)]
        if len(cells) < 2:
            continue
        label = cells[0].strip().lower().replace(" ", "_")
        if label in {"citations", "h-index", "h_index", "i10-index", "i10_index"}:
            metrics[label.replace("-", "_")] = cells[1].strip()
            if len(cells) > 2:
                metrics[f"{label.replace('-', '_')}_since_2019"] = cells[2].strip()
    return {key: value for key, value in metrics.items() if value}


def extract_google_scholar_publications(html_text: str, base_url: str) -> list[dict[str, str]]:
    publications: list[dict[str, str]] = []
    for row in re.findall(r'<tr[^>]+class=["\'][^"\']*gsc_a_tr[^"\']*["\'][^>]*>(.*?)</tr>', html_text, flags=re.IGNORECASE | re.DOTALL):
        title_anchor_match = re.search(
            r'<a\b(?=[^>]*class=["\'][^"\']*gsc_a_at[^"\']*["\'])([^>]*)>(.*?)</a>',
            row,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not title_anchor_match:
            continue
        title = _strip_html_text(title_anchor_match.group(2))
        url = parse.urljoin(base_url, unescape(_extract_html_attr(title_anchor_match.group(1), "href")))
        gray_items = [
            _strip_html_text(item)
            for item in re.findall(r'<div[^>]+class=["\'][^"\']*gs_gray[^"\']*["\'][^>]*>(.*?)</div>', row, flags=re.IGNORECASE | re.DOTALL)
        ]
        citation_match = re.search(
            r'<td[^>]+class=["\'][^"\']*gsc_a_c[^"\']*["\'][^>]*>.*?(?:<a[^>]*>|<span[^>]*>)\s*([^<]*?)\s*(?:</a>|</span>)',
            row,
            flags=re.IGNORECASE | re.DOTALL,
        )
        year_match = re.search(
            r'<td[^>]+class=["\'][^"\']*gsc_a_y[^"\']*["\'][^>]*>.*?<span[^>]*>\s*([^<]*?)\s*</span>',
            row,
            flags=re.IGNORECASE | re.DOTALL,
        )
        record = {
            "title": title,
            "url": url,
            "authors": gray_items[0] if gray_items else "",
            "venue": gray_items[1] if len(gray_items) > 1 else "",
            "citations": _strip_html_text(citation_match.group(1)) if citation_match else "",
            "year": _strip_html_text(year_match.group(1)) if year_match else "",
        }
        normalized = {key: value for key, value in record.items() if value}
        if normalized and normalized not in publications:
            publications.append(normalized)
    return publications


def extract_google_scholar_coauthors(html_text: str) -> list[dict[str, str]]:
    coauthors: list[dict[str, str]] = []
    for attrs, label in re.findall(
        r'<a\b(?=[^>]*class=["\'][^"\']*gsc_rsb_aa[^"\']*["\'])([^>]*)>(.*?)</a>',
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        name = _strip_html_text(label)
        if name:
            href = _extract_html_attr(attrs, "href")
            coauthors.append({"name": name, "url": parse.urljoin("https://scholar.google.com", unescape(href))})
    return coauthors


def _extract_html_attr(attrs: str, name: str) -> str:
    match = re.search(rf'\b{re.escape(name)}=["\']([^"\']*)["\']', str(attrs or ""), flags=re.IGNORECASE)
    return str(match.group(1) or "").strip() if match else ""


def build_page_analysis_input(
    *,
    candidate: Candidate,
    target_company: str,
    source_url: str,
    html_text: str,
    extracted_links: dict[str, list[str]],
    max_excerpt_chars: int = DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
) -> dict[str, Any]:
    title, description, visible_text = extract_html_page_context(html_text)
    document_type = detect_document_type(
        source_url=source_url,
        title=title,
        content_type="text/html",
        extracted_links=extracted_links,
    )
    return build_text_analysis_input(
        candidate=candidate,
        target_company=target_company,
        source_url=source_url,
        text=visible_text,
        extracted_links=extracted_links,
        title=title,
        description=description,
        content_type="text/html",
        document_type=document_type,
        max_excerpt_chars=max_excerpt_chars,
    )


def build_text_analysis_input(
    *,
    candidate: Candidate,
    target_company: str,
    source_url: str,
    text: str,
    extracted_links: dict[str, list[str]] | None = None,
    title: str = "",
    description: str = "",
    content_type: str = "text/plain",
    document_type: str = "",
    max_excerpt_chars: int = DEFAULT_MODEL_CONTEXT_CHAR_LIMIT,
) -> dict[str, Any]:
    normalized_text = " ".join(str(text or "").split())
    excerpt = normalized_text[:max_excerpt_chars]
    blocks = build_text_blocks(text)
    return {
        "candidate_id": candidate.candidate_id,
        "candidate_name": candidate.display_name,
        "target_company": target_company,
        "source_url": source_url,
        "source_domain": parse.urlparse(source_url).netloc.lower(),
        "content_type": content_type,
        "document_type": document_type or detect_document_type(source_url=source_url, title=title, content_type=content_type, extracted_links=extracted_links or {}),
        "title": title,
        "description": description,
        "text_excerpt": excerpt,
        "text_blocks": blocks[:30],
        "excerpt_char_count": len(excerpt),
        "raw_asset_policy": RAW_ASSET_POLICY_SUMMARY,
        "candidate_context": {
            "role": candidate.role,
            "organization": candidate.organization,
            "education": candidate.education,
            "work_history": candidate.work_history,
            "linkedin_url": candidate.linkedin_url,
        },
        "extracted_links": {
            "linkedin_urls": list((extracted_links or {}).get("linkedin_urls") or [])[:3],
            "x_urls": list((extracted_links or {}).get("x_urls") or [])[:3],
            "github_urls": list((extracted_links or {}).get("github_urls") or [])[:3],
            "personal_urls": list((extracted_links or {}).get("personal_urls") or [])[:3],
            "resume_urls": list((extracted_links or {}).get("resume_urls") or [])[:3],
            "research_interests": list((extracted_links or {}).get("research_interests") or [])[:6],
            "scholar_verified_domains": list((extracted_links or {}).get("scholar_verified_domains") or [])[:3],
            "scholar_citation_metrics": list((extracted_links or {}).get("scholar_citation_metrics") or [])[:2],
            "scholar_publications": list((extracted_links or {}).get("scholar_publications") or [])[:8],
            "scholar_coauthors": list((extracted_links or {}).get("scholar_coauthors") or [])[:8],
        },
    }


def extract_pdf_front_matter_text(
    pdf_bytes: bytes,
    *,
    fallback_text: str = "",
    max_pages: int = 3,
    max_chars: int = 6000,
) -> str:
    page_limit = max(1, int(max_pages or 1))
    char_limit = max(1, int(max_chars or 6000))
    if PdfReader is not None:
        try:
            reader = PdfReader(BytesIO(pdf_bytes))
            parts: list[str] = []
            for index, page in enumerate(reader.pages):
                if index >= page_limit:
                    break
                try:
                    text = page.extract_text() or ""
                except Exception:
                    text = ""
                if text.strip():
                    parts.append(text.strip())
            front_matter = "\n".join(parts).strip()
            if front_matter:
                return front_matter[:char_limit]
        except Exception:
            pass
    return str(fallback_text or "").strip()[:char_limit]


def build_deterministic_page_analysis(payload: dict[str, Any]) -> dict[str, Any]:
    target_company = str(payload.get("target_company") or "").strip()
    title = str(payload.get("title") or "").strip()
    description = str(payload.get("description") or "").strip()
    excerpt = str(payload.get("text_excerpt") or "").strip()
    document_type = str(payload.get("document_type") or "").strip() or "unknown"
    combined = " ".join([title, description, excerpt]).lower()
    company_match = bool(target_company and target_company.lower() in combined)
    role_signals = []
    for token in ["research", "engineer", "scientist", "safety", "multimodal", "rl", "reinforcement"]:
        if token in combined:
            role_signals.append(token)
    urls = payload.get("extracted_links") or {}
    structured_signals = infer_structured_signals_from_payload(payload)
    confidence_score = 0.25
    if company_match:
        confidence_score += 0.35
    if role_signals:
        confidence_score += 0.2
    if structured_signals["education_signals"] or structured_signals["work_history_signals"]:
        confidence_score += 0.1
    if (urls.get("linkedin_urls") or []) or (urls.get("personal_urls") or []) or (urls.get("x_urls") or []):
        confidence_score += 0.15
    confidence_label = "high" if confidence_score >= 0.75 else "medium" if confidence_score >= 0.45 else "low"
    return {
        "summary": " | ".join(item for item in [title, description] if item)[:400],
        "target_company_relation": "explicit" if company_match else "unclear",
        "role_signals": role_signals[:6],
        "education_signals": structured_signals["education_signals"][:6],
        "work_history_signals": structured_signals["work_history_signals"][:8],
        "affiliation_signals": structured_signals["affiliation_signals"][:6],
        "confidence_label": confidence_label,
        "confidence_score": round(min(confidence_score, 0.95), 2),
        "document_type": document_type,
        "recommended_links": {
            "linkedin_url": ((urls.get("linkedin_urls") or [""])[0]),
            "personal_url": ((urls.get("personal_urls") or [""])[0]),
            "x_url": ((urls.get("x_urls") or [""])[0]),
            "github_url": ((urls.get("github_urls") or [""])[0]),
            "resume_url": ((urls.get("resume_urls") or [""])[0]),
        },
        "notes": "Deterministic page analysis fallback.",
    }


def build_document_evidence_slice(
    *,
    analysis_input: dict[str, Any],
    signals: dict[str, Any],
    source_url: str,
    final_url: str,
    source_text: str,
    max_chars: int = DEFAULT_EVIDENCE_SLICE_MAX_CHARS,
) -> dict[str, Any]:
    source_type = infer_document_source_type(
        analysis_input=analysis_input,
        source_url=source_url,
        final_url=final_url,
    )
    visible_text = _evidence_visible_text(source_text)
    if not visible_text:
        visible_text = str(analysis_input.get("text_excerpt") or "")
    char_limit = min(
        max(500, int(max_chars or DEFAULT_EVIDENCE_SLICE_MAX_CHARS)),
        SOURCE_EVIDENCE_CHAR_LIMITS.get(source_type, DEFAULT_EVIDENCE_SLICE_MAX_CHARS),
    )
    email_contexts = extract_evidence_email_contexts(visible_text)
    lines = _select_source_aware_evidence_lines(
        source_type=source_type,
        visible_text=visible_text,
        analysis_input=analysis_input,
        email_contexts=email_contexts,
        max_chars=char_limit,
    )
    selected_text = _join_lines_with_char_limit(lines, char_limit)
    return {
        "source_url": source_url,
        "final_url": final_url,
        "source_domain": parse.urlparse(final_url or source_url).netloc.lower().removeprefix("www."),
        "source_type": source_type,
        "document_type": str(analysis_input.get("document_type") or "").strip() or "unknown",
        "title": str(analysis_input.get("title") or "").strip(),
        "description": str(analysis_input.get("description") or "").strip(),
        "selected_text": selected_text,
        "text_blocks": lines[:40],
        "email_contexts": email_contexts[:8],
        "links": _compact_evidence_links(signals=signals, analysis_input=analysis_input),
        "structured_signals": {
            "research_interests": list((signals or {}).get("research_interests") or [])[:10],
            "scholar_verified_domains": list((signals or {}).get("scholar_verified_domains") or [])[:5],
            "scholar_citation_metrics": list((signals or {}).get("scholar_citation_metrics") or [])[:2],
            "scholar_publications": list((signals or {}).get("scholar_publications") or [])[:10],
            "scholar_coauthors": list((signals or {}).get("scholar_coauthors") or [])[:10],
            "affiliation_signals": list((signals or {}).get("affiliation_signals") or [])[:6],
        },
        "source_char_count": len(visible_text),
        "selected_char_count": len(selected_text),
        "slicing": {
            "strategy": source_type,
            "max_chars": char_limit,
            "raw_asset_policy": RAW_ASSET_POLICY_SUMMARY,
        },
    }


def analyze_remote_document(
    *,
    candidate: Candidate,
    target_company: str,
    source_url: str,
    asset_dir: Path,
    asset_logger: AssetLogger,
    model_client: ModelClient | None = None,
    source_kind: str,
    asset_prefix: str,
    timeout: int = 30,
    analyze_with_model: bool = True,
    document_type_hint: str = "",
) -> AnalyzedDocumentAsset:
    source_url = str(source_url or "").strip()
    if not source_url:
        raise RuntimeError("Source URL is required.")

    if is_google_doc_document_url(source_url):
        export_url = google_doc_export_url(source_url)
        fetched = fetch_text_url(export_url, timeout=timeout, source_label=source_kind)
        extracted_links = empty_signal_bundle()
        document_type = "google_doc_resume"
        extracted_links["resume_urls"].append(source_url)
        raw_path = asset_logger.write_text(
            asset_dir / f"{asset_prefix}.txt",
            fetched.text,
            asset_type="document_text_export",
            source_kind=source_kind,
            content_type=fetched.content_type or "text/plain",
            is_raw_asset=True,
            model_safe=False,
            metadata={"source_url": source_url, "final_url": fetched.final_url, "document_type": document_type},
        )
        analysis_input = build_text_analysis_input(
            candidate=candidate,
            target_company=target_company,
            source_url=source_url,
            text=fetched.text,
            extracted_links=extracted_links,
            title="Google Docs export",
            description="",
            content_type=fetched.content_type or "text/plain",
            document_type=document_type,
        )
        extracted_text_path = str(raw_path)
        title = "Google Docs export"
        description = ""
        final_url = fetched.final_url
        content_type = fetched.content_type or "text/plain"
        signals = extracted_links
        source_evidence_text = fetched.text
    elif source_url.lower().endswith(".pdf"):
        fetched = fetch_binary_url(source_url, timeout=timeout, source_label=source_kind)
        document_type = infer_pdf_document_type(source_url=source_url, content_type=fetched.content_type, hint=document_type_hint)
        raw_path = asset_logger.write_bytes(
            asset_dir / f"{asset_prefix}.pdf",
            fetched.content,
            asset_type="document_pdf",
            source_kind=source_kind,
            content_type=fetched.content_type or "application/pdf",
            is_raw_asset=True,
            model_safe=False,
            metadata={"source_url": source_url, "final_url": fetched.final_url, "document_type": document_type},
        )
        extraction_result = extract_pdf_text_details(fetched.content)
        extracted_text = extraction_result.text
        source_evidence_text = extract_pdf_front_matter_text(fetched.content, fallback_text=extracted_text)
        extracted_text_file = asset_logger.write_text(
            asset_dir / f"{asset_prefix}_extracted.txt",
            extracted_text,
            asset_type="document_pdf_extracted_text",
            source_kind=source_kind,
            content_type="text/plain",
            is_raw_asset=False,
            model_safe=True,
            metadata={
                "source_url": source_url,
                "final_url": fetched.final_url,
                "document_type": document_type,
                "extraction_method": extraction_result.method,
                "extraction_attempts": extraction_result.attempts,
            },
        )
        signals = empty_signal_bundle()
        if document_type == "pdf_resume" and source_url not in signals["resume_urls"]:
            signals["resume_urls"].append(source_url)
        title = Path(parse.urlparse(source_url).path).name or (
            "Publication PDF" if document_type == "pdf_publication" else "Resume PDF"
        )
        description = "Extracted PDF front matter" if document_type == "pdf_publication" else "Extracted PDF resume text"
        analysis_input = build_text_analysis_input(
            candidate=candidate,
            target_company=target_company,
            source_url=source_url,
            text=extracted_text,
            extracted_links=signals,
            title=title,
            description=description,
            content_type=fetched.content_type or "application/pdf",
            document_type=document_type,
        )
        extracted_text_path = str(extracted_text_file)
        final_url = fetched.final_url
        content_type = fetched.content_type or "application/pdf"
    else:
        fetched = fetch_text_url(source_url, timeout=timeout, source_label=source_kind)
        page_html = fetched.text
        source_evidence_text = page_html
        raw_path = asset_logger.write_text(
            asset_dir / f"{asset_prefix}.html",
            page_html,
            asset_type="document_html",
            source_kind=source_kind,
            content_type=fetched.content_type or "text/html",
            is_raw_asset=True,
            model_safe=False,
            metadata={"source_url": source_url, "final_url": fetched.final_url},
        )
        signals = extract_page_signals(page_html, fetched.final_url)
        analysis_input = build_page_analysis_input(
            candidate=candidate,
            target_company=target_company,
            source_url=fetched.final_url,
            html_text=page_html,
            extracted_links=signals,
        )
        if str(analysis_input.get("document_type") or "").strip() in {"html_resume", "google_doc_resume"}:
            if source_url not in signals["resume_urls"]:
                signals["resume_urls"].append(source_url)
        extracted_text_path = ""
        title = str(analysis_input.get("title") or "")
        description = str(analysis_input.get("description") or "")
        final_url = fetched.final_url
        content_type = fetched.content_type or "text/html"

    analysis_input_path = asset_logger.write_json(
        asset_dir / f"{asset_prefix}_analysis_input.json",
        analysis_input,
        asset_type="document_analysis_input",
        source_kind=source_kind,
        is_raw_asset=False,
        model_safe=True,
        metadata={"source_url": source_url, "final_url": final_url},
    )
    evidence_slice = build_document_evidence_slice(
        analysis_input=analysis_input,
        signals=signals,
        source_url=source_url,
        final_url=final_url,
        source_text=source_evidence_text,
    )
    evidence_slice_path = asset_logger.write_json(
        asset_dir / f"{asset_prefix}_evidence_slice.json",
        evidence_slice,
        asset_type="document_evidence_slice",
        source_kind=source_kind,
        is_raw_asset=False,
        model_safe=True,
        metadata={"source_url": source_url, "final_url": final_url},
    )
    if analyze_with_model and model_client is not None:
        analysis = normalize_analysis(model_client.analyze_page_asset(analysis_input))
    else:
        analysis = normalize_analysis(build_deterministic_page_analysis(analysis_input))
    analysis_path = asset_logger.write_json(
        asset_dir / f"{asset_prefix}_analysis.json",
        analysis,
        asset_type="document_analysis_output",
        source_kind=source_kind,
        is_raw_asset=False,
        model_safe=True,
        metadata={"source_url": source_url, "final_url": final_url},
    )
    merge_analysis_signal_bundle(signals, analysis)
    return AnalyzedDocumentAsset(
        source_url=source_url,
        final_url=final_url,
        content_type=content_type,
        document_type=str(analysis.get("document_type") or analysis_input.get("document_type") or "unknown"),
        raw_path=str(raw_path),
        extracted_text_path=extracted_text_path,
        analysis_input_path=str(analysis_input_path),
        analysis_path=str(analysis_path),
        signals=signals,
        analysis=analysis,
        title=title,
        description=description,
        evidence_slice_path=str(evidence_slice_path),
        evidence_slice=evidence_slice,
    )


def normalize_analysis(analysis: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(analysis or {})
    payload.setdefault("summary", "")
    payload.setdefault("target_company_relation", "unclear")
    payload.setdefault("role_signals", [])
    payload.setdefault("confidence_label", "low")
    payload.setdefault("confidence_score", 0.2)
    payload.setdefault("recommended_links", {})
    payload.setdefault("notes", "")
    payload.setdefault("document_type", "unknown")
    payload["role_signals"] = _normalize_string_list(payload.get("role_signals"))
    for key in STRUCTURED_SIGNAL_KEYS:
        payload[key] = _normalize_signal_records(payload.get(key))
    raw_recommended_links = payload.get("recommended_links") or {}
    recommended_links = raw_recommended_links if isinstance(raw_recommended_links, dict) else {}
    payload["recommended_links"] = {
        "linkedin_url": str(recommended_links.get("linkedin_url") or "").strip(),
        "personal_url": str(recommended_links.get("personal_url") or "").strip(),
        "x_url": str(recommended_links.get("x_url") or "").strip(),
        "github_url": str(recommended_links.get("github_url") or "").strip(),
        "resume_url": str(recommended_links.get("resume_url") or "").strip(),
    }
    return payload


def merge_signal_bundle(target: dict[str, Any], incoming: dict[str, Any]) -> None:
    for key, values in incoming.items():
        current = target.setdefault(key, [] if isinstance(values, list) else values)
        if isinstance(values, list):
            if not isinstance(current, list):
                target[key] = list(values)
                continue
            for value in values:
                if isinstance(value, dict):
                    normalized = json.dumps(value, ensure_ascii=False, sort_keys=True)
                    existing = {json.dumps(item, ensure_ascii=False, sort_keys=True) for item in current if isinstance(item, dict)}
                    if normalized not in existing:
                        current.append(value)
                else:
                    normalized = str(value or "").strip()
                    if normalized and normalized not in current:
                        current.append(normalized)


def merge_analysis_signal_bundle(target: dict[str, Any], analysis: dict[str, Any]) -> None:
    normalized = normalize_analysis(analysis)
    summary = str(normalized.get("summary") or "").strip()
    if summary:
        summaries = target.setdefault("validated_summaries", [])
        if summary not in summaries:
            summaries.append(summary)
    notes = str(normalized.get("notes") or "").strip()
    if notes:
        analysis_notes = target.setdefault("analysis_notes", [])
        if notes not in analysis_notes:
            analysis_notes.append(notes)
    document_type = str(normalized.get("document_type") or "").strip()
    if document_type:
        document_types = target.setdefault("document_types", [])
        if document_type not in document_types:
            document_types.append(document_type)
    for key in STRUCTURED_SIGNAL_KEYS:
        merge_signal_bundle(target, {key: normalized.get(key) or []})
    merge_signal_bundle(target, {"role_signals": normalized.get("role_signals") or []})
    recommended_links = dict(normalized.get("recommended_links") or {})
    merge_signal_bundle(
        target,
        {
            "linkedin_urls": [recommended_links.get("linkedin_url", "")],
            "personal_urls": [recommended_links.get("personal_url", "")],
            "x_urls": [recommended_links.get("x_url", "")],
            "github_urls": [recommended_links.get("github_url", "")],
            "resume_urls": [recommended_links.get("resume_url", "")],
        },
    )


def build_candidate_patch_from_signal_bundle(
    candidate: Candidate,
    signal_bundle: dict[str, Any],
    *,
    target_company: str,
    source_url: str = "",
) -> dict[str, Any]:
    analysis = {
        "role_signals": list(signal_bundle.get("role_signals") or []),
        "education_signals": list(signal_bundle.get("education_signals") or []),
        "work_history_signals": list(signal_bundle.get("work_history_signals") or []),
        "affiliation_signals": list(signal_bundle.get("affiliation_signals") or []),
        "recommended_links": {
            "linkedin_url": _first_nonempty(signal_bundle.get("linkedin_urls") or []),
            "personal_url": _first_nonempty(signal_bundle.get("personal_urls") or []),
            "x_url": _first_nonempty(signal_bundle.get("x_urls") or []),
            "github_url": _first_nonempty(signal_bundle.get("github_urls") or []),
            "resume_url": _first_nonempty(signal_bundle.get("resume_urls") or []),
        },
        "document_type": _first_nonempty(signal_bundle.get("document_types") or []),
        "summary": _first_nonempty(signal_bundle.get("validated_summaries") or []),
        "notes": _first_nonempty(signal_bundle.get("analysis_notes") or []),
    }
    return build_candidate_patch_from_analysis(
        candidate,
        normalize_analysis(analysis),
        target_company=target_company,
        source_url=source_url,
    )


def build_candidate_patch_from_analysis(
    candidate: Candidate,
    analysis: dict[str, Any],
    *,
    target_company: str,
    source_url: str = "",
) -> dict[str, Any]:
    normalized = normalize_analysis(analysis)
    patch: dict[str, Any] = {}
    education = format_education_signals(normalized.get("education_signals") or [])
    work_history = format_work_history_signals(normalized.get("work_history_signals") or [])
    role_signal_text = "; ".join(list(normalized.get("role_signals") or [])[:6]).strip()
    recommended_links = dict(normalized.get("recommended_links") or {})
    if education and not str(candidate.education or "").strip():
        patch["education"] = education
    if work_history and not str(candidate.work_history or "").strip():
        patch["work_history"] = work_history
    if role_signal_text and not str(candidate.focus_areas or "").strip():
        patch["focus_areas"] = role_signal_text
    if not str(candidate.linkedin_url or "").strip() and str(recommended_links.get("linkedin_url") or "").strip():
        patch["linkedin_url"] = str(recommended_links.get("linkedin_url") or "").strip()
    if not str(candidate.media_url or "").strip():
        media_url = _first_nonempty(
            [
                recommended_links.get("personal_url"),
                recommended_links.get("x_url"),
                recommended_links.get("github_url"),
                recommended_links.get("resume_url"),
            ]
        )
        if media_url:
            patch["media_url"] = media_url
    work_items = list(normalized.get("work_history_signals") or [])
    if not str(candidate.role or "").strip() and work_items:
        top_title = str(work_items[0].get("title") or "").strip()
        if top_title:
            patch["role"] = top_title
    metadata = {
        **dict(candidate.metadata or {}),
        "analysis_document_type": str(normalized.get("document_type") or "").strip(),
        "analysis_source_url": source_url,
        "analysis_education_signals": list(normalized.get("education_signals") or [])[:6],
        "analysis_work_history_signals": list(normalized.get("work_history_signals") or [])[:8],
        "analysis_affiliation_signals": list(normalized.get("affiliation_signals") or [])[:6],
    }
    affiliation_items = list(normalized.get("affiliation_signals") or [])
    current_affiliation = next(
        (
            item
            for item in affiliation_items
            if _normalize(item.get("organization")) == _normalize(target_company)
            and str(item.get("relation") or "").strip() in {"current_affiliation", "explicit_current_affiliation"}
        ),
        None,
    )
    if current_affiliation and not str(candidate.organization or "").strip():
        patch["organization"] = target_company
    patch["metadata"] = metadata
    return patch


def format_education_signals(items: list[dict[str, Any]]) -> str:
    formatted: list[str] = []
    for item in items[:6]:
        school = str(item.get("school") or "").strip()
        degree = str(item.get("degree") or "").strip()
        field = str(item.get("field") or "").strip()
        date_range = str(item.get("date_range") or "").strip()
        parts = [school]
        detail = ", ".join(part for part in [degree, field] if part)
        if detail:
            parts.append(detail)
        text = " - ".join(part for part in parts if part)
        if date_range:
            text = f"{text} ({date_range})" if text else date_range
        if text and text not in formatted:
            formatted.append(text)
    return " | ".join(formatted)


def format_work_history_signals(items: list[dict[str, Any]]) -> str:
    formatted: list[str] = []
    for item in items[:8]:
        organization = str(item.get("organization") or "").strip()
        title = str(item.get("title") or "").strip()
        date_range = str(item.get("date_range") or "").strip()
        description = str(item.get("description") or "").strip()
        text = " @ ".join(part for part in [title, organization] if part)
        if date_range:
            text = f"{text} ({date_range})" if text else date_range
        if description:
            text = f"{text}: {description}" if text else description
        if text and text not in formatted:
            formatted.append(text)
    return " | ".join(formatted)


def detect_document_type(
    *,
    source_url: str,
    title: str = "",
    content_type: str = "",
    extracted_links: dict[str, list[str]] | None = None,
) -> str:
    lowered_url = str(source_url or "").strip().lower()
    lowered_title = str(title or "").strip().lower()
    lowered_content_type = str(content_type or "").strip().lower()
    if is_google_doc_document_url(lowered_url):
        return "google_doc_resume"
    if lowered_url.endswith(".pdf") or "application/pdf" in lowered_content_type:
        return "pdf_resume"
    if any(token in lowered_url for token in ["/cv", "/resume"]) or any(token in lowered_title for token in ["cv", "resume", "curriculum vitae"]):
        return "html_resume"
    if "linkedin.com/in/" in lowered_url:
        return "linkedin_profile"
    if any(token in lowered_url for token in ["github.com/", "x.com/", "twitter.com/"]):
        return "social_profile"
    if extracted_links and list((extracted_links or {}).get("resume_urls") or []):
        return "personal_homepage_with_resume"
    parsed = parse.urlparse(lowered_url)
    if parsed.scheme.startswith("http") and parsed.path in {"", "/"}:
        return "personal_homepage"
    return "web_page"


def extract_html_page_context(html_text: str) -> tuple[str, str, str]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    title = " ".join(unescape(title_match.group(1)).split()) if title_match else ""
    meta_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        html_text,
        flags=re.IGNORECASE,
    )
    if not meta_match:
        meta_match = re.search(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
            html_text,
            flags=re.IGNORECASE,
        )
    description = " ".join(unescape(meta_match.group(1)).split()) if meta_match else ""
    visible = re.sub(r"<script.*?</script>|<style.*?</style>", " ", html_text, flags=re.IGNORECASE | re.DOTALL)
    visible = re.sub(r"</(p|div|li|h1|h2|h3|h4|h5|h6|section|article|br)>", "\n", visible, flags=re.IGNORECASE)
    visible = re.sub(r"<[^>]+>", " ", visible)
    visible = "\n".join(line.strip() for line in unescape(visible).splitlines() if line.strip())
    return title, description, visible


def build_text_blocks(text: str, *, max_blocks: int = 40, max_line_chars: int = 180) -> list[str]:
    blocks: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = " ".join(raw_line.split())
        if not line:
            continue
        trimmed = line[:max_line_chars]
        if trimmed not in blocks:
            blocks.append(trimmed)
        if len(blocks) >= max_blocks:
            break
    if not blocks:
        collapsed = " ".join(str(text or "").split())
        if collapsed:
            blocks.append(collapsed[:max_line_chars])
    return blocks


def infer_pdf_document_type(*, source_url: str, content_type: str = "", hint: str = "") -> str:
    normalized_hint = str(hint or "").strip().lower()
    if normalized_hint in {"publication_url", "publication", "paper", "paper_pdf"}:
        return "pdf_publication"
    if normalized_hint in {"resume_url", "resume", "cv", "resume_and_documents"}:
        return "pdf_resume"
    parsed = parse.urlparse(str(source_url or "").strip().lower())
    domain = parsed.netloc.removeprefix("www.")
    path = parsed.path.lower()
    if any(token in path for token in ["/cv", "/resume", "cv.pdf", "resume.pdf", "curriculum-vitae"]):
        return "pdf_resume"
    if domain in PUBLICATION_DOCUMENT_DOMAINS or any(domain.endswith(f".{item}") for item in PUBLICATION_DOCUMENT_DOMAINS):
        return "pdf_publication"
    if any(token in path for token in ["/paper", "/papers", "/publication", "/publications", "/abs/", "/pdf/"]):
        return "pdf_publication"
    return detect_document_type(
        source_url=source_url,
        title=Path(parsed.path).name or "",
        content_type=content_type or "application/pdf",
        extracted_links={},
    )


def infer_document_source_type(*, analysis_input: dict[str, Any], source_url: str, final_url: str = "") -> str:
    url = str(final_url or source_url or "").strip()
    parsed = parse.urlparse(url.lower())
    domain = parsed.netloc.removeprefix("www.")
    document_type = str(analysis_input.get("document_type") or "").strip().lower()
    if domain == "github.com":
        return "github_profile"
    if "scholar.google." in domain:
        return "google_scholar_profile"
    if domain in {"x.com", "twitter.com"}:
        return "x_profile"
    if domain.endswith("substack.com") or "substack" in domain:
        return "substack_profile"
    if document_type in {"google_doc_resume", "html_resume", "pdf_resume", "personal_homepage_with_resume"}:
        return "resume_or_cv"
    if document_type == "pdf_publication":
        return "publication_pdf"
    if domain in PUBLICATION_DOCUMENT_DOMAINS or any(domain.endswith(f".{item}") for item in PUBLICATION_DOCUMENT_DOMAINS):
        return "publication_pdf"
    if document_type == "academic_profile" or _looks_like_academic_source(domain, parsed.path):
        return "academic_profile"
    if document_type == "personal_homepage":
        return "personal_homepage"
    return "web_page"


def extract_evidence_email_contexts(text: str, *, window: int = 180) -> list[dict[str, str]]:
    expanded = _deobfuscate_evidence_email_text(text)
    contexts: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in _EVIDENCE_EMAIL_RE.finditer(expanded):
        email = match.group(0).strip().lower()
        if not email or email in seen:
            continue
        seen.add(email)
        start = max(0, match.start() - window)
        end = min(len(expanded), match.end() + window)
        excerpt = " ".join(expanded[start:end].split())[:600]
        contexts.append({"email": email, "excerpt": excerpt})
    return contexts


def _evidence_visible_text(source_text: str) -> str:
    raw = str(source_text or "")
    if "<" in raw and ">" in raw and re.search(r"</?[a-zA-Z][^>]*>", raw):
        _title, _description, visible = extract_html_page_context(raw)
        return visible
    return raw


def _select_source_aware_evidence_lines(
    *,
    source_type: str,
    visible_text: str,
    analysis_input: dict[str, Any],
    email_contexts: list[dict[str, str]],
    max_chars: int,
) -> list[str]:
    source_lines = build_text_blocks(visible_text, max_blocks=500, max_line_chars=260)
    selected: list[str] = []

    def add(value: Any) -> None:
        line = " ".join(str(value or "").split())
        if line and line not in selected:
            selected.append(line[:260])

    add(analysis_input.get("title"))
    add(analysis_input.get("description"))
    for context in email_contexts:
        add(context.get("excerpt"))

    identity_terms = _identity_terms_from_analysis_input(analysis_input)
    source_keywords = _source_keyword_terms(source_type)
    first_line_quota = {
        "github_profile": 80,
        "google_scholar_profile": 80,
        "x_profile": 50,
        "substack_profile": 70,
        "publication_pdf": 120,
        "resume_or_cv": 140,
        "academic_profile": 120,
        "personal_homepage": 180,
        "web_page": 80,
    }.get(source_type, 80)
    for line in source_lines[:first_line_quota]:
        lower = line.lower()
        if _line_has_email(line) or _line_matches_any(lower, identity_terms) or _line_matches_any(lower, source_keywords):
            add(line)
    if source_type in {"publication_pdf", "resume_or_cv", "personal_homepage", "academic_profile"}:
        for line in source_lines[:first_line_quota]:
            add(line)
            if _lines_char_count(selected) >= max_chars:
                break
    elif len(selected) <= 3:
        for line in source_lines[: min(35, first_line_quota)]:
            add(line)
            if _lines_char_count(selected) >= max_chars:
                break
    return _trim_lines_to_char_limit(selected, max_chars)


def _identity_terms_from_analysis_input(analysis_input: dict[str, Any]) -> list[str]:
    candidate_context = dict(analysis_input.get("candidate_context") or {})
    raw_values = [
        analysis_input.get("candidate_name"),
        analysis_input.get("target_company"),
        candidate_context.get("role"),
        candidate_context.get("organization"),
        candidate_context.get("education"),
        candidate_context.get("work_history"),
        candidate_context.get("linkedin_url"),
    ]
    terms: list[str] = []
    for value in raw_values:
        text = str(value or "").strip().lower()
        if not text:
            continue
        if len(text) <= 80:
            terms.append(text)
        terms.extend(token for token in re.findall(r"[a-z0-9][a-z0-9._+-]{2,}", text) if len(token) >= 3)
    return _dedupe([term for term in terms if term])


def _source_keyword_terms(source_type: str) -> list[str]:
    common = ["email", "mail", "contact", "homepage", "website", "profile", "research", "engineer", "scientist"]
    by_source = {
        "github_profile": ["followers", "following", "repositories", "stars", "contributions", "location"],
        "google_scholar_profile": ["verified email", "homepage", "interests", "cited by", "affiliation"],
        "x_profile": ["followers", "following", "posts", "joined", "bio"],
        "substack_profile": ["subscribe", "substack", "about", "archive", "newsletter"],
        "publication_pdf": ["abstract", "author", "authors", "affiliation", "correspondence"],
        "resume_or_cv": ["education", "experience", "publications", "employment", "skills"],
        "academic_profile": ["faculty", "student", "phd", "university", "lab", "publications"],
        "personal_homepage": ["about", "bio", "publications", "projects", "cv", "resume"],
    }
    return common + by_source.get(source_type, [])


def _line_matches_any(line_lower: str, terms: list[str]) -> bool:
    return any(term and term in line_lower for term in terms)


def _line_has_email(line: str) -> bool:
    return bool(_EVIDENCE_EMAIL_RE.search(_deobfuscate_evidence_email_text(line)))


def _lines_char_count(lines: list[str]) -> int:
    return sum(len(line) + 1 for line in lines)


def _trim_lines_to_char_limit(lines: list[str], max_chars: int) -> list[str]:
    trimmed: list[str] = []
    used = 0
    limit = max(500, int(max_chars or 500))
    for line in lines:
        addition = len(line) + (1 if trimmed else 0)
        if used + addition > limit:
            remaining = limit - used - (1 if trimmed else 0)
            if remaining > 80:
                trimmed.append(line[:remaining])
            break
        trimmed.append(line)
        used += addition
    return trimmed


def _join_lines_with_char_limit(lines: list[str], max_chars: int) -> str:
    return "\n".join(_trim_lines_to_char_limit(lines, max_chars))[: max(500, int(max_chars or 500))]


def _compact_evidence_links(*, signals: dict[str, Any], analysis_input: dict[str, Any]) -> dict[str, list[Any]]:
    extracted_links = dict(analysis_input.get("extracted_links") or {})
    keys = (
        "linkedin_urls",
        "x_urls",
        "github_urls",
        "personal_urls",
        "resume_urls",
        "research_interests",
        "scholar_verified_domains",
        "scholar_citation_metrics",
        "scholar_publications",
        "scholar_coauthors",
    )
    compact: dict[str, list[Any]] = {}
    for key in keys:
        raw_values = list((signals or {}).get(key) or extracted_links.get(key) or [])
        if key in {"scholar_citation_metrics", "scholar_publications", "scholar_coauthors"}:
            compact[key] = [item for item in raw_values if isinstance(item, dict)][:8]
        else:
            values = [str(item or "").strip() for item in raw_values]
            compact[key] = _dedupe([item for item in values if item])[:8]
    return compact


def _deobfuscate_evidence_email_text(text: str) -> str:
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
    return _expand_evidence_group_email_patterns(value)


def _expand_evidence_group_email_patterns(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
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
        replace,
        str(text or ""),
    )


def _looks_like_academic_source(domain: str, path: str) -> bool:
    normalized_domain = str(domain or "").lower()
    normalized_path = str(path or "").lower()
    return (
        normalized_domain.endswith(".edu")
        or ".edu." in normalized_domain
        or any(token in normalized_path for token in ["/people/", "/person/", "/faculty/", "/students/", "/~"])
    )


def infer_structured_signals_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    target_company = str(payload.get("target_company") or "").strip()
    text = "\n".join(list(payload.get("text_blocks") or []))
    lowered_target = _normalize(target_company)
    education_signals: list[dict[str, str]] = []
    work_history_signals: list[dict[str, str]] = []
    affiliation_signals: list[dict[str, str]] = []

    for line in build_text_blocks(text, max_blocks=80):
        lower = line.lower()
        if _looks_like_education_line(line):
            item = _parse_education_line(line)
            if item and item not in education_signals:
                education_signals.append(item)
        if _looks_like_work_line(line):
            item = _parse_work_line(line)
            if item and item not in work_history_signals:
                work_history_signals.append(item)
        if lowered_target and lowered_target in _normalize(line):
            relation = "explicit_current_affiliation"
            if any(token in lower for token in ["prev", "previously", "formerly", "ex-", "ex ", "former"]):
                relation = "past_affiliation"
            affiliation = {
                "organization": target_company,
                "relation": relation,
                "evidence": line[:220],
            }
            if affiliation not in affiliation_signals:
                affiliation_signals.append(affiliation)

    return {
        "education_signals": education_signals[:6],
        "work_history_signals": work_history_signals[:8],
        "affiliation_signals": affiliation_signals[:6],
    }


def extract_pdf_text(pdf_bytes: bytes) -> str:
    return extract_pdf_text_details(pdf_bytes).text


def extract_pdf_text_details(pdf_bytes: bytes) -> PdfExtractionResult:
    candidates: list[tuple[str, str]] = []
    attempts: list[dict[str, Any]] = []

    pypdf_text = _extract_pdf_text_pypdf(pdf_bytes)
    attempts.append({"method": "pypdf", "char_count": len(pypdf_text)})
    if pypdf_text:
        candidates.append(("pypdf", pypdf_text))

    pdfminer_text = _extract_pdf_text_pdfminer(pdf_bytes)
    attempts.append({"method": "pdfminer", "char_count": len(pdfminer_text)})
    if pdfminer_text:
        candidates.append(("pdfminer", pdfminer_text))

    pdftotext_text = _extract_pdf_text_pdftotext(pdf_bytes)
    attempts.append({"method": "pdftotext", "char_count": len(pdftotext_text)})
    if pdftotext_text:
        candidates.append(("pdftotext", pdftotext_text))

    ocr_text = _extract_pdf_text_ocr(pdf_bytes)
    attempts.append({"method": "ocr", "char_count": len(ocr_text)})
    if ocr_text:
        candidates.append(("ocr", ocr_text))

    if not candidates:
        return PdfExtractionResult(text="", method="none", attempts=attempts)

    best_method, best_text = max(candidates, key=lambda item: _score_extracted_text(item[1]))
    return PdfExtractionResult(text=best_text, method=best_method, attempts=attempts)


def _extract_pdf_text_pypdf(pdf_bytes: bytes) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
    except Exception:
        return ""
    parts: list[str] = []
    for page in reader.pages:
        try:
            text = page.extract_text() or ""
        except Exception:
            text = ""
        if text.strip():
            parts.append(text.strip())
    return "\n".join(parts).strip()


def _extract_pdf_text_pdfminer(pdf_bytes: bytes) -> str:
    if pdfminer_extract_text is None:
        return ""
    with tempfile.NamedTemporaryFile(suffix=".pdf") as handle:
        try:
            handle.write(pdf_bytes)
            handle.flush()
            text = pdfminer_extract_text(handle.name) or ""
        except Exception:
            return ""
    return str(text or "").strip()


def _extract_pdf_text_pdftotext(pdf_bytes: bytes) -> str:
    if shutil.which("pdftotext") is None:
        return ""
    with tempfile.TemporaryDirectory(prefix="sourcing-agent-pdftotext-") as tempdir:
        pdf_path = Path(tempdir) / "input.pdf"
        txt_path = Path(tempdir) / "output.txt"
        try:
            pdf_path.write_bytes(pdf_bytes)
            completed = subprocess.run(
                ["pdftotext", "-layout", str(pdf_path), str(txt_path)],
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if completed.returncode != 0 or not txt_path.exists():
                return ""
            return txt_path.read_text(errors="ignore").strip()
        except Exception:
            return ""


def _extract_pdf_text_ocr(pdf_bytes: bytes) -> str:
    if shutil.which("pdftoppm") is None or shutil.which("tesseract") is None:
        return ""
    with tempfile.TemporaryDirectory(prefix="sourcing-agent-pdfocr-") as tempdir:
        pdf_path = Path(tempdir) / "input.pdf"
        output_prefix = Path(tempdir) / "page"
        pdf_path.write_bytes(pdf_bytes)
        try:
            render = subprocess.run(
                ["pdftoppm", "-png", str(pdf_path), str(output_prefix)],
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            if render.returncode != 0:
                return ""
        except Exception:
            return ""

        texts: list[str] = []
        for image_path in sorted(Path(tempdir).glob("page-*.png"))[:10]:
            txt_base = image_path.with_suffix("")
            try:
                ocr = subprocess.run(
                    ["tesseract", str(image_path), str(txt_base), "--psm", "6"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                txt_path = txt_base.with_suffix(".txt")
                if ocr.returncode == 0 and txt_path.exists():
                    text = txt_path.read_text(errors="ignore").strip()
                    if text:
                        texts.append(text)
            except Exception:
                continue
        return "\n".join(texts).strip()


def _score_extracted_text(text: str) -> int:
    normalized = " ".join(str(text or "").split())
    if not normalized:
        return 0
    alpha_count = len(re.findall(r"[A-Za-z]", normalized))
    line_count = len([line for line in str(text or "").splitlines() if line.strip()])
    return len(normalized) + (alpha_count * 2) + (line_count * 10)


def is_google_doc_document_url(url: str) -> bool:
    lowered = str(url or "").strip().lower()
    return "docs.google.com/document/d/" in lowered


def google_doc_export_url(url: str) -> str:
    match = re.search(r"docs\.google\.com/document/d/([^/]+)/", str(url or ""))
    if not match:
        return url
    doc_id = str(match.group(1) or "").strip()
    return f"https://docs.google.com/document/d/{doc_id}/export?format=txt"


def _normalize_signal_records(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    records: list[dict[str, str]] = []
    for item in value:
        if isinstance(item, dict):
            normalized = {str(key): str(val).strip() for key, val in item.items() if str(val).strip()}
            if normalized and normalized not in records:
                records.append(normalized)
        elif isinstance(item, str) and item.strip():
            records.append({"value": item.strip()})
    return records


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    results: list[str] = []
    for item in value:
        normalized = str(item or "").strip()
        if normalized and normalized not in results:
            results.append(normalized)
    return results


def _parse_education_line(line: str) -> dict[str, str] | None:
    school_match = re.search(
        r"([A-Z][A-Za-z&.,'’\\-]+(?:University|Institute|College|School|Academy|MIT|CMU|Stanford|Harvard|Berkeley|OpenAI Residency))",
        line,
    )
    school = school_match.group(1).strip() if school_match else ""
    degree_match = re.search(r"\b(PhD|Ph\.D\.|MS|M\.S\.|MEng|MBA|BS|B\.S\.|BA|B\.A\.)\b", line)
    degree = degree_match.group(1).replace(".", "") if degree_match else ""
    field_match = re.search(r"\b(?:in|,)\s+([A-Z][A-Za-z /&\\-]{3,60})", line)
    field = field_match.group(1).strip() if field_match else ""
    dates = _extract_date_range(line)
    if not school and not degree:
        return None
    return {key: value for key, value in {"school": school, "degree": degree, "field": field, "date_range": dates}.items() if value}


def _parse_work_line(line: str) -> dict[str, str] | None:
    title = ""
    organization = ""
    date_range = _extract_date_range(line)
    if " at " in line:
        left, right = line.split(" at ", 1)
        title, organization = left.strip(" -|"), right.strip(" -|")
    elif " @ " in line:
        left, right = line.split(" @ ", 1)
        title, organization = left.strip(" -|"), right.strip(" -|")
    else:
        org_match = re.search(r"\b(?:Thinking Machines Lab|OpenAI|Google DeepMind|DeepMind|Anthropic|xAI|Mistral AI|Google)\b", line)
        if org_match:
            organization = org_match.group(0).strip()
            title = line.split(organization, 1)[0].strip(" -|,:")
    if not title and not organization:
        return None
    return {key: value for key, value in {"title": title, "organization": organization, "date_range": date_range}.items() if value}


def _looks_like_education_line(line: str) -> bool:
    lower = line.lower()
    return any(token in lower for token in ["university", "college", "institute", "school of", "phd", "b.s", "m.s", "mba", "mit", "cmu", "stanford", "harvard", "berkeley"])


def _looks_like_work_line(line: str) -> bool:
    lower = line.lower()
    return " at " in lower or " @ " in lower or any(
        token in lower
        for token in [
            "member of technical staff",
            "research engineer",
            "research scientist",
            "software engineer",
            "co-founder",
            "cto",
            "engineer",
            "scientist",
        ]
    )


def _extract_date_range(text: str) -> str:
    match = re.search(r"((?:19|20)\d{2}\s*(?:[-–]\s*(?:present|current|(?:19|20)\d{2}))?)", text, flags=re.IGNORECASE)
    return str(match.group(1) or "").strip() if match else ""


def _normalize_url(url: str) -> str:
    return str(url or "").strip()


def _is_google_scholar_profile_url(url: str) -> bool:
    parsed = parse.urlparse(str(url or "").strip())
    domain = parsed.netloc.lower().removeprefix("www.")
    return "scholar.google." in domain and parsed.path.rstrip("/") == "/citations"


def _strip_html_text(value: str) -> str:
    return " ".join(unescape(re.sub(r"<[^>]+>", " ", str(value or ""))).split())


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for item in items:
        normalized = item.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


def _is_meaningful_personal_url(url: str) -> bool:
    lower = str(url or "").strip().lower()
    if not lower.startswith("http"):
        return False
    blocked_tokens = [
        "static.licdn.com",
        "media.licdn.com",
        "github.githubassets.com",
        "github.blog",
        "githubstatus.com",
        "avatars.githubusercontent.com",
        "github-cloud.s3.amazonaws.com",
        "user-images.githubusercontent.com",
        "www.gstatic.com",
        "ssl.gstatic.com",
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "chrome.google.com",
        "google.com/chrome",
        "mozilla.com/firefox",
        "scholar.google.com/schhp",
    ]
    if any(token in lower for token in blocked_tokens):
        return False
    blocked_suffixes = [
        ".css",
        ".js",
        ".png",
        ".jpg",
        ".jpeg",
        ".gif",
        ".webp",
        ".svg",
        ".ico",
        ".pdf",
        ".mp4",
        ".mov",
        ".zip",
        ".gz",
        ".woff",
        ".woff2",
    ]
    return not any(lower.endswith(suffix) for suffix in blocked_suffixes)


def _is_meaningful_github_url(url: str) -> bool:
    parsed = parse.urlparse(str(url or "").strip())
    domain = parsed.netloc.lower().removeprefix("www.")
    if domain != "github.com":
        return False
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts:
        return False
    blocked_first_parts = {
        "about",
        "apps",
        "codespaces",
        "collections",
        "customer-stories",
        "enterprise",
        "events",
        "features",
        "github-copilot",
        "join",
        "login",
        "marketplace",
        "new",
        "notifications",
        "organizations",
        "pricing",
        "search",
        "security",
        "settings",
        "signup",
        "sponsors",
        "topics",
    }
    return path_parts[0].lower() not in blocked_first_parts


def _normalize(value: Any) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _first_nonempty(values: list[Any]) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""
