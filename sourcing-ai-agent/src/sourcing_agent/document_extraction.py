from __future__ import annotations

from dataclasses import dataclass
from html import unescape
from io import BytesIO
import json
from pathlib import Path
import re
import sys
from typing import TYPE_CHECKING, Any
from urllib import parse

from .asset_logger import AssetLogger
from .asset_policy import DEFAULT_MODEL_CONTEXT_CHAR_LIMIT, RAW_ASSET_POLICY_SUMMARY
from .domain import Candidate
from .web_fetch import fetch_binary_url, fetch_text_url

if TYPE_CHECKING:
    from .model_provider import ModelClient


_VENDOR_DIR = Path(__file__).resolve().parents[1] / "_vendor"
if _VENDOR_DIR.exists():
    vendor_path = str(_VENDOR_DIR)
    if vendor_path not in sys.path:
        sys.path.insert(0, vendor_path)

try:  # pragma: no cover - exercised in live/runtime paths
    from pypdf import PdfReader
except Exception:  # pragma: no cover - graceful fallback when vendored dep is unavailable
    PdfReader = None


STRUCTURED_SIGNAL_KEYS = ("education_signals", "work_history_signals", "affiliation_signals")


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
    github_urls = _dedupe([link for link in absolute_links if "github.com/" in link])
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
    return bundle


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
        },
    }


def analyze_remote_document(
    *,
    candidate: Candidate,
    target_company: str,
    source_url: str,
    asset_dir: Path,
    asset_logger: AssetLogger,
    model_client: ModelClient,
    source_kind: str,
    asset_prefix: str,
    timeout: int = 30,
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
    elif source_url.lower().endswith(".pdf"):
        fetched = fetch_binary_url(source_url, timeout=timeout, source_label=source_kind)
        document_type = "pdf_resume"
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
        extracted_text = extract_pdf_text(fetched.content)
        extracted_text_file = asset_logger.write_text(
            asset_dir / f"{asset_prefix}_extracted.txt",
            extracted_text,
            asset_type="document_pdf_extracted_text",
            source_kind=source_kind,
            content_type="text/plain",
            is_raw_asset=False,
            model_safe=True,
            metadata={"source_url": source_url, "final_url": fetched.final_url, "document_type": document_type},
        )
        signals = empty_signal_bundle()
        if source_url not in signals["resume_urls"]:
            signals["resume_urls"].append(source_url)
        analysis_input = build_text_analysis_input(
            candidate=candidate,
            target_company=target_company,
            source_url=source_url,
            text=extracted_text,
            extracted_links=signals,
            title=Path(parse.urlparse(source_url).path).name or "Resume PDF",
            description="Extracted PDF resume text",
            content_type=fetched.content_type or "application/pdf",
            document_type=document_type,
        )
        extracted_text_path = str(extracted_text_file)
        title = Path(parse.urlparse(source_url).path).name or "Resume PDF"
        description = "Extracted PDF resume text"
        final_url = fetched.final_url
        content_type = fetched.content_type or "application/pdf"
    else:
        fetched = fetch_text_url(source_url, timeout=timeout, source_label=source_kind)
        page_html = fetched.text
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
    analysis = normalize_analysis(model_client.analyze_page_asset(analysis_input))
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
    recommended_links = dict(payload.get("recommended_links") or {})
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
        "www.gstatic.com",
        "ssl.gstatic.com",
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "chrome.google.com",
    ]
    if any(token in lower for token in blocked_tokens):
        return False
    blocked_suffixes = [
        ".css",
        ".js",
        ".png",
        ".jpg",
        ".jpeg",
        ".svg",
        ".ico",
        ".woff",
        ".woff2",
    ]
    return not any(lower.endswith(suffix) for suffix in blocked_suffixes)


def _normalize(value: Any) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _first_nonempty(values: list[Any]) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""
