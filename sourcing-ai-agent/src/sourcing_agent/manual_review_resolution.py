from __future__ import annotations

from hashlib import sha1
import json
from pathlib import Path
from typing import Any

from .asset_logger import AssetLogger
from .asset_policy import is_binary_like_url
from .domain import Candidate, EvidenceRecord, format_display_name, make_evidence_id
from .exploratory_enrichment import build_page_analysis_input, extract_page_signals
from .model_provider import DeterministicModelClient, ModelClient
from .storage import SQLiteStore
from .web_fetch import fetch_text_url


def apply_manual_review_resolution(
    *,
    runtime_dir: Path,
    store: SQLiteStore,
    payload: dict[str, Any],
    review_item: dict[str, Any] | None,
    model_client: ModelClient | None = None,
) -> dict[str, Any]:
    client = model_client or DeterministicModelClient()
    candidate = _resolve_candidate(store, payload, review_item)
    if candidate is None:
        return {"status": "candidate_not_found", "candidate_id": str(payload.get("candidate_id") or "")}

    target_company = str(payload.get("target_company") or candidate.target_company or "").strip()
    review_item_id = int(payload.get("review_item_id") or 0)
    review_root = runtime_dir / "manual_review_assets" / _company_key(target_company) / candidate.candidate_id
    review_key = sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:10]
    review_dir = review_root / f"review_{review_item_id or 'adhoc'}_{review_key}"
    logger = AssetLogger(review_dir)
    logger.write_json(
        review_dir / "resolution_input.json",
        {
            "payload": payload,
            "review_item": review_item or {},
            "candidate_before": candidate.to_record(),
        },
        asset_type="manual_review_resolution_input",
        source_kind="manual_review",
        is_raw_asset=False,
        model_safe=True,
    )

    signals = {
        "linkedin_urls": [],
        "x_urls": [],
        "github_urls": [],
        "personal_urls": [],
        "resume_urls": [],
        "descriptions": [],
        "validated_summaries": [],
        "role_signals": [],
        "analysis_notes": [],
    }
    added_evidence: list[EvidenceRecord] = []
    source_links = _normalize_source_links(payload)
    fetch_assets = bool(payload.get("fetch_assets", True))

    for index, item in enumerate(source_links, start=1):
        link_label = str(item.get("label") or f"Manual Review Source {index}").strip()
        url = str(item.get("url") or "").strip()
        source_type = str(item.get("source_type") or "manual_review_source_link").strip()
        notes = str(item.get("notes") or "").strip()
        if not url:
            continue
        manifest_path = logger.write_json(
            review_dir / "sources" / f"source_{index:02d}.json",
            item,
            asset_type="manual_review_source_manifest",
            source_kind="manual_review",
            is_raw_asset=False,
            model_safe=True,
        )
        added_evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, source_type, link_label, url),
                candidate_id=candidate.candidate_id,
                source_type=source_type,
                title=link_label,
                url=url,
                summary=notes or f"Manual review provided {link_label.lower()} for {candidate.display_name}.",
                source_dataset="manual_review",
                source_path=str(manifest_path),
                metadata={"review_item_id": review_item_id, "provided": True},
            )
        )
        if "linkedin.com/in/" in url and url not in signals["linkedin_urls"]:
            signals["linkedin_urls"].append(url)
        if any(token in url.lower() for token in ["/cv", "/resume", ".pdf"]):
            if url not in signals["resume_urls"]:
                signals["resume_urls"].append(url)
        if not fetch_assets or is_binary_like_url(url):
            continue
        try:
            fetched = fetch_text_url(url, timeout=30, source_label="manual_review")
        except Exception as exc:
            signals["analysis_notes"].append(f"fetch_failed:{url}:{str(exc)[:160]}")
            continue
        raw_path = logger.write_text(
            review_dir / "sources" / f"source_{index:02d}.html",
            fetched.text,
            asset_type="manual_review_source_html",
            source_kind="manual_review",
            content_type=fetched.content_type or "text/html",
            is_raw_asset=True,
            model_safe=False,
            metadata={"source_url": url, "final_url": fetched.final_url},
        )
        page_signals = extract_page_signals(fetched.text, fetched.final_url)
        _merge_signal_map(signals, page_signals)
        analysis_input = build_page_analysis_input(
            candidate=candidate,
            target_company=target_company,
            source_url=fetched.final_url,
            html_text=fetched.text,
            extracted_links=page_signals,
        )
        analysis_input_path = logger.write_json(
            review_dir / "analysis" / f"source_{index:02d}_input.json",
            analysis_input,
            asset_type="manual_review_analysis_input",
            source_kind="manual_review",
            is_raw_asset=False,
            model_safe=True,
            metadata={"source_url": fetched.final_url},
        )
        analysis = client.analyze_page_asset(analysis_input)
        analysis_path = logger.write_json(
            review_dir / "analysis" / f"source_{index:02d}.json",
            analysis,
            asset_type="manual_review_analysis_output",
            source_kind="manual_review",
            is_raw_asset=False,
            model_safe=True,
            metadata={"source_url": fetched.final_url},
        )
        summary = str(analysis.get("summary") or "").strip()
        if summary and summary not in signals["validated_summaries"]:
            signals["validated_summaries"].append(summary)
        for role_signal in list(analysis.get("role_signals") or []):
            normalized_signal = str(role_signal or "").strip()
            if normalized_signal and normalized_signal not in signals["role_signals"]:
                signals["role_signals"].append(normalized_signal)
        signals["analysis_notes"].append(f"analyzed:{fetched.final_url}")
        added_evidence.append(
            EvidenceRecord(
                evidence_id=make_evidence_id(candidate.candidate_id, "manual_review_analysis", link_label, fetched.final_url),
                candidate_id=candidate.candidate_id,
                source_type="manual_review_analysis",
                title=f"{link_label} analysis",
                url=fetched.final_url,
                summary=summary or "Manual review page analyzed for additional identity signals.",
                source_dataset="manual_review_analysis",
                source_path=str(analysis_path),
                metadata={
                    "analysis_input_path": str(analysis_input_path),
                    "raw_path": str(raw_path),
                    "role_signals": signals["role_signals"][:8],
                },
            )
        )

    candidate_patch = dict(payload.get("candidate_patch") or {})
    if not str(candidate_patch.get("linkedin_url") or "").strip() and signals["linkedin_urls"]:
        candidate_patch["linkedin_url"] = signals["linkedin_urls"][0]
    if not str(candidate_patch.get("media_url") or "").strip():
        media_url = _pick_media_url(signals)
        if media_url:
            candidate_patch["media_url"] = media_url

    patched_candidate = _apply_candidate_patch(
        candidate,
        candidate_patch,
        review_dir=review_dir,
        notes=str(payload.get("notes") or "").strip(),
        signals=signals,
        source_links=source_links,
    )
    saved_candidate = store.upsert_candidate(patched_candidate)
    stored_evidence = store.upsert_evidence_records(added_evidence)

    metadata = {
        "asset_root": str(review_dir),
        "source_links": source_links,
        "fetched_asset_count": len(list((review_dir / "sources").glob("*"))) if (review_dir / "sources").exists() else 0,
        "analysis_count": len(list((review_dir / "analysis").glob("*.json"))) if (review_dir / "analysis").exists() else 0,
        "candidate_patch": candidate_patch,
        "resolved_signals": signals,
    }
    return {
        "status": "applied",
        "candidate": saved_candidate.to_record(),
        "evidence": stored_evidence,
        "metadata": metadata,
        "artifact_root": str(review_dir),
    }


def _resolve_candidate(store: SQLiteStore, payload: dict[str, Any], review_item: dict[str, Any] | None) -> Candidate | None:
    candidate_id = str(payload.get("candidate_id") or (review_item or {}).get("candidate_id") or "").strip()
    if candidate_id:
        candidate = store.get_candidate(candidate_id)
        if candidate is not None:
            return candidate
    target_company = str(payload.get("target_company") or (review_item or {}).get("target_company") or "").strip()
    candidate_name = str(payload.get("candidate_name") or "").strip()
    if not candidate_name and review_item:
        candidate_name = str(((review_item.get("candidate") or {}).get("name_en") or "")).strip()
    if candidate_name and target_company:
        candidate = store.find_candidate_by_name(target_company=target_company, name_en=candidate_name)
        if candidate is not None:
            return candidate
    snapshot = dict((review_item or {}).get("candidate") or payload.get("candidate") or {})
    if snapshot:
        return Candidate(**snapshot)
    return None


def _normalize_source_links(payload: dict[str, Any]) -> list[dict[str, Any]]:
    links = payload.get("source_links") or payload.get("links") or []
    normalized: list[dict[str, Any]] = []
    for item in links:
        if isinstance(item, str):
            candidate = {"url": item}
        elif isinstance(item, dict):
            candidate = dict(item)
        else:
            continue
        url = str(candidate.get("url") or "").strip()
        if not url:
            continue
        normalized.append(
            {
                "label": str(candidate.get("label") or candidate.get("title") or "").strip(),
                "url": url,
                "source_type": str(candidate.get("source_type") or "").strip(),
                "notes": str(candidate.get("notes") or "").strip(),
            }
        )
    return normalized


def _apply_candidate_patch(
    candidate: Candidate,
    patch: dict[str, Any],
    *,
    review_dir: Path,
    notes: str,
    signals: dict[str, list[str]],
    source_links: list[dict[str, Any]],
) -> Candidate:
    record = candidate.to_record()
    for field_name in [
        "name_en",
        "name_zh",
        "display_name",
        "category",
        "target_company",
        "organization",
        "employment_status",
        "role",
        "team",
        "joined_at",
        "left_at",
        "current_destination",
        "ethnicity_background",
        "investment_involvement",
        "focus_areas",
        "education",
        "work_history",
        "linkedin_url",
        "media_url",
        "source_dataset",
        "source_path",
    ]:
        if field_name in patch:
            record[field_name] = str(patch.get(field_name) or "").strip()

    note_parts = [str(candidate.notes or "").strip(), notes]
    note_parts.extend(signals.get("validated_summaries", [])[:2])
    record["notes"] = " | ".join(part for part in note_parts if part)
    metadata = dict(candidate.metadata)
    metadata.update(dict(patch.get("metadata") or {}))
    metadata["manual_review_links"] = source_links
    metadata["manual_review_artifact_root"] = str(review_dir)
    metadata["manual_review_signals"] = {
        "linkedin_urls": signals.get("linkedin_urls", [])[:3],
        "x_urls": signals.get("x_urls", [])[:3],
        "github_urls": signals.get("github_urls", [])[:3],
        "personal_urls": signals.get("personal_urls", [])[:3],
        "resume_urls": signals.get("resume_urls", [])[:3],
        "role_signals": signals.get("role_signals", [])[:8],
    }
    if not str(record.get("linkedin_url") or "").strip() and signals.get("linkedin_urls"):
        record["linkedin_url"] = signals["linkedin_urls"][0]
        metadata["profile_url"] = signals["linkedin_urls"][0]
    if not str(record.get("media_url") or "").strip():
        media_url = _pick_media_url(signals)
        if media_url:
            record["media_url"] = media_url
    if not str(record.get("display_name") or "").strip():
        record["display_name"] = format_display_name(str(record.get("name_en") or ""), str(record.get("name_zh") or ""))
    if not str(record.get("source_dataset") or "").strip():
        record["source_dataset"] = "manual_review"
    record["source_path"] = str(review_dir)
    record["metadata"] = metadata
    return Candidate(**record)


def _merge_signal_map(target: dict[str, list[str]], incoming: dict[str, list[str]]) -> None:
    for key, values in incoming.items():
        current = target.setdefault(key, [])
        for value in values:
            normalized = str(value or "").strip()
            if normalized and normalized not in current:
                current.append(normalized)


def _pick_media_url(signals: dict[str, list[str]]) -> str:
    for key in ["x_urls", "github_urls", "personal_urls", "resume_urls"]:
        values = list(signals.get(key) or [])
        if values:
            return str(values[0] or "").strip()
    return ""


def _company_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum()) or "unknown"
