from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from .asset_logger import AssetLogger
from .domain import Candidate, EvidenceRecord, merge_candidate, make_evidence_id
from .storage import SQLiteStore


class CandidateArtifactError(RuntimeError):
    pass


def materialize_company_candidate_view(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str = "",
) -> dict[str, Any]:
    company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(runtime_dir, target_company, snapshot_id=snapshot_id)
    company_name = str(identity_payload.get("canonical_name") or target_company).strip() or target_company

    merged_candidates: dict[str, Candidate] = {}
    merged_evidence: dict[str, dict[str, Any]] = {}
    source_snapshots = _load_company_history_snapshots(snapshot_dir.parent, company_name)
    for source_snapshot in source_snapshots:
        for candidate in source_snapshot["candidates"]:
            existing = merged_candidates.get(candidate.candidate_id)
            merged_candidates[candidate.candidate_id] = candidate if existing is None else _merge_candidates(existing, candidate, prefer_incoming=True)
        for evidence in source_snapshot["evidence"]:
            merged_evidence[_evidence_key(evidence)] = dict(evidence)

    sqlite_candidates = store.list_candidates_for_company(company_name)
    if not sqlite_candidates and company_name != target_company:
        sqlite_candidates = store.list_candidates_for_company(target_company)
    sqlite_evidence = store.list_evidence_for_company(company_name)
    if not sqlite_evidence and company_name != target_company:
        sqlite_evidence = store.list_evidence_for_company(target_company)

    for candidate in sqlite_candidates:
        existing = merged_candidates.get(candidate.candidate_id)
        merged_candidates[candidate.candidate_id] = candidate if existing is None else _merge_candidates(existing, candidate, prefer_incoming=True)
    for evidence in sqlite_evidence:
        merged_evidence[_evidence_key(evidence)] = dict(evidence)

    materialized_candidates = sorted(merged_candidates.values(), key=lambda item: (item.display_name or item.name_en).lower())
    materialized_evidence = sorted(
        merged_evidence.values(),
        key=lambda item: (
            str(item.get("candidate_id") or ""),
            str(item.get("source_type") or ""),
            str(item.get("title") or ""),
        ),
    )
    return {
        "target_company": company_name,
        "company_key": company_key,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": snapshot_dir,
        "company_identity": identity_payload,
        "candidates": materialized_candidates,
        "evidence": materialized_evidence,
        "source_snapshots": source_snapshots,
        "sqlite_candidate_count": len(sqlite_candidates),
        "sqlite_evidence_count": len(sqlite_evidence),
    }


def build_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str = "",
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    materialized_view = materialize_company_candidate_view(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
    )
    snapshot_dir = Path(materialized_view["snapshot_dir"])
    artifact_dir = Path(output_dir) if output_dir else (snapshot_dir / "normalized_artifacts")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(snapshot_dir)

    candidates = list(materialized_view["candidates"])
    evidence = list(materialized_view["evidence"])
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        evidence_by_candidate[str(item.get("candidate_id") or "").strip()].append(item)

    normalized_candidates: list[dict[str, Any]] = []
    reusable_documents: list[dict[str, Any]] = []
    manual_review_backlog: list[dict[str, Any]] = []
    profile_completion_backlog: list[dict[str, Any]] = []
    status_counter: Counter[str] = Counter()
    profile_detail_count = 0
    manual_review_confirmed_count = 0
    missing_linkedin_count = 0

    for candidate in candidates:
        candidate_evidence = evidence_by_candidate.get(candidate.candidate_id, [])
        normalized = _normalize_candidate(candidate, candidate_evidence)
        normalized_candidates.append(normalized)
        reusable_documents.append(_build_reusable_document(candidate, candidate_evidence, normalized))
        status_counter[normalized["status_bucket"]] += 1
        if normalized["has_profile_detail"]:
            profile_detail_count += 1
        if normalized["manual_review_confirmed"]:
            manual_review_confirmed_count += 1
        if not normalized["has_linkedin_url"]:
            missing_linkedin_count += 1
        if normalized["needs_profile_completion"]:
            profile_completion_backlog.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "status_bucket": normalized["status_bucket"],
                    "category": candidate.category,
                    "employment_status": candidate.employment_status,
                    "reason": "profile_detail_gap",
                    "linkedin_url": candidate.linkedin_url,
                    "source_datasets": normalized["source_datasets"],
                }
            )
        if normalized["needs_manual_review"]:
            manual_review_backlog.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "display_name": candidate.display_name,
                    "status_bucket": normalized["status_bucket"],
                    "category": candidate.category,
                    "employment_status": candidate.employment_status,
                    "reason": normalized["manual_review_reason"],
                    "known_urls": normalized["urls"],
                    "source_datasets": normalized["source_datasets"],
                }
            )

    materialized_documents = {
        "snapshot": {
            "company_key": materialized_view["company_key"],
            "snapshot_id": snapshot_dir.name,
            "snapshot_dir": str(snapshot_dir),
            "materialized_at": _utc_now_iso(),
            "materialized_from": "sqlite_plus_company_snapshots",
            "target_company": materialized_view["target_company"],
            "company_identity": materialized_view["company_identity"],
            "source_snapshots": [
                {
                    "snapshot_id": item["snapshot_id"],
                    "candidate_count": item["candidate_count"],
                    "evidence_count": item["evidence_count"],
                    "source_path": item["source_path"],
                }
                for item in materialized_view["source_snapshots"]
            ],
            "sqlite_candidate_count": materialized_view["sqlite_candidate_count"],
            "sqlite_evidence_count": materialized_view["sqlite_evidence_count"],
        },
        "candidates": [candidate.to_record() for candidate in candidates],
        "evidence": evidence,
    }
    artifact_summary = {
        "target_company": materialized_view["target_company"],
        "company_key": materialized_view["company_key"],
        "snapshot_id": snapshot_dir.name,
        "candidate_count": len(candidates),
        "evidence_count": len(evidence),
        "status_counts": dict(status_counter),
        "profile_detail_count": profile_detail_count,
        "manual_review_confirmed_count": manual_review_confirmed_count,
        "missing_linkedin_count": missing_linkedin_count,
        "manual_review_backlog_count": len(manual_review_backlog),
        "profile_completion_backlog_count": len(profile_completion_backlog),
        "source_snapshot_count": len(materialized_view["source_snapshots"]),
        "sqlite_candidate_count": materialized_view["sqlite_candidate_count"],
        "sqlite_evidence_count": materialized_view["sqlite_evidence_count"],
        "created_at": _utc_now_iso(),
    }

    materialized_path = logger.write_json(
        artifact_dir / "materialized_candidate_documents.json",
        materialized_documents,
        asset_type="materialized_candidate_documents",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    normalized_path = logger.write_json(
        artifact_dir / "normalized_candidates.json",
        normalized_candidates,
        asset_type="normalized_candidates",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    reusable_path = logger.write_json(
        artifact_dir / "reusable_candidate_documents.json",
        reusable_documents,
        asset_type="reusable_candidate_documents",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    backlog_path = logger.write_json(
        artifact_dir / "manual_review_backlog.json",
        manual_review_backlog,
        asset_type="manual_review_backlog",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    profile_completion_backlog_path = logger.write_json(
        artifact_dir / "profile_completion_backlog.json",
        profile_completion_backlog,
        asset_type="profile_completion_backlog",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    summary_path = logger.write_json(
        artifact_dir / "artifact_summary.json",
        artifact_summary,
        asset_type="candidate_artifact_summary",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    return {
        "status": "built",
        "target_company": materialized_view["target_company"],
        "snapshot_id": snapshot_dir.name,
        "artifact_dir": str(artifact_dir),
        "artifact_paths": {
            "materialized_candidate_documents": str(materialized_path),
            "normalized_candidates": str(normalized_path),
            "reusable_candidate_documents": str(reusable_path),
            "manual_review_backlog": str(backlog_path),
            "profile_completion_backlog": str(profile_completion_backlog_path),
            "artifact_summary": str(summary_path),
        },
        "summary": artifact_summary,
    }


def build_evidence_records_from_payloads(payloads: list[dict[str, Any]]) -> list[EvidenceRecord]:
    records: list[EvidenceRecord] = []
    for item in payloads:
        candidate_id = str(item.get("candidate_id") or "").strip()
        source_type = str(item.get("source_type") or "").strip()
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        source_dataset = str(item.get("source_dataset") or "").strip()
        source_path = str(item.get("source_path") or "").strip()
        if not candidate_id or not source_type:
            continue
        evidence_id = str(item.get("evidence_id") or "").strip() or make_evidence_id(
            candidate_id,
            source_dataset or source_type,
            title,
            url or source_path,
        )
        records.append(
            EvidenceRecord(
                evidence_id=evidence_id,
                candidate_id=candidate_id,
                source_type=source_type,
                title=title,
                url=url,
                summary=str(item.get("summary") or "").strip(),
                source_dataset=source_dataset,
                source_path=source_path,
                metadata=dict(item.get("metadata") or {}),
            )
        )
    return records


def _load_company_history_snapshots(company_dir: Path, target_company: str) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if not candidate_doc_path.exists():
            continue
        try:
            payload = json.loads(candidate_doc_path.read_text())
        except json.JSONDecodeError:
            continue
        candidates: list[Candidate] = []
        for item in list(payload.get("candidates") or []):
            candidate = _candidate_from_payload(item)
            if candidate is None:
                continue
            if _normalize_key(candidate.target_company) != _normalize_key(target_company):
                continue
            candidates.append(candidate)
        if not candidates:
            continue
        evidence: list[dict[str, Any]] = []
        for item in list(payload.get("evidence") or []):
            evidence_item = _evidence_payload_from_item(item)
            if evidence_item is not None:
                evidence.append(evidence_item)
        snapshots.append(
            {
                "snapshot_id": snapshot_dir.name,
                "source_path": str(candidate_doc_path),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "candidates": candidates,
                "evidence": evidence,
            }
        )
    return snapshots


def _candidate_from_payload(payload: dict[str, Any]) -> Candidate | None:
    if not isinstance(payload, dict):
        return None
    record = {
        "candidate_id": str(payload.get("candidate_id") or "").strip(),
        "name_en": str(payload.get("name_en") or "").strip(),
        "name_zh": str(payload.get("name_zh") or "").strip(),
        "display_name": str(payload.get("display_name") or "").strip(),
        "category": str(payload.get("category") or "").strip(),
        "target_company": str(payload.get("target_company") or "").strip(),
        "organization": str(payload.get("organization") or "").strip(),
        "employment_status": str(payload.get("employment_status") or "").strip(),
        "role": str(payload.get("role") or "").strip(),
        "team": str(payload.get("team") or "").strip(),
        "joined_at": str(payload.get("joined_at") or "").strip(),
        "left_at": str(payload.get("left_at") or "").strip(),
        "current_destination": str(payload.get("current_destination") or "").strip(),
        "ethnicity_background": str(payload.get("ethnicity_background") or "").strip(),
        "investment_involvement": str(payload.get("investment_involvement") or "").strip(),
        "focus_areas": str(payload.get("focus_areas") or "").strip(),
        "education": str(payload.get("education") or "").strip(),
        "work_history": str(payload.get("work_history") or "").strip(),
        "notes": str(payload.get("notes") or "").strip(),
        "linkedin_url": str(payload.get("linkedin_url") or "").strip(),
        "media_url": str(payload.get("media_url") or "").strip(),
        "source_dataset": str(payload.get("source_dataset") or "").strip(),
        "source_path": str(payload.get("source_path") or "").strip(),
        "metadata": dict(payload.get("metadata") or {}),
    }
    if not record["candidate_id"] or not record["name_en"] or not record["target_company"]:
        return None
    if not record["display_name"]:
        record["display_name"] = record["name_en"]
    return Candidate(**record)


def _evidence_payload_from_item(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    candidate_id = str(payload.get("candidate_id") or "").strip()
    source_type = str(payload.get("source_type") or "").strip()
    title = str(payload.get("title") or "").strip()
    url = str(payload.get("url") or "").strip()
    source_dataset = str(payload.get("source_dataset") or "").strip()
    source_path = str(payload.get("source_path") or "").strip()
    if not candidate_id or not source_type:
        return None
    return {
        "evidence_id": str(payload.get("evidence_id") or "").strip() or make_evidence_id(
            candidate_id,
            source_dataset or source_type,
            title,
            url or source_path,
        ),
        "candidate_id": candidate_id,
        "source_type": source_type,
        "title": title,
        "url": url,
        "summary": str(payload.get("summary") or "").strip(),
        "source_dataset": source_dataset,
        "source_path": source_path,
        "metadata": dict(payload.get("metadata") or {}),
    }


def _merge_candidates(existing: Candidate, incoming: Candidate, *, prefer_incoming: bool) -> Candidate:
    if prefer_incoming:
        return merge_candidate(incoming, existing)
    return merge_candidate(existing, incoming)


def _evidence_key(payload: dict[str, Any]) -> str:
    evidence_id = str(payload.get("evidence_id") or "").strip()
    if evidence_id:
        return evidence_id
    return make_evidence_id(
        str(payload.get("candidate_id") or "").strip(),
        str(payload.get("source_dataset") or payload.get("source_type") or "").strip(),
        str(payload.get("title") or "").strip(),
        str(payload.get("url") or payload.get("source_path") or "").strip(),
    )


def _normalize_candidate(candidate: Candidate, evidence: list[dict[str, Any]]) -> dict[str, Any]:
    source_datasets = sorted(
        {
            str(item).strip()
            for item in [candidate.source_dataset] + [str(item.get("source_dataset") or "").strip() for item in evidence]
            if str(item).strip()
        }
    )
    source_types = sorted({str(item.get("source_type") or "").strip() for item in evidence if str(item.get("source_type") or "").strip()})
    urls = _dedupe_urls(
        [candidate.linkedin_url, candidate.media_url]
        + [str(item.get("url") or "").strip() for item in evidence]
    )
    manual_review_confirmed = any(item.get("source_dataset") in {"manual_review", "manual_review_analysis"} for item in evidence) and candidate.category != "lead"
    has_profile_detail = bool(candidate.education or candidate.work_history) or any(
        str(item.get("source_type") or "").strip() in {"linkedin_profile_detail", "manual_review_analysis"}
        for item in evidence
    )
    has_linkedin_url = bool(str(candidate.linkedin_url or "").strip())
    status_bucket = _status_bucket(candidate)
    needs_profile_completion = has_linkedin_url and not has_profile_detail
    needs_manual_review = status_bucket == "lead" or (not has_linkedin_url and not manual_review_confirmed)
    reason = ""
    if status_bucket == "lead":
        reason = "unresolved_lead"
    elif not has_linkedin_url and not manual_review_confirmed:
        reason = "missing_linkedin"
    elif not has_profile_detail:
        reason = "profile_detail_gap"
    return {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "name_en": candidate.name_en,
        "category": candidate.category,
        "employment_status": candidate.employment_status,
        "status_bucket": status_bucket,
        "role": candidate.role,
        "team": candidate.team,
        "focus_areas": candidate.focus_areas,
        "has_linkedin_url": has_linkedin_url,
        "has_profile_detail": has_profile_detail,
        "needs_profile_completion": needs_profile_completion,
        "manual_review_confirmed": manual_review_confirmed,
        "needs_manual_review": needs_manual_review,
        "manual_review_reason": reason,
        "evidence_count": len(evidence),
        "source_datasets": source_datasets,
        "source_types": source_types,
        "urls": urls[:12],
        "source_path": candidate.source_path,
    }


def _build_reusable_document(candidate: Candidate, evidence: list[dict[str, Any]], normalized: dict[str, Any]) -> dict[str, Any]:
    evidence_summaries = [str(item.get("summary") or "").strip() for item in evidence if str(item.get("summary") or "").strip()]
    evidence_titles = [str(item.get("title") or "").strip() for item in evidence if str(item.get("title") or "").strip()]
    profile_document = " | ".join(
        part
        for part in [
            candidate.display_name,
            candidate.role,
            candidate.team,
            candidate.focus_areas,
            candidate.education,
            candidate.work_history,
            candidate.notes,
        ]
        if str(part or "").strip()
    )
    evidence_document = " | ".join(evidence_summaries[:8] or evidence_titles[:8])
    semantic_document = " | ".join(
        part
        for part in [
            candidate.display_name,
            normalized["status_bucket"],
            candidate.role,
            candidate.team,
            candidate.focus_areas,
            candidate.education,
            candidate.work_history,
            candidate.notes,
            evidence_document,
        ]
        if str(part or "").strip()
    )
    return {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "status_bucket": normalized["status_bucket"],
        "manual_review_confirmed": normalized["manual_review_confirmed"],
        "has_profile_detail": normalized["has_profile_detail"],
        "has_linkedin_url": normalized["has_linkedin_url"],
        "needs_profile_completion": normalized["needs_profile_completion"],
        "source_datasets": normalized["source_datasets"],
        "source_types": normalized["source_types"],
        "urls": normalized["urls"],
        "profile_document": profile_document,
        "evidence_document": evidence_document,
        "semantic_document": semantic_document,
    }


def _resolve_company_snapshot(runtime_dir: str | Path, target_company: str, *, snapshot_id: str = "") -> tuple[str, Path, dict[str, Any]]:
    root = Path(runtime_dir)
    company_assets_dir = root / "company_assets"
    normalized_target = _normalize_key(target_company)
    company_dir = None
    for candidate in sorted(company_assets_dir.iterdir()) if company_assets_dir.exists() else []:
        if candidate.is_dir() and _normalize_key(candidate.name) == normalized_target:
            company_dir = candidate
            break
    if company_dir is None:
        raise CandidateArtifactError(f"Company assets not found for {target_company}")
    latest_path = company_dir / "latest_snapshot.json"
    latest_payload = json.loads(latest_path.read_text()) if latest_path.exists() else {}
    resolved_snapshot_id = str(snapshot_id or latest_payload.get("snapshot_id") or "").strip()
    if not resolved_snapshot_id:
        snapshot_dirs = sorted(path.name for path in company_dir.iterdir() if path.is_dir())
        if not snapshot_dirs:
            raise CandidateArtifactError(f"No snapshots found under {company_dir}")
        resolved_snapshot_id = snapshot_dirs[-1]
    snapshot_dir = company_dir / resolved_snapshot_id
    if not snapshot_dir.exists():
        raise CandidateArtifactError(f"Snapshot not found: {snapshot_dir}")
    return company_dir.name, snapshot_dir, dict(latest_payload.get("company_identity") or {})


def _status_bucket(candidate: Candidate) -> str:
    status = str(candidate.employment_status or "").strip().lower()
    category = str(candidate.category or "").strip().lower()
    if status == "current":
        return "current"
    if status == "former":
        return "former"
    if category == "lead":
        return "lead"
    return category or "unknown"


def _dedupe_urls(items: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        results.append(value)
    return results


def _normalize_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
