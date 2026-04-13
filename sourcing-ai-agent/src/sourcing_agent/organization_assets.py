from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha1
import json
from pathlib import Path
from typing import Any

from .asset_logger import AssetLogger
from .candidate_artifacts import (
    CandidateArtifactError,
    _load_company_snapshot_identity,
    _resolve_company_snapshot,
    load_company_snapshot_candidate_documents,
)
from .company_registry import normalize_company_key
from .domain import candidate_searchable_text, normalize_name_token
from .storage import SQLiteStore


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text())


def _normalized_artifact_dir(snapshot_dir: Path, asset_view: str) -> Path:
    base_dir = snapshot_dir / "normalized_artifacts"
    normalized_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    if normalized_view == "canonical_merged":
        return base_dir
    return base_dir / normalized_view


def _company_artifact_summary_path(snapshot_dir: Path, asset_view: str) -> Path:
    return _normalized_artifact_dir(snapshot_dir, asset_view) / "artifact_summary.json"


def _completeness_ledger_path(snapshot_dir: Path, asset_view: str) -> Path:
    return _normalized_artifact_dir(snapshot_dir, asset_view) / "organization_completeness_ledger.json"


def _shard_bundle_manifest_path(snapshot_dir: Path, asset_view: str) -> Path:
    return _normalized_artifact_dir(snapshot_dir, asset_view) / "acquisition_shard_bundles" / "manifest.json"


def _shard_bundle_dir(snapshot_dir: Path, asset_view: str, shard_key: str) -> Path:
    return _normalized_artifact_dir(snapshot_dir, asset_view) / "acquisition_shard_bundles" / shard_key


def _normalize_query_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_string_list(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    normalized: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = _normalize_query_text(item)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        normalized.append(text)
    return normalized


_PROFILE_DETAIL_METADATA_FIELDS = ("headline", "summary", "about", "languages", "skills", "public_identifier")


def _entry_linkedin_url(entry: dict[str, Any]) -> str:
    for key in ("linkedin_url", "linkedinUrl", "linkedInUrl", "profile_url", "profileUrl", "url"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    meta = dict(entry.get("_meta") or {})
    for key in ("linkedin_url", "linkedinUrl", "linkedInUrl", "profile_url", "profileUrl", "url"):
        value = str(meta.get(key) or "").strip()
        if value:
            return value
    return ""


def _entry_display_name(entry: dict[str, Any]) -> str:
    for key in ("display_name", "displayName", "full_name", "fullName", "name"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    first_name = str(entry.get("first_name") or entry.get("firstName") or "").strip()
    last_name = str(entry.get("last_name") or entry.get("lastName") or "").strip()
    combined = " ".join(part for part in [first_name, last_name] if part).strip()
    if combined:
        return combined
    return ""


def _entry_current_positions(entry: dict[str, Any]) -> list[dict[str, Any]]:
    positions = entry.get("currentPositions")
    if isinstance(positions, list):
        return [dict(item) for item in positions if isinstance(item, dict)]
    return []


def _entry_role(entry: dict[str, Any]) -> str:
    for key in ("headline", "occupation", "title", "role"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    positions = _entry_current_positions(entry)
    if positions:
        first = positions[0]
        title = str(first.get("title") or first.get("position") or "").strip()
        company = str(first.get("companyName") or first.get("company") or "").strip()
        if title and company:
            return f"{title} at {company}"
        if title:
            return title
    return ""


def _entry_location(entry: dict[str, Any]) -> str:
    location = entry.get("location")
    if isinstance(location, dict):
        for key in ("linkedinText", "full", "name", "text"):
            value = str(location.get(key) or "").strip()
            if value:
                return value
    return str(entry.get("location") or entry.get("location_normalized") or "").strip()


def _entry_summary(entry: dict[str, Any]) -> str:
    for key in ("summary", "about", "description"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    return ""


def _entry_company_name(entry: dict[str, Any]) -> str:
    for key in ("current_company_name", "currentCompanyName", "companyName"):
        value = str(entry.get(key) or "").strip()
        if value:
            return value
    positions = _entry_current_positions(entry)
    if positions:
        value = str(positions[0].get("companyName") or positions[0].get("company") or "").strip()
        if value:
            return value
    return ""


def _entry_searchable_text(entry: dict[str, Any]) -> str:
    parts = [
        _entry_display_name(entry),
        _entry_role(entry),
        _entry_company_name(entry),
        _entry_location(entry),
        _entry_summary(entry),
    ]
    return " | ".join(part for part in parts if part)


def _stub_candidate_id(target_company: str, shard_key: str, entry: dict[str, Any]) -> str:
    url = _entry_linkedin_url(entry)
    display_name = _entry_display_name(entry)
    payload = "|".join(
        [
            normalize_company_key(target_company),
            shard_key,
            normalize_name_token(display_name),
            url.strip().lower(),
        ]
    )
    return sha1(payload.encode("utf-8")).hexdigest()[:16]


def _normalize_linkedin_url_key(store: SQLiteStore, url: str) -> str:
    normalized = store.normalize_linkedin_profile_url(str(url or "").strip())
    if normalized:
        return normalized
    return str(url or "").strip().lower()


def _build_candidate_indexes(
    *,
    store: SQLiteStore,
    candidates: list[Any],
    evidence: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    candidate_by_url: dict[str, dict[str, Any]] = {}
    candidate_by_name: dict[str, dict[str, Any]] = {}
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in list(evidence or []):
        candidate_id = str(item.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        evidence_by_candidate[candidate_id].append(dict(item))
    for candidate in list(candidates or []):
        record = candidate.to_record() if hasattr(candidate, "to_record") else dict(candidate)
        candidate_id = str(record.get("candidate_id") or "").strip()
        if not candidate_id:
            continue
        linkedin_url = str(record.get("linkedin_url") or "").strip()
        if linkedin_url:
            candidate_by_url[_normalize_linkedin_url_key(store, linkedin_url)] = record
        display_name = str(record.get("display_name") or record.get("name_en") or "").strip()
        if display_name:
            candidate_by_name[normalize_name_token(display_name)] = record
    return candidate_by_url, evidence_by_candidate, candidate_by_name


def _load_shard_entries(shard_row: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    metadata = dict(shard_row.get("metadata") or {})
    source_candidates = [
        str(metadata.get("raw_path") or "").strip(),
        str(shard_row.get("source_path") or "").strip(),
        str(metadata.get("response_path") or "").strip(),
        str(metadata.get("summary_path") or "").strip(),
    ]
    lane = str(shard_row.get("lane") or "").strip().lower()
    for candidate in source_candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if not path.exists():
            continue
        resolved = path
        if lane == "company_employees" and path.name == "harvest_company_employees_summary.json":
            resolved = path.with_name("harvest_company_employees_visible.json")
        try:
            payload = _json_load(resolved)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(payload, list):
            entries = [dict(item) for item in payload if isinstance(item, dict)]
            return entries, str(resolved)
        if isinstance(payload, dict):
            for key in ("items", "entries", "profiles"):
                values = payload.get(key)
                if isinstance(values, list):
                    entries = [dict(item) for item in values if isinstance(item, dict)]
                    return entries, str(resolved)
    return [], ""


def _candidate_has_profile_detail(candidate: Any) -> bool:
    if str(getattr(candidate, "education", "") or "").strip() or str(getattr(candidate, "work_history", "") or "").strip():
        return True
    metadata = dict(getattr(candidate, "metadata", {}) or {})
    for field_name in _PROFILE_DETAIL_METADATA_FIELDS:
        value = metadata.get(field_name)
        if isinstance(value, list):
            if any(str(item or "").strip() for item in value):
                return True
            continue
        if isinstance(value, dict):
            if value:
                return True
            continue
        if str(value or "").strip():
            return True
    return False


def _candidate_has_explicit_profile_capture(candidate: Any) -> bool:
    metadata = dict(getattr(candidate, "metadata", {}) or {})
    for field_name in ("profile_url", "public_identifier", "raw_linkedin_url", "sanity_linkedin_url"):
        if str(metadata.get(field_name) or "").strip():
            return True
    return False


def _candidate_needs_manual_review(candidate: Any) -> bool:
    metadata = dict(getattr(candidate, "metadata", {}) or {})
    if not bool(metadata.get("membership_review_required")):
        return False
    decision = str(metadata.get("membership_review_decision") or "").strip().lower()
    return decision not in {"manual_confirmed_member", "manual_non_member"}


def load_company_snapshot_registry_summary(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    try:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(runtime_dir, target_company, snapshot_id=snapshot_id)
    except CandidateArtifactError:
        return {}

    summary_path = _company_artifact_summary_path(snapshot_dir, asset_view)
    if summary_path.exists():
        try:
            summary = _json_load(summary_path)
        except (OSError, ValueError, json.JSONDecodeError):
            summary = {}
        if isinstance(summary, dict) and summary:
            return {
                "summary": summary,
                "source_path": str(summary_path),
                "snapshot_dir": str(snapshot_dir),
                "identity_payload": identity_payload,
                "company_key": str(identity_payload.get("company_key") or company_key).strip() or company_key,
            }

    try:
        candidate_payload = load_company_snapshot_candidate_documents(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            view=asset_view,
        )
    except CandidateArtifactError:
        return {}

    raw_payload: dict[str, Any] = {}
    source_path = Path(str(candidate_payload.get("source_path") or "")).expanduser()
    if source_path.exists():
        try:
            loaded_payload = _json_load(source_path)
            if isinstance(loaded_payload, dict):
                raw_payload = loaded_payload
        except (OSError, ValueError, json.JSONDecodeError):
            raw_payload = {}

    snapshot_metadata = dict(raw_payload.get("snapshot") or {})
    source_snapshot_selection = dict(snapshot_metadata.get("source_snapshot_selection") or {})
    selected_snapshot_ids = _normalize_string_list(
        source_snapshot_selection.get("selected_snapshot_ids")
        or [item.get("snapshot_id") for item in list(snapshot_metadata.get("source_snapshots") or []) if isinstance(item, dict)]
        or [snapshot_id]
    )
    candidates = list(candidate_payload.get("candidates") or [])
    evidence = list(candidate_payload.get("evidence") or [])
    profile_detail_count = sum(1 for candidate in candidates if _candidate_has_profile_detail(candidate))
    explicit_profile_capture_count = sum(1 for candidate in candidates if _candidate_has_explicit_profile_capture(candidate))
    missing_linkedin_count = sum(1 for candidate in candidates if not str(getattr(candidate, "linkedin_url", "") or "").strip())
    manual_review_backlog_count = sum(1 for candidate in candidates if _candidate_needs_manual_review(candidate))
    profile_completion_backlog_count = sum(
        1
        for candidate in candidates
        if str(getattr(candidate, "linkedin_url", "") or "").strip() and not _candidate_has_profile_detail(candidate)
    )
    summary = {
        "target_company": str(candidate_payload.get("target_company") or target_company),
        "company_key": str(candidate_payload.get("company_key") or company_key),
        "snapshot_id": snapshot_dir.name,
        "asset_view": asset_view,
        "candidate_count": len(candidates),
        "evidence_count": len(evidence),
        "profile_detail_count": profile_detail_count,
        "explicit_profile_capture_count": explicit_profile_capture_count,
        "missing_linkedin_count": missing_linkedin_count,
        "manual_review_backlog_count": manual_review_backlog_count,
        "profile_completion_backlog_count": profile_completion_backlog_count,
        "source_snapshot_count": max(len(selected_snapshot_ids), len(list(snapshot_metadata.get("source_snapshots") or [])), 1),
        "source_snapshot_selection": source_snapshot_selection
        or {
            "mode": "candidate_documents_fallback",
            "selected_snapshot_ids": selected_snapshot_ids,
            "reason": "artifact_summary_missing_used_candidate_documents",
        },
        "selected_snapshot_ids": selected_snapshot_ids,
    }
    return {
        "summary": summary,
        "source_path": str(source_path) if source_path else str(candidate_payload.get("source_path") or ""),
        "snapshot_dir": str(snapshot_dir),
        "identity_payload": identity_payload,
        "company_key": str(identity_payload.get("company_key") or company_key).strip() or company_key,
    }


def _summarize_inferred_baseline_lane_coverage(
    candidate_payload: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    def _blank_lane_summary() -> dict[str, Any]:
        return {
            "inferred_candidate_count": 0,
            "inferred_profile_detail_count": 0,
            "inferred_linkedin_url_count": 0,
            "inferred_status_counts": {},
            "inferred_category_counts": {},
            "inferred_profile_detail_ratio": 0.0,
            "inferred_ready": False,
            "inferred_source_kind": "",
            "inferred_source_path": "",
        }

    current_summary = _blank_lane_summary()
    former_summary = _blank_lane_summary()
    if not isinstance(candidate_payload, dict):
        return current_summary, former_summary

    source_kind = str(candidate_payload.get("source_kind") or "").strip()
    source_path = str(candidate_payload.get("source_path") or "").strip()
    current_status_counts: dict[str, int] = defaultdict(int)
    current_category_counts: dict[str, int] = defaultdict(int)
    former_status_counts: dict[str, int] = defaultdict(int)
    former_category_counts: dict[str, int] = defaultdict(int)

    for candidate in list(candidate_payload.get("candidates") or []):
        category = str(getattr(candidate, "category", "") or "").strip().lower()
        employment_status = str(getattr(candidate, "employment_status", "") or "").strip().lower()
        if category not in {"employee", "former_employee", "investor"} and employment_status not in {"current", "former"}:
            continue
        lane_summary = former_summary if (employment_status == "former" or category == "former_employee") else current_summary
        status_counts = former_status_counts if lane_summary is former_summary else current_status_counts
        category_counts = former_category_counts if lane_summary is former_summary else current_category_counts
        lane_summary["inferred_candidate_count"] += 1
        if str(getattr(candidate, "linkedin_url", "") or "").strip():
            lane_summary["inferred_linkedin_url_count"] += 1
        if _candidate_has_profile_detail(candidate):
            lane_summary["inferred_profile_detail_count"] += 1
        status_key = employment_status or "unknown"
        category_key = category or "unknown"
        status_counts[status_key] += 1
        category_counts[category_key] += 1

    for lane_summary, status_counts, category_counts in [
        (current_summary, current_status_counts, current_category_counts),
        (former_summary, former_status_counts, former_category_counts),
    ]:
        inferred_candidate_count = int(lane_summary.get("inferred_candidate_count") or 0)
        inferred_profile_detail_count = int(lane_summary.get("inferred_profile_detail_count") or 0)
        lane_summary["inferred_status_counts"] = dict(status_counts)
        lane_summary["inferred_category_counts"] = dict(category_counts)
        lane_summary["inferred_profile_detail_ratio"] = round(
            (inferred_profile_detail_count / inferred_candidate_count) if inferred_candidate_count > 0 else 0.0,
            6,
        )
        lane_summary["inferred_ready"] = bool(
            inferred_candidate_count > 0 and int(lane_summary.get("inferred_linkedin_url_count") or 0) > 0
        )
        lane_summary["inferred_source_kind"] = source_kind
        lane_summary["inferred_source_path"] = source_path

    return current_summary, former_summary


def _should_materialize_standard_bundle(shard_row: dict[str, Any]) -> bool:
    lane = str(shard_row.get("lane") or "").strip().lower()
    if lane not in {"company_employees", "profile_search"}:
        return False
    if lane == "company_employees":
        search_query = _normalize_query_text(shard_row.get("search_query"))
        shard_id = str(shard_row.get("shard_id") or "").strip().lower()
        if not search_query and shard_id in {"", "root"}:
            return False
    return True


def _build_stub_candidate(
    *,
    shard_row: dict[str, Any],
    raw_entry: dict[str, Any],
    source_path: str,
    target_company: str,
) -> dict[str, Any]:
    display_name = _entry_display_name(raw_entry)
    linkedin_url = _entry_linkedin_url(raw_entry)
    search_query = _normalize_query_text(shard_row.get("search_query"))
    searchable_text = _entry_searchable_text(raw_entry)
    candidate_id = _stub_candidate_id(target_company, str(shard_row.get("shard_key") or ""), raw_entry)
    employment_scope = str(shard_row.get("employment_scope") or "all").strip().lower()
    employment_status = "former" if employment_scope == "former" else "current"
    return {
        "candidate_id": candidate_id,
        "name_en": display_name,
        "name_zh": "",
        "display_name": display_name,
        "category": "former_employee" if employment_status == "former" else "employee",
        "target_company": target_company,
        "organization": _entry_company_name(raw_entry) or target_company,
        "employment_status": employment_status,
        "role": _entry_role(raw_entry),
        "team": "",
        "joined_at": "",
        "left_at": "",
        "current_destination": "",
        "ethnicity_background": "",
        "investment_involvement": "",
        "focus_areas": search_query,
        "education": "",
        "work_history": "",
        "notes": "",
        "linkedin_url": linkedin_url,
        "media_url": "",
        "source_dataset": f"{normalize_company_key(target_company)}_{str(shard_row.get('lane') or '').strip()}_standard_bundle",
        "source_path": source_path,
        "metadata": {
            "standard_bundle_fallback": True,
            "standard_bundle_searchable_text": searchable_text,
            "standard_bundle_source": {
                "shard_key": str(shard_row.get("shard_key") or ""),
                "shard_id": str(shard_row.get("shard_id") or ""),
                "shard_title": str(shard_row.get("shard_title") or ""),
                "lane": str(shard_row.get("lane") or ""),
                "employment_scope": str(shard_row.get("employment_scope") or ""),
                "search_query": search_query,
            },
            "raw_entry": dict(raw_entry),
        },
    }


def _build_stub_evidence(
    *,
    candidate_record: dict[str, Any],
    shard_row: dict[str, Any],
    raw_entry: dict[str, Any],
    source_path: str,
) -> dict[str, Any]:
    title = _entry_role(raw_entry) or _entry_display_name(raw_entry) or str(shard_row.get("shard_title") or "").strip()
    payload = "|".join(
        [
            str(candidate_record.get("candidate_id") or ""),
            str(shard_row.get("shard_key") or ""),
            title,
            _entry_linkedin_url(raw_entry),
        ]
    )
    evidence_id = sha1(payload.encode("utf-8")).hexdigest()[:16]
    return {
        "evidence_id": evidence_id,
        "candidate_id": str(candidate_record.get("candidate_id") or ""),
        "source_type": str(shard_row.get("lane") or ""),
        "title": title,
        "url": _entry_linkedin_url(raw_entry),
        "summary": _entry_searchable_text(raw_entry),
        "source_dataset": str(candidate_record.get("source_dataset") or ""),
        "source_path": source_path,
        "metadata": {
            "standard_bundle_fallback": True,
            "shard_key": str(shard_row.get("shard_key") or ""),
            "search_query": _normalize_query_text(shard_row.get("search_query")),
        },
    }


def _standard_bundle_metadata(summary_payload: dict[str, Any], bundle_path: Path, summary_path: Path) -> dict[str, Any]:
    return {
        "bundle_path": str(bundle_path),
        "summary_path": str(summary_path),
        "bundle_candidate_count": int(summary_payload.get("bundle_candidate_count") or 0),
        "matched_candidate_count": int(summary_payload.get("matched_candidate_count") or 0),
        "stub_candidate_count": int(summary_payload.get("stub_candidate_count") or 0),
        "raw_entry_count": int(summary_payload.get("raw_entry_count") or 0),
        "built_at": str(summary_payload.get("built_at") or ""),
    }


def build_acquisition_shard_standard_bundle(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    shard_row: dict[str, Any],
    asset_view: str = "canonical_merged",
    snapshot_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot_dir = Path(runtime_dir) / "company_assets" / normalize_company_key(target_company) / snapshot_id
    if not snapshot_dir.exists():
        return {"status": "missing_snapshot", "snapshot_id": snapshot_id, "shard_key": str(shard_row.get("shard_key") or "")}
    if not _should_materialize_standard_bundle(shard_row):
        return {"status": "skipped", "reason": "not_standardizable", "shard_key": str(shard_row.get("shard_key") or "")}

    source_entries, source_path = _load_shard_entries(shard_row)
    if snapshot_payload is None:
        try:
            snapshot_payload = load_company_snapshot_candidate_documents(
                runtime_dir=runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                view=asset_view,
            )
        except CandidateArtifactError:
            snapshot_payload = {
                "candidates": [],
                "evidence": [],
                "artifact_summary": {},
                "snapshot_dir": str(snapshot_dir),
                "asset_view": asset_view,
            }

    candidates = list(snapshot_payload.get("candidates") or [])
    evidence = [dict(item) for item in list(snapshot_payload.get("evidence") or [])]
    candidate_by_url, evidence_by_candidate, candidate_by_name = _build_candidate_indexes(
        store=store,
        candidates=candidates,
        evidence=evidence,
    )

    bundle_candidates: list[dict[str, Any]] = []
    bundle_evidence: list[dict[str, Any]] = []
    bundle_candidate_ids: set[str] = set()
    evidence_ids: set[str] = set()
    matched_candidate_count = 0
    stub_candidate_count = 0
    matched_entry_count = 0

    for raw_entry in list(source_entries or []):
        candidate_record: dict[str, Any] | None = None
        url = _entry_linkedin_url(raw_entry)
        url_key = _normalize_linkedin_url_key(store, url) if url else ""
        if url_key:
            candidate_record = candidate_by_url.get(url_key)
        if candidate_record is None:
            name_key = normalize_name_token(_entry_display_name(raw_entry))
            if name_key:
                candidate_record = candidate_by_name.get(name_key)
        if candidate_record is not None:
            matched_entry_count += 1
            candidate_id = str(candidate_record.get("candidate_id") or "").strip()
            if candidate_id and candidate_id not in bundle_candidate_ids:
                bundle_candidate_ids.add(candidate_id)
                bundle_candidates.append(dict(candidate_record))
                matched_candidate_count += 1
                for evidence_record in evidence_by_candidate.get(candidate_id, []):
                    evidence_id = str(evidence_record.get("evidence_id") or "").strip()
                    if evidence_id and evidence_id not in evidence_ids:
                        evidence_ids.add(evidence_id)
                        bundle_evidence.append(dict(evidence_record))
            continue
        stub_candidate = _build_stub_candidate(
            shard_row=shard_row,
            raw_entry=dict(raw_entry),
            source_path=source_path,
            target_company=target_company,
        )
        candidate_id = str(stub_candidate.get("candidate_id") or "").strip()
        if candidate_id and candidate_id not in bundle_candidate_ids:
            bundle_candidate_ids.add(candidate_id)
            bundle_candidates.append(stub_candidate)
            stub_candidate_count += 1
            stub_evidence = _build_stub_evidence(
                candidate_record=stub_candidate,
                shard_row=shard_row,
                raw_entry=dict(raw_entry),
                source_path=source_path,
            )
            evidence_id = str(stub_evidence.get("evidence_id") or "").strip()
            if evidence_id and evidence_id not in evidence_ids:
                evidence_ids.add(evidence_id)
                bundle_evidence.append(stub_evidence)

    shard_key = str(shard_row.get("shard_key") or "").strip()
    bundle_dir = _shard_bundle_dir(snapshot_dir, asset_view, shard_key)
    bundle_dir.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(bundle_dir)
    bundle_payload = {
        "snapshot": {
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "snapshot_dir": str(snapshot_dir),
            "built_at": _utc_now_iso(),
        },
        "shard": {
            "shard_key": shard_key,
            "shard_id": str(shard_row.get("shard_id") or ""),
            "shard_title": str(shard_row.get("shard_title") or ""),
            "lane": str(shard_row.get("lane") or ""),
            "employment_scope": str(shard_row.get("employment_scope") or ""),
            "search_query": _normalize_query_text(shard_row.get("search_query")),
            "company_scope": _normalize_string_list(shard_row.get("company_scope") or []),
            "locations": _normalize_string_list(shard_row.get("locations") or []),
            "function_ids": _normalize_string_list(shard_row.get("function_ids") or []),
            "source_path": source_path,
            "result_count": int(shard_row.get("result_count") or 0),
            "estimated_total_count": int(shard_row.get("estimated_total_count") or 0),
        },
        "candidates": bundle_candidates,
        "evidence": bundle_evidence,
    }
    summary_payload = {
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "shard_key": shard_key,
        "shard_id": str(shard_row.get("shard_id") or ""),
        "shard_title": str(shard_row.get("shard_title") or ""),
        "lane": str(shard_row.get("lane") or ""),
        "employment_scope": str(shard_row.get("employment_scope") or ""),
        "search_query": _normalize_query_text(shard_row.get("search_query")),
        "bundle_candidate_count": len(bundle_candidates),
        "matched_candidate_count": matched_candidate_count,
        "stub_candidate_count": stub_candidate_count,
        "matched_entry_count": matched_entry_count,
        "raw_entry_count": len(source_entries),
        "evidence_count": len(bundle_evidence),
        "built_at": _utc_now_iso(),
        "source_path": source_path,
    }
    bundle_path = logger.write_json(
        Path("bundle.json"),
        bundle_payload,
        asset_type="acquisition_shard_standard_bundle",
        source_kind="organization_asset_reuse",
        is_raw_asset=False,
        model_safe=True,
    )
    summary_path = logger.write_json(
        Path("summary.json"),
        summary_payload,
        asset_type="acquisition_shard_standard_bundle_summary",
        source_kind="organization_asset_reuse",
        is_raw_asset=False,
        model_safe=True,
    )
    updated_row = dict(shard_row)
    updated_metadata = dict(updated_row.get("metadata") or {})
    updated_metadata["standard_bundle"] = _standard_bundle_metadata(summary_payload, bundle_path, summary_path)
    updated_row["metadata"] = updated_metadata
    persisted = store.upsert_acquisition_shard_registry(updated_row)
    return {
        "status": "built",
        "shard_key": shard_key,
        "bundle_path": str(bundle_path),
        "summary_path": str(summary_path),
        "summary": summary_payload,
        "row": persisted,
    }


def ensure_acquisition_shard_bundles_for_snapshot(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    snapshot_dir = Path(runtime_dir) / "company_assets" / normalize_company_key(target_company) / snapshot_id
    if not snapshot_dir.exists():
        return {"status": "missing_snapshot", "snapshot_id": snapshot_id, "bundle_count": 0}
    rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=[snapshot_id],
        statuses=["completed", "completed_with_cap", "skipped_high_overlap"],
        limit=5000,
    )
    if not rows:
        return {"status": "missing_registry", "snapshot_id": snapshot_id, "bundle_count": 0}
    try:
        snapshot_payload = load_company_snapshot_candidate_documents(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            view=asset_view,
        )
    except CandidateArtifactError:
        snapshot_payload = {
            "candidates": [],
            "evidence": [],
            "artifact_summary": {},
            "snapshot_dir": str(snapshot_dir),
            "asset_view": asset_view,
        }

    manifest_entries: list[dict[str, Any]] = []
    for row in rows:
        if not _should_materialize_standard_bundle(row):
            continue
        bundle_result = build_acquisition_shard_standard_bundle(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            shard_row=row,
            asset_view=asset_view,
            snapshot_payload=snapshot_payload,
        )
        if str(bundle_result.get("status") or "") != "built":
            continue
        summary_payload = dict(bundle_result.get("summary") or {})
        manifest_entries.append(
            {
                "shard_key": str(bundle_result.get("shard_key") or ""),
                "bundle_path": str(bundle_result.get("bundle_path") or ""),
                "summary_path": str(bundle_result.get("summary_path") or ""),
                "lane": str(summary_payload.get("lane") or ""),
                "employment_scope": str(summary_payload.get("employment_scope") or ""),
                "search_query": str(summary_payload.get("search_query") or ""),
                "bundle_candidate_count": int(summary_payload.get("bundle_candidate_count") or 0),
                "matched_candidate_count": int(summary_payload.get("matched_candidate_count") or 0),
                "stub_candidate_count": int(summary_payload.get("stub_candidate_count") or 0),
                "raw_entry_count": int(summary_payload.get("raw_entry_count") or 0),
                "built_at": str(summary_payload.get("built_at") or ""),
            }
        )
    manifest_entries.sort(
        key=lambda item: (
            str(item.get("lane") or ""),
            str(item.get("employment_scope") or ""),
            str(item.get("search_query") or ""),
            str(item.get("shard_key") or ""),
        )
    )
    manifest_path = _shard_bundle_manifest_path(snapshot_dir, asset_view)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(manifest_path.parent)
    manifest_payload = {
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "bundle_count": len(manifest_entries),
        "entries": manifest_entries,
        "built_at": _utc_now_iso(),
    }
    logger.write_json(
        Path("manifest.json"),
        manifest_payload,
        asset_type="acquisition_shard_standard_bundle_manifest",
        source_kind="organization_asset_reuse",
        is_raw_asset=False,
        model_safe=True,
    )
    return {
        "status": "completed",
        "snapshot_id": snapshot_id,
        "manifest_path": str(manifest_path),
        "bundle_count": len(manifest_entries),
        "entries": manifest_entries,
    }


def _summarize_shard_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    unique_queries = _normalize_string_list(row.get("search_query") for row in rows)
    standard_bundle_ready = 0
    bundled_candidate_count = 0
    bundled_stub_count = 0
    for row in rows:
        metadata = dict(row.get("metadata") or {})
        standard_bundle = dict(metadata.get("standard_bundle") or {})
        if standard_bundle:
            standard_bundle_ready += 1
            bundled_candidate_count += int(standard_bundle.get("bundle_candidate_count") or row.get("result_count") or 0)
            bundled_stub_count += int(standard_bundle.get("stub_candidate_count") or 0)
    return {
        "row_count": len(rows),
        "unique_query_count": len(unique_queries),
        "queries": unique_queries,
        "result_count": sum(int(row.get("result_count") or 0) for row in rows),
        "estimated_total_count": sum(int(row.get("estimated_total_count") or 0) for row in rows),
        "provider_cap_hit_count": sum(1 for row in rows if bool(row.get("provider_cap_hit"))),
        "standard_bundle_ready_count": standard_bundle_ready,
        "standard_bundle_candidate_count": bundled_candidate_count,
        "standard_bundle_stub_candidate_count": bundled_stub_count,
    }


def _merge_lane_coverage_summary(explicit_summary: dict[str, Any], inferred_summary: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(explicit_summary or {})
    inferred = dict(inferred_summary or {})
    if inferred:
        merged.update(inferred)
    inferred_candidate_count = _safe_int(merged.get("inferred_candidate_count"))
    standard_bundle_candidate_count = _safe_int(merged.get("standard_bundle_candidate_count"))
    merged["effective_candidate_count"] = max(
        inferred_candidate_count,
        standard_bundle_candidate_count if inferred_candidate_count <= 0 else 0,
    )
    merged["effective_ready"] = bool(
        _safe_int(merged.get("standard_bundle_ready_count")) > 0 or merged.get("inferred_ready")
    )
    return merged


def _build_current_effective_lane_coverage(
    *,
    company_employees_current: dict[str, Any],
    profile_search_current: dict[str, Any],
) -> dict[str, Any]:
    company_lane = dict(company_employees_current or {})
    profile_lane = dict(profile_search_current or {})
    return {
        "source_lane_keys": ["company_employees_current", "profile_search_current"],
        "component_ready_count": int(bool(company_lane.get("effective_ready"))) + int(bool(profile_lane.get("effective_ready"))),
        "effective_candidate_count": max(
            _safe_int(company_lane.get("effective_candidate_count")),
            _safe_int(profile_lane.get("effective_candidate_count")),
        ),
        "effective_ready": bool(company_lane.get("effective_ready")) or bool(profile_lane.get("effective_ready")),
        "company_employees_current": company_lane,
        "profile_search_current": profile_lane,
    }


def _build_former_effective_lane_coverage(profile_search_former: dict[str, Any]) -> dict[str, Any]:
    former_lane = dict(profile_search_former or {})
    return {
        "source_lane_keys": ["profile_search_former"],
        "component_ready_count": int(bool(former_lane.get("effective_ready"))),
        "effective_candidate_count": _safe_int(former_lane.get("effective_candidate_count")),
        "effective_ready": bool(former_lane.get("effective_ready")),
        "profile_search_former": former_lane,
    }


def ensure_organization_completeness_ledger(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
    selected_snapshot_ids: list[str] | None = None,
) -> dict[str, Any]:
    snapshot_dir = Path(runtime_dir) / "company_assets" / normalize_company_key(target_company) / snapshot_id
    if not snapshot_dir.exists():
        return {"status": "missing_snapshot", "snapshot_id": snapshot_id}

    normalized_snapshot_ids = _normalize_string_list(selected_snapshot_ids or [snapshot_id])
    if not normalized_snapshot_ids:
        normalized_snapshot_ids = [snapshot_id]

    for selected_snapshot_id in normalized_snapshot_ids:
        from .asset_reuse_planning import ensure_acquisition_shard_registry_for_snapshot

        ensure_acquisition_shard_registry_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=selected_snapshot_id,
        )
        ensure_acquisition_shard_bundles_for_snapshot(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=selected_snapshot_id,
            asset_view=asset_view,
        )

    org_rows = store.list_organization_asset_registry(
        target_company=target_company,
        asset_view=asset_view,
        limit=500,
    )
    org_row = next((row for row in org_rows if str(row.get("snapshot_id") or "") == snapshot_id), {})
    if not org_row:
        org_row = store.get_authoritative_organization_asset_registry(target_company=target_company, asset_view=asset_view)

    try:
        candidate_payload = load_company_snapshot_candidate_documents(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            view=asset_view,
        )
        artifact_summary = dict(candidate_payload.get("artifact_summary") or {})
        inferred_current_coverage, inferred_former_coverage = _summarize_inferred_baseline_lane_coverage(candidate_payload)
    except CandidateArtifactError:
        artifact_summary = {}
        inferred_current_coverage = {}
        inferred_former_coverage = {}

    shard_rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=normalized_snapshot_ids,
        statuses=["completed", "completed_with_cap", "skipped_high_overlap"],
        limit=5000,
    )
    current_roster_rows = [row for row in shard_rows if str(row.get("lane") or "") == "company_employees"]
    current_profile_rows = [
        row
        for row in shard_rows
        if str(row.get("lane") or "") == "profile_search"
        and str(row.get("employment_scope") or "").lower() != "former"
    ]
    former_profile_rows = [
        row
        for row in shard_rows
        if str(row.get("lane") or "") == "profile_search"
        and str(row.get("employment_scope") or "").lower() == "former"
    ]

    manifest_entries: list[dict[str, Any]] = []
    for selected_snapshot_id in normalized_snapshot_ids:
        manifest_path = _shard_bundle_manifest_path(
            Path(runtime_dir) / "company_assets" / normalize_company_key(target_company) / selected_snapshot_id,
            asset_view,
        )
        if not manifest_path.exists():
            continue
        try:
            manifest_payload = _json_load(manifest_path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        for item in list(manifest_payload.get("entries") or []):
            if not isinstance(item, dict):
                continue
            manifest_entries.append({**dict(item), "snapshot_id": selected_snapshot_id})

    candidate_count = int(org_row.get("candidate_count") or artifact_summary.get("candidate_count") or 0)
    profile_detail_count = int(org_row.get("profile_detail_count") or artifact_summary.get("profile_detail_count") or 0)
    completeness_score = float(org_row.get("completeness_score") or 0.0)
    bundle_ready_count = len(manifest_entries)
    profile_detail_ratio = (profile_detail_count / candidate_count) if candidate_count > 0 else 0.0
    current_lane_coverage = _merge_lane_coverage_summary(
        _summarize_shard_rows(current_roster_rows),
        inferred_current_coverage,
    )
    current_profile_lane_coverage = _merge_lane_coverage_summary(
        _summarize_shard_rows(current_profile_rows),
        {},
    )
    former_lane_coverage = _merge_lane_coverage_summary(
        _summarize_shard_rows(former_profile_rows),
        inferred_former_coverage,
    )
    readiness = "minimal"
    if candidate_count > 0 and completeness_score >= 60 and bundle_ready_count > 0:
        readiness = "ready"
    elif candidate_count > 0 and (
        bundle_ready_count > 0
        or current_roster_rows
        or current_profile_rows
        or former_profile_rows
        or current_lane_coverage.get("effective_ready")
        or former_lane_coverage.get("effective_ready")
    ):
        readiness = "partial"
    elif candidate_count > 0:
        readiness = "baseline_only"

    current_effective_lane_coverage = _build_current_effective_lane_coverage(
        company_employees_current=current_lane_coverage,
        profile_search_current=current_profile_lane_coverage,
    )
    former_effective_lane_coverage = _build_former_effective_lane_coverage(former_lane_coverage)

    ledger_payload = {
        "target_company": str(org_row.get("target_company") or target_company),
        "company_key": str(org_row.get("company_key") or normalize_company_key(target_company)),
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "selected_snapshot_ids": normalized_snapshot_ids,
        "authoritative_snapshot_id": str(org_row.get("snapshot_id") or snapshot_id),
        "organization_asset": {
            "candidate_count": candidate_count,
            "evidence_count": int(org_row.get("evidence_count") or artifact_summary.get("evidence_count") or 0),
            "profile_detail_count": profile_detail_count,
            "missing_linkedin_count": int(org_row.get("missing_linkedin_count") or artifact_summary.get("missing_linkedin_count") or 0),
            "manual_review_backlog_count": int(org_row.get("manual_review_backlog_count") or artifact_summary.get("manual_review_backlog_count") or 0),
            "profile_completion_backlog_count": int(
                org_row.get("profile_completion_backlog_count")
                or artifact_summary.get("profile_completion_backlog_count")
                or 0
            ),
            "completeness_score": completeness_score,
            "completeness_band": str(org_row.get("completeness_band") or ""),
            "profile_detail_ratio": round(profile_detail_ratio, 6),
        },
        "lane_coverage": {
            "company_employees_current": current_lane_coverage,
            "profile_search_current": current_profile_lane_coverage,
            "profile_search_former": former_lane_coverage,
            "current_effective": current_effective_lane_coverage,
            "former_effective": former_effective_lane_coverage,
        },
        "standard_bundles": {
            "bundle_count": bundle_ready_count,
            "candidate_count": sum(int(item.get("bundle_candidate_count") or 0) for item in manifest_entries),
            "stub_candidate_count": sum(int(item.get("stub_candidate_count") or 0) for item in manifest_entries),
            "entries": manifest_entries,
        },
        "readiness": readiness,
        "delta_planning_ready": bool(
            current_roster_rows
            or current_profile_rows
            or former_profile_rows
            or current_lane_coverage.get("effective_ready")
            or former_lane_coverage.get("effective_ready")
        ),
        "built_at": _utc_now_iso(),
    }

    ledger_path = _completeness_ledger_path(snapshot_dir, asset_view)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(ledger_path.parent)
    logger.write_json(
        Path("organization_completeness_ledger.json"),
        ledger_payload,
        asset_type="organization_completeness_ledger",
        source_kind="organization_asset_reuse",
        is_raw_asset=False,
        model_safe=True,
    )
    try:
        from .asset_reuse_planning import build_organization_asset_registry_record

        selected_snapshot_ids = _normalize_string_list(
            ledger_payload.get("selected_snapshot_ids")
            or org_row.get("selected_snapshot_ids")
            or [snapshot_id]
        )
        registry_summary = {
            **dict(org_row.get("summary") or {}),
            **dict(artifact_summary or {}),
            "target_company": str(ledger_payload.get("target_company") or target_company),
            "company_key": str(ledger_payload.get("company_key") or normalize_company_key(target_company)),
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "candidate_count": candidate_count,
            "evidence_count": int(ledger_payload.get("organization_asset", {}).get("evidence_count") or 0),
            "profile_detail_count": profile_detail_count,
            "explicit_profile_capture_count": int(
                org_row.get("explicit_profile_capture_count") or artifact_summary.get("explicit_profile_capture_count") or 0
            ),
            "missing_linkedin_count": int(ledger_payload.get("organization_asset", {}).get("missing_linkedin_count") or 0),
            "manual_review_backlog_count": int(
                ledger_payload.get("organization_asset", {}).get("manual_review_backlog_count") or 0
            ),
            "profile_completion_backlog_count": int(
                ledger_payload.get("organization_asset", {}).get("profile_completion_backlog_count") or 0
            ),
            "source_snapshot_count": len(selected_snapshot_ids),
            "source_snapshot_selection": {
                **dict(org_row.get("source_snapshot_selection") or {}),
                "selected_snapshot_ids": selected_snapshot_ids,
            },
            "selected_snapshot_ids": selected_snapshot_ids,
            "lane_coverage": dict(ledger_payload.get("lane_coverage") or {}),
            "current_lane_coverage": current_effective_lane_coverage,
            "former_lane_coverage": former_effective_lane_coverage,
        }
        registry_record = build_organization_asset_registry_record(
            target_company=str(ledger_payload.get("target_company") or target_company),
            company_key=str(ledger_payload.get("company_key") or normalize_company_key(target_company)),
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            summary=registry_summary,
            current_lane_coverage=current_effective_lane_coverage,
            former_lane_coverage=former_effective_lane_coverage,
            source_path=str(org_row.get("source_path") or ledger_path),
            source_job_id=str(org_row.get("source_job_id") or ""),
            authoritative=bool(org_row.get("authoritative")),
        )
        store.upsert_organization_asset_registry(
            registry_record,
            authoritative=bool(org_row.get("authoritative")),
        )
    except Exception:
        pass
    return {
        "status": "built",
        "ledger_path": str(ledger_path),
        "summary": ledger_payload,
    }


def discover_normalized_company_snapshots(
    *,
    runtime_dir: str | Path,
    asset_view: str = "canonical_merged",
) -> list[dict[str, Any]]:
    company_assets_dir = Path(runtime_dir) / "company_assets"
    if not company_assets_dir.exists():
        return []
    discovered: list[dict[str, Any]] = []
    for company_dir in sorted(path for path in company_assets_dir.iterdir() if path.is_dir()):
        for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
            identity_payload = _load_company_snapshot_identity(snapshot_dir, fallback_payload={})
            target_company = (
                str(identity_payload.get("canonical_name") or "").strip()
                or company_dir.name
            )
            summary_result = load_company_snapshot_registry_summary(
                runtime_dir=runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_dir.name,
                asset_view=asset_view,
            )
            summary = dict(summary_result.get("summary") or {})
            if not summary:
                continue
            discovered.append(
                {
                    "company_key": str(identity_payload.get("company_key") or company_dir.name).strip() or company_dir.name,
                    "target_company": str(summary.get("target_company") or target_company).strip() or target_company,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": str(snapshot_dir),
                    "summary_path": str(summary_result.get("source_path") or ""),
                }
            )
    return discovered


def warmup_existing_organization_assets(
    *,
    runtime_dir: str | Path,
    store: SQLiteStore,
    asset_view: str = "canonical_merged",
    max_companies: int = 0,
) -> dict[str, Any]:
    from .asset_reuse_planning import (
        backfill_organization_asset_registry_for_company,
        ensure_acquisition_shard_registry_for_snapshot,
    )

    discovered = discover_normalized_company_snapshots(runtime_dir=runtime_dir, asset_view=asset_view)
    companies_by_key: dict[str, dict[str, Any]] = {}
    for item in discovered:
        company_key = str(item.get("company_key") or "").strip() or normalize_company_key(str(item.get("target_company") or ""))
        company_entry = companies_by_key.setdefault(
            company_key,
            {
                "target_company": str(item.get("target_company") or ""),
                "company_key": company_key,
                "snapshot_ids": [],
            },
        )
        snapshot_id = str(item.get("snapshot_id") or "").strip()
        if snapshot_id:
            company_entry["snapshot_ids"].append(snapshot_id)
    companies = list(companies_by_key.values())
    companies.sort(key=lambda item: (normalize_company_key(str(item.get("target_company") or "")), str(item.get("target_company") or "")))
    if max_companies > 0:
        companies = companies[: max(1, int(max_companies))]

    results: list[dict[str, Any]] = []
    warmed_snapshot_count = 0
    for item in companies:
        target_company = str(item.get("target_company") or "").strip()
        if not target_company:
            continue
        backfill_result = backfill_organization_asset_registry_for_company(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            asset_view=asset_view,
        )
        selected_record = dict(backfill_result.get("selected_record") or {})
        selected_snapshot_ids = _normalize_string_list(
            selected_record.get("selected_snapshot_ids") or [selected_record.get("snapshot_id")]
        )
        shard_warmup_results: list[dict[str, Any]] = []
        for snapshot_id in selected_snapshot_ids:
            if not snapshot_id:
                continue
            shard_registry_result = ensure_acquisition_shard_registry_for_snapshot(
                runtime_dir=runtime_dir,
                store=store,
                target_company=target_company,
                snapshot_id=snapshot_id,
            )
            bundle_result = ensure_acquisition_shard_bundles_for_snapshot(
                runtime_dir=runtime_dir,
                store=store,
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
            )
            warmed_snapshot_count += 1
            shard_warmup_results.append(
                {
                    "snapshot_id": snapshot_id,
                    "registry": shard_registry_result,
                    "bundles": {
                        "status": str(bundle_result.get("status") or ""),
                        "bundle_count": int(bundle_result.get("bundle_count") or 0),
                        "manifest_path": str(bundle_result.get("manifest_path") or ""),
                    },
                }
            )
        ledger_result = {}
        if selected_record:
            ledger_result = ensure_organization_completeness_ledger(
                runtime_dir=runtime_dir,
                store=store,
                target_company=target_company,
                snapshot_id=str(selected_record.get("snapshot_id") or ""),
                asset_view=asset_view,
                selected_snapshot_ids=selected_snapshot_ids,
            )
        results.append(
            {
                "target_company": target_company,
                "company_key": str(item.get("company_key") or ""),
                "backfill": {
                    "status": str(backfill_result.get("status") or ""),
                    "selected_snapshot_id": str(backfill_result.get("selected_snapshot_id") or ""),
                    "record_count": int(backfill_result.get("record_count") or 0),
                },
                "selected_snapshot_ids": selected_snapshot_ids,
                "shard_warmup": shard_warmup_results,
                "ledger": {
                    "status": str(ledger_result.get("status") or ""),
                    "ledger_path": str(ledger_result.get("ledger_path") or ""),
                    "readiness": str(dict(ledger_result.get("summary") or {}).get("readiness") or ""),
                },
            }
        )

    return {
        "status": "completed",
        "asset_view": asset_view,
        "company_count": len(companies),
        "snapshot_count": warmed_snapshot_count,
        "results": results,
        "completed_at": _utc_now_iso(),
    }
