from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any, Callable

from .asset_logger import AssetLogger
from .asset_paths import iter_company_asset_snapshot_dirs, load_company_snapshot_identity, resolve_company_snapshot_dir
from .asset_registration import sync_company_asset_registration
from .candidate_artifacts import (
    CandidateArtifactError,
    _resolve_company_snapshot,
    load_authoritative_company_snapshot_candidate_documents,
)
from .company_registry import normalize_company_key
from .domain import normalize_name_token
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .profile_timeline import (
    timeline_has_complete_profile_detail,
)
from .storage import ControlPlaneStore, _build_asset_membership_row


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_load(path: Path) -> Any:
    return json.loads(path.read_text())


def _run_sync_step(callback: Callable[[], Any]) -> dict[str, Any]:
    try:
        return {
            "status": "completed",
            "result": callback(),
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _summarize_sync_status(sync_status: dict[str, Any]) -> str:
    for key, value in dict(sync_status or {}).items():
        if key == "overall_status" or not isinstance(value, dict):
            continue
        if str(value.get("status") or "") == "failed":
            return "partial_failure"
    return "completed"


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


def _load_authoritative_snapshot_candidate_payload(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore | None,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
    allow_materialization_fallback: bool = True,
    allow_candidate_documents_fallback: bool | None = True,
) -> dict[str, Any]:
    return load_authoritative_company_snapshot_candidate_documents(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        view=asset_view,
        allow_materialization_fallback=allow_materialization_fallback,
        allow_candidate_documents_fallback=allow_candidate_documents_fallback,
    )


def _candidate_membership_rows_from_records(
    *,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    artifact_kind: str,
    artifact_key: str,
    candidates: list[dict[str, Any]],
    lane: str = "",
    employment_scope: str = "",
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate_record in candidates:
        if isinstance(candidate_record, dict):
            record = dict(candidate_record or {})
        else:
            to_record = getattr(candidate_record, "to_record", None)
            if not callable(to_record):
                continue
            record = dict(to_record() or {})
        rows.append(
            _build_asset_membership_row(
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                artifact_kind=artifact_kind,
                artifact_key=artifact_key,
                candidate_record=record,
                lane=lane,
                employment_scope=employment_scope,
            )
        )
    return rows


def load_cached_organization_completeness_ledger(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id:
        return {}
    snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=target_company,
        snapshot_id=normalized_snapshot_id,
    )
    if snapshot_dir is None:
        return {}
    ledger_path = _completeness_ledger_path(snapshot_dir, asset_view)
    if not ledger_path.exists():
        return {}
    try:
        payload = json.loads(ledger_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


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


def _normalize_linkedin_url_key(url: str) -> str:
    normalized = normalize_linkedin_profile_url_key(str(url or "").strip())
    if normalized:
        return normalized
    return str(url or "").strip().lower()


def _build_candidate_indexes(
    *,
    store: ControlPlaneStore,
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
            candidate_by_url[_normalize_linkedin_url_key(linkedin_url)] = record
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
    metadata = dict(getattr(candidate, "metadata", {}) or {})
    payload = dict(candidate.to_record()) if hasattr(candidate, "to_record") else {}
    timeline_like = {
        "experience_lines": metadata.get("experience_lines") or payload.get("experience_lines"),
        "education_lines": metadata.get("education_lines") or payload.get("education_lines"),
        "headline": metadata.get("headline") or payload.get("headline"),
        "summary": metadata.get("summary") or payload.get("summary"),
        "about": metadata.get("about") or payload.get("about"),
        "primary_email": metadata.get("primary_email") or metadata.get("email") or payload.get("primary_email"),
        "languages": metadata.get("languages") or payload.get("languages"),
        "skills": metadata.get("skills") or payload.get("skills"),
        "profile_capture_kind": metadata.get("profile_capture_kind") or payload.get("profile_capture_kind"),
        "profile_capture_source_path": metadata.get("profile_capture_source_path")
        or payload.get("profile_capture_source_path"),
        "source_path": metadata.get("profile_timeline_source_path")
        or metadata.get("source_path")
        or str(getattr(candidate, "source_path", "") or ""),
    }
    if timeline_has_complete_profile_detail(timeline_like):
        return True
    if (
        str(getattr(candidate, "education", "") or "").strip()
        or str(getattr(candidate, "work_history", "") or "").strip()
    ):
        return True
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


def _merge_registry_summary_with_completeness_ledger(
    *,
    snapshot_dir: Path,
    asset_view: str,
    summary: dict[str, Any] | None,
) -> dict[str, Any]:
    merged_summary = dict(summary or {})
    ledger_path = _completeness_ledger_path(snapshot_dir, asset_view)
    if not ledger_path.exists():
        return merged_summary
    try:
        ledger_payload = _json_load(ledger_path)
    except (OSError, ValueError, json.JSONDecodeError):
        return merged_summary
    if not isinstance(ledger_payload, dict) or not ledger_payload:
        return merged_summary

    organization_asset = dict(ledger_payload.get("organization_asset") or {})
    lane_coverage = dict(ledger_payload.get("lane_coverage") or {})
    selected_snapshot_ids = _normalize_string_list(ledger_payload.get("selected_snapshot_ids") or [])
    source_snapshot_selection = dict(merged_summary.get("source_snapshot_selection") or {})

    merged_summary.update(
        {
            "target_company": str(ledger_payload.get("target_company") or merged_summary.get("target_company") or ""),
            "company_key": str(ledger_payload.get("company_key") or merged_summary.get("company_key") or ""),
            "snapshot_id": str(ledger_payload.get("snapshot_id") or merged_summary.get("snapshot_id") or ""),
            "asset_view": str(ledger_payload.get("asset_view") or merged_summary.get("asset_view") or asset_view),
            "candidate_count": max(
                _safe_int(merged_summary.get("candidate_count")),
                _safe_int(organization_asset.get("candidate_count")),
            ),
            "evidence_count": max(
                _safe_int(merged_summary.get("evidence_count")),
                _safe_int(organization_asset.get("evidence_count")),
            ),
            "profile_detail_count": max(
                _safe_int(merged_summary.get("profile_detail_count")),
                _safe_int(organization_asset.get("profile_detail_count")),
            ),
            "missing_linkedin_count": max(
                _safe_int(merged_summary.get("missing_linkedin_count")),
                _safe_int(organization_asset.get("missing_linkedin_count")),
            ),
            "manual_review_backlog_count": max(
                _safe_int(merged_summary.get("manual_review_backlog_count")),
                _safe_int(organization_asset.get("manual_review_backlog_count")),
            ),
            "profile_completion_backlog_count": max(
                _safe_int(merged_summary.get("profile_completion_backlog_count")),
                _safe_int(organization_asset.get("profile_completion_backlog_count")),
            ),
            "selected_snapshot_ids": selected_snapshot_ids
            or _normalize_string_list(merged_summary.get("selected_snapshot_ids") or []),
            "materialization_generation_key": str(
                organization_asset.get("materialization_generation_key")
                or merged_summary.get("materialization_generation_key")
                or ""
            ),
            "materialization_generation_sequence": max(
                _safe_int(merged_summary.get("materialization_generation_sequence")),
                _safe_int(organization_asset.get("materialization_generation_sequence")),
            ),
            "materialization_watermark": str(
                organization_asset.get("materialization_watermark")
                or merged_summary.get("materialization_watermark")
                or ""
            ),
            "exact_membership_summary": dict(
                organization_asset.get("exact_membership_summary")
                or merged_summary.get("exact_membership_summary")
                or {}
            ),
        }
    )
    if merged_summary.get("selected_snapshot_ids"):
        merged_summary["source_snapshot_count"] = max(
            _safe_int(merged_summary.get("source_snapshot_count")),
            len(list(merged_summary.get("selected_snapshot_ids") or [])),
        )
        merged_summary["source_snapshot_selection"] = {
            **source_snapshot_selection,
            "selected_snapshot_ids": list(merged_summary.get("selected_snapshot_ids") or []),
        }
    if lane_coverage:
        merged_summary["lane_coverage"] = lane_coverage
        merged_summary["current_lane_coverage"] = dict(
            lane_coverage.get("current_effective") or merged_summary.get("current_lane_coverage") or {}
        )
        merged_summary["former_lane_coverage"] = dict(
            lane_coverage.get("former_effective") or merged_summary.get("former_lane_coverage") or {}
        )
    return merged_summary


def load_company_snapshot_registry_summary(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore | None = None,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    try:
        company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
            runtime_dir, target_company, snapshot_id=snapshot_id
        )
    except CandidateArtifactError:
        return {}

    summary_path = _company_artifact_summary_path(snapshot_dir, asset_view)
    if summary_path.exists():
        try:
            summary = _json_load(summary_path)
        except (OSError, ValueError, json.JSONDecodeError):
            summary = {}
        if isinstance(summary, dict) and summary:
            summary = _merge_registry_summary_with_completeness_ledger(
                snapshot_dir=snapshot_dir,
                asset_view=asset_view,
                summary=summary,
            )
            return {
                "summary": summary,
                "source_path": str(summary_path),
                "snapshot_dir": str(snapshot_dir),
                "identity_payload": identity_payload,
                "company_key": str(identity_payload.get("company_key") or company_key).strip() or company_key,
            }

    try:
        candidate_payload = _load_authoritative_snapshot_candidate_payload(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            allow_materialization_fallback=store is not None,
        )
    except CandidateArtifactError:
        return {}

    raw_payload: dict[str, Any] = dict(candidate_payload.get("source_payload") or {})
    source_path = Path(str(candidate_payload.get("source_path") or "")).expanduser()
    if not raw_payload and source_path.exists():
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
        or [
            item.get("snapshot_id")
            for item in list(snapshot_metadata.get("source_snapshots") or [])
            if isinstance(item, dict)
        ]
        or [snapshot_id]
    )
    candidates = list(candidate_payload.get("candidates") or [])
    evidence = list(candidate_payload.get("evidence") or [])
    profile_detail_count = sum(1 for candidate in candidates if _candidate_has_profile_detail(candidate))
    explicit_profile_capture_count = sum(
        1 for candidate in candidates if _candidate_has_explicit_profile_capture(candidate)
    )
    missing_linkedin_count = sum(
        1 for candidate in candidates if not str(getattr(candidate, "linkedin_url", "") or "").strip()
    )
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
        "source_snapshot_count": max(
            len(selected_snapshot_ids), len(list(snapshot_metadata.get("source_snapshots") or [])), 1
        ),
        "source_snapshot_selection": source_snapshot_selection
        or {
            "mode": "candidate_documents_fallback",
            "selected_snapshot_ids": selected_snapshot_ids,
            "reason": "artifact_summary_missing_used_candidate_documents",
        },
        "selected_snapshot_ids": selected_snapshot_ids,
    }
    summary = _merge_registry_summary_with_completeness_ledger(
        snapshot_dir=snapshot_dir,
        asset_view=asset_view,
        summary=summary,
    )
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
        if category not in {"employee", "former_employee", "investor"} and employment_status not in {
            "current",
            "former",
        }:
            continue
        lane_summary = (
            former_summary if (employment_status == "former" or category == "former_employee") else current_summary
        )
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
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    shard_row: dict[str, Any],
    asset_view: str = "canonical_merged",
    snapshot_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
    )
    if snapshot_dir is None or not snapshot_dir.exists():
        return {
            "status": "missing_snapshot",
            "snapshot_id": snapshot_id,
            "shard_key": str(shard_row.get("shard_key") or ""),
        }
    if not _should_materialize_standard_bundle(shard_row):
        return {"status": "skipped", "reason": "not_standardizable", "shard_key": str(shard_row.get("shard_key") or "")}

    source_entries, source_path = _load_shard_entries(shard_row)
    if snapshot_payload is None:
        try:
            snapshot_payload = _load_authoritative_snapshot_candidate_payload(
                runtime_dir=runtime_dir,
                store=store,
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
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
        url_key = _normalize_linkedin_url_key(url) if url else ""
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
    bundle_payload: dict[str, Any] = {
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
    summary_payload: dict[str, Any] = {
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
    generation = store.register_asset_materialization(
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        artifact_kind="acquisition_shard_bundle",
        artifact_key=shard_key,
        source_path=str(bundle_dir / "summary.json"),
        summary=summary_payload,
        metadata={
            "lane": str(shard_row.get("lane") or ""),
            "employment_scope": str(shard_row.get("employment_scope") or ""),
            "search_query": _normalize_query_text(shard_row.get("search_query")),
            "source_path": source_path,
        },
        members=_candidate_membership_rows_from_records(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=shard_key,
            candidates=bundle_candidates,
            lane=str(shard_row.get("lane") or ""),
            employment_scope=str(shard_row.get("employment_scope") or ""),
        ),
    )
    membership_summary = (
        store.summarize_asset_membership_index(generation_key=str(generation.get("generation_key") or ""))
        if generation
        else {}
    )
    generation_fields = {
        "materialization_generation_key": str(generation.get("generation_key") or ""),
        "materialization_generation_sequence": int(generation.get("generation_sequence") or 0),
        "materialization_watermark": str(generation.get("generation_watermark") or ""),
        "membership_summary": membership_summary,
    }
    bundle_payload["snapshot"].update(generation_fields)
    bundle_payload["shard"].update(generation_fields)
    summary_payload.update(generation_fields)
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
    updated_row["materialization_generation_key"] = str(generation.get("generation_key") or "")
    updated_row["materialization_generation_sequence"] = int(generation.get("generation_sequence") or 0)
    updated_row["materialization_watermark"] = str(generation.get("generation_watermark") or "")
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
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
) -> dict[str, Any]:
    snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
    )
    if snapshot_dir is None or not snapshot_dir.exists():
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
        snapshot_payload = _load_authoritative_snapshot_candidate_payload(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
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
                "materialization_generation_key": str(summary_payload.get("materialization_generation_key") or ""),
                "materialization_generation_sequence": int(
                    summary_payload.get("materialization_generation_sequence") or 0
                ),
                "materialization_watermark": str(summary_payload.get("materialization_watermark") or ""),
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
            bundled_candidate_count += int(
                standard_bundle.get("bundle_candidate_count") or row.get("result_count") or 0
            )
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


def _merge_lane_coverage_summary(
    explicit_summary: dict[str, Any], inferred_summary: dict[str, Any] | None
) -> dict[str, Any]:
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
        "component_ready_count": int(bool(company_lane.get("effective_ready")))
        + int(bool(profile_lane.get("effective_ready"))),
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
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str = "canonical_merged",
    selected_snapshot_ids: list[str] | None = None,
) -> dict[str, Any]:
    snapshot_dir = resolve_company_snapshot_dir(
        runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
    )
    if snapshot_dir is None or not snapshot_dir.exists():
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
    org_row: dict[str, Any] = next(
        (row for row in org_rows if str(row.get("snapshot_id") or "") == snapshot_id),
        {},
    )
    if not org_row:
        org_row = store.get_authoritative_organization_asset_registry(
            target_company=target_company, asset_view=asset_view
        )

    try:
        candidate_payload = _load_authoritative_snapshot_candidate_payload(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
        )
        artifact_summary = dict(candidate_payload.get("artifact_summary") or {})
        inferred_current_coverage, inferred_former_coverage = _summarize_inferred_baseline_lane_coverage(
            candidate_payload
        )
    except CandidateArtifactError:
        candidate_payload = {}
        artifact_summary = {}
        inferred_current_coverage = {}
        inferred_former_coverage = {}

    organization_generation = store.get_asset_materialization_generation(
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        artifact_kind="organization_asset",
        artifact_key=asset_view,
    )
    if not organization_generation and candidate_payload:
        organization_generation = store.register_asset_materialization(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            artifact_kind="organization_asset",
            artifact_key=asset_view,
            source_path=str(_company_artifact_summary_path(snapshot_dir, asset_view)),
            summary=artifact_summary,
            metadata={
                "source_kind": "organization_completeness_ledger_repair",
                "snapshot_dir": str(snapshot_dir),
            },
            members=_candidate_membership_rows_from_records(
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                artifact_kind="organization_asset",
                artifact_key=asset_view,
                candidates=list(candidate_payload.get("candidates") or []),
                lane="baseline",
            ),
        )
    exact_membership_summary = (
        store.summarize_asset_membership_index(generation_key=str(organization_generation.get("generation_key") or ""))
        if organization_generation
        else {}
    )
    exact_current_count = int(dict(exact_membership_summary.get("employment_scope_counts") or {}).get("current") or 0)
    exact_former_count = int(dict(exact_membership_summary.get("employment_scope_counts") or {}).get("former") or 0)
    if exact_current_count > 0:
        inferred_current_coverage = {
            **dict(inferred_current_coverage or {}),
            "inferred_candidate_count": max(
                exact_current_count,
                _safe_int(dict(inferred_current_coverage or {}).get("inferred_candidate_count")),
            ),
            "effective_candidate_count": max(
                exact_current_count,
                _safe_int(dict(inferred_current_coverage or {}).get("effective_candidate_count")),
            ),
            "inferred_ready": True,
            "exact_member_count": exact_current_count,
            "materialization_generation_key": str(organization_generation.get("generation_key") or ""),
        }
    if exact_former_count > 0:
        inferred_former_coverage = {
            **dict(inferred_former_coverage or {}),
            "inferred_candidate_count": max(
                exact_former_count,
                _safe_int(dict(inferred_former_coverage or {}).get("inferred_candidate_count")),
            ),
            "effective_candidate_count": max(
                exact_former_count,
                _safe_int(dict(inferred_former_coverage or {}).get("effective_candidate_count")),
            ),
            "inferred_ready": True,
            "exact_member_count": exact_former_count,
            "materialization_generation_key": str(organization_generation.get("generation_key") or ""),
        }

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
        if str(row.get("lane") or "") == "profile_search" and str(row.get("employment_scope") or "").lower() != "former"
    ]
    former_profile_rows = [
        row
        for row in shard_rows
        if str(row.get("lane") or "") == "profile_search" and str(row.get("employment_scope") or "").lower() == "former"
    ]

    manifest_entries: list[dict[str, Any]] = []
    for selected_snapshot_id in normalized_snapshot_ids:
        selected_snapshot_dir = resolve_company_snapshot_dir(
            runtime_dir,
            target_company=target_company,
            snapshot_id=selected_snapshot_id,
        )
        if selected_snapshot_dir is None:
            continue
        manifest_path = _shard_bundle_manifest_path(selected_snapshot_dir, asset_view)
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

    candidate_count = max(
        int(org_row.get("candidate_count") or artifact_summary.get("candidate_count") or 0),
        int(exact_membership_summary.get("member_count") or 0),
    )
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

    preserved_generation_key = str(
        organization_generation.get("generation_key")
        or org_row.get("materialization_generation_key")
        or artifact_summary.get("materialization_generation_key")
        or ""
    )
    preserved_generation_sequence = int(
        organization_generation.get("generation_sequence")
        or org_row.get("materialization_generation_sequence")
        or artifact_summary.get("materialization_generation_sequence")
        or 0
    )
    preserved_generation_watermark = str(
        organization_generation.get("generation_watermark")
        or org_row.get("materialization_watermark")
        or artifact_summary.get("materialization_watermark")
        or ""
    )

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
            "missing_linkedin_count": int(
                org_row.get("missing_linkedin_count") or artifact_summary.get("missing_linkedin_count") or 0
            ),
            "manual_review_backlog_count": int(
                org_row.get("manual_review_backlog_count") or artifact_summary.get("manual_review_backlog_count") or 0
            ),
            "profile_completion_backlog_count": int(
                org_row.get("profile_completion_backlog_count")
                or artifact_summary.get("profile_completion_backlog_count")
                or 0
            ),
            "completeness_score": completeness_score,
            "completeness_band": str(org_row.get("completeness_band") or ""),
            "profile_detail_ratio": round(profile_detail_ratio, 6),
            "materialization_generation_key": preserved_generation_key,
            "materialization_generation_sequence": preserved_generation_sequence,
            "materialization_watermark": preserved_generation_watermark,
            "exact_membership_summary": exact_membership_summary,
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
    sync_status: dict[str, Any] = {}
    try:
        from .asset_reuse_planning import build_organization_asset_registry_record
        from .organization_execution_profile import ensure_organization_execution_profile

        organization_asset_raw = ledger_payload.get("organization_asset")
        organization_asset_summary = dict(organization_asset_raw) if isinstance(organization_asset_raw, dict) else {}
        lane_coverage_raw = ledger_payload.get("lane_coverage")
        lane_coverage_payload = dict(lane_coverage_raw) if isinstance(lane_coverage_raw, dict) else {}
        selected_snapshot_ids = _normalize_string_list(
            ledger_payload.get("selected_snapshot_ids") or org_row.get("selected_snapshot_ids") or [snapshot_id]
        )
        registry_summary = {
            **dict(org_row.get("summary") or {}),
            **dict(artifact_summary or {}),
            "target_company": str(ledger_payload.get("target_company") or target_company),
            "company_key": str(ledger_payload.get("company_key") or normalize_company_key(target_company)),
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "candidate_count": candidate_count,
            "evidence_count": int(organization_asset_summary.get("evidence_count") or 0),
            "profile_detail_count": profile_detail_count,
            "explicit_profile_capture_count": int(
                org_row.get("explicit_profile_capture_count")
                or artifact_summary.get("explicit_profile_capture_count")
                or 0
            ),
            "missing_linkedin_count": int(organization_asset_summary.get("missing_linkedin_count") or 0),
            "manual_review_backlog_count": int(organization_asset_summary.get("manual_review_backlog_count") or 0),
            "profile_completion_backlog_count": int(
                organization_asset_summary.get("profile_completion_backlog_count") or 0
            ),
            "source_snapshot_count": len(selected_snapshot_ids),
            "source_snapshot_selection": {
                **dict(org_row.get("source_snapshot_selection") or {}),
                "selected_snapshot_ids": selected_snapshot_ids,
            },
            "selected_snapshot_ids": selected_snapshot_ids,
            "lane_coverage": lane_coverage_payload,
            "current_lane_coverage": current_effective_lane_coverage,
            "former_lane_coverage": former_effective_lane_coverage,
            "exact_membership_summary": dict(organization_asset_summary.get("exact_membership_summary") or {}),
            "materialization_generation_key": preserved_generation_key,
            "materialization_generation_sequence": preserved_generation_sequence,
            "materialization_watermark": preserved_generation_watermark,
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
        sync_status["organization_asset_registry_refresh"] = _run_sync_step(
            lambda: store.upsert_organization_asset_registry(
                registry_record,
                authoritative=bool(org_row.get("authoritative")),
            )
        )
        if str(sync_status["organization_asset_registry_refresh"].get("status") or "") == "failed":
            sync_status["organization_execution_profile_refresh"] = {
                "status": "skipped",
                "reason": "organization_asset_registry_refresh_failed",
            }
        else:
            sync_status["organization_execution_profile_refresh"] = _run_sync_step(
                lambda: ensure_organization_execution_profile(
                    runtime_dir=runtime_dir,
                    store=store,
                    target_company=str(ledger_payload.get("target_company") or target_company),
                    asset_view=asset_view,
                )
            )
    except Exception as exc:
        sync_status["dependency_load"] = {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
        }
    sync_status["overall_status"] = _summarize_sync_status(sync_status)
    return {
        "status": "built",
        "ledger_path": str(ledger_path),
        "summary": ledger_payload,
        "sync_status": sync_status,
    }


def discover_normalized_company_snapshots(
    *,
    runtime_dir: str | Path,
    asset_view: str = "canonical_merged",
) -> list[dict[str, Any]]:
    snapshot_dirs = iter_company_asset_snapshot_dirs(
        runtime_dir,
        prefer_hot_cache=True,
        existing_only=True,
    )
    if not snapshot_dirs:
        return []
    discovered: list[dict[str, Any]] = []
    for snapshot_dir in snapshot_dirs:
        company_dir = snapshot_dir.parent
        identity_payload = load_company_snapshot_identity(snapshot_dir, fallback_payload={})
        target_company = str(identity_payload.get("canonical_name") or "").strip() or company_dir.name
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
    store: ControlPlaneStore,
    asset_view: str = "canonical_merged",
    max_companies: int = 0,
) -> dict[str, Any]:
    discovered = discover_normalized_company_snapshots(runtime_dir=runtime_dir, asset_view=asset_view)
    companies_by_key: dict[str, dict[str, Any]] = {}
    for item in discovered:
        company_key = str(item.get("company_key") or "").strip() or normalize_company_key(
            str(item.get("target_company") or "")
        )
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
    companies.sort(
        key=lambda item: (
            normalize_company_key(str(item.get("target_company") or "")),
            str(item.get("target_company") or ""),
        )
    )
    if max_companies > 0:
        companies = companies[: max(1, int(max_companies))]

    from .company_registry import refresh_company_identity_registry

    identity_registry_refresh = refresh_company_identity_registry(
        runtime_dir=runtime_dir,
        companies=[
            str(item.get("target_company") or "") for item in companies if str(item.get("target_company") or "").strip()
        ],
    )

    results: list[dict[str, Any]] = []
    warmed_snapshot_count = 0
    for item in companies:
        target_company = str(item.get("target_company") or "").strip()
        if not target_company:
            continue
        preferred_snapshot_ids = _normalize_string_list(item.get("snapshot_ids") or [])
        registration_result = sync_company_asset_registration(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=preferred_snapshot_ids[-1] if preferred_snapshot_ids else "",
            asset_view=asset_view,
            selected_snapshot_ids=preferred_snapshot_ids,
            registry_refresh_mode="backfill",
            refresh_company_identity=False,
        )
        selected_snapshot_ids = _normalize_string_list(
            registration_result.get("selected_snapshot_ids") or preferred_snapshot_ids
        )
        warmed_snapshot_count += len(selected_snapshot_ids)
        sync_status = dict(registration_result.get("sync_status") or {})
        registry_refresh = dict(sync_status.get("acquisition_shard_registry_refresh") or {})
        bundle_refresh = dict(sync_status.get("acquisition_shard_bundle_refresh") or {})
        shard_warmup_results: list[dict[str, Any]] = []
        registry_entries = list(registry_refresh.get("results") or [])
        bundle_entries = list(bundle_refresh.get("results") or [])
        bundle_entries_by_snapshot = {
            str(entry.get("snapshot_id") or ""): dict(entry)
            for entry in bundle_entries
            if str(entry.get("snapshot_id") or "").strip()
        }
        for entry in registry_entries:
            snapshot_id = str(entry.get("snapshot_id") or "").strip()
            bundle_entry = bundle_entries_by_snapshot.get(snapshot_id, {})
            bundle_result = dict(bundle_entry.get("result") or {})
            shard_warmup_results.append(
                {
                    "snapshot_id": snapshot_id,
                    "registry": dict(entry.get("result") or {}),
                    "bundles": {
                        "status": str(bundle_entry.get("status") or ""),
                        "bundle_count": int(bundle_result.get("bundle_count") or 0),
                        "manifest_path": str(bundle_result.get("manifest_path") or ""),
                    },
                }
            )
        ledger_result = dict(sync_status.get("organization_completeness_ledger_refresh", {}).get("result") or {})
        backfill_result = dict(sync_status.get("organization_asset_registry_refresh", {}).get("result") or {})
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
        "company_identity_registry_refresh": identity_registry_refresh,
        "company_count": len(companies),
        "snapshot_count": warmed_snapshot_count,
        "results": results,
        "completed_at": _utc_now_iso(),
    }
