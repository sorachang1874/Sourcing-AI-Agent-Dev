from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path
from typing import Any

from .connectors import CompanyIdentity
from .domain import Candidate, EvidenceRecord, make_evidence_id, normalize_candidate

_CANDIDATE_FIELD_NAMES = {field.name for field in fields(Candidate)}
_EVIDENCE_FIELD_NAMES = {field.name for field in fields(EvidenceRecord)}


def load_candidate_document_state(candidate_doc_path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(candidate_doc_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    candidates = candidate_records_from_payload(payload.get("candidates"))
    evidence = evidence_records_from_payload(payload.get("evidence"))
    state: dict[str, Any] = {}
    if candidates:
        state["candidates"] = candidates
    if evidence:
        state["evidence"] = evidence
    if candidates:
        state.update(_stage_checkpoint_state_from_candidate_documents(payload, candidate_doc_path))
    return state


def _stage_checkpoint_state_from_candidate_documents(payload: dict[str, Any], candidate_doc_path: Path) -> dict[str, Any]:
    """Restore workflow stage checkpoints from durable candidate documents.

    Candidate documents are the event-time artifact for stage outputs. Recovery
    must not re-run a completed enrichment stage just because older checkpoints
    omitted the explicit task progress row.
    """

    acquisition_stage = dict(payload.get("acquisition_stage") or {})
    task_type = str(acquisition_stage.get("task_type") or "").strip()
    phase = str(acquisition_stage.get("phase") or "").strip()
    enrichment_scope = str(payload.get("enrichment_scope") or "").strip()
    state: dict[str, Any] = {}
    if (
        task_type == "enrich_linkedin_profiles"
        or phase == "linkedin_stage_1"
        or enrichment_scope == "linkedin_stage_1"
    ) and _linkedin_stage_profile_prefetch_terminal(payload):
        state["linkedin_stage_completed"] = True
        state["linkedin_stage_candidate_doc_path"] = candidate_doc_path
    if (
        task_type == "enrich_public_web_signals"
        or phase == "public_web_stage_2"
        or enrichment_scope == "public_web_stage_2"
    ):
        state["public_web_stage_completed"] = True
        state["public_web_stage_candidate_doc_path"] = candidate_doc_path
    return state


def _linkedin_stage_profile_prefetch_terminal(payload: dict[str, Any]) -> bool:
    enrichment_summary = dict(payload.get("enrichment_summary") or {})
    profile_prefetch = dict(enrichment_summary.get("profile_prefetch") or {})
    if not profile_prefetch:
        return False
    status = str(profile_prefetch.get("status") or "").strip().lower()
    if status != "completed":
        return False
    registry_terminal_summary = dict(profile_prefetch.get("registry_terminal_summary") or {})
    profile_prefetch_queue = dict(profile_prefetch.get("profile_prefetch_queue") or {})
    if bool(registry_terminal_summary.get("all_requested_terminal")):
        return True
    if bool(profile_prefetch_queue.get("registry_all_requested_terminal")):
        return True
    requested_url_count = _safe_int(
        registry_terminal_summary.get("requested_url_count"),
        profile_prefetch_queue.get("requested_url_count"),
        profile_prefetch.get("requested_url_count"),
    )
    terminal_url_count = _safe_int(
        registry_terminal_summary.get("terminal_url_count"),
        profile_prefetch_queue.get("registry_terminal_url_count"),
    )
    open_url_count = _safe_int(
        registry_terminal_summary.get("open_url_count"),
        profile_prefetch_queue.get("registry_open_url_count"),
    )
    if requested_url_count > 0:
        return terminal_url_count >= requested_url_count and open_url_count <= 0
    queued_worker_count = _safe_int(profile_prefetch.get("queued_worker_count"), profile_prefetch_queue.get("queued_worker_count"))
    return queued_worker_count <= 0 and status == "completed"


def _safe_int(*values: Any) -> int:
    for value in values:
        if value in (None, ""):
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def candidate_records_from_payload(payload: Any) -> list[Candidate]:
    records: list[Candidate] = []
    for item in list(payload or []):
        if not isinstance(item, dict):
            continue
        candidate = candidate_record_from_payload(item)
        if candidate is None:
            continue
        records.append(candidate)
    return records


def candidate_record_from_payload(payload: dict[str, Any]) -> Candidate | None:
    """Decode candidate_documents records through the stable Candidate core.

    Candidate documents are durable serving artifacts and may carry additive
    card/UI fields. Those fields must not make old snapshots unreadable, so the
    loader projects the known Candidate fields and preserves unknown fields in
    metadata instead of calling Candidate(**payload) directly.
    """

    if not isinstance(payload, dict):
        return None
    record = {field_name: payload.get(field_name) for field_name in _CANDIDATE_FIELD_NAMES if field_name in payload}
    candidate_id = str(record.get("candidate_id") or "").strip()
    name_en = str(record.get("name_en") or payload.get("display_name") or "").strip()
    if not candidate_id or not name_en:
        return None
    record["candidate_id"] = candidate_id
    record["name_en"] = name_en
    record["display_name"] = str(record.get("display_name") or name_en).strip()
    metadata = dict(record.get("metadata") or {}) if isinstance(record.get("metadata"), dict) else {}
    for key, value in payload.items():
        if key in _CANDIDATE_FIELD_NAMES or key in metadata or value in (None, "", [], {}):
            continue
        metadata[key] = value
    record["metadata"] = metadata
    try:
        return normalize_candidate(Candidate(**record))
    except TypeError:
        return None


def evidence_records_from_payload(payload: Any) -> list[EvidenceRecord]:
    records: list[EvidenceRecord] = []
    for item in list(payload or []):
        if not isinstance(item, dict):
            continue
        evidence = evidence_record_from_payload(item)
        if evidence is None:
            continue
        records.append(evidence)
    return records


def evidence_record_from_payload(payload: dict[str, Any]) -> EvidenceRecord | None:
    if not isinstance(payload, dict):
        return None
    record = {field_name: payload.get(field_name) for field_name in _EVIDENCE_FIELD_NAMES if field_name in payload}
    candidate_id = str(record.get("candidate_id") or "").strip()
    source_type = str(record.get("source_type") or "").strip()
    if not candidate_id or not source_type:
        return None
    title = str(record.get("title") or "").strip()
    url = str(record.get("url") or "").strip()
    source_dataset = str(record.get("source_dataset") or source_type).strip() or source_type
    source_path = str(record.get("source_path") or "").strip()
    evidence_id = str(record.get("evidence_id") or "").strip() or make_evidence_id(
        candidate_id,
        source_dataset,
        title,
        url or source_path,
    )
    metadata = dict(record.get("metadata") or {}) if isinstance(record.get("metadata"), dict) else {}
    for key, value in payload.items():
        if key in _EVIDENCE_FIELD_NAMES or key in metadata or value in (None, "", [], {}):
            continue
        metadata[key] = value
    return EvidenceRecord(
        evidence_id=evidence_id,
        candidate_id=candidate_id,
        source_type=source_type,
        title=title,
        url=url,
        summary=str(record.get("summary") or "").strip(),
        source_dataset=source_dataset,
        source_path=source_path,
        metadata=metadata,
    )


def company_identity_from_record(payload: dict[str, Any]) -> CompanyIdentity | None:
    if not payload:
        return None
    try:
        return CompanyIdentity(**payload)
    except TypeError:
        return None


def read_json_dict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def read_json_list(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    return [dict(item) for item in list(payload or []) if isinstance(item, dict)]


def merge_background_reconcile_candidate(existing: Candidate | None, incoming: Candidate) -> Candidate:
    if existing is None:
        return normalize_candidate(incoming)
    record = existing.to_record()
    incoming_record = incoming.to_record()
    existing_metadata = dict(existing.metadata or {})
    incoming_metadata = dict(incoming.metadata or {})
    membership_review_locked = str(existing.category or "").strip().lower() == "non_member" or bool(
        existing_metadata.get("membership_review_decision")
    )
    if str(incoming.category or "").strip():
        if not membership_review_locked or str(incoming.category or "").strip().lower() == "non_member":
            record["category"] = incoming.category
    for key, value in incoming_record.items():
        if key in {"candidate_id", "category", "metadata"}:
            continue
        if _has_present_field_value(value):
            record[key] = value
    merged_metadata = dict(existing_metadata)
    merged_metadata.update(incoming_metadata)
    record["metadata"] = merged_metadata
    return normalize_candidate(Candidate(**record))


def _has_present_field_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return value is not None
