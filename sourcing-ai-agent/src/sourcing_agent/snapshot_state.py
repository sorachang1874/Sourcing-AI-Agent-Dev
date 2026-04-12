from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .connectors import CompanyIdentity
from .domain import Candidate, EvidenceRecord, normalize_candidate


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
    return state


def candidate_records_from_payload(payload: Any) -> list[Candidate]:
    records: list[Candidate] = []
    for item in list(payload or []):
        if not isinstance(item, dict):
            continue
        try:
            records.append(normalize_candidate(Candidate(**item)))
        except TypeError:
            continue
    return records


def evidence_records_from_payload(payload: Any) -> list[EvidenceRecord]:
    records: list[EvidenceRecord] = []
    for item in list(payload or []):
        if not isinstance(item, dict):
            continue
        try:
            records.append(EvidenceRecord(**item))
        except TypeError:
            continue
    return records


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
