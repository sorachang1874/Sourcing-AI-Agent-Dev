from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, fields
from typing import Any

from .candidate_artifacts import (
    CandidateArtifactError,
    build_evidence_records_from_payloads,
    load_authoritative_company_snapshot_candidate_documents,
)
from .domain import Candidate, EvidenceRecord
from .results_store import open_snapshot_artifact_store, read_snapshot_candidate_shard
from .storage import ControlPlaneStore

_CANDIDATE_FIELD_NAMES = {field.name for field in fields(Candidate)}


@dataclass(frozen=True, slots=True)
class AuthoritativeCandidateSnapshot:
    target_company: str
    company_key: str
    snapshot_id: str
    asset_view: str
    source_kind: str
    source_path: str
    company_identity: dict[str, Any]
    candidates: list[Candidate]
    candidate_lookup: dict[str, Candidate]
    evidence_records: list[EvidenceRecord]
    evidence_lookup: dict[str, list[dict[str, Any]]]


def load_authoritative_candidate_snapshot(
    *,
    runtime_dir: str,
    target_company: str,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    store: ControlPlaneStore | None = None,
    allow_materialization_fallback: bool = True,
    allow_candidate_documents_fallback: bool | None = None,
) -> AuthoritativeCandidateSnapshot:
    snapshot_payload = load_authoritative_company_snapshot_candidate_documents(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        view=asset_view,
        allow_materialization_fallback=allow_materialization_fallback,
        allow_candidate_documents_fallback=allow_candidate_documents_fallback,
    )
    candidates = [candidate for candidate in list(snapshot_payload.get("candidates") or []) if isinstance(candidate, Candidate)]
    candidate_lookup = {
        str(candidate.candidate_id or "").strip(): candidate
        for candidate in candidates
        if str(candidate.candidate_id or "").strip()
    }
    evidence_records = build_evidence_records_from_payloads(list(snapshot_payload.get("evidence") or []))
    evidence_lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for evidence in evidence_records:
        candidate_id = str(evidence.candidate_id or "").strip()
        if not candidate_id:
            continue
        evidence_lookup[candidate_id].append(evidence.to_record())
    return AuthoritativeCandidateSnapshot(
        target_company=str(snapshot_payload.get("target_company") or target_company).strip() or target_company,
        company_key=str(snapshot_payload.get("company_key") or "").strip(),
        snapshot_id=str(snapshot_payload.get("snapshot_id") or snapshot_id).strip(),
        asset_view=str(snapshot_payload.get("asset_view") or asset_view).strip() or "canonical_merged",
        source_kind=str(snapshot_payload.get("source_kind") or "").strip(),
        source_path=str(snapshot_payload.get("source_path") or "").strip(),
        company_identity=dict(snapshot_payload.get("company_identity") or {}),
        candidates=candidates,
        candidate_lookup=candidate_lookup,
        evidence_records=evidence_records,
        evidence_lookup=dict(evidence_lookup),
    )


def load_authoritative_candidate_detail(
    *,
    runtime_dir: str,
    target_company: str,
    candidate_id: str,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    store: ControlPlaneStore | None = None,
    allow_materialization_fallback: bool = True,
    allow_candidate_documents_fallback: bool | None = None,
) -> dict[str, Any]:
    normalized_candidate_id = str(candidate_id or "").strip()
    if not normalized_candidate_id:
        raise CandidateArtifactError("candidate_id is required")

    try:
        artifact_store = open_snapshot_artifact_store(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
        )
        shard_payload = read_snapshot_candidate_shard(artifact_store, candidate_id=normalized_candidate_id)
        candidate = _candidate_from_payload(
            dict(
                shard_payload.get("candidate")
                or shard_payload.get("materialized_candidate")
                or shard_payload.get("normalized_candidate")
                or {}
            )
        )
        evidence_records = build_evidence_records_from_payloads(list(shard_payload.get("evidence") or []))
        if candidate is not None:
            return {
                "status": "loaded",
                "target_company": artifact_store.target_company,
                "company_key": artifact_store.company_key,
                "snapshot_id": artifact_store.snapshot_id,
                "asset_view": artifact_store.asset_view,
                "source_kind": "candidate_shard",
                "candidate": candidate,
                "evidence_records": evidence_records,
                "evidence": [item.to_record() for item in evidence_records],
            }
    except CandidateArtifactError:
        pass

    snapshot = load_authoritative_candidate_snapshot(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        store=store,
        allow_materialization_fallback=allow_materialization_fallback,
        allow_candidate_documents_fallback=allow_candidate_documents_fallback,
    )
    candidate = snapshot.candidate_lookup.get(normalized_candidate_id)
    return {
        "status": "loaded" if candidate is not None else "missing",
        "target_company": snapshot.target_company,
        "company_key": snapshot.company_key,
        "snapshot_id": snapshot.snapshot_id,
        "asset_view": snapshot.asset_view,
        "source_kind": snapshot.source_kind,
        "candidate": candidate,
        "evidence_records": build_evidence_records_from_payloads(
            list(snapshot.evidence_lookup.get(normalized_candidate_id) or [])
        ),
        "evidence": list(snapshot.evidence_lookup.get(normalized_candidate_id) or []),
    }


def load_authoritative_candidate_details(
    *,
    runtime_dir: str,
    target_company: str,
    candidate_ids: list[str],
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    store: ControlPlaneStore | None = None,
    allow_materialization_fallback: bool = True,
    allow_candidate_documents_fallback: bool | None = None,
) -> dict[str, dict[str, Any]]:
    normalized_candidate_ids = [
        str(candidate_id or "").strip() for candidate_id in list(candidate_ids or []) if str(candidate_id or "").strip()
    ]
    if not normalized_candidate_ids:
        return {}

    details: dict[str, dict[str, Any]] = {}
    missing_candidate_ids = list(normalized_candidate_ids)
    try:
        artifact_store = open_snapshot_artifact_store(
            runtime_dir=runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
        )
    except CandidateArtifactError:
        artifact_store = None
    if artifact_store is not None:
        unresolved_candidate_ids: list[str] = []
        for normalized_candidate_id in normalized_candidate_ids:
            try:
                shard_payload = read_snapshot_candidate_shard(artifact_store, candidate_id=normalized_candidate_id)
            except CandidateArtifactError:
                unresolved_candidate_ids.append(normalized_candidate_id)
                continue
            candidate = _candidate_from_payload(
                dict(
                    shard_payload.get("candidate")
                    or shard_payload.get("materialized_candidate")
                    or shard_payload.get("normalized_candidate")
                    or {}
                )
            )
            if candidate is None:
                unresolved_candidate_ids.append(normalized_candidate_id)
                continue
            evidence_records = build_evidence_records_from_payloads(list(shard_payload.get("evidence") or []))
            details[normalized_candidate_id] = {
                "status": "loaded",
                "target_company": artifact_store.target_company,
                "company_key": artifact_store.company_key,
                "snapshot_id": artifact_store.snapshot_id,
                "asset_view": artifact_store.asset_view,
                "source_kind": "candidate_shard",
                "candidate": candidate,
                "evidence_records": evidence_records,
                "evidence": [item.to_record() for item in evidence_records],
            }
        missing_candidate_ids = unresolved_candidate_ids
    if not missing_candidate_ids:
        return details

    snapshot = load_authoritative_candidate_snapshot(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        store=store,
        allow_materialization_fallback=allow_materialization_fallback,
        allow_candidate_documents_fallback=allow_candidate_documents_fallback,
    )
    for normalized_candidate_id in missing_candidate_ids:
        candidate = snapshot.candidate_lookup.get(normalized_candidate_id)
        details[normalized_candidate_id] = {
            "status": "loaded" if candidate is not None else "missing",
            "target_company": snapshot.target_company,
            "company_key": snapshot.company_key,
            "snapshot_id": snapshot.snapshot_id,
            "asset_view": snapshot.asset_view,
            "source_kind": snapshot.source_kind,
            "candidate": candidate,
            "evidence_records": build_evidence_records_from_payloads(
                list(snapshot.evidence_lookup.get(normalized_candidate_id) or [])
            ),
            "evidence": list(snapshot.evidence_lookup.get(normalized_candidate_id) or []),
        }
    return details


def _candidate_from_payload(payload: dict[str, Any]) -> Candidate | None:
    if not isinstance(payload, dict):
        return None
    normalized = {
        field_name: payload.get(field_name)
        for field_name in _CANDIDATE_FIELD_NAMES
        if field_name in payload
    }
    normalized["candidate_id"] = str(normalized.get("candidate_id") or "").strip()
    normalized["name_en"] = str(normalized.get("name_en") or "").strip()
    if not normalized["candidate_id"] or not normalized["name_en"]:
        return None
    metadata = normalized.get("metadata")
    normalized["metadata"] = dict(metadata or {}) if isinstance(metadata, dict) else {}
    return Candidate(**normalized)
