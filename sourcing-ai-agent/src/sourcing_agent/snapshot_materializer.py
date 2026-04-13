from __future__ import annotations
from pathlib import Path
from typing import Any

from .acquisition import AcquisitionEngine
from .asset_logger import AssetLogger
from .candidate_artifacts import build_company_candidate_artifacts
from .connectors import CompanyIdentity
from .domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, normalize_name_token
from .seed_discovery import SearchSeedSnapshot, build_candidates_from_seed_snapshot
from .snapshot_state import (
    company_identity_from_record as _company_identity_from_record,
    load_candidate_document_state as _load_candidate_document_state,
    merge_background_reconcile_candidate as _merge_background_reconcile_candidate,
    read_json_dict as _read_json_dict,
    read_json_list as _read_json_list,
)
from .storage import SQLiteStore


class SnapshotMaterializer:
    def __init__(
        self,
        *,
        runtime_dir: Path,
        store: SQLiteStore,
        acquisition_engine: AcquisitionEngine,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.store = store
        self.acquisition_engine = acquisition_engine

    def apply_search_seed_workers_to_snapshot(
        self,
        *,
        snapshot_dir: Path,
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        discovery_dir = snapshot_dir / "search_seed_discovery"
        summary_path = discovery_dir / "summary.json"
        entries_path = discovery_dir / "entries.json"
        if not summary_path.exists():
            return {"status": "skipped", "reason": "search_seed_summary_missing"}

        existing_summary = _read_json_dict(summary_path)
        existing_entries = _read_json_list(entries_path)
        original_entry_count = len(existing_entries)
        existing_query_summaries = [
            dict(item)
            for item in list(existing_summary.get("query_summaries") or [])
            if isinstance(item, dict)
        ]
        existing_errors = [
            str(item or "").strip()
            for item in list(existing_summary.get("errors") or [])
            if str(item or "").strip()
        ]
        applied_worker_ids: list[int] = []
        query_summaries = list(existing_query_summaries)
        entries = list(existing_entries)
        errors = list(existing_errors)

        for worker in sorted(
            pending_workers,
            key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)),
        ):
            applied_worker_ids.append(int(worker.get("worker_id") or 0))
            output = dict(worker.get("output") or {})
            worker_entries = [dict(item) for item in list(output.get("entries") or []) if isinstance(item, dict)]
            if worker_entries:
                entries.extend(worker_entries)
            worker_summary = dict(output.get("summary") or {})
            if worker_summary:
                query_summaries = _upsert_search_seed_query_summaries(query_summaries, [worker_summary])
            worker_errors = [
                str(item or "").strip()
                for item in list(output.get("errors") or [])
                if str(item or "").strip()
            ]
            if worker_errors:
                errors.extend(worker_errors)

        deduped_entries = _dedupe_search_seed_entries(entries)
        deduped_errors = _dedupe_preserving_order(errors)
        queued_query_count = sum(
            1 for item in query_summaries if str(item.get("status") or "").strip().lower() == "queued"
        )
        stop_reason = _resolve_search_seed_reconcile_stop_reason(
            previous_stop_reason=str(existing_summary.get("stop_reason") or "").strip(),
            query_summaries=query_summaries,
        )
        identity = _company_identity_from_record(dict(existing_summary.get("company_identity") or {}))
        if identity is None:
            return {"status": "skipped", "reason": "company_identity_missing"}

        logger = AssetLogger(snapshot_dir)
        logger.write_json(
            entries_path,
            deduped_entries,
            asset_type="search_seed_entries",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        updated_summary_payload = {
            **existing_summary,
            "snapshot_id": snapshot_dir.name,
            "target_company": identity.canonical_name,
            "company_identity": identity.to_record(),
            "entry_count": len(deduped_entries),
            "query_summaries": query_summaries,
            "errors": deduped_errors,
            "stop_reason": stop_reason,
            "queued_query_count": queued_query_count,
        }
        logger.write_json(
            summary_path,
            updated_summary_payload,
            asset_type="search_seed_summary",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=deduped_entries,
            query_summaries=query_summaries,
            accounts_used=[
                str(item or "").strip()
                for item in list(updated_summary_payload.get("accounts_used") or [])
                if str(item or "").strip()
            ],
            errors=deduped_errors,
            stop_reason=stop_reason,
            summary_path=summary_path,
            entries_path=entries_path,
        )
        search_candidates, search_evidence = build_candidates_from_seed_snapshot(search_seed_snapshot)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        existing_candidate_doc_payload = _read_json_dict(candidate_doc_path)
        existing_candidate_state = _load_candidate_document_state(candidate_doc_path) if candidate_doc_path.exists() else {}
        merged_candidates = {
            candidate.candidate_id: candidate
            for candidate in list(existing_candidate_state.get("candidates") or [])
            if isinstance(candidate, Candidate)
        }
        for candidate in search_candidates:
            existing_candidate = merged_candidates.get(candidate.candidate_id)
            merged_candidates[candidate.candidate_id] = _merge_background_reconcile_candidate(existing_candidate, candidate)
        merged_evidence = {
            evidence.evidence_id: evidence
            for evidence in list(existing_candidate_state.get("evidence") or [])
            if isinstance(evidence, EvidenceRecord)
        }
        for evidence in search_evidence:
            merged_evidence[evidence.evidence_id] = evidence
        merged_candidate_list = sorted(list(merged_candidates.values()), key=lambda item: item.display_name)
        merged_evidence_list = list(merged_evidence.values())
        logger.write_json(
            candidate_doc_path,
            {
                **existing_candidate_doc_payload,
                "candidates": [item.to_record() for item in merged_candidate_list],
                "evidence": [item.to_record() for item in merged_evidence_list],
                "candidate_count": len(merged_candidate_list),
                "evidence_count": len(merged_evidence_list),
                "search_seed_background_reconcile": {
                    "applied_worker_count": len(pending_workers),
                    "applied_worker_ids": applied_worker_ids,
                    "added_entry_count": max(len(deduped_entries) - original_entry_count, 0),
                    "queued_query_count": queued_query_count,
                    "stop_reason": stop_reason,
                },
            },
            asset_type="candidate_documents",
            source_kind="background_search_seed_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        return {
            "status": "applied",
            "snapshot_id": snapshot_dir.name,
            "search_seed_snapshot": search_seed_snapshot,
            "candidate_doc_path": candidate_doc_path,
            "applied_worker_count": len(pending_workers),
            "worker_ids": applied_worker_ids,
            "entry_count": len(deduped_entries),
            "added_entry_count": max(len(deduped_entries) - original_entry_count, 0),
            "queued_query_count": queued_query_count,
            "stop_reason": stop_reason,
            "candidate_count": len(merged_candidate_list),
            "evidence_count": len(merged_evidence_list),
        }

    def synchronize_snapshot_candidate_documents(
        self,
        *,
        request: JobRequest,
        snapshot_dir: Path,
        reason: str,
    ) -> dict[str, Any]:
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if not candidate_doc_path.exists():
            return {"status": "skipped", "reason": "candidate_documents_missing"}
        candidate_document_state = _load_candidate_document_state(candidate_doc_path)
        candidates = list(candidate_document_state.get("candidates") or [])
        evidence = list(candidate_document_state.get("evidence") or [])
        if not candidates:
            return {"status": "skipped", "reason": "candidate_documents_empty"}
        identity = resolve_snapshot_company_identity(snapshot_dir, fallback_target_company=request.target_company)
        if identity is None:
            return {"status": "skipped", "reason": "company_identity_missing"}
        normalization_task = AcquisitionTask(
            task_id=f"normalize::{reason}",
            task_type="normalize_asset_snapshot",
            title="Synchronize snapshot candidates",
            description="Synchronize candidate_documents into normalized store and artifacts.",
            status="ready",
            blocking=False,
            metadata={"cost_policy": {}},
        )
        normalization_execution = self.acquisition_engine._normalize_snapshot(
            normalization_task,
            {
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "company_identity": identity,
                "candidate_doc_path": candidate_doc_path,
                "candidates": candidates,
                "evidence": evidence,
            },
            job_request=request,
        )
        if str(normalization_execution.status or "") != "completed":
            return {
                "status": "failed",
                "reason": "normalization_not_completed",
                "detail": str(normalization_execution.detail or ""),
            }
        preferred_source_snapshot_ids = [
            str(item or "").strip()
            for item in list(dict(request.execution_preferences or {}).get("delta_baseline_snapshot_ids") or [])
            if str(item or "").strip()
        ]
        artifact_build = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company=request.target_company,
            snapshot_id=snapshot_dir.name,
            preferred_source_snapshot_ids=preferred_source_snapshot_ids or None,
        )
        return {
            "status": "completed",
            "snapshot_id": snapshot_dir.name,
            "candidate_count": int(normalization_execution.payload.get("candidate_count") or len(candidates)),
            "evidence_count": int(normalization_execution.payload.get("evidence_count") or len(evidence)),
            "manifest_path": str(normalization_execution.payload.get("manifest_path") or ""),
            "artifact_dir": str(artifact_build.get("artifact_dir") or ""),
            "artifact_paths": dict(artifact_build.get("artifact_paths") or {}),
            "state_updates": {
                **dict(normalization_execution.state_updates or {}),
                "candidate_doc_path": candidate_doc_path,
            },
        }


def resolve_snapshot_company_identity(snapshot_dir: Path, *, fallback_target_company: str = "") -> CompanyIdentity | None:
    identity = _company_identity_from_record(_read_json_dict(snapshot_dir / "identity.json"))
    if identity is not None:
        return identity
    candidate_doc_payload = _read_json_dict(snapshot_dir / "candidate_documents.json")
    snapshot_payload = dict(candidate_doc_payload.get("snapshot") or {})
    identity = _company_identity_from_record(dict(snapshot_payload.get("company_identity") or {}))
    if identity is not None:
        return identity
    acquisition_sources = dict(candidate_doc_payload.get("acquisition_sources") or {})
    for key in ("roster_snapshot", "search_seed_snapshot"):
        source_payload = dict(acquisition_sources.get(key) or {})
        identity = _company_identity_from_record(dict(source_payload.get("company_identity") or {}))
        if identity is not None:
            return identity
    search_seed_summary = _read_json_dict(snapshot_dir / "search_seed_discovery" / "summary.json")
    identity = _company_identity_from_record(dict(search_seed_summary.get("company_identity") or {}))
    if identity is not None:
        return identity
    normalized_target_company = str(fallback_target_company or "").strip()
    if not normalized_target_company:
        return None
    company_key = normalize_name_token(normalized_target_company)
    return CompanyIdentity(
        requested_name=normalized_target_company,
        canonical_name=normalized_target_company,
        company_key=company_key,
        linkedin_slug="",
    )


def _search_seed_query_summary_key(summary: dict[str, Any]) -> str:
    return "|".join(
        [
            str(summary.get("query") or "").strip().lower(),
            str(summary.get("bundle_id") or "").strip().lower(),
            str(summary.get("source_family") or "").strip().lower(),
            str(summary.get("execution_mode") or "").strip().lower(),
            str(summary.get("mode") or "").strip().lower(),
        ]
    )


def _upsert_search_seed_query_summaries(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = [dict(item) for item in existing if isinstance(item, dict)]
    index_by_key = {
        _search_seed_query_summary_key(item): position
        for position, item in enumerate(merged)
        if _search_seed_query_summary_key(item)
    }
    for item in incoming:
        if not isinstance(item, dict):
            continue
        summary = dict(item)
        key = _search_seed_query_summary_key(summary)
        if key and key in index_by_key:
            merged[index_by_key[key]] = summary
            continue
        merged.append(summary)
        if key:
            index_by_key[key] = len(merged) - 1
    return merged


def _dedupe_search_seed_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in entries:
        if not isinstance(item, dict):
            continue
        key = str(item.get("seed_key") or "").strip()
        if not key:
            key = "|".join(
                [
                    normalize_name_token(str(item.get("full_name") or "")),
                    normalize_name_token(str(item.get("profile_url") or item.get("source_query") or "")),
                ]
            )
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(dict(item))
    return deduped


def _dedupe_preserving_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in values:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _resolve_search_seed_reconcile_stop_reason(
    *,
    previous_stop_reason: str,
    query_summaries: list[dict[str, Any]],
) -> str:
    if any(str(item.get("status") or "").strip().lower() == "queued" for item in query_summaries):
        return "queued_background_search"
    modes = {
        str(item.get("mode") or "").strip().lower()
        for item in query_summaries
        if str(item.get("mode") or "").strip()
    }
    if modes.intersection({"harvest_profile_search", "provider_people_search"}):
        return "provider_people_search_fallback"
    normalized_previous = str(previous_stop_reason or "").strip()
    if normalized_previous and normalized_previous != "queued_background_search":
        return normalized_previous
    return "completed"
