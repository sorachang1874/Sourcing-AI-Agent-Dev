from __future__ import annotations
from collections import defaultdict
from dataclasses import replace
from pathlib import Path
from typing import Any

from .acquisition import AcquisitionEngine
from .asset_logger import AssetLogger
from .candidate_artifacts import build_company_candidate_artifacts
from .connectors import CompanyIdentity, CompanyRosterSnapshot, build_candidates_from_roster
from .domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, normalize_name_token
from .enrichment import (
    _apply_non_member_profile,
    _apply_verified_profile,
    _candidate_profile_urls,
    _load_harvest_profile_payload_from_registry_or_snapshot,
    _names_match,
    _profile_matches_candidate,
    extract_linkedin_slug,
)
from .model_provider import ModelClient
from .seed_discovery import SearchSeedSnapshot, build_candidates_from_seed_snapshot
from .snapshot_state import (
    company_identity_from_record as _company_identity_from_record,
    load_candidate_document_state as _load_candidate_document_state,
    merge_background_reconcile_candidate as _merge_background_reconcile_candidate,
    read_json_dict as _read_json_dict,
    read_json_list as _read_json_list,
)
from .storage import SQLiteStore
from .runtime_tuning import resolved_materialization_global_writer_budget, runtime_inflight_slot


class SnapshotMaterializer:
    def __init__(
        self,
        *,
        runtime_dir: Path,
        store: SQLiteStore,
        acquisition_engine: AcquisitionEngine,
        model_client: ModelClient | None = None,
    ) -> None:
        self.runtime_dir = Path(runtime_dir)
        self.store = store
        self.acquisition_engine = acquisition_engine
        self.model_client = model_client

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

    def apply_company_roster_workers_to_snapshot(
        self,
        *,
        snapshot_dir: Path,
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        harvest_dir = snapshot_dir / "harvest_company_employees"
        merged_path = harvest_dir / "harvest_company_employees_merged.json"
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        headless_path = harvest_dir / "harvest_company_employees_headless.json"
        summary_path = harvest_dir / "harvest_company_employees_summary.json"
        raw_manifest_path = harvest_dir / "harvest_company_employees_raw.json"

        existing_summary = _read_json_dict(summary_path)
        identity = _resolve_company_roster_identity(existing_summary, pending_workers, snapshot_dir=snapshot_dir)
        if identity is None:
            return {"status": "skipped", "reason": "company_identity_missing"}

        original_entries = _read_json_list(merged_path)
        merged_entries: list[dict[str, Any]] = [dict(item) for item in original_entries if isinstance(item, dict)]
        page_summaries = [
            dict(item)
            for item in list(existing_summary.get("page_summaries") or [])
            if isinstance(item, dict)
        ]
        shard_summaries = [
            dict(item)
            for item in list(existing_summary.get("shard_summaries") or [])
            if isinstance(item, dict)
        ]
        errors = [
            str(item or "").strip()
            for item in list(existing_summary.get("errors") or [])
            if str(item or "").strip()
        ]
        applied_worker_ids: list[int] = []
        seen_keys = {
            _company_roster_entry_key(item)
            for item in merged_entries
            if _company_roster_entry_key(item)
        }
        original_entry_count = len(merged_entries)

        for worker in sorted(
            pending_workers,
            key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)),
        ):
            applied_worker_ids.append(int(worker.get("worker_id") or 0))
            worker_result = _materialize_company_roster_worker_snapshot(
                acquisition_engine=self.acquisition_engine,
                worker=worker,
                fallback_identity=identity,
            )
            worker_errors = [
                str(item or "").strip()
                for item in list(worker_result.get("errors") or [])
                if str(item or "").strip()
            ]
            if worker_errors:
                errors.extend(worker_errors)
            worker_snapshot = worker_result.get("roster_snapshot")
            if not isinstance(worker_snapshot, CompanyRosterSnapshot):
                continue
            worker_summary = dict(worker_result.get("worker_summary") or {})
            worker_entries = _annotate_company_roster_entries(
                worker_snapshot.raw_entries,
                worker_summary=worker_summary,
            )
            for entry in worker_entries:
                member_key = _company_roster_entry_key(entry)
                if not member_key or member_key in seen_keys:
                    continue
                seen_keys.add(member_key)
                merged_entries.append(entry)
            page_summaries.extend(
                _annotate_company_roster_page_summaries(
                    worker_snapshot.page_summaries,
                    worker_summary=worker_summary,
                )
            )
            shard_summaries = _upsert_company_roster_shard_summaries(
                shard_summaries,
                [_build_company_roster_shard_summary(worker_snapshot, worker_summary=worker_summary)],
            )

        if not merged_entries and not original_entries:
            return {"status": "skipped", "reason": "company_roster_entries_missing"}

        logger = AssetLogger(snapshot_dir)
        visible_entries = [item for item in merged_entries if not bool(item.get("is_headless"))]
        headless_entries = [item for item in merged_entries if bool(item.get("is_headless"))]
        expected_shard_ids = _expected_segmented_company_roster_shard_ids(snapshot_dir)
        available_shard_ids = sorted(
            {
                str(item.get("shard_id") or "").strip()
                for item in shard_summaries
                if str(item.get("shard_id") or "").strip()
            }
        )
        completion_status = "completed"
        stop_reason = str(existing_summary.get("stop_reason") or "").strip() or "completed_background_company_roster"
        if expected_shard_ids:
            completion_status = "completed" if set(expected_shard_ids).issubset(set(available_shard_ids)) else "partial"
            stop_reason = (
                "completed_segmented_background_company_roster"
                if completion_status == "completed"
                else "partial_segmented_background_company_roster"
            )

        logger.write_json(
            raw_manifest_path,
            {
                "segmented": bool(expected_shard_ids or available_shard_ids),
                "completion_status": completion_status,
                "expected_shard_count": len(expected_shard_ids),
                "available_shard_count": len(available_shard_ids),
                "available_shard_ids": available_shard_ids,
                "company_identity": identity.to_record(),
                "applied_worker_ids": applied_worker_ids,
                "shards": shard_summaries,
            },
            asset_type="harvest_company_employees_payload",
            source_kind="background_company_roster_reconcile",
            is_raw_asset=True,
            model_safe=False,
        )
        logger.write_json(
            merged_path,
            merged_entries,
            asset_type="company_roster_merged",
            source_kind="background_company_roster_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            visible_path,
            visible_entries,
            asset_type="company_roster_visible",
            source_kind="background_company_roster_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            headless_path,
            headless_entries,
            asset_type="company_roster_headless",
            source_kind="background_company_roster_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        updated_summary_payload = {
            **existing_summary,
            "snapshot_id": snapshot_dir.name,
            "target_company": identity.canonical_name,
            "company_identity": identity.to_record(),
            "segmented": bool(expected_shard_ids or available_shard_ids),
            "completion_status": completion_status,
            "expected_shard_count": len(expected_shard_ids),
            "available_shard_count": len(available_shard_ids),
            "available_shard_ids": available_shard_ids,
            "raw_entry_count": len(merged_entries),
            "visible_entry_count": len(visible_entries),
            "headless_entry_count": len(headless_entries),
            "page_summaries": page_summaries,
            "shard_summaries": shard_summaries,
            "accounts_used": _dedupe_preserving_order(
                [
                    *[
                        str(item or "").strip()
                        for item in list(existing_summary.get("accounts_used") or [])
                        if str(item or "").strip()
                    ],
                    "harvest_company_employees",
                ]
            ),
            "errors": _dedupe_preserving_order(errors),
            "stop_reason": stop_reason,
            "background_company_roster_reconcile": {
                "applied_worker_count": len(pending_workers),
                "applied_worker_ids": applied_worker_ids,
                "added_entry_count": max(len(merged_entries) - original_entry_count, 0),
            },
        }
        logger.write_json(
            summary_path,
            updated_summary_payload,
            asset_type="company_roster_summary",
            source_kind="background_company_roster_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company=identity.canonical_name,
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=merged_entries,
            visible_entries=visible_entries,
            headless_entries=headless_entries,
            page_summaries=page_summaries,
            accounts_used=list(updated_summary_payload.get("accounts_used") or []),
            errors=list(updated_summary_payload.get("errors") or []),
            stop_reason=stop_reason,
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=summary_path,
        )

        roster_candidates, roster_evidence = build_candidates_from_roster(roster_snapshot)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        existing_candidate_doc_payload = _read_json_dict(candidate_doc_path)
        existing_candidate_state = _load_candidate_document_state(candidate_doc_path) if candidate_doc_path.exists() else {}
        merged_candidates = {
            candidate.candidate_id: candidate
            for candidate in list(existing_candidate_state.get("candidates") or [])
            if isinstance(candidate, Candidate)
        }
        for candidate in roster_candidates:
            existing_candidate = merged_candidates.get(candidate.candidate_id)
            merged_candidates[candidate.candidate_id] = _merge_background_reconcile_candidate(existing_candidate, candidate)
        merged_evidence = {
            evidence.evidence_id: evidence
            for evidence in list(existing_candidate_state.get("evidence") or [])
            if isinstance(evidence, EvidenceRecord)
        }
        for evidence in roster_evidence:
            merged_evidence[evidence.evidence_id] = evidence
        merged_candidate_list = sorted(list(merged_candidates.values()), key=lambda item: item.display_name)
        merged_evidence_list = list(merged_evidence.values())
        logger.write_json(
            candidate_doc_path,
            {
                **existing_candidate_doc_payload,
                "snapshot_id": snapshot_dir.name,
                "target_company": identity.canonical_name,
                "snapshot": {
                    **dict(existing_candidate_doc_payload.get("snapshot") or {}),
                    "company_identity": identity.to_record(),
                },
                "candidates": [item.to_record() for item in merged_candidate_list],
                "evidence": [item.to_record() for item in merged_evidence_list],
                "candidate_count": len(merged_candidate_list),
                "evidence_count": len(merged_evidence_list),
                "company_roster_background_reconcile": {
                    "applied_worker_count": len(pending_workers),
                    "applied_worker_ids": applied_worker_ids,
                    "added_entry_count": max(len(merged_entries) - original_entry_count, 0),
                    "completion_status": completion_status,
                    "available_shard_count": len(available_shard_ids),
                },
            },
            asset_type="candidate_documents",
            source_kind="background_company_roster_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        return {
            "status": "applied",
            "snapshot_id": snapshot_dir.name,
            "roster_snapshot": roster_snapshot,
            "candidate_doc_path": candidate_doc_path,
            "applied_worker_count": len(pending_workers),
            "worker_ids": applied_worker_ids,
            "entry_count": len(merged_entries),
            "added_entry_count": max(len(merged_entries) - original_entry_count, 0),
            "candidate_count": len(merged_candidate_list),
            "evidence_count": len(merged_evidence_list),
            "completion_status": completion_status,
            "available_shard_count": len(available_shard_ids),
            "expected_shard_count": len(expected_shard_ids),
            "stop_reason": stop_reason,
        }

    def apply_harvest_profile_workers_to_snapshot(
        self,
        *,
        snapshot_dir: Path,
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if not candidate_doc_path.exists():
            return {"status": "skipped", "reason": "candidate_documents_missing"}

        existing_candidate_doc_payload = _read_json_dict(candidate_doc_path)
        existing_candidate_state = _load_candidate_document_state(candidate_doc_path)
        existing_candidates = [
            candidate
            for candidate in list(existing_candidate_state.get("candidates") or [])
            if isinstance(candidate, Candidate)
        ]
        if not existing_candidates:
            return {"status": "skipped", "reason": "candidate_documents_empty"}

        identity = resolve_snapshot_company_identity(
            snapshot_dir,
            fallback_target_company=str(existing_candidate_doc_payload.get("target_company") or ""),
        )
        if identity is None:
            return {"status": "skipped", "reason": "company_identity_missing"}

        candidate_by_id = {
            candidate.candidate_id: candidate
            for candidate in existing_candidates
            if str(candidate.candidate_id or "").strip()
        }
        evidence_by_id = {
            evidence.evidence_id: evidence
            for evidence in list(existing_candidate_state.get("evidence") or [])
            if isinstance(evidence, EvidenceRecord) and str(evidence.evidence_id or "").strip()
        }
        candidate_ids_by_profile_url: dict[str, list[str]] = defaultdict(list)
        for candidate in existing_candidates:
            candidate_id = str(candidate.candidate_id or "").strip()
            if not candidate_id:
                continue
            for profile_url in _candidate_profile_urls(candidate):
                normalized_profile_url = str(profile_url or "").strip()
                if normalized_profile_url and candidate_id not in candidate_ids_by_profile_url[normalized_profile_url]:
                    candidate_ids_by_profile_url[normalized_profile_url].append(candidate_id)

        requested_profile_urls = _dedupe_preserving_order(
            [
                profile_url
                for worker in sorted(
                    pending_workers,
                    key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)),
                )
                for profile_url in _worker_requested_profile_urls(worker)
            ]
        )
        if not requested_profile_urls:
            return {"status": "skipped", "reason": "requested_profile_urls_missing"}

        registry_entries: dict[str, dict[str, Any]] = {}
        if self.store is not None:
            registry_entries = self.store.get_linkedin_profile_registry_bulk(requested_profile_urls)

        applied_worker_ids: list[int] = []
        resolved_candidate_ids: list[str] = []
        manual_review_candidate_ids: list[str] = []
        non_member_candidate_ids: list[str] = []
        fetched_profile_urls: list[str] = []
        unmatched_profile_urls: list[str] = []
        seen_profile_urls: set[str] = set()

        for worker in sorted(
            pending_workers,
            key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)),
        ):
            applied_worker_ids.append(int(worker.get("worker_id") or 0))
            for requested_profile_url in _worker_requested_profile_urls(worker):
                normalized_requested_profile_url = str(requested_profile_url or "").strip()
                if not normalized_requested_profile_url or normalized_requested_profile_url in seen_profile_urls:
                    continue
                seen_profile_urls.add(normalized_requested_profile_url)
                normalized_registry_key = normalized_requested_profile_url
                if self.store is not None:
                    normalized_registry_key = self.store.normalize_linkedin_profile_url(normalized_requested_profile_url)
                payload = _load_harvest_profile_payload_from_registry_or_snapshot(
                    registry_entry=dict(registry_entries.get(normalized_registry_key) or {}),
                    snapshot_dir=snapshot_dir,
                    profile_url=normalized_requested_profile_url,
                    normalized_profile_key=normalized_registry_key,
                )
                if payload is None:
                    unmatched_profile_urls.append(normalized_requested_profile_url)
                    continue

                parsed = dict(payload.get("parsed") or {})
                raw_path = Path(str(payload.get("raw_path") or "")).expanduser()
                account_id = str(payload.get("account_id") or "harvest_profile_registry").strip() or "harvest_profile_registry"
                fetched_profile_urls.append(normalized_requested_profile_url)
                prioritized_candidate_ids = _prioritized_candidate_ids_for_profile(
                    requested_profile_url=normalized_requested_profile_url,
                    profile_payload=parsed,
                    candidate_ids_by_profile_url=candidate_ids_by_profile_url,
                    candidate_by_id=candidate_by_id,
                )
                matched_candidate_id = ""

                for candidate_id in prioritized_candidate_ids:
                    candidate = candidate_by_id.get(candidate_id)
                    if candidate is None:
                        continue
                    if not _profile_matches_candidate(parsed, candidate, identity, model_client=self.model_client):
                        continue
                    merged_candidate, resolved_profile, evidence = _apply_verified_profile(
                        candidate,
                        parsed,
                        raw_path,
                        account_id,
                        extract_linkedin_slug(normalized_requested_profile_url),
                        identity,
                        model_client=self.model_client,
                        resolution_source="background_harvest_prefetch_reconcile",
                    )
                    candidate_by_id[candidate_id] = merged_candidate
                    for profile_url in _candidate_profile_urls(merged_candidate):
                        normalized_profile_url = str(profile_url or "").strip()
                        if normalized_profile_url and candidate_id not in candidate_ids_by_profile_url[normalized_profile_url]:
                            candidate_ids_by_profile_url[normalized_profile_url].append(candidate_id)
                    for evidence_record in evidence:
                        evidence_by_id[evidence_record.evidence_id] = evidence_record
                    matched_candidate_id = candidate_id
                    resolved_candidate_ids.append(candidate_id)
                    if bool(resolved_profile.get("membership_review_required")):
                        manual_review_candidate_ids.append(candidate_id)
                    break

                if matched_candidate_id:
                    continue

                for candidate_id in prioritized_candidate_ids:
                    candidate = candidate_by_id.get(candidate_id)
                    if candidate is None:
                        continue
                    if not _names_match(candidate.name_en, str(parsed.get("full_name") or "")):
                        continue
                    merged_candidate, _, evidence = _apply_non_member_profile(
                        candidate,
                        parsed,
                        raw_path,
                        account_id,
                    )
                    candidate_by_id[candidate_id] = merged_candidate
                    for profile_url in _candidate_profile_urls(merged_candidate):
                        normalized_profile_url = str(profile_url or "").strip()
                        if normalized_profile_url and candidate_id not in candidate_ids_by_profile_url[normalized_profile_url]:
                            candidate_ids_by_profile_url[normalized_profile_url].append(candidate_id)
                    for evidence_record in evidence:
                        evidence_by_id[evidence_record.evidence_id] = evidence_record
                    matched_candidate_id = candidate_id
                    non_member_candidate_ids.append(candidate_id)
                    break

                if not matched_candidate_id:
                    unmatched_profile_urls.append(normalized_requested_profile_url)

        if not fetched_profile_urls and not resolved_candidate_ids and not non_member_candidate_ids:
            return {
                "status": "skipped",
                "reason": "harvest_profile_payloads_missing",
                "snapshot_id": snapshot_dir.name,
                "applied_worker_count": len(pending_workers),
                "worker_ids": applied_worker_ids,
                "requested_url_count": len(requested_profile_urls),
                "unmatched_profile_url_count": len(_dedupe_preserving_order(unmatched_profile_urls)),
            }

        logger = AssetLogger(snapshot_dir)
        merged_candidate_list = sorted(list(candidate_by_id.values()), key=lambda item: item.display_name)
        merged_evidence_list = list(evidence_by_id.values())
        resolved_candidate_ids = _dedupe_preserving_order(resolved_candidate_ids)
        manual_review_candidate_ids = _dedupe_preserving_order(manual_review_candidate_ids)
        non_member_candidate_ids = _dedupe_preserving_order(non_member_candidate_ids)
        fetched_profile_urls = _dedupe_preserving_order(fetched_profile_urls)
        unmatched_profile_urls = _dedupe_preserving_order(unmatched_profile_urls)

        logger.write_json(
            candidate_doc_path,
            {
                **existing_candidate_doc_payload,
                "snapshot_id": snapshot_dir.name,
                "target_company": identity.canonical_name,
                "snapshot": {
                    **dict(existing_candidate_doc_payload.get("snapshot") or {}),
                    "company_identity": identity.to_record(),
                },
                "candidates": [item.to_record() for item in merged_candidate_list],
                "evidence": [item.to_record() for item in merged_evidence_list],
                "candidate_count": len(merged_candidate_list),
                "evidence_count": len(merged_evidence_list),
                "harvest_prefetch_background_reconcile": {
                    "applied_worker_count": len(pending_workers),
                    "applied_worker_ids": applied_worker_ids,
                    "requested_url_count": len(requested_profile_urls),
                    "fetched_profile_url_count": len(fetched_profile_urls),
                    "resolved_candidate_count": len(resolved_candidate_ids),
                    "manual_review_candidate_count": len(manual_review_candidate_ids),
                    "non_member_candidate_count": len(non_member_candidate_ids),
                    "unmatched_profile_url_count": len(unmatched_profile_urls),
                },
            },
            asset_type="candidate_documents",
            source_kind="background_harvest_prefetch_reconcile",
            is_raw_asset=False,
            model_safe=True,
        )
        return {
            "status": "applied",
            "snapshot_id": snapshot_dir.name,
            "candidate_doc_path": candidate_doc_path,
            "applied_worker_count": len(pending_workers),
            "worker_ids": applied_worker_ids,
            "requested_url_count": len(requested_profile_urls),
            "fetched_profile_url_count": len(fetched_profile_urls),
            "resolved_candidate_count": len(resolved_candidate_ids),
            "manual_review_candidate_count": len(manual_review_candidate_ids),
            "non_member_candidate_count": len(non_member_candidate_ids),
            "candidate_ids": resolved_candidate_ids,
            "manual_review_candidate_ids": manual_review_candidate_ids,
            "non_member_candidate_ids": non_member_candidate_ids,
            "unmatched_profile_urls": unmatched_profile_urls,
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
        with runtime_inflight_slot(
            "materialization_writer",
            budget=resolved_materialization_global_writer_budget(dict(request.execution_preferences or {})),
            metadata={
                "phase": "snapshot_candidate_document_sync",
                "snapshot_id": snapshot_dir.name,
                "reason": str(reason or "").strip(),
                "candidate_count": len(candidates),
            },
        ) as writer_slot:
            normalization_task = AcquisitionTask(
                task_id=f"normalize::{reason}",
                task_type="normalize_asset_snapshot",
                title="Synchronize snapshot candidates",
                description="Synchronize candidate_documents into normalized store and artifacts.",
                status="ready",
                blocking=False,
                metadata={"cost_policy": {}},
            )
            effective_request = request
            if str(reason or "").strip() == "pre_retrieval_refresh":
                execution_preferences = dict(request.execution_preferences or {})
                if not str(execution_preferences.get("artifact_build_profile") or "").strip():
                    effective_request = replace(
                        request,
                        execution_preferences={
                            **execution_preferences,
                            "artifact_build_profile": "foreground_fast",
                        },
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
                job_request=effective_request,
            )
            if str(normalization_execution.status or "") != "completed":
                return {
                    "status": "failed",
                    "reason": "normalization_not_completed",
                    "detail": str(normalization_execution.detail or ""),
                    "materialization_writer_slot": dict(writer_slot),
                }
            artifact_build = {
                "artifact_dir": str(normalization_execution.payload.get("artifact_dir") or ""),
                "artifact_paths": dict(normalization_execution.payload.get("artifact_paths") or {}),
                "sync_status": dict(normalization_execution.payload.get("sync_status") or {}),
            }
            if not artifact_build["artifact_dir"] or not artifact_build["artifact_paths"]:
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
                "sync_status": dict(artifact_build.get("sync_status") or {}),
                "materialization_writer_slot": dict(writer_slot),
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


def _worker_requested_profile_urls(worker: dict[str, Any]) -> list[str]:
    metadata = dict(worker.get("metadata") or {})
    output = dict(worker.get("output") or {})
    summary = dict(output.get("summary") or {})
    return _dedupe_preserving_order(
        [
            str(item or "").strip()
            for item in [
                *list(metadata.get("profile_urls") or []),
                *list(summary.get("requested_urls") or []),
                *list(summary.get("queued_urls") or []),
            ]
            if str(item or "").strip()
        ]
    )


def _prioritized_candidate_ids_for_profile(
    *,
    requested_profile_url: str,
    profile_payload: dict[str, Any],
    candidate_ids_by_profile_url: dict[str, list[str]],
    candidate_by_id: dict[str, Candidate],
) -> list[str]:
    alias_urls = _dedupe_preserving_order(
        [
            requested_profile_url,
            str(profile_payload.get("requested_profile_url") or "").strip(),
            str(profile_payload.get("profile_url") or "").strip(),
            str(profile_payload.get("url") or "").strip(),
        ]
    )
    prioritized: list[str] = []
    seen_candidate_ids: set[str] = set()
    for alias_url in alias_urls:
        for candidate_id in list(candidate_ids_by_profile_url.get(alias_url) or []):
            if candidate_id in seen_candidate_ids:
                continue
            seen_candidate_ids.add(candidate_id)
            prioritized.append(candidate_id)
    for candidate_id in candidate_by_id:
        if candidate_id in seen_candidate_ids:
            continue
        prioritized.append(candidate_id)
    return prioritized


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


def _resolve_company_roster_identity(
    summary_payload: dict[str, Any],
    workers: list[dict[str, Any]],
    *,
    snapshot_dir: Path,
) -> CompanyIdentity | None:
    identity = _company_identity_from_record(dict(summary_payload.get("company_identity") or {}))
    if identity is not None:
        return identity
    identity = resolve_snapshot_company_identity(snapshot_dir)
    if identity is not None:
        return identity
    for worker in workers:
        output = dict(worker.get("output") or {})
        worker_summary = dict(output.get("summary") or {})
        metadata = dict(worker.get("metadata") or {})
        for payload in (worker_summary.get("company_identity"), metadata.get("identity")):
            identity = _company_identity_from_record(dict(payload or {}))
            if identity is not None:
                return identity
    return None


def _company_roster_entry_key(entry: dict[str, Any]) -> str:
    linkedin_url = str(entry.get("linkedin_url") or entry.get("profile_url") or "").strip().lower()
    if linkedin_url:
        return linkedin_url
    member_key = str(entry.get("member_key") or entry.get("member_id") or "").strip().lower()
    if member_key:
        return member_key
    return "|".join(
        [
            str(entry.get("full_name") or "").strip().lower(),
            str(entry.get("headline") or "").strip().lower(),
            str(entry.get("location") or "").strip().lower(),
        ]
    )


def _materialize_company_roster_worker_snapshot(
    *,
    acquisition_engine: AcquisitionEngine,
    worker: dict[str, Any],
    fallback_identity: CompanyIdentity,
) -> dict[str, Any]:
    output = dict(worker.get("output") or {})
    worker_summary = dict(output.get("summary") or {})
    metadata = dict(worker.get("metadata") or {})
    identity = _company_identity_from_record(dict(worker_summary.get("company_identity") or metadata.get("identity") or {}))
    effective_identity = identity or fallback_identity
    snapshot_dir_value = str(worker_summary.get("snapshot_dir") or metadata.get("snapshot_dir") or "").strip()
    if not snapshot_dir_value:
        return {
            "status": "skipped",
            "reason": "worker_snapshot_dir_missing",
            "worker_summary": worker_summary,
            "errors": ["worker_snapshot_dir_missing"],
        }
    worker_snapshot_dir = Path(snapshot_dir_value).expanduser()
    try:
        roster_snapshot = acquisition_engine.harvest_company_connector.fetch_company_roster(
            effective_identity,
            worker_snapshot_dir,
            max_pages=max(1, int(worker_summary.get("requested_pages") or metadata.get("max_pages") or 1)),
            page_limit=max(
                1,
                int(worker_summary.get("requested_item_limit") or metadata.get("page_limit") or 25),
            ),
            company_filters=dict(worker_summary.get("company_filters") or metadata.get("company_filters") or {}),
            allow_shared_provider_cache=False,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "reason": "worker_snapshot_materialization_failed",
            "worker_summary": worker_summary,
            "errors": [f"worker {int(worker.get('worker_id') or 0)}: {exc}"],
        }
    return {
        "status": "completed",
        "roster_snapshot": roster_snapshot,
        "worker_summary": worker_summary,
        "errors": [],
    }


def _annotate_company_roster_entries(
    entries: list[dict[str, Any]],
    *,
    worker_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    shard_id = str(worker_summary.get("shard_id") or "").strip()
    shard_title = str(worker_summary.get("title") or "").strip()
    shard_filters = dict(worker_summary.get("company_filters") or {})
    shard_function_ids = [
        str(item).strip()
        for item in list(shard_filters.get("function_ids") or [])
        if str(item).strip()
    ]
    exclude_function_ids = {
        str(item).strip()
        for item in list(shard_filters.get("exclude_function_ids") or [])
        if str(item).strip()
    }
    shard_function_ids = [item for item in shard_function_ids if item not in exclude_function_ids]

    annotated: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        normalized_entry = dict(entry)
        if shard_id:
            normalized_entry["source_shard_id"] = shard_id
            normalized_entry["source_shard_title"] = shard_title
            normalized_entry["source_shard_filters"] = shard_filters
            if shard_function_ids and not normalized_entry.get("function_ids"):
                normalized_entry["function_ids"] = shard_function_ids
        annotated.append(normalized_entry)
    return annotated


def _annotate_company_roster_page_summaries(
    page_summaries: list[dict[str, Any]],
    *,
    worker_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    shard_id = str(worker_summary.get("shard_id") or "").strip()
    shard_title = str(worker_summary.get("title") or "").strip()
    annotated: list[dict[str, Any]] = []
    for page_summary in page_summaries:
        if not isinstance(page_summary, dict):
            continue
        if not shard_id:
            annotated.append(dict(page_summary))
            continue
        annotated.append(
            {
                **dict(page_summary),
                "shard_id": shard_id,
                "shard_title": shard_title,
            }
        )
    return annotated


def _build_company_roster_shard_summary(
    roster_snapshot: CompanyRosterSnapshot,
    *,
    worker_summary: dict[str, Any],
) -> dict[str, Any]:
    shard_id = str(worker_summary.get("shard_id") or "").strip()
    if not shard_id:
        return {}
    return {
        "shard_id": shard_id,
        "title": str(worker_summary.get("title") or "").strip(),
        "strategy_id": str(worker_summary.get("strategy_id") or "").strip(),
        "scope_note": str(worker_summary.get("scope_note") or "").strip(),
        "company_filters": dict(worker_summary.get("company_filters") or {}),
        "requested_max_pages": int(worker_summary.get("requested_pages") or 1),
        "requested_page_limit": int(worker_summary.get("requested_item_limit") or 25),
        "returned_entry_count": len(roster_snapshot.raw_entries),
        "returned_visible_entry_count": len(roster_snapshot.visible_entries),
        "returned_headless_entry_count": len(roster_snapshot.headless_entries),
        "unique_entry_count": len(roster_snapshot.raw_entries),
        "duplicate_entry_count": 0,
        "stop_reason": roster_snapshot.stop_reason,
        "summary_path": str(roster_snapshot.summary_path),
    }


def _upsert_company_roster_shard_summaries(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = [dict(item) for item in existing if isinstance(item, dict)]
    index_by_key = {
        str(item.get("shard_id") or "").strip(): position
        for position, item in enumerate(merged)
        if str(item.get("shard_id") or "").strip()
    }
    for item in incoming:
        if not isinstance(item, dict):
            continue
        shard_summary = dict(item)
        shard_id = str(shard_summary.get("shard_id") or "").strip()
        if not shard_id:
            continue
        if shard_id in index_by_key:
            merged[index_by_key[shard_id]] = shard_summary
            continue
        index_by_key[shard_id] = len(merged)
        merged.append(shard_summary)
    return merged


def _expected_segmented_company_roster_shard_ids(snapshot_dir: Path) -> list[str]:
    plan_path = snapshot_dir / "harvest_company_employees" / "adaptive_shard_plan.json"
    if not plan_path.exists():
        return []
    plan_payload = _read_json_dict(plan_path)
    seen: set[str] = set()
    shard_ids: list[str] = []
    for item in list(plan_payload.get("shards") or []):
        if not isinstance(item, dict):
            continue
        shard_id = str(item.get("shard_id") or "").strip()
        if not shard_id or shard_id in seen:
            continue
        seen.add(shard_id)
        shard_ids.append(shard_id)
    return shard_ids
