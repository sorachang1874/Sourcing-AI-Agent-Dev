from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Callable

from .candidate_artifacts import CandidateArtifactError, load_snapshot_candidate_artifact_payload
from .domain import Candidate, normalize_candidate
from .outreach_layering import build_outreach_layer_analysis
from .person_asset_writer import PersonAssetWriter
from .person_identity import (
    build_person_summary_view,
    resolve_candidate_identity_key,
    resolve_person_identity_key,
    resolve_profile_url_key,
)
from .serving_projection_writer import ServingProjectionWriter
from .storage import ControlPlaneStore
from .retrieval_runtime import OUTREACH_LAYER_KEY_BY_INDEX

CandidatePageLoader = Callable[[int, int], dict[str, Any] | None]


class ServingProjectionMigrationBackfill:
    """Offline migration helper for completed legacy job results."""

    def __init__(
        self,
        store: ControlPlaneStore,
        *,
        writer: ServingProjectionWriter | None = None,
        writer_id: str = "serving_projection_migration_v1",
    ) -> None:
        self.store = store
        self.writer = writer or ServingProjectionWriter(store, writer_id=writer_id)
        self.writer_id = writer_id

    def backfill_run_scope_projection(
        self,
        *,
        run_id: str,
        candidate_page_loader: CandidatePageLoader,
        collection_id: str = "",
        scope_label: str = "",
        scope_spec: dict[str, Any] | None = None,
        page_size: int = 250,
        max_candidates: int = 50_000,
        force: bool = False,
    ) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return {"status": "invalid", "reason": "run_id_required"}
        existing_link = self.store.repos.serving_projection.get_run_link(normalized_run_id)
        if existing_link and not force:
            return {
                "status": "skipped_existing_projection",
                "run_id": normalized_run_id,
                "projection_id": str(existing_link.get("projection_id") or ""),
                "link": existing_link,
            }
        members: list[dict[str, Any]] = []
        offset = 0
        normalized_page_size = min(max(int(page_size or 250), 1), 250)
        hard_limit = max(1, int(max_candidates or 50_000))
        while len(members) < hard_limit:
            page = candidate_page_loader(offset, min(normalized_page_size, hard_limit - len(members)))
            if page is None:
                return {
                    "status": "failed",
                    "reason": "candidate_page_loader_returned_none",
                    "run_id": normalized_run_id,
                    "offset": offset,
                }
            candidates = [dict(item) for item in list(page.get("candidates") or []) if isinstance(item, dict)]
            if not candidates:
                break
            for index, candidate in enumerate(candidates, start=offset):
                members.append(_projection_member_from_candidate(candidate, run_id=normalized_run_id, rank_index=index))
            if not bool(page.get("has_more")):
                break
            next_offset = page.get("next_offset")
            try:
                offset = max(offset + len(candidates), int(next_offset or 0))
            except (TypeError, ValueError):
                offset += len(candidates)
        result = self.writer.publish_run_scope_projection(
            run_id=normalized_run_id,
            collection_id=collection_id,
            members=members,
            replace_members=True,
            scope_label=scope_label or f"Migrated run {normalized_run_id}",
            scope_spec=dict(scope_spec or {}),
            counts={
                "result_count": len(members),
                "candidate_count": len(members),
                "count_scope": "exact_projection",
            },
            readiness={
                "row": "complete",
                "profile": "unknown",
                "card": "unknown",
                "migration": "completed",
            },
            provenance={
                "migration_source": "legacy_job_results",
                "source_run_id": normalized_run_id,
                "writer_id": self.writer_id,
            },
            metadata={
                "writer_id": self.writer_id,
                "migration": True,
                "force": bool(force),
            },
            state="serving",
        )
        return {
            "status": "backfilled",
            "run_id": normalized_run_id,
            "projection_id": str(dict(result.get("projection") or {}).get("projection_id") or ""),
            "member_count": int(result.get("member_count") or 0),
            "link": result.get("link") or {},
        }

    def backfill_person_summary_views(
        self,
        *,
        projection_ids: list[str] | tuple[str, ...] | None = None,
        projection_limit: int = 250,
        page_size: int = 1000,
        max_members_per_projection: int = 100_000,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Repair projection rows to the canonical PersonSummaryView contract.

        This is a migration/backfill owner path. Public readers must not repair
        identity or summary fields on demand because that recreates the old
        multi-source display contract.
        """

        projections = self._target_projections(
            projection_ids=projection_ids,
            projection_limit=projection_limit,
        )
        resolved_page_size = max(1, min(5000, int(page_size or 1000)))
        resolved_max_members = max(1, int(max_members_per_projection or 100_000))
        projection_results: list[dict[str, Any]] = []
        total_processed = 0
        total_changed = 0
        for projection in projections:
            projection_id = str(projection.get("projection_id") or "").strip()
            if not projection_id:
                continue
            source_run_id = str(projection.get("source_run_id") or "").strip()
            source_candidates = _source_candidate_index_for_projection(projection)
            processed_count = 0
            changed_count = 0
            source_payload_match_count = 0
            offset = 0
            while processed_count < resolved_max_members:
                page_limit = min(resolved_page_size, resolved_max_members - processed_count)
                members = self.store.repos.serving_projection.list_members(
                    projection_id,
                    offset=offset,
                    limit=page_limit,
                    visible_only=False,
                )
                if not members:
                    break
                normalized_members: list[dict[str, Any]] = []
                for member in members:
                    source_candidate = _source_candidate_for_member(member, source_candidates)
                    if source_candidate:
                        source_payload_match_count += 1
                    normalized_member = _projection_member_with_person_summary_view(
                        dict(member),
                        projection_id=projection_id,
                        source_run_id=source_run_id,
                        source_candidate=source_candidate,
                    )
                    if _projection_member_summary_changed(member, normalized_member):
                        changed_count += 1
                    normalized_members.append(normalized_member)
                if normalized_members and not dry_run:
                    self.store.repos.serving_projection.upsert_members(projection_id, normalized_members)
                processed_count += len(members)
                if len(members) < page_limit:
                    break
                offset += len(members)
            truncated = (
                self.store.repos.serving_projection.count_members(projection_id, visible_only=False) > processed_count
            )
            projection_results.append(
                {
                    "projection_id": projection_id,
                    "processed_count": processed_count,
                    "changed_count": changed_count,
                    "source_payload_match_count": source_payload_match_count,
                    "truncated": truncated,
                }
            )
            total_processed += processed_count
            total_changed += changed_count
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "projection_count": len(projection_results),
            "processed_member_count": total_processed,
            "changed_member_count": total_changed,
            "page_size": resolved_page_size,
            "max_members_per_projection": resolved_max_members,
            "projections": projection_results,
            "read_contract": {
                "source": "serving_projection_members",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def backfill_projection_person_search_indexes(
        self,
        *,
        projection_ids: list[str] | tuple[str, ...] | None = None,
        projection_limit: int = 250,
        member_page_size: int = 1000,
        max_members_per_projection: int = 100_000,
        stale_only: bool = True,
        rebuild_person_indexes: bool = True,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        projections = self._target_projections(
            projection_ids=projection_ids,
            projection_limit=projection_limit,
        )
        index_writer = PersonAssetWriter(self.store, writer_id=f"{self.writer_id}_person_search_index")
        projection_results: list[dict[str, Any]] = []
        rebuilt_count = 0
        skipped_count = 0
        for projection in projections:
            projection_id = str(projection.get("projection_id") or "").strip()
            if not projection_id:
                continue
            member_count = self.store.repos.serving_projection.count_members(projection_id, visible_only=True)
            index_count = self.store.count_projection_person_search_index(projection_id)
            readiness = dict(projection.get("readiness") or {})
            already_exact = (
                index_count == member_count
                and member_count > 0
                and str(readiness.get("index_count_scope") or "").strip() == "exact_projection"
            )
            if stale_only and already_exact:
                skipped_count += 1
                projection_results.append(
                    {
                        "projection_id": projection_id,
                        "status": "skipped_fresh_index",
                        "member_count": member_count,
                        "indexed_count": index_count,
                    }
                )
                continue
            if dry_run:
                projection_results.append(
                    {
                        "projection_id": projection_id,
                        "status": "would_rebuild",
                        "member_count": member_count,
                        "indexed_count": index_count,
                    }
                )
                rebuilt_count += 1
                continue
            result = index_writer.rebuild_projection_person_search_index(
                projection_id=projection_id,
                count_scope="exact_projection",
                raw_profile_index_watermark=str(projection.get("raw_profile_index_watermark") or "").strip()
                or str(projection.get("updated_at") or "").strip(),
                evidence_index_watermark=str(projection.get("evidence_index_watermark") or "").strip()
                or str(projection.get("updated_at") or "").strip(),
                member_page_size=member_page_size,
                max_members=max_members_per_projection,
                rebuild_person_indexes=bool(rebuild_person_indexes),
            )
            rebuilt_count += 1
            projection_results.append({"projection_id": projection_id, **result})
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "projection_count": len(projection_results),
            "rebuilt_count": rebuilt_count,
            "skipped_count": skipped_count,
            "stale_only": bool(stale_only),
            "rebuild_person_indexes": bool(rebuild_person_indexes),
            "projections": projection_results,
            "read_contract": {
                "source": "projection_person_search_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def backfill_collection_authoritative_local_asset_snapshots(
        self,
        *,
        runtime_dir: str | Path,
        companies: list[str] | tuple[str, ...] | None = None,
        include_existing: bool = False,
        company_limit: int = 250,
        max_members_per_company: int = 100_000,
        dry_run: bool = True,
    ) -> dict[str, Any]:
        """Publish local company asset snapshots into collection projections.

        This is an explicit maintenance owner path for local asset consumption.
        The collection overview must continue to read collection pointers only;
        it must not scan ``runtime/company_assets`` on request.
        """

        runtime_root = Path(runtime_dir).expanduser()
        candidates = _local_asset_snapshot_candidates(runtime_root, companies=companies)
        resolved_limit = max(1, int(company_limit or 250))
        resolved_max_members = max(1, int(max_members_per_company or 100_000))
        results: list[dict[str, Any]] = []
        planned_count = 0
        applied_count = 0
        skipped_existing_count = 0
        skipped_invalid_count = 0
        for snapshot in candidates[:resolved_limit]:
            collection_id = f"company:{snapshot['company_key']}"
            existing_pointer = self.store.repos.serving_projection.get_authoritative_pointer(collection_id)
            if existing_pointer and not include_existing:
                skipped_existing_count += 1
                results.append(
                    {
                        **snapshot,
                        "collection_id": collection_id,
                        "status": "skipped_existing_pointer",
                        "active_projection_id": str(existing_pointer.get("active_projection_id") or ""),
                    }
                )
                continue
            loaded_payload = _load_local_asset_snapshot_payload(snapshot)
            payload = dict(loaded_payload.get("source_payload") or {})
            members = _members_from_local_asset_payload(
                payload,
                collection_id=collection_id,
                snapshot_id=str(snapshot.get("snapshot_id") or ""),
                max_members=resolved_max_members,
            )
            if not members:
                skipped_invalid_count += 1
                results.append(
                    {
                        **snapshot,
                        "collection_id": collection_id,
                        "status": "skipped_no_projectable_members",
                    }
                )
                continue
            projection_id = _local_asset_projection_id(
                collection_id=collection_id,
                snapshot_id=str(snapshot.get("snapshot_id") or ""),
                candidate_payload_path=str(snapshot.get("candidate_payload_path") or ""),
                member_count=len(members),
            )
            planned_count += 1
            profile_ready_count = sum(1 for item in members if str(item.get("profile_readiness") or "") == "ready")
            result_payload = {
                **snapshot,
                "collection_id": collection_id,
                "status": "dry_run_ready" if dry_run else "applied",
                "projection_id": projection_id,
                "member_count": len(members),
                "profile_ready_count": profile_ready_count,
            }
            if not dry_run:
                publish_result = self.writer.publish_collection_authoritative_projection(
                    collection_id=collection_id,
                    active_collection_version=str(snapshot.get("snapshot_id") or ""),
                    projection_id=projection_id,
                    members=members,
                    replace_members=True,
                    scope_label=f"{snapshot['company_key']} local asset snapshot",
                    scope_spec={
                        "target_scope": "local_asset_snapshot",
                        "coverage_status": "local_asset_snapshot",
                        "coverage_kind": "snapshot_payload",
                        "source_snapshot_id": str(snapshot.get("snapshot_id") or ""),
                        "candidate_payload_source": "runtime_company_assets",
                    },
                    counts={
                        "result_count": len(members),
                        "candidate_count": len(members),
                        "member_count": len(members),
                        "visible_member_count": len(members),
                        "profile_fetch_required_count": len(members),
                        "profile_fetched_count": profile_ready_count,
                        "card_materialized_count": len(members),
                        "count_scope": "exact_projection",
                    },
                    readiness={
                        "member_rows": "complete",
                        "profile": "source_payload",
                        "card": "source_payload",
                        "row_count": len(members),
                        "profile_required_count": len(members),
                        "profile_ready_count": profile_ready_count,
                        "card_ready_count": len(members),
                        "count_scope": "exact_projection",
                    },
                    provenance={
                        "phase": "local_asset_collection_backfill_v1",
                        "candidate_payload_path": str(snapshot.get("candidate_payload_path") or ""),
                        "candidate_payload_source_kind": str(snapshot.get("candidate_payload_source_kind") or ""),
                        "candidate_payload_snapshot_dir": str(snapshot.get("snapshot_dir") or ""),
                        "source_snapshot_id": str(snapshot.get("snapshot_id") or ""),
                        "profile_detail_count": int(snapshot.get("profile_detail_count") or 0),
                        "structured_timeline_count": int(snapshot.get("structured_timeline_count") or 0),
                    },
                    metadata={
                        "writer_id": self.writer_id,
                        "maintenance_owner": "local_asset_collection_backfill",
                        "normal_reader_repair": False,
                    },
                    state="serving",
                )
                result_payload["projection"] = dict(publish_result.get("projection") or {})
                result_payload["pointer"] = dict(publish_result.get("pointer") or {})
                applied_count += 1
            results.append(result_payload)
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "candidate_company_count": len(candidates),
            "planned_count": planned_count,
            "applied_count": applied_count,
            "skipped_existing_count": skipped_existing_count,
            "skipped_invalid_count": skipped_invalid_count,
            "include_existing": bool(include_existing),
            "dry_run": bool(dry_run),
            "companies": results,
            "read_contract": {
                "source": "runtime_company_assets->collection_authoritative_projection",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def backfill_person_raw_evidence_indexes(
        self,
        *,
        projection_ids: list[str] | tuple[str, ...] | None = None,
        projection_limit: int = 250,
        member_page_size: int = 1000,
        max_members_per_projection: int = 100_000,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        projections = self._target_projections(
            projection_ids=projection_ids,
            projection_limit=projection_limit,
        )
        index_writer = PersonAssetWriter(self.store, writer_id=f"{self.writer_id}_raw_evidence_index")
        projection_results: list[dict[str, Any]] = []
        processed_projection_count = 0
        total_processed_members = 0
        total_raw_indexed = 0
        total_evidence_indexed = 0
        for projection in projections:
            projection_id = str(projection.get("projection_id") or "").strip()
            if not projection_id:
                continue
            member_count = self.store.repos.serving_projection.count_members(projection_id, visible_only=True)
            if dry_run:
                projection_results.append(
                    {
                        "projection_id": projection_id,
                        "status": "would_backfill",
                        "member_count": member_count,
                    }
                )
                processed_projection_count += 1
                continue
            result = index_writer.rebuild_person_indexes_for_projection(
                projection_id=projection_id,
                raw_profile_index_watermark=str(projection.get("raw_profile_index_watermark") or "").strip()
                or str(projection.get("updated_at") or "").strip(),
                evidence_index_watermark=str(projection.get("evidence_index_watermark") or "").strip()
                or str(projection.get("updated_at") or "").strip(),
                member_page_size=member_page_size,
                max_members=max_members_per_projection,
            )
            processed_projection_count += 1
            total_processed_members += int(result.get("processed_member_count") or 0)
            total_raw_indexed += int(result.get("raw_profile_indexed_count") or 0)
            total_evidence_indexed += int(result.get("candidate_evidence_indexed_count") or 0)
            projection_results.append({"projection_id": projection_id, **result})
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "projection_count": processed_projection_count,
            "processed_member_count": total_processed_members,
            "raw_profile_indexed_count": total_raw_indexed,
            "candidate_evidence_indexed_count": total_evidence_indexed,
            "projections": projection_results,
            "read_contract": {
                "source": "raw_profile_index+candidate_evidence_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def backfill_projection_layer_assignments(
        self,
        *,
        projection_ids: list[str] | tuple[str, ...] | None = None,
        projection_limit: int = 250,
        member_page_size: int = 1000,
        max_members_per_projection: int = 100_000,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Materialize deterministic outreach layer fields on projection rows.

        Collection-authoritative projections do not always have a workflow job
        shell, so this maintenance owner updates the canonical projection
        members directly instead of relying on job-scoped overlay artifacts.
        """

        projections = self._target_projections(
            projection_ids=projection_ids,
            projection_limit=projection_limit,
        )
        projection_results: list[dict[str, Any]] = []
        processed_projection_count = 0
        updated_projection_count = 0
        total_processed_members = 0
        total_updated_members = 0
        for projection in projections:
            projection_id = str(projection.get("projection_id") or "").strip()
            if not projection_id:
                continue
            processed_count = 0
            updated_count = 0
            offset = 0
            distribution = {"layer_0": 0, "layer_1": 0, "layer_2": 0, "layer_3": 0}
            while processed_count < max(1, int(max_members_per_projection or 100_000)):
                page_limit = min(
                    max(1, int(member_page_size or 1000)),
                    max(1, int(max_members_per_projection or 100_000)) - processed_count,
                )
                members = self.store.repos.serving_projection.list_members(
                    projection_id,
                    offset=offset,
                    limit=page_limit,
                    visible_only=True,
                )
                if not members:
                    break
                records = [_candidate_record_from_projection_member(member) for member in members]
                candidates = [
                    candidate
                    for candidate in (_candidate_from_projection_record(record) for record in records)
                    if candidate is not None
                ]
                analysis = (
                    build_outreach_layer_analysis(
                        candidates=candidates,
                        query="",
                        model_client=None,
                        max_ai_verifications=0,
                        registry_raw_paths={},
                    )
                    if candidates
                    else {"candidates": []}
                )
                layer_by_candidate_id: dict[str, dict[str, Any]] = {}
                for item in list(analysis.get("candidates") or []):
                    if not isinstance(item, dict):
                        continue
                    candidate_id = str(item.get("candidate_id") or "").strip()
                    if not candidate_id:
                        continue
                    layer_index = max(0, min(3, _coerce_int(item.get("final_layer"), 0)))
                    layer_by_candidate_id[candidate_id] = {
                        "outreach_layer": layer_index,
                        "outreach_layer_key": OUTREACH_LAYER_KEY_BY_INDEX.get(layer_index, "layer_0_roster"),
                        "outreach_layer_source": str(item.get("final_layer_source") or "deterministic"),
                    }
                updates: list[dict[str, Any]] = []
                for member, record in zip(members, records, strict=False):
                    candidate_id = str(record.get("candidate_id") or member.get("candidate_id") or "").strip()
                    layer_payload = dict(layer_by_candidate_id.get(candidate_id) or {})
                    if not layer_payload:
                        layer_payload = {
                            "outreach_layer": 0,
                            "outreach_layer_key": "layer_0_roster",
                            "outreach_layer_source": "deterministic",
                        }
                    layer_index = max(0, min(3, _coerce_int(layer_payload.get("outreach_layer"), 0)))
                    distribution[f"layer_{layer_index}"] = int(distribution.get(f"layer_{layer_index}") or 0) + 1
                    public_summary = dict(member.get("public_summary") or {})
                    member_metadata = dict(member.get("metadata") or {})
                    projection_metrics = dict(member.get("projection_metrics") or {})
                    public_summary.update(layer_payload)
                    public_summary["metadata"] = {
                        **dict(public_summary.get("metadata") or {}),
                        **layer_payload,
                    }
                    member_metadata.update(layer_payload)
                    projection_metrics.update(layer_payload)
                    if (
                        dict(member.get("public_summary") or {}).get("outreach_layer") == public_summary.get("outreach_layer")
                        and dict(member.get("metadata") or {}).get("outreach_layer") == member_metadata.get("outreach_layer")
                    ):
                        continue
                    updates.append(
                        {
                            **dict(member),
                            "public_summary": public_summary,
                            "metadata": member_metadata,
                            "projection_metrics": projection_metrics,
                        }
                    )
                if updates and not dry_run:
                    updated_count += self.store.repos.serving_projection.upsert_members(projection_id, updates)
                else:
                    updated_count += len(updates)
                processed_count += len(members)
                if len(members) < page_limit:
                    break
                offset += len(members)
            if processed_count and not dry_run:
                self.store.repos.serving_projection.upsert(
                    {
                        **projection,
                        "readiness": {
                            **dict(projection.get("readiness") or {}),
                            "layering": "complete",
                        },
                        "metadata": {
                            **dict(projection.get("metadata") or {}),
                            "layer_assignment_source": "collection_projection_layer_backfill",
                            "layer_assignment_updated_member_count": updated_count,
                            "layer_assignment_distribution": distribution,
                        },
                    }
                )
            processed_projection_count += 1
            if updated_count:
                updated_projection_count += 1
            total_processed_members += processed_count
            total_updated_members += updated_count
            projection_results.append(
                {
                    "projection_id": projection_id,
                    "processed_member_count": processed_count,
                    "updated_member_count": updated_count,
                    "layer_counts": distribution,
                }
            )
        return {
            "status": "dry_run" if dry_run else "backfilled",
            "projection_count": processed_projection_count,
            "updated_projection_count": updated_projection_count,
            "processed_member_count": total_processed_members,
            "updated_member_count": total_updated_members,
            "projections": projection_results,
            "read_contract": {
                "source": "serving_projection_members",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def _target_projections(
        self,
        *,
        projection_ids: list[str] | tuple[str, ...] | None,
        projection_limit: int,
    ) -> list[dict[str, Any]]:
        explicit_ids = [
            str(item or "").strip()
            for item in list(projection_ids or [])
            if str(item or "").strip()
        ]
        if explicit_ids:
            return [
                projection
                for projection_id in explicit_ids
                for projection in [self.store.repos.serving_projection.get(projection_id)]
                if projection
            ]
        return self.store.repos.serving_projection.list(limit=max(1, int(projection_limit or 250)))


def _projection_member_from_candidate(candidate: dict[str, Any], *, run_id: str, rank_index: int) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or candidate.get("id") or "").strip()
    linkedin_url = str(candidate.get("linkedin_url") or candidate.get("profile_url") or "").strip()
    profile_url_key = resolve_profile_url_key(candidate.get("profile_url_key"), linkedin_url)
    person_identity_key = resolve_person_identity_key(
        person_identity_key=str(candidate.get("person_identity_key") or ""),
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        candidate_identity_key=str(candidate.get("candidate_identity_key") or ""),
        candidate_id=candidate_id,
    )
    candidate_identity_key = resolve_candidate_identity_key(
        candidate_identity_key=str(candidate.get("candidate_identity_key") or ""),
        person_identity_key=person_identity_key,
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        candidate_id=candidate_id,
    )
    public_summary = build_person_summary_view(
        candidate,
        candidate_id=candidate_id,
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        person_identity_key=person_identity_key,
        source_run_id=run_id,
    )
    return {
        "candidate_identity_key": candidate_identity_key,
        "person_identity_key": person_identity_key,
        "profile_url_key": profile_url_key,
        "candidate_id": candidate_id,
        "rank_index": rank_index,
        "employment_scope": str(candidate.get("employment_status") or candidate.get("employment_scope") or "").strip(),
        "source_run_id": run_id,
        "row_readiness": "ready",
        "profile_readiness": "unknown",
        "card_readiness": "unknown",
        "public_summary": public_summary,
        "projection_metrics": {
            "migration_source": "legacy_job_results",
            "has_profile_detail": bool(candidate.get("has_profile_detail")),
        },
    }


def _projection_member_with_person_summary_view(
    member: dict[str, Any],
    *,
    projection_id: str,
    source_run_id: str,
    source_candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    public_summary = dict(member.get("public_summary") or {})
    source_public_summary = dict(dict(source_candidate or {}).get("public_summary") or {})
    source_metadata = dict(dict(source_candidate or {}).get("metadata") or {})
    source_payload = {
        **dict(source_candidate or {}),
        **source_metadata,
        **source_public_summary,
        **public_summary,
    }
    candidate_id = str(member.get("candidate_id") or public_summary.get("candidate_id") or public_summary.get("id") or "").strip()
    linkedin_url = str(public_summary.get("linkedin_url") or public_summary.get("profile_url") or "").strip()
    profile_url_key = resolve_profile_url_key(member.get("profile_url_key"), public_summary.get("profile_url_key"), linkedin_url)
    person_identity_key = resolve_person_identity_key(
        person_identity_key=str(member.get("person_identity_key") or public_summary.get("person_identity_key") or ""),
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        candidate_identity_key=str(member.get("candidate_identity_key") or ""),
        candidate_id=candidate_id,
    )
    candidate_identity_key = resolve_candidate_identity_key(
        candidate_identity_key=str(member.get("candidate_identity_key") or ""),
        person_identity_key=person_identity_key,
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        candidate_id=candidate_id,
    )
    summary = build_person_summary_view(
        source_payload,
        candidate_id=candidate_id,
        profile_url_key=profile_url_key,
        linkedin_url=linkedin_url,
        person_identity_key=person_identity_key,
        source_projection_id=projection_id,
        source_run_id=source_run_id or str(member.get("source_run_id") or ""),
    )
    profile_readiness = str(member.get("profile_readiness") or "unknown").strip() or "unknown"
    if profile_readiness in {"", "unknown", "pending"} and _summary_has_profile_detail(summary):
        profile_readiness = "ready"
    return {
        **member,
        "projection_id": projection_id,
        "candidate_identity_key": candidate_identity_key,
        "person_identity_key": person_identity_key,
        "profile_url_key": profile_url_key,
        "candidate_id": candidate_id,
        "source_run_id": source_run_id or str(member.get("source_run_id") or ""),
        "profile_readiness": profile_readiness,
        "public_summary": {**public_summary, **summary},
    }


def _projection_member_summary_changed(before: dict[str, Any], after: dict[str, Any]) -> bool:
    for key in ("candidate_identity_key", "person_identity_key", "profile_url_key", "candidate_id", "source_run_id"):
        if str(dict(before).get(key) or "").strip() != str(dict(after).get(key) or "").strip():
            return True
    if str(dict(before).get("profile_readiness") or "").strip() != str(dict(after).get("profile_readiness") or "").strip():
        return True
    before_summary = dict(dict(before).get("public_summary") or {})
    after_summary = dict(dict(after).get("public_summary") or {})
    for key in (
        "candidate_id",
        "display_name",
        "name",
        "headline",
        "current_company",
        "location",
        "linkedin_url",
        "profile_url_key",
        "person_identity_key",
        "source_projection_id",
        "source_run_id",
        "summary",
        "experience_lines",
        "education_lines",
        "experience",
        "education",
        "profile_capture_kind",
    ):
        if before_summary.get(key) != after_summary.get(key):
            return True
    return False


def _candidate_record_from_projection_member(member: dict[str, Any]) -> dict[str, Any]:
    payload = dict(member or {})
    public_summary = dict(payload.get("public_summary") or {})
    projection_metrics = dict(payload.get("projection_metrics") or {})
    member_metadata = dict(payload.get("metadata") or {})
    record = dict(public_summary)
    candidate_id = str(
        record.get("candidate_id")
        or payload.get("candidate_id")
        or payload.get("candidate_identity_key")
        or ""
    ).strip()
    if candidate_id:
        record["candidate_id"] = candidate_id
    if not str(record.get("name_en") or "").strip():
        record["name_en"] = str(
            record.get("display_name")
            or record.get("full_name")
            or record.get("name")
            or candidate_id
            or ""
        ).strip()
    if not str(record.get("display_name") or "").strip() and str(record.get("name_en") or "").strip():
        record["display_name"] = str(record.get("name_en") or "").strip()
    education_lines = _normalized_text_lines(
        record.get("education_lines")
        or public_summary.get("education_lines")
        or member_metadata.get("education_lines")
        or projection_metrics.get("education_lines")
    )
    experience_lines = _normalized_text_lines(
        record.get("experience_lines")
        or public_summary.get("experience_lines")
        or member_metadata.get("experience_lines")
        or projection_metrics.get("experience_lines")
    )
    if education_lines and not str(record.get("education") or "").strip():
        record["education"] = " / ".join(education_lines)
    if experience_lines and not str(record.get("work_history") or "").strip():
        record["work_history"] = " / ".join(experience_lines)
    metadata = {
        **dict(record.get("metadata") or {}),
        **member_metadata,
        **projection_metrics,
        "candidate_identity_key": str(payload.get("candidate_identity_key") or "").strip(),
        "person_identity_key": str(payload.get("person_identity_key") or "").strip(),
        "profile_url_key": str(payload.get("profile_url_key") or record.get("profile_url_key") or "").strip(),
    }
    record["metadata"] = {key: value for key, value in metadata.items() if value not in ("", None, [], {})}
    return record


def _normalized_text_lines(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, (list, tuple, set)):
        values = [str(item or "") for item in value]
    else:
        values = [str(value or "")]
    lines: list[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item or "").strip()
        if not text:
            continue
        normalized = " ".join(text.split()).lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        lines.append(text)
    return lines


def _candidate_from_projection_record(record: dict[str, Any]) -> Candidate | None:
    if not isinstance(record, dict):
        return None
    candidate_fields = set(Candidate.__dataclass_fields__)
    candidate_payload = {field: record.get(field) for field in candidate_fields if field in record}
    if not str(candidate_payload.get("candidate_id") or "").strip():
        return None
    if not str(candidate_payload.get("name_en") or "").strip():
        candidate_payload["name_en"] = str(
            record.get("display_name")
            or record.get("full_name")
            or record.get("name")
            or candidate_payload.get("candidate_id")
            or ""
        ).strip()
    try:
        return normalize_candidate(Candidate(**candidate_payload))
    except TypeError:
        return None


def _summary_has_profile_detail(summary: dict[str, Any]) -> bool:
    payload = dict(summary or {})
    if bool(payload.get("has_profile_detail")):
        return True
    if str(payload.get("profile_capture_kind") or "").strip():
        return True
    return bool(payload.get("experience_lines") or payload.get("education_lines"))


def _source_candidate_index_for_projection(projection: dict[str, Any]) -> dict[str, dict[str, Any]]:
    provenance = dict(dict(projection or {}).get("provenance") or {})
    payload = _load_projection_source_candidate_payload(provenance)
    if not payload:
        return {}
    candidates = list(payload.get("candidates") or []) if isinstance(payload, dict) else list(payload or [])
    indexed: dict[str, dict[str, Any]] = {}
    for raw_candidate in candidates:
        if not isinstance(raw_candidate, dict):
            continue
        candidate = dict(raw_candidate)
        public_summary = dict(candidate.get("public_summary") or {})
        profile_key = resolve_profile_url_key(
            candidate.get("profile_url_key"),
            public_summary.get("profile_url_key"),
            candidate.get("linkedin_url"),
            public_summary.get("linkedin_url"),
            candidate.get("profile_url"),
            public_summary.get("profile_url"),
        )
        candidate_id = str(candidate.get("candidate_id") or candidate.get("id") or public_summary.get("candidate_id") or "").strip()
        person_key = resolve_person_identity_key(
            person_identity_key=str(candidate.get("person_identity_key") or public_summary.get("person_identity_key") or ""),
            profile_url_key=profile_key,
            linkedin_url=str(candidate.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            candidate_identity_key=str(candidate.get("candidate_identity_key") or public_summary.get("candidate_identity_key") or ""),
            candidate_id=candidate_id,
        )
        candidate_key = resolve_candidate_identity_key(
            candidate_identity_key=str(candidate.get("candidate_identity_key") or public_summary.get("candidate_identity_key") or ""),
            person_identity_key=person_key,
            profile_url_key=profile_key,
            linkedin_url=str(candidate.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            candidate_id=candidate_id,
        )
        for key in (candidate_key, person_key, profile_key, candidate_id):
            normalized_key = str(key or "").strip()
            if normalized_key and normalized_key not in indexed:
                indexed[normalized_key] = candidate
    return indexed


def _load_projection_source_candidate_payload(provenance: dict[str, Any]) -> dict[str, Any]:
    snapshot_dir = Path(str(provenance.get("candidate_payload_snapshot_dir") or "").strip()).expanduser()
    source_kind = str(provenance.get("candidate_payload_source_kind") or "").strip()
    if source_kind.startswith("materialized_candidate_documents") and str(snapshot_dir).strip():
        try:
            loaded = load_snapshot_candidate_artifact_payload(
                snapshot_dir=snapshot_dir,
                target_company=str(provenance.get("target_company") or snapshot_dir.parent.name or ""),
                company_key=str(provenance.get("company_key") or snapshot_dir.parent.name or ""),
                allow_candidate_documents_fallback=False,
            )
        except CandidateArtifactError:
            loaded = {}
        payload = dict(dict(loaded).get("source_payload") or {})
        if payload:
            return _normalize_candidate_payload(payload)

    payload_path = Path(str(provenance.get("candidate_payload_path") or "").strip()).expanduser()
    if not str(payload_path).strip() or not payload_path.exists() or not payload_path.is_file():
        return {}
    if payload_path.name == "manifest.json" and str(snapshot_dir).strip():
        try:
            loaded = load_snapshot_candidate_artifact_payload(
                snapshot_dir=snapshot_dir,
                target_company=str(provenance.get("target_company") or snapshot_dir.parent.name or ""),
                company_key=str(provenance.get("company_key") or snapshot_dir.parent.name or ""),
                allow_candidate_documents_fallback=False,
            )
        except CandidateArtifactError:
            loaded = {}
        payload = dict(dict(loaded).get("source_payload") or {})
        if payload:
            return _normalize_candidate_payload(payload)
    return _load_candidate_payload(payload_path)


def _source_candidate_for_member(
    member: dict[str, Any],
    source_candidates: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not source_candidates:
        return {}
    public_summary = dict(dict(member or {}).get("public_summary") or {})
    for key in (
        member.get("candidate_identity_key"),
        member.get("person_identity_key"),
        member.get("profile_url_key"),
        member.get("candidate_id"),
        public_summary.get("candidate_identity_key"),
        public_summary.get("person_identity_key"),
        public_summary.get("profile_url_key"),
        public_summary.get("candidate_id"),
    ):
        normalized_key = str(key or "").strip()
        if normalized_key and normalized_key in source_candidates:
            return dict(source_candidates[normalized_key])
    return {}


def _local_asset_snapshot_candidates(
    runtime_dir: Path,
    *,
    companies: list[str] | tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    company_assets_dir = runtime_dir / "company_assets"
    if not company_assets_dir.exists() or not company_assets_dir.is_dir():
        return []
    requested_company_keys = {
        str(company or "").strip().lower().replace(" ", "").replace("_", "").replace("-", "")
        for company in list(companies or [])
        if str(company or "").strip()
    }
    candidates: list[dict[str, Any]] = []
    for company_dir in sorted(path for path in company_assets_dir.iterdir() if path.is_dir()):
        normalized_company_key = company_dir.name.lower().replace(" ", "").replace("_", "").replace("-", "")
        if requested_company_keys and normalized_company_key not in requested_company_keys:
            continue
        best_valid: dict[str, Any] = {}
        best_score: tuple[int, int, int, int, str] | None = None
        for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
            snapshot = _inspect_local_asset_snapshot(snapshot_dir=snapshot_dir, company_key=company_dir.name)
            source_path = str(snapshot.get("candidate_payload_path") or "")
            source_kind = str(snapshot.get("candidate_payload_source_kind") or "")
            candidate_count = int(snapshot.get("candidate_count") or 0)
            if candidate_count <= 0 or not source_path:
                continue
            profile_detail_count = int(snapshot.get("profile_detail_count") or 0)
            structured_timeline_count = int(snapshot.get("structured_timeline_count") or 0)
            source_priority = 1 if source_kind.startswith("materialized_candidate_documents") else 0
            score = (
                source_priority,
                profile_detail_count,
                structured_timeline_count,
                candidate_count,
                snapshot_dir.name,
            )
            if best_score is not None and score <= best_score:
                continue
            best_score = score
            best_valid = {
                **snapshot,
                "selection_policy": "best_profile_complete_local_asset",
            }
        if best_valid:
            candidates.append(best_valid)
    return candidates


def _candidate_payload_path_for_snapshot(snapshot_dir: Path) -> Path | None:
    for path in (
        snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json",
        snapshot_dir / "candidate_documents.json",
    ):
        if path.exists() and path.is_file():
            return path
    return None


def _inspect_local_asset_snapshot(*, snapshot_dir: Path, company_key: str) -> dict[str, Any]:
    normalized_dir = snapshot_dir / "normalized_artifacts"
    summary = _load_json_file(normalized_dir / "artifact_summary.json")
    manifest_path = normalized_dir / "manifest.json"
    materialized_path = normalized_dir / "materialized_candidate_documents.json"
    candidate_doc_path = snapshot_dir / "candidate_documents.json"
    source_path: Path | None = None
    source_kind = ""
    if manifest_path.exists() and manifest_path.is_file() and _manifest_candidate_shards_complete(manifest_path):
        source_path = manifest_path
        source_kind = "materialized_candidate_documents_manifest"
    elif materialized_path.exists() and materialized_path.is_file():
        source_path = materialized_path
        source_kind = "materialized_candidate_documents"
    elif candidate_doc_path.exists() and candidate_doc_path.is_file():
        source_path = candidate_doc_path
        source_kind = "candidate_documents"
    if source_path is None:
        return {}
    candidate_count = _coerce_int(summary.get("candidate_count"), 0)
    profile_detail_count = _coerce_int(summary.get("profile_detail_count"), 0)
    structured_timeline_count = _coerce_int(summary.get("structured_timeline_count"), 0)
    if source_kind in {"materialized_candidate_documents", "candidate_documents"} or not summary:
        payload = _load_candidate_payload(source_path)
        candidate_count = len(list(payload.get("candidates") or [])) if isinstance(payload, dict) else 0
        profile_detail_count = _candidate_profile_detail_count(payload)
        structured_timeline_count = _candidate_structured_timeline_count(payload)
    return {
        "company_key": company_key,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "candidate_payload_path": str(source_path),
        "candidate_payload_source_kind": source_kind,
        "candidate_count": candidate_count,
        "profile_detail_count": profile_detail_count,
        "structured_timeline_count": structured_timeline_count,
    }


def _manifest_candidate_shards_complete(manifest_path: Path) -> bool:
    manifest = _load_json_file(manifest_path)
    if not manifest:
        return False
    artifact_dir = manifest_path.parent
    entries = [entry for entry in list(manifest.get("candidate_shards") or []) if isinstance(entry, dict)]
    if not entries:
        return False
    for entry in entries:
        shard_path = artifact_dir / str(entry.get("path") or "").strip()
        if not shard_path.exists() or not shard_path.is_file():
            return False
    return True


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _load_local_asset_snapshot_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    snapshot_dir = Path(str(snapshot.get("snapshot_dir") or "")).expanduser()
    company_key = str(snapshot.get("company_key") or snapshot_dir.parent.name or "").strip()
    if str(snapshot_dir).strip() and snapshot_dir.exists() and snapshot_dir.is_dir():
        try:
            loaded = load_snapshot_candidate_artifact_payload(
                snapshot_dir=snapshot_dir,
                target_company=company_key,
                company_key=company_key,
                allow_candidate_documents_fallback=True,
            )
        except CandidateArtifactError:
            loaded = {}
        payload = dict(dict(loaded).get("source_payload") or {})
        if payload:
            return {
                **dict(loaded),
                "source_payload": _normalize_candidate_payload(payload),
            }

    payload_path = Path(str(snapshot.get("candidate_payload_path") or "")).expanduser()
    if payload_path.exists() and payload_path.is_file():
        return {
            "status": "loaded",
            "source_kind": "candidate_payload_file",
            "source_path": str(payload_path),
            "source_payload": _load_candidate_payload(payload_path),
            "artifact_summary": {},
        }
    return {"status": "missing", "source_payload": {}}


def _load_candidate_payload(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return _normalize_candidate_payload(payload)


def _normalize_candidate_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, list):
        return {"candidates": [item for item in payload if isinstance(item, dict)]}
    if isinstance(payload, dict):
        return {"candidates": [item for item in list(payload.get("candidates") or []) if isinstance(item, dict)]}
    return {"candidates": []}


def _coerce_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(fallback or 0)


def _candidate_profile_detail_count(payload: dict[str, Any]) -> int:
    return sum(
        1
        for candidate in list(dict(payload or {}).get("candidates") or [])
        if isinstance(candidate, dict)
        and _summary_has_profile_detail(
            {
                **candidate,
                **dict(candidate.get("metadata") or {}),
                **dict(candidate.get("public_summary") or {}),
            }
        )
    )


def _candidate_structured_timeline_count(payload: dict[str, Any]) -> int:
    count = 0
    for candidate in list(dict(payload or {}).get("candidates") or []):
        if not isinstance(candidate, dict):
            continue
        metadata = dict(candidate.get("metadata") or {})
        public_summary = dict(candidate.get("public_summary") or {})
        if (
            candidate.get("experience_lines")
            or candidate.get("education_lines")
            or metadata.get("experience_lines")
            or metadata.get("education_lines")
            or public_summary.get("experience_lines")
            or public_summary.get("education_lines")
        ):
            count += 1
    return count


def _members_from_local_asset_payload(
    payload: dict[str, Any],
    *,
    collection_id: str,
    snapshot_id: str,
    max_members: int,
) -> list[dict[str, Any]]:
    members: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw_candidate in enumerate(list(payload.get("candidates") or [])[: max(1, int(max_members or 100_000))], start=1):
        candidate = dict(raw_candidate or {})
        public_summary = dict(candidate.get("public_summary") or {})
        metadata = dict(candidate.get("metadata") or {})
        summary_source = {**candidate, **metadata, **public_summary}
        profile_key = resolve_profile_url_key(
            candidate.get("profile_url_key"),
            public_summary.get("profile_url_key"),
            candidate.get("linkedin_url"),
            public_summary.get("linkedin_url"),
            candidate.get("profile_url"),
            public_summary.get("profile_url"),
            metadata.get("profile_url"),
        )
        candidate_id = str(candidate.get("candidate_id") or candidate.get("id") or public_summary.get("candidate_id") or "").strip()
        person_key = resolve_person_identity_key(
            person_identity_key=str(candidate.get("person_identity_key") or public_summary.get("person_identity_key") or ""),
            profile_url_key=profile_key,
            linkedin_url=str(candidate.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            candidate_identity_key=str(candidate.get("candidate_identity_key") or public_summary.get("candidate_identity_key") or ""),
            candidate_id=candidate_id,
        )
        candidate_key = resolve_candidate_identity_key(
            candidate_identity_key=str(candidate.get("candidate_identity_key") or public_summary.get("candidate_identity_key") or ""),
            person_identity_key=person_key,
            profile_url_key=profile_key,
            linkedin_url=str(candidate.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            candidate_id=candidate_id,
        )
        if not candidate_key or candidate_key in seen:
            continue
        seen.add(candidate_key)
        summary = build_person_summary_view(
            summary_source,
            candidate_id=candidate_id,
            profile_url_key=profile_key,
            linkedin_url=str(candidate.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            person_identity_key=person_key,
            source_projection_id="",
            source_run_id="",
        )
        summary.update(
            {
                "source_collection_id": collection_id,
                "source_snapshot_id": snapshot_id,
            }
        )
        members.append(
            {
                "candidate_identity_key": candidate_key,
                "person_identity_key": person_key or candidate_key,
                "profile_url_key": profile_key,
                "candidate_id": candidate_id,
                "rank_index": index,
                "rank_key": f"{index:08d}:{candidate_key}",
                "employment_scope": str(
                    candidate.get("employment_scope")
                    or public_summary.get("employment_scope")
                    or public_summary.get("employment_status")
                    or summary.get("employment_status")
                    or ""
                ).strip(),
                "row_readiness": "ready",
                "profile_readiness": "ready" if _summary_has_profile_detail(summary) else "unknown",
                "card_readiness": "ready",
                "visibility_state": "visible",
                "public_summary": summary,
                "projection_metrics": {
                    "source_snapshot_id": snapshot_id,
                    "repair_phase": "local_asset_collection_backfill_v1",
                },
                "metadata": {
                    "maintenance_owner": "local_asset_collection_backfill",
                    "raw_candidate_payload_not_copied": True,
                },
            }
        )
    return members


def _local_asset_projection_id(
    *,
    collection_id: str,
    snapshot_id: str,
    candidate_payload_path: str,
    member_count: int,
) -> str:
    digest = hashlib.sha256(
        "|".join([collection_id, snapshot_id, candidate_payload_path, str(member_count)]).encode("utf-8")
    ).hexdigest()[:24]
    return f"proj_localasset_{digest}"
