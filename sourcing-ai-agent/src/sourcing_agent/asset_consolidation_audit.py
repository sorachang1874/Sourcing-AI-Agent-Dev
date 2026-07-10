from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .asset_paths import (
    company_assets_roots,
    iter_company_asset_snapshot_dirs,
    load_company_snapshot_json,
    load_latest_snapshot_pointer,
    resolve_company_snapshot_dir_by_key,
)
from .company_registry import normalize_company_key
from .person_identity import resolve_candidate_identity_key, resolve_profile_url_key
from .storage import ControlPlaneStore

REUSABLE_SHARD_STATUSES = {"completed", "completed_with_cap", "skipped_high_overlap"}
ACTIVE_PROJECTION_STATES = {"serving", "active", "ready", "published"}
ARCHIVE_CANDIDATE_CLASSIFICATION = "archive_candidate_no_increment_duplicate"
DEFAULT_OVERLAP_CANDIDATE_LIMIT = 25_000
DEFAULT_OVERLAP_SNAPSHOT_LIMIT = 25


def audit_asset_consolidation(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company: str = "",
    asset_view: str = "canonical_merged",
    limit: int = 250,
    include_local_only: bool = True,
    include_overlap: bool = False,
    overlap_candidate_limit: int = DEFAULT_OVERLAP_CANDIDATE_LIMIT,
    overlap_snapshot_limit: int = DEFAULT_OVERLAP_SNAPSHOT_LIMIT,
) -> dict[str, Any]:
    """Build a read-only deletion/archival preflight for historical company assets.

    The audit intentionally uses registry/projection/CRM/person-asset metadata as
    its online source of truth. It does not read large candidate documents or
    compute member-level overlap; that belongs in a later production-scale
    overlap job after this dependency graph proves which snapshots are safe to
    inspect or archive.
    """

    normalized_company_filter = _normalize_company_filter(company)
    normalized_asset_view = _normalize_text(asset_view) or "canonical_merged"
    normalized_limit = max(1, int(limit or 250))

    registry_rows = store.list_organization_asset_registry(
        target_company=company if normalized_company_filter else "",
        asset_view=normalized_asset_view,
        limit=max(normalized_limit, 1000),
    )
    if normalized_company_filter:
        registry_rows = [
            row
            for row in registry_rows
            if _company_matches(row, normalized_company_filter)
        ]
    local_snapshots = _discover_local_snapshots(
        runtime_dir=runtime_dir,
        company_filter=normalized_company_filter,
        include_local_only=include_local_only,
    )
    snapshot_index = _build_snapshot_index(
        registry_rows=registry_rows,
        local_snapshots=local_snapshots,
        asset_view=normalized_asset_view,
    )
    companies = _company_order(snapshot_index, limit=normalized_limit)

    company_reports: list[dict[str, Any]] = []
    for company_key in companies:
        report = _audit_company(
            runtime_dir=runtime_dir,
            store=store,
            company_key=company_key,
            snapshot_records=snapshot_index[company_key],
            asset_view=normalized_asset_view,
            limit=normalized_limit,
            include_overlap=include_overlap,
            overlap_candidate_limit=overlap_candidate_limit,
            overlap_snapshot_limit=overlap_snapshot_limit,
        )
        company_reports.append(report)

    total_snapshots = sum(len(report["snapshots"]) for report in company_reports)
    archive_candidates = sum(
        1
        for report in company_reports
        for snapshot in report["snapshots"]
        if snapshot["classification"] == ARCHIVE_CANDIDATE_CLASSIFICATION
    )
    blocked_snapshots = sum(
        1
        for report in company_reports
        for snapshot in report["snapshots"]
        if snapshot["deletion_blockers"]
    )
    review_required = sum(
        1
        for report in company_reports
        for snapshot in report["snapshots"]
        if snapshot["classification"].startswith("review_")
    )
    overlap_summaries = [dict(report.get("overlap_subsumption") or {}) for report in company_reports]
    return {
        "status": "ok",
        "read_only": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "contract_version": "asset_consolidation_audit_v2" if include_overlap else "asset_consolidation_audit_v1",
        "company_filter": normalized_company_filter,
        "asset_view": normalized_asset_view,
        "summary": {
            "company_count": len(company_reports),
            "snapshot_count": total_snapshots,
            "archive_candidate_count": archive_candidates,
            "blocked_snapshot_count": blocked_snapshots,
            "review_required_count": review_required,
            "deletion_ready_snapshot_count": sum(
                1
                for report in company_reports
                for snapshot in report["snapshots"]
                if snapshot["archive_ready"] and not snapshot["deletion_blockers"]
            ),
            "overlap_enabled": bool(include_overlap),
            "overlap_archive_candidate_count": sum(
                int(summary.get("archive_candidate_count") or 0) for summary in overlap_summaries
            ),
            "overlap_subsumed_archive_candidate_count": sum(
                int(summary.get("subsumed_count") or 0) for summary in overlap_summaries
            ),
            "overlap_review_archive_candidate_count": sum(
                int(summary.get("review_required_count") or 0) for summary in overlap_summaries
            ),
        },
        "companies": company_reports,
    }


def _audit_company(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company_key: str,
    snapshot_records: dict[str, dict[str, Any]],
    asset_view: str,
    limit: int,
    include_overlap: bool,
    overlap_candidate_limit: int,
    overlap_snapshot_limit: int,
) -> dict[str, Any]:
    target_company = _best_target_company(snapshot_records, company_key)
    collection_id = f"company:{company_key}"
    registry_rows = [
        record["registry_row"]
        for record in snapshot_records.values()
        if record.get("registry_row")
    ]
    authoritative_snapshot_ids = _authoritative_snapshot_ids(registry_rows)
    selected_snapshot_ids = _selected_snapshot_ids(registry_rows)
    latest_local_snapshot_ids = {
        str(record.get("latest_snapshot_id") or "").strip()
        for record in snapshot_records.values()
        if str(record.get("latest_snapshot_id") or "").strip()
    }
    shard_rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=list(snapshot_records.keys()),
        statuses=[],
        limit=max(1000, limit * 20),
    )
    shard_dependencies = _summarize_shards_by_snapshot(shard_rows)
    projection_dependencies = _projection_dependencies_by_snapshot(
        store=store,
        collection_id=collection_id,
        snapshot_ids=set(snapshot_records.keys()),
        limit=limit,
    )
    projection_ids = sorted(
        {
            dependency["projection_id"]
            for dependencies in projection_dependencies.values()
            for dependency in dependencies
            if dependency.get("projection_id")
        }
    )
    crm_dependencies, person_asset_dependencies = _crm_person_dependencies_by_projection(
        store=store,
        projection_ids=projection_ids,
        limit=limit,
    )
    pointer = store.repos.serving_projection.get_authoritative_pointer(collection_id)
    active_pointer_projection_id = _normalize_text(pointer.get("active_projection_id"))

    snapshot_reports = []
    for snapshot_id, record in sorted(
        snapshot_records.items(),
        key=lambda item: _snapshot_sort_key(item[1]),
        reverse=True,
    ):
        registry_row = dict(record.get("registry_row") or {})
        projection_refs = projection_dependencies.get(snapshot_id, [])
        crm_refs = [
            ref
            for projection in projection_refs
            for ref in crm_dependencies.get(_normalize_text(projection.get("projection_id")), [])
        ]
        person_asset_refs = [
            ref
            for projection in projection_refs
            for ref in person_asset_dependencies.get(_normalize_text(projection.get("projection_id")), [])
        ]
        shard_summary = shard_dependencies.get(snapshot_id, _empty_shard_summary())
        blockers = _deletion_blockers(
            snapshot_id=snapshot_id,
            registry_row=registry_row,
            authoritative_snapshot_ids=authoritative_snapshot_ids,
            selected_snapshot_ids=selected_snapshot_ids,
            latest_local_snapshot_ids=latest_local_snapshot_ids,
            shard_summary=shard_summary,
            projection_refs=projection_refs,
            crm_refs=crm_refs,
            person_asset_refs=person_asset_refs,
            active_pointer_projection_id=active_pointer_projection_id,
        )
        classification = _classify_snapshot(
            registry_row=registry_row,
            local_snapshot=record.get("local_snapshot") or {},
            shard_summary=shard_summary,
            projection_refs=projection_refs,
            blockers=blockers,
        )
        snapshot_reports.append(
            {
                "snapshot_id": snapshot_id,
                "classification": classification,
                "archive_ready": classification == ARCHIVE_CANDIDATE_CLASSIFICATION and not blockers,
                "deletion_blockers": blockers,
                "registry": _summarize_registry_row(registry_row),
                "local_snapshot": _summarize_local_snapshot(record.get("local_snapshot") or {}),
                "shards": shard_summary,
                "projection_dependencies": projection_refs,
                "crm_dependency_count": len(crm_refs),
                "person_asset_dependency_count": len(person_asset_refs),
                "crm_dependency_samples": crm_refs[:5],
                "person_asset_dependency_samples": person_asset_refs[:5],
            }
        )

    overlap_subsumption = _empty_overlap_summary(enabled=False)
    if include_overlap:
        overlap_subsumption = _attach_overlap_evidence(
            runtime_dir=runtime_dir,
            store=store,
            company_key=company_key,
            target_company=target_company,
            asset_view=asset_view,
            collection_pointer=pointer,
            authoritative_snapshot_ids=authoritative_snapshot_ids,
            selected_snapshot_ids=selected_snapshot_ids,
            snapshots=snapshot_reports,
            max_candidates_per_snapshot=max(1, int(overlap_candidate_limit or DEFAULT_OVERLAP_CANDIDATE_LIMIT)),
            max_archive_snapshots=max(1, int(overlap_snapshot_limit or DEFAULT_OVERLAP_SNAPSHOT_LIMIT)),
        )

    return {
        "company_key": company_key,
        "target_company": target_company,
        "collection_id": collection_id,
        "asset_view": asset_view,
        "authoritative_snapshot_ids": sorted(authoritative_snapshot_ids),
        "selected_snapshot_ids": sorted(selected_snapshot_ids),
        "latest_local_snapshot_ids": sorted(latest_local_snapshot_ids),
        "collection_authoritative_pointer": {
            "active_projection_id": active_pointer_projection_id,
            "active_collection_version": _normalize_text(pointer.get("active_collection_version")),
            "state": _normalize_text(pointer.get("state")),
        },
        "snapshots": snapshot_reports,
        "overlap_subsumption": overlap_subsumption,
        "summary": {
            "snapshot_count": len(snapshot_reports),
            "archive_candidate_count": sum(
                1 for snapshot in snapshot_reports if snapshot["classification"] == ARCHIVE_CANDIDATE_CLASSIFICATION
            ),
            "blocked_snapshot_count": sum(1 for snapshot in snapshot_reports if snapshot["deletion_blockers"]),
            "reusable_shard_snapshot_count": sum(
                1
                for snapshot in snapshot_reports
                if snapshot["shards"]["reusable_shard_count"] > 0
            ),
            "active_projection_dependency_snapshot_count": sum(
                1
                for snapshot in snapshot_reports
                if any(ref.get("active_pointer") or ref.get("active_state") for ref in snapshot["projection_dependencies"])
            ),
            "overlap_enabled": bool(include_overlap),
            "overlap_subsumed_archive_candidate_count": int(overlap_subsumption.get("subsumed_count") or 0),
            "overlap_review_archive_candidate_count": int(overlap_subsumption.get("review_required_count") or 0),
        },
    }


def _discover_local_snapshots(
    *,
    runtime_dir: str | Path,
    company_filter: str,
    include_local_only: bool,
) -> list[dict[str, Any]]:
    if not include_local_only:
        return []
    snapshots: list[dict[str, Any]] = []
    latest_by_company_dir: dict[str, str] = {}
    snapshot_dirs = (
        _iter_filtered_company_snapshot_dirs(runtime_dir=runtime_dir, company_filter=company_filter)
        if company_filter
        else iter_company_asset_snapshot_dirs(runtime_dir, prefer_hot_cache=True, existing_only=True)
    )
    for snapshot_dir in snapshot_dirs:
        resolved_company_key = _local_asset_company_key(snapshot_dir.parent.name)
        if company_filter and company_filter != resolved_company_key:
            continue
        if resolved_company_key not in latest_by_company_dir:
            latest_by_company_dir[resolved_company_key] = _normalize_text(
                load_latest_snapshot_pointer(snapshot_dir.parent).get("snapshot_id")
            )
        snapshots.append(
            {
                "company_key": resolved_company_key,
                "snapshot_id": _normalize_text(snapshot_dir.name),
                "path": str(snapshot_dir),
                "latest_snapshot_id": latest_by_company_dir.get(resolved_company_key, ""),
                "exists": snapshot_dir.exists(),
            }
        )
    return snapshots


def _iter_filtered_company_snapshot_dirs(*, runtime_dir: str | Path, company_filter: str) -> list[Path]:
    """List only the requested company's snapshot dirs.

    W5 overlap can be run for very large companies after a repair apply. A
    company-filtered audit must not enumerate every historical company tree just
    to discard it later.
    """

    normalized_filter = _normalize_text(company_filter)
    if not normalized_filter:
        return []
    snapshot_dirs: list[Path] = []
    seen_snapshot_keys: set[tuple[str, str]] = set()
    for root in company_assets_roots(runtime_dir, prefer_hot_cache=True, existing_only=True):
        if not root.exists():
            continue
        matched_company_dirs: list[Path] = []
        direct = root / normalized_filter
        if direct.exists() and direct.is_dir():
            matched_company_dirs.append(direct)
        # Keep alias/case coverage without descending into every company's snapshots.
        for company_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            resolved_company_key = _local_asset_company_key(company_dir.name)
            if resolved_company_key == normalized_filter and company_dir not in matched_company_dirs:
                matched_company_dirs.append(company_dir)
        for company_dir in matched_company_dirs:
            company_key = _local_asset_company_key(company_dir.name)
            for snapshot_dir in sorted(path for path in company_dir.iterdir() if path.is_dir()):
                snapshot_key = (company_key, _normalize_text(snapshot_dir.name))
                if not snapshot_key[1] or snapshot_key in seen_snapshot_keys:
                    continue
                seen_snapshot_keys.add(snapshot_key)
                snapshot_dirs.append(snapshot_dir)
    return snapshot_dirs


def _build_snapshot_index(
    *,
    registry_rows: list[dict[str, Any]],
    local_snapshots: list[dict[str, Any]],
    asset_view: str,
) -> dict[str, dict[str, dict[str, Any]]]:
    index: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for row in registry_rows:
        snapshot_id = _normalize_text(row.get("snapshot_id"))
        if not snapshot_id:
            continue
        company_key = _company_key_from_row(row)
        if not company_key:
            continue
        record = index[company_key].setdefault(snapshot_id, {"snapshot_id": snapshot_id})
        record["registry_row"] = dict(row)
        record["asset_view"] = _normalize_text(row.get("asset_view")) or asset_view
    for snapshot in local_snapshots:
        snapshot_id = _normalize_text(snapshot.get("snapshot_id"))
        company_key = _normalize_text(snapshot.get("company_key"))
        if not snapshot_id or not company_key:
            continue
        record = index[company_key].setdefault(snapshot_id, {"snapshot_id": snapshot_id})
        record["local_snapshot"] = dict(snapshot)
        record.setdefault("asset_view", asset_view)
        if snapshot.get("latest_snapshot_id"):
            record["latest_snapshot_id"] = _normalize_text(snapshot.get("latest_snapshot_id"))
    return index


def _projection_dependencies_by_snapshot(
    *,
    store: ControlPlaneStore,
    collection_id: str,
    snapshot_ids: set[str],
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    dependencies: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not snapshot_ids:
        return dependencies
    projections = store.repos.serving_projection.list(
        collection_id=collection_id,
        limit=max(1000, limit * 5),
    )
    pointer = store.repos.serving_projection.get_authoritative_pointer(collection_id)
    active_projection_id = _normalize_text(pointer.get("active_projection_id"))
    for projection in projections:
        referenced_snapshot_ids = _projection_snapshot_ids(projection)
        for snapshot_id in sorted(referenced_snapshot_ids & snapshot_ids):
            projection_id = _normalize_text(projection.get("projection_id"))
            state = _normalize_text(projection.get("state")).lower()
            dependencies[snapshot_id].append(
                {
                    "projection_id": projection_id,
                    "projection_type": _normalize_text(projection.get("projection_type")),
                    "state": state,
                    "source_run_id": _normalize_text(projection.get("source_run_id")),
                    "active_state": state in ACTIVE_PROJECTION_STATES,
                    "active_pointer": bool(active_projection_id and projection_id == active_projection_id),
                    "member_count": store.count_serving_projection_members(projection_id, visible_only=False)
                    if projection_id
                    else 0,
                    "reference_fields": sorted(_projection_snapshot_reference_fields(projection, snapshot_id)),
                }
            )
    return dependencies


def _projection_snapshot_ids(projection: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for field in ("source_collection_version", "raw_profile_index_watermark", "evidence_index_watermark"):
        values.update(_extract_snapshot_like_values(projection.get(field)))
    for payload_field in ("scope_spec", "counts", "readiness", "provenance", "metadata"):
        values.update(_extract_snapshot_like_values(projection.get(payload_field)))
    return values


def _projection_snapshot_reference_fields(projection: dict[str, Any], snapshot_id: str) -> set[str]:
    fields: set[str] = set()
    for field in ("source_collection_version", "raw_profile_index_watermark", "evidence_index_watermark"):
        if snapshot_id in _extract_snapshot_like_values(projection.get(field)):
            fields.add(field)
    for payload_field in ("scope_spec", "counts", "readiness", "provenance", "metadata"):
        if snapshot_id in _extract_snapshot_like_values(projection.get(payload_field)):
            fields.add(payload_field)
    return fields


def _crm_person_dependencies_by_projection(
    *,
    store: ControlPlaneStore,
    projection_ids: list[str],
    limit: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    crm_dependencies: dict[str, list[dict[str, Any]]] = defaultdict(list)
    person_asset_dependencies: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for projection_id in projection_ids:
        for row in store.list_crm_records(source_projection_id=projection_id, limit=max(50, limit)):
            crm_dependencies[projection_id].append(
                {
                    "crm_record_id": _normalize_text(row.get("crm_record_id")),
                    "person_identity_key": _normalize_text(row.get("person_identity_key")),
                    "lifecycle_status": _normalize_text(row.get("lifecycle_status")),
                    "visibility_status": _normalize_text(row.get("visibility_status")),
                }
            )
        for row in store.list_person_assets(source_projection_id=projection_id, limit=max(50, limit)):
            person_asset_dependencies[projection_id].append(
                {
                    "asset_id": _normalize_text(row.get("asset_id")),
                    "person_identity_key": _normalize_text(row.get("person_identity_key")),
                    "asset_type": _normalize_text(row.get("asset_type")),
                    "status": _normalize_text(row.get("status")),
                }
            )
    return crm_dependencies, person_asset_dependencies


def _summarize_shards_by_snapshot(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = defaultdict(_empty_shard_summary)
    for row in rows:
        snapshot_id = _normalize_text(row.get("snapshot_id"))
        if not snapshot_id:
            continue
        summary = result[snapshot_id]
        status = _normalize_text(row.get("status")).lower()
        lane = _normalize_text(row.get("lane")) or "unknown"
        query = _normalize_text(row.get("search_query") or row.get("shard_title") or row.get("shard_id"))
        summary["total_shard_count"] += 1
        summary["result_count"] += _safe_int(row.get("result_count"))
        summary["lanes"][lane] = summary["lanes"].get(lane, 0) + 1
        summary["statuses"][status or "unknown"] = summary["statuses"].get(status or "unknown", 0) + 1
        if status in REUSABLE_SHARD_STATUSES:
            summary["reusable_shard_count"] += 1
            if query:
                summary["reusable_queries"].append(query)
        if len(summary["sample_shards"]) < 5:
            summary["sample_shards"].append(
                {
                    "shard_key": _normalize_text(row.get("shard_key")),
                    "lane": lane,
                    "status": status,
                    "employment_scope": _normalize_text(row.get("employment_scope")),
                    "search_query": query,
                    "result_count": _safe_int(row.get("result_count")),
                }
            )
    for summary in result.values():
        summary["reusable_queries"] = _dedupe_texts(summary["reusable_queries"])[:20]
        summary["lanes"] = dict(sorted(summary["lanes"].items()))
        summary["statuses"] = dict(sorted(summary["statuses"].items()))
    return result


def _deletion_blockers(
    *,
    snapshot_id: str,
    registry_row: dict[str, Any],
    authoritative_snapshot_ids: set[str],
    selected_snapshot_ids: set[str],
    latest_local_snapshot_ids: set[str],
    shard_summary: dict[str, Any],
    projection_refs: list[dict[str, Any]],
    crm_refs: list[dict[str, Any]],
    person_asset_refs: list[dict[str, Any]],
    active_pointer_projection_id: str,
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    if snapshot_id in authoritative_snapshot_ids:
        blockers.append({"type": "authoritative_registry_pointer", "severity": "hard"})
    if snapshot_id in selected_snapshot_ids:
        blockers.append({"type": "selected_source_snapshot", "severity": "hard"})
    if snapshot_id in latest_local_snapshot_ids:
        blockers.append({"type": "latest_snapshot_pointer", "severity": "review"})
    if shard_summary.get("reusable_shard_count", 0) > 0:
        blockers.append(
            {
                "type": "reusable_acquisition_shard",
                "severity": "hard",
                "count": int(shard_summary.get("reusable_shard_count") or 0),
            }
        )
    active_projection_refs = [
        ref
        for ref in projection_refs
        if ref.get("active_state") or ref.get("active_pointer") or ref.get("projection_id") == active_pointer_projection_id
    ]
    if active_projection_refs:
        blockers.append(
            {
                "type": "active_serving_projection_dependency",
                "severity": "hard",
                "projection_ids": _dedupe_texts(ref.get("projection_id") for ref in active_projection_refs)[:10],
            }
        )
    if crm_refs:
        blockers.append(
            {
                "type": "crm_record_source_projection_dependency",
                "severity": "hard",
                "count": len(crm_refs),
            }
        )
    if person_asset_refs:
        blockers.append(
            {
                "type": "person_asset_source_projection_dependency",
                "severity": "hard",
                "count": len(person_asset_refs),
            }
        )
    if registry_row and not _normalize_text(registry_row.get("status")):
        blockers.append({"type": "registry_status_missing", "severity": "review"})
    return blockers


def _classify_snapshot(
    *,
    registry_row: dict[str, Any],
    local_snapshot: dict[str, Any],
    shard_summary: dict[str, Any],
    projection_refs: list[dict[str, Any]],
    blockers: list[dict[str, Any]],
) -> str:
    if registry_row.get("authoritative"):
        return "keep_authoritative_serving"
    if shard_summary.get("reusable_shard_count", 0) > 0:
        return "keep_reusable_shard_source"
    if projection_refs:
        return "keep_projection_dependency"
    if blockers:
        return "review_blocked_dependency"
    if not registry_row and local_snapshot:
        return "review_local_only_snapshot"
    if registry_row:
        return ARCHIVE_CANDIDATE_CLASSIFICATION
    return "review_missing_registry_or_dependency"


def _attach_overlap_evidence(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company_key: str,
    target_company: str,
    asset_view: str,
    collection_pointer: dict[str, Any],
    authoritative_snapshot_ids: set[str],
    selected_snapshot_ids: set[str],
    snapshots: list[dict[str, Any]],
    max_candidates_per_snapshot: int,
    max_archive_snapshots: int,
) -> dict[str, Any]:
    canonical_reference = _load_collection_pointer_identity_set(
        store=store,
        collection_pointer=collection_pointer,
        max_candidates=max_candidates_per_snapshot,
    )
    if canonical_reference:
        reference_source = "collection_authoritative_pointer"
        reference_projection_id = _normalize_text(canonical_reference.get("projection_id"))
        reference_snapshot_ids = _dedupe_texts([canonical_reference.get("active_collection_version")])
        reference_sets = {_normalize_text(canonical_reference.get("reference_key")): canonical_reference}
        reference_identity_keys = set(canonical_reference.get("identity_keys") or set())
        legacy_reference_snapshot_ids = _dedupe_texts([*sorted(authoritative_snapshot_ids), *sorted(selected_snapshot_ids)])
    else:
        reference_source = "organization_asset_registry"
        reference_projection_id = ""
        reference_snapshot_ids = _dedupe_texts([*sorted(authoritative_snapshot_ids), *sorted(selected_snapshot_ids)])
        legacy_reference_snapshot_ids = []
        reference_sets: dict[str, dict[str, Any]] = {}
        reference_identity_keys: set[str] = set()
        for snapshot_id in reference_snapshot_ids:
            identity_set = _load_snapshot_identity_set(
                runtime_dir=runtime_dir,
                company_key=company_key,
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                max_candidates=max_candidates_per_snapshot,
            )
            reference_sets[snapshot_id] = identity_set
            reference_identity_keys.update(identity_set.get("identity_keys") or set())

    reference_errors = [
        _normalize_text(payload.get("error") or payload.get("status"))
        for payload in reference_sets.values()
        if payload.get("status") != "loaded"
    ]

    archive_snapshots = [
        snapshot
        for snapshot in snapshots
        if snapshot.get("classification") == ARCHIVE_CANDIDATE_CLASSIFICATION and not snapshot.get("deletion_blockers")
    ][:max_archive_snapshots]
    results: list[dict[str, Any]] = []
    if not reference_identity_keys:
        for snapshot in archive_snapshots:
            snapshot_id = _normalize_text(snapshot.get("snapshot_id"))
            result = {
                "snapshot_id": snapshot_id,
                "status": "review_reference_identity_missing",
                "source_path": "",
                "candidate_count": 0,
                "identity_count": 0,
                "reference_identity_count": 0,
                "overlap_count": 0,
                "unique_count": 0,
                "overlap_ratio": 0.0,
                "subsumed_by_reference": False,
                "truncated": False,
                "error": "reference_identity_missing",
                "reference_errors": reference_errors,
                "unique_identity_samples": [],
            }
            snapshot["overlap_subsumption"] = result
            snapshot["archive_ready"] = False
            results.append(result)
        for snapshot in snapshots:
            if snapshot.get("classification") == ARCHIVE_CANDIDATE_CLASSIFICATION and "overlap_subsumption" not in snapshot:
                snapshot["overlap_subsumption"] = {
                    "status": "not_evaluated",
                    "reason": "outside_overlap_snapshot_limit",
                }
                snapshot["archive_ready"] = False
        return {
            "enabled": True,
            "contract_version": "asset_overlap_subsumption_v1",
            "scope": "archive_candidates_only",
            "reference_source": reference_source,
            "reference_projection_id": reference_projection_id,
            "reference_snapshot_ids": reference_snapshot_ids,
            "legacy_registry_reference_snapshot_ids": legacy_reference_snapshot_ids,
            "reference_identity_count": 0,
            "reference_snapshot_count": len(reference_sets),
            "archive_candidate_count": len(archive_snapshots),
            "evaluated_archive_candidate_count": len(results),
            "subsumed_count": 0,
            "review_required_count": len(results),
            "max_candidates_per_snapshot": max_candidates_per_snapshot,
            "max_archive_snapshots": max_archive_snapshots,
            "reference_loads": _summarize_reference_loads(reference_sets),
            "results": results,
        }
    for snapshot in archive_snapshots:
        snapshot_id = _normalize_text(snapshot.get("snapshot_id"))
        candidate_set = _load_snapshot_identity_set(
            runtime_dir=runtime_dir,
            company_key=company_key,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            max_candidates=max_candidates_per_snapshot,
        )
        identity_keys = set(candidate_set.get("identity_keys") or set())
        overlap_count = len(identity_keys & reference_identity_keys)
        unique_count = max(0, len(identity_keys) - overlap_count)
        overlap_ratio = (overlap_count / len(identity_keys)) if identity_keys else 0.0
        status = _overlap_status(
            candidate_set=candidate_set,
            reference_identity_keys=reference_identity_keys,
            unique_count=unique_count,
            overlap_ratio=overlap_ratio,
        )
        result = {
            "snapshot_id": snapshot_id,
            "status": status,
            "source_path": _normalize_text(candidate_set.get("source_path")),
            "candidate_count": int(candidate_set.get("candidate_count") or 0),
            "identity_count": len(identity_keys),
            "reference_identity_count": len(reference_identity_keys),
            "overlap_count": overlap_count,
            "unique_count": unique_count,
            "overlap_ratio": round(overlap_ratio, 6),
            "subsumed_by_reference": status == "subsumed_by_reference",
            "truncated": bool(candidate_set.get("truncated")),
            "error": _normalize_text(candidate_set.get("error")),
            "unique_identity_samples": sorted(identity_keys - reference_identity_keys)[:10],
        }
        snapshot["overlap_subsumption"] = result
        if status != "subsumed_by_reference":
            snapshot["archive_ready"] = False
        results.append(result)

    for snapshot in snapshots:
        if snapshot.get("classification") == ARCHIVE_CANDIDATE_CLASSIFICATION and "overlap_subsumption" not in snapshot:
            snapshot["overlap_subsumption"] = {
                "status": "not_evaluated",
                "reason": "outside_overlap_snapshot_limit",
            }
            snapshot["archive_ready"] = False

    return {
        "enabled": True,
        "contract_version": "asset_overlap_subsumption_v1",
        "scope": "archive_candidates_only",
        "reference_source": reference_source,
        "reference_projection_id": reference_projection_id,
        "reference_snapshot_ids": reference_snapshot_ids,
        "legacy_registry_reference_snapshot_ids": legacy_reference_snapshot_ids,
        "reference_identity_count": len(reference_identity_keys),
        "reference_snapshot_count": len(reference_sets),
        "archive_candidate_count": len(archive_snapshots),
        "evaluated_archive_candidate_count": len(results),
        "subsumed_count": sum(1 for item in results if item.get("subsumed_by_reference")),
        "review_required_count": sum(1 for item in results if not item.get("subsumed_by_reference")),
        "max_candidates_per_snapshot": max_candidates_per_snapshot,
        "max_archive_snapshots": max_archive_snapshots,
        "reference_loads": _summarize_reference_loads(reference_sets),
        "results": results,
    }


def _summarize_reference_loads(reference_sets: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "reference_key": snapshot_id,
            "source_type": payload.get("source_type", "snapshot_payload"),
            "snapshot_id": payload.get("snapshot_id", snapshot_id),
            "projection_id": payload.get("projection_id", ""),
            "status": payload.get("status"),
            "identity_count": len(payload.get("identity_keys") or set()),
            "candidate_count": payload.get("candidate_count", 0),
            "truncated": bool(payload.get("truncated")),
            "source_path": payload.get("source_path", ""),
            "error": payload.get("error", ""),
        }
        for snapshot_id, payload in reference_sets.items()
    ]


def _load_collection_pointer_identity_set(
    *,
    store: ControlPlaneStore,
    collection_pointer: dict[str, Any],
    max_candidates: int,
) -> dict[str, Any]:
    projection_id = _normalize_text(collection_pointer.get("active_projection_id"))
    if not projection_id:
        return {}
    active_collection_version = _normalize_text(collection_pointer.get("active_collection_version"))
    projection = store.repos.serving_projection.get(projection_id)
    if not projection:
        return {
            "reference_key": f"projection:{projection_id}",
            "source_type": "collection_authoritative_projection",
            "projection_id": projection_id,
            "snapshot_id": active_collection_version,
            "active_collection_version": active_collection_version,
            "status": "missing_projection",
            "candidate_count": 0,
            "identity_count": 0,
            "identity_keys": set(),
            "error": "active_projection_not_found",
        }
    state = _normalize_text(projection.get("state")).lower()
    if state not in ACTIVE_PROJECTION_STATES:
        return {
            "reference_key": f"projection:{projection_id}",
            "source_type": "collection_authoritative_projection",
            "projection_id": projection_id,
            "snapshot_id": active_collection_version,
            "active_collection_version": active_collection_version,
            "status": "projection_not_active",
            "candidate_count": 0,
            "identity_count": 0,
            "identity_keys": set(),
            "error": f"projection_state:{state or 'missing'}",
        }
    total_count = store.count_serving_projection_members(projection_id, visible_only=True)
    members = store.list_serving_projection_members(
        projection_id,
        limit=max(1, int(max_candidates or DEFAULT_OVERLAP_CANDIDATE_LIMIT)),
        visible_only=True,
    )
    identity_keys = {
        _normalize_text(
            member.get("candidate_identity_key")
            or member.get("person_identity_key")
            or dict(member.get("public_summary") or {}).get("person_identity_key")
            or dict(member.get("public_summary") or {}).get("profile_url_key")
        )
        for member in members
        if _normalize_text(
            member.get("candidate_identity_key")
            or member.get("person_identity_key")
            or dict(member.get("public_summary") or {}).get("person_identity_key")
            or dict(member.get("public_summary") or {}).get("profile_url_key")
        )
    }
    if not identity_keys:
        status = "empty_projection_members"
        error = "projection_identity_missing"
    else:
        status = "loaded"
        error = ""
    return {
        "reference_key": f"projection:{projection_id}",
        "source_type": "collection_authoritative_projection",
        "projection_id": projection_id,
        "snapshot_id": active_collection_version,
        "active_collection_version": active_collection_version,
        "status": status,
        "source_path": projection_id,
        "candidate_count": total_count,
        "scanned_candidate_count": len(members),
        "identity_count": len(identity_keys),
        "identity_keys": identity_keys,
        "truncated": total_count > len(members),
        "error": error,
    }


def _empty_overlap_summary(*, enabled: bool) -> dict[str, Any]:
    return {
        "enabled": bool(enabled),
        "contract_version": "asset_overlap_subsumption_v1",
        "scope": "archive_candidates_only",
        "reference_snapshot_ids": [],
        "reference_identity_count": 0,
        "archive_candidate_count": 0,
        "evaluated_archive_candidate_count": 0,
        "subsumed_count": 0,
        "review_required_count": 0,
        "results": [],
    }


def _overlap_status(
    *,
    candidate_set: dict[str, Any],
    reference_identity_keys: set[str],
    unique_count: int,
    overlap_ratio: float,
) -> str:
    if candidate_set.get("status") != "loaded":
        return "review_candidate_identity_load_failed"
    if bool(candidate_set.get("truncated")):
        return "review_candidate_identity_truncated"
    if not reference_identity_keys:
        return "review_reference_identity_missing"
    if int(candidate_set.get("identity_count") or 0) <= 0:
        return "review_candidate_identity_missing"
    if unique_count == 0 and overlap_ratio >= 0.999:
        return "subsumed_by_reference"
    return "review_unique_candidates_present"


def _load_snapshot_identity_set(
    *,
    runtime_dir: str | Path,
    company_key: str,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    max_candidates: int,
) -> dict[str, Any]:
    snapshot_dir = resolve_company_snapshot_dir_by_key(
        runtime_dir,
        company_key=company_key,
        snapshot_id=snapshot_id,
        prefer_hot_cache=True,
    )
    if snapshot_dir is None:
        return {
            "status": "missing_snapshot_dir",
            "snapshot_id": snapshot_id,
            "candidate_count": 0,
            "identity_count": 0,
            "identity_keys": set(),
            "error": "snapshot_dir_not_found",
        }
    payload_path = _snapshot_candidate_identity_payload_path(snapshot_dir, asset_view=asset_view)
    if payload_path is None:
        return {
            "status": "missing_candidate_payload",
            "snapshot_id": snapshot_id,
            "snapshot_dir": str(snapshot_dir),
            "candidate_count": 0,
            "identity_count": 0,
            "identity_keys": set(),
            "error": "candidate_payload_not_found",
        }
    payload = load_company_snapshot_json(payload_path)
    candidates = list(payload.get("candidates") or [])
    if not candidates:
        return {
            "status": "empty_candidate_payload",
            "snapshot_id": snapshot_id,
            "snapshot_dir": str(snapshot_dir),
            "source_path": str(payload_path),
            "candidate_count": 0,
            "identity_count": 0,
            "identity_keys": set(),
            "error": "candidate_payload_empty",
        }
    target_keys = {_resolve_company_key(target_company), _resolve_company_key(company_key)}
    identity_keys: set[str] = set()
    scanned_count = 0
    for candidate in candidates[:max_candidates]:
        if not isinstance(candidate, dict):
            continue
        candidate_target_key = _resolve_company_key(
            candidate.get("target_company")
            or candidate.get("company")
            or candidate.get("organization")
            or ""
        )
        if candidate_target_key and target_keys and candidate_target_key not in target_keys:
            continue
        identity_key = _candidate_identity_from_payload(candidate)
        if identity_key:
            identity_keys.add(identity_key)
        scanned_count += 1
    return {
        "status": "loaded",
        "snapshot_id": snapshot_id,
        "snapshot_dir": str(snapshot_dir),
        "source_path": str(payload_path),
        "candidate_count": len(candidates),
        "scanned_candidate_count": scanned_count,
        "identity_count": len(identity_keys),
        "identity_keys": identity_keys,
        "truncated": len(candidates) > max_candidates,
    }


def _snapshot_candidate_identity_payload_path(snapshot_dir: Path, *, asset_view: str) -> Path | None:
    normalized_view = _normalize_text(asset_view) or "canonical_merged"
    normalized_dir = Path(snapshot_dir)
    candidates: list[Path] = []
    artifact_dir = normalized_dir / "normalized_artifacts"
    if normalized_view != "canonical_merged":
        candidates.extend(
            [
                normalized_dir / "candidate_documents.json",
                artifact_dir / normalized_view / "materialized_candidate_documents.json",
                artifact_dir / normalized_view / "manifest.json",
            ]
        )
    candidates.extend(
        [
            normalized_dir / "candidate_documents.json",
            artifact_dir / "materialized_candidate_documents.json",
            artifact_dir / "manifest.json",
        ]
    )
    for candidate in candidates:
        if not candidate.exists() or not candidate.is_file():
            continue
        payload = load_company_snapshot_json(candidate)
        if isinstance(payload.get("candidates"), list):
            return candidate
    return None


def _candidate_identity_from_payload(payload: dict[str, Any]) -> str:
    public_summary = dict(payload.get("public_summary") or {})
    metadata = dict(payload.get("metadata") or {})
    profile_url_key = resolve_profile_url_key(
        payload.get("profile_url_key"),
        public_summary.get("profile_url_key"),
        payload.get("linkedin_url"),
        public_summary.get("linkedin_url"),
        payload.get("profile_url"),
        metadata.get("profile_url"),
        metadata.get("linkedin_url"),
    )
    if profile_url_key:
        return f"linkedin:{profile_url_key}"
    person_identity_key = _normalize_text(payload.get("person_identity_key"))
    if person_identity_key:
        return person_identity_key
    return resolve_candidate_identity_key(
        candidate_identity_key=_normalize_text(payload.get("candidate_identity_key")),
        person_identity_key="",
        profile_url_key=profile_url_key,
        linkedin_url=_normalize_text(payload.get("linkedin_url") or public_summary.get("linkedin_url")),
        candidate_id=_normalize_text(payload.get("candidate_id") or payload.get("id")),
    )


def _empty_shard_summary() -> dict[str, Any]:
    return {
        "total_shard_count": 0,
        "reusable_shard_count": 0,
        "result_count": 0,
        "lanes": {},
        "statuses": {},
        "reusable_queries": [],
        "sample_shards": [],
    }


def _summarize_registry_row(row: dict[str, Any]) -> dict[str, Any]:
    if not row:
        return {"present": False}
    return {
        "present": True,
        "registry_id": _safe_int(row.get("registry_id")),
        "target_company": _normalize_text(row.get("target_company")),
        "company_key": _normalize_text(row.get("company_key")),
        "snapshot_id": _normalize_text(row.get("snapshot_id")),
        "asset_view": _normalize_text(row.get("asset_view")),
        "status": _normalize_text(row.get("status")),
        "authoritative": bool(row.get("authoritative")),
        "candidate_count": _safe_int(row.get("candidate_count")),
        "profile_detail_count": _safe_int(row.get("profile_detail_count")),
        "source_snapshot_count": _safe_int(row.get("source_snapshot_count")),
        "selected_snapshot_ids": _dedupe_texts(row.get("selected_snapshot_ids") or []),
        "source_path": _normalize_text(row.get("source_path")),
        "source_job_id": _normalize_text(row.get("source_job_id")),
        "updated_at": _normalize_text(row.get("updated_at")),
    }


def _summarize_local_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not snapshot:
        return {"present": False}
    return {
        "present": True,
        "path": _normalize_text(snapshot.get("path")),
        "latest_snapshot_id": _normalize_text(snapshot.get("latest_snapshot_id")),
        "exists": bool(snapshot.get("exists")),
    }


def _authoritative_snapshot_ids(registry_rows: list[dict[str, Any]]) -> set[str]:
    return {
        _normalize_text(row.get("snapshot_id"))
        for row in registry_rows
        if bool(row.get("authoritative")) and _normalize_text(row.get("snapshot_id"))
    }


def _selected_snapshot_ids(registry_rows: list[dict[str, Any]]) -> set[str]:
    """Return source snapshots selected by the current authoritative row only.

    Historical registry rows often carry their own `selected_snapshot_ids`; using
    every row would make old snapshots block their own cleanup forever. W5
    deletion blockers should reflect the current authoritative source contract.
    """

    selected: set[str] = set()
    authoritative_rows = [row for row in registry_rows if bool(row.get("authoritative"))]
    for row in authoritative_rows:
        selected.update(_dedupe_texts(row.get("selected_snapshot_ids") or []))
        selection = dict(row.get("source_snapshot_selection") or {})
        selected.update(_dedupe_texts(selection.get("selected_snapshot_ids") or []))
        selected.update(_dedupe_texts(selection.get("reusable_source_snapshot_ids") or []))
    return {item for item in selected if item}


def _extract_snapshot_like_values(value: Any) -> set[str]:
    values: set[str] = set()
    if value is None:
        return values
    if isinstance(value, str):
        text = _normalize_text(value)
        if text:
            values.add(text)
        return values
    if isinstance(value, dict):
        for key, child in value.items():
            normalized_key = _normalize_text(key).lower()
            if (
                normalized_key == "snapshot_id"
                or normalized_key.endswith("_snapshot_id")
                or normalized_key.endswith("_snapshot_ids")
                or normalized_key in {"selected_snapshot_ids", "source_snapshot_ids", "reusable_source_snapshot_ids"}
            ):
                values.update(_extract_snapshot_like_values(child))
            elif isinstance(child, (dict, list, tuple, set)):
                values.update(_extract_snapshot_like_values(child))
        return values
    if isinstance(value, (list, tuple, set)):
        for child in value:
            values.update(_extract_snapshot_like_values(child))
    return values


def _snapshot_sort_key(record: dict[str, Any]) -> tuple[int, int, str, str]:
    row = dict(record.get("registry_row") or {})
    return (
        1 if bool(row.get("authoritative")) else 0,
        _safe_int(row.get("candidate_count")),
        _normalize_text(row.get("updated_at")),
        _normalize_text(record.get("snapshot_id")),
    )


def _company_order(
    snapshot_index: dict[str, dict[str, dict[str, Any]]],
    *,
    limit: int,
) -> list[str]:
    return sorted(
        snapshot_index.keys(),
        key=lambda company_key: (
            -len(snapshot_index[company_key]),
            company_key,
        ),
    )[:limit]


def _best_target_company(snapshot_records: dict[str, dict[str, Any]], company_key: str) -> str:
    for record in snapshot_records.values():
        row = dict(record.get("registry_row") or {})
        target_company = _normalize_text(row.get("target_company"))
        if target_company:
            return target_company
    return company_key


def _company_matches(row: dict[str, Any], company_key: str) -> bool:
    return company_key in {
        _company_key_from_row(row),
        _resolve_company_key(row.get("target_company")),
        _resolve_company_key(row.get("company_key")),
    }


def _company_key_from_row(row: dict[str, Any]) -> str:
    return _resolve_company_key(row.get("company_key") or row.get("target_company"))


def _local_asset_company_key(value: Any) -> str:
    """Normalize local asset directory names without triggering alias discovery."""

    return normalize_company_key(_normalize_text(value))


def _normalize_company_filter(company: str) -> str:
    return _resolve_company_key(company) if _normalize_text(company) else ""


def _resolve_company_key(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    # W5 asset audits must not call the dynamic company alias resolver here.
    # That resolver discovers aliases by scanning local assets, which turns a
    # company-scoped audit into recursive full asset discovery.
    return normalize_company_key(text)


def _safe_int(value: Any) -> int:
    try:
        if value in {None, ""}:
            return 0
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_texts(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = _normalize_text(value)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result
