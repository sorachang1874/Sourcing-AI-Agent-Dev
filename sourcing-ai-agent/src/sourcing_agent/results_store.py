from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifact_cache import mark_hot_cache_snapshot_access
from .candidate_artifacts import CandidateArtifactError, _resolve_company_snapshot
from .snapshot_state import read_json_dict

_DEFAULT_CANDIDATE_PAGE_SIZE = 50


@dataclass(frozen=True, slots=True)
class SnapshotArtifactStore:
    runtime_dir: Path
    target_company: str
    snapshot_id: str
    asset_view: str
    company_key: str
    snapshot_dir: Path
    artifact_dir: Path
    company_identity: dict[str, Any]


def open_snapshot_artifact_store(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
) -> SnapshotArtifactStore:
    company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
        runtime_dir,
        target_company,
        snapshot_id=snapshot_id,
    )
    mark_hot_cache_snapshot_access(snapshot_dir, runtime_dir=runtime_dir)
    normalized_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    artifact_dir = snapshot_dir / "normalized_artifacts"
    if normalized_view != "canonical_merged":
        artifact_dir = artifact_dir / normalized_view
    return SnapshotArtifactStore(
        runtime_dir=Path(runtime_dir),
        target_company=str(identity_payload.get("canonical_name") or target_company).strip() or target_company,
        snapshot_id=snapshot_dir.name,
        asset_view=normalized_view,
        company_key=str(identity_payload.get("company_key") or company_key).strip() or company_key,
        snapshot_dir=snapshot_dir,
        artifact_dir=artifact_dir,
        company_identity=dict(identity_payload or {}),
    )


def read_snapshot_artifact_summary(store: SnapshotArtifactStore) -> dict[str, Any]:
    payload = read_json_dict(store.artifact_dir / "artifact_summary.json")
    if not payload:
        return {}
    payload.setdefault("target_company", store.target_company)
    payload.setdefault("company_key", store.company_key)
    payload.setdefault("snapshot_id", store.snapshot_id)
    payload.setdefault("asset_view", store.asset_view)
    return payload


def read_snapshot_candidate_manifest(store: SnapshotArtifactStore) -> dict[str, Any]:
    payload = read_json_dict(store.artifact_dir / "manifest.json")
    if not payload:
        raise CandidateArtifactError(
            f"Candidate manifest not found for {store.target_company} snapshot {store.snapshot_id} ({store.asset_view})"
        )
    payload.setdefault("target_company", store.target_company)
    payload.setdefault("company_key", store.company_key)
    payload.setdefault("snapshot_id", store.snapshot_id)
    payload.setdefault("asset_view", store.asset_view)
    return payload


def read_snapshot_candidate_page(store: SnapshotArtifactStore, *, page_number: int) -> dict[str, Any]:
    normalized_page_number = max(int(page_number or 0), 1)
    manifest_payload = read_snapshot_candidate_manifest(store)
    page_path_map = {
        int(item.get("page") or 0): str(item.get("path") or "").strip()
        for item in list(manifest_payload.get("pages") or [])
        if int(item.get("page") or 0) > 0 and str(item.get("path") or "").strip()
    }
    relative_path = page_path_map.get(normalized_page_number) or f"pages/page-{normalized_page_number:04d}.json"
    payload = read_json_dict(store.artifact_dir / relative_path)
    if not payload:
        raise CandidateArtifactError(
            f"Candidate page {normalized_page_number} not found for {store.target_company} snapshot {store.snapshot_id}"
        )
    payload.setdefault("target_company", store.target_company)
    payload.setdefault("snapshot_id", store.snapshot_id)
    payload.setdefault("asset_view", store.asset_view)
    payload.setdefault("page", normalized_page_number)
    return payload


def read_snapshot_candidate_window(
    store: SnapshotArtifactStore,
    *,
    offset: int = 0,
    limit: int = 0,
) -> dict[str, Any]:
    manifest_payload = read_snapshot_candidate_manifest(store)
    pagination = dict(manifest_payload.get("pagination") or {})
    page_size = max(
        int(pagination.get("page_size") or 0)
        or int(read_snapshot_artifact_summary(store).get("candidate_page_size") or 0)
        or _DEFAULT_CANDIDATE_PAGE_SIZE,
        1,
    )
    total_candidates = max(int(manifest_payload.get("candidate_count") or 0), 0)
    normalized_offset = max(int(offset or 0), 0)
    normalized_limit = max(int(limit or 0), 0)
    if total_candidates <= 0 or normalized_offset >= total_candidates:
        return {
            "target_company": store.target_company,
            "snapshot_id": store.snapshot_id,
            "asset_view": store.asset_view,
            "offset": normalized_offset,
            "limit": normalized_limit,
            "page_size": page_size,
            "page_count": max(int(pagination.get("page_count") or 0), 0),
            "total_candidate_count": total_candidates,
            "candidates": [],
        }

    end_index = total_candidates if normalized_limit <= 0 else min(total_candidates, normalized_offset + normalized_limit)
    start_page = (normalized_offset // page_size) + 1
    end_page = max(start_page, ((max(end_index - 1, normalized_offset)) // page_size) + 1)
    combined_candidates: list[dict[str, Any]] = []
    for page_number in range(start_page, end_page + 1):
        page_payload = read_snapshot_candidate_page(store, page_number=page_number)
        combined_candidates.extend(
            dict(item)
            for item in list(page_payload.get("candidates") or [])
            if isinstance(item, dict)
        )

    slice_offset = normalized_offset - ((start_page - 1) * page_size)
    requested_count = max(end_index - normalized_offset, 0)
    selected_candidates = combined_candidates[slice_offset : slice_offset + requested_count]
    return {
        "target_company": store.target_company,
        "snapshot_id": store.snapshot_id,
        "asset_view": store.asset_view,
        "offset": normalized_offset,
        "limit": normalized_limit,
        "page_size": page_size,
        "page_count": max(int(pagination.get("page_count") or 0), 0),
        "total_candidate_count": total_candidates,
        "candidates": selected_candidates,
    }


def read_snapshot_candidate_shard(store: SnapshotArtifactStore, *, candidate_id: str) -> dict[str, Any]:
    normalized_candidate_id = str(candidate_id or "").strip()
    if not normalized_candidate_id:
        raise CandidateArtifactError("candidate_id is required")
    manifest_payload = read_snapshot_candidate_manifest(store)
    shard_entry = next(
        (
            dict(item)
            for item in list(manifest_payload.get("candidate_shards") or [])
            if str(item.get("candidate_id") or "").strip() == normalized_candidate_id
        ),
        {},
    )
    shard_path = str(shard_entry.get("path") or "").strip()
    if not shard_path:
        raise CandidateArtifactError(
            f"Candidate shard not found for {normalized_candidate_id} in {store.target_company} snapshot {store.snapshot_id}"
        )
    payload = read_json_dict(store.artifact_dir / shard_path)
    if not payload:
        raise CandidateArtifactError(
            f"Candidate shard payload missing for {normalized_candidate_id} in {store.target_company} snapshot {store.snapshot_id}"
        )
    payload.setdefault("candidate_id", normalized_candidate_id)
    payload.setdefault("snapshot_id", store.snapshot_id)
    payload.setdefault("asset_view", store.asset_view)
    return payload


def read_snapshot_backlog_view(store: SnapshotArtifactStore, *, backlog_kind: str) -> dict[str, Any]:
    normalized_kind = str(backlog_kind or "").strip().lower()
    if normalized_kind not in {"manual_review", "profile_completion"}:
        raise CandidateArtifactError(f"Unsupported backlog kind: {backlog_kind}")
    relative_path = str(
        dict(read_snapshot_candidate_manifest(store).get("backlogs") or {}).get(normalized_kind) or ""
    ).strip() or f"backlogs/{normalized_kind}.json"
    payload = read_json_dict(store.artifact_dir / relative_path)
    if not payload:
        raise CandidateArtifactError(
            f"Backlog view {normalized_kind} not found for {store.target_company} snapshot {store.snapshot_id}"
        )
    payload.setdefault("target_company", store.target_company)
    payload.setdefault("snapshot_id", store.snapshot_id)
    payload.setdefault("asset_view", store.asset_view)
    return payload


def read_snapshot_publishable_email_lookup(store: SnapshotArtifactStore) -> dict[str, Any]:
    payload = read_json_dict(store.artifact_dir / "publishable_primary_emails.json")
    if not payload:
        return {}
    by_candidate_id = {
        str(candidate_id or "").strip(): dict(item)
        for candidate_id, item in dict(payload.get("by_candidate_id") or {}).items()
        if str(candidate_id or "").strip() and isinstance(item, dict)
    }
    by_profile_url_key = {
        str(profile_url_key or "").strip(): dict(item)
        for profile_url_key, item in dict(payload.get("by_profile_url_key") or {}).items()
        if str(profile_url_key or "").strip() and isinstance(item, dict)
    }
    return {
        **dict(payload),
        "by_candidate_id": by_candidate_id,
        "by_profile_url_key": by_profile_url_key,
    }
