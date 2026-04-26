from __future__ import annotations

import json
import os
import re
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from .artifact_cache import (
    apply_hot_cache_retention_plan,
    build_hot_cache_governance_policy,
    collect_hot_cache_inventory,
    mark_hot_cache_snapshot_access,
    materialize_link_first_file,
    plan_hot_cache_retention,
)
from .asset_logger import AssetLogger
from .asset_paths import (
    company_snapshot_group_key,
    hot_cache_snapshot_dir,
    iter_company_asset_snapshot_dirs,
    load_company_snapshot_identity,
    load_company_snapshot_json,
    resolve_company_snapshot_match_selection,
    score_company_snapshot_dir_match,
)
from .asset_registration import sync_company_asset_registration
from .candidate_materialization import (
    candidate_from_payload as _candidate_from_payload,
)
from .candidate_materialization import (
    candidate_level_function_ids as _candidate_level_function_ids,
)
from .candidate_materialization import (
    consolidate_materialized_duplicates as _consolidate_materialized_duplicates,
)
from .candidate_materialization import (
    evidence_key as _evidence_key,
)
from .candidate_materialization import (
    has_explicit_manual_review_resolution as _has_explicit_manual_review_resolution,
)
from .candidate_materialization import (
    ingest_materialized_candidate as _ingest_materialized_candidate,
)
from .candidate_materialization import (
    load_company_history_snapshots as _load_company_history_snapshots,
)
from .candidate_materialization import (
    normalize_key as _normalize_key,
)
from .candidate_materialization import (
    normalize_string_list as _normalize_string_list,
)
from .candidate_materialization import (
    remap_evidence_candidate as _remap_evidence_candidate,
)
from .candidate_materialization import (
    select_source_snapshots_for_materialization as _select_source_snapshots_for_materialization,
)
from .canonicalization import canonicalize_company_records
from .domain import (
    Candidate,
    EvidenceRecord,
    candidate_searchable_text,
    derive_candidate_facets,
    derive_candidate_role_bucket,
    make_evidence_id,
)
from .profile_timeline import (
    candidate_profile_lookup_url,
    normalized_primary_email_metadata,
    normalized_text_lines,
    primary_email_requires_suppression,
    profile_snapshot_from_source_path,
    resolve_candidate_profile_timeline,
    timeline_has_complete_profile_detail,
)
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .runtime_tuning import (
    resolved_candidate_artifact_parallelism,
    resolved_materialization_global_writer_budget,
    runtime_inflight_slot,
)
from .storage import ControlPlaneStore, _build_asset_membership_row


class CandidateArtifactError(RuntimeError):
    pass


_CANDIDATE_SHARD_PAGE_SIZE = 50
_DEFAULT_ARTIFACT_BUILD_PROFILE = "full"
_FOREGROUND_FAST_ARTIFACT_BUILD_PROFILE = "foreground_fast"
_DEFAULT_ARTIFACT_BUILD_PARALLEL_MIN_CANDIDATES = 24
_FOREGROUND_FAST_ARTIFACT_BUILD_PARALLEL_MIN_CANDIDATES = 16
_DEFAULT_ARTIFACT_BUILD_MAX_WORKERS = 10
_FOREGROUND_FAST_ARTIFACT_BUILD_MAX_WORKERS = 12
_CANDIDATE_ARTIFACT_FORCE_SERIAL_ENV = "SOURCING_CANDIDATE_ARTIFACT_FORCE_SERIAL"
_CANDIDATE_ARTIFACT_DISABLE_BATCH_JSON_WRITES_ENV = "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BATCH_JSON_WRITES"
_CANDIDATE_ARTIFACT_DISABLE_BULK_STATE_UPSERT_ENV = "SOURCING_CANDIDATE_ARTIFACT_DISABLE_BULK_STATE_UPSERT"


def _candidate_artifact_flag_enabled(env_name: str) -> bool:
    raw_value = str(os.getenv(env_name) or "").strip().lower()
    if not raw_value:
        return False
    return raw_value not in {"0", "false", "no", "off"}


def _candidate_artifact_elapsed_ms(started_at: float) -> float:
    return round((time.perf_counter() - started_at) * 1000, 2)


def _candidate_artifact_batch_json_writes_enabled() -> bool:
    return not _candidate_artifact_flag_enabled(_CANDIDATE_ARTIFACT_DISABLE_BATCH_JSON_WRITES_ENV)


def _candidate_artifact_bulk_state_upsert_enabled() -> bool:
    return not _candidate_artifact_flag_enabled(_CANDIDATE_ARTIFACT_DISABLE_BULK_STATE_UPSERT_ENV)


def compatibility_artifact_export_enabled() -> bool:
    raw_value = str(os.getenv("SOURCING_WRITE_COMPATIBILITY_ARTIFACTS") or "").strip().lower()
    if not raw_value:
        return False
    return raw_value not in {"0", "false", "no", "off"}


def _candidate_artifact_build_profile_options(build_profile: str) -> dict[str, Any]:
    normalized_profile = str(build_profile or _DEFAULT_ARTIFACT_BUILD_PROFILE).strip().lower()
    if normalized_profile == _FOREGROUND_FAST_ARTIFACT_BUILD_PROFILE:
        return {
            "name": normalized_profile,
            "write_compatibility_exports": False,
            "sync_hot_cache": False,
            "apply_hot_cache_retention": False,
        }
    return {
        "name": _DEFAULT_ARTIFACT_BUILD_PROFILE,
        "write_compatibility_exports": True,
        "sync_hot_cache": True,
        "apply_hot_cache_retention": True,
    }


def _default_candidate_artifact_parallel_limits(build_profile: str) -> tuple[int, int]:
    normalized_profile = str(build_profile or _DEFAULT_ARTIFACT_BUILD_PROFILE).strip().lower()
    cpu_budget = max(1, int(os.cpu_count() or 1))
    if normalized_profile == _FOREGROUND_FAST_ARTIFACT_BUILD_PROFILE:
        return (
            _FOREGROUND_FAST_ARTIFACT_BUILD_PARALLEL_MIN_CANDIDATES,
            min(_FOREGROUND_FAST_ARTIFACT_BUILD_MAX_WORKERS, max(1, cpu_budget)),
        )
    return (
        _DEFAULT_ARTIFACT_BUILD_PARALLEL_MIN_CANDIDATES,
        min(_DEFAULT_ARTIFACT_BUILD_MAX_WORKERS, max(1, cpu_budget)),
    )


def _candidate_artifact_parallel_workers(
    candidate_count: int,
    *,
    build_profile: str,
    runtime_tuning_overrides: dict[str, Any] | None = None,
) -> tuple[int, dict[str, int]]:
    normalized_candidate_count = max(0, int(candidate_count or 0))
    if normalized_candidate_count <= 1:
        tuning = resolved_candidate_artifact_parallelism(
            runtime_tuning_overrides,
            candidate_count=normalized_candidate_count,
            default_parallel_min_candidates=1,
            default_max_workers=1,
        )
        return 1, tuning
    if _candidate_artifact_flag_enabled(_CANDIDATE_ARTIFACT_FORCE_SERIAL_ENV):
        tuning = resolved_candidate_artifact_parallelism(
            runtime_tuning_overrides,
            candidate_count=normalized_candidate_count,
            default_parallel_min_candidates=1,
            default_max_workers=1,
        )
        tuning["parallel_workers"] = 1
        return 1, tuning
    default_min_candidates, default_max_workers = _default_candidate_artifact_parallel_limits(build_profile)
    try:
        env_min_candidates = int(
            os.getenv("SOURCING_CANDIDATE_ARTIFACT_PARALLEL_MIN_CANDIDATES") or default_min_candidates
        )
    except ValueError:
        env_min_candidates = default_min_candidates
    try:
        env_max_workers = int(os.getenv("SOURCING_CANDIDATE_ARTIFACT_MAX_WORKERS") or default_max_workers)
    except ValueError:
        env_max_workers = default_max_workers
    tuning = resolved_candidate_artifact_parallelism(
        runtime_tuning_overrides,
        candidate_count=normalized_candidate_count,
        default_parallel_min_candidates=max(1, env_min_candidates),
        default_max_workers=max(1, env_max_workers),
    )
    return max(1, int(tuning.get("parallel_workers") or 1)), tuning


def legacy_candidate_documents_fallback_enabled() -> bool:
    raw_value = str(os.getenv("SOURCING_ENABLE_LEGACY_CANDIDATE_DOCUMENTS_FALLBACK") or "").strip().lower()
    if not raw_value:
        return False
    return raw_value not in {"0", "false", "no", "off"}


def _resolved_candidate_documents_fallback(
    *,
    store: ControlPlaneStore | None,
    explicit: bool | None,
) -> bool:
    if explicit is not None:
        return bool(explicit)
    resolver = getattr(store, "candidate_documents_fallback_enabled", None)
    if callable(resolver):
        try:
            return bool(resolver())
        except Exception:
            return False
    return legacy_candidate_documents_fallback_enabled()


def compatibility_artifact_dir(snapshot_dir: Path, *, asset_view: str) -> Path:
    artifact_dir = snapshot_dir / "normalized_artifacts"
    if str(asset_view or "canonical_merged").strip() != "canonical_merged":
        artifact_dir = artifact_dir / (str(asset_view or "canonical_merged").strip() or "canonical_merged")
    return artifact_dir


def _compatibility_artifact_paths(artifact_dir: Path) -> dict[str, str]:
    return {
        "materialized_candidate_documents": str(artifact_dir / "materialized_candidate_documents.json"),
        "normalized_candidates": str(artifact_dir / "normalized_candidates.json"),
        "reusable_candidate_documents": str(artifact_dir / "reusable_candidate_documents.json"),
    }


def _load_existing_compatibility_view_payloads(artifact_dir: Path) -> dict[str, Any]:
    return {
        "materialized_candidate_documents": load_company_snapshot_json(
            artifact_dir / "materialized_candidate_documents.json"
        ),
        "normalized_candidates": list(load_company_snapshot_json(artifact_dir / "normalized_candidates.json") or []),
        "reusable_documents": list(
            load_company_snapshot_json(artifact_dir / "reusable_candidate_documents.json") or []
        ),
        "manual_review_backlog": list(load_company_snapshot_json(artifact_dir / "manual_review_backlog.json") or []),
        "profile_completion_backlog": list(
            load_company_snapshot_json(artifact_dir / "profile_completion_backlog.json") or []
        ),
    }


def _artifact_view_alias_source_asset_view(
    artifact_summary: dict[str, Any] | None,
    manifest_payload: dict[str, Any] | None,
) -> str:
    return str(
        dict(manifest_payload or {}).get("alias_asset_view") or dict(artifact_summary or {}).get("alias_asset_view") or ""
    ).strip()


def _resolve_serving_artifact_view_payload_source(
    *,
    snapshot_dir: Path,
    artifact_dir: Path,
    asset_view: str,
    artifact_summary: dict[str, Any] | None,
    manifest_payload: dict[str, Any] | None,
) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    alias_asset_view = _artifact_view_alias_source_asset_view(artifact_summary, manifest_payload)
    normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    if not alias_asset_view or alias_asset_view == normalized_asset_view:
        return (
            artifact_dir,
            dict(artifact_summary or {}),
            dict(manifest_payload or {}),
            _load_existing_compatibility_view_payloads(artifact_dir),
        )
    source_artifact_dir = compatibility_artifact_dir(snapshot_dir, asset_view=alias_asset_view)
    source_summary = dict(load_company_snapshot_json(source_artifact_dir / "artifact_summary.json") or {})
    source_manifest = dict(load_company_snapshot_json(source_artifact_dir / "manifest.json") or {})
    resolved_summary = {
        **source_summary,
        **dict(artifact_summary or {}),
        "asset_view": normalized_asset_view,
        "alias_asset_view": alias_asset_view,
        "storage_mode": "alias",
    }
    resolved_manifest = {
        **source_manifest,
        **dict(manifest_payload or {}),
        "asset_view": normalized_asset_view,
        "alias_asset_view": alias_asset_view,
        "storage_mode": "alias",
    }
    return (
        source_artifact_dir,
        resolved_summary,
        resolved_manifest,
        _load_existing_compatibility_view_payloads(source_artifact_dir),
    )


def materialize_company_candidate_view(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str = "",
    preferred_source_snapshot_ids: list[str] | None = None,
) -> dict[str, Any]:
    company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
        runtime_dir, target_company, snapshot_id=snapshot_id, prefer_hot_cache=False
    )
    company_name = str(identity_payload.get("canonical_name") or target_company).strip() or target_company

    merged_candidates: dict[str, Candidate] = {}
    merged_evidence: dict[str, dict[str, Any]] = {}
    candidate_aliases: dict[str, str] = {}
    identity_index: dict[str, str] = {}
    source_snapshots = _load_company_history_snapshots(snapshot_dir.parent, company_name)
    source_snapshots, source_snapshot_selection = _select_source_snapshots_for_materialization(
        source_snapshots,
        current_snapshot_id=snapshot_dir.name,
        preferred_source_snapshot_ids=preferred_source_snapshot_ids,
    )
    for source_snapshot in source_snapshots:
        for candidate in source_snapshot["candidates"]:
            _ingest_materialized_candidate(
                candidate,
                merged_candidates=merged_candidates,
                candidate_aliases=candidate_aliases,
                identity_index=identity_index,
                prefer_incoming=True,
            )
        for evidence in source_snapshot["evidence"]:
            remapped_evidence = _remap_evidence_candidate(evidence, candidate_aliases)
            merged_evidence[_evidence_key(remapped_evidence)] = remapped_evidence

    merged_candidates, merged_evidence = _consolidate_materialized_duplicates(merged_candidates, merged_evidence)
    canonical_candidates, canonical_evidence, canonicalization_summary = canonicalize_company_records(
        list(merged_candidates.values()),
        build_evidence_records_from_payloads(list(merged_evidence.values())),
    )

    materialized_candidates = canonical_candidates
    materialized_evidence = [item.to_record() for item in canonical_evidence]
    return {
        "target_company": company_name,
        "company_key": company_key,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": snapshot_dir,
        "company_identity": identity_payload,
        "candidates": materialized_candidates,
        "evidence": materialized_evidence,
        "source_snapshots": source_snapshots,
        "source_snapshot_selection": source_snapshot_selection,
        "control_plane_candidate_count": 0,
        "control_plane_evidence_count": 0,
        "canonicalization": canonicalization_summary,
    }


def build_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str = "",
    output_dir: str | Path | None = None,
    preferred_source_snapshot_ids: list[str] | None = None,
    build_profile: str = _DEFAULT_ARTIFACT_BUILD_PROFILE,
    runtime_tuning_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    build_options = _candidate_artifact_build_profile_options(build_profile)
    materialized_view = materialize_company_candidate_view(
        runtime_dir=runtime_dir,
        store=store,
        target_company=target_company,
        snapshot_id=snapshot_id,
        preferred_source_snapshot_ids=preferred_source_snapshot_ids,
    )
    snapshot_dir = Path(materialized_view["snapshot_dir"])
    artifact_dir = Path(output_dir) if output_dir else (snapshot_dir / "normalized_artifacts")
    artifact_dir.mkdir(parents=True, exist_ok=True)
    logger = AssetLogger(snapshot_dir)

    candidates = list(materialized_view["candidates"])
    evidence = list(materialized_view["evidence"])
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        evidence_by_candidate[str(item.get("candidate_id") or "").strip()].append(item)

    merged_view_result = _write_artifact_view(
        store=store,
        logger=logger,
        artifact_dir=artifact_dir,
        materialized_view=materialized_view,
        candidates=candidates,
        evidence=evidence,
        evidence_by_candidate=evidence_by_candidate,
        asset_view="canonical_merged",
        build_profile=build_profile,
        runtime_tuning_overrides=runtime_tuning_overrides,
    )
    strict_candidates = [
        candidate
        for candidate in candidates
        if _is_strict_roster_candidate(candidate, evidence_by_candidate.get(candidate.candidate_id, []))
    ]
    strict_candidate_ids = {candidate.candidate_id for candidate in strict_candidates}
    strict_evidence = [item for item in evidence if str(item.get("candidate_id") or "").strip() in strict_candidate_ids]
    strict_evidence_by_candidate = {
        candidate_id: items
        for candidate_id, items in evidence_by_candidate.items()
        if candidate_id in strict_candidate_ids
    }
    strict_matches_canonical = len(strict_candidates) == len(candidates) and len(strict_evidence) == len(evidence)
    strict_view_result = (
        _write_artifact_view_alias(
            store=store,
            logger=logger,
            artifact_dir=artifact_dir / "strict_roster_only",
            materialized_view=materialized_view,
            candidates=strict_candidates,
            source_view_result=merged_view_result,
            asset_view="strict_roster_only",
            alias_asset_view="canonical_merged",
        )
        if strict_matches_canonical
        else _write_artifact_view(
            store=store,
            logger=logger,
            artifact_dir=artifact_dir / "strict_roster_only",
            materialized_view=materialized_view,
            candidates=strict_candidates,
            evidence=strict_evidence,
            evidence_by_candidate=strict_evidence_by_candidate,
            asset_view="strict_roster_only",
            build_profile=build_profile,
            runtime_tuning_overrides=runtime_tuning_overrides,
        )
    )
    merged_view_result["summary"]["derived_views"] = {
        "strict_roster_only": {
            "candidate_count": strict_view_result["summary"]["candidate_count"],
            "evidence_count": strict_view_result["summary"]["evidence_count"],
            "manual_review_backlog_count": strict_view_result["summary"]["manual_review_backlog_count"],
            "profile_completion_backlog_count": strict_view_result["summary"]["profile_completion_backlog_count"],
            "artifact_dir": str(artifact_dir / "strict_roster_only"),
        }
    }
    strict_finalize = _finalize_artifact_view_materialization(
        store=store,
        logger=logger,
        artifact_dir=artifact_dir / "strict_roster_only",
        target_company=materialized_view["target_company"],
        snapshot_id=snapshot_dir.name,
        asset_view="strict_roster_only",
        candidates=strict_candidates,
        payloads=dict(strict_view_result.get("payloads") or {}),
        summary=dict(strict_view_result["summary"]),
        write_compatibility_exports=bool(build_options.get("write_compatibility_exports"))
        and not bool(strict_view_result["summary"].get("storage_mode") == "alias"),
    )
    strict_view_result["artifact_paths"].update(dict(strict_finalize.get("artifact_paths") or {}))
    strict_view_result["summary"] = dict(strict_view_result["summary"] or {})
    strict_view_result["summary"].update(
        {
            "materialization_generation_key": str(
                dict(strict_finalize.get("generation") or {}).get("generation_key") or ""
            ),
            "materialization_generation_sequence": int(
                dict(strict_finalize.get("generation") or {}).get("generation_sequence") or 0
            ),
            "materialization_watermark": str(
                dict(strict_finalize.get("generation") or {}).get("generation_watermark") or ""
            ),
            "membership_summary": dict(strict_finalize.get("membership_summary") or {}),
        }
    )
    strict_hot_cache_sync = (
        _sync_snapshot_artifact_view_to_hot_cache(
            runtime_dir=runtime_dir,
            snapshot_dir=snapshot_dir,
            company_key=str(materialized_view.get("company_key") or ""),
            target_company=materialized_view["target_company"],
            company_identity=dict(materialized_view.get("company_identity") or {}),
            asset_view="strict_roster_only",
        )
        if bool(build_options.get("sync_hot_cache"))
        else {
            "status": "skipped",
            "reason": "build_profile_deferred",
            "asset_view": "strict_roster_only",
            "snapshot_dir": "",
        }
    )
    merged_view_result["summary"]["derived_views"]["strict_roster_only"].update(
        {
            "materialization_generation_key": str(
                dict(strict_finalize.get("generation") or {}).get("generation_key") or ""
            ),
            "materialization_generation_sequence": int(
                dict(strict_finalize.get("generation") or {}).get("generation_sequence") or 0
            ),
            "materialization_watermark": str(
                dict(strict_finalize.get("generation") or {}).get("generation_watermark") or ""
            ),
            "membership_summary": dict(strict_finalize.get("membership_summary") or {}),
            "hot_cache_sync": strict_hot_cache_sync,
        }
    )
    merged_finalize = _finalize_artifact_view_materialization(
        store=store,
        logger=logger,
        artifact_dir=artifact_dir,
        target_company=materialized_view["target_company"],
        snapshot_id=snapshot_dir.name,
        asset_view="canonical_merged",
        candidates=candidates,
        payloads=dict(merged_view_result.get("payloads") or {}),
        summary=merged_view_result["summary"],
        write_compatibility_exports=bool(build_options.get("write_compatibility_exports")),
    )
    merged_view_result["artifact_paths"].update(dict(merged_finalize.get("artifact_paths") or {}))
    merged_hot_cache_sync = (
        _sync_snapshot_artifact_view_to_hot_cache(
            runtime_dir=runtime_dir,
            snapshot_dir=snapshot_dir,
            company_key=str(materialized_view.get("company_key") or ""),
            target_company=materialized_view["target_company"],
            company_identity=dict(materialized_view.get("company_identity") or {}),
            asset_view="canonical_merged",
        )
        if bool(build_options.get("sync_hot_cache"))
        else {
            "status": "skipped",
            "reason": "build_profile_deferred",
            "asset_view": "canonical_merged",
            "snapshot_dir": "",
        }
    )
    registration_sync = sync_company_asset_registration(
        runtime_dir=runtime_dir,
        store=store,
        target_company=materialized_view["target_company"],
        snapshot_id=snapshot_dir.name,
        asset_view="canonical_merged",
        company_key=str(materialized_view.get("company_key") or ""),
        registry_summary=merged_view_result["summary"],
        source_path=str(dict(merged_finalize.get("artifact_paths") or {}).get("artifact_summary") or ""),
        selected_snapshot_ids=list(
            dict(merged_view_result["summary"].get("source_snapshot_selection") or {}).get("selected_snapshot_ids")
            or []
        ),
        authoritative=True,
    )
    sync_status = dict(registration_sync.get("sync_status") or {})
    sync_status["hot_cache_retention"] = (
        _apply_configured_hot_cache_retention_policy(
            runtime_dir=runtime_dir,
            store=store,
        )
        if bool(build_options.get("apply_hot_cache_retention"))
        else {
            "status": "skipped",
            "reason": "build_profile_deferred",
        }
    )
    return {
        "status": "built",
        "target_company": materialized_view["target_company"],
        "snapshot_id": snapshot_dir.name,
        "artifact_dir": str(artifact_dir),
        "artifact_paths": merged_view_result["artifact_paths"],
        "summary": merged_view_result["summary"],
        "sync_status": sync_status,
        "hot_cache_sync": merged_hot_cache_sync,
        "build_profile": str(build_options.get("name") or _DEFAULT_ARTIFACT_BUILD_PROFILE),
        "views": {
            "canonical_merged": {
                "artifact_dir": str(artifact_dir),
                "artifact_paths": merged_view_result["artifact_paths"],
                "summary": merged_view_result["summary"],
                "hot_cache_sync": merged_hot_cache_sync,
            },
            "strict_roster_only": {
                "artifact_dir": str(artifact_dir / "strict_roster_only"),
                "artifact_paths": strict_view_result["artifact_paths"],
                "summary": strict_view_result["summary"],
                "hot_cache_sync": strict_hot_cache_sync,
            },
        },
    }


def _apply_configured_hot_cache_retention_policy(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
) -> dict[str, Any]:
    policy = build_hot_cache_governance_policy(runtime_dir=runtime_dir)
    ttl_seconds = int(policy.get("effective_ttl_seconds") or policy.get("ttl_seconds") or 0)
    size_budget_bytes = int(policy.get("effective_size_budget_bytes") or policy.get("size_budget_bytes") or 0)
    max_bytes_per_company = int(policy.get("max_bytes_per_company") or 0)
    max_generations_per_scope = int(policy.get("max_generations_per_scope") or 0)
    if ttl_seconds <= 0 and size_budget_bytes <= 0 and max_bytes_per_company <= 0 and max_generations_per_scope <= 0:
        return {
            "status": "skipped",
            "reason": str(policy.get("skip_reason") or "retention_policy_not_configured"),
            "ttl_seconds": ttl_seconds,
            "size_budget_bytes": int(policy.get("size_budget_bytes") or 0),
            "effective_size_budget_bytes": size_budget_bytes,
            "max_bytes_per_company": max_bytes_per_company,
            "keep_latest_snapshots_per_company": int(policy.get("keep_latest_snapshots_per_company") or 1),
            "max_generations_per_scope": max_generations_per_scope,
            "size_budget_source": str(policy.get("size_budget_source") or ""),
            "company_budget_source": str(policy.get("company_budget_source") or ""),
            "auto_retention_enabled": bool(policy.get("auto_retention_enabled")),
            "self_tuning_enabled": bool(policy.get("self_tuning_enabled")),
            "pressure_level": str(policy.get("pressure_level") or ""),
            "disk_free_bytes": int(policy.get("disk_free_bytes") or 0),
            "free_space_reserve_bytes": int(policy.get("free_space_reserve_bytes") or 0),
        }
    cleanup_summary = cleanup_candidate_artifact_hot_cache(
        runtime_dir=runtime_dir,
        store=store,
        dry_run=False,
        ttl_seconds=ttl_seconds,
        size_budget_bytes=size_budget_bytes,
        max_bytes_per_company=max_bytes_per_company,
        keep_latest_snapshots_per_company=int(policy.get("keep_latest_snapshots_per_company") or 1),
        max_generations_per_scope=max_generations_per_scope,
    )
    cleanup_summary["retention_policy"] = {
        "size_budget_bytes": int(policy.get("size_budget_bytes") or 0),
        "effective_size_budget_bytes": size_budget_bytes,
        "max_bytes_per_company": max_bytes_per_company,
        "size_budget_source": str(policy.get("size_budget_source") or ""),
        "company_budget_source": str(policy.get("company_budget_source") or ""),
        "auto_retention_enabled": bool(policy.get("auto_retention_enabled")),
        "self_tuning_enabled": bool(policy.get("self_tuning_enabled")),
        "pressure_level": str(policy.get("pressure_level") or ""),
        "max_generations_per_scope": max_generations_per_scope,
        "disk_free_bytes": int(policy.get("disk_free_bytes") or 0),
        "free_space_reserve_bytes": int(policy.get("free_space_reserve_bytes") or 0),
        "hot_cache_total_bytes": int(policy.get("hot_cache_total_bytes") or 0),
        "free_space_deficit_bytes": int(policy.get("free_space_deficit_bytes") or 0),
    }
    return cleanup_summary


def _iter_filtered_company_snapshot_groups(
    *,
    runtime_root: Path,
    requested_companies: list[str],
    requested_snapshot_id: str,
    prefer_hot_cache: bool = True,
) -> list[dict[str, Any]]:
    grouped_snapshots: dict[str, dict[str, Any]] = {}
    for candidate_snapshot_dir in iter_company_asset_snapshot_dirs(
        runtime_root,
        prefer_hot_cache=prefer_hot_cache,
        existing_only=True,
    ):
        company_dir = candidate_snapshot_dir.parent
        latest_payload = load_company_snapshot_json(company_dir / "latest_snapshot.json")
        preview_identity = load_company_snapshot_identity(
            candidate_snapshot_dir,
            fallback_payload=latest_payload,
        )
        if requested_companies and not any(
            score_company_snapshot_dir_match(company_dir, preview_identity, item) > 0 for item in requested_companies
        ):
            continue
        if requested_snapshot_id and candidate_snapshot_dir.name != requested_snapshot_id:
            continue
        company_key = company_snapshot_group_key(
            company_dir=company_dir,
            latest_payload=latest_payload,
            identity_payload=preview_identity,
        )
        company_group = grouped_snapshots.setdefault(
            company_key,
            {
                "company_dir_name": company_dir.name,
                "snapshots": [],
            },
        )
        company_group["snapshots"].append(
            {
                "snapshot_dir": candidate_snapshot_dir,
                "latest_payload": latest_payload,
            }
        )
    return list(grouped_snapshots.values())


def _iter_candidate_artifact_view_dirs(snapshot_dir: Path) -> list[tuple[str, Path]]:
    normalized_root = snapshot_dir / "normalized_artifacts"
    if not normalized_root.exists():
        return []
    view_dirs: list[tuple[str, Path]] = []
    if normalized_root.is_dir():
        view_dirs.append(("canonical_merged", normalized_root))
        for child in sorted(normalized_root.iterdir()):
            if not child.is_dir():
                continue
            if child.name in {"candidate_shards", "pages", "backlogs"}:
                continue
            view_dirs.append((child.name, child))
    return view_dirs


def _expected_hot_cache_paths_from_manifest(
    manifest_payload: dict[str, Any],
    *,
    keep_compatibility_exports: bool,
) -> set[str]:
    expected = {
        "manifest.json",
        "artifact_summary.json",
        "asset_registry.json",
        "snapshot_manifest.json",
        "manual_review_backlog.json",
        "profile_completion_backlog.json",
        "organization_completeness_ledger.json",
    }
    for entry in list(manifest_payload.get("candidate_shards") or []):
        path = str(dict(entry).get("path") or "").strip()
        if path:
            expected.add(path)
    for entry in list(manifest_payload.get("pages") or []):
        path = str(dict(entry).get("path") or "").strip()
        if path:
            expected.add(path)
    for key in ("manual_review", "profile_completion"):
        backlog_path = str(dict(manifest_payload.get("backlogs") or {}).get(key) or "").strip()
        if backlog_path:
            expected.add(backlog_path)
    for auxiliary_path in dict(manifest_payload.get("auxiliary") or {}).values():
        normalized_path = str(auxiliary_path or "").strip()
        if normalized_path:
            expected.add(normalized_path)
    if keep_compatibility_exports:
        expected.update(
            {
                "materialized_candidate_documents.json",
                "normalized_candidates.json",
                "reusable_candidate_documents.json",
            }
        )
    return expected


def _list_hot_cache_json_paths(artifact_dir: Path, *, asset_view: str) -> list[str]:
    if asset_view == "canonical_merged":
        discovered_paths: set[str] = set()
        for child in artifact_dir.iterdir():
            if child.is_file() and child.suffix == ".json":
                discovered_paths.add(child.name)
                continue
            if child.is_dir() and child.name in {"candidate_shards", "pages", "backlogs"}:
                discovered_paths.update(
                    str(path.relative_to(artifact_dir)) for path in child.rglob("*.json") if path.is_file()
                )
        return sorted(discovered_paths)
    return sorted({str(path.relative_to(artifact_dir)) for path in artifact_dir.rglob("*.json") if path.is_file()})


def cleanup_candidate_artifact_hot_cache(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore | None = None,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    dry_run: bool = True,
    drop_compatibility_exports: bool = True,
    ttl_seconds: int = 0,
    size_budget_bytes: int = 0,
    max_bytes_per_company: int = 0,
    keep_latest_snapshots_per_company: int = 1,
    max_generations_per_scope: int = 0,
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir)
    requested_companies = [_normalize_key(item) for item in list(companies or []) if str(item or "").strip()]
    requested_snapshot_id = str(snapshot_id or "").strip()
    snapshot_groups = _iter_filtered_company_snapshot_groups(
        runtime_root=runtime_root,
        requested_companies=requested_companies,
        requested_snapshot_id=requested_snapshot_id,
        prefer_hot_cache=True,
    )
    if not snapshot_groups:
        return {
            "status": "missing_runtime_company_assets",
            "runtime_dir": str(runtime_root),
            "dry_run": dry_run,
            "drop_compatibility_exports": drop_compatibility_exports,
            "scanned_snapshot_count": 0,
            "scanned_view_count": 0,
            "orphan_file_count": 0,
            "deleted_file_count": 0,
            "pruned_state_count": 0,
            "retention": {
                "status": "skipped_missing_runtime_company_assets",
                "ttl_seconds": max(0, int(ttl_seconds or 0)),
                "size_budget_bytes": max(0, int(size_budget_bytes or 0)),
                "max_bytes_per_company": max(0, int(max_bytes_per_company or 0)),
                "keep_latest_snapshots_per_company": max(1, int(keep_latest_snapshots_per_company or 1)),
                "max_generations_per_scope": max(0, int(max_generations_per_scope or 0)),
            },
            "companies": [],
        }

    scanned_snapshot_count = 0
    scanned_view_count = 0
    orphan_file_count = 0
    deleted_file_count = 0
    pruned_state_count = 0
    company_summaries: list[dict[str, Any]] = []

    for company_group in snapshot_groups:
        company_summary: dict[str, Any] = {
            "company_dir": str(company_group.get("company_dir_name") or ""),
            "snapshots": [],
        }
        for snapshot_entry in list(company_group.get("snapshots") or []):
            scanned_snapshot_count += 1
            snapshot_dir = Path(snapshot_entry["snapshot_dir"])
            identity_payload = load_company_snapshot_identity(
                snapshot_dir,
                fallback_payload=dict(snapshot_entry.get("latest_payload") or {}),
            )
            target_company = (
                str(identity_payload.get("canonical_name") or "").strip()
                or str(identity_payload.get("requested_name") or "").strip()
                or str(identity_payload.get("company_key") or "").strip()
                or snapshot_dir.parent.name
            )
            snapshot_summary: dict[str, Any] = {
                "snapshot_id": snapshot_dir.name,
                "views": [],
            }
            for asset_view, artifact_dir in _iter_candidate_artifact_view_dirs(snapshot_dir):
                manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
                if not manifest_payload:
                    continue
                scanned_view_count += 1
                keep_compatibility_exports = not drop_compatibility_exports
                expected_paths = _expected_hot_cache_paths_from_manifest(
                    manifest_payload,
                    keep_compatibility_exports=keep_compatibility_exports,
                )
                actual_paths = _list_hot_cache_json_paths(artifact_dir, asset_view=asset_view)
                orphan_paths = [path for path in actual_paths if path not in expected_paths]
                orphan_file_count += len(orphan_paths)
                deleted_paths: list[str] = []
                if orphan_paths and not dry_run:
                    for relative_path in orphan_paths:
                        artifact_path = artifact_dir / relative_path
                        artifact_path.unlink(missing_ok=True)
                        deleted_paths.append(relative_path)
                    for nested_dir in sorted(
                        [path for path in artifact_dir.rglob("*") if path.is_dir()],
                        reverse=True,
                    ):
                        try:
                            nested_dir.rmdir()
                        except OSError:
                            continue
                    deleted_file_count += len(deleted_paths)
                active_candidate_ids = [
                    str(dict(entry).get("candidate_id") or "").strip()
                    for entry in list(manifest_payload.get("candidate_shards") or [])
                    if str(dict(entry).get("candidate_id") or "").strip()
                ]
                pruned_states: list[dict[str, Any]] = []
                if store is not None:
                    if dry_run:
                        existing_states = store.list_candidate_materialization_states(
                            target_company=target_company,
                            snapshot_id=snapshot_dir.name,
                            asset_view=asset_view,
                        )
                        active_ids = set(active_candidate_ids)
                        pruned_states = [
                            item
                            for item in existing_states
                            if str(item.get("candidate_id") or "").strip() not in active_ids
                        ]
                    else:
                        pruned_states = store.prune_candidate_materialization_states(
                            target_company=target_company,
                            snapshot_id=snapshot_dir.name,
                            asset_view=asset_view,
                            active_candidate_ids=active_candidate_ids,
                        )
                    pruned_state_count += len(pruned_states)
                snapshot_summary["views"].append(
                    {
                        "asset_view": asset_view,
                        "artifact_dir": str(artifact_dir),
                        "orphan_paths": orphan_paths,
                        "deleted_paths": deleted_paths,
                        "pruned_state_candidate_ids": [
                            str(item.get("candidate_id") or "").strip() for item in pruned_states
                        ],
                    }
                )
            if snapshot_summary["views"]:
                company_summary["snapshots"].append(snapshot_summary)
        if company_summary["snapshots"]:
            company_summaries.append(company_summary)

    retention_summary: dict[str, Any] = {
        "status": "skipped",
        "ttl_seconds": max(0, int(ttl_seconds or 0)),
        "size_budget_bytes": max(0, int(size_budget_bytes or 0)),
        "max_bytes_per_company": max(0, int(max_bytes_per_company or 0)),
        "keep_latest_snapshots_per_company": max(1, int(keep_latest_snapshots_per_company or 1)),
        "max_generations_per_scope": max(0, int(max_generations_per_scope or 0)),
    }
    if (
        int(ttl_seconds or 0) > 0
        or int(size_budget_bytes or 0) > 0
        or int(max_bytes_per_company or 0) > 0
        or int(max_generations_per_scope or 0) > 0
    ):
        inventory = collect_hot_cache_inventory(runtime_root)
        scoped_snapshot_records = [
            dict(item)
            for item in list(inventory.get("snapshot_records") or [])
            if isinstance(item, dict)
            and (
                not requested_companies
                or _normalize_key(str(item.get("company_key") or item.get("target_company") or ""))
                in requested_companies
            )
            and (not requested_snapshot_id or str(item.get("snapshot_id") or "").strip() == requested_snapshot_id)
        ]
        scoped_generation_keys = {
            str(generation_key).strip()
            for item in scoped_snapshot_records
            for generation_key in list(item.get("generation_keys") or [])
            if str(generation_key).strip()
        }
        scoped_generation_records = [
            dict(item)
            for item in list(inventory.get("generation_records") or [])
            if isinstance(item, dict)
            and (
                not requested_companies
                or _normalize_key(str(item.get("company_key") or item.get("target_company") or ""))
                in requested_companies
                or str(item.get("generation_key") or "").strip() in scoped_generation_keys
            )
            and (
                not requested_snapshot_id
                or str(item.get("snapshot_id") or "").strip() == requested_snapshot_id
                or str(item.get("generation_key") or "").strip() in scoped_generation_keys
            )
        ]
        scoped_inventory = {
            **inventory,
            "snapshot_records": scoped_snapshot_records,
            "generation_records": scoped_generation_records,
            "total_snapshot_bytes": sum(int(item.get("total_bytes") or 0) for item in scoped_snapshot_records),
            "total_generation_bytes": sum(int(item.get("total_bytes") or 0) for item in scoped_generation_records),
        }
        scoped_inventory["total_bytes"] = int(scoped_inventory["total_snapshot_bytes"]) + int(
            scoped_inventory["total_generation_bytes"]
        )
        retention_plan = plan_hot_cache_retention(
            runtime_root,
            ttl_seconds=ttl_seconds,
            size_budget_bytes=size_budget_bytes,
            max_bytes_per_company=max_bytes_per_company,
            keep_latest_snapshots_per_company=keep_latest_snapshots_per_company,
            max_generations_per_scope=max_generations_per_scope,
            inventory=scoped_inventory,
        )
        retention_apply = apply_hot_cache_retention_plan(retention_plan, dry_run=dry_run)
        retention_summary = {
            **retention_apply,
            "ttl_seconds": max(0, int(ttl_seconds or 0)),
            "size_budget_bytes": max(0, int(size_budget_bytes or 0)),
            "max_bytes_per_company": max(0, int(max_bytes_per_company or 0)),
            "keep_latest_snapshots_per_company": max(1, int(keep_latest_snapshots_per_company or 1)),
            "max_generations_per_scope": max(0, int(max_generations_per_scope or 0)),
            "planned_snapshot_eviction_count": int(retention_plan.get("planned_snapshot_eviction_count") or 0),
            "planned_generation_eviction_count": int(retention_plan.get("planned_generation_eviction_count") or 0),
            "estimated_retained_total_bytes": int(retention_plan.get("estimated_retained_total_bytes") or 0),
        }
        if store is not None:
            evicted_snapshot_records = [
                dict(item)
                for item in list(retention_plan.get("eviction_candidates") or [])
                if isinstance(item, dict) and str(item.get("kind") or "") == "snapshot"
            ]
            evicted_state_candidate_ids: list[str] = []
            for snapshot_record in evicted_snapshot_records:
                target_company = str(snapshot_record.get("target_company") or "").strip()
                snapshot_name = str(snapshot_record.get("snapshot_id") or "").strip()
                for asset_view in list(snapshot_record.get("asset_views") or []):
                    if not target_company or not snapshot_name or not str(asset_view or "").strip():
                        continue
                    if dry_run:
                        pruned_states = store.list_candidate_materialization_states(
                            target_company=target_company,
                            snapshot_id=snapshot_name,
                            asset_view=str(asset_view),
                        )
                    else:
                        pruned_states = store.prune_candidate_materialization_states(
                            target_company=target_company,
                            snapshot_id=snapshot_name,
                            asset_view=str(asset_view),
                            active_candidate_ids=[],
                        )
                    pruned_state_count += len(pruned_states)
                    evicted_state_candidate_ids.extend(
                        str(item.get("candidate_id") or "").strip()
                        for item in pruned_states
                        if str(item.get("candidate_id") or "").strip()
                    )
            retention_summary["pruned_state_candidate_ids"] = sorted(set(evicted_state_candidate_ids))

    return {
        "status": "completed",
        "runtime_dir": str(runtime_root),
        "dry_run": dry_run,
        "drop_compatibility_exports": drop_compatibility_exports,
        "ttl_seconds": max(0, int(ttl_seconds or 0)),
        "size_budget_bytes": max(0, int(size_budget_bytes or 0)),
        "max_bytes_per_company": max(0, int(max_bytes_per_company or 0)),
        "keep_latest_snapshots_per_company": max(1, int(keep_latest_snapshots_per_company or 1)),
        "scanned_snapshot_count": scanned_snapshot_count,
        "scanned_view_count": scanned_view_count,
        "orphan_file_count": orphan_file_count,
        "deleted_file_count": deleted_file_count,
        "pruned_state_count": pruned_state_count,
        "retention": retention_summary,
        "companies": company_summaries,
    }


def repair_missing_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    force_rebuild_artifacts: bool = False,
) -> dict[str, Any]:
    from .asset_reuse_planning import backfill_organization_asset_registry_for_company
    from .organization_assets import ensure_organization_completeness_ledger

    runtime_root = Path(runtime_dir)
    requested_companies = [_normalize_key(item) for item in list(companies or []) if str(item or "").strip()]
    requested_snapshot_id = str(snapshot_id or "").strip()
    snapshot_groups = _iter_filtered_company_snapshot_groups(
        runtime_root=runtime_root,
        requested_companies=requested_companies,
        requested_snapshot_id=requested_snapshot_id,
        prefer_hot_cache=False,
    )
    if not snapshot_groups:
        return {
            "status": "missing_runtime_company_assets",
            "runtime_dir": str(runtime_root),
            "scanned_snapshot_count": 0,
            "repair_candidate_count": 0,
            "repaired_snapshot_count": 0,
            "skipped_existing_count": 0,
            "missing_candidate_documents_count": 0,
            "registry_refresh_count": 0,
            "errors": [],
            "companies": [],
        }

    scanned_snapshot_count = 0
    repair_candidate_count = 0
    repaired_snapshot_count = 0
    force_rebuilt_snapshot_count = 0
    ledger_refresh_only_count = 0
    skipped_existing_count = 0
    missing_candidate_documents_count = 0
    registry_refresh_count = 0
    errors: list[dict[str, Any]] = []
    company_summaries: list[dict[str, Any]] = []

    for company_group in snapshot_groups:
        company_dir_name = str(company_group.get("company_dir_name") or "")
        company_summary: dict[str, Any] = {
            "company_dir": company_dir_name,
            "repaired_snapshots": [],
            "ledger_refreshed_snapshots": [],
            "skipped_existing_snapshots": [],
            "missing_candidate_document_snapshots": [],
            "registry_refresh": {},
        }
        for snapshot_entry in list(company_group.get("snapshots") or []):
            candidate_snapshot_dir = Path(snapshot_entry["snapshot_dir"])
            latest_payload = dict(snapshot_entry.get("latest_payload") or {})
            scanned_snapshot_count += 1
            candidate_doc_path = candidate_snapshot_dir / "candidate_documents.json"
            normalized_dir = candidate_snapshot_dir / "normalized_artifacts"
            manifest_path = normalized_dir / "manifest.json"
            artifact_summary_path = normalized_dir / "artifact_summary.json"
            ledger_path = normalized_dir / "organization_completeness_ledger.json"
            if not candidate_doc_path.exists():
                missing_candidate_documents_count += 1
                company_summary["missing_candidate_document_snapshots"].append(candidate_snapshot_dir.name)
                continue
            needs_artifact_repair = (
                bool(force_rebuild_artifacts) or not manifest_path.exists() or not artifact_summary_path.exists()
            )
            needs_ledger_refresh = not ledger_path.exists()
            if not needs_artifact_repair and not needs_ledger_refresh:
                skipped_existing_count += 1
                company_summary["skipped_existing_snapshots"].append(candidate_snapshot_dir.name)
                continue
            repair_candidate_count += 1
            try:
                identity_payload = load_company_snapshot_identity(
                    candidate_snapshot_dir,
                    fallback_payload=latest_payload,
                )
                target_company = (
                    str(identity_payload.get("canonical_name") or "").strip()
                    or str(identity_payload.get("requested_name") or "").strip()
                    or str(identity_payload.get("company_key") or "").strip()
                    or company_dir_name
                )
                registry_target_company = (
                    str(identity_payload.get("company_key") or "").strip() or company_dir_name or target_company
                )
                artifact_result: dict[str, Any] = {}
                if needs_artifact_repair:
                    artifact_result = build_company_candidate_artifacts(
                        runtime_dir=runtime_root,
                        store=store,
                        target_company=target_company,
                        snapshot_id=candidate_snapshot_dir.name,
                    )
                ledger_result = ensure_organization_completeness_ledger(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=registry_target_company,
                    snapshot_id=candidate_snapshot_dir.name,
                    asset_view="canonical_merged",
                )
                snapshot_summary = {
                    "snapshot_id": candidate_snapshot_dir.name,
                    "target_company": str(artifact_result.get("target_company") or target_company),
                    "artifact_dir": str(artifact_result.get("artifact_dir") or normalized_dir),
                    "artifact_paths": dict(artifact_result.get("artifact_paths") or {}),
                    "candidate_count": int(dict(artifact_result.get("summary") or {}).get("candidate_count") or 0),
                    "ledger_path": str(ledger_result.get("ledger_path") or ""),
                    "repair_mode": (
                        "force_rebuild"
                        if needs_artifact_repair and force_rebuild_artifacts
                        else ("repair_missing_artifacts" if needs_artifact_repair else "refresh_ledger")
                    ),
                }
                if needs_artifact_repair:
                    if force_rebuild_artifacts:
                        force_rebuilt_snapshot_count += 1
                    else:
                        repaired_snapshot_count += 1
                    company_summary["repaired_snapshots"].append(snapshot_summary)
                else:
                    ledger_refresh_only_count += 1
                    company_summary["ledger_refreshed_snapshots"].append(snapshot_summary)
            except Exception as exc:
                errors.append(
                    {
                        "company_dir": company_dir_name,
                        "snapshot_id": candidate_snapshot_dir.name,
                        "error": str(exc),
                    }
                )
        touched_snapshots = list(company_summary.get("repaired_snapshots") or []) + list(
            company_summary.get("ledger_refreshed_snapshots") or []
        )
        if touched_snapshots:
            try:
                refresh_result = backfill_organization_asset_registry_for_company(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=company_dir_name,
                    asset_view="canonical_merged",
                )
                registry_refresh_count += 1
            except Exception as exc:
                refresh_result = {
                    "status": "failed",
                    "error": str(exc),
                }
                errors.append(
                    {
                        "company_dir": company_dir_name,
                        "snapshot_id": "",
                        "error": f"organization registry refresh failed: {exc}",
                    }
                )
            company_summary["registry_refresh"] = refresh_result
        if (
            company_summary["repaired_snapshots"]
            or company_summary["ledger_refreshed_snapshots"]
            or company_summary["skipped_existing_snapshots"]
            or company_summary["missing_candidate_document_snapshots"]
        ):
            company_summaries.append(company_summary)

    return {
        "status": "completed_with_errors" if errors else "completed",
        "runtime_dir": str(runtime_root),
        "company_filters": [str(item or "").strip() for item in list(companies or []) if str(item or "").strip()],
        "snapshot_filter": requested_snapshot_id,
        "scanned_snapshot_count": scanned_snapshot_count,
        "repair_candidate_count": repair_candidate_count,
        "repaired_snapshot_count": repaired_snapshot_count,
        "force_rebuilt_snapshot_count": force_rebuilt_snapshot_count,
        "ledger_refresh_only_count": ledger_refresh_only_count,
        "skipped_existing_count": skipped_existing_count,
        "missing_candidate_documents_count": missing_candidate_documents_count,
        "registry_refresh_count": registry_refresh_count,
        "errors": errors,
        "companies": company_summaries,
    }


def backfill_structured_timeline_for_company_assets(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    backfill_profile_registry: bool = True,
    profile_resume: bool = True,
    profile_progress_interval: int = 200,
    refresh_registry: bool = True,
) -> dict[str, Any]:
    from .profile_registry_backfill import backfill_linkedin_profile_registry

    runtime_root = Path(runtime_dir)
    requested_companies = [str(item or "").strip() for item in list(companies or []) if str(item or "").strip()]
    requested_snapshot_id = str(snapshot_id or "").strip()

    profile_registry_results: list[dict[str, Any]] = []
    if backfill_profile_registry:
        company_scopes = requested_companies or [""]
        for company in company_scopes:
            profile_registry_results.append(
                backfill_linkedin_profile_registry(
                    runtime_dir=runtime_root,
                    store=store,
                    company=company,
                    snapshot_id=requested_snapshot_id,
                    resume=profile_resume,
                    progress_interval=max(1, int(profile_progress_interval or 1)),
                )
            )

    artifact_backfill_result = rewrite_structured_timeline_in_company_candidate_artifacts(
        runtime_dir=runtime_root,
        store=store,
        companies=requested_companies or None,
        snapshot_id=requested_snapshot_id,
        refresh_registry=refresh_registry,
    )

    status = str(artifact_backfill_result.get("status") or "completed")
    if any(str(item.get("status") or "") != "completed" for item in profile_registry_results):
        status = "completed_with_errors"
    return {
        "status": status,
        "runtime_dir": str(runtime_root),
        "company_filters": requested_companies,
        "snapshot_filter": requested_snapshot_id,
        "profile_registry_backfill": {
            "enabled": backfill_profile_registry,
            "company_run_count": len(profile_registry_results),
            "files_processed_this_run": sum(
                int(item.get("files_processed_this_run") or 0) for item in profile_registry_results
            ),
            "results": profile_registry_results,
        },
        "artifact_backfill": artifact_backfill_result,
    }


def repair_projected_profile_signals_in_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    companies: list[str] | None = None,
    snapshot_id: str = "",
) -> dict[str, Any]:
    runtime_root = Path(runtime_dir)
    requested_companies = [_normalize_key(item) for item in list(companies or []) if str(item or "").strip()]
    requested_snapshot_id = str(snapshot_id or "").strip()
    snapshot_groups = _iter_filtered_company_snapshot_groups(
        runtime_root=runtime_root,
        requested_companies=requested_companies,
        requested_snapshot_id=requested_snapshot_id,
        prefer_hot_cache=False,
    )
    if not snapshot_groups:
        return {
            "status": "missing_runtime_company_assets",
            "runtime_dir": str(runtime_root),
            "scanned_snapshot_count": 0,
            "updated_snapshot_count": 0,
            "updated_view_count": 0,
            "errors": [],
            "companies": [],
        }

    scanned_snapshot_count = 0
    updated_snapshot_count = 0
    updated_view_count = 0
    errors: list[dict[str, Any]] = []
    company_summaries: list[dict[str, Any]] = []

    for company_group in snapshot_groups:
        company_dir_name = str(company_group.get("company_dir_name") or "")
        company_summary: dict[str, Any] = {
            "company_dir": company_dir_name,
            "updated_snapshots": [],
        }
        for snapshot_entry in list(company_group.get("snapshots") or []):
            candidate_snapshot_dir = Path(snapshot_entry["snapshot_dir"])
            scanned_snapshot_count += 1
            normalized_dir = candidate_snapshot_dir / "normalized_artifacts"
            if not normalized_dir.exists():
                continue
            try:
                updated_views: list[dict[str, Any]] = []
                for asset_view, artifact_dir in (
                    ("canonical_merged", normalized_dir),
                    ("strict_roster_only", normalized_dir / "strict_roster_only"),
                ):
                    repair_result = _repair_projected_profile_signal_view(artifact_dir)
                    if not repair_result["updated"]:
                        continue
                    updated_view_count += 1
                    updated_views.append(
                        {
                            "asset_view": asset_view,
                            "materialized_candidate_count": int(repair_result["materialized_candidate_count"] or 0),
                            "materialized_updated_count": int(repair_result["materialized_updated_count"] or 0),
                            "normalized_updated_count": int(repair_result["normalized_updated_count"] or 0),
                            "reusable_updated_count": int(repair_result["reusable_updated_count"] or 0),
                        }
                    )
                if not updated_views:
                    continue
                updated_snapshot_count += 1
                company_summary["updated_snapshots"].append(
                    {
                        "snapshot_id": candidate_snapshot_dir.name,
                        "views": updated_views,
                    }
                )
            except Exception as exc:
                errors.append(
                    {
                        "company_dir": company_dir_name,
                        "snapshot_id": candidate_snapshot_dir.name,
                        "error": str(exc),
                    }
                )
        if company_summary["updated_snapshots"]:
            company_summaries.append(company_summary)

    return {
        "status": "completed_with_errors" if errors else "completed",
        "runtime_dir": str(runtime_root),
        "company_filters": [str(item or "").strip() for item in list(companies or []) if str(item or "").strip()],
        "snapshot_filter": requested_snapshot_id,
        "scanned_snapshot_count": scanned_snapshot_count,
        "updated_snapshot_count": updated_snapshot_count,
        "updated_view_count": updated_view_count,
        "errors": errors,
        "companies": company_summaries,
    }


def rewrite_structured_timeline_in_company_candidate_artifacts(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    companies: list[str] | None = None,
    snapshot_id: str = "",
    refresh_registry: bool = True,
) -> dict[str, Any]:
    from .asset_reuse_planning import backfill_organization_asset_registry_for_company

    runtime_root = Path(runtime_dir)
    requested_companies = [_normalize_key(item) for item in list(companies or []) if str(item or "").strip()]
    requested_snapshot_id = str(snapshot_id or "").strip()
    snapshot_groups = _iter_filtered_company_snapshot_groups(
        runtime_root=runtime_root,
        requested_companies=requested_companies,
        requested_snapshot_id=requested_snapshot_id,
        prefer_hot_cache=False,
    )
    if not snapshot_groups:
        return {
            "status": "missing_runtime_company_assets",
            "runtime_dir": str(runtime_root),
            "scanned_snapshot_count": 0,
            "rewritten_snapshot_count": 0,
            "rewritten_view_count": 0,
            "rebuilt_missing_snapshot_count": 0,
            "missing_candidate_documents_count": 0,
            "registry_refresh_count": 0,
            "errors": [],
            "companies": [],
        }

    scanned_snapshot_count = 0
    rewritten_snapshot_count = 0
    rewritten_view_count = 0
    rebuilt_missing_snapshot_count = 0
    missing_candidate_documents_count = 0
    registry_refresh_count = 0
    errors: list[dict[str, Any]] = []
    company_summaries: list[dict[str, Any]] = []

    for company_group in snapshot_groups:
        company_dir_name = str(company_group.get("company_dir_name") or "")
        company_summary: dict[str, Any] = {
            "company_dir": company_dir_name,
            "rewritten_snapshots": [],
            "rebuilt_missing_snapshots": [],
            "missing_candidate_document_snapshots": [],
            "registry_refresh": {},
        }
        touched_company = False
        for snapshot_entry in list(company_group.get("snapshots") or []):
            candidate_snapshot_dir = Path(snapshot_entry["snapshot_dir"])
            latest_payload = dict(snapshot_entry.get("latest_payload") or {})
            scanned_snapshot_count += 1
            candidate_doc_path = candidate_snapshot_dir / "candidate_documents.json"
            if not candidate_doc_path.exists():
                missing_candidate_documents_count += 1
                company_summary["missing_candidate_document_snapshots"].append(candidate_snapshot_dir.name)
                continue
            try:
                identity_payload = load_company_snapshot_identity(
                    candidate_snapshot_dir,
                    fallback_payload=latest_payload,
                )
                target_company = (
                    str(identity_payload.get("canonical_name") or "").strip()
                    or str(identity_payload.get("requested_name") or "").strip()
                    or str(identity_payload.get("company_key") or "").strip()
                    or company_dir_name
                )
                normalized_dir = candidate_snapshot_dir / "normalized_artifacts"
                canonical_manifest_path = normalized_dir / "manifest.json"
                canonical_summary_path = normalized_dir / "artifact_summary.json"
                strict_manifest_path = normalized_dir / "strict_roster_only" / "manifest.json"
                strict_summary_path = normalized_dir / "strict_roster_only" / "artifact_summary.json"
                can_direct_rewrite = canonical_manifest_path.exists() and canonical_summary_path.exists()
                strict_exists = strict_manifest_path.exists() and strict_summary_path.exists()
                strict_incomplete = strict_manifest_path.exists() != strict_summary_path.exists()
                if not can_direct_rewrite or strict_incomplete:
                    artifact_result = _bootstrap_candidate_artifacts_from_candidate_documents(
                        runtime_dir=runtime_root,
                        store=store,
                        target_company=target_company,
                        snapshot_id=candidate_snapshot_dir.name,
                    )
                    rebuilt_missing_snapshot_count += 1
                    touched_company = True
                    rebuilt_summary = {
                        "snapshot_id": candidate_snapshot_dir.name,
                        "target_company": str(artifact_result.get("target_company") or target_company),
                        "artifact_dir": str(artifact_result.get("artifact_dir") or normalized_dir),
                        "artifact_paths": dict(artifact_result.get("artifact_paths") or {}),
                        "candidate_count": int(dict(artifact_result.get("summary") or {}).get("candidate_count") or 0),
                        "structured_timeline_count": int(
                            dict(artifact_result.get("summary") or {}).get("structured_timeline_count") or 0
                        ),
                        "rewrite_mode": "candidate_documents_bootstrap",
                    }
                    company_summary["rebuilt_missing_snapshots"].append(rebuilt_summary)
                    continue

                rewritten_views: list[dict[str, Any]] = []
                canonical_result = _rewrite_existing_candidate_artifact_view(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=target_company,
                    snapshot_id=candidate_snapshot_dir.name,
                    asset_view="canonical_merged",
                )
                rewritten_views.append(
                    {
                        "asset_view": "canonical_merged",
                        "artifact_dir": str(canonical_result.get("artifact_dir") or normalized_dir),
                        "candidate_count": int(dict(canonical_result.get("summary") or {}).get("candidate_count") or 0),
                        "structured_timeline_count": int(
                            dict(canonical_result.get("summary") or {}).get("structured_timeline_count") or 0
                        ),
                        "structured_experience_count": int(
                            dict(canonical_result.get("summary") or {}).get("structured_experience_count") or 0
                        ),
                        "structured_education_count": int(
                            dict(canonical_result.get("summary") or {}).get("structured_education_count") or 0
                        ),
                    }
                )
                if strict_exists:
                    strict_result = _rewrite_existing_candidate_artifact_view(
                        runtime_dir=runtime_root,
                        store=store,
                        target_company=target_company,
                        snapshot_id=candidate_snapshot_dir.name,
                        asset_view="strict_roster_only",
                    )
                    rewritten_views.append(
                        {
                            "asset_view": "strict_roster_only",
                            "artifact_dir": str(
                                strict_result.get("artifact_dir") or (normalized_dir / "strict_roster_only")
                            ),
                            "candidate_count": int(
                                dict(strict_result.get("summary") or {}).get("candidate_count") or 0
                            ),
                            "structured_timeline_count": int(
                                dict(strict_result.get("summary") or {}).get("structured_timeline_count") or 0
                            ),
                            "structured_experience_count": int(
                                dict(strict_result.get("summary") or {}).get("structured_experience_count") or 0
                            ),
                            "structured_education_count": int(
                                dict(strict_result.get("summary") or {}).get("structured_education_count") or 0
                            ),
                        }
                    )
                rewritten_snapshot_count += 1
                rewritten_view_count += len(rewritten_views)
                touched_company = True
                company_summary["rewritten_snapshots"].append(
                    {
                        "snapshot_id": candidate_snapshot_dir.name,
                        "target_company": target_company,
                        "rewrite_mode": "existing_artifact_rewrite",
                        "views": rewritten_views,
                    }
                )
            except Exception as exc:
                errors.append(
                    {
                        "company_dir": company_dir_name,
                        "snapshot_id": candidate_snapshot_dir.name,
                        "error": str(exc),
                    }
                )

        if touched_company and refresh_registry:
            try:
                refresh_result = backfill_organization_asset_registry_for_company(
                    runtime_dir=runtime_root,
                    store=store,
                    target_company=company_dir_name,
                    asset_view="canonical_merged",
                )
                registry_refresh_count += 1
            except Exception as exc:
                refresh_result = {
                    "status": "failed",
                    "error": str(exc),
                }
                errors.append(
                    {
                        "company_dir": company_dir_name,
                        "snapshot_id": "",
                        "error": f"organization registry refresh failed: {exc}",
                    }
                )
            company_summary["registry_refresh"] = refresh_result
        elif touched_company:
            company_summary["registry_refresh"] = {
                "status": "skipped",
                "reason": "disabled",
            }

        if (
            company_summary["rewritten_snapshots"]
            or company_summary["rebuilt_missing_snapshots"]
            or company_summary["missing_candidate_document_snapshots"]
        ):
            company_summaries.append(company_summary)

    return {
        "status": "completed_with_errors" if errors else "completed",
        "runtime_dir": str(runtime_root),
        "company_filters": [str(item or "").strip() for item in list(companies or []) if str(item or "").strip()],
        "snapshot_filter": requested_snapshot_id,
        "scanned_snapshot_count": scanned_snapshot_count,
        "rewritten_snapshot_count": rewritten_snapshot_count,
        "rewritten_view_count": rewritten_view_count,
        "rebuilt_missing_snapshot_count": rebuilt_missing_snapshot_count,
        "missing_candidate_documents_count": missing_candidate_documents_count,
        "registry_refresh_count": registry_refresh_count,
        "errors": errors,
        "companies": company_summaries,
    }


def load_snapshot_candidate_artifact_payload(
    *,
    snapshot_dir: Path,
    target_company: str,
    asset_view: str = "canonical_merged",
    company_key: str = "",
    company_identity: dict[str, Any] | None = None,
    allow_candidate_documents_fallback: bool | None = None,
) -> dict[str, Any]:
    resolved_snapshot_dir = Path(snapshot_dir).expanduser()
    normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    resolved_allow_candidate_documents_fallback = (
        legacy_candidate_documents_fallback_enabled()
        if allow_candidate_documents_fallback is None
        else bool(allow_candidate_documents_fallback)
    )
    resolved_company_key = (
        str(company_key or resolved_snapshot_dir.parent.name).strip() or resolved_snapshot_dir.parent.name
    )
    identity_payload = dict(company_identity or {})
    company_name = (
        str(identity_payload.get("canonical_name") or "").strip()
        or str(identity_payload.get("requested_name") or "").strip()
        or str(target_company or "").strip()
    )
    artifact_dir = compatibility_artifact_dir(resolved_snapshot_dir, asset_view=normalized_asset_view)
    artifact_summary = load_company_snapshot_json(artifact_dir / "artifact_summary.json")
    manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
    effective_artifact_dir, artifact_summary, manifest_payload, compatibility_payloads = (
        _resolve_serving_artifact_view_payload_source(
            snapshot_dir=resolved_snapshot_dir,
            artifact_dir=artifact_dir,
            asset_view=normalized_asset_view,
            artifact_summary=artifact_summary,
            manifest_payload=manifest_payload,
        )
    )

    manifest_path = artifact_dir / "manifest.json"
    if manifest_payload:
        payload = _reconstruct_compatibility_payloads_from_serving_artifacts(
            snapshot_dir=resolved_snapshot_dir,
            artifact_dir=effective_artifact_dir,
            target_company=company_name or target_company,
            company_key=resolved_company_key,
            company_identity=identity_payload,
            asset_view=normalized_asset_view,
            artifact_summary=artifact_summary,
        )
        if payload:
            return {
                "status": "loaded",
                "target_company": company_name or target_company,
                "company_key": resolved_company_key,
                "snapshot_id": resolved_snapshot_dir.name,
                "snapshot_dir": str(resolved_snapshot_dir),
                "company_identity": identity_payload,
                "asset_view": normalized_asset_view,
                "artifact_summary": artifact_summary,
                "source_kind": (
                    "materialized_candidate_documents_manifest"
                    if normalized_asset_view == "canonical_merged"
                    else f"materialized_candidate_documents_manifest:{normalized_asset_view}"
                ),
                "source_path": str(manifest_path),
                "source_payload": payload,
                "manifest_payload": manifest_payload,
                "normalized_candidates": list(payload.get("normalized_candidates") or []),
                "reusable_documents": list(payload.get("reusable_documents") or []),
                "manual_review_backlog": list(payload.get("manual_review_backlog") or []),
                "profile_completion_backlog": list(payload.get("profile_completion_backlog") or []),
            }

    materialized_path = effective_artifact_dir / "materialized_candidate_documents.json"
    if materialized_path.exists():
        payload = load_company_snapshot_json(materialized_path)
        if payload:
            return {
                "status": "loaded",
                "target_company": company_name or target_company,
                "company_key": resolved_company_key,
                "snapshot_id": resolved_snapshot_dir.name,
                "snapshot_dir": str(resolved_snapshot_dir),
                "company_identity": identity_payload,
                "asset_view": normalized_asset_view,
                "artifact_summary": artifact_summary,
                "source_kind": (
                    "materialized_candidate_documents"
                    if normalized_asset_view == "canonical_merged"
                    else f"materialized_candidate_documents:{normalized_asset_view}"
                ),
                "source_path": str(materialized_path),
                "source_payload": payload,
                "manifest_payload": manifest_payload,
                "normalized_candidates": list(compatibility_payloads.get("normalized_candidates") or []),
                "reusable_documents": list(compatibility_payloads.get("reusable_documents") or []),
                "manual_review_backlog": list(compatibility_payloads.get("manual_review_backlog") or []),
                "profile_completion_backlog": list(compatibility_payloads.get("profile_completion_backlog") or []),
            }

    if resolved_allow_candidate_documents_fallback and normalized_asset_view == "canonical_merged":
        candidate_doc_path = resolved_snapshot_dir / "candidate_documents.json"
        if candidate_doc_path.exists():
            payload = load_company_snapshot_json(candidate_doc_path)
            if payload:
                return {
                    "status": "loaded",
                    "target_company": company_name or target_company,
                    "company_key": resolved_company_key,
                    "snapshot_id": resolved_snapshot_dir.name,
                    "snapshot_dir": str(resolved_snapshot_dir),
                    "company_identity": identity_payload,
                    "asset_view": normalized_asset_view,
                    "artifact_summary": artifact_summary,
                    "source_kind": "candidate_documents",
                    "source_path": str(candidate_doc_path),
                    "source_payload": payload,
                    "manifest_payload": manifest_payload,
                    "normalized_candidates": [],
                    "reusable_documents": [],
                    "manual_review_backlog": [],
                    "profile_completion_backlog": [],
                }

    raise CandidateArtifactError(f"No candidate artifact payload found under {resolved_snapshot_dir}")


def _repair_projected_profile_signal_view(artifact_dir: Path) -> dict[str, Any]:
    materialized_path = artifact_dir / "materialized_candidate_documents.json"
    if not materialized_path.exists() and (artifact_dir / "manifest.json").exists():
        _export_compatibility_artifacts_from_serving_view(artifact_dir)
    if not materialized_path.exists():
        return {
            "updated": False,
            "materialized_candidate_count": 0,
            "materialized_updated_count": 0,
            "normalized_updated_count": 0,
            "reusable_updated_count": 0,
        }

    materialized_payload = load_company_snapshot_json(materialized_path)
    candidates = [
        dict(item or {}) for item in list(materialized_payload.get("candidates") or []) if isinstance(item, dict)
    ]
    if not candidates:
        return {
            "updated": False,
            "materialized_candidate_count": 0,
            "materialized_updated_count": 0,
            "normalized_updated_count": 0,
            "reusable_updated_count": 0,
        }

    materialized_updated_count = 0
    projected_candidates: list[dict[str, Any]] = []
    signal_by_candidate_id: dict[str, dict[str, Any]] = {}
    timeline_cache: dict[str, dict[str, Any]] = {}
    for item in candidates:
        projected = _project_profile_signal_fields_into_record(item)
        projected = _repair_projected_profile_signal_record_from_local_timeline(
            projected,
            timeline_cache=timeline_cache,
        )
        if projected != item:
            materialized_updated_count += 1
        projected_candidates.append(projected)
        candidate_id = str(projected.get("candidate_id") or "").strip()
        if candidate_id:
            signal_by_candidate_id[candidate_id] = _extract_projectable_profile_signals(projected)

    normalized_updated_count = _repair_projected_profile_signal_projection_list(
        artifact_dir / "normalized_candidates.json",
        signal_by_candidate_id,
    )
    reusable_updated_count = _repair_projected_profile_signal_projection_list(
        artifact_dir / "reusable_candidate_documents.json",
        signal_by_candidate_id,
    )

    if materialized_updated_count:
        materialized_payload["candidates"] = projected_candidates
        materialized_path.write_text(json.dumps(materialized_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "updated": bool(materialized_updated_count or normalized_updated_count or reusable_updated_count),
        "materialized_candidate_count": len(projected_candidates),
        "materialized_updated_count": materialized_updated_count,
        "normalized_updated_count": normalized_updated_count,
        "reusable_updated_count": reusable_updated_count,
    }


def _repair_projected_profile_signal_record_from_local_timeline(
    record: dict[str, Any],
    *,
    timeline_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    projected = dict(record or {})
    if not projected:
        return projected
    if (
        normalized_primary_email_metadata(projected.get("primary_email_metadata"))
        or not str(projected.get("primary_email") or "").strip()
    ):
        return projected
    metadata = dict(projected.get("metadata") or {})
    if _repair_projected_profile_signal_record_from_profile_source_paths(
        projected,
        metadata=metadata,
        timeline_cache=timeline_cache,
    ):
        projected["metadata"] = metadata
        return projected
    timeline = resolve_candidate_profile_timeline(
        payload=projected,
        metadata=metadata,
        source_path=str(projected.get("source_path") or "").strip(),
        timeline_cache=timeline_cache,
    )
    if not normalized_primary_email_metadata(timeline.get("primary_email_metadata")):
        return projected
    if str(timeline.get("primary_email") or "").strip() != str(projected.get("primary_email") or "").strip():
        return projected
    _merge_resolved_profile_signals_into_metadata(metadata, timeline)
    _merge_resolved_profile_signals_into_record(projected, timeline)
    projected["metadata"] = metadata
    return projected


def _repair_projected_profile_signal_record_from_profile_source_paths(
    record: dict[str, Any],
    *,
    metadata: dict[str, Any],
    timeline_cache: dict[str, dict[str, Any]] | None = None,
) -> bool:
    primary_email = str(record.get("primary_email") or "").strip()
    if not primary_email:
        return False
    for source_path in _candidate_profile_signal_source_paths(record, metadata):
        profile_snapshot = profile_snapshot_from_source_path(source_path, cache=timeline_cache)
        snapshot_primary_email = str(profile_snapshot.get("primary_email") or "").strip()
        snapshot_email_metadata = normalized_primary_email_metadata(profile_snapshot.get("primary_email_metadata"))
        if not snapshot_email_metadata or snapshot_primary_email != primary_email:
            continue
        metadata["primary_email_metadata"] = snapshot_email_metadata
        record["primary_email_metadata"] = snapshot_email_metadata
        return True
    return False


def _candidate_profile_signal_source_paths(record: dict[str, Any], metadata: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    for value in (
        record.get("profile_timeline_source_path"),
        metadata.get("profile_timeline_source_path"),
        record.get("profile_capture_source_path"),
        metadata.get("profile_capture_source_path"),
        record.get("source_path"),
        metadata.get("source_path"),
    ):
        normalized = str(value or "").strip()
        if normalized and normalized not in paths:
            paths.append(normalized)
    return paths


def _repair_projected_profile_signal_projection_list(
    payload_path: Path,
    signal_by_candidate_id: dict[str, dict[str, Any]],
) -> int:
    if not payload_path.exists():
        return 0
    try:
        payload = json.loads(payload_path.read_text())
    except (OSError, json.JSONDecodeError):
        return 0
    if not isinstance(payload, list):
        return 0
    updated_count = 0
    projected_payload: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            projected_payload.append(item)
            continue
        candidate_id = str(item.get("candidate_id") or "").strip()
        signal_payload = dict(signal_by_candidate_id.get(candidate_id) or {})
        projected = _project_profile_signal_fields_into_record(item, signal_payload=signal_payload)
        if projected != item:
            updated_count += 1
        projected_payload.append(projected)
    if updated_count:
        payload_path.write_text(json.dumps(projected_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return updated_count


def _rewrite_existing_candidate_artifact_view(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
) -> dict[str, Any]:
    loaded = load_company_snapshot_candidate_documents(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        view=asset_view,
        prefer_hot_cache=False,
    )
    if not str(loaded.get("source_kind") or "").startswith("materialized_candidate_documents"):
        raise CandidateArtifactError(
            f"Structured timeline rewrite requires materialized candidate artifacts for {target_company}:{snapshot_id}:{asset_view}"
        )

    source_payload = dict(loaded.get("source_payload") or {})
    snapshot_metadata = dict(source_payload.get("snapshot") or {})
    snapshot_dir = Path(str(loaded.get("snapshot_dir") or "")).expanduser()
    logger = AssetLogger(snapshot_dir)
    candidates = list(loaded.get("candidates") or [])
    evidence = list(loaded.get("evidence") or [])
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        evidence_by_candidate[str(item.get("candidate_id") or "").strip()].append(item)

    materialized_view = {
        "company_key": str(
            snapshot_metadata.get("company_key") or loaded.get("company_key") or _normalize_key(target_company)
        ),
        "snapshot_dir": snapshot_dir,
        "target_company": str(loaded.get("target_company") or target_company),
        "company_identity": dict(loaded.get("company_identity") or {}),
        "source_snapshots": [
            {
                "snapshot_id": str(item.get("snapshot_id") or "").strip(),
                "candidate_count": int(item.get("candidate_count") or 0),
                "evidence_count": int(item.get("evidence_count") or 0),
                "source_path": str(item.get("source_path") or "").strip(),
            }
            for item in list(snapshot_metadata.get("source_snapshots") or [])
            if isinstance(item, dict)
        ],
        "source_snapshot_selection": dict(
            snapshot_metadata.get("source_snapshot_selection")
            or dict(loaded.get("artifact_summary") or {}).get("source_snapshot_selection")
            or {}
        ),
        "control_plane_candidate_count": int(
            snapshot_metadata.get("control_plane_candidate_count")
            or snapshot_metadata.get("sqlite_candidate_count")
            or dict(loaded.get("artifact_summary") or {}).get("control_plane_candidate_count")
            or dict(loaded.get("artifact_summary") or {}).get("sqlite_candidate_count")
            or 0
        ),
        "control_plane_evidence_count": int(
            snapshot_metadata.get("control_plane_evidence_count")
            or snapshot_metadata.get("sqlite_evidence_count")
            or dict(loaded.get("artifact_summary") or {}).get("control_plane_evidence_count")
            or dict(loaded.get("artifact_summary") or {}).get("sqlite_evidence_count")
            or 0
        ),
    }
    artifact_dir = snapshot_dir / "normalized_artifacts"
    if asset_view != "canonical_merged":
        artifact_dir = artifact_dir / asset_view
    return _persist_candidate_artifact_view(
        runtime_dir=runtime_dir,
        store=store,
        logger=logger,
        artifact_dir=artifact_dir,
        materialized_view=materialized_view,
        candidates=candidates,
        evidence=evidence,
        evidence_by_candidate=evidence_by_candidate,
        asset_view=asset_view,
        target_company=str(loaded.get("target_company") or target_company),
        snapshot_id=str(loaded.get("snapshot_id") or snapshot_id),
        status="rewritten",
    )


def _bootstrap_candidate_artifacts_from_candidate_documents(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
) -> dict[str, Any]:
    loaded = load_company_snapshot_candidate_documents(
        runtime_dir=runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        view="canonical_merged",
        prefer_hot_cache=False,
        allow_candidate_documents_fallback=True,
    )
    if str(loaded.get("source_kind") or "") != "candidate_documents":
        raise CandidateArtifactError(
            f"Candidate-documents bootstrap expected root candidate_documents payload for {target_company}:{snapshot_id}"
        )
    snapshot_dir = Path(str(loaded.get("snapshot_dir") or "")).expanduser()
    logger = AssetLogger(snapshot_dir)
    candidates = list(loaded.get("candidates") or [])
    evidence = list(loaded.get("evidence") or [])
    evidence_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in evidence:
        evidence_by_candidate[str(item.get("candidate_id") or "").strip()].append(item)

    materialized_view = {
        "company_key": str(loaded.get("company_key") or _normalize_key(target_company)),
        "snapshot_dir": snapshot_dir,
        "target_company": str(loaded.get("target_company") or target_company),
        "company_identity": dict(loaded.get("company_identity") or {}),
        "source_snapshots": [
            {
                "snapshot_id": str(loaded.get("snapshot_id") or snapshot_id),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "source_path": str(loaded.get("source_path") or ""),
            }
        ],
        "source_snapshot_selection": {
            "mode": "current_snapshot_candidate_documents_bootstrap",
            "reason": (
                "Structured timeline backfill bootstrapped normalized artifacts from the current snapshot "
                "candidate_documents payload."
            ),
            "selected_snapshot_ids": [str(loaded.get("snapshot_id") or snapshot_id)],
            "excluded_snapshot_ids": [],
        },
        "control_plane_candidate_count": 0,
        "control_plane_evidence_count": 0,
    }
    canonical_result = _persist_candidate_artifact_view(
        runtime_dir=runtime_dir,
        store=store,
        logger=logger,
        artifact_dir=snapshot_dir / "normalized_artifacts",
        materialized_view=materialized_view,
        candidates=candidates,
        evidence=evidence,
        evidence_by_candidate=evidence_by_candidate,
        asset_view="canonical_merged",
        target_company=str(loaded.get("target_company") or target_company),
        snapshot_id=str(loaded.get("snapshot_id") or snapshot_id),
        status="built_from_candidate_documents",
    )
    strict_candidates = [
        candidate
        for candidate in candidates
        if _is_strict_roster_candidate(candidate, evidence_by_candidate.get(candidate.candidate_id, []))
    ]
    strict_candidate_ids = {candidate.candidate_id for candidate in strict_candidates}
    strict_evidence = [item for item in evidence if str(item.get("candidate_id") or "").strip() in strict_candidate_ids]
    strict_evidence_by_candidate = {
        candidate_id: items
        for candidate_id, items in evidence_by_candidate.items()
        if candidate_id in strict_candidate_ids
    }
    strict_result = _persist_candidate_artifact_view(
        runtime_dir=runtime_dir,
        store=store,
        logger=logger,
        artifact_dir=snapshot_dir / "normalized_artifacts" / "strict_roster_only",
        materialized_view=materialized_view,
        candidates=strict_candidates,
        evidence=strict_evidence,
        evidence_by_candidate=strict_evidence_by_candidate,
        asset_view="strict_roster_only",
        target_company=str(loaded.get("target_company") or target_company),
        snapshot_id=str(loaded.get("snapshot_id") or snapshot_id),
        status="built_from_candidate_documents",
    )
    canonical_result["views"] = {
        "canonical_merged": canonical_result,
        "strict_roster_only": strict_result,
    }
    return canonical_result


def _persist_candidate_artifact_view(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    logger: AssetLogger,
    artifact_dir: Path,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    evidence: list[dict[str, Any]],
    evidence_by_candidate: dict[str, list[dict[str, Any]]],
    asset_view: str,
    target_company: str,
    snapshot_id: str,
    status: str,
    build_profile: str = _DEFAULT_ARTIFACT_BUILD_PROFILE,
    runtime_tuning_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidates = _backfill_candidate_provider_function_ids(
        snapshot_dir=Path(materialized_view["snapshot_dir"]),
        materialized_view=materialized_view,
        candidates=candidates,
    )
    view_result = _write_artifact_view(
        store=store,
        logger=logger,
        artifact_dir=artifact_dir,
        materialized_view=materialized_view,
        candidates=candidates,
        evidence=evidence,
        evidence_by_candidate=evidence_by_candidate,
        asset_view=asset_view,
        build_profile=build_profile,
        runtime_tuning_overrides=runtime_tuning_overrides,
    )
    finalize = _finalize_artifact_view_materialization(
        store=store,
        logger=logger,
        artifact_dir=artifact_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        candidates=candidates,
        payloads=dict(view_result.get("payloads") or {}),
        summary=dict(view_result.get("summary") or {}),
    )
    view_result["artifact_paths"].update(dict(finalize.get("artifact_paths") or {}))
    view_result["summary"] = dict(view_result.get("summary") or {})
    view_result["summary"].update(
        {
            "materialization_generation_key": str(dict(finalize.get("generation") or {}).get("generation_key") or ""),
            "materialization_generation_sequence": int(
                dict(finalize.get("generation") or {}).get("generation_sequence") or 0
            ),
            "materialization_watermark": str(dict(finalize.get("generation") or {}).get("generation_watermark") or ""),
            "membership_summary": dict(finalize.get("membership_summary") or {}),
        }
    )

    sync_status: dict[str, Any] = {}
    if asset_view == "canonical_merged":
        registration_sync = sync_company_asset_registration(
            runtime_dir=runtime_dir,
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            company_key=str(materialized_view.get("company_key") or ""),
            registry_summary=view_result["summary"],
            source_path=str(dict(finalize.get("artifact_paths") or {}).get("artifact_summary") or ""),
            selected_snapshot_ids=list(
                dict(view_result["summary"].get("source_snapshot_selection") or {}).get("selected_snapshot_ids") or []
            ),
            authoritative=True,
        )
        sync_status = dict(registration_sync.get("sync_status") or {})
    hot_cache_sync = _sync_snapshot_artifact_view_to_hot_cache(
        runtime_dir=runtime_dir,
        snapshot_dir=Path(materialized_view["snapshot_dir"]),
        company_key=str(materialized_view.get("company_key") or ""),
        target_company=target_company,
        company_identity=dict(materialized_view.get("company_identity") or {}),
        asset_view=asset_view,
    )

    return {
        "status": status,
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "artifact_dir": str(artifact_dir),
        "artifact_paths": dict(view_result.get("artifact_paths") or {}),
        "summary": dict(view_result.get("summary") or {}),
        "sync_status": sync_status,
        "hot_cache_sync": hot_cache_sync,
    }


def _sync_snapshot_artifact_view_to_hot_cache(
    *,
    runtime_dir: str | Path,
    snapshot_dir: Path,
    company_key: str,
    target_company: str,
    company_identity: dict[str, Any],
    asset_view: str,
) -> dict[str, Any]:
    normalized_company_key = str(company_key or snapshot_dir.parent.name).strip() or snapshot_dir.parent.name
    hot_snapshot = hot_cache_snapshot_dir(runtime_dir, normalized_company_key, snapshot_dir.name)
    if hot_snapshot is None:
        return {"status": "disabled", "snapshot_dir": "", "asset_view": asset_view}
    source_artifact_dir = compatibility_artifact_dir(snapshot_dir, asset_view=asset_view)
    if not source_artifact_dir.exists():
        return {
            "status": "missing_source_artifacts",
            "snapshot_dir": str(hot_snapshot),
            "asset_view": asset_view,
            "source_artifact_dir": str(source_artifact_dir),
        }
    hot_snapshot.mkdir(parents=True, exist_ok=True)
    hot_company_dir = hot_snapshot.parent
    hot_company_dir.mkdir(parents=True, exist_ok=True)
    hot_artifact_dir = compatibility_artifact_dir(hot_snapshot, asset_view=asset_view)
    view_sync = _sync_serving_artifact_view_to_hot_cache(
        source_artifact_dir=source_artifact_dir,
        hot_artifact_dir=hot_artifact_dir,
    )
    root_mode_counts: dict[str, int] = {}
    for filename in ("identity.json", "retrieval_index_summary.json"):
        source_path = snapshot_dir / filename
        if not source_path.exists() and not source_path.is_symlink():
            continue
        sync_mode = materialize_link_first_file(source_path, hot_snapshot / filename)
        root_mode_counts[sync_mode] = int(root_mode_counts.get(sync_mode) or 0) + 1
    AssetLogger(hot_company_dir).write_json(
        hot_company_dir / "latest_snapshot.json",
        {
            "company_identity": dict(company_identity or {}),
            "snapshot_id": snapshot_dir.name,
            "snapshot_dir": str(hot_snapshot),
        },
        asset_type="latest_snapshot_pointer",
        source_kind="candidate_artifact_hot_cache_sync",
        is_raw_asset=False,
        model_safe=True,
    )
    return {
        "status": str(view_sync.get("status") or "completed"),
        "snapshot_dir": str(hot_snapshot),
        "asset_view": asset_view,
        "artifact_dir": str(hot_artifact_dir),
        "file_count": int(view_sync.get("file_count") or 0),
        "directory_count": int(view_sync.get("directory_count") or 0),
        "mode_counts": dict(view_sync.get("mode_counts") or {}),
        "root_mode_counts": root_mode_counts,
        "target_company": target_company,
    }


def _sync_serving_artifact_view_to_hot_cache(
    *,
    source_artifact_dir: Path,
    hot_artifact_dir: Path,
) -> dict[str, Any]:
    manifest_payload = load_company_snapshot_json(source_artifact_dir / "manifest.json")
    if not manifest_payload:
        return {
            "status": "missing_source_manifest",
            "source_artifact_dir": str(source_artifact_dir),
            "destination_artifact_dir": str(hot_artifact_dir),
            "file_count": 0,
            "directory_count": 0,
            "mode_counts": {},
            "copied_paths": [],
            "deleted_paths": [],
            "missing_paths": [],
        }
    expected_paths = _expected_hot_cache_paths_from_manifest(
        manifest_payload,
        keep_compatibility_exports=False,
    )
    hot_artifact_dir.mkdir(parents=True, exist_ok=True)
    copied_paths: list[str] = []
    missing_paths: list[str] = []
    mode_counts: dict[str, int] = {}
    for relative_path in sorted(expected_paths):
        source_path = source_artifact_dir / relative_path
        if not source_path.exists() and not source_path.is_symlink():
            missing_paths.append(relative_path)
            continue
        sync_mode = materialize_link_first_file(source_path, hot_artifact_dir / relative_path)
        copied_paths.append(relative_path)
        mode_counts[sync_mode] = int(mode_counts.get(sync_mode) or 0) + 1
    stale_paths = [
        relative_path
        for relative_path in _list_hot_cache_json_paths(hot_artifact_dir, asset_view="canonical_merged")
        if relative_path not in expected_paths
    ]
    for relative_path in stale_paths:
        (hot_artifact_dir / relative_path).unlink(missing_ok=True)
    for nested_dir in sorted([path for path in hot_artifact_dir.rglob("*") if path.is_dir()], reverse=True):
        try:
            nested_dir.rmdir()
        except OSError:
            continue
    return {
        "status": "completed",
        "source_artifact_dir": str(source_artifact_dir),
        "destination_artifact_dir": str(hot_artifact_dir),
        "file_count": len(copied_paths),
        "directory_count": len([path for path in hot_artifact_dir.rglob("*") if path.is_dir()]),
        "mode_counts": mode_counts,
        "copied_paths": copied_paths,
        "deleted_paths": stale_paths,
        "missing_paths": missing_paths,
    }


def load_company_snapshot_candidate_documents(
    *,
    runtime_dir: str | Path,
    target_company: str,
    snapshot_id: str = "",
    view: str = "canonical_merged",
    prefer_hot_cache: bool = True,
    allow_candidate_documents_fallback: bool | None = None,
) -> dict[str, Any]:
    company_key, snapshot_dir, identity_payload = _resolve_company_snapshot(
        runtime_dir,
        target_company,
        snapshot_id=snapshot_id,
        prefer_hot_cache=prefer_hot_cache,
    )
    mark_hot_cache_snapshot_access(snapshot_dir, runtime_dir=runtime_dir)
    asset_view = str(view or "canonical_merged").strip() or "canonical_merged"
    loaded_payload = load_snapshot_candidate_artifact_payload(
        snapshot_dir=snapshot_dir,
        target_company=target_company,
        asset_view=asset_view,
        company_key=company_key,
        company_identity=identity_payload,
        allow_candidate_documents_fallback=allow_candidate_documents_fallback,
    )
    company_name = str(loaded_payload.get("target_company") or target_company).strip() or target_company
    payload = dict(loaded_payload.get("source_payload") or {})
    artifact_summary = dict(loaded_payload.get("artifact_summary") or {})

    target_keys = {_normalize_key(target_company), _normalize_key(company_name), _normalize_key(company_key)}
    candidates: list[Candidate] = []
    candidate_ids: set[str] = set()
    for item in list(payload.get("candidates") or []):
        candidate = _candidate_from_payload(item)
        if candidate is None:
            continue
        if _normalize_key(candidate.target_company) not in target_keys:
            continue
        candidates.append(candidate)
        candidate_ids.add(candidate.candidate_id)

    evidence_records = build_evidence_records_from_payloads(list(payload.get("evidence") or []))
    evidence = [
        item.to_record() for item in evidence_records if not candidate_ids or item.candidate_id in candidate_ids
    ]

    return {
        "status": "loaded",
        "target_company": company_name,
        "company_key": company_key,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "company_identity": identity_payload,
        "asset_view": asset_view,
        "source_kind": str(loaded_payload.get("source_kind") or ""),
        "source_path": str(loaded_payload.get("source_path") or ""),
        "source_payload": payload,
        "artifact_summary": artifact_summary,
        "manifest_payload": dict(loaded_payload.get("manifest_payload") or {}),
        "normalized_candidates": list(loaded_payload.get("normalized_candidates") or []),
        "reusable_documents": list(loaded_payload.get("reusable_documents") or []),
        "manual_review_backlog": list(loaded_payload.get("manual_review_backlog") or []),
        "profile_completion_backlog": list(loaded_payload.get("profile_completion_backlog") or []),
        "candidates": candidates,
        "evidence": evidence,
    }


def load_authoritative_company_snapshot_candidate_documents(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore | None,
    target_company: str,
    snapshot_id: str = "",
    view: str = "canonical_merged",
    prefer_hot_cache: bool = True,
    allow_materialization_fallback: bool = True,
    allow_candidate_documents_fallback: bool | None = None,
) -> dict[str, Any]:
    normalized_target_company = str(target_company or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_view = str(view or "canonical_merged").strip() or "canonical_merged"
    if not normalized_target_company:
        raise CandidateArtifactError("target_company is required")

    resolved_allow_candidate_documents_fallback = _resolved_candidate_documents_fallback(
        store=store,
        explicit=allow_candidate_documents_fallback,
    )
    try:
        snapshot_payload = load_company_snapshot_candidate_documents(
            runtime_dir=runtime_dir,
            target_company=normalized_target_company,
            snapshot_id=normalized_snapshot_id,
            view=normalized_view,
            prefer_hot_cache=prefer_hot_cache,
            allow_candidate_documents_fallback=False,
        )
    except CandidateArtifactError:
        if not resolved_allow_candidate_documents_fallback:
            raise
        snapshot_payload = load_company_snapshot_candidate_documents(
            runtime_dir=runtime_dir,
            target_company=normalized_target_company,
            snapshot_id=normalized_snapshot_id,
            view=normalized_view,
            prefer_hot_cache=prefer_hot_cache,
            allow_candidate_documents_fallback=True,
        )
    if (
        not allow_materialization_fallback
        or store is None
        or str(snapshot_payload.get("source_kind") or "").strip().lower() != "candidate_documents"
    ):
        return snapshot_payload
    try:
        build_company_candidate_artifacts(
            runtime_dir=runtime_dir,
            store=store,
            target_company=normalized_target_company,
            snapshot_id=normalized_snapshot_id,
        )
    except Exception:
        return snapshot_payload
    try:
        return load_company_snapshot_candidate_documents(
            runtime_dir=runtime_dir,
            target_company=normalized_target_company,
            snapshot_id=normalized_snapshot_id,
            view=normalized_view,
            prefer_hot_cache=prefer_hot_cache,
            allow_candidate_documents_fallback=False,
        )
    except CandidateArtifactError:
        return snapshot_payload


def build_evidence_records_from_payloads(payloads: list[dict[str, Any]]) -> list[EvidenceRecord]:
    records: list[EvidenceRecord] = []
    for item in payloads:
        candidate_id = str(item.get("candidate_id") or "").strip()
        source_type = str(item.get("source_type") or "").strip()
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        source_dataset = str(item.get("source_dataset") or "").strip()
        source_path = str(item.get("source_path") or "").strip()
        if not candidate_id or not source_type:
            continue
        evidence_id = str(item.get("evidence_id") or "").strip() or make_evidence_id(
            candidate_id,
            source_dataset or source_type,
            title,
            url or source_path,
        )
        records.append(
            EvidenceRecord(
                evidence_id=evidence_id,
                candidate_id=candidate_id,
                source_type=source_type,
                title=title,
                url=url,
                summary=str(item.get("summary") or "").strip(),
                source_dataset=source_dataset,
                source_path=source_path,
                metadata=dict(item.get("metadata") or {}),
            )
        )
    return records


def _materialized_payload_function_ids(payload: dict[str, Any]) -> list[str]:
    metadata = dict(payload.get("metadata") or {})
    return _candidate_level_function_ids(
        payload.get("function_ids"),
        metadata.get("function_ids"),
        source_shard_filters=metadata.get("source_shard_filters"),
    )


def _candidate_provider_function_id_snapshot_ids(
    *,
    snapshot_dir: Path,
    materialized_view: dict[str, Any],
) -> list[str]:
    snapshot_ids = [
        str(snapshot_dir.name).strip(),
        *[
            str(item).strip()
            for item in list(
                dict(materialized_view.get("source_snapshot_selection") or {}).get("selected_snapshot_ids") or []
            )
            if str(item).strip()
        ],
        *[
            str(dict(item).get("snapshot_id") or "").strip()
            for item in list(materialized_view.get("source_snapshots") or [])
            if str(dict(item).get("snapshot_id") or "").strip()
        ],
    ]
    ledger_payload = load_company_snapshot_json(
        snapshot_dir / "normalized_artifacts" / "organization_completeness_ledger.json"
    )
    snapshot_ids.extend(
        str(item).strip() for item in list(ledger_payload.get("selected_snapshot_ids") or []) if str(item).strip()
    )
    deduped_snapshot_ids: list[str] = []
    seen_snapshot_ids: set[str] = set()
    for snapshot_ref in snapshot_ids:
        if snapshot_ref in seen_snapshot_ids:
            continue
        seen_snapshot_ids.add(snapshot_ref)
        deduped_snapshot_ids.append(snapshot_ref)
    return deduped_snapshot_ids


def _load_snapshot_provider_function_id_map(snapshot_dir: Path) -> dict[str, list[str]]:
    function_ids_by_candidate_id: dict[str, list[str]] = {}
    try:
        payload = load_snapshot_candidate_artifact_payload(
            snapshot_dir=snapshot_dir,
            target_company=snapshot_dir.parent.name,
            asset_view="canonical_merged",
            company_key=snapshot_dir.parent.name,
            company_identity=load_company_snapshot_identity(snapshot_dir, fallback_payload={}),
            allow_candidate_documents_fallback=False,
        )
    except CandidateArtifactError:
        return function_ids_by_candidate_id
    materialized_payload = dict(payload.get("source_payload") or {})
    for item in list(materialized_payload.get("candidates") or []):
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("candidate_id") or "").strip()
        function_ids = _materialized_payload_function_ids(item)
        if not candidate_id or not function_ids:
            continue
        function_ids_by_candidate_id[candidate_id] = _normalize_string_list(
            [*function_ids_by_candidate_id.get(candidate_id, []), *function_ids]
        )

    return function_ids_by_candidate_id


def _backfill_candidate_provider_function_ids(
    *,
    snapshot_dir: Path,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
) -> list[Candidate]:
    candidate_ids = {
        str(candidate.candidate_id or "").strip()
        for candidate in candidates
        if str(candidate.candidate_id or "").strip()
    }
    if not candidate_ids:
        return candidates

    function_ids_by_candidate_id: dict[str, list[str]] = {}
    for snapshot_ref in _candidate_provider_function_id_snapshot_ids(
        snapshot_dir=snapshot_dir,
        materialized_view=materialized_view,
    ):
        source_snapshot_dir = snapshot_dir.parent / snapshot_ref
        if not source_snapshot_dir.exists():
            continue
        snapshot_function_ids = _load_snapshot_provider_function_id_map(source_snapshot_dir)
        for candidate_id, function_ids in snapshot_function_ids.items():
            if candidate_id not in candidate_ids or not function_ids:
                continue
            function_ids_by_candidate_id[candidate_id] = _normalize_string_list(
                [*function_ids_by_candidate_id.get(candidate_id, []), *function_ids]
            )

    if not function_ids_by_candidate_id:
        return candidates

    updated_candidates: list[Candidate] = []
    for candidate in candidates:
        candidate_id = str(candidate.candidate_id or "").strip()
        incoming_function_ids = function_ids_by_candidate_id.get(candidate_id, [])
        if not incoming_function_ids:
            updated_candidates.append(candidate)
            continue
        record = candidate.to_record()
        metadata = dict(record.get("metadata") or {})
        existing_function_ids = _candidate_level_function_ids(
            metadata.get("function_ids"),
            source_shard_filters=metadata.get("source_shard_filters"),
        )
        merged_function_ids = _normalize_string_list([*existing_function_ids, *incoming_function_ids])
        if not merged_function_ids or merged_function_ids == existing_function_ids:
            updated_candidates.append(candidate)
            continue
        metadata["function_ids"] = merged_function_ids
        record["metadata"] = metadata
        updated_candidates.append(Candidate(**record))
    return updated_candidates


def _provider_function_ids(candidate: Candidate, evidence: list[dict[str, Any]]) -> list[str]:
    metadata = dict(candidate.metadata or {})
    values = _candidate_level_function_ids(
        metadata.get("function_ids"),
        source_shard_filters=metadata.get("source_shard_filters"),
    )
    if values:
        return values
    for item in evidence:
        evidence_metadata = dict(item.get("metadata") or {})
        values = _candidate_level_function_ids(
            evidence_metadata.get("function_ids"),
            source_shard_filters=evidence_metadata.get("source_shard_filters"),
        )
        if values:
            return values
    return []


def _normalize_candidate(candidate: Candidate, evidence: list[dict[str, Any]]) -> dict[str, Any]:
    metadata = dict(candidate.metadata or {})
    functional_facets = derive_candidate_facets(candidate)
    role_bucket = derive_candidate_role_bucket(candidate)
    function_ids = _provider_function_ids(candidate, evidence)
    source_datasets = sorted(
        {
            str(item).strip()
            for item in [candidate.source_dataset]
            + [str(item.get("source_dataset") or "").strip() for item in evidence]
            if str(item).strip()
        }
    )
    source_types = sorted(
        {str(item.get("source_type") or "").strip() for item in evidence if str(item.get("source_type") or "").strip()}
    )
    urls = _dedupe_urls(
        [candidate.linkedin_url, candidate.media_url] + [str(item.get("url") or "").strip() for item in evidence]
    )
    manual_review_confirmed = (
        any(item.get("source_dataset") in {"manual_review", "manual_review_analysis"} for item in evidence)
        and candidate.category != "lead"
    )
    has_explicit_profile_capture = _has_explicit_profile_capture(evidence)
    has_profile_detail = _has_reusable_profile_fields(candidate)
    has_linkedin_url = bool(str(candidate.linkedin_url or "").strip())
    membership_review_required = bool(metadata.get("membership_review_required"))
    status_bucket = _status_bucket(candidate, membership_review_required=membership_review_required)
    needs_profile_completion = has_linkedin_url and not has_profile_detail
    needs_manual_review = (
        membership_review_required or status_bucket == "lead" or (not has_linkedin_url and not manual_review_confirmed)
    )
    reason = ""
    if membership_review_required:
        reason = str(metadata.get("membership_review_reason") or "").strip() or "suspicious_membership"
    elif status_bucket == "lead":
        reason = "unresolved_lead"
    elif not has_linkedin_url and not manual_review_confirmed:
        reason = "missing_linkedin"
    elif not has_profile_detail:
        reason = "profile_detail_gap"
    return {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "name_en": candidate.name_en,
        "category": candidate.category,
        "employment_status": candidate.employment_status,
        "status_bucket": status_bucket,
        "role_bucket": role_bucket,
        "function_ids": function_ids,
        "functional_facets": functional_facets,
        "role": candidate.role,
        "team": candidate.team,
        "focus_areas": candidate.focus_areas,
        "has_linkedin_url": has_linkedin_url,
        "has_profile_detail": has_profile_detail,
        "has_explicit_profile_capture": has_explicit_profile_capture,
        "needs_profile_completion": needs_profile_completion,
        "manual_review_confirmed": manual_review_confirmed,
        "needs_manual_review": needs_manual_review,
        "manual_review_reason": reason,
        "membership_review_required": membership_review_required,
        "membership_review_decision": str(metadata.get("membership_review_decision") or "").strip(),
        "manual_review_rationale": str(metadata.get("membership_review_rationale") or "").strip(),
        "manual_review_triggers": list(metadata.get("membership_review_triggers") or []),
        "manual_review_trigger_keywords": list(metadata.get("membership_review_trigger_keywords") or []),
        "evidence_count": len(evidence),
        "source_datasets": source_datasets,
        "source_types": source_types,
        "urls": urls[:12],
        "source_path": candidate.source_path,
    }


_PROFILE_SIGNAL_SCALAR_KEYS = (
    "headline",
    "summary",
    "about",
    "profile_location",
    "public_identifier",
)
_PROFILE_SIGNAL_METADATA_SCALAR_KEYS = _PROFILE_SIGNAL_SCALAR_KEYS + (
    "avatar_url",
    "photo_url",
    "primary_email",
)
_PROFILE_SIGNAL_LIST_KEYS = (
    "languages",
    "skills",
)


def _build_profile_timeline_registry_rows(
    *,
    store: ControlPlaneStore,
    candidates: list[Candidate],
) -> dict[str, dict[str, Any]]:
    profile_urls = [
        profile_url
        for profile_url in [
            candidate_profile_lookup_url(candidate.to_record(), dict(candidate.metadata or {}))
            for candidate in candidates
        ]
        if profile_url
    ]
    bulk_rows = store.get_linkedin_profile_registry_bulk(profile_urls)
    resolved: dict[str, dict[str, Any]] = dict(bulk_rows)
    for profile_url in profile_urls:
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        payload = dict(bulk_rows.get(normalized_key) or bulk_rows.get(profile_url) or {})
        if payload:
            resolved[profile_url] = payload
    return resolved


def _resolve_materialized_candidate_timeline(
    *,
    candidate: Candidate,
    normalized: dict[str, Any],
    registry_rows: dict[str, dict[str, Any]],
    timeline_cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidate_record = candidate.to_record()
    metadata = dict(candidate.metadata or {})
    profile_url = candidate_profile_lookup_url(candidate_record, metadata)
    registry_row = dict(registry_rows.get(profile_url) or {}) if profile_url else {}
    resolved = resolve_candidate_profile_timeline(
        payload=candidate_record,
        metadata=metadata,
        source_path=str(normalized.get("source_path") or candidate.source_path or ""),
        registry_row=registry_row,
        timeline_cache=timeline_cache,
    )
    return {
        "experience_lines": normalized_text_lines(resolved.get("experience_lines")),
        "education_lines": normalized_text_lines(resolved.get("education_lines")),
        "headline": str(resolved.get("headline") or "").strip(),
        "summary": str(resolved.get("summary") or "").strip(),
        "about": str(resolved.get("about") or "").strip(),
        "profile_location": str(resolved.get("profile_location") or "").strip(),
        "public_identifier": str(resolved.get("public_identifier") or "").strip(),
        "avatar_url": str(resolved.get("avatar_url") or "").strip(),
        "photo_url": str(resolved.get("photo_url") or "").strip(),
        "primary_email": str(resolved.get("primary_email") or "").strip(),
        "primary_email_metadata": normalized_primary_email_metadata(resolved.get("primary_email_metadata")),
        "languages": normalized_text_lines(resolved.get("languages")),
        "skills": normalized_text_lines(resolved.get("skills")),
        "profile_capture_kind": str(resolved.get("profile_capture_kind") or "").strip(),
        "profile_capture_source_path": str(resolved.get("profile_capture_source_path") or "").strip(),
        "source_kind": str(resolved.get("source_kind") or "").strip(),
        "source_path": str(resolved.get("source_path") or "").strip(),
    }


def _build_materialized_candidate_record(
    candidate: Candidate,
    normalized: dict[str, Any],
    timeline: dict[str, Any],
) -> dict[str, Any]:
    record = candidate.to_record()
    metadata = dict(record.get("metadata") or {})
    function_ids = _normalize_string_list(normalized.get("function_ids"))
    if function_ids:
        record["function_ids"] = function_ids
        metadata["function_ids"] = function_ids
    else:
        record.pop("function_ids", None)
        metadata.pop("function_ids", None)
    experience_lines = normalized_text_lines(timeline.get("experience_lines"))
    education_lines = normalized_text_lines(timeline.get("education_lines"))
    if experience_lines:
        record["experience_lines"] = experience_lines
        metadata["experience_lines"] = experience_lines
    if education_lines:
        record["education_lines"] = education_lines
        metadata["education_lines"] = education_lines
    if str(timeline.get("source_kind") or "").strip():
        record["profile_timeline_source"] = str(timeline.get("source_kind") or "").strip()
        metadata["profile_timeline_source"] = str(timeline.get("source_kind") or "").strip()
    if str(timeline.get("source_path") or "").strip():
        record["profile_timeline_source_path"] = str(timeline.get("source_path") or "").strip()
        metadata["profile_timeline_source_path"] = str(timeline.get("source_path") or "").strip()
    if str(timeline.get("profile_capture_kind") or "").strip():
        record["profile_capture_kind"] = str(timeline.get("profile_capture_kind") or "").strip()
        metadata["profile_capture_kind"] = str(timeline.get("profile_capture_kind") or "").strip()
    if str(timeline.get("profile_capture_source_path") or "").strip():
        record["profile_capture_source_path"] = str(timeline.get("profile_capture_source_path") or "").strip()
        metadata["profile_capture_source_path"] = str(timeline.get("profile_capture_source_path") or "").strip()
    _merge_resolved_profile_signals_into_metadata(metadata, timeline)
    _merge_resolved_profile_signals_into_record(record, timeline)
    if not str(record.get("media_url") or "").strip():
        media_url = _resolved_profile_media_url(metadata, timeline)
        if media_url:
            record["media_url"] = media_url
    record["metadata"] = metadata
    record = _project_profile_signal_fields_into_record(record)
    if primary_email_requires_suppression({}, context=record):
        record.pop("primary_email", None)
        record.pop("email", None)
        record.pop("primary_email_metadata", None)
    record["has_profile_detail"] = bool(normalized.get("has_profile_detail"))
    record["needs_profile_completion"] = bool(normalized.get("needs_profile_completion"))
    return record


def _candidate_with_resolved_profile_timeline(
    candidate: Candidate,
    timeline: dict[str, Any],
) -> Candidate:
    record = candidate.to_record()
    metadata = dict(record.get("metadata") or {})
    function_ids = _candidate_level_function_ids(
        metadata.get("function_ids"),
        source_shard_filters=metadata.get("source_shard_filters"),
    )
    if function_ids:
        metadata["function_ids"] = function_ids
    else:
        metadata.pop("function_ids", None)
    experience_lines = normalized_text_lines(timeline.get("experience_lines"))
    education_lines = normalized_text_lines(timeline.get("education_lines"))
    if experience_lines:
        metadata["experience_lines"] = experience_lines
    if education_lines:
        metadata["education_lines"] = education_lines
    if str(timeline.get("source_kind") or "").strip():
        metadata["profile_timeline_source"] = str(timeline.get("source_kind") or "").strip()
    if str(timeline.get("source_path") or "").strip():
        metadata["profile_timeline_source_path"] = str(timeline.get("source_path") or "").strip()
    if str(timeline.get("profile_capture_kind") or "").strip():
        metadata["profile_capture_kind"] = str(timeline.get("profile_capture_kind") or "").strip()
    if str(timeline.get("profile_capture_source_path") or "").strip():
        metadata["profile_capture_source_path"] = str(timeline.get("profile_capture_source_path") or "").strip()
    _merge_resolved_profile_signals_into_metadata(metadata, timeline)
    if not str(record.get("media_url") or "").strip():
        media_url = _resolved_profile_media_url(metadata, timeline)
        if media_url:
            record["media_url"] = media_url
    record["metadata"] = metadata
    return Candidate(**record)


def _build_normalized_candidate_record(normalized: dict[str, Any], timeline: dict[str, Any]) -> dict[str, Any]:
    record = dict(normalized)
    experience_lines = normalized_text_lines(timeline.get("experience_lines"))
    education_lines = normalized_text_lines(timeline.get("education_lines"))
    if experience_lines:
        record["experience_lines"] = experience_lines
    if education_lines:
        record["education_lines"] = education_lines
    if str(timeline.get("source_kind") or "").strip():
        record["profile_timeline_source"] = str(timeline.get("source_kind") or "").strip()
    if str(timeline.get("source_path") or "").strip():
        record["profile_timeline_source_path"] = str(timeline.get("source_path") or "").strip()
    if str(timeline.get("profile_capture_kind") or "").strip():
        record["profile_capture_kind"] = str(timeline.get("profile_capture_kind") or "").strip()
    if str(timeline.get("profile_capture_source_path") or "").strip():
        record["profile_capture_source_path"] = str(timeline.get("profile_capture_source_path") or "").strip()
    _merge_resolved_profile_signals_into_record(record, timeline)
    if not str(record.get("media_url") or "").strip():
        media_url = _resolved_profile_media_url(record, timeline)
        if media_url:
            record["media_url"] = media_url
    return record


def _build_reusable_document(
    candidate: Candidate,
    evidence: list[dict[str, Any]],
    normalized: dict[str, Any],
    timeline: dict[str, Any],
) -> dict[str, Any]:
    evidence_summaries = [
        str(item.get("summary") or "").strip() for item in evidence if str(item.get("summary") or "").strip()
    ]
    evidence_titles = [
        str(item.get("title") or "").strip() for item in evidence if str(item.get("title") or "").strip()
    ]
    experience_lines = normalized_text_lines(timeline.get("experience_lines"))
    education_lines = normalized_text_lines(timeline.get("education_lines"))
    timeline_document = " | ".join([*experience_lines, *education_lines])
    profile_document = " | ".join(
        part
        for part in [
            candidate_searchable_text(candidate, include_notes=True),
            timeline_document,
        ]
        if str(part or "").strip()
    )
    evidence_document = " | ".join(evidence_summaries[:8] or evidence_titles[:8])
    semantic_document = " | ".join(
        part
        for part in [
            profile_document,
            normalized["status_bucket"],
            normalized["role_bucket"],
            " ".join(normalized["functional_facets"]),
            evidence_document,
        ]
        if str(part or "").strip()
    )
    reusable_document = {
        "candidate_id": candidate.candidate_id,
        "display_name": candidate.display_name,
        "status_bucket": normalized["status_bucket"],
        "role_bucket": normalized["role_bucket"],
        "function_ids": normalized["function_ids"],
        "functional_facets": normalized["functional_facets"],
        "manual_review_confirmed": normalized["manual_review_confirmed"],
        "has_profile_detail": normalized["has_profile_detail"],
        "has_explicit_profile_capture": normalized["has_explicit_profile_capture"],
        "has_linkedin_url": normalized["has_linkedin_url"],
        "needs_profile_completion": normalized["needs_profile_completion"],
        "source_datasets": normalized["source_datasets"],
        "source_types": normalized["source_types"],
        "urls": normalized["urls"],
        "profile_document": profile_document,
        "evidence_document": evidence_document,
        "semantic_document": semantic_document,
        "experience_lines": experience_lines,
        "education_lines": education_lines,
        "profile_capture_kind": str(timeline.get("profile_capture_kind") or "").strip(),
        "profile_capture_source_path": str(timeline.get("profile_capture_source_path") or "").strip(),
        "profile_timeline_source": str(timeline.get("source_kind") or "").strip(),
        "profile_timeline_source_path": str(timeline.get("source_path") or "").strip(),
    }
    _merge_resolved_profile_signals_into_record(reusable_document, timeline)
    return reusable_document


def _write_artifact_view(
    *,
    store: ControlPlaneStore,
    logger: AssetLogger,
    artifact_dir: Path,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    evidence: list[dict[str, Any]],
    evidence_by_candidate: dict[str, list[dict[str, Any]]],
    asset_view: str,
    build_profile: str,
    runtime_tuning_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    snapshot_id = str(Path(materialized_view["snapshot_dir"]).name)
    total_started_at = time.perf_counter()
    run_id = _snapshot_materialization_run_id(
        target_company=str(materialized_view.get("target_company") or ""),
        snapshot_id=snapshot_id,
        asset_view=asset_view,
    )
    store.start_snapshot_materialization_run(
        run_id=run_id,
        target_company=str(materialized_view.get("target_company") or ""),
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        summary={
            "artifact_dir": str(artifact_dir),
            "candidate_count": len(candidates),
            "evidence_count": len(evidence),
        },
    )
    try:
        writer_budget = resolved_materialization_global_writer_budget(runtime_tuning_overrides)
        writer_wait_ms = 0.0

        def _record_writer_slot(slot: dict[str, Any]) -> None:
            nonlocal writer_wait_ms
            writer_wait_ms += float(slot.get("wait_ms") or 0.0)

        payload_build_started_at = time.perf_counter()
        payloads = _build_artifact_view_payloads(
            store=store,
            artifact_dir=artifact_dir,
            materialized_view=materialized_view,
            candidates=candidates,
            evidence=evidence,
            evidence_by_candidate=evidence_by_candidate,
            asset_view=asset_view,
            build_profile=build_profile,
            runtime_tuning_overrides=runtime_tuning_overrides,
        )
        timings_ms = dict(dict(payloads.get("artifact_summary") or {}).get("timings_ms") or {})
        timings_ms.setdefault("payload_build_total", _candidate_artifact_elapsed_ms(payload_build_started_at))
        incremental_artifacts = dict(payloads.get("incremental_artifacts") or {})
        dirty_candidate_shards = list(incremental_artifacts.get("dirty_candidate_shards") or [])
        page_payloads = list(incremental_artifacts.get("page_payloads") or [])
        backlog_payloads = dict(incremental_artifacts.get("backlog_payloads") or {})
        manifest_payload = dict(incremental_artifacts.get("manifest_payload") or {})
        candidate_states = list(incremental_artifacts.get("candidate_states") or [])
        active_candidate_ids = list(incremental_artifacts.get("active_candidate_ids") or [])
        full_scope_state_replace_eligible = bool(
            incremental_artifacts.get("full_scope_state_replace_eligible")
        )
        removed_state_shard_paths = {
            str(item).strip()
            for item in list(incremental_artifacts.get("removed_state_shard_paths") or [])
            if str(item).strip()
        }
        stale_shard_paths = {
            str(item).strip()
            for item in list(incremental_artifacts.get("stale_shard_paths") or [])
            if str(item).strip()
        }
        shard_write_entries = [
            {
                "path": artifact_dir / relative_path,
                "payload": shard_payload,
                "asset_type": "candidate_shard",
                "source_kind": "candidate_artifact_builder",
                "is_raw_asset": False,
                "model_safe": True,
                "metadata": {
                    "candidate_id": str(shard.get("candidate_id") or "").strip(),
                    "fingerprint": str(shard.get("fingerprint") or "").strip(),
                    "asset_view": asset_view,
                },
            }
            for shard in dirty_candidate_shards
            for relative_path in [str(shard.get("relative_path") or "").strip()]
            for shard_payload in [dict(shard.get("payload") or {})]
            if relative_path and shard_payload
        ]
        shard_write_parallel_workers, _ = _candidate_artifact_parallel_workers(
            len(shard_write_entries),
            build_profile=build_profile,
            runtime_tuning_overrides=runtime_tuning_overrides,
        )
        step_started_at = time.perf_counter()
        with runtime_inflight_slot(
            "materialization_writer",
            budget=writer_budget,
            metadata={"asset_view": asset_view, "phase": "shard_write", "entry_count": len(shard_write_entries)},
        ) as writer_slot:
            _record_writer_slot(dict(writer_slot))
            logger.write_json_batch(
                shard_write_entries,
                parallel_workers=shard_write_parallel_workers,
            )
        timings_ms["shard_write"] = _candidate_artifact_elapsed_ms(step_started_at)

        step_started_at = time.perf_counter()
        with runtime_inflight_slot(
            "materialization_writer",
            budget=writer_budget,
            metadata={"asset_view": asset_view, "phase": "manifest_write", "entry_count": 1},
        ) as writer_slot:
            _record_writer_slot(dict(writer_slot))
            manifest_path = logger.write_json(
                artifact_dir / "manifest.json",
                manifest_payload,
                asset_type="candidate_manifest",
                source_kind="candidate_artifact_builder",
                is_raw_asset=False,
                model_safe=True,
            )
        timings_ms["manifest_write"] = _candidate_artifact_elapsed_ms(step_started_at)
        kept_page_paths: set[str] = set()
        page_write_entries = []
        for page in page_payloads:
            relative_path = str(page.get("relative_path") or "").strip()
            page_payload = dict(page.get("payload") or {})
            if not relative_path or not page_payload:
                continue
            kept_page_paths.add(relative_path)
            page_write_entries.append(
                {
                    "path": artifact_dir / relative_path,
                    "payload": page_payload,
                    "asset_type": "candidate_page",
                    "source_kind": "candidate_artifact_builder",
                    "is_raw_asset": False,
                    "model_safe": True,
                    "metadata": {
                        "asset_view": asset_view,
                        "page": int(page_payload.get("page") or 0),
                    },
                }
            )
        page_write_parallel_workers, _ = _candidate_artifact_parallel_workers(
            len(page_write_entries),
            build_profile=build_profile,
            runtime_tuning_overrides=runtime_tuning_overrides,
        )
        step_started_at = time.perf_counter()
        with runtime_inflight_slot(
            "materialization_writer",
            budget=writer_budget,
            metadata={"asset_view": asset_view, "phase": "page_write", "entry_count": len(page_write_entries)},
        ) as writer_slot:
            _record_writer_slot(dict(writer_slot))
            logger.write_json_batch(
                page_write_entries,
                parallel_workers=page_write_parallel_workers,
            )
        timings_ms["page_write"] = _candidate_artifact_elapsed_ms(step_started_at)
        pages_dir = artifact_dir / "pages"
        step_started_at = time.perf_counter()
        if pages_dir.exists():
            for existing_page in pages_dir.glob("*.json"):
                relative_existing = str(existing_page.relative_to(artifact_dir))
                if relative_existing not in kept_page_paths:
                    existing_page.unlink(missing_ok=True)
        timings_ms["page_cleanup"] = _candidate_artifact_elapsed_ms(step_started_at)

        step_started_at = time.perf_counter()
        with runtime_inflight_slot(
            "materialization_writer",
            budget=writer_budget,
            metadata={"asset_view": asset_view, "phase": "auxiliary_write", "entry_count": 5},
        ) as writer_slot:
            _record_writer_slot(dict(writer_slot))
            manual_review_view_path = logger.write_json(
                artifact_dir / "backlogs" / "manual_review.json",
                dict(backlog_payloads.get("manual_review") or {}),
                asset_type="manual_review_backlog_view",
                source_kind="candidate_artifact_builder",
                is_raw_asset=False,
                model_safe=True,
            )
            profile_completion_view_path = logger.write_json(
                artifact_dir / "backlogs" / "profile_completion.json",
                dict(backlog_payloads.get("profile_completion") or {}),
                asset_type="profile_completion_backlog_view",
                source_kind="candidate_artifact_builder",
                is_raw_asset=False,
                model_safe=True,
            )
            publishable_primary_emails_path = logger.write_json(
                artifact_dir / "publishable_primary_emails.json",
                dict(payloads.get("publishable_primary_emails") or {}),
                asset_type="publishable_primary_emails",
                source_kind="candidate_artifact_builder",
                is_raw_asset=False,
                model_safe=True,
            )
            backlog_path = logger.write_json(
                artifact_dir / "manual_review_backlog.json",
                payloads["manual_review_backlog"],
                asset_type="manual_review_backlog",
                source_kind="candidate_artifact_builder",
                is_raw_asset=False,
                model_safe=True,
            )
            profile_completion_backlog_path = logger.write_json(
                artifact_dir / "profile_completion_backlog.json",
                payloads["profile_completion_backlog"],
                asset_type="profile_completion_backlog",
                source_kind="candidate_artifact_builder",
                is_raw_asset=False,
                model_safe=True,
            )
        timings_ms["auxiliary_write"] = _candidate_artifact_elapsed_ms(step_started_at)

        step_started_at = time.perf_counter()
        with runtime_inflight_slot(
            "materialization_writer",
            budget=writer_budget,
            metadata={"asset_view": asset_view, "phase": "state_upsert", "entry_count": len(candidate_states)},
        ) as writer_slot:
            _record_writer_slot(dict(writer_slot))
            if full_scope_state_replace_eligible:
                store.replace_candidate_materialization_state_scope(
                    target_company=str(materialized_view.get("target_company") or ""),
                    snapshot_id=snapshot_id,
                    asset_view=asset_view,
                    states=candidate_states,
                )
            elif _candidate_artifact_bulk_state_upsert_enabled():
                store.bulk_upsert_candidate_materialization_states(states=candidate_states)
            else:
                for state in candidate_states:
                    if not isinstance(state, dict):
                        continue
                    store.upsert_candidate_materialization_state(
                        target_company=str(state.get("target_company") or ""),
                        snapshot_id=str(state.get("snapshot_id") or ""),
                        asset_view=str(state.get("asset_view") or ""),
                        candidate_id=str(state.get("candidate_id") or ""),
                        fingerprint=str(state.get("fingerprint") or ""),
                        shard_path=str(state.get("shard_path") or ""),
                        list_page=int(state.get("list_page") or 0),
                        dirty_reason=str(state.get("dirty_reason") or ""),
                        materialized_at=str(state.get("materialized_at") or ""),
                        metadata=dict(state.get("metadata") or {}),
                    )
        timings_ms["state_upsert"] = _candidate_artifact_elapsed_ms(step_started_at)
        timings_ms["materialization_writer_wait"] = round(writer_wait_ms, 2)
        timings_ms["materialization_writer_budget"] = float(writer_budget)

        step_started_at = time.perf_counter()
        pruned_states: list[dict[str, Any]] = []
        if full_scope_state_replace_eligible:
            stale_shard_paths.update(removed_state_shard_paths)
        else:
            pruned_states = store.prune_candidate_materialization_states(
                target_company=str(materialized_view.get("target_company") or ""),
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                active_candidate_ids=active_candidate_ids,
            )
        timings_ms["prune_states"] = _candidate_artifact_elapsed_ms(step_started_at)
        stale_shard_paths.update(
            str(item.get("shard_path") or "").strip()
            for item in pruned_states
            if str(item.get("shard_path") or "").strip()
        )
        step_started_at = time.perf_counter()
        for stale_path in sorted(stale_shard_paths):
            (artifact_dir / stale_path).unlink(missing_ok=True)
        timings_ms["stale_cleanup"] = _candidate_artifact_elapsed_ms(step_started_at)

        timings_ms["view_write_total"] = _candidate_artifact_elapsed_ms(total_started_at)
        payloads["artifact_summary"] = dict(payloads.get("artifact_summary") or {})
        payloads["artifact_summary"]["timings_ms"] = timings_ms
        run_summary = dict(payloads["artifact_summary"] or {})
        run_summary.update(
            {
                "run_id": run_id,
                "manifest_path": str(manifest_path),
                "manual_review_view_path": str(manual_review_view_path),
                "profile_completion_view_path": str(profile_completion_view_path),
                "publishable_primary_emails_path": str(publishable_primary_emails_path),
            }
        )
        store.complete_snapshot_materialization_run(
            run_id=run_id,
            status="completed",
            dirty_candidate_count=int(run_summary.get("dirty_candidate_count") or 0),
            completed_candidate_count=len(candidates),
            reused_candidate_count=int(run_summary.get("reused_candidate_count") or 0),
            summary=run_summary,
        )
    except Exception as exc:
        store.complete_snapshot_materialization_run(
            run_id=run_id,
            status="failed",
            dirty_candidate_count=0,
            completed_candidate_count=0,
            reused_candidate_count=0,
            summary={
                "artifact_dir": str(artifact_dir),
                "error": f"{type(exc).__name__}: {exc}",
            },
        )
        raise

    return {
        "artifact_paths": {
            "manual_review_backlog": str(backlog_path),
            "profile_completion_backlog": str(profile_completion_backlog_path),
            "manifest": str(manifest_path),
            "manual_review_backlog_view": str(manual_review_view_path),
            "profile_completion_backlog_view": str(profile_completion_view_path),
            "publishable_primary_emails": str(publishable_primary_emails_path),
        },
        "summary": payloads["artifact_summary"],
        "payloads": payloads,
    }


def _write_artifact_view_alias(
    *,
    store: ControlPlaneStore,
    logger: AssetLogger,
    artifact_dir: Path,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    source_view_result: dict[str, Any],
    asset_view: str,
    alias_asset_view: str,
) -> dict[str, Any]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    snapshot_id = str(Path(materialized_view["snapshot_dir"]).name)
    total_started_at = time.perf_counter()
    run_id = _snapshot_materialization_run_id(
        target_company=str(materialized_view.get("target_company") or ""),
        snapshot_id=snapshot_id,
        asset_view=asset_view,
    )
    store.start_snapshot_materialization_run(
        run_id=run_id,
        target_company=str(materialized_view.get("target_company") or ""),
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        summary={
            "artifact_dir": str(artifact_dir),
            "candidate_count": len(candidates),
            "storage_mode": "alias",
            "alias_asset_view": alias_asset_view,
        },
    )
    try:
        payloads = _build_aliased_artifact_view_payloads(
            materialized_view=materialized_view,
            candidates=candidates,
            source_view_result=source_view_result,
            asset_view=asset_view,
            alias_asset_view=alias_asset_view,
        )
        timings_ms = dict(dict(payloads.get("artifact_summary") or {}).get("timings_ms") or {})
        timings_ms["alias_prepare_total"] = _candidate_artifact_elapsed_ms(total_started_at)
        payloads["artifact_summary"] = dict(payloads.get("artifact_summary") or {})
        payloads["artifact_summary"]["timings_ms"] = timings_ms
        run_summary = dict(payloads["artifact_summary"] or {})
        run_summary["run_id"] = run_id
        store.complete_snapshot_materialization_run(
            run_id=run_id,
            status="completed",
            dirty_candidate_count=0,
            completed_candidate_count=len(candidates),
            reused_candidate_count=len(candidates),
            summary=run_summary,
        )
    except Exception as exc:
        store.complete_snapshot_materialization_run(
            run_id=run_id,
            status="failed",
            dirty_candidate_count=0,
            completed_candidate_count=0,
            reused_candidate_count=0,
            summary={
                "artifact_dir": str(artifact_dir),
                "error": f"{type(exc).__name__}: {exc}",
                "storage_mode": "alias",
            },
        )
        raise
    return {
        "artifact_paths": {},
        "summary": payloads["artifact_summary"],
        "payloads": payloads,
    }


def _build_aliased_artifact_view_payloads(
    *,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    source_view_result: dict[str, Any],
    asset_view: str,
    alias_asset_view: str,
) -> dict[str, Any]:
    source_payloads = dict(source_view_result.get("payloads") or {})
    source_summary = dict(source_view_result.get("summary") or {})
    materialized_documents = deepcopy(dict(source_payloads.get("materialized_documents") or {}))
    materialized_snapshot = dict(materialized_documents.get("snapshot") or {})
    materialized_snapshot["asset_view"] = asset_view
    materialized_documents["snapshot"] = materialized_snapshot
    manifest_payload = deepcopy(dict(dict(source_payloads.get("incremental_artifacts") or {}).get("manifest_payload") or {}))
    manifest_payload.update(
        {
            "asset_view": asset_view,
            "alias_asset_view": alias_asset_view,
            "storage_mode": "alias",
        }
    )
    summary = deepcopy(source_summary)
    summary.update(
        {
            "asset_view": asset_view,
            "alias_asset_view": alias_asset_view,
            "storage_mode": "alias",
            "dirty_candidate_count": 0,
            "reused_candidate_count": len(candidates),
            "state_upsert_candidate_count": 0,
        }
    )
    summary["timings_ms"] = {"alias_prepare_total": 0.0}
    summary["build_execution"] = {
        "parallel_workers": 0,
        "batch_json_writes_enabled": False,
        "bulk_state_upsert_enabled": False,
        "storage_mode": "alias",
    }
    return {
        "materialized_documents": materialized_documents,
        "normalized_candidates": list(source_payloads.get("normalized_candidates") or []),
        "reusable_documents": list(source_payloads.get("reusable_documents") or []),
        "manual_review_backlog": list(source_payloads.get("manual_review_backlog") or []),
        "profile_completion_backlog": list(source_payloads.get("profile_completion_backlog") or []),
        "publishable_primary_emails": deepcopy(dict(source_payloads.get("publishable_primary_emails") or {})),
        "artifact_summary": summary,
        "incremental_artifacts": {
            "active_candidate_ids": [candidate.candidate_id for candidate in candidates],
            "candidate_states": [],
            "dirty_candidate_shards": [],
            "stale_shard_paths": [],
            "page_payloads": [],
            "manifest_payload": manifest_payload,
            "backlog_payloads": {},
        },
    }


def _candidate_membership_rows(
    *,
    store: ControlPlaneStore,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    candidates: list[Candidate],
) -> list[dict[str, Any]]:
    return [
        _build_asset_membership_row(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            artifact_kind="organization_asset",
            artifact_key=asset_view,
            candidate_record=candidate.to_record(),
            lane="baseline",
            employment_scope=str(candidate.employment_status or "").strip(),
        )
        for candidate in candidates
    ]


def _candidate_materialization_state_requires_upsert(
    previous_state: dict[str, Any],
    next_state: dict[str, Any],
) -> bool:
    if not previous_state:
        return True
    previous_metadata = dict(previous_state.get("metadata") or {})
    next_metadata = dict(next_state.get("metadata") or {})
    return any(
        (
            str(previous_state.get("fingerprint") or "").strip()
            != str(next_state.get("fingerprint") or "").strip(),
            str(previous_state.get("shard_path") or "").strip() != str(next_state.get("shard_path") or "").strip(),
            int(previous_state.get("list_page") or 0) != int(next_state.get("list_page") or 0),
            str(previous_state.get("dirty_reason") or "").strip() != str(next_state.get("dirty_reason") or "").strip(),
            str(previous_state.get("materialized_at") or "").strip()
            != str(next_state.get("materialized_at") or "").strip(),
            json.dumps(previous_metadata, ensure_ascii=False, sort_keys=True)
            != json.dumps(next_metadata, ensure_ascii=False, sort_keys=True),
        )
    )


def _finalize_artifact_view_materialization(
    *,
    store: ControlPlaneStore,
    logger: AssetLogger,
    artifact_dir: Path,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    candidates: list[Candidate],
    payloads: dict[str, Any],
    summary: dict[str, Any],
    write_compatibility_exports: bool = True,
) -> dict[str, Any]:
    source_path = artifact_dir / "artifact_summary.json"
    finalize_started_at = time.perf_counter()
    timings_ms = dict(summary.get("timings_ms") or {})
    step_started_at = time.perf_counter()
    generation = store.register_asset_materialization(
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        artifact_kind="organization_asset",
        artifact_key=asset_view,
        source_path=str(source_path),
        summary=summary,
        metadata={
            "artifact_dir": str(artifact_dir),
            "source_kind": "candidate_artifact_builder",
        },
        members=_candidate_membership_rows(
            store=store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            candidates=candidates,
        ),
    )
    timings_ms["generation_register"] = _candidate_artifact_elapsed_ms(step_started_at)
    step_started_at = time.perf_counter()
    membership_summary = (
        store.summarize_asset_membership_index(generation_key=str(generation.get("generation_key") or ""))
        if generation
        else {}
    )
    timings_ms["membership_summary"] = _candidate_artifact_elapsed_ms(step_started_at)
    generation_fields = {
        "materialization_generation_key": str(generation.get("generation_key") or ""),
        "materialization_generation_sequence": int(generation.get("generation_sequence") or 0),
        "materialization_watermark": str(generation.get("generation_watermark") or ""),
        "membership_summary": membership_summary,
    }
    summary.update(generation_fields)
    materialized_documents = dict(payloads.get("materialized_documents") or {})
    snapshot_payload = dict(materialized_documents.get("snapshot") or {})
    snapshot_payload.update(generation_fields)
    materialized_documents["snapshot"] = snapshot_payload
    payloads["materialized_documents"] = materialized_documents
    payloads["artifact_summary"] = summary
    manifest_payload = dict(dict(payloads.get("incremental_artifacts") or {}).get("manifest_payload") or {})
    if manifest_payload:
        manifest_payload.update(generation_fields)
        payloads.setdefault("incremental_artifacts", {})["manifest_payload"] = manifest_payload
    step_started_at = time.perf_counter()
    snapshot_manifest_path = _write_snapshot_manifest(
        logger=logger,
        artifact_dir=artifact_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        payloads=payloads,
        summary=summary,
    )
    timings_ms["snapshot_manifest_write"] = _candidate_artifact_elapsed_ms(step_started_at)
    step_started_at = time.perf_counter()
    summary_path = logger.write_json(
        artifact_dir / "artifact_summary.json",
        summary,
        asset_type="candidate_artifact_summary",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    timings_ms["artifact_summary_write"] = _candidate_artifact_elapsed_ms(step_started_at)
    if manifest_payload:
        step_started_at = time.perf_counter()
        logger.write_json(
            artifact_dir / "manifest.json",
            manifest_payload,
            asset_type="candidate_manifest",
            source_kind="candidate_artifact_builder",
            is_raw_asset=False,
            model_safe=True,
        )
        timings_ms["manifest_refresh_write"] = _candidate_artifact_elapsed_ms(step_started_at)
    compatibility_paths: dict[str, str] = {}
    compatibility_exports_enabled = bool(write_compatibility_exports) and compatibility_artifact_export_enabled()
    step_started_at = time.perf_counter()
    if compatibility_exports_enabled:
        compatibility_paths = _write_compatibility_artifacts(
            logger=logger,
            artifact_dir=artifact_dir,
            materialized_documents=materialized_documents,
            normalized_candidates=list(payloads.get("normalized_candidates") or []),
            reusable_documents=list(payloads.get("reusable_documents") or []),
        )
    timings_ms["compatibility_write"] = _candidate_artifact_elapsed_ms(step_started_at)
    timings_ms["finalize_total"] = _candidate_artifact_elapsed_ms(finalize_started_at)
    summary["timings_ms"] = timings_ms
    return {
        "generation": generation,
        "membership_summary": membership_summary,
        "artifact_paths": {
            **_compatibility_artifact_paths(artifact_dir),
            **compatibility_paths,
            "artifact_summary": str(summary_path),
            "manifest": str(artifact_dir / "manifest.json"),
            "snapshot_manifest": str(snapshot_manifest_path),
        },
        "compatibility_exports_enabled": compatibility_exports_enabled,
    }


def _write_snapshot_manifest(
    *,
    logger: AssetLogger,
    artifact_dir: Path,
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    payloads: dict[str, Any],
    summary: dict[str, Any],
) -> Path:
    materialized_documents = dict(payloads.get("materialized_documents") or {})
    snapshot_payload = dict(materialized_documents.get("snapshot") or {})
    snapshot_manifest_payload = {
        "target_company": str(snapshot_payload.get("target_company") or target_company).strip() or target_company,
        "company_key": str(snapshot_payload.get("company_key") or summary.get("company_key") or "").strip(),
        "snapshot_id": str(snapshot_payload.get("snapshot_id") or snapshot_id).strip() or snapshot_id,
        "asset_view": str(snapshot_payload.get("asset_view") or asset_view).strip() or asset_view,
        "company_identity": dict(snapshot_payload.get("company_identity") or {}),
        "source_snapshots": [
            dict(item)
            for item in list(snapshot_payload.get("source_snapshots") or summary.get("source_snapshots") or [])
            if isinstance(item, dict)
        ],
        "source_snapshot_selection": dict(
            snapshot_payload.get("source_snapshot_selection") or summary.get("source_snapshot_selection") or {}
        ),
        "control_plane_candidate_count": int(
            snapshot_payload.get("control_plane_candidate_count")
            or snapshot_payload.get("sqlite_candidate_count")
            or summary.get("control_plane_candidate_count")
            or summary.get("sqlite_candidate_count")
            or 0
        ),
        "control_plane_evidence_count": int(
            snapshot_payload.get("control_plane_evidence_count")
            or snapshot_payload.get("sqlite_evidence_count")
            or summary.get("control_plane_evidence_count")
            or summary.get("sqlite_evidence_count")
            or 0
        ),
        "materialization_generation_key": str(summary.get("materialization_generation_key") or "").strip(),
        "materialization_generation_sequence": int(summary.get("materialization_generation_sequence") or 0),
        "materialization_watermark": str(summary.get("materialization_watermark") or "").strip(),
        "membership_summary": dict(summary.get("membership_summary") or {}),
        "manifest_path": "manifest.json",
        "artifact_summary_path": "artifact_summary.json",
        "created_at": str(summary.get("created_at") or summary.get("updated_at") or "").strip(),
    }
    return logger.write_json(
        artifact_dir / "snapshot_manifest.json",
        snapshot_manifest_payload,
        asset_type="candidate_snapshot_manifest",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )


def _write_compatibility_artifacts(
    *,
    logger: AssetLogger,
    artifact_dir: Path,
    materialized_documents: dict[str, Any],
    normalized_candidates: list[dict[str, Any]],
    reusable_documents: list[dict[str, Any]],
) -> dict[str, str]:
    materialized_path = logger.write_json(
        artifact_dir / "materialized_candidate_documents.json",
        materialized_documents,
        asset_type="materialized_candidate_documents",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    normalized_path = logger.write_json(
        artifact_dir / "normalized_candidates.json",
        normalized_candidates,
        asset_type="normalized_candidates",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    reusable_path = logger.write_json(
        artifact_dir / "reusable_candidate_documents.json",
        reusable_documents,
        asset_type="reusable_candidate_documents",
        source_kind="candidate_artifact_builder",
        is_raw_asset=False,
        model_safe=True,
    )
    return {
        "materialized_candidate_documents": str(materialized_path),
        "normalized_candidates": str(normalized_path),
        "reusable_candidate_documents": str(reusable_path),
    }


def _reconstruct_compatibility_payloads_from_serving_artifacts(
    *,
    snapshot_dir: Path,
    artifact_dir: Path,
    target_company: str,
    company_key: str,
    company_identity: dict[str, Any],
    asset_view: str,
    artifact_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
    if not manifest_payload:
        return {}
    summary_payload = dict(artifact_summary or {}) or load_company_snapshot_json(artifact_dir / "artifact_summary.json")
    materialized_candidates: list[dict[str, Any]] = []
    normalized_candidates: list[dict[str, Any]] = []
    reusable_documents: list[dict[str, Any]] = []
    evidence_by_key: dict[str, dict[str, Any]] = {}
    manual_review_backlog: list[dict[str, Any]] = []
    profile_completion_backlog: list[dict[str, Any]] = []
    for entry in list(manifest_payload.get("candidate_shards") or []):
        if not isinstance(entry, dict):
            continue
        shard_path = artifact_dir / str(entry.get("path") or "").strip()
        candidate_id = str(entry.get("candidate_id") or "").strip()
        fingerprint = str(entry.get("fingerprint") or "").strip()
        shard_payload = _load_candidate_shard_payload(shard_path, candidate_id=candidate_id, fingerprint=fingerprint)
        if shard_payload is None:
            raise CandidateArtifactError(f"Candidate shard payload missing for {candidate_id} in {artifact_dir}")
        materialized_candidates.append(dict(shard_payload.get("materialized_candidate") or {}))
        normalized_candidates.append(dict(shard_payload.get("normalized_candidate") or {}))
        reusable_documents.append(dict(shard_payload.get("reusable_document") or {}))
        for evidence_item in list(shard_payload.get("evidence") or []):
            if not isinstance(evidence_item, dict):
                continue
            evidence_key = str(evidence_item.get("evidence_id") or "").strip() or json.dumps(
                {
                    "candidate_id": str(evidence_item.get("candidate_id") or "").strip(),
                    "title": str(evidence_item.get("title") or "").strip(),
                    "url": str(evidence_item.get("url") or "").strip(),
                    "source_path": str(evidence_item.get("source_path") or "").strip(),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            if evidence_key not in evidence_by_key:
                evidence_by_key[evidence_key] = dict(evidence_item)
        manual_review_item = dict(shard_payload.get("manual_review_backlog_item") or {})
        if manual_review_item:
            manual_review_backlog.append(manual_review_item)
        profile_completion_item = dict(shard_payload.get("profile_completion_backlog_item") or {})
        if profile_completion_item:
            profile_completion_backlog.append(profile_completion_item)
    if not manual_review_backlog:
        manual_review_backlog = list(
            dict(load_company_snapshot_json(artifact_dir / "backlogs" / "manual_review.json") or {}).get("items") or []
        )
    if not profile_completion_backlog:
        profile_completion_backlog = list(
            dict(load_company_snapshot_json(artifact_dir / "backlogs" / "profile_completion.json") or {}).get("items")
            or []
        )
    snapshot_metadata = {
        "company_key": str(company_key or summary_payload.get("company_key") or snapshot_dir.parent.name).strip(),
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "materialized_at": str(
            manifest_payload.get("materialized_at")
            or summary_payload.get("created_at")
            or summary_payload.get("updated_at")
            or ""
        ).strip(),
        "materialized_from": "snapshot_manifest_shards",
        "asset_view": asset_view,
        "target_company": str(target_company or summary_payload.get("target_company") or "").strip() or target_company,
        "company_identity": dict(company_identity or {}),
        "source_snapshots": [
            dict(item)
            for item in list(manifest_payload.get("source_snapshots") or summary_payload.get("source_snapshots") or [])
            if isinstance(item, dict)
        ],
        "source_snapshot_selection": dict(
            manifest_payload.get("source_snapshot_selection") or summary_payload.get("source_snapshot_selection") or {}
        ),
        "control_plane_candidate_count": int(
            manifest_payload.get("control_plane_candidate_count")
            or manifest_payload.get("sqlite_candidate_count")
            or summary_payload.get("control_plane_candidate_count")
            or summary_payload.get("sqlite_candidate_count")
            or 0
        ),
        "control_plane_evidence_count": int(
            manifest_payload.get("control_plane_evidence_count")
            or manifest_payload.get("sqlite_evidence_count")
            or summary_payload.get("control_plane_evidence_count")
            or summary_payload.get("sqlite_evidence_count")
            or 0
        ),
        "materialization_generation_key": str(
            manifest_payload.get("materialization_generation_key")
            or summary_payload.get("materialization_generation_key")
            or ""
        ).strip(),
        "materialization_generation_sequence": int(
            manifest_payload.get("materialization_generation_sequence")
            or summary_payload.get("materialization_generation_sequence")
            or 0
        ),
        "materialization_watermark": str(
            manifest_payload.get("materialization_watermark") or summary_payload.get("materialization_watermark") or ""
        ).strip(),
        "membership_summary": dict(
            manifest_payload.get("membership_summary") or summary_payload.get("membership_summary") or {}
        ),
    }
    return {
        "snapshot": snapshot_metadata,
        "candidates": materialized_candidates,
        "evidence": list(evidence_by_key.values()),
        "normalized_candidates": normalized_candidates,
        "reusable_documents": reusable_documents,
        "manual_review_backlog": manual_review_backlog,
        "profile_completion_backlog": profile_completion_backlog,
    }


def _export_compatibility_artifacts_from_serving_view(artifact_dir: Path) -> dict[str, str]:
    manifest_payload = load_company_snapshot_json(artifact_dir / "manifest.json")
    if not manifest_payload:
        return {
            "materialized_candidate_documents": "",
            "normalized_candidates": "",
            "reusable_candidate_documents": "",
        }
    snapshot_dir = artifact_dir.parent if artifact_dir.name == "normalized_artifacts" else artifact_dir.parent.parent
    artifact_summary = load_company_snapshot_json(artifact_dir / "artifact_summary.json")
    identity_payload = load_company_snapshot_identity(snapshot_dir, fallback_payload={})
    target_company = (
        str(identity_payload.get("canonical_name") or "").strip()
        or str(identity_payload.get("requested_name") or "").strip()
        or str(artifact_summary.get("target_company") or "").strip()
        or snapshot_dir.parent.name
    )
    effective_artifact_dir, artifact_summary, manifest_payload, _ = _resolve_serving_artifact_view_payload_source(
        snapshot_dir=snapshot_dir,
        artifact_dir=artifact_dir,
        asset_view=str(manifest_payload.get("asset_view") or artifact_summary.get("asset_view") or "canonical_merged"),
        artifact_summary=artifact_summary,
        manifest_payload=manifest_payload,
    )
    reconstructed = _reconstruct_compatibility_payloads_from_serving_artifacts(
        snapshot_dir=snapshot_dir,
        artifact_dir=effective_artifact_dir,
        target_company=target_company,
        company_key=str(identity_payload.get("company_key") or snapshot_dir.parent.name).strip()
        or snapshot_dir.parent.name,
        company_identity=identity_payload,
        asset_view=str(manifest_payload.get("asset_view") or artifact_summary.get("asset_view") or "canonical_merged"),
        artifact_summary=artifact_summary,
    )
    snapshot_root = snapshot_dir if snapshot_dir.is_dir() else artifact_dir
    logger = AssetLogger(snapshot_root)
    return _write_compatibility_artifacts(
        logger=logger,
        artifact_dir=artifact_dir,
        materialized_documents={
            "snapshot": dict(reconstructed.get("snapshot") or {}),
            "candidates": list(reconstructed.get("candidates") or []),
            "evidence": list(reconstructed.get("evidence") or []),
        },
        normalized_candidates=list(reconstructed.get("normalized_candidates") or []),
        reusable_documents=list(reconstructed.get("reusable_documents") or []),
    )


def _build_artifact_view_payloads(
    *,
    store: ControlPlaneStore,
    artifact_dir: Path,
    materialized_view: dict[str, Any],
    candidates: list[Candidate],
    evidence: list[dict[str, Any]],
    evidence_by_candidate: dict[str, list[dict[str, Any]]],
    asset_view: str,
    build_profile: str,
    runtime_tuning_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target_company = str(materialized_view.get("target_company") or "")
    snapshot_id = str(Path(materialized_view["snapshot_dir"]).name)
    total_started_at = time.perf_counter()
    timings_ms: dict[str, float] = {}
    normalized_candidates: list[dict[str, Any]] = []
    reusable_documents: list[dict[str, Any]] = []
    manual_review_backlog: list[dict[str, Any]] = []
    profile_completion_backlog: list[dict[str, Any]] = []
    status_counter: Counter[str] = Counter()
    profile_detail_count = 0
    explicit_profile_capture_count = 0
    manual_review_confirmed_count = 0
    missing_linkedin_count = 0
    function_id_candidate_count = 0
    structured_timeline_count = 0
    structured_experience_count = 0
    structured_education_count = 0
    step_started_at = time.perf_counter()
    profile_registry_rows = _build_profile_timeline_registry_rows(store=store, candidates=candidates)
    timings_ms["profile_registry_lookup"] = _candidate_artifact_elapsed_ms(step_started_at)
    step_started_at = time.perf_counter()
    existing_states = {
        str(item.get("candidate_id") or "").strip(): item
        for item in store.list_candidate_materialization_states(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
        )
        if str(item.get("candidate_id") or "").strip()
    }
    timings_ms["existing_state_load"] = _candidate_artifact_elapsed_ms(step_started_at)
    materialized_candidate_records: list[dict[str, Any]] = []
    candidate_states: list[dict[str, Any]] = []
    candidate_state_upserts: list[dict[str, Any]] = []
    dirty_candidate_shards: list[dict[str, Any]] = []
    stale_shard_paths: set[str] = set()
    active_candidate_ids: list[str] = []
    current_candidate_ids_set: set[str] = set()
    publishable_primary_emails: dict[str, Any] = {
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "by_candidate_id": {},
        "by_profile_url_key": {},
    }
    parallel_workers, parallel_tuning = _candidate_artifact_parallel_workers(
        len(candidates),
        build_profile=build_profile,
        runtime_tuning_overrides=runtime_tuning_overrides,
    )
    timeline_cache: dict[str, dict[str, Any]] = {}

    def _prepare_candidate(index: int, candidate: Candidate) -> dict[str, Any]:
        candidate_evidence = sorted(
            list(evidence_by_candidate.get(candidate.candidate_id, [])),
            key=_evidence_key,
        )
        profile_url = candidate_profile_lookup_url(candidate.to_record(), dict(candidate.metadata or {}))
        profile_registry_row = dict(profile_registry_rows.get(profile_url) or {}) if profile_url else {}
        fingerprint = _build_candidate_materialization_fingerprint(
            candidate=candidate,
            evidence=candidate_evidence,
            profile_registry_row=profile_registry_row,
        )
        previous_state = dict(existing_states.get(candidate.candidate_id) or {})
        list_page = (index // _CANDIDATE_SHARD_PAGE_SIZE) + 1 if candidates else 0
        shard_relative_path = str(previous_state.get("shard_path") or "").strip()
        dirty_reason = ""
        shard_payload = None
        if (
            previous_state
            and str(previous_state.get("fingerprint") or "").strip() == fingerprint
            and shard_relative_path
        ):
            shard_payload = _load_candidate_shard_payload(
                artifact_dir / shard_relative_path,
                candidate_id=candidate.candidate_id,
                fingerprint=fingerprint,
            )
            if shard_payload is None:
                dirty_reason = "missing_shard_payload"
        else:
            dirty_reason = "fingerprint_changed" if previous_state else "missing_state"
        if not shard_relative_path or dirty_reason:
            shard_relative_path = _candidate_shard_relative_path(candidate.candidate_id, fingerprint)
        if shard_payload is None:
            shard_payload = _build_candidate_shard_payload(
                candidate=candidate,
                evidence=candidate_evidence,
                profile_registry_rows=profile_registry_rows,
                timeline_cache=(timeline_cache if parallel_workers <= 1 else {}),
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                fingerprint=fingerprint,
            )
            shard_payload["materialized_at"] = _utc_now_iso()
        previous_shard_path = str(previous_state.get("shard_path") or "").strip()
        return {
            "candidate": candidate,
            "candidate_evidence": candidate_evidence,
            "profile_url": profile_url,
            "profile_url_key": normalize_linkedin_profile_url_key(profile_url) if profile_url else "",
            "fingerprint": fingerprint,
            "previous_state": previous_state,
            "list_page": list_page,
            "shard_relative_path": shard_relative_path,
            "dirty_reason": dirty_reason,
            "shard_payload": shard_payload,
            "previous_shard_path": previous_shard_path,
        }

    prepared_candidates: list[dict[str, Any]]
    step_started_at = time.perf_counter()
    if parallel_workers > 1:
        with ThreadPoolExecutor(
            max_workers=parallel_workers,
            thread_name_prefix=f"artifact-build-{asset_view}",
        ) as executor:
            prepared_candidates = list(
                executor.map(
                    lambda item: _prepare_candidate(item[0], item[1]),
                    list(enumerate(candidates)),
                )
            )
    else:
        prepared_candidates = [_prepare_candidate(index, candidate) for index, candidate in enumerate(candidates)]
    timings_ms["prepare_candidates"] = _candidate_artifact_elapsed_ms(step_started_at)

    step_started_at = time.perf_counter()
    for prepared in prepared_candidates:
        candidate = prepared["candidate"]
        active_candidate_ids.append(candidate.candidate_id)
        current_candidate_ids_set.add(candidate.candidate_id)
        profile_url = str(prepared.get("profile_url") or "").strip()
        profile_url_key = str(prepared.get("profile_url_key") or "").strip()
        fingerprint = str(prepared.get("fingerprint") or "").strip()
        previous_state = dict(prepared.get("previous_state") or {})
        list_page = int(prepared.get("list_page") or 0)
        shard_relative_path = str(prepared.get("shard_relative_path") or "").strip()
        dirty_reason = str(prepared.get("dirty_reason") or "").strip()
        shard_payload = dict(prepared.get("shard_payload") or {})
        if dirty_reason:
            dirty_candidate_shards.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "fingerprint": fingerprint,
                    "relative_path": shard_relative_path,
                    "payload": shard_payload,
                }
            )
        previous_shard_path = str(prepared.get("previous_shard_path") or "").strip()
        if previous_shard_path and previous_shard_path != shard_relative_path:
            stale_shard_paths.add(previous_shard_path)

        materialized_record = dict(shard_payload.get("materialized_candidate") or {})
        normalized_record = dict(shard_payload.get("normalized_candidate") or {})
        reusable_record = dict(shard_payload.get("reusable_document") or {})
        manual_review_item = dict(shard_payload.get("manual_review_backlog_item") or {})
        profile_completion_item = dict(shard_payload.get("profile_completion_backlog_item") or {})
        primary_email = str(materialized_record.get("primary_email") or "").strip()
        primary_email_metadata = normalized_primary_email_metadata(materialized_record.get("primary_email_metadata"))
        if primary_email:
            overlay_payload: dict[str, Any] = {
                "primary_email": primary_email,
            }
            if primary_email_metadata:
                overlay_payload["primary_email_metadata"] = primary_email_metadata
            publishable_primary_emails["by_candidate_id"][candidate.candidate_id] = dict(overlay_payload)
            if profile_url_key:
                publishable_primary_emails["by_profile_url_key"][profile_url_key] = dict(overlay_payload)

        if materialized_record.get("experience_lines") or materialized_record.get("education_lines"):
            structured_timeline_count += 1
        if materialized_record.get("experience_lines"):
            structured_experience_count += 1
        if materialized_record.get("education_lines"):
            structured_education_count += 1
        materialized_candidate_records.append(materialized_record)
        normalized_candidates.append(normalized_record)
        reusable_documents.append(reusable_record)
        status_counter[str(normalized_record.get("status_bucket") or "")] += 1
        if normalized_record.get("has_profile_detail"):
            profile_detail_count += 1
        if normalized_record.get("has_explicit_profile_capture"):
            explicit_profile_capture_count += 1
        if normalized_record.get("manual_review_confirmed"):
            manual_review_confirmed_count += 1
        if not normalized_record.get("has_linkedin_url"):
            missing_linkedin_count += 1
        if list(normalized_record.get("function_ids") or []):
            function_id_candidate_count += 1
        if profile_completion_item:
            profile_completion_backlog.append(profile_completion_item)
        if manual_review_item:
            manual_review_backlog.append(manual_review_item)
        candidate_state = {
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "candidate_id": candidate.candidate_id,
            "fingerprint": fingerprint,
            "shard_path": shard_relative_path,
            "list_page": list_page,
            "dirty_reason": "",
            "materialized_at": str(
                shard_payload.get("materialized_at") or previous_state.get("materialized_at") or _utc_now_iso()
            ),
            "metadata": {
                "display_name": candidate.display_name,
                "status_bucket": str(normalized_record.get("status_bucket") or ""),
                "has_profile_detail": bool(normalized_record.get("has_profile_detail")),
                "needs_manual_review": bool(normalized_record.get("needs_manual_review")),
                "needs_profile_completion": bool(normalized_record.get("needs_profile_completion")),
                "profile_url_key": normalize_linkedin_profile_url_key(profile_url) if profile_url else "",
            },
        }
        candidate_states.append(candidate_state)
        if _candidate_materialization_state_requires_upsert(previous_state, candidate_state):
            candidate_state_upserts.append(candidate_state)
    timings_ms["assemble_candidate_outputs"] = _candidate_artifact_elapsed_ms(step_started_at)

    step_started_at = time.perf_counter()
    materialized_documents = {
        "snapshot": {
            "company_key": materialized_view["company_key"],
            "snapshot_id": snapshot_id,
            "snapshot_dir": str(materialized_view["snapshot_dir"]),
            "materialized_at": _utc_now_iso(),
            "materialized_from": "company_snapshot_history",
            "asset_view": asset_view,
            "target_company": materialized_view["target_company"],
            "company_identity": materialized_view["company_identity"],
            "source_snapshots": [
                {
                    "snapshot_id": item["snapshot_id"],
                    "candidate_count": item["candidate_count"],
                    "evidence_count": item["evidence_count"],
                    "source_path": item["source_path"],
                }
                for item in materialized_view["source_snapshots"]
            ],
            "source_snapshot_selection": dict(materialized_view.get("source_snapshot_selection") or {}),
            "control_plane_candidate_count": materialized_view["control_plane_candidate_count"],
            "control_plane_evidence_count": materialized_view["control_plane_evidence_count"],
        },
        "candidates": materialized_candidate_records,
        "evidence": evidence,
    }
    page_payloads: list[dict[str, Any]] = []
    for page_index in range(0, len(normalized_candidates), _CANDIDATE_SHARD_PAGE_SIZE):
        page_number = (page_index // _CANDIDATE_SHARD_PAGE_SIZE) + 1
        page_candidates = normalized_candidates[page_index : page_index + _CANDIDATE_SHARD_PAGE_SIZE]
        page_payloads.append(
            {
                "relative_path": f"pages/page-{page_number:04d}.json",
                "payload": {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": asset_view,
                    "page": page_number,
                    "page_size": _CANDIDATE_SHARD_PAGE_SIZE,
                    "candidate_count": len(page_candidates),
                    "total_candidate_count": len(normalized_candidates),
                    "candidates": page_candidates,
                },
                }
            )
    candidate_shard_entries = [
        {
            "candidate_id": str(state.get("candidate_id") or ""),
            "fingerprint": str(state.get("fingerprint") or ""),
            "path": str(state.get("shard_path") or ""),
            "page": int(state.get("list_page") or 0),
            "display_name": str(dict(state.get("metadata") or {}).get("display_name") or ""),
            "status_bucket": str(dict(state.get("metadata") or {}).get("status_bucket") or ""),
            "has_profile_detail": bool(dict(state.get("metadata") or {}).get("has_profile_detail")),
            "needs_manual_review": bool(dict(state.get("metadata") or {}).get("needs_manual_review")),
            "needs_profile_completion": bool(dict(state.get("metadata") or {}).get("needs_profile_completion")),
        }
        for state in candidate_states
    ]
    page_count = len(page_payloads)
    removed_candidate_count = len(
        [candidate_id for candidate_id in existing_states if candidate_id not in current_candidate_ids_set]
    )
    artifact_summary = {
        "target_company": materialized_view["target_company"],
        "company_key": materialized_view["company_key"],
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "candidate_count": len(candidates),
        "evidence_count": len(evidence),
        "status_counts": dict(status_counter),
        "profile_detail_count": profile_detail_count,
        "explicit_profile_capture_count": explicit_profile_capture_count,
        "manual_review_confirmed_count": manual_review_confirmed_count,
        "missing_linkedin_count": missing_linkedin_count,
        "function_id_candidate_count": function_id_candidate_count,
        "structured_timeline_count": structured_timeline_count,
        "structured_experience_count": structured_experience_count,
        "structured_education_count": structured_education_count,
        "manual_review_backlog_count": len(manual_review_backlog),
        "profile_completion_backlog_count": len(profile_completion_backlog),
        "publishable_primary_email_candidate_count": len(publishable_primary_emails["by_candidate_id"]),
        "source_snapshot_count": len(materialized_view["source_snapshots"]),
        "source_snapshots": [
            {
                "snapshot_id": str(item.get("snapshot_id") or "").strip(),
                "candidate_count": int(item.get("candidate_count") or 0),
                "evidence_count": int(item.get("evidence_count") or 0),
                "source_path": str(item.get("source_path") or "").strip(),
            }
            for item in list(materialized_view.get("source_snapshots") or [])
            if isinstance(item, dict)
        ],
        "source_snapshot_selection_mode": str(
            (materialized_view.get("source_snapshot_selection") or {}).get("mode") or "all_history_snapshots"
        ),
        "source_snapshot_selection": dict(materialized_view.get("source_snapshot_selection") or {}),
        "control_plane_candidate_count": materialized_view["control_plane_candidate_count"],
        "control_plane_evidence_count": materialized_view["control_plane_evidence_count"],
        "candidate_shard_count": len(candidate_states),
        "candidate_page_size": _CANDIDATE_SHARD_PAGE_SIZE,
        "candidate_page_count": page_count,
        "dirty_candidate_count": len(dirty_candidate_shards),
        "reused_candidate_count": max(0, len(candidate_states) - len(dirty_candidate_shards)),
        "state_upsert_candidate_count": len(candidate_state_upserts),
        "removed_candidate_count": removed_candidate_count,
        "created_at": _utc_now_iso(),
    }
    timings_ms["page_manifest_build"] = _candidate_artifact_elapsed_ms(step_started_at)
    timings_ms["payload_build_total"] = _candidate_artifact_elapsed_ms(total_started_at)
    artifact_summary["timings_ms"] = timings_ms
    full_scope_state_replace_eligible = bool(candidate_states) and len(candidate_state_upserts) == len(candidate_states)
    artifact_summary["build_execution"] = {
        "parallel_workers": parallel_workers,
        "parallel_min_candidates": int(parallel_tuning.get("min_candidates") or 1),
        "parallel_worker_cap": int(parallel_tuning.get("max_workers") or parallel_workers),
        "runtime_tuning_profile": str((runtime_tuning_overrides or {}).get("runtime_tuning_profile") or ""),
        "batch_json_writes_enabled": _candidate_artifact_batch_json_writes_enabled(),
        "bulk_state_upsert_enabled": _candidate_artifact_bulk_state_upsert_enabled(),
        "full_scope_state_replace_eligible": full_scope_state_replace_eligible,
    }
    manifest_payload = {
        "target_company": target_company,
        "company_key": str(materialized_view.get("company_key") or ""),
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "materialized_at": _utc_now_iso(),
        "candidate_count": len(candidate_states),
        "pagination": {
            "page_size": _CANDIDATE_SHARD_PAGE_SIZE,
            "page_count": page_count,
        },
        "manual_review_backlog_count": len(manual_review_backlog),
        "profile_completion_backlog_count": len(profile_completion_backlog),
        "dirty_candidate_count": len(dirty_candidate_shards),
        "reused_candidate_count": max(0, len(candidate_states) - len(dirty_candidate_shards)),
        "candidate_shards": candidate_shard_entries,
        "pages": [
            {
                "page": int(dict(item.get("payload") or {}).get("page") or 0),
                "path": str(item.get("relative_path") or ""),
                "candidate_count": int(dict(item.get("payload") or {}).get("candidate_count") or 0),
            }
            for item in page_payloads
        ],
        "backlogs": {
            "manual_review": "backlogs/manual_review.json",
            "profile_completion": "backlogs/profile_completion.json",
        },
        "auxiliary": {
            "publishable_primary_emails": "publishable_primary_emails.json",
        },
        "source_snapshots": [
            {
                "snapshot_id": str(item.get("snapshot_id") or "").strip(),
                "candidate_count": int(item.get("candidate_count") or 0),
                "evidence_count": int(item.get("evidence_count") or 0),
                "source_path": str(item.get("source_path") or "").strip(),
            }
            for item in list(materialized_view.get("source_snapshots") or [])
            if isinstance(item, dict)
        ],
        "source_snapshot_selection": dict(materialized_view.get("source_snapshot_selection") or {}),
        "control_plane_candidate_count": int(materialized_view.get("control_plane_candidate_count") or 0),
        "control_plane_evidence_count": int(materialized_view.get("control_plane_evidence_count") or 0),
    }
    return {
        "materialized_documents": materialized_documents,
        "normalized_candidates": normalized_candidates,
        "reusable_documents": reusable_documents,
        "manual_review_backlog": manual_review_backlog,
        "profile_completion_backlog": profile_completion_backlog,
        "publishable_primary_emails": publishable_primary_emails,
        "artifact_summary": artifact_summary,
        "incremental_artifacts": {
            "active_candidate_ids": active_candidate_ids,
            "candidate_states": candidate_state_upserts,
            "full_scope_state_replace_eligible": full_scope_state_replace_eligible,
            "removed_state_shard_paths": sorted(
                str(dict(existing_states.get(candidate_id) or {}).get("shard_path") or "").strip()
                for candidate_id in existing_states
                if candidate_id not in current_candidate_ids_set
                and str(dict(existing_states.get(candidate_id) or {}).get("shard_path") or "").strip()
            ),
            "dirty_candidate_shards": dirty_candidate_shards,
            "stale_shard_paths": sorted(stale_shard_paths),
            "page_payloads": page_payloads,
            "manifest_payload": manifest_payload,
            "backlog_payloads": {
                "manual_review": {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": asset_view,
                    "candidate_count": len(manual_review_backlog),
                    "candidates": manual_review_backlog,
                },
                "profile_completion": {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": asset_view,
                    "candidate_count": len(profile_completion_backlog),
                    "candidates": profile_completion_backlog,
                },
            },
        },
    }


def _snapshot_materialization_run_id(*, target_company: str, snapshot_id: str, asset_view: str) -> str:
    seed = "|".join([_normalize_key(target_company), snapshot_id, asset_view, _utc_now_iso()])
    return f"materialize_{sha1(seed.encode('utf-8')).hexdigest()[:16]}"


def _candidate_shard_relative_path(candidate_id: str, fingerprint: str) -> str:
    safe_candidate_id = re.sub(r"[^A-Za-z0-9._-]+", "_", str(candidate_id or "").strip()) or "candidate"
    normalized_fingerprint = str(fingerprint or "").strip() or "unknown"
    return f"candidates/{safe_candidate_id}.{normalized_fingerprint}.json"


def _build_candidate_materialization_fingerprint(
    *,
    candidate: Candidate,
    evidence: list[dict[str, Any]],
    profile_registry_row: dict[str, Any],
) -> str:
    fingerprint_payload = {
        "candidate": candidate.to_record(),
        "candidate_source": _path_signature(candidate.source_path),
        "evidence": [
            {
                "evidence_id": str(item.get("evidence_id") or "").strip(),
                "source_type": str(item.get("source_type") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "url": str(item.get("url") or "").strip(),
                "summary": str(item.get("summary") or "").strip(),
                "source_dataset": str(item.get("source_dataset") or "").strip(),
                "source_path": str(item.get("source_path") or "").strip(),
                "metadata": dict(item.get("metadata") or {}),
            }
            for item in sorted(list(evidence or []), key=_evidence_key)
        ],
        "profile_registry": _profile_registry_fingerprint_payload(profile_registry_row),
    }
    return sha1(json.dumps(fingerprint_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:24]


def _profile_registry_fingerprint_payload(profile_registry_row: dict[str, Any]) -> dict[str, Any]:
    raw_path = str(profile_registry_row.get("last_raw_path") or "").strip()
    return {
        "profile_url_key": str(profile_registry_row.get("profile_url_key") or "").strip(),
        "profile_url": str(profile_registry_row.get("profile_url") or "").strip(),
        "status": str(profile_registry_row.get("status") or "").strip(),
        "retry_count": int(profile_registry_row.get("retry_count") or 0),
        "last_error": str(profile_registry_row.get("last_error") or "").strip(),
        "last_snapshot_dir": str(profile_registry_row.get("last_snapshot_dir") or "").strip(),
        "last_raw_path": raw_path,
        "last_raw_path_signature": _path_signature(raw_path),
        "last_fetched_at": str(profile_registry_row.get("last_fetched_at") or "").strip(),
        "last_failed_at": str(profile_registry_row.get("last_failed_at") or "").strip(),
        "source_shards": sorted(_normalize_string_list(profile_registry_row.get("source_shards"))),
        "source_jobs": sorted(_normalize_string_list(profile_registry_row.get("source_jobs"))),
    }


def _path_signature(path_value: str | Path | None) -> dict[str, Any]:
    path_text = str(path_value or "").strip()
    if not path_text:
        return {}
    path = Path(path_text)
    try:
        stat = path.stat()
    except OSError:
        return {
            "path": path_text,
            "exists": False,
        }
    return {
        "path": path_text,
        "exists": True,
        "size_bytes": int(stat.st_size),
        "mtime_ns": int(stat.st_mtime_ns),
    }


def _load_candidate_shard_payload(
    path: Path,
    *,
    candidate_id: str,
    fingerprint: str,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("candidate_id") or "").strip() != str(candidate_id or "").strip():
        return None
    if str(payload.get("fingerprint") or "").strip() != str(fingerprint or "").strip():
        return None
    if not isinstance(payload.get("materialized_candidate"), dict):
        return None
    if not isinstance(payload.get("normalized_candidate"), dict):
        return None
    if not isinstance(payload.get("reusable_document"), dict):
        return None
    return payload


def _build_candidate_shard_payload(
    *,
    candidate: Candidate,
    evidence: list[dict[str, Any]],
    profile_registry_rows: dict[str, dict[str, Any]],
    timeline_cache: dict[str, dict[str, Any]],
    target_company: str,
    snapshot_id: str,
    asset_view: str,
    fingerprint: str,
) -> dict[str, Any]:
    timeline = _resolve_materialized_candidate_timeline(
        candidate=candidate,
        normalized={"source_path": candidate.source_path},
        registry_rows=profile_registry_rows,
        timeline_cache=timeline_cache,
    )
    enriched_candidate = _candidate_with_resolved_profile_timeline(candidate, timeline)
    normalized = _normalize_candidate(enriched_candidate, evidence)
    if _timeline_contains_profile_detail(timeline):
        normalized["has_profile_detail"] = True
        normalized["needs_profile_completion"] = False
    materialized_record = _build_materialized_candidate_record(enriched_candidate, normalized, timeline)
    normalized_record = _build_normalized_candidate_record(normalized, timeline)
    reusable_record = _build_reusable_document(enriched_candidate, evidence, normalized, timeline)
    manual_review_item: dict[str, Any] = {}
    profile_completion_item: dict[str, Any] = {}
    if normalized["needs_manual_review"]:
        manual_review_item = {
            "candidate_id": candidate.candidate_id,
            "display_name": candidate.display_name,
            "status_bucket": normalized["status_bucket"],
            "category": candidate.category,
            "employment_status": candidate.employment_status,
            "reason": normalized["manual_review_reason"],
            "known_urls": normalized["urls"],
            "source_datasets": normalized["source_datasets"],
        }
    if normalized["needs_profile_completion"]:
        profile_completion_item = {
            "candidate_id": candidate.candidate_id,
            "display_name": candidate.display_name,
            "status_bucket": normalized["status_bucket"],
            "category": candidate.category,
            "employment_status": candidate.employment_status,
            "reason": "profile_detail_gap",
            "linkedin_url": candidate.linkedin_url,
            "source_datasets": normalized["source_datasets"],
        }
    return {
        "candidate_id": candidate.candidate_id,
        "fingerprint": fingerprint,
        "target_company": target_company,
        "snapshot_id": snapshot_id,
        "asset_view": asset_view,
        "materialized_at": "",
        "materialized_candidate": materialized_record,
        "normalized_candidate": normalized_record,
        "reusable_document": reusable_record,
        "evidence": evidence,
        "manual_review_backlog_item": manual_review_item,
        "profile_completion_backlog_item": profile_completion_item,
    }


def _is_strict_roster_candidate(candidate: Candidate, evidence: list[dict[str, Any]]) -> bool:
    metadata = dict(candidate.metadata or {})
    if candidate.category not in {"employee", "former_employee", "investor"}:
        return False
    if bool(metadata.get("membership_review_required")):
        return False
    if bool(metadata.get("target_company_mismatch")):
        return False
    membership_decision = str(metadata.get("membership_review_decision") or "").strip().lower()
    if membership_decision.endswith("non_member"):
        return False
    if _has_explicit_manual_review_resolution(candidate):
        return True
    explicit_profile_capture = _has_explicit_profile_capture(evidence)
    functional_facets = derive_candidate_facets(candidate)
    source_tokens = " ".join(
        [
            str(candidate.source_dataset or "").strip().lower(),
            " ".join(str(item.get("source_dataset") or "").strip().lower() for item in evidence),
            " ".join(str(item.get("source_type") or "").strip().lower() for item in evidence),
        ]
    )
    if any(
        token in source_tokens
        for token in [
            "linkedin_company_people",
            "company_employees",
            "company_roster",
            "roster",
            "harvest_profile_search",
            "search_seed",
            "profile_detail",
            "manual_review",
        ]
    ):
        if not functional_facets:
            return False
        return explicit_profile_capture or bool(str(candidate.linkedin_url or "").strip())
    if explicit_profile_capture and functional_facets:
        return True
    return False


def _resolve_company_snapshot(
    runtime_dir: str | Path,
    target_company: str,
    *,
    snapshot_id: str = "",
    prefer_hot_cache: bool = True,
) -> tuple[str, Path, dict[str, Any]]:
    selection = resolve_company_snapshot_match_selection(
        runtime_dir,
        target_company=target_company,
        snapshot_id=snapshot_id,
        prefer_hot_cache=prefer_hot_cache,
    )
    if selection is None:
        raise CandidateArtifactError(f"Company assets not found for {target_company}")
    company_dir = Path(selection["company_dir"])
    latest_payload = dict(selection.get("latest_payload") or {})
    snapshot_dir = Path(selection["snapshot_dir"])
    if not snapshot_dir.exists():
        raise CandidateArtifactError(f"Snapshot not found: {snapshot_dir}")
    identity_payload = dict(selection.get("identity_payload") or {}) or load_company_snapshot_identity(
        snapshot_dir, fallback_payload=latest_payload
    )
    resolved_company_key = (
        str(identity_payload.get("company_key") or selection.get("company_key") or company_dir.name).strip()
        or company_dir.name
    )
    return resolved_company_key, snapshot_dir, identity_payload


def _status_bucket(candidate: Candidate, *, membership_review_required: bool = False) -> str:
    if membership_review_required:
        return "lead"
    status = str(candidate.employment_status or "").strip().lower()
    category = str(candidate.category or "").strip().lower()
    if status == "current":
        return "current"
    if status == "former":
        return "former"
    if category == "lead":
        return "lead"
    return category or "unknown"


def _has_explicit_profile_capture(evidence: list[dict[str, Any]]) -> bool:
    return any(
        str(item.get("source_type") or "").strip()
        in {"linkedin_profile_detail", "manual_review_analysis", "linkedin_profile_non_member"}
        for item in evidence
    )


def _has_reusable_profile_fields(candidate: Candidate) -> bool:
    candidate_record = candidate.to_record()
    metadata = dict(candidate.metadata or {})
    timeline_like = {
        "experience_lines": metadata.get("experience_lines") or candidate_record.get("experience_lines"),
        "education_lines": metadata.get("education_lines") or candidate_record.get("education_lines"),
        "headline": metadata.get("headline") or candidate_record.get("headline"),
        "summary": metadata.get("summary") or candidate_record.get("summary"),
        "about": metadata.get("about") or candidate_record.get("about"),
        "primary_email": metadata.get("primary_email")
        or metadata.get("email")
        or candidate_record.get("primary_email"),
        "languages": metadata.get("languages") or candidate_record.get("languages"),
        "skills": metadata.get("skills") or candidate_record.get("skills"),
        "profile_capture_kind": metadata.get("profile_capture_kind") or candidate_record.get("profile_capture_kind"),
        "profile_capture_source_path": metadata.get("profile_capture_source_path")
        or candidate_record.get("profile_capture_source_path"),
        "source_path": metadata.get("profile_timeline_source_path")
        or metadata.get("source_path")
        or str(candidate.source_path or ""),
    }
    if timeline_has_complete_profile_detail(timeline_like):
        return True
    if str(candidate.education or "").strip():
        return True
    work_history = str(candidate.work_history or "").strip()
    if not work_history:
        return False
    if _is_roster_baseline_only(candidate):
        return False
    return True


def _is_roster_baseline_only(candidate: Candidate) -> bool:
    source_dataset = str(candidate.source_dataset or "").strip()
    role = str(candidate.role or "").strip()
    work_history = str(candidate.work_history or "").strip()
    notes = str(candidate.notes or "")
    return (
        source_dataset.endswith("_linkedin_company_people")
        and bool(work_history)
        and work_history == role
        and "LinkedIn company roster baseline." in notes
        and not str(candidate.education or "").strip()
    )


def _merge_resolved_profile_signals_into_metadata(
    metadata: dict[str, Any],
    timeline: dict[str, Any],
) -> None:
    if _resolved_timeline_requires_primary_email_scrub(timeline):
        metadata.pop("primary_email", None)
        metadata.pop("email", None)
        metadata.pop("primary_email_metadata", None)
    for key in _PROFILE_SIGNAL_METADATA_SCALAR_KEYS:
        value = str(timeline.get(key) or "").strip()
        if value and not str(metadata.get(key) or "").strip():
            metadata[key] = value
    email_metadata = normalized_primary_email_metadata(timeline.get("primary_email_metadata"))
    if email_metadata and str(timeline.get("primary_email") or "").strip():
        metadata["primary_email_metadata"] = email_metadata
    for key in _PROFILE_SIGNAL_LIST_KEYS:
        values = normalized_text_lines(timeline.get(key))
        if values and not normalized_text_lines(metadata.get(key)):
            metadata[key] = values


def _project_profile_signal_fields_into_record(
    record: dict[str, Any],
    *,
    signal_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    projected = dict(record)
    metadata = dict(projected.get("metadata") or {})
    source_payload = dict(signal_payload or {})
    for key in ("experience_lines", "education_lines"):
        values = normalized_text_lines(projected.get(key))
        if not values:
            values = normalized_text_lines(source_payload.get(key))
        if not values:
            values = normalized_text_lines(metadata.get(key))
        if values:
            projected[key] = values
    for key in _PROFILE_SIGNAL_METADATA_SCALAR_KEYS:
        value = str(projected.get(key) or "").strip()
        if not value:
            value = str(source_payload.get(key) or "").strip()
        if not value:
            value = str(metadata.get(key) or "").strip()
        if value:
            projected[key] = value
    primary_email_metadata = normalized_primary_email_metadata(projected.get("primary_email_metadata"))
    if not primary_email_metadata:
        primary_email_metadata = normalized_primary_email_metadata(source_payload.get("primary_email_metadata"))
    if not primary_email_metadata:
        primary_email_metadata = normalized_primary_email_metadata(metadata.get("primary_email_metadata"))
    if primary_email_metadata and str(projected.get("primary_email") or "").strip():
        projected["primary_email_metadata"] = primary_email_metadata
    elif not str(projected.get("primary_email") or "").strip():
        projected.pop("primary_email_metadata", None)
    for key in _PROFILE_SIGNAL_LIST_KEYS:
        values = normalized_text_lines(projected.get(key))
        if not values:
            values = normalized_text_lines(source_payload.get(key))
        if not values:
            values = normalized_text_lines(metadata.get(key))
        if values:
            projected[key] = values
    if not str(projected.get("media_url") or "").strip():
        media_url = _resolved_profile_media_url(metadata, projected)
        if media_url:
            projected["media_url"] = media_url
    return projected


def _extract_projectable_profile_signals(record: dict[str, Any]) -> dict[str, Any]:
    projected: dict[str, Any] = {}
    for key in ("experience_lines", "education_lines", *_PROFILE_SIGNAL_METADATA_SCALAR_KEYS):
        value = record.get(key)
        if key in {"experience_lines", "education_lines"}:
            normalized = normalized_text_lines(value)
            if normalized:
                projected[key] = normalized
            continue
        text_value = str(value or "").strip()
        if text_value:
            projected[key] = text_value
    primary_email_metadata = normalized_primary_email_metadata(record.get("primary_email_metadata"))
    if primary_email_metadata and str(projected.get("primary_email") or "").strip():
        projected["primary_email_metadata"] = primary_email_metadata
    for key in _PROFILE_SIGNAL_LIST_KEYS:
        values = normalized_text_lines(record.get(key))
        if values:
            projected[key] = values
    media_url = str(record.get("media_url") or "").strip()
    if media_url:
        projected["media_url"] = media_url
    return projected


def _merge_resolved_profile_signals_into_record(
    record: dict[str, Any],
    timeline: dict[str, Any],
) -> None:
    if _resolved_timeline_requires_primary_email_scrub(timeline):
        record.pop("primary_email", None)
        record.pop("email", None)
        record.pop("primary_email_metadata", None)
    for key in _PROFILE_SIGNAL_METADATA_SCALAR_KEYS:
        value = str(timeline.get(key) or "").strip()
        if value and not str(record.get(key) or "").strip():
            record[key] = value
    email_metadata = normalized_primary_email_metadata(timeline.get("primary_email_metadata"))
    if email_metadata and str(timeline.get("primary_email") or "").strip():
        record["primary_email_metadata"] = email_metadata
    for key in _PROFILE_SIGNAL_LIST_KEYS:
        values = normalized_text_lines(timeline.get(key))
        if values and not normalized_text_lines(record.get(key)):
            record[key] = values


def _resolved_timeline_requires_primary_email_scrub(timeline: dict[str, Any]) -> bool:
    return primary_email_requires_suppression(timeline)


def _resolved_profile_media_url(metadata: dict[str, Any], timeline: dict[str, Any]) -> str:
    for value in (
        timeline.get("avatar_url"),
        timeline.get("photo_url"),
        metadata.get("avatar_url"),
        metadata.get("photo_url"),
    ):
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""


def _timeline_contains_profile_detail(timeline: dict[str, Any]) -> bool:
    return timeline_has_complete_profile_detail(timeline)


def _dedupe_urls(items: list[str]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        results.append(value)
    return results


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
