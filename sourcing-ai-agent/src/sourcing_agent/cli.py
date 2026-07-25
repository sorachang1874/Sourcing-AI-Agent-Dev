from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from . import cli_parsers
from urllib import error as urllib_error
from urllib import request as urllib_request

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .api import create_server
from .artifact_cache import run_hot_cache_governance_cycle
from .asset_catalog import AssetCatalog
from .asset_coverage_backfill import backfill_authoritative_population_coverage
from .asset_reuse_audit import (
    audit_authoritative_reuse_planning_many,
    audit_authoritative_reuse_planning_matrix,
    compare_authoritative_reuse_planning_matrix_reports,
)
from .asset_reuse_planning import backfill_organization_asset_registry_for_company
from .asset_sync import AssetBundleManager
from .authoritative_serving_repair import repair_authoritative_serving_generation
from .authoritative_source_provenance import normalize_authoritative_source_provenance
from .candidate_artifacts import (
    audit_candidate_artifact_hot_cache,
    backfill_structured_timeline_for_company_assets,
    build_company_candidate_artifacts,
    cleanup_candidate_artifact_hot_cache,
    repair_missing_company_candidate_artifacts,
    repair_paginated_candidate_artifacts_from_materialized,
    repair_projected_profile_signals_in_company_candidate_artifacts,
)
from .cloud_asset_import import hydrate_cloud_generation, import_cloud_assets
from .company_asset_completion import CompanyAssetCompletionManager
from .company_asset_supplement import CompanyAssetSupplementManager
from .company_registry import normalize_company_key
from .control_plane_postgres import (
    export_control_plane_snapshot,
    sync_control_plane_snapshot_to_postgres,
    sync_runtime_control_plane_to_postgres,
)
from .job_result_lifecycle_backfill import backfill_job_result_lifecycle
from .local_postgres import describe_control_plane_runtime
from .model_provider import OpenAICompatibleChatModelClient, QwenResponsesModelClient, build_model_client
from .object_storage import build_object_storage_client
from .orchestrator import (
    SourcingOrchestrator,
    _runner_subprocess_env,
    _spawn_detached_process,
    _workflow_runner_process_alive,
)
from .outreach_layering import analyze_company_outreach_layers
from .profile_registry_backfill import backfill_linkedin_profile_registry
from .public_web_quality import evaluate_public_web_quality_paths, write_public_web_quality_report
from .runtime_rebuild import rebuild_runtime_control_plane
from .search_provider import build_search_provider
from .semantic_provider import build_semantic_provider
from .service_daemon import SingleInstanceError, WorkerDaemonService, read_service_status
from .settings import load_settings
from .snapshot_materialization_backfill import backfill_snapshot_full_materialization_items
from .storage import ControlPlaneStore, _json_safe_payload
from .workflow_submission import (
    normalize_workflow_submission_payload,
    workflow_runtime_uses_managed_runner,
)


class HostedWorkflowSubmissionError(RuntimeError):
    pass


def _runner_environment(project_root: Path) -> dict[str, str]:
    return _runner_subprocess_env(project_root)


def _resolve_launch_path(project_root: Path, raw_path: str, *, default_name: str = "") -> Path:
    candidate = str(raw_path or "").strip()
    if candidate:
        path = Path(candidate).expanduser()
        if not path.is_absolute():
            path = project_root / path
        return path.resolve()
    if not default_name:
        raise ValueError("launch path is required")
    return (project_root / "runtime" / "service_logs" / default_name).resolve()


def launch_detached_command(
    *,
    command: list[str],
    log_path: str,
    cwd: str = "",
    description: str = "",
    startup_wait_seconds: float = 0.15,
    status_path: str = "",
) -> dict[str, Any]:
    if not command:
        raise ValueError("launch_detached_command requires a non-empty command")
    catalog = AssetCatalog.discover()
    project_root = catalog.project_root
    runner_env = _runner_environment(project_root)
    resolved_cwd = _resolve_launch_path(project_root, cwd) if str(cwd or "").strip() else project_root.resolve()
    resolved_log_path = _resolve_launch_path(project_root, log_path, default_name="detached-launch.log")
    result = _spawn_detached_process(
        command=command,
        cwd=resolved_cwd,
        log_path=resolved_log_path,
        env=runner_env,
        startup_wait_seconds=float(startup_wait_seconds or 0.15),
    )
    payload: dict[str, Any] = {
        "description": str(description or "").strip(),
        "cwd": str(resolved_cwd),
        "launched_at": datetime.now(timezone.utc).isoformat(),
        **result,
    }
    resolved_status_path = None
    if str(status_path or "").strip():
        resolved_status_path = _resolve_launch_path(project_root, status_path)
        resolved_status_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload["status_path"] = str(resolved_status_path)
    return payload


def build_runtime_store(settings) -> ControlPlaneStore:
    store = ControlPlaneStore(settings.db_path)
    # CLI/serve entry points must not expose a fresh, unmigrated schema to
    # daemon readers that run before any lazy bootstrap: apply the versioned
    # migration ledger at construction (idempotent), mirroring the
    # isolated-test-runtime prepare path.
    store._control_plane_postgres.ensure_bootstrapped()  # noqa: SLF001
    return store


def build_control_plane_runtime_summary() -> dict[str, Any]:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    summary = describe_control_plane_runtime(
        base_dir=catalog.project_root,
        runtime_dir=settings.runtime_dir,
    )
    store = build_runtime_store(settings)
    compatibility_shadow_ephemeral = store.compatibility_shadow_is_ephemeral()
    settings_db_path_role = (
        "compatibility_shadow_seed_path" if compatibility_shadow_ephemeral else "disk_live_control_plane_path"
    )
    summary.update(
        {
            "project_root": str(catalog.project_root),
            "settings_db_path": str(settings.db_path),
            "settings_db_path_exists": settings.db_path.exists(),
            "settings_db_path_role": settings_db_path_role,
            "settings_db_path_is_live_authoritative": not compatibility_shadow_ephemeral,
            "control_plane_postgres_live_mode": store.control_plane_postgres_live_mode(),
            "compatibility_shadow_backend": store.compatibility_shadow_backend(),
            "compatibility_shadow_connect_target": store.compatibility_shadow_connect_target(),
            "compatibility_shadow_ephemeral": compatibility_shadow_ephemeral,
            "sqlite_shadow_backend": store.sqlite_shadow_backend(),
            "sqlite_shadow_connect_target": store.sqlite_shadow_connect_target(),
            "sqlite_shadow_ephemeral": store.sqlite_shadow_is_ephemeral(),
            "bootstrap_candidate_store_enabled": store.bootstrap_candidate_store_enabled(),
            "candidate_documents_fallback_enabled": store.candidate_documents_fallback_enabled(),
        }
    )
    summary["control_plane_storage_banner"] = _control_plane_storage_banner(summary)
    return summary


def _load_cli_json_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _serving_artifact_dir(
    *,
    runtime_dir: str | Path,
    company_key: str,
    snapshot_id: str,
    asset_view: str,
    source_path: str = "",
) -> Path:
    source_path_text = str(source_path or "").strip()
    if source_path_text:
        source = Path(source_path_text).expanduser()
        if source.name == "artifact_summary.json":
            return source.parent
        if source.is_dir():
            return source
    normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    artifact_dir = Path(runtime_dir) / "company_assets" / company_key / snapshot_id / "normalized_artifacts"
    if normalized_asset_view != "canonical_merged":
        artifact_dir = artifact_dir / normalized_asset_view
    return artifact_dir


def _resolve_company_serving_snapshot(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company: str,
    snapshot_id: str,
    asset_view: str,
) -> dict[str, Any]:
    normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
    registry = store.get_authoritative_organization_asset_registry(
        target_company=company,
        asset_view=normalized_asset_view,
    )
    company_key = str(registry.get("company_key") or normalize_company_key(company)).strip()
    resolved_snapshot_id = str(snapshot_id or registry.get("snapshot_id") or "").strip()
    if not resolved_snapshot_id:
        latest_path = Path(runtime_dir) / "company_assets" / company_key / "latest_snapshot.json"
        latest_payload = _load_cli_json_payload(latest_path)
        resolved_snapshot_id = str(latest_payload.get("snapshot_id") or "").strip()
    artifact_dir = _serving_artifact_dir(
        runtime_dir=runtime_dir,
        company_key=company_key,
        snapshot_id=resolved_snapshot_id,
        asset_view=normalized_asset_view,
        source_path=str(registry.get("source_path") or ""),
    )
    return {
        "company": company,
        "company_key": company_key,
        "snapshot_id": resolved_snapshot_id,
        "asset_view": normalized_asset_view,
        "registry": registry,
        "artifact_dir": artifact_dir,
    }


def _summarize_serving_page_provenance(
    *,
    artifact_dir: Path,
    manifest: dict[str, Any],
    sample_pages: int,
) -> dict[str, Any]:
    pages = list(manifest.get("pages") or [])
    if sample_pages > 0:
        pages = pages[:sample_pages]
    candidate_rows_scanned = 0
    source_matches_records = 0
    matched_keywords_records = 0
    candidate_rows_with_source_matches = 0
    candidate_rows_with_matched_keywords = 0
    multi_source_match_records = 0
    sampled_keywords: set[str] = set()
    for page in pages:
        relative_path = str(dict(page or {}).get("path") or "").strip()
        if not relative_path:
            continue
        payload = _load_cli_json_payload(artifact_dir / relative_path)
        for candidate in list(payload.get("candidates") or []):
            if not isinstance(candidate, dict):
                continue
            candidate_rows_scanned += 1
            source_matches = [item for item in list(candidate.get("source_matches") or []) if isinstance(item, dict)]
            matched_keywords = [str(item).strip() for item in list(candidate.get("matched_keywords") or []) if str(item).strip()]
            if source_matches:
                candidate_rows_with_source_matches += 1
                source_matches_records += len(source_matches)
                if len(source_matches) > 1:
                    multi_source_match_records += 1
                for item in source_matches:
                    for keyword in list(item.get("matched_keywords") or []):
                        keyword_text = str(keyword or "").strip()
                        if keyword_text and len(sampled_keywords) < 20:
                            sampled_keywords.add(keyword_text)
            if matched_keywords:
                candidate_rows_with_matched_keywords += 1
                matched_keywords_records += len(matched_keywords)
                for keyword_text in matched_keywords:
                    if len(sampled_keywords) < 20:
                        sampled_keywords.add(keyword_text)
    return {
        "pages_scanned": len(pages),
        "candidate_rows_scanned": candidate_rows_scanned,
        "source_matches_records": source_matches_records,
        "matched_keywords_records": matched_keywords_records,
        "candidate_rows_with_source_matches": candidate_rows_with_source_matches,
        "candidate_rows_with_matched_keywords": candidate_rows_with_matched_keywords,
        "multi_source_match_candidate_rows": multi_source_match_records,
        "sampled_keywords": sorted(sampled_keywords),
    }


def _sample_candidate_shard_projection_version(*, artifact_dir: Path, manifest: dict[str, Any]) -> str:
    for shard in list(manifest.get("candidate_shards") or [])[:5]:
        relative_path = str(dict(shard or {}).get("path") or "").strip()
        if not relative_path:
            continue
        payload = _load_cli_json_payload(artifact_dir / relative_path)
        projection_version = str(payload.get("projection_version") or "").strip()
        if projection_version:
            return projection_version
    return ""


def audit_company_serving_view(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    company: str,
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    job_id: str = "",
    sample_pages: int = 2,
) -> dict[str, Any]:
    resolved = _resolve_company_serving_snapshot(
        runtime_dir=runtime_dir,
        store=store,
        company=company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
    )
    artifact_dir = Path(resolved["artifact_dir"])
    manifest_path = artifact_dir / "manifest.json"
    summary_path = artifact_dir / "artifact_summary.json"
    manifest = _load_cli_json_payload(manifest_path)
    artifact_summary = _load_cli_json_payload(summary_path)
    registry = dict(resolved.get("registry") or {})
    job_result_view = store.get_job_result_view(job_id=job_id) if str(job_id or "").strip() else None
    selected_snapshot_id = str(resolved.get("snapshot_id") or "").strip()
    page_count = int(dict(manifest.get("pagination") or {}).get("page_count") or len(list(manifest.get("pages") or [])))
    sampled_shard_projection_version = _sample_candidate_shard_projection_version(
        artifact_dir=artifact_dir,
        manifest=manifest,
    )
    artifact_projection_version = str(
        artifact_summary.get("projection_version")
        or manifest.get("projection_version")
        or sampled_shard_projection_version
        or ""
    )
    provenance = (
        _summarize_serving_page_provenance(
            artifact_dir=artifact_dir,
            manifest=manifest,
            sample_pages=max(0, int(sample_pages or 0)),
        )
        if manifest
        else {
            "pages_scanned": 0,
            "candidate_rows_scanned": 0,
            "source_matches_records": 0,
            "matched_keywords_records": 0,
            "candidate_rows_with_source_matches": 0,
            "candidate_rows_with_matched_keywords": 0,
            "multi_source_match_candidate_rows": 0,
            "sampled_keywords": [],
        }
    )
    drift: dict[str, Any] = {}
    if job_result_view:
        job_snapshot_id = str(job_result_view.get("snapshot_id") or "").strip()
        job_asset_view = str(job_result_view.get("asset_view") or "").strip() or "canonical_merged"
        drift = {
            "job_points_to_selected_snapshot": bool(job_snapshot_id and job_snapshot_id == selected_snapshot_id),
            "job_points_to_selected_asset_view": job_asset_view == str(resolved.get("asset_view") or ""),
            "job_snapshot_id": job_snapshot_id,
            "selected_snapshot_id": selected_snapshot_id,
            "policy_required_for_repoint": not bool(job_snapshot_id and job_snapshot_id == selected_snapshot_id),
        }
    return {
        "status": "ok" if manifest_path.exists() and summary_path.exists() else "missing_artifact",
        "target_company": company,
        "company_key": str(resolved.get("company_key") or ""),
        "snapshot_id": selected_snapshot_id,
        "asset_view": str(resolved.get("asset_view") or ""),
        "paths": {
            "artifact_dir": str(artifact_dir),
            "manifest": str(manifest_path),
            "artifact_summary": str(summary_path),
        },
        "artifact": {
            "manifest_exists": manifest_path.exists(),
            "artifact_summary_exists": summary_path.exists(),
            "candidate_count": int(
                artifact_summary.get("candidate_count") or manifest.get("candidate_count") or 0
            ),
            "page_count": page_count,
            "candidate_shard_count": int(
                artifact_summary.get("candidate_shard_count")
                or len(list(manifest.get("candidate_shards") or []))
                or 0
            ),
            "build_profile": str(artifact_summary.get("build_profile") or manifest.get("build_profile") or ""),
            "projection_version": artifact_projection_version,
            "projection_version_source": (
                "artifact_summary"
                if str(artifact_summary.get("projection_version") or "").strip()
                else "manifest"
                if str(manifest.get("projection_version") or "").strip()
                else "candidate_shard_sample"
                if sampled_shard_projection_version
                else ""
            ),
            "materialization_generation_key": str(
                artifact_summary.get("materialization_generation_key")
                or manifest.get("materialization_generation_key")
                or ""
            ),
            "materialization_generation_sequence": int(
                artifact_summary.get("materialization_generation_sequence")
                or manifest.get("materialization_generation_sequence")
                or 0
            ),
            "source_snapshot_selection": dict(
                artifact_summary.get("source_snapshot_selection")
                or manifest.get("source_snapshot_selection")
                or {}
            ),
            "timings_ms": dict(artifact_summary.get("timings_ms") or {}),
        },
        "source_provenance": provenance,
        "registry": registry,
        "job_result_view": job_result_view or {},
        "drift": drift,
        "repoint_policy": {
            "historical_replay": "keep existing job_result_view; do not move old history silently",
            "serve_latest_company_asset": "explicitly repoint job_result_view to selected company serving asset",
        },
    }


def repoint_job_result_view(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    job_id: str,
    company: str = "",
    snapshot_id: str = "",
    asset_view: str = "canonical_merged",
    policy: str = "historical_replay",
    reason: str = "",
    apply: bool = False,
) -> dict[str, Any]:
    normalized_policy = str(policy or "historical_replay").strip().lower()
    if normalized_policy not in {"historical_replay", "serve_latest_company_asset"}:
        raise ValueError("policy must be historical_replay or serve_latest_company_asset")
    current_view = store.get_job_result_view(job_id=job_id) or {}
    target_company = str(company or current_view.get("target_company") or "").strip()
    if not target_company:
        raise ValueError("company is required when the job has no existing result view")
    if normalized_policy == "historical_replay":
        return {
            "status": "noop",
            "applied": False,
            "policy": normalized_policy,
            "reason": str(reason or "").strip(),
            "job_id": job_id,
            "current_view": current_view,
            "message": "Historical replay keeps the existing job_result_view pointer.",
        }
    audit = audit_company_serving_view(
        runtime_dir=runtime_dir,
        store=store,
        company=target_company,
        snapshot_id=snapshot_id,
        asset_view=asset_view,
        job_id=job_id,
        sample_pages=1,
    )
    registry = dict(audit.get("registry") or {})
    artifact = dict(audit.get("artifact") or {})
    paths = dict(audit.get("paths") or {})
    selected_snapshot_id = str(audit.get("snapshot_id") or "").strip()
    if not selected_snapshot_id:
        raise ValueError("snapshot_id could not be resolved")
    next_payload = {
        "job_id": job_id,
        "target_company": target_company,
        "source_kind": "company_snapshot",
        "view_kind": "asset_population",
        "snapshot_id": selected_snapshot_id,
        "asset_view": str(audit.get("asset_view") or asset_view or "canonical_merged"),
        "source_path": str(registry.get("source_path") or paths.get("artifact_summary") or ""),
        "authoritative_snapshot_id": str(registry.get("snapshot_id") or selected_snapshot_id),
        "materialization_generation_key": str(
            registry.get("materialization_generation_key")
            or artifact.get("materialization_generation_key")
            or ""
        ),
        "summary": {
            "candidate_count": int(artifact.get("candidate_count") or 0),
            "page_count": int(artifact.get("page_count") or 0),
            "build_profile": str(artifact.get("build_profile") or ""),
            "projection_version": str(artifact.get("projection_version") or ""),
            "repoint_policy": normalized_policy,
            "repoint_reason": str(reason or "").strip(),
        },
        "metadata": {
            "repoint_policy": normalized_policy,
            "repoint_reason": str(reason or "").strip(),
            "previous_view": current_view,
        },
    }
    if not apply:
        return {
            "status": "dry_run",
            "applied": False,
            "policy": normalized_policy,
            "job_id": job_id,
            "current_view": current_view,
            "next_view": next_payload,
            "audit": audit,
        }
    updated_view = store.upsert_job_result_view(**next_payload)
    return {
        "status": "updated",
        "applied": True,
        "policy": normalized_policy,
        "job_id": job_id,
        "previous_view": current_view,
        "updated_view": updated_view,
        "audit": audit,
    }


def _cli_first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = " ".join(str(value or "").split()).strip()
        if text:
            return text
    return ""


_CLI_RECOVERY_LARGE_LIST_KEYS = {
    "accepted_candidate_ids",
    "board_visible_candidate_ids",
    "candidate_identity_keys",
    "candidate_ids",
    "completed_urls",
    "cumulative_candidate_ids",
    "deferred_urls",
    "delta_candidate_ids",
    "linkedin_urls",
    "member_keys",
    "missing_candidate_ids",
    "patch_candidate_ids",
    "profile_urls",
    "queued_urls",
    "requested_candidate_ids",
    "skipped_candidate_ids",
    "source_profile_urls",
    "urls",
}


def _compact_cli_recovery_payload(value: Any, *, parent_key: str = "", max_items: int = 24) -> Any:
    """Keep recovery CLI output operator-readable for large pressure runs.

    The durable recovery payload can legitimately contain thousands of
    candidate/profile ids. The CLI is a diagnostic surface, not a replay
    artifact, so it should expose cardinality and a sample instead of dumping
    full leaf arrays into the terminal.
    """

    if isinstance(value, dict):
        return {
            str(key): _compact_cli_recovery_payload(item, parent_key=str(key), max_items=max_items)
            for key, item in value.items()
        }
    if isinstance(value, list):
        scalar_list = all(not isinstance(item, (dict, list, tuple, set)) for item in value)
        should_compact = parent_key in _CLI_RECOVERY_LARGE_LIST_KEYS or scalar_list
        if should_compact and len(value) > max_items:
            sample = [
                _compact_cli_recovery_payload(item, parent_key=parent_key, max_items=max_items)
                for item in value[:max_items]
            ]
            return {
                "count": len(value),
                "sample": sample,
                "truncated": True,
                "omitted_count": max(0, len(value) - len(sample)),
            }
        return [_compact_cli_recovery_payload(item, parent_key=parent_key, max_items=max_items) for item in value]
    return value


def _job_summary_candidate_source(job: dict[str, Any]) -> dict[str, Any]:
    summary = dict(job.get("summary") or {})
    candidate_source = summary.get("candidate_source")
    if isinstance(candidate_source, dict):
        return dict(candidate_source)
    asset_population = summary.get("asset_population")
    if isinstance(asset_population, dict):
        nested_source = dict(asset_population).get("candidate_source")
        if isinstance(nested_source, dict):
            return dict(nested_source)
    return {}


def _job_result_view_has_overlay(view: dict[str, Any], candidate_source: dict[str, Any]) -> bool:
    payloads = [
        dict(candidate_source or {}),
        dict(view or {}),
        dict(dict(view or {}).get("metadata") or {}),
        dict(dict(candidate_source or {}).get("result_view_metadata") or {}),
    ]
    for payload in payloads:
        if str(payload.get("asset_population_overlay_path") or "").strip():
            return True
        if str(payload.get("mode") or "").strip().lower() == "job_asset_population_overlay":
            return True
    return False


def _job_result_view_auto_repoint_eligibility(
    *,
    job: dict[str, Any],
    view: dict[str, Any],
    candidate_source: dict[str, Any],
) -> dict[str, Any]:
    request = dict(job.get("request") or {})
    plan = dict(job.get("plan") or {})
    asset_reuse_plan = dict(plan.get("asset_reuse_plan") or {})
    execution_semantics = dict(
        plan.get("effective_execution_semantics")
        or plan.get("execution_semantics")
        or plan.get("organization_execution_profile")
        or {}
    )
    planner_mode = str(asset_reuse_plan.get("planner_mode") or plan.get("planner_mode") or "").strip()
    target_scope = str(request.get("target_scope") or "").strip().lower()
    keywords = [item for item in list(request.get("keywords") or []) if str(item or "").strip()]
    has_overlay = _job_result_view_has_overlay(view, candidate_source)
    source_kind = str(view.get("source_kind") or candidate_source.get("source_kind") or "").strip()
    view_kind = str(view.get("view_kind") or "").strip()
    full_local_reuse = bool(
        execution_semantics.get("full_local_asset_reuse")
        or asset_reuse_plan.get("full_local_asset_reuse")
        or planner_mode == "reuse_snapshot_only"
    )
    if has_overlay:
        return {
            "eligible": False,
            "classification": "scoped_overlay",
            "blocked_reason": "job uses a job-scoped asset population overlay",
            "planner_mode": planner_mode,
            "target_scope": target_scope,
        }
    if planner_mode == "delta_from_snapshot":
        return {
            "eligible": False,
            "classification": "scoped_delta",
            "blocked_reason": "delta_from_snapshot jobs require operator review before repoint",
            "planner_mode": planner_mode,
            "target_scope": target_scope,
        }
    if target_scope and target_scope != "full_company_asset":
        return {
            "eligible": False,
            "classification": "non_full_company_request",
            "blocked_reason": "request target_scope is not full_company_asset",
            "planner_mode": planner_mode,
            "target_scope": target_scope,
        }
    if keywords and not full_local_reuse:
        return {
            "eligible": False,
            "classification": "thematic_request_without_full_reuse_proof",
            "blocked_reason": "keyword-scoped job lacks reuse_snapshot_only/full_local_asset_reuse proof",
            "planner_mode": planner_mode,
            "target_scope": target_scope,
        }
    if full_local_reuse:
        return {
            "eligible": True,
            "classification": "full_local_asset_reuse",
            "blocked_reason": "",
            "planner_mode": planner_mode,
            "target_scope": target_scope,
        }
    if (
        (not target_scope or target_scope == "full_company_asset")
        and source_kind == "company_snapshot"
        and (not view_kind or view_kind == "asset_population")
        and not keywords
    ):
        return {
            "eligible": True,
            "classification": "legacy_full_company_asset_population",
            "blocked_reason": "",
            "planner_mode": planner_mode,
            "target_scope": target_scope,
        }
    return {
        "eligible": False,
        "classification": "unknown_or_ranked_result",
        "blocked_reason": "job does not have full-local/full-asset reuse proof",
        "planner_mode": planner_mode,
        "target_scope": target_scope,
    }


def _build_job_result_view_consistency_record(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    job: dict[str, Any],
    asset_view: str,
    apply: bool,
) -> dict[str, Any]:
    job_id = str(job.get("job_id") or "").strip()
    view = store.get_job_result_view(job_id=job_id) or {}
    candidate_source = _job_summary_candidate_source(job)
    request = dict(job.get("request") or {})
    normalized_asset_view = (
        str(
            asset_view
            or candidate_source.get("asset_view")
            or view.get("asset_view")
            or request.get("asset_view")
            or "canonical_merged"
        ).strip()
        or "canonical_merged"
    )
    target_company = _cli_first_non_empty_text(
        view.get("target_company"),
        candidate_source.get("target_company"),
        request.get("target_company"),
    )
    authoritative = (
        store.get_authoritative_organization_asset_registry(
            target_company=target_company,
            asset_view=normalized_asset_view,
        )
        if target_company
        else {}
    )
    view_snapshot_id = str(view.get("snapshot_id") or "").strip()
    summary_snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    authoritative_snapshot_id = str(authoritative.get("snapshot_id") or "").strip()
    view_asset_view = str(view.get("asset_view") or "").strip() or normalized_asset_view
    summary_asset_view = str(candidate_source.get("asset_view") or "").strip() or normalized_asset_view
    consistency = {
        "job_result_view_matches_summary": bool(
            view_snapshot_id
            and summary_snapshot_id
            and view_snapshot_id == summary_snapshot_id
            and view_asset_view == summary_asset_view
        ),
        "job_result_view_matches_authoritative": bool(
            view_snapshot_id
            and authoritative_snapshot_id
            and view_snapshot_id == authoritative_snapshot_id
            and view_asset_view == normalized_asset_view
        ),
        "summary_matches_authoritative": bool(
            summary_snapshot_id
            and authoritative_snapshot_id
            and summary_snapshot_id == authoritative_snapshot_id
            and summary_asset_view == normalized_asset_view
        ),
    }
    drift_fields: list[str] = []
    if not view:
        drift_fields.append("missing_job_result_view")
    if not authoritative_snapshot_id:
        drift_fields.append("missing_authoritative_registry")
    if view_snapshot_id and summary_snapshot_id and not consistency["job_result_view_matches_summary"]:
        drift_fields.append("job_result_view_vs_summary")
    if view_snapshot_id and authoritative_snapshot_id and not consistency["job_result_view_matches_authoritative"]:
        drift_fields.append("job_result_view_vs_authoritative")
    if summary_snapshot_id and authoritative_snapshot_id and not consistency["summary_matches_authoritative"]:
        drift_fields.append("summary_vs_authoritative")
    eligibility = _job_result_view_auto_repoint_eligibility(
        job=job,
        view=view,
        candidate_source=candidate_source,
    )
    needs_repoint = bool(
        view
        and authoritative_snapshot_id
        and (
            view_snapshot_id != authoritative_snapshot_id
            or view_asset_view != normalized_asset_view
        )
    )
    recommended_action = "none"
    applied_result: dict[str, Any] = {}
    if not view:
        recommended_action = "missing_job_result_view"
    elif not authoritative_snapshot_id:
        recommended_action = "missing_authoritative_registry"
    elif needs_repoint and bool(eligibility.get("eligible")):
        recommended_action = "apply_repoint_to_authoritative" if apply else "dry_run_repoint_to_authoritative"
        if apply:
            applied_result = repoint_job_result_view(
                runtime_dir=runtime_dir,
                store=store,
                job_id=job_id,
                company=target_company,
                snapshot_id=authoritative_snapshot_id,
                asset_view=normalized_asset_view,
                policy="serve_latest_company_asset",
                reason="audit-job-result-view-consistency",
                apply=True,
            )
    elif needs_repoint:
        recommended_action = "manual_review_required"
    elif drift_fields:
        recommended_action = "manual_summary_or_registry_review_required"
    return {
        "job_id": job_id,
        "target_company": target_company,
        "asset_view": normalized_asset_view,
        "job": {
            "status": str(job.get("status") or ""),
            "stage": str(job.get("stage") or ""),
            "job_type": str(job.get("job_type") or ""),
            "target_scope": str(request.get("target_scope") or ""),
            "keywords": [str(item) for item in list(request.get("keywords") or []) if str(item or "").strip()],
            "updated_at": str(job.get("updated_at") or ""),
        },
        "job_summary_candidate_source": {
            "source_kind": str(candidate_source.get("source_kind") or ""),
            "snapshot_id": summary_snapshot_id,
            "asset_view": summary_asset_view,
            "candidate_count": int(candidate_source.get("candidate_count") or 0),
            "asset_population_overlay_path": str(candidate_source.get("asset_population_overlay_path") or ""),
        },
        "job_result_view": {
            "view_id": str(view.get("view_id") or ""),
            "source_kind": str(view.get("source_kind") or ""),
            "view_kind": str(view.get("view_kind") or ""),
            "snapshot_id": view_snapshot_id,
            "asset_view": view_asset_view,
            "candidate_count": int(dict(view.get("summary") or {}).get("candidate_count") or 0),
            "source_path": str(view.get("source_path") or ""),
        },
        "authoritative_registry": {
            "snapshot_id": authoritative_snapshot_id,
            "asset_view": str(authoritative.get("asset_view") or normalized_asset_view),
            "candidate_count": int(authoritative.get("candidate_count") or 0),
            "source_path": str(authoritative.get("source_path") or ""),
            "materialization_generation_key": str(authoritative.get("materialization_generation_key") or ""),
        },
        "consistency": consistency,
        "drift_fields": drift_fields,
        "eligibility": eligibility,
        "recommended_action": recommended_action,
        "applied": bool(applied_result),
        "applied_result": applied_result,
    }


def audit_job_result_view_consistency(
    *,
    runtime_dir: str | Path,
    store: ControlPlaneStore,
    job_id: str = "",
    company: str = "",
    asset_view: str = "canonical_merged",
    limit: int = 200,
    apply: bool = False,
) -> dict[str, Any]:
    normalized_job_id = str(job_id or "").strip()
    company_key_filter = normalize_company_key(company) if str(company or "").strip() else ""
    if normalized_job_id:
        job = store.get_job(normalized_job_id)
        jobs = [job] if job is not None else []
    else:
        jobs = store.list_jobs(statuses=["completed"], limit=max(1, int(limit or 200)))
    records: list[dict[str, Any]] = []
    for job in [dict(item or {}) for item in jobs if isinstance(item, dict)]:
        request = dict(job.get("request") or {})
        candidate_source = _job_summary_candidate_source(job)
        view = store.get_job_result_view(job_id=str(job.get("job_id") or "")) or {}
        target_company = _cli_first_non_empty_text(
            view.get("target_company"),
            candidate_source.get("target_company"),
            request.get("target_company"),
        )
        if company_key_filter and normalize_company_key(target_company) != company_key_filter:
            continue
        records.append(
            _build_job_result_view_consistency_record(
                runtime_dir=runtime_dir,
                store=store,
                job=job,
                asset_view=asset_view,
                apply=apply,
            )
        )
    action_counts: dict[str, int] = {}
    classification_counts: dict[str, int] = {}
    for record in records:
        action = str(record.get("recommended_action") or "unknown")
        action_counts[action] = action_counts.get(action, 0) + 1
        classification = str(dict(record.get("eligibility") or {}).get("classification") or "unknown")
        classification_counts[classification] = classification_counts.get(classification, 0) + 1
    return {
        "status": "completed",
        "applied": bool(apply),
        "job_id": normalized_job_id,
        "company": str(company or "").strip(),
        "asset_view": str(asset_view or "canonical_merged").strip() or "canonical_merged",
        "summary": {
            "scanned_job_count": len(jobs),
            "reported_job_count": len(records),
            "drift_job_count": sum(1 for record in records if record.get("drift_fields")),
            "auto_repoint_candidate_count": sum(
                1
                for record in records
                if str(record.get("recommended_action") or "") in {
                    "dry_run_repoint_to_authoritative",
                    "apply_repoint_to_authoritative",
                }
            ),
            "manual_review_count": sum(
                1
                for record in records
                if str(record.get("recommended_action") or "") == "manual_review_required"
            ),
            "applied_repoint_count": sum(1 for record in records if bool(record.get("applied"))),
            "action_counts": action_counts,
            "classification_counts": classification_counts,
        },
        "jobs": records,
    }


def _control_plane_storage_banner(summary: dict[str, Any]) -> dict[str, Any]:
    live_mode = str(summary.get("control_plane_postgres_live_mode") or "").strip().lower()
    compatibility_shadow_backend = str(
        summary.get("compatibility_shadow_backend") or summary.get("sqlite_shadow_backend") or ""
    ).strip().lower()
    if live_mode != "postgres_only":
        return {
            "status": "non_pg_control_plane",
            "severity": "error",
            "message": (
                "This runtime is not PG-only. Disk-backed live SQLite control-plane mode is retired; "
                "migrate state into Postgres before live or hosted use."
            ),
            "migration_exit": (
                "Set SOURCING_CONTROL_PLANE_POSTGRES_DSN, "
                "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only, "
                "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1, and migrate any remaining SQLite-only state into PG."
            ),
        }
    if compatibility_shadow_backend == "disk":
        return {
            "status": "disk_shadow_rejected",
            "severity": "error",
            "message": (
                "PG-only live mode must use an ephemeral shared_memory compatibility shadow, "
                "not disk-backed shadow storage."
            ),
            "migration_exit": "Set SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory before serving live traffic.",
        }
    return {
        "status": "pg_only",
        "severity": "ok",
        "message": (
            "Postgres is the live authoritative control plane; disk-backed live SQLite is retired, "
            "and only an ephemeral compatibility shadow remains."
        ),
        "migration_exit": "",
    }


def build_orchestrator() -> SourcingOrchestrator:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    semantic_provider = build_semantic_provider(settings.semantic)
    agent_runtime = AgentRuntimeCoordinator(store)
    acquisition_engine = AcquisitionEngine(catalog, settings, store, model_client, worker_runtime=agent_runtime)
    return SourcingOrchestrator(
        catalog=catalog,
        store=store,
        jobs_dir=settings.jobs_dir,
        model_client=model_client,
        semantic_provider=semantic_provider,
        acquisition_engine=acquisition_engine,
        agent_runtime=agent_runtime,
    )


def build_asset_bundle_manager() -> AssetBundleManager:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    return AssetBundleManager(catalog.project_root, settings.runtime_dir)


def build_object_storage():
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    return build_object_storage_client(settings.object_storage)


def build_asset_completion_manager() -> CompanyAssetCompletionManager:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    return CompanyAssetCompletionManager(
        runtime_dir=settings.runtime_dir,
        store=store,
        settings=settings,
        model_client=model_client,
    )


def build_asset_supplement_manager() -> CompanyAssetSupplementManager:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    model_client = build_model_client(settings.model_provider, settings.qwen)
    return CompanyAssetSupplementManager(
        runtime_dir=settings.runtime_dir,
        store=store,
        settings=settings,
        model_client=model_client,
    )


def run_target_candidate_public_web_experiment_command(args) -> dict[str, Any]:
    return {
        "status": "retired",
        "reason": "legacy_target_candidate_public_web_experiment_retired",
        "normal_path": False,
        "report_visible": True,
        "migration_phase": "W7e_legacy_target_public_web_cli_experiment_retired",
        "canonical_entrypoint": "make test-crm-public-web-live-product-validation",
        "canonical_module": "crm_public_web_owner",
        "canonical_storage_owner": "crm_public_web_v1",
        "retirement_mode": "permanent_hard_disable",
    }


def evaluate_public_web_quality_command(args) -> dict[str, Any]:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    input_paths = [Path(str(item)).expanduser() for item in list(args.experiment_dir or []) if str(item or "").strip()]
    if not input_paths:
        input_paths = [settings.runtime_dir / "public_web" / "experiments"]
    report = evaluate_public_web_quality_paths(input_paths)
    written: dict[str, str] = {}
    output_dir_value = str(args.output_dir or "").strip()
    if output_dir_value:
        written = write_public_web_quality_report(report, Path(output_dir_value).expanduser())
        report = {**report, "written": written}
    if bool(args.fail_on_high_risk) and int(dict(report.get("summary") or {}).get("high_issue_count") or 0) > 0:
        report = {**report, "status": "failed"}
    if bool(getattr(args, "summary_only", False)):
        summary_report: dict[str, Any] = {
            "status": report.get("status", "ok"),
            "summary": report.get("summary", {}),
            "input_paths": [str(path) for path in input_paths],
        }
        if written:
            summary_report["written"] = written
        return summary_report
    return report


def spawn_workflow_runner(job_id: str, *, auto_job_daemon: bool) -> dict[str, object]:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    runner_env = _runner_environment(catalog.project_root)
    log_dir = settings.runtime_dir / "service_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"workflow-runner-{job_id}.log"
    command = [
        sys.executable,
        "-m",
        "sourcing_agent.cli",
        "supervise-workflow" if auto_job_daemon else "execute-workflow",
        "--job-id",
        job_id,
    ]
    if auto_job_daemon:
        command.append("--auto-job-daemon")

    return {
        "job_id": job_id,
        **_spawn_detached_process(
            command=command,
            cwd=catalog.project_root,
            log_path=log_path,
            env=runner_env,
        ),
    }


def _is_process_alive(pid: int) -> bool:
    return _workflow_runner_process_alive(pid)


def _wait_for_workflow_runner_progress(
    orchestrator: SourcingOrchestrator,
    *,
    job_id: str,
    pid: int,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, object]:
    return orchestrator._wait_for_workflow_runner_progress(  # noqa: SLF001
        job_id=job_id,
        pid=pid,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )


def start_workflow_runner_with_handshake(
    orchestrator: SourcingOrchestrator,
    *,
    job_id: str,
    auto_job_daemon: bool,
    handshake_timeout_seconds: float,
    poll_seconds: float = 0.1,
    max_attempts: int = 2,
) -> dict[str, object]:
    return orchestrator._start_workflow_runner_with_handshake(  # noqa: SLF001
        job_id=job_id,
        auto_job_daemon=auto_job_daemon,
        handshake_timeout_seconds=handshake_timeout_seconds,
        poll_seconds=poll_seconds,
        max_attempts=max_attempts,
    )


def run_server_runtime_watchdog_once(
    orchestrator: SourcingOrchestrator,
    *,
    shared_service_name: str = "worker-recovery-daemon",
    hosted_service_name: str = "server-runtime-watchdog",
) -> dict[str, Any]:
    return orchestrator.run_hosted_runtime_watchdog_once(
        {
            "shared_service_name": shared_service_name,
            "hosted_runtime_watchdog_service_name": hosted_service_name,
            "hosted_runtime_source": hosted_service_name,
        }
    )


def start_server_runtime_watchdog(
    orchestrator: SourcingOrchestrator,
    *,
    poll_seconds: float = 15.0,
    shared_service_name: str = "worker-recovery-daemon",
    hosted_service_name: str = "server-runtime-watchdog",
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _loop() -> None:
        while not stop_event.is_set():
            try:
                service = WorkerDaemonService(
                    runtime_dir=orchestrator.runtime_dir,
                    recovery_callback=lambda _: run_server_runtime_watchdog_once(
                        orchestrator,
                        shared_service_name=shared_service_name,
                        hosted_service_name=hosted_service_name,
                    ),
                    service_name=hosted_service_name,
                    poll_seconds=max(1.0, float(poll_seconds or 15.0)),
                    callback_payload={"hosted_runtime_source": hosted_service_name},
                )

                def _stop_bridge() -> None:
                    stop_event.wait()
                    service.request_stop()

                threading.Thread(target=_stop_bridge, name=f"{hosted_service_name}-stop", daemon=True).start()
                service.run_forever()
            except SingleInstanceError:
                return
            except Exception:
                return

    thread = threading.Thread(
        target=_loop,
        name="server-runtime-watchdog",
        daemon=True,
    )
    thread.start()
    return stop_event, thread


def start_shared_recovery_service(
    orchestrator: SourcingOrchestrator,
    *,
    poll_seconds: float = 5.0,
    service_name: str = "worker-recovery-daemon",
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _loop() -> None:
        while not stop_event.is_set():
            try:
                service = WorkerDaemonService(
                    runtime_dir=orchestrator.runtime_dir,
                    recovery_callback=lambda payload: orchestrator.run_worker_recovery_once(payload),
                    service_name=service_name,
                    poll_seconds=max(1.0, float(poll_seconds or 5.0)),
                    stale_after_seconds=180,
                    total_limit=4,
                    callback_payload={
                        "workflow_resume_stale_after_seconds": 60,
                        "workflow_queue_resume_stale_after_seconds": 60,
                        "runtime_heartbeat_source": "shared_recovery_daemon",
                        "runtime_heartbeat_service_name": service_name,
                        "runtime_heartbeat_interval_seconds": 60,
                    },
                )

                def _stop_bridge() -> None:
                    stop_event.wait()
                    service.request_stop()

                threading.Thread(target=_stop_bridge, name=f"{service_name}-stop", daemon=True).start()
                service.run_forever()
            except SingleInstanceError:
                return
            except Exception:
                return

    thread = threading.Thread(
        target=_loop,
        name=service_name,
        daemon=True,
    )
    thread.start()
    return stop_event, thread


class RecoveryCoverageError(RuntimeError):
    """serve refused to start because nothing drives worker recovery (Step 5a).

    Recovery being driven is a runtime invariant, not an implicit deployment
    convention (study docs/RECOVERY_DRIVING_REDESIGN_STUDY.md §5). serve fails
    closed rather than start a server that silently believes recovery is
    covered when no in-process thread and no fresh external daemon exist.
    """


def external_recovery_daemon_is_fresh(
    runtime_dir: str | Path,
    *,
    service_name: str = "worker-recovery-daemon",
) -> tuple[bool, dict[str, Any]]:
    """Return (is_fresh, status) for an out-of-process recovery daemon.

    A standalone systemd daemon (the production guarantee) holds the flock, so
    the in-process shared thread yields with SingleInstanceError. Coverage in
    that case means a reachable daemon whose status file resolves "running" —
    read_service_status only keeps "running" when the lock is held AND the
    heartbeat is fresh (poll_seconds*3+2 budget); a dead/stale daemon is
    downgraded to "stale"/"not_started"/"corrupted" (service_daemon.py
    ~656-716). We treat only "running" as covering recovery.
    """

    status = read_service_status(runtime_dir, service_name)
    resolved = str(status.get("status") or "").strip()
    return resolved == "running", status


def assert_recovery_coverage_or_fail_closed(
    orchestrator: SourcingOrchestrator,
    *,
    shared_recovery_thread: threading.Thread | None,
    watchdog_disabled: bool,
    allow_uncovered_recovery: bool = False,
    service_name: str = "worker-recovery-daemon",
    recheck_window_seconds: float = 5.0,
    recheck_interval_seconds: float = 0.25,
    sleep: Any = time.sleep,
) -> dict[str, Any]:
    """Fail closed unless something actually drives worker recovery (Step 5a).

    Coverage sources, in priority order:
      1. In the explicit dev-only compatibility mode, a live in-process shared
         recovery thread covers recovery.
      2. In the default API-only mode, or when that compatibility thread yielded
         to an external daemon and exited, a FRESH external recovery daemon must
         exist. Because a just-launched systemd daemon can race serve's boot, we
         re-check within a short bounded window before deciding, then refuse.

    If neither source covers recovery, raise RecoveryCoverageError so serve never
    starts believing recovery is covered when nothing drives it. The
    --allow-uncovered-recovery opt-out is the ONLY way to proceed uncovered
    ("API-only, recovery elsewhere, I accept it"); it is intentionally loud.
    """

    if shared_recovery_thread is not None and shared_recovery_thread.is_alive():
        return {
            "coverage": "in_process_shared_recovery_thread",
            "watchdog_disabled": watchdog_disabled,
        }

    # No live in-process thread: require a fresh external daemon, tolerating a
    # short boot race before refusing.
    deadline = time.monotonic() + max(0.0, float(recheck_window_seconds or 0.0))
    interval = max(0.01, float(recheck_interval_seconds or 0.25))
    is_fresh, status = external_recovery_daemon_is_fresh(orchestrator.runtime_dir, service_name=service_name)
    while not is_fresh and time.monotonic() < deadline:
        sleep(min(interval, max(0.0, deadline - time.monotonic())))
        is_fresh, status = external_recovery_daemon_is_fresh(orchestrator.runtime_dir, service_name=service_name)

    if is_fresh:
        return {
            "coverage": "external_recovery_daemon",
            "watchdog_disabled": watchdog_disabled,
            "external_status": str(status.get("status") or ""),
        }

    detail = {
        "event": "serve_recovery_coverage_check",
        "coverage": "none",
        "watchdog_disabled": watchdog_disabled,
        "service_name": service_name,
        "external_status": str(status.get("status") or "not_started"),
        "external_stale_reason": str(status.get("stale_reason") or ""),
        "runtime_dir": str(orchestrator.runtime_dir),
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    if allow_uncovered_recovery:
        detail["coverage"] = "opt_out_allow_uncovered_recovery"
        detail["severity"] = "warning"
        detail["message"] = (
            "serve starting WITHOUT a recovery driver because --allow-uncovered-recovery "
            "was set: no in-process recovery thread and no fresh external "
            "worker-recovery-daemon. Stuck/in-flight work will NOT be recovered by "
            "this process. You asserted recovery runs elsewhere."
        )
        print(json.dumps(detail, ensure_ascii=False), file=sys.stderr, flush=True)
        return detail

    detail["severity"] = "error"
    detail["message"] = (
        "serve refused to start: worker recovery is not covered. No in-process "
        "recovery thread is running and no fresh external worker-recovery-daemon "
        "was found. Start the standalone recovery daemon, use the dev-only "
        "--enable-runtime-watchdog compatibility path, or pass "
        "--allow-uncovered-recovery if recovery truly runs elsewhere."
    )
    print(json.dumps(detail, ensure_ascii=False), file=sys.stderr, flush=True)
    raise RecoveryCoverageError(detail["message"])


def _resolve_hosted_api_base_url(explicit_base_url: str = "") -> str:
    base_url = (
        str(explicit_base_url or "").strip()
        or str(os.environ.get("SOURCING_AGENT_API_BASE_URL") or "").strip()
        or "http://127.0.0.1:8765"
    )
    return base_url.rstrip("/")


def _submit_hosted_workflow_request(
    payload: dict[str, Any],
    *,
    base_url: str,
    timeout_seconds: float = 15.0,
) -> dict[str, Any]:
    request_payload = normalize_workflow_submission_payload(
        {**dict(payload), "runtime_execution_mode": "hosted"},
        default_runtime_execution_mode="hosted",
        hosted_auto_job_daemon=False,
    )
    workflow_request = urllib_request.Request(
        f"{base_url}/api/workflows",
        data=json.dumps(request_payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
    try:
        with opener.open(workflow_request, timeout=max(1.0, float(timeout_seconds or 15.0))) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib_error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8")
        except Exception:
            detail = ""
        raise HostedWorkflowSubmissionError(
            f"HTTP {exc.code} from hosted API {base_url}/api/workflows" + (f": {detail}" if detail else "")
        ) from exc
    except urllib_error.URLError as exc:
        raise HostedWorkflowSubmissionError(f"could not reach hosted API at {base_url} ({exc.reason})") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Sourcing AI Agent backend MVP")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for _configure in cli_parsers.CONFIGURATORS:
        _configure(subparsers)

    args = parser.parse_args()
    handler = _CLI_COMMAND_HANDLERS.get(str(args.command or ""))
    if handler is None:
        parser.error(f"unknown command: {args.command}")
        return
    handler(args)


def _cli_cmd_show_control_plane_runtime(args: argparse.Namespace) -> None:
    print(json.dumps(build_control_plane_runtime_summary(), ensure_ascii=False, indent=2))
    return


def _cli_cmd_target_candidate_public_web_experiment(args: argparse.Namespace) -> None:
    # Name deliberately drops the run_ prefix: the pre-agent contract forbids
    # the retired direct-call token (the experiment entry point invoked without
    # its _command wrapper suffix) anywhere in cli source.
    print(json.dumps(run_target_candidate_public_web_experiment_command(args), ensure_ascii=False, indent=2))
    return


def _cli_cmd_evaluate_public_web_quality(args: argparse.Namespace) -> None:
    result = evaluate_public_web_quality_command(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get("status") == "failed":
        sys.exit(1)
    return


def _cli_cmd_refresh_company_public_web_assets(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    collector_inputs: dict[str, Any] = {}
    collector_sources: list[Any] = []
    if str(args.collector_input_json or "").strip():
        collector_input_path = Path(str(args.collector_input_json).strip()).expanduser()
        collector_payload = json.loads(collector_input_path.read_text(encoding="utf-8"))
        collector_inputs = dict(collector_payload.get("collector_inputs") or collector_payload)
    if str(args.collector_source_json or "").strip():
        collector_source_path = Path(str(args.collector_source_json).strip()).expanduser()
        collector_source_payload = json.loads(collector_source_path.read_text(encoding="utf-8"))
        if isinstance(collector_source_payload, dict):
            raw_sources = collector_source_payload.get("collector_sources")
            collector_sources = list(raw_sources or []) if isinstance(raw_sources, list) else []
        elif isinstance(collector_source_payload, list):
            collector_sources = list(collector_source_payload)
    collector_sources.extend(
        str(item or "").strip() for item in list(args.collector_source_url or []) if str(item or "").strip()
    )
    payload = {
        "target_company": str(args.target_company or "").strip(),
        "source_families": [str(item or "").strip() for item in list(args.source_family or []) if str(item or "").strip()],
        "seed_urls": [str(item or "").strip() for item in list(args.seed_url or []) if str(item or "").strip()],
        "options": {
            "max_assets": max(1, int(args.max_assets or 50)),
            "collection_mode": str(args.collection_mode or "seed_url_only").strip() or "seed_url_only",
            "max_queries": max(1, int(args.max_queries or 6)),
            "max_results_per_query": max(1, int(args.max_results_per_query or 10)),
            "discover_collector_sources": bool(args.discover_collector_sources),
            "max_discovered_collector_sources": max(1, int(args.max_discovered_collector_sources or 12)),
        },
        "force_refresh": bool(args.force_refresh),
        "requested_by": str(args.requested_by or "cli").strip() or "cli",
        "run_id": str(args.run_id or "").strip(),
    }
    if collector_inputs:
        payload["collector_inputs"] = collector_inputs
    if collector_sources:
        payload["collector_sources"] = collector_sources
    result = orchestrator.refresh_company_public_web_assets(payload)
    if str(args.output or "").strip():
        output_path = Path(str(args.output).strip()).expanduser()
        if not output_path.is_absolute():
            output_path = AssetCatalog.discover().project_root / output_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_list_company_public_web_assets(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.list_company_public_web_assets(
        {
            "target_company": str(args.target_company or "").strip(),
            "company_key": str(args.company_key or "").strip(),
            "source_family": str(args.source_family or "").strip(),
            "status": str(args.status or "").strip(),
            "limit": max(1, int(args.limit or 100)),
        }
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_store_command_group_1(args: argparse.Namespace) -> None:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    store = build_runtime_store(settings)
    if args.command == "backfill-linkedin-profile-registry":
        checkpoint_path = (
            Path(args.checkpoint_path).expanduser() if str(args.checkpoint_path or "").strip() else None
        )

        def _progress(payload: dict[str, object]) -> None:
            print(json.dumps({"progress": payload}, ensure_ascii=False))

        result = backfill_linkedin_profile_registry(
            runtime_dir=settings.runtime_dir,
            store=store,
            company=str(args.company or "").strip(),
            snapshot_id=str(args.snapshot_id or "").strip(),
            resume=not bool(args.no_resume),
            checkpoint_path=checkpoint_path,
            progress_interval=max(1, int(args.progress_interval or 1)),
            progress_callback=_progress,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "backfill-organization-asset-registry":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        if not companies:
            raise SystemExit("backfill-organization-asset-registry requires at least one --company")
        results = []
        for company in companies:
            results.append(
                backfill_organization_asset_registry_for_company(
                    runtime_dir=settings.runtime_dir,
                    store=store,
                    target_company=company,
                    asset_view=str(args.asset_view or "canonical_merged"),
                )
            )
        print(
            json.dumps(
                {
                    "asset_view": str(args.asset_view or "canonical_merged"),
                    "results": results,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "backfill-authoritative-population-coverage":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        result = backfill_authoritative_population_coverage(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=companies,
            asset_view=str(args.asset_view or "canonical_merged"),
            include_non_authoritative=bool(args.include_non_authoritative),
            dry_run=not bool(args.apply),
            force=bool(args.force),
            limit=max(1, int(args.limit or 1000)),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "repair-authoritative-serving-generation":
        result = repair_authoritative_serving_generation(
            runtime_dir=settings.runtime_dir,
            store=store,
            company=str(args.company or "").strip(),
            queries=[str(item or "").strip() for item in list(args.query or []) if str(item or "").strip()],
            asset_view=str(args.asset_view or "canonical_merged"),
            snapshot_id=str(args.snapshot_id or "").strip(),
            repair_snapshot_id=str(args.repair_snapshot_id or "").strip(),
            build_profile=str(args.build_profile or "foreground_fast").strip() or "foreground_fast",
            output_dir=args.output_dir or None,
            apply=bool(args.apply),
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "normalize-authoritative-source-provenance":
        result = normalize_authoritative_source_provenance(
            store=store,
            company=str(args.company or "").strip(),
            asset_view=str(args.asset_view or "canonical_merged"),
            apply=bool(args.apply),
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "backfill-job-result-lifecycle":
        orchestrator = build_orchestrator()

        def _progress(job_id: str, stats: Any) -> None:
            if args.verbose:
                payload = stats.to_dict() if hasattr(stats, "to_dict") else dict(stats or {})
                payload["job_id"] = str(job_id or "")
                print(json.dumps({"progress": payload}, ensure_ascii=False))

        result = backfill_job_result_lifecycle(
            orchestrator=orchestrator,
            dry_run=bool(args.dry_run),
            batch_size=max(1, int(args.batch_size or 100)),
            progress_callback=_progress,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "backfill-snapshot-full-materialization-items":
        orchestrator = build_orchestrator()

        def _progress(job_id: str, stats: Any) -> None:
            if args.verbose:
                payload = stats.to_dict() if hasattr(stats, "to_dict") else dict(stats or {})
                payload["job_id"] = str(job_id or "")
                print(json.dumps({"progress": payload}, ensure_ascii=False))

        result = backfill_snapshot_full_materialization_items(
            orchestrator=orchestrator,
            dry_run=not bool(args.apply),
            limit=max(1, int(args.limit or 100000)),
            batch_size=max(1, int(args.batch_size or 100)),
            progress_callback=_progress,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "backfill-local-apply-closure-items":
        orchestrator = build_orchestrator()
        result = orchestrator.backfill_local_apply_closure_items(
            {
                "dry_run": not bool(args.apply),
                "job_id": str(args.job_id or "").strip(),
                "local_apply_backlog_worker_limit": max(1, int(args.limit or 100)),
                "local_apply_backlog_job_scan_limit": max(1, int(args.job_scan_limit or 500)),
            }
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "repair-excel-intake-artifacts":
        orchestrator = build_orchestrator()
        result = orchestrator.repair_excel_intake_artifacts(
            {
                "job_id": str(args.job_id or "").strip(),
                "dry_run": not bool(args.apply),
                "run_now": bool(args.run_now),
                "source": "repair_excel_intake_artifacts_cli",
            }
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "backfill-search-seed-discovery-items":
        orchestrator = build_orchestrator()
        result = orchestrator.backfill_search_seed_discovery_query_items(
            {
                "dry_run": not bool(args.apply),
                "job_id": str(args.job_id or "").strip(),
                "search_seed_discovery_worker_limit": max(1, int(args.limit or 100)),
                "search_seed_discovery_job_scan_limit": max(1, int(args.job_scan_limit or 500)),
            }
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "rebuild-runtime-control-plane":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        result = rebuild_runtime_control_plane(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=companies or None,
            snapshot_id=str(args.snapshot_id or "").strip(),
            rebuild_missing_artifacts=not bool(args.skip_missing_artifact_repair),
            rebuild_company_assets=not bool(args.skip_company_assets),
            rebuild_jobs=not bool(args.skip_jobs),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "repair-company-candidate-artifacts":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        result = repair_missing_company_candidate_artifacts(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=companies or None,
            snapshot_id=str(args.snapshot_id or "").strip(),
            force_rebuild_artifacts=bool(args.force_rebuild),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "repair-paginated-candidate-artifacts":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        result = repair_paginated_candidate_artifacts_from_materialized(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=companies or None,
            snapshot_id=str(args.snapshot_id or "").strip(),
            asset_view=str(args.asset_view or "canonical_merged"),
            include_history=bool(args.include_history),
            dry_run=bool(args.dry_run),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "backfill-structured-timeline":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        result = backfill_structured_timeline_for_company_assets(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=companies or None,
            snapshot_id=str(args.snapshot_id or "").strip(),
            backfill_profile_registry=not bool(args.skip_profile_registry_backfill),
            profile_resume=not bool(args.profile_no_resume),
            profile_progress_interval=max(1, int(args.profile_progress_interval or 1)),
            refresh_registry=not bool(args.skip_registry_refresh),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "repair-profile-signal-projection":
        companies = [str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()]
        result = repair_projected_profile_signals_in_company_candidate_artifacts(
            runtime_dir=settings.runtime_dir,
            companies=companies or None,
            snapshot_id=str(args.snapshot_id or "").strip(),
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return
    if args.command == "show-linkedin-profile-registry-metrics":
        metrics = store.repos.linkedin_profile_registry.get_metrics(lookback_hours=max(0, int(args.lookback_hours or 0)))
        print(json.dumps(metrics, ensure_ascii=False, indent=2))
        return


def _cli_cmd_export_control_plane_snapshot(args: argparse.Namespace) -> None:
    catalog = AssetCatalog.discover()
    settings = load_settings(catalog.project_root)
    runtime_dir = (
        Path(str(args.runtime_dir or "").strip()).expanduser()
        if str(args.runtime_dir or "").strip()
        else settings.runtime_dir
    )
    output_path = (
        Path(str(args.output or "").strip()).expanduser()
        if str(args.output or "").strip()
        else runtime_dir / "object_sync" / "control_plane" / "control_plane_snapshot.json"
    )
    sqlite_path = (
        Path(str(args.sqlite_path or "").strip()).expanduser()
        if str(args.sqlite_path or "").strip()
        else None
    )
    print(
        json.dumps(
            export_control_plane_snapshot(
                runtime_dir=runtime_dir,
                output_path=output_path,
                sqlite_path=sqlite_path,
                tables=[str(item or "").strip() for item in list(args.table or []) if str(item or "").strip()],
                include_all_sqlite_tables=bool(args.all_sqlite_tables),
                source_backend=str(args.source_backend or "postgres"),
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_sync_control_plane_postgres(args: argparse.Namespace) -> None:
    try:
        if str(args.snapshot or "").strip():
            summary = sync_control_plane_snapshot_to_postgres(
                snapshot_path=str(args.snapshot or "").strip(),
                dsn=str(args.dsn or "").strip(),
                tables=(
                    [str(item or "").strip() for item in list(args.table or []) if str(item or "").strip()]
                    or None
                ),
                truncate_first=bool(args.truncate_first),
                validate_postgres=bool(args.validate_postgres),
            )
        else:
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            runtime_dir = (
                Path(str(args.runtime_dir or "").strip()).expanduser()
                if str(args.runtime_dir or "").strip()
                else settings.runtime_dir
            )
            sqlite_path = (
                Path(str(args.sqlite_path or "").strip()).expanduser()
                if str(args.sqlite_path or "").strip()
                else settings.db_path
            )
            state_path = (
                Path(str(args.state_path or "").strip()).expanduser()
                if str(args.state_path or "").strip()
                else None
            )
            summary = sync_runtime_control_plane_to_postgres(
                runtime_dir=runtime_dir,
                sqlite_path=sqlite_path,
                dsn=str(args.dsn or "").strip(),
                tables=[str(item or "").strip() for item in list(args.table or []) if str(item or "").strip()],
                truncate_first=bool(args.truncate_first),
                state_path=state_path,
                min_interval_seconds=float(args.min_interval_seconds or 0.0),
                force=bool(args.force),
                include_all_sqlite_tables=bool(args.all_sqlite_tables),
                validate_postgres=bool(args.validate_postgres),
                direct_stream=bool(args.direct_stream),
                chunk_size=int(args.chunk_size or 0),
                commit_every_chunks=int(args.commit_every_chunks or 0),
                progress_every_chunks=int(args.progress_every_chunks or 0),
                chunk_pause_seconds=float(args.chunk_pause_seconds or 0.0),
            )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_store_command_group_2(args: argparse.Namespace) -> None:
    bundle_manager = build_asset_bundle_manager()
    if args.command == "export-company-snapshot-bundle":
        print(
            json.dumps(
                bundle_manager.export_company_snapshot_bundle(
                    args.company,
                    snapshot_id=args.snapshot_id,
                    output_dir=args.output_dir or None,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "export-company-handoff-bundle":
        print(
            json.dumps(
                bundle_manager.export_company_handoff_bundle(
                    args.company,
                    output_dir=args.output_dir or None,
                    include_live_tests=not args.without_live_tests,
                    include_manual_review=not args.without_manual_review,
                    include_jobs=not args.without_jobs,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "export-control-plane-snapshot-bundle":
        print(
            json.dumps(
                bundle_manager.export_control_plane_snapshot_bundle(output_dir=args.output_dir or None),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command in {"build-company-candidate-artifacts", "rebuild-company-serving-view"}:
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        print(
            json.dumps(
                build_company_candidate_artifacts(
                    runtime_dir=settings.runtime_dir,
                    store=store,
                    target_company=args.company,
                    snapshot_id=args.snapshot_id,
                    output_dir=args.output_dir or None,
                    preferred_source_snapshot_ids=[
                        str(item or "").strip()
                        for item in list(args.preferred_source_snapshot_id or [])
                        if str(item or "").strip()
                    ],
                    build_profile=str(args.build_profile or "default").strip() or "default",
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "audit-company-serving-view":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        print(
            json.dumps(
                audit_company_serving_view(
                    runtime_dir=settings.runtime_dir,
                    store=store,
                    company=args.company,
                    snapshot_id=args.snapshot_id,
                    asset_view=args.asset_view,
                    job_id=args.job_id,
                    sample_pages=int(args.sample_pages or 0),
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "audit-hot-cache-serving-artifacts":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        payload = audit_candidate_artifact_hot_cache(
            runtime_dir=settings.runtime_dir,
            companies=list(args.company or []),
            snapshot_id=str(args.snapshot_id or "").strip(),
            asset_view=str(args.asset_view or "canonical_merged").strip() or "canonical_merged",
            limit=max(0, int(args.limit or 0)),
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "cleanup-hot-cache-serving-artifacts":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        payload = cleanup_candidate_artifact_hot_cache(
            runtime_dir=settings.runtime_dir,
            store=store,
            companies=list(args.company or []),
            snapshot_id=str(args.snapshot_id or "").strip(),
            dry_run=not bool(args.apply),
            drop_compatibility_exports=not bool(args.keep_compatibility_exports),
            ttl_seconds=max(0, int(args.ttl_seconds or 0)),
            size_budget_bytes=max(0, int(args.size_budget_bytes or 0)),
            max_bytes_per_company=max(0, int(args.max_bytes_per_company or 0)),
            keep_latest_snapshots_per_company=max(1, int(args.keep_latest_snapshots_per_company or 1)),
            max_generations_per_scope=max(0, int(args.max_generations_per_scope or 0)),
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "audit-authoritative-reuse-planning":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        payload = audit_authoritative_reuse_planning_many(
            runtime_dir=settings.runtime_dir,
            store=store,
            company=args.company,
            queries=list(args.query or []),
            asset_view=args.asset_view,
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "audit-authoritative-reuse-planning-matrix":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        matrix_path = Path(str(args.matrix).strip()).expanduser()
        if not matrix_path.is_absolute():
            matrix_path = catalog.project_root / matrix_path
        matrix_payload = json.loads(matrix_path.read_text(encoding="utf-8"))
        payload = audit_authoritative_reuse_planning_matrix(
            runtime_dir=settings.runtime_dir,
            store=store,
            matrix=matrix_payload if isinstance(matrix_payload, dict) else {},
            default_asset_view=args.asset_view,
            include_full_audit=not bool(args.summary_only),
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        if bool(args.strict) and payload.get("status") == "failed_expectations":
            sys.exit(2)
        return
    if args.command == "compare-authoritative-reuse-planning-matrix":
        left_path = Path(str(args.left).strip()).expanduser()
        right_path = Path(str(args.right).strip()).expanduser()
        left_payload = json.loads(left_path.read_text(encoding="utf-8"))
        right_payload = json.loads(right_path.read_text(encoding="utf-8"))
        payload = compare_authoritative_reuse_planning_matrix_reports(
            left=left_payload if isinstance(left_payload, dict) else {},
            right=right_payload if isinstance(right_payload, dict) else {},
            compare_fields=list(args.field or []) or None,
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        if bool(args.strict) and payload.get("status") == "drift":
            sys.exit(2)
        return
    if args.command == "repoint-job-result-view":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        print(
            json.dumps(
                repoint_job_result_view(
                    runtime_dir=settings.runtime_dir,
                    store=store,
                    job_id=args.job_id,
                    company=args.company,
                    snapshot_id=args.snapshot_id,
                    asset_view=args.asset_view,
                    policy=args.policy,
                    reason=args.reason,
                    apply=bool(args.apply),
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "audit-job-result-view-consistency":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        store = build_runtime_store(settings)
        payload = audit_job_result_view_consistency(
            runtime_dir=settings.runtime_dir,
            store=store,
            job_id=str(args.job_id or "").strip(),
            company=str(args.company or "").strip(),
            asset_view=str(args.asset_view or "canonical_merged").strip() or "canonical_merged",
            limit=max(1, int(args.limit or 200)),
            apply=bool(args.apply),
        )
        if str(args.output or "").strip():
            output_path = Path(str(args.output).strip()).expanduser()
            if not output_path.is_absolute():
                output_path = catalog.project_root / output_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return
    if args.command == "segment-company-outreach-layers":
        catalog = AssetCatalog.discover()
        settings = load_settings(catalog.project_root)
        model_client = None
        if not args.no_ai:
            provider = str(args.provider or "auto").strip().lower()
            if provider == "qwen":
                if not settings.qwen.enabled:
                    raise SystemExit(
                        "Qwen is not enabled; provide qwen api key/base_url in runtime/secrets/providers.local.json"
                    )
                model_client = QwenResponsesModelClient(settings.qwen)
            elif provider == "openai":
                if not settings.model_provider.enabled:
                    raise SystemExit(
                        "OpenAI-compatible provider is not enabled; check model_provider config in runtime/secrets/providers.local.json"
                    )
                model_client = OpenAICompatibleChatModelClient(settings.model_provider)
            else:
                model_client = build_model_client(settings.model_provider, settings.qwen)
        result = analyze_company_outreach_layers(
            runtime_dir=settings.runtime_dir,
            target_company=args.company,
            snapshot_id=args.snapshot_id,
            view=args.asset_view,
            query=args.query,
            model_client=model_client,
            max_ai_verifications=max(0, int(args.max_ai_verifications or 0)),
            ai_workers=max(1, int(args.ai_workers or 1)),
            ai_max_retries=max(0, int(args.ai_max_retries or 0)),
            ai_retry_backoff_seconds=max(0.0, float(args.ai_retry_backoff_seconds or 0.0)),
            output_dir=args.output_dir or None,
        )
        if args.summary_only:
            compact = {
                "status": str(result.get("status") or ""),
                "target_company": str(result.get("target_company") or ""),
                "snapshot_id": str(result.get("snapshot_id") or ""),
                "asset_view": str(result.get("asset_view") or ""),
                "ai_prompt_template_version": str((result.get("ai_prompt_template") or {}).get("version") or ""),
                "candidate_count": int(result.get("candidate_count") or 0),
                "layer_counts": {
                    "layer_0_roster": int((result.get("layers") or {}).get("layer_0_roster", {}).get("count") or 0),
                    "layer_1_name_signal": int(
                        (result.get("layers") or {}).get("layer_1_name_signal", {}).get("count") or 0
                    ),
                    "layer_2_greater_china_region_experience": int(
                        (result.get("layers") or {}).get("layer_2_greater_china_region_experience", {}).get("count")
                        or 0
                    ),
                    "layer_3_mainland_china_experience_or_chinese_language": int(
                        (result.get("layers") or {})
                        .get("layer_3_mainland_china_experience_or_chinese_language", {})
                        .get("count")
                        or 0
                    ),
                },
                "legacy_layer_count_aliases": {
                    "layer_2_greater_china_experience": int(
                        (result.get("layers") or {}).get("layer_2_greater_china_experience", {}).get("count") or 0
                    ),
                    "layer_3_mainland_or_chinese_language": int(
                        (result.get("layers") or {}).get("layer_3_mainland_or_chinese_language", {}).get("count")
                        or 0
                    ),
                },
                "cumulative_layer_counts": dict(result.get("cumulative_layer_counts") or {}),
                "final_layer_distribution": dict(result.get("final_layer_distribution") or {}),
                "ai_verification": dict(result.get("ai_verification") or {}),
                "analysis_paths": dict(result.get("analysis_paths") or {}),
            }
            print(json.dumps(compact, ensure_ascii=False, indent=2))
            return
        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "complete-company-assets":
        manager = build_asset_completion_manager()
        print(
            json.dumps(
                manager.complete_company_assets(
                    target_company=args.company,
                    snapshot_id=args.snapshot_id,
                    profile_detail_limit=args.profile_detail_limit,
                    exploration_limit=args.exploration_limit,
                    build_artifacts=not args.without_artifacts,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "supplement-company-assets":
        manager = build_asset_supplement_manager()
        if args.import_local_bootstrap_package:
            print(
                json.dumps(
                    manager.import_local_bootstrap_package(
                        target_company=args.company,
                        snapshot_id=args.snapshot_id,
                        sync_project_local_package=not args.skip_project_local_package_sync,
                        build_artifacts=not args.without_artifacts,
                    ),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return
        print(
            json.dumps(
                manager.supplement_snapshot(
                    target_company=args.company,
                    snapshot_id=args.snapshot_id,
                    rebuild_linkedin_stage_1=bool(args.rebuild_linkedin_stage_1),
                    run_former_search_seed=bool(args.run_former_search_seed),
                    former_search_limit=int(args.former_search_limit or 25),
                    former_search_pages=int(args.former_search_pages or 1),
                    former_search_queries=list(args.former_query or []),
                    former_filter_hints={"keywords": list(args.former_keyword or [])},
                    profile_scope=str(args.profile_scope or "none"),
                    profile_limit=int(args.profile_limit or 0),
                    profile_only_missing_detail=bool(args.profile_only_missing_detail)
                    and not bool(args.profile_all_known_urls),
                    profile_force_refresh=bool(args.profile_force_refresh),
                    repair_current_roster_profile_refs=bool(args.repair_current_roster_profile_refs),
                    repair_current_roster_registry_aliases=bool(args.repair_current_roster_registry_aliases),
                    build_artifacts=not args.without_artifacts,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "restore-asset-bundle":
        print(
            json.dumps(
                bundle_manager.restore_bundle(
                    args.manifest,
                    target_runtime_dir=args.target_runtime_dir or None,
                    conflict=args.conflict,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    storage_client = build_object_storage()
    if args.command == "upload-asset-bundle":
        print(
            json.dumps(
                bundle_manager.upload_bundle(
                    args.manifest,
                    storage_client,
                    max_workers=args.max_workers or None,
                    resume=not args.no_resume,
                    archive_mode=str(args.archive_mode or "auto"),
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "publish-candidate-generation":
        publish_payload = bundle_manager.publish_candidate_generation(
            target_company=str(args.company or "").strip(),
            snapshot_id=str(args.snapshot_id or "").strip(),
            asset_view=str(args.asset_view or "canonical_merged"),
            client=storage_client,
            max_workers=args.max_workers or None,
            resume=not bool(args.no_resume),
            include_compatibility_exports=bool(args.include_compatibility_exports),
        )
        if bool(args.skip_hot_cache_governance):
            publish_payload["hot_cache_governance"] = {"status": "skipped_by_flag"}
        else:
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            publish_payload["hot_cache_governance"] = run_hot_cache_governance_cycle(
                runtime_dir=settings.runtime_dir,
                store=build_runtime_store(settings),
                min_interval_seconds=0.0,
                force=True,
            )
        print(json.dumps(publish_payload, ensure_ascii=False, indent=2))
        return
    if args.command == "delete-asset-bundle":
        delete_summary = bundle_manager.delete_bundle(
            bundle_kind=args.bundle_kind,
            bundle_id=args.bundle_id,
            client=storage_client,
            max_workers=args.max_workers or None,
            prune_local_index=not bool(args.keep_local_index),
        )
        try:
            catalog = AssetCatalog.discover()
            settings = load_settings(catalog.project_root)
            store = build_runtime_store(settings)
            effective_store_target = str(store.compatibility_shadow_connect_target()).strip() or str(settings.db_path)
            delete_summary["ledger"] = store.record_cloud_asset_operation(
                operation_type="gc_delete_bundle",
                bundle_kind=str(args.bundle_kind or "").strip(),
                bundle_id=str(args.bundle_id or "").strip(),
                status=str(delete_summary.get("status") or ""),
                sync_run_id=str(delete_summary.get("sync_run_id") or ""),
                target_runtime_dir=str(bundle_manager.runtime_dir),
                target_db_path=effective_store_target,
                summary=delete_summary,
                metadata={
                    "keep_local_index": bool(args.keep_local_index),
                    "max_workers": int(args.max_workers or 0),
                },
            )
        except Exception as exc:
            delete_summary["ledger"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            }
        print(
            json.dumps(
                delete_summary,
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "download-asset-bundle":
        print(
            json.dumps(
                bundle_manager.download_bundle(
                    bundle_kind=args.bundle_kind,
                    bundle_id=args.bundle_id,
                    client=storage_client,
                    output_dir=args.output_dir or None,
                    max_workers=args.max_workers or None,
                    resume=not args.no_resume,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "import-cloud-assets":
        storage_client = None
        if not str(args.manifest or "").strip():
            storage_client = build_object_storage()
        print(
            json.dumps(
                import_cloud_assets(
                    bundle_manager=bundle_manager,
                    manifest_path=str(args.manifest or "").strip(),
                    bundle_kind=str(args.bundle_kind or "").strip(),
                    bundle_id=str(args.bundle_id or "").strip(),
                    storage_client=storage_client,
                    output_dir=args.output_dir or None,
                    max_workers=args.max_workers or None,
                    resume=not bool(args.no_resume),
                    target_runtime_dir=args.target_runtime_dir or None,
                    conflict=str(args.conflict or "skip"),
                    target_db_path=args.target_db_path or None,
                    companies=[
                        str(item or "").strip() for item in list(args.company or []) if str(item or "").strip()
                    ],
                    snapshot_id=str(args.snapshot_id or "").strip(),
                    asset_view=str(args.asset_view or "canonical_merged"),
                    generation_key=str(args.generation_key or "").strip(),
                    prefer_generation=not bool(args.disable_generation_first),
                    allow_legacy_bundle_fallback=not bool(args.disable_legacy_bundle_fallback),
                    prefer_local_link=not bool(args.disable_local_link),
                    run_artifact_repair=not bool(args.skip_artifact_repair),
                    run_org_warmup=not bool(args.skip_org_warmup),
                    run_profile_registry_backfill=not bool(args.skip_profile_registry_backfill),
                    profile_registry_resume=not bool(args.profile_no_resume),
                    profile_progress_interval=max(1, int(args.profile_progress_interval or 1)),
                    run_hot_cache_governance=not bool(args.skip_hot_cache_governance),
                    force_hot_cache_governance=True,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "hydrate-candidate-generation":
        storage_client = None
        if not str(args.manifest or "").strip():
            storage_client = build_object_storage()
        print(
            json.dumps(
                hydrate_cloud_generation(
                    bundle_manager=bundle_manager,
                    generation_manifest_path=str(args.manifest or "").strip(),
                    generation_key=str(args.generation_key or "").strip(),
                    storage_client=storage_client,
                    target_company=str(args.company or "").strip(),
                    company_key=str(args.company_key or "").strip(),
                    snapshot_id=str(args.snapshot_id or "").strip(),
                    asset_view=str(args.asset_view or "canonical_merged"),
                    max_workers=args.max_workers or None,
                    resume=not bool(args.no_resume),
                    prefer_local_link=not bool(args.disable_local_link),
                    run_hot_cache_governance=not bool(args.skip_hot_cache_governance),
                    force_hot_cache_governance=True,
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
        return


def _cli_cmd_launch_detached(args: argparse.Namespace) -> None:
    launch_command = list(args.launch_command or [])
    if launch_command and launch_command[0] == "--":
        launch_command = launch_command[1:]
    if not launch_command:
        raise SystemExit("launch-detached requires a command after --")
    print(
        json.dumps(
            launch_detached_command(
                command=launch_command,
                log_path=str(args.log_path or ""),
                cwd=str(args.cwd or ""),
                description=str(args.description or ""),
                startup_wait_seconds=float(args.startup_wait_seconds or 0.2),
                status_path=str(args.status_path or ""),
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_bootstrap(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(json.dumps(orchestrator.bootstrap(), ensure_ascii=False, indent=2))
    return


def _cli_cmd_run_job(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    if args.asset_view:
        payload["asset_view"] = args.asset_view
    if args.must_have_facet:
        payload["must_have_facets"] = list(args.must_have_facet)
    if args.must_have_primary_role_bucket:
        payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
    print(json.dumps(orchestrator.run_job(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_plan(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    if args.asset_view:
        payload["asset_view"] = args.asset_view
    if args.must_have_facet:
        payload["must_have_facets"] = list(args.must_have_facet)
    if args.must_have_primary_role_bucket:
        payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
    print(json.dumps(orchestrator.plan_workflow(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_explain_workflow(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    if args.file:
        payload = json.loads(Path(args.file).read_text())
    elif args.plan_review_id > 0:
        payload = {"plan_review_id": int(args.plan_review_id)}
    else:
        raise SystemExit("explain-workflow requires either --file or --plan-review-id")
    if args.asset_view:
        payload["asset_view"] = args.asset_view
    if args.must_have_facet:
        payload["must_have_facets"] = list(args.must_have_facet)
    if args.must_have_primary_role_bucket:
        payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
    print(json.dumps(orchestrator.explain_workflow(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_intake_excel(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.ingest_excel_contacts(
                {
                    "file_path": str(args.file or "").strip(),
                    "target_company": str(args.target_company or "").strip(),
                    "snapshot_id": str(args.snapshot_id or "").strip(),
                    "attach_to_snapshot": bool(args.attach_to_snapshot),
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_continue_excel_intake(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    print(json.dumps(orchestrator.continue_excel_intake_review(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_promote_asset_default_pointer(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    coverage_proof: dict[str, Any] = {}
    if str(args.coverage_proof_file or "").strip():
        coverage_proof = json.loads(Path(args.coverage_proof_file).read_text())
    elif str(args.coverage_proof_json or "").strip():
        coverage_proof = json.loads(str(args.coverage_proof_json or "{}"))
    print(
        json.dumps(
            orchestrator.promote_asset_default_pointer(
                {
                    "company_key": str(args.company or "").strip(),
                    "snapshot_id": str(args.snapshot_id or "").strip(),
                    "scope_kind": str(args.scope_kind or "company").strip(),
                    "scope_key": str(args.scope_key or "").strip(),
                    "asset_kind": str(args.asset_kind or "company_asset").strip(),
                    "lifecycle_status": str(args.lifecycle_status or "canonical").strip(),
                    "coverage_proof": coverage_proof,
                    "promoted_by_job_id": str(args.promoted_by_job_id or "").strip(),
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_review_plan(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    if args.file:
        payload = json.loads(Path(args.file).read_text())
    else:
        compiled = orchestrator.compile_plan_review_instruction(
            {
                "review_id": int(args.review_id),
                "instruction": str(args.instruction or ""),
                "reviewer": str(args.reviewer or ""),
                "notes": str(args.notes or ""),
                "action": str(args.action or "approved"),
            }
        )
        if compiled.get("status") == "not_found":
            raise SystemExit(f"Plan review session {args.review_id} not found")
        if compiled.get("status") == "invalid":
            raise SystemExit(str(compiled.get("reason") or "invalid review-plan instruction payload"))
        if args.preview:
            print(json.dumps(compiled, ensure_ascii=False, indent=2))
            return
        payload = dict(compiled.get("review_payload") or {})
        reviewed = orchestrator.review_plan_session(payload)
        if isinstance(reviewed, dict):
            reviewed["instruction_compiler"] = dict(compiled.get("instruction_compiler") or {})
            reviewed["intent_rewrite"] = dict(compiled.get("intent_rewrite") or {})
        print(json.dumps(reviewed, ensure_ascii=False, indent=2))
        return
    print(json.dumps(orchestrator.review_plan_session(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_refine_results(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    if args.file:
        payload = json.loads(Path(args.file).read_text())
    else:
        payload = {
            "job_id": str(args.job_id or ""),
            "instruction": str(args.instruction or ""),
        }
        compiled = orchestrator.compile_post_acquisition_refinement(payload)
        if compiled.get("status") == "not_found":
            raise SystemExit(f"Baseline job {args.job_id} not found")
        if compiled.get("status") == "invalid":
            raise SystemExit(str(compiled.get("reason") or "invalid refine-results payload"))
        if args.preview:
            print(json.dumps(compiled, ensure_ascii=False, indent=2))
            return
    print(json.dumps(orchestrator.apply_post_acquisition_refinement(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_plan_reviews(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.list_plan_review_sessions(args.target_company)
    if args.brief:
        reviews = []
        for item in list(result.get("plan_reviews") or []):
            request_payload = dict(item.get("request") or {})
            reviews.append(
                {
                    "review_id": item.get("review_id"),
                    "target_company": item.get("target_company"),
                    "status": item.get("status"),
                    "risk_level": item.get("risk_level"),
                    "required_before_execution": item.get("required_before_execution"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "raw_user_request": request_payload.get("raw_user_request"),
                }
            )
        result = {"plan_reviews": reviews}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_start_workflow(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    if args.file:
        payload = json.loads(Path(args.file).read_text())
    elif args.plan_review_id > 0:
        payload = {"plan_review_id": int(args.plan_review_id)}
    else:
        raise SystemExit("start-workflow requires either --file or --plan-review-id")
    if args.asset_view:
        payload["asset_view"] = args.asset_view
    if args.must_have_facet:
        payload["must_have_facets"] = list(args.must_have_facet)
    if args.must_have_primary_role_bucket:
        payload["must_have_primary_role_buckets"] = list(args.must_have_primary_role_bucket)
    if args.blocking:
        print(json.dumps(orchestrator.run_workflow_blocking(payload), ensure_ascii=False, indent=2))
        return
    if args.no_auto_job_daemon:
        payload["auto_job_daemon"] = False
    payload = normalize_workflow_submission_payload(
        payload,
        default_runtime_execution_mode=str(args.runtime_execution_mode or "hosted"),
        hosted_auto_job_daemon=False,
    )
    if workflow_runtime_uses_managed_runner(payload.get("runtime_execution_mode")):
        queued = orchestrator.start_workflow_runner_managed(payload)
    else:
        hosted_api_base_url = _resolve_hosted_api_base_url(args.hosted_api_base_url)
        try:
            queued = _submit_hosted_workflow_request(
                payload,
                base_url=hosted_api_base_url,
                timeout_seconds=float(args.hosted_api_timeout_seconds or 15.0),
            )
        except HostedWorkflowSubmissionError as exc:
            raise SystemExit(
                "Hosted workflow submission failed. Start `serve` and retry, or use "
                "`--runtime-execution-mode managed_subprocess` for a standalone local run. "
                f"Details: {exc}"
            ) from exc
    print(json.dumps(queued, ensure_ascii=False, indent=2))
    return


def _cli_cmd_execute_workflow(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    recovery_payload: dict[str, object] | None = None
    if args.auto_job_daemon:
        recovery_payload = {"auto_job_daemon": True}
    print(
        json.dumps(
            {
                "event": "workflow_runner_started",
                "job_id": str(args.job_id or ""),
                "auto_job_daemon": bool(args.auto_job_daemon),
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
        flush=True,
    )
    result = orchestrator.run_queued_workflow(args.job_id, recovery_payload=recovery_payload)
    print(
        json.dumps(
            {
                "event": "workflow_runner_finished",
                "job_id": str(args.job_id or ""),
                "status": str(result.get("status") or ""),
                "stage": str(result.get("artifact", {}).get("summary", {}).get("stage") or ""),
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
        flush=True,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_supervise_workflow(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            {
                "event": "workflow_runner_started",
                "job_id": str(args.job_id or ""),
                "auto_job_daemon": bool(args.auto_job_daemon),
                "mode": "supervisor",
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
        flush=True,
    )
    result = orchestrator.run_workflow_supervisor(
        args.job_id,
        auto_job_daemon=bool(args.auto_job_daemon),
        poll_seconds=float(args.poll_seconds or 2.0),
        max_ticks=int(args.max_ticks or 0),
    )
    print(
        json.dumps(
            {
                "event": "workflow_runner_finished",
                "job_id": str(args.job_id or ""),
                "status": str(result.get("status") or ""),
                "stage": str(result.get("stage") or ""),
                "mode": "supervisor",
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
        flush=True,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_job(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.get_job_results(args.job_id)
    if result is None:
        raise SystemExit(f"Job {args.job_id} not found")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_progress(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.get_job_progress(args.job_id)
    if result is None:
        raise SystemExit(f"Job {args.job_id} not found")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_system_progress(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.get_system_progress(
                {
                    "active_limit": args.active_limit,
                    "object_sync_limit": args.object_sync_limit,
                    "profile_registry_lookback_hours": args.profile_registry_lookback_hours,
                    "force_refresh": bool(args.force_refresh),
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_show_trace(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.get_job_trace(args.job_id)
    if result is None:
        raise SystemExit(f"Job {args.job_id} not found")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_workers(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.get_job_workers(args.job_id)
    if result is None:
        raise SystemExit(f"Job {args.job_id} not found")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_scheduler(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    result = orchestrator.get_job_scheduler(args.job_id)
    if result is None:
        raise SystemExit(f"Job {args.job_id} not found")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return


def _cli_cmd_cleanup_workflow_duplicates(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.cleanup_duplicate_inflight_workflows(
                {
                    "target_company": args.target_company,
                    "active_limit": args.active_limit,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_cleanup_blocked_workflow_residue(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.cleanup_blocked_workflow_residue(
                {
                    "target_company": args.target_company,
                    "active_limit": args.active_limit,
                    "dry_run": args.dry_run,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_supersede_workflow_jobs(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.supersede_workflow_jobs(
                {
                    "job_ids": list(args.job_id or []),
                    "replacement_job_id": args.replacement_job_id,
                    "reason": args.reason,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_show_recoverable_workers(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.list_recoverable_agent_workers(
                {
                    "stale_after_seconds": args.stale_after_seconds,
                    "lane_id": args.lane_id,
                    "job_id": args.job_id,
                    "limit": args.limit,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_cleanup_recoverable_workers(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.cleanup_recoverable_workers(
                {
                    "stale_after_seconds": args.stale_after_seconds,
                    "lane_id": args.lane_id,
                    "job_id": args.job_id,
                    "target_company": args.target_company,
                    "parent_job_statuses": list(args.parent_job_status or []),
                    "limit": args.limit,
                    "dry_run": bool(args.dry_run),
                    "include_missing_jobs": bool(args.include_missing_jobs),
                    "terminal_workflows_only": bool(args.terminal_workflows_only),
                    "status": args.status,
                    "reason": args.reason,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_interrupt_worker(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(orchestrator.interrupt_agent_worker({"worker_id": args.worker_id}), ensure_ascii=False, indent=2)
    )
    return


def _cli_cmd_run_worker_daemon_once(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    recovery_payload = {
        "owner_id": args.owner_id,
        "lease_seconds": args.lease_seconds,
        "stale_after_seconds": args.stale_after_seconds,
        "total_limit": args.total_limit,
        "job_id": args.job_id,
    }
    if bool(args.disable_search_seed_discovery):
        recovery_payload["search_seed_discovery_enabled"] = False
    if bool(args.disable_profile_prefetch_refill):
        recovery_payload["profile_prefetch_refill_enabled"] = False
    if bool(args.disable_snapshot_full_materialization):
        recovery_payload["snapshot_full_materialization_enabled"] = False
    if bool(args.disable_projection_facet_layering):
        recovery_payload["projection_facet_layering_enabled"] = False
    if bool(args.disable_excel_intake_recovery):
        recovery_payload["excel_intake_recovery_enabled"] = False
    if bool(args.disable_post_recovery_housekeeping):
        recovery_payload["post_recovery_housekeeping_enabled"] = False
    recovery_result = _json_safe_payload(orchestrator.run_worker_recovery_once(recovery_payload))
    print(
        json.dumps(
            _compact_cli_recovery_payload(recovery_result),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_run_worker_daemon(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    recovery_payload = {
        "owner_id": args.owner_id,
        "lease_seconds": args.lease_seconds,
        "stale_after_seconds": args.stale_after_seconds,
        "total_limit": args.total_limit,
        "job_id": args.job_id,
        "poll_seconds": args.poll_seconds,
        "max_ticks": args.max_ticks,
    }
    if bool(args.disable_search_seed_discovery):
        recovery_payload["search_seed_discovery_enabled"] = False
    if bool(args.disable_profile_prefetch_refill):
        recovery_payload["profile_prefetch_refill_enabled"] = False
    if bool(args.disable_snapshot_full_materialization):
        recovery_payload["snapshot_full_materialization_enabled"] = False
    if bool(args.disable_projection_facet_layering):
        recovery_payload["projection_facet_layering_enabled"] = False
    if bool(args.disable_excel_intake_recovery):
        recovery_payload["excel_intake_recovery_enabled"] = False
    if bool(args.disable_post_recovery_housekeeping):
        recovery_payload["post_recovery_housekeeping_enabled"] = False
    print(
        json.dumps(
            orchestrator.run_worker_recovery_forever(recovery_payload),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_run_worker_daemon_service(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    try:
        payload = {
            "service_name": args.service_name,
            "owner_id": args.owner_id,
            "lease_seconds": args.lease_seconds,
            "stale_after_seconds": args.stale_after_seconds,
            "total_limit": args.total_limit,
            "job_id": args.job_id,
            "job_scoped": bool(args.job_scoped),
            "explicit_worker_ids": list(args.explicit_worker_id or []),
            "force_release_explicit_worker_leases": bool(args.force_release_explicit_worker_leases),
            "profile_prefetch_nonblocking_submit": bool(args.profile_prefetch_nonblocking_submit),
            "poll_seconds": args.poll_seconds,
            "max_ticks": args.max_ticks,
            "idle_stop_ticks": args.idle_stop_ticks,
            "workflow_resume_stale_after_seconds": args.workflow_auto_resume_stale_after_seconds,
            "workflow_queue_resume_stale_after_seconds": args.workflow_queue_auto_takeover_stale_after_seconds,
        }
        if bool(args.disable_workflow_auto_resume):
            payload["workflow_auto_resume_enabled"] = False
        if bool(args.disable_workflow_explicit_job_resume):
            payload["workflow_resume_explicit_job"] = False
        if bool(args.disable_workflow_queue_auto_takeover):
            payload["workflow_queue_auto_takeover_enabled"] = False
        if bool(args.disable_profile_prefetch_refill):
            payload["profile_prefetch_refill_enabled"] = False
        if bool(args.profile_prefetch_refill_before_worker_recovery):
            payload["profile_prefetch_refill_before_worker_recovery"] = True
        if bool(args.disable_remote_event_followup):
            payload["remote_event_followup_enabled"] = False
        if bool(args.disable_search_seed_discovery):
            payload["search_seed_discovery_enabled"] = False
        if bool(args.disable_snapshot_full_materialization):
            payload["snapshot_full_materialization_enabled"] = False
        if bool(args.disable_projection_facet_layering):
            payload["projection_facet_layering_enabled"] = False
        if bool(args.disable_excel_intake_recovery):
            payload["excel_intake_recovery_enabled"] = False
        if bool(args.disable_post_recovery_housekeeping):
            payload["post_recovery_housekeeping_enabled"] = False
        print(
            json.dumps(
                orchestrator.run_worker_daemon_service(payload),
                ensure_ascii=False,
                indent=2,
            )
        )
    except SingleInstanceError as exc:
        raise SystemExit(str(exc)) from exc
    return


def _cli_cmd_run_server_runtime_watchdog_service(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    try:
        print(
            json.dumps(
                orchestrator.run_hosted_runtime_watchdog_service(
                    {
                        "hosted_runtime_watchdog_service_name": args.service_name,
                        "shared_service_name": args.shared_service_name,
                        "hosted_runtime_watchdog_poll_seconds": args.poll_seconds,
                        "hosted_runtime_watchdog_max_ticks": args.max_ticks,
                    }
                ),
                ensure_ascii=False,
                indent=2,
            )
        )
    except SingleInstanceError as exc:
        raise SystemExit(str(exc)) from exc
    return


def _cli_cmd_show_daemon_status(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.get_worker_daemon_status(
                {"service_name": args.service_name, "include_details": True}
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_write_worker_daemon_systemd_unit(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.write_worker_daemon_systemd_unit(
                {
                    "service_name": args.service_name,
                    "output_path": args.output_path,
                    "python_bin": args.python_bin,
                    "user_name": args.user_name,
                    "lease_seconds": args.lease_seconds,
                    "stale_after_seconds": args.stale_after_seconds,
                    "total_limit": args.total_limit,
                    "poll_seconds": args.poll_seconds,
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_record_feedback(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    print(json.dumps(orchestrator.record_criteria_feedback(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_review_suggestion(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    print(json.dumps(orchestrator.review_pattern_suggestion(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_review_manual_item(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    print(json.dumps(orchestrator.review_manual_review_item(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_synthesize_manual_review(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.synthesize_manual_review_item(
                {
                    "review_item_id": int(args.review_item_id),
                    "force_refresh": bool(args.force_refresh),
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    return


def _cli_cmd_configure_confidence_policy(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    print(json.dumps(orchestrator.configure_confidence_policy(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_recompile_criteria(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    payload = json.loads(Path(args.file).read_text())
    print(json.dumps(orchestrator.recompile_criteria(payload), ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_criteria(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(json.dumps(orchestrator.list_criteria_patterns(args.target_company), ensure_ascii=False, indent=2))
    return


def _cli_cmd_show_manual_review(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(
        json.dumps(
            orchestrator.list_manual_review_items(args.target_company, args.job_id), ensure_ascii=False, indent=2
        )
    )
    return


def _cli_cmd_test_model(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    print(json.dumps(orchestrator.healthcheck_model(), ensure_ascii=False, indent=2))
    return


def _cli_cmd_serve(args: argparse.Namespace) -> None:
    orchestrator = build_orchestrator()
    shared_recovery_stop = None
    shared_recovery_thread = None
    watchdog_stop = None
    watchdog_thread = None
    server = None
    in_process_recovery_enabled = bool(args.enable_runtime_watchdog)
    try:
        if in_process_recovery_enabled:
            shared_recovery_stop, shared_recovery_thread = start_shared_recovery_service(orchestrator)
            watchdog_stop, watchdog_thread = start_server_runtime_watchdog(
                orchestrator,
                poll_seconds=float(args.runtime_watchdog_poll_seconds or 15.0),
            )
        # C3a: recovery is external by default. The compatibility threads
        # above only exist behind an explicit dev opt-in. If neither that
        # path nor a fresh external daemon covers recovery, refuse to serve.
        assert_recovery_coverage_or_fail_closed(
            orchestrator,
            shared_recovery_thread=shared_recovery_thread,
            watchdog_disabled=not in_process_recovery_enabled,
            allow_uncovered_recovery=bool(getattr(args, "allow_uncovered_recovery", False)),
        )
        orchestrator.start_background_organization_asset_warmup()
        server = create_server(orchestrator, host=args.host, port=args.port)
        print(f"Serving on http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
    finally:
        try:
            if server is not None:
                server.server_close()
        finally:
            if watchdog_stop is not None:
                watchdog_stop.set()
            if shared_recovery_stop is not None:
                shared_recovery_stop.set()
            if watchdog_thread is not None:
                watchdog_thread.join(timeout=max(1.0, float(args.runtime_watchdog_poll_seconds or 15.0)))
            if shared_recovery_thread is not None:
                shared_recovery_thread.join(timeout=5.0)


# WS2 god-file wave: cli slice 1 (2026-07-22) — the top-level dispatch
# ladder is a handler registry; bodies moved verbatim (dedent-only), the
# two grouped blocks keep their shared setup, and the post-prelude
# branches construct the orchestrator per handler (one runs per process).
# Parser configuration is slice 2. Adding a subcommand = one handler +
# one registry row.
_CLI_COMMAND_HANDLERS: dict[str, Callable[[argparse.Namespace], None]] = {
    "show-control-plane-runtime": _cli_cmd_show_control_plane_runtime,
    "run-target-candidate-public-web-experiment": _cli_cmd_target_candidate_public_web_experiment,
    "evaluate-public-web-quality": _cli_cmd_evaluate_public_web_quality,
    "refresh-company-public-web-assets": _cli_cmd_refresh_company_public_web_assets,
    "list-company-public-web-assets": _cli_cmd_list_company_public_web_assets,
    "backfill-linkedin-profile-registry": _cli_cmd_store_command_group_1,
    "backfill-organization-asset-registry": _cli_cmd_store_command_group_1,
    "backfill-authoritative-population-coverage": _cli_cmd_store_command_group_1,
    "repair-authoritative-serving-generation": _cli_cmd_store_command_group_1,
    "normalize-authoritative-source-provenance": _cli_cmd_store_command_group_1,
    "backfill-job-result-lifecycle": _cli_cmd_store_command_group_1,
    "backfill-snapshot-full-materialization-items": _cli_cmd_store_command_group_1,
    "backfill-local-apply-closure-items": _cli_cmd_store_command_group_1,
    "repair-excel-intake-artifacts": _cli_cmd_store_command_group_1,
    "backfill-search-seed-discovery-items": _cli_cmd_store_command_group_1,
    "rebuild-runtime-control-plane": _cli_cmd_store_command_group_1,
    "repair-company-candidate-artifacts": _cli_cmd_store_command_group_1,
    "repair-paginated-candidate-artifacts": _cli_cmd_store_command_group_1,
    "backfill-structured-timeline": _cli_cmd_store_command_group_1,
    "repair-profile-signal-projection": _cli_cmd_store_command_group_1,
    "show-linkedin-profile-registry-metrics": _cli_cmd_store_command_group_1,
    "export-control-plane-snapshot": _cli_cmd_export_control_plane_snapshot,
    "sync-control-plane-postgres": _cli_cmd_sync_control_plane_postgres,
    "export-company-snapshot-bundle": _cli_cmd_store_command_group_2,
    "export-company-handoff-bundle": _cli_cmd_store_command_group_2,
    "export-control-plane-snapshot-bundle": _cli_cmd_store_command_group_2,
    "build-company-candidate-artifacts": _cli_cmd_store_command_group_2,
    "rebuild-company-serving-view": _cli_cmd_store_command_group_2,
    "audit-company-serving-view": _cli_cmd_store_command_group_2,
    "audit-hot-cache-serving-artifacts": _cli_cmd_store_command_group_2,
    "cleanup-hot-cache-serving-artifacts": _cli_cmd_store_command_group_2,
    "audit-authoritative-reuse-planning": _cli_cmd_store_command_group_2,
    "audit-authoritative-reuse-planning-matrix": _cli_cmd_store_command_group_2,
    "compare-authoritative-reuse-planning-matrix": _cli_cmd_store_command_group_2,
    "repoint-job-result-view": _cli_cmd_store_command_group_2,
    "audit-job-result-view-consistency": _cli_cmd_store_command_group_2,
    "segment-company-outreach-layers": _cli_cmd_store_command_group_2,
    "complete-company-assets": _cli_cmd_store_command_group_2,
    "supplement-company-assets": _cli_cmd_store_command_group_2,
    "restore-asset-bundle": _cli_cmd_store_command_group_2,
    "upload-asset-bundle": _cli_cmd_store_command_group_2,
    "publish-candidate-generation": _cli_cmd_store_command_group_2,
    "delete-asset-bundle": _cli_cmd_store_command_group_2,
    "download-asset-bundle": _cli_cmd_store_command_group_2,
    "import-cloud-assets": _cli_cmd_store_command_group_2,
    "hydrate-candidate-generation": _cli_cmd_store_command_group_2,
    "launch-detached": _cli_cmd_launch_detached,
    "bootstrap": _cli_cmd_bootstrap,
    "run-job": _cli_cmd_run_job,
    "plan": _cli_cmd_plan,
    "explain-workflow": _cli_cmd_explain_workflow,
    "intake-excel": _cli_cmd_intake_excel,
    "continue-excel-intake": _cli_cmd_continue_excel_intake,
    "promote-asset-default-pointer": _cli_cmd_promote_asset_default_pointer,
    "review-plan": _cli_cmd_review_plan,
    "refine-results": _cli_cmd_refine_results,
    "show-plan-reviews": _cli_cmd_show_plan_reviews,
    "start-workflow": _cli_cmd_start_workflow,
    "execute-workflow": _cli_cmd_execute_workflow,
    "supervise-workflow": _cli_cmd_supervise_workflow,
    "show-job": _cli_cmd_show_job,
    "show-progress": _cli_cmd_show_progress,
    "show-system-progress": _cli_cmd_show_system_progress,
    "show-trace": _cli_cmd_show_trace,
    "show-workers": _cli_cmd_show_workers,
    "show-scheduler": _cli_cmd_show_scheduler,
    "cleanup-workflow-duplicates": _cli_cmd_cleanup_workflow_duplicates,
    "cleanup-blocked-workflow-residue": _cli_cmd_cleanup_blocked_workflow_residue,
    "supersede-workflow-jobs": _cli_cmd_supersede_workflow_jobs,
    "show-recoverable-workers": _cli_cmd_show_recoverable_workers,
    "cleanup-recoverable-workers": _cli_cmd_cleanup_recoverable_workers,
    "interrupt-worker": _cli_cmd_interrupt_worker,
    "run-worker-daemon-once": _cli_cmd_run_worker_daemon_once,
    "run-worker-daemon": _cli_cmd_run_worker_daemon,
    "run-worker-daemon-service": _cli_cmd_run_worker_daemon_service,
    "run-server-runtime-watchdog-service": _cli_cmd_run_server_runtime_watchdog_service,
    "show-daemon-status": _cli_cmd_show_daemon_status,
    "write-worker-daemon-systemd-unit": _cli_cmd_write_worker_daemon_systemd_unit,
    "record-feedback": _cli_cmd_record_feedback,
    "review-suggestion": _cli_cmd_review_suggestion,
    "review-manual-item": _cli_cmd_review_manual_item,
    "synthesize-manual-review": _cli_cmd_synthesize_manual_review,
    "configure-confidence-policy": _cli_cmd_configure_confidence_policy,
    "recompile-criteria": _cli_cmd_recompile_criteria,
    "show-criteria": _cli_cmd_show_criteria,
    "show-manual-review": _cli_cmd_show_manual_review,
    "test-model": _cli_cmd_test_model,
    "serve": _cli_cmd_serve,
}


if __name__ == "__main__":
    main()
