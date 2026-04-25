from __future__ import annotations

import ast
import csv
import fcntl
import hashlib
import io
import json
import os
import socket
import sqlite3
import sys
import threading
import time
import uuid
import zipfile
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .acquisition import AcquisitionEngine, _normalize_company_employee_shards
from .agent_runtime import AgentRuntimeCoordinator
from .artifact_cache import (
    collect_hot_cache_inventory,
    load_hot_cache_governance_state,
    run_hot_cache_governance_cycle,
)
from .asset_catalog import AssetCatalog
from .asset_paths import (
    resolve_company_snapshot_dir,
    resolve_snapshot_dir_from_source_path,
    resolve_snapshot_serving_artifact_path,
)
from .asset_reuse_planning import apply_asset_reuse_plan_to_sourcing_plan, compile_asset_reuse_plan
from .authoritative_candidates import load_authoritative_candidate_detail, load_authoritative_candidate_details
from .candidate_artifacts import (
    CandidateArtifactError,
    build_company_candidate_artifacts,
    build_evidence_records_from_payloads,
)
from .candidate_artifacts import (
    load_company_snapshot_candidate_documents as _legacy_load_company_snapshot_candidate_documents,
)
from .candidate_materialization import build_asset_population_overlay
from .company_asset_completion import CompanyAssetCompletionManager
from .company_asset_supplement import CompanyAssetSupplementManager
from .company_registry import normalize_company_key
from .confidence_policy import apply_policy_control, build_confidence_policy
from .connectors import CompanyIdentity, CompanyRosterSnapshot
from .control_plane_postgres import (
    load_control_plane_postgres_sync_state,
    sync_runtime_control_plane_to_postgres,
)
from .criteria_evolution import CriteriaEvolutionEngine
from .domain import (
    AcquisitionTask,
    Candidate,
    EvidenceRecord,
    JobRequest,
    derive_candidate_facets,
    derive_candidate_role_bucket,
    normalize_candidate,
)
from .excel_intake import ExcelIntakeService, group_contacts_by_company_hints
from .execution_preferences import merge_execution_preferences, normalize_execution_preferences
from .execution_semantics import (
    compile_asset_reuse_lane_semantics,
    compile_execution_semantics,
    dispatch_requires_delta_for_request,
    requested_dispatch_lane_requirements,
)
from .ingestion import load_bootstrap_bundle
from .local_postgres import resolve_default_control_plane_db_path
from .manual_review import build_manual_review_items
from .manual_review_resolution import apply_manual_review_resolution
from .manual_review_synthesis import compile_manual_review_synthesis
from .model_provider import DeterministicModelClient, ModelClient
from .organization_assets import warmup_existing_organization_assets
from .organization_execution_profile import (
    ensure_organization_execution_profile,
    organization_execution_profile_snapshot_reuse_limit,
)
from .outreach_layering import analyze_company_outreach_layers
from .pattern_suggestions import derive_pattern_suggestions
from .plan_review import apply_plan_review_decision, build_plan_review_gate
from .planning import build_sourcing_plan, hydrate_sourcing_plan
from .post_acquisition_refinement import (
    apply_refinement_patch,
    compile_refinement_patch_from_instruction,
    normalize_refinement_patch,
)
from .process_supervision import (
    build_subprocess_env as _runner_subprocess_env,
)
from .process_supervision import (
    process_alive as _workflow_runner_process_alive,
)
from .process_supervision import (
    read_text_tail as _read_text_tail,
)
from .process_supervision import (
    resolve_timeout as _resolve_timeout,
)
from .process_supervision import (
    service_status_is_ready as _service_status_is_ready,
)
from .process_supervision import (
    spawn_detached_process as _spawn_detached_process,
)
from .process_supervision import (
    wait_for_job_status_transition as _wait_for_job_status_transition,
)
from .process_supervision import (
    wait_for_service_ready as _wait_for_service_ready,
)
from .profile_timeline import (
    candidate_profile_lookup_url as _candidate_profile_lookup_url,
)
from .profile_timeline import (
    normalized_primary_email_metadata as _normalized_primary_email_metadata,
)
from .profile_timeline import (
    normalized_text_lines as _normalized_text_lines,
)
from .profile_timeline import (
    primary_email_requires_suppression as _primary_email_requires_suppression,
)
from .profile_timeline import (
    resolve_candidate_profile_timeline as _resolve_candidate_profile_timeline,
)
from .profile_timeline import (
    timeline_has_complete_profile_detail as _timeline_has_complete_profile_detail,
)
from .query_intent_policy import list_business_rewrite_policy_catalog
from .query_intent_rewrite import interpret_query_intent_rewrite, summarize_query_intent_rewrite
from .recovery_sidecar import (
    build_hosted_runtime_watchdog_command as _build_hosted_runtime_watchdog_command,
)
from .recovery_sidecar import (
    build_hosted_runtime_watchdog_config as _build_hosted_runtime_watchdog_config,
)
from .recovery_sidecar import (
    build_job_scoped_recovery_callback_payload as _build_job_scoped_recovery_callback_payload_impl,
)
from .recovery_sidecar import (
    build_job_scoped_recovery_command as _build_job_scoped_recovery_command,
)
from .recovery_sidecar import (
    build_job_scoped_recovery_config as _build_job_scoped_recovery_config,
)
from .recovery_sidecar import (
    build_recovery_bootstrap_payload as _build_recovery_bootstrap_payload_impl,
)
from .recovery_sidecar import (
    build_shared_recovery_callback_payload as _build_shared_recovery_callback_payload_impl,
)
from .recovery_sidecar import (
    build_shared_recovery_command as _build_shared_recovery_command,
)
from .recovery_sidecar import (
    build_shared_recovery_config as _build_shared_recovery_config,
)
from .request_matching import (
    MATCH_THRESHOLD,
    baseline_selection_reason,
    build_request_family_match_explanation,
    build_request_matching_bundle,
    matching_request_family_signature,
    matching_request_signature,
    request_family_score,
    request_family_signature,
    request_signature,
)
from .request_normalization import (
    build_request_preview_payload as _shared_build_request_preview_payload,
)
from .request_normalization import (
    canonicalize_request_payload as _shared_canonicalize_request_payload,
)
from .request_normalization import (
    expand_request_intent_axes_patch as _shared_expand_request_intent_axes_patch,
)
from .request_normalization import (
    extract_query_signal_terms as _shared_extract_query_signal_terms,
)
from .request_normalization import (
    has_structured_request_signals as _shared_has_structured_request_signals,
)
from .request_normalization import (
    merge_unique_request_string_values as _shared_merge_unique_request_string_values,
)
from .request_normalization import (
    normalize_request_query_signal as _shared_normalize_request_query_signal,
)
from .request_normalization import (
    should_skip_query_signal as _shared_should_skip_query_signal,
)
from .request_normalization import (
    supplement_request_query_signals as _shared_supplement_request_query_signals,
)
from .rerun_policy import decide_rerun_policy
from .result_diff import build_result_diff
from .results_store import (
    open_snapshot_artifact_store,
    read_snapshot_artifact_summary,
    read_snapshot_candidate_shard,
    read_snapshot_candidate_window,
    read_snapshot_publishable_email_lookup,
)
from .retrieval_runtime import (
    OUTREACH_LAYER_KEY_BY_INDEX,
    candidate_source_is_snapshot_authoritative,
    load_candidate_document_candidate_source,
    load_company_snapshot_candidate_documents_with_materialization_fallback,
    load_outreach_layering_context,
    load_retrieval_candidate_source,
    resolve_candidate_source_snapshot_dir,
)
from .review_plan_instructions import compile_review_payload_from_instruction
from .runtime_environment import shared_provider_cache_context
from .runtime_tuning import (
    build_materialization_streaming_budget_report,
    resolved_materialization_global_writer_budget,
    runtime_inflight_slot,
)
from .scoring import score_candidates
from .seed_discovery import SearchSeedSnapshot
from .semantic_provider import SemanticProvider
from .semantic_retrieval import rank_semantic_candidates
from .service_daemon import WorkerDaemonService, read_service_status, render_systemd_unit, request_service_stop
from .snapshot_materializer import SnapshotMaterializer, resolve_snapshot_company_identity
from .snapshot_state import (
    candidate_records_from_payload as _candidate_records_from_payload,
)
from .snapshot_state import (
    company_identity_from_record as _company_identity_from_record,
)
from .snapshot_state import (
    evidence_records_from_payload as _evidence_records_from_payload,
)
from .snapshot_state import (
    load_candidate_document_state as _load_candidate_document_state,
)
from .snapshot_state import (
    merge_background_reconcile_candidate as _merge_background_reconcile_candidate,
)
from .snapshot_state import (
    read_json_dict as _read_json_dict,
)
from .snapshot_state import (
    read_json_list as _read_json_list,
)
from .storage import SQLiteStore, _build_asset_membership_row
from .storage import _json_safe_payload as _storage_json_safe_payload
from .worker_daemon import PersistentWorkerRecoveryDaemon
from .worker_scheduler import effective_worker_status, summarize_scheduler
from .workflow_refresh import (
    extract_progress_metrics as _extract_progress_metrics,
)
from .workflow_refresh import (
    extract_refresh_metrics as _extract_refresh_metrics,
)
from .workflow_refresh import (
    resolve_reconcile_snapshot_dir as _resolve_reconcile_snapshot_dir,
)
from .workflow_refresh import (
    resolve_reconcile_snapshot_id as _resolve_reconcile_snapshot_id,
)
from .workflow_refresh import (
    runtime_refresh_metric_subset as _runtime_refresh_metric_subset,
)
from .workflow_refresh import (
    worker_blocks_acquisition_resume as _worker_blocks_acquisition_resume,
)
from .workflow_refresh import (
    worker_has_background_candidate_output as _worker_has_background_candidate_output,
)
from .workflow_refresh import (
    worker_has_completed_background_company_roster_output as _worker_has_completed_background_company_roster_output,
)
from .workflow_refresh import (
    worker_has_completed_background_harvest_prefetch as _worker_has_completed_background_harvest_prefetch,
)
from .workflow_refresh import (
    worker_has_completed_background_search_output as _worker_has_completed_background_search_output,
)
from .workflow_refresh import (
    worker_has_inline_incremental_ingest_output as _worker_has_inline_incremental_ingest_output,
)
from .workflow_submission import normalize_workflow_submission_payload

# Backwards-compatible patch seam while live reads move to the shared
# authoritative snapshot resolver.
load_company_snapshot_candidate_documents = _legacy_load_company_snapshot_candidate_documents

_WORKFLOW_STAGE_SUMMARY_STAGE_ORDER = (
    "linkedin_stage_1",
    "stage_1_preview",
    "public_web_stage_2",
    "stage_2_final",
)
_CHINA_TIME_ZONE = ZoneInfo("Asia/Shanghai")


def _timeline_requires_primary_email_scrub(
    timeline: dict[str, Any],
    *,
    context: dict[str, Any] | None = None,
) -> bool:
    return _primary_email_requires_suppression(timeline, context=context)


def _normalize_candidate_level_function_ids(
    *sources: Any,
    source_shard_filters: Any | None = None,
) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for source in sources:
        for item in list(source or []):
            text = str(item or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            values.append(text)
    if len(values) != 1:
        return []
    shard_filters = dict(source_shard_filters or {})
    include_ids: list[str] = []
    excluded_ids: set[str] = set()
    for item in list(shard_filters.get("function_ids") or []):
        text = str(item or "").strip()
        if text and text not in include_ids:
            include_ids.append(text)
    for item in list(shard_filters.get("exclude_function_ids") or []):
        text = str(item or "").strip()
        if text:
            excluded_ids.add(text)
    effective_shard_ids = [item for item in include_ids if item not in excluded_ids]
    if effective_shard_ids and values == effective_shard_ids:
        return []
    return values


class SourcingOrchestrator:
    def __init__(
        self,
        catalog: AssetCatalog,
        store: SQLiteStore,
        jobs_dir: str | Path,
        model_client: ModelClient,
        semantic_provider: SemanticProvider,
        acquisition_engine: AcquisitionEngine,
        agent_runtime: AgentRuntimeCoordinator | None = None,
    ) -> None:
        self.catalog = catalog
        self.store = store
        self.jobs_dir = Path(jobs_dir)
        self.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir = self.jobs_dir.parent
        self.job_locks_dir = self.runtime_dir / "job_locks"
        self.job_locks_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_metrics_dir = self.runtime_dir / "runtime_metrics"
        self.runtime_metrics_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_metrics_snapshot_path = self.runtime_metrics_dir / "latest.json"
        self.organization_asset_warmup_status_path = self.runtime_metrics_dir / "organization_asset_warmup.json"
        self.model_client = model_client
        self.semantic_provider = semantic_provider
        self.acquisition_engine = acquisition_engine
        self.criteria_evolution = CriteriaEvolutionEngine(catalog, store, model_client)
        self.agent_runtime = agent_runtime or AgentRuntimeCoordinator(store)
        self.acquisition_engine.worker_runtime = self.agent_runtime
        self.snapshot_materializer = SnapshotMaterializer(
            runtime_dir=self.runtime_dir,
            store=self.store,
            acquisition_engine=self.acquisition_engine,
            model_client=self.model_client,
        )
        self._dispatch_lock = threading.Lock()
        self._progress_takeover_lock = threading.Lock()
        self._progress_takeover_inflight: dict[str, dict[str, Any]] = {}
        self._progress_probe_cache_lock = threading.Lock()
        self._progress_probe_cache: dict[str, dict[str, Any]] = {}
        self._snapshot_publishable_email_lookup_lock = threading.Lock()
        self._snapshot_publishable_email_lookup_cache: dict[tuple[str, str], dict[str, dict[str, dict[str, Any]]]] = {}
        self._asset_population_payload_cache_lock = threading.Lock()
        self._asset_population_payload_cache: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        self._asset_population_summary_cache_lock = threading.Lock()
        self._asset_population_summary_cache: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        self._organization_asset_warmup_lock = threading.Lock()
        self._organization_asset_warmup_thread: threading.Thread | None = None
        self._plan_hydration_lock = threading.Lock()
        self._plan_hydration_inflight: dict[str, dict[str, Any]] = {}
        self._plan_hydration_signature_inflight: dict[str, dict[str, Any]] = {}
        self._plan_hydration_slots = threading.BoundedSemaphore(
            max(1, int(_env_int("SOURCING_PLAN_HYDRATION_MAX_PARALLEL", 2)))
        )
        self._inline_incremental_writer_locks_lock = threading.Lock()
        self._inline_incremental_writer_locks: dict[str, threading.Lock] = {}
        self.acquisition_engine.inline_worker_completion_callback = self._handle_completed_recovery_worker_result
        if hasattr(self.acquisition_engine, "multi_source_enricher"):
            self.acquisition_engine.multi_source_enricher.worker_runtime = self.agent_runtime
            exploratory = getattr(self.acquisition_engine.multi_source_enricher, "exploratory_enricher", None)
            if exploratory is not None:
                exploratory.worker_runtime = self.agent_runtime

    def bootstrap(self) -> dict[str, Any]:
        bundle = load_bootstrap_bundle(self.catalog)
        self.store.replace_bootstrap_data(bundle.candidates, bundle.evidence)
        summary = {
            "status": "bootstrapped",
            "candidate_count": len(bundle.candidates),
            "evidence_count": len(bundle.evidence),
            "candidate_breakdown": bundle.stats.get("candidate_counts", {}),
            "asset_paths": bundle.stats.get("assets", {}),
        }
        bootstrap_path = self.jobs_dir.parent / "bootstrap_summary.json"
        bootstrap_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        return summary

    def ensure_bootstrapped(self) -> dict[str, Any] | None:
        if self.store.candidate_count() == 0:
            return self.bootstrap()
        bootstrap_path = self.jobs_dir.parent / "bootstrap_summary.json"
        if bootstrap_path.exists():
            return json.loads(bootstrap_path.read_text())
        return None

    def start_background_organization_asset_warmup(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        enabled = _coerce_bool(
            payload.get("enabled"),
            _env_bool("STARTUP_ORGANIZATION_ASSET_WARMUP_ENABLED", False),
        )
        if not enabled:
            status_payload = {
                "status": "disabled",
                "reason": "startup_warmup_disabled",
                "observed_at": _utc_now_iso(),
            }
            self._write_organization_asset_warmup_status(status_payload)
            return status_payload

        with self._organization_asset_warmup_lock:
            existing = self._load_organization_asset_warmup_status()
            if self._organization_asset_warmup_thread is not None and self._organization_asset_warmup_thread.is_alive():
                return {
                    **existing,
                    "status": "running",
                    "thread_alive": True,
                }
            asset_view = str(payload.get("asset_view") or "canonical_merged").strip() or "canonical_merged"
            max_companies = _coerce_int(
                payload.get("max_companies"),
                _env_int("STARTUP_ORGANIZATION_ASSET_WARMUP_MAX_COMPANIES", 0),
            )
            started_at = _utc_now_iso()
            self._write_organization_asset_warmup_status(
                {
                    "status": "running",
                    "asset_view": asset_view,
                    "max_companies": max_companies,
                    "started_at": started_at,
                    "observed_at": started_at,
                }
            )
            worker_payload = {
                "asset_view": asset_view,
                "max_companies": max_companies,
                "started_at": started_at,
            }
            thread = threading.Thread(
                target=self._run_background_organization_asset_warmup,
                args=(worker_payload,),
                name="organization-asset-warmup",
                daemon=True,
            )
            self._organization_asset_warmup_thread = thread
            thread.start()
        return {
            "status": "running",
            "asset_view": asset_view,
            "max_companies": max_companies,
            "started_at": started_at,
            "thread_alive": True,
        }

    def _run_background_organization_asset_warmup(self, payload: dict[str, Any]) -> None:
        started_at = str(payload.get("started_at") or _utc_now_iso())
        asset_view = str(payload.get("asset_view") or "canonical_merged").strip() or "canonical_merged"
        max_companies = _coerce_int(payload.get("max_companies"), 0)
        try:
            result = warmup_existing_organization_assets(
                runtime_dir=self.runtime_dir,
                store=self.store,
                asset_view=asset_view,
                max_companies=max_companies,
            )
            self._write_organization_asset_warmup_status(
                {
                    **dict(result or {}),
                    "status": "completed",
                    "asset_view": asset_view,
                    "max_companies": max_companies,
                    "started_at": started_at,
                    "finished_at": _utc_now_iso(),
                }
            )
        except Exception as exc:
            self._write_organization_asset_warmup_status(
                {
                    "status": "failed",
                    "asset_view": asset_view,
                    "max_companies": max_companies,
                    "started_at": started_at,
                    "finished_at": _utc_now_iso(),
                    "error": str(exc),
                }
            )

    def _write_organization_asset_warmup_status(self, payload: dict[str, Any]) -> dict[str, Any]:
        status_payload = {
            **dict(payload or {}),
            "observed_at": str(dict(payload or {}).get("observed_at") or _utc_now_iso()),
        }
        self.organization_asset_warmup_status_path.write_text(json.dumps(status_payload, ensure_ascii=False, indent=2))
        return status_payload

    def _load_organization_asset_warmup_status(self) -> dict[str, Any]:
        if not self.organization_asset_warmup_status_path.exists():
            return {"status": "not_started"}
        try:
            payload = json.loads(self.organization_asset_warmup_status_path.read_text())
        except (OSError, ValueError, json.JSONDecodeError):
            return {"status": "corrupted"}
        if not isinstance(payload, dict):
            return {"status": "corrupted"}
        if str(payload.get("status") or "") == "running" and not (
            self._organization_asset_warmup_thread and self._organization_asset_warmup_thread.is_alive()
        ):
            payload["status"] = "stale"
        payload["thread_alive"] = bool(
            self._organization_asset_warmup_thread and self._organization_asset_warmup_thread.is_alive()
        )
        return payload

    def supplement_company_assets(self, payload: dict[str, Any]) -> dict[str, Any]:
        target_company = str(payload.get("target_company") or payload.get("company") or "").strip()
        if not target_company:
            return {"status": "invalid", "reason": "target_company is required"}
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.acquisition_engine.settings,
            model_client=self.model_client,
        )
        if bool(payload.get("import_local_bootstrap_package")):
            return manager.import_local_bootstrap_package(
                target_company=target_company,
                snapshot_id=str(payload.get("snapshot_id") or "").strip(),
                sync_project_local_package=bool(payload.get("sync_project_local_package", True)),
                build_artifacts=bool(payload.get("build_artifacts", True)),
            )
        return manager.supplement_snapshot(
            target_company=target_company,
            snapshot_id=str(payload.get("snapshot_id") or "").strip(),
            run_former_search_seed=bool(payload.get("run_former_search_seed")),
            former_search_limit=int(payload.get("former_search_limit") or 25),
            former_search_pages=int(payload.get("former_search_pages") or 1),
            former_search_queries=[
                str(item).strip() for item in list(payload.get("former_search_queries") or []) if str(item).strip()
            ],
            former_filter_hints={
                str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
                for key, values in dict(payload.get("former_filter_hints") or {}).items()
            },
            profile_scope=str(payload.get("profile_scope") or "none"),
            profile_limit=int(payload.get("profile_limit") or 0),
            profile_only_missing_detail=bool(payload.get("profile_only_missing_detail", False)),
            profile_force_refresh=bool(payload.get("profile_force_refresh", False)),
            repair_current_roster_profile_refs=bool(payload.get("repair_current_roster_profile_refs", False)),
            repair_current_roster_registry_aliases=bool(payload.get("repair_current_roster_registry_aliases", False)),
            build_artifacts=bool(payload.get("build_artifacts", True)),
        )

    def complete_job_candidate_profiles(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"status": "invalid", "reason": "job_id is required"}
        requested_candidate_ids = _merge_unique_request_string_values(payload.get("candidate_ids") or [])
        if not requested_candidate_ids:
            return {"status": "invalid", "reason": "candidate_ids is required"}
        job = self.store.get_job(normalized_job_id)
        if job is None:
            return {"status": "not_found", "reason": "job not found", "job_id": normalized_job_id}

        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        job_summary = dict(job.get("summary") or {})
        stage1_preview_summary = dict(
            dict(workflow_stage_summaries.get("summaries") or {}).get("stage_1_preview") or {}
        )
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        candidate_source, _ = self._resolve_job_candidate_source(
            job=job,
            request=request,
            job_summary=job_summary,
            stage1_preview_summary=stage1_preview_summary,
        )
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        if not target_company or not snapshot_id:
            return {
                "status": "invalid",
                "reason": "snapshot-backed stage1 results are required before profile completion can run",
                "job_id": normalized_job_id,
            }
        try:
            snapshot_payload = self._load_company_snapshot_candidate_documents_with_materialization_fallback(
                target_company=target_company,
                snapshot_id=snapshot_id,
                view=asset_view,
            )
        except CandidateArtifactError:
            return {
                "status": "invalid",
                "reason": "snapshot candidate documents are unavailable for this job",
                "job_id": normalized_job_id,
                "target_company": target_company,
                "snapshot_id": snapshot_id,
            }

        available_candidate_ids = {
            str(getattr(candidate, "candidate_id", "") or "").strip()
            for candidate in list(snapshot_payload.get("candidates") or [])
            if str(getattr(candidate, "candidate_id", "") or "").strip()
        }
        accepted_candidate_ids = [
            candidate_id for candidate_id in requested_candidate_ids if candidate_id in available_candidate_ids
        ]
        skipped_candidate_ids = [
            candidate_id for candidate_id in requested_candidate_ids if candidate_id not in available_candidate_ids
        ]
        if not accepted_candidate_ids:
            return {
                "status": "invalid",
                "reason": "none of the requested candidate_ids are present in the job snapshot",
                "job_id": normalized_job_id,
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "requested_candidate_ids": requested_candidate_ids,
                "skipped_candidate_ids": skipped_candidate_ids,
            }

        completion_manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.acquisition_engine.settings,
            model_client=self.model_client,
        )
        result = completion_manager.complete_snapshot_profiles(
            target_company=target_company,
            snapshot_id=snapshot_id,
            employment_scope="all",
            profile_limit=0,
            only_missing_profile_detail=False,
            force_refresh=_coerce_bool(payload.get("force_refresh"), True),
            allow_live_refetch_for_unmatched=_coerce_bool(payload.get("allow_live_refetch_for_unmatched"), True),
            build_artifacts=_coerce_bool(payload.get("build_artifacts"), True),
            candidate_ids=accepted_candidate_ids,
        )
        return {
            "status": "completed",
            "job_id": normalized_job_id,
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "requested_candidate_ids": requested_candidate_ids,
            "accepted_candidate_ids": accepted_candidate_ids,
            "skipped_candidate_ids": skipped_candidate_ids,
            "profile_completion": result,
        }

    def ingest_excel_contacts(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            service = ExcelIntakeService(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.acquisition_engine.settings,
                model_client=self.model_client,
            )
            return service.ingest_contacts(payload)
        except Exception as exc:
            return {
                "status": "invalid",
                "reason": str(exc),
            }

    def continue_excel_intake_review(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            service = ExcelIntakeService(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.acquisition_engine.settings,
                model_client=self.model_client,
            )
            return service.continue_review(payload)
        except Exception as exc:
            return {
                "status": "invalid",
                "reason": str(exc),
            }

    def start_excel_intake_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not str(payload.get("file_path") or "").strip() and not str(payload.get("file_content_base64") or "").strip():
            return {
                "status": "invalid",
                "reason": "file_path or file_content_base64 is required",
            }

        normalized_payload = dict(payload)
        explicit_target_company = str(normalized_payload.get("target_company") or "").strip()
        parent_history_id = str(normalized_payload.get("history_id") or "").strip()
        attach_to_snapshot = _coerce_bool(normalized_payload.get("attach_to_snapshot"), True)
        build_artifacts = _coerce_bool(normalized_payload.get("build_artifacts"), True)
        filename = str(normalized_payload.get("filename") or "").strip()
        batch_id = uuid.uuid4().hex[:12]
        service = ExcelIntakeService(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.acquisition_engine.settings,
            model_client=self.model_client,
        )
        prepare_dir = self.runtime_dir / "excel_intake_batches" / batch_id
        prepare_dir.mkdir(parents=True, exist_ok=True)
        try:
            prepared_contact_batch = service.prepare_contacts(normalized_payload, intake_dir=prepare_dir)
        except Exception as exc:
            return {
                "status": "invalid",
                "reason": str(exc),
            }

        prepared_contacts = [
            dict(item)
            for item in list(prepared_contact_batch.get("contacts") or [])
            if isinstance(item, dict)
        ]
        if not prepared_contacts:
            return {
                "status": "invalid",
                "reason": "no_contacts_detected",
            }

        grouped_contacts: dict[str, Any]
        if explicit_target_company:
            grouped_contacts = {
                "groups": [
                    {
                        "company": explicit_target_company,
                        "company_key": normalize_company_key(explicit_target_company),
                        "contacts": [
                            {
                                **contact,
                                "company": explicit_target_company,
                                "route_target_company": explicit_target_company,
                                "uploaded_company": str(contact.get("company") or "").strip(),
                                "company_hints": [explicit_target_company],
                            }
                            for contact in prepared_contacts
                        ],
                        "source_companies": sorted(
                            {
                                str(contact.get("company") or "").strip()
                                for contact in prepared_contacts
                                if str(contact.get("company") or "").strip()
                            }
                        ),
                        "row_count": len(prepared_contacts),
                    }
                ],
                "unassigned_contacts": [],
                "unassigned_row_count": 0,
            }
        else:
            grouped_contacts = group_contacts_by_company_hints(prepared_contacts)

        group_items = grouped_contacts.get("groups")
        if not isinstance(group_items, list):
            group_items = []
        groups = [
            dict(item)
            for item in group_items
            if isinstance(item, dict) and str(item.get("company") or "").strip()
        ]
        if not groups:
            return {
                "status": "invalid",
                "reason": "no_company_groups_detected",
                "unassigned_row_count": _coerce_int(grouped_contacts.get("unassigned_row_count"), 0),
            }

        queued_groups: list[dict[str, Any]] = []
        for group in groups:
            target_company = str(group.get("company") or "").strip()
            group_history_id = parent_history_id if len(groups) == 1 and parent_history_id else str(uuid.uuid4())
            group_query_text = (
                str(normalized_payload.get("query_text") or "").strip()
                if explicit_target_company and len(groups) == 1
                else ""
            ) or f"Excel 批量导入 {target_company} 候选人"
            queued = self._queue_excel_intake_workflow_job(
                target_company=target_company,
                history_id=group_history_id,
                parent_history_id=parent_history_id,
                query_text=group_query_text,
                filename=filename,
                attach_to_snapshot=attach_to_snapshot,
                build_artifacts=build_artifacts,
                batch_id=batch_id,
                source_companies=[
                    str(item or "").strip()
                    for item in list(group.get("source_companies") or [])
                    if str(item or "").strip()
                ],
                prepared_contact_batch={
                    "workbook": dict(prepared_contact_batch.get("workbook") or {}),
                    "schema_inference": dict(prepared_contact_batch.get("schema_inference") or {}),
                    "contacts": [
                        dict(item)
                        for item in list(group.get("contacts") or [])
                        if isinstance(item, dict)
                    ],
                },
            )
            queued["row_count"] = int(group.get("row_count") or 0)
            queued["source_companies"] = [
                str(item or "").strip()
                for item in list(group.get("source_companies") or [])
                if str(item or "").strip()
            ]
            queued_groups.append(queued)

        unassigned_contact_items = grouped_contacts.get("unassigned_contacts")
        if not isinstance(unassigned_contact_items, list):
            unassigned_contact_items = []
        response = {
            "status": "queued",
            "workflow_kind": "excel_intake_batch" if (len(queued_groups) > 1 or not explicit_target_company) else "excel_intake",
            "batch_id": batch_id,
            "input_filename": filename,
            "total_row_count": int(dict(prepared_contact_batch.get("workbook") or {}).get("detected_contact_row_count") or 0),
            "created_job_count": len(queued_groups),
            "group_count": len(queued_groups),
            "unassigned_row_count": _coerce_int(grouped_contacts.get("unassigned_row_count"), 0),
            "unassigned_rows": [
                {
                    "row_key": str(item.get("row_key") or "").strip(),
                    "name": str(item.get("name") or "").strip(),
                    "company": str(item.get("company") or "").strip(),
                    "title": str(item.get("title") or "").strip(),
                }
                for item in unassigned_contact_items[:12]
                if isinstance(item, dict)
            ],
            "groups": queued_groups,
        }
        if len(queued_groups) == 1:
            first_group = queued_groups[0]
            response["job_id"] = str(first_group.get("job_id") or "")
            response["history_id"] = str(first_group.get("history_id") or "")
            response["query_text"] = str(first_group.get("query_text") or "")
        return response

    def _queue_excel_intake_workflow_job(
        self,
        *,
        target_company: str,
        history_id: str,
        parent_history_id: str,
        query_text: str,
        filename: str,
        attach_to_snapshot: bool,
        build_artifacts: bool,
        batch_id: str,
        source_companies: list[str],
        prepared_contact_batch: dict[str, Any],
    ) -> dict[str, Any]:
        request = JobRequest.from_payload(
            {
                "raw_user_request": query_text,
                "query": query_text,
                "target_company": target_company,
                "target_scope": "full_company_asset",
                "asset_view": "canonical_merged",
                "retrieval_strategy": "asset_population",
                "planning_mode": "excel_intake",
                "analysis_stage_mode": "single_stage",
                "employment_statuses": ["current", "former"],
            }
        )
        job_id = uuid.uuid4().hex[:12]
        prepared_contacts = [
            dict(item)
            for item in list(prepared_contact_batch.get("contacts") or [])
            if isinstance(item, dict)
        ]
        initial_stage_payload = {
            "status": "running",
            "title": "Excel 上传",
            "text": "正在解析 Excel 文件并对联系人做本地去重。",
            "workflow_kind": "excel_intake",
            "target_company": target_company,
            "input_filename": filename,
            "row_count": len(prepared_contacts),
            "started_at": _utc_now_iso(),
        }
        initial_summary = {
            "workflow_kind": "excel_intake",
            "message": "Excel intake queued.",
            "target_company": target_company,
            "input_filename": filename,
            "attach_to_snapshot": attach_to_snapshot,
            "build_artifacts": build_artifacts,
            "batch_id": batch_id,
            "row_count": len(prepared_contacts),
            "linkedin_stage_1": initial_stage_payload,
        }
        self._save_excel_intake_job_state(
            job_id=job_id,
            request=request,
            status="queued",
            stage="acquiring",
            summary_payload=initial_summary,
        )
        self.store.append_job_event(
            job_id,
            "acquiring",
            "queued",
            "Excel intake workflow created.",
            {
                "workflow_kind": "excel_intake",
                "target_company": target_company,
                "input_filename": filename,
                "row_count": len(prepared_contacts),
                "batch_id": batch_id,
            },
        )
        self._persist_frontend_history_link(
            history_id=history_id,
            query_text=query_text,
            target_company=target_company,
            job_id=job_id,
            phase="running",
            request_payload=request.to_record(),
            plan_payload=None,
            metadata={
                "source": "excel_intake_workflow",
                "workflow_kind": "excel_intake",
                "attach_to_snapshot": attach_to_snapshot,
                "build_artifacts": build_artifacts,
                "input_filename": filename,
                "batch_id": batch_id,
                "parent_history_id": parent_history_id,
                "row_count": len(prepared_contacts),
                "source_companies": source_companies,
            },
        )
        thread = threading.Thread(
            target=self._run_excel_intake_workflow,
            kwargs={
                "job_id": job_id,
                "request": request,
                "payload": {
                    "filename": filename,
                    "attach_to_snapshot": attach_to_snapshot,
                    "build_artifacts": build_artifacts,
                    "target_company": target_company,
                    "query_text": query_text,
                    "prepared_contact_batch": prepared_contact_batch,
                },
            },
            name=f"excel-intake-{job_id}",
            daemon=True,
        )
        thread.start()
        return {
            "status": "queued",
            "job_id": job_id,
            "history_id": history_id,
            "query_text": query_text,
            "workflow_kind": "excel_intake",
            "target_company": target_company,
        }

    def plan_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        explained = self.explain_workflow(payload)
        request_payload = dict(explained.get("request") or {})
        ingress_request_payload = dict(explained.get("ingress_request") or request_payload)
        plan_payload = dict(explained.get("plan") or {})
        execution_bundle = dict(explained.get("execution_bundle") or {})
        request_preview = dict(explained.get("request_preview") or execution_bundle.get("request_preview") or {})
        plan_review_gate = dict(explained.get("plan_review_gate") or {})
        plan_review_session = dict(explained.get("plan_review_session") or {})
        intent_rewrite = dict(
            explained.get("intent_rewrite") or _build_intent_rewrite_payload(request_payload=request_payload)
        )
        status = str(explained.get("status") or "invalid").strip() or "invalid"
        if not request_payload or not plan_payload:
            return {
                "status": status,
                "reason": str(explained.get("reason") or ""),
                "request": request_payload,
                "request_preview": request_preview,
                "plan": plan_payload,
                "plan_review_gate": plan_review_gate,
                "plan_review_session": _plan_review_session_api_summary(plan_review_session),
                "intent_rewrite": intent_rewrite,
            }

        ingress_request = JobRequest.from_payload(ingress_request_payload or request_payload)
        effective_request = JobRequest.from_payload(request_payload)
        plan = hydrate_sourcing_plan(plan_payload)
        if not int(plan_review_session.get("review_id") or 0):
            plan_review_session = self.store.create_plan_review_session(
                target_company=effective_request.target_company,
                request_payload=effective_request.to_record(),
                plan_payload=plan.to_record(),
                gate_payload=plan_review_gate,
                execution_bundle_payload=execution_bundle,
            )
        self._persist_frontend_history_link(
            history_id=str(payload.get("history_id") or "").strip(),
            query_text=str(payload.get("raw_user_request") or effective_request.raw_user_request or "").strip(),
            target_company=effective_request.target_company,
            review_id=int(plan_review_session.get("review_id") or 0),
            phase="plan",
            request_payload=effective_request.to_record(),
            plan_payload=plan.to_record(),
            metadata={
                "source": "plan_workflow",
                **_frontend_plan_semantics_metadata(explained),
            },
        )
        criteria_artifacts = self._persist_criteria_artifacts(
            request=ingress_request,
            plan_payload=plan.to_record(),
            source_kind="plan",
            compiler_kind="planning",
            job_id="",
        )
        return {
            "status": status,
            "request": effective_request.to_record(),
            "request_preview": request_preview
            or _build_request_preview_payload(
                request=ingress_request,
                effective_request=effective_request,
            ),
            "plan": plan.to_record(),
            "plan_review_gate": plan_review_gate,
            "plan_review_session": _plan_review_session_api_summary(plan_review_session),
            "intent_rewrite": intent_rewrite,
            "dispatch_preview": dict(explained.get("dispatch_preview") or {}),
            "organization_execution_profile": dict(explained.get("organization_execution_profile") or {}),
            "asset_reuse_plan": dict(explained.get("asset_reuse_plan") or {}),
            "lane_preview": dict(explained.get("lane_preview") or {}),
            "effective_execution_semantics": dict(explained.get("effective_execution_semantics") or {}),
            **criteria_artifacts,
        }

    def submit_plan_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_payload = normalize_workflow_submission_payload(dict(payload or {}))
        history_id = str(normalized_payload.get("history_id") or uuid.uuid4()).strip()
        query_text = str(normalized_payload.get("raw_user_request") or "").strip()
        if not query_text:
            return {
                "status": "invalid",
                "reason": "raw_user_request is required",
                "history_id": history_id,
                "phase": "plan",
            }

        existing_link = self.store.get_frontend_history_link(history_id) or {}
        plan_request_id = uuid.uuid4().hex
        queued_at = datetime.now(timezone.utc).isoformat()
        request_payload = dict(existing_link.get("request") or {})
        request_payload.update(dict(normalized_payload or {}))
        request_payload["raw_user_request"] = query_text
        plan_generation = {
            "status": "queued",
            "request_id": plan_request_id,
            "queued_at": queued_at,
            "submitted_at": queued_at,
        }
        self._persist_frontend_history_link(
            history_id=history_id,
            query_text=query_text,
            target_company=(
                str(request_payload.get("target_company") or "").strip()
                or str(existing_link.get("target_company") or "").strip()
            ),
            review_id=int(existing_link.get("review_id") or 0),
            job_id=str(existing_link.get("job_id") or "").strip(),
            phase="plan",
            request_payload=request_payload,
            plan_payload={},
            metadata={
                "source": "plan_workflow_submit",
                "plan_generation": plan_generation,
            },
            merge_metadata=True,
        )
        self._queue_plan_hydration(
            history_id=history_id,
            payload={**dict(normalized_payload or {}), "history_id": history_id},
            plan_request_id=plan_request_id,
            queued_at=queued_at,
        )
        return {
            "status": "pending",
            "history_id": history_id,
            "phase": "plan",
            "query_text": query_text,
            "request": request_payload,
            "request_preview": _build_request_preview_payload(request=request_payload),
            "plan": {},
            "plan_review_gate": {},
            "plan_review_session": {},
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=request_payload),
            "metadata": {
                "plan_generation": plan_generation,
            },
        }

    def _queue_plan_hydration(
        self,
        *,
        history_id: str,
        payload: dict[str, Any],
        plan_request_id: str,
        queued_at: str,
    ) -> None:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return
        request_signature = _plan_hydration_request_signature(payload)
        inflight_record = {
            "request_id": str(plan_request_id or "").strip(),
            "queued_at": str(queued_at or "").strip(),
            "payload": dict(payload or {}),
            "request_signature": request_signature,
        }
        should_start_thread = False
        with self._plan_hydration_lock:
            self._plan_hydration_inflight[normalized_history_id] = inflight_record
            signature_record = dict(self._plan_hydration_signature_inflight.get(request_signature) or {})
            histories = dict(signature_record.get("histories") or {})
            histories[normalized_history_id] = inflight_record
            if signature_record:
                signature_record["histories"] = histories
                signature_record["updated_at"] = _utc_now_iso()
                self._plan_hydration_signature_inflight[request_signature] = signature_record
            else:
                should_start_thread = True
                self._plan_hydration_signature_inflight[request_signature] = {
                    "request_signature": request_signature,
                    "primary_history_id": normalized_history_id,
                    "primary_request_id": str(plan_request_id or "").strip(),
                    "queued_at": str(queued_at or "").strip(),
                    "payload": dict(payload or {}),
                    "histories": histories,
                    "created_at": _utc_now_iso(),
                    "updated_at": _utc_now_iso(),
                }
        if not should_start_thread:
            return
        thread = threading.Thread(
            target=self._run_plan_hydration,
            kwargs={
                "history_id": normalized_history_id,
                "payload": dict(payload or {}),
                "plan_request_id": str(plan_request_id or "").strip(),
                "queued_at": str(queued_at or "").strip(),
                "request_signature": request_signature,
            },
            name=f"plan-hydration-{request_signature[:16]}",
            daemon=True,
        )
        thread.start()

    def _plan_hydration_is_current(self, history_id: str, plan_request_id: str) -> bool:
        normalized_history_id = str(history_id or "").strip()
        normalized_request_id = str(plan_request_id or "").strip()
        if not normalized_history_id or not normalized_request_id:
            return False
        with self._plan_hydration_lock:
            current = dict(self._plan_hydration_inflight.get(normalized_history_id) or {})
        return str(current.get("request_id") or "").strip() == normalized_request_id

    def _current_plan_hydration_consumers(
        self,
        *,
        request_signature: str,
        fallback_history_id: str,
        fallback_payload: dict[str, Any],
        fallback_plan_request_id: str,
        fallback_queued_at: str,
    ) -> list[dict[str, Any]]:
        normalized_signature = str(request_signature or "").strip()
        fallback_record = {
            "history_id": str(fallback_history_id or "").strip(),
            "request_id": str(fallback_plan_request_id or "").strip(),
            "queued_at": str(fallback_queued_at or "").strip(),
            "payload": dict(fallback_payload or {}),
            "request_signature": normalized_signature,
        }
        with self._plan_hydration_lock:
            signature_record = dict(self._plan_hydration_signature_inflight.get(normalized_signature) or {})
            histories = dict(signature_record.get("histories") or {})
            if not histories and fallback_record["history_id"]:
                histories[fallback_record["history_id"]] = dict(fallback_record)
            consumers: list[dict[str, Any]] = []
            for consumer_history_id, raw_record in histories.items():
                record = dict(raw_record or {})
                normalized_history_id = str(consumer_history_id or record.get("history_id") or "").strip()
                normalized_request_id = str(record.get("request_id") or "").strip()
                if not normalized_history_id or not normalized_request_id:
                    continue
                current = dict(self._plan_hydration_inflight.get(normalized_history_id) or {})
                if str(current.get("request_id") or "").strip() != normalized_request_id:
                    continue
                consumers.append(
                    {
                        "history_id": normalized_history_id,
                        "request_id": normalized_request_id,
                        "queued_at": str(record.get("queued_at") or "").strip(),
                        "payload": dict(record.get("payload") or {}),
                        "request_signature": normalized_signature,
                    }
                )
        return consumers

    def _clear_plan_hydration_inflight(self, history_id: str, plan_request_id: str) -> None:
        normalized_history_id = str(history_id or "").strip()
        normalized_request_id = str(plan_request_id or "").strip()
        if not normalized_history_id or not normalized_request_id:
            return
        with self._plan_hydration_lock:
            current = dict(self._plan_hydration_inflight.get(normalized_history_id) or {})
            if str(current.get("request_id") or "").strip() == normalized_request_id:
                self._plan_hydration_inflight.pop(normalized_history_id, None)
                signature = str(current.get("request_signature") or "").strip()
                signature_record = dict(self._plan_hydration_signature_inflight.get(signature) or {})
                histories = dict(signature_record.get("histories") or {})
                histories.pop(normalized_history_id, None)
                if histories:
                    signature_record["histories"] = histories
                    signature_record["updated_at"] = _utc_now_iso()
                    self._plan_hydration_signature_inflight[signature] = signature_record
                elif signature:
                    self._plan_hydration_signature_inflight.pop(signature, None)

    def _clear_plan_hydration_consumers(self, consumers: list[dict[str, Any]]) -> None:
        for consumer in consumers:
            self._clear_plan_hydration_inflight(
                str(consumer.get("history_id") or ""),
                str(consumer.get("request_id") or ""),
            )

    def _run_plan_hydration(
        self,
        *,
        history_id: str,
        payload: dict[str, Any],
        plan_request_id: str,
        queued_at: str,
        request_signature: str = "",
    ) -> None:
        acquired_slot = False
        started_at = ""
        normalized_request_signature = str(request_signature or "").strip() or _plan_hydration_request_signature(payload)
        consumers: list[dict[str, Any]] = []
        try:
            self._plan_hydration_slots.acquire()
            acquired_slot = True
            consumers = self._current_plan_hydration_consumers(
                request_signature=normalized_request_signature,
                fallback_history_id=history_id,
                fallback_payload=payload,
                fallback_plan_request_id=plan_request_id,
                fallback_queued_at=queued_at,
            )
            if not consumers:
                return

            started_at = datetime.now(timezone.utc).isoformat()
            for consumer in consumers:
                request_payload = dict(consumer.get("payload") or payload or {})
                consumer_queued_at = str(consumer.get("queued_at") or queued_at or "").strip()
                query_text = str(request_payload.get("raw_user_request") or "").strip()
                self._persist_frontend_history_link(
                    history_id=str(consumer.get("history_id") or ""),
                    query_text=query_text,
                    target_company=str(request_payload.get("target_company") or "").strip(),
                    phase="plan",
                    request_payload=request_payload,
                    plan_payload={},
                    metadata={
                        "source": "plan_workflow_async",
                        "plan_generation": {
                            "status": "running",
                            "request_id": str(consumer.get("request_id") or ""),
                            "queued_at": consumer_queued_at,
                            "submitted_at": consumer_queued_at,
                            "started_at": started_at,
                            "request_signature": normalized_request_signature,
                            "coalesced_count": len(consumers),
                            "queue_wait_ms": _milliseconds_between_iso(consumer_queued_at, started_at),
                        },
                    },
                    merge_metadata=True,
                )

            started_perf = time.perf_counter()
            result = self.plan_workflow(dict(payload or {}))
            total_ms = round((time.perf_counter() - started_perf) * 1000, 2)
            consumers = self._current_plan_hydration_consumers(
                request_signature=normalized_request_signature,
                fallback_history_id=history_id,
                fallback_payload=payload,
                fallback_plan_request_id=plan_request_id,
                fallback_queued_at=queued_at,
            )
            if not consumers:
                return

            result_status = str(result.get("status") or "").strip() or "invalid"
            if result_status == "invalid" or not dict(result.get("plan") or {}):
                reason = str(result.get("reason") or "plan_generation_failed").strip() or "plan_generation_failed"
                completed_at = datetime.now(timezone.utc).isoformat()
                for consumer in consumers:
                    request_payload = dict(result.get("request") or consumer.get("payload") or payload or {})
                    consumer_payload = dict(consumer.get("payload") or {})
                    query_text = str(
                        request_payload.get("raw_user_request")
                        or consumer_payload.get("raw_user_request")
                        or payload.get("raw_user_request")
                        or ""
                    ).strip()
                    consumer_queued_at = str(consumer.get("queued_at") or queued_at or "").strip()
                    self._persist_frontend_history_link(
                        history_id=str(consumer.get("history_id") or ""),
                        query_text=query_text,
                        target_company=str(
                            request_payload.get("target_company") or consumer_payload.get("target_company") or ""
                        ).strip(),
                        phase="plan",
                        request_payload=request_payload,
                        plan_payload={},
                        metadata={
                            "source": "plan_workflow_async_failed",
                            "plan_generation": {
                                "status": "failed",
                                "request_id": str(consumer.get("request_id") or ""),
                                "queued_at": consumer_queued_at,
                                "submitted_at": consumer_queued_at,
                                "started_at": started_at,
                                "completed_at": completed_at,
                                "total_ms": total_ms,
                                "error_message": reason,
                                "request_signature": normalized_request_signature,
                                "coalesced_count": len(consumers),
                                "queue_wait_ms": _milliseconds_between_iso(consumer_queued_at, started_at),
                            },
                        },
                        merge_metadata=True,
                    )
                return

            plan_payload = dict(result.get("plan") or {})
            plan_review_session = dict(result.get("plan_review_session") or {})
            completed_at = datetime.now(timezone.utc).isoformat()
            for consumer in consumers:
                consumer_payload = dict(consumer.get("payload") or {})
                request_payload = dict(result.get("request") or consumer_payload or payload or {})
                if consumer_payload.get("history_id"):
                    request_payload["history_id"] = str(consumer_payload.get("history_id") or "").strip()
                query_text = str(
                    request_payload.get("raw_user_request")
                    or consumer_payload.get("raw_user_request")
                    or payload.get("raw_user_request")
                    or ""
                ).strip()
                consumer_queued_at = str(consumer.get("queued_at") or queued_at or "").strip()
                self._persist_frontend_history_link(
                    history_id=str(consumer.get("history_id") or ""),
                    query_text=query_text,
                    target_company=str(request_payload.get("target_company") or "").strip(),
                    review_id=int(plan_review_session.get("review_id") or 0),
                    phase="plan",
                    request_payload=request_payload,
                    plan_payload=plan_payload,
                    metadata={
                        "source": "plan_workflow_async_completed",
                        **_frontend_plan_semantics_metadata(result),
                        "plan_generation": {
                            "status": "completed",
                            "request_id": str(consumer.get("request_id") or ""),
                            "queued_at": consumer_queued_at,
                            "submitted_at": consumer_queued_at,
                            "started_at": started_at,
                            "completed_at": completed_at,
                            "total_ms": total_ms,
                            "plan_status": result_status,
                            "request_signature": normalized_request_signature,
                            "coalesced_count": len(consumers),
                            "queue_wait_ms": _milliseconds_between_iso(consumer_queued_at, started_at),
                        },
                    },
                    merge_metadata=True,
                )
        except Exception as exc:
            consumers = self._current_plan_hydration_consumers(
                request_signature=normalized_request_signature,
                fallback_history_id=history_id,
                fallback_payload=payload,
                fallback_plan_request_id=plan_request_id,
                fallback_queued_at=queued_at,
            )
            completed_at = datetime.now(timezone.utc).isoformat()
            for consumer in consumers:
                request_payload = dict(consumer.get("payload") or payload or {})
                consumer_queued_at = str(consumer.get("queued_at") or queued_at or "").strip()
                self._persist_frontend_history_link(
                    history_id=str(consumer.get("history_id") or ""),
                    query_text=str(request_payload.get("raw_user_request") or "").strip(),
                    target_company=str(request_payload.get("target_company") or "").strip(),
                    phase="plan",
                    request_payload=request_payload,
                    plan_payload={},
                    metadata={
                        "source": "plan_workflow_async_failed",
                        "plan_generation": {
                            "status": "failed",
                            "request_id": str(consumer.get("request_id") or ""),
                            "queued_at": consumer_queued_at,
                            "submitted_at": consumer_queued_at,
                            "started_at": started_at,
                            "completed_at": completed_at,
                            "error_message": str(exc),
                            "request_signature": normalized_request_signature,
                            "coalesced_count": len(consumers),
                            "queue_wait_ms": _milliseconds_between_iso(consumer_queued_at, started_at),
                        },
                    },
                    merge_metadata=True,
                )
        finally:
            if acquired_slot:
                self._plan_hydration_slots.release()
            consumers = consumers or [
                {
                    "history_id": history_id,
                    "request_id": plan_request_id,
                }
            ]
            self._clear_plan_hydration_consumers(consumers)

    def explain_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        started_at = time.perf_counter()
        normalized_submission = normalize_workflow_submission_payload(dict(payload or {}))
        resolve_plan_started_at = time.perf_counter()
        resolved = self._resolve_workflow_plan_for_explain(normalized_submission)
        resolve_plan_ms = round((time.perf_counter() - resolve_plan_started_at) * 1000, 2)
        resolve_plan_step_timings = dict(resolved.get("resolve_plan_timings_ms") or {})
        resolve_plan_timing_breakdown = dict(resolved.get("resolve_plan_timing_breakdown_ms") or {})

        status = str(resolved.get("status") or "invalid").strip() or "invalid"
        request_payload = dict(resolved.get("request") or {})
        plan_payload = dict(resolved.get("plan") or {})
        execution_bundle = dict(resolved.get("execution_bundle") or {})
        request_preview = dict(resolved.get("request_preview") or execution_bundle.get("request_preview") or {})
        plan_review_gate = dict(resolved.get("plan_review_gate") or {})
        plan_review_session = dict(resolved.get("plan_review_session") or {})
        intent_rewrite = dict(
            resolved.get("intent_rewrite") or _build_intent_rewrite_payload(request_payload=request_payload)
        )

        if not request_payload or not plan_payload:
            return {
                "status": status,
                "reason": str(resolved.get("reason") or ""),
                "ingress_normalization": {
                    "raw_payload": dict(payload or {}),
                    "normalized_submission_payload": normalized_submission,
                },
                "planning": {
                    "plan_review_gate": plan_review_gate,
                    "plan_review_session": plan_review_session,
                },
                "timing_breakdown_ms": resolve_plan_timing_breakdown,
                "timings_ms": {
                    **resolve_plan_step_timings,
                    "resolve_plan": resolve_plan_ms,
                    "dispatch_preview": 0.0,
                    "total": round((time.perf_counter() - started_at) * 1000, 2),
                },
            }

        ingress_request = JobRequest.from_payload(
            dict(execution_bundle.get("request") or resolved.get("ingress_request") or request_payload)
        )
        effective_request = JobRequest.from_payload(dict(execution_bundle.get("effective_request") or request_payload))
        plan = hydrate_sourcing_plan(plan_payload)
        organization_execution_profile = dict(
            execution_bundle.get("organization_execution_profile")
            or plan_payload.get("organization_execution_profile")
            or dict(plan_payload.get("acquisition_strategy") or {}).get("organization_execution_profile")
            or {}
        )
        asset_reuse_plan = dict(execution_bundle.get("asset_reuse_plan") or plan_payload.get("asset_reuse_plan") or {})
        candidate_source_preview = {
            "source_kind": "company_snapshot"
            if str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
            else "",
            "snapshot_id": str(asset_reuse_plan.get("baseline_snapshot_id") or ""),
            "asset_view": str(
                asset_reuse_plan.get("baseline_asset_view") or effective_request.asset_view or "canonical_merged"
            ),
        }
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=effective_request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source_preview,
        )

        dispatch_started_at = time.perf_counter()
        runtime_execution_mode = (
            str(normalized_submission.get("runtime_execution_mode") or "hosted").strip().lower() or "hosted"
        )
        dispatch_request, force_fresh_suppression = self._maybe_suppress_inherited_force_fresh_run(
            payload=normalized_submission,
            request=effective_request,
            asset_reuse_plan=asset_reuse_plan,
            runtime_execution_mode=runtime_execution_mode,
        )
        if force_fresh_suppression:
            try:
                asset_reuse_plan = compile_asset_reuse_plan(
                    runtime_dir=self.runtime_dir,
                    store=self.store,
                    request=dispatch_request,
                    plan=plan,
                )
                plan = apply_asset_reuse_plan_to_sourcing_plan(plan, asset_reuse_plan)
            except Exception as exc:
                plan.assumptions = list(plan.assumptions or []) + [
                    f"Asset reuse replanning after force_fresh suppression skipped: {exc}"
                ]
                asset_reuse_plan = dict(plan.asset_reuse_plan or {})
        dispatch_context = self._build_query_dispatch_context(normalized_submission, dispatch_request)
        dispatch_context["asset_reuse_plan"] = asset_reuse_plan
        dispatch_context["organization_execution_profile"] = dict(organization_execution_profile or {})
        if force_fresh_suppression:
            dispatch_context["force_fresh_run_suppression"] = force_fresh_suppression
        dispatch = self._resolve_query_dispatch_decision(dispatch_request, dispatch_context)
        dispatch_preview_ms = round((time.perf_counter() - dispatch_started_at) * 1000, 2)

        strategy = str(dispatch.get("strategy") or "").strip()
        dispatch_effective_request = dispatch_request
        if strategy == "reuse_snapshot":
            dispatch_effective_request = self._request_with_snapshot_reuse(dispatch_request, dispatch)
        elif strategy == "delta_from_snapshot":
            dispatch_effective_request = self._request_with_delta_baseline_hints(
                dispatch_request,
                asset_reuse_plan,
                dispatch,
            )

        request_matching = dict(dispatch_context.get("request_matching") or {})
        request_preview = request_preview or _build_request_preview_payload(
            request=ingress_request,
            effective_request=dispatch_request,
        )
        target_company = str(
            dispatch_request.target_company
            or effective_request.target_company
            or ingress_request.target_company
            or request_payload.get("target_company")
            or ""
        ).strip()
        asset_view = (
            str(
                asset_reuse_plan.get("baseline_asset_view")
                or dispatch_request.asset_view
                or effective_request.asset_view
                or ingress_request.asset_view
                or "canonical_merged"
            ).strip()
            or "canonical_merged"
        )
        cloud_asset_operations = self.store.list_cloud_asset_operations(
            scoped_company=target_company,
            limit=5,
        )
        generation_watermarks = {
            "baseline": {
                "snapshot_id": str(asset_reuse_plan.get("baseline_snapshot_id") or ""),
                "generation_key": str(asset_reuse_plan.get("baseline_generation_key") or ""),
                "generation_sequence": int(asset_reuse_plan.get("baseline_generation_sequence") or 0),
                "generation_watermark": str(asset_reuse_plan.get("baseline_generation_watermark") or ""),
            },
            "organization_execution_profile": {
                "target_company": str(organization_execution_profile.get("target_company") or target_company),
                "asset_view": str(organization_execution_profile.get("asset_view") or asset_view),
                "source_snapshot_id": str(organization_execution_profile.get("source_snapshot_id") or ""),
                "source_generation_key": str(organization_execution_profile.get("source_generation_key") or ""),
                "source_generation_sequence": int(
                    organization_execution_profile.get("source_generation_sequence") or 0
                ),
                "source_generation_watermark": str(
                    organization_execution_profile.get("source_generation_watermark") or ""
                ),
            },
        }

        return {
            "status": status,
            "request": dispatch_request.to_record(),
            "ingress_request": ingress_request.to_record(),
            "plan": plan.to_record(),
            "execution_bundle": execution_bundle,
            "request_preview": request_preview,
            "intent_rewrite": intent_rewrite,
            "plan_review_gate": plan_review_gate,
            "plan_review_session": plan_review_session,
            "organization_execution_profile": organization_execution_profile,
            "asset_reuse_plan": asset_reuse_plan,
            "effective_execution_semantics": effective_execution_semantics,
            "ingress_normalization": {
                "raw_payload": dict(payload or {}),
                "normalized_submission_payload": normalized_submission,
                "prepared_request": ingress_request.to_record(),
                "effective_request": effective_request.to_record(),
                "request_preview": request_preview,
                "intent_rewrite": intent_rewrite,
            },
            "planning": {
                "plan": plan.to_record(),
                "plan_review_gate": plan_review_gate,
                "plan_review_session": plan_review_session,
                "organization_execution_profile": organization_execution_profile,
                "asset_reuse_plan": asset_reuse_plan,
                "effective_execution_semantics": effective_execution_semantics,
            },
            "dispatch_matching_normalization": {
                "runtime_execution_mode": runtime_execution_mode,
                "request_matching": request_matching,
                "matching_request": dict(request_matching.get("matching_request") or {}),
                "matching_family_request": dict(request_matching.get("matching_family_request") or {}),
                "request_signature": str(dispatch_context.get("request_signature") or ""),
                "request_family_signature": str(dispatch_context.get("request_family_signature") or ""),
                "force_fresh_run_suppression": force_fresh_suppression,
                "scope": str(dispatch_context.get("scope") or ""),
                "requester_id": str(dispatch_context.get("requester_id") or ""),
                "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
            },
            "dispatch_preview": {
                **dict(dispatch or {}),
                "request_after_dispatch_hints": dispatch_effective_request.to_record(),
            },
            "lane_preview": self._build_workflow_lane_preview(
                request=dispatch_request,
                asset_reuse_plan=asset_reuse_plan,
                organization_execution_profile=organization_execution_profile,
            ),
            "generation_watermarks": generation_watermarks,
            "cloud_asset_operations": {
                "count": len(cloud_asset_operations),
                "items": cloud_asset_operations,
            },
            "timing_breakdown_ms": {
                **resolve_plan_timing_breakdown,
            },
            "timings_ms": {
                **resolve_plan_step_timings,
                "resolve_plan": resolve_plan_ms,
                "dispatch_preview": dispatch_preview_ms,
                "total": round((time.perf_counter() - started_at) * 1000, 2),
            },
        }

    def start_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload = normalize_workflow_submission_payload(
            {**dict(payload or {}), "runtime_execution_mode": "hosted"},
            default_runtime_execution_mode="hosted",
            hosted_auto_job_daemon=False,
        )
        queued = self.queue_workflow(payload)
        if queued.get("status") == "needs_plan_review":
            return queued

        job_id = str(queued.get("job_id") or "")
        workflow_status = str(queued.get("status") or "").strip()
        if not job_id:
            return queued
        job = self.store.get_job(job_id)
        review_id = int(dict(queued.get("plan_review_session") or {}).get("review_id") or 0)
        history_id = str(payload.get("history_id") or "").strip()
        if not history_id and review_id > 0:
            linked_history = next(
                iter(self.store.list_frontend_history_links_for_review(review_id, limit=1)),
                None,
            )
            history_id = str((linked_history or {}).get("history_id") or "").strip()
        self._persist_frontend_history_link(
            history_id=history_id,
            query_text=(
                str((job or {}).get("request", {}).get("raw_user_request") or "").strip()
                or str(payload.get("raw_user_request") or "").strip()
            ),
            target_company=str((job or {}).get("request", {}).get("target_company") or "").strip(),
            review_id=review_id,
            job_id=job_id,
            phase=self._frontend_history_phase_from_job(job),
            request_payload=dict((job or {}).get("request") or {}),
            plan_payload=dict((job or {}).get("plan") or dict(queued.get("plan") or {})),
            metadata={
                "source": "start_workflow",
                "workflow_status": workflow_status,
                "dispatch": dict(queued.get("dispatch") or {}),
            },
        )
        dispatch_payload = dict(queued.get("dispatch") or {})
        matched_job_status = str(dispatch_payload.get("matched_job_status") or "").strip()
        should_run_worker = workflow_status == "queued" or (
            workflow_status == "joined_existing_job" and matched_job_status == "queued"
        )
        if should_run_worker:
            queued["hosted_dispatch"] = self._start_hosted_workflow_thread(job_id, source="start_workflow")
            queued["shared_recovery"] = self.ensure_shared_recovery(payload)
            queued["job_recovery"] = self.ensure_job_scoped_recovery(job_id, payload)
            return queued
        if workflow_status == "joined_existing_job":
            queued["shared_recovery"] = self.ensure_shared_recovery(payload)
            queued["job_recovery"] = self.ensure_job_scoped_recovery(job_id, payload)
            return queued
        if workflow_status == "reused_completed_job":
            queued["shared_recovery"] = {"status": "not_needed"}
            queued["job_recovery"] = {"status": "not_needed"}
            return queued
        return queued

    def start_workflow_runner_managed(
        self,
        payload: dict[str, Any],
        *,
        handshake_timeout_seconds: float | None = None,
        handshake_poll_seconds: float = 0.1,
        handshake_max_attempts: int = 2,
    ) -> dict[str, Any]:
        payload = normalize_workflow_submission_payload(
            dict(payload or {}),
            default_runtime_execution_mode=str(
                dict(payload or {}).get("runtime_execution_mode") or "managed_subprocess"
            ),
            hosted_auto_job_daemon=False,
        )
        queued = self.queue_workflow(payload)
        queue_status = str(queued.get("status") or "")
        if queue_status == "needs_plan_review":
            return queued
        dispatch = dict(queued.get("dispatch") or {})
        matched_job_status = str(dispatch.get("matched_job_status") or "")
        should_spawn_runner = queue_status == "queued" or (
            queue_status == "joined_existing_job" and matched_job_status == "queued"
        )
        auto_job_daemon = bool(dict(payload or {}).get("auto_job_daemon", False))
        hosted_runtime_watchdog_status = {"status": "disabled", "scope": "hosted_runtime_watchdog"}
        shared_recovery_status = {"status": "disabled", "scope": "shared"}
        recovery_status = {"status": "disabled", "scope": "job_scoped"}
        if auto_job_daemon and (should_spawn_runner or queue_status == "joined_existing_job"):
            hosted_runtime_watchdog_status = self._ensure_hosted_runtime_watchdog_deferred(payload)
            shared_recovery_status = self._ensure_shared_recovery_deferred(payload)
        if should_spawn_runner and auto_job_daemon:
            recovery_status = {
                "status": "supervised_by_workflow_runner",
                "scope": "job_scoped",
                "job_id": str(queued.get("job_id") or ""),
            }
        if should_spawn_runner:
            runner_control = self._start_workflow_runner_with_handshake(
                job_id=str(queued.get("job_id") or ""),
                auto_job_daemon=auto_job_daemon,
                handshake_timeout_seconds=handshake_timeout_seconds,
                poll_seconds=handshake_poll_seconds,
                max_attempts=handshake_max_attempts,
            )
            queued["hosted_runtime_watchdog"] = hosted_runtime_watchdog_status
            queued["shared_recovery"] = shared_recovery_status
            queued["job_recovery"] = recovery_status
            queued["workflow_runner"] = dict(runner_control.get("runner") or {})
            queued["workflow_runner_control"] = runner_control
            self._persist_workflow_runtime_controls_deferred(str(queued.get("job_id") or ""), queued)
            return queued
        queued["hosted_runtime_watchdog"] = (
            hosted_runtime_watchdog_status
            if queue_status == "joined_existing_job"
            else {"status": "not_needed", "scope": "hosted_runtime_watchdog"}
        )
        queued["shared_recovery"] = (
            shared_recovery_status
            if queue_status == "joined_existing_job"
            else {"status": "not_needed", "scope": "shared"}
        )
        queued["job_recovery"] = {"status": "not_needed", "scope": "job_scoped"}
        self._persist_workflow_runtime_controls_deferred(str(queued.get("job_id") or ""), queued)
        return queued

    def ensure_hosted_runtime_watchdog(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._start_hosted_runtime_watchdog(dict(payload or {}))

    def ensure_shared_recovery(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._start_shared_recovery(dict(payload or {}))

    def ensure_job_scoped_recovery(self, job_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._start_job_scoped_recovery(job_id, dict(payload or {}))

    def queue_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_workflow_plan(payload)
        if resolved.get("status") == "needs_plan_review":
            return resolved
        request = JobRequest.from_payload(dict(resolved.get("request") or {}))
        plan = hydrate_sourcing_plan(dict(resolved.get("plan") or {}))
        asset_reuse_plan = dict(plan.asset_reuse_plan or {})
        runtime_execution_mode = str(payload.get("runtime_execution_mode") or "hosted").strip().lower() or "hosted"
        request, force_fresh_suppression = self._maybe_suppress_inherited_force_fresh_run(
            payload=payload,
            request=request,
            asset_reuse_plan=asset_reuse_plan,
            runtime_execution_mode=runtime_execution_mode,
        )
        if force_fresh_suppression:
            try:
                asset_reuse_plan = compile_asset_reuse_plan(
                    runtime_dir=self.runtime_dir,
                    store=self.store,
                    request=request,
                    plan=plan,
                )
                plan = apply_asset_reuse_plan_to_sourcing_plan(plan, asset_reuse_plan)
            except Exception as exc:
                plan.assumptions = list(plan.assumptions or []) + [
                    f"Asset reuse replanning after force_fresh suppression skipped: {exc}"
                ]
                asset_reuse_plan = dict(plan.asset_reuse_plan or {})
        dispatch_context = self._build_query_dispatch_context(payload, request)
        dispatch_context["asset_reuse_plan"] = asset_reuse_plan
        dispatch_context["organization_execution_profile"] = dict(
            plan.organization_execution_profile or plan.acquisition_strategy.organization_execution_profile or {}
        )
        if force_fresh_suppression:
            dispatch_context["force_fresh_run_suppression"] = force_fresh_suppression
        with self._dispatch_lock:
            dispatch = self._resolve_query_dispatch_decision(request, dispatch_context)
            strategy = str(dispatch.get("strategy") or "new_job").strip()
            matched_job = dict(dispatch.get("matched_job") or {})
            matched_snapshot_id = str(dispatch.get("matched_snapshot_id") or "").strip()
            force_reuse_snapshot_only = bool(dispatch.get("force_reuse_snapshot_only"))
            if strategy in {"join_inflight", "reuse_completed"} and matched_job:
                matched_job_id = str(matched_job.get("job_id") or "")
                matched_job_status = str(matched_job.get("status") or "").strip()
                response_status = "joined_existing_job" if strategy == "join_inflight" else "reused_completed_job"
                dispatch_payload = {
                    "strategy": strategy,
                    "scope": str(dispatch.get("scope") or ""),
                    "request_signature": str(dispatch.get("request_signature") or ""),
                    "request_family_signature": str(dispatch.get("request_family_signature") or ""),
                    "matched_job_id": matched_job_id,
                    "matched_job_status": matched_job_status,
                    "matched_job_stage": str(matched_job.get("stage") or ""),
                    "matched_registry_id": int(dispatch.get("matched_registry_id") or 0),
                    "force_reuse_snapshot_only": force_reuse_snapshot_only,
                    "requester_id": str(dispatch_context.get("requester_id") or ""),
                    "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                    "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
                }
                if force_fresh_suppression:
                    dispatch_payload["force_fresh_run_suppressed"] = force_fresh_suppression
                if dispatch_context.get("organization_execution_profile"):
                    dispatch_payload["organization_execution_profile"] = dict(
                        dispatch_context["organization_execution_profile"]
                    )
                request_family_match_explanation = dict(dispatch.get("request_family_match_explanation") or {})
                if request_family_match_explanation:
                    dispatch_payload["request_family_match_explanation"] = request_family_match_explanation
                    dispatch_payload["reuse_basis"] = str(request_family_match_explanation.get("reuse_basis") or "")
                self.store.record_query_dispatch(
                    target_company=request.target_company,
                    request_payload=request.to_record(),
                    strategy=strategy,
                    status=response_status,
                    source_job_id=matched_job_id,
                    created_job_id="",
                    requester_id=str(dispatch_context.get("requester_id") or ""),
                    tenant_id=str(dispatch_context.get("tenant_id") or ""),
                    idempotency_key=str(dispatch_context.get("idempotency_key") or ""),
                    payload=dispatch_payload,
                )
                return {
                    "job_id": matched_job_id,
                    "status": response_status,
                    "stage": str(matched_job.get("stage") or ""),
                    "plan": plan.to_record(),
                    "plan_review_session": resolved.get("plan_review_session") or {},
                    "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
                    "dispatch": dispatch_payload,
                }
            requires_delta_acquisition = self._dispatch_requires_delta_for_request(
                request=request,
                asset_reuse_plan=asset_reuse_plan,
            )
            if strategy == "reuse_snapshot" and matched_snapshot_id:
                if requires_delta_acquisition and not force_reuse_snapshot_only:
                    request = self._request_with_delta_baseline_hints(request, asset_reuse_plan, dispatch)
                    strategy = "delta_from_snapshot"
                else:
                    request = self._request_with_snapshot_reuse(request, dispatch)
            elif requires_delta_acquisition:
                request = self._request_with_delta_baseline_hints(request, asset_reuse_plan, dispatch)
                baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
                if baseline_snapshot_id:
                    strategy = "delta_from_snapshot"
                    matched_snapshot_id = baseline_snapshot_id
                    dispatch = dict(dispatch)
                    dispatch["matched_snapshot_id"] = baseline_snapshot_id
                    snapshot_context = self._load_snapshot_reuse_context_from_snapshot(
                        target_company=request.target_company,
                        snapshot_id=baseline_snapshot_id,
                    )
                    if snapshot_context:
                        dispatch["matched_snapshot_dir"] = str(snapshot_context.get("snapshot_dir") or "")
                        dispatch["matched_snapshot_source_path"] = str(snapshot_context.get("source_path") or "")
                    baseline_selection_explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
                    matched_registry_id = int(
                        dispatch.get("matched_registry_id")
                        or baseline_selection_explanation.get("matched_registry_id")
                        or 0
                    )
                    if matched_registry_id:
                        dispatch["matched_registry_id"] = matched_registry_id
                    if (
                        not dict(dispatch.get("request_family_match_explanation") or {})
                        and baseline_selection_explanation
                    ):
                        dispatch["request_family_match_explanation"] = {
                            "selection_mode": str(
                                baseline_selection_explanation.get("selection_mode")
                                or "organization_asset_registry_lane_coverage"
                            ),
                            "dispatch_strategy": "delta_from_snapshot",
                            "reuse_basis": str(
                                baseline_selection_explanation.get("reuse_basis")
                                or "organization_asset_registry_lane_coverage"
                            ),
                            "matched_registry_id": matched_registry_id,
                            "matched_registry_snapshot_id": str(
                                baseline_selection_explanation.get("matched_registry_snapshot_id")
                                or baseline_snapshot_id
                            ),
                            "current_lane_effective_ready": bool(
                                baseline_selection_explanation.get("current_lane_effective_ready")
                            ),
                            "former_lane_effective_ready": bool(
                                baseline_selection_explanation.get("former_lane_effective_ready")
                            ),
                            "current_lane_effective_candidate_count": int(
                                baseline_selection_explanation.get("current_lane_effective_candidate_count") or 0
                            ),
                            "former_lane_effective_candidate_count": int(
                                baseline_selection_explanation.get("former_lane_effective_candidate_count") or 0
                            ),
                            "lane_requirements_ready": bool(
                                baseline_selection_explanation.get("lane_requirements_ready")
                            ),
                        }
            job_id = self._create_workflow_job(
                request,
                plan,
                dispatch_context=dispatch_context,
                runtime_execution_mode=runtime_execution_mode,
                plan_review_session=dict(resolved.get("plan_review_session") or {}),
            )
            dispatch_payload = {
                "strategy": strategy if strategy in {"reuse_snapshot", "delta_from_snapshot"} else "new_job",
                "scope": str(dispatch.get("scope") or ""),
                "request_signature": str(dispatch.get("request_signature") or ""),
                "request_family_signature": str(dispatch.get("request_family_signature") or ""),
                "matched_job_id": str(matched_job.get("job_id") or ""),
                "matched_job_status": str(matched_job.get("status") or ""),
                "matched_job_stage": str(matched_job.get("stage") or ""),
                "matched_snapshot_id": matched_snapshot_id,
                "matched_snapshot_dir": str(dispatch.get("matched_snapshot_dir") or ""),
                "matched_snapshot_source_path": str(dispatch.get("matched_snapshot_source_path") or ""),
                "matched_registry_id": int(dispatch.get("matched_registry_id") or 0),
                "force_reuse_snapshot_only": force_reuse_snapshot_only,
                "requester_id": str(dispatch_context.get("requester_id") or ""),
                "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
            }
            if force_fresh_suppression:
                dispatch_payload["force_fresh_run_suppressed"] = force_fresh_suppression
            if dispatch_context.get("organization_execution_profile"):
                dispatch_payload["organization_execution_profile"] = dict(
                    dispatch_context["organization_execution_profile"]
                )
            if asset_reuse_plan:
                dispatch_payload["asset_reuse_plan"] = {
                    "baseline_reuse_available": bool(asset_reuse_plan.get("baseline_reuse_available")),
                    "baseline_snapshot_id": str(asset_reuse_plan.get("baseline_snapshot_id") or ""),
                    "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
                    "requires_delta_acquisition": requires_delta_acquisition,
                    "missing_current_company_employee_shard_count": int(
                        asset_reuse_plan.get("missing_current_company_employee_shard_count") or 0
                    ),
                    "missing_current_profile_search_query_count": int(
                        asset_reuse_plan.get("missing_current_profile_search_query_count") or 0
                    ),
                    "missing_former_profile_search_query_count": int(
                        asset_reuse_plan.get("missing_former_profile_search_query_count") or 0
                    ),
                }
            request_family_match_explanation = dict(dispatch.get("request_family_match_explanation") or {})
            if request_family_match_explanation:
                dispatch_payload["request_family_match_explanation"] = request_family_match_explanation
                dispatch_payload["reuse_basis"] = str(request_family_match_explanation.get("reuse_basis") or "")
            self.store.record_query_dispatch(
                target_company=request.target_company,
                request_payload=request.to_record(),
                strategy=strategy if strategy in {"reuse_snapshot", "delta_from_snapshot"} else "new_job",
                status="queued",
                source_job_id=str(matched_job.get("job_id") or "")
                if strategy in {"reuse_snapshot", "delta_from_snapshot"}
                else "",
                created_job_id=job_id,
                requester_id=str(dispatch_context.get("requester_id") or ""),
                tenant_id=str(dispatch_context.get("tenant_id") or ""),
                idempotency_key=str(dispatch_context.get("idempotency_key") or ""),
                payload=dispatch_payload,
            )
        criteria_artifacts = self._persist_criteria_artifacts(
            request=request,
            plan_payload=plan.to_record(),
            source_kind="workflow",
            compiler_kind="planning",
            job_id=job_id,
        )
        return {
            "job_id": job_id,
            "status": "queued",
            "stage": "planning",
            "plan": plan.to_record(),
            "plan_review_session": resolved.get("plan_review_session") or {},
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
            "dispatch": dispatch_payload,
            **criteria_artifacts,
        }

    def run_queued_workflow(
        self,
        job_id: str,
        *,
        recovery_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        recovery_settings = dict(recovery_payload or {})
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        wait_for_terminal = bool(recovery_settings.get("auto_job_daemon", False))
        if self._job_is_terminal(job):
            return {
                "job_id": job_id,
                "status": str(job.get("status") or ""),
                "stage": str(job.get("stage") or ""),
                "reason": "already_terminal",
            }

        run_result: dict[str, Any] = {}
        with self._job_run_lock(job_id) as lock_handle:
            if lock_handle is None:
                latest_job = self.store.get_job(job_id) or {}
                return {
                    "job_id": job_id,
                    "status": "skipped",
                    "stage": str(latest_job.get("stage") or ""),
                    "reason": "already_running",
                }

            job = self.store.get_job(job_id) or {}
            if str(job.get("job_type") or "") != "workflow":
                return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
            if self._job_is_terminal(job):
                return {
                    "job_id": job_id,
                    "status": str(job.get("status") or ""),
                    "stage": str(job.get("stage") or ""),
                    "reason": "already_terminal",
                }
            if recovery_payload is not None:
                self.ensure_shared_recovery(recovery_payload)
                self.ensure_job_scoped_recovery(job_id, recovery_payload)

            job_status = str(job.get("status") or "")
            job_stage = str(job.get("stage") or "")
            request = JobRequest.from_payload(dict(job.get("request") or {}))
            plan = hydrate_sourcing_plan(dict(job.get("plan") or {}))
            if job_status == "blocked" and job_stage == "acquiring":
                run_result = self._resume_blocked_workflow_if_ready(job_id, assume_lock=True)
            elif job_status == "running" and job_stage == "acquiring":
                run_result = self._resume_running_workflow_if_ready(job_id, assume_lock=True)
            elif job_status == "queued":
                self._run_workflow(job_id, request, plan)
                latest_job = self.store.get_job(job_id) or {}
                run_result = {
                    "job_id": job_id,
                    "status": str(latest_job.get("status") or ""),
                    "stage": str(latest_job.get("stage") or ""),
                }
            else:
                run_result = {
                    "job_id": job_id,
                    "status": "skipped",
                    "stage": job_stage,
                    "reason": "job_not_runnable",
                }
        if wait_for_terminal:
            wait_timeout_seconds = max(
                5.0,
                float(recovery_settings.get("job_recovery_poll_seconds") or 2.0)
                * float(recovery_settings.get("job_recovery_max_ticks") or 900)
                + 5.0,
            )
            self._wait_for_workflow_terminal_status(job_id, timeout_seconds=wait_timeout_seconds)
        latest_job = self.store.get_job(job_id) or {}
        return {
            "job_id": job_id,
            "status": str(latest_job.get("status") or run_result.get("status") or ""),
            "stage": str(latest_job.get("stage") or run_result.get("stage") or ""),
            **{key: value for key, value in run_result.items() if key not in {"job_id", "status", "stage"}},
        }

    def run_workflow_supervisor(
        self,
        job_id: str,
        *,
        auto_job_daemon: bool = True,
        poll_seconds: float = 2.0,
        max_ticks: int = 0,
    ) -> dict[str, Any]:
        tick = 0
        last_run_result: dict[str, Any] = {}
        last_recovery_result: dict[str, Any] = {}
        while True:
            tick += 1
            last_run_result = self.run_queued_workflow(job_id)
            latest_job = self.store.get_job(job_id) or {}
            job_status = str(latest_job.get("status") or last_run_result.get("status") or "").strip().lower()
            job_stage = str(latest_job.get("stage") or last_run_result.get("stage") or "").strip().lower()
            job_summary = dict(latest_job.get("summary") or {})
            if job_status in {"completed", "failed"}:
                return {
                    "job_id": job_id,
                    "status": job_status,
                    "stage": job_stage,
                    "ticks": tick,
                    "run_result": last_run_result,
                    "recovery_result": last_recovery_result,
                }
            if (
                job_status == "blocked"
                and job_stage == "retrieving"
                and str(job_summary.get("awaiting_user_action") or "").strip() == "continue_stage2"
            ):
                return {
                    "job_id": job_id,
                    "status": "waiting_for_stage2_approval",
                    "stage": job_stage,
                    "ticks": tick,
                    "run_result": last_run_result,
                }
            if auto_job_daemon:
                runtime_controls = self._build_live_runtime_controls_payload(latest_job) if latest_job else {}
                workers = self.agent_runtime.list_workers(job_id=job_id)
                worker_summary = _job_worker_summary(workers)
                runtime_health = (
                    _classify_job_runtime_health(
                        job=latest_job,
                        workers=workers,
                        worker_summary=worker_summary,
                        runtime_controls=runtime_controls,
                        blocked_task=str(job_summary.get("blocked_task") or ""),
                    )
                    if latest_job
                    else {}
                )
                recovery_payload = {
                    "job_id": job_id,
                    "workflow_stale_scope_job_id": job_id,
                    "workflow_auto_resume_enabled": True,
                    "workflow_resume_stale_after_seconds": 0,
                    "workflow_resume_limit": 1,
                    "workflow_queue_auto_takeover_enabled": True,
                    "workflow_queue_resume_stale_after_seconds": 0,
                    "workflow_queue_resume_limit": 1,
                    "runtime_heartbeat_source": "workflow_supervisor",
                }
                if str(runtime_health.get("classification") or "") in {
                    "runner_not_alive",
                    "runner_takeover_pending",
                    "waiting_on_remote_provider",
                }:
                    recovery_payload["stale_after_seconds"] = 0
                last_recovery_result = self.run_worker_recovery_once(recovery_payload)
                latest_job = self.store.get_job(job_id) or latest_job
                job_status = str(latest_job.get("status") or "").strip().lower()
                job_stage = str(latest_job.get("stage") or "").strip().lower()
                if job_status in {"completed", "failed"}:
                    return {
                        "job_id": job_id,
                        "status": job_status,
                        "stage": job_stage,
                        "ticks": tick,
                        "run_result": last_run_result,
                        "recovery_result": last_recovery_result,
                    }
            if max_ticks > 0 and tick >= max_ticks:
                latest_job = self.store.get_job(job_id) or latest_job
                return {
                    "job_id": job_id,
                    "status": str(latest_job.get("status") or "running"),
                    "stage": str(latest_job.get("stage") or ""),
                    "ticks": tick,
                    "run_result": last_run_result,
                    "recovery_result": last_recovery_result,
                    "reason": "max_ticks_reached",
                }
            time.sleep(max(0.2, float(poll_seconds or 2.0)))

    def _start_hosted_workflow_thread(self, job_id: str, *, source: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found", "source": source}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job", "source": source}

        job_status = str(job.get("status") or "").strip().lower()
        job_stage = str(job.get("stage") or "").strip().lower()
        job_summary = dict(job.get("summary") or {})
        dispatch_mode = ""
        if job_status == "queued":
            dispatch_mode = "workflow"
        elif (
            job_status == "blocked"
            and job_stage == "retrieving"
            and str(job_summary.get("awaiting_user_action") or "") == "continue_stage2"
            and str(job_summary.get("stage2_transition_state") or "") == "queued"
        ):
            dispatch_mode = "continue_stage2"
        active_hosted_dispatch = _active_hosted_dispatch_marker(job_summary.get("hosted_dispatch"))
        if active_hosted_dispatch and str(active_hosted_dispatch.get("mode") or "").strip() in {"", dispatch_mode}:
            return {
                "job_id": job_id,
                "status": "skipped",
                "reason": "hosted_dispatch_inflight",
                "source": source,
                "hosted_dispatch": active_hosted_dispatch,
            }

        if job_status == "queued":
            thread_name = f"hosted-workflow-{job_id}"
            self._persist_hosted_dispatch_marker(
                job_id,
                {
                    "status": "started",
                    "mode": "workflow",
                    "source": source,
                    "thread_name": thread_name,
                    "dispatched_at": _utc_now_iso(),
                },
            )
            thread = threading.Thread(
                target=self.run_queued_workflow,
                kwargs={"job_id": job_id},
                name=thread_name,
                daemon=True,
            )
            thread.start()
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="running",
                detail=f"Hosted runtime watchdog dispatched queued workflow from {source}.",
                payload={"source": source, "mode": "run_queued_workflow"},
            )
            return {"job_id": job_id, "status": "started", "mode": "workflow", "source": source}

        if (
            job_status == "blocked"
            and job_stage == "retrieving"
            and str(job_summary.get("awaiting_user_action") or "") == "continue_stage2"
            and str(job_summary.get("stage2_transition_state") or "") == "queued"
        ):
            thread_name = f"hosted-stage2-{job_id}"
            self._persist_hosted_dispatch_marker(
                job_id,
                {
                    "status": "started",
                    "mode": "continue_stage2",
                    "source": source,
                    "thread_name": thread_name,
                    "dispatched_at": _utc_now_iso(),
                },
            )
            thread = threading.Thread(
                target=self._continue_workflow_stage2_worker,
                kwargs={"job_id": job_id, "approval_payload": {}},
                name=thread_name,
                daemon=True,
            )
            thread.start()
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="running",
                detail=f"Hosted runtime watchdog resumed queued stage-2 analysis from {source}.",
                payload={"source": source, "mode": "continue_stage2"},
            )
            return {"job_id": job_id, "status": "started", "mode": "continue_stage2", "source": source}

        return {
            "job_id": job_id,
            "status": "skipped",
            "reason": "job_not_hosted_dispatchable",
            "job_status": job_status,
            "job_stage": job_stage,
            "source": source,
        }

    def run_hosted_runtime_watchdog_once(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        hosted_dispatch_limit = max(1, int(payload.get("hosted_dispatch_limit") or 20))
        hosted_source = (
            str(payload.get("hosted_runtime_source") or "server_runtime_watchdog").strip() or "server_runtime_watchdog"
        )
        worker_recovery = self.run_worker_recovery_once(
            {
                "auto_job_daemon": False,
                "workflow_resume_explicit_job": False,
                "workflow_auto_resume_enabled": True,
                "workflow_queue_auto_takeover_enabled": False,
                **payload,
            }
        )

        dispatch_candidates = self.store.list_jobs(
            job_type="workflow",
            statuses=["queued", "blocked"],
            limit=hosted_dispatch_limit,
        )
        hosted_dispatch: list[dict[str, Any]] = []
        for job in dispatch_candidates:
            job_id = str(job.get("job_id") or "").strip()
            if not job_id:
                continue
            hosted_dispatch.append(self._start_hosted_workflow_thread(job_id, source=hosted_source))

        control_plane_postgres_sync = sync_runtime_control_plane_to_postgres(
            runtime_dir=self.runtime_dir,
            sqlite_path=getattr(
                self.store,
                "sqlite_shadow_connect_target",
                lambda: resolve_default_control_plane_db_path(self.runtime_dir, base_dir=self.runtime_dir),
            )(),
            dsn=str(payload.get("control_plane_postgres_dsn") or ""),
            tables=list(payload.get("control_plane_postgres_tables") or []),
            truncate_first=bool(payload.get("control_plane_postgres_truncate_first")),
            min_interval_seconds=float(payload.get("control_plane_postgres_sync_min_interval_seconds") or 0.0),
            force=bool(payload.get("control_plane_postgres_sync_force")),
        )
        hot_cache_governance = run_hot_cache_governance_cycle(
            runtime_dir=self.runtime_dir,
            store=self.store,
            min_interval_seconds=float(payload.get("hot_cache_governance_min_interval_seconds") or 0.0),
            force=bool(payload.get("hot_cache_governance_force")),
        )
        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="hosted_runtime_watchdog_once")
        return {
            "status": "completed",
            "mode": "hosted",
            "worker_recovery": worker_recovery,
            "hosted_dispatch": hosted_dispatch,
            "control_plane_postgres_sync": control_plane_postgres_sync,
            "hot_cache_governance": hot_cache_governance,
            "runtime_metrics": {
                "status": str(runtime_metrics.get("status") or ""),
                "observed_at": str(runtime_metrics.get("observed_at") or ""),
                "metrics": dict(runtime_metrics.get("metrics") or {}),
            },
        }

    def cleanup_duplicate_inflight_workflows(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        active_limit = max(1, int(payload.get("active_limit") or 200))
        target_company_filter = str(payload.get("target_company") or "").strip().lower()
        active_jobs = self.store.list_jobs(
            job_type="workflow",
            statuses=["queued", "running", "blocked"],
            limit=active_limit,
        )
        results: list[dict[str, Any]] = []
        for job in active_jobs:
            job_id = str(job.get("job_id") or "").strip()
            if not job_id:
                continue
            request_payload = dict(job.get("request") or {})
            target_company = str(request_payload.get("target_company") or "").strip()
            if target_company_filter and target_company.lower() != target_company_filter:
                continue
            request_sig = str(job.get("matching_request_signature") or "").strip() or matching_request_signature(
                request_payload
            )
            if not request_sig:
                continue
            completed_matches = self.store.list_jobs_by_request_signature(
                request_signature_value=request_sig,
                target_company=target_company,
                statuses=["completed"],
                requester_id=str(job.get("requester_id") or ""),
                tenant_id=str(job.get("tenant_id") or ""),
                scope="auto",
                exclude_job_id=job_id,
                limit=10,
            )
            if not completed_matches:
                continue
            replacement = completed_matches[0]
            replacement_created_at = str(replacement.get("created_at") or "")
            job_created_at = str(job.get("created_at") or "")
            if replacement_created_at and job_created_at and replacement_created_at <= job_created_at:
                continue
            supersede_reason = "Superseded by newer completed workflow with the same request signature."
            superseded = self.store.supersede_workflow_job(
                job_id=job_id,
                replacement_job_id=str(replacement.get("job_id") or ""),
                reason=supersede_reason,
            )
            if superseded is None:
                continue
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="superseded",
                detail=supersede_reason,
                payload={
                    "replacement_job_id": str(replacement.get("job_id") or ""),
                    "replacement_created_at": replacement_created_at,
                    "request_signature": request_sig,
                    "superseded_worker_count": int(superseded.get("superseded_worker_count") or 0),
                },
            )
            results.append(
                {
                    "job_id": job_id,
                    "replacement_job_id": str(replacement.get("job_id") or ""),
                    "target_company": target_company,
                    "superseded_worker_count": int(superseded.get("superseded_worker_count") or 0),
                }
            )
        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="cleanup_duplicate_inflight_workflows")
        return {
            "status": "completed",
            "superseded_jobs": results,
            "superseded_count": len(results),
            "runtime_metrics": {
                "status": str(runtime_metrics.get("status") or ""),
                "observed_at": str(runtime_metrics.get("observed_at") or ""),
                "metrics": dict(runtime_metrics.get("metrics") or {}),
            },
        }

    def _blocked_workflow_cleanup_candidate(
        self,
        job: dict[str, Any],
        *,
        runtime_health: dict[str, Any] | None = None,
        target_company_filter: str = "",
    ) -> dict[str, Any] | None:
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return None
        if str(job.get("job_type") or "").strip().lower() != "workflow":
            return None
        if str(job.get("status") or "").strip().lower() != "blocked":
            return None
        if str(job.get("stage") or "").strip().lower() != "acquiring":
            return None
        request_payload = dict(job.get("request") or {})
        target_company = str(request_payload.get("target_company") or "").strip()
        if target_company_filter and target_company.lower() != target_company_filter:
            return None
        request_family_sig = str(
            job.get("matching_request_family_signature") or ""
        ).strip() or matching_request_family_signature(request_payload)
        if not request_family_sig:
            return None
        runtime_health_payload = dict(runtime_health or {})
        if not runtime_health_payload:
            runtime_controls = self._build_live_runtime_controls_payload(job)
            workers = self.agent_runtime.list_workers(job_id=job_id)
            worker_summary = _job_worker_summary(workers)
            job_summary = dict(job.get("summary") or {})
            runtime_health_payload = _classify_job_runtime_health(
                job=job,
                workers=workers,
                worker_summary=worker_summary,
                runtime_controls=runtime_controls,
                blocked_task=str(job_summary.get("blocked_task") or ""),
            )
        if str(runtime_health_payload.get("classification") or "").strip() != "blocked_ready_for_resume":
            return None
        replacement = self.store.find_latest_job_by_request_family_signature(
            request_family_signature_value=request_family_sig,
            target_company=target_company,
            statuses=["completed", "superseded"],
            requester_id=str(job.get("requester_id") or ""),
            tenant_id=str(job.get("tenant_id") or ""),
            scope="auto",
            exclude_job_id=job_id,
            limit=20,
        )
        if replacement is None:
            return None
        if not _workflow_job_is_newer(replacement, job):
            return None
        replacement_job_id = str(replacement.get("job_id") or "").strip()
        if not replacement_job_id:
            return None
        return {
            "job_id": job_id,
            "target_company": target_company,
            "request_family_signature": request_family_sig,
            "runtime_health": runtime_health_payload,
            "replacement_job_id": replacement_job_id,
            "replacement_status": str(replacement.get("status") or ""),
            "replacement_stage": str(replacement.get("stage") or ""),
            "replacement_created_at": str(replacement.get("created_at") or ""),
            "replacement_updated_at": str(replacement.get("updated_at") or ""),
            "cleanup_reason": (
                "Superseded blocked workflow residue after a newer completed/superseded workflow "
                "covered the same request family."
            ),
        }

    def cleanup_blocked_workflow_residue(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        active_limit = max(1, int(payload.get("active_limit") or 200))
        target_company_filter = str(payload.get("target_company") or "").strip().lower()
        dry_run = _coerce_bool(payload.get("dry_run"), False)
        blocked_jobs = self.store.list_jobs(
            job_type="workflow",
            statuses=["blocked"],
            limit=active_limit,
        )
        candidates: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []
        for job in blocked_jobs:
            candidate = self._blocked_workflow_cleanup_candidate(
                job,
                target_company_filter=target_company_filter,
            )
            if candidate is None:
                continue
            candidates.append(candidate)
            if dry_run:
                continue
            superseded = self.store.supersede_workflow_job(
                job_id=str(candidate.get("job_id") or ""),
                replacement_job_id=str(candidate.get("replacement_job_id") or ""),
                reason=str(candidate.get("cleanup_reason") or ""),
            )
            if superseded is None:
                continue
            self.store.append_job_event(
                str(candidate.get("job_id") or ""),
                stage="runtime_control",
                status="superseded",
                detail=str(candidate.get("cleanup_reason") or ""),
                payload={
                    "replacement_job_id": str(candidate.get("replacement_job_id") or ""),
                    "request_family_signature": str(candidate.get("request_family_signature") or ""),
                    "replacement_status": str(candidate.get("replacement_status") or ""),
                    "replacement_stage": str(candidate.get("replacement_stage") or ""),
                    "superseded_worker_count": int(superseded.get("superseded_worker_count") or 0),
                },
            )
            results.append(
                {
                    **candidate,
                    "status": "superseded",
                    "superseded_worker_count": int(superseded.get("superseded_worker_count") or 0),
                }
            )
        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="cleanup_blocked_workflow_residue")
        return {
            "status": "completed",
            "dry_run": dry_run,
            "cleanup_candidates": candidates,
            "cleanup_candidate_count": len(candidates),
            "results": results,
            "cleaned_count": len(results),
            "runtime_metrics": {
                "status": str(runtime_metrics.get("status") or ""),
                "observed_at": str(runtime_metrics.get("observed_at") or ""),
                "metrics": dict(runtime_metrics.get("metrics") or {}),
            },
        }

    def supersede_workflow_jobs(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        job_ids = [str(item or "").strip() for item in list(payload.get("job_ids") or []) if str(item or "").strip()]
        replacement_job_id = str(payload.get("replacement_job_id") or "").strip()
        reason = str(payload.get("reason") or "").strip() or "Superseded by operator cleanup."
        results: list[dict[str, Any]] = []
        for job_id in job_ids:
            superseded = self.store.supersede_workflow_job(
                job_id=job_id,
                replacement_job_id=replacement_job_id,
                reason=reason,
            )
            if superseded is None:
                results.append({"job_id": job_id, "status": "not_found"})
                continue
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="superseded",
                detail=reason,
                payload={
                    "replacement_job_id": replacement_job_id,
                    "superseded_worker_count": int(superseded.get("superseded_worker_count") or 0),
                },
            )
            results.append(
                {
                    "job_id": job_id,
                    "status": "superseded",
                    "replacement_job_id": replacement_job_id,
                    "superseded_worker_count": int(superseded.get("superseded_worker_count") or 0),
                }
            )
        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="supersede_workflow_jobs")
        return {
            "status": "completed",
            "results": results,
            "runtime_metrics": {
                "status": str(runtime_metrics.get("status") or ""),
                "observed_at": str(runtime_metrics.get("observed_at") or ""),
                "metrics": dict(runtime_metrics.get("metrics") or {}),
            },
        }

    def continue_workflow_stage2(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            return {"status": "invalid", "reason": "job_id is required"}
        job = self.store.get_job(job_id)
        if job is None:
            return {"status": "not_found", "job_id": job_id}
        if str(job.get("job_type") or "") != "workflow":
            return {"status": "invalid", "reason": "job is not a workflow", "job_id": job_id}

        with self._job_run_lock(job_id) as lock_handle:
            if lock_handle is None:
                latest_job = self.store.get_job(job_id) or {}
                return {
                    "status": "conflict",
                    "reason": "workflow_already_running",
                    "job_id": job_id,
                    "job_status": str(latest_job.get("status") or ""),
                    "job_stage": str(latest_job.get("stage") or ""),
                }

            job = self.store.get_job(job_id) or {}
            job_status = str(job.get("status") or "").strip().lower()
            job_stage = str(job.get("stage") or "").strip().lower()
            job_summary = dict(job.get("summary") or {})
            if job_status == "completed":
                return {"status": "already_completed", "job_id": job_id}
            if job_status != "blocked" or job_stage != "retrieving":
                return {
                    "status": "invalid",
                    "reason": "workflow_not_waiting_for_stage2",
                    "job_id": job_id,
                    "job_status": str(job.get("status") or ""),
                    "job_stage": str(job.get("stage") or ""),
                }
            if str(job_summary.get("awaiting_user_action") or "") != "continue_stage2":
                return {
                    "status": "invalid",
                    "reason": "stage2_not_required",
                    "job_id": job_id,
                }
            transition_state = str(job_summary.get("stage2_transition_state") or "").strip().lower()
            if transition_state in {"queued", "running"}:
                return {
                    "status": "conflict",
                    "reason": "stage2_already_requested",
                    "job_id": job_id,
                    "stage2_transition_state": transition_state,
                }

            request_payload = dict(job.get("request") or {})
            request_payload["analysis_stage_mode"] = "two_stage"

            stage1_preview = dict(job_summary.get("stage1_preview") or {})
            queued_summary = {
                **job_summary,
                "message": "Stage 2 analysis approved and queued.",
                "awaiting_user_action": "continue_stage2",
                "stage2_transition_state": "queued",
                "stage1_preview": stage1_preview,
            }
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="blocked",
                stage="retrieving",
                request_payload=request_payload,
                plan_payload=dict(job.get("plan") or {}),
                summary_payload=queued_summary,
                artifact_path=str(job.get("artifact_path") or ""),
            )
            self.store.append_job_event(
                job_id,
                stage="retrieving",
                status="blocked",
                detail="Stage 2 analysis approved and queued for hosted execution.",
                payload={
                    "analysis_stage": "stage_2_final",
                },
            )

        thread = threading.Thread(
            target=self._continue_workflow_stage2_worker,
            kwargs={"job_id": job_id, "approval_payload": payload},
            name=f"continue-stage2-{job_id}",
            daemon=True,
        )
        thread.start()
        return {
            "status": "queued",
            "job_id": job_id,
            "stage": "retrieving",
            "analysis_stage": "stage_2_final",
        }

    def _continue_workflow_stage2_worker(
        self, *, job_id: str, approval_payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        try:
            return self._continue_workflow_stage2_blocking(job_id=job_id, approval_payload=dict(approval_payload or {}))
        except Exception as exc:
            job = self.store.get_job(job_id) or {}
            request_payload = dict(job.get("request") or {})
            plan_payload = dict(job.get("plan") or {})
            if request_payload and plan_payload:
                try:
                    self._mark_workflow_failed(
                        job_id,
                        JobRequest.from_payload(request_payload),
                        hydrate_sourcing_plan(plan_payload),
                        exc,
                    )
                except Exception:
                    pass
            return {"job_id": job_id, "status": "failed", "error": str(exc)}

    def _continue_workflow_stage2_blocking(self, *, job_id: str, approval_payload: dict[str, Any]) -> dict[str, Any]:
        with self._job_run_lock(job_id) as lock_handle:
            if lock_handle is None:
                latest_job = self.store.get_job(job_id) or {}
                return {
                    "job_id": job_id,
                    "status": "skipped",
                    "reason": "already_running",
                    "job_status": str(latest_job.get("status") or ""),
                    "job_stage": str(latest_job.get("stage") or ""),
                }

            job = self.store.get_job(job_id)
            if job is None:
                return {"job_id": job_id, "status": "not_found"}
            if str(job.get("job_type") or "") != "workflow":
                return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}

            job_status = str(job.get("status") or "").strip().lower()
            job_stage = str(job.get("stage") or "").strip().lower()
            job_summary = dict(job.get("summary") or {})
            if job_status != "blocked" or job_stage != "retrieving":
                return {"job_id": job_id, "status": "skipped", "reason": "workflow_not_blocked_for_stage2"}
            if str(job_summary.get("awaiting_user_action") or "") != "continue_stage2":
                return {"job_id": job_id, "status": "skipped", "reason": "stage2_not_required"}

            request_payload = dict(job.get("request") or {})
            request_payload["analysis_stage_mode"] = "two_stage"
            request = JobRequest.from_payload(request_payload)
            plan = hydrate_sourcing_plan(dict(job.get("plan") or {}))
            stage1_preview = dict(job_summary.get("stage1_preview") or {})
            acquiring_summary = dict(job_summary)
            acquiring_summary.update(
                {
                    "message": "Stage 2 analysis is running.",
                    "stage2_transition_state": "running",
                    "awaiting_user_action": "continue_stage2",
                    "stage1_preview": stage1_preview,
                }
            )
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="retrieving",
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                summary_payload=acquiring_summary,
                artifact_path=str(job.get("artifact_path") or ""),
            )
            self.store.update_agent_runtime_session_status(job_id, "running")
            self.store.append_job_event(
                job_id,
                "retrieving",
                "running",
                "Stage 2 AI-assisted analysis started after user approval.",
                {"analysis_stage": "stage_2_final"},
            )

            acquisition_progress = _normalize_acquisition_progress_payload(job_summary.get("acquisition_progress"))
            acquisition_state = self._restore_acquisition_state(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_progress=acquisition_progress,
            )
            layering_summary = self._run_outreach_layering_after_acquisition(
                job_id=job_id,
                request=request,
                acquisition_state=acquisition_state,
                allow_ai=True,
                analysis_stage_label="stage_2_final",
            )
            if layering_summary:
                acquisition_state["outreach_layering"] = layering_summary

            artifact = self._execute_retrieval(
                job_id,
                request,
                plan,
                job_type="workflow",
                runtime_policy={
                    "workflow_snapshot_id": str(acquisition_state.get("snapshot_id") or "").strip(),
                    "outreach_layering": dict(acquisition_state.get("outreach_layering") or {}),
                    "analysis_stage": "stage_2_final",
                },
            )
            final_summary = self._persist_completed_workflow_summary(
                job_id=job_id,
                request=request,
                plan=plan,
                artifact=artifact,
                preserved_summary=acquiring_summary,
            )
            self.store.update_agent_runtime_session_status(job_id, "completed")
            self.store.append_job_event(job_id, "retrieving", "completed", "Stage 2 analysis completed.", final_summary)
            self.store.append_job_event(
                job_id, "completed", "completed", "Workflow completed after stage 2 analysis.", final_summary
            )
            return {
                "job_id": job_id,
                "status": "completed",
                "artifact": artifact,
            }

    def run_workflow_blocking(self, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_workflow_plan(payload)
        if resolved.get("status") == "needs_plan_review":
            return resolved
        request = JobRequest.from_payload(dict(resolved.get("request") or {}))
        plan = hydrate_sourcing_plan(dict(resolved.get("plan") or {}))
        job_id = self._create_workflow_job(
            request,
            plan,
            plan_review_session=dict(resolved.get("plan_review_session") or {}),
        )
        self._persist_criteria_artifacts(
            request=request,
            plan_payload=plan.to_record(),
            source_kind="workflow_blocking",
            compiler_kind="planning",
            job_id=job_id,
        )
        self._run_workflow(job_id, request, plan)
        latest_job = self.store.get_job(job_id) or {}
        latest_status = str(latest_job.get("status") or "").strip().lower()
        latest_stage = str(latest_job.get("stage") or "").strip().lower()
        if latest_stage == "acquiring" and latest_status in {"blocked", "running"}:
            self.run_workflow_supervisor(
                job_id,
                auto_job_daemon=True,
                poll_seconds=float(payload.get("job_recovery_poll_seconds") or 2.0),
                max_ticks=int(payload.get("job_recovery_max_ticks") or 900),
            )
        snapshot = self.get_job_results(job_id)
        if snapshot is None:
            raise RuntimeError(f"Workflow {job_id} disappeared")
        return snapshot

    def run_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = JobRequest.from_payload(self._prepare_request_payload(payload))
        plan = self._build_augmented_sourcing_plan(request)
        job_id = uuid.uuid4().hex[:12]
        criteria_artifacts = self._persist_criteria_artifacts(
            request=request,
            plan_payload=plan.to_record(),
            source_kind="retrieval",
            compiler_kind="planning",
            job_id=job_id,
        )
        artifact = self._run_retrieval_job(
            job_id=job_id,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            job_type="retrieval",
            criteria_artifacts=criteria_artifacts,
            event_detail="Synchronous retrieval job started.",
        )
        return artifact

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        payload = self.store.get_job(job_id)
        if payload is None:
            return None
        payload["events"] = self.store.list_job_events(job_id)
        payload["intent_rewrite"] = _build_intent_rewrite_payload(request_payload=dict(payload.get("request") or {}))
        return payload

    def get_job_api(self, job_id: str, *, include_details: bool = False) -> dict[str, Any] | None:
        payload = self.store.get_job(job_id)
        if payload is None:
            return None
        if include_details:
            payload["events"] = self.store.list_job_events(job_id)
            payload["intent_rewrite"] = _build_intent_rewrite_payload(request_payload=dict(payload.get("request") or {}))
            return payload
        request_payload = dict(payload.get("request") or {})
        return {
            "job_id": str(payload.get("job_id") or ""),
            "job_type": str(payload.get("job_type") or ""),
            "status": str(payload.get("status") or ""),
            "stage": str(payload.get("stage") or ""),
            "artifact_path": str(payload.get("artifact_path") or ""),
            "summary": dict(payload.get("summary") or {}),
            "created_at": str(payload.get("created_at") or ""),
            "updated_at": str(payload.get("updated_at") or ""),
            "request": {
                "raw_user_request": str(request_payload.get("raw_user_request") or ""),
                "query": str(request_payload.get("query") or ""),
                "target_company": str(request_payload.get("target_company") or ""),
                "target_scope": str(request_payload.get("target_scope") or ""),
            },
            "request_preview": _load_job_request_preview(payload),
        }

    def _candidate_record_profile_url_key(self, record: dict[str, Any]) -> str:
        payload = dict(record or {})
        metadata = dict(payload.get("metadata") or {})
        profile_url = _candidate_profile_lookup_url(payload, metadata)
        if not profile_url:
            return ""
        return self.store.normalize_linkedin_profile_url(profile_url)

    def _record_has_publishable_primary_email(self, record: dict[str, Any]) -> bool:
        payload = dict(record or {})
        metadata = dict(payload.get("metadata") or {})
        email = str(
            payload.get("primary_email")
            or payload.get("email")
            or metadata.get("primary_email")
            or metadata.get("email")
            or ""
        ).strip()
        if not email:
            return False
        return not _timeline_requires_primary_email_scrub(
            {},
            context={
                **payload,
                "metadata": metadata,
            },
        )

    def _extract_publishable_primary_email_overlay(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record or {})
        metadata = dict(payload.get("metadata") or {})
        email = str(
            payload.get("primary_email")
            or payload.get("email")
            or metadata.get("primary_email")
            or metadata.get("email")
            or ""
        ).strip()
        if not email:
            return {}
        if _timeline_requires_primary_email_scrub(
            {},
            context={
                **payload,
                "metadata": metadata,
            },
        ):
            return {}
        overlay: dict[str, Any] = {"primary_email": email}
        email_metadata = _normalized_primary_email_metadata(
            payload.get("primary_email_metadata") or metadata.get("primary_email_metadata")
        )
        if email_metadata:
            overlay["primary_email_metadata"] = email_metadata
        return overlay

    def _load_snapshot_publishable_email_lookup(
        self,
        *,
        target_company: str,
        snapshot_id: str,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        normalized_target_company = str(target_company or "").strip()
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if not normalized_target_company or not normalized_snapshot_id:
            return {}
        cache_key = (normalize_company_key(normalized_target_company), normalized_snapshot_id)
        with self._snapshot_publishable_email_lookup_lock:
            cached = self._snapshot_publishable_email_lookup_cache.get(cache_key)
        if cached is not None:
            return cached
        lookup: dict[str, dict[str, dict[str, Any]]] = {
            "by_candidate_id": {},
            "by_profile_url_key": {},
        }
        try:
            artifact_store = open_snapshot_artifact_store(
                runtime_dir=self.runtime_dir,
                target_company=normalized_target_company,
                snapshot_id=normalized_snapshot_id,
                asset_view="strict_roster_only",
            )
            lookup = read_snapshot_publishable_email_lookup(artifact_store)
            if not lookup:
                strict_window = read_snapshot_candidate_window(artifact_store, offset=0, limit=0)
                lookup = self._build_publishable_email_lookup_from_candidate_records(
                    list(strict_window.get("candidates") or [])
                )
        except CandidateArtifactError:
            lookup = {}
        if lookup:
            with self._snapshot_publishable_email_lookup_lock:
                self._snapshot_publishable_email_lookup_cache[cache_key] = lookup
            return lookup
        try:
            snapshot_payload = self._load_company_snapshot_candidate_documents_with_materialization_fallback(
                target_company=normalized_target_company,
                snapshot_id=normalized_snapshot_id,
                view="strict_roster_only",
            )
        except CandidateArtifactError:
            snapshot_payload = {}
        for candidate in list(snapshot_payload.get("candidates") or []):
            record = candidate.to_record()
            overlay = self._extract_publishable_primary_email_overlay(record)
            if not overlay:
                continue
            candidate_id = str(record.get("candidate_id") or "").strip()
            if candidate_id:
                lookup["by_candidate_id"][candidate_id] = dict(overlay)
            profile_url_key = self._candidate_record_profile_url_key(record)
            if profile_url_key:
                lookup["by_profile_url_key"][profile_url_key] = dict(overlay)
        with self._snapshot_publishable_email_lookup_lock:
            self._snapshot_publishable_email_lookup_cache[cache_key] = lookup
        return lookup

    def _build_publishable_email_lookup_from_candidate_records(
        self,
        candidate_records: list[dict[str, Any]],
    ) -> dict[str, dict[str, dict[str, Any]]]:
        lookup: dict[str, dict[str, dict[str, Any]]] = {
            "by_candidate_id": {},
            "by_profile_url_key": {},
        }
        for record in list(candidate_records or []):
            candidate_payload = dict(record or {})
            metadata = dict(candidate_payload.get("metadata") or {})
            primary_email = str(
                candidate_payload.get("primary_email")
                or candidate_payload.get("email")
                or metadata.get("primary_email")
                or metadata.get("email")
                or ""
            ).strip()
            if not primary_email:
                continue
            overlay: dict[str, Any] = {"primary_email": primary_email}
            primary_email_metadata = _normalized_primary_email_metadata(
                candidate_payload.get("primary_email_metadata") or metadata.get("primary_email_metadata")
            )
            if primary_email_metadata:
                overlay["primary_email_metadata"] = primary_email_metadata
            candidate_id = str(candidate_payload.get("candidate_id") or "").strip()
            if candidate_id:
                lookup["by_candidate_id"][candidate_id] = dict(overlay)
            profile_url_key = self._candidate_record_profile_url_key(candidate_payload)
            if profile_url_key:
                lookup["by_profile_url_key"][profile_url_key] = dict(overlay)
        return lookup

    def _load_company_snapshot_candidate_documents_with_materialization_fallback(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        view: str = "canonical_merged",
        allow_materialization_fallback: bool = True,
        allow_candidate_documents_fallback: bool = False,
    ) -> dict[str, Any]:
        return load_company_snapshot_candidate_documents_with_materialization_fallback(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company=target_company,
            snapshot_id=snapshot_id,
            view=view,
            allow_materialization_fallback=allow_materialization_fallback,
            allow_candidate_documents_fallback=allow_candidate_documents_fallback,
        )

    def _organization_execution_profile_from_plan_payload(
        self,
        *,
        plan_payload: dict[str, Any],
        request: JobRequest,
        candidate_source: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        organization_execution_profile = dict(plan_payload.get("organization_execution_profile") or {})
        if not organization_execution_profile:
            organization_execution_profile = dict(
                dict(plan_payload.get("acquisition_strategy") or {}).get("organization_execution_profile") or {}
            )
        if organization_execution_profile or not str(request.target_company or "").strip():
            return organization_execution_profile
        asset_view = (
            str(dict(candidate_source or {}).get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        return self.store.get_organization_execution_profile(
            target_company=request.target_company,
            asset_view=asset_view,
        )

    def _candidate_source_result_view_stub(self, candidate_source: dict[str, Any] | None) -> dict[str, Any]:
        payload = dict(candidate_source or {})
        nested = dict(payload.get("result_view") or {})
        summary_payload = dict(nested.get("summary") or payload.get("result_view_summary") or {})
        metadata_payload = dict(nested.get("metadata") or payload.get("result_view_metadata") or {})
        if str(payload.get("asset_population_overlay_path") or "").strip():
            metadata_payload.setdefault(
                "asset_population_overlay_path",
                str(payload.get("asset_population_overlay_path") or "").strip(),
            )
        if dict(payload.get("asset_population_patch") or {}):
            metadata_payload.setdefault("asset_population_patch", dict(payload.get("asset_population_patch") or {}))
        stub = {
            "view_id": str(nested.get("view_id") or payload.get("result_view_id") or "").strip(),
            "job_id": str(nested.get("job_id") or payload.get("job_id") or "").strip(),
            "target_company": str(nested.get("target_company") or payload.get("target_company") or "").strip(),
            "source_kind": str(nested.get("source_kind") or payload.get("source_kind") or "").strip(),
            "view_kind": str(nested.get("view_kind") or payload.get("result_view_kind") or "").strip(),
            "snapshot_id": str(nested.get("snapshot_id") or payload.get("snapshot_id") or "").strip(),
            "asset_view": str(nested.get("asset_view") or payload.get("asset_view") or "canonical_merged").strip()
            or "canonical_merged",
            "source_path": str(nested.get("source_path") or payload.get("source_path") or "").strip(),
            "authoritative_snapshot_id": str(
                nested.get("authoritative_snapshot_id") or payload.get("authoritative_snapshot_id") or ""
            ).strip(),
            "materialization_generation_key": str(
                nested.get("materialization_generation_key") or payload.get("materialization_generation_key") or ""
            ).strip(),
            "request_signature": str(nested.get("request_signature") or payload.get("request_signature") or "").strip(),
            "summary": summary_payload,
            "metadata": metadata_payload,
        }
        if not any(
            [
                stub["view_id"],
                stub["source_kind"],
                stub["view_kind"],
                stub["snapshot_id"],
                stub["source_path"],
                stub["authoritative_snapshot_id"],
                stub["materialization_generation_key"],
            ]
        ):
            return {}
        return stub

    def _candidate_source_materialized_path(self, *, snapshot_dir: Path, asset_view: str) -> Path | None:
        return resolve_snapshot_serving_artifact_path(
            snapshot_dir,
            asset_view=asset_view,
            allow_candidate_documents_fallback=False,
        )

    def _resolve_candidate_source_snapshot_dir(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
    ) -> Path | None:
        return resolve_candidate_source_snapshot_dir(
            runtime_dir=self.runtime_dir,
            request=request,
            candidate_source=candidate_source,
        )

    def _asset_population_member_row_for_candidate(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        candidate: Candidate,
    ) -> dict[str, Any]:
        return _build_asset_membership_row(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            artifact_kind="organization_asset",
            artifact_key=asset_view,
            candidate_record=candidate.to_record(),
            lane="baseline",
            employment_scope=str(candidate.employment_status or "").strip(),
        )

    def _build_asset_population_generation_patch_candidate_source(
        self,
        *,
        request: JobRequest,
        current_candidate_source: dict[str, Any],
        baseline_candidate_source: dict[str, Any],
        base_generation_key: str,
    ) -> dict[str, Any]:
        normalized_target_company = str(request.target_company or "").strip()
        current_snapshot_id = str(current_candidate_source.get("snapshot_id") or "").strip()
        baseline_snapshot_id = str(baseline_candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(current_candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        resolved_base_generation: dict[str, Any] = {}
        normalized_base_generation_key = str(base_generation_key or "").strip()
        if (
            not normalized_target_company
            or not current_snapshot_id
            or not baseline_snapshot_id
        ):
            return {}
        if normalized_base_generation_key:
            resolved_base_generation = dict(
                self.store._get_asset_materialization_generation_by_key(normalized_base_generation_key) or {}
            )
        if not resolved_base_generation:
            baseline_source_generation_key = str(
                baseline_candidate_source.get("materialization_generation_key")
                or dict(baseline_candidate_source.get("materialization") or {}).get("generation_key")
                or ""
            ).strip()
            if baseline_source_generation_key:
                resolved_base_generation = dict(
                    self.store._get_asset_materialization_generation_by_key(baseline_source_generation_key) or {}
                )
        if not resolved_base_generation:
            resolved_base_generation = dict(
                self.store.get_asset_materialization_generation(
                    target_company=normalized_target_company,
                    snapshot_id=baseline_snapshot_id,
                    asset_view=asset_view,
                    artifact_kind="organization_asset",
                    artifact_key=asset_view,
                )
                or {}
            )
        resolved_base_generation_key = str(resolved_base_generation.get("generation_key") or "").strip()
        if not resolved_base_generation_key:
            return {}
        overlay = build_asset_population_overlay(
            baseline_candidates=list(baseline_candidate_source.get("candidates") or []),
            baseline_evidence_lookup=dict(baseline_candidate_source.get("evidence_lookup") or {}),
            delta_candidates=list(current_candidate_source.get("candidates") or []),
            delta_evidence_lookup=dict(current_candidate_source.get("evidence_lookup") or {}),
            member_key_resolver=lambda candidate: str(
                self._asset_population_member_row_for_candidate(
                    target_company=normalized_target_company,
                    snapshot_id=current_snapshot_id,
                    asset_view=asset_view,
                    candidate=candidate,
                ).get("member_key")
                or ""
            ).strip(),
        )
        merged_candidates = list(overlay.get("candidates") or [])
        if not merged_candidates:
            return {}
        touched_candidates = list(overlay.get("touched_candidates") or [])
        patch_members = [
            self._asset_population_member_row_for_candidate(
                target_company=normalized_target_company,
                snapshot_id=current_snapshot_id,
                asset_view=asset_view,
                candidate=candidate,
            )
            for candidate in touched_candidates
        ]
        patch_generation = self.store.patch_asset_materialization(
            target_company=normalized_target_company,
            snapshot_id=current_snapshot_id,
            asset_view=asset_view,
            artifact_kind="organization_asset",
            artifact_key=asset_view,
            base_generation_key=resolved_base_generation_key,
            source_path=str(current_candidate_source.get("source_path") or "").strip(),
            summary={
                "target_company": normalized_target_company,
                "snapshot_id": current_snapshot_id,
                "asset_view": asset_view,
                "candidate_count": len(merged_candidates),
                "delta_candidate_count": len(touched_candidates),
                "materialization_mode": "generation_member_patch",
            },
            metadata={
                "asset_population_patch": {
                    "base_snapshot_id": baseline_snapshot_id,
                    "delta_snapshot_id": current_snapshot_id,
                    "added_member_keys": list(overlay.get("added_member_keys") or []),
                    "updated_member_keys": list(overlay.get("updated_member_keys") or []),
                    "removed_member_keys": list(overlay.get("removed_member_keys") or []),
                    "delta_candidate_ids": list(overlay.get("delta_candidate_ids") or []),
                }
            },
            members=patch_members,
            removed_member_keys=list(overlay.get("removed_member_keys") or []),
        )
        if not patch_generation:
            return {}
        patch_summary = {
            "mode": "generation_member_patch",
            "base_snapshot_id": baseline_snapshot_id,
            "delta_snapshot_id": current_snapshot_id,
            "base_generation_key": resolved_base_generation_key,
            "base_generation_sequence": int(resolved_base_generation.get("generation_sequence") or 0),
            "base_generation_watermark": str(resolved_base_generation.get("generation_watermark") or "").strip(),
            "generation_key": str(patch_generation.get("generation_key") or "").strip(),
            "generation_sequence": int(patch_generation.get("generation_sequence") or 0),
            "generation_watermark": str(patch_generation.get("generation_watermark") or "").strip(),
            "candidate_count": len(merged_candidates),
            "delta_candidate_count": len(touched_candidates),
            "added_member_keys": list(overlay.get("added_member_keys") or []),
            "updated_member_keys": list(overlay.get("updated_member_keys") or []),
            "removed_member_keys": list(overlay.get("removed_member_keys") or []),
            "delta_candidate_ids": list(overlay.get("delta_candidate_ids") or []),
        }
        return {
            "source_kind": str(current_candidate_source.get("source_kind") or "company_snapshot").strip()
            or "company_snapshot",
            "target_company": normalized_target_company,
            "snapshot_id": current_snapshot_id,
            "asset_view": asset_view,
            "source_path": str(current_candidate_source.get("source_path") or "").strip(),
            "authoritative_snapshot_id": baseline_snapshot_id,
            "materialization_generation_key": str(patch_generation.get("generation_key") or "").strip(),
            "materialization_generation_sequence": int(patch_generation.get("generation_sequence") or 0),
            "materialization_watermark": str(patch_generation.get("generation_watermark") or "").strip(),
            "candidate_count": len(merged_candidates),
            "unfiltered_candidate_count": len(merged_candidates),
            "candidates": merged_candidates,
            "evidence_lookup": dict(overlay.get("evidence_lookup") or {}),
            "asset_population_patch": patch_summary,
        }

    def _job_asset_population_overlay_path(self, job_id: str) -> Path:
        return self.jobs_dir / f"{str(job_id or '').strip()}.asset_population.json"

    def _candidate_source_asset_population_overlay_path(self, candidate_source: dict[str, Any]) -> Path | None:
        source_payload = dict(candidate_source or {})
        result_view_metadata = dict(source_payload.get("result_view_metadata") or {})
        patch_payload = dict(source_payload.get("asset_population_patch") or {})
        overlay_path = (
            str(source_payload.get("asset_population_overlay_path") or "").strip()
            or str(result_view_metadata.get("asset_population_overlay_path") or "").strip()
            or str(patch_payload.get("overlay_path") or "").strip()
        )
        if not overlay_path:
            return None
        path = Path(overlay_path).expanduser()
        return path if path.exists() else None

    def _candidate_source_prefers_asset_population_overlay(self, candidate_source: dict[str, Any]) -> bool:
        source_payload = dict(candidate_source or {})
        if not source_payload:
            return False
        patch_payload = dict(source_payload.get("asset_population_patch") or {})
        result_view_metadata = dict(source_payload.get("result_view_metadata") or {})
        result_view_patch_payload = dict(result_view_metadata.get("asset_population_patch") or {})
        if patch_payload or result_view_patch_payload:
            return True
        if candidate_source_is_snapshot_authoritative(source_payload):
            return False
        return True

    def _load_candidate_source_asset_population_overlay(self, candidate_source: dict[str, Any]) -> dict[str, Any]:
        if not self._candidate_source_prefers_asset_population_overlay(candidate_source):
            return {}
        overlay_path = self._candidate_source_asset_population_overlay_path(candidate_source)
        if overlay_path is None:
            return {}
        try:
            payload = json.loads(overlay_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return dict(payload) if isinstance(payload, dict) else {}

    def _write_job_asset_population_overlay(
        self,
        *,
        job_id: str,
        request: JobRequest,
        candidate_source: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        candidates = [candidate for candidate in list(candidate_source.get("candidates") or []) if isinstance(candidate, Candidate)]
        if not candidates:
            return {}
        overlay_path = self._job_asset_population_overlay_path(normalized_job_id)
        overlay_payload = {
            "job_id": normalized_job_id,
            "target_company": str(request.target_company or candidate_source.get("target_company") or "").strip(),
            "snapshot_id": str(candidate_source.get("snapshot_id") or "").strip(),
            "authoritative_snapshot_id": str(candidate_source.get("authoritative_snapshot_id") or "").strip(),
            "asset_view": str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged",
            "source_kind": str(candidate_source.get("source_kind") or "").strip(),
            "source_path": str(candidate_source.get("source_path") or "").strip(),
            "materialization_generation_key": str(candidate_source.get("materialization_generation_key") or "").strip(),
            "materialization_generation_sequence": int(candidate_source.get("materialization_generation_sequence") or 0),
            "materialization_watermark": str(candidate_source.get("materialization_watermark") or "").strip(),
            "candidate_count": len(candidates),
            "candidates": [
                {
                    **candidate.to_record(),
                    "role_bucket": derive_candidate_role_bucket(candidate),
                    "functional_facets": derive_candidate_facets(candidate),
                }
                for candidate in candidates
            ],
            "evidence_lookup": {
                str(candidate_id or "").strip(): [
                    dict(item) for item in list(items or []) if isinstance(item, dict)
                ]
                for candidate_id, items in dict(candidate_source.get("evidence_lookup") or {}).items()
                if str(candidate_id or "").strip()
            },
            "asset_population_patch": dict(candidate_source.get("asset_population_patch") or {}),
            "created_at": _utc_now_iso(),
        }
        overlay_path.write_text(
            json.dumps(_storage_json_safe_payload(overlay_payload), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return {
            "path": str(overlay_path),
            "candidate_count": len(candidates),
        }

    def _build_job_result_view_payload(
        self,
        *,
        job_id: str,
        request: JobRequest,
        candidate_source: dict[str, Any],
        organization_execution_profile: dict[str, Any] | None = None,
        summary_payload: dict[str, Any] | None = None,
        effective_execution_semantics: dict[str, Any] | None = None,
        baseline_selection_explanation: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        source_payload = dict(candidate_source or {})
        source_kind = str(source_payload.get("source_kind") or "").strip()
        if not source_kind:
            return {}
        organization_execution_profile = dict(organization_execution_profile or {})
        summary_payload = dict(summary_payload or {})
        effective_execution_semantics = dict(effective_execution_semantics or {})
        baseline_selection_explanation = dict(baseline_selection_explanation or {})
        asset_view = (
            str(source_payload.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        snapshot_id = str(source_payload.get("snapshot_id") or "").strip()
        source_path = str(source_payload.get("source_path") or "").strip()
        if candidate_source_is_snapshot_authoritative(source_payload) and snapshot_id and not source_path:
            snapshot_dir = self._resolve_candidate_source_snapshot_dir(
                request=request,
                candidate_source={
                    **source_payload,
                    "asset_view": asset_view,
                    "snapshot_id": snapshot_id,
                },
            )
            if snapshot_dir is not None:
                materialized_path = self._candidate_source_materialized_path(
                    snapshot_dir=snapshot_dir,
                    asset_view=asset_view,
                )
                if materialized_path is not None:
                    source_path = str(materialized_path)

        authoritative_snapshot_id = str(organization_execution_profile.get("source_snapshot_id") or "").strip()
        if not authoritative_snapshot_id:
            authoritative_snapshot_id = str(
                baseline_selection_explanation.get("matched_registry_snapshot_id") or ""
            ).strip()
        authoritative_snapshot_id = str(
            source_payload.get("authoritative_snapshot_id") or authoritative_snapshot_id or ""
        ).strip()
        materialization_generation_key = str(
            source_payload.get("materialization_generation_key")
            or organization_execution_profile.get("source_generation_key")
            or ""
        ).strip()
        if str(request.target_company or "").strip() and candidate_source_is_snapshot_authoritative(source_payload):
            authoritative_registry = self.store.get_authoritative_organization_asset_registry(
                target_company=request.target_company,
                asset_view=asset_view,
            )
            if not authoritative_snapshot_id:
                authoritative_snapshot_id = str(authoritative_registry.get("snapshot_id") or "").strip()
            if not materialization_generation_key:
                materialization_generation_key = str(
                    authoritative_registry.get("materialization_generation_key") or ""
                ).strip()

        candidate_count = summary_payload.get("candidate_count")
        if candidate_count in (None, "", 0):
            candidate_count = (
                dict(summary_payload.get("candidate_source") or {}).get("candidate_count")
                or source_payload.get("candidate_count")
                or source_payload.get("unfiltered_candidate_count")
                or summary_payload.get("returned_matches")
                or 0
            )
        view_kind = "asset_population" if candidate_source_is_snapshot_authoritative(source_payload) else "ranked_results"
        return {
            "job_id": normalized_job_id,
            "target_company": str(request.target_company or "").strip(),
            "source_kind": source_kind,
            "view_kind": view_kind,
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "source_path": source_path,
            "authoritative_snapshot_id": authoritative_snapshot_id,
            "materialization_generation_key": materialization_generation_key,
            "request_signature_value": request_signature(request.to_record()),
            "summary": {
                "candidate_count": int(candidate_count or 0),
                "returned_matches": int(summary_payload.get("returned_matches") or 0),
                "total_matches": int(summary_payload.get("total_matches") or 0),
                "default_results_mode": str(effective_execution_semantics.get("default_results_mode") or "").strip(),
                "analysis_stage": str(summary_payload.get("analysis_stage") or "").strip(),
                "summary_provider": str(summary_payload.get("summary_provider") or "").strip(),
            },
            "metadata": {
                "target_scope": str(request.target_scope or "").strip(),
                "analysis_stage_mode": str(getattr(request, "analysis_stage_mode", "") or "").strip(),
                "organization_execution_profile": {
                    "source_snapshot_id": str(organization_execution_profile.get("source_snapshot_id") or "").strip(),
                    "source_generation_key": str(
                        organization_execution_profile.get("source_generation_key") or ""
                    ).strip(),
                    "source_generation_sequence": int(
                        organization_execution_profile.get("source_generation_sequence") or 0
                    ),
                },
                "effective_execution_semantics": {
                    "default_results_mode": str(
                        effective_execution_semantics.get("default_results_mode") or ""
                    ).strip(),
                    "asset_population_supported": bool(effective_execution_semantics.get("asset_population_supported")),
                },
                "asset_population_overlay_path": str(source_payload.get("asset_population_overlay_path") or "").strip(),
                "asset_population_patch": dict(source_payload.get("asset_population_patch") or {}),
            },
        }

    def _persist_job_result_view(
        self,
        *,
        job_id: str,
        request: JobRequest,
        candidate_source: dict[str, Any],
        organization_execution_profile: dict[str, Any] | None = None,
        summary_payload: dict[str, Any] | None = None,
        effective_execution_semantics: dict[str, Any] | None = None,
        baseline_selection_explanation: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = self._build_job_result_view_payload(
            job_id=job_id,
            request=request,
            candidate_source=candidate_source,
            organization_execution_profile=organization_execution_profile,
            summary_payload=summary_payload,
            effective_execution_semantics=effective_execution_semantics,
            baseline_selection_explanation=baseline_selection_explanation,
        )
        if not payload:
            return {}
        return self.store.upsert_job_result_view(
            job_id=str(payload.get("job_id") or "").strip(),
            target_company=str(payload.get("target_company") or "").strip(),
            source_kind=str(payload.get("source_kind") or "").strip(),
            view_kind=str(payload.get("view_kind") or "").strip(),
            snapshot_id=str(payload.get("snapshot_id") or "").strip(),
            asset_view=str(payload.get("asset_view") or "canonical_merged").strip() or "canonical_merged",
            source_path=str(payload.get("source_path") or "").strip(),
            authoritative_snapshot_id=str(payload.get("authoritative_snapshot_id") or "").strip(),
            materialization_generation_key=str(payload.get("materialization_generation_key") or "").strip(),
            request_signature_value=str(payload.get("request_signature_value") or "").strip(),
            summary=dict(payload.get("summary") or {}),
            metadata=dict(payload.get("metadata") or {}),
        )

    def _apply_job_result_view_to_candidate_source(
        self,
        candidate_source: dict[str, Any] | None,
        result_view: dict[str, Any] | None,
    ) -> dict[str, Any]:
        resolved = dict(candidate_source or {})
        view_payload = dict(result_view or {})
        if not view_payload:
            return resolved
        for field in ("source_kind", "snapshot_id", "asset_view", "source_path"):
            value = str(view_payload.get(field) or "").strip()
            if value and not str(resolved.get(field) or "").strip():
                resolved[field] = value
        summary_payload = dict(view_payload.get("summary") or {})
        metadata_payload = dict(view_payload.get("metadata") or {})
        if summary_payload and resolved.get("candidate_count") in (None, "", 0):
            resolved["candidate_count"] = int(summary_payload.get("candidate_count") or 0)
        authoritative_snapshot_id = str(view_payload.get("authoritative_snapshot_id") or "").strip()
        if authoritative_snapshot_id and not str(resolved.get("authoritative_snapshot_id") or "").strip():
            resolved["authoritative_snapshot_id"] = authoritative_snapshot_id
        materialization_generation_key = str(view_payload.get("materialization_generation_key") or "").strip()
        if materialization_generation_key and not str(resolved.get("materialization_generation_key") or "").strip():
            resolved["materialization_generation_key"] = materialization_generation_key
        request_sig = str(view_payload.get("request_signature") or "").strip()
        if request_sig and not str(resolved.get("request_signature") or "").strip():
            resolved["request_signature"] = request_sig
        overlay_path = str(metadata_payload.get("asset_population_overlay_path") or "").strip()
        if overlay_path and not str(resolved.get("asset_population_overlay_path") or "").strip():
            resolved["asset_population_overlay_path"] = overlay_path
        patch_payload = dict(metadata_payload.get("asset_population_patch") or {})
        if patch_payload and not dict(resolved.get("asset_population_patch") or {}):
            resolved["asset_population_patch"] = patch_payload
        result_view_ref: dict[str, Any] = {
            "view_id": str(view_payload.get("view_id") or "").strip(),
            "job_id": str(view_payload.get("job_id") or "").strip(),
            "target_company": str(view_payload.get("target_company") or resolved.get("target_company") or "").strip(),
            "source_kind": str(view_payload.get("source_kind") or resolved.get("source_kind") or "").strip(),
            "view_kind": str(view_payload.get("view_kind") or "").strip(),
            "snapshot_id": str(view_payload.get("snapshot_id") or resolved.get("snapshot_id") or "").strip(),
            "asset_view": str(
                view_payload.get("asset_view") or resolved.get("asset_view") or "canonical_merged"
            ).strip()
            or "canonical_merged",
            "source_path": str(view_payload.get("source_path") or resolved.get("source_path") or "").strip(),
            "authoritative_snapshot_id": authoritative_snapshot_id,
            "materialization_generation_key": materialization_generation_key,
            "request_signature": request_sig,
        }
        if summary_payload:
            result_view_ref["summary"] = summary_payload
            resolved["result_view_summary"] = summary_payload
        if metadata_payload:
            result_view_ref["metadata"] = metadata_payload
            resolved["result_view_metadata"] = metadata_payload
        resolved["result_view"] = result_view_ref
        resolved["result_view_id"] = str(result_view_ref.get("view_id") or "").strip()
        resolved["result_view_kind"] = str(result_view_ref.get("view_kind") or "").strip()
        return resolved

    def _resolve_job_candidate_source(
        self,
        *,
        job: dict[str, Any] | None,
        request: JobRequest,
        job_summary: dict[str, Any] | None = None,
        stage1_preview_summary: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        summary_payload = dict(job_summary or {})
        preview_payload = dict(stage1_preview_summary or {})
        candidate_source = dict(
            summary_payload.get("candidate_source") or preview_payload.get("candidate_source") or {}
        )
        if str(request.target_company or "").strip() and not str(candidate_source.get("target_company") or "").strip():
            candidate_source["target_company"] = str(request.target_company or "").strip()
        result_view = self._candidate_source_result_view_stub(candidate_source)
        normalized_job_id = str(dict(job or {}).get("job_id") or "").strip()
        if normalized_job_id:
            stored_view = self.store.get_job_result_view(job_id=normalized_job_id)
            if stored_view:
                result_view = stored_view
        resolved_source = self._apply_job_result_view_to_candidate_source(candidate_source, result_view)
        if candidate_source_is_snapshot_authoritative(resolved_source):
            snapshot_dir = self._resolve_candidate_source_snapshot_dir(
                request=request,
                candidate_source=resolved_source,
            )
            if snapshot_dir is not None:
                materialized_path = self._candidate_source_materialized_path(
                    snapshot_dir=snapshot_dir,
                    asset_view=str(
                        resolved_source.get("asset_view") or request.asset_view or "canonical_merged"
                    ).strip()
                    or "canonical_merged",
                )
                if materialized_path is not None:
                    source_path = Path(str(resolved_source.get("source_path") or "").strip()).expanduser()
                    if not source_path.exists():
                        resolved_source["source_path"] = str(materialized_path)
                        if result_view:
                            result_view = {
                                **dict(result_view),
                                "source_path": str(materialized_path),
                            }
        return resolved_source, dict(result_view or {})

    def _snapshot_publishable_email_lookup_for_request(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
    ) -> dict[str, dict[str, dict[str, Any]]]:
        if not candidate_source_is_snapshot_authoritative(candidate_source):
            return {}
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(dict(candidate_source or {}).get("snapshot_id") or "").strip()
        if not target_company or not snapshot_id:
            return {}
        return self._load_snapshot_publishable_email_lookup(
            target_company=target_company,
            snapshot_id=snapshot_id,
        )

    def _apply_publishable_email_overlay_from_lookup(
        self,
        *,
        source_record: dict[str, Any],
        serialized_record: dict[str, Any],
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        lookup = dict(publishable_email_lookup or {})
        if not lookup or self._record_has_publishable_primary_email(serialized_record):
            return serialized_record
        payload = dict(source_record or {})
        candidate_id = str(payload.get("candidate_id") or serialized_record.get("candidate_id") or "").strip()
        overlay = {}
        if candidate_id:
            overlay = dict(dict(lookup.get("by_candidate_id") or {}).get(candidate_id) or {})
        if not overlay:
            profile_url_key = self._candidate_record_profile_url_key(payload) or self._candidate_record_profile_url_key(
                serialized_record
            )
            if profile_url_key:
                overlay = dict(dict(lookup.get("by_profile_url_key") or {}).get(profile_url_key) or {})
        if not overlay:
            return serialized_record
        updated = dict(serialized_record)
        updated["primary_email"] = str(overlay.get("primary_email") or "").strip()
        if not updated["primary_email"]:
            return serialized_record
        metadata = dict(updated.get("metadata") or {})
        metadata["primary_email"] = updated["primary_email"]
        email_metadata = _normalized_primary_email_metadata(overlay.get("primary_email_metadata"))
        if email_metadata:
            updated["primary_email_metadata"] = email_metadata
            metadata["primary_email_metadata"] = email_metadata
        else:
            updated.pop("primary_email_metadata", None)
            metadata.pop("primary_email_metadata", None)
        updated["metadata"] = metadata
        return updated

    def _publishable_email_lookup_for_job(self, job: dict[str, Any] | None) -> dict[str, dict[str, dict[str, Any]]]:
        if not isinstance(job, dict) or not job:
            return {}
        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        job_summary = dict(job.get("summary") or {})
        stage1_preview_summary = dict(
            dict(workflow_stage_summaries.get("summaries") or {}).get("stage_1_preview") or {}
        )
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        candidate_source, _ = self._resolve_job_candidate_source(
            job=job,
            request=request,
            job_summary=job_summary,
            stage1_preview_summary=stage1_preview_summary,
        )
        return self._snapshot_publishable_email_lookup_for_request(
            request=request,
            candidate_source=candidate_source,
        )

    def _sanitize_persisted_candidate_contact_record(
        self,
        record: dict[str, Any],
        *,
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        updated = self._apply_publishable_email_overlay_from_lookup(
            source_record=record,
            serialized_record=dict(record or {}),
            publishable_email_lookup=publishable_email_lookup,
        )
        metadata = dict(updated.get("metadata") or {})
        if not self._record_has_publishable_primary_email(updated):
            updated.pop("primary_email", None)
            updated.pop("email", None)
            updated.pop("primary_email_metadata", None)
            metadata.pop("primary_email", None)
            metadata.pop("email", None)
            metadata.pop("primary_email_metadata", None)
            updated["metadata"] = metadata
            return updated
        primary_email = str(
            updated.get("primary_email")
            or updated.get("email")
            or metadata.get("primary_email")
            or metadata.get("email")
            or ""
        ).strip()
        if not primary_email:
            return updated
        updated["primary_email"] = primary_email
        metadata["primary_email"] = primary_email
        email_metadata = _normalized_primary_email_metadata(
            updated.get("primary_email_metadata") or metadata.get("primary_email_metadata")
        )
        if email_metadata:
            updated["primary_email_metadata"] = email_metadata
            metadata["primary_email_metadata"] = email_metadata
        else:
            updated.pop("primary_email_metadata", None)
            metadata.pop("primary_email_metadata", None)
        updated["metadata"] = metadata
        return updated

    def _sanitize_candidate_contact_records_for_api(
        self,
        records: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        lookup_cache: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        history_job_cache: dict[str, str] = {}
        sanitized_records: list[dict[str, Any]] = []
        for record in list(records or []):
            serialized = dict(record or {})
            job_id = str(serialized.get("job_id") or "").strip()
            if not job_id:
                history_id = str(serialized.get("history_id") or "").strip()
                if history_id:
                    if history_id not in history_job_cache:
                        resolved = self.store.resolve_frontend_history_link(history_id) or {}
                        history_job_cache[history_id] = str(resolved.get("job_id") or "").strip()
                    job_id = history_job_cache.get(history_id, "")
            publishable_email_lookup = {}
            if job_id:
                if job_id not in lookup_cache:
                    lookup_cache[job_id] = self._publishable_email_lookup_for_job(self.store.get_job(job_id))
                publishable_email_lookup = lookup_cache.get(job_id) or {}
            sanitized_records.append(
                self._sanitize_persisted_candidate_contact_record(
                    serialized,
                    publishable_email_lookup=publishable_email_lookup,
                )
            )
        return sanitized_records

    def _build_job_results_context(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        job = self._maybe_promote_workflow_job_to_completed_from_final_results(job)
        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        plan_payload = dict(job.get("plan") or {})
        job_summary = dict(job.get("summary") or {})
        stage1_preview_summary = dict(
            dict(workflow_stage_summaries.get("summaries") or {}).get("stage_1_preview") or {}
        )
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or {})
        candidate_source, result_view = self._resolve_job_candidate_source(
            job=job,
            request=request,
            job_summary=job_summary,
            stage1_preview_summary=stage1_preview_summary,
        )
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=candidate_source,
        )
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source,
        )
        publishable_email_lookup = self._snapshot_publishable_email_lookup_for_request(
            request=request,
            candidate_source=candidate_source,
        )
        return {
            "job": job,
            "workflow_stage_summaries": workflow_stage_summaries,
            "plan_payload": plan_payload,
            "job_summary": job_summary,
            "stage1_preview_summary": stage1_preview_summary,
            "request": request,
            "candidate_source": candidate_source,
            "result_view": result_view,
            "effective_execution_semantics": effective_execution_semantics,
            "publishable_email_lookup": publishable_email_lookup,
        }

    def _asset_population_cache_key(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        candidate_source: dict[str, Any],
    ) -> tuple[str, str, str, str, str]:
        source_payload = dict(candidate_source or {})
        return (
            normalize_company_key(target_company),
            str(snapshot_id or "").strip(),
            str(asset_view or "canonical_merged").strip() or "canonical_merged",
            str(source_payload.get("source_path") or "").strip(),
            str(
                source_payload.get("materialization_generation_key")
                or source_payload.get("authoritative_snapshot_id")
                or ""
            ).strip(),
        )

    def _invalidate_asset_population_snapshot_cache(self, *, target_company: str, snapshot_id: str) -> None:
        normalized_company_key = normalize_company_key(target_company)
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if not normalized_company_key or not normalized_snapshot_id:
            return
        with self._asset_population_payload_cache_lock:
            stale_payload_keys = [
                key
                for key in self._asset_population_payload_cache
                if key[0] == normalized_company_key and key[1] == normalized_snapshot_id
            ]
            for key in stale_payload_keys:
                self._asset_population_payload_cache.pop(key, None)
        with self._asset_population_summary_cache_lock:
            stale_summary_keys = [
                key
                for key in self._asset_population_summary_cache
                if key[0] == normalized_company_key and key[1] == normalized_snapshot_id
            ]
            for key in stale_summary_keys:
                self._asset_population_summary_cache.pop(key, None)

    def _read_job_asset_population_artifact_summary(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
    ) -> dict[str, Any]:
        target_company = str(request.target_company or "").strip()
        candidate_source = dict(candidate_source or {})
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        if not target_company or not snapshot_id:
            return {}
        cache_key = self._asset_population_cache_key(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            candidate_source=candidate_source,
        )
        with self._asset_population_summary_cache_lock:
            cached_payload = self._asset_population_summary_cache.get(cache_key)
        if cached_payload is not None:
            return dict(cached_payload)

        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            overlay_summary = {
                "candidate_count": int(overlay_payload.get("candidate_count") or 0),
                "materialization_generation_key": str(
                    overlay_payload.get("materialization_generation_key")
                    or candidate_source.get("materialization_generation_key")
                    or ""
                ).strip(),
                "materialization_generation_sequence": int(
                    overlay_payload.get("materialization_generation_sequence")
                    or candidate_source.get("materialization_generation_sequence")
                    or 0
                ),
                "materialization_watermark": str(
                    overlay_payload.get("materialization_watermark")
                    or candidate_source.get("materialization_watermark")
                    or ""
                ).strip(),
                "asset_population_patch": dict(
                    overlay_payload.get("asset_population_patch") or candidate_source.get("asset_population_patch") or {}
                ),
            }
            with self._asset_population_summary_cache_lock:
                self._asset_population_summary_cache[cache_key] = dict(overlay_summary)
            return overlay_summary

        try:
            artifact_store = open_snapshot_artifact_store(
                runtime_dir=self.runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
            )
            artifact_summary_payload = read_snapshot_artifact_summary(artifact_store)
            if artifact_summary_payload:
                with self._asset_population_summary_cache_lock:
                    self._asset_population_summary_cache[cache_key] = dict(artifact_summary_payload)
                return artifact_summary_payload
        except CandidateArtifactError:
            pass

        summary_path: Path | None = None
        snapshot_dir = self._resolve_candidate_source_snapshot_dir(
            request=request,
            candidate_source=candidate_source,
        )
        if snapshot_dir is not None:
            normalized_dir = snapshot_dir / "normalized_artifacts"
            summary_path = normalized_dir / "artifact_summary.json"
            if asset_view != "canonical_merged":
                summary_path = normalized_dir / asset_view / "artifact_summary.json"

        summary_payload: dict[str, Any] = {}
        if summary_path is not None and summary_path.exists():
            try:
                loaded = json.loads(summary_path.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    summary_payload = loaded
            except (OSError, json.JSONDecodeError):
                summary_payload = {}
        with self._asset_population_summary_cache_lock:
            self._asset_population_summary_cache[cache_key] = dict(summary_payload)
        return summary_payload

    def _build_job_asset_population_summary_payload(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        effective_execution_semantics: dict[str, Any],
        fallback_candidate_count: int = 0,
    ) -> dict[str, Any]:
        candidate_source = dict(candidate_source or {})
        asset_population_supported = bool(dict(effective_execution_semantics or {}).get("asset_population_supported"))
        can_fallback_to_snapshot_population = bool(
            str(request.target_company or "").strip() and candidate_source_is_snapshot_authoritative(candidate_source)
        )
        if not asset_population_supported and not can_fallback_to_snapshot_population:
            return {"available": False, "candidate_count": 0, "candidates": []}
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        if not target_company or not snapshot_id:
            return {"available": False, "candidate_count": 0, "candidates": []}
        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            return {
                "available": True,
                "default_selected": str(dict(effective_execution_semantics or {}).get("default_results_mode") or "")
                .strip()
                .lower()
                == "asset_population",
                "source_kind": "job_asset_population_overlay",
                "snapshot_id": str(overlay_payload.get("snapshot_id") or snapshot_id),
                "asset_view": str(overlay_payload.get("asset_view") or asset_view),
                "source_path": str(self._candidate_source_asset_population_overlay_path(candidate_source) or ""),
                "candidate_count": int(
                    overlay_payload.get("candidate_count")
                    or candidate_source.get("candidate_count")
                    or fallback_candidate_count
                    or 0
                ),
                "candidates": [],
                "artifact_summary": {
                    "candidate_count": int(
                        overlay_payload.get("candidate_count")
                        or candidate_source.get("candidate_count")
                        or fallback_candidate_count
                        or 0
                    ),
                    "asset_population_patch": dict(
                        overlay_payload.get("asset_population_patch")
                        or candidate_source.get("asset_population_patch")
                        or {}
                    ),
                },
            }
        artifact_summary = self._read_job_asset_population_artifact_summary(
            request=request,
            candidate_source=candidate_source,
        )
        artifact_source_path = str(candidate_source.get("source_path") or "").strip()
        snapshot_dir = self._resolve_candidate_source_snapshot_dir(
            request=request,
            candidate_source=candidate_source,
        )
        if snapshot_dir is not None:
            artifact_path = self._candidate_source_materialized_path(snapshot_dir=snapshot_dir, asset_view=asset_view)
            if artifact_path is not None:
                artifact_source_path = str(artifact_path)
        resolved_source_kind = str(candidate_source.get("source_kind") or "")
        if artifact_source_path.endswith(("/manifest.json", "\\manifest.json")) or artifact_source_path.endswith(
            ("/artifact_summary.json", "\\artifact_summary.json", "/snapshot_manifest.json", "\\snapshot_manifest.json")
        ):
            resolved_source_kind = "materialized_candidate_documents_manifest"
        candidate_count = int(
            artifact_summary.get("candidate_count")
            or candidate_source.get("candidate_count")
            or fallback_candidate_count
            or 0
        )
        return {
            "available": True,
            "default_selected": str(dict(effective_execution_semantics or {}).get("default_results_mode") or "")
            .strip()
            .lower()
            == "asset_population",
            "source_kind": resolved_source_kind,
            "snapshot_id": snapshot_id,
            "asset_view": asset_view,
            "source_path": artifact_source_path,
            "candidate_count": candidate_count,
            "candidates": [],
            "artifact_summary": artifact_summary,
        }

    def _select_job_results_mode(
        self,
        *,
        request: JobRequest,
        effective_execution_semantics: dict[str, Any],
        ranked_result_count: int,
        asset_population_summary: dict[str, Any],
    ) -> str:
        preferred_results_mode = (
            str(dict(effective_execution_semantics or {}).get("default_results_mode") or "").strip().lower()
        )
        asset_population_count = int(dict(asset_population_summary or {}).get("candidate_count") or 0)
        asset_population_available = bool(dict(asset_population_summary or {}).get("available")) and (
            asset_population_count > 0
            or preferred_results_mode == "asset_population"
            or str(request.target_scope or "").strip().lower() == "full_company_asset"
        )
        if asset_population_available and (
            preferred_results_mode != "ranked_results"
            or bool(dict(asset_population_summary or {}).get("default_selected"))
            or ranked_result_count == 0
            or str(request.target_scope or "").strip().lower() == "full_company_asset"
        ):
            return "asset_population"
        return "ranked_results"

    def _build_job_results_payload(
        self,
        job_id: str,
        *,
        include_runtime_details: bool,
        include_manual_review_items: bool,
        include_asset_population_candidates: bool = True,
        asset_population_offset: int = 0,
        asset_population_limit: int | None = None,
        load_asset_population_profile_timeline: bool = True,
    ) -> dict[str, Any] | None:
        context = self._build_job_results_context(job_id)
        if context is None:
            return None
        job = dict(context.get("job") or {})
        workflow_stage_summaries = dict(context.get("workflow_stage_summaries") or {})
        request = context.get("request") or JobRequest.from_payload({})
        candidate_source = dict(context.get("candidate_source") or {})
        effective_execution_semantics = dict(context.get("effective_execution_semantics") or {})
        publishable_email_lookup = context.get("publishable_email_lookup")
        default_results_mode = (
            str(dict(effective_execution_semantics or {}).get("default_results_mode") or "").strip().lower()
        )
        should_hide_ranked_results = bool(
            default_results_mode == "asset_population" and not str(job.get("artifact_path") or "").strip()
        )
        ranked_result_count = self.store.count_job_results(job_id)
        ranked_results = (
            []
            if should_hide_ranked_results
            else [
                self._serialize_candidate_api_record(
                    record,
                    load_profile_timeline=True,
                    publishable_email_lookup=publishable_email_lookup,
                )
                for record in self.store.get_job_results(job_id)
            ]
        )
        asset_population = self._build_job_asset_population_payload(
            request=request,
            candidate_source=candidate_source,
            effective_execution_semantics=effective_execution_semantics,
            publishable_email_lookup=publishable_email_lookup,
            offset=asset_population_offset,
            limit=asset_population_limit,
            include_candidates=include_asset_population_candidates,
            load_profile_timeline=load_asset_population_profile_timeline,
            fallback_candidate_count=int(
                dict(context.get("job_summary") or {}).get("candidate_count")
                or dict(context.get("stage1_preview_summary") or {}).get("candidate_count")
                or 0
            ),
        )
        payload: dict[str, Any] = {
            "job": job,
            "results": ranked_results,
            "ranked_results": ranked_results,
            "ranked_result_count": ranked_result_count,
            "asset_population": asset_population,
            "request_preview": _load_job_request_preview(job),
            "effective_execution_semantics": effective_execution_semantics,
        }
        if include_manual_review_items:
            payload["manual_review_items"] = self.store.list_manual_review_items(job_id=job_id, status="", limit=100)
        else:
            payload["manual_review_count"] = self.store.count_manual_review_items(job_id=job_id, status="")
        if include_runtime_details:
            payload.update(
                {
                    "events": self.store.list_job_events(job_id),
                    "agent_runtime_session": self.store.get_agent_runtime_session(job_id=job_id) or {},
                    "agent_trace_spans": self.store.list_agent_trace_spans(job_id=job_id),
                    "agent_workers": self.agent_runtime.list_workers(job_id=job_id),
                    "intent_rewrite": _build_intent_rewrite_payload(request_payload=dict(job.get("request") or {})),
                    "workflow_stage_summaries": workflow_stage_summaries,
                }
            )
        return payload

    def _build_job_dashboard_job_payload(self, job: dict[str, Any]) -> dict[str, Any]:
        request_payload = dict(job.get("request") or {})
        summary_payload = dict(job.get("summary") or {})
        return {
            "job_id": str(job.get("job_id") or ""),
            "status": str(job.get("status") or ""),
            "stage": str(job.get("stage") or ""),
            "request": {
                "raw_user_request": str(request_payload.get("raw_user_request") or ""),
                "query": str(request_payload.get("query") or ""),
                "target_company": str(request_payload.get("target_company") or ""),
                "target_scope": str(request_payload.get("target_scope") or ""),
            },
            "summary": summary_payload,
        }

    def get_job_dashboard(self, job_id: str) -> dict[str, Any] | None:
        context = self._build_job_results_context(job_id)
        if context is None:
            return None
        job = dict(context.get("job") or {})
        request = context.get("request") or JobRequest.from_payload({})
        candidate_source = dict(context.get("candidate_source") or {})
        effective_execution_semantics = dict(context.get("effective_execution_semantics") or {})
        publishable_email_lookup = context.get("publishable_email_lookup")
        ranked_result_count = self.store.count_job_results(job_id)
        asset_population = self._build_job_asset_population_payload(
            request=request,
            candidate_source=candidate_source,
            effective_execution_semantics=effective_execution_semantics,
            include_candidates=False,
            fallback_candidate_count=int(
                dict(context.get("job_summary") or {}).get("candidate_count")
                or dict(context.get("stage1_preview_summary") or {}).get("candidate_count")
                or 0
            ),
        )
        result_mode = self._select_job_results_mode(
            request=request,
            effective_execution_semantics=effective_execution_semantics,
            ranked_result_count=ranked_result_count,
            asset_population_summary=asset_population,
        )
        preview_limit = min(max(_env_int("JOB_RESULTS_DASHBOARD_PREVIEW_CANDIDATE_LIMIT", 24), 0), 120)
        ranked_results_preview: list[dict[str, Any]] = []
        if result_mode == "asset_population" and preview_limit > 0 and int(asset_population.get("candidate_count") or 0) > 0:
            asset_population = self._build_job_asset_population_payload(
                request=request,
                candidate_source=candidate_source,
                effective_execution_semantics=effective_execution_semantics,
                publishable_email_lookup=publishable_email_lookup,
                offset=0,
                limit=preview_limit,
                include_candidates=True,
                load_profile_timeline=False,
                fallback_candidate_count=int(
                    dict(context.get("job_summary") or {}).get("candidate_count")
                    or dict(context.get("stage1_preview_summary") or {}).get("candidate_count")
                    or 0
                ),
            )
        elif preview_limit > 0 and ranked_result_count > 0:
            ranked_results_preview = [
                self._serialize_candidate_api_record(
                    record,
                    load_profile_timeline=False,
                    publishable_email_lookup=publishable_email_lookup,
                )
                for record in self.store.get_job_results_page(
                    job_id,
                    offset=0,
                    limit=preview_limit,
                    include_evidence=False,
                )
            ]
        return {
            "job": self._build_job_dashboard_job_payload(job),
            "results": ranked_results_preview,
            "ranked_results": ranked_results_preview,
            "ranked_result_count": ranked_result_count,
            "asset_population": asset_population,
            "manual_review_count": self.store.count_manual_review_items(job_id=job_id, status=""),
            "request_preview": _load_job_request_preview(job),
            "effective_execution_semantics": effective_execution_semantics,
        }

    def get_job_results(self, job_id: str) -> dict[str, Any] | None:
        return self._build_job_results_payload(
            job_id,
            include_runtime_details=True,
            include_manual_review_items=True,
        )

    def get_job_results_api(
        self,
        job_id: str,
        *,
        include_candidates: bool = False,
    ) -> dict[str, Any] | None:
        return self._build_job_results_payload(
            job_id,
            include_runtime_details=True,
            include_manual_review_items=True,
            include_asset_population_candidates=include_candidates,
            load_asset_population_profile_timeline=include_candidates,
        )

    def get_job_candidate_page(
        self,
        job_id: str,
        *,
        offset: int = 0,
        limit: int = 120,
        lightweight: bool = False,
    ) -> dict[str, Any] | None:
        context = self._build_job_results_context(job_id)
        if context is None:
            return None
        normalized_offset = max(int(offset or 0), 0)
        normalized_limit = min(max(int(limit or 0), 1), 250)
        candidate_page_delay_ms = max(0, _env_int("SOURCING_TEST_CANDIDATE_PAGE_DELAY_MS", 0))
        if candidate_page_delay_ms > 0:
            time.sleep(min(candidate_page_delay_ms, 5_000) / 1000.0)
        request = context.get("request") or JobRequest.from_payload({})
        candidate_source = dict(context.get("candidate_source") or {})
        effective_execution_semantics = dict(context.get("effective_execution_semantics") or {})
        publishable_email_lookup = context.get("publishable_email_lookup")
        ranked_result_count = self.store.count_job_results(job_id)
        asset_population_summary = self._build_job_asset_population_payload(
            request=request,
            candidate_source=candidate_source,
            effective_execution_semantics=effective_execution_semantics,
            include_candidates=False,
            fallback_candidate_count=int(
                dict(context.get("job_summary") or {}).get("candidate_count")
                or dict(context.get("stage1_preview_summary") or {}).get("candidate_count")
                or 0
            ),
        )
        result_mode = self._select_job_results_mode(
            request=request,
            effective_execution_semantics=effective_execution_semantics,
            ranked_result_count=ranked_result_count,
            asset_population_summary=asset_population_summary,
        )
        if result_mode == "asset_population":
            asset_population_page = self._build_job_asset_population_payload(
                request=request,
                candidate_source=candidate_source,
                effective_execution_semantics=effective_execution_semantics,
                publishable_email_lookup=publishable_email_lookup,
                offset=normalized_offset,
                limit=normalized_limit,
                include_candidates=True,
                load_profile_timeline=not lightweight,
            )
            total_candidates = int(asset_population_page.get("candidate_count") or 0)
            candidates = list(asset_population_page.get("candidates") or [])
            return {
                "job_id": job_id,
                "result_mode": "asset_population",
                "offset": normalized_offset,
                "limit": normalized_limit,
                "returned_count": len(candidates),
                "total_candidates": total_candidates,
                "has_more": bool(asset_population_page.get("has_more")),
                "next_offset": asset_population_page.get("next_offset"),
                "candidates": candidates,
            }

        ranked_results = [
            self._serialize_candidate_api_record(
                record,
                load_profile_timeline=not lightweight,
                publishable_email_lookup=publishable_email_lookup,
            )
            for record in self.store.get_job_results_page(
                job_id,
                offset=normalized_offset,
                limit=normalized_limit,
                include_evidence=False,
            )
        ]
        next_offset = normalized_offset + len(ranked_results)
        return {
            "job_id": job_id,
            "result_mode": "ranked_results",
            "offset": normalized_offset,
            "limit": normalized_limit,
            "returned_count": len(ranked_results),
            "total_candidates": ranked_result_count,
            "has_more": next_offset < ranked_result_count,
            "next_offset": next_offset if next_offset < ranked_result_count else None,
            "candidates": ranked_results,
        }

    def _frontend_history_phase_from_job(self, job: dict[str, Any] | None) -> str:
        if not isinstance(job, dict) or not job:
            return "plan"
        status = str(job.get("status") or "").strip().lower()
        if status == "completed":
            return "results"
        return "running"

    def _persist_frontend_history_link(
        self,
        *,
        history_id: str,
        query_text: str = "",
        target_company: str = "",
        review_id: int = 0,
        job_id: str = "",
        phase: str = "",
        request_payload: dict[str, Any] | None = None,
        plan_payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        merge_metadata: bool = False,
    ) -> dict[str, Any] | None:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return None
        resolved_metadata = dict(metadata or {})
        if merge_metadata:
            existing = self.store.get_frontend_history_link(normalized_history_id)
            resolved_metadata = _merge_nested_payloads(dict((existing or {}).get("metadata") or {}), resolved_metadata)
        return self.store.upsert_frontend_history_link(
            {
                "history_id": normalized_history_id,
                "query_text": str(query_text or "").strip(),
                "target_company": str(target_company or "").strip(),
                "review_id": int(review_id or 0),
                "job_id": str(job_id or "").strip(),
                "phase": str(phase or "").strip(),
                "request": dict(request_payload or {}),
                "plan": dict(plan_payload or {}),
                "metadata": resolved_metadata,
            }
        )

    def _sync_frontend_history_phase_for_job(
        self,
        *,
        job: dict[str, Any] | None = None,
        phase: str = "",
    ) -> None:
        job_payload = dict(job or {})
        job_id = str(job_payload.get("job_id") or "").strip()
        if not job_id:
            return
        request_payload = dict(job_payload.get("request") or {})
        plan_payload = dict(job_payload.get("plan") or {})
        resolved_phase = str(phase or self._frontend_history_phase_from_job(job_payload)).strip()
        if not resolved_phase:
            return
        query_text = str(request_payload.get("raw_user_request") or request_payload.get("query") or "").strip()
        target_company = str(request_payload.get("target_company") or "").strip()
        for link in self.store.list_frontend_history_links_for_job(job_id, limit=50):
            history_id = str(link.get("history_id") or "").strip()
            if not history_id:
                continue
            self._persist_frontend_history_link(
                history_id=history_id,
                query_text=query_text or str(link.get("query_text") or "").strip(),
                target_company=target_company or str(link.get("target_company") or "").strip(),
                review_id=int(link.get("review_id") or 0),
                job_id=job_id,
                phase=resolved_phase,
                request_payload=request_payload or dict(link.get("request") or {}),
                plan_payload=plan_payload or dict(link.get("plan") or {}),
                metadata=dict(link.get("metadata") or {}),
            )

    def get_frontend_history_recovery(self, history_id: str) -> dict[str, Any]:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return {"status": "invalid", "reason": "history_id is required"}
        resolved = self.store.resolve_frontend_history_link(normalized_history_id)
        if resolved is None:
            return {"status": "not_found", "history_id": normalized_history_id}
        review_id = int(resolved.get("review_id") or 0)
        job_id = str(resolved.get("job_id") or "").strip()
        review_session = self.store.get_plan_review_session(review_id) if review_id > 0 else None
        job = self.store.get_job(job_id) if job_id else None
        request_payload = dict(resolved.get("request") or {})
        plan_payload = dict(resolved.get("plan") or {})
        if not request_payload and review_session is not None:
            request_payload = dict(review_session.get("request") or {})
        if not plan_payload and review_session is not None:
            plan_payload = dict(review_session.get("plan") or {})
        if not request_payload and job is not None:
            request_payload = dict(job.get("request") or {})
        if not plan_payload and job is not None:
            plan_payload = dict(job.get("plan") or {})
        query_text = (
            str(resolved.get("query_text") or "").strip() or str(request_payload.get("raw_user_request") or "").strip()
        )
        target_company = (
            str(resolved.get("target_company") or "").strip()
            or str(request_payload.get("target_company") or "").strip()
            or str((review_session or {}).get("target_company") or "").strip()
        )
        if job is not None:
            phase = self._frontend_history_phase_from_job(job)
        elif review_session is not None or request_payload or plan_payload:
            phase = "plan"
        else:
            phase = str(resolved.get("phase") or "").strip() or "idle"
        metadata = dict(resolved.get("metadata") or {})
        plan_generation = dict(metadata.get("plan_generation") or {})
        error_message = str(plan_generation.get("error_message") or "").strip()
        if job is not None:
            request_preview = _load_job_request_preview(job)
        elif request_payload and plan_payload:
            try:
                request = JobRequest.from_payload(request_payload)
                plan = hydrate_sourcing_plan(plan_payload)
                request_preview = _build_request_preview_payload(
                    request=request,
                    effective_request=_build_effective_retrieval_request(request, plan),
                )
            except Exception:
                request_preview = _build_request_preview_payload(request=request_payload)
        elif request_payload:
            request_preview = _build_request_preview_payload(request=request_payload)
        else:
            request_preview = {}
        return {
            "status": "found",
            "recovery": {
                "history_id": normalized_history_id,
                "source": str(resolved.get("source") or ""),
                "query_text": query_text,
                "target_company": target_company,
                "review_id": review_id,
                "job_id": job_id,
                "phase": phase,
                "request": request_payload,
                "plan": plan_payload,
                "plan_review_gate": dict((review_session or {}).get("gate") or {}),
                "plan_review_session": _plan_review_session_api_summary(review_session),
                "request_preview": request_preview,
                "metadata": metadata,
                "error_message": error_message,
                "job": _job_api_summary(job),
                "created_at": _serialize_api_timestamp(str(resolved.get("created_at") or "")),
                "updated_at": _serialize_api_timestamp(str(resolved.get("updated_at") or "")),
            },
        }

    def list_frontend_history(self, *, limit: int = 24) -> dict[str, Any]:
        try:
            resolved_limit = max(1, min(int(limit or 24), 100))
        except (TypeError, ValueError):
            resolved_limit = 24
        history: list[dict[str, Any]] = []
        for row in self.store.list_frontend_history_links(limit=resolved_limit):
            history_id = str(row.get("history_id") or "").strip()
            if not history_id:
                continue
            recovered = self.get_frontend_history_recovery(history_id)
            if recovered.get("status") != "found":
                continue
            history.append(dict(recovered.get("recovery") or {}))
        return {
            "history": history[:resolved_limit],
            "count": len(history[:resolved_limit]),
        }

    def delete_frontend_history(self, history_id: str) -> dict[str, Any]:
        normalized_history_id = str(history_id or "").strip()
        if not normalized_history_id:
            return {"status": "invalid", "reason": "history_id is required"}
        deleted = self.store.delete_frontend_history_link(normalized_history_id)
        return {
            "status": "deleted" if deleted else "not_found",
            "history_id": normalized_history_id,
        }

    def _maybe_promote_workflow_job_to_completed_from_final_results(
        self,
        job: dict[str, Any],
    ) -> dict[str, Any]:
        if str(job.get("job_type") or "") != "workflow":
            return job
        if str(job.get("status") or "").strip().lower() in {
            "completed",
            "failed",
            "superseded",
            "cancelled",
            "canceled",
        }:
            return job
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return job

        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        stage2_final_summary = dict(dict(workflow_stage_summaries.get("summaries") or {}).get("stage_2_final") or {})
        job_summary = dict(job.get("summary") or {})
        if str(job_summary.get("awaiting_user_action") or "").strip():
            return job
        if str(stage2_final_summary.get("analysis_stage") or "").strip().lower() not in {"", "stage_2_final"}:
            return job
        if str(stage2_final_summary.get("status") or "").strip().lower() != "completed":
            return job
        result_count = self.store.count_job_results(job_id)
        manual_review_count = self.store.count_manual_review_items(job_id=job_id, status="")
        if result_count <= 0 and manual_review_count <= 0:
            return job

        request = JobRequest.from_payload(dict(job.get("request") or {}))
        plan_payload = dict(job.get("plan") or {})
        final_summary = self._persist_completed_workflow_summary(
            job_id=job_id,
            request=request,
            plan=plan_payload,
            artifact={
                "summary": stage2_final_summary,
                "artifact_path": str(job.get("artifact_path") or stage2_final_summary.get("artifact_path") or ""),
            },
            preserved_summary=job_summary,
        )
        self.store.update_agent_runtime_session_status(job_id, "completed")
        self._sync_frontend_history_phase_for_job(
            job={
                **job,
                "status": "completed",
                "stage": "completed",
            },
            phase="results",
        )
        completed_events = self.store.list_job_events(job_id, stage="completed", limit=1, descending=True)
        if not completed_events:
            self.store.append_job_event(
                job_id,
                "completed",
                "completed",
                "Workflow completed after results reconciliation.",
                final_summary,
            )
        return self.store.get_job(job_id) or job

    def _build_effective_execution_semantics(
        self,
        *,
        request: JobRequest,
        organization_execution_profile: dict[str, Any],
        asset_reuse_plan: dict[str, Any],
        candidate_source: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return compile_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source,
        )

    def _build_job_asset_population_payload(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        effective_execution_semantics: dict[str, Any],
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
        offset: int = 0,
        limit: int | None = None,
        include_candidates: bool = True,
        load_profile_timeline: bool = True,
        fallback_candidate_count: int = 0,
    ) -> dict[str, Any]:
        candidate_source = dict(candidate_source or {})
        asset_population_supported = bool(dict(effective_execution_semantics or {}).get("asset_population_supported"))
        can_fallback_to_snapshot_population = bool(
            str(request.target_company or "").strip() and candidate_source_is_snapshot_authoritative(candidate_source)
        )
        if not asset_population_supported and not can_fallback_to_snapshot_population:
            return {"available": False, "candidate_count": 0, "candidates": []}
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        if not target_company or not snapshot_id:
            return {"available": False, "candidate_count": 0, "candidates": []}
        normalized_offset = max(int(offset or 0), 0)
        if not include_candidates:
            return self._build_job_asset_population_summary_payload(
                request=request,
                candidate_source=candidate_source,
                effective_execution_semantics=effective_execution_semantics,
                fallback_candidate_count=fallback_candidate_count,
            )
        normalized_limit = max(int(limit or 0), 0) if limit is not None else 0
        cache_key = self._asset_population_cache_key(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            candidate_source=candidate_source,
        )
        if normalized_offset == 0 and normalized_limit == 0:
            with self._asset_population_payload_cache_lock:
                cached_payload = self._asset_population_payload_cache.get(cache_key)
            if cached_payload is not None:
                return cached_payload
        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            overlay_candidates = [
                dict(item) for item in list(overlay_payload.get("candidates") or []) if isinstance(item, dict)
            ]
            total_candidates = int(overlay_payload.get("candidate_count") or len(overlay_candidates) or 0)
            if normalized_limit > 0:
                page_records = overlay_candidates[normalized_offset : normalized_offset + normalized_limit]
            else:
                page_records = overlay_candidates[normalized_offset:]
            self._attach_snapshot_outreach_layering_metadata_to_records(
                request=request,
                candidate_source=candidate_source,
                records=page_records,
            )
            timeline_preview_limit = (
                max(0, _env_int("JOB_RESULTS_ASSET_TIMELINE_PREVIEW_LIMIT", 96)) if load_profile_timeline else 0
            )
            serialized_candidates = [
                self._serialize_asset_population_candidate_api_record(
                    dict(record),
                    load_profile_timeline=bool(
                        load_profile_timeline and (normalized_offset + index) < timeline_preview_limit
                    ),
                    publishable_email_lookup=publishable_email_lookup,
                )
                for index, record in enumerate(page_records)
            ]
            next_offset = normalized_offset + len(serialized_candidates)
            has_more = next_offset < total_candidates
            payload = {
                "available": True,
                "default_selected": str(dict(effective_execution_semantics or {}).get("default_results_mode") or "")
                .strip()
                .lower()
                == "asset_population",
                "source_kind": "job_asset_population_overlay",
                "snapshot_id": str(overlay_payload.get("snapshot_id") or snapshot_id),
                "asset_view": str(overlay_payload.get("asset_view") or asset_view),
                "source_path": str(self._candidate_source_asset_population_overlay_path(candidate_source) or ""),
                "candidate_count": total_candidates,
                "offset": normalized_offset,
                "limit": len(serialized_candidates),
                "has_more": has_more,
                "next_offset": next_offset if has_more else None,
                "candidates": serialized_candidates,
            }
            if normalized_offset == 0 and normalized_limit == 0:
                with self._asset_population_payload_cache_lock:
                    self._asset_population_payload_cache[cache_key] = payload
            return payload
        try:
            artifact_store = open_snapshot_artifact_store(
                runtime_dir=self.runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
            )
            snapshot_window = read_snapshot_candidate_window(
                artifact_store,
                offset=normalized_offset,
                limit=normalized_limit,
            )
        except CandidateArtifactError:
            snapshot_window = {}
            artifact_store = None
        if snapshot_window:
            page_records = [
                dict(item) for item in list(snapshot_window.get("candidates") or []) if isinstance(item, dict)
            ]
            self._attach_snapshot_outreach_layering_metadata_to_records(
                request=request,
                candidate_source=candidate_source,
                records=page_records,
            )
            total_candidates = int(snapshot_window.get("total_candidate_count") or 0)
            timeline_preview_limit = (
                max(0, _env_int("JOB_RESULTS_ASSET_TIMELINE_PREVIEW_LIMIT", 96)) if load_profile_timeline else 0
            )
            serialized_candidates = [
                self._serialize_asset_population_candidate_api_record(
                    record,
                    load_profile_timeline=bool(
                        load_profile_timeline and (normalized_offset + index) < timeline_preview_limit
                    ),
                    publishable_email_lookup=publishable_email_lookup,
                )
                for index, record in enumerate(page_records)
            ]
            next_offset = normalized_offset + len(serialized_candidates)
            has_more = next_offset < total_candidates
            artifact_source_path = (
                str(artifact_store.artifact_dir / "manifest.json")
                if artifact_store is not None
                else str(candidate_source.get("source_path") or "").strip()
            )
            payload = {
                "available": True,
                "default_selected": str(dict(effective_execution_semantics or {}).get("default_results_mode") or "")
                .strip()
                .lower()
                == "asset_population",
                "source_kind": "materialized_candidate_documents_manifest",
                "snapshot_id": snapshot_id,
                "asset_view": asset_view,
                "source_path": artifact_source_path,
                "candidate_count": total_candidates,
                "offset": normalized_offset,
                "limit": len(serialized_candidates),
                "has_more": has_more,
                "next_offset": next_offset if has_more else None,
                "candidates": serialized_candidates,
            }
            if normalized_offset == 0 and normalized_limit == 0:
                with self._asset_population_payload_cache_lock:
                    self._asset_population_payload_cache[cache_key] = payload
            return payload
        try:
            snapshot_payload = self._load_company_snapshot_candidate_documents_with_materialization_fallback(
                target_company=target_company,
                snapshot_id=snapshot_id,
                view=asset_view,
                allow_candidate_documents_fallback=self._candidate_source_allows_candidate_documents_fallback(
                    candidate_source
                ),
            )
        except CandidateArtifactError:
            return {"available": False, "candidate_count": 0, "candidates": []}
        snapshot_candidates = list(snapshot_payload.get("candidates") or [])
        self._attach_snapshot_outreach_layering_metadata(
            request=request,
            candidate_source=candidate_source,
            candidates=snapshot_candidates,
        )
        raw_candidate_lookup = self._load_asset_population_raw_candidate_lookup(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            candidate_source=candidate_source,
        )
        total_candidates = len(snapshot_candidates)
        if normalized_limit > 0:
            page_candidates = snapshot_candidates[normalized_offset : normalized_offset + normalized_limit]
        else:
            page_candidates = snapshot_candidates[normalized_offset:]
        timeline_preview_limit = (
            max(0, _env_int("JOB_RESULTS_ASSET_TIMELINE_PREVIEW_LIMIT", 96)) if load_profile_timeline else 0
        )
        serialized_candidates = [
            self._serialize_asset_population_candidate_api_record(
                {
                    **self._merge_asset_population_candidate_record(
                        candidate=candidate,
                        raw_candidate_lookup=raw_candidate_lookup,
                    ),
                    "role_bucket": derive_candidate_role_bucket(candidate),
                    "functional_facets": derive_candidate_facets(candidate),
                },
                load_profile_timeline=bool(load_profile_timeline and (normalized_offset + index) < timeline_preview_limit),
                publishable_email_lookup=publishable_email_lookup,
            )
            for index, candidate in enumerate(page_candidates)
        ]
        next_offset = normalized_offset + len(serialized_candidates)
        has_more = next_offset < total_candidates
        payload = {
            "available": True,
            "default_selected": str(dict(effective_execution_semantics or {}).get("default_results_mode") or "")
            .strip()
            .lower()
            == "asset_population",
            "source_kind": str(snapshot_payload.get("source_kind") or candidate_source.get("source_kind") or ""),
            "snapshot_id": str(snapshot_payload.get("snapshot_id") or snapshot_id),
            "asset_view": str(snapshot_payload.get("asset_view") or asset_view),
            "source_path": str(snapshot_payload.get("source_path") or candidate_source.get("source_path") or ""),
            "candidate_count": total_candidates,
            "offset": normalized_offset,
            "limit": len(serialized_candidates),
            "has_more": has_more,
            "next_offset": next_offset if has_more else None,
            "candidates": serialized_candidates,
        }
        if normalized_offset == 0 and normalized_limit == 0:
            with self._asset_population_payload_cache_lock:
                self._asset_population_payload_cache[cache_key] = payload
        return payload

    def _load_asset_population_candidate_shard_payload(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        effective_execution_semantics: dict[str, Any],
        candidate_id: str,
    ) -> dict[str, Any] | None:
        asset_population_supported = bool(dict(effective_execution_semantics or {}).get("asset_population_supported"))
        can_fallback_to_snapshot_population = bool(
            str(request.target_company or "").strip() and candidate_source_is_snapshot_authoritative(candidate_source)
        )
        if not asset_population_supported and not can_fallback_to_snapshot_population:
            return None
        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            overlay_candidates = {
                str(dict(item).get("candidate_id") or "").strip(): dict(item)
                for item in list(overlay_payload.get("candidates") or [])
                if isinstance(item, dict) and str(dict(item).get("candidate_id") or "").strip()
            }
            candidate_record = dict(overlay_candidates.get(str(candidate_id or "").strip()) or {})
            if candidate_record:
                evidence_lookup = dict(overlay_payload.get("evidence_lookup") or {})
                return {
                    "candidate_id": str(candidate_record.get("candidate_id") or "").strip(),
                    "snapshot_id": str(overlay_payload.get("snapshot_id") or candidate_source.get("snapshot_id") or ""),
                    "asset_view": str(overlay_payload.get("asset_view") or candidate_source.get("asset_view") or ""),
                    "normalized_candidate": dict(candidate_record),
                    "materialized_candidate": dict(candidate_record),
                    "evidence": [
                        dict(item)
                        for item in list(evidence_lookup.get(str(candidate_record.get("candidate_id") or "").strip()) or [])
                        if isinstance(item, dict)
                    ],
                }
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        if not target_company or not snapshot_id:
            return None
        try:
            artifact_store = open_snapshot_artifact_store(
                runtime_dir=self.runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
            )
            return read_snapshot_candidate_shard(artifact_store, candidate_id=candidate_id)
        except CandidateArtifactError:
            return None

    def _merge_asset_population_candidate_shard_record(self, shard_payload: dict[str, Any]) -> dict[str, Any]:
        normalized_record = dict(dict(shard_payload or {}).get("normalized_candidate") or {})
        materialized_record = dict(dict(shard_payload or {}).get("materialized_candidate") or {})
        if not normalized_record and not materialized_record:
            return {}
        merged = dict(normalized_record)
        for key, value in materialized_record.items():
            if key == "metadata":
                continue
            if value not in (None, "", [], {}):
                merged[key] = value
        merged_metadata = dict(normalized_record.get("metadata") or {})
        merged_metadata.update(dict(materialized_record.get("metadata") or {}))
        if merged_metadata:
            merged["metadata"] = merged_metadata
        return merged

    def _load_job_asset_population_candidate_record(
        self,
        *,
        job: dict[str, Any],
        candidate_id: str,
        load_profile_timeline: bool = True,
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> dict[str, Any] | None:
        normalized_candidate_id = str(candidate_id or "").strip()
        if not normalized_candidate_id:
            return None
        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        plan_payload = dict(job.get("plan") or {})
        job_summary = dict(job.get("summary") or {})
        stage1_preview_summary = dict(
            dict(workflow_stage_summaries.get("summaries") or {}).get("stage_1_preview") or {}
        )
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or {})
        candidate_source, _ = self._resolve_job_candidate_source(
            job=job,
            request=request,
            job_summary=job_summary,
            stage1_preview_summary=stage1_preview_summary,
        )
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=candidate_source,
        )
        if publishable_email_lookup is None:
            publishable_email_lookup = self._snapshot_publishable_email_lookup_for_request(
                request=request,
                candidate_source=candidate_source,
            )
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source,
        )
        asset_population_supported = bool(dict(effective_execution_semantics or {}).get("asset_population_supported"))
        can_fallback_to_snapshot_population = bool(
            str(request.target_company or "").strip() and candidate_source_is_snapshot_authoritative(candidate_source)
        )
        if not asset_population_supported and not can_fallback_to_snapshot_population:
            return None
        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            overlay_record = next(
                (
                    dict(item)
                    for item in list(overlay_payload.get("candidates") or [])
                    if isinstance(item, dict)
                    and str(dict(item).get("candidate_id") or "").strip() == normalized_candidate_id
                ),
                {},
            )
            if overlay_record:
                self._attach_snapshot_outreach_layering_metadata_to_records(
                    request=request,
                    candidate_source=candidate_source,
                    records=[overlay_record],
                )
                return self._serialize_candidate_api_record(
                    overlay_record,
                    load_profile_timeline=load_profile_timeline,
                    publishable_email_lookup=publishable_email_lookup,
                )
        shard_payload = self._load_asset_population_candidate_shard_payload(
            request=request,
            candidate_source=candidate_source,
            effective_execution_semantics=effective_execution_semantics,
            candidate_id=normalized_candidate_id,
        )
        if shard_payload:
            merged_record = self._merge_asset_population_candidate_shard_record(shard_payload)
            self._attach_snapshot_outreach_layering_metadata_to_records(
                request=request,
                candidate_source=candidate_source,
                records=[merged_record],
            )
            return self._serialize_candidate_api_record(
                merged_record,
                load_profile_timeline=load_profile_timeline,
                publishable_email_lookup=publishable_email_lookup,
            )

        target_company = str(request.target_company or "").strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        if not target_company or not snapshot_id:
            return None
        try:
            snapshot_payload = self._load_company_snapshot_candidate_documents_with_materialization_fallback(
                target_company=target_company,
                snapshot_id=snapshot_id,
                view=asset_view,
                allow_candidate_documents_fallback=self._candidate_source_allows_candidate_documents_fallback(
                    candidate_source
                ),
            )
        except CandidateArtifactError:
            return None
        snapshot_candidates = list(snapshot_payload.get("candidates") or [])
        self._attach_snapshot_outreach_layering_metadata(
            request=request,
            candidate_source=candidate_source,
            candidates=snapshot_candidates,
        )
        raw_candidate_lookup = self._load_asset_population_raw_candidate_lookup(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view=asset_view,
            candidate_source=candidate_source,
        )
        matched_candidate = next(
            (
                candidate
                for candidate in snapshot_candidates
                if str(getattr(candidate, "candidate_id", "") or "").strip() == normalized_candidate_id
            ),
            None,
        )
        if matched_candidate is None:
            return None
        return self._serialize_candidate_api_record(
            {
                **self._merge_asset_population_candidate_record(
                    candidate=matched_candidate,
                    raw_candidate_lookup=raw_candidate_lookup,
                ),
                "role_bucket": derive_candidate_role_bucket(matched_candidate),
                "functional_facets": derive_candidate_facets(matched_candidate),
            },
            load_profile_timeline=load_profile_timeline,
            publishable_email_lookup=publishable_email_lookup,
        )

    def _load_asset_population_raw_candidate_lookup(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        asset_view: str,
        candidate_source: dict[str, Any],
    ) -> dict[str, dict[str, Any]]:
        try:
            loaded = load_company_snapshot_candidate_documents_with_materialization_fallback(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=target_company,
                snapshot_id=snapshot_id,
                view=asset_view,
                allow_materialization_fallback=True,
                allow_candidate_documents_fallback=self._candidate_source_allows_candidate_documents_fallback(
                    candidate_source
                ),
            )
        except CandidateArtifactError:
            return {}
        return {
            str(candidate.candidate_id or "").strip(): candidate.to_record()
            for candidate in list(loaded.get("candidates") or [])
            if isinstance(candidate, Candidate) and str(candidate.candidate_id or "").strip()
        }

    def _merge_asset_population_candidate_record(
        self,
        *,
        candidate: Candidate,
        raw_candidate_lookup: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        base_record = candidate.to_record()
        raw_record = dict(raw_candidate_lookup.get(str(candidate.candidate_id or "").strip()) or {})
        if not raw_record:
            return base_record
        merged = dict(base_record)
        for key, value in raw_record.items():
            if key == "metadata":
                continue
            if value not in (None, "", [], {}):
                merged[key] = value
        merged_metadata = dict(base_record.get("metadata") or {})
        merged_metadata.update(dict(raw_record.get("metadata") or {}))
        if merged_metadata:
            merged["metadata"] = merged_metadata
        return merged

    def _serialize_candidate_api_record(
        self,
        record: dict[str, Any],
        *,
        load_profile_timeline: bool,
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        payload = dict(record or {})
        metadata = dict(payload.get("metadata") or {})
        source_path = str(payload.get("source_path") or metadata.get("source_path") or "").strip()
        timeline = self._resolve_candidate_profile_timeline(
            payload=payload,
            metadata=metadata,
            source_path=source_path,
            load_profile_timeline=load_profile_timeline,
        )
        education_lines = list(timeline.get("education_lines") or [])
        experience_lines = list(timeline.get("experience_lines") or [])
        suppress_embedded_primary_email = _timeline_requires_primary_email_scrub(
            timeline,
            context={
                **payload,
                "metadata": metadata,
            },
        )
        serialized = dict(payload)
        serialized["source_path"] = source_path
        serialized["metadata"] = metadata
        function_ids = _normalize_candidate_level_function_ids(
            payload.get("function_ids"),
            metadata.get("function_ids"),
            source_shard_filters=metadata.get("source_shard_filters"),
        )
        if function_ids:
            serialized["function_ids"] = function_ids
        for field_name in (
            "headline",
            "summary",
            "about",
            "profile_location",
            "public_identifier",
            "avatar_url",
            "photo_url",
            "primary_email",
        ):
            if field_name == "primary_email" and suppress_embedded_primary_email:
                continue
            metadata_value = str(metadata.get(field_name) or "").strip()
            if metadata_value and not serialized.get(field_name):
                serialized[field_name] = metadata_value
        for field_name in (
            "headline",
            "summary",
            "about",
            "profile_location",
            "public_identifier",
            "avatar_url",
            "photo_url",
            "primary_email",
        ):
            field_value = timeline.get(field_name)
            if field_name == "primary_email":
                if suppress_embedded_primary_email:
                    serialized.pop("primary_email", None)
                    serialized.pop("email", None)
                    continue
                if field_value:
                    serialized[field_name] = field_value
                else:
                    serialized.pop("primary_email", None)
                    serialized.pop("email", None)
                continue
            if field_value and not serialized.get(field_name):
                serialized[field_name] = field_value
        if not str(serialized.get("linkedin_url") or "").strip():
            linkedin_lookup_url = _candidate_profile_lookup_url(payload, metadata)
            if linkedin_lookup_url:
                serialized["linkedin_url"] = linkedin_lookup_url
        email_metadata = _normalized_primary_email_metadata(timeline.get("primary_email_metadata"))
        if email_metadata and str(serialized.get("primary_email") or "").strip():
            serialized["primary_email_metadata"] = email_metadata
        else:
            serialized.pop("primary_email_metadata", None)
        if not str(serialized.get("primary_email") or "").strip():
            serialized.pop("primary_email", None)
            serialized.pop("email", None)
        if not serialized.get("media_url"):
            media_url = str(
                serialized.get("avatar_url")
                or serialized.get("photo_url")
                or metadata.get("avatar_url")
                or metadata.get("photo_url")
                or payload.get("media_url")
                or ""
            ).strip()
            if media_url:
                serialized["media_url"] = media_url
        for field_name in ("outreach_layer", "outreach_layer_key", "outreach_layer_source"):
            if serialized.get(field_name) not in (None, ""):
                continue
            metadata_lookup_value = str(metadata.get(field_name) or "").strip()
            if metadata_lookup_value:
                serialized[field_name] = metadata_lookup_value
        if education_lines:
            serialized["education_lines"] = education_lines
        if experience_lines:
            serialized["experience_lines"] = experience_lines
        return self._apply_publishable_email_overlay_from_lookup(
            source_record=payload,
            serialized_record=serialized,
            publishable_email_lookup=publishable_email_lookup,
        )

    def _serialize_asset_population_candidate_api_record(
        self,
        record: dict[str, Any],
        *,
        load_profile_timeline: bool,
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        serialized = self._serialize_candidate_api_record(
            record,
            load_profile_timeline=load_profile_timeline,
            publishable_email_lookup=publishable_email_lookup,
        )
        compact: dict[str, Any] = {}
        for field_name in (
            "candidate_id",
            "display_name",
            "name_en",
            "name_zh",
            "category",
            "employment_status",
            "organization",
            "role",
            "headline",
            "summary",
            "linkedin_url",
            "source_dataset",
            "team",
            "role_bucket",
            "function_ids",
            "focus_areas",
            "outreach_layer",
            "outreach_layer_key",
            "outreach_layer_source",
            "profile_location",
            "public_identifier",
            "media_url",
            "avatar_url",
            "photo_url",
            "primary_email",
            "primary_email_metadata",
            "profile_capture_kind",
            "current_destination",
            "joined_at",
            "left_at",
        ):
            value = serialized.get(field_name)
            if value in (None, "", [], {}):
                continue
            compact[field_name] = value

        education_lines = _normalized_text_lines(serialized.get("education_lines"))
        experience_lines = _normalized_text_lines(serialized.get("experience_lines"))
        if education_lines:
            compact["education_lines"] = education_lines
        if experience_lines:
            compact["experience_lines"] = experience_lines

        if serialized.get("matched_fields"):
            compact["matched_fields"] = list(serialized.get("matched_fields") or [])

        has_profile_detail = _timeline_has_complete_profile_detail(
            {
                "experience_lines": experience_lines,
                "education_lines": education_lines,
                "headline": serialized.get("headline"),
                "summary": serialized.get("summary"),
                "about": serialized.get("about"),
                "primary_email": serialized.get("primary_email"),
                "languages": serialized.get("languages"),
                "skills": serialized.get("skills"),
                "profile_capture_kind": serialized.get("profile_capture_kind"),
                "profile_capture_source_path": serialized.get("profile_capture_source_path"),
                "source_path": serialized.get("source_path"),
            }
        )
        compact["has_profile_detail"] = has_profile_detail
        linkedin_url = str(serialized.get("linkedin_url") or "").strip()
        needs_profile_completion = bool(linkedin_url and not has_profile_detail)
        compact["needs_profile_completion"] = needs_profile_completion
        low_profile_richness = bool(
            linkedin_url
            and has_profile_detail
            and not needs_profile_completion
            and (not experience_lines or not education_lines)
        )
        compact["low_profile_richness"] = low_profile_richness
        return compact

    def _resolve_candidate_profile_timeline(
        self,
        *,
        payload: dict[str, Any],
        metadata: dict[str, Any],
        source_path: str,
        load_profile_timeline: bool,
    ) -> dict[str, Any]:
        experience_lines = _normalized_text_lines(payload.get("experience_lines") or metadata.get("experience_lines"))
        education_lines = _normalized_text_lines(payload.get("education_lines") or metadata.get("education_lines"))
        if not load_profile_timeline:
            capture_kind = str(
                payload.get("profile_capture_kind") or metadata.get("profile_capture_kind") or ""
            ).strip()
            profile_capture_source_path = str(
                payload.get("profile_capture_source_path")
                or metadata.get("profile_capture_source_path")
                or source_path
                or ""
            ).strip()
            return {
                "experience_lines": experience_lines,
                "education_lines": education_lines,
                "headline": str(payload.get("headline") or metadata.get("headline") or "").strip(),
                "summary": str(payload.get("summary") or metadata.get("summary") or "").strip(),
                "about": str(payload.get("about") or metadata.get("about") or "").strip(),
                "profile_location": str(
                    payload.get("profile_location")
                    or metadata.get("profile_location")
                    or payload.get("location")
                    or metadata.get("location")
                    or ""
                ).strip(),
                "public_identifier": str(
                    payload.get("public_identifier")
                    or metadata.get("public_identifier")
                    or payload.get("username")
                    or metadata.get("username")
                    or ""
                ).strip(),
                "avatar_url": str(
                    payload.get("avatar_url")
                    or payload.get("photo_url")
                    or payload.get("media_url")
                    or metadata.get("avatar_url")
                    or metadata.get("photo_url")
                    or metadata.get("media_url")
                    or ""
                ).strip(),
                "photo_url": str(
                    payload.get("photo_url")
                    or payload.get("avatar_url")
                    or metadata.get("photo_url")
                    or metadata.get("avatar_url")
                    or ""
                ).strip(),
                "primary_email": str(
                    payload.get("primary_email")
                    or payload.get("email")
                    or metadata.get("primary_email")
                    or metadata.get("email")
                    or ""
                ).strip(),
                "primary_email_metadata": _normalized_primary_email_metadata(
                    payload.get("primary_email_metadata") or metadata.get("primary_email_metadata")
                ),
                "profile_capture_kind": capture_kind,
                "profile_capture_source_path": profile_capture_source_path,
                "source_kind": "",
                "source_path": source_path,
            }
        profile_url = _candidate_profile_lookup_url(payload, metadata)
        if not profile_url:
            resolved = _resolve_candidate_profile_timeline(
                payload=payload,
                metadata=metadata,
                source_path=source_path,
            )
        else:
            resolved = _resolve_candidate_profile_timeline(
                payload=payload,
                metadata=metadata,
                source_path=source_path,
                registry_row=self.store.get_linkedin_profile_registry(profile_url),
            )
        if experience_lines and not resolved.get("experience_lines"):
            resolved["experience_lines"] = experience_lines
        if education_lines and not resolved.get("education_lines"):
            resolved["education_lines"] = education_lines
        return {
            "experience_lines": _normalized_text_lines(resolved.get("experience_lines")),
            "education_lines": _normalized_text_lines(resolved.get("education_lines")),
            "headline": str(resolved.get("headline") or "").strip(),
            "summary": str(resolved.get("summary") or "").strip(),
            "about": str(resolved.get("about") or "").strip(),
            "profile_location": str(resolved.get("profile_location") or "").strip(),
            "public_identifier": str(resolved.get("public_identifier") or "").strip(),
            "avatar_url": str(resolved.get("avatar_url") or "").strip(),
            "photo_url": str(resolved.get("photo_url") or "").strip(),
            "primary_email": str(resolved.get("primary_email") or "").strip(),
            "primary_email_metadata": _normalized_primary_email_metadata(resolved.get("primary_email_metadata")),
            "profile_capture_kind": str(resolved.get("profile_capture_kind") or "").strip(),
            "profile_capture_source_path": str(resolved.get("profile_capture_source_path") or "").strip(),
            "source_kind": str(resolved.get("source_kind") or "").strip(),
            "source_path": str(resolved.get("source_path") or source_path or "").strip(),
        }

    def _build_job_candidate_api_context(self, job: dict[str, Any]) -> dict[str, Any]:
        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        plan_payload = dict(job.get("plan") or {})
        job_summary = dict(job.get("summary") or {})
        stage1_preview_summary = dict(
            dict(workflow_stage_summaries.get("summaries") or {}).get("stage_1_preview") or {}
        )
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or {})
        candidate_source, result_view = self._resolve_job_candidate_source(
            job=job,
            request=request,
            job_summary=job_summary,
            stage1_preview_summary=stage1_preview_summary,
        )
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=candidate_source,
        )
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source,
        )
        publishable_email_lookup = self._snapshot_publishable_email_lookup_for_request(
            request=request,
            candidate_source=candidate_source,
        )
        return {
            "workflow_stage_summaries": workflow_stage_summaries,
            "plan_payload": plan_payload,
            "job_summary": job_summary,
            "stage1_preview_summary": stage1_preview_summary,
            "request": request,
            "candidate_source": candidate_source,
            "result_view": result_view,
            "effective_execution_semantics": effective_execution_semantics,
            "publishable_email_lookup": publishable_email_lookup,
        }

    def _build_job_candidate_api_payload(
        self,
        *,
        normalized_candidate_id: str,
        result_record: dict[str, Any] | None,
        candidate: Candidate | None,
        load_profile_timeline: bool = True,
        publishable_email_lookup: dict[str, dict[str, dict[str, Any]]] | None = None,
    ) -> dict[str, Any]:
        candidate_payload = candidate.to_record() if candidate is not None else {}
        if result_record is not None:
            candidate_payload.update(
                {
                    "candidate_id": normalized_candidate_id,
                    "display_name": str(
                        result_record.get("display_name") or candidate_payload.get("display_name") or ""
                    ),
                    "name_en": str(result_record.get("name_en") or candidate_payload.get("name_en") or ""),
                    "name_zh": str(result_record.get("name_zh") or candidate_payload.get("name_zh") or ""),
                    "category": str(result_record.get("category") or candidate_payload.get("category") or ""),
                    "organization": str(
                        result_record.get("organization") or candidate_payload.get("organization") or ""
                    ),
                    "role": str(result_record.get("role") or candidate_payload.get("role") or ""),
                    "team": str(result_record.get("team") or candidate_payload.get("team") or ""),
                    "employment_status": str(
                        result_record.get("employment_status") or candidate_payload.get("employment_status") or ""
                    ),
                    "focus_areas": str(result_record.get("focus_areas") or candidate_payload.get("focus_areas") or ""),
                    "linkedin_url": str(
                        result_record.get("linkedin_url") or candidate_payload.get("linkedin_url") or ""
                    ),
                    "media_url": str(result_record.get("media_url") or candidate_payload.get("media_url") or ""),
                    "avatar_url": str(result_record.get("avatar_url") or candidate_payload.get("avatar_url") or ""),
                    "photo_url": str(result_record.get("photo_url") or candidate_payload.get("photo_url") or ""),
                    "primary_email": str(
                        result_record.get("primary_email") or candidate_payload.get("primary_email") or ""
                    ),
                    "primary_email_metadata": _normalized_primary_email_metadata(
                        result_record.get("primary_email_metadata") or candidate_payload.get("primary_email_metadata")
                    ),
                    "function_ids": _normalize_candidate_level_function_ids(
                        result_record.get("function_ids"),
                        candidate_payload.get("function_ids"),
                        source_shard_filters=dict(candidate_payload.get("metadata") or {}).get("source_shard_filters"),
                    ),
                    "confidence_label": str(result_record.get("confidence_label") or ""),
                    "confidence_score": result_record.get("confidence_score"),
                    "confidence_reason": str(result_record.get("confidence_reason") or ""),
                    "explanation": str(result_record.get("explanation") or ""),
                    "matched_fields": list(result_record.get("matched_fields") or []),
                }
            )
            experience_lines = _normalized_text_lines(result_record.get("experience_lines"))
            education_lines = _normalized_text_lines(result_record.get("education_lines"))
            if experience_lines:
                candidate_payload["experience_lines"] = experience_lines
            if education_lines:
                candidate_payload["education_lines"] = education_lines
        candidate_metadata = dict(candidate_payload.get("metadata") or {})
        if result_record is not None:
            candidate_metadata.update(dict(result_record.get("metadata") or {}))
        candidate_source_path = str(
            candidate_payload.get("source_path")
            or (result_record or {}).get("source_path")
            or candidate_metadata.get("source_path")
            or ""
        ).strip()
        candidate_payload["metadata"] = candidate_metadata
        candidate_payload["source_path"] = candidate_source_path
        candidate_timeline = self._resolve_candidate_profile_timeline(
            payload=candidate_payload,
            metadata=candidate_metadata,
            source_path=candidate_source_path,
            load_profile_timeline=load_profile_timeline,
        )
        if candidate_timeline.get("experience_lines"):
            candidate_payload["experience_lines"] = list(candidate_timeline.get("experience_lines") or [])
        if candidate_timeline.get("education_lines"):
            candidate_payload["education_lines"] = list(candidate_timeline.get("education_lines") or [])
        for field_name in (
            "avatar_url",
            "photo_url",
            "primary_email",
            "headline",
            "summary",
            "about",
            "profile_location",
        ):
            field_value = candidate_timeline.get(field_name)
            if field_name == "primary_email":
                if _timeline_requires_primary_email_scrub(
                    candidate_timeline,
                    context={
                        **candidate_payload,
                        "metadata": candidate_metadata,
                    },
                ):
                    candidate_payload.pop("primary_email", None)
                    candidate_payload.pop("email", None)
                    continue
                if field_value:
                    candidate_payload[field_name] = field_value
                continue
            if field_value and not candidate_payload.get(field_name):
                candidate_payload[field_name] = field_value
        candidate_email_metadata = _normalized_primary_email_metadata(candidate_timeline.get("primary_email_metadata"))
        if candidate_email_metadata and str(candidate_payload.get("primary_email") or "").strip():
            candidate_payload["primary_email_metadata"] = candidate_email_metadata
        else:
            candidate_payload.pop("primary_email_metadata", None)
        candidate_payload = self._apply_publishable_email_overlay_from_lookup(
            source_record=result_record or candidate_payload,
            serialized_record=candidate_payload,
            publishable_email_lookup=publishable_email_lookup,
        )
        if not str(candidate_payload.get("primary_email") or "").strip():
            candidate_payload.pop("primary_email", None)
            candidate_payload.pop("email", None)
        if not candidate_payload.get("media_url"):
            media_url = str(candidate_payload.get("avatar_url") or candidate_payload.get("photo_url") or "").strip()
            if media_url:
                candidate_payload["media_url"] = media_url
        return candidate_payload

    def _load_authoritative_candidate_detail_for_source(
        self,
        *,
        request: JobRequest | None,
        candidate_source: dict[str, Any],
        candidate_id: str,
    ) -> dict[str, Any]:
        if not candidate_source_is_snapshot_authoritative(candidate_source):
            return {}
        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            overlay_record = next(
                (
                    dict(item)
                    for item in list(overlay_payload.get("candidates") or [])
                    if isinstance(item, dict) and str(dict(item).get("candidate_id") or "").strip() == str(candidate_id or "").strip()
                ),
                {},
            )
            if overlay_record:
                overlay_candidate_payload = {
                    field_name: overlay_record.get(field_name)
                    for field_name in Candidate.__dataclass_fields__
                    if field_name in overlay_record
                }
                return {
                    "status": "loaded",
                    "target_company": str(
                        overlay_payload.get("target_company")
                        or candidate_source.get("target_company")
                        or (request.target_company if isinstance(request, JobRequest) else "")
                        or ""
                    ).strip(),
                    "company_key": normalize_company_key(
                        str(
                            overlay_payload.get("target_company")
                            or candidate_source.get("target_company")
                            or (request.target_company if isinstance(request, JobRequest) else "")
                            or ""
                        ).strip()
                    ),
                    "snapshot_id": str(overlay_payload.get("snapshot_id") or candidate_source.get("snapshot_id") or ""),
                    "asset_view": str(overlay_payload.get("asset_view") or candidate_source.get("asset_view") or "canonical_merged"),
                    "source_kind": "job_asset_population_overlay",
                    "candidate": normalize_candidate(Candidate(**overlay_candidate_payload)),
                    "evidence_records": build_evidence_records_from_payloads(
                        list(dict(overlay_payload.get("evidence_lookup") or {}).get(str(candidate_id or "").strip()) or [])
                    ),
                    "evidence": [
                        dict(item)
                        for item in list(
                            dict(overlay_payload.get("evidence_lookup") or {}).get(str(candidate_id or "").strip()) or []
                        )
                        if isinstance(item, dict)
                    ],
                }
        target_company = str(
            (request.target_company if isinstance(request, JobRequest) else "")
            or candidate_source.get("target_company")
            or ""
        ).strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = str(
            candidate_source.get("asset_view")
            or (request.asset_view if isinstance(request, JobRequest) else "")
            or "canonical_merged"
        ).strip() or "canonical_merged"
        if not target_company or not snapshot_id:
            return {}
        try:
            return load_authoritative_candidate_detail(
                runtime_dir=str(self.runtime_dir),
                target_company=target_company,
                candidate_id=candidate_id,
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                store=self.store,
                allow_candidate_documents_fallback=self._candidate_source_allows_candidate_documents_fallback(
                    candidate_source
                ),
            )
        except CandidateArtifactError:
            return {}

    def _load_authoritative_candidate_details_for_source(
        self,
        *,
        request: JobRequest | None,
        candidate_source: dict[str, Any],
        candidate_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not candidate_source_is_snapshot_authoritative(candidate_source):
            return {}
        overlay_payload = self._load_candidate_source_asset_population_overlay(candidate_source)
        if overlay_payload:
            overlay_candidates = {
                str(dict(item).get("candidate_id") or "").strip(): dict(item)
                for item in list(overlay_payload.get("candidates") or [])
                if isinstance(item, dict) and str(dict(item).get("candidate_id") or "").strip()
            }
            evidence_lookup = dict(overlay_payload.get("evidence_lookup") or {})
            overlay_details: dict[str, dict[str, Any]] = {}
            for candidate_id in list(candidate_ids or []):
                normalized_candidate_id = str(candidate_id or "").strip()
                if not normalized_candidate_id:
                    continue
                overlay_record = dict(overlay_candidates.get(normalized_candidate_id) or {})
                if not overlay_record:
                    continue
                overlay_candidate_payload = {
                    field_name: overlay_record.get(field_name)
                    for field_name in Candidate.__dataclass_fields__
                    if field_name in overlay_record
                }
                overlay_details[normalized_candidate_id] = {
                    "status": "loaded",
                    "target_company": str(
                        overlay_payload.get("target_company")
                        or candidate_source.get("target_company")
                        or (request.target_company if isinstance(request, JobRequest) else "")
                        or ""
                    ).strip(),
                    "company_key": normalize_company_key(
                        str(
                            overlay_payload.get("target_company")
                            or candidate_source.get("target_company")
                            or (request.target_company if isinstance(request, JobRequest) else "")
                            or ""
                        ).strip()
                    ),
                    "snapshot_id": str(
                        overlay_payload.get("snapshot_id") or candidate_source.get("snapshot_id") or ""
                    ).strip(),
                    "asset_view": str(
                        overlay_payload.get("asset_view") or candidate_source.get("asset_view") or "canonical_merged"
                    ).strip()
                    or "canonical_merged",
                    "source_kind": "job_asset_population_overlay",
                    "candidate": normalize_candidate(Candidate(**overlay_candidate_payload)),
                    "evidence_records": build_evidence_records_from_payloads(
                        list(evidence_lookup.get(normalized_candidate_id) or [])
                    ),
                    "evidence": [
                        dict(item) for item in list(evidence_lookup.get(normalized_candidate_id) or []) if isinstance(item, dict)
                    ],
                }
            if overlay_details:
                return overlay_details
        target_company = str(
            (request.target_company if isinstance(request, JobRequest) else "")
            or candidate_source.get("target_company")
            or ""
        ).strip()
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        asset_view = str(
            candidate_source.get("asset_view")
            or (request.asset_view if isinstance(request, JobRequest) else "")
            or "canonical_merged"
        ).strip() or "canonical_merged"
        if not target_company or not snapshot_id:
            return {}
        try:
            return load_authoritative_candidate_details(
                runtime_dir=str(self.runtime_dir),
                target_company=target_company,
                candidate_ids=list(candidate_ids or []),
                snapshot_id=snapshot_id,
                asset_view=asset_view,
                store=self.store,
                allow_candidate_documents_fallback=self._candidate_source_allows_candidate_documents_fallback(
                    candidate_source
                ),
            )
        except CandidateArtifactError:
            return {}

    def _candidate_source_allows_candidate_documents_fallback(self, candidate_source: dict[str, Any]) -> bool:
        source_path_value = str(candidate_source.get("source_path") or "").strip()
        if not source_path_value:
            return False
        return Path(source_path_value).name == "candidate_documents.json"

    def get_job_candidate_detail(self, job_id: str, candidate_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        normalized_candidate_id = str(candidate_id or "").strip()
        if not normalized_candidate_id:
            return None
        context = self._build_job_candidate_api_context(job)
        request = context.get("request")
        candidate_source = dict(context.get("candidate_source") or {})
        effective_execution_semantics = dict(context.get("effective_execution_semantics") or {})
        publishable_email_lookup = context.get("publishable_email_lookup")
        authoritative_detail = self._load_authoritative_candidate_detail_for_source(
            request=request,
            candidate_source=candidate_source,
            candidate_id=normalized_candidate_id,
        )
        result_records = self.store.get_job_results_for_candidates(
            job_id,
            [normalized_candidate_id],
            include_evidence=False,
        )
        raw_result_record = result_records[0] if result_records else None
        result_record = (
            self._serialize_candidate_api_record(
                raw_result_record,
                load_profile_timeline=True,
                publishable_email_lookup=publishable_email_lookup,
            )
            if raw_result_record is not None
            else None
        )
        manual_review_items = [
            item
            for item in self.store.list_manual_review_items(job_id=job_id, status="", limit=200)
            if str(item.get("candidate_id") or "").strip() == normalized_candidate_id
        ]
        asset_population_record = None
        if result_record is None:
            asset_population_record = self._load_job_asset_population_candidate_record(
                job=job,
                candidate_id=normalized_candidate_id,
                publishable_email_lookup=publishable_email_lookup,
            )
            if asset_population_record is not None:
                result_record = asset_population_record
        candidate = authoritative_detail.get("candidate")
        if not isinstance(candidate, Candidate) and not candidate_source_is_snapshot_authoritative(candidate_source):
            candidate = self.store.get_candidate(normalized_candidate_id)
        if result_record is None and candidate is None and not manual_review_items:
            return None
        candidate_payload = self._build_job_candidate_api_payload(
            normalized_candidate_id=normalized_candidate_id,
            result_record=result_record,
            candidate=candidate,
            load_profile_timeline=True,
            publishable_email_lookup=publishable_email_lookup,
        )
        evidence = [dict(item) for item in list(authoritative_detail.get("evidence") or []) if isinstance(item, dict)]
        if not evidence and not candidate_source_is_snapshot_authoritative(candidate_source):
            evidence = self.store.list_evidence(normalized_candidate_id)
        if not evidence and asset_population_record is not None:
            shard_payload = self._load_asset_population_candidate_shard_payload(
                request=request,
                candidate_source=candidate_source,
                effective_execution_semantics=effective_execution_semantics,
                candidate_id=normalized_candidate_id,
            )
            if shard_payload is not None:
                evidence = [dict(item) for item in list(shard_payload.get("evidence") or []) if isinstance(item, dict)]
        return {
            "job": job,
            "candidate": candidate_payload,
            "result": result_record or {},
            "evidence": evidence,
            "manual_review_items": manual_review_items,
            "request_preview": _load_job_request_preview(job),
        }

    def get_job_candidate_details_batch(self, job_id: str, candidate_ids: list[str]) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        normalized_candidate_ids: list[str] = []
        seen_candidate_ids: set[str] = set()
        for item in list(candidate_ids or []):
            normalized_candidate_id = str(item or "").strip()
            if not normalized_candidate_id or normalized_candidate_id in seen_candidate_ids:
                continue
            seen_candidate_ids.add(normalized_candidate_id)
            normalized_candidate_ids.append(normalized_candidate_id)
        if not normalized_candidate_ids:
            return {"candidates": [], "not_found_candidate_ids": []}
        context = self._build_job_candidate_api_context(job)
        request = context.get("request")
        candidate_source = dict(context.get("candidate_source") or {})
        publishable_email_lookup = context.get("publishable_email_lookup")
        raw_result_records = self.store.get_job_results_for_candidates(
            job_id,
            normalized_candidate_ids,
            include_evidence=False,
        )
        raw_result_map = {
            str(record.get("candidate_id") or "").strip(): record
            for record in raw_result_records
            if str(record.get("candidate_id") or "").strip()
        }
        authoritative_details = self._load_authoritative_candidate_details_for_source(
            request=request,
            candidate_source=candidate_source,
            candidate_ids=normalized_candidate_ids,
        )
        candidates_payload: list[dict[str, Any]] = []
        not_found_candidate_ids: list[str] = []
        for candidate_id in normalized_candidate_ids:
            raw_result_record = raw_result_map.get(candidate_id)
            result_record = (
                self._serialize_candidate_api_record(
                    raw_result_record,
                    load_profile_timeline=False,
                    publishable_email_lookup=publishable_email_lookup,
                )
                if raw_result_record is not None
                else None
            )
            if result_record is None:
                asset_population_record = self._load_job_asset_population_candidate_record(
                    job=job,
                    candidate_id=candidate_id,
                    load_profile_timeline=False,
                    publishable_email_lookup=publishable_email_lookup,
                )
                if asset_population_record is not None:
                    result_record = asset_population_record
            authoritative_detail = dict(authoritative_details.get(candidate_id) or {})
            candidate = authoritative_detail.get("candidate")
            if not isinstance(candidate, Candidate):
                candidate = self.store.get_candidate(candidate_id)
            if result_record is None and candidate is None:
                not_found_candidate_ids.append(candidate_id)
                continue
            candidates_payload.append(
                self._build_job_candidate_api_payload(
                    normalized_candidate_id=candidate_id,
                    result_record=result_record,
                    candidate=candidate,
                    load_profile_timeline=False,
                    publishable_email_lookup=publishable_email_lookup,
                )
            )
        return {
            "candidates": candidates_payload,
            "not_found_candidate_ids": not_found_candidate_ids,
        }

    def get_job_progress(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        job = self._maybe_promote_workflow_job_to_completed_from_final_results(job)
        event_summary = self.store.get_job_progress_event_summary(job_id, hydrate_if_missing=True)
        workers = self.agent_runtime.list_workers(job_id=job_id)
        result_count = self.store.count_job_results(job_id)
        manual_review_count = self.store.count_manual_review_items(job_id=job_id, status="")
        runtime_controls = self._build_live_runtime_controls_payload(job, use_poll_cache=True)
        auto_recovery = self._maybe_auto_recover_workflow_on_progress(
            job=job,
            events=[],
            workers=workers,
            runtime_controls=runtime_controls,
        )
        if str(auto_recovery.get("status") or "") in {"triggered_inline", "completed"}:
            job = self.store.get_job(job_id) or job
            event_summary = self.store.get_job_progress_event_summary(job_id, hydrate_if_missing=True)
            workers = self.agent_runtime.list_workers(job_id=job_id)
            result_count = self.store.count_job_results(job_id)
            manual_review_count = self.store.count_manual_review_items(job_id=job_id, status="")
            runtime_controls = self._build_live_runtime_controls_payload(job, use_poll_cache=False)
        payload = _build_job_progress_payload(
            job=job,
            events=[],
            event_summary=event_summary,
            workers=workers,
            result_count=result_count,
            manual_review_count=manual_review_count,
            runtime_controls=runtime_controls,
        )
        if auto_recovery:
            payload["auto_recovery"] = auto_recovery
        payload["workflow_stage_summaries"] = self._load_workflow_stage_summaries(job=job)
        return payload

    def _workflow_requested_execution_mode(
        self, job: dict[str, Any], *, events: list[dict[str, Any]] | None = None
    ) -> str:
        summary = dict(job.get("summary") or {})
        summary_mode = str(summary.get("runtime_execution_mode") or "").strip().lower()
        if summary_mode:
            return summary_mode

        request_payload = dict(job.get("request") or {})
        request_mode = str(request_payload.get("runtime_execution_mode") or "").strip().lower()
        if request_mode:
            return request_mode

        runtime_controls = dict(summary.get("runtime_controls") or {})
        if runtime_controls:
            if runtime_controls.get("hosted_runtime_watchdog"):
                return "hosted"
            if runtime_controls.get("workflow_runner") or runtime_controls.get("workflow_runner_control"):
                return "managed_subprocess"

        planning_events = list(events or self.store.list_job_events(str(job.get("job_id") or ""), stage="planning"))
        for event in planning_events:
            payload = dict(event.get("payload") or {})
            event_mode = str(payload.get("runtime_execution_mode") or "").strip().lower()
            if event_mode:
                return event_mode
        return ""

    def _workflow_prefers_hosted_execution(
        self, job: dict[str, Any], *, events: list[dict[str, Any]] | None = None
    ) -> bool:
        mode = self._workflow_requested_execution_mode(job, events=events)
        if mode in {"managed_subprocess", "runner_managed", "detached_sidecar"}:
            return False
        if mode == "hosted":
            return True

        summary = dict(job.get("summary") or {})
        runtime_controls = dict(summary.get("runtime_controls") or {})
        if runtime_controls.get("workflow_runner") or runtime_controls.get("workflow_runner_control"):
            return False
        return True

    def _maybe_auto_recover_workflow_on_progress(
        self,
        *,
        job: dict[str, Any],
        events: list[dict[str, Any]],
        workers: list[dict[str, Any]],
        runtime_controls: dict[str, Any],
    ) -> dict[str, Any]:
        if str(job.get("job_type") or "") != "workflow":
            return {}
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return {}
        if str(job.get("status") or "").strip().lower() in {"completed", "failed"}:
            return {}

        worker_summary = _job_worker_summary(workers)
        runtime_health = _classify_job_runtime_health(
            job=job,
            workers=workers,
            worker_summary=worker_summary,
            runtime_controls=runtime_controls,
            blocked_task=str(dict(job.get("summary") or {}).get("blocked_task") or ""),
        )
        classification = str(runtime_health.get("classification") or "").strip()
        if classification not in {
            "runner_not_alive",
            "runner_takeover_pending",
            "blocked_on_acquisition_workers",
            "blocked_ready_for_resume",
            "queued_waiting_for_runner",
            "runner_failed_to_start",
        }:
            return {
                "status": "skipped",
                "reason": "runtime_not_takeover_eligible",
                "classification": classification,
            }

        cooldown_seconds = max(3, int(_env_int("WORKFLOW_PROGRESS_AUTO_TAKEOVER_COOLDOWN_SECONDS", 15)))
        recent_events = self.store.list_job_events(
            job_id,
            stage="runtime_control",
            limit=20,
            descending=True,
        )
        for event in recent_events:
            payload = dict(event.get("payload") or {})
            if str(payload.get("control") or "") != "progress_auto_takeover":
                continue
            created_at = _parse_timestamp(str(event.get("created_at") or ""))
            if created_at is None:
                break
            age_seconds = max(int((datetime.now(timezone.utc) - created_at).total_seconds()), 0)
            if age_seconds < cooldown_seconds:
                return {
                    "status": "skipped",
                    "reason": "cooldown_active",
                    "classification": classification,
                    "cooldown_seconds": cooldown_seconds,
                    "seconds_until_retry": max(cooldown_seconds - age_seconds, 0),
                }
            break

        recovery_payload = {
            "job_id": job_id,
            "workflow_stale_scope_job_id": job_id,
            "workflow_resume_explicit_job": True,
            "workflow_auto_resume_enabled": True,
            "workflow_resume_stale_after_seconds": 0,
            "workflow_resume_limit": 1,
            "workflow_queue_auto_takeover_enabled": True,
            "workflow_queue_resume_stale_after_seconds": 0,
            "workflow_queue_resume_limit": 1,
            "runtime_heartbeat_source": "progress_poll",
            "runtime_heartbeat_interval_seconds": 0,
        }
        if classification in {
            "runner_not_alive",
            "runner_takeover_pending",
            "blocked_on_acquisition_workers",
            "runner_failed_to_start",
        }:
            recovery_payload["stale_after_seconds"] = 0

        requested_execution_mode = self._workflow_requested_execution_mode(job, events=events)
        effective_execution_mode = (
            "hosted" if self._workflow_prefers_hosted_execution(job, events=events) else "managed_subprocess"
        )
        inflight = self._get_progress_takeover_inflight(job_id)
        if inflight:
            queued_at = str(inflight.get("queued_at") or "").strip()
            return {
                "status": "skipped",
                "reason": "already_inflight",
                "classification": classification,
                "requested_execution_mode": requested_execution_mode,
                "effective_execution_mode": effective_execution_mode,
                "queued_at": queued_at,
            }

        return self._queue_progress_auto_takeover(
            job_id=job_id,
            classification=classification,
            requested_execution_mode=requested_execution_mode,
            effective_execution_mode=effective_execution_mode,
            recovery_payload=recovery_payload,
        )

    def _get_progress_takeover_inflight(self, job_id: str) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        with self._progress_takeover_lock:
            current = dict(self._progress_takeover_inflight.get(normalized_job_id) or {})
        if not current:
            return {}
        queued_at_raw = str(current.get("queued_at") or "").strip()
        queued_at = _parse_timestamp(queued_at_raw)
        max_age_seconds = max(15, int(_env_int("WORKFLOW_PROGRESS_AUTO_TAKEOVER_INFLIGHT_STALE_SECONDS", 60)))
        if queued_at is not None:
            age_seconds = max(int((datetime.now(timezone.utc) - queued_at).total_seconds()), 0)
            if age_seconds >= max_age_seconds:
                with self._progress_takeover_lock:
                    latest = dict(self._progress_takeover_inflight.get(normalized_job_id) or {})
                    if str(latest.get("queued_at") or "").strip() == queued_at_raw:
                        self._progress_takeover_inflight.pop(normalized_job_id, None)
                self.store.append_job_event(
                    normalized_job_id,
                    stage="runtime_control",
                    status="failed",
                    detail="Cleared stale progress auto takeover inflight marker after timeout.",
                    payload={
                        "control": "progress_auto_takeover_reset",
                        "queued_at": queued_at_raw,
                        "age_seconds": age_seconds,
                        "timeout_seconds": max_age_seconds,
                    },
                )
                return {}
        return current

    def _queue_progress_auto_takeover(
        self,
        *,
        job_id: str,
        classification: str,
        requested_execution_mode: str,
        effective_execution_mode: str,
        recovery_payload: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"status": "skipped", "reason": "invalid_job_id", "classification": classification}
        queued_at = datetime.now(timezone.utc).isoformat()
        control_payload = {
            "control": "progress_auto_takeover",
            "classification": classification,
            "requested_execution_mode": requested_execution_mode,
            "effective_execution_mode": effective_execution_mode,
            "recovery_status": "queued",
            "queued_at": queued_at,
        }
        with self._progress_takeover_lock:
            if normalized_job_id in self._progress_takeover_inflight:
                current = dict(self._progress_takeover_inflight.get(normalized_job_id) or {})
                return {
                    "status": "skipped",
                    "reason": "already_inflight",
                    "classification": classification,
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": effective_execution_mode,
                    "queued_at": str(current.get("queued_at") or ""),
                }
            self._progress_takeover_inflight[normalized_job_id] = {
                "classification": classification,
                "requested_execution_mode": requested_execution_mode,
                "effective_execution_mode": effective_execution_mode,
                "queued_at": queued_at,
            }

        self.store.append_job_event(
            normalized_job_id,
            stage="runtime_control",
            status="running",
            detail=f"Progress polling queued automatic workflow takeover for `{classification}`.",
            payload=control_payload,
        )
        thread = threading.Thread(
            target=self._run_progress_auto_takeover,
            kwargs={
                "job_id": normalized_job_id,
                "classification": classification,
                "requested_execution_mode": requested_execution_mode,
                "effective_execution_mode": effective_execution_mode,
                "recovery_payload": dict(recovery_payload),
                "queued_at": queued_at,
            },
            name=f"progress-auto-takeover-{normalized_job_id}",
            daemon=True,
        )
        thread.start()
        return {
            "status": "queued",
            "classification": classification,
            "requested_execution_mode": requested_execution_mode,
            "effective_execution_mode": effective_execution_mode,
            "recovery_status": "queued",
            "queued_at": queued_at,
        }

    def _run_progress_auto_takeover(
        self,
        *,
        job_id: str,
        classification: str,
        requested_execution_mode: str,
        effective_execution_mode: str,
        recovery_payload: dict[str, Any],
        queued_at: str,
    ) -> None:
        try:
            recovery = self.run_worker_recovery_once(recovery_payload)
            workflow_resume = self._extract_progress_takeover_workflow_resume(job_id=job_id, recovery=recovery)
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="running",
                detail=f"Progress auto takeover finished for `{classification}`.",
                payload={
                    "control": "progress_auto_takeover_result",
                    "classification": classification,
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": effective_execution_mode,
                    "queued_at": queued_at,
                    "recovery_status": str(recovery.get("status") or ""),
                    "workflow_resume": workflow_resume,
                },
            )
        except Exception as exc:
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="failed",
                detail=f"Progress auto takeover failed for `{classification}`: {exc}",
                payload={
                    "control": "progress_auto_takeover_result",
                    "classification": classification,
                    "requested_execution_mode": requested_execution_mode,
                    "effective_execution_mode": effective_execution_mode,
                    "queued_at": queued_at,
                    "recovery_status": "failed",
                    "error": str(exc),
                },
            )
        finally:
            with self._progress_takeover_lock:
                self._progress_takeover_inflight.pop(str(job_id or "").strip(), None)

    def _extract_progress_takeover_workflow_resume(
        self,
        *,
        job_id: str,
        recovery: dict[str, Any],
    ) -> list[dict[str, str]]:
        normalized_job_id = str(job_id or "").strip()
        return [
            {
                "job_id": str(item.get("job_id") or ""),
                "status": str(item.get("status") or ""),
                "reason": str(item.get("reason") or ""),
                "resume_mode": str(item.get("resume_mode") or ""),
                "job_status": str(item.get("job_status") or ""),
                "job_stage": str(item.get("job_stage") or ""),
            }
            for item in list(recovery.get("workflow_resume") or [])
            if str(item.get("job_id") or "").strip() == normalized_job_id
        ]

    def get_job_trace(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        return {
            "job_id": job_id,
            "agent_runtime_session": self.store.get_agent_runtime_session(job_id=job_id) or {},
            "agent_trace_spans": self.store.list_agent_trace_spans(job_id=job_id),
            "agent_workers": self.agent_runtime.list_workers(job_id=job_id),
        }

    def get_job_workers(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        return {
            "job_id": job_id,
            "agent_runtime_session": self.store.get_agent_runtime_session(job_id=job_id) or {},
            "agent_workers": self.agent_runtime.list_workers(job_id=job_id),
        }

    def get_job_scheduler(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        session = self.store.get_agent_runtime_session(job_id=job_id) or {}
        workers = self.agent_runtime.list_workers(job_id=job_id)
        plan_payload = dict(job.get("plan") or {})
        return {
            "job_id": job_id,
            "agent_runtime_session": session,
            "scheduler": summarize_scheduler(plan_payload=plan_payload, workers=workers),
        }

    def interrupt_agent_worker(self, payload: dict[str, Any]) -> dict[str, Any]:
        worker_id = int(payload.get("worker_id") or 0)
        if worker_id <= 0:
            return {"status": "invalid", "reason": "worker_id is required"}
        worker = self.agent_runtime.interrupt_worker(worker_id)
        if worker is None:
            return {"status": "not_found", "worker_id": worker_id}
        return {"status": "interrupt_requested", "worker": worker}

    def list_recoverable_agent_workers(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        workers = self.store.list_recoverable_agent_workers(
            limit=int(payload.get("limit") or 100),
            stale_after_seconds=int(payload.get("stale_after_seconds") or 180),
            lane_id=str(payload.get("lane_id") or ""),
            job_id=str(payload.get("job_id") or ""),
        )
        return {
            "recoverable_workers": workers,
            "count": len(workers),
            "stale_after_seconds": int(payload.get("stale_after_seconds") or 180),
            "lane_id": str(payload.get("lane_id") or ""),
            "job_id": str(payload.get("job_id") or ""),
        }

    def cleanup_recoverable_workers(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        stale_after_seconds = max(1, int(payload.get("stale_after_seconds") or 180))
        limit = max(1, int(payload.get("limit") or 200))
        dry_run = _coerce_bool(payload.get("dry_run"), False)
        terminal_workflows_only = _coerce_bool(payload.get("terminal_workflows_only"), True)
        include_missing_jobs = _coerce_bool(payload.get("include_missing_jobs"), False)
        target_company_filter = str(payload.get("target_company") or "").strip().lower()
        parent_job_statuses = {
            str(item or "").strip().lower()
            for item in list(payload.get("parent_job_statuses") or [])
            if str(item or "").strip()
        }
        override_status = str(payload.get("status") or "").strip().lower()
        reason = str(payload.get("reason") or "").strip() or "Retired stale recoverable worker during operator cleanup."

        recoverable_workers = self.store.list_recoverable_agent_workers(
            limit=limit,
            stale_after_seconds=stale_after_seconds,
            lane_id=str(payload.get("lane_id") or ""),
            job_id=str(payload.get("job_id") or ""),
        )

        candidates: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for worker in recoverable_workers:
            job_id = str(worker.get("job_id") or "").strip()
            job = self.store.get_job(job_id) or {}
            job_type = str(job.get("job_type") or "").strip().lower()
            job_status = str(job.get("status") or "").strip().lower()
            job_stage = str(job.get("stage") or "").strip().lower()
            request_payload = dict(job.get("request") or {})
            target_company = str(request_payload.get("target_company") or "").strip()

            if target_company_filter and target_company.lower() != target_company_filter:
                continue

            parent_missing = not bool(job)
            terminal_workflow = job_type == "workflow" and self._job_is_terminal(job)
            if terminal_workflows_only and not terminal_workflow:
                if include_missing_jobs and parent_missing:
                    pass
                else:
                    skipped.append(
                        {
                            "worker_id": int(worker.get("worker_id") or 0),
                            "job_id": job_id,
                            "reason": "parent_job_not_terminal_workflow",
                            "parent_job_status": job_status,
                            "parent_job_stage": job_stage,
                        }
                    )
                    continue

            if parent_job_statuses and job_status not in parent_job_statuses:
                skipped.append(
                    {
                        "worker_id": int(worker.get("worker_id") or 0),
                        "job_id": job_id,
                        "reason": "parent_job_status_filtered",
                        "parent_job_status": job_status,
                        "parent_job_stage": job_stage,
                    }
                )
                continue

            cleanup_status = override_status or ("superseded" if job_status == "superseded" else "cancelled")
            candidates.append(
                {
                    "worker_id": int(worker.get("worker_id") or 0),
                    "job_id": job_id,
                    "lane_id": str(worker.get("lane_id") or ""),
                    "worker_key": str(worker.get("worker_key") or ""),
                    "worker_status": str(worker.get("status") or ""),
                    "wait_stage": str(worker.get("wait_stage") or ""),
                    "updated_at": str(worker.get("updated_at") or ""),
                    "parent_job_type": job_type,
                    "parent_job_status": job_status,
                    "parent_job_stage": job_stage,
                    "target_company": target_company,
                    "cleanup_status": cleanup_status,
                    "cleanup_reason": reason,
                }
            )

        if dry_run:
            return {
                "status": "preview",
                "candidate_count": len(candidates),
                "skipped_count": len(skipped),
                "candidates": candidates,
                "skipped": skipped[:50],
            }

        retired_results: list[dict[str, Any]] = []
        grouped_worker_ids: dict[str, list[int]] = {}
        for candidate in candidates:
            grouped_worker_ids.setdefault(str(candidate["cleanup_status"]), []).append(int(candidate["worker_id"]))

        retired_by_worker_id: dict[int, dict[str, Any]] = {}
        for cleanup_status, worker_ids in grouped_worker_ids.items():
            retired_workers = self.store.retire_agent_workers(
                worker_ids=worker_ids,
                status=cleanup_status,
                reason=reason,
                cleanup_metadata={"source": "recoverable_worker_cleanup"},
            )
            for worker in retired_workers:
                retired_by_worker_id[int(worker.get("worker_id") or 0)] = worker

        for candidate in candidates:
            worker_id = int(candidate["worker_id"])
            retired = retired_by_worker_id.get(worker_id)
            if retired is None:
                retired_results.append({**candidate, "cleanup_result": "not_found"})
                continue
            retired_results.append(
                {
                    **candidate,
                    "cleanup_result": "retired",
                    "final_worker_status": str(retired.get("status") or ""),
                }
            )

        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="cleanup_recoverable_workers")
        return {
            "status": "completed",
            "retired_count": len([item for item in retired_results if item.get("cleanup_result") == "retired"]),
            "skipped_count": len(skipped),
            "results": retired_results,
            "skipped": skipped[:50],
            "runtime_metrics": {
                "status": str(runtime_metrics.get("status") or ""),
                "observed_at": str(runtime_metrics.get("observed_at") or ""),
                "metrics": dict(runtime_metrics.get("metrics") or {}),
            },
        }

    def run_worker_recovery_once(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        recovery_stale_after_seconds = _coerce_int(payload.get("stale_after_seconds"), 180)
        daemon = self._build_worker_recovery_daemon(payload)
        summary = daemon.run_once()
        explicit_job_id = str(payload.get("job_id") or "")
        workflow_stale_scope_job_id = str(payload.get("workflow_stale_scope_job_id") or "")
        workflow_resume_explicit_job = _coerce_bool(
            payload.get("workflow_resume_explicit_job"),
            True,
        )
        workflow_resume_stale_after_seconds = _coerce_int(
            payload.get("workflow_resume_stale_after_seconds"),
            _env_int("WORKFLOW_AUTO_RESUME_STALE_SECONDS", 60),
        )
        workflow_resume_limit = _coerce_int(
            payload.get("workflow_resume_limit"),
            _env_int("WORKFLOW_AUTO_RESUME_LIMIT", 50),
        )
        workflow_auto_resume_enabled = _coerce_bool(
            payload.get("workflow_auto_resume_enabled"),
            _env_bool("WORKFLOW_AUTO_RESUME_ENABLED", True),
        )
        workflow_queue_resume_stale_after_seconds = _coerce_int(
            payload.get("workflow_queue_resume_stale_after_seconds"),
            _env_int("WORKFLOW_QUEUE_AUTO_TAKEOVER_STALE_SECONDS", 60),
        )
        workflow_queue_resume_limit = _coerce_int(
            payload.get("workflow_queue_resume_limit"),
            _env_int("WORKFLOW_QUEUE_AUTO_TAKEOVER_LIMIT", 50),
        )
        workflow_queue_auto_takeover_enabled = _coerce_bool(
            payload.get("workflow_queue_auto_takeover_enabled"),
            _env_bool("WORKFLOW_QUEUE_AUTO_TAKEOVER_ENABLED", True),
        )
        blocked_workflow_cleanup = {"status": "skipped", "reason": "not_requested", "results": []}
        if not explicit_job_id:
            blocked_workflow_cleanup = self.cleanup_blocked_workflow_residue(
                {
                    "active_limit": max(
                        20,
                        int(payload.get("blocked_workflow_cleanup_limit") or workflow_resume_limit or 50),
                    ),
                }
            )
        workflow_resume = self._resume_blocked_workflows_after_recovery(
            summary,
            explicit_job_id=explicit_job_id if workflow_resume_explicit_job else "",
            stale_job_scope_job_id=workflow_stale_scope_job_id,
            include_stale_acquiring=workflow_auto_resume_enabled,
            stale_after_seconds=workflow_resume_stale_after_seconds,
            resume_limit=workflow_resume_limit,
            include_stale_queued=workflow_queue_auto_takeover_enabled,
            queued_stale_after_seconds=workflow_queue_resume_stale_after_seconds,
            queued_resume_limit=workflow_queue_resume_limit,
        )
        post_completion_reconcile = self._reconcile_completed_workflows_after_recovery(
            summary,
            explicit_job_id=explicit_job_id,
        )
        explicit_job_followup_rounds = 0
        if explicit_job_id.strip():
            explicit_job_followup_rounds = max(
                0,
                _coerce_int(
                    payload.get("explicit_job_followup_rounds"),
                    _env_int("WORKFLOW_EXPLICIT_JOB_RECOVERY_FOLLOWUP_ROUNDS", 6),
                ),
            )
        if explicit_job_followup_rounds > 0:
            recovery_daemon_limit = max(10, int(payload.get("total_limit") or 4) * 10)
            recovery_owner_id = str(summary.get("owner_id") or payload.get("owner_id") or "").strip()
            followup_rounds_executed = 0
            for _ in range(explicit_job_followup_rounds):
                recoverable_workers = self.store.list_recoverable_agent_workers(
                    limit=recovery_daemon_limit,
                    stale_after_seconds=recovery_stale_after_seconds,
                    job_id=explicit_job_id,
                )
                if not recoverable_workers:
                    break
                followup_payload = dict(payload)
                followup_payload["job_id"] = explicit_job_id
                if recovery_owner_id:
                    followup_payload["owner_id"] = recovery_owner_id
                followup_daemon = self._build_worker_recovery_daemon(followup_payload)
                followup_summary = followup_daemon.run_once()
                summary = self._merge_worker_recovery_daemon_summaries(summary, followup_summary)
                workflow_resume.extend(
                    self._resume_blocked_workflows_after_recovery(
                        followup_summary,
                        explicit_job_id=explicit_job_id if workflow_resume_explicit_job else "",
                        stale_job_scope_job_id=workflow_stale_scope_job_id,
                        include_stale_acquiring=workflow_auto_resume_enabled,
                        stale_after_seconds=workflow_resume_stale_after_seconds,
                        resume_limit=workflow_resume_limit,
                        include_stale_queued=workflow_queue_auto_takeover_enabled,
                        queued_stale_after_seconds=workflow_queue_resume_stale_after_seconds,
                        queued_resume_limit=workflow_queue_resume_limit,
                    )
                )
                post_completion_reconcile.extend(
                    self._reconcile_completed_workflows_after_recovery(
                        followup_summary,
                        explicit_job_id=explicit_job_id,
                    )
                )
                followup_rounds_executed += 1
                if (
                    int(followup_summary.get("claimed_count") or 0) <= 0
                    and int(followup_summary.get("executed_count") or 0) <= 0
                ):
                    break
            if followup_rounds_executed > 0:
                summary["followup_rounds_executed"] = (
                    int(summary.get("followup_rounds_executed") or 0) + followup_rounds_executed
                )
        runtime_heartbeat = self._emit_runtime_heartbeats_after_recovery(
            payload=payload,
            daemon_summary=summary,
            workflow_resume=workflow_resume,
            post_completion_reconcile=post_completion_reconcile,
            explicit_job_id=explicit_job_id,
        )
        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="worker_recovery_once")
        return {
            "status": "completed",
            "daemon": summary,
            "blocked_workflow_cleanup": blocked_workflow_cleanup,
            "workflow_resume": workflow_resume,
            "post_completion_reconcile": post_completion_reconcile,
            "runtime_heartbeat": runtime_heartbeat,
            "runtime_metrics": {
                "status": str(runtime_metrics.get("status") or ""),
                "observed_at": str(runtime_metrics.get("observed_at") or ""),
                "metrics": dict(runtime_metrics.get("metrics") or {}),
            },
        }

    def run_worker_recovery_forever(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        owner_id = str(payload.get("owner_id") or f"recovery-daemon-{uuid.uuid4().hex[:8]}")
        poll_seconds = max(0.1, float(payload.get("poll_seconds") or 5.0))
        max_ticks = int(payload.get("max_ticks") or 0)
        tick = 0
        last_result: dict[str, Any] = {"status": "completed", "daemon": {}, "workflow_resume": []}
        while True:
            tick += 1
            last_result = self.run_worker_recovery_once({**payload, "owner_id": owner_id})
            last_result["tick"] = tick
            if max_ticks > 0 and tick >= max_ticks:
                break
            time.sleep(poll_seconds)
        return last_result

    def run_worker_daemon_service(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        job_id = str(payload.get("job_id") or "").strip()
        if bool(payload.get("job_scoped")) and job_id:
            summary = self._run_job_scoped_recovery_service(job_id, payload)
            return {
                "status": "completed",
                "scope": "job_scoped",
                "job_id": job_id,
                "service": summary,
            }
        service = self._build_worker_daemon_service(payload)
        summary = service.run_forever(max_ticks=int(payload.get("max_ticks") or 0))
        return {"status": "completed", "service": summary}

    def run_hosted_runtime_watchdog_service(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        config = self._hosted_runtime_watchdog_config(payload)
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda _: self.run_hosted_runtime_watchdog_once(
                {
                    "shared_service_name": str(config["shared_service_name"]),
                    "hosted_runtime_watchdog_service_name": str(config["service_name"]),
                    "hosted_runtime_source": str(config["service_name"]),
                }
            ),
            service_name=str(config["service_name"]),
            poll_seconds=float(config["poll_seconds"]),
            callback_payload={
                "hosted_runtime_source": str(config["service_name"]),
                "shared_service_name": str(config["shared_service_name"]),
            },
        )
        summary = service.run_forever(max_ticks=int(config["max_ticks"]))
        return {
            "status": "completed",
            "scope": "hosted_runtime_watchdog",
            "service": summary,
        }

    def get_worker_daemon_status(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        job_id = str(payload.get("job_id") or "").strip()
        service_name = str(payload.get("service_name") or "worker-recovery-daemon")
        if job_id:
            job = self.store.get_job(job_id)
            if job is None:
                return {"job_id": job_id, "status": "not_found"}
            runtime_controls = self._build_live_runtime_controls_payload(job)
            return {
                "job_id": job_id,
                "status": "ok",
                "runtime_controls": runtime_controls,
                "recovery_services": {
                    "hosted": dict((runtime_controls.get("hosted_runtime_watchdog") or {}).get("service_status") or {}),
                    "shared": dict((runtime_controls.get("shared_recovery") or {}).get("service_status") or {}),
                    "job_scoped": dict((runtime_controls.get("job_recovery") or {}).get("service_status") or {}),
                },
            }
        return read_service_status(self.runtime_dir, service_name)

    def request_runtime_service_shutdown(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        requested_by = str(payload.get("requested_by") or "").strip() or "operator"
        reason = str(payload.get("reason") or "").strip() or "operator_shutdown_requested"
        requested_service_names = payload.get("service_names") or payload.get("service_name")
        job_id = str(payload.get("job_id") or "").strip()
        service_names = (
            _normalize_service_name_list(requested_service_names)
            if requested_service_names
            else ([] if job_id else ["worker-recovery-daemon"])
        )
        if bool(payload.get("include_hosted_watchdog")):
            service_names.append(str(_build_hosted_runtime_watchdog_config({})["service_name"]))
        if bool(payload.get("include_shared_recovery")):
            service_names.append(str(_build_shared_recovery_config({})["service_name"]))
        if job_id:
            service_names.append(str(_build_job_scoped_recovery_config(job_id, {})["service_name"]))
        service_names = _dedupe_texts(service_names)
        requests: list[dict[str, Any]] = []
        for service_name in service_names:
            status = read_service_status(self.runtime_dir, service_name)
            requests.append(
                {
                    "service_name": service_name,
                    "before": status,
                    "stop_request": request_service_stop(
                        self.runtime_dir,
                        service_name,
                        reason=reason,
                        requested_by=requested_by,
                        target_status=status,
                    ),
                }
            )
        return {
            "status": "requested" if requests else "invalid",
            "requested_count": len(requests),
            "requests": requests,
        }

    def cancel_workflow_job(self, job_id: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        normalized_job_id = str(job_id or payload.get("job_id") or "").strip()
        if not normalized_job_id:
            return {"status": "invalid", "reason": "job_id is required"}
        job = self.store.get_job(normalized_job_id)
        if job is None:
            return {"status": "not_found", "job_id": normalized_job_id}
        if str(job.get("job_type") or "") != "workflow":
            return {"status": "invalid", "job_id": normalized_job_id, "reason": "job is not a workflow"}
        current_status = str(job.get("status") or "").strip().lower()
        if current_status in {"completed", "failed", "superseded", "cancelled", "canceled"}:
            return {
                "status": "already_terminal",
                "job_id": normalized_job_id,
                "job_status": current_status,
                "job": job,
            }

        reason = str(payload.get("reason") or "").strip() or "Workflow cancelled by operator."
        summary = dict(job.get("summary") or {})
        summary["message"] = reason
        summary["cancelled_reason"] = reason
        summary["cancelled_at"] = datetime.now(timezone.utc).isoformat()
        self.store.save_job(
            job_id=normalized_job_id,
            job_type="workflow",
            status="cancelled",
            stage="completed",
            request_payload=dict(job.get("request") or {}),
            plan_payload=dict(job.get("plan") or {}),
            execution_bundle_payload=dict(job.get("execution_bundle") or {}),
            summary_payload=summary,
            artifact_path=str(job.get("artifact_path") or ""),
            requester_id=str(job.get("requester_id") or ""),
            tenant_id=str(job.get("tenant_id") or ""),
            idempotency_key=str(job.get("idempotency_key") or ""),
        )
        self.store.update_agent_runtime_session_status(normalized_job_id, "cancelled")
        self.store.release_workflow_job_lease(normalized_job_id)
        active_workers = [
            worker
            for worker in self.agent_runtime.list_workers(job_id=normalized_job_id)
            if str(effective_worker_status(worker) or "") not in {"completed", "failed", "cancelled", "canceled", "superseded"}
        ]
        retired_workers = self.store.retire_agent_workers(
            worker_ids=[int(worker.get("worker_id") or 0) for worker in active_workers],
            status="cancelled",
            reason=reason,
            cleanup_metadata={"source": "workflow_cancel_api"},
        )
        stop_services = self.request_runtime_service_shutdown(
            {
                "job_id": normalized_job_id,
                "reason": reason,
                "requested_by": str(payload.get("requested_by") or "operator"),
            }
        )
        self.store.append_job_event(
            normalized_job_id,
            "runtime_control",
            "cancelled",
            reason,
            {
                "cancelled": True,
                "retired_worker_count": len(retired_workers),
                "service_shutdown": stop_services,
            },
        )
        refreshed = self.store.get_job(normalized_job_id) or {}
        return {
            "status": "cancelled",
            "job_id": normalized_job_id,
            "retired_worker_count": len(retired_workers),
            "service_shutdown": stop_services,
            "job": refreshed,
        }

    def get_runtime_health(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        if not _coerce_bool(payload.get("force_refresh"), False):
            cached = self._load_runtime_metrics_snapshot(payload)
            if cached is not None:
                cache_payload = dict(cached.get("cache") or {})
                cache_payload["status"] = "hit"
                cached["cache"] = cache_payload
                cached["runtime_environment"] = shared_provider_cache_context(runtime_dir=self.runtime_dir)
                return cached
        health = self._compute_runtime_health(payload)
        return self._persist_runtime_metrics_snapshot(health, source="runtime_health")

    def _compute_runtime_health(self, payload: dict[str, Any]) -> dict[str, Any]:
        active_limit = max(10, int(payload.get("active_limit") or 50))
        stale_after_seconds = max(
            1, int(payload.get("stale_after_seconds") or _env_int("WORKFLOW_AUTO_RESUME_STALE_SECONDS", 60))
        )
        queue_stale_after_seconds = max(
            1,
            int(payload.get("queue_stale_after_seconds") or _env_int("WORKFLOW_QUEUE_AUTO_TAKEOVER_STALE_SECONDS", 60)),
        )
        recoverable_worker_limit = max(10, int(payload.get("recoverable_worker_limit") or 100))
        recoverable_worker_stale_after_seconds = max(
            1,
            int(payload.get("recoverable_worker_stale_after_seconds") or 180),
        )

        providers = self.healthcheck_model()
        workflow_counts = self.store.summarize_jobs(job_type="workflow")
        active_jobs = self.store.list_jobs(
            job_type="workflow",
            statuses=["queued", "running", "blocked"],
            limit=active_limit,
        )
        stale_acquiring_jobs = self.store.list_stale_workflow_jobs_in_acquiring(
            stale_after_seconds=stale_after_seconds,
            limit=active_limit,
        )
        stale_queue_jobs = self.store.list_stale_workflow_jobs_in_queue(
            stale_after_seconds=queue_stale_after_seconds,
            limit=active_limit,
        )
        recoverable_workers = self.store.list_recoverable_agent_workers(
            limit=recoverable_worker_limit,
            stale_after_seconds=recoverable_worker_stale_after_seconds,
        )
        shared_recovery_status = read_service_status(
            self.runtime_dir,
            str(payload.get("shared_service_name") or "worker-recovery-daemon"),
        )
        hosted_runtime_watchdog_status = read_service_status(
            self.runtime_dir,
            str(payload.get("hosted_runtime_watchdog_service_name") or "server-runtime-watchdog"),
        )
        organization_asset_warmup_status = self._load_organization_asset_warmup_status()

        stalled_jobs: list[dict[str, Any]] = []
        tracked_job_recoveries: list[dict[str, Any]] = []
        pre_retrieval_refresh_count = 0
        inline_company_roster_worker_count = 0
        inline_search_seed_worker_count = 0
        inline_harvest_prefetch_worker_count = 0
        background_reconcile_job_count = 0
        background_company_roster_reconcile_count = 0
        background_search_seed_reconcile_count = 0
        background_harvest_prefetch_reconcile_count = 0
        active_job_runtime_health: dict[str, dict[str, Any]] = {}
        cleanup_eligible_blocked_jobs: list[dict[str, Any]] = []
        for job in active_jobs:
            job_id = str(job.get("job_id") or "")
            runtime_controls = self._build_live_runtime_controls_payload(job)
            workers = self.agent_runtime.list_workers(job_id=job_id)
            worker_summary = _job_worker_summary(workers)
            job_summary = dict(job.get("summary") or {})
            runtime_health = _classify_job_runtime_health(
                job=job,
                workers=workers,
                worker_summary=worker_summary,
                runtime_controls=runtime_controls,
                blocked_task=str(job_summary.get("blocked_task") or ""),
            )
            if job_id:
                active_job_runtime_health[job_id] = runtime_health
            cleanup_candidate = self._blocked_workflow_cleanup_candidate(job, runtime_health=runtime_health)
            if cleanup_candidate is not None:
                cleanup_eligible_blocked_jobs.append(cleanup_candidate)
            refresh_metrics = _extract_refresh_metrics(job_summary)
            if refresh_metrics:
                if int(refresh_metrics.get("pre_retrieval_refresh_count") or 0) > 0:
                    pre_retrieval_refresh_count += 1
                inline_company_roster_worker_count += int(
                    refresh_metrics.get("inline_company_roster_worker_count") or 0
                )
                inline_search_seed_worker_count += int(refresh_metrics.get("inline_search_seed_worker_count") or 0)
                inline_harvest_prefetch_worker_count += int(
                    refresh_metrics.get("inline_harvest_prefetch_worker_count") or 0
                )
                if int(refresh_metrics.get("background_reconcile_count") or 0) > 0:
                    background_reconcile_job_count += 1
                if int(refresh_metrics.get("background_company_roster_reconcile_count") or 0) > 0:
                    background_company_roster_reconcile_count += 1
                if int(refresh_metrics.get("background_search_seed_reconcile_count") or 0) > 0:
                    background_search_seed_reconcile_count += 1
                if int(refresh_metrics.get("background_harvest_prefetch_reconcile_count") or 0) > 0:
                    background_harvest_prefetch_reconcile_count += 1
            if str(runtime_health.get("state") or "") == "stalled":
                stalled_jobs.append(
                    {
                        "job_id": str(job.get("job_id") or ""),
                        "status": str(job.get("status") or ""),
                        "stage": str(job.get("stage") or ""),
                        "updated_at": str(job.get("updated_at") or ""),
                        "runtime_health": runtime_health,
                    }
                )
            job_recovery = dict(runtime_controls.get("job_recovery") or {})
            if job_recovery:
                tracked_job_recoveries.append(
                    {
                        "job_id": str(job.get("job_id") or ""),
                        "status": str(job.get("status") or ""),
                        "stage": str(job.get("stage") or ""),
                        "job_recovery": {
                            "status": str(job_recovery.get("status") or ""),
                            "service_name": str(job_recovery.get("service_name") or ""),
                            "service_ready": bool(job_recovery.get("service_ready")),
                            "service_status": dict(job_recovery.get("service_status") or {}),
                        },
                    }
                )

        cleanup_eligible_job_ids = {
            str(item.get("job_id") or "").strip()
            for item in cleanup_eligible_blocked_jobs
            if str(item.get("job_id") or "").strip()
        }
        stalled_jobs = [
            item for item in stalled_jobs if str(item.get("job_id") or "").strip() not in cleanup_eligible_job_ids
        ]
        stale_acquiring_jobs = [
            job
            for job in stale_acquiring_jobs
            if (
                str(dict(active_job_runtime_health.get(str(job.get("job_id") or ""), {})).get("state") or "")
                == "stalled"
                and str(job.get("job_id") or "").strip() not in cleanup_eligible_job_ids
            )
        ]
        stale_queue_jobs = [
            job
            for job in stale_queue_jobs
            if str(dict(active_job_runtime_health.get(str(job.get("job_id") or ""), {})).get("state") or "")
            == "stalled"
            or str(job.get("job_id") or "") not in active_job_runtime_health
        ]

        shared_ready = _service_status_is_ready(shared_recovery_status)
        hosted_ready = _service_status_is_ready(hosted_runtime_watchdog_status)
        job_recovery_ready_count = sum(
            1 for item in tracked_job_recoveries if bool(dict(item.get("job_recovery") or {}).get("service_ready"))
        )
        job_recovery_not_ready_count = max(len(tracked_job_recoveries) - job_recovery_ready_count, 0)

        status = "ok"
        if (
            str(providers.get("status") or "") != "ready"
            or (
                active_jobs
                and not shared_ready
                and not hosted_ready
                and str(shared_recovery_status.get("status") or "") in {"not_started", "stale", "failed", "corrupted"}
                and str(hosted_runtime_watchdog_status.get("status") or "")
                in {"not_started", "stale", "failed", "corrupted"}
            )
            or stalled_jobs
            or stale_acquiring_jobs
            or stale_queue_jobs
        ):
            status = "degraded"
        if str(shared_recovery_status.get("status") or "") in {"failed", "corrupted"} and str(
            hosted_runtime_watchdog_status.get("status") or ""
        ) in {"failed", "corrupted", "not_started"}:
            status = "failed"

        metrics = {
            "workflow_jobs": workflow_counts,
            "active_job_count": len(active_jobs),
            "stalled_job_count": len(stalled_jobs),
            "cleanup_eligible_blocked_job_count": len(cleanup_eligible_blocked_jobs),
            "stale_acquiring_job_count": len(stale_acquiring_jobs),
            "stale_queue_job_count": len(stale_queue_jobs),
            "recoverable_worker_count": len(recoverable_workers),
            "tracked_job_recovery_count": len(tracked_job_recoveries),
            "pre_retrieval_refresh_job_count": pre_retrieval_refresh_count,
            "inline_company_roster_worker_count": inline_company_roster_worker_count,
            "inline_search_seed_worker_count": inline_search_seed_worker_count,
            "inline_harvest_prefetch_worker_count": inline_harvest_prefetch_worker_count,
            "background_reconcile_job_count": background_reconcile_job_count,
            "background_company_roster_reconcile_job_count": background_company_roster_reconcile_count,
            "background_search_seed_reconcile_job_count": background_search_seed_reconcile_count,
            "background_harvest_prefetch_reconcile_job_count": background_harvest_prefetch_reconcile_count,
            "shared_recovery_ready": int(shared_ready),
            "hosted_runtime_watchdog_ready": int(hosted_ready),
            "job_recovery_ready_count": job_recovery_ready_count,
            "job_recovery_not_ready_count": job_recovery_not_ready_count,
            "organization_asset_warmup_running": int(
                str(organization_asset_warmup_status.get("status") or "") == "running"
            ),
            "organization_asset_warmup_completed": int(
                str(organization_asset_warmup_status.get("status") or "") == "completed"
            ),
            "organization_asset_warmup_company_count": int(organization_asset_warmup_status.get("company_count") or 0),
        }
        return {
            "status": status,
            "observed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "runtime_environment": shared_provider_cache_context(runtime_dir=self.runtime_dir),
            "providers": providers,
            "services": {
                "shared_recovery": shared_recovery_status,
                "hosted_runtime_watchdog": hosted_runtime_watchdog_status,
                "organization_asset_warmup": organization_asset_warmup_status,
                "job_recoveries": tracked_job_recoveries,
            },
            "service_readiness": {
                "shared_recovery_ready": shared_ready,
                "hosted_runtime_watchdog_ready": hosted_ready,
                "job_recovery_ready_count": job_recovery_ready_count,
                "job_recovery_not_ready_count": job_recovery_not_ready_count,
                "auto_recovery_ready": bool(shared_ready or hosted_ready),
                "recommended_workflow_entrypoint": "serve",
                "standalone_cli_fallback": "managed_subprocess",
            },
            "metrics": metrics,
            "stalled_jobs": stalled_jobs,
            "cleanup_eligible_blocked_jobs": cleanup_eligible_blocked_jobs,
            "stale_jobs": {
                "acquiring": [
                    {
                        "job_id": str(job.get("job_id") or ""),
                        "status": str(job.get("status") or ""),
                        "stage": str(job.get("stage") or ""),
                        "updated_at": str(job.get("updated_at") or ""),
                    }
                    for job in stale_acquiring_jobs
                ],
                "queued": [
                    {
                        "job_id": str(job.get("job_id") or ""),
                        "status": str(job.get("status") or ""),
                        "stage": str(job.get("stage") or ""),
                        "updated_at": str(job.get("updated_at") or ""),
                    }
                    for job in stale_queue_jobs
                ],
            },
            "recoverable_workers": {
                "count": len(recoverable_workers),
                "sample": [
                    {
                        "worker_id": int(worker.get("worker_id") or 0),
                        "job_id": str(worker.get("job_id") or ""),
                        "lane_id": str(worker.get("lane_id") or ""),
                        "status": str(worker.get("status") or ""),
                    }
                    for worker in recoverable_workers[:10]
                ],
            },
        }

    def get_runtime_metrics(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        health = self.get_runtime_health(payload)
        metrics = dict(health.get("metrics") or {})
        return {
            "status": str(health.get("status") or ""),
            "observed_at": str(health.get("observed_at") or ""),
            "metrics": metrics,
            "refresh_metrics": _runtime_refresh_metric_subset(metrics),
            "services": {
                "shared_recovery": dict(dict(health.get("services") or {}).get("shared_recovery") or {}),
                "hosted_runtime_watchdog": dict(
                    dict(health.get("services") or {}).get("hosted_runtime_watchdog") or {}
                ),
                "tracked_job_recovery_count": int(metrics.get("tracked_job_recovery_count") or 0),
            },
            "service_readiness": dict(health.get("service_readiness") or {}),
        }

    def get_system_progress(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        active_limit = max(1, int(payload.get("active_limit") or 10))
        object_sync_limit = max(1, int(payload.get("object_sync_limit") or 20))
        profile_registry_lookback_hours = max(0, int(payload.get("profile_registry_lookback_hours") or 24))
        cloud_asset_operation_limit = max(1, int(payload.get("cloud_asset_operation_limit") or 10))
        target_company = str(payload.get("target_company") or "").strip()
        asset_view = str(payload.get("asset_view") or "canonical_merged").strip() or "canonical_merged"

        runtime = self.get_runtime_metrics(payload)
        active_jobs = self.store.list_jobs(
            job_type="workflow",
            statuses=["queued", "running", "blocked"],
            limit=active_limit,
        )
        workflow_items: list[dict[str, Any]] = []
        for job in active_jobs:
            job_id = str(job.get("job_id") or "")
            progress = self.get_job_progress(job_id) or {}
            progress_payload = dict(progress.get("progress") or {})
            workflow_items.append(
                {
                    "job_id": job_id,
                    "target_company": str(dict(job.get("request") or {}).get("target_company") or ""),
                    "status": str(job.get("status") or ""),
                    "stage": str(job.get("stage") or ""),
                    "updated_at": str(job.get("updated_at") or ""),
                    "runtime_health": dict(progress_payload.get("runtime_health") or {}),
                    "counters": dict(progress_payload.get("counters") or {}),
                    "latest_metrics": dict(progress_payload.get("latest_metrics") or {}),
                    "refresh_metrics": dict(
                        dict(progress_payload.get("latest_metrics") or {}).get("refresh_metrics") or {}
                    ),
                    "pre_retrieval_refresh": dict(
                        dict(progress_payload.get("latest_metrics") or {}).get("pre_retrieval_refresh") or {}
                    ),
                    "background_reconcile": dict(
                        dict(progress_payload.get("latest_metrics") or {}).get("background_reconcile") or {}
                    ),
                }
            )

        object_sync = self._collect_object_sync_progress(limit=object_sync_limit)
        profile_registry = self.store.get_linkedin_profile_registry_metrics(
            lookback_hours=profile_registry_lookback_hours,
        )
        cloud_asset_operations = self.store.list_cloud_asset_operations(
            scoped_company=target_company,
            limit=cloud_asset_operation_limit,
        )
        return {
            "status": str(runtime.get("status") or ""),
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "runtime": runtime,
            "workflow_jobs": {
                "count": len(workflow_items),
                "items": workflow_items,
            },
            "profile_registry": profile_registry,
            "object_sync": object_sync,
            "cloud_asset_operations": {
                "count": len(cloud_asset_operations),
                "items": cloud_asset_operations,
            },
            "company_asset": self._collect_company_asset_progress(
                target_company=target_company,
                asset_view=asset_view,
            ),
        }

    def _collect_company_asset_progress(self, *, target_company: str, asset_view: str) -> dict[str, Any]:
        normalized_company = str(target_company or "").strip()
        normalized_asset_view = str(asset_view or "canonical_merged").strip() or "canonical_merged"
        if not normalized_company:
            return {}
        authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company=normalized_company,
            asset_view=normalized_asset_view,
        )
        execution_profile = self.store.get_organization_execution_profile(
            target_company=normalized_company,
            asset_view=normalized_asset_view,
        )
        return {
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
            "authoritative_registry": {
                "snapshot_id": str(authoritative.get("snapshot_id") or ""),
                "status": str(authoritative.get("status") or ""),
                "candidate_count": int(authoritative.get("candidate_count") or 0),
                "materialization_generation_key": str(authoritative.get("materialization_generation_key") or ""),
                "materialization_generation_sequence": int(
                    authoritative.get("materialization_generation_sequence") or 0
                ),
                "materialization_watermark": str(authoritative.get("materialization_watermark") or ""),
            },
            "execution_profile": {
                "source_snapshot_id": str(execution_profile.get("source_snapshot_id") or ""),
                "status": str(execution_profile.get("status") or ""),
                "source_generation_key": str(execution_profile.get("source_generation_key") or ""),
                "source_generation_sequence": int(execution_profile.get("source_generation_sequence") or 0),
                "source_generation_watermark": str(execution_profile.get("source_generation_watermark") or ""),
                "org_scale_band": str(execution_profile.get("org_scale_band") or ""),
                "default_acquisition_mode": str(execution_profile.get("default_acquisition_mode") or ""),
            },
        }

    def _collect_object_sync_progress(self, *, limit: int) -> dict[str, Any]:
        asset_exports_dir = self.runtime_dir / "asset_exports"
        progress_files: list[Path] = []
        if asset_exports_dir.exists():
            progress_files.extend(asset_exports_dir.glob("*/upload_progress.json"))
            progress_files.extend(asset_exports_dir.glob("*/download_progress.json"))
        generation_sync_dir = self.runtime_dir / "object_sync" / "generations"
        if generation_sync_dir.exists():
            progress_files.extend(generation_sync_dir.glob("*/upload_progress.json"))
            progress_files.extend(generation_sync_dir.glob("*/hydrate_progress.json"))
        transfers: list[dict[str, Any]] = []
        for path in progress_files:
            try:
                payload = json.loads(path.read_text())
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            status = str(payload.get("status") or "")
            completion_ratio = float(payload.get("completion_ratio") or 0.0)
            direction = "download"
            if path.name == "upload_progress.json":
                direction = "upload"
            elif path.name == "hydrate_progress.json":
                direction = "hydrate"
            transfers.append(
                {
                    "bundle_id": str(payload.get("bundle_id") or path.parent.name),
                    "bundle_kind": str(payload.get("bundle_kind") or ""),
                    "direction": direction,
                    "status": status,
                    "updated_at": str(payload.get("updated_at") or ""),
                    "completion_ratio": completion_ratio,
                    "requested_file_count": int(payload.get("requested_file_count") or 0),
                    "completed_file_count": int(payload.get("completed_file_count") or 0),
                    "remaining_file_count": int(payload.get("remaining_file_count") or 0),
                    "transfer_mode": str(payload.get("transfer_mode") or "files"),
                    "bundle_dir": str(path.parent),
                    "progress_path": str(path),
                    "archive": dict(payload.get("archive") or {}),
                }
            )
        transfers.sort(
            key=lambda item: (
                _parse_timestamp(str(item.get("updated_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)
            ),
            reverse=True,
        )
        status_counts = Counter(str(item.get("status") or "") for item in transfers if str(item.get("status") or ""))
        active_transfer_count = sum(
            1
            for item in transfers
            if str(item.get("status") or "") == "running"
            or (
                str(item.get("status") or "") not in {"uploaded", "downloaded", "failed"}
                and float(item.get("completion_ratio") or 0.0) < 1.0
            )
        )
        bundle_index = self._load_runtime_json(
            self.runtime_dir / "object_sync" / "bundle_index.json",
            default={"updated_at": "", "bundles": []},
        )
        tracked_bundles = list(bundle_index.get("bundles") or [])
        generation_index = self._load_runtime_json(
            self.runtime_dir / "object_sync" / "generation_index.json",
            default={"updated_at": "", "generations": []},
        )
        tracked_generations = list(generation_index.get("generations") or [])
        hot_cache_inventory = collect_hot_cache_inventory(self.runtime_dir)
        hot_cache_governance = load_hot_cache_governance_state(self.runtime_dir)
        control_plane_postgres_sync = load_control_plane_postgres_sync_state(self.runtime_dir)
        hot_cache_snapshots = list(hot_cache_inventory.get("snapshot_records") or [])
        hot_cache_generations = list(hot_cache_inventory.get("generation_records") or [])
        return {
            "tracked_bundle_count": len(tracked_bundles),
            "bundle_index_updated_at": str(bundle_index.get("updated_at") or ""),
            "tracked_generation_count": len(tracked_generations),
            "generation_index_updated_at": str(generation_index.get("updated_at") or ""),
            "active_transfer_count": active_transfer_count,
            "status_counts": dict(status_counts),
            "hot_cache": {
                "status": str(hot_cache_inventory.get("status") or ""),
                "snapshot_count": len(hot_cache_snapshots),
                "generation_dir_count": len(hot_cache_generations),
                "total_snapshot_bytes": int(hot_cache_inventory.get("total_snapshot_bytes") or 0),
                "total_generation_bytes": int(hot_cache_inventory.get("total_generation_bytes") or 0),
                "total_bytes": int(hot_cache_inventory.get("total_bytes") or 0),
                "largest_snapshots": [
                    {
                        "company_key": str(item.get("company_key") or ""),
                        "snapshot_id": str(item.get("snapshot_id") or ""),
                        "total_bytes": int(item.get("total_bytes") or 0),
                        "asset_views": list(item.get("asset_views") or []),
                    }
                    for item in sorted(
                        hot_cache_snapshots,
                        key=lambda item: int(dict(item).get("total_bytes") or 0),
                        reverse=True,
                    )[:3]
                ],
                "largest_generation_dirs": [
                    {
                        "generation_key": str(item.get("generation_key") or ""),
                        "company_key": str(item.get("company_key") or ""),
                        "snapshot_id": str(item.get("snapshot_id") or ""),
                        "total_bytes": int(item.get("total_bytes") or 0),
                    }
                    for item in sorted(
                        hot_cache_generations,
                        key=lambda item: int(dict(item).get("total_bytes") or 0),
                        reverse=True,
                    )[:3]
                ],
                "governance": {
                    "status": str(hot_cache_governance.get("status") or ""),
                    "last_checked_at": str(hot_cache_governance.get("last_checked_at") or ""),
                    "last_completed_at": str(hot_cache_governance.get("last_completed_at") or ""),
                    "last_error": str(hot_cache_governance.get("last_error") or ""),
                    "last_success_status": str(
                        dict(hot_cache_governance.get("last_success_summary") or {}).get("status") or ""
                    ),
                },
            },
            "control_plane_postgres": {
                "status": str(control_plane_postgres_sync.get("status") or ""),
                "last_checked_at": str(control_plane_postgres_sync.get("last_checked_at") or ""),
                "last_completed_at": str(control_plane_postgres_sync.get("last_completed_at") or ""),
                "last_sync_status": str(control_plane_postgres_sync.get("last_sync_status") or ""),
                "last_error": str(control_plane_postgres_sync.get("last_error") or ""),
                "last_success_status": str(
                    dict(control_plane_postgres_sync.get("last_success_summary") or {}).get("status") or ""
                ),
            },
            "recent_transfers": transfers[:limit],
        }

    def _load_runtime_json(self, path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError, json.JSONDecodeError):
            return default

    def _refresh_runtime_metrics_snapshot(
        self,
        payload: dict[str, Any] | None = None,
        *,
        source: str,
    ) -> dict[str, Any]:
        health = self._compute_runtime_health(dict(payload or {}))
        return self._persist_runtime_metrics_snapshot(health, source=source)

    def _persist_runtime_metrics_snapshot(self, health: dict[str, Any], *, source: str) -> dict[str, Any]:
        snapshot = dict(health)
        snapshot["cache"] = {
            "status": "materialized",
            "source": source,
            "snapshot_path": str(self.runtime_metrics_snapshot_path),
        }
        self.runtime_metrics_snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2))
        return snapshot

    def _load_runtime_metrics_snapshot(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        if not self._runtime_metrics_cache_allowed(payload):
            return None
        if not self.runtime_metrics_snapshot_path.exists():
            return None
        max_age_seconds = max(1, _env_int("RUNTIME_METRICS_CACHE_MAX_AGE_SECONDS", 15))
        try:
            cached = json.loads(self.runtime_metrics_snapshot_path.read_text())
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        observed_at = _parse_timestamp(str(cached.get("observed_at") or ""))
        if observed_at is None:
            return None
        age_seconds = max(
            int((datetime.now(timezone.utc) - observed_at).total_seconds()),
            0,
        )
        if age_seconds > max_age_seconds:
            return None
        cache_payload = dict(cached.get("cache") or {})
        cache_payload.update(
            {
                "snapshot_age_seconds": age_seconds,
                "max_age_seconds": max_age_seconds,
            }
        )
        cached["cache"] = cache_payload
        return cached

    def _runtime_metrics_cache_allowed(self, payload: dict[str, Any]) -> bool:
        custom_keys = {
            "active_limit",
            "stale_after_seconds",
            "queue_stale_after_seconds",
            "recoverable_worker_limit",
            "recoverable_worker_stale_after_seconds",
            "shared_service_name",
        }
        return not any(key in payload for key in custom_keys)

    def _workflow_job_lease_owner(self) -> str:
        hostname = socket.gethostname().strip() or "localhost"
        return f"{hostname}:{os.getpid()}:{threading.get_ident()}"

    def _persist_workflow_runtime_controls(self, job_id: str, controls: dict[str, Any]) -> None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return
        job = self.store.get_job(normalized_job_id)
        if job is None:
            return
        summary = dict(job.get("summary") or {})
        runtime_controls = dict(summary.get("runtime_controls") or {})
        previous_runtime_controls = dict(runtime_controls)
        updated = False
        for key in (
            "hosted_runtime_watchdog",
            "shared_recovery",
            "job_recovery",
            "workflow_runner",
            "workflow_runner_control",
        ):
            value = controls.get(key)
            if not isinstance(value, dict) or not value:
                continue
            runtime_controls[key] = dict(value)
            updated = True
        if not updated:
            return
        summary["runtime_controls"] = runtime_controls
        self.store.save_job(
            job_id=normalized_job_id,
            job_type=str(job.get("job_type") or ""),
            status=str(job.get("status") or ""),
            stage=str(job.get("stage") or ""),
            request_payload=dict(job.get("request") or {}),
            plan_payload=dict(job.get("plan") or {}),
            summary_payload=summary,
            artifact_path=str(job.get("artifact_path") or ""),
            requester_id=str(job.get("requester_id") or ""),
            tenant_id=str(job.get("tenant_id") or ""),
            idempotency_key=str(job.get("idempotency_key") or ""),
        )
        for event in _build_runtime_control_change_events(
            previous_controls=previous_runtime_controls,
            current_controls=runtime_controls,
        ):
            self.store.append_job_event(
                normalized_job_id,
                stage="runtime_control",
                status=str(event.get("status") or "updated"),
                detail=str(event.get("detail") or "Runtime control updated."),
                payload=dict(event.get("payload") or {}),
            )
        self._refresh_runtime_metrics_snapshot(source="runtime_control")

    def _persist_workflow_runtime_controls_deferred(
        self,
        job_id: str,
        controls: dict[str, Any],
        *,
        max_attempts: int = 6,
        initial_delay_seconds: float = 0.05,
        run_async: bool = True,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"status": "skipped", "reason": "missing_job_id"}

        controls_payload = {
            key: dict(value) for key, value in dict(controls or {}).items() if isinstance(value, dict) and value
        }
        if not controls_payload:
            return {"status": "skipped", "reason": "no_controls"}

        attempts = max(1, int(max_attempts or 1))
        delay_seconds = max(0.01, float(initial_delay_seconds or 0.05))

        def _run() -> None:
            delay = delay_seconds
            for attempt in range(attempts):
                try:
                    self._persist_workflow_runtime_controls(normalized_job_id, controls_payload)
                    return
                except sqlite3.OperationalError as exc:
                    if "database is locked" not in str(exc).lower() or attempt + 1 >= attempts:
                        return
                    time.sleep(delay)
                    delay = min(delay * 2.0, 1.0)

        if run_async:
            threading.Thread(
                target=_run,
                name=f"workflow-runtime-controls-{normalized_job_id}",
                daemon=True,
            ).start()
            return {
                "status": "scheduled",
                "job_id": normalized_job_id,
                "max_attempts": attempts,
            }

        _run()
        return {
            "status": "completed",
            "job_id": normalized_job_id,
            "max_attempts": attempts,
        }

    def _persist_hosted_dispatch_marker(self, job_id: str, hosted_dispatch: dict[str, Any]) -> None:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return
        job = self.store.get_job(normalized_job_id)
        if job is None:
            return
        summary = dict(job.get("summary") or {})
        summary["hosted_dispatch"] = dict(hosted_dispatch or {})
        self.store.save_job(
            job_id=normalized_job_id,
            job_type=str(job.get("job_type") or ""),
            status=str(job.get("status") or ""),
            stage=str(job.get("stage") or ""),
            request_payload=dict(job.get("request") or {}),
            plan_payload=dict(job.get("plan") or {}),
            summary_payload=summary,
            artifact_path=str(job.get("artifact_path") or ""),
            requester_id=str(job.get("requester_id") or ""),
            tenant_id=str(job.get("tenant_id") or ""),
            idempotency_key=str(job.get("idempotency_key") or ""),
        )

    def _ensure_hosted_runtime_watchdog_deferred(
        self,
        payload: dict[str, Any] | None = None,
        *,
        run_async: bool = True,
    ) -> dict[str, Any]:
        normalized_payload = dict(payload or {})
        if not bool(normalized_payload.get("auto_job_daemon", False)):
            return {"status": "disabled", "scope": "hosted_runtime_watchdog"}

        config = self._hosted_runtime_watchdog_config(normalized_payload)
        existing_status = read_service_status(self.runtime_dir, str(config["service_name"]))
        if _service_status_is_ready(existing_status):
            return {
                "status": "already_running",
                **self._recovery_status_fields(
                    config=config,
                    scope="hosted_runtime_watchdog",
                    poll_seconds=float(existing_status.get("poll_seconds") or config["poll_seconds"]),
                    max_ticks=int(existing_status.get("max_ticks") or config["max_ticks"]),
                ),
                "mode": "sidecar",
                "handshake": {
                    "status": "already_running",
                    "service_status": existing_status,
                },
            }

        def _run() -> None:
            try:
                self.ensure_hosted_runtime_watchdog(normalized_payload)
            except Exception:
                return

        if run_async:
            threading.Thread(
                target=_run,
                name=f"hosted-runtime-watchdog-deferred-{str(config['service_name'])}",
                daemon=True,
            ).start()
            return {
                "status": "scheduled",
                **self._recovery_status_fields(config=config, scope="hosted_runtime_watchdog"),
                "mode": "deferred",
            }

        return self.ensure_hosted_runtime_watchdog(normalized_payload)

    def _ensure_shared_recovery_deferred(
        self,
        payload: dict[str, Any] | None = None,
        *,
        run_async: bool = True,
    ) -> dict[str, Any]:
        normalized_payload = dict(payload or {})
        if not bool(normalized_payload.get("auto_job_daemon", False)):
            return {"status": "disabled", "scope": "shared"}

        config = self._shared_recovery_config(normalized_payload)
        existing_status = read_service_status(self.runtime_dir, str(config["service_name"]))
        if _service_status_is_ready(existing_status):
            return {
                "status": "already_running",
                **self._recovery_status_fields(
                    config=config,
                    scope="shared",
                    poll_seconds=float(existing_status.get("poll_seconds") or config["poll_seconds"]),
                    max_ticks=int(existing_status.get("max_ticks") or config["max_ticks"]),
                ),
                "mode": "sidecar",
                "handshake": {
                    "status": "already_running",
                    "service_status": existing_status,
                },
            }

        def _run() -> None:
            try:
                self.ensure_shared_recovery(normalized_payload)
            except Exception:
                return

        if run_async:
            threading.Thread(
                target=_run,
                name=f"shared-recovery-deferred-{str(config['service_name'])}",
                daemon=True,
            ).start()
            return {
                "status": "scheduled",
                **self._recovery_status_fields(config=config, scope="shared"),
                "mode": "deferred",
            }

        return self.ensure_shared_recovery(normalized_payload)

    def _get_progress_probe_cache(
        self,
        *,
        cache_key: str,
        ttl_seconds: float,
        loader: Callable[[], dict[str, Any]],
    ) -> dict[str, Any]:
        normalized_key = str(cache_key or "").strip()
        ttl = max(0.0, float(ttl_seconds or 0.0))
        if not normalized_key or ttl <= 0:
            return dict(loader() or {})
        now = time.monotonic()
        with self._progress_probe_cache_lock:
            cached = dict(self._progress_probe_cache.get(normalized_key) or {})
        cached_observed_at = float(cached.get("observed_monotonic") or 0.0)
        if cached_observed_at > 0 and (now - cached_observed_at) < ttl:
            return dict(cached.get("value") or {})
        value = dict(loader() or {})
        with self._progress_probe_cache_lock:
            self._progress_probe_cache[normalized_key] = {
                "observed_monotonic": now,
                "value": value,
            }
            if len(self._progress_probe_cache) > 256:
                expired_keys = [
                    key
                    for key, payload in self._progress_probe_cache.items()
                    if (now - float(dict(payload).get("observed_monotonic") or 0.0)) >= ttl
                ]
                for key in expired_keys[:128]:
                    self._progress_probe_cache.pop(key, None)
        return value

    def _read_progress_service_status(self, service_name: str, *, use_cache: bool) -> dict[str, Any]:
        normalized_service_name = str(service_name or "").strip()
        if not normalized_service_name:
            return {}
        if not use_cache:
            return read_service_status(self.runtime_dir, normalized_service_name)
        ttl_seconds = _env_float("WORKFLOW_PROGRESS_SERVICE_STATUS_CACHE_SECONDS", 0.75)
        return self._get_progress_probe_cache(
            cache_key=f"service_status::{normalized_service_name}",
            ttl_seconds=ttl_seconds,
            loader=lambda: read_service_status(self.runtime_dir, normalized_service_name),
        )

    def _read_progress_runner_snapshot(
        self,
        *,
        pid: int,
        runner_status: str,
        log_path_value: str,
        use_cache: bool,
    ) -> dict[str, Any]:
        normalized_log_path = str(log_path_value or "").strip()
        normalized_runner_status = str(runner_status or "").strip()
        if pid <= 0:
            return {}

        def _load_snapshot() -> dict[str, Any]:
            process_alive = _workflow_runner_process_alive(pid)
            snapshot: dict[str, Any] = {"process_alive": process_alive}
            if normalized_log_path and (not process_alive or normalized_runner_status != "started"):
                snapshot["log_tail"] = _read_text_tail(Path(normalized_log_path))
            return snapshot

        if not use_cache:
            return _load_snapshot()
        ttl_seconds = _env_float(
            "WORKFLOW_PROGRESS_RUNNER_STATUS_CACHE_SECONDS",
            _env_float("WORKFLOW_PROGRESS_SERVICE_STATUS_CACHE_SECONDS", 0.75),
        )
        return self._get_progress_probe_cache(
            cache_key=f"runner_status::{pid}::{normalized_runner_status}::{normalized_log_path}",
            ttl_seconds=ttl_seconds,
            loader=_load_snapshot,
        )

    def _build_live_runtime_controls_payload(
        self, job: dict[str, Any], *, use_poll_cache: bool = False
    ) -> dict[str, Any]:
        summary = dict(job.get("summary") or {})
        runtime_controls = dict(summary.get("runtime_controls") or {})
        payload: dict[str, Any] = {}
        for key in (
            "hosted_runtime_watchdog",
            "shared_recovery",
            "job_recovery",
            "workflow_runner",
            "workflow_runner_control",
        ):
            value = runtime_controls.get(key)
            if not isinstance(value, dict) or not value:
                continue
            control = dict(value)
            if key in {"hosted_runtime_watchdog", "shared_recovery", "job_recovery"}:
                service_name = str(control.get("service_name") or "").strip()
                if service_name:
                    service_status = self._read_progress_service_status(service_name, use_cache=use_poll_cache)
                    control["service_status"] = service_status
                    control["service_ready"] = _service_status_is_ready(service_status)
            elif key == "workflow_runner":
                pid = int(control.get("pid") or 0)
                runner_snapshot = self._read_progress_runner_snapshot(
                    pid=pid,
                    runner_status=str(control.get("status") or ""),
                    log_path_value=str(control.get("log_path") or ""),
                    use_cache=use_poll_cache,
                )
                if runner_snapshot:
                    control.update(runner_snapshot)
            payload[key] = control
        job_id = str(job.get("job_id") or "").strip()
        if job_id:
            workflow_job_lease = self.store.get_workflow_job_lease(job_id)
            if workflow_job_lease:
                lease_payload = dict(workflow_job_lease)
                lease_payload["heartbeat_age_seconds"] = _workflow_job_lease_heartbeat_age_seconds(lease_payload)
                lease_payload["lease_alive"] = _workflow_job_lease_is_alive(lease_payload)
                payload["workflow_job_lease"] = lease_payload
        return payload

    def _emit_runtime_heartbeats_after_recovery(
        self,
        *,
        payload: dict[str, Any],
        daemon_summary: dict[str, Any],
        workflow_resume: list[dict[str, Any]],
        post_completion_reconcile: list[dict[str, Any]],
        explicit_job_id: str,
    ) -> list[dict[str, Any]]:
        source = str(
            payload.get("runtime_heartbeat_source")
            or ("job_recovery_daemon" if explicit_job_id else "shared_recovery_daemon")
        ).strip()
        interval_seconds = max(
            0,
            _coerce_int(
                payload.get("runtime_heartbeat_interval_seconds"),
                _env_int("WORKFLOW_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", 60),
            ),
        )
        service_name = str(
            payload.get("runtime_heartbeat_service_name")
            or payload.get("service_name")
            or ("job-recovery-" + explicit_job_id if explicit_job_id else "worker-recovery-daemon")
        ).strip()

        daemon_jobs = {
            str(item.get("job_id") or "").strip(): dict(item)
            for item in list(daemon_summary.get("jobs") or [])
            if str(item.get("job_id") or "").strip()
        }
        workflow_resume_by_job = {
            str(item.get("job_id") or "").strip(): dict(item)
            for item in workflow_resume
            if str(item.get("job_id") or "").strip()
        }
        reconcile_by_job = {
            str(item.get("job_id") or "").strip(): dict(item)
            for item in post_completion_reconcile
            if str(item.get("job_id") or "").strip()
        }
        target_job_ids: list[str] = []
        for candidate in [
            explicit_job_id,
            *daemon_jobs.keys(),
            *workflow_resume_by_job.keys(),
            *reconcile_by_job.keys(),
        ]:
            normalized = str(candidate or "").strip()
            if not normalized or normalized in target_job_ids:
                continue
            target_job_ids.append(normalized)

        emitted: list[dict[str, Any]] = []
        for job_id in target_job_ids:
            job = self.store.get_job(job_id)
            if job is None or str(job.get("job_type") or "") != "workflow":
                continue
            runtime_controls = self._build_live_runtime_controls_payload(job)
            workers = self.agent_runtime.list_workers(job_id=job_id)
            worker_summary = _job_worker_summary(workers)
            runtime_health = _classify_job_runtime_health(
                job=job,
                workers=workers,
                worker_summary=worker_summary,
                runtime_controls=runtime_controls,
                blocked_task=str(dict(job.get("summary") or {}).get("blocked_task") or ""),
            )
            heartbeat_payload = {
                "source": source,
                "service_name": service_name,
                "job_status": str(job.get("status") or ""),
                "job_stage": str(job.get("stage") or ""),
                "job_updated_at": str(job.get("updated_at") or ""),
                "blocked_task": str(dict(job.get("summary") or {}).get("blocked_task") or ""),
                "runtime_health": runtime_health,
                "worker_counts": {
                    "completed": int(worker_summary["by_status"].get("completed") or 0),
                    "running": int(worker_summary["by_status"].get("running") or 0),
                    "queued": int(worker_summary["by_status"].get("queued") or 0),
                    "waiting_remote_search": int(worker_summary["by_status"].get("waiting_remote_search") or 0),
                    "waiting_remote_harvest": int(worker_summary["by_status"].get("waiting_remote_harvest") or 0),
                    "blocked": int(worker_summary["by_status"].get("blocked") or 0),
                    "failed": int(worker_summary["by_status"].get("failed") or 0),
                },
                "runtime_controls": {
                    key: _runtime_control_event_snapshot(key, runtime_controls.get(key))
                    for key in (
                        "hosted_runtime_watchdog",
                        "shared_recovery",
                        "job_recovery",
                        "workflow_runner",
                        "workflow_runner_control",
                    )
                    if _runtime_control_event_snapshot(key, runtime_controls.get(key))
                },
                "daemon_summary": {
                    "recoverable_count": int(daemon_summary.get("recoverable_count") or 0),
                    "claimed_count": int(daemon_summary.get("claimed_count") or 0),
                    "executed_count": int(daemon_summary.get("executed_count") or 0),
                    "job": dict(daemon_jobs.get(job_id) or {}),
                },
            }
            if job_id in workflow_resume_by_job:
                heartbeat_payload["workflow_resume"] = dict(workflow_resume_by_job[job_id])
            if job_id in reconcile_by_job:
                heartbeat_payload["post_completion_reconcile"] = dict(reconcile_by_job[job_id])
            emitted.append(
                self._append_runtime_heartbeat_event_if_due(
                    job_id=job_id,
                    source=source,
                    interval_seconds=interval_seconds,
                    payload=heartbeat_payload,
                )
            )
        return emitted

    def _append_runtime_heartbeat_event_if_due(
        self,
        *,
        job_id: str,
        source: str,
        interval_seconds: int,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        latest_events = self.store.list_job_events(
            job_id,
            stage="runtime_heartbeat",
            limit=20,
            descending=True,
        )
        latest_for_source = next(
            (
                event
                for event in latest_events
                if str(dict(event.get("payload") or {}).get("source") or "").strip() == source
            ),
            None,
        )
        if latest_for_source is not None and interval_seconds > 0:
            latest_created_at = _parse_timestamp(str(latest_for_source.get("created_at") or ""))
            if latest_created_at is not None:
                age_seconds = max(
                    int((datetime.now(timezone.utc) - latest_created_at).total_seconds()),
                    0,
                )
                if age_seconds < interval_seconds:
                    return {
                        "job_id": job_id,
                        "source": source,
                        "status": "skipped",
                        "reason": "heartbeat_interval_not_elapsed",
                        "seconds_until_next": max(interval_seconds - age_seconds, 0),
                    }
        runtime_health = dict(payload.get("runtime_health") or {})
        classification = str(runtime_health.get("classification") or "unknown")
        detail = (
            f"Runtime heartbeat from {source}: {classification} "
            f"(status={payload.get('job_status') or ''}, stage={payload.get('job_stage') or ''})."
        )
        self.store.append_job_event(
            job_id,
            stage="runtime_heartbeat",
            status=str(runtime_health.get("state") or "observed"),
            detail=detail,
            payload=payload,
        )
        return {
            "job_id": job_id,
            "source": source,
            "status": "emitted",
            "classification": classification,
        }

    def write_worker_daemon_systemd_unit(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        service = self._build_worker_daemon_service(payload)
        target_path = Path(
            str(
                payload.get("output_path")
                or (self.catalog.project_root / "configs" / "systemd" / f"{service.service_name}.service")
            )
        )
        written = service.write_systemd_unit(
            project_root=self.catalog.project_root,
            output_path=target_path,
            python_bin=str(payload.get("python_bin") or "/usr/bin/env python3"),
            user_name=str(payload.get("user_name") or ""),
        )
        return {
            "status": "written",
            "service_name": service.service_name,
            "output_path": str(written),
            "unit": render_systemd_unit(
                project_root=self.catalog.project_root,
                service_name=service.service_name,
                poll_seconds=float(payload.get("poll_seconds") or 5.0),
                lease_seconds=int(payload.get("lease_seconds") or 300),
                stale_after_seconds=int(payload.get("stale_after_seconds") or 180),
                total_limit=int(payload.get("total_limit") or 4),
                python_bin=str(payload.get("python_bin") or "/usr/bin/env python3"),
                user_name=str(payload.get("user_name") or ""),
            ),
        }

    def _merge_worker_recovery_daemon_summaries(
        self,
        base: dict[str, Any],
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(base or {})
        extra_summary = dict(extra or {})
        if not merged:
            return extra_summary
        if not extra_summary:
            return merged

        merged["owner_id"] = str(merged.get("owner_id") or extra_summary.get("owner_id") or "").strip()
        merged["job_id"] = str(merged.get("job_id") or extra_summary.get("job_id") or "").strip()
        merged["recoverable_count"] = int(extra_summary.get("recoverable_count") or 0)
        merged["claimed_count"] = int(merged.get("claimed_count") or 0) + int(extra_summary.get("claimed_count") or 0)
        merged["executed_count"] = int(merged.get("executed_count") or 0) + int(extra_summary.get("executed_count") or 0)

        merged_jobs_by_id: dict[str, dict[str, Any]] = {}
        for job_summary in list(merged.get("jobs") or []):
            job_id = str(dict(job_summary or {}).get("job_id") or "").strip()
            if not job_id:
                continue
            merged_jobs_by_id[job_id] = dict(job_summary)
        for job_summary in list(extra_summary.get("jobs") or []):
            normalized = dict(job_summary or {})
            job_id = str(normalized.get("job_id") or "").strip()
            if not job_id:
                continue
            existing = dict(merged_jobs_by_id.get(job_id) or {})
            if not existing:
                merged_jobs_by_id[job_id] = normalized
                continue
            retried = list(existing.get("retried") or [])
            for item in list(normalized.get("retried") or []):
                if item not in retried:
                    retried.append(item)
            merged_jobs_by_id[job_id] = {
                **existing,
                **normalized,
                "claimed_count": int(existing.get("claimed_count") or 0) + int(normalized.get("claimed_count") or 0),
                "executed_count": int(existing.get("executed_count") or 0) + int(normalized.get("executed_count") or 0),
                "retried": retried,
            }
        merged["jobs"] = [
            merged_jobs_by_id[job_id]
            for job_id in sorted(merged_jobs_by_id)
        ]
        return merged

    def _build_worker_recovery_daemon(self, payload: dict[str, Any]) -> PersistentWorkerRecoveryDaemon:
        owner_id = str(payload.get("owner_id") or f"recovery-daemon-{uuid.uuid4().hex[:8]}")
        stale_after_raw = payload.get("stale_after_seconds")
        stale_after_seconds = _coerce_int(stale_after_raw, 180)
        return PersistentWorkerRecoveryDaemon(
            store=self.store,
            agent_runtime=self.agent_runtime,
            acquisition_engine=self.acquisition_engine,
            owner_id=owner_id,
            completion_callback=self._handle_completed_recovery_worker_result,
            lease_seconds=int(payload.get("lease_seconds") or 300),
            stale_after_seconds=stale_after_seconds,
            total_limit=int(payload.get("total_limit") or 4),
            job_id=str(payload.get("job_id") or ""),
        )

    def _build_worker_daemon_service(self, payload: dict[str, Any]) -> WorkerDaemonService:
        callback_payload: dict[str, Any] = {}
        job_id = str(payload.get("job_id") or "").strip()
        if job_id:
            callback_payload["job_id"] = job_id
        for key in (
            "workflow_auto_resume_enabled",
            "workflow_resume_explicit_job",
            "workflow_resume_stale_after_seconds",
            "workflow_resume_limit",
            "workflow_queue_auto_takeover_enabled",
            "workflow_queue_resume_stale_after_seconds",
            "workflow_queue_resume_limit",
            "workflow_stale_scope_job_id",
            "runtime_heartbeat_source",
            "runtime_heartbeat_interval_seconds",
            "runtime_heartbeat_service_name",
        ):
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None or value == "":
                continue
            callback_payload[key] = value
        if "runtime_heartbeat_source" not in callback_payload:
            callback_payload["runtime_heartbeat_source"] = "job_recovery_daemon" if job_id else "shared_recovery_daemon"
        if "runtime_heartbeat_service_name" not in callback_payload:
            callback_payload["runtime_heartbeat_service_name"] = str(
                payload.get("service_name") or "worker-recovery-daemon"
            )
        if "runtime_heartbeat_interval_seconds" not in callback_payload:
            callback_payload["runtime_heartbeat_interval_seconds"] = _env_int(
                "WORKFLOW_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", 60
            )
        return WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda daemon_payload: self.run_worker_recovery_once(daemon_payload),
            service_name=str(payload.get("service_name") or "worker-recovery-daemon"),
            owner_id=str(payload.get("owner_id") or ""),
            callback_payload=callback_payload,
            poll_seconds=float(payload.get("poll_seconds") or 5.0),
            lease_seconds=int(payload.get("lease_seconds") or 300),
            stale_after_seconds=int(payload.get("stale_after_seconds") or 180),
            total_limit=int(payload.get("total_limit") or 4),
        )

    def _run_job_scoped_recovery_service(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        config = self._job_scoped_recovery_config(job_id, payload)
        service_ref: dict[str, WorkerDaemonService] = {}
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda daemon_payload: self._run_job_scoped_recovery_tick(
                job_id=job_id,
                daemon_payload=daemon_payload,
                service_ref=service_ref,
            ),
            service_name=str(config["service_name"]),
            owner_id=str(payload.get("owner_id") or ""),
            poll_seconds=float(config["poll_seconds"]),
            lease_seconds=int(config["lease_seconds"]),
            stale_after_seconds=int(config["stale_after_seconds"]),
            total_limit=int(config["total_limit"]),
            callback_payload=self._build_job_scoped_recovery_callback_payload(job_id, payload, config=config),
        )
        service_ref["service"] = service
        return service.run_forever(max_ticks=int(config["max_ticks"]))

    def _recovery_status_fields(
        self,
        *,
        config: dict[str, Any],
        scope: str,
        job_id: str = "",
        poll_seconds: float | None = None,
        max_ticks: int | None = None,
    ) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "service_name": str(config["service_name"]),
            "poll_seconds": float(config["poll_seconds"] if poll_seconds is None else poll_seconds),
            "max_ticks": int(config["max_ticks"] if max_ticks is None else max_ticks),
            "scope": scope,
        }
        if "shared_service_name" in config:
            fields["shared_service_name"] = str(config.get("shared_service_name") or "")
        for key in (
            "stale_after_seconds",
            "total_limit",
            "workflow_resume_stale_after_seconds",
            "workflow_queue_resume_stale_after_seconds",
        ):
            if key in config:
                fields[key] = int(config[key])
        if job_id:
            fields["job_id"] = job_id
        return fields

    def _spawn_recovery_sidecar(
        self,
        *,
        command: list[str],
        service_name: str,
        job_id: str = "",
    ) -> dict[str, Any]:
        log_dir = self.runtime_dir / "service_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{service_name}.log"
        spawned = _spawn_detached_process(
            command=command,
            cwd=self.catalog.project_root,
            log_path=log_path,
            env=_runner_subprocess_env(self.catalog.project_root),
        )
        if job_id:
            return {
                **spawned,
                "job_id": job_id,
                "service_name": service_name,
            }
        return {
            **spawned,
            "service_name": service_name,
        }

    def _start_recovery_thread(
        self,
        *,
        service: WorkerDaemonService,
        config: dict[str, Any],
        scope: str,
        job_id: str = "",
    ) -> dict[str, Any]:
        def _target() -> None:
            try:
                service.run_forever(max_ticks=int(config["max_ticks"]))
            except Exception:
                return

        threading.Thread(
            target=_target,
            name=f"{str(config['service_name'])}-thread",
            daemon=True,
        ).start()
        return {
            "status": "started",
            **self._recovery_status_fields(
                config=config,
                scope=scope,
                job_id=job_id,
            ),
            "mode": "thread_fallback",
        }

    def _start_recovery_sidecar_with_fallback(
        self,
        *,
        payload: dict[str, Any],
        config: dict[str, Any],
        scope: str,
        spawn_sidecar: Callable[[], dict[str, Any]],
        start_thread_fallback: Callable[[], dict[str, Any]],
        job_id: str = "",
    ) -> dict[str, Any]:
        service_name = str(config["service_name"])
        existing_status = read_service_status(self.runtime_dir, service_name)
        if _service_status_is_ready(existing_status):
            return {
                "status": "already_running",
                **self._recovery_status_fields(
                    config=config,
                    scope=scope,
                    job_id=job_id,
                    poll_seconds=float(existing_status.get("poll_seconds") or config["poll_seconds"]),
                    max_ticks=int(existing_status.get("max_ticks") or config["max_ticks"]),
                ),
                "mode": "sidecar",
                "handshake": {
                    "status": "already_running",
                    "service_status": existing_status,
                },
            }

        spawned = spawn_sidecar()
        if str(spawned.get("status") or "") == "started":
            handshake = self._wait_for_recovery_service_ready(
                service_name=service_name,
                pid=int(spawned.get("pid") or 0),
                timeout_seconds=float(config["startup_timeout_seconds"]),
                poll_seconds=float(config["startup_poll_seconds"]),
            )
            started_payload = {
                **spawned,
                **self._recovery_status_fields(config=config, scope=scope, job_id=job_id),
                "mode": "sidecar",
                "handshake": handshake,
            }
            if str(handshake.get("status") or "") == "ready":
                return started_payload
            if str(handshake.get("status") or "") == "timeout_process_alive":
                bootstrap = self.run_worker_recovery_once(
                    self._build_recovery_bootstrap_payload(
                        payload,
                        config=config,
                        scope=scope,
                        job_id=job_id,
                    )
                )
                return {
                    **started_payload,
                    "status": "started_deferred",
                    "bootstrap": bootstrap,
                }
            spawned = {
                **spawned,
                "status": "failed_to_start",
                "handshake": handshake,
            }

        thread_fallback = start_thread_fallback()
        if str(thread_fallback.get("status") or "") == "started":
            return {
                **thread_fallback,
                "fallback_from": spawned,
            }
        return {
            "status": "failed_to_start",
            **self._recovery_status_fields(config=config, scope=scope, job_id=job_id),
            "sidecar": spawned,
            "thread_fallback": thread_fallback,
        }

    def _start_shared_recovery(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not bool(payload.get("auto_job_daemon", False)):
            return {"status": "disabled", "scope": "shared"}
        config = self._shared_recovery_config(payload)
        return self._start_recovery_sidecar_with_fallback(
            payload=payload,
            config=config,
            scope="shared",
            spawn_sidecar=lambda: self._spawn_shared_recovery_sidecar(payload, config=config),
            start_thread_fallback=lambda: self._start_shared_recovery_thread_fallback(payload, config=config),
        )

    def _start_hosted_runtime_watchdog(self, payload: dict[str, Any]) -> dict[str, Any]:
        if not bool(payload.get("auto_job_daemon", False)):
            return {"status": "disabled", "scope": "hosted_runtime_watchdog"}
        config = self._hosted_runtime_watchdog_config(payload)
        return self._start_recovery_sidecar_with_fallback(
            payload=payload,
            config=config,
            scope="hosted_runtime_watchdog",
            spawn_sidecar=lambda: self._spawn_hosted_runtime_watchdog_sidecar(config=config),
            start_thread_fallback=lambda: self._start_hosted_runtime_watchdog_thread_fallback(config=config),
        )

    def _start_job_scoped_recovery(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not bool(payload.get("auto_job_daemon", False)):
            return {"status": "disabled", "job_id": job_id, "scope": "job_scoped"}
        config = self._job_scoped_recovery_config(job_id, payload)
        return self._start_recovery_sidecar_with_fallback(
            payload=payload,
            config=config,
            scope="job_scoped",
            spawn_sidecar=lambda: self._spawn_job_scoped_recovery_sidecar(job_id, payload, config=config),
            start_thread_fallback=lambda: self._start_job_scoped_recovery_thread_fallback(
                job_id, payload, config=config
            ),
            job_id=job_id,
        )

    def _job_scoped_recovery_config(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return _build_job_scoped_recovery_config(job_id, payload)

    def _hosted_runtime_watchdog_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _build_hosted_runtime_watchdog_config(payload)

    def _shared_recovery_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _build_shared_recovery_config(payload)

    def _build_job_scoped_recovery_callback_payload(
        self,
        job_id: str,
        payload: dict[str, Any],
        *,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _build_job_scoped_recovery_callback_payload_impl(job_id, payload, config=config)

    def _build_shared_recovery_callback_payload(
        self,
        payload: dict[str, Any],
        *,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _build_shared_recovery_callback_payload_impl(payload, config=config)

    def _wait_for_recovery_service_ready(
        self,
        *,
        service_name: str,
        pid: int,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> dict[str, Any]:
        return _wait_for_service_ready(
            read_status=lambda: read_service_status(self.runtime_dir, service_name),
            pid=pid,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )

    def _build_recovery_bootstrap_payload(
        self,
        payload: dict[str, Any],
        *,
        config: dict[str, Any],
        scope: str,
        job_id: str = "",
    ) -> dict[str, Any]:
        return _build_recovery_bootstrap_payload_impl(
            payload,
            config=config,
            scope=scope,
            job_id=job_id,
        )

    def _spawn_job_scoped_recovery_sidecar(
        self,
        job_id: str,
        payload: dict[str, Any],
        *,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        service_name = str(config["service_name"])
        command = _build_job_scoped_recovery_command(job_id, config=config, python_executable=sys.executable)
        return self._spawn_recovery_sidecar(command=command, service_name=service_name, job_id=job_id)

    def _spawn_shared_recovery_sidecar(
        self,
        payload: dict[str, Any],
        *,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        service_name = str(config["service_name"])
        command = _build_shared_recovery_command(config=config, python_executable=sys.executable)
        return self._spawn_recovery_sidecar(command=command, service_name=service_name)

    def _spawn_hosted_runtime_watchdog_sidecar(
        self,
        *,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        service_name = str(config["service_name"])
        command = _build_hosted_runtime_watchdog_command(config=config, python_executable=sys.executable)
        return self._spawn_recovery_sidecar(command=command, service_name=service_name)

    def _start_job_scoped_recovery_thread_fallback(
        self,
        job_id: str,
        payload: dict[str, Any],
        *,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        service_ref: dict[str, WorkerDaemonService] = {}
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda daemon_payload: self._run_job_scoped_recovery_tick(
                job_id=job_id,
                daemon_payload=daemon_payload,
                service_ref=service_ref,
            ),
            service_name=str(config["service_name"]),
            poll_seconds=float(config["poll_seconds"]),
            lease_seconds=int(config["lease_seconds"]),
            stale_after_seconds=int(config["stale_after_seconds"]),
            total_limit=int(config["total_limit"]),
            callback_payload=self._build_job_scoped_recovery_callback_payload(job_id, payload, config=config),
        )
        service_ref["service"] = service
        return self._start_recovery_thread(
            service=service,
            config=config,
            scope="job_scoped",
            job_id=job_id,
        )

    def _start_shared_recovery_thread_fallback(
        self,
        payload: dict[str, Any],
        *,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        service = self._build_worker_daemon_service(
            {
                "service_name": str(config["service_name"]),
                "poll_seconds": float(config["poll_seconds"]),
                "lease_seconds": int(config["lease_seconds"]),
                "stale_after_seconds": int(config["stale_after_seconds"]),
                "total_limit": int(config["total_limit"]),
                **self._build_shared_recovery_callback_payload(payload, config=config),
            }
        )
        return self._start_recovery_thread(
            service=service,
            config=config,
            scope="shared",
        )

    def _start_hosted_runtime_watchdog_thread_fallback(
        self,
        *,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda _: self.run_hosted_runtime_watchdog_once(
                {
                    "shared_service_name": str(config["shared_service_name"]),
                    "hosted_runtime_watchdog_service_name": str(config["service_name"]),
                    "hosted_runtime_source": str(config["service_name"]),
                }
            ),
            service_name=str(config["service_name"]),
            poll_seconds=float(config["poll_seconds"]),
            callback_payload={
                "hosted_runtime_source": str(config["service_name"]),
                "shared_service_name": str(config["shared_service_name"]),
            },
        )
        return self._start_recovery_thread(
            service=service,
            config=config,
            scope="hosted_runtime_watchdog",
        )

    def _run_job_scoped_recovery_tick(
        self,
        *,
        job_id: str,
        daemon_payload: dict[str, Any],
        service_ref: dict[str, WorkerDaemonService],
    ) -> dict[str, Any]:
        summary = self.run_worker_recovery_once({**daemon_payload, "job_id": job_id})
        job = self.store.get_job(job_id) or {}
        workers = self.agent_runtime.list_workers(job_id=job_id)
        pending_workers = [
            worker for worker in workers if str(effective_worker_status(worker) or "") not in {"completed", "failed"}
        ]
        if str(job.get("status") or "") in {"completed", "failed"} and not pending_workers:
            service = service_ref.get("service")
            if service is not None:
                service.request_stop()
        return summary

    def healthcheck_model(self) -> dict[str, Any]:
        model_health = self.model_client.healthcheck()
        semantic_health = self.semantic_provider.healthcheck()
        overall = "ready"
        if any(item.get("status") == "degraded" for item in [model_health, semantic_health]):
            overall = "degraded"
        return {
            "status": overall,
            "providers": {
                "model": model_health,
                "semantic": semantic_health,
            },
        }

    def review_plan_session(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_id = int(payload.get("review_id") or payload.get("plan_review_id") or 0)
        action = str(payload.get("action") or payload.get("status") or "").strip().lower()
        session = self.store.get_plan_review_session(review_id)
        if session is None:
            return {"status": "not_found", "review_id": review_id}
        if action in {"approve", "approved"}:
            status = "approved"
        elif action in {"reject", "rejected"}:
            status = "rejected"
        else:
            status = "needs_changes"
        request_payload = dict(session.get("request") or {})
        plan_payload = dict(session.get("plan") or {})
        if status == "approved":
            request_payload, plan_payload = apply_plan_review_decision(
                request_payload,
                plan_payload,
                dict(payload.get("decision") or payload.get("decision_payload") or payload),
            )
        execution_bundle: dict[str, Any] = {}
        try:
            execution_bundle = self._build_execution_bundle(
                request=request_payload,
                plan=plan_payload,
                source=f"plan_review_{status}",
                plan_review_session=session,
            )
        except Exception:
            execution_bundle = {}
        reviewed = self.store.review_plan_session(
            review_id=review_id,
            status=status,
            reviewer=str(payload.get("reviewer") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
            decision_payload=dict(payload.get("decision") or payload.get("decision_payload") or {}),
            request_payload=request_payload,
            plan_payload=plan_payload,
            execution_bundle_payload=execution_bundle,
        )
        if reviewed is None:
            return {"status": "not_found", "review_id": review_id}
        return {"status": "reviewed", "review": reviewed}

    def compile_plan_review_instruction(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_id = int(payload.get("review_id") or payload.get("plan_review_id") or 0)
        instruction = str(payload.get("instruction") or "").strip()
        if review_id <= 0:
            return {"status": "invalid", "reason": "review_id is required"}
        if not instruction:
            return {"status": "invalid", "reason": "instruction is required"}
        session = self.store.get_plan_review_session(review_id)
        if session is None:
            return {"status": "not_found", "review_id": review_id}
        request_payload = dict(session.get("request") or {})
        plan_payload = dict(session.get("plan") or {})
        gate_payload = dict(session.get("gate") or {})
        compiled = compile_review_payload_from_instruction(
            review_id=review_id,
            instruction=instruction,
            reviewer=str(payload.get("reviewer") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
            action=str(payload.get("action") or "approved").strip(),
            target_company=str(request_payload.get("target_company") or ""),
            model_client=self.model_client,
            gate_payload=gate_payload,
            request_payload=request_payload,
            plan_payload=plan_payload,
        )
        return {
            "status": "compiled",
            "review_id": review_id,
            "review_payload": dict(compiled.get("review_payload") or {}),
            "instruction_compiler": dict(compiled.get("instruction_compiler") or {}),
            "intent_rewrite": _build_intent_rewrite_payload(
                request_payload=request_payload,
                instruction=instruction,
            ),
        }

    def list_plan_review_sessions(self, target_company: str = "", status: str = "") -> dict[str, Any]:
        return {
            "plan_reviews": self.store.list_plan_review_sessions(
                target_company=target_company, status=status, limit=100
            )
        }

    def list_query_dispatches(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        limit = 100
        raw_limit = payload.get("limit")
        if raw_limit not in {None, ""}:
            limit = max(1, _coerce_int(raw_limit, 100))
        dispatches = self.store.list_query_dispatches(
            target_company=str(payload.get("target_company") or ""),
            requester_id=str(payload.get("requester_id") or payload.get("user_id") or ""),
            tenant_id=str(payload.get("tenant_id") or payload.get("workspace_id") or ""),
            limit=limit,
        )
        return {"query_dispatches": dispatches}

    def compile_post_acquisition_refinement(self, payload: dict[str, Any]) -> dict[str, Any]:
        baseline_job = self._resolve_baseline_job_for_refinement(payload)
        if isinstance(baseline_job, dict) and baseline_job.get("status") in {"not_found", "invalid"}:
            return baseline_job
        assert isinstance(baseline_job, dict)

        base_request = JobRequest.from_payload(dict(baseline_job.get("request") or {})).to_record()
        instruction = str(payload.get("instruction") or "").strip()
        if instruction:
            compiled = compile_refinement_patch_from_instruction(
                instruction=instruction,
                base_request=base_request,
                model_client=self.model_client,
            )
            request_patch = dict(compiled.get("request_patch") or {})
            merged_request = dict(compiled.get("merged_request") or {})
            instruction_compiler = dict(compiled.get("instruction_compiler") or {})
        else:
            request_patch = normalize_refinement_patch(dict(payload.get("request_patch") or payload.get("patch") or {}))
            if not request_patch:
                return {"status": "invalid", "reason": "instruction or request_patch is required"}
            merged_request = JobRequest.from_payload(apply_refinement_patch(base_request, request_patch)).to_record()
            instruction_compiler = {
                "source": "explicit_patch",
                "provider": "",
                "model_patch": {},
                "deterministic_patch": request_patch,
                "supplemented_keys": [],
                "fallback_used": False,
            }

        request = JobRequest.from_payload(merged_request)
        plan = self._build_augmented_sourcing_plan(request)
        effective_request = _build_effective_retrieval_request(request, plan)
        return {
            "status": "compiled",
            "baseline_job_id": str(baseline_job.get("job_id") or ""),
            "request_patch": request_patch,
            "request": request.to_record(),
            "request_preview": _build_request_preview_payload(
                request=request,
                effective_request=effective_request,
            ),
            "plan": plan.to_record(),
            "instruction_compiler": instruction_compiler,
            "baseline_candidate_source": dict((baseline_job.get("summary") or {}).get("candidate_source") or {}),
            "intent_rewrite": _build_intent_rewrite_payload(
                request_payload=request.to_record(),
                instruction=instruction if instruction else None,
            ),
        }

    def apply_post_acquisition_refinement(self, payload: dict[str, Any]) -> dict[str, Any]:
        compiled = self.compile_post_acquisition_refinement(payload)
        if compiled.get("status") != "compiled":
            return compiled

        baseline_job_id = str(compiled.get("baseline_job_id") or "")
        baseline_job = self.store.get_job(baseline_job_id) if baseline_job_id else None
        if baseline_job is None:
            return {"status": "not_found", "reason": "baseline job not found", "baseline_job_id": baseline_job_id}

        request = JobRequest.from_payload(dict(compiled.get("request") or {}))
        plan_payload = dict(compiled.get("plan") or {})
        candidate_source = dict(compiled.get("baseline_candidate_source") or {})
        criteria_artifacts = self._persist_criteria_artifacts(
            request=request,
            plan_payload=plan_payload,
            source_kind="post_acquisition_refinement",
            compiler_kind="refinement_instruction",
            job_id="",
        )
        runtime_policy = {
            "mode": "post_acquisition_refinement",
            "baseline_job_id": baseline_job_id,
            "baseline_candidate_source": candidate_source,
        }
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        if snapshot_id:
            runtime_policy["workflow_snapshot_id"] = snapshot_id
        artifact = self._run_retrieval_job(
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            job_type="retrieval_refinement",
            criteria_artifacts=criteria_artifacts,
            runtime_policy=runtime_policy,
            event_detail="Retrieval refinement started from an existing acquired roster.",
        )
        rerun_job_id = str(artifact.get("job_id") or "")
        baseline_results = self.store.get_job_results(baseline_job_id)
        rerun_results = self.store.get_job_results(rerun_job_id)
        baseline_version = _criteria_like_version_from_job(
            baseline_job,
            patterns=self.store.list_criteria_patterns(target_company=request.target_company, status=""),
        )
        rerun_version = self.store.get_criteria_version(int(criteria_artifacts.get("criteria_version_id") or 0))
        diff = build_result_diff(
            baseline_results,
            rerun_results,
            baseline_job_id=baseline_job_id,
            rerun_job_id=rerun_job_id,
            baseline_version=baseline_version,
            rerun_version=rerun_version,
            trigger_feedback={
                "feedback_type": "post_acquisition_refinement",
                "subject": "instruction",
                "value": str(payload.get("instruction") or ""),
            },
        )
        diff_artifact = {
            "target_company": request.target_company,
            "baseline_job_id": baseline_job_id,
            "rerun_job_id": rerun_job_id,
            "request_patch": dict(compiled.get("request_patch") or {}),
            "instruction_compiler": dict(compiled.get("instruction_compiler") or {}),
            "diff": diff,
        }
        diff_artifact_path = self.jobs_dir / f"criteria_diff_refinement_{baseline_job_id or 'none'}_{rerun_job_id}.json"
        diff_artifact_path.write_text(json.dumps(diff_artifact, ensure_ascii=False, indent=2))
        stored = self.store.record_criteria_result_diff(
            target_company=request.target_company,
            trigger_feedback_id=0,
            criteria_version_id=int(criteria_artifacts.get("criteria_version_id") or 0),
            baseline_job_id=baseline_job_id,
            rerun_job_id=rerun_job_id,
            summary_payload=dict(diff.get("summary") or {}),
            diff_payload=diff,
            artifact_path=str(diff_artifact_path),
        )
        return {
            "status": "completed",
            "baseline_job_id": baseline_job_id,
            "rerun_job_id": rerun_job_id,
            "request_patch": dict(compiled.get("request_patch") or {}),
            "request": request.to_record(),
            "request_preview": dict(compiled.get("request_preview") or {}),
            "plan": plan_payload,
            "instruction_compiler": dict(compiled.get("instruction_compiler") or {}),
            "intent_rewrite": dict(compiled.get("intent_rewrite") or {}),
            "diff_id": int(stored.get("diff_id") or 0),
            "diff_artifact_path": str(diff_artifact_path),
            "diff": diff,
            "rerun_result": artifact,
        }

    def _resolve_baseline_job_for_refinement(self, payload: dict[str, Any]) -> dict[str, Any]:
        baseline_job_id = str(payload.get("job_id") or payload.get("baseline_job_id") or "").strip()
        if not baseline_job_id:
            return {"status": "invalid", "reason": "job_id is required"}
        baseline_job = self.store.get_job(baseline_job_id)
        if baseline_job is None:
            return {"status": "not_found", "baseline_job_id": baseline_job_id}
        if str(baseline_job.get("status") or "").strip() != "completed":
            return {
                "status": "invalid",
                "reason": "baseline job must be completed before refinement",
                "baseline_job_id": baseline_job_id,
            }
        return baseline_job

    def list_manual_review_items(
        self, target_company: str = "", job_id: str = "", status: str = "open"
    ) -> dict[str, Any]:
        return {
            "manual_review_items": self.store.list_manual_review_items(
                target_company=target_company,
                job_id=job_id,
                status=status,
                limit=200,
            )
        }

    def list_candidate_review_records(
        self,
        *,
        job_id: str = "",
        history_id: str = "",
        candidate_id: str = "",
        status: str = "",
    ) -> dict[str, Any]:
        return {
            "candidate_review_records": self._sanitize_candidate_contact_records_for_api(
                self.store.list_candidate_review_records(
                    job_id=job_id,
                    history_id=history_id,
                    candidate_id=candidate_id,
                    status=status,
                    limit=500,
                )
            )
        }

    def upsert_candidate_review_record(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = self.store.upsert_candidate_review_record(payload)
        return {
            "status": "upserted",
            "candidate_review_record": record,
        }

    def list_target_candidates(
        self,
        *,
        job_id: str = "",
        history_id: str = "",
        candidate_id: str = "",
        follow_up_status: str = "",
    ) -> dict[str, Any]:
        return {
            "target_candidates": self._sanitize_candidate_contact_records_for_api(
                self.store.list_target_candidates(
                    job_id=job_id,
                    history_id=history_id,
                    candidate_id=candidate_id,
                    follow_up_status=follow_up_status,
                    limit=500,
                )
            )
        }

    def upsert_target_candidate(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = self.store.upsert_target_candidate(payload)
        return {
            "status": "upserted",
            "target_candidate": record,
        }

    def import_target_candidates_from_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = str(payload.get("job_id") or "").strip()
        if not job_id:
            return {"status": "invalid", "reason": "job_id is required"}
        job = self.store.get_job(job_id)
        if job is None:
            return {"status": "not_found", "reason": "job not found", "job_id": job_id}
        history_id = str(payload.get("history_id") or "").strip()
        follow_up_status = str(payload.get("follow_up_status") or "pending_outreach").strip() or "pending_outreach"
        limit = min(max(_coerce_int(payload.get("limit"), 2000), 1), 5000)
        imported: list[dict[str, Any]] = []
        offset = 0
        while len(imported) < limit:
            page = self.get_job_candidate_page(
                job_id,
                offset=offset,
                limit=min(250, limit - len(imported)),
                lightweight=True,
            )
            if page is None:
                return {"status": "not_found", "reason": "job not found", "job_id": job_id}
            candidates = [dict(item) for item in list(page.get("candidates") or []) if isinstance(item, dict)]
            if not candidates:
                break
            for candidate in candidates:
                candidate_id = str(candidate.get("id") or candidate.get("candidate_id") or "").strip()
                linkedin_url = str(candidate.get("linkedin_url") or candidate.get("profile_url") or "").strip()
                candidate_name = str(
                    candidate.get("name") or candidate.get("display_name") or candidate.get("candidate_name") or ""
                ).strip()
                if not candidate_id and not linkedin_url and not candidate_name:
                    continue
                record = self.store.upsert_target_candidate(
                    {
                        "candidate_id": candidate_id,
                        "history_id": history_id,
                        "job_id": job_id,
                        "candidate_name": candidate_name,
                        "headline": str(candidate.get("headline") or candidate.get("title") or "").strip(),
                        "current_company": str(
                            candidate.get("current_company")
                            or candidate.get("organization")
                            or candidate.get("company")
                            or ""
                        ).strip(),
                        "avatar_url": str(candidate.get("avatar_url") or candidate.get("photo_url") or "").strip(),
                        "linkedin_url": linkedin_url,
                        "primary_email": str(candidate.get("primary_email") or "").strip(),
                        "follow_up_status": follow_up_status,
                        "quality_score": candidate.get("score"),
                        "metadata": {
                            "source": "job_candidate_import",
                            "job_id": job_id,
                            "history_id": history_id,
                            "result_mode": str(page.get("result_mode") or ""),
                            "excel_intake": bool(
                                str(dict(job.get("summary") or {}).get("workflow_kind") or "").strip() == "excel_intake"
                            ),
                            "candidate": candidate,
                        },
                    }
                )
                imported.append(record)
                if len(imported) >= limit:
                    break
            if not bool(page.get("has_more")):
                break
            next_offset = page.get("next_offset")
            offset = _coerce_int(next_offset, offset + len(candidates))
        return {
            "status": "imported",
            "job_id": job_id,
            "history_id": history_id,
            "imported_count": len(imported),
            "target_candidates": self._sanitize_candidate_contact_records_for_api(imported),
        }

    def list_asset_default_pointers(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        query = dict(payload or {})
        return {
            "asset_default_pointers": self.store.list_asset_default_pointers(
                company_key=str(query.get("company_key") or query.get("target_company") or ""),
                scope_kind=str(query.get("scope_kind") or ""),
                asset_kind=str(query.get("asset_kind") or ""),
                limit=min(max(_coerce_int(query.get("limit"), 500), 1), 1000),
            )
        }

    def promote_asset_default_pointer(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self.store.promote_asset_default_pointer(payload)
        status = str(result.get("status") or "").strip()
        if status == "promoted":
            return {
                "status": "promoted",
                "asset_default_pointer": dict(result.get("default_pointer") or {}),
                "replacement_plan": {
                    key: value
                    for key, value in result.items()
                    if key not in {"default_pointer", "history"}
                },
                "history": list(result.get("history") or []),
            }
        return result

    def export_target_candidates_archive(self, payload: dict[str, Any]) -> dict[str, Any]:
        requested_record_ids = [
            str(item or "").strip()
            for item in list(payload.get("record_ids") or [])
            if str(item or "").strip()
        ]
        try:
            requested_limit = min(max(int(payload.get("limit") or 500), 1), 2000)
        except (TypeError, ValueError):
            return {
                "status": "invalid",
                "reason": "limit must be an integer",
            }
        records: list[dict[str, Any]] = []
        if requested_record_ids:
            seen_record_ids: set[str] = set()
            for record_id in requested_record_ids:
                if record_id in seen_record_ids:
                    continue
                seen_record_ids.add(record_id)
                record = self.store.get_target_candidate(record_id)
                if record is not None:
                    records.append(record)
        else:
            records = self.store.list_target_candidates(
                job_id=str(payload.get("job_id") or ""),
                history_id=str(payload.get("history_id") or ""),
                candidate_id=str(payload.get("candidate_id") or ""),
                follow_up_status=str(payload.get("follow_up_status") or ""),
                limit=requested_limit,
            )
        if not records:
            return {
                "status": "not_found",
                "reason": "no target candidates matched the requested export scope",
            }

        base_name = f"target-candidates-{_china_local_filename_timestamp()}"
        csv_rows: list[dict[str, str]] = []
        profile_inventory: list[dict[str, Any]] = []
        written_profile_paths: set[str] = set()
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for ordinal, record in enumerate(records, start=1):
                export_item = self._build_target_candidate_export_item(record, ordinal=ordinal)
                csv_rows.append(dict(export_item.get("csv_row") or {}))
                inventory_entry = dict(export_item.get("profile_inventory") or {})
                archive_path = str(export_item.get("profile_archive_path") or "").strip()
                profile_bytes = export_item.get("profile_bytes")
                if archive_path and isinstance(profile_bytes, bytes) and archive_path not in written_profile_paths:
                    archive.writestr(f"{base_name}/{archive_path}", profile_bytes)
                    written_profile_paths.add(archive_path)
                if inventory_entry:
                    profile_inventory.append(inventory_entry)
            archive.writestr(
                f"{base_name}/target_candidates_summary.csv",
                self._build_target_candidate_export_csv(csv_rows),
            )
            archive.writestr(
                f"{base_name}/profiles/profile_inventory.json",
                json.dumps(
                    {
                        "generated_at": _china_now_iso(),
                        "record_count": len(records),
                        "packaged_profile_count": len(
                            [
                                entry
                                for entry in profile_inventory
                                if str(entry.get("status") or "").strip() == "packaged"
                            ]
                        ),
                        "profiles": profile_inventory,
                    },
                    ensure_ascii=False,
                    indent=2,
                ).encode("utf-8"),
            )
        return {
            "status": "ok",
            "filename": f"{base_name}.zip",
            "content_type": "application/zip",
            "body": archive_buffer.getvalue(),
            "record_count": len(records),
            "packaged_profile_count": len(
                [
                    entry
                    for entry in profile_inventory
                    if str(entry.get("status") or "").strip() == "packaged"
                ]
            ),
        }

    def _build_target_candidate_export_item(self, record: dict[str, Any], *, ordinal: int) -> dict[str, Any]:
        payload = dict(record or {})
        record_metadata = dict(payload.get("metadata") or {})
        candidate_id = str(payload.get("candidate_id") or "").strip()
        job_id = str(payload.get("job_id") or "").strip()
        detail_payload = self.get_job_candidate_detail(job_id, candidate_id) if job_id and candidate_id else None
        candidate_payload = dict(detail_payload.get("candidate") or {}) if isinstance(detail_payload, dict) else {}
        result_payload = dict(detail_payload.get("result") or {}) if isinstance(detail_payload, dict) else {}
        if not candidate_payload and candidate_id:
            candidate = self.store.get_candidate(candidate_id)
            if candidate is not None:
                candidate_payload = candidate.to_record()

        combined_metadata: dict[str, Any] = {}
        for source in (
            candidate_payload.get("metadata"),
            result_payload.get("metadata"),
            record_metadata,
        ):
            if isinstance(source, dict):
                combined_metadata.update(dict(source))

        combined_payload = dict(candidate_payload)
        for source in (result_payload, payload):
            if not isinstance(source, dict):
                continue
            for field_name in (
                "candidate_id",
                "display_name",
                "name_en",
                "name_zh",
                "employment_status",
                "headline",
                "linkedin_url",
                "primary_email",
                "primary_email_metadata",
                "outreach_layer",
                "outreach_layer_key",
                "outreach_layer_source",
                "profile_location",
                "public_identifier",
                "source_path",
                "work_history",
                "education",
                "experience_lines",
                "education_lines",
            ):
                value = source.get(field_name)
                if field_name == "primary_email_metadata":
                    if value and field_name not in combined_payload:
                        combined_payload[field_name] = value
                    continue
                if value not in (None, "", [], {}) and combined_payload.get(field_name) in (None, "", [], {}):
                    combined_payload[field_name] = value
        combined_payload["metadata"] = combined_metadata
        combined_payload["source_path"] = _first_non_empty_text(
            combined_payload.get("source_path"),
            result_payload.get("source_path"),
            combined_metadata.get("source_path"),
        )
        timeline = self._resolve_candidate_profile_timeline(
            payload=combined_payload,
            metadata=combined_metadata,
            source_path=str(combined_payload.get("source_path") or ""),
            load_profile_timeline=True,
        )
        linkedin_url = _first_non_empty_text(
            combined_payload.get("linkedin_url"),
            result_payload.get("linkedin_url"),
            payload.get("linkedin_url"),
        )
        registry_row = self.store.get_linkedin_profile_registry(linkedin_url) if linkedin_url else None
        export_name = _first_non_empty_text(
            combined_payload.get("display_name"),
            combined_payload.get("name_en"),
            payload.get("candidate_name"),
            candidate_id,
            f"candidate-{ordinal}",
        )
        experience_lines = _normalized_text_lines(
            timeline.get("experience_lines")
            or combined_payload.get("experience_lines")
            or combined_payload.get("work_history")
        )
        education_lines = _normalized_text_lines(
            timeline.get("education_lines")
            or combined_payload.get("education_lines")
            or combined_payload.get("education")
        )
        exportable_email = self._target_candidate_exportable_primary_email(
            _first_non_empty_text(
                combined_payload.get("primary_email"),
                payload.get("primary_email"),
            ),
            _normalized_primary_email_metadata(
                combined_payload.get("primary_email_metadata")
                or combined_metadata.get("primary_email_metadata")
            ),
        )
        profile_source_path = self._resolve_target_candidate_export_profile_source_path(
            timeline=timeline,
            registry_row=registry_row,
            fallback_source_path=str(combined_payload.get("source_path") or ""),
        )
        profile_archive_path = ""
        profile_bytes: bytes | None = None
        profile_inventory = {
            "record_id": str(payload.get("record_id") or payload.get("id") or "").strip(),
            "candidate_id": candidate_id,
            "candidate_name": export_name,
            "linkedin_url": linkedin_url,
            "status": "missing",
            "archive_path": "",
            "source_path": str(profile_source_path or ""),
        }
        if profile_source_path is not None:
            try:
                profile_bytes = profile_source_path.read_bytes()
            except OSError:
                profile_bytes = None
            if profile_bytes is not None:
                profile_archive_path = self._target_candidate_profile_archive_path(
                    candidate_name=export_name,
                    linkedin_url=linkedin_url,
                    public_identifier=_first_non_empty_text(
                        timeline.get("public_identifier"),
                        combined_payload.get("public_identifier"),
                        dict(registry_row or {}).get("sanity_linkedin_url"),
                    ),
                    source_path=profile_source_path,
                )
                profile_inventory.update(
                    {
                        "status": "packaged",
                        "archive_path": profile_archive_path,
                    }
                )

        outreach_layer_key = _first_non_empty_text(
            combined_payload.get("outreach_layer_key"),
            result_payload.get("outreach_layer_key"),
        )
        csv_row = {
            "姓名": export_name,
            "LinkedIn 链接": linkedin_url,
            "华人线索信息分层结果": self._target_candidate_outreach_layer_label(
                outreach_layer_key=outreach_layer_key,
                outreach_layer=combined_payload.get("outreach_layer") or result_payload.get("outreach_layer"),
            ),
            "在职状态": self._target_candidate_employment_status_label(
                _first_non_empty_text(
                    combined_payload.get("employment_status"),
                    result_payload.get("employment_status"),
                )
            ),
            "工作经历": " | ".join(experience_lines),
            "教育经历": " | ".join(education_lines),
            "地区": _first_non_empty_text(
                timeline.get("profile_location"),
                combined_payload.get("profile_location"),
                combined_payload.get("location"),
            ),
            "Email": exportable_email,
        }
        return {
            "csv_row": csv_row,
            "profile_archive_path": profile_archive_path,
            "profile_bytes": profile_bytes,
            "profile_inventory": profile_inventory,
        }

    def _build_target_candidate_export_csv(self, rows: list[dict[str, str]]) -> bytes:
        output = io.StringIO(newline="")
        writer = csv.DictWriter(
            output,
            fieldnames=[
                "姓名",
                "LinkedIn 链接",
                "华人线索信息分层结果",
                "在职状态",
                "工作经历",
                "教育经历",
                "地区",
                "Email",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({key: str(row.get(key) or "") for key in writer.fieldnames})
        return output.getvalue().encode("utf-8-sig")

    def _target_candidate_exportable_primary_email(
        self,
        primary_email: str,
        metadata: dict[str, Any] | None,
    ) -> str:
        email = str(primary_email or "").strip()
        normalized_metadata = _normalized_primary_email_metadata(metadata)
        if not email:
            return ""
        if (
            str(normalized_metadata.get("source") or "").strip().lower() == "harvestapi"
            and normalized_metadata.get("foundInLinkedInProfile") is not True
        ):
            return ""
        quality_score = normalized_metadata.get("qualityScore")
        if isinstance(quality_score, int) and quality_score < 80:
            return ""
        if str(normalized_metadata.get("status") or "").strip().lower() in {
            "low_confidence",
            "suppressed",
            "invalid",
            "rejected",
        }:
            return ""
        return email

    def _target_candidate_outreach_layer_label(self, *, outreach_layer_key: str, outreach_layer: Any) -> str:
        normalized_key = str(outreach_layer_key or "").strip()
        if not normalized_key:
            try:
                normalized_key = OUTREACH_LAYER_KEY_BY_INDEX.get(int(outreach_layer or 0), "")
            except (TypeError, ValueError):
                normalized_key = ""
        labels = {
            "layer_0_roster": "Layer 0 · 全量候选人",
            "layer_1_name_signal": "Layer 1 · 中文姓名信号",
            "layer_2_greater_china_region_experience": "Layer 2 · 泛华人地区经历",
            "layer_3_mainland_china_experience_or_chinese_language": "Layer 3 · 大陆经历/中文信号",
        }
        return labels.get(normalized_key, normalized_key)

    def _target_candidate_employment_status_label(self, employment_status: str) -> str:
        normalized = str(employment_status or "").strip().lower()
        if normalized == "current":
            return "在职"
        if normalized == "former":
            return "已离职"
        if normalized == "lead":
            return "线索"
        return str(employment_status or "").strip()

    def _resolve_target_candidate_export_profile_source_path(
        self,
        *,
        timeline: dict[str, Any],
        registry_row: dict[str, Any] | None,
        fallback_source_path: str,
    ) -> Path | None:
        candidate_paths: list[tuple[str, bool]] = [
            (str(timeline.get("profile_capture_source_path") or "").strip(), True),
            (str(dict(registry_row or {}).get("last_raw_path") or "").strip(), True),
            (str(fallback_source_path or "").strip(), False),
        ]
        for raw_path, trusted in candidate_paths:
            if not raw_path:
                continue
            if not trusted and not _target_candidate_export_path_looks_like_profile(raw_path):
                continue
            resolved = self._resolve_export_file_path(raw_path)
            if resolved is not None:
                return resolved
        return None

    def _resolve_export_file_path(self, raw_path: str) -> Path | None:
        normalized = str(raw_path or "").strip()
        if not normalized:
            return None
        candidates = [Path(normalized)]
        if not Path(normalized).is_absolute():
            candidates.extend(
                [
                    self.runtime_dir / normalized,
                    self.catalog.project_root / normalized,
                ]
            )
        for candidate in candidates:
            try:
                if candidate.is_file():
                    return candidate
            except OSError:
                continue
        return None

    def _target_candidate_profile_archive_path(
        self,
        *,
        candidate_name: str,
        linkedin_url: str,
        public_identifier: str,
        source_path: Path,
    ) -> str:
        slug = _target_candidate_archive_slug_from_linkedin(public_identifier or linkedin_url)
        name_component = _target_candidate_archive_name_component(candidate_name, fallback="candidate")
        slug_component = _target_candidate_archive_name_component(slug, fallback="profile")
        suffix = source_path.suffix if source_path.suffix else ".json"
        return f"profiles/{name_component}__{slug_component}{suffix}"

    def synthesize_manual_review_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_item_id = int(payload.get("review_item_id") or 0)
        if review_item_id <= 0:
            return {"status": "invalid", "reason": "review_item_id is required"}
        review_item = self.store.get_manual_review_item(review_item_id)
        if review_item is None:
            return {"status": "not_found", "review_item_id": review_item_id}
        metadata = dict(review_item.get("metadata") or {})
        if metadata.get("manual_review_synthesis") and not bool(payload.get("force_refresh")):
            return {
                "status": "cached",
                "review_item_id": review_item_id,
                "manual_review_item": review_item,
                "synthesis": dict(metadata.get("manual_review_synthesis") or {}),
                "synthesis_compiler": dict(metadata.get("manual_review_synthesis_compiler") or {}),
            }

        compiled = compile_manual_review_synthesis(
            review_item,
            model_client=self.model_client,
        )
        updated = self.store.merge_manual_review_item_metadata(
            review_item_id,
            {
                "manual_review_synthesis": dict(compiled.get("synthesis") or {}),
                "manual_review_synthesis_compiler": dict(compiled.get("synthesis_compiler") or {}),
            },
        )
        return {
            "status": "compiled",
            "review_item_id": review_item_id,
            "manual_review_item": updated or review_item,
            "synthesis": dict(compiled.get("synthesis") or {}),
            "synthesis_compiler": dict(compiled.get("synthesis_compiler") or {}),
        }

    def review_manual_review_item(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_item_id = int(payload.get("review_item_id") or 0)
        review_item = self.store.get_manual_review_item(review_item_id) if review_item_id > 0 else None
        resolution: dict[str, Any] | None = None
        if _has_manual_review_resolution_payload(payload):
            resolution = apply_manual_review_resolution(
                runtime_dir=self.jobs_dir.parent,
                store=self.store,
                payload=payload,
                review_item=review_item,
                model_client=self.model_client,
            )
            if resolution.get("status") == "candidate_not_found":
                return resolution
        if review_item_id <= 0:
            if resolution is None:
                return {"status": "invalid", "reason": "review_item_id or manual review resolution payload is required"}
            return {"status": "applied_without_queue", "resolution": resolution}
        item = self.store.review_manual_review_item(
            review_item_id=review_item_id,
            action=str(payload.get("action") or payload.get("status") or "").strip(),
            reviewer=str(payload.get("reviewer") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
            candidate_payload=dict((resolution or {}).get("candidate") or {}),
            evidence_payload=list((resolution or {}).get("evidence") or []),
            metadata_merge={
                **dict(payload.get("metadata_merge") or {}),
                **dict((resolution or {}).get("metadata") or {}),
            },
        )
        if item is None:
            return {"status": "not_found", "review_item_id": review_item_id}
        response = {"status": "reviewed", "manual_review_item": item}
        if resolution is not None:
            response["resolution"] = resolution
        return response

    def record_criteria_feedback(self, payload: dict[str, Any]) -> dict[str, Any]:
        feedback = self.store.record_criteria_feedback(payload)
        suggestions = self._suggest_patterns_from_feedback(int(feedback.get("feedback_id") or 0))
        recompile = self.criteria_evolution.recompile_after_feedback(payload, int(feedback["feedback_id"]))
        rerun = self._rerun_after_recompile_if_requested(payload, feedback, recompile)
        return {
            "status": "recorded",
            "feedback": feedback,
            "suggestions": suggestions,
            "recompile": recompile,
            "rerun": rerun,
        }

    def recompile_criteria(self, payload: dict[str, Any]) -> dict[str, Any]:
        trigger_feedback_id = int(payload.get("trigger_feedback_id") or 0)
        recompile = self.criteria_evolution.recompile_after_feedback(payload, trigger_feedback_id)
        rerun = self._rerun_after_recompile_if_requested(payload, {"feedback_id": trigger_feedback_id}, recompile)
        return {**recompile, "rerun": rerun}

    def configure_confidence_policy(self, payload: dict[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action") or "").strip().lower()
        request_payload = dict(payload.get("request") or payload.get("request_payload") or {})
        if request_payload:
            request_payload = JobRequest.from_payload(request_payload).to_record()
        target_company = str(payload.get("target_company") or request_payload.get("target_company") or "").strip()
        scope_kind = str(payload.get("scope_kind") or "request_family").strip() or "request_family"
        reviewer = str(payload.get("reviewer") or "").strip()
        notes = str(payload.get("notes") or "").strip()
        if not target_company:
            return {"status": "invalid", "reason": "target_company is required"}
        if action in {"clear", "disable", "deactivate"}:
            result = self.store.deactivate_confidence_policy_control(
                control_id=int(payload.get("control_id") or 0),
                target_company=target_company,
                request_payload=request_payload,
                scope_kind=scope_kind,
            )
            return {"status": "cleared", **result}

        if action in {"freeze", "freeze_current"}:
            feedback_items = self.store.list_criteria_feedback(target_company=target_company, limit=500)
            auto_policy = build_confidence_policy(
                target_company=target_company,
                feedback_items=feedback_items,
                request_payload=request_payload,
            )
            control = self.store.create_confidence_policy_control(
                target_company=target_company,
                request_payload=request_payload,
                scope_kind=scope_kind,
                control_mode="freeze",
                high_threshold=float(auto_policy.get("high_threshold") or 0.75),
                medium_threshold=float(auto_policy.get("medium_threshold") or 0.45),
                reviewer=reviewer,
                notes=notes,
                locked_policy=auto_policy,
            )
            return {"status": "configured", "control": control, "source_policy": auto_policy}

        if action in {"override", "set"}:
            control = self.store.create_confidence_policy_control(
                target_company=target_company,
                request_payload=request_payload,
                scope_kind=scope_kind,
                control_mode="override",
                high_threshold=float(payload.get("high_threshold") or 0.75),
                medium_threshold=float(payload.get("medium_threshold") or 0.45),
                reviewer=reviewer,
                notes=notes,
                locked_policy={},
            )
            return {"status": "configured", "control": control}
        return {"status": "invalid", "reason": "Unknown action. Use freeze_current, override, or clear."}

    def review_pattern_suggestion(self, payload: dict[str, Any]) -> dict[str, Any]:
        suggestion_id = int(payload.get("suggestion_id") or 0)
        action = str(payload.get("action") or payload.get("status") or "").strip()
        review = self.store.review_pattern_suggestion(
            suggestion_id=suggestion_id,
            action=action,
            reviewer=str(payload.get("reviewer") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
        )
        if review is None:
            return {"status": "not_found", "suggestion_id": suggestion_id}
        if str(review.get("status") or "") != "applied":
            return {
                **review,
                "decision_status": review.get("status") or "",
                "status": "reviewed",
                "recompile": {"status": "not_requested"},
                "rerun": {"status": "not_requested"},
            }

        suggestion = dict(review.get("suggestion") or {})
        source_feedback_id = int(suggestion.get("source_feedback_id") or 0)
        source_feedback = self.store.get_criteria_feedback(source_feedback_id) if source_feedback_id else {}
        recompile_payload = {
            "target_company": str(suggestion.get("target_company") or payload.get("target_company") or ""),
            "job_id": str(suggestion.get("source_job_id") or (source_feedback or {}).get("job_id") or ""),
            "request_payload": dict(((source_feedback or {}).get("metadata") or {}).get("request_payload") or {}),
            "criteria_version_id": int(payload.get("criteria_version_id") or 0),
            "feedback_type": str(
                (
                    (source_feedback or {}).get("feedback_type")
                    or ((source_feedback or {}).get("metadata") or {}).get("feedback_type")
                    or payload.get("feedback_type")
                    or ""
                )
            ),
            "subject": str(suggestion.get("subject") or payload.get("subject") or ""),
            "value": str(suggestion.get("value") or payload.get("value") or ""),
            "rerun_retrieval": payload.get("rerun_retrieval"),
            "rerun_request_overrides": payload.get("rerun_request_overrides") or {},
        }
        recompile = self.criteria_evolution.recompile_after_feedback(recompile_payload, source_feedback_id)
        rerun_feedback = {
            "feedback_id": source_feedback_id,
            "feedback_type": recompile_payload.get("feedback_type")
            or str(((source_feedback or {}).get("feedback_type") or "")),
            "subject": recompile_payload.get("subject") or "",
            "value": recompile_payload.get("value") or "",
        }
        rerun = self._rerun_after_recompile_if_requested(recompile_payload, rerun_feedback, recompile)
        return {
            **review,
            "decision_status": review.get("status") or "",
            "status": "reviewed",
            "recompile": recompile,
            "rerun": rerun,
        }

    def list_criteria_patterns(self, target_company: str = "") -> dict[str, Any]:
        return {
            "patterns": self.store.list_criteria_patterns(target_company=target_company),
            "suggestions": self.store.list_pattern_suggestions(target_company=target_company, limit=100),
            "feedback": self.store.list_criteria_feedback(target_company=target_company, limit=50),
            "versions": self.store.list_criteria_versions(target_company=target_company, limit=20),
            "compiler_runs": self.store.list_criteria_compiler_runs(target_company=target_company, limit=50),
            "result_diffs": self.store.list_criteria_result_diffs(target_company=target_company, limit=50),
            "confidence_policy_runs": self.store.list_confidence_policy_runs(target_company=target_company, limit=50),
            "confidence_policy_controls": self.store.list_confidence_policy_controls(
                target_company=target_company, limit=50
            ),
        }

    def _build_execution_bundle(
        self,
        *,
        request: JobRequest | dict[str, Any],
        plan: Any,
        effective_request: JobRequest | dict[str, Any] | None = None,
        source: str,
        plan_review_session: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request_obj = request if isinstance(request, JobRequest) else JobRequest.from_payload(dict(request or {}))
        plan_obj = hydrate_sourcing_plan(dict(plan or {})) if isinstance(plan, dict) else plan
        plan_payload = plan_obj.to_record()
        if effective_request is None:
            effective_request_obj = _build_effective_retrieval_request(request_obj, plan_obj)
        else:
            effective_request_obj = (
                effective_request
                if isinstance(effective_request, JobRequest)
                else JobRequest.from_payload(dict(effective_request or {}))
            )
        organization_execution_profile = dict(plan_payload.get("organization_execution_profile") or {})
        if not organization_execution_profile:
            organization_execution_profile = dict(
                dict(plan_payload.get("acquisition_strategy") or {}).get("organization_execution_profile") or {}
            )
        bundle_source: dict[str, Any] = {
            "kind": str(source or "").strip() or "unknown",
            "frozen_at": _utc_now_iso(),
        }
        review_session_payload = dict(plan_review_session or {})
        review_id = int(review_session_payload.get("review_id") or 0)
        if review_id > 0:
            bundle_source["plan_review_id"] = review_id
            bundle_source["plan_review_status"] = str(review_session_payload.get("status") or "")
            bundle_source["approved_at"] = str(review_session_payload.get("approved_at") or "")
        return {
            "bundle_version": 1,
            "source": bundle_source,
            "target_company": str(effective_request_obj.target_company or request_obj.target_company or "").strip(),
            "request": request_obj.to_record(),
            "effective_request": effective_request_obj.to_record(),
            "request_matching": build_request_matching_bundle(effective_request_obj.to_record()),
            "request_preview": _build_request_preview_payload(
                request=request_obj,
                effective_request=effective_request_obj,
            ),
            "plan": plan_payload,
            "asset_reuse_plan": dict(plan_payload.get("asset_reuse_plan") or {}),
            "organization_execution_profile": organization_execution_profile,
        }

    def _resolve_workflow_plan_for_explain(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_id = int(payload.get("plan_review_id") or 0)
        if review_id:
            review_started_at = time.perf_counter()
            resolved = self._resolve_workflow_plan(payload)
            review_resolve_ms = round((time.perf_counter() - review_started_at) * 1000, 2)
            execution_bundle = dict(resolved.get("execution_bundle") or {})
            request_payload = dict(execution_bundle.get("request") or resolved.get("request") or {})
            effective_request_payload = dict(execution_bundle.get("effective_request") or resolved.get("request") or {})
            request_preview = dict(execution_bundle.get("request_preview") or {})
            if not request_preview and request_payload:
                request_preview = _build_request_preview_payload(
                    request=request_payload,
                    effective_request=effective_request_payload or request_payload,
                )
            return {
                **dict(resolved or {}),
                "ingress_request": request_payload,
                "request_preview": request_preview,
                "resolve_plan_timings_ms": {
                    "resolve_review_session": review_resolve_ms,
                },
            }

        prepared_payload, prepare_request_breakdown_ms = self._prepare_request_payload_with_diagnostics(payload)
        prepare_request_ms = float(prepare_request_breakdown_ms.get("total") or 0.0)
        ingress_request = JobRequest.from_payload(prepared_payload)

        plan_started_at = time.perf_counter()
        plan = self._build_augmented_sourcing_plan(ingress_request)
        build_plan_ms = round((time.perf_counter() - plan_started_at) * 1000, 2)

        effective_request = _build_effective_retrieval_request(ingress_request, plan)

        gate_started_at = time.perf_counter()
        plan_review_gate = build_plan_review_gate(ingress_request, plan)
        plan_review_gate = self._augment_plan_review_gate_with_runtime_context(ingress_request, plan_review_gate)
        build_plan_review_gate_ms = round((time.perf_counter() - gate_started_at) * 1000, 2)

        bundle_started_at = time.perf_counter()
        execution_bundle = self._build_execution_bundle(
            request=ingress_request,
            plan=plan,
            effective_request=effective_request,
            source="workflow_explain",
        )
        build_execution_bundle_ms = round((time.perf_counter() - bundle_started_at) * 1000, 2)

        preview_started_at = time.perf_counter()
        request_preview = _build_request_preview_payload(
            request=ingress_request,
            effective_request=effective_request,
        )
        build_request_preview_ms = round((time.perf_counter() - preview_started_at) * 1000, 2)
        result = {
            "request": effective_request.to_record(),
            "plan": plan.to_record(),
            "execution_bundle": execution_bundle,
            "plan_review_gate": plan_review_gate,
            "plan_review_session": {},
            "request_preview": request_preview,
            "ingress_request": ingress_request.to_record(),
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=effective_request.to_record()),
            "resolve_plan_timings_ms": {
                "prepare_request": prepare_request_ms,
                "build_plan": build_plan_ms,
                "build_plan_review_gate": build_plan_review_gate_ms,
                "build_execution_bundle": build_execution_bundle_ms,
                "build_request_preview": build_request_preview_ms,
            },
            "resolve_plan_timing_breakdown_ms": {
                "prepare_request": prepare_request_breakdown_ms,
            },
        }
        if bool(plan_review_gate.get("required_before_execution")) and not bool(payload.get("skip_plan_review")):
            existing_pending = self.store.find_pending_plan_review_session(
                target_company=ingress_request.target_company,
                request_payload=effective_request.to_record(),
            )
            result["status"] = "needs_plan_review"
            result["reason"] = (
                "existing_pending_plan_review" if existing_pending is not None else "plan_review_required"
            )
            if existing_pending is not None:
                result["plan_review_session"] = existing_pending
            return result
        result["status"] = "ready"
        return result

    def _resolve_workflow_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_id = int(payload.get("plan_review_id") or 0)
        if review_id:
            review_session = self.store.get_plan_review_session(review_id)
            if review_session is None:
                return {
                    "status": "needs_plan_review",
                    "reason": "plan_review_id not found",
                    "plan_review_id": review_id,
                }
            stored_execution_bundle = dict(review_session.get("execution_bundle") or {})
            request_payload = dict(stored_execution_bundle.get("request") or review_session.get("request") or {})
            plan_payload = dict(stored_execution_bundle.get("plan") or review_session.get("plan") or {})
            resolved_target_company = str(request_payload.get("target_company") or "").strip()
            if resolved_target_company:
                plan_payload = _ensure_plan_company_scope(plan_payload, resolved_target_company)
            if not str(request_payload.get("target_company") or "").strip():
                inferred_target_company = _infer_target_company_from_review_payloads(
                    request_payload=request_payload,
                    plan_payload=plan_payload,
                    decision_payload=dict(review_session.get("decision") or {}),
                )
                if inferred_target_company:
                    request_payload["target_company"] = inferred_target_company
                    plan_payload = _ensure_plan_company_scope(plan_payload, inferred_target_company)
            request_payload = _apply_plan_review_execution_overrides(request_payload, payload)
            if str(review_session.get("status") or "") not in {"approved", "ready"}:
                return {
                    "status": "needs_plan_review",
                    "reason": "plan review session is not approved yet",
                    "plan_review_session": review_session,
                    "plan_review_gate": review_session.get("gate") or {},
                    "request": request_payload,
                    "plan": plan_payload,
                    "execution_bundle": stored_execution_bundle,
                    "intent_rewrite": _build_intent_rewrite_payload(request_payload=request_payload),
                }
            bundle_request_payload = dict(stored_execution_bundle.get("request") or {})
            override_requested = bool(bundle_request_payload) and request_payload != bundle_request_payload
            execution_bundle = stored_execution_bundle
            if not execution_bundle or override_requested:
                try:
                    reviewed_request = JobRequest.from_payload(request_payload)
                    rebuilt_plan = self._build_augmented_sourcing_plan(reviewed_request)
                    request_payload = reviewed_request.to_record()
                    plan_payload = _ensure_plan_company_scope(
                        rebuilt_plan.to_record(),
                        str(reviewed_request.target_company or resolved_target_company or "").strip(),
                    )
                    execution_bundle = self._build_execution_bundle(
                        request=reviewed_request,
                        plan=rebuilt_plan,
                        source="plan_review_override_rebuild" if override_requested else "plan_review_legacy_rebuild",
                        plan_review_session=review_session,
                    )
                except Exception:
                    execution_bundle = stored_execution_bundle
            return {
                "status": "ready",
                "request": request_payload,
                "plan": plan_payload,
                "execution_bundle": execution_bundle,
                "plan_review_session": review_session,
                "intent_rewrite": _build_intent_rewrite_payload(request_payload=request_payload),
            }

        request = JobRequest.from_payload(self._prepare_request_payload(payload))
        plan = self._build_augmented_sourcing_plan(request)
        effective_request = _build_effective_retrieval_request(request, plan)
        plan_review_gate = build_plan_review_gate(request, plan)
        plan_review_gate = self._augment_plan_review_gate_with_runtime_context(request, plan_review_gate)
        if bool(plan_review_gate.get("required_before_execution")) and not bool(payload.get("skip_plan_review")):
            existing_pending = self.store.find_pending_plan_review_session(
                target_company=request.target_company,
                request_payload=effective_request.to_record(),
            )
            if existing_pending is not None:
                return {
                    "status": "needs_plan_review",
                    "reason": "existing_pending_plan_review",
                    "request": dict(existing_pending.get("request") or {}),
                    "plan": dict(existing_pending.get("plan") or {}),
                    "plan_review_gate": dict(existing_pending.get("gate") or {}),
                    "plan_review_session": existing_pending,
                    "execution_bundle": dict(existing_pending.get("execution_bundle") or {}),
                    "intent_rewrite": _build_intent_rewrite_payload(
                        request_payload=dict(existing_pending.get("request") or {})
                    ),
                }
            execution_bundle = self._build_execution_bundle(
                request=request,
                plan=plan,
                effective_request=effective_request,
                source="plan_review_session_created",
            )
            review_session = self.store.create_plan_review_session(
                target_company=request.target_company,
                request_payload=effective_request.to_record(),
                plan_payload=plan.to_record(),
                gate_payload=plan_review_gate,
                execution_bundle_payload=execution_bundle,
            )
            return {
                "status": "needs_plan_review",
                "request": effective_request.to_record(),
                "plan": plan.to_record(),
                "plan_review_gate": plan_review_gate,
                "plan_review_session": review_session,
                "execution_bundle": execution_bundle,
                "intent_rewrite": _build_intent_rewrite_payload(request_payload=effective_request.to_record()),
            }
        execution_bundle = self._build_execution_bundle(
            request=request,
            plan=plan,
            effective_request=effective_request,
            source="workflow_ready",
        )
        return {
            "status": "ready",
            "request": effective_request.to_record(),
            "plan": plan.to_record(),
            "execution_bundle": execution_bundle,
            "plan_review_session": {},
            "plan_review_gate": plan_review_gate,
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=effective_request.to_record()),
        }

    def _augment_plan_review_gate_with_runtime_context(
        self,
        request: JobRequest,
        plan_review_gate: dict[str, Any],
    ) -> dict[str, Any]:
        gate = dict(plan_review_gate or {})
        if not request.target_company:
            return gate
        try:
            reusable_snapshot = self.acquisition_engine.detect_reusable_roster_snapshot(request.target_company)
        except Exception:
            reusable_snapshot = None
        if reusable_snapshot is None:
            return gate

        hints = dict(gate.get("execution_mode_hints") or {})
        hints["local_reusable_roster_snapshot"] = reusable_snapshot
        gate["execution_mode_hints"] = hints

        suggested_actions = list(gate.get("suggested_actions") or [])
        if bool(hints.get("incremental_rerun_recommended")):
            action = (
                f"Detected local reusable roster snapshot {reusable_snapshot.get('snapshot_id')} "
                f"({reusable_snapshot.get('visible_entry_count')} visible members); prefer incremental reruns unless a fresh live roster is explicitly required."
            )
            if action not in suggested_actions:
                suggested_actions.append(action)
        gate["suggested_actions"] = suggested_actions
        return gate

    def _prepare_request_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        prepared_payload, _ = self._prepare_request_payload_with_diagnostics(payload)
        return prepared_payload

    def _should_auto_enable_local_bootstrap_fallback(
        self,
        *,
        target_company: str,
        execution_preferences: dict[str, Any],
    ) -> bool:
        if not target_company or "allow_local_bootstrap_fallback" in execution_preferences:
            return False
        bootstrap_loaded = getattr(self.store, "bootstrap_candidate_store_loaded", None)
        if not callable(bootstrap_loaded) or not bool(bootstrap_loaded()):
            return False
        if int(self.store.candidate_count_for_company(target_company) or 0) <= 0:
            return False
        snapshot_dir = resolve_company_snapshot_dir(
            self.runtime_dir,
            target_company=target_company,
            snapshot_id="",
        )
        if snapshot_dir is None or not snapshot_dir.exists():
            return True
        authoritative_serving_path = resolve_snapshot_serving_artifact_path(
            snapshot_dir,
            asset_view="canonical_merged",
            allow_candidate_documents_fallback=False,
        )
        return authoritative_serving_path is None

    def _local_bootstrap_fallback_enabled_for_request(self, request: JobRequest) -> bool:
        execution_preferences = dict(request.execution_preferences or {})
        if "allow_local_bootstrap_fallback" in execution_preferences:
            return bool(execution_preferences.get("allow_local_bootstrap_fallback"))
        return self._should_auto_enable_local_bootstrap_fallback(
            target_company=str(request.target_company or "").strip(),
            execution_preferences=execution_preferences,
        )

    def _prepare_request_payload_with_diagnostics(
        self,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, float]]:
        started_at = time.perf_counter()
        breakdown_ms: dict[str, float] = {}

        step_started_at = time.perf_counter()
        fallback_request = JobRequest.from_payload(payload).to_record()
        breakdown_ms["materialize_fallback_request"] = round((time.perf_counter() - step_started_at) * 1000, 2)
        raw_user_request = str(fallback_request.get("raw_user_request") or fallback_request.get("query") or "").strip()

        step_started_at = time.perf_counter()
        explicit_execution_preferences = normalize_execution_preferences(
            payload,
            target_company=str(fallback_request.get("target_company") or payload.get("target_company") or ""),
        )
        request_target_company = str(fallback_request.get("target_company") or payload.get("target_company") or "").strip()
        if self._should_auto_enable_local_bootstrap_fallback(
            target_company=request_target_company,
            execution_preferences=explicit_execution_preferences,
        ):
            explicit_execution_preferences = {
                **dict(explicit_execution_preferences or {}),
                "allow_local_bootstrap_fallback": True,
            }
        breakdown_ms["normalize_execution_preferences"] = round((time.perf_counter() - step_started_at) * 1000, 2)

        step_started_at = time.perf_counter()
        normalized_patch = self.model_client.normalize_request(
            {
                "raw_user_request": raw_user_request,
                "input_payload": dict(payload or {}),
                "fallback_request": fallback_request,
                "current_request": fallback_request,
            }
        )
        breakdown_ms["llm_normalize_request"] = round((time.perf_counter() - step_started_at) * 1000, 2)

        step_started_at = time.perf_counter()
        merged_payload = dict(fallback_request)
        if isinstance(normalized_patch, dict) and normalized_patch:
            merged_payload = _merge_request_payload(
                fallback_request,
                normalized_patch,
                explicit_execution_preferences=explicit_execution_preferences,
            )
        elif explicit_execution_preferences:
            merged_payload["execution_preferences"] = explicit_execution_preferences
        breakdown_ms["merge_normalized_request"] = round((time.perf_counter() - step_started_at) * 1000, 2)

        step_started_at = time.perf_counter()
        merged_payload = _supplement_request_query_signals(
            merged_payload,
            raw_text=raw_user_request,
            include_raw_keyword_extraction=not _shared_has_structured_request_signals(normalized_patch),
        )
        breakdown_ms["deterministic_signal_supplement"] = round((time.perf_counter() - step_started_at) * 1000, 2)

        step_started_at = time.perf_counter()
        canonical_payload = _canonicalize_request_payload(merged_payload)
        breakdown_ms["canonicalize_request_payload"] = round((time.perf_counter() - step_started_at) * 1000, 2)

        step_started_at = time.perf_counter()
        prepared_payload = JobRequest.from_payload(canonical_payload).to_record()
        breakdown_ms["final_request_materialization"] = round((time.perf_counter() - step_started_at) * 1000, 2)
        breakdown_ms["total"] = round((time.perf_counter() - started_at) * 1000, 2)
        return prepared_payload, breakdown_ms

    def _build_augmented_sourcing_plan(self, request: JobRequest):
        organization_execution_profile = self._resolve_organization_execution_profile(request)
        plan = build_sourcing_plan(
            request,
            self.catalog,
            self.model_client,
            organization_execution_profile=organization_execution_profile,
        )
        try:
            asset_reuse_plan = compile_asset_reuse_plan(
                runtime_dir=self.runtime_dir,
                store=self.store,
                request=request,
                plan=plan,
            )
        except Exception as exc:
            plan.assumptions = list(plan.assumptions or []) + [f"Asset reuse planning skipped: {exc}"]
            asset_reuse_plan = {}
        return apply_asset_reuse_plan_to_sourcing_plan(plan, asset_reuse_plan)

    def _resolve_organization_execution_profile(self, request: JobRequest) -> dict[str, Any]:
        target_company = str(request.target_company or "").strip()
        if not target_company:
            return {}
        try:
            return ensure_organization_execution_profile(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=target_company,
                asset_view=str(request.asset_view or "canonical_merged").strip() or "canonical_merged",
            )
        except Exception as exc:
            return {
                "target_company": target_company,
                "asset_view": str(request.asset_view or "canonical_merged").strip() or "canonical_merged",
                "status": "error",
                "org_scale_band": "unknown",
                "default_acquisition_mode": "full_company_roster",
                "reason_codes": ["organization_execution_profile_resolution_failed"],
                "explanation": {"reason": str(exc)},
                "summary": {},
            }

    def _build_query_dispatch_context(self, payload: dict[str, Any], request: JobRequest) -> dict[str, Any]:
        request_payload = request.to_record()
        request_matching = build_request_matching_bundle(request_payload)
        requester_value = payload.get("requester")
        requester_value_resolved = (
            str(dict(requester_value).get("id") or "").strip()
            if isinstance(requester_value, dict)
            else str(requester_value or "").strip()
        )
        requester_id = str(
            payload.get("requester_id") or payload.get("user_id") or requester_value_resolved or ""
        ).strip()
        tenant_id = str(payload.get("tenant_id") or payload.get("workspace_id") or payload.get("org_id") or "").strip()
        idempotency_key = str(payload.get("idempotency_key") or payload.get("query_idempotency_key") or "").strip()
        scope = str(payload.get("query_dispatch_scope") or "").strip().lower()
        if scope not in {"global", "tenant", "requester"}:
            scope = "tenant" if tenant_id else ("requester" if requester_id else "global")
        force_fresh_run = bool(request.execution_preferences.get("force_fresh_run"))
        allow_result_reuse = bool(payload.get("allow_result_reuse", True))
        allow_join_inflight = bool(payload.get("allow_join_inflight", True))
        if force_fresh_run and bool(payload.get("respect_force_fresh", True)):
            allow_result_reuse = False
            allow_join_inflight = False
        return {
            "dispatch_enabled": bool(payload.get("query_dispatch_enabled", True)),
            "allow_join_inflight": allow_join_inflight,
            "allow_result_reuse": allow_result_reuse,
            "scope": scope,
            "requester_id": requester_id,
            "tenant_id": tenant_id,
            "idempotency_key": idempotency_key,
            "request_matching": request_matching,
            "request_signature": str(
                request_matching.get("matching_request_signature") or matching_request_signature(request_payload)
            ),
            "request_family_signature": str(
                request_matching.get("matching_request_family_signature")
                or matching_request_family_signature(request_payload)
            ),
        }

    def _request_with_snapshot_reuse(self, request: JobRequest, dispatch: dict[str, Any]) -> JobRequest:
        snapshot_id = str(dispatch.get("matched_snapshot_id") or "").strip()
        if not snapshot_id:
            return request
        payload = request.to_record()
        execution_preferences = dict(payload.get("execution_preferences") or {})
        execution_preferences["reuse_existing_roster"] = True
        execution_preferences["reuse_snapshot_id"] = snapshot_id
        baseline_job_id = str(
            dispatch.get("matched_job_id") or dict(dispatch.get("matched_job") or {}).get("job_id") or ""
        ).strip()
        if baseline_job_id:
            execution_preferences["reuse_baseline_job_id"] = baseline_job_id
        payload["execution_preferences"] = execution_preferences
        return JobRequest(**payload)

    def _payload_explicitly_requests_force_fresh(self, payload: dict[str, Any]) -> bool:
        execution_preferences = payload.get("execution_preferences")
        if isinstance(execution_preferences, dict) and "force_fresh_run" in execution_preferences:
            return True
        return "force_fresh_run" in payload

    def _maybe_suppress_inherited_force_fresh_run(
        self,
        *,
        payload: dict[str, Any],
        request: JobRequest,
        asset_reuse_plan: dict[str, Any],
        runtime_execution_mode: str,
    ) -> tuple[JobRequest, dict[str, Any]]:
        execution_preferences = dict(request.execution_preferences or {})
        if not bool(execution_preferences.get("force_fresh_run")):
            return request, {}
        if str(runtime_execution_mode or "").strip().lower() != "hosted":
            return request, {}
        if self._payload_explicitly_requests_force_fresh(payload):
            return request, {}
        lane_requirements = self._requested_dispatch_lane_requirements(request)
        baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
        baseline_reuse_available = bool(asset_reuse_plan.get("baseline_reuse_available"))
        current_ready = bool(asset_reuse_plan.get("baseline_current_effective_ready"))
        former_ready = bool(asset_reuse_plan.get("baseline_former_effective_ready"))
        current_count = int(asset_reuse_plan.get("baseline_current_effective_candidate_count") or 0)
        former_count = int(asset_reuse_plan.get("baseline_former_effective_candidate_count") or 0)
        if not baseline_snapshot_id or not baseline_reuse_available:
            registry_row = self.store.get_authoritative_organization_asset_registry(
                target_company=request.target_company,
                asset_view=str(request.asset_view or "canonical_merged").strip() or "canonical_merged",
            )
            if registry_row:
                baseline_snapshot_id = str(registry_row.get("snapshot_id") or "").strip()
                baseline_reuse_available = bool(baseline_snapshot_id)
                current_ready = bool(registry_row.get("current_lane_effective_ready"))
                former_ready = bool(registry_row.get("former_lane_effective_ready"))
                current_count = int(registry_row.get("current_lane_effective_candidate_count") or 0)
                former_count = int(registry_row.get("former_lane_effective_candidate_count") or 0)
        if not baseline_snapshot_id or not baseline_reuse_available:
            return request, {}
        baseline_has_reusable_lane = bool(
            (bool(lane_requirements.get("need_current")) and current_ready and current_count > 0)
            or (bool(lane_requirements.get("need_former")) and former_ready and former_count > 0)
        )
        if not baseline_has_reusable_lane:
            return request, {}
        payload_record = request.to_record()
        updated_execution_preferences = dict(payload_record.get("execution_preferences") or {})
        updated_execution_preferences.pop("force_fresh_run", None)
        if baseline_snapshot_id:
            updated_execution_preferences["reuse_existing_roster"] = True
        payload_record["execution_preferences"] = updated_execution_preferences
        suppressed_request = JobRequest(**payload_record)
        return suppressed_request, {
            "reason": "effective_baseline_ready",
            "baseline_snapshot_id": baseline_snapshot_id,
            "planner_mode": str(asset_reuse_plan.get("planner_mode") or ""),
            "requested_employment_statuses": list(lane_requirements.get("employment_statuses") or []),
            "current_lane_effective_ready": bool(current_ready),
            "former_lane_effective_ready": bool(former_ready),
            "current_lane_effective_candidate_count": int(current_count),
            "former_lane_effective_candidate_count": int(former_count),
        }

    def _request_with_delta_baseline_hints(
        self,
        request: JobRequest,
        asset_reuse_plan: dict[str, Any],
        dispatch: dict[str, Any] | None = None,
    ) -> JobRequest:
        baseline_snapshot_id = str(asset_reuse_plan.get("baseline_snapshot_id") or "").strip()
        selected_snapshot_ids = [
            str(item or "").strip()
            for item in list(asset_reuse_plan.get("selected_snapshot_ids") or [])
            if str(item or "").strip()
        ]
        if not baseline_snapshot_id and not selected_snapshot_ids:
            return request
        payload = request.to_record()
        execution_preferences = dict(payload.get("execution_preferences") or {})
        if baseline_snapshot_id:
            execution_preferences["delta_baseline_snapshot_id"] = baseline_snapshot_id
        if selected_snapshot_ids:
            execution_preferences["delta_baseline_snapshot_ids"] = selected_snapshot_ids
        baseline_job_id = str(
            dict(dispatch or {}).get("matched_job_id")
            or dict(dict(dispatch or {}).get("matched_job") or {}).get("job_id")
            or ""
        ).strip()
        if baseline_job_id:
            execution_preferences["reuse_baseline_job_id"] = baseline_job_id
        payload["execution_preferences"] = execution_preferences
        return JobRequest(**payload)

    def _build_dispatch_request_family_match_explanation(
        self,
        *,
        request_payload: dict[str, Any],
        matched_job: dict[str, Any],
        strategy: str,
        match: dict[str, Any] | None = None,
        selection_mode: str = "",
    ) -> dict[str, Any]:
        matched_request = dict(matched_job.get("request") or {})
        if not matched_request:
            return {}
        explanation = dict(
            (dict(match or {}).get("explanation") or {})
            or build_request_family_match_explanation(
                request_payload,
                matched_request,
                match=match,
                selection_mode=selection_mode or strategy,
            )
        )
        if not explanation:
            return {}
        explanation["selection_mode"] = str(selection_mode or strategy or "").strip() or "request_family_score"
        explanation["dispatch_strategy"] = str(strategy or "")
        explanation["matched_job_id"] = str(matched_job.get("job_id") or "")
        explanation["matched_job_status"] = str(matched_job.get("status") or "")
        explanation["matched_job_stage"] = str(matched_job.get("stage") or "")
        return explanation

    def _load_snapshot_reuse_context_from_snapshot(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        source_path_hint: str = "",
        asset_view: str = "canonical_merged",
    ) -> dict[str, Any]:
        normalized_snapshot_id = str(snapshot_id or "").strip()
        if not normalized_snapshot_id:
            return {}
        snapshot_dir = resolve_company_snapshot_dir(
            self.runtime_dir,
            target_company=target_company,
            snapshot_id=normalized_snapshot_id,
        )
        if snapshot_dir is None or not snapshot_dir.exists():
            return {}
        return {
            "snapshot_id": normalized_snapshot_id,
            "snapshot_dir": str(snapshot_dir),
            "source_path": self._snapshot_reuse_source_path(
                snapshot_dir=snapshot_dir,
                asset_view=asset_view,
                source_path_hint=source_path_hint,
            ),
        }

    def _snapshot_reuse_source_path(
        self,
        *,
        snapshot_dir: Path,
        asset_view: str = "canonical_merged",
        source_path_hint: str = "",
    ) -> str:
        candidate_paths: list[Path] = []
        if str(source_path_hint or "").strip():
            hint_path = Path(source_path_hint).expanduser()
            if hint_path.exists():
                try:
                    hint_path.resolve().relative_to(snapshot_dir.resolve())
                except ValueError:
                    pass
                else:
                    candidate_paths.append(hint_path)
        resolved_serving_path = resolve_snapshot_serving_artifact_path(
            snapshot_dir,
            asset_view=asset_view,
            allow_candidate_documents_fallback=False,
        )
        if resolved_serving_path is not None:
            candidate_paths.append(resolved_serving_path)
        for candidate_path in candidate_paths:
            if candidate_path.exists():
                return str(candidate_path)
        return ""

    def _requested_dispatch_lane_requirements(self, request: JobRequest) -> dict[str, Any]:
        return requested_dispatch_lane_requirements(request)

    def _dispatch_requires_delta_for_request(
        self,
        *,
        request: JobRequest,
        asset_reuse_plan: dict[str, Any],
    ) -> bool:
        return dispatch_requires_delta_for_request(request=request, asset_reuse_plan=asset_reuse_plan)

    def _asset_reuse_plan_with_registry_lane_fallback(
        self,
        *,
        asset_reuse_plan: dict[str, Any] | None,
        registry_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        resolved = dict(asset_reuse_plan or {})
        registry_row = dict(registry_row or {})
        if not registry_row:
            return resolved
        if "baseline_reuse_available" not in resolved:
            resolved["baseline_reuse_available"] = True
        fallback_mappings = (
            ("baseline_snapshot_id", "snapshot_id"),
            ("baseline_current_effective_ready", "current_lane_effective_ready"),
            ("baseline_former_effective_ready", "former_lane_effective_ready"),
            ("baseline_current_effective_candidate_count", "current_lane_effective_candidate_count"),
            ("baseline_former_effective_candidate_count", "former_lane_effective_candidate_count"),
        )
        for plan_key, registry_key in fallback_mappings:
            value = resolved.get(plan_key)
            if value in {None, ""}:
                resolved[plan_key] = registry_row.get(registry_key)
        if not list(resolved.get("selected_snapshot_ids") or []):
            selected_snapshot_ids = [
                str(item).strip()
                for item in list(registry_row.get("selected_snapshot_ids") or [])
                if str(item).strip()
            ]
            if selected_snapshot_ids:
                resolved["selected_snapshot_ids"] = selected_snapshot_ids
        return resolved

    def _build_registry_dispatch_reuse_explanation(
        self,
        *,
        request: JobRequest,
        strategy: str,
        registry_row: dict[str, Any],
        asset_reuse_plan: dict[str, Any] | None = None,
        matched_job: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        asset_reuse_plan = dict(asset_reuse_plan or {})
        matched_job = dict(matched_job or {})
        request_payload = request.to_record()
        lane_semantics = compile_asset_reuse_lane_semantics(
            request=request,
            asset_reuse_plan=asset_reuse_plan,
        )
        explanation = {}
        if matched_job and dict(matched_job.get("request") or {}):
            explanation = self._build_dispatch_request_family_match_explanation(
                request_payload=request_payload,
                matched_job=matched_job,
                strategy=strategy,
                selection_mode="organization_asset_registry_lane_coverage",
            )
        if not explanation:
            explanation = {
                "selection_mode": "organization_asset_registry_lane_coverage",
                "dispatch_strategy": str(strategy or ""),
                "matched_job_id": str(matched_job.get("job_id") or ""),
                "matched_job_status": str(matched_job.get("status") or ""),
                "matched_job_stage": str(matched_job.get("stage") or ""),
                "score": 0.0,
                "exact_request_match": False,
                "exact_family_match": False,
                "matched_fields": [],
                "mismatched_fields": [],
                "reasons": ["organization_asset_registry_lane_coverage"],
                "field_details": [],
            }
        explanation["reuse_basis"] = "organization_asset_registry_lane_coverage"
        explanation["matched_registry_id"] = int(registry_row.get("registry_id") or 0)
        explanation["matched_registry_snapshot_id"] = str(registry_row.get("snapshot_id") or "")
        explanation["matched_snapshot_candidate_count"] = int(registry_row.get("candidate_count") or 0)
        explanation["registry_authoritative"] = bool(registry_row.get("authoritative"))
        explanation["requested_employment_statuses"] = list(lane_semantics.get("employment_statuses") or [])
        explanation["requested_need_current_lane"] = bool(lane_semantics.get("need_current"))
        explanation["requested_need_former_lane"] = bool(lane_semantics.get("need_former"))
        explanation["current_lane_effective_ready"] = bool(
            dict(lane_semantics.get("current_lane") or {}).get("baseline_ready")
        )
        explanation["former_lane_effective_ready"] = bool(
            dict(lane_semantics.get("former_lane") or {}).get("baseline_ready")
        )
        explanation["current_lane_effective_candidate_count"] = int(
            dict(lane_semantics.get("current_lane") or {}).get("baseline_effective_candidate_count") or 0
        )
        explanation["former_lane_effective_candidate_count"] = int(
            dict(lane_semantics.get("former_lane") or {}).get("baseline_effective_candidate_count") or 0
        )
        explanation["baseline_reuse_available"] = bool(lane_semantics.get("baseline_reuse_available"))
        explanation["requires_delta_acquisition"] = bool(lane_semantics.get("requires_delta_acquisition"))
        explanation["planner_mode"] = str(lane_semantics.get("planner_mode") or "")
        explanation["lane_requirements_ready"] = bool(lane_semantics.get("lane_requirements_ready"))
        explanation["current_lane_delta_required"] = bool(
            dict(lane_semantics.get("current_lane") or {}).get("delta_required")
        )
        explanation["former_lane_delta_required"] = bool(
            dict(lane_semantics.get("former_lane") or {}).get("delta_required")
        )
        explanation["selected_snapshot_ids"] = [
            str(item).strip()
            for item in list(
                asset_reuse_plan.get("selected_snapshot_ids") or registry_row.get("selected_snapshot_ids") or []
            )
            if str(item).strip()
        ]
        return explanation

    def _resolve_organization_asset_registry_snapshot_reuse_match(
        self,
        request: JobRequest,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        request_payload = request.to_record()
        target_company = str(request.target_company or "").strip()
        target_scope = str(request.target_scope or "").strip().lower()
        if not target_company or target_scope != "full_company_asset":
            return {}

        asset_view = str(request.asset_view or "canonical_merged").strip() or "canonical_merged"
        registry_row = self.store.get_authoritative_organization_asset_registry(
            target_company=target_company,
            asset_view=asset_view,
        )
        if not registry_row:
            return {}

        asset_reuse_plan = dict(context.get("asset_reuse_plan") or {})
        if asset_reuse_plan and not bool(asset_reuse_plan.get("baseline_reuse_available")):
            return {}

        effective_asset_reuse_plan = self._asset_reuse_plan_with_registry_lane_fallback(
            asset_reuse_plan=asset_reuse_plan,
            registry_row=registry_row,
        )
        lane_semantics = compile_asset_reuse_lane_semantics(
            request=request,
            asset_reuse_plan=effective_asset_reuse_plan,
        )
        current_lane_payload = dict(lane_semantics.get("current_lane") or {})
        former_lane_payload = dict(lane_semantics.get("former_lane") or {})
        current_count = int(current_lane_payload.get("baseline_effective_candidate_count") or 0)
        former_count = int(former_lane_payload.get("baseline_effective_candidate_count") or 0)
        baseline_snapshot_id = str(
            effective_asset_reuse_plan.get("baseline_snapshot_id") or registry_row.get("snapshot_id") or ""
        ).strip()
        if not baseline_snapshot_id:
            return {}

        planner_requires_delta = bool(asset_reuse_plan.get("requires_delta_acquisition"))
        requires_delta = bool(lane_semantics.get("requires_delta_acquisition"))
        effective_candidate_total = max(
            int(registry_row.get("candidate_count") or 0),
            current_count + former_count,
        )
        organization_execution_profile = dict(context.get("organization_execution_profile") or {})
        if not organization_execution_profile:
            organization_execution_profile = self.store.get_organization_execution_profile(
                target_company=target_company,
                asset_view=asset_view,
            )
        snapshot_reuse_limit = organization_execution_profile_snapshot_reuse_limit(organization_execution_profile)
        lane_candidate_requirements_met = bool(lane_semantics.get("lane_candidate_requirements_met"))
        lane_requirements_ready = bool(lane_semantics.get("lane_requirements_ready"))
        force_reuse_snapshot_only = bool(
            (lane_requirements_ready or (not requires_delta and lane_candidate_requirements_met))
            and effective_candidate_total > 0
            and snapshot_reuse_limit > 0
            and effective_candidate_total <= snapshot_reuse_limit
        )
        force_fresh_run_suppression = dict(context.get("force_fresh_run_suppression") or {})
        if (
            not force_reuse_snapshot_only
            and str(force_fresh_run_suppression.get("reason") or "").strip() == "effective_baseline_ready"
            and lane_requirements_ready
            and effective_candidate_total > 0
        ):
            force_reuse_snapshot_only = True
        effective_strategy = (
            "delta_from_snapshot" if requires_delta and not force_reuse_snapshot_only else "reuse_snapshot"
        )
        if not requires_delta:
            if bool(lane_semantics.get("need_current")) and current_count <= 0:
                return {}
            if bool(lane_semantics.get("need_former")) and former_count <= 0:
                return {}

        snapshot_context = self._load_snapshot_reuse_context_from_snapshot(
            target_company=target_company,
            snapshot_id=baseline_snapshot_id,
        )
        if not snapshot_context:
            return {}

        matched_job = {}
        source_job_id = str(registry_row.get("source_job_id") or "").strip()
        if source_job_id:
            matched_job = dict(self.store.get_job(source_job_id) or {})

        explanation = self._build_registry_dispatch_reuse_explanation(
            request=request,
            strategy=effective_strategy,
            registry_row=registry_row,
            asset_reuse_plan=effective_asset_reuse_plan,
            matched_job=matched_job,
        )
        explanation["requires_delta_acquisition"] = requires_delta
        explanation["planner_requires_delta_acquisition"] = planner_requires_delta
        explanation["lane_candidate_requirements_met"] = lane_candidate_requirements_met
        explanation["lane_requirements_ready"] = lane_requirements_ready
        explanation["force_reuse_snapshot_only"] = force_reuse_snapshot_only
        if force_fresh_run_suppression:
            explanation["force_fresh_run_suppression"] = force_fresh_run_suppression
        explanation["organization_execution_profile"] = organization_execution_profile
        explanation["snapshot_reuse_limit"] = snapshot_reuse_limit

        return {
            "strategy": effective_strategy,
            "scope": str(context.get("scope") or "global"),
            "request_signature": str(context.get("request_signature") or request_signature(request_payload)),
            "request_family_signature": str(
                context.get("request_family_signature") or request_family_signature(request_payload)
            ),
            "matched_job": matched_job,
            "matched_snapshot_id": str(snapshot_context.get("snapshot_id") or ""),
            "matched_snapshot_dir": str(snapshot_context.get("snapshot_dir") or ""),
            "matched_snapshot_source_path": str(snapshot_context.get("source_path") or ""),
            "matched_registry_id": int(registry_row.get("registry_id") or 0),
            "force_reuse_snapshot_only": force_reuse_snapshot_only,
            "baseline_match": {},
            "request_family_match_explanation": explanation,
        }

    def _resolve_snapshot_reuse_job_match(
        self,
        request: JobRequest,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        request_payload = request.to_record()
        request_matching = dict(context.get("request_matching") or build_request_matching_bundle(request_payload))
        best_job: dict[str, Any] | None = None
        best_match: dict[str, Any] | None = None
        best_snapshot: dict[str, Any] | None = None
        best_sort_key: tuple[float, str, str] | None = None
        allowed_job_types = {"workflow", "retrieval", "retrieval_rerun"}
        scope = str(context.get("scope") or "global")
        requester_id = str(context.get("requester_id") or "")
        tenant_id = str(context.get("tenant_id") or "")
        target_company = str(request.target_company or "").strip().lower()
        if not target_company:
            return {}

        for candidate in self.store.list_jobs(statuses=["completed"], limit=200):
            if str(candidate.get("job_type") or "") not in allowed_job_types:
                continue
            candidate_request = dict(candidate.get("request") or {})
            if str(candidate_request.get("target_company") or "").strip().lower() != target_company:
                continue
            if not _dispatch_scope_matches_job(candidate, scope=scope, requester_id=requester_id, tenant_id=tenant_id):
                continue
            snapshot_context = self._load_snapshot_reuse_context_from_job(
                candidate, target_company=request.target_company
            )
            if not snapshot_context:
                continue
            match = request_family_score(
                request_payload,
                candidate_request,
                left_bundle=request_matching,
                right_bundle=dict(candidate.get("request_matching") or {}),
            )
            score = float(match.get("score") or 0.0)
            if score < MATCH_THRESHOLD and not bool(match.get("exact_family_match")):
                continue
            sort_key = (score, str(candidate.get("updated_at") or ""), str(candidate.get("created_at") or ""))
            if best_sort_key is None or sort_key > best_sort_key:
                best_job = candidate
                best_match = match
                best_snapshot = snapshot_context
                best_sort_key = sort_key

        if best_job is None or best_match is None or best_snapshot is None:
            return {}
        return {
            "strategy": "reuse_snapshot",
            "scope": scope,
            "request_signature": str(context.get("request_signature") or request_signature(request_payload)),
            "request_family_signature": str(
                context.get("request_family_signature") or request_family_signature(request_payload)
            ),
            "matched_job": best_job,
            "matched_snapshot_id": str(best_snapshot.get("snapshot_id") or ""),
            "matched_snapshot_dir": str(best_snapshot.get("snapshot_dir") or ""),
            "matched_snapshot_source_path": str(best_snapshot.get("source_path") or ""),
            "baseline_match": dict(best_match),
            "request_family_match_explanation": self._build_dispatch_request_family_match_explanation(
                request_payload=request_payload,
                matched_job=best_job,
                strategy="reuse_snapshot",
                match=best_match,
            ),
        }

    def _load_snapshot_reuse_context_from_job(
        self,
        job: dict[str, Any],
        *,
        target_company: str,
    ) -> dict[str, Any]:
        summary = dict(job.get("summary") or {})
        candidate_source = dict(summary.get("candidate_source") or {})
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        if not snapshot_id:
            snapshot_id = str(dict(summary.get("pre_retrieval_refresh") or {}).get("snapshot_id") or "").strip()
        if not snapshot_id:
            return {}
        return self._load_snapshot_reuse_context_from_snapshot(
            target_company=target_company,
            snapshot_id=snapshot_id,
            source_path_hint=str(candidate_source.get("source_path") or "").strip(),
        )

    def _resolve_small_org_company_asset_snapshot_reuse_job_match(
        self,
        request: JobRequest,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        def _safe_int(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        request_payload = request.to_record()
        request_matching = dict(context.get("request_matching") or build_request_matching_bundle(request_payload))
        target_company = str(request.target_company or "").strip().lower()
        target_scope = str(request.target_scope or "").strip().lower()
        if not target_company or target_scope != "full_company_asset":
            return {}

        best_job: dict[str, Any] | None = None
        best_match: dict[str, Any] | None = None
        best_snapshot: dict[str, Any] | None = None
        best_candidate_count = 0
        best_sort_key: tuple[str, str] | None = None
        allowed_job_types = {"workflow", "retrieval", "retrieval_rerun"}
        scope = str(context.get("scope") or "global")
        requester_id = str(context.get("requester_id") or "")
        tenant_id = str(context.get("tenant_id") or "")
        organization_execution_profile = dict(context.get("organization_execution_profile") or {})
        if not organization_execution_profile:
            organization_execution_profile = self.store.get_organization_execution_profile(
                target_company=request.target_company,
                asset_view=str(request.asset_view or "canonical_merged").strip() or "canonical_merged",
            )
        snapshot_reuse_limit = organization_execution_profile_snapshot_reuse_limit(organization_execution_profile)
        if snapshot_reuse_limit <= 0:
            return {}

        for candidate in self.store.list_jobs(statuses=["completed"], limit=200):
            if str(candidate.get("job_type") or "") not in allowed_job_types:
                continue
            candidate_request = dict(candidate.get("request") or {})
            if str(candidate_request.get("target_company") or "").strip().lower() != target_company:
                continue
            if str(candidate_request.get("target_scope") or "").strip().lower() != "full_company_asset":
                continue
            if not _dispatch_scope_matches_job(candidate, scope=scope, requester_id=requester_id, tenant_id=tenant_id):
                continue

            plan_payload = dict(candidate.get("plan") or {})
            plan_strategy = (
                str(dict(plan_payload.get("acquisition_strategy") or {}).get("strategy_type") or "").strip().lower()
            )
            if plan_strategy and plan_strategy != "full_company_roster":
                continue

            summary = dict(candidate.get("summary") or {})
            candidate_source = dict(summary.get("candidate_source") or {})
            if not candidate_source_is_snapshot_authoritative(candidate_source):
                continue
            candidate_count = _safe_int(candidate_source.get("candidate_count"))
            unfiltered_candidate_count = _safe_int(
                candidate_source.get("unfiltered_candidate_count") or candidate_count or 0
            )
            if candidate_count <= 0 or unfiltered_candidate_count <= 0:
                continue
            if candidate_count != unfiltered_candidate_count:
                continue
            if unfiltered_candidate_count > snapshot_reuse_limit:
                continue

            snapshot_context = self._load_snapshot_reuse_context_from_job(
                candidate, target_company=request.target_company
            )
            if not snapshot_context:
                continue

            match = request_family_score(
                request_payload,
                candidate_request,
                left_bundle=request_matching,
                right_bundle=dict(candidate.get("request_matching") or {}),
            )
            sort_key = (str(candidate.get("updated_at") or ""), str(candidate.get("created_at") or ""))
            if (
                best_job is None
                or unfiltered_candidate_count > best_candidate_count
                or (
                    unfiltered_candidate_count == best_candidate_count
                    and (best_sort_key is None or sort_key > best_sort_key)
                )
            ):
                best_job = candidate
                best_match = match
                best_snapshot = snapshot_context
                best_candidate_count = unfiltered_candidate_count
                best_sort_key = sort_key

        if best_job is None or best_match is None or best_snapshot is None:
            return {}

        explanation = self._build_dispatch_request_family_match_explanation(
            request_payload=request_payload,
            matched_job=best_job,
            strategy="reuse_snapshot",
            match=best_match,
            selection_mode="small_org_company_asset_snapshot_reuse",
        )
        explanation["reuse_basis"] = "same_company_small_org_full_asset_snapshot"
        explanation["matched_snapshot_candidate_count"] = best_candidate_count

        return {
            "strategy": "reuse_snapshot",
            "scope": scope,
            "request_signature": str(context.get("request_signature") or request_signature(request_payload)),
            "request_family_signature": str(
                context.get("request_family_signature") or request_family_signature(request_payload)
            ),
            "matched_job": best_job,
            "matched_snapshot_id": str(best_snapshot.get("snapshot_id") or ""),
            "matched_snapshot_dir": str(best_snapshot.get("snapshot_dir") or ""),
            "matched_snapshot_source_path": str(best_snapshot.get("source_path") or ""),
            "baseline_match": dict(best_match),
            "request_family_match_explanation": explanation,
        }

    def _maybe_prefer_registry_snapshot_over_completed_match(
        self,
        *,
        request: JobRequest,
        context: dict[str, Any],
        completed_match: dict[str, Any],
        registry_snapshot_reuse_match: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if str(request.target_scope or "").strip().lower() != "full_company_asset":
            return {}

        candidate_registry_match = dict(registry_snapshot_reuse_match or {})
        if not candidate_registry_match:
            candidate_registry_match = self._resolve_organization_asset_registry_snapshot_reuse_match(request, context)
        if not candidate_registry_match:
            return {}
        if str(candidate_registry_match.get("strategy") or "").strip() != "reuse_snapshot":
            return {}

        matched_snapshot_id = str(candidate_registry_match.get("matched_snapshot_id") or "").strip()
        if not matched_snapshot_id:
            return {}

        completed_snapshot_context = self._load_snapshot_reuse_context_from_job(
            completed_match,
            target_company=request.target_company,
        )
        completed_snapshot_id = str(completed_snapshot_context.get("snapshot_id") or "").strip()
        if completed_snapshot_id and completed_snapshot_id == matched_snapshot_id:
            return {}

        preferred_match = dict(candidate_registry_match)
        explanation = dict(preferred_match.get("request_family_match_explanation") or {})
        explanation["completed_match_reuse_suppressed"] = True
        explanation["completed_match_reuse_suppressed_reason"] = "authoritative_registry_snapshot_preferred"
        explanation["completed_match_job_id"] = str(completed_match.get("job_id") or "")
        explanation["completed_match_snapshot_id"] = completed_snapshot_id
        preferred_match["request_family_match_explanation"] = explanation
        return preferred_match

    def _resolve_query_dispatch_decision(self, request: JobRequest, context: dict[str, Any]) -> dict[str, Any]:
        request_payload = request.to_record()
        request_sig = str(context.get("request_signature") or request_signature(request_payload))
        request_family_sig = str(context.get("request_family_signature") or request_family_signature(request_payload))
        scope = str(context.get("scope") or "global")
        requester_id = str(context.get("requester_id") or "")
        tenant_id = str(context.get("tenant_id") or "")
        idempotency_key = str(context.get("idempotency_key") or "")
        if not bool(context.get("dispatch_enabled", True)):
            return {
                "strategy": "new_job",
                "scope": scope,
                "request_signature": request_sig,
                "request_family_signature": request_family_sig,
                "matched_job": {},
            }

        if idempotency_key:
            matched_by_idempotency = self.store.find_latest_job_by_idempotency_key(
                idempotency_key=idempotency_key,
                target_company=request.target_company,
                statuses=["queued", "running", "blocked", "completed"],
                requester_id=requester_id,
                tenant_id=tenant_id,
                scope=scope,
            )
            if matched_by_idempotency:
                matched_status = str(matched_by_idempotency.get("status") or "")
                strategy = "reuse_completed" if matched_status == "completed" else "join_inflight"
                return {
                    "strategy": strategy,
                    "scope": scope,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matched_job": matched_by_idempotency,
                    "request_family_match_explanation": self._build_dispatch_request_family_match_explanation(
                        request_payload=request_payload,
                        matched_job=matched_by_idempotency,
                        strategy=f"{strategy}_via_idempotency",
                    ),
                }

        if bool(context.get("allow_join_inflight", True)):
            inflight_match = self.store.find_latest_job_by_request_signature(
                request_signature_value=request_sig,
                target_company=request.target_company,
                statuses=["queued", "running", "blocked"],
                requester_id=requester_id,
                tenant_id=tenant_id,
                scope=scope,
            )
            if inflight_match:
                return {
                    "strategy": "join_inflight",
                    "scope": scope,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matched_job": inflight_match,
                    "request_family_match_explanation": self._build_dispatch_request_family_match_explanation(
                        request_payload=request_payload,
                        matched_job=inflight_match,
                        strategy="join_inflight",
                    ),
                }
            inflight_family_match = self.store.find_latest_job_by_request_family_signature(
                request_family_signature_value=request_family_sig,
                target_company=request.target_company,
                statuses=["queued", "running", "blocked"],
                requester_id=requester_id,
                tenant_id=tenant_id,
                scope=scope,
            )
            if inflight_family_match:
                return {
                    "strategy": "join_inflight",
                    "scope": scope,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matched_job": inflight_family_match,
                    "request_family_match_explanation": self._build_dispatch_request_family_match_explanation(
                        request_payload=request_payload,
                        matched_job=inflight_family_match,
                        strategy="join_inflight",
                    ),
                }

        if bool(context.get("allow_result_reuse", True)):
            completed_match = self.store.find_latest_job_by_request_signature(
                request_signature_value=request_sig,
                target_company=request.target_company,
                statuses=["completed"],
                requester_id=requester_id,
                tenant_id=tenant_id,
                scope=scope,
            )
            registry_snapshot_reuse_match: dict[str, Any] = {}
            if completed_match:
                registry_snapshot_reuse_match = self._maybe_prefer_registry_snapshot_over_completed_match(
                    request=request,
                    context=context,
                    completed_match=completed_match,
                )
                if not registry_snapshot_reuse_match:
                    return {
                        "strategy": "reuse_completed",
                        "scope": scope,
                        "request_signature": request_sig,
                        "request_family_signature": request_family_sig,
                        "matched_job": completed_match,
                        "request_family_match_explanation": self._build_dispatch_request_family_match_explanation(
                            request_payload=request_payload,
                            matched_job=completed_match,
                            strategy="reuse_completed",
                        ),
                    }
            if not registry_snapshot_reuse_match:
                registry_snapshot_reuse_match = self._resolve_organization_asset_registry_snapshot_reuse_match(
                    request, context
                )
            if registry_snapshot_reuse_match:
                if not dict(registry_snapshot_reuse_match.get("matched_job") or {}):
                    snapshot_reuse_match = self._resolve_snapshot_reuse_job_match(request, context)
                    snapshot_matched_job = dict(snapshot_reuse_match.get("matched_job") or {})
                    if snapshot_matched_job and (
                        str(snapshot_reuse_match.get("matched_snapshot_id") or "").strip()
                        == str(registry_snapshot_reuse_match.get("matched_snapshot_id") or "").strip()
                    ):
                        registry_snapshot_reuse_match["matched_job"] = snapshot_matched_job
                        explanation = dict(registry_snapshot_reuse_match.get("request_family_match_explanation") or {})
                        if explanation:
                            explanation["matched_job_id"] = str(snapshot_matched_job.get("job_id") or "")
                            explanation["matched_job_status"] = str(snapshot_matched_job.get("status") or "")
                            explanation["matched_job_stage"] = str(snapshot_matched_job.get("stage") or "")
                            registry_snapshot_reuse_match["request_family_match_explanation"] = explanation
                return registry_snapshot_reuse_match
            snapshot_reuse_match = self._resolve_snapshot_reuse_job_match(request, context)
            if snapshot_reuse_match:
                return snapshot_reuse_match
            company_asset_snapshot_reuse_match = self._resolve_small_org_company_asset_snapshot_reuse_job_match(
                request, context
            )
            if company_asset_snapshot_reuse_match:
                return company_asset_snapshot_reuse_match

        return {
            "strategy": "new_job",
            "scope": scope,
            "request_signature": request_sig,
            "request_family_signature": request_family_sig,
            "matched_job": {},
        }

    def _build_workflow_lane_preview(
        self,
        *,
        request: JobRequest,
        asset_reuse_plan: dict[str, Any],
        organization_execution_profile: dict[str, Any],
    ) -> dict[str, Any]:
        lane_semantics = compile_asset_reuse_lane_semantics(
            request=request,
            asset_reuse_plan=asset_reuse_plan,
            organization_execution_profile=organization_execution_profile,
        )
        return {
            "employment_statuses": list(lane_semantics.get("employment_statuses") or []),
            "current": dict(lane_semantics.get("current_lane") or {}),
            "former": dict(lane_semantics.get("former_lane") or {}),
        }

    def _create_workflow_job(
        self,
        request: JobRequest,
        plan,
        *,
        dispatch_context: dict[str, Any] | None = None,
        runtime_execution_mode: str = "hosted",
        execution_bundle_payload: dict[str, Any] | None = None,
        plan_review_session: dict[str, Any] | None = None,
    ) -> str:
        job_id = uuid.uuid4().hex[:12]
        dispatch_context = dict(dispatch_context or {})
        execution_bundle = dict(execution_bundle_payload or {})
        if not execution_bundle:
            try:
                execution_bundle = self._build_execution_bundle(
                    request=request,
                    plan=plan,
                    effective_request=request,
                    source="workflow_job_queued",
                    plan_review_session=plan_review_session,
                )
            except Exception:
                execution_bundle = {}
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            execution_bundle_payload=execution_bundle,
            summary_payload={
                "message": "Workflow queued",
                "runtime_execution_mode": str(runtime_execution_mode or "hosted").strip().lower() or "hosted",
            },
            requester_id=str(dispatch_context.get("requester_id") or ""),
            tenant_id=str(dispatch_context.get("tenant_id") or ""),
            idempotency_key=str(dispatch_context.get("idempotency_key") or ""),
        )
        self.store.append_job_event(
            job_id,
            stage="planning",
            status="queued",
            detail="Workflow created from user request.",
            payload={
                "target_company": request.target_company,
                "retrieval_strategy": plan.retrieval_plan.strategy,
                "query_dispatch_scope": str(dispatch_context.get("scope") or ""),
                "requester_id": str(dispatch_context.get("requester_id") or ""),
                "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
                "runtime_execution_mode": str(runtime_execution_mode or "hosted").strip().lower() or "hosted",
            },
        )
        return job_id

    def _run_workflow(self, job_id: str, request: JobRequest, plan) -> None:
        try:
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="planning",
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                summary_payload={"message": "Workflow is running"},
            )
            with self.agent_runtime.traced_lane(
                job_id=job_id,
                request=request,
                plan_payload=plan.to_record(),
                runtime_mode="workflow",
                lane_id="triage_planner",
                span_name="planning_replay",
                stage="planning",
                input_payload={"retrieval_strategy": plan.retrieval_plan.strategy},
            ) as planning_span:
                self.store.append_job_event(job_id, "planning", "completed", "Planning stage completed.")
                self.agent_runtime.complete_span(
                    planning_span,
                    status="completed",
                    output_payload={"intent_summary": plan.intent_summary},
                    handoff_to_lane="acquisition_specialist",
                )
            self._run_workflow_from_acquisition(job_id, request, plan)
        except Exception as exc:
            self._mark_workflow_failed(job_id, request, plan, exc)

    def _run_workflow_from_acquisition(
        self,
        job_id: str,
        request: JobRequest,
        plan,
        *,
        resume_mode: bool = False,
    ) -> dict[str, Any]:
        bootstrap_summary = None
        job_summary = self._workflow_job_summary(job_id)
        acquisition_progress = _normalize_acquisition_progress_payload(job_summary.get("acquisition_progress"))
        acquisition_state = self._restore_acquisition_state(
            job_id=job_id,
            request=request,
            plan=plan,
            acquisition_progress=acquisition_progress,
        )
        current_stage = "acquiring"
        running_summary = dict(job_summary)
        running_summary["message"] = "Resuming acquisition tasks" if resume_mode else "Running acquisition tasks"
        running_summary.pop("blocked_task", None)
        running_summary.pop("blocked_task_id", None)
        if acquisition_progress:
            running_summary["acquisition_progress"] = acquisition_progress
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage=current_stage,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload=running_summary,
        )
        self.store.update_agent_runtime_session_status(job_id, "running")
        if resume_mode:
            self.store.append_job_event(
                job_id, "acquiring", "running", "Resuming blocked workflow after worker recovery."
            )
        completed_task_ids = self._effective_acquisition_completed_task_ids(
            request=request,
            plan=plan,
            acquisition_progress=acquisition_progress,
            acquisition_state=acquisition_state,
        )
        for task_index, task in enumerate(plan.acquisition_tasks):
            checkpoint_reusable = self._acquisition_task_checkpoint_reusable(task, acquisition_state)
            if task.task_id in completed_task_ids and checkpoint_reusable:
                acquisition_progress = self._record_reusable_acquisition_task_skip(
                    job_id=job_id,
                    request=request,
                    plan=plan,
                    task=task,
                    acquisition_state=acquisition_state,
                    resume_mode=resume_mode,
                    restored_from_checkpoint=True,
                    inferred_from_current_state=False,
                    detail="restored from acquisition checkpoint.",
                )
                if task.task_type == "enrich_public_web_signals":
                    acquisition_progress, advanced_directly_to_results = (
                        self._consume_remaining_reusable_acquisition_tasks_after_public_web_stage(
                            job_id=job_id,
                            request=request,
                            plan=plan,
                            acquisition_progress=acquisition_progress,
                            acquisition_state=acquisition_state,
                            completed_task_ids=completed_task_ids,
                            remaining_tasks=plan.acquisition_tasks[task_index + 1 :],
                            resume_mode=resume_mode,
                        )
                    )
                    if advanced_directly_to_results:
                        break
                continue
            if checkpoint_reusable:
                completed_task_ids.add(task.task_id)
                acquisition_progress = self._record_reusable_acquisition_task_skip(
                    job_id=job_id,
                    request=request,
                    plan=plan,
                    task=task,
                    acquisition_state=acquisition_state,
                    resume_mode=resume_mode,
                    restored_from_checkpoint=False,
                    inferred_from_current_state=True,
                    detail="satisfied by current acquisition state.",
                )
                if task.task_type == "enrich_public_web_signals":
                    acquisition_progress, advanced_directly_to_results = (
                        self._consume_remaining_reusable_acquisition_tasks_after_public_web_stage(
                            job_id=job_id,
                            request=request,
                            plan=plan,
                            acquisition_progress=acquisition_progress,
                            acquisition_state=acquisition_state,
                            completed_task_ids=completed_task_ids,
                            remaining_tasks=plan.acquisition_tasks[task_index + 1 :],
                            resume_mode=resume_mode,
                        )
                    )
                    if advanced_directly_to_results:
                        break
                continue
            if (
                request.target_company.strip().lower() == "anthropic"
                and task.task_type == "acquire_full_roster"
                and _should_use_local_anthropic_assets(request)
            ):
                bootstrap_summary = self.ensure_bootstrapped()
            lane_id = _lane_for_task(task, plan.to_record())
            with self.agent_runtime.traced_lane(
                job_id=job_id,
                request=request,
                plan_payload=plan.to_record(),
                runtime_mode="workflow",
                lane_id=lane_id,
                span_name=task.task_type,
                stage="acquiring",
                input_payload={"task": task.to_record(), "resume_mode": resume_mode},
                handoff_from_lane="triage_planner",
            ) as task_span:
                execution = self.acquisition_engine.execute_task(
                    task,
                    request,
                    request.target_company,
                    acquisition_state,
                    bootstrap_summary,
                )
                self.agent_runtime.complete_span(
                    task_span,
                    status=execution.status,
                    output_payload=execution.payload,
                    handoff_to_lane="enrichment_specialist" if task.task_type == "acquire_full_roster" else "",
                )
            acquisition_state.update(execution.state_updates)
            self._hydrate_acquisition_state_candidate_documents(acquisition_state)
            if execution.status == "completed":
                self._capture_inline_background_reconcile_cursor(
                    job_id=job_id,
                    task=task,
                    acquisition_state=acquisition_state,
                )
            self.store.append_job_event(
                job_id,
                stage=current_stage,
                status=execution.status,
                detail=f"{task.title}: {execution.detail}",
                payload=execution.payload,
            )
            if execution.status == "completed":
                acquisition_progress = self._record_acquisition_task_completion(
                    job_id=job_id,
                    request=request,
                    plan=plan,
                    task=task,
                    execution=execution,
                    acquisition_state=acquisition_state,
                )
                completed_task_ids.add(task.task_id)
                if task.task_type == "enrich_linkedin_profiles":
                    self._publish_stage1_preview_after_linkedin_stage(
                        job_id=job_id,
                        request=request,
                        plan=plan,
                        acquisition_state=acquisition_state,
                        acquisition_progress=acquisition_progress,
                    )
                elif task.task_type == "enrich_public_web_signals":
                    self._mark_public_web_stage_2_completed(
                        job_id=job_id,
                        request=request,
                        plan=plan,
                        acquisition_state=acquisition_state,
                        acquisition_progress=acquisition_progress,
                    )
                    acquisition_progress, advanced_directly_to_results = (
                        self._consume_remaining_reusable_acquisition_tasks_after_public_web_stage(
                            job_id=job_id,
                            request=request,
                            plan=plan,
                            acquisition_progress=acquisition_progress,
                            acquisition_state=acquisition_state,
                            completed_task_ids=completed_task_ids,
                            remaining_tasks=plan.acquisition_tasks[task_index + 1 :],
                            resume_mode=resume_mode,
                        )
                    )
                    if advanced_directly_to_results:
                        break
            if execution.status == "blocked" and task.blocking:
                summary = self._workflow_job_summary(job_id)
                summary["message"] = execution.detail
                summary["blocked_task"] = task.task_type
                summary["blocked_task_id"] = task.task_id
                if acquisition_progress:
                    summary["acquisition_progress"] = acquisition_progress
                self.store.save_job(
                    job_id=job_id,
                    job_type="workflow",
                    status="blocked",
                    stage="acquiring",
                    request_payload=request.to_record(),
                    plan_payload=plan.to_record(),
                    summary_payload=summary,
                )
                self.store.update_agent_runtime_session_status(job_id, "blocked")
                return {
                    "status": "blocked",
                    "blocked_task": task.task_type,
                    "detail": execution.detail,
                }

        direct_asset_population_finalization = self._resolve_asset_population_direct_finalization_context(
            request=request,
            plan=plan,
            acquisition_state=acquisition_state,
        )
        pre_retrieval_refresh = self._refresh_running_workflow_before_retrieval(
            job_id=job_id,
            request=request,
            plan=plan,
            acquisition_state=acquisition_state,
            include_materialization_refresh=not bool(direct_asset_population_finalization),
            defer_harvest_prefetch_refresh_to_background=bool(direct_asset_population_finalization),
        )
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            acquisition_state["pre_retrieval_refresh"] = pre_retrieval_refresh
        elif (
            direct_asset_population_finalization
            and str(pre_retrieval_refresh.get("reason") or "").strip().lower()
            == "harvest_prefetch_refresh_deferred_to_background"
        ):
            existing_background_snapshot_materialization = dict(
                direct_asset_population_finalization.get("background_snapshot_materialization") or {}
            )
            deferred_harvest_refresh = self._build_deferred_background_snapshot_materialization(
                acquisition_state=acquisition_state,
                reason="harvest_prefetch_refresh_deferred_to_background",
                target_company=request.target_company,
                force=True,
                metadata={
                    "harvest_prefetch_worker_count": int(pre_retrieval_refresh.get("harvest_prefetch_worker_count") or 0),
                    "deferred_components": ["harvest_prefetch_refresh"],
                },
            )
            if deferred_harvest_refresh:
                merged_components = [
                    str(item).strip()
                    for item in list(existing_background_snapshot_materialization.get("deferred_components") or [])
                    if str(item).strip()
                ]
                merged_components.extend(
                    str(item).strip()
                    for item in list(deferred_harvest_refresh.get("deferred_components") or [])
                    if str(item).strip()
                )
                merged_background_snapshot_materialization = {
                    **existing_background_snapshot_materialization,
                    **deferred_harvest_refresh,
                }
                if merged_components:
                    merged_background_snapshot_materialization["deferred_components"] = list(
                        dict.fromkeys(merged_components)
                    )
                direct_asset_population_finalization[
                    "background_snapshot_materialization"
                ] = merged_background_snapshot_materialization

        if self._should_use_two_stage_workflow_analysis(request) and self._should_require_stage2_confirmation(request):
            blocked_summary = self._workflow_job_summary(job_id)
            if self._stage1_preview_ready(blocked_summary):
                blocked_summary = self._build_post_acquisition_workflow_summary(
                    base_summary=blocked_summary,
                    message="Public Web Stage 2 acquisition completed. Awaiting user approval for stage 2 AI analysis.",
                    acquisition_progress=acquisition_progress,
                    pre_retrieval_refresh=pre_retrieval_refresh,
                    awaiting_stage2_confirmation=True,
                )
                self._save_workflow_job_state(
                    job_id=job_id,
                    request=request,
                    plan=plan,
                    status="blocked",
                    stage="retrieving",
                    summary_payload=blocked_summary,
                )
                self.store.update_agent_runtime_session_status(job_id, "blocked")
                self.store.append_job_event(
                    job_id,
                    "retrieving",
                    "blocked",
                    "Public Web Stage 2 acquisition completed; awaiting user approval for stage 2 AI analysis.",
                    {
                        "analysis_stage": "stage_2_final",
                        "awaiting_user_action": "continue_stage2",
                        "stage1_preview_artifact_path": str(
                            dict(blocked_summary.get("stage1_preview") or {}).get("artifact_path") or ""
                        ),
                    },
                )
                return {
                    "status": "blocked",
                    "stage": "retrieving",
                    "reason": "awaiting_stage2_confirmation",
                    "preview_artifact": dict(blocked_summary.get("stage1_preview") or {}),
                }

        return self._complete_workflow_after_acquisition(
            job_id=job_id,
            request=request,
            plan=plan,
            acquisition_progress=acquisition_progress,
            acquisition_state=acquisition_state,
            pre_retrieval_refresh=pre_retrieval_refresh,
            preparation_message=(
                "Preparing asset population finalization"
                if direct_asset_population_finalization
                else "Preparing final retrieval"
            ),
            retrieval_message=(
                "Finalizing asset population"
                if direct_asset_population_finalization
                else "Retrieving candidates"
            ),
            retrieval_runtime_mode=(
                "direct_asset_population_finalization" if direct_asset_population_finalization else ""
            ),
            candidate_source_override=dict(
                direct_asset_population_finalization.get("candidate_source") or {}
            ),
            background_snapshot_materialization=dict(
                direct_asset_population_finalization.get("background_snapshot_materialization") or {}
            ),
        )

    def _resolve_asset_population_direct_finalization_context(
        self,
        *,
        request: JobRequest,
        plan: Any,
        acquisition_state: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot_id = str(acquisition_state.get("snapshot_id") or "").strip()
        if not snapshot_id:
            return {}
        current_candidate_source = self._load_retrieval_candidate_source(
            request,
            snapshot_id=snapshot_id,
            allow_materialization_fallback=False,
        )
        current_direct_finalization_context: dict[str, Any] = {}
        if candidate_source_is_snapshot_authoritative(current_candidate_source):
            current_direct_finalization_context = self._asset_population_fast_path_context(
                request=request,
                plan=plan,
                candidate_source=current_candidate_source,
                runtime_policy={
                    "workflow_snapshot_id": snapshot_id,
                    "analysis_stage": "stage_2_final",
                    "mode": "direct_asset_population_finalization",
                },
            )
        current_background_snapshot_materialization = (
            self._build_deferred_background_snapshot_materialization(
                acquisition_state=acquisition_state,
                reason="current_snapshot_materialization_deferred_to_background",
                target_company=request.target_company,
                metadata={"deferred_components": ["snapshot_materialization"]},
            )
            if current_direct_finalization_context
            else {}
        )
        plan_payload = _plan_payload(plan)
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or getattr(plan, "asset_reuse_plan", {}) or {})
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=current_candidate_source,
        )
        baseline_snapshot_id = str(
            organization_execution_profile.get("source_snapshot_id")
            or asset_reuse_plan.get("baseline_snapshot_id")
            or dict(request.execution_preferences or {}).get("delta_baseline_snapshot_id")
            or ""
        ).strip()
        if not current_direct_finalization_context:
            return {}
        should_prefer_authoritative_baseline = bool(
            baseline_snapshot_id
            and baseline_snapshot_id != snapshot_id
            and bool(asset_reuse_plan.get("baseline_reuse_available"))
            and bool(asset_reuse_plan.get("requires_delta_acquisition"))
            and str(request.target_scope or "").strip().lower() == "full_company_asset"
            and str(
                dict(current_direct_finalization_context.get("effective_execution_semantics") or {}).get(
                    "default_results_mode"
                )
                or ""
            ).strip().lower()
            == "asset_population"
        )
        if should_prefer_authoritative_baseline:
            baseline_candidate_source = self._load_retrieval_candidate_source(
                request,
                snapshot_id=baseline_snapshot_id,
                allow_materialization_fallback=True,
            )
            baseline_candidate_count = len(list(baseline_candidate_source.get("candidates") or []))
            current_candidate_count = len(list(current_candidate_source.get("candidates") or []))
            if (
                candidate_source_is_snapshot_authoritative(baseline_candidate_source)
                and baseline_candidate_count > current_candidate_count
            ):
                patched_candidate_source = self._build_asset_population_generation_patch_candidate_source(
                    request=request,
                    current_candidate_source=current_candidate_source,
                    baseline_candidate_source=baseline_candidate_source,
                    base_generation_key=str(
                        organization_execution_profile.get("source_generation_key")
                        or asset_reuse_plan.get("baseline_generation_key")
                        or ""
                    ).strip(),
                )
                if patched_candidate_source:
                    patch_direct_finalization_context = self._asset_population_fast_path_context(
                        request=request,
                        plan=plan,
                        candidate_source=patched_candidate_source,
                        runtime_policy={
                            "workflow_snapshot_id": snapshot_id,
                            "analysis_stage": "stage_2_final",
                            "mode": "generation_member_patch_finalization",
                        },
                    )
                    if not patch_direct_finalization_context:
                        return {}
                    background_snapshot_materialization = self._build_deferred_background_snapshot_materialization(
                        acquisition_state=acquisition_state,
                        authoritative_snapshot_id=baseline_snapshot_id,
                        reason="delta_snapshot_materialization_deferred_to_background",
                        target_company=request.target_company,
                        force=True,
                        metadata={
                            "candidate_count_hint": current_candidate_count,
                            "authoritative_candidate_count_hint": baseline_candidate_count,
                            "deferred_components": [
                                "snapshot_materialization",
                                "baseline_generation_patch",
                            ],
                        },
                    )
                    return {
                        "snapshot_id": snapshot_id,
                        "candidate_source": patched_candidate_source,
                        **patch_direct_finalization_context,
                        "background_snapshot_materialization": background_snapshot_materialization,
                    }
                return {}
        result = {
            "snapshot_id": snapshot_id,
            "candidate_source": current_candidate_source,
            **current_direct_finalization_context,
        }
        if current_background_snapshot_materialization:
            result["background_snapshot_materialization"] = current_background_snapshot_materialization
        return result

    def _complete_workflow_after_acquisition(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_progress: dict[str, Any],
        acquisition_state: dict[str, Any],
        pre_retrieval_refresh: dict[str, Any],
        preparation_message: str,
        retrieval_message: str,
        retrieval_runtime_mode: str = "",
        candidate_source_override: dict[str, Any] | None = None,
        background_snapshot_materialization: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current_stage = "retrieving"
        stage2_final_started_at = _utc_now_iso()
        retrieving_summary = self._build_post_acquisition_workflow_summary(
            base_summary=self._workflow_job_summary(job_id),
            message=preparation_message,
            acquisition_progress=acquisition_progress,
            pre_retrieval_refresh=pre_retrieval_refresh,
            stage2_final_started_at=stage2_final_started_at,
        )
        self._save_workflow_job_state(
            job_id=job_id,
            request=request,
            plan=plan,
            status="running",
            stage=current_stage,
            summary_payload=retrieving_summary,
        )
        self.store.append_job_event(job_id, "retrieving", "running", "Retrieval stage started.")
        outreach_layering_summary = self._run_outreach_layering_after_acquisition(
            job_id=job_id,
            request=request,
            acquisition_state=acquisition_state,
            allow_ai=False,
            analysis_stage_label="stage_2_final",
            event_stage=current_stage,
        )
        if outreach_layering_summary:
            acquisition_state["outreach_layering"] = outreach_layering_summary
        retrieving_summary = self._build_post_acquisition_workflow_summary(
            base_summary=self._workflow_job_summary(job_id),
            message=retrieval_message,
            acquisition_progress=acquisition_progress,
            pre_retrieval_refresh=pre_retrieval_refresh,
            stage2_final_started_at=stage2_final_started_at,
            outreach_layering=outreach_layering_summary,
        )
        self._save_workflow_job_state(
            job_id=job_id,
            request=request,
            plan=plan,
            status="running",
            stage=current_stage,
            summary_payload=retrieving_summary,
        )
        runtime_policy = {
            "workflow_snapshot_id": str(acquisition_state.get("snapshot_id") or "").strip(),
            "outreach_layering": dict(acquisition_state.get("outreach_layering") or {}),
        }
        if retrieval_runtime_mode:
            runtime_policy["mode"] = retrieval_runtime_mode
        if dict(background_snapshot_materialization or {}):
            runtime_policy["background_snapshot_materialization"] = dict(background_snapshot_materialization or {})
        artifact = self._execute_retrieval(
            job_id,
            request,
            plan,
            job_type="workflow",
            runtime_policy=runtime_policy,
            candidate_source_override=dict(candidate_source_override or {}),
            persist_job_state=False,
        )
        final_summary = self._persist_completed_workflow_summary(
            job_id=job_id,
            request=request,
            plan=plan,
            artifact=artifact,
            preserved_summary=retrieving_summary,
        )
        self.store.update_agent_runtime_session_status(job_id, "completed")
        self.store.append_job_event(job_id, "retrieving", "completed", "Retrieval stage completed.", final_summary)
        self.store.append_job_event(job_id, "completed", "completed", "Workflow completed.", final_summary)
        return {
            "status": "completed",
            "artifact": artifact,
        }

    def _workflow_job_summary(self, job_id: str) -> dict[str, Any]:
        return dict((self.store.get_job(job_id) or {}).get("summary") or {})

    def _consume_remaining_reusable_acquisition_tasks_after_public_web_stage(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_progress: dict[str, Any],
        acquisition_state: dict[str, Any],
        completed_task_ids: set[str],
        remaining_tasks: list[AcquisitionTask],
        resume_mode: bool,
    ) -> tuple[dict[str, Any], bool]:
        reusable_tasks: list[AcquisitionTask] = []
        non_reusable_tasks: list[AcquisitionTask] = []
        for remaining_task in list(remaining_tasks or []):
            if not isinstance(remaining_task, AcquisitionTask):
                continue
            if not self._acquisition_task_checkpoint_reusable(remaining_task, acquisition_state):
                non_reusable_tasks.append(remaining_task)
                continue
            reusable_tasks.append(remaining_task)
        if non_reusable_tasks:
            deferred_finalization_context = self._resolve_asset_population_direct_finalization_context(
                request=request,
                plan=plan,
                acquisition_state=acquisition_state,
            )
            background_snapshot_materialization = dict(
                deferred_finalization_context.get("background_snapshot_materialization") or {}
            )
            deferrable_task_types = {"normalize_asset_snapshot", "build_retrieval_index"}
            if (
                not background_snapshot_materialization
                or any(task.task_type not in deferrable_task_types for task in non_reusable_tasks)
            ):
                return acquisition_progress, False
            reusable_tasks.extend(non_reusable_tasks)
        if not reusable_tasks:
            return acquisition_progress, False

        updated_progress = acquisition_progress
        for remaining_task in reusable_tasks:
            restored_from_checkpoint = remaining_task.task_id in completed_task_ids
            completed_task_ids.add(remaining_task.task_id)
            updated_progress = self._record_reusable_acquisition_task_skip(
                job_id=job_id,
                request=request,
                plan=plan,
                task=remaining_task,
                acquisition_state=acquisition_state,
                resume_mode=resume_mode,
                restored_from_checkpoint=restored_from_checkpoint,
                inferred_from_current_state=not restored_from_checkpoint,
                detail=(
                    "restored from acquisition checkpoint."
                    if restored_from_checkpoint
                    else (
                        "deferred to background snapshot materialization while final results use the authoritative baseline snapshot."
                        if remaining_task.task_type in {"normalize_asset_snapshot", "build_retrieval_index"}
                        else "satisfied by direct reusable snapshot finalization."
                    )
                ),
            )
        return updated_progress, True

    def _record_reusable_acquisition_task_skip(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        task: AcquisitionTask,
        acquisition_state: dict[str, Any],
        resume_mode: bool,
        restored_from_checkpoint: bool,
        inferred_from_current_state: bool,
        detail: str,
    ) -> dict[str, Any]:
        checkpoint_payload = {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "resume_mode": resume_mode,
            "restored_from_checkpoint": restored_from_checkpoint,
            "inferred_from_current_state": inferred_from_current_state,
        }
        self.store.append_job_event(
            job_id,
            stage="acquiring",
            status="skipped",
            detail=f"{task.title}: {detail}",
            payload=checkpoint_payload,
        )
        acquisition_progress = self._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=task,
            execution=SimpleNamespace(
                status="skipped",
                detail=detail,
                payload=checkpoint_payload,
            ),
            acquisition_state=acquisition_state,
        )
        if task.task_type == "enrich_linkedin_profiles":
            self._publish_stage1_preview_after_linkedin_stage(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state=acquisition_state,
                acquisition_progress=acquisition_progress,
            )
        elif task.task_type == "enrich_public_web_signals":
            self._mark_public_web_stage_2_completed(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state=acquisition_state,
                acquisition_progress=acquisition_progress,
            )
        return acquisition_progress

    def _record_acquisition_task_completion(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        task: AcquisitionTask,
        execution: Any,
        acquisition_state: dict[str, Any],
    ) -> dict[str, Any]:
        summary = self._workflow_job_summary(job_id)
        progress = _normalize_acquisition_progress_payload(summary.get("acquisition_progress"))
        completed_task_ids = [
            str(item).strip()
            for item in list(progress.get("completed_task_ids") or [])
            if str(item).strip() and str(item).strip() != task.task_id
        ]
        completed_task_ids.append(task.task_id)
        tasks_payload = dict(progress.get("tasks") or {})
        tasks_payload[task.task_id] = {
            "task_id": task.task_id,
            "task_type": task.task_type,
            "title": task.title,
            "status": str(execution.status or ""),
            "detail": str(execution.detail or ""),
            "payload": dict(execution.payload or {}),
            "completed_at": _utc_now_iso(),
        }
        phases_payload = dict(progress.get("phases") or {})
        phase_id = str(task.metadata.get("acquisition_phase") or "").strip()
        phase_title = str(task.metadata.get("acquisition_phase_title") or phase_id.replace("_", " ").title()).strip()
        if phase_id:
            ordered_phase_task_ids = [
                acquisition_task.task_id
                for acquisition_task in getattr(plan, "acquisition_tasks", [])
                if isinstance(acquisition_task, AcquisitionTask)
                and str(acquisition_task.metadata.get("acquisition_phase") or "").strip() == phase_id
            ]
            phase_completed_task_ids = [
                str(item).strip()
                for item in list(dict(phases_payload.get(phase_id) or {}).get("completed_task_ids") or [])
                if str(item).strip()
                and str(item).strip() in ordered_phase_task_ids
                and str(item).strip() != task.task_id
            ]
            phase_completed_task_ids.append(task.task_id)
            phases_payload[phase_id] = {
                "phase_id": phase_id,
                "title": phase_title,
                "task_ids": ordered_phase_task_ids,
                "completed_task_ids": phase_completed_task_ids,
                "completed_task_count": len(phase_completed_task_ids),
                "task_count": len(ordered_phase_task_ids),
                "status": "completed"
                if ordered_phase_task_ids and len(phase_completed_task_ids) >= len(ordered_phase_task_ids)
                else "running",
                "last_completed_task_id": task.task_id,
                "completed_at": _utc_now_iso(),
            }
        progress.update(
            {
                "status": "running",
                "completed_task_ids": completed_task_ids,
                "last_completed_task_id": task.task_id,
                "last_completed_task_type": task.task_type,
                "active_phase_id": phase_id,
                "active_phase_title": phase_title if phase_id else "",
                "latest_state": _serialize_acquisition_state_payload(acquisition_state),
                "tasks": tasks_payload,
                "phases": phases_payload,
            }
        )
        summary["message"] = f"Acquisition checkpoint updated after {task.title}."
        summary["acquisition_progress"] = progress
        summary.pop("blocked_task", None)
        summary.pop("blocked_task_id", None)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
            summary_payload=summary,
        )
        return progress

    def _restore_acquisition_state(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_progress: dict[str, Any],
    ) -> dict[str, Any]:
        state = self._build_acquisition_state(job_id, _plan_payload(plan))
        latest_state_payload = dict(acquisition_progress.get("latest_state") or {})
        restored_state = _deserialize_acquisition_state_payload(latest_state_payload)
        state.update(restored_state)
        task_progress = {
            str(task_id or "").strip(): dict(payload or {})
            for task_id, payload in dict(acquisition_progress.get("tasks") or {}).items()
            if str(task_id or "").strip()
        }
        completed_task_types = {
            str(task_payload.get("task_type") or "").strip()
            for task_payload in task_progress.values()
            if str(task_payload.get("status") or "").strip() in {"completed", "skipped"}
        }
        if "enrich_linkedin_profiles" in completed_task_types:
            state["linkedin_stage_completed"] = True
        if "enrich_public_web_signals" in completed_task_types:
            state["public_web_stage_completed"] = True
        reusable_snapshot_state = self._load_reusable_snapshot_state(request)
        for key, value in reusable_snapshot_state.items():
            existing_value = state.get(key)
            if existing_value in (None, "", [], {}):
                state[key] = value
        snapshot_dir = state.get("snapshot_dir")
        self._hydrate_acquisition_state_candidate_documents(state)
        if not isinstance(state.get("roster_snapshot"), CompanyRosterSnapshot) and isinstance(snapshot_dir, Path):
            roster_snapshot = self._restore_roster_snapshot_from_snapshot_dir(
                snapshot_dir=snapshot_dir,
                identity=state.get("company_identity"),
            )
            if roster_snapshot is not None:
                state["roster_snapshot"] = roster_snapshot
                if not isinstance(state.get("company_identity"), CompanyIdentity):
                    state["company_identity"] = roster_snapshot.company_identity
        manifest_path = state.get("manifest_path")
        if not isinstance(manifest_path, Path):
            if isinstance(snapshot_dir, Path):
                candidate_manifest_path = snapshot_dir / "manifest.json"
                if candidate_manifest_path.exists():
                    state["manifest_path"] = candidate_manifest_path
        if not isinstance(state.get("search_seed_snapshot"), SearchSeedSnapshot) and isinstance(snapshot_dir, Path):
            search_seed_snapshot = _restore_search_seed_snapshot_from_snapshot_dir(
                snapshot_dir=snapshot_dir,
                identity=state.get("company_identity"),
            )
            if search_seed_snapshot is not None:
                state["search_seed_snapshot"] = search_seed_snapshot
        return state

    def _hydrate_acquisition_state_candidate_documents(self, state: dict[str, Any]) -> None:
        snapshot_dir = state.get("snapshot_dir")
        candidate_doc_path = state.get("candidate_doc_path")
        if not isinstance(candidate_doc_path, Path) and isinstance(snapshot_dir, Path):
            candidate_doc_candidate = snapshot_dir / "candidate_documents.json"
            if candidate_doc_candidate.exists():
                state["candidate_doc_path"] = candidate_doc_candidate
                candidate_doc_path = candidate_doc_candidate
        if not isinstance(candidate_doc_path, Path):
            return
        candidate_document_state = _load_candidate_document_state(candidate_doc_path)
        if candidate_document_state:
            state.update(candidate_document_state)
        if bool(state.get("linkedin_stage_completed")) and not isinstance(
            state.get("linkedin_stage_candidate_doc_path"), Path
        ):
            linkedin_stage_candidate_doc_path = (
                snapshot_dir / "candidate_documents.linkedin_stage_1.json"
                if isinstance(snapshot_dir, Path)
                else candidate_doc_path
            )
            state["linkedin_stage_candidate_doc_path"] = (
                linkedin_stage_candidate_doc_path
                if isinstance(linkedin_stage_candidate_doc_path, Path) and linkedin_stage_candidate_doc_path.exists()
                else candidate_doc_path
            )
        if bool(state.get("public_web_stage_completed")) and not isinstance(
            state.get("public_web_stage_candidate_doc_path"),
            Path,
        ):
            public_web_stage_candidate_doc_path = (
                snapshot_dir / "candidate_documents.public_web_stage_2.json"
                if isinstance(snapshot_dir, Path)
                else candidate_doc_path
            )
            state["public_web_stage_candidate_doc_path"] = (
                public_web_stage_candidate_doc_path
                if isinstance(public_web_stage_candidate_doc_path, Path)
                and public_web_stage_candidate_doc_path.exists()
                else candidate_doc_path
            )

    def _restore_roster_snapshot_from_snapshot_dir(
        self,
        *,
        snapshot_dir: Path,
        identity: Any,
    ) -> CompanyRosterSnapshot | None:
        if not snapshot_dir.exists():
            return None

        effective_identity = identity if isinstance(identity, CompanyIdentity) else None
        roster_layouts = (
            {
                "summary_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json",
                "merged_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_merged.json",
                "visible_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_visible.json",
                "headless_path": snapshot_dir / "harvest_company_employees" / "harvest_company_employees_headless.json",
            },
            {
                "summary_path": snapshot_dir / "linkedin_company_people_summary.json",
                "merged_path": snapshot_dir / "linkedin_company_people_all.json",
                "visible_path": snapshot_dir / "linkedin_company_people_visible.json",
                "headless_path": snapshot_dir / "linkedin_company_people_headless.json",
            },
        )
        for layout in roster_layouts:
            summary_path = Path(layout["summary_path"])
            if not summary_path.exists():
                continue
            summary_payload = _read_json_dict(summary_path)
            payload_identity = _company_identity_from_record(dict(summary_payload.get("company_identity") or {}))
            restored_identity = effective_identity or payload_identity
            if restored_identity is None:
                continue
            restored = _restore_company_roster_snapshot(
                {
                    "snapshot_id": str(summary_payload.get("snapshot_id") or snapshot_dir.name),
                    "target_company": str(summary_payload.get("target_company") or restored_identity.canonical_name),
                    "company_identity": restored_identity.to_record(),
                    "snapshot_dir": str(snapshot_dir),
                    "merged_path": str(layout["merged_path"]),
                    "visible_path": str(layout["visible_path"]),
                    "headless_path": str(layout["headless_path"]),
                    "summary_path": str(summary_path),
                    "accounts_used": list(summary_payload.get("accounts_used") or []),
                    "errors": list(summary_payload.get("errors") or []),
                    "stop_reason": str(summary_payload.get("stop_reason") or ""),
                }
            )
            if restored is not None:
                return restored

        if effective_identity is None:
            return None

        harvest_dir = snapshot_dir / "harvest_company_employees"
        segmented_snapshot = self._restore_segmented_roster_snapshot_from_snapshot_dir(
            snapshot_dir=snapshot_dir,
            harvest_dir=harvest_dir,
            identity=effective_identity,
        )
        if segmented_snapshot is not None:
            return segmented_snapshot

        queue_summary_path = harvest_dir / "harvest_company_employees_queue_summary.json"
        raw_path = harvest_dir / "harvest_company_employees_raw.json"
        if not queue_summary_path.exists() and not raw_path.exists():
            return None

        queue_summary = _read_json_dict(queue_summary_path)
        queue_status = str(queue_summary.get("status") or "").strip().lower()
        artifact_paths = dict(queue_summary.get("artifact_paths") or {})
        dataset_items_path_value = str(
            artifact_paths.get("dataset_items") or (harvest_dir / "harvest_company_employees_queue_dataset_items.json")
        ).strip()
        dataset_items_path = Path(dataset_items_path_value).expanduser() if dataset_items_path_value else None
        if queue_summary_path.exists() and queue_status not in {"", "completed"} and not raw_path.exists():
            return None
        if (
            queue_summary_path.exists()
            and queue_status == "completed"
            and not raw_path.exists()
            and (dataset_items_path is None or not dataset_items_path.exists())
        ):
            return None

        try:
            return self.acquisition_engine.harvest_company_connector.fetch_company_roster(
                effective_identity,
                snapshot_dir,
                max_pages=max(1, int(queue_summary.get("requested_pages") or 1)),
                page_limit=max(1, int(queue_summary.get("requested_item_limit") or 25)),
                company_filters=dict(queue_summary.get("company_filters") or {}),
                allow_shared_provider_cache=False,
            )
        except Exception:
            return None

    def _restore_segmented_roster_snapshot_from_snapshot_dir(
        self,
        *,
        snapshot_dir: Path,
        harvest_dir: Path,
        identity: CompanyIdentity,
    ) -> CompanyRosterSnapshot | None:
        plan_path = harvest_dir / "adaptive_shard_plan.json"
        if not plan_path.exists():
            return None

        plan_payload = _read_json_dict(plan_path)
        shards = _normalize_company_employee_shards(plan_payload.get("shards"))
        if not shards:
            return None

        shard_root = harvest_dir / "shards"
        available_shards: list[dict[str, Any]] = []
        for shard in shards:
            shard_id = str(shard.get("shard_id") or "").strip()
            if not shard_id:
                continue
            shard_harvest_dir = shard_root / shard_id / "harvest_company_employees"
            raw_path = shard_harvest_dir / "harvest_company_employees_raw.json"
            if raw_path.exists():
                available_shards.append(dict(shard))
                continue

            queue_summary_path = shard_harvest_dir / "harvest_company_employees_queue_summary.json"
            if not queue_summary_path.exists():
                continue

            queue_summary = _read_json_dict(queue_summary_path)
            if str(queue_summary.get("status") or "").strip().lower() != "completed":
                continue

            artifact_paths = dict(queue_summary.get("artifact_paths") or {})
            dataset_items_path_value = str(
                artifact_paths.get("dataset_items")
                or (shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json")
            ).strip()
            dataset_items_path = Path(dataset_items_path_value).expanduser() if dataset_items_path_value else None
            if dataset_items_path is None or not dataset_items_path.exists():
                continue
            available_shards.append(dict(shard))

        if not available_shards:
            return None

        try:
            return self.acquisition_engine._fetch_segmented_harvest_company_roster(
                identity=identity,
                snapshot_dir=snapshot_dir,
                shards=available_shards,
                allow_shared_provider_cache=False,
                expected_shard_count=len(shards),
                summary_stop_reason=(
                    "completed_segmented" if len(available_shards) >= len(shards) else "partial_segmented"
                ),
            )
        except Exception:
            return None

    def _effective_acquisition_completed_task_ids(
        self,
        *,
        request: JobRequest,
        plan: Any,
        acquisition_progress: dict[str, Any],
        acquisition_state: dict[str, Any],
    ) -> set[str]:
        completed_task_ids = {
            str(item).strip()
            for item in list(acquisition_progress.get("completed_task_ids") or [])
            if str(item).strip()
        }
        latest_state_payload = dict(acquisition_progress.get("latest_state") or {})
        execution_preferences = dict(request.execution_preferences or {})
        should_infer_checkpoint_completion = bool(latest_state_payload) or bool(
            str(execution_preferences.get("reuse_snapshot_id") or "").strip()
        )
        if not should_infer_checkpoint_completion:
            return completed_task_ids
        execution_preferences = dict(request.execution_preferences or {})
        reused_snapshot_checkpoint = bool(str(execution_preferences.get("reuse_snapshot_id") or "").strip())
        legacy_multisource_checkpoint = "enrich-multisource-profiles" in completed_task_ids or any(
            str(item).strip() == "enrich-multisource-profiles"
            for item in list(dict(acquisition_progress.get("tasks") or {}).keys())
        )
        if reused_snapshot_checkpoint or legacy_multisource_checkpoint:
            candidate_doc_path = acquisition_state.get("candidate_doc_path")
            if isinstance(candidate_doc_path, Path) and candidate_doc_path.exists():
                for task in getattr(plan, "acquisition_tasks", []):
                    if not isinstance(task, AcquisitionTask):
                        continue
                    if task.task_type in {"enrich_linkedin_profiles", "enrich_public_web_signals"}:
                        completed_task_ids.add(task.task_id)
        for task in getattr(plan, "acquisition_tasks", []):
            if not isinstance(task, AcquisitionTask):
                continue
            if self._acquisition_task_checkpoint_reusable(task, acquisition_state):
                completed_task_ids.add(task.task_id)
        return completed_task_ids

    def _resolved_acquisition_snapshot_dir(self, acquisition_state: dict[str, Any]) -> Path | None:
        snapshot_dir = acquisition_state.get("snapshot_dir")
        if isinstance(snapshot_dir, Path):
            return snapshot_dir
        manifest_path = acquisition_state.get("manifest_path")
        if isinstance(manifest_path, Path):
            return manifest_path.parent
        return None

    def _build_deferred_background_snapshot_materialization(
        self,
        *,
        acquisition_state: dict[str, Any],
        reason: str,
        target_company: str = "",
        authoritative_snapshot_id: str = "",
        force: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot_dir = self._resolved_acquisition_snapshot_dir(acquisition_state)
        snapshot_id = str(acquisition_state.get("snapshot_id") or "").strip()
        if not isinstance(snapshot_dir, Path):
            normalized_target_company = str(target_company or "").strip()
            if normalized_target_company and snapshot_id:
                snapshot_dir = resolve_company_snapshot_dir(
                    self.runtime_dir,
                    target_company=normalized_target_company,
                    snapshot_id=snapshot_id,
                )
        if not isinstance(snapshot_dir, Path):
            return {}
        if not snapshot_id:
            snapshot_id = str(snapshot_dir.name or "").strip()
        if not snapshot_id:
            return {}
        readiness = self._snapshot_artifact_readiness(snapshot_dir)
        materialization_refresh_required = bool(readiness.get("materialization_refresh_required"))
        retrieval_index_refresh_required = bool(readiness.get("retrieval_index_refresh_required"))
        if not force and not materialization_refresh_required and not retrieval_index_refresh_required:
            return {}
        payload: dict[str, Any] = {
            "status": "deferred",
            "snapshot_id": snapshot_id,
            "reason": str(reason or "").strip() or "snapshot_materialization_deferred_to_background",
            "materialization_refresh_required": materialization_refresh_required,
            "retrieval_index_refresh_required": retrieval_index_refresh_required,
            "candidate_doc_path": str(readiness.get("candidate_doc_path") or ""),
            "deferred_at": _utc_now_iso(),
        }
        authoritative_snapshot_id = str(authoritative_snapshot_id or "").strip()
        if authoritative_snapshot_id:
            payload["authoritative_snapshot_id"] = authoritative_snapshot_id
        if dict(metadata or {}):
            payload.update(dict(metadata or {}))
        return payload

    def _equivalent_baseline_snapshot_active(
        self,
        acquisition_state: dict[str, Any],
        *,
        snapshot_dir: Path | None = None,
    ) -> bool:
        if not bool(acquisition_state.get("equivalent_baseline_reused")):
            return False
        resolved_snapshot_dir = (
            snapshot_dir if isinstance(snapshot_dir, Path) else self._resolved_acquisition_snapshot_dir(acquisition_state)
        )
        if not isinstance(resolved_snapshot_dir, Path):
            return False
        current_snapshot_id = str(acquisition_state.get("snapshot_id") or resolved_snapshot_dir.name).strip()
        equivalent_baseline_snapshot_id = str(
            acquisition_state.get("equivalent_baseline_snapshot_id") or current_snapshot_id
        ).strip()
        return bool(equivalent_baseline_snapshot_id) and equivalent_baseline_snapshot_id == resolved_snapshot_dir.name

    def _reused_snapshot_serving_artifacts_ready(self, acquisition_state: dict[str, Any]) -> bool:
        if not bool(acquisition_state.get("reused_snapshot_checkpoint")):
            return False
        snapshot_dir = self._resolved_acquisition_snapshot_dir(acquisition_state)
        if not isinstance(snapshot_dir, Path):
            return False
        candidate_doc_path = acquisition_state.get("candidate_doc_path")
        if not isinstance(candidate_doc_path, Path) or not candidate_doc_path.exists():
            return False
        if candidate_doc_path.name not in {
            "materialized_candidate_documents.json",
            "reusable_candidate_documents.json",
        }:
            return False
        if "normalized_artifacts" not in candidate_doc_path.parts:
            return False
        artifact_dir = candidate_doc_path.parent
        return (artifact_dir / "manifest.json").exists() and (artifact_dir / "artifact_summary.json").exists()

    def _reused_snapshot_candidate_documents_ready_for_direct_reuse(
        self,
        acquisition_state: dict[str, Any],
    ) -> bool:
        if not bool(acquisition_state.get("reused_snapshot_checkpoint")):
            return False
        snapshot_dir = self._resolved_acquisition_snapshot_dir(acquisition_state)
        if not isinstance(snapshot_dir, Path):
            return False
        candidate_doc_path = acquisition_state.get("candidate_doc_path")
        if not isinstance(candidate_doc_path, Path) or not candidate_doc_path.exists():
            return False
        return candidate_doc_path.name in {
            "candidate_documents.json",
            "materialized_candidate_documents.json",
            "reusable_candidate_documents.json",
        }

    def _load_reusable_snapshot_state(self, request: JobRequest) -> dict[str, Any]:
        execution_preferences = dict(request.execution_preferences or {})
        snapshot_id = str(execution_preferences.get("reuse_snapshot_id") or "").strip()
        if not snapshot_id or not request.target_company.strip():
            return {}
        try:
            snapshot_payload = self._load_company_snapshot_candidate_documents_with_materialization_fallback(
                target_company=request.target_company,
                snapshot_id=snapshot_id,
                view="canonical_merged",
                allow_candidate_documents_fallback=True,
            )
        except CandidateArtifactError:
            return {}
        snapshot_dir = Path(str(snapshot_payload.get("snapshot_dir") or "")).expanduser()
        if not snapshot_dir.exists():
            return {}
        state: dict[str, Any] = {
            "snapshot_id": str(snapshot_payload.get("snapshot_id") or snapshot_id),
            "snapshot_dir": snapshot_dir,
            "reused_snapshot_checkpoint": True,
        }
        identity = _company_identity_from_record(dict(snapshot_payload.get("company_identity") or {}))
        if identity is not None:
            state["company_identity"] = identity
        candidate_doc_path = self._resolve_reusable_snapshot_candidate_doc_path(snapshot_payload)
        if candidate_doc_path is not None:
            state["candidate_doc_path"] = candidate_doc_path
        candidates = [item for item in list(snapshot_payload.get("candidates") or []) if isinstance(item, Candidate)]
        evidence = [
            item for item in list(snapshot_payload.get("evidence") or []) if isinstance(item, (EvidenceRecord, dict))
        ]
        if candidates:
            state["candidates"] = candidates
        if evidence:
            state["evidence"] = [
                item if isinstance(item, EvidenceRecord) else EvidenceRecord(**item)
                for item in evidence
                if isinstance(item, EvidenceRecord) or isinstance(item, dict)
            ]
        if candidate_doc_path is not None or candidates:
            state["linkedin_stage_completed"] = True
            state["public_web_stage_completed"] = True
        linkedin_stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        if linkedin_stage_candidate_doc_path.exists():
            state["linkedin_stage_candidate_doc_path"] = linkedin_stage_candidate_doc_path
        public_web_stage_candidate_doc_path = snapshot_dir / "candidate_documents.public_web_stage_2.json"
        if public_web_stage_candidate_doc_path.exists():
            state["public_web_stage_candidate_doc_path"] = public_web_stage_candidate_doc_path
        manifest_path = snapshot_dir / "manifest.json"
        if manifest_path.exists():
            state["manifest_path"] = manifest_path
        return state

    def _resolve_reusable_snapshot_candidate_doc_path(self, snapshot_payload: dict[str, Any]) -> Path | None:
        candidate_paths: list[Path] = []
        source_path_value = str(snapshot_payload.get("source_path") or "").strip()
        if source_path_value:
            source_path = Path(source_path_value).expanduser()
            if source_path.name in {"candidate_documents.json", "materialized_candidate_documents.json"}:
                candidate_paths.append(source_path)
        snapshot_payload_id = str(snapshot_payload.get("snapshot_id") or "").strip()
        source_payload = dict(snapshot_payload.get("source_payload") or {})
        snapshot_metadata = dict(source_payload.get("snapshot") or {})
        for source_snapshot in list(snapshot_metadata.get("source_snapshots") or []):
            source_snapshot_payload = dict(source_snapshot or {})
            source_snapshot_id = str(source_snapshot_payload.get("snapshot_id") or "").strip()
            source_snapshot_path_value = str(source_snapshot_payload.get("source_path") or "").strip()
            if source_snapshot_id != snapshot_payload_id or not source_snapshot_path_value:
                continue
            resolved_snapshot_dir = resolve_snapshot_dir_from_source_path(source_snapshot_path_value)
            if resolved_snapshot_dir is None:
                continue
            candidate_paths.extend(
                (
                    resolved_snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json",
                    resolved_snapshot_dir / "candidate_documents.json",
                )
            )
        snapshot_dir_value = str(snapshot_payload.get("snapshot_dir") or "").strip()
        if snapshot_dir_value:
            snapshot_dir = Path(snapshot_dir_value).expanduser()
            candidate_paths.extend(
                (
                    snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json",
                    snapshot_dir / "candidate_documents.json",
                )
            )
        seen: set[str] = set()
        for candidate_path in candidate_paths:
            key = str(candidate_path)
            if key in seen:
                continue
            seen.add(key)
            if candidate_path.exists():
                return candidate_path
        return None

    def _assess_acquisition_resume_readiness(
        self,
        *,
        job: dict[str, Any],
        blocked_task: str,
        workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        plan = hydrate_sourcing_plan(dict(job.get("plan") or {}))
        job_summary = dict(job.get("summary") or {})
        acquisition_progress = _normalize_acquisition_progress_payload(job_summary.get("acquisition_progress"))
        acquisition_state = self._restore_acquisition_state(
            job_id=str(job.get("job_id") or ""),
            request=request,
            plan=plan,
            acquisition_progress=acquisition_progress,
        )
        pending_worker_records = self._collect_acquisition_blocking_worker_records(
            workers=workers,
            blocked_task=blocked_task,
        )
        pending_workers = [_summarize_worker_runtime(worker) for worker in pending_worker_records]
        active_workers = [_summarize_worker_runtime(worker) for worker in _collect_active_acquisition_workers(workers)]
        baseline_ready, baseline_reason = _acquisition_state_is_baseline_ready(acquisition_state)
        if (
            _blocked_task_requires_roster_baseline(plan, blocked_task)
            and not _acquisition_state_has_roster_snapshot(acquisition_state)
            and not baseline_ready
        ):
            baseline_reason = "awaiting_roster_snapshot"
        requires_stage1_completion = blocked_task in {
            "enrich_linkedin_profiles",
            "enrich_profiles_multisource",
        } and not bool(acquisition_state.get("linkedin_stage_completed"))
        critical_pending_workers = [
            worker for worker in pending_worker_records if _worker_requires_blocking_acquisition_completion(worker)
        ]
        if _can_resume_full_roster_with_background_harvest_workers(
            blocked_task=blocked_task,
            baseline_ready=baseline_ready,
            critical_pending_workers=critical_pending_workers,
        ):
            critical_pending_workers = []
        if critical_pending_workers:
            return {
                "status": "waiting",
                "reason": "critical_pending_workers_remaining",
                "request": request,
                "plan": plan,
                "acquisition_state": acquisition_state,
                "baseline_ready": baseline_ready,
                "baseline_reason": baseline_reason,
                "pending_worker_count": len(pending_workers),
                "pending_workers": pending_workers[:10],
                "critical_pending_worker_count": len(critical_pending_workers),
            }
        if pending_worker_records and (not baseline_ready or requires_stage1_completion):
            return {
                "status": "waiting",
                "reason": "pending_workers_remaining" if not requires_stage1_completion else "linkedin_stage_pending",
                "request": request,
                "plan": plan,
                "acquisition_state": acquisition_state,
                "baseline_ready": baseline_ready,
                "baseline_reason": baseline_reason,
                "pending_worker_count": len(pending_workers),
                "pending_workers": pending_workers[:10],
            }
        if not blocked_task and active_workers and (not baseline_ready or requires_stage1_completion):
            return {
                "status": "waiting",
                "reason": "active_acquisition_workers_remaining"
                if not requires_stage1_completion
                else "linkedin_stage_pending",
                "request": request,
                "plan": plan,
                "acquisition_state": acquisition_state,
                "baseline_ready": baseline_ready,
                "baseline_reason": baseline_reason,
                "pending_worker_count": 0,
                "pending_workers": active_workers[:10],
            }
        return {
            "status": "ready",
            "request": request,
            "plan": plan,
            "acquisition_state": acquisition_state,
            "baseline_ready": baseline_ready,
            "baseline_reason": baseline_reason,
            "pending_worker_count": len(pending_workers),
            "pending_workers": pending_workers[:10],
        }

    def _search_seed_snapshot_has_employment_scope(
        self,
        snapshot: SearchSeedSnapshot | None,
        employment_scope: str,
    ) -> bool:
        if not isinstance(snapshot, SearchSeedSnapshot):
            return False
        normalized_scope = str(employment_scope or "").strip().lower()
        if normalized_scope not in {"current", "former"}:
            return bool(list(snapshot.entries or []))
        for entry in list(snapshot.entries or []):
            if not isinstance(entry, dict):
                continue
            entry_scope = str(
                entry.get("employment_status")
                or entry.get("employment_scope")
                or dict(entry.get("metadata") or {}).get("employment_status")
                or ""
            ).strip().lower()
            if entry_scope == normalized_scope:
                return True
        return False

    def _candidate_state_has_employment_scope(
        self,
        acquisition_state: dict[str, Any],
        employment_scope: str,
    ) -> bool:
        normalized_scope = str(employment_scope or "").strip().lower()
        if normalized_scope not in {"current", "former"}:
            return bool(list(acquisition_state.get("candidates") or []))
        for candidate in list(acquisition_state.get("candidates") or []):
            if isinstance(candidate, Candidate):
                candidate_scope = str(candidate.employment_status or "").strip().lower()
            elif isinstance(candidate, dict):
                candidate_scope = str(
                    candidate.get("employment_status") or candidate.get("employment_scope") or ""
                ).strip().lower()
            else:
                continue
            if candidate_scope == normalized_scope:
                return True
        return False

    def _acquisition_task_checkpoint_reusable(self, task: AcquisitionTask, acquisition_state: dict[str, Any]) -> bool:
        if task.task_type == "resolve_company_identity":
            return isinstance(acquisition_state.get("company_identity"), CompanyIdentity) and isinstance(
                acquisition_state.get("snapshot_dir"),
                Path,
            )
        if task.task_type == "acquire_full_roster":
            roster_snapshot = acquisition_state.get("roster_snapshot")
            search_seed_snapshot = acquisition_state.get("search_seed_snapshot")
            candidate_doc_path = acquisition_state.get("candidate_doc_path")
            roster_ready = isinstance(roster_snapshot, CompanyRosterSnapshot) and bool(
                list(roster_snapshot.visible_entries or []) or list(roster_snapshot.raw_entries or [])
            )
            search_seed_ready = isinstance(search_seed_snapshot, SearchSeedSnapshot) and bool(
                list(search_seed_snapshot.entries or [])
            )
            candidate_ready = bool(acquisition_state.get("candidates"))
            candidate_doc_ready = isinstance(candidate_doc_path, Path) and candidate_doc_path.exists()
            reused_snapshot_checkpoint = bool(acquisition_state.get("reused_snapshot_checkpoint"))
            allow_search_seed_baseline_for_full_roster = bool(
                acquisition_state.get("allow_search_seed_baseline_for_full_roster")
            )
            strategy_type = str(task.metadata.get("strategy_type") or "").strip()
            current_search_seed_ready = self._search_seed_snapshot_has_employment_scope(search_seed_snapshot, "current")
            current_candidate_ready = self._candidate_state_has_employment_scope(acquisition_state, "current")
            former_search_seed_ready = self._search_seed_snapshot_has_employment_scope(search_seed_snapshot, "former")
            former_candidate_ready = self._candidate_state_has_employment_scope(acquisition_state, "former")
            if strategy_type == "full_company_roster":
                return (
                    roster_ready
                    or candidate_ready
                    or candidate_doc_ready
                    or (reused_snapshot_checkpoint and candidate_doc_ready)
                    or (allow_search_seed_baseline_for_full_roster and search_seed_ready)
                )
            if strategy_type == "scoped_search_roster":
                return current_search_seed_ready or current_candidate_ready
            if strategy_type == "former_employee_search":
                return former_search_seed_ready or former_candidate_ready
            return roster_ready or search_seed_ready or candidate_ready or candidate_doc_ready
        if task.task_type == "acquire_former_search_seed":
            candidate_doc_path = acquisition_state.get("candidate_doc_path")
            former_candidate_ready = self._candidate_state_has_employment_scope(acquisition_state, "former")
            reused_snapshot_checkpoint = bool(acquisition_state.get("reused_snapshot_checkpoint"))
            if (
                isinstance(candidate_doc_path, Path)
                and candidate_doc_path.exists()
                and former_candidate_ready
            ):
                return True
            if reused_snapshot_checkpoint and former_candidate_ready:
                return True
            return self._search_seed_snapshot_has_employment_scope(
                acquisition_state.get("search_seed_snapshot"),
                "former",
            )
        if task.task_type == "enrich_linkedin_profiles":
            stage_path = acquisition_state.get("linkedin_stage_candidate_doc_path")
            candidate_ready = bool(acquisition_state.get("candidates"))
            if (
                bool(acquisition_state.get("linkedin_stage_completed"))
                and isinstance(stage_path, Path)
                and stage_path.exists()
            ):
                return True
            if bool(acquisition_state.get("reused_snapshot_checkpoint")) and candidate_ready:
                return True
            return bool(acquisition_state.get("linkedin_stage_completed")) and (
                isinstance(acquisition_state.get("candidate_doc_path"), Path) or candidate_ready
            )
        if task.task_type == "enrich_public_web_signals":
            stage_path = acquisition_state.get("public_web_stage_candidate_doc_path")
            candidate_ready = bool(acquisition_state.get("candidates"))
            if (
                bool(acquisition_state.get("public_web_stage_completed"))
                and isinstance(stage_path, Path)
                and stage_path.exists()
            ):
                return True
            if bool(acquisition_state.get("reused_snapshot_checkpoint")) and candidate_ready:
                return True
            return bool(acquisition_state.get("public_web_stage_completed")) and (
                isinstance(acquisition_state.get("candidate_doc_path"), Path) or candidate_ready
            )
        if task.task_type == "enrich_profiles_multisource":
            candidate_doc_path = acquisition_state.get("candidate_doc_path")
            return isinstance(candidate_doc_path, Path) and candidate_doc_path.exists()
        if task.task_type == "normalize_asset_snapshot":
            snapshot_dir = self._resolved_acquisition_snapshot_dir(acquisition_state)
            if self._equivalent_baseline_snapshot_active(acquisition_state, snapshot_dir=snapshot_dir):
                return True
            if self._reused_snapshot_candidate_documents_ready_for_direct_reuse(acquisition_state):
                return True
            if self._reused_snapshot_serving_artifacts_ready(acquisition_state):
                return True
            return isinstance(snapshot_dir, Path) and not self._snapshot_materialization_refresh_required(snapshot_dir)
        if task.task_type == "build_retrieval_index":
            snapshot_dir = self._resolved_acquisition_snapshot_dir(acquisition_state)
            if self._equivalent_baseline_snapshot_active(acquisition_state, snapshot_dir=snapshot_dir):
                return True
            if self._reused_snapshot_candidate_documents_ready_for_direct_reuse(acquisition_state):
                return True
            if (
                bool(acquisition_state.get("reused_snapshot_checkpoint"))
                and isinstance(snapshot_dir, Path)
                and isinstance(acquisition_state.get("candidate_doc_path"), Path)
                and (snapshot_dir / "retrieval_index_summary.json").exists()
            ):
                return True
            return isinstance(snapshot_dir, Path) and not self._snapshot_retrieval_index_refresh_required(snapshot_dir)
        return False

    def _build_acquisition_state(self, job_id: str, plan_payload: dict[str, Any]) -> dict[str, Any]:
        state: dict[str, Any] = {
            "job_id": job_id,
            "plan_payload": plan_payload,
            "runtime_mode": "workflow",
        }
        workers = self.agent_runtime.list_workers(job_id=job_id)
        for worker in workers:
            metadata = dict(worker.get("metadata") or {})
            snapshot_dir = str(metadata.get("root_snapshot_dir") or metadata.get("snapshot_dir") or "").strip()
            if snapshot_dir:
                snapshot_path = Path(snapshot_dir).expanduser()
                state["snapshot_dir"] = snapshot_path
                state["snapshot_id"] = snapshot_path.name
            identity_payload = dict(metadata.get("identity") or {})
            if identity_payload:
                try:
                    state["company_identity"] = CompanyIdentity(**identity_payload)
                except TypeError:
                    pass
            if "snapshot_dir" in state and "company_identity" in state:
                break
        return state

    def _run_outreach_layering_after_acquisition(
        self,
        *,
        job_id: str,
        request: JobRequest,
        acquisition_state: dict[str, Any],
        allow_ai: bool | None = None,
        analysis_stage_label: str = "",
        event_stage: str = "",
        allow_background_defer: bool = True,
    ) -> dict[str, Any]:
        if not _env_bool("OUTREACH_LAYERING_ENABLED", True):
            return {}
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(acquisition_state.get("snapshot_id") or "").strip()
        if not target_company or not snapshot_id:
            return {}
        latest_job = self.store.get_job(job_id) or {}
        resolved_event_stage = str(event_stage or latest_job.get("stage") or "acquiring").strip() or "acquiring"
        query_text = str(request.raw_user_request or request.query or "").strip()
        if allow_ai is None:
            ai_requested = _env_bool("OUTREACH_LAYERING_ENABLE_AI", False)
        else:
            ai_requested = bool(allow_ai)
        model_client: ModelClient | None = None
        if ai_requested and bool(getattr(self.model_client, "supports_outreach_ai_verification", lambda: False)()):
            model_client = self.model_client
        if model_client is None:
            cached_summary = self._load_cached_outreach_layering_summary(
                request=request,
                target_company=target_company,
                snapshot_id=snapshot_id,
                query_text=query_text,
                analysis_stage_label=analysis_stage_label,
            )
            if cached_summary:
                self.store.append_job_event(
                    job_id,
                    stage=resolved_event_stage,
                    status="completed",
                    detail="Reused cached outreach layering analysis from the authoritative snapshot.",
                    payload=cached_summary,
                )
                return cached_summary
            if allow_background_defer and str(request.target_scope or "").strip().lower() == "full_company_asset":
                deferred_summary_payload = {
                    "status": "deferred",
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "query": query_text,
                    "execution_scope": "all_queries_default",
                    "analysis_stage": analysis_stage_label or "stage_2_final",
                    "ai_requested": ai_requested,
                    "ai_enabled": False,
                    "reason": "deferred_for_asset_population_fast_path",
                    "deferred_at": _utc_now_iso(),
                }
                self.store.append_job_event(
                    job_id,
                    stage=resolved_event_stage,
                    status="completed",
                    detail="Deferred outreach layering so full local-asset reuse can finalize immediately.",
                    payload=deferred_summary_payload,
                )
                return deferred_summary_payload
        max_ai_verifications = self._resolve_outreach_ai_verification_budget(request) if model_client else 0
        ai_workers = max(1, _env_int("OUTREACH_LAYERING_AI_WORKERS", 6))
        ai_max_retries = max(0, _env_int("OUTREACH_LAYERING_AI_MAX_RETRIES", 2))
        ai_retry_backoff_seconds = _env_float("OUTREACH_LAYERING_AI_RETRY_BACKOFF_SECONDS", 0.8)
        summary_payload: dict[str, Any]
        try:
            result = analyze_company_outreach_layers(
                runtime_dir=self.runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                view=request.asset_view or "canonical_merged",
                query=query_text,
                model_client=model_client,
                store=self.store,
                max_ai_verifications=max_ai_verifications or 0,
                ai_workers=ai_workers,
                ai_max_retries=ai_max_retries,
                ai_retry_backoff_seconds=ai_retry_backoff_seconds,
            )
            layer_schema = dict(result.get("layer_schema") or {})
            primary_layer_keys = [
                str(item).strip() for item in list(layer_schema.get("primary_layer_keys") or []) if str(item).strip()
            ] or [
                "layer_0_roster",
                "layer_1_name_signal",
                "layer_2_greater_china_region_experience",
                "layer_3_mainland_china_experience_or_chinese_language",
            ]
            layer_counts = {
                key: int((result.get("layers") or {}).get(key, {}).get("count") or 0) for key in primary_layer_keys
            }
            summary_payload = {
                "status": "completed",
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "query": query_text,
                "execution_scope": "all_queries_default",
                "analysis_stage": analysis_stage_label or "stage_2_final",
                "ai_requested": ai_requested,
                "ai_enabled": bool(model_client),
                "ai_prompt_template_version": str((result.get("ai_prompt_template") or {}).get("version") or ""),
                "analysis_paths": dict(result.get("analysis_paths") or {}),
                "candidate_count": int(result.get("candidate_count") or 0),
                "layer_schema": layer_schema,
                "layer_counts": layer_counts,
                "cumulative_layer_counts": dict(result.get("cumulative_layer_counts") or {}),
                "final_layer_distribution": dict(result.get("final_layer_distribution") or {}),
                "ai_verification": dict(result.get("ai_verification") or {}),
            }
            self.store.append_job_event(
                job_id,
                stage=resolved_event_stage,
                status="completed",
                detail="Outreach layering completed and attached to retrieval runtime policy.",
                payload=summary_payload,
            )
            return summary_payload
        except Exception as exc:
            summary_payload = {
                "status": "failed",
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "query": query_text,
                "execution_scope": "all_queries_default",
                "analysis_stage": analysis_stage_label or "stage_2_final",
                "ai_requested": ai_requested,
                "ai_enabled": bool(model_client),
                "error": str(exc),
            }
            self.store.append_job_event(
                job_id,
                stage=resolved_event_stage,
                status="completed",
                detail="Outreach layering failed; workflow continues with base retrieval.",
                payload=summary_payload,
            )
            return summary_payload

    def _outreach_layering_requires_background_reconcile(
        self,
        *,
        request: JobRequest,
        summary_payload: dict[str, Any] | None,
    ) -> bool:
        if str(request.target_scope or "").strip().lower() != "full_company_asset":
            return False
        summary = dict(summary_payload or {})
        outreach_layering = dict(summary.get("outreach_layering") or {})
        return (
            str(outreach_layering.get("status") or "").strip().lower() in {"deferred", "scheduled"}
            and str(outreach_layering.get("reason") or "").strip() == "deferred_for_asset_population_fast_path"
            and bool(
                str(outreach_layering.get("snapshot_id") or "").strip()
                or str(dict(summary.get("candidate_source") or {}).get("snapshot_id") or "").strip()
            )
        )

    def _mark_outreach_layering_background_scheduled(
        self,
        *,
        request: JobRequest,
        summary_payload: dict[str, Any] | None,
        source: str,
    ) -> dict[str, Any]:
        summary = dict(summary_payload or {})
        if not self._outreach_layering_requires_background_reconcile(request=request, summary_payload=summary):
            return summary
        outreach_layering = dict(summary.get("outreach_layering") or {})
        if str(outreach_layering.get("status") or "").strip().lower() == "scheduled":
            return summary
        outreach_layering["status"] = "scheduled"
        outreach_layering["scheduled_at"] = str(outreach_layering.get("scheduled_at") or _utc_now_iso()).strip()
        outreach_layering["background_execution_source"] = str(source or "").strip()
        summary["outreach_layering"] = outreach_layering
        return summary

    def _queue_background_outreach_layering_reconcile(
        self,
        *,
        job_id: str,
        source: str,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"status": "invalid", "reason": "job_id_missing"}
        self.store.append_job_event(
            normalized_job_id,
            "completed",
            "running",
            "Queued background outreach layering reconcile for deferred full-asset finalization.",
            {"source": str(source or "").strip()},
        )
        thread = threading.Thread(
            target=self._run_background_outreach_layering_reconcile,
            kwargs={"job_id": normalized_job_id, "source": str(source or "").strip()},
            name=f"background-outreach-layering-{normalized_job_id}",
            daemon=True,
        )
        thread.start()
        return {"status": "scheduled", "job_id": normalized_job_id, "source": str(source or "").strip()}

    def _run_background_outreach_layering_reconcile(self, *, job_id: str, source: str) -> dict[str, Any]:
        try:
            return self._reconcile_completed_workflow_if_needed(job_id)
        except Exception as exc:
            self.store.append_job_event(
                job_id,
                "completed",
                "failed",
                "Background outreach layering reconcile crashed.",
                {"source": str(source or "").strip(), "error": str(exc)},
            )
            return {
                "job_id": str(job_id or "").strip(),
                "status": "failed",
                "reason": "background_outreach_layering_crashed",
                "error": str(exc),
            }

    def _snapshot_materialization_requires_background_reconcile(
        self,
        *,
        summary_payload: dict[str, Any] | None,
    ) -> bool:
        summary = dict(summary_payload or {})
        snapshot_materialization = dict(summary.get("background_snapshot_materialization") or {})
        status = str(snapshot_materialization.get("status") or "").strip().lower()
        return bool(
            str(snapshot_materialization.get("snapshot_id") or "").strip()
            and status in {"deferred", "scheduled"}
        )

    def _mark_snapshot_materialization_background_scheduled(
        self,
        *,
        summary_payload: dict[str, Any] | None,
        source: str,
    ) -> dict[str, Any]:
        summary = dict(summary_payload or {})
        if not self._snapshot_materialization_requires_background_reconcile(summary_payload=summary):
            return summary
        snapshot_materialization = dict(summary.get("background_snapshot_materialization") or {})
        if str(snapshot_materialization.get("status") or "").strip().lower() == "scheduled":
            return summary
        snapshot_materialization["status"] = "scheduled"
        snapshot_materialization["scheduled_at"] = (
            str(snapshot_materialization.get("scheduled_at") or _utc_now_iso()).strip()
        )
        snapshot_materialization["background_execution_source"] = str(source or "").strip()
        summary["background_snapshot_materialization"] = snapshot_materialization
        return summary

    def _queue_background_snapshot_materialization_reconcile(
        self,
        *,
        job_id: str,
        source: str,
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"status": "invalid", "reason": "job_id_missing"}
        self.store.append_job_event(
            normalized_job_id,
            "completed",
            "running",
            "Queued background snapshot materialization reconcile for deferred finalization.",
            {"source": str(source or "").strip()},
        )
        thread = threading.Thread(
            target=self._run_background_snapshot_materialization_reconcile,
            kwargs={"job_id": normalized_job_id, "source": str(source or "").strip()},
            name=f"background-snapshot-materialization-{normalized_job_id}",
            daemon=True,
        )
        thread.start()
        return {"status": "scheduled", "job_id": normalized_job_id, "source": str(source or "").strip()}

    def _run_background_snapshot_materialization_reconcile(self, *, job_id: str, source: str) -> dict[str, Any]:
        try:
            return self._reconcile_completed_workflow_after_deferred_snapshot_materialization(job_id)
        except Exception as exc:
            self.store.append_job_event(
                job_id,
                "completed",
                "failed",
                "Background snapshot materialization reconcile crashed.",
                {"source": str(source or "").strip(), "error": str(exc)},
            )
            return {
                "job_id": str(job_id or "").strip(),
                "status": "failed",
                "reason": "background_snapshot_materialization_crashed",
                "error": str(exc),
            }

    def _load_cached_outreach_layering_summary(
        self,
        *,
        request: JobRequest,
        target_company: str,
        snapshot_id: str,
        query_text: str,
        analysis_stage_label: str,
    ) -> dict[str, Any]:
        context = load_outreach_layering_context(
            runtime_dir=self.runtime_dir,
            request=request,
            candidate_source={
                "source_kind": "company_snapshot",
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "asset_view": request.asset_view or "canonical_merged",
                "source_path": "",
            },
            runtime_policy={},
        )
        analysis_path = str(context.get("analysis_path") or "").strip()
        if not analysis_path:
            return {}
        return {
            "status": str(context.get("status") or "completed"),
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "query": query_text,
            "execution_scope": "all_queries_default",
            "analysis_stage": analysis_stage_label or "stage_2_final",
            "ai_requested": False,
            "ai_enabled": False,
            "analysis_paths": {
                "full": analysis_path,
            },
            "candidate_count": int(context.get("candidate_layer_coverage") or 0),
            "layer_counts": dict(context.get("layer_counts") or {}),
            "cumulative_layer_counts": dict(context.get("cumulative_layer_counts") or {}),
            "ai_verification": dict(context.get("ai_verification") or {}),
        }

    def _should_use_two_stage_workflow_analysis(self, request: JobRequest) -> bool:
        return str(getattr(request, "analysis_stage_mode", "") or "").strip().lower() == "two_stage"

    def _should_require_stage2_confirmation(self, request: JobRequest) -> bool:
        return bool(dict(getattr(request, "execution_preferences", {}) or {}).get("require_stage2_confirmation"))

    def _workflow_plan_includes_public_web_stage(self, plan: Any) -> bool:
        for task in list(getattr(plan, "acquisition_tasks", []) or []):
            if isinstance(task, AcquisitionTask) and task.task_type == "enrich_public_web_signals":
                return True
        return False

    def _stage1_preview_ready(self, job_summary: dict[str, Any] | None) -> bool:
        preview = dict((job_summary or {}).get("stage1_preview") or {})
        return bool(str(preview.get("artifact_path") or "").strip())

    def _merge_workflow_stage_summary_fields(
        self,
        summary_payload: dict[str, Any] | None,
        *source_payloads: dict[str, Any] | None,
    ) -> dict[str, Any]:
        merged = dict(summary_payload or {})
        for key in (
            "status",
            "started_at",
            "completed_at",
            "message",
            "text",
            "analysis_stage_mode",
            "stage1_preview",
            "request_preview",
            "linkedin_stage_1",
            "public_web_stage_2",
            "pre_retrieval_refresh",
            "acquisition_progress",
            "background_snapshot_materialization",
            "background_reconcile",
        ):
            if merged.get(key) not in (None, "", [], {}):
                continue
            for source in source_payloads:
                value = dict(source or {}).get(key)
                if value not in (None, "", [], {}):
                    merged[key] = value
                    break
        return merged

    def _restore_completed_workflow_stage_summary_contract(
        self,
        summary_payload: dict[str, Any] | None,
        *source_payloads: dict[str, Any] | None,
    ) -> dict[str, Any]:
        restored = self._merge_workflow_stage_summary_fields(summary_payload, *source_payloads)
        analysis_stage = str(restored.get("analysis_stage") or "").strip().lower()
        if analysis_stage != "stage_2_final":
            return restored
        if not str(restored.get("status") or "").strip():
            restored["status"] = "completed"
        started_at_candidates = [
            str(restored.get("started_at") or "").strip(),
            str(restored.get("stage_2_final_started_at") or "").strip(),
            str(dict(restored.get("public_web_stage_2") or {}).get("completed_at") or "").strip(),
        ]
        completed_at_candidates = [
            str(restored.get("completed_at") or "").strip(),
        ]
        for source in source_payloads:
            source_payload = dict(source or {})
            started_at_candidates.extend(
                [
                    str(source_payload.get("started_at") or "").strip(),
                    str(source_payload.get("stage_2_final_started_at") or "").strip(),
                    str(dict(source_payload.get("public_web_stage_2") or {}).get("completed_at") or "").strip(),
                ]
            )
            completed_at_candidates.append(str(source_payload.get("completed_at") or "").strip())
        resolved_started_at = _earliest_timestamp_string(*started_at_candidates)
        resolved_completed_at = _latest_timestamp_string(*completed_at_candidates)
        if resolved_started_at:
            restored["started_at"] = resolved_started_at
        if resolved_completed_at:
            restored["completed_at"] = resolved_completed_at
        return restored

    def _resolve_workflow_snapshot_dir(
        self,
        *,
        request: JobRequest,
        summary_payloads: list[dict[str, Any] | None],
        job_id: str = "",
    ) -> Path | None:
        normalized_job_id = str(job_id or "").strip()
        if normalized_job_id:
            result_view = self.store.get_job_result_view(job_id=normalized_job_id)
            if result_view:
                snapshot_dir = resolve_snapshot_dir_from_source_path(
                    str(result_view.get("source_path") or "").strip(),
                    runtime_dir=self.runtime_dir,
                )
                if snapshot_dir is not None:
                    return snapshot_dir
                snapshot_dir = resolve_company_snapshot_dir(
                    self.runtime_dir,
                    target_company=str(result_view.get("target_company") or request.target_company or "").strip(),
                    snapshot_id=str(result_view.get("snapshot_id") or "").strip(),
                )
                if snapshot_dir is not None:
                    return snapshot_dir
        for payload in summary_payloads:
            summary = dict(payload or {})
            candidate_paths = [
                summary.get("candidate_doc_path"),
                summary.get("stage_candidate_doc_path"),
                dict(summary.get("candidate_source") or {}).get("source_path"),
                dict(summary.get("linkedin_stage_1") or {}).get("candidate_doc_path"),
                dict(summary.get("linkedin_stage_1") or {}).get("stage_candidate_doc_path"),
                dict(summary.get("public_web_stage_2") or {}).get("candidate_doc_path"),
                dict(summary.get("public_web_stage_2") or {}).get("stage_candidate_doc_path"),
                dict(dict(summary.get("stage1_preview") or {}).get("candidate_source") or {}).get("source_path"),
            ]
            for path_value in candidate_paths:
                snapshot_dir = resolve_snapshot_dir_from_source_path(
                    path_value,
                    runtime_dir=self.runtime_dir,
                )
                if snapshot_dir is not None:
                    return snapshot_dir
            candidate_snapshot_ids = [
                summary.get("snapshot_id"),
                dict(summary.get("candidate_source") or {}).get("snapshot_id"),
                dict(summary.get("linkedin_stage_1") or {}).get("snapshot_id"),
                dict(summary.get("public_web_stage_2") or {}).get("snapshot_id"),
                dict(dict(summary.get("stage1_preview") or {}).get("candidate_source") or {}).get("snapshot_id"),
            ]
            target_company = str(request.target_company or "").strip()
            if not target_company:
                continue
            for snapshot_id_value in candidate_snapshot_ids:
                snapshot_id = str(snapshot_id_value or "").strip()
                if not snapshot_id:
                    continue
                candidate = resolve_company_snapshot_dir(
                    self.runtime_dir,
                    target_company=target_company,
                    snapshot_id=snapshot_id,
                )
                if candidate is not None:
                    return candidate
        return None

    def _save_workflow_job_state(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        status: str,
        stage: str,
        summary_payload: dict[str, Any],
        artifact_path: str = "",
    ) -> None:
        resolved_artifact_path = str(artifact_path or "").strip()
        if not resolved_artifact_path:
            resolved_artifact_path = str((self.store.get_job(job_id) or {}).get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status=status,
            stage=stage,
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
            summary_payload=summary_payload,
            artifact_path=resolved_artifact_path,
        )

    def _save_excel_intake_job_state(
        self,
        *,
        job_id: str,
        request: JobRequest,
        status: str,
        stage: str,
        summary_payload: dict[str, Any],
        artifact_path: str = "",
    ) -> None:
        resolved_artifact_path = str(artifact_path or "").strip()
        if not resolved_artifact_path:
            resolved_artifact_path = str((self.store.get_job(job_id) or {}).get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type="excel_intake",
            status=status,
            stage=stage,
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload=summary_payload,
            artifact_path=resolved_artifact_path,
        )

    def _compact_excel_intake_job_summary(
        self,
        *,
        result: dict[str, Any],
        request: JobRequest,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        summary_payload = dict(result.get("summary") or {})
        attachment_summary = dict(result.get("attachment_summary") or {})
        attachment_artifact = dict(attachment_summary.get("artifact_result") or {})
        workbook_payload = dict(result.get("workbook") or {})
        return {
            "workflow_kind": "excel_intake",
            "intake_id": str(result.get("intake_id") or "").strip(),
            "target_company": str(request.target_company or "").strip(),
            "input_filename": str(payload.get("filename") or workbook_payload.get("source_path") or "").strip(),
            "attach_to_snapshot": _coerce_bool(payload.get("attach_to_snapshot"), True),
            "build_artifacts": _coerce_bool(payload.get("build_artifacts"), True),
            "workbook": {
                "sheet_count": int(workbook_payload.get("sheet_count") or 0),
                "detected_contact_row_count": int(workbook_payload.get("detected_contact_row_count") or 0),
                "sheet_names": [
                    str(item or "").strip()
                    for item in list(workbook_payload.get("sheet_names") or [])
                    if str(item or "").strip()
                ],
            },
            "intake_summary": {
                "total_rows": int(summary_payload.get("total_rows") or 0),
                "persisted_candidate_count": int(summary_payload.get("persisted_candidate_count") or 0),
                "persisted_evidence_count": int(summary_payload.get("persisted_evidence_count") or 0),
                "local_exact_hit_count": int(summary_payload.get("local_exact_hit_count") or 0),
                "manual_review_local_count": int(summary_payload.get("manual_review_local_count") or 0),
                "fetched_direct_linkedin_count": int(summary_payload.get("fetched_direct_linkedin_count") or 0),
                "fetched_via_search_count": int(summary_payload.get("fetched_via_search_count") or 0),
                "manual_review_search_count": int(summary_payload.get("manual_review_search_count") or 0),
                "unresolved_count": int(summary_payload.get("unresolved_count") or 0),
            },
            "attachment_summary": {
                "status": str(attachment_summary.get("status") or "").strip(),
                "reason": str(attachment_summary.get("reason") or "").strip(),
                "snapshot_id": str(attachment_summary.get("snapshot_id") or "").strip(),
                "candidate_count": int(attachment_summary.get("candidate_count") or 0),
                "evidence_count": int(attachment_summary.get("evidence_count") or 0),
                "candidate_doc_path": str(attachment_summary.get("candidate_doc_path") or "").strip(),
                "stage_candidate_doc_path": str(attachment_summary.get("stage_candidate_doc_path") or "").strip(),
                "summary_path": str(attachment_summary.get("summary_path") or "").strip(),
                "artifact_result": {
                    "status": str(attachment_artifact.get("status") or "").strip(),
                    "generation_key": str(attachment_artifact.get("generation_key") or "").strip(),
                    "generation_sequence": int(attachment_artifact.get("generation_sequence") or 0),
                },
            },
            "artifact_paths": {
                key: str(value or "").strip()
                for key, value in dict(result.get("artifact_paths") or {}).items()
                if str(key or "").strip() and str(value or "").strip()
            },
        }

    def _run_excel_intake_workflow(
        self,
        *,
        job_id: str,
        request: JobRequest,
        payload: dict[str, Any],
    ) -> None:
        started_at = _utc_now_iso()
        filename = str(payload.get("filename") or "").strip()
        initial_summary = {
            "workflow_kind": "excel_intake",
            "message": "正在解析 Excel 文件并对联系人做本地去重。",
            "target_company": str(request.target_company or "").strip(),
            "input_filename": filename,
            "attach_to_snapshot": _coerce_bool(payload.get("attach_to_snapshot"), True),
            "build_artifacts": _coerce_bool(payload.get("build_artifacts"), True),
            "linkedin_stage_1": {
                "status": "running",
                "title": "Excel 上传",
                "text": "正在解析 Excel 文件并对联系人做本地去重。",
                "workflow_kind": "excel_intake",
                "target_company": str(request.target_company or "").strip(),
                "input_filename": filename,
                "started_at": started_at,
            },
        }
        self._save_excel_intake_job_state(
            job_id=job_id,
            request=request,
            status="running",
            stage="acquiring",
            summary_payload=initial_summary,
        )
        self.store.append_job_event(
            job_id,
            "acquiring",
            "running",
            "Excel workbook uploaded. Parsing contacts and checking local assets.",
            {
                "workflow_kind": "excel_intake",
                "target_company": request.target_company,
                "input_filename": filename,
            },
        )
        try:
            service = ExcelIntakeService(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.acquisition_engine.settings,
                model_client=self.model_client,
            )
            result = service.ingest_contacts(payload)
            if str(result.get("status") or "").strip().lower() == "invalid":
                raise ValueError(str(result.get("reason") or "excel intake invalid"))

            compact_summary = self._compact_excel_intake_job_summary(result=result, request=request, payload=payload)
            attachment_summary = dict(result.get("attachment_summary") or {})
            snapshot_id = str(attachment_summary.get("snapshot_id") or "").strip()
            candidate_doc_path = str(attachment_summary.get("candidate_doc_path") or "").strip()
            if (
                str(attachment_summary.get("status") or "").strip().lower() != "completed"
                or not snapshot_id
                or not candidate_doc_path
            ):
                reason = str(attachment_summary.get("reason") or "snapshot_attachment_required").strip()
                raise ValueError(f"excel intake workflow requires attached snapshot results: {reason}")

            ingest_completed_at = _utc_now_iso()
            linkedin_stage_payload = {
                "status": "completed",
                "title": "Excel 上传",
                "text": "Excel 文件解析完成，已完成联系人匹配、去重与可自动拉取的 LinkedIn 补全。",
                "workflow_kind": "excel_intake",
                "target_company": str(request.target_company or "").strip(),
                "input_filename": filename,
                "started_at": started_at,
                "completed_at": ingest_completed_at,
                "intake_id": str(result.get("intake_id") or "").strip(),
                "total_rows": int(dict(result.get("summary") or {}).get("total_rows") or 0),
                "persisted_candidate_count": int(
                    dict(result.get("summary") or {}).get("persisted_candidate_count") or 0
                ),
                "manual_review_row_count": sum(
                    int(dict(result.get("summary") or {}).get(field_name) or 0)
                    for field_name in (
                        "manual_review_local_count",
                        "manual_review_search_count",
                        "unresolved_count",
                    )
                ),
                "snapshot_id": snapshot_id,
            }
            linkedin_stage_summary_path = self._persist_workflow_stage_summary_file(
                request=request,
                stage_name="linkedin_stage_1",
                summary_payload=linkedin_stage_payload,
            )
            if linkedin_stage_summary_path:
                linkedin_stage_payload["summary_path"] = linkedin_stage_summary_path
            after_ingest_summary = {
                **compact_summary,
                "message": "Excel intake completed. Building candidate board preview.",
                "linkedin_stage_1": linkedin_stage_payload,
            }
            self._save_excel_intake_job_state(
                job_id=job_id,
                request=request,
                status="running",
                stage="acquiring",
                summary_payload=after_ingest_summary,
            )
            self.store.append_job_event(
                job_id,
                "acquiring",
                "completed",
                "Excel upload and candidate enrichment completed.",
                linkedin_stage_payload,
            )

            candidate_source = self._load_retrieval_candidate_source(
                request,
                snapshot_id=snapshot_id,
                candidate_doc_path=candidate_doc_path,
            )
            effective_execution_semantics = self._build_effective_execution_semantics(
                request=request,
                organization_execution_profile={},
                asset_reuse_plan={},
                candidate_source=candidate_source,
            )
            preview_started_at = _utc_now_iso()
            preview_artifact = self._execute_asset_population_fast_path(
                job_id=job_id,
                request=request,
                plan={},
                job_type="excel_intake",
                runtime_policy={
                    "analysis_stage": "stage_1_preview",
                    "summary_mode": "deterministic",
                },
                candidate_source=candidate_source,
                organization_execution_profile={},
                effective_execution_semantics=effective_execution_semantics,
                plan_asset_reuse_plan={},
                persist_job_state=False,
                artifact_name_suffix="preview",
                artifact_status="preview_ready",
            )
            preview_artifact_summary = dict(preview_artifact.get("summary") or {})
            preview_completed_at = _utc_now_iso()
            stage1_preview_payload = {
                "status": "completed",
                "title": "候选人对齐",
                "text": str(preview_artifact_summary.get("text") or "").strip(),
                "workflow_kind": "excel_intake",
                "analysis_stage": "stage_1_preview",
                "started_at": preview_started_at,
                "completed_at": preview_completed_at,
                "returned_matches": int(preview_artifact_summary.get("returned_matches") or 0),
                "total_matches": int(preview_artifact_summary.get("total_matches") or 0),
                "manual_review_queue_count": int(
                    preview_artifact_summary.get("manual_review_queue_count") or 0
                ),
                "candidate_source": dict(preview_artifact_summary.get("candidate_source") or {}),
                "artifact_path": str(preview_artifact.get("artifact_path") or "").strip(),
            }
            stage1_preview_summary_path = self._persist_workflow_stage_summary_file(
                request=request,
                stage_name="stage_1_preview",
                summary_payload=stage1_preview_payload,
            )
            if stage1_preview_summary_path:
                stage1_preview_payload["summary_path"] = stage1_preview_summary_path
            preview_summary = {
                **after_ingest_summary,
                "message": "Excel preview ready. Finalizing company asset board.",
                "stage1_preview": stage1_preview_payload,
            }
            self._save_excel_intake_job_state(
                job_id=job_id,
                request=request,
                status="running",
                stage="retrieving",
                summary_payload=preview_summary,
                artifact_path=str(preview_artifact.get("artifact_path") or ""),
            )
            self.store.append_job_event(
                job_id,
                "retrieving",
                "running",
                "Excel candidate preview is ready. Finalizing result view.",
                {
                    "analysis_stage": "stage_1_preview",
                    "summary": preview_artifact_summary,
                    "preview_artifact_path": str(preview_artifact.get("artifact_path") or ""),
                },
            )

            public_web_stage_payload = {
                "status": "completed",
                "title": "公司资产归档",
                "text": "Excel 候选人已并入公司资产，并完成当前 snapshot 的 artifact 更新。",
                "workflow_kind": "excel_intake",
                "snapshot_id": snapshot_id,
                "candidate_doc_path": candidate_doc_path,
                "started_at": preview_completed_at,
                "completed_at": _utc_now_iso(),
                "candidate_count": int(attachment_summary.get("candidate_count") or 0),
                "evidence_count": int(attachment_summary.get("evidence_count") or 0),
            }
            public_web_stage_summary_path = self._persist_workflow_stage_summary_file(
                request=request,
                stage_name="public_web_stage_2",
                summary_payload=public_web_stage_payload,
            )
            if public_web_stage_summary_path:
                public_web_stage_payload["summary_path"] = public_web_stage_summary_path
            pre_final_summary = {
                **preview_summary,
                "message": "Excel candidates archived. Finalizing results.",
                "public_web_stage_2": public_web_stage_payload,
            }
            self._save_excel_intake_job_state(
                job_id=job_id,
                request=request,
                status="running",
                stage="retrieving",
                summary_payload=pre_final_summary,
                artifact_path=str(preview_artifact.get("artifact_path") or ""),
            )
            self.store.append_job_event(
                job_id,
                "retrieving",
                "completed",
                "Excel candidates archived into company assets.",
                public_web_stage_payload,
            )

            final_artifact = self._execute_asset_population_fast_path(
                job_id=job_id,
                request=request,
                plan={},
                job_type="excel_intake",
                runtime_policy={
                    "analysis_stage": "stage_2_final",
                    "summary_mode": "deterministic",
                },
                candidate_source=candidate_source,
                organization_execution_profile={},
                effective_execution_semantics=effective_execution_semantics,
                plan_asset_reuse_plan={},
                persist_job_state=True,
                artifact_name_suffix="",
                artifact_status="completed",
            )
            final_summary = {
                **dict(final_artifact.get("summary") or {}),
                **compact_summary,
                "workflow_kind": "excel_intake",
                "message": str(dict(final_artifact.get("summary") or {}).get("text") or "Excel intake workflow completed."),
                "analysis_stage": "stage_2_final",
                "linkedin_stage_1": linkedin_stage_payload,
                "stage1_preview": stage1_preview_payload,
                "public_web_stage_2": public_web_stage_payload,
            }
            final_stage_summary_path = self._persist_workflow_stage_summary_file(
                request=request,
                stage_name="stage_2_final",
                summary_payload=final_summary,
            )
            if final_stage_summary_path:
                final_summary["stage_summary_path"] = final_stage_summary_path
            self._save_excel_intake_job_state(
                job_id=job_id,
                request=request,
                status="completed",
                stage="completed",
                summary_payload=final_summary,
                artifact_path=str(final_artifact.get("artifact_path") or ""),
            )
            self.store.append_job_event(
                job_id,
                "completed",
                "completed",
                "Excel intake workflow completed.",
                {
                    "workflow_kind": "excel_intake",
                    "snapshot_id": snapshot_id,
                    "candidate_count": int(dict(final_artifact.get("summary") or {}).get("total_matches") or 0),
                    "manual_review_queue_count": int(
                        dict(final_artifact.get("summary") or {}).get("manual_review_queue_count") or 0
                    ),
                },
            )
            self._sync_frontend_history_phase_for_job(
                job={
                    "job_id": job_id,
                    "status": "completed",
                    "stage": "completed",
                    "request": request.to_record(),
                    "plan": {},
                },
                phase="results",
            )
        except Exception as exc:
            failed_summary = {
                **dict((self.store.get_job(job_id) or {}).get("summary") or {}),
                "workflow_kind": "excel_intake",
                "message": str(exc),
                "failed_at": _utc_now_iso(),
            }
            self._save_excel_intake_job_state(
                job_id=job_id,
                request=request,
                status="failed",
                stage="failed",
                summary_payload=failed_summary,
            )
            self.store.append_job_event(job_id, "failed", "failed", str(exc))
            self._sync_frontend_history_phase_for_job(
                job={
                    "job_id": job_id,
                    "status": "failed",
                    "stage": "failed",
                    "request": request.to_record(),
                    "plan": {},
                },
                phase="results",
            )

    def _persist_acquisition_stage_summary_file(
        self,
        *,
        request: JobRequest,
        stage_name: str,
        summary_payload: dict[str, Any],
        acquisition_state: dict[str, Any],
    ) -> str:
        return self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=stage_name,
            summary_payload=summary_payload,
            snapshot_dir=self._resolved_acquisition_snapshot_dir(acquisition_state),
        )

    def _build_post_acquisition_workflow_summary(
        self,
        *,
        base_summary: dict[str, Any],
        message: str,
        acquisition_progress: dict[str, Any],
        pre_retrieval_refresh: dict[str, Any],
        stage2_final_started_at: str = "",
        awaiting_stage2_confirmation: bool = False,
        outreach_layering: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        summary = dict(base_summary or {})
        summary["message"] = message
        summary.pop("blocked_task", None)
        summary.pop("blocked_task_id", None)
        summary.pop("awaiting_user_action", None)
        summary.pop("stage2_transition_state", None)
        if stage2_final_started_at:
            summary["stage_2_final_started_at"] = str(
                summary.get("stage_2_final_started_at") or stage2_final_started_at
            ).strip()
        if acquisition_progress:
            completed_progress = dict(acquisition_progress)
            completed_progress["status"] = "completed"
            completed_progress["completed_at"] = _utc_now_iso()
            summary["acquisition_progress"] = completed_progress
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            summary["pre_retrieval_refresh"] = pre_retrieval_refresh
        if awaiting_stage2_confirmation:
            summary["analysis_stage_mode"] = "two_stage"
            summary["awaiting_user_action"] = "continue_stage2"
            summary["stage2_transition_state"] = "idle"
        if outreach_layering:
            summary["outreach_layering"] = dict(outreach_layering)
        return summary

    def _persist_workflow_stage_summary_file(
        self,
        *,
        request: JobRequest,
        stage_name: str,
        summary_payload: dict[str, Any],
        snapshot_dir: Path | None = None,
    ) -> str:
        effective_snapshot_dir = snapshot_dir
        if effective_snapshot_dir is None:
            effective_snapshot_dir = self._resolve_workflow_snapshot_dir(
                request=request,
                summary_payloads=[summary_payload],
            )
        if not isinstance(effective_snapshot_dir, Path):
            return ""
        stage_summary_dir = effective_snapshot_dir / "workflow_stage_summaries"
        stage_summary_dir.mkdir(parents=True, exist_ok=True)
        stage_summary_path = stage_summary_dir / f"{stage_name}.json"
        stage_record = {
            "stage": stage_name,
            "target_company": str(request.target_company or "").strip(),
            "snapshot_id": effective_snapshot_dir.name,
            "saved_at": _utc_now_iso(),
            **dict(summary_payload or {}),
        }
        stage_summary_path.write_text(
            json.dumps(_storage_json_safe_payload(stage_record), ensure_ascii=False, indent=2)
        )
        return str(stage_summary_path)

    def _build_workflow_stage_summary_fallback_record(
        self,
        *,
        stage_name: str,
        job_summary: dict[str, Any],
    ) -> dict[str, Any]:
        record = self._workflow_stage_summary_payload_from_job_summary(
            stage_name=stage_name,
            job_summary=job_summary,
        )
        if not record:
            return {}
        record.setdefault("stage", stage_name)
        return record

    def _workflow_stage_summary_payload_from_job_summary(
        self,
        *,
        stage_name: str,
        job_summary: dict[str, Any],
    ) -> dict[str, Any]:
        if stage_name == "linkedin_stage_1":
            return dict(job_summary.get("linkedin_stage_1") or {})
        elif stage_name == "stage_1_preview":
            return dict(job_summary.get("stage1_preview") or {})
        elif stage_name == "public_web_stage_2":
            return dict(job_summary.get("public_web_stage_2") or {})
        elif stage_name == "stage_2_final":
            return dict(job_summary)
        return {}

    def _normalize_workflow_stage_summary_record(
        self,
        *,
        stage_name: str,
        stage_record: dict[str, Any],
        job: dict[str, Any],
        job_summary: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(stage_record or {})
        if not normalized:
            return {}
        normalized.setdefault("stage", stage_name)
        summary_stage_payload = self._workflow_stage_summary_payload_from_job_summary(
            stage_name=stage_name,
            job_summary=job_summary,
        )
        for key in (
            "status",
            "analysis_stage",
            "message",
            "title",
            "text",
            "workflow_kind",
            "candidate_source",
            "candidate_count",
            "returned_matches",
            "total_matches",
            "manual_review_count",
            "manual_review_queue_count",
            "artifact_path",
        ):
            value = normalized.get(key)
            if value not in (None, "", [], {}):
                continue
            source_value = summary_stage_payload.get(key)
            if source_value not in (None, "", [], {}):
                normalized[key] = source_value

        job_created_at = str(job.get("created_at") or "").strip()
        job_updated_at = str(job.get("updated_at") or "").strip()
        linkedin_stage = dict(job_summary.get("linkedin_stage_1") or {})
        stage1_preview = dict(job_summary.get("stage1_preview") or {})
        public_web_stage_2 = dict(job_summary.get("public_web_stage_2") or {})
        analysis_stage = str(job_summary.get("analysis_stage") or "").strip().lower()

        started_at_candidates = [
            str(normalized.get("started_at") or "").strip(),
            str(summary_stage_payload.get("started_at") or "").strip(),
        ]
        completed_at_candidates = [
            str(normalized.get("completed_at") or "").strip(),
            str(summary_stage_payload.get("completed_at") or "").strip(),
            str(normalized.get("saved_at") or "").strip(),
        ]
        if stage_name == "linkedin_stage_1":
            started_at_candidates.append(job_created_at)
            completed_at_candidates.extend(
                [
                    str(stage1_preview.get("started_at") or "").strip(),
                    str(stage1_preview.get("completed_at") or "").strip(),
                    str(stage1_preview.get("ready_at") or "").strip(),
                ]
            )
        elif stage_name == "stage_1_preview":
            started_at_candidates.extend(
                [
                    str(linkedin_stage.get("completed_at") or "").strip(),
                    str(stage1_preview.get("ready_at") or "").strip(),
                ]
            )
            completed_at_candidates.extend(
                [
                    str(stage1_preview.get("ready_at") or "").strip(),
                    str(public_web_stage_2.get("started_at") or "").strip(),
                    str(public_web_stage_2.get("completed_at") or "").strip(),
                    str(job_summary.get("stage_2_final_started_at") or "").strip(),
                ]
            )
            if analysis_stage == "stage_1_preview":
                started_at_candidates.append(str(job_summary.get("started_at") or "").strip())
                completed_at_candidates.append(str(job_summary.get("completed_at") or "").strip())
        elif stage_name == "public_web_stage_2":
            started_at_candidates.extend(
                [
                    str(stage1_preview.get("completed_at") or "").strip(),
                    str(stage1_preview.get("ready_at") or "").strip(),
                ]
            )
            completed_at_candidates.extend(
                [
                    str(job_summary.get("stage_2_final_started_at") or "").strip(),
                ]
            )
            if analysis_stage == "public_web_stage_2":
                completed_at_candidates.append(str(job_summary.get("completed_at") or "").strip())
        elif stage_name == "stage_2_final":
            started_at_candidates.extend(
                [
                    str(job_summary.get("started_at") or "").strip(),
                    str(job_summary.get("stage_2_final_started_at") or "").strip(),
                    str(public_web_stage_2.get("completed_at") or "").strip(),
                ]
            )
            completed_at_candidates.extend(
                [
                    str(job_summary.get("completed_at") or "").strip(),
                    job_updated_at,
                ]
            )

        if not str(normalized.get("started_at") or "").strip():
            resolved_started_at = _earliest_timestamp_string(*started_at_candidates)
            if resolved_started_at:
                normalized["started_at"] = resolved_started_at
        if not str(normalized.get("completed_at") or "").strip():
            resolved_completed_at = (
                _latest_timestamp_string(*completed_at_candidates)
                if stage_name == "stage_2_final"
                else _earliest_timestamp_string(*completed_at_candidates)
            )
            if resolved_completed_at:
                normalized["completed_at"] = resolved_completed_at
        if not str(normalized.get("status") or "").strip():
            if str(normalized.get("completed_at") or "").strip():
                normalized["status"] = "completed"
            elif str(normalized.get("started_at") or "").strip():
                normalized["status"] = "running"
        return normalized

    def _repair_stage1_preview_summary_for_asset_population(
        self,
        *,
        job_summary: dict[str, Any],
        summaries: dict[str, dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        stage1_summary = dict(summaries.get("stage_1_preview") or {})
        final_summary = dict(summaries.get("stage_2_final") or {})
        if not stage1_summary or not final_summary:
            return summaries

        preview_candidate_source = dict(stage1_summary.get("candidate_source") or {})
        final_candidate_source = dict(final_summary.get("candidate_source") or job_summary.get("candidate_source") or {})
        default_results_mode = str(
            final_summary.get("default_results_mode") or job_summary.get("default_results_mode") or ""
        ).strip().lower()
        preview_candidate_count = int(
            preview_candidate_source.get("candidate_count")
            or stage1_summary.get("returned_matches")
            or stage1_summary.get("total_matches")
            or 0
        )
        final_candidate_count = int(
            final_candidate_source.get("candidate_count")
            or final_summary.get("returned_matches")
            or final_summary.get("total_matches")
            or 0
        )
        if default_results_mode != "asset_population" or final_candidate_count <= preview_candidate_count:
            return summaries
        if final_summary.get("manual_review_queue_count") is not None:
            manual_review_queue_count = int(final_summary.get("manual_review_queue_count") or 0)
        elif job_summary.get("manual_review_queue_count") is not None:
            manual_review_queue_count = int(job_summary.get("manual_review_queue_count") or 0)
        else:
            manual_review_queue_count = int(stage1_summary.get("manual_review_queue_count") or 0)

        repaired_stage1_summary = dict(stage1_summary)
        repaired_stage1_summary["candidate_source"] = final_candidate_source
        repaired_stage1_summary["total_matches"] = final_candidate_count
        repaired_stage1_summary["returned_matches"] = final_candidate_count
        repaired_stage1_summary["manual_review_queue_count"] = manual_review_queue_count
        repaired_stage1_summary["repaired_from_stage_2_final"] = True

        preview_payload = dict(repaired_stage1_summary.get("stage1_preview") or {})
        if preview_payload:
            preview_payload["candidate_source"] = final_candidate_source
            preview_payload["total_matches"] = final_candidate_count
            preview_payload["returned_matches"] = final_candidate_count
            preview_payload["manual_review_queue_count"] = int(
                repaired_stage1_summary.get("manual_review_queue_count") or 0
            )
            repaired_stage1_summary["stage1_preview"] = preview_payload

        repaired_summaries = dict(summaries)
        repaired_summaries["stage_1_preview"] = repaired_stage1_summary
        return repaired_summaries

    def _load_workflow_stage_summaries(self, *, job: dict[str, Any]) -> dict[str, Any]:
        job_summary = dict(job.get("summary") or {})
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        snapshot_dir = self._resolve_workflow_snapshot_dir(
            request=request,
            summary_payloads=[job_summary],
            job_id=str(job.get("job_id") or ""),
        )
        stage_summary_dir = snapshot_dir / "workflow_stage_summaries" if isinstance(snapshot_dir, Path) else None
        stage_path_hints = {
            "linkedin_stage_1": str(dict(job_summary.get("linkedin_stage_1") or {}).get("summary_path") or "").strip(),
            "stage_1_preview": str(dict(job_summary.get("stage1_preview") or {}).get("summary_path") or "").strip(),
            "public_web_stage_2": str(
                dict(job_summary.get("public_web_stage_2") or {}).get("summary_path") or ""
            ).strip(),
            "stage_2_final": str(job_summary.get("stage_summary_path") or "").strip(),
        }
        summaries: dict[str, dict[str, Any]] = {}
        for stage_name in _WORKFLOW_STAGE_SUMMARY_STAGE_ORDER:
            candidate_paths: list[Path] = []
            hinted_path_raw = str(stage_path_hints.get(stage_name) or "").strip()
            if hinted_path_raw:
                candidate_paths.append(Path(hinted_path_raw).expanduser())
            if stage_summary_dir is not None:
                candidate_paths.append(stage_summary_dir / f"{stage_name}.json")
            stage_record: dict[str, Any] = {}
            seen_paths: set[str] = set()
            for candidate_path in candidate_paths:
                candidate_key = str(candidate_path)
                if not candidate_key or candidate_key in seen_paths:
                    continue
                seen_paths.add(candidate_key)
                if not candidate_path.exists():
                    continue
                stage_record = _read_json_dict(candidate_path)
                if stage_record:
                    stage_record.setdefault("stage", stage_name)
                    stage_record.setdefault("summary_path", str(candidate_path))
                    break
            if not stage_record:
                stage_record = self._build_workflow_stage_summary_fallback_record(
                    stage_name=stage_name,
                    job_summary=job_summary,
                )
            if stage_record:
                stage_record = self._normalize_workflow_stage_summary_record(
                    stage_name=stage_name,
                    stage_record=stage_record,
                    job=job,
                    job_summary=job_summary,
                )
                summaries[stage_name] = stage_record
        summaries = self._repair_stage1_preview_summary_for_asset_population(
            job_summary=job_summary,
            summaries=summaries,
        )
        directory = str(stage_summary_dir) if isinstance(stage_summary_dir, Path) else ""
        return {
            "directory": directory,
            "stage_order": [
                stage_name for stage_name in _WORKFLOW_STAGE_SUMMARY_STAGE_ORDER if stage_name in summaries
            ],
            "summaries": summaries,
        }

    def _build_stage1_preview_record(
        self,
        *,
        preview_artifact: dict[str, Any],
        started_at: str = "",
        completed_at: str = "",
    ) -> dict[str, Any]:
        preview_summary = dict(preview_artifact.get("summary") or {})
        preview_record = {
            "status": "ready",
            "analysis_stage": str(preview_summary.get("analysis_stage") or "stage_1_preview"),
            "summary_text": str(preview_summary.get("text") or ""),
            "total_matches": int(preview_summary.get("total_matches") or 0),
            "returned_matches": int(preview_summary.get("returned_matches") or 0),
            "manual_review_queue_count": int(preview_summary.get("manual_review_queue_count") or 0),
            "candidate_source": dict(preview_summary.get("candidate_source") or {}),
            "outreach_layering": dict(preview_summary.get("outreach_layering") or {}),
            "artifact_path": str(preview_artifact.get("artifact_path") or ""),
            "ready_at": str(completed_at or _utc_now_iso()).strip(),
        }
        if str(started_at or "").strip():
            preview_record["started_at"] = str(started_at).strip()
        if str(completed_at or "").strip():
            preview_record["completed_at"] = str(completed_at).strip()
        return preview_record

    def _build_stage1_preview_summary(
        self,
        *,
        request: JobRequest,
        preview_artifact: dict[str, Any],
        acquisition_progress: dict[str, Any],
        pre_retrieval_refresh: dict[str, Any],
        started_at: str = "",
        completed_at: str = "",
        awaiting_stage2_confirmation: bool = False,
        will_continue_public_web_stage: bool = False,
        acquisition_progress_status: str = "running",
        message: str = "",
    ) -> dict[str, Any]:
        preview_summary = dict(preview_artifact.get("summary") or {})
        preview_payload = self._build_stage1_preview_record(
            preview_artifact=preview_artifact,
            started_at=started_at,
            completed_at=completed_at,
        )
        analysis_stage_mode = str(getattr(request, "analysis_stage_mode", "") or "").strip().lower() or "single_stage"
        summary_payload = {
            **preview_summary,
            "status": "completed",
            "started_at": str(started_at or "").strip(),
            "completed_at": str(completed_at or _utc_now_iso()).strip(),
            "message": message
            or (
                "Stage 1 preview ready. Awaiting user approval for stage 2 AI analysis."
                if awaiting_stage2_confirmation
                else (
                    "Stage 1 preview ready. Continuing Public Web Stage 2 acquisition."
                    if will_continue_public_web_stage
                    else "Stage 1 preview ready. Continuing snapshot materialization."
                )
            ),
            "analysis_stage_mode": analysis_stage_mode,
            "stage1_preview": preview_payload,
            "request_preview": _build_request_preview_payload(
                request=request,
                effective_request=dict(preview_artifact.get("effective_request") or {}),
            ),
        }
        if awaiting_stage2_confirmation:
            summary_payload["awaiting_user_action"] = "continue_stage2"
            summary_payload["stage2_transition_state"] = "idle"
        if acquisition_progress:
            progress_payload = dict(acquisition_progress)
            if acquisition_progress_status == "completed":
                progress_payload["status"] = "completed"
                progress_payload["completed_at"] = _utc_now_iso()
            summary_payload["acquisition_progress"] = progress_payload
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            summary_payload["pre_retrieval_refresh"] = pre_retrieval_refresh
        return summary_payload

    def _publish_stage1_preview_after_linkedin_stage(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_state: dict[str, Any],
        acquisition_progress: dict[str, Any],
    ) -> dict[str, Any]:
        latest_job = self.store.get_job(job_id) or {}
        latest_summary = dict(latest_job.get("summary") or {})
        will_continue_public_web_stage = self._workflow_plan_includes_public_web_stage(plan)
        if self._stage1_preview_ready(latest_summary):
            return {"status": "skipped", "reason": "stage1_preview_already_ready"}
        if not (
            bool(acquisition_state.get("linkedin_stage_completed"))
            or (
                bool(acquisition_state.get("reused_snapshot_checkpoint"))
                and isinstance(acquisition_state.get("candidate_doc_path"), Path)
            )
        ):
            return {"status": "skipped", "reason": "linkedin_stage_not_ready"}

        snapshot_id = str(acquisition_state.get("snapshot_id") or "").strip()
        candidate_doc_path = acquisition_state.get("candidate_doc_path")
        stage_candidate_doc_path = acquisition_state.get("linkedin_stage_candidate_doc_path")
        latest_artifact_path = str(latest_job.get("artifact_path") or "")
        linkedin_stage_started_at = _earliest_timestamp_string(
            str(dict(latest_summary.get("linkedin_stage_1") or {}).get("started_at") or "").strip(),
            str(latest_job.get("created_at") or "").strip(),
        )
        linkedin_stage_payload = {
            "status": "completed",
            "snapshot_id": snapshot_id,
            "candidate_doc_path": str(candidate_doc_path or ""),
            "stage_candidate_doc_path": str(stage_candidate_doc_path or ""),
            "started_at": linkedin_stage_started_at,
            "completed_at": _utc_now_iso(),
        }
        linkedin_stage_summary_path = self._persist_acquisition_stage_summary_file(
            request=request,
            stage_name="linkedin_stage_1",
            summary_payload=linkedin_stage_payload,
            acquisition_state=acquisition_state,
        )
        if linkedin_stage_summary_path:
            linkedin_stage_payload["summary_path"] = linkedin_stage_summary_path
        if str(dict(latest_summary.get("linkedin_stage_1") or {}).get("status") or "").strip() != "completed":
            self.store.append_job_event(
                job_id,
                "acquiring",
                "completed",
                "LinkedIn Stage 1 acquisition completed.",
                linkedin_stage_payload,
            )

        working_summary = dict(latest_summary)
        working_summary["message"] = "Building Stage 1 deterministic preview"
        working_summary["linkedin_stage_1"] = linkedin_stage_payload
        working_summary.pop("blocked_task", None)
        working_summary.pop("blocked_task_id", None)
        if acquisition_progress:
            working_summary["acquisition_progress"] = dict(acquisition_progress)
        self._save_workflow_job_state(
            job_id=job_id,
            request=request,
            plan=plan,
            status="running",
            stage="acquiring",
            summary_payload=working_summary,
            artifact_path=latest_artifact_path,
        )

        pre_retrieval_refresh = self._refresh_running_workflow_before_retrieval(
            job_id=job_id,
            request=request,
            plan=plan,
            acquisition_state=acquisition_state,
            include_materialization_refresh=False,
            allow_stage_candidate_document_fast_path=True,
        )
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            acquisition_state["pre_retrieval_refresh"] = pre_retrieval_refresh

        stage1_preview_started_at = _utc_now_iso()
        self.store.append_job_event(job_id, "acquiring", "running", "Stage 1 deterministic preview started.")
        try:
            preview_artifact = self._execute_retrieval(
                job_id,
                request,
                plan,
                job_type="workflow",
                runtime_policy={
                    "workflow_snapshot_id": snapshot_id,
                    "workflow_candidate_doc_path": str(candidate_doc_path or ""),
                    "workflow_stage_candidate_doc_path": str(stage_candidate_doc_path or ""),
                    "outreach_layering": dict(acquisition_state.get("outreach_layering") or {}),
                    "analysis_stage": "stage_1_preview",
                    "summary_mode": "deterministic",
                },
                persist_job_state=False,
                artifact_name_suffix="preview",
                artifact_status="preview_ready",
            )
            preview_summary = self._build_stage1_preview_summary(
                request=request,
                preview_artifact=preview_artifact,
                acquisition_progress=acquisition_progress,
                pre_retrieval_refresh=pre_retrieval_refresh,
                started_at=stage1_preview_started_at,
                completed_at=_utc_now_iso(),
                awaiting_stage2_confirmation=False,
                will_continue_public_web_stage=will_continue_public_web_stage,
                acquisition_progress_status="running",
                message=(
                    "Stage 1 preview ready. Continuing Public Web Stage 2 acquisition."
                    if will_continue_public_web_stage
                    else "Stage 1 preview ready. Continuing snapshot materialization."
                ),
            )
            preview_summary["linkedin_stage_1"] = linkedin_stage_payload
            preview_summary_path = self._persist_acquisition_stage_summary_file(
                request=request,
                stage_name="stage_1_preview",
                summary_payload=preview_summary,
                acquisition_state=acquisition_state,
            )
            if preview_summary_path:
                preview_stage_payload = dict(preview_summary.get("stage1_preview") or {})
                preview_stage_payload["summary_path"] = preview_summary_path
                preview_summary["stage1_preview"] = preview_stage_payload
            self._save_workflow_job_state(
                job_id=job_id,
                request=request,
                plan=plan,
                status="running",
                stage="acquiring",
                summary_payload=preview_summary,
                artifact_path=latest_artifact_path,
            )
            self.store.append_job_event(
                job_id,
                "acquiring",
                "completed",
                (
                    "Stage 1 preview ready; continuing Public Web Stage 2 acquisition."
                    if will_continue_public_web_stage
                    else "Stage 1 preview ready; continuing snapshot materialization."
                ),
                {
                    "analysis_stage": "stage_1_preview",
                    "preview_artifact_path": str(preview_artifact.get("artifact_path") or ""),
                    "summary": dict(preview_artifact.get("summary") or {}),
                },
            )
            return {
                "status": "completed",
                "preview_artifact": preview_artifact,
                "pre_retrieval_refresh": pre_retrieval_refresh,
            }
        except Exception as exc:
            failed_summary = dict(working_summary)
            failed_summary["message"] = (
                "Stage 1 preview failed; continuing Public Web Stage 2 acquisition."
                if will_continue_public_web_stage
                else "Stage 1 preview failed; continuing snapshot materialization."
            )
            failed_summary["stage1_preview"] = {
                "status": "failed",
                "started_at": stage1_preview_started_at,
                "analysis_stage": "stage_1_preview",
                "artifact_path": "",
                "error": str(exc),
                "failed_at": _utc_now_iso(),
            }
            failed_preview_summary_path = self._persist_acquisition_stage_summary_file(
                request=request,
                stage_name="stage_1_preview",
                summary_payload=failed_summary,
                acquisition_state=acquisition_state,
            )
            if failed_preview_summary_path:
                failed_summary["stage1_preview"]["summary_path"] = failed_preview_summary_path
            if str(pre_retrieval_refresh.get("status") or "") == "completed":
                failed_summary["pre_retrieval_refresh"] = pre_retrieval_refresh
            self._save_workflow_job_state(
                job_id=job_id,
                request=request,
                plan=plan,
                status="running",
                stage="acquiring",
                summary_payload=failed_summary,
                artifact_path=latest_artifact_path,
            )
            self.store.append_job_event(
                job_id,
                "acquiring",
                "failed",
                (
                    "Stage 1 preview failed; continuing Public Web Stage 2 acquisition."
                    if will_continue_public_web_stage
                    else "Stage 1 preview failed; continuing snapshot materialization."
                ),
                {
                    "analysis_stage": "stage_1_preview",
                    "error": str(exc),
                },
            )
            return {
                "status": "failed",
                "reason": "stage1_preview_failed",
                "error": str(exc),
                "pre_retrieval_refresh": pre_retrieval_refresh,
            }

    def _mark_public_web_stage_2_completed(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_state: dict[str, Any],
        acquisition_progress: dict[str, Any],
    ) -> dict[str, Any]:
        latest_job = self.store.get_job(job_id) or {}
        latest_summary = dict(latest_job.get("summary") or {})
        if str(dict(latest_summary.get("public_web_stage_2") or {}).get("status") or "").strip() == "completed":
            return {"status": "skipped", "reason": "public_web_stage_2_already_marked"}
        if not bool(acquisition_state.get("public_web_stage_completed")):
            return {"status": "skipped", "reason": "public_web_stage_not_ready"}

        stage_payload = {
            "status": "completed",
            "snapshot_id": str(acquisition_state.get("snapshot_id") or "").strip(),
            "candidate_doc_path": str(acquisition_state.get("candidate_doc_path") or ""),
            "stage_candidate_doc_path": str(acquisition_state.get("public_web_stage_candidate_doc_path") or ""),
            "started_at": _earliest_timestamp_string(
                str(dict(latest_summary.get("public_web_stage_2") or {}).get("started_at") or "").strip(),
                str(dict(latest_summary.get("stage1_preview") or {}).get("completed_at") or "").strip(),
            ),
            "completed_at": _utc_now_iso(),
        }
        public_web_stage_summary_path = self._persist_acquisition_stage_summary_file(
            request=request,
            stage_name="public_web_stage_2",
            summary_payload=stage_payload,
            acquisition_state=acquisition_state,
        )
        if public_web_stage_summary_path:
            stage_payload["summary_path"] = public_web_stage_summary_path
        updated_summary = dict(latest_summary)
        updated_summary["message"] = "Public Web Stage 2 acquisition completed."
        updated_summary["public_web_stage_2"] = stage_payload
        updated_summary.pop("blocked_task", None)
        updated_summary.pop("blocked_task_id", None)
        if acquisition_progress:
            updated_summary["acquisition_progress"] = dict(acquisition_progress)
        self._save_workflow_job_state(
            job_id=job_id,
            request=request,
            plan=plan,
            status="running",
            stage="acquiring",
            summary_payload=updated_summary,
            artifact_path=str(latest_job.get("artifact_path") or ""),
        )
        self.store.append_job_event(
            job_id,
            "acquiring",
            "completed",
            "Public Web Stage 2 acquisition completed.",
            stage_payload,
        )
        return {"status": "completed", "public_web_stage_2": stage_payload}

    def _persist_completed_workflow_summary(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        artifact: dict[str, Any],
        preserved_summary: dict[str, Any],
    ) -> dict[str, Any]:
        final_summary = dict(artifact.get("summary") or {})
        latest_summary = self._workflow_job_summary(job_id)
        final_summary = self._restore_completed_workflow_stage_summary_contract(
            final_summary,
            preserved_summary,
            latest_summary,
        )
        final_summary.setdefault("status", "completed")
        final_summary.setdefault("analysis_stage", "stage_2_final")
        final_started_at = _earliest_timestamp_string(
            str(final_summary.get("started_at") or "").strip(),
            str(preserved_summary.get("stage_2_final_started_at") or "").strip(),
            str(latest_summary.get("stage_2_final_started_at") or "").strip(),
            str(dict(final_summary.get("public_web_stage_2") or {}).get("completed_at") or "").strip(),
        )
        if final_started_at:
            final_summary["started_at"] = final_started_at
        final_summary.setdefault("completed_at", _utc_now_iso())
        schedule_background_outreach_layering = self._outreach_layering_requires_background_reconcile(
            request=request,
            summary_payload=final_summary,
        )
        schedule_background_snapshot_materialization = self._snapshot_materialization_requires_background_reconcile(
            summary_payload=final_summary,
        )
        if schedule_background_outreach_layering:
            final_summary = self._mark_outreach_layering_background_scheduled(
                request=request,
                summary_payload=final_summary,
                source="workflow_completion",
            )
        if schedule_background_snapshot_materialization:
            final_summary = self._mark_snapshot_materialization_background_scheduled(
                summary_payload=final_summary,
                source="workflow_completion",
            )
        final_stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(final_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=final_summary,
        )
        if final_stage_summary_path:
            final_summary["stage_summary_path"] = final_stage_summary_path
        self._save_workflow_job_state(
            job_id=job_id,
            request=request,
            plan=plan,
            status="completed",
            stage="completed",
            summary_payload=final_summary,
            artifact_path=str(artifact.get("artifact_path") or ""),
        )
        self._sync_frontend_history_phase_for_job(
            job={
                "job_id": job_id,
                "status": "completed",
                "stage": "completed",
                "request": request.to_record(),
                "plan": _plan_payload(plan),
            },
            phase="results",
        )
        if schedule_background_outreach_layering:
            self._queue_background_outreach_layering_reconcile(
                job_id=job_id,
                source="workflow_completion",
            )
        if schedule_background_snapshot_materialization:
            self._queue_background_snapshot_materialization_reconcile(
                job_id=job_id,
                source="workflow_completion",
            )
        self.cleanup_duplicate_inflight_workflows({"target_company": request.target_company, "active_limit": 200})
        return final_summary

    def _build_background_reconcile_cursor(self, workers: list[dict[str, Any]]) -> dict[str, Any]:
        normalized_workers = [dict(worker or {}) for worker in list(workers or []) if isinstance(worker, dict)]
        if not normalized_workers:
            return {}
        return {
            "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in normalized_workers),
            "worker_ids": [
                int(worker.get("worker_id") or 0)
                for worker in normalized_workers
                if int(worker.get("worker_id") or 0) > 0
            ],
            "applied_worker_count": len(normalized_workers),
        }

    def _worker_newer_than_reconcile_cursor(
        self,
        worker: dict[str, Any],
        cursor: dict[str, Any],
    ) -> bool:
        if not cursor:
            return True
        last_worker_updated_at = str(cursor.get("last_worker_updated_at") or "").strip()
        worker_updated_at = str(worker.get("updated_at") or "").strip()
        if last_worker_updated_at and worker_updated_at:
            if worker_updated_at > last_worker_updated_at:
                return True
            if worker_updated_at < last_worker_updated_at:
                return False
        consumed_worker_ids = {
            int(item)
            for item in list(cursor.get("worker_ids") or [])
            if int(item or 0) > 0
        }
        worker_id = int(worker.get("worker_id") or 0)
        if worker_id > 0 and worker_id in consumed_worker_ids:
            return False
        return not bool(last_worker_updated_at and worker_updated_at)

    def _pending_background_workers_after_cursor(
        self,
        workers: list[dict[str, Any]],
        *,
        acquisition_state: dict[str, Any],
        worker_kind: str,
    ) -> list[dict[str, Any]]:
        cursor_state = dict(acquisition_state.get("background_reconcile_cursor") or {})
        worker_cursor = dict(cursor_state.get(worker_kind) or {})
        if not worker_cursor:
            return [dict(worker or {}) for worker in list(workers or []) if isinstance(worker, dict)]
        return [
            dict(worker or {})
            for worker in list(workers or [])
            if isinstance(worker, dict) and self._worker_newer_than_reconcile_cursor(worker, worker_cursor)
        ]

    def _capture_inline_background_reconcile_cursor(
        self,
        *,
        job_id: str,
        task: AcquisitionTask,
        acquisition_state: dict[str, Any],
    ) -> None:
        if task.task_type != "enrich_linkedin_profiles":
            return
        workers = self.agent_runtime.list_workers(job_id=job_id)
        completed_harvest_workers = [
            worker for worker in workers if _worker_has_completed_background_harvest_prefetch(worker)
        ]
        harvest_cursor = self._build_background_reconcile_cursor(completed_harvest_workers)
        if not harvest_cursor:
            return
        cursor_state = dict(acquisition_state.get("background_reconcile_cursor") or {})
        cursor_state["harvest_prefetch"] = harvest_cursor
        acquisition_state["background_reconcile_cursor"] = cursor_state

    def _resolve_stage1_preview_candidate_doc_path(
        self,
        *,
        request: JobRequest,
        plan: Any,
        runtime_policy: dict[str, Any],
    ) -> str:
        snapshot_id = str(runtime_policy.get("workflow_snapshot_id") or "").strip()
        full_candidate_doc_path = str(runtime_policy.get("workflow_candidate_doc_path") or "").strip()
        stage_candidate_doc_path = str(runtime_policy.get("workflow_stage_candidate_doc_path") or "").strip()
        if not full_candidate_doc_path:
            return stage_candidate_doc_path
        if not stage_candidate_doc_path or stage_candidate_doc_path == full_candidate_doc_path:
            return full_candidate_doc_path

        preview_candidate_source = load_candidate_document_candidate_source(
            request=request,
            candidate_doc_path=full_candidate_doc_path,
            snapshot_id=snapshot_id,
            asset_view=request.asset_view,
        )
        if not list(preview_candidate_source.get("candidates") or []):
            return stage_candidate_doc_path

        plan_payload = _plan_payload(plan)
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or getattr(plan, "asset_reuse_plan", {}) or {})
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=preview_candidate_source,
        )
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=preview_candidate_source,
        )
        if (
            str(dict(effective_execution_semantics or {}).get("default_results_mode") or "").strip().lower()
            == "asset_population"
        ):
            return full_candidate_doc_path
        return stage_candidate_doc_path

    def _refresh_running_workflow_before_retrieval(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_state: dict[str, Any],
        include_materialization_refresh: bool = True,
        allow_stage_candidate_document_fast_path: bool = False,
        defer_harvest_prefetch_refresh_to_background: bool = False,
    ) -> dict[str, Any]:
        snapshot_dir = self._resolved_acquisition_snapshot_dir(acquisition_state)
        if not isinstance(snapshot_dir, Path):
            return {"status": "skipped", "reason": "snapshot_dir_missing"}
        workers = self.agent_runtime.list_workers(job_id=job_id)
        completed_company_roster_workers = [
            worker for worker in workers if _worker_has_completed_background_company_roster_output(worker)
        ]
        completed_search_seed_workers = [
            worker for worker in workers if _worker_has_completed_background_search_output(worker)
        ]
        completed_harvest_workers = [
            worker for worker in workers if _worker_has_completed_background_harvest_prefetch(worker)
        ]
        completed_company_roster_workers = self._pending_background_workers_after_cursor(
            completed_company_roster_workers,
            acquisition_state=acquisition_state,
            worker_kind="company_roster",
        )
        completed_search_seed_workers = self._pending_background_workers_after_cursor(
            completed_search_seed_workers,
            acquisition_state=acquisition_state,
            worker_kind="search_seed",
        )
        completed_harvest_workers = self._pending_background_workers_after_cursor(
            completed_harvest_workers,
            acquisition_state=acquisition_state,
            worker_kind="harvest_prefetch",
        )
        equivalent_baseline_snapshot_active = self._equivalent_baseline_snapshot_active(
            acquisition_state,
            snapshot_dir=snapshot_dir,
        )
        reused_snapshot_serving_artifacts_ready = self._reused_snapshot_serving_artifacts_ready(acquisition_state)
        reused_snapshot_candidate_documents_ready = self._reused_snapshot_candidate_documents_ready_for_direct_reuse(
            acquisition_state
        )
        materialization_refresh_pending = self._snapshot_materialization_refresh_required(snapshot_dir)
        if (
            equivalent_baseline_snapshot_active
            or reused_snapshot_serving_artifacts_ready
            or reused_snapshot_candidate_documents_ready
        ):
            materialization_refresh_pending = False
        materialization_refresh_required = materialization_refresh_pending and include_materialization_refresh
        stage_candidate_doc_path = acquisition_state.get("linkedin_stage_candidate_doc_path")
        if (
            allow_stage_candidate_document_fast_path
            and not completed_company_roster_workers
            and not completed_search_seed_workers
            and completed_harvest_workers
            and not materialization_refresh_required
            and isinstance(stage_candidate_doc_path, Path)
            and stage_candidate_doc_path.exists()
        ):
            return {
                "status": "skipped",
                "reason": "stage_candidate_documents_ready",
                "snapshot_id": str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                "company_roster_worker_count": 0,
                "search_seed_worker_count": 0,
                "harvest_prefetch_worker_count": len(completed_harvest_workers),
                "stage_candidate_doc_path": str(stage_candidate_doc_path),
            }
        if (
            defer_harvest_prefetch_refresh_to_background
            and not completed_company_roster_workers
            and not completed_search_seed_workers
            and completed_harvest_workers
        ):
            return {
                "status": "skipped",
                "reason": "harvest_prefetch_refresh_deferred_to_background",
                "snapshot_id": str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                "company_roster_worker_count": 0,
                "search_seed_worker_count": 0,
                "harvest_prefetch_worker_count": len(completed_harvest_workers),
                "materialization_refresh_required": materialization_refresh_required,
                "materialization_refresh_pending": materialization_refresh_pending,
                "materialization_refresh_deferred": (
                    materialization_refresh_pending and not include_materialization_refresh
                ),
            }
        if (
            not completed_company_roster_workers
            and not completed_search_seed_workers
            and not completed_harvest_workers
            and not materialization_refresh_required
        ):
            return {
                "status": "skipped",
                "reason": (
                    "equivalent_baseline_reused"
                    if equivalent_baseline_snapshot_active
                    else (
                        "reused_snapshot_serving_artifacts"
                        if reused_snapshot_serving_artifacts_ready
                        else (
                            "reused_snapshot_checkpoint"
                            if reused_snapshot_candidate_documents_ready
                            else (
                                "materialization_refresh_deferred"
                                if materialization_refresh_pending and not include_materialization_refresh
                                else "no_completed_background_workers"
                            )
                        )
                    )
                ),
            }

        refresh_summary: dict[str, Any] = {
            "status": "completed",
            "snapshot_id": str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
            "company_roster_worker_count": len(completed_company_roster_workers),
            "search_seed_worker_count": len(completed_search_seed_workers),
            "harvest_prefetch_worker_count": len(completed_harvest_workers),
            "materialization_refresh_required": materialization_refresh_required,
            "materialization_refresh_pending": materialization_refresh_pending,
        }
        if materialization_refresh_pending and not include_materialization_refresh:
            refresh_summary["materialization_refresh_deferred"] = True

        cursor_state = dict(acquisition_state.get("background_reconcile_cursor") or {})
        company_roster_update: dict[str, Any] = {}
        search_seed_update: dict[str, Any] = {}
        harvest_prefetch_update: dict[str, Any] = {}
        baseline_profile_prefetch: dict[str, Any] = {}
        if completed_company_roster_workers:
            company_roster_update = self._apply_background_company_roster_workers_to_snapshot(
                snapshot_dir=snapshot_dir,
                pending_workers=completed_company_roster_workers,
            )
            roster_snapshot = company_roster_update.get("roster_snapshot")
            if isinstance(roster_snapshot, CompanyRosterSnapshot):
                acquisition_state["roster_snapshot"] = roster_snapshot
                acquisition_state["company_identity"] = roster_snapshot.company_identity
            refresh_summary["company_roster"] = {
                key: (str(value) if isinstance(value, Path) else value)
                for key, value in company_roster_update.items()
                if key not in {"roster_snapshot"}
            }
            cursor_state["company_roster"] = self._build_background_reconcile_cursor(completed_company_roster_workers)
        if completed_search_seed_workers:
            search_seed_update = self._apply_background_search_seed_workers_to_snapshot(
                snapshot_dir=snapshot_dir,
                pending_workers=completed_search_seed_workers,
            )
            search_seed_snapshot = search_seed_update.get("search_seed_snapshot")
            if isinstance(search_seed_snapshot, SearchSeedSnapshot):
                acquisition_state["search_seed_snapshot"] = search_seed_snapshot
            refresh_summary["search_seed"] = {
                key: (str(value) if isinstance(value, Path) else value)
                for key, value in search_seed_update.items()
                if key not in {"search_seed_snapshot"}
            }
            cursor_state["search_seed"] = self._build_background_reconcile_cursor(completed_search_seed_workers)
        if completed_harvest_workers:
            harvest_prefetch_update = self._apply_background_harvest_prefetch_workers_to_snapshot(
                snapshot_dir=snapshot_dir,
                pending_workers=completed_harvest_workers,
            )
            if str(harvest_prefetch_update.get("candidate_doc_path") or "").strip():
                acquisition_state["candidate_doc_path"] = Path(
                    str(harvest_prefetch_update.get("candidate_doc_path") or "")
                ).expanduser()
            refresh_summary["harvest_prefetch"] = {
                key: (str(value) if isinstance(value, Path) else value)
                for key, value in harvest_prefetch_update.items()
            }
        if completed_harvest_workers:
            cursor_state["harvest_prefetch"] = self._build_background_reconcile_cursor(completed_harvest_workers)
        if cursor_state:
            acquisition_state["background_reconcile_cursor"] = cursor_state

        if completed_company_roster_workers and str(company_roster_update.get("status") or "") == "applied":
            baseline_profile_prefetch = self._queue_background_profile_prefetch_from_available_baselines(
                job_id=job_id,
                request=request,
                plan_payload=_plan_payload(plan),
                snapshot_dir=snapshot_dir,
                roster_snapshot=acquisition_state.get("roster_snapshot"),
                search_seed_snapshot=acquisition_state.get("search_seed_snapshot"),
            )
            if baseline_profile_prefetch:
                refresh_summary.setdefault("company_roster", {})["profile_prefetch"] = dict(baseline_profile_prefetch)
                if "search_seed" in refresh_summary:
                    refresh_summary["search_seed"]["profile_prefetch"] = dict(baseline_profile_prefetch)
        elif completed_search_seed_workers and str(search_seed_update.get("status") or "") == "applied":
            baseline_profile_prefetch = self._queue_background_profile_prefetch_from_search_seed_snapshot(
                job_id=job_id,
                request=request,
                plan_payload=_plan_payload(plan),
                snapshot_dir=snapshot_dir,
                search_seed_snapshot=acquisition_state.get("search_seed_snapshot"),
            )
            if baseline_profile_prefetch:
                refresh_summary.setdefault("search_seed", {})["profile_prefetch"] = dict(baseline_profile_prefetch)

        if str(baseline_profile_prefetch.get("status") or "").strip().lower() == "completed":
            existing_harvest_worker_ids = {
                int(worker.get("worker_id") or 0)
                for worker in completed_harvest_workers
                if int(worker.get("worker_id") or 0) > 0
            }
            immediate_harvest_workers = self._collect_completed_harvest_prefetch_workers_for_snapshot(
                job_id=job_id,
                snapshot_dir=snapshot_dir,
                exclude_worker_ids=existing_harvest_worker_ids,
            )
            if immediate_harvest_workers:
                immediate_harvest_update = self._apply_background_harvest_prefetch_workers_to_snapshot(
                    snapshot_dir=snapshot_dir,
                    pending_workers=immediate_harvest_workers,
                )
                if str(immediate_harvest_update.get("status") or "") == "applied":
                    completed_harvest_workers.extend(immediate_harvest_workers)
                    harvest_prefetch_update = immediate_harvest_update
                    if str(immediate_harvest_update.get("candidate_doc_path") or "").strip():
                        acquisition_state["candidate_doc_path"] = Path(
                            str(immediate_harvest_update.get("candidate_doc_path") or "")
                        ).expanduser()
                    refresh_summary["harvest_prefetch"] = {
                        key: (str(value) if isinstance(value, Path) else value)
                        for key, value in immediate_harvest_update.items()
                    }
                    cursor_state["harvest_prefetch"] = self._build_background_reconcile_cursor(
                        completed_harvest_workers
                    )
                    acquisition_state["background_reconcile_cursor"] = cursor_state

        if (
            completed_company_roster_workers
            or completed_search_seed_workers
            or completed_harvest_workers
            or materialization_refresh_required
        ):
            sync_result = self._synchronize_snapshot_candidate_documents(
                request=request,
                snapshot_dir=snapshot_dir,
                reason="pre_retrieval_refresh",
            )
            state_updates = dict(sync_result.get("state_updates") or {})
            acquisition_state.update(state_updates)
            refresh_summary["sync"] = {key: value for key, value in sync_result.items() if key != "state_updates"}

        latest_job = self.store.get_job(job_id) or {}
        summary = dict(latest_job.get("summary") or {})
        summary["pre_retrieval_refresh"] = refresh_summary
        background_reconcile = dict(summary.get("background_reconcile") or {})
        if completed_company_roster_workers:
            background_reconcile["company_roster"] = self._build_inline_background_reconcile_record(
                worker_kind="company_roster",
                workers=completed_company_roster_workers,
                snapshot_id=str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                apply_result=company_roster_update,
                sync_result=sync_result,
                profile_prefetch=baseline_profile_prefetch,
            )
        if completed_search_seed_workers:
            background_reconcile["search_seed"] = self._build_inline_background_reconcile_record(
                worker_kind="search_seed",
                workers=completed_search_seed_workers,
                snapshot_id=str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                apply_result=search_seed_update,
                sync_result=sync_result,
                profile_prefetch=baseline_profile_prefetch,
            )
        if completed_harvest_workers:
            background_reconcile["harvest_prefetch"] = self._build_inline_background_reconcile_record(
                worker_kind="harvest_prefetch",
                workers=completed_harvest_workers,
                snapshot_id=str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                apply_result=harvest_prefetch_update,
                sync_result=sync_result,
            )
        if background_reconcile:
            summary["background_reconcile"] = background_reconcile
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "running"),
            stage=str(latest_job.get("stage") or "acquiring"),
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
            summary_payload=summary,
            artifact_path=str(latest_job.get("artifact_path") or ""),
        )
        self.store.append_job_event(
            job_id,
            stage="acquiring",
            status="completed",
            detail="Pre-retrieval refresh synchronized snapshot artifacts before retrieval.",
            payload=refresh_summary,
        )
        return refresh_summary

    def _snapshot_artifact_readiness(self, snapshot_dir: Path) -> dict[str, Any]:
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        canonical_manifest_path = snapshot_dir / "normalized_artifacts" / "manifest.json"
        strict_manifest_path = snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json"
        canonical_summary_path = snapshot_dir / "normalized_artifacts" / "artifact_summary.json"
        strict_summary_path = snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json"
        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
        candidate_documents_present = candidate_doc_path.exists()
        normalized_paths = (
            canonical_manifest_path,
            strict_manifest_path,
            canonical_summary_path,
            strict_summary_path,
        )
        normalized_artifacts_ready = all(path.exists() for path in normalized_paths)
        materialization_refresh_required = False
        retrieval_index_refresh_required = False
        candidate_doc_mtime = 0.0
        normalized_oldest_mtime = 0.0
        normalized_newest_mtime = 0.0
        try:
            if candidate_documents_present:
                candidate_doc_mtime = candidate_doc_path.stat().st_mtime
            if normalized_artifacts_ready:
                normalized_mtimes = [path.stat().st_mtime for path in normalized_paths]
                normalized_oldest_mtime = min(normalized_mtimes)
                normalized_newest_mtime = max(normalized_mtimes)
            if candidate_documents_present:
                if not normalized_artifacts_ready:
                    materialization_refresh_required = True
                else:
                    materialization_refresh_required = normalized_oldest_mtime < candidate_doc_mtime
            if candidate_documents_present or normalized_artifacts_ready:
                if not retrieval_index_path.exists():
                    retrieval_index_refresh_required = True
                else:
                    dependency_mtime = max(candidate_doc_mtime, normalized_newest_mtime)
                    retrieval_index_refresh_required = retrieval_index_path.stat().st_mtime < dependency_mtime
        except OSError:
            materialization_refresh_required = candidate_documents_present
            retrieval_index_refresh_required = candidate_documents_present or normalized_artifacts_ready
        return {
            "candidate_documents_present": candidate_documents_present,
            "normalized_artifacts_ready": normalized_artifacts_ready,
            "materialization_refresh_required": materialization_refresh_required,
            "retrieval_index_refresh_required": retrieval_index_refresh_required,
            "candidate_doc_path": candidate_doc_path,
            "canonical_manifest_path": canonical_manifest_path,
            "strict_manifest_path": strict_manifest_path,
            "canonical_summary_path": canonical_summary_path,
            "strict_summary_path": strict_summary_path,
            "retrieval_index_path": retrieval_index_path,
        }

    def _snapshot_materialization_refresh_required(self, snapshot_dir: Path) -> bool:
        readiness = self._snapshot_artifact_readiness(snapshot_dir)
        return bool(readiness.get("materialization_refresh_required"))

    def _snapshot_retrieval_index_refresh_required(self, snapshot_dir: Path) -> bool:
        readiness = self._snapshot_artifact_readiness(snapshot_dir)
        return bool(readiness.get("retrieval_index_refresh_required"))

    def _resolve_outreach_ai_verification_budget(self, request: JobRequest) -> int | None:
        configured_budget = _env_optional_int("OUTREACH_LAYERING_MAX_AI_VERIFICATIONS")
        if configured_budget is not None:
            return max(0, configured_budget)

        if any(
            int(value or 0) > 0
            for value in [
                request.profile_detail_limit,
                request.publication_scan_limit,
                request.publication_lead_limit,
                request.exploration_limit,
                request.scholar_coauthor_follow_up_limit,
            ]
        ):
            return None
        return 0

    def _resume_blocked_workflows_after_recovery(
        self,
        daemon_summary: dict[str, Any],
        *,
        explicit_job_id: str = "",
        stale_job_scope_job_id: str = "",
        include_stale_acquiring: bool = True,
        stale_after_seconds: int = 60,
        resume_limit: int = 50,
        include_stale_queued: bool = True,
        queued_stale_after_seconds: int = 60,
        queued_resume_limit: int = 50,
    ) -> list[dict[str, Any]]:
        candidate_job_ids = {
            str(item.get("job_id") or "").strip()
            for item in list(daemon_summary.get("jobs") or [])
            if str(item.get("job_id") or "").strip()
        }
        if explicit_job_id.strip():
            candidate_job_ids.add(explicit_job_id.strip())
        if include_stale_acquiring:
            stale_jobs = self.store.list_stale_workflow_jobs_in_acquiring(
                statuses=["running", "blocked"],
                stale_after_seconds=max(0, int(stale_after_seconds or 0)),
                limit=max(1, int(resume_limit or 50)),
            )
            for job in stale_jobs:
                job_id = str(job.get("job_id") or "").strip()
                if stale_job_scope_job_id and job_id != stale_job_scope_job_id:
                    continue
                if job_id:
                    candidate_job_ids.add(job_id)
        if include_stale_queued:
            stale_queued_jobs = self.store.list_stale_workflow_jobs_in_queue(
                stale_after_seconds=max(0, int(queued_stale_after_seconds or 0)),
                limit=max(1, int(queued_resume_limit or 50)),
            )
            for job in stale_queued_jobs:
                job_id = str(job.get("job_id") or "").strip()
                if stale_job_scope_job_id and job_id != stale_job_scope_job_id:
                    continue
                if job_id:
                    candidate_job_ids.add(job_id)
        results: list[dict[str, Any]] = []
        for job_id in sorted(candidate_job_ids)[: max(1, int(resume_limit or 50))]:
            job = self.store.get_job(job_id) or {}
            job_status = str(job.get("status") or "").strip().lower()
            job_stage = str(job.get("stage") or "").strip().lower()
            if job_stage == "planning" and job_status == "running":
                results.append(self._resume_planning_workflow_if_ready(job_id))
                continue
            if job_stage == "acquiring" and job_status in {"running", "blocked"}:
                results.append(self._resume_acquiring_workflow_if_ready(job_id))
                continue
            if job_status == "queued":
                results.append(self._resume_queued_workflow_if_ready(job_id))
                continue
            results.append(
                {
                    "job_id": job_id,
                    "status": "skipped",
                    "reason": "job_not_resumable",
                    "job_status": job_status,
                    "job_stage": job_stage,
                }
            )
        return results

    def _release_stale_workflow_job_lease_for_recovery(self, job_id: str) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"job_id": normalized_job_id, "released": False, "reason": "job_id_missing"}
        lease = self.store.get_workflow_job_lease(normalized_job_id)
        takeover_state = self._workflow_recovery_takeover_state(normalized_job_id)
        if not lease:
            return {
                "job_id": normalized_job_id,
                "released": False,
                "reason": "lease_missing",
                "classification": str(takeover_state.get("classification") or ""),
            }
        if not bool(takeover_state.get("allowed")):
            return {
                "job_id": normalized_job_id,
                "released": False,
                "reason": "runtime_not_takeover_safe",
                "classification": str(takeover_state.get("classification") or ""),
            }

        self.store.release_workflow_job_lease(normalized_job_id)
        self.store.append_job_event(
            normalized_job_id,
            stage="runtime_control",
            status="recovered",
            detail="Released stale workflow job lease after runner loss so recovery can take over.",
            payload={
                "classification": str(takeover_state.get("classification") or ""),
                "previous_lease_owner": str(lease.get("lease_owner") or ""),
                "previous_lease_token": str(lease.get("lease_token") or ""),
                "workflow_runner_pid": int(takeover_state.get("runner_pid") or 0),
                "workflow_runner_alive": bool(takeover_state.get("runner_alive")),
            },
        )
        return {
            "job_id": normalized_job_id,
            "released": True,
            "classification": str(takeover_state.get("classification") or ""),
            "lease_owner": str(lease.get("lease_owner") or ""),
            "lease_token": str(lease.get("lease_token") or ""),
        }

    def _workflow_recovery_takeover_state(
        self,
        job_id: str,
        *,
        ignore_lease_owner: str = "",
        ignore_lease_token: str = "",
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {"job_id": normalized_job_id, "allowed": False, "reason": "job_id_missing"}
        job = self.store.get_job(normalized_job_id) or {}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": normalized_job_id, "allowed": False, "reason": "not_workflow_job"}

        summary = dict(job.get("summary") or {})
        runtime_controls = self._build_live_runtime_controls_payload(job)
        workflow_job_lease = dict(runtime_controls.get("workflow_job_lease") or {})
        if workflow_job_lease:
            lease_owner = str(workflow_job_lease.get("lease_owner") or "").strip()
            lease_token = str(workflow_job_lease.get("lease_token") or "").strip()
            if lease_owner and lease_owner == str(ignore_lease_owner or "").strip():
                token_filter = str(ignore_lease_token or "").strip()
                if not token_filter or lease_token == token_filter:
                    runtime_controls.pop("workflow_job_lease", None)
        workers = self.agent_runtime.list_workers(job_id=normalized_job_id)
        blocked_task = str(summary.get("blocked_task") or "")
        worker_summary = _job_worker_summary(workers)
        runtime_health = _classify_job_runtime_health(
            job=job,
            workers=workers,
            worker_summary=worker_summary,
            runtime_controls=runtime_controls,
            blocked_task=blocked_task,
        )
        classification = str(runtime_health.get("classification") or "").strip()
        runner = dict(runtime_controls.get("workflow_runner") or {})
        runner_control = dict(runtime_controls.get("workflow_runner_control") or {})
        runner_pid = int(runner.get("pid") or 0)
        runner_alive = (
            bool(runner.get("process_alive"))
            if "process_alive" in runner
            else _workflow_runner_process_alive(runner_pid)
        )
        runner_known_dead = runner_pid > 0 and not runner_alive
        runner_attempted = bool(runner) or str(runner_control.get("status") or "").strip().lower() in {
            "started",
            "started_deferred",
            "failed",
        }

        allowed = False
        if classification == "runner_not_alive":
            allowed = True
        elif classification == "blocked_ready_for_resume" and (runner_known_dead or runner_attempted):
            allowed = True
        elif classification == "queued_waiting_for_runner" and (runner_known_dead or not runner_attempted):
            allowed = True
        return {
            "job_id": normalized_job_id,
            "allowed": allowed,
            "classification": classification,
            "runtime_health": runtime_health,
            "runner_pid": runner_pid,
            "runner_alive": runner_alive,
            "runner_attempted": runner_attempted,
        }

    def _recover_stale_workflow_job_lock(
        self,
        job_id: str,
        lock_path: Path,
        *,
        ignore_lease_owner: str = "",
        ignore_lease_token: str = "",
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        takeover_state = self._workflow_recovery_takeover_state(
            normalized_job_id,
            ignore_lease_owner=ignore_lease_owner,
            ignore_lease_token=ignore_lease_token,
        )
        if not bool(takeover_state.get("allowed")):
            return {
                "job_id": normalized_job_id,
                "recovered": False,
                "reason": "runtime_not_takeover_safe",
                "classification": str(takeover_state.get("classification") or ""),
            }
        try:
            lock_path.unlink(missing_ok=True)
        except OSError as exc:
            return {
                "job_id": normalized_job_id,
                "recovered": False,
                "reason": "lock_unlink_failed",
                "classification": str(takeover_state.get("classification") or ""),
                "error": str(exc),
            }
        self.store.append_job_event(
            normalized_job_id,
            stage="runtime_control",
            status="recovered",
            detail="Removed stale workflow job file lock after runner loss so recovery can take over.",
            payload={
                "classification": str(takeover_state.get("classification") or ""),
                "lock_path": str(lock_path),
                "workflow_runner_pid": int(takeover_state.get("runner_pid") or 0),
                "workflow_runner_alive": bool(takeover_state.get("runner_alive")),
            },
        )
        return {
            "job_id": normalized_job_id,
            "recovered": True,
            "classification": str(takeover_state.get("classification") or ""),
        }

    def _resume_queued_workflow_if_ready(
        self,
        job_id: str,
        *,
        assume_lock: bool = False,
        allow_stale_lease_takeover: bool = True,
    ) -> dict[str, Any]:
        if not assume_lock:
            job = self.store.get_job(job_id) or {}
            if (
                str(job.get("job_type") or "") == "workflow"
                and str(job.get("status") or "").strip().lower() == "queued"
                and self._workflow_prefers_hosted_execution(job)
            ):
                takeover_state = self._workflow_recovery_takeover_state(job_id)
                if not bool(takeover_state.get("allowed")):
                    return {
                        "job_id": job_id,
                        "status": "skipped",
                        "reason": "runtime_not_takeover_safe",
                        "classification": str(takeover_state.get("classification") or ""),
                    }
                hosted_result = self._start_hosted_workflow_thread(job_id, source="workflow_recovery")
                hosted_status = str(hosted_result.get("status") or "").strip()
                if hosted_status == "started":
                    return {
                        "job_id": job_id,
                        "status": "takeover_started",
                        "mode": "hosted",
                        "hosted_dispatch": hosted_result,
                    }
                if hosted_status == "skipped" and str(hosted_result.get("reason") or "").strip() == "hosted_dispatch_inflight":
                    return {
                        "job_id": job_id,
                        "status": "takeover_inflight",
                        "mode": "hosted",
                        "hosted_dispatch": hosted_result,
                    }
                return {
                    "job_id": job_id,
                    "status": "takeover_failed",
                    "mode": "hosted",
                    "hosted_dispatch": hosted_result,
                }
        if not assume_lock:
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
                    takeover = (
                        self._release_stale_workflow_job_lease_for_recovery(job_id)
                        if allow_stale_lease_takeover
                        else {"released": False}
                    )
                    if bool(takeover.get("released")):
                        return self._resume_queued_workflow_if_ready(
                            job_id,
                            assume_lock=False,
                            allow_stale_lease_takeover=False,
                        )
                    latest_job = self.store.get_job(job_id) or {}
                    return {
                        "job_id": job_id,
                        "status": "skipped",
                        "stage": str(latest_job.get("stage") or ""),
                        "reason": "already_running",
                    }
                return self._resume_queued_workflow_if_ready(job_id, assume_lock=True)
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        job_status = str(job.get("status") or "").strip().lower()
        if job_status != "queued":
            return {
                "job_id": job_id,
                "status": "skipped",
                "reason": "job_not_queued",
                "job_status": str(job.get("status") or ""),
                "job_stage": str(job.get("stage") or ""),
            }
        if self._workflow_prefers_hosted_execution(job):
            hosted_result = self._start_hosted_workflow_thread(job_id, source="workflow_recovery")
            hosted_status = str(hosted_result.get("status") or "").strip()
            if hosted_status == "started":
                return {
                    "job_id": job_id,
                    "status": "takeover_started",
                    "mode": "hosted",
                    "hosted_dispatch": hosted_result,
                }
            if hosted_status == "skipped" and str(hosted_result.get("reason") or "").strip() == "hosted_dispatch_inflight":
                return {
                    "job_id": job_id,
                    "status": "takeover_inflight",
                    "mode": "hosted",
                    "hosted_dispatch": hosted_result,
                }
            return {
                "job_id": job_id,
                "status": "takeover_failed",
                "mode": "hosted",
                "hosted_dispatch": hosted_result,
            }
        runner_control = self._start_workflow_runner_with_handshake(
            job_id=job_id,
            auto_job_daemon=True,
            handshake_timeout_seconds=None,
            poll_seconds=0.1,
            max_attempts=1,
        )
        runner_status = str(runner_control.get("status") or "").strip()
        runner_payload = dict(runner_control.get("runner") or {})
        self._persist_workflow_runtime_controls(
            job_id,
            {
                "workflow_runner": runner_payload,
                "workflow_runner_control": runner_control,
            },
        )
        if runner_status == "started":
            return {
                "job_id": job_id,
                "status": "takeover_started",
                "runner": runner_payload,
                "workflow_runner_control": runner_control,
            }
        if runner_status == "started_deferred":
            return {
                "job_id": job_id,
                "status": "takeover_started_deferred",
                "runner": runner_payload,
                "workflow_runner_control": runner_control,
            }
        return {
            "job_id": job_id,
            "status": "takeover_failed",
            "runner": runner_payload,
            "workflow_runner_control": runner_control,
        }

    def _spawn_workflow_takeover_runner(self, job_id: str, *, auto_job_daemon: bool) -> dict[str, Any]:
        log_dir = self.runtime_dir / "service_logs"
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
            **_spawn_detached_process(
                command=command,
                cwd=self.catalog.project_root,
                log_path=log_path,
                env=_runner_subprocess_env(self.catalog.project_root),
            ),
            "job_id": job_id,
        }

    def _start_workflow_runner_with_handshake(
        self,
        *,
        job_id: str,
        auto_job_daemon: bool,
        handshake_timeout_seconds: float | None,
        poll_seconds: float = 0.1,
        max_attempts: int = 2,
    ) -> dict[str, Any]:
        attempts_payload: list[dict[str, Any]] = []
        terminal_statuses = {"completed", "failed"}
        timeout_seconds = self._resolve_workflow_runner_handshake_timeout(handshake_timeout_seconds)
        for attempt in range(1, max(1, int(max_attempts or 1)) + 1):
            runner = self._spawn_workflow_takeover_runner(job_id, auto_job_daemon=auto_job_daemon)
            attempt_payload: dict[str, Any] = {
                "attempt": attempt,
                "runner": dict(runner),
            }
            if str(runner.get("status") or "") != "started":
                attempts_payload.append(attempt_payload)
                continue
            handshake = self._wait_for_workflow_runner_progress(
                job_id=job_id,
                pid=int(runner.get("pid") or 0),
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
            attempt_payload["handshake"] = dict(handshake)
            attempts_payload.append(attempt_payload)
            handshake_status = str(handshake.get("status") or "")
            handshake_job_status = str(handshake.get("job_status") or "").strip().lower()
            if handshake_status == "advanced" and handshake_job_status not in terminal_statuses:
                pid = int(runner.get("pid") or 0)
                if pid > 0 and not _workflow_runner_process_alive(pid):
                    attempt_payload["handshake"] = {
                        **dict(handshake),
                        "status": "runner_exited_after_advance",
                        "pid": pid,
                    }
                    continue
            if handshake_status in {"advanced", "timeout_runner_alive"}:
                return {
                    "status": "started" if handshake_status == "advanced" else "started_deferred",
                    "attempts": attempts_payload,
                    "runner": dict(runner),
                    "handshake": dict(handshake),
                }

        takeover = self.run_worker_recovery_once(
            {
                "job_id": job_id,
                "workflow_auto_resume_enabled": True,
                "workflow_resume_stale_after_seconds": 0,
                "workflow_resume_limit": 1,
                "workflow_queue_auto_takeover_enabled": True,
                "workflow_queue_resume_stale_after_seconds": 0,
                "workflow_queue_resume_limit": 1,
            }
        )
        return {
            "status": "failed",
            "attempts": attempts_payload,
            "takeover": takeover,
        }

    def _wait_for_workflow_runner_progress(
        self,
        *,
        job_id: str,
        pid: int,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> dict[str, Any]:
        return _wait_for_job_status_transition(
            get_job=lambda: self.get_job(job_id) or {},
            pid=pid,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
            waiting_statuses={"queued"},
        )

    def _resolve_workflow_runner_handshake_timeout(self, value: float | None) -> float:
        return _resolve_timeout(
            value,
            env_key="START_WORKFLOW_RUNNER_HANDSHAKE_SECONDS",
            default=4.0,
            minimum=0.2,
        )

    def _resume_acquiring_workflow_if_ready(
        self,
        job_id: str,
        *,
        assume_lock: bool = False,
        allow_stale_lease_takeover: bool = True,
    ) -> dict[str, Any]:
        if not assume_lock:
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
                    takeover = (
                        self._release_stale_workflow_job_lease_for_recovery(job_id)
                        if allow_stale_lease_takeover
                        else {"released": False}
                    )
                    if bool(takeover.get("released")):
                        return self._resume_acquiring_workflow_if_ready(
                            job_id,
                            assume_lock=False,
                            allow_stale_lease_takeover=False,
                        )
                    latest_job = self.store.get_job(job_id) or {}
                    return {
                        "job_id": job_id,
                        "status": "skipped",
                        "stage": str(latest_job.get("stage") or ""),
                        "reason": "already_running",
                    }
                return self._resume_acquiring_workflow_if_ready(job_id, assume_lock=True)
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        job_status = str(job.get("status") or "")
        job_stage = str(job.get("stage") or "")
        if job_stage != "acquiring":
            return {"job_id": job_id, "status": "skipped", "reason": "job_not_in_acquisition"}
        if job_status == "blocked":
            return self._resume_blocked_workflow_if_ready(
                job_id,
                assume_lock=True,
                allow_stale_lease_takeover=allow_stale_lease_takeover,
            )
        if job_status == "running":
            return self._resume_running_workflow_if_ready(
                job_id,
                assume_lock=True,
                allow_stale_lease_takeover=allow_stale_lease_takeover,
            )
        return {
            "job_id": job_id,
            "status": "skipped",
            "reason": "job_not_resumable_acquisition_state",
            "job_status": job_status,
            "job_stage": job_stage,
        }

    def _planning_stage_completed(self, job_id: str) -> bool:
        return any(
            str(event.get("stage") or "").strip().lower() == "planning"
            and str(event.get("status") or "").strip().lower() == "completed"
            for event in self.store.list_job_events(job_id)
        )

    def _resume_planning_workflow_if_ready(
        self,
        job_id: str,
        *,
        assume_lock: bool = False,
        allow_stale_lease_takeover: bool = True,
    ) -> dict[str, Any]:
        if not assume_lock:
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
                    takeover = (
                        self._release_stale_workflow_job_lease_for_recovery(job_id)
                        if allow_stale_lease_takeover
                        else {"released": False}
                    )
                    if bool(takeover.get("released")):
                        return self._resume_planning_workflow_if_ready(
                            job_id,
                            assume_lock=False,
                            allow_stale_lease_takeover=False,
                        )
                    latest_job = self.store.get_job(job_id) or {}
                    return {
                        "job_id": job_id,
                        "status": "skipped",
                        "stage": str(latest_job.get("stage") or ""),
                        "reason": "already_running",
                    }
                return self._resume_planning_workflow_if_ready(job_id, assume_lock=True)
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        if str(job.get("status") or "") != "running" or str(job.get("stage") or "") != "planning":
            return {"job_id": job_id, "status": "skipped", "reason": "job_not_running_in_planning"}

        request = JobRequest.from_payload(dict(job.get("request") or {}))
        plan = hydrate_sourcing_plan(dict(job.get("plan") or {}))
        planning_completed = self._planning_stage_completed(job_id)
        try:
            if planning_completed:
                resume_result = self._run_workflow_from_acquisition(job_id, request, plan, resume_mode=True)
                resume_mode_name = "running_planning_to_acquisition_recovery"
            else:
                self._run_workflow(job_id, request, plan)
                resume_result = {"status": "replayed_from_planning"}
                resume_mode_name = "running_planning_replay"
            latest_job = self.store.get_job(job_id) or {}
            return {
                "job_id": job_id,
                "status": "resumed",
                "resume_mode": resume_mode_name,
                "planning_completed": planning_completed,
                "resume_result": resume_result,
                "job_status": str(latest_job.get("status") or ""),
                "job_stage": str(latest_job.get("stage") or ""),
            }
        except Exception as exc:
            self._mark_workflow_failed(job_id, request, plan, exc)
            return {
                "job_id": job_id,
                "status": "failed",
                "resume_mode": "running_planning_recovery",
                "planning_completed": planning_completed,
                "error": str(exc),
            }

    def _reconcile_completed_workflows_after_recovery(
        self,
        daemon_summary: dict[str, Any],
        *,
        explicit_job_id: str = "",
    ) -> list[dict[str, Any]]:
        candidate_job_ids = {
            str(item.get("job_id") or "").strip()
            for item in list(daemon_summary.get("jobs") or [])
            if str(item.get("job_id") or "").strip()
        }
        if explicit_job_id.strip():
            candidate_job_ids.add(explicit_job_id.strip())
        results: list[dict[str, Any]] = []
        for job_id in sorted(candidate_job_ids):
            results.append(self._reconcile_completed_workflow_if_needed(job_id))
        return results

    def _reconcile_completed_workflow_after_deferred_snapshot_materialization(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        if str(job.get("status") or "") != "completed":
            return {"job_id": job_id, "status": "skipped", "reason": "job_not_completed"}

        request = JobRequest.from_payload(dict(job.get("request") or {}))
        if not request.target_company.strip():
            return {"job_id": job_id, "status": "skipped", "reason": "target_company_missing"}
        plan_payload = dict(job.get("plan") or {})
        job_summary = dict(job.get("summary") or {})
        snapshot_materialization = dict(job_summary.get("background_snapshot_materialization") or {})
        snapshot_status = str(snapshot_materialization.get("status") or "").strip().lower()
        snapshot_id = str(snapshot_materialization.get("snapshot_id") or "").strip()
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}
        if snapshot_status in {"completed", "failed"}:
            return {"job_id": job_id, "status": "skipped", "reason": "already_reconciled"}
        snapshot_dir = resolve_company_snapshot_dir(
            self.runtime_dir,
            target_company=request.target_company,
            snapshot_id=snapshot_id,
        )
        if snapshot_dir is None or not snapshot_dir.exists():
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_dir_missing"}

        self.store.append_job_event(
            job_id,
            "completed",
            "running",
            "Background snapshot materialization reconcile started for deferred finalization.",
            payload={
                "snapshot_id": snapshot_id,
                "authoritative_snapshot_id": str(snapshot_materialization.get("authoritative_snapshot_id") or ""),
            },
        )
        sync_result = self._synchronize_snapshot_candidate_documents(
            request=request,
            snapshot_dir=snapshot_dir,
            reason="background_snapshot_materialization_reconcile",
        )
        if str(sync_result.get("status") or "") not in {"completed", "skipped"}:
            self.store.append_job_event(
                job_id,
                "completed",
                "failed",
                "Background snapshot materialization reconcile failed.",
                payload={"snapshot_id": snapshot_id, "sync_result": sync_result},
            )
            return {
                "job_id": job_id,
                "status": "failed",
                "reason": str(sync_result.get("reason") or "snapshot_sync_failed"),
            }

        retrieval_artifact = self._execute_retrieval(
            job_id,
            request,
            hydrate_sourcing_plan(plan_payload),
            job_type="workflow",
            runtime_policy={
                "mode": "background_reconcile",
                "workflow_snapshot_id": snapshot_id,
            },
        )
        latest_job = self.store.get_job(job_id) or job
        updated_summary = self._restore_completed_workflow_stage_summary_contract(
            dict(latest_job.get("summary") or {}),
            job_summary,
        )
        updated_summary["background_snapshot_materialization"] = {
            **snapshot_materialization,
            "status": "completed",
            "reconciled_at": _utc_now_iso(),
            "artifact_dir": str(sync_result.get("artifact_dir") or ""),
            "artifact_paths": dict(sync_result.get("artifact_paths") or {}),
            "sync_result": {
                key: value
                for key, value in dict(sync_result).items()
                if key != "state_updates"
            },
        }
        stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(updated_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=updated_summary,
        )
        if stage_summary_path:
            updated_summary["stage_summary_path"] = stage_summary_path
        artifact_path_value = str(retrieval_artifact.get("artifact_path") or latest_job.get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "completed"),
            stage=str(latest_job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=updated_summary,
            artifact_path=artifact_path_value,
        )
        artifact_path = Path(artifact_path_value).expanduser()
        if artifact_path.exists():
            try:
                artifact = json.loads(artifact_path.read_text())
            except (OSError, json.JSONDecodeError):
                artifact = {}
            artifact["summary"] = updated_summary
            artifact["background_snapshot_materialization"] = dict(
                updated_summary.get("background_snapshot_materialization") or {}
            )
            artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.append_job_event(
            job_id,
            "completed",
            "completed",
            "Background snapshot materialization reconcile refreshed results after deferred finalization.",
            payload=dict(updated_summary.get("background_snapshot_materialization") or {}),
        )
        return {
            "job_id": job_id,
            "status": "completed",
            "snapshot_id": snapshot_id,
            "artifact": retrieval_artifact,
            "sync_result": sync_result,
        }

    def _reconcile_completed_workflow_if_needed(self, job_id: str) -> dict[str, Any]:
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        if str(job.get("status") or "") != "completed":
            return {"job_id": job_id, "status": "skipped", "reason": "job_not_completed"}

        request = JobRequest.from_payload(dict(job.get("request") or {}))
        if not request.target_company.strip():
            return {"job_id": job_id, "status": "skipped", "reason": "target_company_missing"}
        plan_payload = dict(job.get("plan") or {})
        job_summary = dict(job.get("summary") or {})
        if self._snapshot_materialization_requires_background_reconcile(summary_payload=job_summary):
            return self._reconcile_completed_workflow_after_deferred_snapshot_materialization(job_id)
        reconcile_state = dict(job_summary.get("background_reconcile") or {})
        last_worker_updated_at = str(reconcile_state.get("last_worker_updated_at") or "").strip()
        company_roster_state = dict(reconcile_state.get("company_roster") or {})
        last_company_roster_worker_updated_at = str(company_roster_state.get("last_worker_updated_at") or "").strip()
        search_seed_state = dict(reconcile_state.get("search_seed") or {})
        last_search_seed_worker_updated_at = str(search_seed_state.get("last_worker_updated_at") or "").strip()
        workers = self.agent_runtime.list_workers(job_id=job_id)
        completed_company_roster_workers = [
            worker for worker in workers if _worker_has_completed_background_company_roster_output(worker)
        ]
        pending_company_roster_workers = [
            worker
            for worker in completed_company_roster_workers
            if not last_company_roster_worker_updated_at
            or str(worker.get("updated_at") or "").strip() > last_company_roster_worker_updated_at
        ]
        if pending_company_roster_workers:
            return self._reconcile_completed_workflow_after_company_roster(
                job=job,
                request=request,
                plan_payload=plan_payload,
                job_summary=job_summary,
                pending_workers=pending_company_roster_workers,
            )
        harvest_prefetch_state = dict(reconcile_state.get("harvest_prefetch") or {})
        last_harvest_worker_updated_at = str(harvest_prefetch_state.get("last_worker_updated_at") or "").strip()
        completed_harvest_workers = [
            worker for worker in workers if _worker_has_completed_background_harvest_prefetch(worker)
        ]
        pending_harvest_workers = [
            worker
            for worker in completed_harvest_workers
            if not last_harvest_worker_updated_at
            or str(worker.get("updated_at") or "").strip() > last_harvest_worker_updated_at
        ]
        if pending_harvest_workers:
            return self._reconcile_completed_workflow_after_harvest_prefetch(
                job=job,
                request=request,
                plan_payload=plan_payload,
                job_summary=job_summary,
                pending_workers=pending_harvest_workers,
            )

        completed_search_seed_workers = [
            worker for worker in workers if _worker_has_completed_background_search_output(worker)
        ]
        pending_search_seed_workers = [
            worker
            for worker in completed_search_seed_workers
            if not last_search_seed_worker_updated_at
            or str(worker.get("updated_at") or "").strip() > last_search_seed_worker_updated_at
        ]
        if pending_search_seed_workers:
            return self._reconcile_completed_workflow_after_search_seed(
                job=job,
                request=request,
                plan_payload=plan_payload,
                job_summary=job_summary,
                pending_workers=pending_search_seed_workers,
            )

        completed_exploration_workers = [
            worker
            for worker in workers
            if str(worker.get("lane_id") or "") == "exploration_specialist"
            and str(worker.get("status") or "") == "completed"
            and _worker_has_background_candidate_output(worker)
        ]
        if self._outreach_layering_requires_background_reconcile(request=request, summary_payload=job_summary):
            return self._reconcile_completed_workflow_after_outreach_layering(
                job=job,
                request=request,
                plan_payload=plan_payload,
                job_summary=job_summary,
            )
        if not completed_exploration_workers:
            return {"job_id": job_id, "status": "skipped", "reason": "no_completed_background_exploration_workers"}

        pending_workers = [
            worker
            for worker in completed_exploration_workers
            if not last_worker_updated_at or str(worker.get("updated_at") or "").strip() > last_worker_updated_at
        ]
        if not pending_workers:
            return {"job_id": job_id, "status": "skipped", "reason": "already_reconciled"}

        job_result_view = self.store.get_job_result_view(job_id=job_id)
        snapshot_id = _resolve_reconcile_snapshot_id(
            job_summary,
            pending_workers,
            job_result_view=job_result_view,
        )
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}

        self.store.append_job_event(
            job_id,
            "completed",
            "running",
            "Background reconcile started after worker recovery.",
            payload={
                "snapshot_id": snapshot_id,
                "pending_worker_count": len(pending_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
            },
        )

        candidate_upsert_count = 0
        evidence_upsert_count = 0
        worker_errors: list[dict[str, Any]] = []
        for worker in sorted(
            pending_workers,
            key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)),
        ):
            try:
                applied = self._apply_completed_exploration_worker_output(request.target_company, worker)
            except Exception as exc:
                worker_errors.append(
                    {
                        "worker_id": int(worker.get("worker_id") or 0),
                        "worker_key": str(worker.get("worker_key") or ""),
                        "error": str(exc),
                    }
                )
                continue
            candidate_upsert_count += int(applied.get("candidate_upsert_count") or 0)
            evidence_upsert_count += int(applied.get("evidence_upsert_count") or 0)

        artifact_build = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company=request.target_company,
            snapshot_id=snapshot_id,
            preferred_source_snapshot_ids=[
                str(item or "").strip()
                for item in list(dict(request.execution_preferences or {}).get("delta_baseline_snapshot_ids") or [])
                if str(item or "").strip()
            ]
            or None,
        )
        artifact = self._execute_retrieval(
            job_id,
            request,
            plan_payload,
            job_type="workflow",
            runtime_policy={
                "mode": "background_reconcile",
                "summary_mode": "deterministic",
                "workflow_snapshot_id": snapshot_id,
            },
        )
        last_worker_update = max(str(worker.get("updated_at") or "").strip() for worker in pending_workers)
        background_reconcile = {
            "status": "completed",
            "snapshot_id": snapshot_id,
            "reconciled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "applied_worker_count": len(pending_workers),
            "candidate_upsert_count": candidate_upsert_count,
            "evidence_upsert_count": evidence_upsert_count,
            "last_worker_updated_at": last_worker_update,
            "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
            "artifact_dir": str(artifact_build.get("artifact_dir") or ""),
            "artifact_paths": dict(artifact_build.get("artifact_paths") or {}),
            "errors": worker_errors,
        }
        latest_job = self.store.get_job(job_id) or job
        updated_summary = self._restore_completed_workflow_stage_summary_contract(
            dict(latest_job.get("summary") or {}),
            job_summary,
        )
        updated_summary["background_reconcile"] = background_reconcile
        stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(updated_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=updated_summary,
        )
        if stage_summary_path:
            updated_summary["stage_summary_path"] = stage_summary_path
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "completed"),
            stage=str(latest_job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=updated_summary,
            artifact_path=str(artifact.get("artifact_path") or latest_job.get("artifact_path") or ""),
        )
        artifact_path = Path(str(artifact.get("artifact_path") or "")).expanduser()
        if artifact_path.exists():
            artifact["summary"] = updated_summary
            artifact["background_reconcile"] = background_reconcile
            artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.append_job_event(
            job_id,
            "completed",
            "completed",
            "Background reconcile refreshed retrieval results after exploration recovery.",
            payload=background_reconcile,
        )
        return {
            "job_id": job_id,
            "status": "reconciled",
            "snapshot_id": snapshot_id,
            "applied_worker_count": len(pending_workers),
            "candidate_upsert_count": candidate_upsert_count,
            "evidence_upsert_count": evidence_upsert_count,
            "artifact_dir": str(artifact_build.get("artifact_dir") or ""),
            "errors": worker_errors,
        }

    def _reconcile_completed_workflow_after_outreach_layering(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        job_summary: dict[str, Any],
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id") or "").strip()
        outreach_layering = dict(job_summary.get("outreach_layering") or {})
        current_status = str(outreach_layering.get("status") or "").strip().lower()
        if current_status == "running":
            return {"job_id": job_id, "status": "skipped", "reason": "background_outreach_layering_running"}
        snapshot_dir = self._resolve_workflow_snapshot_dir(
            request=request,
            summary_payloads=[job_summary],
            job_id=job_id,
        )
        if not isinstance(snapshot_dir, Path) or not snapshot_dir.exists():
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_dir_missing"}
        snapshot_id = str(
            outreach_layering.get("snapshot_id")
            or dict(job_summary.get("candidate_source") or {}).get("snapshot_id")
            or snapshot_dir.name
        ).strip()
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}

        running_summary = self._merge_workflow_stage_summary_fields(dict(job_summary), job_summary)
        running_outreach_layering = dict(running_summary.get("outreach_layering") or {})
        running_outreach_layering["status"] = "running"
        running_outreach_layering["snapshot_id"] = snapshot_id
        running_outreach_layering["started_at"] = _utc_now_iso()
        running_summary["outreach_layering"] = running_outreach_layering
        self.store.save_job(
            job_id=job_id,
            job_type=str(job.get("job_type") or "workflow"),
            status=str(job.get("status") or "completed"),
            stage=str(job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=running_summary,
            artifact_path=str(job.get("artifact_path") or ""),
        )
        self.store.append_job_event(
            job_id,
            "completed",
            "running",
            "Background outreach layering reconcile started for deferred full-asset finalization.",
            {"snapshot_id": snapshot_id},
        )

        layering_summary = self._run_outreach_layering_after_acquisition(
            job_id=job_id,
            request=request,
            acquisition_state={"snapshot_id": snapshot_id, "snapshot_dir": snapshot_dir},
            allow_ai=False,
            analysis_stage_label=str(job_summary.get("analysis_stage") or "stage_2_final"),
            event_stage="completed",
            allow_background_defer=False,
        )
        latest_job = self.store.get_job(job_id) or job
        updated_summary = self._restore_completed_workflow_stage_summary_contract(
            dict(latest_job.get("summary") or {}),
            job_summary,
        )
        updated_summary["outreach_layering"] = dict(layering_summary)
        background_reconcile = dict(updated_summary.get("background_reconcile") or {})
        background_reconcile["outreach_layering"] = {
            "status": str(layering_summary.get("status") or ""),
            "snapshot_id": snapshot_id,
            "reconciled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "analysis_paths": dict(layering_summary.get("analysis_paths") or {}),
            "candidate_count": int(layering_summary.get("candidate_count") or 0),
            "reason": str(layering_summary.get("reason") or ""),
        }
        updated_summary["background_reconcile"] = background_reconcile
        self._invalidate_asset_population_snapshot_cache(
            target_company=request.target_company,
            snapshot_id=snapshot_id,
        )
        stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(updated_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=updated_summary,
        )
        if stage_summary_path:
            updated_summary["stage_summary_path"] = stage_summary_path
        artifact_path_value = str(latest_job.get("artifact_path") or job.get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "completed"),
            stage=str(latest_job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=updated_summary,
            artifact_path=artifact_path_value,
        )
        artifact_path = Path(artifact_path_value).expanduser()
        if artifact_path.exists():
            try:
                artifact = json.loads(artifact_path.read_text())
            except (OSError, json.JSONDecodeError):
                artifact = {}
            artifact["summary"] = updated_summary
            artifact["background_reconcile"] = background_reconcile
            artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.append_job_event(
            job_id,
            "completed",
            "completed",
            "Background outreach layering reconcile completed.",
            payload=background_reconcile["outreach_layering"],
        )
        return {
            "job_id": job_id,
            "status": "reconciled_outreach_layering",
            "snapshot_id": snapshot_id,
            "outreach_layering": dict(layering_summary),
        }

    def _apply_background_search_seed_workers_to_snapshot(
        self,
        *,
        snapshot_dir: Path,
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self.snapshot_materializer.apply_search_seed_workers_to_snapshot(
            snapshot_dir=snapshot_dir,
            pending_workers=pending_workers,
        )

    def _apply_background_company_roster_workers_to_snapshot(
        self,
        *,
        snapshot_dir: Path,
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self.snapshot_materializer.apply_company_roster_workers_to_snapshot(
            snapshot_dir=snapshot_dir,
            pending_workers=pending_workers,
        )

    def _apply_background_harvest_prefetch_workers_to_snapshot(
        self,
        *,
        snapshot_dir: Path,
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return self.snapshot_materializer.apply_harvest_profile_workers_to_snapshot(
            snapshot_dir=snapshot_dir,
            pending_workers=pending_workers,
        )

    def _queue_background_profile_prefetch_from_available_baselines(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        roster_snapshot: CompanyRosterSnapshot | None,
        search_seed_snapshot: SearchSeedSnapshot | None,
    ) -> dict[str, Any]:
        if not isinstance(snapshot_dir, Path):
            return {"status": "skipped", "reason": "snapshot_dir_missing"}
        effective_roster_snapshot = roster_snapshot
        if not isinstance(effective_roster_snapshot, CompanyRosterSnapshot):
            effective_roster_snapshot = self._restore_roster_snapshot_from_snapshot_dir(
                snapshot_dir=snapshot_dir,
                identity=resolve_snapshot_company_identity(snapshot_dir, fallback_target_company=request.target_company),
            )
        effective_search_seed_snapshot = search_seed_snapshot
        if not isinstance(effective_search_seed_snapshot, SearchSeedSnapshot):
            effective_search_seed_snapshot = _restore_search_seed_snapshot_from_snapshot_dir(
                snapshot_dir=snapshot_dir,
                identity=resolve_snapshot_company_identity(snapshot_dir, fallback_target_company=request.target_company),
            )
        if not isinstance(effective_roster_snapshot, CompanyRosterSnapshot) and not isinstance(
            effective_search_seed_snapshot,
            SearchSeedSnapshot,
        ):
            return {"status": "skipped", "reason": "baseline_snapshot_missing"}
        return self.acquisition_engine._queue_background_profile_prefetch_from_full_roster_baselines(
            roster_snapshot=effective_roster_snapshot,
            search_seed_snapshot=effective_search_seed_snapshot,
            snapshot_dir=snapshot_dir,
            job_id=job_id,
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            runtime_mode="workflow",
            allow_shared_provider_cache=True,
        )

    def _queue_background_profile_prefetch_from_search_seed_snapshot(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        search_seed_snapshot: SearchSeedSnapshot | None,
    ) -> dict[str, Any]:
        if not isinstance(snapshot_dir, Path):
            return {"status": "skipped", "reason": "snapshot_dir_missing"}
        if not isinstance(search_seed_snapshot, SearchSeedSnapshot):
            return {"status": "skipped", "reason": "search_seed_snapshot_missing"}
        return self._queue_background_profile_prefetch_from_available_baselines(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            roster_snapshot=None,
            search_seed_snapshot=search_seed_snapshot,
        )

    def _synchronize_snapshot_candidate_documents(
        self,
        *,
        request: JobRequest,
        snapshot_dir: Path,
        reason: str,
    ) -> dict[str, Any]:
        sync_result = self.snapshot_materializer.synchronize_snapshot_candidate_documents(
            request=request,
            snapshot_dir=snapshot_dir,
            reason=reason,
        )
        if str(sync_result.get("status") or "").strip().lower() in {"completed", "skipped"}:
            self._invalidate_asset_population_snapshot_cache(
                target_company=request.target_company,
                snapshot_id=str(snapshot_dir.name),
            )
        return sync_result

    def _inline_incremental_writer_lock_for_job(self, job_id: str) -> threading.Lock:
        normalized_job_id = str(job_id or "").strip()
        with self._inline_incremental_writer_locks_lock:
            lock = self._inline_incremental_writer_locks.get(normalized_job_id)
            if lock is None:
                lock = threading.Lock()
                self._inline_incremental_writer_locks[normalized_job_id] = lock
            return lock

    def _worker_snapshot_dir_for_inline_incremental(
        self,
        *,
        worker: dict[str, Any],
        worker_kind: str,
    ) -> str:
        metadata = dict(worker.get("metadata") or {})
        snapshot_dir_value = (
            metadata.get("root_snapshot_dir")
            if worker_kind == "company_roster"
            else metadata.get("snapshot_dir")
        )
        return os.path.normpath(str(Path(str(snapshot_dir_value or "")).expanduser()))

    def _collect_inline_incremental_worker_batch(
        self,
        *,
        job_id: str,
        worker_kind: str,
        snapshot_dir: Path,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        target_snapshot_dir = os.path.normpath(str(Path(snapshot_dir).expanduser()))
        completed_workers: list[dict[str, Any]] = []
        remaining_workers: list[dict[str, Any]] = []
        terminal_statuses = {"completed", "failed", "skipped", "cancelled"}
        for worker in self.agent_runtime.list_workers(job_id=job_id):
            metadata = dict(worker.get("metadata") or {})
            recovery_kind = str(metadata.get("recovery_kind") or "").strip()
            if worker_kind == "company_roster" and recovery_kind != "harvest_company_employees":
                continue
            if worker_kind == "harvest_prefetch" and recovery_kind != "harvest_profile_batch":
                continue
            if self._worker_snapshot_dir_for_inline_incremental(worker=worker, worker_kind=worker_kind) != target_snapshot_dir:
                continue
            status = str(worker.get("status") or "").strip().lower()
            if status == "completed":
                if not _worker_has_inline_incremental_ingest_output(worker):
                    completed_workers.append(dict(worker))
                continue
            if status not in terminal_statuses:
                remaining_workers.append(dict(worker))
        completed_workers.sort(key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)))
        remaining_workers.sort(key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)))
        return completed_workers, remaining_workers

    def _collect_completed_harvest_prefetch_workers_for_snapshot(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        exclude_worker_ids: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        normalized_snapshot_dir = os.path.normpath(str(Path(snapshot_dir).expanduser()))
        excluded_ids = {int(item) for item in list(exclude_worker_ids or set()) if int(item or 0) > 0}
        completed_workers: list[dict[str, Any]] = []
        for worker in self.agent_runtime.list_workers(job_id=job_id):
            worker_id = int(worker.get("worker_id") or 0)
            if worker_id > 0 and worker_id in excluded_ids:
                continue
            if not _worker_has_completed_background_harvest_prefetch(worker):
                continue
            if (
                self._worker_snapshot_dir_for_inline_incremental(worker=worker, worker_kind="harvest_prefetch")
                != normalized_snapshot_dir
            ):
                continue
            completed_workers.append(dict(worker))
        completed_workers.sort(key=lambda item: (str(item.get("updated_at") or ""), int(item.get("worker_id") or 0)))
        return completed_workers

    def _record_inline_incremental_ingest_on_worker(
        self,
        *,
        worker_id: int,
        worker_kind: str,
        snapshot_id: str,
        apply_result: dict[str, Any],
        sync_result: dict[str, Any],
        applied_worker_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        if worker_id <= 0:
            return {}
        worker = self.store.get_agent_worker(worker_id=worker_id)
        if worker is None:
            return {}
        resolved_applied_worker_ids = [
            int(item)
            for item in list(applied_worker_ids or apply_result.get("worker_ids") or [])
            if int(item or 0) > 0
        ]
        checkpoint = dict(worker.get("checkpoint") or {})
        output = dict(worker.get("output") or {})
        marker = {
            "worker_kind": worker_kind,
            "snapshot_id": snapshot_id,
            "applied_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "apply_status": str(apply_result.get("status") or ""),
            "sync_status": str(sync_result.get("status") or ""),
            "sync_reason": str(sync_result.get("reason") or ""),
            "candidate_doc_path": str(apply_result.get("candidate_doc_path") or ""),
            "applied_worker_ids": resolved_applied_worker_ids,
            "applied_worker_count": len(resolved_applied_worker_ids),
            "writer_scope": "job",
            "sync_policy": "same_kind_micro_batch_single_writer",
        }
        output["inline_incremental_ingest"] = marker
        self.store.checkpoint_agent_worker(
            worker_id,
            checkpoint_payload=checkpoint,
            output_payload=output,
            status=str(worker.get("status") or "completed"),
        )
        return marker

    def _build_sync_artifact_result(self, sync_result: dict[str, Any]) -> dict[str, Any]:
        artifact_paths = {
            str(key): str(value)
            for key, value in dict(sync_result.get("artifact_paths") or {}).items()
            if str(key).strip() and str(value).strip()
        }
        artifact_dir = str(sync_result.get("artifact_dir") or "").strip()
        summary_path_value = str(artifact_paths.get("artifact_summary") or "").strip()
        if not summary_path_value and artifact_dir:
            summary_path_value = str(Path(artifact_dir).expanduser() / "artifact_summary.json")
        artifact_summary = _read_json_dict(Path(summary_path_value).expanduser()) if summary_path_value else {}
        return {
            "status": str(sync_result.get("status") or ""),
            "artifact_dir": artifact_dir,
            "artifact_paths": artifact_paths,
            "summary": artifact_summary,
            "sync_status": dict(sync_result.get("sync_status") or {}),
        }

    def _build_harvest_prefetch_profile_completion_result(
        self,
        *,
        apply_result: dict[str, Any],
        sync_result: dict[str, Any],
    ) -> dict[str, Any]:
        unmatched_profile_urls = [
            str(item or "").strip()
            for item in list(apply_result.get("unmatched_profile_urls") or [])
            if str(item or "").strip()
        ]
        result_metrics = {
            "requested_url_count": int(apply_result.get("requested_url_count") or 0),
            "fetched_profile_count": int(apply_result.get("fetched_profile_url_count") or 0),
            "resolved_candidate_count": int(apply_result.get("resolved_candidate_count") or 0),
            "manual_review_candidate_count": int(apply_result.get("manual_review_candidate_count") or 0),
            "non_member_candidate_count": int(apply_result.get("non_member_candidate_count") or 0),
            "unmatched_profile_url_count": len(unmatched_profile_urls),
        }
        sync_status = str(sync_result.get("status") or "").strip()
        return {
            **dict(apply_result),
            "status": "completed" if sync_status == "completed" else str(apply_result.get("status") or ""),
            "result": result_metrics,
            "artifact_result": self._build_sync_artifact_result(sync_result),
            "sync_status": sync_status,
            "sync_reason": str(sync_result.get("reason") or ""),
        }

    def _build_inline_background_reconcile_record(
        self,
        *,
        worker_kind: str,
        workers: list[dict[str, Any]],
        snapshot_id: str,
        apply_result: dict[str, Any],
        sync_result: dict[str, Any],
        profile_prefetch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        worker_ids = [int(worker.get("worker_id") or 0) for worker in workers if int(worker.get("worker_id") or 0) > 0]
        status = (
            "completed"
            if str(sync_result.get("status") or "").strip().lower() == "completed"
            else ("inline_applied" if str(apply_result.get("status") or "").strip() == "applied" else str(apply_result.get("status") or ""))
        )
        base_record = {
            "status": status,
            "snapshot_id": snapshot_id,
            "reconciled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "applied_worker_count": len(worker_ids),
            "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in workers),
            "worker_ids": worker_ids,
            "source": "inline_incremental_ingest",
            "sync_result": {
                key: value
                for key, value in sync_result.items()
                if key != "state_updates"
            },
        }
        if worker_kind == "company_roster":
            record = {
                **base_record,
                "entry_count": int(apply_result.get("entry_count") or 0),
                "added_entry_count": int(apply_result.get("added_entry_count") or 0),
                "candidate_count": int(sync_result.get("candidate_count") or apply_result.get("candidate_count") or 0),
                "evidence_count": int(sync_result.get("evidence_count") or apply_result.get("evidence_count") or 0),
                "completion_status": str(apply_result.get("completion_status") or ""),
                "available_shard_count": int(apply_result.get("available_shard_count") or 0),
                "expected_shard_count": int(apply_result.get("expected_shard_count") or 0),
            }
            if profile_prefetch:
                record["profile_prefetch"] = dict(profile_prefetch)
            return record
        if worker_kind == "search_seed":
            record = {
                **base_record,
                "entry_count": int(apply_result.get("entry_count") or 0),
                "added_entry_count": int(apply_result.get("added_entry_count") or 0),
            }
            if profile_prefetch:
                record["profile_prefetch"] = dict(profile_prefetch)
            return record
        profile_completion_result = self._build_harvest_prefetch_profile_completion_result(
            apply_result=apply_result,
            sync_result=sync_result,
        )
        return {
            **base_record,
            "resume_result": {
                "status": "completed" if status == "completed" else status,
                "sync_result": {
                    key: value
                    for key, value in sync_result.items()
                    if key != "state_updates"
                },
                "profile_completion_result": profile_completion_result,
            },
        }

    def _remaining_inline_background_workers(
        self,
        *,
        job_id: str,
        worker_kind: str,
        exclude_worker_ids: set[int] | None = None,
        snapshot_dir: Path | None = None,
    ) -> list[dict[str, Any]]:
        exclude_ids = {int(item) for item in list(exclude_worker_ids or set()) if int(item or 0) > 0}
        normalized_snapshot_dir = (
            os.path.normpath(str(Path(snapshot_dir).expanduser()))
            if isinstance(snapshot_dir, Path)
            else ""
        )
        remaining: list[dict[str, Any]] = []
        for worker in self.agent_runtime.list_workers(job_id=job_id):
            worker_id = int(worker.get("worker_id") or 0)
            if worker_id > 0 and worker_id in exclude_ids:
                continue
            metadata = dict(worker.get("metadata") or {})
            recovery_kind = str(metadata.get("recovery_kind") or "").strip()
            status = str(worker.get("status") or "").strip().lower()
            output = dict(worker.get("output") or {})
            inline_ingest = dict(output.get("inline_incremental_ingest") or {})
            consumed_inline = bool(str(inline_ingest.get("applied_at") or "").strip())
            if worker_kind == "company_roster" and recovery_kind != "harvest_company_employees":
                continue
            if worker_kind == "harvest_prefetch" and recovery_kind != "harvest_profile_batch":
                continue
            if (
                normalized_snapshot_dir
                and self._worker_snapshot_dir_for_inline_incremental(worker=worker, worker_kind=worker_kind)
                != normalized_snapshot_dir
            ):
                continue
            if status == "completed" and not consumed_inline:
                remaining.append(dict(worker))
                continue
            if status not in {"completed", "failed", "skipped", "cancelled"}:
                remaining.append(dict(worker))
        return remaining

    def _persist_running_job_inline_reconcile_state(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        worker_kind: str,
        workers: list[dict[str, Any]],
        snapshot_dir: Path,
        apply_result: dict[str, Any],
        sync_result: dict[str, Any],
        background_reconcile_record: dict[str, Any] | None = None,
    ) -> None:
        summary = dict(job.get("summary") or {})
        progress = _normalize_acquisition_progress_payload(summary.get("acquisition_progress"))
        latest_state = _deserialize_acquisition_state_payload(dict(progress.get("latest_state") or {}))
        latest_state["snapshot_id"] = str(apply_result.get("snapshot_id") or snapshot_dir.name)
        latest_state["snapshot_dir"] = snapshot_dir
        candidate_doc_path_value = str(
            sync_result.get("state_updates", {}).get("candidate_doc_path")
            or apply_result.get("candidate_doc_path")
            or ""
        ).strip()
        if candidate_doc_path_value:
            latest_state["candidate_doc_path"] = Path(candidate_doc_path_value).expanduser()
        if worker_kind == "company_roster" and isinstance(apply_result.get("roster_snapshot"), CompanyRosterSnapshot):
            latest_state["roster_snapshot"] = apply_result.get("roster_snapshot")
        cursor_state = dict(latest_state.get("background_reconcile_cursor") or {})
        cursor_state[worker_kind] = self._build_background_reconcile_cursor(workers)
        latest_state["background_reconcile_cursor"] = cursor_state
        progress["latest_state"] = _serialize_acquisition_state_payload(latest_state)
        summary["acquisition_progress"] = progress
        if background_reconcile_record:
            background_reconcile = dict(summary.get("background_reconcile") or {})
            background_reconcile[worker_kind] = dict(background_reconcile_record)
            summary["background_reconcile"] = background_reconcile
        self.store.save_job(
            job_id=str(job.get("job_id") or ""),
            job_type=str(job.get("job_type") or "workflow"),
            status=str(job.get("status") or "running"),
            stage=str(job.get("stage") or "acquiring"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=summary,
            artifact_path=str(job.get("artifact_path") or ""),
        )

    def _inline_incremental_sync_for_running_job(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        worker_kind: str,
        snapshot_dir: Path,
        applied_worker_ids: list[int],
        sync_reason: str,
        remaining_workers: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        resolved_remaining_workers = (
            list(remaining_workers)
            if remaining_workers is not None
            else self._remaining_inline_background_workers(
                job_id=str(job.get("job_id") or ""),
                worker_kind=worker_kind,
                exclude_worker_ids=set(applied_worker_ids),
                snapshot_dir=snapshot_dir,
            )
        )
        if resolved_remaining_workers:
            return {
                "status": "deferred",
                "reason": "same_kind_background_workers_still_inflight",
                "remaining_worker_count": len(resolved_remaining_workers),
                "remaining_worker_ids": [
                    int(worker.get("worker_id") or 0) for worker in resolved_remaining_workers
                ],
                "writer_scope": "job",
                "sync_policy": "same_kind_micro_batch_single_writer",
                "materialization_streaming": build_materialization_streaming_budget_report(
                    {},
                    provider_response_count=len(applied_worker_ids),
                    profile_url_count=0,
                    pending_delta_count=len(applied_worker_ids) + len(resolved_remaining_workers),
                    active_writer_count=0,
                    queued_writer_count=len(resolved_remaining_workers),
                ),
            }
        writer_budget = resolved_materialization_global_writer_budget({})
        with runtime_inflight_slot(
            "materialization_writer",
            budget=writer_budget,
            metadata={
                "job_id": str(job.get("job_id") or ""),
                "worker_kind": worker_kind,
                "source": "inline_incremental_sync",
            },
        ) as writer_slot:
            sync_result = self._synchronize_snapshot_candidate_documents(
                request=request,
                snapshot_dir=snapshot_dir,
                reason=sync_reason,
            )
        sync_result["writer_slot"] = dict(writer_slot)
        sync_result["materialization_streaming"] = build_materialization_streaming_budget_report(
            {},
            provider_response_count=len(applied_worker_ids),
            profile_url_count=0,
            pending_delta_count=len(applied_worker_ids),
            active_writer_count=0,
            queued_writer_count=0,
            oldest_pending_delta_age_ms=float(writer_slot.get("wait_ms") or 0.0),
        )
        sync_result.setdefault("writer_scope", "job")
        sync_result.setdefault("sync_policy", "same_kind_micro_batch_single_writer")
        return sync_result

    def _process_inline_incremental_worker_batch(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        snapshot_dir: Path,
        worker_kind: str,
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            return {"status": "skipped", "reason": "job_id_missing"}
        with self._inline_incremental_writer_lock_for_job(job_id):
            completed_workers, remaining_workers = self._collect_inline_incremental_worker_batch(
                job_id=job_id,
                worker_kind=worker_kind,
                snapshot_dir=snapshot_dir,
            )
            if not completed_workers:
                return {"status": "skipped", "reason": "no_completed_unconsumed_inline_workers"}

            if str(job.get("status") or "").strip().lower() == "completed":
                job_summary = dict(job.get("summary") or {})
                if worker_kind == "company_roster":
                    reconcile_result = self._reconcile_completed_workflow_after_company_roster(
                        job=job,
                        request=request,
                        plan_payload=plan_payload,
                        job_summary=job_summary,
                        pending_workers=completed_workers,
                    )
                else:
                    reconcile_result = self._reconcile_completed_workflow_after_harvest_prefetch(
                        job=job,
                        request=request,
                        plan_payload=plan_payload,
                        job_summary=job_summary,
                        pending_workers=completed_workers,
                    )
                reconcile_status = str(reconcile_result.get("status") or "").strip()
                if reconcile_status.startswith("reconciled_") or reconcile_status == "completed":
                    marker_sync_result = {
                        "status": (
                            "completed"
                            if reconcile_status.startswith("reconciled_")
                            else reconcile_status
                        ),
                        "reason": "completed_workflow_inline_reconcile",
                        "writer_scope": "job",
                        "sync_policy": "same_kind_micro_batch_single_writer",
                    }
                    applied_worker_ids = [int(worker.get("worker_id") or 0) for worker in completed_workers]
                    marker_apply_result = {
                        "status": "applied",
                        "snapshot_id": str(reconcile_result.get("snapshot_id") or snapshot_dir.name),
                        "worker_ids": applied_worker_ids,
                    }
                    for worker in completed_workers:
                        self._record_inline_incremental_ingest_on_worker(
                            worker_id=int(worker.get("worker_id") or 0),
                            worker_kind=worker_kind,
                            snapshot_id=str(reconcile_result.get("snapshot_id") or snapshot_dir.name),
                            apply_result=marker_apply_result,
                            sync_result=marker_sync_result,
                            applied_worker_ids=list(applied_worker_ids),
                        )
                return {
                    **dict(reconcile_result),
                    "batched_worker_count": len(completed_workers),
                    "batched_worker_ids": [int(worker.get("worker_id") or 0) for worker in completed_workers],
                }

            profile_prefetch: dict[str, Any] = {}
            if worker_kind == "company_roster":
                apply_result = self._apply_background_company_roster_workers_to_snapshot(
                    snapshot_dir=snapshot_dir,
                    pending_workers=completed_workers,
                )
                if str(apply_result.get("status") or "") != "applied":
                    return {
                        "status": "skipped",
                        "reason": str(apply_result.get("reason") or "company_roster_apply_skipped"),
                    }
                profile_prefetch = self._queue_background_profile_prefetch_from_available_baselines(
                    job_id=job_id,
                    request=request,
                    plan_payload=plan_payload,
                    snapshot_dir=snapshot_dir,
                    roster_snapshot=apply_result.get("roster_snapshot"),
                    search_seed_snapshot=_restore_search_seed_snapshot_from_snapshot_dir(
                        snapshot_dir=snapshot_dir,
                        identity=resolve_snapshot_company_identity(
                            snapshot_dir,
                            fallback_target_company=request.target_company,
                        ),
                    ),
                )
                sync_reason = "inline_background_company_roster_reconcile"
            else:
                apply_result = self._apply_background_harvest_prefetch_workers_to_snapshot(
                    snapshot_dir=snapshot_dir,
                    pending_workers=completed_workers,
                )
                if str(apply_result.get("status") or "") != "applied":
                    return {
                        "status": "skipped",
                        "reason": str(apply_result.get("reason") or "harvest_prefetch_apply_skipped"),
                    }
                sync_reason = "inline_background_harvest_prefetch_reconcile"

            applied_worker_ids = [
                int(item)
                for item in list(apply_result.get("worker_ids") or [])
                if int(item or 0) > 0
            ] or [int(worker.get("worker_id") or 0) for worker in completed_workers if int(worker.get("worker_id") or 0) > 0]
            snapshot_id = str(apply_result.get("snapshot_id") or snapshot_dir.name)
            self._invalidate_asset_population_snapshot_cache(
                target_company=request.target_company,
                snapshot_id=snapshot_id,
            )
            sync_result = self._inline_incremental_sync_for_running_job(
                job=job,
                request=request,
                worker_kind=worker_kind,
                snapshot_dir=snapshot_dir,
                applied_worker_ids=applied_worker_ids,
                sync_reason=sync_reason,
                remaining_workers=remaining_workers,
            )
            background_reconcile_record = self._build_inline_background_reconcile_record(
                worker_kind=worker_kind,
                workers=completed_workers,
                snapshot_id=snapshot_id,
                apply_result=apply_result,
                sync_result=sync_result,
                profile_prefetch=profile_prefetch,
            )
            if worker_kind == "harvest_prefetch":
                sync_status = str(sync_result.get("status") or "").strip().lower()
                self.store.append_job_event(
                    job_id,
                    stage=str(job.get("stage") or "acquiring"),
                    status="running",
                    detail="Background harvest profile prefetch reconcile started after worker recovery.",
                    payload={
                        "snapshot_id": snapshot_id,
                        "pending_worker_count": len(applied_worker_ids),
                        "worker_ids": applied_worker_ids,
                    },
                )
                if sync_status == "completed":
                    self.store.append_job_event(
                        job_id,
                        stage=str(job.get("stage") or "acquiring"),
                        status="running",
                        detail="Background harvest profile prefetch rebuild refreshed retrieval artifacts.",
                        payload={
                            "snapshot_id": snapshot_id,
                            "sync_result": {
                                key: value
                                for key, value in sync_result.items()
                                if key != "state_updates"
                            },
                        },
                    )
                    self.store.append_job_event(
                        job_id,
                        stage=str(job.get("stage") or "acquiring"),
                        status="running",
                        detail="Background harvest profile prefetch merged local profile detail into candidate artifacts.",
                        payload={
                            "snapshot_id": snapshot_id,
                            "profile_completion": dict(
                                dict(background_reconcile_record.get("resume_result") or {}).get("profile_completion_result")
                                or {}
                            ),
                        },
                    )
                else:
                    self.store.append_job_event(
                        job_id,
                        stage=str(job.get("stage") or "acquiring"),
                        status="running",
                        detail="Background harvest profile prefetch delta was applied; full candidate artifact rebuild is deferred until remaining same-kind workers drain.",
                        payload={
                            "snapshot_id": snapshot_id,
                            "sync_result": {
                                key: value
                                for key, value in sync_result.items()
                                if key != "state_updates"
                            },
                        },
                    )
            for worker in completed_workers:
                self._record_inline_incremental_ingest_on_worker(
                    worker_id=int(worker.get("worker_id") or 0),
                    worker_kind=worker_kind,
                    snapshot_id=snapshot_id,
                    apply_result=apply_result,
                    sync_result=sync_result,
                    applied_worker_ids=applied_worker_ids,
                )
            self._persist_running_job_inline_reconcile_state(
                job=job,
                request=request,
                plan_payload=plan_payload,
                worker_kind=worker_kind,
                workers=completed_workers,
                snapshot_dir=snapshot_dir,
                apply_result=apply_result,
                sync_result=sync_result,
                background_reconcile_record=background_reconcile_record,
            )
            if worker_kind == "company_roster":
                event_payload: dict[str, Any] = {
                    "snapshot_id": snapshot_id,
                    "worker_ids": applied_worker_ids,
                    "applied_worker_count": len(applied_worker_ids),
                    "apply_result": {
                        key: value
                        for key, value in apply_result.items()
                        if key not in {"roster_snapshot"}
                    },
                    "sync_result": {
                        key: value
                        for key, value in sync_result.items()
                        if key != "state_updates"
                    },
                    "writer_scope": "job",
                    "sync_policy": "same_kind_micro_batch_single_writer",
                }
                if profile_prefetch:
                    event_payload["profile_prefetch"] = dict(profile_prefetch)
                self.store.append_job_event(
                    job_id,
                    stage=str(job.get("stage") or "acquiring"),
                    status="running",
                    detail="Background company-roster workers were incrementally applied to the live snapshot.",
                    payload=event_payload,
                )
            return {
                "status": "processed",
                "worker_kind": worker_kind,
                "snapshot_id": snapshot_id,
                "batched_worker_count": len(applied_worker_ids),
                "batched_worker_ids": applied_worker_ids,
                "apply_result": apply_result,
                "sync_result": sync_result,
                "profile_prefetch": profile_prefetch,
            }

    def _handle_completed_recovery_worker_result(self, result: dict[str, Any]) -> None:
        if str(result.get("worker_status") or "").strip().lower() != "completed":
            return
        worker_id = int(result.get("worker_id") or 0)
        if worker_id <= 0:
            return
        worker = self.store.get_agent_worker(worker_id=worker_id)
        if worker is None:
            return
        metadata = dict(worker.get("metadata") or {})
        recovery_kind = str(metadata.get("recovery_kind") or "").strip()
        if recovery_kind not in {"harvest_company_employees", "harvest_profile_batch"}:
            return
        job_id = str(worker.get("job_id") or "").strip()
        if not job_id:
            return
        job = self.store.get_job(job_id)
        if job is None:
            return
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        plan_payload = dict(job.get("plan") or {})
        snapshot_dir_value = str(metadata.get("root_snapshot_dir") or metadata.get("snapshot_dir") or "").strip()
        if not snapshot_dir_value:
            return
        snapshot_dir = Path(snapshot_dir_value).expanduser()
        self._process_inline_incremental_worker_batch(
            job=job,
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            worker_kind=("company_roster" if recovery_kind == "harvest_company_employees" else "harvest_prefetch"),
        )

    def _reconcile_completed_workflow_after_company_roster(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        job_summary: dict[str, Any],
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id") or "")
        job_result_view = self.store.get_job_result_view(job_id=job_id)
        snapshot_id = _resolve_reconcile_snapshot_id(
            job_summary,
            pending_workers,
            job_result_view=job_result_view,
        )
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}
        snapshot_dir = _resolve_reconcile_snapshot_dir(
            self.runtime_dir,
            request.target_company,
            job_summary,
            pending_workers,
            job_result_view=job_result_view,
        )
        if snapshot_dir is None:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_dir_missing"}

        self.store.append_job_event(
            job_id,
            "completed",
            "running",
            "Background company-roster reconcile started after worker recovery.",
            payload={
                "snapshot_id": snapshot_id,
                "pending_worker_count": len(pending_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
            },
        )
        company_roster_update = self._apply_background_company_roster_workers_to_snapshot(
            snapshot_dir=snapshot_dir,
            pending_workers=pending_workers,
        )
        if str(company_roster_update.get("status") or "") != "applied":
            return {
                "job_id": job_id,
                "status": "skipped",
                "reason": str(company_roster_update.get("reason") or "company_roster_apply_skipped"),
            }
        profile_prefetch = self._queue_background_profile_prefetch_from_available_baselines(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            roster_snapshot=company_roster_update.get("roster_snapshot"),
            search_seed_snapshot=_restore_search_seed_snapshot_from_snapshot_dir(
                snapshot_dir=snapshot_dir,
                identity=resolve_snapshot_company_identity(snapshot_dir, fallback_target_company=request.target_company),
            ),
        )
        immediate_harvest_prefetch_workers: list[dict[str, Any]] = []
        harvest_prefetch_update: dict[str, Any] = {}
        if str(profile_prefetch.get("status") or "").strip().lower() == "completed":
            immediate_harvest_prefetch_workers = self._collect_completed_harvest_prefetch_workers_for_snapshot(
                job_id=job_id,
                snapshot_dir=snapshot_dir,
            )
            if immediate_harvest_prefetch_workers:
                self.store.append_job_event(
                    job_id,
                    "completed",
                    "running",
                    "Background harvest profile prefetch reconcile started after worker recovery.",
                    payload={
                        "snapshot_id": snapshot_id,
                        "pending_worker_count": len(immediate_harvest_prefetch_workers),
                        "worker_ids": [
                            int(worker.get("worker_id") or 0) for worker in immediate_harvest_prefetch_workers
                        ],
                        "source": "company_roster_reconcile_immediate_prefetch",
                    },
                )
                harvest_prefetch_update = self._apply_background_harvest_prefetch_workers_to_snapshot(
                    snapshot_dir=snapshot_dir,
                    pending_workers=immediate_harvest_prefetch_workers,
                )
                if str(harvest_prefetch_update.get("status") or "") != "applied":
                    immediate_harvest_prefetch_workers = []
        sync_result = self._synchronize_snapshot_candidate_documents(
            request=request,
            snapshot_dir=snapshot_dir,
            reason="background_company_roster_reconcile",
        )
        if str(sync_result.get("status") or "") not in {"completed", "skipped"}:
            return {
                "job_id": job_id,
                "status": "failed",
                "reason": str(sync_result.get("reason") or "snapshot_sync_failed"),
            }
        layering_summary: dict[str, Any] = {}
        if str(sync_result.get("status") or "") == "completed":
            layering_summary = self._run_outreach_layering_after_acquisition(
                job_id=job_id,
                request=request,
                acquisition_state={"snapshot_id": snapshot_id, "snapshot_dir": snapshot_dir},
            )
        retrieval_artifact = self._execute_retrieval(
            job_id,
            request,
            hydrate_sourcing_plan(plan_payload),
            job_type="workflow",
            runtime_policy={
                "mode": "background_reconcile",
                "workflow_snapshot_id": snapshot_id,
                "outreach_layering": layering_summary,
            },
        )

        latest_job = self.store.get_job(job_id) or job
        updated_summary = self._restore_completed_workflow_stage_summary_contract(
            dict(latest_job.get("summary") or {}),
            job_summary,
        )
        background_reconcile = dict(updated_summary.get("background_reconcile") or {})
        company_roster_record = {
            "status": "completed",
            "snapshot_id": snapshot_id,
            "reconciled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "applied_worker_count": len(pending_workers),
            "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in pending_workers),
            "worker_ids": list(company_roster_update.get("worker_ids") or []),
            "entry_count": int(company_roster_update.get("entry_count") or 0),
            "added_entry_count": int(company_roster_update.get("added_entry_count") or 0),
            "candidate_count": int(sync_result.get("candidate_count") or company_roster_update.get("candidate_count") or 0),
            "evidence_count": int(sync_result.get("evidence_count") or company_roster_update.get("evidence_count") or 0),
            "completion_status": str(company_roster_update.get("completion_status") or ""),
            "available_shard_count": int(company_roster_update.get("available_shard_count") or 0),
            "expected_shard_count": int(company_roster_update.get("expected_shard_count") or 0),
            "artifact_dir": str(sync_result.get("artifact_dir") or ""),
            "artifact_paths": dict(sync_result.get("artifact_paths") or {}),
            "profile_prefetch": dict(profile_prefetch),
        }
        if layering_summary:
            company_roster_record["outreach_layering"] = layering_summary
        background_reconcile["company_roster"] = company_roster_record
        if immediate_harvest_prefetch_workers:
            self.store.append_job_event(
                job_id,
                "completed",
                "running",
                "Background harvest profile prefetch rebuild refreshed retrieval artifacts.",
                payload={
                    "snapshot_id": snapshot_id,
                    "sync_result": {key: value for key, value in sync_result.items() if key != "state_updates"},
                },
            )
            harvest_prefetch_record = self._build_inline_background_reconcile_record(
                worker_kind="harvest_prefetch",
                workers=immediate_harvest_prefetch_workers,
                snapshot_id=snapshot_id,
                apply_result=harvest_prefetch_update,
                sync_result=sync_result,
            )
            self.store.append_job_event(
                job_id,
                "completed",
                "running",
                "Background harvest profile prefetch merged local profile detail into candidate artifacts.",
                payload={
                    "snapshot_id": snapshot_id,
                    "profile_completion": dict(
                        dict(harvest_prefetch_record.get("resume_result") or {}).get("profile_completion_result")
                        or {}
                    ),
                },
            )
            applied_harvest_worker_ids = [
                int(worker.get("worker_id") or 0)
                for worker in immediate_harvest_prefetch_workers
                if int(worker.get("worker_id") or 0) > 0
            ]
            for worker in immediate_harvest_prefetch_workers:
                self._record_inline_incremental_ingest_on_worker(
                    worker_id=int(worker.get("worker_id") or 0),
                    worker_kind="harvest_prefetch",
                    snapshot_id=snapshot_id,
                    apply_result=harvest_prefetch_update,
                    sync_result=sync_result,
                    applied_worker_ids=applied_harvest_worker_ids,
                )
            background_reconcile["harvest_prefetch"] = harvest_prefetch_record
        updated_summary["background_reconcile"] = background_reconcile
        if layering_summary:
            updated_summary["outreach_layering"] = layering_summary
        stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(updated_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=updated_summary,
        )
        if stage_summary_path:
            updated_summary["stage_summary_path"] = stage_summary_path
        artifact_path_value = str(retrieval_artifact.get("artifact_path") or latest_job.get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "completed"),
            stage=str(latest_job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=updated_summary,
            artifact_path=artifact_path_value,
        )
        artifact_path = Path(artifact_path_value).expanduser()
        if artifact_path.exists():
            try:
                artifact = json.loads(artifact_path.read_text())
            except (OSError, json.JSONDecodeError):
                artifact = {}
            artifact["summary"] = updated_summary
            artifact["background_reconcile"] = background_reconcile
            artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.append_job_event(
            job_id,
            "completed",
            "completed",
            "Background reconcile refreshed results after company-roster worker recovery.",
            payload=company_roster_record,
        )
        return {
            "job_id": job_id,
            "status": "reconciled_company_roster",
            "snapshot_id": snapshot_id,
            "applied_worker_count": len(pending_workers),
            "worker_ids": list(company_roster_update.get("worker_ids") or []),
            "entry_count": int(company_roster_update.get("entry_count") or 0),
            "added_entry_count": int(company_roster_update.get("added_entry_count") or 0),
        }

    def _reconcile_completed_workflow_after_harvest_prefetch(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        job_summary: dict[str, Any],
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id") or "")
        job_result_view = self.store.get_job_result_view(job_id=job_id)
        snapshot_id = _resolve_reconcile_snapshot_id(
            job_summary,
            pending_workers,
            job_result_view=job_result_view,
        )
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}

        self.store.append_job_event(
            job_id,
            "completed",
            "running",
            "Background harvest profile prefetch reconcile started after worker recovery.",
            payload={
                "snapshot_id": snapshot_id,
                "pending_worker_count": len(pending_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
            },
        )
        try:
            plan = hydrate_sourcing_plan(plan_payload)
            snapshot_dir = _resolve_reconcile_snapshot_dir(
                self.runtime_dir,
                request.target_company,
                job_summary,
                pending_workers,
                job_result_view=job_result_view,
            )
            if snapshot_dir is None:
                return {"job_id": job_id, "status": "skipped", "reason": "snapshot_dir_missing"}
            harvest_prefetch_update = self._apply_background_harvest_prefetch_workers_to_snapshot(
                snapshot_dir=snapshot_dir,
                pending_workers=pending_workers,
            )
            sync_result = self._synchronize_snapshot_candidate_documents(
                request=request,
                snapshot_dir=snapshot_dir,
                reason="background_harvest_prefetch_reconcile",
            )
            if str(sync_result.get("status") or "") not in {"completed", "skipped"}:
                raise RuntimeError(
                    str(sync_result.get("detail") or sync_result.get("reason") or "snapshot_sync_failed")
                )
            self.store.append_job_event(
                job_id,
                "completed",
                "running",
                "Background harvest profile prefetch rebuild refreshed retrieval artifacts.",
                payload={
                    "snapshot_id": snapshot_id,
                    "sync_result": {key: value for key, value in sync_result.items() if key != "state_updates"},
                },
            )
            profile_completion_result = self._build_harvest_prefetch_profile_completion_result(
                apply_result=harvest_prefetch_update,
                sync_result=sync_result,
            )
            self.store.append_job_event(
                job_id,
                "completed",
                "running",
                "Background harvest profile prefetch merged local profile detail into candidate artifacts.",
                payload={
                    "snapshot_id": snapshot_id,
                    "profile_completion": profile_completion_result,
                },
            )
            layering_summary: dict[str, Any] = {}
            if str(sync_result.get("status") or "") == "completed":
                layering_summary = self._run_outreach_layering_after_acquisition(
                    job_id=job_id,
                    request=request,
                    acquisition_state={"snapshot_id": snapshot_id, "snapshot_dir": snapshot_dir},
                )
            retrieval_artifact = self._execute_retrieval(
                job_id,
                request,
                plan,
                job_type="workflow",
                runtime_policy={
                    "workflow_snapshot_id": snapshot_id,
                    "outreach_layering": layering_summary,
                },
            )
            resume_result = {
                "status": "completed",
                "artifact": retrieval_artifact,
                "sync_result": sync_result,
                "profile_completion_result": profile_completion_result,
            }
            if layering_summary:
                resume_result["outreach_layering"] = layering_summary
        except Exception as exc:
            self.store.append_job_event(
                job_id,
                "completed",
                "failed",
                "Background harvest profile prefetch reconcile failed.",
                payload={"error": str(exc)},
            )
            return {
                "job_id": job_id,
                "status": "failed",
                "reason": "background_harvest_prefetch_reconcile_failed",
                "error": str(exc),
            }

        latest_job = self.store.get_job(job_id) or job
        updated_summary = self._restore_completed_workflow_stage_summary_contract(
            dict(latest_job.get("summary") or {}),
            job_summary,
        )
        background_reconcile = dict(updated_summary.get("background_reconcile") or {})
        harvest_prefetch_record = {
            "status": "completed",
            "snapshot_id": snapshot_id,
            "reconciled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "applied_worker_count": len(pending_workers),
            "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in pending_workers),
            "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
            "resume_result": resume_result,
        }
        outreach_layering_result = resume_result.get("outreach_layering")
        if isinstance(outreach_layering_result, dict) and outreach_layering_result:
            updated_summary["outreach_layering"] = dict(outreach_layering_result)
        background_reconcile["harvest_prefetch"] = harvest_prefetch_record
        updated_summary["background_reconcile"] = background_reconcile
        stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(updated_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=updated_summary,
        )
        if stage_summary_path:
            updated_summary["stage_summary_path"] = stage_summary_path
        artifact_path_value = str(latest_job.get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "completed"),
            stage=str(latest_job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=updated_summary,
            artifact_path=artifact_path_value,
        )
        artifact_path = Path(artifact_path_value).expanduser()
        if artifact_path.exists():
            try:
                artifact = json.loads(artifact_path.read_text())
            except (OSError, json.JSONDecodeError):
                artifact = {}
            artifact["summary"] = updated_summary
            artifact["background_reconcile"] = background_reconcile
            artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.append_job_event(
            job_id,
            "completed",
            "completed",
            "Background reconcile refreshed results after harvest profile prefetch recovery.",
            payload=harvest_prefetch_record,
        )
        return {
            "job_id": job_id,
            "status": "reconciled_harvest_prefetch",
            "snapshot_id": snapshot_id,
            "applied_worker_count": len(pending_workers),
            "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
        }

    def _reconcile_completed_workflow_after_search_seed(
        self,
        *,
        job: dict[str, Any],
        request: JobRequest,
        plan_payload: dict[str, Any],
        job_summary: dict[str, Any],
        pending_workers: list[dict[str, Any]],
    ) -> dict[str, Any]:
        job_id = str(job.get("job_id") or "")
        job_result_view = self.store.get_job_result_view(job_id=job_id)
        snapshot_id = _resolve_reconcile_snapshot_id(
            job_summary,
            pending_workers,
            job_result_view=job_result_view,
        )
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}
        snapshot_dir = _resolve_reconcile_snapshot_dir(
            self.runtime_dir,
            request.target_company,
            job_summary,
            pending_workers,
            job_result_view=job_result_view,
        )
        if snapshot_dir is None:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_dir_missing"}

        self.store.append_job_event(
            job_id,
            "completed",
            "running",
            "Background search-seed reconcile started after worker recovery.",
            payload={
                "snapshot_id": snapshot_id,
                "pending_worker_count": len(pending_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
            },
        )
        search_seed_update = self._apply_background_search_seed_workers_to_snapshot(
            snapshot_dir=snapshot_dir,
            pending_workers=pending_workers,
        )
        if str(search_seed_update.get("status") or "") != "applied":
            return {
                "job_id": job_id,
                "status": "skipped",
                "reason": str(search_seed_update.get("reason") or "search_seed_apply_skipped"),
            }
        search_seed_profile_prefetch: dict[str, Any] = {}
        search_seed_snapshot = search_seed_update.get("search_seed_snapshot")
        if isinstance(search_seed_snapshot, SearchSeedSnapshot):
            search_seed_profile_prefetch = self._queue_background_profile_prefetch_from_search_seed_snapshot(
                job_id=job_id,
                request=request,
                plan_payload=plan_payload,
                snapshot_dir=snapshot_dir,
                search_seed_snapshot=search_seed_snapshot,
            )
        sync_result = self._synchronize_snapshot_candidate_documents(
            request=request,
            snapshot_dir=snapshot_dir,
            reason="background_search_seed_reconcile",
        )
        if str(sync_result.get("status") or "") not in {"completed", "skipped"}:
            return {
                "job_id": job_id,
                "status": "failed",
                "reason": str(sync_result.get("reason") or "snapshot_sync_failed"),
            }
        layering_summary: dict[str, Any] = {}
        if str(sync_result.get("status") or "") == "completed":
            layering_summary = self._run_outreach_layering_after_acquisition(
                job_id=job_id,
                request=request,
                acquisition_state={"snapshot_id": snapshot_id, "snapshot_dir": snapshot_dir},
            )
        retrieval_artifact = self._execute_retrieval(
            job_id,
            request,
            hydrate_sourcing_plan(plan_payload),
            job_type="workflow",
            runtime_policy={
                "mode": "background_reconcile",
                "workflow_snapshot_id": snapshot_id,
                "outreach_layering": layering_summary,
            },
        )

        latest_job = self.store.get_job(job_id) or job
        updated_summary = self._restore_completed_workflow_stage_summary_contract(
            dict(latest_job.get("summary") or {}),
            job_summary,
        )
        background_reconcile = dict(updated_summary.get("background_reconcile") or {})
        search_seed_record = {
            "status": "completed",
            "snapshot_id": snapshot_id,
            "reconciled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "applied_worker_count": len(pending_workers),
            "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in pending_workers),
            "worker_ids": list(search_seed_update.get("worker_ids") or []),
            "entry_count": int(search_seed_update.get("entry_count") or 0),
            "added_entry_count": int(search_seed_update.get("added_entry_count") or 0),
            "queued_query_count": int(search_seed_update.get("queued_query_count") or 0),
            "stop_reason": str(search_seed_update.get("stop_reason") or ""),
            "candidate_count": int(
                sync_result.get("candidate_count") or search_seed_update.get("candidate_count") or 0
            ),
            "evidence_count": int(sync_result.get("evidence_count") or search_seed_update.get("evidence_count") or 0),
            "artifact_dir": str(sync_result.get("artifact_dir") or ""),
            "artifact_paths": dict(sync_result.get("artifact_paths") or {}),
            "profile_prefetch": dict(search_seed_profile_prefetch),
        }
        if layering_summary:
            search_seed_record["outreach_layering"] = layering_summary
        background_reconcile["search_seed"] = search_seed_record
        updated_summary["background_reconcile"] = background_reconcile
        if layering_summary:
            updated_summary["outreach_layering"] = layering_summary
        stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(updated_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=updated_summary,
        )
        if stage_summary_path:
            updated_summary["stage_summary_path"] = stage_summary_path
        artifact_path_value = str(retrieval_artifact.get("artifact_path") or latest_job.get("artifact_path") or "")
        self.store.save_job(
            job_id=job_id,
            job_type=str(latest_job.get("job_type") or "workflow"),
            status=str(latest_job.get("status") or "completed"),
            stage=str(latest_job.get("stage") or "completed"),
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload=updated_summary,
            artifact_path=artifact_path_value,
        )
        artifact_path = Path(artifact_path_value).expanduser()
        if artifact_path.exists():
            try:
                artifact = json.loads(artifact_path.read_text())
            except (OSError, json.JSONDecodeError):
                artifact = {}
            artifact["summary"] = updated_summary
            artifact["background_reconcile"] = background_reconcile
            artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.append_job_event(
            job_id,
            "completed",
            "completed",
            "Background reconcile refreshed results after search-seed worker recovery.",
            payload=search_seed_record,
        )
        return {
            "job_id": job_id,
            "status": "reconciled_search_seed",
            "snapshot_id": snapshot_id,
            "applied_worker_count": len(pending_workers),
            "worker_ids": list(search_seed_update.get("worker_ids") or []),
            "entry_count": int(search_seed_update.get("entry_count") or 0),
            "added_entry_count": int(search_seed_update.get("added_entry_count") or 0),
        }

    def _apply_completed_exploration_worker_output(self, target_company: str, worker: dict[str, Any]) -> dict[str, Any]:
        output = dict(worker.get("output") or {})
        candidate_payload = dict(output.get("candidate") or {})
        evidence_payloads = [dict(item) for item in list(output.get("evidence") or []) if isinstance(item, dict)]
        candidate_upsert_count = 0
        candidate_to_store: Candidate | None = None
        if candidate_payload:
            candidate_record = dict(candidate_payload)
            if not str(candidate_record.get("target_company") or "").strip():
                candidate_record["target_company"] = target_company
            candidate = normalize_candidate(Candidate(**candidate_record))
            existing_candidate = self.store.get_candidate(candidate.candidate_id)
            candidate_to_store = _merge_background_reconcile_candidate(existing_candidate, candidate)
            self.store.upsert_candidate(candidate_to_store)
            candidate_upsert_count = 1
        evidence_records: list[EvidenceRecord] = []
        for item in evidence_payloads:
            try:
                evidence_records.append(EvidenceRecord(**item))
            except TypeError:
                continue
        if evidence_records:
            self.store.upsert_evidence_records(evidence_records)
        snapshot_sync = self._apply_background_reconcile_snapshot_candidate_update(
            target_company=target_company,
            worker=worker,
            candidate=candidate_to_store,
            evidence_records=evidence_records,
        )
        return {
            "candidate_upsert_count": candidate_upsert_count,
            "evidence_upsert_count": len(evidence_records),
            "snapshot_sync": snapshot_sync,
        }

    def _resolve_background_reconcile_snapshot_dir(
        self,
        *,
        target_company: str,
        worker: dict[str, Any],
        candidate: Candidate | None,
        evidence_records: list[EvidenceRecord],
    ) -> Path | None:
        metadata = dict(worker.get("metadata") or {})
        snapshot_dir_value = str(metadata.get("snapshot_dir") or "").strip()
        if snapshot_dir_value:
            return Path(snapshot_dir_value).expanduser()

        source_paths: list[str] = []
        if candidate is not None and str(candidate.source_path or "").strip():
            source_paths.append(str(candidate.source_path or "").strip())
        source_paths.extend(
            str(item.source_path or "").strip() for item in evidence_records if str(item.source_path or "").strip()
        )
        for source_path in source_paths:
            resolved_snapshot_dir = resolve_snapshot_dir_from_source_path(source_path)
            if resolved_snapshot_dir is not None:
                return resolved_snapshot_dir

        snapshot_id = str(metadata.get("snapshot_id") or "").strip()
        resolved_snapshot_dir = resolve_company_snapshot_dir(
            self.runtime_dir,
            target_company=target_company,
            snapshot_id=snapshot_id,
        )
        if resolved_snapshot_dir is not None:
            return resolved_snapshot_dir
        return None

    def _apply_background_reconcile_snapshot_candidate_update(
        self,
        *,
        target_company: str,
        worker: dict[str, Any],
        candidate: Candidate | None,
        evidence_records: list[EvidenceRecord],
    ) -> dict[str, Any]:
        if candidate is None and not evidence_records:
            return {"status": "skipped", "reason": "no_candidate_output"}
        snapshot_dir = self._resolve_background_reconcile_snapshot_dir(
            target_company=target_company,
            worker=worker,
            candidate=candidate,
            evidence_records=evidence_records,
        )
        if snapshot_dir is None:
            return {"status": "skipped", "reason": "snapshot_dir_missing"}

        update_paths = [snapshot_dir / "candidate_documents.json"]
        for stage_name in ("candidate_documents.linkedin_stage_1.json", "candidate_documents.public_web_stage_2.json"):
            stage_path = snapshot_dir / stage_name
            if stage_path.exists():
                update_paths.append(stage_path)

        payload = _read_json_dict(update_paths[0])
        if not payload:
            payload = {
                "snapshot": {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": target_company,
                }
            }
        merged_candidates = {
            item.candidate_id: item for item in _candidate_records_from_payload(payload.get("candidates") or [])
        }
        if candidate is not None:
            merged_candidates[candidate.candidate_id] = _merge_background_reconcile_candidate(
                merged_candidates.get(candidate.candidate_id),
                candidate,
            )
        merged_evidence = {
            item.evidence_id: item for item in _evidence_records_from_payload(payload.get("evidence") or [])
        }
        for item in evidence_records:
            merged_evidence[item.evidence_id] = item

        updated_payload = {
            **payload,
            "candidates": [item.to_record() for item in merged_candidates.values()],
            "evidence": [item.to_record() for item in merged_evidence.values()],
            "candidate_count": len(merged_candidates),
            "evidence_count": len(merged_evidence),
        }
        background_reconcile = dict(updated_payload.get("background_reconcile") or {})
        background_reconcile["exploration_worker"] = {
            "worker_id": int(worker.get("worker_id") or 0),
            "worker_key": str(worker.get("worker_key") or ""),
            "updated_at": str(worker.get("updated_at") or ""),
            "candidate_id": str(candidate.candidate_id if candidate is not None else ""),
            "evidence_count": len(evidence_records),
        }
        updated_payload["background_reconcile"] = background_reconcile

        serialized_payload = json.dumps(_storage_json_safe_payload(updated_payload), ensure_ascii=False, indent=2)
        for candidate_doc_path in update_paths:
            candidate_doc_path.parent.mkdir(parents=True, exist_ok=True)
            candidate_doc_path.write_text(serialized_payload, encoding="utf-8")
        return {
            "status": "updated",
            "snapshot_dir": str(snapshot_dir),
            "candidate_count": len(merged_candidates),
            "evidence_count": len(merged_evidence),
            "updated_paths": [str(path) for path in update_paths],
        }

    def _resume_blocked_workflow_if_ready(
        self,
        job_id: str,
        *,
        assume_lock: bool = False,
        allow_stale_lease_takeover: bool = True,
    ) -> dict[str, Any]:
        if not assume_lock:
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
                    takeover = (
                        self._release_stale_workflow_job_lease_for_recovery(job_id)
                        if allow_stale_lease_takeover
                        else {"released": False}
                    )
                    if bool(takeover.get("released")):
                        return self._resume_blocked_workflow_if_ready(
                            job_id,
                            assume_lock=False,
                            allow_stale_lease_takeover=False,
                        )
                    latest_job = self.store.get_job(job_id) or {}
                    return {
                        "job_id": job_id,
                        "status": "skipped",
                        "stage": str(latest_job.get("stage") or ""),
                        "reason": "already_running",
                    }
                return self._resume_blocked_workflow_if_ready(job_id, assume_lock=True)
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        if str(job.get("status") or "") != "blocked" or str(job.get("stage") or "") != "acquiring":
            return {"job_id": job_id, "status": "skipped", "reason": "job_not_blocked_in_acquisition"}
        blocked_task = str(dict(job.get("summary") or {}).get("blocked_task") or "")
        if not blocked_task:
            return {"job_id": job_id, "status": "skipped", "reason": "blocked_task_missing"}

        workers = self.agent_runtime.list_workers(job_id=job_id)
        readiness = self._assess_acquisition_resume_readiness(
            job=job,
            blocked_task=blocked_task,
            workers=workers,
        )
        if str(readiness.get("status") or "") != "ready":
            return {
                "job_id": job_id,
                "status": "waiting",
                "reason": str(readiness.get("reason") or "pending_workers_remaining"),
                "baseline_ready": bool(readiness.get("baseline_ready")),
                "baseline_reason": str(readiness.get("baseline_reason") or ""),
                "pending_worker_count": int(readiness.get("pending_worker_count") or 0),
                "pending_workers": list(readiness.get("pending_workers") or [])[:10],
            }

        request = readiness["request"]
        plan = readiness["plan"]
        if (
            blocked_task == "acquire_full_roster"
            and str(readiness.get("baseline_reason") or "") == "search_seed_entries_present"
        ):
            job_summary = dict(job.get("summary") or {})
            acquisition_progress = _normalize_acquisition_progress_payload(job_summary.get("acquisition_progress"))
            latest_state = dict(acquisition_progress.get("latest_state") or {})
            latest_state["allow_search_seed_baseline_for_full_roster"] = True
            acquisition_progress["latest_state"] = latest_state
            job_summary["acquisition_progress"] = acquisition_progress
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="blocked",
                stage="acquiring",
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                summary_payload=job_summary,
            )
        try:
            resume_result = self._run_workflow_from_acquisition(job_id, request, plan, resume_mode=True)
            latest_job = self.store.get_job(job_id) or {}
            return {
                "job_id": job_id,
                "status": "resumed",
                "blocked_task": blocked_task,
                "baseline_ready": bool(readiness.get("baseline_ready")),
                "baseline_reason": str(readiness.get("baseline_reason") or ""),
                "resume_result": resume_result,
                "job_status": str(latest_job.get("status") or ""),
                "job_stage": str(latest_job.get("stage") or ""),
            }
        except Exception as exc:
            self._mark_workflow_failed(job_id, request, plan, exc)
            return {
                "job_id": job_id,
                "status": "failed",
                "blocked_task": blocked_task,
                "error": str(exc),
            }

    def _resume_running_workflow_if_ready(
        self,
        job_id: str,
        *,
        assume_lock: bool = False,
        allow_stale_lease_takeover: bool = True,
    ) -> dict[str, Any]:
        if not assume_lock:
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
                    takeover = (
                        self._release_stale_workflow_job_lease_for_recovery(job_id)
                        if allow_stale_lease_takeover
                        else {"released": False}
                    )
                    if bool(takeover.get("released")):
                        return self._resume_running_workflow_if_ready(
                            job_id,
                            assume_lock=False,
                            allow_stale_lease_takeover=False,
                        )
                    latest_job = self.store.get_job(job_id) or {}
                    return {
                        "job_id": job_id,
                        "status": "skipped",
                        "stage": str(latest_job.get("stage") or ""),
                        "reason": "already_running",
                    }
                return self._resume_running_workflow_if_ready(job_id, assume_lock=True)
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        if str(job.get("status") or "") != "running" or str(job.get("stage") or "") != "acquiring":
            return {"job_id": job_id, "status": "skipped", "reason": "job_not_running_in_acquisition"}

        workers = self.agent_runtime.list_workers(job_id=job_id)
        blocked_task = str(dict(job.get("summary") or {}).get("blocked_task") or "")
        readiness = self._assess_acquisition_resume_readiness(
            job=job,
            blocked_task=blocked_task,
            workers=workers,
        )
        if str(readiness.get("status") or "") != "ready":
            return {
                "job_id": job_id,
                "status": "waiting",
                "reason": str(readiness.get("reason") or "pending_workers_remaining"),
                "baseline_ready": bool(readiness.get("baseline_ready")),
                "baseline_reason": str(readiness.get("baseline_reason") or ""),
                "pending_worker_count": int(readiness.get("pending_worker_count") or 0),
                "pending_workers": list(readiness.get("pending_workers") or [])[:10],
            }

        request = readiness["request"]
        plan = readiness["plan"]
        try:
            resume_result = self._run_workflow_from_acquisition(job_id, request, plan, resume_mode=True)
            latest_job = self.store.get_job(job_id) or {}
            return {
                "job_id": job_id,
                "status": "resumed",
                "resume_mode": "running_acquiring_recovery",
                "baseline_ready": bool(readiness.get("baseline_ready")),
                "baseline_reason": str(readiness.get("baseline_reason") or ""),
                "resume_result": resume_result,
                "job_status": str(latest_job.get("status") or ""),
                "job_stage": str(latest_job.get("stage") or ""),
            }
        except Exception as exc:
            self._mark_workflow_failed(job_id, request, plan, exc)
            return {
                "job_id": job_id,
                "status": "failed",
                "resume_mode": "running_acquiring_recovery",
                "error": str(exc),
            }

    def _collect_acquisition_blocking_workers(
        self,
        *,
        workers: list[dict[str, Any]],
        blocked_task: str,
    ) -> list[dict[str, Any]]:
        return [
            _summarize_worker_runtime(worker)
            for worker in self._collect_acquisition_blocking_worker_records(
                workers=workers,
                blocked_task=blocked_task,
            )
        ]

    def _collect_acquisition_blocking_worker_records(
        self,
        *,
        workers: list[dict[str, Any]],
        blocked_task: str,
    ) -> list[dict[str, Any]]:
        return [worker for worker in workers if _worker_blocks_acquisition_resume(worker, blocked_task=blocked_task)]

    def _finalize_failed_acquisition_progress(
        self,
        *,
        acquisition_progress: dict[str, Any],
        plan: Any,
        exc: Exception,
    ) -> dict[str, Any]:
        progress = _normalize_acquisition_progress_payload(acquisition_progress)
        if not progress:
            return {}
        failed_at = _utc_now_iso()
        error_text = str(exc)
        tasks_payload = {
            str(task_id or "").strip(): dict(payload or {})
            for task_id, payload in dict(progress.get("tasks") or {}).items()
            if str(task_id or "").strip()
        }
        phases_payload = {
            str(phase_id or "").strip(): dict(payload or {})
            for phase_id, payload in dict(progress.get("phases") or {}).items()
            if str(phase_id or "").strip()
        }
        plan_task_map = {
            task.task_id: task
            for task in getattr(plan, "acquisition_tasks", [])
            if isinstance(task, AcquisitionTask) and str(task.task_id or "").strip()
        }
        failed_task_ids: list[str] = []
        for task_id, task_payload in list(tasks_payload.items()):
            task_status = str(task_payload.get("status") or "").strip().lower()
            if task_status in {"completed", "skipped", "failed"}:
                continue
            task_payload["status"] = "failed"
            task_payload["detail"] = error_text
            task_payload["error"] = error_text
            task_payload["failed_at"] = failed_at
            tasks_payload[task_id] = task_payload
            failed_task_ids.append(task_id)

        active_phase_id = str(progress.get("active_phase_id") or "").strip()
        active_phase_title = str(progress.get("active_phase_title") or "").strip()
        if active_phase_id:
            phase_payload = dict(phases_payload.get(active_phase_id) or {})
            phase_payload["phase_id"] = active_phase_id
            phase_payload["title"] = (
                phase_payload.get("title") or active_phase_title or active_phase_id.replace("_", " ").title()
            )
            phase_task_ids = [
                str(item or "").strip() for item in list(phase_payload.get("task_ids") or []) if str(item or "").strip()
            ]
            if not phase_task_ids:
                phase_task_ids = [
                    task.task_id
                    for task in getattr(plan, "acquisition_tasks", [])
                    if isinstance(task, AcquisitionTask)
                    and str(task.metadata.get("acquisition_phase") or "").strip() == active_phase_id
                ]
            phase_completed_task_ids = [
                str(item or "").strip()
                for item in list(phase_payload.get("completed_task_ids") or [])
                if str(item or "").strip()
            ]
            phase_payload["task_ids"] = phase_task_ids
            phase_payload["completed_task_ids"] = phase_completed_task_ids
            phase_payload["completed_task_count"] = len(phase_completed_task_ids)
            phase_payload["task_count"] = len(phase_task_ids)
            phase_payload["status"] = "failed"
            phase_payload["failed_at"] = failed_at
            phase_payload["error"] = error_text
            phases_payload[active_phase_id] = phase_payload
            if not failed_task_ids:
                for task_id in phase_task_ids:
                    task_status = str(dict(tasks_payload.get(task_id) or {}).get("status") or "").strip().lower()
                    if task_status in {"completed", "skipped", "failed"}:
                        continue
                    task = plan_task_map.get(task_id)
                    fallback_payload = dict(tasks_payload.get(task_id) or {})
                    fallback_payload["task_id"] = task_id
                    if isinstance(task, AcquisitionTask):
                        fallback_payload["task_type"] = task.task_type
                        fallback_payload["title"] = task.title
                    fallback_payload["status"] = "failed"
                    fallback_payload["detail"] = error_text
                    fallback_payload["error"] = error_text
                    fallback_payload["failed_at"] = failed_at
                    tasks_payload[task_id] = fallback_payload
                    failed_task_ids.append(task_id)
                    break

        progress["status"] = "failed"
        progress["failed_at"] = failed_at
        progress["tasks"] = tasks_payload
        progress["phases"] = phases_payload
        if failed_task_ids:
            progress["last_failed_task_id"] = failed_task_ids[-1]
        return progress

    def _mark_workflow_failed(self, job_id: str, request: JobRequest, plan: Any, exc: Exception) -> None:
        existing = self.store.get_job(job_id) or {}
        if str(existing.get("status") or "") == "completed":
            return
        failure_summary = dict(existing.get("summary") or {})
        failure_summary["error"] = str(exc)
        failure_summary["message"] = str(exc)
        acquisition_progress = self._finalize_failed_acquisition_progress(
            acquisition_progress=failure_summary.get("acquisition_progress") or {},
            plan=plan,
            exc=exc,
        )
        if acquisition_progress:
            failure_summary["acquisition_progress"] = acquisition_progress
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="failed",
            stage="failed",
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
            summary_payload=failure_summary,
        )
        self.store.update_agent_runtime_session_status(job_id, "failed")
        self.store.append_job_event(job_id, "failed", "failed", str(exc))

    def _wait_for_workflow_terminal_status(
        self, job_id: str, *, timeout_seconds: float, poll_seconds: float = 0.5
    ) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.5, timeout_seconds)
        latest_job = self.store.get_job(job_id) or {}
        while time.monotonic() < deadline:
            latest_job = self.store.get_job(job_id) or {}
            if str(latest_job.get("status") or "") in {"completed", "failed"}:
                return latest_job
            time.sleep(max(0.1, poll_seconds))
        return latest_job

    @contextmanager
    def _job_run_lock(self, job_id: str):
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            yield None
            return
        lease_owner = self._workflow_job_lease_owner()
        lease_token = uuid.uuid4().hex
        lease_seconds = max(30, _env_int("WORKFLOW_JOB_RUN_LEASE_SECONDS", 900))
        lease = self.store.acquire_workflow_job_lease(
            normalized_job_id,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
            lease_token=lease_token,
        )
        if not bool(lease.get("acquired")):
            yield None
            return
        use_filesystem_lock = not bool(self.store.workflow_job_coordination_uses_postgres())
        lock_path = self.job_locks_dir / f"{job_id}.lock"

        def _cleanup_lock_path() -> None:
            try:
                if lock_path.exists():
                    lock_path.unlink()
            except OSError:
                return

        handle = None
        if use_filesystem_lock:
            handle = lock_path.open("a+", encoding="utf-8")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                handle.close()
                _cleanup_lock_path()
                recovered_lock = self._recover_stale_workflow_job_lock(
                    normalized_job_id,
                    lock_path,
                    ignore_lease_owner=lease_owner,
                    ignore_lease_token=lease_token,
                )
                if bool(recovered_lock.get("recovered")):
                    handle = lock_path.open("a+", encoding="utf-8")
                    try:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        handle.close()
                        _cleanup_lock_path()
                        self.store.release_workflow_job_lease(
                            normalized_job_id,
                            lease_owner=lease_owner,
                            lease_token=lease_token,
                        )
                        yield None
                        return
                else:
                    _cleanup_lock_path()
                    self.store.release_workflow_job_lease(
                        normalized_job_id,
                        lease_owner=lease_owner,
                        lease_token=lease_token,
                    )
                    yield None
                    return
        renew_interval_seconds = max(5, min(max(lease_seconds // 3, 1), 60))
        stop_renewal = threading.Event()

        def _renew_lease_forever() -> None:
            while not stop_renewal.wait(renew_interval_seconds):
                try:
                    renewed = self.store.renew_workflow_job_lease(
                        normalized_job_id,
                        lease_owner=lease_owner,
                        lease_token=lease_token,
                        lease_seconds=lease_seconds,
                    )
                except Exception:
                    break
                if not renewed or str(renewed.get("lease_token") or "") != lease_token:
                    break

        renewal_thread = threading.Thread(
            target=_renew_lease_forever,
            name=f"workflow-job-lease-{normalized_job_id}",
            daemon=True,
        )
        renewal_thread.start()
        try:
            yield {
                "handle": handle,
                "uses_filesystem_lock": use_filesystem_lock,
                "lease_owner": lease_owner,
                "lease_token": lease_token,
                "lease_seconds": lease_seconds,
            }
        finally:
            stop_renewal.set()
            renewal_thread.join(timeout=max(1.0, float(renew_interval_seconds)))
            if handle is not None:
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                finally:
                    handle.close()
                _cleanup_lock_path()
            self.store.release_workflow_job_lease(
                normalized_job_id,
                lease_owner=lease_owner,
                lease_token=lease_token,
            )

    def _job_is_terminal(self, job: dict[str, Any] | None) -> bool:
        if not isinstance(job, dict):
            return False
        return str(job.get("status") or "") in {"completed", "failed", "superseded", "cancelled", "canceled"}

    def _asset_population_fast_path_context(
        self,
        *,
        request: JobRequest,
        plan: Any,
        candidate_source: dict[str, Any],
        runtime_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        candidate_source = dict(candidate_source or {})
        if not candidate_source_is_snapshot_authoritative(candidate_source):
            return {}
        runtime_policy = dict(runtime_policy or {})
        analysis_stage = str(runtime_policy.get("analysis_stage") or "stage_2_final").strip().lower() or "stage_2_final"
        runtime_mode = str(runtime_policy.get("mode") or "").strip().lower()
        workflow_snapshot_id = str(runtime_policy.get("workflow_snapshot_id") or "").strip()
        asset_view = (
            str(candidate_source.get("asset_view") or request.asset_view or "canonical_merged").strip()
            or "canonical_merged"
        )
        allow_preview_fast_path = analysis_stage == "stage_1_preview"
        allow_direct_snapshot_fast_path = bool(
            not workflow_snapshot_id
            and (
                list(request.keywords or [])
                or list(request.must_have_keywords or [])
                or list(request.organization_keywords or [])
                or list(request.must_have_facets or [])
                or list(request.must_have_primary_role_buckets or [])
            )
        )
        if asset_view != "canonical_merged":
            return {}
        plan_payload = _plan_payload(plan)
        asset_reuse_plan = dict(plan_payload.get("asset_reuse_plan") or getattr(plan, "asset_reuse_plan", {}) or {})
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=candidate_source,
        )
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=asset_reuse_plan,
            candidate_source=candidate_source,
        )
        allow_workflow_snapshot_asset_population_fast_path = bool(
            workflow_snapshot_id
            and analysis_stage == "stage_2_final"
            and runtime_mode != "post_acquisition_refinement"
            and str(dict(effective_execution_semantics or {}).get("default_results_mode") or "").strip().lower()
            == "asset_population"
        )
        if (
            not allow_preview_fast_path
            and not allow_direct_snapshot_fast_path
            and not allow_workflow_snapshot_asset_population_fast_path
        ):
            return {}
        if (
            not allow_preview_fast_path
            and not allow_direct_snapshot_fast_path
            and not allow_workflow_snapshot_asset_population_fast_path
            and (
                str(dict(effective_execution_semantics or {}).get("default_results_mode") or "").strip().lower()
                != "asset_population"
            )
        ):
            return {}
        return {
            "organization_execution_profile": organization_execution_profile,
            "asset_reuse_plan": asset_reuse_plan,
            "effective_execution_semantics": effective_execution_semantics,
        }

    def _build_asset_population_fast_path_manual_review_items(
        self,
        *,
        request: JobRequest,
        candidates: list[Candidate],
        snapshot_id: str,
    ) -> tuple[list[dict[str, Any]], int]:
        limit = max(0, _env_int("ASSET_POPULATION_FAST_PATH_MANUAL_REVIEW_LIMIT", 200))
        items: list[dict[str, Any]] = []
        total_count = 0
        seen: set[tuple[str, str]] = set()
        for candidate in candidates:
            candidate_payload = candidate.to_record()
            metadata = dict(candidate.metadata or {})
            reasons: list[str] = []
            review_type = ""
            priority = "medium"
            if bool(metadata.get("membership_review_required")):
                review_type = "manual_identity_resolution"
                priority = "high"
                reasons.append("suspicious_membership")
            elif candidate.category == "lead":
                review_type = "manual_identity_resolution"
                priority = "high"
                reasons.append("unverified_membership")
            elif not str(candidate.linkedin_url or "").strip():
                review_type = "missing_primary_profile"
                reasons.append("missing_linkedin_profile")
            else:
                experience_lines = _normalized_text_lines(
                    metadata.get("experience_lines") or candidate_payload.get("experience_lines")
                )
                education_lines = _normalized_text_lines(
                    metadata.get("education_lines") or candidate_payload.get("education_lines")
                )
                has_profile_detail = _timeline_has_complete_profile_detail(
                    {
                        "experience_lines": experience_lines,
                        "education_lines": education_lines,
                        "headline": metadata.get("headline") or candidate_payload.get("headline") or candidate.role,
                        "summary": metadata.get("summary") or candidate_payload.get("summary") or candidate.notes,
                        "about": metadata.get("about") or candidate_payload.get("about"),
                        "primary_email": (
                            metadata.get("primary_email")
                            or metadata.get("email")
                            or candidate_payload.get("primary_email")
                            or candidate_payload.get("email")
                        ),
                        "languages": metadata.get("languages") or candidate_payload.get("languages"),
                        "skills": metadata.get("skills") or candidate_payload.get("skills"),
                        "profile_capture_kind": (
                            metadata.get("profile_capture_kind")
                            or candidate_payload.get("profile_capture_kind")
                        ),
                        "profile_capture_source_path": (
                            metadata.get("profile_capture_source_path")
                            or candidate_payload.get("profile_capture_source_path")
                        ),
                        "source_path": metadata.get("profile_timeline_source_path")
                        or candidate_payload.get("profile_timeline_source_path")
                        or metadata.get("source_path")
                        or candidate_payload.get("source_path")
                        or candidate.source_path,
                    }
                )
                needs_profile_completion = bool(candidate.linkedin_url and not has_profile_detail)
                low_profile_richness = bool(
                    candidate.linkedin_url
                    and has_profile_detail
                    and not needs_profile_completion
                    and (not experience_lines or not education_lines)
                )
                if needs_profile_completion:
                    review_type = "profile_completion_gap"
                    reasons.append("missing_profile_detail")
                elif low_profile_richness:
                    review_type = "profile_enrichment_opportunity"
                    reasons.append("low_profile_richness")

            if not review_type:
                continue
            dedupe_key = (str(candidate.candidate_id or "").strip(), review_type)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            total_count += 1
            if limit > 0 and len(items) >= limit:
                continue
            items.append(
                {
                    "candidate_id": candidate.candidate_id,
                    "target_company": candidate.target_company,
                    "review_type": review_type,
                    "priority": priority,
                    "status": "open",
                    "summary": (
                        f"{candidate.display_name or candidate.name_en} requires manual review because "
                        f"{', '.join(reasons)}."
                    ),
                    "candidate": candidate.to_record(),
                    "evidence": [],
                    "metadata": {
                        "reasons": reasons,
                        "snapshot_id": snapshot_id,
                    },
                }
            )
        return items, total_count

    def _execute_asset_population_fast_path(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        job_type: str,
        runtime_policy: dict[str, Any],
        candidate_source: dict[str, Any],
        organization_execution_profile: dict[str, Any],
        effective_execution_semantics: dict[str, Any],
        plan_asset_reuse_plan: dict[str, Any],
        persist_job_state: bool,
        artifact_name_suffix: str,
        artifact_status: str,
    ) -> dict[str, Any]:
        analysis_stage = str(runtime_policy.get("analysis_stage") or "stage_2_final").strip() or "stage_2_final"
        preview_mode = analysis_stage == "stage_1_preview" or bool(runtime_policy.get("deterministic_preview"))
        effective_request = _build_effective_retrieval_request(request, plan)
        candidates = list(candidate_source.get("candidates") or [])
        candidate_count = len(candidates)
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        criteria_patterns = self.store.list_criteria_patterns(target_company=request.target_company)
        confidence_feedback = self.store.list_criteria_feedback(target_company=request.target_company, limit=500)
        confidence_policy = build_confidence_policy(
            target_company=request.target_company,
            feedback_items=confidence_feedback,
            request_payload=effective_request.to_record(),
        )
        control = self.store.find_active_confidence_policy_control(
            target_company=request.target_company,
            request_payload=effective_request.to_record(),
        )
        confidence_policy = apply_policy_control(confidence_policy, control)
        manual_review_items, manual_review_queue_count = self._build_asset_population_fast_path_manual_review_items(
            request=request,
            candidates=candidates,
            snapshot_id=snapshot_id,
        )
        baseline_selection_explanation = dict(plan_asset_reuse_plan.get("baseline_selection_explanation") or {})
        candidate_source_summary = {
            "source_kind": str(candidate_source.get("source_kind") or ""),
            "snapshot_id": snapshot_id,
            "asset_view": str(candidate_source.get("asset_view") or ""),
            "candidate_count": candidate_count,
            "unfiltered_candidate_count": candidate_count,
            "source_path": str(candidate_source.get("source_path") or ""),
            "authoritative_snapshot_id": str(candidate_source.get("authoritative_snapshot_id") or "").strip(),
            "materialization_generation_key": str(candidate_source.get("materialization_generation_key") or "").strip(),
            "materialization_generation_sequence": int(candidate_source.get("materialization_generation_sequence") or 0),
            "materialization_watermark": str(candidate_source.get("materialization_watermark") or "").strip(),
        }
        if dict(candidate_source.get("asset_population_patch") or {}):
            candidate_source_summary["asset_population_patch"] = dict(candidate_source.get("asset_population_patch") or {})
        if (
            baseline_selection_explanation
            and candidate_source_is_snapshot_authoritative(candidate_source)
            and snapshot_id == str(baseline_selection_explanation.get("matched_registry_snapshot_id") or "").strip()
        ):
            candidate_source_summary["baseline_selection_explanation"] = baseline_selection_explanation
        overlay_info = (
            self._write_job_asset_population_overlay(
                job_id=job_id,
                request=request,
                candidate_source=candidate_source,
            )
            if not preview_mode
            else {}
        )
        if overlay_info:
            candidate_source_summary["asset_population_overlay_path"] = str(overlay_info.get("path") or "").strip()
            existing_patch_payload = candidate_source_summary.get("asset_population_patch")
            if isinstance(existing_patch_payload, dict) and existing_patch_payload:
                patch_payload = dict(existing_patch_payload)
                patch_payload["overlay_path"] = str(overlay_info.get("path") or "").strip()
                candidate_source_summary["asset_population_patch"] = patch_payload
        if preview_mode:
            summary_text = (
                f"Stage 1 preview is ready with {candidate_count} candidates. "
                "Final asset population is still materializing."
            )
        else:
            summary_text = f"Local asset population is ready with {candidate_count} candidates."
            if (
                list(request.keywords or [])
                or list(request.must_have_keywords or [])
                or list(request.organization_keywords or [])
            ):
                summary_text += " Results default to the full asset population; use board filters to narrow by intent."
        if manual_review_queue_count > 0:
            summary_text += (
                f" {manual_review_queue_count} candidates currently need manual review or profile completion."
            )
        effective_request_overrides = _effective_request_overrides(request, effective_request)
        summary = {
            "text": summary_text,
            "total_matches": candidate_count,
            "returned_matches": candidate_count,
            "retrieval_strategy": _plan_retrieval_strategy(plan),
            "criteria_pattern_count": len(criteria_patterns),
            "summary_provider": "asset_population_fast_path",
            "rerun_mode": runtime_policy.get("mode", "standard"),
            "analysis_stage": analysis_stage,
            "analysis_stage_mode": str(getattr(request, "analysis_stage_mode", "") or "single_stage"),
            "deterministic_preview": preview_mode,
            "confidence_policy": confidence_policy["summary"],
            "manual_review_queue_count": manual_review_queue_count,
            "semantic_hit_count": 0,
            "semantic_rerank_limit": request.semantic_rerank_limit,
            "candidate_source": candidate_source_summary,
            "asset_population_fast_path": True,
            "default_results_mode": str(dict(effective_execution_semantics or {}).get("default_results_mode") or ""),
        }
        if dict(runtime_policy.get("outreach_layering") or {}):
            summary["outreach_layering"] = dict(runtime_policy.get("outreach_layering") or {})
        if dict(runtime_policy.get("background_snapshot_materialization") or {}):
            summary["background_snapshot_materialization"] = dict(
                runtime_policy.get("background_snapshot_materialization") or {}
            )
        if overlay_info:
            summary["asset_population_overlay"] = {
                "path": str(overlay_info.get("path") or "").strip(),
                "candidate_count": int(overlay_info.get("candidate_count") or 0),
            }
        if effective_request_overrides:
            summary["effective_request_overrides"] = effective_request_overrides
        if control:
            summary["confidence_policy_control"] = {
                "control_id": int(control.get("control_id") or 0),
                "control_mode": str(control.get("control_mode") or ""),
                "scope_kind": str(control.get("scope_kind") or ""),
                "selection_reason": str(control.get("selection_reason") or ""),
            }
        if persist_job_state:
            result_view = self._persist_job_result_view(
                job_id=job_id,
                request=request,
                candidate_source=candidate_source_summary,
                organization_execution_profile=organization_execution_profile,
                summary_payload=summary,
                effective_execution_semantics=effective_execution_semantics,
                baseline_selection_explanation=baseline_selection_explanation,
            )
            if result_view:
                candidate_source_summary = self._apply_job_result_view_to_candidate_source(
                    candidate_source_summary,
                    result_view,
                )
                summary["candidate_source"] = candidate_source_summary
        artifact = {
            "job_id": job_id,
            "status": artifact_status,
            "request": request.to_record(),
            "effective_request": effective_request.to_record(),
            "request_preview": _build_request_preview_payload(
                request=request,
                effective_request=effective_request,
            ),
            "plan": _plan_payload(plan),
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
            "summary": summary,
            "matches": [],
            "manual_review_items": manual_review_items,
            "criteria_patterns_applied": criteria_patterns,
            "semantic_hits": [],
            "confidence_policy": confidence_policy,
            "confidence_policy_control": control or {},
            "runtime_policy": runtime_policy,
        }
        artifact_filename = f"{job_id}.json" if not artifact_name_suffix else f"{job_id}.{artifact_name_suffix}.json"
        artifact_path = self.jobs_dir / artifact_filename
        artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.replace_job_results(job_id, [])
        self.store.replace_manual_review_items(job_id, manual_review_items)
        if persist_job_state:
            self.store.save_job(
                job_id=job_id,
                job_type=job_type,
                status="completed",
                stage="completed",
                request_payload=request.to_record(),
                plan_payload=_plan_payload(plan),
                summary_payload=summary,
                artifact_path=str(artifact_path),
            )
        artifact["artifact_path"] = str(artifact_path)
        return artifact

    def _execute_retrieval(
        self,
        job_id: str,
        request: JobRequest,
        plan,
        job_type: str,
        runtime_policy: dict[str, Any] | None = None,
        candidate_source_override: dict[str, Any] | None = None,
        *,
        persist_job_state: bool = True,
        artifact_name_suffix: str = "",
        artifact_status: str = "completed",
    ) -> dict[str, Any]:
        runtime_policy = dict(runtime_policy or {})
        analysis_stage = str(runtime_policy.get("analysis_stage") or "stage_2_final").strip() or "stage_2_final"
        preview_mode = analysis_stage == "stage_1_preview" or bool(runtime_policy.get("deterministic_preview"))
        preview_candidate_doc_path = (
            self._resolve_stage1_preview_candidate_doc_path(
                request=request,
                plan=plan,
                runtime_policy=runtime_policy,
            )
            if preview_mode
            else ""
        )
        candidate_source = dict(candidate_source_override or {})
        if not candidate_source:
            candidate_source = self._load_retrieval_candidate_source(
                request,
                snapshot_id=str(runtime_policy.get("workflow_snapshot_id") or "").strip(),
                allow_materialization_fallback=not preview_mode,
                candidate_doc_path=preview_candidate_doc_path if preview_mode else None,
            )
        plan_payload = _plan_payload(plan)
        organization_execution_profile = self._organization_execution_profile_from_plan_payload(
            plan_payload=plan_payload,
            request=request,
            candidate_source=candidate_source,
        )
        plan_asset_reuse_plan = dict(getattr(plan, "asset_reuse_plan", {}) or {})
        baseline_selection_explanation = dict(plan_asset_reuse_plan.get("baseline_selection_explanation") or {})
        effective_execution_semantics = self._build_effective_execution_semantics(
            request=request,
            organization_execution_profile=organization_execution_profile,
            asset_reuse_plan=plan_asset_reuse_plan,
            candidate_source=candidate_source,
        )
        asset_population_fast_path = self._asset_population_fast_path_context(
            request=request,
            plan=plan,
            candidate_source=candidate_source,
            runtime_policy=runtime_policy,
        )
        if asset_population_fast_path:
            return self._execute_asset_population_fast_path(
                job_id=job_id,
                request=request,
                plan=plan,
                job_type=job_type,
                runtime_policy=runtime_policy,
                candidate_source=candidate_source,
                organization_execution_profile=dict(
                    asset_population_fast_path.get("organization_execution_profile") or organization_execution_profile
                ),
                effective_execution_semantics=dict(
                    asset_population_fast_path.get("effective_execution_semantics") or {}
                ),
                plan_asset_reuse_plan=dict(asset_population_fast_path.get("asset_reuse_plan") or plan_asset_reuse_plan),
                persist_job_state=persist_job_state,
                artifact_name_suffix=artifact_name_suffix,
                artifact_status=artifact_status,
            )
        effective_request = _build_effective_retrieval_request(request, plan)
        candidates = list(candidate_source.get("candidates") or [])
        unfiltered_candidate_count = len(candidates)
        outreach_layering_context = self._load_outreach_layering_context(
            request=request,
            candidate_source=candidate_source,
            runtime_policy=runtime_policy,
        )
        if outreach_layering_context:
            candidates = self._attach_outreach_layering_metadata(candidates, outreach_layering_context)
        if outreach_layering_context:
            filter_policy = _resolve_outreach_layer_filter_policy(effective_request)
            baseline_candidates = list(candidates)
            applied_filter = {
                "enabled": False,
                "policy_scope": str(filter_policy.get("scope") or ""),
                "initial_candidate_count": len(baseline_candidates),
                "min_layer": int(filter_policy.get("min_layer") or 0),
                "fallback_min_layer": int(filter_policy.get("fallback_min_layer") or 0),
                "fallback_applied": False,
                "filtered_candidate_count": len(baseline_candidates),
            }
            if bool(filter_policy.get("enabled")):
                min_layer = int(filter_policy.get("min_layer") or 0)
                fallback_min_layer = int(filter_policy.get("fallback_min_layer") or 0)
                filtered = [
                    item for item in baseline_candidates if int(item.metadata.get("outreach_layer") or 0) >= min_layer
                ]
                if not filtered and 0 < fallback_min_layer < min_layer:
                    fallback_candidates = [
                        item
                        for item in baseline_candidates
                        if int(item.metadata.get("outreach_layer") or 0) >= fallback_min_layer
                    ]
                    if fallback_candidates:
                        filtered = fallback_candidates
                        applied_filter["fallback_applied"] = True
                        applied_filter["min_layer"] = fallback_min_layer
                if filtered:
                    candidates = filtered
                applied_filter["enabled"] = True
                applied_filter["filtered_candidate_count"] = len(candidates)
            outreach_layering_context["applied_filter"] = applied_filter
        source_evidence_lookup = dict(candidate_source.get("evidence_lookup") or {})
        criteria_patterns = self.store.list_criteria_patterns(target_company=request.target_company)
        retrieval_strategy = _plan_retrieval_strategy(plan)
        semantic_hits = {}
        semantic_enabled = _env_bool("SOURCING_RETRIEVAL_ENABLE_SEMANTIC", False)
        if (
            not preview_mode
            and semantic_enabled
            and retrieval_strategy in {"hybrid", "semantic", "semantic_heavy"}
            and effective_request.semantic_rerank_limit > 0
        ):
            semantic_hits = rank_semantic_candidates(
                candidates,
                effective_request,
                criteria_patterns=criteria_patterns,
                semantic_fields=_plan_semantic_fields(plan),
                limit=effective_request.semantic_rerank_limit,
                semantic_provider=self.semantic_provider,
            )
        confidence_feedback = self.store.list_criteria_feedback(target_company=request.target_company, limit=500)
        confidence_policy = build_confidence_policy(
            target_company=request.target_company,
            feedback_items=confidence_feedback,
            request_payload=effective_request.to_record(),
        )
        control = self.store.find_active_confidence_policy_control(
            target_company=request.target_company,
            request_payload=effective_request.to_record(),
        )
        confidence_policy = apply_policy_control(confidence_policy, control)
        scored = score_candidates(
            candidates,
            effective_request,
            criteria_patterns=criteria_patterns,
            confidence_policy=confidence_policy,
            semantic_hits=semantic_hits,
        )
        evidence_lookup: dict[str, list[dict[str, Any]]] = {}
        matches = []
        persisted_results = []
        for rank, item in enumerate(scored[: effective_request.top_k], start=1):
            evidence = list(source_evidence_lookup.get(item.candidate.candidate_id) or [])
            if not evidence and not candidate_source_is_snapshot_authoritative(candidate_source):
                evidence = self.store.list_evidence(item.candidate.candidate_id)
            self.store.upsert_candidate(item.candidate)
            if evidence:
                evidence_records = _evidence_records_from_payload(evidence)
                if evidence_records:
                    self.store.upsert_evidence_records(evidence_records)
            evidence_lookup[item.candidate.candidate_id] = evidence
            match = {
                "candidate_id": item.candidate.candidate_id,
                "display_name": item.candidate.display_name,
                "name_en": item.candidate.name_en,
                "name_zh": item.candidate.name_zh,
                "category": item.candidate.category,
                "target_company": item.candidate.target_company,
                "organization": item.candidate.organization,
                "employment_status": item.candidate.employment_status,
                "role_bucket": derive_candidate_role_bucket(item.candidate),
                "functional_facets": derive_candidate_facets(item.candidate),
                "role": item.candidate.role,
                "team": item.candidate.team,
                "focus_areas": item.candidate.focus_areas,
                "education": item.candidate.education,
                "work_history": item.candidate.work_history,
                "notes": item.candidate.notes,
                "linkedin_url": item.candidate.linkedin_url,
                "score": item.score,
                "semantic_score": item.semantic_score,
                "confidence_label": item.confidence_label,
                "confidence_score": item.confidence_score,
                "confidence_reason": item.confidence_reason,
                "outreach_layer": int(item.candidate.metadata.get("outreach_layer") or 0),
                "outreach_layer_key": str(item.candidate.metadata.get("outreach_layer_key") or ""),
                "outreach_layer_source": str(item.candidate.metadata.get("outreach_layer_source") or ""),
                "rank": rank,
                "matched_fields": item.matched_fields,
                "explanation": item.explanation,
                "evidence": evidence[:3],
            }
            matches.append(match)
            persisted_results.append(
                {
                    "candidate_id": item.candidate.candidate_id,
                    "rank": rank,
                    "score": item.score,
                    "semantic_score": item.semantic_score,
                    "confidence_label": item.confidence_label,
                    "confidence_score": item.confidence_score,
                    "confidence_reason": item.confidence_reason,
                    "outreach_layer": int(item.candidate.metadata.get("outreach_layer") or 0),
                    "outreach_layer_key": str(item.candidate.metadata.get("outreach_layer_key") or ""),
                    "outreach_layer_source": str(item.candidate.metadata.get("outreach_layer_source") or ""),
                    "explanation": item.explanation,
                    "matched_fields": item.matched_fields,
                }
            )
        manual_review_items = build_manual_review_items(effective_request, scored, evidence_lookup)
        if (
            not manual_review_items
            and not matches
            and not list(request.categories or [])
            and _request_uses_default_full_employment_scope(request)
        ):
            relaxed_review_request = JobRequest.from_payload(
                {
                    **effective_request.to_record(),
                    "categories": [],
                    "employment_statuses": [],
                }
            )
            relaxed_review_request.categories = []
            relaxed_review_request.employment_statuses = []
            relaxed_review_scored = score_candidates(
                candidates,
                relaxed_review_request,
                criteria_patterns=criteria_patterns,
                confidence_policy=confidence_policy,
                semantic_hits=semantic_hits,
            )
            relaxed_review_evidence_lookup: dict[str, list[dict[str, Any]]] = {}
            for item in relaxed_review_scored[: max(relaxed_review_request.top_k + 5, 10)]:
                evidence = list(source_evidence_lookup.get(item.candidate.candidate_id) or [])
                if not evidence and not candidate_source_is_snapshot_authoritative(candidate_source):
                    evidence = self.store.list_evidence(item.candidate.candidate_id)
                relaxed_review_evidence_lookup[item.candidate.candidate_id] = evidence
            manual_review_items = build_manual_review_items(
                relaxed_review_request,
                relaxed_review_scored,
                relaxed_review_evidence_lookup,
            )
        candidate_source_summary: dict[str, Any] = {
            "source_kind": str(candidate_source.get("source_kind") or ""),
            "snapshot_id": str(candidate_source.get("snapshot_id") or ""),
            "asset_view": str(candidate_source.get("asset_view") or ""),
            "candidate_count": len(candidates),
            "unfiltered_candidate_count": unfiltered_candidate_count,
            "source_path": str(candidate_source.get("source_path") or ""),
        }
        if (
            baseline_selection_explanation
            and candidate_source_is_snapshot_authoritative(candidate_source)
            and str(candidate_source.get("snapshot_id") or "").strip()
            == str(baseline_selection_explanation.get("matched_registry_snapshot_id") or "").strip()
        ):
            candidate_source_summary["baseline_selection_explanation"] = baseline_selection_explanation
        summary_mode = str(runtime_policy.get("summary_mode") or "").strip().lower()
        model_summary_enabled = _env_bool("SOURCING_RETRIEVAL_ENABLE_MODEL_SUMMARY", False)
        if preview_mode:
            summary_mode = "deterministic"
        if not model_summary_enabled:
            summary_mode = "deterministic"
        summary_provider = self.model_client.provider_name()
        if summary_mode == "deterministic":
            summary_provider = "deterministic"
            summary_text = DeterministicModelClient().summarize(effective_request, matches, len(scored))
        else:
            summary_text = self.model_client.summarize(effective_request, matches, len(scored))
        if (
            not matches
            and str(effective_request.target_scope or "").strip().lower() == "full_company_asset"
            and int(candidate_source_summary.get("candidate_count") or 0) > 0
        ):
            if preview_mode:
                summary_text = (
                    f"Stage 1 preview is ready with {int(candidate_source_summary.get('candidate_count') or 0)} "
                    "candidates. Keyword-ranked matches are currently 0 under the active thematic filters while final "
                    "asset population is still materializing."
                )
            else:
                summary_text = (
                    f"Local asset population is ready with {int(candidate_source_summary.get('candidate_count') or 0)} "
                    "candidates. Keyword-ranked matches are currently 0 under the active thematic filters."
                )
        effective_request_overrides = _effective_request_overrides(request, effective_request)
        summary = {
            "text": summary_text,
            "total_matches": len(scored),
            "returned_matches": len(matches),
            "retrieval_strategy": retrieval_strategy,
            "criteria_pattern_count": len(criteria_patterns),
            "summary_provider": summary_provider,
            "rerun_mode": runtime_policy.get("mode", "standard"),
            "analysis_stage": analysis_stage,
            "analysis_stage_mode": str(getattr(request, "analysis_stage_mode", "") or "single_stage"),
            "deterministic_preview": preview_mode,
            "confidence_policy": confidence_policy["summary"],
            "manual_review_queue_count": len(manual_review_items),
            "semantic_hit_count": len(semantic_hits),
            "semantic_rerank_limit": request.semantic_rerank_limit,
            "candidate_source": candidate_source_summary,
        }
        if effective_request_overrides:
            summary["effective_request_overrides"] = effective_request_overrides
        if outreach_layering_context:
            summary["outreach_layering"] = {
                "status": str(outreach_layering_context.get("status") or ""),
                "analysis_path": str(outreach_layering_context.get("analysis_path") or ""),
                "layer_counts": dict(outreach_layering_context.get("layer_counts") or {}),
                "cumulative_layer_counts": dict(outreach_layering_context.get("cumulative_layer_counts") or {}),
                "ai_verification": dict(outreach_layering_context.get("ai_verification") or {}),
                "candidate_layer_coverage": int(outreach_layering_context.get("candidate_layer_coverage") or 0),
                "applied_filter": dict(outreach_layering_context.get("applied_filter") or {}),
            }
        if control:
            summary["confidence_policy_control"] = {
                "control_id": int(control.get("control_id") or 0),
                "control_mode": str(control.get("control_mode") or ""),
                "scope_kind": str(control.get("scope_kind") or ""),
                "selection_reason": str(control.get("selection_reason") or ""),
            }
        if persist_job_state:
            result_view = self._persist_job_result_view(
                job_id=job_id,
                request=request,
                candidate_source=candidate_source_summary,
                organization_execution_profile=organization_execution_profile,
                summary_payload=summary,
                effective_execution_semantics={**effective_execution_semantics},
                baseline_selection_explanation=baseline_selection_explanation,
            )
            if result_view:
                candidate_source_summary = self._apply_job_result_view_to_candidate_source(
                    candidate_source_summary,
                    result_view,
                )
                summary["candidate_source"] = candidate_source_summary
        artifact = {
            "job_id": job_id,
            "status": artifact_status,
            "request": request.to_record(),
            "effective_request": effective_request.to_record(),
            "request_preview": _build_request_preview_payload(
                request=request,
                effective_request=effective_request,
            ),
            "plan": _plan_payload(plan),
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
            "summary": summary,
            "matches": matches,
            "manual_review_items": manual_review_items,
            "criteria_patterns_applied": criteria_patterns,
            "semantic_hits": list(semantic_hits.values()),
            "confidence_policy": confidence_policy,
            "confidence_policy_control": control or {},
            "runtime_policy": runtime_policy,
        }
        artifact_filename = f"{job_id}.json" if not artifact_name_suffix else f"{job_id}.{artifact_name_suffix}.json"
        artifact_path = self.jobs_dir / artifact_filename
        artifact_path.write_text(json.dumps(_storage_json_safe_payload(artifact), ensure_ascii=False, indent=2))
        self.store.replace_job_results(job_id, persisted_results)
        self.store.replace_manual_review_items(job_id, manual_review_items)
        if persist_job_state:
            self.store.save_job(
                job_id=job_id,
                job_type=job_type,
                status="completed",
                stage="completed",
                request_payload=request.to_record(),
                plan_payload=_plan_payload(plan),
                summary_payload=summary,
                artifact_path=str(artifact_path),
            )
        artifact["artifact_path"] = str(artifact_path)
        return artifact

    def _load_retrieval_candidate_source(
        self,
        request: JobRequest,
        *,
        snapshot_id: str = "",
        allow_materialization_fallback: bool = True,
        candidate_doc_path: str | Path | None = None,
    ) -> dict[str, Any]:
        return load_retrieval_candidate_source(
            runtime_dir=self.runtime_dir,
            store=self.store,
            request=request,
            snapshot_id=snapshot_id,
            allow_materialization_fallback=allow_materialization_fallback,
            allow_legacy_bootstrap_fallback=self._local_bootstrap_fallback_enabled_for_request(request),
            candidate_doc_path=candidate_doc_path,
        )

    def _load_outreach_layering_context(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        runtime_policy: dict[str, Any],
    ) -> dict[str, Any]:
        return load_outreach_layering_context(
            runtime_dir=self.runtime_dir,
            request=request,
            candidate_source=candidate_source,
            runtime_policy=runtime_policy,
        )

    def _attach_snapshot_outreach_layering_metadata(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        candidates: list[Candidate],
    ) -> dict[str, Any]:
        if not candidates:
            return {}
        context = self._load_outreach_layering_context(
            request=request,
            candidate_source=candidate_source,
            runtime_policy={},
        )
        if context:
            self._attach_outreach_layering_metadata(candidates, context)
        return context

    def _attach_snapshot_outreach_layering_metadata_to_records(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not records:
            return {}
        context = self._load_outreach_layering_context(
            request=request,
            candidate_source=candidate_source,
            runtime_policy={},
        )
        layer_map = dict(context.get("layer_map") or {})
        if not layer_map:
            return context
        for record in records:
            candidate_id = str(dict(record or {}).get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            layer_payload = dict(layer_map.get(candidate_id) or {})
            if not layer_payload:
                continue
            metadata = dict(record.get("metadata") or {})
            metadata.update(layer_payload)
            record["metadata"] = metadata
            for field_name in ("outreach_layer", "outreach_layer_key", "outreach_layer_source"):
                field_value = layer_payload.get(field_name)
                if field_value not in (None, "") and record.get(field_name) in (None, ""):
                    record[field_name] = field_value
        return context

    def _attach_outreach_layering_metadata(
        self,
        candidates: list[Candidate],
        context: dict[str, Any],
    ) -> list[Candidate]:
        layer_map = dict(context.get("layer_map") or {})
        if not layer_map:
            return candidates
        for candidate in candidates:
            layer_payload = dict(layer_map.get(candidate.candidate_id) or {})
            if not layer_payload:
                continue
            metadata = dict(candidate.metadata or {})
            metadata.update(layer_payload)
            candidate.metadata = metadata
        return candidates

    def _run_retrieval_job(
        self,
        *,
        job_id: str = "",
        request_payload: dict[str, Any],
        plan_payload: dict[str, Any],
        job_type: str,
        criteria_artifacts: dict[str, Any] | None = None,
        runtime_policy: dict[str, Any] | None = None,
        event_detail: str = "Synchronous retrieval job started.",
    ) -> dict[str, Any]:
        request = JobRequest.from_payload(request_payload)
        job_id = job_id or uuid.uuid4().hex[:12]
        self.store.save_job(
            job_id=job_id,
            job_type=job_type,
            status="running",
            stage="retrieving",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
        )
        self.store.append_job_event(job_id, "retrieving", "running", event_detail)
        try:
            with self.agent_runtime.traced_lane(
                job_id=job_id,
                request=request,
                plan_payload=_plan_payload(plan_payload),
                runtime_mode=job_type,
                lane_id="retrieval_specialist",
                span_name="retrieval_execution",
                stage="retrieving",
                input_payload={"runtime_policy": runtime_policy},
                handoff_from_lane="triage_planner",
            ) as retrieval_span:
                if request.target_company.strip().lower() == "anthropic" and _should_use_local_anthropic_assets(
                    request
                ):
                    bootstrap_summary = self.ensure_bootstrapped()
                    if bootstrap_summary:
                        self.store.append_job_event(
                            job_id,
                            "acquiring",
                            "completed",
                            "Loaded existing local Anthropic asset snapshot.",
                            bootstrap_summary,
                        )
                artifact = self._execute_retrieval(
                    job_id,
                    request,
                    plan_payload,
                    job_type=job_type,
                    runtime_policy=runtime_policy,
                )
                self.agent_runtime.complete_span(
                    retrieval_span,
                    status="completed",
                    output_payload={
                        "summary": artifact.get("summary") or {},
                        "returned_matches": len(artifact.get("matches") or []),
                    },
                    handoff_to_lane="review_specialist",
                )
            policy_run = self.store.record_confidence_policy_run(
                target_company=request.target_company,
                job_id=job_id,
                criteria_version_id=int((criteria_artifacts or {}).get("criteria_version_id") or 0),
                trigger_feedback_id=int((criteria_artifacts or {}).get("trigger_feedback_id") or 0),
                policy_payload=dict(artifact.get("confidence_policy") or {}),
            )
            artifact["confidence_policy_run_id"] = policy_run["policy_run_id"]
            if criteria_artifacts:
                artifact.update(criteria_artifacts)
            self.store.update_agent_runtime_session_status(job_id, "completed")
            self.store.append_job_event(job_id, "completed", "completed", "Retrieval job completed.")
            return artifact
        except Exception as exc:
            failure_summary = {"error": str(exc)}
            self.store.save_job(
                job_id=job_id,
                job_type=job_type,
                status="failed",
                stage="failed",
                request_payload=request.to_record(),
                plan_payload=plan_payload,
                summary_payload=failure_summary,
            )
            self.store.update_agent_runtime_session_status(job_id, "failed")
            self.store.append_job_event(job_id, "failed", "failed", str(exc))
            raise

    def _rerun_after_recompile_if_requested(
        self,
        payload: dict[str, Any],
        feedback: dict[str, Any],
        recompile: dict[str, Any],
    ) -> dict[str, Any]:
        if not payload.get("rerun_retrieval"):
            return {"status": "not_requested"}
        if recompile.get("status") != "recompiled":
            return {"status": "skipped", "reason": "Criteria were not successfully recompiled."}

        request_payload = dict(recompile.get("request") or {})
        override_payload = payload.get("rerun_request_overrides") or {}
        if isinstance(override_payload, dict) and override_payload:
            request_payload.update(override_payload)
        plan_payload = dict(recompile.get("plan") or {})
        target_company = str(request_payload.get("target_company") or payload.get("target_company") or "").strip()
        baseline_job_id = str(payload.get("job_id") or payload.get("baseline_job_id") or "").strip()
        baseline_job: dict[str, Any] | None = None
        baseline_selection = {
            "selected_via": "none",
            "reason": "No baseline job has been selected yet.",
            "request_signature": request_signature(request_payload),
            "request_family_signature": request_family_signature(request_payload),
        }
        if not baseline_job_id and target_company:
            baseline_job = self.store.find_best_completed_job_match(
                target_company=target_company,
                request_payload=request_payload,
            )
            baseline_job_id = str((baseline_job or {}).get("job_id") or "")
        elif baseline_job_id:
            baseline_job = self.store.get_job(baseline_job_id)
            baseline_selection = {
                "selected_via": "explicit_job_id",
                "reason": "Baseline job was explicitly provided by the caller.",
                "request_signature": request_signature(request_payload),
                "request_family_signature": request_family_signature(request_payload),
                "matched_request_signature": request_signature((baseline_job or {}).get("request") or {}),
                "matched_request_family_signature": request_family_signature((baseline_job or {}).get("request") or {}),
                "family_score": 100.0 if baseline_job else 0.0,
                "exact_request_match": True if baseline_job else False,
                "exact_family_match": True if baseline_job else False,
                "reasons": ["explicit_job_id"],
            }
        if baseline_job and baseline_job.get("baseline_match"):
            baseline_selection = {
                **dict(baseline_job.get("baseline_match") or {}),
                "reason": baseline_selection_reason(dict(baseline_job.get("baseline_match") or {})),
            }
        baseline_results = self.store.get_job_results(baseline_job_id) if baseline_job_id else []
        policy = decide_rerun_policy(
            payload,
            feedback=feedback,
            recompile=recompile,
            baseline_job=baseline_job,
        )
        if policy.get("status") != "approved":
            return {
                "status": policy.get("status") or "skipped",
                "baseline_job_id": baseline_job_id,
                "baseline_selection": baseline_selection,
                "policy": policy,
            }

        effective_overrides = dict(policy.get("effective_request_overrides") or {})
        if effective_overrides:
            request_payload.update(effective_overrides)

        criteria_artifacts = {
            "criteria_version_id": recompile.get("criteria_version_id", 0),
            "criteria_compiler_run_id": recompile.get("criteria_compiler_run_id", 0),
            "criteria_request_signature": recompile.get("criteria_request_signature", ""),
            "trigger_feedback_id": int(feedback.get("feedback_id") or 0),
        }
        rerun_artifact = self._run_retrieval_job(
            request_payload=request_payload,
            plan_payload=plan_payload,
            job_type="retrieval_rerun",
            criteria_artifacts=criteria_artifacts,
            runtime_policy=dict(policy.get("runtime_policy") or {}),
            event_detail="Retrieval rerun started after criteria feedback recompile.",
        )
        rerun_results = self.store.get_job_results(rerun_artifact["job_id"])
        baseline_version_id = int(recompile.get("base_version_id") or 0)
        rerun_version_id = int(recompile.get("criteria_version_id") or 0)
        baseline_version = self.store.get_criteria_version(baseline_version_id)
        rerun_version = self.store.get_criteria_version(rerun_version_id)
        trigger_feedback = {
            "feedback_id": int(feedback.get("feedback_id") or 0),
            "feedback_type": str(payload.get("feedback_type") or feedback.get("feedback_type") or ""),
            "subject": str(payload.get("subject") or feedback.get("subject") or ""),
            "value": str(payload.get("value") or feedback.get("value") or ""),
        }
        diff_payload = build_result_diff(
            baseline_results,
            rerun_results,
            baseline_job_id=baseline_job_id,
            rerun_job_id=rerun_artifact["job_id"],
            baseline_version=baseline_version,
            rerun_version=rerun_version,
            trigger_feedback=trigger_feedback,
        )
        diff_artifact = {
            "target_company": target_company,
            "trigger_feedback_id": int(feedback.get("feedback_id") or 0),
            "criteria_version_id": int(recompile.get("criteria_version_id") or 0),
            "baseline_job_id": baseline_job_id,
            "rerun_job_id": rerun_artifact["job_id"],
            "diff": diff_payload,
            "policy": policy,
            "baseline_selection": baseline_selection,
        }
        diff_artifact_path = (
            self.jobs_dir / f"criteria_diff_{baseline_job_id or 'none'}_{rerun_artifact['job_id']}.json"
        )
        diff_artifact_path.write_text(json.dumps(diff_artifact, ensure_ascii=False, indent=2))
        stored = self.store.record_criteria_result_diff(
            target_company=target_company,
            trigger_feedback_id=int(feedback.get("feedback_id") or 0),
            criteria_version_id=int(recompile.get("criteria_version_id") or 0),
            baseline_job_id=baseline_job_id,
            rerun_job_id=rerun_artifact["job_id"],
            summary_payload=diff_payload["summary"],
            diff_payload=diff_payload,
            artifact_path=str(diff_artifact_path),
        )
        return {
            "status": "completed",
            "baseline_job_id": baseline_job_id,
            "rerun_job_id": rerun_artifact["job_id"],
            "baseline_selection": baseline_selection,
            "policy": policy,
            "diff_id": stored["diff_id"],
            "diff_artifact_path": str(diff_artifact_path),
            "diff": diff_payload,
            "rerun_result": rerun_artifact,
        }

    def _persist_criteria_artifacts(
        self,
        *,
        request: JobRequest,
        plan_payload: dict[str, Any],
        source_kind: str,
        compiler_kind: str,
        job_id: str,
    ) -> dict[str, Any]:
        patterns = self.store.list_criteria_patterns(target_company=request.target_company, status="")
        version = self.store.create_criteria_version(
            target_company=request.target_company,
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            patterns=patterns,
            source_kind=source_kind,
            evolution_stage="planned",
            notes="Planning artifacts persisted for auditability and future criteria evolution.",
        )
        compiler_run = self.store.record_criteria_compiler_run(
            version_id=version["version_id"],
            job_id=job_id,
            provider_name=self.model_client.provider_name(),
            compiler_kind=compiler_kind,
            status="completed",
            input_payload=request.to_record(),
            output_payload=plan_payload,
            notes="Compiled from current request, patterns, and deterministic/model-assisted planning logic.",
        )
        return {
            "criteria_version_id": version["version_id"],
            "criteria_compiler_run_id": compiler_run["compiler_run_id"],
            "criteria_request_signature": version["request_signature"],
        }

    def _suggest_patterns_from_feedback(self, feedback_id: int) -> list[dict[str, Any]]:
        feedback = self.store.get_criteria_feedback(feedback_id)
        if feedback is None:
            return []
        candidate_id = str(feedback.get("candidate_id") or "").strip()
        job_id = str(feedback.get("job_id") or "").strip()
        candidate = self.store.get_candidate(candidate_id) if candidate_id else None
        job_result = None
        if job_id and candidate_id:
            for item in self.store.get_job_results(job_id):
                if str(item.get("candidate_id") or "") == candidate_id:
                    job_result = item
                    break
        existing_patterns = self.store.list_criteria_patterns(
            target_company=str(feedback.get("target_company") or ""),
            status="",
            limit=500,
        )
        suggestions = derive_pattern_suggestions(
            feedback=feedback,
            candidate=candidate,
            job_result=job_result,
            existing_patterns=existing_patterns,
        )
        return self.store.record_pattern_suggestions(suggestions)


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = " ".join(str(value or "").split()).strip()
        if text:
            return text
    return ""


def _target_candidate_export_path_looks_like_profile(value: str) -> bool:
    normalized = str(value or "").strip().lower().replace("\\", "/")
    if not normalized:
        return False
    return any(
        marker in normalized
        for marker in (
            "/profiles/",
            "/harvest_profiles/",
            "linkedin_profile",
            "profile_registry",
            "profile_capture",
        )
    )


def _target_candidate_archive_slug_from_linkedin(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    lowered = normalized.lower()
    if "linkedin.com/in/" in lowered:
        normalized = lowered.split("linkedin.com/in/", 1)[-1].split("?", 1)[0].split("#", 1)[0]
    return normalized.strip("/ ")


def _target_candidate_archive_name_component(value: str, *, fallback: str) -> str:
    collapsed = " ".join(str(value or "").split()).strip()
    if not collapsed:
        return fallback
    safe_chars = [
        character
        if character.isalnum() or character in {"-", "_"}
        else "_"
        for character in collapsed
    ]
    normalized = "".join(safe_chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    normalized = normalized.strip("._-")
    return normalized[:96] or fallback


def _plan_payload(plan: Any) -> dict[str, Any]:
    if hasattr(plan, "to_record"):
        return plan.to_record()
    if isinstance(plan, dict):
        return dict(plan)
    return {}


def _plan_retrieval_strategy(plan: Any) -> str:
    if hasattr(plan, "retrieval_plan"):
        retrieval_plan = getattr(plan, "retrieval_plan")
        strategy = getattr(retrieval_plan, "strategy", "")
        if strategy:
            return str(strategy)
    if isinstance(plan, dict):
        retrieval_plan = plan.get("retrieval_plan") or {}
        if isinstance(retrieval_plan, dict):
            strategy = retrieval_plan.get("strategy")
            if strategy:
                return str(strategy)
    return "hybrid"


def _plan_semantic_fields(plan: Any) -> list[str]:
    if hasattr(plan, "retrieval_plan"):
        retrieval_plan = getattr(plan, "retrieval_plan")
        fields = getattr(retrieval_plan, "semantic_fields", None)
        if isinstance(fields, list) and fields:
            return [str(item) for item in fields if str(item).strip()]
    if isinstance(plan, dict):
        retrieval_plan = plan.get("retrieval_plan") or {}
        if isinstance(retrieval_plan, dict):
            fields = retrieval_plan.get("semantic_fields") or []
            if isinstance(fields, list) and fields:
                return [str(item) for item in fields if str(item).strip()]
    return ["role", "team", "focus_areas", "derived_facets", "education", "work_history", "notes"]


def _build_effective_retrieval_request(request: JobRequest, plan: Any) -> JobRequest:
    plan_payload = _plan_payload(plan)
    request_patch: dict[str, Any] = {}
    for key in (
        "categories",
        "employment_statuses",
        "must_have_facets",
        "must_have_primary_role_buckets",
        "organization_keywords",
        "must_have_keywords",
        "exclude_keywords",
    ):
        if _has_non_empty_list(getattr(request, key, [])):
            continue
        values = _plan_filter_values(plan_payload, key)
        if values:
            request_patch[key] = values
    execution_keywords = _merge_unique_string_values(
        getattr(request, "keywords", []),
        dict(dict(plan_payload.get("acquisition_strategy") or {}).get("filter_hints") or {}).get("keywords") or [],
    )
    if execution_keywords and execution_keywords != list(getattr(request, "keywords", []) or []):
        request_patch["keywords"] = execution_keywords
    if not request_patch:
        return request
    return JobRequest.from_payload(_merge_request_payload(request.to_record(), request_patch))


def _plan_filter_values(plan_payload: dict[str, Any], key: str) -> list[str]:
    retrieval_payload = dict(plan_payload.get("retrieval_plan") or {})
    structured_filters = list(retrieval_payload.get("structured_filters") or [])
    values = _merge_unique_string_values(
        *[_parse_filter_expression(item, key) for item in structured_filters],
        _parse_filter_expression(str(plan_payload.get("criteria_summary") or ""), key, multi_segment=True),
    )
    return values


def _parse_filter_expression(raw_value: str, key: str, *, multi_segment: bool = False) -> list[str]:
    expressions = [raw_value]
    if multi_segment:
        expressions = [segment.strip() for segment in str(raw_value or "").split(";") if segment.strip()]
    prefix = f"{key}="
    for expression in expressions:
        normalized = str(expression or "").strip()
        if not normalized.startswith(prefix):
            continue
        literal_text = normalized[len(prefix) :].strip()
        try:
            parsed = ast.literal_eval(literal_text)
        except (SyntaxError, ValueError):
            parsed = literal_text
        if isinstance(parsed, str):
            return _merge_unique_string_values([parsed])
        if isinstance(parsed, (list, tuple, set)):
            return _merge_unique_string_values(parsed)
    return []


def _effective_request_overrides(request: JobRequest, effective_request: JobRequest) -> dict[str, list[str]]:
    overrides: dict[str, list[str]] = {}
    for key in (
        "categories",
        "employment_statuses",
        "must_have_facets",
        "must_have_primary_role_buckets",
        "organization_keywords",
        "must_have_keywords",
        "exclude_keywords",
    ):
        original_values = list(getattr(request, key, []) or [])
        effective_values = list(getattr(effective_request, key, []) or [])
        if _merge_unique_string_values(original_values) == _merge_unique_string_values(effective_values):
            continue
        if effective_values:
            overrides[key] = effective_values
    return overrides


def _request_uses_default_full_employment_scope(request: JobRequest) -> bool:
    normalized = {
        str(item or "").strip().lower() for item in list(request.employment_statuses or []) if str(item or "").strip()
    }
    return normalized in (set(), {"current", "former"})


def _lane_for_task(task: Any, plan_payload: dict[str, Any]) -> str:
    strategy_type = str((task.metadata if hasattr(task, "metadata") else {}).get("strategy_type") or "")
    search_strategy = dict(plan_payload.get("search_strategy") or {})
    source_families = {
        str(item.get("source_family") or "").strip()
        for item in search_strategy.get("query_bundles") or []
        if isinstance(item, dict)
    }
    if strategy_type == "investor_firm_roster":
        return "investor_graph_specialist"
    if task.task_type == "enrich_profiles_multisource":
        return "enrichment_specialist"
    if "public_interviews" in source_families and task.task_type == "acquire_full_roster":
        return "public_media_specialist"
    return "acquisition_specialist"


def _criteria_like_version_from_job(job: dict[str, Any], *, patterns: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version_id": 0,
        "request": dict(job.get("request") or {}),
        "plan": dict(job.get("plan") or {}),
        "patterns": list(patterns or []),
    }


def _merge_request_payload(
    base_payload: dict[str, Any],
    patch_payload: dict[str, Any],
    *,
    explicit_execution_preferences: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(base_payload or {})
    initial_target_company = str(patch_payload.get("target_company") or merged.get("target_company") or "").strip()
    expanded_axes_patch = _shared_expand_request_intent_axes_patch(
        patch_payload,
        target_company=initial_target_company,
    )
    effective_patch: dict[str, Any] = dict(expanded_axes_patch)
    for key, value in dict(patch_payload or {}).items():
        if key == "intent_axes":
            continue
        if _has_non_empty_value(value) or _has_non_empty_list(value) or _has_non_empty_dict(value):
            effective_patch[key] = value
    target_company = str(effective_patch.get("target_company") or merged.get("target_company") or "").strip()
    base_execution_preferences = normalize_execution_preferences(
        merged, target_company=str(merged.get("target_company") or "")
    )
    patch_execution_preferences = normalize_execution_preferences(
        effective_patch,
        target_company=target_company,
    )
    merged_execution_preferences = merge_execution_preferences(patch_execution_preferences, base_execution_preferences)
    if explicit_execution_preferences:
        merged_execution_preferences = merge_execution_preferences(
            explicit_execution_preferences, merged_execution_preferences
        )
    if merged_execution_preferences:
        merged["execution_preferences"] = merged_execution_preferences
    for key in [
        "target_company",
        "target_scope",
        "query",
    ]:
        candidate = effective_patch.get(key)
        if _has_non_empty_value(candidate):
            merged[key] = candidate
    target_company = str(merged.get("target_company") or target_company or "").strip()
    if not _has_non_empty_value(merged.get("retrieval_strategy")):
        candidate_strategy = str(effective_patch.get("retrieval_strategy") or "").strip().lower()
        if candidate_strategy in {"structured", "hybrid", "semantic"}:
            merged["retrieval_strategy"] = candidate_strategy
    else:
        candidate_strategy = str(effective_patch.get("retrieval_strategy") or "").strip().lower()
        if candidate_strategy in {"structured", "hybrid", "semantic"}:
            merged["retrieval_strategy"] = candidate_strategy
    for key in [
        "categories",
        "employment_statuses",
    ]:
        candidate = effective_patch.get(key)
        if _has_non_empty_list(candidate) and isinstance(candidate, (list, tuple, set)):
            merged[key] = list(candidate)
    merged["keywords"] = _merge_unique_request_string_values(
        merged.get("keywords"),
        effective_patch.get("keywords"),
        effective_patch.get("research_direction_keywords"),
        effective_patch.get("technology_keywords"),
        effective_patch.get("model_keywords"),
        effective_patch.get("product_keywords"),
        target_company=target_company,
    )
    merged["must_have_keywords"] = _merge_unique_request_string_values(
        merged.get("must_have_keywords"),
        effective_patch.get("must_have_keywords"),
        target_company=target_company,
    )
    merged["organization_keywords"] = _merge_unique_request_string_values(
        merged.get("organization_keywords"),
        effective_patch.get("organization_keywords"),
        effective_patch.get("team_keywords"),
        effective_patch.get("sub_org_keywords"),
        effective_patch.get("project_keywords"),
        target_company=target_company,
    )
    merged["must_have_facets"] = _merge_unique_request_string_values(
        merged.get("must_have_facets"),
        effective_patch.get("must_have_facets"),
        target_company=target_company,
    )
    merged["must_have_primary_role_buckets"] = _merge_unique_request_string_values(
        merged.get("must_have_primary_role_buckets"),
        effective_patch.get("must_have_primary_role_buckets"),
        target_company=target_company,
    )
    merged_scope = merged.get("scope_disambiguation")
    patch_scope = effective_patch.get("scope_disambiguation")
    if isinstance(patch_scope, dict) and patch_scope:
        patch_scope_dict = dict(patch_scope)
        if not isinstance(merged_scope, dict) or not merged_scope:
            merged["scope_disambiguation"] = patch_scope_dict
        else:
            merged_scope_dict = dict(patch_scope_dict)
            for key, value in dict(merged_scope).items():
                if key not in merged_scope_dict:
                    merged_scope_dict[key] = value
            merged["scope_disambiguation"] = merged_scope_dict
    return merged


def _apply_plan_review_execution_overrides(
    reviewed_request_payload: dict[str, Any],
    execution_payload: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(reviewed_request_payload or {})
    payload = dict(execution_payload or {})
    if not payload:
        return merged

    target_company = str(merged.get("target_company") or payload.get("target_company") or "").strip()
    explicit_execution_preferences = normalize_execution_preferences(payload, target_company=target_company)
    if explicit_execution_preferences:
        current_execution_preferences = normalize_execution_preferences(merged, target_company=target_company)
        merged["execution_preferences"] = merge_execution_preferences(
            explicit_execution_preferences,
            current_execution_preferences,
        )

    for key in [
        "asset_view",
        "target_scope",
        "retrieval_strategy",
        "planning_mode",
        "analysis_stage_mode",
    ]:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            merged[key] = value

    for key in [
        "categories",
        "employment_statuses",
        "keywords",
        "must_have_facets",
        "must_have_primary_role_buckets",
        "must_have_keywords",
        "exclude_keywords",
        "organization_keywords",
    ]:
        value = payload.get(key)
        if isinstance(value, list) and any(str(item or "").strip() for item in value):
            merged[key] = list(value)

    for key in [
        "semantic_rerank_limit",
        "top_k",
        "slug_resolution_limit",
        "profile_detail_limit",
        "publication_scan_limit",
        "publication_lead_limit",
        "exploration_limit",
        "scholar_coauthor_follow_up_limit",
    ]:
        if key in payload and payload.get(key) is not None:
            merged[key] = payload.get(key)

    scope_disambiguation = payload.get("scope_disambiguation")
    if isinstance(scope_disambiguation, dict) and scope_disambiguation:
        merged["scope_disambiguation"] = dict(scope_disambiguation)
    return merged


def _infer_target_company_from_review_payloads(
    *,
    request_payload: dict[str, Any],
    plan_payload: dict[str, Any],
    decision_payload: dict[str, Any],
) -> str:
    existing = str(request_payload.get("target_company") or "").strip()
    if existing:
        return existing

    candidates: list[str] = []
    for key in ("target_company", "company", "canonical_company", "company_name"):
        value = str(decision_payload.get(key) or "").strip()
        if value:
            candidates.append(value)

    scope_disambiguation = decision_payload.get("scope_disambiguation")
    if isinstance(scope_disambiguation, dict):
        for key in ("target_company", "canonical_company", "parent_company"):
            value = str(scope_disambiguation.get(key) or "").strip()
            if value:
                candidates.append(value)

    for scope_key in ("confirmed_company_scope", "confirmed_scope", "company_scope"):
        raw_scope = decision_payload.get(scope_key)
        if raw_scope is None:
            continue
        if isinstance(raw_scope, str):
            scope_items = [raw_scope]
        else:
            scope_items = list(raw_scope)
        for item in scope_items:
            value = str(item or "").strip()
            if value:
                candidates.append(value)

    acquisition_strategy = dict(plan_payload.get("acquisition_strategy") or {})
    for item in list(acquisition_strategy.get("company_scope") or []):
        value = str(item).strip()
        if value:
            candidates.append(value)
    filter_hints = dict(acquisition_strategy.get("filter_hints") or {})
    for item in list(filter_hints.get("current_companies") or []):
        value = str(item).strip()
        if value:
            candidates.append(value)

    seen: set[str] = set()
    for candidate in candidates:
        normalized = candidate.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        return normalized
    return ""


def _plan_review_session_api_summary(session: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(session or {})
    if not payload:
        return {}
    return {
        "review_id": int(payload.get("review_id") or 0),
        "target_company": str(payload.get("target_company") or ""),
        "status": str(payload.get("status") or ""),
        "risk_level": str(payload.get("risk_level") or ""),
        "required_before_execution": bool(payload.get("required_before_execution")),
        "reviewer": str(payload.get("reviewer") or ""),
        "approved_at": str(payload.get("approved_at") or ""),
        "created_at": str(payload.get("created_at") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
    }


def _job_api_summary(job: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(job or {})
    if not payload:
        return {}
    return {
        "job_id": str(payload.get("job_id") or ""),
        "job_type": str(payload.get("job_type") or ""),
        "status": str(payload.get("status") or ""),
        "stage": str(payload.get("stage") or ""),
        "created_at": str(payload.get("created_at") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
    }


def _frontend_plan_semantics_metadata(payload: dict[str, Any] | None) -> dict[str, Any]:
    source = dict(payload or {})
    planning = dict(source.get("planning") or {})
    metadata: dict[str, Any] = {}
    for key in (
        "request_preview",
        "dispatch_preview",
        "organization_execution_profile",
        "asset_reuse_plan",
        "lane_preview",
        "effective_execution_semantics",
    ):
        value = source.get(key)
        if not isinstance(value, dict) or not value:
            value = planning.get(key)
        if isinstance(value, dict) and value:
            metadata[key] = dict(value)
    return metadata


def _merge_nested_payloads(base: dict[str, Any] | None, patch: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(base or {})
    for key, value in dict(patch or {}).items():
        if (
            isinstance(value, dict)
            and isinstance(merged.get(key), dict)
        ):
            merged[key] = _merge_nested_payloads(
                dict(merged.get(key) or {}),
                dict(value or {}),
            )
            continue
        merged[key] = value
    return merged


def _ensure_plan_company_scope(plan_payload: dict[str, Any], target_company: str) -> dict[str, Any]:
    normalized_target = str(target_company or "").strip()
    if not normalized_target:
        return dict(plan_payload or {})

    updated_plan = dict(plan_payload or {})
    updated_plan["target_company"] = normalized_target
    acquisition_strategy = dict(updated_plan.get("acquisition_strategy") or {})
    company_scope = [
        str(item).strip() for item in list(acquisition_strategy.get("company_scope") or []) if str(item).strip()
    ]
    if not company_scope:
        company_scope = [normalized_target]
    elif not any(item.lower() == normalized_target.lower() for item in company_scope):
        company_scope = [normalized_target, *company_scope]
    acquisition_strategy["company_scope"] = company_scope

    filter_hints = dict(acquisition_strategy.get("filter_hints") or {})
    current_companies = [
        str(item).strip() for item in list(filter_hints.get("current_companies") or []) if str(item).strip()
    ]
    if not current_companies:
        current_companies = [normalized_target]
    elif not any(item.lower() == normalized_target.lower() for item in current_companies):
        current_companies = [normalized_target, *current_companies]
    filter_hints["current_companies"] = current_companies
    acquisition_strategy["filter_hints"] = filter_hints

    updated_plan["acquisition_strategy"] = acquisition_strategy

    acquisition_tasks = list(updated_plan.get("acquisition_tasks") or [])
    for task in acquisition_tasks:
        if not isinstance(task, dict):
            continue
        status = str(task.get("status") or "").strip()
        if status == "needs_input":
            task["status"] = "ready"
    updated_plan["acquisition_tasks"] = acquisition_tasks

    return updated_plan


def _build_job_progress_payload(
    *,
    job: dict[str, Any],
    events: list[dict[str, Any]],
    event_summary: dict[str, Any] | None = None,
    workers: list[dict[str, Any]],
    result_count: int,
    manual_review_count: int,
    runtime_controls: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job_summary = dict(job.get("summary") or {})
    materialized_event_summary = dict(event_summary or {})
    milestones = (
        _job_stage_milestones_from_summary(
            materialized_event_summary,
            current_stage=str(job.get("stage") or ""),
            job_status=str(job.get("status") or ""),
        )
        if materialized_event_summary
        else _job_stage_milestones(
            events,
            current_stage=str(job.get("stage") or ""),
            job_status=str(job.get("status") or ""),
        )
    )
    worker_summary = _job_worker_summary(workers)
    latest_event = dict(materialized_event_summary.get("latest_event") or (events[-1] if events else {}))
    started_at = str(job.get("created_at") or "")
    updated_at = str(job.get("updated_at") or "")
    runtime_controls_payload = dict(runtime_controls or {})
    runtime_health = _classify_job_runtime_health(
        job=job,
        workers=workers,
        worker_summary=worker_summary,
        runtime_controls=runtime_controls_payload,
        blocked_task=str(job_summary.get("blocked_task") or ""),
    )
    progress_payload = {
        "stage_order": [item["stage"] for item in milestones],
        "current_stage": str(job.get("stage") or ""),
        "completed_stages": [item["stage"] for item in milestones if item["status"] == "completed"],
        "milestones": milestones,
        "timing": {
            "started_at": started_at,
            "updated_at": updated_at,
            "elapsed_seconds": _elapsed_seconds(started_at, updated_at),
        },
        "latest_event": latest_event,
        "worker_summary": worker_summary,
        "latest_metrics": _extract_progress_metrics(
            job_summary,
            events,
            result_count=int(result_count or 0),
            manual_review_count=int(manual_review_count or 0),
            precomputed_event_metrics=dict(materialized_event_summary.get("latest_metrics") or {}),
        ),
        "runtime_health": runtime_health,
        "counters": {
            "event_count": int(materialized_event_summary.get("event_count") or len(events)),
            "worker_count": len(workers),
            "completed_worker_count": int(worker_summary["by_status"].get("completed") or 0),
            "running_worker_count": int(worker_summary["by_status"].get("running") or 0),
            "queued_worker_count": int(worker_summary["by_status"].get("queued") or 0)
            + int(worker_summary["by_status"].get("waiting_remote_search") or 0)
            + int(worker_summary["by_status"].get("waiting_remote_harvest") or 0),
            "waiting_remote_search_count": int(worker_summary["by_status"].get("waiting_remote_search") or 0),
            "waiting_remote_harvest_count": int(worker_summary["by_status"].get("waiting_remote_harvest") or 0),
            "blocked_worker_count": int(worker_summary["by_status"].get("blocked") or 0),
            "failed_worker_count": int(worker_summary["by_status"].get("failed") or 0),
            "result_count": int(result_count or 0),
            "manual_review_count": int(manual_review_count or 0),
        },
    }
    if runtime_controls_payload:
        progress_payload["runtime_controls"] = runtime_controls_payload
    return {
        "job_id": str(job.get("job_id") or ""),
        "status": str(job.get("status") or ""),
        "stage": str(job.get("stage") or ""),
        "started_at": started_at,
        "updated_at": updated_at,
        "elapsed_seconds": _elapsed_seconds(started_at, updated_at),
        "blocked_task": str(job_summary.get("blocked_task") or ""),
        "awaiting_user_action": str(job_summary.get("awaiting_user_action") or ""),
        "current_message": str(job_summary.get("message") or latest_event.get("detail") or ""),
        "progress": progress_payload,
    }


def _job_stage_milestones_from_summary(
    event_summary: dict[str, Any],
    *,
    current_stage: str = "",
    job_status: str = "",
) -> list[dict[str, Any]]:
    ordered_stages = ["planning", "acquiring", "retrieving", "completed", "failed"]
    stage_sequence = [
        str(item).strip() for item in list(event_summary.get("stage_sequence") or []) if str(item).strip()
    ]
    stage_stats = {
        str(stage).strip(): dict(stats or {})
        for stage, stats in dict(event_summary.get("stage_stats") or {}).items()
        if str(stage).strip()
    }
    stage_order = [stage for stage in ordered_stages if stage in stage_stats or stage in stage_sequence]
    for stage in stage_sequence:
        if stage not in stage_order and stage in stage_stats:
            stage_order.append(stage)
    for stage in stage_stats:
        if stage not in stage_order:
            stage_order.append(stage)
    normalized_current_stage = str(current_stage or "").strip()
    normalized_job_status = str(job_status or "").strip()
    stage_position = {stage: index for index, stage in enumerate(stage_order)}
    current_position = stage_position.get(normalized_current_stage, len(stage_order))
    milestones: list[dict[str, Any]] = []
    for stage in stage_order:
        stats = dict(stage_stats.get(stage) or {})
        if not stats:
            continue
        completed_at = str(stats.get("completed_at") or "")
        milestone_status = str(stats.get("latest_status") or "")
        if normalized_current_stage:
            stage_pos = stage_position.get(stage, len(stage_order))
            if stage == normalized_current_stage and normalized_job_status not in {"completed", "failed"}:
                milestone_status = normalized_job_status or "running"
                completed_at = ""
            elif stage_pos < current_position:
                milestone_status = "completed"
            elif stage_pos > current_position and milestone_status in {"completed", "failed"}:
                completed_at = str(stats.get("latest_at") or completed_at)
        milestones.append(
            {
                "stage": stage,
                "status": milestone_status,
                "started_at": str(stats.get("started_at") or ""),
                "completed_at": completed_at,
                "latest_detail": str(stats.get("latest_detail") or ""),
                "latest_payload": dict(stats.get("latest_payload") or {}),
            }
        )
    return milestones


def _job_stage_milestones(
    events: list[dict[str, Any]],
    *,
    current_stage: str = "",
    job_status: str = "",
) -> list[dict[str, Any]]:
    ordered_stages = ["planning", "acquiring", "retrieving", "completed", "failed"]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        stage = str(event.get("stage") or "").strip()
        if not stage or stage in {"runtime_control", "runtime_heartbeat"}:
            continue
        grouped[stage].append(event)
    stage_order = [stage for stage in ordered_stages if stage in grouped]
    for stage in grouped:
        if stage not in stage_order:
            stage_order.append(stage)
    normalized_current_stage = str(current_stage or "").strip()
    normalized_job_status = str(job_status or "").strip()
    stage_position = {stage: index for index, stage in enumerate(stage_order)}
    current_position = stage_position.get(normalized_current_stage, len(stage_order))
    milestones: list[dict[str, Any]] = []
    for stage in stage_order:
        stage_events = grouped.get(stage) or []
        if not stage_events:
            continue
        started_at = str(stage_events[0].get("created_at") or "")
        completed_at = ""
        for event in reversed(stage_events):
            if str(event.get("status") or "") in {"completed", "failed"}:
                completed_at = str(event.get("created_at") or "")
                break
        latest = stage_events[-1]
        milestone_status = str(latest.get("status") or "")
        if normalized_current_stage:
            stage_pos = stage_position.get(stage, len(stage_order))
            if stage == normalized_current_stage and normalized_job_status not in {"completed", "failed"}:
                milestone_status = normalized_job_status or "running"
                completed_at = ""
            elif stage_pos < current_position:
                milestone_status = "completed"
            elif stage_pos > current_position and milestone_status in {"completed", "failed"}:
                completed_at = str(latest.get("created_at") or "")
        milestones.append(
            {
                "stage": stage,
                "status": milestone_status,
                "started_at": started_at,
                "completed_at": completed_at,
                "latest_detail": str(latest.get("detail") or ""),
                "event_count": len(stage_events),
                "elapsed_seconds": _elapsed_seconds(started_at, str(latest.get("created_at") or "")),
            }
        )
    return milestones


def _job_worker_summary(workers: list[dict[str, Any]]) -> dict[str, Any]:
    by_status = Counter(effective_worker_status(worker) for worker in workers)
    raw_by_status = Counter(str(worker.get("status") or "unknown") for worker in workers)
    lanes: dict[str, dict[str, Any]] = {}
    for worker in workers:
        lane_id = str(worker.get("lane_id") or "unknown")
        lane = lanes.setdefault(
            lane_id,
            {
                "lane_id": lane_id,
                "worker_count": 0,
                "by_status": Counter(),
                "raw_by_status": Counter(),
                "last_updated_at": "",
            },
        )
        lane["worker_count"] += 1
        status = effective_worker_status(worker)
        lane["by_status"][status] += 1
        lane["raw_by_status"][str(worker.get("status") or "unknown")] += 1
        updated_at = str(worker.get("updated_at") or "")
        if updated_at and updated_at > str(lane.get("last_updated_at") or ""):
            lane["last_updated_at"] = updated_at
    return {
        "by_status": dict(by_status),
        "raw_by_status": dict(raw_by_status),
        "by_lane": [
            {
                **lane,
                "by_status": dict(lane["by_status"]),
                "raw_by_status": dict(lane["raw_by_status"]),
            }
            for lane in sorted(lanes.values(), key=lambda item: str(item.get("lane_id") or ""))
        ],
    }


def _build_runtime_control_change_events(
    *,
    previous_controls: dict[str, Any],
    current_controls: dict[str, Any],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for control_key in (
        "hosted_runtime_watchdog",
        "shared_recovery",
        "job_recovery",
        "workflow_runner",
        "workflow_runner_control",
    ):
        previous_snapshot = _runtime_control_event_snapshot(control_key, previous_controls.get(control_key))
        current_snapshot = _runtime_control_event_snapshot(control_key, current_controls.get(control_key))
        if not current_snapshot or current_snapshot == previous_snapshot:
            continue
        payload: dict[str, Any] = {
            "control": control_key,
            "snapshot": current_snapshot,
        }
        if previous_snapshot:
            payload["previous_snapshot"] = previous_snapshot
        events.append(
            {
                "status": str(current_snapshot.get("status") or "updated"),
                "detail": _describe_runtime_control_change(control_key, current_snapshot, previous_snapshot),
                "payload": payload,
            }
        )
    return events


def _runtime_control_event_snapshot(control_key: str, value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        return {}
    snapshot: dict[str, Any] = {}
    for key in ("status", "scope", "mode", "service_name", "pid", "job_id", "log_path"):
        raw = value.get(key)
        if raw in (None, "", [], {}):
            continue
        snapshot[key] = raw
    handshake = value.get("handshake")
    if isinstance(handshake, dict) and handshake:
        handshake_snapshot: dict[str, Any] = {}
        for key in ("status", "pid", "job_status", "job_stage"):
            raw = handshake.get(key)
            if raw in (None, "", [], {}):
                continue
            handshake_snapshot[key] = raw
        service_status = handshake.get("service_status")
        if isinstance(service_status, dict) and service_status:
            service_snapshot = {
                key: service_status.get(key)
                for key in ("status", "lock_status", "service_name")
                if service_status.get(key) not in (None, "", [], {})
            }
            if service_snapshot:
                handshake_snapshot["service_status"] = service_snapshot
        if handshake_snapshot:
            snapshot["handshake"] = handshake_snapshot
    attempts = value.get("attempts")
    if isinstance(attempts, list) and attempts:
        snapshot["attempt_count"] = len(attempts)
    takeover = value.get("takeover")
    if isinstance(takeover, dict) and takeover:
        snapshot["takeover_status"] = str(takeover.get("status") or "")
    fallback_from = value.get("fallback_from")
    if isinstance(fallback_from, dict) and fallback_from:
        snapshot["fallback_from_status"] = str(fallback_from.get("status") or "")
    return snapshot


def _describe_runtime_control_change(
    control_key: str,
    current_snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any],
) -> str:
    label = {
        "hosted_runtime_watchdog": "Hosted runtime watchdog",
        "shared_recovery": "Shared recovery",
        "job_recovery": "Job-scoped recovery",
        "workflow_runner": "Workflow runner",
        "workflow_runner_control": "Workflow runner control",
    }.get(control_key, control_key.replace("_", " "))
    status = str(current_snapshot.get("status") or "updated")
    details: list[str] = []
    mode = str(current_snapshot.get("mode") or "").strip()
    if mode:
        details.append(f"mode={mode}")
    service_name = str(current_snapshot.get("service_name") or "").strip()
    if service_name:
        details.append(f"service={service_name}")
    pid = int(current_snapshot.get("pid") or 0)
    if pid > 0:
        details.append(f"pid={pid}")
    handshake = dict(current_snapshot.get("handshake") or {})
    handshake_status = str(handshake.get("status") or "").strip()
    if handshake_status:
        details.append(f"handshake={handshake_status}")
    if previous_snapshot:
        previous_status = str(previous_snapshot.get("status") or "").strip()
        if previous_status and previous_status != status:
            details.append(f"previous_status={previous_status}")
    suffix = f" ({', '.join(details)})" if details else ""
    return f"{label} updated: {status}{suffix}."


def _active_hosted_dispatch_marker(payload: Any) -> dict[str, Any]:
    marker = dict(payload or {}) if isinstance(payload, dict) else {}
    status = str(marker.get("status") or "").strip().lower()
    if not marker or status in {"", "failed", "cleared"}:
        return {}
    dispatched_at = _parse_timestamp(str(marker.get("dispatched_at") or ""))
    if dispatched_at is None:
        return {}
    grace_seconds = max(5, int(_env_int("WORKFLOW_HOSTED_DISPATCH_GRACE_SECONDS", 20)))
    age_seconds = max(int((datetime.now(timezone.utc) - dispatched_at).total_seconds()), 0)
    if age_seconds >= grace_seconds:
        return {}
    return {
        **marker,
        "age_seconds": age_seconds,
        "grace_seconds": grace_seconds,
    }


def _normalize_service_name_list(value: Any) -> list[str]:
    if isinstance(value, str):
        value = [value]
    return _dedupe_texts(value)


def _dedupe_texts(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = " ".join(str(item or "").split()).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _classify_job_runtime_health(
    *,
    job: dict[str, Any],
    workers: list[dict[str, Any]],
    worker_summary: dict[str, Any],
    runtime_controls: dict[str, Any],
    blocked_task: str,
) -> dict[str, Any]:
    status = str(job.get("status") or "").strip().lower()
    stage = str(job.get("stage") or "").strip().lower()
    blocked_task_value = str(blocked_task or "").strip()
    runner = dict(runtime_controls.get("workflow_runner") or {})
    runner_control = dict(runtime_controls.get("workflow_runner_control") or {})
    workflow_job_lease = dict(runtime_controls.get("workflow_job_lease") or {})
    hosted_runtime_watchdog = dict(runtime_controls.get("hosted_runtime_watchdog") or {})
    shared_recovery = dict(runtime_controls.get("shared_recovery") or {})
    job_recovery = dict(runtime_controls.get("job_recovery") or {})
    runner_status = str(runner_control.get("status") or "").strip().lower()
    runner_pid = int(runner.get("pid") or 0)
    explicit_runner_present = bool(runner)
    lease_alive = bool(workflow_job_lease.get("lease_alive"))
    runner_alive = (
        bool(runner.get("process_alive"))
        if "process_alive" in runner
        else (_workflow_runner_process_alive(runner_pid) if runner_pid > 0 else False)
    )
    if not explicit_runner_present:
        runner_alive = runner_alive or lease_alive
    runner_attempted = (
        bool(runner)
        or bool(runner_control)
        or bool(workflow_job_lease)
        or runner_status
        in {
            "started",
            "started_deferred",
            "failed",
        }
    )
    runner_known_present = bool(runner) or bool(runner_control) or bool(workflow_job_lease)
    runtime_idle_seconds = _job_runtime_idle_seconds(job)
    stale_runner_threshold_seconds = max(
        30,
        int(_env_int("WORKFLOW_AUTO_RESUME_STALE_SECONDS", 60)),
    )
    blocking_workers = _progress_blocking_workers(workers, blocked_task=blocked_task_value)
    waiting_remote_search_count = int(worker_summary.get("by_status", {}).get("waiting_remote_search") or 0)
    waiting_remote_harvest_count = int(worker_summary.get("by_status", {}).get("waiting_remote_harvest") or 0)
    remote_wait_count = waiting_remote_search_count + waiting_remote_harvest_count
    active_worker_count = (
        int(worker_summary.get("by_status", {}).get("running") or 0)
        + int(worker_summary.get("by_status", {}).get("queued") or 0)
        + waiting_remote_search_count
        + waiting_remote_harvest_count
        + int(worker_summary.get("by_status", {}).get("blocked") or 0)
    )

    if status == "completed":
        return {
            "state": "terminal",
            "classification": "completed",
            "detail": "Workflow completed.",
        }
    if status == "failed":
        return {
            "state": "terminal",
            "classification": "failed",
            "detail": "Workflow failed.",
        }
    if str(runner_control.get("status") or "").strip().lower() == "failed":
        return {
            "state": "stalled",
            "classification": "runner_failed_to_start",
            "detail": "Workflow runner failed to start and takeover fallback was used.",
        }
    hosted_runtime_watchdog_ready = (not _recovery_control_requires_ready(hosted_runtime_watchdog)) or bool(
        hosted_runtime_watchdog.get("service_ready")
    )
    shared_recovery_ready = (not _recovery_control_requires_ready(shared_recovery)) or bool(
        shared_recovery.get("service_ready")
    )
    job_recovery_ready = (not _recovery_control_requires_ready(job_recovery)) or bool(job_recovery.get("service_ready"))
    recovery_ready = hosted_runtime_watchdog_ready or shared_recovery_ready or job_recovery_ready
    if runner_known_present and not runner_alive and status in {"queued", "running", "blocked"}:
        if remote_wait_count > 0 and recovery_ready:
            return {
                "state": "progressing",
                "classification": "waiting_on_remote_provider",
                "detail": (
                    "Workflow runner exited, but remote acquisition jobs are still in flight and recovery daemons "
                    "can reconcile them."
                ),
                "waiting_remote_search_count": waiting_remote_search_count,
                "waiting_remote_harvest_count": waiting_remote_harvest_count,
                "pending_worker_count": len(blocking_workers),
            }
        if blocking_workers and recovery_ready:
            return {
                "state": "progressing",
                "classification": "runner_takeover_pending",
                "detail": (
                    "Workflow runner exited while acquisition workers are still active; recovery daemons should "
                    "take over and resume automatically."
                ),
                "blocked_task": blocked_task_value,
                "pending_worker_count": len(blocking_workers),
            }
        return {
            "state": "stalled",
            "classification": "runner_not_alive",
            "detail": "Workflow runner is no longer alive while the workflow is still in progress.",
        }
    if status == "blocked":
        awaiting_user_action = str(dict(job.get("summary") or {}).get("awaiting_user_action") or "").strip()
        if stage == "retrieving" and awaiting_user_action == "continue_stage2":
            transition_state = (
                str(dict(job.get("summary") or {}).get("stage2_transition_state") or "").strip() or "idle"
            )
            return {
                "state": "progressing",
                "classification": "awaiting_stage2_confirmation",
                "detail": (
                    "Stage 1 preview is ready and the workflow is waiting for explicit user approval "
                    "before running stage 2 AI analysis."
                ),
                "awaiting_user_action": awaiting_user_action,
                "stage2_transition_state": transition_state,
            }
        if blocking_workers:
            return {
                "state": "stalled",
                "classification": "blocked_on_acquisition_workers",
                "detail": f"Workflow is blocked on acquisition task `{blocked_task_value or 'unknown'}` with {len(blocking_workers)} pending worker(s).",
                "blocked_task": blocked_task_value,
                "pending_worker_count": len(blocking_workers),
            }
        return {
            "state": "stalled",
            "classification": "blocked_ready_for_resume",
            "detail": f"Workflow is blocked on acquisition task `{blocked_task_value or 'unknown'}` but no blocking workers remain.",
            "blocked_task": blocked_task_value,
            "pending_worker_count": 0,
        }
    if status == "queued":
        active_hosted_dispatch = _active_hosted_dispatch_marker(dict(job.get("summary") or {}).get("hosted_dispatch"))
        if active_hosted_dispatch:
            return {
                "state": "progressing",
                "classification": "hosted_dispatch_inflight",
                "detail": "Workflow was recently dispatched to hosted execution and is still within the runner grace window.",
                "hosted_dispatch": active_hosted_dispatch,
            }
        return {
            "state": "progressing",
            "classification": "queued_waiting_for_runner",
            "detail": "Workflow is queued and waiting for runner takeover.",
        }
    if status == "running" and stage == "retrieving":
        return {
            "state": "progressing",
            "classification": "retrieval_running",
            "detail": "Retrieval stage is running.",
        }
    if status == "running" and stage == "acquiring" and blocking_workers:
        return {
            "state": "progressing",
            "classification": "acquisition_workers_running",
            "detail": f"Acquisition is still waiting on {len(blocking_workers)} blocking worker(s) for `{blocked_task_value or 'acquisition'}`.",
            "blocked_task": blocked_task_value,
            "pending_worker_count": len(blocking_workers),
        }
    if status == "running" and active_worker_count > 0:
        return {
            "state": "progressing",
            "classification": "workers_running",
            "detail": f"Workflow has {active_worker_count} active worker(s).",
            "active_worker_count": active_worker_count,
        }
    if (
        status == "running"
        and stage == "acquiring"
        and not runner_alive
        and not runner_attempted
        and active_worker_count <= 0
        and runtime_idle_seconds >= stale_runner_threshold_seconds
    ):
        return {
            "state": "stalled",
            "classification": "runner_not_alive",
            "detail": (
                "Workflow acquisition has been idle without an observable runner or active workers; "
                "recovery takeover is safe."
            ),
            "idle_seconds": runtime_idle_seconds,
        }
    if (
        not runner_alive
        and _recovery_control_requires_ready(shared_recovery)
        and not bool(shared_recovery.get("service_ready"))
        and not hosted_runtime_watchdog_ready
        and (remote_wait_count > 0 or len(blocking_workers) > 0)
    ):
        return {
            "state": "stalled",
            "classification": "shared_recovery_not_ready",
            "detail": "Shared recovery daemon is not ready yet.",
        }
    if (
        not runner_alive
        and _recovery_control_requires_ready(job_recovery)
        and not bool(job_recovery.get("service_ready"))
        and not hosted_runtime_watchdog_ready
        and (remote_wait_count > 0 or len(blocking_workers) > 0)
    ):
        return {
            "state": "stalled",
            "classification": "job_recovery_not_ready",
            "detail": "Job-scoped recovery daemon is not ready yet.",
        }
    return {
        "state": "progressing",
        "classification": "healthy_running",
        "detail": "Workflow is running normally.",
    }


def _recovery_control_requires_ready(control: dict[str, Any]) -> bool:
    if not control:
        return False
    status = str(control.get("status") or "").strip().lower()
    return status not in {"", "disabled", "not_needed", "failed_to_start"}


def _progress_blocking_workers(workers: list[dict[str, Any]], *, blocked_task: str) -> list[dict[str, Any]]:
    if not blocked_task:
        return []
    pending_workers: list[dict[str, Any]] = []
    for worker in workers:
        if not _worker_blocks_acquisition_resume(worker, blocked_task=blocked_task):
            continue
        pending_workers.append(worker)
    return pending_workers


def _dispatch_scope_matches_job(
    job_payload: dict[str, Any],
    *,
    scope: str,
    requester_id: str = "",
    tenant_id: str = "",
) -> bool:
    normalized_scope = str(scope or "").strip().lower()
    if normalized_scope not in {"global", "tenant", "requester"}:
        normalized_scope = "tenant" if tenant_id else ("requester" if requester_id else "global")
    if normalized_scope == "global":
        return True
    if normalized_scope == "tenant":
        return bool(tenant_id and str(job_payload.get("tenant_id") or "").strip() == str(tenant_id).strip())
    if normalized_scope == "requester":
        return bool(requester_id and str(job_payload.get("requester_id") or "").strip() == str(requester_id).strip())
    return False


def _summarize_worker_runtime(worker: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(worker.get("metadata") or {})
    return {
        "worker_id": int(worker.get("worker_id") or 0),
        "lane_id": str(worker.get("lane_id") or ""),
        "worker_key": str(worker.get("worker_key") or ""),
        "status": str(worker.get("status") or ""),
        "recovery_kind": str(metadata.get("recovery_kind") or ""),
    }


def _collect_active_acquisition_workers(workers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active_workers: list[dict[str, Any]] = []
    for worker in workers:
        status = str(worker.get("status") or "").strip().lower()
        if status in {"completed", "failed", "interrupted", "cancelled", "canceled"}:
            continue
        if str(worker.get("lane_id") or "").strip() == "exploration_specialist":
            continue
        active_workers.append(worker)
    return active_workers


def _worker_requires_blocking_acquisition_completion(worker: dict[str, Any]) -> bool:
    lane_id = str(worker.get("lane_id") or "").strip()
    metadata = dict(worker.get("metadata") or {})
    recovery_kind = str(metadata.get("recovery_kind") or "").strip().lower()
    if lane_id == "acquisition_specialist":
        return recovery_kind in {"", "harvest_company_employees"}
    return False


def _worker_is_background_company_roster_harvest(worker: dict[str, Any]) -> bool:
    lane_id = str(worker.get("lane_id") or "").strip()
    if lane_id != "acquisition_specialist":
        return False
    metadata = dict(worker.get("metadata") or {})
    recovery_kind = str(metadata.get("recovery_kind") or "").strip().lower()
    return recovery_kind == "harvest_company_employees"


def _can_resume_full_roster_with_background_harvest_workers(
    *,
    blocked_task: str,
    baseline_ready: bool,
    critical_pending_workers: list[dict[str, Any]],
) -> bool:
    if str(blocked_task or "").strip() != "acquire_full_roster":
        return False
    if not baseline_ready or not critical_pending_workers:
        return False
    return all(_worker_is_background_company_roster_harvest(worker) for worker in critical_pending_workers)


def _acquisition_state_is_baseline_ready(state: dict[str, Any]) -> tuple[bool, str]:
    candidate_doc_path = state.get("candidate_doc_path")
    if isinstance(candidate_doc_path, Path) and candidate_doc_path.exists():
        return True, "candidate_documents_ready"
    manifest_path = state.get("manifest_path")
    if isinstance(manifest_path, Path) and manifest_path.exists():
        return True, "manifest_ready"
    if list(state.get("candidates") or []):
        return True, "candidate_records_present"
    search_seed_snapshot = state.get("search_seed_snapshot")
    if isinstance(search_seed_snapshot, SearchSeedSnapshot) and list(search_seed_snapshot.entries or []):
        return True, "search_seed_entries_present"
    roster_snapshot = state.get("roster_snapshot")
    if isinstance(roster_snapshot, CompanyRosterSnapshot) and (
        list(roster_snapshot.visible_entries or []) or list(roster_snapshot.raw_entries or [])
    ):
        return True, "roster_snapshot_present"
    return False, ""


def _acquisition_state_has_roster_snapshot(state: dict[str, Any]) -> bool:
    roster_snapshot = state.get("roster_snapshot")
    return isinstance(roster_snapshot, CompanyRosterSnapshot) and bool(
        list(roster_snapshot.visible_entries or []) or list(roster_snapshot.raw_entries or [])
    )


def _blocked_task_requires_roster_baseline(plan: Any, blocked_task: str) -> bool:
    if str(blocked_task or "").strip() != "acquire_full_roster":
        return False
    for task in list(getattr(plan, "acquisition_tasks", []) or []):
        if not isinstance(task, AcquisitionTask):
            continue
        if task.task_type != "acquire_full_roster":
            continue
        if str(task.metadata.get("strategy_type") or "").strip() == "full_company_roster":
            return True
    return False


def _elapsed_seconds(started_at: str, ended_at: str) -> int:
    if not started_at or not ended_at:
        return 0
    start_dt = _parse_timestamp(started_at)
    end_dt = _parse_timestamp(ended_at)
    if start_dt is None or end_dt is None:
        return 0
    return max(int((end_dt - start_dt).total_seconds()), 0)


def _earliest_timestamp_string(*values: str) -> str:
    earliest_value = ""
    earliest_dt: datetime | None = None
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        parsed = _parse_timestamp(raw)
        if parsed is None:
            continue
        if earliest_dt is None or parsed < earliest_dt:
            earliest_dt = parsed
            earliest_value = parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return earliest_value


def _latest_timestamp_string(*values: str) -> str:
    latest_value = ""
    latest_dt: datetime | None = None
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        parsed = _parse_timestamp(raw)
        if parsed is None:
            continue
        if latest_dt is None or parsed > latest_dt:
            latest_dt = parsed
            latest_value = parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    return latest_value


def _job_runtime_idle_seconds(job: dict[str, Any]) -> int:
    updated_at = str(job.get("updated_at") or "").strip()
    updated_dt = _parse_timestamp(updated_at)
    if updated_dt is None:
        return 0
    return max(int((datetime.now(timezone.utc) - updated_dt).total_seconds()), 0)


def _workflow_job_lease_heartbeat_age_seconds(lease: dict[str, Any]) -> int | None:
    updated_at = str(lease.get("updated_at") or lease.get("created_at") or "").strip()
    updated_dt = _parse_timestamp(updated_at)
    if updated_dt is None:
        return None
    return max(int((datetime.now(timezone.utc) - updated_dt).total_seconds()), 0)


def _workflow_job_lease_is_alive(lease: dict[str, Any]) -> bool:
    if not isinstance(lease, dict) or not lease:
        return False
    if bool(lease.get("expired")):
        return False
    heartbeat_age_seconds = _workflow_job_lease_heartbeat_age_seconds(lease)
    if heartbeat_age_seconds is None:
        return False
    stale_after_seconds = max(
        45,
        int(_env_int("WORKFLOW_JOB_LEASE_HEARTBEAT_STALE_SECONDS", 90)),
    )
    return heartbeat_age_seconds <= stale_after_seconds


def _parse_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


def _milliseconds_between_iso(start_value: str, end_value: str) -> int | None:
    start = _parse_timestamp(start_value)
    end = _parse_timestamp(end_value)
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


_PLAN_HYDRATION_SIGNATURE_IGNORED_KEYS = frozenset(
    {
        "history_id",
        "frontend_history_id",
        "plan_request_id",
        "request_id",
        "idempotency_key",
        "queued_at",
        "submitted_at",
        "started_at",
        "completed_at",
    }
)


def _normalize_plan_hydration_signature_value(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in sorted(value.items(), key=lambda item: str(item[0])):
            key = str(raw_key or "").strip()
            if not key or key in _PLAN_HYDRATION_SIGNATURE_IGNORED_KEYS:
                continue
            normalized[key] = _normalize_plan_hydration_signature_value(raw_value)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_plan_hydration_signature_value(item) for item in value]
    if isinstance(value, set):
        return sorted(_normalize_plan_hydration_signature_value(item) for item in value)
    if isinstance(value, Path):
        return str(value)
    return value


def _plan_hydration_request_signature(payload: dict[str, Any] | None) -> str:
    normalized = _normalize_plan_hydration_signature_value(dict(payload or {}))
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _serialize_api_timestamp(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = _parse_timestamp(raw)
    if parsed is None:
        return raw
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _workflow_job_is_newer(candidate: dict[str, Any], baseline: dict[str, Any]) -> bool:
    candidate_created_at = _parse_timestamp(str(candidate.get("created_at") or ""))
    baseline_created_at = _parse_timestamp(str(baseline.get("created_at") or ""))
    if candidate_created_at is not None and baseline_created_at is not None:
        if candidate_created_at > baseline_created_at:
            return True
        if candidate_created_at < baseline_created_at:
            return False
    candidate_updated_at = _parse_timestamp(str(candidate.get("updated_at") or ""))
    baseline_updated_at = _parse_timestamp(str(baseline.get("updated_at") or ""))
    if candidate_updated_at is not None and baseline_updated_at is not None:
        return candidate_updated_at > baseline_updated_at
    return False


def _normalize_acquisition_progress_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        return {}
    progress = dict(payload)
    normalized_ids = []
    seen_ids: set[str] = set()
    for item in list(progress.get("completed_task_ids") or []):
        task_id = str(item or "").strip()
        if not task_id or task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        normalized_ids.append(task_id)
    progress["completed_task_ids"] = normalized_ids
    progress["tasks"] = dict(progress.get("tasks") or {})
    progress["phases"] = dict(progress.get("phases") or {})
    progress["latest_state"] = dict(progress.get("latest_state") or {})
    if not (
        progress["completed_task_ids"]
        or progress["tasks"]
        or progress["phases"]
        or progress["latest_state"]
        or str(progress.get("status") or "").strip()
        or str(progress.get("last_completed_task_id") or "").strip()
        or str(progress.get("last_completed_task_type") or "").strip()
    ):
        return {}
    return progress


def _serialize_acquisition_state_payload(state: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    snapshot_id = str(state.get("snapshot_id") or "").strip()
    if snapshot_id:
        payload["snapshot_id"] = snapshot_id
    for path_key in (
        "snapshot_dir",
        "candidate_doc_path",
        "manifest_path",
        "linkedin_stage_candidate_doc_path",
        "public_web_stage_candidate_doc_path",
    ):
        value = state.get(path_key)
        if isinstance(value, Path):
            payload[path_key] = str(value)
    identity = state.get("company_identity")
    if isinstance(identity, CompanyIdentity):
        payload["company_identity"] = identity.to_record()
    roster_snapshot = state.get("roster_snapshot")
    if isinstance(roster_snapshot, CompanyRosterSnapshot):
        payload["roster_snapshot"] = roster_snapshot.to_record()
    search_seed_snapshot = state.get("search_seed_snapshot")
    if isinstance(search_seed_snapshot, SearchSeedSnapshot):
        payload["search_seed_snapshot"] = search_seed_snapshot.to_record()
    for dict_key in ("normalization_scope", "investor_firm_plan", "background_reconcile_cursor"):
        value = state.get(dict_key)
        if isinstance(value, dict) and value:
            payload[dict_key] = dict(value)
    for bool_key in (
        "linkedin_stage_completed",
        "public_web_stage_completed",
        "reused_snapshot_checkpoint",
        "allow_search_seed_baseline_for_full_roster",
    ):
        if bool_key in state:
            payload[bool_key] = bool(state.get(bool_key))
    candidate_doc_path = payload.get("candidate_doc_path") or ""
    if not candidate_doc_path:
        candidates = state.get("candidates")
        if isinstance(candidates, list) and candidates:
            payload["candidates"] = [
                item.to_record() if isinstance(item, Candidate) else dict(item or {})
                for item in candidates
                if isinstance(item, Candidate) or isinstance(item, dict)
            ]
        evidence = state.get("evidence")
        if isinstance(evidence, list) and evidence:
            payload["evidence"] = [
                item.to_record() if isinstance(item, EvidenceRecord) else dict(item or {})
                for item in evidence
                if isinstance(item, EvidenceRecord) or isinstance(item, dict)
            ]
    return payload


def _deserialize_acquisition_state_payload(payload: dict[str, Any]) -> dict[str, Any]:
    restored: dict[str, Any] = {}
    snapshot_id = str(payload.get("snapshot_id") or "").strip()
    if snapshot_id:
        restored["snapshot_id"] = snapshot_id
    for path_key in (
        "snapshot_dir",
        "candidate_doc_path",
        "manifest_path",
        "linkedin_stage_candidate_doc_path",
        "public_web_stage_candidate_doc_path",
    ):
        path_value = str(payload.get(path_key) or "").strip()
        if path_value:
            restored[path_key] = Path(path_value).expanduser()
    company_identity_payload = dict(payload.get("company_identity") or {})
    identity = _company_identity_from_record(company_identity_payload)
    if identity is not None:
        restored["company_identity"] = identity
    roster_snapshot = _restore_company_roster_snapshot(dict(payload.get("roster_snapshot") or {}))
    if roster_snapshot is not None:
        restored["roster_snapshot"] = roster_snapshot
    search_seed_snapshot = _restore_search_seed_snapshot(dict(payload.get("search_seed_snapshot") or {}))
    if search_seed_snapshot is not None:
        restored["search_seed_snapshot"] = search_seed_snapshot
    for dict_key in ("normalization_scope", "investor_firm_plan", "background_reconcile_cursor"):
        mapping_value = payload.get(dict_key)
        if isinstance(mapping_value, dict) and mapping_value:
            restored[dict_key] = dict(mapping_value)
    for bool_key in (
        "linkedin_stage_completed",
        "public_web_stage_completed",
        "reused_snapshot_checkpoint",
        "allow_search_seed_baseline_for_full_roster",
    ):
        if bool_key in payload:
            restored[bool_key] = bool(payload.get(bool_key))
    candidates = _candidate_records_from_payload(payload.get("candidates"))
    if candidates:
        restored["candidates"] = candidates
    evidence = _evidence_records_from_payload(payload.get("evidence"))
    if evidence:
        restored["evidence"] = evidence
    return restored


def _restore_company_roster_snapshot(payload: dict[str, Any]) -> CompanyRosterSnapshot | None:
    if not payload:
        return None
    identity = _company_identity_from_record(dict(payload.get("company_identity") or {}))
    snapshot_dir_value = str(payload.get("snapshot_dir") or "").strip()
    merged_path_value = str(payload.get("merged_path") or "").strip()
    visible_path_value = str(payload.get("visible_path") or "").strip()
    headless_path_value = str(payload.get("headless_path") or "").strip()
    summary_path_value = str(payload.get("summary_path") or "").strip()
    if (
        identity is None
        or not snapshot_dir_value
        or not merged_path_value
        or not visible_path_value
        or not headless_path_value
        or not summary_path_value
    ):
        return None
    snapshot_dir = Path(snapshot_dir_value).expanduser()
    merged_path = Path(merged_path_value).expanduser()
    visible_path = Path(visible_path_value).expanduser()
    headless_path = Path(headless_path_value).expanduser()
    summary_path = Path(summary_path_value).expanduser()
    if not merged_path.exists() or not visible_path.exists() or not headless_path.exists():
        return None
    summary_payload = _read_json_dict(summary_path)
    raw_entries = _read_json_list(merged_path)
    visible_entries = _read_json_list(visible_path)
    headless_entries = _read_json_list(headless_path)
    return CompanyRosterSnapshot(
        snapshot_id=str(payload.get("snapshot_id") or snapshot_dir.name),
        target_company=str(payload.get("target_company") or identity.canonical_name),
        company_identity=identity,
        snapshot_dir=snapshot_dir,
        raw_entries=raw_entries,
        visible_entries=visible_entries,
        headless_entries=headless_entries,
        page_summaries=[
            dict(item) for item in list(summary_payload.get("page_summaries") or []) if isinstance(item, dict)
        ],
        accounts_used=[
            str(item or "")
            for item in list(summary_payload.get("accounts_used") or payload.get("accounts_used") or [])
            if str(item or "").strip()
        ],
        errors=[
            str(item or "")
            for item in list(summary_payload.get("errors") or payload.get("errors") or [])
            if str(item or "").strip()
        ],
        stop_reason=str(summary_payload.get("stop_reason") or payload.get("stop_reason") or ""),
        merged_path=merged_path,
        visible_path=visible_path,
        headless_path=headless_path,
        summary_path=summary_path,
    )


def _restore_search_seed_snapshot(payload: dict[str, Any]) -> SearchSeedSnapshot | None:
    if not payload:
        return None
    identity = _company_identity_from_record(dict(payload.get("company_identity") or {}))
    snapshot_dir_value = str(payload.get("snapshot_dir") or "").strip()
    summary_path_value = str(payload.get("summary_path") or "").strip()
    if identity is None or not snapshot_dir_value or not summary_path_value:
        return None
    snapshot_dir = Path(snapshot_dir_value).expanduser()
    summary_path = Path(summary_path_value).expanduser()
    entries_path_value = str(payload.get("entries_path") or "").strip()
    entries_path = Path(entries_path_value).expanduser() if entries_path_value else summary_path.parent / "entries.json"
    if not entries_path.exists():
        return None
    summary_payload = _read_json_dict(summary_path)
    entries = _read_json_list(entries_path)
    return SearchSeedSnapshot(
        snapshot_id=str(payload.get("snapshot_id") or snapshot_dir.name),
        target_company=str(payload.get("target_company") or identity.canonical_name),
        company_identity=identity,
        snapshot_dir=snapshot_dir,
        entries=entries,
        query_summaries=[
            dict(item) for item in list(summary_payload.get("query_summaries") or []) if isinstance(item, dict)
        ],
        accounts_used=[
            str(item or "")
            for item in list(summary_payload.get("accounts_used") or payload.get("accounts_used") or [])
            if str(item or "").strip()
        ],
        errors=[
            str(item or "")
            for item in list(summary_payload.get("errors") or payload.get("errors") or [])
            if str(item or "").strip()
        ],
        stop_reason=str(summary_payload.get("stop_reason") or payload.get("stop_reason") or ""),
        summary_path=summary_path,
        entries_path=entries_path,
    )


def _restore_search_seed_snapshot_from_snapshot_dir(
    *,
    snapshot_dir: Path,
    identity: Any,
) -> SearchSeedSnapshot | None:
    if not isinstance(snapshot_dir, Path):
        return None
    effective_identity = identity if isinstance(identity, CompanyIdentity) else None
    if effective_identity is None:
        effective_identity = _company_identity_from_record(_read_json_dict(snapshot_dir / "identity.json"))
    if effective_identity is None:
        return None
    summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
    entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
    if not summary_path.exists() or not entries_path.exists():
        return None
    return _restore_search_seed_snapshot(
        {
            "snapshot_id": snapshot_dir.name,
            "target_company": effective_identity.canonical_name,
            "company_identity": effective_identity.to_record(),
            "snapshot_dir": str(snapshot_dir),
            "summary_path": str(summary_path),
            "entries_path": str(entries_path),
        }
    )


def _canonicalize_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _shared_canonicalize_request_payload(payload)


def _build_intent_rewrite_payload(
    *,
    request_payload: dict[str, Any] | None = None,
    instruction: str | None = None,
) -> dict[str, Any]:
    payload = {
        "request": _build_single_intent_rewrite_entry(_request_intent_text(request_payload)),
        "policy_catalog": list_business_rewrite_policy_catalog(),
    }
    if instruction is not None:
        payload["instruction"] = _build_single_intent_rewrite_entry(instruction)
    return payload


def _request_intent_text(request_payload: dict[str, Any] | None) -> str:
    payload = dict(request_payload or {})
    return str(payload.get("raw_user_request") or payload.get("query") or "").strip()


def _build_single_intent_rewrite_entry(text: str) -> dict[str, Any]:
    normalized = " ".join(str(text or "").strip().split())
    rewrite = interpret_query_intent_rewrite(normalized)
    summary = summarize_query_intent_rewrite(normalized)
    return {
        "matched": bool(rewrite),
        "summary": summary,
        "rewrite": rewrite,
    }


def _resolve_outreach_layer_filter_policy(request: JobRequest) -> dict[str, Any]:
    globally_enabled = _env_bool("OUTREACH_LAYERING_FILTER_ENABLED", True)
    force_all_queries = _env_bool("OUTREACH_LAYERING_FILTER_ALL_QUERIES", False)
    rewrite = interpret_query_intent_rewrite(
        " ".join(
            item
            for item in [
                str(request.raw_user_request or "").strip(),
                str(request.query or "").strip(),
            ]
            if item
        )
    )
    intent_triggered = _request_targets_greater_china_outreach(request, rewrite)
    enabled = globally_enabled and (force_all_queries or intent_triggered)
    min_layer = max(0, min(3, _env_int("OUTREACH_LAYERING_DEFAULT_MIN_LAYER", 2)))
    fallback_min_layer = max(0, min(3, _env_int("OUTREACH_LAYERING_FALLBACK_MIN_LAYER", 1)))
    if fallback_min_layer >= min_layer:
        fallback_min_layer = 0
    if not enabled or min_layer <= 0:
        return {
            "enabled": False,
            "scope": "all_queries_default" if force_all_queries else "greater_china_intent_only",
            "min_layer": min_layer,
            "fallback_min_layer": fallback_min_layer,
        }
    return {
        "enabled": True,
        "scope": "all_queries_default" if force_all_queries else "greater_china_intent_only",
        "min_layer": min_layer,
        "fallback_min_layer": fallback_min_layer,
    }


def _request_targets_greater_china_outreach(request: JobRequest, rewrite: dict[str, Any] | None = None) -> bool:
    normalized_keywords = {
        str(item or "").strip().lower() for item in list(request.keywords or []) if str(item or "").strip()
    }
    normalized_facets = {
        str(item or "").strip().lower() for item in list(request.must_have_facets or []) if str(item or "").strip()
    }
    normalized_query = " ".join(
        item
        for item in [
            str(request.raw_user_request or "").strip(),
            str(request.query or "").strip(),
        ]
        if item
    ).lower()
    if "greater china experience" in normalized_keywords or "chinese bilingual outreach" in normalized_keywords:
        return True
    if {
        "greater_china_region_experience",
        "mainland_china_experience_or_chinese_language",
    } & normalized_facets:
        return True
    if any(token in normalized_query for token in ["华人", "泛华人", "chinese member", "chinese members"]):
        return True
    rewrite_payload = dict(rewrite or {})
    rewrite_id = str(rewrite_payload.get("rewrite_id") or "").strip()
    if rewrite_id == "greater_china_outreach":
        return True
    for item in list(rewrite_payload.get("additional_rewrites") or []):
        if str((item or {}).get("rewrite_id") or "").strip() == "greater_china_outreach":
            return True
    return False


def _supplement_request_query_signals(
    payload: dict[str, Any],
    *,
    raw_text: str,
    include_raw_keyword_extraction: bool = True,
) -> dict[str, Any]:
    return _shared_supplement_request_query_signals(
        payload,
        raw_text=raw_text,
        include_raw_keyword_extraction=include_raw_keyword_extraction,
    )


def _extract_query_signal_terms(raw_text: str, *, target_company: str) -> dict[str, list[str]]:
    return _shared_extract_query_signal_terms(raw_text, target_company=target_company)


def _normalize_request_query_signal(value: str, *, target_company: str) -> str:
    return _shared_normalize_request_query_signal(value, target_company=target_company)


def _should_skip_query_signal(value: str, *, target_company: str) -> bool:
    return _shared_should_skip_query_signal(value, target_company=target_company)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _china_now_iso() -> str:
    return datetime.now(_CHINA_TIME_ZONE).isoformat(timespec="seconds")


def _china_local_filename_timestamp() -> str:
    return datetime.now(_CHINA_TIME_ZONE).strftime("%Y%m%dT%H%M")


def _has_non_empty_value(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _has_non_empty_list(value: Any) -> bool:
    return isinstance(value, list) and any(str(item or "").strip() for item in value)


def _has_non_empty_dict(value: Any) -> bool:
    return isinstance(value, dict) and bool(value)


def _merge_unique_string_values(*sources: Any) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for source in sources:
        if isinstance(source, str):
            items = [source]
        else:
            items = list(source or [])
        for item in items:
            value = " ".join(str(item or "").split()).strip()
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(value)
    return merged


def _merge_unique_request_string_values(*sources: Any, target_company: str = "") -> list[str]:
    return _shared_merge_unique_request_string_values(*sources, target_company=target_company)


def _build_request_preview_payload(
    *,
    request: JobRequest | dict[str, Any],
    effective_request: JobRequest | dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _shared_build_request_preview_payload(request=request, effective_request=effective_request)


def _load_job_request_preview(job: dict[str, Any]) -> dict[str, Any]:
    execution_bundle = dict(job.get("execution_bundle") or {})
    bundle_preview = dict(execution_bundle.get("request_preview") or {})
    if bundle_preview:
        return bundle_preview
    artifact_path = str(job.get("artifact_path") or "").strip()
    if artifact_path:
        artifact_payload = _read_json_dict(Path(artifact_path))
        preview = dict(artifact_payload.get("request_preview") or {})
        if preview:
            return preview
        request_payload = dict(artifact_payload.get("request") or {})
        effective_request_payload = dict(artifact_payload.get("effective_request") or {})
        if request_payload:
            return _build_request_preview_payload(
                request=request_payload,
                effective_request=effective_request_payload or request_payload,
            )
    request_payload = dict(execution_bundle.get("request") or job.get("request") or {})
    if not request_payload:
        return {}
    effective_request_payload = dict(execution_bundle.get("effective_request") or {})
    if effective_request_payload:
        return _build_request_preview_payload(
            request=request_payload,
            effective_request=effective_request_payload,
        )
    plan_payload = dict(job.get("plan") or {})
    if plan_payload:
        try:
            plan = hydrate_sourcing_plan(plan_payload)
        except Exception:
            plan = None
        if plan is not None:
            request = JobRequest.from_payload(request_payload)
            return _build_request_preview_payload(
                request=request,
                effective_request=_build_effective_retrieval_request(request, plan),
            )
    return _build_request_preview_payload(request=request_payload)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(value)


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_optional_int(name: str) -> int | None:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _has_manual_review_resolution_payload(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("apply_resolution") is True:
        return True
    for key in ["source_links", "links", "candidate_patch", "candidate"]:
        value = payload.get(key)
        if isinstance(value, dict) and value:
            return True
        if isinstance(value, list) and value:
            return True
    for key in ["candidate_id", "candidate_name"]:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return True
    return False


def _should_use_local_anthropic_assets(request: JobRequest) -> bool:
    execution_preferences = dict(request.execution_preferences or {})
    if bool(execution_preferences.get("allow_local_bootstrap_fallback")):
        return True
    return not bool(execution_preferences.get("force_fresh_run"))
