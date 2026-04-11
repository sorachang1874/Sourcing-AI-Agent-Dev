from __future__ import annotations

import ast
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import socket
import sys
import threading
import time
from types import SimpleNamespace
import uuid
from typing import Any, Callable

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .asset_catalog import AssetCatalog
from .candidate_artifacts import (
    CandidateArtifactError,
    build_company_candidate_artifacts,
    load_company_snapshot_candidate_documents,
)
from .company_asset_supplement import CompanyAssetSupplementManager
from .connectors import CompanyIdentity, CompanyRosterSnapshot
from .confidence_policy import apply_policy_control, build_confidence_policy
from .criteria_evolution import CriteriaEvolutionEngine
from .domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, derive_candidate_facets, derive_candidate_role_bucket, normalize_candidate, normalize_name_token
from .ingestion import load_bootstrap_bundle
from .manual_review import build_manual_review_items
from .manual_review_resolution import apply_manual_review_resolution
from .manual_review_synthesis import compile_manual_review_synthesis
from .model_provider import DeterministicModelClient, ModelClient
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
    process_alive as _workflow_runner_process_alive,
    read_text_tail as _read_text_tail,
    resolve_timeout as _resolve_timeout,
    service_status_is_ready as _service_status_is_ready,
    spawn_detached_process as _spawn_detached_process,
    wait_for_job_status_transition as _wait_for_job_status_transition,
    wait_for_service_ready as _wait_for_service_ready,
)
from .query_intent_rewrite import interpret_query_intent_rewrite, summarize_query_intent_rewrite
from .recovery_sidecar import (
    build_hosted_runtime_watchdog_command as _build_hosted_runtime_watchdog_command,
    build_hosted_runtime_watchdog_config as _build_hosted_runtime_watchdog_config,
    build_job_scoped_recovery_callback_payload as _build_job_scoped_recovery_callback_payload_impl,
    build_job_scoped_recovery_command as _build_job_scoped_recovery_command,
    build_job_scoped_recovery_config as _build_job_scoped_recovery_config,
    build_recovery_bootstrap_payload as _build_recovery_bootstrap_payload_impl,
    build_shared_recovery_callback_payload as _build_shared_recovery_callback_payload_impl,
    build_shared_recovery_command as _build_shared_recovery_command,
    build_shared_recovery_config as _build_shared_recovery_config,
)
from .request_matching import (
    MATCH_THRESHOLD,
    baseline_selection_reason,
    request_family_score,
    request_family_signature,
    request_signature,
)
from .review_plan_instructions import compile_review_payload_from_instruction
from .result_diff import build_result_diff
from .rerun_policy import decide_rerun_policy
from .scoring import score_candidates
from .outreach_layering import analyze_company_outreach_layers
from .seed_discovery import SearchSeedSnapshot
from .snapshot_materializer import SnapshotMaterializer
from .snapshot_state import (
    candidate_records_from_payload as _candidate_records_from_payload,
    company_identity_from_record as _company_identity_from_record,
    evidence_records_from_payload as _evidence_records_from_payload,
    load_candidate_document_state as _load_candidate_document_state,
    merge_background_reconcile_candidate as _merge_background_reconcile_candidate,
    read_json_dict as _read_json_dict,
    read_json_list as _read_json_list,
)
from .semantic_retrieval import rank_semantic_candidates
from .semantic_provider import SemanticProvider
from .service_daemon import WorkerDaemonService, read_service_status, render_systemd_unit
from .storage import SQLiteStore, _json_safe_payload as _storage_json_safe_payload
from .worker_daemon import PersistentWorkerRecoveryDaemon
from .worker_scheduler import effective_worker_status, summarize_scheduler
from .execution_preferences import merge_execution_preferences, normalize_execution_preferences
from .workflow_refresh import (
    extract_progress_metrics as _extract_progress_metrics,
    extract_refresh_metrics as _extract_refresh_metrics,
    resolve_reconcile_snapshot_dir as _resolve_reconcile_snapshot_dir,
    resolve_reconcile_snapshot_id as _resolve_reconcile_snapshot_id,
    runtime_refresh_metric_subset as _runtime_refresh_metric_subset,
    worker_blocks_acquisition_resume as _worker_blocks_acquisition_resume,
    worker_has_background_candidate_output as _worker_has_background_candidate_output,
    worker_has_completed_background_harvest_prefetch as _worker_has_completed_background_harvest_prefetch,
    worker_has_completed_background_search_output as _worker_has_completed_background_search_output,
)


_OUTREACH_LAYER_KEY_BY_INDEX = {
    0: "layer_0_roster",
    1: "layer_1_name_signal",
    2: "layer_2_greater_china_region_experience",
    3: "layer_3_mainland_china_experience_or_chinese_language",
}

_WORKFLOW_STAGE_SUMMARY_STAGE_ORDER = (
    "linkedin_stage_1",
    "stage_1_preview",
    "public_web_stage_2",
    "stage_2_final",
)


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
        )
        self._dispatch_lock = threading.Lock()
        if hasattr(self.acquisition_engine, "multi_source_enricher"):
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
        return manager.supplement_snapshot(
            target_company=target_company,
            snapshot_id=str(payload.get("snapshot_id") or "").strip(),
            run_former_search_seed=bool(payload.get("run_former_search_seed")),
            former_search_limit=int(payload.get("former_search_limit") or 25),
            former_search_pages=int(payload.get("former_search_pages") or 1),
            former_search_queries=[str(item).strip() for item in list(payload.get("former_search_queries") or []) if str(item).strip()],
            former_filter_hints={
                str(key): [str(item).strip() for item in list(values or []) if str(item).strip()]
                for key, values in dict(payload.get("former_filter_hints") or {}).items()
            },
            profile_scope=str(payload.get("profile_scope") or "none"),
            profile_limit=int(payload.get("profile_limit") or 0),
            profile_only_missing_detail=bool(payload.get("profile_only_missing_detail", False)),
            profile_force_refresh=bool(payload.get("profile_force_refresh", False)),
            repair_current_roster_profile_refs=bool(payload.get("repair_current_roster_profile_refs", False)),
            build_artifacts=bool(payload.get("build_artifacts", True)),
        )

    def plan_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = JobRequest.from_payload(self._prepare_request_payload(payload))
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        plan_review_gate = build_plan_review_gate(request, plan)
        plan_review_gate = self._augment_plan_review_gate_with_runtime_context(request, plan_review_gate)
        plan_review_session = self.store.create_plan_review_session(
            target_company=request.target_company,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            gate_payload=plan_review_gate,
        )
        criteria_artifacts = self._persist_criteria_artifacts(
            request=request,
            plan_payload=plan.to_record(),
            source_kind="plan",
            compiler_kind="planning",
            job_id="",
        )
        return {
            "request": request.to_record(),
            "plan": plan.to_record(),
            "plan_review_gate": plan_review_gate,
            "plan_review_session": plan_review_session,
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
            **criteria_artifacts,
        }

    def start_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        queued = self.queue_workflow(payload)
        if queued.get("status") == "needs_plan_review":
            return queued

        job_id = str(queued.get("job_id") or "")
        workflow_status = str(queued.get("status") or "").strip()
        if not job_id:
            return queued
        dispatch_payload = dict(queued.get("dispatch") or {})
        matched_job_status = str(dispatch_payload.get("matched_job_status") or "").strip()
        should_run_worker = workflow_status == "queued" or (
            workflow_status == "joined_existing_job" and matched_job_status == "queued"
        )
        if should_run_worker:
            queued["shared_recovery"] = self.ensure_shared_recovery(payload)
            queued["job_recovery"] = self.ensure_job_scoped_recovery(job_id, payload)
            worker = threading.Thread(target=self.run_queued_workflow, kwargs={"job_id": job_id}, daemon=True)
            worker.start()
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
            shared_recovery_status if queue_status == "joined_existing_job" else {"status": "not_needed", "scope": "shared"}
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
        dispatch_context = self._build_query_dispatch_context(payload, request)
        with self._dispatch_lock:
            dispatch = self._resolve_query_dispatch_decision(request, dispatch_context)
            strategy = str(dispatch.get("strategy") or "new_job").strip()
            matched_job = dict(dispatch.get("matched_job") or {})
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
                    "requester_id": str(dispatch_context.get("requester_id") or ""),
                    "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                    "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
                }
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
            if strategy == "reuse_snapshot" and matched_job:
                request = self._request_with_snapshot_reuse(request, dispatch)
            job_id = self._create_workflow_job(request, plan, dispatch_context=dispatch_context)
            dispatch_payload = {
                "strategy": strategy if strategy == "reuse_snapshot" else "new_job",
                "scope": str(dispatch.get("scope") or ""),
                "request_signature": str(dispatch.get("request_signature") or ""),
                "request_family_signature": str(dispatch.get("request_family_signature") or ""),
                "matched_job_id": str(matched_job.get("job_id") or ""),
                "matched_job_status": str(matched_job.get("status") or ""),
                "matched_job_stage": str(matched_job.get("stage") or ""),
                "matched_snapshot_id": str(dispatch.get("matched_snapshot_id") or ""),
                "matched_snapshot_dir": str(dispatch.get("matched_snapshot_dir") or ""),
                "matched_snapshot_source_path": str(dispatch.get("matched_snapshot_source_path") or ""),
                "requester_id": str(dispatch_context.get("requester_id") or ""),
                "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
            }
            self.store.record_query_dispatch(
                target_company=request.target_company,
                request_payload=request.to_record(),
                strategy=strategy if strategy == "reuse_snapshot" else "new_job",
                status="queued",
                source_job_id=str(matched_job.get("job_id") or "") if strategy == "reuse_snapshot" else "",
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
        job = self.store.get_job(job_id)
        if job is None:
            return {"job_id": job_id, "status": "not_found"}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": job_id, "status": "skipped", "reason": "not_workflow_job"}
        wait_for_terminal = bool((recovery_payload or {}).get("auto_job_daemon", False))
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
                float(recovery_payload.get("job_recovery_poll_seconds") or 2.0)
                * float(recovery_payload.get("job_recovery_max_ticks") or 900)
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
                runtime_health = _classify_job_runtime_health(
                    job=latest_job,
                    workers=workers,
                    worker_summary=worker_summary,
                    runtime_controls=runtime_controls,
                    blocked_task=str(job_summary.get("blocked_task") or ""),
                ) if latest_job else {}
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
                last_recovery_result = self.run_worker_recovery_once(
                    recovery_payload
                )
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

        if job_status == "queued":
            thread = threading.Thread(
                target=self.run_queued_workflow,
                kwargs={"job_id": job_id},
                name=f"hosted-workflow-{job_id}",
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
            thread = threading.Thread(
                target=self._continue_workflow_stage2_worker,
                kwargs={"job_id": job_id, "approval_payload": {}},
                name=f"hosted-stage2-{job_id}",
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
        hosted_source = str(payload.get("hosted_runtime_source") or "server_runtime_watchdog").strip() or "server_runtime_watchdog"
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

        runtime_metrics = self._refresh_runtime_metrics_snapshot(source="hosted_runtime_watchdog_once")
        return {
            "status": "completed",
            "mode": "hosted",
            "worker_recovery": worker_recovery,
            "hosted_dispatch": hosted_dispatch,
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
            request_sig = request_signature(request_payload)
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
            supersede_reason = (
                "Superseded by newer completed workflow with the same request signature."
            )
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

    def supersede_workflow_jobs(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        job_ids = [
            str(item or "").strip()
            for item in list(payload.get("job_ids") or [])
            if str(item or "").strip()
        ]
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
            execution_preferences = dict(request_payload.get("execution_preferences") or {})
            allow_high_cost = payload.get("allow_high_cost_sources")
            if allow_high_cost is None:
                allow_high_cost = True
            execution_preferences["allow_high_cost_sources"] = bool(allow_high_cost)
            request_payload["execution_preferences"] = execution_preferences
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
                    "allow_high_cost_sources": bool(allow_high_cost),
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

    def _continue_workflow_stage2_worker(self, *, job_id: str, approval_payload: dict[str, Any] | None = None) -> dict[str, Any]:
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
            execution_preferences = dict(request_payload.get("execution_preferences") or {})
            allow_high_cost = approval_payload.get("allow_high_cost_sources")
            if allow_high_cost is None:
                allow_high_cost = execution_preferences.get("allow_high_cost_sources")
            if allow_high_cost is None:
                allow_high_cost = True
            execution_preferences["allow_high_cost_sources"] = bool(allow_high_cost)
            request_payload["execution_preferences"] = execution_preferences
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
                {"analysis_stage": "stage_2_final", "allow_high_cost_sources": bool(allow_high_cost)},
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
            self.store.append_job_event(job_id, "completed", "completed", "Workflow completed after stage 2 analysis.", final_summary)
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
        job_id = self._create_workflow_job(request, plan)
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
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
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

    def get_job_results(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        workflow_stage_summaries = self._load_workflow_stage_summaries(job=job)
        return {
            "job": job,
            "events": self.store.list_job_events(job_id),
            "results": self.store.get_job_results(job_id),
            "manual_review_items": self.store.list_manual_review_items(job_id=job_id, status="", limit=100),
            "agent_runtime_session": self.store.get_agent_runtime_session(job_id=job_id) or {},
            "agent_trace_spans": self.store.list_agent_trace_spans(job_id=job_id),
            "agent_workers": self.agent_runtime.list_workers(job_id=job_id),
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=dict(job.get("request") or {})),
            "workflow_stage_summaries": workflow_stage_summaries,
        }

    def get_job_progress(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        events = self.store.list_job_events(job_id)
        workers = self.agent_runtime.list_workers(job_id=job_id)
        results = self.store.get_job_results(job_id)
        manual_review_items = self.store.list_manual_review_items(job_id=job_id, status="", limit=100)
        runtime_controls = self._build_live_runtime_controls_payload(job)
        payload = _build_job_progress_payload(
            job=job,
            events=events,
            workers=workers,
            results=results,
            manual_review_items=manual_review_items,
            runtime_controls=runtime_controls,
        )
        payload["workflow_stage_summaries"] = self._load_workflow_stage_summaries(job=job)
        return payload

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

    def get_runtime_health(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        if not _coerce_bool(payload.get("force_refresh"), False):
            cached = self._load_runtime_metrics_snapshot(payload)
            if cached is not None:
                cache_payload = dict(cached.get("cache") or {})
                cache_payload["status"] = "hit"
                cached["cache"] = cache_payload
                return cached
        health = self._compute_runtime_health(payload)
        return self._persist_runtime_metrics_snapshot(health, source="runtime_health")

    def _compute_runtime_health(self, payload: dict[str, Any]) -> dict[str, Any]:
        active_limit = max(10, int(payload.get("active_limit") or 50))
        stale_after_seconds = max(1, int(payload.get("stale_after_seconds") or _env_int("WORKFLOW_AUTO_RESUME_STALE_SECONDS", 60)))
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

        stalled_jobs: list[dict[str, Any]] = []
        tracked_job_recoveries: list[dict[str, Any]] = []
        pre_retrieval_refresh_count = 0
        inline_search_seed_worker_count = 0
        inline_harvest_prefetch_worker_count = 0
        background_reconcile_job_count = 0
        background_search_seed_reconcile_count = 0
        background_harvest_prefetch_reconcile_count = 0
        for job in active_jobs:
            runtime_controls = self._build_live_runtime_controls_payload(job)
            workers = self.agent_runtime.list_workers(job_id=str(job.get("job_id") or ""))
            worker_summary = _job_worker_summary(workers)
            job_summary = dict(job.get("summary") or {})
            runtime_health = _classify_job_runtime_health(
                job=job,
                workers=workers,
                worker_summary=worker_summary,
                runtime_controls=runtime_controls,
                blocked_task=str(job_summary.get("blocked_task") or ""),
            )
            refresh_metrics = _extract_refresh_metrics(job_summary)
            if refresh_metrics:
                if int(refresh_metrics.get("pre_retrieval_refresh_count") or 0) > 0:
                    pre_retrieval_refresh_count += 1
                inline_search_seed_worker_count += int(refresh_metrics.get("inline_search_seed_worker_count") or 0)
                inline_harvest_prefetch_worker_count += int(refresh_metrics.get("inline_harvest_prefetch_worker_count") or 0)
                if int(refresh_metrics.get("background_reconcile_count") or 0) > 0:
                    background_reconcile_job_count += 1
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

        shared_ready = _service_status_is_ready(shared_recovery_status)
        hosted_ready = _service_status_is_ready(hosted_runtime_watchdog_status)

        status = "ok"
        if (
            str(providers.get("status") or "") != "ready"
            or (
                active_jobs
                and not shared_ready
                and not hosted_ready
                and str(shared_recovery_status.get("status") or "") in {"not_started", "stale", "failed", "corrupted"}
                and str(hosted_runtime_watchdog_status.get("status") or "") in {"not_started", "stale", "failed", "corrupted"}
            )
            or stalled_jobs
            or stale_acquiring_jobs
            or stale_queue_jobs
        ):
            status = "degraded"
        if (
            str(shared_recovery_status.get("status") or "") in {"failed", "corrupted"}
            and str(hosted_runtime_watchdog_status.get("status") or "") in {"failed", "corrupted", "not_started"}
        ):
            status = "failed"

        metrics = {
            "workflow_jobs": workflow_counts,
            "active_job_count": len(active_jobs),
            "stalled_job_count": len(stalled_jobs),
            "stale_acquiring_job_count": len(stale_acquiring_jobs),
            "stale_queue_job_count": len(stale_queue_jobs),
            "recoverable_worker_count": len(recoverable_workers),
            "tracked_job_recovery_count": len(tracked_job_recoveries),
            "pre_retrieval_refresh_job_count": pre_retrieval_refresh_count,
            "inline_search_seed_worker_count": inline_search_seed_worker_count,
            "inline_harvest_prefetch_worker_count": inline_harvest_prefetch_worker_count,
            "background_reconcile_job_count": background_reconcile_job_count,
            "background_search_seed_reconcile_job_count": background_search_seed_reconcile_count,
            "background_harvest_prefetch_reconcile_job_count": background_harvest_prefetch_reconcile_count,
        }
        return {
            "status": status,
            "observed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "providers": providers,
            "services": {
                "shared_recovery": shared_recovery_status,
                "hosted_runtime_watchdog": hosted_runtime_watchdog_status,
                "job_recoveries": tracked_job_recoveries,
            },
            "metrics": metrics,
            "stalled_jobs": stalled_jobs,
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
                "hosted_runtime_watchdog": dict(dict(health.get("services") or {}).get("hosted_runtime_watchdog") or {}),
                "tracked_job_recovery_count": int(metrics.get("tracked_job_recovery_count") or 0),
            },
        }

    def get_system_progress(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        active_limit = max(1, int(payload.get("active_limit") or 10))
        object_sync_limit = max(1, int(payload.get("object_sync_limit") or 20))
        profile_registry_lookback_hours = max(0, int(payload.get("profile_registry_lookback_hours") or 24))

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
                    "refresh_metrics": dict(dict(progress_payload.get("latest_metrics") or {}).get("refresh_metrics") or {}),
                    "pre_retrieval_refresh": dict(dict(progress_payload.get("latest_metrics") or {}).get("pre_retrieval_refresh") or {}),
                    "background_reconcile": dict(dict(progress_payload.get("latest_metrics") or {}).get("background_reconcile") or {}),
                }
            )

        object_sync = self._collect_object_sync_progress(limit=object_sync_limit)
        profile_registry = self.store.get_linkedin_profile_registry_metrics(
            lookback_hours=profile_registry_lookback_hours,
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
        }

    def _collect_object_sync_progress(self, *, limit: int) -> dict[str, Any]:
        asset_exports_dir = self.runtime_dir / "asset_exports"
        progress_files: list[Path] = []
        if asset_exports_dir.exists():
            progress_files.extend(asset_exports_dir.glob("*/upload_progress.json"))
            progress_files.extend(asset_exports_dir.glob("*/download_progress.json"))
        transfers: list[dict[str, Any]] = []
        for path in progress_files:
            try:
                payload = json.loads(path.read_text())
            except (OSError, ValueError, json.JSONDecodeError):
                continue
            status = str(payload.get("status") or "")
            completion_ratio = float(payload.get("completion_ratio") or 0.0)
            transfers.append(
                {
                    "bundle_id": str(payload.get("bundle_id") or path.parent.name),
                    "bundle_kind": str(payload.get("bundle_kind") or ""),
                    "direction": "upload" if path.name == "upload_progress.json" else "download",
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
            key=lambda item: (_parse_timestamp(str(item.get("updated_at") or "")) or datetime.min.replace(tzinfo=timezone.utc)),
            reverse=True,
        )
        status_counts = Counter(
            str(item.get("status") or "")
            for item in transfers
            if str(item.get("status") or "")
        )
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
        return {
            "tracked_bundle_count": len(tracked_bundles),
            "bundle_index_updated_at": str(bundle_index.get("updated_at") or ""),
            "active_transfer_count": active_transfer_count,
            "status_counts": dict(status_counts),
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
        for key in ("hosted_runtime_watchdog", "shared_recovery", "job_recovery", "workflow_runner", "workflow_runner_control"):
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
            key: dict(value)
            for key, value in dict(controls or {}).items()
            if isinstance(value, dict) and value
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

    def _build_live_runtime_controls_payload(self, job: dict[str, Any]) -> dict[str, Any]:
        summary = dict(job.get("summary") or {})
        runtime_controls = dict(summary.get("runtime_controls") or {})
        if not runtime_controls:
            return {}
        payload: dict[str, Any] = {}
        for key in ("hosted_runtime_watchdog", "shared_recovery", "job_recovery", "workflow_runner", "workflow_runner_control"):
            value = runtime_controls.get(key)
            if not isinstance(value, dict) or not value:
                continue
            control = dict(value)
            if key in {"hosted_runtime_watchdog", "shared_recovery", "job_recovery"}:
                service_name = str(control.get("service_name") or "").strip()
                if service_name:
                    service_status = read_service_status(self.runtime_dir, service_name)
                    control["service_status"] = service_status
                    control["service_ready"] = _service_status_is_ready(service_status)
            elif key == "workflow_runner":
                pid = int(control.get("pid") or 0)
                control["process_alive"] = _workflow_runner_process_alive(pid)
                log_path_value = str(control.get("log_path") or "").strip()
                if log_path_value and (not control["process_alive"] or str(control.get("status") or "") != "started"):
                    control["log_tail"] = _read_text_tail(Path(log_path_value))
            payload[key] = control
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
        for candidate in [explicit_job_id, *daemon_jobs.keys(), *workflow_resume_by_job.keys(), *reconcile_by_job.keys()]:
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
                    for key in ("hosted_runtime_watchdog", "shared_recovery", "job_recovery", "workflow_runner", "workflow_runner_control")
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

    def _build_worker_recovery_daemon(self, payload: dict[str, Any]) -> PersistentWorkerRecoveryDaemon:
        owner_id = str(payload.get("owner_id") or f"recovery-daemon-{uuid.uuid4().hex[:8]}")
        stale_after_raw = payload.get("stale_after_seconds")
        stale_after_seconds = 180 if stale_after_raw in {None, ""} else int(stale_after_raw)
        return PersistentWorkerRecoveryDaemon(
            store=self.store,
            agent_runtime=self.agent_runtime,
            acquisition_engine=self.acquisition_engine,
            owner_id=owner_id,
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
            callback_payload["runtime_heartbeat_service_name"] = str(payload.get("service_name") or "worker-recovery-daemon")
        if "runtime_heartbeat_interval_seconds" not in callback_payload:
            callback_payload["runtime_heartbeat_interval_seconds"] = _env_int("WORKFLOW_RUNTIME_HEARTBEAT_INTERVAL_SECONDS", 60)
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
            start_thread_fallback=lambda: self._start_job_scoped_recovery_thread_fallback(job_id, payload, config=config),
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
            worker
            for worker in workers
            if str(effective_worker_status(worker) or "") not in {"completed", "failed"}
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
        reviewed = self.store.review_plan_session(
            review_id=review_id,
            status=status,
            reviewer=str(payload.get("reviewer") or "").strip(),
            notes=str(payload.get("notes") or "").strip(),
            decision_payload=dict(payload.get("decision") or payload.get("decision_payload") or {}),
            request_payload=request_payload,
            plan_payload=plan_payload,
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
        return {"plan_reviews": self.store.list_plan_review_sessions(target_company=target_company, status=status, limit=100)}

    def list_query_dispatches(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        limit = 100
        raw_limit = payload.get("limit")
        if raw_limit not in {None, ""}:
            try:
                limit = max(1, int(raw_limit))
            except (TypeError, ValueError):
                limit = 100
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
            request_patch = normalize_refinement_patch(
                dict(payload.get("request_patch") or payload.get("patch") or {})
            )
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
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        return {
            "status": "compiled",
            "baseline_job_id": str(baseline_job.get("job_id") or ""),
            "request_patch": request_patch,
            "request": request.to_record(),
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

    def list_manual_review_items(self, target_company: str = "", job_id: str = "", status: str = "open") -> dict[str, Any]:
        return {
            "manual_review_items": self.store.list_manual_review_items(
                target_company=target_company,
                job_id=job_id,
                status=status,
                limit=200,
            )
        }

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
            metadata_merge=dict((resolution or {}).get("metadata") or {}),
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
            "feedback_type": str(((source_feedback or {}).get("feedback_type") or ((source_feedback or {}).get("metadata") or {}).get("feedback_type") or payload.get("feedback_type") or "")),
            "subject": str(suggestion.get("subject") or payload.get("subject") or ""),
            "value": str(suggestion.get("value") or payload.get("value") or ""),
            "rerun_retrieval": payload.get("rerun_retrieval"),
            "rerun_request_overrides": payload.get("rerun_request_overrides") or {},
        }
        recompile = self.criteria_evolution.recompile_after_feedback(recompile_payload, source_feedback_id)
        rerun_feedback = {
            "feedback_id": source_feedback_id,
            "feedback_type": recompile_payload.get("feedback_type") or str(((source_feedback or {}).get("feedback_type") or "")),
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
            "confidence_policy_controls": self.store.list_confidence_policy_controls(target_company=target_company, limit=50),
        }

    def _resolve_workflow_plan(self, payload: dict[str, Any]) -> dict[str, Any]:
        review_id = int(payload.get("plan_review_id") or 0)
        if review_id:
            review_session = self.store.get_plan_review_session(review_id)
            if review_session is None:
                return {"status": "needs_plan_review", "reason": "plan_review_id not found", "plan_review_id": review_id}
            request_payload = dict(review_session.get("request") or {})
            plan_payload = dict(review_session.get("plan") or {})
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
                    "intent_rewrite": _build_intent_rewrite_payload(request_payload=request_payload),
                }
            try:
                reviewed_request = JobRequest.from_payload(request_payload)
                rebuilt_plan = build_sourcing_plan(reviewed_request, self.catalog, self.model_client).to_record()
                plan_payload = _ensure_plan_company_scope(
                    rebuilt_plan,
                    str(reviewed_request.target_company or resolved_target_company or "").strip(),
                )
                request_payload = reviewed_request.to_record()
            except Exception:
                pass
            return {
                "status": "ready",
                "request": request_payload,
                "plan": plan_payload,
                "plan_review_session": review_session,
                "intent_rewrite": _build_intent_rewrite_payload(request_payload=request_payload),
            }

        request = JobRequest.from_payload(self._prepare_request_payload(payload))
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        plan_review_gate = build_plan_review_gate(request, plan)
        plan_review_gate = self._augment_plan_review_gate_with_runtime_context(request, plan_review_gate)
        if bool(plan_review_gate.get("required_before_execution")) and not bool(payload.get("skip_plan_review")):
            existing_pending = self.store.find_pending_plan_review_session(
                target_company=request.target_company,
                request_payload=request.to_record(),
            )
            if existing_pending is not None:
                return {
                    "status": "needs_plan_review",
                    "reason": "existing_pending_plan_review",
                    "request": dict(existing_pending.get("request") or {}),
                    "plan": dict(existing_pending.get("plan") or {}),
                    "plan_review_gate": dict(existing_pending.get("gate") or {}),
                    "plan_review_session": existing_pending,
                    "intent_rewrite": _build_intent_rewrite_payload(
                        request_payload=dict(existing_pending.get("request") or {})
                    ),
                }
            review_session = self.store.create_plan_review_session(
                target_company=request.target_company,
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                gate_payload=plan_review_gate,
            )
            return {
                "status": "needs_plan_review",
                "request": request.to_record(),
                "plan": plan.to_record(),
                "plan_review_gate": plan_review_gate,
                "plan_review_session": review_session,
                "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
            }
        return {
            "status": "ready",
            "request": request.to_record(),
            "plan": plan.to_record(),
            "plan_review_session": {},
            "plan_review_gate": plan_review_gate,
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=request.to_record()),
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
        canonical_request = JobRequest.from_payload(payload).to_record()
        planning_mode = str(canonical_request.get("planning_mode") or "").strip().lower()
        if planning_mode not in {"model_assisted", "llm_brief", "product_brief_model_assisted"}:
            return canonical_request
        explicit_execution_preferences = normalize_execution_preferences(
            payload,
            target_company=str(canonical_request.get("target_company") or payload.get("target_company") or ""),
        )
        normalized_patch = self.model_client.normalize_request(
            {
                "raw_user_request": canonical_request.get("raw_user_request") or canonical_request.get("query") or "",
                "current_request": canonical_request,
            }
        )
        if not isinstance(normalized_patch, dict) or not normalized_patch:
            return canonical_request
        merged_payload = _merge_request_payload(
            canonical_request,
            normalized_patch,
            explicit_execution_preferences=explicit_execution_preferences,
        )
        return JobRequest.from_payload(_canonicalize_request_payload(merged_payload)).to_record()

    def _build_query_dispatch_context(self, payload: dict[str, Any], request: JobRequest) -> dict[str, Any]:
        requester_value = payload.get("requester")
        requester_value_resolved = (
            str(dict(requester_value).get("id") or "").strip()
            if isinstance(requester_value, dict)
            else str(requester_value or "").strip()
        )
        requester_id = str(
            payload.get("requester_id")
            or payload.get("user_id")
            or requester_value_resolved
            or ""
        ).strip()
        tenant_id = str(
            payload.get("tenant_id")
            or payload.get("workspace_id")
            or payload.get("org_id")
            or ""
        ).strip()
        idempotency_key = str(
            payload.get("idempotency_key")
            or payload.get("query_idempotency_key")
            or ""
        ).strip()
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
            "request_signature": request_signature(request.to_record()),
            "request_family_signature": request_family_signature(request.to_record()),
        }

    def _request_with_snapshot_reuse(self, request: JobRequest, dispatch: dict[str, Any]) -> JobRequest:
        snapshot_id = str(dispatch.get("matched_snapshot_id") or "").strip()
        if not snapshot_id:
            return request
        payload = request.to_record()
        execution_preferences = dict(payload.get("execution_preferences") or {})
        execution_preferences["reuse_existing_roster"] = True
        execution_preferences["reuse_snapshot_id"] = snapshot_id
        baseline_job_id = str(dispatch.get("matched_job_id") or dict(dispatch.get("matched_job") or {}).get("job_id") or "").strip()
        if baseline_job_id:
            execution_preferences["reuse_baseline_job_id"] = baseline_job_id
        payload["execution_preferences"] = execution_preferences
        return JobRequest.from_payload(payload)

    def _resolve_snapshot_reuse_job_match(
        self,
        request: JobRequest,
        context: dict[str, Any],
    ) -> dict[str, Any]:
        request_payload = request.to_record()
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
            snapshot_context = self._load_snapshot_reuse_context_from_job(candidate, target_company=request.target_company)
            if not snapshot_context:
                continue
            match = request_family_score(request_payload, candidate_request)
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
            "request_family_signature": str(context.get("request_family_signature") or request_family_signature(request_payload)),
            "matched_job": best_job,
            "matched_snapshot_id": str(best_snapshot.get("snapshot_id") or ""),
            "matched_snapshot_dir": str(best_snapshot.get("snapshot_dir") or ""),
            "matched_snapshot_source_path": str(best_snapshot.get("source_path") or ""),
            "baseline_match": dict(best_match),
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
        try:
            snapshot_payload = load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company=target_company,
                snapshot_id=snapshot_id,
                view="canonical_merged",
            )
        except CandidateArtifactError:
            return {}
        return {
            "snapshot_id": str(snapshot_payload.get("snapshot_id") or ""),
            "snapshot_dir": str(snapshot_payload.get("snapshot_dir") or ""),
            "source_path": str(snapshot_payload.get("source_path") or ""),
        }

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
            if completed_match:
                return {
                    "strategy": "reuse_completed",
                    "scope": scope,
                    "request_signature": request_sig,
                    "request_family_signature": request_family_sig,
                    "matched_job": completed_match,
                }
            snapshot_reuse_match = self._resolve_snapshot_reuse_job_match(request, context)
            if snapshot_reuse_match:
                return snapshot_reuse_match

        return {
            "strategy": "new_job",
            "scope": scope,
            "request_signature": request_sig,
            "request_family_signature": request_family_sig,
            "matched_job": {},
        }

    def _create_workflow_job(self, request: JobRequest, plan, *, dispatch_context: dict[str, Any] | None = None) -> str:
        job_id = uuid.uuid4().hex[:12]
        dispatch_context = dict(dispatch_context or {})
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Workflow queued"},
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
            self.store.append_job_event(job_id, "acquiring", "running", "Resuming blocked workflow after worker recovery.")
        completed_task_ids = self._effective_acquisition_completed_task_ids(
            request=request,
            plan=plan,
            acquisition_progress=acquisition_progress,
            acquisition_state=acquisition_state,
        )
        for task in plan.acquisition_tasks:
            if task.task_id in completed_task_ids and self._acquisition_task_checkpoint_reusable(task, acquisition_state):
                checkpoint_payload = {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "resume_mode": resume_mode,
                    "restored_from_checkpoint": True,
                }
                self.store.append_job_event(
                    job_id,
                    stage=current_stage,
                    status="skipped",
                    detail=f"{task.title}: restored from acquisition checkpoint.",
                    payload=checkpoint_payload,
                )
                acquisition_progress = self._record_acquisition_task_completion(
                    job_id=job_id,
                    request=request,
                    plan=plan,
                    task=task,
                    execution=SimpleNamespace(
                        status="skipped",
                        detail="Restored from acquisition checkpoint.",
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

        pre_retrieval_refresh = self._refresh_running_workflow_before_retrieval(
            job_id=job_id,
            request=request,
            plan=plan,
            acquisition_state=acquisition_state,
        )
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            acquisition_state["pre_retrieval_refresh"] = pre_retrieval_refresh

        if self._should_use_two_stage_workflow_analysis(request) and self._should_require_stage2_confirmation(request):
            blocked_summary = self._workflow_job_summary(job_id)
            if self._stage1_preview_ready(blocked_summary):
                blocked_summary["message"] = "Public Web Stage 2 acquisition completed. Awaiting user approval for stage 2 AI analysis."
                blocked_summary["analysis_stage_mode"] = "two_stage"
                blocked_summary["awaiting_user_action"] = "continue_stage2"
                blocked_summary["stage2_transition_state"] = "idle"
                blocked_summary.pop("blocked_task", None)
                blocked_summary.pop("blocked_task_id", None)
                if acquisition_progress:
                    completed_progress = dict(acquisition_progress)
                    completed_progress["status"] = "completed"
                    completed_progress["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    blocked_summary["acquisition_progress"] = completed_progress
                if str(pre_retrieval_refresh.get("status") or "") == "completed":
                    blocked_summary["pre_retrieval_refresh"] = pre_retrieval_refresh
                self.store.save_job(
                    job_id=job_id,
                    job_type="workflow",
                    status="blocked",
                    stage="retrieving",
                    request_payload=request.to_record(),
                    plan_payload=plan.to_record(),
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

        current_stage = "retrieving"
        outreach_layering_summary = self._run_outreach_layering_after_acquisition(
            job_id=job_id,
            request=request,
            acquisition_state=acquisition_state,
            allow_ai=False,
            analysis_stage_label="stage_2_final",
        )
        if outreach_layering_summary:
            acquisition_state["outreach_layering"] = outreach_layering_summary
        retrieving_summary = self._workflow_job_summary(job_id)
        retrieving_summary["message"] = "Retrieving candidates"
        retrieving_summary.pop("blocked_task", None)
        retrieving_summary.pop("blocked_task_id", None)
        retrieving_summary.pop("awaiting_user_action", None)
        retrieving_summary.pop("stage2_transition_state", None)
        if acquisition_progress:
            completed_progress = dict(acquisition_progress)
            completed_progress["status"] = "completed"
            completed_progress["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            retrieving_summary["acquisition_progress"] = completed_progress
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            retrieving_summary["pre_retrieval_refresh"] = pre_retrieval_refresh
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage=current_stage,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload=retrieving_summary,
        )
        self.store.append_job_event(job_id, "retrieving", "running", "Retrieval stage started.")
        artifact = self._execute_retrieval(
            job_id,
            request,
            plan,
            job_type="workflow",
            runtime_policy={
                "workflow_snapshot_id": str(acquisition_state.get("snapshot_id") or "").strip(),
                "outreach_layering": dict(acquisition_state.get("outreach_layering") or {}),
            },
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
            "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
                if str(item).strip() and str(item).strip() in ordered_phase_task_ids and str(item).strip() != task.task_id
            ]
            phase_completed_task_ids.append(task.task_id)
            phases_payload[phase_id] = {
                "phase_id": phase_id,
                "title": phase_title,
                "task_ids": ordered_phase_task_ids,
                "completed_task_ids": phase_completed_task_ids,
                "completed_task_count": len(phase_completed_task_ids),
                "task_count": len(ordered_phase_task_ids),
                "status": "completed" if ordered_phase_task_ids and len(phase_completed_task_ids) >= len(ordered_phase_task_ids) else "running",
                "last_completed_task_id": task.task_id,
                "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        candidate_doc_path = state.get("candidate_doc_path")
        if not isinstance(candidate_doc_path, Path) and isinstance(snapshot_dir, Path):
            candidate_doc_candidate = snapshot_dir / "candidate_documents.json"
            if candidate_doc_candidate.exists():
                state["candidate_doc_path"] = candidate_doc_candidate
                candidate_doc_path = candidate_doc_candidate
        if isinstance(candidate_doc_path, Path):
            candidate_document_state = _load_candidate_document_state(candidate_doc_path)
            if candidate_document_state:
                state.update(candidate_document_state)
            if bool(state.get("linkedin_stage_completed")) and not isinstance(state.get("linkedin_stage_candidate_doc_path"), Path):
                state["linkedin_stage_candidate_doc_path"] = candidate_doc_path
            if bool(state.get("public_web_stage_completed")) and not isinstance(
                state.get("public_web_stage_candidate_doc_path"),
                Path,
            ):
                state["public_web_stage_candidate_doc_path"] = candidate_doc_path
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

    def _load_reusable_snapshot_state(self, request: JobRequest) -> dict[str, Any]:
        execution_preferences = dict(request.execution_preferences or {})
        snapshot_id = str(execution_preferences.get("reuse_snapshot_id") or "").strip()
        if not snapshot_id or not request.target_company.strip():
            return {}
        try:
            snapshot_payload = load_company_snapshot_candidate_documents(
                runtime_dir=self.runtime_dir,
                target_company=request.target_company,
                snapshot_id=snapshot_id,
                view="canonical_merged",
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
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        if candidate_doc_path.exists():
            state["candidate_doc_path"] = candidate_doc_path
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
        requires_stage1_completion = blocked_task in {"enrich_linkedin_profiles", "enrich_profiles_multisource"} and not bool(
            acquisition_state.get("linkedin_stage_completed")
        )
        critical_pending_workers = [
            worker
            for worker in pending_worker_records
            if _worker_requires_blocking_acquisition_completion(worker)
        ]
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
                "reason": "active_acquisition_workers_remaining" if not requires_stage1_completion else "linkedin_stage_pending",
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

    def _acquisition_task_checkpoint_reusable(self, task: AcquisitionTask, acquisition_state: dict[str, Any]) -> bool:
        if task.task_type == "resolve_company_identity":
            return isinstance(acquisition_state.get("company_identity"), CompanyIdentity) and isinstance(
                acquisition_state.get("snapshot_dir"),
                Path,
            )
        if task.task_type == "acquire_full_roster":
            roster_snapshot = acquisition_state.get("roster_snapshot")
            search_seed_snapshot = acquisition_state.get("search_seed_snapshot")
            roster_ready = isinstance(roster_snapshot, CompanyRosterSnapshot) and bool(
                list(roster_snapshot.visible_entries or []) or list(roster_snapshot.raw_entries or [])
            )
            search_seed_ready = isinstance(search_seed_snapshot, SearchSeedSnapshot) and bool(
                list(search_seed_snapshot.entries or [])
            )
            candidate_ready = bool(acquisition_state.get("candidates"))
            strategy_type = str(task.metadata.get("strategy_type") or "").strip()
            if strategy_type == "full_company_roster":
                # Recovery can continue from search-seed discovery even when the
                # full roster task was the original blocker. This lets resume
                # paths move forward with a usable lower-bound baseline instead
                # of replaying the expensive roster fetch.
                return roster_ready or search_seed_ready or candidate_ready
            if strategy_type in {"scoped_search_roster", "former_employee_search"}:
                return search_seed_ready or candidate_ready
            return roster_ready or search_seed_ready or candidate_ready
        if task.task_type == "acquire_former_search_seed":
            if (
                bool(acquisition_state.get("reused_snapshot_checkpoint"))
                and isinstance(acquisition_state.get("candidate_doc_path"), Path)
                and bool(acquisition_state.get("candidates"))
            ):
                return True
            return isinstance(acquisition_state.get("search_seed_snapshot"), SearchSeedSnapshot)
        if task.task_type == "enrich_linkedin_profiles":
            stage_path = acquisition_state.get("linkedin_stage_candidate_doc_path")
            if (
                bool(acquisition_state.get("linkedin_stage_completed"))
                and isinstance(stage_path, Path)
                and stage_path.exists()
            ):
                return True
            if (
                bool(acquisition_state.get("reused_snapshot_checkpoint"))
                and isinstance(acquisition_state.get("candidate_doc_path"), Path)
                and bool(acquisition_state.get("candidates"))
            ):
                return True
            return bool(acquisition_state.get("linkedin_stage_completed")) and isinstance(
                acquisition_state.get("candidate_doc_path"),
                Path,
            )
        if task.task_type == "enrich_public_web_signals":
            stage_path = acquisition_state.get("public_web_stage_candidate_doc_path")
            if (
                bool(acquisition_state.get("public_web_stage_completed"))
                and isinstance(stage_path, Path)
                and stage_path.exists()
            ):
                return True
            if (
                bool(acquisition_state.get("reused_snapshot_checkpoint"))
                and isinstance(acquisition_state.get("candidate_doc_path"), Path)
                and bool(acquisition_state.get("candidates"))
            ):
                return True
            return bool(acquisition_state.get("public_web_stage_completed")) and isinstance(
                acquisition_state.get("candidate_doc_path"),
                Path,
            )
        if task.task_type == "enrich_profiles_multisource":
            candidate_doc_path = acquisition_state.get("candidate_doc_path")
            return isinstance(candidate_doc_path, Path) and candidate_doc_path.exists()
        if task.task_type == "normalize_asset_snapshot":
            manifest_path = acquisition_state.get("manifest_path")
            return isinstance(manifest_path, Path) and manifest_path.exists()
        if task.task_type == "build_retrieval_index":
            snapshot_dir = acquisition_state.get("snapshot_dir")
            return isinstance(snapshot_dir, Path) and (snapshot_dir / "retrieval_index_summary.json").exists()
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
    ) -> dict[str, Any]:
        if not _env_bool("OUTREACH_LAYERING_ENABLED", True):
            return {}
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(acquisition_state.get("snapshot_id") or "").strip()
        if not target_company or not snapshot_id:
            return {}
        query_text = str(request.raw_user_request or request.query or "").strip()
        if allow_ai is None:
            ai_requested = _env_bool("OUTREACH_LAYERING_ENABLE_AI", True)
        else:
            ai_requested = bool(allow_ai)
        model_client: ModelClient | None = None
        if ai_requested and bool(getattr(self.model_client, "supports_outreach_ai_verification", lambda: False)()):
            model_client = self.model_client
        max_ai_verifications = (
            self._resolve_outreach_ai_verification_budget(request)
            if model_client
            else 0
        )
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
                max_ai_verifications=max_ai_verifications,
                ai_workers=ai_workers,
                ai_max_retries=ai_max_retries,
                ai_retry_backoff_seconds=ai_retry_backoff_seconds,
            )
            layer_schema = dict(result.get("layer_schema") or {})
            primary_layer_keys = [
                str(item).strip()
                for item in list(layer_schema.get("primary_layer_keys") or [])
                if str(item).strip()
            ] or [
                "layer_0_roster",
                "layer_1_name_signal",
                "layer_2_greater_china_region_experience",
                "layer_3_mainland_china_experience_or_chinese_language",
            ]
            layer_counts = {
                key: int((result.get("layers") or {}).get(key, {}).get("count") or 0)
                for key in primary_layer_keys
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
                stage="acquiring",
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
                stage="acquiring",
                status="completed",
                detail="Outreach layering failed; workflow continues with base retrieval.",
                payload=summary_payload,
            )
            return summary_payload

    def _should_use_two_stage_workflow_analysis(self, request: JobRequest) -> bool:
        return str(getattr(request, "analysis_stage_mode", "") or "").strip().lower() == "two_stage"

    def _should_require_stage2_confirmation(self, request: JobRequest) -> bool:
        return bool(dict(getattr(request, "execution_preferences", {}) or {}).get("require_stage2_confirmation"))

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
            "analysis_stage_mode",
            "stage1_preview",
            "request_preview",
            "linkedin_stage_1",
            "public_web_stage_2",
            "pre_retrieval_refresh",
            "acquisition_progress",
        ):
            if merged.get(key) not in (None, "", [], {}):
                continue
            for source in source_payloads:
                value = dict(source or {}).get(key)
                if value not in (None, "", [], {}):
                    merged[key] = value
                    break
        return merged

    def _resolve_workflow_snapshot_dir(
        self,
        *,
        request: JobRequest,
        summary_payloads: list[dict[str, Any] | None],
    ) -> Path | None:
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
                candidate_path = Path(str(path_value or "")).expanduser()
                if not candidate_path.exists():
                    continue
                if candidate_path.name.startswith("candidate_documents."):
                    return candidate_path.parent
                if candidate_path.name == "candidate_documents.json":
                    return candidate_path.parent
                if candidate_path.name == "materialized_candidate_documents.json":
                    return candidate_path.parent.parent
            candidate_snapshot_ids = [
                summary.get("snapshot_id"),
                dict(summary.get("candidate_source") or {}).get("snapshot_id"),
                dict(summary.get("linkedin_stage_1") or {}).get("snapshot_id"),
                dict(summary.get("public_web_stage_2") or {}).get("snapshot_id"),
                dict(dict(summary.get("stage1_preview") or {}).get("candidate_source") or {}).get("snapshot_id"),
            ]
            company_key = normalize_name_token(str(request.target_company or "").strip())
            if not company_key:
                continue
            for snapshot_id_value in candidate_snapshot_ids:
                snapshot_id = str(snapshot_id_value or "").strip()
                if not snapshot_id:
                    continue
                candidate = self.runtime_dir / "company_assets" / company_key / snapshot_id
                if candidate.exists():
                    return candidate
        return None

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
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        if stage_name == "linkedin_stage_1":
            record = dict(job_summary.get("linkedin_stage_1") or {})
        elif stage_name == "stage_1_preview":
            record = dict(job_summary.get("stage1_preview") or {})
        elif stage_name == "public_web_stage_2":
            record = dict(job_summary.get("public_web_stage_2") or {})
        elif stage_name == "stage_2_final":
            record = dict(job_summary)
        else:
            record = {}
        if not record:
            return {}
        record.setdefault("stage", stage_name)
        return record

    def _load_workflow_stage_summaries(self, *, job: dict[str, Any]) -> dict[str, Any]:
        job_summary = dict(job.get("summary") or {})
        request = JobRequest.from_payload(dict(job.get("request") or {}))
        snapshot_dir = self._resolve_workflow_snapshot_dir(
            request=request,
            summary_payloads=[job_summary],
        )
        stage_summary_dir = snapshot_dir / "workflow_stage_summaries" if isinstance(snapshot_dir, Path) else None
        stage_path_hints = {
            "linkedin_stage_1": str(dict(job_summary.get("linkedin_stage_1") or {}).get("summary_path") or "").strip(),
            "stage_1_preview": str(dict(job_summary.get("stage1_preview") or {}).get("summary_path") or "").strip(),
            "public_web_stage_2": str(dict(job_summary.get("public_web_stage_2") or {}).get("summary_path") or "").strip(),
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
                summaries[stage_name] = stage_record
        directory = str(stage_summary_dir) if isinstance(stage_summary_dir, Path) else ""
        return {
            "directory": directory,
            "stage_order": [stage_name for stage_name in _WORKFLOW_STAGE_SUMMARY_STAGE_ORDER if stage_name in summaries],
            "summaries": summaries,
        }

    def _build_stage1_preview_record(
        self,
        *,
        preview_artifact: dict[str, Any],
    ) -> dict[str, Any]:
        preview_summary = dict(preview_artifact.get("summary") or {})
        return {
            "status": "ready",
            "analysis_stage": str(preview_summary.get("analysis_stage") or "stage_1_preview"),
            "summary_text": str(preview_summary.get("text") or ""),
            "total_matches": int(preview_summary.get("total_matches") or 0),
            "returned_matches": int(preview_summary.get("returned_matches") or 0),
            "manual_review_queue_count": int(preview_summary.get("manual_review_queue_count") or 0),
            "candidate_source": dict(preview_summary.get("candidate_source") or {}),
            "outreach_layering": dict(preview_summary.get("outreach_layering") or {}),
            "artifact_path": str(preview_artifact.get("artifact_path") or ""),
            "ready_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def _build_stage1_preview_summary(
        self,
        *,
        request: JobRequest,
        preview_artifact: dict[str, Any],
        acquisition_progress: dict[str, Any],
        pre_retrieval_refresh: dict[str, Any],
        awaiting_stage2_confirmation: bool = False,
        acquisition_progress_status: str = "running",
        message: str = "",
    ) -> dict[str, Any]:
        preview_summary = dict(preview_artifact.get("summary") or {})
        preview_payload = self._build_stage1_preview_record(preview_artifact=preview_artifact)
        analysis_stage_mode = str(getattr(request, "analysis_stage_mode", "") or "").strip().lower() or "single_stage"
        summary_payload = {
            **preview_summary,
            "message": message or (
                "Stage 1 preview ready. Awaiting user approval for stage 2 AI analysis."
                if awaiting_stage2_confirmation
                else "Stage 1 preview ready. Continuing Public Web Stage 2 acquisition."
            ),
            "analysis_stage_mode": analysis_stage_mode,
            "stage1_preview": preview_payload,
            "request_preview": {
                "target_company": request.target_company,
                "keywords": list(request.keywords or []),
                "must_have_facets": list(request.must_have_facets or []),
                "must_have_primary_role_buckets": list(request.must_have_primary_role_buckets or []),
            },
        }
        if awaiting_stage2_confirmation:
            summary_payload["awaiting_user_action"] = "continue_stage2"
            summary_payload["stage2_transition_state"] = "idle"
        if acquisition_progress:
            progress_payload = dict(acquisition_progress)
            if acquisition_progress_status == "completed":
                progress_payload["status"] = "completed"
                progress_payload["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        linkedin_stage_payload = {
            "status": "completed",
            "snapshot_id": snapshot_id,
            "candidate_doc_path": str(candidate_doc_path or ""),
            "stage_candidate_doc_path": str(stage_candidate_doc_path or ""),
            "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        linkedin_stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name="linkedin_stage_1",
            summary_payload=linkedin_stage_payload,
            snapshot_dir=acquisition_state.get("snapshot_dir") if isinstance(acquisition_state.get("snapshot_dir"), Path) else None,
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
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
            summary_payload=working_summary,
            artifact_path=str(latest_job.get("artifact_path") or ""),
        )

        pre_retrieval_refresh = self._refresh_running_workflow_before_retrieval(
            job_id=job_id,
            request=request,
            plan=plan,
            acquisition_state=acquisition_state,
        )
        if str(pre_retrieval_refresh.get("status") or "") == "completed":
            acquisition_state["pre_retrieval_refresh"] = pre_retrieval_refresh

        outreach_layering_summary = self._run_outreach_layering_after_acquisition(
            job_id=job_id,
            request=request,
            acquisition_state=acquisition_state,
            allow_ai=False,
            analysis_stage_label="stage_1_preview",
        )
        if outreach_layering_summary:
            acquisition_state["outreach_layering"] = outreach_layering_summary

        self.store.append_job_event(job_id, "acquiring", "running", "Stage 1 deterministic preview started.")
        try:
            preview_artifact = self._execute_retrieval(
                job_id,
                request,
                plan,
                job_type="workflow",
                runtime_policy={
                    "workflow_snapshot_id": snapshot_id,
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
                awaiting_stage2_confirmation=False,
                acquisition_progress_status="running",
                message="Stage 1 preview ready. Continuing Public Web Stage 2 acquisition.",
            )
            preview_summary["linkedin_stage_1"] = linkedin_stage_payload
            preview_summary_path = self._persist_workflow_stage_summary_file(
                request=request,
                stage_name="stage_1_preview",
                summary_payload=preview_summary,
                snapshot_dir=acquisition_state.get("snapshot_dir") if isinstance(acquisition_state.get("snapshot_dir"), Path) else None,
            )
            if preview_summary_path:
                preview_stage_payload = dict(preview_summary.get("stage1_preview") or {})
                preview_stage_payload["summary_path"] = preview_summary_path
                preview_summary["stage1_preview"] = preview_stage_payload
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="acquiring",
                request_payload=request.to_record(),
                plan_payload=_plan_payload(plan),
                summary_payload=preview_summary,
                artifact_path=str(latest_job.get("artifact_path") or ""),
            )
            self.store.append_job_event(
                job_id,
                "acquiring",
                "completed",
                "Stage 1 preview ready; continuing Public Web Stage 2 acquisition.",
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
                "outreach_layering": outreach_layering_summary,
            }
        except Exception as exc:
            failed_summary = dict(working_summary)
            failed_summary["message"] = "Stage 1 preview failed; continuing Public Web Stage 2 acquisition."
            failed_summary["stage1_preview"] = {
                "status": "failed",
                "analysis_stage": "stage_1_preview",
                "artifact_path": "",
                "error": str(exc),
                "failed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            failed_preview_summary_path = self._persist_workflow_stage_summary_file(
                request=request,
                stage_name="stage_1_preview",
                summary_payload=failed_summary,
                snapshot_dir=acquisition_state.get("snapshot_dir") if isinstance(acquisition_state.get("snapshot_dir"), Path) else None,
            )
            if failed_preview_summary_path:
                failed_summary["stage1_preview"]["summary_path"] = failed_preview_summary_path
            if str(pre_retrieval_refresh.get("status") or "") == "completed":
                failed_summary["pre_retrieval_refresh"] = pre_retrieval_refresh
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage="acquiring",
                request_payload=request.to_record(),
                plan_payload=_plan_payload(plan),
                summary_payload=failed_summary,
                artifact_path=str(latest_job.get("artifact_path") or ""),
            )
            self.store.append_job_event(
                job_id,
                "acquiring",
                "failed",
                "Stage 1 preview failed; continuing Public Web Stage 2 acquisition.",
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
                "outreach_layering": outreach_layering_summary,
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
            "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        public_web_stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name="public_web_stage_2",
            summary_payload=stage_payload,
            snapshot_dir=acquisition_state.get("snapshot_dir") if isinstance(acquisition_state.get("snapshot_dir"), Path) else None,
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
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
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
        final_summary = self._merge_workflow_stage_summary_fields(
            final_summary,
            preserved_summary,
            latest_summary,
        )
        final_stage_summary_path = self._persist_workflow_stage_summary_file(
            request=request,
            stage_name=str(final_summary.get("analysis_stage") or "stage_2_final"),
            summary_payload=final_summary,
        )
        if final_stage_summary_path:
            final_summary["stage_summary_path"] = final_stage_summary_path
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=_plan_payload(plan),
            summary_payload=final_summary,
            artifact_path=str(artifact.get("artifact_path") or ""),
        )
        self.cleanup_duplicate_inflight_workflows({"target_company": request.target_company, "active_limit": 200})
        return final_summary

    def _refresh_running_workflow_before_retrieval(
        self,
        *,
        job_id: str,
        request: JobRequest,
        plan: Any,
        acquisition_state: dict[str, Any],
    ) -> dict[str, Any]:
        snapshot_dir = acquisition_state.get("snapshot_dir")
        if not isinstance(snapshot_dir, Path):
            return {"status": "skipped", "reason": "snapshot_dir_missing"}
        workers = self.agent_runtime.list_workers(job_id=job_id)
        completed_search_seed_workers = [
            worker
            for worker in workers
            if _worker_has_completed_background_search_output(worker)
        ]
        completed_harvest_workers = [
            worker
            for worker in workers
            if _worker_has_completed_background_harvest_prefetch(worker)
        ]
        if not completed_search_seed_workers and not completed_harvest_workers:
            return {"status": "skipped", "reason": "no_completed_background_workers"}

        refresh_summary: dict[str, Any] = {
            "status": "completed",
            "snapshot_id": str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
            "search_seed_worker_count": len(completed_search_seed_workers),
            "harvest_prefetch_worker_count": len(completed_harvest_workers),
        }

        search_seed_update: dict[str, Any] = {}
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

        if completed_search_seed_workers or completed_harvest_workers:
            sync_result = self._synchronize_snapshot_candidate_documents(
                request=request,
                snapshot_dir=snapshot_dir,
                reason="pre_retrieval_refresh",
            )
            state_updates = dict(sync_result.get("state_updates") or {})
            acquisition_state.update(state_updates)
            refresh_summary["sync"] = {
                key: value
                for key, value in sync_result.items()
                if key != "state_updates"
            }

        latest_job = self.store.get_job(job_id) or {}
        summary = dict(latest_job.get("summary") or {})
        summary["pre_retrieval_refresh"] = refresh_summary
        background_reconcile = dict(summary.get("background_reconcile") or {})
        if completed_search_seed_workers:
            background_reconcile["search_seed"] = {
                "status": "inline_refreshed",
                "snapshot_id": str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "applied_worker_count": len(completed_search_seed_workers),
                "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in completed_search_seed_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in completed_search_seed_workers],
                "entry_count": int((search_seed_update.get("entry_count") or 0)),
                "added_entry_count": int((search_seed_update.get("added_entry_count") or 0)),
            }
        if completed_harvest_workers:
            background_reconcile["harvest_prefetch"] = {
                "status": "inline_refreshed",
                "snapshot_id": str(acquisition_state.get("snapshot_id") or snapshot_dir.name),
                "refreshed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "applied_worker_count": len(completed_harvest_workers),
                "last_worker_updated_at": max(str(worker.get("updated_at") or "").strip() for worker in completed_harvest_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in completed_harvest_workers],
            }
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
            detail="Pre-retrieval refresh applied completed background acquisition outputs.",
            payload=refresh_summary,
        )
        return refresh_summary

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
        if not lease:
            return {"job_id": normalized_job_id, "released": False, "reason": "lease_missing"}

        job = self.store.get_job(normalized_job_id) or {}
        if str(job.get("job_type") or "") != "workflow":
            return {"job_id": normalized_job_id, "released": False, "reason": "not_workflow_job"}

        summary = dict(job.get("summary") or {})
        runtime_controls = dict(summary.get("runtime_controls") or {})
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
        runner_alive = bool(runner.get("process_alive")) if "process_alive" in runner else _workflow_runner_process_alive(runner_pid)
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
        if not allowed:
            return {
                "job_id": normalized_job_id,
                "released": False,
                "reason": "runtime_not_takeover_safe",
                "classification": classification,
            }

        self.store.release_workflow_job_lease(normalized_job_id)
        self.store.append_job_event(
            normalized_job_id,
            stage="runtime_control",
            status="recovered",
            detail="Released stale workflow job lease after runner loss so recovery can take over.",
            payload={
                "classification": classification,
                "previous_lease_owner": str(lease.get("lease_owner") or ""),
                "previous_lease_token": str(lease.get("lease_token") or ""),
                "workflow_runner_pid": runner_pid,
                "workflow_runner_alive": runner_alive,
            },
        )
        return {
            "job_id": normalized_job_id,
            "released": True,
            "classification": classification,
            "lease_owner": str(lease.get("lease_owner") or ""),
            "lease_token": str(lease.get("lease_token") or ""),
        }

    def _resume_queued_workflow_if_ready(
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
        reconcile_state = dict(job_summary.get("background_reconcile") or {})
        last_worker_updated_at = str(reconcile_state.get("last_worker_updated_at") or "").strip()
        search_seed_state = dict(reconcile_state.get("search_seed") or {})
        last_search_seed_worker_updated_at = str(search_seed_state.get("last_worker_updated_at") or "").strip()
        workers = self.agent_runtime.list_workers(job_id=job_id)
        harvest_prefetch_state = dict(reconcile_state.get("harvest_prefetch") or {})
        last_harvest_worker_updated_at = str(harvest_prefetch_state.get("last_worker_updated_at") or "").strip()
        completed_harvest_workers = [
            worker
            for worker in workers
            if _worker_has_completed_background_harvest_prefetch(worker)
        ]
        pending_harvest_workers = [
            worker
            for worker in completed_harvest_workers
            if not last_harvest_worker_updated_at or str(worker.get("updated_at") or "").strip() > last_harvest_worker_updated_at
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
            worker
            for worker in workers
            if _worker_has_completed_background_search_output(worker)
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
        if not completed_exploration_workers:
            return {"job_id": job_id, "status": "skipped", "reason": "no_completed_background_exploration_workers"}

        pending_workers = [
            worker
            for worker in completed_exploration_workers
            if not last_worker_updated_at or str(worker.get("updated_at") or "").strip() > last_worker_updated_at
        ]
        if not pending_workers:
            return {"job_id": job_id, "status": "skipped", "reason": "already_reconciled"}

        snapshot_id = _resolve_reconcile_snapshot_id(job_summary, pending_workers)
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
        updated_summary = dict(latest_job.get("summary") or {})
        updated_summary = self._merge_workflow_stage_summary_fields(updated_summary, job_summary)
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

    def _synchronize_snapshot_candidate_documents(
        self,
        *,
        request: JobRequest,
        snapshot_dir: Path,
        reason: str,
    ) -> dict[str, Any]:
        return self.snapshot_materializer.synchronize_snapshot_candidate_documents(
            request=request,
            snapshot_dir=snapshot_dir,
            reason=reason,
        )

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
        snapshot_id = _resolve_reconcile_snapshot_id(job_summary, pending_workers)
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
            snapshot_dir = _resolve_reconcile_snapshot_dir(self.runtime_dir, request.target_company, job_summary, pending_workers)
            if snapshot_dir is None:
                return {"job_id": job_id, "status": "skipped", "reason": "snapshot_dir_missing"}
            sync_result = self._synchronize_snapshot_candidate_documents(
                request=request,
                snapshot_dir=snapshot_dir,
                reason="background_harvest_prefetch_reconcile",
            )
            if str(sync_result.get("status") or "") not in {"completed", "skipped"}:
                raise RuntimeError(str(sync_result.get("detail") or sync_result.get("reason") or "snapshot_sync_failed"))
            self.store.append_job_event(
                job_id,
                "completed",
                "running",
                "Background harvest profile prefetch rebuild refreshed retrieval artifacts.",
                payload={
                    "snapshot_id": snapshot_id,
                    "sync_result": {
                        key: value
                        for key, value in sync_result.items()
                        if key != "state_updates"
                    },
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
        updated_summary = dict(latest_job.get("summary") or {})
        updated_summary = self._merge_workflow_stage_summary_fields(updated_summary, job_summary)
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
        if isinstance(resume_result.get("outreach_layering"), dict) and resume_result.get("outreach_layering"):
            updated_summary["outreach_layering"] = dict(resume_result.get("outreach_layering") or {})
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
        snapshot_id = _resolve_reconcile_snapshot_id(job_summary, pending_workers)
        if not snapshot_id:
            return {"job_id": job_id, "status": "skipped", "reason": "snapshot_id_missing"}
        snapshot_dir = _resolve_reconcile_snapshot_dir(self.runtime_dir, request.target_company, job_summary, pending_workers)
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
        updated_summary = dict(latest_job.get("summary") or {})
        updated_summary = self._merge_workflow_stage_summary_fields(updated_summary, job_summary)
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
            "candidate_count": int(sync_result.get("candidate_count") or search_seed_update.get("candidate_count") or 0),
            "evidence_count": int(sync_result.get("evidence_count") or search_seed_update.get("evidence_count") or 0),
            "artifact_dir": str(sync_result.get("artifact_dir") or ""),
            "artifact_paths": dict(sync_result.get("artifact_paths") or {}),
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
        return {
            "candidate_upsert_count": candidate_upsert_count,
            "evidence_upsert_count": len(evidence_records),
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
        return [
            worker
            for worker in workers
            if _worker_blocks_acquisition_resume(worker, blocked_task=blocked_task)
        ]
    def _mark_workflow_failed(self, job_id: str, request: JobRequest, plan: Any, exc: Exception) -> None:
        existing = self.store.get_job(job_id) or {}
        if str(existing.get("status") or "") == "completed":
            return
        failure_summary = dict(existing.get("summary") or {})
        failure_summary["error"] = str(exc)
        failure_summary["message"] = str(exc)
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

    def _wait_for_workflow_terminal_status(self, job_id: str, *, timeout_seconds: float, poll_seconds: float = 0.5) -> dict[str, Any]:
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
        lock_path = self.job_locks_dir / f"{job_id}.lock"
        handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
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
                renewed = self.store.renew_workflow_job_lease(
                    normalized_job_id,
                    lease_owner=lease_owner,
                    lease_token=lease_token,
                    lease_seconds=lease_seconds,
                )
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
                "lease_owner": lease_owner,
                "lease_token": lease_token,
                "lease_seconds": lease_seconds,
            }
        finally:
            stop_renewal.set()
            renewal_thread.join(timeout=max(1.0, float(renew_interval_seconds)))
            self.store.release_workflow_job_lease(
                normalized_job_id,
                lease_owner=lease_owner,
                lease_token=lease_token,
            )
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _job_is_terminal(self, job: dict[str, Any] | None) -> bool:
        if not isinstance(job, dict):
            return False
        return str(job.get("status") or "") in {"completed", "failed", "superseded", "cancelled", "canceled"}

    def _execute_retrieval(
        self,
        job_id: str,
        request: JobRequest,
        plan,
        job_type: str,
        runtime_policy: dict[str, Any] | None = None,
        *,
        persist_job_state: bool = True,
        artifact_name_suffix: str = "",
        artifact_status: str = "completed",
    ) -> dict[str, Any]:
        runtime_policy = dict(runtime_policy or {})
        analysis_stage = str(runtime_policy.get("analysis_stage") or "stage_2_final").strip() or "stage_2_final"
        preview_mode = analysis_stage == "stage_1_preview" or bool(runtime_policy.get("deterministic_preview"))
        candidate_source = self._load_retrieval_candidate_source(
            request,
            snapshot_id=str(runtime_policy.get("workflow_snapshot_id") or "").strip(),
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
                filtered = [item for item in baseline_candidates if int(item.metadata.get("outreach_layer") or 0) >= min_layer]
                if not filtered and 0 < fallback_min_layer < min_layer:
                    fallback_candidates = [
                        item for item in baseline_candidates if int(item.metadata.get("outreach_layer") or 0) >= fallback_min_layer
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
        if (
            not preview_mode
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
            if not evidence and str(candidate_source.get("source_kind") or "") != "company_snapshot":
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
            and not list(request.employment_statuses or [])
        ):
            relaxed_review_request = JobRequest.from_payload(
                {
                    **effective_request.to_record(),
                    "categories": [],
                    "employment_statuses": [],
                }
            )
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
                if not evidence and str(candidate_source.get("source_kind") or "") != "company_snapshot":
                    evidence = self.store.list_evidence(item.candidate.candidate_id)
                relaxed_review_evidence_lookup[item.candidate.candidate_id] = evidence
            manual_review_items = build_manual_review_items(
                relaxed_review_request,
                relaxed_review_scored,
                relaxed_review_evidence_lookup,
            )
        summary_mode = str(runtime_policy.get("summary_mode") or "").strip().lower()
        if preview_mode:
            summary_mode = "deterministic"
        if not summary_mode and not bool(effective_request.execution_preferences.get("allow_high_cost_sources")):
            summary_mode = "deterministic"
        summary_provider = self.model_client.provider_name()
        if summary_mode == "deterministic":
            summary_provider = "deterministic"
            summary_text = DeterministicModelClient().summarize(effective_request, matches, len(scored))
        else:
            summary_text = self.model_client.summarize(effective_request, matches, len(scored))
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
            "candidate_source": {
                "source_kind": str(candidate_source.get("source_kind") or ""),
                "snapshot_id": str(candidate_source.get("snapshot_id") or ""),
                "asset_view": str(candidate_source.get("asset_view") or ""),
                "candidate_count": len(candidates),
                "unfiltered_candidate_count": unfiltered_candidate_count,
                "source_path": str(candidate_source.get("source_path") or ""),
            },
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
        artifact = {
            "job_id": job_id,
            "status": artifact_status,
            "request": request.to_record(),
            "effective_request": effective_request.to_record(),
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

    def _load_retrieval_candidate_source(self, request: JobRequest, *, snapshot_id: str = "") -> dict[str, Any]:
        if request.target_company:
            try:
                snapshot_payload = load_company_snapshot_candidate_documents(
                    runtime_dir=self.runtime_dir,
                    target_company=request.target_company,
                    snapshot_id=snapshot_id,
                    view=request.asset_view,
                )
            except CandidateArtifactError:
                if request.asset_view != "canonical_merged":
                    raise
                snapshot_payload = {}
            if list(snapshot_payload.get("candidates") or []):
                evidence_lookup: dict[str, list[dict[str, Any]]] = defaultdict(list)
                for item in list(snapshot_payload.get("evidence") or []):
                    candidate_id = str(item.get("candidate_id") or "").strip()
                    if not candidate_id:
                        continue
                    evidence_lookup[candidate_id].append(item)
                return {
                    "source_kind": "company_snapshot",
                    "snapshot_id": str(snapshot_payload.get("snapshot_id") or ""),
                    "asset_view": str(snapshot_payload.get("asset_view") or "canonical_merged"),
                    "source_path": str(snapshot_payload.get("source_path") or ""),
                    "candidates": list(snapshot_payload.get("candidates") or []),
                    "evidence_lookup": dict(evidence_lookup),
                }
        return {
            "source_kind": "sqlite_store",
            "snapshot_id": "",
            "asset_view": "",
            "source_path": "",
            "candidates": self.store.list_candidates(),
            "evidence_lookup": {},
        }

    def _load_outreach_layering_context(
        self,
        *,
        request: JobRequest,
        candidate_source: dict[str, Any],
        runtime_policy: dict[str, Any],
    ) -> dict[str, Any]:
        if str(candidate_source.get("source_kind") or "") != "company_snapshot":
            return {}
        snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
        source_path = str(candidate_source.get("source_path") or "").strip()
        if not snapshot_id or not source_path:
            return {}
        analysis_path = ""
        runtime_layering = dict(runtime_policy.get("outreach_layering") or {})
        runtime_analysis_paths = dict(runtime_layering.get("analysis_paths") or {})
        runtime_full_path = str(runtime_analysis_paths.get("full") or "").strip()
        if runtime_full_path and Path(runtime_full_path).exists():
            analysis_path = runtime_full_path
        else:
            snapshot_dir = Path(source_path).resolve().parent.parent
            candidates = sorted(snapshot_dir.glob("layered_segmentation/greater_china_outreach_*/layered_analysis.json"))
            if candidates:
                analysis_path = str(candidates[-1])
        if not analysis_path:
            return {}
        try:
            analysis_payload = json.loads(Path(analysis_path).read_text())
        except Exception:
            return {}
        layer_map: dict[str, dict[str, Any]] = {}
        for item in list(analysis_payload.get("candidates") or []):
            candidate_id = str(item.get("candidate_id") or "").strip()
            if not candidate_id:
                continue
            final_layer = int(item.get("final_layer") or 0)
            layer_map[candidate_id] = {
                "outreach_layer": final_layer,
                "outreach_layer_key": _OUTREACH_LAYER_KEY_BY_INDEX.get(final_layer, "layer_0_roster"),
                "outreach_layer_source": str(item.get("final_layer_source") or ""),
            }
        if not layer_map:
            return {}
        layer_schema = dict(analysis_payload.get("layer_schema") or {})
        primary_layer_keys = [
            str(item).strip()
            for item in list(layer_schema.get("primary_layer_keys") or [])
            if str(item).strip()
        ] or list(_OUTREACH_LAYER_KEY_BY_INDEX.values())
        layer_counts = {
            key: int((analysis_payload.get("layers") or {}).get(key, {}).get("count") or 0)
            for key in primary_layer_keys
        }
        return {
            "status": str(analysis_payload.get("status") or "completed"),
            "snapshot_id": snapshot_id,
            "analysis_path": analysis_path,
            "candidate_layer_coverage": len(layer_map),
            "layer_map": layer_map,
            "layer_counts": layer_counts,
            "cumulative_layer_counts": dict(analysis_payload.get("cumulative_layer_counts") or {}),
            "ai_verification": dict(analysis_payload.get("ai_verification") or {}),
        }

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
                if request.target_company.strip().lower() == "anthropic" and _should_use_local_anthropic_assets(request):
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
                    output_payload={"summary": artifact.get("summary") or {}, "returned_matches": len(artifact.get("matches") or [])},
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
        diff_artifact_path = self.jobs_dir / f"criteria_diff_{baseline_job_id or 'none'}_{rerun_artifact['job_id']}.json"
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
        literal_text = normalized[len(prefix):].strip()
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
    base_execution_preferences = normalize_execution_preferences(merged, target_company=str(merged.get("target_company") or ""))
    patch_execution_preferences = normalize_execution_preferences(
        patch_payload,
        target_company=str(patch_payload.get("target_company") or merged.get("target_company") or ""),
    )
    merged_execution_preferences = merge_execution_preferences(patch_execution_preferences, base_execution_preferences)
    if explicit_execution_preferences:
        merged_execution_preferences = merge_execution_preferences(explicit_execution_preferences, merged_execution_preferences)
    if merged_execution_preferences:
        merged["execution_preferences"] = merged_execution_preferences
    for key in [
        "target_company",
        "target_scope",
        "query",
    ]:
        if _has_non_empty_value(merged.get(key)):
            continue
        candidate = patch_payload.get(key)
        if _has_non_empty_value(candidate):
            merged[key] = candidate
    if not _has_non_empty_value(merged.get("retrieval_strategy")):
        candidate_strategy = str(patch_payload.get("retrieval_strategy") or "").strip().lower()
        if candidate_strategy in {"structured", "hybrid", "semantic"}:
            merged["retrieval_strategy"] = candidate_strategy
    for key in [
        "categories",
        "employment_statuses",
    ]:
        if _has_non_empty_list(merged.get(key)):
            continue
        candidate = patch_payload.get(key)
        if _has_non_empty_list(candidate):
            merged[key] = list(candidate)
    merged["keywords"] = _merge_unique_string_values(
        merged.get("keywords"),
        patch_payload.get("keywords"),
        patch_payload.get("research_direction_keywords"),
        patch_payload.get("technology_keywords"),
        patch_payload.get("model_keywords"),
        patch_payload.get("product_keywords"),
    )
    merged["must_have_keywords"] = _merge_unique_string_values(
        merged.get("must_have_keywords"),
        patch_payload.get("must_have_keywords"),
    )
    merged["organization_keywords"] = _merge_unique_string_values(
        merged.get("organization_keywords"),
        patch_payload.get("organization_keywords"),
        patch_payload.get("team_keywords"),
        patch_payload.get("sub_org_keywords"),
        patch_payload.get("project_keywords"),
    )
    merged["must_have_facets"] = _merge_unique_string_values(
        merged.get("must_have_facets"),
        patch_payload.get("must_have_facets"),
    )
    merged["must_have_primary_role_buckets"] = _merge_unique_string_values(
        merged.get("must_have_primary_role_buckets"),
        patch_payload.get("must_have_primary_role_buckets"),
    )
    merged_scope = merged.get("scope_disambiguation")
    patch_scope = patch_payload.get("scope_disambiguation")
    if isinstance(patch_scope, dict) and patch_scope:
        if not isinstance(merged_scope, dict) or not merged_scope:
            merged["scope_disambiguation"] = dict(patch_scope)
        else:
            merged_scope_dict = dict(merged_scope)
            for key, value in patch_scope.items():
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


def _ensure_plan_company_scope(plan_payload: dict[str, Any], target_company: str) -> dict[str, Any]:
    normalized_target = str(target_company or "").strip()
    if not normalized_target:
        return dict(plan_payload or {})

    updated_plan = dict(plan_payload or {})
    updated_plan["target_company"] = normalized_target
    acquisition_strategy = dict(updated_plan.get("acquisition_strategy") or {})
    company_scope = [
        str(item).strip()
        for item in list(acquisition_strategy.get("company_scope") or [])
        if str(item).strip()
    ]
    if not company_scope:
        company_scope = [normalized_target]
    elif not any(item.lower() == normalized_target.lower() for item in company_scope):
        company_scope = [normalized_target, *company_scope]
    acquisition_strategy["company_scope"] = company_scope

    filter_hints = dict(acquisition_strategy.get("filter_hints") or {})
    current_companies = [
        str(item).strip()
        for item in list(filter_hints.get("current_companies") or [])
        if str(item).strip()
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
    workers: list[dict[str, Any]],
    results: list[dict[str, Any]],
    manual_review_items: list[dict[str, Any]],
    runtime_controls: dict[str, Any] | None = None,
) -> dict[str, Any]:
    job_summary = dict(job.get("summary") or {})
    milestones = _job_stage_milestones(events)
    worker_summary = _job_worker_summary(workers)
    latest_event = dict(events[-1]) if events else {}
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
        "latest_metrics": _extract_progress_metrics(job_summary, events, results, manual_review_items),
        "runtime_health": runtime_health,
        "counters": {
            "event_count": len(events),
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
            "result_count": len(results),
            "manual_review_count": len(manual_review_items),
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


def _job_stage_milestones(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        milestones.append(
            {
                "stage": stage,
                "status": str(latest.get("status") or ""),
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
    for control_key in ("hosted_runtime_watchdog", "shared_recovery", "job_recovery", "workflow_runner", "workflow_runner_control"):
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
    hosted_runtime_watchdog = dict(runtime_controls.get("hosted_runtime_watchdog") or {})
    shared_recovery = dict(runtime_controls.get("shared_recovery") or {})
    job_recovery = dict(runtime_controls.get("job_recovery") or {})
    runner_status = str(runner_control.get("status") or "").strip().lower()
    runner_pid = int(runner.get("pid") or 0)
    runner_alive = bool(runner.get("process_alive")) if "process_alive" in runner else (
        _workflow_runner_process_alive(runner_pid) if runner_pid > 0 else False
    )
    runner_attempted = bool(runner) or runner_status in {"started", "started_deferred", "failed"}
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
    hosted_runtime_watchdog_ready = (
        not _recovery_control_requires_ready(hosted_runtime_watchdog)
    ) or bool(hosted_runtime_watchdog.get("service_ready"))
    shared_recovery_ready = (not _recovery_control_requires_ready(shared_recovery)) or bool(shared_recovery.get("service_ready"))
    job_recovery_ready = (not _recovery_control_requires_ready(job_recovery)) or bool(job_recovery.get("service_ready"))
    recovery_ready = hosted_runtime_watchdog_ready or shared_recovery_ready or job_recovery_ready
    if runner and not runner_alive and status in {"queued", "running", "blocked"}:
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
            transition_state = str(dict(job.get("summary") or {}).get("stage2_transition_state") or "").strip() or "idle"
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


def _elapsed_seconds(started_at: str, ended_at: str) -> int:
    if not started_at or not ended_at:
        return 0
    start_dt = _parse_timestamp(started_at)
    end_dt = _parse_timestamp(ended_at)
    if start_dt is None or end_dt is None:
        return 0
    return max(int((end_dt - start_dt).total_seconds()), 0)


def _job_runtime_idle_seconds(job: dict[str, Any]) -> int:
    updated_at = str(job.get("updated_at") or "").strip()
    updated_dt = _parse_timestamp(updated_at)
    if updated_dt is None:
        return 0
    return max(int((datetime.now(timezone.utc) - updated_dt).total_seconds()), 0)


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
    for dict_key in ("normalization_scope", "investor_firm_plan"):
        value = state.get(dict_key)
        if isinstance(value, dict) and value:
            payload[dict_key] = dict(value)
    for bool_key in ("linkedin_stage_completed", "public_web_stage_completed", "reused_snapshot_checkpoint"):
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
        value = str(payload.get(path_key) or "").strip()
        if value:
            restored[path_key] = Path(value).expanduser()
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
    for dict_key in ("normalization_scope", "investor_firm_plan"):
        value = payload.get(dict_key)
        if isinstance(value, dict) and value:
            restored[dict_key] = dict(value)
    for bool_key in ("linkedin_stage_completed", "public_web_stage_completed", "reused_snapshot_checkpoint"):
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
    if identity is None or not snapshot_dir_value or not merged_path_value or not visible_path_value or not headless_path_value or not summary_path_value:
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
        page_summaries=[dict(item) for item in list(summary_payload.get("page_summaries") or []) if isinstance(item, dict)],
        accounts_used=[str(item or "") for item in list(summary_payload.get("accounts_used") or payload.get("accounts_used") or []) if str(item or "").strip()],
        errors=[str(item or "") for item in list(summary_payload.get("errors") or payload.get("errors") or []) if str(item or "").strip()],
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
        query_summaries=[dict(item) for item in list(summary_payload.get("query_summaries") or []) if isinstance(item, dict)],
        accounts_used=[str(item or "") for item in list(summary_payload.get("accounts_used") or payload.get("accounts_used") or []) if str(item or "").strip()],
        errors=[str(item or "") for item in list(summary_payload.get("errors") or payload.get("errors") or []) if str(item or "").strip()],
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


PARENT_COMPANY_ALIASES = {
    "google deepmind": ("Google", "Google DeepMind"),
    "deepmind": ("Google", "Google DeepMind"),
}


def _canonicalize_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    canonical = dict(payload or {})
    target_company = str(canonical.get("target_company") or "").strip()
    if not target_company:
        return canonical
    mapped = PARENT_COMPANY_ALIASES.get(target_company.lower())
    if not mapped:
        return canonical
    parent_company, org_label = mapped
    canonical["target_company"] = parent_company
    organization_keywords = [str(item or "").strip() for item in list(canonical.get("organization_keywords") or [])]
    merged_keywords: list[str] = []
    seen: set[str] = set()
    for item in [org_label, *organization_keywords]:
        normalized = " ".join(item.split()).strip()
        if not normalized or normalized.lower() == parent_company.lower():
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        merged_keywords.append(normalized)
    canonical["organization_keywords"] = merged_keywords
    return canonical


def _build_intent_rewrite_payload(
    *,
    request_payload: dict[str, Any] | None = None,
    instruction: str | None = None,
) -> dict[str, Any]:
    payload = {
        "request": _build_single_intent_rewrite_entry(_request_intent_text(request_payload)),
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
        str(item or "").strip().lower()
        for item in list(request.keywords or [])
        if str(item or "").strip()
    }
    normalized_facets = {
        str(item or "").strip().lower()
        for item in list(request.must_have_facets or [])
        if str(item or "").strip()
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


def _has_non_empty_value(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _has_non_empty_list(value: Any) -> bool:
    return isinstance(value, list) and any(str(item or "").strip() for item in value)


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
