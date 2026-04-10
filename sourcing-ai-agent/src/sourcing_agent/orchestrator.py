from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import datetime
import fcntl
import json
import os
from pathlib import Path
import threading
import time
import uuid
from typing import Any

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .asset_catalog import AssetCatalog
from .candidate_artifacts import (
    CandidateArtifactError,
    build_company_candidate_artifacts,
    load_company_snapshot_candidate_documents,
)
from .company_asset_supplement import CompanyAssetSupplementManager
from .connectors import CompanyIdentity
from .confidence_policy import apply_policy_control, build_confidence_policy
from .criteria_evolution import CriteriaEvolutionEngine
from .domain import Candidate, EvidenceRecord, JobRequest, derive_candidate_facets, derive_candidate_role_bucket, normalize_candidate
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
from .query_intent_rewrite import interpret_query_intent_rewrite, summarize_query_intent_rewrite
from .request_matching import baseline_selection_reason, request_family_signature, request_signature
from .review_plan_instructions import compile_review_payload_from_instruction
from .result_diff import build_result_diff
from .rerun_policy import decide_rerun_policy
from .scoring import score_candidates
from .outreach_layering import analyze_company_outreach_layers
from .semantic_retrieval import rank_semantic_candidates
from .semantic_provider import SemanticProvider
from .service_daemon import WorkerDaemonService, read_service_status, render_systemd_unit
from .storage import SQLiteStore
from .worker_daemon import PersistentWorkerRecoveryDaemon
from .worker_scheduler import effective_worker_status, summarize_scheduler
from .execution_preferences import merge_execution_preferences, normalize_execution_preferences


_OUTREACH_LAYER_KEY_BY_INDEX = {
    0: "layer_0_roster",
    1: "layer_1_name_signal",
    2: "layer_2_greater_china_region_experience",
    3: "layer_3_mainland_china_experience_or_chinese_language",
}


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
        self.model_client = model_client
        self.semantic_provider = semantic_provider
        self.acquisition_engine = acquisition_engine
        self.criteria_evolution = CriteriaEvolutionEngine(catalog, store, model_client)
        self.agent_runtime = agent_runtime or AgentRuntimeCoordinator(store)
        self.acquisition_engine.worker_runtime = self.agent_runtime
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
            queued["job_recovery"] = self._start_job_scoped_recovery(job_id, payload)
            worker = threading.Thread(target=self.run_queued_workflow, kwargs={"job_id": job_id}, daemon=True)
            worker.start()
            return queued
        if workflow_status == "joined_existing_job":
            queued["job_recovery"] = self._start_job_scoped_recovery(job_id, payload)
            return queued
        if workflow_status == "reused_completed_job":
            queued["job_recovery"] = {"status": "not_needed"}
            return queued
        return queued

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
            job_id = self._create_workflow_job(request, plan, dispatch_context=dispatch_context)
            dispatch_payload = {
                "strategy": "new_job",
                "scope": str(dispatch.get("scope") or ""),
                "request_signature": str(dispatch.get("request_signature") or ""),
                "request_family_signature": str(dispatch.get("request_family_signature") or ""),
                "matched_job_id": "",
                "matched_job_status": "",
                "matched_job_stage": "",
                "requester_id": str(dispatch_context.get("requester_id") or ""),
                "tenant_id": str(dispatch_context.get("tenant_id") or ""),
                "idempotency_key": str(dispatch_context.get("idempotency_key") or ""),
            }
            self.store.record_query_dispatch(
                target_company=request.target_company,
                request_payload=request.to_record(),
                strategy="new_job",
                status="queued",
                source_job_id="",
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
                self._start_job_scoped_recovery(job_id, recovery_payload)

            job_status = str(job.get("status") or "")
            job_stage = str(job.get("stage") or "")
            request = JobRequest.from_payload(dict(job.get("request") or {}))
            plan = hydrate_sourcing_plan(dict(job.get("plan") or {}))
            if job_status == "blocked" and job_stage == "acquiring":
                run_result = self._resume_blocked_workflow_if_ready(job_id, assume_lock=True)
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
        if (
            str(latest_job.get("status") or "") == "blocked"
            and str(latest_job.get("stage") or "") == "acquiring"
        ):
            self.run_queued_workflow(
                job_id,
                recovery_payload={
                    "auto_job_daemon": True,
                    "job_recovery_poll_seconds": float(payload.get("job_recovery_poll_seconds") or 2.0),
                    "job_recovery_max_ticks": int(payload.get("job_recovery_max_ticks") or 900),
                    "job_recovery_lease_seconds": int(payload.get("job_recovery_lease_seconds") or 300),
                    "job_recovery_stale_after_seconds": int(payload.get("job_recovery_stale_after_seconds") or 2),
                    "job_recovery_total_limit": int(payload.get("job_recovery_total_limit") or 8),
                },
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
        return {
            "job": job,
            "events": self.store.list_job_events(job_id),
            "results": self.store.get_job_results(job_id),
            "manual_review_items": self.store.list_manual_review_items(job_id=job_id, status="", limit=100),
            "agent_runtime_session": self.store.get_agent_runtime_session(job_id=job_id) or {},
            "agent_trace_spans": self.store.list_agent_trace_spans(job_id=job_id),
            "agent_workers": self.agent_runtime.list_workers(job_id=job_id),
            "intent_rewrite": _build_intent_rewrite_payload(request_payload=dict(job.get("request") or {})),
        }

    def get_job_progress(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get_job(job_id)
        if job is None:
            return None
        events = self.store.list_job_events(job_id)
        workers = self.agent_runtime.list_workers(job_id=job_id)
        results = self.store.get_job_results(job_id)
        manual_review_items = self.store.list_manual_review_items(job_id=job_id, status="", limit=100)
        return _build_job_progress_payload(
            job=job,
            events=events,
            workers=workers,
            results=results,
            manual_review_items=manual_review_items,
        )

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

    def run_worker_recovery_once(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        daemon = self._build_worker_recovery_daemon(payload)
        summary = daemon.run_once()
        workflow_resume = self._resume_blocked_workflows_after_recovery(
            summary,
            explicit_job_id=str(payload.get("job_id") or ""),
        )
        post_completion_reconcile = self._reconcile_completed_workflows_after_recovery(
            summary,
            explicit_job_id=str(payload.get("job_id") or ""),
        )
        return {
            "status": "completed",
            "daemon": summary,
            "workflow_resume": workflow_resume,
            "post_completion_reconcile": post_completion_reconcile,
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
        service = self._build_worker_daemon_service(payload)
        summary = service.run_forever(max_ticks=int(payload.get("max_ticks") or 0))
        return {"status": "completed", "service": summary}

    def get_worker_daemon_status(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        service_name = str(payload.get("service_name") or "worker-recovery-daemon")
        return read_service_status(self.jobs_dir.parent, service_name)

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
        return PersistentWorkerRecoveryDaemon(
            store=self.store,
            agent_runtime=self.agent_runtime,
            acquisition_engine=self.acquisition_engine,
            owner_id=owner_id,
            lease_seconds=int(payload.get("lease_seconds") or 300),
            stale_after_seconds=int(payload.get("stale_after_seconds") or 180),
            total_limit=int(payload.get("total_limit") or 4),
            job_id=str(payload.get("job_id") or ""),
        )

    def _build_worker_daemon_service(self, payload: dict[str, Any]) -> WorkerDaemonService:
        callback_payload: dict[str, Any] = {}
        job_id = str(payload.get("job_id") or "").strip()
        if job_id:
            callback_payload["job_id"] = job_id
        return WorkerDaemonService(
            runtime_dir=self.jobs_dir.parent,
            recovery_callback=lambda daemon_payload: self.run_worker_recovery_once(daemon_payload),
            service_name=str(payload.get("service_name") or "worker-recovery-daemon"),
            owner_id=str(payload.get("owner_id") or ""),
            callback_payload=callback_payload,
            poll_seconds=float(payload.get("poll_seconds") or 5.0),
            lease_seconds=int(payload.get("lease_seconds") or 300),
            stale_after_seconds=int(payload.get("stale_after_seconds") or 180),
            total_limit=int(payload.get("total_limit") or 4),
        )

    def _start_job_scoped_recovery(self, job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not bool(payload.get("auto_job_daemon", False)):
            return {"status": "disabled"}

        service_name = str(payload.get("job_recovery_service_name") or f"job-recovery-{job_id}").strip()
        if not service_name:
            service_name = f"job-recovery-{job_id}"
        existing_status = read_service_status(self.runtime_dir, service_name)
        if str(existing_status.get("status") or "") in {"starting", "running"} and str(existing_status.get("lock_status") or "") == "locked":
            return {
                "status": "already_running",
                "service_name": service_name,
                "job_id": job_id,
                "poll_seconds": float(existing_status.get("poll_seconds") or payload.get("job_recovery_poll_seconds") or 2.0),
                "max_ticks": int(payload.get("job_recovery_max_ticks") or 900),
            }

        service_ref: dict[str, WorkerDaemonService] = {}
        poll_seconds = max(0.5, float(payload.get("job_recovery_poll_seconds") or 2.0))
        max_ticks = max(1, int(payload.get("job_recovery_max_ticks") or 900))
        service = WorkerDaemonService(
            runtime_dir=self.runtime_dir,
            recovery_callback=lambda daemon_payload: self._run_job_scoped_recovery_tick(
                job_id=job_id,
                daemon_payload=daemon_payload,
                service_ref=service_ref,
            ),
            service_name=service_name,
            poll_seconds=poll_seconds,
            lease_seconds=int(payload.get("job_recovery_lease_seconds") or 300),
            stale_after_seconds=int(payload.get("job_recovery_stale_after_seconds") or 2),
            total_limit=int(payload.get("job_recovery_total_limit") or 8),
            callback_payload={"job_id": job_id},
        )
        service_ref["service"] = service

        def _target() -> None:
            try:
                service.run_forever(max_ticks=max_ticks)
            except Exception:
                return

        threading.Thread(
            target=_target,
            name=f"{service_name}-thread",
            daemon=True,
        ).start()
        return {
            "status": "started",
            "service_name": service_name,
            "job_id": job_id,
            "poll_seconds": poll_seconds,
            "max_ticks": max_ticks,
            "stale_after_seconds": int(payload.get("job_recovery_stale_after_seconds") or 2),
            "total_limit": int(payload.get("job_recovery_total_limit") or 8),
            "scope": "job_scoped",
        }

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
        if force_fresh_run and bool(payload.get("respect_force_fresh", True)):
            allow_result_reuse = False
        return {
            "dispatch_enabled": bool(payload.get("query_dispatch_enabled", True)),
            "allow_join_inflight": bool(payload.get("allow_join_inflight", True)),
            "allow_result_reuse": allow_result_reuse,
            "scope": scope,
            "requester_id": requester_id,
            "tenant_id": tenant_id,
            "idempotency_key": idempotency_key,
            "request_signature": request_signature(request.to_record()),
            "request_family_signature": request_family_signature(request.to_record()),
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
        acquisition_state = self._build_acquisition_state(job_id, plan.to_record())
        current_stage = "acquiring"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage=current_stage,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Resuming acquisition tasks" if resume_mode else "Running acquisition tasks"},
        )
        self.store.update_agent_runtime_session_status(job_id, "running")
        if resume_mode:
            self.store.append_job_event(job_id, "acquiring", "running", "Resuming blocked workflow after worker recovery.")
        for task in plan.acquisition_tasks:
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
            if execution.status == "blocked" and task.blocking:
                summary = {
                    "message": execution.detail,
                    "blocked_task": task.task_type,
                }
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

        outreach_layering_summary = self._run_outreach_layering_after_acquisition(
            job_id=job_id,
            request=request,
            acquisition_state=acquisition_state,
        )
        if outreach_layering_summary:
            acquisition_state["outreach_layering"] = outreach_layering_summary

        current_stage = "retrieving"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage=current_stage,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Retrieving candidates"},
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
        self.store.update_agent_runtime_session_status(job_id, "completed")
        self.store.append_job_event(job_id, "retrieving", "completed", "Retrieval stage completed.", artifact.get("summary", {}))
        self.store.append_job_event(job_id, "completed", "completed", "Workflow completed.", artifact.get("summary", {}))
        return {
            "status": "completed",
            "artifact": artifact,
        }

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
    ) -> dict[str, Any]:
        if not _env_bool("OUTREACH_LAYERING_ENABLED", True):
            return {}
        target_company = str(request.target_company or "").strip()
        snapshot_id = str(acquisition_state.get("snapshot_id") or "").strip()
        if not target_company or not snapshot_id:
            return {}
        query_text = str(request.raw_user_request or request.query or "").strip()
        ai_requested = _env_bool("OUTREACH_LAYERING_ENABLE_AI", True)
        model_client: ModelClient | None = None
        if ai_requested and bool(getattr(self.model_client, "supports_outreach_ai_verification", lambda: False)()):
            model_client = self.model_client
        max_ai_verifications = _env_int("OUTREACH_LAYERING_MAX_AI_VERIFICATIONS", 0) if model_client else 0
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

    def _resume_blocked_workflows_after_recovery(
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
            results.append(self._resume_blocked_workflow_if_ready(job_id))
        return results

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
        updated_summary["background_reconcile"] = background_reconcile
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
            artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
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
            artifact_build = build_company_candidate_artifacts(
                runtime_dir=self.runtime_dir,
                store=self.store,
                target_company=request.target_company,
                snapshot_id=snapshot_id,
            )
            self.store.append_job_event(
                job_id,
                "completed",
                "running",
                "Background harvest profile prefetch rebuild refreshed retrieval artifacts.",
                payload={
                    "snapshot_id": snapshot_id,
                    "artifact_dir": str(artifact_build.get("artifact_dir") or ""),
                    "artifact_paths": dict(artifact_build.get("artifact_paths") or {}),
                },
            )
            retrieval_artifact = self._execute_retrieval(
                job_id,
                request,
                plan,
                job_type="workflow",
                runtime_policy={"workflow_snapshot_id": snapshot_id},
            )
            resume_result = {
                "status": "completed",
                "artifact": retrieval_artifact,
                "artifact_build": artifact_build,
            }
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
        background_reconcile["harvest_prefetch"] = harvest_prefetch_record
        updated_summary["background_reconcile"] = background_reconcile
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
            artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
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

    def _resume_blocked_workflow_if_ready(self, job_id: str, *, assume_lock: bool = False) -> dict[str, Any]:
        if not assume_lock:
            with self._job_run_lock(job_id) as lock_handle:
                if lock_handle is None:
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
        if not workers:
            return {"job_id": job_id, "status": "skipped", "reason": "no_workers"}
        pending_workers = [
            {
                "worker_id": int(worker.get("worker_id") or 0),
                "lane_id": str(worker.get("lane_id") or ""),
                "worker_key": str(worker.get("worker_key") or ""),
                "status": str(worker.get("status") or ""),
            }
            for worker in workers
            if str(worker.get("status") or "") != "completed"
        ]
        if pending_workers:
            return {
                "job_id": job_id,
                "status": "waiting",
                "reason": "pending_workers_remaining",
                "pending_worker_count": len(pending_workers),
                "pending_workers": pending_workers[:10],
            }

        request = JobRequest.from_payload(dict(job.get("request") or {}))
        plan = hydrate_sourcing_plan(dict(job.get("plan") or {}))
        try:
            resume_result = self._run_workflow_from_acquisition(job_id, request, plan, resume_mode=True)
            latest_job = self.store.get_job(job_id) or {}
            return {
                "job_id": job_id,
                "status": "resumed",
                "blocked_task": blocked_task,
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

    def _mark_workflow_failed(self, job_id: str, request: JobRequest, plan: Any, exc: Exception) -> None:
        existing = self.store.get_job(job_id) or {}
        if str(existing.get("status") or "") == "completed":
            return
        failure_summary = {"error": str(exc)}
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
        lock_path = self.job_locks_dir / f"{job_id}.lock"
        handle = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            yield None
            return
        try:
            yield handle
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()

    def _job_is_terminal(self, job: dict[str, Any] | None) -> bool:
        if not isinstance(job, dict):
            return False
        return str(job.get("status") or "") in {"completed", "failed"}

    def _execute_retrieval(
        self,
        job_id: str,
        request: JobRequest,
        plan,
        job_type: str,
        runtime_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        runtime_policy = dict(runtime_policy or {})
        candidate_source = self._load_retrieval_candidate_source(
            request,
            snapshot_id=str(runtime_policy.get("workflow_snapshot_id") or "").strip(),
        )
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
            filter_policy = _resolve_outreach_layer_filter_policy(request)
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
        if retrieval_strategy in {"hybrid", "semantic", "semantic_heavy"} and request.semantic_rerank_limit > 0:
            semantic_hits = rank_semantic_candidates(
                candidates,
                request,
                criteria_patterns=criteria_patterns,
                semantic_fields=_plan_semantic_fields(plan),
                limit=request.semantic_rerank_limit,
                semantic_provider=self.semantic_provider,
            )
        confidence_feedback = self.store.list_criteria_feedback(target_company=request.target_company, limit=500)
        confidence_policy = build_confidence_policy(
            target_company=request.target_company,
            feedback_items=confidence_feedback,
            request_payload=request.to_record(),
        )
        control = self.store.find_active_confidence_policy_control(
            target_company=request.target_company,
            request_payload=request.to_record(),
        )
        confidence_policy = apply_policy_control(confidence_policy, control)
        scored = score_candidates(
            candidates,
            request,
            criteria_patterns=criteria_patterns,
            confidence_policy=confidence_policy,
            semantic_hits=semantic_hits,
        )
        evidence_lookup: dict[str, list[dict[str, Any]]] = {}
        matches = []
        persisted_results = []
        for rank, item in enumerate(scored[: request.top_k], start=1):
            evidence = list(source_evidence_lookup.get(item.candidate.candidate_id) or [])
            if not evidence and str(candidate_source.get("source_kind") or "") != "company_snapshot":
                evidence = self.store.list_evidence(item.candidate.candidate_id)
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
        manual_review_items = build_manual_review_items(request, scored, evidence_lookup)
        summary_provider = self.model_client.provider_name()
        if runtime_policy.get("summary_mode") == "deterministic":
            summary_provider = "deterministic"
            summary_text = DeterministicModelClient().summarize(request, matches, len(scored))
        else:
            summary_text = self.model_client.summarize(request, matches, len(scored))
        summary = {
            "text": summary_text,
            "total_matches": len(scored),
            "returned_matches": len(matches),
            "retrieval_strategy": retrieval_strategy,
            "criteria_pattern_count": len(criteria_patterns),
            "summary_provider": summary_provider,
            "rerun_mode": runtime_policy.get("mode", "standard"),
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
            "status": "completed",
            "request": request.to_record(),
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
        artifact_path = self.jobs_dir / f"{job_id}.json"
        artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
        self.store.replace_job_results(job_id, persisted_results)
        self.store.replace_manual_review_items(job_id, manual_review_items)
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
        "keywords",
        "must_have_keywords",
        "organization_keywords",
        "must_have_facets",
        "must_have_primary_role_buckets",
    ]:
        if _has_non_empty_list(merged.get(key)):
            continue
        candidate = patch_payload.get(key)
        if _has_non_empty_list(candidate):
            merged[key] = list(candidate)
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
) -> dict[str, Any]:
    job_summary = dict(job.get("summary") or {})
    milestones = _job_stage_milestones(events)
    worker_summary = _job_worker_summary(workers)
    latest_event = dict(events[-1]) if events else {}
    started_at = str(job.get("created_at") or "")
    updated_at = str(job.get("updated_at") or "")
    return {
        "job_id": str(job.get("job_id") or ""),
        "status": str(job.get("status") or ""),
        "stage": str(job.get("stage") or ""),
        "started_at": started_at,
        "updated_at": updated_at,
        "elapsed_seconds": _elapsed_seconds(started_at, updated_at),
        "blocked_task": str(job_summary.get("blocked_task") or ""),
        "current_message": str(job_summary.get("message") or latest_event.get("detail") or ""),
        "progress": {
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
        },
    }


def _job_stage_milestones(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered_stages = ["planning", "acquiring", "retrieving", "completed", "failed"]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        stage = str(event.get("stage") or "").strip()
        if not stage:
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


def _extract_progress_metrics(
    job_summary: dict[str, Any],
    events: list[dict[str, Any]],
    results: list[dict[str, Any]],
    manual_review_items: list[dict[str, Any]],
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    interesting_keys = {
        "snapshot_id",
        "entry_count",
        "query_count",
        "queued_query_count",
        "queued_harvest_worker_count",
        "queued_exploration_count",
        "candidate_count",
        "evidence_count",
        "observed_company_candidate_count",
        "stop_reason",
    }
    for event in events:
        payload = dict(event.get("payload") or {})
        for key in interesting_keys:
            value = payload.get(key)
            if value is None or value == "" or value == []:
                continue
            metrics[key] = value
        if isinstance(payload.get("artifact_paths"), dict) and payload.get("artifact_paths"):
            metrics["artifact_paths"] = dict(payload.get("artifact_paths") or {})
    candidate_source = dict(job_summary.get("candidate_source") or {})
    if candidate_source:
        metrics["candidate_source"] = candidate_source
    metrics["result_count"] = len(results)
    metrics["manual_review_count"] = len(manual_review_items)
    if "manual_review_queue_count" in job_summary:
        metrics["manual_review_queue_count"] = int(job_summary.get("manual_review_queue_count") or 0)
    if "semantic_hit_count" in job_summary:
        metrics["semantic_hit_count"] = int(job_summary.get("semantic_hit_count") or 0)
    outreach_layering = dict(job_summary.get("outreach_layering") or {})
    if outreach_layering:
        metrics["outreach_layering"] = outreach_layering
    return metrics


def _elapsed_seconds(started_at: str, ended_at: str) -> int:
    if not started_at or not ended_at:
        return 0
    try:
        start_dt = datetime.strptime(started_at, "%Y-%m-%d %H:%M:%S")
        end_dt = datetime.strptime(ended_at, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return 0
    return max(int((end_dt - start_dt).total_seconds()), 0)


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


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _worker_has_background_candidate_output(worker: dict[str, Any]) -> bool:
    output = dict(worker.get("output") or {})
    if isinstance(output.get("candidate"), dict) and output.get("candidate"):
        return True
    evidence = output.get("evidence")
    return isinstance(evidence, list) and any(isinstance(item, dict) for item in evidence)


def _worker_has_completed_background_harvest_prefetch(worker: dict[str, Any]) -> bool:
    if str(worker.get("lane_id") or "") != "enrichment_specialist":
        return False
    if str(worker.get("status") or "") != "completed":
        return False
    metadata = dict(worker.get("metadata") or {})
    return str(metadata.get("recovery_kind") or "") == "harvest_profile_batch"


def _resolve_reconcile_snapshot_id(job_summary: dict[str, Any], workers: list[dict[str, Any]]) -> str:
    candidate_source = dict(job_summary.get("candidate_source") or {})
    snapshot_id = str(candidate_source.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    reconcile_state = dict(job_summary.get("background_reconcile") or {})
    snapshot_id = str(reconcile_state.get("snapshot_id") or "").strip()
    if snapshot_id:
        return snapshot_id
    for worker in workers:
        snapshot_dir = Path(str(dict(worker.get("metadata") or {}).get("snapshot_dir") or "")).expanduser()
        if snapshot_dir.name:
            return snapshot_dir.name
    return ""


def _merge_background_reconcile_candidate(existing: Candidate | None, incoming: Candidate) -> Candidate:
    if existing is None:
        return normalize_candidate(incoming)
    record = existing.to_record()
    incoming_record = incoming.to_record()
    existing_metadata = dict(existing.metadata or {})
    incoming_metadata = dict(incoming.metadata or {})
    membership_review_locked = str(existing.category or "").strip().lower() == "non_member" or bool(
        existing_metadata.get("membership_review_decision")
    )
    if str(incoming.category or "").strip():
        if not membership_review_locked or str(incoming.category or "").strip().lower() == "non_member":
            record["category"] = incoming.category
    for key, value in incoming_record.items():
        if key in {"candidate_id", "category", "metadata"}:
            continue
        if _has_present_field_value(value):
            record[key] = value
    merged_metadata = dict(existing_metadata)
    merged_metadata.update(incoming_metadata)
    record["metadata"] = merged_metadata
    return normalize_candidate(Candidate(**record))


def _has_present_field_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return value is not None


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
