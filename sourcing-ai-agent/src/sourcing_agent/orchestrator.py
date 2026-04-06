from __future__ import annotations

import json
from pathlib import Path
import threading
import uuid
from typing import Any

from .acquisition import AcquisitionEngine
from .agent_runtime import AgentRuntimeCoordinator
from .asset_catalog import AssetCatalog
from .confidence_policy import apply_policy_control, build_confidence_policy
from .criteria_evolution import CriteriaEvolutionEngine
from .domain import JobRequest
from .ingestion import load_bootstrap_bundle
from .manual_review import build_manual_review_items
from .manual_review_resolution import apply_manual_review_resolution
from .model_provider import DeterministicModelClient, ModelClient
from .pattern_suggestions import derive_pattern_suggestions
from .plan_review import apply_plan_review_decision, build_plan_review_gate
from .planning import build_sourcing_plan, hydrate_sourcing_plan
from .request_matching import baseline_selection_reason, request_family_signature, request_signature
from .result_diff import build_result_diff
from .rerun_policy import decide_rerun_policy
from .scoring import score_candidates
from .semantic_retrieval import rank_semantic_candidates
from .semantic_provider import SemanticProvider
from .service_daemon import WorkerDaemonService, read_service_status, render_systemd_unit
from .storage import SQLiteStore
from .worker_daemon import PersistentWorkerRecoveryDaemon
from .worker_scheduler import summarize_scheduler


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
        self.model_client = model_client
        self.semantic_provider = semantic_provider
        self.acquisition_engine = acquisition_engine
        self.criteria_evolution = CriteriaEvolutionEngine(catalog, store, model_client)
        self.agent_runtime = agent_runtime or AgentRuntimeCoordinator(store)
        self.acquisition_engine.worker_runtime = self.agent_runtime
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

    def plan_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = JobRequest.from_payload(payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        plan_review_gate = build_plan_review_gate(request, plan)
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
            **criteria_artifacts,
        }

    def start_workflow(self, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_workflow_plan(payload)
        if resolved.get("status") == "needs_plan_review":
            return resolved
        request = JobRequest.from_payload(dict(resolved.get("request") or {}))
        plan = hydrate_sourcing_plan(dict(resolved.get("plan") or {}))
        job_id = self._create_workflow_job(request, plan)
        criteria_artifacts = self._persist_criteria_artifacts(
            request=request,
            plan_payload=plan.to_record(),
            source_kind="workflow",
            compiler_kind="planning",
            job_id=job_id,
        )
        worker = threading.Thread(target=self._run_workflow, args=(job_id, request, plan), daemon=True)
        worker.start()
        return {
            "job_id": job_id,
            "status": "queued",
            "stage": "planning",
            "plan": plan.to_record(),
            "plan_review_session": resolved.get("plan_review_session") or {},
            **criteria_artifacts,
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
        snapshot = self.get_job_results(job_id)
        if snapshot is None:
            raise RuntimeError(f"Workflow {job_id} disappeared")
        return snapshot

    def run_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = JobRequest.from_payload(payload)
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
        }

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
        )
        return {
            "recoverable_workers": workers,
            "count": len(workers),
            "stale_after_seconds": int(payload.get("stale_after_seconds") or 180),
            "lane_id": str(payload.get("lane_id") or ""),
        }

    def run_worker_recovery_once(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        daemon = self._build_worker_recovery_daemon(payload)
        summary = daemon.run_once()
        return {"status": "completed", "daemon": summary}

    def run_worker_recovery_forever(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = dict(payload or {})
        daemon = self._build_worker_recovery_daemon(payload)
        summary = daemon.run_forever(
            poll_seconds=float(payload.get("poll_seconds") or 5.0),
            max_ticks=int(payload.get("max_ticks") or 0),
        )
        return {"status": "completed", "daemon": summary}

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
        )

    def _build_worker_daemon_service(self, payload: dict[str, Any]) -> WorkerDaemonService:
        return WorkerDaemonService(
            runtime_dir=self.jobs_dir.parent,
            recovery_callback=lambda daemon_payload: self.run_worker_recovery_once(daemon_payload),
            service_name=str(payload.get("service_name") or "worker-recovery-daemon"),
            owner_id=str(payload.get("owner_id") or ""),
            poll_seconds=float(payload.get("poll_seconds") or 5.0),
            lease_seconds=int(payload.get("lease_seconds") or 300),
            stale_after_seconds=int(payload.get("stale_after_seconds") or 180),
            total_limit=int(payload.get("total_limit") or 4),
        )

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

    def list_plan_review_sessions(self, target_company: str = "", status: str = "") -> dict[str, Any]:
        return {"plan_reviews": self.store.list_plan_review_sessions(target_company=target_company, status=status, limit=100)}

    def list_manual_review_items(self, target_company: str = "", job_id: str = "", status: str = "open") -> dict[str, Any]:
        return {
            "manual_review_items": self.store.list_manual_review_items(
                target_company=target_company,
                job_id=job_id,
                status=status,
                limit=200,
            )
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
            if str(review_session.get("status") or "") not in {"approved", "ready"}:
                return {
                    "status": "needs_plan_review",
                    "reason": "plan review session is not approved yet",
                    "plan_review_session": review_session,
                    "plan_review_gate": review_session.get("gate") or {},
                }
            return {
                "status": "ready",
                "request": dict(review_session.get("request") or {}),
                "plan": dict(review_session.get("plan") or {}),
                "plan_review_session": review_session,
            }

        request = JobRequest.from_payload(payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        plan_review_gate = build_plan_review_gate(request, plan)
        if bool(plan_review_gate.get("required_before_execution")) and not bool(payload.get("skip_plan_review")):
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
            }
        return {
            "status": "ready",
            "request": request.to_record(),
            "plan": plan.to_record(),
            "plan_review_session": {},
        }

    def _create_workflow_job(self, request: JobRequest, plan) -> str:
        job_id = uuid.uuid4().hex[:12]
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Workflow queued"},
        )
        self.store.append_job_event(
            job_id,
            stage="planning",
            status="queued",
            detail="Workflow created from user request.",
            payload={"target_company": request.target_company, "retrieval_strategy": plan.retrieval_plan.strategy},
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

            bootstrap_summary = None
            acquisition_state: dict[str, Any] = {
                "job_id": job_id,
                "plan_payload": plan.to_record(),
                "runtime_mode": "workflow",
            }
            current_stage = "acquiring"
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="running",
                stage=current_stage,
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                summary_payload={"message": "Running acquisition tasks"},
            )
            for task in plan.acquisition_tasks:
                if request.target_company.strip().lower() == "anthropic" and task.task_type == "acquire_full_roster":
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
                    input_payload={"task": task.to_record()},
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
                    return

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
            artifact = self._execute_retrieval(job_id, request, plan, job_type="workflow")
            self.store.update_agent_runtime_session_status(job_id, "completed")
            self.store.append_job_event(job_id, "completed", "completed", "Workflow completed.", artifact.get("summary", {}))
        except Exception as exc:
            failure_summary = {"error": str(exc)}
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="failed",
                stage="failed",
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                summary_payload=failure_summary,
            )
            self.store.update_agent_runtime_session_status(job_id, "failed")
            self.store.append_job_event(job_id, "failed", "failed", str(exc))

    def _execute_retrieval(
        self,
        job_id: str,
        request: JobRequest,
        plan,
        job_type: str,
        runtime_policy: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        runtime_policy = dict(runtime_policy or {})
        candidates = self.store.list_candidates()
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
                if request.target_company.strip().lower() == "anthropic":
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
    return ["role", "team", "focus_areas", "education", "work_history", "notes"]


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
