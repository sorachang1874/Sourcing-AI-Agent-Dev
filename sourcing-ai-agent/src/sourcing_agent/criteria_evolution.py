from __future__ import annotations

from typing import Any

from .asset_catalog import AssetCatalog
from .domain import JobRequest
from .model_provider import ModelClient
from .planning import build_sourcing_plan
from .storage import ControlPlaneStore


class CriteriaEvolutionEngine:
    def __init__(self, catalog: AssetCatalog, store: ControlPlaneStore, model_client: ModelClient) -> None:
        self.catalog = catalog
        self.store = store
        self.model_client = model_client

    def recompile_after_feedback(self, feedback_payload: dict[str, Any], feedback_id: int) -> dict[str, Any]:
        base_version = self._resolve_base_version(feedback_payload)
        request_payload = self._resolve_request_payload(feedback_payload, base_version)
        if not request_payload:
            return {
                "status": "skipped",
                "reason": "No prior request or criteria version was available for recompilation.",
                "trigger_feedback_id": feedback_id,
            }

        request = JobRequest.from_payload(request_payload)
        if not request.target_company:
            target_company = str(feedback_payload.get("target_company") or "").strip()
            if not target_company and base_version:
                target_company = str(base_version.get("target_company") or "").strip()
            request = JobRequest.from_payload(
                {
                    **request_payload,
                    "target_company": target_company,
                }
            )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        patterns = self.store.list_criteria_patterns(target_company=request.target_company, status="")
        version = self.store.create_criteria_version(
            target_company=request.target_company,
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            patterns=patterns,
            source_kind="feedback_recompile",
            parent_version_id=int(base_version.get("version_id") or 0) if base_version else 0,
            trigger_feedback_id=feedback_id,
            evolution_stage="feedback_recompiled",
            notes="Auto-recompiled after human review feedback updated criteria patterns.",
        )
        compiler_run = self.store.record_criteria_compiler_run(
            version_id=version["version_id"],
            job_id=str(feedback_payload.get("job_id") or ""),
            trigger_feedback_id=feedback_id,
            provider_name=self.model_client.provider_name(),
            compiler_kind="feedback_recompile",
            status="completed",
            input_payload=request.to_record(),
            output_payload=plan.to_record(),
            notes="Triggered automatically after criteria feedback persistence.",
        )
        return {
            "status": "recompiled",
            "trigger_feedback_id": feedback_id,
            "base_version_id": int(base_version.get("version_id") or 0) if base_version else 0,
            "criteria_version_id": version["version_id"],
            "criteria_compiler_run_id": compiler_run["compiler_run_id"],
            "criteria_request_signature": version["request_signature"],
            "request": request.to_record(),
            "plan": plan.to_record(),
            "pattern_count": len(patterns),
        }

    def _resolve_base_version(self, feedback_payload: dict[str, Any]) -> dict[str, Any] | None:
        explicit_version_id = int(feedback_payload.get("criteria_version_id") or 0)
        target_company = str(feedback_payload.get("target_company") or "").strip()
        job_id = str(feedback_payload.get("job_id") or "").strip()
        if explicit_version_id:
            version = self.store.get_criteria_version(explicit_version_id)
            if version is not None:
                return version
        if job_id:
            job = self.store.get_job(job_id)
            if job is not None:
                request_payload = job.get("request") or {}
                target_company = str(request_payload.get("target_company") or target_company).strip()
        if target_company:
            return self.store.get_latest_criteria_version(target_company=target_company)
        return self.store.get_latest_criteria_version()

    def _resolve_request_payload(self, feedback_payload: dict[str, Any], base_version: dict[str, Any] | None) -> dict[str, Any]:
        explicit_request = feedback_payload.get("request") or feedback_payload.get("request_payload")
        if isinstance(explicit_request, dict) and explicit_request:
            return explicit_request
        job_id = str(feedback_payload.get("job_id") or "").strip()
        if job_id:
            job = self.store.get_job(job_id)
            if job is not None and isinstance(job.get("request"), dict):
                return job["request"]
        if base_version and isinstance(base_version.get("request"), dict):
            return base_version["request"]
        return {}
