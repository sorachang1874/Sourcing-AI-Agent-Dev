import contextlib
import io
import json
import os
import socket
import sqlite3
import tempfile
import threading
import time
import unittest
import unittest.mock
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import request as urllib_request

from sourcing_agent.acquisition import AcquisitionEngine, AcquisitionExecution
from sourcing_agent.api import _request_priority_lane, create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.asset_reuse_planning import (
    backfill_organization_asset_registry_for_company,
    build_acquisition_shard_registry_record,
    compile_asset_reuse_plan,
    ensure_organization_asset_registry,
    evaluate_organization_asset_registry_promotion,
)
from sourcing_agent.cli import run_server_runtime_watchdog_once
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.company_shard_planning import FORMER_FUNCTION_SHARD_PLAN_MARKER
from sourcing_agent.connectors import CompanyIdentity, CompanyRosterSnapshot
from sourcing_agent.domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.durable_runtime import (
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
    legacy_job_operation_id,
    legacy_job_workflow_run_id,
)
from sourcing_agent.enrichment import MultiSourceEnrichmentResult
from sourcing_agent.harvest_connectors import HarvestExecutionResult
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import (
    SourcingOrchestrator,
    _deserialize_acquisition_state_payload,
    _earliest_timestamp_string,
    _job_runtime_idle_seconds,
    _serialize_acquisition_state_payload,
)
from sourcing_agent.organization_assets import (
    ensure_acquisition_shard_bundles_for_snapshot,
    ensure_organization_completeness_ledger,
)
from sourcing_agent.planning import build_sourcing_plan, hydrate_sourcing_plan
from sourcing_agent.request_matching import matching_request_signature
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.service_daemon import WorkerDaemonService, read_service_status, read_service_stop_request
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from sourcing_agent.workflow_refresh import _worker_is_terminal_for_acquisition_resume


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = AssetCatalog.discover()
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.semantic_provider = LocalSemanticProvider()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=self.acquisition_engine,
        )
        self._runtime_env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        self._runtime_env_patcher.start()

    def tearDown(self) -> None:
        self._runtime_env_patcher.stop()
        last_error: OSError | None = None
        for _ in range(5):
            try:
                self.tempdir.cleanup()
                last_error = None
                break
            except OSError as exc:
                last_error = exc
                time.sleep(0.05)
        if last_error is not None:
            raise last_error

    def _write_company_snapshot_candidate_documents(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidates: list[dict[str, object]],
    ) -> tuple[Path, Path]:
        normalized_key = normalize_company_key(target_company)
        company_key = canonicalize_company_key(target_company) or normalized_key
        identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "aliases": [normalized_key] if normalized_key and normalized_key != company_key else [],
        }
        company_dir = Path(self.tempdir.name) / "company_assets" / company_key
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": target_company,
                        "snapshot_id": snapshot_id,
                        "company_identity": identity,
                    },
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "candidates": candidates,
                    "evidence": [],
                    "candidate_count": len(candidates),
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity,
                    "target_company": target_company,
                    "company_key": company_key,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "retrieval_index_summary.json").write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return snapshot_dir, candidate_doc_path

    def _write_snapshot_normalized_artifacts(
        self,
        *,
        snapshot_dir: Path,
        target_company: str,
        include_strict: bool = True,
        include_serving_docs: bool = False,
    ) -> None:
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": target_company,
            "candidate_count": 1,
        }
        (normalized_dir / "manifest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if include_serving_docs:
            serving_payload = {
                "target_company": target_company,
                "snapshot_id": snapshot_dir.name,
                "candidates": [],
                "evidence": [],
                "candidate_count": 0,
                "evidence_count": 0,
            }
            (normalized_dir / "materialized_candidate_documents.json").write_text(
                json.dumps(serving_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (normalized_dir / "reusable_candidate_documents.json").write_text(
                json.dumps(serving_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        if include_strict:
            strict_dir = normalized_dir / "strict_roster_only"
            strict_dir.mkdir(parents=True, exist_ok=True)
            (strict_dir / "manifest.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (strict_dir / "artifact_summary.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

    def _write_harvest_profile_raw(
        self,
        *,
        snapshot_dir: Path,
        profile_url: str,
        full_name: str,
        headline: str,
        current_company: str,
        experience: list[dict[str, object]] | None = None,
        avatar_url: str = "",
    ) -> Path:
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        slug = profile_url.rstrip("/").split("/")[-1] or "profile"
        raw_path = harvest_dir / f"{slug}.json"
        raw_path.write_text(
            json.dumps(
                {
                    "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                    "item": {
                        "fullName": full_name,
                        "profileUrl": profile_url,
                        "headline": headline,
                        "photoUrl": avatar_url,
                        "currentCompany": current_company,
                        "location": {"full": "San Francisco Bay Area"},
                        "experience": list(experience or []),
                        "education": [],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.repos.linkedin_profile_registry.mark_fetched(
            profile_url,
            raw_path=str(raw_path),
            source_jobs=["test-harvest-prefetch"],
            snapshot_dir=str(snapshot_dir),
        )
        return raw_path

    def _upsert_authoritative_org_registry(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidate_count: int,
        source_path: str,
        current_ready: bool,
        former_ready: bool,
        current_count: int,
        former_count: int,
        source_job_id: str = "",
        materialization_generation_key: str = "",
        materialization_generation_sequence: int = 0,
        materialization_watermark: str = "",
    ) -> dict[str, object]:
        return self.store.upsert_organization_asset_registry(
            {
                "target_company": target_company,
                "company_key": normalize_company_key(target_company),
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": candidate_count,
                "evidence_count": 0,
                "profile_detail_count": candidate_count,
                "explicit_profile_capture_count": candidate_count,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "standard_bundles": {"bundle_count": 1},
                "completeness_score": 100.0,
                "completeness_band": "high",
                "current_lane_coverage": {
                    "effective_candidate_count": current_count,
                    "effective_ready": current_ready,
                    "company_employees_current": {
                        "effective_candidate_count": current_count,
                        "effective_ready": current_ready,
                        "inferred_candidate_count": current_count,
                        "inferred_ready": current_ready,
                    },
                },
                "former_lane_coverage": {
                    "effective_candidate_count": former_count,
                    "effective_ready": former_ready,
                    "standard_bundle_ready_count": 1 if former_ready and former_count > 0 else 0,
                    "inferred_candidate_count": former_count,
                    "inferred_profile_detail_count": former_count,
                    "inferred_linkedin_url_count": former_count,
                    "profile_search_former": {
                        "effective_candidate_count": former_count,
                        "effective_ready": former_ready,
                        "standard_bundle_ready_count": 1 if former_ready and former_count > 0 else 0,
                        "inferred_candidate_count": former_count,
                        "inferred_profile_detail_count": former_count,
                        "inferred_linkedin_url_count": former_count,
                    },
                },
                "current_lane_effective_candidate_count": current_count,
                "former_lane_effective_candidate_count": former_count,
                "current_lane_effective_ready": current_ready,
                "former_lane_effective_ready": former_ready,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {"selected_snapshot_ids": [snapshot_id]},
                "source_path": source_path,
                "source_job_id": source_job_id,
                "materialization_generation_key": materialization_generation_key,
                "materialization_generation_sequence": materialization_generation_sequence,
                "materialization_watermark": materialization_watermark,
                "summary": {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "candidate_count": candidate_count,
                    "profile_detail_count": candidate_count,
                    "standard_bundles": {"bundle_count": 1},
                    "current_lane_coverage": {
                        "effective_candidate_count": current_count,
                        "effective_ready": current_ready,
                    },
                    "former_lane_coverage": {
                        "effective_candidate_count": former_count,
                        "effective_ready": former_ready,
                    },
                },
            },
            authoritative=True,
        )

    def _register_materialization_generation(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        artifact_kind: str,
        artifact_key: str,
        candidates: list[dict[str, object]],
        lane: str = "",
        employment_scope: str = "",
    ) -> dict[str, object]:
        company_key = normalize_company_key(target_company)
        members = []
        for candidate in list(candidates or []):
            candidate_id = str(candidate.get("candidate_id") or "").strip()
            linkedin_url = str(candidate.get("linkedin_url") or "").strip()
            profile_url_key = normalize_linkedin_profile_url_key(linkedin_url) if linkedin_url else ""
            member_key = profile_url_key or candidate_id
            if not member_key:
                continue
            members.append(
                {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "artifact_kind": artifact_kind,
                    "artifact_key": artifact_key,
                    "lane": lane,
                    "employment_scope": employment_scope or str(candidate.get("employment_status") or ""),
                    "member_key": member_key,
                    "member_key_kind": "profile_url_key" if profile_url_key else "candidate_id",
                    "candidate_id": candidate_id,
                    "profile_url_key": profile_url_key,
                }
            )
        return self.store.register_asset_materialization(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            artifact_kind=artifact_kind,
            artifact_key=artifact_key,
            source_path=str(
                Path(self.tempdir.name)
                / "company_assets"
                / company_key
                / snapshot_id
                / f"{artifact_kind}_{artifact_key}.json"
            ),
            summary={
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "artifact_kind": artifact_kind,
                "artifact_key": artifact_key,
                "candidate_count": len(candidates),
            },
            metadata={"test_case": "materialization_generation"},
            members=members,
        )

    def test_two_stage_workflow_publishes_stage1_preview_and_continues_public_web_stage2_when_enabled(self) -> None:
        snapshot_id = "20260411T130000"
        snapshot_dir = self.settings.company_assets_dir / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        company_identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
        )
        candidate = Candidate(
            candidate_id="acme_infra_1",
            name_en="Taylor Infra",
            display_name="Taylor Infra",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="infra platform systems",
            linkedin_url="https://www.linkedin.com/in/taylor-infra/",
        )
        request_payload = {
            "raw_user_request": "给我 Acme 的 Infra 方向成员",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "semantic_rerank_limit": 0,
            "analysis_stage_mode": "two_stage",
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = self.orchestrator._create_workflow_job(request, plan)
        retrieval_calls: list[dict[str, object]] = []

        def fake_execute_task(
            task: AcquisitionTask,
            _request: JobRequest,
            _target_company: str,
            _state: dict[str, object],
            _bootstrap_summary: dict[str, object] | None,
        ) -> AcquisitionExecution:
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved company identity.",
                    payload={"snapshot_id": snapshot_id},
                    state_updates={
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "company_identity": company_identity,
                    },
                )
            if task.task_type == "enrich_linkedin_profiles":
                candidate_doc_path = snapshot_dir / "candidate_documents.json"
                linkedin_stage_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
                candidate_doc_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                linkedin_stage_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built LinkedIn stage-1 candidate documents.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_candidate_doc_path": linkedin_stage_path,
                        "linkedin_stage_completed": True,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )
            if task.task_type == "enrich_public_web_signals":
                candidate_doc_path = snapshot_dir / "candidate_documents.json"
                public_web_stage_path = snapshot_dir / "candidate_documents.public_web_stage_2.json"
                candidate_doc_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                public_web_stage_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Extended candidate documents with public-web stage-2 evidence.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                        "public_web_stage_candidate_doc_path": public_web_stage_path,
                        "public_web_stage_completed": True,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )
            if task.task_type == "normalize_asset_snapshot":
                manifest_path = snapshot_dir / "manifest.json"
                manifest_path.write_text(json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Normalized snapshot.",
                    payload={"manifest_path": str(manifest_path)},
                    state_updates={"manifest_path": manifest_path},
                )
            if task.task_type == "build_retrieval_index":
                index_path = snapshot_dir / "retrieval_index_summary.json"
                index_path.write_text(json.dumps({"status": "completed"}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built retrieval index.",
                    payload={"retrieval_index_summary": str(index_path)},
                    state_updates={},
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=f"Completed {task.task_type}.",
                payload={},
                state_updates={},
            )

        def fake_execute_retrieval(
            current_job_id: str,
            current_request: JobRequest,
            current_plan,
            job_type: str,
            runtime_policy: dict[str, object] | None = None,
            candidate_source_override: dict[str, object] | None = None,
            *,
            persist_job_state: bool = True,
            artifact_name_suffix: str = "",
            artifact_status: str = "completed",
        ) -> dict[str, object]:
            runtime_policy = dict(runtime_policy or {})
            analysis_stage = str(runtime_policy.get("analysis_stage") or "stage_2_final")
            current_job = self.store.get_job(current_job_id) or {}
            retrieval_calls.append(
                {
                    "analysis_stage": analysis_stage,
                    "job_stage": str(current_job.get("stage") or ""),
                    "job_status": str(current_job.get("status") or ""),
                    "persist_job_state": persist_job_state,
                }
            )
            summary = {
                "text": f"{analysis_stage} summary",
                "total_matches": 1,
                "returned_matches": 1,
                "manual_review_queue_count": 0,
                "analysis_stage": analysis_stage,
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "candidate_count": 1,
                },
                "outreach_layering": {"status": "completed", "analysis_stage": analysis_stage},
            }
            artifact_path = self.settings.jobs_dir / (
                f"{current_job_id}.json"
                if not artifact_name_suffix
                else f"{current_job_id}.{artifact_name_suffix}.json"
            )
            artifact = {
                "job_id": current_job_id,
                "status": artifact_status,
                "request": current_request.to_record(),
                "plan": current_plan.to_record(),
                "summary": summary,
                "matches": [],
                "manual_review_items": [],
                "artifact_path": str(artifact_path),
            }
            self.store.replace_job_results(
                current_job_id,
                [
                    {
                        "candidate_id": candidate.candidate_id,
                        "rank": 1,
                        "score": 1.0,
                        "semantic_score": 0.0,
                        "confidence_label": "high",
                        "confidence_score": 1.0,
                        "confidence_reason": "test",
                        "explanation": f"{analysis_stage} explanation",
                        "matched_fields": ["focus_areas"],
                        "outreach_layer": 0,
                        "outreach_layer_key": "layer_0_roster",
                        "outreach_layer_source": "rules",
                    }
                ],
            )
            self.store.repos.manual_review.replace_items(current_job_id, [])
            if persist_job_state:
                self.store.save_job(
                    job_id=current_job_id,
                    job_type=job_type,
                    status="completed",
                    stage="completed",
                    request_payload=current_request.to_record(),
                    plan_payload=current_plan.to_record(),
                    summary_payload=summary,
                    artifact_path=str(artifact_path),
                )
            return artifact

        layering_calls: list[dict[str, object]] = []

        def fake_run_outreach_layering_after_acquisition(
            *,
            job_id: str,
            request: JobRequest,
            acquisition_state: dict[str, object],
            allow_ai: bool | None = None,
            allow_background_defer: bool | None = None,
            analysis_stage_label: str = "",
            event_stage: str = "",
        ) -> dict[str, object]:
            current_job = self.store.get_job(job_id) or {}
            layering_calls.append(
                {
                    "analysis_stage": analysis_stage_label,
                    "job_stage": str(current_job.get("stage") or ""),
                    "job_status": str(current_job.get("status") or ""),
                    "event_stage": event_stage,
                    "allow_ai": bool(allow_ai),
                    "allow_background_defer": bool(allow_background_defer),
                }
            )
            return {"status": "completed", "analysis_stage": analysis_stage_label or "stage_2_final"}

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "execute_task",
                side_effect=fake_execute_task,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                side_effect=fake_run_outreach_layering_after_acquisition,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                side_effect=fake_execute_retrieval,
            ),
        ):
            run_result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(run_result["status"], "completed")
        self.assertEqual(
            [str(item["analysis_stage"]) for item in retrieval_calls],
            ["stage_1_preview", "stage_2_final"],
        )
        self.assertEqual(
            [str(item["analysis_stage"]) for item in layering_calls],
            ["stage_2_final"],
        )
        self.assertEqual(layering_calls[0]["job_stage"], "retrieving")
        self.assertEqual(layering_calls[0]["event_stage"], "retrieving")
        self.assertEqual(retrieval_calls[0]["job_stage"], "acquiring")
        self.assertEqual(retrieval_calls[1]["job_stage"], "retrieving")

        snapshot = self.orchestrator.get_job_results(job_id)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertFalse(snapshot["job"]["summary"].get("awaiting_user_action"))
        self.assertEqual(snapshot["job"]["summary"]["stage1_preview"]["status"], "ready")
        self.assertEqual(snapshot["job"]["summary"]["public_web_stage_2"]["status"], "completed")
        event_details = [str(item.get("detail") or "") for item in snapshot["events"]]
        self.assertIn("LinkedIn Stage 1 acquisition completed.", event_details)
        self.assertIn("Stage 1 preview ready; continuing Public Web Stage 2 acquisition.", event_details)
        self.assertIn("Public Web Stage 2 acquisition completed.", event_details)

    def test_queue_workflow_delta_dispatch_label_uses_baseline_context_when_match_misses(self) -> None:
        snapshot_id = "20260413T030404"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_current_dispatch",
                    name_en="Dispatch Current",
                    display_name="Dispatch Current",
                    category="employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="current",
                    role="Research Scientist",
                    focus_areas="multimodal veo",
                    linkedin_url="https://www.linkedin.com/in/google-dispatch-current/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_google_former_dispatch",
                    name_en="Dispatch Former",
                    display_name="Dispatch Former",
                    category="employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Research Scientist",
                    focus_areas="video generation",
                    linkedin_url="https://www.linkedin.com/in/google-dispatch-former/",
                ).to_record(),
            ],
        )
        normalized_dir = candidate_doc_path.parent / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Google",
                    "company_key": "google",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 3724,
                    "evidence_count": 0,
                    "profile_detail_count": 3724,
                    "explicit_profile_capture_count": 3724,
                    "missing_linkedin_count": 0,
                    "manual_review_backlog_count": 0,
                    "profile_completion_backlog_count": 0,
                    "source_snapshot_count": 1,
                    "current_lane_coverage": {
                        "effective_candidate_count": 3710,
                        "effective_ready": True,
                    },
                    "former_lane_coverage": {
                        "effective_candidate_count": 14,
                        "effective_ready": True,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        registry_row = self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=3724,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=3710,
            former_count=14,
        )

        original_resolve = self.orchestrator._resolve_query_dispatch_decision

        def _force_new_job_dispatch(request, context):  # noqa: ANN001
            return {
                "strategy": "new_job",
                "scope": str(context.get("scope") or "global"),
                "request_signature": str(context.get("request_signature") or ""),
                "request_family_signature": str(context.get("request_family_signature") or ""),
                "matched_job": {},
            }

        self.orchestrator._resolve_query_dispatch_decision = _force_new_job_dispatch
        try:
            queued = self.orchestrator.queue_workflow(
                {
                    "raw_user_request": "帮我找Google做多模态方向的人（包括Veo和Nano Banana组）",
                    "target_company": "Google",
                    "categories": ["employee"],
                    "top_k": 10,
                    "skip_plan_review": True,
                }
            )
        finally:
            self.orchestrator._resolve_query_dispatch_decision = original_resolve

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "delta_from_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_id)
        self.assertEqual(int(queued["dispatch"]["matched_registry_id"] or 0), int(registry_row["registry_id"] or 0))
        self.assertEqual(queued["dispatch"]["reuse_basis"], "organization_asset_registry_lane_coverage")
        self.assertEqual(
            queued["dispatch"]["request_family_match_explanation"]["matched_registry_snapshot_id"],
            snapshot_id,
        )
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertEqual(execution_preferences.get("delta_baseline_snapshot_id"), snapshot_id)

    def test_queue_workflow_suppresses_inherited_force_fresh_when_effective_baseline_ready(self) -> None:
        snapshot_id = "20260413T020202"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_current",
                    name_en="Vision Engineer",
                    display_name="Vision Engineer",
                    category="employee",
                    target_company="Google",
                    organization="Google DeepMind",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="multimodal Veo vision-language",
                    linkedin_url="https://www.linkedin.com/in/google-vision-engineer/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_google_former",
                    name_en="Video Researcher",
                    display_name="Video Researcher",
                    category="former_employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Research Scientist",
                    focus_areas="video generation multimodal",
                    linkedin_url="https://www.linkedin.com/in/google-video-researcher/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=2,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=1,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要 Google 公司全量成员，重新跑，不要高成本。",
                "target_company": "Google",
            }
        )
        self.assertTrue(plan_result["request"]["execution_preferences"]["force_fresh_run"])

        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        queued = self.orchestrator.queue_workflow(
            {
                "plan_review_id": review_id,
                "runtime_execution_mode": "hosted",
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "reuse_snapshot")
        self.assertEqual(
            dict(queued["dispatch"].get("force_fresh_run_suppressed") or {}).get("reason"),
            "effective_baseline_ready",
        )
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertNotIn("force_fresh_run", execution_preferences)
        self.assertEqual(execution_preferences.get("reuse_snapshot_id"), snapshot_id)
        self.assertTrue(execution_preferences.get("reuse_existing_roster"))

    def test_queue_workflow_rebuilds_google_keyword_shard_plan_from_approved_review_request(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "给我 Google 负责多模态和 Veo 的研究员",
                "target_company": "Google",
                "keywords": ["multimodal", "Veo", "Nano Banana"],
                "execution_preferences": {
                    "use_company_employees_lane": True,
                    "confirmed_company_scope": ["Google", "Google DeepMind"],
                },
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        acquire_task = next(
            task for task in queued["plan"]["acquisition_tasks"] if task["task_type"] == "acquire_full_roster"
        )
        shard_policy = dict(acquire_task["metadata"].get("company_employee_shard_policy") or {})

        self.assertEqual(
            acquire_task["metadata"]["company_employee_shard_strategy"], "adaptive_large_org_keyword_probe"
        )
        self.assertEqual(shard_policy.get("mode"), "keyword_union")
        self.assertTrue(shard_policy.get("force_keyword_shards"))
        self.assertEqual(
            shard_policy.get("root_filters", {}).get("function_ids"),
            ["8", "9", "19", "24"],
        )
        self.assertTrue(
            any("Nano Banana" in query for query in acquire_task["metadata"].get("search_seed_queries") or [])
        )
        self.assertFalse(
            any("Researcher" in query for query in acquire_task["metadata"].get("search_seed_queries") or [])
        )

    def test_server_runtime_watchdog_bootstraps_stale_shared_recovery(self) -> None:
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_hosted_runtime_watchdog_once",
            return_value={
                "status": "completed",
                "mode": "hosted",
                "worker_recovery": {"status": "completed"},
                "hosted_dispatch": [],
            },
        ) as hosted_mock:
            result = run_server_runtime_watchdog_once(self.orchestrator)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["mode"], "hosted")
        hosted_mock.assert_called_once()
        hosted_payload = dict(hosted_mock.call_args[0][0] or {})
        self.assertEqual(hosted_payload["hosted_runtime_watchdog_service_name"], "server-runtime-watchdog")
        self.assertEqual(hosted_payload["shared_service_name"], "worker-recovery-daemon")

    def test_run_worker_recovery_once_publishes_stage1_preview_before_post_profile_tail(self) -> None:
        class _FakeDaemon:
            def run_once(self) -> dict[str, object]:
                return {
                    "owner_id": "stage1-preview-bridge",
                    "recoverable_count": 0,
                    "claimed_count": 0,
                    "executed_count": 0,
                    "jobs": [],
                }

        request_payload = {
            "raw_user_request": "Find Google Gemini former researchers",
            "target_company": "Google",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["former"],
            "keywords": ["Gemini"],
            "top_k": 10,
            "execution_preferences": {"delta_baseline_snapshot_id": "20260511T000000"},
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_stage1_preview_bridge_tail_open"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id="snapshot-google-stage1-preview-bridge",
            candidates=[
                Candidate(
                    candidate_id="cand_google_gemini_bridge",
                    name_en="Google Gemini Bridge",
                    display_name="Google Gemini Bridge",
                    category="former_employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Gemini Research Scientist",
                    linkedin_url="https://www.linkedin.com/in/google-gemini-bridge/",
                ).to_record()
            ],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={
                "message": "Waiting for post-profile materialization.",
                "acquisition_progress": {
                    "latest_state": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": str(snapshot_dir),
                        "candidate_doc_path": str(candidate_doc_path),
                    }
                },
            },
        )
        self.store.upsert_job_result_lifecycle(
            job_id=job_id,
            fields={
                "target_company": "Google",
                "current_snapshot_id": snapshot_dir.name,
                "projection_source_snapshot_id": snapshot_dir.name,
                "baseline_snapshot_id": "20260511T000000",
                "served_candidate_count": 0,
                "expected_candidate_count": 1,
                "delta_profile_progress_applicable": True,
                "delta_profile_required_count": 1,
                "delta_profile_fetched_count": 0,
                "stage1_former_search_returned_count": 1,
                "stage1_deduped_candidate_count": 1,
                "stage1_deduped_profile_url_count": 1,
                "stage1_profile_fetch_required_count": 1,
                "stage1_profile_fetched_count": 0,
                "metadata": {
                    "delta_profile_denominator_promoted": True,
                    "stage1_terminal_promoted_at": "2026-05-13T00:00:00+00:00",
                },
                "source_validation_status": "validated",
            },
        )
        self.store.upsert_job_materialization_item(
            item_id="local_apply_stage1_preview_bridge_tail_open",
            job_id=job_id,
            target_company="Google",
            snapshot_id=snapshot_dir.name,
            item_kind="local_apply_closure",
            source="worker_completion_event",
            reason="harvest_profile_batch_completed_needs_local_apply_closure",
            status="queued",
            phase="queued",
            source_worker_ids=[881],
            metadata={"recovery_kind": "harvest_profile_batch", "snapshot_dir": str(snapshot_dir)},
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_build_worker_recovery_daemon",
                return_value=_FakeDaemon(),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_search_seed_discovery_query_queue_once",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_profile_prefetch_refill_queue_once",
                return_value={"status": "idle", "dispatched_url_count": 0, "queued_worker_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_local_apply_backlog_drain_once",
                return_value={"status": "idle", "claimed_count": 0, "completed_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_drain_event_level_materialization_for_job",
                return_value={"status": "idle", "claimed_count": 0, "completed_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_board_visible_apply_queue_once",
                return_value={"status": "idle", "claimed_count": 0, "completed_count": 0},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_snapshot_full_materialization_queue_once",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_recover_stale_excel_intake_jobs_once",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_remote_event_followup_rounds",
                side_effect=lambda **kwargs: (
                    kwargs["summary"],
                    kwargs["workflow_resume"],
                    kwargs["post_completion_reconcile"],
                    {"status": "skipped", "reason": "unit"},
                ),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_emit_runtime_heartbeats_after_recovery",
                return_value={"status": "skipped", "reason": "unit"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_resume_blocked_workflows_after_recovery",
                side_effect=AssertionError("Stage 1 preview bridge must not resume finalization"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_reconcile_completed_workflows_after_recovery",
                side_effect=AssertionError("Stage 1 preview bridge must not reconcile completion"),
            ),
        ):
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": job_id,
                    "explicit_job_followup_rounds": 0,
                    "search_seed_discovery_enabled": False,
                    "snapshot_full_materialization_enabled": False,
                    "excel_intake_recovery_enabled": False,
                    "post_recovery_housekeeping_enabled": False,
                    "workflow_auto_resume_enabled": True,
                    "workflow_resume_explicit_job": True,
                    "workflow_queue_auto_takeover_enabled": False,
                    "post_completion_reconcile_enabled": True,
                }
            )

        bridge = dict(recovery.get("stage1_preview_recovery") or {})
        self.assertEqual(str(bridge.get("status") or ""), "completed")
        self.assertEqual(recovery["workflow_resume"], [])
        latest_job = self.store.get_job(job_id) or {}
        self.assertEqual(str(latest_job.get("status") or ""), "blocked")
        summary = dict(latest_job.get("summary") or {})
        stage1_preview = dict(summary.get("stage1_preview") or {})
        self.assertEqual(str(stage1_preview.get("status") or ""), "ready")
        self.assertTrue(str(stage1_preview.get("artifact_path") or ""))
        terminal_resume = dict(summary.get("terminal_stage_artifact_resume") or {})
        self.assertTrue(bool(terminal_resume.get("preview_only_recovery")))

    def test_execute_segmented_harvest_company_roster_workers_emits_inline_callback_for_completed_local_shards(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find MiroMind.ai people",
            "target_company": "MiroMind.ai",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_segmented_inline_company_roster_callback"
        snapshot_dir = self.settings.company_assets_dir / "miromindai" / "snapshot-segmented-inline-callback"
        shard_snapshot_dir = snapshot_dir / "harvest_company_employees" / "shards" / "us_core"
        shard_harvest_dir = shard_snapshot_dir / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="MiroMind.ai",
            canonical_name="MiroMind.ai",
            company_key="miromindai",
            linkedin_slug="miromind-ai",
            linkedin_company_url="https://www.linkedin.com/company/miromind-ai/",
        )
        dataset_items_path = shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json"
        dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "id": "miromind_member_1",
                        "linkedinUrl": "https://www.linkedin.com/in/miromind-member-1/",
                        "firstName": "Mira",
                        "lastName": "Agent",
                        "summary": "Research Engineer at MiroMind.ai",
                        "currentPositions": [
                            {
                                "companyName": "MiroMind.ai",
                                "title": "Research Engineer",
                                "current": True,
                            }
                        ],
                        "location": {"linkedinText": "San Francisco Bay Area"},
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (shard_harvest_dir / "harvest_company_employees_queue_summary.json").write_text(
            json.dumps(
                {
                    "logical_name": "harvest_company_employees",
                    "company_identity": identity.to_record(),
                    "status": "completed",
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {"locations": ["United States"]},
                    "artifact_paths": {"dataset_items": str(dataset_items_path)},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        completed_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::miromindai::us_core",
            stage="acquiring",
            span_name="harvest_company_employees:miromindai:us_core",
            budget_payload={"max_pages": 1, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(shard_snapshot_dir),
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
                "company_filters": {"locations": ["United States"]},
                "max_pages": 1,
                "page_limit": 25,
            },
            handoff_from_lane="triage_planner",
        )
        queued_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::miromindai::global_rest",
            stage="acquiring",
            span_name="harvest_company_employees:miromindai:global_rest",
            budget_payload={"max_pages": 4, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir / "harvest_company_employees" / "shards" / "global_rest"),
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
                "company_filters": {},
                "max_pages": 4,
                "page_limit": 25,
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            completed_worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
            output_payload={
                "summary": {
                    "company_identity": identity.to_record(),
                    "status": "completed",
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {"locations": ["United States"]},
                    "snapshot_dir": str(shard_snapshot_dir),
                    "root_snapshot_dir": str(snapshot_dir),
                    "shard_id": "us_core",
                    "title": "United States",
                    "strategy_id": "small_org_roster",
                }
            },
        )
        self.orchestrator.agent_runtime.complete_worker(
            queued_worker,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest", "recovery_kind": "harvest_company_employees"},
            output_payload={"summary": {"status": "queued"}},
        )

        completed_result = {
            "worker_id": completed_worker.worker_id,
            "worker_status": "completed",
            "summary": {
                "company_identity": identity.to_record(),
                "status": "completed",
                "snapshot_dir": str(shard_snapshot_dir),
                "root_snapshot_dir": str(snapshot_dir),
            },
        }
        queued_result = {
            "worker_id": queued_worker.worker_id,
            "worker_status": "queued",
            "summary": {
                "company_identity": identity.to_record(),
                "status": "queued",
                "snapshot_dir": str(snapshot_dir / "harvest_company_employees" / "shards" / "global_rest"),
                "root_snapshot_dir": str(snapshot_dir),
            },
        }

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_execute_harvest_company_roster_worker",
                side_effect=[completed_result, queued_result],
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={"status": "queued", "queued_worker_count": 1},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=AssertionError("segmented inline callback should defer sync while same-kind worker remains queued"),
            ),
        ):
            summary = self.acquisition_engine._execute_segmented_harvest_company_roster_workers(
                identity=identity,
                snapshot_dir=snapshot_dir,
                shards=[
                    {
                        "shard_id": "us_core",
                        "title": "United States",
                        "strategy_id": "small_org_roster",
                        "company_filters": {"locations": ["United States"]},
                        "max_pages": 1,
                        "page_limit": 25,
                    },
                    {
                        "shard_id": "global_rest",
                        "title": "Global",
                        "strategy_id": "small_org_roster",
                        "company_filters": {},
                        "max_pages": 4,
                        "page_limit": 25,
                    },
                ],
                job_id=job_id,
                request_payload=request.to_record(),
                plan_payload=plan_payload,
                runtime_mode="workflow",
                allow_shared_provider_cache=True,
            )

        self.assertEqual(int(summary.get("queued_count") or 0), 1)
        self.assertEqual(int(summary.get("completed_count") or 0), 1)
        self.assertFalse((snapshot_dir / "candidate_documents.json").exists())
        refreshed_worker = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert refreshed_worker is not None
        output = dict(refreshed_worker.get("output") or {})
        self.assertFalse(dict(output.get("inline_incremental_ingest") or {}))
        closure_items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
            statuses=["queued"],
        )
        self.assertEqual(len(closure_items), 1)
        self.assertEqual(closure_items[0]["source_worker_ids"], [completed_worker.worker_id])
        self.assertEqual(closure_items[0]["metadata"]["worker_kind"], "company_roster")
        self.assertEqual(
            closure_items[0]["metadata"]["provider_completion_result"]["source"],
            "segmented_harvest_company_roster_inprocess",
        )

    def _seed_company_roster_inline_worker(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        request: JobRequest,
        plan_payload: dict,
    ) -> int:
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::lock-narrowing",
            stage="acquiring",
            span_name="harvest_company_employees:lock-narrowing",
            budget_payload={},
            input_payload={},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
            output_payload={"summary": {"status": "completed"}},
        )
        return handle.worker_id

    def _seed_search_seed_inline_worker(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        request: JobRequest,
        plan_payload: dict,
    ) -> int:
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="search_seed_discovery::lock-narrowing",
            stage="planning",
            span_name="search_seed_discovery:lock-narrowing",
            budget_payload={},
            input_payload={},
            metadata={
                "recovery_kind": "search_seed_discovery",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="search_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "search_seed_discovery"},
            output_payload={"summary": {"status": "completed"}},
        )
        return handle.worker_id

    def test_job_scoped_recovery_open_work_counts_board_visible_delta_apply_as_daemon_owned(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_open_work_board_visible_delta_apply"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={},
            artifact_path=str(Path(self.tempdir.name) / "company_assets" / "openai" / "snapshot-open-work"),
        )
        self.store.upsert_job_materialization_item(
            item_id="job-open-work|delta|board-visible",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-open-work",
            item_kind="board_visible_delta_apply",
            status="queued",
            phase="queued",
            candidate_ids=["candidate-1"],
        )
        self.store.upsert_job_materialization_item(
            item_id="job-open-work|snapshot|full",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id="snapshot-open-work",
            item_kind="snapshot_full_materialization",
            status="queued",
            phase="queued",
        )

        summary = self.orchestrator._job_scoped_recovery_open_work_summary(job_id=job_id)

        self.assertEqual(summary["materialization_open_item_count"], 2)
        self.assertEqual(summary["daemon_owned_materialization_open_item_count"], 1)
        self.assertEqual(summary["background_materialization_open_item_count"], 1)
        self.assertEqual(summary["daemon_owned_open_work_count"], 1)
        self.assertEqual(summary["non_daemon_open_work_count"], 1)
        self.assertEqual(summary["materialization_kind_counts"]["board_visible_delta_apply"], 1)
        self.assertEqual(summary["materialization_kind_counts"]["snapshot_full_materialization"], 1)

    def test_resume_blocked_workflow_ignores_pending_exploration_workers(self) -> None:
        request_payload = {
            "raw_user_request": "Find Thinking Machines Lab people",
            "target_company": "Thinking Machines Lab",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_resume_exploration_blocked"
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-resume-exploration"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for queued exploration workers",
                "blocked_task": "enrich_profiles_multisource",
            },
        )

        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key="candidate::queued_exploration",
            stage="enriching",
            span_name="explore_candidate:Queued Exploration Lead",
            budget_payload={"max_queries": 7},
            input_payload={
                "candidate_id": "candidate::queued_exploration",
                "display_name": "Queued Exploration Lead",
                "candidate": {
                    "candidate_id": "candidate::queued_exploration",
                    "display_name": "Queued Exploration Lead",
                },
            },
            metadata={
                "target_company": "Thinking Machines Lab",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="public_media_specialist",
        )

        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        original_execute_task = self.acquisition_engine.execute_task

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved identity.",
                    payload={"snapshot_dir": str(snapshot_dir)},
                    state_updates={
                        "company_identity": identity,
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                    },
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=f"{task.task_type} completed.",
                payload={},
                state_updates={},
            )

        self.acquisition_engine.execute_task = fake_execute_task
        try:
            resume = self.orchestrator._resume_blocked_workflow_if_ready(job_id)
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["job_status"], "completed")
        worker = self.orchestrator.agent_runtime.get_worker(handle.worker_id)
        self.assertIsNotNone(worker)
        assert worker is not None
        self.assertEqual(str(worker.get("status") or ""), "running")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")

    def test_resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow({**request_payload, "skip_plan_review": True})["plan"]
        job_id = "job_resume_from_search_seed_baseline"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-baseline"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        summary_path = discovery_dir / "summary.json"
        entries_path = discovery_dir / "entries.json"
        entries_payload = [
            {
                "seed_key": "reflection-infra-01",
                "full_name": "Infra Builder",
                "headline": "Infrastructure Engineer",
                "source_type": "web_search",
                "source_query": "Reflection AI infra",
                "profile_url": "https://www.linkedin.com/in/infra-builder/",
            }
        ]
        summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Reflection AI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [{"query": "Reflection AI infra", "status": "completed"}],
                    "queued_query_count": 0,
                    "errors": [],
                    "stop_reason": "",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        entries_path.write_text(json.dumps(entries_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries_payload,
            query_summaries=[{"query": "Reflection AI infra", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
            entries_path=entries_path,
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for former/background search workers.",
                "blocked_task": "acquire_full_roster",
                "acquisition_progress": {
                    "latest_state": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": str(snapshot_dir),
                        "company_identity": identity.to_record(),
                        "search_seed_snapshot": search_seed_snapshot.to_record(),
                    }
                },
            },
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::pending",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={"query": "Reflection AI infra"},
            metadata={
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )

        original_execute_task = self.acquisition_engine.execute_task
        executed_task_types: list[str] = []
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        manifest_path = snapshot_dir / "manifest.json"
        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):  # noqa: ARG001
            executed_task_types.append(task.task_type)
            if task.task_type == "enrich_linkedin_profiles":
                candidate_doc_path.write_text(
                    json.dumps(
                        {
                            "snapshot": {"company_identity": identity.to_record()},
                            "candidates": [
                                Candidate(
                                    candidate_id="cand_reflection_infra",
                                    name_en="Infra Builder",
                                    display_name="Infra Builder",
                                    category="employee",
                                    target_company="Reflection AI",
                                    organization="Reflection AI",
                                    employment_status="current",
                                    role="Infrastructure Engineer",
                                    focus_areas="infra systems",
                                ).to_record()
                            ],
                            "evidence": [],
                            "candidate_count": 1,
                            "evidence_count": 0,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built candidate documents from search seed.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_completed": True,
                    },
                )
            if task.task_type == "enrich_public_web_signals":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Extended candidate documents with public-web signals.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "candidate_doc_path": candidate_doc_path,
                        "public_web_stage_candidate_doc_path": candidate_doc_path,
                        "public_web_stage_completed": True,
                    },
                )
            if task.task_type == "normalize_asset_snapshot":
                manifest_path.write_text(
                    json.dumps(
                        {"snapshot_id": snapshot_dir.name, "company_identity": identity.to_record()},
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Normalized snapshot.",
                    payload={"manifest_path": str(manifest_path)},
                    state_updates={"manifest_path": manifest_path},
                )
            if task.task_type == "build_retrieval_index":
                retrieval_index_path.write_text(
                    json.dumps({"status": "built"}, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built retrieval index.",
                    payload={"retrieval_index_summary": str(retrieval_index_path)},
                    state_updates={},
                )
            raise AssertionError(f"unexpected task execution: {task.task_type}")

        self.acquisition_engine.execute_task = fake_execute_task
        try:
            resume = self.orchestrator._resume_blocked_workflow_if_ready(job_id)
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertTrue(bool(resume["baseline_ready"]))
        self.assertEqual(resume["baseline_reason"], "search_seed_entries_present")
        expected_task_types = ["enrich_linkedin_profiles"]
        if any(
            str(task.task_type or "") == "enrich_public_web_signals"
            for task in hydrate_sourcing_plan(plan_payload).acquisition_tasks
        ):
            expected_task_types.append("enrich_public_web_signals")
        self.assertEqual(
            executed_task_types,
            expected_task_types,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertTrue(snapshot["asset_population"]["available"])
        self.assertGreaterEqual(len(snapshot["asset_population"]["candidates"]), 1)

