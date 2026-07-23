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

    def test_execute_retrieval_uses_legacy_bootstrap_store_only_when_explicitly_opted_in(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="reflection_emp_1",
                    name_en="Infra Lead",
                    display_name="Infra Lead",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Head of Infrastructure",
                    focus_areas="infra systems",
                ),
                Candidate(
                    candidate_id="openai_emp_1",
                    name_en="Other Infra",
                    display_name="Other Infra",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Infrastructure Engineer",
                    focus_areas="infra systems",
                ),
            ],
            [],
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我寻找Reflection AI的Infra方向成员",
                "target_company": "Reflection AI",
                "keywords": ["infra"],
                "top_k": 5,
                "execution_preferences": {"allow_local_bootstrap_fallback": True},
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_ENABLE_BOOTSTRAP_CANDIDATE_STORE": "1"},
            clear=False,
        ):
            artifact = self.orchestrator._execute_retrieval(
                job_id="bootstrap_company_scoped",
                request=request,
                plan=plan,
                job_type="workflow",
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["candidate_source"]["source_kind"], "legacy_bootstrap_store")
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], 1)
        self.assertEqual([item["candidate_id"] for item in artifact["matches"]], ["reflection_emp_1"])

    def test_build_sourcing_plan_omits_public_web_stage_by_default(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的基础设施成员",
                "target_company": "Reflection AI",
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        task_types = [task.task_type for task in plan.acquisition_tasks]
        self.assertIn("acquire_former_search_seed", task_types)
        self.assertIn("enrich_linkedin_profiles", task_types)
        self.assertNotIn("enrich_public_web_signals", task_types)
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")
        self.assertEqual(former_task.metadata["acquisition_phase"], "linkedin_stage_1")
        self.assertEqual(former_task.metadata["strategy_type"], "former_employee_search")
        search_bundles = list(plan.search_strategy.query_bundles or [])
        self.assertFalse(
            [
                bundle.bundle_id
                for bundle in search_bundles
                if bundle.source_family in {"public_web_search", "publication_and_blog", "public_interviews"}
            ]
        )
        self.assertTrue(
            all(
                bundle.execution_mode == "paid_fallback"
                or bundle.source_family in {"linkedin_people_search", "targeted_people_search"}
                for bundle in search_bundles
            )
        )

    def test_build_sourcing_plan_includes_public_web_stage_when_two_stage_enabled(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的基础设施成员",
                "target_company": "Reflection AI",
                "analysis_stage_mode": "two_stage",
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        task_types = [task.task_type for task in plan.acquisition_tasks]
        self.assertIn("enrich_public_web_signals", task_types)
        self.assertLess(task_types.index("enrich_linkedin_profiles"), task_types.index("enrich_public_web_signals"))
        public_web_task = next(task for task in plan.acquisition_tasks if task.task_type == "enrich_public_web_signals")
        self.assertEqual(public_web_task.metadata["acquisition_phase"], "public_web_stage_2")

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

    def test_ensure_hosted_runtime_watchdog_starts_sidecar_process(self) -> None:
        captured: dict[str, object] = {}

        class FakeProcess:
            pid = 97531

            def poll(self):  # type: ignore[no-untyped-def]
                return None

        def fake_popen(command, **kwargs):  # type: ignore[no-untyped-def]
            captured["command"] = command
            captured["kwargs"] = kwargs
            return FakeProcess()

        with (
            unittest.mock.patch(
                "sourcing_agent.process_supervision.subprocess.Popen",
                side_effect=fake_popen,
            ),
            unittest.mock.patch(
                "sourcing_agent.process_supervision.time.sleep",
                return_value=None,
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                side_effect=[
                    {"service_name": "server-runtime-watchdog", "status": "not_started", "lock_status": "missing"},
                    {"service_name": "server-runtime-watchdog", "status": "running", "lock_status": "locked"},
                ],
            ),
        ):
            result = self.orchestrator.ensure_hosted_runtime_watchdog(
                {
                    "auto_job_daemon": True,
                    "hosted_runtime_watchdog_poll_seconds": 9.0,
                }
            )

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["mode"], "sidecar")
        self.assertEqual(result["scope"], "hosted_runtime_watchdog")
        self.assertEqual(result["pid"], 97531)
        self.assertEqual(result["handshake"]["status"], "ready")
        command = list(captured["command"])
        self.assertIn("run-server-runtime-watchdog-service", command)
        self.assertIn("--service-name", command)
        self.assertIn("--shared-service-name", command)
        self.assertIn("server-runtime-watchdog", command)
        self.assertIn("worker-recovery-daemon", command)

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

    def test_server_runtime_watchdog_skips_shared_restart_when_service_ready(self) -> None:
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_hosted_runtime_watchdog_once",
            return_value={
                "status": "completed",
                "mode": "hosted",
                "worker_recovery": {"status": "completed"},
                "hosted_dispatch": [{"job_id": "job-1", "status": "started"}],
            },
        ) as hosted_mock:
            result = run_server_runtime_watchdog_once(self.orchestrator)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["mode"], "hosted")
        hosted_mock.assert_called_once()

    def test_hosted_runtime_watchdog_reports_retired_shadow_pg_sync(self) -> None:
        # B4.3f: the shadow-sourced watchdog sync is retired — since B4.1 it mirrored an
        # empty in-memory DB. The watchdog must report the retired status and never call
        # the SQLite->PG sync routine.
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_worker_recovery_once",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_runtime_metrics_snapshot",
            return_value={"status": "ok", "observed_at": "2026-04-21T00:00:00Z"},
        ):
            result = self.orchestrator.run_hosted_runtime_watchdog_once(
                {"control_plane_postgres_dsn": "postgresql://demo/demo"}
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["control_plane_postgres_sync"],
            {"status": "retired", "reason": "sqlite_shadow_retired_b4_3f"},
        )

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

    def test_scoped_parallel_former_seed_is_joined_when_current_lane_fails(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-scoped-former-join-on-failure"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        former_started = threading.Event()
        former_finished = threading.Event()

        def _fake_current_discover(*args, **kwargs):  # noqa: ANN002, ANN003
            self.assertTrue(former_started.wait(timeout=1.0))
            raise RuntimeError("current lane failed")

        def _fake_former_search(*args, **kwargs):  # noqa: ANN002, ANN003
            former_started.set()
            time.sleep(0.2)
            former_finished.set()
            return AcquisitionExecution(
                task_id="acquire-search-seed-pool-former-search-seed",
                status="completed",
                detail="Former search seed ready.",
                payload={},
                state_updates={},
            )

        task = AcquisitionTask(
            task_id="acquire-search-seed-pool",
            task_type="acquire_search_seed_pool",
            title="Acquire scoped search seeds",
            description="Acquire current and former search lanes.",
            status="ready",
            metadata={
                "strategy_type": "scoped_search_roster",
                "include_former_search_seed": True,
                "search_seed_queries": ["infra"],
                "employment_statuses": ["current", "former"],
            },
        )
        started_at = time.monotonic()
        with (
            unittest.mock.patch.object(
                self.acquisition_engine.search_seed_acquirer,
                "discover",
                side_effect=_fake_current_discover,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                side_effect=_fake_former_search,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "current lane failed"):
                self.acquisition_engine._acquire_search_seed_pool(
                    task,
                    {
                        "company_identity": identity,
                        "snapshot_dir": snapshot_dir,
                        "job_id": "job_scoped_parallel_former_join_on_failure",
                        "plan_payload": {},
                        "runtime_mode": "workflow",
                    },
                    JobRequest(
                        raw_user_request="Find Reflection AI infra members",
                        target_company="Reflection AI",
                        categories=["employee", "former_employee"],
                    ),
                )

        self.assertTrue(former_finished.is_set())
        self.assertGreaterEqual(time.monotonic() - started_at, 0.15)
        leaked_threads = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("acquisition-scoped-former-seed")
        ]
        self.assertEqual(leaked_threads, [])

    def test_full_roster_parallel_former_seed_is_joined_when_roster_lane_fails(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-roster-former-join-on-failure"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        former_started = threading.Event()
        former_finished = threading.Event()

        def _fake_roster_fetch(*args, **kwargs):  # noqa: ANN002, ANN003
            self.assertTrue(former_started.wait(timeout=1.0))
            raise RuntimeError("roster lane failed")

        def _fake_former_search(*args, **kwargs):  # noqa: ANN002, ANN003
            former_started.set()
            time.sleep(0.2)
            former_finished.set()
            return AcquisitionExecution(
                task_id="acquire-full-roster-former-search-seed",
                status="completed",
                detail="Former search seed ready.",
                payload={},
                state_updates={},
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire company roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "include_former_search_seed": True,
                "cost_policy": {"allow_company_employee_api": False, "allow_cached_roster_fallback": False},
            },
        )
        started_at = time.monotonic()
        with (
            unittest.mock.patch.object(
                self.acquisition_engine.roster_connector,
                "fetch_company_roster",
                side_effect=_fake_roster_fetch,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                side_effect=_fake_former_search,
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_full_roster_parallel_former_join_on_failure",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra members",
                    target_company="Reflection AI",
                    categories=["employee", "former_employee"],
                ),
            )

        self.assertEqual(execution.status, "blocked")
        self.assertTrue(former_finished.is_set())
        self.assertGreaterEqual(time.monotonic() - started_at, 0.15)
        leaked_threads = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("acquisition-former-seed")
        ]
        self.assertEqual(leaked_threads, [])

    def test_acquire_full_roster_reuse_request_falls_back_to_live_when_no_cached_roster(self) -> None:
        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )
        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-reuse-miss-live-roster"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        seen: dict[str, object] = {}

        def _fake_fetch_company_roster(
            _identity,
            _snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
        ):
            seen["called"] = True
            seen["allow_shared_provider_cache"] = allow_shared_provider_cache
            roster_dir = snapshot_dir / "harvest_company_employees"
            roster_dir.mkdir(parents=True, exist_ok=True)
            merged_path = roster_dir / "harvest_company_employees_merged.json"
            visible_path = roster_dir / "harvest_company_employees_visible.json"
            headless_path = roster_dir / "harvest_company_employees_headless.json"
            summary_path = roster_dir / "harvest_company_employees_summary.json"
            entry = {
                "full_name": "Ada Lovable",
                "headline": "Engineer at Lovable",
                "linkedin_url": "https://www.linkedin.com/in/ada-lovable/",
            }
            merged_path.write_text(json.dumps([entry]), encoding="utf-8")
            visible_path.write_text(json.dumps([entry]), encoding="utf-8")
            headless_path.write_text("[]", encoding="utf-8")
            summary_path.write_text(json.dumps({"visible_entry_count": 1}), encoding="utf-8")
            return CompanyRosterSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company="Lovable",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                raw_entries=[entry],
                visible_entries=[entry],
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": 1}],
                accounts_used=["harvest_company_employees"],
                errors=[],
                stop_reason="completed",
                merged_path=merged_path,
                visible_path=visible_path,
                headless_path=headless_path,
                summary_path=summary_path,
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "include_former_search_seed": False,
                "cost_policy": {"allow_company_employee_api": True},
            },
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch_company_roster,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="基于现有 roster 或实时获取 Lovable 全部成员",
                    target_company="Lovable",
                    categories=["employee"],
                    execution_preferences={"reuse_existing_roster": True},
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertTrue(seen.get("called"))
        self.assertEqual(execution.payload["acquisition_mode"], "live_roster_acquisition")
        self.assertTrue(execution.payload["reuse_existing_roster_miss"])
        self.assertEqual(execution.payload["reuse_existing_roster_miss_reason"], "no_cached_roster_snapshot")

    def test_enrich_profiles_hydrates_disk_roster_when_state_has_search_seed_only(self) -> None:
        identity = CompanyIdentity(
            requested_name="Wispr Flow",
            canonical_name="Wispr Flow",
            company_key="wisprflow",
            linkedin_slug="wispr-flow",
            linkedin_company_url="https://www.linkedin.com/company/wispr-flow/",
        )
        snapshot_dir = self.settings.company_assets_dir / "wisprflow" / "snapshot-hydrate-roster"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        roster_dir = snapshot_dir / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)
        roster_summary_path = roster_dir / "harvest_company_employees_summary.json"
        roster_visible_path = roster_dir / "harvest_company_employees_visible.json"
        roster_merged_path = roster_dir / "harvest_company_employees_merged.json"
        roster_headless_path = roster_dir / "harvest_company_employees_headless.json"
        roster_row = {
            "full_name": "Current Wispr",
            "headline": "Engineer at Wispr Flow",
            "location": "San Francisco Bay Area",
            "linkedin_url": "https://www.linkedin.com/in/current-wispr/",
            "member_key": "current-wispr",
        }
        roster_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "completion_status": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        roster_visible_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_merged_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_headless_path.write_text("[]", encoding="utf-8")

        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        seed_summary_path = discovery_dir / "summary.json"
        seed_entries_path = discovery_dir / "entries.json"
        seed_entry = {
            "full_name": "Former Wispr",
            "headline": "Former ML Engineer at Wispr Flow",
            "profile_url": "https://www.linkedin.com/in/former-wispr/",
            "employment_status": "former",
            "source_type": "harvest_profile_search",
        }
        seed_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        seed_entries_path.write_text(json.dumps([seed_entry], ensure_ascii=False), encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Wispr Flow",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[seed_entry],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            summary_path=seed_summary_path,
            entries_path=seed_entries_path,
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        seen_candidate_names: list[str] = []

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            seen_candidate_names.extend(candidate.display_name for candidate in candidates_arg)
            return MultiSourceEnrichmentResult(candidates=list(candidates_arg), evidence=[])

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            execution = self.acquisition_engine._enrich_profiles(
                AcquisitionTask(
                    task_id="enrich-profiles",
                    task_type="enrich_profiles_multisource",
                    title="Enrich profiles",
                    description="Run profile enrichment",
                    status="ready",
                    blocking=True,
                    metadata={"cost_policy": {}, "enrichment_scope": "linkedin_stage_1"},
                ),
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="帮我找 Wispr Flow 的全部成员",
                    target_company="Wispr Flow",
                    categories=["employee", "former_employee"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 2)
        self.assertEqual(set(seen_candidate_names), {"Current Wispr", "Former Wispr"})
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertEqual(candidate_doc["candidate_count"], 2)
        self.assertIn("roster_snapshot", candidate_doc["acquisition_sources"])
        self.assertIn("search_seed_snapshot", candidate_doc["acquisition_sources"])

    def test_enrich_profiles_hydrates_disk_search_seed_when_state_has_roster_only(self) -> None:
        identity = CompanyIdentity(
            requested_name="Wispr Flow",
            canonical_name="Wispr Flow",
            company_key="wisprflow",
            linkedin_slug="wispr-flow",
            linkedin_company_url="https://www.linkedin.com/company/wispr-flow/",
        )
        snapshot_dir = self.settings.company_assets_dir / "wisprflow" / "snapshot-hydrate-search-seed"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        roster_dir = snapshot_dir / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)
        roster_summary_path = roster_dir / "harvest_company_employees_summary.json"
        roster_visible_path = roster_dir / "harvest_company_employees_visible.json"
        roster_merged_path = roster_dir / "harvest_company_employees_merged.json"
        roster_headless_path = roster_dir / "harvest_company_employees_headless.json"
        roster_row = {
            "full_name": "Current Wispr",
            "headline": "Engineer at Wispr Flow",
            "location": "San Francisco Bay Area",
            "linkedin_url": "https://www.linkedin.com/in/current-wispr/",
            "member_key": "current-wispr",
        }
        roster_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "completion_status": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        roster_visible_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_merged_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_headless_path.write_text("[]", encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Wispr Flow",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[roster_row],
            visible_entries=[roster_row],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            merged_path=roster_merged_path,
            visible_path=roster_visible_path,
            headless_path=roster_headless_path,
            summary_path=roster_summary_path,
        )

        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        seed_summary_path = discovery_dir / "summary.json"
        seed_entries_path = discovery_dir / "entries.json"
        seed_entry = {
            "full_name": "Former Wispr",
            "headline": "Former ML Engineer at Wispr Flow",
            "profile_url": "https://www.linkedin.com/in/former-wispr/",
            "employment_status": "former",
            "source_type": "harvest_profile_search",
        }
        seed_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        seed_entries_path.write_text(json.dumps([seed_entry], ensure_ascii=False), encoding="utf-8")
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        seen_candidate_names: list[str] = []

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            seen_candidate_names.extend(candidate.display_name for candidate in candidates_arg)
            return MultiSourceEnrichmentResult(candidates=list(candidates_arg), evidence=[])

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            execution = self.acquisition_engine._enrich_profiles(
                AcquisitionTask(
                    task_id="enrich-profiles",
                    task_type="enrich_profiles_multisource",
                    title="Enrich profiles",
                    description="Run profile enrichment",
                    status="ready",
                    blocking=True,
                    metadata={"cost_policy": {}, "enrichment_scope": "linkedin_stage_1"},
                ),
                {
                    "roster_snapshot": roster_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="帮我找 Wispr Flow 的全部成员",
                    target_company="Wispr Flow",
                    categories=["employee", "former_employee"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 2)
        self.assertEqual(set(seen_candidate_names), {"Current Wispr", "Former Wispr"})
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertEqual(candidate_doc["candidate_count"], 2)
        self.assertIn("roster_snapshot", candidate_doc["acquisition_sources"])
        self.assertIn("search_seed_snapshot", candidate_doc["acquisition_sources"])

    def test_enrich_profiles_hydrates_disk_search_seed_lane_when_state_has_partial_scoped_seed(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-hydrate-multi-scoped-search"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        current_lane_dir = discovery_dir / "current"
        current_lane_dir.mkdir(parents=True, exist_ok=True)
        in_memory_summary_path = discovery_dir / "summary.json"
        in_memory_entries_path = discovery_dir / "entries.json"
        in_memory_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [{"query": "OpenAI Agent", "status": "completed"}],
                    "stop_reason": "partial_agent_worker_completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        agent_entry = {
            "seed_key": "agent-seed",
            "full_name": "Ari Agent",
            "headline": "Agent Research at OpenAI",
            "profile_url": "https://www.linkedin.com/in/ari-agent/",
            "employment_status": "current",
            "source_query": "OpenAI Agent",
            "source_type": "harvest_profile_search",
        }
        in_memory_entries_path.write_text(json.dumps([agent_entry], ensure_ascii=False), encoding="utf-8")
        multimodal_entry = {
            "seed_key": "multimodal-seed",
            "full_name": "Mira Multimodal",
            "headline": "Multimodal Research at OpenAI",
            "profile_url": "https://www.linkedin.com/in/mira-multimodal/",
            "employment_status": "current",
            "source_query": "OpenAI Multimodal",
            "source_type": "harvest_profile_search",
        }
        (current_lane_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "employment_scope": "current",
                    "employment_status": "current",
                    "strategy_type": "scoped_search_roster",
                    "entry_count": 1,
                    "query_summaries": [{"query": "OpenAI Multimodal", "status": "completed"}],
                    "stop_reason": "partial_multimodal_worker_completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (current_lane_dir / "entries.json").write_text(
            json.dumps([multimodal_entry], ensure_ascii=False),
            encoding="utf-8",
        )
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[agent_entry],
            query_summaries=[{"query": "OpenAI Agent", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="partial_agent_worker_completed",
            summary_path=in_memory_summary_path,
            entries_path=in_memory_entries_path,
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        seen_candidate_names: list[str] = []
        seen_linkedin_urls: list[str] = []

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            seen_candidate_names.extend(candidate.display_name for candidate in candidates_arg)
            seen_linkedin_urls.extend(candidate.linkedin_url for candidate in candidates_arg if candidate.linkedin_url)
            return MultiSourceEnrichmentResult(candidates=list(candidates_arg), evidence=[])

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            execution = self.acquisition_engine._enrich_profiles(
                AcquisitionTask(
                    task_id="enrich-profiles",
                    task_type="enrich_profiles_multisource",
                    title="Enrich profiles",
                    description="Run profile enrichment",
                    status="ready",
                    blocking=True,
                    metadata={
                        "cost_policy": {},
                        "strategy_type": "scoped_search_roster",
                        "enrichment_scope": "linkedin_stage_1",
                    },
                ),
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="帮我找 OpenAI 做 Agent 和 Multimodal 方向的人",
                    target_company="OpenAI",
                    categories=["employee"],
                    keywords=["Agent", "Multimodal"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 2)
        self.assertEqual(set(seen_candidate_names), {"Ari Agent", "Mira Multimodal"})
        self.assertEqual(
            set(seen_linkedin_urls),
            {
                "https://www.linkedin.com/in/ari-agent/",
                "https://www.linkedin.com/in/mira-multimodal/",
            },
        )
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertEqual(candidate_doc["candidate_count"], 2)
        source_snapshot = candidate_doc["acquisition_sources"]["search_seed_snapshot"]
        self.assertEqual(source_snapshot["entry_count"], 2)

    def test_background_outreach_layering_reconcile_retries_completion_lease_inflight(self) -> None:
        with (
            unittest.mock.patch.dict(
                os.environ,
                {
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_MAX_ATTEMPTS": "3",
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_RETRY_SECONDS": "0",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_reconcile_completed_workflow_if_needed",
                side_effect=[
                    {
                        "job_id": "job_outreach_layering_retry",
                        "status": "skipped",
                        "reason": "completed_workflow_reconcile_inflight",
                    },
                    {
                        "job_id": "job_outreach_layering_retry",
                        "status": "reconciled_outreach_layering",
                    },
                ],
            ) as reconcile,
        ):
            result = self.orchestrator._run_background_outreach_layering_reconcile(
                job_id="job_outreach_layering_retry",
                source="workflow_completion",
            )

        self.assertEqual(result["status"], "reconciled_outreach_layering")
        self.assertEqual(result["attempt_count"], 2)
        self.assertTrue(result["background_retry"])
        self.assertEqual(reconcile.call_count, 2)

    def test_background_outreach_layering_reconcile_continues_after_adjacent_reconcile(self) -> None:
        job_id = "job_outreach_layering_after_adjacent_reconcile"
        request_payload = {
            "raw_user_request": "Find current and former researchers",
            "target_company": "Acme",
            "target_scope": "full_company_asset",
        }
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request_payload,
            plan_payload={},
            summary_payload={
                "candidate_source": {"snapshot_id": "snapshot-adjacent-reconcile"},
                "outreach_layering": {
                    "status": "scheduled",
                    "snapshot_id": "snapshot-adjacent-reconcile",
                    "reason": "deferred_for_asset_population_fast_path",
                },
            },
            artifact_path="",
        )
        initial_event_count = len(self.store.list_job_events(job_id))
        with (
            unittest.mock.patch.dict(
                os.environ,
                {
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_MAX_ATTEMPTS": "3",
                    "OUTREACH_LAYERING_BACKGROUND_RECONCILE_RETRY_SECONDS": "0",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_reconcile_completed_workflow_if_needed",
                side_effect=[
                    {"job_id": job_id, "status": "reconciled_harvest_prefetch"},
                    {"job_id": job_id, "status": "reconciled_outreach_layering"},
                ],
            ) as reconcile,
        ):
            result = self.orchestrator._run_background_outreach_layering_reconcile(
                job_id=job_id,
                source="workflow_completion",
            )

        self.assertEqual(result["status"], "reconciled_outreach_layering")
        self.assertEqual(result["attempt_count"], 2)
        self.assertTrue(result["background_retry"])
        self.assertEqual(reconcile.call_count, 2)
        retry_events = [
            event
            for event in self.store.list_job_events(job_id)[initial_event_count:]
            if str(dict(event.get("payload") or {}).get("retry_reason") or "")
            == "background_outreach_layering_pending_after_adjacent_reconcile"
        ]
        self.assertEqual(len(retry_events), 1)

    def test_reconcile_completed_workflow_after_background_search_seed(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-reconcile"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        (discovery_dir / "entries.json").write_text(
            json.dumps(
                [
                    {
                        "seed_key": "baseline",
                        "full_name": "Baseline Lead",
                        "source_type": "harvest_profile_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/baseline-lead/",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Reflection AI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [
                        {
                            "query": "Reflection AI infra",
                            "bundle_id": "bundle-infra",
                            "source_family": "people_search",
                            "execution_mode": "web_search",
                            "mode": "web_search",
                            "status": "queued",
                            "seed_entry_count": 0,
                        }
                    ],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "queued_background_search",
                    "queued_query_count": 1,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="baseline-candidate",
                            name_en="Baseline Lead",
                            display_name="Baseline Lead",
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

        artifact_path = self.settings.jobs_dir / f"{job_id}.result.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False), encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle-infra::01",
            stage="acquiring",
            span_name="search_bundle:bundle-infra",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "Reflection AI infra"}},
            metadata={
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "summary": {
                    "query": "Reflection AI infra",
                    "bundle_id": "bundle-infra",
                    "source_family": "people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "new-lead",
                        "full_name": "Infra Builder",
                        "headline": "Platform Engineer",
                        "source_type": "web_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/infra-builder/",
                    }
                ],
                "errors": [],
            },
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_apply_background_search_seed_workers_to_snapshot",
            side_effect=AssertionError("completed workflow search-seed worker-summary merge is retired"),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(str(reconcile.get("status") or ""), "skipped")
        self.assertEqual(
            str(reconcile.get("reason") or ""),
            "search_seed_reconcile_requires_durable_local_apply_closure_item",
        )
        self.assertEqual(int(reconcile.get("local_apply_closure_item_count") or 0), 0)
        self.assertEqual(int(reconcile.get("search_seed_discovery_item_count") or 0), 0)
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        search_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "search_seed"
        )
        self.assertFalse(search_reconcile)
        updated_entries = json.loads((discovery_dir / "entries.json").read_text())
        self.assertEqual(len(updated_entries), 1)
        updated_summary = json.loads((discovery_dir / "summary.json").read_text())
        self.assertEqual(int(updated_summary["queued_query_count"]), 1)
        candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertEqual(int(candidate_doc["candidate_count"]), 1)
        structured_events = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("event_family") == "completed_workflow_reconcile"
        ]
        self.assertTrue(
            any(str(event.get("phase") or "") == "worker_summary_merge_retired" for event in structured_events)
        )

    def test_completed_search_seed_no_candidate_delta_skips_prefetch_and_materialize(self) -> None:
        request_payload = {
            "raw_user_request": "帮我找OpenAI做Infra方向的人",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_no_candidate_delta"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-no-delta"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        baseline_entry = {
            "seed_key": "baseline-infra",
            "full_name": "Baseline Infra",
            "source_type": "harvest_profile_search",
            "source_query": "OpenAI Infra",
            "profile_url": "https://www.linkedin.com/in/baseline-infra/",
        }
        (discovery_dir / "entries.json").write_text(
            json.dumps([baseline_entry], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "provider_people_search_fallback",
                    "queued_query_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "candidates": [
                        Candidate(
                            candidate_id="baseline-infra-candidate",
                            name_en="Baseline Infra",
                            display_name="Baseline Infra",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Infrastructure Engineer",
                            linkedin_url="https://www.linkedin.com/in/baseline-infra/",
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
        artifact_path = self.settings.jobs_dir / f"{job_id}.result.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False), encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                    "source_path": str(candidate_doc_path),
                    "candidate_count": 1,
                },
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="former::seed_queries::01",
            stage="acquiring",
            span_name="search_bundle:former-seed",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "Infra"}},
            metadata={
                "recovery_kind": "search_seed_discovery",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={
                "stage": "completed",
                "recovery_kind": "search_seed_discovery",
                "provider_name": "dataforseo_google_organic",
            },
            output_payload={
                "summary": {
                    "query": "Infra",
                    "bundle_id": "seed_queries",
                    "source_family": "public_web_search",
                    "execution_mode": "low_cost_web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "result_count": 9,
                    "linkedin_result_count": 0,
                    "seed_entry_count": 0,
                },
                "entries": [],
                "errors": [],
                "seed_entry_count": 0,
            },
        )

        enqueue = self.orchestrator._enqueue_local_apply_closure_item_for_completed_worker_result(
            {"worker_id": worker_handle.worker_id, "worker_status": "completed", "source": "unit_test"}
        )
        self.assertEqual(str(enqueue.get("status") or ""), "enqueued")

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                side_effect=AssertionError("zero-delta search seed must not queue profile prefetch"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=AssertionError("zero-delta search seed must not full materialize"),
            ),
        ):
            queue_result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {"job_id": job_id, "local_apply_closure_item_limit": 1}
            )

        self.assertEqual(int(queue_result.get("completed_count") or 0), 1)
        reconcile = dict(dict(queue_result["items"][0]).get("callback_result") or {})
        self.assertEqual(reconcile["status"], "reconciled_search_seed")
        self.assertEqual(reconcile["sync_status"], "skipped")
        self.assertEqual(reconcile["sync_reason"], "search_seed_no_candidate_delta")
        worker = self.store.get_agent_worker(worker_id=worker_handle.worker_id)
        assert worker is not None
        inline_ingest = dict(dict(worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(inline_ingest["sync_reason"], "search_seed_no_candidate_delta")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        search_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "search_seed"
        )
        self.assertEqual(int(search_reconcile["added_entry_count"]), 0)
        self.assertEqual(
            str(dict(search_reconcile.get("profile_prefetch") or {}).get("reason") or ""),
            "search_seed_no_candidate_delta",
        )
        self.assertEqual(len(json.loads((discovery_dir / "entries.json").read_text(encoding="utf-8"))), 1)

    def test_reconcile_completed_workflow_after_background_company_roster(self) -> None:
        request_payload = {
            "raw_user_request": "Find Manus AI people",
            "target_company": "Manus AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_company_roster_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "manusai" / "snapshot-company-roster-reconcile"
        shard_snapshot_dir = snapshot_dir / "harvest_company_employees" / "shards" / "all_people"
        shard_harvest_dir = shard_snapshot_dir / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Manus AI",
            canonical_name="Manus AI",
            company_key="manusai",
            linkedin_slug="manus-ai",
            linkedin_company_url="https://www.linkedin.com/company/manus-ai/",
        )
        dataset_items_path = shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json"
        dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "id": "manus_member_1",
                        "linkedinUrl": "https://www.linkedin.com/in/manus-member-1/",
                        "firstName": "Ada",
                        "lastName": "Planner",
                        "summary": "Engineer at Manus AI",
                        "currentPositions": [
                            {
                                "companyName": "Manus AI",
                                "title": "Software Engineer",
                                "current": True,
                            }
                        ],
                        "location": {"linkedinText": "San Francisco Bay Area"},
                        "_meta": {
                            "pagination": {
                                "totalElements": 1,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "previousElements": 0,
                                "pageSize": 25,
                            },
                            "query": {
                                "currentCompanies": ["https://www.linkedin.com/company/manus-ai/"],
                            },
                        },
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
                    "company_filters": {},
                    "artifact_paths": {"dataset_items": str(dataset_items_path)},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        artifact_path = self.settings.jobs_dir / f"{job_id}.result.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False), encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::manusai::all_people",
            stage="acquiring",
            span_name="harvest_company_employees:manusai:all_people",
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
                "max_pages": 1,
                "page_limit": 25,
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "summary": {
                    "company_identity": identity.to_record(),
                    "status": "completed",
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {},
                    "snapshot_dir": str(shard_snapshot_dir),
                    "root_snapshot_dir": str(snapshot_dir),
                    "shard_id": "all_people",
                    "title": "All People",
                    "strategy_id": "small_org_roster",
                }
            },
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={
                    "status": "queued",
                    "requested_url_count": 1,
                    "dispatched_url_count": 1,
                    "cached_profile_count": 0,
                    "queued_worker_count": 1,
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value={
                    "status": "completed",
                    "candidate_count": 1,
                    "evidence_count": 1,
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "materialized_candidate_documents": str(
                            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                        )
                    },
                    "state_updates": {},
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={
                    "status": "completed",
                    "snapshot_id": snapshot_dir.name,
                    "layer_counts": {"layer_0_roster": 1},
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after company-roster reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_company_roster")
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertGreaterEqual(int(candidate_doc["candidate_count"]), 1)
        self.assertTrue(
            any(str(item.get("name_en") or "") == "Ada Planner" for item in list(candidate_doc.get("candidates") or []))
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        company_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "company_roster"
        )
        self.assertEqual(int(company_reconcile["applied_worker_count"]), 1)
        self.assertEqual(int(dict(company_reconcile.get("profile_prefetch") or {}).get("queued_worker_count") or 0), 1)

    def test_refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_refresh"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-pre-retrieval-refresh"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity.to_record(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (discovery_dir / "entries.json").write_text(
            json.dumps(
                [
                    {
                        "seed_key": "baseline",
                        "full_name": "Baseline Lead",
                        "source_type": "harvest_profile_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/baseline-lead/",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Reflection AI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "completed",
                    "queued_query_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="baseline-candidate",
                            name_en="Baseline Lead",
                            display_name="Baseline Lead",
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
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="bundle-infra::01",
            stage="acquiring",
            span_name="search_bundle:bundle-infra",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "Reflection AI infra"}},
            metadata={
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
            output_payload={
                "summary": {
                    "query": "Reflection AI infra",
                    "bundle_id": "bundle-infra",
                    "source_family": "people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "new-lead",
                        "full_name": "Infra Builder",
                        "headline": "Platform Engineer",
                        "source_type": "web_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/infra-builder/",
                    }
                ],
                "errors": [],
            },
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.build_company_candidate_artifacts",
                return_value={
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "materialized_candidate_documents": str(
                            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                        )
                    },
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                return_value={
                    "status": "queued",
                    "requested_url_count": 2,
                    "dispatched_url_count": 1,
                    "cached_profile_count": 1,
                    "queued_worker_count": 1,
                },
            ),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
            )

        self.assertEqual(refresh["status"], "completed")
        self.assertEqual(int(refresh["search_seed"]["added_entry_count"]), 1)
        self.assertEqual(int(dict(refresh["search_seed"].get("profile_prefetch") or {}).get("queued_worker_count") or 0), 1)
        self.assertEqual(str(refresh["sync"]["status"]), "completed")
        updated_candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertGreaterEqual(int(updated_candidate_doc["candidate_count"]), 2)
        self.assertIsNotNone(
            self.store.find_candidate_by_name(
                target_company="Reflection AI",
                name_en="Infra Builder",
            )
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        background_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {})
        self.assertIn("search_seed", background_reconcile)
        self.assertEqual(
            int(dict(background_reconcile["search_seed"].get("profile_prefetch") or {}).get("queued_worker_count") or 0),
            1,
        )
        progress = self.orchestrator.get_job_progress(job_id)
        assert progress is not None
        latest_metrics = dict(dict(progress.get("progress") or {}).get("latest_metrics") or {})
        self.assertIn("pre_retrieval_refresh", latest_metrics)
        self.assertIn("refresh_metrics", latest_metrics)
        refresh_metrics = dict(latest_metrics.get("refresh_metrics") or {})
        self.assertEqual(int(refresh_metrics.get("pre_retrieval_refresh_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("inline_search_seed_worker_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("background_search_seed_reconcile_count") or 0), 1)

    def test_refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store(
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
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_company_roster_refresh"
        snapshot_dir = self.settings.company_assets_dir / "miromindai" / "snapshot-company-roster-refresh"
        shard_snapshot_dir = snapshot_dir / "harvest_company_employees" / "shards" / "us_core"
        shard_harvest_dir = shard_snapshot_dir / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.parent.mkdir(parents=True, exist_ok=True)
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "MiroMind.ai",
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
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
                        "_meta": {
                            "pagination": {
                                "totalElements": 1,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "previousElements": 0,
                                "pageSize": 25,
                            },
                            "query": {
                                "currentCompanies": ["https://www.linkedin.com/company/miromind-ai/"],
                                "locations": ["United States"],
                            },
                        },
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
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
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
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed"},
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

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={
                    "status": "queued",
                    "requested_url_count": 1,
                    "dispatched_url_count": 1,
                    "cached_profile_count": 0,
                    "queued_worker_count": 1,
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=AssertionError("pre-retrieval profile completion must not run full snapshot sync"),
            ),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
            )

        self.assertEqual(refresh["status"], "completed")
        self.assertEqual(int(refresh["company_roster"]["added_entry_count"]), 1)
        self.assertEqual(
            int(dict(refresh["company_roster"].get("profile_prefetch") or {}).get("queued_worker_count") or 0),
            1,
        )
        candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertGreaterEqual(int(candidate_doc["candidate_count"]), 1)
        self.assertTrue(
            any(str(item.get("name_en") or "") == "Mira Agent" for item in list(candidate_doc.get("candidates") or []))
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        background_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {})
        self.assertIn("company_roster", background_reconcile)
        self.assertEqual(int(background_reconcile["company_roster"]["applied_worker_count"]), 1)
        progress = self.orchestrator.get_job_progress(job_id)
        assert progress is not None
        latest_metrics = dict(dict(progress.get("progress") or {}).get("latest_metrics") or {})
        refresh_metrics = dict(latest_metrics.get("refresh_metrics") or {})
        self.assertEqual(int(refresh_metrics.get("pre_retrieval_refresh_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("inline_company_roster_worker_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("background_company_roster_reconcile_count") or 0), 1)

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

    def test_post_profile_completion_missing_candidate_ids_queues_full_materialization_without_sync(
        self,
    ) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find OpenAI infra people",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["infra"],
                "top_k": 5,
            }
        )
        plan_payload = self.orchestrator.plan_workflow(request.to_record())["plan"]
        job_id = "job_profile_completion_missing_candidate_ids"
        snapshot_id = "snapshot-profile-completion-missing-candidate-ids"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Acquiring",
                "background_snapshot_materialization": {
                    "status": "deferred",
                    "snapshot_id": snapshot_id,
                    "reason": "unit_profile_completion_missing_candidate_ids",
                },
            },
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("missing candidate-id profile completion must queue, not run full sync"),
        ) as sync_mock:
            post_profile_policy = self.orchestrator._post_profile_completion_materialization_policy(
                job=self.store.get_job(job_id) or {},
                request=request,
                plan_payload=plan_payload,
                snapshot_dir=snapshot_dir,
                candidate_ids=[],
                remaining_workers=[],
                worker_ids=[101],
                source="unit_missing_candidate_ids",
            )
            sync_result = self.orchestrator._inline_incremental_sync_for_running_job(
                job=self.store.get_job(job_id) or {},
                request=request,
                worker_kind="harvest_prefetch",
                snapshot_dir=snapshot_dir,
                applied_worker_ids=[101],
                sync_reason="inline_background_harvest_prefetch_reconcile",
                candidate_ids=[],
                remaining_workers=[],
                defer_full_snapshot_materialization_for_profile_delta=bool(
                    post_profile_policy.get("defer_full_snapshot_materialization_for_profile_delta")
                ),
            )

        sync_mock.assert_not_called()
        self.assertEqual(str(post_profile_policy.get("materialization_contract") or ""), "snapshot_full_materialization_queued")
        self.assertEqual(str(sync_result.get("status") or ""), "deferred")
        self.assertEqual(str(sync_result.get("materialization_contract") or ""), "snapshot_full_materialization_queued")
        self.assertFalse(bool(sync_result.get("full_snapshot_materialization_performed")))
        self.assertTrue(bool(sync_result.get("full_snapshot_materialization_required")))
        waiting_commands = [
            command
            for command in self.store.list_workflow_commands(
                workflow_run_id=legacy_job_workflow_run_id(job_id),
                statuses=["retry_wait"],
                limit=0,
            )
            if str(command.get("command_type") or "") == SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE
        ]
        self.assertEqual(len(waiting_commands), 1)
        self.assertTrue(
            dict(dict(waiting_commands[0].get("payload") or {}).get("materialization_metadata") or {}).get(
                "pending_until_workflow_completion"
            )
        )
        self.assertEqual(str(dict(waiting_commands[0].get("result") or {}).get("status") or ""), "waiting_prerequisite")
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id(job_id),
            command_type=SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
            limit=10,
        )
        self.assertFalse(ready_commands)

    def test_post_profile_full_materialization_release_is_idempotent_and_not_redeferred(
        self,
    ) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find OpenAI infra people",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["infra"],
                "top_k": 5,
            }
        )
        plan_payload = self.orchestrator.plan_workflow(request.to_record())["plan"]
        job_id = "job_profile_completion_release_idempotent"
        snapshot_id = "snapshot-profile-completion-release-idempotent"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "background_snapshot_materialization": {
                    "status": "deferred",
                    "snapshot_id": snapshot_id,
                    "reason": "unit_profile_completion_release_idempotent",
                },
            },
        )
        first_policy = self.orchestrator._post_profile_completion_materialization_policy(
            job=self.store.get_job(job_id) or {},
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            candidate_ids=["openai-release-idempotent"],
            remaining_workers=[],
            worker_ids=[201],
            source="unit_release_idempotent_first",
        )
        self.assertEqual(str(first_policy.get("materialization_contract") or ""), "board_visible_profile_delta")
        self.orchestrator._release_snapshot_full_materialization_items_for_completed_workflow(
            job_id=job_id,
            source="unit_workflow_completion",
        )
        queued_commands = [
            command
            for command in self.store.list_workflow_commands(
                workflow_run_id=legacy_job_workflow_run_id(job_id),
                statuses=["queued"],
                limit=0,
            )
            if str(command.get("command_type") or "") == SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE
        ]
        self.assertEqual(len(queued_commands), 1)

        second_policy = self.orchestrator._post_profile_completion_materialization_policy(
            job=self.store.get_job(job_id) or {},
            request=request,
            plan_payload=plan_payload,
            snapshot_dir=snapshot_dir,
            candidate_ids=["openai-release-idempotent"],
            remaining_workers=[],
            worker_ids=[201],
            source="unit_release_idempotent_duplicate",
        )
        self.assertEqual(str(dict(second_policy.get("snapshot_full_materialization_item") or {}).get("status")), "queued")
        waiting_commands = [
            command
            for command in self.store.list_workflow_commands(
                workflow_run_id=legacy_job_workflow_run_id(job_id),
                statuses=["retry_wait"],
                limit=0,
            )
            if str(command.get("command_type") or "") == SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE
            and dict(dict(command.get("payload") or {}).get("materialization_metadata") or {}).get(
                "pending_until_workflow_completion"
            )
        ]
        self.assertFalse(waiting_commands)

    def test_harvest_profile_apply_candidate_ids_include_non_member_materialized_profiles(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-non-member-delta-sync"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/alex-chatgpt/"
        candidate = Candidate(
            candidate_id="alex-chatgpt",
            name_en="Alex ChatGPT",
            display_name="Alex ChatGPT",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="ChatGPT Engineer",
            linkedin_url="",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Alex ChatGPT",
            headline="Research Engineer at Anthropic",
            current_company="Anthropic",
            experience=[{"companyName": "Anthropic", "title": "Research Engineer", "current": True}],
        )
        worker = {
            "worker_id": 123,
            "updated_at": "2026-04-30T00:00:00+00:00",
            "metadata": {
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
            },
            "checkpoint": {"run_id": "run-non-member", "dataset_id": "dataset-non-member"},
            "output": {"summary": {"status": "completed", "requested_urls": [profile_url]}},
        }

        with unittest.mock.patch.object(
            self.store.repos.linkedin_profile_registry,
            "mark_fetched",
            side_effect=AssertionError("materialization must not re-upsert already-terminal fetched URLs"),
        ):
            result = self.orchestrator.snapshot_materializer.apply_harvest_profile_workers_to_snapshot(
                snapshot_dir=snapshot_dir,
                pending_workers=[worker],
            )

        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["resolved_candidate_count"], 0)
        self.assertEqual(result["non_member_candidate_count"], 1)
        self.assertEqual(result["candidate_ids"], ["alex-chatgpt"])
        self.assertEqual(result["non_member_candidate_ids"], ["alex-chatgpt"])
        event_records = list(result.get("profile_materialized_candidate_records") or [])
        self.assertEqual(len(event_records), 1)
        event_record = dict(event_records[0])
        self.assertEqual(event_record["candidate_id"], "alex-chatgpt")
        self.assertTrue(event_record.get("experience_lines"))
        self.assertEqual(event_record.get("profile_capture_kind"), "provider_profile_detail")
        self.assertEqual(result["registry_terminal_backfill_requested_count"], 0)
        self.assertEqual(result["registry_terminal_backfill_skipped_count"], 1)

    def test_run_workflow_from_acquisition_publishes_preview_when_blocked_execute_has_open_apply(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_terminal_linkedin_blocked_execute_open_apply"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-terminal-linkedin-blocked-execute"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate = Candidate(
            candidate_id="openai-bridge-blocked-execute",
            name_en="OpenAI Bridge Blocked Execute",
            display_name="OpenAI Bridge Blocked Execute",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/openai-bridge-blocked-execute/",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Planning completed."},
        )
        self.store.upsert_job_materialization_item(
            item_id="local_apply_terminal_bridge_blocked_execute",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id=snapshot_dir.name,
            item_kind="local_apply_closure",
            source="worker_completion_event",
            reason="harvest_profile_batch_completed_needs_local_apply_closure",
            status="queued",
            phase="queued",
            source_worker_ids=[78],
            metadata={"recovery_kind": "harvest_profile_batch", "snapshot_dir": str(snapshot_dir)},
        )

        executed_task_types: list[str] = []
        retrieval_calls: list[str] = []

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):  # noqa: ARG001
            executed_task_types.append(task.task_type)
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved company identity.",
                    payload={"snapshot_dir": str(snapshot_dir)},
                    state_updates={
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "company_identity": identity,
                    },
                )
            if task.task_type == "acquire_full_roster":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Acquired current candidates.",
                    payload={"candidate_count": 1},
                    state_updates={"candidates": [candidate]},
                )
            if task.task_type == "enrich_linkedin_profiles":
                candidate_doc_path.write_text(
                    json.dumps(
                        {
                            "snapshot": {
                                "snapshot_id": snapshot_dir.name,
                                "company_identity": identity.to_record(),
                            },
                            "acquisition_stage": {
                                "phase": "linkedin_stage_1",
                                "task_id": task.task_id,
                                "task_type": "enrich_linkedin_profiles",
                                "status": "completed",
                                "stage_checkpoint_source": "linkedin_profile_registry_terminal_scope",
                            },
                            "enrichment_scope": "linkedin_stage_1",
                            "enrichment_summary": {
                                "profile_prefetch": {
                                    "status": "completed",
                                    "requested_url_count": 1,
                                    "registry_terminal_summary": {
                                        "requested_url_count": 1,
                                        "terminal_url_count": 1,
                                        "open_url_count": 0,
                                        "all_requested_terminal": True,
                                    },
                                    "profile_prefetch_queue": {
                                        "requested_url_count": 1,
                                        "registry_terminal_url_count": 1,
                                        "registry_open_url_count": 0,
                                        "registry_all_requested_terminal": True,
                                        "terminal_queue_state_leak_count": 0,
                                    },
                                }
                            },
                            "candidates": [candidate.to_record()],
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
                    status="blocked",
                    detail="Stage 1 terminal artifact exists; local apply remains open.",
                    payload={"candidate_doc_path": str(candidate_doc_path), "candidate_count": 1},
                    state_updates={
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_completed": True,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )
            raise AssertionError(f"unexpected task execution: {task.task_type}")

        def fake_execute_retrieval(job_id_arg, request_arg, plan_arg, **kwargs):  # noqa: ARG001
            runtime_policy = dict(kwargs.get("runtime_policy") or {})
            retrieval_calls.append(str(runtime_policy.get("analysis_stage") or ""))
            return {
                "artifact_path": str(self.settings.jobs_dir / f"{job_id_arg}.preview.json"),
                "summary": {
                    "text": "Stage 1 preview is ready with 1 candidates.",
                    "analysis_stage": str(runtime_policy.get("analysis_stage") or "stage_1_preview"),
                    "total_matches": 1,
                    "returned_matches": 1,
                    "manual_review_queue_count": 0,
                },
                "matches": [],
            }

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "execute_task",
            side_effect=fake_execute_task,
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_execute_retrieval",
            side_effect=fake_execute_retrieval,
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_running_workflow_before_retrieval",
            return_value={"status": "skipped", "reason": "test"},
        ):
            result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(result["status"], "blocked")
        self.assertIn("enrich_linkedin_profiles", executed_task_types)
        self.assertEqual(retrieval_calls, ["stage_1_preview"])
        latest_job = self.store.get_job(job_id) or {}
        summary = dict(latest_job.get("summary") or {})
        stage1_preview = dict(summary.get("stage1_preview") or {})
        self.assertEqual(str(stage1_preview.get("status") or ""), "ready")
        self.assertEqual(str(summary.get("blocked_task") or ""), "enrich_linkedin_profiles")

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

    def test_company_roster_inline_reconcile_runs_prefetch_outside_writer_lock(self) -> None:
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
        job_id = "job_company_roster_lock_narrowing"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-company-roster-lock-narrowing"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_id = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        prefetch_called_outside_lock = threading.Event()
        sync_called = threading.Event()
        prefetch_kwargs_seen: list[dict] = []
        ordering_log: list[str] = []
        per_job_lock = self.orchestrator._inline_incremental_writer_lock_for_job(job_id)

        def _fake_apply_company_roster(**_kwargs):
            ordering_log.append("apply_under_lock")
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "roster_snapshot": None,
            }

        def _fake_queue_prefetch(**kwargs):
            prefetch_kwargs_seen.append(kwargs)
            ordering_log.append("prefetch_outside_lock")
            # Per-job lock must already be released when the prefetch fires.
            self.assertTrue(
                per_job_lock.acquire(blocking=False),
                "company_roster prefetch must run outside the per-job writer lock",
            )
            per_job_lock.release()
            prefetch_called_outside_lock.set()
            return {
                "status": "queued",
                "queued_worker_count": 1,
                "queued_urls": ["https://www.linkedin.com/in/openai-roster-tail/"],
                "dispatched_url_count": 1,
            }

        def _fake_sync(**_kwargs):
            ordering_log.append("sync")
            sync_called.set()
            return {
                "status": "deferred",
                "reason": "same_kind_background_workers_still_inflight",
                "writer_scope": "job",
                "sync_policy": "same_kind_micro_batch_single_writer",
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                side_effect=_fake_apply_company_roster,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                side_effect=_fake_queue_prefetch,
            ) as queue_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_running_job_inline_reconcile_state",
                return_value=None,
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertTrue(prefetch_called_outside_lock.is_set())
        self.assertTrue(sync_called.is_set())
        # Apply must precede prefetch (prefetch needs apply_result), and prefetch must precede sync.
        self.assertEqual(
            ordering_log[: 3],
            ["apply_under_lock", "prefetch_outside_lock", "sync"],
        )
        self.assertEqual(len(prefetch_kwargs_seen), 1)
        self.assertEqual(
            prefetch_kwargs_seen[0].get("load_cached_profile_payloads"),
            False,
            "company_roster inline reconcile prefetch must use registry-only marker path",
        )
        self.assertEqual(
            prefetch_kwargs_seen[0].get("submit_provider"),
            True,
            "company_roster inline reconcile must let the registry scheduler fill available provider slots immediately",
        )
        queue_mock.assert_called_once()

    def test_company_roster_pending_profile_prefetch_defers_full_materialization(self) -> None:
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
        job_id = "job_company_roster_prefetch_defers_full_sync"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-company-roster-prefetch-defers"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_id = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )
        captured_sync_results: list[dict] = []

        def _fake_apply_company_roster(**_kwargs):
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "candidate_count": 145,
                "evidence_count": 145,
                "candidate_doc_path": str(snapshot_dir / "candidate_documents.json"),
                "roster_snapshot": None,
            }

        def _capture_persist(**kwargs):
            captured_sync_results.append(dict(kwargs.get("sync_result") or {}))
            return None

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                side_effect=_fake_apply_company_roster,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                return_value={
                    "status": "queued",
                    "requested_url_count": 145,
                    "queued_worker_count": 2,
                    "dispatched_url_count": 100,
                    "deferred_url_count": 45,
                    "queued_urls": ["https://www.linkedin.com/in/openai-roster-prefetch-a/"],
                    "deferred_urls": ["https://www.linkedin.com/in/openai-roster-prefetch-tail/"],
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=AssertionError(
                    "company_roster must not full-materialize while profile prefetch is pending"
                ),
            ) as full_sync_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_running_job_inline_reconcile_state",
                side_effect=_capture_persist,
            ),
        ):
            result = self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertEqual(result["status"], "processed")
        sync_result = dict(result.get("sync_result") or {})
        self.assertEqual(sync_result["status"], "deferred")
        self.assertEqual(sync_result["reason"], "profile_prefetch_workers_still_inflight")
        self.assertEqual(sync_result["materialization_contract"], "pre_profile_full_snapshot_materialization_deferred")
        self.assertTrue(sync_result["full_snapshot_materialization_required"])
        self.assertFalse(sync_result["full_snapshot_materialization_performed"])
        self.assertEqual(sync_result["candidate_count"], 0)
        full_sync_mock.assert_not_called()
        self.assertEqual(captured_sync_results, [sync_result])
        worker_after = self.store.get_agent_worker(worker_id=worker_id)
        assert worker_after is not None
        ingest_marker = dict(dict(worker_after.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(ingest_marker.get("sync_status"), "deferred")
        self.assertEqual(ingest_marker.get("sync_reason"), "profile_prefetch_workers_still_inflight")
        phase_b_events = [
            dict(event)
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("pipeline_order")
            == "company_roster_apply_to_profile_prefetch_before_materialization"
        ]
        self.assertEqual(len(phase_b_events), 1)
        phase_b_payload = dict(phase_b_events[0].get("payload") or {})
        self.assertEqual(phase_b_payload.get("kind"), "profile_prefetch_phase_b_group")
        self.assertEqual(phase_b_payload.get("requested_url_count"), 145)
        self.assertEqual(phase_b_payload.get("dispatched_url_count"), 100)
        self.assertEqual(phase_b_payload.get("queued_worker_count"), 2)

    def test_company_roster_inline_reconcile_prefetch_does_not_block_peer_remote_completion(
        self,
    ) -> None:
        """Holding the per-job lock during a peer's apply must not block the next prefetch path."""
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
        job_id = "job_company_roster_peer_lock_contention"
        snapshot_dir = (
            self.settings.company_assets_dir / "openai" / "snapshot-company-roster-peer-lock-contention"
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_id = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        prefetch_called_with_peer_holding_lock = threading.Event()
        peer_lock_held_during_prefetch = threading.Event()

        def _fake_apply(**_kwargs):
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "roster_snapshot": None,
            }

        def _fake_queue(**_kwargs):
            # Simulate a peer remote completion grabbing the per-job writer lock right now.
            peer_lock = self.orchestrator._inline_incremental_writer_lock_for_job(job_id)
            self.assertTrue(
                peer_lock.acquire(timeout=2.0),
                "Per-job writer lock must be released before company_roster prefetch fires",
            )
            try:
                peer_lock_held_during_prefetch.set()
                prefetch_called_with_peer_holding_lock.set()
            finally:
                peer_lock.release()
            return {"status": "queued", "queued_worker_count": 0, "dispatched_url_count": 0}

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                side_effect=_fake_queue,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                return_value={
                    "status": "deferred",
                    "reason": "same_kind_background_workers_still_inflight",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_running_job_inline_reconcile_state",
                return_value=None,
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertTrue(prefetch_called_with_peer_holding_lock.is_set())
        self.assertTrue(peer_lock_held_during_prefetch.is_set())

    def test_search_seed_inline_reconcile_runs_prefetch_outside_writer_lock(self) -> None:
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
        job_id = "job_search_seed_lock_narrowing"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-seed-lock-narrowing"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_id = self._seed_search_seed_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        prefetch_kwargs_seen: list[dict] = []
        ordering_log: list[str] = []
        per_job_lock = self.orchestrator._inline_incremental_writer_lock_for_job(job_id)

        def _fake_apply_search_seed(**_kwargs):
            ordering_log.append("apply_under_lock")
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "search_seed_snapshot": object(),
            }

        def _fake_queue(**kwargs):
            prefetch_kwargs_seen.append(kwargs)
            ordering_log.append("prefetch_outside_lock")
            self.assertTrue(
                per_job_lock.acquire(blocking=False),
                "search_seed prefetch must run outside the per-job writer lock",
            )
            per_job_lock.release()
            return {"status": "queued", "queued_worker_count": 0, "dispatched_url_count": 0}

        def _fake_sync(**_kwargs):
            ordering_log.append("sync")
            return {
                "status": "deferred",
                "reason": "same_kind_background_workers_still_inflight",
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_search_seed_workers_to_snapshot",
                side_effect=_fake_apply_search_seed,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                side_effect=_fake_queue,
            ) as queue_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_running_job_inline_reconcile_state",
                return_value=None,
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertEqual(ordering_log[:3], ["apply_under_lock", "prefetch_outside_lock", "sync"])
        self.assertEqual(len(prefetch_kwargs_seen), 1)
        self.assertEqual(
            prefetch_kwargs_seen[0].get("submit_provider"),
            True,
            "search_seed inline reconcile must let the registry scheduler fill available provider slots immediately",
        )
        # Search-seed wrapper passes load_cached_profile_payloads=False internally.
        # Validate the wrapper itself has that contract by exercising the helper.
        delegated_kwargs: list[dict] = []

        def _capture_inner(**kwargs):
            delegated_kwargs.append(kwargs)
            return {"status": "queued"}

        from sourcing_agent.seed_discovery import SearchSeedSnapshot

        # Exercise the wrapper with a real SearchSeedSnapshot to ensure it forwards the registry-only flag.
        sentinel_snapshot = SearchSeedSnapshot.__new__(SearchSeedSnapshot)
        with unittest.mock.patch.object(
            self.orchestrator,
            "_queue_background_profile_prefetch_from_available_baselines",
            side_effect=_capture_inner,
        ):
            self.orchestrator._queue_background_profile_prefetch_from_search_seed_snapshot(
                job_id=job_id,
                request=request,
                plan_payload=plan_payload,
                snapshot_dir=snapshot_dir,
                search_seed_snapshot=sentinel_snapshot,
            )
        self.assertEqual(len(delegated_kwargs), 1)
        self.assertEqual(
            delegated_kwargs[0].get("load_cached_profile_payloads"),
            False,
            "search_seed inline reconcile prefetch must use registry-only marker path",
        )
        queue_mock.assert_called_once()

    def test_completed_company_roster_reconcile_runs_prefetch_outside_writer_lock(self) -> None:
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
        job_id = "job_completed_company_roster_lock_narrowing"
        snapshot_dir = (
            self.settings.company_assets_dir / "openai" / "snapshot-completed-company-roster-lock-narrowing"
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_id = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        prefetch_kwargs_seen: list[dict] = []
        ordering_log: list[str] = []
        per_job_lock = self.orchestrator._inline_incremental_writer_lock_for_job(job_id)

        def _fake_apply(**_kwargs):
            ordering_log.append("apply_under_lock")
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "roster_snapshot": None,
            }

        def _fake_queue(**kwargs):
            prefetch_kwargs_seen.append(kwargs)
            ordering_log.append("prefetch_outside_lock")
            self.assertTrue(
                per_job_lock.acquire(blocking=False),
                "completed company_roster prefetch must run outside the per-job writer lock",
            )
            per_job_lock.release()
            return {"status": "queued", "queued_worker_count": 0, "dispatched_url_count": 0}

        def _fake_sync(**_kwargs):
            ordering_log.append("sync_outside_lock")
            self.assertTrue(
                per_job_lock.acquire(blocking=False),
                "completed company_roster materialize must run outside the per-job writer lock",
            )
            per_job_lock.release()
            return {
                "status": "completed",
                "candidate_count": 0,
                "evidence_count": 0,
                "artifact_dir": str(snapshot_dir),
                "artifact_paths": {},
                "writer_scope": "job",
                "sync_policy": "same_kind_micro_batch_single_writer",
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                side_effect=_fake_queue,
            ) as queue_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"job_id": job_id, "status": "completed", "artifact_path": str(artifact_path)},
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertEqual(
            ordering_log[: 3],
            ["apply_under_lock", "prefetch_outside_lock", "sync_outside_lock"],
        )
        self.assertEqual(len(prefetch_kwargs_seen), 1)
        self.assertEqual(
            prefetch_kwargs_seen[0].get("load_cached_profile_payloads"),
            False,
            "completed company_roster prefetch must use registry-only marker path",
        )
        queue_mock.assert_called_once()

    def test_completed_search_seed_reconcile_runs_prefetch_outside_writer_lock(self) -> None:
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
        job_id = "job_completed_search_seed_lock_narrowing"
        snapshot_dir = (
            self.settings.company_assets_dir / "openai" / "snapshot-completed-search-seed-lock-narrowing"
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_id = self._seed_search_seed_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        prefetch_kwargs_seen: list[dict] = []
        ordering_log: list[str] = []
        per_job_lock = self.orchestrator._inline_incremental_writer_lock_for_job(job_id)
        sentinel_search_seed_snapshot = SearchSeedSnapshot.__new__(SearchSeedSnapshot)

        def _fake_apply(**_kwargs):
            ordering_log.append("apply_under_lock")
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "search_seed_snapshot": sentinel_search_seed_snapshot,
            }

        def _fake_queue(**kwargs):
            prefetch_kwargs_seen.append(kwargs)
            ordering_log.append("prefetch_outside_lock")
            self.assertTrue(
                per_job_lock.acquire(blocking=False),
                "completed search_seed prefetch must run outside the per-job writer lock",
            )
            per_job_lock.release()
            return {"status": "queued", "queued_worker_count": 0, "dispatched_url_count": 0}

        def _fake_sync(**_kwargs):
            ordering_log.append("sync_outside_lock")
            self.assertTrue(
                per_job_lock.acquire(blocking=False),
                "completed search_seed materialize must run outside the per-job writer lock",
            )
            per_job_lock.release()
            return {
                "status": "completed",
                "candidate_count": 0,
                "evidence_count": 0,
                "writer_scope": "job",
                "sync_policy": "same_kind_micro_batch_single_writer",
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_search_seed_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                side_effect=_fake_queue,
            ) as queue_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"job_id": job_id, "status": "completed", "artifact_path": str(artifact_path)},
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertEqual(
            ordering_log[: 3],
            ["apply_under_lock", "prefetch_outside_lock", "sync_outside_lock"],
        )
        self.assertEqual(len(prefetch_kwargs_seen), 1)
        queue_mock.assert_called_once()

    def test_harvest_profile_terminal_noop_completion_skips_local_apply_closure(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_profile_terminal_noop_callback"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-profile-terminal-noop"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/openai-terminal-noop/"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::terminal-noop",
            stage="enriching",
            span_name="harvest_profile_batch:terminal-noop",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={
                "persisted_profile_count": 0,
                "unresolved_urls": [profile_url],
                "summary": {
                    "status": "completed",
                    "requested_url_count": 1,
                    "persisted_profile_count": 0,
                    "unresolved_url_count": 1,
                },
            },
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_inline_incremental_worker_batch",
            side_effect=AssertionError("terminal no-op profile worker must not run local apply"),
        ):
            result = self.orchestrator._enqueue_local_apply_closure_item_for_completed_worker_result(
                {
                    "worker_id": worker.worker_id,
                    "worker_status": "completed",
                    "source": "worker_completion_callback",
                }
            )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "harvest_profile_terminal_without_materializable_payload")
        items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
        )
        self.assertEqual(items, [])
        worker_after = self.store.get_agent_worker(worker_id=worker.worker_id)
        assert worker_after is not None
        ingest_marker = dict(dict(worker_after.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertTrue(ingest_marker.get("terminal_profile_noop"))
        self.assertEqual(ingest_marker.get("materialization_contract"), "profile_terminal_no_materializable_delta")
        self.assertEqual(ingest_marker.get("candidate_count"), 0)

    def test_existing_local_apply_closure_consumes_harvest_profile_terminal_noop(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_profile_terminal_noop_existing_item"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-profile-terminal-noop-existing"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/openai-terminal-noop-existing/"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::terminal-noop-existing",
            stage="enriching",
            span_name="harvest_profile_batch:terminal-noop-existing",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={
                "persisted_profile_count": 0,
                "unresolved_urls": [profile_url],
                "summary": {
                    "status": "completed",
                    "requested_url_count": 1,
                    "persisted_profile_count": 0,
                    "unresolved_url_count": 1,
                },
            },
        )
        item = self.orchestrator._enqueue_local_apply_closure_item(
            job=self.store.get_job(job_id),
            request=request,
            snapshot_id=snapshot_dir.name,
            worker_kind="harvest_prefetch",
            worker_ids=[worker.worker_id],
            reason="provider_worker_completed_needs_local_apply_closure",
        )
        self.assertEqual(item["status"], "queued")

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_inline_incremental_worker_batch",
            side_effect=AssertionError("terminal no-op profile worker must not run local apply"),
        ):
            result = self.orchestrator._run_local_apply_closure_item_queue_once(
                {"job_id": job_id, "local_apply_closure_item_limit": 1}
            )

        self.assertEqual(result["completed_count"], 1)
        completed_items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="local_apply_closure",
            statuses=["completed"],
        )
        self.assertEqual(len(completed_items), 1)
        self.assertEqual(
            dict(completed_items[0].get("metadata") or {}).get("reason"),
            "harvest_profile_terminal_without_materializable_payload",
        )
        worker_after = self.store.get_agent_worker(worker_id=worker.worker_id)
        assert worker_after is not None
        ingest_marker = dict(dict(worker_after.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertTrue(ingest_marker.get("terminal_profile_noop"))

    def test_harvest_profile_local_apply_closure_disables_inline_board_visible_apply(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_profile_local_apply_no_inline_board_visible"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-profile-local-apply-no-inline-board"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/openai-local-apply-no-inline-board/"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::no-inline-board",
            stage="enriching",
            span_name="harvest_profile_batch:no-inline-board",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )

        def _fake_inline_batch(**kwargs):
            self.assertEqual(int(kwargs.get("board_visible_inline_apply_chunk_limit")), 0)
            return {"status": "processed", "candidate_count": 1}

        with unittest.mock.patch.object(
            self.orchestrator,
            "_process_inline_incremental_worker_batch",
            side_effect=_fake_inline_batch,
        ) as inline_batch:
            result = self.orchestrator._process_local_apply_closure_worker(
                worker=self.store.get_agent_worker(worker_id=worker.worker_id) or {},
                source="unit_local_apply",
                allowed_worker_ids={worker.worker_id},
                profile_apply_budget_ms=8000,
            )

        inline_batch.assert_called_once()
        self.assertEqual(result["status"], "processed")

    def test_completed_workflow_harvest_reconcile_replays_snapshot_cached_profiles_before_final_sync(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_harvest_reconcile_snapshot_cached_profiles"
        current_url = "https://www.linkedin.com/in/openai-agent-current-final-tail/"
        cached_url = "https://www.linkedin.com/in/openai-agent-current-cached/"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-completed-harvest-reconcile-snapshot-cached",
            candidates=[
                Candidate(
                    candidate_id="openai-agent-current-final-tail",
                    name_en="Current Final Tail",
                    display_name="Current Final Tail",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Agent Systems Researcher at OpenAI",
                    linkedin_url=current_url,
                    metadata={"seed_source_type": "harvest_profile_search", "seed_query": "OpenAI Agent"},
                ).to_record(),
                Candidate(
                    candidate_id="openai-agent-current-cached",
                    name_en="Current Cached",
                    display_name="Current Cached",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Agent Research Engineer at OpenAI",
                    linkedin_url=cached_url,
                    metadata={"seed_source_type": "harvest_profile_search", "seed_query": "OpenAI Agent"},
                ).to_record(),
            ],
        )
        current_raw_path = self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=current_url,
            full_name="Current Final Tail",
            headline="Agent Systems Researcher at OpenAI",
            current_company="OpenAI",
            experience=[
                {
                    "title": "Agent Systems Researcher",
                    "companyName": "OpenAI",
                    "startDate": {"year": 2024},
                    "endDate": {"text": "Present"},
                }
            ],
        )
        cached_raw_path = self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=cached_url,
            full_name="Current Cached",
            headline="Agent Research Engineer at OpenAI",
            current_company="OpenAI",
            experience=[
                {
                    "title": "Agent Research Engineer",
                    "companyName": "OpenAI",
                    "startDate": {"year": 2023},
                    "endDate": {"text": "Present"},
                }
            ],
        )
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "status": "completed",
                    "request": request.to_record(),
                    "plan": plan_payload,
                    "summary": {
                        "analysis_stage": "stage_2_final",
                        "candidate_source": {
                            "source_kind": "company_snapshot",
                            "snapshot_id": snapshot_dir.name,
                            "candidate_count": 2,
                        },
                    },
                    "matches": [],
                    "manual_review_items": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "analysis_stage": "stage_2_final",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_dir.name,
                    "candidate_count": 2,
                },
            },
            artifact_path=str(artifact_path),
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::completed-final-tail",
            stage="enriching",
            span_name="harvest_profile_batch:completed-final-tail",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [current_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [current_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [current_url]}},
        )

        sync_result = {
            "status": "completed",
            "reason": "background_harvest_prefetch_reconcile",
            "snapshot_id": snapshot_dir.name,
            "candidate_count": 2,
            "evidence_count": 2,
            "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
            "artifact_paths": {},
            "state_updates": {
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
            },
        }
        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_handle_harvest_profile_completion_event",
                return_value={"profile_prefetch": {"status": "completed", "dispatched_url_count": 0}},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value=sync_result,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={"status": "skipped", "reason": "test"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"artifact_path": str(artifact_path), "status": "completed"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_outreach_layering_requires_background_reconcile",
                return_value=False,
            ),
        ):
            result = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(str(result.get("status") or ""), "reconciled_harvest_prefetch")
        reconciled_candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        reconcile_summary = dict(reconciled_candidate_doc.get("harvest_prefetch_background_reconcile") or {})
        self.assertEqual(int(reconcile_summary.get("requested_url_count") or 0), 1)
        self.assertEqual(int(reconcile_summary.get("reconcile_profile_url_count") or 0), 2)
        self.assertEqual(int(reconcile_summary.get("snapshot_cached_profile_url_count") or 0), 1)
        self.assertTrue(bool(reconcile_summary.get("full_snapshot_cached_profile_reconcile")))
        self.assertEqual(int(reconcile_summary.get("profile_materialized_candidate_count") or 0), 2)
        candidates_by_id = {
            str(item.get("candidate_id") or ""): dict(item)
            for item in list(reconciled_candidate_doc.get("candidates") or [])
        }
        current_candidate = candidates_by_id["openai-agent-current-final-tail"]
        cached_candidate = candidates_by_id["openai-agent-current-cached"]
        self.assertTrue(str(current_candidate.get("work_history") or "").strip())
        self.assertTrue(str(cached_candidate.get("work_history") or "").strip())
        self.assertEqual(str(current_candidate.get("source_path") or ""), str(current_raw_path))
        self.assertEqual(str(cached_candidate.get("source_path") or ""), str(cached_raw_path))

    def test_completed_workflow_reconcile_same_owner_nested_attempt_coalesces(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI workflow runtime people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["workflow"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_reconcile_same_owner_nested_attempt"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"analysis_stage": "stage_2_final"},
        )
        calls: list[str] = []

        def _inner_callback() -> dict[str, str]:
            calls.append("inner")
            return {"status": "inner_ran"}

        def _outer_callback() -> dict[str, object]:
            calls.append("outer")
            nested = self.orchestrator._run_completed_workflow_reconcile_with_inflight_slot(
                job_id,
                reconcile_kind="harvest_prefetch",
                snapshot_id="snapshot-nested-attempt",
                worker_ids=[5],
                callback=_inner_callback,
            )
            calls.append(f"nested:{nested.get('status')}:{nested.get('reason')}")
            return {"status": "outer_ran", "nested": nested}

        with unittest.mock.patch("sourcing_agent.storage._utc_now_timestamp", return_value="2026-05-23 00:00:00"):
            result = self.orchestrator._run_completed_workflow_reconcile_with_inflight_slot(
                job_id,
                reconcile_kind="harvest_prefetch",
                snapshot_id="snapshot-nested-attempt",
                worker_ids=[5],
                callback=_outer_callback,
            )

        self.assertEqual(result.get("status"), "outer_ran")
        self.assertEqual(calls, ["outer", "nested:skipped:completed_workflow_reconcile_inflight"])
        structured_events = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("event_family") == "completed_workflow_reconcile"
        ]
        self.assertTrue(
            any(
                str(event.get("phase") or "") == "coalesced"
                and str(event.get("skip_reason") or "") == "completed_workflow_reconcile_inflight"
                and event.get("worker_ids") == [5]
                for event in structured_events
            )
        )

    def test_snapshot_full_materialization_queue_waits_for_workflow_completion_without_last_error(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_snapshot_full_materialization_waits_for_completion"
        snapshot_id = "snapshot-full-materialization-waits-for-completion"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "analysis_stage": "stage_2_final",
                "background_snapshot_materialization": {
                    "status": "scheduled",
                    "snapshot_id": snapshot_id,
                },
            },
        )
        self.orchestrator._enqueue_snapshot_full_materialization_item(job_id=job_id, source="unit_test")

        recovery = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "post_completion_reconcile_enabled": False,
                "snapshot_full_materialization_item_limit": 1,
            }
        )

        snapshot_queue = dict(recovery.get("snapshot_full_materialization") or {})
        self.assertEqual(snapshot_queue["claimed_count"], 1)
        self.assertEqual(snapshot_queue["failed_count"], 0)
        waiting_items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="snapshot_full_materialization",
            statuses=["waiting_prerequisite"],
        )
        self.assertEqual(len(waiting_items), 1)
        self.assertEqual(waiting_items[0]["last_error"], "")
        waiting_metadata = dict(waiting_items[0].get("metadata") or {})
        self.assertEqual(str(waiting_metadata.get("failure_reason") or ""), "job_not_completed")
        self.assertEqual(
            str(dict(waiting_metadata.get("snapshot_full_materialization") or {}).get("reason") or ""),
            "job_not_completed",
        )

    def test_snapshot_full_materialization_release_clears_old_wait_error(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_snapshot_full_materialization_release_clears_error"
        snapshot_id = "snapshot-full-materialization-release-clears-error"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "analysis_stage": "stage_2_final",
                "background_snapshot_materialization": {
                    "status": "scheduled",
                    "snapshot_id": snapshot_id,
                },
            },
        )
        item = self.orchestrator._enqueue_snapshot_full_materialization_item(
            job_id=job_id,
            source="unit_test",
            metadata={"pending_until_workflow_completion": True},
        )
        self.store.mark_job_materialization_item_failed(
            str(item.get("item_id") or ""),
            error_text="job_not_completed",
            retryable=True,
            metadata={
                "pending_until_workflow_completion": True,
                "failure_reason": "job_not_completed",
            },
        )

        release = self.orchestrator._release_snapshot_full_materialization_items_for_completed_workflow(
            job_id=job_id,
            source="unit_test_release",
        )

        self.assertEqual(str(release.get("status") or ""), "released")
        queued_items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="snapshot_full_materialization",
            statuses=["queued"],
        )
        self.assertEqual(len(queued_items), 1)
        self.assertEqual(queued_items[0]["last_error"], "")

    def test_snapshot_full_materialization_queue_does_not_scan_summary_without_item(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Health people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Health"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_snapshot_full_materialization_summary_only"
        snapshot_id = "snapshot-full-materialization-summary-only"
        snapshot_dir = self.settings.company_assets_dir / "openai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "analysis_stage": "stage_2_final",
                "background_snapshot_materialization": {
                    "status": "scheduled",
                    "snapshot_id": snapshot_id,
                    "reason": "background_snapshot_materialization_reconcile",
                },
            },
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value={"status": "completed"},
            ) as sync_mock,
            unittest.mock.patch.object(self.orchestrator, "_execute_retrieval") as retrieval_mock,
        ):
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": job_id,
                    "post_completion_reconcile_enabled": False,
                    "snapshot_full_materialization_item_limit": 1,
                }
            )

        snapshot_queue = dict(recovery.get("snapshot_full_materialization") or {})
        self.assertEqual(snapshot_queue["status"], "idle")
        self.assertEqual(snapshot_queue["reason"], "no_ready_snapshot_full_materialization_items")
        self.assertNotIn("enqueue_scan", snapshot_queue)
        sync_mock.assert_not_called()
        retrieval_mock.assert_not_called()
        items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="snapshot_full_materialization",
        )
        self.assertEqual(items, [])

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

    def test_refresh_running_workflow_before_retrieval_defers_materialization_only_refresh_for_preview(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find xAI people",
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_materialization_deferred"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-pre-retrieval-preview"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("preview refresh should not force a materialization-only sync"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
                include_materialization_refresh=False,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "materialization_refresh_deferred")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        self.assertFalse(bool(dict(refreshed_job.get("summary") or {}).get("pre_retrieval_refresh")))

    def test_refresh_running_workflow_before_retrieval_skips_sync_when_preview_can_use_stage_candidate_documents(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI coding people",
            "target_company": "OpenAI",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Coding"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_preview_stage_candidate_docs"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-preview-stage-candidate-docs"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity.to_record(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {"company_identity": identity.to_record()},
                    "candidates": [
                        Candidate(
                            candidate_id="openai-preview-base",
                            name_en="Preview Base",
                            display_name="Preview Base",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-preview-base/",
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
        linkedin_stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        linkedin_stage_candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-preview-stage",
                            name_en="Preview Stage",
                            display_name="Preview Stage",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-preview-stage/",
                            metadata={"headline": "Engineer at OpenAI"},
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
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::preview-skip",
            stage="enriching",
            span_name="harvest_profile_batch:preview-skip",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-preview-stage/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed"}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("stage-1 preview should skip sync when stage candidate docs are ready"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "linkedin_stage_candidate_doc_path": linkedin_stage_candidate_doc_path,
                },
                include_materialization_refresh=False,
                allow_stage_candidate_document_fast_path=True,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "stage_candidate_documents_ready")
        self.assertEqual(refresh["snapshot_id"], snapshot_dir.name)
        self.assertEqual(int(refresh["harvest_prefetch_worker_count"] or 0), 1)
        self.assertEqual(refresh["stage_candidate_doc_path"], str(linkedin_stage_candidate_doc_path))

    def test_refresh_running_workflow_before_retrieval_defers_harvest_prefetch_for_direct_finalization(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI multimodal people",
            "target_company": "OpenAI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["multimodal"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_direct_finalization_harvest_deferred"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-direct-finalization-harvest-deferred"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-direct-finalization",
                            name_en="Direct Finalization Candidate",
                            display_name="Direct Finalization Candidate",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Research Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-direct-finalization/",
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
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::direct-finalization",
            stage="enriching",
            span_name="harvest_profile_batch:direct-finalization",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-direct-finalization/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed"}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("direct finalization should not block on harvest-prefetch sync"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
                include_materialization_refresh=False,
                defer_harvest_prefetch_refresh_to_background=True,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "harvest_prefetch_refresh_deferred_to_background")
        self.assertEqual(refresh["snapshot_id"], snapshot_dir.name)
        self.assertEqual(int(refresh["harvest_prefetch_worker_count"] or 0), 1)
        self.assertTrue(bool(refresh.get("materialization_refresh_pending")))
        self.assertTrue(bool(refresh.get("materialization_refresh_deferred")))

    def test_refresh_running_workflow_before_retrieval_skips_materialization_for_equivalent_baseline_reuse(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find xAI people",
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_equivalent_baseline_reuse"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-pre-retrieval-equivalent-baseline"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "normalized_artifacts").mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "normalized_artifacts" / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                    "candidate_shards": [],
                    "pages": [],
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("equivalent baseline reuse should skip pre-retrieval materialization refresh"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "equivalent_baseline_reused": True,
                    "equivalent_baseline_snapshot_id": snapshot_dir.name,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "equivalent_baseline_reused")

    def test_refresh_running_workflow_before_retrieval_skips_materialization_for_reused_serving_snapshot(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_reused_serving_snapshot"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-pre-retrieval-reused-serving",
            candidates=[],
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="Reflection AI",
            include_strict=True,
            include_serving_docs=True,
        )
        stale_time = time.time() - 60
        fresh_time = time.time() + 60
        os.utime(candidate_doc_path, (fresh_time, fresh_time))
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json",
            snapshot_dir / "normalized_artifacts" / "reusable_candidate_documents.json",
        ):
            os.utime(path, (stale_time, stale_time))
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )

        materialized_candidate_doc_path = snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("reused serving snapshot should skip pre-retrieval materialization refresh"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": materialized_candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "reused_snapshot_serving_artifacts")

    def test_refresh_running_workflow_before_retrieval_skips_materialization_for_reused_snapshot_candidate_documents(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI post-train people",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Post-train"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_reused_snapshot_candidate_docs"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-pre-retrieval-reused-candidate-docs",
            candidates=[
                Candidate(
                    candidate_id="cand_reuse_checkpoint",
                    name_en="Reuse Checkpoint",
                    display_name="Reuse Checkpoint",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/reuse-checkpoint/",
                ).to_record()
            ],
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("reused snapshot candidate documents should skip pre-retrieval materialization refresh"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "reused_snapshot_checkpoint")

    def test_run_workflow_from_acquisition_reused_snapshot_short_circuits_after_public_web_stage(self) -> None:
        request_payload = {
            "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
            "target_company": "Reflection AI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Post-train"],
            "top_k": 10,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-direct-finalization",
            candidates=[
                Candidate(
                    candidate_id="cand_direct_finalization",
                    name_en="Direct Finalization",
                    display_name="Direct Finalization",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/direct-finalization/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_direct_finalization_former",
                    name_en="Direct Finalization Former",
                    display_name="Direct Finalization Former",
                    category="former_employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="former",
                    role="Research Scientist",
                    linkedin_url="https://www.linkedin.com/in/direct-finalization-former/",
                ).to_record(),
            ],
        )
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflection-ai",
            linkedin_company_url="https://www.linkedin.com/company/reflection-ai/",
        )
        job_id = "job_reused_snapshot_direct_finalization"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Planning completed."},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_load_reusable_snapshot_state",
            return_value={
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
                "company_identity": identity,
                "reused_snapshot_checkpoint": True,
                "linkedin_stage_completed": True,
                "public_web_stage_completed": True,
                "candidates": [
                    Candidate(
                        candidate_id="cand_direct_finalization",
                        name_en="Direct Finalization",
                        display_name="Direct Finalization",
                        category="employee",
                        target_company="Reflection AI",
                        organization="Reflection AI",
                        employment_status="current",
                        role="Research Engineer",
                        linkedin_url="https://www.linkedin.com/in/direct-finalization/",
                    ),
                    Candidate(
                        candidate_id="cand_direct_finalization_former",
                        name_en="Direct Finalization Former",
                        display_name="Direct Finalization Former",
                        category="former_employee",
                        target_company="Reflection AI",
                        organization="Reflection AI",
                        employment_status="former",
                        role="Research Scientist",
                        linkedin_url="https://www.linkedin.com/in/direct-finalization-former/",
                    ),
                ],
            },
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_publish_stage1_preview_after_linkedin_stage",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_mark_public_web_stage_2_completed",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_running_workflow_before_retrieval",
            return_value={"status": "skipped", "reason": "reused_snapshot_checkpoint"},
        ) as refresh_running_workflow_before_retrieval, unittest.mock.patch.object(
            self.orchestrator,
            "_run_outreach_layering_after_acquisition",
            return_value={},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_execute_retrieval",
            return_value={
                "artifact_path": str(self.settings.jobs_dir / f"{job_id}.json"),
                "summary": {
                    "text": "Local asset population is ready with 1 candidates.",
                    "analysis_stage": "stage_2_final",
                },
                "matches": [],
            },
        ) as execute_retrieval, unittest.mock.patch.object(
            self.acquisition_engine,
            "execute_task",
            side_effect=AssertionError("reused snapshot should skip residual acquisition task execution"),
        ):
            result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(execute_retrieval.call_count, 1)
        refresh_running_workflow_before_retrieval.assert_called_once()
        self.assertFalse(
            bool(refresh_running_workflow_before_retrieval.call_args.kwargs.get("include_materialization_refresh", True))
        )
        self.assertFalse(bool(execute_retrieval.call_args.kwargs.get("persist_job_state", True)))
        self.assertEqual(
            str(dict(execute_retrieval.call_args.kwargs.get("runtime_policy") or {}).get("mode") or ""),
            "direct_asset_population_finalization",
        )
        self.assertEqual(
            str(
                dict(dict(execute_retrieval.call_args.kwargs.get("runtime_policy") or {}).get(
                    "background_snapshot_materialization"
                ) or {}).get("status")
                or ""
            ),
            "deferred",
        )
        latest_job = self.store.get_job(job_id) or {}
        progress = dict(dict(latest_job.get("summary") or {}).get("acquisition_progress") or {})
        completed_task_types = {
            str(dict(payload or {}).get("task_type") or "").strip()
            for payload in dict(progress.get("tasks") or {}).values()
        }
        self.assertIn("normalize_asset_snapshot", completed_task_types)
        self.assertIn("build_retrieval_index", completed_task_types)

    def test_run_workflow_from_acquisition_merges_harvest_defer_into_background_materialization(self) -> None:
        request_payload = {
            "raw_user_request": "我想要OpenAI做Multimodal方向的人",
            "target_company": "OpenAI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["multimodal"],
            "top_k": 10,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        plan.acquisition_tasks = []
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-openai-direct-finalization-harvest-defer",
            candidates=[
                Candidate(
                    candidate_id="cand_openai_harvest_defer",
                    name_en="Harvest Defer",
                    display_name="Harvest Defer",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/openai-harvest-defer/",
                ).to_record()
            ],
        )
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        job_id = "job_direct_finalization_harvest_defer"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Planning completed."},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_restore_acquisition_state",
            return_value={
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
                "company_identity": identity,
                "reused_snapshot_checkpoint": True,
                "linkedin_stage_completed": True,
                "public_web_stage_completed": True,
                "candidates": [
                    Candidate(
                        candidate_id="cand_openai_harvest_defer",
                        name_en="Harvest Defer",
                        display_name="Harvest Defer",
                        category="employee",
                        target_company="OpenAI",
                        organization="OpenAI",
                        employment_status="current",
                        role="Research Engineer",
                        linkedin_url="https://www.linkedin.com/in/openai-harvest-defer/",
                    )
                ],
            },
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_publish_stage1_preview_after_linkedin_stage",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_mark_public_web_stage_2_completed",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_running_workflow_before_retrieval",
            return_value={
                "status": "skipped",
                "reason": "harvest_prefetch_refresh_deferred_to_background",
                "harvest_prefetch_worker_count": 2,
            },
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_run_outreach_layering_after_acquisition",
            return_value={},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_execute_retrieval",
            return_value={
                "artifact_path": str(self.settings.jobs_dir / f"{job_id}.json"),
                "summary": {
                    "text": "Local asset population is ready with 1 candidates.",
                    "analysis_stage": "stage_2_final",
                },
                "matches": [],
            },
        ) as execute_retrieval, unittest.mock.patch.object(
            self.acquisition_engine,
            "execute_task",
            side_effect=AssertionError("reused snapshot should skip residual acquisition task execution"),
        ):
            result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(result["status"], "completed")
        runtime_policy = dict(execute_retrieval.call_args.kwargs.get("runtime_policy") or {})
        background_snapshot_materialization = dict(runtime_policy.get("background_snapshot_materialization") or {})
        self.assertEqual(str(background_snapshot_materialization.get("status") or ""), "deferred")
        self.assertEqual(int(background_snapshot_materialization.get("harvest_prefetch_worker_count") or 0), 2)
        self.assertEqual(
            list(background_snapshot_materialization.get("deferred_components") or []),
            ["snapshot_materialization", "harvest_prefetch_refresh"],
        )

    def test_refresh_running_workflow_before_retrieval_uses_snapshot_id_fallback_for_equivalent_baseline_reuse(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find xAI people",
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_equivalent_baseline_reuse_fallback"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-pre-retrieval-equivalent-baseline-fallback"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                    "evidence": [],
                    "candidate_count": 0,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "xAI",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "normalized_artifacts").mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "normalized_artifacts" / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                    "candidate_shards": [],
                    "pages": [],
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

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("equivalent baseline reuse should skip refresh when snapshot_id fallback matches"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state={
                    "snapshot_id": snapshot_dir.name,
                    "manifest_path": manifest_path,
                    "candidate_doc_path": candidate_doc_path,
                    "equivalent_baseline_reused": True,
                },
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "equivalent_baseline_reused")

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

    def test_manual_review_items_are_snapshot_scoped_and_cleanup_old_snapshots(self) -> None:
        old_snapshot_path = str(
            self.settings.company_assets_dir / "acme" / "20260406T120000" / "candidate_documents.json"
        )
        new_snapshot_path = str(
            self.settings.company_assets_dir / "acme" / "20260407T120000" / "candidate_documents.json"
        )
        old_item = {
            "candidate_id": "cand_manual",
            "target_company": "Acme",
            "review_type": "manual_identity_resolution",
            "priority": "high",
            "status": "open",
            "summary": "Old snapshot item.",
            "candidate": {
                "candidate_id": "cand_manual",
                "name_en": "Alice Example",
                "display_name": "Alice Example",
                "category": "lead",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": old_snapshot_path,
                "metadata": {},
            },
            "evidence": [
                {
                    "candidate_id": "cand_manual",
                    "source_type": "publication_match",
                    "source_path": old_snapshot_path,
                    "metadata": {},
                }
            ],
            "metadata": {},
        }
        old_other_item = {
            "candidate_id": "cand_old_only",
            "target_company": "Acme",
            "review_type": "needs_human_validation",
            "priority": "medium",
            "status": "open",
            "summary": "Old-only snapshot item.",
            "candidate": {
                "candidate_id": "cand_old_only",
                "name_en": "Bob Old",
                "display_name": "Bob Old",
                "category": "employee",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": old_snapshot_path,
                "metadata": {},
            },
            "evidence": [],
            "metadata": {},
        }
        new_item = {
            "candidate_id": "cand_manual",
            "target_company": "Acme",
            "review_type": "manual_identity_resolution",
            "priority": "high",
            "status": "open",
            "summary": "New snapshot item.",
            "candidate": {
                "candidate_id": "cand_manual",
                "name_en": "Alice Example",
                "display_name": "Alice Example",
                "category": "lead",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": new_snapshot_path,
                "metadata": {},
            },
            "evidence": [
                {
                    "candidate_id": "cand_manual",
                    "source_type": "publication_match",
                    "source_path": new_snapshot_path,
                    "metadata": {},
                }
            ],
            "metadata": {},
        }

        self.store.repos.manual_review.replace_items("job_old", [old_item, old_other_item])
        self.store.repos.manual_review.replace_items("job_new", [new_item])

        all_items = self.store.repos.manual_review.list_items(target_company="Acme", status="", limit=10)
        status_by_candidate = {(item["candidate_id"], item["job_id"]): item["status"] for item in all_items}
        snapshot_by_candidate = {
            (item["candidate_id"], item["job_id"]): item["metadata"].get("snapshot_id") for item in all_items
        }
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_old")], "20260406T120000")
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_new")], "20260407T120000")
        self.assertEqual(status_by_candidate[("cand_manual", "job_old")], "superseded")
        self.assertEqual(status_by_candidate[("cand_manual", "job_new")], "open")

        cleanup = self.store.repos.manual_review.cleanup_items(target_company="Acme", snapshot_id="20260407T120000")
        self.assertGreaterEqual(cleanup["out_of_scope_count"], 1)
        open_items = self.store.repos.manual_review.list_items(target_company="Acme", status="open", limit=10)
        self.assertEqual(len(open_items), 1)
        self.assertEqual(open_items[0]["candidate_id"], "cand_manual")
        self.assertEqual(open_items[0]["metadata"].get("snapshot_id"), "20260407T120000")

    def test_mark_linkedin_profile_registry_queued_preserves_fetched_when_raw_exists(self) -> None:
        profile_url = "https://www.linkedin.com/in/registry-preserve/"
        raw_path = str(
            self.settings.company_assets_dir / "reflectionai" / "registry-preserve" / "harvest_profiles" / "cached.json"
        )
        self.store.repos.linkedin_profile_registry.mark_fetched(
            profile_url,
            raw_path=raw_path,
            snapshot_dir=str(self.settings.company_assets_dir / "reflectionai" / "registry-preserve"),
        )

        updated = self.store.repos.linkedin_profile_registry.mark_queued(
            profile_url,
            snapshot_dir=str(self.settings.company_assets_dir / "reflectionai" / "queued-overwrite-attempt"),
        )

        self.assertEqual(str(updated.get("status") or ""), "fetched")
        self.assertEqual(str(updated.get("last_raw_path") or ""), raw_path)

    def test_snapshot_materializer_pre_retrieval_refresh_uses_foreground_fast_artifact_profile(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-foreground-fast"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_sync_fast_1",
            name_en="Alice Fast",
            display_name="Alice Fast",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-fast/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        manifest_path = snapshot_dir / "manifest.json"
        captured_execution_preferences: dict[str, object] = {}

        def fake_normalize_snapshot(
            task: AcquisitionTask,
            state: dict[str, object],
            job_request: JobRequest | None = None,
        ) -> AcquisitionExecution:
            captured_execution_preferences.update(dict(getattr(job_request, "execution_preferences", {}) or {}))
            return AcquisitionExecution(
                task_id=str(task.task_id),
                status="completed",
                detail="Normalized and materialized snapshot.",
                payload={
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "manifest_path": str(manifest_path),
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "manifest": str(snapshot_dir / "normalized_artifacts" / "manifest.json"),
                    },
                    "sync_status": {"overall_status": "completed"},
                },
                state_updates={"manifest_path": manifest_path},
            )

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_normalize_snapshot",
                side_effect=fake_normalize_snapshot,
            ) as normalize_snapshot,
            unittest.mock.patch(
                "sourcing_agent.snapshot_materializer.build_company_candidate_artifacts",
                side_effect=AssertionError("should not rebuild artifacts when normalize already did"),
            ),
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="pre_retrieval_refresh",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(str(captured_execution_preferences.get("artifact_build_profile") or ""), "foreground_fast")
        normalize_snapshot.assert_called_once()

    def test_snapshot_materializer_background_harvest_refresh_uses_foreground_fast_artifact_profile(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-background-harvest-fast"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_sync_harvest_fast_1",
            name_en="Alice Harvest Fast",
            display_name="Alice Harvest Fast",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-harvest-fast/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        captured_execution_preferences: dict[str, object] = {}

        def fake_normalize_snapshot(
            task: AcquisitionTask,
            state: dict[str, object],
            job_request: JobRequest | None = None,
        ) -> AcquisitionExecution:
            captured_execution_preferences.update(dict(getattr(job_request, "execution_preferences", {}) or {}))
            return AcquisitionExecution(
                task_id=str(task.task_id),
                status="completed",
                detail="Normalized and materialized snapshot.",
                payload={
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "manifest_path": str(snapshot_dir / "manifest.json"),
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "manifest": str(snapshot_dir / "normalized_artifacts" / "manifest.json"),
                    },
                    "sync_status": {"overall_status": "completed"},
                },
                state_updates={"manifest_path": snapshot_dir / "manifest.json"},
            )

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_normalize_snapshot",
            side_effect=fake_normalize_snapshot,
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="background_harvest_prefetch_reconcile",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(str(captured_execution_preferences.get("artifact_build_profile") or ""), "foreground_fast")

    def test_snapshot_materializer_background_snapshot_reconcile_uses_foreground_fast_artifact_profile(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-background-materialization-fast"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_sync_snapshot_fast_1",
            name_en="Alice Snapshot Fast",
            display_name="Alice Snapshot Fast",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-snapshot-fast/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        captured_execution_preferences: dict[str, object] = {}

        def fake_normalize_snapshot(
            task: AcquisitionTask,
            state: dict[str, object],
            job_request: JobRequest | None = None,
        ) -> AcquisitionExecution:
            captured_execution_preferences.update(dict(getattr(job_request, "execution_preferences", {}) or {}))
            return AcquisitionExecution(
                task_id=str(task.task_id),
                status="completed",
                detail="Normalized and materialized snapshot.",
                payload={
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "manifest_path": str(snapshot_dir / "manifest.json"),
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "manifest": str(snapshot_dir / "normalized_artifacts" / "manifest.json"),
                    },
                    "sync_status": {"overall_status": "completed"},
                },
                state_updates={"manifest_path": snapshot_dir / "manifest.json"},
            )

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_normalize_snapshot",
            side_effect=fake_normalize_snapshot,
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="background_snapshot_materialization_reconcile",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(str(captured_execution_preferences.get("artifact_build_profile") or ""), "foreground_fast")

