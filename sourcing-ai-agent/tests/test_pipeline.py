import contextlib
import io
import json
import os
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
from sourcing_agent.asset_reuse_planning import (
    backfill_organization_asset_registry_for_company,
    build_acquisition_shard_registry_record,
    compile_asset_reuse_plan,
    ensure_organization_asset_registry,
    evaluate_organization_asset_registry_promotion,
)
from sourcing_agent.cli import run_server_runtime_watchdog_once
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.connectors import CompanyIdentity, CompanyRosterSnapshot
from sourcing_agent.domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.enrichment import MultiSourceEnrichmentResult
from sourcing_agent.harvest_connectors import HarvestExecutionResult
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
from sourcing_agent.storage import SQLiteStore
from sourcing_agent.workflow_refresh import _worker_is_terminal_for_acquisition_resume


class PipelineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = AssetCatalog.discover()
        self.store = SQLiteStore(f"{self.tempdir.name}/test.db")
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

    def test_startup_organization_asset_warmup_is_opt_in_by_default(self) -> None:
        with unittest.mock.patch.dict(os.environ, {"STARTUP_ORGANIZATION_ASSET_WARMUP_ENABLED": ""}, clear=False):
            status = self.orchestrator.start_background_organization_asset_warmup()

        self.assertEqual(status["status"], "disabled")
        self.assertEqual(status["reason"], "startup_warmup_disabled")
        persisted = json.loads((Path(self.tempdir.name) / "runtime_metrics" / "organization_asset_warmup.json").read_text())
        self.assertEqual(persisted["status"], "disabled")

    def _write_company_snapshot_candidate_documents(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidates: list[dict[str, object]],
    ) -> tuple[Path, Path]:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / normalize_company_key(target_company) / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
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
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": target_company,
                        "canonical_name": target_company,
                        "company_key": normalize_company_key(target_company),
                        "linkedin_slug": normalize_company_key(target_company),
                    },
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
        self.store.mark_linkedin_profile_registry_fetched(
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
            profile_url_key = self.store.normalize_linkedin_profile_url(linkedin_url) if linkedin_url else ""
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

    def test_bootstrap_and_job_execution(self) -> None:
        summary = self.orchestrator.bootstrap()
        self.assertGreaterEqual(summary["candidate_count"], 120)
        self.assertGreaterEqual(summary["candidate_breakdown"].get("employee", 0), 100)

        result = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练", "推理"],
                "top_k": 3,
            }
        )
        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["name_en"], "Da Yan")
        self.assertIn("intent_rewrite", result)
        self.assertFalse(result["intent_rewrite"]["request"]["matched"])

    def test_plan_workflow_returns_request_preview_and_infers_gemini_product_manager_scope(self) -> None:
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想找Gemini的产品经理",
            }
        )

        self.assertEqual(planned["request"]["target_company"], "Google")
        self.assertEqual(planned["request"]["employment_statuses"], ["current", "former"])
        self.assertEqual(planned["request"]["must_have_primary_role_buckets"], ["product_management"])
        self.assertIn("Gemini", planned["request"]["organization_keywords"])
        self.assertIn("Google DeepMind", planned["request"]["organization_keywords"])
        self.assertEqual(planned["request_preview"]["target_company"], "Google")
        self.assertEqual(planned["request_preview"]["must_have_primary_role_buckets"], ["product_management"])
        self.assertIn("Gemini", planned["request_preview"]["organization_keywords"])
        self.assertEqual(
            planned["request_preview"]["intent_axes"]["population_boundary"]["employment_statuses"],
            ["current", "former"],
        )
        self.assertEqual(
            planned["request_preview"]["intent_axes"]["scope_boundary"]["target_company"],
            "Google",
        )
        self.assertEqual(
            planned["request_preview"]["intent_axes"]["thematic_constraints"]["must_have_primary_role_buckets"],
            ["product_management"],
        )
        self.assertTrue(
            any(
                item.get("rewrite_id") == "greater_china_outreach"
                for item in list(planned["intent_rewrite"].get("policy_catalog") or [])
                if isinstance(item, dict)
            )
        )
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["filter_hints"]["function_ids"],
            ["19"],
        )
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["strategy_type"],
            "scoped_search_roster",
        )
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["filter_hints"]["current_companies"],
            ["Google"],
        )
        self.assertEqual(
            planned["plan"]["organization_execution_profile"]["default_acquisition_mode"],
            "scoped_search_roster",
        )

    def test_resolve_company_identity_uses_manual_target_company_linkedin_override(self) -> None:
        task = AcquisitionTask(
            task_id="resolve_company_identity",
            task_type="resolve_company_identity",
            title="Resolve company identity",
            description="Resolve target company to LinkedIn identity.",
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Safe Superintelligence Inc 的成员",
                "target_company": "Safe Superintelligence Inc",
                "execution_preferences": {
                    "target_company_linkedin_url": "https://www.linkedin.com/company/ssi-ai/",
                },
            }
        )

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_discover_company_identity_candidates",
            side_effect=AssertionError("manual override should bypass observed-company lookup"),
        ):
            execution = self.acquisition_engine.execute_task(
                task,
                request,
                "Safe Superintelligence Inc",
                state={},
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["company_identity"]["linkedin_slug"], "ssi-ai")
        self.assertEqual(
            execution.payload["company_identity"]["linkedin_company_url"],
            "https://www.linkedin.com/company/ssi-ai/",
        )
        self.assertEqual(execution.payload["company_identity"]["resolver"], "manual_review_override")

    def test_plan_workflow_task_metadata_carries_effective_request(self) -> None:
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想找Gemini的产品经理",
            }
        )

        first_task = next(
            task
            for task in list(planned["plan"].get("acquisition_tasks") or [])
            if isinstance(task, dict)
            and isinstance(dict(task.get("metadata") or {}).get("intent_view"), dict)
            and dict(dict(task.get("metadata") or {}).get("intent_view") or {}).get("effective_request")
        )
        intent_view = dict(dict(first_task.get("metadata") or {}).get("intent_view") or {})
        effective_request = dict(intent_view.get("effective_request") or {})
        self.assertEqual(effective_request.get("target_company"), "Google")
        self.assertEqual(effective_request.get("employment_statuses"), ["current", "former"])
        self.assertEqual(effective_request.get("must_have_primary_role_buckets"), ["product_management"])
        self.assertIn("Gemini", list(effective_request.get("organization_keywords") or []))

    def test_plan_workflow_uses_cached_authoritative_baseline_without_runtime_backfill(self) -> None:
        snapshot_id = "snapshot-cached-registry-only"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_reflection_cached",
                    name_en="Infra One",
                    display_name="Infra One",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Infrastructure Engineer",
                    focus_areas="infra platform",
                    linkedin_url="https://www.linkedin.com/in/reflection-cached/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.asset_reuse_planning.ensure_organization_asset_registry",
                side_effect=AssertionError("plan path should not backfill organization registry"),
            ),
            unittest.mock.patch(
                "sourcing_agent.asset_reuse_planning.ensure_organization_completeness_ledger",
                side_effect=AssertionError("plan path should not rebuild completeness ledger when cache exists"),
            ),
        ):
            planned = self.orchestrator.plan_workflow(
                {
                    "raw_user_request": "帮我找Reflection AI做Infra方向的人",
                    "target_company": "Reflection AI",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "keywords": ["Infra"],
                }
            )

        asset_reuse_plan = dict(planned["plan"].get("asset_reuse_plan") or {})
        self.assertTrue(asset_reuse_plan.get("baseline_reuse_available"))
        self.assertEqual(asset_reuse_plan.get("baseline_snapshot_id"), snapshot_id)
        self.assertEqual(asset_reuse_plan.get("baseline_resolution_mode"), "cached_only")

    def test_plan_workflow_infers_openai_scope_from_chatgpt_product_manager_query(self) -> None:
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想找ChatGPT的产品经理",
            }
        )

        self.assertEqual(planned["request"]["target_company"], "OpenAI")
        self.assertEqual(planned["request"]["employment_statuses"], ["current", "former"])
        self.assertEqual(planned["request"]["must_have_primary_role_buckets"], ["product_management"])
        self.assertEqual(planned["request_preview"]["target_company"], "OpenAI")
        self.assertIn("ChatGPT", planned["request"]["organization_keywords"])
        self.assertIn("ChatGPT", planned["request_preview"]["organization_keywords"])
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["filter_hints"]["function_ids"],
            ["19"],
        )

    def test_plan_workflow_materializes_intent_axes_only_request_normalization(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "intent_axes": {
                        "population_boundary": {
                            "categories": ["employee"],
                            "employment_statuses": ["current", "former"],
                        },
                        "scope_boundary": {
                            "target_company": "Google",
                            "organization_keywords": ["Google DeepMind", "Gemini"],
                            "scope_disambiguation": {
                                "inferred_scope": "both",
                                "sub_org_candidates": ["Google DeepMind", "Gemini"],
                                "confidence": 0.81,
                            },
                        },
                        "acquisition_lane_policy": {
                            "keyword_priority_only": True,
                        },
                        "fallback_policy": {
                            "provider_people_search_query_strategy": "all_queries_union",
                            "run_former_search_seed": True,
                        },
                        "thematic_constraints": {
                            "must_have_primary_role_buckets": ["product_management"],
                            "keywords": ["Gemini"],
                        },
                    }
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, RequestNormalizingModelClient()
            ),
        )

        planned = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想找Gemini的产品经理",
            }
        )

        self.assertEqual(planned["request"]["target_company"], "Google")
        self.assertEqual(planned["request"]["employment_statuses"], ["current", "former"])
        self.assertEqual(planned["request"]["must_have_primary_role_buckets"], ["product_management"])
        self.assertEqual(
            planned["request"]["execution_preferences"]["provider_people_search_query_strategy"],
            "all_queries_union",
        )
        self.assertTrue(planned["request"]["execution_preferences"]["keyword_priority_only"])
        self.assertTrue(planned["request"]["execution_preferences"]["run_former_search_seed"])
        self.assertIn("Gemini", planned["request"]["organization_keywords"])
        self.assertEqual(planned["request_preview"]["intent_axes"]["scope_boundary"]["target_company"], "Google")
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["filter_hints"]["function_ids"],
            ["19"],
        )

    def test_plan_workflow_preserves_unknown_meta_team_keyword_without_hardcoded_mapping(self) -> None:
        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想找Meta TBD的产品经理",
            }
        )

        self.assertEqual(planned["request"]["target_company"], "Meta")
        self.assertEqual(planned["request"]["must_have_primary_role_buckets"], ["product_management"])
        self.assertIn("TBD", planned["request"]["organization_keywords"])
        self.assertNotIn("Meta TBD", planned["request"]["organization_keywords"])
        self.assertIn("TBD", planned["request_preview"]["organization_keywords"])
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["filter_hints"]["function_ids"],
            ["19"],
        )

    def test_execute_retrieval_defaults_to_deterministic_summary(self) -> None:
        candidate = Candidate(
            candidate_id="acme_1",
            name_en="Alex Builder",
            display_name="Alex Builder",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infra Engineer",
            focus_areas="GPU systems",
            linkedin_url="https://www.linkedin.com/in/alex-builder/",
            source_dataset="sqlite_candidates",
        )
        self.store.upsert_candidate(candidate)

        class _ExplodingModel(DeterministicModelClient):
            def summarize(self, request, matches, total_matches):  # type: ignore[override]
                raise AssertionError("model summary should be disabled by default")

        self.orchestrator.model_client = _ExplodingModel()
        self.acquisition_engine.model_client = self.orchestrator.model_client

        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找做 GPU systems 的 infra engineer",
                "target_company": "",
                "target_scope": "scoped_search",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["GPU systems", "infra"],
            }
        )
        request = JobRequest.from_payload(dict(planned["request"]))
        artifact = self.orchestrator._execute_retrieval(
            "job_deterministic_default",
            request,
            dict(planned["plan"]),
            job_type="workflow",
        )
        self.assertEqual(artifact["summary"]["summary_provider"], "deterministic")
        self.assertEqual(artifact["summary"]["returned_matches"], 1)

    def test_execute_retrieval_skips_ranked_scoring_for_asset_population_default_view(self) -> None:
        snapshot_id = "20260417T090000"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="anthropic_1",
            name_en="Ada Researcher",
            display_name="Ada Researcher",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-researcher/",
            source_dataset="candidate_documents",
            source_path=str(snapshot_dir / "candidate_documents.json"),
            metadata={"headline": "Research Engineer at Anthropic"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [candidate.to_record()], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 做 Pre-training 的人",
                "target_company": "Anthropic",
                "target_scope": "scoped_search",
                "employment_statuses": ["current", "former"],
                "keywords": ["Pre-train"],
                "must_have_facets": ["pre_training"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 1,
            },
        }

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.score_candidates", side_effect=AssertionError("should skip ranked scoring")
        ):
            artifact = self.orchestrator._execute_retrieval(
                "job_asset_population_fast_path",
                request,
                plan,
                job_type="workflow",
                runtime_policy={"workflow_snapshot_id": snapshot_id, "analysis_stage": "stage_1_preview"},
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["summary_provider"], "asset_population_fast_path")
        self.assertTrue(artifact["summary"]["asset_population_fast_path"])
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], 1)
        self.assertEqual(artifact["summary"]["returned_matches"], 1)
        self.assertEqual(artifact["matches"], [])
        self.assertIn("Stage 1 preview is ready", artifact["summary"]["text"])

    def test_execute_retrieval_persists_job_result_view_for_snapshot_backed_results(self) -> None:
        snapshot_id = "20260418T030303"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="anthropic_result_view_1",
            name_en="Nina Systems",
            display_name="Nina Systems",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Systems Engineer",
            linkedin_url="https://www.linkedin.com/in/nina-systems/",
            source_dataset="candidate_documents",
            source_path=str(snapshot_dir / "candidate_documents.json"),
            metadata={"headline": "Systems Engineer at Anthropic"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [candidate.to_record()], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Anthropic 的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "asset_view": "canonical_merged",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
                "source_snapshot_id": snapshot_id,
                "source_generation_key": "gen_anthropic_1",
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 1,
            },
        }

        artifact = self.orchestrator._execute_retrieval(
            "job_asset_population_result_view_persisted",
            request,
            plan,
            job_type="workflow",
            runtime_policy={"workflow_snapshot_id": snapshot_id},
            persist_job_state=True,
        )

        self.assertEqual(artifact["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        result_view = self.store.get_job_result_view(job_id="job_asset_population_result_view_persisted")
        assert result_view is not None
        self.assertEqual(result_view["source_kind"], "company_snapshot")
        self.assertEqual(result_view["view_kind"], "asset_population")
        self.assertEqual(result_view["company_key"], "anthropic")
        self.assertEqual(result_view["snapshot_id"], snapshot_id)
        self.assertEqual(result_view["authoritative_snapshot_id"], snapshot_id)
        self.assertEqual(result_view["materialization_generation_key"], "gen_anthropic_1")
        self.assertTrue(str(result_view.get("request_signature") or ""))

        saved_job = self.store.get_job("job_asset_population_result_view_persisted")
        assert saved_job is not None
        saved_candidate_source = dict(dict(saved_job.get("summary") or {}).get("candidate_source") or {})
        self.assertEqual(saved_candidate_source["result_view_id"], result_view["view_id"])
        self.assertEqual(saved_candidate_source["result_view_kind"], "asset_population")
        self.assertEqual(saved_candidate_source["authoritative_snapshot_id"], snapshot_id)
        self.assertEqual(saved_candidate_source["materialization_generation_key"], "gen_anthropic_1")

    def test_execute_retrieval_stage1_preview_skips_materialization_fallback_for_snapshot_candidates(self) -> None:
        snapshot_id = "20260418T040404"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="anthropic_preview_skip_materialization",
            name_en="Ada Preview",
            display_name="Ada Preview",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-preview/",
            source_dataset="candidate_documents",
            source_path=str(snapshot_dir / "candidate_documents.json"),
            metadata={"headline": "Research Engineer at Anthropic"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [candidate.to_record()], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 做 Pre-training 的人",
                "target_company": "Anthropic",
                "target_scope": "scoped_search",
                "employment_statuses": ["current", "former"],
                "keywords": ["Pre-train"],
                "must_have_facets": ["pre_training"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 1,
            },
        }

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.build_company_candidate_artifacts",
                side_effect=AssertionError("stage1 preview should not force materialization fallback"),
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.score_candidates",
                side_effect=AssertionError("stage1 preview should stay on asset-population fast path"),
            ),
        ):
            artifact = self.orchestrator._execute_retrieval(
                "job_asset_population_preview_skip_materialization",
                request,
                plan,
                job_type="workflow",
                runtime_policy={"workflow_snapshot_id": snapshot_id, "analysis_stage": "stage_1_preview"},
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["summary_provider"], "asset_population_fast_path")
        self.assertTrue(artifact["summary"]["asset_population_fast_path"])
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], 1)
        self.assertEqual(artifact["summary"]["returned_matches"], 1)

    def test_execute_retrieval_stage1_preview_can_read_stage_candidate_documents_directly(self) -> None:
        snapshot_id = "20260418T040505"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        candidate = Candidate(
            candidate_id="anthropic_preview_stage_doc_direct",
            name_en="Stage Preview Ada",
            display_name="Stage Preview Ada",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/stage-preview-ada/",
            source_dataset="candidate_documents",
            source_path=str(stage_candidate_doc_path),
            metadata={"headline": "Research Engineer at Anthropic"},
        )
        stage_candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": "Anthropic",
                        "company_identity": {
                            "requested_name": "Anthropic",
                            "canonical_name": "Anthropic",
                            "company_key": "anthropic",
                            "linkedin_slug": "anthropic",
                        },
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

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 做 Pre-training 的人",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Pre-train"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "asset_view": "canonical_merged",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
                "source_snapshot_id": snapshot_id,
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 1,
            },
        }

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.build_company_candidate_artifacts",
                side_effect=AssertionError("stage preview direct candidate-doc source should avoid materialization"),
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.score_candidates",
                side_effect=AssertionError("stage preview direct candidate-doc source should stay on fast path"),
            ),
        ):
            artifact = self.orchestrator._execute_retrieval(
                "job_asset_population_preview_stage_candidate_doc_direct",
                request,
                plan,
                job_type="workflow",
                runtime_policy={
                    "workflow_snapshot_id": snapshot_id,
                    "workflow_stage_candidate_doc_path": str(stage_candidate_doc_path),
                    "analysis_stage": "stage_1_preview",
                },
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["summary_provider"], "asset_population_fast_path")
        self.assertTrue(artifact["summary"]["asset_population_fast_path"])
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], 1)
        self.assertEqual(
            artifact["summary"]["candidate_source"]["source_path"],
            str(stage_candidate_doc_path),
        )
        self.assertEqual(artifact["summary"]["returned_matches"], 1)

    def test_execute_retrieval_stage1_preview_prefers_authoritative_candidate_documents_for_asset_population(
        self,
    ) -> None:
        snapshot_id = "20260418T040606"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        full_candidates = [
            Candidate(
                candidate_id=f"anthropic_preview_full_{index}",
                name_en=f"Full Candidate {index}",
                display_name=f"Full Candidate {index}",
                category="employee",
                target_company="Anthropic",
                organization="Anthropic",
                employment_status="current" if index < 2 else "former",
                role="Research Engineer",
                linkedin_url=f"https://www.linkedin.com/in/full-candidate-{index}/",
                source_dataset="candidate_documents",
                source_path=str(candidate_doc_path),
                metadata={"headline": "Research Engineer at Anthropic"},
            )
            for index in range(3)
        ]
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "Anthropic",
                    "snapshot_id": snapshot_id,
                    "candidates": [candidate.to_record() for candidate in full_candidates],
                    "evidence": [],
                    "candidate_count": len(full_candidates),
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        stage_candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": "Anthropic",
                        "company_identity": {
                            "requested_name": "Anthropic",
                            "canonical_name": "Anthropic",
                            "company_key": "anthropic",
                            "linkedin_slug": "anthropic",
                        },
                    },
                    "candidates": [full_candidates[0].to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 做 Reasoning 的人",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "asset_view": "canonical_merged",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
                "source_snapshot_id": snapshot_id,
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": len(full_candidates),
            },
        }

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.build_company_candidate_artifacts",
                side_effect=AssertionError("stage preview should stay on asset-population fast path"),
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.score_candidates",
                side_effect=AssertionError("stage preview should stay on asset-population fast path"),
            ),
        ):
            artifact = self.orchestrator._execute_retrieval(
                "job_asset_population_preview_prefers_authoritative_candidate_doc",
                request,
                plan,
                job_type="workflow",
                runtime_policy={
                    "workflow_snapshot_id": snapshot_id,
                    "workflow_candidate_doc_path": str(candidate_doc_path),
                    "workflow_stage_candidate_doc_path": str(stage_candidate_doc_path),
                    "analysis_stage": "stage_1_preview",
                },
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["summary_provider"], "asset_population_fast_path")
        self.assertTrue(artifact["summary"]["asset_population_fast_path"])
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], len(full_candidates))
        self.assertEqual(
            artifact["summary"]["candidate_source"]["source_path"],
            str(candidate_doc_path),
        )
        self.assertEqual(artifact["summary"]["returned_matches"], len(full_candidates))

    def test_load_workflow_stage_summaries_resolves_snapshot_via_job_result_view(self) -> None:
        job_id = "job_stage_summary_result_view"
        snapshot_id = "snapshot-stage-summary-result-view"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        materialized_path = normalized_dir / "materialized_candidate_documents.json"
        materialized_path.write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stage_summary_dir = snapshot_dir / "workflow_stage_summaries"
        stage_summary_dir.mkdir(parents=True, exist_ok=True)
        (stage_summary_dir / "stage_2_final.json").write_text(
            json.dumps(
                {
                    "stage": "stage_2_final",
                    "message": "loaded from job result view",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Anthropic 的全部成员",
                "target_company": "Anthropic",
                "employment_statuses": ["current", "former"],
                "top_k": 10,
            }
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload={"message": "Workflow completed."},
        )
        self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Anthropic",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id=snapshot_id,
            source_path=str(materialized_path),
            summary={"candidate_count": 0},
        )

        saved_job = self.store.get_job(job_id)
        assert saved_job is not None
        stage_summaries = self.orchestrator._load_workflow_stage_summaries(job=saved_job)

        self.assertEqual(Path(stage_summaries["directory"]).resolve(), stage_summary_dir.resolve())
        self.assertEqual(
            stage_summaries["summaries"]["stage_2_final"]["message"],
            "loaded from job result view",
        )

    def test_load_workflow_stage_summaries_repairs_stage1_preview_asset_population_counts(self) -> None:
        snapshot_id = "snapshot-stage1-preview-repair"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "Anthropic",
                    "snapshot_id": snapshot_id,
                    "candidates": [
                        {"candidate_id": "cand_1"},
                        {"candidate_id": "cand_2"},
                        {"candidate_id": "cand_3"},
                    ],
                    "evidence": [],
                    "candidate_count": 3,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        stage_candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "target_company": "Anthropic",
                    },
                    "candidates": [{"candidate_id": "cand_1"}],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        stage_summary_dir = snapshot_dir / "workflow_stage_summaries"
        stage_summary_dir.mkdir(parents=True, exist_ok=True)
        stage1_summary_path = stage_summary_dir / "stage_1_preview.json"
        stage2_summary_path = stage_summary_dir / "stage_2_final.json"
        stage1_summary_path.write_text(
            json.dumps(
                {
                    "stage": "stage_1_preview",
                    "analysis_stage": "stage_1_preview",
                    "summary_provider": "asset_population_fast_path",
                    "returned_matches": 1,
                    "total_matches": 1,
                    "manual_review_queue_count": 1,
                    "candidate_source": {
                        "source_kind": "company_snapshot",
                        "snapshot_id": snapshot_id,
                        "asset_view": "canonical_merged",
                        "candidate_count": 1,
                        "source_path": str(candidate_doc_path),
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        stage2_summary_path.write_text(
            json.dumps(
                {
                    "stage": "stage_2_final",
                    "analysis_stage": "stage_2_final",
                    "default_results_mode": "asset_population",
                    "returned_matches": 3,
                    "total_matches": 3,
                    "manual_review_queue_count": 0,
                    "candidate_source": {
                        "source_kind": "company_snapshot",
                        "snapshot_id": snapshot_id,
                        "asset_view": "canonical_merged",
                        "candidate_count": 3,
                        "source_path": str(candidate_doc_path),
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        job = {
            "job_id": "job_stage1_preview_repair",
            "request": {
                "raw_user_request": "帮我找 Anthropic 做 Reasoning 的人",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
            },
            "summary": {
                "stage1_preview": {"summary_path": str(stage1_summary_path)},
                "stage_summary_path": str(stage2_summary_path),
                "default_results_mode": "asset_population",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 3,
                    "source_path": str(candidate_doc_path),
                },
            },
        }

        stage_summaries = self.orchestrator._load_workflow_stage_summaries(job=job)
        repaired_stage1_summary = dict(stage_summaries["summaries"]["stage_1_preview"])

        self.assertEqual(repaired_stage1_summary["returned_matches"], 3)
        self.assertEqual(repaired_stage1_summary["total_matches"], 3)
        self.assertEqual(repaired_stage1_summary["manual_review_queue_count"], 0)
        self.assertEqual(
            dict(repaired_stage1_summary["candidate_source"])["source_path"],
            str(candidate_doc_path),
        )
        self.assertTrue(repaired_stage1_summary["repaired_from_stage_2_final"])

    def test_load_workflow_stage_summaries_fills_missing_stage_timestamps_from_job_progression(self) -> None:
        job = {
            "job_id": "job_stage_summary_fallback_timestamps",
            "created_at": "2026-04-22T10:00:00+00:00",
            "updated_at": "2026-04-22T10:00:05+00:00",
            "request": {
                "raw_user_request": "帮我找 OpenAI 做 Coding 的人",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
            },
            "summary": {
                "status": "completed",
                "analysis_stage": "stage_2_final",
                "stage_2_final_started_at": "2026-04-22T10:00:04+00:00",
                "completed_at": "2026-04-22T10:00:05+00:00",
                "linkedin_stage_1": {
                    "status": "completed",
                    "completed_at": "2026-04-22T10:00:02+00:00",
                },
                "stage1_preview": {
                    "status": "ready",
                    "returned_matches": 47,
                    "manual_review_queue_count": 5,
                },
                "public_web_stage_2": {
                    "status": "completed",
                    "started_at": "2026-04-22T10:00:03+00:00",
                    "completed_at": "2026-04-22T10:00:04+00:00",
                },
            },
        }

        stage_summaries = self.orchestrator._load_workflow_stage_summaries(job=job)
        linkedin_stage = dict(stage_summaries["summaries"]["linkedin_stage_1"])
        stage1_preview = dict(stage_summaries["summaries"]["stage_1_preview"])
        final_stage = dict(stage_summaries["summaries"]["stage_2_final"])

        self.assertEqual(str(linkedin_stage.get("started_at") or ""), "2026-04-22T10:00:00Z")
        self.assertEqual(str(linkedin_stage.get("completed_at") or ""), "2026-04-22T10:00:02+00:00")
        self.assertEqual(str(stage1_preview.get("started_at") or ""), "2026-04-22T10:00:02Z")
        self.assertEqual(str(stage1_preview.get("completed_at") or ""), "2026-04-22T10:00:03Z")
        self.assertEqual(int(stage1_preview.get("returned_matches") or 0), 47)
        self.assertEqual(str(final_stage.get("started_at") or ""), "2026-04-22T10:00:04Z")
        self.assertEqual(str(final_stage.get("completed_at") or ""), "2026-04-22T10:00:05+00:00")

    def test_get_job_results_exposes_top_level_request_preview(self) -> None:
        result = self.orchestrator.run_job(
            {
                "raw_user_request": "我想找Gemini的产品经理",
            }
        )

        payload = self.orchestrator.get_job_results(str(result["job_id"]))
        assert payload is not None
        self.assertEqual(payload["request_preview"]["target_company"], "Google")
        self.assertEqual(payload["request_preview"]["must_have_primary_role_buckets"], ["product_management"])

    def test_run_job_prefers_latest_company_snapshot_over_broader_sqlite_pool(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_dir = company_dir / "20260408T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260408T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        snapshot_candidate = Candidate(
            candidate_id="snap_1",
            name_en="Snapshot Researcher",
            display_name="Snapshot Researcher",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            focus_areas="alignment research",
            source_dataset="acme_snapshot",
        )
        off_snapshot_candidate = Candidate(
            candidate_id="sqlite_1",
            name_en="SQLite GPU Engineer",
            display_name="SQLite GPU Engineer",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="gpu cluster platform",
            source_dataset="acme_sqlite_only",
        )
        self.store.upsert_candidate(snapshot_candidate)
        self.store.upsert_candidate(off_snapshot_candidate)
        (normalized_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["gpu"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertEqual(result["summary"]["candidate_source"]["snapshot_id"], "20260408T120000")
        self.assertEqual(result["summary"]["candidate_source"]["asset_view"], "canonical_merged")
        self.assertEqual(result["summary"]["summary_provider"], "asset_population_fast_path")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["summary"]["returned_matches"], 1)
        self.assertEqual(result["matches"], [])

    def test_cleanup_duplicate_inflight_workflows_supersedes_older_active_job(self) -> None:
        request_payload = {
            "target_company": "Reflection AI",
            "raw_user_request": "给我Reflection AI的Infra方向的人",
        }
        active_job_id = "job_active_duplicate"
        completed_job_id = "job_completed_duplicate"

        self.store.save_job(
            job_id=active_job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload=request_payload,
            plan_payload={},
            summary_payload={"message": "Retrieving candidates"},
        )
        session = self.store.create_agent_runtime_session(
            job_id=active_job_id,
            target_company="Reflection AI",
            request_payload=request_payload,
            plan_payload={},
            runtime_mode="workflow",
            lanes=[{"lane_id": "enrichment_specialist"}],
        )
        span = self.store.create_agent_trace_span(
            session_id=int(session["session_id"]),
            job_id=active_job_id,
            lane_id="enrichment_specialist",
            span_name="duplicate cleanup",
            stage="acquiring",
        )
        worker = self.store.create_or_resume_agent_worker(
            session_id=int(session["session_id"]),
            job_id=active_job_id,
            span_id=int(span["span_id"]),
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::duplicate",
            metadata={"recovery_kind": "harvest_profile_batch"},
        )
        self.store.mark_agent_worker_running(int(worker["worker_id"]))

        self.store.save_job(
            job_id=completed_job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request_payload,
            plan_payload={},
            summary_payload={"message": "Workflow completed."},
        )

        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET created_at = '2026-04-11 10:00:00', updated_at = '2026-04-11 10:00:00' WHERE job_id = ?",
                (active_job_id,),
            )
            self.store._connection.execute(
                "UPDATE jobs SET created_at = '2026-04-11 11:00:00', updated_at = '2026-04-11 11:00:00' WHERE job_id = ?",
                (completed_job_id,),
            )

        cleanup = self.orchestrator.cleanup_duplicate_inflight_workflows({"target_company": "Reflection AI"})

        self.assertEqual(cleanup["status"], "completed")
        self.assertEqual(cleanup["superseded_count"], 1)
        self.assertEqual(cleanup["superseded_jobs"][0]["job_id"], active_job_id)
        self.assertEqual(cleanup["superseded_jobs"][0]["replacement_job_id"], completed_job_id)
        refreshed_job = self.store.get_job(active_job_id)
        self.assertEqual(refreshed_job["status"], "superseded")
        refreshed_worker = self.store.get_agent_worker(worker_id=int(worker["worker_id"]))
        self.assertEqual(refreshed_worker["status"], "superseded")

    def test_cleanup_blocked_workflow_residue_supersedes_older_family_job(self) -> None:
        blocked_job_id = "job_blocked_family_residue"
        replacement_job_id = "job_completed_family_replacement"
        base_request = {
            "target_company": "Reflection AI",
            "raw_user_request": "给我Reflection AI的Infra方向的成员",
            "categories": ["employee", "former_employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Infra"],
        }
        blocked_request = {**base_request, "top_k": 10}
        replacement_request = {**base_request, "top_k": 5}

        self.store.save_job(
            job_id=blocked_job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=blocked_request,
            plan_payload={},
            summary_payload={"message": "Awaiting resume", "blocked_task": "acquire_full_roster"},
        )
        self.store.save_job(
            job_id=replacement_job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=replacement_request,
            plan_payload={},
            summary_payload={"message": "Workflow completed."},
        )

        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET created_at = '2026-04-12 18:18:59', updated_at = '2026-04-12 18:39:58' WHERE job_id = ?",
                (blocked_job_id,),
            )
            self.store._connection.execute(
                "UPDATE jobs SET created_at = '2026-04-12 18:25:48', updated_at = '2026-04-12 18:25:57' WHERE job_id = ?",
                (replacement_job_id,),
            )

        cleanup = self.orchestrator.cleanup_blocked_workflow_residue({"target_company": "Reflection AI"})

        self.assertEqual(cleanup["status"], "completed")
        self.assertEqual(cleanup["cleanup_candidate_count"], 1)
        self.assertEqual(cleanup["cleaned_count"], 1)
        self.assertEqual(cleanup["results"][0]["job_id"], blocked_job_id)
        self.assertEqual(cleanup["results"][0]["replacement_job_id"], replacement_job_id)
        refreshed_job = self.store.get_job(blocked_job_id)
        self.assertEqual(refreshed_job["status"], "superseded")

    def test_runtime_health_surfaces_cleanup_eligible_blocked_jobs_without_degrading(self) -> None:
        blocked_job_id = "job_health_blocked_family_residue"
        replacement_job_id = "job_health_completed_family_replacement"
        base_request = {
            "target_company": "Reflection AI",
            "raw_user_request": "给我Reflection AI的Infra方向的成员",
            "categories": ["employee", "former_employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Infra"],
        }
        blocked_request = {**base_request, "top_k": 10}
        replacement_request = {**base_request, "top_k": 5}

        self.store.save_job(
            job_id=blocked_job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=blocked_request,
            plan_payload={},
            summary_payload={"message": "Awaiting resume", "blocked_task": "acquire_full_roster"},
        )
        self.store.save_job(
            job_id=replacement_job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=replacement_request,
            plan_payload={},
            summary_payload={"message": "Workflow completed."},
        )

        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET created_at = '2026-04-12 18:18:59', updated_at = '2026-04-12 18:39:58' WHERE job_id = ?",
                (blocked_job_id,),
            )
            self.store._connection.execute(
                "UPDATE jobs SET created_at = '2026-04-12 18:25:48', updated_at = '2026-04-12 18:25:57' WHERE job_id = ?",
                (replacement_job_id,),
            )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.read_service_status",
            return_value={"status": "running", "lock_status": "locked", "pid_alive": True},
        ):
            health = self.orchestrator.get_runtime_health({"active_limit": 10, "force_refresh": True})

        self.assertEqual(health["status"], "ok")
        self.assertEqual(int(health["metrics"]["stalled_job_count"] or 0), 0)
        self.assertEqual(int(health["metrics"]["cleanup_eligible_blocked_job_count"] or 0), 1)
        self.assertEqual(health["cleanup_eligible_blocked_jobs"][0]["job_id"], blocked_job_id)

    def test_cleanup_recoverable_workers_retires_terminal_workflow_workers(self) -> None:
        failed_request = {
            "target_company": "Anthropic",
            "raw_user_request": "failed worker cleanup",
        }
        failed_job_id = "job_failed_worker_cleanup"
        self.store.save_job(
            job_id=failed_job_id,
            job_type="workflow",
            status="failed",
            stage="failed",
            request_payload=failed_request,
            plan_payload={},
            summary_payload={"message": "Workflow failed."},
        )
        failed_session = self.store.create_agent_runtime_session(
            job_id=failed_job_id,
            target_company="Anthropic",
            request_payload=failed_request,
            plan_payload={},
            runtime_mode="workflow",
            lanes=[{"lane_id": "enrichment_specialist"}],
        )
        failed_span = self.store.create_agent_trace_span(
            session_id=int(failed_session["session_id"]),
            job_id=failed_job_id,
            lane_id="enrichment_specialist",
            span_name="failed cleanup",
            stage="acquiring",
        )
        failed_worker = self.store.create_or_resume_agent_worker(
            session_id=int(failed_session["session_id"]),
            job_id=failed_job_id,
            span_id=int(failed_span["span_id"]),
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::failed-cleanup",
            metadata={"recovery_kind": "harvest_profile_batch"},
        )
        self.store.mark_agent_worker_running(int(failed_worker["worker_id"]))
        self.store.checkpoint_agent_worker(
            int(failed_worker["worker_id"]),
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={},
            status="running",
        )

        superseded_request = {
            "target_company": "Reflection AI",
            "raw_user_request": "superseded worker cleanup",
        }
        superseded_job_id = "job_superseded_worker_cleanup"
        self.store.save_job(
            job_id=superseded_job_id,
            job_type="workflow",
            status="superseded",
            stage="completed",
            request_payload=superseded_request,
            plan_payload={},
            summary_payload={"message": "Workflow superseded."},
        )
        superseded_session = self.store.create_agent_runtime_session(
            job_id=superseded_job_id,
            target_company="Reflection AI",
            request_payload=superseded_request,
            plan_payload={},
            runtime_mode="workflow",
            lanes=[{"lane_id": "exploration_specialist"}],
        )
        superseded_span = self.store.create_agent_trace_span(
            session_id=int(superseded_session["session_id"]),
            job_id=superseded_job_id,
            lane_id="exploration_specialist",
            span_name="superseded cleanup",
            stage="retrieving",
        )
        superseded_worker = self.store.create_or_resume_agent_worker(
            session_id=int(superseded_session["session_id"]),
            job_id=superseded_job_id,
            span_id=int(superseded_span["span_id"]),
            lane_id="exploration_specialist",
            worker_key="search::superseded-cleanup",
            metadata={},
        )
        self.store.checkpoint_agent_worker(
            int(superseded_worker["worker_id"]),
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={},
            status="queued",
        )

        preview = self.orchestrator.cleanup_recoverable_workers({"dry_run": True, "limit": 20})
        self.assertEqual(preview["status"], "preview")
        self.assertEqual(preview["candidate_count"], 2)

        cleanup = self.orchestrator.cleanup_recoverable_workers({"limit": 20})
        self.assertEqual(cleanup["status"], "completed")
        self.assertEqual(cleanup["retired_count"], 2)

        refreshed_failed = self.store.get_agent_worker(worker_id=int(failed_worker["worker_id"]))
        self.assertEqual(refreshed_failed["status"], "cancelled")
        self.assertEqual(
            dict(refreshed_failed.get("metadata") or {}).get("cleanup", {}).get("source"), "recoverable_worker_cleanup"
        )

        refreshed_superseded = self.store.get_agent_worker(worker_id=int(superseded_worker["worker_id"]))
        self.assertEqual(refreshed_superseded["status"], "superseded")

    def test_superseded_worker_is_terminal_for_acquisition_resume(self) -> None:
        self.assertTrue(_worker_is_terminal_for_acquisition_resume({"status": "superseded"}))

    def test_run_job_can_explicitly_use_strict_roster_only_view(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_dir = company_dir / "20260408T120000"
        normalized_dir = snapshot_dir / "normalized_artifacts"
        strict_dir = normalized_dir / "strict_roster_only"
        strict_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260408T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        canonical_candidate = Candidate(
            candidate_id="canon_1",
            name_en="Canonical Researcher",
            display_name="Canonical Researcher",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            focus_areas="alignment research",
            source_dataset="acme_snapshot",
        )
        strict_candidate = Candidate(
            candidate_id="strict_1",
            name_en="Strict Recruiter",
            display_name="Strict Recruiter",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Talent Recruiter",
            focus_areas="technical recruiting",
            source_dataset="acme_strict_snapshot",
        )
        (normalized_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [canonical_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (strict_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [strict_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "Acme",
                "asset_view": "strict_roster_only",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["recruiting"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertEqual(result["summary"]["candidate_source"]["asset_view"], "strict_roster_only")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["candidate_id"], "strict_1")
        self.assertEqual(result["matches"][0]["role_bucket"], "recruiting")
        self.assertIn("recruiting", result["matches"][0]["functional_facets"])

    def test_execute_retrieval_uses_plan_inferred_member_filters(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="emp_1",
                    name_en="Infra Lead",
                    display_name="Infra Lead",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Head of GPU Infrastructure",
                    focus_areas="infrastructure platform",
                ),
                Candidate(
                    candidate_id="inv_1",
                    name_en="Investor",
                    display_name="Investor",
                    category="investor",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Investor at Reflection AI",
                    focus_areas="ai infrastructure investing",
                ),
            ],
            [],
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我寻找Reflection AI的Infra方向在职成员",
                "target_company": "Reflection AI",
                "keywords": ["infra"],
                "top_k": 5,
            }
        )
        self.assertEqual(request.categories, [])
        self.assertEqual(request.employment_statuses, ["current"])

        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        artifact = self.orchestrator._execute_retrieval(
            job_id="plan_inferred_filters",
            request=request,
            plan=plan,
            job_type="workflow",
            persist_job_state=False,
        )

        self.assertEqual(artifact["summary"]["total_matches"], 1)
        self.assertEqual([item["candidate_id"] for item in artifact["matches"]], ["emp_1"])
        self.assertEqual(
            artifact["summary"]["effective_request_overrides"]["categories"],
            ["employee", "former_employee"],
        )
        self.assertNotIn("employment_statuses", artifact["summary"].get("effective_request_overrides", {}))

    def test_execute_retrieval_defaults_to_current_and_former_member_pool(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="emp_1",
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
                    candidate_id="former_1",
                    name_en="Former Infra",
                    display_name="Former Infra",
                    category="former_employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="former",
                    role="Infrastructure Engineer",
                    focus_areas="infra platform",
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
            }
        )
        self.assertEqual(request.employment_statuses, ["current", "former"])
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        artifact = self.orchestrator._execute_retrieval(
            job_id="plan_inferred_filters_all_members",
            request=request,
            plan=plan,
            job_type="workflow",
            persist_job_state=False,
        )

        self.assertEqual(artifact["summary"]["total_matches"], 2)
        self.assertEqual(
            {item["candidate_id"] for item in artifact["matches"]},
            {"emp_1", "former_1"},
        )
        self.assertEqual(
            artifact["summary"]["effective_request_overrides"]["categories"],
            ["employee", "former_employee"],
        )
        self.assertNotIn("employment_statuses", artifact["summary"].get("effective_request_overrides", {}))

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

    def test_ai_first_request_normalization_preserves_ambiguous_team_terms_for_search(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "target_company": "Meta",
                    "organization_keywords": ["Meta TBD"],
                    "keywords": ["infra"],
                    "scope_disambiguation": {
                        "inferred_scope": "uncertain",
                        "sub_org_candidates": ["Meta TBD"],
                        "confidence": 0.41,
                        "rationale": "TBD may be a sub-org or team label, but the term is ambiguous.",
                    },
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, RequestNormalizingModelClient()
            ),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "给我 Meta TBD 的 infra 成员",
            }
        )

        self.assertEqual(plan_result["request"]["target_company"], "Meta")
        self.assertEqual(plan_result["request"]["employment_statuses"], ["current", "former"])
        self.assertEqual(plan_result["request"]["organization_keywords"], ["TBD"])
        self.assertIn("infra", [item.lower() for item in plan_result["request"]["keywords"]])
        self.assertEqual(plan_result["request"]["scope_disambiguation"]["inferred_scope"], "uncertain")
        self.assertEqual(plan_result["request"]["scope_disambiguation"]["sub_org_candidates"], ["TBD"])
        filter_hint_keywords = list(plan_result["plan"]["acquisition_strategy"]["filter_hints"]["keywords"] or [])
        self.assertIn("infra", [item.lower() for item in filter_hint_keywords])
        self.assertIn("TBD", filter_hint_keywords)
        self.assertTrue(
            any("TBD" in query for query in plan_result["plan"]["acquisition_strategy"]["search_seed_queries"])
        )
        self.assertTrue(any("TBD" in item for item in plan_result["plan"]["open_questions"]))

    def test_request_normalization_does_not_keep_full_parenthetical_chinese_scaffold_as_keyword(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "给我Google做多模态（在Veo和Nano Banana团队）的人",
                "target_company": "Google",
            }
        )

        request_keywords = list(plan_result["request"]["keywords"] or [])
        self.assertIn("Veo", request_keywords)
        self.assertIn("Nano Banana", request_keywords)
        self.assertNotIn("在Veo和Nano Banana团队", request_keywords)
        search_seed_queries = list(plan_result["plan"]["acquisition_strategy"]["search_seed_queries"] or [])
        self.assertNotIn("在Veo和Nano Banana团队", search_seed_queries)

    def test_ai_first_request_normalization_does_not_reintroduce_parenthetical_wrapper_phrase(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "target_company": "Google",
                    "employment_statuses": ["current", "former"],
                    "organization_keywords": ["Google DeepMind", "Veo", "Nano Banana"],
                    "keywords": ["multimodal", "Veo", "Nano Banana"],
                    "must_have_facets": ["multimodal"],
                    "scope_disambiguation": {
                        "inferred_scope": "both",
                        "sub_org_candidates": ["Google DeepMind", "Veo", "Nano Banana"],
                        "confidence": 0.82,
                    },
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, RequestNormalizingModelClient()
            ),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "给我Google做多模态（在Veo和Nano Banana团队）的人",
                "target_company": "Google",
            }
        )

        request_keywords = list(plan_result["request"]["keywords"] or [])
        self.assertIn("multimodal", request_keywords)
        self.assertIn("Veo", request_keywords)
        self.assertIn("Nano Banana", request_keywords)
        self.assertNotIn("在Veo和Nano Banana团队", request_keywords)
        search_seed_queries = list(plan_result["plan"]["acquisition_strategy"]["search_seed_queries"] or [])
        self.assertFalse(any("在Veo和Nano Banana团队" in query for query in search_seed_queries))

    def test_run_job_supports_must_have_facet_hard_filter(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="recruit_1",
                    name_en="Recruiter",
                    display_name="Recruiter",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Technical Recruiter",
                    focus_areas="technical recruiting",
                ),
                Candidate(
                    candidate_id="eng_1",
                    name_en="Engineer",
                    display_name="Engineer",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Software Engineer",
                    focus_areas="training systems",
                    notes="Occasionally helps with recruiting events.",
                ),
            ],
            [],
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "FacetCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "must_have_facet": "recruiting",
                "keywords": ["recruiting"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["candidate_id"], "recruit_1")

    def test_run_job_supports_primary_role_bucket_hard_filter(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="founder_1",
                    name_en="Founder",
                    display_name="Founder",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Founder and CTO",
                    focus_areas="infrastructure systems platform",
                ),
                Candidate(
                    candidate_id="infra_1",
                    name_en="Infra Engineer",
                    display_name="Infra Engineer",
                    category="employee",
                    target_company="FacetCo",
                    organization="FacetCo",
                    employment_status="current",
                    role="Infrastructure Engineer",
                    focus_areas="distributed systems runtime platform",
                ),
            ],
            [],
        )

        result = self.orchestrator.run_job(
            {
                "target_company": "FacetCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "must_have_facet": "infra_systems",
                "must_have_primary_role_bucket": "infra",
                "keywords": ["infra"],
                "top_k": 5,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["summary"]["total_matches"], 1)
        self.assertEqual(result["matches"][0]["candidate_id"], "infra_1")
        self.assertEqual(result["matches"][0]["role_bucket"], "infra_systems")

    def test_plan_and_async_workflow(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 xAI 当前偏基础设施和训练系统的核心技术成员，先做全量资产获取再检索。",
                "target_company": "xAI",
            }
        )
        self.assertEqual(plan_result["plan"]["target_company"], "xAI")
        self.assertEqual(plan_result["plan"]["retrieval_plan"]["strategy"], "structured")
        self.assertGreaterEqual(len(plan_result["plan"]["acquisition_tasks"]), 4)
        self.assertTrue(plan_result["plan"]["intent_brief"]["identified_request"])
        self.assertTrue(plan_result["plan"]["intent_brief"]["default_execution_strategy"])
        self.assertEqual(plan_result["plan_review_gate"]["status"], "requires_review")
        self.assertEqual(plan_result["plan_review_session"]["status"], "pending")

        gated = self.orchestrator.start_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的华人技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        self.assertEqual(gated["status"], "needs_plan_review")
        review_id = gated["plan_review_session"]["review_id"]
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "extra_source_families": ["Engineering Blog"],
                },
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")
        workflow = self.orchestrator.start_workflow({"plan_review_id": review_id})
        self.assertIn("job_id", workflow)
        for _ in range(300):
            snapshot = self.orchestrator.get_job_results(workflow["job_id"])
            if snapshot and snapshot["job"]["status"] in {"completed", "blocked", "failed"}:
                asset_population_count = int(dict(snapshot.get("asset_population") or {}).get("candidate_count") or 0)
                if snapshot["job"]["status"] != "completed" or asset_population_count > 0 or snapshot["results"]:
                    break
            time.sleep(0.05)
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["job"]["status"], "completed")
        asset_population = dict(snapshot.get("asset_population") or {})
        self.assertTrue(asset_population.get("available"))
        self.assertGreaterEqual(int(asset_population.get("candidate_count") or 0), 1)
        self.assertTrue(snapshot["agent_runtime_session"])
        self.assertGreaterEqual(len(snapshot["agent_trace_spans"]), 2)

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

    def test_build_sourcing_plan_omits_former_search_seed_for_current_only_requests(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find Acme infra researchers",
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["infra"],
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        task_types = [task.task_type for task in plan.acquisition_tasks]
        self.assertNotIn("acquire_former_search_seed", task_types)

    def test_build_sourcing_plan_full_company_defaults_do_not_require_high_cost_approval(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Surge AI 的全部成员",
                "target_company": "Surge AI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
            }
        )

        plan = build_sourcing_plan(request, self.catalog, self.model_client)

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertFalse(
            any("high-cost LinkedIn provider calls" in item for item in list(plan.open_questions or []))
        )
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")
        self.assertEqual(former_task.metadata["search_channel_order"], ["harvest_profile_search"])
        self.assertEqual(former_task.metadata["cost_policy"]["provider_people_search_mode"], "primary_only")
        self.assertEqual(former_task.metadata["search_query_bundles"], [])

    def test_acquire_former_search_seed_uses_primary_provider_search_mode(self) -> None:
        task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="",
            source_hint="",
            status="ready",
            blocking=True,
            metadata={
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
                "filter_hints": {},
                "search_seed_queries": ["infra"],
                "former_provider_people_search_min_expected_results": 25,
            },
        )
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
        )
        state = {
            "company_identity": identity,
            "snapshot_dir": self.settings.company_assets_dir / "reflectionai" / "snap1",
        }
        state["snapshot_dir"].mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Reflection AI",
                "execution_preferences": {},
            }
        )
        sentinel = AcquisitionExecution(task_id=task.task_id, status="completed", detail="ok", payload={})
        with unittest.mock.patch.object(
            self.acquisition_engine, "_acquire_search_seed_pool", return_value=sentinel
        ) as mocked:
            result = self.acquisition_engine._acquire_former_search_seed(task, state, request)
        self.assertIs(result, sentinel)
        delegated_task = mocked.call_args.args[0]
        self.assertEqual(delegated_task.metadata["cost_policy"]["provider_people_search_mode"], "primary_only")
        self.assertEqual(
            delegated_task.metadata["cost_policy"]["provider_people_search_min_expected_results"],
            25,
        )
        self.assertEqual(delegated_task.metadata["search_channel_order"], ["harvest_profile_search"])

    def test_acquire_former_search_seed_keeps_primary_provider_search_when_high_cost_not_allowed(self) -> None:
        task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="",
            source_hint="",
            status="ready",
            blocking=True,
            metadata={
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
                "filter_hints": {},
                "search_seed_queries": ["infra"],
                "search_channel_order": ["web_search"],
            },
        )
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
        )
        state = {
            "company_identity": identity,
            "snapshot_dir": self.settings.company_assets_dir / "reflectionai" / "snap1-low-cost",
        }
        state["snapshot_dir"].mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Reflection AI",
                "execution_preferences": {},
            }
        )
        sentinel = AcquisitionExecution(task_id=task.task_id, status="completed", detail="ok", payload={})
        with unittest.mock.patch.object(
            self.acquisition_engine, "_acquire_search_seed_pool", return_value=sentinel
        ) as mocked:
            result = self.acquisition_engine._acquire_former_search_seed(task, state, request)
        self.assertIs(result, sentinel)
        delegated_task = mocked.call_args.args[0]
        self.assertEqual(delegated_task.metadata["cost_policy"]["provider_people_search_mode"], "primary_only")
        self.assertEqual(
            delegated_task.metadata["cost_policy"]["provider_people_search_min_expected_results"],
            50,
        )
        self.assertEqual(delegated_task.metadata["search_channel_order"], ["harvest_profile_search"])
        self.assertEqual(delegated_task.metadata["search_query_bundles"], [])

    def test_acquire_former_search_seed_keeps_primary_provider_search_for_scoped_search(self) -> None:
        task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="",
            source_hint="",
            status="ready",
            blocking=True,
            metadata={
                "cost_policy": {"provider_people_search_mode": "primary_only"},
                "filter_hints": {},
                "search_seed_queries": ["Pre-train"],
                "search_channel_order": ["harvest_profile_search"],
            },
        )
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
        )
        state = {
            "company_identity": identity,
            "snapshot_dir": self.settings.company_assets_dir / "openai" / "snap1-scoped",
        }
        state["snapshot_dir"].mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "OpenAI",
                "execution_preferences": {},
            }
        )
        sentinel = AcquisitionExecution(task_id=task.task_id, status="completed", detail="ok", payload={})
        with unittest.mock.patch.object(
            self.acquisition_engine, "_acquire_search_seed_pool", return_value=sentinel
        ) as mocked:
            result = self.acquisition_engine._acquire_former_search_seed(task, state, request)
        self.assertIs(result, sentinel)
        delegated_task = mocked.call_args.args[0]
        self.assertEqual(delegated_task.metadata["cost_policy"]["provider_people_search_mode"], "primary_only")
        self.assertEqual(delegated_task.metadata["search_channel_order"], ["harvest_profile_search"])

    def test_acquire_former_search_seed_treats_zero_result_as_completed(self) -> None:
        task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="",
            source_hint="",
            status="ready",
            blocking=True,
            metadata={
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
                "filter_hints": {},
                "search_seed_queries": ["infra"],
            },
        )
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snap-former-zero"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload({"target_company": "Reflection AI"})
        zero_result_snapshot = SearchSeedSnapshot(
            snapshot_id="snap-former-zero",
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[{"query": "infra", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            summary_path=snapshot_dir / "summary.json",
        )
        blocked = AcquisitionExecution(
            task_id="delegated-former",
            status="blocked",
            detail="Search-seed acquisition finished but did not recover any candidate leads.",
            payload=zero_result_snapshot.to_record(),
            state_updates={"search_seed_snapshot": zero_result_snapshot},
        )
        with unittest.mock.patch.object(self.acquisition_engine, "_acquire_search_seed_pool", return_value=blocked):
            result = self.acquisition_engine._acquire_former_search_seed(
                task,
                {"company_identity": identity, "snapshot_dir": snapshot_dir},
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertTrue(result.payload["former_search_zero_result"])
        self.assertIn("did not add any new candidates", result.detail)

    def test_acquire_former_search_seed_merges_existing_current_snapshot(self) -> None:
        task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="",
            source_hint="",
            status="ready",
            blocking=True,
            metadata={
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
                "filter_hints": {"current_companies": ["https://www.linkedin.com/company/openai/"]},
                "search_seed_queries": ["Reasoning"],
            },
        )
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snap-merge-current-former"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "current_1",
                    "full_name": "Current Example",
                    "source_type": "harvest_profile_search",
                    "employment_status": "current",
                    "profile_url": "https://www.linkedin.com/in/current-example/",
                }
            ],
            query_summaries=[
                {"query": "Reasoning", "status": "completed", "mode": "harvest_profile_search", "lane": "current"}
            ],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_primary",
            summary_path=discovery_dir / "summary.json",
            entries_path=discovery_dir / "entries.json",
        )
        former_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "former_1",
                    "full_name": "Former Example",
                    "source_type": "harvest_profile_search",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                }
            ],
            query_summaries=[
                {"query": "Reasoning", "status": "completed", "mode": "harvest_profile_search", "lane": "former"}
            ],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_primary",
            summary_path=discovery_dir / "summary.json",
            entries_path=discovery_dir / "entries.json",
        )
        request = JobRequest.from_payload(
            {
                "target_company": "OpenAI",
                "execution_preferences": {},
            }
        )
        delegated = AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail="ok",
            payload=former_snapshot.to_record(),
            state_updates={"search_seed_snapshot": former_snapshot},
        )
        with unittest.mock.patch.object(self.acquisition_engine, "_acquire_search_seed_pool", return_value=delegated):
            result = self.acquisition_engine._acquire_former_search_seed(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "search_seed_snapshot": current_snapshot,
                },
                request,
            )

        merged_snapshot = result.state_updates["search_seed_snapshot"]
        self.assertEqual(result.status, "completed")
        self.assertEqual(len(merged_snapshot.entries), 2)
        self.assertEqual(result.payload["lane_entry_count"], 1)
        self.assertEqual(result.payload["merged_search_seed_entry_count"], 2)
        persisted_entries = json.loads((discovery_dir / "entries.json").read_text(encoding="utf-8"))
        persisted_summary = json.loads((discovery_dir / "summary.json").read_text(encoding="utf-8"))
        self.assertEqual(len(persisted_entries), 2)
        self.assertEqual(len(persisted_summary["query_summaries"]), 2)

    def test_search_seed_discovery_primary_only_skips_web_queries(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snap2"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        provider_entries = [
            {
                "seed_key": "entry_1",
                "full_name": "Former Member",
                "headline": "Research Engineer",
                "profile_url": "https://www.linkedin.com/in/former-member/",
            }
        ]
        provider_summaries = [{"query": "__past_company_only__", "status": "completed"}]
        with (
            unittest.mock.patch.object(
                self.acquisition_engine.search_seed_acquirer,
                "_execute_query_spec",
                side_effect=AssertionError("web query path should be skipped"),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.search_seed_acquirer,
                "_provider_people_search_fallback",
                return_value=(provider_entries, provider_summaries, [], ["acct_1"]),
            ),
        ):
            snapshot = self.acquisition_engine.search_seed_acquirer.discover(
                identity,
                snapshot_dir,
                search_seed_queries=["Reflection AI infra"],
                filter_hints={"past_companies": ["https://www.linkedin.com/company/reflectionai/"]},
                cost_policy={
                    "provider_people_search_mode": "primary_only",
                    "provider_people_search_min_expected_results": 10,
                },
                employment_status="former",
            )
        self.assertEqual(snapshot.stop_reason, "provider_people_search_primary")
        self.assertEqual(len(snapshot.entries), 1)
        self.assertEqual(snapshot.query_summaries[-1]["status"], "completed")

    def test_acquisition_progress_tracks_phase_buckets(self) -> None:
        request = JobRequest.from_payload({"target_company": "Reflection AI"})
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "phase_progress_job"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={},
        )
        task = next(task for task in plan.acquisition_tasks if task.task_type == "enrich_linkedin_profiles")
        progress = self.orchestrator._record_acquisition_task_completion(  # noqa: SLF001
            job_id=job_id,
            request=request,
            plan=plan,
            task=task,
            execution=AcquisitionExecution(task_id=task.task_id, status="completed", detail="ok", payload={}),
            acquisition_state={"snapshot_id": "snap1"},
        )
        self.assertEqual(progress["active_phase_id"], "linkedin_stage_1")
        self.assertIn("linkedin_stage_1", progress["phases"])
        self.assertEqual(progress["phases"]["linkedin_stage_1"]["completed_task_count"], 1)

    def test_spawn_workflow_takeover_runner_uses_supervisor_when_auto_daemon_enabled(self) -> None:
        with unittest.mock.patch(
            "sourcing_agent.orchestrator._spawn_detached_process", return_value={"status": "started", "pid": 123}
        ) as mocked:
            result = self.orchestrator._spawn_workflow_takeover_runner("job123", auto_job_daemon=True)  # noqa: SLF001
        self.assertEqual(result["status"], "started")
        self.assertIn("supervise-workflow", mocked.call_args.kwargs["command"])
        with unittest.mock.patch(
            "sourcing_agent.orchestrator._spawn_detached_process", return_value={"status": "started", "pid": 456}
        ) as mocked:
            self.orchestrator._spawn_workflow_takeover_runner("job456", auto_job_daemon=False)  # noqa: SLF001
        self.assertIn("execute-workflow", mocked.call_args.kwargs["command"])

    def test_queue_workflow_and_run_saved_job(self) -> None:
        gated = self.orchestrator.start_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        self.assertEqual(gated["status"], "needs_plan_review")
        review_id = gated["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job_id = queued["job_id"]

        before = self.orchestrator.get_job(job_id)
        self.assertIsNotNone(before)
        assert before is not None
        self.assertEqual(before["status"], "queued")

        run_result = self.orchestrator.run_queued_workflow(job_id)
        self.assertEqual(run_result["status"], "completed")

        after = self.orchestrator.get_job_results(job_id)
        self.assertIsNotNone(after)
        assert after is not None
        self.assertEqual(after["job"]["status"], "completed")
        self.assertGreaterEqual(len(after["results"]), 1)

    def test_two_stage_workflow_blocks_after_preview_and_continue_stage2_completes(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_id = "20260411T120000"
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
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
            notes="Builds infrastructure for model training.",
            linkedin_url="https://www.linkedin.com/in/taylor-infra/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        analysis_dir = snapshot_dir / "layered_segmentation" / "greater_china_outreach_20260411T120000Z"
        analysis_dir.mkdir(parents=True, exist_ok=True)
        analysis_path = analysis_dir / "layered_analysis.json"
        analysis_path.write_text(
            json.dumps(
                {
                    "status": "completed",
                    "layer_schema": {
                        "primary_layer_keys": [
                            "layer_0_roster",
                            "layer_1_name_signal",
                            "layer_2_greater_china_region_experience",
                            "layer_3_mainland_china_experience_or_chinese_language",
                        ]
                    },
                    "layers": {
                        "layer_0_roster": {"count": 1},
                        "layer_1_name_signal": {"count": 0},
                        "layer_2_greater_china_region_experience": {"count": 0},
                        "layer_3_mainland_china_experience_or_chinese_language": {"count": 0},
                    },
                    "cumulative_layer_counts": {
                        "layer_0_roster_plus": 1,
                        "layer_1_name_signal_plus": 0,
                        "layer_2_greater_china_region_experience_plus": 0,
                        "layer_3_mainland_china_experience_or_chinese_language_plus": 0,
                    },
                    "ai_verification": {"requested": 0, "completed": 0},
                    "candidates": [
                        {
                            "candidate_id": "acme_infra_1",
                            "final_layer": 0,
                            "final_layer_source": "rules",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        request_payload = {
            "raw_user_request": "给我 Acme 的 Infra 方向成员",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "semantic_rerank_limit": 0,
            "analysis_stage_mode": "two_stage",
            "execution_preferences": {
                "require_stage2_confirmation": True,
                "reuse_existing_roster": True,
                "reuse_snapshot_id": snapshot_id,
            },
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = self.orchestrator._create_workflow_job(request, plan)
        layering_summary = {
            "status": "completed",
            "target_company": "Acme",
            "snapshot_id": snapshot_id,
            "analysis_stage": "stage_1_preview",
            "analysis_paths": {"full": str(analysis_path)},
            "layer_schema": {
                "primary_layer_keys": [
                    "layer_0_roster",
                    "layer_1_name_signal",
                    "layer_2_greater_china_region_experience",
                    "layer_3_mainland_china_experience_or_chinese_language",
                ]
            },
            "layer_counts": {
                "layer_0_roster": 1,
                "layer_1_name_signal": 0,
                "layer_2_greater_china_region_experience": 0,
                "layer_3_mainland_china_experience_or_chinese_language": 0,
            },
            "cumulative_layer_counts": {
                "layer_0_roster_plus": 1,
                "layer_1_name_signal_plus": 0,
                "layer_2_greater_china_region_experience_plus": 0,
                "layer_3_mainland_china_experience_or_chinese_language_plus": 0,
            },
            "ai_verification": {"requested": 0, "completed": 0},
        }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_run_outreach_layering_after_acquisition",
            return_value={**layering_summary, "analysis_stage": "stage_2_final"},
        ):
            first_pass = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)
            self.assertEqual(first_pass["status"], "blocked")
            self.assertEqual(first_pass["reason"], "awaiting_stage2_confirmation")

            preview_snapshot = self.orchestrator.get_job_results(job_id)
            assert preview_snapshot is not None
            self.assertEqual(preview_snapshot["job"]["status"], "blocked")
            self.assertEqual(preview_snapshot["job"]["stage"], "retrieving")
            self.assertEqual(preview_snapshot["job"]["summary"]["awaiting_user_action"], "continue_stage2")
            self.assertEqual(preview_snapshot["job"]["summary"]["stage1_preview"]["returned_matches"], 1)
            self.assertTrue(preview_snapshot["asset_population"]["available"])
            self.assertEqual(preview_snapshot["asset_population"]["candidate_count"], 1)

            continue_result = self.orchestrator.continue_workflow_stage2({"job_id": job_id})
            self.assertEqual(continue_result["status"], "queued")
            for _ in range(40):
                final_snapshot = self.orchestrator.get_job_results(job_id)
                if final_snapshot and final_snapshot["job"]["status"] == "completed":
                    break
                time.sleep(0.05)
            else:
                final_snapshot = self.orchestrator.get_job_results(job_id)

        assert final_snapshot is not None
        self.assertEqual(final_snapshot["job"]["status"], "completed")
        self.assertEqual(final_snapshot["job"]["summary"]["analysis_stage"], "stage_2_final")
        self.assertTrue(final_snapshot["asset_population"]["available"])
        self.assertEqual(final_snapshot["asset_population"]["candidate_count"], 1)

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
            self.store.replace_manual_review_items(current_job_id, [])
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

    def test_single_stage_workflow_skips_public_web_stage_and_publishes_stage_progress_markers(self) -> None:
        snapshot_id = "20260411T131500"
        snapshot_dir = self.settings.company_assets_dir / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        company_identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
        )
        candidate = Candidate(
            candidate_id="acme_infra_2",
            name_en="Jordan Infra",
            display_name="Jordan Infra",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="infra platform systems",
            linkedin_url="https://www.linkedin.com/in/jordan-infra/",
        )
        request_payload = {
            "raw_user_request": "给我 Acme 的 Infra 方向成员",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "semantic_rerank_limit": 0,
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
            self.store.replace_manual_review_items(current_job_id, [])
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

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "execute_task",
                side_effect=fake_execute_task,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={"status": "completed", "analysis_stage": "stage_2_final"},
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

        snapshot = self.orchestrator.get_job_results(job_id)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertEqual(snapshot["job"]["summary"]["analysis_stage"], "stage_2_final")
        self.assertEqual(snapshot["job"]["summary"]["analysis_stage_mode"], "single_stage")
        self.assertEqual(snapshot["job"]["summary"]["linkedin_stage_1"]["status"], "completed")
        self.assertEqual(snapshot["job"]["summary"]["stage1_preview"]["status"], "ready")
        self.assertTrue(Path(snapshot["job"]["summary"]["linkedin_stage_1"]["summary_path"]).exists())
        self.assertTrue(Path(snapshot["job"]["summary"]["stage1_preview"]["summary_path"]).exists())
        self.assertNotIn("public_web_stage_2", snapshot["job"]["summary"])
        self.assertTrue(Path(snapshot["job"]["summary"]["stage_summary_path"]).exists())
        self.assertEqual(
            snapshot["workflow_stage_summaries"]["stage_order"],
            ["linkedin_stage_1", "stage_1_preview", "stage_2_final"],
        )
        self.assertEqual(
            snapshot["workflow_stage_summaries"]["summaries"]["stage_2_final"]["stage"],
            "stage_2_final",
        )
        self.assertTrue(
            Path(snapshot["workflow_stage_summaries"]["summaries"]["stage_2_final"]["summary_path"]).exists()
        )
        self.assertTrue(
            str(snapshot["workflow_stage_summaries"]["summaries"]["stage_1_preview"].get("completed_at") or "").strip()
        )
        self.assertTrue(
            str(snapshot["workflow_stage_summaries"]["summaries"]["stage_2_final"].get("completed_at") or "").strip()
        )
        progress = self.orchestrator.get_job_progress(job_id)
        assert progress is not None
        self.assertEqual(
            progress["workflow_stage_summaries"]["stage_order"],
            ["linkedin_stage_1", "stage_1_preview", "stage_2_final"],
        )

    def test_run_workflow_from_acquisition_skips_tasks_that_become_reusable_mid_run(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI post-train members",
            "target_company": "Reflection AI",
            "categories": ["employee", "former_employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Post-train"],
            "top_k": 5,
            "analysis_stage_mode": "two_stage",
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = self.orchestrator._create_workflow_job(request, plan)
        snapshot_id = "20260421T213349"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="reflection_post_train_1",
            name_en="Reflection Post Train",
            display_name="Reflection Post Train",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Research Engineer",
            focus_areas="post-train",
            linkedin_url="https://www.linkedin.com/in/reflection-post-train/",
        )
        candidate_payload = {
            "target_company": "Reflection AI",
            "snapshot_id": snapshot_id,
            "candidates": [candidate.to_record()],
            "evidence": [],
            "candidate_count": 1,
            "evidence_count": 0,
        }
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        linkedin_stage_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        public_web_stage_path = snapshot_dir / "candidate_documents.public_web_stage_2.json"
        for path in (candidate_doc_path, linkedin_stage_path, public_web_stage_path):
            path.write_text(json.dumps(candidate_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": {"canonical_name": "Reflection AI"}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
        retrieval_index_path.write_text(json.dumps({"status": "built"}, ensure_ascii=False, indent=2), encoding="utf-8")
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )

        executed_task_types: list[str] = []

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):  # noqa: ARG001
            executed_task_types.append(task.task_type)
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved identity.",
                    payload={"snapshot_dir": str(snapshot_dir)},
                    state_updates={
                        "company_identity": identity,
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                    },
                )
            if task.task_type in {"acquire_full_roster", "acquire_former_search_seed"}:
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail=f"{task.task_type} completed.",
                    payload={},
                    state_updates={},
                )
            if task.task_type == "enrich_linkedin_profiles":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Reused baseline stage-1 snapshot.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_candidate_doc_path": linkedin_stage_path,
                        "linkedin_stage_completed": True,
                        "public_web_stage_candidate_doc_path": public_web_stage_path,
                        "public_web_stage_completed": True,
                    },
                )
            if task.task_type == "enrich_public_web_signals":
                raise AssertionError("public-web stage should be skipped once the current state already satisfies it")
            if task.task_type == "normalize_asset_snapshot":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Normalized snapshot.",
                    payload={"manifest_path": str(manifest_path)},
                    state_updates={"manifest_path": manifest_path},
                )
            if task.task_type == "build_retrieval_index":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built retrieval index.",
                    payload={"retrieval_index_summary": str(retrieval_index_path)},
                    state_updates={},
                )
            raise AssertionError(f"unexpected task type: {task.task_type}")

        retrieval_calls: list[str] = []

        def fake_execute_retrieval(
            current_job_id,
            current_request,
            current_plan,
            *,
            job_type="workflow",
            runtime_policy=None,
            candidate_source_override=None,
            persist_job_state=True,
            artifact_name_suffix="",
            artifact_status="completed",
        ):
            analysis_stage = str(dict(runtime_policy or {}).get("analysis_stage") or "stage_2_final")
            retrieval_calls.append(analysis_stage)
            artifact_path = self.settings.jobs_dir / (
                f"{current_job_id}.json"
                if not artifact_name_suffix
                else f"{current_job_id}.{artifact_name_suffix}.json"
            )
            summary = {
                "text": f"{analysis_stage} summary",
                "total_matches": 1,
                "returned_matches": 1,
                "analysis_stage": analysis_stage,
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "candidate_count": 1,
                },
                "outreach_layering": {"status": "completed", "analysis_stage": analysis_stage},
            }
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
            self.store.replace_manual_review_items(current_job_id, [])
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

        with (
            unittest.mock.patch.object(self.acquisition_engine, "execute_task", side_effect=fake_execute_task),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={"status": "completed", "analysis_stage": "stage_2_final"},
            ),
            unittest.mock.patch.object(self.orchestrator, "_execute_retrieval", side_effect=fake_execute_retrieval),
            unittest.mock.patch.object(
                self.orchestrator,
                "_refresh_running_workflow_before_retrieval",
                return_value={"status": "skipped", "reason": "test"},
            ),
        ):
            run_result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(run_result["status"], "completed")
        self.assertIn("resolve_company_identity", executed_task_types)
        self.assertIn("enrich_linkedin_profiles", executed_task_types)
        self.assertNotIn("enrich_public_web_signals", executed_task_types)
        self.assertEqual(retrieval_calls, ["stage_1_preview", "stage_2_final"])
        snapshot = self.orchestrator.get_job_results(job_id)
        assert snapshot is not None
        skipped_details = [str(item.get("detail") or "") for item in snapshot["events"] if item.get("status") == "skipped"]
        self.assertTrue(
            any(
                "Enrich public-web signals: satisfied by current acquisition state." in detail
                for detail in skipped_details
            )
        )

    def test_background_harvest_prefetch_reconcile_preserves_stage_progress_markers(self) -> None:
        snapshot_id = "20260411T132500"
        snapshot_dir = self.settings.company_assets_dir / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request_payload = {
            "raw_user_request": "给我 Acme 的 Infra 方向成员",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "semantic_rerank_limit": 0,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = self.orchestrator._create_workflow_job(request, plan)
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        initial_summary = {
            "text": "initial final summary",
            "analysis_stage": "stage_2_final",
            "analysis_stage_mode": "single_stage",
            "candidate_source": {
                "source_kind": "company_snapshot",
                "snapshot_id": snapshot_id,
                "candidate_count": 1,
            },
            "linkedin_stage_1": {
                "status": "completed",
                "snapshot_id": snapshot_id,
                "candidate_doc_path": str(snapshot_dir / "candidate_documents.json"),
            },
            "stage1_preview": {
                "status": "ready",
                "analysis_stage": "stage_1_preview",
                "artifact_path": str(self.settings.jobs_dir / f"{job_id}.preview.json"),
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "candidate_count": 1,
                },
            },
        }
        artifact_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "status": "completed",
                    "request": request.to_record(),
                    "plan": plan.to_record(),
                    "summary": initial_summary,
                    "matches": [],
                    "manual_review_items": [],
                    "artifact_path": str(artifact_path),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload=initial_summary,
            artifact_path=str(artifact_path),
        )

        def fake_sync_snapshot(*, request: JobRequest, snapshot_dir: Path, reason: str) -> dict[str, object]:
            return {
                "status": "completed",
                "snapshot_id": snapshot_id,
                "candidate_count": 1,
                "evidence_count": 1,
                "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                "artifact_paths": {},
                "state_updates": {},
            }

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
            summary = {
                "text": "reconciled final summary",
                "analysis_stage": "stage_2_final",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "candidate_count": 1,
                },
                "total_matches": 1,
                "returned_matches": 1,
            }
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
            artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2))
            return artifact

        pending_workers = [
            {
                "worker_id": 101,
                "status": "completed",
                "updated_at": "2026-04-11 06:40:00",
                "metadata": {
                    "snapshot_dir": str(snapshot_dir),
                },
            }
        ]
        job = self.store.get_job(job_id) or {}
        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=fake_sync_snapshot,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={"status": "completed", "analysis_stage": "stage_2_final"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                side_effect=fake_execute_retrieval,
            ),
        ):
            reconcile_result = self.orchestrator._reconcile_completed_workflow_after_harvest_prefetch(
                job=job,
                request=request,
                plan_payload=plan.to_record(),
                job_summary=initial_summary,
                pending_workers=pending_workers,
            )

        self.assertEqual(reconcile_result["status"], "reconciled_harvest_prefetch")
        latest_job = self.store.get_job(job_id) or {}
        latest_summary = dict(latest_job.get("summary") or {})
        self.assertEqual(latest_summary["analysis_stage"], "stage_2_final")
        self.assertEqual(latest_summary["analysis_stage_mode"], "single_stage")
        self.assertEqual(latest_summary["linkedin_stage_1"]["status"], "completed")
        self.assertEqual(latest_summary["stage1_preview"]["status"], "ready")
        self.assertNotIn("public_web_stage_2", latest_summary)
        self.assertTrue(Path(latest_summary["stage_summary_path"]).exists())
        snapshot = self.orchestrator.get_job_results(job_id)
        assert snapshot is not None
        self.assertEqual(
            snapshot["workflow_stage_summaries"]["stage_order"],
            ["linkedin_stage_1", "stage_1_preview", "stage_2_final"],
        )
        self.assertEqual(
            snapshot["workflow_stage_summaries"]["summaries"]["linkedin_stage_1"]["stage"],
            "linkedin_stage_1",
        )

    def test_queue_workflow_joins_inflight_exact_request(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "skip_plan_review": True,
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")
        second = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(second["status"], "joined_existing_job")
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(second["dispatch"]["strategy"], "join_inflight")
        dispatches = self.store.list_query_dispatches(target_company="Anthropic", limit=10)
        self.assertTrue(any(str(item.get("strategy") or "") == "join_inflight" for item in dispatches))

    def test_queue_workflow_joins_inflight_request_family_signature(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "profile_detail_limit": 200,
            "skip_plan_review": True,
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")

        second = self.orchestrator.queue_workflow(
            {
                **payload,
                "raw_user_request": "帮我找 Anthropic 做基础设施的技术成员，top10 就行。",
                "top_k": 10,
                "profile_detail_limit": 50,
            }
        )
        self.assertEqual(second["status"], "joined_existing_job")
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(second["dispatch"]["strategy"], "join_inflight")
        self.assertEqual(second["dispatch"]["request_family_signature"], first["dispatch"]["request_family_signature"])
        self.assertNotEqual(second["dispatch"]["request_signature"], first["dispatch"]["request_signature"])

    def test_storage_matching_signature_finds_equivalent_effective_request(self) -> None:
        raw_query_payload = {
            "raw_user_request": "我想找 Google Gemini 的产品经理",
            "target_company": "Google",
        }
        self.store.save_job(
            job_id="job-matching-signature",
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=raw_query_payload,
            plan_payload={},
            summary_payload={"message": "queued"},
        )

        structured_payload = {
            "target_company": "Google",
            "organization_keywords": ["Gemini", "Google DeepMind"],
            "keywords": ["Gemini"],
            "must_have_primary_role_buckets": ["product_management"],
        }
        matched = self.store.find_latest_job_by_request_signature(
            request_signature_value=matching_request_signature(structured_payload),
            target_company="Google",
            statuses=["queued"],
        )
        self.assertIsNotNone(matched)
        assert matched is not None
        self.assertEqual(matched["job_id"], "job-matching-signature")
        self.assertTrue(bool(matched.get("request_matching")))
        self.assertEqual(
            str(matched.get("matching_request_signature") or ""),
            matching_request_signature(raw_query_payload),
        )

    def test_queue_workflow_reuses_completed_job_with_tenant_scope(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "skip_plan_review": True,
            "tenant_id": "tenant-a",
            "requester_id": "user-1",
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")
        run_result = self.orchestrator.run_queued_workflow(str(first.get("job_id") or ""))
        self.assertEqual(run_result["status"], "completed")

        same_tenant_other_user = self.orchestrator.queue_workflow({**payload, "requester_id": "user-2"})
        self.assertEqual(same_tenant_other_user["status"], "reused_completed_job")
        self.assertEqual(same_tenant_other_user["job_id"], first["job_id"])
        self.assertEqual(same_tenant_other_user["dispatch"]["strategy"], "reuse_completed")

        different_tenant = self.orchestrator.queue_workflow(
            {**payload, "tenant_id": "tenant-b", "requester_id": "user-3"}
        )
        self.assertEqual(different_tenant["status"], "queued")
        self.assertNotEqual(different_tenant["job_id"], first["job_id"])

    def test_queue_workflow_reuses_completed_snapshot_for_related_request(self) -> None:
        company_dir = self.settings.company_assets_dir / "anthropic"
        snapshot_dir = company_dir / "20260408T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "company_identity": {
                        "requested_name": "Anthropic",
                        "canonical_name": "Anthropic",
                        "company_key": "anthropic",
                        "linkedin_slug": "anthropic",
                    },
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
                        "company_identity": {
                            "requested_name": "Anthropic",
                            "canonical_name": "Anthropic",
                            "company_key": "anthropic",
                            "linkedin_slug": "anthropic",
                        }
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="cand_snapshot_reuse",
                            name_en="Infra Researcher",
                            display_name="Infra Researcher",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Research Engineer",
                            focus_areas="infra gpu systems",
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
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "company_identity": {
                        "requested_name": "Anthropic",
                        "canonical_name": "Anthropic",
                        "company_key": "anthropic",
                        "linkedin_slug": "anthropic",
                    },
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
        baseline_payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU"],
            "top_k": 3,
        }
        baseline_request = JobRequest.from_payload(baseline_payload)
        baseline_plan = self.orchestrator.plan_workflow({**baseline_payload, "skip_plan_review": True})["plan"]
        self.store.save_job(
            job_id="baseline_snapshot_job",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=baseline_request.to_record(),
            plan_payload=baseline_plan,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {
                    "snapshot_id": snapshot_dir.name,
                    "source_kind": "company_snapshot",
                    "source_path": str(candidate_doc_path),
                },
            },
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前做网络平台方向的人。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "网络"],
                "top_k": 8,
                "skip_plan_review": True,
            }
        )
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "reuse_snapshot")
        self.assertEqual(queued["dispatch"]["matched_job_id"], "baseline_snapshot_job")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_dir.name)

        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertEqual(execution_preferences.get("reuse_snapshot_id"), snapshot_dir.name)
        self.assertEqual(execution_preferences.get("reuse_baseline_job_id"), "baseline_snapshot_job")

        original_execute_task = self.acquisition_engine.execute_task
        executed_task_types: list[str] = []

        def fail_if_called(task, job_request, target_company, state, bootstrap_summary=None):  # noqa: ARG001
            executed_task_types.append(task.task_type)
            raise AssertionError(f"unexpected acquisition task execution: {task.task_type}")

        self.acquisition_engine.execute_task = fail_if_called
        try:
            run_result = self.orchestrator.run_queued_workflow(str(queued.get("job_id") or ""))
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        self.assertEqual(run_result["status"], "completed")
        self.assertEqual(executed_task_types, [])
        snapshot = self.orchestrator.get_job_results(str(queued.get("job_id") or ""))
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertGreaterEqual(len(snapshot["results"]), 1)

    def test_queue_workflow_prefers_authoritative_registry_snapshot_over_completed_query_specific_snapshot(
        self,
    ) -> None:
        authoritative_snapshot_id = "20260413T010101"
        _, authoritative_candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=authoritative_snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_authoritative",
                    name_en="Authoritative Researcher",
                    display_name="Authoritative Researcher",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="pre-train systems",
                    linkedin_url="https://www.linkedin.com/in/authoritative-researcher/",
                ).to_record(),
            ],
        )
        registry_row = self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=authoritative_snapshot_id,
            candidate_count=1,
            source_path=str(authoritative_candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=0,
        )

        live_snapshot_id = "20260416T225318"
        _, live_candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=live_snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_live",
                    name_en="Live Snapshot Candidate",
                    display_name="Live Snapshot Candidate",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="pre-training",
                    linkedin_url="https://www.linkedin.com/in/live-snapshot-candidate/",
                ).to_record(),
            ],
        )

        payload = {
            "raw_user_request": "帮我找 Anthropic 做 Pre-train 的人。",
            "target_company": "Anthropic",
            "target_scope": "full_company_asset",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Pre-train"],
            "top_k": 8,
            "skip_plan_review": True,
        }
        completed_request = JobRequest.from_payload(payload)
        completed_plan = self.orchestrator.plan_workflow(payload)["plan"]
        self.store.save_job(
            job_id="completed_live_snapshot_job",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=completed_request.to_record(),
            plan_payload=completed_plan,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {
                    "snapshot_id": live_snapshot_id,
                    "source_kind": "company_snapshot",
                    "source_path": str(live_candidate_doc_path),
                },
            },
        )

        queued = self.orchestrator.queue_workflow(dict(payload))

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "reuse_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], authoritative_snapshot_id)
        self.assertEqual(int(queued["dispatch"]["matched_registry_id"] or 0), int(registry_row["registry_id"] or 0))
        self.assertEqual(queued["dispatch"]["reuse_basis"], "organization_asset_registry_lane_coverage")
        self.assertNotEqual(str(queued.get("job_id") or ""), "completed_live_snapshot_job")
        explanation = dict(queued["dispatch"].get("request_family_match_explanation") or {})
        self.assertTrue(bool(explanation.get("completed_match_reuse_suppressed")))
        self.assertEqual(
            explanation.get("completed_match_reuse_suppressed_reason"),
            "authoritative_registry_snapshot_preferred",
        )
        self.assertEqual(explanation.get("completed_match_snapshot_id"), live_snapshot_id)

    def test_queue_workflow_uses_idempotency_key_first(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "skip_plan_review": True,
            "tenant_id": "tenant-a",
            "requester_id": "user-1",
            "idempotency_key": "req-42",
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")

        inflight_repeat = self.orchestrator.queue_workflow(
            {
                **payload,
                "keywords": ["this payload is intentionally different"],
                "query": "same idempotency should still dedupe",
            }
        )
        self.assertEqual(inflight_repeat["status"], "joined_existing_job")
        self.assertEqual(inflight_repeat["job_id"], first["job_id"])
        self.assertEqual(inflight_repeat["dispatch"]["strategy"], "join_inflight")

        run_result = self.orchestrator.run_queued_workflow(str(first.get("job_id") or ""))
        self.assertEqual(run_result["status"], "completed")

        completed_repeat = self.orchestrator.queue_workflow(
            {
                **payload,
                "keywords": ["changed again after completion"],
                "query": "completed idempotent reuse",
            }
        )
        self.assertEqual(completed_repeat["status"], "reused_completed_job")
        self.assertEqual(completed_repeat["job_id"], first["job_id"])
        self.assertEqual(completed_repeat["dispatch"]["strategy"], "reuse_completed")

    def test_queue_workflow_reuses_authoritative_registry_snapshot_without_family_match(self) -> None:
        snapshot_id = "20260413T010101"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_registry_current",
                    name_en="Infra Generalist",
                    display_name="Infra Generalist",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="infrastructure systems",
                    linkedin_url="https://www.linkedin.com/in/infra-generalist/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_registry_former",
                    name_en="Alignment Former",
                    display_name="Alignment Former",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="former",
                    role="Researcher",
                    focus_areas="alignment safety",
                    linkedin_url="https://www.linkedin.com/in/alignment-former/",
                ).to_record(),
            ],
        )
        registry_row = self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=2,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=1,
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 做安全和对齐方向的人。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["安全", "对齐"],
                "top_k": 8,
                "skip_plan_review": True,
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "reuse_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_id)
        self.assertEqual(int(queued["dispatch"]["matched_registry_id"] or 0), int(registry_row["registry_id"] or 0))
        self.assertEqual(queued["dispatch"]["reuse_basis"], "organization_asset_registry_lane_coverage")
        self.assertEqual(queued["dispatch"]["matched_job_id"], "")
        self.assertTrue(queued["dispatch"]["force_reuse_snapshot_only"])
        self.assertEqual(
            queued["dispatch"]["request_family_match_explanation"]["selection_mode"],
            "organization_asset_registry_lane_coverage",
        )

        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertEqual(execution_preferences.get("reuse_snapshot_id"), snapshot_id)
        self.assertEqual(execution_preferences.get("reuse_existing_roster"), True)

        original_execute_task = self.acquisition_engine.execute_task
        executed_task_types: list[str] = []

        def fail_if_called(task, job_request, target_company, state, bootstrap_summary=None):  # noqa: ARG001
            executed_task_types.append(task.task_type)
            raise AssertionError(f"unexpected acquisition task execution: {task.task_type}")

        self.acquisition_engine.execute_task = fail_if_called
        try:
            run_result = self.orchestrator.run_queued_workflow(str(queued.get("job_id") or ""))
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        self.assertEqual(run_result["status"], "completed")
        self.assertEqual(executed_task_types, [])
        snapshot = self.orchestrator.get_job_results(str(queued.get("job_id") or ""))
        assert snapshot is not None
        candidate_source = dict(dict(snapshot["job"].get("summary") or {}).get("candidate_source") or {})
        self.assertEqual(
            dict(candidate_source.get("baseline_selection_explanation") or {}).get("reuse_basis"),
            "organization_asset_registry_lane_coverage",
        )
        self.assertEqual(
            dict(candidate_source.get("baseline_selection_explanation") or {}).get("matched_registry_snapshot_id"),
            snapshot_id,
        )

    def test_queue_workflow_registry_reuse_requires_delta_when_former_lane_missing(self) -> None:
        snapshot_id = "20260413T020202"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_registry_only_current",
                    name_en="Current Only",
                    display_name="Current Only",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="infrastructure systems",
                    linkedin_url="https://www.linkedin.com/in/current-only/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 的前员工。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["former"],
                "top_k": 6,
                "skip_plan_review": True,
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "new_job")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], "")
        self.assertEqual(int(queued["dispatch"]["matched_registry_id"] or 0), 0)

        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertEqual(execution_preferences.get("acquisition_strategy_override"), "former_employee_search")

    def test_queue_workflow_large_org_registry_baseline_still_dispatches_delta(self) -> None:
        snapshot_id = "20260413T030303"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_current",
                    name_en="Vision Current",
                    display_name="Vision Current",
                    category="employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="current",
                    role="Research Scientist",
                    focus_areas="multimodal veo nano banana",
                    linkedin_url="https://www.linkedin.com/in/google-vision-current/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_google_former",
                    name_en="Video Former",
                    display_name="Video Former",
                    category="employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Research Scientist",
                    focus_areas="video generation multimodal",
                    linkedin_url="https://www.linkedin.com/in/google-video-former/",
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
                    "candidate_count": 5035,
                    "evidence_count": 0,
                    "profile_detail_count": 5035,
                    "explicit_profile_capture_count": 5035,
                    "missing_linkedin_count": 0,
                    "manual_review_backlog_count": 0,
                    "profile_completion_backlog_count": 0,
                    "source_snapshot_count": 1,
                    "current_lane_coverage": {
                        "effective_candidate_count": 2535,
                        "effective_ready": True,
                    },
                    "former_lane_coverage": {
                        "effective_candidate_count": 2500,
                        "effective_ready": True,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=5035,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=2535,
            former_count=2500,
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找Google做多模态方向的人（包括Veo和Nano Banana组）",
                "target_company": "Google",
                "categories": ["employee"],
                "top_k": 10,
                "skip_plan_review": True,
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "delta_from_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_id)
        self.assertEqual(queued["dispatch"]["reuse_basis"], "organization_asset_registry_lane_coverage")
        self.assertEqual(queued["dispatch"]["asset_reuse_plan"]["planner_mode"], "delta_from_snapshot")
        self.assertTrue(queued["dispatch"]["asset_reuse_plan"]["requires_delta_acquisition"])

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

    def test_start_workflow_reuses_pending_plan_review_session(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
        }
        first = self.orchestrator.start_workflow(dict(payload))
        self.assertEqual(first["status"], "needs_plan_review")
        second = self.orchestrator.start_workflow(dict(payload))
        self.assertEqual(second["status"], "needs_plan_review")
        self.assertEqual(second["reason"], "existing_pending_plan_review")
        self.assertEqual(
            int(second["plan_review_session"]["review_id"] or 0),
            int(first["plan_review_session"]["review_id"] or 0),
        )

    def test_llm_request_normalization_feeds_structured_plan_by_default(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "target_company": "Google DeepMind",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "organization_keywords": ["Veo"],
                    "keywords": ["Post-train", "Math"],
                    "scope_disambiguation": {
                        "inferred_scope": "sub_org_only",
                        "sub_org_candidates": ["Google DeepMind", "Veo"],
                        "confidence": 0.86,
                        "rationale": "Detected product and sub-org terms.",
                    },
                    "retrieval_strategy": "hybrid",
                    "query": "Google DeepMind Veo post-train math members",
                    "execution_preferences": {
                        "precision_recall_bias": "precision_first",
                    },
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, RequestNormalizingModelClient()
            ),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要Veo团队的Post-train和Math方向成员",
            }
        )

        self.assertEqual(plan_result["request"]["target_company"], "Google")
        self.assertEqual(plan_result["request"]["categories"], ["employee"])
        self.assertEqual(plan_result["request"]["employment_statuses"], ["current"])
        self.assertEqual(plan_result["request"]["organization_keywords"], ["Google DeepMind", "Veo"])
        self.assertIn("Post-train", plan_result["request"]["keywords"])
        self.assertIn("Math", plan_result["request"]["keywords"])
        self.assertIn("Veo", plan_result["request"]["keywords"])
        self.assertEqual(plan_result["request"]["scope_disambiguation"]["inferred_scope"], "sub_org_only")
        self.assertEqual(
            plan_result["request"]["scope_disambiguation"]["sub_org_candidates"],
            ["Google DeepMind", "Veo"],
        )
        self.assertEqual(plan_result["request"]["retrieval_strategy"], "hybrid")
        self.assertEqual(
            plan_result["request"]["execution_preferences"],
            {
                "precision_recall_bias": "precision_first",
            },
        )
        self.assertEqual(plan_result["plan"]["acquisition_strategy"]["strategy_type"], "scoped_search_roster")
        self.assertIn("Google", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertIn("Google DeepMind", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertIn("Veo", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertNotIn("Post-train", plan_result["plan"]["acquisition_strategy"]["company_scope"])
        self.assertEqual(
            plan_result["plan"]["organization_execution_profile"]["default_acquisition_mode"],
            "scoped_search_roster",
        )
        filter_hint_keywords = list(plan_result["plan"]["acquisition_strategy"]["filter_hints"]["keywords"] or [])
        self.assertIn("Post-train", filter_hint_keywords)
        self.assertIn("Math", filter_hint_keywords)
        self.assertEqual(
            plan_result["plan"]["acquisition_strategy"]["cost_policy"]["precision_recall_bias"],
            "precision_first",
        )
        self.assertEqual(
            plan_result["plan"]["acquisition_strategy"]["cost_policy"]["provider_people_search_mode"],
            "primary_only",
        )
        self.assertGreaterEqual(
            int(
                plan_result["plan"]["acquisition_strategy"]["cost_policy"][
                    "provider_people_search_min_expected_results"
                ]
                or 0
            ),
            50,
        )
        self.assertEqual(plan_result["plan_review_gate"]["status"], "requires_review")
        self.assertTrue(plan_result["plan_review_gate"]["required_before_execution"])
        self.assertNotIn("google_scope_ambiguity_requires_confirmation", plan_result["plan_review_gate"]["reasons"])
        self.assertEqual(
            plan_result["plan"]["acquisition_strategy"]["strategy_decision_explanation"]["decision_source"],
            "organization_execution_profile",
        )
        self.assertIn(
            "targeted_people_search",
            [item["bundle_id"] for item in plan_result["plan"]["search_strategy"]["query_bundles"]],
        )
        self.assertTrue(
            any(
                "团队或子组织范围：" in item and "Veo" in item
                for item in plan_result["plan"]["intent_brief"]["identified_request"]
            )
        )

    def test_llm_request_normalization_preserves_natural_language_shorthand_rewrite(self) -> None:
        class RequestNormalizingModelClient(DeterministicModelClient):
            def normalize_request(self, payload: dict[str, object]) -> dict[str, object]:
                return {
                    "target_company": "Anthropic",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "retrieval_strategy": "hybrid",
                    "query": "Anthropic members",
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=RequestNormalizingModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, RequestNormalizingModelClient()
            ),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 的华人成员",
                "target_company": "Anthropic",
            }
        )

        self.assertEqual(plan_result["request"]["target_company"], "Anthropic")
        self.assertEqual(plan_result["request"]["categories"], ["employee"])
        self.assertEqual(plan_result["request"]["employment_statuses"], ["current"])
        self.assertEqual(
            plan_result["request"]["keywords"],
            ["Greater China experience", "Chinese bilingual outreach"],
        )
        self.assertTrue(plan_result["intent_rewrite"]["request"]["matched"])
        self.assertEqual(
            plan_result["intent_rewrite"]["request"]["rewrite"]["rewrite_id"],
            "greater_china_outreach",
        )
        self.assertTrue(
            any("自然语言简称改写" in item for item in plan_result["plan"]["intent_brief"]["identified_request"]),
        )
        self.assertTrue(
            any(
                item.get("rewrite_id") == "greater_china_outreach"
                for item in list(plan_result["intent_rewrite"].get("policy_catalog") or [])
                if isinstance(item, dict)
            )
        )
        self.assertIn(
            "这类简称默认按公开的地区 / 语言 / 学习工作经历口径理解，而不是身份标签判断。",
            plan_result["plan"]["intent_brief"]["target_output"],
        )

    def test_plan_stage_colloquial_full_company_request_can_skip_review(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要 Humans& 公司全量成员，重新跑。",
                "target_company": "Humans&",
            }
        )

        self.assertEqual(
            plan_result["request"]["execution_preferences"]["acquisition_strategy_override"], "full_company_roster"
        )
        self.assertTrue(plan_result["request"]["execution_preferences"]["force_fresh_run"])
        self.assertTrue(plan_result["request"]["execution_preferences"]["use_company_employees_lane"])
        self.assertEqual(plan_result["plan"]["acquisition_strategy"]["strategy_type"], "full_company_roster")
        self.assertEqual(plan_result["plan"]["open_questions"], [])
        self.assertEqual(plan_result["plan_review_gate"]["status"], "ready")
        self.assertFalse(plan_result["plan_review_gate"]["required_before_execution"])
        self.assertIn("review_id", plan_result["plan_review_session"])
        self.assertTrue(plan_result["plan"]["acquisition_strategy"]["cost_policy"]["allow_company_employee_api"])

    def test_plan_workflow_surfaces_incremental_execution_hints_for_large_org(self) -> None:
        roster_dir = self.settings.company_assets_dir / "anthropic" / "20260409T000000" / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)
        visible_entries = [
            {
                "name": "Ada Lovelace",
                "fullName": "Ada Lovelace",
                "linkedinUrl": "https://www.linkedin.com/in/ada",
            }
        ]
        (roster_dir / "harvest_company_employees_visible.json").write_text(
            json.dumps(visible_entries, ensure_ascii=False, indent=2)
        )
        (roster_dir / "harvest_company_employees_merged.json").write_text(
            json.dumps(visible_entries, ensure_ascii=False, indent=2)
        )
        (roster_dir / "harvest_company_employees_headless.json").write_text(
            json.dumps([], ensure_ascii=False, indent=2)
        )
        (roster_dir / "harvest_company_employees_summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260409T000000",
                    "target_company": "Anthropic",
                    "visible_entry_count": 1,
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出 Anthropic 的所有成员，先全量获取 roster。",
                "target_company": "Anthropic",
            }
        )

        hints = dict(plan_result["plan_review_gate"]["execution_mode_hints"])
        self.assertEqual(hints["segmented_company_employee_shard_strategy"], "adaptive_us_function_partition")
        self.assertTrue(hints["adaptive_probe_required_before_live_roster"])
        self.assertEqual(
            hints["adaptive_company_employee_shard_policy"]["root_filters"],
            {"locations": ["United States"]},
        )
        self.assertEqual(
            hints["adaptive_company_employee_shard_policy"]["partition_rules"][0]["include_patch"]["function_ids"],
            ["8"],
        )
        self.assertTrue(hints["incremental_rerun_recommended"])
        self.assertEqual(
            hints["recommended_decision_patch"],
            {
                "reuse_existing_roster": True,
                "run_former_search_seed": True,
            },
        )
        self.assertEqual(
            hints["local_reusable_roster_snapshot"]["snapshot_id"],
            "20260409T000000",
        )
        self.assertEqual(
            hints["local_reusable_roster_snapshot"]["source_kind"],
            "harvest_company_employees",
        )
        self.assertTrue(
            any("prefer incremental reruns" in item for item in plan_result["plan_review_gate"]["suggested_actions"])
        )

    def test_anthropic_force_fresh_run_skips_local_bootstrap_in_acquisition(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-fresh"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "execution_preferences": {"force_fresh_run": True},
            }
        )
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire full company roster",
            description="Acquire roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster"},
        )
        state = {
            "company_identity": CompanyIdentity(
                requested_name="Anthropic",
                canonical_name="Anthropic",
                company_key="anthropic",
                linkedin_slug="anthropicresearch",
                linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
            ),
            "snapshot_dir": snapshot_dir,
        }

        expected = AcquisitionExecution(
            task_id=task.task_id,
            status="completed",
            detail="live acquisition",
            payload={"source": "live"},
        )

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_anthropic_local_asset_task",
                side_effect=AssertionError("should not use local Anthropic bootstrap"),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_full_roster",
                return_value=expected,
            ) as acquire_mock,
        ):
            result = self.acquisition_engine.execute_task(
                task,
                request,
                request.target_company,
                state,
                bootstrap_summary={"candidate_count": 258},
            )

        acquire_mock.assert_called_once()
        self.assertEqual(result.detail, "live acquisition")
        self.assertEqual(result.payload["source"], "live")

    def test_anthropic_force_fresh_run_workflow_skips_local_bootstrap(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="anthropic_live_1",
                    name_en="Live Candidate",
                    display_name="Live Candidate",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Researcher",
                    focus_areas="Greater China experience",
                )
            ],
            [],
        )
        request_payload = {
            "raw_user_request": "帮我重新跑 Anthropic 全量成员，不沿用旧资产。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Greater China experience"],
            "execution_preferences": {"force_fresh_run": True},
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_anthropic_force_fresh"

        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Workflow queued"},
        )

        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-live"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
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
                payload={"bootstrap_summary_seen": bootstrap_summary or {}},
                state_updates={},
            )

        self.acquisition_engine.execute_task = fake_execute_task
        with unittest.mock.patch.object(
            self.orchestrator,
            "ensure_bootstrapped",
            side_effect=AssertionError("should not bootstrap local Anthropic assets"),
        ):
            try:
                result = self.orchestrator.run_queued_workflow(job_id)
            finally:
                self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(result["status"], "completed")
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")

    def test_anthropic_local_bootstrap_roster_materializes_snapshot_candidate_documents(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-bootstrap-materialized"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Anthropic",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
            }
        )
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        acquire_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="acquire-full-roster",
                task_type="acquire_full_roster",
                title="Acquire full company roster",
                description="Acquire roster",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "snapshot_id": snapshot_dir.name,
            },
            bootstrap_summary={"candidate_count": 3108},
        )

        self.assertEqual(acquire_result.status, "completed")
        self.assertEqual(
            acquire_result.state_updates["candidate_doc_path"],
            snapshot_dir / "candidate_documents.json",
        )
        self.assertGreater(len(acquire_result.state_updates["candidates"]), 0)
        candidate_doc_payload = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertEqual(candidate_doc_payload["enrichment_scope"], "bootstrap_local_assets")
        self.assertEqual(candidate_doc_payload["candidate_count"], len(acquire_result.state_updates["candidates"]))

        normalize_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="normalize-asset-snapshot",
                task_type="normalize_asset_snapshot",
                title="Normalize snapshot",
                description="Normalize snapshot",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "snapshot_id": snapshot_dir.name,
                **acquire_result.state_updates,
            },
            bootstrap_summary={"candidate_count": 3108},
        )
        self.assertEqual(normalize_result.status, "completed")
        self.assertTrue((snapshot_dir / "manifest.json").exists())

        build_index_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="build-retrieval-index",
                task_type="build_retrieval_index",
                title="Build retrieval index",
                description="Build retrieval index",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "snapshot_id": snapshot_dir.name,
                **acquire_result.state_updates,
                **normalize_result.state_updates,
            },
            bootstrap_summary={"candidate_count": 3108},
        )
        self.assertEqual(build_index_result.status, "completed")
        self.assertTrue((snapshot_dir / "retrieval_index_summary.json").exists())

    def test_anthropic_local_asset_enrichment_marks_stage_completion(self) -> None:
        baseline_snapshot_id = "baseline-stage-mark"
        baseline_snapshot_dir = self.settings.company_assets_dir / "anthropic" / baseline_snapshot_id
        baseline_snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-stage-mark"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Anthropic",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "execution_preferences": {"delta_baseline_snapshot_id": baseline_snapshot_id},
            }
        )
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (baseline_snapshot_dir / "identity.json").write_text(
            json.dumps(identity.to_record(), ensure_ascii=False, indent=2)
        )
        baseline_candidate = Candidate(
            candidate_id="anthropic-baseline-1",
            name_en="Baseline Researcher",
            display_name="Baseline Researcher",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Scientist",
            focus_areas="Pre-training",
            source_dataset="baseline",
            source_path=str(baseline_snapshot_dir / "candidate_documents.json"),
        )
        baseline_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                baseline_candidate.candidate_id,
                "baseline",
                baseline_candidate.role,
                "https://www.linkedin.com/in/baseline-researcher",
            ),
            candidate_id=baseline_candidate.candidate_id,
            source_type="linkedin_profile",
            title=baseline_candidate.role,
            url="https://www.linkedin.com/in/baseline-researcher",
            summary="Baseline candidate from reusable snapshot.",
            source_dataset="baseline",
            source_path=str(baseline_snapshot_dir / "candidate_documents.json"),
        )
        baseline_payload = {
            "candidates": [baseline_candidate.to_record()],
            "evidence": [baseline_evidence.to_record()],
            "candidate_count": 1,
            "evidence_count": 1,
        }
        for path in (
            baseline_snapshot_dir / "candidate_documents.json",
            baseline_snapshot_dir / "candidate_documents.linkedin_stage_1.json",
            baseline_snapshot_dir / "candidate_documents.public_web_stage_2.json",
        ):
            path.write_text(json.dumps(baseline_payload, ensure_ascii=False, indent=2))

        search_seed_dir = snapshot_dir / "search_seed_discovery"
        search_seed_dir.mkdir(parents=True, exist_ok=True)
        search_seed_summary_path = search_seed_dir / "summary.json"
        search_seed_entries_path = search_seed_dir / "entries.json"
        search_seed_summary_path.write_text(json.dumps({"status": "ok"}, ensure_ascii=False, indent=2))
        search_seed_entries_payload = [
            {
                "full_name": "Former Seed Candidate",
                "headline": "Former Member",
                "profile_url": "https://www.linkedin.com/in/former-seed-candidate",
                "slug": "former-seed-candidate",
                "source_query": "__past_company_only__",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "metadata": {},
            }
        ]
        search_seed_entries_path.write_text(json.dumps(search_seed_entries_payload, ensure_ascii=False, indent=2))
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=search_seed_entries_payload,
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_primary",
            summary_path=search_seed_summary_path,
            entries_path=search_seed_entries_path,
        )
        base_state = {
            "company_identity": identity,
            "snapshot_dir": snapshot_dir,
            "snapshot_id": snapshot_dir.name,
            "search_seed_snapshot": search_seed_snapshot,
        }

        linkedin_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="enrich-linkedin-profiles",
                task_type="enrich_linkedin_profiles",
                title="Enrich LinkedIn profiles",
                description="LinkedIn stage 1",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            dict(base_state),
            bootstrap_summary={"candidate_count": 3108},
        )
        self.assertTrue(linkedin_result.state_updates["linkedin_stage_completed"])
        self.assertTrue(linkedin_result.payload["local_reuse_materialized"])
        self.assertEqual(
            linkedin_result.state_updates["candidate_doc_path"],
            snapshot_dir / "candidate_documents.json",
        )
        self.assertEqual(
            linkedin_result.state_updates["linkedin_stage_candidate_doc_path"],
            snapshot_dir / "candidate_documents.linkedin_stage_1.json",
        )
        linkedin_payload = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertEqual(linkedin_payload["candidate_count"], 2)
        self.assertEqual(
            sorted(item["name_en"] for item in linkedin_payload["candidates"]),
            ["Baseline Researcher", "Former Seed Candidate"],
        )

        state_after_linkedin = {**dict(base_state), **linkedin_result.state_updates}

        normalize_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="normalize-asset-snapshot",
                task_type="normalize_asset_snapshot",
                title="Normalize snapshot",
                description="Normalize snapshot",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            dict(state_after_linkedin),
            bootstrap_summary={"candidate_count": 3108},
        )
        self.assertEqual(normalize_result.status, "completed")
        self.assertTrue((snapshot_dir / "manifest.json").exists())
        self.assertEqual(self.store.candidate_count_for_company("Anthropic"), 2)

        build_index_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="build-retrieval-index",
                task_type="build_retrieval_index",
                title="Build retrieval index",
                description="Build retrieval index",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            dict({**state_after_linkedin, **normalize_result.state_updates}),
            bootstrap_summary={"candidate_count": 3108},
        )
        self.assertEqual(build_index_result.status, "completed")
        self.assertTrue((snapshot_dir / "retrieval_index_summary.json").exists())

        public_web_result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="enrich-public-web-signals",
                task_type="enrich_public_web_signals",
                title="Enrich public web",
                description="Public Web Stage 2",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            dict({**state_after_linkedin, **normalize_result.state_updates}),
            bootstrap_summary={"candidate_count": 3108},
        )
        self.assertTrue(public_web_result.state_updates["public_web_stage_completed"])
        self.assertTrue(public_web_result.payload["local_reuse_materialized"])
        self.assertEqual(
            public_web_result.state_updates["candidate_doc_path"],
            snapshot_dir / "candidate_documents.json",
        )
        self.assertEqual(
            public_web_result.state_updates["public_web_stage_candidate_doc_path"],
            snapshot_dir / "candidate_documents.public_web_stage_2.json",
        )

    def test_delta_baseline_linkedin_reuse_marks_public_web_complete_when_baseline_is_final(self) -> None:
        baseline_snapshot_id = "baseline-reflection-final"
        baseline_snapshot_dir, _ = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id=baseline_snapshot_id,
            candidates=[],
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-reflection-final"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        request = JobRequest.from_payload(
            {
                "target_company": "Reflection AI",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "execution_preferences": {"delta_baseline_snapshot_id": baseline_snapshot_id},
            }
        )
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        stage1_candidate = Candidate(
            candidate_id="reflection-current-1",
            name_en="Current Reflection Researcher",
            display_name="Current Reflection Researcher",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Research Engineer",
            focus_areas="Post-train",
            linkedin_url="https://www.linkedin.com/in/reflection-current-1/",
        )
        final_candidate = Candidate(
            candidate_id="reflection-former-1",
            name_en="Former Reflection Researcher",
            display_name="Former Reflection Researcher",
            category="former_employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="former",
            role="Research Engineer",
            focus_areas="Post-train",
            linkedin_url="https://www.linkedin.com/in/reflection-former-1/",
        )
        stage1_payload = {
            "target_company": "Reflection AI",
            "snapshot_id": baseline_snapshot_id,
            "candidates": [stage1_candidate.to_record()],
            "evidence": [],
            "candidate_count": 1,
            "evidence_count": 0,
        }
        final_payload = {
            "target_company": "Reflection AI",
            "snapshot_id": baseline_snapshot_id,
            "candidates": [stage1_candidate.to_record(), final_candidate.to_record()],
            "evidence": [],
            "candidate_count": 2,
            "evidence_count": 0,
        }
        (baseline_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(final_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (baseline_snapshot_dir / "candidate_documents.linkedin_stage_1.json").write_text(
            json.dumps(stage1_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (baseline_snapshot_dir / "candidate_documents.public_web_stage_2.json").write_text(
            json.dumps(final_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        result = self.acquisition_engine.execute_task(
            AcquisitionTask(
                task_id="enrich-linkedin-profiles",
                task_type="enrich_linkedin_profiles",
                title="Enrich LinkedIn profiles",
                description="LinkedIn stage 1",
                status="ready",
                blocking=True,
                metadata={"strategy_type": "full_company_roster"},
            ),
            request,
            request.target_company,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "snapshot_id": snapshot_dir.name,
            },
            bootstrap_summary=None,
        )

        self.assertEqual(result.status, "completed")
        self.assertTrue(result.state_updates["linkedin_stage_completed"])
        self.assertTrue(result.state_updates["public_web_stage_completed"])
        self.assertEqual(result.state_updates["candidate_doc_path"], snapshot_dir / "candidate_documents.json")
        self.assertEqual(
            result.state_updates["linkedin_stage_candidate_doc_path"],
            snapshot_dir / "candidate_documents.linkedin_stage_1.json",
        )
        self.assertEqual(
            result.state_updates["public_web_stage_candidate_doc_path"],
            snapshot_dir / "candidate_documents.json",
        )
        current_payload = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        stage1_materialized_payload = json.loads(
            (snapshot_dir / "candidate_documents.linkedin_stage_1.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current_payload["candidate_count"], 2)
        self.assertEqual(stage1_materialized_payload["candidate_count"], 1)
        self.assertEqual(current_payload["enrichment_scope"], "public_web_stage_2")
        self.assertEqual(stage1_materialized_payload["enrichment_scope"], "linkedin_stage_1")
        hydrated_state = {
            "company_identity": identity,
            "snapshot_dir": snapshot_dir,
            "snapshot_id": snapshot_dir.name,
            **result.state_updates,
        }
        self.orchestrator._hydrate_acquisition_state_candidate_documents(hydrated_state)
        normalize_task = AcquisitionTask(
            task_id="normalize-reflection-final",
            task_type="normalize_asset_snapshot",
            title="Normalize snapshot",
            description="Normalize snapshot",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster"},
        )
        equivalent_baseline = self.acquisition_engine._equivalent_delta_baseline_snapshot(
            task=normalize_task,
            job_request=request,
            identity=identity,
            snapshot_dir=snapshot_dir,
            candidates=list(hydrated_state.get("candidates") or []),
            evidence=list(hydrated_state.get("evidence") or []),
        )
        self.assertEqual(str(equivalent_baseline.get("snapshot_id") or ""), baseline_snapshot_id)

    def test_run_queued_workflow_skips_terminal_job_without_restarting(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 当前成员",
                "target_company": "Anthropic",
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_already_completed"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "done"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_run_workflow",
            side_effect=AssertionError("completed workflow should not be re-executed"),
        ):
            result = self.orchestrator.run_queued_workflow(job_id)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["reason"], "already_terminal")

    def test_post_acquisition_refinement_reruns_retrieval_against_existing_snapshot(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_id = "20260408T150000"
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        multimodal = Candidate(
            candidate_id="acme_multi",
            name_en="Mina Vision",
            display_name="Mina Vision",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
            focus_areas="multimodal foundation models",
            source_dataset="acme_snapshot",
        )
        recruiting = Candidate(
            candidate_id="acme_recruit",
            name_en="Rita Recruit",
            display_name="Rita Recruit",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Recruiter",
            focus_areas="technical recruiting",
            source_dataset="acme_snapshot",
        )
        infra = Candidate(
            candidate_id="acme_infra",
            name_en="Ian Systems",
            display_name="Ian Systems",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="distributed systems platform",
            source_dataset="acme_snapshot",
        )
        snapshot_payload = {
            "candidates": [multimodal.to_record(), recruiting.to_record(), infra.to_record()],
            "evidence": [],
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(snapshot_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        for candidate in [multimodal, recruiting, infra]:
            self.store.upsert_candidate(candidate)

        baseline = self.orchestrator.run_job(
            {
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "top_k": 3,
            }
        )
        self.assertEqual(baseline["summary"]["returned_matches"], 3)

        compiled = self.orchestrator.compile_post_acquisition_refinement(
            {
                "job_id": baseline["job_id"],
                "instruction": "只看 multimodal researcher，top 1",
            }
        )
        self.assertEqual(compiled["status"], "compiled")
        self.assertEqual(compiled["request_patch"]["categories"], ["researcher"])
        self.assertEqual(compiled["request_patch"]["must_have_facets"], ["multimodal"])
        self.assertEqual(compiled["request_patch"]["top_k"], 1)
        self.assertIn("intent_rewrite", compiled)
        self.assertTrue(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(compiled["request_preview"]["must_have_facets"], ["multimodal"])
        self.assertEqual(compiled["request_preview"]["categories"], ["researcher"])

        refined = self.orchestrator.apply_post_acquisition_refinement(
            {
                "job_id": baseline["job_id"],
                "instruction": "只看 multimodal researcher，top 1",
            }
        )
        self.assertEqual(refined["status"], "completed")
        self.assertEqual(refined["request"]["categories"], ["researcher"])
        self.assertEqual(refined["request"]["must_have_facets"], ["multimodal"])
        self.assertEqual(refined["rerun_result"]["summary"]["candidate_source"]["snapshot_id"], snapshot_id)
        self.assertEqual(refined["rerun_result"]["summary"]["returned_matches"], 1)
        self.assertEqual(refined["rerun_result"]["matches"][0]["candidate_id"], "acme_multi")
        self.assertGreaterEqual(refined["diff"]["summary"]["removed_count"], 1)
        self.assertIn("intent_rewrite", refined)
        self.assertTrue(refined["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(refined["request_preview"]["must_have_facets"], ["multimodal"])
        self.assertEqual(refined["request_preview"]["categories"], ["researcher"])

    def test_refinement_compile_exposes_intent_rewrite_for_shorthand_instruction(self) -> None:
        baseline = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "top_k": 3,
            }
        )

        compiled = self.orchestrator.compile_post_acquisition_refinement(
            {
                "job_id": baseline["job_id"],
                "instruction": "只看更偏华人的成员",
            }
        )

        self.assertEqual(compiled["status"], "compiled")
        self.assertTrue(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(
            compiled["intent_rewrite"]["instruction"]["rewrite"]["rewrite_id"],
            "greater_china_outreach",
        )

    def test_stage1_preview_request_preview_uses_effective_request_shape(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Google 做多模态的人",
                "target_company": "Google",
                "categories": ["employee"],
                "keywords": ["multimodal"],
                "employment_statuses": ["current", "former"],
            }
        )

        summary = self.orchestrator._build_stage1_preview_summary(
            request=request,
            preview_artifact={
                "summary": {
                    "text": "preview",
                    "analysis_stage": "stage_1_preview",
                    "total_matches": 12,
                    "returned_matches": 5,
                },
                "effective_request": {
                    "raw_user_request": "给我 Google 做多模态的人",
                    "target_company": "Google",
                    "categories": ["researcher"],
                    "employment_statuses": ["current", "former"],
                    "organization_keywords": ["Google DeepMind"],
                    "keywords": ["multimodal", "Veo"],
                    "must_have_keywords": ["Veo"],
                    "must_have_facets": ["multimodal"],
                    "must_have_primary_role_buckets": ["research"],
                    "scope_disambiguation": {
                        "inferred_scope": "both",
                        "sub_org_candidates": ["Google DeepMind"],
                    },
                },
            },
            acquisition_progress={},
            pre_retrieval_refresh={},
        )

        self.assertEqual(summary["request_preview"]["request_view"], "effective_request")
        self.assertEqual(summary["request_preview"]["categories"], ["researcher", "engineer"])
        self.assertEqual(summary["request_preview"]["organization_keywords"], ["Google DeepMind"])
        self.assertEqual(summary["request_preview"]["must_have_keywords"], ["Veo"])
        self.assertEqual(summary["request_preview"]["must_have_facets"], ["multimodal"])
        self.assertEqual(
            summary["request_preview"]["intent_axes"]["scope_boundary"]["organization_keywords"],
            ["Google DeepMind"],
        )
        self.assertEqual(
            summary["request_preview"]["intent_axes"]["thematic_constraints"]["must_have_keywords"],
            ["Veo"],
        )
        self.assertEqual(
            summary["request_preview"]["intent_axes"]["thematic_constraints"]["must_have_facets"],
            ["multimodal"],
        )

    def test_manual_review_synthesis_is_cached_without_resolving_queue_item(self) -> None:
        class ManualReviewSynthesisModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def synthesize_manual_review(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "summary": "Public evidence supports the affiliation, but the LinkedIn profile still carries conflicting content.",
                    "confidence_takeaways": [
                        "The queue exists because structured membership signals and profile content are not fully aligned."
                    ],
                    "conflict_points": ["LinkedIn content contains suspicious mixed signals."],
                    "recommended_checks": ["Verify the experience timeline directly before resolving the item."],
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=ManualReviewSynthesisModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, ManualReviewSynthesisModelClient()
            ),
        )
        candidate = Candidate(
            candidate_id="cand_manual_synth",
            name_en="Lia Example",
            display_name="Lia Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url="https://www.linkedin.com/in/lia-example/",
            metadata={
                "membership_review_decision": "uncertain",
                "membership_review_rationale": "Headline and supporting evidence disagree.",
                "membership_review_trigger_keywords": ["openai", "spam"],
            },
        )
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    "cand_manual_synth",
                    "linkedin_profile_detail",
                    "LinkedIn",
                    "https://www.linkedin.com/in/lia-example/",
                ),
                candidate_id="cand_manual_synth",
                source_type="linkedin_profile_detail",
                title="LinkedIn profile detail",
                url="https://www.linkedin.com/in/lia-example/",
                summary="LinkedIn profile detail captured for Lia Example; target-company membership requires manual review.",
                source_dataset="linkedin_profile_detail",
                source_path="/tmp/lia-profile.json",
            ).to_record(),
            {
                "evidence_id": "ev_public",
                "candidate_id": "cand_manual_synth",
                "source_type": "public_web_search",
                "title": "Official page mention",
                "url": "https://example.com/lia",
                "summary": "Official page suggests Acme affiliation.",
                "source_dataset": "public_web_search",
                "source_path": "/tmp/lia-web.json",
            },
        ]
        stored_items = self.store.replace_manual_review_items(
            "job_manual_synth",
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "target_company": "Acme",
                    "review_type": "manual_identity_resolution",
                    "priority": "high",
                    "status": "open",
                    "summary": "Needs manual review.",
                    "candidate": candidate.to_record(),
                    "evidence": evidence,
                    "metadata": {
                        "reasons": ["suspicious_membership"],
                        "confidence_label": "medium",
                        "confidence_score": 0.62,
                        "explanation": "Structured signals exist, but the profile content is noisy.",
                    },
                }
            ],
        )
        review_item_id = int(stored_items[0]["review_item_id"])

        synthesized = orchestrator.synthesize_manual_review_item({"review_item_id": review_item_id})
        self.assertEqual(synthesized["status"], "compiled")
        self.assertIn("conflicting content", synthesized["synthesis"]["summary"])
        self.assertEqual(synthesized["manual_review_item"]["status"], "open")
        self.assertEqual(
            synthesized["manual_review_item"]["metadata"]["manual_review_synthesis"]["summary"],
            synthesized["synthesis"]["summary"],
        )

        cached = orchestrator.synthesize_manual_review_item({"review_item_id": review_item_id})
        self.assertEqual(cached["status"], "cached")
        self.assertEqual(cached["synthesis"]["summary"], synthesized["synthesis"]["summary"])

    def test_manual_review_queue_and_plan_review_flow(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="lead_1",
                    name_en="Open Lead",
                    display_name="Open Lead",
                    category="lead",
                    target_company="LeadCo",
                    organization="LeadCo",
                    employment_status="unknown",
                    role="Researcher",
                    focus_areas="reinforcement learning post-train",
                    notes="Mentioned on a company engineering blog but no confirmed LinkedIn profile.",
                )
            ],
            [],
        )
        result = self.orchestrator.run_job(
            {
                "target_company": "LeadCo",
                "keywords": ["reinforcement learning"],
                "top_k": 5,
            }
        )
        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(result["summary"]["manual_review_queue_count"], 1)
        queue = self.orchestrator.list_manual_review_items(job_id=result["job_id"])
        self.assertGreaterEqual(len(queue["manual_review_items"]), 1)
        self.assertEqual(queue["manual_review_items"][0]["review_type"], "manual_identity_resolution")
        reviewed = self.orchestrator.review_manual_review_item(
            {
                "review_item_id": queue["manual_review_items"][0]["review_item_id"],
                "action": "resolved",
                "reviewer": "tester",
                "notes": "Reviewed manually.",
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")
        self.assertEqual(reviewed["manual_review_item"]["status"], "resolved")

    def test_plan_review_can_force_full_company_roster_company_employees_and_fresh_run(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想了解 Humans& 的 Coding 方向的 Researcher。",
                "target_company": "Humans&",
                "categories": ["researcher"],
                "employment_statuses": ["current"],
                "keywords": ["coding", "code generation"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "acquisition_strategy_override": "full_company_roster",
                    "use_company_employees_lane": True,
                    "force_fresh_run": True,
                },
            }
        )
        review_plan = reviewed["review"]["plan"]
        acquisition = review_plan["acquisition_strategy"]
        self.assertEqual(acquisition["strategy_type"], "full_company_roster")
        self.assertEqual(acquisition["company_scope"], ["Humans&"])
        self.assertTrue(acquisition["cost_policy"]["allow_company_employee_api"])
        self.assertFalse(acquisition["cost_policy"]["allow_cached_roster_fallback"])
        self.assertFalse(acquisition["cost_policy"]["allow_historical_profile_inheritance"])
        self.assertFalse(acquisition["cost_policy"]["allow_shared_provider_cache"])
        acquire_task = next(
            task for task in review_plan["acquisition_tasks"] if task["task_type"] == "acquire_full_roster"
        )
        self.assertEqual(acquire_task["metadata"]["strategy_type"], "full_company_roster")
        self.assertTrue(acquire_task["metadata"]["cost_policy"]["allow_company_employee_api"])
        self.assertFalse(acquire_task["metadata"]["cost_policy"]["allow_cached_roster_fallback"])
        self.assertFalse(acquire_task["metadata"]["cost_policy"]["allow_historical_profile_inheritance"])
        self.assertFalse(acquire_task["metadata"]["cost_policy"]["allow_shared_provider_cache"])

    def test_plan_review_persists_execution_preferences_into_review_request_and_queued_job(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出 Anthropic 的全量成员，重新跑，不沿用旧资产。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "acquisition_strategy_override": "full_company_roster",
                    "use_company_employees_lane": True,
                    "force_fresh_run": True,
                },
            }
        )

        reviewed_request = reviewed["review"]["request"]
        for key, value in {
            "acquisition_strategy_override": "full_company_roster",
            "use_company_employees_lane": True,
            "force_fresh_run": True,
        }.items():
            self.assertEqual(reviewed_request["execution_preferences"][key], value)

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job = self.orchestrator.get_job(queued["job_id"])
        self.assertIsNotNone(job)
        assert job is not None
        for key, value in {
            "acquisition_strategy_override": "full_company_roster",
            "use_company_employees_lane": True,
            "force_fresh_run": True,
        }.items():
            self.assertEqual(job["request"]["execution_preferences"][key], value)

    def test_queue_workflow_uses_frozen_execution_bundle_from_approved_review(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 做基础设施方向的人",
                "target_company": "Anthropic",
                "keywords": ["infra", "infrastructure"],
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
        reviewed_session = dict(reviewed["review"] or {})
        reviewed_bundle = dict(reviewed_session.get("execution_bundle") or {})
        self.assertTrue(reviewed_bundle)
        self.assertTrue(dict(reviewed_bundle.get("request_preview") or {}))
        self.assertEqual(
            int(dict(reviewed_bundle.get("source") or {}).get("plan_review_id") or 0),
            review_id,
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_build_augmented_sourcing_plan",
            side_effect=AssertionError("approved review should use frozen execution bundle"),
        ):
            queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})

        self.assertEqual(queued["status"], "queued")
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        job_bundle = dict(queued_job.get("execution_bundle") or {})
        self.assertTrue(job_bundle)
        self.assertTrue(dict(job_bundle.get("request_preview") or {}))
        self.assertEqual(
            int(dict(job_bundle.get("source") or {}).get("plan_review_id") or 0),
            review_id,
        )

    def test_plan_review_execution_overrides_can_force_fresh_run_without_joining_inflight(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出 Google 负责多模态的人",
                "target_company": "Google",
                "keywords": ["multimodal", "Veo"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )

        first = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(first["status"], "queued")

        second = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(second["status"], "joined_existing_job")
        self.assertEqual(second["job_id"], first["job_id"])

        fresh = self.orchestrator.queue_workflow(
            {
                "plan_review_id": review_id,
                "execution_preferences": {"force_fresh_run": True},
                "asset_view": "strict_roster_only",
            }
        )
        self.assertEqual(fresh["status"], "queued")
        self.assertNotEqual(fresh["job_id"], first["job_id"])

        fresh_job = self.orchestrator.get_job(fresh["job_id"])
        self.assertIsNotNone(fresh_job)
        assert fresh_job is not None
        self.assertTrue(fresh_job["request"]["execution_preferences"]["force_fresh_run"])
        self.assertEqual(fresh_job["request"]["asset_view"], "strict_roster_only")

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

    def test_queue_workflow_keeps_explicit_force_fresh_when_requested_at_queue_time(self) -> None:
        snapshot_id = "20260413T020303"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_current_2",
                    name_en="Multimodal Lead",
                    display_name="Multimodal Lead",
                    category="employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="current",
                    role="Research Scientist",
                    focus_areas="multimodal Veo Nano Banana",
                    linkedin_url="https://www.linkedin.com/in/google-multimodal-lead/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要 Google 公司全量成员，重新跑，不要高成本。",
                "target_company": "Google",
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)

        queued = self.orchestrator.queue_workflow(
            {
                "plan_review_id": review_id,
                "runtime_execution_mode": "hosted",
                "execution_preferences": {"force_fresh_run": True},
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "new_job")
        self.assertNotIn("force_fresh_run_suppressed", queued["dispatch"])
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertTrue(execution_preferences.get("force_fresh_run"))

    def test_plan_workflow_former_only_query_does_not_report_missing_current_profile_queries(self) -> None:
        snapshot_id = "snapshot-former-only"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Anthropic",
                "company_key": "anthropic",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 100,
                "evidence_count": 100,
                "profile_detail_count": 50,
                "explicit_profile_capture_count": 50,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 80,
                "completeness_band": "high",
                "selected_snapshot_ids": [snapshot_id],
                "summary": {"candidate_count": 100, "evidence_count": 100},
                "source_path": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
            },
            authoritative=True,
        )

        plan_result = self.orchestrator.plan_workflow({"raw_user_request": "帮我找Anthropic的前员工"})

        self.assertEqual(plan_result["request"]["employment_statuses"], ["former"])
        self.assertEqual(plan_result["request_preview"]["categories"], ["former_employee"])
        self.assertEqual(
            plan_result["plan"]["acquisition_strategy"]["strategy_type"],
            "former_employee_search",
        )
        self.assertEqual(
            plan_result["plan"]["asset_reuse_plan"]["missing_current_profile_search_query_count"],
            0,
        )

    def test_organization_completeness_ledger_infers_former_coverage_from_candidate_documents(self) -> None:
        snapshot_id = "snapshot-former-ledger"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": "Anthropic",
                        "snapshot_id": snapshot_id,
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="anthropic-current-1",
                            name_en="Current One",
                            display_name="Current One",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/current-one/",
                            metadata={"headline": "Engineer at Anthropic"},
                        ).to_record(),
                        Candidate(
                            candidate_id="anthropic-former-1",
                            name_en="Former One",
                            display_name="Former One",
                            category="former_employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="former",
                            role="Researcher",
                            linkedin_url="https://www.linkedin.com/in/former-one/",
                            work_history="Anthropic Researcher",
                            metadata={"summary": "Worked on pretraining"},
                        ).to_record(),
                        Candidate(
                            candidate_id="anthropic-former-2",
                            name_en="Former Two",
                            display_name="Former Two",
                            category="former_employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="former",
                            role="Scientist",
                            linkedin_url="https://www.linkedin.com/in/former-two/",
                            education="MIT",
                        ).to_record(),
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Anthropic",
                "company_key": "anthropic",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 3,
                "evidence_count": 0,
                "profile_detail_count": 3,
                "explicit_profile_capture_count": 0,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 82,
                "completeness_band": "high",
                "selected_snapshot_ids": [snapshot_id],
                "summary": {"candidate_count": 3, "evidence_count": 0},
                "source_path": str(snapshot_dir / "candidate_documents.json"),
            },
            authoritative=True,
        )

        ledger_result = ensure_organization_completeness_ledger(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            selected_snapshot_ids=[snapshot_id],
        )

        former_lane = ledger_result["summary"]["lane_coverage"]["profile_search_former"]
        self.assertEqual(former_lane["inferred_candidate_count"], 2)
        self.assertEqual(former_lane["inferred_profile_detail_count"], 2)
        self.assertTrue(former_lane["effective_ready"])
        self.assertTrue(ledger_result["summary"]["delta_planning_ready"])
        organization_asset = dict(ledger_result["summary"].get("organization_asset") or {})
        self.assertEqual(
            int(dict(organization_asset.get("exact_membership_summary") or {}).get("member_count") or 0),
            3,
        )
        self.assertTrue(str(organization_asset.get("materialization_generation_key") or ""))
        persisted = self.store.get_authoritative_organization_asset_registry(
            target_company="Anthropic",
            asset_view="canonical_merged",
        )
        self.assertEqual(persisted["former_lane_effective_candidate_count"], 2)
        self.assertTrue(persisted["former_lane_effective_ready"])
        self.assertGreaterEqual(persisted["current_lane_effective_candidate_count"], 1)
        self.assertTrue(isinstance(persisted["former_lane_coverage"], dict))
        self.assertTrue(str(persisted.get("materialization_generation_key") or ""))
        execution_profile = self.store.get_organization_execution_profile(
            target_company="Anthropic",
            asset_view="canonical_merged",
        )
        self.assertEqual(execution_profile["target_company"], "Anthropic")
        self.assertEqual(execution_profile["source_snapshot_id"], snapshot_id)
        self.assertEqual(
            str(execution_profile.get("source_generation_key") or ""),
            str(persisted.get("materialization_generation_key") or ""),
        )
        self.assertIn(execution_profile["default_acquisition_mode"], {"hybrid", "full_company_roster"})
        self.assertGreaterEqual(execution_profile["former_lane_effective_candidate_count"], 2)

    def test_organization_completeness_ledger_reports_execution_profile_refresh_failure(self) -> None:
        snapshot_id = "snapshot-ledger-sync-failure"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": "Anthropic",
                        "snapshot_id": snapshot_id,
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="anthropic-sync-1",
                            name_en="Sync Failure",
                            display_name="Sync Failure",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/anthropic-sync-1/",
                        ).to_record(),
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Anthropic",
                "company_key": "anthropic",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 1,
                "evidence_count": 0,
                "profile_detail_count": 1,
                "explicit_profile_capture_count": 0,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 85,
                "completeness_band": "high",
                "selected_snapshot_ids": [snapshot_id],
                "summary": {"candidate_count": 1, "evidence_count": 0},
                "source_path": str(snapshot_dir / "candidate_documents.json"),
            },
            authoritative=True,
        )

        with unittest.mock.patch(
            "sourcing_agent.organization_execution_profile.ensure_organization_execution_profile",
            side_effect=RuntimeError("profile refresh boom"),
        ):
            ledger_result = ensure_organization_completeness_ledger(
                runtime_dir=self.settings.runtime_dir,
                store=self.store,
                target_company="Anthropic",
                snapshot_id=snapshot_id,
                asset_view="canonical_merged",
                selected_snapshot_ids=[snapshot_id],
            )

        self.assertEqual(ledger_result["status"], "built")
        sync_status = dict(ledger_result.get("sync_status") or {})
        self.assertEqual(sync_status.get("overall_status"), "partial_failure")
        self.assertEqual(
            dict(sync_status.get("organization_asset_registry_refresh") or {}).get("status"),
            "completed",
        )
        self.assertEqual(
            dict(sync_status.get("organization_execution_profile_refresh") or {}).get("status"),
            "failed",
        )
        self.assertIn(
            "profile refresh boom",
            str(dict(sync_status.get("organization_execution_profile_refresh") or {}).get("error") or ""),
        )

    def test_acquisition_shard_bundle_materializes_generation_and_membership_index(self) -> None:
        snapshot_id = "snapshot-shard-generation"
        company_dir = self.settings.company_assets_dir / "anthropic"
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity.to_record(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"target_company": "Anthropic", "snapshot_id": snapshot_id},
                    "candidates": [
                        Candidate(
                            candidate_id="anthropic-current-1",
                            name_en="Current One",
                            display_name="Current One",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/current-one/",
                        ).to_record()
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        raw_source_path = snapshot_dir / "search_seed_raw.json"
        raw_source_path.write_text(
            json.dumps(
                [
                    {
                        "fullName": "Current One",
                        "jobTitle": "Engineer",
                        "profileUrl": "https://www.linkedin.com/in/current-one/",
                        "companyName": "Anthropic",
                    },
                    {
                        "fullName": "Former Two",
                        "jobTitle": "Scientist",
                        "profileUrl": "https://www.linkedin.com/in/former-two/",
                        "companyName": "Anthropic",
                    },
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        shard_row = build_acquisition_shard_registry_record(
            target_company="Anthropic",
            company_key="anthropic",
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="reasoning",
            shard_title="Reasoning",
            search_query="Reasoning",
            company_filters={
                "companies": ["Anthropic"],
                "locations": ["United States"],
                "function_ids": ["8"],
                "search_query": "Reasoning",
            },
            result_count=2,
            estimated_total_count=2,
            status="completed",
            source_path=str(raw_source_path),
            metadata={"test_case": "generation_watermark"},
        )
        self.store.upsert_acquisition_shard_registry(shard_row)

        bundle_result = ensure_acquisition_shard_bundles_for_snapshot(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
        )

        self.assertEqual(bundle_result["status"], "completed")
        self.assertEqual(bundle_result["bundle_count"], 1)
        entry = bundle_result["entries"][0]
        self.assertTrue(str(entry.get("materialization_generation_key") or ""))
        persisted = self.store.list_acquisition_shard_registry(
            target_company="Anthropic",
            snapshot_ids=[snapshot_id],
            limit=10,
        )[0]
        self.assertEqual(
            str(persisted.get("materialization_generation_key") or ""),
            str(entry.get("materialization_generation_key") or ""),
        )
        membership_summary = self.store.summarize_asset_membership_index(
            generation_key=str(entry.get("materialization_generation_key") or "")
        )
        self.assertEqual(int(membership_summary.get("member_count") or 0), 2)

    def test_plan_workflow_mixed_query_reuses_embedded_former_baseline_without_former_delta(self) -> None:
        snapshot_id = "snapshot-mixed-former-embedded"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": "Anthropic",
                        "snapshot_id": snapshot_id,
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="anthropic-current-1",
                            name_en="Current One",
                            display_name="Current One",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Pre-training Engineer",
                            linkedin_url="https://www.linkedin.com/in/current-one/",
                            metadata={"headline": "Pre-training Engineer at Anthropic"},
                        ).to_record(),
                        Candidate(
                            candidate_id="anthropic-current-2",
                            name_en="Current Two",
                            display_name="Current Two",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Platform Engineer",
                            linkedin_url="https://www.linkedin.com/in/current-two/",
                            metadata={"headline": "Platform Engineer at Anthropic"},
                        ).to_record(),
                        Candidate(
                            candidate_id="anthropic-former-1",
                            name_en="Former One",
                            display_name="Former One",
                            category="former_employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="former",
                            role="Pre-training Researcher",
                            linkedin_url="https://www.linkedin.com/in/former-one/",
                            work_history="Anthropic pre-training researcher",
                            metadata={"summary": "Worked on pre-training and scaling laws"},
                        ).to_record(),
                        Candidate(
                            candidate_id="anthropic-former-2",
                            name_en="Former Two",
                            display_name="Former Two",
                            category="former_employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="former",
                            role="ML Scientist",
                            linkedin_url="https://www.linkedin.com/in/former-two/",
                            education="Stanford",
                            metadata={"headline": "Former Anthropic ML Scientist"},
                        ).to_record(),
                        Candidate(
                            candidate_id="anthropic-former-3",
                            name_en="Former Three",
                            display_name="Former Three",
                            category="former_employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="former",
                            role="Research Engineer",
                            linkedin_url="https://www.linkedin.com/in/former-three/",
                            work_history="Anthropic research engineer",
                            metadata={"summary": "Worked on alignment and pretraining"},
                        ).to_record(),
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Anthropic",
                "company_key": "anthropic",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 5,
                "evidence_count": 0,
                "profile_detail_count": 5,
                "explicit_profile_capture_count": 0,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 84,
                "completeness_band": "high",
                "selected_snapshot_ids": [snapshot_id],
                "summary": {"candidate_count": 5, "evidence_count": 0},
                "source_path": str(snapshot_dir / "candidate_documents.json"),
            },
            authoritative=True,
        )

        plan_result = self.orchestrator.plan_workflow({"raw_user_request": "帮我找Anthropic做Pre-training的人"})

        asset_reuse_plan = dict(plan_result["plan"]["asset_reuse_plan"] or {})
        self.assertTrue(asset_reuse_plan["baseline_former_embedded_sufficient"])
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_query_count"], 0)
        self.assertGreaterEqual(asset_reuse_plan["covered_former_profile_search_query_count"], 1)
        self.assertGreaterEqual(asset_reuse_plan["baseline_current_effective_candidate_count"], 2)
        self.assertGreaterEqual(asset_reuse_plan["baseline_former_effective_candidate_count"], 3)
        self.assertTrue(asset_reuse_plan["baseline_former_effective_ready"])
        self.assertEqual(
            dict(asset_reuse_plan.get("baseline_selection_explanation") or {}).get("reuse_basis"),
            "organization_asset_registry_lane_coverage",
        )
        self.assertTrue(
            dict(asset_reuse_plan.get("baseline_selection_explanation") or {}).get(
                "baseline_former_embedded_sufficient"
            )
        )
        former_task = next(
            task
            for task in list(plan_result["plan"]["acquisition_tasks"] or [])
            if task["task_type"] == "acquire_former_search_seed"
        )
        self.assertEqual(
            former_task["metadata"]["asset_reuse_plan"]["missing_former_profile_search_query_count"],
            0,
        )
        self.assertTrue(former_task["metadata"]["delta_execution_plan"]["delta_noop"])
        self.assertEqual(
            former_task["metadata"]["asset_reuse_plan"]["baseline_selection_explanation"]["reuse_basis"],
            "organization_asset_registry_lane_coverage",
        )
        self.assertEqual(
            former_task["metadata"]["delta_execution_plan"]["baseline_selection_explanation"]["selection_mode"],
            "organization_asset_registry_lane_coverage",
        )

    def test_ensure_organization_asset_registry_promotes_candidate_documents_fallback_snapshot(self) -> None:
        old_snapshot_id = "20260411T122319"
        old_snapshot_dir = self.settings.company_assets_dir / "anthropic" / old_snapshot_id / "normalized_artifacts"
        old_snapshot_dir.mkdir(parents=True, exist_ok=True)
        old_summary = {
            "target_company": "Anthropic",
            "company_key": "anthropic",
            "snapshot_id": old_snapshot_id,
            "asset_view": "canonical_merged",
            "candidate_count": 3108,
            "evidence_count": 3108,
            "profile_detail_count": 15,
            "explicit_profile_capture_count": 15,
            "missing_linkedin_count": 0,
            "manual_review_backlog_count": 0,
            "profile_completion_backlog_count": 3093,
            "source_snapshot_count": 1,
        }
        (old_snapshot_dir / "artifact_summary.json").write_text(json.dumps(old_summary, ensure_ascii=False, indent=2))
        old_identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (old_snapshot_dir.parent / "identity.json").write_text(
            json.dumps(old_identity.to_record(), ensure_ascii=False, indent=2)
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Anthropic",
                "company_key": "anthropic",
                "snapshot_id": old_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 3108,
                "evidence_count": 3108,
                "profile_detail_count": 15,
                "explicit_profile_capture_count": 15,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 3093,
                "source_snapshot_count": 1,
                "completeness_score": 29.77,
                "completeness_band": "low",
                "selected_snapshot_ids": [old_snapshot_id],
                "summary": dict(old_summary),
                "source_path": str(old_snapshot_dir / "artifact_summary.json"),
            },
            authoritative=True,
        )

        new_snapshot_id = "20260413T033754"
        new_snapshot_dir = self.settings.company_assets_dir / "anthropic" / new_snapshot_id
        new_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (new_snapshot_dir / "identity.json").write_text(
            json.dumps(old_identity.to_record(), ensure_ascii=False, indent=2)
        )
        (new_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": "Anthropic",
                        "snapshot_id": new_snapshot_id,
                        "source_snapshot_selection": {
                            "mode": "prefer_latest_complete",
                            "selected_snapshot_ids": [old_snapshot_id, new_snapshot_id],
                        },
                        "source_snapshots": [
                            {"snapshot_id": old_snapshot_id, "candidate_count": 3108, "evidence_count": 3108},
                            {"snapshot_id": new_snapshot_id, "candidate_count": 3455, "evidence_count": 0},
                        ],
                    },
                    "candidates": [
                        Candidate(
                            candidate_id=f"anthropic-current-{index}",
                            name_en=f"Current {index}",
                            display_name=f"Current {index}",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=f"https://www.linkedin.com/in/current-{index}/",
                            metadata={"headline": "Engineer at Anthropic"},
                        ).to_record()
                        for index in range(1, 7)
                    ]
                    + [
                        Candidate(
                            candidate_id=f"anthropic-former-{index}",
                            name_en=f"Former {index}",
                            display_name=f"Former {index}",
                            category="former_employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="former",
                            role="Researcher",
                            linkedin_url=f"https://www.linkedin.com/in/former-{index}/",
                            work_history="Anthropic researcher",
                            metadata={"summary": "Worked on pretraining"},
                        ).to_record()
                        for index in range(1, 4)
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        selected = ensure_organization_asset_registry(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Anthropic",
            asset_view="canonical_merged",
        )

        self.assertEqual(selected["snapshot_id"], new_snapshot_id)
        self.assertTrue(selected["authoritative"])
        self.assertEqual(selected["selected_snapshot_ids"], [old_snapshot_id, new_snapshot_id])
        self.assertIn("candidate_documents.json", selected["source_path"])
        self.assertGreater(float(selected["completeness_score"]), 29.77)
        self.assertGreaterEqual(selected["former_lane_effective_candidate_count"], 0)

    def test_backfill_organization_asset_registry_uses_existing_completeness_ledger_lane_coverage(self) -> None:
        snapshot_id = "20260416T012752"
        snapshot_dir = self.settings.company_assets_dir / "xai" / snapshot_id
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "company_key": "xai",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 2941,
                    "evidence_count": 5768,
                    "profile_detail_count": 2738,
                    "explicit_profile_capture_count": 2697,
                    "missing_linkedin_count": 0,
                    "manual_review_backlog_count": 0,
                    "profile_completion_backlog_count": 203,
                    "source_snapshot_count": 1,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (normalized_dir / "organization_completeness_ledger.json").write_text(
            json.dumps(
                {
                    "target_company": "xAI",
                    "company_key": "xai",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "selected_snapshot_ids": [snapshot_id],
                    "organization_asset": {
                        "candidate_count": 2941,
                        "evidence_count": 5768,
                        "profile_detail_count": 2738,
                        "missing_linkedin_count": 0,
                        "manual_review_backlog_count": 0,
                        "profile_completion_backlog_count": 203,
                        "materialization_generation_key": "09eb9527ee403ef7f5a8fc465b1c7d75",
                        "materialization_generation_sequence": 4,
                        "materialization_watermark": "4:09eb9527ee40",
                        "exact_membership_summary": {
                            "member_count": 2941,
                            "employment_scope_counts": {"current": 2286, "former": 655},
                        },
                    },
                    "lane_coverage": {
                        "current_effective": {
                            "effective_candidate_count": 2286,
                            "effective_ready": True,
                            "source_lane_keys": ["company_employees_current"],
                        },
                        "former_effective": {
                            "effective_candidate_count": 655,
                            "effective_ready": True,
                            "source_lane_keys": ["profile_search_former"],
                        },
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = backfill_organization_asset_registry_for_company(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="xAI",
            asset_view="canonical_merged",
        )

        self.assertEqual(result["selected_snapshot_id"], snapshot_id)
        persisted = self.store.get_authoritative_organization_asset_registry(
            target_company="xAI",
            asset_view="canonical_merged",
        )
        self.assertEqual(persisted["current_lane_effective_candidate_count"], 2286)
        self.assertEqual(persisted["former_lane_effective_candidate_count"], 655)
        self.assertTrue(persisted["current_lane_effective_ready"])
        self.assertTrue(persisted["former_lane_effective_ready"])
        self.assertEqual(
            dict(persisted.get("current_lane_coverage") or {}).get("source_lane_keys"),
            ["company_employees_current"],
        )
        self.assertEqual(
            dict(persisted.get("former_lane_coverage") or {}).get("source_lane_keys"),
            ["profile_search_former"],
        )

    def test_backfill_organization_asset_registry_falls_back_to_membership_summary_lane_coverage(self) -> None:
        snapshot_id = "20260421T222710"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / snapshot_id
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Reflection AI",
                    "company_key": "reflectionai",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 153,
                    "evidence_count": 197,
                    "profile_detail_count": 153,
                    "explicit_profile_capture_count": 153,
                    "missing_linkedin_count": 0,
                    "manual_review_backlog_count": 0,
                    "profile_completion_backlog_count": 0,
                    "membership_summary": {
                        "member_count": 153,
                        "employment_scope_counts": {"current": 147, "former": 6},
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = backfill_organization_asset_registry_for_company(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Reflection AI",
            asset_view="canonical_merged",
        )

        self.assertEqual(result["selected_snapshot_id"], snapshot_id)
        persisted = self.store.get_authoritative_organization_asset_registry(
            target_company="Reflection AI",
            asset_view="canonical_merged",
        )
        self.assertEqual(persisted["current_lane_effective_candidate_count"], 147)
        self.assertEqual(persisted["former_lane_effective_candidate_count"], 6)
        self.assertTrue(persisted["current_lane_effective_ready"])
        self.assertTrue(persisted["former_lane_effective_ready"])
        self.assertEqual(
            dict(persisted.get("current_lane_coverage") or {}).get("source_lane_keys"),
            ["baseline_membership_summary:current"],
        )
        self.assertEqual(
            dict(persisted.get("former_lane_coverage") or {}).get("source_lane_keys"),
            ["baseline_membership_summary:former"],
        )

    def test_evaluate_organization_asset_registry_promotion_allows_richer_snapshot_despite_source_snapshot_bias(
        self,
    ) -> None:
        existing = {
            "snapshot_id": "20260409T080131",
            "candidate_count": 3108,
            "evidence_count": 3297,
            "profile_detail_count": 3038,
            "missing_linkedin_count": 12,
            "profile_completion_backlog_count": 58,
            "completeness_score": 83.26,
            "current_lane_effective_candidate_count": 0,
            "former_lane_effective_candidate_count": 0,
            "source_snapshot_count": 3,
        }
        candidate = {
            "snapshot_id": "20260414T023651",
            "candidate_count": 3455,
            "evidence_count": 3767,
            "profile_detail_count": 3389,
            "missing_linkedin_count": 9,
            "profile_completion_backlog_count": 57,
            "completeness_score": 80.45,
            "current_lane_effective_candidate_count": 0,
            "former_lane_effective_candidate_count": 0,
            "source_snapshot_count": 1,
        }

        decision = evaluate_organization_asset_registry_promotion(
            existing_authoritative=existing,
            candidate_record=candidate,
        )

        self.assertTrue(decision["subsumption_higher"])
        self.assertFalse(decision["completeness_higher"])
        self.assertTrue(decision["coverage_materially_higher"])
        self.assertTrue(decision["source_snapshot_count_bias"])
        self.assertTrue(decision["promote"])
        self.assertEqual(
            decision["reason"],
            "materially_higher_coverage_despite_snapshot_count_bias",
        )

    def test_plan_workflow_reuses_former_lane_bundle_even_when_query_text_differs(self) -> None:
        snapshot_id = "snapshot-former-bundle-fallback"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"target_company": "Anthropic", "snapshot_id": snapshot_id},
                    "candidates": [
                        Candidate(
                            candidate_id=f"anthropic-current-{index}",
                            name_en=f"Current {index}",
                            display_name=f"Current {index}",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=f"https://www.linkedin.com/in/current-{index}/",
                            metadata={"headline": "Engineer at Anthropic"},
                        ).to_record()
                        for index in range(1, 7)
                    ],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Anthropic",
                "company_key": "anthropic",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 6,
                "evidence_count": 0,
                "profile_detail_count": 6,
                "explicit_profile_capture_count": 0,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 80,
                "completeness_band": "high",
                "selected_snapshot_ids": [snapshot_id],
                "summary": {"candidate_count": 6, "evidence_count": 0},
                "source_path": str(snapshot_dir / "candidate_documents.json"),
            },
            authoritative=True,
        )
        former_lane_row = build_acquisition_shard_registry_record(
            target_company="Anthropic",
            company_key="anthropic",
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope="former",
            strategy_type="former_employee_search",
            shard_id="Pre-train Training",
            shard_title="Pre-train Training",
            search_query="Pre-train Training",
            company_filters={
                "companies": ["https://www.linkedin.com/company/anthropicresearch/"],
                "search_query": "Pre-train Training",
            },
            result_count=42,
            status="completed",
            metadata={
                "standard_bundle": {
                    "bundle_candidate_count": 42,
                    "stub_candidate_count": 0,
                }
            },
        )
        self.store.upsert_acquisition_shard_registry(former_lane_row)

        plan_result = self.orchestrator.plan_workflow({"raw_user_request": "帮我找Anthropic做Pre-training的人"})

        asset_reuse_plan = dict(plan_result["plan"]["asset_reuse_plan"] or {})
        self.assertTrue(
            asset_reuse_plan["baseline_former_embedded_sufficient"]
            or asset_reuse_plan["baseline_full_company_lane_reuse_sufficient"]
        )
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_query_count"], 0)
        self.assertGreaterEqual(asset_reuse_plan["covered_former_profile_search_query_count"], 1)
        self.assertTrue(asset_reuse_plan["baseline_former_effective_ready"])
        self.assertGreaterEqual(asset_reuse_plan["baseline_former_effective_candidate_count"], 42)
        self.assertEqual(
            dict(asset_reuse_plan.get("baseline_selection_explanation") or {}).get(
                "covered_former_profile_search_query_count"
            ),
            int(asset_reuse_plan.get("covered_former_profile_search_query_count") or 0),
        )

    def test_large_org_authoritative_full_baseline_can_skip_keyword_delta_search(self) -> None:
        snapshot_id = "snapshot-anthropic-large-local-baseline"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="anthropic-current-1",
                    name_en="Current One",
                    display_name="Current One",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Member of Technical Staff",
                    linkedin_url="https://www.linkedin.com/in/anthropic-current-1/",
                    work_history="Anthropic coding systems",
                ).to_record(),
                Candidate(
                    candidate_id="anthropic-former-1",
                    name_en="Former One",
                    display_name="Former One",
                    category="former_employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="former",
                    role="Software Engineer",
                    linkedin_url="https://www.linkedin.com/in/anthropic-former-1/",
                    work_history="Anthropic coding infrastructure",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=3400,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=3000,
            former_count=400,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "target_company": "Anthropic",
                "keywords": ["Coding"],
                "employment_statuses": ["current", "former"],
                "categories": ["employee", "former_employee"],
            }
        )

        asset_reuse_plan = dict(plan_result["plan"]["asset_reuse_plan"] or {})
        self.assertTrue(asset_reuse_plan["baseline_reuse_available"])
        self.assertTrue(asset_reuse_plan["baseline_current_embedded_sufficient"])
        self.assertTrue(asset_reuse_plan["baseline_former_embedded_sufficient"])
        self.assertFalse(asset_reuse_plan["requires_delta_acquisition"])
        self.assertEqual(asset_reuse_plan["missing_current_profile_search_query_count"], 0)
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_query_count"], 0)
        self.assertEqual(asset_reuse_plan["planner_mode"], "reuse_snapshot_only")
        explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
        self.assertTrue(explanation.get("baseline_current_embedded_sufficient"))
        self.assertTrue(explanation.get("baseline_former_embedded_sufficient"))
        self.assertFalse(explanation.get("current_lane_delta_required"))
        self.assertFalse(explanation.get("former_lane_delta_required"))

    def test_large_org_full_company_query_reuses_complete_baseline_without_former_delta(self) -> None:
        snapshot_id = "snapshot-anthropic-full-company-baseline"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="anthropic-current-1",
                    name_en="Current One",
                    display_name="Current One",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Member of Technical Staff",
                    linkedin_url="https://www.linkedin.com/in/anthropic-current-1/",
                    work_history="Anthropic coding systems",
                ).to_record(),
                Candidate(
                    candidate_id="anthropic-former-1",
                    name_en="Former One",
                    display_name="Former One",
                    category="former_employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="former",
                    role="Software Engineer",
                    linkedin_url="https://www.linkedin.com/in/anthropic-former-1/",
                    work_history="Anthropic coding infrastructure",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=3455,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=3014,
            former_count=429,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "给我Anthropic的全部成员",
            }
        )

        asset_reuse_plan = dict(plan_result["plan"]["asset_reuse_plan"] or {})
        self.assertTrue(asset_reuse_plan["baseline_reuse_available"])
        self.assertTrue(asset_reuse_plan["baseline_full_company_lane_reuse_sufficient"])
        self.assertFalse(asset_reuse_plan["requires_delta_acquisition"])
        self.assertEqual(asset_reuse_plan["missing_current_company_employee_shard_count"], 0)
        self.assertEqual(asset_reuse_plan["missing_current_profile_search_query_count"], 0)
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_query_count"], 0)
        self.assertEqual(asset_reuse_plan["planner_mode"], "reuse_snapshot_only")
        explanation = dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
        self.assertTrue(explanation.get("baseline_full_company_lane_reuse_sufficient"))
        self.assertFalse(explanation.get("former_lane_delta_required"))

    def test_full_company_authoritative_baseline_covers_explicit_current_shards_without_delta(self) -> None:
        snapshot_id = "snapshot-anthropic-explicit-current-shards"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="anthropic-current-1",
                    name_en="Current One",
                    display_name="Current One",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Member of Technical Staff",
                    linkedin_url="https://www.linkedin.com/in/anthropic-current-1/",
                ).to_record(),
                Candidate(
                    candidate_id="anthropic-former-1",
                    name_en="Former One",
                    display_name="Former One",
                    category="former_employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="former",
                    role="Software Engineer",
                    linkedin_url="https://www.linkedin.com/in/anthropic-former-1/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=3455,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=3014,
            former_count=429,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "给我Anthropic的全部成员",
            }
        )
        request = JobRequest.from_payload(dict(plan_result["request"] or {}))
        plan = hydrate_sourcing_plan(dict(plan_result["plan"] or {}))
        current_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        current_metadata = dict(current_task.metadata or {})
        current_metadata["company_employee_shards"] = [
            {
                "shard_id": "engineering",
                "title": "Engineering",
                "strategy_id": "manual_explicit_shard_test",
                "company_filters": {
                    "locations": ["United States"],
                    "function_ids": ["8"],
                },
                "max_pages": 1,
                "page_limit": 25,
            }
        ]
        current_intent_view = dict(current_metadata.get("intent_view") or {})
        current_intent_view["company_employee_shards"] = list(current_metadata["company_employee_shards"])
        current_metadata["intent_view"] = current_intent_view
        current_task.metadata = current_metadata
        former_task = next(
            (task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed"), None
        )
        if former_task is not None:
            former_metadata = dict(former_task.metadata or {})
            former_metadata["search_seed_queries"] = []
            former_metadata["search_query_bundles"] = []
            former_intent_view = dict(former_metadata.get("intent_view") or {})
            former_intent_view["search_seed_queries"] = []
            former_intent_view["search_query_bundles"] = []
            former_metadata["intent_view"] = former_intent_view
            former_task.metadata = former_metadata

        asset_reuse_plan = compile_asset_reuse_plan(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            request=request,
            plan=plan,
        )

        self.assertTrue(asset_reuse_plan["baseline_full_company_lane_reuse_sufficient"])
        self.assertFalse(asset_reuse_plan["requires_delta_acquisition"])
        self.assertEqual(asset_reuse_plan["missing_current_company_employee_shard_count"], 0)
        self.assertEqual(asset_reuse_plan["covered_current_company_employee_shard_count"], 1)
        self.assertEqual(
            dict(
                list(asset_reuse_plan.get("covered_current_company_employee_shards") or [])[0].get("metadata") or {}
            ).get("coverage_reason"),
            "authoritative_full_company_lane_reuse",
        )

    def test_google_delta_reuse_only_requires_pretrain_when_multimodal_shards_are_already_covered(self) -> None:
        snapshot_id = "20260412T022230"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_mm_current",
                    name_en="MM Current",
                    display_name="MM Current",
                    category="employee",
                    target_company="Google",
                    organization="Google DeepMind",
                    employment_status="current",
                    role="Research Scientist",
                    focus_areas="multimodal vision-language video generation",
                    linkedin_url="https://www.linkedin.com/in/google-mm-current/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_google_mm_former",
                    name_en="MM Former",
                    display_name="MM Former",
                    category="former_employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Research Scientist",
                    focus_areas="multimodal video generation",
                    linkedin_url="https://www.linkedin.com/in/google-mm-former/",
                ).to_record(),
            ],
        )
        normalized_dir = snapshot_dir / "normalized_artifacts"
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
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=3724,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=3710,
            former_count=14,
        )

        for query in ["Multimodal Multimodality", "Vision-language", "Video generation"]:
            self.store.upsert_acquisition_shard_registry(
                build_acquisition_shard_registry_record(
                    target_company="Google",
                    company_key="google",
                    snapshot_id=snapshot_id,
                    lane="company_employees",
                    employment_scope="current",
                    strategy_type="adaptive_large_org_keyword_probe",
                    shard_id=query,
                    shard_title=query,
                    search_query=query,
                    company_filters={
                        "companies": [
                            "https://www.linkedin.com/company/google/",
                            "https://www.linkedin.com/company/deepmind/",
                        ],
                        "locations": ["United States"],
                        "function_ids": ["8", "9", "19", "24"],
                        "search_query": query,
                    },
                    result_count=2500,
                    estimated_total_count=3900,
                    status="completed_with_cap",
                )
            )

        for query in ["Multimodal", "Vision-language", "Video generation"]:
            self.store.upsert_acquisition_shard_registry(
                build_acquisition_shard_registry_record(
                    target_company="Google",
                    company_key="google",
                    snapshot_id=snapshot_id,
                    lane="profile_search",
                    employment_scope="former",
                    strategy_type="former_employee_search",
                    shard_id=query,
                    shard_title=query,
                    search_query=query,
                    company_filters={
                        "companies": [
                            "https://www.linkedin.com/company/google/",
                            "https://www.linkedin.com/company/deepmind/",
                        ],
                        "locations": ["United States"],
                        "function_ids": ["8", "9", "19", "24"],
                        "search_query": query,
                    },
                    result_count=180,
                    estimated_total_count=260,
                    status="completed",
                )
            )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找Google做多模态方向和Pre-train方向的人",
                "target_company": "Google",
            }
        )
        self.assertIn("Pre-train", plan_result["request"]["keywords"])
        self.assertIn("Pre-train", plan_result["request_preview"]["keywords"])
        asset_reuse_plan = dict(plan_result["plan"]["asset_reuse_plan"] or {})
        self.assertTrue(asset_reuse_plan["baseline_reuse_available"])
        self.assertTrue(asset_reuse_plan["requires_delta_acquisition"])
        self.assertEqual(asset_reuse_plan["baseline_snapshot_id"], snapshot_id)
        self.assertEqual(asset_reuse_plan["missing_current_profile_search_queries"], ["Pre-train"])
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_queries"], ["Pre-train"])
        self.assertGreaterEqual(asset_reuse_plan["covered_current_profile_search_query_count"], 1)
        self.assertGreaterEqual(asset_reuse_plan["covered_former_profile_search_query_count"], 1)

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找Google做多模态方向和Pre-train方向的人",
                "target_company": "Google",
                "categories": ["employee"],
                "top_k": 10,
                "skip_plan_review": True,
            }
        )
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "delta_from_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_id)
        self.assertEqual(
            queued["dispatch"]["request_family_match_explanation"]["dispatch_strategy"],
            "delta_from_snapshot",
        )
        queued_asset_reuse_plan = dict(queued["plan"]["asset_reuse_plan"] or {})
        self.assertEqual(queued_asset_reuse_plan["missing_current_profile_search_queries"], ["Pre-train"])
        self.assertEqual(queued_asset_reuse_plan["missing_former_profile_search_queries"], ["Pre-train"])
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        self.assertIn("Pre-train", list(dict(queued_job.get("request") or {}).get("keywords") or []))

    def test_asset_reuse_plan_uses_exact_membership_subsumption_for_current_profile_search(self) -> None:
        snapshot_id = "snapshot-exact-overlap"
        baseline_candidates = [
            Candidate(
                candidate_id="cand_google_mm_1",
                name_en="Multimodal One",
                display_name="Multimodal One",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Research Scientist",
                focus_areas="multimodal Veo",
                linkedin_url="https://www.linkedin.com/in/google-mm-1/",
            ).to_record(),
        ]
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=baseline_candidates,
        )
        baseline_generation = self._register_materialization_generation(
            target_company="Google",
            snapshot_id=snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=baseline_candidates,
        )
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
            materialization_generation_key=str(baseline_generation.get("generation_key") or ""),
            materialization_generation_sequence=int(baseline_generation.get("generation_sequence") or 0),
            materialization_watermark=str(baseline_generation.get("generation_watermark") or ""),
        )
        shard_candidates = [
            *baseline_candidates,
            Candidate(
                candidate_id="cand_google_mm_2",
                name_en="Multimodal Two",
                display_name="Multimodal Two",
                category="employee",
                target_company="Google",
                organization="Google",
                employment_status="current",
                role="Engineer",
                focus_areas="multimodal Nano Banana",
                linkedin_url="https://www.linkedin.com/in/google-mm-2/",
            ).to_record(),
        ]
        shard_row = build_acquisition_shard_registry_record(
            target_company="Google",
            company_key="google",
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="Multimodal",
            shard_title="Multimodal",
            search_query="Multimodal",
            company_filters={
                "companies": [
                    "https://www.linkedin.com/company/google/",
                    "https://www.linkedin.com/company/deepmind/",
                ],
                "locations": ["United States"],
                "function_ids": ["8", "9", "19", "24"],
                "search_query": "Multimodal",
            },
            result_count=2,
            estimated_total_count=2,
            status="completed",
        )
        shard_generation = self._register_materialization_generation(
            target_company="Google",
            snapshot_id=snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(shard_row.get("shard_key") or "multimodal"),
            candidates=shard_candidates,
            lane="profile_search",
            employment_scope="current",
        )
        shard_row["materialization_generation_key"] = str(shard_generation.get("generation_key") or "")
        shard_row["materialization_generation_sequence"] = int(shard_generation.get("generation_sequence") or 0)
        shard_row["materialization_watermark"] = str(shard_generation.get("generation_watermark") or "")
        self.store.upsert_acquisition_shard_registry(shard_row)

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找Google做多模态方向的当前成员",
                "target_company": "Google",
                "employment_statuses": ["current"],
                "keywords": ["Multimodal"],
                "categories": ["employee"],
            }
        )

        asset_reuse_plan = dict(plan_result["plan"].get("asset_reuse_plan") or {})
        self.assertEqual(asset_reuse_plan["baseline_generation_watermark"], baseline_generation["generation_watermark"])
        self.assertIn("Multimodal", list(asset_reuse_plan.get("missing_current_profile_search_queries") or []))
        self.assertEqual(asset_reuse_plan["covered_current_profile_search_query_count"], 0)
        self.assertEqual(
            int(dict(asset_reuse_plan.get("current_profile_search_exact_overlap_summary") or {}).get("gap_count") or 0),
            1,
        )
        self.assertEqual(
            str(dict(asset_reuse_plan.get("baseline_selection_explanation") or {}).get("reuse_basis") or ""),
            "organization_asset_registry_exact_membership",
        )
        self.assertEqual(
            int(
                dict(asset_reuse_plan.get("baseline_selection_explanation") or {})
                .get("current_profile_search_exact_overlap_summary", {})
                .get("gap_count")
                or 0
            ),
            1,
        )

    def test_asset_reuse_plan_reuses_selected_snapshot_directional_shards_without_delta(self) -> None:
        baseline_snapshot_id = "snapshot-openai-baseline"
        reasoning_snapshot_id = "snapshot-openai-reasoning"
        _, baseline_candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id=baseline_snapshot_id,
            candidates=[],
        )
        baseline_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=baseline_snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=[],
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": baseline_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 0,
                "evidence_count": 0,
                "profile_detail_count": 0,
                "explicit_profile_capture_count": 0,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 2,
                "completeness_score": 100.0,
                "completeness_band": "high",
                "current_lane_effective_candidate_count": 0,
                "former_lane_effective_candidate_count": 0,
                "current_lane_effective_ready": False,
                "former_lane_effective_ready": False,
                "selected_snapshot_ids": [baseline_snapshot_id, reasoning_snapshot_id],
                "source_snapshot_selection": {"selected_snapshot_ids": [baseline_snapshot_id, reasoning_snapshot_id]},
                "source_path": str(baseline_candidate_doc_path),
                "materialization_generation_key": str(baseline_generation.get("generation_key") or ""),
                "materialization_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
                "materialization_watermark": str(baseline_generation.get("generation_watermark") or ""),
                "summary": {
                    "target_company": "OpenAI",
                    "snapshot_id": baseline_snapshot_id,
                    "candidate_count": 0,
                    "current_lane_coverage": {"effective_candidate_count": 0, "effective_ready": False},
                    "former_lane_coverage": {"effective_candidate_count": 0, "effective_ready": False},
                    "selected_snapshot_ids": [baseline_snapshot_id, reasoning_snapshot_id],
                },
            },
            authoritative=True,
        )

        current_candidates = [
            Candidate(
                candidate_id="openai-reasoning-current-1",
                name_en="Reasoning Current",
                display_name="Reasoning Current",
                category="employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Research Scientist",
                focus_areas="Reasoning",
                linkedin_url="https://www.linkedin.com/in/openai-reasoning-current-1/",
            ).to_record(),
        ]
        former_candidates = [
            Candidate(
                candidate_id="openai-reasoning-former-1",
                name_en="Reasoning Former",
                display_name="Reasoning Former",
                category="former_employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="former",
                role="Research Scientist",
                focus_areas="Reasoning",
                linkedin_url="https://www.linkedin.com/in/openai-reasoning-former-1/",
            ).to_record(),
        ]
        current_row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=reasoning_snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="Reasoning-current",
            shard_title="Reasoning",
            search_query="Reasoning",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Reasoning",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        current_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=reasoning_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(current_row.get("shard_key") or "reasoning-current"),
            candidates=current_candidates,
            lane="profile_search",
            employment_scope="current",
        )
        current_row["materialization_generation_key"] = str(current_generation.get("generation_key") or "")
        current_row["materialization_generation_sequence"] = int(current_generation.get("generation_sequence") or 0)
        current_row["materialization_watermark"] = str(current_generation.get("generation_watermark") or "")
        self.store.upsert_acquisition_shard_registry(current_row)

        former_row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=reasoning_snapshot_id,
            lane="profile_search",
            employment_scope="former",
            strategy_type="former_employee_search",
            shard_id="Reasoning-former",
            shard_title="Reasoning",
            search_query="Reasoning",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Reasoning",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        former_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=reasoning_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(former_row.get("shard_key") or "reasoning-former"),
            candidates=former_candidates,
            lane="profile_search",
            employment_scope="former",
        )
        former_row["materialization_generation_key"] = str(former_generation.get("generation_key") or "")
        former_row["materialization_generation_sequence"] = int(former_generation.get("generation_sequence") or 0)
        former_row["materialization_watermark"] = str(former_generation.get("generation_watermark") or "")
        self.store.upsert_acquisition_shard_registry(former_row)

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "target_company": "OpenAI",
                "keywords": ["Reasoning"],
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
            }
        )

        asset_reuse_plan = dict(plan_result["plan"].get("asset_reuse_plan") or {})
        self.assertTrue(asset_reuse_plan["baseline_reuse_available"])
        self.assertFalse(asset_reuse_plan["requires_delta_acquisition"])
        self.assertEqual(asset_reuse_plan["missing_current_profile_search_query_count"], 0)
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_query_count"], 0)
        self.assertEqual(
            list(asset_reuse_plan.get("covered_current_profile_search_queries") or [])[0].get("coverage_mode"),
            "selected_snapshot_registry_match",
        )
        self.assertEqual(
            list(asset_reuse_plan.get("covered_former_profile_search_queries") or [])[0].get("coverage_mode"),
            "selected_snapshot_registry_match",
        )
        self.assertEqual(
            int(
                dict(asset_reuse_plan.get("current_profile_search_exact_overlap_summary") or {}).get(
                    "selected_snapshot_match_count"
                )
                or 0
            ),
            1,
        )

    def test_asset_reuse_plan_tolerates_tiny_same_snapshot_exact_overlap_gap_for_former_profile_search(self) -> None:
        snapshot_id = "snapshot-openai-coding-near-gap"
        current_candidate = Candidate(
            candidate_id="openai-coding-current-1",
            name_en="Coding Current",
            display_name="Coding Current",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Engineer",
            focus_areas="Coding",
            linkedin_url="https://www.linkedin.com/in/openai-coding-current-1/",
        ).to_record()
        former_overlap_candidates = [
            Candidate(
                candidate_id=f"openai-coding-former-{index}",
                name_en=f"Coding Former {index}",
                display_name=f"Coding Former {index}",
                category="former_employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="former",
                role="Engineer",
                focus_areas="Coding",
                linkedin_url=f"https://www.linkedin.com/in/openai-coding-former-{index}/",
            ).to_record()
            for index in range(1, 25)
        ]
        former_extra_baseline_candidates = [
            Candidate(
                candidate_id=f"openai-coding-former-{index}",
                name_en=f"Coding Former {index}",
                display_name=f"Coding Former {index}",
                category="former_employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="former",
                role="Engineer",
                focus_areas="Coding",
                linkedin_url=f"https://www.linkedin.com/in/openai-coding-former-{index}/",
            ).to_record()
            for index in range(27, 102)
        ]
        baseline_candidates = [current_candidate, *former_overlap_candidates, *former_extra_baseline_candidates]
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            candidates=baseline_candidates,
        )
        baseline_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            artifact_kind="authoritative_snapshot",
            artifact_key="baseline",
            candidates=baseline_candidates,
        )
        self._upsert_authoritative_org_registry(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            candidate_count=len(baseline_candidates),
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=99,
            materialization_generation_key=str(baseline_generation.get("generation_key") or ""),
            materialization_generation_sequence=int(baseline_generation.get("generation_sequence") or 0),
            materialization_watermark=str(baseline_generation.get("generation_watermark") or ""),
        )

        current_row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="Coding-current",
            shard_title="Coding",
            search_query="Coding",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Coding",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        current_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(current_row.get("shard_key") or "coding-current"),
            candidates=[current_candidate],
            lane="profile_search",
            employment_scope="current",
        )
        current_row["materialization_generation_key"] = str(current_generation.get("generation_key") or "")
        current_row["materialization_generation_sequence"] = int(current_generation.get("generation_sequence") or 0)
        current_row["materialization_watermark"] = str(current_generation.get("generation_watermark") or "")
        self.store.upsert_acquisition_shard_registry(current_row)

        former_row_candidates = [
            Candidate(
                candidate_id=f"openai-coding-former-{index}",
                name_en=f"Coding Former {index}",
                display_name=f"Coding Former {index}",
                category="former_employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="former",
                role="Engineer",
                focus_areas="Coding",
                linkedin_url=f"https://www.linkedin.com/in/openai-coding-former-{index}/",
            ).to_record()
            for index in range(1, 27)
        ]
        former_row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=snapshot_id,
            lane="profile_search",
            employment_scope="former",
            strategy_type="former_employee_search",
            shard_id="Coding-former",
            shard_title="Coding",
            search_query="Coding",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Coding",
            },
            result_count=26,
            estimated_total_count=26,
            status="completed",
        )
        former_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(former_row.get("shard_key") or "coding-former"),
            candidates=former_row_candidates,
            lane="profile_search",
            employment_scope="former",
        )
        former_row["materialization_generation_key"] = str(former_generation.get("generation_key") or "")
        former_row["materialization_generation_sequence"] = int(former_generation.get("generation_sequence") or 0)
        former_row["materialization_watermark"] = str(former_generation.get("generation_watermark") or "")
        self.store.upsert_acquisition_shard_registry(former_row)

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找OpenAI做Coding方向的人",
                "target_company": "OpenAI",
                "keywords": ["Coding"],
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
            }
        )

        asset_reuse_plan = dict(plan_result["plan"].get("asset_reuse_plan") or {})
        self.assertTrue(asset_reuse_plan["baseline_reuse_available"])
        self.assertFalse(asset_reuse_plan["requires_delta_acquisition"])
        self.assertEqual(asset_reuse_plan["missing_current_profile_search_query_count"], 0)
        self.assertEqual(asset_reuse_plan["missing_former_profile_search_query_count"], 0)
        self.assertEqual(
            list(asset_reuse_plan.get("covered_current_profile_search_queries") or [])[0].get("coverage_mode"),
            "exact_membership_subsumption",
        )
        self.assertEqual(
            list(asset_reuse_plan.get("covered_former_profile_search_queries") or [])[0].get("coverage_mode"),
            "exact_membership_near_subsumption",
        )
        self.assertEqual(
            int(
                dict(asset_reuse_plan.get("former_profile_search_exact_overlap_summary") or {}).get(
                    "near_subsumed_count"
                )
                or 0
            ),
            1,
        )

    def test_queue_workflow_backfills_missing_target_company_from_approved_review_scope(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我寻找LangChain Infra方向的人",
                "target_company": "",
                "keywords": ["infra"],
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        reviewed = self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "confirmed_company_scope": ["LangChain"],
                },
            }
        )
        self.assertEqual(reviewed["status"], "reviewed")
        self.assertEqual(reviewed["review"]["request"]["target_company"], "LangChain")

        queued = self.orchestrator.queue_workflow({"plan_review_id": review_id})
        self.assertEqual(queued["status"], "queued")
        job = self.orchestrator.get_job(queued["job_id"])
        self.assertIsNotNone(job)
        assert job is not None
        self.assertEqual(job["request"]["target_company"], "LangChain")

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

    def test_compile_plan_review_instruction_uses_model_then_schema_validation(self) -> None:
        class ReviewInstructionModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def normalize_review_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "use_company_employees_lane": True,
                        "force_fresh_run": True,
                        "unsupported_field": "ignored",
                    }
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=ReviewInstructionModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, ReviewInstructionModelClient()
            ),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想了解 Humans& 的 Coding 方向的 Researcher。",
                "target_company": "Humans&",
                "categories": ["researcher"],
                "employment_statuses": ["current"],
                "keywords": ["coding", "code generation"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        compiled = orchestrator.compile_plan_review_instruction(
            {
                "review_id": review_id,
                "reviewer": "tester",
                "instruction": "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。",
            }
        )

        self.assertEqual(compiled["status"], "compiled")
        self.assertEqual(compiled["instruction_compiler"]["source"], "model")
        self.assertEqual(compiled["instruction_compiler"]["provider"], "test-model")
        self.assertIn("intent_rewrite", compiled)
        self.assertTrue(compiled["intent_rewrite"]["request"]["matched"])
        self.assertEqual(
            str(dict(compiled["intent_rewrite"]["request"].get("rewrite") or {}).get("rewrite_id") or ""),
            "researcher_role_focus",
        )
        self.assertFalse(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(
            compiled["review_payload"]["decision"],
            {
                "acquisition_strategy_override": "full_company_roster",
                "use_company_employees_lane": True,
                "force_fresh_run": True,
                "confirmed_company_scope": ["Humans&"],
            },
        )

    def test_compile_plan_review_instruction_inferrs_company_employees_lane_from_colloquial_request(self) -> None:
        class ReviewInstructionModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def normalize_review_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "confirmed_company_scope": ["Humans&"],
                        "force_fresh_run": True,
                    }
                }

        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=ReviewInstructionModelClient(),
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(
                self.catalog, self.settings, self.store, ReviewInstructionModelClient()
            ),
        )

        plan_result = orchestrator.plan_workflow(
            {
                "raw_user_request": "我想了解 Humans& 的 Coding 方向的 Researcher。",
                "target_company": "Humans&",
                "categories": ["researcher"],
                "employment_statuses": ["current"],
                "keywords": ["coding", "code generation"],
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        compiled = orchestrator.compile_plan_review_instruction(
            {
                "review_id": review_id,
                "reviewer": "tester",
                "instruction": "我要求 scope 大一些，要公司全量成员，要重新跑。",
            }
        )

        self.assertEqual(compiled["status"], "compiled")
        self.assertIn("intent_rewrite", compiled)
        self.assertTrue(compiled["intent_rewrite"]["request"]["matched"])
        self.assertEqual(
            str(dict(compiled["intent_rewrite"]["request"].get("rewrite") or {}).get("rewrite_id") or ""),
            "researcher_role_focus",
        )
        self.assertFalse(compiled["intent_rewrite"]["instruction"]["matched"])
        self.assertEqual(
            compiled["review_payload"]["decision"],
            {
                "acquisition_strategy_override": "full_company_roster",
                "confirmed_company_scope": ["Humans&"],
                "force_fresh_run": True,
                "use_company_employees_lane": True,
            },
        )
        self.assertEqual(compiled["instruction_compiler"]["policy_inferred_keys"], ["use_company_employees_lane"])

    def test_workflow_progress_schema_summarizes_stage_and_worker_state(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的华人技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        review_id = plan_result["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )
        workflow = self.orchestrator.run_workflow_blocking({"plan_review_id": review_id})
        progress = self.orchestrator.get_job_progress(workflow["job"]["job_id"])
        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["status"], "completed")
        self.assertEqual(progress["stage"], "completed")
        self.assertIn("elapsed_seconds", progress)
        self.assertIn("timing", progress["progress"])
        self.assertGreaterEqual(progress["progress"]["counters"]["result_count"], 1)
        self.assertTrue(progress["progress"]["milestones"])
        self.assertIn("worker_summary", progress["progress"])
        self.assertGreaterEqual(len(progress["progress"]["completed_stages"]), 1)

    def test_job_progress_keeps_current_acquiring_stage_running_until_stage_transition(self) -> None:
        job_id = "job_progress_acquiring_running"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Acquisition checkpoint updated after Resolve company identifiers."},
        )
        self.store.append_job_event(job_id, "planning", "started", "Planning started.")
        self.store.append_job_event(job_id, "planning", "completed", "Planning completed.")
        self.store.append_job_event(job_id, "acquiring", "completed", "Resolve company identifiers completed.")

        progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        milestones = list(progress["progress"]["milestones"])
        acquiring = next(item for item in milestones if item["stage"] == "acquiring")
        planning = next(item for item in milestones if item["stage"] == "planning")
        self.assertEqual(planning["status"], "completed")
        self.assertEqual(acquiring["status"], "running")
        self.assertEqual(acquiring["completed_at"], "")
        self.assertNotIn("acquiring", list(progress["progress"]["completed_stages"]))

    def test_job_progress_surfaces_live_runtime_controls(self) -> None:
        job_id = "job_progress_runtime_controls"
        runner_log = self.settings.runtime_dir / "service_logs" / "workflow-runner-job_progress_runtime_controls.log"
        runner_log.parent.mkdir(parents=True, exist_ok=True)
        runner_log.write_text("runner exited\n", encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={
                "message": "queued",
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "started",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "job_recovery": {
                        "status": "started_deferred",
                        "service_name": f"job-recovery-{job_id}",
                        "scope": "job_scoped",
                    },
                    "workflow_runner": {
                        "status": "started",
                        "pid": 77701,
                        "log_path": str(runner_log),
                    },
                },
            },
        )

        def fake_read_service_status(_runtime_dir, service_name):  # type: ignore[no-untyped-def]
            if service_name == "worker-recovery-daemon":
                return {"service_name": service_name, "status": "running", "lock_status": "locked"}
            if service_name == f"job-recovery-{job_id}":
                return {"service_name": service_name, "status": "stale", "lock_status": "free"}
            return {"service_name": service_name, "status": "running", "lock_status": "locked"}

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                side_effect=fake_read_service_status,
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator._workflow_runner_process_alive",
                return_value=False,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "run_worker_recovery_once",
                return_value={"status": "completed", "workflow_resume": []},
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_controls = dict(progress["progress"].get("runtime_controls") or {})
        self.assertTrue(runtime_controls["shared_recovery"]["service_ready"])
        self.assertFalse(runtime_controls["job_recovery"]["service_ready"])
        self.assertFalse(runtime_controls["workflow_runner"]["process_alive"])
        self.assertIn("runner exited", runtime_controls["workflow_runner"]["log_tail"])
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "runner_not_alive")
        self.assertEqual(runtime_health.get("state"), "stalled")
        self.assertEqual(progress["auto_recovery"]["status"], "queued")

    def test_get_job_progress_triggers_auto_recovery_when_runner_is_not_alive(self) -> None:
        job_id = "job_progress_auto_takeover"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Workflow is running", "runtime_execution_mode": "hosted"},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )

        captured_payloads: list[dict[str, object]] = []
        recovery_called = threading.Event()
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_worker_recovery_once",
            side_effect=lambda payload=None: (
                captured_payloads.append(dict(payload or {}))
                or recovery_called.set()
                or {
                    "status": "completed",
                    "workflow_resume": [
                        {
                            "job_id": job_id,
                            "status": "resumed",
                            "resume_mode": "running_acquiring_recovery",
                            "job_status": "running",
                            "job_stage": "acquiring",
                        }
                    ],
                }
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(recovery_called.wait(timeout=1.0))
        self.assertEqual(len(captured_payloads), 1)
        self.assertEqual(captured_payloads[0]["job_id"], job_id)
        self.assertEqual(int(captured_payloads[0]["stale_after_seconds"]), 0)
        self.assertTrue(bool(captured_payloads[0]["workflow_queue_auto_takeover_enabled"]))
        self.assertEqual(progress["auto_recovery"]["status"], "queued")
        self.assertEqual(progress["auto_recovery"]["classification"], "runner_not_alive")

        with unittest.mock.patch.object(self.orchestrator, "run_worker_recovery_once") as recovery_mock:
            second_progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(second_progress)
        assert second_progress is not None
        recovery_mock.assert_not_called()
        self.assertEqual(second_progress["auto_recovery"]["status"], "skipped")
        self.assertEqual(second_progress["auto_recovery"]["reason"], "cooldown_active")

    def test_get_job_progress_retries_after_stale_auto_recovery_inflight(self) -> None:
        job_id = "job_progress_auto_takeover_stale_inflight"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Workflow is running", "runtime_execution_mode": "hosted"},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )
        self.orchestrator._progress_takeover_inflight[job_id] = {
            "classification": "runner_not_alive",
            "requested_execution_mode": "hosted",
            "effective_execution_mode": "hosted",
            "queued_at": "2026-01-01T00:00:00+00:00",
        }

        captured_payloads: list[dict[str, object]] = []
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_worker_recovery_once",
            side_effect=lambda payload=None: (
                captured_payloads.append(dict(payload or {}))
                or {
                    "status": "completed",
                    "workflow_resume": [
                        {
                            "job_id": job_id,
                            "status": "resumed",
                            "resume_mode": "running_acquiring_recovery",
                            "job_status": "running",
                            "job_stage": "acquiring",
                        }
                    ],
                }
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["auto_recovery"]["status"], "queued")
        self.assertEqual(len(captured_payloads), 1)
        runtime_events = self.store.list_job_events(job_id, stage="runtime_control", limit=10, descending=True)
        self.assertTrue(
            any(
                str(dict(event.get("payload") or {}).get("control") or "") == "progress_auto_takeover_reset"
                for event in runtime_events
            )
        )

    def test_runtime_health_treats_remote_wait_with_recovery_as_progressing(self) -> None:
        job_id = "job_runtime_remote_wait"
        request_payload = {
            "raw_user_request": "Find Google infra people",
            "target_company": "Google",
            "categories": ["employee"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for remote Harvest run",
                "blocked_task": "enrich_linkedin_profiles",
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "started",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "job_recovery": {
                        "status": "started",
                        "service_name": f"job-recovery-{job_id}",
                        "scope": "job_scoped",
                    },
                    "workflow_runner": {
                        "status": "started",
                        "pid": 88001,
                    },
                },
            },
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::01",
            stage="acquiring",
            span_name="harvest_profile_batch",
            budget_payload={"requested_url_count": 100},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/example/"]},
            metadata={"recovery_kind": "harvest_profile_batch"},
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="waiting_remote_harvest",
            checkpoint_payload={"run_id": "run-remote"},
            output_payload={"summary": {"status": "submitted"}},
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                side_effect=[
                    {"service_name": "worker-recovery-daemon", "status": "running", "lock_status": "locked"},
                    {"service_name": f"job-recovery-{job_id}", "status": "running", "lock_status": "locked"},
                ],
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator._workflow_runner_process_alive",
                return_value=False,
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "waiting_on_remote_provider")
        self.assertEqual(runtime_health.get("state"), "progressing")
        self.assertEqual(int(runtime_health.get("waiting_remote_harvest_count") or 0), 1)

    def test_runtime_health_treats_remote_wait_with_hosted_watchdog_as_progressing(self) -> None:
        job_id = "job_runtime_remote_wait_hosted_watchdog"
        request_payload = {
            "raw_user_request": "Find Google infra people",
            "target_company": "Google",
            "categories": ["employee"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Waiting for remote Harvest run",
                "blocked_task": "enrich_linkedin_profiles",
                "runtime_controls": {
                    "hosted_runtime_watchdog": {
                        "status": "started",
                        "service_name": "server-runtime-watchdog",
                        "scope": "hosted_runtime_watchdog",
                    },
                    "shared_recovery": {
                        "status": "scheduled",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "workflow_runner": {
                        "status": "started",
                        "pid": 88002,
                    },
                },
            },
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::01",
            stage="acquiring",
            span_name="harvest_profile_batch",
            budget_payload={"requested_url_count": 100},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/example/"]},
            metadata={"recovery_kind": "harvest_profile_batch"},
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="waiting_remote_harvest",
            checkpoint_payload={"run_id": "run-remote"},
            output_payload={"summary": {"status": "submitted"}},
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                side_effect=[
                    {"service_name": "server-runtime-watchdog", "status": "running", "lock_status": "locked"},
                    {"service_name": "worker-recovery-daemon", "status": "stale", "lock_status": "locked"},
                ],
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator._workflow_runner_process_alive",
                return_value=False,
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_controls = dict(progress["progress"].get("runtime_controls") or {})
        self.assertTrue(bool(runtime_controls["hosted_runtime_watchdog"]["service_ready"]))
        self.assertFalse(bool(runtime_controls["shared_recovery"]["service_ready"]))
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "waiting_on_remote_provider")
        self.assertEqual(runtime_health.get("state"), "progressing")

    def test_persist_workflow_runtime_controls_emits_diff_events_only(self) -> None:
        job_id = "job_runtime_control_events"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )

        self.orchestrator._persist_workflow_runtime_controls(
            job_id,
            {
                "shared_recovery": {
                    "status": "started",
                    "service_name": "worker-recovery-daemon",
                    "scope": "shared",
                    "mode": "sidecar",
                    "handshake": {"status": "ready"},
                },
                "workflow_runner_control": {
                    "status": "started",
                    "handshake": {"status": "advanced", "job_status": "running", "job_stage": "planning"},
                    "runner": {"status": "started", "pid": 321},
                },
            },
        )
        first_events = self.store.list_job_events(job_id)
        self.assertEqual(len(first_events), 2)
        self.assertEqual(first_events[0]["stage"], "runtime_control")
        self.assertEqual(first_events[1]["payload"]["control"], "workflow_runner_control")

        self.orchestrator._persist_workflow_runtime_controls(
            job_id,
            {
                "shared_recovery": {
                    "status": "started",
                    "service_name": "worker-recovery-daemon",
                    "scope": "shared",
                    "mode": "sidecar",
                    "handshake": {"status": "ready"},
                },
                "workflow_runner_control": {
                    "status": "started",
                    "handshake": {"status": "advanced", "job_status": "running", "job_stage": "planning"},
                    "runner": {"status": "started", "pid": 321},
                },
            },
        )
        second_events = self.store.list_job_events(job_id)
        self.assertEqual(len(second_events), 2)

        self.orchestrator._persist_workflow_runtime_controls(
            job_id,
            {
                "shared_recovery": {
                    "status": "already_running",
                    "service_name": "worker-recovery-daemon",
                    "scope": "shared",
                    "mode": "sidecar",
                    "handshake": {"status": "already_running"},
                }
            },
        )
        final_events = self.store.list_job_events(job_id)
        self.assertEqual(len(final_events), 3)
        self.assertEqual(final_events[-1]["status"], "already_running")
        self.assertEqual(final_events[-1]["payload"]["control"], "shared_recovery")

    def test_persist_workflow_runtime_controls_deferred_retries_locked_database(self) -> None:
        job_id = "job_runtime_control_deferred_retry"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )

        original = self.orchestrator._persist_workflow_runtime_controls
        call_count = {"count": 0}

        def flaky(job_id_value: str, controls_value: dict[str, object]) -> None:
            call_count["count"] += 1
            if call_count["count"] == 1:
                raise sqlite3.OperationalError("database is locked")
            original(job_id_value, controls_value)

        with unittest.mock.patch.object(
            self.orchestrator,
            "_persist_workflow_runtime_controls",
            side_effect=flaky,
        ):
            result = self.orchestrator._persist_workflow_runtime_controls_deferred(
                job_id,
                {
                    "workflow_runner_control": {
                        "status": "started",
                        "handshake": {"status": "advanced", "job_status": "running", "job_stage": "planning"},
                    }
                },
                max_attempts=3,
                initial_delay_seconds=0.0,
                run_async=False,
            )

        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(call_count["count"], 2)
        stored_job = self.store.get_job(job_id)
        self.assertIsNotNone(stored_job)
        assert stored_job is not None
        runtime_controls = dict(dict(stored_job.get("summary") or {}).get("runtime_controls") or {})
        self.assertEqual(runtime_controls["workflow_runner_control"]["status"], "started")

    def test_start_workflow_runner_managed_defers_runtime_control_persistence(self) -> None:
        queued_payload = {
            "job_id": "job_start_workflow_managed",
            "status": "queued",
            "dispatch": {},
        }
        runner_control = {
            "status": "started",
            "runner": {"status": "started", "job_id": "job_start_workflow_managed", "pid": 321},
            "handshake": {"status": "advanced", "job_status": "running", "job_stage": "planning"},
        }
        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "queue_workflow",
                return_value=dict(queued_payload),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_ensure_hosted_runtime_watchdog_deferred",
                return_value={"status": "scheduled", "scope": "hosted_runtime_watchdog", "mode": "deferred"},
            ) as hosted_watchdog_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "ensure_hosted_runtime_watchdog",
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_ensure_shared_recovery_deferred",
                return_value={"status": "scheduled", "scope": "shared", "mode": "deferred"},
            ) as shared_recovery_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "ensure_shared_recovery",
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_start_workflow_runner_with_handshake",
                return_value=runner_control,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_workflow_runtime_controls_deferred",
                return_value={"status": "scheduled"},
            ) as deferred_mock,
        ):
            result = self.orchestrator.start_workflow_runner_managed(
                {"target_company": "Google", "auto_job_daemon": True}
            )

        hosted_watchdog_mock.assert_called_once()
        shared_recovery_mock.assert_called_once()
        deferred_mock.assert_called_once()
        self.assertEqual(deferred_mock.call_args.args[0], "job_start_workflow_managed")
        persisted_controls = deferred_mock.call_args.args[1]
        self.assertEqual(persisted_controls["hosted_runtime_watchdog"]["status"], "scheduled")
        self.assertEqual(persisted_controls["workflow_runner_control"]["status"], "started")
        self.assertEqual(result["hosted_runtime_watchdog"]["status"], "scheduled")
        self.assertEqual(result["workflow_runner_control"]["status"], "started")

    def test_sqlite_store_enables_wal_and_busy_timeout(self) -> None:
        journal_mode = self.store._connection.execute("PRAGMA journal_mode").fetchone()[0]
        busy_timeout = self.store._connection.execute("PRAGMA busy_timeout").fetchone()[0]
        self.assertEqual(str(journal_mode).lower(), "wal")
        self.assertGreaterEqual(int(busy_timeout), 60000)

    def test_job_progress_classifies_blocked_acquisition_workers(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_progress_blocked_workers"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::01",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI former employee"},
            metadata={"target_company": "xAI"},
            handoff_from_lane="triage_planner",
        )

        progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "blocked_on_acquisition_workers")
        self.assertEqual(int(runtime_health.get("pending_worker_count") or 0), 1)

    def test_get_job_progress_triggers_auto_recovery_when_blocked_on_acquisition_workers(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_progress_blocked_workers_takeover"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::01",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI former employee"},
            metadata={"target_company": "xAI"},
            handoff_from_lane="triage_planner",
        )

        captured_payloads: list[dict[str, object]] = []
        recovery_called = threading.Event()
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_worker_recovery_once",
            side_effect=lambda payload=None: (
                captured_payloads.append(dict(payload or {}))
                or recovery_called.set()
                or {
                    "status": "completed",
                    "workflow_resume": [],
                }
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertTrue(recovery_called.wait(timeout=1.0))
        self.assertEqual(len(captured_payloads), 1)
        self.assertEqual(captured_payloads[0]["job_id"], job_id)
        self.assertEqual(int(captured_payloads[0]["stale_after_seconds"]), 0)
        self.assertEqual(progress["auto_recovery"]["status"], "queued")
        self.assertEqual(progress["auto_recovery"]["classification"], "blocked_on_acquisition_workers")

    def test_get_job_progress_queues_auto_recovery_without_blocking_response(self) -> None:
        job_id = "job_progress_async_takeover"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Workflow is running", "runtime_execution_mode": "hosted"},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )

        recovery_started = threading.Event()
        recovery_can_finish = threading.Event()

        def _slow_recovery(payload=None):  # type: ignore[no-untyped-def]
            recovery_started.set()
            recovery_can_finish.wait(timeout=1.0)
            return {"status": "completed", "workflow_resume": []}

        started_at = time.perf_counter()
        with unittest.mock.patch.object(self.orchestrator, "run_worker_recovery_once", side_effect=_slow_recovery):
            progress = self.orchestrator.get_job_progress(job_id)
            elapsed = time.perf_counter() - started_at
            self.assertTrue(recovery_started.wait(timeout=1.0))
            self.assertLess(elapsed, 0.2)
            self.assertIsNotNone(progress)
            assert progress is not None
            self.assertEqual(progress["auto_recovery"]["status"], "queued")
            self.assertEqual(progress["auto_recovery"]["classification"], "runner_not_alive")
            recovery_can_finish.set()

        runtime_events = self.store.list_job_events(job_id, stage="runtime_control", limit=10, descending=True)
        controls = [dict(event.get("payload") or {}).get("control") for event in runtime_events]
        self.assertIn("progress_auto_takeover", controls)

    def test_get_job_progress_short_ttl_caches_runtime_service_status(self) -> None:
        job_id = "job_progress_service_status_cache"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={
                "message": "Retrieval is running",
                "runtime_execution_mode": "hosted",
                "runtime_controls": {
                    "hosted_runtime_watchdog": {
                        "status": "started",
                        "service_name": "server-runtime-watchdog",
                        "scope": "hosted_runtime_watchdog",
                    }
                },
            },
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.time.monotonic",
                side_effect=[100.0, 100.2, 101.2],
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                return_value={
                    "service_name": "server-runtime-watchdog",
                    "status": "running",
                    "lock_status": "locked",
                },
            ) as status_mock,
        ):
            first_progress = self.orchestrator.get_job_progress(job_id)
            second_progress = self.orchestrator.get_job_progress(job_id)
            third_progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(first_progress)
        self.assertIsNotNone(second_progress)
        self.assertIsNotNone(third_progress)
        assert first_progress is not None
        assert second_progress is not None
        assert third_progress is not None
        self.assertEqual(status_mock.call_count, 2)
        self.assertEqual(
            first_progress["progress"]["runtime_controls"]["hosted_runtime_watchdog"]["service_status"]["status"],
            "running",
        )
        self.assertEqual(
            second_progress["progress"]["runtime_controls"]["hosted_runtime_watchdog"]["service_status"]["status"],
            "running",
        )

    def test_get_job_progress_uses_count_queries_instead_of_loading_full_results(self) -> None:
        job_id = "job_progress_count_only"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload={"target_company": "OpenAI"},
            plan_payload={},
            summary_payload={"message": "Retrieval is running"},
        )

        with (
            unittest.mock.patch.object(
                self.store,
                "get_job_results",
                side_effect=AssertionError("progress should not load full job results"),
            ),
            unittest.mock.patch.object(
                self.store,
                "list_manual_review_items",
                side_effect=AssertionError("progress should not load full manual review rows"),
            ),
            unittest.mock.patch.object(
                self.store,
                "count_job_results",
                return_value=12,
            ) as result_count_mock,
            unittest.mock.patch.object(
                self.store,
                "count_manual_review_items",
                return_value=5,
            ) as manual_review_count_mock,
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        result_count_mock.assert_called()
        manual_review_count_mock.assert_called()
        self.assertEqual(progress["progress"]["counters"]["result_count"], 12)
        self.assertEqual(progress["progress"]["counters"]["manual_review_count"], 5)
        latest_metrics = dict(progress["progress"].get("latest_metrics") or {})
        self.assertEqual(int(latest_metrics.get("result_count") or 0), 12)
        self.assertEqual(int(latest_metrics.get("manual_review_count") or 0), 5)

    def test_get_job_progress_uses_materialized_event_summary_instead_of_loading_full_events(self) -> None:
        job_id = "job_progress_event_summary"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload={"target_company": "OpenAI"},
            plan_payload={},
            summary_payload={"message": "Retrieval is running"},
        )
        self.store.append_job_event(
            job_id,
            "planning",
            "completed",
            "Planning done.",
            {"snapshot_id": "snapshot-1", "candidate_count": 7},
        )
        self.store.append_job_event(
            job_id,
            "retrieving",
            "running",
            "Retrieval running.",
            {"artifact_paths": {"results": "/tmp/results.json"}},
        )

        with unittest.mock.patch.object(
            self.store,
            "list_job_events",
            side_effect=AssertionError("progress should not load full job events"),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        self.assertEqual(progress["progress"]["counters"]["event_count"], 2)
        self.assertEqual(progress["progress"]["latest_event"]["detail"], "Retrieval running.")
        milestones = {item["stage"]: item for item in progress["progress"]["milestones"]}
        self.assertEqual(milestones["planning"]["status"], "completed")
        self.assertEqual(milestones["retrieving"]["status"], "running")
        latest_metrics = dict(progress["progress"].get("latest_metrics") or {})
        self.assertEqual(str(latest_metrics.get("snapshot_id") or ""), "snapshot-1")
        self.assertEqual(int(latest_metrics.get("candidate_count") or 0), 7)
        self.assertEqual(
            dict(latest_metrics.get("artifact_paths") or {}).get("results"),
            "/tmp/results.json",
        )

    def test_runtime_health_ignores_stale_shared_recovery_when_runner_is_alive(self) -> None:
        job_id = "job_runtime_runner_alive_shared_stale"
        runner_log = self.settings.runtime_dir / "service_logs" / "workflow-runner-job_runtime_runner_alive.log"
        runner_log.parent.mkdir(parents=True, exist_ok=True)
        runner_log.write_text("runner healthy\n", encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={
                "message": "Running acquisition tasks",
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "scheduled",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "workflow_runner": {
                        "status": "started",
                        "pid": 77702,
                        "log_path": str(runner_log),
                    },
                },
            },
        )

        with (
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                return_value={"service_name": "worker-recovery-daemon", "status": "stale", "lock_status": "locked"},
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator._workflow_runner_process_alive",
                return_value=True,
            ),
        ):
            progress = self.orchestrator.get_job_progress(job_id)

        self.assertIsNotNone(progress)
        assert progress is not None
        runtime_controls = dict(progress["progress"].get("runtime_controls") or {})
        self.assertFalse(bool(runtime_controls["shared_recovery"]["service_ready"]))
        self.assertTrue(bool(runtime_controls["workflow_runner"]["process_alive"]))
        runtime_health = dict(progress["progress"].get("runtime_health") or {})
        self.assertEqual(runtime_health.get("classification"), "healthy_running")
        self.assertEqual(runtime_health.get("state"), "progressing")

    def test_run_worker_recovery_once_emits_runtime_heartbeat(self) -> None:
        job_id = "job_runtime_heartbeat"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "Running acquisition tasks"},
        )

        recovery = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "workflow_auto_resume_enabled": False,
                "workflow_queue_auto_takeover_enabled": False,
                "runtime_heartbeat_interval_seconds": 0,
            }
        )

        self.assertEqual(recovery["status"], "completed")
        heartbeat = list(recovery.get("runtime_heartbeat") or [])
        self.assertEqual(len(heartbeat), 1)
        self.assertEqual(heartbeat[0]["status"], "emitted")
        heartbeat_events = self.store.list_job_events(job_id, stage="runtime_heartbeat")
        self.assertEqual(len(heartbeat_events), 1)
        self.assertEqual(heartbeat_events[0]["payload"]["source"], "job_recovery_daemon")

        second = self.orchestrator.run_worker_recovery_once(
            {
                "job_id": job_id,
                "workflow_auto_resume_enabled": False,
                "workflow_queue_auto_takeover_enabled": False,
                "runtime_heartbeat_interval_seconds": 3600,
            }
        )
        second_heartbeat = list(second.get("runtime_heartbeat") or [])
        self.assertEqual(len(second_heartbeat), 1)
        self.assertEqual(second_heartbeat[0]["status"], "skipped")
        self.assertEqual(len(self.store.list_job_events(job_id, stage="runtime_heartbeat")), 1)

    def test_runtime_job_events_are_compacted_by_group(self) -> None:
        job_id = "job_runtime_event_compaction"
        for index in range(20):
            self.store.append_job_event(
                job_id,
                stage="runtime_heartbeat",
                status="observed",
                detail="heartbeat",
                payload={"source": "shared_recovery_daemon", "sequence": index},
            )
        heartbeat_events = self.store.list_job_events(job_id, stage="runtime_heartbeat")
        self.assertEqual(len(heartbeat_events), 12)
        self.assertEqual(int(heartbeat_events[0]["payload"]["sequence"]), 8)
        self.assertEqual(int(heartbeat_events[-1]["payload"]["sequence"]), 19)

        for index in range(12):
            self.store.append_job_event(
                job_id,
                stage="runtime_control",
                status="updated",
                detail="control",
                payload={"control": "shared_recovery", "sequence": index},
            )
        control_events = self.store.list_job_events(job_id, stage="runtime_control")
        self.assertEqual(len(control_events), 8)
        self.assertEqual(int(control_events[0]["payload"]["sequence"]), 4)
        self.assertEqual(int(control_events[-1]["payload"]["sequence"]), 11)

    def test_job_run_lock_uses_store_backed_lease(self) -> None:
        job_id = "job_store_lease_lock"
        with self.orchestrator._job_run_lock(job_id) as first_lock:
            self.assertIsNotNone(first_lock)
            lease = self.store.get_workflow_job_lease(job_id)
            self.assertIsNotNone(lease)
            assert lease is not None
            self.assertEqual(str(lease.get("job_id") or ""), job_id)
            self.assertFalse(bool(lease.get("expired")))
            with self.orchestrator._job_run_lock(job_id) as second_lock:
                self.assertIsNone(second_lock)
        released = self.store.get_workflow_job_lease(job_id)
        self.assertIsNone(released)

    def test_job_run_lock_skips_file_lock_when_postgres_coordination_is_authoritative(self) -> None:
        job_id = "job_postgres_lease_lock"
        with (
            unittest.mock.patch.object(self.store, "workflow_job_coordination_uses_postgres", return_value=True),
            unittest.mock.patch("sourcing_agent.orchestrator.fcntl.flock") as flock_mock,
        ):
            with self.orchestrator._job_run_lock(job_id) as lock_handle:
                self.assertIsNotNone(lock_handle)
                assert lock_handle is not None
                self.assertFalse(bool(lock_handle.get("uses_filesystem_lock")))
        flock_mock.assert_not_called()
        self.assertIsNone(self.store.get_workflow_job_lease(job_id))

    def test_job_runtime_idle_seconds_uses_utc_storage_timestamps(self) -> None:
        updated_at = (datetime.now(timezone.utc) - timedelta(seconds=7)).strftime("%Y-%m-%d %H:%M:%S")
        idle_seconds = _job_runtime_idle_seconds({"updated_at": updated_at})
        self.assertGreaterEqual(idle_seconds, 5)
        self.assertLess(idle_seconds, 30)

    def test_release_stale_workflow_job_lease_when_runner_is_missing_and_idle(self) -> None:
        job_id = "job_release_stale_idle_runner_missing"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Running acquisition tasks"},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )
        lease = self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner="test-runner",
            lease_seconds=900,
            lease_token="lease-token-test",
        )
        self.assertTrue(bool(lease.get("acquired")))
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE workflow_job_leases SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )

        released = self.orchestrator._release_stale_workflow_job_lease_for_recovery(job_id)

        self.assertTrue(bool(released.get("released")))
        self.assertEqual(str(released.get("classification") or ""), "runner_not_alive")
        self.assertIsNone(self.store.get_workflow_job_lease(job_id))

    def test_release_stale_workflow_job_lease_skips_when_lease_heartbeat_is_fresh(self) -> None:
        job_id = "job_skip_release_when_lease_fresh"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Running local acquisition stage"},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )
        lease = self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner="test-runner",
            lease_seconds=900,
            lease_token="lease-token-fresh",
        )
        self.assertTrue(bool(lease.get("acquired")))

        released = self.orchestrator._release_stale_workflow_job_lease_for_recovery(job_id)

        self.assertFalse(bool(released.get("released")))
        self.assertEqual(str(released.get("reason") or ""), "runtime_not_takeover_safe")
        self.assertEqual(str(released.get("classification") or ""), "healthy_running")
        self.assertIsNotNone(self.store.get_workflow_job_lease(job_id))

    def test_job_run_lock_recovers_stale_file_lock_when_runner_is_missing_and_idle(self) -> None:
        job_id = "job_recover_stale_file_lock"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Google"},
            plan_payload={},
            summary_payload={"message": "Runner disappeared before resume."},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.fcntl.flock",
            side_effect=[OSError("stale lock"), None, None],
        ):
            with self.orchestrator._job_run_lock(job_id) as lock_handle:
                self.assertIsNotNone(lock_handle)

        self.assertIsNone(self.store.get_workflow_job_lease(job_id))
        recovery_events = [
            event
            for event in self.store.list_job_events(job_id, stage="runtime_control")
            if str(event.get("status") or "") == "recovered"
        ]
        self.assertTrue(
            any("stale workflow job file lock" in str(event.get("detail") or "") for event in recovery_events)
        )

    def test_resume_queued_workflow_prefers_hosted_dispatch_when_requested(self) -> None:
        job_id = "job_resume_queued_hosted"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "Workflow queued", "runtime_execution_mode": "hosted"},
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_start_hosted_workflow_thread",
                return_value={"job_id": job_id, "status": "started", "mode": "workflow", "source": "workflow_recovery"},
            ) as hosted_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_start_workflow_runner_with_handshake",
            ) as runner_mock,
        ):
            result = self.orchestrator._resume_queued_workflow_if_ready(job_id)

        hosted_mock.assert_called_once_with(job_id, source="workflow_recovery")
        runner_mock.assert_not_called()
        self.assertEqual(result["status"], "takeover_started")
        self.assertEqual(result["mode"], "hosted")

    def test_resume_queued_hosted_workflow_dispatches_without_holding_job_lock(self) -> None:
        job_id = "job_resume_queued_hosted_no_outer_lock"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "Workflow queued", "runtime_execution_mode": "hosted"},
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_start_hosted_workflow_thread",
                return_value={"job_id": job_id, "status": "started", "mode": "workflow", "source": "workflow_recovery"},
            ) as hosted_mock,
            unittest.mock.patch.object(self.orchestrator, "_job_run_lock") as lock_mock,
        ):
            result = self.orchestrator._resume_queued_workflow_if_ready(job_id)

        hosted_mock.assert_called_once_with(job_id, source="workflow_recovery")
        lock_mock.assert_not_called()
        self.assertEqual(result["status"], "takeover_started")
        self.assertEqual(result["mode"], "hosted")

    def test_start_workflow_dispatches_hosted_runner_before_recovery_daemons(self) -> None:
        call_order: list[str] = []

        def fake_hosted_dispatch(job_id: str, *, source: str) -> dict[str, object]:
            call_order.append(f"dispatch:{source}")
            return {"job_id": job_id, "status": "started", "mode": "workflow", "source": source}

        def fake_shared_recovery(payload: dict[str, object] | None = None) -> dict[str, object]:
            call_order.append("shared_recovery")
            return {"status": "stubbed"}

        def fake_job_recovery(job_id: str, payload: dict[str, object] | None = None) -> dict[str, object]:
            call_order.append("job_recovery")
            return {"status": "stubbed", "job_id": job_id}

        with (
            unittest.mock.patch.object(self.orchestrator, "_start_hosted_workflow_thread", side_effect=fake_hosted_dispatch),
            unittest.mock.patch.object(self.orchestrator, "ensure_shared_recovery", side_effect=fake_shared_recovery),
            unittest.mock.patch.object(self.orchestrator, "ensure_job_scoped_recovery", side_effect=fake_job_recovery),
        ):
            workflow = self.orchestrator.start_workflow(
                {
                    "target_company": "Reflection AI",
                    "categories": ["employee"],
                    "skip_plan_review": True,
                }
            )

        self.assertEqual(workflow["status"], "queued")
        self.assertEqual(call_order[:3], ["dispatch:start_workflow", "shared_recovery", "job_recovery"])

    def test_start_hosted_workflow_thread_skips_duplicate_dispatch_with_fresh_marker(self) -> None:
        job_id = "job_hosted_dispatch_deduped"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "MiroMind.ai"},
            plan_payload={},
            summary_payload={"message": "Workflow queued", "runtime_execution_mode": "hosted"},
        )

        with unittest.mock.patch.object(threading.Thread, "start", autospec=True, return_value=None) as start_mock:
            first = self.orchestrator._start_hosted_workflow_thread(job_id, source="start_workflow")
            second = self.orchestrator._start_hosted_workflow_thread(job_id, source="workflow_recovery")

        self.assertEqual(first["status"], "started")
        self.assertEqual(second["status"], "skipped")
        self.assertEqual(second["reason"], "hosted_dispatch_inflight")
        self.assertEqual(start_mock.call_count, 1)
        stored_job = self.store.get_job(job_id) or {}
        hosted_dispatch = dict(dict(stored_job.get("summary") or {}).get("hosted_dispatch") or {})
        self.assertEqual(hosted_dispatch.get("mode"), "workflow")
        self.assertEqual(hosted_dispatch.get("source"), "start_workflow")

    def test_progress_auto_recovery_skips_fresh_hosted_dispatch_for_queued_job(self) -> None:
        job_id = "job_hosted_dispatch_progress_guard"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "MiroMind.ai"},
            plan_payload={},
            summary_payload={
                "message": "Workflow queued",
                "runtime_execution_mode": "hosted",
                "hosted_dispatch": {
                    "status": "started",
                    "mode": "workflow",
                    "source": "start_workflow",
                    "dispatched_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        )

        job = self.store.get_job(job_id) or {}
        result = self.orchestrator._maybe_auto_recover_workflow_on_progress(
            job=job,
            events=[],
            workers=[],
            runtime_controls={},
        )

        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "runtime_not_takeover_eligible")
        self.assertEqual(result["classification"], "hosted_dispatch_inflight")

    def test_start_hosted_stage2_dispatch_ignores_recent_workflow_marker(self) -> None:
        job_id = "job_stage2_dispatch_not_blocked_by_workflow_marker"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="retrieving",
            request_payload={"target_company": "MiroMind.ai"},
            plan_payload={},
            summary_payload={
                "message": "Stage 1 preview ready",
                "runtime_execution_mode": "hosted",
                "awaiting_user_action": "continue_stage2",
                "stage2_transition_state": "queued",
                "hosted_dispatch": {
                    "status": "started",
                    "mode": "workflow",
                    "source": "start_workflow",
                    "dispatched_at": datetime.now(timezone.utc).isoformat(),
                },
            },
        )

        with unittest.mock.patch.object(threading.Thread, "start", autospec=True, return_value=None) as start_mock:
            result = self.orchestrator._start_hosted_workflow_thread(job_id, source="workflow_recovery")

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["mode"], "continue_stage2")
        self.assertEqual(start_mock.call_count, 1)

    def test_runtime_health_prefers_materialized_snapshot_when_fresh(self) -> None:
        first = self.orchestrator.get_runtime_health({})
        self.assertEqual(str(dict(first.get("cache") or {}).get("status") or ""), "materialized")
        self.assertTrue(self.orchestrator.runtime_metrics_snapshot_path.exists())
        with unittest.mock.patch.object(
            self.store, "list_jobs", side_effect=AssertionError("runtime health should use cached snapshot")
        ):
            cached = self.orchestrator.get_runtime_health({})
        self.assertEqual(str(dict(cached.get("cache") or {}).get("status") or ""), "hit")

    def test_storage_serializes_path_payloads_for_job_events_and_summaries(self) -> None:
        job_id = "job_storage_json_safe"
        payload_path = Path(self.tempdir.name) / "payload.json"
        self.store.append_job_event(
            job_id,
            stage="acquiring",
            status="completed",
            detail="json safe payload",
            payload={"path": payload_path, "nested": {"items": [payload_path]}},
        )
        events = self.store.list_job_events(job_id)
        self.assertEqual(events[0]["payload"]["path"], str(payload_path))
        self.assertEqual(events[0]["payload"]["nested"]["items"][0], str(payload_path))

        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={"artifact_dir": payload_path},
            summary_payload={"candidate_doc_path": payload_path},
        )
        stored = self.store.get_job(job_id)
        assert stored is not None
        self.assertEqual(stored["summary"]["candidate_doc_path"], str(payload_path))
        self.assertEqual(stored["plan"]["artifact_dir"], str(payload_path))

    def test_runtime_metrics_reports_refresh_and_reconcile_counters(self) -> None:
        job_id = "job_runtime_metrics_refresh"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={
                "message": "refreshing",
                "pre_retrieval_refresh": {
                    "status": "completed",
                    "snapshot_id": "snapshot-1",
                    "search_seed_worker_count": 2,
                    "harvest_prefetch_worker_count": 1,
                },
                "background_reconcile": {
                    "search_seed": {
                        "status": "inline_refreshed",
                        "applied_worker_count": 2,
                        "added_entry_count": 5,
                    },
                    "harvest_prefetch": {
                        "status": "inline_refreshed",
                        "applied_worker_count": 1,
                    },
                },
            },
        )

        runtime = self.orchestrator.get_runtime_metrics({"force_refresh": True})
        metrics = dict(runtime.get("metrics") or {})
        refresh_metrics = dict(runtime.get("refresh_metrics") or {})
        service_readiness = dict(runtime.get("service_readiness") or {})
        self.assertEqual(int(metrics.get("pre_retrieval_refresh_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("inline_search_seed_worker_count") or 0), 2)
        self.assertEqual(int(metrics.get("inline_harvest_prefetch_worker_count") or 0), 1)
        self.assertEqual(int(metrics.get("background_reconcile_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("background_search_seed_reconcile_job_count") or 0), 1)
        self.assertEqual(int(metrics.get("background_harvest_prefetch_reconcile_job_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("pre_retrieval_refresh_job_count") or 0), 1)
        self.assertEqual(int(refresh_metrics.get("background_reconcile_job_count") or 0), 1)
        self.assertIn(service_readiness.get("recommended_workflow_entrypoint"), {"serve"})
        self.assertIn(service_readiness.get("standalone_cli_fallback"), {"managed_subprocess"})
        self.assertIn("auto_recovery_ready", service_readiness)

    def test_runtime_health_reports_stalled_jobs(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_runtime_health_stalled"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::02",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={"query": "xAI former employee"},
            metadata={"target_company": "xAI"},
            handoff_from_lane="triage_planner",
        )

        health = self.orchestrator.get_runtime_health({"active_limit": 10})

        self.assertEqual(health["status"], "degraded")
        self.assertGreaterEqual(int(health["metrics"]["stalled_job_count"] or 0), 1)
        self.assertEqual(health["stalled_jobs"][0]["job_id"], job_id)
        self.assertEqual(
            health["stalled_jobs"][0]["runtime_health"]["classification"],
            "blocked_on_acquisition_workers",
        )

    def test_runtime_health_excludes_stale_acquiring_jobs_when_remote_workers_are_progressing(self) -> None:
        request_payload = {
            "raw_user_request": "Find Google multimodal people",
            "target_company": "Google",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_runtime_health_remote_progress"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting on keyword shards"},
        )
        with self.store._lock, self.store._connection:
            self.store._connection.execute(
                "UPDATE jobs SET updated_at = ? WHERE job_id = ?",
                ("2026-01-01 00:00:00", job_id),
            )

        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::google::kw_veo",
            stage="acquiring",
            span_name="harvest_company_employees:kw_veo",
            budget_payload={"max_pages": 100, "page_limit": 25},
            input_payload={"search_query": "Veo"},
            metadata={"target_company": "Google"},
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.checkpoint_worker(
            handle,
            checkpoint_payload={"stage": "waiting_remote_harvest"},
            output_payload={"message": "Submitted remote harvest run."},
            status="queued",
        )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.read_service_status",
            return_value={"status": "running", "lock_status": "locked", "pid_alive": True},
        ):
            health = self.orchestrator.get_runtime_health({"active_limit": 10, "force_refresh": True})

        self.assertEqual(health["status"], "ok")
        self.assertEqual(int(health["metrics"]["stale_acquiring_job_count"] or 0), 0)
        self.assertEqual(list(health["stale_jobs"]["acquiring"]), [])

    def test_get_system_progress_aggregates_workflow_registry_and_object_sync(self) -> None:
        job_id = "job_system_progress"
        snapshot_id = "snapshot-system-progress"
        candidates = [
            Candidate(
                candidate_id="reflection-current-1",
                name_en="Reflection One",
                display_name="Reflection One",
                category="employee",
                target_company="Reflection AI",
                organization="Reflection AI",
                employment_status="current",
                role="Engineer",
                linkedin_url="https://www.linkedin.com/in/reflection-one/",
            ).to_record(),
        ]
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            candidates=candidates,
        )
        generation = self._register_materialization_generation(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=candidates,
        )
        registry = self._upsert_authoritative_org_registry(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
            materialization_generation_key=str(generation.get("generation_key") or ""),
            materialization_generation_sequence=int(generation.get("generation_sequence") or 0),
            materialization_watermark=str(generation.get("generation_watermark") or ""),
        )
        self.store.upsert_organization_execution_profile(
            {
                "target_company": "Reflection AI",
                "company_key": "reflectionai",
                "asset_view": "canonical_merged",
                "source_registry_id": int(registry.get("registry_id") or 0),
                "source_snapshot_id": snapshot_id,
                "source_generation_key": str(generation.get("generation_key") or ""),
                "source_generation_sequence": int(generation.get("generation_sequence") or 0),
                "source_generation_watermark": str(generation.get("generation_watermark") or ""),
                "status": "ready",
                "org_scale_band": "small",
                "default_acquisition_mode": "full_company_roster",
                "prefer_delta_from_baseline": False,
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "provider_search",
                "baseline_candidate_count": 1,
                "current_lane_effective_candidate_count": 1,
                "former_lane_effective_candidate_count": 0,
                "completeness_score": 100.0,
                "completeness_band": "high",
                "profile_detail_ratio": 1.0,
                "summary": {"source_snapshot_id": snapshot_id},
            }
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={
                "message": "Running acquisition",
                "pre_retrieval_refresh": {
                    "status": "completed",
                    "snapshot_id": "snapshot-system-progress",
                    "search_seed_worker_count": 1,
                    "harvest_prefetch_worker_count": 0,
                },
                "background_reconcile": {
                    "search_seed": {
                        "status": "inline_refreshed",
                        "applied_worker_count": 1,
                        "added_entry_count": 2,
                    }
                },
            },
        )
        self.store.record_cloud_asset_operation(
            operation_type="import_bundle",
            bundle_kind="company_snapshot",
            bundle_id="reflection_bundle",
            status="completed",
            scoped_companies=["Reflection AI"],
            scoped_snapshot_id=snapshot_id,
            summary={"imported_snapshot_id": snapshot_id},
        )
        self.store.mark_linkedin_profile_registry_queued("https://www.linkedin.com/in/demo-profile")
        self.store.record_linkedin_profile_registry_event(
            "https://www.linkedin.com/in/demo-profile",
            event_type="lookup_attempt",
            event_status="requested",
            detail="registry lookup",
        )
        bundle_dir = self.settings.runtime_dir / "asset_exports" / "bundle_demo"
        bundle_dir.mkdir(parents=True, exist_ok=True)
        (bundle_dir / "upload_progress.json").write_text(
            json.dumps(
                {
                    "status": "running",
                    "bundle_kind": "company_snapshot",
                    "bundle_id": "bundle_demo",
                    "requested_file_count": 3,
                    "completed_file_count": 1,
                    "remaining_file_count": 2,
                    "completion_ratio": 0.3333,
                    "updated_at": "2026-04-11T01:00:00+00:00",
                    "transfer_mode": "archive",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        object_sync_dir = self.settings.runtime_dir / "object_sync"
        object_sync_dir.mkdir(parents=True, exist_ok=True)
        (object_sync_dir / "bundle_index.json").write_text(
            json.dumps(
                {
                    "updated_at": "2026-04-11T01:05:00+00:00",
                    "bundles": [{"bundle_kind": "company_snapshot", "bundle_id": "bundle_demo"}],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        progress = self.orchestrator.get_system_progress(
            {
                "active_limit": 5,
                "object_sync_limit": 5,
                "profile_registry_lookback_hours": 24,
                "cloud_asset_operation_limit": 5,
                "target_company": "Reflection AI",
                "force_refresh": True,
            }
        )

        self.assertIn("runtime", progress)
        self.assertEqual(progress["workflow_jobs"]["count"], 1)
        self.assertEqual(progress["workflow_jobs"]["items"][0]["job_id"], job_id)
        self.assertEqual(
            int(progress["workflow_jobs"]["items"][0]["refresh_metrics"]["pre_retrieval_refresh_count"] or 0),
            1,
        )
        self.assertEqual(
            int(
                progress["workflow_jobs"]["items"][0]["refresh_metrics"]["background_search_seed_reconcile_count"] or 0
            ),
            1,
        )
        self.assertEqual(
            str(progress["workflow_jobs"]["items"][0]["pre_retrieval_refresh"]["snapshot_id"] or ""),
            "snapshot-system-progress",
        )
        self.assertEqual(progress["profile_registry"]["lookup_attempts"], 1)
        self.assertEqual(progress["object_sync"]["tracked_bundle_count"], 1)
        self.assertEqual(progress["object_sync"]["active_transfer_count"], 1)
        self.assertEqual(progress["object_sync"]["recent_transfers"][0]["bundle_id"], "bundle_demo")
        self.assertEqual(progress["cloud_asset_operations"]["count"], 1)
        self.assertEqual(progress["cloud_asset_operations"]["items"][0]["bundle_id"], "reflection_bundle")
        self.assertEqual(
            str(progress["company_asset"]["authoritative_registry"]["materialization_watermark"] or ""),
            str(generation.get("generation_watermark") or ""),
        )
        self.assertEqual(
            str(progress["company_asset"]["execution_profile"]["source_generation_watermark"] or ""),
            str(generation.get("generation_watermark") or ""),
        )

    def test_get_worker_daemon_status_can_aggregate_job_runtime_controls(self) -> None:
        job_id = "job_daemon_status_runtime_controls"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={
                "runtime_controls": {
                    "shared_recovery": {
                        "status": "started",
                        "service_name": "worker-recovery-daemon",
                        "scope": "shared",
                    },
                    "job_recovery": {
                        "status": "started",
                        "service_name": f"job-recovery-{job_id}",
                        "scope": "job_scoped",
                    },
                }
            },
        )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.read_service_status",
            side_effect=[
                {"service_name": "worker-recovery-daemon", "status": "running", "lock_status": "locked"},
                {"service_name": f"job-recovery-{job_id}", "status": "running", "lock_status": "locked"},
            ],
        ):
            status = self.orchestrator.get_worker_daemon_status({"job_id": job_id})

        self.assertEqual(status["status"], "ok")
        self.assertEqual(status["job_id"], job_id)
        self.assertEqual(status["recovery_services"]["shared"]["service_name"], "worker-recovery-daemon")
        self.assertEqual(status["recovery_services"]["job_scoped"]["service_name"], f"job-recovery-{job_id}")

    def test_run_workflow_blocking_triggers_job_recovery_when_acquisition_is_blocked(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )

        with (
            unittest.mock.patch.object(self.orchestrator, "_run_workflow") as mocked_run_workflow,
            unittest.mock.patch.object(self.orchestrator, "run_queued_workflow") as mocked_run_queued,
        ):

            def _mark_blocked(job_id: str, request: JobRequest, plan: object) -> None:
                self.store.save_job(
                    job_id=job_id,
                    job_type="workflow",
                    status="blocked",
                    stage="acquiring",
                    request_payload=request.to_record(),
                    plan_payload=plan.to_record() if hasattr(plan, "to_record") else {},
                    summary_payload={
                        "blocked_task": "acquire_full_roster",
                        "message": "queued_background_harvest",
                    },
                )

            mocked_run_workflow.side_effect = _mark_blocked
            mocked_run_queued.return_value = {"status": "blocked", "stage": "acquiring"}
            snapshot = self.orchestrator.run_workflow_blocking(
                {"plan_review_id": review_id, "job_recovery_poll_seconds": 0.1, "job_recovery_max_ticks": 3}
            )

        self.assertIn(snapshot["job"]["status"], {"blocked", "completed"})
        mocked_run_queued.assert_called_once()
        call_args, call_kwargs = mocked_run_queued.call_args
        self.assertEqual(call_args, (snapshot["job"]["job_id"],))
        self.assertEqual(call_kwargs, {})

    def test_run_worker_daemon_service_job_scoped_stops_after_terminal_job(self) -> None:
        job_id = "job_terminal_job_scoped_daemon"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "done"},
        )

        result = self.orchestrator.run_worker_daemon_service(
            {
                "service_name": f"job-recovery-{job_id}",
                "job_id": job_id,
                "job_scoped": True,
                "poll_seconds": 0.1,
                "max_ticks": 5,
                "workflow_resume_stale_after_seconds": 0,
                "workflow_queue_resume_stale_after_seconds": 0,
            }
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["scope"], "job_scoped")
        self.assertEqual(result["job_id"], job_id)
        self.assertEqual(result["service"]["status"], "stopped")
        self.assertEqual(int(result["service"]["tick"] or 0), 1)
        callback_payload = dict(result["service"].get("callback_payload") or {})
        self.assertEqual(callback_payload.get("job_id"), job_id)
        self.assertFalse(bool(callback_payload.get("workflow_resume_explicit_job")))
        self.assertEqual(int(callback_payload.get("workflow_resume_stale_after_seconds")), 0)
        self.assertEqual(int(callback_payload.get("workflow_queue_resume_stale_after_seconds")), 0)

    def test_ensure_job_scoped_recovery_starts_sidecar_process(self) -> None:
        captured: dict[str, object] = {}

        class FakeProcess:
            pid = 24680

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
                    {
                        "service_name": "job-recovery-job_sidecar_spawn",
                        "status": "not_started",
                        "lock_status": "missing",
                    },
                    {"service_name": "job-recovery-job_sidecar_spawn", "status": "running", "lock_status": "locked"},
                ],
            ),
        ):
            result = self.orchestrator.ensure_job_scoped_recovery(
                "job_sidecar_spawn",
                {
                    "auto_job_daemon": True,
                    "job_recovery_workflow_resume_stale_after_seconds": 3,
                    "job_recovery_workflow_queue_resume_stale_after_seconds": 4,
                },
            )

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["mode"], "sidecar")
        self.assertEqual(result["pid"], 24680)
        self.assertEqual(result["handshake"]["status"], "ready")
        command = list(captured["command"])
        self.assertIn("run-worker-daemon-service", command)
        self.assertIn("--job-scoped", command)
        self.assertIn("--workflow-auto-resume-stale-after-seconds", command)
        self.assertIn("--workflow-queue-auto-takeover-stale-after-seconds", command)
        self.assertIn("3", command)
        self.assertIn("4", command)
        kwargs = dict(captured["kwargs"])
        self.assertEqual(kwargs["cwd"], str(self.catalog.project_root))
        self.assertEqual(kwargs["stdin"], unittest.mock.ANY)
        self.assertEqual(kwargs["stderr"], unittest.mock.ANY)

    def test_ensure_shared_recovery_starts_sidecar_process(self) -> None:
        captured: dict[str, object] = {}

        class FakeProcess:
            pid = 13579

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
                    {"service_name": "worker-recovery-daemon", "status": "not_started", "lock_status": "missing"},
                    {"service_name": "worker-recovery-daemon", "status": "running", "lock_status": "locked"},
                ],
            ),
        ):
            result = self.orchestrator.ensure_shared_recovery(
                {
                    "auto_job_daemon": True,
                    "shared_recovery_workflow_resume_stale_after_seconds": 11,
                    "shared_recovery_workflow_queue_resume_stale_after_seconds": 12,
                }
            )

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["mode"], "sidecar")
        self.assertEqual(result["scope"], "shared")
        self.assertEqual(result["pid"], 13579)
        self.assertEqual(result["handshake"]["status"], "ready")
        command = list(captured["command"])
        self.assertIn("run-worker-daemon-service", command)
        self.assertNotIn("--job-scoped", command)
        self.assertIn("--workflow-auto-resume-stale-after-seconds", command)
        self.assertIn("--workflow-queue-auto-takeover-stale-after-seconds", command)
        self.assertIn("11", command)
        self.assertIn("12", command)

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

    def test_hosted_runtime_watchdog_uses_store_sqlite_shadow_target_for_pg_sync(self) -> None:
        shadow_target = "file:sourcing-agent-shadow-test?mode=memory&cache=shared"
        self.orchestrator.store.sqlite_shadow_connect_target = unittest.mock.Mock(return_value=shadow_target)  # type: ignore[method-assign]

        with unittest.mock.patch.object(
            self.orchestrator,
            "run_worker_recovery_once",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_runtime_metrics_snapshot",
            return_value={"status": "ok", "observed_at": "2026-04-21T00:00:00Z"},
        ), unittest.mock.patch(
            "sourcing_agent.orchestrator.sync_runtime_control_plane_to_postgres",
            return_value={"status": "disabled"},
        ) as sync_mock:
            result = self.orchestrator.run_hosted_runtime_watchdog_once(
                {"control_plane_postgres_dsn": "postgresql://demo/demo"}
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(sync_mock.call_args.kwargs["sqlite_path"], shadow_target)

    def test_read_service_status_marks_process_not_alive_as_stale(self) -> None:
        runtime_dir = Path(self.tempdir.name) / "runtime_service_status"
        service_dir = runtime_dir / "services" / "stale-service"
        service_dir.mkdir(parents=True, exist_ok=True)
        lock_path = service_dir / "service.lock"
        lock_path.touch()
        status_path = service_dir / "status.json"
        status_path.write_text(
            json.dumps(
                {
                    "service_name": "stale-service",
                    "status": "running",
                    "pid": 999999,
                    "lock_path": str(lock_path),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        with unittest.mock.patch("sourcing_agent.service_daemon.os.kill", side_effect=OSError("not alive")):
            status = read_service_status(runtime_dir, "stale-service")

        self.assertEqual(status["status"], "stale")
        self.assertEqual(status["stale_reason"], "process_not_alive")
        self.assertFalse(bool(status["pid_alive"]))
        self.assertEqual(status["lock_status"], "free")

    def test_read_service_status_marks_expired_heartbeat_as_stale(self) -> None:
        runtime_dir = Path(self.tempdir.name) / "runtime_service_heartbeat"
        service_dir = runtime_dir / "services" / "heartbeat-service"
        service_dir.mkdir(parents=True, exist_ok=True)
        lock_path = service_dir / "service.lock"
        lock_path.touch()
        status_path = service_dir / "status.json"
        status_path.write_text(
            json.dumps(
                {
                    "service_name": "heartbeat-service",
                    "status": "running",
                    "pid": 1,
                    "poll_seconds": 2.0,
                    "updated_at": "2026-01-01T00:00:00+00:00",
                    "lock_path": str(lock_path),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        with unittest.mock.patch("sourcing_agent.service_daemon.os.kill", return_value=None):
            status = read_service_status(runtime_dir, "heartbeat-service")

        self.assertEqual(status["status"], "stale")
        self.assertEqual(status["stale_reason"], "heartbeat_expired")
        self.assertTrue(bool(status["pid_alive"]))
        self.assertGreater(float(status["heartbeat_age_seconds"]), float(status["heartbeat_timeout_seconds"]))

    def test_worker_daemon_service_emits_lifecycle_logs(self) -> None:
        runtime_dir = Path(self.tempdir.name) / "runtime_worker_logs"
        stderr = io.StringIO()
        service = WorkerDaemonService(
            runtime_dir=runtime_dir,
            service_name="loggable-daemon",
            poll_seconds=0.01,
            recovery_callback=lambda payload: {  # noqa: ARG005
                "status": "completed",
                "daemon": {"recoverable_count": 0, "claimed_count": 0, "executed_count": 0, "jobs": []},
                "workflow_resume": [],
                "post_completion_reconcile": [],
            },
        )

        with contextlib.redirect_stderr(stderr):
            summary = service.run_forever(max_ticks=1)

        logs = stderr.getvalue()
        self.assertEqual(summary["status"], "stopped")
        self.assertIn('"event": "service_start"', logs)
        self.assertIn('"event": "service_tick"', logs)
        self.assertIn('"event": "service_stop"', logs)

    def test_hydrate_cached_prefetch_profiles_reuses_registry_raw(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-prefetch-cache"
        raw_dir = snapshot_dir / "harvest_profiles"
        raw_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/cached-infra/"
        raw_path = raw_dir / "cached_infra.json"
        raw_path.write_text(
            json.dumps(
                {
                    "_harvest_request": {"profile_url": profile_url},
                    "item": {
                        "firstName": "Cached",
                        "lastName": "Infra",
                        "linkedinUrl": profile_url,
                        "headline": "Infrastructure Engineer",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.mark_linkedin_profile_registry_fetched(
            profile_url,
            raw_path=str(raw_path),
            source_jobs=["job-registry-bootstrap"],
            snapshot_dir=str(snapshot_dir),
        )

        prefetched = self.acquisition_engine.multi_source_enricher._hydrate_cached_prefetch_profiles(
            [profile_url],
            snapshot_dir,
            source_jobs=["job-registry-reuse"],
        )

        self.assertIn(profile_url, prefetched)
        self.assertEqual(prefetched[profile_url]["parsed"]["full_name"], "Cached Infra")
        registry_entry = self.store.get_linkedin_profile_registry(profile_url) or {}
        self.assertEqual(str(registry_entry.get("status") or ""), "fetched")
        self.assertEqual(str(registry_entry.get("last_raw_path") or ""), str(raw_path))

    def test_ensure_job_scoped_recovery_bootstraps_when_sidecar_not_ready(self) -> None:
        class FakeProcess:
            pid = 86420

            def poll(self):  # type: ignore[no-untyped-def]
                return None

        with (
            unittest.mock.patch(
                "sourcing_agent.process_supervision.subprocess.Popen",
                return_value=FakeProcess(),
            ),
            unittest.mock.patch(
                "sourcing_agent.process_supervision.time.sleep",
                return_value=None,
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                return_value={
                    "service_name": "job-recovery-job_sidecar_deferred",
                    "status": "not_started",
                    "lock_status": "missing",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_wait_for_recovery_service_ready",
                return_value={
                    "status": "timeout_process_alive",
                    "service_status": {
                        "service_name": "job-recovery-job_sidecar_deferred",
                        "status": "not_started",
                        "lock_status": "missing",
                    },
                    "pid": 86420,
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "run_worker_recovery_once",
                return_value={"status": "completed", "workflow_resume": []},
            ) as bootstrap_mock,
        ):
            result = self.orchestrator.ensure_job_scoped_recovery(
                "job_sidecar_deferred",
                {
                    "auto_job_daemon": True,
                    "job_recovery_startup_timeout_seconds": 0.2,
                    "job_recovery_startup_poll_seconds": 0.1,
                },
            )

        self.assertEqual(result["status"], "started_deferred")
        self.assertEqual(result["handshake"]["status"], "timeout_process_alive")
        bootstrap_mock.assert_called_once()
        bootstrap_payload = dict(bootstrap_mock.call_args[0][0] or {})
        self.assertEqual(bootstrap_payload["job_id"], "job_sidecar_deferred")
        self.assertFalse(bool(bootstrap_payload["workflow_resume_explicit_job"]))

    def test_run_worker_recovery_once_can_skip_immediate_explicit_workflow_resume(self) -> None:
        job_id = "job_skip_explicit_resume"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_resume_queued_workflow_if_ready",
            side_effect=AssertionError("explicit queued workflow takeover should stay stale-gated"),
        ):
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": job_id,
                    "workflow_resume_explicit_job": False,
                    "workflow_auto_resume_enabled": False,
                    "workflow_queue_auto_takeover_enabled": False,
                }
            )

        self.assertEqual(recovery["status"], "completed")
        self.assertEqual(list(recovery.get("workflow_resume") or []), [])

    def test_run_worker_recovery_once_job_scoped_stale_scan_ignores_unrelated_jobs(self) -> None:
        scoped_job_id = "job_scoped_resume"
        unrelated_job_id = "job_unrelated_queued"
        self.store.save_job(
            job_id=scoped_job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Reflection AI"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )
        self.store.save_job(
            job_id=unrelated_job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload={"target_company": "Other Co"},
            plan_payload={},
            summary_payload={"message": "queued"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_resume_queued_workflow_if_ready",
            side_effect=lambda job_id, assume_lock=False: {  # type: ignore[no-untyped-def]
                "job_id": job_id,
                "status": "resumed",
                "assume_lock": assume_lock,
            },
        ) as resume_queued:
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "job_id": scoped_job_id,
                    "workflow_resume_explicit_job": False,
                    "workflow_auto_resume_enabled": False,
                    "workflow_queue_auto_takeover_enabled": True,
                    "workflow_queue_resume_stale_after_seconds": 0,
                    "workflow_stale_scope_job_id": scoped_job_id,
                }
            )

        self.assertEqual(recovery["status"], "completed")
        workflow_resume = list(recovery.get("workflow_resume") or [])
        self.assertEqual(len(workflow_resume), 1)
        self.assertEqual(workflow_resume[0]["job_id"], scoped_job_id)
        resume_queued.assert_called_once_with(scoped_job_id)

    def test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_id = "20260408T120000"
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        baseline_candidate = Candidate(
            candidate_id="acme_1",
            name_en="Alex Builder",
            display_name="Alex Builder",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            focus_areas="",
            source_dataset="baseline_snapshot",
            linkedin_url="https://www.linkedin.com/in/alex-builder/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [baseline_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.upsert_candidate(baseline_candidate)

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 Acme 偏 GPU systems 的 current researcher",
                "target_company": "Acme",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["GPU systems"],
                "top_k": 3,
            }
        )
        request = JobRequest.from_payload(dict(plan_result["request"]))
        job_id = "job_completed_reconcile"
        initial_artifact = self.orchestrator._execute_retrieval(
            job_id,
            request,
            dict(plan_result["plan"]),
            job_type="workflow",
            runtime_policy={"workflow_snapshot_id": snapshot_id, "summary_mode": "deterministic"},
        )
        self.assertEqual(initial_artifact["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertTrue(initial_artifact["summary"]["candidate_source"]["source_path"].endswith("manifest.json"))
        initial_results = self.orchestrator.get_job_results(job_id)
        assert initial_results is not None
        self.assertEqual(initial_results["job"]["summary"]["returned_matches"], 1)
        self.assertTrue(initial_results["asset_population"]["available"])
        self.assertEqual(len(initial_results["asset_population"]["candidates"]), 1)
        self.assertEqual(initial_results["asset_population"]["candidates"][0].get("focus_areas", ""), "")

        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=dict(plan_result["plan"]),
            runtime_mode="workflow",
            lane_id="exploration_specialist",
            worker_key=baseline_candidate.candidate_id,
            stage="enriching",
            span_name="explore_candidate:Alex Builder",
            budget_payload={"max_queries": 7},
            input_payload={
                "candidate_id": baseline_candidate.candidate_id,
                "display_name": baseline_candidate.display_name,
                "candidate": baseline_candidate.to_record(),
            },
            metadata={
                "target_company": "Acme",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": dict(plan_result["plan"]),
                "runtime_mode": "workflow",
            },
            handoff_from_lane="enrichment_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_handle,
            status="running",
            checkpoint_payload={"stage": "waiting_remote_search"},
            output_payload={"summary": {"status": "queued"}},
        )

        original_explore = self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate

        def _complete_background_exploration(**kwargs):
            candidate = kwargs["candidate"]
            enriched_candidate = Candidate(
                **{
                    **candidate.to_record(),
                    "focus_areas": "GPU systems",
                    "notes": "Exploration: validated GPU systems work",
                    "metadata": {
                        **dict(candidate.metadata or {}),
                        "exploration_validated_summaries": ["Validated Acme GPU systems affiliation."],
                    },
                }
            )
            evidence = [
                EvidenceRecord(
                    evidence_id=make_evidence_id(
                        candidate.candidate_id, "exploration_summary", "Acme GPU systems", "https://example.com/acme"
                    ),
                    candidate_id=candidate.candidate_id,
                    source_type="exploration_summary",
                    title="Acme GPU systems",
                    url="https://example.com/acme",
                    summary="Validated Acme GPU systems affiliation.",
                    source_dataset="background_exploration",
                    source_path=str(snapshot_dir / "exploration" / candidate.candidate_id),
                    metadata={"provider": "test"},
                )
            ]
            worker = self.store.get_agent_worker(
                job_id=kwargs["job_id"],
                lane_id="exploration_specialist",
                worker_key=candidate.candidate_id,
            )
            if worker is not None:
                self.store.complete_agent_worker(
                    int(worker["worker_id"]),
                    status="completed",
                    checkpoint_payload={"stage": "completed", "completed_queries": ["1"]},
                    output_payload={
                        "candidate": enriched_candidate.to_record(),
                        "evidence": [item.to_record() for item in evidence],
                        "summary": {"candidate_id": candidate.candidate_id, "status": "completed"},
                        "errors": [],
                    },
                )
            return {
                "worker_status": "completed",
                "candidate": enriched_candidate,
                "evidence": evidence,
                "summary": {"candidate_id": candidate.candidate_id, "status": "completed"},
                "errors": [],
            }

        self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate = (
            _complete_background_exploration
        )
        try:
            recovery = self.orchestrator.run_worker_recovery_once({"job_id": job_id, "total_limit": 1})
        finally:
            self.acquisition_engine.multi_source_enricher.exploratory_enricher._explore_candidate = original_explore

        self.assertEqual(recovery["daemon"]["executed_count"], 1)
        self.assertEqual(recovery["post_completion_reconcile"][0]["status"], "reconciled")
        refreshed = self.orchestrator.get_job_results(job_id)
        assert refreshed is not None
        self.assertEqual(refreshed["job"]["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        self.assertTrue(refreshed["job"]["summary"]["candidate_source"]["source_path"].endswith("manifest.json"))
        self.assertEqual(refreshed["job"]["summary"]["background_reconcile"]["applied_worker_count"], 1)
        self.assertEqual(refreshed["job"]["summary"]["returned_matches"], 1)
        self.assertTrue(refreshed["asset_population"]["available"])
        self.assertEqual(refreshed["asset_population"]["candidates"][0].get("focus_areas"), "GPU systems")
        self.assertEqual(
            self.store.list_evidence(baseline_candidate.candidate_id)[0]["source_type"],
            "exploration_summary",
        )

    def test_acquire_search_seed_pool_allows_baseline_when_background_queue_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-queued"
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        queued_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-queued",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "queued_lead",
                    "full_name": "Queued Lead",
                    "source_type": "harvest_profile_search",
                }
            ],
            query_summaries=[
                {
                    "query": "Thinking Machines Lab former employee",
                    "status": "queued",
                }
            ],
            accounts_used=[],
            errors=[],
            stop_reason="queued_background_search",
            summary_path=summary_path,
        )

        self.acquisition_engine.search_seed_acquirer.discover = lambda *args, **kwargs: queued_snapshot
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire search seed pool",
            description="Acquire former employee leads",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "former_employee_search",
                "search_seed_queries": ["Thinking Machines Lab former employee"],
                "cost_policy": {},
                "employment_statuses": ["former"],
            },
        )
        execution = self.acquisition_engine._acquire_search_seed_pool(
            task,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "job_id": "job_queued",
                "plan_payload": {},
                "runtime_mode": "workflow",
            },
            JobRequest(
                raw_user_request="Find former Thinking Machines Lab members",
                target_company="Thinking Machines Lab",
                categories=["former_employee"],
                employment_statuses=["former"],
            ),
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["queued_query_count"], 1)
        self.assertEqual(execution.state_updates["search_seed_snapshot"].stop_reason, "queued_background_search")
        self.assertEqual(execution.payload["entry_count"], 1)

    def test_acquire_search_seed_pool_queues_profile_prefetch_immediately_for_recovered_entries(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-seed-prefetch"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        summary_path = discovery_dir / "summary.json"
        summary_path.write_text("{}", encoding="utf-8")
        entries_path = discovery_dir / "entries.json"
        queued_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "reflection-infra-01",
                    "full_name": "Infra Builder",
                    "headline": "Infrastructure Engineer",
                    "source_type": "web_search",
                    "source_query": "Reflection AI infra",
                    "profile_url": "https://www.linkedin.com/in/infra-builder/",
                }
            ],
            query_summaries=[{"query": "Reflection AI infra", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            summary_path=summary_path,
            entries_path=entries_path,
        )

        self.acquisition_engine.search_seed_acquirer.discover = lambda *args, **kwargs: queued_snapshot
        prefetch_summary = {
            "status": "queued",
            "requested_url_count": 1,
            "dispatched_url_count": 1,
            "cached_profile_count": 0,
            "queued_worker_count": 1,
            "queued_urls": ["https://www.linkedin.com/in/infra-builder/"],
            "failed_urls": [],
            "summary_paths": [str(snapshot_dir / "harvest_profiles" / "queued.json")],
            "errors": [],
        }
        task = AcquisitionTask(
            task_id="search-seed",
            task_type="acquire_search_seed_pool",
            title="Acquire search seeds",
            description="Acquire search seeds",
            status="ready",
            metadata={
                "strategy_type": "scoped_search_roster",
                "search_seed_queries": ["Reflection AI infra"],
                "employment_statuses": ["current"],
            },
        )
        with unittest.mock.patch.object(
            self.acquisition_engine.multi_source_enricher,
            "queue_background_profile_prefetch",
            return_value=prefetch_summary,
        ) as prefetch_mock:
            execution = self.acquisition_engine._acquire_search_seed_pool(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_search_seed_prefetch",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra members",
                    target_company="Reflection AI",
                    categories=["employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(
            dict(execution.payload.get("profile_prefetch") or {}).get("queued_worker_count"),
            1,
        )
        prefetch_mock.assert_called_once()

    def test_acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-seed-incremental-prefetch"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        summary_path = discovery_dir / "summary.json"
        summary_path.write_text("{}", encoding="utf-8")
        entries_path = discovery_dir / "entries.json"

        def _fake_discover(*args, **kwargs):  # noqa: ANN002, ANN003
            on_incremental = kwargs.get("on_incremental_query_result")
            if callable(on_incremental):
                on_incremental(
                    {
                        "query": "Reflection AI infra",
                        "raw_path": str(discovery_dir / "web_query_01.json"),
                        "entries": [
                            {
                                "seed_key": "infra-01",
                                "full_name": "Infra Builder",
                                "headline": "Infrastructure Engineer",
                                "source_type": "web_search",
                                "source_query": "Reflection AI infra",
                                "profile_url": "https://www.linkedin.com/in/infra-builder/",
                            }
                        ],
                    }
                )
                on_incremental(
                    {
                        "query": "Reflection AI platform",
                        "raw_path": str(discovery_dir / "web_query_02.json"),
                        "entries": [
                            {
                                "seed_key": "infra-01",
                                "full_name": "Infra Builder",
                                "headline": "Infrastructure Engineer",
                                "source_type": "web_search",
                                "source_query": "Reflection AI infra",
                                "profile_url": "https://www.linkedin.com/in/infra-builder/",
                            },
                            {
                                "seed_key": "platform-01",
                                "full_name": "Platform Builder",
                                "headline": "Platform Engineer",
                                "source_type": "web_search",
                                "source_query": "Reflection AI platform",
                                "profile_url": "https://www.linkedin.com/in/platform-builder/",
                            },
                        ],
                    }
                )
            return SearchSeedSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company="Reflection AI",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                entries=[
                    {
                        "seed_key": "infra-01",
                        "full_name": "Infra Builder",
                        "headline": "Infrastructure Engineer",
                        "source_type": "web_search",
                        "source_query": "Reflection AI infra",
                        "profile_url": "https://www.linkedin.com/in/infra-builder/",
                    },
                    {
                        "seed_key": "platform-01",
                        "full_name": "Platform Builder",
                        "headline": "Platform Engineer",
                        "source_type": "web_search",
                        "source_query": "Reflection AI platform",
                        "profile_url": "https://www.linkedin.com/in/platform-builder/",
                    },
                ],
                query_summaries=[
                    {"query": "Reflection AI infra", "status": "completed"},
                    {"query": "Reflection AI platform", "status": "completed"},
                ],
                accounts_used=[],
                errors=[],
                stop_reason="completed",
                summary_path=summary_path,
                entries_path=entries_path,
            )

        self.acquisition_engine.search_seed_acquirer.discover = _fake_discover
        dispatched_url_groups: list[list[str]] = []

        def _capture_prefetch(*, candidates, **kwargs):  # noqa: ANN002, ANN003
            dispatched_urls = sorted(
                {
                    str(candidate.linkedin_url or "").strip()
                    for candidate in list(candidates or [])
                    if str(candidate.linkedin_url or "").strip()
                }
            )
            dispatched_url_groups.append(dispatched_urls)
            return {
                "status": "queued",
                "requested_url_count": len(dispatched_urls),
                "dispatched_url_count": len(dispatched_urls),
                "cached_profile_count": 0,
                "queued_worker_count": 1 if dispatched_urls else 0,
                "queued_urls": list(dispatched_urls),
                "failed_urls": [],
                "summary_paths": [str(snapshot_dir / "harvest_profiles" / f"queued_{len(dispatched_url_groups)}.json")],
                "errors": [],
            }

        task = AcquisitionTask(
            task_id="search-seed",
            task_type="acquire_search_seed_pool",
            title="Acquire search seeds",
            description="Acquire search seeds",
            status="ready",
            metadata={
                "strategy_type": "scoped_search_roster",
                "search_seed_queries": ["Reflection AI infra", "Reflection AI platform"],
                "employment_statuses": ["current"],
            },
        )
        with unittest.mock.patch.object(
            self.acquisition_engine.multi_source_enricher,
            "queue_background_profile_prefetch",
            side_effect=_capture_prefetch,
        ) as prefetch_mock:
            execution = self.acquisition_engine._acquire_search_seed_pool(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_search_seed_incremental_prefetch",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra and platform members",
                    target_company="Reflection AI",
                    categories=["employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(prefetch_mock.call_count, 2)
        self.assertEqual(
            dispatched_url_groups,
            [
                ["https://www.linkedin.com/in/infra-builder/"],
                ["https://www.linkedin.com/in/platform-builder/"],
            ],
        )
        self.assertEqual(
            dict(execution.payload.get("profile_prefetch") or {}).get("queued_worker_count"),
            2,
        )
        self.assertEqual(
            dict(execution.payload.get("profile_prefetch") or {}).get("requested_url_count"),
            2,
        )

    def test_acquire_search_seed_pool_still_blocks_when_background_queue_has_no_entries(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-queued-empty"
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        queued_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-queued-empty",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[
                {
                    "query": "Thinking Machines Lab former employee",
                    "status": "queued",
                }
            ],
            accounts_used=[],
            errors=[],
            stop_reason="queued_background_search",
            summary_path=summary_path,
        )

        self.acquisition_engine.search_seed_acquirer.discover = lambda *args, **kwargs: queued_snapshot
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire search seed pool",
            description="Acquire former employee leads",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "former_employee_search",
                "search_seed_queries": ["Thinking Machines Lab former employee"],
                "cost_policy": {},
                "employment_statuses": ["former"],
            },
        )
        execution = self.acquisition_engine._acquire_search_seed_pool(
            task,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "job_id": "job_queued_empty",
                "plan_payload": {},
                "runtime_mode": "workflow",
            },
            JobRequest(
                raw_user_request="Find former Thinking Machines Lab members",
                target_company="Thinking Machines Lab",
                categories=["former_employee"],
                employment_statuses=["former"],
            ),
        )

        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_query_count"], 1)

    def test_acquire_search_seed_pool_passes_effective_request_payload_from_intent_view(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "snapshot-effective-request"
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        captured: dict[str, object] = {}

        def _fake_discover(*args, **kwargs):  # noqa: ANN002, ANN003
            captured["request_payload"] = dict(kwargs.get("request_payload") or {})
            return SearchSeedSnapshot(
                snapshot_id="snapshot-effective-request",
                target_company="Google",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                entries=[
                    {
                        "seed_key": "lead_1",
                        "full_name": "Gemini PM",
                        "source_type": "harvest_profile_search",
                    }
                ],
                query_summaries=[{"query": "Gemini", "status": "completed"}],
                accounts_used=[],
                errors=[],
                stop_reason="completed",
                summary_path=summary_path,
            )

        self.acquisition_engine.search_seed_acquirer.discover = _fake_discover  # type: ignore[method-assign]
        task = AcquisitionTask(
            task_id="acquire-search-seed",
            task_type="acquire_search_seed_pool",
            title="Acquire search seed pool",
            description="Acquire scoped search leads",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "scoped_search_roster",
                "search_seed_queries": ["Gemini"],
                "search_query_bundles": [],
                "cost_policy": {},
                "employment_statuses": ["current", "former"],
            },
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找 Gemini 的产品经理",
                "query": "Gemini product manager",
                "target_company": "WrongCo",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        execution = self.acquisition_engine._acquire_search_seed_pool(
            task,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "job_id": "job_effective_request",
                "plan_payload": {},
                "runtime_mode": "workflow",
            },
            request,
        )

        self.assertEqual(execution.status, "completed")
        request_payload = dict(captured.get("request_payload") or {})
        self.assertEqual(request_payload.get("target_company"), "Google")
        self.assertEqual(request_payload.get("organization_keywords"), ["Google DeepMind", "Gemini"])
        self.assertEqual(request_payload.get("keywords"), ["Gemini"])

    def test_acquire_search_seed_pool_prefers_task_intent_view_execution_fields_over_stale_metadata(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "snapshot-intent-view-preferred"
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        captured: dict[str, object] = {}

        def _fake_discover(*args, **kwargs):  # noqa: ANN002, ANN003
            captured["search_seed_queries"] = list(kwargs.get("search_seed_queries") or [])
            captured["filter_hints"] = dict(kwargs.get("filter_hints") or {})
            captured["cost_policy"] = dict(kwargs.get("cost_policy") or {})
            captured["intent_view"] = dict(kwargs.get("intent_view") or {})
            return SearchSeedSnapshot(
                snapshot_id="snapshot-intent-view-preferred",
                target_company="Google",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                entries=[
                    {
                        "seed_key": "lead_1",
                        "full_name": "Former Gemini PM",
                        "source_type": "harvest_profile_search",
                    }
                ],
                query_summaries=[{"query": "Gemini", "status": "completed"}],
                accounts_used=[],
                errors=[],
                stop_reason="completed",
                summary_path=summary_path,
            )

        self.acquisition_engine.search_seed_acquirer.discover = _fake_discover  # type: ignore[method-assign]
        task = AcquisitionTask(
            task_id="acquire-search-seed",
            task_type="acquire_search_seed_pool",
            title="Acquire search seed pool",
            description="Acquire scoped search leads",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "scoped_search_roster",
                "search_seed_queries": ["Wrong query"],
                "search_query_bundles": [],
                "filter_hints": {
                    "current_companies": ["WrongCo"],
                    "keywords": ["Wrong"],
                },
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
                "employment_statuses": ["current"],
                "search_channel_order": ["web_search"],
                "intent_view": {
                    "target_company": "Google",
                    "strategy_type": "former_employee_search",
                    "employment_statuses": ["former"],
                    "filter_hints": {
                        "past_companies": ["https://www.linkedin.com/company/google/"],
                        "keywords": ["Gemini"],
                    },
                    "search_seed_queries": ["Gemini"],
                    "search_query_bundles": [
                        {
                            "bundle_id": "former_gemini",
                            "source_family": "linkedin_people_search",
                            "execution_mode": "paid_fallback",
                            "queries": ["Gemini"],
                        }
                    ],
                    "cost_policy": {"provider_people_search_mode": "primary_only"},
                    "search_channel_order": ["harvest_profile_search"],
                },
            },
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找 Google Gemini 的前员工",
                "target_company": "Google",
            }
        )

        execution = self.acquisition_engine._acquire_search_seed_pool(
            task,
            {
                "company_identity": identity,
                "snapshot_dir": snapshot_dir,
                "job_id": "job_intent_view_preferred",
                "plan_payload": {},
                "runtime_mode": "workflow",
            },
            request,
        )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(captured.get("search_seed_queries"), ["Gemini"])
        self.assertEqual(
            dict(captured.get("filter_hints") or {}).get("past_companies"),
            ["https://www.linkedin.com/company/google/"],
        )
        self.assertEqual(
            dict(captured.get("cost_policy") or {}).get("provider_people_search_mode"),
            "primary_only",
        )
        self.assertEqual(
            dict(captured.get("intent_view") or {}).get("search_seed_queries"),
            ["Gemini"],
        )

    def test_task_execution_view_preserves_explicit_empty_intent_lists_over_stale_metadata(self) -> None:
        task = AcquisitionTask(
            task_id="task-empty-intent-lists",
            task_type="acquire_search_seed_pool",
            title="Acquire search seed pool",
            description="test",
            status="ready",
            metadata={
                "search_seed_queries": ["Wrong query"],
                "search_query_bundles": [
                    {
                        "bundle_id": "wrong_bundle",
                        "queries": ["Wrong query"],
                    }
                ],
                "company_employee_shards": [
                    {
                        "shard_id": "wrong-shard",
                        "company_filters": {"search_query": "Wrong query"},
                    }
                ],
                "intent_view": {
                    "search_seed_queries": [],
                    "search_query_bundles": [],
                    "company_employee_shards": [],
                },
            },
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找 Google 的人",
                "target_company": "Google",
            }
        )

        self.assertEqual(self.acquisition_engine._effective_search_seed_queries(task, request), [])
        self.assertEqual(self.acquisition_engine._task_search_query_bundles(task, request), [])
        self.assertEqual(self.acquisition_engine._effective_company_employee_shards(task, request), [])

    def test_task_execution_int_falls_back_when_field_is_non_scalar(self) -> None:
        task = AcquisitionTask(
            task_id="task-int-default",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="test",
            status="ready",
            metadata={
                "intent_view": {
                    "max_pages": [1, 2, 3],
                }
            },
        )
        request = JobRequest.from_payload({"target_company": "Reflection AI"})

        self.assertEqual(
            self.acquisition_engine._task_execution_int(task, request, "max_pages", default=10),
            10,
        )

    def test_enrich_profiles_uses_effective_request_payload_and_target_company(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "snapshot-effective-enrich"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        roster_summary_path = snapshot_dir / "roster_summary.json"
        for path, payload in [
            (merged_path, "[]"),
            (visible_path, "[]"),
            (headless_path, "[]"),
            (roster_summary_path, "{}"),
        ]:
            path.write_text(payload, encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Google",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[],
            visible_entries=[],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=roster_summary_path,
        )
        roster_candidate = Candidate(
            candidate_id="roster_1",
            name_en="Gemini PM",
            display_name="Gemini PM",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Product Manager",
            linkedin_url="https://www.linkedin.com/in/gemini-pm/",
            source_dataset="google_linkedin_company_people",
        )
        roster_evidence = EvidenceRecord(
            evidence_id="ev_roster_1",
            candidate_id="roster_1",
            source_type="linkedin_company_people",
            title="Roster row",
            url="https://www.linkedin.com/in/gemini-pm/",
            summary="Current roster row",
            source_dataset="google_linkedin_company_people",
            source_path=str(roster_summary_path),
        )
        captured: dict[str, object] = {}

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, request_arg, *args, **kwargs):  # noqa: ANN001, ARG001
            captured["request"] = request_arg
            captured["request_payload"] = dict(kwargs.get("request_payload") or {})
            return MultiSourceEnrichmentResult(
                candidates=list(candidates_arg),
                evidence=[],
            )

        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            with unittest.mock.patch(
                "sourcing_agent.acquisition.build_candidates_from_roster",
                return_value=([roster_candidate], [roster_evidence]),
            ):
                execution = self.acquisition_engine._enrich_profiles(
                    AcquisitionTask(
                        task_id="enrich-linkedin",
                        task_type="enrich_linkedin_profiles",
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
                    JobRequest.from_payload(
                        {
                            "raw_user_request": "找 Gemini 的产品经理",
                            "query": "Gemini product manager",
                            "target_company": "WrongCo",
                            "intent_axes": {
                                "population_boundary": {
                                    "categories": ["employee"],
                                    "employment_statuses": ["current", "former"],
                                },
                                "scope_boundary": {
                                    "target_company": "Google",
                                    "organization_keywords": ["Google DeepMind", "Gemini"],
                                },
                                "thematic_constraints": {
                                    "must_have_primary_role_buckets": ["product_management"],
                                    "keywords": ["Gemini"],
                                },
                            },
                        }
                    ),
                )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        request_arg = captured["request"]
        assert isinstance(request_arg, JobRequest)
        self.assertEqual(request_arg.target_company, "Google")
        request_payload = dict(captured.get("request_payload") or {})
        self.assertEqual(request_payload.get("target_company"), "Google")

    def test_run_worker_recovery_once_auto_resumes_stale_acquiring_workflow(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        stale_job_id = "job_stale_auto_resume"
        self.store.save_job(
            job_id=stale_job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request_payload,
            plan_payload=plan_payload,
            summary_payload={"message": "stale acquiring heartbeat"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_resume_acquiring_workflow_if_ready",
            return_value={"job_id": stale_job_id, "status": "resumed"},
        ) as resume_mock:
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "workflow_resume_stale_after_seconds": 0,
                    "workflow_resume_limit": 10,
                    "workflow_auto_resume_enabled": True,
                }
            )

        resume_mock.assert_any_call(stale_job_id)
        self.assertTrue(
            any(str(item.get("job_id") or "") == stale_job_id for item in list(recovery.get("workflow_resume") or []))
        )

    def test_workflow_supervisor_uses_immediate_worker_recovery_when_runner_is_dead(self) -> None:
        request_payload = {
            "raw_user_request": "Find Humans& infra members",
            "target_company": "Humans&",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
        }
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_supervisor_runner_dead"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request_payload,
            plan_payload=plan_payload,
            summary_payload={
                "runtime_controls": {
                    "workflow_runner": {
                        "status": "started",
                        "pid": 999999,
                        "process_alive": False,
                        "job_id": job_id,
                    },
                    "workflow_runner_control": {
                        "status": "started",
                        "handshake": {
                            "status": "advanced",
                            "job_status": "running",
                            "job_stage": "acquiring",
                        },
                    },
                }
            },
        )
        running_worker = {
            "worker_id": 1,
            "job_id": job_id,
            "lane_id": "acquisition_specialist",
            "worker_key": "harvest_company_employees::humansand",
            "status": "running",
            "updated_at": "2026-04-11 05:40:00",
        }
        captured_payloads: list[dict[str, object]] = []

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "run_queued_workflow",
                return_value={"job_id": job_id, "status": "skipped", "stage": "acquiring", "reason": "already_running"},
            ),
            unittest.mock.patch.object(
                self.orchestrator.agent_runtime,
                "list_workers",
                return_value=[running_worker],
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "run_worker_recovery_once",
                side_effect=lambda payload=None: (
                    captured_payloads.append(dict(payload or {}))
                    or {
                        "status": "completed",
                        "daemon": {},
                        "workflow_resume": [],
                    }
                ),
            ),
        ):
            result = self.orchestrator.run_workflow_supervisor(
                job_id, auto_job_daemon=True, max_ticks=1, poll_seconds=0.01
            )

        self.assertEqual(result["status"], "running")
        self.assertEqual(captured_payloads[-1]["stale_after_seconds"], 0)

    def test_build_worker_recovery_daemon_preserves_zero_stale_after_seconds(self) -> None:
        daemon = self.orchestrator._build_worker_recovery_daemon({"stale_after_seconds": 0})  # noqa: SLF001
        self.assertEqual(daemon.stale_after_seconds, 1)

    def test_run_workflow_blocking_uses_inline_supervisor_for_acquiring_jobs(self) -> None:
        payload = {
            "raw_user_request": "给我 Humans& 的 Infra 方向成员",
            "target_company": "Humans&",
            "keywords": ["infra"],
            "skip_plan_review": True,
        }
        captured: dict[str, object] = {}

        original_run_workflow = self.orchestrator._run_workflow

        def fake_run_workflow(job_id, request, plan):  # noqa: ARG001
            original_run_workflow(job_id, request, plan)
            job = self.store.get_job(job_id) or {}
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="blocked",
                stage="acquiring",
                request_payload=request.to_record(),
                plan_payload=plan.to_record(),
                summary_payload={
                    **dict(job.get("summary") or {}),
                    "message": "Queued remote acquisition worker.",
                    "blocked_task": "acquire_full_roster",
                },
            )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_workflow",
                side_effect=fake_run_workflow,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "run_workflow_supervisor",
                side_effect=lambda job_id, **kwargs: (
                    captured.update({"job_id": job_id, **kwargs})
                    or {
                        "job_id": job_id,
                        "status": "completed",
                        "stage": "completed",
                    }
                ),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "get_job_results",
                return_value={"job": {"status": "blocked", "stage": "acquiring"}, "events": [], "results": []},
            ),
        ):
            result = self.orchestrator.run_workflow_blocking(payload)

        self.assertEqual(result["job"]["status"], "blocked")
        self.assertEqual(captured["auto_job_daemon"], True)
        self.assertEqual(captured["poll_seconds"], 2.0)
        self.assertEqual(captured["max_ticks"], 900)

    def test_restore_acquisition_state_rehydrates_completed_harvest_queue_dataset_into_roster_snapshot(self) -> None:
        request_payload = {
            "raw_user_request": "给我 Humans& 的 Infra 方向成员",
            "target_company": "Humans&",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        self.assertEqual(str(task.metadata.get("strategy_type") or ""), "full_company_roster")

        identity = CompanyIdentity(
            requested_name="Humans&",
            canonical_name="Humans&",
            company_key="humansand",
            linkedin_slug="humansand",
            linkedin_company_url="https://www.linkedin.com/company/humansand/",
        )
        snapshot_dir = self.settings.company_assets_dir / "humansand" / "20260411T134937"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        dataset_items_path = harvest_dir / "harvest_company_employees_queue_dataset_items.json"
        dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "id": "ACwAADrFuoABrf4CdDIhNYV7PWlLMli2NmmIWPI",
                        "linkedinUrl": "https://www.linkedin.com/in/ACwAADrFuoABrf4CdDIhNYV7PWlLMli2NmmIWPI",
                        "firstName": "Ziang",
                        "lastName": "L.",
                        "summary": "Infrastructure systems for large models.",
                        "currentPositions": [
                            {
                                "companyName": "humans&",
                                "title": "Member of Technical Staff",
                                "current": True,
                            }
                        ],
                        "location": {"linkedinText": "San Francisco Bay Area"},
                        "_meta": {
                            "pagination": {
                                "totalElements": 24,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "previousElements": 0,
                                "pageSize": 25,
                            },
                            "query": {
                                "currentCompanies": ["https://www.linkedin.com/company/humansand/"],
                            },
                        },
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (harvest_dir / "harvest_company_employees_queue_summary.json").write_text(
            json.dumps(
                {
                    "logical_name": "harvest_company_employees",
                    "company_identity": identity.to_record(),
                    "status": "completed",
                    "requested_pages": 1,
                    "requested_item_limit": 25,
                    "company_filters": {},
                    "artifact_paths": {
                        "dataset_items": str(dataset_items_path),
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        empty_seed_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        empty_seed_summary_path.parent.mkdir(parents=True, exist_ok=True)
        empty_seed_entries_path = empty_seed_summary_path.parent / "entries.json"
        empty_seed_summary_path.write_text(json.dumps({}, ensure_ascii=False, indent=2), encoding="utf-8")
        empty_seed_entries_path.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")
        empty_search_seed = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Humans&",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            summary_path=empty_seed_summary_path,
            entries_path=empty_seed_entries_path,
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"search_seed_snapshot": empty_search_seed},
            )
        )

        restored = self.orchestrator._restore_acquisition_state(
            job_id="job_restore_harvest_queue_snapshot",
            request=request,
            plan=plan,
            acquisition_progress={
                "latest_state": {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": identity.to_record(),
                    "search_seed_snapshot": empty_search_seed.to_record(),
                }
            },
        )

        roster_snapshot = restored.get("roster_snapshot")
        self.assertIsInstance(roster_snapshot, CompanyRosterSnapshot)
        assert isinstance(roster_snapshot, CompanyRosterSnapshot)
        self.assertEqual(len(roster_snapshot.visible_entries), 1)
        self.assertTrue(roster_snapshot.summary_path.exists())
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(task, restored),
        )

    def test_restore_roster_snapshot_rehydrates_segmented_harvest_queue_shards(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "20260412T022230"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        shard_root = harvest_dir / "shards"
        shard_root.mkdir(parents=True, exist_ok=True)

        shards = [
            {
                "strategy_id": "adaptive_large_org_keyword_probe",
                "shard_id": "kw_veo",
                "title": "United States / Veo",
                "scope_note": "Keyword union shard",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {
                    "companies": [
                        "https://www.linkedin.com/company/google/",
                        "https://www.linkedin.com/company/deepmind/",
                    ],
                    "locations": ["United States"],
                    "function_ids": ["8", "9", "19", "24"],
                    "search_query": "Veo",
                },
            },
            {
                "strategy_id": "adaptive_large_org_keyword_probe",
                "shard_id": "kw_nanobanana",
                "title": "United States / Nano Banana",
                "scope_note": "Keyword union shard",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {
                    "companies": [
                        "https://www.linkedin.com/company/google/",
                        "https://www.linkedin.com/company/deepmind/",
                    ],
                    "locations": ["United States"],
                    "function_ids": ["8", "9", "19", "24"],
                    "search_query": "Nano Banana",
                },
            },
        ]
        (harvest_dir / "adaptive_shard_plan.json").write_text(
            json.dumps({"status": "planned", "shards": shards}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        shard_profiles = {
            "kw_veo": [
                {
                    "id": "veo_member",
                    "linkedinUrl": "https://www.linkedin.com/in/veo-member/",
                    "firstName": "Alice",
                    "lastName": "Vision",
                    "summary": "Works on Veo video generation.",
                    "currentPositions": [
                        {
                            "companyName": "Google DeepMind",
                            "title": "Research Scientist",
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
                            "currentCompanies": ["https://www.linkedin.com/company/google/"],
                            "searchQuery": "Veo",
                        },
                    },
                }
            ],
            "kw_nanobanana": [
                {
                    "id": "nanobanana_member",
                    "linkedinUrl": "https://www.linkedin.com/in/nanobanana-member/",
                    "firstName": "Bob",
                    "lastName": "Multimodal",
                    "summary": "Works on Nano Banana multimodal generation.",
                    "currentPositions": [
                        {
                            "companyName": "Google",
                            "title": "Software Engineer",
                            "current": True,
                        }
                    ],
                    "location": {"linkedinText": "Mountain View, California, United States"},
                    "_meta": {
                        "pagination": {
                            "totalElements": 1,
                            "totalPages": 1,
                            "pageNumber": 1,
                            "previousElements": 0,
                            "pageSize": 25,
                        },
                        "query": {
                            "currentCompanies": ["https://www.linkedin.com/company/google/"],
                            "searchQuery": "Nano Banana",
                        },
                    },
                }
            ],
        }
        for shard_id, entries in shard_profiles.items():
            shard_harvest_dir = shard_root / shard_id / "harvest_company_employees"
            shard_harvest_dir.mkdir(parents=True, exist_ok=True)
            dataset_items_path = shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json"
            dataset_items_path.write_text(
                json.dumps(entries, ensure_ascii=False, indent=2),
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
                        "artifact_paths": {
                            "dataset_items": str(dataset_items_path),
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        restored = self.orchestrator._restore_roster_snapshot_from_snapshot_dir(
            snapshot_dir=snapshot_dir,
            identity=identity,
        )

        self.assertIsInstance(restored, CompanyRosterSnapshot)
        assert isinstance(restored, CompanyRosterSnapshot)
        self.assertEqual(len(restored.visible_entries), 2)
        self.assertTrue(restored.summary_path.exists())
        self.assertEqual(
            {item.get("source_shard_id") for item in restored.raw_entries},
            {"kw_veo", "kw_nanobanana"},
        )

    def test_restore_roster_snapshot_rehydrates_partial_segmented_harvest_queue_shards_without_cached_reuse(
        self,
    ) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "20260412T033015"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        shard_root = harvest_dir / "shards"
        shard_root.mkdir(parents=True, exist_ok=True)

        shards = [
            {
                "strategy_id": "adaptive_large_org_keyword_probe",
                "shard_id": "kw_gemini",
                "title": "United States / Gemini",
                "scope_note": "Keyword union shard",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {
                    "companies": ["https://www.linkedin.com/company/google/"],
                    "locations": ["United States"],
                    "function_ids": ["8", "24"],
                    "search_query": "Gemini",
                },
            },
            {
                "strategy_id": "adaptive_large_org_keyword_probe",
                "shard_id": "kw_imagen",
                "title": "United States / Imagen",
                "scope_note": "Keyword union shard",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {
                    "companies": ["https://www.linkedin.com/company/google/"],
                    "locations": ["United States"],
                    "function_ids": ["8", "24"],
                    "search_query": "Imagen",
                },
            },
        ]
        (harvest_dir / "adaptive_shard_plan.json").write_text(
            json.dumps({"status": "planned", "shards": shards}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        shard_harvest_dir = shard_root / "kw_gemini" / "harvest_company_employees"
        shard_harvest_dir.mkdir(parents=True, exist_ok=True)
        dataset_items_path = shard_harvest_dir / "harvest_company_employees_queue_dataset_items.json"
        dataset_items_path.write_text(
            json.dumps(
                [
                    {
                        "id": "gemini_member",
                        "linkedinUrl": "https://www.linkedin.com/in/gemini-member/",
                        "firstName": "Grace",
                        "lastName": "Model",
                        "summary": "Works on Gemini.",
                        "currentPositions": [
                            {
                                "companyName": "Google",
                                "title": "Research Engineer",
                                "current": True,
                            }
                        ],
                        "location": {"linkedinText": "Mountain View, California, United States"},
                        "_meta": {
                            "pagination": {
                                "totalElements": 1,
                                "totalPages": 1,
                                "pageNumber": 1,
                                "previousElements": 0,
                                "pageSize": 25,
                            },
                            "query": {
                                "currentCompanies": ["https://www.linkedin.com/company/google/"],
                                "searchQuery": "Gemini",
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
                    "company_filters": shards[0]["company_filters"],
                    "artifact_paths": {"dataset_items": str(dataset_items_path)},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        restored = self.orchestrator._restore_roster_snapshot_from_snapshot_dir(
            snapshot_dir=snapshot_dir,
            identity=identity,
        )

        self.assertIsInstance(restored, CompanyRosterSnapshot)
        assert isinstance(restored, CompanyRosterSnapshot)
        self.assertEqual(len(restored.visible_entries), 1)
        summary_payload = json.loads(restored.summary_path.read_text())
        self.assertEqual(str(summary_payload.get("completion_status") or ""), "partial")
        self.assertEqual(str(summary_payload.get("stop_reason") or ""), "partial_segmented")
        self.assertIsNone(self.acquisition_engine._load_cached_roster_snapshot(identity))

    def test_full_company_roster_checkpoint_requires_roster_snapshot_not_search_seed_only(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "snapshot-search-seed-only"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        entries_payload = [
            {
                "seed_key": "former_1",
                "full_name": "Former Seed",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/former-seed/",
            }
        ]
        entries_path.write_text(json.dumps(entries_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Google",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries_payload,
            query_summaries=[{"query": "Veo", "status": "completed"}],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=summary_path,
            entries_path=entries_path,
        )
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire Google roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster"},
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"search_seed_snapshot": search_seed_snapshot},
            )
        )

        scoped_task = AcquisitionTask(
            task_id="acquire-scoped-roster",
            task_type="acquire_full_roster",
            title="Acquire scoped roster",
            description="Acquire scoped roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "scoped_search_roster"},
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                scoped_task,
                {"search_seed_snapshot": search_seed_snapshot},
            )
        )

    def test_former_search_seed_checkpoint_requires_former_scope_state(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-former-checkpoint"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")

        current_entries = [
            {
                "seed_key": "openai_current_1",
                "full_name": "Current Seed",
                "source_type": "harvest_profile_search",
                "employment_status": "current",
                "profile_url": "https://www.linkedin.com/in/openai-current-seed/",
            }
        ]
        entries_path.write_text(json.dumps(current_entries, ensure_ascii=False, indent=2), encoding="utf-8")
        current_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=current_entries,
            query_summaries=[{"query": "Coding", "status": "completed"}],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_primary",
            summary_path=summary_path,
            entries_path=entries_path,
        )
        former_task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="Acquire former-member search seeds",
            status="ready",
            blocking=True,
            metadata={"search_seed_queries": ["Coding"]},
        )

        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                former_task,
                {"search_seed_snapshot": current_seed_snapshot},
            )
        )

        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-former-current-only-candidates",
            candidates=[
                Candidate(
                    candidate_id="openai_current_only",
                    name_en="Current Only",
                    display_name="Current Only",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Engineer",
                    linkedin_url="https://www.linkedin.com/in/openai-current-only/",
                ).to_record()
            ],
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                former_task,
                {
                    "candidate_doc_path": candidate_doc_path,
                    "candidates": [
                        Candidate(
                            candidate_id="openai_current_only",
                            name_en="Current Only",
                            display_name="Current Only",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-current-only/",
                        )
                    ],
                },
            )
        )

        former_entries = [
            {
                "seed_key": "openai_former_1",
                "full_name": "Former Seed",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/openai-former-seed/",
            }
        ]
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                former_task,
                {
                    "search_seed_snapshot": SearchSeedSnapshot(
                        snapshot_id=snapshot_dir.name,
                        target_company="OpenAI",
                        company_identity=identity,
                        snapshot_dir=snapshot_dir,
                        entries=former_entries,
                        query_summaries=[{"query": "Coding", "status": "completed"}],
                        accounts_used=["harvest_profile_search"],
                        errors=[],
                        stop_reason="provider_people_search_primary",
                        summary_path=summary_path,
                        entries_path=entries_path,
                    )
                },
            )
        )

    def test_scoped_current_roster_checkpoint_requires_current_scope_state(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-current-checkpoint"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        former_entries = [
            {
                "seed_key": "openai_former_only",
                "full_name": "Former Only",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/openai-former-only/",
            }
        ]
        entries_path.write_text(json.dumps(former_entries, ensure_ascii=False, indent=2), encoding="utf-8")
        scoped_task = AcquisitionTask(
            task_id="acquire-scoped-roster",
            task_type="acquire_full_roster",
            title="Acquire current company roster",
            description="Acquire current company roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "scoped_search_roster"},
        )

        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                scoped_task,
                {
                    "search_seed_snapshot": SearchSeedSnapshot(
                        snapshot_id=snapshot_dir.name,
                        target_company="OpenAI",
                        company_identity=identity,
                        snapshot_dir=snapshot_dir,
                        entries=former_entries,
                        query_summaries=[{"query": "Coding", "status": "completed"}],
                        accounts_used=["harvest_profile_search"],
                        errors=[],
                        stop_reason="provider_people_search_primary",
                        summary_path=summary_path,
                        entries_path=entries_path,
                    )
                },
            )
        )

        current_entries = [
            {
                "seed_key": "openai_current_only",
                "full_name": "Current Only",
                "source_type": "harvest_profile_search",
                "employment_status": "current",
                "profile_url": "https://www.linkedin.com/in/openai-current-only/",
            }
        ]
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                scoped_task,
                {
                    "search_seed_snapshot": SearchSeedSnapshot(
                        snapshot_id=snapshot_dir.name,
                        target_company="OpenAI",
                        company_identity=identity,
                        snapshot_dir=snapshot_dir,
                        entries=current_entries,
                        query_summaries=[{"query": "Coding", "status": "completed"}],
                        accounts_used=["harvest_profile_search"],
                        errors=[],
                        stop_reason="provider_people_search_primary",
                        summary_path=summary_path,
                        entries_path=entries_path,
                    )
                },
            )
        )

    def test_normalize_checkpoint_requires_fully_materialized_snapshot_artifacts(self) -> None:
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="xAI",
            snapshot_id="snapshot-normalize-checkpoint",
            candidates=[
                Candidate(
                    candidate_id="cand_xai_checkpoint",
                    name_en="Preview Candidate",
                    display_name="Preview Candidate",
                    category="employee",
                    target_company="xAI",
                    organization="xAI",
                    employment_status="current",
                    role="Engineer",
                    linkedin_url="https://www.linkedin.com/in/preview-candidate/",
                ).to_record()
            ],
        )
        task = AcquisitionTask(
            task_id="normalize-checkpoint",
            task_type="normalize_asset_snapshot",
            title="Normalize snapshot",
            description="Persist snapshot artifacts",
        )

        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="xAI",
            include_strict=False,
        )
        self.assertTrue(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "manifest_path": snapshot_dir / "manifest.json",
                },
            )
        )

        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="xAI",
            include_strict=True,
        )
        fresh_time = time.time() + 10
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (fresh_time, fresh_time))
        self.assertFalse(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "manifest_path": snapshot_dir / "manifest.json",
                },
            )
        )

    def test_build_retrieval_index_checkpoint_requires_summary_newer_than_materialized_snapshot(self) -> None:
        snapshot_dir, _ = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-retrieval-checkpoint",
            candidates=[
                Candidate(
                    candidate_id="cand_openai_checkpoint",
                    name_en="Retriever Candidate",
                    display_name="Retriever Candidate",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/retriever-candidate/",
                ).to_record()
            ],
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
            include_strict=True,
        )
        normalized_time = time.time() + 20
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (normalized_time, normalized_time))

        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
        retrieval_index_path.write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stale_time = normalized_time - 10
        os.utime(retrieval_index_path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="build-retrieval-checkpoint",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Build retrieval index summary",
        )
        self.assertTrue(self.orchestrator._snapshot_retrieval_index_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )

        fresh_time = normalized_time + 10
        os.utime(retrieval_index_path, (fresh_time, fresh_time))
        self.assertFalse(self.orchestrator._snapshot_retrieval_index_refresh_required(snapshot_dir))
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )

    def test_equivalent_baseline_reuse_skips_normalize_asset_snapshot_checkpoint(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "openai" / "baseline-equivalent-normalize"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "OpenAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
            include_strict=True,
        )
        stale_time = time.time() - 60
        fresh_time = time.time() + 60
        os.utime(candidate_doc_path, (fresh_time, fresh_time))
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="normalize-equivalent-baseline",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Persist the authoritative snapshot artifacts",
        )
        self.assertTrue(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "snapshot_id": snapshot_dir.name,
                    "equivalent_baseline_reused": True,
                    "equivalent_baseline_snapshot_id": snapshot_dir.name,
                },
            )
        )

    def test_reused_snapshot_serving_artifacts_skip_normalize_asset_snapshot_checkpoint(self) -> None:
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-reused-serving-normalize",
            candidates=[
                Candidate(
                    candidate_id="cand_reflection_checkpoint",
                    name_en="Reflection Researcher",
                    display_name="Reflection Researcher",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Researcher",
                    linkedin_url="https://www.linkedin.com/in/reflection-checkpoint/",
                ).to_record()
            ],
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

        task = AcquisitionTask(
            task_id="normalize-reused-serving-checkpoint",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Persist the authoritative snapshot artifacts",
        )
        materialized_candidate_doc_path = snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
        self.assertTrue(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertTrue(
            self.orchestrator._reused_snapshot_serving_artifacts_ready(
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": materialized_candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                }
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": materialized_candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                },
            )
        )

    def test_reused_snapshot_candidate_documents_skip_normalize_and_build_retrieval_index_checkpoints(self) -> None:
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-reused-direct-candidate-docs",
            candidates=[
                Candidate(
                    candidate_id="cand_reflection_direct_reuse",
                    name_en="Reflection Direct Reuse",
                    display_name="Reflection Direct Reuse",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/reflection-direct-reuse/",
                ).to_record()
            ],
        )
        normalize_task = AcquisitionTask(
            task_id="normalize-reused-direct-candidate-docs",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Persist the authoritative snapshot artifacts",
        )
        build_index_task = AcquisitionTask(
            task_id="build-index-reused-direct-candidate-docs",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Prepare retrieval-ready artifacts",
        )

        acquisition_state = {
            "snapshot_dir": snapshot_dir,
            "candidate_doc_path": candidate_doc_path,
            "reused_snapshot_checkpoint": True,
        }
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                normalize_task,
                acquisition_state,
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                build_index_task,
                acquisition_state,
            )
        )

    def test_equivalent_baseline_reuse_skips_build_retrieval_index_checkpoint(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "openai" / "baseline-equivalent-index"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "OpenAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
            include_strict=True,
        )
        normalized_time = time.time() + 20
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (normalized_time, normalized_time))

        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
        retrieval_index_path.write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stale_time = normalized_time - 10
        os.utime(retrieval_index_path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="build-equivalent-baseline-index",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Build retrieval index summary",
        )
        self.assertTrue(self.orchestrator._snapshot_retrieval_index_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "snapshot_id": snapshot_dir.name,
                    "equivalent_baseline_reused": True,
                    "equivalent_baseline_snapshot_id": snapshot_dir.name,
                },
            )
        )

    def test_resume_running_workflow_skips_completed_acquisition_tasks_and_defers_materialization(self) -> None:
        request_payload = {
            "raw_user_request": "Find Acme infra researchers",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 1,
            "analysis_stage_mode": "two_stage",
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_resume_checkpoint_skip"
        company_dir = self.settings.company_assets_dir / "acme"
        snapshot_dir = company_dir / "20260410T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        manifest_path = snapshot_dir / "manifest.json"
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="cand_resume_checkpoint",
            name_en="Alice Infra",
            display_name="Alice Infra",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Research Engineer",
            focus_areas="infra systems",
            linkedin_url="https://www.linkedin.com/in/alice-infra",
            source_dataset="checkpoint_test",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "checkpoint_test",
                "Acme infra",
                candidate.linkedin_url,
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile",
            title="Acme infra",
            url=candidate.linkedin_url,
            summary="Checkpoint recovery candidate",
            source_dataset="checkpoint_test",
            source_path=str(candidate_doc_path),
        )
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence.to_record()],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "company_identity": identity.to_record(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(identity.to_record(), ensure_ascii=False, indent=2))

        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Workflow runner died during acquisition"},
        )

        tasks_by_type = {task.task_type: task for task in plan.acquisition_tasks}
        acquisition_state = {
            "snapshot_id": snapshot_dir.name,
            "snapshot_dir": snapshot_dir,
            "company_identity": identity,
        }
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["resolve_company_identity"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["resolve_company_identity"].task_id,
                status="completed",
                detail="Resolved company identity.",
                payload={"snapshot_dir": str(snapshot_dir)},
                state_updates=dict(acquisition_state),
            ),
            acquisition_state=dict(acquisition_state),
        )
        acquisition_state["candidates"] = [candidate]
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["acquire_full_roster"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["acquire_full_roster"].task_id,
                status="completed",
                detail="Acquired roster.",
                payload={"candidate_count": 1},
                state_updates={"candidates": [candidate]},
            ),
            acquisition_state=dict(acquisition_state),
        )
        acquisition_state["candidate_doc_path"] = candidate_doc_path
        acquisition_state["evidence"] = [evidence]
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["enrich_linkedin_profiles"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["enrich_linkedin_profiles"].task_id,
                status="completed",
                detail="Enriched LinkedIn profiles.",
                payload={"candidate_doc_path": str(candidate_doc_path)},
                state_updates={
                    "candidate_doc_path": candidate_doc_path,
                    "linkedin_stage_candidate_doc_path": candidate_doc_path,
                    "linkedin_stage_completed": True,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
            ),
            acquisition_state=dict(acquisition_state),
        )
        self.orchestrator._record_acquisition_task_completion(
            job_id=job_id,
            request=request,
            plan=plan,
            task=tasks_by_type["enrich_public_web_signals"],
            execution=AcquisitionExecution(
                task_id=tasks_by_type["enrich_public_web_signals"].task_id,
                status="completed",
                detail="Enriched public-web signals.",
                payload={"candidate_doc_path": str(candidate_doc_path)},
                state_updates={
                    "candidate_doc_path": candidate_doc_path,
                    "public_web_stage_candidate_doc_path": candidate_doc_path,
                    "public_web_stage_completed": True,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
            ),
            acquisition_state={
                **dict(acquisition_state),
                "candidate_doc_path": candidate_doc_path,
                "linkedin_stage_candidate_doc_path": candidate_doc_path,
                "linkedin_stage_completed": True,
            },
        )

        executed_task_types: list[str] = []
        original_execute_task = self.acquisition_engine.execute_task
        try:
            with unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ):

                def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):
                    executed_task_types.append(task.task_type)
                    self.assertIn(task.task_type, {"normalize_asset_snapshot", "build_retrieval_index"})
                    self.assertEqual(str(state.get("candidate_doc_path")), str(candidate_doc_path))
                    self.assertEqual(len(list(state.get("candidates") or [])), 1)
                    self.assertEqual(len(list(state.get("evidence") or [])), 1)
                    if task.task_type == "normalize_asset_snapshot":
                        manifest_path.write_text(
                            json.dumps(
                                {
                                    "snapshot_id": snapshot_dir.name,
                                    "company_identity": identity.to_record(),
                                },
                                ensure_ascii=False,
                                indent=2,
                            )
                        )
                        return AcquisitionExecution(
                            task_id=task.task_id,
                            status="completed",
                            detail="Normalized snapshot.",
                            payload={"manifest_path": str(manifest_path)},
                            state_updates={"manifest_path": manifest_path},
                        )
                    (snapshot_dir / "retrieval_index_summary.json").write_text(
                        json.dumps({"status": "built"}, ensure_ascii=False, indent=2)
                    )
                    return AcquisitionExecution(
                        task_id=task.task_id,
                        status="completed",
                        detail="Built retrieval index.",
                        payload={"retrieval_index_summary": str(snapshot_dir / "retrieval_index_summary.json")},
                        state_updates={},
                    )

                self.acquisition_engine.execute_task = fake_execute_task
                resume = self.orchestrator._resume_running_workflow_if_ready(job_id)
        finally:
            self.acquisition_engine.execute_task = original_execute_task

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["job_status"], "completed")
        self.assertEqual(executed_task_types, [])
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        skipped_task_ids = {
            str((event.get("payload") or {}).get("task_id") or "")
            for event in list(snapshot["events"])
            if str(event.get("status") or "") == "skipped"
        }
        self.assertTrue(
            {
                tasks_by_type["resolve_company_identity"].task_id,
                tasks_by_type["acquire_full_roster"].task_id,
                tasks_by_type["enrich_linkedin_profiles"].task_id,
                tasks_by_type["enrich_public_web_signals"].task_id,
                tasks_by_type["normalize_asset_snapshot"].task_id,
                tasks_by_type["build_retrieval_index"].task_id,
            }.issubset(skipped_task_ids)
        )
        asset_population = dict(snapshot.get("asset_population") or {})
        self.assertTrue(asset_population.get("available"))
        self.assertGreaterEqual(int(asset_population.get("candidate_count") or 0), 1)

    def test_resume_running_workflow_reclaims_stale_job_lease_when_runner_is_dead(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_resume_reclaims_stale_lease"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={
                "message": "runner died after acquisition worker completed",
                "runtime_controls": {
                    "workflow_runner": {
                        "status": "started",
                        "pid": 999999,
                        "process_alive": False,
                        "job_id": job_id,
                    },
                    "workflow_runner_control": {
                        "status": "started",
                        "handshake": {
                            "status": "advanced",
                            "job_status": "running",
                            "job_stage": "acquiring",
                        },
                    },
                },
            },
        )
        lease = self.store.acquire_workflow_job_lease(
            job_id,
            lease_owner="stale-runner-owner",
            lease_seconds=900,
            lease_token="stale-runner-token",
        )
        self.assertTrue(bool(lease.get("acquired")))

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_assess_acquisition_resume_readiness",
                return_value={
                    "status": "ready",
                    "request": request,
                    "plan": plan,
                    "baseline_ready": True,
                    "baseline_reason": "all_workers_completed",
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_workflow_from_acquisition",
                return_value={"status": "completed"},
            ) as resume_mock,
        ):
            resume = self.orchestrator._resume_running_workflow_if_ready(job_id)

        self.assertEqual(resume["status"], "resumed")
        resume_mock.assert_called_once()
        self.assertIsNone(self.store.get_workflow_job_lease(job_id))
        recovery_events = [
            event
            for event in self.store.list_job_events(job_id, stage="runtime_control")
            if str(event.get("status") or "") == "recovered"
        ]
        self.assertTrue(recovery_events)

    def test_run_worker_recovery_once_auto_takes_over_stale_queued_workflow(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        stale_job_id = "job_stale_queued_takeover"
        self.store.save_job(
            job_id=stale_job_id,
            job_type="workflow",
            status="queued",
            stage="planning",
            request_payload=request_payload,
            plan_payload=plan_payload,
            summary_payload={"message": "workflow queued but runner missing"},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_start_hosted_workflow_thread",
            return_value={
                "status": "started",
                "job_id": stale_job_id,
                "source": "workflow_recovery",
            },
        ) as takeover_mock:
            recovery = self.orchestrator.run_worker_recovery_once(
                {
                    "workflow_auto_resume_enabled": False,
                    "workflow_queue_auto_takeover_enabled": True,
                    "workflow_queue_resume_stale_after_seconds": 0,
                    "workflow_queue_resume_limit": 10,
                }
            )

        takeover_mock.assert_any_call(stale_job_id, source="workflow_recovery")
        self.assertTrue(
            any(
                str(item.get("job_id") or "") == stale_job_id
                and str(item.get("status") or "") == "takeover_started"
                and str(item.get("mode") or "") == "hosted"
                for item in list(recovery.get("workflow_resume") or [])
            )
        )

    def test_acquire_full_roster_blocks_when_background_harvest_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-harvest-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster", "cost_policy": {"allow_company_employee_api": True}},
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "execute_with_checkpoint",
                return_value=HarvestExecutionResult(
                    logical_name="harvest_company_employees",
                    checkpoint={"run_id": "run-harvest", "dataset_id": "dataset-harvest", "status": "submitted"},
                    pending=True,
                    message="Submitted async harvest run.",
                ),
            ),
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "fetch_company_roster",
                side_effect=AssertionError("fetch_company_roster should not run while harvest worker is queued"),
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab people",
                    target_company="Thinking Machines Lab",
                    categories=["employee"],
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_harvest_queued", lane_id="acquisition_specialist"
        )
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_company_employees")

    def test_acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-harvest-partial-baseline"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        summary_path = discovery_dir / "summary.json"
        entries_path = discovery_dir / "entries.json"
        entries_payload = [
            {
                "seed_key": "reflection-former-01",
                "full_name": "Former Infra Builder",
                "headline": "Infrastructure Engineer at Reflection AI",
                "source_type": "linkedin_search",
                "source_query": "Reflection AI former infra",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/former-infra-builder/",
            }
        ]
        summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Reflection AI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [{"query": "Reflection AI former infra", "status": "completed"}],
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
        former_search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries_payload,
            query_summaries=[{"query": "Reflection AI former infra", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
            entries_path=entries_path,
        )

        self.acquisition_engine.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
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
                "include_former_search_seed": True,
                "cost_policy": {"allow_company_employee_api": True},
            },
        )
        prefetch_summary = {
            "status": "queued",
            "requested_url_count": 1,
            "dispatched_url_count": 1,
            "cached_profile_count": 0,
            "queued_worker_count": 1,
            "queued_urls": ["https://www.linkedin.com/in/former-infra-builder/"],
            "failed_urls": [],
            "summary_paths": [str(snapshot_dir / "harvest_profiles" / "queued.json")],
            "errors": [],
        }
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "execute_with_checkpoint",
                return_value=HarvestExecutionResult(
                    logical_name="harvest_company_employees",
                    checkpoint={"run_id": "run-harvest", "dataset_id": "dataset-harvest", "status": "submitted"},
                    pending=True,
                    message="Submitted async harvest run.",
                ),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                return_value=AcquisitionExecution(
                    task_id="acquire-full-roster-former-search-seed",
                    status="completed",
                    detail="Former search seed ready.",
                    payload={"entry_count": 1},
                    state_updates={"search_seed_snapshot": former_search_seed_snapshot},
                ),
            ) as former_search_mock,
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher,
                "queue_background_profile_prefetch",
                return_value=prefetch_summary,
            ) as prefetch_mock,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_partial_baseline",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra members",
                    target_company="Reflection AI",
                    categories=["employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertTrue(bool(execution.payload.get("baseline_ready_from_search_seed")))
        self.assertEqual(
            dict(execution.payload.get("profile_prefetch") or {}).get("queued_worker_count"),
            1,
        )
        self.assertIs(execution.state_updates.get("search_seed_snapshot"), former_search_seed_snapshot)
        self.assertTrue(bool(execution.state_updates.get("allow_search_seed_baseline_for_full_roster")))
        former_search_mock.assert_called_once()
        prefetch_mock.assert_called_once()

    def test_acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-former-search-background"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[
                {
                    "full_name": "Current Infra Builder",
                    "title": "Infrastructure Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/current-infra-builder/",
                }
            ],
            visible_entries=[
                {
                    "full_name": "Current Infra Builder",
                    "title": "Infrastructure Engineer",
                    "linkedin_url": "https://www.linkedin.com/in/current-infra-builder/",
                }
            ],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            merged_path=snapshot_dir / "linkedin_company_people_all.json",
            visible_path=snapshot_dir / "linkedin_company_people_visible.json",
            headless_path=snapshot_dir / "linkedin_company_people_headless.json",
            summary_path=snapshot_dir / "linkedin_company_people_summary.json",
        )
        former_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Reflection AI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "reflection-former-01",
                    "full_name": "Former Infra Builder",
                    "headline": "Former Infrastructure Engineer at Reflection AI",
                    "source_type": "linkedin_search",
                    "source_query": "Reflection AI former infra",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-infra-builder/",
                }
            ],
            query_summaries=[{"query": "Reflection AI former infra", "status": "queued"}],
            accounts_used=[],
            errors=[],
            stop_reason="queued_background_search",
            summary_path=snapshot_dir / "search_seed_discovery" / "summary.json",
            entries_path=snapshot_dir / "search_seed_discovery" / "entries.json",
        )
        prefetch_summary = {
            "status": "queued",
            "requested_url_count": 1,
            "dispatched_url_count": 1,
            "cached_profile_count": 0,
            "queued_worker_count": 1,
            "queued_urls": ["https://www.linkedin.com/in/former-infra-builder/"],
            "failed_urls": [],
            "summary_paths": [str(snapshot_dir / "harvest_profiles" / "queued.json")],
            "errors": [],
        }
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "include_former_search_seed": True,
            },
        )

        with (
            unittest.mock.patch.object(
                self.acquisition_engine.roster_connector,
                "fetch_company_roster",
                return_value=roster_snapshot,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                return_value=AcquisitionExecution(
                    task_id="acquire-full-roster-former-search-seed",
                    status="blocked",
                    detail="Former search queued background tasks.",
                    payload={"stop_reason": "queued_background_search", "queued_query_count": 1},
                    state_updates={"search_seed_snapshot": former_snapshot},
                ),
            ) as former_search_mock,
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher,
                "queue_background_profile_prefetch",
                return_value=prefetch_summary,
            ) as prefetch_mock,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra members",
                    target_company="Reflection AI",
                    categories=["employee", "former_employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertTrue(bool(execution.payload.get("baseline_ready_from_roster")))
        self.assertEqual(str(execution.payload.get("former_search_seed_status") or ""), "blocked")
        self.assertEqual(
            dict(execution.payload.get("profile_prefetch") or {}).get("queued_worker_count"),
            1,
        )
        self.assertIs(execution.state_updates.get("roster_snapshot"), roster_snapshot)
        self.assertIs(execution.state_updates.get("search_seed_snapshot"), former_snapshot)
        former_search_mock.assert_called_once()
        prefetch_mock.assert_called_once()

    def test_acquire_full_roster_force_fresh_run_disables_shared_provider_cache_at_execution_time(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-fresh-runtime"
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
            allow_shared_provider_cache=True,
        ):
            seen["allow_shared_provider_cache"] = allow_shared_provider_cache
            seen["max_pages"] = max_pages
            seen["page_limit"] = page_limit
            merged_path = snapshot_dir / "merged.json"
            visible_path = snapshot_dir / "visible.json"
            headless_path = snapshot_dir / "headless.json"
            summary_path = snapshot_dir / "summary.json"
            merged_path.write_text("[]", encoding="utf-8")
            visible_path.write_text("[]", encoding="utf-8")
            headless_path.write_text("[]", encoding="utf-8")
            summary_path.write_text("{}", encoding="utf-8")
            return CompanyRosterSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company="Anthropic",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                raw_entries=[{"full_name": "Ada Example"}],
                visible_entries=[{"full_name": "Ada Example"}],
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": 1}],
                accounts_used=["harvest_company_employees"],
                errors=[],
                stop_reason="",
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
                "cost_policy": {
                    "allow_company_employee_api": True,
                    "allow_cached_roster_fallback": True,
                    "allow_shared_provider_cache": True,
                },
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
                    raw_user_request="重新跑 Anthropic 全量 roster",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertFalse(bool(seen["allow_shared_provider_cache"]))

    def test_acquire_full_roster_can_reuse_existing_harvest_snapshot_for_incremental_run(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        existing_snapshot_dir = (
            self.settings.company_assets_dir / "anthropic" / "20260409T000000" / "harvest_company_employees"
        )
        existing_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (existing_snapshot_dir / "harvest_company_employees_merged.json").write_text(
            json.dumps(
                [
                    {
                        "full_name": "Ada Example",
                        "headline": "Researcher",
                        "linkedin_url": "https://www.linkedin.com/in/ada-example/",
                    }
                ]
            ),
            encoding="utf-8",
        )
        (existing_snapshot_dir / "harvest_company_employees_visible.json").write_text(
            json.dumps(
                [
                    {
                        "full_name": "Ada Example",
                        "headline": "Researcher",
                        "linkedin_url": "https://www.linkedin.com/in/ada-example/",
                    }
                ]
            ),
            encoding="utf-8",
        )
        (existing_snapshot_dir / "harvest_company_employees_headless.json").write_text(
            "[]",
            encoding="utf-8",
        )
        (existing_snapshot_dir / "harvest_company_employees_summary.json").write_text(
            json.dumps(
                {
                    "company_identity": identity.to_record(),
                    "raw_entry_count": 1,
                    "visible_entry_count": 1,
                    "headless_entry_count": 0,
                    "page_summaries": [{"page": 1, "entry_count": 1}],
                    "accounts_used": ["harvest_company_employees"],
                    "errors": [],
                    "stop_reason": "completed",
                }
            ),
            encoding="utf-8",
        )

        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "20260409T111111"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
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
            side_effect=AssertionError("live roster fetch should be skipped when reusing existing roster"),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="基于现有 roster 做增量",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"reuse_existing_roster": True},
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["acquisition_mode"], "reused_cached_roster")
        reused_summary = snapshot_dir / "harvest_company_employees" / "harvest_company_employees_summary.json"
        self.assertTrue(reused_summary.exists())
        summary_payload = json.loads(reused_summary.read_text())
        self.assertEqual(summary_payload["reused_from_snapshot_id"], "20260409T000000")

    def test_acquire_full_roster_uses_adaptive_shard_policy_to_plan_live_shards(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-adaptive-shard-policy"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )

        planned_shards = [
            {
                "strategy_id": "adaptive_us_function_partition",
                "shard_id": "engineering",
                "title": "United States / Engineering",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
            },
            {
                "strategy_id": "adaptive_us_function_partition",
                "shard_id": "remaining_after_engineering",
                "title": "United States / Remaining after Engineering",
                "max_pages": 100,
                "page_limit": 25,
                "company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]},
            },
        ]
        seen_filters: list[dict[str, object]] = []

        def _fake_fetch_company_roster(
            _identity,
            shard_snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
        ):
            seen_filters.append(dict(company_filters or {}))
            raw_entries = [
                {
                    "member_key": Path(shard_snapshot_dir).name,
                    "full_name": f"Member {Path(shard_snapshot_dir).name}",
                    "headline": "Engineer",
                    "location": "United States",
                    "linkedin_url": f"https://www.linkedin.com/in/{Path(shard_snapshot_dir).name}/",
                    "is_headless": False,
                    "page": 1,
                }
            ]
            shard_dir = Path(shard_snapshot_dir)
            merged_path = shard_dir / "merged.json"
            visible_path = shard_dir / "visible.json"
            headless_path = shard_dir / "headless.json"
            summary_path = shard_dir / "summary.json"
            for path, payload in [
                (merged_path, "[]"),
                (visible_path, "[]"),
                (headless_path, "[]"),
                (summary_path, "{}"),
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(payload, encoding="utf-8")
            return CompanyRosterSnapshot(
                snapshot_id=shard_dir.name,
                target_company="Anthropic",
                company_identity=identity,
                snapshot_dir=shard_dir,
                raw_entries=raw_entries,
                visible_entries=list(raw_entries),
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
                "cost_policy": {"allow_company_employee_api": True},
                "company_employee_shard_policy": {
                    "strategy_id": "adaptive_us_function_partition",
                    "root_title": "United States",
                    "root_filters": {"locations": ["United States"]},
                    "partition_rules": [
                        {
                            "rule_id": "engineering",
                            "title": "Engineering",
                            "include_patch": {"function_ids": ["8"]},
                            "remainder_exclude_patch": {"exclude_function_ids": ["8"]},
                        }
                    ],
                    "max_pages": 100,
                    "page_limit": 25,
                    "probe_max_pages": 1,
                    "probe_page_limit": 25,
                    "provider_result_cap": 2500,
                },
            },
        )
        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_resolve_adaptive_company_employee_shards",
                return_value={"status": "planned", "detail": "planned", "shards": planned_shards},
            ) as plan_mock,
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "fetch_company_roster",
                side_effect=_fake_fetch_company_roster,
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="Anthropic 全量 roster",
                    target_company="Anthropic",
                    categories=["employee"],
                    employment_statuses=["current"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        plan_mock.assert_called_once()
        self.assertEqual(
            seen_filters,
            [
                {"locations": ["United States"], "function_ids": ["8"]},
                {"locations": ["United States"], "exclude_function_ids": ["8"]},
            ],
        )

    def test_acquire_full_roster_merges_segmented_harvest_shards(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-segmented-harvest"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        seen_filters: list[dict[str, object]] = []

        def _fake_fetch_company_roster(
            _identity,
            shard_snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
        ):
            seen_filters.append(dict(company_filters or {}))
            shard_id = Path(shard_snapshot_dir).name
            merged_path = Path(shard_snapshot_dir) / "merged.json"
            visible_path = Path(shard_snapshot_dir) / "visible.json"
            headless_path = Path(shard_snapshot_dir) / "headless.json"
            summary_path = Path(shard_snapshot_dir) / "summary.json"
            for path, payload in [
                (merged_path, "[]"),
                (visible_path, "[]"),
                (headless_path, "[]"),
                (summary_path, "{}"),
            ]:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(payload, encoding="utf-8")
            if shard_id == "us_engineering":
                raw_entries = [
                    {
                        "member_key": "alice",
                        "full_name": "Alice Example",
                        "headline": "Engineer",
                        "location": "San Francisco Bay Area",
                        "linkedin_url": "https://www.linkedin.com/in/alice-example/",
                        "is_headless": False,
                        "page": 1,
                    },
                    {
                        "member_key": "shared",
                        "full_name": "Shared Person",
                        "headline": "Engineer",
                        "location": "New York City Metropolitan Area",
                        "linkedin_url": "https://www.linkedin.com/in/shared-person/",
                        "is_headless": False,
                        "page": 1,
                    },
                ]
            else:
                raw_entries = [
                    {
                        "member_key": "shared",
                        "full_name": "Shared Person",
                        "headline": "Staff",
                        "location": "New York City Metropolitan Area",
                        "linkedin_url": "https://www.linkedin.com/in/shared-person/",
                        "is_headless": False,
                        "page": 1,
                    },
                    {
                        "member_key": "bob",
                        "full_name": "Bob Example",
                        "headline": "Ops",
                        "location": "Mountain View, California, United States",
                        "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                        "is_headless": False,
                        "page": 1,
                    },
                ]
            return CompanyRosterSnapshot(
                snapshot_id=Path(shard_snapshot_dir).name,
                target_company="Anthropic",
                company_identity=identity,
                snapshot_dir=Path(shard_snapshot_dir),
                raw_entries=raw_entries,
                visible_entries=list(raw_entries),
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": len(raw_entries)}],
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
                "cost_policy": {"allow_company_employee_api": True},
                "company_employee_shards": [
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_engineering",
                        "title": "United States / Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                    },
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_non_engineering",
                        "title": "United States / Exclude Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]},
                    },
                ],
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
                    raw_user_request="找 Anthropic 美国成员",
                    target_company="Anthropic",
                    categories=["employee"],
                    employment_statuses=["current"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(execution.state_updates["roster_snapshot"].visible_entries), 3)
        self.assertEqual(
            seen_filters,
            [
                {"locations": ["United States"], "function_ids": ["8"]},
                {"locations": ["United States"], "exclude_function_ids": ["8"]},
            ],
        )
        self.assertIn("segmented Harvest company-employee shards", execution.detail)

    def test_acquire_full_roster_blocks_when_segmented_background_harvest_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-segmented-harvest-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
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
                "cost_policy": {"allow_company_employee_api": True},
                "company_employee_shards": [
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_engineering",
                        "title": "United States / Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "function_ids": ["8"]},
                    },
                    {
                        "strategy_id": "us_primary_function_split",
                        "shard_id": "us_non_engineering",
                        "title": "United States / Exclude Engineering",
                        "max_pages": 100,
                        "page_limit": 25,
                        "company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]},
                    },
                ],
            },
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "execute_with_checkpoint",
                return_value=HarvestExecutionResult(
                    logical_name="harvest_company_employees",
                    checkpoint={"run_id": "run-harvest", "dataset_id": "dataset-harvest", "status": "submitted"},
                    pending=True,
                    message="Submitted async harvest run.",
                ),
            ),
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "fetch_company_roster",
                side_effect=AssertionError(
                    "fetch_company_roster should not run while segmented harvest workers are queued"
                ),
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_segmented_harvest_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Anthropic US people",
                    target_company="Anthropic",
                    categories=["employee"],
                    employment_statuses=["current"],
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_segmented_harvest_queued",
            lane_id="acquisition_specialist",
        )
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 2)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest_shards")
        self.assertEqual(len(workers), 2)
        self.assertTrue(all(worker["metadata"]["root_snapshot_dir"] == str(snapshot_dir) for worker in workers))
        company_filters = [dict(worker["metadata"].get("company_filters") or {}) for worker in workers]
        self.assertIn({"locations": ["United States"], "function_ids": ["8"]}, company_filters)
        self.assertIn({"locations": ["United States"], "exclude_function_ids": ["8"]}, company_filters)

        resumed_state = self.orchestrator._build_acquisition_state("job_segmented_harvest_queued", {})
        self.assertEqual(resumed_state["snapshot_dir"], snapshot_dir)

    def test_acquire_search_seed_pool_allows_provider_only_former_pass_without_queries(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-former-provider-only"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        seeded_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "former_1",
                    "full_name": "Former Example",
                    "source_type": "harvest_profile_search",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                }
            ],
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=summary_path,
        )
        task = AcquisitionTask(
            task_id="former-seed-only",
            task_type="acquire_search_seed_pool",
            title="Acquire former seed",
            description="Former-only provider recall",
            status="ready",
            blocking=False,
            metadata={
                "strategy_type": "former_employee_search",
                "employment_statuses": ["former"],
                "search_seed_queries": [],
                "search_query_bundles": [],
                "filter_hints": {"past_companies": ["Anthropic"]},
                "cost_policy": {"provider_people_search_mode": "fallback_only"},
            },
        )
        with unittest.mock.patch.object(
            self.acquisition_engine.search_seed_acquirer,
            "discover",
            return_value=seeded_snapshot,
        ):
            execution = self.acquisition_engine._acquire_search_seed_pool(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="找 Anthropic former",
                    target_company="Anthropic",
                    categories=["former_employee"],
                    employment_statuses=["former"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertEqual(len(execution.state_updates["search_seed_snapshot"].entries), 1)

    def test_acquire_full_roster_adds_default_former_search_seed_snapshot(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-roster-with-former"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        summary_path = snapshot_dir / "summary.json"
        for path, payload in [
            (merged_path, "[]"),
            (visible_path, "[]"),
            (headless_path, "[]"),
            (summary_path, "{}"),
        ]:
            path.write_text(payload, encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[{"full_name": "Ada Current"}],
            visible_entries=[{"full_name": "Ada Current"}],
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
        former_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        former_summary_path.parent.mkdir(parents=True, exist_ok=True)
        former_summary_path.write_text("{}", encoding="utf-8")
        former_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "former_1",
                    "full_name": "Former Ada",
                    "source_type": "harvest_profile_search",
                    "employment_status": "former",
                    "profile_url": "https://www.linkedin.com/in/former-ada/",
                }
            ],
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=former_summary_path,
        )
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
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
                "cost_policy": {"allow_company_employee_api": True},
                "include_former_search_seed": True,
                "former_provider_people_search_min_expected_results": 50,
            },
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.harvest_company_connector),
                "fetch_company_roster",
                return_value=roster_snapshot,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_search_seed_pool",
                return_value=AcquisitionExecution(
                    task_id="former-seed",
                    status="completed",
                    detail="Captured 1 former seed.",
                    payload={},
                    state_updates={"search_seed_snapshot": former_snapshot},
                ),
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="找 Anthropic 全量成员",
                    target_company="Anthropic",
                    categories=["employee", "former_employee"],
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertIn("search_seed_snapshot", execution.state_updates)
        self.assertEqual(execution.payload["former_search_seed_entry_count"], 1)

    def test_enrich_profiles_merges_roster_and_search_seed_candidates_before_enrichment(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropicresearch",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-merge-enrichment"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        merged_path = snapshot_dir / "merged.json"
        visible_path = snapshot_dir / "visible.json"
        headless_path = snapshot_dir / "headless.json"
        roster_summary_path = snapshot_dir / "roster_summary.json"
        for path, payload in [
            (merged_path, "[]"),
            (visible_path, "[]"),
            (headless_path, "[]"),
            (roster_summary_path, "{}"),
        ]:
            path.write_text(payload, encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[],
            visible_entries=[],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            merged_path=merged_path,
            visible_path=visible_path,
            headless_path=headless_path,
            summary_path=roster_summary_path,
        )
        seed_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        seed_summary_path.parent.mkdir(parents=True, exist_ok=True)
        seed_summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=seed_summary_path,
        )
        roster_candidate = Candidate(
            candidate_id="roster_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/ada-example/",
            source_dataset="anthropic_linkedin_company_people",
            notes="LinkedIn company roster baseline.",
        )
        seed_candidate = Candidate(
            candidate_id="seed_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="former_employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="former",
            role="Former Research Scientist",
            linkedin_url="https://www.linkedin.com/in/ada-example/",
            source_dataset="anthropic_search_seed_candidates",
        )
        roster_evidence = EvidenceRecord(
            evidence_id="ev_roster_1",
            candidate_id="roster_1",
            source_type="linkedin_company_people",
            title="Roster row",
            url="https://www.linkedin.com/in/ada-example/",
            summary="Current roster row",
            source_dataset="anthropic_linkedin_company_people",
            source_path=str(roster_summary_path),
        )
        seed_evidence = EvidenceRecord(
            evidence_id="ev_seed_1",
            candidate_id="seed_1",
            source_type="harvest_profile_search",
            title="Former seed",
            url="https://www.linkedin.com/in/ada-example/",
            summary="Former seed row",
            source_dataset="anthropic_search_seed_candidates",
            source_path=str(seed_summary_path),
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            return MultiSourceEnrichmentResult(
                candidates=list(candidates_arg),
                evidence=[],
            )

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            with (
                unittest.mock.patch(
                    "sourcing_agent.acquisition.build_candidates_from_roster",
                    return_value=([roster_candidate], [roster_evidence]),
                ),
                unittest.mock.patch(
                    "sourcing_agent.acquisition.build_candidates_from_seed_snapshot",
                    return_value=([seed_candidate], [seed_evidence]),
                ),
            ):
                execution = self.acquisition_engine._enrich_profiles(
                    AcquisitionTask(
                        task_id="enrich-profiles",
                        task_type="enrich_profiles_multisource",
                        title="Enrich profiles",
                        description="Run profile enrichment",
                        status="ready",
                        blocking=True,
                        metadata={"cost_policy": {}},
                    ),
                    {
                        "roster_snapshot": roster_snapshot,
                        "search_seed_snapshot": search_seed_snapshot,
                        "snapshot_dir": snapshot_dir,
                    },
                    JobRequest(
                        raw_user_request="找 Anthropic 成员",
                        target_company="Anthropic",
                        categories=["employee"],
                    ),
                )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 1)
        self.assertEqual(execution.payload["acquisition_canonicalization"]["canonical_candidate_count"], 1)

    def test_enrich_profiles_completes_when_background_exploration_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-exploration-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-exploration-queued",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Queued Exploration Lead",
                    "source_type": "harvest_profile_search",
                    "linkedin_url": "https://www.linkedin.com/in/queued-exploration/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        pending_candidate = Candidate(
            candidate_id="lead_1",
            name_en="Queued Exploration Lead",
            display_name="Queued Exploration Lead",
            category="lead",
            target_company="Thinking Machines Lab",
            organization="Thinking Machines Lab",
            linkedin_url="https://www.linkedin.com/in/queued-exploration/",
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        self.acquisition_engine.multi_source_enricher.enrich = lambda *args, **kwargs: MultiSourceEnrichmentResult(
            candidates=[pending_candidate],
            evidence=[],
            artifact_paths={"exploration_summary": str(snapshot_dir / "exploration" / "summary.json")},
            queued_exploration_count=1,
            stop_reason="queued_background_exploration",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={"cost_policy": {}},
        )
        try:
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_enrichment_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab people",
                    target_company="Thinking Machines Lab",
                    categories=["employee"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["queued_exploration_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_exploration")
        self.assertEqual(execution.payload["candidate_count"], 1)

    def test_enrich_profiles_completes_when_background_harvest_profile_batch_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Thinking Machines Lab",
            canonical_name="Thinking Machines Lab",
            company_key="thinkingmachineslab",
            linkedin_slug="thinkingmachinesai",
            linkedin_company_url="https://www.linkedin.com/company/thinkingmachinesai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "thinkingmachineslab" / "snapshot-harvest-profile-queued"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-harvest-profile-queued",
            target_company="Thinking Machines Lab",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Queued Harvest Lead",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/queued-harvest/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        self.acquisition_engine.multi_source_enricher.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings = replace(
            self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={
                "profile_detail_limit": 5,
                "slug_resolution_limit": 5,
                "publication_scan_limit": 0,
                "publication_lead_limit": 0,
                "exploration_limit": 0,
                "cost_policy": {},
            },
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
                "execute_batch_with_checkpoint",
                return_value=HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-profile", "dataset_id": "dataset-profile", "status": "submitted"},
                    pending=True,
                    message="Submitted async harvest batch.",
                ),
            ),
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
                "fetch_profiles_by_urls",
                side_effect=AssertionError(
                    "fetch_profiles_by_urls should not run while harvest batch worker is queued"
                ),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.slug_resolver,
                "resolve",
                return_value={"results": [], "errors": [], "summary_path": None},
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.publication_connector,
                "enrich",
                return_value={
                    "matched_candidates": [],
                    "lead_candidates": [],
                    "publication_matches": [],
                    "coauthor_edges": [],
                    "scholar_coauthor_prospects": [],
                    "artifact_paths": {},
                    "errors": [],
                    "evidence": [],
                },
            ),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_profile_queued",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Thinking Machines Lab people",
                    target_company="Thinking Machines Lab",
                    categories=["employee"],
                    profile_detail_limit=5,
                    slug_resolution_limit=5,
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_harvest_profile_queued", lane_id="enrichment_specialist"
        )
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 1)
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_profile_batch")

    def test_enrich_profiles_degrades_when_background_harvest_profile_batch_submit_fails(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-profile-failed"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-harvest-profile-failed",
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Failed Harvest Lead",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/failed-harvest/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        self.acquisition_engine.multi_source_enricher.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings = replace(
            self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={
                "profile_detail_limit": 5,
                "slug_resolution_limit": 5,
                "publication_scan_limit": 0,
                "publication_lead_limit": 0,
                "exploration_limit": 0,
                "cost_policy": {},
            },
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
                "execute_batch_with_checkpoint",
                side_effect=RuntimeError("Harvest API request failed: Remote end closed connection without response"),
            ),
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.profile_connector),
                "fetch_profile",
                return_value=None,
            ),
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
                "fetch_profiles_by_urls",
                return_value={},
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.slug_resolver,
                "resolve",
                return_value={"results": [], "errors": [], "summary_path": None},
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.publication_connector,
                "enrich",
                return_value={
                    "matched_candidates": [],
                    "lead_candidates": [],
                    "publication_matches": [],
                    "coauthor_edges": [],
                    "scholar_coauthor_prospects": [],
                    "artifact_paths": {},
                    "errors": [],
                    "evidence": [],
                },
            ),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_profile_failed",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find OpenAI RL people",
                    target_company="OpenAI",
                    categories=["employee"],
                    profile_detail_limit=5,
                    slug_resolution_limit=5,
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_harvest_profile_failed",
            lane_id="enrichment_specialist",
        )
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 1)
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 0)
        self.assertEqual(execution.payload["stop_reason"], "")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["status"], "failed")
        self.assertIn("Remote end closed connection", workers[0]["output"]["summary"]["message"])
        self.assertEqual(
            workers[0]["output"]["summary"]["failed_urls"],
            ["https://www.linkedin.com/in/failed-harvest/"],
        )

    def test_enrich_profiles_writes_baseline_when_full_roster_profile_prefetch_is_pending(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-enrich-full-prefetch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-enrich-full-prefetch",
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Ada Example",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/ada-example/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        self.acquisition_engine.multi_source_enricher.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings = replace(
            self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "full_roster_profile_prefetch": True,
                "profile_detail_limit": 5,
                "slug_resolution_limit": 5,
                "publication_scan_limit": 0,
                "publication_lead_limit": 0,
                "exploration_limit": 0,
                "cost_policy": {},
            },
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
                "execute_batch_with_checkpoint",
                return_value=HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-profile", "dataset_id": "dataset-profile", "status": "submitted"},
                    pending=True,
                    message="Submitted async harvest batch.",
                ),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.slug_resolver,
                "resolve",
                side_effect=AssertionError("slug resolution should not continue before full roster prefetch completes"),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.publication_connector,
                "enrich",
                side_effect=AssertionError(
                    "publication enrichment should not continue before full roster prefetch completes"
                ),
            ),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_harvest_profile_full_prefetch",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Anthropic people",
                    target_company="Anthropic",
                    categories=["employee"],
                    profile_detail_limit=5,
                    slug_resolution_limit=5,
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_harvest_profile_full_prefetch",
            lane_id="enrichment_specialist",
        )
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
        self.assertEqual(workers[0]["metadata"]["recovery_kind"], "harvest_profile_batch")
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        stage_candidate_doc_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
        self.assertTrue(candidate_doc_path.exists())
        self.assertFalse(stage_candidate_doc_path.exists())
        payload = json.loads(candidate_doc_path.read_text())
        self.assertEqual(int(payload["candidate_count"]), 1)
        self.assertEqual(payload["background_reconcile"]["kind"], "harvest_profile_prefetch")
        self.assertTrue(execution.payload["background_reconcile_pending"])

    def test_scoped_search_prefetch_queues_all_known_profile_urls(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-scoped-full-prefetch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-scoped-full-prefetch",
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Reasoning Person A",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/reasoning-person-a/",
                },
                {
                    "seed_key": "lead_2",
                    "full_name": "Reasoning Person B",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/reasoning-person-b/",
                },
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        self.acquisition_engine.multi_source_enricher.worker_runtime = self.orchestrator.agent_runtime
        self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings = replace(
            self.acquisition_engine.multi_source_enricher.harvest_profile_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "scoped_search_roster",
                "full_roster_profile_prefetch": True,
                "profile_detail_limit": 1,
                "slug_resolution_limit": 1,
                "publication_scan_limit": 0,
                "publication_lead_limit": 0,
                "exploration_limit": 0,
                "cost_policy": {},
            },
        )
        with (
            unittest.mock.patch.object(
                type(self.acquisition_engine.multi_source_enricher.harvest_profile_connector),
                "execute_batch_with_checkpoint",
                return_value=HarvestExecutionResult(
                    logical_name="harvest_profile_scraper_batch",
                    checkpoint={"run_id": "run-profile", "dataset_id": "dataset-profile", "status": "submitted"},
                    pending=True,
                    message="Submitted async harvest batch.",
                ),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.slug_resolver,
                "resolve",
                side_effect=AssertionError(
                    "slug resolution should not continue before scoped-search profile prefetch completes"
                ),
            ),
            unittest.mock.patch.object(
                self.acquisition_engine.multi_source_enricher.publication_connector,
                "enrich",
                side_effect=AssertionError(
                    "publication enrichment should not continue before scoped-search profile prefetch completes"
                ),
            ),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_scoped_profile_full_prefetch",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find OpenAI Reasoning people",
                    target_company="OpenAI",
                    categories=["employee"],
                    employment_statuses=["current", "former"],
                    profile_detail_limit=1,
                    slug_resolution_limit=1,
                ),
            )

        workers = self.orchestrator.agent_runtime.list_workers(
            job_id="job_scoped_profile_full_prefetch",
            lane_id="enrichment_specialist",
        )
        queued_urls = sorted(
            {
                str(profile_url)
                for worker in workers
                for profile_url in list(dict(worker.get("metadata") or {}).get("profile_urls") or [])
                if str(profile_url).strip()
            }
        )
        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 2)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 2)
        self.assertEqual(
            queued_urls,
            [
                "https://www.linkedin.com/in/reasoning-person-a/",
                "https://www.linkedin.com/in/reasoning-person-b/",
            ],
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        self.assertTrue(candidate_doc_path.exists())
        payload = json.loads(candidate_doc_path.read_text())
        self.assertEqual(int(payload["candidate_count"]), 2)
        self.assertEqual(payload["background_reconcile"]["kind"], "harvest_profile_prefetch")
        self.assertTrue(execution.payload["background_reconcile_pending"])

    def test_enrich_profiles_force_fresh_run_disables_shared_provider_cache_at_execution_time(self) -> None:
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-enrich-fresh-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id="snapshot-enrich-fresh-runtime",
            target_company="Anthropic",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "lead_1",
                    "full_name": "Ada Example",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/ada-example/",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=summary_path,
        )
        seen: dict[str, object] = {}
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        self.acquisition_engine.multi_source_enricher.enrich = lambda *args, **kwargs: (
            seen.update({"cost_policy": dict(kwargs.get("cost_policy") or {})})
            or MultiSourceEnrichmentResult(
                candidates=[],
                evidence=[],
            )
        )
        task = AcquisitionTask(
            task_id="enrich-profiles",
            task_type="enrich_profiles_multisource",
            title="Enrich profiles",
            description="Run profile enrichment",
            status="ready",
            blocking=True,
            metadata={"cost_policy": {"allow_shared_provider_cache": True}},
        )
        try:
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="重新跑 Anthropic enrichment",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertFalse(bool(seen["cost_policy"]["allow_shared_provider_cache"]))

    def test_reconcile_completed_workflow_after_background_harvest_prefetch(self) -> None:
        request_payload = {
            "raw_user_request": "Find Humans& coding researchers",
            "target_company": "Humans&",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["coding"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_prefetch_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "humansand" / "snapshot-reconcile"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
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
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::abc123",
            stage="enriching",
            span_name="harvest_profile_batch:abc123",
            budget_payload={"requested_url_count": 2},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/test"]},
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

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.build_company_candidate_artifacts",
            return_value={
                "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                "artifact_paths": {
                    "materialized_candidate_documents": str(
                        snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                    )
                },
            },
        ):
            with unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after background harvest reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ):
                reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        refreshed_job = self.store.get_job(job_id)
        self.assertEqual(reconcile["status"], "reconciled_harvest_prefetch")
        self.assertIsNotNone(refreshed_job)
        assert refreshed_job is not None
        self.assertEqual(refreshed_job["status"], "completed")
        self.assertEqual(refreshed_job["stage"], "completed")
        summary = dict(refreshed_job.get("summary") or {})
        self.assertIn("background_reconcile", summary)
        self.assertIn("harvest_prefetch", dict(summary.get("background_reconcile") or {}))

    def test_reconcile_completed_workflow_after_background_harvest_prefetch_uses_shared_snapshot_apply_contract(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_prefetch_reconcile_shared_apply"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-reconcile-shared-apply"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url = "https://www.linkedin.com/in/openai-shared-apply/"
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
                            candidate_id="openai-shared-apply",
                            name_en="Shared Apply",
                            display_name="Shared Apply",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url,
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
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Shared Apply",
            headline="Research Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Research Engineer", "current": True}],
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
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::shared-apply",
            stage="enriching",
            span_name="harvest_profile_batch:shared-apply",
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
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value={
                    "status": "completed",
                    "state_updates": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                    },
                    "candidate_count": 1,
                    "evidence_count": 1,
                },
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.CompanyAssetCompletionManager.complete_snapshot_profiles",
                side_effect=AssertionError("completed reconcile should use shared harvest delta apply instead of legacy completion manager"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after background harvest reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_harvest_prefetch")
        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        refreshed_candidate = list(candidate_doc.get("candidates") or [])[0]
        self.assertIn("harvest_profiles", str(refreshed_candidate.get("source_path") or ""))
        self.assertGreaterEqual(len(list(candidate_doc.get("evidence") or [])), 1)
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        background_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {})
        self.assertIn("harvest_prefetch", background_reconcile)

    def test_persist_completed_workflow_summary_schedules_background_outreach_layering_reconcile(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
                "top_k": 10,
            }
        )
        plan_payload = self.orchestrator.plan_workflow(request.to_record())["plan"]
        plan = hydrate_sourcing_plan(plan_payload)
        job_id = "job_schedule_deferred_outreach_layering"
        artifact_path = self.settings.jobs_dir / f"{job_id}.result.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False), encoding="utf-8")
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Retrieving candidates",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": "snapshot-scheduled-outreach-layering",
                    "asset_view": "canonical_merged",
                },
                "outreach_layering": {
                    "status": "deferred",
                    "snapshot_id": "snapshot-scheduled-outreach-layering",
                    "reason": "deferred_for_asset_population_fast_path",
                },
            },
            artifact_path=str(artifact_path),
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_queue_background_outreach_layering_reconcile",
            return_value={"status": "scheduled", "job_id": job_id},
        ) as queue_reconcile:
            final_summary = self.orchestrator._persist_completed_workflow_summary(
                job_id=job_id,
                request=request,
                plan=plan,
                artifact={
                    "artifact_path": str(artifact_path),
                    "summary": {
                        "analysis_stage": "stage_2_final",
                        "candidate_source": {
                            "source_kind": "company_snapshot",
                            "snapshot_id": "snapshot-scheduled-outreach-layering",
                            "asset_view": "canonical_merged",
                        },
                        "outreach_layering": {
                            "status": "deferred",
                            "snapshot_id": "snapshot-scheduled-outreach-layering",
                            "reason": "deferred_for_asset_population_fast_path",
                        },
                    },
                },
                preserved_summary={
                    "candidate_source": {
                        "source_kind": "company_snapshot",
                        "snapshot_id": "snapshot-scheduled-outreach-layering",
                        "asset_view": "canonical_merged",
                    },
                    "outreach_layering": {
                        "status": "deferred",
                        "snapshot_id": "snapshot-scheduled-outreach-layering",
                        "reason": "deferred_for_asset_population_fast_path",
                    },
                },
            )

        self.assertEqual(str(dict(final_summary.get("outreach_layering") or {}).get("status") or ""), "scheduled")
        queue_reconcile.assert_called_once_with(
            job_id=job_id,
            source="workflow_completion",
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        self.assertEqual(
            str(dict(dict(refreshed_job.get("summary") or {}).get("outreach_layering") or {}).get("status") or ""),
            "scheduled",
        )

    def test_reconcile_completed_workflow_after_deferred_outreach_layering(self) -> None:
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
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_deferred_outreach_layering_reconcile"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-deferred-outreach-layering"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
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
                "status": "completed",
                "message": "Workflow completed.",
                "analysis_stage": "stage_2_final",
                "started_at": "2026-04-22T02:26:04+00:00",
                "completed_at": "2026-04-22T02:26:27+00:00",
                "stage_2_final_started_at": "2026-04-22T02:26:04+00:00",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_dir.name,
                    "asset_view": "canonical_merged",
                },
                "public_web_stage_2": {
                    "status": "completed",
                    "started_at": "2026-04-22T02:25:03+00:00",
                    "completed_at": "2026-04-22T02:26:04+00:00",
                },
                "outreach_layering": {
                    "status": "scheduled",
                    "snapshot_id": snapshot_dir.name,
                    "reason": "deferred_for_asset_population_fast_path",
                },
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_run_outreach_layering_after_acquisition",
            return_value={
                "status": "completed",
                "snapshot_id": snapshot_dir.name,
                "analysis_paths": {"full": str(snapshot_dir / "layered_segmentation" / "layered_analysis.json")},
                "candidate_count": 153,
                "layer_counts": {"layer_0_roster": 153},
            },
        ) as run_layering:
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_outreach_layering")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        refreshed_summary = dict(refreshed_job.get("summary") or {})
        self.assertEqual(str(dict(refreshed_summary.get("outreach_layering") or {}).get("status") or ""), "completed")
        self.assertEqual(str(refreshed_summary.get("status") or ""), "completed")
        self.assertEqual(str(refreshed_summary.get("started_at") or ""), "2026-04-22T02:26:04Z")
        self.assertEqual(str(refreshed_summary.get("completed_at") or ""), "2026-04-22T02:26:27Z")
        background_reconcile = dict(refreshed_summary.get("background_reconcile") or {})
        self.assertEqual(
            str(dict(background_reconcile.get("outreach_layering") or {}).get("status") or ""),
            "completed",
        )
        run_layering.assert_called_once()
        self.assertFalse(bool(run_layering.call_args.kwargs.get("allow_background_defer", True)))
        artifact_payload = json.loads(artifact_path.read_text())
        self.assertEqual(
            str(dict(dict(artifact_payload.get("summary") or {}).get("outreach_layering") or {}).get("status") or ""),
            "completed",
        )
        self.assertEqual(str(dict(artifact_payload.get("summary") or {}).get("status") or ""), "completed")
        stage_summary_path = Path(str(dict(artifact_payload.get("summary") or {}).get("stage_summary_path") or ""))
        self.assertTrue(stage_summary_path.exists())
        stage_summary_payload = json.loads(stage_summary_path.read_text(encoding="utf-8"))
        self.assertEqual(str(stage_summary_payload.get("status") or ""), "completed")
        self.assertEqual(str(stage_summary_payload.get("started_at") or ""), "2026-04-22T02:26:04Z")
        self.assertEqual(str(stage_summary_payload.get("completed_at") or ""), "2026-04-22T02:26:27Z")

    def test_reconcile_completed_workflow_after_background_harvest_prefetch_uses_job_result_view_when_summary_sparse(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Humans& coding researchers",
            "target_company": "Humans&",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["coding"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_prefetch_reconcile_result_view"
        snapshot_dir = self.settings.company_assets_dir / "humansand" / "snapshot-reconcile-result-view"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2),
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
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Humans&",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id=snapshot_dir.name,
            source_path=str(candidate_doc_path),
            summary={"candidate_count": 0},
        )
        worker_handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::rv",
            stage="enriching",
            span_name="harvest_profile_batch:rv",
            budget_payload={"requested_url_count": 2},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/test"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir.parent / "missing-snapshot"),
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

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value={
                    "status": "completed",
                    "state_updates": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                    },
                },
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.CompanyAssetCompletionManager.complete_snapshot_profiles",
                return_value={"status": "completed"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after background harvest reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ),
        ):
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_harvest_prefetch")

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
                "_execute_retrieval",
                return_value={
                    "job_id": job_id,
                    "status": "completed",
                    "summary": {"message": "Workflow completed after search-seed reconcile."},
                    "artifact_path": str(artifact_path),
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={
                    "status": "completed",
                    "snapshot_id": snapshot_dir.name,
                    "layer_counts": {"layer_0_roster": 2},
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
            reconcile = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(reconcile["status"], "reconciled_search_seed")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        search_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get(
            "search_seed"
        )
        self.assertEqual(int(search_reconcile["applied_worker_count"]), 1)
        self.assertEqual(int(dict(search_reconcile.get("profile_prefetch") or {}).get("queued_worker_count") or 0), 1)
        self.assertEqual(str(dict(search_reconcile.get("outreach_layering") or {}).get("status") or ""), "completed")
        updated_entries = json.loads((discovery_dir / "entries.json").read_text())
        self.assertEqual(len(updated_entries), 2)
        updated_summary = json.loads((discovery_dir / "summary.json").read_text())
        self.assertEqual(int(updated_summary["queued_query_count"]), 0)
        candidate_doc = json.loads(candidate_doc_path.read_text())
        self.assertGreaterEqual(int(candidate_doc["candidate_count"]), 2)
        self.assertEqual(
            str(dict(refreshed_job.get("summary") or {}).get("outreach_layering", {}).get("status") or ""),
            "completed",
        )
        self.assertIsNotNone(
            self.store.find_candidate_by_name(
                target_company="Reflection AI",
                name_en="Infra Builder",
            )
        )

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
                return_value={
                    "status": "completed",
                    "state_updates": {
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                    },
                    "candidate_count": 1,
                    "evidence_count": 1,
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

    def test_refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infrastructure people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_harvest_prefetch_refresh"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-prefetch-refresh"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url = "https://www.linkedin.com/in/openai-infra-builder/"
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
                            candidate_id="openai-infra-builder",
                            name_en="Infra Builder",
                            display_name="Infra Builder",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url,
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
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Infra Builder",
            headline="Infrastructure Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Infrastructure Engineer", "current": True}],
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
            worker_key="harvest_profile_batch::refresh",
            stage="enriching",
            span_name="harvest_profile_batch:refresh",
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
            worker_handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            return_value={
                "status": "completed",
                "state_updates": {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
                "candidate_count": 1,
                "evidence_count": 1,
            },
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
        self.assertEqual(str(dict(refresh.get("harvest_prefetch") or {}).get("status") or ""), "applied")
        self.assertEqual(int(dict(refresh.get("harvest_prefetch") or {}).get("resolved_candidate_count") or 0), 1)
        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        refreshed_candidate = list(candidate_doc.get("candidates") or [])[0]
        self.assertIn("harvest_profiles", str(refreshed_candidate.get("source_path") or ""))
        self.assertGreaterEqual(len(list(candidate_doc.get("evidence") or [])), 1)
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        background_reconcile = dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {})
        self.assertIn("harvest_prefetch", background_reconcile)
        self.assertEqual(int(background_reconcile["harvest_prefetch"]["applied_worker_count"] or 0), 1)

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
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertGreaterEqual(int(candidate_doc.get("candidate_count") or 0), 1)
        self.assertTrue(
            any(str(item.get("name_en") or "") == "Mira Agent" for item in list(candidate_doc.get("candidates") or []))
        )
        refreshed_worker = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert refreshed_worker is not None
        inline_ingest = dict(dict(refreshed_worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_ingest.get("worker_kind") or ""), "company_roster")
        self.assertEqual(str(inline_ingest.get("sync_status") or ""), "deferred")
        self.assertEqual(str(inline_ingest.get("sync_reason") or ""), "same_kind_background_workers_still_inflight")
        self.assertEqual(int(inline_ingest.get("applied_worker_count") or 0), 1)
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        acquisition_progress = dict(dict(refreshed_job.get("summary") or {}).get("acquisition_progress") or {})
        latest_state = dict(acquisition_progress.get("latest_state") or {})
        company_cursor = dict(dict(latest_state.get("background_reconcile_cursor") or {}).get("company_roster") or {})
        self.assertEqual([int(item) for item in list(company_cursor.get("worker_ids") or [])], [completed_worker.worker_id])

    def test_handle_completed_recovery_worker_result_micro_batches_completed_harvest_prefetch_workers_and_syncs_once(
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
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_inline_harvest_prefetch_micro_batch"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-inline-harvest-prefetch-batch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url_a = "https://www.linkedin.com/in/openai-inline-a/"
        profile_url_b = "https://www.linkedin.com/in/openai-inline-b/"
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
                            candidate_id="openai-inline-a",
                            name_en="Inline A",
                            display_name="Inline A",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url_a,
                        ).to_record(),
                        Candidate(
                            candidate_id="openai-inline-b",
                            name_en="Inline B",
                            display_name="Inline B",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url_b,
                        ).to_record(),
                    ],
                    "evidence": [],
                    "candidate_count": 2,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url_a,
            full_name="Inline A",
            headline="Infrastructure Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Infrastructure Engineer", "current": True}],
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url_b,
            full_name="Inline B",
            headline="Platform Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Platform Engineer", "current": True}],
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
        worker_a = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-a",
            stage="enriching",
            span_name="harvest_profile_batch:inline-a",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url_a]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url_a],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        worker_b = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-b",
            stage="enriching",
            span_name="harvest_profile_batch:inline-b",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url_b]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url_b],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_a,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url_a]}},
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_b,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url_b]}},
        )

        synchronize_mock = unittest.mock.Mock(
            return_value={
                "status": "completed",
                "state_updates": {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
                "candidate_count": 2,
                "evidence_count": 2,
            }
        )
        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            synchronize_mock,
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_a.worker_id, "worker_status": "completed"}
            )
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_b.worker_id, "worker_status": "completed"}
            )

        self.assertEqual(synchronize_mock.call_count, 1)
        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(list(candidate_doc.get("evidence") or [])), 2)
        worker_a_row = self.store.get_agent_worker(worker_id=worker_a.worker_id)
        worker_b_row = self.store.get_agent_worker(worker_id=worker_b.worker_id)
        assert worker_a_row is not None
        assert worker_b_row is not None
        inline_a = dict(dict(worker_a_row.get("output") or {}).get("inline_incremental_ingest") or {})
        inline_b = dict(dict(worker_b_row.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(int(inline_a.get("applied_worker_count") or 0), 2)
        self.assertEqual(int(inline_b.get("applied_worker_count") or 0), 2)
        self.assertEqual(str(inline_a.get("sync_status") or ""), "completed")
        self.assertEqual(str(inline_b.get("sync_status") or ""), "completed")
        self.assertEqual(
            sorted(int(item) for item in list(inline_a.get("applied_worker_ids") or [])),
            sorted([worker_a.worker_id, worker_b.worker_id]),
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        acquisition_progress = dict(dict(refreshed_job.get("summary") or {}).get("acquisition_progress") or {})
        latest_state = dict(acquisition_progress.get("latest_state") or {})
        harvest_cursor = dict(dict(latest_state.get("background_reconcile_cursor") or {}).get("harvest_prefetch") or {})
        self.assertEqual(
            sorted(int(item) for item in list(harvest_cursor.get("worker_ids") or [])),
            sorted([worker_a.worker_id, worker_b.worker_id]),
        )

    def test_handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending(
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
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_inline_harvest_prefetch_callback"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-inline-harvest-prefetch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url = "https://www.linkedin.com/in/openai-inline-builder/"
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
                            candidate_id="openai-inline-builder",
                            name_en="Inline Builder",
                            display_name="Inline Builder",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url,
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
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Inline Builder",
            headline="Platform Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Platform Engineer", "current": True}],
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
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-completed",
            stage="enriching",
            span_name="harvest_profile_batch:inline-completed",
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
        queued_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-pending",
            stage="enriching",
            span_name="harvest_profile_batch:inline-pending",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-inline-pending/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/openai-inline-pending/"],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            completed_worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )
        self.orchestrator.agent_runtime.complete_worker(
            queued_worker,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "queued"}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("inline callback should debounce sync while same-kind worker remains in flight"),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {
                    "worker_id": completed_worker.worker_id,
                    "worker_status": "completed",
                }
            )

        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        refreshed_candidate = list(candidate_doc.get("candidates") or [])[0]
        self.assertIn("harvest_profiles", str(refreshed_candidate.get("source_path") or ""))
        self.assertGreaterEqual(len(list(candidate_doc.get("evidence") or [])), 1)
        refreshed_worker = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert refreshed_worker is not None
        inline_ingest = dict(dict(refreshed_worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_ingest.get("worker_kind") or ""), "harvest_prefetch")
        self.assertEqual(str(inline_ingest.get("sync_status") or ""), "deferred")
        self.assertEqual(str(inline_ingest.get("sync_reason") or ""), "same_kind_background_workers_still_inflight")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        acquisition_progress = dict(dict(refreshed_job.get("summary") or {}).get("acquisition_progress") or {})
        latest_state = dict(acquisition_progress.get("latest_state") or {})
        harvest_cursor = dict(dict(latest_state.get("background_reconcile_cursor") or {}).get("harvest_prefetch") or {})
        self.assertEqual([int(item) for item in list(harvest_cursor.get("worker_ids") or [])], [completed_worker.worker_id])

    def test_refresh_running_workflow_before_retrieval_syncs_missing_materialization_without_background_workers(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find Anthropic people",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_materialization_only"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-pre-retrieval-materialization"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "Anthropic",
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
            return_value={
                "status": "completed",
                "reason": "pre_retrieval_refresh",
                "state_updates": {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                },
            },
        ) as synchronize_snapshot:
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
        self.assertTrue(bool(refresh.get("materialization_refresh_required")))
        synchronize_snapshot.assert_called_once()
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        pre_retrieval_refresh = dict(dict(refreshed_job.get("summary") or {}).get("pre_retrieval_refresh") or {})
        self.assertEqual(pre_retrieval_refresh.get("snapshot_id"), snapshot_dir.name)
        self.assertTrue(bool(pre_retrieval_refresh.get("materialization_refresh_required")))
        self.assertEqual(str(dict(pre_retrieval_refresh.get("sync") or {}).get("status") or ""), "completed")

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

    def test_refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI coding people",
            "target_company": "OpenAI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Coding"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_pre_retrieval_consumed_harvest_workers"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-consumed-harvest-workers"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "OpenAI",
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
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
        )
        (snapshot_dir / "retrieval_index_summary.json").write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
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
            worker_key="harvest_profile_batch::consumed",
            stage="enriching",
            span_name="harvest_profile_batch:consumed",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-consumed/"]},
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
        workers = self.orchestrator.agent_runtime.list_workers(job_id=job_id)
        completed_harvest_workers = [
            worker for worker in workers if str(worker.get("status") or "") == "completed" and worker.get("worker_id")
        ]
        cursor = self.orchestrator._build_background_reconcile_cursor(completed_harvest_workers)
        acquisition_state = _deserialize_acquisition_state_payload(
            _serialize_acquisition_state_payload(
                {
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "background_reconcile_cursor": {"harvest_prefetch": cursor},
                }
            )
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("consumed harvest workers should not trigger a second pre-retrieval sync"),
        ):
            refresh = self.orchestrator._refresh_running_workflow_before_retrieval(
                job_id=job_id,
                request=request,
                plan=plan,
                acquisition_state=acquisition_state,
            )

        self.assertEqual(refresh["status"], "skipped")
        self.assertEqual(refresh["reason"], "no_completed_background_workers")

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

    def test_asset_population_fast_path_context_allows_workflow_snapshot_final_results(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
                "top_k": 10,
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        candidate_source = {
            "source_kind": "company_snapshot",
            "target_company": "Reflection AI",
            "snapshot_id": "snapshot-reflection-fast-path",
            "asset_view": "canonical_merged",
            "source_path": str(
                self.settings.company_assets_dir / "reflectionai" / "snapshot-reflection-fast-path" / "candidate_documents.json"
            ),
            "candidates": [],
            "evidence_lookup": {},
        }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_organization_execution_profile_from_plan_payload",
            return_value={"org_scale_band": "startup"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_build_effective_execution_semantics",
            return_value={"default_results_mode": "asset_population"},
        ):
            context = self.orchestrator._asset_population_fast_path_context(
                request=request,
                plan=plan,
                candidate_source=candidate_source,
                runtime_policy={
                    "workflow_snapshot_id": "snapshot-reflection-fast-path",
                    "analysis_stage": "stage_2_final",
                },
            )

        self.assertEqual(
            str(dict(context.get("effective_execution_semantics") or {}).get("default_results_mode") or ""),
            "asset_population",
        )

    def test_resolve_asset_population_direct_finalization_context_builds_generation_patch_overlay(self) -> None:
        baseline_snapshot_id = "snapshot-reflection-baseline-patch"
        current_snapshot_id = "snapshot-reflection-delta-patch"
        baseline_candidates = [
            Candidate(
                candidate_id="cand-base-1",
                name_en="Baseline One",
                display_name="Baseline One",
                category="employee",
                target_company="Reflection AI",
                organization="Reflection AI",
                employment_status="current",
                role="Research Engineer",
                linkedin_url="https://www.linkedin.com/in/reflection-base-one/",
            ).to_record(),
            Candidate(
                candidate_id="cand-base-2",
                name_en="Baseline Two",
                display_name="Baseline Two",
                category="employee",
                target_company="Reflection AI",
                organization="Reflection AI",
                employment_status="former",
                role="Research Scientist",
                linkedin_url="https://www.linkedin.com/in/reflection-base-two/",
            ).to_record(),
        ]
        current_candidates = [
            Candidate(
                candidate_id="cand-delta-3",
                name_en="Delta Three",
                display_name="Delta Three",
                category="employee",
                target_company="Reflection AI",
                organization="Reflection AI",
                employment_status="current",
                role="Post-train Researcher",
                linkedin_url="https://www.linkedin.com/in/reflection-delta-three/",
            ).to_record()
        ]
        self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id=baseline_snapshot_id,
            candidates=baseline_candidates,
        )
        self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id=current_snapshot_id,
            candidates=current_candidates,
        )
        baseline_generation = self._register_materialization_generation(
            target_company="Reflection AI",
            snapshot_id=baseline_snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=baseline_candidates,
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
                "top_k": 10,
            }
        )
        plan = {
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": True,
                "baseline_snapshot_id": baseline_snapshot_id,
                "baseline_generation_key": str(baseline_generation.get("generation_key") or ""),
                "baseline_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
            },
            "organization_execution_profile": {
                "source_snapshot_id": baseline_snapshot_id,
                "source_generation_key": str(baseline_generation.get("generation_key") or ""),
                "source_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
                "org_scale_band": "startup",
            },
        }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_build_effective_execution_semantics",
            return_value={"default_results_mode": "asset_population", "asset_population_supported": True},
        ):
            context = self.orchestrator._resolve_asset_population_direct_finalization_context(
                request=request,
                plan=plan,
                acquisition_state={"snapshot_id": current_snapshot_id},
            )

        candidate_source = dict(context.get("candidate_source") or {})
        patch_summary = dict(candidate_source.get("asset_population_patch") or {})
        patched_generation = self.store.get_asset_materialization_generation(
            target_company="Reflection AI",
            snapshot_id=current_snapshot_id,
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
        )

        self.assertEqual(context["snapshot_id"], current_snapshot_id)
        self.assertEqual(len(list(candidate_source.get("candidates") or [])), 3)
        self.assertEqual(str(candidate_source.get("authoritative_snapshot_id") or ""), baseline_snapshot_id)
        self.assertEqual(str(patch_summary.get("base_snapshot_id") or ""), baseline_snapshot_id)
        self.assertEqual(set(patch_summary.get("delta_candidate_ids") or []), {"cand-delta-3"})
        self.assertEqual(str(patched_generation.get("generation_key") or ""), str(patch_summary.get("generation_key") or ""))
        self.assertEqual(
            self.store.summarize_asset_membership_index(
                generation_key=str(patched_generation.get("generation_key") or "")
            )["member_count"],
            3,
        )
        self.assertEqual(str(dict(context.get("background_snapshot_materialization") or {}).get("status") or ""), "deferred")

    def test_execute_retrieval_with_candidate_source_override_persists_asset_population_overlay(self) -> None:
        baseline_snapshot_id = "snapshot-openai-baseline-overlay"
        current_snapshot_id = "snapshot-openai-delta-overlay"
        baseline_candidates = [
            Candidate(
                candidate_id="cand-openai-base-1",
                name_en="Base One",
                display_name="Base One",
                category="employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Research Engineer",
                linkedin_url="https://www.linkedin.com/in/openai-base-one/",
            ).to_record(),
            Candidate(
                candidate_id="cand-openai-base-2",
                name_en="Base Two",
                display_name="Base Two",
                category="employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="former",
                role="Research Scientist",
                linkedin_url="https://www.linkedin.com/in/openai-base-two/",
            ).to_record(),
        ]
        current_candidates = [
            Candidate(
                candidate_id="cand-openai-delta-3",
                name_en="Delta Three",
                display_name="Delta Three",
                category="employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Coding Researcher",
                linkedin_url="https://www.linkedin.com/in/openai-delta-three/",
            ).to_record()
        ]
        self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id=baseline_snapshot_id,
            candidates=baseline_candidates,
        )
        self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id=current_snapshot_id,
            candidates=current_candidates,
        )
        baseline_generation = self._register_materialization_generation(
            target_company="OpenAI",
            snapshot_id=baseline_snapshot_id,
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            candidates=baseline_candidates,
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找OpenAI做Coding方向的人",
                "target_company": "OpenAI",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Coding"],
                "top_k": 10,
            }
        )
        plan = {
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": True,
                "baseline_snapshot_id": baseline_snapshot_id,
                "baseline_generation_key": str(baseline_generation.get("generation_key") or ""),
                "baseline_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
            },
            "organization_execution_profile": {
                "source_snapshot_id": baseline_snapshot_id,
                "source_generation_key": str(baseline_generation.get("generation_key") or ""),
                "source_generation_sequence": int(baseline_generation.get("generation_sequence") or 0),
                "org_scale_band": "startup",
            },
        }

        with unittest.mock.patch.object(
            self.orchestrator,
            "_build_effective_execution_semantics",
            return_value={"default_results_mode": "asset_population", "asset_population_supported": True},
        ):
            context = self.orchestrator._resolve_asset_population_direct_finalization_context(
                request=request,
                plan=plan,
                acquisition_state={"snapshot_id": current_snapshot_id},
            )
            artifact = self.orchestrator._execute_retrieval(
                "job_openai_generation_patch_overlay",
                request,
                plan,
                "workflow",
                runtime_policy={"workflow_snapshot_id": current_snapshot_id},
                candidate_source_override=dict(context.get("candidate_source") or {}),
            )

        candidate_source_summary = dict(artifact.get("summary", {}).get("candidate_source") or {})
        overlay_path = str(candidate_source_summary.get("asset_population_overlay_path") or "")
        dashboard = self.orchestrator.get_job_dashboard("job_openai_generation_patch_overlay") or {}
        asset_population = dict(dashboard.get("asset_population") or {})

        self.assertTrue(overlay_path)
        self.assertTrue(Path(overlay_path).exists())
        self.assertEqual(asset_population.get("source_kind"), "job_asset_population_overlay")
        self.assertEqual(int(asset_population.get("candidate_count") or 0), 3)
        self.assertEqual(len(list(asset_population.get("candidates") or [])), 3)

    def test_run_outreach_layering_after_acquisition_reuses_cached_snapshot_analysis(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            }
        )
        snapshot_dir, _ = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-cached-outreach-layering",
            candidates=[
                Candidate(
                    candidate_id="cand_cached_outreach_layering",
                    name_en="Cached Layering",
                    display_name="Cached Layering",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/cached-layering/",
                ).to_record()
            ],
        )
        analysis_dir = snapshot_dir / "layered_segmentation" / "greater_china_outreach_20260422T000000Z"
        analysis_dir.mkdir(parents=True, exist_ok=True)
        (analysis_dir / "layered_analysis.json").write_text(
            json.dumps(
                {
                    "status": "completed",
                    "candidates": [
                        {
                            "candidate_id": "cand_cached_outreach_layering",
                            "final_layer": 1,
                            "final_layer_source": "deterministic",
                        }
                    ],
                    "layers": {
                        "layer_0_roster": {"count": 0},
                        "layer_1_name_signal": {"count": 1},
                        "layer_2_greater_china_region_experience": {"count": 0},
                        "layer_3_mainland_china_experience_or_chinese_language": {"count": 0},
                    },
                    "cumulative_layer_counts": {
                        "layer_0_roster": 1,
                        "layer_1_name_signal": 1,
                    },
                    "ai_verification": {},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id="job_cached_outreach_layering",
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload={"message": "Retrieving"},
        )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.analyze_company_outreach_layers",
            side_effect=AssertionError("cached outreach layering should be reused instead of recomputed"),
        ):
            summary = self.orchestrator._run_outreach_layering_after_acquisition(
                job_id="job_cached_outreach_layering",
                request=request,
                acquisition_state={"snapshot_id": snapshot_dir.name},
                allow_ai=False,
                analysis_stage_label="stage_2_final",
                event_stage="retrieving",
            )

        self.assertEqual(summary["status"], "completed")
        self.assertTrue(str(dict(summary.get("analysis_paths") or {}).get("full") or "").endswith("layered_analysis.json"))
        self.assertEqual(int(summary.get("candidate_count") or 0), 1)

    def test_run_outreach_layering_after_acquisition_defers_full_asset_population_without_cache(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            }
        )
        snapshot_dir, _ = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-outreach-layering-deferred",
            candidates=[
                Candidate(
                    candidate_id="cand_deferred_outreach_layering",
                    name_en="Deferred Layering",
                    display_name="Deferred Layering",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                ).to_record()
            ],
        )
        self.store.save_job(
            job_id="job_deferred_outreach_layering",
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload=request.to_record(),
            plan_payload={},
            summary_payload={"message": "Retrieving"},
        )

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.analyze_company_outreach_layers",
            side_effect=AssertionError("full asset population finalization should defer uncached outreach layering"),
        ):
            summary = self.orchestrator._run_outreach_layering_after_acquisition(
                job_id="job_deferred_outreach_layering",
                request=request,
                acquisition_state={"snapshot_id": snapshot_dir.name},
                allow_ai=False,
                analysis_stage_label="stage_2_final",
                event_stage="retrieving",
            )

        self.assertEqual(summary["status"], "deferred")
        self.assertEqual(summary["reason"], "deferred_for_asset_population_fast_path")

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

    def test_earliest_timestamp_string_prefers_stage_transition_start(self) -> None:
        self.assertEqual(
            _earliest_timestamp_string(
                "2026-04-22T03:31:39Z",
                "2026-04-22T03:26:04Z",
                "",
            ),
            "2026-04-22T03:26:04Z",
        )

    def test_restore_completed_workflow_stage_summary_contract_keeps_latest_completed_at(self) -> None:
        restored = self.orchestrator._restore_completed_workflow_stage_summary_contract(
            {
                "analysis_stage": "stage_2_final",
                "status": "completed",
                "started_at": "2026-04-24T05:51:25Z",
                "completed_at": "2026-04-24T05:50:20Z",
            },
            {
                "stage_2_final_started_at": "2026-04-24T05:51:25Z",
                "completed_at": "2026-04-24T05:51:44Z",
                "public_web_stage_2": {"completed_at": "2026-04-24T05:50:21Z"},
            },
        )

        self.assertEqual(restored["started_at"], "2026-04-24T05:50:21Z")
        self.assertEqual(restored["completed_at"], "2026-04-24T05:51:44Z")

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

    def test_resume_blocked_workflow_after_workers_complete(self) -> None:
        request_payload = {
            "raw_user_request": "Find former xAI employees",
            "target_company": "xAI",
            "categories": ["former_employee"],
            "employment_statuses": ["former"],
            "organization_keywords": ["xAI"],
            "top_k": 1,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_resume_blocked"
        snapshot_dir = self.settings.company_assets_dir / "xai" / "snapshot-resume"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="blocked",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Waiting for queued workers", "blocked_task": "acquire_full_roster"},
        )

        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="relationship_web::01",
            stage="acquiring",
            span_name="search_bundle:relationship_web",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {
                    "bundle_id": "relationship_web",
                    "query": "xAI former employee",
                    "source_family": "public_web_search",
                },
                "query": "xAI former employee",
                "index": 1,
            },
            metadata={
                "index": 1,
                "identity": CompanyIdentity(
                    requested_name="xAI",
                    canonical_name="xAI",
                    company_key="xai",
                    linkedin_slug="xai",
                    linkedin_company_url="https://www.linkedin.com/company/xai/",
                ).to_record(),
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "former",
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
                "result_limit": 10,
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "raw_path": str(discovery_dir / "web_query_01.json")},
            output_payload={
                "summary": {"query": "xAI former employee", "status": "completed"},
                "entries": [],
                "errors": [],
            },
        )

        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="cand_resume_1",
                    name_en="Former XAI Engineer",
                    display_name="Former XAI Engineer",
                    category="former_employee",
                    target_company="xAI",
                    organization="xAI",
                    employment_status="former",
                    role="Engineer",
                    focus_areas="systems",
                )
            ],
            [
                EvidenceRecord(
                    evidence_id=make_evidence_id(
                        "cand_resume_1", "seed", "xAI former employee", "https://example.com/xai"
                    ),
                    candidate_id="cand_resume_1",
                    source_type="web_search",
                    title="xAI former employee",
                    url="https://example.com/xai",
                    summary="Synthetic evidence for resume test.",
                    source_dataset="test",
                    source_path=str(snapshot_dir / "synthetic_evidence.json"),
                )
            ],
        )

        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
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
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertGreaterEqual(len(snapshot["results"]), 1)

    def test_acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists(self) -> None:
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
        plan = hydrate_sourcing_plan(plan_payload)
        job_id = "job_resume_readiness_background_roster"
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-search-baseline-ready"
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
                "message": "Waiting for background roster worker.",
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
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::reflectionai",
            stage="acquiring",
            span_name="harvest_company_employees:Reflection AI",
            budget_payload={"max_pages": 100, "page_limit": 25},
            input_payload={"company_identity": identity.to_record()},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "identity": identity.to_record(),
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )

        job = self.store.get_job(job_id)
        assert job is not None
        workers = self.orchestrator.agent_runtime.list_workers(job_id=job_id)
        readiness = self.orchestrator._assess_acquisition_resume_readiness(
            job=job,
            blocked_task="acquire_full_roster",
            workers=workers,
        )

        self.assertEqual(readiness["status"], "ready")
        self.assertTrue(bool(readiness["baseline_ready"]))
        self.assertEqual(readiness["baseline_reason"], "search_seed_entries_present")
        self.assertEqual(int(readiness["pending_worker_count"] or 0), 1)
        self.assertEqual(len(list(readiness["pending_workers"] or [])), 1)
        self.assertEqual(
            next(
                task.task_type
                for task in list(plan.acquisition_tasks)
                if str(task.task_type or "") == "acquire_full_roster"
            ),
            "acquire_full_roster",
        )

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
        expected_task_types.extend(["normalize_asset_snapshot", "build_retrieval_index"])
        self.assertEqual(
            executed_task_types,
            expected_task_types,
        )
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")
        self.assertGreaterEqual(len(snapshot["results"]), 1)

    def test_resume_running_planning_workflow_continues_from_acquisition_when_planning_completed(self) -> None:
        request_payload = {
            "raw_user_request": "Find Reflection AI infra members",
            "target_company": "Reflection AI",
            "categories": ["employee", "former_employee"],
            "keywords": ["infra"],
            "top_k": 3,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow({**request_payload, "skip_plan_review": True})["plan"]
        job_id = "job_resume_running_planning"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Workflow is running"},
        )
        self.store.append_job_event(job_id, "planning", "queued", "Workflow created from user request.")
        self.store.append_job_event(job_id, "planning", "completed", "Planning stage completed.")

        original_run_from_acquisition = self.orchestrator._run_workflow_from_acquisition
        captured: dict[str, object] = {}

        def fake_run_from_acquisition(job_id_arg, request_arg, plan_arg, *, resume_mode=False):
            captured["job_id"] = job_id_arg
            captured["resume_mode"] = resume_mode
            captured["request"] = request_arg
            captured["plan"] = plan_arg
            self.store.save_job(
                job_id=job_id_arg,
                job_type="workflow",
                status="completed",
                stage="completed",
                request_payload=request_arg.to_record(),
                plan_payload=plan_arg.to_record(),
                summary_payload={"message": "Recovered from stale planning runner."},
            )
            return {"status": "completed"}

        self.orchestrator._run_workflow_from_acquisition = fake_run_from_acquisition
        try:
            resume = self.orchestrator._resume_planning_workflow_if_ready(job_id)
        finally:
            self.orchestrator._run_workflow_from_acquisition = original_run_from_acquisition

        snapshot = self.orchestrator.get_job_results(job_id)
        self.assertEqual(resume["status"], "resumed")
        self.assertEqual(resume["resume_mode"], "running_planning_to_acquisition_recovery")
        self.assertTrue(bool(resume["planning_completed"]))
        self.assertEqual(captured["job_id"], job_id)
        self.assertTrue(bool(captured["resume_mode"]))
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot["job"]["status"], "completed")

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

        self.store.replace_manual_review_items("job_old", [old_item, old_other_item])
        self.store.replace_manual_review_items("job_new", [new_item])

        all_items = self.store.list_manual_review_items(target_company="Acme", status="", limit=10)
        status_by_candidate = {(item["candidate_id"], item["job_id"]): item["status"] for item in all_items}
        snapshot_by_candidate = {
            (item["candidate_id"], item["job_id"]): item["metadata"].get("snapshot_id") for item in all_items
        }
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_old")], "20260406T120000")
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_new")], "20260407T120000")
        self.assertEqual(status_by_candidate[("cand_manual", "job_old")], "superseded")
        self.assertEqual(status_by_candidate[("cand_manual", "job_new")], "open")

        cleanup = self.store.cleanup_manual_review_items(target_company="Acme", snapshot_id="20260407T120000")
        self.assertGreaterEqual(cleanup["out_of_scope_count"], 1)
        open_items = self.store.list_manual_review_items(target_company="Acme", status="open", limit=10)
        self.assertEqual(len(open_items), 1)
        self.assertEqual(open_items[0]["candidate_id"], "cand_manual")
        self.assertEqual(open_items[0]["metadata"].get("snapshot_id"), "20260407T120000")

    def test_investor_firm_workflow_uses_tiered_firm_roster(self) -> None:
        self.orchestrator.bootstrap()
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找出Anthropic投资机构里与Anthropic投资直接相关的全量成员，再后续筛选决策相关角色。",
                "target_company": "Anthropic",
                "categories": ["investor"],
                "keywords": ["Anthropic投资", "决策"],
            }
        )
        self.assertEqual(plan_result["plan"]["acquisition_strategy"]["strategy_type"], "investor_firm_roster")
        review_id = plan_result["plan_review_session"]["review_id"]
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {},
            }
        )
        workflow = self.orchestrator.run_workflow_blocking({"plan_review_id": review_id})
        self.assertEqual(workflow["job"]["status"], "completed")
        self.assertGreaterEqual(len(workflow["results"]), 1)
        self.assertGreaterEqual(workflow["job"]["summary"].get("manual_review_queue_count", 0), 0)

    def test_investor_firm_roster_uses_snapshot_assets_without_sqlite_fallback(self) -> None:
        snapshot_id = "20260418T010101"
        target_company = "Acme Capital"
        snapshot_dir, _ = self._write_company_snapshot_candidate_documents(
            target_company=target_company,
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_investor_1",
                    name_en="Dana Partner",
                    display_name="Dana Partner",
                    category="investor",
                    target_company=target_company,
                    organization="North Ridge Ventures",
                    employment_status="current",
                    role="Partner",
                    focus_areas="AI infrastructure investing",
                    linkedin_url="https://www.linkedin.com/in/dana-partner/",
                ).to_record()
            ],
        )
        task = AcquisitionTask(
            task_id="acquire-investor-firm-roster",
            task_type="acquire_full_roster",
            title="Acquire investor firm roster",
            description="Use snapshot-authoritative investor members to build firm roster.",
            metadata={"strategy_type": "investor_firm_roster"},
        )
        job_request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Acme Capital投资机构里做AI基础设施方向的决策成员。",
                "target_company": target_company,
                "categories": ["investor"],
                "keywords": ["AI基础设施", "决策"],
            }
        )
        state = {
            "snapshot_id": snapshot_id,
            "snapshot_dir": snapshot_dir,
            "company_identity": CompanyIdentity(
                requested_name=target_company,
                canonical_name=target_company,
                company_key=normalize_company_key(target_company),
                linkedin_slug=normalize_company_key(target_company),
                local_asset_available=True,
            ),
        }

        original_list_candidates = self.store.list_candidates
        original_list_evidence = self.store.list_evidence

        def fail_list_candidates(*args, **kwargs):  # noqa: ARG001
            raise AssertionError("unexpected sqlite candidate fallback")

        def fail_list_evidence(*args, **kwargs):  # noqa: ARG001
            raise AssertionError("unexpected sqlite evidence fallback")

        self.store.list_candidates = fail_list_candidates
        self.store.list_evidence = fail_list_evidence
        try:
            execution = self.acquisition_engine.execute_task(task, job_request, target_company, state)
        finally:
            self.store.list_candidates = original_list_candidates
            self.store.list_evidence = original_list_evidence

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["strategy_type"], "investor_firm_roster")
        self.assertEqual(execution.payload["candidate_count"], 1)
        self.assertEqual(len(execution.state_updates["candidates"]), 1)
        self.assertEqual(execution.state_updates["candidates"][0].candidate_id, "cand_investor_1")

    def test_feedback_triggers_criteria_recompile(self) -> None:
        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找 xAI 的 RL researcher",
                "target_company": "xAI",
                "keywords": ["RL"],
            }
        )
        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "accepted_alias",
                "subject": "RL",
                "value": "reinforcement learning",
            }
        )
        self.assertEqual(feedback_result["status"], "recorded")
        self.assertEqual(feedback_result["recompile"]["status"], "recompiled")
        self.assertEqual(feedback_result["recompile"]["base_version_id"], plan_result["criteria_version_id"])
        self.assertGreater(feedback_result["recompile"]["criteria_version_id"], plan_result["criteria_version_id"])
        versions = self.store.list_criteria_versions(target_company="xAI", limit=2)
        self.assertEqual(versions[0]["evolution_stage"], "feedback_recompiled")
        self.assertEqual(versions[0]["parent_version_id"], plan_result["criteria_version_id"])
        runs = self.store.list_criteria_compiler_runs(target_company="xAI", limit=2)
        self.assertEqual(runs[0]["compiler_kind"], "feedback_recompile")

    def test_feedback_rerun_produces_result_diff(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="cand_rl_1",
                    name_en="Jane RL",
                    display_name="Jane RL",
                    category="employee",
                    target_company="TestCo",
                    organization="TestCo",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="reinforcement learning systems",
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "TestCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["RL"],
                "top_k": 5,
            }
        )
        self.assertEqual(initial["summary"]["total_matches"], 0)

        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "target_company": "TestCo",
                "feedback_type": "accepted_alias",
                "subject": "RL",
                "value": "reinforcement learning",
                "rerun_retrieval": True,
            }
        )
        self.assertEqual(feedback_result["rerun"]["status"], "completed")
        self.assertEqual(feedback_result["rerun"]["baseline_job_id"], initial["job_id"])
        self.assertEqual(feedback_result["rerun"]["policy"]["mode"], "cheap")
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["added_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["rerun_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["added_pattern_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["added"][0]["display_name"], "Jane RL")
        self.assertEqual(
            feedback_result["rerun"]["diff"]["rule_changes"]["pattern_changes"]["added"][0]["value"],
            "reinforcement learning",
        )
        self.assertIn(
            "Criteria changes materially affected retrieval output",
            feedback_result["rerun"]["diff"]["impact_explanations"][0],
        )
        self.assertEqual(feedback_result["rerun"]["diff"]["candidate_impacts"]["summary"]["candidate_impact_count"], 1)
        self.assertEqual(
            feedback_result["rerun"]["diff"]["candidate_impacts"]["summary"]["attributed_candidate_count"], 1
        )
        self.assertEqual(
            feedback_result["rerun"]["diff"]["candidate_impacts"]["items"][0]["rule_triggers"][0]["pattern_type"],
            "alias",
        )
        self.assertIn(
            "Entered results because",
            feedback_result["rerun"]["diff"]["candidate_impacts"]["items"][0]["attribution_explanation"],
        )
        diffs = self.store.list_criteria_result_diffs(target_company="TestCo", limit=5)
        self.assertEqual(len(diffs), 1)
        self.assertEqual(diffs[0]["baseline_job_id"], initial["job_id"])
        self.assertEqual(diffs[0]["summary"]["added_pattern_count"], 1)
        self.assertEqual(feedback_result["rerun"]["rerun_result"]["summary"]["summary_provider"], "deterministic")

    def test_low_signal_feedback_can_be_gated_off(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="cand1",
                    name_en="Alice",
                    display_name="Alice",
                    category="employee",
                    target_company="GateCo",
                    organization="GateCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="distributed systems",
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "GateCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["systems"],
                "top_k": 5,
            }
        )
        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "target_company": "GateCo",
                "feedback_type": "note_only",
                "subject": "observation",
                "value": "check later",
                "rerun_retrieval": True,
            }
        )
        self.assertEqual(feedback_result["rerun"]["status"], "gated_off")
        self.assertEqual(feedback_result["rerun"]["policy"]["estimated_cost_tier"], "none")

    def test_recompile_rerun_selects_baseline_by_request_family(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="family_a",
                    name_en="Alice Systems",
                    display_name="Alice Systems",
                    category="employee",
                    target_company="FamilyCo",
                    organization="FamilyCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="distributed systems platform",
                ),
                Candidate(
                    candidate_id="family_b",
                    name_en="Bob Inference",
                    display_name="Bob Inference",
                    category="employee",
                    target_company="FamilyCo",
                    organization="FamilyCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="inference runtime",
                ),
            ],
            [],
        )
        systems_job = self.orchestrator.run_job(
            {
                "target_company": "FamilyCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["systems"],
                "top_k": 5,
            }
        )
        inference_job = self.orchestrator.run_job(
            {
                "target_company": "FamilyCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["inference"],
                "top_k": 5,
            }
        )
        self.assertNotEqual(systems_job["job_id"], inference_job["job_id"])
        self.assertEqual(inference_job["matches"][0]["display_name"], "Bob Inference")

        rerun = self.orchestrator.recompile_criteria(
            {
                "request": {
                    "target_company": "FamilyCo",
                    "categories": ["employee"],
                    "employment_statuses": ["current"],
                    "keywords": ["systems"],
                    "top_k": 5,
                },
                "rerun_retrieval": "full",
            }
        )
        self.assertEqual(rerun["rerun"]["status"], "completed")
        self.assertEqual(rerun["rerun"]["baseline_job_id"], systems_job["job_id"])
        self.assertEqual(
            rerun["rerun"]["baseline_selection"]["matched_request_family_signature"],
            rerun["rerun"]["baseline_selection"]["request_family_signature"],
        )
        self.assertIn("exact request signature match", rerun["rerun"]["baseline_selection"]["reason"].lower())

    def test_confidence_evolution_changes_label_on_rerun(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="conf_1",
                    name_en="Jane Research",
                    display_name="Jane Research",
                    category="employee",
                    target_company="ConfCo",
                    organization="ConfCo",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="systems platform",
                    linkedin_url="https://linkedin.com/in/jane-research",
                    metadata={"exploration_links": ["https://example.com/jane"]},
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "ConfCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["research"],
                "top_k": 5,
            }
        )
        self.assertEqual(initial["matches"][0]["confidence_label"], "medium")

        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "candidate_id": "conf_1",
                "target_company": "ConfCo",
                "feedback_type": "must_have_signal",
                "subject": "research",
                "value": "research engineer",
                "rerun_retrieval": "full",
            }
        )
        self.assertEqual(feedback_result["rerun"]["status"], "completed")
        self.assertGreaterEqual(len(feedback_result["suggestions"]), 1)
        self.assertEqual(feedback_result["suggestions"][0]["status"], "suggested")
        self.assertEqual(feedback_result["rerun"]["rerun_result"]["matches"][0]["confidence_label"], "high")
        self.assertEqual(feedback_result["rerun"]["diff"]["summary"]["moved_count"], 1)
        self.assertEqual(feedback_result["rerun"]["diff"]["moved"][0]["old_confidence_label"], "medium")
        self.assertEqual(feedback_result["rerun"]["diff"]["moved"][0]["new_confidence_label"], "high")
        self.assertGreaterEqual(feedback_result["rerun"]["rerun_result"]["confidence_policy"]["high_threshold"], 0.63)
        self.assertEqual(feedback_result["rerun"]["rerun_result"]["confidence_policy"]["scope_kind"], "request_family")
        feedback_rows = self.store.list_criteria_feedback(target_company="ConfCo", limit=5)
        self.assertTrue(feedback_rows[0]["metadata"].get("request_family_signature"))
        self.assertTrue(feedback_rows[0]["metadata"].get("matching_request_family_signature"))
        self.assertTrue(feedback_rows[0]["metadata"].get("request_matching"))
        runs = self.store.list_confidence_policy_runs(target_company="ConfCo", limit=5)
        self.assertGreaterEqual(len(runs), 2)
        self.assertEqual(runs[0]["job_id"], feedback_result["rerun"]["rerun_job_id"])
        self.assertEqual(runs[0]["scope_kind"], "request_family")
        self.assertTrue(runs[0]["request_family_signature"])
        self.assertTrue(runs[0]["matching_request_family_signature"])
        criteria_snapshot = self.orchestrator.list_criteria_patterns("ConfCo")
        self.assertGreaterEqual(len(criteria_snapshot["suggestions"]), 1)
        self.assertTrue(criteria_snapshot["suggestions"][0]["matching_request_family_signature"])

    def test_suggestion_review_applies_pattern_and_recompiles(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="review_1",
                    name_en="Maya Systems",
                    display_name="Maya Systems",
                    category="employee",
                    target_company="ReviewCo",
                    organization="ReviewCo",
                    employment_status="current",
                    role="Engineer",
                    team="Training Infrastructure",
                    focus_areas="distributed training systems",
                    linkedin_url="https://linkedin.com/in/maya-systems",
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(
            {
                "target_company": "ReviewCo",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["infra"],
                "top_k": 5,
            }
        )
        feedback_result = self.orchestrator.record_criteria_feedback(
            {
                "job_id": initial["job_id"],
                "candidate_id": "review_1",
                "target_company": "ReviewCo",
                "feedback_type": "false_negative_pattern",
                "subject": "infra",
                "value": "training infrastructure",
            }
        )
        must_signal_suggestion = next(
            item for item in feedback_result["suggestions"] if item["pattern_type"] == "must_signal"
        )
        review_result = self.orchestrator.review_pattern_suggestion(
            {
                "suggestion_id": must_signal_suggestion["suggestion_id"],
                "action": "apply",
                "reviewer": "human",
                "notes": "This suggestion should become an active filter.",
                "rerun_retrieval": "full",
            }
        )
        self.assertEqual(review_result["status"], "reviewed")
        self.assertEqual(review_result["suggestion"]["status"], "applied")
        self.assertIsNotNone(review_result["applied_pattern"])
        self.assertEqual(review_result["applied_pattern"]["pattern_type"], "must_signal")
        self.assertEqual(review_result["recompile"]["status"], "recompiled")
        self.assertEqual(review_result["rerun"]["status"], "completed")
        active_patterns = self.store.list_criteria_patterns(target_company="ReviewCo", status="active", limit=50)
        self.assertTrue(
            any(item["pattern_id"] == review_result["applied_pattern"]["pattern_id"] for item in active_patterns)
        )

    def test_manual_confidence_policy_freeze_blocks_auto_band_shift(self) -> None:
        request_payload = {
            "target_company": "FreezeCo",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
            "top_k": 5,
        }
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="freeze_1",
                    name_en="Freeze Candidate",
                    display_name="Freeze Candidate",
                    category="employee",
                    target_company="FreezeCo",
                    organization="FreezeCo",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="systems platform",
                    linkedin_url="https://linkedin.com/in/freeze-candidate",
                    metadata={"publication_id": "paper-1"},
                )
            ],
            [],
        )
        initial = self.orchestrator.run_job(request_payload)
        self.assertEqual(initial["matches"][0]["confidence_label"], "medium")
        freeze_result = self.orchestrator.configure_confidence_policy(
            {
                "action": "freeze_current",
                "target_company": "FreezeCo",
                "request_payload": request_payload,
                "scope_kind": "request_family",
                "reviewer": "human",
                "notes": "Freeze this family before more feedback arrives.",
            }
        )
        self.assertEqual(freeze_result["status"], "configured")
        self.assertEqual(freeze_result["control"]["control_mode"], "freeze")

        for idx in range(5):
            self.orchestrator.record_criteria_feedback(
                {
                    "target_company": "FreezeCo",
                    "request_payload": request_payload,
                    "feedback_type": "false_negative_pattern",
                    "subject": f"unrelated-{idx}",
                    "value": f"multimodal vision stack {idx}",
                }
            )

        frozen_run = self.orchestrator.run_job(request_payload)
        self.assertEqual(frozen_run["matches"][0]["confidence_label"], "medium")
        self.assertEqual(frozen_run["confidence_policy_control"]["control_mode"], "freeze")

        cleared = self.orchestrator.configure_confidence_policy(
            {
                "action": "clear",
                "target_company": "FreezeCo",
                "request_payload": request_payload,
                "scope_kind": "request_family",
            }
        )
        self.assertEqual(cleared["status"], "cleared")
        unfrozen_run = self.orchestrator.run_job(request_payload)
        self.assertEqual(unfrozen_run["matches"][0]["confidence_label"], "high")
        self.assertLess(unfrozen_run["confidence_policy"]["high_threshold"], 0.75)

    def test_http_api_smoke(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            bootstrap_req = urllib_request.Request(
                f"http://{host}:{port}/api/bootstrap",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(bootstrap_req) as response:
                bootstrap_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(bootstrap_resp["status"], "bootstrapped")

            plan_payload = json.dumps(
                {
                    "raw_user_request": "帮我为 xAI 设计一个 sourcing 工作流，先拿全量数据资产，再决定检索方式。",
                    "target_company": "xAI",
                },
                ensure_ascii=False,
            ).encode("utf-8")
            plan_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan",
                data=plan_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(plan_req) as response:
                plan_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(plan_resp["plan"]["target_company"], "xAI")
            self.assertIn("intent_rewrite", plan_resp)
            self.assertIn("request", plan_resp["intent_rewrite"])
            compile_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan/review/compile-instruction",
                data=json.dumps(
                    {
                        "review_id": plan_resp["plan_review_session"]["review_id"],
                        "reviewer": "tester",
                        "instruction": "改成 full company roster，不允许高成本 source。",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(compile_review_req) as response:
                compile_review_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(compile_review_resp["status"], "compiled")
            self.assertIn("review_payload", compile_review_resp)
            self.assertIn("instruction_compiler", compile_review_resp)
            self.assertIn("intent_rewrite", compile_review_resp)
            self.assertIn("instruction", compile_review_resp["intent_rewrite"])
            plan_reviews_req = urllib_request.Request(
                f"http://{host}:{port}/api/plan/reviews",
                method="GET",
            )
            with opener.open(plan_reviews_req) as response:
                plan_reviews_resp = json.loads(response.read().decode("utf-8"))
            self.assertGreaterEqual(len(plan_reviews_resp["plan_reviews"]), 1)

            job_payload = json.dumps(
                {
                    "target_company": "Anthropic",
                    "categories": ["investor"],
                    "keywords": ["领投", "决策", "Anthropic投资"],
                    "top_k": 3,
                },
                ensure_ascii=False,
            ).encode("utf-8")
            job_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs",
                data=job_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(job_req) as response:
                job_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(job_resp["status"], "completed")
            self.assertGreaterEqual(len(job_resp["matches"]), 1)
            self.assertIn("confidence_label", job_resp["matches"][0])

            result_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/results",
                method="GET",
            )
            with opener.open(result_req) as response:
                result_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(result_resp["job"]["job_id"], job_resp["job_id"])
            self.assertTrue(result_resp["agent_runtime_session"])
            self.assertIn("agent_workers", result_resp)
            self.assertIn("intent_rewrite", result_resp)
            self.assertIn("workflow_stage_summaries", result_resp)

            refine_compile_req = urllib_request.Request(
                f"http://{host}:{port}/api/results/refine/compile-instruction",
                data=json.dumps(
                    {
                        "job_id": job_resp["job_id"],
                        "instruction": "top 1",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(refine_compile_req) as response:
                refine_compile_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(refine_compile_resp["status"], "compiled")
            self.assertEqual(refine_compile_resp["request_patch"]["top_k"], 1)
            self.assertEqual(refine_compile_resp["baseline_job_id"], job_resp["job_id"])
            self.assertIn("intent_rewrite", refine_compile_resp)

            refine_req = urllib_request.Request(
                f"http://{host}:{port}/api/results/refine",
                data=json.dumps(
                    {
                        "job_id": job_resp["job_id"],
                        "instruction": "top 1",
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(refine_req) as response:
                refine_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(refine_resp["status"], "completed")
            self.assertEqual(refine_resp["baseline_job_id"], job_resp["job_id"])
            self.assertEqual(refine_resp["request_patch"]["top_k"], 1)
            self.assertEqual(refine_resp["rerun_result"]["summary"]["returned_matches"], 1)

            progress_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/progress",
                method="GET",
            )
            with opener.open(progress_req) as response:
                progress_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(progress_resp["job_id"], job_resp["job_id"])
            self.assertIn("progress", progress_resp)
            self.assertIn("counters", progress_resp["progress"])

            worker_handle = self.orchestrator.agent_runtime.begin_worker(
                job_id=job_resp["job_id"],
                request=JobRequest.from_payload(self.orchestrator.get_job(job_resp["job_id"])["request"]),
                plan_payload=job_resp["plan"],
                runtime_mode="workflow",
                lane_id="exploration_specialist",
                worker_key="candidate::demo",
                stage="enriching",
                span_name="explore_candidate:demo",
                budget_payload={"max_queries": 2},
                input_payload={"candidate_id": "cand_demo"},
                handoff_from_lane="public_media_specialist",
            )
            self.orchestrator.agent_runtime.checkpoint_worker(
                worker_handle,
                checkpoint_payload={"completed_queries": ["1"]},
                output_payload={"explored_query_count": 1},
            )

            worker_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/workers",
                method="GET",
            )
            with opener.open(worker_req) as response:
                worker_resp = json.loads(response.read().decode("utf-8"))
            self.assertGreaterEqual(len(worker_resp["agent_workers"]), 1)

            scheduler_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_resp['job_id']}/scheduler",
                method="GET",
            )
            with opener.open(scheduler_req) as response:
                scheduler_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("scheduler", scheduler_resp)
            self.assertIn("resumable_workers", scheduler_resp["scheduler"])

            recoverable_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/recoverable",
                method="GET",
            )
            with opener.open(recoverable_req) as response:
                recoverable_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("recoverable_workers", recoverable_resp)
            self.assertIn("count", recoverable_resp)

            interrupt_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/interrupt",
                data=json.dumps({"worker_id": worker_handle.worker_id}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(interrupt_req) as response:
                interrupt_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(interrupt_resp["status"], "interrupt_requested")

            daemon_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/daemon/run-once",
                data=json.dumps({"owner_id": "test-daemon", "total_limit": 1}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(daemon_req) as response:
                daemon_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(daemon_resp["status"], "completed")
            self.assertIn("daemon", daemon_resp)

            daemon_status_req = urllib_request.Request(
                f"http://{host}:{port}/api/workers/daemon/status",
                method="GET",
            )
            with opener.open(daemon_status_req) as response:
                daemon_status_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", daemon_status_resp)
            self.assertIn("service_name", daemon_status_resp)

            manual_review_items = self.store.replace_manual_review_items(
                job_resp["job_id"],
                [
                    {
                        "candidate_id": str(job_resp["matches"][0]["candidate_id"]),
                        "target_company": "Anthropic",
                        "review_type": "manual_identity_resolution",
                        "priority": "high",
                        "status": "open",
                        "summary": "Conflicting public and profile evidence needs synthesis.",
                        "candidate": dict(job_resp["matches"][0]),
                        "evidence": [
                            EvidenceRecord(
                                evidence_id=make_evidence_id(
                                    str(job_resp["matches"][0]["candidate_id"]),
                                    "linkedin_profile_detail",
                                    "LinkedIn profile",
                                    str(
                                        job_resp["matches"][0].get("linkedin_url") or "https://linkedin.com/in/example"
                                    ),
                                ),
                                candidate_id=str(job_resp["matches"][0]["candidate_id"]),
                                source_type="linkedin_profile_detail",
                                title="LinkedIn profile",
                                url=str(
                                    job_resp["matches"][0].get("linkedin_url") or "https://linkedin.com/in/example"
                                ),
                                summary="LinkedIn profile exists, but target-company relationship still needs confirmation.",
                                source_dataset="linkedin_profile_detail",
                                source_path="/tmp/linkedin-profile.json",
                            ).to_record(),
                            {
                                "evidence_id": "ev_public_manual_smoke",
                                "candidate_id": str(job_resp["matches"][0]["candidate_id"]),
                                "source_type": "public_web_search",
                                "title": "Public web mention",
                                "url": "https://example.com/manual-review-smoke",
                                "summary": "A public page references the candidate alongside Anthropic-related work.",
                                "source_dataset": "public_web_search",
                                "source_path": "/tmp/public-search.json",
                            },
                        ],
                        "metadata": {
                            "reasons": ["suspicious_membership"],
                            "confidence_label": "medium",
                            "confidence_score": 0.61,
                            "explanation": "Signals are promising, but the evidence needs a human-authored conclusion.",
                        },
                    }
                ],
            )
            review_item_id = int(manual_review_items[0]["review_item_id"])

            manual_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review",
                method="GET",
            )
            with opener.open(manual_review_req) as response:
                manual_review_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("manual_review_items", manual_review_resp)
            self.assertGreaterEqual(len(manual_review_resp["manual_review_items"]), 1)

            synthesize_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review/synthesize",
                data=json.dumps({"review_item_id": review_item_id}, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(synthesize_req) as response:
                synthesize_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(synthesize_resp["status"], "compiled")
            self.assertEqual(synthesize_resp["review_item_id"], review_item_id)
            self.assertIn("summary", synthesize_resp["synthesis"])
            self.assertEqual(
                synthesize_resp["manual_review_item"]["metadata"]["manual_review_synthesis"]["summary"],
                synthesize_resp["synthesis"]["summary"],
            )

            synthesize_cached_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review/synthesize",
                data=json.dumps({"review_item_id": review_item_id}, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(synthesize_cached_req) as response:
                synthesize_cached_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(synthesize_cached_resp["status"], "cached")
            self.assertEqual(synthesize_cached_resp["review_item_id"], review_item_id)

            provider_health_req = urllib_request.Request(
                f"http://{host}:{port}/api/providers/health",
                method="GET",
            )
            with opener.open(provider_health_req) as response:
                provider_health = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", provider_health)
            self.assertIn("semantic", provider_health["providers"])

            runtime_health_req = urllib_request.Request(
                f"http://{host}:{port}/api/runtime/health",
                method="GET",
            )
            with opener.open(runtime_health_req) as response:
                runtime_health = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", runtime_health)
            self.assertIn("metrics", runtime_health)
            self.assertIn("services", runtime_health)

            runtime_metrics_req = urllib_request.Request(
                f"http://{host}:{port}/api/runtime/metrics",
                method="GET",
            )
            with opener.open(runtime_metrics_req) as response:
                runtime_metrics = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", runtime_metrics)
            self.assertIn("metrics", runtime_metrics)
            self.assertIn("refresh_metrics", runtime_metrics)

            runtime_progress_req = urllib_request.Request(
                f"http://{host}:{port}/api/runtime/progress",
                method="GET",
            )
            with opener.open(runtime_progress_req) as response:
                runtime_progress = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", runtime_progress)
            self.assertIn("workflow_jobs", runtime_progress)
            self.assertIn("profile_registry", runtime_progress)
            self.assertIn("object_sync", runtime_progress)
            self.assertIn("runtime", runtime_progress)

            health_req = urllib_request.Request(
                f"http://{host}:{port}/health",
                method="GET",
            )
            with opener.open(health_req) as response:
                health_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("status", health_resp)
            self.assertIn("metrics", health_resp)

            feedback_payload = json.dumps(
                {
                    "target_company": "Anthropic",
                    "feedback_type": "accepted_alias",
                    "subject": "华人",
                    "value": "Chinese",
                },
                ensure_ascii=False,
            ).encode("utf-8")
            feedback_req = urllib_request.Request(
                f"http://{host}:{port}/api/criteria/feedback",
                data=feedback_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(feedback_req) as response:
                feedback_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(feedback_resp["status"], "recorded")
            self.assertEqual(feedback_resp["recompile"]["status"], "recompiled")

            pattern_req = urllib_request.Request(
                f"http://{host}:{port}/api/criteria/patterns",
                method="GET",
            )
            with opener.open(pattern_req) as response:
                pattern_resp = json.loads(response.read().decode("utf-8"))
            self.assertGreaterEqual(len(pattern_resp["patterns"]), 1)
            self.assertGreaterEqual(len(pattern_resp["versions"]), 1)
            self.assertGreaterEqual(len(pattern_resp["compiler_runs"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_light_routes_are_classified_for_reserved_lane(self) -> None:
        self.assertEqual(_request_priority_lane("OPTIONS", "/api/jobs"), "light")
        self.assertEqual(_request_priority_lane("GET", "/health"), "light")
        self.assertEqual(_request_priority_lane("GET", "/api/frontend-history/history-1"), "light")
        self.assertEqual(_request_priority_lane("GET", "/api/jobs/job123/dashboard"), "light")
        self.assertEqual(_request_priority_lane("GET", "/api/jobs/job123/progress"), "light")
        self.assertEqual(_request_priority_lane("POST", "/api/plan/submit"), "light")
        self.assertEqual(_request_priority_lane("POST", "/api/runtime/services/shutdown"), "light")
        self.assertEqual(_request_priority_lane("POST", "/api/jobs/job123/cancel"), "light")
        self.assertEqual(_request_priority_lane("POST", "/api/jobs"), "shared")
        self.assertEqual(_request_priority_lane("GET", "/api/jobs/job123/results"), "shared")

    def test_cancel_workflow_job_marks_job_terminal_and_requests_job_service_stop(self) -> None:
        job_id = "workflow_cancel_api_job"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="queued",
            stage="plan_review",
            request_payload={"target_company": "Anthropic"},
            plan_payload={},
            summary_payload={},
        )

        result = self.orchestrator.cancel_workflow_job(
            job_id,
            {"reason": "operator requested cancel", "requested_by": "unit-test"},
        )

        self.assertEqual(result["status"], "cancelled")
        refreshed = self.store.get_job(job_id) or {}
        self.assertEqual(refreshed["status"], "cancelled")
        self.assertEqual(refreshed["stage"], "completed")
        self.assertEqual(dict(refreshed.get("summary") or {}).get("cancelled_reason"), "operator requested cancel")
        stop_request = read_service_stop_request(self.settings.runtime_dir, f"job-recovery-{job_id}")
        self.assertEqual(stop_request["status"], "requested")
        self.assertEqual(stop_request["requested_by"], "unit-test")
        shared_stop_request = read_service_stop_request(self.settings.runtime_dir, "worker-recovery-daemon")
        self.assertEqual(shared_stop_request["status"], "not_requested")

    def test_http_api_light_health_request_bypasses_busy_shared_lane(self) -> None:
        job_started = threading.Event()
        release_job = threading.Event()
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))

        def slow_run_job(payload: dict[str, object]) -> dict[str, object]:
            job_started.set()
            self.assertEqual(payload["target_company"], "Anthropic")
            self.assertTrue(release_job.wait(timeout=5))
            return {"status": "completed", "job_id": "job-heavy", "matches": []}

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_API_MAX_PARALLEL_REQUESTS": "1",
                "SOURCING_API_LIGHT_REQUEST_RESERVED": "1",
            },
            clear=False,
        ):
            server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address

        try:
            with unittest.mock.patch.object(self.orchestrator, "run_job", side_effect=slow_run_job):
                errors: list[Exception] = []
                responses: list[dict[str, object]] = []

                def issue_heavy_job() -> None:
                    try:
                        req = urllib_request.Request(
                            f"http://{host}:{port}/api/jobs",
                            data=json.dumps({"target_company": "Anthropic"}, ensure_ascii=False).encode("utf-8"),
                            headers={"Content-Type": "application/json"},
                            method="POST",
                        )
                        with opener.open(req, timeout=5.0) as response:
                            responses.append(json.loads(response.read().decode("utf-8")))
                    except Exception as exc:  # pragma: no cover - assertion below captures failures
                        errors.append(exc)

                heavy_thread = threading.Thread(target=issue_heavy_job, daemon=True)
                heavy_thread.start()
                self.assertTrue(job_started.wait(timeout=1.5))

                health_req = urllib_request.Request(f"http://{host}:{port}/health", method="GET")
                started_at = time.monotonic()
                with opener.open(health_req, timeout=1.0) as response:
                    health_payload = json.loads(response.read().decode("utf-8"))
                elapsed = time.monotonic() - started_at

                self.assertLess(
                    elapsed,
                    0.75,
                    "light health requests should not wait for the busy shared lane",
                )
                self.assertIn("status", health_payload)

                release_job.set()
                heavy_thread.join(timeout=2.0)
                self.assertFalse(heavy_thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(str(responses[0]["status"] or ""), "completed")
        finally:
            release_job.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_supports_local_frontend_cors(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            preflight_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflows/explain",
                method="OPTIONS",
                headers={
                    "Origin": "http://127.0.0.1:4173",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "Content-Type",
                },
            )
            with opener.open(preflight_req) as response:
                self.assertEqual(response.status, 204)
                self.assertEqual(response.headers.get("Access-Control-Allow-Origin"), "http://127.0.0.1:4173")
                self.assertIn("POST", str(response.headers.get("Access-Control-Allow-Methods") or ""))
                self.assertIn("Content-Type", str(response.headers.get("Access-Control-Allow-Headers") or ""))

            explain_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflows/explain",
                data=json.dumps(
                    {
                        "raw_user_request": "帮我找Reflection AI做Infra方向的人",
                        "target_company": "Reflection AI",
                        "skip_plan_review": True,
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Origin": "http://127.0.0.1:4173",
                },
                method="POST",
            )
            with opener.open(explain_req) as response:
                explain_resp = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.headers.get("Access-Control-Allow-Origin"), "http://127.0.0.1:4173")
            self.assertEqual(explain_resp["status"], "ready")
            self.assertIn("dispatch_preview", explain_resp)

            local_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review?status=open",
                headers={
                    "Origin": "http://localhost:4174",
                },
                method="GET",
            )
            with opener.open(local_review_req) as response:
                local_review_resp = json.loads(response.read().decode("utf-8"))
                self.assertEqual(response.headers.get("Access-Control-Allow-Origin"), "http://localhost:4174")
            self.assertIn("manual_review_items", local_review_resp)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_query_dispatch_filters(self) -> None:
        first = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
                "skip_plan_review": True,
                "tenant_id": "tenant-a",
                "requester_id": "user-1",
            }
        )
        self.assertEqual(first["status"], "queued")
        second = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施", "GPU", "预训练"],
                "top_k": 3,
                "skip_plan_review": True,
                "tenant_id": "tenant-a",
                "requester_id": "user-1",
            }
        )
        self.assertEqual(second["status"], "joined_existing_job")

        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            filtered_req = urllib_request.Request(
                f"http://{host}:{port}/api/query-dispatches?target_company=Anthropic&tenant_id=tenant-a&requester_id=user-1&limit=5",
                method="GET",
            )
            with opener.open(filtered_req) as response:
                filtered_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("query_dispatches", filtered_resp)
            self.assertGreaterEqual(len(filtered_resp["query_dispatches"]), 1)
            first_dispatch = dict(filtered_resp["query_dispatches"][0])
            self.assertEqual(first_dispatch.get("target_company"), "Anthropic")
            self.assertEqual(first_dispatch.get("tenant_id"), "tenant-a")
            self.assertEqual(first_dispatch.get("requester_id"), "user-1")

            invalid_limit_req = urllib_request.Request(
                f"http://{host}:{port}/api/query-dispatches?limit=not-a-number",
                method="GET",
            )
            with opener.open(invalid_limit_req) as response:
                invalid_limit_resp = json.loads(response.read().decode("utf-8"))
            self.assertIn("query_dispatches", invalid_limit_resp)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_manual_review_route_supports_job_filter_and_candidate_detail_route(self) -> None:
        candidate = Candidate(
            candidate_id="cand_api_manual_review",
            name_en="Lina Example",
            display_name="Lina Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            team="Infra",
            focus_areas="distributed training; infra",
            education="Tsinghua University",
            work_history="Acme | Research Engineer",
            linkedin_url="https://www.linkedin.com/in/lina-example/",
            metadata={"location": "San Francisco Bay Area"},
        )
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    candidate.candidate_id,
                    "linkedin_profile_detail",
                    "LinkedIn profile detail",
                    "https://www.linkedin.com/in/lina-example/",
                ),
                candidate_id=candidate.candidate_id,
                source_type="linkedin_profile_detail",
                title="LinkedIn profile detail",
                url="https://www.linkedin.com/in/lina-example/",
                summary="Lina Example works on infrastructure for distributed model training.",
                source_dataset="linkedin_profile_detail",
                source_path="/tmp/lina-example.json",
            )
        ]
        self.store.replace_bootstrap_data([candidate], evidence)
        job_id = "jobapicandidatedetail"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={"target_company": "Acme"},
            plan_payload={},
            summary_payload={"message": "completed"},
        )
        self.store.replace_job_results(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "rank": 1,
                    "score": 0.93,
                    "confidence_label": "high",
                    "confidence_score": 0.93,
                    "confidence_reason": "Strong structured evidence",
                    "explanation": "Candidate detail route should expose this record.",
                    "matched_fields": [{"field": "focus_areas", "value": "infra"}],
                }
            ],
        )
        self.store.replace_manual_review_items(
            job_id,
            [
                {
                    "candidate_id": candidate.candidate_id,
                    "target_company": "Acme",
                    "review_type": "manual_identity_resolution",
                    "priority": "medium",
                    "status": "open",
                    "summary": "Needs one more human check.",
                    "candidate": candidate.to_record(),
                    "evidence": [
                        item.to_record() if isinstance(item, EvidenceRecord) else dict(item) for item in evidence
                    ],
                    "metadata": {"recommended_action": "Verify latest affiliation."},
                }
            ],
        )

        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            manual_review_req = urllib_request.Request(
                f"http://{host}:{port}/api/manual-review?job_id={job_id}&status=open",
                method="GET",
            )
            with opener.open(manual_review_req) as response:
                manual_review_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(len(manual_review_resp["manual_review_items"]), 1)
            self.assertEqual(
                str(manual_review_resp["manual_review_items"][0]["candidate_id"] or ""),
                candidate.candidate_id,
            )

            candidate_req = urllib_request.Request(
                f"http://{host}:{port}/api/jobs/{job_id}/candidates/{candidate.candidate_id}",
                method="GET",
            )
            with opener.open(candidate_req) as response:
                candidate_resp = json.loads(response.read().decode("utf-8"))
            self.assertEqual(str(candidate_resp["candidate"]["candidate_id"] or ""), candidate.candidate_id)
            self.assertEqual(str(candidate_resp["result"]["confidence_label"] or ""), "high")
            self.assertGreaterEqual(len(candidate_resp["evidence"]), 1)
            self.assertEqual(len(candidate_resp["manual_review_items"]), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_workflow_endpoint_defaults_to_hosted_execution(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with unittest.mock.patch.object(
                self.orchestrator,
                "start_workflow",
                return_value={"status": "queued", "job_id": "job-api-workflow"},
            ) as mocked_start:
                workflow_req = urllib_request.Request(
                    f"http://{host}:{port}/api/workflows",
                    data=json.dumps({"plan_review_id": 42}, ensure_ascii=False).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with opener.open(workflow_req) as response:
                    workflow_resp = json.loads(response.read().decode("utf-8"))
                    status_code = response.status
            self.assertEqual(status_code, 202)
            self.assertEqual(workflow_resp["job_id"], "job-api-workflow")
            mocked_start.assert_called_once()
            payload = mocked_start.call_args[0][0]
            self.assertEqual(payload["plan_review_id"], 42)
            self.assertEqual(payload["runtime_execution_mode"], "hosted")
            self.assertEqual(payload["analysis_stage_mode"], "single_stage")
            self.assertFalse(payload["auto_job_daemon"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_workflow_endpoint_executes_hosted_dispatch_path(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        dispatched = threading.Event()

        def fake_run_queued_workflow(job_id: str, recovery_payload=None):  # type: ignore[no-untyped-def]
            job = self.store.get_job(job_id) or {}
            self.store.save_job(
                job_id=job_id,
                job_type="workflow",
                status="completed",
                stage="completed",
                request_payload=dict(job.get("request") or {}),
                plan_payload=dict(job.get("plan") or {}),
                summary_payload={"message": "Workflow completed in hosted smoke."},
            )
            dispatched.set()
            return {"status": "completed", "job_id": job_id, "recovery_payload": dict(recovery_payload or {})}

        try:
            with unittest.mock.patch.object(
                self.orchestrator,
                "run_queued_workflow",
                side_effect=fake_run_queued_workflow,
            ):
                workflow_req = urllib_request.Request(
                    f"http://{host}:{port}/api/workflows",
                    data=json.dumps(
                        {
                            "target_company": "Reflection AI",
                            "categories": ["employee"],
                            "skip_plan_review": True,
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with opener.open(workflow_req) as response:
                    workflow_resp = json.loads(response.read().decode("utf-8"))
                    status_code = response.status

            self.assertEqual(status_code, 202)
            self.assertIn(workflow_resp["status"], {"queued", "joined_existing_job"})
            self.assertTrue(dispatched.wait(timeout=3.0))
            stored_job = self.store.get_job(str(workflow_resp["job_id"]))
            self.assertIsNotNone(stored_job)
            assert stored_job is not None
            self.assertEqual(str(stored_job.get("status") or ""), "completed")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_mark_linkedin_profile_registry_queued_preserves_fetched_when_raw_exists(self) -> None:
        profile_url = "https://www.linkedin.com/in/registry-preserve/"
        raw_path = str(
            self.settings.company_assets_dir / "reflectionai" / "registry-preserve" / "harvest_profiles" / "cached.json"
        )
        self.store.mark_linkedin_profile_registry_fetched(
            profile_url,
            raw_path=raw_path,
            snapshot_dir=str(self.settings.company_assets_dir / "reflectionai" / "registry-preserve"),
        )

        updated = self.store.mark_linkedin_profile_registry_queued(
            profile_url,
            snapshot_dir=str(self.settings.company_assets_dir / "reflectionai" / "queued-overwrite-attempt"),
        )

        self.assertEqual(str(updated.get("status") or ""), "fetched")
        self.assertEqual(str(updated.get("last_raw_path") or ""), raw_path)

    def test_http_api_workflow_endpoint_can_request_managed_subprocess_execution(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with unittest.mock.patch.object(
                self.orchestrator,
                "start_workflow_runner_managed",
                return_value={"status": "queued", "job_id": "job-api-managed"},
            ) as mocked_start:
                workflow_req = urllib_request.Request(
                    f"http://{host}:{port}/api/workflows",
                    data=json.dumps(
                        {
                            "plan_review_id": 43,
                            "runtime_execution_mode": "managed_subprocess",
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with opener.open(workflow_req) as response:
                    workflow_resp = json.loads(response.read().decode("utf-8"))
                    status_code = response.status
            self.assertEqual(status_code, 202)
            self.assertEqual(workflow_resp["job_id"], "job-api-managed")
            mocked_start.assert_called_once()
            payload = mocked_start.call_args[0][0]
            self.assertEqual(payload["plan_review_id"], 43)
            self.assertEqual(payload["runtime_execution_mode"], "managed_subprocess")
            self.assertEqual(payload["analysis_stage_mode"], "single_stage")
            self.assertTrue(payload["auto_job_daemon"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_http_api_continue_stage2_endpoint_delegates_to_orchestrator(self) -> None:
        server = create_server(self.orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            with unittest.mock.patch.object(
                self.orchestrator,
                "continue_workflow_stage2",
                return_value={"status": "queued", "job_id": "job-stage2", "analysis_stage": "stage_2_final"},
            ) as mocked_continue:
                continue_req = urllib_request.Request(
                    f"http://{host}:{port}/api/workflows/job-stage2/continue-stage2",
                    data=json.dumps({}, ensure_ascii=False).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with opener.open(continue_req) as response:
                    continue_resp = json.loads(response.read().decode("utf-8"))
                    status_code = response.status
            self.assertEqual(status_code, 202)
            self.assertEqual(continue_resp["job_id"], "job-stage2")
            mocked_continue.assert_called_once()
            payload = mocked_continue.call_args[0][0]
            self.assertEqual(payload["job_id"], "job-stage2")
            self.assertEqual(payload, {"job_id": "job-stage2"})
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_job_trace_endpoint_returns_spans(self) -> None:
        self.orchestrator.bootstrap()
        result = self.orchestrator.run_job(
            {
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["基础设施"],
                "top_k": 2,
            }
        )
        trace = self.orchestrator.get_job_trace(result["job_id"])
        self.assertIsNotNone(trace)
        self.assertTrue(trace["agent_runtime_session"])
        lane_ids = [item["lane_id"] for item in trace["agent_trace_spans"]]
        self.assertIn("retrieval_specialist", lane_ids)

    def test_normalize_snapshot_inherits_historical_explicit_profile_captures(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T120000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T120000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            focus_areas="Research Engineer | distributed systems",
            education="MIT",
            work_history="Acme | Example Labs",
            notes="Historical enriched snapshot.",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            source_dataset="acme_linkedin_company_people",
            source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
            metadata={
                "public_identifier": "alice-example",
                "profile_url": "https://www.linkedin.com/in/alice-example/",
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
                "membership_review_rationale": "Profile content looks implausible for the target company.",
                "membership_review_triggers": ["suspicious_profile_content"],
                "membership_review_trigger_keywords": ["healer"],
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "linkedin_profile_detail",
                    "Research Engineer",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="linkedin_profile_detail",
                title="Research Engineer",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Historical profile detail capture.",
                source_dataset="linkedin_profile_detail",
                source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
            ),
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "linkedin_profile_membership_review",
                    "Membership review",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="linkedin_profile_membership_review",
                title="Membership review",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Historical suspicious membership review.",
                source_dataset="linkedin_profile_membership_review",
                source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
                metadata={"decision": "suspicious_member"},
            ),
        ]
        historical_lead = Candidate(
            candidate_id="cand_acme_lead",
            name_en="Historical Lead",
            display_name="Historical Lead",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Research lead from publication",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "publications" / "lead.json"),
        )
        historical_lead_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                historical_lead.candidate_id,
                "publication_match",
                "Great Paper",
                "https://example.com/paper",
            ),
            candidate_id=historical_lead.candidate_id,
            source_type="publication_match",
            title="Great Paper",
            url="https://example.com/paper",
            summary="Historical lead that should not be dropped by normalize.",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "publications" / "lead.json"),
        )
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record(), historical_lead.to_record()],
                    "evidence": [item.to_record() for item in [*old_evidence, historical_lead_evidence]],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Member of Technical Staff at Acme",
            focus_areas="Member of Technical Staff at Acme",
            notes="LinkedIn company roster baseline. Location: San Francisco.",
            linkedin_url="https://www.linkedin.com/in/ACwAAExampleBaseline",
            source_dataset="acme_linkedin_company_people",
            source_path=str(current_snapshot_dir / "harvest_company_employees" / "visible.json"),
            metadata={
                "profile_url": "https://www.linkedin.com/in/ACwAAExampleBaseline",
                "snapshot_id": current_snapshot_dir.name,
            },
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "acme_linkedin_company_people",
                    "Roster row",
                    "https://www.linkedin.com/company/acme/",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="linkedin_company_people",
                title="Roster row",
                url="https://www.linkedin.com/company/acme/",
                summary="Current roster baseline.",
                source_dataset="acme_linkedin_company_people",
                source_path=str(current_snapshot_dir / "harvest_company_employees" / "visible.json"),
            )
        ]
        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": current_snapshot_dir.name},
                    "candidates": [baseline_candidate.to_record()],
                    "evidence": [item.to_record() for item in baseline_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_acme_1")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.education, "MIT")
        self.assertEqual(stored_candidate.work_history, "Acme | Example Labs")
        self.assertTrue(stored_candidate.metadata.get("membership_review_required"))
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "suspicious_member")
        self.assertIsNotNone(self.store.get_candidate("cand_acme_lead"))

        stored_evidence = self.store.list_evidence("cand_acme_1")
        stored_source_types = {item["source_type"] for item in stored_evidence}
        self.assertIn("linkedin_company_people", stored_source_types)
        self.assertIn("linkedin_profile_detail", stored_source_types)
        self.assertIn("linkedin_profile_membership_review", stored_source_types)
        lead_evidence = self.store.list_evidence("cand_acme_lead")
        self.assertEqual(lead_evidence[0]["source_type"], "publication_match")

        rewritten_payload = json.loads((current_snapshot_dir / "candidate_documents.json").read_text())
        rewritten_candidate = rewritten_payload["candidates"][0]
        rewritten_source_types = {item["source_type"] for item in rewritten_payload["evidence"]}
        self.assertEqual(rewritten_candidate["education"], "MIT")
        self.assertEqual(rewritten_candidate["work_history"], "Acme | Example Labs")
        self.assertTrue(rewritten_candidate["metadata"]["membership_review_required"])
        self.assertIn("linkedin_profile_detail", rewritten_source_types)
        self.assertEqual(rewritten_payload["historical_profile_inheritance"]["matched_candidate_count"], 1)
        self.assertGreaterEqual(rewritten_payload["historical_profile_inheritance"]["inherited_evidence_count"], 2)
        self.assertEqual(rewritten_payload["historical_profile_inheritance"]["carried_forward_candidate_count"], 1)
        rewritten_candidate_ids = {item["candidate_id"] for item in rewritten_payload["candidates"]}
        self.assertIn("cand_acme_lead", rewritten_candidate_ids)

    def test_normalize_snapshot_reuses_large_historical_baseline_for_sparse_full_company_refresh(self) -> None:
        company_dir = self.settings.company_assets_dir / "bigco"
        old_snapshot_dir = company_dir / "20260406T120000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T120000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        historical_candidates: list[Candidate] = []
        for index in range(1001):
            historical_candidates.append(
                Candidate(
                    candidate_id=f"cand_bigco_{index:04d}",
                    name_en=f"Person {index:04d}",
                    display_name=f"Person {index:04d}",
                    category="employee",
                    target_company="BigCo",
                    organization="BigCo",
                    employment_status="current",
                    role="Software Engineer",
                    linkedin_url=f"https://www.linkedin.com/in/bigco-{index:04d}/",
                    source_dataset="bigco_linkedin_company_people",
                    source_path=str(old_snapshot_dir / "harvest_company_employees" / f"{index:04d}.json"),
                )
            )

        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [item.to_record() for item in historical_candidates],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        current_seed = Candidate(
            candidate_id="seed_bigco_0001",
            name_en="Person 0001",
            display_name="Person 0001",
            category="employee",
            target_company="BigCo",
            organization="BigCo",
            employment_status="current",
            role="Software Engineer",
            linkedin_url="https://www.linkedin.com/in/bigco-0001/",
            source_dataset="bigco_scoped_search_seed",
            source_path=str(current_snapshot_dir / "search_seed_discovery" / "seed.json"),
        )
        current_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    current_seed.candidate_id,
                    "bigco_scoped_search_seed",
                    "Scoped seed",
                    current_seed.linkedin_url,
                ),
                candidate_id=current_seed.candidate_id,
                source_type="profile_search_seed",
                title="Scoped seed",
                url=current_seed.linkedin_url,
                summary="Sparse scoped seed refresh.",
                source_dataset="bigco_scoped_search_seed",
                source_path=str(current_snapshot_dir / "search_seed_discovery" / "seed.json"),
            )
        ]
        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": current_snapshot_dir.name},
                    "candidates": [current_seed.to_record()],
                    "evidence": [item.to_record() for item in current_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        identity = CompanyIdentity(
            requested_name="BigCo",
            canonical_name="BigCo",
            company_key="bigco",
            linkedin_slug="bigco",
            linkedin_company_url="https://www.linkedin.com/company/bigco/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )

        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [current_seed],
                "evidence": current_evidence,
            },
            JobRequest(
                raw_user_request="帮我找 BigCo 做 infra 的人",
                target_company="BigCo",
                target_scope="full_company_asset",
                categories=["employee"],
            ),
        )

        self.assertEqual(result.status, "completed")
        rewritten_payload = json.loads((current_snapshot_dir / "candidate_documents.json").read_text())
        self.assertTrue(rewritten_payload["historical_profile_inheritance"]["baseline_snapshot_reused"])
        self.assertEqual(
            rewritten_payload["historical_profile_inheritance"]["baseline_snapshot_id"],
            old_snapshot_dir.name,
        )
        self.assertEqual(rewritten_payload["candidate_count"], 1001)
        self.assertEqual(self.store.candidate_count_for_company("BigCo"), 1001)

    def test_normalize_snapshot_force_fresh_run_skips_historical_profile_inheritance_at_execution_time(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-normalize-fresh-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        candidate = Candidate(
            candidate_id="anthropic_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
            metadata={"cost_policy": {"allow_historical_profile_inheritance": True}},
        )
        with (
            unittest.mock.patch(
                "sourcing_agent.acquisition._inherit_historical_profile_captures",
                side_effect=AssertionError("force_fresh_run should skip historical profile inheritance"),
            ),
            unittest.mock.patch(
                "sourcing_agent.acquisition.canonicalize_company_records",
                return_value=([candidate], [], {"merged_candidate_count": 0}),
            ),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [],
                },
                JobRequest(
                    raw_user_request="重新跑 Anthropic normalize",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("anthropic_1")
        self.assertIsNotNone(stored_candidate)

    def test_load_delta_baseline_material_reads_external_canonical_root(self) -> None:
        canonical_root = Path(self.tempdir.name) / "canonical_company_assets"
        snapshot_id = "20260408T120000"
        snapshot_dir = canonical_root / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": {
                        "requested_name": "Anthropic",
                        "canonical_name": "Anthropic",
                        "company_key": "anthropic",
                        "linkedin_slug": "anthropicresearch",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "company_identity": {
                            "requested_name": "Anthropic",
                            "canonical_name": "Anthropic",
                            "company_key": "anthropic",
                            "linkedin_slug": "anthropicresearch",
                        },
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="anthropic_hist_1",
                            name_en="Ada Example",
                            display_name="Ada Example",
                            category="employee",
                            target_company="Anthropic",
                            organization="Anthropic",
                            employment_status="current",
                            role="Research Engineer",
                            linkedin_url="https://www.linkedin.com/in/ada-example/",
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

        request = JobRequest(
            raw_user_request="帮我找 Anthropic 做研究的人",
            target_company="Anthropic",
            execution_preferences={"delta_baseline_snapshot_id": snapshot_id},
        )
        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root)},
            clear=False,
        ):
            engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
            baseline = engine._load_delta_baseline_material(request, baseline_snapshot_id=snapshot_id)

        self.assertEqual(baseline["snapshot_id"], snapshot_id)
        self.assertEqual(Path(str(baseline["snapshot_dir"])).resolve(), snapshot_dir.resolve())
        self.assertEqual(baseline["candidate_document_payload"]["candidate_count"], 1)
        self.assertEqual(
            Path(str(baseline["source_candidate_doc_path"])).resolve(),
            (snapshot_dir / "candidate_documents.json").resolve(),
        )

    def test_normalize_snapshot_mirrors_hot_cache_and_refreshes_retrieval_index(self) -> None:
        canonical_root = Path(self.tempdir.name) / "canonical_company_assets"
        hot_cache_root = Path(self.tempdir.name) / "hot_cache_company_assets"
        snapshot_id = "snapshot-hot-cache"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_hot_cache_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
        )
        normalize_task = AcquisitionTask(
            task_id="normalize_hot_cache",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        retrieval_task = AcquisitionTask(
            task_id="retrieval_hot_cache",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Build retrieval index",
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
            engine._write_latest_snapshot_pointer(identity, snapshot_id, snapshot_dir)
            with unittest.mock.patch(
                "sourcing_agent.acquisition.build_company_candidate_artifacts",
                return_value={"artifact_dir": "", "artifact_paths": {}, "sync_status": {}},
            ):
                normalize_result = engine._normalize_snapshot(
                    normalize_task,
                    {
                        "company_identity": identity,
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )

            self.assertEqual(normalize_result.status, "completed")
            self.assertEqual(normalize_result.payload["hot_cache_sync"]["status"], "completed")
            hot_cache_snapshot_dir = hot_cache_root / "acme" / snapshot_id
            self.assertTrue((hot_cache_root / "acme" / "latest_snapshot.json").exists())
            self.assertTrue(hot_cache_snapshot_dir.exists())
            self.assertTrue((hot_cache_snapshot_dir / "candidate_documents.json").exists())

            stale_marker = hot_cache_snapshot_dir / "stale.json"
            stale_marker.write_text("stale", encoding="utf-8")
            retrieval_result = engine._build_retrieval_index(
                retrieval_task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
            )

        self.assertEqual(retrieval_result.status, "completed")
        self.assertEqual(retrieval_result.payload["hot_cache_sync"]["status"], "completed")
        self.assertTrue((snapshot_dir / "retrieval_index_summary.json").exists())
        self.assertTrue((hot_cache_root / "acme" / snapshot_id / "retrieval_index_summary.json").exists())
        self.assertFalse((hot_cache_root / "acme" / snapshot_id / "stale.json").exists())

    def test_normalize_snapshot_materializes_candidate_artifacts_immediately(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-normalize-immediate-materialize"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
        )
        artifact_build_result = {
            "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
            "artifact_paths": {
                "materialized_candidate_documents": str(
                    snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                ),
                "artifact_summary": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
            },
            "sync_status": {
                "overall_status": "completed",
                "organization_asset_registry_refresh": {"status": "completed"},
            },
        }
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        request = JobRequest(
            raw_user_request="帮我找 Acme 的人",
            target_company="Acme",
            execution_preferences={"delta_baseline_snapshot_ids": ["baseline-1", "baseline-2"]},
        )

        with unittest.mock.patch(
            "sourcing_agent.acquisition.build_company_candidate_artifacts",
            return_value=artifact_build_result,
        ) as build_artifacts:
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [],
                },
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.payload["artifact_materialization_status"], "completed")
        self.assertEqual(result.payload["artifact_dir"], artifact_build_result["artifact_dir"])
        self.assertEqual(result.payload["artifact_paths"], artifact_build_result["artifact_paths"])
        self.assertEqual(result.payload["sync_status"], artifact_build_result["sync_status"])
        build_artifacts.assert_called_once_with(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id=snapshot_dir.name,
            preferred_source_snapshot_ids=["baseline-1", "baseline-2"],
            build_profile="full",
        )

    def test_normalize_snapshot_reuses_equivalent_delta_baseline_snapshot(self) -> None:
        baseline_snapshot_id = "snapshot-baseline-equivalent"
        current_snapshot_id = "snapshot-current-equivalent"
        baseline_snapshot_dir = self.settings.company_assets_dir / "xai" / baseline_snapshot_id
        baseline_snapshot_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir = self.settings.company_assets_dir / "xai" / current_snapshot_id
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        candidate = Candidate(
            candidate_id="xai_equivalent_1",
            name_en="Equivalent Candidate",
            display_name="Equivalent Candidate",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/equivalent-candidate/",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "xai_candidates",
                "Equivalent profile",
                candidate.linkedin_url,
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile",
            title="Equivalent profile",
            url=candidate.linkedin_url,
            source_dataset="xai_candidates",
            source_path=str(baseline_snapshot_dir / "candidate_documents.json"),
            summary="Equivalent baseline evidence.",
        )
        for snapshot_dir in (baseline_snapshot_dir, current_snapshot_dir):
            (snapshot_dir / "candidate_documents.json").write_text(
                json.dumps(
                    {
                        "snapshot": {"company_identity": identity.to_record()},
                        "candidates": [candidate.to_record()],
                        "evidence": [evidence.to_record()],
                        "candidate_count": 1,
                        "evidence_count": 1,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
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
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=baseline_snapshot_dir,
            target_company="xAI",
            include_strict=True,
        )
        task = AcquisitionTask(
            task_id="normalize-equivalent-baseline",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        request = JobRequest(
            raw_user_request="给我 xAI 的所有成员",
            target_company="xAI",
            execution_preferences={"delta_baseline_snapshot_ids": [baseline_snapshot_id]},
        )

        with unittest.mock.patch(
            "sourcing_agent.acquisition.build_company_candidate_artifacts",
            side_effect=AssertionError("equivalent baseline should bypass artifact rebuild"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition._inherit_historical_profile_captures",
            side_effect=AssertionError("equivalent baseline should bypass historical inheritance"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition.canonicalize_company_records",
            side_effect=AssertionError("equivalent baseline should bypass canonicalization"),
        ), unittest.mock.patch.object(
            self.store,
            "replace_company_data",
            side_effect=AssertionError("equivalent baseline should bypass SQLite replace"),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": current_snapshot_id,
                    "snapshot_dir": current_snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.payload["artifact_materialization_status"], "reused_equivalent_snapshot")
        self.assertEqual(result.payload["reused_equivalent_snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_dir"], baseline_snapshot_dir)
        self.assertEqual(result.state_updates["candidate_doc_path"], baseline_snapshot_dir / "candidate_documents.json")
        self.assertEqual(result.state_updates["manifest_path"], baseline_snapshot_dir / "manifest.json")

    def test_normalize_snapshot_reuses_legacy_serving_baseline_without_strict_exports(self) -> None:
        baseline_snapshot_id = "snapshot-baseline-legacy-serving"
        current_snapshot_id = "snapshot-current-legacy-serving"
        baseline_snapshot_dir = self.settings.company_assets_dir / "xai" / baseline_snapshot_id
        baseline_snapshot_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir = self.settings.company_assets_dir / "xai" / current_snapshot_id
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        candidate = Candidate(
            candidate_id="xai_equivalent_legacy_1",
            name_en="Equivalent Legacy Candidate",
            display_name="Equivalent Legacy Candidate",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/equivalent-legacy-candidate/",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "xai_candidates",
                "Equivalent legacy profile",
                candidate.linkedin_url,
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile",
            title="Equivalent legacy profile",
            url=candidate.linkedin_url,
            source_dataset="xai_candidates",
            source_path=str(baseline_snapshot_dir / "candidate_documents.json"),
            summary="Equivalent baseline evidence.",
        )
        for snapshot_dir in (baseline_snapshot_dir, current_snapshot_dir):
            (snapshot_dir / "candidate_documents.json").write_text(
                json.dumps(
                    {
                        "snapshot": {"company_identity": identity.to_record()},
                        "candidates": [candidate.to_record()],
                        "evidence": [evidence.to_record()],
                        "candidate_count": 1,
                        "evidence_count": 1,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
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
        normalized_dir = baseline_snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        (normalized_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": baseline_snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_shards": [],
                    "pages": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        task = AcquisitionTask(
            task_id="normalize-equivalent-legacy-serving-baseline",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        request = JobRequest(
            raw_user_request="给我 xAI 的所有成员",
            target_company="xAI",
            execution_preferences={"delta_baseline_snapshot_ids": [baseline_snapshot_id]},
        )

        with unittest.mock.patch(
            "sourcing_agent.acquisition.build_company_candidate_artifacts",
            side_effect=AssertionError("legacy serving baseline should bypass artifact rebuild"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition._inherit_historical_profile_captures",
            side_effect=AssertionError("legacy serving baseline should bypass historical inheritance"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition.canonicalize_company_records",
            side_effect=AssertionError("legacy serving baseline should bypass canonicalization"),
        ), unittest.mock.patch.object(
            self.store,
            "replace_company_data",
            side_effect=AssertionError("legacy serving baseline should bypass SQLite replace"),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": current_snapshot_id,
                    "snapshot_dir": current_snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.payload["artifact_materialization_status"], "reused_equivalent_snapshot")
        self.assertEqual(result.payload["reused_equivalent_snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_dir"], baseline_snapshot_dir)

    def test_snapshot_materializer_reuses_artifacts_built_during_normalize(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-prebuilt-artifacts"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity.to_record(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="acme_sync_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
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
        normalize_payload = {
            "candidate_count": 1,
            "evidence_count": 0,
            "manifest_path": str(manifest_path),
            "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
            "artifact_paths": {
                "materialized_candidate_documents": str(
                    snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                ),
            },
            "sync_status": {"overall_status": "completed"},
        }

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_normalize_snapshot",
                return_value=AcquisitionExecution(
                    task_id="normalize::sync",
                    status="completed",
                    detail="Normalized and materialized snapshot.",
                    payload=normalize_payload,
                    state_updates={"manifest_path": manifest_path},
                ),
            ) as normalize_snapshot,
            unittest.mock.patch(
                "sourcing_agent.snapshot_materializer.build_company_candidate_artifacts",
                side_effect=AssertionError("should not rebuild artifacts when normalize already did"),
            ),
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="unit_test",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["artifact_dir"], normalize_payload["artifact_dir"])
        self.assertEqual(result["artifact_paths"], normalize_payload["artifact_paths"])
        self.assertEqual(result["sync_status"], normalize_payload["sync_status"])
        self.assertEqual(result["materialization_writer_slot"]["lane"], "materialization_writer")
        self.assertEqual(result["materialization_writer_slot"]["phase"], "snapshot_candidate_document_sync")
        normalize_snapshot.assert_called_once()

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

    def test_normalize_snapshot_canonicalizes_same_name_current_candidates(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "20260407T130000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        lead_candidate = Candidate(
            candidate_id="lead_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
            source_path=str(snapshot_dir / "publications" / "alice.html"),
        )
        employee_candidate = Candidate(
            candidate_id="employee_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            work_history="Acme",
            education="MIT",
            source_dataset="acme_linkedin_company_people",
            source_path=str(snapshot_dir / "harvest_profiles" / "alice-example.json"),
            metadata={"public_identifier": "alice-example"},
        )
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id("lead_alice", "publication_match", "Paper", "https://example.com/paper"),
                candidate_id="lead_alice",
                source_type="publication_match",
                title="Paper",
                url="https://example.com/paper",
                summary="Lead from publication.",
                source_dataset="publication_match",
                source_path=str(snapshot_dir / "publications" / "alice.html"),
            ),
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    "employee_alice",
                    "linkedin_profile_detail",
                    "Research Engineer",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id="employee_alice",
                source_type="linkedin_profile_detail",
                title="Research Engineer",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Profile detail for Alice.",
                source_dataset="linkedin_profile_detail",
                source_path=str(snapshot_dir / "harvest_profiles" / "alice-example.json"),
            ),
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        result = self.acquisition_engine._normalize_snapshot(
            AcquisitionTask(
                task_id="normalize_canonical",
                task_type="normalize_asset_snapshot",
                title="Normalize canonical",
                description="Persist snapshot with canonical dedupe.",
            ),
            {
                "company_identity": identity,
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidates": [lead_candidate, employee_candidate],
                "evidence": evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored = self.store.list_candidates_for_company("Acme")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].candidate_id, "employee_alice")
        self.assertEqual(stored[0].category, "employee")
        self.assertEqual(stored[0].linkedin_url, "https://www.linkedin.com/in/alice-example/")
        stored_evidence = self.store.list_evidence("employee_alice")
        stored_source_types = {item["source_type"] for item in stored_evidence}
        self.assertIn("publication_match", stored_source_types)
        self.assertIn("linkedin_profile_detail", stored_source_types)
        payload = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertEqual(payload["canonicalization"]["canonical_candidate_count"], 1)
        self.assertEqual(payload["canonicalization"]["name_merge_count"], 1)

    def test_normalize_snapshot_inherits_manual_review_confirmed_membership(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T130000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T140000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_manual_member",
            name_en="Jeremy Example",
            display_name="Jeremy Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Publication author lead",
            media_url="https://jeremy.example.com/",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "manual_review_assets" / "jeremy" / "resolution.json"),
            metadata={
                "manual_review_artifact_root": str(old_snapshot_dir / "manual_review_assets" / "jeremy"),
                "manual_review_links": [{"label": "Homepage", "url": "https://jeremy.example.com/"}],
                "membership_review_required": False,
                "membership_review_decision": "manual_confirmed_member",
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "manual_review",
                    "Homepage",
                    "https://jeremy.example.com/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="manual_review_link",
                title="Homepage",
                url="https://jeremy.example.com/",
                summary="Manual review confirmed current membership.",
                source_dataset="manual_review",
                source_path=str(old_snapshot_dir / "manual_review_assets" / "jeremy" / "source_01.json"),
            )
        ]
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record()],
                    "evidence": [item.to_record() for item in old_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_manual_member",
            name_en="Jeremy Example",
            display_name="Jeremy Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
            source_path=str(current_snapshot_dir / "publications" / "jeremy.json"),
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "publication_match",
                    "Blog post",
                    "https://example.com/jeremy",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="publication_match",
                title="Blog post",
                url="https://example.com/jeremy",
                summary="Current snapshot still only has publication lead evidence.",
                source_dataset="publication_match",
                source_path=str(current_snapshot_dir / "publications" / "jeremy.json"),
            )
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_manual_member")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.category, "employee")
        self.assertEqual(stored_candidate.employment_status, "current")
        self.assertEqual(stored_candidate.media_url, "https://jeremy.example.com/")
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "manual_confirmed_member")
        self.assertFalse(stored_candidate.metadata.get("membership_review_required"))
        self.assertTrue(stored_candidate.metadata.get("manual_review_artifact_root"))

    def test_normalize_snapshot_inherits_manual_non_member_resolution(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T140000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T150000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_manual_non_member",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="non_member",
            target_company="Acme",
            organization="Other Org",
            employment_status="",
            role="Data Analyst",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path=str(old_snapshot_dir / "manual_review_assets" / "rabia" / "resolution.json"),
            metadata={
                "manual_review_artifact_root": str(old_snapshot_dir / "manual_review_assets" / "rabia"),
                "manual_review_links": [{"label": "LinkedIn", "url": "https://www.linkedin.com/in/rabia-example/"}],
                "target_company_mismatch": True,
                "membership_review_required": False,
                "membership_review_decision": "manual_non_member",
                "membership_review_rationale": "Manual review rejected this profile as unrelated to the target company.",
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "manual_review",
                    "LinkedIn",
                    "https://www.linkedin.com/in/rabia-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="manual_review_link",
                title="LinkedIn",
                url="https://www.linkedin.com/in/rabia-example/",
                summary="Manual review rejected this candidate as a non-member.",
                source_dataset="manual_review",
                source_path=str(old_snapshot_dir / "manual_review_assets" / "rabia" / "source_01.json"),
            )
        ]
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record()],
                    "evidence": [item.to_record() for item in old_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_manual_non_member",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="OpenAI at Acme",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path=str(current_snapshot_dir / "search_seeds" / "rabia.json"),
            metadata={
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
            },
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "acme_search_seed_candidates",
                    "Search seed",
                    "https://www.linkedin.com/in/rabia-example/",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="harvest_profile_search",
                title="Search seed",
                url="https://www.linkedin.com/in/rabia-example/",
                summary="Current snapshot still has the suspicious search-seed candidate.",
                source_dataset="acme_search_seed_candidates",
                source_path=str(current_snapshot_dir / "search_seeds" / "rabia.json"),
            )
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_manual_non_member")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.category, "non_member")
        self.assertEqual(stored_candidate.organization, "Other Org")
        self.assertEqual(stored_candidate.employment_status, "")
        self.assertFalse(stored_candidate.metadata.get("membership_review_required"))
        self.assertTrue(stored_candidate.metadata.get("target_company_mismatch"))
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "manual_non_member")
