"""Orchestrator planning behavior — ported from the frozen test_pipeline.py god-file.

WS3 Tier 3 salvage wave 1 (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md;
R-009 disposition = salvage-then-delete): these plan_workflow intent-inference
tests (Gemini->Google scope, ChatGPT->OpenAI scope, effective-request task
metadata, intent-axes-only normalization, unknown-keyword preservation) are
still-valid planner contract coverage, re-homed onto the repo-standard
PG fixture (the god-file had ZERO PG fixtures). Old->new port mapping recorded
in docs/governance/REGRESSION_INDEX.md; the freeze ratchet
(tests/test_pipeline_freeze.py) shrinks in the same change.
"""

import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class OrchestratorPlanningTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")
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
        env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        env_patcher.start()
        self.addCleanup(env_patcher.stop)


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
        # FLIPPED 2026-07-22 (WS1 Step 3): the hard-large default profile no
        # longer steers strategy; the Gemini sub-org scope preference (a scope
        # rule, not a size fork — recall via multi-company-page roster) decides
        # this query, and the profile stays advisory.
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["strategy_type"],
            "full_company_roster",
        )
        self.assertEqual(
            planned["plan"]["acquisition_strategy"]["filter_hints"]["current_companies"],
            ["https://www.linkedin.com/company/google/", "https://www.linkedin.com/company/deepmind/"],
        )
        self.assertEqual(
            planned["plan"]["organization_execution_profile"]["default_acquisition_mode"],
            "scoped_search_roster",
        )



    def test_plan_workflow_materializes_intent_axes_only_request_normalization(self) -> None:
        # Ported from the frozen test_pipeline.py (forensic salvage 2026-07-22).
        # Calibrated: acquisition_lane_policy.keyword_priority_only no longer
        # passes through to execution_preferences — that semantic is derived
        # from the strategy/keyword shape in acquisition_strategy cost_policy
        # (keyword_priority_only := scoped_search_roster + keyword hints), so
        # the old passthrough assertion is retired with this note.
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

        client = RequestNormalizingModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=AcquisitionEngine(self.catalog, self.settings, self.store, client),
        )

        planned = orchestrator.plan_workflow({"raw_user_request": "我想找Gemini的产品经理"})

        self.assertEqual(planned["request"]["target_company"], "Google")
        self.assertEqual(planned["request"]["employment_statuses"], ["current", "former"])
        self.assertEqual(planned["request"]["must_have_primary_role_buckets"], ["product_management"])
        self.assertEqual(
            planned["request"]["execution_preferences"]["provider_people_search_query_strategy"],
            "all_queries_union",
        )
        self.assertTrue(planned["request"]["execution_preferences"]["run_former_search_seed"])
        self.assertIn("Gemini", planned["request"]["organization_keywords"])
        self.assertEqual(planned["request_preview"]["intent_axes"]["scope_boundary"]["target_company"], "Google")

    # -- shard-A port group G7 (2026-07-22): parenthetical normalization
    # edge — the Chinese full-parenthetical scaffold must not survive as a
    # keyword nor be reintroduced by AI-first normalization.

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



if __name__ == "__main__":
    unittest.main()
