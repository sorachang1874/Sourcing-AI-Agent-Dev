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

import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
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



if __name__ == "__main__":
    unittest.main()
