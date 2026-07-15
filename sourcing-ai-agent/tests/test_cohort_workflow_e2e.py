from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.scripted_provider_scenario import load_scripted_provider_invocations
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import pg_durable_runtime_env

REPO_ROOT = Path(__file__).resolve().parents[1]


class CohortWorkflowScriptedE2ETest(unittest.TestCase):
    def test_explicit_multi_role_cohort_reaches_asset_population_without_live_submission(self) -> None:
        profile_url = "https://www.linkedin.com/in/scripted-cohort-research-engineer/"
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            runtime_dir = root / "runtime"
            runtime_dir.mkdir(parents=True)
            scenario_path = root / "cohort_workflow_scenario.json"
            scenario_path.write_text(
                json.dumps(
                    {
                        "harvest": {
                            "rules": [
                                {
                                    "name": "cohort_profile_search",
                                    "match": {"logical_name": "harvest_profile_search"},
                                    "body": [
                                        {
                                            "linkedinUrl": profile_url,
                                            "publicIdentifier": "scripted-cohort-research-engineer",
                                            "fullName": "Scripted Cohort Research Engineer",
                                            "headline": "Research Scientist and Software Engineer",
                                            "currentCompany": "OpenAI",
                                        }
                                    ],
                                },
                                {
                                    "name": "cohort_profile_scraper",
                                    "match": {"logical_name": "harvest_profile_scraper_batch"},
                                    "body": [
                                        {
                                            "linkedinUrl": profile_url,
                                            "publicIdentifier": "scripted-cohort-research-engineer",
                                            "fullName": "Scripted Cohort Research Engineer",
                                            "headline": "Research Scientist and Software Engineer at OpenAI",
                                            "currentCompany": "OpenAI",
                                            "experience": [
                                                {
                                                    "companyName": "OpenAI",
                                                    "title": "Research Scientist and Software Engineer",
                                                    "dateRange": "2024 - Present",
                                                }
                                            ],
                                            "item": {
                                                "profileUrl": profile_url,
                                                "fullName": "Scripted Cohort Research Engineer",
                                                "headline": "Research Scientist and Software Engineer at OpenAI",
                                                "currentCompany": "OpenAI",
                                                "experience": [
                                                    {
                                                        "companyName": "OpenAI",
                                                        "title": "Research Scientist and Software Engineer",
                                                        "dateRange": "2024 - Present",
                                                    }
                                                ],
                                            },
                                        }
                                    ],
                                },
                            ]
                        }
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with pg_durable_runtime_env(
                runtime_dir=runtime_dir,
                schema_label="cohort_workflow_scripted_e2e",
            ):
                with patch.dict(
                    os.environ,
                    {
                        "SOURCING_RUNTIME_DIR": str(runtime_dir),
                        "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                        "SOURCING_SCRIPTED_PROVIDER_SCENARIO": str(scenario_path),
                        "SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0",
                        "SOURCING_SEMANTIC_PROVIDER_ENABLED": "0",
                    },
                    clear=False,
                ):
                    settings = load_settings(REPO_ROOT)
                    store = ControlPlaneStore(settings.db_path)
                    try:
                        catalog = AssetCatalog.discover()
                        model_client = DeterministicModelClient()
                        acquisition_engine = AcquisitionEngine(
                            catalog,
                            settings,
                            store,
                            model_client,
                        )
                        orchestrator = SourcingOrchestrator(
                            catalog=catalog,
                            store=store,
                            jobs_dir=settings.jobs_dir,
                            model_client=model_client,
                            semantic_provider=LocalSemanticProvider(),
                            acquisition_engine=acquisition_engine,
                        )
                        with patch(
                            "sourcing_agent.harvest_connectors._submit_harvest_actor_run",
                            side_effect=AssertionError("scripted Cohort E2E must not submit a live Harvest run"),
                        ):
                            result = orchestrator.run_workflow_blocking(
                                {
                                    "raw_user_request": (
                                        "Find current OpenAI people who are both researchers and engineers"
                                    ),
                                    "target_company": "OpenAI",
                                    "skip_plan_review": True,
                                    "force_fresh": True,
                                    "job_recovery_poll_seconds": 0.01,
                                    "job_recovery_max_ticks": 3,
                                    "cohort_selection": {
                                        "schema_version": "cohort_selection.v1",
                                        "role_bucket_ids": ["research", "engineering"],
                                        "employment_statuses": ["current"],
                                        "role_match": "all",
                                        "source": "user_explicit",
                                    },
                                }
                            )
                        job_id = str(dict(result.get("job") or {}).get("job_id") or "")
                        background_threads = [
                            thread
                            for thread in threading.enumerate()
                            if thread.name == f"background-outreach-layering-{job_id}"
                        ]
                        for thread in background_threads:
                            thread.join(timeout=20)
                        self.assertFalse(
                            any(thread.is_alive() for thread in background_threads),
                            "background outreach reconcile did not settle before fixture teardown",
                        )
                        result = orchestrator.get_job_results(job_id) or result
                        invocations = load_scripted_provider_invocations()
                    finally:
                        store.close()

            job = dict(result.get("job") or {})
            self.assertEqual((job.get("status"), job.get("stage")), ("completed", "completed"))
            self.assertTrue(str(job.get("job_id") or ""))
            self.assertEqual(result["provider_execution_manifest"]["schema_version"], "cohort_provider_manifest.v1")

            asset_population = dict(result.get("asset_population") or {})
            self.assertEqual(asset_population.get("candidate_count"), 1)
            candidate = asset_population["candidates"][0]
            self.assertEqual(candidate["display_name"], "Scripted Cohort Research Engineer")
            self.assertEqual(candidate["linkedin_url"], profile_url)
            self.assertEqual(candidate["employment_status"], "current")

            audit_paths = list(
                runtime_dir.glob("company_assets/**/cohort_provider_discovery/cohort_execution_result.json")
            )
            self.assertEqual(len(audit_paths), 1)
            audit = json.loads(audit_paths[0].read_text(encoding="utf-8"))
            self.assertEqual(audit["candidate_count"], 1)
            self.assertEqual(len(audit["lane_summaries"]), 2)
            self.assertTrue(all(item["row_count"] == 1 for item in audit["lane_summaries"]))

            candidate_document_paths = list(runtime_dir.glob("company_assets/**/candidate_documents.json"))
            self.assertEqual(len(candidate_document_paths), 1)
            candidate_documents = json.loads(candidate_document_paths[0].read_text(encoding="utf-8"))
            persisted_candidate = candidate_documents["candidates"][0]
            self.assertEqual(
                persisted_candidate["metadata"]["cohort_role_bucket_ids"],
                ["research", "engineering"],
            )
            self.assertEqual(
                persisted_candidate["metadata"]["cohort_employment_statuses"],
                ["current"],
            )
            self.assertEqual(
                persisted_candidate["metadata"]["cohort_role_proof"]["verifier_id"],
                "cohort_headline_role_classifier",
            )
            self.assertEqual(
                [item.get("logical_name") for item in invocations],
                [
                    "harvest_profile_search",
                    "harvest_profile_search",
                    "harvest_profile_scraper_batch",
                ],
            )


if __name__ == "__main__":
    unittest.main()
