"""Acquisition-side profile-prefetch pending contracts — shard-B port group A.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/NONBAND_OWNERSHIP_R2_SHARD_B_
2026-07-22.md group A): submit-fail degrade (completed with zero queued
harvest workers and the provider message on the failed worker),
baseline-write-while-prefetch-pending (background_reconcile_pending), the
root-candidate-superset preservation contract, and scoped-search
queue-all-known-urls blocking (blocking_reason=harvest_profile_prefetch_
pending) had zero non-frozen coverage. Ported verbatim onto the repo-standard
PG fixture (standalone file rather than the test_enrichment lane member — no
lane-contract churn). Old->new mapping in docs/governance/REGRESSION_INDEX.md;
freeze ratchet shrinks in the same change.
"""

import json
import os
import tempfile
import unittest
import unittest.mock
from dataclasses import replace
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import AcquisitionTask, Candidate, JobRequest
from sourcing_agent.enrichment import MultiSourceEnrichmentResult
from sourcing_agent.harvest_support import HarvestExecutionResult
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AcquisitionPrefetchContractsTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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
        runtime_env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        runtime_env_patcher.start()
        self.addCleanup(runtime_env_patcher.stop)

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
        # CALIBRATED 2026-07-22 (WS7 recon §1.4 contract): submit failure no
        # longer fails the worker — the worker completes with the failed URLs
        # recorded in its output summary (url-level failure log; the unified
        # retry rides the registry retry gate).
        self.assertEqual(workers[0]["status"], "completed")
        worker_summary = dict(dict(workers[0].get("output") or {}).get("summary") or {})
        self.assertIn("https://www.linkedin.com/in/failed-harvest/", list(worker_summary.get("failed_urls") or []))
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
        self.assertEqual(execution.status, "blocked")
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
        self.assertEqual(execution.payload["blocking_reason"], "harvest_profile_prefetch_pending")

    def test_enrich_profiles_pending_profile_prefetch_preserves_root_candidate_superset(self) -> None:
        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )
        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-enrich-preserve-root"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        search_seed_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_seed_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_seed_summary_path.write_text("{}", encoding="utf-8")
        current_candidate = Candidate(
            candidate_id="lovable-current-1",
            name_en="Lovable Current",
            display_name="Lovable Current",
            target_company="Lovable",
            organization="Lovable",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/lovable-current-1/",
        )
        former_candidate = Candidate(
            candidate_id="lovable-former-1",
            name_en="Lovable Former",
            display_name="Lovable Former",
            target_company="Lovable",
            organization="Lovable",
            employment_status="former",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/lovable-former-1/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "Lovable",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [current_candidate.to_record(), former_candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 2,
                    "evidence_count": 0,
                    "company_roster_background_reconcile": {
                        "applied_worker_count": 1,
                        "added_entry_count": 2,
                    },
                }
            ),
            encoding="utf-8",
        )
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Lovable",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "seed_key": "former-1",
                    "full_name": "Lovable Former",
                    "source_type": "harvest_profile_search",
                    "profile_url": "https://www.linkedin.com/in/lovable-former-1/",
                    "employment_status": "former",
                }
            ],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="",
            summary_path=search_seed_summary_path,
        )
        task = AcquisitionTask(
            task_id="enrich-linkedin-profiles",
            task_type="enrich_linkedin_profiles",
            title="Enrich LinkedIn profiles",
            description="Run LinkedIn Stage 1 enrichment",
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
        with unittest.mock.patch.object(
            self.acquisition_engine.multi_source_enricher,
            "enrich",
            return_value=MultiSourceEnrichmentResult(
                candidates=[former_candidate],
                evidence=[],
                queued_harvest_worker_count=1,
                stop_reason="queued_background_harvest",
                profile_prefetch={"status": "queued", "reason": "reused_active_harvest_profile_queue"},
            ),
        ):
            execution = self.acquisition_engine._enrich_profiles(
                task,
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "job_id": "job_preserve_root_candidate_docs",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Lovable people",
                    target_company="Lovable",
                    categories=["employee"],
                    profile_detail_limit=5,
                    slug_resolution_limit=5,
                ),
            )

        self.assertEqual(execution.status, "blocked")
        root_payload = json.loads(candidate_doc_path.read_text())
        self.assertEqual(root_payload["candidate_count"], 2)
        self.assertEqual(
            {candidate["candidate_id"] for candidate in root_payload["candidates"]},
            {"lovable-current-1", "lovable-former-1"},
        )
        self.assertEqual(
            root_payload["root_candidate_documents_contract"]["status"],
            "superset_preserved",
        )
        self.assertEqual(root_payload["company_roster_background_reconcile"]["added_entry_count"], 2)
        stage_payload = json.loads((snapshot_dir / "candidate_documents.linkedin_stage_1.json").read_text())
        self.assertEqual(stage_payload["candidate_count"], 1)
        self.assertEqual(stage_payload["candidates"][0]["candidate_id"], "lovable-former-1")
        self.assertTrue(execution.state_updates["linkedin_stage_completed"])
        self.assertEqual(
            execution.state_updates["linkedin_stage_candidate_doc_path"],
            snapshot_dir / "candidate_documents.linkedin_stage_1.json",
        )

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
                    execution_preferences={"harvest_profile_batch_submit_global_inflight": 2},
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
        self.assertEqual(execution.status, "blocked")
        self.assertEqual(execution.payload["queued_harvest_worker_count"], 1)
        self.assertEqual(execution.payload["stop_reason"], "queued_background_harvest")
        self.assertEqual(len(workers), 1)
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
        self.assertEqual(execution.payload["blocking_reason"], "harvest_profile_prefetch_pending")


if __name__ == "__main__":
    unittest.main()
