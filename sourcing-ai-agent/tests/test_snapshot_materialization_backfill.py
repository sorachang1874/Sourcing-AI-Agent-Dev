import tempfile
import unittest
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
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
from sourcing_agent.snapshot_materialization_backfill import backfill_snapshot_full_materialization_items

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class SnapshotMaterializationBackfillTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
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

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _save_completed_workflow_with_scheduled_materialization(self, *, job_id: str, snapshot_id: str) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find OpenAI Agent people",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["Agent"],
                "top_k": 5,
            }
        )
        plan_payload = self.orchestrator.plan_workflow(request.to_record())["plan"]
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

    def test_backfill_snapshot_full_materialization_items_dry_run_does_not_persist(self) -> None:
        self._save_completed_workflow_with_scheduled_materialization(
            job_id="job_snapshot_materialization_backfill_dry_run",
            snapshot_id="snapshot-backfill-dry-run",
        )

        result = backfill_snapshot_full_materialization_items(orchestrator=self.orchestrator, dry_run=True)

        self.assertTrue(result["dry_run"])
        self.assertEqual(result["eligible_jobs"], 1)
        self.assertEqual(result["items_enqueued"], 1)
        items = self.store.list_job_materialization_items(
            job_id="job_snapshot_materialization_backfill_dry_run",
            item_kind="snapshot_full_materialization",
        )
        self.assertEqual(items, [])

    def test_backfill_snapshot_full_materialization_items_apply_is_idempotent(self) -> None:
        self._save_completed_workflow_with_scheduled_materialization(
            job_id="job_snapshot_materialization_backfill_apply",
            snapshot_id="snapshot-backfill-apply",
        )

        first = backfill_snapshot_full_materialization_items(orchestrator=self.orchestrator, dry_run=False)
        second = backfill_snapshot_full_materialization_items(orchestrator=self.orchestrator, dry_run=False)

        self.assertEqual(first["eligible_jobs"], 1)
        self.assertEqual(first["items_enqueued"], 1)
        self.assertEqual(second["existing_items"], 1)
        self.assertEqual(second["items_enqueued"], 0)
        items = self.store.list_job_materialization_items(
            job_id="job_snapshot_materialization_backfill_apply",
            item_kind="snapshot_full_materialization",
        )
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["snapshot_id"], "snapshot-backfill-apply")
        self.assertEqual(items[0]["status"], "queued")


if __name__ == "__main__":
    unittest.main()
