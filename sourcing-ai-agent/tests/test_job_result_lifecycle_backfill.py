import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent import job_result_lifecycle_backfill as lifecycle_backfill
from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.job_result_lifecycle_backfill import backfill_job_result_lifecycle
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


class JobResultLifecycleBackfillTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def _save_job(self, job_id: str, *, summary_payload: dict | None = None) -> None:
        self.store.save_job(
            job_id=job_id,
            job_type="company_candidate_sourcing",
            status="completed",
            stage="completed",
            request_payload={"target_company": "test_company", "company_key": "test_company"},
            summary_payload=summary_payload or {},
        )

    def test_backfill_skips_jobs_with_validated_lifecycle_rows(self) -> None:
        job_id = "test_job_with_lifecycle"
        self._save_job(job_id)
        self.store.upsert_job_result_lifecycle(
            job_id=job_id,
            fields={
                "state": "completed",
                "source_validation_status": "validated",
            },
        )

        result = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=False,
            batch_size=10,
        )

        self.assertEqual(result["total_jobs"], 1)
        self.assertEqual(result["jobs_with_lifecycle"], 1)
        self.assertEqual(result["jobs_backfilled"], 0)
        self.assertEqual(result["jobs_skipped"], 1)
        self.assertEqual(result["jobs_repair_required"], 0)

    def test_backfill_does_not_synthesize_lifecycle_for_legacy_jobs_without_evidence(self) -> None:
        job_id = "test_legacy_job"
        self._save_job(job_id)
        self.assertIsNone(self.store.get_job_result_lifecycle(job_id))

        result = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=False,
            batch_size=10,
        )

        self.assertEqual(result["total_jobs"], 1)
        self.assertEqual(result["jobs_with_lifecycle"], 0)
        self.assertEqual(result["jobs_backfilled"], 0)
        self.assertEqual(result["jobs_skipped"], 1)
        self.assertEqual(result["jobs_repair_required"], 1)
        self.assertEqual(
            result["repair_required_samples"],
            [{"job_id": job_id, "reason": "serialized_job_result_lifecycle_missing"}],
        )
        self.assertIsNone(self.store.get_job_result_lifecycle(job_id))

    def test_backfill_migrates_serialized_result_view_lifecycle_evidence(self) -> None:
        job_id = "test_serialized_lifecycle_job"
        self._save_job(job_id)
        self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Test Company",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id="snapshot-current",
            asset_view="canonical_merged",
            source_path="/tmp/snapshot-current/manifest.json",
            summary={"candidate_count": 12},
            metadata={
                "result_view_lifecycle": {
                    "state": "current_snapshot_serving",
                    "baseline_snapshot_id": "snapshot-baseline",
                    "current_snapshot_id": "snapshot-current",
                    "served_snapshot_id": "snapshot-current",
                    "baseline_candidate_count": 10,
                    "served_candidate_count": 12,
                    "expected_candidate_count": 12,
                    "delta_profile_required_count": 2,
                    "delta_profile_fetched_count": 2,
                    "delta_profile_materialized_count": 2,
                }
            },
        )

        result = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=False,
            batch_size=10,
        )

        self.assertEqual(result["jobs_backfilled"], 1)
        self.assertEqual(result["jobs_repair_required"], 0)
        self.assertEqual(
            result["migrated_sources"],
            {"job_result_view.metadata.result_view_lifecycle": 1},
        )
        lifecycle = self.store.get_job_result_lifecycle(job_id)
        self.assertIsNotNone(lifecycle)
        assert lifecycle is not None
        self.assertEqual(lifecycle["source_validation_status"], "validated")
        self.assertEqual(lifecycle["state"], "current_snapshot_serving")
        self.assertEqual(lifecycle["served_snapshot_id"], "snapshot-current")
        self.assertEqual(lifecycle["delta_profile_materialized_count"], 2)
        self.assertEqual(lifecycle["metadata"]["migration_source"], "job_result_view.metadata.result_view_lifecycle")
        self.assertTrue(lifecycle["metadata"]["legacy_projection_retired"])

    def test_backfill_dry_run_reports_serialized_evidence_without_persisting(self) -> None:
        job_id = "test_dry_run_job"
        self._save_job(
            job_id,
            summary_payload={
                "result_view_lifecycle": {
                    "state": "current_snapshot_serving",
                    "served_snapshot_id": "snapshot-current",
                    "served_candidate_count": 5,
                    "expected_candidate_count": 5,
                }
            },
        )

        result = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=True,
            batch_size=10,
        )

        self.assertEqual(result["jobs_backfilled"], 1)
        self.assertEqual(result["jobs_repair_required"], 0)
        self.assertEqual(result["migrated_sources"], {"jobs.summary.result_view_lifecycle": 1})
        self.assertIsNone(self.store.get_job_result_lifecycle(job_id))

    def test_backfill_handles_mixed_jobs_without_creating_synthetic_rows(self) -> None:
        job_ids = ["test_multi_job_0", "test_multi_job_1", "test_multi_job_2"]
        for job_id in job_ids:
            self._save_job(job_id)
        self.store.upsert_job_result_lifecycle(
            job_id=job_ids[0],
            fields={
                "state": "completed",
                "source_validation_status": "validated",
            },
        )
        self.store.upsert_job_result_view(
            job_id=job_ids[1],
            target_company="Test Company",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id="snapshot-current",
            asset_view="canonical_merged",
            source_path="/tmp/snapshot-current/manifest.json",
            summary={"candidate_count": 1},
            metadata={"result_view_lifecycle": {"state": "current_snapshot_serving", "served_candidate_count": 1}},
        )

        result = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=False,
            batch_size=10,
        )

        self.assertEqual(result["total_jobs"], 3)
        self.assertEqual(result["jobs_with_lifecycle"], 1)
        self.assertEqual(result["jobs_backfilled"], 1)
        self.assertEqual(result["jobs_repair_required"], 1)
        self.assertIsNotNone(self.store.get_job_result_lifecycle(job_ids[0]))
        self.assertIsNotNone(self.store.get_job_result_lifecycle(job_ids[1]))
        self.assertIsNone(self.store.get_job_result_lifecycle(job_ids[2]))

    def test_backfill_idempotency_for_migrated_serialized_evidence(self) -> None:
        job_id = "test_idempotent_job"
        self._save_job(
            job_id,
            summary_payload={
                "result_view_lifecycle": {
                    "state": "current_snapshot_serving",
                    "served_snapshot_id": "snapshot-current",
                    "served_candidate_count": 5,
                    "expected_candidate_count": 5,
                }
            },
        )

        result1 = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=False,
            batch_size=10,
        )
        self.assertEqual(result1["jobs_backfilled"], 1)
        lifecycle1 = self.store.get_job_result_lifecycle(job_id)
        self.assertIsNotNone(lifecycle1)
        assert lifecycle1 is not None
        updated_at1 = lifecycle1["updated_at"]

        result2 = backfill_job_result_lifecycle(
            orchestrator=self.orchestrator,
            dry_run=False,
            batch_size=10,
        )
        self.assertEqual(result2["jobs_backfilled"], 0)
        self.assertEqual(result2["jobs_skipped"], 1)
        lifecycle2 = self.store.get_job_result_lifecycle(job_id)
        self.assertIsNotNone(lifecycle2)
        assert lifecycle2 is not None
        self.assertEqual(lifecycle2["updated_at"], updated_at1)

    def test_backfill_runs_schema_preflight_before_first_lifecycle_read(self) -> None:
        job_id = "test_schema_preflight_job"
        self._save_job(job_id)

        call_order: list[str] = []
        original_get = self.store.get_job_result_lifecycle

        def _recording_get(job_id_arg: str):
            call_order.append("get")
            return original_get(job_id_arg)

        def _recording_preflight(orchestrator: SourcingOrchestrator):
            call_order.append("preflight")
            return {"status": "skipped", "reason": "test"}

        with (
            mock.patch.object(self.store, "get_job_result_lifecycle", side_effect=_recording_get),
            mock.patch.object(
                lifecycle_backfill,
                "_ensure_job_result_lifecycle_schema",
                side_effect=_recording_preflight,
            ),
        ):
            result = backfill_job_result_lifecycle(
                orchestrator=self.orchestrator,
                dry_run=True,
                batch_size=10,
            )

        self.assertEqual(result["jobs_repair_required"], 1)
        self.assertGreaterEqual(len(call_order), 2)
        self.assertEqual(call_order[0], "preflight")
        self.assertEqual(call_order[1], "get")


if __name__ == "__main__":
    unittest.main()
