import inspect
import tempfile
import unittest

from sourcing_agent.legacy_public_web_retirement_audit import audit_legacy_public_web_retirement
from sourcing_agent.legacy_public_web_storage import (
    archive_legacy_target_public_web_tables,
    drop_legacy_target_public_web_tables,
    list_legacy_target_public_web_runs,
    seed_legacy_target_public_web_batch,
    seed_legacy_target_public_web_promotion,
    seed_legacy_target_public_web_run,
)
from sourcing_agent.storage import ControlPlaneStore

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class LegacyPublicWebRetirementAuditTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_new_sqlite_shadow_does_not_bootstrap_empty_legacy_public_web_tables(self) -> None:
        with self.store._lock, self.store._connection:
            rows = self.store._connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name IN (
                    'target_candidate_public_web_batches',
                    'target_candidate_public_web_runs',
                    'target_candidate_public_web_promotions'
                  )
                ORDER BY name
                """
            ).fetchall()

        self.assertEqual([row["name"] for row in rows], [])
        self.assertEqual(self.store.list_target_candidate_public_web_batches(), [])
        self.assertEqual(self.store.list_target_candidate_public_web_runs(), [])
        self.assertEqual(self.store.list_target_candidate_public_web_promotions(), [])

    def test_sqlite_normal_schema_no_longer_bootstraps_legacy_public_web_tables(self) -> None:
        init_schema_source = inspect.getsource(ControlPlaneStore.init_schema)
        migration_schema_source = inspect.getsource(
            ControlPlaneStore._ensure_legacy_target_public_web_sqlite_tables_for_migration
        )

        self.assertNotIn("CREATE TABLE IF NOT EXISTS target_candidate_public_web_batches", init_schema_source)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS target_candidate_public_web_runs", init_schema_source)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS target_candidate_public_web_promotions", init_schema_source)
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS target_candidate_public_web_runs",
            migration_schema_source,
        )

    def test_audit_allows_deletion_when_only_crm_owner_rows_exist(self) -> None:
        self.store.upsert_crm_record(
            {
                "crm_record_id": "crmrec_1",
                "workspace_id": "default",
                "person_identity_key": "linkedin:ada",
                "display_name": "Ada Lovelace",
                "source_collection_id": "company:example",
            }
        )
        self.store.upsert_crm_public_web_batch(
            {
                "batch_id": "crm-batch-1",
                "workspace_id": "default",
                "idempotency_key": "crm-batch-1",
                "status": "completed",
                "summary": {"status": "completed"},
            }
        )
        self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-run-1",
                "batch_id": "crm-batch-1",
                "crm_record_id": "crmrec_1",
                "workspace_id": "default",
                "candidate_name": "Ada Lovelace",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
                "execution_backend": "crm_public_web_v1",
            }
        )
        self.store.upsert_crm_public_web_promotion(
            {
                "promotion_id": "crmpromo_1",
                "crm_record_id": "crmrec_1",
                "workspace_id": "default",
                "run_id": "crm-run-1",
                "signal_id": "signal-1",
                "action": "promote",
                "signal_type": "homepage_url",
                "normalized_value": "https://ada.example/",
            }
        )
        self.store.upsert_collection_authoritative_pointer(
            {
                "collection_id": "company:example",
                "active_projection_id": "proj_example",
                "active_collection_version": "20260525T000000",
                "state": "active",
            }
        )

        report = audit_legacy_public_web_retirement(store=self.store)

        self.assertEqual(report["status"], "ready_for_physical_deletion")
        self.assertTrue(report["deletion_allowed"])
        self.assertEqual(report["deletion_blockers"], [])
        self.assertEqual(report["summary"]["legacy_target_candidate_public_web_row_count"], 0)
        self.assertEqual(report["summary"]["crm_public_web_run_count"], 1)
        self.assertFalse(report["deletion_gate"]["company_asset_overview_required_before_deletion"])
        self.assertEqual(report["collections"]["ids"], ["company:example"])

    def test_audit_blocks_deletion_when_legacy_rows_remain(self) -> None:
        seed_legacy_target_public_web_batch(
            self.store,
            {
                "batch_id": "legacy-batch-1",
                "idempotency_key": "legacy-batch-1",
                "status": "completed",
                "summary": {"status": "completed"},
            }
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "legacy-run-1",
                "batch_id": "legacy-batch-1",
                "record_id": "target-1",
                "candidate_name": "Grace Hopper",
                "current_company": "Example AI",
                "status": "completed",
                "phase": "completed",
            }
        )
        seed_legacy_target_public_web_promotion(
            self.store,
            {
                "promotion_id": "legacy-promo-1",
                "record_id": "target-1",
                "run_id": "legacy-run-1",
                "signal_id": "signal-legacy",
                "action": "promote",
                "signal_type": "email",
                "normalized_value": "grace@example.org",
            }
        )

        report = audit_legacy_public_web_retirement(store=self.store)

        self.assertEqual(report["status"], "blocked")
        self.assertFalse(report["deletion_allowed"])
        self.assertEqual(report["summary"]["legacy_target_candidate_public_web_row_count"], 3)
        self.assertTrue(report["deletion_gate"]["requires_migration_or_cold_backup"])
        self.assertEqual(
            report["deletion_blockers"][0]["blocker"],
            "legacy_target_candidate_public_web_rows_present",
        )
        self.assertEqual(
            report["legacy_target_candidate_public_web"]["runs"]["company_counts"],
            {"Example AI": 1},
        )

    def test_migration_only_reader_returns_empty_when_legacy_table_is_absent(self) -> None:
        with self.store._lock, self.store._connection:
            self.store._connection.execute("DROP TABLE IF EXISTS target_candidate_public_web_runs")

        self.assertEqual(list_legacy_target_public_web_runs(self.store, limit=10), [])
        report = audit_legacy_public_web_retirement(store=self.store)
        self.assertEqual(report["status"], "ready_for_physical_deletion")
        self.assertEqual(report["summary"]["legacy_target_candidate_public_web_run_count"], 0)

    def test_drop_blocks_non_empty_legacy_tables_without_cold_archive(self) -> None:
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "legacy-run-requires-archive",
                "idempotency_key": "legacy-run-requires-archive",
                "record_id": "target-archive-required",
                "status": "completed",
            },
        )

        result = drop_legacy_target_public_web_tables(self.store)

        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "archive_required_before_non_empty_legacy_public_web_drop")
        self.assertEqual(len(list_legacy_target_public_web_runs(self.store, limit=10)), 1)

    def test_archive_then_drop_legacy_tables_removes_migration_rows(self) -> None:
        seed_legacy_target_public_web_batch(
            self.store,
            {
                "batch_id": "legacy-batch-drop",
                "idempotency_key": "legacy-batch-drop",
                "status": "completed",
            },
        )
        seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "legacy-run-drop",
                "batch_id": "legacy-batch-drop",
                "idempotency_key": "legacy-run-drop",
                "record_id": "target-drop",
                "candidate_name": "Katherine Johnson",
                "status": "completed",
            },
        )
        archive_path = f"{self.tempdir.name}/legacy_public_web_archive.json"

        archive = archive_legacy_target_public_web_tables(self.store, archive_path)
        result = drop_legacy_target_public_web_tables(self.store, archive_path=archive_path)
        report = audit_legacy_public_web_retirement(store=self.store)

        self.assertEqual(archive["status"], "archived")
        self.assertEqual(archive["row_count"], 2)
        self.assertEqual(result["status"], "dropped")
        self.assertEqual(result["pre_drop"]["row_count"], 2)
        self.assertEqual(report["status"], "ready_for_physical_deletion")
        self.assertEqual(list_legacy_target_public_web_runs(self.store, limit=10), [])
        with self.store._lock, self.store._connection:
            rows = self.store._connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name LIKE 'target_candidate_public_web_%'
                """
            ).fetchall()
        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
