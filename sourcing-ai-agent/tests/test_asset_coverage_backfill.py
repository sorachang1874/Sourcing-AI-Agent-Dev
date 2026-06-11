import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_coverage_backfill import backfill_authoritative_population_coverage
from sourcing_agent.asset_coverage_contracts import build_population_coverage_contract
from sourcing_agent.asset_reuse_planning import build_acquisition_shard_registry_record

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AssetCoverageBackfillTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_population_coverage_contract_suppresses_legacy_inference_by_default(self) -> None:
        row = {
            "target_company": "Acme",
            "company_key": "acme",
            "snapshot_id": "snap-full",
            "asset_view": "canonical_merged",
            "candidate_count": 100,
            "summary": {
                "candidate_count": 100,
                "standard_bundles": {"bundle_count": 1},
            },
        }

        strict_contract = build_population_coverage_contract(registry_row=row)
        migration_contract = build_population_coverage_contract(
            registry_row=row,
            allow_legacy_inference=True,
        )

        self.assertFalse(strict_contract["full_company_coverage_proven"])
        self.assertTrue(strict_contract["legacy_inference_suppressed"])
        self.assertIn("legacy_population_coverage_inference_suppressed", strict_contract["reason_codes"])
        self.assertTrue(migration_contract["full_company_coverage_proven"])
        self.assertEqual(migration_contract["proof_source"], "legacy_standard_bundle")

    def test_live_roster_summary_company_employee_lane_becomes_explicit_full_company_coverage(self) -> None:
        row = {
            "target_company": "Lovable",
            "company_key": "lovable",
            "snapshot_id": "snap-live-roster",
            "asset_view": "canonical_merged",
            "candidate_count": 140,
            "summary": {
                "candidate_count": 140,
                "lane_coverage": {
                    "company_employees_current": {
                        "row_count": 1,
                        "result_count": 120,
                        "effective_candidate_count": 120,
                        "effective_ready": True,
                    },
                    "profile_search_former": {
                        "row_count": 1,
                        "result_count": 25,
                        "effective_candidate_count": 20,
                        "effective_ready": True,
                    },
                },
            },
        }

        strict_contract = build_population_coverage_contract(registry_row=row)
        write_time_contract = build_population_coverage_contract(
            registry_row=row,
            allow_legacy_inference=True,
        )

        self.assertFalse(strict_contract["full_company_coverage_proven"])
        self.assertTrue(strict_contract["legacy_inference_suppressed"])
        self.assertTrue(write_time_contract["full_company_coverage_proven"])
        self.assertEqual(write_time_contract["coverage_kind"], "full_company_roster")
        self.assertEqual(write_time_contract["proof_source"], "summary_company_employee_lane_coverage")

    def test_dry_run_reports_full_company_population_coverage_without_writing(self) -> None:
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Acme",
                "company_key": "acme",
                "snapshot_id": "snap-full",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 100,
                "profile_detail_count": 100,
                "selected_snapshot_ids": ["snap-full"],
                "summary": {
                    "candidate_count": 100,
                    "standard_bundles": {"bundle_count": 1},
                },
            },
            authoritative=True,
        )

        result = backfill_authoritative_population_coverage(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Acme"],
            dry_run=True,
        )
        row = self.store.get_authoritative_organization_asset_registry(target_company="Acme")

        self.assertEqual(result["status"], "dry_run")
        self.assertEqual(result["changed_count"], 1)
        self.assertEqual(result["persisted_count"], 0)
        self.assertEqual(result["results"][0]["population_coverage"]["coverage_kind"], "full_company_roster")
        self.assertNotIn("population_coverage", row["summary"])

    def test_apply_persists_explicit_scoped_population_coverage(self) -> None:
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Skild AI",
                "company_key": "skildai",
                "snapshot_id": "snap-agent",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 17,
                "profile_detail_count": 17,
                "selected_snapshot_ids": ["snap-agent"],
                "summary": {"candidate_count": 17},
            },
            authoritative=True,
        )
        self.store.upsert_acquisition_shard_registry(
            build_acquisition_shard_registry_record(
                target_company="Skild AI",
                company_key="skildai",
                snapshot_id="snap-agent",
                lane="profile_search",
                employment_scope="current",
                strategy_type="scoped_search_roster",
                shard_id="Agent",
                shard_title="Agent",
                search_query="Agent",
                company_filters={"companies": ["Skild AI"], "search_query": "Agent"},
                result_count=17,
                status="completed",
            )
        )

        result = backfill_authoritative_population_coverage(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["Skild AI"],
            dry_run=False,
        )
        row = self.store.get_authoritative_organization_asset_registry(target_company="Skild AI")
        coverage = row["summary"]["population_coverage"]

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["persisted_count"], 1)
        self.assertEqual(coverage["coverage_kind"], "scoped_search")
        self.assertTrue(coverage["exact_scoped_coverage_available"])
        self.assertFalse(coverage["full_company_coverage_proven"])
        self.assertEqual(row["source_snapshot_selection"]["population_coverage"]["coverage_kind"], "scoped_search")

    def test_existing_population_coverage_is_skipped_without_force(self) -> None:
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "xAI",
                "company_key": "xai",
                "snapshot_id": "snap-existing",
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": 10,
                "summary": {
                    "candidate_count": 10,
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                    },
                },
            },
            authoritative=True,
        )

        result = backfill_authoritative_population_coverage(
            runtime_dir=self.runtime_dir,
            store=self.store,
            companies=["xAI"],
            dry_run=False,
        )

        self.assertEqual(result["persisted_count"], 0)
        self.assertEqual(result["results"][0]["status"], "skipped_existing_population_coverage")


if __name__ == "__main__":
    unittest.main()
