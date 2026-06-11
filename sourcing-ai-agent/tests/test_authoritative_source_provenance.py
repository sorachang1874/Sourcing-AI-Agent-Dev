import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_reuse_planning import (
    build_acquisition_shard_registry_record,
    upsert_organization_asset_registry_with_guard,
)
from sourcing_agent.authoritative_source_provenance import normalize_authoritative_source_provenance

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AuthoritativeSourceProvenanceTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_authoritative_row(self) -> None:
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": "repair-snapshot",
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 100,
                "selected_snapshot_ids": ["repair-snapshot", "health-source", "stale-no-shard"],
                "source_snapshot_selection": {
                    "mode": "preferred_snapshot_subset",
                    "serving_snapshot_id": "repair-snapshot",
                    "selected_snapshot_ids": ["repair-snapshot", "health-source", "stale-no-shard"],
                    "serving_generation_repair": {
                        "base_snapshot_id": "health-source",
                        "repair_snapshot_id": "repair-snapshot",
                    },
                },
                "summary": {
                    "candidate_count": 100,
                    "selected_snapshot_ids": ["repair-snapshot", "health-source", "stale-no-shard"],
                },
            },
            authoritative=True,
        )
        self.store.upsert_acquisition_shard_registry(
            build_acquisition_shard_registry_record(
                target_company="OpenAI",
                company_key="openai",
                snapshot_id="health-source",
                lane="profile_search",
                employment_scope="current",
                strategy_type="scoped_search_roster",
                shard_id="health-current",
                shard_title="health",
                search_query="Health",
                company_filters={"search_query": "Health"},
                result_count=49,
                source_path="runtime/company_assets/openai/health-source/raw.json",
                status="completed",
            )
        )

    def test_normalize_authoritative_source_provenance_drops_selected_ids_without_shard_proof(self) -> None:
        self._seed_authoritative_row()

        dry_run = normalize_authoritative_source_provenance(
            store=self.store,
            company="OpenAI",
            apply=False,
        )

        self.assertEqual(dry_run["status"], "dry_run")
        self.assertEqual(dry_run["normalized_selected_snapshot_ids"], ["repair-snapshot", "health-source"])
        self.assertEqual(dry_run["dropped_snapshot_ids_without_reusable_shard_rows"], ["stale-no-shard"])

        result = normalize_authoritative_source_provenance(
            store=self.store,
            company="OpenAI",
            apply=True,
        )

        self.assertEqual(result["status"], "applied")
        row = self.store.get_authoritative_organization_asset_registry(
            target_company="OpenAI",
            asset_view="canonical_merged",
        )
        self.assertEqual(row["selected_snapshot_ids"], ["repair-snapshot", "health-source"])
        selection = dict(row["source_snapshot_selection"])
        self.assertEqual(selection["reusable_source_snapshot_ids"], ["health-source"])
        self.assertEqual(selection["archived_source_snapshot_ids_without_shard_registry_rows"], ["stale-no-shard"])

    def test_authoritative_guard_drops_new_selected_source_ids_without_shard_proof(self) -> None:
        self._seed_authoritative_row()

        result = upsert_organization_asset_registry_with_guard(
            store=self.store,
            candidate_record={
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": "new-serving",
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 120,
                "selected_snapshot_ids": ["new-serving", "health-source", "stale-no-shard"],
                "source_snapshot_selection": {
                    "mode": "preferred_snapshot_subset",
                    "serving_snapshot_id": "new-serving",
                    "selected_snapshot_ids": ["new-serving", "health-source", "stale-no-shard"],
                },
                "summary": {
                    "candidate_count": 120,
                    "selected_snapshot_ids": ["new-serving", "health-source", "stale-no-shard"],
                },
            },
        )

        self.assertEqual(result["selected_snapshot_ids"], ["new-serving", "health-source"])
        selection = dict(result["source_snapshot_selection"])
        self.assertEqual(selection["reusable_source_snapshot_ids"], ["health-source"])
        self.assertEqual(selection["archived_source_snapshot_ids_without_shard_registry_rows"], ["stale-no-shard"])
        self.assertEqual(
            selection["provenance_normalization_reason"],
            "selected_source_snapshot_missing_reusable_shard_registry_row",
        )
        coverage = dict(result["summary"]["population_coverage"])
        self.assertEqual(coverage["coverage_kind"], "scoped_search")
        self.assertTrue(coverage["exact_scoped_coverage_available"])
        self.assertEqual(coverage["write_source"], "authoritative_registry_write")


if __name__ == "__main__":
    unittest.main()
