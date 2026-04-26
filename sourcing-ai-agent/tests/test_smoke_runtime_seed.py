import tempfile
import unittest
from pathlib import Path

from sourcing_agent.company_registry import resolve_company_alias_key
from sourcing_agent.smoke_runtime_seed import seed_reference_smoke_runtime
from sourcing_agent.storage import ControlPlaneStore


class SmokeRuntimeSeedTest(unittest.TestCase):
    def test_seed_reference_smoke_runtime_populates_authoritative_assets_and_openai_reasoning_shards(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir)
            store = ControlPlaneStore(runtime_dir / "sourcing_agent.db")

            result = seed_reference_smoke_runtime(runtime_dir=runtime_dir, store=store)

            self.assertEqual(result["status"], "seeded")
            self.assertGreaterEqual(len(list(result.get("authoritative_assets") or [])), 5)

            openai_authoritative = store.get_authoritative_organization_asset_registry(
                target_company="OpenAI",
                asset_view="canonical_merged",
            )
            self.assertEqual(str(openai_authoritative.get("snapshot_id") or ""), "20260414T120300")
            self.assertEqual(
                list(openai_authoritative.get("selected_snapshot_ids") or []),
                ["20260414T120300", "20260414T120301"],
            )

            google_authoritative = store.get_authoritative_organization_asset_registry(
                target_company="Google",
                asset_view="canonical_merged",
            )
            self.assertEqual(str(google_authoritative.get("snapshot_id") or ""), "20260414T120400")

            humans_key = resolve_company_alias_key("Humans&")
            self.assertTrue(
                (runtime_dir / "company_assets" / humans_key / "20260414T120100" / "candidate_documents.json").exists()
            )
            self.assertFalse(
                (runtime_dir / "company_assets" / "humans" / "20260414T120100" / "candidate_documents.json").exists()
            )

            openai_rows = store.list_acquisition_shard_registry(
                target_company="OpenAI",
                snapshot_ids=["20260414T120301"],
                statuses=["completed"],
                limit=10,
            )
            self.assertEqual(len(openai_rows), 2)
            generation_keys = {str(row.get("materialization_generation_key") or "") for row in openai_rows}
            self.assertTrue(all(generation_keys))

            openai_snapshot_dir = runtime_dir / "company_assets" / "openai" / "20260414T120301"
            self.assertTrue((openai_snapshot_dir / "candidate_documents.json").exists())
            self.assertTrue((openai_snapshot_dir / "normalized_artifacts" / "artifact_summary.json").exists())


if __name__ == "__main__":
    unittest.main()
