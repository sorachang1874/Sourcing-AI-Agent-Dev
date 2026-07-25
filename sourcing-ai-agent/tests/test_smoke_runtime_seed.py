import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.company_registry import resolve_company_alias_key
from sourcing_agent.smoke_runtime_seed import seed_reference_smoke_runtime
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class SmokeRuntimeSeedTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def test_seed_reference_smoke_runtime_populates_authoritative_assets_and_openai_reasoning_shards(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir)
            store = self.make_pg_store(runtime_dir / "sourcing_agent.db")

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
            self.assertEqual(
                list(google_authoritative.get("selected_snapshot_ids") or []),
                ["20260414T120400", "20260414T120401", "20260414T120402", "20260414T120403"],
            )

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

            google_rows = store.list_acquisition_shard_registry(
                target_company="Google",
                snapshot_ids=["20260414T120401", "20260414T120402", "20260414T120403"],
                statuses=["completed"],
                limit=10,
            )
            self.assertEqual(len(google_rows), 6)
            google_generation_keys = {str(row.get("materialization_generation_key") or "") for row in google_rows}
            self.assertTrue(all(google_generation_keys))

            meta_authoritative = store.get_authoritative_organization_asset_registry(
                target_company="Meta",
                asset_view="canonical_merged",
            )
            self.assertEqual(str(meta_authoritative.get("snapshot_id") or ""), "20260414T120500")
            self.assertEqual(
                list(meta_authoritative.get("selected_snapshot_ids") or []),
                ["20260414T120500", "20260414T120501"],
            )
            meta_rows = store.list_acquisition_shard_registry(
                target_company="Meta",
                snapshot_ids=["20260414T120501"],
                statuses=["completed"],
                limit=10,
            )
            self.assertEqual(len(meta_rows), 2)
            meta_generation_keys = {str(row.get("materialization_generation_key") or "") for row in meta_rows}
            self.assertTrue(all(meta_generation_keys))

            openai_snapshot_dir = runtime_dir / "company_assets" / "openai" / "20260414T120301"
            self.assertTrue((openai_snapshot_dir / "candidate_documents.json").exists())
            self.assertTrue((openai_snapshot_dir / "normalized_artifacts" / "artifact_summary.json").exists())

            baseline_candidate_path = (
                runtime_dir / "company_assets" / "openai" / "20260414T120300" / "candidate_documents.json"
            )
            baseline_payload = json.loads(baseline_candidate_path.read_text(encoding="utf-8"))
            baseline_candidate = baseline_payload["candidates"][0]
            self.assertTrue(baseline_candidate["experience_lines"])
            self.assertTrue(baseline_candidate["education_lines"])
            self.assertTrue(baseline_candidate["has_profile_detail"])

            baseline_summary_path = (
                runtime_dir
                / "company_assets"
                / "openai"
                / "20260414T120300"
                / "normalized_artifacts"
                / "artifact_summary.json"
            )
            baseline_summary = json.loads(baseline_summary_path.read_text(encoding="utf-8"))
            self.assertEqual(baseline_summary["structured_experience_count"], 300)
            self.assertEqual(baseline_summary["structured_education_count"], 300)

    def test_seed_reference_smoke_runtime_can_opt_into_google_large_real_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir)
            store = self.make_pg_store(runtime_dir / "sourcing_agent.db")

            with patch.dict(
                "os.environ",
                {"SOURCING_SEED_GOOGLE_LARGE_BASELINE_REAL_ASSET": "1"},
                clear=False,
            ):
                result = seed_reference_smoke_runtime(runtime_dir=runtime_dir, store=store)

            google_large = dict(result.get("google_large_baseline_real_asset") or {})
            self.assertEqual(google_large.get("status"), "seeded")
            self.assertGreaterEqual(int(google_large.get("candidate_count") or 0), 3000)

            google_authoritative = store.get_authoritative_organization_asset_registry(
                target_company="Google",
                asset_view="canonical_merged",
            )
            self.assertEqual(str(google_authoritative.get("snapshot_id") or ""), "20260511T000000")
            self.assertGreaterEqual(int(google_authoritative.get("candidate_count") or 0), 3000)
            coverage = dict(
                dict(google_authoritative.get("source_snapshot_selection") or {}).get("population_coverage")
                or dict(google_authoritative.get("summary") or {}).get("population_coverage")
                or {}
            )
            self.assertFalse(bool(coverage.get("full_company_coverage_proven")))
            self.assertEqual(str(coverage.get("coverage_kind") or ""), "scoped_asset")

            candidate_documents_path = (
                runtime_dir / "company_assets" / "google" / "20260511T000000" / "candidate_documents.json"
            )
            candidate_payload = json.loads(candidate_documents_path.read_text(encoding="utf-8"))
            candidate_text = json.dumps(candidate_payload, ensure_ascii=False).lower()
            self.assertNotIn("vision-language", candidate_text)

    def test_seed_reference_smoke_runtime_can_seed_google_8k_baseline_excluding_gemini(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            runtime_dir = Path(tempdir)
            store = self.make_pg_store(runtime_dir / "sourcing_agent.db")

            with patch.dict(
                "os.environ",
                {
                    "SOURCING_SEED_GOOGLE_LARGE_BASELINE_REAL_ASSET": "1",
                    "SOURCING_SEED_GOOGLE_LARGE_BASELINE_MAX_CANDIDATES": "9000",
                    "SOURCING_SEED_GOOGLE_LARGE_BASELINE_EXCLUDE_TERMS": "gemini",
                },
                clear=False,
            ):
                result = seed_reference_smoke_runtime(runtime_dir=runtime_dir, store=store)

            google_large = dict(result.get("google_large_baseline_real_asset") or {})
            self.assertEqual(google_large.get("status"), "seeded")
            self.assertGreaterEqual(int(google_large.get("candidate_count") or 0), 8000)
            self.assertLessEqual(int(google_large.get("candidate_count") or 0), 9000)
            self.assertEqual(google_large.get("seed_exclude_terms"), ["gemini"])

            candidate_documents_path = (
                runtime_dir / "company_assets" / "google" / "20260511T000000" / "candidate_documents.json"
            )
            candidate_payload = json.loads(candidate_documents_path.read_text(encoding="utf-8"))
            candidate_text = json.dumps(candidate_payload, ensure_ascii=False).lower()
            self.assertNotIn("gemini", candidate_text)
            self.assertGreaterEqual(int(candidate_payload.get("candidate_count") or 0), 8000)


if __name__ == "__main__":
    unittest.main()
