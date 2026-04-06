import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.company_asset_completion import CompanyAssetCompletionManager
from sourcing_agent.domain import Candidate
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import SQLiteStore


class _FakeHarvestProfileConnector:
    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None):
        results = {}
        raw_path = Path(snapshot_dir) / "harvest_profiles" / "fake_profile.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"ok": True}))
        for url in profile_urls:
            results[url] = {
                "raw_path": raw_path,
                "account_id": "fake_harvest",
                "parsed": {
                    "full_name": "Former Example",
                    "profile_url": url,
                    "headline": "Research Engineer at NewCo",
                    "summary": "Former Example previously worked at Acme.",
                    "location": "San Francisco",
                    "current_company": "NewCo",
                    "experience": [
                        {"company": "Acme", "title": "Member of Technical Staff"},
                        {"company": "NewCo", "title": "Research Engineer"},
                    ],
                    "education": [{"school": "Stanford University", "degree": "MS"}],
                    "more_profiles": [],
                    "public_identifier": "former-example",
                    "publications": [],
                },
            }
        return results


class CompanyAssetCompletionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "secrets").mkdir(parents=True, exist_ok=True)
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "linkedin_slug": "acme",
                        "aliases": ["acme ai"],
                    },
                }
            )
        )
        current_snapshot_candidate = Candidate(
            candidate_id="current1",
            name_en="Current Snapshot",
            display_name="Current Snapshot",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/current-snapshot",
            source_dataset="acme_roster_snapshot",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [current_snapshot_candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        self.store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")
        self.store.upsert_candidate(
            Candidate(
                candidate_id="former1",
                name_en="Former Example",
                display_name="Former Example",
                category="former_employee",
                target_company="Acme",
                employment_status="former",
                role="Search seed",
                linkedin_url="https://www.linkedin.com/in/former-example",
                source_dataset="acme_search_seed",
            )
        )
        self.settings = load_settings(self.project_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_complete_company_assets_enriches_known_profile_urls_and_builds_artifacts(self) -> None:
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=_FakeHarvestProfileConnector(),
        )
        result = manager.complete_company_assets(
            target_company="Acme",
            profile_detail_limit=3,
            exploration_limit=0,
            build_artifacts=True,
        )
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["materialized_view"]["candidate_count"], 2)
        self.assertTrue(result["profile_completion"]["provider_enabled"])
        self.assertEqual(result["profile_completion"]["fetched_profile_count"], 2)
        self.assertEqual(len(result["profile_completion"]["completed_candidates"]), 1)
        candidate = self.store.get_candidate("former1")
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertIn("Stanford", candidate.education)
        self.assertIn("Acme", candidate.work_history)
        self.assertIsNotNone(self.store.get_candidate("current1"))
        artifact_summary_path = Path(result["artifact_result"]["artifact_paths"]["artifact_summary"])
        self.assertTrue(artifact_summary_path.exists())


if __name__ == "__main__":
    unittest.main()
