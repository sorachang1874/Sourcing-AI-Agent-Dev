import json
from hashlib import sha1
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from sourcing_agent.company_asset_completion import CompanyAssetCompletionManager
from sourcing_agent.company_asset_supplement import CompanyAssetSupplementManager
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import SQLiteStore


class CompanyAssetSupplementTest(unittest.TestCase):
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
                        "linkedin_company_url": "https://www.linkedin.com/company/acme/",
                        "aliases": ["acme ai"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Acme",
                    "canonical_name": "Acme",
                    "company_key": "acme",
                    "linkedin_slug": "acme",
                    "linkedin_company_url": "https://www.linkedin.com/company/acme/",
                    "aliases": ["acme ai"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2))
        self.store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")
        self.settings = load_settings(self.project_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_run_former_search_seed_persists_candidates_and_summary(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        search_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_summary_path.write_text(json.dumps({"ok": True}, ensure_ascii=False, indent=2))
        fake_snapshot = SearchSeedSnapshot(
            snapshot_id="20260406T120000",
            target_company="Acme",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "full_name": "Former Example",
                    "headline": "Research Engineer at NewCo",
                    "location": "San Francisco",
                    "source_type": "harvest_profile_search",
                    "source_query": "__past_company_only__",
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                    "slug": "former-example",
                    "employment_status": "former",
                    "target_company": "Acme",
                    "metadata": {"current_company": "NewCo"},
                }
            ],
            query_summaries=[{"query": "__past_company_only__", "mode": "harvest_profile_search", "seed_entry_count": 1}],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=search_summary_path,
        )

        with mock.patch("sourcing_agent.company_asset_supplement.SearchSeedAcquirer.discover", return_value=fake_snapshot):
            result = manager.run_former_search_seed(
                target_company="Acme",
                snapshot_id="20260406T120000",
                limit=80,
                pages=4,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["inserted_candidate_count"], 1)
        self.assertEqual(result["requested_limit"], 80)
        self.assertEqual(result["requested_pages"], 4)
        stored = self.store.list_candidates_for_company("Acme")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].display_name, "Former Example")
        self.assertTrue(Path(result["summary_path"]).exists())

    def test_run_former_search_seed_inherits_snapshot_locations(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (harvest_dir / "harvest_company_employees_raw.json").write_text(
            json.dumps(
                {
                    "segmented": True,
                    "shards": [
                        {"company_filters": {"locations": ["United States"], "function_ids": ["8"]}},
                        {"company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]}},
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        search_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_summary_path.write_text(json.dumps({"ok": True}, ensure_ascii=False, indent=2))
        fake_snapshot = SearchSeedSnapshot(
            snapshot_id="20260406T120000",
            target_company="Acme",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=search_summary_path,
        )
        captured: dict[str, object] = {}

        def _capture_discover(*args, **kwargs):
            captured["filter_hints"] = dict(kwargs.get("filter_hints") or {})
            return fake_snapshot

        with mock.patch("sourcing_agent.company_asset_supplement.SearchSeedAcquirer.discover", side_effect=_capture_discover):
            result = manager.run_former_search_seed(
                target_company="Acme",
                snapshot_id="20260406T120000",
                limit=80,
                pages=4,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            captured["filter_hints"],
            {
                "past_companies": ["https://www.linkedin.com/company/acme/"],
                "locations": ["United States"],
            },
        )

    def test_run_former_search_seed_expands_to_probe_total_before_discovery(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        search_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_summary_path.write_text(json.dumps({"ok": True}, ensure_ascii=False, indent=2))
        fake_snapshot = SearchSeedSnapshot(
            snapshot_id="20260406T120000",
            target_company="Acme",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[
                {
                    "query": "__past_company_only__",
                    "mode": "harvest_profile_search",
                    "requested_limit": 500,
                    "requested_pages": 20,
                    "effective_limit": 559,
                    "effective_pages": 23,
                    "seed_entry_count": 559,
                    "probe": {
                        "probe_performed": True,
                        "probe_query_text": "",
                        "probe_limit": 25,
                        "probe_pages": 1,
                        "requested_limit": 500,
                        "requested_pages": 20,
                        "effective_limit": 559,
                        "effective_pages": 23,
                        "provider_total_count": 559,
                        "provider_total_pages": 23,
                        "probe_returned_count": 25,
                        "probe_raw_path": "/tmp/probe.json",
                    },
                }
            ],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=search_summary_path,
        )
        captured: dict[str, object] = {}

        def _capture_discover(*args, **kwargs):
            captured["cost_policy"] = dict(kwargs.get("cost_policy") or {})
            return fake_snapshot

        with mock.patch("sourcing_agent.company_asset_supplement.SearchSeedAcquirer.discover", side_effect=_capture_discover):
            result = manager.run_former_search_seed(
                target_company="Acme",
                snapshot_id="20260406T120000",
                limit=500,
                pages=20,
            )

        self.assertEqual(result["requested_limit"], 500)
        self.assertEqual(result["requested_pages"], 20)
        self.assertEqual(result["effective_limit"], 559)
        self.assertEqual(result["effective_pages"], 23)
        self.assertEqual(result["former_search_probe"]["provider_total_count"], 559)
        self.assertEqual(captured["cost_policy"]["provider_people_search_min_expected_results"], 500)
        self.assertEqual(captured["cost_policy"]["provider_people_search_pages"], 20)

    def test_repair_current_roster_profile_refs_restores_visible_roster_url(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        visible_path.write_text(
            json.dumps(
                [
                    {
                        "full_name": "Alice Example",
                        "member_key": "opaque-alice",
                        "linkedin_url": "https://www.linkedin.com/in/opaque-alice",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        candidate_id = sha1("acme|aliceexample|opaque-alice".encode("utf-8")).hexdigest()[:16]
        self.store.upsert_candidate(
            Candidate(
                candidate_id=candidate_id,
                name_en="Alice Example",
                display_name="Alice Example",
                category="employee",
                target_company="Acme",
                organization="Acme",
                employment_status="current",
                linkedin_url="https://www.linkedin.com/in/alice-example",
                source_dataset="acme_linkedin_company_people",
                metadata={
                    "profile_url": "https://www.linkedin.com/in/alice-example",
                    "more_profiles": ["https://www.linkedin.com/in/legacy-alice"],
                },
            )
        )

        result = manager.repair_current_roster_profile_refs(target_company="Acme", snapshot_id="20260406T120000")

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["repaired_count"], 1)
        updated = self.store.get_candidate(candidate_id)
        assert updated is not None
        self.assertEqual(updated.linkedin_url, "https://www.linkedin.com/in/opaque-alice")
        self.assertEqual(updated.metadata.get("profile_url"), "https://www.linkedin.com/in/opaque-alice")
        self.assertIn("https://www.linkedin.com/in/alice-example", updated.metadata.get("more_profiles") or [])


if __name__ == "__main__":
    unittest.main()
