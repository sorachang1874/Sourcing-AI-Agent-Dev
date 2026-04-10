import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.profile_registry_backfill import backfill_linkedin_profile_registry
from sourcing_agent.storage import SQLiteStore


class ProfileRegistryBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name) / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_backfill_populates_registry_from_historical_harvest_payloads(self) -> None:
        harvest_dir = self.runtime_dir / "company_assets" / "anthropic" / "20260410T000000" / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        fetched_payload = {
            "_harvest_request": {
                "kind": "url",
                "value": "https://www.linkedin.com/in/fetched-user/",
                "profile_url": "https://www.linkedin.com/in/fetched-user/",
            },
            "item": {
                "fullName": "Fetched User",
                "profileUrl": "https://www.linkedin.com/in/fetched-user/",
            },
        }
        unrecoverable_payload = {
            "_harvest_request": {
                "kind": "url",
                "value": "https://www.linkedin.com/in/restricted-user/",
                "profile_url": "https://www.linkedin.com/in/restricted-user/",
            },
            "errors": [{"error": "Member is restricted", "status": 404}],
        }
        (harvest_dir / "fetched.json").write_text(json.dumps(fetched_payload), encoding="utf-8")
        (harvest_dir / "restricted.json").write_text(json.dumps(unrecoverable_payload), encoding="utf-8")

        progress_updates: list[dict[str, object]] = []
        result = backfill_linkedin_profile_registry(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="anthropic",
            snapshot_id="20260410T000000",
            progress_interval=1,
            progress_callback=lambda payload: progress_updates.append(payload),
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["files_processed_this_run"], 2)
        self.assertGreaterEqual(len(progress_updates), 1)

        fetched_entry = self.store.get_linkedin_profile_registry("https://www.linkedin.com/in/fetched-user/")
        self.assertIsNotNone(fetched_entry)
        assert fetched_entry is not None
        self.assertEqual(fetched_entry["status"], "fetched")

        restricted_entry = self.store.get_linkedin_profile_registry("https://www.linkedin.com/in/restricted-user/")
        self.assertIsNotNone(restricted_entry)
        assert restricted_entry is not None
        self.assertEqual(restricted_entry["status"], "unrecoverable")

    def test_backfill_resume_skips_processed_paths(self) -> None:
        harvest_dir = self.runtime_dir / "company_assets" / "anthropic" / "20260410T000100" / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "_harvest_request": {
                "kind": "url",
                "value": "https://www.linkedin.com/in/first-user/",
                "profile_url": "https://www.linkedin.com/in/first-user/",
            },
            "item": {"fullName": "First User", "profileUrl": "https://www.linkedin.com/in/first-user/"},
        }
        (harvest_dir / "a_first.json").write_text(json.dumps(payload), encoding="utf-8")

        first_run = backfill_linkedin_profile_registry(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="anthropic",
            snapshot_id="20260410T000100",
            progress_interval=10,
        )
        self.assertEqual(first_run["files_processed_this_run"], 1)

        payload_second = {
            "_harvest_request": {
                "kind": "url",
                "value": "https://www.linkedin.com/in/second-user/",
                "profile_url": "https://www.linkedin.com/in/second-user/",
            },
            "item": {"fullName": "Second User", "profileUrl": "https://www.linkedin.com/in/second-user/"},
        }
        (harvest_dir / "z_second.json").write_text(json.dumps(payload_second), encoding="utf-8")

        second_run = backfill_linkedin_profile_registry(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="anthropic",
            snapshot_id="20260410T000100",
            progress_interval=10,
            resume=True,
        )
        self.assertEqual(second_run["files_processed_this_run"], 1)
        second_entry = self.store.get_linkedin_profile_registry("https://www.linkedin.com/in/second-user/")
        self.assertIsNotNone(second_entry)


if __name__ == "__main__":
    unittest.main()
