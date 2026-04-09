import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.company_asset_completion import CompanyAssetCompletionManager
from sourcing_agent.domain import Candidate
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import SQLiteStore


class _FakeHarvestProfileConnector:
    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None, use_cache=True):
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
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                    "headline": "Research Engineer at NewCo",
                    "summary": "Former Example previously worked at Acme.",
                    "location": "San Francisco",
                    "current_company": "NewCo",
                    "experience": [
                        {"companyName": "Acme", "title": "Member of Technical Staff"},
                        {"companyName": "NewCo", "title": "Research Engineer"},
                    ],
                    "education": [{"school": "Stanford University", "degree": "MS"}],
                    "more_profiles": [],
                    "public_identifier": "former-example",
                    "publications": [],
                },
            }
        return results

    def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None, use_cache=True):
        return self.fetch_profiles_by_urls([profile_url], snapshot_dir, asset_logger=asset_logger, use_cache=use_cache).get(profile_url)


class _ShuffledHarvestProfileConnector:
    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None, use_cache=True):
        raw_dir = Path(snapshot_dir) / "harvest_profiles"
        raw_dir.mkdir(parents=True, exist_ok=True)
        profiles = [
            {
                "full_name": "First Example",
                "profile_url": "https://www.linkedin.com/in/first-example/",
                "headline": "Engineer at OtherCo",
                "summary": "First Example previously worked at Acme.",
                "location": "San Francisco",
                "current_company": "OtherCo",
                "experience": [{"companyName": "Acme", "title": "Engineer"}],
                "education": [{"school": "MIT", "degree": "BS"}],
                "more_profiles": [],
                "public_identifier": "first-example",
                "publications": [],
            },
            {
                "full_name": "Second Example",
                "profile_url": "https://www.linkedin.com/in/second-example/",
                "headline": "Scientist at OtherCo",
                "summary": "Second Example previously worked at Acme.",
                "location": "San Francisco",
                "current_company": "OtherCo",
                "experience": [{"companyName": "Acme", "title": "Scientist"}],
                "education": [{"school": "Stanford", "degree": "MS"}],
                "more_profiles": [],
                "public_identifier": "second-example",
                "publications": [],
            },
        ]
        raw_paths = []
        for index, profile in enumerate(profiles, start=1):
            raw_path = raw_dir / f"shuffled_{index}.json"
            raw_path.write_text(json.dumps({"ok": True, "profile": profile}))
            raw_paths.append(raw_path)
        return {
            profile_urls[0]: {"raw_path": raw_paths[0], "account_id": "fake_harvest", "parsed": profiles[1]},
            profile_urls[1]: {"raw_path": raw_paths[1], "account_id": "fake_harvest", "parsed": profiles[0]},
        }

    def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None, use_cache=True):
        return None


class _NonMemberHarvestProfileConnector:
    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None, use_cache=True):
        raw_path = Path(snapshot_dir) / "harvest_profiles" / "non_member_profile.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"ok": True}))
        return {
            url: {
                "raw_path": raw_path,
                "account_id": "fake_harvest",
                "parsed": {
                    "full_name": "False Positive",
                    "profile_url": "https://www.linkedin.com/in/false-positive/",
                    "headline": "Engineer at OtherCo",
                    "summary": "Did not work at Acme.",
                    "location": "San Francisco",
                    "current_company": "OtherCo",
                    "experience": [{"companyName": "OtherCo", "title": "Engineer"}],
                    "education": [{"school": "Stanford University", "degree": "MS"}],
                    "more_profiles": [],
                    "public_identifier": "false-positive",
                    "publications": [],
                },
            }
            for url in profile_urls
        }

    def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None, use_cache=True):
        raw_path = Path(snapshot_dir) / "harvest_profiles" / "non_member_profile.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"ok": True}))
        return {
            "raw_path": raw_path,
            "account_id": "fake_harvest",
            "parsed": {
                "full_name": "False Positive",
                "profile_url": "https://www.linkedin.com/in/false-positive/",
                "headline": "Engineer at OtherCo",
                "summary": "Did not work at Acme.",
                "location": "San Francisco",
                "current_company": "OtherCo",
                "experience": [{"companyName": "OtherCo", "title": "Engineer"}],
                "education": [{"school": "Stanford University", "degree": "MS"}],
                "more_profiles": [],
                "public_identifier": "false-positive",
                "publications": [],
            },
        }


class _SuspiciousHarvestProfileConnector:
    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None, use_cache=True):
        raw_path = Path(snapshot_dir) / "harvest_profiles" / "suspicious_profile.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"ok": True}))
        return {
            url: {
                "raw_path": raw_path,
                "account_id": "fake_harvest",
                "parsed": {
                    "full_name": "Suspicious Example",
                    "profile_url": "https://www.linkedin.com/in/suspicious-example/",
                    "headline": "Spiritual healer and tarot advisor",
                    "summary": "Spiritual healing, tarot, spell work, and psychic support.",
                    "location": "San Francisco",
                    "current_company": "Acme",
                    "experience": [{"companyName": "Acme", "title": "Advisor", "description": "Psychic healing"}],
                    "education": [{"school": "Stanford University", "degree": "MS"}],
                    "more_profiles": [],
                    "public_identifier": "suspicious-example",
                    "publications": [],
                },
            }
            for url in profile_urls
        }

    def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None, use_cache=True):
        return self.fetch_profiles_by_urls([profile_url], snapshot_dir, asset_logger=asset_logger, use_cache=use_cache).get(profile_url)


class _SuspiciousMembershipModelClient(DeterministicModelClient):
    def judge_profile_membership(self, payload: dict[str, object]) -> dict[str, str]:
        triggers = dict(payload.get("heuristic_triggers") or {})
        keywords = [str(item).strip().lower() for item in list(triggers.get("keywords") or [])]
        if "spiritual" in keywords or "healer" in keywords:
            return {
                "decision": "suspicious_member",
                "confidence_label": "high",
                "rationale": "Structured company match exists, but the profile content looks unrelated and suspicious.",
            }
        return super().judge_profile_membership(payload)


class _RefreshBatchOnlyHarvestProfileConnector:
    def __init__(self) -> None:
        self.batch_calls: list[dict[str, object]] = []

    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None, use_cache=True):
        self.batch_calls.append({"urls": list(profile_urls), "use_cache": use_cache})
        raw_path = Path(snapshot_dir) / "harvest_profiles" / "refresh_batch_profile.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"ok": True}))
        if use_cache:
            return {
                url: {
                    "raw_path": raw_path,
                    "account_id": "fake_harvest",
                    "parsed": {
                        "full_name": "Different Person",
                        "profile_url": url,
                        "headline": "Engineer at OtherCo",
                        "summary": "No useful match.",
                        "location": "San Francisco",
                        "current_company": "OtherCo",
                        "experience": [{"companyName": "OtherCo", "title": "Engineer"}],
                        "education": [],
                        "more_profiles": [],
                        "public_identifier": "different-person",
                        "publications": [],
                    },
                }
                for url in profile_urls
            }
        return {
            url: {
                "raw_path": raw_path,
                "account_id": "fake_harvest",
                "parsed": {
                    "full_name": "Former Example",
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                    "headline": "Research Engineer at NewCo",
                    "summary": "Former Example previously worked at Acme.",
                    "location": "San Francisco",
                    "current_company": "NewCo",
                    "experience": [
                        {"companyName": "Acme", "title": "Member of Technical Staff"},
                        {"companyName": "NewCo", "title": "Research Engineer"},
                    ],
                    "education": [{"school": "Stanford University", "degree": "MS"}],
                    "more_profiles": [],
                    "public_identifier": "former-example",
                    "publications": [],
                },
            }
            for url in profile_urls
        }

    def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None, use_cache=True):
        raise AssertionError("refresh path should use batched fetch_profiles_by_urls instead of single profile fetches")


class _ParallelTrackingHarvestProfileConnector:
    def __init__(self) -> None:
        self.batch_calls: list[list[str]] = []
        self._lock = threading.Lock()
        self._active_calls = 0
        self.max_active_calls = 0
        self._second_batch_started = threading.Event()

    def fetch_profiles_by_urls(self, profile_urls, snapshot_dir, asset_logger=None, use_cache=True):
        with self._lock:
            self.batch_calls.append(list(profile_urls))
            self._active_calls += 1
            self.max_active_calls = max(self.max_active_calls, self._active_calls)
            if len(self.batch_calls) >= 2:
                self._second_batch_started.set()
        self._second_batch_started.wait(timeout=0.5)
        raw_path = Path(snapshot_dir) / "harvest_profiles" / f"parallel_{len(profile_urls)}.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"ok": True, "use_cache": use_cache}))
        try:
            return {
                url: {
                    "raw_path": raw_path,
                    "account_id": "fake_harvest",
                    "parsed": {"profile_url": url, "full_name": url.rsplit("/", 1)[-1]},
                }
                for url in profile_urls
            }
        finally:
            with self._lock:
                self._active_calls -= 1

    def fetch_profile_by_url(self, profile_url, snapshot_dir, asset_logger=None, use_cache=True):
        return self.fetch_profiles_by_urls([profile_url], snapshot_dir, asset_logger=asset_logger, use_cache=use_cache).get(profile_url)


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
                linkedin_url="https://www.linkedin.com/in/ACwAAOldFormer/",
                metadata={"seed_slug": "ACwAAOldFormer"},
                source_dataset="acme_search_seed",
            )
        )
        self.settings = load_settings(self.project_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_load_settings_defaults_profile_scraper_to_email_collection(self) -> None:
        self.assertTrue(self.settings.harvest.profile_scraper.collect_email)

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
        self.assertEqual(candidate.linkedin_url, "https://www.linkedin.com/in/former-example/")
        self.assertIsNotNone(self.store.get_candidate("current1"))
        artifact_summary_path = Path(result["artifact_result"]["artifact_paths"]["artifact_summary"])
        self.assertTrue(artifact_summary_path.exists())

    def test_complete_company_assets_matches_profiles_even_when_batch_response_is_reordered(self) -> None:
        self.store.upsert_candidate(
            Candidate(
                candidate_id="former2",
                name_en="First Example",
                display_name="First Example",
                category="former_employee",
                target_company="Acme",
                employment_status="former",
                linkedin_url="https://www.linkedin.com/in/ACwAAFirst/",
                metadata={"seed_slug": "ACwAAFirst"},
                source_dataset="acme_search_seed",
            )
        )
        self.store.upsert_candidate(
            Candidate(
                candidate_id="former3",
                name_en="Second Example",
                display_name="Second Example",
                category="former_employee",
                target_company="Acme",
                employment_status="former",
                linkedin_url="https://www.linkedin.com/in/ACwAASecond/",
                metadata={"seed_slug": "ACwAASecond"},
                source_dataset="acme_search_seed",
            )
        )
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=_ShuffledHarvestProfileConnector(),
        )
        result = manager.complete_company_assets(
            target_company="Acme",
            profile_detail_limit=4,
            exploration_limit=0,
            build_artifacts=False,
        )
        completed_ids = {item["candidate_id"] for item in result["profile_completion"]["completed_candidates"]}
        self.assertIn("former2", completed_ids)
        self.assertIn("former3", completed_ids)
        first = self.store.get_candidate("former2")
        second = self.store.get_candidate("former3")
        assert first is not None
        assert second is not None
        self.assertEqual(first.linkedin_url, "https://www.linkedin.com/in/first-example/")
        self.assertEqual(second.linkedin_url, "https://www.linkedin.com/in/second-example/")

    def test_complete_company_assets_refreshes_unresolved_profiles_via_batch_call(self) -> None:
        connector = _RefreshBatchOnlyHarvestProfileConnector()
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=connector,
        )
        result = manager.complete_company_assets(
            target_company="Acme",
            profile_detail_limit=3,
            exploration_limit=0,
            build_artifacts=False,
        )
        self.assertEqual(len(connector.batch_calls), 4)
        self.assertTrue(connector.batch_calls[0]["use_cache"])
        self.assertFalse(connector.batch_calls[1]["use_cache"])
        self.assertEqual(
            set(connector.batch_calls[0]["urls"]),
            {
                "https://www.linkedin.com/in/ACwAAOldFormer/",
                "https://www.linkedin.com/in/current-snapshot",
            },
        )
        self.assertEqual(set(connector.batch_calls[0]["urls"]), set(connector.batch_calls[1]["urls"]))
        self.assertEqual(connector.batch_calls[2], {"urls": ["https://www.linkedin.com/in/current-snapshot"], "use_cache": True})
        self.assertEqual(connector.batch_calls[3], {"urls": ["https://www.linkedin.com/in/current-snapshot"], "use_cache": False})
        completed_ids = {item["candidate_id"] for item in result["profile_completion"]["completed_candidates"]}
        self.assertIn("former1", completed_ids)

    def test_complete_company_assets_marks_name_matched_non_member_profiles(self) -> None:
        self.store.upsert_candidate(
            Candidate(
                candidate_id="former_false_positive",
                name_en="False Positive",
                display_name="False Positive",
                category="former_employee",
                target_company="Acme",
                employment_status="former",
                linkedin_url="https://www.linkedin.com/in/ACwAAFalsePositive/",
                metadata={"seed_slug": "ACwAAFalsePositive"},
                source_dataset="acme_search_seed",
            )
        )
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=_NonMemberHarvestProfileConnector(),
        )
        result = manager.complete_company_assets(
            target_company="Acme",
            profile_detail_limit=4,
            exploration_limit=0,
            build_artifacts=False,
        )
        non_member_ids = {item["candidate_id"] for item in result["profile_completion"]["non_member_candidates"]}
        self.assertIn("former_false_positive", non_member_ids)
        candidate = self.store.get_candidate("former_false_positive")
        assert candidate is not None
        self.assertEqual(candidate.category, "non_member")
        self.assertEqual(candidate.employment_status, "")
        self.assertTrue(candidate.metadata.get("target_company_mismatch"))

    def test_complete_company_assets_routes_suspicious_membership_to_manual_review(self) -> None:
        self.store.upsert_candidate(
            Candidate(
                candidate_id="suspicious_member",
                name_en="Suspicious Example",
                display_name="Suspicious Example",
                category="employee",
                target_company="Acme",
                employment_status="current",
                linkedin_url="https://www.linkedin.com/in/suspicious-example/",
                source_dataset="acme_roster",
            )
        )
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=_SuspiciousMembershipModelClient(),
            harvest_profile_connector=_SuspiciousHarvestProfileConnector(),
        )
        result = manager.complete_company_assets(
            target_company="Acme",
            profile_detail_limit=4,
            exploration_limit=0,
            build_artifacts=False,
        )
        manual_review_ids = {item["candidate_id"] for item in result["profile_completion"]["manual_review_candidates"]}
        self.assertIn("suspicious_member", manual_review_ids)
        candidate = self.store.get_candidate("suspicious_member")
        assert candidate is not None
        self.assertTrue(candidate.metadata.get("membership_review_required"))
        self.assertEqual(candidate.metadata.get("membership_review_reason"), "suspicious_membership")

    def test_complete_snapshot_profiles_filters_by_employment_scope(self) -> None:
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=_FakeHarvestProfileConnector(),
        )
        captured: dict[str, object] = {}

        def _capture_targets(**kwargs):
            captured["candidate_ids"] = [candidate.candidate_id for candidate in kwargs["candidates"]]
            return {
                "provider_enabled": True,
                "requested_candidate_count": len(kwargs["candidates"]),
                "requested_url_count": len(kwargs["candidates"]),
                "fetched_profile_count": 0,
                "resolved_candidate_count": 0,
                "completed_candidates": [],
                "non_member_candidates": [],
                "manual_review_candidates": [],
                "skipped_candidates": [],
                "errors": [],
            }

        with mock.patch.object(manager, "_complete_known_profile_targets", side_effect=_capture_targets):
            result = manager.complete_snapshot_profiles(
                target_company="Acme",
                snapshot_id="20260406T120000",
                employment_scope="current",
                profile_limit=0,
                only_missing_profile_detail=True,
                build_artifacts=False,
            )

        self.assertEqual(result["employment_scope"], "current")
        self.assertEqual(captured["candidate_ids"], ["current1"])

    def test_complete_snapshot_profiles_can_force_refresh_profile_fetches(self) -> None:
        connector = _RefreshBatchOnlyHarvestProfileConnector()
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=connector,
        )
        result = manager.complete_snapshot_profiles(
            target_company="Acme",
            snapshot_id="20260406T120000",
            employment_scope="current",
            profile_limit=1,
            only_missing_profile_detail=False,
            force_refresh=True,
            build_artifacts=False,
        )

        self.assertEqual(result["force_refresh"], True)
        self.assertGreaterEqual(len(connector.batch_calls), 1)
        self.assertEqual(connector.batch_calls[0]["use_cache"], False)

    def test_fetch_profile_batches_submits_multiple_batches_in_parallel(self) -> None:
        connector = _ParallelTrackingHarvestProfileConnector()
        manager = CompanyAssetCompletionManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
            harvest_profile_connector=connector,
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        logger = AssetLogger(snapshot_dir)
        urls = [f"https://www.linkedin.com/in/parallel-batch-{index:03d}/" for index in range(250)]

        fetched, errors = manager._fetch_profile_batches(
            urls,
            snapshot_dir=snapshot_dir,
            logger=logger,
            use_cache=False,
        )

        self.assertEqual(errors, [])
        self.assertEqual(len(fetched), 250)
        self.assertEqual(len(connector.batch_calls), 3)
        self.assertGreaterEqual(connector.max_active_calls, 2)


if __name__ == "__main__":
    unittest.main()
