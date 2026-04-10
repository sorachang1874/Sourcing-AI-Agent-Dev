import tempfile
import unittest

from sourcing_agent.storage import SQLiteStore


class StorageProfileRegistryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(f"{self.tempdir.name}/registry.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_registry_lifecycle_updates_status_retry_and_sources(self) -> None:
        profile_url = "https://www.linkedin.com/in/Registry-Test-User/"
        self.store.mark_linkedin_profile_registry_queued(
            profile_url,
            source_shards=["seed:query_a"],
            source_jobs=["job_1"],
            run_id="run_queued",
            dataset_id="dataset_queued",
            snapshot_dir="/tmp/snapshot_a",
        )
        queued = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(queued)
        assert queued is not None
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["retry_count"], 0)
        self.assertIn("seed:query_a", queued["source_shards"])
        self.assertIn("job_1", queued["source_jobs"])

        self.store.mark_linkedin_profile_registry_failed(
            profile_url,
            error="temporary timeout",
            retryable=True,
            source_shards=["seed:query_b"],
            source_jobs=["job_2"],
        )
        failed_once = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(failed_once)
        assert failed_once is not None
        self.assertEqual(failed_once["status"], "failed_retryable")
        self.assertEqual(failed_once["retry_count"], 1)
        self.assertIn("seed:query_a", failed_once["source_shards"])
        self.assertIn("seed:query_b", failed_once["source_shards"])
        self.assertIn("job_2", failed_once["source_jobs"])

        self.store.mark_linkedin_profile_registry_failed(
            profile_url,
            error="temporary timeout again",
            retryable=True,
        )
        failed_twice = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(failed_twice)
        assert failed_twice is not None
        self.assertEqual(failed_twice["retry_count"], 2)

        self.store.mark_linkedin_profile_registry_fetched(
            profile_url,
            raw_path="/tmp/raw_profile.json",
            source_jobs=["job_3"],
            run_id="run_done",
            dataset_id="dataset_done",
            snapshot_dir="/tmp/snapshot_b",
        )
        fetched = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["status"], "fetched")
        self.assertEqual(fetched["retry_count"], 0)
        self.assertEqual(fetched["last_error"], "")
        self.assertEqual(fetched["last_raw_path"], "/tmp/raw_profile.json")
        self.assertEqual(fetched["last_run_id"], "run_done")
        self.assertEqual(fetched["last_dataset_id"], "dataset_done")
        self.assertIn("job_3", fetched["source_jobs"])

    def test_registry_preserves_unrecoverable_status_until_fetched(self) -> None:
        profile_url = "linkedin.com/in/unrecoverable-user"
        self.store.mark_linkedin_profile_registry_failed(
            profile_url,
            error="member restricted",
            retryable=False,
        )
        unrecoverable = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(unrecoverable)
        assert unrecoverable is not None
        self.assertEqual(unrecoverable["status"], "unrecoverable")

        self.store.mark_linkedin_profile_registry_queued(profile_url, source_shards=["seed:retry"])
        still_unrecoverable = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(still_unrecoverable)
        assert still_unrecoverable is not None
        self.assertEqual(still_unrecoverable["status"], "unrecoverable")

        self.store.mark_linkedin_profile_registry_fetched(
            profile_url,
            raw_path="/tmp/fetched_after_override.json",
        )
        fetched = self.store.get_linkedin_profile_registry(profile_url)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["status"], "fetched")

    def test_registry_bulk_lookup_returns_normalized_keys(self) -> None:
        profile_url = "https://www.linkedin.com/in/Bulk-User/"
        normalized_key = self.store.normalize_linkedin_profile_url(profile_url)
        self.store.mark_linkedin_profile_registry_queued(profile_url, source_shards=["seed:bulk"])
        bulk = self.store.get_linkedin_profile_registry_bulk([profile_url, "https://www.linkedin.com/in/missing"])
        self.assertIn(normalized_key, bulk)
        self.assertEqual(bulk[normalized_key]["profile_url"], profile_url)

    def test_registry_alias_lookup_resolves_to_canonical_entry(self) -> None:
        canonical_url = "https://www.linkedin.com/in/alice-example/"
        alias_raw_url = "linkedin.com/in/ALICE-EXAMPLE"
        alias_sanity_url = "https://www.linkedin.com/in/alice-example"
        self.store.mark_linkedin_profile_registry_fetched(
            canonical_url,
            raw_path="/tmp/alice-example.json",
            alias_urls=[alias_raw_url, alias_sanity_url],
            raw_linkedin_url=alias_raw_url,
            sanity_linkedin_url=alias_sanity_url,
        )

        by_alias = self.store.get_linkedin_profile_registry(alias_raw_url)
        self.assertIsNotNone(by_alias)
        assert by_alias is not None
        self.assertEqual(by_alias["profile_url_key"], self.store.normalize_linkedin_profile_url(canonical_url))
        alias_keys = {
            self.store.normalize_linkedin_profile_url(item)
            for item in list(by_alias.get("alias_urls", []))
            if str(item or "").strip()
        }
        self.assertIn(self.store.normalize_linkedin_profile_url(alias_sanity_url), alias_keys)

        bulk = self.store.get_linkedin_profile_registry_bulk([alias_raw_url, canonical_url])
        self.assertIn(self.store.normalize_linkedin_profile_url(alias_raw_url), bulk)
        self.assertIn(self.store.normalize_linkedin_profile_url(canonical_url), bulk)

    def test_registry_url_level_lease_blocks_duplicate_claims(self) -> None:
        profile_url = "https://www.linkedin.com/in/lease-test/"
        lease_a = self.store.acquire_linkedin_profile_registry_lease(profile_url, lease_owner="worker-a", lease_seconds=120)
        self.assertTrue(lease_a.get("acquired"))
        self.assertEqual(lease_a.get("lease_owner"), "worker-a")

        lease_b = self.store.acquire_linkedin_profile_registry_lease(profile_url, lease_owner="worker-b", lease_seconds=120)
        self.assertFalse(lease_b.get("acquired"))
        self.assertEqual(lease_b.get("lease_owner"), "worker-a")

        released = self.store.release_linkedin_profile_registry_lease(profile_url, lease_owner="worker-a")
        self.assertTrue(released)

        lease_c = self.store.acquire_linkedin_profile_registry_lease(profile_url, lease_owner="worker-b", lease_seconds=120)
        self.assertTrue(lease_c.get("acquired"))
        self.assertEqual(lease_c.get("lease_owner"), "worker-b")

    def test_profile_registry_metrics_summary(self) -> None:
        profile_url = "https://www.linkedin.com/in/metrics-user/"
        self.store.record_linkedin_profile_registry_event(profile_url, event_type="lookup_attempt")
        self.store.record_linkedin_profile_registry_event(profile_url, event_type="cache_hit_registry")
        self.store.record_linkedin_profile_registry_event(profile_url, event_type="live_fetch_requested")
        self.store.record_linkedin_profile_registry_event(profile_url, event_type="duplicate_fetch_blocked")
        self.store.record_linkedin_profile_registry_event(
            profile_url,
            event_type="live_fetch_success",
            metadata={"retry_count_before": 1},
            duration_ms=1250,
        )
        self.store.record_linkedin_profile_registry_event(
            profile_url,
            event_type="live_fetch_failed",
            event_status="unrecoverable",
            metadata={"retry_count_before": 2},
        )

        metrics = self.store.get_linkedin_profile_registry_metrics(lookback_hours=0)
        self.assertGreaterEqual(metrics["cache_hit_rate"], 1.0)
        self.assertGreaterEqual(metrics["duplicate_request_rate"], 0.5)
        self.assertEqual(metrics["retry_success_count"], 1)
        self.assertEqual(metrics["retry_failure_count"], 1)
        self.assertGreater(metrics["queued_duration_ms_p50"], 0)
        self.assertGreater(metrics["unrecoverable_ratio"], 0.0)

    def test_backfill_run_checkpoint_persistence(self) -> None:
        run = self.store.upsert_linkedin_profile_registry_backfill_run(
            "run::anthropic::snapshot",
            scope_company="anthropic",
            scope_snapshot_id="20260410T000000",
            checkpoint={"last_processed_path": "/tmp/a.json"},
            summary={"processed_total": 10},
            status="running",
        )
        self.assertIsNotNone(run)
        fetched = self.store.get_linkedin_profile_registry_backfill_run("run::anthropic::snapshot")
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["scope_company"], "anthropic")
        self.assertEqual(fetched["checkpoint"]["last_processed_path"], "/tmp/a.json")


if __name__ == "__main__":
    unittest.main()
