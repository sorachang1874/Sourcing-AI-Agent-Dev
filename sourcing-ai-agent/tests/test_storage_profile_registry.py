import json
import tempfile
import unittest
import unittest.mock

from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class StorageProfileRegistryTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = self.make_pg_store(f"{self.tempdir.name}/registry.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_registry_lifecycle_updates_status_retry_and_sources(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/Registry-Test-User/"
        repo.mark_queued(
            profile_url,
            source_shards=["seed:query_a"],
            source_jobs=["job_1"],
            run_id="run_queued",
            dataset_id="dataset_queued",
            snapshot_dir="/tmp/snapshot_a",
        )
        queued = repo.get(profile_url)
        self.assertIsNotNone(queued)
        assert queued is not None
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["retry_count"], 0)
        self.assertIn("seed:query_a", queued["source_shards"])
        self.assertIn("job_1", queued["source_jobs"])

        repo.mark_failed(
            profile_url,
            error="temporary timeout",
            retryable=True,
            source_shards=["seed:query_b"],
            source_jobs=["job_2"],
        )
        failed_once = repo.get(profile_url)
        self.assertIsNotNone(failed_once)
        assert failed_once is not None
        self.assertEqual(failed_once["status"], "failed_retryable")
        self.assertEqual(failed_once["retry_count"], 1)
        self.assertEqual(failed_once["refill_queue_state"], "retry_wait")
        self.assertEqual(failed_once["last_refill_trigger_kind"], "profile_retry")
        self.assertEqual(failed_once["last_refill_plan_reason"], "profile_retry_wait")
        self.assertEqual(failed_once["last_refill_deferred_reason"], "temporary timeout")
        self.assertEqual(failed_once["last_refill_attempt_count"], 0)
        self.assertTrue(failed_once["refill_not_before_at"])
        self.assertIn("seed:query_a", failed_once["source_shards"])
        self.assertIn("seed:query_b", failed_once["source_shards"])
        self.assertIn("job_2", failed_once["source_jobs"])

        repo.mark_failed(
            profile_url,
            error="temporary timeout again",
            retryable=True,
        )
        failed_twice = repo.get(profile_url)
        self.assertIsNotNone(failed_twice)
        assert failed_twice is not None
        self.assertEqual(failed_twice["status"], "unrecoverable")
        self.assertEqual(failed_twice["retry_count"], 2)
        self.assertEqual(failed_twice["refill_queue_state"], "")

        repo.mark_fetched(
            profile_url,
            raw_path="/tmp/raw_profile.json",
            source_jobs=["job_3"],
            run_id="run_done",
            dataset_id="dataset_done",
            snapshot_dir="/tmp/snapshot_b",
        )
        fetched = repo.get(profile_url)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["status"], "fetched")
        self.assertEqual(fetched["retry_count"], 0)
        self.assertEqual(fetched["last_error"], "")
        self.assertEqual(fetched["last_raw_path"], "/tmp/raw_profile.json")
        self.assertEqual(fetched["last_run_id"], "run_done")
        self.assertEqual(fetched["last_dataset_id"], "dataset_done")
        self.assertIn("job_3", fetched["source_jobs"])

    def test_retryable_failure_enters_refill_queue_after_not_before(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        future_url = "https://www.linkedin.com/in/retry-future/"
        ready_url = "https://www.linkedin.com/in/retry-ready/"
        repo.mark_failed(
            future_url,
            error="temporary provider timeout",
            retryable=True,
            retry_delay_seconds=3600,
            source_jobs=["job_retry"],
            snapshot_dir="/tmp/snapshot_retry",
        )
        repo.mark_failed(
            ready_url,
            error="temporary provider timeout",
            retryable=True,
            retry_delay_seconds=1,
            source_jobs=["job_retry"],
            snapshot_dir="/tmp/snapshot_retry",
        )
        repo.record_refill_plan_items(
            deferred_profile_urls=[ready_url],
            source_jobs=["job_retry"],
            snapshot_dir="/tmp/snapshot_retry",
            trigger_kind="profile_retry",
            plan_reason="profile_retry_wait",
            deferred_reason="temporary provider timeout",
            deferred_queue_state="retry_wait",
            refill_not_before_at="2000-01-01 00:00:00",
        )

        items = repo.list_refill_queue_items(
            states=["retry_wait"],
            source_job="job_retry",
            snapshot_dir="/tmp/snapshot_retry",
            limit=10,
        )

        self.assertEqual([item["profile_url"] for item in items], [ready_url])
        self.assertEqual(items[0]["status"], "failed_retryable")
        self.assertEqual(items[0]["refill_queue_state"], "retry_wait")
        self.assertEqual(items[0]["last_refill_attempt_count"], 0)
        self.assertEqual(
            repo.get(future_url)["refill_queue_state"],
            "retry_wait",
        )

    def test_batch_registry_lease_uses_bulk_alias_resolution(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_urls = [
            "https://www.linkedin.com/in/profile-bulk-alias-1/",
            "https://www.linkedin.com/in/profile-bulk-alias-2/",
            "https://www.linkedin.com/in/profile-bulk-alias-1-alias/",
        ]
        alias_key = normalize_linkedin_profile_url_key(profile_urls[2])
        canonical_key_1 = normalize_linkedin_profile_url_key(profile_urls[0])
        canonical_key_2 = normalize_linkedin_profile_url_key(profile_urls[1])
        select_calls: list[dict[str, object]] = []
        native_acquire_calls: list[list[str]] = []
        native_release_calls: list[list[str]] = []

        class _FakePostgres:
            def acquire_linkedin_profile_registry_leases(self, keys, **kwargs):
                native_acquire_calls.append(list(keys or []))
                return [
                    {
                        "profile_url_key": key,
                        "lease_owner": kwargs["lease_owner"],
                        "lease_token": kwargs["lease_token"],
                        "lease_expires_at": "2999-01-01 00:00:00",
                        "created_at": "2026-01-01 00:00:00",
                        "updated_at": "2026-01-01 00:00:00",
                    }
                    for key in list(keys or [])
                ]

            def release_linkedin_profile_registry_leases(self, keys, **kwargs):
                native_release_calls.append(list(keys or []))
                return len(list(keys or []))

        def _prefer_read(table_name: str) -> bool:
            return table_name in {
                "linkedin_profile_registry_aliases",
                "linkedin_profile_registry_leases",
            }

        def _skip_sqlite_fallback(table_name: str) -> bool:
            return table_name in {
                "linkedin_profile_registry_aliases",
                "linkedin_profile_registry_leases",
            }

        def _select_rows(table_name: str, **kwargs):
            select_calls.append(
                {
                    "table_name": table_name,
                    "where_sql": kwargs.get("where_sql"),
                    "params": list(kwargs.get("params") or []),
                }
            )
            if table_name != "linkedin_profile_registry_aliases":
                return []
            params = set(kwargs.get("params") or [])
            rows = []
            if alias_key in params:
                rows.append({"alias_url_key": alias_key, "profile_url_key": canonical_key_1})
            return rows

        def _per_url_resolver(_key: str) -> str:
            raise AssertionError("batch lease path must not use per-URL alias resolution")

        repo._adapter = _FakePostgres()  # type: ignore[assignment]
        repo._should_prefer_read = _prefer_read  # type: ignore[method-assign]
        repo._strict_authoritative = _skip_sqlite_fallback  # type: ignore[method-assign]
        repo._select_rows = _select_rows  # type: ignore[method-assign]
        repo._resolve_key = _per_url_resolver  # type: ignore[method-assign]

        acquire = repo.acquire_leases(
            profile_urls,
            lease_owner="owner-bulk-alias",
            lease_token="token-bulk-alias",
        )
        released = repo.release_leases(
            profile_urls,
            lease_owner="owner-bulk-alias",
            lease_token="token-bulk-alias",
        )

        self.assertTrue(acquire["acquired"])
        self.assertEqual(acquire["acquired_urls"], profile_urls)
        self.assertEqual(released, 2)
        self.assertEqual(native_acquire_calls, [[canonical_key_1, canonical_key_2]])
        self.assertEqual(native_release_calls, [[canonical_key_1, canonical_key_2]])
        self.assertEqual(len(select_calls), 2)
        self.assertEqual(select_calls[0]["table_name"], "linkedin_profile_registry_aliases")

    def test_refill_plan_items_use_bulk_postgres_registry_writes(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        active_urls = [
            "https://www.linkedin.com/in/refill-bulk-active-a/",
            "https://www.linkedin.com/in/refill-bulk-active-b/",
        ]
        deferred_urls = ["https://www.linkedin.com/in/refill-bulk-deferred/"]
        select_calls: list[dict[str, object]] = []

        class _FakePostgres:
            def __init__(self) -> None:
                self.bulk_upserts: list[tuple[str, list[dict[str, object]]]] = []

            def should_prefer_read(self, table_name: str) -> bool:
                return table_name in {
                    "linkedin_profile_registry",
                    "linkedin_profile_registry_aliases",
                }

            def is_authoritative(self, table_name: str) -> bool:
                return self.should_prefer_read(table_name)

            def bulk_upsert_rows(self, table_name, rows):
                payload_rows = [dict(row or {}) for row in list(rows or [])]
                self.bulk_upserts.append((str(table_name or ""), payload_rows))
                return len(payload_rows)

            def update_row_returning(self, **_kwargs):
                raise AssertionError("refill plan hot path must not use per-row PG updates")

        def _select_rows(table_name: str, **kwargs):
            select_calls.append(
                {
                    "table_name": table_name,
                    "where_sql": kwargs.get("where_sql"),
                    "params": list(kwargs.get("params") or []),
                }
            )
            return []

        def _per_url_resolver(_key: str) -> str:
            raise AssertionError("refill plan hot path must not use per-URL alias resolution")

        def _single_registry_read(_url: str):
            raise AssertionError("refill plan hot path must not read registry one URL at a time")

        fake_postgres = _FakePostgres()
        repo._adapter = fake_postgres  # type: ignore[assignment]
        repo._select_rows = _select_rows  # type: ignore[method-assign]
        repo._resolve_key = _per_url_resolver  # type: ignore[method-assign]
        repo.get = _single_registry_read  # type: ignore[method-assign]

        summary = repo.record_refill_plan_items(
            active_profile_urls=active_urls,
            deferred_profile_urls=deferred_urls,
            source_shards_by_url={active_urls[0]: ["openai::current"]},
            source_jobs=["job_refill_bulk"],
            snapshot_dir="/tmp/snapshot_refill_bulk",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="ready_to_dispatch",
            active_owner_worker_id=91,
            active_owner_run_id="run-refill-bulk",
            active_owner_dataset_id="dataset-refill-bulk",
            active_owner_payload_hash="payload-refill-bulk",
            deferred_reason="worker_budget_deferred",
        )

        self.assertEqual(summary["status"], "recorded")
        self.assertEqual(summary["active_item_count"], 2)
        self.assertEqual(summary["deferred_item_count"], 1)
        registry_bulk_upserts = [
            rows for table_name, rows in fake_postgres.bulk_upserts if table_name == "linkedin_profile_registry"
        ]
        alias_bulk_upserts = [
            rows for table_name, rows in fake_postgres.bulk_upserts if table_name == "linkedin_profile_registry_aliases"
        ]
        self.assertEqual(len(registry_bulk_upserts), 1)
        self.assertEqual(len(registry_bulk_upserts[0]), 3)
        self.assertEqual(len(alias_bulk_upserts), 1)
        self.assertEqual(len(alias_bulk_upserts[0]), 3)
        rows_by_key = {str(row["profile_url_key"]): row for row in registry_bulk_upserts[0]}
        active_key = normalize_linkedin_profile_url_key(active_urls[0])
        deferred_key = normalize_linkedin_profile_url_key(deferred_urls[0])
        self.assertEqual(rows_by_key[active_key]["refill_queue_state"], "planned_dispatch")
        self.assertEqual(rows_by_key[active_key]["refill_owner_worker_id"], 91)
        self.assertIn("openai::current", json.loads(str(rows_by_key[active_key]["source_shards_json"])))
        self.assertEqual(rows_by_key[deferred_key]["refill_queue_state"], "deferred_budget")
        self.assertEqual(rows_by_key[deferred_key]["refill_owner_worker_id"], 0)
        self.assertIn("job_refill_bulk", json.loads(str(rows_by_key[deferred_key]["source_jobs_json"])))
        self.assertLessEqual(len(select_calls), 3)
        self.assertEqual(select_calls[0]["table_name"], "linkedin_profile_registry_aliases")

    def test_postgres_batch_retryable_failure_moves_planned_dispatch_to_retry_wait(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/refill-bulk-retry/"
        profile_key = normalize_linkedin_profile_url_key(profile_url)
        existing_row = {
            "profile_url_key": profile_key,
            "profile_url": profile_url,
            "raw_linkedin_url": "",
            "sanity_linkedin_url": "",
            "status": "queued",
            "retry_count": 0,
            "last_error": "",
            "last_run_id": "run-initial",
            "last_dataset_id": "dataset-initial",
            "last_snapshot_dir": "/tmp/snapshot_retry_bulk",
            "last_raw_path": "",
            "first_queued_at": "2026-01-01 00:00:00",
            "last_queued_at": "2026-01-01 00:00:00",
            "last_fetched_at": "",
            "last_failed_at": "",
            "source_shards_json": json.dumps(["enrichment_background_prefetch"]),
            "source_jobs_json": json.dumps(["job_retry_bulk"]),
            "refill_queue_state": "planned_dispatch",
            "last_refill_trigger_kind": "profile_prefetch_provider_submit",
            "last_refill_plan_reason": "remote_provider_submitted",
            "last_refill_deferred_reason": "",
            "last_refill_planned_at": "2026-01-01 00:00:00",
            "refill_not_before_at": "",
            "refill_plan_batch_size": 50,
            "refill_plan_batch_count": 1,
            "refill_plan_window_url_count": 50,
            "last_refill_attempt_count": 1,
            "refill_owner_worker_id": 12,
            "refill_owner_run_id": "run-initial",
            "refill_owner_dataset_id": "dataset-initial",
            "refill_owner_payload_hash": "payload-initial",
            "refill_terminal_status": "",
            "refill_terminal_at": "",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
        }

        class _FakePostgres:
            def __init__(self) -> None:
                self.bulk_upserts: list[tuple[str, list[dict[str, object]]]] = []

            def should_prefer_read(self, table_name: str) -> bool:
                return table_name in {
                    "linkedin_profile_registry",
                    "linkedin_profile_registry_aliases",
                }

            def is_authoritative(self, table_name: str) -> bool:
                return self.should_prefer_read(table_name)

            def select_many(self, table_name, **kwargs):
                if table_name == "linkedin_profile_registry":
                    return [existing_row]
                return []

            def bulk_upsert_rows(self, table_name, rows):
                payload_rows = [dict(row or {}) for row in list(rows or [])]
                self.bulk_upserts.append((str(table_name or ""), payload_rows))
                return len(payload_rows)

        fake_postgres = _FakePostgres()
        repo._adapter = fake_postgres  # type: ignore[assignment]

        processed = repo.backfill_batch(
            [
                {
                    "profile_url": profile_url,
                    "status": "failed_retryable",
                    "error": "background_prefetch_unresolved",
                    "retryable": True,
                    "source_shards": ["enrichment_background_prefetch"],
                    "source_jobs": ["job_retry_bulk"],
                    "run_id": "run-failed",
                    "dataset_id": "dataset-failed",
                    "snapshot_dir": "/tmp/snapshot_retry_bulk",
                }
            ]
        )

        self.assertEqual(processed, 1)
        registry_bulk_upserts = [
            rows for table_name, rows in fake_postgres.bulk_upserts if table_name == "linkedin_profile_registry"
        ]
        self.assertEqual(len(registry_bulk_upserts), 1)
        retry_row = dict(registry_bulk_upserts[0][0])
        self.assertEqual(retry_row["profile_url_key"], profile_key)
        self.assertEqual(retry_row["status"], "failed_retryable")
        self.assertEqual(retry_row["retry_count"], 1)
        self.assertEqual(retry_row["refill_queue_state"], "retry_wait")
        self.assertEqual(retry_row["last_refill_trigger_kind"], "profile_retry")
        self.assertEqual(retry_row["last_refill_plan_reason"], "profile_retry_wait")
        self.assertEqual(retry_row["last_refill_deferred_reason"], "background_prefetch_unresolved")
        self.assertTrue(str(retry_row["refill_not_before_at"]))
        self.assertEqual(retry_row["refill_owner_worker_id"], 0)
        self.assertEqual(retry_row["refill_owner_run_id"], "")
        self.assertEqual(retry_row["refill_terminal_status"], "retryable_failed")

    def test_retryable_failure_without_workflow_scope_does_not_create_refill_queue_item(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        orphan_url = "https://www.linkedin.com/in/retry-orphan/"
        repo.mark_failed(
            orphan_url,
            error="maintenance fetch timeout",
            retryable=True,
            retry_delay_seconds=1,
            source_shards=["maintenance_completion"],
            snapshot_dir="/tmp/snapshot_orphan",
        )

        orphan = repo.get(orphan_url)
        self.assertIsNotNone(orphan)
        assert orphan is not None
        self.assertEqual(orphan["status"], "failed_retryable")
        self.assertEqual(orphan["retry_count"], 1)
        self.assertEqual(orphan["refill_queue_state"], "")
        self.assertEqual(orphan["refill_not_before_at"], "")
        self.assertEqual(
            repo.list_refill_queue_items(
                states=["retry_wait"],
                source_job="job_missing",
                snapshot_dir="/tmp/snapshot_orphan",
                limit=10,
            ),
            [],
        )

    def test_retryable_failure_inherits_existing_workflow_scope_for_refill_queue(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/retry-inherit-scope/"
        repo.mark_queued(
            profile_url,
            source_jobs=["job_existing_scope"],
            snapshot_dir="/tmp/snapshot_existing_scope",
        )
        repo.mark_failed(
            profile_url,
            error="provider timeout",
            retryable=True,
            retry_delay_seconds=1,
        )
        repo.record_refill_plan_items(
            deferred_profile_urls=[profile_url],
            source_jobs=["job_existing_scope"],
            snapshot_dir="/tmp/snapshot_existing_scope",
            trigger_kind="profile_retry",
            plan_reason="profile_retry_wait",
            deferred_reason="provider timeout",
            deferred_queue_state="retry_wait",
            refill_not_before_at="2000-01-01 00:00:00",
        )

        items = repo.list_refill_queue_items(
            states=["retry_wait"],
            source_job="job_existing_scope",
            snapshot_dir="/tmp/snapshot_existing_scope",
            limit=10,
        )
        self.assertEqual([item["profile_url"] for item in items], [profile_url])

    def test_provider_owned_planned_dispatch_is_gate_visible_but_not_ready_dispatchable(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/planned-dispatch-owned/"
        repo.mark_queued(
            profile_url,
            source_jobs=["job_planned_dispatch"],
            run_id="run-planned",
            dataset_id="dataset-planned",
            snapshot_dir="/tmp/snapshot_planned",
        )
        repo.record_refill_plan_items(
            active_profile_urls=[profile_url],
            source_jobs=["job_planned_dispatch"],
            snapshot_dir="/tmp/snapshot_planned",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
            active_owner_worker_id=123,
            active_owner_run_id="run-planned",
            active_owner_dataset_id="dataset-planned",
            active_owner_payload_hash="payload-planned",
        )

        ready_items = repo.list_refill_queue_items(
            states=["planned_dispatch"],
            source_job="job_planned_dispatch",
            snapshot_dir="/tmp/snapshot_planned",
            limit=10,
        )
        gate_items = repo.list_refill_queue_items(
            states=["planned_dispatch"],
            source_job="job_planned_dispatch",
            snapshot_dir="/tmp/snapshot_planned",
            limit=10,
            ready_only=False,
        )

        self.assertEqual(ready_items, [])
        self.assertEqual([item["profile_url"] for item in gate_items], [profile_url])
        self.assertEqual(gate_items[0]["refill_owner_worker_id"], 123)
        self.assertEqual(gate_items[0]["refill_owner_run_id"], "run-planned")

    def test_profile_registry_batch_leases_preserve_item_level_contention(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        first_url = "https://www.linkedin.com/in/batch-lease-first/"
        second_url = "https://www.linkedin.com/in/batch-lease-second/"
        repo.acquire_lease(
            second_url,
            lease_owner="other-worker",
            lease_seconds=120,
            lease_token="other-token",
        )

        batch = repo.acquire_leases(
            [first_url, second_url],
            lease_owner="batch-worker",
            lease_seconds=120,
            lease_token="batch-token",
        )

        self.assertFalse(batch["acquired"])
        self.assertEqual(batch["acquired_urls"], [first_url])
        self.assertEqual(batch["contended_urls"], [second_url])
        self.assertTrue(batch["leases_by_url"][first_url]["acquired"])
        self.assertFalse(batch["leases_by_url"][second_url]["acquired"])
        self.assertEqual(batch["leases_by_url"][second_url]["lease_owner"], "other-worker")

        released = repo.release_leases(
            [first_url, second_url],
            lease_owner="batch-worker",
            lease_token="batch-token",
        )

        self.assertEqual(released, 1)
        self.assertEqual(repo.get_lease(first_url), {})
        second_lease = repo.get_lease(second_url)
        assert second_lease is not None
        self.assertEqual(second_lease["lease_owner"], "other-worker")

    def test_retryable_failure_retries_once_then_becomes_unrecoverable(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/retry-once-contract/"
        repo.mark_queued(
            profile_url,
            source_jobs=["job_retry_once"],
            snapshot_dir="/tmp/snapshot_retry_once",
        )
        repo.mark_failed(
            profile_url,
            error="first provider timeout",
            retryable=True,
            retry_delay_seconds=1,
        )
        first_failure = repo.get(profile_url)
        assert first_failure is not None
        self.assertEqual(first_failure["status"], "failed_retryable")
        self.assertEqual(first_failure["retry_count"], 1)
        self.assertEqual(first_failure["refill_queue_state"], "retry_wait")

        repo.mark_failed(
            profile_url,
            error="retry provider timeout",
            retryable=True,
            retry_delay_seconds=1,
        )
        terminal_failure = repo.get(profile_url)
        assert terminal_failure is not None
        self.assertEqual(terminal_failure["status"], "unrecoverable")
        self.assertEqual(terminal_failure["retry_count"], 2)
        self.assertEqual(terminal_failure["refill_queue_state"], "")
        self.assertEqual(
            repo.list_refill_queue_items(
                states=["retry_wait"],
                source_job="job_retry_once",
                snapshot_dir="/tmp/snapshot_retry_once",
                limit=10,
            ),
            [],
        )

    def test_registry_preserves_unrecoverable_status_until_fetched(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "linkedin.com/in/unrecoverable-user"
        repo.mark_failed(
            profile_url,
            error="member restricted",
            retryable=False,
        )
        unrecoverable = repo.get(profile_url)
        self.assertIsNotNone(unrecoverable)
        assert unrecoverable is not None
        self.assertEqual(unrecoverable["status"], "unrecoverable")

        repo.mark_queued(profile_url, source_shards=["seed:retry"])
        still_unrecoverable = repo.get(profile_url)
        self.assertIsNotNone(still_unrecoverable)
        assert still_unrecoverable is not None
        self.assertEqual(still_unrecoverable["status"], "unrecoverable")

        repo.mark_fetched(
            profile_url,
            raw_path="/tmp/fetched_after_override.json",
        )
        fetched = repo.get(profile_url)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["status"], "fetched")

    def test_registry_records_deferred_coalescing_without_retry_increment(self) -> None:
        profile_url = "https://www.linkedin.com/in/deferred-coalescing-user/"
        self.store.repos.linkedin_profile_registry.mark_deferred_for_coalescing(
            profile_url,
            reason="final_tail_unproven",
            source_shards=["enrichment_background_prefetch"],
            source_jobs=["job_tail"],
            snapshot_dir="/tmp/snapshot_tail",
        )

        deferred = self.store.repos.linkedin_profile_registry.get(profile_url)
        self.assertIsNotNone(deferred)
        assert deferred is not None
        self.assertEqual(deferred["status"], "deferred_coalescing")
        self.assertEqual(deferred["retry_count"], 0)
        self.assertEqual(deferred["last_error"], "final_tail_unproven")
        self.assertEqual(deferred["last_snapshot_dir"], "/tmp/snapshot_tail")
        self.assertIn("enrichment_background_prefetch", deferred["source_shards"])
        self.assertIn("job_tail", deferred["source_jobs"])

    def test_deferred_coalescing_does_not_downgrade_fetched_registry_row(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/fetched-before-tail/"
        repo.mark_fetched(
            profile_url,
            raw_path="/tmp/fetched-before-tail.json",
            source_jobs=["job_cached"],
        )
        repo.mark_deferred_for_coalescing(
            profile_url,
            reason="final_tail_unproven",
            source_jobs=["job_tail"],
        )

        fetched = repo.get(profile_url)
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["status"], "fetched")
        self.assertEqual(fetched["last_raw_path"], "/tmp/fetched-before-tail.json")
        self.assertIn("job_cached", fetched["source_jobs"])
        self.assertIn("job_tail", fetched["source_jobs"])

    def test_registry_records_refill_plan_item_state_without_overwriting_lifecycle(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        active_url = "https://www.linkedin.com/in/refill-active/"
        deferred_url = "https://www.linkedin.com/in/refill-deferred/"
        fetched_url = "https://www.linkedin.com/in/refill-fetched/"
        repo.mark_fetched(
            fetched_url,
            raw_path="/tmp/refill-fetched.json",
            source_jobs=["job_cached"],
        )

        summary = repo.record_refill_plan_items(
            active_profile_urls=[active_url, fetched_url],
            deferred_profile_urls=[deferred_url],
            source_shards_by_url={
                active_url: ["openai::agent"],
                deferred_url: ["openai::infra"],
                fetched_url: ["openai::cached"],
            },
            source_jobs=["job_refill_plan"],
            snapshot_dir="/tmp/snapshot_refill",
            trigger_kind="profile_prefetch_refill",
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )

        self.assertEqual(summary["status"], "recorded")
        self.assertEqual(summary["active_item_count"], 1)
        self.assertEqual(summary["deferred_item_count"], 1)
        self.assertEqual(summary["requested_active_item_count"], 2)
        self.assertEqual(summary["terminal_skipped_item_count"], 1)
        active = repo.get(active_url)
        deferred = repo.get(deferred_url)
        fetched = repo.get(fetched_url)
        assert active is not None
        assert deferred is not None
        assert fetched is not None
        self.assertEqual(active["status"], "queued")
        self.assertEqual(active["refill_queue_state"], "planned_dispatch")
        self.assertEqual(active["last_refill_plan_reason"], "ready_to_dispatch")
        self.assertEqual(active["last_refill_attempt_count"], 1)
        self.assertIn("openai::agent", active["source_shards"])
        self.assertEqual(deferred["status"], "queued")
        self.assertEqual(deferred["refill_queue_state"], "deferred_budget")
        self.assertEqual(deferred["last_refill_deferred_reason"], "worker_budget_deferred")
        self.assertEqual(deferred["last_refill_attempt_count"], 0)
        self.assertEqual(fetched["status"], "fetched")
        self.assertEqual(fetched["last_raw_path"], "/tmp/refill-fetched.json")
        self.assertEqual(fetched["refill_queue_state"], "")
        self.assertEqual(fetched["last_refill_attempt_count"], 0)
        self.assertEqual(fetched["refill_terminal_status"], "completed")

    def test_registry_refill_plan_never_reopens_terminal_items(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        fetched_url = "https://www.linkedin.com/in/refill-terminal-fetched/"
        unrecoverable_url = "https://www.linkedin.com/in/refill-terminal-unrecoverable/"
        repo.mark_fetched(
            fetched_url,
            raw_path="/tmp/refill-terminal-fetched.json",
            source_jobs=["job_terminal_before"],
        )
        repo.mark_failed(
            unrecoverable_url,
            error="terminal failure",
            retryable=False,
            source_jobs=["job_terminal_before"],
        )

        summary = repo.record_refill_plan_items(
            active_profile_urls=[fetched_url],
            deferred_profile_urls=[unrecoverable_url],
            source_jobs=["job_terminal_refill"],
            snapshot_dir="/tmp/snapshot_terminal_refill",
            trigger_kind="profile_prefetch_refill",
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )

        self.assertEqual(summary["status"], "skipped")
        self.assertEqual(summary["reason"], "terminal_items_already_closed")
        self.assertEqual(summary["active_item_count"], 0)
        self.assertEqual(summary["deferred_item_count"], 0)
        self.assertEqual(summary["terminal_skipped_active_item_count"], 1)
        self.assertEqual(summary["terminal_skipped_deferred_item_count"], 1)
        fetched = repo.get(fetched_url)
        unrecoverable = repo.get(unrecoverable_url)
        assert fetched is not None
        assert unrecoverable is not None
        self.assertEqual(fetched["status"], "fetched")
        self.assertEqual(fetched["refill_queue_state"], "")
        self.assertEqual(fetched["refill_terminal_status"], "completed")
        self.assertIn("job_terminal_refill", fetched["source_jobs"])
        self.assertEqual(unrecoverable["status"], "unrecoverable")
        self.assertEqual(unrecoverable["refill_queue_state"], "")
        self.assertEqual(unrecoverable["refill_terminal_status"], "terminal_failed")
        self.assertIn("job_terminal_refill", unrecoverable["source_jobs"])
        self.assertEqual(
            repo.list_refill_queue_items(
                states=["planned_dispatch", "deferred_budget", "dispatch_claimed"],
                source_job="job_terminal_refill",
                snapshot_dir="/tmp/snapshot_terminal_refill",
                limit=10,
            ),
            [],
        )

    def test_dispatch_claim_not_before_does_not_delay_budget_deferred_items(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        active_url = "https://www.linkedin.com/in/refill-active-delay/"
        deferred_url = "https://www.linkedin.com/in/refill-deferred-no-delay/"
        repo.record_refill_plan_items(
            active_profile_urls=[active_url],
            deferred_profile_urls=[deferred_url],
            source_jobs=["job_refill_delay"],
            snapshot_dir="/tmp/snapshot_refill_delay",
            trigger_kind="profile_prefetch_refill",
            plan_reason="ready_to_dispatch",
            active_queue_state="dispatch_claimed",
            active_reason="provider_submit_claimed",
            active_refill_not_before_at="2999-01-01 00:00:00",
            deferred_reason="worker_budget_deferred",
        )

        self.assertEqual(
            repo.list_refill_queue_items(
                states=["dispatch_claimed"],
                source_job="job_refill_delay",
                snapshot_dir="/tmp/snapshot_refill_delay",
                limit=10,
            ),
            [],
        )
        ready_deferred = repo.list_refill_queue_items(
            states=["deferred_budget"],
            source_job="job_refill_delay",
            snapshot_dir="/tmp/snapshot_refill_delay",
            limit=10,
        )
        self.assertEqual([item["profile_url"] for item in ready_deferred], [deferred_url])

    def test_registry_records_dispatch_claim_as_recoverable_scheduler_ownership(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        claimed_url = "https://www.linkedin.com/in/refill-dispatch-claimed/"
        repo.record_refill_plan_items(
            active_profile_urls=[claimed_url],
            source_jobs=["job_dispatch_claim"],
            snapshot_dir="/tmp/snapshot_dispatch_claim",
            trigger_kind="profile_prefetch_refill",
            plan_reason="ready_to_dispatch",
            active_queue_state="dispatch_claimed",
            active_reason="provider_submit_claimed",
            active_refill_not_before_at="2000-01-01 00:00:00",
        )

        claimed = repo.get(claimed_url)
        assert claimed is not None
        self.assertEqual(claimed["refill_queue_state"], "dispatch_claimed")
        self.assertEqual(claimed["last_refill_deferred_reason"], "provider_submit_claimed")
        self.assertEqual(claimed["last_refill_attempt_count"], 0)

        ready = repo.list_refill_queue_items(
            states=["dispatch_claimed"],
            source_job="job_dispatch_claim",
            snapshot_dir="/tmp/snapshot_dispatch_claim",
            limit=10,
        )
        self.assertEqual([item["profile_url"] for item in ready], [claimed_url])

        repo.record_refill_plan_items(
            active_profile_urls=[claimed_url],
            source_jobs=["job_dispatch_claim"],
            snapshot_dir="/tmp/snapshot_dispatch_claim",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
        )
        submitted = repo.get(claimed_url)
        assert submitted is not None
        self.assertEqual(submitted["refill_queue_state"], "planned_dispatch")
        self.assertEqual(submitted["last_refill_attempt_count"], 1)

        repo.record_refill_plan_items(
            active_profile_urls=[claimed_url],
            source_jobs=["job_dispatch_claim"],
            snapshot_dir="/tmp/snapshot_dispatch_claim",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
        )
        resubmitted = repo.get(claimed_url)
        assert resubmitted is not None
        self.assertEqual(resubmitted["last_refill_attempt_count"], 1)

    def test_registry_records_remote_envelope_owner_and_terminal_completion(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/refill-remote-owner/"
        repo.record_refill_plan_items(
            active_profile_urls=[profile_url],
            source_jobs=["job_remote_owner"],
            snapshot_dir="/tmp/snapshot_remote_owner",
            trigger_kind="profile_prefetch_refill",
            plan_reason="ready_to_dispatch",
            active_queue_state="dispatch_claimed",
            active_reason="provider_submit_claimed",
            active_refill_not_before_at="2999-01-01 00:00:00",
        )

        claimed = repo.get(profile_url)
        assert claimed is not None
        self.assertEqual(claimed["refill_queue_state"], "dispatch_claimed")
        self.assertEqual(claimed["refill_owner_worker_id"], 0)
        self.assertEqual(claimed["refill_owner_run_id"], "")

        repo.record_refill_plan_items(
            active_profile_urls=[profile_url],
            source_jobs=["job_remote_owner"],
            snapshot_dir="/tmp/snapshot_remote_owner",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
            active_owner_worker_id=42,
            active_owner_run_id="run_remote_owner",
            active_owner_dataset_id="dataset_remote_owner",
            active_owner_payload_hash="payload123",
        )
        owned = repo.get(profile_url)
        assert owned is not None
        self.assertEqual(owned["refill_queue_state"], "planned_dispatch")
        self.assertEqual(owned["refill_owner_worker_id"], 42)
        self.assertEqual(owned["refill_owner_run_id"], "run_remote_owner")
        self.assertEqual(owned["refill_owner_dataset_id"], "dataset_remote_owner")
        self.assertEqual(owned["refill_owner_payload_hash"], "payload123")
        self.assertEqual(owned["last_refill_attempt_count"], 1)

        repo.record_refill_plan_items(
            active_profile_urls=[profile_url],
            source_jobs=["job_remote_owner"],
            snapshot_dir="/tmp/snapshot_remote_owner",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
        )
        duplicate = repo.get(profile_url)
        assert duplicate is not None
        self.assertEqual(duplicate["refill_owner_worker_id"], 42)
        self.assertEqual(duplicate["last_refill_attempt_count"], 1)

        repo.mark_fetched(
            profile_url,
            raw_path="/tmp/remote-owner.json",
            source_jobs=["job_remote_owner"],
            run_id="run_remote_owner",
            dataset_id="dataset_remote_owner",
            snapshot_dir="/tmp/snapshot_remote_owner",
        )
        fetched = repo.get(profile_url)
        assert fetched is not None
        self.assertEqual(fetched["status"], "fetched")
        self.assertEqual(fetched["refill_queue_state"], "")
        self.assertEqual(fetched["refill_owner_worker_id"], 42)
        self.assertEqual(fetched["refill_owner_run_id"], "run_remote_owner")
        self.assertEqual(fetched["refill_terminal_status"], "completed")
        self.assertTrue(fetched["refill_terminal_at"])

    def test_registry_lists_refill_queue_items_by_state_job_and_snapshot(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        target_url = "https://www.linkedin.com/in/refill-list-target/"
        other_job_url = "https://www.linkedin.com/in/refill-list-other-job/"
        active_url = "https://www.linkedin.com/in/refill-list-active/"
        repo.record_refill_plan_items(
            active_profile_urls=[active_url],
            deferred_profile_urls=[target_url, other_job_url],
            source_jobs=["job_refill_list"],
            snapshot_dir="/tmp/snapshot_refill_list",
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )
        repo.record_refill_plan_items(
            deferred_profile_urls=[other_job_url],
            source_jobs=["job_other"],
            snapshot_dir="/tmp/snapshot_other",
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )

        items = repo.list_refill_queue_items(
            states=["deferred_budget"],
            source_job="job_refill_list",
            snapshot_dir="/tmp/snapshot_refill_list",
            limit=10,
        )

        self.assertEqual([item["profile_url"] for item in items], [target_url])
        self.assertEqual(items[0]["refill_queue_state"], "deferred_budget")
        self.assertEqual(items[0]["last_refill_deferred_reason"], "worker_budget_deferred")

    def test_registry_refill_queue_respects_deferred_coalescing_not_before(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        future_url = "https://www.linkedin.com/in/refill-coalescing-future/"
        ready_url = "https://www.linkedin.com/in/refill-coalescing-ready/"
        repo.record_refill_plan_items(
            deferred_profile_urls=[future_url],
            source_jobs=["job_coalescing"],
            snapshot_dir="/tmp/snapshot_coalescing",
            trigger_kind="profile_tiny_tail_coalescing",
            plan_reason="tiny_tail_coalescing_wait",
            deferred_reason="final_tail_unproven",
            deferred_queue_state="deferred_coalescing",
            refill_not_before_at="2999-01-01 00:00:00",
        )
        repo.record_refill_plan_items(
            deferred_profile_urls=[ready_url],
            source_jobs=["job_coalescing"],
            snapshot_dir="/tmp/snapshot_coalescing",
            trigger_kind="profile_tiny_tail_coalescing",
            plan_reason="tiny_tail_coalescing_wait",
            deferred_reason="final_tail_unproven",
            deferred_queue_state="deferred_coalescing",
            refill_not_before_at="2000-01-01 00:00:00",
        )

        items = repo.list_refill_queue_items(
            states=["deferred_coalescing"],
            source_job="job_coalescing",
            snapshot_dir="/tmp/snapshot_coalescing",
            limit=10,
        )
        self.assertEqual([item["profile_url"] for item in items], [ready_url])
        self.assertEqual(items[0]["refill_queue_state"], "deferred_coalescing")
        self.assertEqual(items[0]["refill_not_before_at"], "2000-01-01 00:00:00")
        append_replan_items = repo.list_refill_queue_items(
            states=["deferred_coalescing"],
            source_job="job_coalescing",
            snapshot_dir="/tmp/snapshot_coalescing",
            limit=10,
            ready_only=False,
        )
        self.assertCountEqual(
            [item["profile_url"] for item in append_replan_items],
            [future_url, ready_url],
        )

        repo.record_refill_plan_items(
            deferred_profile_urls=[future_url],
            source_jobs=["job_coalescing"],
            snapshot_dir="/tmp/snapshot_coalescing",
            trigger_kind="profile_tiny_tail_coalescing",
            plan_reason="tiny_tail_coalescing_wait",
            deferred_reason="final_tail_unproven",
            deferred_queue_state="deferred_coalescing",
            refill_not_before_at="2000-01-01 00:00:00",
        )
        ready_items = repo.list_refill_queue_items(
            states=["deferred_coalescing"],
            source_job="job_coalescing",
            snapshot_dir="/tmp/snapshot_coalescing",
            limit=10,
        )

        self.assertCountEqual(
            [item["profile_url"] for item in ready_items],
            [future_url, ready_url],
        )

    def test_registry_rejects_unknown_deferred_refill_queue_state(self) -> None:
        with self.assertRaises(ValueError):
            self.store.repos.linkedin_profile_registry.record_refill_plan_items(
                deferred_profile_urls=["https://www.linkedin.com/in/refill-invalid-state/"],
                deferred_queue_state="waiting_profile_coalescing",
            )
        with self.assertRaises(ValueError):
            self.store.repos.linkedin_profile_registry.record_refill_plan_items(
                active_profile_urls=["https://www.linkedin.com/in/refill-invalid-active-state/"],
                active_queue_state="provider_worker_pending",
            )

    def test_registry_bulk_lookup_returns_normalized_keys(self) -> None:
        profile_url = "https://www.linkedin.com/in/Bulk-User/"
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        self.store.repos.linkedin_profile_registry.mark_queued(profile_url, source_shards=["seed:bulk"])
        bulk = self.store.repos.linkedin_profile_registry.get_bulk([profile_url, "https://www.linkedin.com/in/missing"])
        self.assertIn(normalized_key, bulk)
        self.assertEqual(bulk[normalized_key]["profile_url"], profile_url)

    def test_registry_bulk_queued_marker_preserves_dispatch_contract(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        dispatch_url = "https://www.linkedin.com/in/bulk-dispatch-owned/"
        fetched_url = "https://www.linkedin.com/in/bulk-dispatch-fetched/"
        repo.record_refill_plan_items(
            active_profile_urls=[dispatch_url],
            source_jobs=["job_bulk_dispatch"],
            snapshot_dir="/tmp/bulk-dispatch",
            trigger_kind="profile_prefetch_provider_submit",
            plan_reason="remote_provider_submitted",
            active_queue_state="planned_dispatch",
            active_owner_worker_id=42,
            active_owner_run_id="run-before",
            active_owner_dataset_id="dataset-before",
            active_owner_payload_hash="payload-before",
        )
        repo.mark_fetched(
            fetched_url,
            raw_path="/tmp/fetched.json",
            source_jobs=["job_bulk_dispatch"],
            snapshot_dir="/tmp/bulk-dispatch",
        )

        summary = repo.mark_queued_many(
            [dispatch_url, fetched_url],
            source_shards=["seed:bulk-dispatch"],
            source_jobs=["job_bulk_dispatch"],
            run_id="run-after",
            dataset_id="dataset-after",
            snapshot_dir="/tmp/bulk-dispatch",
        )
        entries = repo.get_bulk([dispatch_url, fetched_url])
        dispatch_entry = entries[normalize_linkedin_profile_url_key(dispatch_url)]
        fetched_entry = entries[normalize_linkedin_profile_url_key(fetched_url)]

        self.assertEqual(summary["queued_count"], 2)
        self.assertEqual(dispatch_entry["status"], "queued")
        self.assertEqual(dispatch_entry["last_run_id"], "run-after")
        self.assertEqual(dispatch_entry["last_dataset_id"], "dataset-after")
        self.assertEqual(dispatch_entry["last_snapshot_dir"], "/tmp/bulk-dispatch")
        self.assertEqual(dispatch_entry["refill_queue_state"], "planned_dispatch")
        self.assertEqual(dispatch_entry["refill_owner_worker_id"], 42)
        self.assertIn("seed:bulk-dispatch", dispatch_entry["source_shards"])
        self.assertEqual(fetched_entry["status"], "fetched")
        self.assertEqual(fetched_entry["last_raw_path"], "/tmp/fetched.json")
        self.assertEqual(fetched_entry["refill_queue_state"], "")

    def test_registry_alias_lookup_resolves_to_canonical_entry(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        canonical_url = "https://www.linkedin.com/in/alice-example/"
        alias_raw_url = "linkedin.com/in/ALICE-EXAMPLE"
        alias_sanity_url = "https://www.linkedin.com/in/alice-example"
        repo.mark_fetched(
            canonical_url,
            raw_path="/tmp/alice-example.json",
            alias_urls=[alias_raw_url, alias_sanity_url],
            raw_linkedin_url=alias_raw_url,
            sanity_linkedin_url=alias_sanity_url,
        )

        by_alias = repo.get(alias_raw_url)
        self.assertIsNotNone(by_alias)
        assert by_alias is not None
        self.assertEqual(by_alias["profile_url_key"], normalize_linkedin_profile_url_key(canonical_url))
        alias_keys = {
            normalize_linkedin_profile_url_key(item)
            for item in list(by_alias.get("alias_urls", []))
            if str(item or "").strip()
        }
        self.assertIn(normalize_linkedin_profile_url_key(alias_sanity_url), alias_keys)

        bulk = repo.get_bulk([alias_raw_url, canonical_url])
        self.assertIn(normalize_linkedin_profile_url_key(alias_raw_url), bulk)
        self.assertIn(normalize_linkedin_profile_url_key(canonical_url), bulk)

    def test_profile_registry_scope_summary_requires_all_job_snapshot_rows_terminal(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        fetched_url = "https://www.linkedin.com/in/scope-fetched/"
        unrecoverable_url = "https://www.linkedin.com/in/scope-unrecoverable/"
        open_url = "https://www.linkedin.com/in/scope-open/"
        other_job_url = "https://www.linkedin.com/in/scope-other-job/"
        repo.mark_fetched(
            fetched_url,
            raw_path="/tmp/scope-fetched.json",
            source_jobs=["job_scope"],
            snapshot_dir="/tmp/snapshot_scope",
        )
        repo.mark_failed(
            unrecoverable_url,
            error="profile not found",
            retryable=False,
            source_jobs=["job_scope"],
            snapshot_dir="/tmp/snapshot_scope",
        )
        repo.mark_queued(
            open_url,
            source_jobs=["job_scope"],
            snapshot_dir="/tmp/snapshot_scope",
        )
        repo.mark_fetched(
            other_job_url,
            raw_path="/tmp/scope-other-job.json",
            source_jobs=["job_other"],
            snapshot_dir="/tmp/snapshot_scope",
        )

        open_summary = repo.summarize_scope(
            source_job="job_scope",
            snapshot_dir="/tmp/snapshot_scope",
        )

        self.assertEqual(open_summary["requested_url_count"], 3)
        self.assertEqual(open_summary["terminal_url_count"], 2)
        self.assertEqual(open_summary["open_url_count"], 1)
        self.assertFalse(open_summary["all_requested_terminal"])

        repo.mark_fetched(
            open_url,
            raw_path="/tmp/scope-open.json",
            source_jobs=["job_scope"],
            snapshot_dir="/tmp/snapshot_scope",
        )
        terminal_summary = repo.summarize_scope(
            source_job="job_scope",
            snapshot_dir="/tmp/snapshot_scope",
        )

        self.assertEqual(terminal_summary["requested_url_count"], 3)
        self.assertEqual(terminal_summary["terminal_url_count"], 3)
        self.assertEqual(terminal_summary["open_url_count"], 0)
        self.assertTrue(terminal_summary["all_requested_terminal"])

    def test_registry_url_level_lease_blocks_duplicate_claims(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/lease-test/"
        lease_a = repo.acquire_lease(profile_url, lease_owner="worker-a", lease_seconds=120)
        self.assertTrue(lease_a.get("acquired"))
        self.assertEqual(lease_a.get("lease_owner"), "worker-a")

        lease_b = repo.acquire_lease(profile_url, lease_owner="worker-b", lease_seconds=120)
        self.assertFalse(lease_b.get("acquired"))
        self.assertEqual(lease_b.get("lease_owner"), "worker-a")

        released = repo.release_lease(profile_url, lease_owner="worker-a")
        self.assertTrue(released)

        lease_c = repo.acquire_lease(profile_url, lease_owner="worker-b", lease_seconds=120)
        self.assertTrue(lease_c.get("acquired"))
        self.assertEqual(lease_c.get("lease_owner"), "worker-b")

    def test_runtime_provider_limiter_respects_budget_and_release(self) -> None:
        first = self.store.acquire_runtime_provider_limiter_slot(
            "harvest_profile_scraper_actor",
            lease_owner="worker-a",
            budget=1,
            lease_token="lease-a",
        )
        self.assertTrue(first.get("acquired"))
        self.assertEqual(first.get("active_count"), 1)

        second = self.store.acquire_runtime_provider_limiter_slot(
            "harvest_profile_scraper_actor",
            lease_owner="worker-b",
            budget=1,
            lease_token="lease-b",
        )
        self.assertFalse(second.get("acquired"))
        self.assertEqual(second.get("active_count"), 1)

        self.assertTrue(
            self.store.release_runtime_provider_limiter_slot(
                "lease-a",
                limiter_key="harvest_profile_scraper_actor",
            )
        )
        third = self.store.acquire_runtime_provider_limiter_slot(
            "harvest_profile_scraper_actor",
            lease_owner="worker-b",
            budget=1,
            lease_token="lease-b",
        )
        self.assertTrue(third.get("acquired"))
        self.assertEqual(third.get("active_count"), 1)

    def test_runtime_provider_limiter_empty_token_is_new_attempt_not_refresh(self) -> None:
        with unittest.mock.patch("sourcing_agent.storage._utc_now_timestamp", return_value="2026-05-23 00:00:00"):
            first = self.store.acquire_runtime_provider_limiter_slot(
                "completed_workflow_reconcile:test-job:harvest_prefetch",
                lease_owner="completed_workflow_reconcile:test-job:harvest_prefetch",
                budget=1,
            )
            second = self.store.acquire_runtime_provider_limiter_slot(
                "completed_workflow_reconcile:test-job:harvest_prefetch",
                lease_owner="completed_workflow_reconcile:test-job:harvest_prefetch",
                budget=1,
            )

        self.assertTrue(first.get("acquired"))
        self.assertFalse(second.get("acquired"))
        self.assertEqual(second.get("active_count"), 1)
        self.assertNotEqual(first.get("lease_token"), second.get("lease_token"))

    def test_runtime_provider_limiter_status_reports_available_slots_without_acquire(self) -> None:
        empty = self.store.get_runtime_provider_limiter_status(
            "harvest_profile_scraper_actor",
            budget=2,
        )
        self.assertTrue(empty["available"])
        self.assertEqual(empty["active_count"], 0)
        self.assertEqual(empty["available_count"], 2)

        first = self.store.acquire_runtime_provider_limiter_slot(
            "harvest_profile_scraper_actor",
            lease_owner="worker-a",
            budget=2,
            lease_token="lease-status-a",
        )
        self.assertTrue(first.get("acquired"))
        partially_available = self.store.get_runtime_provider_limiter_status(
            "harvest_profile_scraper_actor",
            budget=2,
        )
        self.assertTrue(partially_available["available"])
        self.assertEqual(partially_available["active_count"], 1)
        self.assertEqual(partially_available["available_count"], 1)

        second = self.store.acquire_runtime_provider_limiter_slot(
            "harvest_profile_scraper_actor",
            lease_owner="worker-b",
            budget=2,
            lease_token="lease-status-b",
        )
        self.assertTrue(second.get("acquired"))
        full = self.store.get_runtime_provider_limiter_status(
            "harvest_profile_scraper_actor",
            budget=2,
        )
        self.assertFalse(full["available"])
        self.assertEqual(full["active_count"], 2)
        self.assertEqual(full["available_count"], 0)

    def test_profile_registry_metrics_summary(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/metrics-user/"
        repo.record_event(profile_url, event_type="lookup_attempt")
        repo.record_event(profile_url, event_type="cache_hit_registry")
        repo.record_event(profile_url, event_type="live_fetch_requested")
        repo.record_event(profile_url, event_type="duplicate_fetch_blocked")
        repo.record_event(
            profile_url,
            event_type="live_fetch_success",
            metadata={"retry_count_before": 1},
            duration_ms=1250,
        )
        repo.record_event(
            profile_url,
            event_type="live_fetch_failed",
            event_status="unrecoverable",
            metadata={"retry_count_before": 2},
        )

        metrics = repo.get_metrics(lookback_hours=0)
        self.assertGreaterEqual(metrics["cache_hit_rate"], 1.0)
        self.assertGreaterEqual(metrics["duplicate_request_rate"], 0.5)
        self.assertEqual(metrics["retry_success_count"], 1)
        self.assertEqual(metrics["retry_failure_count"], 1)
        self.assertGreater(metrics["queued_duration_ms_p50"], 0)
        self.assertGreater(metrics["unrecoverable_ratio"], 0.0)

    def test_backfill_run_checkpoint_persistence(self) -> None:
        run = self.store.repos.linkedin_profile_registry.upsert_backfill_run(
            "run::anthropic::snapshot",
            scope_company="anthropic",
            scope_snapshot_id="20260410T000000",
            checkpoint={"last_processed_path": "/tmp/a.json"},
            summary={"processed_total": 10},
            status="running",
        )
        self.assertIsNotNone(run)
        fetched = self.store.repos.linkedin_profile_registry.get_backfill_run("run::anthropic::snapshot")
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched["scope_company"], "anthropic")
        self.assertEqual(fetched["checkpoint"]["last_processed_path"], "/tmp/a.json")

    def test_registry_groups_refill_queue_items_by_source_job_and_snapshot(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        repo.record_refill_plan_items(
            deferred_profile_urls=[
                "https://www.linkedin.com/in/grouped-refill-a/",
                "https://www.linkedin.com/in/grouped-refill-b/",
            ],
            source_jobs=["job_group_a"],
            snapshot_dir="/tmp/snapshot-a",
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )
        repo.record_refill_plan_items(
            deferred_profile_urls=["https://www.linkedin.com/in/grouped-refill-c/"],
            source_jobs=["job_group_b"],
            snapshot_dir="/tmp/snapshot-b",
            plan_reason="ready_to_dispatch",
            deferred_reason="worker_budget_deferred",
        )

        groups = repo.list_refill_queue_groups(limit=10)
        group_keys = {
            (str(group["source_job"]), str(group["snapshot_dir"])): int(group["item_count"])
            for group in groups
        }

        self.assertEqual(group_keys[("job_group_a", "/tmp/snapshot-a")], 2)
        self.assertEqual(group_keys[("job_group_b", "/tmp/snapshot-b")], 1)
        job_a_groups = repo.list_refill_queue_groups(
            source_job="job_group_a",
            limit=10,
        )
        self.assertEqual(len(job_a_groups), 1)
        self.assertEqual(job_a_groups[0]["source_job"], "job_group_a")
        self.assertEqual(job_a_groups[0]["states"], {"deferred_budget": 2})


if __name__ == "__main__":
    unittest.main()
