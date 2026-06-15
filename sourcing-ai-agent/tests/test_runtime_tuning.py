import threading
import unittest
from unittest import mock

from sourcing_agent.domain import JobRequest
from sourcing_agent.runtime_tuning import (
    acquire_runtime_provider_limiter_slot,
    apply_runtime_timing_overrides_to_mapping,
    build_materialization_streaming_budget_report,
    build_provider_backpressure_budget_report,
    resolved_candidate_artifact_parallelism,
    resolved_harvest_company_roster_global_inflight,
    resolved_harvest_company_roster_parallel_shards,
    resolved_harvest_people_search_global_inflight,
    resolved_harvest_prefetch_submit_workers,
    resolved_harvest_profile_actor_global_inflight,
    resolved_harvest_profile_batch_submit_global_inflight,
    resolved_harvest_profile_scrape_global_inflight,
    resolved_harvest_scripted_sleep_seconds_cap,
    resolved_lane_budget_caps,
    resolved_materialization_coalescing_window_ms,
    resolved_materialization_global_writer_budget,
    resolved_parallel_exploration_workers,
    resolved_parallel_search_workers,
    resolved_provider_people_search_parallel_queries,
    resolved_runtime_positive_int,
    runtime_inflight_slot,
)


class RuntimeTuningTest(unittest.TestCase):
    def test_fast_smoke_profile_exposes_throughput_overrides(self) -> None:
        payload = apply_runtime_timing_overrides_to_mapping(
            {},
            runtime_timing_overrides={"runtime_tuning_profile": "fast_smoke"},
        )

        self.assertEqual(payload["provider_people_search_parallel_queries"], 6)
        self.assertEqual(payload["harvest_company_roster_parallel_shards"], 8)
        self.assertEqual(payload["candidate_artifact_parallel_min_candidates"], 12)
        self.assertEqual(payload["candidate_artifact_max_workers"], 4)
        self.assertEqual(payload["parallel_search_workers"], 6)
        self.assertEqual(payload["parallel_exploration_workers"], 4)
        self.assertEqual(payload["harvest_prefetch_submit_workers"], 4)
        self.assertEqual(payload["harvest_profile_actor_global_inflight"], 2)
        self.assertEqual(payload["harvest_profile_batch_submit_global_inflight"], 2)
        self.assertEqual(payload["harvest_profile_scrape_global_inflight"], 4)
        self.assertEqual(payload["harvest_people_search_global_inflight"], 4)
        self.assertEqual(payload["harvest_company_roster_global_inflight"], 4)
        self.assertEqual(payload["materialization_global_writer_budget"], 2)
        self.assertEqual(payload["materialization_coalescing_window_ms"], 50)
        self.assertEqual(payload["search_worker_unit_budget"], 12)
        self.assertEqual(payload["public_media_worker_unit_budget"], 10)
        self.assertEqual(payload["exploration_worker_unit_budget"], 8)

    def test_resolved_runtime_positive_int_prefers_explicit_context(self) -> None:
        value = resolved_runtime_positive_int(
            {
                "runtime_tuning_profile": "fast_smoke",
                "provider_people_search_parallel_queries": 3,
            },
            key="provider_people_search_parallel_queries",
        )

        self.assertEqual(value, 3)

    def test_job_request_preserves_harvest_global_inflight_preferences(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "Find Mistral AI members",
                "target_company": "Mistral AI",
                "execution_preferences": {
                    "harvest_profile_batch_submit_global_inflight": 2,
                    "harvest_global_inflight_budget": 5,
                    "harvest_company_roster_global_inflight": 3,
                },
            }
        )

        self.assertEqual(request.execution_preferences["harvest_profile_batch_submit_global_inflight"], 2)
        self.assertEqual(request.execution_preferences["harvest_global_inflight_budget"], 5)
        self.assertEqual(request.execution_preferences["harvest_company_roster_global_inflight"], 3)

    def test_resolved_provider_people_search_parallel_queries_caps_by_query_count(self) -> None:
        resolved = resolved_provider_people_search_parallel_queries(
            {"runtime_tuning_profile": "fast_smoke"},
            cost_policy={"provider_people_search_parallel_queries": 4},
            query_count=5,
            default=4,
        )

        self.assertEqual(resolved, 5)

    def test_resolved_harvest_company_roster_parallel_shards_caps_by_shard_count(self) -> None:
        resolved = resolved_harvest_company_roster_parallel_shards(
            {"harvest_company_roster_parallel_shards": 12},
            shard_count=3,
            default=8,
        )

        self.assertEqual(resolved, 3)

    def test_resolved_candidate_artifact_parallelism_uses_runtime_profile_budget(self) -> None:
        tuning = resolved_candidate_artifact_parallelism(
            {"runtime_tuning_profile": "fast_smoke"},
            candidate_count=40,
            default_parallel_min_candidates=24,
            default_max_workers=10,
        )

        self.assertEqual(tuning["min_candidates"], 12)
        self.assertEqual(tuning["max_workers"], 4)
        self.assertEqual(tuning["parallel_workers"], 4)

    def test_resolved_scheduler_parallel_limits_use_runtime_profile_budget(self) -> None:
        self.assertEqual(
            resolved_parallel_search_workers(
                {"runtime_tuning_profile": "fast_smoke"},
                cost_policy={"parallel_search_workers": 3},
                default=3,
            ),
            6,
        )
        self.assertEqual(
            resolved_parallel_exploration_workers(
                {"runtime_tuning_profile": "fast_smoke"},
                cost_policy={"parallel_exploration_workers": 2},
                default=2,
            ),
            4,
        )

    def test_resolved_harvest_prefetch_submit_workers_caps_by_chunk_count(self) -> None:
        self.assertEqual(
            resolved_harvest_prefetch_submit_workers(
                {"runtime_tuning_profile": "fast_smoke"},
                chunk_count=3,
                default=1,
            ),
            3,
        )
        self.assertEqual(
            resolved_harvest_prefetch_submit_workers(
                {"harvest_prefetch_submit_workers": 2},
                chunk_count=5,
                default=1,
            ),
            2,
        )

    def test_resolved_lane_budget_caps_use_runtime_profile_budget(self) -> None:
        caps = resolved_lane_budget_caps(
            {"runtime_tuning_profile": "fast_smoke"},
            cost_policy={"search_worker_unit_budget": 8},
        )

        self.assertEqual(caps["search_planner"], 12)
        self.assertEqual(caps["public_media_specialist"], 10)
        self.assertEqual(caps["exploration_specialist"], 8)

    def test_resolved_global_inflight_budget_uses_specific_and_generic_overrides(self) -> None:
        self.assertEqual(
            resolved_harvest_profile_scrape_global_inflight(
                {"harvest_profile_scrape_global_inflight": 2, "harvest_global_inflight_budget": 5}
            ),
            2,
        )
        self.assertEqual(
            resolved_harvest_people_search_global_inflight({"harvest_global_inflight_budget": 3}),
            3,
        )
        self.assertEqual(
            resolved_harvest_company_roster_global_inflight({}),
            4,
        )
        self.assertEqual(
            resolved_harvest_profile_actor_global_inflight({}),
            4,
        )
        self.assertEqual(
            resolved_harvest_profile_batch_submit_global_inflight({}),
            1,
        )
        self.assertEqual(
            resolved_materialization_global_writer_budget({"materialization_global_writer_budget": 1}),
            1,
        )
        with mock.patch.dict("os.environ", {"SOURCING_HARVEST_GLOBAL_INFLIGHT_BUDGET": "2"}, clear=False):
            self.assertEqual(resolved_harvest_company_roster_global_inflight({}), 2)

    def test_scripted_harvest_sleep_cap_prefers_context_then_isolated_env(self) -> None:
        with mock.patch.dict("os.environ", {"SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP": "0.2"}, clear=False):
            self.assertEqual(resolved_harvest_scripted_sleep_seconds_cap({}), 0.2)
            self.assertEqual(
                resolved_harvest_scripted_sleep_seconds_cap({"harvest_scripted_sleep_seconds_cap": 0.05}),
                0.05,
            )

    def test_runtime_provider_limiter_falls_back_when_store_lacks_contract(self) -> None:
        payload = acquire_runtime_provider_limiter_slot(
            object(),
            limiter_key="harvest_profile_scraper_actor",
            budget=4,
            lease_owner="unit-test",
            wait_timeout_seconds=0,
        )

        self.assertTrue(payload["acquired"])
        self.assertFalse(payload["db_limiter_enabled"])
        self.assertEqual(payload["reason"], "store_limiter_unavailable")

    def test_provider_backpressure_budget_report_uses_shared_runtime_tuning(self) -> None:
        report = build_provider_backpressure_budget_report(
            {"harvest_global_inflight_budget": 3, "harvest_profile_actor_global_inflight": 2},
            observed_limiter_slots=[
                {
                    "limiter_key": "harvest_profile_scraper_actor",
                    "active_count": 2,
                    "budget": 2,
                    "wait_ms": 125.4,
                }
            ],
            active_provider_worker_count=1,
            queued_provider_worker_count=2,
            waiting_remote_harvest_count=1,
        )

        self.assertEqual(report["budgets"]["harvest_global_inflight_budget"], 3)
        self.assertEqual(report["budgets"]["harvest_profile_actor_global_inflight"], 2)
        self.assertTrue(report["limiter_exhausted"])
        self.assertTrue(report["backpressure_detected"])
        self.assertEqual(report["provider_worker_backlog_count"], 3)
        self.assertEqual(report["recommended_action"], "throttle_provider_submit")

    def test_materialization_coalescing_window_prefers_context_and_env(self) -> None:
        self.assertEqual(
            resolved_materialization_coalescing_window_ms({"materialization_coalescing_window_ms": 125}),
            125,
        )
        with mock.patch.dict("os.environ", {"SOURCING_MATERIALIZATION_COALESCING_WINDOW_MS": "250"}, clear=False):
            self.assertEqual(resolved_materialization_coalescing_window_ms({}), 250)

    def test_materialization_streaming_budget_report_coalesces_nearby_deltas(self) -> None:
        report = build_materialization_streaming_budget_report(
            {
                "materialization_global_writer_budget": 1,
                "materialization_coalescing_window_ms": 500,
            },
            provider_response_count=2,
            profile_url_count=80,
            pending_delta_count=3,
            active_writer_count=0,
            queued_writer_count=1,
            oldest_pending_delta_age_ms=120.4,
        )

        self.assertTrue(report["provider_response_level_streaming_ready"])
        self.assertTrue(report["delta_materialization_ready"])
        self.assertTrue(report["writer_slot_available"])
        self.assertTrue(report["should_hold_for_coalescing"])
        self.assertEqual(report["recommended_action"], "coalesce_window")

    def test_materialization_streaming_budget_report_waits_when_writer_budget_is_full(self) -> None:
        report = build_materialization_streaming_budget_report(
            {"materialization_global_writer_budget": 1},
            provider_response_count=1,
            profile_url_count=30,
            pending_delta_count=1,
            active_writer_count=1,
        )

        self.assertFalse(report["writer_slot_available"])
        self.assertEqual(report["recommended_action"], "wait_for_writer_slot")

    def test_runtime_inflight_slot_reports_budget_metadata(self) -> None:
        with runtime_inflight_slot("unit-test-lane", budget=1, metadata={"source": "test"}) as slot:
            self.assertEqual(slot["lane"], "unit-test-lane")
            self.assertEqual(slot["budget"], 1)
            self.assertEqual(slot["source"], "test")
            self.assertGreaterEqual(float(slot["wait_ms"]), 0.0)
            self.assertFalse(slot["reentrant"])
            self.assertTrue(slot["acquired"])

    def test_runtime_inflight_slot_is_reentrant_for_nested_writer_calls(self) -> None:
        with runtime_inflight_slot("materialization_writer", budget=1, metadata={"phase": "outer"}) as outer:
            with runtime_inflight_slot("materialization_writer", budget=2, metadata={"phase": "inner"}) as inner:
                self.assertFalse(outer["reentrant"])
                self.assertTrue(inner["reentrant"])
                self.assertTrue(inner["acquired"])
                self.assertEqual(inner["wait_ms"], 0.0)

    def test_runtime_inflight_slot_can_fail_fast_when_nonblocking(self) -> None:
        with runtime_inflight_slot("unit-test-nonblocking-lane", budget=1) as outer:
            self.assertTrue(outer["acquired"])
            observed: list[dict] = []

            def _try_nonblocking() -> None:
                with runtime_inflight_slot(
                    "unit-test-nonblocking-lane",
                    budget=1,
                    blocking=False,
                ) as inner:
                    observed.append(dict(inner))

            thread = threading.Thread(target=_try_nonblocking)
            thread.start()
            thread.join(timeout=2.0)
            self.assertFalse(thread.is_alive())
            self.assertEqual(len(observed), 1)
            self.assertFalse(observed[0]["acquired"])
            self.assertEqual(observed[0]["reason"], "runtime_inflight_slot_unavailable")


if __name__ == "__main__":
    unittest.main()
