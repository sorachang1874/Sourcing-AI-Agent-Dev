import unittest
from unittest import mock

from sourcing_agent.runtime_tuning import (
    apply_runtime_timing_overrides_to_mapping,
    build_materialization_streaming_budget_report,
    resolved_candidate_artifact_parallelism,
    resolved_harvest_company_roster_global_inflight,
    resolved_harvest_company_roster_parallel_shards,
    resolved_harvest_people_search_global_inflight,
    resolved_harvest_prefetch_submit_workers,
    resolved_harvest_profile_scrape_global_inflight,
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
            resolved_materialization_global_writer_budget({"materialization_global_writer_budget": 1}),
            1,
        )
        with mock.patch.dict("os.environ", {"SOURCING_HARVEST_GLOBAL_INFLIGHT_BUDGET": "2"}, clear=False):
            self.assertEqual(resolved_harvest_company_roster_global_inflight({}), 2)

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

    def test_runtime_inflight_slot_is_reentrant_for_nested_writer_calls(self) -> None:
        with runtime_inflight_slot("materialization_writer", budget=1, metadata={"phase": "outer"}) as outer:
            with runtime_inflight_slot("materialization_writer", budget=2, metadata={"phase": "inner"}) as inner:
                self.assertFalse(outer["reentrant"])
                self.assertTrue(inner["reentrant"])
                self.assertEqual(inner["wait_ms"], 0.0)


if __name__ == "__main__":
    unittest.main()
