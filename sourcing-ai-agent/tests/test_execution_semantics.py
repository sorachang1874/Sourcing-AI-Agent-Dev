import unittest

from sourcing_agent.domain import JobRequest
from sourcing_agent.execution_semantics import (
    compile_asset_reuse_lane_semantics,
    compile_execution_semantics,
    dispatch_requires_delta_for_request,
)


class ExecutionSemanticsTest(unittest.TestCase):
    def test_compile_execution_semantics_prefers_full_local_asset_reuse_for_authoritative_snapshot(self) -> None:
        request = JobRequest(target_company="OpenAI", target_scope="full_company_asset")

        semantics = compile_execution_semantics(
            request=request,
            organization_execution_profile={
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
            },
            asset_reuse_plan={
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 404,
            },
            candidate_source={
                "source_kind": "company_snapshot",
                "snapshot_id": "20260422T171923",
                "asset_view": "canonical_merged",
            },
        )

        self.assertEqual(semantics["effective_acquisition_mode"], "full_local_asset_reuse")
        self.assertEqual(semantics["default_results_mode"], "asset_population")
        self.assertTrue(semantics["asset_population_supported"])
        self.assertFalse(semantics["current_lane_delta_required"])
        self.assertFalse(semantics["former_lane_delta_required"])
        self.assertEqual(semantics["execution_strategy_label"], "全量本地资产复用")

    def test_compile_execution_semantics_returns_shared_strategy_label_for_scoped_delta(self) -> None:
        request = JobRequest(target_company="Google", target_scope="full_company_asset")

        semantics = compile_execution_semantics(
            request=request,
            organization_execution_profile={
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "current_lane_default": "keyword_search",
                "former_lane_default": "keyword_search",
            },
            asset_reuse_plan={
                "baseline_reuse_available": True,
                "requires_delta_acquisition": True,
                "baseline_candidate_count": 1200,
                "missing_current_profile_search_query_count": 1,
                "missing_former_profile_search_query_count": 1,
            },
            candidate_source={},
        )

        self.assertEqual(semantics["effective_acquisition_mode"], "baseline_reuse_with_delta")
        self.assertEqual(semantics["execution_strategy_label"], "Scoped search + Baseline 复用增量")

    def test_compile_execution_semantics_labels_full_roster_as_live_acquisition(self) -> None:
        request = JobRequest(target_company="Physical Intelligence", target_scope="full_company_asset")

        semantics = compile_execution_semantics(
            request=request,
            organization_execution_profile={
                "org_scale_band": "small",
                "default_acquisition_mode": "full_company_roster",
                "current_lane_default": "live_acquisition",
                "former_lane_default": "live_acquisition",
            },
            asset_reuse_plan={
                "baseline_reuse_available": False,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 0,
            },
            candidate_source={},
        )

        self.assertEqual(semantics["effective_acquisition_mode"], "full_live_roster")
        self.assertEqual(semantics["execution_strategy_label"], "全量 live roster")

    def test_dispatch_requires_delta_only_for_requested_lanes(self) -> None:
        request = JobRequest(target_company="OpenAI", employment_statuses=["former"])

        self.assertFalse(
            dispatch_requires_delta_for_request(
                request=request,
                asset_reuse_plan={
                    "requires_delta_acquisition": True,
                    "missing_current_company_employee_shard_count": 1,
                    "missing_current_profile_search_query_count": 1,
                    "missing_former_profile_search_query_count": 0,
                },
            )
        )
        self.assertTrue(
            dispatch_requires_delta_for_request(
                request=request,
                asset_reuse_plan={
                    "requires_delta_acquisition": True,
                    "missing_current_company_employee_shard_count": 0,
                    "missing_current_profile_search_query_count": 0,
                    "missing_former_profile_search_query_count": 1,
                },
            )
        )

    def test_compile_asset_reuse_lane_semantics_returns_single_source_of_truth_for_lane_behaviors(self) -> None:
        request = JobRequest(target_company="OpenAI", employment_statuses=["former"])

        lane_semantics = compile_asset_reuse_lane_semantics(
            request=request,
            asset_reuse_plan={
                "baseline_reuse_available": True,
                "planner_mode": "delta_from_snapshot",
                "baseline_current_effective_ready": True,
                "baseline_current_effective_candidate_count": 1200,
                "baseline_former_effective_ready": True,
                "baseline_former_effective_candidate_count": 180,
                "missing_current_company_employee_shard_count": 1,
                "missing_current_profile_search_query_count": 1,
                "missing_former_profile_search_query_count": 0,
            },
            organization_execution_profile={
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
            },
        )

        self.assertEqual(lane_semantics["employment_statuses"], ["former"])
        self.assertFalse(lane_semantics["requires_delta_acquisition"])
        self.assertEqual(lane_semantics["current_lane"]["planned_behavior"], "not_requested")
        self.assertEqual(lane_semantics["former_lane"]["planned_behavior"], "reuse_baseline")
        self.assertFalse(lane_semantics["former_lane"]["delta_required"])
        self.assertTrue(lane_semantics["lane_requirements_ready"])


if __name__ == "__main__":
    unittest.main()
