import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_reuse_audit import (
    _compile_warnings,
    _summarize_shard_registry,
    audit_authoritative_reuse_planning,
    audit_authoritative_reuse_planning_matrix,
    compare_authoritative_reuse_planning_matrix_reports,
    summarize_authoritative_reuse_audit,
)
from sourcing_agent.asset_reuse_planning import build_acquisition_shard_registry_record

from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AssetReuseAuditTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        super().tearDown()

    def _upsert_scoped_skild_asset(self) -> None:
        snapshot_id = "skild-agent-scoped"
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Skild AI",
                "company_key": "skildai",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 17,
                "profile_detail_count": 17,
                "completeness_score": 82.0,
                "current_lane_effective_candidate_count": 12,
                "former_lane_effective_candidate_count": 5,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {
                    "mode": "single_scoped_search_snapshot",
                    "selected_snapshot_ids": [snapshot_id],
                    "population_coverage": {
                        "coverage_kind": "scoped_search",
                        "coverage_status": "partial",
                        "coverage_scope": "Agent",
                    },
                },
                "summary": {
                    "candidate_count": 17,
                    "population_coverage": {
                        "coverage_kind": "scoped_search",
                        "coverage_status": "partial",
                        "coverage_scope": "Agent",
                    },
                },
            },
            authoritative=True,
        )
        for employment_scope, result_count in (("current", 12), ("former", 5)):
            self.store.upsert_acquisition_shard_registry(
                build_acquisition_shard_registry_record(
                    target_company="Skild AI",
                    company_key="skildai",
                    snapshot_id=snapshot_id,
                    lane="profile_search",
                    employment_scope=employment_scope,
                    strategy_type="scoped_search_roster",
                    shard_id="Agent",
                    shard_title="Agent",
                    search_query="Agent",
                    company_filters={
                        "companies": ["Skild AI"],
                        "search_query": "Agent",
                    },
                    result_count=result_count,
                    status="completed",
                )
            )

    def test_audit_warns_full_company_request_when_only_scoped_authoritative_asset_exists(self) -> None:
        self._upsert_scoped_skild_asset()

        audit = audit_authoritative_reuse_planning(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="Skild AI",
            query="帮我找Skild AI所有人",
        )

        self.assertEqual(
            audit["request"]["requested_population_boundary"]["boundary_type"],
            "full_company_roster",
        )
        self.assertFalse(audit["baseline_population_coverage_contract"]["full_company_coverage_proven"])
        self.assertIn(
            "full_company_request_not_satisfied_by_scoped_authoritative_asset",
            audit["warnings"],
        )
        self.assertTrue(audit["planner"]["asset_reuse_plan"]["requires_delta_acquisition"])
        self.assertFalse(audit["planner"]["effective_execution_semantics"]["full_local_asset_reuse"])

    def test_audit_keeps_ordinary_directional_query_scoped_before_profile_default(self) -> None:
        self._upsert_scoped_skild_asset()

        audit = audit_authoritative_reuse_planning(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="Skild AI",
            query="帮我找Skild AI做Agent方向的人",
        )

        strategy = audit["planner"]["acquisition_strategy"]
        explanation = audit["planner"]["strategy_decision_explanation"]
        reuse_plan = audit["planner"]["asset_reuse_plan"]
        self.assertEqual(
            audit["request"]["requested_population_boundary"]["boundary_type"],
            "scoped_directional",
        )
        self.assertEqual(strategy["strategy_type"], "scoped_search_roster")
        self.assertEqual(explanation["decision_source"], "request_population_boundary")
        self.assertIn("directional_boundary_stays_scoped", explanation["reason_codes"])
        self.assertFalse(reuse_plan["requires_delta_acquisition"])
        self.assertEqual(reuse_plan["planner_mode"], "reuse_snapshot_only")
        self.assertFalse(reuse_plan["baseline_full_company_coverage_proven"])

    def test_audit_reports_full_company_filter_from_baseline_when_coverage_is_proven(self) -> None:
        snapshot_id = "xai-full-company"
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "xAI",
                "company_key": "xai",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 3600,
                "profile_detail_count": 3500,
                "completeness_score": 92.0,
                "current_lane_effective_candidate_count": 3300,
                "former_lane_effective_candidate_count": 300,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {
                    "mode": "full_company_roster",
                    "selected_snapshot_ids": [snapshot_id],
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                    },
                },
            },
            authoritative=True,
        )

        audit = audit_authoritative_reuse_planning(
            runtime_dir=self.runtime_dir,
            store=self.store,
            company="xAI",
            query="我要 xAI 做 Coding 方向的全部成员",
        )

        reuse_plan = audit["planner"]["asset_reuse_plan"]
        self.assertEqual(
            audit["request"]["requested_population_boundary"]["boundary_type"],
            "scoped_directional",
        )
        self.assertTrue(audit["baseline_population_coverage_contract"]["full_company_coverage_proven"])
        self.assertTrue(reuse_plan["baseline_full_company_coverage_proven"])
        self.assertTrue(reuse_plan["full_company_filter_from_baseline"])
        self.assertFalse(reuse_plan["requires_delta_acquisition"])
        self.assertEqual(
            audit["planner"]["effective_execution_semantics"]["execution_strategy_label"],
            "全量本地资产复用",
        )

    def test_audit_matrix_runs_strict_case_expectations_without_full_payload(self) -> None:
        self._upsert_scoped_skild_asset()

        report = audit_authoritative_reuse_planning_matrix(
            runtime_dir=self.runtime_dir,
            store=self.store,
            matrix={
                "matrix_version": 1,
                "cases": [
                    {
                        "case_id": "skild_agent",
                        "company": "Skild AI",
                        "query": "帮我找Skild AI做Agent方向的人",
                        "expectations": {
                            "requested_population_boundary": "scoped_directional",
                            "strategy_type": "scoped_search_roster",
                            "planner_mode": "reuse_snapshot_only",
                            "requires_delta_acquisition": False,
                        },
                    }
                ],
            },
            include_full_audit=False,
        )

        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["failure_count"], 0)
        self.assertEqual(report["cases"][0]["summary"]["planner_mode"], "reuse_snapshot_only")
        self.assertNotIn("audit", report["cases"][0])

    def test_compare_authoritative_reuse_planning_matrix_reports_detects_planner_drift(self) -> None:
        left = {
            "cases": [
                {
                    "case_id": "openai_agent",
                    "summary": {
                        "requested_population_boundary": "scoped_directional",
                        "strategy_type": "scoped_search_roster",
                        "planner_mode": "reuse_snapshot_only",
                        "requires_delta_acquisition": False,
                    },
                }
            ]
        }
        right = {
            "cases": [
                {
                    "case_id": "openai_agent",
                    "summary": {
                        "requested_population_boundary": "scoped_directional",
                        "strategy_type": "scoped_search_roster",
                        "planner_mode": "delta_from_snapshot",
                        "requires_delta_acquisition": True,
                    },
                }
            ]
        }

        diff = compare_authoritative_reuse_planning_matrix_reports(left=left, right=right)

        self.assertEqual(diff["status"], "drift")
        self.assertEqual(diff["drift_count"], 1)
        self.assertEqual(
            {item["field"] for item in diff["cases"][0]["field_diffs"]},
            {"planner_mode", "requires_delta_acquisition"},
        )

    def test_summary_and_warnings_surface_same_snapshot_materialization_gap(self) -> None:
        gap = {
            "snapshot_id": "snap-current",
            "search_query": "Health",
            "gap_reason": "baseline_generation_missing_members",
        }
        asset_reuse_plan = {
            "baseline_snapshot_id": "snap-current",
            "planner_mode": "delta_from_snapshot",
            "requires_delta_acquisition": True,
            "missing_current_profile_search_query_count": 1,
            "missing_former_profile_search_query_count": 0,
            "current_profile_search_exact_overlap_gaps": [gap],
            "former_profile_search_exact_overlap_gaps": [],
        }

        warnings = _compile_warnings(
            request_boundary={"boundary_type": "scoped_directional"},
            authoritative_row={"snapshot_id": "snap-current"},
            selected_row={"snapshot_id": "snap-current"},
            selected_snapshot_ids=["snap-current"],
            population_contract={"full_company_coverage_proven": True},
            shard_summary={"missing_selected_snapshot_ids": [], "rows": [{"snapshot_id": "snap-current"}]},
            asset_reuse_plan=asset_reuse_plan,
            execution_semantics={},
            inventory={"ordered_candidate_rows": [{"snapshot_id": "snap-current"}]},
        )
        audit = {
            "target_company": "OpenAI",
            "asset_view": "canonical_merged",
            "query": "我想要OpenAI在health组的人",
            "request": {"requested_population_boundary": {"boundary_type": "scoped_directional"}},
            "scoped_shard_registry": {"missing_selected_snapshot_ids": []},
            "planner": {
                "acquisition_strategy": {"strategy_type": "scoped_search_roster"},
                "strategy_decision_explanation": {"decision_source": "organization_execution_profile"},
                "asset_reuse_plan": asset_reuse_plan,
                "effective_execution_semantics": {
                    "full_local_asset_reuse": False,
                    "execution_strategy_label": "Baseline 复用 + 缺口增量",
                },
            },
            "warnings": warnings,
        }

        summary = summarize_authoritative_reuse_audit(audit, case_id="openai_health")

        self.assertIn("baseline_generation_missing_current_profile_shard_members", warnings)
        self.assertIn("baseline_generation_lags_same_snapshot_shard_materialization", warnings)
        self.assertEqual(summary["missing_current_profile_search_query_count"], 1)
        self.assertEqual(summary["current_profile_exact_overlap_gap_count"], 1)
        self.assertTrue(summary["baseline_generation_lags_same_snapshot_shard_materialization"])

    def test_shard_registry_summary_does_not_treat_serving_snapshot_as_missing_source_shard(self) -> None:
        summary = _summarize_shard_registry(
            rows=[
                {
                    "snapshot_id": "health-source",
                    "lane": "profile_search",
                    "employment_scope": "current",
                    "search_query": "Health",
                    "status": "completed",
                }
            ],
            selected_snapshot_ids=["repair-serving", "health-source", "stale-source"],
            serving_snapshot_id="repair-serving",
        )

        self.assertEqual(summary["selected_snapshot_ids_without_shard_registry_rows"], ["repair-serving", "stale-source"])
        self.assertEqual(summary["serving_snapshot_ids_without_shard_registry_rows"], ["repair-serving"])
        self.assertEqual(summary["missing_selected_snapshot_ids"], ["stale-source"])


if __name__ == "__main__":
    unittest.main()
