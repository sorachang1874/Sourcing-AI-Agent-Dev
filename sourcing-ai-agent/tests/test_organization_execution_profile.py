import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_reuse_planning import (
    build_organization_asset_registry_candidate_inventory,
    build_acquisition_shard_registry_record,
    compile_asset_reuse_plan,
    evaluate_organization_asset_registry_promotion,
    normalize_organization_asset_lifecycle_status,
    select_organization_asset_registry_promotion_candidate,
)
from sourcing_agent.domain import (
    AcquisitionStrategyPlan,
    AcquisitionTask,
    JobRequest,
    PublicationCoveragePlan,
    RetrievalPlan,
    SearchStrategyPlan,
    SourcingPlan,
)
from sourcing_agent.organization_execution_profile import (
    _select_best_organization_asset_registry_row,
    ensure_organization_execution_profile,
)
from sourcing_agent.storage import SQLiteStore


class OrganizationExecutionProfileTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _scoped_search_plan(self, *, target_company: str, profile: dict[str, object]) -> SourcingPlan:
        filter_hints = {
            "current_companies": [target_company],
            "keywords": ["TBD"],
            "scope_keywords": ["TBD"],
        }
        shared_metadata = {
            "strategy_type": "scoped_search_roster",
            "search_seed_queries": ["TBD"],
            "filter_hints": filter_hints,
            "employment_statuses": ["current", "former"],
        }
        return SourcingPlan(
            target_company=target_company,
            target_scope="full_company_asset",
            intent_summary="test",
            criteria_summary="test",
            retrieval_plan=RetrievalPlan(strategy="structured", reason="test"),
            acquisition_strategy=AcquisitionStrategyPlan(
                strategy_type="scoped_search_roster",
                target_population=f"TBD within {target_company}",
                company_scope=[target_company, "TBD"],
                roster_sources=["harvest_profile_search"],
                search_channel_order=["harvest_profile_search", "profile_detail_api"],
                search_seed_queries=["TBD"],
                filter_hints=filter_hints,
                organization_execution_profile=dict(profile or {}),
            ),
            publication_coverage=PublicationCoveragePlan(coverage_goal="test"),
            search_strategy=SearchStrategyPlan(planner_mode="deterministic", objective="test"),
            acquisition_tasks=[
                AcquisitionTask(
                    task_id="acquire-full-roster",
                    task_type="acquire_full_roster",
                    title="Acquire scoped current roster",
                    description="test",
                    metadata=dict(shared_metadata),
                ),
                AcquisitionTask(
                    task_id="acquire-former-search-seed",
                    task_type="acquire_former_search_seed",
                    title="Acquire scoped former roster",
                    description="test",
                    metadata={
                        **shared_metadata,
                        "strategy_type": "former_employee_search",
                    },
                ),
            ],
            organization_execution_profile=dict(profile or {}),
        )

    def test_promotion_allows_materially_higher_lane_coverage_with_stable_quality(self) -> None:
        decision = evaluate_organization_asset_registry_promotion(
            existing_authoritative={
                "snapshot_id": "20260422T151623",
                "candidate_count": 332,
                "evidence_count": 0,
                "profile_detail_count": 332,
                "missing_linkedin_count": 0,
                "profile_completion_backlog_count": 0,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 252,
                "former_lane_effective_candidate_count": 80,
                "source_snapshot_count": 9,
            },
            candidate_record={
                "snapshot_id": "20260422T171923",
                "candidate_count": 404,
                "evidence_count": 0,
                "profile_detail_count": 404,
                "missing_linkedin_count": 0,
                "profile_completion_backlog_count": 0,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "source_snapshot_count": 11,
            },
        )

        self.assertTrue(decision["subsumption_higher"])
        self.assertTrue(decision["coverage_materially_higher"])
        self.assertTrue(decision["effective_lane_total_materially_higher"])
        self.assertTrue(decision["promote"])
        self.assertEqual(decision["reason"], "materially_higher_coverage_with_stable_quality")

    def test_partial_lifecycle_asset_cannot_promote_to_authoritative_baseline(self) -> None:
        decision = evaluate_organization_asset_registry_promotion(
            existing_authoritative=None,
            candidate_record={
                "snapshot_id": "20260424T120000",
                "status": "partial",
                "candidate_count": 500,
                "profile_detail_count": 500,
                "completeness_score": 95.0,
                "current_lane_effective_candidate_count": 500,
            },
        )

        self.assertFalse(decision["promote"])
        self.assertEqual(decision["reason"], "lifecycle_state_not_promotable")
        self.assertEqual(normalize_organization_asset_lifecycle_status("completed"), "ready")

    def test_large_org_supplemental_baseline_does_not_unlock_delta_reuse(self) -> None:
        snapshot_id = "20260423T062947"
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "Meta",
                "company_key": "meta",
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 2,
                "evidence_count": 4,
                "profile_detail_count": 2,
                "explicit_profile_capture_count": 0,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 81.5,
                "completeness_band": "high",
                "current_lane_coverage": {
                    "effective_candidate_count": 2,
                    "effective_ready": True,
                    "inferred_source_kind": "membership_summary",
                    "source_lane_keys": ["baseline_membership_summary:current"],
                },
                "former_lane_coverage": {"effective_candidate_count": 0, "effective_ready": False},
                "current_lane_effective_candidate_count": 2,
                "former_lane_effective_candidate_count": 0,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": False,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {
                    "mode": "all_history_snapshots",
                    "selected_snapshot_ids": [snapshot_id],
                },
                "summary": {
                    "candidate_count": 2,
                    "profile_detail_count": 2,
                    "membership_summary": {
                        "member_count": 2,
                        "lane_counts": {"baseline": 2},
                    },
                },
            },
            authoritative=True,
        )

        profile = ensure_organization_execution_profile(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Meta",
            asset_view="canonical_merged",
        )

        self.assertEqual(profile["org_scale_band"], "large")
        self.assertEqual(profile["default_acquisition_mode"], "scoped_search_roster")
        self.assertFalse(profile["prefer_delta_from_baseline"])
        self.assertFalse(profile["summary"]["coverage_baseline_reuse_ready"])
        self.assertEqual(profile["current_lane_default"], "keyword_search")
        self.assertIn("large_org_supplemental_baseline_not_coverage_ready", profile["reason_codes"])

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Meta的TBD组的人",
                "target_company": "Meta",
                "keywords": ["TBD"],
                "organization_keywords": ["TBD"],
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
            }
        )
        asset_reuse_plan = compile_asset_reuse_plan(
            runtime_dir=self.runtime_dir,
            store=self.store,
            request=request,
            plan=self._scoped_search_plan(target_company="Meta", profile=profile),
        )

        self.assertFalse(asset_reuse_plan["baseline_reuse_available"])
        self.assertEqual(asset_reuse_plan["reason"], "baseline_lacks_large_org_coverage_contract")

    def test_large_org_mode_only_multi_snapshot_selection_is_not_coverage_proof(self) -> None:
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": "openai-aggregate",
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 404,
                "profile_detail_count": 404,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": ["openai-coding", "openai-reasoning"],
                "source_snapshot_selection": {
                    "mode": "preferred_snapshot_subset",
                    "selected_snapshot_ids": ["openai-coding", "openai-reasoning"],
                },
            },
            authoritative=True,
        )

        profile = ensure_organization_execution_profile(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            asset_view="canonical_merged",
        )

        self.assertFalse(profile["summary"]["coverage_baseline_reuse_ready"])
        self.assertFalse(profile["prefer_delta_from_baseline"])
        self.assertIn("large_org_supplemental_baseline_not_coverage_ready", profile["reason_codes"])

    def test_large_org_promoted_aggregate_contract_unlocks_coverage_baseline(self) -> None:
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": "openai-aggregate",
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 404,
                "profile_detail_count": 404,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": ["openai-coding", "openai-reasoning"],
                "source_snapshot_selection": {
                    "mode": "preferred_snapshot_subset",
                    "selected_snapshot_ids": ["openai-coding", "openai-reasoning"],
                    "coverage_proof": {
                        "contract": "promoted_aggregate_coverage",
                        "status": "promoted",
                        "proof_kind": "promoted_authoritative_aggregate",
                        "covered_snapshot_ids": ["openai-coding", "openai-reasoning"],
                    },
                },
            },
            authoritative=True,
        )

        profile = ensure_organization_execution_profile(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            asset_view="canonical_merged",
        )

        self.assertTrue(profile["summary"]["coverage_baseline_reuse_ready"])
        self.assertTrue(profile["prefer_delta_from_baseline"])
        self.assertEqual(profile["current_lane_default"], "reuse_baseline")
        self.assertEqual(profile["former_lane_default"], "reuse_baseline")

    def test_ensure_execution_profile_promotes_better_registry_row_and_aligns_source_snapshot(self) -> None:
        old_snapshot_id = "20260422T151623"
        newer_snapshot_id = "20260422T171923"
        company_key = "openai"
        old_row = self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": old_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 332,
                "evidence_count": 0,
                "profile_detail_count": 332,
                "explicit_profile_capture_count": 332,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 9,
                "completeness_score": 88.0,
                "completeness_band": "high",
                "current_lane_coverage": {"effective_candidate_count": 252, "effective_ready": True},
                "former_lane_coverage": {"effective_candidate_count": 80, "effective_ready": True},
                "current_lane_effective_candidate_count": 252,
                "former_lane_effective_candidate_count": 80,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [old_snapshot_id],
                "source_snapshot_selection": {
                    "mode": "all_history_snapshots",
                    "selected_snapshot_ids": [old_snapshot_id],
                },
                "materialization_generation_key": "old-generation",
                "materialization_generation_sequence": 1,
                "materialization_watermark": "1:old-generation",
                "summary": {
                    "candidate_count": 332,
                },
            },
            authoritative=True,
        )
        newer_row = self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": newer_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": False,
                "candidate_count": 404,
                "evidence_count": 0,
                "profile_detail_count": 404,
                "explicit_profile_capture_count": 404,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 11,
                "completeness_score": 88.0,
                "completeness_band": "high",
                "current_lane_coverage": {"effective_candidate_count": 293, "effective_ready": True},
                "former_lane_coverage": {"effective_candidate_count": 111, "effective_ready": True},
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
                "source_snapshot_selection": {
                    "mode": "all_history_snapshots",
                    "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
                },
                "materialization_generation_key": "newer-generation",
                "materialization_generation_sequence": 5,
                "materialization_watermark": "5:newer-generation",
                "summary": {
                    "candidate_count": 404,
                },
            },
            authoritative=False,
        )
        for employment_scope, strategy_type, result_count in (
            ("current", "scoped_search_roster", 47),
            ("former", "former_employee_search", 26),
        ):
            self.store.upsert_acquisition_shard_registry(
                build_acquisition_shard_registry_record(
                    target_company="OpenAI",
                    company_key=company_key,
                    snapshot_id=newer_snapshot_id,
                    lane="profile_search",
                    employment_scope=employment_scope,
                    strategy_type=strategy_type,
                    shard_id="Coding",
                    shard_title="Coding",
                    search_query="Coding",
                    company_filters={
                        "companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["24", "8"],
                        "search_query": "Coding",
                    },
                    result_count=result_count,
                    status="completed",
                )
            )

        profile = ensure_organization_execution_profile(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="OpenAI",
            asset_view="canonical_merged",
        )

        self.assertEqual(profile["source_registry_id"], int(newer_row["registry_id"]))
        self.assertEqual(profile["source_snapshot_id"], newer_snapshot_id)
        self.assertEqual(profile["baseline_candidate_count"], 404)
        self.assertEqual(profile["current_profile_search_shard_count"], 1)
        self.assertEqual(profile["former_profile_search_shard_count"], 1)

        authoritative = self.store.get_authoritative_organization_asset_registry(
            target_company="OpenAI",
            asset_view="canonical_merged",
        )
        self.assertEqual(authoritative["registry_id"], newer_row["registry_id"])
        self.assertEqual(authoritative["snapshot_id"], newer_snapshot_id)
        rows = self.store.list_organization_asset_registry(
            target_company="OpenAI",
            asset_view="canonical_merged",
            limit=10,
        )
        old_row_refreshed = next(row for row in rows if int(row["registry_id"]) == int(old_row["registry_id"]))
        self.assertFalse(old_row_refreshed["authoritative"])

    def test_select_best_registry_row_does_not_regress_after_promoting_better_candidate(self) -> None:
        company_key = "openai"
        old_snapshot_id = "20260422T151623"
        newer_snapshot_id = "20260422T171923"
        aggregate_snapshot_id = "20260416T095325"
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": old_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 332,
                "profile_detail_count": 331,
                "evidence_count": 746,
                "completeness_score": 87.97,
                "current_lane_effective_candidate_count": 252,
                "former_lane_effective_candidate_count": 80,
                "selected_snapshot_ids": [old_snapshot_id],
            },
            authoritative=True,
        )
        newer_row = self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": newer_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": False,
                "candidate_count": 404,
                "profile_detail_count": 404,
                "evidence_count": 858,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
            },
            authoritative=False,
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": aggregate_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": False,
                "candidate_count": 175,
                "profile_detail_count": 175,
                "evidence_count": 397,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 130,
                "former_lane_effective_candidate_count": 45,
                "selected_snapshot_ids": [aggregate_snapshot_id, old_snapshot_id, newer_snapshot_id],
            },
            authoritative=False,
        )

        selected = _select_best_organization_asset_registry_row(
            store=self.store,
            target_company="OpenAI",
            asset_view="canonical_merged",
        )

        self.assertEqual(int(selected["registry_id"]), int(newer_row["registry_id"]))
        self.assertEqual(selected["snapshot_id"], newer_snapshot_id)

    def test_promotion_candidate_selection_does_not_chain_regress_on_explicit_baseline_inclusion(self) -> None:
        old_snapshot_id = "20260422T151623"
        newer_snapshot_id = "20260422T171923"
        aggregate_snapshot_id = "20260416T095325"

        old_row = {
            "registry_id": 35230,
            "snapshot_id": old_snapshot_id,
            "authoritative": True,
            "candidate_count": 332,
            "profile_detail_count": 331,
            "evidence_count": 746,
            "profile_completion_backlog_count": 1,
            "completeness_score": 87.97,
            "current_lane_effective_candidate_count": 252,
            "former_lane_effective_candidate_count": 80,
            "selected_snapshot_ids": [old_snapshot_id],
        }
        newer_row = {
            "registry_id": 35232,
            "snapshot_id": newer_snapshot_id,
            "authoritative": False,
            "candidate_count": 404,
            "profile_detail_count": 404,
            "evidence_count": 858,
            "profile_completion_backlog_count": 0,
            "completeness_score": 88.0,
            "current_lane_effective_candidate_count": 293,
            "former_lane_effective_candidate_count": 111,
            "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
        }
        aggregate_row = {
            "registry_id": 35154,
            "snapshot_id": aggregate_snapshot_id,
            "authoritative": False,
            "candidate_count": 175,
            "profile_detail_count": 175,
            "evidence_count": 397,
            "profile_completion_backlog_count": 0,
            "completeness_score": 88.0,
            "current_lane_effective_candidate_count": 130,
            "former_lane_effective_candidate_count": 45,
            "selected_snapshot_ids": [aggregate_snapshot_id, old_snapshot_id, newer_snapshot_id],
        }

        selected, decision = select_organization_asset_registry_promotion_candidate(
            candidate_records=[old_row, newer_row, aggregate_row],
            existing_authoritative=old_row,
        )

        self.assertEqual(selected["snapshot_id"], newer_snapshot_id)
        self.assertTrue(decision["promote"])
        self.assertEqual(decision["reason"], "explicit_baseline_inclusion")

    def test_explicit_baseline_inclusion_does_not_promote_thinner_aggregate_over_richer_authoritative(self) -> None:
        richer_authoritative = {
            "snapshot_id": "20260422T171923",
            "candidate_count": 404,
            "evidence_count": 858,
            "profile_detail_count": 404,
            "missing_linkedin_count": 0,
            "profile_completion_backlog_count": 0,
            "completeness_score": 88.0,
            "current_lane_effective_candidate_count": 293,
            "former_lane_effective_candidate_count": 111,
            "source_snapshot_count": 11,
        }
        thinner_aggregate = {
            "snapshot_id": "20260416T095325",
            "candidate_count": 175,
            "evidence_count": 397,
            "profile_detail_count": 175,
            "missing_linkedin_count": 0,
            "profile_completion_backlog_count": 0,
            "completeness_score": 88.0,
            "current_lane_effective_candidate_count": 130,
            "former_lane_effective_candidate_count": 45,
            "source_snapshot_count": 11,
            "selected_snapshot_ids": [
                "20260417T041804",
                "20260422T145316",
                "20260422T151623",
                "20260422T161258",
                "20260422T171923",
                "20260413T130221",
                "20260413T132015",
                "20260413T140350",
                "20260415T163054",
                "20260415T171051",
                "20260416T095325",
            ],
        }

        decision = evaluate_organization_asset_registry_promotion(
            existing_authoritative=richer_authoritative,
            candidate_record=thinner_aggregate,
        )

        self.assertTrue(decision["explicit_baseline_inclusion"])
        self.assertFalse(decision["explicit_baseline_inclusion_promotable"])
        self.assertFalse(decision["promote"])
        self.assertEqual(decision["reason"], "guard_rejected")

    def test_candidate_inventory_prioritizes_promoted_row_before_authoritative(self) -> None:
        company_key = "openai"
        old_snapshot_id = "20260422T151623"
        newer_snapshot_id = "20260422T171923"
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": old_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": 332,
                "profile_detail_count": 331,
                "evidence_count": 746,
                "completeness_score": 87.97,
                "current_lane_effective_candidate_count": 252,
                "former_lane_effective_candidate_count": 80,
                "selected_snapshot_ids": [old_snapshot_id],
            },
            authoritative=True,
        )
        newer_row = self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": newer_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": False,
                "candidate_count": 404,
                "profile_detail_count": 404,
                "evidence_count": 858,
                "completeness_score": 88.0,
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
            },
            authoritative=False,
        )

        inventory = build_organization_asset_registry_candidate_inventory(
            store=self.store,
            target_company="OpenAI",
            asset_view="canonical_merged",
            limit=20,
        )

        self.assertEqual(inventory["selected_row"]["snapshot_id"], newer_snapshot_id)
        self.assertEqual(inventory["ordered_candidate_rows"][0]["snapshot_id"], newer_snapshot_id)
        self.assertEqual(int(inventory["ordered_candidate_rows"][0]["registry_id"]), int(newer_row["registry_id"]))
