import unittest

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.publication_planning import compile_publication_coverage_plan


class PlanningModulesTest(unittest.TestCase):
    def test_scoped_strategy_for_large_company_query(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Gemini Team 的 Pre-train 方向的 Researcher 和 Engineer",
            query="Gemini pre-train researcher engineer",
            target_company="Google",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertIn("Google DeepMind", strategy.company_scope)
        self.assertIn("Gemini", strategy.company_scope)
        self.assertIn("general_web_search_relation_check", strategy.search_channel_order)
        self.assertIn("Researcher", " ".join(strategy.filter_hints.get("job_titles", [])))
        self.assertEqual(strategy.cost_policy.get("provider_people_search_mode"), "fallback_only")
        self.assertFalse(strategy.cost_policy.get("collect_email"))

    def test_former_employee_strategy_prefers_past_company_recall(self) -> None:
        request = JobRequest(
            raw_user_request="找 xAI 已离职的 RL researcher",
            query="xAI former RL researcher",
            target_company="xAI",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["former_employee"], ["former"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "former_employee_search")
        self.assertEqual(strategy.filter_hints.get("past_companies"), ["xAI"])
        self.assertNotIn("exclude_current_companies", strategy.filter_hints)
        self.assertFalse(strategy.cost_policy.get("allow_company_employee_api"))
        self.assertEqual(strategy.cost_policy.get("provider_people_search_min_expected_results"), 50)

    def test_publication_coverage_includes_engineering_and_blog(self) -> None:
        request = JobRequest(
            raw_user_request="找 Anthropic 工程和研究方向的技术成员",
            query="Anthropic engineering research contributors",
            target_company="Anthropic",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)
        coverage = compile_publication_coverage_plan(request, strategy)

        families = [item.family for item in coverage.source_families]
        self.assertIn("official_research", families)
        self.assertIn("official_engineering", families)
        self.assertIn("official_blog_and_docs", families)
        self.assertIn("Use LLM extraction for acknowledgement, contributor, and weakly structured bylines.", coverage.extraction_strategy)

    def test_sourcing_plan_contains_search_strategy_and_filter_layers(self) -> None:
        request = JobRequest(
            raw_user_request="在 YouTube 和 Podcast 上检索所有 Gemini Team 的访谈内容，找到 Post-train 方向 researcher",
            query="Gemini post-train interview researcher",
            target_company="Google",
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        layer_ids = [item.get("layer_id") for item in plan.retrieval_plan.filter_layers]
        bundle_ids = [item.bundle_id for item in plan.search_strategy.query_bundles]
        self.assertIn("semantic_vector_rerank", layer_ids)
        self.assertIn("manual_review_queue", layer_ids)
        self.assertIn("public_interviews", bundle_ids)
