import unittest

from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.publication_planning import compile_publication_coverage_plan
from sourcing_agent.search_planning import compile_search_strategy


class SearchPlanningTest(unittest.TestCase):
    def test_public_interview_bundle_is_compiled_for_interview_queries(self) -> None:
        request = JobRequest(
            raw_user_request="在 YouTube 和 Podcast 上检索所有 Gemini Team 的访谈内容，找到其中的 Gemini 成员",
            query="Gemini Team interviews podcast YouTube",
            target_company="Google",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)
        search_plan = compile_search_strategy(request, strategy, publication, DeterministicModelClient())

        bundle_ids = [item.bundle_id for item in search_plan.query_bundles]
        self.assertIn("public_interviews", bundle_ids)
        interview_bundle = next(item for item in search_plan.query_bundles if item.bundle_id == "public_interviews")
        self.assertEqual(interview_bundle.source_family, "public_interviews")
        self.assertIn("YouTube", " ".join(interview_bundle.queries))

    def test_scoped_search_plan_keeps_paid_people_search_as_fallback(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Gemini Team 的 Post-train 方向 Researcher",
            query="Gemini post-train researcher",
            target_company="Google",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)
        search_plan = compile_search_strategy(request, strategy, publication, DeterministicModelClient())

        fallback_bundle = next(item for item in search_plan.query_bundles if item.bundle_id == "targeted_people_search")
        self.assertEqual(fallback_bundle.execution_mode, "paid_fallback")
        self.assertTrue(
            any("paid" in rule.lower() or "LinkedIn URL" in rule for rule in search_plan.follow_up_rules),
            search_plan.follow_up_rules,
        )
