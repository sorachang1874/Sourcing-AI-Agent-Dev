import unittest

from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.publication_planning import compile_publication_coverage_plan
from sourcing_agent.search_planning import compile_search_strategy


class SearchPlanningTest(unittest.TestCase):
    def test_publication_coverage_naturalizes_directional_topic_queries(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我OpenAI做RL、Post-train和Text方向的人",
                "query": "OpenAI RL Post-train Text people",
                "target_company": "OpenAI",
                "keywords": ["RL", "Post-train"],
                "must_have_facets": ["text"],
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current", "former"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)

        self.assertIn("OpenAI Reinforcement Learning research", publication.seed_queries)
        self.assertIn("OpenAI Post-train contributor", publication.seed_queries)
        self.assertIn("OpenAI Language Model contributor", publication.seed_queries)

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

    def test_targeted_people_search_bundle_uses_natural_keyword_queries(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要OpenAI做RL、Eval、Infra方向的人",
                "query": "OpenAI RL Eval Infra people",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["RL", "Eval", "Infra"],
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current", "former"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)
        search_plan = compile_search_strategy(request, strategy, publication, DeterministicModelClient())

        fallback_bundle = next(item for item in search_plan.query_bundles if item.bundle_id == "targeted_people_search")
        self.assertCountEqual(
            fallback_bundle.queries,
            ["Reinforcement Learning", "Evaluation", "Infrastructure"],
        )

    def test_relationship_bundle_dedupes_company_scope_terms(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning方向的人",
                "target_company": "OpenAI",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
                "must_have_facets": ["reasoning"],
                "must_have_primary_role_buckets": ["research"],
                "primary_role_bucket_mode": "soft",
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["researcher", "engineer"], ["current", "former"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)
        search_plan = compile_search_strategy(request, strategy, publication, DeterministicModelClient())

        relationship_bundle = next(item for item in search_plan.query_bundles if item.bundle_id == "relationship_web")
        self.assertIn("OpenAI team", relationship_bundle.queries)
        self.assertNotIn("OpenAI OpenAI team", relationship_bundle.queries)

    def test_model_merge_dedupes_query_variants_by_signature(self) -> None:
        class _FakeModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "openai"

            def plan_search_strategy(self, request, context):  # noqa: ANN001, ANN202
                return {
                    "planner_mode": "model_assisted",
                    "query_bundles": [
                        {
                            "bundle_id": "targeted_people_search",
                            "source_family": "linkedin_people_search",
                            "priority": "medium",
                            "objective": "paid fallback",
                            "execution_mode": "paid_fallback",
                            "queries": [
                                "Vision-language",
                                "Vision Language",
                                "vision_language",
                                "Veo",
                            ],
                            "filters": {},
                        }
                    ],
                    "follow_up_rules": [
                        "Keep paid fallback as last resort.",
                        "Keep paid fallback as last resort",
                    ],
                    "review_triggers": [],
                }

        request = JobRequest(
            raw_user_request="给我 Google Veo 相关成员",
            query="Google Veo members",
            target_company="Google",
            planning_mode="llm_brief",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)
        search_plan = compile_search_strategy(request, strategy, publication, _FakeModelClient())

        bundle = next(item for item in search_plan.query_bundles if item.bundle_id == "targeted_people_search")
        self.assertEqual(bundle.queries, ["Vision-language", "Veo"])
