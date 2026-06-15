import unittest

from sourcing_agent.domain import JobRequest
from sourcing_agent.request_normalization import resolve_request_intent_view, supplement_request_query_signals
from sourcing_agent.semantic_intent import compile_semantic_brief


class SemanticIntentTest(unittest.TestCase):
    def test_semantic_brief_defaults_technical_queries_to_research_and_engineering(self) -> None:
        brief = compile_semantic_brief(
            raw_text="我想要 OpenAI 做 Reasoning 方向的人",
            target_company="OpenAI",
            target_scope="full_company_asset",
            categories=["employee"],
            employment_statuses=["current", "former"],
            organization_keywords=[],
            keywords=["Reasoning"],
            must_have_keywords=[],
            must_have_facets=[],
            must_have_primary_role_buckets=[],
            execution_preferences={"keyword_priority_only": True},
        )

        role_targeting = dict(brief.get("role_targeting") or {})
        self.assertEqual(role_targeting.get("provenance"), "default_technical")
        self.assertCountEqual(role_targeting.get("resolved_role_buckets") or [], ["research", "engineering"])
        self.assertCountEqual(role_targeting.get("function_ids") or [], ["24", "8"])

    def test_semantic_brief_keeps_explicit_product_manager_role(self) -> None:
        brief = compile_semantic_brief(
            raw_text="帮我找 Google 的 product manager",
            target_company="Google",
            target_scope="full_company_asset",
            categories=["employee"],
            employment_statuses=["current"],
            organization_keywords=["Gemini"],
            keywords=[],
            must_have_keywords=[],
            must_have_facets=["product_management"],
            must_have_primary_role_buckets=["product_management"],
            execution_preferences={},
        )

        role_targeting = dict(brief.get("role_targeting") or {})
        self.assertEqual(role_targeting.get("provenance"), "text_explicit")
        self.assertEqual(role_targeting.get("resolved_role_buckets"), ["product_management"])
        self.assertEqual(role_targeting.get("function_ids"), ["19"])

    def test_semantic_brief_does_not_force_technical_split_for_plain_full_roster_request(self) -> None:
        brief = compile_semantic_brief(
            raw_text="给我 xAI 的所有成员",
            target_company="xAI",
            target_scope="full_company_asset",
            categories=["employee", "former_employee"],
            employment_statuses=["current", "former"],
            organization_keywords=[],
            keywords=[],
            must_have_keywords=[],
            must_have_facets=[],
            must_have_primary_role_buckets=[],
            execution_preferences={},
        )

        role_targeting = dict(brief.get("role_targeting") or {})
        self.assertEqual(role_targeting.get("resolved_role_buckets"), [])
        self.assertEqual(role_targeting.get("function_ids"), [])

    def test_request_intent_view_treats_infra_as_theme_not_role_bucket(self) -> None:
        payload = supplement_request_query_signals(
            {
                "raw_user_request": "给我 OpenAI 做 Infra 和 Post-train 方向的人",
                "query": "OpenAI Infra 和 Post-train 方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
            },
            raw_text="给我 OpenAI 做 Infra 和 Post-train 方向的人",
        )

        request = JobRequest.from_payload(payload)
        intent_view = resolve_request_intent_view(request)
        role_targeting = dict(dict(intent_view.get("semantic_brief") or {}).get("role_targeting") or {})

        self.assertIn("Infra", intent_view["keywords"])
        self.assertNotIn("infra_systems", intent_view["must_have_primary_role_buckets"])
        self.assertIn(
            role_targeting.get("provenance"),
            {"structured", "default_technical", "default_technical_from_weak_structured_singleton"},
        )
        self.assertCountEqual(role_targeting.get("resolved_role_buckets") or [], ["research", "engineering"])
        self.assertCountEqual(role_targeting.get("function_ids") or [], ["24", "8"])

    def test_intent_view_drops_model_emitted_infra_systems_for_infra_theme(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 PostHog 做 Infra 方向的人",
                "query": "PostHog Infra 方向的人",
                "target_company": "PostHog",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["Infra"],
                "must_have_primary_role_buckets": ["infra_systems"],
            }
        )

        intent_view = resolve_request_intent_view(request)

        self.assertEqual(intent_view["keywords"], ["Infra"])
        self.assertEqual(intent_view["must_have_primary_role_buckets"], [])

    def test_request_intent_view_keeps_explicit_infrastructure_engineer_role(self) -> None:
        payload = supplement_request_query_signals(
            {
                "raw_user_request": "帮我找 OpenAI 的 infrastructure engineer",
                "query": "OpenAI infrastructure engineer",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
            },
            raw_text="帮我找 OpenAI 的 infrastructure engineer",
        )

        request = JobRequest.from_payload(payload)
        intent_view = resolve_request_intent_view(request)

        self.assertIn("infra_systems", intent_view["must_have_primary_role_buckets"])

    def test_request_intent_view_treats_agent_as_theme_keyword(self) -> None:
        payload = supplement_request_query_signals(
            {
                "raw_user_request": "帮我找OpenAI做Agent方向的人",
                "query": "帮我找OpenAI做Agent方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
            },
            raw_text="帮我找OpenAI做Agent方向的人",
        )

        request = JobRequest.from_payload(payload)
        intent_view = resolve_request_intent_view(request)
        role_targeting = dict(dict(intent_view.get("semantic_brief") or {}).get("role_targeting") or {})

        self.assertEqual(intent_view["keywords"], ["Agent"])
        self.assertEqual(intent_view["must_have_primary_role_buckets"], [])
        self.assertCountEqual(role_targeting.get("resolved_role_buckets") or [], ["research", "engineering"])
        self.assertCountEqual(role_targeting.get("function_ids") or [], ["24", "8"])

    def test_requested_population_boundary_distinguishes_full_roster_from_directional_filter(self) -> None:
        full_roster = resolve_request_intent_view(
            JobRequest.from_payload(
                {
                    "raw_user_request": "给我 xAI 的所有成员",
                    "target_company": "xAI",
                    "employment_statuses": ["current", "former"],
                }
            )
        )
        directional_filter = resolve_request_intent_view(
            JobRequest.from_payload(
                {
                    "raw_user_request": "我要 xAI 做 Coding 方向的全部成员",
                    "query": "xAI coding all members",
                    "target_company": "xAI",
                    "keywords": ["Coding"],
                    "employment_statuses": ["current", "former"],
                }
            )
        )

        self.assertEqual(
            full_roster["requested_population_boundary"]["boundary_type"],
            "full_company_roster",
        )
        self.assertEqual(
            directional_filter["requested_population_boundary"]["boundary_type"],
            "scoped_directional",
        )
        self.assertTrue(
            directional_filter["requested_population_boundary"]["full_company_filter_allowed"],
        )
        self.assertTrue(
            directional_filter["requested_population_boundary"]["full_company_filter_requires_coverage_proof"],
        )

    def test_openai_health_group_is_scoped_directional_boundary(self) -> None:
        intent_view = resolve_request_intent_view(
            JobRequest.from_payload(
                {
                    "raw_user_request": "我想要OpenAI在health组的人",
                    "target_company": "OpenAI",
                    "employment_statuses": ["current", "former"],
                }
            )
        )

        self.assertEqual(
            intent_view["requested_population_boundary"]["boundary_type"],
            "scoped_directional",
        )
        self.assertIn("Health", intent_view["keywords"])

    def test_openai_whisper_group_preserves_explicit_ascii_scope_keyword(self) -> None:
        payload = supplement_request_query_signals(
            {
                "raw_user_request": "帮我找OpenAI在Whisper组的人",
                "target_company": "OpenAI",
                "employment_statuses": ["current", "former"],
            },
            raw_text="帮我找OpenAI在Whisper组的人",
        )
        intent_view = resolve_request_intent_view(JobRequest.from_payload(payload))

        self.assertIn("Whisper", intent_view["keywords"])
        self.assertEqual(
            intent_view["requested_population_boundary"]["boundary_type"],
            "scoped_directional",
        )


if __name__ == "__main__":
    unittest.main()
