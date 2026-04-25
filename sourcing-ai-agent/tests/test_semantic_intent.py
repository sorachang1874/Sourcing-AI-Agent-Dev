import unittest

from sourcing_agent.domain import JobRequest
from sourcing_agent.request_normalization import resolve_request_intent_view
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

    def test_request_intent_view_treats_infra_theme_as_weak_role_signal(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 OpenAI 做 Infra 和 Post-train 方向的人",
                "query": "OpenAI Infra 和 Post-train 方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Infra", "Post-train"],
            }
        )

        intent_view = resolve_request_intent_view(request)
        role_targeting = dict(dict(intent_view.get("semantic_brief") or {}).get("role_targeting") or {})

        self.assertIn(
            role_targeting.get("provenance"),
            {"structured", "default_technical", "default_technical_from_weak_structured_singleton"},
        )
        self.assertCountEqual(role_targeting.get("resolved_role_buckets") or [], ["research", "engineering"])
        self.assertCountEqual(role_targeting.get("function_ids") or [], ["24", "8"])


if __name__ == "__main__":
    unittest.main()
