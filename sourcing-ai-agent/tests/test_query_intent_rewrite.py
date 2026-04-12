from __future__ import annotations

import unittest

from sourcing_agent.domain import JobRequest
from sourcing_agent.query_intent_rewrite import apply_query_intent_rewrite, interpret_query_intent_rewrite


class QueryIntentRewriteTest(unittest.TestCase):
    def test_multimodal_project_rewrite_applies_without_greater_china_terms(self) -> None:
        rewrite = interpret_query_intent_rewrite("给我 Google 做多模态（在 Veo 和 Nano Banana 团队）的人")

        self.assertEqual(str(rewrite.get("rewrite_id") or ""), "multimodal_project_focus")
        self.assertIn("multimodal", list(rewrite.get("keywords") or []))
        self.assertIn("Veo", list(rewrite.get("keywords") or []))
        self.assertIn("Nano Banana", list(rewrite.get("keywords") or []))
        self.assertIn("multimodal", list(rewrite.get("must_have_facets") or []))
        self.assertEqual(
            rewrite.get("request_patch", {}).get("must_have_facets"),
            ["multimodal"],
        )

    def test_job_request_receives_multimodal_and_research_rewrite_without_greater_china_terms(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Google 负责多模态（参与 Veo 和 Nano Banana）的研究员",
                "target_company": "Google",
            }
        )

        self.assertIn("multimodal", request.keywords)
        self.assertIn("Veo", request.keywords)
        self.assertIn("Nano Banana", request.keywords)
        self.assertIn("multimodal", request.must_have_facets)
        self.assertIn("research", request.must_have_facets)
        self.assertIn("research", request.must_have_primary_role_buckets)

    def test_apply_query_intent_rewrite_preserves_greater_china_and_multimodal_together(self) -> None:
        payload = apply_query_intent_rewrite(
            {
                "raw_user_request": "帮我找 Google Veo 团队的华人研究员",
                "target_company": "Google",
            }
        )

        self.assertIn("Greater China experience", list(payload.get("keywords") or []))
        self.assertIn("Chinese bilingual outreach", list(payload.get("keywords") or []))
        self.assertIn("Veo", list(payload.get("keywords") or []))
        self.assertIn("research", list(payload.get("must_have_facets") or []))

    def test_interpret_query_intent_rewrite_exposes_structured_request_patch_and_policy_metadata(self) -> None:
        rewrite = interpret_query_intent_rewrite("帮我找 Google Veo 团队的华人研究员")

        self.assertEqual(rewrite.get("policy_layer"), "business_policy")
        self.assertIn("Greater China experience", list(rewrite.get("request_patch", {}).get("keywords") or []))
        self.assertTrue(any(item.get("policy_layer") == "business_policy" for item in list(rewrite.get("additional_rewrites") or [])))

    def test_multimodal_project_rewrite_can_be_triggered_by_known_product_signal_from_shared_knowledge(self) -> None:
        rewrite = interpret_query_intent_rewrite("给我 Google Veo 团队的人")

        self.assertEqual(str(rewrite.get("rewrite_id") or ""), "multimodal_project_focus")
        self.assertIn("Veo", list(rewrite.get("keywords") or []))
        self.assertIn("Veo", list(rewrite.get("matched_terms") or []))

    def test_multimodal_project_rewrite_normalizes_hyphenated_known_product_signal(self) -> None:
        rewrite = interpret_query_intent_rewrite("给我 Google Nano-Banana 团队的人")

        self.assertEqual(str(rewrite.get("rewrite_id") or ""), "multimodal_project_focus")
        self.assertIn("Nano Banana", list(rewrite.get("keywords") or []))
        self.assertIn("Nano Banana", list(rewrite.get("matched_terms") or []))

    def test_research_role_rewrite_uses_shared_role_knowledge_aliases(self) -> None:
        rewrite = interpret_query_intent_rewrite("给我 Applied Scientist")

        self.assertEqual(str(rewrite.get("rewrite_id") or ""), "researcher_role_focus")
        self.assertIn("research", list(rewrite.get("must_have_primary_role_buckets") or []))


if __name__ == "__main__":
    unittest.main()
