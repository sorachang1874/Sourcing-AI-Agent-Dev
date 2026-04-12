from __future__ import annotations

import unittest

from sourcing_agent.query_intent_policy import (
    list_business_rewrite_policy_catalog,
    match_business_rewrite_policies,
)


class QueryIntentPolicyTest(unittest.TestCase):
    def test_business_policy_catalog_exposes_structured_trigger_sources_and_request_patch(self) -> None:
        catalog = list_business_rewrite_policy_catalog()

        greater_china = next(item for item in catalog if item["rewrite_id"] == "greater_china_outreach")
        self.assertEqual(greater_china["policy_layer"], "business_policy")
        self.assertIn("华人", greater_china["trigger_sources"]["terms"])
        self.assertEqual(
            greater_china["request_patch"]["keywords"],
            ["Greater China experience", "Chinese bilingual outreach"],
        )

        multimodal = next(item for item in catalog if item["rewrite_id"] == "multimodal_project_focus")
        self.assertEqual(multimodal["trigger_sources"]["scope_rewrite_tags"], ["multimodal_project_focus"])
        self.assertEqual(multimodal["request_patch"]["must_have_facets"], ["multimodal"])

    def test_business_policy_matches_greater_china_shorthand(self) -> None:
        rewrites = match_business_rewrite_policies("帮我找华人研究员")

        rewrite_ids = [str(item.get("rewrite_id") or "") for item in rewrites]
        self.assertIn("greater_china_outreach", rewrite_ids)
        self.assertIn("researcher_role_focus", rewrite_ids)

    def test_business_policy_uses_knowledge_backed_multimodal_scope_tags(self) -> None:
        rewrites = match_business_rewrite_policies("给我 Google Veo 团队的人")

        multimodal = next(item for item in rewrites if str(item.get("rewrite_id") or "") == "multimodal_project_focus")
        self.assertEqual(multimodal["policy_layer"], "business_policy")
        self.assertIn("Veo", list(multimodal.get("keywords") or []))
        self.assertIn("Veo", list(multimodal.get("matched_terms") or []))
        self.assertEqual(multimodal["request_patch"]["must_have_facets"], ["multimodal"])

    def test_business_policy_uses_shared_role_knowledge(self) -> None:
        rewrites = match_business_rewrite_policies("给我 Applied Scientist")

        researcher = next(item for item in rewrites if str(item.get("rewrite_id") or "") == "researcher_role_focus")
        self.assertEqual(researcher["policy_layer"], "business_policy")
        self.assertIn("research", list(researcher.get("must_have_primary_role_buckets") or []))
        self.assertIn("applied scientist", list(researcher.get("matched_terms") or []))


if __name__ == "__main__":
    unittest.main()
