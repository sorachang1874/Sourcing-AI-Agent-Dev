import unittest

from sourcing_agent.query_signal_knowledge import (
    default_large_org_priority_function_ids,
    lookup_scope_signal,
    match_scope_signals_by_rewrite_tag,
    related_company_scope_labels,
    related_company_scope_urls,
    role_bucket_function_ids,
    role_bucket_matched_terms,
    role_buckets_from_text,
    scope_review_hints,
)


class QuerySignalKnowledgeTest(unittest.TestCase):
    def test_lookup_scope_signal_canonicalizes_known_products_and_models(self) -> None:
        self.assertEqual(lookup_scope_signal("Gemini")["target_company"], "Google")
        self.assertEqual(lookup_scope_signal("ChatGPT")["target_company"], "OpenAI")
        self.assertEqual(lookup_scope_signal("Claude")["target_company"], "Anthropic")
        self.assertEqual(lookup_scope_signal("o1")["target_company"], "OpenAI")

    def test_google_related_scope_helpers_reuse_shared_scope_table(self) -> None:
        self.assertEqual(
            related_company_scope_labels("Google", ["Gemini", "Veo"]),
            ["Google DeepMind"],
        )
        self.assertEqual(
            related_company_scope_urls("Google", ["Gemini"]),
            ["https://www.linkedin.com/company/deepmind/"],
        )
        self.assertEqual(
            scope_review_hints("Google", ["Gemini"]),
            ["Gemini", "Google DeepMind"],
        )
        self.assertEqual(
            [item["canonical_label"] for item in match_scope_signals_by_rewrite_tag("Google Veo team", "multimodal_project_focus")],
            ["Veo"],
        )

    def test_role_bucket_knowledge_drives_function_ids_and_text_matching(self) -> None:
        self.assertEqual(role_buckets_from_text("我想找产品经理和PM"), ["product_management"])
        self.assertEqual(role_bucket_function_ids(["product_management"]), ["19"])
        self.assertEqual(role_bucket_matched_terms("Applied Scientist", "research"), ["applied scientist"])
        self.assertEqual(default_large_org_priority_function_ids(), ["8", "9", "19", "24"])


if __name__ == "__main__":
    unittest.main()
