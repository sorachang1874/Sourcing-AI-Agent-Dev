import unittest

from sourcing_agent.request_matching import (
    build_request_matching_bundle,
    matching_request_family_signature,
    matching_request_signature,
    request_family_score,
    request_family_signature,
    request_signature,
)


class RequestMatchingTest(unittest.TestCase):
    def test_family_signature_ignores_runtime_limits(self) -> None:
        left = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
            "top_k": 5,
            "semantic_rerank_limit": 8,
        }
        right = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
            "top_k": 20,
            "semantic_rerank_limit": 20,
        }
        self.assertNotEqual(request_signature(left), request_signature(right))
        self.assertEqual(request_family_signature(left), request_family_signature(right))
        score = request_family_score(left, right)
        self.assertTrue(score["exact_family_match"])
        self.assertFalse(score["exact_request_match"])

    def test_family_score_penalizes_different_query_family(self) -> None:
        left = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["RL"],
        }
        right = {
            "target_company": "xAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["inference"],
        }
        score = request_family_score(left, right)
        self.assertFalse(score["exact_family_match"])
        self.assertLess(score["score"], 50.0)

    def test_family_signature_distinguishes_asset_view(self) -> None:
        left = {
            "target_company": "xAI",
            "asset_view": "canonical_merged",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
        }
        right = {
            "target_company": "xAI",
            "asset_view": "strict_roster_only",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["systems"],
        }
        self.assertNotEqual(request_signature(left), request_signature(right))
        self.assertNotEqual(request_family_signature(left), request_family_signature(right))
        score = request_family_score(left, right)
        self.assertFalse(score["exact_family_match"])
        self.assertLess(score["score"], 70.0)

    def test_family_signature_normalizes_must_have_facet_aliases(self) -> None:
        left = {
            "target_company": "xAI",
            "must_have_facet": "multimodality",
            "categories": ["employee"],
        }
        right = {
            "target_company": "xAI",
            "must_have_facets": ["multimodal"],
            "categories": ["employee"],
        }
        self.assertEqual(request_signature(left), request_signature(right))
        self.assertEqual(request_family_signature(left), request_family_signature(right))

    def test_family_signature_normalizes_primary_role_bucket_aliases(self) -> None:
        left = {
            "target_company": "xAI",
            "must_have_primary_role_bucket": "infra",
            "categories": ["employee"],
        }
        right = {
            "target_company": "xAI",
            "must_have_primary_role_buckets": ["infra_systems"],
            "categories": ["employee"],
        }
        self.assertEqual(request_signature(left), request_signature(right))
        self.assertEqual(request_family_signature(left), request_family_signature(right))

    def test_matching_signature_uses_effective_request_normalization(self) -> None:
        raw_query_payload = {
            "raw_user_request": "我想找 Google Gemini 的产品经理",
            "target_company": "Google",
        }
        structured_payload = {
            "target_company": "Google",
            "organization_keywords": ["Gemini", "Google DeepMind"],
            "keywords": ["Gemini"],
            "must_have_primary_role_buckets": ["product_management"],
        }

        self.assertNotEqual(request_signature(raw_query_payload), request_signature(structured_payload))
        self.assertEqual(
            matching_request_signature(raw_query_payload),
            matching_request_signature(structured_payload),
        )
        self.assertEqual(
            matching_request_family_signature(raw_query_payload),
            matching_request_family_signature(structured_payload),
        )

        bundle = build_request_matching_bundle(raw_query_payload)
        self.assertEqual(bundle["matching_family_request"]["target_company"], "google")
        self.assertIn("gemini", bundle["matching_family_request"]["organization_keywords"])
        self.assertIn("product_management", bundle["matching_family_request"]["must_have_primary_role_buckets"])
