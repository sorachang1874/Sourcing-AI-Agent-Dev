import unittest

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.scoring import build_query_terms, candidate_matches_structured_filters, score_candidates


class ScoringOutreachTermTests(unittest.TestCase):
    def test_scoring_prefers_intent_view_over_conflicting_flat_fields(self) -> None:
        candidate = Candidate(
            candidate_id="cand_intent_view",
            name_en="Gemini PM",
            display_name="Gemini PM",
            category="employee",
            target_company="Google",
            organization="Google DeepMind",
            employment_status="current",
            role="Product Manager",
            focus_areas="Gemini product",
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找 Gemini 的产品经理",
                "query": "Gemini product manager",
                "target_company": "WrongCo",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        self.assertTrue(candidate_matches_structured_filters(candidate, request))
        scored = score_candidates([candidate], request)
        self.assertEqual(len(scored), 1)

    def test_build_query_terms_excludes_outreach_only_terms(self) -> None:
        request = JobRequest(
            target_company="Acme",
            keywords=["Greater China experience", "Chinese bilingual outreach", "multimodal"],
            must_have_keywords=["Greater China experience"],
            must_have_facets=["greater_china_region_experience", "multimodal"],
            must_have_primary_role_buckets=["research"],
        )

        terms = [str(item).strip().lower() for item in build_query_terms(request)]
        joined = " ".join(terms)
        self.assertIn("multimodal", joined)
        self.assertNotIn("greater china experience", joined)
        self.assertNotIn("chinese bilingual outreach", joined)
        self.assertNotIn("greater_china_region_experience", joined)

    def test_build_query_terms_excludes_organization_keywords_from_thematic_pool(self) -> None:
        request = JobRequest(
            target_company="Google",
            keywords=["multimodal", "Veo", "Google DeepMind"],
            organization_keywords=["Google DeepMind", "Nano Banana"],
        )

        terms = [str(item).strip().lower() for item in build_query_terms(request)]
        joined = " ".join(terms)
        self.assertIn("multimodal", joined)
        self.assertIn("veo", joined)
        self.assertNotIn("google deepmind", joined)
        self.assertNotIn("nano banana", joined)

    def test_structured_filter_ignores_outreach_only_must_have_keywords(self) -> None:
        candidate = Candidate(
            candidate_id="cand_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Scientist",
        )

        outreach_request = JobRequest(
            target_company="Acme",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_keywords=["Greater China experience"],
        )
        self.assertTrue(candidate_matches_structured_filters(candidate, outreach_request))

        topical_request = JobRequest(
            target_company="Acme",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_keywords=["multimodal"],
        )
        self.assertFalse(candidate_matches_structured_filters(candidate, topical_request))

    def test_structured_filter_ignores_unsupported_thematic_must_have_facets(self) -> None:
        candidate = Candidate(
            candidate_id="cand_pretrain",
            name_en="Pat Pretrain",
            display_name="Pat Pretrain",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            focus_areas="Large language model training systems",
        )
        request = JobRequest(
            target_company="Anthropic",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_facets=["pre_training"],
            keywords=["Pre-train"],
        )

        self.assertTrue(candidate_matches_structured_filters(candidate, request))

    def test_profile_metadata_summary_and_skills_are_searchable(self) -> None:
        candidate = Candidate(
            candidate_id="cand_2",
            name_en="Sam Systems",
            display_name="Sam Systems",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Software Engineer",
            metadata={
                "summary": "Builds infrastructure platforms for training clusters and internal runtime tooling.",
                "skills": ["Kubernetes", "Distributed Systems"],
            },
        )
        request = JobRequest(
            target_company="Acme",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["infra", "kubernetes"],
        )

        scored = score_candidates([candidate], request)
        self.assertEqual(len(scored), 1)
        self.assertEqual(scored[0].candidate.candidate_id, "cand_2")

    def test_scoring_uses_acquisition_provenance_and_scope_keywords(self) -> None:
        candidate = Candidate(
            candidate_id="cand_3",
            name_en="Veo Builder",
            display_name="Veo Builder",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Research Scientist, Veo",
            metadata={
                "seed_query": "Veo",
                "scope_keywords": ["Google DeepMind"],
                "summary": "Builds Veo video generation systems.",
            },
            notes="Discovered from low-cost search seed acquisition. Query: Veo. Source: harvest_profile_search.",
        )
        request = JobRequest(
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            organization_keywords=["Google DeepMind", "Veo"],
            keywords=["Veo"],
        )

        self.assertTrue(candidate_matches_structured_filters(candidate, request))
        scored = score_candidates([candidate], request)
        self.assertEqual(len(scored), 1)
        matched_fields = scored[0].matched_fields
        self.assertTrue(any(str(item.get("field") or "") == "acquisition_signals" for item in matched_fields))
        self.assertTrue(any(str(item.get("field") or "") == "organization_scope" for item in matched_fields))

    def test_scope_keyword_does_not_match_generic_multimodal_aliases(self) -> None:
        generic_candidate = Candidate(
            candidate_id="cand_4",
            name_en="Generic Multimodal",
            display_name="Generic Multimodal",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Research Scientist, Multimodal Systems",
            focus_areas="Multimodal foundation models",
        )
        veo_candidate = Candidate(
            candidate_id="cand_5",
            name_en="Veo Lead",
            display_name="Veo Lead",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Research Engineer, Veo",
            focus_areas="Veo video generation",
        )
        request = JobRequest(
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["multimodal", "Veo"],
        )

        scored = score_candidates([generic_candidate, veo_candidate], request)
        self.assertEqual([item.candidate.candidate_id for item in scored[:2]], ["cand_5", "cand_4"])
        generic_matched_keywords = {str(item.get("keyword") or "") for item in scored[1].matched_fields}
        self.assertNotIn("Veo", generic_matched_keywords)

    def test_scope_keyword_does_not_score_from_acquisition_only_signal(self) -> None:
        candidate = Candidate(
            candidate_id="cand_6",
            name_en="Weak Veo Seed",
            display_name="Weak Veo Seed",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Research Scientist",
            metadata={
                "seed_query": "Veo",
            },
            notes="Discovered from low-cost search seed acquisition. Query: Veo. Source: harvest_profile_search.",
        )
        request = JobRequest(
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            keywords=["Veo"],
        )

        scored = score_candidates([candidate], request)
        self.assertEqual(scored, [])


if __name__ == "__main__":
    unittest.main()
