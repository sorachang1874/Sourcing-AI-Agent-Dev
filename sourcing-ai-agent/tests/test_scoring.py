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

    def test_default_technical_population_categories_are_soft_retrieval_hints(self) -> None:
        candidate = Candidate(
            candidate_id="cand_head_infra",
            name_en="Head of Infra",
            display_name="Head of Infra",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Head of Infrastructure",
            focus_areas="infra systems",
        )
        investor = Candidate(
            candidate_id="cand_investor",
            name_en="Infra Investor",
            display_name="Infra Investor",
            category="investor",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Investor",
            focus_areas="ai infrastructure investing",
        )
        default_technical_request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的 Infra 方向成员",
                "target_company": "Reflection AI",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Infra"],
            }
        )
        explicit_researcher_request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的 Infra 方向 Researcher",
                "target_company": "Reflection AI",
                "categories": ["researcher"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Infra"],
            }
        )

        self.assertTrue(candidate_matches_structured_filters(candidate, default_technical_request))
        self.assertFalse(candidate_matches_structured_filters(investor, default_technical_request))
        self.assertFalse(candidate_matches_structured_filters(candidate, explicit_researcher_request))

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


class ScoringSourceMatchProvenanceTests(unittest.TestCase):
    """Pass-5 H5 contract: candidates whose raw text fields don't contain a queried keyword
    must still pass recall and scoring when their source-shard provenance proves the match.

    `source_matches` / `matched_keywords` are how scoped sharding records "this candidate came
    from a profile-search shard for keyword X" without forcing every shard's seed query into
    every text field. If recall ignores them, the frontend recall filter on a scoped query
    silently hides candidates the workflow already paid to fetch for that exact keyword.
    """

    def _make_candidate_without_raw_text_match(self) -> Candidate:
        return Candidate(
            candidate_id="cand_source_match_provenance",
            name_en="Pat Provenance",
            display_name="Pat Provenance",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Member of Technical Staff",
            team="",
            focus_areas="",
            work_history="OpenAI infrastructure",
            education="",
            notes="",
            metadata={
                "matched_keywords": ["Reasoning"],
                "source_matches": [
                    {
                        "field": "source_seed_query",
                        "matched_on": "Reasoning",
                        "source_type": "scoped_search_roster",
                    }
                ],
            },
        )

    def _request_for_keyword(self, keyword: str) -> JobRequest:
        # Use a non-magical raw_user_request so the intent normalizer does not infer
        # must_have_facets/role_buckets that trip earlier filter gates and mask the recall
        # invariant we care about.
        return JobRequest.from_payload(
            {
                "raw_user_request": f"OpenAI {keyword}",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": [keyword],
            }
        )

    def test_candidate_matches_structured_filters_honors_matched_keywords_metadata(self) -> None:
        """When the request asks for `must_have_keywords=["Reasoning"]` but the candidate's
        raw text fields don't mention `Reasoning`, the structured-filter gate must still
        accept the candidate if `metadata.matched_keywords` proves the match."""

        candidate = self._make_candidate_without_raw_text_match()
        request = JobRequest.from_payload(
            {
                "raw_user_request": "OpenAI Reasoning",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "must_have_keywords": ["Reasoning"],
            }
        )
        self.assertTrue(
            candidate_matches_structured_filters(candidate, request),
            "candidate matched only via source_matches must still pass must_have_keywords",
        )

    def test_score_candidates_returns_candidate_when_only_source_matches_carry_keyword(self) -> None:
        """Recall test: with no raw-text occurrence of the keyword, scoring must still return
        the candidate based on the `matched_keywords` provenance."""

        candidate = self._make_candidate_without_raw_text_match()
        request = self._request_for_keyword("Reasoning")
        scored = score_candidates([candidate], request)
        self.assertEqual(len(scored), 1, "candidate must be recalled via source_matches")
        scored_record = scored[0]
        self.assertEqual(scored_record.candidate.candidate_id, candidate.candidate_id)
        # Provenance trail should be reflected in matched_fields so the API surface can
        # explain WHY the candidate was returned.
        matched_keywords_in_fields = {
            str(item.get("matched_on") or item.get("keyword") or "").strip().lower()
            for item in (scored_record.matched_fields or [])
        }
        self.assertTrue(
            "reasoning" in matched_keywords_in_fields,
            f"expected `Reasoning` in matched_fields explanation, got {matched_keywords_in_fields}",
        )

    def test_score_candidates_excludes_candidate_when_neither_text_nor_source_matches_match(self) -> None:
        """Negative control: if neither raw text nor `matched_keywords` mentions the keyword,
        the candidate must NOT be returned. (Without this, the H5 fix would over-recall.)"""

        candidate = self._make_candidate_without_raw_text_match()
        request = self._request_for_keyword("MultimodalUnrelated")
        scored = score_candidates([candidate], request)
        self.assertEqual(scored, [])


if __name__ == "__main__":
    unittest.main()
