import unittest

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.scoring import build_query_terms, candidate_matches_structured_filters


class ScoringOutreachTermTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
