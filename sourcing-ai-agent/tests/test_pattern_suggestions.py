import unittest

from sourcing_agent.domain import Candidate
from sourcing_agent.pattern_suggestions import derive_pattern_suggestions


class PatternSuggestionsTest(unittest.TestCase):
    def test_negative_feedback_suggests_related_exclude_patterns(self) -> None:
        candidate = Candidate(
            candidate_id="cand1",
            name_en="John Systems",
            display_name="John Systems",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Research Engineer",
            focus_areas="distributed systems platform",
            notes="Served as infra liaison for cluster runtime.",
        )
        job_result = {
            "candidate_id": "cand1",
            "matched_fields": [
                {
                    "keyword": "systems",
                    "matched_on": "systems",
                    "field": "focus_areas",
                    "value": "distributed systems platform",
                    "weight": 4,
                }
            ],
        }
        suggestions = derive_pattern_suggestions(
            feedback={
                "feedback_id": 1,
                "job_id": "job1",
                "candidate_id": "cand1",
                "target_company": "xAI",
                "feedback_type": "false_positive_pattern",
                "subject": "systems",
                "value": "distributed systems",
                "metadata": {
                    "request_payload": {
                        "target_company": "xAI",
                        "keywords": ["systems"],
                    }
                },
            },
            candidate=candidate,
            job_result=job_result,
            existing_patterns=[
                {"pattern_type": "exclude_signal", "subject": "systems", "value": "distributed systems"},
                {"pattern_type": "confidence_penalty", "subject": "systems", "value": "distributed systems"},
            ],
        )
        self.assertGreaterEqual(len(suggestions), 1)
        self.assertEqual(suggestions[0]["pattern_type"], "exclude_signal")
        self.assertEqual(suggestions[0]["value"], "distributed systems platform")

    def test_positive_feedback_suggests_must_and_alias_patterns(self) -> None:
        candidate = Candidate(
            candidate_id="cand2",
            name_en="Jane Infra",
            display_name="Jane Infra",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Engineer",
            team="Pretraining Infrastructure",
            focus_areas="distributed training systems",
        )
        suggestions = derive_pattern_suggestions(
            feedback={
                "feedback_id": 2,
                "job_id": "job2",
                "candidate_id": "cand2",
                "target_company": "xAI",
                "feedback_type": "false_negative_pattern",
                "subject": "infra",
                "value": "distributed training",
                "metadata": {
                    "request_payload": {
                        "target_company": "xAI",
                        "keywords": ["infra"],
                    }
                },
            },
            candidate=candidate,
            job_result=None,
            existing_patterns=[],
        )
        pattern_types = {item["pattern_type"] for item in suggestions}
        values = {item["value"] for item in suggestions}
        self.assertIn("must_signal", pattern_types)
        self.assertIn("alias", pattern_types)
        self.assertTrue(
            "distributed training systems" in values or "Pretraining Infrastructure" in values
        )
