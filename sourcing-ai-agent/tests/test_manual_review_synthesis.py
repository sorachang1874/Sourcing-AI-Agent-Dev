import unittest

from sourcing_agent.manual_review_synthesis import compile_manual_review_synthesis
from sourcing_agent.model_provider import DeterministicModelClient


class ManualReviewSynthesisTest(unittest.TestCase):
    def _review_item(self) -> dict[str, object]:
        return {
            "review_item_id": 7,
            "review_type": "manual_identity_resolution",
            "summary": "Needs manual review.",
            "candidate": {
                "candidate_id": "cand_1",
                "display_name": "Lia Example",
                "role": "Member of Technical Staff",
                "organization": "Thinking Machines Lab",
                "linkedin_url": "https://www.linkedin.com/in/lia-example/",
                "notes": "Profile contains suspicious mixed signals.",
                "metadata": {
                    "membership_review_decision": "uncertain",
                    "membership_review_rationale": "Headline and matched experience disagree on the organization signal.",
                    "membership_review_trigger_keywords": ["openai", "spam"],
                },
            },
            "evidence": [
                {
                    "source_type": "linkedin_profile_detail",
                    "title": "LinkedIn profile detail",
                    "summary": "LinkedIn profile detail captured for Lia Example; target-company membership requires manual review.",
                    "url": "https://www.linkedin.com/in/lia-example/",
                },
                {
                    "source_type": "public_web_search",
                    "title": "Official page mention",
                    "summary": "Official page suggests Thinking Machines Lab affiliation.",
                    "url": "https://example.com/lia",
                },
            ],
            "metadata": {
                "reasons": ["suspicious_membership", "medium_confidence_needs_review"],
                "confidence_label": "medium",
                "confidence_score": 0.61,
                "explanation": "Structured roster match exists, but the profile content is noisy.",
            },
        }

    def test_compile_manual_review_synthesis_deterministic(self) -> None:
        compiled = compile_manual_review_synthesis(self._review_item(), model_client=DeterministicModelClient())

        self.assertEqual(compiled["synthesis_compiler"]["source"], "deterministic")
        self.assertTrue(compiled["synthesis"]["summary"])
        self.assertGreaterEqual(len(compiled["synthesis"]["confidence_takeaways"]), 1)
        self.assertGreaterEqual(len(compiled["synthesis"]["conflict_points"]), 1)
        self.assertGreaterEqual(len(compiled["synthesis"]["recommended_checks"]), 1)

    def test_compile_manual_review_synthesis_uses_model_when_available(self) -> None:
        class _ModelClient(DeterministicModelClient):
            def provider_name(self) -> str:
                return "test-model"

            def synthesize_manual_review(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
                return {
                    "summary": "Evidence conflict centers on a plausible company match with noisy profile content.",
                    "confidence_takeaways": ["Public evidence supports the affiliation, but LinkedIn content still needs human inspection."],
                    "conflict_points": ["Headline and supporting evidence are not fully aligned."],
                    "recommended_checks": ["Verify the experience timeline directly on LinkedIn."],
                }

        compiled = compile_manual_review_synthesis(self._review_item(), model_client=_ModelClient())

        self.assertEqual(compiled["synthesis_compiler"]["source"], "model")
        self.assertEqual(compiled["synthesis_compiler"]["provider"], "test-model")
        self.assertIn("plausible company match", compiled["synthesis"]["summary"])
        self.assertEqual(
            compiled["synthesis"]["recommended_checks"],
            ["Verify the experience timeline directly on LinkedIn."],
        )


if __name__ == "__main__":
    unittest.main()
