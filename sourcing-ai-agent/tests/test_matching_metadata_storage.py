import tempfile
import unittest
from pathlib import Path

from sourcing_agent.request_matching import (
    matching_request_family_signature,
    matching_request_signature,
    request_family_signature,
    request_signature,
)
from sourcing_agent.storage import SQLiteStore


RAW_GEMINI_PM_REQUEST = {
    "raw_user_request": "我想找 Google Gemini 的产品经理",
    "target_company": "Google",
}

STRUCTURED_GEMINI_PM_REQUEST = {
    "target_company": "Google",
    "organization_keywords": ["Gemini", "Google DeepMind"],
    "keywords": ["Gemini"],
    "must_have_primary_role_buckets": ["product_management"],
}


class MatchingMetadataStorageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(Path(self.tempdir.name) / "test.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_confidence_policy_control_matches_equivalent_request_family(self) -> None:
        control = self.store.create_confidence_policy_control(
            target_company="Google",
            request_payload=RAW_GEMINI_PM_REQUEST,
            scope_kind="request_family",
            control_mode="freeze",
            high_threshold=0.7,
            medium_threshold=0.4,
            reviewer="human",
        )
        matched = self.store.find_active_confidence_policy_control(
            target_company="Google",
            request_payload=STRUCTURED_GEMINI_PM_REQUEST,
        )
        self.assertIsNotNone(matched)
        assert matched is not None
        self.assertEqual(matched["control_id"], control["control_id"])
        self.assertEqual(matched["selection_reason"], "request_family_control")
        self.assertEqual(
            matched["matching_request_family_signature"],
            matching_request_family_signature(STRUCTURED_GEMINI_PM_REQUEST),
        )

    def test_pattern_suggestions_dedupe_by_matching_request_family(self) -> None:
        suggestion_key = {
            "target_company": "Google",
            "source_feedback_id": 0,
            "candidate_id": "cand-1",
            "pattern_type": "must_signal",
            "subject": "gemini",
            "value": "gemini reasoning",
            "status": "suggested",
            "confidence": "medium",
            "rationale": "Carry forward the stronger signal.",
            "evidence": {"field": "focus_areas"},
            "metadata": {"signal_field": "focus_areas"},
        }
        self.store.record_pattern_suggestions(
            [
                {
                    **suggestion_key,
                    "request_signature": request_signature(RAW_GEMINI_PM_REQUEST),
                    "request_family_signature": request_family_signature(RAW_GEMINI_PM_REQUEST),
                    "matching_request_signature": matching_request_signature(RAW_GEMINI_PM_REQUEST),
                    "matching_request_family_signature": matching_request_family_signature(RAW_GEMINI_PM_REQUEST),
                    "source_job_id": "job-raw",
                }
            ]
        )
        self.store.record_pattern_suggestions(
            [
                {
                    **suggestion_key,
                    "request_signature": request_signature(STRUCTURED_GEMINI_PM_REQUEST),
                    "request_family_signature": request_family_signature(STRUCTURED_GEMINI_PM_REQUEST),
                    "matching_request_signature": matching_request_signature(STRUCTURED_GEMINI_PM_REQUEST),
                    "matching_request_family_signature": matching_request_family_signature(STRUCTURED_GEMINI_PM_REQUEST),
                    "source_job_id": "job-structured",
                }
            ]
        )

        suggestions = self.store.list_pattern_suggestions(target_company="Google", limit=10)
        self.assertEqual(len(suggestions), 1)
        self.assertEqual(suggestions[0]["source_job_id"], "job-structured")
        self.assertEqual(
            suggestions[0]["matching_request_family_signature"],
            matching_request_family_signature(STRUCTURED_GEMINI_PM_REQUEST),
        )


if __name__ == "__main__":
    unittest.main()
