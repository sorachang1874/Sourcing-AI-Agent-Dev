import unittest

from sourcing_agent.result_diff import build_result_diff


class ResultDiffTest(unittest.TestCase):
    def test_detects_added_removed_and_moved_candidates(self) -> None:
        baseline = [
            {
                "candidate_id": "a",
                "display_name": "Alice",
                "rank": 1,
                "score": 9.0,
                "confidence_label": "high",
                "matched_fields": [{"keyword": "systems", "matched_on": "systems", "field": "focus_areas"}],
            },
            {
                "candidate_id": "b",
                "display_name": "Bob",
                "rank": 2,
                "score": 7.0,
                "confidence_label": "medium",
                "matched_fields": [{"keyword": "research", "matched_on": "research", "field": "role"}],
            },
        ]
        rerun = [
            {
                "candidate_id": "b",
                "display_name": "Bob",
                "rank": 1,
                "score": 8.5,
                "confidence_label": "high",
                "matched_fields": [{"keyword": "research", "matched_on": "research", "field": "role"}],
            },
            {
                "candidate_id": "c",
                "display_name": "Carol",
                "rank": 2,
                "score": 7.2,
                "confidence_label": "medium",
                "matched_fields": [{"keyword": "RL", "matched_on": "reinforcement learning", "field": "focus_areas"}],
            },
        ]
        baseline_version = {
            "version_id": 10,
            "request": {"keywords": ["RL"], "retrieval_strategy": "hybrid"},
            "plan": {"retrieval_plan": {"strategy": "hybrid"}},
            "patterns": [],
        }
        rerun_version = {
            "version_id": 11,
            "request": {"keywords": ["RL"], "retrieval_strategy": "hybrid"},
            "plan": {"retrieval_plan": {"strategy": "hybrid"}},
            "patterns": [
                {
                    "pattern_type": "alias",
                    "subject": "RL",
                    "value": "reinforcement learning",
                    "status": "active",
                    "confidence": "high",
                }
            ],
        }
        diff = build_result_diff(
            baseline,
            rerun,
            baseline_job_id="job1",
            rerun_job_id="job2",
            baseline_version=baseline_version,
            rerun_version=rerun_version,
            trigger_feedback={"feedback_type": "accepted_alias", "subject": "RL", "value": "reinforcement learning"},
        )

        self.assertEqual(diff["summary"]["added_count"], 1)
        self.assertEqual(diff["summary"]["removed_count"], 1)
        self.assertEqual(diff["summary"]["moved_count"], 1)
        self.assertTrue(diff["summary"]["top_candidate_changed"])
        self.assertEqual(diff["summary"]["added_pattern_count"], 1)
        self.assertEqual(diff["summary"]["baseline_version_id"], 10)
        self.assertEqual(diff["summary"]["rerun_version_id"], 11)
        self.assertEqual(diff["added"][0]["candidate_id"], "c")
        self.assertEqual(diff["removed"][0]["candidate_id"], "a")
        self.assertEqual(diff["moved"][0]["candidate_id"], "b")
        self.assertEqual(diff["rule_changes"]["pattern_changes"]["added"][0]["subject"], "RL")
        self.assertGreaterEqual(len(diff["rule_changes"]["explanations"]), 2)
        self.assertGreaterEqual(len(diff["result_changes"]["explanations"]), 2)
        self.assertGreaterEqual(len(diff["impact_explanations"]), 1)
        self.assertEqual(diff["candidate_impacts"]["summary"]["candidate_impact_count"], 3)
        self.assertEqual(diff["candidate_impacts"]["summary"]["attributed_candidate_count"], 1)
        self.assertEqual(diff["candidate_impacts"]["items"][0]["candidate_id"], "c")
        self.assertEqual(diff["candidate_impacts"]["items"][0]["change_type"], "added")
        self.assertEqual(diff["candidate_impacts"]["items"][0]["rule_triggers"][0]["source"], "pattern_added")
        self.assertIn("Entered results because", diff["candidate_impacts"]["items"][0]["attribution_explanation"])
