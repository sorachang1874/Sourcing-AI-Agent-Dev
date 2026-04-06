import tempfile
import unittest

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.confidence_policy import build_confidence_policy
from sourcing_agent.scoring import score_candidates
from sourcing_agent.storage import SQLiteStore


class CriteriaEvolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(f"{self.tempdir.name}/test.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_feedback_persists_alias_pattern(self) -> None:
        result = self.store.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "accepted_alias",
                "subject": "RL",
                "value": "reinforcement learning",
                "reviewer": "human",
                "notes": "RL queries should expand to reinforcement learning.",
            }
        )
        self.assertGreaterEqual(result["feedback_id"], 1)
        patterns = self.store.list_criteria_patterns(target_company="xAI", pattern_type="alias")
        self.assertEqual(len(patterns), 1)
        self.assertEqual(patterns[0]["subject"], "RL")

    def test_must_have_feedback_persists_confidence_boost_patterns(self) -> None:
        result = self.store.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "must_have_signal",
                "subject": "research",
                "value": "research engineer",
            }
        )
        pattern_types = {item["pattern_type"] for item in result["patterns"]}
        self.assertEqual(pattern_types, {"must_signal", "confidence_boost"})

    def test_criteria_version_and_compiler_run_persist(self) -> None:
        version = self.store.create_criteria_version(
            target_company="xAI",
            request_payload={"target_company": "xAI", "keywords": ["RL"]},
            plan_payload={"retrieval_plan": {"strategy": "hybrid"}},
            patterns=[],
            source_kind="plan",
        )
        compiler_run = self.store.record_criteria_compiler_run(
            version_id=version["version_id"],
            job_id="job123",
            provider_name="deterministic",
            compiler_kind="planning",
            status="completed",
            input_payload={"query": "xAI RL researcher"},
            output_payload={"strategy": "hybrid"},
        )
        self.assertGreaterEqual(version["version_id"], 1)
        self.assertGreaterEqual(compiler_run["compiler_run_id"], 1)
        versions = self.store.list_criteria_versions(target_company="xAI")
        runs = self.store.list_criteria_compiler_runs(version_id=version["version_id"])
        self.assertEqual(len(versions), 1)
        self.assertEqual(len(runs), 1)

    def test_alias_pattern_affects_scoring(self) -> None:
        self.store.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "accepted_alias",
                "subject": "RL",
                "value": "reinforcement learning",
            }
        )
        candidate = Candidate(
            candidate_id="cand1",
            name_en="Jane Doe",
            display_name="Jane Doe",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Research Engineer",
            focus_areas="reinforcement learning systems",
        )
        request = JobRequest(target_company="xAI", keywords=["RL"])
        patterns = self.store.list_criteria_patterns(target_company="xAI")
        scored = score_candidates([candidate], request, criteria_patterns=patterns)
        self.assertEqual(len(scored), 1)
        self.assertGreater(scored[0].score, 0)

    def test_confidence_penalty_pattern_lowers_confidence(self) -> None:
        candidate = Candidate(
            candidate_id="cand2",
            name_en="John Systems",
            display_name="John Systems",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Research Engineer",
            focus_areas="distributed systems",
            linkedin_url="https://linkedin.com/in/john-systems",
            metadata={"public_identifier": "john-systems"},
        )
        request = JobRequest(target_company="xAI", keywords=["systems"])
        baseline = score_candidates([candidate], request, criteria_patterns=[])
        self.assertEqual(baseline[0].confidence_label, "high")

        self.store.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "false_positive_pattern",
                "subject": "systems",
                "value": "distributed systems",
            }
        )
        patterns = self.store.list_criteria_patterns(target_company="xAI")
        scored = score_candidates([candidate], request, criteria_patterns=patterns)
        self.assertEqual(len(scored), 1)
        self.assertEqual(scored[0].confidence_label, "medium")
        self.assertIn("confidence_penalty", scored[0].confidence_reason)

    def test_confidence_boost_pattern_raises_confidence(self) -> None:
        candidate = Candidate(
            candidate_id="cand3",
            name_en="Jane Research",
            display_name="Jane Research",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Research Engineer",
            focus_areas="systems platform",
            linkedin_url="https://linkedin.com/in/jane-research",
            metadata={"exploration_links": ["https://example.com/jane"]},
        )
        request = JobRequest(target_company="xAI", keywords=["research"])
        baseline = score_candidates([candidate], request, criteria_patterns=[])
        self.assertEqual(baseline[0].confidence_label, "medium")

        self.store.record_criteria_feedback(
            {
                "target_company": "xAI",
                "feedback_type": "must_have_signal",
                "subject": "research",
                "value": "research engineer",
            }
        )
        patterns = self.store.list_criteria_patterns(target_company="xAI")
        scored = score_candidates([candidate], request, criteria_patterns=patterns)
        self.assertEqual(len(scored), 1)
        self.assertEqual(scored[0].confidence_label, "high")
        self.assertIn("confidence_boost", scored[0].confidence_reason)

    def test_company_level_confidence_policy_lowers_high_threshold(self) -> None:
        candidate = Candidate(
            candidate_id="cand4",
            name_en="Policy Candidate",
            display_name="Policy Candidate",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Engineer",
            focus_areas="systems platform",
            linkedin_url="https://linkedin.com/in/policy-candidate",
            metadata={"publication_id": "paper-1"},
        )
        request = JobRequest(target_company="xAI", keywords=["systems"])
        baseline = score_candidates([candidate], request, criteria_patterns=[])
        self.assertEqual(baseline[0].confidence_score, 0.65)
        self.assertEqual(baseline[0].confidence_label, "medium")

        for payload in [
            {"target_company": "xAI", "feedback_type": "false_negative_pattern", "subject": "infra", "value": "systems"},
            {"target_company": "xAI", "feedback_type": "must_have_signal", "subject": "infra", "value": "platform"},
            {"target_company": "xAI", "feedback_type": "must_have_signal", "subject": "backend", "value": "systems engineer"},
            {"target_company": "xAI", "feedback_type": "false_negative_pattern", "subject": "platform", "value": "systems platform"},
            {"target_company": "xAI", "feedback_type": "confidence_boost_signal", "subject": "systems", "value": "platform"},
        ]:
            self.store.record_criteria_feedback(payload)
        feedback = self.store.list_criteria_feedback(target_company="xAI")
        policy = build_confidence_policy(target_company="xAI", feedback_items=feedback)
        scored = score_candidates([candidate], request, criteria_patterns=[], confidence_policy=policy)
        self.assertLess(policy["high_threshold"], 0.75)
        self.assertEqual(scored[0].confidence_label, "high")
        self.assertIn("band_thresholds", scored[0].confidence_reason)
