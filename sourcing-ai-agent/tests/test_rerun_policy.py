import unittest

from sourcing_agent.rerun_policy import decide_rerun_policy


class RerunPolicyTest(unittest.TestCase):
    def test_auto_alias_feedback_approves_cheap_rerun(self) -> None:
        decision = decide_rerun_policy(
            {"rerun_retrieval": True},
            feedback={"feedback_type": "accepted_alias"},
            recompile={"status": "recompiled"},
            baseline_job={
                "job_id": "job1",
                "request": {"top_k": 5, "semantic_rerank_limit": 12},
                "plan": {"retrieval_plan": {"strategy": "hybrid"}},
                "summary": {"total_matches": 0},
            },
        )
        self.assertEqual(decision["status"], "approved")
        self.assertEqual(decision["mode"], "cheap")
        self.assertEqual(decision["runtime_policy"]["summary_mode"], "deterministic")
        self.assertEqual(decision["effective_request_overrides"]["top_k"], 5)
        self.assertEqual(decision["effective_request_overrides"]["semantic_rerank_limit"], 8)

    def test_unknown_feedback_can_be_gated_off_in_auto_mode(self) -> None:
        decision = decide_rerun_policy(
            {"rerun_retrieval": True},
            feedback={"feedback_type": "note_only"},
            recompile={"status": "recompiled"},
            baseline_job={
                "job_id": "job1",
                "request": {"top_k": 5},
                "plan": {"retrieval_plan": {"strategy": "hybrid"}},
                "summary": {"total_matches": 3},
            },
        )
        self.assertEqual(decision["status"], "gated_off")
        self.assertEqual(decision["mode"], "none")

    def test_explicit_full_rerun_bypasses_auto_gate(self) -> None:
        decision = decide_rerun_policy(
            {"rerun_retrieval": "full"},
            feedback={"feedback_type": "note_only"},
            recompile={"status": "recompiled"},
            baseline_job={
                "job_id": "job1",
                "request": {"top_k": 5},
                "plan": {"retrieval_plan": {"strategy": "hybrid"}},
                "summary": {"total_matches": 3},
            },
        )
        self.assertEqual(decision["status"], "approved")
        self.assertEqual(decision["mode"], "full")
        self.assertEqual(decision["runtime_policy"]["summary_mode"], "default")
