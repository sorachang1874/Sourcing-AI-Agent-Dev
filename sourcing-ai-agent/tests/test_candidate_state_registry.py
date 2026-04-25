import tempfile
import unittest

from sourcing_agent.storage import SQLiteStore


class CandidateStateRegistryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = SQLiteStore(f"{self.tempdir.name}/state.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_candidate_review_registry_upsert_is_stable_per_job_and_candidate(self) -> None:
        created = self.store.upsert_candidate_review_record(
            {
                "job_id": "job_1",
                "history_id": "history_1",
                "candidate_id": "cand_1",
                "candidate_name": "Alice Example",
                "headline": "Research Engineer",
                "current_company": "Reflection AI",
                "status": "needs_review",
                "source": "manual_add",
            }
        )
        updated = self.store.upsert_candidate_review_record(
            {
                "job_id": "job_1",
                "history_id": "history_1",
                "candidate_id": "cand_1",
                "candidate_name": "Alice Example",
                "headline": "Research Engineer",
                "current_company": "Reflection AI",
                "status": "verified_keep",
                "comment": "Confirmed by analyst.",
                "source": "backend_override",
            }
        )

        records = self.store.list_candidate_review_records(job_id="job_1")
        self.assertEqual(len(records), 1)
        self.assertEqual(created["id"], updated["id"])
        self.assertEqual(records[0]["status"], "verified_keep")
        self.assertEqual(records[0]["comment"], "Confirmed by analyst.")
        self.assertEqual(records[0]["source"], "backend_override")

    def test_candidate_review_registry_accepts_profile_completion_status(self) -> None:
        created = self.store.upsert_candidate_review_record(
            {
                "job_id": "job_profile",
                "candidate_id": "cand_profile",
                "candidate_name": "Profile Gap Example",
                "status": "needs_profile_completion",
                "source": "manual_review",
            }
        )

        records = self.store.list_candidate_review_records(job_id="job_profile")
        self.assertEqual(len(records), 1)
        self.assertEqual(created["status"], "needs_profile_completion")
        self.assertEqual(records[0]["status"], "needs_profile_completion")

    def test_candidate_review_registry_accepts_low_profile_richness_status(self) -> None:
        created = self.store.upsert_candidate_review_record(
            {
                "job_id": "job_profile_richness",
                "candidate_id": "cand_profile_richness",
                "candidate_name": "Sparse Profile Example",
                "status": "low_profile_richness",
                "source": "manual_review",
            }
        )

        records = self.store.list_candidate_review_records(job_id="job_profile_richness")
        self.assertEqual(len(records), 1)
        self.assertEqual(created["status"], "low_profile_richness")
        self.assertEqual(records[0]["status"], "low_profile_richness")

    def test_target_candidate_registry_upsert_is_stable_per_candidate_identity(self) -> None:
        created = self.store.upsert_target_candidate(
            {
                "candidate_id": "cand_target",
                "candidate_name": "Bob Example",
                "headline": "Member of Technical Staff",
                "current_company": "Anthropic",
                "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                "follow_up_status": "pending_outreach",
            }
        )
        updated = self.store.upsert_target_candidate(
            {
                "candidate_id": "cand_target",
                "candidate_name": "Bob Example",
                "headline": "Member of Technical Staff",
                "current_company": "Anthropic",
                "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                "follow_up_status": "accepted",
                "quality_score": 9,
                "comment": "Strong fit.",
            }
        )

        records = self.store.list_target_candidates()
        self.assertEqual(len(records), 1)
        self.assertEqual(created["id"], updated["id"])
        self.assertEqual(records[0]["follow_up_status"], "accepted")
        self.assertEqual(records[0]["quality_score"], 9.0)
        self.assertEqual(records[0]["comment"], "Strong fit.")


if __name__ == "__main__":
    unittest.main()
