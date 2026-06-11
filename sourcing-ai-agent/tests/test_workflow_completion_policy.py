import unittest

from sourcing_agent.workflow_completion_policy import (
    COMPLETION_POLICY_VERSION,
    completion_policy_first_blocker,
    evaluate_linkedin_completion_policies,
)


class WorkflowCompletionPolicyTest(unittest.TestCase):
    def test_policy_blocks_delta_profile_fetch_before_board_visibility(self) -> None:
        evaluation = evaluate_linkedin_completion_policies(
            job_id="job_policy_fetch",
            lifecycle={
                "delta_profile_required_count": 10,
                "delta_profile_fetched_count": 7,
                "delta_profile_board_visible_count": 7,
            },
        )
        blocker = completion_policy_first_blocker(evaluation)

        self.assertEqual(evaluation["policy_version"], COMPLETION_POLICY_VERSION)
        self.assertEqual(blocker["reason"], "delta_profile_fetch_incomplete")
        self.assertEqual(evaluation["policies"]["profile_fetch_terminal"]["status"], "blocked")

    def test_policy_uses_patch_watermark_for_board_visible_terminal(self) -> None:
        evaluation = evaluate_linkedin_completion_policies(
            job_id="job_policy_patch",
            lifecycle={
                "delta_profile_required_count": 10,
                "delta_profile_fetched_count": 7,
                "delta_profile_board_visible_count": 7,
            },
            patch_projection={"delta_profile_board_visible_count": 10},
            workflow_current_state={
                "completion_proofs": {
                    "serving_finalized": {
                        "status": "proved",
                        "event_id": "evt_serving_patch",
                        "sequence_number": 11,
                    }
                }
            },
        )
        blocker = completion_policy_first_blocker(evaluation)

        self.assertEqual(blocker, {})
        self.assertEqual(evaluation["policies"]["profile_fetch_terminal"]["status"], "proved")
        self.assertEqual(evaluation["policies"]["board_visible_terminal"]["status"], "proved")

    def test_policy_blocks_only_completion_blocking_materialization_items(self) -> None:
        evaluation = evaluate_linkedin_completion_policies(
            job_id="job_policy_items",
            lifecycle={
                "delta_profile_required_count": 4,
                "delta_profile_fetched_count": 4,
                "delta_profile_board_visible_count": 4,
            },
            materialization_items=[
                {
                    "item_id": "snapshot-tail",
                    "item_kind": "snapshot_full_materialization",
                    "status": "queued",
                },
                {
                    "item_id": "board-visible",
                    "item_kind": "board_visible_delta_apply",
                    "status": "queued",
                },
            ],
            blocking_materialization_item_kinds={"board_visible_delta_apply", "local_apply_closure"},
        )
        blocker = completion_policy_first_blocker(evaluation)

        self.assertEqual(blocker["reason"], "active_materialization_items_present")
        self.assertEqual(blocker["active_materialization_item_count"], 1)
        self.assertEqual(blocker["active_materialization_item_ids"], ["board-visible"])
        self.assertEqual(evaluation["policies"]["board_visible_terminal"]["status"], "blocked")

    def test_policy_records_serving_finalized_proof_independently_of_background_merge(self) -> None:
        evaluation = evaluate_linkedin_completion_policies(
            job_id="job_policy_serving",
            lifecycle={
                "delta_profile_required_count": 0,
                "stage1_deduped_candidate_count": 12,
            },
            workflow_current_state={
                "completion_proofs": {
                    "serving_finalized": {
                        "status": "proved",
                        "event_id": "evt_1",
                        "sequence_number": 7,
                    }
                }
            },
        )

        self.assertEqual(evaluation["status"], "proved")
        self.assertEqual(evaluation["policies"]["serving_finalized"]["status"], "proved")
        self.assertEqual(evaluation["policies"]["collection_merge_terminal"]["status"], "not_applicable")

    def test_policy_blocks_terminal_without_serving_finalized_proof(self) -> None:
        evaluation = evaluate_linkedin_completion_policies(
            job_id="job_policy_missing_serving",
            lifecycle={
                "delta_profile_required_count": 0,
                "stage1_deduped_candidate_count": 12,
            },
            workflow_current_state={},
        )

        blocker = completion_policy_first_blocker(evaluation)

        self.assertEqual(evaluation["status"], "blocked")
        self.assertEqual(blocker["reason"], "serving_finalized_proof_missing")
        self.assertEqual(evaluation["policies"]["serving_finalized"]["status"], "blocked")

    def test_policy_exempts_serving_finalized_when_durable_run_is_absent(self) -> None:
        evaluation = evaluate_linkedin_completion_policies(
            job_id="job_policy_legacy_no_run",
            lifecycle={
                "delta_profile_required_count": 0,
                "stage1_deduped_candidate_count": 12,
            },
            workflow_current_state={},
            workflow_run_absent=True,
        )

        self.assertEqual(evaluation["status"], "proved")
        self.assertEqual(
            evaluation["policies"]["serving_finalized"],
            {
                "status": "not_applicable",
                "reason": "legacy_job_without_durable_workflow_run",
            },
        )

    def test_policy_run_absent_flag_does_not_override_recorded_proof_or_other_blockers(self) -> None:
        proved = evaluate_linkedin_completion_policies(
            job_id="job_policy_absent_with_proof",
            lifecycle={
                "delta_profile_required_count": 0,
                "stage1_deduped_candidate_count": 12,
            },
            workflow_current_state={
                "completion_proofs": {
                    "serving_finalized": {
                        "status": "proved",
                        "event_id": "evt_absent_proof",
                        "sequence_number": 3,
                    }
                }
            },
            workflow_run_absent=True,
        )
        self.assertEqual(proved["policies"]["serving_finalized"]["status"], "proved")

        still_blocked = evaluate_linkedin_completion_policies(
            job_id="job_policy_absent_with_workers",
            active_worker_count=2,
            lifecycle={
                "delta_profile_required_count": 0,
                "stage1_deduped_candidate_count": 12,
            },
            workflow_current_state={},
            workflow_run_absent=True,
        )
        self.assertEqual(still_blocked["status"], "blocked")
        self.assertEqual(
            completion_policy_first_blocker(still_blocked)["reason"],
            "active_workers_present",
        )


if __name__ == "__main__":
    unittest.main()
