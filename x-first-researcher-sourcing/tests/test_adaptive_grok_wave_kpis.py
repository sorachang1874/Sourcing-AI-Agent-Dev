from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.test_adaptive_grok_wave_runner import (
    TEST_EFFECTIVE_PROMPT_POLICY,
    _completed_live_run,
    _empty_result,
)
from x_first import adaptive_grok_wave_runner as runner
from x_first.adaptive_grok_wave_kpis import (
    AdaptiveWaveKPIError,
    _summarize_validated_pair,
    analyze_operator_bundle,
    analyze_sanitized_pair_paths,
)


def _evidence(
    subject: str,
    *,
    kind: str,
    relation: str | None,
    supports: list[tuple[str, str]],
) -> dict[str, object]:
    return {
        "kind": kind,
        "thread_relation": relation,
        "supports": [
            {"dimension": dimension, "asserted_value": asserted_value}
            for dimension, asserted_value in supports
        ],
        "url": f"https://x.com/{subject}/status/123456" if relation is not None else f"https://x.com/{subject}",
    }


def _candidate(
    handle: str,
    *,
    lab_state: str,
    pretraining_state: str,
    stable_id: str | None,
    bio: str | None,
    evidence: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "handle": handle,
        "platform_user_id": stable_id,
        "bio_excerpt": bio,
        "target_lab_affiliation_state": lab_state,
        "pretraining_experience_state": pretraining_state,
        "evidence": evidence,
    }


def _summary_inputs() -> tuple[dict[str, object], dict[str, object]]:
    candidates = [
        _candidate(
            "PrivateA",
            lab_state="current",
            pretraining_state="current",
            stable_id="100",
            bio="Private candidate A biography.",
            evidence=[
                _evidence(
                    "PrivateA",
                    kind="bio",
                    relation=None,
                    supports=[("target_lab_affiliation_state", "current")],
                ),
                _evidence(
                    "PrivateA",
                    kind="post",
                    relation="self_post",
                    supports=[("pretraining_experience_state", "current")],
                ),
                _evidence(
                    "PrivateA",
                    kind="post",
                    relation="reply",
                    supports=[("pretraining_experience_state", "historical")],
                ),
            ],
        ),
        _candidate(
            "PrivateB",
            lab_state="historical",
            pretraining_state="historical",
            stable_id=None,
            bio=None,
            evidence=[
                _evidence(
                    "PrivateB",
                    kind="mention",
                    relation="reply",
                    supports=[
                        ("target_lab_affiliation_state", "historical"),
                        ("pretraining_experience_state", "historical"),
                    ],
                )
            ],
        ),
        _candidate(
            "PrivateC",
            lab_state="current",
            pretraining_state="historical",
            stable_id="300",
            bio=None,
            evidence=[
                _evidence(
                    "PrivateC",
                    kind="thread",
                    relation="thread_root",
                    supports=[
                        ("target_lab_affiliation_state", "current"),
                        ("pretraining_experience_state", "historical"),
                    ],
                )
            ],
        ),
        _candidate(
            "PrivateD",
            lab_state="ambiguous",
            pretraining_state="unsupported",
            stable_id=None,
            bio=None,
            evidence=[],
        ),
    ]
    tool_counts = {
        "x_keyword_search": 2,
        "x_semantic_search": 1,
        "x_user_search": 1,
    }
    result: dict[str, object] = {
        "status": "X_SEARCH_PARTIAL",
        "counts": {"candidates_retained": 4},
        "native_x_tool_provenance": {"tool_calls_reported": 4},
        "local_reconciliation": {
            "candidate_records_validated": 4,
            "evidence_items_validated": 5,
            "post_urls_structurally_validated": 4,
            "tool_calls_completed": 4,
            "tool_counts": tool_counts,
        },
        "candidates": candidates,
    }
    coverage = []
    for handle, post_attempted, reply_attempted in (
        ("privatea", True, True),
        ("privateb", False, True),
        ("privatec", True, False),
        ("privated", False, False),
    ):
        coverage.append(
            {
                "handle_key": handle,
                "authored_post": {"attempted": post_attempted, "query_argument_sha256s": []},
                "authored_reply": {"attempted": reply_attempted, "query_argument_sha256s": []},
            }
        )
    receipt: dict[str, object] = {
        "status": "completed",
        "process": {"elapsed_ms": 4_000},
        "session_proof": {"completed_tool_calls": 4, "tool_counts": tool_counts},
        "reconciliation": {
            "candidate_count": 4,
            "evidence_count": 5,
            "post_url_count": 4,
            "candidate_surface_coverage": coverage,
        },
    }
    return result, receipt


class AdaptiveGrokWaveKPITests(unittest.TestCase):
    def test_summary_covers_surfaces_states_profiles_and_reconciliation_without_candidate_data(self) -> None:
        result, receipt = _summary_inputs()
        summary = _summarize_validated_pair(result, receipt, validation_basis="synthetic_test")

        self.assertEqual(summary["throughput"]["candidate_count"], 4)
        self.assertEqual(summary["throughput"]["completed_native_x_calls"], 4)
        self.assertEqual(summary["throughput"]["unique_candidates_per_completed_native_x_call"], 1.0)
        self.assertEqual(summary["throughput"]["wall_time_ms_per_candidate"], 1000.0)
        self.assertEqual(
            summary["evidence"]["counts_by_kind"],
            {"bio": 1, "mention": 1, "post": 2, "thread": 1},
        )
        self.assertEqual(summary["evidence"]["candidates_with_post_evidence"], 2)
        self.assertEqual(summary["evidence"]["candidates_with_reply_evidence"], 2)
        self.assertEqual(
            summary["evidence"]["candidate_surface_overlap"],
            {"post_only": 1, "reply_only": 1, "both": 1, "neither": 1},
        )
        self.assertEqual(
            summary["authored_surface_attempt_coverage"]["authored_reply"],
            {"covered_candidates": 2, "denominator_candidates": 4, "coverage_rate": 0.5},
        )
        self.assertEqual(summary["candidate_state_matrix"]["counts"]["current"]["current"], 1)
        self.assertEqual(summary["candidate_state_matrix"]["counts"]["current"]["historical"], 1)
        self.assertEqual(summary["candidate_state_matrix"]["counts"]["historical"]["historical"], 1)
        self.assertEqual(summary["candidate_state_matrix"]["counts"]["ambiguous"]["unsupported"], 1)
        proposals = summary["model_evidence_proposal_matrix"]["counts"]
        self.assertEqual(proposals["current"]["conflict"], 1)
        self.assertEqual(proposals["historical"]["historical"], 1)
        self.assertEqual(proposals["current"]["historical"], 1)
        self.assertEqual(proposals["none"]["none"], 1)
        self.assertEqual(summary["profile_coverage"]["stable_platform_user_id"]["coverage_rate"], 0.5)
        self.assertEqual(summary["profile_coverage"]["bio_excerpt"]["coverage_rate"], 0.25)
        self.assertFalse(summary["model_reported_vs_ledger_discrepancies"]["any_discrepancy"])

        serialized = json.dumps(summary, sort_keys=True)
        for private_value in (
            "PrivateA",
            "privatea",
            "Private candidate A biography.",
            "https://x.com/PrivateA/status/123456",
        ):
            self.assertNotIn(private_value, serialized)

    def test_zero_denominators_are_null_instead_of_misleading_zero_rates(self) -> None:
        result = {
            "status": "X_SEARCH_PARTIAL",
            "counts": {"candidates_retained": 0},
            "native_x_tool_provenance": {"tool_calls_reported": 0},
            "local_reconciliation": {
                "candidate_records_validated": 0,
                "evidence_items_validated": 0,
                "post_urls_structurally_validated": 0,
                "tool_calls_completed": 0,
                "tool_counts": {},
            },
            "candidates": [],
        }
        receipt = {
            "status": "completed",
            "process": {"elapsed_ms": 0},
            "session_proof": {"completed_tool_calls": 0, "tool_counts": {}},
            "reconciliation": {
                "candidate_count": 0,
                "evidence_count": 0,
                "post_url_count": 0,
                "candidate_surface_coverage": [],
            },
        }
        summary = _summarize_validated_pair(result, receipt, validation_basis="synthetic_test")
        self.assertIsNone(summary["throughput"]["unique_candidates_per_completed_native_x_call"])
        self.assertIsNone(summary["throughput"]["wall_time_ms_per_candidate"])
        self.assertIsNone(summary["profile_coverage"]["bio_excerpt"]["coverage_rate"])

    def test_real_validator_replay_and_extracted_pair_paths_produce_same_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            runner,
            "DEFAULT_EFFECTIVE_PROMPT_POLICY",
            TEST_EFFECTIVE_PROMPT_POLICY,
        ):
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            bundle_summary = analyze_operator_bundle(run_root, approval_root=approvals)
            pair_summary = analyze_sanitized_pair_paths(
                run_root / "sanitized.json",
                run_root / "operator-receipt.json",
            )

            self.assertEqual(bundle_summary["validation_basis"], "operator_bundle_replayed")
            self.assertEqual(
                pair_summary["validation_basis"],
                "receipt_result_contracts_and_hash_reconciled",
            )
            for key in (
                "execution",
                "throughput",
                "evidence",
                "authored_surface_attempt_coverage",
                "candidate_state_matrix",
                "model_evidence_proposal_matrix",
                "profile_coverage",
                "model_reported_vs_ledger_discrepancies",
            ):
                self.assertEqual(bundle_summary[key], pair_summary[key])
            self.assertEqual(bundle_summary["throughput"]["candidate_count"], 0)
            self.assertEqual(bundle_summary["throughput"]["completed_native_x_calls"], 1)

            sanitized_path = run_root / "sanitized.json"
            tampered = json.loads(sanitized_path.read_text())
            tampered["status_reason"] = "Changed after bundle validation."
            sanitized_path.write_text(runner.canonical_json(tampered) + "\n")
            os.chmod(sanitized_path, 0o600)
            with self.assertRaisesRegex(AdaptiveWaveKPIError, "operator_bundle_replay_invalid"):
                analyze_operator_bundle(run_root, approval_root=approvals)

    def test_model_reported_call_mismatch_is_a_metric_not_a_validation_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            runner,
            "DEFAULT_EFFECTIVE_PROMPT_POLICY",
            TEST_EFFECTIVE_PROMPT_POLICY,
        ):
            model_result = _empty_result()
            model_result["native_x_tool_provenance"]["tool_calls_reported"] = 0
            model_result["native_x_tool_provenance"]["queries"] = []
            model_result["local_reconciliation"]["tool_calls_completed"] = 0
            model_result["local_reconciliation"]["tool_counts"] = {}
            root = Path(directory)
            run_root, approvals = _completed_live_run(root, model_result=model_result)

            summary = analyze_operator_bundle(run_root, approval_root=approvals)
            discrepancy = summary["model_reported_vs_ledger_discrepancies"]
            self.assertTrue(discrepancy["any_discrepancy"])
            self.assertEqual(
                discrepancy["completed_native_x_calls"],
                {
                    "model_provenance_reported": 0,
                    "model_local_reported": 1,
                    "ledger": 1,
                    "provenance_delta_from_ledger": -1,
                    "local_delta_from_ledger": 0,
                },
            )

    def test_pair_path_rejects_non_private_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = root / "result.json"
            receipt = root / "receipt.json"
            result.write_text("{}\n")
            receipt.write_text("{}\n")
            os.chmod(result, 0o644)
            os.chmod(receipt, 0o600)
            with self.assertRaisesRegex(AdaptiveWaveKPIError, "private_json_metadata_invalid"):
                analyze_sanitized_pair_paths(result, receipt)


if __name__ == "__main__":
    unittest.main()
