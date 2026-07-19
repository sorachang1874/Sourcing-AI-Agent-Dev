from __future__ import annotations

import copy
import unittest

from tests.test_selected_subject_adapter import ROOT, _campaign_intent, _selection
from x_first.portable_campaign_package import (
    PortableCampaignPackageError,
    build_fixture_simulate_package,
)
from x_first.recall_pool_schema import MiniDraft202012Error
from x_first.research_orchestration import (
    _content_sha256,
    build_campaign_plan,
    load_policy,
    strict_load_json,
    text_sha256,
)
from x_first.selected_subject_adapter import build_selected_subject_request_binding


def _selected_result(*, request: dict, plan: dict, catalog: dict, selection: dict) -> dict:
    base = strict_load_json(ROOT / "fixtures" / "portable_research_campaign_result_fixture_v1.json")
    result = copy.deepcopy(base)
    result.update(
        campaign_id=request["campaign_id"],
        request_sha256=request["request_sha256"],
        plan_sha256=plan["plan_sha256"],
        catalog_sha256=catalog["catalog_sha256"],
    )
    result["subject_outcomes"] = [
        {
            "seed_ref": "selected_linkedin_subject",
            "terminal_state": "analyzed",
            "x_account_refs": ["xacct_selected_x"],
            "reason": "fixture handle analyzed",
        },
        {
            "seed_ref": "selected_in_progress_subject",
            "terminal_state": "research_in_progress",
            "x_account_refs": ["xacct_progress_x"],
            "reason": "fixture pagination continuation remains available",
        },
        {
            "seed_ref": "selected_name_only_subject",
            "terminal_state": "handle_resolution_required",
            "x_account_refs": [],
            "reason": "fixture name-only subject needs resolution",
        },
    ]
    result["external_accounts"] = [
        {
            "x_account_ref": "xacct_progress_x",
            "platform_user_id": None,
            "current_handle": "progress_x",
            "profile_url": "https://x.com/progress_x",
            "identity_status": "provisional_handle",
            "handle_history_proposals": [],
        },
        {
            "x_account_ref": "xacct_selected_x",
            "platform_user_id": None,
            "current_handle": "selected_x",
            "profile_url": "https://x.com/selected_x",
            "identity_status": "provisional_handle",
            "handle_history_proposals": [],
        }
    ]
    query = "resolve selected researcher to @selected_x"
    observed_value = "Fixture X profile matches selected researcher handle proposal."
    retrieval_receipt = {
        "schema_version": "x.handle_resolution.retrieval_receipt.v1",
        "query_sha256": text_sha256(query),
        "canonical_url": "https://x.com/selected_x",
        "observed_at": "2026-07-18T08:00:00Z",
        "content_sha256": text_sha256(observed_value),
        "source_status": "fixture_synthetic",
        "receipt_locator": None,
        "receipt_sha256": "",
    }
    retrieval_receipt["receipt_sha256"] = _content_sha256(
        retrieval_receipt, "receipt_sha256"
    )
    result["handle_resolution_evidence"] = [
        {
            "evidence_id": "hre_selected_x",
            "seed_ref": "selected_linkedin_subject",
            "x_account_ref": "xacct_selected_x",
            "evidence_kind": "x_profile_name",
            "canonical_url": "https://x.com/selected_x",
            "observed_at": "2026-07-18T08:00:00Z",
            "query_text": query,
            "query_sha256": text_sha256(query),
            "observed_value": observed_value,
            "content_sha256": text_sha256(observed_value),
            "source_status": "fixture_synthetic",
            "receipt_ref": f"sha256:{retrieval_receipt['receipt_sha256']}",
            "retrieval_receipt": retrieval_receipt,
        }
    ]
    progress_query = "resolve selected in-progress researcher to @progress_x"
    progress_value = "Fixture X profile matches in-progress researcher handle proposal."
    progress_receipt = {
        "schema_version": "x.handle_resolution.retrieval_receipt.v1",
        "query_sha256": text_sha256(progress_query),
        "canonical_url": "https://x.com/progress_x",
        "observed_at": "2026-07-18T08:00:00Z",
        "content_sha256": text_sha256(progress_value),
        "source_status": "fixture_synthetic",
        "receipt_locator": None,
        "receipt_sha256": "",
    }
    progress_receipt["receipt_sha256"] = _content_sha256(
        progress_receipt, "receipt_sha256"
    )
    result["handle_resolution_evidence"].append(
        {
            "evidence_id": "hre_progress_x",
            "seed_ref": "selected_in_progress_subject",
            "x_account_ref": "xacct_progress_x",
            "evidence_kind": "x_profile_name",
            "canonical_url": "https://x.com/progress_x",
            "observed_at": "2026-07-18T08:00:00Z",
            "query_text": progress_query,
            "query_sha256": text_sha256(progress_query),
            "observed_value": progress_value,
            "content_sha256": text_sha256(progress_value),
            "source_status": "fixture_synthetic",
            "receipt_ref": f"sha256:{progress_receipt['receipt_sha256']}",
            "retrieval_receipt": progress_receipt,
        }
    )
    result["cross_source_link_proposals"] = [
        {
            "proposal_id": "link_progress_x",
            "seed_ref": "selected_in_progress_subject",
            "x_account_ref": "xacct_progress_x",
            "status": "proposed",
            "evidence_refs": ["hre_progress_x"],
            "source_status": "fixture_synthetic",
            "human_review_required": True,
            "automatic_merge_authorized": False,
        },
        {
            "proposal_id": "link_selected_x",
            "seed_ref": "selected_linkedin_subject",
            "x_account_ref": "xacct_selected_x",
            "status": "proposed",
            "evidence_refs": ["hre_selected_x"],
            "source_status": "fixture_synthetic",
            "human_review_required": True,
            "automatic_merge_authorized": False,
        }
    ]
    result["discovery_origins"] = []
    result["surface_attempts"] = []
    result["observations"] = []
    result["handle_resolution_attempts"] = []
    result["handle_resolution_outcomes"] = []
    for handle_index, (handle, account_ref) in enumerate(
        (("progress_x", "xacct_progress_x"), ("selected_x", "xacct_selected_x")),
        start=1,
    ):
        for surface_index, surface in enumerate(("post", "reply"), start=1):
            query_text = (
                f"from:{handle} -filter:replies"
                if surface == "post"
                else f"from:{handle} filter:replies"
            )
            receipt_ref = f"fixture://receipt/{handle}/{surface}"
            is_continuation = handle == "progress_x" and surface == "post"
            result["surface_attempts"].append(
                {
                    "x_account_ref": account_ref,
                    "surface": surface,
                    "ordinal": 1,
                    "query_text": query_text,
                    "query_sha256": text_sha256(query_text),
                    "receipt_ref": receipt_ref,
                    "source_status": "fixture_synthetic",
                    "execution_state": "completed",
                    "bound_observation_count": 1,
                    "result_truncated": is_continuation,
                    "continuation_state": (
                        "continuation_available" if is_continuation else "exhausted"
                    ),
                    "input_continuation_ref": None,
                    "continuation_ref": (
                        "fixture://cursor/progress_x/post/page-2"
                        if is_continuation
                        else None
                    ),
                }
            )
            text = f"Fixture {handle} {surface} discusses coding research."
            object_id = 3000 + handle_index * 10 + surface_index
            result["observations"].append(
                {
                    "observation_id": f"obs_{handle}_{surface}",
                    "x_account_ref": account_ref,
                    "surface": surface,
                    "platform_object_id": str(object_id),
                    "canonical_url": f"https://x.com/{handle}/status/{object_id}",
                    "author_platform_user_id": None,
                    "author_handle": handle,
                    "published_at": f"2026-07-17T0{7 + surface_index}:00:00Z",
                    "observed_at": "2026-07-18T08:00:00Z",
                    "text_or_excerpt": text,
                    "content_sha256": text_sha256(text),
                    "source_status": "fixture_synthetic",
                    "receipt_ref": receipt_ref,
                }
            )
    result["semantic_recall_attempts"] = []
    result["semantic_recall_outcomes"] = [
        {
            "x_account_ref": "xacct_selected_x",
            "question_id": "verify_coding_work",
            "execution_policy": "authored_surface_only",
            "terminal_state": "authored_surface_complete",
            "attempted_query_sha256s": [],
            "stop_reason": "broad_authored_surface_completed",
            "receipt_refs": [
                "fixture://receipt/selected_x/post",
                "fixture://receipt/selected_x/reply",
            ],
            "adaptive_stop_decision": None,
        }
    ]
    result["affiliation_results"] = [
        {
            "x_account_ref": "xacct_selected_x",
            "scope_id": "fixture_lab",
            "temporal_state": "current",
            "matches_temporal_filter": True,
            "evidence_refs": ["selected_linkedin_subject"],
            "reason": "source profile reports current affiliation",
            "source_status": "source_bound",
        }
    ]
    result["dimension_results"] = [
        {
            "x_account_ref": "xacct_selected_x",
            "question_id": "verify_coding_work",
            "dimension_id": "research_workstream",
            "matched_label_ids": ["coding"],
            "relevance_state": "target_core",
            "target_activity_temporal_state": "current",
            "matches_temporal_filter": True,
            "evidence_refs": ["obs_selected_x_post", "obs_selected_x_reply"],
            "reason": "fixture authored surfaces discuss coding",
            "source_status": "fixture_synthetic",
        }
    ]
    for field in (
        "exploratory_findings",
        "experience_verification_queue",
        "optional_channel_evidence",
        "optional_channel_attempts",
        "optional_channel_outcomes",
        "relationship_results",
    ):
        result[field] = []
    result["coverage"] = {
        "subjects_total": 3,
        "terminal_subject_outcomes": 2,
        "resolved_account_count": 2,
        "candidate_authored_post_attempted": 2,
        "candidate_authored_reply_attempted": 2,
        "candidate_authored_both_surface_attempted": 2,
        "observation_count": 4,
        "coverage_source_status": "fixture_synthetic",
        "metric_values": [
            {"metric_id": "candidate_authored_both_surface_coverage_rate", "numerator": 1, "denominator": 2},
            {"metric_id": "source_bound_evidence_rate", "numerator": 0, "denominator": 4},
            {"metric_id": "target_direction_core_rate", "numerator": 1, "denominator": 1},
            {"metric_id": "target_direction_active_rate", "numerator": 1, "denominator": 1},
            {"metric_id": "evidence_backed_temporal_state_rate", "numerator": 2, "denominator": 2},
            {"metric_id": "target_core_unique_account_yield_per_native_call", "numerator": 0, "denominator": 0},
            {"metric_id": "cross_source_handle_resolution_rate", "numerator": 2, "denominator": 3},
        ],
    }
    result["status"] = "partial"
    result["limitations"] = []
    result["result_sha256"] = ""
    result["result_sha256"] = _content_sha256(result, "result_sha256")
    return result


def selected_fixture_package() -> dict:
    selection = _selection()
    policy = load_policy()
    catalog = strict_load_json(ROOT / "fixtures" / "research_scope_catalog_fixture_v1.json")
    request, binding = build_selected_subject_request_binding(
        selection=selection,
        campaign_intent=_campaign_intent(),
        catalog=catalog,
        policy=policy,
    )
    plan = build_campaign_plan(request=request, catalog=catalog, policy=policy)
    result = _selected_result(
        request=request,
        plan=plan,
        catalog=catalog,
        selection=selection,
    )
    return build_fixture_simulate_package(
        selection=selection,
        policy=policy,
        catalog=catalog,
        request=request,
        binding=binding,
        plan=plan,
        result=result,
        validated_at="2026-07-18T08:02:00Z",
    )


class PortableCampaignPackageTests(unittest.TestCase):
    def test_selected_fixture_produces_semantically_valid_bound_receipt(self) -> None:
        package = selected_fixture_package()

        self.assertEqual(package["manifest"]["package_mode"], "fixture_simulate")
        self.assertEqual(package["semantic_validation_receipt"]["validation_status"], "passed")
        self.assertFalse(package["semantic_validation_receipt"]["authority"]["live_authority"])
        self.assertEqual(package["semantic_validation_receipt"]["effects"]["provider_call_count"], 0)

    def test_malformed_rehashed_result_never_receives_a_semantic_receipt(self) -> None:
        package = selected_fixture_package()
        result = copy.deepcopy(package["artifacts"]["result"])
        result["observations"][0].pop("canonical_url")
        result["result_sha256"] = _content_sha256(result, "result_sha256")

        with self.assertRaisesRegex(
            MiniDraft202012Error,
            r"schema_validation_failed:.*missing:canonical_url",
        ):
            build_fixture_simulate_package(
                selection=package["artifacts"]["selection"],
                policy=package["artifacts"]["policy"],
                catalog=package["artifacts"]["catalog"],
                request=package["artifacts"]["request"],
                binding=package["artifacts"]["binding"],
                plan=package["artifacts"]["plan"],
                result=result,
                validated_at="2026-07-18T08:02:00Z",
            )

    def test_non_fixture_handle_resolution_attempt_never_receives_a_semantic_receipt(self) -> None:
        package = selected_fixture_package()
        result = copy.deepcopy(package["artifacts"]["result"])
        subject = next(
            row for row in result["subject_outcomes"] if row["seed_ref"] == "selected_name_only_subject"
        )
        subject["terminal_state"] = "failed"
        subject["reason"] = "resolution execution failed"
        query_text = "resolve selected name only researcher"
        error = {
            "error_code": "x_search_timeout",
            "error_message_sha256": text_sha256("simulated resolution timeout"),
        }
        receipt = {
            "schema_version": "x.handle_resolution.attempt_receipt.v1",
            "query_sha256": text_sha256(query_text),
            "observed_at": "2026-07-18T08:00:00Z",
            "execution_state": "failed",
            "result_truncated": False,
            "continuation_state": "unknown",
            "input_continuation_ref": None,
            "continuation_ref": None,
            "source_status": "receipt_bound",
            "error": error,
            "receipt_locator": "fixture://receipt/handle-resolution/selected-name-only/failed-1",
            "receipt_sha256": "",
        }
        receipt["receipt_sha256"] = _content_sha256(receipt, "receipt_sha256")
        attempt = {
            "attempt_id": "hra_selected_name_only_1",
            "seed_ref": "selected_name_only_subject",
            "ordinal": 1,
            "query_text": query_text,
            "query_sha256": receipt["query_sha256"],
            "observed_at": "2026-07-18T08:00:00Z",
            "receipt_ref": f"sha256:{receipt['receipt_sha256']}",
            "source_status": "receipt_bound",
            "execution_state": "failed",
            "result_truncated": False,
            "continuation_state": "unknown",
            "input_continuation_ref": None,
            "continuation_ref": None,
            "error": error,
            "retrieval_receipt": receipt,
        }
        result["handle_resolution_attempts"] = [attempt]
        result["handle_resolution_outcomes"] = [
            {
                "seed_ref": "selected_name_only_subject",
                "terminal_state": "failed",
                "attempt_ids": [attempt["attempt_id"]],
                "receipt_refs": [attempt["receipt_ref"]],
                "reason_code": "execution_failed",
                "reason": "resolution search timed out before any verified match",
                "error_codes": ["x_search_timeout"],
                "source_status": "receipt_bound",
            }
        ]
        result["result_sha256"] = _content_sha256(result, "result_sha256")

        with self.assertRaisesRegex(PortableCampaignPackageError, "portable_package_not_fixture_only"):
            build_fixture_simulate_package(
                selection=package["artifacts"]["selection"],
                policy=package["artifacts"]["policy"],
                catalog=package["artifacts"]["catalog"],
                request=package["artifacts"]["request"],
                binding=package["artifacts"]["binding"],
                plan=package["artifacts"]["plan"],
                result=result,
                validated_at="2026-07-18T08:02:00Z",
            )

    def test_noncanonical_plan_never_receives_a_semantic_receipt(self) -> None:
        package = selected_fixture_package()
        plan = copy.deepcopy(package["artifacts"]["plan"])
        plan["counts"]["candidate_authored_task_count"] = 99
        plan["plan_sha256"] = _content_sha256(plan, "plan_sha256")

        with self.assertRaisesRegex(PortableCampaignPackageError, "plan_not_canonical"):
            build_fixture_simulate_package(
                selection=package["artifacts"]["selection"],
                policy=package["artifacts"]["policy"],
                catalog=package["artifacts"]["catalog"],
                request=package["artifacts"]["request"],
                binding=package["artifacts"]["binding"],
                plan=plan,
                result=package["artifacts"]["result"],
                validated_at="2026-07-18T08:02:00Z",
            )


if __name__ == "__main__":
    unittest.main()
