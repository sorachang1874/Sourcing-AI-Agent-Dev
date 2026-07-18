from __future__ import annotations

import copy
import inspect
import unittest

import x_first.research_orchestration as orchestration
from x_first.native_x_evidence_contract import classify_single_handle_query_surface
from x_first.research_orchestration import (
    ResearchOrchestrationError,
    build_campaign_plan,
    load_policy,
    strict_load_json,
    validate_campaign_request,
    validate_campaign_result,
    validate_checked_in_assets,
    validate_scope_catalog,
)

ROOT = orchestration.project_root()


def _catalog() -> dict:
    return strict_load_json(ROOT / "fixtures" / "research_scope_catalog_fixture_v1.json")


def _request() -> dict:
    return strict_load_json(ROOT / "fixtures" / "portable_research_campaign_request_fixture_v1.json")


def _plan() -> dict:
    return strict_load_json(ROOT / "fixtures" / "portable_research_campaign_plan_fixture_v1.json")


def _result() -> dict:
    return strict_load_json(ROOT / "fixtures" / "portable_research_campaign_result_fixture_v1.json")


def _rehash(value: dict, field: str) -> None:
    value[field] = orchestration._content_sha256(value, field)


def _complete_adaptive_recall(result: dict, plan: dict) -> None:
    task_by_handle = {row["handle"].casefold(): row for row in plan["candidate_authored_tasks"]}
    question = next(row for row in plan["analysis_contract"]["questions"] if row["question_id"] == "verify_coding_work")
    for account in result["external_accounts"]:
        task = task_by_handle.get(account["current_handle"].casefold())
        if task is None:
            continue
        target = next(row for row in task["recall_targets"] if row["question_id"] == "verify_coding_work")
        selected_queries = [
            row
            for surface in ("post", "reply")
            for row in [item for item in target["recall_queries"] if item["surface"] == surface][
                : target["adaptive_stop_policy"]["minimum_attempts_per_surface"]
            ]
        ]
        attempts = []
        for ordinal, query in enumerate(selected_queries, start=1):
            receipt_ref = f"fixture://receipt/{account['current_handle']}/semantic/{ordinal}"
            attempt = {
                "x_account_ref": account["x_account_ref"],
                "question_id": "verify_coding_work",
                "label_id": query["label_id"],
                "ordinal": ordinal,
                "page_ordinal": 1,
                "surface": query["surface"],
                "query_text": query["query_text"],
                "query_sha256": orchestration.text_sha256(query["query_text"]),
                "receipt_ref": receipt_ref,
                "source_status": "fixture_synthetic",
                "execution_state": "no_result",
                "bound_observation_count": 0,
                "result_truncated": False,
                "continuation_state": "exhausted",
                "input_continuation_ref": None,
                "continuation_ref": None,
                "new_unique_observation_refs": [],
                "new_target_evidence_refs": [],
            }
            result["semantic_recall_attempts"].append(attempt)
            attempts.append(attempt)
        outcome = next(
            row
            for row in result["semantic_recall_outcomes"]
            if row["x_account_ref"] == account["x_account_ref"] and row["question_id"] == "verify_coding_work"
        )
        outcome.update(
            {
                "terminal_state": "adaptive_complete",
                "attempted_query_sha256s": [row["query_sha256"] for row in attempts],
                "stop_reason": "marginal_gain_sustained_low",
                "receipt_refs": [row["receipt_ref"] for row in attempts],
            }
        )
        all_query_hashes = {orchestration.text_sha256(row["query_text"]) for row in target["recall_queries"]}
        covered_pairs = [
            {"label_id": label["label_id"], "surface": surface}
            for label in question["target_labels"]
            for surface in ("post", "reply")
        ]
        audit = {
            "schema_version": "x.semantic_recall.scope_frontier_audit.v1",
            "x_account_ref": account["x_account_ref"],
            "question_id": "verify_coding_work",
            "covered_label_surface_pairs": covered_pairs,
            "attempted_query_sha256s": sorted({row["query_sha256"] for row in attempts}),
            "remaining_query_sha256s": sorted(all_query_hashes - {row["query_sha256"] for row in attempts}),
            "audit_status": "negative_stop_frontier_audited",
            "source_status": "fixture_synthetic",
            "audit_sha256": "",
        }
        _rehash(audit, "audit_sha256")
        decision = {
            "window_size": target["adaptive_stop_policy"]["window_size"],
            "window_query_sha256s": [row["query_sha256"] for row in attempts],
            "observed_new_unique_observation_count": 0,
            "observed_new_target_evidence_count": 0,
            "configured_maximum_new_target_evidence": target["adaptive_stop_policy"][
                "maximum_new_target_evidence_in_window"
            ],
            "remaining_query_sha256s": sorted(all_query_hashes - {row["query_sha256"] for row in attempts}),
            "scope_frontier_audit": audit,
            "scope_frontier_audit_ref": f"sha256:{audit['audit_sha256']}",
            "source_status": "fixture_synthetic",
            "decision_sha256": "",
        }
        _rehash(decision, "decision_sha256")
        outcome["adaptive_stop_decision"] = decision


class ResearchOrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = load_policy()
        self.catalog = _catalog()
        self.request = _request()
        self.plan = _plan()
        self.result = _result()

    def test_checked_in_portable_boundary_is_valid(self) -> None:
        self.assertEqual(validate_checked_in_assets(), [])
        validate_scope_catalog(self.catalog, policy=self.policy)
        validate_campaign_request(self.request, catalog=self.catalog, policy=self.policy)
        self.assertEqual(
            build_campaign_plan(request=self.request, catalog=self.catalog, policy=self.policy),
            self.plan,
        )
        validate_campaign_result(
            self.result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )

    def test_candidate_authored_surface_is_default_and_other_channels_are_opt_in(self) -> None:
        request = copy.deepcopy(self.request)
        request["channel_overrides"] = []
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        enabled = [row["channel_id"] for row in plan["resolved_channels"] if row["enabled"]]
        self.assertEqual(enabled, ["candidate_authored_surface"])
        self.assertEqual(plan["optional_channel_tasks"], [])

    def test_verification_can_use_authored_post_and_reply_without_alias_searches(self) -> None:
        request = copy.deepcopy(self.request)
        question = next(row for row in request["analysis_questions"] if row["question_id"] == "verify_coding_work")
        question["recall_execution_policy"] = "authored_surface_only"
        question["adaptive_stop_policy"] = None
        _rehash(request, "request_sha256")

        validate_campaign_request(request, catalog=self.catalog, policy=self.policy)
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        for task in plan["candidate_authored_tasks"]:
            target = next(row for row in task["recall_targets"] if row["question_id"] == "verify_coding_work")
            self.assertEqual(target["execution_policy"], "authored_surface_only")
            self.assertEqual(target["recall_queries"], [])

    def test_coverage_queries_mechanically_prove_post_and_reply_without_boolean_escape(self) -> None:
        self.assertEqual(len(self.plan["candidate_authored_tasks"]), 2)
        for task in self.plan["candidate_authored_tasks"]:
            self.assertEqual(task["question_ids"], ["discover_research_topics", "verify_coding_work"])
            self.assertEqual([row["surface"] for row in task["coverage_queries"]], ["post", "reply"])
            for query in task["coverage_queries"]:
                expected_surface = "authored_post" if query["surface"] == "post" else "authored_reply"
                self.assertEqual(
                    classify_single_handle_query_surface(query["query_text"]),
                    (task["handle"].casefold(), expected_surface),
                )
                self.assertNotIn(" OR ", query["query_text"])
        verification = next(
            target
            for row in self.plan["candidate_authored_tasks"]
            for target in row["recall_targets"]
            if target["question_id"] == "verify_coding_work"
        )
        self.assertEqual(
            verification["recall_aliases"],
            ["code generation", "coding", "software engineering"],
        )
        self.assertEqual(len(verification["recall_queries"]), 6)
        for query in verification["recall_queries"]:
            self.assertNotIn(" OR ", query["query_text"])

    def test_exploratory_and_verification_questions_share_generic_runtime_taxonomy(self) -> None:
        taxonomy = next(row for row in self.catalog["taxonomies"] if row["dimension_id"] == "research_workstream")
        labels = {row["label_id"] for row in taxonomy["labels"]}
        self.assertTrue(
            {
                "pretraining",
                "posttraining",
                "midtraining",
                "evaluation",
                "benchmark",
                "infrastructure",
                "data",
                "coding",
                "mathematics",
                "multimodal",
            }.issubset(labels)
        )
        questions = {row["question_id"]: row for row in self.plan["analysis_contract"]["questions"]}
        self.assertEqual(questions["discover_research_topics"]["target_labels"], [])
        self.assertEqual(
            questions["verify_coding_work"]["target_labels"][0]["label_id"],
            "coding",
        )
        self.assertNotIn("pretraining_core", self.plan["analysis_contract"]["quality_metric_ids"])
        self.assertIn("target_direction_core_rate", self.plan["analysis_contract"]["quality_metric_ids"])

    def test_scope_catalog_requires_bounded_complete_and_fresh_project_family(self) -> None:
        partial = copy.deepcopy(self.catalog)
        partial["coverage_assertions"][0]["coverage_status"] = "partial"
        _rehash(partial, "catalog_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_coverage_incomplete"):
            build_campaign_plan(request=self.request, catalog=partial, policy=self.policy)

        stale = copy.deepcopy(self.catalog)
        stale["scope_nodes"][1]["refresh_after"] = "2026-07-18T06:00:00Z"
        _rehash(stale, "catalog_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_selected_node_stale_or_unknown"):
            build_campaign_plan(request=self.request, catalog=stale, policy=self.policy)

    def test_complete_project_family_membership_is_typed_and_digest_bound(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        coverage = catalog["coverage_assertions"][0]
        coverage["member_scope_ids"] = ["fixture_lab", "fixture_model", "fixture_product"]
        coverage["member_set_sha256"] = orchestration.canonical_sha256(coverage["member_scope_ids"])
        _rehash(catalog, "catalog_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_coverage_member_outside_claim"):
            validate_scope_catalog(catalog, policy=self.policy)

        catalog = copy.deepcopy(self.catalog)
        catalog["coverage_assertions"][0]["member_set_sha256"] = "0" * 64
        _rehash(catalog, "catalog_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_coverage_member_digest_invalid"):
            validate_scope_catalog(catalog, policy=self.policy)

    def test_explicit_scope_selection_must_cover_every_required_kind(self) -> None:
        request = copy.deepcopy(self.request)
        request["scope_selection"] = {
            "selection_mode": "explicit_nodes",
            "root_scope_ids": ["fixture_lab"],
            "selected_scope_ids": ["fixture_model"],
            "required_fresh_scope_kinds": ["model", "product", "capability"],
        }
        _rehash(request, "request_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_explicit_required_kind_empty"):
            build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)

    def test_catalog_rejects_verification_timestamp_after_snapshot_generation(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        catalog["scope_nodes"][0]["last_verified_at"] = "2026-07-18T00:00:01Z"
        _rehash(catalog, "catalog_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_node_freshness_window_invalid"):
            validate_scope_catalog(catalog, policy=self.policy)

    def test_catalog_rejects_one_alias_owned_by_multiple_runtime_labels(self) -> None:
        catalog = copy.deepcopy(self.catalog)
        taxonomy = next(row for row in catalog["taxonomies"] if row["dimension_id"] == "research_workstream")
        taxonomy["labels"][1]["aliases"].append(taxonomy["labels"][0]["aliases"][0].upper())
        _rehash(catalog, "catalog_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "scope_catalog_label_alias_ambiguous"):
            validate_scope_catalog(catalog, policy=self.policy)

    def test_linkedin_and_x_profile_hosts_cannot_cross_identity_namespaces(self) -> None:
        request = copy.deepcopy(self.request)
        linkedin = next(row for row in request["seed_inputs"] if row["source_kind"] == "linkedin_profile")
        linkedin["source_profile_url"] = "https://x.com/linked_seed"
        _rehash(request, "request_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_linkedin_seed_profile_host_invalid"):
            validate_campaign_request(request, catalog=self.catalog, policy=self.policy)

        request = copy.deepcopy(self.request)
        x_seed = next(row for row in request["seed_inputs"] if row["source_kind"] == "x_account")
        x_seed["source_profile_url"] = "https://www.linkedin.com/in/fixture-author"
        _rehash(request, "request_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_x_seed_profile_host_invalid"):
            validate_campaign_request(request, catalog=self.catalog, policy=self.policy)

        request = copy.deepcopy(self.request)
        x_seed = next(row for row in request["seed_inputs"] if row["source_kind"] == "x_account")
        x_seed["source_profile_url"] = "https://x.com/a_different_handle"
        _rehash(request, "request_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_x_seed_profile_handle_invalid"):
            validate_campaign_request(request, catalog=self.catalog, policy=self.policy)

    def test_cross_source_seed_is_proposal_and_name_only_seed_stays_in_resolution_queue(self) -> None:
        linked_tasks = [row for row in self.plan["candidate_authored_tasks"] if row["handle"] == "linked_seed"]
        self.assertEqual(len(linked_tasks), 1)
        for task in linked_tasks:
            self.assertEqual(task["handle_binding_statuses"], ["cross_source_link_proposed"])
            self.assertEqual(task["handle_proposals"][0]["seed_ref"], "seed_linkedin_profile")
            self.assertEqual(task["handle_proposals"][0]["source_status"], "fixture_synthetic")
        self.assertEqual(self.plan["handle_resolution_queue"][0]["seed_ref"], "seed_name_only")
        self.assertEqual(self.plan["handle_resolution_queue"][0]["source_status"], "fixture_synthetic")
        self.assertFalse(self.plan["integration_boundary"]["canonical_person_write_allowed"])
        self.assertTrue(self.plan["integration_boundary"]["cross_source_links_are_reversible_proposals"])

    def test_affiliation_and_target_activity_have_independent_temporal_axes(self) -> None:
        self.assertTrue(self.plan["temporal_scope"]["axes_are_independent"])
        self.assertEqual(
            self.plan["temporal_scope"]["affiliation_states"],
            ["current", "historical", "ambiguous"],
        )
        self.assertEqual(
            self.plan["temporal_scope"]["target_activity_states"],
            ["current", "historical", "ambiguous"],
        )

    def test_experience_queue_activates_only_for_explicit_query_request(self) -> None:
        request = copy.deepcopy(self.request)
        request["experience_verification"] = {
            "enabled": True,
            "trigger": "explicit_query_request",
            "dimension_ids": ["china_asia_professional_educational_experience"],
        }
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        self.assertEqual(plan["experience_verification"]["queue_activation"], "after_base_population")
        self.assertFalse(plan["experience_verification"]["base_population_rewrite_allowed"])
        self.assertFalse(plan["experience_verification"]["identity_inference_allowed"])

        request["experience_verification"]["trigger"] = "not_requested"
        _rehash(request, "request_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_experience_trigger_invalid"):
            validate_campaign_request(request, catalog=self.catalog, policy=self.policy)

    def test_result_is_terminal_total_for_every_portable_subject(self) -> None:
        result = copy.deepcopy(self.result)
        result["subject_outcomes"].pop()
        result["coverage"]["terminal_subject_outcomes"] -= 1
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_campaign_subject_denominator_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_result_rejects_boolean_surface_query_and_unbound_source_upgrade(self) -> None:
        result = copy.deepcopy(self.result)
        attempt = result["surface_attempts"][0]
        attempt["query_text"] += " OR coding"
        attempt["query_sha256"] = orchestration.text_sha256(attempt["query_text"])
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_surface_attempt_query_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_result_rejects_metric_lie_and_observation_hash_or_url_rebinding(self) -> None:
        result = copy.deepcopy(self.result)
        result["coverage"]["metric_values"][2]["numerator"] = 2
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_quality_metric_value_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["observations"][0]["text_or_excerpt"] += " tampered"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_observation_content_hash_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["observations"][0]["canonical_url"] = "https://x.com/another_author/status/1001"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_observation_url_topology_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["dimension_results"][0]["source_status"] = "source_bound"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_dimension_source_upgrade_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_positive_results_and_multi_target_all_require_bound_evidence(self) -> None:
        result = copy.deepcopy(self.result)
        row = next(
            item
            for item in result["dimension_results"]
            if item["question_id"] == "verify_coding_work" and item["relevance_state"] == "target_core"
        )
        row["evidence_refs"] = []
        row["source_status"] = "model_mediated_unverified"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_positive_dimension_without_evidence"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        request = copy.deepcopy(self.request)
        question = next(row for row in request["analysis_questions"] if row["question_id"] == "verify_coding_work")
        question["target_label_ids"] = ["coding", "data"]
        question["target_match_operator"] = "all"
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        result = copy.deepcopy(self.result)
        result["request_sha256"] = request["request_sha256"]
        result["plan_sha256"] = plan["plan_sha256"]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_dimension_all_target_incomplete"):
            validate_campaign_result(
                result,
                request=request,
                plan=plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_adaptive_recall_accepts_both_surfaces_without_exhausting_alias_matrix(self) -> None:
        result = copy.deepcopy(self.result)
        _complete_adaptive_recall(result, self.plan)
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )
        planned_query_count = sum(
            len(target["recall_queries"])
            for task in self.plan["candidate_authored_tasks"]
            for target in task["recall_targets"]
            if target["question_id"] == "verify_coding_work"
        )
        self.assertLess(len(result["semantic_recall_attempts"]), planned_query_count)

    def test_surface_pagination_requires_consumed_continuation_and_allows_failed_retry(self) -> None:
        result = copy.deepcopy(self.result)
        first_page = result["surface_attempts"][0]
        first_page.update(
            {
                "result_truncated": True,
                "continuation_state": "continuation_available",
                "continuation_ref": "fixture://cursor/fixture-author/post/page-2",
            }
        )
        subject = next(row for row in result["subject_outcomes"] if row["seed_ref"] == "seed_x_account")
        subject["terminal_state"] = "research_in_progress"
        subject["reason"] = "Post pagination continuation remains unconsumed"
        result["dimension_results"] = [
            row for row in result["dimension_results"] if row["x_account_ref"] != "xacct_fixture_author"
        ]
        result["affiliation_results"] = [
            row for row in result["affiliation_results"] if row["x_account_ref"] != "xacct_fixture_author"
        ]
        result["exploratory_findings"] = [
            row for row in result["exploratory_findings"] if row["x_account_ref"] != "xacct_fixture_author"
        ]
        result["semantic_recall_outcomes"] = [
            row for row in result["semantic_recall_outcomes"] if row["x_account_ref"] != "xacct_fixture_author"
        ]
        result["coverage"]["terminal_subject_outcomes"] = 2
        metrics = {row["metric_id"]: row for row in result["coverage"]["metric_values"]}
        metrics["candidate_authored_both_surface_coverage_rate"]["numerator"] = 1
        metrics["target_direction_core_rate"].update({"numerator": 0, "denominator": 1})
        metrics["target_direction_active_rate"].update({"numerator": 1, "denominator": 1})
        metrics["evidence_backed_temporal_state_rate"].update({"numerator": 2, "denominator": 2})
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )
        complete_lie = copy.deepcopy(result)
        complete_lie["status"] = "complete"
        _rehash(complete_lie, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_status_derivation_invalid"):
            validate_campaign_result(
                complete_lie,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        first_page = result["surface_attempts"][0]
        first_page.update(
            {
                "result_truncated": True,
                "continuation_state": "continuation_available",
                "continuation_ref": "fixture://cursor/fixture-author/post/page-2",
            }
        )
        failed_page = {
            **first_page,
            "ordinal": 2,
            "receipt_ref": "fixture://receipt/fixture_author/post/page-2-failed",
            "source_status": "fixture_synthetic",
            "execution_state": "failed",
            "bound_observation_count": 0,
            "result_truncated": False,
            "continuation_state": "unknown",
            "input_continuation_ref": first_page["continuation_ref"],
            "continuation_ref": None,
        }
        retry_page = {
            **failed_page,
            "ordinal": 3,
            "receipt_ref": "fixture://receipt/fixture_author/post/page-2-retry",
            "execution_state": "no_result",
            "continuation_state": "exhausted",
        }
        result["surface_attempts"].extend([failed_page, retry_page])
        authored_outcome = next(
            row
            for row in result["semantic_recall_outcomes"]
            if row["x_account_ref"] == "xacct_fixture_author" and row["execution_policy"] == "authored_surface_only"
        )
        authored_outcome["receipt_refs"].append(retry_page["receipt_ref"])
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )

        broken = copy.deepcopy(result)
        broken["surface_attempts"][-1]["input_continuation_ref"] = "fixture://cursor/wrong"
        _rehash(broken, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_surface_attempt_pagination_invalid",
        ):
            validate_campaign_result(
                broken,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_negative_adaptive_stop_covers_every_target_label_surface_pair(self) -> None:
        request = copy.deepcopy(self.request)
        question = next(row for row in request["analysis_questions"] if row["question_id"] == "verify_coding_work")
        question["target_label_ids"] = ["coding", "data"]
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        result = copy.deepcopy(self.result)
        result["request_sha256"] = request["request_sha256"]
        result["plan_sha256"] = plan["plan_sha256"]
        _complete_adaptive_recall(result, plan)
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_adaptive_target_label_coverage_invalid",
        ):
            validate_campaign_result(
                result,
                request=request,
                plan=plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_adaptive_frontier_audit_is_content_addressed_and_recomputable(self) -> None:
        result = copy.deepcopy(self.result)
        _complete_adaptive_recall(result, self.plan)
        outcome = next(
            row for row in result["semantic_recall_outcomes"] if row["terminal_state"] == "adaptive_complete"
        )
        decision = outcome["adaptive_stop_decision"]
        decision["scope_frontier_audit"]["question_id"] = "discover_research_topics"
        _rehash(decision["scope_frontier_audit"], "audit_sha256")
        decision["scope_frontier_audit_ref"] = f"sha256:{decision['scope_frontier_audit']['audit_sha256']}"
        _rehash(decision, "decision_sha256")
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_scope_frontier_audit_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_positive_target_match_can_stop_before_the_alias_matrix_is_exhausted(self) -> None:
        result = copy.deepcopy(self.result)
        _complete_adaptive_recall(result, self.plan)
        account_ref = "xacct_fixture_author"
        matching_attempts = [row for row in result["semantic_recall_attempts"] if row["x_account_ref"] == account_ref]
        winner = matching_attempts[0]
        result["semantic_recall_attempts"] = [
            row for row in result["semantic_recall_attempts"] if row["x_account_ref"] != account_ref
        ] + [winner]
        evidence_id = "obs_semantic_target_proven"
        evidence_text = "Fixture author explicitly reports current coding research."
        winner.update(
            {
                "execution_state": "completed",
                "bound_observation_count": 1,
                "new_unique_observation_refs": [evidence_id],
                "new_target_evidence_refs": [evidence_id],
            }
        )
        result["observations"].append(
            {
                "observation_id": evidence_id,
                "x_account_ref": account_ref,
                "surface": winner["surface"],
                "platform_object_id": "3001",
                "canonical_url": "https://x.com/fixture_author/status/3001",
                "author_platform_user_id": None,
                "author_handle": "fixture_author",
                "published_at": "2026-07-17T10:00:00Z",
                "observed_at": "2026-07-18T08:00:00Z",
                "text_or_excerpt": evidence_text,
                "content_sha256": orchestration.text_sha256(evidence_text),
                "source_status": "fixture_synthetic",
                "receipt_ref": winner["receipt_ref"],
            }
        )
        dimension = next(
            row
            for row in result["dimension_results"]
            if row["x_account_ref"] == account_ref and row["question_id"] == "verify_coding_work"
        )
        dimension["evidence_refs"].append(evidence_id)
        outcome = next(
            row
            for row in result["semantic_recall_outcomes"]
            if row["x_account_ref"] == account_ref and row["question_id"] == "verify_coding_work"
        )
        outcome.update(
            {
                "terminal_state": "target_match_proven",
                "attempted_query_sha256s": [winner["query_sha256"]],
                "stop_reason": "configured_target_match_proven",
                "receipt_refs": [winner["receipt_ref"]],
                "adaptive_stop_decision": None,
            }
        )
        result["coverage"]["observation_count"] += 1
        source_bound_metric = next(
            row for row in result["coverage"]["metric_values"] if row["metric_id"] == "source_bound_evidence_rate"
        )
        source_bound_metric["denominator"] += 1
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )

    def test_fixture_and_account_resolution_provenance_cannot_be_laundered(self) -> None:
        result = copy.deepcopy(self.result)
        result["observations"][0]["source_status"] = "model_mediated_unverified"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_observation_source_status_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["handle_resolution_evidence"][0]["query_text"] += " tampered"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_handle_resolution_evidence_hash_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_unseeded_same_name_handle_stays_an_auditable_ambiguous_proposal(self) -> None:
        result = copy.deepcopy(self.result)
        account_ref = "xacct_same_name_candidate"
        result["external_accounts"].append(
            {
                "x_account_ref": account_ref,
                "platform_user_id": None,
                "current_handle": "same_name_ai",
                "profile_url": "https://x.com/same_name_ai",
                "identity_status": "provisional_handle",
                "handle_history_proposals": [],
            }
        )
        query_text = "Resolve seed_name_only against https://x.com/same_name_ai"
        observed_value = "Same Name AI researcher profile"
        receipt = {
            "schema_version": "x.handle_resolution.retrieval_receipt.v1",
            "query_sha256": orchestration.text_sha256(query_text),
            "canonical_url": "https://x.com/same_name_ai",
            "observed_at": "2026-07-18T08:00:00Z",
            "content_sha256": orchestration.text_sha256(observed_value),
            "source_status": "fixture_synthetic",
            "receipt_locator": "fixture://receipt/handle-resolution/same-name-ai",
            "receipt_sha256": "",
        }
        _rehash(receipt, "receipt_sha256")
        evidence_id = "handle_resolution_same_name_candidate"
        result["handle_resolution_evidence"].append(
            {
                "evidence_id": evidence_id,
                "seed_ref": "seed_name_only",
                "x_account_ref": account_ref,
                "evidence_kind": "x_profile_name",
                "canonical_url": "https://x.com/same_name_ai",
                "observed_at": "2026-07-18T08:00:00Z",
                "query_text": query_text,
                "query_sha256": orchestration.text_sha256(query_text),
                "observed_value": observed_value,
                "content_sha256": orchestration.text_sha256(observed_value),
                "source_status": "fixture_synthetic",
                "receipt_ref": f"sha256:{receipt['receipt_sha256']}",
                "retrieval_receipt": receipt,
            }
        )
        result["cross_source_link_proposals"].append(
            {
                "proposal_id": "link_seed_name_only_to_same_name_ai",
                "seed_ref": "seed_name_only",
                "x_account_ref": account_ref,
                "status": "ambiguous",
                "evidence_refs": [evidence_id],
                "source_status": "fixture_synthetic",
                "human_review_required": True,
                "automatic_merge_authorized": False,
            }
        )
        result["coverage"]["resolved_account_count"] += 1
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )

        wrong_host = copy.deepcopy(result)
        evidence = wrong_host["handle_resolution_evidence"][-1]
        evidence["canonical_url"] = "https://example.com/same_name_ai"
        evidence["retrieval_receipt"]["canonical_url"] = evidence["canonical_url"]
        _rehash(evidence["retrieval_receipt"], "receipt_sha256")
        evidence["receipt_ref"] = f"sha256:{evidence['retrieval_receipt']['receipt_sha256']}"
        _rehash(wrong_host, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_handle_resolution_evidence_binding_invalid",
        ):
            validate_campaign_result(
                wrong_host,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["handle_resolution_evidence"] = []
        result["cross_source_link_proposals"][0]["evidence_refs"] = [
            "fixture://external/linkedin/fixture_researcher#x-link"
        ]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_link_proposal_resolution_evidence_missing",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_optional_no_result_cannot_hide_failed_attempt_and_evidence_is_bound(self) -> None:
        result = copy.deepcopy(self.result)
        attempt = result["optional_channel_attempts"][0]
        attempt.update(
            {
                "execution_state": "failed",
                "continuation_state": "unknown",
                "input_continuation_ref": None,
                "continuation_ref": None,
            }
        )
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_optional_no_result_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        attempt = result["optional_channel_attempts"][0]
        evidence_text = "Fixture direct-credit page names one project contributor."
        evidence_id = "optional_evidence_hash_bound"
        result["optional_channel_evidence"] = [
            {
                "evidence_id": evidence_id,
                "task_id": attempt["task_id"],
                "evidence_kind": "project_direct_credit",
                "canonical_url": "https://fixture.invalid/project/credits",
                "observed_at": "2026-07-18T08:00:00Z",
                "text_or_excerpt": evidence_text,
                "content_sha256": "0" * 64,
                "receipt_ref": attempt["receipt_ref"],
                "source_status": "fixture_synthetic",
            }
        ]
        attempt.update(
            {
                "execution_state": "completed",
                "bound_evidence_count": 1,
                "evidence_refs": [evidence_id],
            }
        )
        result["optional_channel_outcomes"][0].update(
            {
                "terminal_state": "completed",
                "evidence_refs": [evidence_id],
            }
        )
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_optional_evidence_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_exploratory_dimension_cannot_claim_target_state(self) -> None:
        result = copy.deepcopy(self.result)
        exploratory = next(
            row for row in result["dimension_results"] if row["question_id"] == "discover_research_topics"
        )
        exploratory["relevance_state"] = "target_adjacent"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_exploratory_dimension_state_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_native_call_cost_denominator_includes_failed_optional_calls(self) -> None:
        result = copy.deepcopy(self.result)
        for attempt in result["surface_attempts"]:
            attempt["source_status"] = "receipt_bound"
        for observation in result["observations"]:
            observation["source_status"] = "source_bound"
        for row in result["dimension_results"] + result["exploratory_findings"]:
            row["source_status"] = "source_bound"
        optional_attempt = result["optional_channel_attempts"][0]
        optional_attempt.update(
            {
                "source_status": "receipt_bound",
                "execution_state": "failed",
                "continuation_state": "unknown",
                "continuation_ref": None,
            }
        )
        result["optional_channel_outcomes"][0].update(
            {
                "terminal_state": "failed",
                "source_status": "receipt_bound",
            }
        )
        result["coverage"]["coverage_source_status"] = "receipt_bound"
        metrics = {row["metric_id"]: row for row in result["coverage"]["metric_values"]}
        metrics["source_bound_evidence_rate"]["numerator"] = 4
        metrics["target_core_unique_account_yield_per_native_call"].update({"numerator": 1, "denominator": 5})
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=self.request,
            plan=self.plan,
            catalog=self.catalog,
            policy=self.policy,
        )

        result["coverage"]["metric_values"][5]["denominator"] = 4
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_quality_metric_value_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_target_direction_rate_denominator_is_question_account_rows(self) -> None:
        request = copy.deepcopy(self.request)
        request["analysis_questions"].append(
            {
                "question_id": "verify_data_work",
                "analysis_mode": "verification",
                "question_text": "Does this account provide evidence of data research?",
                "dimension_id": "research_workstream",
                "target_label_ids": ["data"],
                "target_match_operator": "any",
                "recall_execution_policy": "authored_surface_only",
                "adaptive_stop_policy": None,
            }
        )
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        result = copy.deepcopy(self.result)
        result["request_sha256"] = request["request_sha256"]
        result["plan_sha256"] = plan["plan_sha256"]
        for account in result["external_accounts"]:
            account_ref = account["x_account_ref"]
            result["dimension_results"].append(
                {
                    "x_account_ref": account_ref,
                    "question_id": "verify_data_work",
                    "dimension_id": "research_workstream",
                    "matched_label_ids": [],
                    "relevance_state": "out_of_scope",
                    "target_activity_temporal_state": "unsupported",
                    "matches_temporal_filter": False,
                    "evidence_refs": [],
                    "reason": "fixture carries no evidence for the second verification question",
                    "source_status": "model_mediated_unverified",
                }
            )
            surface_receipts = [
                row["receipt_ref"] for row in result["surface_attempts"] if row["x_account_ref"] == account_ref
            ]
            result["semantic_recall_outcomes"].append(
                {
                    "x_account_ref": account_ref,
                    "question_id": "verify_data_work",
                    "execution_policy": "authored_surface_only",
                    "terminal_state": "authored_surface_complete",
                    "attempted_query_sha256s": [],
                    "stop_reason": "broad_authored_surface_completed",
                    "receipt_refs": surface_receipts,
                    "adaptive_stop_decision": None,
                }
            )
        metrics = {row["metric_id"]: row for row in result["coverage"]["metric_values"]}
        metrics["target_direction_core_rate"].update({"numerator": 1, "denominator": 4})
        metrics["target_direction_active_rate"].update({"numerator": 2, "denominator": 4})
        metrics["evidence_backed_temporal_state_rate"].update({"numerator": 4, "denominator": 6})
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=request,
            plan=plan,
            catalog=self.catalog,
            policy=self.policy,
        )

        metrics["target_direction_core_rate"]["denominator"] = 2
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_quality_metric_value_invalid",
        ):
            validate_campaign_result(
                result,
                request=request,
                plan=plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_identity_and_receipt_bindings_fail_closed(self) -> None:
        result = copy.deepcopy(self.result)
        for account in result["external_accounts"]:
            account["identity_status"] = "stable_platform_id"
            account["platform_user_id"] = "424242"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_platform_user_id_duplicate"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        recall_query = next(
            query
            for task in self.plan["candidate_authored_tasks"]
            if task["handle"] == "fixture_author"
            for target in task["recall_targets"]
            if target["question_id"] == "verify_coding_work"
            for query in target["recall_queries"]
        )
        result["semantic_recall_attempts"] = [
            {
                "x_account_ref": "xacct_fixture_author",
                "question_id": "verify_coding_work",
                "label_id": recall_query["label_id"],
                "ordinal": 1,
                "page_ordinal": 1,
                "surface": recall_query["surface"],
                "query_text": recall_query["query_text"],
                "query_sha256": orchestration.text_sha256(recall_query["query_text"]),
                "receipt_ref": "fixture://receipt/fixture_author/post",
                "source_status": "unverified",
                "execution_state": "no_result",
                "bound_observation_count": 0,
                "result_truncated": False,
                "continuation_state": "exhausted",
                "input_continuation_ref": None,
                "continuation_ref": None,
                "new_unique_observation_refs": [],
                "new_target_evidence_refs": [],
            }
        ]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_receipt_source_status_conflict"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        recall_query = next(
            query
            for task in self.plan["candidate_authored_tasks"]
            if task["handle"] == "fixture_author"
            for target in task["recall_targets"]
            if target["question_id"] == "verify_coding_work"
            for query in target["recall_queries"]
        )
        result["semantic_recall_attempts"] = [
            {
                "x_account_ref": "xacct_fixture_author",
                "question_id": "verify_coding_work",
                "label_id": recall_query["label_id"],
                "ordinal": 1,
                "page_ordinal": 1,
                "surface": recall_query["surface"],
                "query_text": recall_query["query_text"],
                "query_sha256": orchestration.text_sha256(recall_query["query_text"]),
                "receipt_ref": "fixture://receipt/fixture_author/post",
                "source_status": "fixture_synthetic",
                "execution_state": "no_result",
                "bound_observation_count": 0,
                "result_truncated": False,
                "continuation_state": "exhausted",
                "input_continuation_ref": None,
                "continuation_ref": None,
                "new_unique_observation_refs": [],
                "new_target_evidence_refs": [],
            }
        ]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_receipt_reused_across_attempts"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_account_evidence_and_cross_source_link_ownership_fail_closed(self) -> None:
        result = copy.deepcopy(self.result)
        row = next(
            item
            for item in result["dimension_results"]
            if item["x_account_ref"] == "xacct_fixture_author" and item["question_id"] == "verify_coding_work"
        )
        row["evidence_refs"] = ["obs_linked_post"]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_dimension_evidence_owner_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["affiliation_results"][0]["evidence_refs"] = ["seed_linkedin_profile"]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_affiliation_binding_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["cross_source_link_proposals"][0]["evidence_refs"] = ["fixture://not-in-seed"]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_link_proposal_evidence_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["cross_source_link_proposals"][0]["status"] = "rejected"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_link_proposal_coverage_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_discovered_accounts_require_candidate_authored_followup_before_complete(self) -> None:
        request = copy.deepcopy(self.request)
        request["seed_inputs"] = [row for row in request["seed_inputs"] if row["seed_ref"] != "seed_name_only"]
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        result = copy.deepcopy(self.result)
        result["request_sha256"] = request["request_sha256"]
        result["plan_sha256"] = plan["plan_sha256"]
        result["subject_outcomes"] = [row for row in result["subject_outcomes"] if row["seed_ref"] != "seed_name_only"]
        result["coverage"]["subjects_total"] = 2
        result["coverage"]["terminal_subject_outcomes"] = 2
        cross_source_metric = next(
            row
            for row in result["coverage"]["metric_values"]
            if row["metric_id"] == "cross_source_handle_resolution_rate"
        )
        cross_source_metric["denominator"] = 1
        _complete_adaptive_recall(result, plan)
        result["status"] = "complete"
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=request,
            plan=plan,
            catalog=self.catalog,
            policy=self.policy,
        )

        result["external_accounts"].append(
            {
                "x_account_ref": "xacct_discovered_pending",
                "platform_user_id": None,
                "current_handle": "pending_lead",
                "profile_url": "https://x.com/pending_lead",
                "identity_status": "provisional_handle",
                "handle_history_proposals": [],
            }
        )
        optional_evidence_id = "optional_evidence_direct_credit_lead"
        optional_evidence_text = "Fixture project credits @pending_lead as a contributor."
        result["optional_channel_evidence"] = [
            {
                "evidence_id": optional_evidence_id,
                "task_id": plan["optional_channel_tasks"][0]["task_id"],
                "evidence_kind": "project_direct_credit",
                "canonical_url": "https://fixture.invalid/project/direct-credit/lead",
                "observed_at": "2026-07-18T08:00:00Z",
                "text_or_excerpt": optional_evidence_text,
                "content_sha256": orchestration.text_sha256(optional_evidence_text),
                "receipt_ref": "fixture://receipt/optional/direct-credit/1",
                "source_status": "fixture_synthetic",
            }
        ]
        result["discovery_origins"] = [
            {
                "x_account_ref": "xacct_discovered_pending",
                "origin_task_id": plan["optional_channel_tasks"][0]["task_id"],
                "channel_id": "project_direct_credit",
                "evidence_refs": [optional_evidence_id],
                "source_status": "fixture_synthetic",
                "requires_candidate_authored_followup": True,
            }
        ]
        result["optional_channel_outcomes"][0].update(
            {
                "terminal_state": "completed",
                "evidence_refs": [optional_evidence_id],
                "reason": "synthetic direct-credit discovery",
            }
        )
        result["optional_channel_attempts"][0]["evidence_refs"] = [optional_evidence_id]
        result["optional_channel_attempts"][0].update(
            {
                "execution_state": "completed",
                "bound_evidence_count": 1,
                "continuation_state": "exhausted",
            }
        )
        result["coverage"]["resolved_account_count"] = 3
        result["status"] = "partial"
        _rehash(result, "result_sha256")
        validate_campaign_result(
            result,
            request=request,
            plan=plan,
            catalog=self.catalog,
            policy=self.policy,
        )

    def test_optional_attempt_bound_count_is_recomputed(self) -> None:
        result = copy.deepcopy(self.result)
        result["optional_channel_attempts"][0].update(
            {
                "execution_state": "completed",
                "bound_evidence_count": 1,
                "continuation_state": "unknown",
            }
        )
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_optional_channel_attempt_bound_count_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_result_rejects_experience_rows_when_query_did_not_request_them(self) -> None:
        result = copy.deepcopy(self.result)
        result["experience_verification_queue"] = [
            {
                "x_account_ref": "xacct_fixture_author",
                "dimension_id": "china_asia_professional_educational_experience",
                "status": "weak_proxy",
                "evidence_refs": ["obs_fixture_post"],
                "reason": "synthetic only",
                "source_status": "fixture_synthetic",
                "identity_inference_performed": False,
            }
        ]
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_experience_queue_not_requested"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_explicit_experience_queue_is_terminal_total_for_analyzed_accounts(self) -> None:
        request = copy.deepcopy(self.request)
        request["experience_verification"] = {
            "enabled": True,
            "trigger": "explicit_query_request",
            "dimension_ids": ["china_asia_professional_educational_experience"],
        }
        _rehash(request, "request_sha256")
        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        result = copy.deepcopy(self.result)
        result["request_sha256"] = request["request_sha256"]
        result["plan_sha256"] = plan["plan_sha256"]
        result["experience_verification_queue"] = []
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_experience_queue_terminal_total_invalid",
        ):
            validate_campaign_result(
                result,
                request=request,
                plan=plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_result_status_optional_outcomes_and_source_trust_fail_closed(self) -> None:
        result = copy.deepcopy(self.result)
        result["status"] = "complete"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_status_derivation_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["optional_channel_outcomes"] = []
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_result_optional_channel_terminal_total_invalid",
        ):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

        result = copy.deepcopy(self.result)
        result["affiliation_results"][0]["source_status"] = "source_bound"
        _rehash(result, "result_sha256")
        with self.assertRaisesRegex(ResearchOrchestrationError, "portable_result_affiliation_source_upgrade_invalid"):
            validate_campaign_result(
                result,
                request=self.request,
                plan=self.plan,
                catalog=self.catalog,
                policy=self.policy,
            )

    def test_conversation_graph_requires_an_enabled_anchor_channel(self) -> None:
        request = copy.deepcopy(self.request)
        request["channel_overrides"] = [
            {"channel_id": "project_direct_credit", "enabled": False},
            {"channel_id": "official_source", "enabled": False},
            {"channel_id": "conversation_graph", "enabled": True},
        ]
        _rehash(request, "request_sha256")
        with self.assertRaisesRegex(
            ResearchOrchestrationError,
            "portable_campaign_conversation_graph_anchor_missing",
        ):
            build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)

    def test_portable_module_has_no_sourcing_agent_runtime_import(self) -> None:
        source = inspect.getsource(orchestration)
        self.assertNotIn("import sourcing_agent", source)
        self.assertNotIn("from sourcing_agent", source)
        self.assertFalse((ROOT / "src" / "x_first" / "sourcing_agent").exists())
        self.assertTrue((ROOT / "contracts" / "research_orchestration_contract_registry.v1.json").is_file())

    def test_checked_assets_are_inside_standalone_sibling(self) -> None:
        registry = strict_load_json(ROOT / "contracts" / "research_orchestration_contract_registry.v1.json")
        for path in registry["checked_assets"]:
            resolved = (ROOT / path).resolve()
            self.assertTrue(resolved.is_relative_to(ROOT.resolve()))
            self.assertTrue(resolved.is_file())


if __name__ == "__main__":
    unittest.main()
