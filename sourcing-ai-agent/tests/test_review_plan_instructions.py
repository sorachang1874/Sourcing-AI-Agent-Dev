import unittest

from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.review_plan_instructions import (
    build_review_payload_from_instruction,
    compile_review_payload_from_instruction,
    normalize_review_decision,
    parse_review_instruction,
)


class ReviewPlanInstructionsTest(unittest.TestCase):
    class _ModelReviewClient(DeterministicModelClient):
        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def provider_name(self) -> str:
            return "test-model"

        def normalize_review_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
            return dict(self.payload)

    class _FailingModelReviewClient(DeterministicModelClient):
        def provider_name(self) -> str:
            return "failing-model"

        def normalize_review_instruction(self, payload: dict[str, object]) -> dict[str, object]:  # noqa: ARG002
            raise RuntimeError("provider unavailable")

    def test_parse_instruction_for_full_roster_company_employees_and_fresh_run(self) -> None:
        decision = parse_review_instruction(
            "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。",
            target_company="Humans&",
        )

        self.assertEqual(decision["acquisition_strategy_override"], "full_company_roster")
        self.assertTrue(decision["use_company_employees_lane"])
        self.assertTrue(decision["force_fresh_run"])
        self.assertFalse(decision["allow_high_cost_sources"])

    def test_parse_instruction_extracts_source_families_scope_and_bias(self) -> None:
        decision = parse_review_instruction(
            "范围只看 Thinking Machines Lab，增加 Engineering Blog 和 OpenReview，策略保持 balanced。",
            target_company="Thinking Machines Lab",
        )

        self.assertEqual(decision["confirmed_company_scope"], ["Thinking Machines Lab"])
        self.assertEqual(decision["extra_source_families"], ["OpenReview", "Engineering Blog"])
        self.assertEqual(decision["precision_recall_bias"], "balanced")

    def test_parse_instruction_supports_incremental_reuse_and_former_seed(self) -> None:
        decision = parse_review_instruction(
            "沿用现有 roster，只补 former 增量，不要重抓 current roster。",
            target_company="Anthropic",
        )

        self.assertTrue(decision["reuse_existing_roster"])
        self.assertTrue(decision["run_former_search_seed"])

    def test_parse_instruction_supports_more_colloquial_incremental_reuse_and_former_seed(self) -> None:
        decision = parse_review_instruction(
            "基于之前抓过的 roster 继续做增量，只补 former 和新的方法，不重新拉公司全量。",
            target_company="Anthropic",
        )

        self.assertTrue(decision["reuse_existing_roster"])
        self.assertTrue(decision["run_former_search_seed"])

    def test_parse_instruction_supports_keyword_first_without_company_employees(self) -> None:
        decision = parse_review_instruction(
            "只用 search API 做 keyword-first acquisition，不要 company-employees，former 也要，多 query 并集。",
            target_company="Google",
        )

        self.assertEqual(decision["acquisition_strategy_override"], "scoped_search_roster")
        self.assertTrue(decision["keyword_priority_only"])
        self.assertFalse(decision["use_company_employees_lane"])
        self.assertTrue(decision["run_former_search_seed"])
        self.assertEqual(decision["provider_people_search_query_strategy"], "all_queries_union")
        self.assertNotIn("reuse_existing_roster", decision)

    def test_parse_instruction_supports_explicit_scoped_search_roster(self) -> None:
        decision = parse_review_instruction(
            "直接指定 scoped search roster，并明确不要 company-employees。",
            target_company="Google",
        )

        self.assertEqual(decision["acquisition_strategy_override"], "scoped_search_roster")
        self.assertFalse(decision["use_company_employees_lane"])

    def test_build_review_payload_uses_instruction_as_default_notes(self) -> None:
        payload = build_review_payload_from_instruction(
            review_id=42,
            instruction="改成 full company roster，不允许高成本。",
            reviewer="tester",
            target_company="Humans&",
        )

        self.assertEqual(payload["review_id"], 42)
        self.assertEqual(payload["action"], "approved")
        self.assertEqual(payload["reviewer"], "tester")
        self.assertEqual(payload["notes"], "改成 full company roster，不允许高成本。")
        self.assertEqual(payload["decision"]["acquisition_strategy_override"], "full_company_roster")
        self.assertFalse(payload["decision"]["allow_high_cost_sources"])

    def test_normalize_review_decision_enforces_whitelist_and_schema(self) -> None:
        decision = normalize_review_decision(
            {
                "decision": {
                    "company_scope": ["Thinking Machines Lab"],
                    "allow_high_cost_sources": "false",
                    "precision_recall_bias": "balanced",
                    "use_company_employees_lane": "yes",
                    "unknown_field": "ignored",
                }
            },
            target_company="Thinking Machines Lab",
            allowed_fields={"company_scope", "allow_high_cost_sources", "precision_recall_bias", "use_company_employees_lane"},
        )

        self.assertEqual(
            decision,
            {
                "confirmed_company_scope": ["Thinking Machines Lab"],
                "allow_high_cost_sources": False,
                "precision_recall_bias": "balanced",
                "use_company_employees_lane": True,
            },
        )

    def test_normalize_review_decision_accepts_keyword_first_lane_controls(self) -> None:
        decision = normalize_review_decision(
            {
                "decision": {
                    "keyword_priority_only": "true",
                    "use_company_employees_lane": "false",
                    "provider_people_search_query_strategy": "union",
                    "provider_people_search_max_queries": "6",
                    "large_org_keyword_probe_mode": "yes",
                }
            },
            target_company="Google",
            allowed_fields={
                "keyword_priority_only",
                "use_company_employees_lane",
                "provider_people_search_query_strategy",
                "provider_people_search_max_queries",
                "large_org_keyword_probe_mode",
            },
        )

        self.assertEqual(
            decision,
            {
                "keyword_priority_only": True,
                "use_company_employees_lane": False,
                "provider_people_search_query_strategy": "all_queries_union",
                "provider_people_search_max_queries": 6,
                "large_org_keyword_probe_mode": True,
            },
        )

    def test_compile_review_payload_prefers_model_and_supplements_missing_fields(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=7,
            instruction="改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。",
            reviewer="tester",
            target_company="Humans&",
            model_client=self._ModelReviewClient(
                {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "allow_high_cost_sources": False,
                    }
                }
            ),
            gate_payload={"editable_fields": ["company_scope", "allow_high_cost_sources", "acquisition_strategy_override", "use_company_employees_lane", "force_fresh_run", "reuse_existing_roster", "run_former_search_seed"]},
        )

        payload = compiled["review_payload"]
        compiler = compiled["instruction_compiler"]
        self.assertEqual(payload["decision"]["acquisition_strategy_override"], "full_company_roster")
        self.assertFalse(payload["decision"]["allow_high_cost_sources"])
        self.assertTrue(payload["decision"]["use_company_employees_lane"])
        self.assertTrue(payload["decision"]["force_fresh_run"])
        self.assertEqual(compiler["source"], "model")
        self.assertTrue(compiler["fallback_used"])
        self.assertEqual(
            sorted(compiler["supplemented_keys"]),
            ["confirmed_company_scope", "force_fresh_run", "use_company_employees_lane"],
        )

    def test_compile_review_payload_falls_back_when_model_output_is_invalid(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=8,
            instruction="改成 full company roster，不允许高成本。",
            reviewer="tester",
            target_company="Humans&",
            model_client=self._ModelReviewClient(
                {
                    "decision": {
                        "unsupported_field": "x",
                    }
                }
            ),
            gate_payload={"editable_fields": ["allow_high_cost_sources", "acquisition_strategy_override"]},
        )

        payload = compiled["review_payload"]
        compiler = compiled["instruction_compiler"]
        self.assertEqual(payload["decision"]["acquisition_strategy_override"], "full_company_roster")
        self.assertFalse(payload["decision"]["allow_high_cost_sources"])
        self.assertEqual(compiler["source"], "deterministic")
        self.assertTrue(compiler["fallback_used"])

    def test_compile_review_payload_inferrs_company_employees_lane_from_high_level_intent(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=9,
            instruction="我要求 scope 大一些，要公司全量成员，要重新跑。",
            reviewer="tester",
            target_company="Humans&",
            model_client=self._ModelReviewClient(
                {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "confirmed_company_scope": ["Humans&"],
                        "force_fresh_run": True,
                    }
                }
            ),
            gate_payload={"editable_fields": ["company_scope", "acquisition_strategy_override", "use_company_employees_lane", "force_fresh_run", "reuse_existing_roster", "run_former_search_seed"]},
            request_payload={"employment_statuses": ["current"], "categories": ["researcher"]},
            plan_payload={"acquisition_strategy": {"strategy_type": "scoped_search_roster"}},
        )

        payload = compiled["review_payload"]
        compiler = compiled["instruction_compiler"]
        self.assertEqual(payload["decision"]["acquisition_strategy_override"], "full_company_roster")
        self.assertEqual(payload["decision"]["confirmed_company_scope"], ["Humans&"])
        self.assertTrue(payload["decision"]["force_fresh_run"])
        self.assertTrue(payload["decision"]["use_company_employees_lane"])
        self.assertEqual(compiler["policy_inferred_keys"], ["use_company_employees_lane"])

    def test_compile_review_payload_inferrs_company_employees_lane_from_intent_axes_only_request(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=10,
            instruction="我要求 scope 大一些，要公司全量成员，要重新跑。",
            reviewer="tester",
            target_company="Humans&",
            model_client=self._ModelReviewClient(
                {
                    "decision": {
                        "acquisition_strategy_override": "full_company_roster",
                        "confirmed_company_scope": ["Humans&"],
                        "force_fresh_run": True,
                    }
                }
            ),
            gate_payload={"editable_fields": ["company_scope", "acquisition_strategy_override", "use_company_employees_lane", "force_fresh_run"]},
            request_payload={
                "raw_user_request": "给我 Humans& 全量成员",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Humans&",
                    },
                },
            },
            plan_payload={"acquisition_strategy": {"strategy_type": "scoped_search_roster"}},
        )

        payload = compiled["review_payload"]
        compiler = compiled["instruction_compiler"]
        self.assertTrue(payload["decision"]["use_company_employees_lane"])
        self.assertEqual(compiler["policy_inferred_keys"], ["use_company_employees_lane"])

    def test_compile_review_payload_preserves_incremental_reuse_decision(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=12,
            instruction="沿用现有 roster，只补 former 增量，不要重抓 current roster。",
            reviewer="tester",
            target_company="Anthropic",
            gate_payload={"editable_fields": ["reuse_existing_roster", "run_former_search_seed", "acquisition_strategy_override"]},
        )

        decision = compiled["review_payload"]["decision"]
        self.assertTrue(decision["reuse_existing_roster"])
        self.assertTrue(decision["run_former_search_seed"])

    def test_compile_review_payload_preserves_keyword_first_axes(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=13,
            instruction="只用 search API 做 keyword-first acquisition，不要 company-employees，former 也要，多 query 并集。",
            reviewer="tester",
            target_company="Google",
            gate_payload={
                "editable_fields": [
                    "acquisition_strategy_override",
                    "keyword_priority_only",
                    "use_company_employees_lane",
                    "run_former_search_seed",
                    "provider_people_search_query_strategy",
                ]
            },
        )

        decision = compiled["review_payload"]["decision"]
        self.assertEqual(decision["acquisition_strategy_override"], "scoped_search_roster")
        self.assertTrue(decision["keyword_priority_only"])
        self.assertFalse(decision["use_company_employees_lane"])
        self.assertTrue(decision["run_former_search_seed"])
        self.assertEqual(decision["provider_people_search_query_strategy"], "all_queries_union")

    def test_compile_review_payload_falls_back_when_model_raises(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=10,
            instruction="改成 full company roster，不允许高成本。",
            reviewer="tester",
            target_company="Humans&",
            model_client=self._FailingModelReviewClient(),
            gate_payload={"editable_fields": ["allow_high_cost_sources", "acquisition_strategy_override"]},
        )

        self.assertEqual(compiled["review_payload"]["decision"]["acquisition_strategy_override"], "full_company_roster")
        self.assertFalse(compiled["review_payload"]["decision"]["allow_high_cost_sources"])
        self.assertEqual(compiled["instruction_compiler"]["source"], "deterministic")

    def test_compile_review_payload_surfaces_request_and_instruction_intent_rewrite(self) -> None:
        compiled = compile_review_payload_from_instruction(
            review_id=11,
            instruction="我要求 scope 大一些，并且后续优先看更偏华人的成员。",
            reviewer="tester",
            target_company="Anthropic",
            gate_payload={"editable_fields": ["company_scope", "acquisition_strategy_override", "force_fresh_run"]},
            request_payload={
                "raw_user_request": "帮我找 Anthropic 的华人成员",
                "target_company": "Anthropic",
            },
        )

        compiler = compiled["instruction_compiler"]
        self.assertEqual(compiler["request_intent_rewrite"]["rewrite_id"], "greater_china_outreach")
        self.assertEqual(compiler["instruction_intent_rewrite"]["rewrite_id"], "greater_china_outreach")
        self.assertIn(
            "中国大陆 / 港澳台 / 新加坡公开学习或工作经历",
            compiler["request_intent_rewrite"]["targeting_terms"],
        )


if __name__ == "__main__":
    unittest.main()
