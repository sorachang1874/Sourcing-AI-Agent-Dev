import unittest

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.publication_planning import compile_publication_coverage_plan


class PlanningModulesTest(unittest.TestCase):
    def test_scoped_strategy_for_large_company_query(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Gemini Team 的 Pre-train 方向的 Researcher 和 Engineer",
            query="Gemini pre-train researcher engineer",
            target_company="Google",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertIn("Google DeepMind", strategy.company_scope)
        self.assertIn("Gemini", strategy.company_scope)
        self.assertIn("general_web_search_relation_check", strategy.search_channel_order)
        self.assertIn("Researcher", " ".join(strategy.filter_hints.get("job_titles", [])))
        self.assertEqual(strategy.cost_policy.get("provider_people_search_mode"), "fallback_only")
        self.assertTrue(strategy.cost_policy.get("collect_email"))

    def test_former_employee_strategy_prefers_past_company_recall(self) -> None:
        request = JobRequest(
            raw_user_request="找 xAI 已离职的 RL researcher",
            query="xAI former RL researcher",
            target_company="xAI",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["former_employee"], ["former"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "former_employee_search")
        self.assertEqual(strategy.filter_hints.get("past_companies"), ["xAI"])
        self.assertNotIn("exclude_current_companies", strategy.filter_hints)
        self.assertFalse(strategy.cost_policy.get("allow_company_employee_api"))
        self.assertEqual(strategy.cost_policy.get("provider_people_search_min_expected_results"), 50)

    def test_publication_coverage_includes_engineering_and_blog(self) -> None:
        request = JobRequest(
            raw_user_request="找 Anthropic 工程和研究方向的技术成员",
            query="Anthropic engineering research contributors",
            target_company="Anthropic",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)
        coverage = compile_publication_coverage_plan(request, strategy)

        families = [item.family for item in coverage.source_families]
        self.assertIn("official_research", families)
        self.assertIn("official_engineering", families)
        self.assertIn("official_blog_and_docs", families)
        self.assertIn("Use LLM extraction for acknowledgement, contributor, and weakly structured bylines.", coverage.extraction_strategy)

    def test_sourcing_plan_contains_search_strategy_and_filter_layers(self) -> None:
        request = JobRequest(
            raw_user_request="在 YouTube 和 Podcast 上检索所有 Gemini Team 的访谈内容，找到 Post-train 方向 researcher",
            query="Gemini post-train interview researcher",
            target_company="Google",
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        layer_ids = [item.get("layer_id") for item in plan.retrieval_plan.filter_layers]
        bundle_ids = [item.bundle_id for item in plan.search_strategy.query_bundles]
        self.assertIn("semantic_vector_rerank", layer_ids)
        self.assertIn("manual_review_queue", layer_ids)
        self.assertIn("public_interviews", bundle_ids)

    def test_sourcing_plan_surfaces_must_have_facets(self) -> None:
        request = JobRequest(
            raw_user_request="找 TML 的 multimodal 成员",
            query="multimodal",
            target_company="Thinking Machines Lab",
            must_have_facets=["multimodal"],
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        self.assertIn("must_have_facets=['multimodal']", plan.criteria_summary)
        self.assertIn("must_have_facets=['multimodal']", plan.retrieval_plan.structured_filters)
        filter_layer = next(item for item in plan.retrieval_plan.filter_layers if item.get("layer_id") == "must_exclude_filters")
        self.assertEqual(filter_layer["criteria"]["must_have_facets"], ["multimodal"])
        self.assertIn("derived_facets", plan.retrieval_plan.semantic_fields)

    def test_sourcing_plan_surfaces_primary_role_bucket_filters(self) -> None:
        request = JobRequest(
            raw_user_request="找 TML 的 infra 系统主力成员",
            query="infra systems",
            target_company="Thinking Machines Lab",
            must_have_primary_role_buckets=["infra_systems"],
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        self.assertIn(
            "must_have_primary_role_buckets=['infra_systems']",
            plan.criteria_summary,
        )
        self.assertIn(
            "must_have_primary_role_buckets=['infra_systems']",
            plan.retrieval_plan.structured_filters,
        )
        population_layer = next(item for item in plan.retrieval_plan.filter_layers if item.get("layer_id") == "population_scope")
        self.assertEqual(
            population_layer["criteria"]["must_have_primary_role_buckets"],
            ["infra_systems"],
        )
        self.assertNotIn("notes", plan.retrieval_plan.semantic_fields)

    def test_sourcing_plan_contains_structured_intent_brief(self) -> None:
        request = JobRequest(
            raw_user_request="我想了解 Humans& 的 Coding 方向的 Researcher。",
            target_company="Humans&",
            categories=["researcher"],
            employment_statuses=["current"],
            keywords=["coding", "code generation", "developer tools"],
            top_k=8,
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        self.assertIn("目标组织：Humans&", plan.intent_brief.identified_request)
        self.assertIn("目标人群：researcher", plan.intent_brief.identified_request)
        self.assertTrue(
            any("Coding" in item or "coding" in item for item in plan.intent_brief.identified_request),
        )
        self.assertTrue(
            any("优先返回高置信当前成员" in item for item in plan.intent_brief.target_output),
        )
        self.assertTrue(
            any("company identity resolve" in item for item in plan.intent_brief.default_execution_strategy),
        )

    def test_sourcing_plan_rewrites_natural_language_shorthand_into_auditable_filters(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 的华人成员",
                "target_company": "Anthropic",
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        self.assertEqual(
            request.keywords,
            ["Greater China experience", "Chinese bilingual outreach"],
        )
        self.assertTrue(
            any("自然语言简称改写" in item for item in plan.intent_brief.identified_request),
        )
        self.assertIn(
            "这类简称默认按公开的地区 / 语言 / 学习工作经历口径理解，而不是身份标签判断。",
            plan.intent_brief.target_output,
        )

    def test_full_company_preferences_surface_in_plan_without_extra_review(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要 Humans& 公司全量成员，重新跑，不要高成本。",
                "target_company": "Humans&",
                "planning_mode": "heuristic",
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        self.assertEqual(request.execution_preferences["acquisition_strategy_override"], "full_company_roster")
        self.assertTrue(request.execution_preferences["force_fresh_run"])
        self.assertTrue(request.execution_preferences["use_company_employees_lane"])
        self.assertFalse(request.execution_preferences["allow_high_cost_sources"])
        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertTrue(plan.acquisition_strategy.cost_policy["allow_company_employee_api"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["allow_cached_roster_fallback"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["allow_historical_profile_inheritance"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["allow_shared_provider_cache"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["high_cost_requires_approval"])
        self.assertEqual(plan.open_questions, [])
        self.assertTrue(
            any("company-employees lane" in item for item in plan.intent_brief.default_execution_strategy),
        )
        self.assertTrue(
            any("fresh run" in item for item in plan.intent_brief.default_execution_strategy),
        )

    def test_incremental_roster_reuse_preferences_are_inferred_from_text(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "基于现有 roster 只做增量，补 former，不要重抓 current roster。",
                "target_company": "Anthropic",
            }
        )

        self.assertTrue(request.execution_preferences["reuse_existing_roster"])
        self.assertTrue(request.execution_preferences["run_former_search_seed"])

    def test_incremental_roster_reuse_preferences_support_more_colloquial_text(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "基于之前抓过的 roster 继续做增量，只补 former 和新的方法，不重新拉公司全量。",
                "target_company": "Anthropic",
            }
        )

        self.assertTrue(request.execution_preferences["reuse_existing_roster"])
        self.assertTrue(request.execution_preferences["run_former_search_seed"])

    def test_full_company_roster_plan_uses_large_org_budget_and_default_former_seed(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找出 Anthropic 的所有成员，先全量获取 roster。",
                "target_company": "Anthropic",
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(acquire_task.metadata["max_pages"], 100)
        self.assertEqual(acquire_task.metadata["page_limit"], 25)
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], "adaptive_us_function_partition")
        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        self.assertEqual(
            acquire_task.metadata["company_employee_shard_policy"]["root_filters"],
            {"locations": ["United States"]},
        )
        self.assertEqual(
            acquire_task.metadata["company_employee_shard_policy"]["partition_rules"][0]["include_patch"]["function_ids"],
            ["8"],
        )
        self.assertTrue(acquire_task.metadata["include_former_search_seed"])
