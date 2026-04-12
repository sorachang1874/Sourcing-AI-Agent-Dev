import unittest

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.domain import JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.plan_review import apply_plan_review_decision, build_plan_review_gate
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.publication_planning import compile_publication_coverage_plan
from sourcing_agent.request_normalization import resolve_request_intent_view
from sourcing_agent.search_planning import compile_search_strategy


class PlanningModulesTest(unittest.TestCase):
    def test_resolve_request_intent_view_merges_axes_into_effective_execution_semantics(self) -> None:
        intent_view = resolve_request_intent_view(
            {
                "raw_user_request": "我想找Gemini的产品经理",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                        "confirmed_company_scope": ["Google", "Google DeepMind"],
                    },
                    "acquisition_lane_policy": {
                        "keyword_priority_only": True,
                    },
                    "fallback_policy": {
                        "provider_people_search_query_strategy": "all_queries_union",
                        "run_former_search_seed": True,
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        self.assertEqual(intent_view["target_company"], "Google")
        self.assertEqual(intent_view["categories"], ["employee"])
        self.assertEqual(intent_view["employment_statuses"], ["current", "former"])
        self.assertEqual(intent_view["organization_keywords"], ["Google DeepMind", "Gemini"])
        self.assertEqual(intent_view["must_have_primary_role_buckets"], ["product_management"])
        self.assertTrue(intent_view["execution_preferences"]["keyword_priority_only"])
        self.assertTrue(intent_view["execution_preferences"]["run_former_search_seed"])
        self.assertEqual(
            intent_view["execution_preferences"]["provider_people_search_query_strategy"],
            "all_queries_union",
        )
        self.assertEqual(
            intent_view["execution_preferences"]["confirmed_company_scope"],
            ["Google", "Google DeepMind"],
        )

    def test_job_request_materializes_intent_axes_only_payload(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想找Gemini的产品经理",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "acquisition_lane_policy": {
                        "keyword_priority_only": True,
                    },
                    "fallback_policy": {
                        "provider_people_search_query_strategy": "all_queries_union",
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        self.assertEqual(request.target_company, "Google")
        self.assertEqual(request.employment_statuses, ["current", "former"])
        self.assertEqual(request.must_have_primary_role_buckets, ["product_management"])
        self.assertEqual(request.organization_keywords, ["Google DeepMind", "Gemini"])
        self.assertTrue(request.execution_preferences["keyword_priority_only"])
        self.assertEqual(
            request.execution_preferences["provider_people_search_query_strategy"],
            "all_queries_union",
        )

    def test_plan_review_decision_backfills_missing_target_company_from_confirmed_scope(self) -> None:
        request_payload = {
            "raw_user_request": "帮我寻找LangChain Infra方向的人",
            "target_company": "",
            "execution_preferences": {},
        }
        plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "full_company_roster",
                "company_scope": [],
                "filter_hints": {},
                "cost_policy": {},
            },
            "acquisition_tasks": [],
        }

        updated_request, updated_plan = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {"confirmed_company_scope": ["LangChain"]},
        )

        self.assertEqual(updated_request["target_company"], "LangChain")
        self.assertEqual(updated_plan["acquisition_strategy"]["company_scope"][0], "LangChain")
        self.assertEqual(updated_plan["acquisition_strategy"]["filter_hints"]["current_companies"][0], "LangChain")

    def test_plan_review_decision_backfills_missing_target_company_from_scope_disambiguation(self) -> None:
        request_payload = {
            "raw_user_request": "帮我寻找LangChain Infra方向的人",
            "target_company": "",
            "execution_preferences": {},
        }
        plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "full_company_roster",
                "company_scope": [],
                "filter_hints": {},
                "cost_policy": {},
            },
            "acquisition_tasks": [],
        }

        updated_request, updated_plan = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {"scope_disambiguation": {"target_company": "LangChain"}},
        )

        self.assertEqual(updated_request["target_company"], "LangChain")
        self.assertEqual(updated_plan["acquisition_strategy"]["company_scope"][0], "LangChain")
        self.assertEqual(updated_plan["acquisition_strategy"]["filter_hints"]["current_companies"][0], "LangChain")

    def test_plan_review_decision_applies_keyword_first_lane_preferences(self) -> None:
        request_payload = {
            "raw_user_request": "给我 Google 做多模态的人",
            "target_company": "Google",
            "execution_preferences": {},
        }
        plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "full_company_roster",
                "company_scope": ["Google"],
                "filter_hints": {},
                "cost_policy": {
                    "allow_company_employee_api": True,
                    "provider_people_search_query_strategy": "first_hit",
                },
                "reasoning": [],
            },
            "acquisition_tasks": [],
        }

        updated_request, updated_plan = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {
                "keyword_priority_only": True,
                "use_company_employees_lane": False,
                "run_former_search_seed": True,
                "provider_people_search_query_strategy": "all_queries_union",
                "provider_people_search_max_queries": 6,
            },
        )

        prefs = updated_request["execution_preferences"]
        cost_policy = updated_plan["acquisition_strategy"]["cost_policy"]
        self.assertTrue(prefs["keyword_priority_only"])
        self.assertFalse(prefs["use_company_employees_lane"])
        self.assertTrue(prefs["run_former_search_seed"])
        self.assertEqual(prefs["provider_people_search_query_strategy"], "all_queries_union")
        self.assertEqual(cost_policy["keyword_priority_only"], True)
        self.assertEqual(cost_policy["allow_company_employee_api"], False)
        self.assertEqual(cost_policy["provider_people_search_query_strategy"], "all_queries_union")
        self.assertEqual(cost_policy["provider_people_search_max_queries"], 6)

    def test_acquisition_keyword_hints_exclude_outreach_only_terms(self) -> None:
        request = JobRequest(
            raw_user_request="帮我找 Anthropic 的华人成员，偏多模态研究",
            query="Anthropic multimodal Veo Nano Banana",
            target_company="Anthropic",
            keywords=[
                "Greater China experience",
                "Chinese bilingual outreach",
                "multimodal",
                "Veo",
            ],
            must_have_keywords=["Nano Banana"],
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)

        keyword_hints = list(strategy.filter_hints.get("keywords") or [])
        self.assertIn("multimodal", keyword_hints)
        self.assertIn("Veo", keyword_hints)
        self.assertIn("Nano Banana", keyword_hints)
        self.assertNotIn("Greater China experience", keyword_hints)
        self.assertNotIn("Chinese bilingual outreach", keyword_hints)

    def test_google_suborg_signal_prefers_full_roster_strategy(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Gemini Team 的 Pre-train 方向的 Researcher 和 Engineer",
            query="Gemini pre-train researcher engineer",
            target_company="Google",
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "full_company_roster")
        self.assertIn("Google DeepMind", strategy.company_scope)
        self.assertIn("general_web_search_relation_check", strategy.search_channel_order)
        self.assertEqual(strategy.filter_hints.get("function_ids"), ["8", "9", "19", "24"])
        self.assertNotIn("job_titles", strategy.filter_hints)
        self.assertTrue(any("Gemini" in query for query in strategy.search_seed_queries))
        self.assertTrue(strategy.cost_policy.get("allow_company_employee_api"))
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

    def test_acquisition_strategy_prefers_intent_view_over_conflicting_flat_fields(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找产品经理",
                "query": "product manager",
                "target_company": "WrongCo",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "full_company_roster")
        self.assertEqual(
            strategy.filter_hints.get("current_companies"),
            [
                "https://www.linkedin.com/company/google/",
                "https://www.linkedin.com/company/deepmind/",
            ],
        )
        self.assertEqual(strategy.filter_hints.get("function_ids"), ["19"])
        self.assertIn("Gemini", strategy.filter_hints.get("keywords") or [])
        self.assertTrue(any("Google" in query for query in strategy.search_seed_queries))

    def test_publication_and_search_planning_use_intent_view_scope_and_keywords(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找产品经理",
                "query": "product manager",
                "target_company": "WrongCo",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current"], retrieval_plan)
        publication = compile_publication_coverage_plan(request, strategy)
        search_plan = compile_search_strategy(request, strategy, publication, DeterministicModelClient())

        families = [item.family for item in publication.source_families]
        self.assertIn("product_subbrand_pages", families)
        self.assertTrue(any("Gemini contributor" in query for query in publication.seed_queries))

        relationship_bundle = next(item for item in search_plan.query_bundles if item.bundle_id == "relationship_web")
        self.assertTrue(any("Google" in query for query in relationship_bundle.queries))
        self.assertTrue(any("Gemini" in query for query in relationship_bundle.queries))
        self.assertIn("official_blog_and_docs", families)
        self.assertIn(
            "Use LLM extraction for acknowledgement, contributor, and weakly structured bylines.",
            publication.extraction_strategy,
        )

    def test_build_sourcing_plan_uses_intent_view_target_company_for_top_level_plan_and_tasks(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找产品经理",
                "query": "product manager",
                "target_company": "WrongCo",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")

        self.assertEqual(plan.target_company, "Google")
        self.assertEqual(acquire_task.status, "ready")
        self.assertEqual(acquire_task.metadata["max_pages"], 100)

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

    def test_google_suborg_scope_requires_review_for_full_roster(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Google 负责多模态（参与 Nano Banana 和 Veo）的华人研究员",
            query="Google multimodal Veo Nano Banana researchers",
            target_company="Google",
            categories=["employee", "researcher"],
            employment_statuses=["current"],
            organization_keywords=["Google DeepMind", "Veo", "Nano Banana"],
            keywords=[
                "Greater China experience",
                "Chinese bilingual outreach",
                "multimodal",
                "Veo",
                "Nano Banana",
            ],
            execution_preferences={"use_company_employees_lane": True},
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        gate = build_plan_review_gate(request, plan)

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(gate["status"], "requires_review")
        self.assertIn("google_scope_ambiguity_requires_confirmation", gate["reasons"])

    def test_google_scope_llm_disambiguation_triggers_review_for_sub_org_scope(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Google 负责多模态（参与 Nano Banana 和 Veo）的研究员",
            query="Google multimodal Veo Nano Banana researchers",
            target_company="Google",
            categories=["employee", "researcher"],
            employment_statuses=["current"],
            organization_keywords=["Google DeepMind", "Veo", "Nano Banana"],
            execution_preferences={
                "use_company_employees_lane": True,
                "allow_high_cost_sources": False,
            },
            scope_disambiguation={
                "inferred_scope": "sub_org_only",
                "sub_org_candidates": ["Google DeepMind", "Veo", "Nano Banana"],
                "confidence": 0.92,
                "rationale": "User mentions DeepMind product lines explicitly.",
                "source": "llm",
            },
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        gate = build_plan_review_gate(request, plan)

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(gate["status"], "requires_review")
        self.assertIn("google_scope_ambiguity_requires_confirmation", gate["reasons"])
        self.assertEqual(gate["scope_disambiguation"]["source"], "llm")
        self.assertEqual(gate["scope_disambiguation"]["inferred_scope"], "sub_org_only")

    def test_google_scope_review_can_be_skipped_when_scope_is_already_confirmed(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Google 负责多模态（参与 Nano Banana 和 Veo）的研究员",
            query="Google multimodal Veo Nano Banana researchers",
            target_company="Google",
            categories=["employee", "researcher"],
            employment_statuses=["current"],
            organization_keywords=["Google DeepMind", "Veo", "Nano Banana"],
            execution_preferences={
                "use_company_employees_lane": True,
                "allow_high_cost_sources": False,
                "confirmed_company_scope": ["Google", "Google DeepMind"],
            },
            scope_disambiguation={
                "inferred_scope": "sub_org_only",
                "sub_org_candidates": ["Google DeepMind", "Veo", "Nano Banana"],
                "confidence": 0.88,
                "rationale": "User intent aligns with DeepMind sub-org.",
                "source": "llm",
            },
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        gate = build_plan_review_gate(request, plan)

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(gate["status"], "ready")
        self.assertNotIn("google_scope_ambiguity_requires_confirmation", gate["reasons"])
        self.assertEqual(gate["scope_disambiguation"]["source"], "llm")

    def test_plan_review_gate_exposes_keyword_first_controls(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Google 做多模态的人",
            query="Google multimodal people",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current", "former"],
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        gate = build_plan_review_gate(request, plan)

        self.assertIn("keyword_priority_only", gate["editable_fields"])
        self.assertIn("former_keyword_queries_only", gate["editable_fields"])
        self.assertIn("provider_people_search_query_strategy", gate["editable_fields"])
        self.assertIn("provider_people_search_max_queries", gate["editable_fields"])
        self.assertIn("large_org_keyword_probe_mode", gate["editable_fields"])

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

    def test_google_full_roster_enables_large_org_keyword_probe_mode(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Google 负责多模态和 Veo 的研究员，全量跑 roster。",
                "target_company": "Google",
                "keywords": ["multimodal", "Veo", "Nano Banana"],
                "execution_preferences": {
                    "use_company_employees_lane": True,
                    "confirmed_company_scope": ["Google", "Google DeepMind"],
                },
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        shard_policy = dict(acquire_task.metadata.get("company_employee_shard_policy") or {})

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertTrue(plan.acquisition_strategy.cost_policy.get("large_org_keyword_probe_mode"))
        self.assertTrue(plan.acquisition_strategy.cost_policy.get("keyword_priority_only"))
        self.assertTrue(plan.acquisition_strategy.cost_policy.get("former_keyword_queries_only"))
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], "adaptive_large_org_keyword_probe")
        self.assertEqual(shard_policy.get("mode"), "keyword_union")
        self.assertTrue(shard_policy.get("force_keyword_shards"))
        self.assertEqual(
            shard_policy.get("root_filters", {}).get("companies"),
            [
                "https://www.linkedin.com/company/google/",
                "https://www.linkedin.com/company/deepmind/",
            ],
        )
        self.assertEqual(
            shard_policy.get("root_filters", {}).get("function_ids"),
            ["8", "9", "19", "24"],
        )
        self.assertEqual(
            plan.acquisition_strategy.filter_hints.get("locations"),
            ["United States"],
        )
        self.assertEqual(
            plan.acquisition_strategy.filter_hints.get("function_ids"),
            ["8", "9", "19", "24"],
        )
        self.assertTrue(
            any("Multimodal" in item["include_patch"]["search_query"] for item in list(shard_policy.get("keyword_shards") or []))
        )
        self.assertTrue(any("Nano Banana" in query for query in acquire_task.metadata.get("search_seed_queries", [])))
        self.assertFalse(any("Researcher" in query for query in acquire_task.metadata.get("search_seed_queries", [])))

    def test_plan_review_sync_keeps_large_org_keyword_shard_policy(self) -> None:
        request_payload = {
            "raw_user_request": "给我 Google 多模态研究员",
            "target_company": "Google",
            "execution_preferences": {
                "use_company_employees_lane": True,
                "confirmed_company_scope": ["Google", "Google DeepMind"],
            },
        }
        plan_payload = {
            "target_company": "Google",
            "acquisition_strategy": {
                "strategy_type": "full_company_roster",
                "company_scope": ["Google", "Google DeepMind"],
                "filter_hints": {
                    "keywords": ["multimodal", "Veo", "Nano Banana"],
                    "locations": ["United States"],
                    "function_ids": ["8", "9", "19", "24"],
                },
                "cost_policy": {
                    "large_org_keyword_probe_mode": True,
                },
                "search_channel_order": ["provider_people_search_api"],
                "search_seed_queries": ["Google multimodal researcher"],
            },
            "publication_coverage": {"source_families": []},
            "acquisition_tasks": [
                {
                    "task_type": "acquire_full_roster",
                    "status": "ready",
                    "metadata": {},
                }
            ],
        }

        _, updated_plan = apply_plan_review_decision(request_payload, plan_payload, {})
        acquire_task = updated_plan["acquisition_tasks"][0]
        shard_policy = dict(acquire_task["metadata"].get("company_employee_shard_policy") or {})

        self.assertEqual(acquire_task["metadata"]["company_employee_shard_strategy"], "adaptive_large_org_keyword_probe")
        self.assertEqual(shard_policy.get("mode"), "keyword_union")
        self.assertTrue(shard_policy.get("force_keyword_shards"))
        self.assertEqual(
            shard_policy.get("root_filters", {}).get("function_ids"),
            ["8", "9", "19", "24"],
        )

    def test_open_questions_require_confirmation_for_ambiguous_new_terms(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我Google做Avocado和Meta TBD方向的研究员",
                "query": "Google Avocado Meta TBD researchers",
                "target_company": "Google",
                "categories": ["employee", "researcher"],
                "employment_statuses": ["current"],
                "execution_preferences": {"use_company_employees_lane": True},
            }
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        gate = build_plan_review_gate(request, plan)

        self.assertTrue(any("Avocado" in item or "TBD" in item for item in plan.open_questions))
        self.assertEqual(gate["status"], "requires_review")
        self.assertIn("open_questions_present", gate["reasons"])
