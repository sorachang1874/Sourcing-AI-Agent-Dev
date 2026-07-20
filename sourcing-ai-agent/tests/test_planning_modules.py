import unittest

from sourcing_agent.acquisition_strategy import compile_acquisition_strategy
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_reuse_planning import (
    _baseline_population_default_reuse_sufficient,
    _large_authoritative_baseline_local_reuse_eligible,
    _planned_company_employee_delta_specs,
    _planned_profile_search_query_specs,
    apply_asset_reuse_plan_to_sourcing_plan,
)
from sourcing_agent.domain import AcquisitionTask, JobRequest, RetrievalPlan
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.organization_execution_profile import build_organization_execution_profile
from sourcing_agent.plan_review import apply_plan_review_decision, build_plan_review_gate
from sourcing_agent.planning import build_sourcing_plan
from sourcing_agent.publication_planning import compile_publication_coverage_plan
from sourcing_agent.request_normalization import build_request_preview_payload, resolve_request_intent_view
from sourcing_agent.search_planning import compile_search_strategy


class PlanningModulesTest(unittest.TestCase):
    def test_directional_technical_queries_default_to_researcher_and_engineer_population(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            }
        )

        intent_view = resolve_request_intent_view(
            request,
            fallback_categories=["employee", "former_employee"],
            fallback_employment_statuses=["current", "former"],
        )

        self.assertEqual(intent_view["categories"], ["researcher", "engineer"])

    def test_vision_language_keyword_stays_single_shard_in_intent_view(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Google做vision-language方向的人",
                "target_company": "Google",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["vision-language"],
            }
        )

        intent_view = resolve_request_intent_view(
            request,
            fallback_categories=["employee", "former_employee"],
            fallback_employment_statuses=["current", "former"],
        )

        self.assertEqual(intent_view["keywords"], ["Vision-language"])
        self.assertEqual(intent_view["must_have_facets"], ["multimodal"])

    def test_explicit_role_queries_do_not_expand_beyond_requested_role_population(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI做Post-train方向的Researcher",
                "target_company": "Reflection AI",
                "categories": ["researcher"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            }
        )

        intent_view = resolve_request_intent_view(
            request,
            fallback_categories=["employee", "former_employee"],
            fallback_employment_statuses=["current", "former"],
        )

        self.assertEqual(intent_view["categories"], ["researcher"])

    def test_implicit_single_role_categories_from_model_assisted_queries_expand_back_to_default_technical_population(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
                "target_company": "Reflection AI",
                "categories": ["researcher"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            }
        )

        intent_view = resolve_request_intent_view(
            request,
            fallback_categories=["employee", "former_employee"],
            fallback_employment_statuses=["current", "former"],
        )

        self.assertEqual(intent_view["categories"], ["researcher", "engineer"])

    def test_structured_single_role_requests_without_raw_query_remain_narrow(self) -> None:
        request = JobRequest.from_payload(
            {
                "target_company": "Reflection AI",
                "categories": ["researcher"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Post-train"],
            }
        )

        intent_view = resolve_request_intent_view(
            request,
            fallback_categories=["employee", "former_employee"],
            fallback_employment_statuses=["current", "former"],
        )

        self.assertEqual(intent_view["categories"], ["researcher"])

    def test_large_authoritative_baseline_local_reuse_accepts_small_backlog_ratios(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Anthropic做Pre-training的人",
                "target_company": "Anthropic",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Pre-train"],
            }
        )
        plan = build_sourcing_plan(
            request,
            AssetCatalog.discover(),
            DeterministicModelClient(),
            organization_execution_profile={"org_scale_band": "large"},
        )

        baseline = {
            "authoritative": True,
            "candidate_count": 3455,
            "current_lane_effective_candidate_count": 3015,
            "former_lane_effective_candidate_count": 556,
            "completeness_score": 86.95,
            "profile_detail_count": 3389,
            "missing_linkedin_count": 9,
            "profile_completion_backlog_count": 57,
            "manual_review_backlog_count": 12,
        }
        hidden_legacy_eligible = _large_authoritative_baseline_local_reuse_eligible(
            baseline=baseline,
            plan=plan,
            baseline_candidate_count=3455,
            current_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 3015,
            },
            former_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 556,
            },
        )
        explicit_coverage_eligible = _large_authoritative_baseline_local_reuse_eligible(
            baseline={
                **baseline,
                "source_snapshot_selection": {
                    "population_coverage": {
                        "coverage_kind": "full_company_roster",
                        "coverage_status": "verified",
                    },
                },
            },
            plan=plan,
            baseline_candidate_count=3455,
            current_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 3015,
            },
            former_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 556,
            },
        )

        self.assertFalse(hidden_legacy_eligible)
        self.assertTrue(explicit_coverage_eligible)

    def test_multi_snapshot_all_history_union_does_not_unlock_population_default_reuse(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Google做多模态方向的人",
                "target_company": "Google",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Multimodal"],
            }
        )
        plan = build_sourcing_plan(
            request,
            AssetCatalog.discover(),
            DeterministicModelClient(),
            organization_execution_profile={
                "target_company": "Google",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
            },
        )

        eligible = _baseline_population_default_reuse_sufficient(
            request=request,
            plan=plan,
            current_strategy_type="scoped_search_roster",
            baseline={
                "authoritative": True,
                "candidate_count": 3600,
                "selected_snapshot_ids": ["google-baseline", "google-multimodal"],
                "source_snapshot_selection": {
                    "mode": "all_history_snapshots",
                    "selected_snapshot_ids": ["google-baseline", "google-multimodal"],
                },
            },
            baseline_candidate_count=3600,
            current_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 3100,
            },
            former_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 520,
            },
        )

        self.assertFalse(eligible)

    def test_mode_only_preferred_multi_snapshot_subset_does_not_unlock_population_default_reuse(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Google做多模态方向的人",
                "target_company": "Google",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Multimodal"],
            }
        )
        plan = build_sourcing_plan(
            request,
            AssetCatalog.discover(),
            DeterministicModelClient(),
            organization_execution_profile={
                "target_company": "Google",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
            },
        )

        eligible = _baseline_population_default_reuse_sufficient(
            request=request,
            plan=plan,
            current_strategy_type="scoped_search_roster",
            baseline={
                "authoritative": True,
                "candidate_count": 3600,
                "selected_snapshot_ids": ["google-baseline", "google-multimodal"],
                "source_snapshot_selection": {
                    "mode": "preferred_snapshot_subset",
                    "selected_snapshot_ids": ["google-baseline", "google-multimodal"],
                },
            },
            baseline_candidate_count=3600,
            current_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 3100,
            },
            former_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 520,
            },
        )

        self.assertFalse(eligible)

    def test_promoted_multi_snapshot_subset_can_unlock_population_default_reuse(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Google做多模态方向的人",
                "target_company": "Google",
                "target_scope": "full_company_asset",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Multimodal"],
            }
        )
        plan = build_sourcing_plan(
            request,
            AssetCatalog.discover(),
            DeterministicModelClient(),
            organization_execution_profile={
                "target_company": "Google",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
            },
        )

        eligible = _baseline_population_default_reuse_sufficient(
            request=request,
            plan=plan,
            current_strategy_type="scoped_search_roster",
            baseline={
                "authoritative": True,
                "candidate_count": 3600,
                "selected_snapshot_ids": ["google-baseline", "google-multimodal"],
                "source_snapshot_selection": {
                    "mode": "preferred_snapshot_subset",
                    "selected_snapshot_ids": ["google-baseline", "google-multimodal"],
                    "coverage_proof": {
                        "contract": "promoted_aggregate_coverage",
                        "status": "promoted",
                        "proof_kind": "promoted_authoritative_aggregate",
                        "covered_snapshot_ids": ["google-baseline", "google-multimodal"],
                        "coverage_scope": "request_family",
                    },
                },
            },
            baseline_candidate_count=3600,
            current_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 3100,
            },
            former_lane_coverage={
                "effective_ready": True,
                "effective_candidate_count": 520,
            },
        )

        self.assertTrue(eligible)

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
                        "use_company_employees_lane": True,
                    },
                    "fallback_policy": {
                        "provider_people_search_query_strategy": "all_queries_union",
                        "run_former_search_seed": True,
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                    "execution_preferences": {
                        "allow_stage1_web_seed_fallback": True,
                    },
                },
            }
        )

        self.assertEqual(intent_view["target_company"], "Google")
        self.assertEqual(intent_view["categories"], ["employee"])
        self.assertEqual(intent_view["employment_statuses"], ["current", "former"])
        self.assertEqual(intent_view["organization_keywords"], ["Google DeepMind", "Gemini"])
        self.assertEqual(intent_view["must_have_primary_role_buckets"], ["product_management"])
        self.assertTrue(intent_view["execution_preferences"]["use_company_employees_lane"])
        self.assertTrue(intent_view["execution_preferences"]["run_former_search_seed"])
        self.assertTrue(intent_view["execution_preferences"]["allow_stage1_web_seed_fallback"])
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
                        "use_company_employees_lane": True,
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
        self.assertTrue(request.execution_preferences["use_company_employees_lane"])
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

    def test_plan_review_decision_applies_lane_and_fallback_preferences(self) -> None:
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
                "use_company_employees_lane": False,
                "run_former_search_seed": True,
                "provider_people_search_query_strategy": "all_queries_union",
                "provider_people_search_max_queries": 6,
                "provider_people_search_pages": 40,
                "provider_people_search_scale_chunk_pages": 4,
            },
        )

        prefs = updated_request["execution_preferences"]
        cost_policy = updated_plan["acquisition_strategy"]["cost_policy"]
        self.assertFalse(prefs["use_company_employees_lane"])
        self.assertTrue(prefs["run_former_search_seed"])
        self.assertEqual(prefs["provider_people_search_query_strategy"], "all_queries_union")
        self.assertEqual(cost_policy["allow_company_employee_api"], False)
        self.assertEqual(cost_policy["provider_people_search_query_strategy"], "all_queries_union")
        self.assertEqual(cost_policy["provider_people_search_max_queries"], 6)
        self.assertEqual(cost_policy["provider_people_search_pages"], 40)
        self.assertEqual(cost_policy["provider_people_search_scale_chunk_pages"], 4)

    def test_plan_review_scoped_search_high_cost_approval_promotes_current_harvest_lane(self) -> None:
        request_payload = {
            "raw_user_request": "我想要 OpenAI 做 Reasoning 方向的人",
            "target_company": "OpenAI",
            "execution_preferences": {},
        }
        plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "scoped_search_roster",
                "company_scope": ["OpenAI"],
                "filter_hints": {
                    "current_companies": ["OpenAI"],
                    "keywords": ["Reasoning", "research"],
                },
                "cost_policy": {
                    "provider_people_search_mode": "fallback_only",
                    "provider_people_search_min_expected_results": 10,
                },
                "reasoning": [],
            },
            "acquisition_tasks": [],
        }

        updated_request, updated_plan = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {
                "provider_people_search_query_strategy": "all_queries_union",
            },
        )

        self.assertEqual(
            updated_request["execution_preferences"]["provider_people_search_query_strategy"],
            "all_queries_union",
        )
        cost_policy = updated_plan["acquisition_strategy"]["cost_policy"]
        self.assertEqual(cost_policy["provider_people_search_mode"], "primary_only")
        self.assertEqual(cost_policy["provider_people_search_query_strategy"], "all_queries_union")
        self.assertEqual(cost_policy["provider_people_search_min_expected_results"], 50)
        self.assertEqual(cost_policy["provider_people_search_pages"], 2)
        self.assertTrue(cost_policy["provider_people_search_accept_zero_results"])
        self.assertTrue(cost_policy["former_keyword_queries_only"])

    def test_plan_review_confirmed_scope_does_not_write_target_company_as_scope_keyword(self) -> None:
        request_payload = {
            "raw_user_request": "帮我找PostHog做Coding方向的人",
            "target_company": "PostHog",
            "execution_preferences": {},
        }
        plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "scoped_search_roster",
                "company_scope": ["PostHog"],
                "filter_hints": {
                    "current_companies": ["PostHog"],
                    "keywords": ["Coding"],
                    "scope_keywords": ["PostHog"],
                },
                "cost_policy": {},
                "reasoning": [],
            },
            "acquisition_tasks": [],
        }

        _updated_request, updated_plan = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {
                "confirmed_company_scope": ["PostHog", "https://www.linkedin.com/company/posthog/"],
            },
        )

        strategy = dict(updated_plan.get("acquisition_strategy") or {})
        self.assertEqual(strategy.get("company_scope"), ["PostHog"])
        self.assertNotIn("scope_keywords", dict(strategy.get("filter_hints") or {}))

    def test_plan_review_gate_exposes_target_company_linkedin_override_field(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Safe Superintelligence Inc 的成员",
                "target_company": "Safe Superintelligence Inc",
            }
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        gate = build_plan_review_gate(request, plan)

        self.assertIn("target_company_linkedin_url", gate["editable_fields"])

    def test_plan_review_decision_persists_target_company_linkedin_override(self) -> None:
        request_payload = {
            "raw_user_request": "给我 Safe Superintelligence Inc 的成员",
            "target_company": "Safe Superintelligence Inc",
            "execution_preferences": {},
        }
        plan_payload = {
            "acquisition_strategy": {
                "strategy_type": "full_company_roster",
                "company_scope": ["Safe Superintelligence Inc"],
                "filter_hints": {},
                "cost_policy": {},
            },
            "acquisition_tasks": [],
        }

        updated_request, _ = apply_plan_review_decision(
            request_payload,
            plan_payload,
            {
                "target_company_linkedin_url": "https://www.linkedin.com/company/ssi-ai/?trk=public_profile_topcard-current-company",
            },
        )

        self.assertEqual(
            updated_request["execution_preferences"]["target_company_linkedin_url"],
            "https://www.linkedin.com/company/ssi-ai/",
        )
        self.assertEqual(updated_request["execution_preferences"]["target_company_linkedin_slug"], "ssi-ai")

    def test_request_preview_exposes_manual_target_company_identity_override(self) -> None:
        preview = build_request_preview_payload(
            request={
                "raw_user_request": "给我 Safe Superintelligence Inc 的成员",
                "target_company": "Safe Superintelligence Inc",
                "execution_preferences": {
                    "target_company_linkedin_url": "https://www.linkedin.com/company/ssi-ai/",
                },
            }
        )

        self.assertEqual(
            preview["target_company_identity"]["linkedin_company_url"],
            "https://www.linkedin.com/company/ssi-ai/",
        )
        self.assertEqual(preview["target_company_identity"]["linkedin_slug"], "ssi-ai")
        self.assertEqual(preview["target_company_identity"]["resolver"], "manual_review_override")

    def test_keyword_priority_scoped_search_uses_function_ids_without_job_titles_or_alias_duplication(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning方向的研究人员和工程师",
                "target_company": "OpenAI",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
                "must_have_facets": ["research"],
                "must_have_primary_role_buckets": ["research"],
                "primary_role_bucket_mode": "soft",
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["researcher", "engineer"], ["current", "former"], retrieval_plan)

        self.assertEqual(strategy.search_seed_queries, ["Reasoning"])
        self.assertCountEqual(strategy.filter_hints.get("function_ids"), ["24", "8"])
        self.assertNotIn("job_titles", strategy.filter_hints)
        self.assertTrue(strategy.cost_policy.get("former_keyword_queries_only"))

    def test_scoped_search_provider_keywords_exclude_role_facet_terms(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找PostHog做Coding方向的人，做scoped search",
                "target_company": "PostHog",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Coding"],
                "must_have_facets": ["research"],
                "must_have_primary_role_buckets": ["research"],
                "primary_role_bucket_mode": "soft",
                "execution_preferences": {
                    "acquisition_strategy_override": "scoped_search_roster",
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["researcher", "engineer"], ["current", "former"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertEqual(strategy.search_seed_queries, ["Coding"])
        self.assertEqual(strategy.filter_hints.get("keywords"), ["Coding"])
        self.assertNotIn("scope_keywords", strategy.filter_hints)
        self.assertCountEqual(strategy.filter_hints.get("function_ids"), ["24", "8"])

    def test_keyword_priority_directional_query_keeps_research_and_engineering_functions(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["employee"], ["current", "former"], retrieval_plan)

        self.assertEqual(strategy.search_seed_queries, ["Reasoning"])
        self.assertCountEqual(strategy.filter_hints.get("function_ids"), ["24", "8"])
        self.assertNotIn("job_titles", strategy.filter_hints)

    def test_openai_agent_directional_query_uses_agent_provider_keyword(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找OpenAI做Agent方向的人",
                "query": "帮我找OpenAI做Agent方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Agent"],
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "OpenAI",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "reason_codes": ["large_scale_signal"],
            },
        )

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertEqual(strategy.search_seed_queries, ["Agent"])
        self.assertEqual(strategy.filter_hints.get("keywords"), ["Agent"])
        self.assertCountEqual(strategy.filter_hints.get("function_ids"), ["24", "8"])
        self.assertNotIn("job_titles", strategy.filter_hints)

    def test_openai_pretrain_directional_query_prefers_scoped_search_keyword_shard(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我OpenAI做Pre-train方向的人",
                "query": "OpenAI Pre-train方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Pre-train"],
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "OpenAI",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "reason_codes": ["large_scale_signal"],
            },
        )

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertEqual(strategy.search_seed_queries, ["Pre-train"])
        self.assertCountEqual(strategy.filter_hints.get("function_ids"), ["24", "8"])
        self.assertNotIn("job_titles", strategy.filter_hints)
        self.assertEqual(
            strategy.strategy_decision_explanation.get("decision_source"),
            "organization_execution_profile",
        )

    def test_openai_infra_and_posttrain_query_keeps_distinct_keyword_shards(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我OpenAI做Infra和Post-train方向的人",
                "query": "OpenAI Infra 和 Post-train 方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Infra", "Post-train"],
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "OpenAI",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "reason_codes": ["large_scale_signal"],
            },
        )

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertEqual(strategy.search_seed_queries, ["Infra", "Post-train"])
        self.assertCountEqual(strategy.filter_hints.get("function_ids"), ["8", "24"])
        self.assertNotIn("job_titles", strategy.filter_hints)
        self.assertTrue(strategy.cost_policy.get("former_keyword_queries_only"))

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
        self.assertIn("Multimodal", keyword_hints)
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
        self.assertEqual(
            strategy.search_channel_order,
            ["harvest_company_employees", "harvest_profile_search", "profile_detail_api"],
        )
        self.assertEqual(strategy.roster_sources, ["harvest_company_employees", "company_directory_pages", "the_org"])
        # Text-inferred directional functions still annotate filter_hints for
        # the scoped/profile-search lanes; the roster lane's function coverage
        # is owned by the per-function shard contract, and there is no
        # large-org priority-list merging (operator directive 2026-07-20).
        self.assertEqual(strategy.filter_hints.get("function_ids"), ["24", "8"])
        self.assertNotIn("job_titles", strategy.filter_hints)
        # The unified full-roster lane never emits generic stage-1 seed text.
        self.assertEqual(strategy.search_seed_queries, [])
        self.assertTrue(strategy.cost_policy.get("allow_company_employee_api"))
        self.assertEqual(strategy.cost_policy.get("provider_people_search_mode"), "fallback_only")
        self.assertFalse(strategy.cost_policy.get("collect_email"))

    def test_organization_execution_profile_prefers_scoped_search_for_large_org_directional_query(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Google 做多模态和 Pre-train 方向的人",
            query="Google multimodal pre-train people",
            target_company="Google",
            keywords=["Multimodal", "Pre-train"],
            employment_statuses=["current", "former"],
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "Google",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "reason_codes": ["large_scale_signal"],
            },
        )

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertEqual(
            strategy.strategy_decision_explanation.get("decision_source"),
            "organization_execution_profile",
        )
        self.assertIn(
            "org_profile_default_scoped_search",
            list(strategy.strategy_decision_explanation.get("reason_codes") or []),
        )
        self.assertEqual(
            dict(strategy.organization_execution_profile).get("default_acquisition_mode"),
            "scoped_search_roster",
        )
        self.assertEqual(strategy.roster_sources, ["harvest_profile_search", "company_suborg_sources"])

    def test_organization_execution_profile_prefers_hybrid_broad_query_full_roster(self) -> None:
        request = JobRequest(
            raw_user_request="帮我找 Anthropic 的成员",
            query="Anthropic employees",
            target_company="Anthropic",
            employment_statuses=["current", "former"],
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "Anthropic",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "reason_codes": ["medium_scale_signal"],
            },
        )

        self.assertEqual(strategy.strategy_type, "full_company_roster")
        self.assertEqual(
            strategy.strategy_decision_explanation.get("decision_source"),
            "organization_execution_profile",
        )
        self.assertIn(
            "hybrid_broad_query",
            list(strategy.strategy_decision_explanation.get("reason_codes") or []),
        )

    def test_execution_profile_keeps_small_org_small_even_with_many_shards(self) -> None:
        profile = build_organization_execution_profile(
            target_company="Reflection AI",
            registry_row={
                "target_company": "Reflection AI",
                "company_key": "reflectionai",
                "snapshot_id": "snap_reflection",
                "candidate_count": 153,
                "current_lane_effective_candidate_count": 147,
                "former_lane_effective_candidate_count": 6,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "completeness_score": 39.83,
                "current_lane_coverage": {"effective_candidate_count": 147, "effective_ready": True},
                "former_lane_coverage": {"effective_candidate_count": 6, "effective_ready": True},
            },
            shard_rows=[
                {"lane": "profile_search", "employment_scope": "current", "estimated_total_count": 153}
                for _ in range(12)
            ],
        )

        self.assertEqual(profile["org_scale_band"], "small")
        self.assertEqual(profile["default_acquisition_mode"], "full_company_roster")
        self.assertIn("small_scale_signal", list(profile.get("reason_codes") or []))

    def test_execution_profile_treats_humansand_mid_hundreds_baseline_as_small_org(self) -> None:
        profile = build_organization_execution_profile(
            target_company="Humans&",
            registry_row={
                "target_company": "Humans&",
                "company_key": "humansand",
                "snapshot_id": "snap_humansand",
                "candidate_count": 600,
                "current_lane_effective_candidate_count": 520,
                "former_lane_effective_candidate_count": 80,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "completeness_score": 90.0,
                "current_lane_coverage": {"effective_candidate_count": 520, "effective_ready": True},
                "former_lane_coverage": {"effective_candidate_count": 80, "effective_ready": True},
            },
        )

        self.assertEqual(profile["org_scale_band"], "small")
        self.assertEqual(profile["default_acquisition_mode"], "full_company_roster")
        self.assertIn("small_scale_signal", list(profile.get("reason_codes") or []))

    def test_execution_profile_promotes_known_large_company_even_with_small_local_baseline(self) -> None:
        profile = build_organization_execution_profile(
            target_company="OpenAI",
            registry_row={
                "target_company": "OpenAI",
                "company_key": "openai",
                "snapshot_id": "snap_openai",
                "candidate_count": 153,
                "current_lane_effective_candidate_count": 117,
                "former_lane_effective_candidate_count": 36,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "completeness_score": 28.35,
                "current_lane_coverage": {"effective_candidate_count": 117, "effective_ready": True},
                "former_lane_coverage": {"effective_candidate_count": 36, "effective_ready": True},
            },
        )

        self.assertEqual(profile["org_scale_band"], "large")
        self.assertEqual(profile["default_acquisition_mode"], "scoped_search_roster")
        self.assertIn("known_large_company_override", list(profile.get("reason_codes") or []))

    def test_execution_profile_treats_xai_as_known_large_company(self) -> None:
        profile = build_organization_execution_profile(target_company="xAI")

        self.assertEqual(profile["org_scale_band"], "large")
        self.assertEqual(profile["default_acquisition_mode"], "scoped_search_roster")
        self.assertIn("fallback_known_large_company", list(profile.get("reason_codes") or []))

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
        self.assertFalse(strategy.cost_policy.get("former_broad_past_company_only"))

    def test_unscoped_former_only_strategy_explicitly_allows_broad_past_company_recall(self) -> None:
        request = JobRequest(
            raw_user_request="找 Anthropic 的前员工",
            target_company="Anthropic",
            employment_statuses=["former"],
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(request, ["former_employee"], ["former"], retrieval_plan)

        self.assertEqual(strategy.strategy_type, "former_employee_search")
        self.assertTrue(strategy.cost_policy.get("former_broad_past_company_only"))
        self.assertFalse(strategy.cost_policy.get("former_keyword_queries_only"))

    def test_directional_all_members_language_without_full_coverage_stays_scoped_search(self) -> None:
        request = JobRequest(
            raw_user_request="我要 xAI 做 Coding 方向的全部成员",
            query="xAI coding all members",
            target_company="xAI",
            keywords=["Coding"],
            must_have_facets=["coding"],
            employment_statuses=["current", "former"],
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "xAI",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "reason_codes": ["known_large_company_override"],
            },
        )

        self.assertEqual(strategy.strategy_type, "scoped_search_roster")
        self.assertEqual(
            strategy.strategy_decision_explanation.get("decision_source"),
            "request_population_boundary",
        )
        self.assertIn(
            "full_company_filter_requires_coverage_proof",
            list(strategy.strategy_decision_explanation.get("reason_codes") or []),
        )
        self.assertEqual(
            dict(strategy.strategy_decision_explanation.get("requested_population_boundary") or {}).get(
                "boundary_type"
            ),
            "scoped_directional",
        )

    def test_directional_all_members_language_with_full_coverage_uses_full_company_filter(self) -> None:
        request = JobRequest(
            raw_user_request="我要 xAI 做 Coding 方向的全部成员",
            query="xAI coding all members",
            target_company="xAI",
            keywords=["Coding"],
            must_have_facets=["coding"],
            employment_statuses=["current", "former"],
        )
        retrieval_plan = RetrievalPlan(strategy="hybrid", reason="test")
        strategy = compile_acquisition_strategy(
            request,
            ["employee"],
            ["current", "former"],
            retrieval_plan,
            organization_execution_profile={
                "target_company": "xAI",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "full_company_coverage_proven": True,
                "coverage_baseline_reuse_ready": True,
                "population_coverage_contract": {
                    "full_company_coverage_proven": True,
                    "coverage_kind": "full_company_roster",
                },
            },
        )

        self.assertEqual(strategy.strategy_type, "full_company_roster")
        self.assertEqual(
            strategy.strategy_decision_explanation.get("decision_source"),
            "request_population_boundary",
        )
        self.assertIn(
            "full_company_filter_allowed",
            list(strategy.strategy_decision_explanation.get("reason_codes") or []),
        )
        self.assertIn(
            "full_company_coverage_proven",
            list(strategy.strategy_decision_explanation.get("reason_codes") or []),
        )

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
                "execution_preferences": {
                    "allow_stage1_web_seed_fallback": True,
                },
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
        # Unified roster contract: the full-roster lane never emits generic
        # stage-1 seed text (updated 2026-07-20 — the old expectation assumed
        # keyword-probe seed queries, a retired lane shape).
        self.assertEqual(strategy.search_seed_queries, [])

    def test_publication_and_search_planning_use_intent_view_scope_and_keywords(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找产品经理",
                "query": "product manager",
                "target_company": "WrongCo",
                "execution_preferences": {
                    "allow_stage1_web_seed_fallback": True,
                },
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

    def test_task_intent_view_carries_execution_fields_for_acquisition_tasks(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找 Google Gemini 的产品经理",
                "query": "Gemini product manager",
                "target_company": "Google",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "fallback_policy": {
                        "run_former_search_seed": True,
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
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")
        linkedin_task = next(task for task in plan.acquisition_tasks if task.task_type == "enrich_linkedin_profiles")

        self.assertEqual(
            acquire_task.metadata["intent_view"]["search_channel_order"],
            acquire_task.metadata["search_channel_order"],
        )
        self.assertEqual(
            acquire_task.metadata["intent_view"]["cost_policy"]["provider_people_search_query_strategy"],
            acquire_task.metadata["cost_policy"]["provider_people_search_query_strategy"],
        )
        self.assertEqual(acquire_task.metadata["intent_view"]["max_pages"], acquire_task.metadata["max_pages"])
        self.assertEqual(former_task.metadata["intent_view"]["employment_statuses"], ["former"])
        self.assertEqual(former_task.metadata["intent_view"]["search_channel_order"], ["harvest_profile_search"])
        self.assertEqual(linkedin_task.metadata["intent_view"]["enrichment_scope"], "linkedin_stage_1")
        self.assertEqual(
            linkedin_task.metadata["intent_view"]["profile_detail_limit"],
            linkedin_task.metadata["profile_detail_limit"],
        )
        semantic_brief = dict(plan.acquisition_strategy.strategy_decision_explanation.get("semantic_brief") or {})
        role_targeting = dict(semantic_brief.get("role_targeting") or {})
        self.assertEqual(semantic_brief.get("target_company"), "Google")
        self.assertEqual(role_targeting.get("resolved_role_buckets"), ["product_management"])
        self.assertEqual(role_targeting.get("function_ids"), ["19"])

    def test_scoped_search_plan_prefetches_full_profile_detail_baseline(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning方向的人",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )

        plan = build_sourcing_plan(
            request,
            AssetCatalog.discover(),
            DeterministicModelClient(),
            organization_execution_profile={
                "target_company": "OpenAI",
                "org_scale_band": "large",
                "default_acquisition_mode": "scoped_search_roster",
                "reason_codes": ["large_scale_signal"],
            },
        )
        linkedin_task = next(task for task in plan.acquisition_tasks if task.task_type == "enrich_linkedin_profiles")

        self.assertEqual(linkedin_task.metadata["strategy_type"], "scoped_search_roster")
        self.assertTrue(linkedin_task.metadata["full_roster_profile_prefetch"])
        self.assertTrue(linkedin_task.metadata["intent_view"]["full_roster_profile_prefetch"])

    def test_asset_reuse_application_syncs_delta_fields_into_task_intent_view(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Reflection AI 的 Infra 成员",
                "target_company": "Reflection AI",
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        updated = apply_asset_reuse_plan_to_sourcing_plan(
            plan,
            {
                "baseline_snapshot_id": "snap_reflection",
                "planner_mode": "delta_from_snapshot",
                "baseline_sufficiency": "delta_required",
                "baseline_readiness": "partial",
                "baseline_completeness_ledger_path": "/tmp/ledger.json",
                "baseline_standard_bundle_count": 2,
                "requires_delta_acquisition": True,
                "selected_snapshot_ids": ["snap_reflection"],
                "missing_current_company_employee_shards": [],
                "missing_current_profile_search_queries": [],
                "missing_former_profile_search_queries": ["Reflection AI Infra employee"],
                "covered_former_profile_search_query_count": 0,
                "missing_former_profile_search_query_count": 1,
            },
        )

        former_task = next(task for task in updated.acquisition_tasks if task.task_type == "acquire_former_search_seed")
        self.assertEqual(former_task.metadata["intent_view"]["asset_reuse_plan"]["baseline_snapshot_id"], "snap_reflection")
        self.assertEqual(former_task.metadata["intent_view"]["delta_execution_plan"]["baseline_snapshot_id"], "snap_reflection")
        self.assertEqual(former_task.metadata["intent_view"]["search_seed_queries"], ["Reflection AI Infra employee"])

    def test_asset_reuse_delta_specs_prefer_intent_view_over_stale_flat_task_fields(self) -> None:
        task = AcquisitionTask(
            task_id="task_google_scope",
            task_type="acquire_full_roster",
            title="Acquire scoped Google roster",
            description="test",
            source_hint="test",
            status="ready",
            metadata={
                "strategy_type": "full_company_roster",
                "company_employee_shards": [
                    {
                        "shard_id": "stale-company-shard",
                        "title": "stale",
                        "company_filters": {"search_query": "stale"},
                    }
                ],
                "search_seed_queries": ["stale flat query"],
                "search_query_bundles": [{"queries": ["stale bundle query"]}],
                "filter_hints": {
                    "current_companies": ["https://www.linkedin.com/company/old-google/"],
                    "function_ids": ["8"],
                },
                "employment_statuses": ["former"],
                "intent_view": {
                    "strategy_type": "scoped_search_roster",
                    "company_employee_shards": [],
                    "search_seed_queries": ["Gemini product manager"],
                    "search_query_bundles": [],
                    "filter_hints": {
                        "current_companies": [
                            "https://www.linkedin.com/company/google/",
                            "https://www.linkedin.com/company/deepmind/",
                        ],
                        "function_ids": ["19"],
                    },
                    "employment_statuses": ["current", "former"],
                },
            },
        )

        company_specs, company_shards = _planned_company_employee_delta_specs(task)
        profile_specs, profile_queries = _planned_profile_search_query_specs(task)

        self.assertEqual(company_specs, [])
        self.assertEqual(company_shards, [])
        self.assertEqual(profile_queries, ["Gemini product manager"])
        self.assertEqual(profile_specs[0]["strategy_type"], "scoped_search_roster")
        self.assertEqual(profile_specs[0]["employment_scope"], "current")
        self.assertEqual(
            profile_specs[0]["company_scope"],
            [
                "https://www.linkedin.com/company/google/",
                "https://www.linkedin.com/company/deepmind/",
            ],
        )

    def test_asset_reuse_delta_specs_prefer_explicit_search_seed_queries_over_broad_filter_keywords(self) -> None:
        task = AcquisitionTask(
            task_id="task_openai_pretrain_scope",
            task_type="acquire_full_roster",
            title="Acquire scoped OpenAI roster",
            description="test",
            source_hint="test",
            status="ready",
            metadata={
                "strategy_type": "scoped_search_roster",
                "search_seed_queries": ["Pre-train"],
                "filter_hints": {
                    "current_companies": ["https://www.linkedin.com/company/openai/"],
                    "function_ids": ["24", "8"],
                    "keywords": ["Pre-train", "research"],
                },
                "employment_statuses": ["current", "former"],
                "intent_view": {
                    "strategy_type": "scoped_search_roster",
                    "search_seed_queries": ["Pre-train"],
                    "filter_hints": {
                        "current_companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["24", "8"],
                        "keywords": ["Pre-train", "research"],
                    },
                    "employment_statuses": ["current", "former"],
                },
            },
        )

        profile_specs, profile_queries = _planned_profile_search_query_specs(task)

        self.assertEqual(profile_queries, ["Pre-train"])
        self.assertEqual(profile_specs[0]["search_query"], "Pre-train")
        self.assertEqual(profile_specs[0]["employment_scope"], "current")
        self.assertEqual(profile_specs[0]["function_ids"], ["24", "8"])

    def test_asset_reuse_delta_specs_do_not_promote_facets_into_provider_shards(self) -> None:
        task = AcquisitionTask(
            task_id="task_google_vision_language_scope",
            task_type="acquire_full_roster",
            title="Acquire scoped Google roster",
            description="test",
            source_hint="test",
            status="ready",
            metadata={
                "strategy_type": "scoped_search_roster",
                "search_seed_queries": ["Vision-language"],
                "filter_hints": {
                    "current_companies": ["https://www.linkedin.com/company/google/"],
                    "function_ids": ["24", "8"],
                    "keywords": ["Vision-language", "Multimodal"],
                },
                "employment_statuses": ["current", "former"],
            },
        )

        profile_specs, profile_queries = _planned_profile_search_query_specs(task)

        self.assertEqual(profile_queries, ["Vision-language"])
        self.assertEqual(len(profile_specs), 1)
        self.assertEqual(profile_specs[0]["search_query"], "Vision-language")
        self.assertEqual(profile_specs[0]["function_ids"], ["24", "8"])

    def test_sourcing_plan_contains_search_strategy_and_filter_layers(self) -> None:
        request = JobRequest(
            raw_user_request="在 YouTube 和 Podcast 上检索所有 Gemini Team 的访谈内容，找到 Post-train 方向 researcher",
            query="Gemini post-train interview researcher",
            target_company="Google",
            execution_preferences={"allow_stage1_web_seed_fallback": True},
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        layer_ids = [item.get("layer_id") for item in plan.retrieval_plan.filter_layers]
        bundle_ids = [item.bundle_id for item in plan.search_strategy.query_bundles]
        self.assertIn("lexical_alias_recall", layer_ids)
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
        self.assertNotIn("google_scope_ambiguity_requires_confirmation", gate["reasons"])
        self.assertEqual(gate["scope_disambiguation"]["source"], "llm")
        self.assertFalse(gate["scope_disambiguation"]["requires_confirmation"])

    def test_plan_review_gate_exposes_lane_controls(self) -> None:
        request = JobRequest(
            raw_user_request="给我 Google 做多模态的人",
            query="Google multimodal people",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current", "former"],
        )
        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        gate = build_plan_review_gate(request, plan)

        self.assertIn("use_company_employees_lane", gate["editable_fields"])
        self.assertIn("former_keyword_queries_only", gate["editable_fields"])
        self.assertIn("provider_people_search_query_strategy", gate["editable_fields"])
        self.assertIn("provider_people_search_max_queries", gate["editable_fields"])
        # Retired org-size/keyword-probe knobs (operator directive 2026-07-20):
        # the roster lane has one unified per-function method, so there is
        # nothing size- or keyword-probe-specific left to edit.
        self.assertNotIn("keyword_priority_only", gate["editable_fields"])
        self.assertNotIn("large_org_keyword_probe_mode", gate["editable_fields"])

    def test_full_company_preferences_surface_in_plan_without_extra_review(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要 Humans& 公司全量成员，重新跑。",
                "target_company": "Humans&",
                "planning_mode": "heuristic",
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())

        self.assertEqual(request.execution_preferences["acquisition_strategy_override"], "full_company_roster")
        self.assertTrue(request.execution_preferences["force_fresh_run"])
        self.assertTrue(request.execution_preferences["use_company_employees_lane"])
        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertTrue(plan.acquisition_strategy.cost_policy["allow_company_employee_api"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["allow_cached_roster_fallback"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["allow_historical_profile_inheritance"])
        self.assertFalse(plan.acquisition_strategy.cost_policy["allow_shared_provider_cache"])
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
        self.assertTrue(plan.acquisition_strategy.cost_policy.get("former_broad_past_company_only"))
        self.assertEqual(acquire_task.metadata["max_pages"], 100)
        self.assertEqual(acquire_task.metadata["page_limit"], 25)
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], "adaptive_us_technical_partition")
        self.assertEqual(acquire_task.metadata["company_employee_shards"], [])
        self.assertEqual(
            acquire_task.metadata["company_employee_shard_policy"]["root_filters"],
            {"locations": ["United States"]},
        )
        self.assertEqual(
            acquire_task.metadata["company_employee_shard_policy"]["request_function_ids"],
            ["8", "24"],
        )
        self.assertEqual(acquire_task.metadata["company_employee_shard_policy"]["partition_rules"], [])
        self.assertTrue(acquire_task.metadata["include_former_search_seed"])

    def test_unified_full_company_roster_defaults_us_location_on_former_seed(self) -> None:
        # Unified roster contract (operator directive 2026-07-20): location is
        # a uniform request axis — every full roster defaults to the United
        # States regardless of org size unless the request opts out.
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 MiroMind.ai 的全部成员",
                "target_company": "MiroMind.ai",
                "employment_statuses": ["current", "former"],
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(plan.acquisition_strategy.filter_hints.get("locations"), ["United States"])
        self.assertEqual(former_task.metadata["filter_hints"].get("locations"), ["United States"])

    def test_small_org_full_company_roster_does_not_emit_generic_stage1_seed_queries(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Lovable的全部成员",
                "target_company": "Lovable",
                "employment_statuses": ["current", "former"],
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(plan.acquisition_strategy.search_seed_queries, [])
        self.assertEqual(acquire_task.metadata["search_seed_queries"], [])
        self.assertEqual(acquire_task.metadata["intent_view"]["search_seed_queries"], [])
        self.assertEqual(former_task.metadata["search_seed_queries"], [])
        self.assertEqual(former_task.metadata["intent_view"]["search_seed_queries"], [])
        manifest = dict(plan.acquisition_strategy.provider_execution_manifest or {})
        lanes = list(manifest.get("lanes") or [])
        self.assertEqual(manifest.get("strategy_type"), "full_company_roster")
        self.assertEqual(
            [(lane.get("provider"), lane.get("operation"), lane.get("query_texts")) for lane in lanes],
            [
                ("harvest_company_employees", "company_employees", []),
                ("harvest_profile_search", "profile_search", []),
            ],
        )
        self.assertFalse(any(lane.get("provider_facing_query") for lane in lanes))

    def test_full_company_roster_ignores_model_false_former_seed_without_user_denial(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Lovable的全部成员",
                "target_company": "Lovable",
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee", "former_employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "acquisition_lane_policy": {
                        "use_company_employees_lane": True,
                    },
                    "fallback_policy": {
                        "run_former_search_seed": False,
                    },
                },
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")

        self.assertTrue(request.execution_preferences["run_former_search_seed"])
        self.assertTrue(acquire_task.metadata["include_former_search_seed"])
        self.assertEqual(former_task.metadata["employment_statuses"], ["former"])

    def test_full_company_roster_honors_explicit_user_former_seed_denial(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Lovable的全部成员，不要 former",
                "target_company": "Lovable",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "execution_preferences": {
                    "run_former_search_seed": False,
                    "use_company_employees_lane": True,
                },
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        task_types = [task.task_type for task in plan.acquisition_tasks]

        self.assertFalse(request.execution_preferences["run_former_search_seed"])
        self.assertNotIn("acquire_former_search_seed", task_types)

    def test_xai_full_company_roster_plan_uses_overflow_tolerant_shard_policy(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 xAI 的所有成员",
                "target_company": "xAI",
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        shard_policy = dict(acquire_task.metadata.get("company_employee_shard_policy") or {})

        self.assertEqual(plan.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(acquire_task.metadata["max_pages"], 100)
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], "adaptive_us_technical_partition")
        self.assertTrue(acquire_task.metadata["include_former_search_seed"])
        self.assertEqual(
            shard_policy.get("root_filters"),
            {"locations": ["United States"]},
        )
        self.assertEqual(shard_policy.get("request_function_ids"), ["8", "24"])
        self.assertEqual(shard_policy.get("partition_rules"), [])
        # filter_hints stays free of paid function filters: roster function
        # coverage is owned by the shard policy's request_function_ids, and
        # profile-search fallback lanes stay broad (operator directive
        # 2026-07-20 — no org-size-based defaulting).
        self.assertNotIn(
            "function_ids",
            plan.acquisition_strategy.filter_hints,
        )
        self.assertTrue(shard_policy.get("allow_overflow_partial"))

    def test_scoped_search_roster_with_current_and_former_adds_former_seed_task(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "我想要OpenAI做Reasoning方向的人",
                "query": "OpenAI Reasoning方向的人",
                "target_company": "OpenAI",
                "categories": ["researcher", "engineer"],
                "employment_statuses": ["current", "former"],
                "keywords": ["Reasoning"],
                "must_have_facets": ["reasoning"],
                "must_have_primary_role_buckets": ["research"],
                "primary_role_bucket_mode": "soft",
                "execution_preferences": {
                    "keyword_priority_only": True,
                    "provider_people_search_query_strategy": "all_queries_union",
                },
            }
        )

        plan = build_sourcing_plan(request, AssetCatalog.discover(), DeterministicModelClient())
        acquire_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster")
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")

        self.assertEqual(plan.acquisition_strategy.strategy_type, "scoped_search_roster")
        self.assertTrue(acquire_task.metadata["include_former_search_seed"])
        self.assertEqual(former_task.metadata["employment_statuses"], ["former"])
        self.assertEqual(former_task.metadata["search_channel_order"], ["harvest_profile_search"])
        self.assertFalse(former_task.metadata["cost_policy"].get("former_broad_past_company_only"))
        self.assertEqual(former_task.metadata["intent_view"]["employment_statuses"], ["former"])
        manifest_lanes = list(dict(plan.acquisition_strategy.provider_execution_manifest or {}).get("lanes") or [])
        self.assertEqual(
            [(lane.get("employment_status"), lane.get("provider"), lane.get("query_texts")) for lane in manifest_lanes],
            [
                ("current", "harvest_profile_search", ["Reasoning"]),
                ("former", "harvest_profile_search", ["Reasoning"]),
            ],
        )
        self.assertTrue(all(lane.get("provider_facing_query") for lane in manifest_lanes))

    def test_google_full_roster_uses_unified_per_function_shard_contract(self) -> None:
        # Unified roster contract (operator directive 2026-07-20): even a
        # Google-scope keyword-heavy request runs the SAME per-function shard
        # policy as every other company — no keyword-probe mode, no org-size
        # forks, no priority function lists.
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
        self.assertNotIn("large_org_keyword_probe_mode", plan.acquisition_strategy.cost_policy)
        self.assertFalse(plan.acquisition_strategy.cost_policy.get("keyword_priority_only"))
        self.assertFalse(plan.acquisition_strategy.cost_policy.get("former_keyword_queries_only"))
        self.assertTrue(plan.acquisition_strategy.cost_policy.get("former_broad_past_company_only"))
        self.assertEqual(acquire_task.metadata["company_employee_shard_strategy"], "adaptive_us_technical_partition")
        self.assertEqual(shard_policy.get("request_function_ids"), ["8", "24"])
        self.assertEqual(shard_policy.get("keyword_shards"), [])
        self.assertEqual(shard_policy.get("partition_rules"), [])
        self.assertNotIn("function_ids", shard_policy.get("root_filters", {}))
        self.assertEqual(shard_policy.get("root_filters", {}).get("locations"), ["United States"])
        self.assertEqual(
            plan.acquisition_strategy.filter_hints.get("locations"),
            ["United States"],
        )
        # The unified full-roster lane never emits generic stage-1 seed text.
        self.assertEqual(acquire_task.metadata.get("search_seed_queries"), [])
        manifest_lanes = list(dict(plan.acquisition_strategy.provider_execution_manifest or {}).get("lanes") or [])
        current_lane = next(lane for lane in manifest_lanes if lane.get("lane_id") == "current_company_employees")
        self.assertEqual(current_lane.get("provider"), "harvest_company_employees")
        self.assertEqual(current_lane.get("reason"), "adaptive_shard_probe_pending")
        self.assertEqual(current_lane.get("request_function_ids"), ["8", "24"])
        self.assertFalse(current_lane.get("provider_facing_query"))

    def test_plan_review_sync_keeps_unified_shard_policy(self) -> None:
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
                },
                "cost_policy": {},
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

        self.assertEqual(acquire_task["metadata"]["company_employee_shard_strategy"], "adaptive_us_technical_partition")
        self.assertEqual(shard_policy.get("request_function_ids"), ["8", "24"])
        self.assertEqual(shard_policy.get("keyword_shards"), [])
        self.assertNotIn("function_ids", shard_policy.get("root_filters", {}))
        self.assertEqual(shard_policy.get("root_filters", {}).get("locations"), ["United States"])

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
