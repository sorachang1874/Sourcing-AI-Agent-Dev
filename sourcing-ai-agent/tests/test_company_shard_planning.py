import unittest

from sourcing_agent.company_shard_planning import (
    build_default_company_employee_shard_policy,
    build_large_org_keyword_probe_shard_policy,
    plan_company_employee_shards_from_policy,
)


class CompanyShardPlanningTest(unittest.TestCase):
    def test_build_default_company_employee_shard_policy_for_anthropic(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
            organization_execution_profile={"org_scale_band": "large"},
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"], "function_ids": ["8", "24"]})
        self.assertTrue(policy["allow_overflow_partial"])
        self.assertEqual(policy["partition_rules"][0]["title"], "Engineering")
        self.assertEqual(policy["provider_result_cap"], 2500)

    def test_build_default_company_employee_shard_policy_for_xai(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "xai",
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"], "function_ids": ["8", "24"]})
        self.assertTrue(policy["allow_overflow_partial"])
        self.assertEqual(
            [item["include_patch"]["exclude_function_ids"] for item in policy["partition_rules"]],
            [["24"], ["8"]],
        )

    def test_build_default_company_employee_shard_policy_for_openai_uses_large_org_technical_default(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "openai",
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"], "function_ids": ["8", "24"]})
        self.assertTrue(policy["allow_overflow_partial"])
        self.assertEqual(
            [item["include_patch"]["exclude_function_ids"] for item in policy["partition_rules"]],
            [["24"], ["8"]],
        )

    def test_plan_company_employee_shards_from_policy_probes_until_remaining_scope_is_within_cap(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
        )
        policy["allow_overflow_partial"] = False

        counts = {
            (("function_ids", ("8", "24")), ("locations", ("United States",))): 3124,
            (
                ("exclude_function_ids", ("24",)),
                ("function_ids", ("8", "24")),
                ("locations", ("United States",)),
            ): 1100,
            (
                ("exclude_function_ids", ("8",)),
                ("function_ids", ("8", "24")),
                ("locations", ("United States",)),
            ): 2024,
        }

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            key = tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in filters.items()))
            return {
                "status": "completed",
                "estimated_total_count": counts.get(key, 0),
                "detail": f"probe {context['title']}",
            }

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(len(plan["shards"]), 2)
        self.assertEqual(plan["shards"][0]["title"], "United States / Engineering")
        self.assertEqual(plan["shards"][0]["company_filters"]["exclude_function_ids"], ["24"])
        self.assertEqual(plan["shards"][1]["title"], "United States / Remaining after Engineering")
        self.assertEqual(plan["shards"][1]["company_filters"]["exclude_function_ids"], ["8"])

    def test_plan_company_employee_shards_from_policy_blocks_when_branch_stays_over_cap(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
        )
        policy["allow_overflow_partial"] = False

        counts = {
            (("function_ids", ("8", "24")), ("locations", ("United States",))): 5000,
            (
                ("exclude_function_ids", ("24",)),
                ("function_ids", ("8", "24")),
                ("locations", ("United States",)),
            ): 3200,
        }

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            key = tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in filters.items()))
            return {
                "status": "completed",
                "estimated_total_count": counts.get(key, 0),
                "detail": f"probe {context['title']}",
            }

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "blocked")
        self.assertEqual(plan["reason"], "partition_branch_over_cap")
        self.assertEqual(plan["overflow_scope"]["estimated_total_count"], 3200)

    def test_plan_company_employee_shards_partition_mode_allows_partial_overflow_when_enabled(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "xai",
            max_pages=100,
            page_limit=25,
        )

        counts = {
            (("function_ids", ("8", "24")), ("locations", ("United States",))): 5600,
            (
                ("exclude_function_ids", ("24",)),
                ("function_ids", ("8", "24")),
                ("locations", ("United States",)),
            ): 3100,
            (
                ("exclude_function_ids", ("8",)),
                ("function_ids", ("8", "24")),
                ("locations", ("United States",)),
            ): 2600,
            (
                ("exclude_function_ids", ("8", "24")),
                ("function_ids", ("8", "24")),
                ("locations", ("United States",)),
            ): 0,
        }

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            key = tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in filters.items()))
            return {
                "status": "completed",
                "estimated_total_count": counts.get(key, 0),
                "detail": f"probe {context['title']}",
            }

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["reason"], "partition_with_capped_shards")
        self.assertEqual(len(plan["shards"]), 2)
        self.assertTrue(any(item.get("provider_cap_limited") for item in plan["shards"]))
        self.assertTrue(any("Research" in str(item.get("title") or "") for item in plan["overflow_scopes"]))

    def test_build_large_org_keyword_probe_shard_policy_for_google_deepmind(self) -> None:
        policy = build_large_org_keyword_probe_shard_policy(
            "google",
            company_scope=["Google", "Google DeepMind"],
            keyword_hints=["multimodal", "Veo", "Nano Banana"],
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_large_org_keyword_probe")
        self.assertEqual(policy["mode"], "keyword_union")
        self.assertTrue(policy["force_keyword_shards"])
        self.assertTrue(policy["allow_overflow_partial"])
        self.assertEqual(
            policy["root_filters"]["companies"],
            [
                "https://www.linkedin.com/company/google/",
                "https://www.linkedin.com/company/deepmind/",
            ],
        )
        self.assertEqual(policy["root_filters"]["locations"], ["United States"])
        self.assertEqual(policy["root_filters"]["function_ids"], ["8", "9", "19", "24"])
        self.assertTrue(any("Multimodal" in item["include_patch"]["search_query"] for item in policy["keyword_shards"]))
        self.assertTrue(any("Veo" in item["include_patch"]["search_query"] for item in policy["keyword_shards"]))

    def test_plan_company_employee_shards_keyword_union_mode(self) -> None:
        policy = build_large_org_keyword_probe_shard_policy(
            "google",
            company_scope=["Google", "Google DeepMind"],
            keyword_hints=["multimodal", "Veo"],
            max_pages=100,
            page_limit=25,
        )

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            search_query = str(filters.get("search_query") or "").strip()
            if context.get("probe_id") == "root":
                return {"status": "completed", "estimated_total_count": 15000}
            if "Multimodal" in search_query:
                return {"status": "completed", "estimated_total_count": 1800}
            if "Veo" in search_query:
                return {"status": "completed", "estimated_total_count": 900}
            return {"status": "completed", "estimated_total_count": 0}

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["reason"], "keyword_union_partition")
        self.assertTrue(plan["union_dedupe_required"])
        self.assertEqual(len(plan["shards"]), 2)
        self.assertTrue(any("Multimodal" in item["company_filters"]["search_query"] for item in plan["shards"]))
        self.assertTrue(any("Veo" in item["company_filters"]["search_query"] for item in plan["shards"]))

    def test_large_org_keyword_probe_policy_dedupes_hyphen_space_and_synonym_queries(self) -> None:
        policy = build_large_org_keyword_probe_shard_policy(
            "google",
            company_scope=["Google", "Google DeepMind"],
            keyword_hints=[
                "vision-language",
                "Vision Language",
                "video-generation",
                "Video generation",
            ],
            max_pages=100,
            page_limit=25,
        )

        queries = [str(item.get("include_patch", {}).get("search_query") or "") for item in list(policy.get("keyword_shards") or [])]
        self.assertEqual(queries.count("Vision-language"), 1)
        self.assertEqual(queries.count("Video generation"), 1)

    def test_plan_company_employee_shards_keyword_union_respects_force_keyword_only(self) -> None:
        policy = build_large_org_keyword_probe_shard_policy(
            "google",
            company_scope=["Google", "Google DeepMind"],
            keyword_hints=["multimodal", "Veo"],
            max_pages=100,
            page_limit=25,
        )

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            search_query = str(filters.get("search_query") or "").strip()
            if context.get("probe_id") == "root":
                return {"status": "completed", "estimated_total_count": 1200}
            if "Multimodal" in search_query:
                return {"status": "completed", "estimated_total_count": 300}
            if "Veo" in search_query:
                return {"status": "completed", "estimated_total_count": 220}
            return {"status": "completed", "estimated_total_count": 0}

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["reason"], "keyword_union_partition")
        self.assertEqual(len(plan["shards"]), 2)

    def test_plan_company_employee_shards_keyword_union_allows_partial_overflow(self) -> None:
        policy = build_large_org_keyword_probe_shard_policy(
            "google",
            company_scope=["Google", "Google DeepMind"],
            keyword_hints=["multimodal", "vision-language", "Veo"],
            max_pages=100,
            page_limit=25,
        )

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            search_query = str(filters.get("search_query") or "").strip()
            if context.get("probe_id") == "root":
                return {"status": "completed", "estimated_total_count": 20000}
            if "Multimodal" in search_query:
                return {"status": "completed", "estimated_total_count": 1200}
            if "vision-language" in search_query.lower():
                return {"status": "completed", "estimated_total_count": 3200}
            if "Veo" in search_query:
                return {"status": "completed", "estimated_total_count": 800}
            return {"status": "completed", "estimated_total_count": 0}

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["reason"], "keyword_union_with_capped_shards")
        self.assertEqual(len(plan["shards"]), 3)
        self.assertTrue(any("vision-language" in str(item.get("title") or "").lower() for item in plan["overflow_scopes"]))
        self.assertTrue(any(item.get("provider_cap_limited") for item in plan["shards"]))


if __name__ == "__main__":
    unittest.main()
