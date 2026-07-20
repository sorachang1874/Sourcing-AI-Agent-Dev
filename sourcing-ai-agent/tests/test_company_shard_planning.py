import unittest

from sourcing_agent.company_shard_planning import (
    build_default_company_employee_shard_policy,
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
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(policy["request_function_ids"], ["8", "24"])
        self.assertEqual(policy["partition_rules"], [])
        self.assertTrue(policy["allow_overflow_partial"])
        self.assertEqual(policy["provider_result_cap"], 2500)

    def test_build_default_company_employee_shard_policy_for_xai(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "xai",
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(policy["request_function_ids"], ["8", "24"])
        self.assertEqual(policy["partition_rules"], [])

    def test_build_default_company_employee_shard_policy_for_openai_uses_large_org_technical_default(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "openai",
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(policy["request_function_ids"], ["8", "24"])
        self.assertEqual(policy["partition_rules"], [])

    def test_technical_partition_never_probes_or_plans_merged_function_queries(self) -> None:
        # operator directive 2026-07-20: function coverage is acquired as
        # separate per-function shard roots — functionIds ["8"] and ["24"]
        # submitted independently.  No probe or shard may ever carry a merged
        # multi-function function_ids list.
        policy = build_default_company_employee_shard_policy(
            "openai",
            max_pages=100,
            page_limit=25,
        )
        self.assertNotIn("function_ids", policy["root_filters"])

        probed_filters: list[dict[str, object]] = []

        def probe_fn(filters, context):  # noqa: ANN001, ANN202
            probed_filters.append(dict(filters))
            return {
                "status": "completed",
                "estimated_total_count": 100,
                "detail": f"probe {context['title']}",
            }

        plan = plan_company_employee_shards_from_policy(policy, probe_fn=probe_fn)

        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["reason"], "request_function_partition")
        for filters in probed_filters:
            self.assertLessEqual(len(list(filters.get("function_ids") or [])), 1)
        self.assertEqual(len(plan["shards"]), 2)
        for shard in plan["shards"]:
            self.assertEqual(str(shard.get("strategy_id") or ""), "request_function_partition")
            self.assertLessEqual(len(list(shard["company_filters"].get("function_ids") or [])), 1)
            self.assertNotIn("exclude_function_ids", shard["company_filters"])
        self.assertEqual(plan["shards"][0]["company_filters"]["function_ids"], ["8"])
        self.assertEqual(plan["shards"][1]["company_filters"]["function_ids"], ["24"])

    def test_plan_company_employee_shards_from_policy_plans_one_root_per_function(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
        )

        counts = {
            (("function_ids", ("8",)), ("locations", ("United States",))): 1100,
            (("function_ids", ("24",)), ("locations", ("United States",))): 900,
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
        self.assertEqual(plan["shards"][0]["title"], "United States / Engineer")
        self.assertEqual(plan["shards"][0]["company_filters"]["function_ids"], ["8"])
        self.assertEqual(plan["shards"][0]["company_filters"]["locations"], ["United States"])
        self.assertEqual(plan["shards"][1]["title"], "United States / Researcher")
        self.assertEqual(plan["shards"][1]["company_filters"]["function_ids"], ["24"])
        self.assertEqual(plan["shards"][1]["company_filters"]["locations"], ["United States"])

    def test_plan_company_employee_shards_from_policy_blocks_when_function_root_stays_over_cap(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
        )
        policy["allow_overflow_partial"] = False

        counts = {
            (("function_ids", ("8",)), ("locations", ("United States",))): 3200,
            (("function_ids", ("24",)), ("locations", ("United States",))): 900,
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
        self.assertEqual(plan["reason"], "function_shard_planning_failed")
        self.assertEqual(plan["failed_function_id"], "8")
        self.assertIn("3200", plan["detail"])

    def test_plan_company_employee_shards_allows_capped_function_root_when_overflow_enabled(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "xai",
            max_pages=100,
            page_limit=25,
        )

        counts = {
            (("function_ids", ("8",)), ("locations", ("United States",))): 3100,
            (("function_ids", ("24",)), ("locations", ("United States",))): 1200,
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
        self.assertEqual(plan["reason"], "request_function_partition_with_capped_shards")
        self.assertEqual(len(plan["shards"]), 2)
        capped = [item for item in plan["shards"] if item.get("provider_cap_limited")]
        self.assertEqual(len(capped), 1)
        self.assertEqual(capped[0]["company_filters"]["function_ids"], ["8"])
        self.assertEqual(capped[0]["estimated_total_count_before_cap"], 3100)
        self.assertTrue(any("Engineer" in str(item.get("title") or "") for item in plan["overflow_scopes"]))


if __name__ == "__main__":
    unittest.main()
