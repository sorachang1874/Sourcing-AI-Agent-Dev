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
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_function_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(policy["partition_rules"][0]["title"], "Engineering")
        self.assertEqual(policy["provider_result_cap"], 2500)

    def test_plan_company_employee_shards_from_policy_probes_until_remaining_scope_is_within_cap(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
        )

        counts = {
            (("locations", ("United States",)),): 3124,
            (("function_ids", ("8",)), ("locations", ("United States",))): 1100,
            (("exclude_function_ids", ("8",)), ("locations", ("United States",))): 2024,
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
        self.assertEqual(plan["shards"][0]["company_filters"]["function_ids"], ["8"])
        self.assertEqual(plan["shards"][1]["title"], "United States / Remaining after Engineering")
        self.assertEqual(plan["shards"][1]["company_filters"]["exclude_function_ids"], ["8"])

    def test_plan_company_employee_shards_from_policy_blocks_when_branch_stays_over_cap(self) -> None:
        policy = build_default_company_employee_shard_policy(
            "anthropic",
            max_pages=100,
            page_limit=25,
        )

        counts = {
            (("locations", ("United States",)),): 5000,
            (("function_ids", ("8",)), ("locations", ("United States",))): 3200,
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


if __name__ == "__main__":
    unittest.main()
