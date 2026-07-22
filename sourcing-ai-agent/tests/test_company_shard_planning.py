import unittest

from sourcing_agent.company_shard_planning import (
    FORMER_FUNCTION_SHARD_PLAN_MARKER,
    build_default_company_employee_shard_policy,
    build_request_scoped_former_search_shard_plan,
    plan_company_employee_shards_from_policy,
)


class CompanyShardPlanningTest(unittest.TestCase):
    def test_build_default_company_employee_shard_policy_for_anthropic(self) -> None:
        policy = build_default_company_employee_shard_policy(
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(policy["request_function_ids"], ["8", "24"])
        self.assertEqual(policy["partition_rules"], [])
        self.assertTrue(policy["allow_overflow_partial"])
        self.assertEqual(policy["provider_result_cap"], 2500)

    def test_build_default_company_employee_shard_policy_for_xai(self) -> None:
        policy = build_default_company_employee_shard_policy(
            max_pages=100,
            page_limit=25,
        )

        self.assertEqual(policy["strategy_id"], "adaptive_us_technical_partition")
        self.assertEqual(policy["root_filters"], {"locations": ["United States"]})
        self.assertEqual(policy["request_function_ids"], ["8", "24"])
        self.assertEqual(policy["partition_rules"], [])

    def test_build_default_company_employee_shard_policy_for_openai_uses_large_org_technical_default(self) -> None:
        policy = build_default_company_employee_shard_policy(
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


class FormerSearchShardPlanTest(unittest.TestCase):
    def test_former_shard_plan_builds_one_marked_shard_per_function_id(self) -> None:
        # operator directive 2026-07-20: the former-member recall lane runs
        # per-function independent shards — never one merged multi-function
        # query — and each shard stamps the plan-derived marker so its single
        # function id may reach the provider payload.
        plan = build_request_scoped_former_search_shard_plan(
            function_ids=["8", "24", "8", " 24 "],
            past_companies=["Anthropic", "Anthropic"],
            locations=None,
        )

        self.assertEqual(plan["strategy_id"], "request_function_partition")
        self.assertEqual(plan["function_ids"], ["8", "24"])
        self.assertEqual(plan["past_companies"], ["Anthropic"])
        self.assertEqual(plan["locations"], ["United States"])
        self.assertEqual(len(plan["shards"]), 2)
        self.assertEqual(
            [str(shard.get("shard_id") or "") for shard in plan["shards"]],
            ["former_function_8", "former_function_24"],
        )
        for shard, function_id in zip(plan["shards"], ["8", "24"]):
            self.assertEqual(shard["strategy_id"], "request_function_partition")
            self.assertEqual(shard["function_ids"], [function_id])
            filter_hints = dict(shard["filter_hints"])
            self.assertEqual(filter_hints["past_companies"], ["Anthropic"])
            self.assertEqual(filter_hints["function_ids"], [function_id])
            self.assertTrue(filter_hints[FORMER_FUNCTION_SHARD_PLAN_MARKER])
            self.assertEqual(filter_hints["locations"], ["United States"])
            self.assertNotIn("exclude_locations", filter_hints)

    def test_former_shard_plan_location_opt_out_and_excludes(self) -> None:
        plan = build_request_scoped_former_search_shard_plan(
            function_ids=["24"],
            past_companies=["xAI"],
            locations=[],
            exclude_locations=["Canada"],
        )

        self.assertEqual(plan["locations"], [])
        self.assertEqual(plan["exclude_locations"], ["Canada"])
        self.assertEqual(len(plan["shards"]), 1)
        filter_hints = dict(plan["shards"][0]["filter_hints"])
        self.assertNotIn("locations", filter_hints)
        self.assertEqual(filter_hints["exclude_locations"], ["Canada"])
        self.assertTrue(filter_hints[FORMER_FUNCTION_SHARD_PLAN_MARKER])

    def test_former_shard_plan_location_passthrough(self) -> None:
        plan = build_request_scoped_former_search_shard_plan(
            function_ids=["8"],
            past_companies=["OpenAI"],
            locations=["United States", "United Kingdom"],
        )

        self.assertEqual(plan["locations"], ["United States", "United Kingdom"])
        filter_hints = dict(plan["shards"][0]["filter_hints"])
        self.assertEqual(filter_hints["locations"], ["United States", "United Kingdom"])

    def test_former_shard_plan_empty_selection_yields_one_broad_shard_without_marker(self) -> None:
        # No function selection keeps the legacy broad recall probe: one
        # shard, NO plan marker, so the connector guardrail strips any
        # non-plan-derived function ids exactly as before (anti-["19"]).
        plan = build_request_scoped_former_search_shard_plan(
            function_ids=[],
            past_companies=["Anthropic"],
            locations=None,
        )

        self.assertEqual(plan["strategy_id"], "broad_former_recall")
        self.assertEqual(plan["function_ids"], [])
        self.assertEqual(len(plan["shards"]), 1)
        shard = plan["shards"][0]
        self.assertEqual(shard["shard_id"], "former_broad")
        self.assertEqual(shard["strategy_id"], "broad_former_recall")
        self.assertEqual(shard["function_ids"], [])
        filter_hints = dict(shard["filter_hints"])
        self.assertEqual(filter_hints["past_companies"], ["Anthropic"])
        self.assertEqual(filter_hints["locations"], ["United States"])
        self.assertNotIn(FORMER_FUNCTION_SHARD_PLAN_MARKER, filter_hints)
        self.assertNotIn("function_ids", filter_hints)




class FormerFilterHintsCompanyUrlNormalizationTest(unittest.TestCase):
    """`_build_former_filter_hints` must emit LinkedIn company URLs for
    past-company lanes (dry-run finding, xAI 2026-07-21): plan-level hints may
    carry the bare slug/name for slug-only registry identities; the provider
    requires URL form."""

    def test_slug_only_identity_resolves_to_url(self) -> None:
        from sourcing_agent.acquisition import _build_former_filter_hints
        from sourcing_agent.connectors import CompanyIdentity

        identity = CompanyIdentity(
            requested_name="xAI", canonical_name="xAI", company_key="xai",
            linkedin_slug="xai", linkedin_company_url="https://www.linkedin.com/company/xai/",
            domain="x.ai", aliases=["x.ai", "x ai"], resolver="manual_review_override", confidence="high",
        )
        hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints={"current_companies": ["xAI"], "locations": ["United States"]},
        )
        self.assertEqual(hints["past_companies"], ["https://www.linkedin.com/company/xai/"])

    def test_slug_derived_url_and_foreign_non_url_dropped(self) -> None:
        from sourcing_agent.acquisition import _build_former_filter_hints
        from sourcing_agent.connectors import CompanyIdentity

        identity = CompanyIdentity(
            requested_name="xAI", canonical_name="xAI", company_key="xai",
            linkedin_slug="xai", linkedin_company_url="", domain="x.ai",
            aliases=["x.ai", "x ai"], resolver="builtin", confidence="high",
        )
        hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints={"current_companies": ["xAI", "Some Other Company"], "locations": ["United States"]},
        )
        self.assertEqual(hints["past_companies"], ["https://www.linkedin.com/company/xai/"])
        self.assertEqual(hints["_dropped_non_url_company_references"], ["Some Other Company"])

    def test_url_passthrough_and_alias_urls_preserved(self) -> None:
        from sourcing_agent.acquisition import _build_former_filter_hints
        from sourcing_agent.connectors import CompanyIdentity

        identity = CompanyIdentity(
            requested_name="Google", canonical_name="Google", company_key="google",
            linkedin_slug="googledeepmind", linkedin_company_url="https://www.linkedin.com/company/googledeepmind/",
            domain="google.com", aliases=["googledeepmind", "deepmind"],
            resolver="manual_review_override", confidence="high",
        )
        hints = _build_former_filter_hints(
            identity=identity,
            base_filter_hints={
                "current_companies": [
                    "https://www.linkedin.com/company/googledeepmind/",
                    "https://www.linkedin.com/company/deepmind/",
                ],
                "locations": ["United States"],
            },
        )
        self.assertEqual(
            hints["past_companies"],
            ["https://www.linkedin.com/company/googledeepmind/", "https://www.linkedin.com/company/deepmind/"],
        )
        self.assertNotIn("_dropped_non_url_company_references", hints)


if __name__ == "__main__":
    unittest.main()
