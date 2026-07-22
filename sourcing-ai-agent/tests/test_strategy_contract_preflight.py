"""strategy_type contract preflight + Step 2/3 flip-target pins (B1, 2026-07-22).

Why: AGENTS.md Contract Field Ownership requires a fast preflight comparing
contract fields across public surfaces BEFORE changing them, and none existed
for strategy_type/default_acquisition_mode — drift would first surface in a
paid live regression (scripts/run_live_large_org_regression.py asserts
strategy literals only under the live triple-gate). Master plan WS1 names this
the hard prerequisite for Steps 2-3.

Two test families:
1. Cross-surface equality: strategy_type must read identically on the plan's
   acquisition_strategy, every strategy-carrying task metadata, the provider
   execution manifest, and the plan-review rebuild.
2. CURRENT-CONTRACT PINS that the unification is ratified to FLIP — each pin
   names its flip batch and must be updated in that same change:
   - size steering (org profile flips strategy) -> flips in WS1 Step 3;
   - former-only fork (former statuses hijack the roster task's strategy,
     no per-function former plan) -> flips in WS1 Step 2.
"""

import unittest

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.plan_review import apply_plan_review_decision
from sourcing_agent.planning import build_sourcing_plan


def _plan(payload: dict, profile: dict | None = None):
    return build_sourcing_plan(
        JobRequest.from_payload(payload),
        AssetCatalog.discover(),
        DeterministicModelClient(),
        organization_execution_profile=profile,
    )


def _strategy_readings(plan) -> dict[str, object]:
    strategy = plan.acquisition_strategy
    task_values = {
        task.task_type: task.metadata.get("strategy_type")
        for task in plan.acquisition_tasks
        if task.metadata.get("strategy_type") is not None
    }
    return {
        "acquisition_strategy": strategy.strategy_type,
        "tasks": task_values,
        "manifest": dict(strategy.provider_execution_manifest or {}).get("strategy_type"),
    }


PLAIN = {
    "raw_user_request": "Lovable full roster",
    "query": "Lovable roster",
    "target_company": "Lovable",
    "categories": ["employee"],
    "employment_statuses": ["current"],
}
HARD_LARGE = {
    "raw_user_request": "Google DeepMind researchers",
    "query": "Google DeepMind roster",
    "target_company": "Google",
    "categories": ["employee"],
    "employment_statuses": ["current"],
}
FORMER_ONLY = {
    "raw_user_request": "Former Thinking Machines Lab people",
    "query": "TML former employees",
    "target_company": "Thinking Machines Lab",
    "categories": ["employee"],
    "employment_statuses": ["former"],
}
LARGE_PROFILE = {"org_scale_band": "large", "default_acquisition_mode": "scoped_search_roster"}


class StrategyTypeCrossSurfaceEqualityTest(unittest.TestCase):
    """The preflight proper: one value, every surface, no derivation drift."""

    def _assert_uniform(self, plan) -> str:
        readings = _strategy_readings(plan)
        expected = readings["acquisition_strategy"]
        self.assertTrue(expected)
        self.assertEqual(readings["manifest"], expected)
        for task_type, value in readings["tasks"].items():
            self.assertEqual(value, expected, f"task {task_type} disagrees with strategy")
        return str(expected)

    def test_plain_roster_reads_identically_everywhere(self) -> None:
        self.assertEqual(self._assert_uniform(_plan(PLAIN)), "full_company_roster")

    def test_former_only_reads_identically_everywhere(self) -> None:
        self._assert_uniform(_plan(FORMER_ONLY))

    def test_size_steered_plan_reads_identically_everywhere(self) -> None:
        self._assert_uniform(_plan(HARD_LARGE, LARGE_PROFILE))

    def test_plan_review_rebuild_preserves_the_uniform_value(self) -> None:
        plan = _plan(PLAIN)
        plan_payload = {
            "target_company": "Lovable",
            "acquisition_strategy": {
                "strategy_type": plan.acquisition_strategy.strategy_type,
                "company_scope": ["Lovable"],
                "filter_hints": {"current_companies": ["Lovable"]},
                "cost_policy": {},
                "search_channel_order": ["provider_people_search_api"],
                "search_seed_queries": [],
            },
            "publication_coverage": {"source_families": []},
            "acquisition_tasks": [
                {"task_type": "acquire_full_roster", "status": "ready", "metadata": {}}
            ],
        }
        _req, updated = apply_plan_review_decision(dict(PLAIN), plan_payload, {})
        metadata = updated["acquisition_tasks"][0]["metadata"]
        self.assertEqual(metadata["strategy_type"], plan.acquisition_strategy.strategy_type)


class UnificationFlipTargetPinsTest(unittest.TestCase):
    """CURRENT contract pins. Each MUST be flipped by its named batch — if one
    of these fails, either the unification step just landed (update the pin in
    the same change) or strategy selection regressed (investigate)."""

    def test_PIN_step3_org_size_still_steers_strategy(self) -> None:
        # WS1 Step 3 flip target: after size-steering retirement this SAME
        # request must stay full_company_roster and the profile may only tune
        # shard parameters (paging/probe budgets), never the strategy.
        without_profile = _plan(HARD_LARGE)
        with_large_profile = _plan(HARD_LARGE, LARGE_PROFILE)
        self.assertEqual(without_profile.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(with_large_profile.acquisition_strategy.strategy_type, "scoped_search_roster")

    def test_PIN_step2_former_only_hijacks_roster_task_without_former_plan(self) -> None:
        # WS1 Step 2 flip target — PLANNING layer. Step 2a (2026-07-22)
        # already unified the EXECUTION dispatch: former_employee_search now
        # routes through _acquire_former_search_seed (per-function former
        # shard plan), pinned by test_request_scoped_roster_shards::
        # test_former_only_strategy_routes_to_the_former_lane_not_keyword_pool.
        # This pin holds the remaining PLANNING-layer facts: the former-only
        # request still hijacks the roster task's strategy_type and the plan
        # carries no per-function former shard metadata — Step 2b (schema/
        # merge unification, employment_status as a first-class shard
        # parameter) flips these.
        plan = _plan(FORMER_ONLY)
        self.assertEqual(plan.acquisition_strategy.strategy_type, "former_employee_search")
        roster_task = next(
            task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster"
        )
        self.assertEqual(roster_task.metadata.get("strategy_type"), "former_employee_search")
        self.assertNotIn("former_function_shard_plan", roster_task.metadata)
        self.assertEqual(
            [task.task_type for task in plan.acquisition_tasks if "former" in task.task_type],
            [],
        )


if __name__ == "__main__":
    unittest.main()
