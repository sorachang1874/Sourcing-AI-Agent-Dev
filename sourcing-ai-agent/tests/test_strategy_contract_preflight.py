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

    def test_step4a_scoped_request_plan_mints_keyword_union_policy(self) -> None:
        # WS1 Step 4a (2026-07-22): a scoped request's roster task carries the
        # request-scoped keyword_union shard policy — the reviewable plan-time
        # contract for the Step 4b execution cutover (design lineage: 233a31a
        # + tombstone T-001). Until 4b lands, execution still runs the
        # seed-pool path; this pin guards the migration target's shape.
        planned = _plan(
            {
                "raw_user_request": "OpenAI Pre-train direction people",
                "query": "OpenAI Pre-train people",
                "target_company": "OpenAI",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["Pre-train"],
            }
        )
        self.assertEqual(planned.acquisition_strategy.strategy_type, "scoped_search_roster")
        roster_task = next(
            task for task in planned.acquisition_tasks if task.task_type == "acquire_full_roster"
        )
        policy = dict(roster_task.metadata.get("scoped_keyword_union_shard_policy") or {})
        self.assertEqual(policy.get("mode"), "keyword_union")
        self.assertEqual(policy.get("strategy_id"), "request_scoped_keyword_union")
        rule_ids = [str(item.get("rule_id") or "") for item in list(policy.get("keyword_shards") or [])]
        self.assertIn("kw_pre_train", rule_ids)
        self.assertTrue(policy.get("allow_overflow_partial"))

    def test_step3_org_size_never_steers_strategy(self) -> None:
        # FLIPPED 2026-07-22 (WS1 Step 3): the org-profile steering branches
        # are retired — the SAME request yields the SAME strategy with or
        # without a large-band profile; the profile survives only as advisory
        # shard-parameter metadata. A directional query is scoped because of
        # the QUERY shape (fallback_rule_directional_scoped), never the size.
        without_profile = _plan(HARD_LARGE)
        with_large_profile = _plan(HARD_LARGE, LARGE_PROFILE)
        self.assertEqual(without_profile.acquisition_strategy.strategy_type, "full_company_roster")
        self.assertEqual(with_large_profile.acquisition_strategy.strategy_type, "full_company_roster")

        directional = {
            "raw_user_request": "Google multimodal researchers working on Veo",
            "query": "Google Veo multimodal researchers",
            "target_company": "Google",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Veo", "multimodal"],
        }
        small_directional = _plan(directional)
        large_directional = _plan(directional, LARGE_PROFILE)
        self.assertEqual(
            small_directional.acquisition_strategy.strategy_type,
            large_directional.acquisition_strategy.strategy_type,
        )

    def test_step2_former_only_plan_carries_the_per_function_former_shard_plan(self) -> None:
        # FLIPPED 2026-07-22 (WS1 Step 2a executor + 2b planning): a
        # former-only request now (a) routes execution through the per-function
        # former lane and (b) carries the request-scoped former shard plan in
        # the roster task metadata — the technical-default function ids
        # ['8','24'] each become one past-company shard with the plan-derived
        # marker, mirroring the full-roster companion seed. strategy_type
        # still reads former_employee_search on the roster task (task-shape
        # unification is Step 5 convergence scope).
        plan = _plan(FORMER_ONLY)
        self.assertEqual(plan.acquisition_strategy.strategy_type, "former_employee_search")
        roster_task = next(
            task for task in plan.acquisition_tasks if task.task_type == "acquire_full_roster"
        )
        former_plan = dict(roster_task.metadata.get("former_function_shard_plan") or {})
        self.assertEqual(sorted(former_plan.get("function_ids") or []), ["24", "8"])
        shards = list(former_plan.get("shards") or [])
        self.assertEqual(len(shards), 2)
        for shard in shards:
            hints = dict(shard.get("filter_hints") or {})
            self.assertEqual(len(hints.get("function_ids") or []), 1)
            self.assertTrue(hints.get("_former_function_shard_plan_derived"))
            self.assertTrue(hints.get("past_companies"))


if __name__ == "__main__":
    unittest.main()
