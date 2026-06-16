"""M2.3 characterization — pin the provider budget/backpressure surface that the
future ProviderScheduler fold MUST preserve verbatim (invariants I3 + I6 from
docs/M2_PROVIDER_TASK_RUNTIME_DESIGN.md §4).

These are pure-function pins, zero behavior change. A failure means a per-lane
inflight budget default, the env-override priority, or a backpressure
recommended_action changed — verify the change is intended before updating.
"""

import os
import unittest
from unittest.mock import patch

from sourcing_agent import runtime_tuning as rt

_HARVEST_ENV_PREFIX = "SOURCING_HARVEST"


def _clear_harvest_env() -> None:
    for key in list(os.environ):
        if key.startswith(_HARVEST_ENV_PREFIX):
            os.environ.pop(key, None)


class PerLaneInflightBudgetDefaultsTest(unittest.TestCase):
    """I3: per-lane inflight budget defaults (no env, no request context)."""

    def setUp(self) -> None:
        patcher = patch.dict(os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        _clear_harvest_env()

    def test_defaults(self) -> None:
        self.assertEqual(rt.resolved_harvest_profile_actor_global_inflight(None), 4)
        self.assertEqual(rt.resolved_harvest_profile_scrape_global_inflight(None), 4)
        self.assertEqual(rt.resolved_harvest_profile_batch_submit_global_inflight(None), 1)
        self.assertEqual(rt.resolved_harvest_people_search_global_inflight(None), 4)
        self.assertEqual(rt.resolved_harvest_company_roster_global_inflight(None), 4)


class InflightBudgetPriorityTest(unittest.TestCase):
    """I3: resolution priority is request_context > generic > specific env > generic env > default."""

    def setUp(self) -> None:
        patcher = patch.dict(os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        _clear_harvest_env()

    def test_specific_env_overrides_default(self) -> None:
        os.environ["SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT"] = "7"
        self.assertEqual(rt.resolved_harvest_profile_actor_global_inflight(None), 7)

    def test_generic_harvest_env_applies_to_harvest_keys(self) -> None:
        os.environ["SOURCING_HARVEST_GLOBAL_INFLIGHT_BUDGET"] = "9"
        # no specific env for people_search -> generic harvest env applies
        self.assertEqual(rt.resolved_harvest_people_search_global_inflight(None), 9)

    def test_specific_env_beats_generic_env(self) -> None:
        os.environ["SOURCING_HARVEST_GLOBAL_INFLIGHT_BUDGET"] = "9"
        os.environ["SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT"] = "6"
        self.assertEqual(rt.resolved_harvest_profile_actor_global_inflight(None), 6)

    def test_request_context_beats_env(self) -> None:
        os.environ["SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT"] = "7"
        self.assertEqual(
            rt.resolved_harvest_profile_actor_global_inflight({"harvest_profile_actor_global_inflight": 3}), 3
        )


class BackpressureReportTest(unittest.TestCase):
    """I6: build_provider_backpressure_budget_report recommended_action transitions."""

    def setUp(self) -> None:
        patcher = patch.dict(os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        _clear_harvest_env()

    def test_within_budget(self) -> None:
        report = rt.build_provider_backpressure_budget_report(None)
        self.assertEqual(report["recommended_action"], "within_budget")
        self.assertFalse(report["backpressure_detected"])
        # the five per-lane budgets are surfaced at their defaults
        self.assertEqual(report["budgets"]["harvest_profile_actor_global_inflight"], 4)
        self.assertEqual(report["budgets"]["harvest_profile_batch_submit_global_inflight"], 1)

    def test_watch_provider_tail_on_backlog(self) -> None:
        report = rt.build_provider_backpressure_budget_report(None, queued_provider_worker_count=2)
        self.assertEqual(report["recommended_action"], "watch_provider_tail")
        self.assertTrue(report["backpressure_detected"])
        self.assertEqual(report["provider_worker_backlog_count"], 2)

    def test_waiting_remote_also_counts_as_backlog(self) -> None:
        report = rt.build_provider_backpressure_budget_report(None, waiting_remote_harvest_count=1)
        self.assertEqual(report["recommended_action"], "watch_provider_tail")

    def test_throttle_on_exhausted_limiter(self) -> None:
        report = rt.build_provider_backpressure_budget_report(
            None,
            observed_limiter_slots=[{"limiter_key": "harvest_profile_scraper_actor", "budget": 4, "active_count": 4}],
        )
        self.assertEqual(report["recommended_action"], "throttle_provider_submit")
        self.assertTrue(report["limiter_exhausted"])
        self.assertEqual(report["limiter_exhausted_keys"], ["harvest_profile_scraper_actor"])

    def test_exhausted_limiter_takes_precedence_over_backlog(self) -> None:
        report = rt.build_provider_backpressure_budget_report(
            None,
            queued_provider_worker_count=5,
            observed_limiter_slots=[{"limiter_key": "harvest_company_employees_actor", "budget": 4, "active_count": 4}],
        )
        self.assertEqual(report["recommended_action"], "throttle_provider_submit")


if __name__ == "__main__":
    unittest.main()
