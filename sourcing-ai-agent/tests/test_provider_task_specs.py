"""M2.1 golden + structural guard for the ProviderTaskSpec registry.

The golden sha1 snapshot pins the registry shape; a failure means a ProviderTaskSpec
changed — verify the change is intended, then update GOLDEN deliberately (do NOT
regenerate to make a red test pass). The structural assertions enforce the M2
invariants: every spec binds to a registered CommandTypeSpec; submit/readiness/retry
fields are from the closed value sets; the R1-refined axes (compound submit, retry
granularity, separated fallback axes) hold.
"""

import json
import unittest
from hashlib import sha1

from sourcing_agent import provider_task_runtime as ptr
from sourcing_agent.durable_runtime import (
    DEFAULT_COMMAND_TYPE_SPECS,
    PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL,
    PROVIDER_AFTER_START_CONTROL_MODE_NOT_APPLICABLE,
    PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
)

_AFTER_START_MODES = frozenset(
    {
        PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
        PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL,
        PROVIDER_AFTER_START_CONTROL_MODE_NOT_APPLICABLE,
    }
)


def _spec_sha1(spec: ptr.ProviderTaskSpec) -> str:
    payload = {
        "provider_task_type": spec.provider_task_type,
        "provider_family": spec.provider_family,
        "command_type": spec.command_type,
        "submit_primary_mode": spec.submit_primary_mode,
        "submit_fallback_mode": spec.submit_fallback_mode,
        "readiness_primary_signal": spec.readiness_primary_signal,
        "readiness_fallback_mechanism": spec.readiness_fallback_mechanism,
        "after_start_mode": spec.after_start_mode,
        "inflight_budget_key": spec.inflight_budget_key,
        "cost_budget_key": spec.cost_budget_key,
        "retry": {
            "granularity": spec.retry.granularity,
            "max_attempts": spec.retry.max_attempts,
            "backoff_seconds_key": spec.retry.backoff_seconds_key,
            "reshape_ladder_key": spec.retry.reshape_ladder_key,
            "retryable_classifier": spec.retry.retryable_classifier,
        },
        "identity_key_recipe": spec.identity_key_recipe,
        "provider_fallback_chain": list(spec.provider_fallback_chain),
        "terminal_admit_handler": spec.terminal_admit_handler,
    }
    return sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


# Golden sha1 per provider_task_type. Update deliberately when a spec intentionally changes.
GOLDEN_PROVIDER_TASK_SNAPSHOT = {
    "harvest.profile_batch": "c6b3b7d0f163fb2cb4a43988f3463aba2c48a587",
    "dataforseo.discovery_query": "77cf65762796884cb6c322ccba9991ce10be6633",
}


class ProviderTaskSpecGoldenTest(unittest.TestCase):
    def test_golden_snapshot_matches(self) -> None:
        observed = {k: _spec_sha1(v) for k, v in ptr.DEFAULT_PROVIDER_TASK_SPECS.items()}
        self.assertEqual(observed, GOLDEN_PROVIDER_TASK_SNAPSHOT)

    def test_registry_count(self) -> None:
        self.assertEqual(len(ptr.DEFAULT_PROVIDER_TASK_SPECS), 2)


class ProviderTaskSpecStructuralTest(unittest.TestCase):
    def test_every_command_type_is_registered(self) -> None:
        self.assertEqual(ptr.unregistered_command_type_bindings(), [])

    def test_keys_match_provider_task_type(self) -> None:
        for key, spec in ptr.DEFAULT_PROVIDER_TASK_SPECS.items():
            self.assertEqual(key, spec.provider_task_type)

    def test_closed_value_sets(self) -> None:
        for spec in ptr.DEFAULT_PROVIDER_TASK_SPECS.values():
            self.assertIn(spec.submit_primary_mode, ptr._SUBMIT_PRIMARY_MODES, spec.provider_task_type)
            self.assertIn(spec.submit_fallback_mode, ptr._SUBMIT_FALLBACK_MODES, spec.provider_task_type)
            self.assertIn(spec.readiness_primary_signal, ptr._READINESS_PRIMARY_SIGNALS, spec.provider_task_type)
            self.assertIn(
                spec.readiness_fallback_mechanism, ptr._READINESS_FALLBACK_MECHANISMS, spec.provider_task_type
            )
            self.assertIn(spec.retry.granularity, ptr._RETRY_GRANULARITIES, spec.provider_task_type)
            self.assertIn(spec.after_start_mode, _AFTER_START_MODES, spec.provider_task_type)

    def test_work_reshape_requires_reshape_ladder(self) -> None:
        for spec in ptr.DEFAULT_PROVIDER_TASK_SPECS.values():
            if spec.retry.granularity == ptr.RETRY_GRANULARITY_WORK_RESHAPE:
                self.assertTrue(spec.retry.reshape_ladder_key, spec.provider_task_type)
            else:
                self.assertEqual(spec.retry.reshape_ladder_key, "", spec.provider_task_type)

    def test_command_types_bound_are_provider_attempt(self) -> None:
        for spec in ptr.DEFAULT_PROVIDER_TASK_SPECS.values():
            cmd = DEFAULT_COMMAND_TYPE_SPECS[spec.command_type]
            self.assertIn("provider_attempt", cmd.running_control_categories, spec.command_type)

    # ── R1 spike fidelity: the two seeded providers exercise the refined axes ──
    def test_harvest_is_compound_submit_and_work_reshape(self) -> None:
        spec = ptr.DEFAULT_PROVIDER_TASK_SPECS["harvest.profile_batch"]
        self.assertEqual(spec.submit_primary_mode, ptr.SUBMIT_MODE_SYNC_RUN)
        self.assertEqual(spec.submit_fallback_mode, ptr.SUBMIT_MODE_ASYNC_SUBMIT_POLL)
        self.assertEqual(spec.retry.granularity, ptr.RETRY_GRANULARITY_WORK_RESHAPE)

    def test_dataforseo_is_batch_item_retry_direct_probe(self) -> None:
        spec = ptr.DEFAULT_PROVIDER_TASK_SPECS["dataforseo.discovery_query"]
        self.assertEqual(spec.submit_primary_mode, ptr.SUBMIT_MODE_BATCH_SUBMIT_POLL_FETCH)
        self.assertEqual(spec.retry.granularity, ptr.RETRY_GRANULARITY_ITEM)
        self.assertEqual(spec.readiness_fallback_mechanism, ptr.READINESS_FALLBACK_DIRECT_PROBE)


if __name__ == "__main__":
    unittest.main()
