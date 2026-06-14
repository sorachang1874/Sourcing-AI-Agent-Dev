"""Recovery-tick domain-drain characterization and drain-binding registry tests.

The characterization tests in this module pin the ``run_worker_recovery_once``
domain-drain contract exactly as observed BEFORE the drain-binding registry
rebind: the uniform flag-gated drain block invokes sixteen domain drains in a
fixed order, passes every drain the same tick payload object, records each
result under a fixed ``recovery_phase_metrics`` phase key with a fixed owner
label, exposes fifteen of the sixteen results under the same key in the
returned recovery summary (``crm_writer_command_owner`` is executed and
metered but intentionally absent from the returned summary), skips each drain
with a pinned ``<phase>_disabled_by_payload`` reason when its payload flag is
false, and propagates drain exceptions without running later drains.

These tests must stay green across the registry rebind and Phase 4 recovery
work; they are the contract for "the ONLY change is how drains are looked up".
"""

from __future__ import annotations

import dataclasses
import tempfile
import unittest
from pathlib import Path
from typing import Any, NamedTuple

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.recovery_drain_registry import (
    DEFAULT_RECOVERY_DRAIN_BINDINGS,
    RecoveryDrainBinding,
    build_recovery_drain_registry,
)
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class CharacterizedDrainPhase(NamedTuple):
    """One row of the audited uniform drain block in run_worker_recovery_once."""

    phase: str
    payload_flag: str
    drain_method: str
    disabled_reason: str
    owner: str
    in_result: bool


# The audited contract: exact call order, payload flag, facade wrapper name,
# skip reason, recovery-phase owner label, and returned-summary visibility of
# every domain drain the recovery tick invokes through its uniform flag-gated
# drain block (orchestrator.py, run_worker_recovery_once).
CHARACTERIZED_DRAIN_PHASES: tuple[CharacterizedDrainPhase, ...] = (
    CharacterizedDrainPhase(
        "crm_public_web_queue_batch",
        "crm_public_web_queue_batch_enabled",
        "_drain_crm_public_web_queue_batch_commands",
        "crm_public_web_queue_batch_disabled_by_payload",
        "crm_public_web_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "crm_public_web_phase_commands",
        "crm_public_web_phase_commands_enabled",
        "_drain_crm_public_web_phase_commands",
        "crm_public_web_phase_commands_disabled_by_payload",
        "crm_public_web_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "company_public_web_refresh_command_owner",
        "company_public_web_refresh_command_owner_enabled",
        "_drain_company_public_web_refresh_commands",
        "company_public_web_refresh_command_owner_disabled_by_payload",
        "company_public_web_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "company_logo_profile_experience_discover_command_owner",
        "company_logo_profile_experience_discover_command_owner_enabled",
        "_drain_company_logo_profile_experience_discover_commands",
        "company_logo_profile_experience_discover_command_owner_disabled_by_payload",
        "company_asset_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "media_asset_cache_command_owner",
        "media_asset_cache_command_owner_enabled",
        "_drain_media_asset_cache_commands",
        "media_asset_cache_command_owner_disabled_by_payload",
        "media_asset_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_run_create_command_owner",
        "acquisition_run_create_command_owner_enabled",
        "_drain_acquisition_run_create_commands",
        "acquisition_run_create_command_owner_disabled_by_payload",
        "acquisition_run_writer",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_intent_resolve_command_owner",
        "acquisition_intent_resolve_command_owner_enabled",
        "_drain_acquisition_intent_resolve_commands",
        "acquisition_intent_resolve_command_owner_disabled_by_payload",
        "acquisition_planner",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_plan_build_command_owner",
        "acquisition_plan_build_command_owner_enabled",
        "_drain_acquisition_plan_build_commands",
        "acquisition_plan_build_command_owner_disabled_by_payload",
        "acquisition_planner",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_plan_review_request_command_owner",
        "acquisition_plan_review_request_command_owner_enabled",
        "_drain_acquisition_plan_review_request_commands",
        "acquisition_plan_review_request_command_owner_disabled_by_payload",
        "acquisition_planner",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_plan_commit_command_owner",
        "acquisition_plan_commit_command_owner_enabled",
        "_drain_acquisition_plan_commit_commands",
        "acquisition_plan_commit_command_owner_disabled_by_payload",
        "acquisition_planner",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_probe_command_owner",
        "acquisition_probe_command_owner_enabled",
        "_drain_acquisition_probe_commands",
        "acquisition_probe_command_owner_disabled_by_payload",
        "acquisition_probe_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "acquisition_scale_plan_command_owner",
        "acquisition_scale_plan_command_owner_enabled",
        "_drain_acquisition_scale_plan_commands",
        "acquisition_scale_plan_command_owner_disabled_by_payload",
        "acquisition_scale_planner",
        True,
    ),
    CharacterizedDrainPhase(
        "operation_native_discovery_activity_owner",
        "operation_native_discovery_activity_owner_enabled",
        "_drain_operation_native_discovery_activity_commands",
        "operation_native_discovery_activity_owner_disabled_by_payload",
        "linkedin_acquisition_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "operation_native_profile_fetch_activity_owner",
        "operation_native_profile_fetch_activity_owner_enabled",
        "_drain_operation_native_profile_fetch_activity_commands",
        "operation_native_profile_fetch_activity_owner_disabled_by_payload",
        "linkedin_profile_activity_owner",
        True,
    ),
    CharacterizedDrainPhase(
        "operation_native_projection_admission_owner",
        "operation_native_projection_admission_owner_enabled",
        "_drain_operation_native_projection_admission_commands",
        "operation_native_projection_admission_owner_disabled_by_payload",
        "serving_projection_owner",
        True,
    ),
    # C1: export.projection.generate worker drain (additive binding before
    # crm_writer); summary-visible like the other registry drains.
    CharacterizedDrainPhase(
        "export_projection_generate_command_owner",
        "export_projection_generate_command_owner_enabled",
        "_drain_export_projection_generate_commands",
        "export_projection_generate_command_owner_disabled_by_payload",
        "projection_exporter",
        True,
    ),
    # Executed and metered like every other drain, but its result is
    # intentionally absent from the returned recovery summary and from the
    # tick-level phase-budget scan (audited pre-registry behavior).
    CharacterizedDrainPhase(
        "crm_writer_command_owner",
        "crm_writer_command_owner_enabled",
        "_drain_crm_writer_commands",
        "crm_writer_command_owner_disabled_by_payload",
        "crm_writer",
        False,
    ),
)

# The two CRM Public Web drain phases stay as named calls inside
# run_worker_recovery_once: tests/test_crm_public_web_runtime_boundary.py pins
# their literal ``callback=lambda: self._drain_...`` source text and is outside
# this change's assigned files. The registry owns the remaining fourteen.
NAMED_CALL_DRAIN_PHASES: frozenset[str] = frozenset(
    {
        "crm_public_web_queue_batch",
        "crm_public_web_phase_commands",
    }
)

# Quiets every non-drain recovery phase that can be disabled by payload so the
# characterization run exercises the drain block against an empty store.
QUIET_RECOVERY_PAYLOAD: dict[str, Any] = {
    "search_seed_discovery_enabled": False,
    "profile_prefetch_refill_enabled": False,
    "profile_refill_command_owner_enabled": False,
    "profile_url_terminal_record_command_owner_enabled": False,
    "stage1_preview_recovery_enabled": False,
    "post_completion_reconcile_enabled": False,
    "snapshot_full_materialization_enabled": False,
    "projection_facet_layering_enabled": False,
    "projection_person_search_index_enabled": False,
    "collection_authoritative_merge_enabled": False,
    "excel_intake_recovery_enabled": False,
    "post_recovery_housekeeping_enabled": False,
}


class RecoveryTickDrainCharacterizationTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "runtime.db")
        self.catalog = AssetCatalog.discover()
        self.settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "providers.local.json",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            db_path=self.runtime_dir / "runtime.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.acquisition_engine = AcquisitionEngine(
            self.catalog,
            self.settings,
            self.store,
            self.model_client,
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.settings.jobs_dir,
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def _install_drain_recorders(self) -> list[dict[str, Any]]:
        calls: list[dict[str, Any]] = []
        for index, row in enumerate(CHARACTERIZED_DRAIN_PHASES):

            def recorder(
                payload: dict[str, Any] | None = None,
                *,
                _phase: str = row.phase,
                _method: str = row.drain_method,
                _index: int = index,
            ) -> dict[str, Any]:
                calls.append({"phase": _phase, "method": _method, "payload": payload})
                return {
                    "status": "completed",
                    "reason": "",
                    "claimed_count": _index + 1,
                    "executed_count": 1,
                    "characterized_phase": _phase,
                }

            setattr(self.orchestrator, row.drain_method, recorder)
        return calls

    def test_recovery_tick_invokes_domain_drains_in_characterized_order_with_tick_payload(self) -> None:
        calls = self._install_drain_recorders()

        result = self.orchestrator.run_worker_recovery_once(dict(QUIET_RECOVERY_PAYLOAD))

        # Exact call order and exact wrapper names.
        self.assertEqual(
            [call["method"] for call in calls],
            [row.drain_method for row in CHARACTERIZED_DRAIN_PHASES],
        )
        # Every drain receives the same tick payload object as its single
        # positional argument, carrying the caller's payload keys.
        payload_ids = {id(call["payload"]) for call in calls}
        self.assertEqual(len(payload_ids), 1)
        for key, value in QUIET_RECOVERY_PAYLOAD.items():
            self.assertEqual(calls[0]["payload"][key], value)

        # Each drain result lands in the returned summary under its phase key,
        # except crm_writer_command_owner which is executed but not returned.
        for index, row in enumerate(CHARACTERIZED_DRAIN_PHASES):
            if row.in_result:
                self.assertEqual(result[row.phase]["characterized_phase"], row.phase)
                self.assertEqual(result[row.phase]["claimed_count"], index + 1)
            else:
                self.assertNotIn(row.phase, result)

        # Phase metrics pin owner label, status, counts, and insertion order.
        metrics = dict(result["recovery_phase_metrics"])
        for index, row in enumerate(CHARACTERIZED_DRAIN_PHASES):
            entry = dict(metrics[row.phase])
            self.assertEqual(entry["phase"], row.phase)
            self.assertEqual(entry["owner"], row.owner)
            self.assertEqual(entry["status"], "completed")
            self.assertEqual(dict(entry["counts"])["claimed_count"], index + 1)
            self.assertEqual(dict(entry["counts"])["executed_count"], 1)
            self.assertTrue(str(entry["max_sync_work"]).strip())
        phase_names = {row.phase for row in CHARACTERIZED_DRAIN_PHASES}
        self.assertEqual(
            [name for name in metrics if name in phase_names],
            [row.phase for row in CHARACTERIZED_DRAIN_PHASES],
        )

        # Drain results returning no budget-exhaustion markers must not flag
        # the tick-level phase-budget scan.
        self.assertFalse(result["phase_budget_exhausted"])

    def test_recovery_tick_skips_each_drain_with_pinned_reason_when_flag_disabled(self) -> None:
        calls = self._install_drain_recorders()
        payload = dict(QUIET_RECOVERY_PAYLOAD)
        for row in CHARACTERIZED_DRAIN_PHASES:
            payload[row.payload_flag] = False

        result = self.orchestrator.run_worker_recovery_once(payload)

        self.assertEqual(calls, [])
        metrics = dict(result["recovery_phase_metrics"])
        for row in CHARACTERIZED_DRAIN_PHASES:
            if row.in_result:
                self.assertEqual(
                    result[row.phase],
                    {"status": "skipped", "reason": row.disabled_reason},
                )
            else:
                self.assertNotIn(row.phase, result)
            entry = dict(metrics[row.phase])
            self.assertEqual(entry["owner"], row.owner)
            self.assertEqual(entry["status"], "skipped")
            self.assertEqual(entry["reason"], row.disabled_reason)

    def test_recovery_tick_propagates_drain_exception_without_running_later_drains(self) -> None:
        calls = self._install_drain_recorders()
        first = CHARACTERIZED_DRAIN_PHASES[0]

        def boom(payload: dict[str, Any] | None = None) -> dict[str, Any]:
            raise RuntimeError("characterized_drain_failure")

        setattr(self.orchestrator, first.drain_method, boom)

        with self.assertRaises(RuntimeError):
            self.orchestrator.run_worker_recovery_once(dict(QUIET_RECOVERY_PAYLOAD))

        self.assertEqual(calls, [])

    def test_phase_budget_scan_sees_summary_drains_but_not_crm_writer(self) -> None:
        # Audited pre-registry behavior: a budget-exhausted reason from any
        # summary-visible drain flips the tick-level phase_budget_exhausted
        # flag, while crm_writer_command_owner is outside that scan.
        def exhausted(payload: dict[str, Any] | None = None) -> dict[str, Any]:
            return {"status": "completed", "reason": "drain_phase_budget_exhausted"}

        self._install_drain_recorders()
        setattr(self.orchestrator, "_drain_media_asset_cache_commands", exhausted)
        in_scan_result = self.orchestrator.run_worker_recovery_once(dict(QUIET_RECOVERY_PAYLOAD))

        self._install_drain_recorders()
        setattr(self.orchestrator, "_drain_crm_writer_commands", exhausted)
        out_of_scan_result = self.orchestrator.run_worker_recovery_once(dict(QUIET_RECOVERY_PAYLOAD))

        self.assertTrue(in_scan_result["phase_budget_exhausted"])
        self.assertFalse(out_of_scan_result["phase_budget_exhausted"])

    def test_orchestrator_builds_registry_in_default_binding_order(self) -> None:
        registry = self.orchestrator._recovery_drain_registry  # noqa: SLF001

        self.assertEqual(registry, DEFAULT_RECOVERY_DRAIN_BINDINGS)
        for binding in registry:
            self.assertTrue(callable(getattr(self.orchestrator, binding.drain_method)))


class RecoveryDrainRegistryUnitTest(unittest.TestCase):
    def test_default_bindings_match_characterized_registry_rows_in_order(self) -> None:
        expected_rows = [
            row for row in CHARACTERIZED_DRAIN_PHASES if row.phase not in NAMED_CALL_DRAIN_PHASES
        ]

        self.assertEqual(
            [binding.phase for binding in DEFAULT_RECOVERY_DRAIN_BINDINGS],
            [row.phase for row in expected_rows],
        )
        for binding, row in zip(DEFAULT_RECOVERY_DRAIN_BINDINGS, expected_rows):
            self.assertEqual(binding.payload_flag, row.payload_flag)
            self.assertEqual(binding.drain_method, row.drain_method)
            self.assertEqual(binding.disabled_reason, row.disabled_reason)
            self.assertEqual(binding.owner, row.owner)
            self.assertEqual(binding.include_in_result, row.in_result)
            # Audited coupling: the only summary-invisible drain
            # (crm_writer_command_owner) is also the only one excluded from
            # the tick-level phase-budget scan.
            self.assertEqual(binding.include_in_phase_budget_scan, row.in_result)
            self.assertTrue(binding.max_sync_work.strip())
            self.assertTrue(binding.skipped_max_sync_work.strip())

    def test_binding_records_are_frozen(self) -> None:
        binding = DEFAULT_RECOVERY_DRAIN_BINDINGS[0]

        with self.assertRaises(dataclasses.FrozenInstanceError):
            binding.phase = "mutated"  # type: ignore[misc]

    def test_duplicate_phase_keys_fail_loudly(self) -> None:
        binding = DEFAULT_RECOVERY_DRAIN_BINDINGS[0]
        duplicate = dataclasses.replace(binding, payload_flag="other_flag_enabled")

        with self.assertRaisesRegex(ValueError, "duplicate recovery drain phase"):
            build_recovery_drain_registry((binding, duplicate))

    def test_duplicate_payload_flags_fail_loudly(self) -> None:
        binding = DEFAULT_RECOVERY_DRAIN_BINDINGS[0]
        duplicate = dataclasses.replace(binding, phase="other_phase")

        with self.assertRaisesRegex(ValueError, "duplicate recovery drain payload flag"):
            build_recovery_drain_registry((binding, duplicate))

    def test_empty_identity_fields_fail_loudly(self) -> None:
        for field_name in ("phase", "owner", "payload_flag", "disabled_reason", "drain_method"):
            broken = dataclasses.replace(DEFAULT_RECOVERY_DRAIN_BINDINGS[0], **{field_name: "  "})
            with self.assertRaisesRegex(ValueError, f"empty {field_name}"):
                build_recovery_drain_registry((broken,))

    def test_empty_registry_fails_loudly(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not be empty"):
            build_recovery_drain_registry(())

    def test_non_binding_entries_fail_loudly(self) -> None:
        with self.assertRaises(TypeError):
            build_recovery_drain_registry(({"phase": "not_a_binding"},))  # type: ignore[arg-type]

    def test_missing_drain_method_on_host_fails_loudly(self) -> None:
        class _Host:
            def _drain_known(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
                return {}

        known = RecoveryDrainBinding(
            phase="known_phase",
            owner="known_owner",
            payload_flag="known_phase_enabled",
            disabled_reason="known_phase_disabled_by_payload",
            drain_method="_drain_known",
            max_sync_work="known sync work",
            skipped_max_sync_work="no known work",
        )
        unknown = dataclasses.replace(
            known,
            phase="unknown_phase",
            payload_flag="unknown_phase_enabled",
            drain_method="_drain_unknown",
        )

        self.assertEqual(build_recovery_drain_registry((known,), drain_host=_Host()), (known,))
        with self.assertRaisesRegex(ValueError, "missing drain method"):
            build_recovery_drain_registry((known, unknown), drain_host=_Host())


if __name__ == "__main__":
    unittest.main()
