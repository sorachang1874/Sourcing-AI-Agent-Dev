"""WS7/W7.2 S4 suite: R6 durable wave-identity extension via `division_id`.

Provenance: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §4.3 (the honest gap: R6's
scalar wave identity cannot host heterogeneous AI batches) + §7 slice S4, OQ6
RATIFIED 2026-07-23, migration 0015_profile_refill_plan_division_id.

Pins the four S4 surfaces:
  1. WRITER — the plan-record moment persists `refill_plan_division_id` through
     `_record_profile_prefetch_batch_plan_items` (the shadow proposal's id when
     a VALIDATED shadow exists, empty otherwise), and the registry repository
     round-trips/clears it in lockstep with the scalar wave identity.
  2. R6 INHERITANCE — `_apply_durable_refill_wave_dispatch_window` carries the
     recorded division id exactly when the scalar identity claims the window;
     with absent/empty division ids the output stays BYTE-IDENTICAL to the
     characterization oracle's R6 goldens
     (test_fetch_profile_batch_characterization.py:387-467), re-driven here
     through the S4 code path.
  3. V6 — the live-division-id set read from the registry field is non-empty
     while a divided wave is in flight (integration with the shadow hook's
     apply-time battery).
  4. HETEROGENEOUS per-batch inheritance — pure-function goldens for
     `inherit_heterogeneous_division_windows` (windows keyed by division_id +
     batch ordinal), plus the structural pin that NO production caller passes
     an AI division pre-flip (the S5 flip is the only sanctioned caller).
"""

from __future__ import annotations

import inspect
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import sourcing_agent.enrichment as enrichment_module
from sourcing_agent.enrichment import (
    _apply_durable_refill_wave_dispatch_window,
    _build_profile_prefetch_batch_plan,
    _build_profile_prefetch_queue_items,
    _record_profile_prefetch_batch_plan_items,
)
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key
from sourcing_agent.model_provider import ScriptedProfileBatchDividerModelClient
from sourcing_agent.profile_batch_division import (
    DURABLE_WAVE_DIVISION_ID_WINDOW_KEY,
    DURABLE_WAVE_QUEUE_STATES,
    REGISTRY_DIVISION_ID_FIELD,
    inherit_heterogeneous_division_windows,
    inherited_division_wave_identity,
    live_division_ids_for_dispatch,
    record_profile_prefetch_division_shadow,
    shadow_plan_division_id,
)
from sourcing_agent.profile_batch_division_contract import (
    VALIDATOR_RESULT_STATUS_FAIL,
    VALIDATOR_RESULT_STATUS_PASS,
    VALIDATOR_V6_WAVE_MINT_ONLY,
    validate_v6_wave_mint_only,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin

_SIMULATE_ENV = {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}
_SCRIPTED_DIVIDER_ENV = {
    "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
    "SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER": "1",
}


def _urls(prefix: str, count: int) -> list[str]:
    return [f"https://www.linkedin.com/in/{prefix}-{i:04d}/" for i in range(count)]


def _budget(available: int, *, active: int = 0, reserved: int = 0, actor: int = 4, submit: int = 4) -> dict[str, int]:
    return {
        "submit_budget": submit,
        "actor_budget": actor,
        "active_worker_count": active,
        "scheduler_reserved_worker_count": reserved,
        "effective_active_worker_count": max(active, reserved),
        "available_new_worker_count": available,
    }


class ShadowPlanDivisionIdTest(unittest.TestCase):
    """Only a VALIDATED shadow proposal links its id to the actual wave."""

    def test_proposed_shadow_record_yields_its_division_id(self) -> None:
        record = {"shadow_status": "proposed", "division_id": "div-0001"}
        self.assertEqual(shadow_plan_division_id(record), "div-0001")

    def test_non_proposed_and_absent_records_yield_empty(self) -> None:
        cases = (
            None,
            {},
            # engaged fallback records carry a minted id, but a REJECTED
            # proposal must never become a persisted wave identity.
            {"shadow_status": "fallback", "division_id": "div-rejected"},
            {"shadow_status": "shadow_error"},
            {"shadow_status": "proposed", "division_id": ""},
        )
        for record in cases:
            with self.subTest(record=record):
                self.assertEqual(shadow_plan_division_id(record), "")


class ScalarWaveInheritanceCompatTest(unittest.TestCase):
    """The oracle's R6 golden variants, re-driven through the S4 code path.

    Absent/empty division ids (every pre-S4 row, every ladder-produced wave
    until the S5 flip) must behave byte-identically to the pinned goldens —
    the expected dicts below are verbatim copies of
    DurableWaveInheritanceCharacterizationTest's assertions.
    """

    RECOMPUTED_WINDOW = {
        "batch_size": 100,
        "max_workers": 4,
        "batch_count": 3,
        "source_mix": {"other": 300},
        "batch_size_reason": "single_durable_unit_ready_set",
    }

    @staticmethod
    def _registry(
        urls: list[str],
        *,
        batch_size: int,
        batch_count: int,
        window_count: int,
        state: str,
        division_id: str | None = None,
    ):
        entry_template: dict[str, Any] = {
            "refill_queue_state": state,
            "refill_plan_batch_size": batch_size,
            "refill_plan_batch_count": batch_count,
            "refill_plan_window_url_count": window_count,
        }
        if division_id is not None:
            entry_template[REGISTRY_DIVISION_ID_FIELD] = division_id
        return {normalize_linkedin_profile_url_key(u): dict(entry_template) for u in urls}

    def _resolve(self, urls: list[str], registry, *, append: bool = False, retry: bool = False):
        return _apply_durable_refill_wave_dispatch_window(
            dict(self.RECOMPUTED_WINDOW),
            dispatch_urls=urls,
            registry_entries=registry,
            append_trigger_replan=append,
            retry_isolated_refill=retry,
        )

    def test_larger_recorded_wave_claims_the_window_byte_identical(self) -> None:
        urls = _urls("durable", 300)
        for division_id in (None, ""):
            with self.subTest(division_id=division_id):
                registry = self._registry(
                    urls,
                    batch_size=232,
                    batch_count=8,
                    window_count=1856,
                    state="deferred_budget",
                    division_id=division_id,
                )
                self.assertEqual(
                    self._resolve(urls, registry),
                    {
                        "batch_size": 232,
                        "batch_count": 2,
                        "max_workers": 2,
                        "source_mix": {"other": 300},
                        "batch_size_contract": "profile_actor_slot_durable_wave_item_packing",
                        "batch_size_reason": "durable_refill_wave_batch_size",
                        "durable_refill_wave_batch_size": 232,
                        "durable_refill_wave_batch_count": 8,
                        "durable_refill_wave_item_count": 300,
                        "durable_refill_wave_window_url_count": 1856,
                    },
                )

    def test_equal_recorded_wave_also_claims_the_window_byte_identical(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(urls, batch_size=100, batch_count=3, window_count=300, state="dispatch_reserved")
        self.assertEqual(
            self._resolve(urls, registry),
            {
                "batch_size": 100,
                "batch_count": 3,
                "max_workers": 3,
                "source_mix": {"other": 300},
                "batch_size_contract": "profile_actor_slot_durable_wave_item_packing",
                "batch_size_reason": "durable_refill_wave_batch_size",
                "durable_refill_wave_batch_size": 100,
                "durable_refill_wave_batch_count": 3,
                "durable_refill_wave_item_count": 300,
                "durable_refill_wave_window_url_count": 300,
            },
        )

    def test_smaller_recorded_wave_keeps_the_recomputed_window(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(urls, batch_size=60, batch_count=2, window_count=120, state="deferred_budget")
        self.assertEqual(self._resolve(urls, registry), dict(self.RECOMPUTED_WINDOW))

    def test_append_replan_retry_isolation_and_empty_urls_never_inherit(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(urls, batch_size=232, batch_count=8, window_count=1856, state="deferred_budget")
        self.assertEqual(self._resolve(urls, registry, append=True), dict(self.RECOMPUTED_WINDOW))
        self.assertEqual(self._resolve(urls, registry, retry=True), dict(self.RECOMPUTED_WINDOW))
        self.assertEqual(self._resolve([], registry), dict(self.RECOMPUTED_WINDOW))


class DivisionIdInheritanceTest(ScalarWaveInheritanceCompatTest):
    """R6 carries the recorded division id exactly when the scalar wave claims.

    Subclasses the compat fixtures so the golden inputs stay single-sourced.
    """

    def test_claiming_wave_with_division_id_carries_it_additively(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(
            urls,
            batch_size=232,
            batch_count=8,
            window_count=1856,
            state="deferred_budget",
            division_id="div-wave-a",
        )
        resolved = self._resolve(urls, registry)
        self.assertEqual(resolved[DURABLE_WAVE_DIVISION_ID_WINDOW_KEY], "div-wave-a")
        baseline = self._resolve(
            urls,
            self._registry(
                urls,
                batch_size=232,
                batch_count=8,
                window_count=1856,
                state="deferred_budget",
            ),
        )
        without_key = {key: value for key, value in resolved.items() if key != DURABLE_WAVE_DIVISION_ID_WINDOW_KEY}
        self.assertEqual(without_key, baseline)

    def test_division_id_survives_a_simulated_durable_wave_continuation(self) -> None:
        # Tick 1: wave minted at 300 urls with a persisted division id claims a
        # recompute. Tick 2 (restart/next tick): only 68 urls remain in the
        # wave; the re-read registry rows still carry the same identity fields
        # — the id inherits exactly as the scalars do.
        urls = _urls("durable", 300)
        registry = self._registry(
            urls,
            batch_size=232,
            batch_count=8,
            window_count=1856,
            state="dispatch_claimed",
            division_id="div-wave-b",
        )
        first = self._resolve(urls, registry)
        self.assertEqual(first[DURABLE_WAVE_DIVISION_ID_WINDOW_KEY], "div-wave-b")
        remaining = urls[232:]
        second = self._resolve(remaining, registry)
        self.assertEqual(second[DURABLE_WAVE_DIVISION_ID_WINDOW_KEY], "div-wave-b")
        self.assertEqual(second["durable_refill_wave_batch_size"], 232)
        self.assertEqual(second["durable_refill_wave_item_count"], len(remaining))

    def test_non_claiming_smaller_wave_never_carries_the_id(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(
            urls,
            batch_size=60,
            batch_count=2,
            window_count=120,
            state="deferred_budget",
            division_id="div-small",
        )
        self.assertEqual(self._resolve(urls, registry), dict(self.RECOMPUTED_WINDOW))

    def test_append_retry_and_empty_never_carry_the_id(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(
            urls,
            batch_size=232,
            batch_count=8,
            window_count=1856,
            state="deferred_budget",
            division_id="div-never",
        )
        self.assertEqual(self._resolve(urls, registry, append=True), dict(self.RECOMPUTED_WINDOW))
        self.assertEqual(self._resolve(urls, registry, retry=True), dict(self.RECOMPUTED_WINDOW))
        self.assertEqual(self._resolve([], registry), dict(self.RECOMPUTED_WINDOW))

    def test_live_id_scan_mirrors_the_scalar_wave_state_filter(self) -> None:
        urls = _urls("scan", 4)
        keys = [normalize_linkedin_profile_url_key(url) for url in urls]
        registry = {
            keys[0]: {"refill_queue_state": "dispatch_reserved", REGISTRY_DIVISION_ID_FIELD: "div-live"},
            keys[1]: {"refill_queue_state": "retry_wait", REGISTRY_DIVISION_ID_FIELD: "div-retry"},
            keys[2]: {"refill_queue_state": "", REGISTRY_DIVISION_ID_FIELD: "div-closed"},
            keys[3]: {"refill_queue_state": "deferred_budget", REGISTRY_DIVISION_ID_FIELD: ""},
        }
        self.assertEqual(live_division_ids_for_dispatch(dispatch_urls=urls, registry_entries=registry), {"div-live"})
        self.assertNotIn("retry_wait", DURABLE_WAVE_QUEUE_STATES)

    def test_multiple_distinct_ids_carry_the_lexicographically_first(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(
            urls,
            batch_size=232,
            batch_count=8,
            window_count=1856,
            state="deferred_budget",
            division_id="div-zz",
        )
        first_key = normalize_linkedin_profile_url_key(urls[0])
        registry[first_key][REGISTRY_DIVISION_ID_FIELD] = "div-aa"
        resolved = self._resolve(urls, registry)
        self.assertEqual(resolved[DURABLE_WAVE_DIVISION_ID_WINDOW_KEY], "div-aa")
        self.assertEqual(
            inherited_division_wave_identity(dispatch_urls=urls, registry_entries=registry),
            {DURABLE_WAVE_DIVISION_ID_WINDOW_KEY: "div-aa"},
        )


class _RecordingRefillRepo:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def record_refill_plan_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(dict(kwargs))
        return {
            "status": "recorded",
            "item_store": "linkedin_profile_registry",
            "active_item_count": len(list(kwargs.get("active_profile_urls") or [])),
            "deferred_item_count": len(list(kwargs.get("deferred_profile_urls") or [])),
        }


class _RecordingStore:
    def __init__(self) -> None:
        self.repo = _RecordingRefillRepo()

    @property
    def repos(self) -> Any:
        store = self

        class _Repos:
            linkedin_profile_registry = store.repo

        return _Repos()


class WriterMintPersistenceTest(unittest.TestCase):
    """The plan-record moment threads the division id into EVERY recorder write."""

    def setUp(self) -> None:
        patcher = mock.patch.dict("os.environ", _SIMULATE_ENV, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _plan(self, url_count: int, *, available: int):
        urls = _urls("mint", url_count)
        items = _build_profile_prefetch_queue_items(urls, source_shards_by_url={}, priority=False, queue_state="ready")
        return _build_profile_prefetch_batch_plan(
            dispatch_urls=urls,
            requested_url_count=url_count,
            candidate_count=url_count,
            priority=False,
            source_shards_by_url={},
            worker_budget=_budget(available),
            queue_items=items,
        )

    def test_division_id_reaches_every_recorder_write_of_the_wave(self) -> None:
        store = _RecordingStore()
        plan = self._plan(400, available=1)  # active chunk(s) + worker-budget deferred remainder
        result = _record_profile_prefetch_batch_plan_items(
            store,
            plan,
            refill_plan_division_id="div-mint-1",
        )
        self.assertGreaterEqual(len(store.repo.calls), 2)
        self.assertTrue(any(call.get("active_profile_urls") for call in store.repo.calls))
        self.assertTrue(any(call.get("deferred_profile_urls") for call in store.repo.calls))
        for call in store.repo.calls:
            self.assertEqual(call.get("refill_plan_division_id"), "div-mint-1")
        self.assertEqual(result.get("status"), "recorded")

    def test_default_mint_persists_empty_division_id(self) -> None:
        store = _RecordingStore()
        plan = self._plan(120, available=4)
        _record_profile_prefetch_batch_plan_items(store, plan)
        self.assertTrue(store.repo.calls)
        for call in store.repo.calls:
            self.assertEqual(call.get("refill_plan_division_id"), "")


class HeterogeneousInheritancePureFunctionTest(unittest.TestCase):
    """Per-batch windows keyed by division_id + batch ordinal (S5-only claim)."""

    INVENTORY = [f"inventory-key-{i:02d}" for i in range(10)]
    DIVISION = {
        "division_id": "div-hetero",
        "batches": [
            {
                "batch_index": 1,
                "member_index_ranges": [[0, 2], [7, 9]],
                "member_count": 6,
                "reason": "roster-heavy cohort",
                "reason_code": "ai_division",
            },
            {
                "batch_index": 2,
                "member_index_ranges": [[3, 6]],
                "member_count": 4,
                "reason": "same source shard",
                "reason_code": "ai_division",
            },
        ],
    }

    def test_full_dispatch_set_claims_wholesale_with_per_batch_windows(self) -> None:
        result = inherit_heterogeneous_division_windows(
            division=self.DIVISION,
            inventory_url_keys=self.INVENTORY,
            dispatch_url_keys=self.INVENTORY,
        )
        self.assertEqual(
            result,
            {
                "division_id": "div-hetero",
                "claimed": True,
                "batch_windows": [
                    {
                        "division_id": "div-hetero",
                        "batch_index": 1,
                        "recorded_member_count": 6,
                        "remaining_member_count": 6,
                        "member_url_keys": [
                            "inventory-key-00",
                            "inventory-key-01",
                            "inventory-key-02",
                            "inventory-key-07",
                            "inventory-key-08",
                            "inventory-key-09",
                        ],
                        "reason": "roster-heavy cohort",
                        "reason_code": "ai_division",
                    },
                    {
                        "division_id": "div-hetero",
                        "batch_index": 2,
                        "recorded_member_count": 4,
                        "remaining_member_count": 4,
                        "member_url_keys": [
                            "inventory-key-03",
                            "inventory-key-04",
                            "inventory-key-05",
                            "inventory-key-06",
                        ],
                        "reason": "same source shard",
                        "reason_code": "ai_division",
                    },
                ],
                "unassigned_url_keys": [],
            },
        )

    def test_continuation_routes_remaining_members_to_their_own_batches(self) -> None:
        result = inherit_heterogeneous_division_windows(
            division=self.DIVISION,
            inventory_url_keys=self.INVENTORY,
            dispatch_url_keys=["inventory-key-01", "inventory-key-04", "inventory-key-08"],
        )
        self.assertTrue(result["claimed"])
        self.assertEqual(
            [(window["batch_index"], window["member_url_keys"]) for window in result["batch_windows"]],
            [(1, ["inventory-key-01", "inventory-key-08"]), (2, ["inventory-key-04"])],
        )
        self.assertEqual([window["remaining_member_count"] for window in result["batch_windows"]], [2, 1])
        self.assertEqual([window["recorded_member_count"] for window in result["batch_windows"]], [6, 4])

    def test_fully_drained_batches_are_omitted(self) -> None:
        result = inherit_heterogeneous_division_windows(
            division=self.DIVISION,
            inventory_url_keys=self.INVENTORY,
            dispatch_url_keys=["inventory-key-00", "inventory-key-09"],
        )
        self.assertTrue(result["claimed"])
        self.assertEqual([window["batch_index"] for window in result["batch_windows"]], [1])

    def test_unassigned_member_fails_the_claim_closed(self) -> None:
        result = inherit_heterogeneous_division_windows(
            division=self.DIVISION,
            inventory_url_keys=self.INVENTORY,
            dispatch_url_keys=["inventory-key-00", "drifted-in-after-mint"],
        )
        self.assertFalse(result["claimed"])
        self.assertEqual(result["unassigned_url_keys"], ["drifted-in-after-mint"])

    def test_empty_division_id_and_empty_dispatch_never_claim(self) -> None:
        no_id = inherit_heterogeneous_division_windows(
            division={"division_id": "", "batches": list(self.DIVISION["batches"])},
            inventory_url_keys=self.INVENTORY,
            dispatch_url_keys=self.INVENTORY,
        )
        self.assertFalse(no_id["claimed"])
        empty_dispatch = inherit_heterogeneous_division_windows(
            division=self.DIVISION,
            inventory_url_keys=self.INVENTORY,
            dispatch_url_keys=[],
        )
        self.assertFalse(empty_dispatch["claimed"])
        self.assertEqual(empty_dispatch["batch_windows"], [])


class V6LiveSetIntegrationTest(unittest.TestCase):
    """S4 writer makes V6's live set non-empty while a divided wave is active."""

    def test_same_id_is_idempotent_and_foreign_id_rejects(self) -> None:
        self.assertIsNone(validate_v6_wave_mint_only("div-a", live_division_ids={"div-a"}))
        failure = validate_v6_wave_mint_only("div-b", live_division_ids={"div-a"})
        self.assertIsNotNone(failure)
        assert failure is not None
        self.assertIn("div-a", failure)

    def test_shadow_apply_time_v6_fails_when_a_divided_wave_is_live(self) -> None:
        urls = _urls("v6live", 600)
        entries = {
            normalize_linkedin_profile_url_key(url): {
                "refill_queue_state": "dispatch_reserved",
                REGISTRY_DIVISION_ID_FIELD: "div-in-flight",
            }
            for url in urls
        }
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            items = _build_profile_prefetch_queue_items(
                urls, source_shards_by_url={}, priority=False, queue_state="ready"
            )
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=urls,
                requested_url_count=len(urls),
                candidate_count=len(urls),
                priority=False,
                source_shards_by_url={},
                worker_budget=_budget(4),
                queue_items=items,
            )
            record = record_profile_prefetch_division_shadow(
                ScriptedProfileBatchDividerModelClient(mode="simulate"),
                plan=plan,
                registry_entries=entries,
                wave_mint_provider_submit=True,
            )
        assert record is not None
        apply_time = {entry["validator"]: entry for entry in record["apply_time_validator_results"]}
        v6 = apply_time[VALIDATOR_V6_WAVE_MINT_ONLY]
        self.assertEqual(v6["status"], VALIDATOR_RESULT_STATUS_FAIL)
        self.assertIn("div-in-flight", v6["reason"])

    def test_shadow_apply_time_v6_passes_with_no_live_ids(self) -> None:
        urls = _urls("v6idle", 600)
        with mock.patch.dict("os.environ", _SCRIPTED_DIVIDER_ENV, clear=False):
            items = _build_profile_prefetch_queue_items(
                urls, source_shards_by_url={}, priority=False, queue_state="ready"
            )
            plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=urls,
                requested_url_count=len(urls),
                candidate_count=len(urls),
                priority=False,
                source_shards_by_url={},
                worker_budget=_budget(4),
                queue_items=items,
            )
            record = record_profile_prefetch_division_shadow(
                ScriptedProfileBatchDividerModelClient(mode="simulate"),
                plan=plan,
                registry_entries={},
                wave_mint_provider_submit=True,
            )
        assert record is not None
        apply_time = {entry["validator"]: entry for entry in record["apply_time_validator_results"]}
        self.assertEqual(apply_time[VALIDATOR_V6_WAVE_MINT_ONLY]["status"], VALIDATOR_RESULT_STATUS_PASS)


class StructuralPinTest(unittest.TestCase):
    """Source-level pins: mint threading present, heterogeneous claim unreachable."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.enrichment_source = Path(enrichment_module.__file__).read_text(encoding="utf-8")
        cls.src_dir = Path(enrichment_module.__file__).resolve().parent

    def test_mint_seam_threads_the_shadow_division_id_once(self) -> None:
        self.assertEqual(
            self.enrichment_source.count("refill_plan_division_id=shadow_plan_division_id(shadow_division_record)"), 1
        )
        # The shadow hook runs BEFORE the recorder so the id can ride the same
        # plan-record write (S4 reorder of the S3 seam).
        hook_position = self.enrichment_source.index(
            "shadow_division_record = record_profile_prefetch_division_shadow("
        )
        recorder_position = self.enrichment_source.index(
            "refill_plan_division_id=shadow_plan_division_id(shadow_division_record)"
        )
        self.assertLess(hook_position, recorder_position)

    def test_scalar_claim_carries_the_division_id_exactly_once(self) -> None:
        self.assertEqual(self.enrichment_source.count("inherited_division_wave_identity("), 1)

    def test_heterogeneous_inheritance_has_no_production_caller_pre_flip(self) -> None:
        # The pure function exists ONLY in its owner module; no module under
        # src/sourcing_agent references it (the S5 flip is the sanctioned first
        # caller — passing an AI division into the plan is structurally
        # impossible today).
        referencing: dict[str, int] = {}
        for path in sorted(self.src_dir.rglob("*.py")):
            count = path.read_text(encoding="utf-8").count("inherit_heterogeneous_division_windows")
            if count:
                referencing[path.name] = count
        self.assertEqual(referencing, {"profile_batch_division.py": 2})  # the def + its module docstring

    def test_r6_window_function_signature_gains_no_division_parameter(self) -> None:
        parameters = list(inspect.signature(_apply_durable_refill_wave_dispatch_window).parameters)
        self.assertEqual(
            parameters,
            ["dispatch_window", "dispatch_urls", "registry_entries", "append_trigger_replan", "retry_isolated_refill"],
        )


class RegistryPersistenceRoundTripTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    """PG round-trip: migration 0015 column + lockstep lifecycle with the scalars."""

    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.store = self.make_pg_store(f"{self.tempdir.name}/registry.db")

    def test_division_id_persists_with_the_wave_and_clears_on_terminal(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        active_url = "https://www.linkedin.com/in/wave-active-0001/"
        deferred_url = "https://www.linkedin.com/in/wave-deferred-0001/"
        result = repo.record_refill_plan_items(
            active_profile_urls=[active_url],
            deferred_profile_urls=[deferred_url],
            source_jobs=["job_wave"],
            snapshot_dir="/tmp/snapshot_wave",
            trigger_kind="profile_prefetch_replan",
            plan_reason="ready_to_dispatch",
            active_queue_state="dispatch_reserved",
            active_reason="scheduler_dispatch_reserved",
            refill_plan_batch_size=50,
            refill_plan_batch_count=2,
            refill_plan_window_url_count=100,
            refill_plan_division_id="div-pg-0001",
        )
        self.assertEqual(result["status"], "recorded")
        for profile_url, expected_state in ((active_url, "dispatch_reserved"), (deferred_url, "deferred_budget")):
            entry = repo.get(profile_url)
            assert entry is not None
            self.assertEqual(entry["refill_queue_state"], expected_state)
            self.assertEqual(entry["refill_plan_batch_size"], 50)
            self.assertEqual(entry[REGISTRY_DIVISION_ID_FIELD], "div-pg-0001")
        # Re-observing the row through a non-terminal write preserves the id
        # exactly as it preserves the scalar identity.
        repo.mark_queued(active_url, source_jobs=["job_wave"], snapshot_dir="/tmp/snapshot_wave")
        requeued = repo.get(active_url)
        assert requeued is not None
        self.assertEqual(requeued[REGISTRY_DIVISION_ID_FIELD], "div-pg-0001")
        # Terminal closure clears the whole wave identity in lockstep.
        repo.mark_fetched(active_url, raw_path="/tmp/raw.json", source_jobs=["job_wave"])
        terminal = repo.get(active_url)
        assert terminal is not None
        self.assertEqual(terminal["status"], "fetched")
        self.assertEqual(terminal["refill_plan_batch_size"], 0)
        self.assertEqual(terminal[REGISTRY_DIVISION_ID_FIELD], "")

    def test_default_write_round_trips_empty_division_id(self) -> None:
        repo = self.store.repos.linkedin_profile_registry
        profile_url = "https://www.linkedin.com/in/wave-plain-0001/"
        repo.record_refill_plan_items(
            active_profile_urls=[profile_url],
            source_jobs=["job_plain"],
            snapshot_dir="/tmp/snapshot_plain",
            active_queue_state="dispatch_reserved",
            refill_plan_batch_size=50,
            refill_plan_batch_count=1,
            refill_plan_window_url_count=50,
        )
        entry = repo.get(profile_url)
        assert entry is not None
        self.assertEqual(entry[REGISTRY_DIVISION_ID_FIELD], "")


if __name__ == "__main__":
    unittest.main()
