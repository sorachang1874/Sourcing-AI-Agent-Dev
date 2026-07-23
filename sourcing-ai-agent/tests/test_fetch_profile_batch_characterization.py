"""Ruling-① characterization baseline for the fetch-profile batch-division plan record.

Provenance: characterization adopted 2026-07-23 as the WS7/W7.1 prerequisite pinned
by operator ruling ① (RATIFIED 2026-07-22, docs/REFACTOR_MASTER_PLAN.md §6.5): the
future AI batch divider becomes an independent plan stage that directly produces
``dispatch_item_specs`` (4-8 batches + members + explainable reason), and the CURRENT
constant rule ladder (recon portrait docs/WS7_STRONG_AGENT_RECON_2026-07-22.md §1.2)
is demoted to acceptance validators. This suite is the "先钉 scheduler plan-record
characterization" prerequisite: it pins the CURRENT pure inputs→outputs surface that
the AI divider will replace, exactly as it behaves today.

Characterized surface (all pure module-level functions in
src/sourcing_agent/enrichment.py; anchors verified on this tree):
  * constant ladder            :465-533  (50/200/300/400/8-cap + 10/5/20 coalescing +
                                          refill queue-state vocabulary R5/R6/R7)
  * sizer R1.a-d               :677  ``_recommended_harvest_profile_prefetch_actor_slot_batch_size``
  * roster split R2            :711  ``_should_split_roster_ready_set_across_actor_slots``
  * simulate/non-live window   :918  ``_recommended_harvest_profile_prefetch_dispatch_window``
  * tiny-batch merge R3        :1007 ``_coalesce_tiny_profile_dispatch_chunks``
  * wave bounding R5           :1062 ``_split_profile_prefetch_dispatch_specs``
  * durable wave inherit R6    :1162 ``_apply_durable_refill_wave_dispatch_window``
  * plan builder + plan record :1327/:1253 ``_build_profile_prefetch_batch_plan`` +
                                          ``ProfilePrefetchBatchPlan.to_record()``

CHARACTERIZATION CONTRACT: every golden below was captured empirically against the
current tree and must pass UNCHANGED. The oracle pins reality, warts included — do
not "fix" a golden to look nicer. The AI divider's acceptance validators must
preserve the invariant shapes pinned here: every batch ≤300 members (provider
envelope), ≤8 batches per wave (provider actor ceiling), R7 ``batch_size_reason``
audit enum, R5 wave bounding (surplus specs defer, never overrun worker budget),
R6 same-wave no-re-split inheritance, and the retry wave staying isolated after
normal-wave closure.

Pinned quirks (current behavior, NOT to be silently repaired here):
  * Plan records only carry the contextual sizing constants
    (``actor_slot_url_target``/``durable_unit_max_urls``/``provider_envelope_max_urls``/
    ``large_ready_set_max_batch_count``/``large_ready_set_threshold_urls``) when the
    plan builder itself overrode the incoming window; records built from a
    preserved-verbatim window report them as 0 (see the low-volume/tail goldens).
  * ``_coalesce_tiny_profile_dispatch_chunks`` can flush a pending tiny tail as its
    own sub-minimum chunk when merging it into the following larger chunk would
    exceed ``max(minimum, len(chunk))`` — the sub-50 spec-level guard downstream is
    what keeps that fragment from dispatching as a tiny batch.
  * The canonical hot path runs under provider mode "simulate", where the window
    ladder emits 75-url batches for a 300-url set (4x75), NOT the R1.b single
    durable envelope; R1.b-d only shape the plan when the incoming window is the
    bare actor-slot window on the queue-quiescent tail path (or in live/scripted
    window construction).

Deliberately NOT covered here (store-entangled, not pure):
  * ``_profile_refill_retry_gate`` (enrichment.py:3900) — reads the profile registry
    via the store; its normal-wave-closure semantics are pinned at the PG level by
    tests/test_enrichment.py retry-wait lane members and
    tests/test_profile_prefetch_scheduler_contract.py invariant guards. The pure
    retry-isolation consequence (retry_wait items → isolated tiny dispatch) IS
    pinned below.
  * ``_harvest_profile_prefetch_new_worker_budget`` (enrichment.py:4082) — counts
    active/reserved workers via the store; plan records here take the budget dict
    as a pure input.
"""

from __future__ import annotations

import unittest
from typing import Any
from unittest import mock

from sourcing_agent.enrichment import (
    HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY,
    PROFILE_REFILL_NORMAL_QUEUE_STATES,
    PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE,
    PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_MARKERS,
    PROFILE_REFILL_RETRY_QUEUE_STATES,
    _apply_durable_refill_wave_dispatch_window,
    _build_profile_prefetch_batch_plan,
    _build_profile_prefetch_queue_items,
    _coalesce_tiny_profile_dispatch_chunks,
    _harvest_profile_low_volume_company_max_urls,
    _harvest_profile_min_non_tail_batch_size,
    _harvest_profile_prefetch_actor_slot_url_target,
    _harvest_profile_prefetch_durable_unit_max_urls,
    _harvest_profile_prefetch_max_batch_count_for_large_ready_set,
    _harvest_profile_prefetch_provider_envelope_max_urls,
    _harvest_profile_prefetch_scale_threshold_urls,
    _harvest_profile_tiny_batch_max_size,
    _recommended_harvest_profile_prefetch_actor_slot_batch_size,
    _recommended_harvest_profile_prefetch_dispatch_window,
    _should_split_roster_ready_set_across_actor_slots,
    _split_profile_prefetch_dispatch_specs,
)
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key

_SIMULATE_ENV = {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}


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


def _plan(
    url_list: list[str],
    *,
    requested: int | None = None,
    candidate: int | None = None,
    window: dict[str, Any] | None = None,
    wbudget: dict[str, int] | None = None,
    source_shards: dict[str, list[str]] | None = None,
    registry_entries: dict[str, dict[str, Any]] | None = None,
    allow_tail: bool = False,
):
    items = _build_profile_prefetch_queue_items(
        url_list,
        source_shards_by_url=source_shards or {},
        priority=False,
        queue_state="ready",
        registry_entries=registry_entries or {},
    )
    return _build_profile_prefetch_batch_plan(
        dispatch_urls=url_list,
        requested_url_count=requested if requested is not None else len(url_list),
        candidate_count=candidate if candidate is not None else len(url_list),
        priority=False,
        source_shards_by_url=source_shards or {},
        worker_budget=wbudget or _budget(4),
        dispatch_window=window,
        queue_items=items,
        allow_under_target_final_tail_dispatch=allow_tail,
    )


def _base_record(**overrides: Any) -> dict[str, Any]:
    """The FULL plan-record shape with a 4-slot idle budget; every scenario golden
    below overrides only what actually differs, and assertEqual compares the whole
    dict, so any added/removed/renamed record key fails the oracle."""

    record: dict[str, Any] = {
        "kind": "profile_prefetch_batch_plan",
        "schema_version": 1,
        "item_store": "linkedin_profile_registry",
        "refill_policy": "continuous_ready_item_refill",
        "plan_reason": "ready_to_dispatch",
        "retry_isolation": False,
        "requested_url_count": 0,
        "candidate_count": 0,
        "queue_item_count": 0,
        "normal_queue_item_count": 0,
        "retry_wait_item_count": 0,
        "planned_dispatch_worker_count": 0,
        "planned_dispatch_item_count": 0,
        "planned_deferred_item_count": 0,
        "planned_tail_coalescing_item_count": 0,
        "planned_worker_budget_deferred_item_count": 0,
        "available_slot_count": 4,
        "planned_new_worker_count": 0,
        "unfilled_available_slot_count": 4,
        "underfilled_with_deferred_items": False,
        "refill_saturation": "no_ready_items",
        "original_dispatch_chunk_count": 0,
        "coalesced_dispatch_chunk_count": 0,
        "tiny_batch_coalesced_count": 0,
        "recommended_batch_size": 250,
        "recommended_batch_count": 0,
        "recommended_max_workers": 4,
        "dispatch_strategy": "configured_prefetch_batch_size",
        "base_dispatch_strategy": "",
        "batch_size_contract": "",
        "batch_size_reason": "",
        "actor_slot_url_target": 0,
        "durable_unit_max_urls": 0,
        "provider_envelope_max_urls": 0,
        "large_ready_set_max_batch_count": 0,
        "large_ready_set_threshold_urls": 0,
        "active_worker_count": 0,
        "scheduler_reserved_worker_count": 0,
        "effective_active_worker_count": 0,
        "submit_budget": 4,
        "actor_budget": 4,
        "available_new_worker_count": 4,
    }
    record.update(overrides)
    return record


class ConstantLadderBaselineTest(unittest.TestCase):
    """Baseline environment pin: the resolved constant ladder the goldens assume.

    If this test fails, the shell carries HARVEST_PROFILE_* overrides (or the
    defaults changed) — the plan-record goldens below are only meaningful against
    this exact ladder."""

    def test_resolved_constant_ladder_matches_defaults(self) -> None:
        self.assertEqual(_harvest_profile_prefetch_actor_slot_url_target(), 50)
        self.assertEqual(_harvest_profile_prefetch_durable_unit_max_urls(), 200)
        self.assertEqual(_harvest_profile_prefetch_provider_envelope_max_urls(), 300)
        self.assertEqual(_harvest_profile_prefetch_scale_threshold_urls(), 400)
        self.assertEqual(_harvest_profile_prefetch_max_batch_count_for_large_ready_set(), 8)
        self.assertEqual(_harvest_profile_min_non_tail_batch_size(), 10)
        self.assertEqual(_harvest_profile_tiny_batch_max_size(), 5)
        self.assertEqual(_harvest_profile_low_volume_company_max_urls(), 20)
        self.assertEqual(HARVEST_PROFILE_NONLIVE_FETCH_CONCURRENCY, 4)

    def test_refill_queue_state_vocabulary_is_pinned(self) -> None:
        # R5/R6/R7 interaction surface: the queue-state vocabulary the retry gate
        # and durable-wave inheritance key on.
        self.assertEqual(
            PROFILE_REFILL_NORMAL_QUEUE_STATES,
            ("deferred_budget", "deferred_coalescing", "dispatch_reserved", "dispatch_claimed"),
        )
        self.assertEqual(PROFILE_REFILL_RETRY_QUEUE_STATES, ("retry_wait",))
        self.assertEqual(PROFILE_REFILL_PROVIDER_OWNED_QUEUE_STATE, "planned_dispatch")
        self.assertEqual(
            PROFILE_REFILL_RETRY_PROVIDER_SUBMIT_MARKERS,
            {"profile_retry_provider_submit", "retry_remote_provider_submitted"},
        )


class SizerBandCharacterizationTest(unittest.TestCase):
    """R1.a-d golden grid: count → (batch_size, R7 reason), boundaries included."""

    GOLDEN_GRID: tuple[tuple[int, int, str], ...] = (
        # R1.a — actor-slot item packing (count <= 50), plus the empty sentinel.
        (0, 50, "no_ready_urls"),
        (1, 50, "actor_slot_item_packing"),
        (25, 50, "actor_slot_item_packing"),
        (49, 50, "actor_slot_item_packing"),
        (50, 50, "actor_slot_item_packing"),
        # R1.b — single durable envelope (50 < count <= 300).
        (51, 51, "single_durable_unit_ready_set"),
        (120, 120, "single_durable_unit_ready_set"),
        (200, 200, "single_durable_unit_ready_set"),
        (299, 299, "single_durable_unit_ready_set"),
        (300, 300, "single_durable_unit_ready_set"),
        # R1.c — balanced envelopes (300 < count, ceil(count/300) <= 8); the reason
        # flips at the 400 scale threshold.
        (301, 151, "bounded_ready_set_balanced_provider_envelopes"),
        (350, 175, "bounded_ready_set_balanced_provider_envelopes"),
        (400, 200, "bounded_ready_set_balanced_provider_envelopes"),
        (401, 201, "large_ready_set_provider_envelope_target"),
        (529, 265, "large_ready_set_provider_envelope_target"),
        (600, 300, "large_ready_set_provider_envelope_target"),
        (1600, 267, "large_ready_set_provider_envelope_target"),
        (2384, 298, "large_ready_set_provider_envelope_target"),
        (2400, 300, "large_ready_set_provider_envelope_target"),
        # R1.d — 8-batch cap (ceil(count/300) > 8): fixed 300 envelope, surplus defers.
        (2401, 300, "large_ready_set_provider_envelope_cap"),
        (3000, 300, "large_ready_set_provider_envelope_cap"),
        (5000, 300, "large_ready_set_provider_envelope_cap"),
    )

    def test_sizer_band_golden_grid(self) -> None:
        for count, expected_size, expected_reason in self.GOLDEN_GRID:
            with self.subTest(count=count):
                self.assertEqual(
                    _recommended_harvest_profile_prefetch_actor_slot_batch_size(count),
                    (expected_size, expected_reason),
                )


class RosterSplitPredicateCharacterizationTest(unittest.TestCase):
    """R2 truth table: (count, source_mix, free slots) → split decision, boundaries included."""

    def test_roster_split_truth_table(self) -> None:
        cases: tuple[tuple[int, dict[str, int] | None, int, bool], ...] = (
            # count band (50, 200]: 50 is out, 51 in, 200 in, 201 out.
            (50, {"company_roster": 50}, 4, False),
            (51, {"company_roster": 51}, 4, True),
            (200, {"company_roster": 200}, 4, True),
            (201, {"company_roster": 201}, 4, False),
            # slot gate: needs >= 2 free slots.
            (120, {"company_roster": 120}, 1, False),
            (120, {"company_roster": 120}, 2, True),
            # roster share gate: >= 0.5 of labeled items.
            (120, {"company_roster": 59, "profile_search": 61}, 4, False),
            (120, {"company_roster": 60, "profile_search": 60}, 4, True),
            (120, {"company_roster": 0, "other": 120}, 4, False),
            (120, None, 4, False),
        )
        for total_urls, source_mix, available, expected in cases:
            with self.subTest(total_urls=total_urls, source_mix=source_mix, available=available):
                self.assertIs(
                    _should_split_roster_ready_set_across_actor_slots(
                        total_urls=total_urls,
                        source_mix=source_mix,
                        available_new_worker_count=available,
                    ),
                    expected,
                )


class SimulateDispatchWindowCharacterizationTest(unittest.TestCase):
    """Non-live (simulate) window goldens — the canonical hot-path window ladder."""

    def _window(self, count: int, *, priority: bool = False, shards: dict[str, list[str]] | None = None):
        with mock.patch.dict("os.environ", _SIMULATE_ENV, clear=False):
            return _recommended_harvest_profile_prefetch_dispatch_window(
                count, priority=priority, source_shards_by_url=shards
            )

    def test_simulate_window_goldens(self) -> None:
        def golden(batch_size: int, batch_count: int, source_mix: dict[str, int], *, priority: bool = False):
            return {
                "batch_size": batch_size,
                "max_workers": 4,
                "batch_count": batch_count,
                "source_mix": {"company_roster": 0, "profile_search": 0, "targeted": 0, "other": 0, **source_mix},
                "strategy": "configured_prefetch_batch_size",
                "priority": priority,
            }

        self.assertEqual(self._window(0), golden(250, 0, {}))
        self.assertEqual(self._window(12), golden(250, 1, {"other": 12}))
        roster_urls = _urls("roster", 120)
        self.assertEqual(
            self._window(120, shards={u: ["harvest_company_employees:acme:current"] for u in roster_urls}),
            golden(250, 1, {"company_roster": 120}),
        )
        # The simulate ladder steps DOWN at 300 (min(250, 75)) — 4x75, not one
        # durable envelope; the R1 sizer does not own this path (pinned quirk).
        self.assertEqual(self._window(300), golden(75, 4, {"other": 300}))
        self.assertEqual(self._window(300, priority=True), golden(25, 12, {"other": 300}, priority=True))
        self.assertEqual(self._window(1600), golden(50, 32, {"other": 1600}))


class TinyBatchCoalescingCharacterizationTest(unittest.TestCase):
    """R3 tiny-batch merge goldens: exact chunk membership in, exact chunk membership out."""

    BIG = [f"x{i}" for i in range(50)]

    def _coalesce(self, chunks: list[list[str]], *, requested: int = 100, candidate: int = 100):
        return _coalesce_tiny_profile_dispatch_chunks(
            chunks, requested_url_count=requested, candidate_count=candidate
        )

    def test_tiny_fragments_accumulate_until_min_non_tail_then_flush(self) -> None:
        self.assertEqual(
            self._coalesce([["a", "b", "c"], ["d", "e", "f"], ["g", "h", "i", "j"], self.BIG]),
            [["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"], self.BIG],
        )

    def test_final_sub_minimum_tail_folds_into_last_run(self) -> None:
        self.assertEqual(
            self._coalesce([self.BIG, ["a", "b"], ["c", "d"]]),
            [[*self.BIG, "a", "b", "c", "d"]],
        )

    def test_pending_tiny_tail_flushes_alone_when_next_chunk_is_full(self) -> None:
        # Pinned quirk: 2 + 50 > max(10, 50) so the pending tail is flushed as its
        # own sub-minimum chunk; the sub-50 spec-level guard downstream is what
        # keeps it from dispatching as a tiny batch.
        self.assertEqual(
            self._coalesce([["a", "b"], self.BIG]),
            [["a", "b"], self.BIG],
        )

    def test_pending_tail_merges_into_smaller_following_chunk(self) -> None:
        # 5-tiny + 5-tiny accumulate to the minimum (10) and flush; the trailing
        # 6-chunk is non-tiny and stays separate.
        self.assertEqual(
            self._coalesce(
                [["a", "b", "c", "d", "e"], ["f", "g", "h", "i", "j"], ["k1", "k2", "k3", "k4", "k5", "k6"]]
            ),
            [["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"], ["k1", "k2", "k3", "k4", "k5", "k6"]],
        )

    def test_low_volume_company_sets_are_left_untouched(self) -> None:
        self.assertEqual(
            self._coalesce([["a"], ["b"], ["c"]], requested=3, candidate=3),
            [["a"], ["b"], ["c"]],
        )

    def test_single_chunk_passthrough_and_lone_final_tiny(self) -> None:
        self.assertEqual(self._coalesce([["a", "b"]]), [["a", "b"]])
        self.assertEqual(self._coalesce([["a", "b"], ["c"]]), [["a", "b", "c"]])


class DurableWaveInheritanceCharacterizationTest(unittest.TestCase):
    """R6 golden dicts: a recomputed window vs. the recorded durable wave identity."""

    RECOMPUTED_WINDOW = {
        "batch_size": 100,
        "max_workers": 4,
        "batch_count": 3,
        "source_mix": {"other": 300},
        "batch_size_reason": "single_durable_unit_ready_set",
    }

    @staticmethod
    def _registry(urls: list[str], *, batch_size: int, batch_count: int, window_count: int, state: str):
        return {
            normalize_linkedin_profile_url_key(u): {
                "refill_queue_state": state,
                "refill_plan_batch_size": batch_size,
                "refill_plan_batch_count": batch_count,
                "refill_plan_window_url_count": window_count,
            }
            for u in urls
        }

    def _resolve(self, urls: list[str], registry, *, append: bool = False, retry: bool = False):
        return _apply_durable_refill_wave_dispatch_window(
            dict(self.RECOMPUTED_WINDOW),
            dispatch_urls=urls,
            registry_entries=registry,
            append_trigger_replan=append,
            retry_isolated_refill=retry,
        )

    def test_larger_recorded_wave_claims_the_window(self) -> None:
        urls = _urls("durable", 300)
        registry = self._registry(urls, batch_size=232, batch_count=8, window_count=1856, state="deferred_budget")
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

    def test_equal_recorded_wave_also_claims_the_window(self) -> None:
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


class DispatchSpecSplitCharacterizationTest(unittest.TestCase):
    """R5 wave bounding on raw specs: exact actives + exact deferred flattening."""

    SPECS = [(1, ["a", "b"]), (2, ["c"]), (3, ["d", "e", "f"])]

    def test_split_goldens(self) -> None:
        cases = (
            (0, [], ["a", "b", "c", "d", "e", "f"]),
            (1, [(1, ["a", "b"])], ["c", "d", "e", "f"]),
            (2, [(1, ["a", "b"]), (2, ["c"])], ["d", "e", "f"]),
            (5, [(1, ["a", "b"]), (2, ["c"]), (3, ["d", "e", "f"])], []),
        )
        for available, expected_active, expected_deferred in cases:
            with self.subTest(available=available):
                self.assertEqual(
                    _split_profile_prefetch_dispatch_specs(
                        self.SPECS, available_new_worker_count=available
                    ),
                    (expected_active, expected_deferred),
                )


class PlanRecordCharacterizationTest(unittest.TestCase):
    """Full plan-record goldens: representative input shapes → the ENTIRE record dict
    plus exact per-batch membership (chunks are contiguous, order-preserving slices
    of the input URL list — that ordering semantic is itself part of the pin)."""

    def setUp(self) -> None:
        patcher = mock.patch.dict("os.environ", _SIMULATE_ENV, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _assert_specs(self, plan, expected: list[tuple[int, list[str]]]) -> None:
        self.assertEqual(plan.dispatch_specs, expected)
        self.assertEqual(
            [(index, [item.profile_url for item in chunk]) for index, chunk in plan.dispatch_item_specs],
            expected,
        )

    def test_small_low_volume_roster_dispatches_one_batch(self) -> None:
        urls = _urls("small", 12)
        plan = _plan(urls)
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=12,
                candidate_count=12,
                queue_item_count=12,
                normal_queue_item_count=12,
                planned_dispatch_worker_count=1,
                planned_dispatch_item_count=12,
                planned_new_worker_count=1,
                unfilled_available_slot_count=3,
                refill_saturation="ready_items_exhausted",
                original_dispatch_chunk_count=1,
                coalesced_dispatch_chunk_count=1,
                recommended_batch_count=1,
            ),
        )
        self._assert_specs(plan, [(1, urls)])
        self.assertEqual(plan.deferred_urls, [])

    def test_sub_50_normal_tail_defers_whole_set_as_coalescing_tail(self) -> None:
        urls = _urls("tail", 47)
        plan = _plan(urls, requested=297, candidate=297)
        self.assertEqual(
            plan.to_record(),
            _base_record(
                plan_reason="deferred_coalescing_sub_50_tail",
                requested_url_count=297,
                candidate_count=297,
                queue_item_count=47,
                normal_queue_item_count=47,
                planned_deferred_item_count=47,
                planned_tail_coalescing_item_count=47,
                refill_saturation="tail_coalescing_wait",
                recommended_batch_count=1,
            ),
        )
        self._assert_specs(plan, [])
        self.assertEqual(plan.deferred_urls, urls)
        self.assertEqual([item.profile_url for item in plan.tail_coalescing_items], urls)

    def test_sub_50_final_tail_dispatches_when_queue_quiescent(self) -> None:
        urls = _urls("finaltail", 47)
        plan = _plan(urls, requested=297, candidate=297, allow_tail=True)
        self.assertEqual(
            plan.to_record(),
            _base_record(
                plan_reason="queue_quiescent_final_tail",
                requested_url_count=297,
                candidate_count=297,
                queue_item_count=47,
                normal_queue_item_count=47,
                planned_dispatch_worker_count=1,
                planned_dispatch_item_count=47,
                planned_new_worker_count=1,
                unfilled_available_slot_count=3,
                refill_saturation="ready_items_exhausted",
                original_dispatch_chunk_count=1,
                coalesced_dispatch_chunk_count=1,
                recommended_batch_count=1,
            ),
        )
        self._assert_specs(plan, [(1, urls)])
        self.assertEqual(plan.deferred_urls, [])

    def test_exactly_50_dispatches_one_actor_slot_batch(self) -> None:
        urls = _urls("fifty", 50)
        plan = _plan(urls, requested=297, candidate=297)
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=297,
                candidate_count=297,
                queue_item_count=50,
                normal_queue_item_count=50,
                planned_dispatch_worker_count=1,
                planned_dispatch_item_count=50,
                planned_new_worker_count=1,
                unfilled_available_slot_count=3,
                refill_saturation="ready_items_exhausted",
                original_dispatch_chunk_count=1,
                coalesced_dispatch_chunk_count=1,
                recommended_batch_count=1,
            ),
        )
        self._assert_specs(plan, [(1, urls)])

    def test_exactly_300_on_simulate_window_splits_into_four_75s(self) -> None:
        # Pinned quirk: the canonical simulate hot path packs 300 as 4x75 (the
        # non-live ladder), NOT the R1.b single durable envelope.
        urls = _urls("threehundred", 300)
        plan = _plan(urls)
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=300,
                candidate_count=300,
                queue_item_count=300,
                normal_queue_item_count=300,
                planned_dispatch_worker_count=4,
                planned_dispatch_item_count=300,
                planned_new_worker_count=4,
                unfilled_available_slot_count=0,
                refill_saturation="filled_available_slots",
                original_dispatch_chunk_count=4,
                coalesced_dispatch_chunk_count=4,
                recommended_batch_size=75,
                recommended_batch_count=4,
            ),
        )
        self._assert_specs(
            plan,
            [(index + 1, urls[index * 75 : (index + 1) * 75]) for index in range(4)],
        )

    def test_exactly_300_on_bare_slot_window_quiescent_collapses_to_single_durable_envelope(self) -> None:
        # R1.b via the plan builder tier (b): a bare actor-slot window on the
        # queue-quiescent tail path grows to the canonical single durable envelope.
        urls = _urls("threehundred", 300)
        plan = _plan(
            urls,
            window={"batch_size": 50, "max_workers": 4, "batch_count": 6, "source_mix": {"other": 300}},
            allow_tail=True,
        )
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=300,
                candidate_count=300,
                queue_item_count=300,
                normal_queue_item_count=300,
                planned_dispatch_worker_count=1,
                planned_dispatch_item_count=300,
                planned_new_worker_count=1,
                unfilled_available_slot_count=3,
                refill_saturation="ready_items_exhausted",
                original_dispatch_chunk_count=1,
                coalesced_dispatch_chunk_count=1,
                recommended_batch_size=300,
                recommended_batch_count=1,
                recommended_max_workers=1,
                dispatch_strategy="",
                batch_size_contract="profile_actor_slot_ready_item_packing",
                batch_size_reason="single_durable_unit_ready_set",
                actor_slot_url_target=50,
                durable_unit_max_urls=200,
                provider_envelope_max_urls=300,
                large_ready_set_max_batch_count=8,
                large_ready_set_threshold_urls=400,
            ),
        )
        self._assert_specs(plan, [(1, urls)])

    def test_large_3000_hits_the_8_cap_and_defers_the_surplus_wave(self) -> None:
        # R1.d + R5: 3000 → 10 chunks of 300 (envelope cap); 8 available slots
        # dispatch the first 8 (the ≤8-batch wave the AI divider must preserve);
        # the 600-url surplus defers to the next wave.
        urls = _urls("large", 3000)
        plan = _plan(
            urls,
            window={"batch_size": 50, "max_workers": 8, "batch_count": 60, "source_mix": {"other": 3000}},
            wbudget=_budget(8, actor=8, submit=8),
            allow_tail=True,
        )
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=3000,
                candidate_count=3000,
                queue_item_count=3000,
                normal_queue_item_count=3000,
                planned_dispatch_worker_count=8,
                planned_dispatch_item_count=2400,
                planned_deferred_item_count=600,
                planned_worker_budget_deferred_item_count=600,
                available_slot_count=8,
                planned_new_worker_count=8,
                unfilled_available_slot_count=0,
                refill_saturation="worker_budget_saturated",
                original_dispatch_chunk_count=10,
                coalesced_dispatch_chunk_count=10,
                recommended_batch_size=300,
                recommended_batch_count=10,
                recommended_max_workers=8,
                dispatch_strategy="",
                batch_size_contract="profile_actor_slot_ready_item_packing",
                batch_size_reason="large_ready_set_provider_envelope_cap",
                actor_slot_url_target=50,
                durable_unit_max_urls=200,
                provider_envelope_max_urls=300,
                large_ready_set_max_batch_count=8,
                large_ready_set_threshold_urls=400,
                submit_budget=8,
                actor_budget=8,
                available_new_worker_count=8,
            ),
        )
        self._assert_specs(
            plan,
            [(index + 1, urls[index * 300 : (index + 1) * 300]) for index in range(8)],
        )
        self.assertEqual(plan.deferred_urls, urls[2400:])

    def test_sub_slot_window_floors_to_50_and_defers_the_55_tail(self) -> None:
        # Contract D1 shape (tier a): a tiny/legacy 10-url window over a 105-url
        # refill set with one slot floors to the 50 actor slot; tiny-merge folds
        # the 5-remainder into the second chunk (50/55); one slot dispatches 50.
        urls = _urls("dee", 105)
        plan = _plan(
            urls,
            window={"batch_size": 10, "max_workers": 4, "batch_count": 11, "source_mix": {"other": 105}},
            wbudget=_budget(1),
        )
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=105,
                candidate_count=105,
                queue_item_count=105,
                normal_queue_item_count=105,
                planned_dispatch_worker_count=1,
                planned_dispatch_item_count=50,
                planned_deferred_item_count=55,
                planned_worker_budget_deferred_item_count=55,
                available_slot_count=1,
                planned_new_worker_count=1,
                unfilled_available_slot_count=0,
                refill_saturation="worker_budget_saturated",
                original_dispatch_chunk_count=3,
                coalesced_dispatch_chunk_count=2,
                tiny_batch_coalesced_count=1,
                recommended_batch_size=50,
                recommended_batch_count=3,
                recommended_max_workers=3,
                dispatch_strategy="",
                batch_size_contract="profile_actor_slot_ready_item_packing",
                batch_size_reason="actor_slot_item_packing",
                actor_slot_url_target=50,
                durable_unit_max_urls=200,
                provider_envelope_max_urls=300,
                large_ready_set_max_batch_count=8,
                large_ready_set_threshold_urls=400,
                available_new_worker_count=1,
            ),
        )
        self._assert_specs(plan, [(1, urls[:50])])
        self.assertEqual(plan.deferred_urls, urls[50:])

    def test_roster_wave_splits_across_actor_slots_with_20_tail_waiting(self) -> None:
        # R2 + R4: a 120-url roster wave force-splits to 50/slot; the sub-50 tail
        # (20) waits in tail-coalescing instead of dispatching tiny.
        urls = _urls("roster", 120)
        shards = {u: ["harvest_company_employees:acme:current"] for u in urls}
        plan = _plan(
            urls,
            window={"batch_size": 120, "max_workers": 4, "batch_count": 1, "source_mix": {"company_roster": 120}},
            source_shards=shards,
        )
        self.assertEqual(
            plan.to_record(),
            _base_record(
                requested_url_count=120,
                candidate_count=120,
                queue_item_count=120,
                normal_queue_item_count=120,
                planned_dispatch_worker_count=2,
                planned_dispatch_item_count=100,
                planned_deferred_item_count=20,
                planned_tail_coalescing_item_count=20,
                planned_new_worker_count=2,
                unfilled_available_slot_count=2,
                refill_saturation="tail_coalescing_wait",
                original_dispatch_chunk_count=3,
                coalesced_dispatch_chunk_count=3,
                recommended_batch_size=50,
                recommended_batch_count=3,
                recommended_max_workers=3,
                dispatch_strategy="",
                batch_size_contract="profile_actor_slot_ready_item_packing",
                batch_size_reason="roster_actor_slot_fill",
                actor_slot_url_target=50,
                durable_unit_max_urls=200,
                provider_envelope_max_urls=300,
                large_ready_set_max_batch_count=8,
                large_ready_set_threshold_urls=400,
            ),
        )
        self._assert_specs(plan, [(1, urls[:50]), (2, urls[50:100])])
        self.assertEqual(plan.deferred_urls, urls[100:])
        self.assertEqual([item.profile_url for item in plan.tail_coalescing_items], urls[100:])

    def test_retry_wait_items_dispatch_as_isolated_wave_even_when_tiny(self) -> None:
        # The retained second-round shape (operator ruling ①: keep url-level
        # failure record + one unified retry): once every queue item is
        # retry_wait, the plan flips to the isolated retry policy and a tiny
        # batch is legal (retry_isolation is an R7-legitimate tiny reason).
        urls = _urls("retry", 7)
        registry = {
            normalize_linkedin_profile_url_key(u): {"status": "queued", "refill_queue_state": "retry_wait"}
            for u in urls
        }
        plan = _plan(urls, requested=297, candidate=297, registry_entries=registry)
        self.assertEqual(
            plan.to_record(),
            _base_record(
                refill_policy="retry_wait_isolated_refill",
                plan_reason="retry_wait_isolated_dispatch",
                retry_isolation=True,
                requested_url_count=297,
                candidate_count=297,
                queue_item_count=7,
                normal_queue_item_count=0,
                retry_wait_item_count=7,
                planned_dispatch_worker_count=1,
                planned_dispatch_item_count=7,
                planned_new_worker_count=1,
                unfilled_available_slot_count=3,
                refill_saturation="ready_items_exhausted",
                original_dispatch_chunk_count=1,
                coalesced_dispatch_chunk_count=1,
                recommended_batch_count=1,
            ),
        )
        self._assert_specs(plan, [(1, urls)])

    def test_empty_ready_set_produces_the_no_ready_items_record(self) -> None:
        plan = _plan([])
        self.assertEqual(
            plan.to_record(),
            _base_record(plan_reason="no_profile_urls"),
        )
        self._assert_specs(plan, [])

    def test_zero_worker_budget_defers_everything(self) -> None:
        urls = _urls("zb", 60)
        plan = _plan(urls, wbudget=_budget(0))
        self.assertEqual(
            plan.to_record(),
            _base_record(
                plan_reason="worker_budget_exhausted",
                requested_url_count=60,
                candidate_count=60,
                queue_item_count=60,
                normal_queue_item_count=60,
                planned_deferred_item_count=60,
                planned_worker_budget_deferred_item_count=60,
                available_slot_count=0,
                unfilled_available_slot_count=0,
                refill_saturation="no_available_slots",
                original_dispatch_chunk_count=1,
                coalesced_dispatch_chunk_count=1,
                recommended_batch_count=1,
                available_new_worker_count=0,
            ),
        )
        self._assert_specs(plan, [])
        self.assertEqual(plan.deferred_urls, urls)


if __name__ == "__main__":
    unittest.main()
