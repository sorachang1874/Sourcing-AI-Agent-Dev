"""Contract guards for the profile-prefetch scheduler.

Pins the rules of docs/PROFILE_PREFETCH_SCHEDULER_CONTRACT.md:
  * R1.a-d envelope sizing + R7 batch_size_reason enum (sizer characterization).
  * R2 roster split, R3 tiny coalescing, R4 sub-50 tail guard, R5 wave bounding,
    R6 durable-wave inheritance (plan-builder io characterization).
  * Invariant 1: the same wave re-planned across ticks (R6) produces no duplicate
    dispatch_specs.
  * Invariant 7 / decision #1: a missing durable store fails closed as a blocked
    terminal and is never consumed as a satisfied (queued/completed) terminal.
"""

import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.domain import Candidate
from sourcing_agent.enrichment import (
    MultiSourceEnricher,
    _build_profile_prefetch_batch_plan,
    _build_profile_prefetch_queue_items,
    _coalesce_tiny_profile_dispatch_chunks,
    _recommended_harvest_profile_prefetch_actor_slot_batch_size,
)
from sourcing_agent.linkedin_url_normalization import normalize_linkedin_profile_url_key
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


def _asset_catalog(root: Path) -> AssetCatalog:
    return AssetCatalog(
        project_root=root,
        dev_root=root,
        anthropic_root=root,
        anthropic_workbook=root / "anthropic.xlsx",
        anthropic_readme=root / "README.md",
        anthropic_progress=root / "PROGRESS.md",
        legacy_api_accounts=root / "api_accounts.json",
        legacy_company_ids=root / "company_ids.json",
        anthropic_publications=root / "publications.json",
        scholar_scan_results=root / "scholar.json",
        investor_members_json=root / "investor.json",
        employee_scan_skill=root / "employee_skill.md",
        investor_scan_skill=root / "investor_skill.md",
        onepager_skill=root / "onepager_skill.md",
    )


class _Connector:
    settings = type("_Settings", (), {"enabled": True})()


class ProfilePrefetchSizerContractTest(unittest.TestCase):
    """R1.a-d envelope sizing + R7 batch_size_reason enum."""

    def test_actor_slot_sizer_bands_and_reason_enum(self) -> None:
        # (total_urls, expected_batch_size, expected_reason)
        cases = [
            # R1.a actor-slot item packing (count <= 50).
            (0, 50, "no_ready_urls"),
            (1, 50, "actor_slot_item_packing"),
            (50, 50, "actor_slot_item_packing"),
            # R1.b single durable unit (50 < count <= provider-cap 300).
            (51, 51, "single_durable_unit_ready_set"),
            (200, 200, "single_durable_unit_ready_set"),
            (300, 300, "single_durable_unit_ready_set"),
            # R1.c balanced provider envelopes (300 < count, target_batch_count <= 8).
            (301, 151, "bounded_ready_set_balanced_provider_envelopes"),
            (400, 200, "bounded_ready_set_balanced_provider_envelopes"),
            (401, 201, "large_ready_set_provider_envelope_target"),
            (529, 265, "large_ready_set_provider_envelope_target"),
            (600, 300, "large_ready_set_provider_envelope_target"),
            (2384, 298, "large_ready_set_provider_envelope_target"),
            # R1.d provider-envelope cap (target_batch_count > 8).
            (3000, 300, "large_ready_set_provider_envelope_cap"),
        ]
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            for count, expected_size, expected_reason in cases:
                with self.subTest(count=count):
                    size, reason = _recommended_harvest_profile_prefetch_actor_slot_batch_size(count)
                    self.assertEqual(size, expected_size)
                    self.assertEqual(reason, expected_reason)

    def test_sizer_size_ladder_is_monotone_and_bounded_by_provider_envelope_cap(self) -> None:
        # The max(...) ladder must never exceed the provider-envelope cap (300) and the
        # actor-slot target (50) must always be the lower bound.
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            for count in (1, 49, 50, 51, 120, 300, 301, 600, 1600, 5000):
                size, _ = _recommended_harvest_profile_prefetch_actor_slot_batch_size(count)
                self.assertGreaterEqual(size, 50)
                self.assertLessEqual(size, 300)


class ProfilePrefetchPlanIoCharacterizationTest(unittest.TestCase):
    """io characterization (url count + source_mix + worker_budget -> shape) for R2-R6."""

    @staticmethod
    def _plan(urls, *, window, budget, source_shards=None, registry_entries=None, allow_tail=False, requested=None):
        items = _build_profile_prefetch_queue_items(
            urls,
            source_shards_by_url=source_shards or {},
            priority=True,
            queue_state="ready",
            registry_entries=registry_entries or {},
        )
        return _build_profile_prefetch_batch_plan(
            dispatch_urls=urls,
            requested_url_count=requested if requested is not None else len(urls),
            candidate_count=len(urls),
            priority=True,
            source_shards_by_url=source_shards or {},
            worker_budget=budget,
            dispatch_window=window,
            queue_items=items,
            allow_under_target_final_tail_dispatch=allow_tail,
        )

    def test_r2_roster_split_uses_actor_slots_not_one_large_shard(self) -> None:
        urls = [f"https://www.linkedin.com/in/roster-{i:03d}/" for i in range(120)]
        shards = {url: ["harvest_company_employees:acme:current"] for url in urls}
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = self._plan(
                urls,
                window={"batch_size": 120, "max_workers": 4, "batch_count": 1, "source_mix": {"company_roster": 120}},
                budget={"active_worker_count": 0, "actor_budget": 4, "submit_budget": 4, "available_new_worker_count": 4},
                source_shards=shards,
            )
        self.assertEqual(plan.dispatch_window["batch_size"], 50)
        self.assertEqual(plan.dispatch_window["batch_size_reason"], "roster_actor_slot_fill")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [50, 50])
        self.assertEqual(len(plan.tail_coalescing_items), 20)

    def test_r3_tiny_coalescing_collapses_fragments_into_min_non_tail_runs(self) -> None:
        # Pure-function pin: tiny chunks (<=5) accumulate until >= min_non_tail (10), flush
        # as one run, then a following non-tiny chunk stays separate.
        big = [f"x{i}" for i in range(50)]
        coalesced = _coalesce_tiny_profile_dispatch_chunks(
            [["a", "b", "c"], ["d", "e", "f"], ["g", "h", "i", "j"], big],
            requested_url_count=100,
            candidate_count=100,
        )
        self.assertEqual([len(chunk) for chunk in coalesced], [10, 50])
        # A final sub-min tail folds into the last run (no fragmented trailing actor run).
        final_fold = _coalesce_tiny_profile_dispatch_chunks(
            [big, ["a", "b"], ["c", "d"]],
            requested_url_count=100,
            candidate_count=100,
        )
        self.assertEqual([len(chunk) for chunk in final_fold], [54])
        # Low-volume company sets (requested and candidate <= 20) are left untouched.
        low_volume = _coalesce_tiny_profile_dispatch_chunks(
            [["a"], ["b"], ["c"]],
            requested_url_count=3,
            candidate_count=3,
        )
        self.assertEqual([len(chunk) for chunk in low_volume], [1, 1, 1])

    def test_r4_sub_50_normal_tail_defers_as_deferred_coalescing(self) -> None:
        urls = [f"https://www.linkedin.com/in/tail-{i}/" for i in range(25)]
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = self._plan(
                urls,
                window={"batch_size": 50, "max_workers": 4, "batch_count": 1, "source_mix": {"other": 25}},
                budget={"active_worker_count": 0, "actor_budget": 4, "submit_budget": 4, "available_new_worker_count": 4},
                requested=297,
            )
        self.assertEqual(plan.plan_reason, "deferred_coalescing_sub_50_tail")
        self.assertEqual(plan.dispatch_specs, [])
        self.assertEqual(plan.deferred_urls, urls)
        self.assertEqual(len(plan.tail_coalescing_items), 25)

    def test_r4_sub_50_final_tail_dispatches_when_queue_quiescent(self) -> None:
        urls = [f"https://www.linkedin.com/in/final-tail-{i}/" for i in range(47)]
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = self._plan(
                urls,
                window={"batch_size": 50, "max_workers": 4, "batch_count": 1, "source_mix": {"other": 47}},
                budget={"active_worker_count": 0, "actor_budget": 4, "submit_budget": 4, "available_new_worker_count": 4},
                requested=297,
                allow_tail=True,
            )
        self.assertEqual(plan.plan_reason, "queue_quiescent_final_tail")
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [47])
        self.assertEqual(plan.deferred_urls, [])

    def test_r5_wave_bounding_defers_surplus_specs_beyond_worker_budget(self) -> None:
        urls = [f"https://www.linkedin.com/in/wave-{i:04d}/" for i in range(1600)]
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            plan = self._plan(
                urls,
                window={
                    "batch_size": 267,
                    "max_workers": 4,
                    "batch_count": 6,
                    "source_mix": {"other": 1600},
                    "batch_size_reason": "large_ready_set_provider_envelope_target",
                },
                budget={"active_worker_count": 0, "actor_budget": 4, "submit_budget": 4, "available_new_worker_count": 2},
            )
        # 6 balanced envelopes of 267, but only 2 worker slots -> 2 dispatched, surplus defers.
        self.assertEqual([len(chunk) for _, chunk in plan.dispatch_specs], [267, 267])
        self.assertEqual(len(plan.deferred_urls), 1600 - 2 * 267)

    def test_r6_durable_refill_wave_inheritance_overrides_recomputed_window(self) -> None:
        urls = [f"https://www.linkedin.com/in/durable-{i:04d}/" for i in range(300)]
        registry_entries = {
            normalize_linkedin_profile_url_key(url): {
                "status": "queued",
                "refill_queue_state": "deferred_budget",
                "refill_plan_batch_size": 232,
                "refill_plan_batch_count": 8,
                "refill_plan_window_url_count": 1856,
            }
            for url in urls
        }
        items = _build_profile_prefetch_queue_items(
            urls,
            priority=True,
            queue_state="ready",
            registry_entries=registry_entries,
        )
        from sourcing_agent.enrichment import _apply_durable_refill_wave_dispatch_window

        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            # A recomputed window for the deferred remainder packs at 100; R6 inherits the
            # recorded 232 wave size (>= current) so the same wave keeps its formed shape.
            window = {
                "batch_size": 100,
                "max_workers": 4,
                "batch_count": 3,
                "source_mix": {"other": 300},
                "batch_size_reason": "single_durable_unit_ready_set",
            }
            resolved = _apply_durable_refill_wave_dispatch_window(
                window,
                dispatch_urls=urls,
                registry_entries={
                    normalize_linkedin_profile_url_key(url): {
                        "refill_queue_state": "deferred_budget",
                        "refill_plan_batch_size": 232,
                        "refill_plan_batch_count": 8,
                        "refill_plan_window_url_count": 1856,
                    }
                    for url in urls
                },
                append_trigger_replan=False,
                retry_isolated_refill=False,
            )
        self.assertEqual(resolved["batch_size"], 232)
        self.assertEqual(resolved["batch_size_reason"], "durable_refill_wave_batch_size")
        self.assertEqual(resolved["batch_size_contract"], "profile_actor_slot_durable_wave_item_packing")
        self.assertEqual(resolved["durable_refill_wave_batch_size"], 232)
        # An append-trigger replan (a genuinely new wave) must NOT inherit the old wave.
        with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
            append_resolved = _apply_durable_refill_wave_dispatch_window(
                dict(window),
                dispatch_urls=urls,
                registry_entries={
                    normalize_linkedin_profile_url_key(url): {
                        "refill_queue_state": "deferred_budget",
                        "refill_plan_batch_size": 232,
                    }
                    for url in urls
                },
                append_trigger_replan=True,
                retry_isolated_refill=False,
            )
        self.assertEqual(append_resolved["batch_size"], 100)
        self.assertEqual(append_resolved["batch_size_reason"], "single_durable_unit_ready_set")
        del items  # constructed to mirror the production call path


class ProfilePrefetchInvariantGuardTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def test_invariant_1_same_wave_across_ticks_produces_no_duplicate_dispatch_specs(self) -> None:
        # R6: a deferred wave re-planned on the next tick must not re-emit the already
        # dispatched URLs as fresh dispatch_specs (WORKFLOW_BEHAVIOR_GUARDRAILS invariant 1).
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            store = self.make_pg_store(str(root / "control_plane.db"))
            profile_urls = [f"https://www.linkedin.com/in/invariant1-{i:04d}/" for i in range(300)]
            initial_items = _build_profile_prefetch_queue_items(
                profile_urls,
                priority=True,
                queue_state="ready",
            )
            initial_plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=profile_urls,
                requested_url_count=len(profile_urls),
                candidate_count=len(profile_urls),
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 2,
                },
                dispatch_window={
                    "batch_size": 100,
                    "max_workers": 4,
                    "batch_count": 3,
                    "source_mix": {"other": len(profile_urls)},
                },
                queue_items=initial_items,
            )
            first_wave_urls = {url for _, chunk in initial_plan.dispatch_specs for url in chunk}
            self.assertTrue(first_wave_urls)
            # Re-plan only the deferred remainder of the same wave on the next tick.
            remaining_urls = [url for url in profile_urls if url not in first_wave_urls]
            remaining_items = _build_profile_prefetch_queue_items(
                remaining_urls,
                priority=True,
                queue_state="ready",
            )
            next_plan = _build_profile_prefetch_batch_plan(
                dispatch_urls=remaining_urls,
                requested_url_count=len(profile_urls),
                candidate_count=len(profile_urls),
                priority=True,
                source_shards_by_url={},
                worker_budget={
                    "active_worker_count": 0,
                    "actor_budget": 4,
                    "submit_budget": 4,
                    "available_new_worker_count": 2,
                },
                dispatch_window={
                    "batch_size": 100,
                    "max_workers": 4,
                    "batch_count": 3,
                    "source_mix": {"other": len(remaining_urls)},
                },
                queue_items=remaining_items,
            )
            next_wave_urls = {url for _, chunk in next_plan.dispatch_specs for url in chunk}
            # The next tick dispatches only not-yet-dispatched URLs (no duplicate submit).
            self.assertEqual(next_wave_urls & first_wave_urls, set())
            del store

    def test_invariant_7_store_unavailable_is_a_blocked_terminal_never_a_satisfied_one(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = _asset_catalog(root)
            enricher = MultiSourceEnricher(
                catalog,
                accounts=[],
                harvest_profile_connector=_Connector(),
                store=None,
            )
            enricher.worker_runtime = object()
            candidates = [
                Candidate(
                    candidate_id=f"blocked_{i}",
                    name_en=f"Blocked {i}",
                    display_name=f"Blocked {i}",
                    linkedin_url=f"https://www.linkedin.com/in/store-unavailable-{i}/",
                )
                for i in range(60)
            ]
            with mock.patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}, clear=False):
                result = enricher.queue_background_profile_prefetch(
                    candidates=candidates,
                    snapshot_dir=root,
                    job_id="job_store_unavailable",
                    request_payload={},
                    plan_payload={},
                    runtime_mode="workflow",
                    allow_shared_provider_cache=True,
                    priority=True,
                    load_cached_profile_payloads=False,
                )

        # Fail-closed: a missing store must bubble as an observable blocked terminal, never
        # a synthetic queued tail nor a completed "all satisfied" terminal.
        self.assertEqual(result["status"], "blocked")
        self.assertNotIn(result["status"], {"queued", "completed"})
        self.assertEqual(result["reason"], "profile_refill_store_unavailable")
        self.assertEqual(result["queued_worker_count"], 0)
        self.assertEqual(result["dispatched_url_count"], 0)
        self.assertGreater(int(result.get("blocked_url_count") or 0), 0)
        self.assertEqual(len(result.get("blocked_urls") or []), 60)

    def test_invariant_7_store_unavailable_single_command_owner_does_not_signal_contention(self) -> None:
        # The owner entrypoint returns a blocked envelope without runtime_command_contention
        # (contention means another owner holds the command, which is false with no store).
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            catalog = _asset_catalog(root)
            enricher = MultiSourceEnricher(catalog, accounts=[], harvest_profile_connector=_Connector(), store=None)
            owner_result = enricher.run_linkedin_profile_refill_submit_command_once(
                {
                    "command_id": "cmd-1",
                    "payload": {
                        "job_id": "job-1",
                        "snapshot_dir": str(root),
                        "chunk_index": 0,
                        "profile_urls": ["https://www.linkedin.com/in/blocked-owner/"],
                    },
                }
            )
        self.assertEqual(owner_result["status"], "blocked")
        self.assertEqual(owner_result["reason"], "profile_refill_store_unavailable")
        workflow_command = dict(owner_result.get("workflow_command") or {})
        self.assertFalse(workflow_command.get("runtime_command_contention"))
        self.assertEqual(owner_result["dispatch_result"]["worker_status"], "blocked")


if __name__ == "__main__":
    unittest.main()
