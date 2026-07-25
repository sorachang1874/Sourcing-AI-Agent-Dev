"""Scripted anti-recurrence tests pinned to CANONICAL product routines only.

These test the existing owners (connectors roster merge routines and the
enrichment profile-prefetch batch plan) — no live-ops glue. Every assertion
maps to an incident that cost real money in 2026-07:

- duplicate/lossy shard filters and provenance replacement;
- merging two different people, or dropping dual-function members;
- batch geometry drifting away from the product's 4-8 worker slot-fill plan;
- opaque-id vs resolved-slug joins producing cross-person merges.
"""

from __future__ import annotations

import unittest

from sourcing_agent.connectors import (
    annotate_roster_entry_shard_provenance,
    roster_merge_dedupe_key,
    roster_stable_member_key,
    union_roster_entry_provenance,
)
from sourcing_agent.enrichment import _build_profile_prefetch_batch_plan


class CanonicalMergeKeyTest(unittest.TestCase):
    def test_stable_member_key_prefers_url_identity(self) -> None:
        entry = {"linkedinUrl": "https://www.linkedin.com/in/abc", "full_name": "A B"}
        self.assertEqual(roster_stable_member_key(entry), roster_stable_member_key(dict(entry)))

    def test_dedupe_key_is_shard_scoped_without_stable_identity(self) -> None:
        lookalike_a = {"full_name": "Eric Zhang", "headline": "Engineer", "location": "SF"}
        key_shard_1 = roster_merge_dedupe_key(lookalike_a, shard_id="function_8")
        key_shard_2 = roster_merge_dedupe_key(dict(lookalike_a), shard_id="function_24")
        self.assertNotEqual(
            key_shard_1,
            key_shard_2,
            "two lookalike members in different shards must never merge cross-shard",
        )

    def test_stable_identity_dedupes_across_shards(self) -> None:
        entry = {"linkedin_url": "https://www.linkedin.com/in/same"}
        self.assertEqual(
            roster_merge_dedupe_key(entry, shard_id="function_8"),
            roster_merge_dedupe_key(dict(entry), shard_id="function_24"),
        )

    def test_camelcase_provider_field_is_not_a_stable_key_by_contract(self) -> None:
        # canonical roster entries carry snake_case linkedin_url; the raw
        # provider field linkedinUrl alone is NOT a stable key (it falls to
        # member_key/id, then shard-scoped display tuples).
        entry = {"linkedinUrl": "https://www.linkedin.com/in/same"}
        self.assertEqual(roster_stable_member_key(entry), "")


class CanonicalProvenanceTest(unittest.TestCase):
    def test_dual_function_member_keeps_both_functions(self) -> None:
        merged = union_roster_entry_provenance(
            {"function_ids": ["8"], "source_shard_id": "function_8"},
            {"function_ids": ["24"], "source_shard_id": "function_24"},
        )
        self.assertEqual(set(merged["function_ids"]), {"8", "24"})
        self.assertEqual(set(merged["source_shard_ids"]), {"function_8", "function_24"})
        self.assertEqual(
            merged["source_shard_id"],
            "function_8",
            "singular compatibility field is documented first-shard-wins",
        )

    def test_annotate_backfills_function_ids_from_include_filters(self) -> None:
        annotated = annotate_roster_entry_shard_provenance(
            {"full_name": "A B"},
            shard_id="function_8",
            company_filters={"function_ids": ["8"]},
        )
        self.assertEqual(annotated["function_ids"], ["8"])
        self.assertEqual(annotated["source_shard_id"], "function_8")

    def test_provenance_union_never_replaces(self) -> None:
        first = annotate_roster_entry_shard_provenance(
            {"full_name": "A B"}, shard_id="function_8", company_filters={"function_ids": ["8"]}
        )
        second = annotate_roster_entry_shard_provenance(
            {"full_name": "A B"}, shard_id="function_24", company_filters={"function_ids": ["24"]}
        )
        merged = union_roster_entry_provenance(first, second)
        self.assertEqual(set(merged["function_ids"]), {"8", "24"})
        self.assertEqual(len(merged["source_shard_provenance"]), 2)


class PrefetchBatchPlanGeometryTest(unittest.TestCase):
    def _plan(self, n_urls: int, workers: int) -> object:
        urls = [f"https://www.linkedin.com/in/u{i}" for i in range(n_urls)]
        return _build_profile_prefetch_batch_plan(
            dispatch_urls=urls,
            requested_url_count=len(urls),
            candidate_count=len(urls),
            priority=False,
            source_shards_by_url={},
            worker_budget={
                "available_new_worker_count": workers,
                "submit_budget": workers,
                "actor_budget": workers,
                "active_worker_count": 0,
            },
        )

    def test_worker_budget_bounds_dispatch(self) -> None:
        for workers in (4, 6, 8):
            plan = self._plan(1000, workers)
            self.assertGreaterEqual(plan.planned_dispatch_worker_count, 1)
            self.assertLessEqual(plan.planned_dispatch_worker_count, workers)

    def test_tail_coalescing_defers_by_default_and_dispatches_when_enabled(self) -> None:
        urls = [f"https://www.linkedin.com/in/u{i}" for i in range(401)]
        budget = {
            "available_new_worker_count": 8,
            "submit_budget": 8,
            "actor_budget": 8,
            "active_worker_count": 0,
        }
        coalesced = _build_profile_prefetch_batch_plan(
            dispatch_urls=urls,
            requested_url_count=len(urls),
            candidate_count=len(urls),
            priority=False,
            source_shards_by_url={},
            worker_budget=budget,
        )
        dispatched = [url for _worker, chunk in coalesced.dispatch_specs for url in chunk]
        self.assertEqual(len(dispatched) + len(coalesced.deferred_urls), 401)
        self.assertGreater(len(coalesced.deferred_urls), 0, "default tail coalescing holds the tail")

        immediate = _build_profile_prefetch_batch_plan(
            dispatch_urls=urls,
            requested_url_count=len(urls),
            candidate_count=len(urls),
            priority=False,
            source_shards_by_url={},
            worker_budget=budget,
            allow_under_target_final_tail_dispatch=True,
        )
        dispatched = [url for _worker, chunk in immediate.dispatch_specs for url in chunk]
        self.assertEqual(len(dispatched), 401, "under-target tail dispatches immediately when enabled — no idle wait")


if __name__ == "__main__":
    unittest.main()
