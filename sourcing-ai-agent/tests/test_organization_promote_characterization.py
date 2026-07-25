"""Ruling-③ S0 characterization baseline for the organization-asset promote surface.

Provenance: characterization adopted 2026-07-24 as the WS7/W7.3 mandatory
prerequisite pinned by operator ruling ③ (OQ6 RATIFIED 2026-07-24,
docs/WS7_AI_PROMOTE_DESIGN.md §6/§9): the future AI promote judge replaces the
``evaluate_organization_asset_registry_promotion`` threshold family, and the
CURRENT deterministic ladder is demoted to acceptance validators at the S5 flip.
No promote characterization oracle existed before this suite (the prior tests —
test_organization_execution_profile.py, test_asset_consolidation_audit.py,
test_authoritative_source_provenance.py — are behavioral spot-checks on
hand-picked scenarios, not whole-structure goldens across the threshold-boundary
grid). This suite is that "先钉 promote-decision characterization" prerequisite:
it pins the CURRENT pure inputs→outputs decision surface that the AI judge will
replace, exactly as it behaves today.

Characterized surface (anchors verified on branch governance-phase0-ttl-20260611):
  * evaluate_organization_asset_registry_promotion  asset_reuse_planning.py:1217
      - three deterministic pre-branches (:1224/:1232/:1241)
      - six subsumption thresholds (:1278-1287) + completeness_higher (:1288)
      - four promote branches OR'd (:1346-1350) + score_gap edges (1.0/3.5)
      - LARGE(>=1000)/SMALL absolute-floor switch (:1290-1293)
  * completeness_score formula (feature, NOT a gate)    asset_reuse_planning.py:740-760
      via build_organization_asset_registry_record (:710); the 0.35 explicit-
      capture discount (:734), clamp[0,100], and the high>=75/medium>=50/low bands.
  * select_organization_asset_registry_promotion_candidate  asset_reuse_planning.py:1410
      sort-desc + incumbent skip + first-promote selection + no-candidate/
      no-incumbent/retained shapes.
  * storage lineage guard TWO refusal shapes            storage.py:8633-8695
      stale_generation_sequence_replay (:8675) +
      source_snapshot_coverage_regression (:8681).

CHARACTERIZATION CONTRACT: every golden below was captured empirically against
the current tree and must pass UNCHANGED. The oracle pins reality, warts
included — do not "fix" a golden to look nicer.

TWO DIFFERENT DEMOTION FATES AT THE S5 FLIP (docs/WS7_AI_PROMOTE_DESIGN.md §6):
  * The §1.2 THRESHOLD-FAMILY goldens (pre-branches, the four promote branches,
    the subsumption/completeness/material thresholds, candidate selection, the
    completeness_score formula grid) BECOME acceptance validators at S5. The AI
    judge may only be MORE conservative than these — it can turn a rule-ladder
    ``promote`` into a reject, never the reverse. Rewriting these into the
    validator battery is the S5 slice's job; until then they pin the seat.
  * The storage-guard TWO refusal-shape goldens
    (:StorageLineageGuardRefusalShapeCharacterizationTest) are PERMANENT-HARD.
    They are the fail-closed floor of ruling ③ and are NEVER demoted to
    validators — no AI design may see, bypass, or soften them. They stay green
    across S5 and every later slice unchanged.

Pinned quirks (CURRENT behavior, NOT to be silently repaired here — S0 pins bugs
as-is and flags them; the fix, if any, is an explicit later slice):
  * COUNT/DETAIL/LANE 0.98 RELAXATION IS NEUTRALIZED. The design §1.2 describes
    the candidate_count / profile_detail / effective_lane_total subsumption
    checks as "candidate >= existing x 0.98". The code (:1279/:1281/:1286) wraps
    each in ``max(existing, int(existing * 0.98))`` — and since
    ``int(existing * 0.98) <= existing`` for every non-negative existing, the
    max collapses to ``existing``. The effective rule is therefore
    ``candidate >= existing`` (STRICTER than the documented 0.98 relaxation);
    only the evidence check (:1282, no max) actually applies its 0.95 factor.
    Pinned by ``test_count_098_relaxation_is_neutralized_candidate_must_reach_existing``.
    Flagged in docs/RESIDUAL_LEDGER.md (WS7-S0-Q1). Whether the AI floor (V_COMP)
    keeps this stricter shape is OQ2, decided at S5 — not here.
  * The reason ternary (:1354-1370) resolves ``explicit_baseline_inclusion``
    FIRST, so a candidate that satisfies both the explicit-baseline path AND
    ``completeness_higher and subsumption_higher`` reports reason
    ``explicit_baseline_inclusion`` (see the quality-recovery golden, whose
    ``completeness_higher`` is also True).
"""

from __future__ import annotations

import unittest
from typing import Any

from sourcing_agent.asset_reuse_planning import (
    build_organization_asset_registry_record,
    evaluate_organization_asset_registry_promotion,
    select_organization_asset_registry_promotion_candidate,
)
from tests.pg_store_fixture import pg_backed_control_plane_store

# Fields the promote decision projects; used both as the golden shape for the
# whole-dict branch goldens and as the compact surface for the boundary sweeps.
_DECISION_SURFACE_KEYS = (
    "promote",
    "reason",
    "explicit_baseline_inclusion",
    "explicit_baseline_inclusion_quality_recovery",
    "explicit_baseline_inclusion_promotable",
    "completeness_higher",
    "subsumption_higher",
    "coverage_materially_higher",
    "effective_lane_total_materially_higher",
    "source_snapshot_count_bias",
    "score_gap",
)


def _rec(**overrides: Any) -> dict[str, Any]:
    """A zeroed registry-row record; scenarios override only what differs. These
    are the same fields ``evaluate`` reads today (asset_reuse_planning.py:1254-1269)."""

    record: dict[str, Any] = {
        "snapshot_id": "snap",
        "status": "ready",
        "candidate_count": 0,
        "evidence_count": 0,
        "profile_detail_count": 0,
        "missing_linkedin_count": 0,
        "profile_completion_backlog_count": 0,
        "completeness_score": 0.0,
        "current_lane_effective_candidate_count": 0,
        "former_lane_effective_candidate_count": 0,
        "source_snapshot_count": 0,
    }
    record.update(overrides)
    return record


def _surface(decision: dict[str, Any]) -> dict[str, Any]:
    return {key: decision[key] for key in _DECISION_SURFACE_KEYS}


class PromoteDecisionPreBranchCharacterizationTest(unittest.TestCase):
    """§1.1 deterministic pre-branches — whole-dict goldens (never AI-judged)."""

    def test_lifecycle_not_promotable_rejects_with_thin_record(self) -> None:
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(snapshot_id="inc", candidate_count=100),
                candidate_record=_rec(snapshot_id="cand", status="archived", candidate_count=200),
            ),
            {
                "promote": False,
                "reason": "lifecycle_state_not_promotable",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "cand",
                "candidate_lifecycle_status": "archived",
            },
        )

    def test_no_incumbent_promotes(self) -> None:
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=None,
                candidate_record=_rec(snapshot_id="cand", candidate_count=200),
            ),
            {
                "promote": True,
                "reason": "no_existing_authoritative",
                "existing_snapshot_id": "",
                "candidate_snapshot_id": "cand",
            },
        )

    def test_same_snapshot_refresh_promotes(self) -> None:
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(snapshot_id="same", candidate_count=100),
                candidate_record=_rec(snapshot_id="same", candidate_count=200),
            ),
            {
                "promote": True,
                "reason": "same_snapshot_refresh",
                "existing_snapshot_id": "same",
                "candidate_snapshot_id": "same",
            },
        )


class PromoteContestedBranchCharacterizationTest(unittest.TestCase):
    """§1.2 the four promote branches + reject — WHOLE-DICT goldens.

    Each golden is the entire decision dict for a candidate shape that fires
    exactly one branch (plus the reject shape). assertEqual over the whole dict
    means any added/removed/renamed key OR any drift in the boolean ladder,
    metrics mirror, or score_gap fails the oracle."""

    def test_branch_higher_completeness_and_subsumption(self) -> None:
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(
                    snapshot_id="inc", candidate_count=100, profile_detail_count=100,
                    evidence_count=100, completeness_score=80.0,
                    current_lane_effective_candidate_count=100,
                ),
                candidate_record=_rec(
                    snapshot_id="cand", candidate_count=110, profile_detail_count=110,
                    evidence_count=110, completeness_score=82.0,
                    current_lane_effective_candidate_count=110, selected_snapshot_ids=["cand"],
                ),
            ),
            {
                "promote": True,
                "reason": "higher_completeness_and_subsumption",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "cand",
                "selected_snapshot_ids": ["cand"],
                "explicit_baseline_inclusion": False,
                "explicit_baseline_inclusion_quality_recovery": False,
                "explicit_baseline_inclusion_promotable": False,
                "completeness_higher": True,
                "subsumption_higher": True,
                "coverage_materially_higher": False,
                "effective_lane_total_materially_higher": False,
                "source_snapshot_count_bias": False,
                "score_gap": -2.0,
                "existing_metrics": {
                    "candidate_count": 100, "evidence_count": 100, "profile_detail_count": 100,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 80.0, "effective_lane_total": 100, "source_snapshot_count": 0,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
                "candidate_metrics": {
                    "candidate_count": 110, "evidence_count": 110, "profile_detail_count": 110,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 82.0, "effective_lane_total": 110, "source_snapshot_count": 0,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
            },
        )

    def test_branch_materially_higher_coverage_with_stable_quality(self) -> None:
        # The recon incident-recovery shape (equal score, wider lane coverage).
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(
                    snapshot_id="20260422T151623", candidate_count=332, profile_detail_count=332,
                    completeness_score=88.0, current_lane_effective_candidate_count=252,
                    former_lane_effective_candidate_count=80, source_snapshot_count=9,
                ),
                candidate_record=_rec(
                    snapshot_id="20260422T171923", candidate_count=404, profile_detail_count=404,
                    completeness_score=88.0, current_lane_effective_candidate_count=293,
                    former_lane_effective_candidate_count=111, source_snapshot_count=11,
                ),
            ),
            {
                "promote": True,
                "reason": "materially_higher_coverage_with_stable_quality",
                "existing_snapshot_id": "20260422T151623",
                "candidate_snapshot_id": "20260422T171923",
                "selected_snapshot_ids": [],
                "explicit_baseline_inclusion": False,
                "explicit_baseline_inclusion_quality_recovery": False,
                "explicit_baseline_inclusion_promotable": False,
                "completeness_higher": False,
                "subsumption_higher": True,
                "coverage_materially_higher": True,
                "effective_lane_total_materially_higher": True,
                "source_snapshot_count_bias": False,
                "score_gap": 0.0,
                "existing_metrics": {
                    "candidate_count": 332, "evidence_count": 0, "profile_detail_count": 332,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 88.0, "effective_lane_total": 332, "source_snapshot_count": 9,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
                "candidate_metrics": {
                    "candidate_count": 404, "evidence_count": 0, "profile_detail_count": 404,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 88.0, "effective_lane_total": 404, "source_snapshot_count": 11,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
            },
        )

    def test_branch_materially_higher_coverage_despite_snapshot_count_bias(self) -> None:
        # subsumption_higher, not completeness_higher, coverage material, incumbent
        # has MORE source snapshots than the candidate (bias), 1.0 < gap <= 3.5.
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(
                    snapshot_id="inc", candidate_count=200, profile_detail_count=200,
                    completeness_score=90.0, current_lane_effective_candidate_count=200,
                    source_snapshot_count=9,
                ),
                candidate_record=_rec(
                    snapshot_id="cand", candidate_count=260, profile_detail_count=260,
                    completeness_score=87.0, current_lane_effective_candidate_count=260,
                    source_snapshot_count=3, selected_snapshot_ids=["cand"],
                ),
            ),
            {
                "promote": True,
                "reason": "materially_higher_coverage_despite_snapshot_count_bias",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "cand",
                "selected_snapshot_ids": ["cand"],
                "explicit_baseline_inclusion": False,
                "explicit_baseline_inclusion_quality_recovery": False,
                "explicit_baseline_inclusion_promotable": False,
                "completeness_higher": False,
                "subsumption_higher": True,
                "coverage_materially_higher": True,
                "effective_lane_total_materially_higher": True,
                "source_snapshot_count_bias": True,
                "score_gap": 3.0,
                "existing_metrics": {
                    "candidate_count": 200, "evidence_count": 0, "profile_detail_count": 200,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 90.0, "effective_lane_total": 200, "source_snapshot_count": 9,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
                "candidate_metrics": {
                    "candidate_count": 260, "evidence_count": 0, "profile_detail_count": 260,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 87.0, "effective_lane_total": 260, "source_snapshot_count": 3,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
            },
        )

    def test_branch_explicit_baseline_inclusion_via_subsumption(self) -> None:
        # Incumbent snapshot is in the candidate's selected_snapshot_ids, the
        # candidate sorts strictly higher, subsumption holds and gap <= 1.0.
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(
                    snapshot_id="inc", candidate_count=100, profile_detail_count=100,
                    evidence_count=100, completeness_score=80.0,
                    current_lane_effective_candidate_count=100,
                ),
                candidate_record=_rec(
                    snapshot_id="cand", candidate_count=120, profile_detail_count=120,
                    evidence_count=120, completeness_score=80.5,
                    current_lane_effective_candidate_count=120, selected_snapshot_ids=["inc", "cand"],
                ),
            ),
            {
                "promote": True,
                "reason": "explicit_baseline_inclusion",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "cand",
                "selected_snapshot_ids": ["inc", "cand"],
                "explicit_baseline_inclusion": True,
                "explicit_baseline_inclusion_quality_recovery": False,
                "explicit_baseline_inclusion_promotable": True,
                "completeness_higher": False,
                "subsumption_higher": True,
                "coverage_materially_higher": True,
                "effective_lane_total_materially_higher": True,
                "source_snapshot_count_bias": False,
                "score_gap": -0.5,
                "existing_metrics": {
                    "candidate_count": 100, "evidence_count": 100, "profile_detail_count": 100,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 80.0, "effective_lane_total": 100, "source_snapshot_count": 0,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
                "candidate_metrics": {
                    "candidate_count": 120, "evidence_count": 120, "profile_detail_count": 120,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 80.5, "effective_lane_total": 120, "source_snapshot_count": 0,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
            },
        )

    def test_branch_explicit_baseline_inclusion_quality_recovery(self) -> None:
        # The sub-50-score quality-recovery clause (:1323-1330). NOTE the pinned
        # quirk: completeness_higher is ALSO True here, but the reason ternary
        # resolves explicit_baseline_inclusion first.
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(
                    snapshot_id="inc", candidate_count=100, profile_detail_count=40,
                    evidence_count=40, completeness_score=40.0, missing_linkedin_count=30,
                    profile_completion_backlog_count=30, current_lane_effective_candidate_count=100,
                ),
                candidate_record=_rec(
                    snapshot_id="cand", candidate_count=100, profile_detail_count=70,
                    evidence_count=70, completeness_score=55.0, missing_linkedin_count=10,
                    profile_completion_backlog_count=5, current_lane_effective_candidate_count=100,
                    selected_snapshot_ids=["inc", "cand"],
                ),
            ),
            {
                "promote": True,
                "reason": "explicit_baseline_inclusion",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "cand",
                "selected_snapshot_ids": ["inc", "cand"],
                "explicit_baseline_inclusion": True,
                "explicit_baseline_inclusion_quality_recovery": True,
                "explicit_baseline_inclusion_promotable": True,
                "completeness_higher": True,
                "subsumption_higher": True,
                "coverage_materially_higher": False,
                "effective_lane_total_materially_higher": False,
                "source_snapshot_count_bias": False,
                "score_gap": -15.0,
                "existing_metrics": {
                    "candidate_count": 100, "evidence_count": 40, "profile_detail_count": 40,
                    "missing_linkedin_count": 30, "profile_completion_backlog_count": 30,
                    "completeness_score": 40.0, "effective_lane_total": 100, "source_snapshot_count": 0,
                    "missing_ratio": 0.3, "profile_gap_ratio": 0.3,
                },
                "candidate_metrics": {
                    "candidate_count": 100, "evidence_count": 70, "profile_detail_count": 70,
                    "missing_linkedin_count": 10, "profile_completion_backlog_count": 5,
                    "completeness_score": 55.0, "effective_lane_total": 100, "source_snapshot_count": 0,
                    "missing_ratio": 0.1, "profile_gap_ratio": 0.05,
                },
            },
        )

    def test_contested_thinner_candidate_is_rejected(self) -> None:
        # No branch fires: thinner candidate, lower score. reason "guard_rejected".
        self.assertEqual(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=_rec(
                    snapshot_id="inc", candidate_count=200, profile_detail_count=200,
                    completeness_score=90.0, current_lane_effective_candidate_count=200,
                    source_snapshot_count=3,
                ),
                candidate_record=_rec(
                    snapshot_id="cand", candidate_count=150, profile_detail_count=150,
                    completeness_score=70.0, current_lane_effective_candidate_count=150,
                    source_snapshot_count=1, selected_snapshot_ids=["cand"],
                ),
            ),
            {
                "promote": False,
                "reason": "guard_rejected",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "cand",
                "selected_snapshot_ids": ["cand"],
                "explicit_baseline_inclusion": False,
                "explicit_baseline_inclusion_quality_recovery": False,
                "explicit_baseline_inclusion_promotable": False,
                "completeness_higher": False,
                "subsumption_higher": False,
                "coverage_materially_higher": False,
                "effective_lane_total_materially_higher": False,
                "source_snapshot_count_bias": True,
                "score_gap": 20.0,
                "existing_metrics": {
                    "candidate_count": 200, "evidence_count": 0, "profile_detail_count": 200,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 90.0, "effective_lane_total": 200, "source_snapshot_count": 3,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
                "candidate_metrics": {
                    "candidate_count": 150, "evidence_count": 0, "profile_detail_count": 150,
                    "missing_linkedin_count": 0, "profile_completion_backlog_count": 0,
                    "completeness_score": 70.0, "effective_lane_total": 150, "source_snapshot_count": 1,
                    "missing_ratio": 0.0, "profile_gap_ratio": 0.0,
                },
            },
        )


class PromoteThresholdBoundaryCharacterizationTest(unittest.TestCase):
    """§1.2 exact decision surface on each threshold edge (boundary sweep).

    Compact projection (`_DECISION_SURFACE_KEYS`) mirrors the divider oracle's
    truth-table style; whole-dict branch shapes are pinned above. These sweeps
    walk each threshold across its pass/fail edge so the ladder's exact decision
    surface is captured."""

    def _decide(self, *, existing: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
        return _surface(
            evaluate_organization_asset_registry_promotion(
                existing_authoritative=existing, candidate_record=candidate
            )
        )

    def test_count_098_relaxation_is_neutralized_candidate_must_reach_existing(self) -> None:
        # PINNED QUIRK: existing_count=100, candidate_count=99. 99 >= int(100*0.98)=98
        # but 99 < max(100, 98)=100, so subsumption FAILS. The documented 0.98
        # relaxation is neutralized by the max(existing, ...) wrap (see module
        # docstring + RESIDUAL_LEDGER WS7-S0-Q1). Everything else is richer, yet
        # the count shortfall alone sinks subsumption → reject.
        self.assertEqual(
            self._decide(
                existing=_rec(candidate_count=100, profile_detail_count=100, completeness_score=80.0,
                              current_lane_effective_candidate_count=100),
                candidate=_rec(snapshot_id="c", candidate_count=99, profile_detail_count=200,
                               evidence_count=200, completeness_score=90.0,
                               current_lane_effective_candidate_count=200),
            ),
            {
                "promote": False, "reason": "guard_rejected",
                "explicit_baseline_inclusion": False,
                "explicit_baseline_inclusion_quality_recovery": False,
                "explicit_baseline_inclusion_promotable": False,
                "completeness_higher": True, "subsumption_higher": False,
                "coverage_materially_higher": True, "effective_lane_total_materially_higher": True,
                "source_snapshot_count_bias": False, "score_gap": -10.0,
            },
        )

    def test_evidence_095_factor_is_the_only_active_subsumption_relaxation(self) -> None:
        # existing_evidence=100 → floor int(100*0.95)=95. candidate 95 passes; 94 fails.
        base_existing = dict(candidate_count=100, profile_detail_count=100, evidence_count=100,
                             completeness_score=80.0, current_lane_effective_candidate_count=100)
        passing = self._decide(
            existing=_rec(**base_existing),
            candidate=_rec(snapshot_id="c", candidate_count=100, profile_detail_count=100,
                           evidence_count=95, completeness_score=82.0,
                           current_lane_effective_candidate_count=100),
        )
        failing = self._decide(
            existing=_rec(**base_existing),
            candidate=_rec(snapshot_id="c", candidate_count=100, profile_detail_count=100,
                           evidence_count=94, completeness_score=82.0,
                           current_lane_effective_candidate_count=100),
        )
        self.assertTrue(passing["subsumption_higher"])
        self.assertEqual(passing["reason"], "higher_completeness_and_subsumption")
        self.assertTrue(passing["promote"])
        self.assertFalse(failing["subsumption_higher"])
        self.assertEqual(failing["reason"], "guard_rejected")
        self.assertFalse(failing["promote"])

    def test_missing_ratio_tolerance_edge_is_plus_0_01(self) -> None:
        base_existing = dict(candidate_count=100, profile_detail_count=100, evidence_count=100,
                             completeness_score=80.0, missing_linkedin_count=0,
                             current_lane_effective_candidate_count=100)
        # candidate missing 1/100 = 0.01 <= 0 + 0.01 → passes.
        at_edge = self._decide(
            existing=_rec(**base_existing),
            candidate=_rec(snapshot_id="c", candidate_count=100, profile_detail_count=100,
                           evidence_count=100, completeness_score=82.0, missing_linkedin_count=1,
                           current_lane_effective_candidate_count=100),
        )
        # candidate missing 2/100 = 0.02 > 0.01 → fails.
        over_edge = self._decide(
            existing=_rec(**base_existing),
            candidate=_rec(snapshot_id="c", candidate_count=100, profile_detail_count=100,
                           evidence_count=100, completeness_score=82.0, missing_linkedin_count=2,
                           current_lane_effective_candidate_count=100),
        )
        self.assertTrue(at_edge["subsumption_higher"])
        self.assertFalse(over_edge["subsumption_higher"])

    def test_profile_gap_ratio_tolerance_edge_is_plus_0_02(self) -> None:
        base_existing = dict(candidate_count=100, profile_detail_count=100, evidence_count=100,
                             completeness_score=80.0, profile_completion_backlog_count=0,
                             current_lane_effective_candidate_count=100)
        # candidate gap 2/100 = 0.02 <= 0 + 0.02 → passes.
        at_edge = self._decide(
            existing=_rec(**base_existing),
            candidate=_rec(snapshot_id="c", candidate_count=100, profile_detail_count=100,
                           evidence_count=100, completeness_score=82.0,
                           profile_completion_backlog_count=2, current_lane_effective_candidate_count=100),
        )
        # candidate gap 3/100 = 0.03 > 0.02 → fails.
        over_edge = self._decide(
            existing=_rec(**base_existing),
            candidate=_rec(snapshot_id="c", candidate_count=100, profile_detail_count=100,
                           evidence_count=100, completeness_score=82.0,
                           profile_completion_backlog_count=3, current_lane_effective_candidate_count=100),
        )
        self.assertTrue(at_edge["subsumption_higher"])
        self.assertFalse(over_edge["subsumption_higher"])

    def test_stable_quality_branch_score_gap_edge_is_1_0(self) -> None:
        # gap exactly 1.0 → promote via stable-quality; gap 1.5 with no snapshot
        # bias → reject (branch C needs bias, branch D needs gap <= 1.0).
        at_edge = self._decide(
            existing=_rec(candidate_count=332, profile_detail_count=332, completeness_score=89.0,
                          current_lane_effective_candidate_count=252, former_lane_effective_candidate_count=80,
                          source_snapshot_count=9),
            candidate=_rec(snapshot_id="c", candidate_count=404, profile_detail_count=404,
                           completeness_score=88.0, current_lane_effective_candidate_count=293,
                           former_lane_effective_candidate_count=111, source_snapshot_count=11),
        )
        over_edge = self._decide(
            existing=_rec(candidate_count=332, profile_detail_count=332, completeness_score=89.5,
                          current_lane_effective_candidate_count=252, former_lane_effective_candidate_count=80,
                          source_snapshot_count=9),
            candidate=_rec(snapshot_id="c", candidate_count=404, profile_detail_count=404,
                           completeness_score=88.0, current_lane_effective_candidate_count=293,
                           former_lane_effective_candidate_count=111, source_snapshot_count=11),
        )
        self.assertEqual(at_edge["score_gap"], 1.0)
        self.assertEqual(at_edge["reason"], "materially_higher_coverage_with_stable_quality")
        self.assertTrue(at_edge["promote"])
        self.assertEqual(over_edge["score_gap"], 1.5)
        self.assertEqual(over_edge["reason"], "guard_rejected")
        self.assertFalse(over_edge["promote"])

    def test_snapshot_count_bias_branch_score_gap_edge_is_3_5(self) -> None:
        # With snapshot-count bias present: gap 3.5 promotes; gap 3.6 rejects.
        at_edge = self._decide(
            existing=_rec(candidate_count=200, profile_detail_count=200, completeness_score=90.5,
                          current_lane_effective_candidate_count=200, source_snapshot_count=9),
            candidate=_rec(snapshot_id="c", candidate_count=260, profile_detail_count=260,
                           completeness_score=87.0, current_lane_effective_candidate_count=260,
                           source_snapshot_count=3),
        )
        over_edge = self._decide(
            existing=_rec(candidate_count=200, profile_detail_count=200, completeness_score=90.6,
                          current_lane_effective_candidate_count=200, source_snapshot_count=9),
            candidate=_rec(snapshot_id="c", candidate_count=260, profile_detail_count=260,
                           completeness_score=87.0, current_lane_effective_candidate_count=260,
                           source_snapshot_count=3),
        )
        self.assertEqual(at_edge["score_gap"], 3.5)
        self.assertEqual(at_edge["reason"], "materially_higher_coverage_despite_snapshot_count_bias")
        self.assertTrue(at_edge["promote"])
        self.assertEqual(over_edge["score_gap"], 3.6)
        self.assertEqual(over_edge["reason"], "guard_rejected")
        self.assertFalse(over_edge["promote"])

    def test_large_org_absolute_floor_switches_to_100_at_1000_count(self) -> None:
        # max(existing, candidate) >= 1000 → absolute floor 100 (not 20). With
        # existing=1000, a +99 candidate misses the 100 floor and rejects; +100
        # reaches it and promotes via material coverage.
        just_below = self._decide(
            existing=_rec(candidate_count=1000, profile_detail_count=1000, completeness_score=88.0,
                          current_lane_effective_candidate_count=1000, source_snapshot_count=2),
            candidate=_rec(snapshot_id="c", candidate_count=1099, profile_detail_count=1099,
                           completeness_score=88.0, current_lane_effective_candidate_count=1099,
                           source_snapshot_count=2, selected_snapshot_ids=["c"]),
        )
        at_floor = self._decide(
            existing=_rec(candidate_count=1000, profile_detail_count=1000, completeness_score=88.0,
                          current_lane_effective_candidate_count=1000, source_snapshot_count=2),
            candidate=_rec(snapshot_id="c", candidate_count=1100, profile_detail_count=1100,
                           completeness_score=88.0, current_lane_effective_candidate_count=1100,
                           source_snapshot_count=2, selected_snapshot_ids=["c"]),
        )
        self.assertFalse(just_below["coverage_materially_higher"])
        self.assertEqual(just_below["reason"], "guard_rejected")
        self.assertFalse(just_below["promote"])
        self.assertTrue(at_floor["coverage_materially_higher"])
        self.assertEqual(at_floor["reason"], "materially_higher_coverage_with_stable_quality")
        self.assertTrue(at_floor["promote"])


class CompletenessScoreFormulaCharacterizationTest(unittest.TestCase):
    """§1.3 the completeness_score formula grid (feature, not a gate).

    Driven through build_organization_asset_registry_record (the only caller
    that computes it). The formula stays a computed AI feature per ruling OQ2;
    this grid pins input→(score, band) so a future refactor can't silently drift
    it. Formula (asset_reuse_planning.py:740): 35 + coverage*45 − missing*18 −
    gap*10 − manual*5 + min(8, snapshot_count*1.5), clamp[0,100]; the 0.35
    explicit-capture discount folds into coverage; bands high>=75/medium>=50/low."""

    GOLDEN_GRID: tuple[tuple[dict[str, int], float, str], ...] = (
        # (summary overrides, expected_score, expected_band)
        ({}, 35.0, "low"),                                                        # base only, no candidates
        ({"candidate_count": 100, "profile_detail_count": 100}, 80.0, "high"),    # full coverage
        ({"candidate_count": 100, "profile_detail_count": 0}, 35.0, "low"),       # no coverage
        ({"candidate_count": 100, "profile_detail_count": 50,
          "missing_linkedin_count": 50}, 48.5, "low"),                            # missing penalty
        ({"candidate_count": 100, "profile_detail_count": 100,
          "profile_completion_backlog_count": 100}, 70.0, "medium"),             # gap penalty
        ({"candidate_count": 100, "profile_detail_count": 100,
          "manual_review_backlog_count": 100}, 75.0, "high"),                     # manual penalty
        ({"candidate_count": 100, "profile_detail_count": 100,
          "source_snapshot_count": 10}, 88.0, "high"),                            # snapshot bonus capped at +8
        ({"candidate_count": 100, "profile_detail_count": 100,
          "source_snapshot_count": 2}, 83.0, "high"),                             # snapshot bonus 2*1.5=3
        ({"candidate_count": 100, "profile_detail_count": 0,
          "explicit_profile_capture_count": 100}, 50.75, "medium"),              # 0.35 discount full
        ({"candidate_count": 100, "profile_detail_count": 0,
          "explicit_profile_capture_count": 50}, 42.88, "low"),                   # 0.35 discount half
        ({"candidate_count": 100, "profile_detail_count": 200}, 80.0, "high"),    # coverage clamped to 1.0
        ({"candidate_count": 10, "profile_detail_count": 0,
          "missing_linkedin_count": 100}, 0.0, "low"),                            # clamped to 0 floor
        ({"candidate_count": 100, "profile_detail_count": 75,
          "source_snapshot_count": 1}, 70.25, "medium"),                          # medium band interior
    )

    def test_completeness_score_formula_golden_grid(self) -> None:
        for summary, expected_score, expected_band in self.GOLDEN_GRID:
            with self.subTest(summary=summary):
                record = build_organization_asset_registry_record(
                    target_company="ACME",
                    company_key="acme",
                    snapshot_id="s",
                    asset_view="canonical_merged",
                    summary=dict(summary),
                )
                self.assertEqual(record["completeness_score"], expected_score)
                self.assertEqual(record["completeness_band"], expected_band)


class PromotionCandidateSelectionCharacterizationTest(unittest.TestCase):
    """§1.2 candidate selection loop — sort-desc, incumbent skip, first-promote."""

    def _cand(self, snapshot_id: str, registry_id: int, *, count: int, score: float) -> dict[str, Any]:
        return _rec(
            snapshot_id=snapshot_id, registry_id=registry_id, candidate_count=count,
            profile_detail_count=count, completeness_score=score,
            current_lane_effective_candidate_count=count,
        )

    def test_no_candidates_returns_empty_pair(self) -> None:
        self.assertEqual(
            select_organization_asset_registry_promotion_candidate(
                candidate_records=[], existing_authoritative=None
            ),
            ({}, {}),
        )

    def test_no_incumbent_selects_the_top_sorted_candidate(self) -> None:
        c1 = self._cand("c1", 1, count=100, score=70.0)
        c2 = self._cand("c2", 2, count=200, score=90.0)
        selected, decision = select_organization_asset_registry_promotion_candidate(
            candidate_records=[c1, c2], existing_authoritative=None
        )
        self.assertEqual(selected["snapshot_id"], "c2")
        self.assertEqual(
            decision,
            {
                "promote": True,
                "reason": "no_existing_authoritative",
                "existing_snapshot_id": "",
                "candidate_snapshot_id": "c2",
            },
        )

    def test_first_sorted_promotable_wins_and_incumbent_is_skipped(self) -> None:
        c1 = self._cand("c1", 1, count=100, score=70.0)
        c2 = self._cand("c2", 2, count=200, score=90.0)
        incumbent = self._cand("inc", 9, count=150, score=80.0)
        selected, decision = select_organization_asset_registry_promotion_candidate(
            candidate_records=[c1, c2], existing_authoritative=incumbent
        )
        self.assertEqual(selected["snapshot_id"], "c2")
        self.assertTrue(decision["promote"])
        self.assertEqual(decision["reason"], "higher_completeness_and_subsumption")
        self.assertEqual(decision["candidate_snapshot_id"], "c2")

    def test_all_candidates_reject_keeps_incumbent(self) -> None:
        c1 = self._cand("c1", 1, count=100, score=70.0)
        c2 = self._cand("c2", 2, count=200, score=90.0)
        incumbent = self._cand("inc", 9, count=500, score=95.0)
        selected, decision = select_organization_asset_registry_promotion_candidate(
            candidate_records=[c1, c2], existing_authoritative=incumbent
        )
        self.assertEqual(selected["snapshot_id"], "inc")
        self.assertEqual(
            decision,
            {
                "promote": False,
                "reason": "existing_authoritative_retained",
                "existing_snapshot_id": "inc",
                "candidate_snapshot_id": "inc",
            },
        )


class StorageLineageGuardRefusalShapeCharacterizationTest(unittest.TestCase):
    """§1.4 the storage lineage guard — the TWO refusal shapes, PERMANENT-HARD.

    Unlike the threshold-family goldens above (which become acceptance validators
    at the S5 flip and the AI may only tighten), THESE goldens are the fail-closed
    floor of ruling ③ (docs/WS7_AI_PROMOTE_DESIGN.md §1.4/§6). They are NEVER
    demoted to validators — no AI design may see, bypass, or soften them. This
    suite pins the whole ``authoritative_promotion_refused`` record shape (the
    sibling test_asset_consolidation_audit.py spot-checks individual keys); on
    refusal the row still lands non-authoritative and the incumbent stays.

    PG-backed (the guard runs against the in-transaction existing_rows read,
    storage.py:8615 — it structurally cannot be a pure function). Skips when no
    local Postgres is available unless SOURCING_REQUIRE_PG_STORE_TESTS=1."""

    @staticmethod
    def _payload(
        snapshot_id: str, *, sequence: int, generation_key: str = "",
        selected: list[str] | None = None, **overrides: Any,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "target_company": "OpenAI",
            "company_key": "openai",
            "snapshot_id": snapshot_id,
            "asset_view": "canonical_merged",
            "status": "ready",
            "candidate_count": 10,
            "materialization_generation_key": generation_key or f"gen-{snapshot_id}",
            "materialization_generation_sequence": sequence,
            "selected_snapshot_ids": list(selected if selected is not None else [snapshot_id]),
        }
        payload.update(overrides)
        return payload

    def _authoritative_map(self, store: Any) -> dict[str, int]:
        rows = store.list_organization_asset_registry(target_company="OpenAI")
        return {str(r["snapshot_id"]): int(bool(r["authoritative"])) for r in rows}

    def test_stale_generation_sequence_replay_refusal_shape(self) -> None:
        with pg_backed_control_plane_store(
            schema_label="promote_oracle_stale_generation"
        ) as store:
            store.upsert_organization_asset_registry(
                self._payload("snap-current", sequence=6, generation_key="lineage-a"),
                authoritative=True,
            )
            result = store.upsert_organization_asset_registry(
                self._payload("snap-stale", sequence=3, generation_key="lineage-a"),
                authoritative=True,
            )
            self.assertEqual(
                result["authoritative_promotion_refused"],
                {
                    "reason": "stale_generation_sequence_replay",
                    "incoming_snapshot_id": "snap-stale",
                    "incoming_generation_key": "lineage-a",
                    "incoming_generation_sequence": 3,
                    "incoming_selected_snapshot_ids": ["snap-stale"],
                    "blocking_snapshot_id": "snap-current",
                    "blocking_generation_key": "lineage-a",
                    "blocking_generation_sequence": 6,
                    "blocking_selected_snapshot_ids": ["snap-current"],
                },
            )
            # The refused row lands non-authoritative; the incumbent stays.
            self.assertFalse(bool(result["authoritative"]))
            self.assertEqual(self._authoritative_map(store), {"snap-current": 1, "snap-stale": 0})

    def test_source_snapshot_coverage_regression_refusal_shape(self) -> None:
        # The 2026-07-22 incident: incumbent merged {104157, 041551}; a stale-job
        # recovery re-materialized 041551 selecting only itself (strict subset).
        with pg_backed_control_plane_store(
            schema_label="promote_oracle_coverage_regression"
        ) as store:
            store.upsert_organization_asset_registry(
                self._payload("snap-104157", sequence=6, selected=["snap-104157", "snap-041551"]),
                authoritative=True,
            )
            result = store.upsert_organization_asset_registry(
                self._payload("snap-041551", sequence=3, selected=["snap-041551"]),
                authoritative=True,
            )
            self.assertEqual(
                result["authoritative_promotion_refused"],
                {
                    "reason": "source_snapshot_coverage_regression",
                    "incoming_snapshot_id": "snap-041551",
                    "incoming_generation_key": "gen-snap-041551",
                    "incoming_generation_sequence": 3,
                    "incoming_selected_snapshot_ids": ["snap-041551"],
                    "blocking_snapshot_id": "snap-104157",
                    "blocking_generation_key": "gen-snap-104157",
                    "blocking_generation_sequence": 6,
                    "blocking_selected_snapshot_ids": ["snap-041551", "snap-104157"],
                },
            )
            self.assertFalse(bool(result["authoritative"]))
            self.assertEqual(self._authoritative_map(store), {"snap-104157": 1, "snap-041551": 0})


if __name__ == "__main__":
    unittest.main()
