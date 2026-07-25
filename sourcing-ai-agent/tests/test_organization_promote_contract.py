"""WS7/W7.3 S1 offline suite for `sourcing.organization_asset.ai_promote_decision.v1`.

Provenance: docs/WS7_AI_PROMOTE_DESIGN.md §2 (schema), §5 (validators-from-rules),
§4 (failure classes F1-F6 + keep-incumbent audit shape); OQ1-OQ8 RATIFIED
2026-07-24 (slice S1 per §7/§9). Pins the promote-decision contract's strict
fail-closed normalization (exact-version lookup, key allowlists at every level,
the structural exclusion of the guard verdict), each V_LINEAGE/V_COMP/V_GEN/
V_PROV/V_LIFECYCLE validator's accept+reject boundary, the §2.1 conjunction
(promote ⟺ guard-pass AND AI-approve AND all-validators-pass, with a blocked
promote downgraded to an effective reject + F5 audit), and the ruling-④
keep-incumbent audit shape for every failure class F1-F6.

Companion oracle: tests/test_organization_promote_characterization.py (S0, the
ruling-③ prerequisite) pins the CURRENT deterministic ladder the validators
formalize; this suite pins the acceptance battery that will gate AI promote
decisions at the S5 flip. Pure/offline: the guard's predicted verdict and the
incumbent descriptor arrive as input parameters — no store or model deps. The S0
oracle stays green untouched.
"""

from __future__ import annotations

import unittest
from typing import Any

from sourcing_agent.organization_promote_contract import (
    AI_DECISION_REASON_CODES,
    DECISION_PROMOTE,
    DECISION_REJECT,
    DECISION_SOURCE_AI_JUDGE,
    DECISION_SOURCE_DETERMINISTIC_PREBRANCH,
    DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
    DECISION_SOURCE_GUARD_REFUSED,
    FALLBACK_REASON_CALL_FAILED,
    FALLBACK_REASON_CIRCUIT_OPEN,
    FALLBACK_REASON_INPUT_STALE,
    FALLBACK_REASON_INVALID_OUTPUT,
    FALLBACK_REASON_MODEL_UNAVAILABLE,
    MAX_COMPLETENESS_SCORE_REGRESSION,
    SCHEMA_FAILURE_VALIDATOR_ID,
    SCHEMA_ID_V1,
    VALIDATOR_IDS,
    VALIDATOR_V_COMP,
    VALIDATOR_V_GEN,
    VALIDATOR_V_LIFECYCLE,
    VALIDATOR_V_LINEAGE,
    VALIDATOR_V_PROV,
    PromoteDecisionContractError,
    build_keep_incumbent_fallback,
    fallback_reason_for_validator,
    is_valid_fallback_reason,
    normalize_ai_promote_decision,
    normalize_fallback_audit,
    normalize_snapshot_descriptor,
    predict_lineage_guard_refusal,
    validate_ai_promote_decision,
    validate_v_comp,
    validate_v_gen,
    validate_v_lifecycle,
    validate_v_lineage,
    validate_v_prov,
)

_HEX = "ab" * 32


def _metrics(**overrides: Any) -> dict[str, Any]:
    metrics = {
        "candidate_count": 200,
        "evidence_count": 200,
        "profile_detail_count": 200,
        "missing_linkedin_count": 0,
        "profile_completion_backlog_count": 0,
        "effective_lane_total": 200,
    }
    metrics.update(overrides)
    return metrics


def _descriptor(
    snapshot_id: str,
    *,
    completeness_score: float = 80.0,
    completeness_band: str = "high",
    selected: list[str] | None = None,
    source_snapshot_count: int = 3,
    generation_sequence: int = 6,
    generation_key: str = "lineage-a",
    lifecycle_status: str = "ready",
    explicit_baseline_inclusion: bool = False,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot_id,
        "metrics": _metrics(**(metrics or {})),
        "completeness_score": completeness_score,
        "completeness_band": completeness_band,
        "selected_snapshot_ids": list(selected if selected is not None else [snapshot_id]),
        "source_snapshot_count": source_snapshot_count,
        "materialization_generation_sequence": generation_sequence,
        "lifecycle_status": lifecycle_status,
        "materialization_generation_key": generation_key,
        "explicit_baseline_inclusion": explicit_baseline_inclusion,
    }


def _shard(**overrides: Any) -> dict[str, Any]:
    shard = {
        "shard_id": "shard-1",
        "lane": "current",
        "search_query": "staff engineer",
        "locations": ["US"],
        "function_ids": ["8", "9"],
        "job_titles": ["Staff Engineer"],
        "seniority": ["senior"],
        "query_family": "eng-core",
        "result_count": 304,
        "estimated_total_count": 0,
        "provider_cap_hit": False,
        "payload_snapshot_sha256": None,
    }
    shard.update(overrides)
    return shard


def _candidate_descriptor(
    *,
    incumbent: dict[str, Any] | None = None,
    candidate: dict[str, Any] | None = None,
    simulate_or_placeholder: bool = False,
) -> dict[str, Any]:
    return {
        "incumbent": incumbent if incumbent is not None else _descriptor("inc", generation_sequence=6),
        "candidate": candidate
        if candidate is not None
        else _descriptor(
            "cand",
            completeness_score=82.0,
            selected=["cand"],
            generation_sequence=7,
            metrics={"candidate_count": 260, "profile_detail_count": 260, "effective_lane_total": 260},
        ),
        "coverage_evidence": {
            "shards": [_shard()],
            "request_population_match": {
                "candidate_covers_incumbent_shards": True,
                "new_shards": [],
                "dropped_shards": [],
            },
        },
        "prior_snapshot_comparison": {
            "candidate_sort_key": [82.0, 260],
            "incumbent_sort_key": [80.0, 200],
            "simulate_or_placeholder_provenance": simulate_or_placeholder,
        },
    }


def _provenance() -> dict[str, Any]:
    return {
        "model_provider": "qwen",
        "requested_model": "qwen3-max",
        "response_model": "qwen3-max",
        "prompt_sha256": _HEX,
        "input_snapshot_sha256": _HEX,
        "usage": {"input_tokens": 1200, "output_tokens": 90},
        "latency_ms": 4200,
    }


def _decision(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "decision_id": "01JAPROMOTE00000000000000",
        "decision_source": DECISION_SOURCE_AI_JUDGE,
        "decision": DECISION_PROMOTE,
        "reason": "candidate adds current-lane coverage at equal completeness; net coverage is a superset",
        "reason_code": "ai_coverage_superset",
        "candidate_descriptor": _candidate_descriptor(),
        "provenance": _provenance(),
        "validator_results": [],
        "fallback": None,
    }
    payload.update(overrides)
    return payload


def _fallback_audit(reason: str, *, provenance: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "decision_source": DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
        "fallback_reason": reason,
        "judge_error": "boom" if reason != FALLBACK_REASON_MODEL_UNAVAILABLE else "",
        "validator_results": [{"validator": VALIDATOR_V_COMP, "status": "fail", "reason": "x"}],
        "provenance": provenance,
    }


def _passing_guard_verdict() -> dict[str, Any]:
    return {"refused": False, "reason": ""}


def _refusing_guard_verdict(reason: str = "source_snapshot_coverage_regression") -> dict[str, Any]:
    return {
        "refused": True,
        "reason": reason,
        "incoming_snapshot_id": "cand",
        "incoming_generation_key": "lineage-a",
        "incoming_generation_sequence": 3,
        "incoming_selected_snapshot_ids": ["cand"],
        "blocking_snapshot_id": "inc",
        "blocking_generation_key": "lineage-a",
        "blocking_generation_sequence": 6,
        "blocking_selected_snapshot_ids": ["cand", "inc"],
    }


def _validate(payload: dict[str, Any], *, guard: dict[str, Any] | None = None, incumbent: dict[str, Any] | None = None):
    return validate_ai_promote_decision(
        payload,
        guard_predicted_verdict=guard if guard is not None else _passing_guard_verdict(),
        incumbent_descriptor=incumbent if incumbent is not None else _descriptor("inc", generation_sequence=6),
    )


def _failed_ids(result: dict[str, Any]) -> list[str]:
    return [failure["validator_id"] for failure in result["failures"]]


class SchemaRoundTripTest(unittest.TestCase):
    """Strict normalization: round-trip identity + fail-closed rejection."""

    def test_valid_ai_decision_round_trips_and_passes_the_battery(self) -> None:
        payload = _decision()
        decision = normalize_ai_promote_decision(payload)
        self.assertEqual(decision.to_payload(), payload)
        # Round-trip fixed point.
        self.assertEqual(normalize_ai_promote_decision(decision.to_payload()).to_payload(), payload)
        result = _validate(payload)
        self.assertTrue(result["valid"])
        self.assertEqual(result["failures"], [])
        self.assertEqual(result["normalized"], payload)
        self.assertEqual(result["effective_decision"], DECISION_PROMOTE)
        self.assertIsNone(result["fallback"])
        statuses = {entry["validator"]: entry["status"] for entry in result["validator_results"]}
        self.assertEqual(set(statuses), set(VALIDATOR_IDS))
        self.assertEqual(set(statuses.values()), {"pass"})

    def test_unknown_schema_version_fails_closed_with_no_auto_upgrade(self) -> None:
        for bad_schema_id in (
            "sourcing.organization_asset.ai_promote_decision.v2",
            "sourcing.organization_asset.ai_promote_decision",
            "",
            None,
            2,
        ):
            with self.subTest(schema_id=bad_schema_id):
                result = _validate(_decision(schema_id=bad_schema_id))
                self.assertFalse(result["valid"])
                self.assertIsNone(result["normalized"])
                self.assertEqual(_failed_ids(result), [SCHEMA_FAILURE_VALIDATOR_ID])
                self.assertEqual(result["effective_decision"], DECISION_REJECT)
                self.assertIn("fail-closed", result["failures"][0]["reason"])

    def test_unknown_keys_are_rejected_at_every_level(self) -> None:
        cases = {
            "top-level": _decision(surprise=1),
            "candidate_descriptor": _decision(
                candidate_descriptor={**_candidate_descriptor(), "guard_verdict": {"refused": False}}
            ),
            "snapshot_descriptor": _decision(
                candidate_descriptor={
                    **_candidate_descriptor(),
                    "candidate": {**_descriptor("cand"), "authoritative_promotion_refused": {}},
                }
            ),
            "metrics": _decision(
                candidate_descriptor={
                    **_candidate_descriptor(),
                    "candidate": {**_descriptor("cand"), "metrics": {**_metrics(), "sneaky": 1}},
                }
            ),
            "coverage_evidence": _decision(
                candidate_descriptor={
                    **_candidate_descriptor(),
                    "coverage_evidence": {
                        "shards": [_shard()],
                        "request_population_match": {
                            "candidate_covers_incumbent_shards": True,
                            "new_shards": [],
                            "dropped_shards": [],
                        },
                        "extra": 1,
                    },
                }
            ),
            "shard": _decision(
                candidate_descriptor={
                    **_candidate_descriptor(),
                    "coverage_evidence": {
                        "shards": [{**_shard(), "extra": True}],
                        "request_population_match": {
                            "candidate_covers_incumbent_shards": True,
                            "new_shards": [],
                            "dropped_shards": [],
                        },
                    },
                }
            ),
            "provenance": _decision(provenance={**_provenance(), "temperature": 0.2}),
            "usage": _decision(
                provenance={**_provenance(), "usage": {"input_tokens": 1, "output_tokens": 1, "total": 2}}
            ),
            "validator_results": _decision(
                validator_results=[{"validator": "V_COMP_regression_floor", "status": "pass", "note": "x"}]
            ),
        }
        for label, payload in cases.items():
            with self.subTest(level=label):
                result = _validate(payload)
                self.assertEqual(_failed_ids(result), [SCHEMA_FAILURE_VALIDATOR_ID])

    def test_guard_verdict_has_no_schema_slot_anywhere(self) -> None:
        # The model can neither see nor author the guard verdict: every attempt to
        # place it in the payload is a strict-allowlist rejection.
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(_decision(authoritative_promotion_refused={"reason": "x"}))
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(_decision(guard_predicted_verdict={"refused": True}))

    def test_structural_violations_are_schema_failures(self) -> None:
        cases = {
            "bad_decision": _decision(decision="maybe"),
            "empty_reason": _decision(reason="   "),
            "reason_too_long": _decision(reason="x" * 241),
            "reason_code_off_vocab": _decision(reason_code="ai_totally_made_up"),
            "ai_reject_code_on_promote_ok_but_prebranch_code_wrong": _decision(reason_code="no_existing_authoritative"),
            "bad_decision_source": _decision(decision_source="model_judge"),
            "bad_decision_id": _decision(decision_id="has space"),
            "bad_membership_hash": _decision(provenance={**_provenance(), "prompt_sha256": "zz"}),
            "bad_validator_status": _decision(
                validator_results=[{"validator": "V_COMP_regression_floor", "status": "ok"}]
            ),
            "negative_metric": _decision(
                candidate_descriptor={
                    **_candidate_descriptor(),
                    "candidate": {**_descriptor("cand"), "metrics": _metrics(candidate_count=-1)},
                }
            ),
            "not_a_mapping": [],
        }
        for label, payload in cases.items():
            with self.subTest(case=label):
                result = _validate(payload)  # type: ignore[arg-type]
                self.assertFalse(result["valid"])
                self.assertEqual(_failed_ids(result), [SCHEMA_FAILURE_VALIDATOR_ID])

    def test_ai_judge_requires_provenance_and_null_fallback(self) -> None:
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(_decision(provenance=None))
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(_decision(fallback=_fallback_audit(FALLBACK_REASON_INVALID_OUTPUT)))

    def test_guard_and_fallback_sources_are_reject_only(self) -> None:
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(
                _decision(
                    decision_source=DECISION_SOURCE_GUARD_REFUSED,
                    decision=DECISION_PROMOTE,
                    reason_code="source_snapshot_coverage_regression",
                    provenance=None,
                )
            )
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(
                _decision(
                    decision_source=DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
                    decision=DECISION_PROMOTE,
                    reason_code="keep_incumbent_judge_fallback",
                    provenance=None,
                    fallback=_fallback_audit(FALLBACK_REASON_MODEL_UNAVAILABLE),
                )
            )

    def test_reason_code_vocabulary_is_keyed_to_decision_source(self) -> None:
        # A guard reason code on an ai_judge record is rejected, and vice versa.
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision(_decision(reason_code="stale_generation_sequence_replay"))
        prebranch = _decision(
            decision_source=DECISION_SOURCE_DETERMINISTIC_PREBRANCH,
            decision=DECISION_PROMOTE,
            reason_code="no_existing_authoritative",
            provenance=None,
        )
        self.assertEqual(normalize_ai_promote_decision(prebranch).reason_code, "no_existing_authoritative")
        with self.assertRaises(PromoteDecisionContractError):
            normalize_ai_promote_decision({**prebranch, "reason_code": "ai_coverage_superset"})

    def test_nullable_payload_snapshot_sha256_round_trips(self) -> None:
        with_hash = _candidate_descriptor()
        with_hash["coverage_evidence"]["shards"][0]["payload_snapshot_sha256"] = _HEX
        payload = _decision(candidate_descriptor=with_hash)
        self.assertEqual(normalize_ai_promote_decision(payload).to_payload(), payload)


class LineageGuardPredictorTest(unittest.TestCase):
    """predict_lineage_guard_refusal mirrors the storage guard's two shapes (S0)."""

    def test_stale_generation_sequence_replay(self) -> None:
        verdict = predict_lineage_guard_refusal(
            incoming_snapshot_id="snap-stale",
            incoming_generation_key="lineage-a",
            incoming_generation_sequence=3,
            incoming_selected_snapshot_ids=["snap-stale"],
            incumbent_snapshot_id="snap-current",
            incumbent_generation_key="lineage-a",
            incumbent_generation_sequence=6,
            incumbent_selected_snapshot_ids=["snap-current"],
        )
        self.assertTrue(verdict["refused"])
        self.assertEqual(verdict["reason"], "stale_generation_sequence_replay")

    def test_source_snapshot_coverage_regression_strict_subset(self) -> None:
        verdict = predict_lineage_guard_refusal(
            incoming_snapshot_id="snap-041551",
            incoming_generation_key="gen-b",
            incoming_generation_sequence=3,
            incoming_selected_snapshot_ids=["snap-041551"],
            incumbent_snapshot_id="snap-104157",
            incumbent_generation_key="gen-a",
            incumbent_generation_sequence=6,
            incumbent_selected_snapshot_ids=["snap-104157", "snap-041551"],
        )
        self.assertTrue(verdict["refused"])
        self.assertEqual(verdict["reason"], "source_snapshot_coverage_regression")
        self.assertEqual(verdict["blocking_selected_snapshot_ids"], ["snap-041551", "snap-104157"])

    def test_equal_or_wider_coverage_is_not_refused(self) -> None:
        verdict = predict_lineage_guard_refusal(
            incoming_snapshot_id="new",
            incoming_generation_key="gen-b",
            incoming_generation_sequence=1,
            incoming_selected_snapshot_ids=["new", "snap-104157", "snap-041551"],
            incumbent_snapshot_id="snap-104157",
            incumbent_generation_key="gen-a",
            incumbent_generation_sequence=6,
            incumbent_selected_snapshot_ids=["snap-104157", "snap-041551"],
        )
        self.assertFalse(verdict["refused"])
        self.assertEqual(verdict["reason"], "")


class ValidatorBatteryUnitTest(unittest.TestCase):
    """Each §5 validator pure function: accept + reject at the design-row boundary."""

    def test_v_lineage_guard_refuse_plus_model_promote_rejects(self) -> None:
        # The core ruling-③ property: a guard refusal is a hard V_LINEAGE failure.
        self.assertIsNone(validate_v_lineage(guard_predicted_verdict=_passing_guard_verdict()))
        failure = validate_v_lineage(guard_predicted_verdict=_refusing_guard_verdict())
        self.assertIsNotNone(failure)
        self.assertIn("source_snapshot_coverage_regression", failure or "")

    def test_v_comp_score_regression_floor_at_the_tolerance(self) -> None:
        inc = normalize_snapshot_descriptor(_descriptor("inc", completeness_score=90.0))
        # Regress exactly the tolerance (1.0) → still passes; beyond it → fails.
        at_edge = normalize_snapshot_descriptor(
            _descriptor("cand", completeness_score=90.0 - MAX_COMPLETENESS_SCORE_REGRESSION)
        )
        over_edge = normalize_snapshot_descriptor(
            _descriptor("cand", completeness_score=90.0 - MAX_COMPLETENESS_SCORE_REGRESSION - 0.01)
        )
        self.assertIsNone(validate_v_comp(candidate=at_edge, incumbent=inc))
        self.assertIsNotNone(validate_v_comp(candidate=over_edge, incumbent=inc))

    def test_v_comp_rejects_strictly_narrower_coverage(self) -> None:
        inc = normalize_snapshot_descriptor(_descriptor("inc", metrics={"effective_lane_total": 200}))
        narrower = normalize_snapshot_descriptor(_descriptor("cand", metrics={"effective_lane_total": 199}))
        wider = normalize_snapshot_descriptor(_descriptor("cand", metrics={"effective_lane_total": 201}))
        self.assertIn("narrower", validate_v_comp(candidate=narrower, incumbent=inc) or "")
        self.assertIsNone(validate_v_comp(candidate=wider, incumbent=inc))

    def test_v_gen_generation_equality_passes_regression_fails(self) -> None:
        inc = normalize_snapshot_descriptor(_descriptor("inc", generation_key="lineage-a", generation_sequence=6))
        equal_seq = normalize_snapshot_descriptor(
            _descriptor("cand", generation_key="lineage-a", generation_sequence=6)
        )
        higher_seq = normalize_snapshot_descriptor(
            _descriptor("cand", generation_key="lineage-a", generation_sequence=7)
        )
        lower_seq = normalize_snapshot_descriptor(
            _descriptor("cand", generation_key="lineage-a", generation_sequence=5)
        )
        self.assertIsNone(validate_v_gen(candidate=equal_seq, incumbent=inc))
        self.assertIsNone(validate_v_gen(candidate=higher_seq, incumbent=inc))
        self.assertIn("regresses", validate_v_gen(candidate=lower_seq, incumbent=inc) or "")

    def test_v_gen_cross_lineage_lower_sequence_is_not_monotonicity_comparable(self) -> None:
        inc = normalize_snapshot_descriptor(_descriptor("inc", generation_key="lineage-a", generation_sequence=6))
        other_lineage = normalize_snapshot_descriptor(
            _descriptor("cand", generation_key="lineage-b", generation_sequence=1)
        )
        self.assertIsNone(validate_v_gen(candidate=other_lineage, incumbent=inc))

    def test_v_prov_rejects_simulate_and_missing_lineage_and_missing_field(self) -> None:
        prov = normalize_ai_promote_decision(_decision()).provenance
        clean = normalize_ai_promote_decision(_decision()).candidate_descriptor
        self.assertIsNone(validate_v_prov(candidate_descriptor=clean, provenance=prov))
        tainted = normalize_ai_promote_decision(
            _decision(candidate_descriptor=_candidate_descriptor(simulate_or_placeholder=True))
        ).candidate_descriptor
        self.assertIn("simulate", validate_v_prov(candidate_descriptor=tainted, provenance=prov) or "")
        no_lineage = normalize_ai_promote_decision(
            _decision(
                candidate_descriptor=_candidate_descriptor(
                    candidate=_descriptor("cand", selected=[], generation_sequence=7)
                )
            )
        ).candidate_descriptor
        self.assertIn(
            "lineage evidence absent", validate_v_prov(candidate_descriptor=no_lineage, provenance=prov) or ""
        )
        self.assertIn("no model provenance", validate_v_prov(candidate_descriptor=clean, provenance=None) or "")

    def test_v_lifecycle_promotable_vs_non_promotable(self) -> None:
        ready = normalize_snapshot_descriptor(_descriptor("cand", lifecycle_status="ready"))
        canonical = normalize_snapshot_descriptor(_descriptor("cand", lifecycle_status="canonical"))
        archived = normalize_snapshot_descriptor(_descriptor("cand", lifecycle_status="archived"))
        self.assertIsNone(validate_v_lifecycle(candidate=ready))
        self.assertIsNone(validate_v_lifecycle(candidate=canonical))
        self.assertIn("not promotable", validate_v_lifecycle(candidate=archived) or "")


class ConjunctionTest(unittest.TestCase):
    """§2.1 promote ⟺ guard-pass AND AI-approve AND all-validators-pass."""

    def test_all_legs_pass_promotes(self) -> None:
        result = _validate(_decision())
        self.assertTrue(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_PROMOTE)
        self.assertIsNone(result["fallback"])

    def test_guard_refuse_downgrades_promote_to_effective_reject_with_audit(self) -> None:
        result = _validate(_decision(), guard=_refusing_guard_verdict())
        self.assertFalse(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_REJECT)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V_LINEAGE])
        self.assertEqual(result["fallback"]["decision_source"], DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT)
        self.assertEqual(result["fallback"]["fallback_reason"], fallback_reason_for_validator(VALIDATOR_V_LINEAGE))
        self.assertEqual(result["fallback"]["provenance"], _provenance())

    def test_comp_regression_downgrades_promote(self) -> None:
        # Incumbent score 95; candidate 90 (>1.0 regression) but the model approves.
        incumbent = _descriptor("inc", completeness_score=95.0)
        candidate = _descriptor(
            "cand",
            completeness_score=90.0,
            selected=["cand"],
            generation_sequence=7,
            metrics={"effective_lane_total": 260},
        )
        payload = _decision(candidate_descriptor=_candidate_descriptor(incumbent=incumbent, candidate=candidate))
        result = _validate(payload, incumbent=incumbent)
        self.assertFalse(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_REJECT)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V_COMP])
        self.assertEqual(result["fallback"]["fallback_reason"], fallback_reason_for_validator(VALIDATOR_V_COMP))

    def test_lifecycle_and_prov_both_flag_first_names_the_fallback(self) -> None:
        candidate = _descriptor("cand", lifecycle_status="archived", selected=[], generation_sequence=7)
        payload = _decision(
            candidate_descriptor=_candidate_descriptor(candidate=candidate, simulate_or_placeholder=True)
        )
        result = _validate(payload)
        self.assertFalse(result["valid"])
        self.assertIn(VALIDATOR_V_PROV, _failed_ids(result))
        self.assertIn(VALIDATOR_V_LIFECYCLE, _failed_ids(result))
        # The fallback names the FIRST failing floor (design-ordered battery).
        self.assertEqual(result["fallback"]["fallback_reason"], fallback_reason_for_validator(_failed_ids(result)[0]))

    def test_ai_reject_is_always_valid_even_when_a_floor_would_block(self) -> None:
        # The AI is more conservative than the ladder: an honest reject stands, no
        # fallback (it is not a blocked promote), and it is always valid.
        payload = _decision(decision=DECISION_REJECT, reason_code="ai_coverage_not_superset")
        result = _validate(payload, guard=_refusing_guard_verdict())
        self.assertTrue(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_REJECT)
        self.assertIsNone(result["fallback"])
        # The floor failure is still recorded for audit.
        self.assertEqual(_failed_ids(result), [VALIDATOR_V_LINEAGE])

    def test_ai_promote_never_overrides_a_reject_floor(self) -> None:
        # Even a maximally-persuasive promote cannot lift a guard refusal.
        for reason_code in sorted(AI_DECISION_REASON_CODES):
            payload = _decision(reason_code=reason_code, decision=DECISION_PROMOTE)
            result = _validate(payload, guard=_refusing_guard_verdict())
            with self.subTest(reason_code=reason_code):
                self.assertEqual(result["effective_decision"], DECISION_REJECT)


class AuditRecordTest(unittest.TestCase):
    """Non-ai_judge records are well-formed audit rows; the battery is skipped."""

    def test_deterministic_prebranch_record_skips_the_battery(self) -> None:
        payload = _decision(
            decision_source=DECISION_SOURCE_DETERMINISTIC_PREBRANCH,
            decision=DECISION_PROMOTE,
            reason="no incumbent authoritative snapshot yet",
            reason_code="no_existing_authoritative",
            provenance=None,
        )
        result = _validate(payload)
        self.assertTrue(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_PROMOTE)
        self.assertEqual({entry["status"] for entry in result["validator_results"]}, {"skipped"})
        self.assertEqual(len(result["validator_results"]), len(VALIDATOR_IDS))

    def test_guard_refused_record_skips_the_battery(self) -> None:
        payload = _decision(
            decision_source=DECISION_SOURCE_GUARD_REFUSED,
            decision=DECISION_REJECT,
            reason="storage guard refused: strict-subset coverage regression",
            reason_code="source_snapshot_coverage_regression",
            provenance=None,
        )
        result = _validate(payload)
        self.assertTrue(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_REJECT)
        self.assertEqual({entry["status"] for entry in result["validator_results"]}, {"skipped"})

    def test_fallback_keep_incumbent_record_skips_the_battery(self) -> None:
        payload = _decision(
            decision_source=DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
            decision=DECISION_REJECT,
            reason="judge unavailable; keeping incumbent authoritative",
            reason_code="keep_incumbent_judge_fallback",
            provenance=None,
            fallback=_fallback_audit(FALLBACK_REASON_MODEL_UNAVAILABLE),
        )
        result = _validate(payload)
        self.assertTrue(result["valid"])
        self.assertEqual(result["effective_decision"], DECISION_REJECT)
        self.assertEqual(result["fallback"]["fallback_reason"], FALLBACK_REASON_MODEL_UNAVAILABLE)


class FallbackAuditShapeTest(unittest.TestCase):
    """Ruling-④ keep-incumbent audit shape: one accepted record per F1-F6."""

    def test_every_failure_class_shape_is_accepted(self) -> None:
        cases = {
            "F1": _fallback_audit(FALLBACK_REASON_MODEL_UNAVAILABLE),
            "F2": _fallback_audit(FALLBACK_REASON_CIRCUIT_OPEN),
            "F3": _fallback_audit(FALLBACK_REASON_CALL_FAILED),
            "F4": _fallback_audit(FALLBACK_REASON_INVALID_OUTPUT, provenance=_provenance()),
            "F5": _fallback_audit(fallback_reason_for_validator(VALIDATOR_V_COMP), provenance=_provenance()),
            "F6": _fallback_audit(FALLBACK_REASON_INPUT_STALE, provenance=_provenance()),
        }
        for label, payload in cases.items():
            with self.subTest(failure_class=label):
                audit = normalize_fallback_audit(payload)
                self.assertEqual(audit.to_payload(), payload)

    def test_invalid_fallback_reasons_are_rejected(self) -> None:
        for bad_reason in ("judge_exploded", "judge_validator_rejected:V_NOPE", ""):
            with self.subTest(reason=bad_reason):
                self.assertFalse(is_valid_fallback_reason(bad_reason))
                with self.assertRaises(PromoteDecisionContractError):
                    payload = _fallback_audit(FALLBACK_REASON_CALL_FAILED)
                    payload["fallback_reason"] = bad_reason
                    normalize_fallback_audit(payload)

    def test_judge_error_is_bounded_to_500_chars(self) -> None:
        payload = _fallback_audit(FALLBACK_REASON_CALL_FAILED)
        payload["judge_error"] = "x" * 501
        with self.assertRaises(PromoteDecisionContractError):
            normalize_fallback_audit(payload)

    def test_build_keep_incumbent_fallback_truncates_and_fails_closed(self) -> None:
        audit = build_keep_incumbent_fallback(fallback_reason=FALLBACK_REASON_CALL_FAILED, judge_error="y" * 900)
        self.assertEqual(len(audit.judge_error), 500)
        self.assertEqual(audit.decision_source, DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT)
        with self.assertRaises(PromoteDecisionContractError):
            build_keep_incumbent_fallback(fallback_reason="not_a_real_reason")

    def test_fallback_reason_helper_rejects_unknown_validator(self) -> None:
        self.assertEqual(
            fallback_reason_for_validator(VALIDATOR_V_GEN),
            "judge_validator_rejected:V_GEN_generation_monotonic",
        )
        with self.assertRaises(PromoteDecisionContractError):
            fallback_reason_for_validator("V_NOPE")


if __name__ == "__main__":
    unittest.main()
