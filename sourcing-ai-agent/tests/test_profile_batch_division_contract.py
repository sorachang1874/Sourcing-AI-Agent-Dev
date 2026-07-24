"""WS7/W7.2 S1 offline suite for `sourcing.profile_prefetch.ai_batch_division.v1`.

Provenance: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §1.2/§1.3/§3, OQ1-OQ8 RATIFIED
2026-07-23 (slice S1 per §7). Pins the divider-output contract's strict
fail-closed normalization (exact-version lookup, key allowlists), each V1-V10
validator's accept+reject boundary, and the ruling-④ fallback audit shape for
every failure class F1-F6. Companion oracle:
tests/test_fetch_profile_batch_characterization.py (28 tests + 36 subtests)
pins the CURRENT ladder the validators formalize; this suite pins the
acceptance battery that will gate AI divisions at the S5 flip.
"""

from __future__ import annotations

import unittest
from typing import Any

from sourcing_agent.profile_batch_division_contract import (
    AI_DIVISION_REASON_CODE,
    DIVISION_SOURCE_AI_DIVIDER,
    DIVISION_SOURCE_RULE_LADDER_FALLBACK,
    FALLBACK_REASON_CALL_FAILED,
    FALLBACK_REASON_CIRCUIT_OPEN,
    FALLBACK_REASON_INPUT_STALE,
    FALLBACK_REASON_INVALID_OUTPUT,
    FALLBACK_REASON_MODEL_UNAVAILABLE,
    SCHEMA_FAILURE_VALIDATOR_ID,
    SCHEMA_ID_V1,
    VALIDATOR_IDS,
    VALIDATOR_V1_PROVIDER_ENVELOPE,
    VALIDATOR_V2_BATCH_CEILING,
    VALIDATOR_V3_EXACT_PARTITION,
    VALIDATOR_V4_TINY_BATCH_REASON,
    VALIDATOR_V7_REASON_AUDIT,
    VALIDATOR_V8_RETRY_ISOLATION,
    VALIDATOR_V9_ROUND_BUDGET,
    VALIDATOR_V10_BATCH_COUNT_BAND,
    BatchDivisionContractError,
    DivisionBatch,
    compute_membership_sha256,
    fallback_reason_for_validator,
    is_valid_fallback_reason,
    normalize_ai_batch_division,
    normalize_fallback_audit,
    validate_ai_batch_division,
    validate_v1_provider_envelope,
    validate_v2_batch_ceiling,
    validate_v3_exact_partition,
    validate_v4_tiny_batch_reason,
    validate_v5_worker_budget,
    validate_v6_wave_mint_only,
    validate_v7_reason_audit,
    validate_v8_retry_isolation,
    validate_v9_round_budget,
    validate_v10_batch_count_band,
)

_HEX = "ab" * 32


def _provenance() -> dict[str, Any]:
    return {
        "model_provider": "qwen",
        "requested_model": "qwen3-max",
        "response_model": "qwen3-max",
        "prompt_sha256": _HEX,
        "input_snapshot_sha256": _HEX,
        "usage": {"input_tokens": 4200, "output_tokens": 610},
        "latency_ms": 8300,
    }


def _batch(
    index: int,
    ranges: list[list[int]],
    *,
    reason: str = "cohort grouped by shard and failure history",
    reason_code: str = AI_DIVISION_REASON_CODE,
) -> dict[str, Any]:
    return {
        "batch_index": index,
        "member_index_ranges": ranges,
        "member_count": sum(end - start + 1 for start, end in ranges),
        "reason": reason,
        "reason_code": reason_code,
    }


def _even_batches(batch_count: int, batch_size: int) -> list[dict[str, Any]]:
    """batch_count contiguous batches of batch_size over [0, batch_count*batch_size)."""
    return [_batch(index + 1, [[index * batch_size, (index + 1) * batch_size - 1]]) for index in range(batch_count)]


def _division(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "division_id": "01JADIV0000000000000000000",
        "division_source": DIVISION_SOURCE_AI_DIVIDER,
        "batches": _even_batches(4, 300),
        "membership_sha256": _HEX,
        "provenance": _provenance(),
        "validator_results": [],
        "fallback": None,
    }
    payload.update(overrides)
    payload.setdefault("batch_count", len(payload["batches"]))
    return payload


def _fallback_audit(reason: str, *, provenance: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "division_source": DIVISION_SOURCE_RULE_LADDER_FALLBACK,
        "fallback_reason": reason,
        "divider_error": "boom" if reason != FALLBACK_REASON_MODEL_UNAVAILABLE else "",
        "validator_results": [{"validator": VALIDATOR_V2_BATCH_CEILING, "status": "fail", "reason": "x"}],
        "provenance": provenance,
    }


def _validate(payload: dict[str, Any], *, inventory: int = 1200, retry: tuple[int, ...] = (), inflight: int = 4):
    return validate_ai_batch_division(
        payload,
        inventory_size=inventory,
        retry_wait_indices=retry,
        actor_global_inflight=inflight,
    )


def _failed_ids(result: dict[str, Any]) -> list[str]:
    return [failure["validator_id"] for failure in result["failures"]]


class SchemaRoundTripTest(unittest.TestCase):
    """Strict normalization: round-trip identity + fail-closed rejection."""

    def test_valid_ai_division_round_trips_and_passes_the_battery(self) -> None:
        payload = _division()
        division = normalize_ai_batch_division(payload)
        self.assertEqual(division.to_payload(), payload)
        # Round-trip fixed point: normalizing the normalized payload is identity.
        self.assertEqual(normalize_ai_batch_division(division.to_payload()).to_payload(), payload)
        result = _validate(payload)
        self.assertTrue(result["valid"])
        self.assertEqual(result["failures"], [])
        self.assertEqual(result["normalized"], payload)
        statuses = {entry["validator"]: entry["status"] for entry in result["validator_results"]}
        self.assertEqual(set(statuses), set(VALIDATOR_IDS))
        self.assertEqual(statuses["V5_worker_budget"], "skipped")
        self.assertEqual(statuses["V6_wave_mint_only"], "skipped")

    def test_unknown_schema_version_fails_closed_with_no_auto_upgrade(self) -> None:
        for bad_schema_id in (
            "sourcing.profile_prefetch.ai_batch_division.v2",
            "sourcing.profile_prefetch.ai_batch_division",
            "",
            None,
            2,
        ):
            with self.subTest(schema_id=bad_schema_id):
                result = _validate(_division(schema_id=bad_schema_id))
                self.assertFalse(result["valid"])
                self.assertIsNone(result["normalized"])
                self.assertEqual(_failed_ids(result), [SCHEMA_FAILURE_VALIDATOR_ID])
                self.assertIn("fail-closed", result["failures"][0]["reason"])

    def test_unknown_keys_are_rejected_at_every_level(self) -> None:
        cases = {
            "top-level": _division(surprise=1),
            "batch": _division(batches=[{**_batch(1, [[0, 299]]), "extra": True}] + _even_batches(4, 300)[1:]),
            "provenance": _division(provenance={**_provenance(), "temperature": 0.2}),
            "usage": _division(
                provenance={**_provenance(), "usage": {"input_tokens": 1, "output_tokens": 1, "total": 2}}
            ),
            "validator_results": _division(
                validator_results=[{"validator": "V1_provider_envelope", "status": "pass", "note": "x"}]
            ),
        }
        for label, payload in cases.items():
            with self.subTest(level=label):
                result = _validate(payload)
                self.assertEqual(_failed_ids(result), [SCHEMA_FAILURE_VALIDATOR_ID])

    def test_structural_violations_are_schema_failures(self) -> None:
        base_batches = _even_batches(4, 300)
        cases = {
            "batch_count_mismatch": _division(batch_count=3),
            "member_count_mismatch": _division(batches=[{**base_batches[0], "member_count": 299}] + base_batches[1:]),
            "non_sequential_batch_index": _division(batches=[{**base_batches[0], "batch_index": 2}] + base_batches[1:]),
            "inverted_range": _division(batches=[_batch(1, [[10, 5]])] + []),
            "negative_range": _division(
                batches=[{**base_batches[0], "member_index_ranges": [[-1, 5]], "member_count": 7}]
            ),
            "bad_membership_hash": _division(membership_sha256="zz"),
            "bad_division_source": _division(division_source="model_divider"),
            "bad_division_id": _division(division_id="has space"),
            "reason_too_long": _division(batches=[_batch(1, [[0, 299]], reason="x" * 241)]),
            "bad_validator_status": _division(
                validator_results=[{"validator": "V1_provider_envelope", "status": "ok"}]
            ),
            "not_a_mapping": [],
        }
        for label, payload in cases.items():
            with self.subTest(case=label):
                result = _validate(payload)  # type: ignore[arg-type]
                self.assertFalse(result["valid"])
                self.assertEqual(_failed_ids(result), [SCHEMA_FAILURE_VALIDATOR_ID])

    def test_ai_division_requires_provenance_and_null_fallback(self) -> None:
        with self.assertRaises(BatchDivisionContractError):
            normalize_ai_batch_division(_division(provenance=None))
        with self.assertRaises(BatchDivisionContractError):
            normalize_ai_batch_division(_division(fallback=_fallback_audit(FALLBACK_REASON_INVALID_OUTPUT)))
        with self.assertRaises(BatchDivisionContractError):
            normalize_ai_batch_division(_division(batches=[], batch_count=0))

    def test_fallback_division_requires_the_audit_object(self) -> None:
        with self.assertRaises(BatchDivisionContractError):
            normalize_ai_batch_division(_division(division_source=DIVISION_SOURCE_RULE_LADDER_FALLBACK, fallback=None))

    def test_membership_sha256_helper_is_order_sensitive(self) -> None:
        forward = compute_membership_sha256([["a", "b"], ["c"]])
        self.assertRegex(forward, r"^[0-9a-f]{64}$")
        self.assertNotEqual(forward, compute_membership_sha256([["b", "a"], ["c"]]))
        self.assertNotEqual(forward, compute_membership_sha256([["c"], ["a", "b"]]))


class ValidatorBatteryUnitTest(unittest.TestCase):
    """Each V1-V10 pure function: accept + reject at the design-table boundary."""

    def _mk(
        self, index: int, size: int, *, start: int = 0, reason_code: str = AI_DIVISION_REASON_CODE
    ) -> DivisionBatch:
        return DivisionBatch(
            batch_index=index,
            member_index_ranges=((start, start + size - 1),),
            member_count=size,
            reason="test cohort",
            reason_code=reason_code,
        )

    def test_v1_provider_envelope_300_passes_301_fails(self) -> None:
        self.assertIsNone(validate_v1_provider_envelope([self._mk(1, 300)]))
        failure = validate_v1_provider_envelope([self._mk(1, 301)])
        self.assertIsNotNone(failure)
        self.assertIn("301", failure or "")

    def test_v2_batch_ceiling_8_passes_9_fails(self) -> None:
        self.assertIsNone(validate_v2_batch_ceiling(8))
        self.assertIsNotNone(validate_v2_batch_ceiling(9))

    def test_v3_exact_partition_accepts_exact_coverage(self) -> None:
        batches = [
            DivisionBatch(1, ((0, 249), (500, 549)), 300, "r", AI_DIVISION_REASON_CODE),
            DivisionBatch(2, ((250, 499),), 250, "r", AI_DIVISION_REASON_CODE),
        ]
        self.assertIsNone(validate_v3_exact_partition(batches, inventory_size=550))

    def test_v3_rejects_overlap_omission_and_out_of_bounds(self) -> None:
        overlap = [
            DivisionBatch(1, ((0, 100),), 101, "r", AI_DIVISION_REASON_CODE),
            DivisionBatch(2, ((100, 199),), 100, "r", AI_DIVISION_REASON_CODE),
        ]
        self.assertIn("duplicate", validate_v3_exact_partition(overlap, inventory_size=200) or "")
        omitted = [DivisionBatch(1, ((0, 98),), 99, "r", AI_DIVISION_REASON_CODE)]
        self.assertIn("omitted", validate_v3_exact_partition(omitted, inventory_size=100) or "")
        invented = [DivisionBatch(1, ((0, 100),), 101, "r", AI_DIVISION_REASON_CODE)]
        self.assertIn("invented", validate_v3_exact_partition(invented, inventory_size=100) or "")

    def test_v3_excludes_retry_wait_indices_from_the_coverage_target(self) -> None:
        # Indices 90-99 are retry_wait: a division over the other 90 is exact.
        batches = [DivisionBatch(1, ((0, 89),), 90, "r", AI_DIVISION_REASON_CODE)]
        self.assertIsNone(validate_v3_exact_partition(batches, inventory_size=100, retry_wait_indices=range(90, 100)))

    def test_v4_tiny_batch_needs_a_ladder_legal_reason(self) -> None:
        self.assertIsNone(validate_v4_tiny_batch_reason([self._mk(1, 50)]))  # 50 is not tiny
        self.assertIsNone(validate_v4_tiny_batch_reason([self._mk(1, 49, reason_code="queue_quiescent_final_tail")]))
        self.assertIsNotNone(validate_v4_tiny_batch_reason([self._mk(1, 49)]))  # ai_division not tiny-legal

    def test_v5_worker_budget_rejects_overrun_only(self) -> None:
        self.assertIsNone(validate_v5_worker_budget(4, available_new_worker_count=4))
        self.assertIsNotNone(validate_v5_worker_budget(5, available_new_worker_count=4))
        self.assertIsNotNone(validate_v5_worker_budget(0, available_new_worker_count=-1))

    def test_v6_wave_mint_only_allows_idempotent_same_id(self) -> None:
        self.assertIsNone(validate_v6_wave_mint_only("d1", live_division_ids=()))
        self.assertIsNone(validate_v6_wave_mint_only("d1", live_division_ids=("d1",)))
        self.assertIsNotNone(validate_v6_wave_mint_only("d2", live_division_ids=("d1",)))

    def test_v7_reason_audit_rejects_missing_reason_and_unknown_code(self) -> None:
        good = self._mk(1, 300)
        self.assertIsNone(validate_v7_reason_audit([good]))
        no_reason = DivisionBatch(1, ((0, 299),), 300, "   ", AI_DIVISION_REASON_CODE)
        self.assertIn("no reason", validate_v7_reason_audit([no_reason]) or "")
        bad_code = DivisionBatch(1, ((0, 299),), 300, "r", "made_up_code")
        self.assertIn("not R7-auditable", validate_v7_reason_audit([bad_code]) or "")

    def test_v8_retry_member_inside_a_batch_rejects(self) -> None:
        batches = [self._mk(1, 300)]
        self.assertIsNone(validate_v8_retry_isolation(batches, retry_wait_indices=(300, 301)))
        failure = validate_v8_retry_isolation(batches, retry_wait_indices=(5,))
        self.assertIn("retry_wait members", failure or "")

    def test_v9_round_budget_binds_to_configured_inflight(self) -> None:
        # OQ3: inflight 4 -> 8 batches = exactly 2 rounds, passes.
        self.assertIsNone(validate_v9_round_budget(8, actor_global_inflight=4))
        self.assertIsNotNone(validate_v9_round_budget(9, actor_global_inflight=4))
        # V9-alone territory: inflight 3, 7 batches -> ceil(7/3)=3 rounds.
        self.assertIsNotNone(validate_v9_round_budget(7, actor_global_inflight=3))
        self.assertIsNone(validate_v9_round_budget(6, actor_global_inflight=3))
        # Fail-closed on a broken inflight config.
        self.assertIsNotNone(validate_v9_round_budget(4, actor_global_inflight=0))

    def test_v10_band_gates_ai_divisions_only(self) -> None:
        self.assertIsNone(validate_v10_batch_count_band(4, division_source=DIVISION_SOURCE_AI_DIVIDER))
        self.assertIsNone(validate_v10_batch_count_band(8, division_source=DIVISION_SOURCE_AI_DIVIDER))
        self.assertIsNotNone(validate_v10_batch_count_band(3, division_source=DIVISION_SOURCE_AI_DIVIDER))
        self.assertIsNotNone(validate_v10_batch_count_band(9, division_source=DIVISION_SOURCE_AI_DIVIDER))
        self.assertIsNone(validate_v10_batch_count_band(10, division_source=DIVISION_SOURCE_RULE_LADDER_FALLBACK))


class BatteryIntegrationTest(unittest.TestCase):
    """Top-level validate_ai_batch_division: composed accept/reject shapes."""

    def test_boundary_301_batch_fails_v1_only(self) -> None:
        batches = [
            _batch(1, [[0, 300]]),  # 301 members
            _batch(2, [[301, 600]]),
            _batch(3, [[601, 900]]),
            _batch(4, [[901, 1199]]),
        ]
        result = _validate(_division(batches=batches), inventory=1200)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V1_PROVIDER_ENVELOPE])

    def test_nine_batches_fail_v2_first_and_v9_v10_alongside(self) -> None:
        # inflight 4: 9 batches would need 3 rounds — but the V2 ceiling is the
        # first (design-ordered) rejection; V9 and V10 also flag.
        result = _validate(_division(batches=_even_batches(9, 100)), inventory=900)
        failed = _failed_ids(result)
        self.assertEqual(failed[0], VALIDATOR_V2_BATCH_CEILING)
        self.assertIn(VALIDATOR_V9_ROUND_BUDGET, failed)
        self.assertIn(VALIDATOR_V10_BATCH_COUNT_BAND, failed)

    def test_v9_alone_rejects_seven_batches_at_inflight_three(self) -> None:
        # ceil(7/3)=3 > 2 while V2 (7<=8) and V10 (7 in [4,8]) both pass: the
        # OQ3 budget check must reject on its own.
        result = _validate(_division(batches=_even_batches(7, 100)), inventory=700, inflight=3)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V9_ROUND_BUDGET])

    def test_overlapping_ranges_fail_v3(self) -> None:
        batches = _even_batches(4, 300)
        batches[1] = _batch(2, [[299, 598]])  # overlaps index 299, omits 599
        result = _validate(_division(batches=batches), inventory=1200)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V3_EXACT_PARTITION])

    def test_out_of_bounds_member_fails_v3(self) -> None:
        result = _validate(_division(batches=_even_batches(4, 300)), inventory=1199)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V3_EXACT_PARTITION])

    def test_retry_wait_member_inside_an_ai_batch_fails_v8_not_v3(self) -> None:
        # Index 42 is retry_wait but the division swallowed it: V8 is the
        # design-assigned rejection; V3's coverage target excludes retry indices.
        result = _validate(_division(batches=_even_batches(4, 300)), inventory=1200, retry=(42,))
        self.assertEqual(_failed_ids(result), [VALIDATOR_V8_RETRY_ISOLATION])

    def test_missing_reason_fails_v7(self) -> None:
        batches = _even_batches(4, 300)
        batches[2] = _batch(3, [[600, 899]], reason="")
        result = _validate(_division(batches=batches), inventory=1200)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V7_REASON_AUDIT])

    def test_tiny_ai_batch_without_legal_reason_fails_v4(self) -> None:
        batches = [
            _batch(1, [[0, 299]]),
            _batch(2, [[300, 599]]),
            _batch(3, [[600, 899]]),
            _batch(4, [[900, 948]]),  # 49 members, reason_code=ai_division
        ]
        result = _validate(_division(batches=batches), inventory=949)
        self.assertEqual(_failed_ids(result), [VALIDATOR_V4_TINY_BATCH_REASON])
        # Same shape with a ladder-legal tiny reason passes V4.
        batches[3] = _batch(4, [[900, 948]], reason_code="queue_quiescent_final_tail")
        self.assertTrue(_validate(_division(batches=batches), inventory=949)["valid"])


class FallbackAuditShapeTest(unittest.TestCase):
    """Ruling-④ audit shape: one accepted record per failure class F1-F6."""

    def test_every_failure_class_shape_is_accepted(self) -> None:
        cases = {
            "F1": _fallback_audit(FALLBACK_REASON_MODEL_UNAVAILABLE),
            "F2": _fallback_audit(FALLBACK_REASON_CIRCUIT_OPEN),
            "F3": _fallback_audit(FALLBACK_REASON_CALL_FAILED),
            "F4": _fallback_audit(FALLBACK_REASON_INVALID_OUTPUT, provenance=_provenance()),
            "F5": _fallback_audit(fallback_reason_for_validator(VALIDATOR_V2_BATCH_CEILING), provenance=_provenance()),
            "F6": _fallback_audit(FALLBACK_REASON_INPUT_STALE, provenance=_provenance()),
        }
        for label, payload in cases.items():
            with self.subTest(failure_class=label):
                audit = normalize_fallback_audit(payload)
                self.assertEqual(audit.to_payload(), payload)

    def test_fallback_division_record_validates_with_battery_skipped(self) -> None:
        payload = _division(
            division_source=DIVISION_SOURCE_RULE_LADDER_FALLBACK,
            batches=[],
            batch_count=0,
            provenance=None,
            fallback=_fallback_audit(FALLBACK_REASON_MODEL_UNAVAILABLE),
        )
        result = _validate(payload)
        self.assertTrue(result["valid"])
        self.assertEqual(result["failures"], [])
        self.assertEqual({entry["status"] for entry in result["validator_results"]}, {"skipped"})
        self.assertEqual(len(result["validator_results"]), len(VALIDATOR_IDS))

    def test_invalid_fallback_reasons_are_rejected(self) -> None:
        for bad_reason in ("divider_exploded", "divider_validator_rejected:V99_nope", ""):
            with self.subTest(reason=bad_reason):
                self.assertFalse(is_valid_fallback_reason(bad_reason))
                with self.assertRaises(BatchDivisionContractError):
                    normalize_fallback_audit(_fallback_audit_with_reason(bad_reason))

    def test_divider_error_is_bounded_to_500_chars(self) -> None:
        payload = _fallback_audit(FALLBACK_REASON_CALL_FAILED)
        payload["divider_error"] = "x" * 501
        with self.assertRaises(BatchDivisionContractError):
            normalize_fallback_audit(payload)

    def test_fallback_reason_helper_rejects_unknown_validator(self) -> None:
        self.assertEqual(
            fallback_reason_for_validator(VALIDATOR_V1_PROVIDER_ENVELOPE),
            "divider_validator_rejected:V1_provider_envelope",
        )
        with self.assertRaises(BatchDivisionContractError):
            fallback_reason_for_validator("V99_nope")


def _fallback_audit_with_reason(reason: str) -> dict[str, Any]:
    payload = _fallback_audit(FALLBACK_REASON_CALL_FAILED)
    payload["fallback_reason"] = reason
    return payload


if __name__ == "__main__":
    unittest.main()
