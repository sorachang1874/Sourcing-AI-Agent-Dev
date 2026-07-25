"""WS7/W7.4 S1 offline suite for `sourcing.pipeline.compensation_intent.v1`.

Provenance: docs/WS7_COMPENSATION_DESIGN.md §2 (schema + the three key design
rules), §4 (recovery-of-recovery ladder + terminal statuses), §5 (paid-dispatch
safety); OQ1-OQ8 RATIFIED 2026-07-24 (slice S1 per §7/§9). Pins the compensation
contract's strict fail-closed normalization (exact-version lookup, key allowlists
at every level, the structural absence of any payload/dispatch slot), each
validator's accept+reject boundary, and the terminal-escalation record shape.

The load-bearing pins, in ruling order:

* OQ6 — ``stage="promote"`` is rejected with the explicit scope-out reason (no
  recovery-tick command-owner seat exists to attach a promote intent to).
* OQ5 / R-019 (RESIDUAL_LEDGER.md:38, `pending remediation`, call-site ceiling
  24) — the ``target_command_owner`` must be an EXISTING recovery-tick seat, a
  command type may belong to seats of exactly ONE stage, and the
  ``owner_idempotency_key`` must match that owner's real key GRAMMAR (exact
  segment shape). Compensation-authored keys (a ``compensation*`` namespace, the
  schema id, or a key embedding the intent's own ``intent_id``) are rejected, and
  so is every namespace-conformant but free-form suffix.
  **Stated honestly (D-C9):** a grammar match is a SHAPE check, not a derivation
  check. Proving a key was actually computed by an owner needs the store, so it
  is the S3 gate ``V_KEY_store_derived`` / ``derivation_verified`` — pinned here
  as a contract surface, not as a claim this offline module can make.
* OQ7 — the attempt ladder is bounded at 3, a spent intent may never carry a
  NULL terminal status (a silent re-loop is inexpressible), and the §4 ladder's
  output always round-trips through ``validate_v_attempt`` so a spent intent can
  still be CLOSED.

Companion oracle: tests/test_recovery_tick_characterization.py (the recovery-tick
floor, 18/18) stays green and UNTOUCHED — S1 wires nothing into the tick. All 18
seat rows are cross-checked against that oracle's ``CHARACTERIZED_PHASE_SEQUENCE``
(phase + owner label), and the 9 registry-bound rows additionally against
``recovery_drain_registry.DEFAULT_RECOVERY_DRAIN_BINDINGS``, so the seat table
cannot drift from either source of truth. The contract MODULE stays pure/offline
(stdlib only); those two cross-checks are lazy imports inside their test methods.
"""

from __future__ import annotations

import unittest
from typing import Any

from sourcing_agent.pipeline_compensation_contract import (
    ANCHOR_IDENTITY_SLOTS_BY_SUB_UNIT_KIND,
    ATTEMPT_MAX_CEILING,
    BACKOFF_OWNER_RECOVERY_TICK_POLL,
    COMPENSABLE_STAGE_VALUES,
    CREATED_BY_PHASE_COMPENSATION_AUDIT,
    DEFAULT_COMPENSATION_SEATS,
    GAP_DETECTED_BY_COMPLETENESS_AUDIT,
    GAP_SIGNAL_VALUES,
    OWNER_KEY_GRAMMARS_BY_COMMAND_TYPE,
    REPLAY_OUTCOME_ALREADY_SUCCEEDED,
    REPLAY_OUTCOME_COMPENSATED,
    REPLAY_OUTCOME_GAP_ABSENT,
    REPLAY_OUTCOME_TRANSIENT_FAILURE,
    SCHEMA_FAILURE_VALIDATOR_ID,
    SCHEMA_ID_V1,
    SIGNAL_BOARD_VISIBLE_BACKLOG,
    SIGNAL_MANIFEST_PARTIAL,
    SIGNAL_RETRY_WAIT_TAIL,
    SIGNAL_ROSTER_PARTIAL,
    SIGNAL_ROSTER_TRUNCATED,
    STAGE_ACQUIRE,
    STAGE_FETCH,
    STAGE_MATERIALIZE,
    STAGE_PROMOTE,
    SUB_UNIT_KIND_ACQUISITION_SHARD,
    SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM,
    SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
    TERMINAL_STATUS_COMPENSATED,
    TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN,
    TERMINAL_STATUS_SUPERSEDED,
    VALIDATOR_IDS,
    VALIDATOR_RESULT_STATUS_PASS,
    VALIDATOR_RESULT_STATUS_SKIPPED,
    VALIDATOR_V_ATTEMPT,
    VALIDATOR_V_DELTA,
    VALIDATOR_V_KEY,
    VALIDATOR_V_KEY_DERIVATION,
    VALIDATOR_V_LINEAGE,
    VALIDATOR_V_SEAT,
    VALIDATOR_V_SIGNAL,
    VALIDATOR_V_STAGE,
    CompensationAttempt,
    CompensationContractError,
    GapEvidenceRef,
    SourceLineage,
    build_compensation_escalation,
    build_compensation_seat_index,
    describe_owner_key_grammars,
    normalize_compensation_escalation,
    normalize_compensation_intent,
    resolve_compensation_outcome,
    validate_compensation_intent,
    validate_v_attempt,
    validate_v_delta,
    validate_v_key,
    validate_v_key_derivation,
    validate_v_lineage,
    validate_v_seat,
    validate_v_signal,
    validate_v_stage,
)

_HEX = "ab" * 32
_SHARD_ID = "profile_search|former|Google Multimodal Researcher|us"
_SEAT_INDEX = build_compensation_seat_index()

# Real owner-key shapes (durable_runtime `command_id_for` = "cmd_" + sha1[:24];
# every `*_idempotency_key` builder = command_type + ":" + sha1[:24]; the
# start-v2 receipt digest is a full sha256).
_COMMAND_ID = "cmd_3d1f0a7c9b2e4856ad01c2f4"
_SCOPE_HASH = "9f2c1a55d0b34e6f7a81be04"
_RECEIPT_DIGEST = "cd" * 32
_ACQUIRE_KEY = f"acquisition.probe.submit:acquisition_run:ar-77:parent:{_COMMAND_ID}"
_FETCH_KEY = f"linkedin.profile_refill.submit_batch:{_SCOPE_HASH}"
_MATERIALIZE_KEY = f"snapshot.compaction.run:{_SCOPE_HASH}"


def _lineage(**overrides: Any) -> dict[str, Any]:
    lineage = {
        "operation_id": "op-2026-07-24-0001",
        "job_id": "job-2026-07-24-0001",
        "materialization_generation_key": "genkey-tml-0007",
        "refill_plan_division_id": "",
    }
    lineage.update(overrides)
    return lineage


def _evidence(**overrides: Any) -> dict[str, Any]:
    evidence = {
        "missing_shard_ids": [_SHARD_ID],
        "truncated_shard_ids": [],
        "retry_wait_url_count": 0,
        "backlog_item_count": 0,
    }
    evidence.update(overrides)
    return evidence


def _acquire_intent(**overrides: Any) -> dict[str, Any]:
    """A valid 补 acquire intent: one missing shard, routed to the probe owner
    under that owner's OWN idempotency key (acquisition_command_owner's shared
    `_plan_acquisition_run_phase_command` grammar)."""
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "intent_id": "01JCMPNSTN0000000000000001",
        "stage": STAGE_ACQUIRE,
        "sub_unit": {
            "kind": SUB_UNIT_KIND_ACQUISITION_SHARD,
            "sub_unit_id": _SHARD_ID,
            "source_lineage": _lineage(),
        },
        "gap": {
            "detected_by": GAP_DETECTED_BY_COMPLETENESS_AUDIT,
            "signal": SIGNAL_ROSTER_PARTIAL,
            "evidence_ref": _evidence(),
        },
        "compensation_action": {
            "target_command_owner": "acquisition_probe_command_owner",
            "owner_idempotency_key": _ACQUIRE_KEY,
            "delta_only": True,
        },
        "attempt": {
            "count": 0,
            "max": ATTEMPT_MAX_CEILING,
            "backoff_owner": BACKOFF_OWNER_RECOVERY_TICK_POLL,
            "terminal_status": None,
        },
        "provenance": {
            "created_at": "2026-07-24T12:00:00Z",
            "created_by_phase": CREATED_BY_PHASE_COMPENSATION_AUDIT,
            "input_snapshot_sha256": _HEX,
        },
    }
    payload.update(overrides)
    return payload


def _fetch_intent(**overrides: Any) -> dict[str, Any]:
    """A valid 补 fetch intent: the retry_wait tail of one registry item, routed
    to the refill command owner. Carries the 议案① `refill_plan_division_id`
    (migration 0015) — fetch-only per D-C1."""
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "intent_id": "01JCMPNSTN0000000000000002",
        "stage": STAGE_FETCH,
        "sub_unit": {
            "kind": SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM,
            "sub_unit_id": "linkedin.com/in/example-person",
            "source_lineage": _lineage(refill_plan_division_id="div-0003"),
        },
        "gap": {
            "detected_by": GAP_DETECTED_BY_COMPLETENESS_AUDIT,
            "signal": SIGNAL_RETRY_WAIT_TAIL,
            "evidence_ref": _evidence(missing_shard_ids=[], retry_wait_url_count=7),
        },
        "compensation_action": {
            "target_command_owner": "profile_refill_command_owner",
            "owner_idempotency_key": _FETCH_KEY,
            "delta_only": True,
        },
        "attempt": {
            "count": 1,
            "max": ATTEMPT_MAX_CEILING,
            "backoff_owner": BACKOFF_OWNER_RECOVERY_TICK_POLL,
            "terminal_status": None,
        },
        "provenance": {
            "created_at": "2026-07-24T12:05:00+00:00",
            "created_by_phase": CREATED_BY_PHASE_COMPENSATION_AUDIT,
            "input_snapshot_sha256": _HEX,
        },
    }
    payload.update(overrides)
    return payload


def _materialize_intent(**overrides: Any) -> dict[str, Any]:
    """A valid 补 materialize intent: one partial snapshot manifest, routed to the
    snapshot_full_materialization queue under snapshot_compaction_run's key."""
    payload: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "intent_id": "01JCMPNSTN0000000000000003",
        "stage": STAGE_MATERIALIZE,
        "sub_unit": {
            "kind": SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
            "sub_unit_id": "snapshot-2026-07-24-0009",
            "source_lineage": _lineage(),
        },
        "gap": {
            "detected_by": GAP_DETECTED_BY_COMPLETENESS_AUDIT,
            "signal": SIGNAL_MANIFEST_PARTIAL,
            "evidence_ref": _evidence(truncated_shard_ids=[_SHARD_ID]),
        },
        "compensation_action": {
            "target_command_owner": "snapshot_full_materialization",
            "owner_idempotency_key": _MATERIALIZE_KEY,
            "delta_only": True,
        },
        "attempt": {
            "count": 0,
            "max": ATTEMPT_MAX_CEILING,
            "backoff_owner": BACKOFF_OWNER_RECOVERY_TICK_POLL,
            "terminal_status": None,
        },
        "provenance": {
            "created_at": "2026-07-24T12:10:00Z",
            "created_by_phase": CREATED_BY_PHASE_COMPENSATION_AUDIT,
            "input_snapshot_sha256": _HEX,
        },
    }
    payload.update(overrides)
    return payload


def _spent_intent() -> dict[str, Any]:
    return _acquire_intent(
        attempt={
            "count": ATTEMPT_MAX_CEILING,
            "max": ATTEMPT_MAX_CEILING,
            "backoff_owner": BACKOFF_OWNER_RECOVERY_TICK_POLL,
            "terminal_status": TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN,
        }
    )


class CompensationIntentSchemaTest(unittest.TestCase):
    """Strict fail-closed normalization (design §2.1)."""

    def test_round_trip_is_byte_identical_for_every_compensable_stage(self) -> None:
        for label, payload in (
            ("acquire", _acquire_intent()),
            ("fetch", _fetch_intent()),
            ("materialize", _materialize_intent()),
        ):
            with self.subTest(stage=label):
                intent = normalize_compensation_intent(payload)
                self.assertEqual(intent.to_payload(), payload)
                self.assertEqual(intent.schema_id, SCHEMA_ID_V1)

    def test_unknown_schema_id_is_rejected_with_no_auto_upgrade(self) -> None:
        for bad_schema in (
            "sourcing.pipeline.compensation_intent.v2",
            "sourcing.pipeline.compensation_intent",
            "",
            None,
            123,
        ):
            with self.subTest(schema_id=bad_schema):
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_intent(_acquire_intent(schema_id=bad_schema))

    def test_non_mapping_payloads_are_rejected(self) -> None:
        for bad in ([], "intent", 7, None):
            with self.subTest(payload=bad):
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_intent(bad)

    def test_unknown_top_level_key_is_rejected(self) -> None:
        payload = _acquire_intent()
        payload["dispatch_payload"] = {"urls": ["https://example.com"]}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)

    def test_every_nested_object_enforces_its_key_allowlist(self) -> None:
        # THE structural property: there is no slot ANYWHERE for a paid payload,
        # a provider request, a budget, or a dispatch target (§2.1 rule 1).
        injections = {
            "sub_unit": ("provider_payload", {"actor": "harvestapi"}),
            "gap": ("recomputed_coverage", {"expected": 10}),
            "compensation_action": ("payload", {"urls": ["https://example.com"]}),
            "attempt": ("unbounded", True),
            "provenance": ("model_provider", "openai"),
        }
        for container, (key, value) in injections.items():
            with self.subTest(container=container, injected=key):
                payload = _acquire_intent()
                payload[container] = {**payload[container], key: value}
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_intent(payload)

    def test_source_lineage_and_evidence_ref_allowlists_are_strict(self) -> None:
        payload = _acquire_intent()
        payload["sub_unit"]["source_lineage"] = {**_lineage(), "workspace_id": "ws-1"}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)
        payload = _acquire_intent()
        payload["gap"]["evidence_ref"] = {**_evidence(), "provider_dataset_id": "ds-1"}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)

    def test_missing_required_top_level_keys_are_rejected(self) -> None:
        for key in ("intent_id", "stage", "sub_unit", "gap", "compensation_action", "attempt", "provenance"):
            with self.subTest(missing=key):
                payload = _acquire_intent()
                payload.pop(key)
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_intent(payload)

    def test_intent_id_shape_is_enforced(self) -> None:
        for bad_id in ("", "has space", "x" * 65, "bad/slash"):
            with self.subTest(intent_id=bad_id):
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_intent(_acquire_intent(intent_id=bad_id))

    def test_sub_unit_id_must_be_non_empty_it_is_the_delta_anchor(self) -> None:
        payload = _acquire_intent()
        payload["sub_unit"]["sub_unit_id"] = "   "
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)

    def test_enum_fields_reject_unknown_values(self) -> None:
        cases = [
            ("sub_unit", "kind", "provider_batch"),
            ("gap", "detected_by", "rival_reconciler"),
            ("gap", "signal", "everything_looks_wrong"),
            ("attempt", "backoff_owner", "busy_loop"),
            ("attempt", "terminal_status", "retrying_forever"),
            ("provenance", "created_by_phase", "some_other_daemon"),
        ]
        for container, key, value in cases:
            with self.subTest(field=f"{container}.{key}"):
                payload = _acquire_intent()
                payload[container] = {**payload[container], key: value}
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_intent(payload)

    def test_provenance_hash_and_timestamp_shapes_are_enforced(self) -> None:
        payload = _acquire_intent()
        payload["provenance"] = {**payload["provenance"], "input_snapshot_sha256": "not-a-hash"}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)
        payload = _acquire_intent()
        payload["provenance"] = {**payload["provenance"], "created_at": "2026-07-24 12:00"}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)

    def test_attempt_count_and_max_are_bounded_by_the_ratified_ceiling(self) -> None:
        payload = _acquire_intent()
        payload["attempt"] = {**payload["attempt"], "max": ATTEMPT_MAX_CEILING + 1}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)
        payload = _acquire_intent()
        payload["attempt"] = {**payload["attempt"], "count": -1}
        with self.assertRaises(CompensationContractError):
            normalize_compensation_intent(payload)

    def test_durable_key_is_the_oq3_identity(self) -> None:
        # OQ3 keys on (stage, sub_unit_id, source_lineage) and source_lineage has
        # FOUR fields — the key must carry all of them (D-C12).
        intent = normalize_compensation_intent(_acquire_intent())
        self.assertEqual(
            intent.durable_key(),
            (STAGE_ACQUIRE, _SHARD_ID, "op-2026-07-24-0001", "job-2026-07-24-0001", "genkey-tml-0007", ""),
        )

    def test_durable_key_distinguishes_two_materialization_generations(self) -> None:
        # REGRESSION (D-C12): the earlier 4-tuple dropped
        # materialization_generation_key + refill_plan_division_id, so two gaps
        # of the same sub-unit from different generations collided and the S3
        # dedup swallowed the second one as a duplicate — it was never compensated.
        first = normalize_compensation_intent(
            _materialize_intent(
                sub_unit={
                    "kind": SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
                    "sub_unit_id": "snapshot-2026-07-24-0009",
                    "source_lineage": _lineage(materialization_generation_key="gen-A"),
                }
            )
        )
        second = normalize_compensation_intent(
            _materialize_intent(
                sub_unit={
                    "kind": SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
                    "sub_unit_id": "snapshot-2026-07-24-0009",
                    "source_lineage": _lineage(materialization_generation_key="gen-B"),
                }
            )
        )
        self.assertNotEqual(first.durable_key(), second.durable_key())
        self.assertEqual(len({first.durable_key(), second.durable_key()}), 2)

    def test_durable_key_distinguishes_two_refill_plan_divisions(self) -> None:
        first = normalize_compensation_intent(
            _fetch_intent(
                sub_unit={
                    "kind": SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM,
                    "sub_unit_id": "linkedin.com/in/example-person",
                    "source_lineage": _lineage(refill_plan_division_id="div-0003"),
                }
            )
        )
        second = normalize_compensation_intent(
            _fetch_intent(
                sub_unit={
                    "kind": SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM,
                    "sub_unit_id": "linkedin.com/in/example-person",
                    "source_lineage": _lineage(refill_plan_division_id="div-0004"),
                }
            )
        )
        self.assertNotEqual(first.durable_key(), second.durable_key())

    def test_durable_key_carries_every_source_lineage_field(self) -> None:
        intent = normalize_compensation_intent(_fetch_intent())
        lineage = intent.sub_unit.source_lineage
        key = intent.durable_key()
        for field_name, value in lineage.to_payload().items():
            with self.subTest(lineage_field=field_name):
                self.assertIn(value, key)


class CompensationSeatTableTest(unittest.TestCase):
    """The OQ1/OQ5 fence: every seat is an EXISTING recovery-tick phase."""

    def test_seat_index_is_unique_and_stage_scoped(self) -> None:
        index = build_compensation_seat_index()
        self.assertEqual(len(index), len(DEFAULT_COMPENSATION_SEATS))
        for seat in DEFAULT_COMPENSATION_SEATS:
            self.assertIn(seat.stage, COMPENSABLE_STAGE_VALUES)
            self.assertTrue(seat.command_types)

    def test_no_seat_exists_for_the_promote_stage(self) -> None:
        # OQ6's load-bearing premise, expressed structurally.
        self.assertEqual([seat.phase for seat in DEFAULT_COMPENSATION_SEATS if seat.stage == STAGE_PROMOTE], [])
        self.assertNotIn(STAGE_PROMOTE, {seat.stage for seat in DEFAULT_COMPENSATION_SEATS})

    def test_every_seat_is_a_phase_the_characterized_tick_already_runs(self) -> None:
        # THE drift guard for all 18 rows (the earlier cut checked only the 9
        # registry-bound ones, so a rename of e.g. `board_visible_apply` in the
        # tick would leave a dead seat with zero test signal — and V_SEAT would
        # keep accepting a phase the tick no longer runs). The tick oracle is the
        # source of truth regression_matrix.py already routes this module to.
        from tests.test_recovery_tick_characterization import CHARACTERIZED_PHASE_SEQUENCE

        characterized = dict(CHARACTERIZED_PHASE_SEQUENCE)
        self.assertEqual(len(DEFAULT_COMPENSATION_SEATS), 18)
        for seat in DEFAULT_COMPENSATION_SEATS:
            with self.subTest(phase=seat.phase):
                self.assertIn(seat.phase, characterized)
                self.assertEqual(seat.owner_label, characterized[seat.phase])

    def test_registry_bound_seats_match_the_real_drain_bindings(self) -> None:
        # Drift guard: the seat table mirrors recovery_drain_registry rather than
        # importing it (a pure contract module must not pull durable_runtime in).
        from sourcing_agent.recovery_drain_registry import DEFAULT_RECOVERY_DRAIN_BINDINGS

        bindings = {binding.phase: binding.owner for binding in DEFAULT_RECOVERY_DRAIN_BINDINGS}
        registry_bound = [seat for seat in DEFAULT_COMPENSATION_SEATS if seat.registry_bound]
        self.assertEqual(len(registry_bound), 9)
        for seat in registry_bound:
            with self.subTest(phase=seat.phase):
                self.assertIn(seat.phase, bindings)
                self.assertEqual(seat.owner_label, bindings[seat.phase])
        # The other 9 are literal tick phases with no drain-registry binding;
        # they are covered by the oracle cross-check above, not by this one.
        unbound = {seat.phase for seat in DEFAULT_COMPENSATION_SEATS if not seat.registry_bound}
        self.assertEqual(len(unbound), 9)
        self.assertEqual(unbound & set(bindings), set())

    def test_no_command_type_spans_two_stages(self) -> None:
        # REGRESSION (D-C11): `legacy_materialization_adapter` (stage=materialize)
        # used to carry `linkedin.discovery_query.run`, which is ALSO the sole
        # command type of the acquire-stage `operation_native_discovery_activity_owner`
        # seat. V_SEAT compares the intent's stage against the SEAT's declared
        # stage, so a table that seats an acquire-family paid command under a
        # materialize row pre-authorizes the crossing from the inside.
        stage_by_command_type: dict[str, set[str]] = {}
        for seat in DEFAULT_COMPENSATION_SEATS:
            for command_type in seat.command_types:
                stage_by_command_type.setdefault(command_type, set()).add(seat.stage)
        for command_type, stages in sorted(stage_by_command_type.items()):
            with self.subTest(command_type=command_type):
                self.assertEqual(len(stages), 1, f"{command_type} spans stages {sorted(stages)}")
        self.assertNotIn(
            "linkedin.discovery_query.run",
            {ct for seat in DEFAULT_COMPENSATION_SEATS if seat.stage == STAGE_MATERIALIZE for ct in seat.command_types},
        )

    def test_a_cross_stage_command_type_table_fails_closed(self) -> None:
        from sourcing_agent.pipeline_compensation_contract import CompensationSeat

        with self.assertRaises(CompensationContractError) as caught:
            build_compensation_seat_index(
                (
                    CompensationSeat(
                        phase="operation_native_discovery_activity_owner",
                        owner_label="linkedin_acquisition_owner",
                        stage=STAGE_ACQUIRE,
                        command_types=("linkedin.discovery_query.run",),
                    ),
                    CompensationSeat(
                        phase="legacy_materialization_adapter",
                        owner_label="durable_runtime_migration_adapter",
                        stage=STAGE_MATERIALIZE,
                        command_types=("linkedin.discovery_query.run",),
                        registry_bound=False,
                    ),
                )
            )
        self.assertIn("may belong to exactly one stage", str(caught.exception))

    def test_every_seat_command_type_has_a_registered_key_grammar(self) -> None:
        for seat in DEFAULT_COMPENSATION_SEATS:
            for command_type in seat.command_types:
                with self.subTest(phase=seat.phase, command_type=command_type):
                    self.assertIn(command_type, OWNER_KEY_GRAMMARS_BY_COMMAND_TYPE)
                    self.assertTrue(describe_owner_key_grammars(command_type))

    def test_a_seat_naming_a_grammarless_command_type_fails_closed(self) -> None:
        from sourcing_agent.pipeline_compensation_contract import CompensationSeat

        with self.assertRaises(CompensationContractError) as caught:
            build_compensation_seat_index(
                (
                    CompensationSeat(
                        phase="acquisition_probe_command_owner",
                        owner_label="acquisition_probe_owner",
                        stage=STAGE_ACQUIRE,
                        command_types=("compensation.topup.dispatch",),
                    ),
                )
            )
        self.assertIn("no owner key grammar", str(caught.exception))

    def test_the_seat_table_no_longer_carries_a_key_check_waiver(self) -> None:
        # REGRESSION: `inherits_root_operation_key=True` was an unconditional
        # early pass in V_KEY for the seat fronting the PAID `acquisition.run.create`.
        for seat in DEFAULT_COMPENSATION_SEATS:
            with self.subTest(phase=seat.phase):
                self.assertFalse(hasattr(seat, "inherits_root_operation_key"))

    def test_all_seven_acquisition_command_owners_are_compensable(self) -> None:
        from sourcing_agent.recovery_drain_registry import DEFAULT_RECOVERY_DRAIN_BINDINGS

        acquisition_phases = {
            binding.phase for binding in DEFAULT_RECOVERY_DRAIN_BINDINGS if binding.phase.startswith("acquisition_")
        }
        self.assertEqual(len(acquisition_phases), 7)
        seat_phases = {seat.phase for seat in DEFAULT_COMPENSATION_SEATS}
        self.assertTrue(acquisition_phases.issubset(seat_phases))

    def test_malformed_seat_rows_fail_loudly(self) -> None:
        from sourcing_agent.pipeline_compensation_contract import CompensationSeat

        good = DEFAULT_COMPENSATION_SEATS[0]
        with self.assertRaises(CompensationContractError):
            build_compensation_seat_index((good, good))
        with self.assertRaises(CompensationContractError):
            build_compensation_seat_index(
                (CompensationSeat(phase="x", owner_label="y", stage=STAGE_PROMOTE, command_types=("a",)),)
            )
        with self.assertRaises(CompensationContractError):
            build_compensation_seat_index(
                (CompensationSeat(phase="x", owner_label="y", stage=STAGE_ACQUIRE, command_types=()),)
            )


class ValidatorStageTest(unittest.TestCase):
    """V_STAGE — OQ6 scope cut."""

    def test_the_three_compensable_stages_pass(self) -> None:
        for stage in (STAGE_ACQUIRE, STAGE_FETCH, STAGE_MATERIALIZE):
            with self.subTest(stage=stage):
                self.assertIsNone(validate_v_stage(stage=stage))

    def test_promote_is_rejected_with_the_explicit_oq6_reason(self) -> None:
        reason = validate_v_stage(stage=STAGE_PROMOTE)
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("OQ6", reason)
        self.assertIn("no dedicated recovery-tick command-owner seat", reason)

    def test_unknown_stage_is_rejected(self) -> None:
        self.assertIsNotNone(validate_v_stage(stage="export"))

    def test_promote_is_also_rejected_at_strict_parse_time(self) -> None:
        result = validate_compensation_intent(_acquire_intent(stage=STAGE_PROMOTE))
        self.assertFalse(result["valid"])
        self.assertEqual([failure["validator_id"] for failure in result["failures"]], [SCHEMA_FAILURE_VALIDATOR_ID])
        self.assertIn("OQ6", result["failures"][0]["reason"])
        self.assertIsNone(result["normalized"])


class ValidatorSeatTest(unittest.TestCase):
    """V_SEAT — the target owner must already exist in the recovery tick."""

    def test_every_seat_accepts_its_own_stage(self) -> None:
        for seat in DEFAULT_COMPENSATION_SEATS:
            with self.subTest(phase=seat.phase):
                self.assertIsNone(
                    validate_v_seat(stage=seat.stage, target_command_owner=seat.phase, seat_index=_SEAT_INDEX)
                )

    def test_an_invented_owner_is_rejected(self) -> None:
        reason = validate_v_seat(
            stage=STAGE_ACQUIRE,
            target_command_owner="compensation_acquire_command_owner",
            seat_index=_SEAT_INDEX,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("not an existing recovery-tick compensation seat", reason)

    def test_cross_stage_routing_is_rejected(self) -> None:
        reason = validate_v_seat(
            stage=STAGE_ACQUIRE,
            target_command_owner="snapshot_full_materialization",
            seat_index=_SEAT_INDEX,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("may never route across stages", reason)

    def test_a_real_tick_phase_without_a_compensation_seat_is_still_rejected(self) -> None:
        # crm_writer_command_owner is a real drain binding but is deliberately
        # NOT a compensation seat (it is absent from the returned recovery
        # summary, so an audit reading the summary would be blind to it).
        self.assertIsNotNone(
            validate_v_seat(
                stage=STAGE_ACQUIRE, target_command_owner="crm_writer_command_owner", seat_index=_SEAT_INDEX
            )
        )


class ValidatorKeyTest(unittest.TestCase):
    """V_KEY — OQ5: the contract can never invent a dispatch identity."""

    def _check(self, *, key: str, owner: str = "acquisition_probe_command_owner", intent_id: str = "intent-1") -> Any:
        return validate_v_key(
            owner_idempotency_key=key,
            intent_id=intent_id,
            target_command_owner=owner,
            seat_index=_SEAT_INDEX,
        )

    def test_real_owner_key_grammars_pass(self) -> None:
        # Every shape transcribed from the code that mints it.
        cases = [
            ("acquisition_run_create_command_owner", f"acquisition.run.create:start-v2:{_RECEIPT_DIGEST}"),
            ("acquisition_intent_resolve_command_owner", f"acquisition.intent.resolve:parent:{_COMMAND_ID}"),
            ("acquisition_plan_build_command_owner", f"acquisition.plan.build:parent:{_COMMAND_ID}"),
            (
                "acquisition_plan_review_request_command_owner",
                f"acquisition.plan_review.request:plan:plan-3:parent:{_COMMAND_ID}",
            ),
            ("acquisition_plan_commit_command_owner", "acquisition.plan.commit:review:1234"),
            (
                "acquisition_probe_command_owner",
                f"acquisition.probe.collect:acquisition_run:ar-1:parent:{_COMMAND_ID}",
            ),
            (
                "acquisition_scale_plan_command_owner",
                f"acquisition.scale.plan:acquisition_run:ar-1:parent:{_COMMAND_ID}",
            ),
            ("operation_native_discovery_activity_owner", f"linkedin.discovery_query.run:{_SCOPE_HASH}"),
            ("profile_refill_command_owner", f"linkedin.profile_refill.submit_batch:{_SCOPE_HASH}"),
            ("profile_url_terminal_record_command_owner", f"linkedin.profile_url_terminal.record:{_SCOPE_HASH}"),
            (
                "operation_native_profile_fetch_activity_owner",
                f"linkedin.profile_fetch.activity.run:source:{_SCOPE_HASH}",
            ),
            (
                "operation_native_profile_fetch_activity_owner",
                f"linkedin.profile_fetch.provider.fetch:activity:{_SCOPE_HASH}",
            ),
            (
                "operation_native_profile_fetch_activity_owner",
                f"linkedin.profile_terminal.admit:activity:{_SCOPE_HASH}",
            ),
            ("local_apply_backlog", f"linkedin.local_profile_delta.apply:{_SCOPE_HASH}"),
            ("board_visible_apply", f"projection.board_visible_patch.publish:{_SCOPE_HASH}"),
            ("snapshot_full_materialization", f"snapshot.compaction.run:{_SCOPE_HASH}"),
            ("collection_authoritative_merge", f"collection.authoritative.merge:{_SCOPE_HASH}"),
            ("legacy_materialization_adapter", f"projection.person_search_index.build:{_SCOPE_HASH}"),
            ("legacy_materialization_adapter", f"projection.facet_layering.build:{_SCOPE_HASH}"),
        ]
        for owner, key in cases:
            with self.subTest(owner=owner, key=key):
                self.assertIsNone(self._check(key=key, owner=owner))

    def test_every_seat_has_at_least_one_admissible_and_one_fabricated_key(self) -> None:
        # Blanket property: for EVERY seat, a bare command type and a
        # namespace-conformant free-form suffix are both rejected.
        for seat in DEFAULT_COMPENSATION_SEATS:
            for command_type in seat.command_types:
                with self.subTest(phase=seat.phase, command_type=command_type):
                    self.assertIsNotNone(self._check(key=command_type, owner=seat.phase))
                    self.assertIsNotNone(self._check(key=f"{command_type}:scope-abc", owner=seat.phase))
                    self.assertIsNotNone(self._check(key=f"{command_type}:", owner=seat.phase))

    def test_a_namespace_conformant_but_fabricated_suffix_is_rejected(self) -> None:
        # THE central regression: the earlier fence was `key == command_type or
        # key.startswith(command_type + ":")`, so every one of these validated and
        # would have minted a brand-new paid command identity at S3 (OQ5/R-019).
        fabricated = [
            ("acquisition_plan_build_command_owner", "acquisition.plan.build:parent:00000000-DOES-NOT-EXIST"),
            ("acquisition_probe_command_owner", "acquisition.probe.submit:brand-new-identity-minted-by-compensation"),
            ("acquisition_probe_command_owner", "acquisition.probe.submit:compensation-topup-2026-07-24-shard-A"),
            ("acquisition_probe_command_owner", "acquisition.probe.submit:TOPUP:whatever-i-want"),
            (
                "acquisition_probe_command_owner",
                f"acquisition.probe.submit:acquisition_run:ar-1:parent:{_COMMAND_ID}:x",
            ),
            ("acquisition_probe_command_owner", "acquisition.probe.submit:acquisition_run:ar-1:parent:cmd-42"),
            ("snapshot_full_materialization", "snapshot.compaction.run:scope-abc"),
            ("snapshot_full_materialization", "snapshot.compaction.run:9f2c1a55d0b34e6f7a81"),
            ("snapshot_full_materialization", f"snapshot.compaction.run:{_SCOPE_HASH.upper()}"),
            ("operation_native_discovery_activity_owner", "linkedin.discovery_query.run:seed-invented-by-compensation"),
            (
                "operation_native_profile_fetch_activity_owner",
                f"linkedin.profile_fetch.activity.run:{_SCOPE_HASH}",
            ),
            ("acquisition_intent_resolve_command_owner", f"acquisition.intent.resolve:{_COMMAND_ID}"),
        ]
        for owner, key in fabricated:
            with self.subTest(owner=owner, key=key):
                reason = self._check(key=key, owner=owner)
                self.assertIsNotNone(reason, f"{key!r} must not validate for {owner!r}")
                assert reason is not None
                self.assertIn("does not match any owner key grammar", reason)

    def test_a_foreign_owner_namespace_is_rejected(self) -> None:
        reason = self._check(key=f"snapshot.compaction.run:{_SCOPE_HASH}", owner="acquisition_probe_command_owner")
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("does not match any owner key grammar", reason)

    def test_an_empty_key_is_rejected(self) -> None:
        for key in ("", "   "):
            with self.subTest(key=key):
                reason = self._check(key=key)
                self.assertIsNotNone(reason)
                assert reason is not None
                self.assertIn("could re-dispatch and re-pay", reason)

    def test_compensation_authored_namespaces_are_rejected(self) -> None:
        for key in (
            "compensation:acquire:shard-1",
            "compensation_intent:acquire:shard-1",
            "sourcing.pipeline.compensation_intent.v1:shard-1",
            "compensation_completeness_audit:shard-1",
            "补_acquire:shard-1",
            "COMPENSATION:shard-1",
        ):
            with self.subTest(key=key):
                reason = self._check(key=key)
                self.assertIsNotNone(reason)
                assert reason is not None
                self.assertTrue("may never mint a dispatch identity" in reason or "schema id" in reason)

    def test_a_key_embedding_the_intent_id_is_rejected(self) -> None:
        reason = self._check(
            key="acquisition.probe.submit:acquisition_run:ar-1:intent:01JCMPNSTN",
            intent_id="01JCMPNSTN",
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("NEW dispatch identity", reason)

    def test_the_root_acquisition_run_seat_no_longer_accepts_an_arbitrary_string(self) -> None:
        # THE second central regression: `inherits_root_operation_key=True` was an
        # unconditional early pass, so ANY non-compensation string validated for
        # the seat fronting the PAID `acquisition.run.create` — the most expensive
        # paid unit in the system. Its real key IS command-type-namespaced
        # (`acquisition.run.create:start-v2:{sha256}`,
        # acquisition_start_v2_create_postgres / _result_postgres), so the waiver's
        # premise was false (D-C10).
        owner = "acquisition_run_create_command_owner"
        for key in (
            "x",
            "totally-made-up-key-9999",
            "acquisition_run:brand_new_paid_run",
            "  padded-fabrication  ",
            "ACQUISITION.RUN.CREATE-but-not-really",
            "agent-start-v2:9f2c1a55",
            "acquisition.run.create",
            "acquisition.run.create:start-v2:not-a-digest",
            f"acquisition.run.create:start-v2:{_SCOPE_HASH}",
            f"acquisition.run.create:parent:{_COMMAND_ID}",
        ):
            with self.subTest(key=key):
                self.assertIsNotNone(self._check(key=key, owner=owner), f"{key!r} must not validate")
        # ...and the one real shape still passes.
        self.assertIsNone(self._check(key=f"acquisition.run.create:start-v2:{_RECEIPT_DIGEST}", owner=owner))
        # ...as does the self-minted rejection, ahead of the grammar check.
        reason = self._check(key="compensation:root:1", owner=owner)
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("may never mint a dispatch identity", reason)

    def test_an_unknown_seat_defers_to_v_seat_without_double_reporting(self) -> None:
        self.assertIsNone(self._check(key="anything", owner="not_a_seat"))


class ValidatorKeyDerivationTest(unittest.TestCase):
    """V_KEY_store_derived — the S3-only half of OQ5 (D-C9).

    The offline grammar check cannot prove derivation; this validator is the
    contract surface S3 must call with the owner's persisted keys.
    """

    def test_a_key_present_in_the_owner_inventory_passes(self) -> None:
        self.assertIsNone(
            validate_v_key_derivation(
                owner_idempotency_key=_ACQUIRE_KEY,
                existing_owner_idempotency_keys=(f"snapshot.compaction.run:{_SCOPE_HASH}", _ACQUIRE_KEY),
            )
        )

    def test_a_grammar_conformant_key_with_no_command_row_is_rejected(self) -> None:
        # The exact hole the grammar check cannot close: well-shaped, wrong hash.
        reason = validate_v_key_derivation(
            owner_idempotency_key=f"snapshot.compaction.run:{'0' * 24}",
            existing_owner_idempotency_keys=(f"snapshot.compaction.run:{_SCOPE_HASH}",),
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("NEW dispatch identity", reason)
        self.assertIn("already_succeeded", reason)

    def test_an_empty_inventory_rejects_everything(self) -> None:
        for inventory in ((), ("", "   ")):
            with self.subTest(inventory=inventory):
                self.assertIsNotNone(
                    validate_v_key_derivation(
                        owner_idempotency_key=_ACQUIRE_KEY, existing_owner_idempotency_keys=inventory
                    )
                )

    def test_the_entrypoint_reports_the_row_as_skipped_when_no_inventory_is_given(self) -> None:
        result = validate_compensation_intent(_acquire_intent())
        self.assertTrue(result["valid"], result["failures"])
        self.assertFalse(result["derivation_verified"])
        row = next(entry for entry in result["validator_results"] if entry["validator"] == VALIDATOR_V_KEY_DERIVATION)
        self.assertEqual(row["status"], VALIDATOR_RESULT_STATUS_SKIPPED)
        self.assertIn("S3 MUST supply the inventory", row["reason"])

    def test_the_entrypoint_verifies_derivation_when_the_inventory_is_supplied(self) -> None:
        result = validate_compensation_intent(_acquire_intent(), existing_owner_idempotency_keys=(_ACQUIRE_KEY,))
        self.assertTrue(result["valid"], result["failures"])
        self.assertTrue(result["derivation_verified"])
        row = next(entry for entry in result["validator_results"] if entry["validator"] == VALIDATOR_V_KEY_DERIVATION)
        self.assertEqual(row["status"], VALIDATOR_RESULT_STATUS_PASS)

    def test_the_entrypoint_fails_an_undeived_key_when_the_inventory_is_supplied(self) -> None:
        result = validate_compensation_intent(
            _acquire_intent(),
            existing_owner_idempotency_keys=(f"acquisition.probe.submit:acquisition_run:ar-99:parent:{_COMMAND_ID}",),
        )
        self.assertFalse(result["valid"])
        self.assertFalse(result["derivation_verified"])
        self.assertEqual([failure["validator_id"] for failure in result["failures"]], [VALIDATOR_V_KEY_DERIVATION])
        self.assertIsNone(result["durable_key"])

    def test_a_schema_failure_still_reports_derivation_unverified(self) -> None:
        result = validate_compensation_intent({"schema_id": "nope"}, existing_owner_idempotency_keys=(_ACQUIRE_KEY,))
        self.assertFalse(result["valid"])
        self.assertFalse(result["derivation_verified"])


class ValidatorDeltaTest(unittest.TestCase):
    """V_DELTA — delta-only, anchored to one observed missing atom."""

    def test_a_shard_anchored_intent_passes_when_the_evidence_names_it(self) -> None:
        self.assertIsNone(
            validate_v_delta(
                kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
                sub_unit_id=_SHARD_ID,
                signal=SIGNAL_ROSTER_PARTIAL,
                evidence_ref=GapEvidenceRef(missing_shard_ids=(_SHARD_ID,)),
                delta_only=True,
            )
        )

    def test_delta_only_false_is_rejected(self) -> None:
        reason = validate_v_delta(
            kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
            sub_unit_id=_SHARD_ID,
            signal=SIGNAL_ROSTER_PARTIAL,
            evidence_ref=GapEvidenceRef(missing_shard_ids=(_SHARD_ID,)),
            delta_only=False,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("delta_only must be true", reason)

    def test_empty_evidence_is_rejected(self) -> None:
        reason = validate_v_delta(
            kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
            sub_unit_id=_SHARD_ID,
            signal=SIGNAL_ROSTER_PARTIAL,
            evidence_ref=GapEvidenceRef(),
            delta_only=True,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("not a detected gap", reason)

    def test_the_signal_must_populate_its_own_evidence_slot(self) -> None:
        # A retry_wait_tail carrying only shard ids is a mis-composed intent.
        reason = validate_v_delta(
            kind=SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM,
            sub_unit_id="linkedin.com/in/x",
            signal=SIGNAL_RETRY_WAIT_TAIL,
            evidence_ref=GapEvidenceRef(missing_shard_ids=(_SHARD_ID,)),
            delta_only=True,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("retry_wait_url_count", reason)

    def test_a_shard_anchor_outside_the_evidence_is_rejected(self) -> None:
        reason = validate_v_delta(
            kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
            sub_unit_id="profile_search|current|Some Other Shard",
            signal=SIGNAL_ROSTER_PARTIAL,
            evidence_ref=GapEvidenceRef(missing_shard_ids=(_SHARD_ID,)),
            delta_only=True,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("not among the identities the evidence names", reason)
        self.assertIn("evidence_ref.missing_shard_ids", reason)

    def test_a_truncated_shard_anchor_is_also_accepted(self) -> None:
        self.assertIsNone(
            validate_v_delta(
                kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
                sub_unit_id=_SHARD_ID,
                signal=SIGNAL_ROSTER_TRUNCATED,
                evidence_ref=GapEvidenceRef(truncated_shard_ids=(_SHARD_ID,)),
                delta_only=True,
            )
        )

    def test_the_anchor_coverage_table_pins_exactly_which_kinds_are_exempt(self) -> None:
        # D-C13: the anchor binding is enforced only for kinds whose evidence
        # slots carry identities. The exemption is DECLARED here, not implied by
        # a hard-coded `if` — so a new identity slot turns the check on by
        # editing the table alone, and any silent widening of the exempt set
        # fails this test.
        self.assertEqual(
            ANCHOR_IDENTITY_SLOTS_BY_SUB_UNIT_KIND[SUB_UNIT_KIND_ACQUISITION_SHARD],
            ("missing_shard_ids", "truncated_shard_ids"),
        )
        exempt = {kind for kind, slots in ANCHOR_IDENTITY_SLOTS_BY_SUB_UNIT_KIND.items() if not slots}
        self.assertEqual(exempt, {SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT, SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM})
        # Every kind has a row: a kind missing from the table would silently be
        # treated as exempt.
        from sourcing_agent.pipeline_compensation_contract import SUB_UNIT_KIND_VALUES

        self.assertEqual(set(ANCHOR_IDENTITY_SLOTS_BY_SUB_UNIT_KIND), set(SUB_UNIT_KIND_VALUES))
        # Each exempt kind's own signals genuinely have no identity slot.
        from sourcing_agent.pipeline_compensation_contract import _REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL

        self.assertEqual(_REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL[SIGNAL_RETRY_WAIT_TAIL], ("retry_wait_url_count",))
        self.assertEqual(_REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL[SIGNAL_BOARD_VISIBLE_BACKLOG], ("backlog_item_count",))

    def test_exempt_kinds_are_anchored_by_their_own_id_not_by_the_evidence(self) -> None:
        # A partial manifest's anchor is the snapshot durable unit; the shard
        # evidence explains WHY it is incomplete. Documented gap, not a silent one.
        self.assertIsNone(
            validate_v_delta(
                kind=SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
                sub_unit_id="snapshot-9",
                signal=SIGNAL_MANIFEST_PARTIAL,
                evidence_ref=GapEvidenceRef(truncated_shard_ids=(_SHARD_ID,)),
                delta_only=True,
            )
        )

    def test_an_unknown_signal_fails_closed_with_the_contract_error(self) -> None:
        # It used to leak a bare KeyError, so a caller catching
        # CompensationContractError crashed instead of rejecting.
        with self.assertRaises(CompensationContractError) as caught:
            validate_v_delta(
                kind=SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
                sub_unit_id="u1",
                signal="new_signal_2027",
                evidence_ref=GapEvidenceRef(backlog_item_count=1),
                delta_only=True,
            )
        self.assertIn("no registered evidence slot", str(caught.exception))

    def test_every_gap_signal_has_an_evidence_slot_row(self) -> None:
        # Pins the two collections in sync so the next signal added to the enum
        # cannot silently arm the fail-closed path above.
        from sourcing_agent.pipeline_compensation_contract import _REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL

        self.assertEqual(set(GAP_SIGNAL_VALUES), set(_REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL))

    def test_board_visible_backlog_uses_its_own_evidence_slot(self) -> None:
        self.assertIsNone(
            validate_v_delta(
                kind=SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
                sub_unit_id="board-unit-3",
                signal=SIGNAL_BOARD_VISIBLE_BACKLOG,
                evidence_ref=GapEvidenceRef(backlog_item_count=4),
                delta_only=True,
            )
        )
        self.assertIsNotNone(
            validate_v_delta(
                kind=SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
                sub_unit_id="board-unit-3",
                signal=SIGNAL_BOARD_VISIBLE_BACKLOG,
                evidence_ref=GapEvidenceRef(retry_wait_url_count=9),
                delta_only=True,
            )
        )


class ValidatorLineageTest(unittest.TestCase):
    """V_LINEAGE — one lineage, never widened."""

    def test_a_lineage_with_either_identity_passes(self) -> None:
        for lineage in (
            SourceLineage(operation_id="op-1", job_id=""),
            SourceLineage(operation_id="", job_id="job-1"),
        ):
            with self.subTest(lineage=lineage):
                self.assertIsNone(validate_v_lineage(stage=STAGE_ACQUIRE, source_lineage=lineage))

    def test_a_lineage_with_no_identity_is_rejected(self) -> None:
        reason = validate_v_lineage(stage=STAGE_ACQUIRE, source_lineage=SourceLineage(operation_id="  ", job_id=""))
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("never be drained cross-lineage", reason)

    def test_refill_plan_division_id_is_fetch_only(self) -> None:
        lineage = SourceLineage(operation_id="op-1", job_id="job-1", refill_plan_division_id="div-1")
        self.assertIsNone(validate_v_lineage(stage=STAGE_FETCH, source_lineage=lineage))
        for stage in (STAGE_ACQUIRE, STAGE_MATERIALIZE):
            with self.subTest(stage=stage):
                reason = validate_v_lineage(stage=stage, source_lineage=lineage)
                self.assertIsNotNone(reason)
                assert reason is not None
                self.assertIn("fetch-only", reason)

    def test_absent_division_id_is_tolerated_for_fetch(self) -> None:
        # D-C1: 议案① S5 has not flipped, so the field is written only for
        # validated shadow proposals; compensation reads it defensively.
        self.assertIsNone(
            validate_v_lineage(stage=STAGE_FETCH, source_lineage=SourceLineage(operation_id="op-1", job_id="job-1"))
        )


class ValidatorSignalTest(unittest.TestCase):
    """V_SIGNAL — owner self-report, stage-scoped (OQ2)."""

    def test_each_stage_accepts_its_own_signals_and_kinds(self) -> None:
        cases = [
            (STAGE_ACQUIRE, SUB_UNIT_KIND_ACQUISITION_SHARD, SIGNAL_ROSTER_PARTIAL),
            (STAGE_ACQUIRE, SUB_UNIT_KIND_ACQUISITION_SHARD, SIGNAL_ROSTER_TRUNCATED),
            (STAGE_FETCH, SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM, SIGNAL_RETRY_WAIT_TAIL),
            (STAGE_MATERIALIZE, SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT, SIGNAL_MANIFEST_PARTIAL),
            (STAGE_MATERIALIZE, SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT, SIGNAL_BOARD_VISIBLE_BACKLOG),
        ]
        for stage, kind, signal in cases:
            with self.subTest(stage=stage, signal=signal):
                self.assertIsNone(
                    validate_v_signal(
                        stage=stage,
                        kind=kind,
                        signal=signal,
                        detected_by=GAP_DETECTED_BY_COMPLETENESS_AUDIT,
                    )
                )

    def test_a_cross_stage_signal_is_rejected(self) -> None:
        reason = validate_v_signal(
            stage=STAGE_ACQUIRE,
            kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
            signal=SIGNAL_RETRY_WAIT_TAIL,
            detected_by=GAP_DETECTED_BY_COMPLETENESS_AUDIT,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("self-reported signal", reason)

    def test_a_cross_stage_sub_unit_kind_is_rejected(self) -> None:
        reason = validate_v_signal(
            stage=STAGE_FETCH,
            kind=SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
            signal=SIGNAL_RETRY_WAIT_TAIL,
            detected_by=GAP_DETECTED_BY_COMPLETENESS_AUDIT,
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("durable atom", reason)

    def test_a_non_audit_detector_is_rejected(self) -> None:
        reason = validate_v_signal(
            stage=STAGE_ACQUIRE,
            kind=SUB_UNIT_KIND_ACQUISITION_SHARD,
            signal=SIGNAL_ROSTER_PARTIAL,
            detected_by="standalone_reconciler",
        )
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("second reconciler", reason)


class ValidatorAttemptTest(unittest.TestCase):
    """V_ATTEMPT — OQ7: bounded, never fail-open."""

    @staticmethod
    def _attempt(count: int, maximum: int = ATTEMPT_MAX_CEILING, terminal: str | None = None) -> CompensationAttempt:
        return CompensationAttempt(
            count=count, max=maximum, backoff_owner=BACKOFF_OWNER_RECOVERY_TICK_POLL, terminal_status=terminal
        )

    def test_attempts_below_the_ceiling_pass_with_an_open_terminal_status(self) -> None:
        for count in (0, 1, 2):
            with self.subTest(count=count):
                self.assertIsNone(validate_v_attempt(attempt=self._attempt(count)))

    def test_attempt_three_passes_only_when_already_escalated(self) -> None:
        self.assertIsNone(
            validate_v_attempt(
                attempt=self._attempt(ATTEMPT_MAX_CEILING, terminal=TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN)
            )
        )

    def test_a_spent_ladder_with_a_null_terminal_status_is_rejected(self) -> None:
        reason = validate_v_attempt(attempt=self._attempt(ATTEMPT_MAX_CEILING))
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("never a silent re-loop, never fail-open", reason)

    def test_attempt_four_is_rejected(self) -> None:
        # 4 > the ratified ceiling: rejected at strict-parse time...
        payload = _acquire_intent()
        payload["attempt"] = {**payload["attempt"], "count": 4, "max": 4}
        result = validate_compensation_intent(payload)
        self.assertFalse(result["valid"])
        self.assertEqual([failure["validator_id"] for failure in result["failures"]], [SCHEMA_FAILURE_VALIDATOR_ID])
        # ...and by the validator when constructed directly.
        reason = validate_v_attempt(attempt=self._attempt(4, maximum=4))
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("exceeds the ratified ceiling", reason)

    def test_count_above_max_is_rejected(self) -> None:
        reason = validate_v_attempt(attempt=self._attempt(3, maximum=2, terminal=TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN))
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("exceeds attempt.max", reason)

    def test_premature_escalation_is_rejected(self) -> None:
        reason = validate_v_attempt(attempt=self._attempt(1, terminal=TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN))
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("requires a spent attempt ladder", reason)

    def test_compensated_and_superseded_terminals_are_legal_at_any_count(self) -> None:
        # REGRESSION (D-C14): this used to exercise only count=1, so it
        # over-claimed "any count" while the count==max boundary was rejected —
        # which is exactly the state the §4 ladder emits for a spent intent whose
        # gap later closes, leaving an escalated row with NO way to ever close.
        for terminal in (TERMINAL_STATUS_COMPENSATED, TERMINAL_STATUS_SUPERSEDED):
            for count in range(0, ATTEMPT_MAX_CEILING + 1):
                with self.subTest(terminal=terminal, count=count):
                    self.assertIsNone(validate_v_attempt(attempt=self._attempt(count, terminal=terminal)))

    def test_a_spent_escalated_intent_can_still_be_closed(self) -> None:
        spent = self._attempt(ATTEMPT_MAX_CEILING, terminal=TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN)
        self.assertIsNone(validate_v_attempt(attempt=spent))
        for outcome, expected in (
            (REPLAY_OUTCOME_ALREADY_SUCCEEDED, TERMINAL_STATUS_COMPENSATED),
            (REPLAY_OUTCOME_GAP_ABSENT, TERMINAL_STATUS_SUPERSEDED),
        ):
            with self.subTest(replay_outcome=outcome):
                resolved = resolve_compensation_outcome(
                    replay_outcome=outcome, attempt_count=spent.count, attempt_max=spent.max
                )
                self.assertEqual(resolved["terminal_status"], expected)
                closed = CompensationAttempt(
                    count=int(resolved["next_attempt_count"]),
                    max=spent.max,
                    backoff_owner=BACKOFF_OWNER_RECOVERY_TICK_POLL,
                    terminal_status=resolved["terminal_status"],
                )
                self.assertIsNone(validate_v_attempt(attempt=closed))

    def test_a_spent_ladder_may_never_carry_a_null_terminal_status(self) -> None:
        # The one shape OQ7 makes inexpressible: fail-open / silent re-loop.
        for maximum in range(1, ATTEMPT_MAX_CEILING + 1):
            with self.subTest(maximum=maximum):
                reason = validate_v_attempt(attempt=self._attempt(maximum, maximum=maximum))
                self.assertIsNotNone(reason)
                assert reason is not None
                self.assertIn("never a silent re-loop, never fail-open", reason)


class CompensationLadderTest(unittest.TestCase):
    """The §4 recovery-of-recovery ladder as a pure function."""

    def test_already_succeeded_terminalizes_compensated_without_escalating(self) -> None:
        outcome = resolve_compensation_outcome(replay_outcome=REPLAY_OUTCOME_ALREADY_SUCCEEDED, attempt_count=1)
        self.assertEqual(outcome["terminal_status"], TERMINAL_STATUS_COMPENSATED)
        self.assertFalse(outcome["escalate"])
        self.assertEqual(outcome["next_attempt_count"], 1)
        self.assertIn("zero re-dispatch, zero payment", outcome["reason"])

    def test_a_successful_replay_terminalizes_compensated(self) -> None:
        outcome = resolve_compensation_outcome(replay_outcome=REPLAY_OUTCOME_COMPENSATED, attempt_count=0)
        self.assertEqual(outcome["terminal_status"], TERMINAL_STATUS_COMPENSATED)

    def test_a_vanished_gap_terminalizes_superseded(self) -> None:
        outcome = resolve_compensation_outcome(replay_outcome=REPLAY_OUTCOME_GAP_ABSENT, attempt_count=2)
        self.assertEqual(outcome["terminal_status"], TERMINAL_STATUS_SUPERSEDED)
        self.assertFalse(outcome["escalate"])

    def test_transient_failures_climb_the_bounded_ladder_then_escalate(self) -> None:
        expected = [
            (0, 1, None, False),
            (1, 2, None, False),
            (2, 3, TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN, True),
        ]
        for count, next_count, terminal, escalate in expected:
            with self.subTest(attempt_count=count):
                outcome = resolve_compensation_outcome(
                    replay_outcome=REPLAY_OUTCOME_TRANSIENT_FAILURE, attempt_count=count
                )
                self.assertEqual(outcome["next_attempt_count"], next_count)
                self.assertEqual(outcome["terminal_status"], terminal)
                self.assertEqual(outcome["escalate"], escalate)

    def test_the_ladder_never_exceeds_the_ceiling(self) -> None:
        outcome = resolve_compensation_outcome(
            replay_outcome=REPLAY_OUTCOME_TRANSIENT_FAILURE, attempt_count=ATTEMPT_MAX_CEILING
        )
        self.assertEqual(outcome["next_attempt_count"], ATTEMPT_MAX_CEILING)
        self.assertEqual(outcome["terminal_status"], TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN)

    def test_every_ladder_outcome_round_trips_through_v_attempt(self) -> None:
        # THE agreement regression (D-C14): the ladder used to emit
        # (count=3, terminal="compensated"/"superseded") states that
        # validate_v_attempt then rejected, so a resolved intent could not be
        # written back at all.
        outcomes = (
            REPLAY_OUTCOME_ALREADY_SUCCEEDED,
            REPLAY_OUTCOME_COMPENSATED,
            REPLAY_OUTCOME_GAP_ABSENT,
            REPLAY_OUTCOME_TRANSIENT_FAILURE,
        )
        checked = 0
        for outcome in outcomes:
            for maximum in range(1, ATTEMPT_MAX_CEILING + 1):
                for count in range(0, maximum + 1):
                    with self.subTest(replay_outcome=outcome, attempt_max=maximum, attempt_count=count):
                        resolved = resolve_compensation_outcome(
                            replay_outcome=outcome, attempt_count=count, attempt_max=maximum
                        )
                        next_attempt = CompensationAttempt(
                            count=int(resolved["next_attempt_count"]),
                            max=maximum,
                            backoff_owner=BACKOFF_OWNER_RECOVERY_TICK_POLL,
                            terminal_status=resolved["terminal_status"],
                        )
                        self.assertIsNone(
                            validate_v_attempt(attempt=next_attempt),
                            f"{outcome} @ {count}/{maximum} -> {resolved}",
                        )
                        checked += 1
        self.assertEqual(checked, 4 * (2 + 3 + 4))

    def test_the_ladder_fails_closed_on_bad_inputs(self) -> None:
        with self.assertRaises(CompensationContractError):
            resolve_compensation_outcome(replay_outcome="retry_forever", attempt_count=0)
        with self.assertRaises(CompensationContractError):
            resolve_compensation_outcome(
                replay_outcome=REPLAY_OUTCOME_TRANSIENT_FAILURE, attempt_count=0, attempt_max=99
            )
        with self.assertRaises(CompensationContractError):
            resolve_compensation_outcome(
                replay_outcome=REPLAY_OUTCOME_TRANSIENT_FAILURE, attempt_count=3, attempt_max=2
            )


class EscalationRecordTest(unittest.TestCase):
    """The §4 terminal `compensation_exhausted_needs_human` record shape."""

    def test_escalation_record_round_trips_and_is_board_visible(self) -> None:
        intent = normalize_compensation_intent(_spent_intent())
        record = build_compensation_escalation(intent=intent, escalation_reason="owner replay backpressured 3x")
        self.assertEqual(record["terminal_status"], TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN)
        self.assertTrue(record["needs_human"])
        self.assertTrue(record["board_visible"])
        self.assertEqual(record["attempt_count"], ATTEMPT_MAX_CEILING)
        self.assertEqual(record["sub_unit_id"], _SHARD_ID)
        self.assertEqual(record["target_command_owner"], "acquisition_probe_command_owner")
        self.assertEqual(normalize_compensation_escalation(record), record)

    def test_escalation_carries_no_payload_or_dispatch_instruction(self) -> None:
        intent = normalize_compensation_intent(_spent_intent())
        record = build_compensation_escalation(intent=intent, escalation_reason="spent")
        for forbidden in ("payload", "owner_idempotency_key", "urls", "command_payload", "provider"):
            self.assertNotIn(forbidden, record)

    def test_escalating_an_unspent_ladder_fails_closed(self) -> None:
        intent = normalize_compensation_intent(_acquire_intent())
        with self.assertRaises(CompensationContractError):
            build_compensation_escalation(intent=intent, escalation_reason="too early")

    def test_escalation_reason_is_bounded(self) -> None:
        intent = normalize_compensation_intent(_spent_intent())
        record = build_compensation_escalation(intent=intent, escalation_reason="z" * 900)
        self.assertEqual(len(record["escalation_reason"]), 500)

    def test_escalation_normalizer_rejects_drifted_records(self) -> None:
        intent = normalize_compensation_intent(_spent_intent())
        good = build_compensation_escalation(intent=intent, escalation_reason="spent")
        for mutate in (
            {"terminal_status": TERMINAL_STATUS_COMPENSATED},
            {"needs_human": False},
            {"board_visible": False},
            {"attempt_count": 1},
            {"schema_id": "sourcing.pipeline.compensation_intent.v2"},
        ):
            with self.subTest(mutation=sorted(mutate)):
                with self.assertRaises(CompensationContractError):
                    normalize_compensation_escalation({**good, **mutate})
        with self.subTest(mutation="unknown key"):
            with self.assertRaises(CompensationContractError):
                normalize_compensation_escalation({**good, "dispatch_payload": {}})

    def test_escalation_rejects_a_count_that_overshoots_max(self) -> None:
        # REGRESSION (D-C15): the normalizer checked only `count < max`, so
        # `count=3, max=1` was ACCEPTED here while validate_v_attempt rejected
        # the equivalent intent state — the two surfaces disagreed on what a
        # well-formed spent ladder is.
        intent = normalize_compensation_intent(_spent_intent())
        good = build_compensation_escalation(intent=intent, escalation_reason="spent")
        with self.assertRaises(CompensationContractError) as caught:
            normalize_compensation_escalation({**good, "attempt_count": 3, "attempt_max": 1})
        self.assertIn("exceeds", str(caught.exception))
        self.assertIsNotNone(
            validate_v_attempt(
                attempt=CompensationAttempt(
                    count=3, max=1, backoff_owner=BACKOFF_OWNER_RECOVERY_TICK_POLL, terminal_status=None
                )
            )
        )

    def test_escalation_accepts_the_boundary_where_count_equals_max(self) -> None:
        intent = normalize_compensation_intent(_spent_intent())
        good = build_compensation_escalation(intent=intent, escalation_reason="spent")
        self.assertEqual(good["attempt_count"], good["attempt_max"])
        self.assertEqual(normalize_compensation_escalation(good), good)


class ValidateCompensationIntentTest(unittest.TestCase):
    """The top-level fail-closed entrypoint."""

    def test_valid_intents_pass_the_whole_battery(self) -> None:
        for label, payload in (
            ("acquire", _acquire_intent()),
            ("fetch", _fetch_intent()),
            ("materialize", _materialize_intent()),
            ("spent+escalated", _spent_intent()),
        ):
            with self.subTest(case=label):
                result = validate_compensation_intent(payload)
                self.assertTrue(result["valid"], result["failures"])
                self.assertEqual(result["failures"], [])
                self.assertEqual(result["normalized"], payload)
                self.assertEqual([entry["validator"] for entry in result["validator_results"]], list(VALIDATOR_IDS))
                offline_rows = [
                    entry for entry in result["validator_results"] if entry["validator"] != VALIDATOR_V_KEY_DERIVATION
                ]
                self.assertTrue(all(entry["status"] == VALIDATOR_RESULT_STATUS_PASS for entry in offline_rows))
                # The store-derivation row is honestly reported as unproven
                # offline — S3 must supply the inventory (D-C9).
                self.assertFalse(result["derivation_verified"])
                self.assertIsNotNone(result["durable_key"])
                self.assertEqual(len(result["durable_key"]), 6)

    def test_a_strict_parse_failure_reports_only_the_schema_validator(self) -> None:
        result = validate_compensation_intent({"schema_id": "nope"})
        self.assertFalse(result["valid"])
        self.assertEqual([failure["validator_id"] for failure in result["failures"]], [SCHEMA_FAILURE_VALIDATOR_ID])
        self.assertIsNone(result["normalized"])
        self.assertEqual(result["validator_results"], [])
        self.assertIsNone(result["durable_key"])

    def test_each_validator_can_be_driven_to_fail_through_the_entrypoint(self) -> None:
        cases: list[tuple[str, dict[str, Any]]] = [
            (
                VALIDATOR_V_SEAT,
                _acquire_intent(
                    compensation_action={
                        "target_command_owner": "compensation_acquire_owner",
                        "owner_idempotency_key": _ACQUIRE_KEY,
                        "delta_only": True,
                    }
                ),
            ),
            (
                VALIDATOR_V_KEY,
                _acquire_intent(
                    compensation_action={
                        "target_command_owner": "acquisition_probe_command_owner",
                        "owner_idempotency_key": "compensation:acquire:shard-1",
                        "delta_only": True,
                    }
                ),
            ),
            (
                VALIDATOR_V_DELTA,
                _acquire_intent(
                    compensation_action={
                        "target_command_owner": "acquisition_probe_command_owner",
                        "owner_idempotency_key": _ACQUIRE_KEY,
                        "delta_only": False,
                    }
                ),
            ),
            (
                VALIDATOR_V_LINEAGE,
                _acquire_intent(
                    sub_unit={
                        "kind": SUB_UNIT_KIND_ACQUISITION_SHARD,
                        "sub_unit_id": _SHARD_ID,
                        "source_lineage": _lineage(operation_id="", job_id=""),
                    }
                ),
            ),
            (
                VALIDATOR_V_SIGNAL,
                _acquire_intent(
                    gap={
                        "detected_by": GAP_DETECTED_BY_COMPLETENESS_AUDIT,
                        "signal": SIGNAL_RETRY_WAIT_TAIL,
                        "evidence_ref": _evidence(missing_shard_ids=[], retry_wait_url_count=3),
                    }
                ),
            ),
            (
                VALIDATOR_V_ATTEMPT,
                _acquire_intent(
                    attempt={
                        "count": ATTEMPT_MAX_CEILING,
                        "max": ATTEMPT_MAX_CEILING,
                        "backoff_owner": BACKOFF_OWNER_RECOVERY_TICK_POLL,
                        "terminal_status": None,
                    }
                ),
            ),
        ]
        for validator_id, payload in cases:
            with self.subTest(validator=validator_id):
                result = validate_compensation_intent(payload)
                self.assertFalse(result["valid"])
                self.assertIn(validator_id, [failure["validator_id"] for failure in result["failures"]])
                self.assertIsNone(result["durable_key"])

    def test_v_stage_is_reachable_when_the_seat_table_is_widened(self) -> None:
        # V_STAGE cannot fire through strict parse (promote is rejected earlier),
        # so drive it directly: the battery row exists and is not vestigial.
        self.assertIn(VALIDATOR_V_STAGE, VALIDATOR_IDS)
        self.assertIsNotNone(validate_v_stage(stage=STAGE_PROMOTE))

    def test_a_custom_seat_table_scopes_the_admissible_owners(self) -> None:
        fetch_only = tuple(seat for seat in DEFAULT_COMPENSATION_SEATS if seat.stage == STAGE_FETCH)
        result = validate_compensation_intent(_acquire_intent(), seats=fetch_only)
        self.assertFalse(result["valid"])
        self.assertIn(VALIDATOR_V_SEAT, [failure["validator_id"] for failure in result["failures"]])
        self.assertTrue(validate_compensation_intent(_fetch_intent(), seats=fetch_only)["valid"])


if __name__ == "__main__":
    unittest.main()
