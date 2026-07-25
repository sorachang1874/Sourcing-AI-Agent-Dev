"""Compensation contract `sourcing.pipeline.compensation_intent.v1` (WS7/W7.4 S1).

Spec: docs/WS7_COMPENSATION_DESIGN.md §2 (schema + the three key design rules),
§4 (recovery-of-recovery ladder + terminal statuses), §5 (paid-dispatch safety).
OQ1-OQ8 RATIFIED 2026-07-24; slice S1 is ADDITIVE ONLY — this module is
standalone (no orchestrator, no store, no durable-runtime import, no dispatch
surface) and everything here is a pure function. NOTHING here is wired into
``run_worker_recovery_once`` / ``recovery_phases`` / any owner drain: that is S3,
and it is gated on a review GO.

Unlike 议案① (divider) and 议案③ (promote), compensation is **not** an AI-native
decision: there is no ``ModelClient`` method, no model call, and no
model-authored field. Every field of a compensation intent is caller-authored
fact composed from signals the pipeline stages ALREADY self-report (OQ2).

THE STRUCTURAL PROPERTY THIS MODULE EXISTS TO ENFORCE (OQ5, the R-019 hard
constraint — RESIDUAL_LEDGER.md:38, status `pending remediation`, call-site
ceiling 24): **a compensation intent can never mint a new dispatch identity.**
R-019's remediation clause blocks the next touch of operation
retry/dispatch/command completion, any new ``_connect_with_transaction_lock``
caller, **or any live/W6/manual operation/acquisition-control signoff** — so
compensation may ride ONLY the recovery tick's existing idempotent command-owner
replays. Three allowlists make that structural rather than conventional:

* ``target_command_owner`` must name a seat in :data:`DEFAULT_COMPENSATION_SEATS`
  — a recovery-tick phase that already exists in the characterized phase
  sequence (tests/test_recovery_tick_characterization.py) and already drains a
  typed command owner. A novel owner name is rejected; there is no way to point
  an intent at a phase the tick does not already run.
* ``owner_idempotency_key`` must live in that seat's OWN command-type namespace
  (``"<command_type>"`` or ``"<command_type>:…"`` — the grammar every owner
  actually uses: ``acquisition_command_owner.py:715/784/855/1869/1956``,
  ``profile_fetch_owner.py:314``, ``durable_runtime.py:2309/2531``). A key the
  compensation layer could only have authored itself (a ``compensation*``
  namespace, the schema id, the audit phase name, or a key embedding the
  intent's own ``intent_id``) is rejected outright. A replay under an owner's own
  key hits that owner's existing ``already_succeeded`` short-circuit and pays
  nothing (§5 rule 1).
* There is NO schema slot anywhere — top level, ``sub_unit``, ``gap``,
  ``compensation_action`` — for a provider payload, a request body, a command
  payload, a budget, or a dispatch target. The strict key allowlists reject any
  attempt to inject one. **The intent carries a pointer, never a payload; it has
  no payment authority of its own** (§2.1 rule 1).

Other structural fail-closed properties (mirroring the two proven precedents,
``profile_batch_division_contract.py`` and ``organization_promote_contract.py``):

* EXACT-VERSION LOOKUP. Payloads are accepted only by exact ``schema_id`` match
  in ``_SCHEMA_NORMALIZERS``. An unknown or missing ``schema_id`` — including a
  future ``...v2`` — is rejected outright; there is NO auto-upgrade and no
  repair path. A repaired intent would launder an unauditable gap record into a
  paid re-dispatch.
* STAGE SCOPE IS RATIFIED, NOT INFERRED (OQ6). ``stage`` accepts only
  ``acquire``/``fetch``/``materialize``. ``promote`` is rejected with an explicit
  reason: promote has no dedicated recovery-tick command-owner seat to attach an
  intent to, and its judgment shape is mid-flip under 议案③ (S5 not landed). See
  D-C2 / D-C6.
* THE ATTEMPT LADDER IS BOUNDED AND CANNOT FAIL OPEN (OQ7). ``attempt.max`` may
  never exceed :data:`ATTEMPT_MAX_CEILING` (3), and an intent whose
  ``attempt.count`` has reached ``attempt.max`` MUST already carry the terminal
  status ``compensation_exhausted_needs_human``. A spent intent with a null
  terminal status is a schema violation, so "silently re-loop forever" is not
  expressible.

Anchors were re-verified against this tree on 2026-07-24; the design's own line
spans had drifted (D-C5). Symbol anchors are used here in preference to line
numbers wherever possible.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable

SCHEMA_ID_V1 = "sourcing.pipeline.compensation_intent.v1"

# ---------------------------------------------------------------------------
# Stage scope (design §0 table + §1; OQ6).
# ---------------------------------------------------------------------------

STAGE_ACQUIRE = "acquire"
STAGE_FETCH = "fetch"
STAGE_MATERIALIZE = "materialize"
COMPENSABLE_STAGE_VALUES = frozenset({STAGE_ACQUIRE, STAGE_FETCH, STAGE_MATERIALIZE})

# Named ONLY so the OQ6 scope cut can be reported as an explicit, auditable
# rejection instead of an anonymous "unknown enum value". `promote` is a real
# pipeline stage that is deliberately NOT compensable yet.
STAGE_PROMOTE = "promote"
DEFERRED_STAGE_VALUES = frozenset({STAGE_PROMOTE})
DEFERRED_STAGE_REASONS: dict[str, str] = {
    STAGE_PROMOTE: (
        "stage 'promote' is scoped OUT of compensation (OQ6 RATIFIED 2026-07-24): promote has no "
        "dedicated recovery-tick command-owner seat to attach an intent to (zero PROMOTE owner "
        "constant in durable_runtime, zero promote binding in recovery_drain_registry, zero promote "
        "row in the characterized phase sequence), and its judgment shape is mid-flip under 议案③ "
        "(S5 not landed). Until 议案③ S5 + a promote command-owner seat land, the existing "
        "keep-incumbent fail-closed IS the promote compensation"
    ),
}

# ---------------------------------------------------------------------------
# Sub-unit kinds — the delta anchor's shape per stage (design §2.1 `sub_unit`).
# ---------------------------------------------------------------------------

SUB_UNIT_KIND_ACQUISITION_SHARD = "acquisition_shard"
SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM = "profile_registry_item"
SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT = "snapshot_durable_unit"
SUB_UNIT_KIND_VALUES = frozenset(
    {
        SUB_UNIT_KIND_ACQUISITION_SHARD,
        SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM,
        SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT,
    }
)
_SUB_UNIT_KINDS_BY_STAGE: dict[str, frozenset[str]] = {
    STAGE_ACQUIRE: frozenset({SUB_UNIT_KIND_ACQUISITION_SHARD}),
    STAGE_FETCH: frozenset({SUB_UNIT_KIND_PROFILE_REGISTRY_ITEM}),
    # A materialize gap is either a snapshot/board durable unit or a per-shard
    # roster hole surfaced by the SAME resolve_segmented_roster_completion
    # contract the acquire stage uses (snapshot_materializer.py routes through
    # it), so the shard kind is legal here too.
    STAGE_MATERIALIZE: frozenset({SUB_UNIT_KIND_SNAPSHOT_DURABLE_UNIT, SUB_UNIT_KIND_ACQUISITION_SHARD}),
}

# ---------------------------------------------------------------------------
# Gap detection (design §2.1 `gap`; OQ2 — owner self-report composed by ONE
# audit, never a rival reconciler).
# ---------------------------------------------------------------------------

GAP_DETECTED_BY_COMPLETENESS_AUDIT = "completeness_audit"
GAP_DETECTED_BY_VALUES = frozenset({GAP_DETECTED_BY_COMPLETENESS_AUDIT})

SIGNAL_ROSTER_PARTIAL = "roster_partial"
SIGNAL_ROSTER_TRUNCATED = "roster_truncated"
SIGNAL_RETRY_WAIT_TAIL = "retry_wait_tail"
SIGNAL_MANIFEST_PARTIAL = "manifest_partial"
SIGNAL_BOARD_VISIBLE_BACKLOG = "board_visible_backlog"
GAP_SIGNAL_VALUES = frozenset(
    {
        SIGNAL_ROSTER_PARTIAL,
        SIGNAL_ROSTER_TRUNCATED,
        SIGNAL_RETRY_WAIT_TAIL,
        SIGNAL_MANIFEST_PARTIAL,
        SIGNAL_BOARD_VISIBLE_BACKLOG,
    }
)
# Which self-reported signal belongs to which stage. `roster_partial` /
# `roster_truncated` come from resolve_segmented_roster_completion
# (company_shard_planning.py `resolve_segmented_roster_completion`), which BOTH
# the direct segmented acquire fetch and the background snapshot reconcile route
# through — hence both stages.
_SIGNALS_BY_STAGE: dict[str, frozenset[str]] = {
    STAGE_ACQUIRE: frozenset({SIGNAL_ROSTER_PARTIAL, SIGNAL_ROSTER_TRUNCATED}),
    STAGE_FETCH: frozenset({SIGNAL_RETRY_WAIT_TAIL}),
    STAGE_MATERIALIZE: frozenset(
        {SIGNAL_MANIFEST_PARTIAL, SIGNAL_BOARD_VISIBLE_BACKLOG, SIGNAL_ROSTER_PARTIAL, SIGNAL_ROSTER_TRUNCATED}
    ),
}
# The evidence slots each signal MAY populate; at least one must be non-empty.
# Evidence is a pointer to the stage's own self-reported signal, never a copy of
# provider data (§2.1). A partial snapshot manifest reports whichever of the two
# shard-hole faces `resolve_segmented_roster_completion` produced.
_REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL: dict[str, tuple[str, ...]] = {
    SIGNAL_ROSTER_PARTIAL: ("missing_shard_ids",),
    SIGNAL_ROSTER_TRUNCATED: ("truncated_shard_ids",),
    SIGNAL_RETRY_WAIT_TAIL: ("retry_wait_url_count",),
    SIGNAL_MANIFEST_PARTIAL: ("missing_shard_ids", "truncated_shard_ids"),
    SIGNAL_BOARD_VISIBLE_BACKLOG: ("backlog_item_count",),
}

# ---------------------------------------------------------------------------
# Attempt ladder (design §4; OQ7).
# ---------------------------------------------------------------------------

ATTEMPT_MAX_CEILING = 3
BACKOFF_OWNER_RECOVERY_TICK_POLL = "recovery_tick_poll"
BACKOFF_OWNER_VALUES = frozenset({BACKOFF_OWNER_RECOVERY_TICK_POLL})

TERMINAL_STATUS_COMPENSATED = "compensated"
TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN = "compensation_exhausted_needs_human"
TERMINAL_STATUS_SUPERSEDED = "superseded"
TERMINAL_STATUS_VALUES = frozenset(
    {TERMINAL_STATUS_COMPENSATED, TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN, TERMINAL_STATUS_SUPERSEDED}
)

# The owner-replay outcomes the §4 ladder maps to a terminal status. These are
# observations about an EXISTING owner's replay, not new dispatch verbs.
REPLAY_OUTCOME_ALREADY_SUCCEEDED = "already_succeeded"
REPLAY_OUTCOME_COMPENSATED = "compensated"
REPLAY_OUTCOME_TRANSIENT_FAILURE = "transient_failure"
REPLAY_OUTCOME_GAP_ABSENT = "gap_absent"
REPLAY_OUTCOME_VALUES = frozenset(
    {
        REPLAY_OUTCOME_ALREADY_SUCCEEDED,
        REPLAY_OUTCOME_COMPENSATED,
        REPLAY_OUTCOME_TRANSIENT_FAILURE,
        REPLAY_OUTCOME_GAP_ABSENT,
    }
)

# ---------------------------------------------------------------------------
# Provenance (design §2.1 `provenance`).
# ---------------------------------------------------------------------------

CREATED_BY_PHASE_COMPENSATION_AUDIT = "compensation_completeness_audit"
CREATED_BY_PHASE_VALUES = frozenset({CREATED_BY_PHASE_COMPENSATION_AUDIT})

MAX_INTENT_ID_LENGTH = 64
MAX_SUB_UNIT_ID_LENGTH = 512
MAX_IDEMPOTENCY_KEY_LENGTH = 512
MAX_ESCALATION_REASON_LENGTH = 500

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_INTENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$")

# Namespaces a compensation-authored key would live in. An `owner_idempotency_key`
# matching any of these was minted by the compensation layer, not inherited from
# an owner — exactly the new dispatch identity OQ5/R-019 forbids.
_COMPENSATION_AUTHORED_KEY_PREFIXES = (
    "compensation",
    "compensation_intent",
    "sourcing.pipeline.compensation",
    CREATED_BY_PHASE_COMPENSATION_AUDIT,
    "补",
)


# ---------------------------------------------------------------------------
# The compensation seat table — the OQ1/OQ5 fence.
#
# Every row is a recovery-tick phase that ALREADY exists in the characterized
# phase sequence (tests/test_recovery_tick_characterization.py
# CHARACTERIZED_PHASE_SEQUENCE) and already drains a typed command owner with its
# own deterministic idempotency key. Nine of these rows are literal
# `recovery_drain_registry.DEFAULT_RECOVERY_DRAIN_BINDINGS` bindings (the suite
# cross-checks phase+owner against the registry so the seat table cannot drift);
# the rest are the tick's literal named phases.
#
# Standalone by design, mirroring the divider/promote precedents: importing the
# drain registry would pull `durable_runtime` into a pure contract module. The
# offline suite does the cross-check instead.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompensationSeat:
    """One compensable recovery-tick seat: WHERE a top-up may be routed, and the
    command-type namespace its idempotency key must come from.

    ``inherits_root_operation_key`` marks the one owner whose key is not
    prefixed with its command type: ``acquisition.run.create`` inherits the ROOT
    operation's own ``idempotency_key`` as a locked identity field
    (acquisition_command_owner.py ``_acquisition_root_locked_identity``). Such a
    key is opaque, so the fence degrades to "non-empty and provably not
    compensation-authored" for that seat only.
    """

    phase: str
    owner_label: str
    stage: str
    command_types: tuple[str, ...]
    inherits_root_operation_key: bool = False
    registry_bound: bool = True


DEFAULT_COMPENSATION_SEATS: tuple[CompensationSeat, ...] = (
    # --- 补 acquire: the 7 acquisition command owners + native discovery ------
    CompensationSeat(
        phase="acquisition_run_create_command_owner",
        owner_label="acquisition_run_writer",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.run.create",),
        inherits_root_operation_key=True,
    ),
    CompensationSeat(
        phase="acquisition_intent_resolve_command_owner",
        owner_label="acquisition_planner",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.intent.resolve",),
    ),
    CompensationSeat(
        phase="acquisition_plan_build_command_owner",
        owner_label="acquisition_planner",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.plan.build",),
    ),
    CompensationSeat(
        phase="acquisition_plan_review_request_command_owner",
        owner_label="acquisition_planner",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.plan_review.request",),
    ),
    CompensationSeat(
        phase="acquisition_plan_commit_command_owner",
        owner_label="acquisition_planner",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.plan.commit",),
    ),
    CompensationSeat(
        phase="acquisition_probe_command_owner",
        owner_label="acquisition_probe_owner",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.probe.submit", "acquisition.probe.collect"),
    ),
    CompensationSeat(
        phase="acquisition_scale_plan_command_owner",
        owner_label="acquisition_scale_planner",
        stage=STAGE_ACQUIRE,
        command_types=("acquisition.scale.plan",),
    ),
    CompensationSeat(
        phase="operation_native_discovery_activity_owner",
        owner_label="linkedin_acquisition_owner",
        stage=STAGE_ACQUIRE,
        command_types=("linkedin.discovery_query.run",),
    ),
    # --- 补 fetch: the refill/terminal-record command owners ------------------
    CompensationSeat(
        phase="profile_refill_command_owner",
        owner_label="linkedin_profile_refill_command_owner",
        stage=STAGE_FETCH,
        command_types=("linkedin.profile_refill.submit_batch",),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="profile_url_terminal_record_command_owner",
        owner_label="linkedin_profile_url_terminal_record_command_owner",
        stage=STAGE_FETCH,
        command_types=("linkedin.profile_url_terminal.record",),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="operation_native_profile_fetch_activity_owner",
        owner_label="linkedin_profile_activity_owner",
        stage=STAGE_FETCH,
        command_types=(
            "linkedin.profile_fetch.activity.run",
            "linkedin.profile_fetch.provider.fetch",
            "linkedin.profile_terminal.admit",
        ),
    ),
    # --- 补 materialize: the one-durable-unit-per-tick drains ------------------
    CompensationSeat(
        phase="local_apply_backlog",
        owner_label="profile_local_apply_command_owner",
        stage=STAGE_MATERIALIZE,
        command_types=("linkedin.local_profile_delta.apply",),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="event_level_materialization_followup",
        owner_label="event_level_local_apply_to_board_visible",
        stage=STAGE_MATERIALIZE,
        command_types=("linkedin.local_profile_delta.apply", "projection.board_visible_patch.publish"),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="profile_refill_event_level_materialization_followup",
        owner_label="event_level_local_apply_to_board_visible",
        stage=STAGE_MATERIALIZE,
        command_types=("linkedin.local_profile_delta.apply", "projection.board_visible_patch.publish"),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="board_visible_apply",
        owner_label="board_visible_projection_owner",
        stage=STAGE_MATERIALIZE,
        command_types=("projection.board_visible_patch.publish",),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="snapshot_full_materialization",
        owner_label="snapshot_full_materialization_queue",
        stage=STAGE_MATERIALIZE,
        command_types=("snapshot.compaction.run",),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="collection_authoritative_merge",
        owner_label="collection_writer_owner",
        stage=STAGE_MATERIALIZE,
        command_types=("collection.authoritative.merge",),
        registry_bound=False,
    ),
    CompensationSeat(
        phase="legacy_materialization_adapter",
        owner_label="durable_runtime_migration_adapter",
        stage=STAGE_MATERIALIZE,
        # The adapter bridges legacy items onto typed commands
        # (orchestrator._LEGACY_MATERIALIZATION_ADAPTER_ITEM_COMMAND_TYPES), so
        # its admissible key namespaces are exactly that map's values.
        command_types=(
            "linkedin.local_profile_delta.apply",
            "projection.board_visible_patch.publish",
            "projection.person_search_index.build",
            "collection.authoritative.merge",
            "linkedin.discovery_query.run",
            "projection.facet_layering.build",
            "snapshot.compaction.run",
        ),
        registry_bound=False,
    ),
)


def build_compensation_seat_index(
    seats: Iterable[CompensationSeat] = DEFAULT_COMPENSATION_SEATS,
) -> dict[str, CompensationSeat]:
    """Index seats by phase, failing loudly on a duplicate or malformed row."""
    index: dict[str, CompensationSeat] = {}
    for seat in seats:
        if not seat.phase or not seat.owner_label or not seat.command_types:
            raise CompensationContractError(f"malformed compensation seat: {seat!r}")
        if seat.stage not in COMPENSABLE_STAGE_VALUES:
            raise CompensationContractError(
                f"compensation seat {seat.phase!r} declares non-compensable stage {seat.stage!r}"
            )
        if seat.phase in index:
            raise CompensationContractError(f"duplicate compensation seat phase {seat.phase!r}")
        index[seat.phase] = seat
    return index


# ---------------------------------------------------------------------------
# Validator ids (design §2 rules + §4/§5 invariants).
# ---------------------------------------------------------------------------

VALIDATOR_V_STAGE = "V_STAGE_compensable"
VALIDATOR_V_SEAT = "V_SEAT_existing_owner"
VALIDATOR_V_KEY = "V_KEY_owner_derived"
VALIDATOR_V_DELTA = "V_DELTA_only_anchor"
VALIDATOR_V_LINEAGE = "V_LINEAGE_scoped"
VALIDATOR_V_SIGNAL = "V_SIGNAL_self_reported"
VALIDATOR_V_ATTEMPT = "V_ATTEMPT_bounded"
VALIDATOR_IDS = (
    VALIDATOR_V_STAGE,
    VALIDATOR_V_SEAT,
    VALIDATOR_V_KEY,
    VALIDATOR_V_DELTA,
    VALIDATOR_V_LINEAGE,
    VALIDATOR_V_SIGNAL,
    VALIDATOR_V_ATTEMPT,
)

# failures[].validator_id for strict-parse rejections, distinct from every
# battery id so callers can tell a malformed record from a rejected one.
SCHEMA_FAILURE_VALIDATOR_ID = "schema"

VALIDATOR_RESULT_STATUS_PASS = "pass"
VALIDATOR_RESULT_STATUS_FAIL = "fail"
VALIDATOR_RESULT_STATUS_SKIPPED = "skipped"
VALIDATOR_RESULT_STATUS_VALUES = frozenset(
    {VALIDATOR_RESULT_STATUS_PASS, VALIDATOR_RESULT_STATUS_FAIL, VALIDATOR_RESULT_STATUS_SKIPPED}
)

# ---------------------------------------------------------------------------
# Strict key allowlists. NOTE what is absent at EVERY level: any provider
# payload, request body, command payload, url list, budget, cost, or dispatch
# target. The intent is a pointer; it structurally cannot carry a paid payload
# (§2.1 rule 1 / §5).
# ---------------------------------------------------------------------------

_TOP_LEVEL_KEYS = frozenset(
    {"schema_id", "intent_id", "stage", "sub_unit", "gap", "compensation_action", "attempt", "provenance"}
)
_SUB_UNIT_KEYS = frozenset({"kind", "sub_unit_id", "source_lineage"})
_SOURCE_LINEAGE_REQUIRED_KEYS = frozenset({"operation_id", "job_id"})
_SOURCE_LINEAGE_OPTIONAL_KEYS = frozenset({"materialization_generation_key", "refill_plan_division_id"})
_SOURCE_LINEAGE_KEYS = _SOURCE_LINEAGE_REQUIRED_KEYS | _SOURCE_LINEAGE_OPTIONAL_KEYS
_GAP_KEYS = frozenset({"detected_by", "signal", "evidence_ref"})
_EVIDENCE_REF_KEYS = frozenset(
    {"missing_shard_ids", "truncated_shard_ids", "retry_wait_url_count", "backlog_item_count"}
)
_COMPENSATION_ACTION_KEYS = frozenset({"target_command_owner", "owner_idempotency_key", "delta_only"})
_ATTEMPT_KEYS = frozenset({"count", "max", "backoff_owner", "terminal_status"})
_PROVENANCE_KEYS = frozenset({"created_at", "created_by_phase", "input_snapshot_sha256"})
_ESCALATION_KEYS = frozenset(
    {
        "schema_id",
        "intent_id",
        "stage",
        "sub_unit_id",
        "source_lineage",
        "target_command_owner",
        "terminal_status",
        "attempt_count",
        "attempt_max",
        "escalation_reason",
        "needs_human",
        "board_visible",
    }
)


class CompensationContractError(ValueError):
    """Fail-closed strict-parse error for the compensation-intent contract."""


# ---------------------------------------------------------------------------
# Normalized dataclasses.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceLineage:
    """The lineage a gap belongs to — the "never cross a lineage" anchor (§2.1
    rule 2). ``refill_plan_division_id`` is the 议案① divider's registry field
    (migration 0015) and is FETCH-ONLY; it is nullable because 议案① S5 has not
    flipped, so today it is written only for validated shadow proposals (D-C1 —
    compensation reads it defensively and falls back to per-URL item identity)."""

    operation_id: str
    job_id: str
    materialization_generation_key: str = ""
    refill_plan_division_id: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "job_id": self.job_id,
            "materialization_generation_key": self.materialization_generation_key,
            "refill_plan_division_id": self.refill_plan_division_id,
        }


@dataclass(frozen=True)
class CompensationSubUnit:
    """WHICH missing atom — the delta-only anchor (§2.1 rule 2)."""

    kind: str
    sub_unit_id: str
    source_lineage: SourceLineage

    def to_payload(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "sub_unit_id": self.sub_unit_id,
            "source_lineage": self.source_lineage.to_payload(),
        }


@dataclass(frozen=True)
class GapEvidenceRef:
    """A POINTER to the stage's self-reported signal, never a copy of provider
    data. ``backlog_item_count`` is the evidence slot for
    ``board_visible_backlog`` (design §2.1 defines the signal but enumerates no
    slot for it — recorded as D-C5)."""

    missing_shard_ids: tuple[str, ...] = ()
    truncated_shard_ids: tuple[str, ...] = ()
    retry_wait_url_count: int = 0
    backlog_item_count: int = 0

    def to_payload(self) -> dict[str, Any]:
        return {
            "missing_shard_ids": list(self.missing_shard_ids),
            "truncated_shard_ids": list(self.truncated_shard_ids),
            "retry_wait_url_count": self.retry_wait_url_count,
            "backlog_item_count": self.backlog_item_count,
        }

    def is_empty(self) -> bool:
        return not (
            self.missing_shard_ids or self.truncated_shard_ids or self.retry_wait_url_count or self.backlog_item_count
        )


@dataclass(frozen=True)
class CompensationGap:
    """The detected gap (§2.1 `gap`)."""

    detected_by: str
    signal: str
    evidence_ref: GapEvidenceRef

    def to_payload(self) -> dict[str, Any]:
        return {
            "detected_by": self.detected_by,
            "signal": self.signal,
            "evidence_ref": self.evidence_ref.to_payload(),
        }


@dataclass(frozen=True)
class CompensationAction:
    """A POINTER to an existing command owner + that owner's OWN idempotency key
    (§2.1 rule 3). There is deliberately no payload slot: the action cannot pay."""

    target_command_owner: str
    owner_idempotency_key: str
    delta_only: bool

    def to_payload(self) -> dict[str, Any]:
        return {
            "target_command_owner": self.target_command_owner,
            "owner_idempotency_key": self.owner_idempotency_key,
            "delta_only": self.delta_only,
        }


@dataclass(frozen=True)
class CompensationAttempt:
    """Recovery-of-recovery accounting (§4, OQ7)."""

    count: int
    max: int
    backoff_owner: str
    terminal_status: str | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "max": self.max,
            "backoff_owner": self.backoff_owner,
            "terminal_status": self.terminal_status,
        }

    def is_spent(self) -> bool:
        return self.count >= self.max


@dataclass(frozen=True)
class CompensationProvenance:
    """Who minted the intent and from which audit snapshot (§2.1 `provenance`)."""

    created_at: str
    created_by_phase: str
    input_snapshot_sha256: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at,
            "created_by_phase": self.created_by_phase,
            "input_snapshot_sha256": self.input_snapshot_sha256,
        }


@dataclass(frozen=True)
class CompensationIntent:
    """The normalized `compensation_intent.v1` record (design §2.1)."""

    schema_id: str
    intent_id: str
    stage: str
    sub_unit: CompensationSubUnit
    gap: CompensationGap
    compensation_action: CompensationAction
    attempt: CompensationAttempt
    provenance: CompensationProvenance

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "intent_id": self.intent_id,
            "stage": self.stage,
            "sub_unit": self.sub_unit.to_payload(),
            "gap": self.gap.to_payload(),
            "compensation_action": self.compensation_action.to_payload(),
            "attempt": self.attempt.to_payload(),
            "provenance": self.provenance.to_payload(),
        }

    def durable_key(self) -> tuple[str, str, str, str]:
        """The OQ3 durable key ``(stage, sub_unit_id, source_lineage)``, flattened
        to the lineage's two identity fields so it is hashable/comparable."""
        return (
            self.stage,
            self.sub_unit.sub_unit_id,
            self.sub_unit.source_lineage.operation_id,
            self.sub_unit.source_lineage.job_id,
        )


# ---------------------------------------------------------------------------
# Strict, fail-closed normalization primitives (mirror the two precedents).
# ---------------------------------------------------------------------------


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CompensationContractError(f"{label} must be an object, got {type(value).__name__}")
    return value


def _require_allowed_keys(payload: Mapping[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = sorted(str(key) for key in payload.keys() if str(key) not in allowed)
    if unknown:
        raise CompensationContractError(f"{label} carries unknown keys {unknown} (strict allowlist)")


def _require_keys(payload: Mapping[str, Any], required: Iterable[str], label: str) -> None:
    missing = sorted(key for key in required if key not in payload)
    if missing:
        raise CompensationContractError(f"{label} is missing required keys {missing}")


def _require_str(value: Any, label: str, *, max_length: int | None = None) -> str:
    if not isinstance(value, str):
        raise CompensationContractError(f"{label} must be a string, got {type(value).__name__}")
    if max_length is not None and len(value) > max_length:
        raise CompensationContractError(f"{label} exceeds {max_length} chars (got {len(value)})")
    return value


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise CompensationContractError(f"{label} must be a boolean, got {type(value).__name__}")
    return value


def _require_int(value: Any, label: str, *, minimum: int | None = None, maximum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CompensationContractError(f"{label} must be an integer, got {type(value).__name__}")
    if minimum is not None and value < minimum:
        raise CompensationContractError(f"{label} must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise CompensationContractError(f"{label} must be <= {maximum}, got {value}")
    return value


def _require_str_list(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise CompensationContractError(f"{label} must be a list of strings")
    return tuple(_require_str(item, f"{label}[{position}]") for position, item in enumerate(value))


def _require_sha256(value: Any, label: str) -> str:
    text = _require_str(value, label)
    if not _SHA256_HEX_RE.fullmatch(text):
        raise CompensationContractError(f"{label} must be 64 lowercase hex chars")
    return text


def _normalize_source_lineage(value: Any, label: str) -> SourceLineage:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _SOURCE_LINEAGE_KEYS, label)
    _require_keys(payload, _SOURCE_LINEAGE_REQUIRED_KEYS, label)
    return SourceLineage(
        operation_id=_require_str(payload["operation_id"], f"{label}.operation_id"),
        job_id=_require_str(payload["job_id"], f"{label}.job_id"),
        materialization_generation_key=_require_str(
            payload.get("materialization_generation_key", ""), f"{label}.materialization_generation_key"
        ),
        refill_plan_division_id=_require_str(
            payload.get("refill_plan_division_id", ""), f"{label}.refill_plan_division_id"
        ),
    )


def _normalize_sub_unit(value: Any, label: str) -> CompensationSubUnit:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _SUB_UNIT_KEYS, label)
    _require_keys(payload, _SUB_UNIT_KEYS, label)
    kind = _require_str(payload["kind"], f"{label}.kind")
    if kind not in SUB_UNIT_KIND_VALUES:
        raise CompensationContractError(f"{label}.kind must be one of {sorted(SUB_UNIT_KIND_VALUES)}, got {kind!r}")
    sub_unit_id = _require_str(payload["sub_unit_id"], f"{label}.sub_unit_id", max_length=MAX_SUB_UNIT_ID_LENGTH)
    if not sub_unit_id.strip():
        raise CompensationContractError(f"{label}.sub_unit_id must be non-empty (it IS the delta anchor)")
    return CompensationSubUnit(
        kind=kind,
        sub_unit_id=sub_unit_id,
        source_lineage=_normalize_source_lineage(payload["source_lineage"], f"{label}.source_lineage"),
    )


def _normalize_evidence_ref(value: Any, label: str) -> GapEvidenceRef:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _EVIDENCE_REF_KEYS, label)
    return GapEvidenceRef(
        missing_shard_ids=_require_str_list(payload.get("missing_shard_ids", []), f"{label}.missing_shard_ids"),
        truncated_shard_ids=_require_str_list(payload.get("truncated_shard_ids", []), f"{label}.truncated_shard_ids"),
        retry_wait_url_count=_require_int(
            payload.get("retry_wait_url_count", 0), f"{label}.retry_wait_url_count", minimum=0
        ),
        backlog_item_count=_require_int(payload.get("backlog_item_count", 0), f"{label}.backlog_item_count", minimum=0),
    )


def _normalize_gap(value: Any, label: str) -> CompensationGap:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _GAP_KEYS, label)
    _require_keys(payload, _GAP_KEYS, label)
    detected_by = _require_str(payload["detected_by"], f"{label}.detected_by")
    if detected_by not in GAP_DETECTED_BY_VALUES:
        raise CompensationContractError(
            f"{label}.detected_by must be one of {sorted(GAP_DETECTED_BY_VALUES)}, got {detected_by!r} "
            "(OQ2: a single completeness audit composes owner self-reports; never a rival reconciler)"
        )
    signal = _require_str(payload["signal"], f"{label}.signal")
    if signal not in GAP_SIGNAL_VALUES:
        raise CompensationContractError(f"{label}.signal must be one of {sorted(GAP_SIGNAL_VALUES)}, got {signal!r}")
    return CompensationGap(
        detected_by=detected_by,
        signal=signal,
        evidence_ref=_normalize_evidence_ref(payload["evidence_ref"], f"{label}.evidence_ref"),
    )


def _normalize_compensation_action(value: Any, label: str) -> CompensationAction:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _COMPENSATION_ACTION_KEYS, label)
    _require_keys(payload, _COMPENSATION_ACTION_KEYS, label)
    return CompensationAction(
        target_command_owner=_require_str(payload["target_command_owner"], f"{label}.target_command_owner"),
        owner_idempotency_key=_require_str(
            payload["owner_idempotency_key"], f"{label}.owner_idempotency_key", max_length=MAX_IDEMPOTENCY_KEY_LENGTH
        ),
        delta_only=_require_bool(payload["delta_only"], f"{label}.delta_only"),
    )


def _normalize_attempt(value: Any, label: str) -> CompensationAttempt:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _ATTEMPT_KEYS, label)
    _require_keys(payload, _ATTEMPT_KEYS, label)
    raw_terminal = payload["terminal_status"]
    terminal_status: str | None
    if raw_terminal is None:
        terminal_status = None
    else:
        terminal_status = _require_str(raw_terminal, f"{label}.terminal_status")
        if terminal_status not in TERMINAL_STATUS_VALUES:
            raise CompensationContractError(
                f"{label}.terminal_status must be null or one of {sorted(TERMINAL_STATUS_VALUES)}, "
                f"got {terminal_status!r}"
            )
    backoff_owner = _require_str(payload["backoff_owner"], f"{label}.backoff_owner")
    if backoff_owner not in BACKOFF_OWNER_VALUES:
        raise CompensationContractError(
            f"{label}.backoff_owner must be one of {sorted(BACKOFF_OWNER_VALUES)}, got {backoff_owner!r} "
            "(OQ7: backoff is the recovery tick's own poll cadence, never a busy loop)"
        )
    return CompensationAttempt(
        count=_require_int(payload["count"], f"{label}.count", minimum=0, maximum=ATTEMPT_MAX_CEILING),
        max=_require_int(payload["max"], f"{label}.max", minimum=1, maximum=ATTEMPT_MAX_CEILING),
        backoff_owner=backoff_owner,
        terminal_status=terminal_status,
    )


def _normalize_provenance(value: Any, label: str) -> CompensationProvenance:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _PROVENANCE_KEYS, label)
    _require_keys(payload, _PROVENANCE_KEYS, label)
    created_at = _require_str(payload["created_at"], f"{label}.created_at")
    if not _TIMESTAMP_RE.fullmatch(created_at):
        raise CompensationContractError(f"{label}.created_at must be an ISO-8601 UTC-offset timestamp")
    created_by_phase = _require_str(payload["created_by_phase"], f"{label}.created_by_phase")
    if created_by_phase not in CREATED_BY_PHASE_VALUES:
        raise CompensationContractError(
            f"{label}.created_by_phase must be one of {sorted(CREATED_BY_PHASE_VALUES)}, got {created_by_phase!r} "
            "(OQ4: the single additive audit phase is the only minter)"
        )
    return CompensationProvenance(
        created_at=created_at,
        created_by_phase=created_by_phase,
        input_snapshot_sha256=_require_sha256(payload["input_snapshot_sha256"], f"{label}.input_snapshot_sha256"),
    )


def _normalize_v1(payload: Mapping[str, Any]) -> CompensationIntent:
    _require_allowed_keys(payload, _TOP_LEVEL_KEYS, "intent")
    _require_keys(payload, _TOP_LEVEL_KEYS, "intent")
    intent_id = _require_str(payload["intent_id"], "intent_id", max_length=MAX_INTENT_ID_LENGTH)
    if not _INTENT_ID_RE.fullmatch(intent_id):
        raise CompensationContractError("intent_id must be 1-64 chars of [A-Za-z0-9_-] (ULID-compatible audit id)")
    stage = _require_str(payload["stage"], "stage")
    if stage in DEFERRED_STAGE_VALUES:
        raise CompensationContractError(DEFERRED_STAGE_REASONS[stage])
    if stage not in COMPENSABLE_STAGE_VALUES:
        raise CompensationContractError(f"stage must be one of {sorted(COMPENSABLE_STAGE_VALUES)}, got {stage!r}")
    return CompensationIntent(
        schema_id=SCHEMA_ID_V1,
        intent_id=intent_id,
        stage=stage,
        sub_unit=_normalize_sub_unit(payload["sub_unit"], "sub_unit"),
        gap=_normalize_gap(payload["gap"], "gap"),
        compensation_action=_normalize_compensation_action(payload["compensation_action"], "compensation_action"),
        attempt=_normalize_attempt(payload["attempt"], "attempt"),
        provenance=_normalize_provenance(payload["provenance"], "provenance"),
    )


_SCHEMA_NORMALIZERS: dict[str, Callable[[Mapping[str, Any]], CompensationIntent]] = {
    SCHEMA_ID_V1: _normalize_v1,
}


def normalize_compensation_intent(payload: Any) -> CompensationIntent:
    """Strict, fail-closed normalization by EXACT schema-id lookup (no upgrade)."""
    mapping = _require_mapping(payload, "intent payload")
    schema_id = mapping.get("schema_id")
    normalizer = _SCHEMA_NORMALIZERS.get(schema_id) if isinstance(schema_id, str) else None
    if normalizer is None:
        raise CompensationContractError(
            f"unknown schema_id {schema_id!r}: exact-version lookup accepts only "
            f"{sorted(_SCHEMA_NORMALIZERS)} (fail-closed, no auto-upgrade)"
        )
    return normalizer(mapping)


# ---------------------------------------------------------------------------
# Validator battery (design §2 rules 1-3, §4, §5) — pure functions, one per
# invariant. Each returns None on pass or a human-auditable failure reason.
# ---------------------------------------------------------------------------


def validate_v_stage(*, stage: str) -> str | None:
    """V_STAGE — the compensable stage set is RATIFIED, not inferred (OQ6).

    Only ``acquire``/``fetch``/``materialize`` are compensable. ``promote`` is
    rejected with the explicit OQ6 reason rather than as an anonymous bad enum:
    promote has no dedicated recovery-tick command-owner seat (zero ``PROMOTE``
    owner constant in ``durable_runtime``, zero binding in
    ``recovery_drain_registry``, zero row in the characterized phase sequence),
    so there is no idempotent per-sub-unit seat to attach an intent to; and its
    decision shape is mid-flip under 议案③ S5.

    Calibration (D-C6, verified 2026-07-24): the design's §0 table says promote
    is "NOT drained by the tick". That is misleading — the promote guard IS
    reached transitively from the tick's ``snapshot_full_materialization`` phase
    (``_run_snapshot_full_materialization_queue_once`` → snapshot materializer →
    ``sync_company_asset_registration`` →
    ``upsert_organization_asset_registry_with_guard`` →
    ``evaluate_organization_asset_registry_promotion``). The load-bearing and
    sufficient premise is the ABSENCE OF A SEAT, which is what this validator
    rests on.
    """
    if stage in DEFERRED_STAGE_VALUES:
        return DEFERRED_STAGE_REASONS[stage]
    if stage not in COMPENSABLE_STAGE_VALUES:
        return f"stage {stage!r} is not a compensable pipeline stage {sorted(COMPENSABLE_STAGE_VALUES)}"
    return None


def validate_v_seat(
    *,
    stage: str,
    target_command_owner: str,
    seat_index: Mapping[str, CompensationSeat],
) -> str | None:
    """V_SEAT — the compensation action may only point at an EXISTING recovery-tick
    command-owner phase, and that phase must belong to the intent's stage (OQ1/OQ5).

    This is half of the "zero new dispatch identity" fence: an intent cannot name
    a phase the recovery tick does not already run, so compensation can never
    introduce a new owner surface (which would double the owner face and re-open
    R-019 — design D-C3).
    """
    seat = seat_index.get(target_command_owner)
    if seat is None:
        return (
            f"target_command_owner {target_command_owner!r} is not an existing recovery-tick compensation seat "
            f"(OQ1/OQ5: compensation reuses existing command owners; minting a new owner is forbidden). "
            f"Known seats: {sorted(seat_index)}"
        )
    if seat.stage != stage:
        return (
            f"target_command_owner {target_command_owner!r} is a {seat.stage!r}-stage seat but the intent declares "
            f"stage {stage!r}: an intent may never route across stages"
        )
    return None


def validate_v_key(
    *,
    owner_idempotency_key: str,
    intent_id: str,
    target_command_owner: str,
    seat_index: Mapping[str, CompensationSeat],
) -> str | None:
    """V_KEY — the idempotency key must be OWNER-DERIVED, never compensation-authored
    (OQ5, the never-double-pay fence + the R-019 zero-new-dispatch-identity rule).

    Two conditions, both required:

    1. **Owner namespace.** The key must be ``"<command_type>"`` or
       ``"<command_type>:…"`` for one of the target seat's own command types —
       the grammar every owner actually uses today
       (``acquisition_command_owner`` ``…:parent:{parent_command_id}`` /
       ``…:plan:{plan_id}:parent:…`` / ``…:review:{review_id}`` /
       ``…:acquisition_run:{id}:parent:…``; ``profile_fetch_owner``
       ``…:source:{scope_hash}``; ``durable_runtime``
       ``linkedin_profile_url_terminal_record_idempotency_key`` /
       ``snapshot_compaction_run_idempotency_key`` ``…:{scope_hash}``). The one
       exception is the ``acquisition.run.create`` seat, whose key is the ROOT
       operation's own opaque ``idempotency_key`` (a locked identity field) —
       for that seat the namespace check is waived, but condition 2 still holds.
    2. **Not self-minted.** A key in a ``compensation*`` namespace, a key equal to
       or prefixed by the schema id or the audit phase name, or a key embedding
       the intent's own ``intent_id`` could only have been authored by the
       compensation layer. Such a key IS a new dispatch identity and is rejected.

    Together these make "the contract invents a dispatch identity" inexpressible:
    the only keys it can carry are keys an existing owner already computes, so a
    replay always lands on that owner's existing ``already_succeeded``
    short-circuit (``acquisition_*_command_already_succeeded`` /
    ``acquisition_run_phase_command_already_succeeded``) and pays nothing.
    """
    key = owner_idempotency_key.strip()
    if not key:
        return "owner_idempotency_key is empty: an intent with no owner fence could re-dispatch and re-pay"
    lowered = key.lower()
    for prefix in _COMPENSATION_AUTHORED_KEY_PREFIXES:
        if lowered.startswith(prefix.lower()):
            return (
                f"owner_idempotency_key {key!r} lives in the compensation layer's own namespace "
                f"({prefix!r}): the contract may never mint a dispatch identity (OQ5 / R-019)"
            )
    if SCHEMA_ID_V1 in key:
        return (
            f"owner_idempotency_key {key!r} embeds the compensation schema id: the key must be the OWNER's own key, "
            "not one authored here"
        )
    if intent_id and intent_id in key:
        return (
            f"owner_idempotency_key {key!r} embeds the intent's own intent_id {intent_id!r}: an intent-derived key "
            "is by definition a NEW dispatch identity, which OQ5/R-019 forbid"
        )
    seat = seat_index.get(target_command_owner)
    if seat is None:
        # V_SEAT already reported the unknown seat; do not double-report here.
        return None
    if seat.inherits_root_operation_key:
        return None
    for command_type in seat.command_types:
        if key == command_type or key.startswith(f"{command_type}:"):
            return None
    return (
        f"owner_idempotency_key {key!r} is not in the {target_command_owner!r} owner's own command-type namespace "
        f"{list(seat.command_types)}: only a key the owner itself computes can hit its already_succeeded "
        "short-circuit (never-double-pay, §5 rule 1)"
    )


def validate_v_delta(
    *,
    kind: str,
    sub_unit_id: str,
    signal: str,
    evidence_ref: GapEvidenceRef,
    delta_only: bool,
) -> str | None:
    """V_DELTA — delta-only, anchored to exactly one named missing atom (§2.1 rule
    2 / §5 rule 2).

    Four conditions: ``delta_only`` must be true (a compensation intent that
    widens scope is not a top-up); the evidence must be non-empty (a "gap" with
    no self-reported evidence is not a gap and must never mint a paid replay);
    the signal's own evidence slot must be populated (a ``retry_wait_tail`` that
    only carries shard ids is a mis-composed intent); and when the delta anchor
    IS a shard, ``sub_unit_id`` must be one of the shard ids the evidence
    actually names — otherwise the intent could top up a shard the audit never
    observed as missing. A snapshot/board durable unit or a profile registry item
    is anchored by its own id, with the shard evidence explaining WHY it is
    incomplete.
    """
    if not delta_only:
        return "delta_only must be true: a compensation top-up may only re-run the single named missing sub-unit"
    if evidence_ref.is_empty():
        return (
            "gap.evidence_ref is empty: an intent with no self-reported evidence is not a detected gap and must "
            "never mint a replay (§5 rule 4, inventory-first)"
        )
    required_slots = _REQUIRED_EVIDENCE_SLOTS_BY_SIGNAL[signal]
    if not any(getattr(evidence_ref, slot) for slot in required_slots):
        return (
            f"signal {signal!r} requires a non-empty gap.evidence_ref slot among "
            f"{[f'evidence_ref.{slot}' for slot in required_slots]}"
        )
    if kind == SUB_UNIT_KIND_ACQUISITION_SHARD:
        named = set(evidence_ref.missing_shard_ids) | set(evidence_ref.truncated_shard_ids)
        if sub_unit_id not in named:
            return (
                f"sub_unit_id {sub_unit_id!r} is not among the shard ids the evidence names {sorted(named)}: "
                "the delta anchor must be one of the observed missing/truncated sub-units"
            )
    return None


def validate_v_lineage(*, stage: str, source_lineage: SourceLineage) -> str | None:
    """V_LINEAGE — the gap belongs to exactly one lineage, and never widens (§2.1
    rule 2, mirroring the promote guard's strict-subset refusal).

    A durable lineage identity (``operation_id`` or ``job_id``) is mandatory: an
    intent with no lineage could be drained against the wrong run.
    ``refill_plan_division_id`` is FETCH-ONLY — it is the 议案① divider's registry
    field (migration 0015) and carries no meaning for acquire/materialize, so
    carrying it elsewhere signals a mis-keyed intent (D-C1).
    """
    if not source_lineage.operation_id.strip() and not source_lineage.job_id.strip():
        return (
            "sub_unit.source_lineage carries neither operation_id nor job_id: a compensation intent must belong to "
            "exactly one lineage and can never be drained cross-lineage"
        )
    if stage != STAGE_FETCH and source_lineage.refill_plan_division_id.strip():
        return (
            f"sub_unit.source_lineage.refill_plan_division_id is fetch-only (议案① migration 0015) but the intent "
            f"declares stage {stage!r}"
        )
    return None


def validate_v_signal(
    *,
    stage: str,
    kind: str,
    signal: str,
    detected_by: str,
) -> str | None:
    """V_SIGNAL — the gap must be an owner SELF-REPORT that belongs to this stage
    (OQ2), and the sub-unit kind must match the stage's durable atom.

    The audit composes signals the stages already emit
    (``resolve_segmented_roster_completion`` → ``roster_partial`` /
    ``roster_truncated``; the refill ``retry_wait`` tail; the snapshot manifest's
    ``completion_status`` → ``manifest_partial``; un-drained board-visible items
    → ``board_visible_backlog``). It never recomputes correctness, so a signal
    outside the stage's self-reported vocabulary means someone re-derived
    coverage — the rival-reconciler failure mode OQ2 rejects.
    """
    if detected_by != GAP_DETECTED_BY_COMPLETENESS_AUDIT:
        return (
            f"gap.detected_by {detected_by!r} is not the single completeness audit (OQ2): compensation never runs a "
            "second reconciler that races the recovery daemon"
        )
    allowed_signals = _SIGNALS_BY_STAGE.get(stage, frozenset())
    if signal not in allowed_signals:
        return f"signal {signal!r} is not a {stage!r}-stage self-reported signal {sorted(allowed_signals)}"
    allowed_kinds = _SUB_UNIT_KINDS_BY_STAGE.get(stage, frozenset())
    if kind not in allowed_kinds:
        return f"sub_unit.kind {kind!r} is not a {stage!r}-stage durable atom {sorted(allowed_kinds)}"
    return None


def validate_v_attempt(*, attempt: CompensationAttempt) -> str | None:
    """V_ATTEMPT — recovery-of-recovery is BOUNDED and can never fail open (OQ7).

    ``max`` may never exceed :data:`ATTEMPT_MAX_CEILING` (3) and ``count`` may
    never exceed ``max``. The load-bearing rule: a SPENT intent
    (``count >= max``) must ALREADY carry ``compensation_exhausted_needs_human``.
    A spent intent with a null terminal status would be a silent re-loop — the
    self-perpetuating dispatch source the paid-dispatch red line forbids — so the
    contract makes it inexpressible. Backoff is the recovery tick's own ≤5 s poll
    cadence (``backoff_owner``), never a busy loop.
    """
    if attempt.max > ATTEMPT_MAX_CEILING:
        return f"attempt.max {attempt.max} exceeds the ratified ceiling {ATTEMPT_MAX_CEILING} (OQ7)"
    if attempt.count > attempt.max:
        return f"attempt.count {attempt.count} exceeds attempt.max {attempt.max}"
    if attempt.is_spent() and attempt.terminal_status != TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN:
        return (
            f"attempt.count {attempt.count} has reached attempt.max {attempt.max} but terminal_status is "
            f"{attempt.terminal_status!r}: a spent intent MUST terminalize to "
            f"{TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN!r} (OQ7 — never a silent re-loop, never fail-open)"
        )
    if not attempt.is_spent() and attempt.terminal_status == TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN:
        return (
            f"terminal_status {TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN!r} requires a spent attempt ladder "
            f"(count {attempt.count} < max {attempt.max})"
        )
    return None


# ---------------------------------------------------------------------------
# The §4 ladder + the terminal escalation record.
# ---------------------------------------------------------------------------


def resolve_compensation_outcome(
    *,
    replay_outcome: str,
    attempt_count: int,
    attempt_max: int = ATTEMPT_MAX_CEILING,
) -> dict[str, Any]:
    """The design §4 recovery-of-recovery ladder as a pure function (OQ7).

    Maps an EXISTING owner's replay observation to the intent's next attempt
    state and terminal status:

    * ``already_succeeded`` / ``compensated`` → ``compensated`` (the gap closed;
      an ``already_succeeded`` replay is the intended idempotent outcome and paid
      nothing).
    * ``gap_absent`` → ``superseded`` (the audit re-reads live signals; a closed
      gap yields no live signal, so the intent is retired, never re-dispatched).
    * ``transient_failure`` → increment the attempt count. Below the ceiling the
      intent stays open with ``terminal_status=None`` and waits for the tick's own
      poll cadence; at the ceiling it terminalizes to
      ``compensation_exhausted_needs_human`` (durable, board-visible, needs-human)
      — never a silent re-loop, never fail-open.

    Returns ``{"terminal_status", "next_attempt_count", "escalate", "reason"}``.
    Fail-closed on an unknown ``replay_outcome`` or an out-of-range ladder.
    """
    if replay_outcome not in REPLAY_OUTCOME_VALUES:
        raise CompensationContractError(
            f"replay_outcome must be one of {sorted(REPLAY_OUTCOME_VALUES)}, got {replay_outcome!r}"
        )
    bounded_max = _require_int(attempt_max, "attempt_max", minimum=1, maximum=ATTEMPT_MAX_CEILING)
    count = _require_int(attempt_count, "attempt_count", minimum=0, maximum=ATTEMPT_MAX_CEILING)
    if count > bounded_max:
        raise CompensationContractError(f"attempt_count {count} exceeds attempt_max {bounded_max}")
    if replay_outcome in {REPLAY_OUTCOME_ALREADY_SUCCEEDED, REPLAY_OUTCOME_COMPENSATED}:
        reason = (
            "owner replay reported already_succeeded: the gap was already closed under the owner's own fence "
            "(zero re-dispatch, zero payment)"
            if replay_outcome == REPLAY_OUTCOME_ALREADY_SUCCEEDED
            else "owner replay closed the named sub-unit gap"
        )
        return {
            "terminal_status": TERMINAL_STATUS_COMPENSATED,
            "next_attempt_count": count,
            "escalate": False,
            "reason": reason,
        }
    if replay_outcome == REPLAY_OUTCOME_GAP_ABSENT:
        return {
            "terminal_status": TERMINAL_STATUS_SUPERSEDED,
            "next_attempt_count": count,
            "escalate": False,
            "reason": "the underlying gap no longer reports a live signal (lineage superseded or closed elsewhere)",
        }
    next_count = min(count + 1, bounded_max)
    if next_count >= bounded_max:
        return {
            "terminal_status": TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN,
            "next_attempt_count": bounded_max,
            "escalate": True,
            "reason": (
                f"owner replay failed transiently {bounded_max} time(s); the bounded ladder is spent and the intent "
                "terminalizes to a durable board-visible needs-human record"
            ),
        }
    return {
        "terminal_status": None,
        "next_attempt_count": next_count,
        "escalate": False,
        "reason": (
            f"owner replay failed transiently; attempt {next_count}/{bounded_max} waits for the recovery tick's own "
            "poll cadence"
        ),
    }


def build_compensation_escalation(
    *,
    intent: CompensationIntent,
    escalation_reason: str,
) -> dict[str, Any]:
    """Assemble the §4 terminal ``compensation_exhausted_needs_human`` record.

    A durable, board-visible needs-human row — the escalation surface the OQ7
    ladder terminalizes into. It carries the intent's identity, the spent attempt
    ladder, and a bounded reason string; it carries NO payload and NO dispatch
    instruction, so an escalation can never be replayed into a paid call. Fail-closed
    if the intent's ladder is not actually spent."""
    if not intent.attempt.is_spent():
        raise CompensationContractError(
            f"escalation requires a spent attempt ladder (count {intent.attempt.count} < max {intent.attempt.max})"
        )
    return {
        "schema_id": SCHEMA_ID_V1,
        "intent_id": intent.intent_id,
        "stage": intent.stage,
        "sub_unit_id": intent.sub_unit.sub_unit_id,
        "source_lineage": intent.sub_unit.source_lineage.to_payload(),
        "target_command_owner": intent.compensation_action.target_command_owner,
        "terminal_status": TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN,
        "attempt_count": intent.attempt.count,
        "attempt_max": intent.attempt.max,
        "escalation_reason": str(escalation_reason or "")[:MAX_ESCALATION_REASON_LENGTH],
        "needs_human": True,
        "board_visible": True,
    }


def normalize_compensation_escalation(value: Any, label: str = "escalation") -> dict[str, Any]:
    """Strictly validate a §4 escalation record (the S2/S3 read surface's shape)."""
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _ESCALATION_KEYS, label)
    _require_keys(payload, _ESCALATION_KEYS, label)
    schema_id = _require_str(payload["schema_id"], f"{label}.schema_id")
    if schema_id != SCHEMA_ID_V1:
        raise CompensationContractError(f"{label}.schema_id must be {SCHEMA_ID_V1!r}, got {schema_id!r}")
    terminal_status = _require_str(payload["terminal_status"], f"{label}.terminal_status")
    if terminal_status != TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN:
        raise CompensationContractError(
            f"{label}.terminal_status must be {TERMINAL_STATUS_EXHAUSTED_NEEDS_HUMAN!r}, got {terminal_status!r}"
        )
    attempt_max = _require_int(payload["attempt_max"], f"{label}.attempt_max", minimum=1, maximum=ATTEMPT_MAX_CEILING)
    attempt_count = _require_int(
        payload["attempt_count"], f"{label}.attempt_count", minimum=0, maximum=ATTEMPT_MAX_CEILING
    )
    if attempt_count < attempt_max:
        raise CompensationContractError(
            f"{label} escalates with an unspent ladder (count {attempt_count} < max {attempt_max})"
        )
    if not _require_bool(payload["needs_human"], f"{label}.needs_human"):
        raise CompensationContractError(f"{label}.needs_human must be true (OQ7: escalation is never fail-open)")
    if not _require_bool(payload["board_visible"], f"{label}.board_visible"):
        raise CompensationContractError(f"{label}.board_visible must be true (OQ7: the escalation is board-visible)")
    _require_str(payload["intent_id"], f"{label}.intent_id", max_length=MAX_INTENT_ID_LENGTH)
    _require_str(payload["stage"], f"{label}.stage")
    _require_str(payload["sub_unit_id"], f"{label}.sub_unit_id", max_length=MAX_SUB_UNIT_ID_LENGTH)
    _require_str(payload["target_command_owner"], f"{label}.target_command_owner")
    _require_str(payload["escalation_reason"], f"{label}.escalation_reason", max_length=MAX_ESCALATION_REASON_LENGTH)
    _normalize_source_lineage(payload["source_lineage"], f"{label}.source_lineage")
    return dict(payload)


# ---------------------------------------------------------------------------
# Top-level fail-closed validation.
# ---------------------------------------------------------------------------


def validate_compensation_intent(
    payload: Any,
    *,
    seats: Iterable[CompensationSeat] = DEFAULT_COMPENSATION_SEATS,
) -> dict[str, Any]:
    """Normalize + run the full battery. Fail-closed.

    Returns ``{"valid", "failures": [{"validator_id", "reason"}...], "normalized",
    "validator_results", "durable_key"}``:

    * ``valid`` — True only when the record parses strictly AND every validator
      passes. Unlike the promote contract (where an honest AI reject is always
      valid), a compensation intent has no "honest negative" form: an intent that
      fails any invariant must never be enqueued, because enqueuing it is what
      could lead to a re-dispatch.
    * ``normalized`` — the round-trippable payload, or ``None`` on a strict-parse
      failure (a single ``validator_id="schema"`` failure).
    * ``durable_key`` — the OQ3 ``(stage, sub_unit_id, operation_id, job_id)``
      identity, present only for a valid intent.

    An unknown ``schema_id`` (including a future ``...v2``) is rejected by
    exact-version lookup with no auto-upgrade and no repair.
    """
    seat_index = build_compensation_seat_index(seats)
    try:
        intent = normalize_compensation_intent(payload)
    except CompensationContractError as error:
        return {
            "valid": False,
            "failures": [{"validator_id": SCHEMA_FAILURE_VALIDATOR_ID, "reason": str(error)}],
            "normalized": None,
            "validator_results": [],
            "durable_key": None,
        }

    outcomes: list[tuple[str, str | None]] = [
        (VALIDATOR_V_STAGE, validate_v_stage(stage=intent.stage)),
        (
            VALIDATOR_V_SEAT,
            validate_v_seat(
                stage=intent.stage,
                target_command_owner=intent.compensation_action.target_command_owner,
                seat_index=seat_index,
            ),
        ),
        (
            VALIDATOR_V_KEY,
            validate_v_key(
                owner_idempotency_key=intent.compensation_action.owner_idempotency_key,
                intent_id=intent.intent_id,
                target_command_owner=intent.compensation_action.target_command_owner,
                seat_index=seat_index,
            ),
        ),
        (
            VALIDATOR_V_DELTA,
            validate_v_delta(
                kind=intent.sub_unit.kind,
                sub_unit_id=intent.sub_unit.sub_unit_id,
                signal=intent.gap.signal,
                evidence_ref=intent.gap.evidence_ref,
                delta_only=intent.compensation_action.delta_only,
            ),
        ),
        (
            VALIDATOR_V_LINEAGE,
            validate_v_lineage(stage=intent.stage, source_lineage=intent.sub_unit.source_lineage),
        ),
        (
            VALIDATOR_V_SIGNAL,
            validate_v_signal(
                stage=intent.stage,
                kind=intent.sub_unit.kind,
                signal=intent.gap.signal,
                detected_by=intent.gap.detected_by,
            ),
        ),
        (VALIDATOR_V_ATTEMPT, validate_v_attempt(attempt=intent.attempt)),
    ]

    failures: list[dict[str, str]] = []
    validator_results: list[dict[str, str]] = []
    for validator_id, failure_reason in outcomes:
        if failure_reason is not None:
            validator_results.append(
                {"validator": validator_id, "status": VALIDATOR_RESULT_STATUS_FAIL, "reason": failure_reason}
            )
            failures.append({"validator_id": validator_id, "reason": failure_reason})
        else:
            validator_results.append({"validator": validator_id, "status": VALIDATOR_RESULT_STATUS_PASS})

    valid = not failures
    return {
        "valid": valid,
        "failures": failures,
        "normalized": intent.to_payload(),
        "validator_results": validator_results,
        "durable_key": list(intent.durable_key()) if valid else None,
    }
