"""Promote-judgment orchestration helper (WS7/W7.3 slice S2, ADDITIVE ONLY).

Spec: docs/WS7_AI_PROMOTE_DESIGN.md §3 (model invocation surface), §4 (ruling-④
failure classes F1-F6 → keep incumbent), OQ4/OQ5/OQ8 RATIFIED 2026-07-24. This
module is the thin seam between the ModelClient promote-judge method
(`judge_organization_asset_promotion`, model_provider.py) and the S1 contract
(`organization_promote_contract`): it builds the AI-visible input payload (the
caller-authored candidate descriptor — coverage/lineage evidence, completeness
signals, generation, prior-snapshot comparison — NEVER the lineage-guard
verdict), calls the model ONCE per contested decision (OQ4), assembles the
caller-owned envelope fields (decision_id, decision_source=ai_judge, provenance
passthrough), and validates through the single-sourced S1 battery
(`validate_ai_promote_decision`, which folds the guard's read-only predicted
verdict and the incumbent descriptor in as PARAMETERS).

ADDITIVE ONLY — no integration into asset_reuse_planning.py / storage.py / the
live promote path (that is S3/S5). Everything here is offline-drivable: the
guard verdict and incumbent arrive as parameters, so no store, PG, or network is
needed to exercise the judgment.

Engagement policy (OQ4): the judge engages ONLY on a CONTESTED decision (an
incumbent exists and the candidate is a different, non-refresh, promotable
snapshot). The deterministic pre-branches (design §1.1: no-incumbent /
same-snapshot-refresh / lifecycle-not-promotable) resolve to promote-or-keep
WITHOUT the model — a non-contested decision never calls it. The caller passes
`contested` (and, for a non-contested decision, `deterministic_promote` to say
which pre-branch outcome applies).

Failure direction (ruling ④, design §4): EVERY failure class — judge
unavailable (F1), circuit open (F2), call failed / timeout (F3), invalid output
(F4), validator rejection (F5), stale input (F6) — maps to KEEP THE INCUMBENT
plus a recorded §4 audit. Unlike the divider (efficiency → rule-ladder
fallback), promotion is asset-correctness-conservative: a failure NEVER flips
authority, and there is NO ladder fallback. The AI can only be MORE conservative
than the rules — an honest reject is a valid decision that keeps the incumbent.

Responsibility split (design §2.2, mirror of divider D7): the model authors ONLY
`{decision, reason, reason_code}`; decision_id, decision_source, the
candidate_descriptor, and provenance are caller-authored fact. Assembling that
envelope is not "repairing" model output — the raw verdict passes through
byte-identical and any defect surfaces as the auditable F4/F5 fallback.

SHADOW SAFETY GATE (S3, added 2026-07-25 — design §7.1). The S3 shadow hook
below runs SYNCHRONOUSLY INSIDE the authoritative write path
(`upsert_organization_asset_registry_with_guard`, right after the store write).
It is therefore gated by TWO independent predicates, not one:

  1. `model_client_supports_promote_judgment` — structural capability ("would a
     call return anything but the F1 marker?"). True for the scripted judge AND
     for the real provider clients.
  2. `promote_shadow_model_calls_permitted` — PERMISSION. A client whose class
     declares `ws7_shadow_billing_free = True` is always permitted; any other
     client requires the dedicated opt-in env
     `SOURCING_WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL`, which is OFF BY DEFAULT and
     is deliberately not one of the three live-provider gate vars.

Consequence: with no opt-in the shadow cannot reach a provider no matter which
client a production caller threads in, so the real path is never easier to
trigger than the scripted one (which has always required
`SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE`). Unsetting the env is the kill
switch for a live wave. The divider's mint-seam shadow carries the mirrored gate
(`SOURCING_WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL`, profile_batch_division.py).
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from .model_provider import (
    ORGANIZATION_PROMOTE_JUDGE_CIRCUIT_ERROR_PREFIX,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_RAW_PREVIEW_KEY,
    WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL_ENV,
    DeterministicModelClient,
    ModelClient,
    ws7_shadow_model_calls_permitted,
)
from .organization_promote_contract import (
    DECISION_SOURCE_AI_JUDGE,
    DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
    FALLBACK_REASON_CALL_FAILED,
    FALLBACK_REASON_CIRCUIT_OPEN,
    FALLBACK_REASON_INPUT_STALE,
    FALLBACK_REASON_INVALID_OUTPUT,
    FALLBACK_REASON_MODEL_UNAVAILABLE,
    MAX_JUDGE_ERROR_LENGTH,
    SCHEMA_ID_V1,
    PromoteDecisionContractError,
    fallback_reason_for_validator,
    normalize_fallback_audit,
    predict_lineage_guard_refusal,
    validate_ai_promote_decision,
)

STATUS_PROMOTED = "promoted"
STATUS_KEPT_INCUMBENT = "kept_incumbent"

# The pre-AI deterministic outcome recorded when the judge is not engaged (OQ4).
SKIP_REASON_NOT_CONTESTED = "decision_not_contested_pre_branch"

# Wire-shape rule for the model's raw output (design §2.2/§3.3): the model
# authors exactly these three keys — anything else is an F4 keep-incumbent.
_MODEL_JUDGMENT_REQUIRED_KEYS = frozenset({"decision", "reason", "reason_code"})


def build_promote_judge_input_payload(
    *,
    candidate_descriptor: Mapping[str, Any],
    decision_id: str,
    contested_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the AI promote-judge input payload (design §3.2).

    Carries the AI-visible candidate descriptor (incumbent + candidate metric
    pairs, completeness_score feature, selected_snapshot_ids/lineage evidence,
    per-shard request populations, simulate-provenance flag) + the
    contested-decision context. It carries NO store internals and NO guard
    verdict — the descriptor schema (S1) structurally has no slot for the guard's
    pass/fail, and the guard verdict flows to validation as a separate parameter,
    never through the model.
    """
    return {
        "task": "organization_asset_promotion_judgment",
        "output_contract_schema_id": SCHEMA_ID_V1,
        "decision_id": str(decision_id),
        "candidate_descriptor": dict(candidate_descriptor),
        "contested_context": dict(contested_context or {}),
    }


def build_keep_incumbent_audit(
    fallback_reason: str,
    *,
    judge_error: str = "",
    validator_results: Sequence[Mapping[str, Any]] = (),
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build + strictly normalize one ruling-④ keep-incumbent audit (design §4).

    Single builder for every failure class F1-F6. A structurally invalid client
    ``provenance`` is dropped to null with an explicit note appended to
    ``judge_error`` — the audit itself must never fail to record (mirror of the
    divider's build_fallback_audit).
    """
    truncated_error = " ".join(str(judge_error or "").strip().split())[:MAX_JUDGE_ERROR_LENGTH]
    payload: dict[str, Any] = {
        "decision_source": DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
        "fallback_reason": str(fallback_reason),
        "judge_error": truncated_error,
        "validator_results": [dict(entry) for entry in validator_results],
        "provenance": dict(provenance) if isinstance(provenance, Mapping) else None,
    }
    try:
        return normalize_fallback_audit(payload).to_payload()
    except PromoteDecisionContractError:
        if payload["provenance"] is None:
            raise
        note = "; provenance dropped: client payload failed strict audit shape"
        payload["provenance"] = None
        payload["judge_error"] = (truncated_error + note)[:MAX_JUDGE_ERROR_LENGTH]
        return normalize_fallback_audit(payload).to_payload()


def stale_input_keep_incumbent_audit(
    *,
    recorded_incumbent_snapshot_id: str,
    current_incumbent_snapshot_id: str,
    provenance: Mapping[str, Any] | None = None,
    validator_results: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """F6 audit for the apply path (design §4): the incumbent authority changed
    between the input snapshot and apply (another writer flipped it).

    Exported for the S3/S5 apply seam — within one synchronous
    ``judge_and_validate_promotion`` call the input cannot go stale.
    """
    return build_keep_incumbent_audit(
        FALLBACK_REASON_INPUT_STALE,
        judge_error=(
            "incumbent authority changed between input snapshot and apply: recorded "
            f"{recorded_incumbent_snapshot_id} != current {current_incumbent_snapshot_id}"
        ),
        validator_results=validator_results,
        provenance=provenance,
    )


def _result(
    *,
    status: str,
    engaged: bool,
    decision: dict[str, Any] | None,
    audit: dict[str, Any] | None,
    decision_id: str,
    contested: bool,
    skip_reason: str = "",
    validator_results: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "status": status,
        "engaged": engaged,
        "decision": decision,
        "audit": audit,
        "decision_id": decision_id,
        "contested": contested,
        "skip_reason": skip_reason,
        "validator_results": [dict(entry) for entry in validator_results],
    }


def judge_and_validate_promotion(
    model_client: ModelClient,
    *,
    candidate_descriptor: Mapping[str, Any],
    guard_predicted_verdict: Mapping[str, Any],
    incumbent_descriptor: Mapping[str, Any],
    contested: bool,
    deterministic_promote: bool = False,
    contested_context: Mapping[str, Any] | None = None,
    decision_id: str | None = None,
    completeness_regression_tolerance: float | None = None,
) -> dict[str, Any]:
    """One promote judgment per contested decision (OQ4): call once, validate via S1.

    Returns ``{"status": "promoted"|"kept_incumbent", "decision", "audit",
    "engaged", "decision_id", "contested", ...}``:

    * ``status="promoted"`` — the candidate replaces the incumbent. ``decision``
      carries the validated ai_promote_decision.v1 record; ``audit`` is None.
    * ``status="kept_incumbent"`` — the incumbent stays authoritative (ruling ④).
      ``decision`` carries the validated record on an HONEST AI reject;
      ``audit`` carries the §4 keep-incumbent audit on any failure class F1-F6.

    Engagement (OQ4): the model is invoked EXACTLY ONCE and ONLY when
    ``contested`` is True. A non-contested decision resolves deterministically
    WITHOUT a model call — ``deterministic_promote`` selects the pre-branch
    outcome (no-incumbent / same-snapshot-refresh promote vs
    lifecycle-not-promotable keep).

    Failure mapping (ruling ④, design §4 — a failure NEVER flips authority, no
    ladder fallback):

    - F1 ``judge_model_unavailable``: structural ``{}`` response
      (DeterministicModelClient / OfflineModelClient / opt-in absent).
    - F2 ``judge_circuit_open``: error string with the shared circuit prefix.
    - F3 ``judge_call_failed``: any other transport/timeout error string.
    - F4 ``judge_invalid_output``: non-JSON output, wire-shape violation
      (anything but exactly ``{decision, reason, reason_code}``), or S1
      strict-parse rejection (``validator_id="schema"``).
    - F5 ``judge_validator_rejected:<validator>``: the AI approved but a hard
      floor (guard pre-filter V_LINEAGE / V_COMP / V_GEN / V_PROV / V_LIFECYCLE)
      rejects — the AI can only be MORE conservative than the rules.

    The guard's read-only predicted verdict and the incumbent descriptor are
    passed to :func:`validate_ai_promote_decision` as PARAMETERS; the model
    never sees or authors the guard verdict (design §2.2, ruling ③).
    """
    resolved_decision_id = str(decision_id or uuid.uuid4().hex)

    if not contested:
        # OQ4: deterministic pre-branch — no model call, no F-audit. The concrete
        # pre-branch reason string (no_existing_authoritative / same_snapshot_refresh
        # / lifecycle_state_not_promotable) is the evaluate path's responsibility
        # (S3/S5); here we only record the engagement outcome.
        return _result(
            status=STATUS_PROMOTED if deterministic_promote else STATUS_KEPT_INCUMBENT,
            engaged=False,
            decision=None,
            audit=None,
            decision_id=resolved_decision_id,
            contested=False,
            skip_reason=SKIP_REASON_NOT_CONTESTED,
        )

    provider = model_client.provider_name()
    payload = build_promote_judge_input_payload(
        candidate_descriptor=candidate_descriptor,
        decision_id=resolved_decision_id,
        contested_context=contested_context,
    )
    response = model_client.judge_organization_asset_promotion(payload)

    def _keep_incumbent(
        fallback_reason: str,
        *,
        judge_error: str,
        validator_results: Sequence[Mapping[str, Any]] = (),
        provenance: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _result(
            status=STATUS_KEPT_INCUMBENT,
            engaged=True,
            decision=None,
            audit=build_keep_incumbent_audit(
                fallback_reason,
                judge_error=judge_error,
                validator_results=validator_results,
                provenance=provenance,
            ),
            decision_id=resolved_decision_id,
            contested=True,
            validator_results=validator_results,
        )

    # F1: structural "judge unavailable" marker (deterministic/offline client,
    # opt-in absent, or no client) — no call was made.
    if not isinstance(response, Mapping) or not response:
        return _keep_incumbent(
            FALLBACK_REASON_MODEL_UNAVAILABLE,
            judge_error=f"model client returned no judgment (provider: {provider})",
        )

    # F2/F3: the client surfaced a call error (circuit-open vs transport/timeout).
    error_text = str(response.get(ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY) or "")
    if error_text:
        reason = (
            FALLBACK_REASON_CIRCUIT_OPEN
            if error_text.startswith(ORGANIZATION_PROMOTE_JUDGE_CIRCUIT_ERROR_PREFIX)
            else FALLBACK_REASON_CALL_FAILED
        )
        return _keep_incumbent(reason, judge_error=error_text)

    raw_provenance = response.get(ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY)
    provenance = dict(raw_provenance) if isinstance(raw_provenance, Mapping) else None
    raw_preview = str(response.get(ORGANIZATION_PROMOTE_JUDGE_RESPONSE_RAW_PREVIEW_KEY) or "")
    judgment_raw = response.get(ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY)

    # F4: no parsed JSON object.
    if not isinstance(judgment_raw, Mapping) or not judgment_raw:
        return _keep_incumbent(
            FALLBACK_REASON_INVALID_OUTPUT,
            judge_error=("model produced no JSON object output" + (f"; raw: {raw_preview}" if raw_preview else "")),
            provenance=provenance,
        )

    # F4: wire-shape violation — the model must author EXACTLY
    # {decision, reason, reason_code} (design §2.2). The guard verdict has no
    # slot here and any extra key is rejected before envelope assembly.
    judgment_keys = {str(key) for key in judgment_raw.keys()}
    unexpected_keys = sorted(judgment_keys - _MODEL_JUDGMENT_REQUIRED_KEYS)
    missing_keys = sorted(_MODEL_JUDGMENT_REQUIRED_KEYS - judgment_keys)
    if unexpected_keys or missing_keys:
        return _keep_incumbent(
            FALLBACK_REASON_INVALID_OUTPUT,
            judge_error=(
                "model output must be exactly {decision, reason, reason_code}; "
                f"unexpected keys {unexpected_keys}, missing keys {missing_keys}"
            ),
            provenance=provenance,
        )

    envelope: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "decision_id": resolved_decision_id,
        "decision_source": DECISION_SOURCE_AI_JUDGE,
        "decision": judgment_raw["decision"],
        "reason": judgment_raw["reason"],
        "reason_code": judgment_raw["reason_code"],
        "candidate_descriptor": dict(candidate_descriptor),
        "provenance": provenance,
        "validator_results": [],
        "fallback": None,
    }
    validate_kwargs: dict[str, Any] = {
        "guard_predicted_verdict": guard_predicted_verdict,
        "incumbent_descriptor": incumbent_descriptor,
    }
    if completeness_regression_tolerance is not None:
        validate_kwargs["completeness_regression_tolerance"] = completeness_regression_tolerance
    result = validate_ai_promote_decision(envelope, **validate_kwargs)

    normalized = result["normalized"]
    # F4: strict-parse rejection (bad decision value, malformed descriptor,
    # invalid reason_code, etc.) — validation single-sourced in S1.
    if normalized is None:
        failures = result["failures"] or [{}]
        return _keep_incumbent(
            FALLBACK_REASON_INVALID_OUTPUT,
            judge_error=str(failures[0].get("reason") or "strict-parse rejection"),
            validator_results=result["validator_results"],
            provenance=provenance,
        )

    if result["effective_decision"] == "promote":
        decision_payload = dict(normalized)
        decision_payload["validator_results"] = list(result["validator_results"])
        return _result(
            status=STATUS_PROMOTED,
            engaged=True,
            decision=decision_payload,
            audit=None,
            decision_id=resolved_decision_id,
            contested=True,
            validator_results=result["validator_results"],
        )

    # effective_decision == "reject".
    if result["fallback"] is not None:
        # F5: the AI approved but a hard floor rejected — keep the incumbent with
        # the §4 F5 audit the conjunction produced (validator_id in fallback_reason).
        return _result(
            status=STATUS_KEPT_INCUMBENT,
            engaged=True,
            decision=None,
            audit=dict(result["fallback"]),
            decision_id=resolved_decision_id,
            contested=True,
            validator_results=result["validator_results"],
        )

    # Honest AI reject: a valid decision that keeps the incumbent (ruling ④ — the
    # AI may be more conservative than the rules). The reject record itself IS the
    # audit; no F-class fired.
    decision_payload = dict(normalized)
    decision_payload["validator_results"] = list(result["validator_results"])
    return _result(
        status=STATUS_KEPT_INCUMBENT,
        engaged=True,
        decision=decision_payload,
        audit=None,
        decision_id=resolved_decision_id,
        contested=True,
        validator_results=result["validator_results"],
    )


# ---------------------------------------------------------------------------
# WS7/W7.3 slice S3 — SHADOW integration (records, NEVER changes the live
# promote decision or which row becomes authoritative).
#
# Design §2.4/§7 S3: the shadow hook runs at the
# ``upsert_organization_asset_registry_with_guard`` seam (asset_reuse_planning.py:1541)
# AFTER the ladder decision and the store write, computes the AI promote judgment,
# and returns a record the live promote path NEVER reads. Authority stays 100%
# the rule ladder (`evaluate`) + the storage lineage guard.
#
# PLACEMENT NOTE (honest calibration vs design §2.4). The design named the record
# an additive ``metadata.ai_promote_decision`` key on the registry row, but the
# ``organization_asset_registry`` table has NO ``metadata``/``metadata_json``
# column and no ``schema_version`` (only ``summary_json`` + lane/selection JSON;
# migrations/0001_baseline.sql). Adding a durable column is a schema change that
# S5 owns (task hard rule: "no schema_version bump on the registry — S5's job").
# So — exactly as the proven divider S3 attached its shadow to the
# ``refill_plan_items`` activity surface and deferred the durable
# ``refill_plan_division_id`` registry field to a LATER slice (S4, migration 0015)
# — this S3 attaches the shadow to the seam's RETURNED record under the sibling
# key ``ai_promote_decision_shadow``. The store write (which row is authoritative)
# is byte-identical with the shadow on or off; the durable
# ``metadata.ai_promote_decision`` column is deferred to S5's schema bump.
# ---------------------------------------------------------------------------

# Sibling key on the guard-wrapper's returned record (record-only surface).
SHADOW_RECORD_KEY = "ai_promote_decision_shadow"
SHADOW_RECORD_KIND = "organization_asset_ai_promote_decision_shadow"
SHADOW_STATUS_ERROR = "shadow_error"

# The deterministic pre-branch reasons `evaluate` emits (asset_reuse_planning.py
# :1227/:1235/:1244); a decision carrying one of these is NOT contested (OQ4) and
# never calls the model.
_DETERMINISTIC_PREBRANCH_REASONS = frozenset(
    {"no_existing_authoritative", "same_snapshot_refresh", "lifecycle_state_not_promotable"}
)
# Pre-branch reasons whose deterministic outcome is a promote (the third,
# lifecycle_state_not_promotable, keeps the incumbent).
_DETERMINISTIC_PROMOTE_REASONS = frozenset({"no_existing_authoritative", "same_snapshot_refresh"})


def model_client_supports_promote_judgment(model_client: Any) -> bool:
    """True when the client overrides the deterministic F1 judge stub.

    Structural capability check for the S3 condition (a) "scripted/live judge
    client available": ``DeterministicModelClient.judge_organization_asset_promotion``
    (and its ``OfflineModelClient`` inheritance) is the structural "judge
    unavailable" marker (design §3.1) — engaging it on every promote decision
    would only mint F1 keep-incumbent audit noise, so the shadow hook does not
    engage at all for such clients (mirror of the divider's
    ``model_client_supports_batch_division``).
    """
    if model_client is None:
        return False
    method = getattr(type(model_client), "judge_organization_asset_promotion", None)
    if method is None:
        return False
    return method is not DeterministicModelClient.judge_organization_asset_promotion


def promote_shadow_model_calls_permitted(model_client: Any) -> bool:
    """SAFETY gate — may the shadow seam actually CALL this client? (2026-07-25)

    Separate from :func:`model_client_supports_promote_judgment`, which answers
    only "would a call return something other than the F1 marker?". Capability
    alone is true for the REAL provider clients as well, and the promote shadow
    runs SYNCHRONOUSLY INSIDE ``upsert_organization_asset_registry_with_guard``
    — the authoritative write path — once per contested decision. Threading a
    real client down from a live-mode caller would therefore have billed a
    provider call from a path that is by definition optional and record-only.

    So the recorder requires capability AND permission:

    * a client whose class declares ``ws7_shadow_billing_free = True`` (the
      scripted judge; local test doubles that make no network call) is always
      permitted — it cannot bill;
    * ANY other client requires the dedicated opt-in env
      ``SOURCING_WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL``, which is OFF by default
      and is deliberately NOT one of the three live-provider gate vars, so
      enabling live mode alone never enables the shadow call.

    With no opt-in the seam returns None before building any payload, so it
    cannot reach a provider no matter what client production threads into it.
    This is the kill switch the wiring batch shipped without: unset the env (the
    default) and the shadow is inert for every real client.
    """

    return ws7_shadow_model_calls_permitted(model_client, allow_real_model_env=WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL_ENV)


def _shadow_int(value: Any) -> int:
    try:
        if isinstance(value, bool):
            return 0
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _shadow_float(value: Any) -> float:
    try:
        if isinstance(value, bool):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _shadow_selected_snapshot_ids(row: Mapping[str, Any]) -> list[str]:
    selection = dict(row.get("source_snapshot_selection") or {}) if isinstance(row, Mapping) else {}
    raw = row.get("selected_snapshot_ids") or selection.get("selected_snapshot_ids") or []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return []
    return [str(value).strip() for value in raw if str(value or "").strip()]


def _shadow_completeness_band(row: Mapping[str, Any], score: float) -> str:
    band = str(row.get("completeness_band") or "").strip()
    if band:
        return band
    if score >= 75:
        return "high"
    if score >= 50:
        return "medium"
    return "low"


def _snapshot_descriptor_from_registry_row(
    row: Mapping[str, Any], *, incumbent_snapshot_id: str = ""
) -> dict[str, Any]:
    """Map an ``organization_asset_registry`` row (duck-typed) to the S1
    SnapshotDescriptor payload shape (design §2.2). Reads exactly the fields
    `evaluate` reads today (asset_reuse_planning.py:1254-1269) plus lineage
    evidence; carries NO guard verdict and NO store internals."""
    row = dict(row or {})
    score = _shadow_float(row.get("completeness_score"))
    selected = _shadow_selected_snapshot_ids(row)
    effective_lane_total = _shadow_int(row.get("current_lane_effective_candidate_count")) + _shadow_int(
        row.get("former_lane_effective_candidate_count")
    )
    return {
        "snapshot_id": str(row.get("snapshot_id") or ""),
        "metrics": {
            "candidate_count": _shadow_int(row.get("candidate_count")),
            "evidence_count": _shadow_int(row.get("evidence_count")),
            "profile_detail_count": _shadow_int(row.get("profile_detail_count")),
            "missing_linkedin_count": _shadow_int(row.get("missing_linkedin_count")),
            "profile_completion_backlog_count": _shadow_int(row.get("profile_completion_backlog_count")),
            "effective_lane_total": effective_lane_total,
        },
        "completeness_score": score,
        "completeness_band": _shadow_completeness_band(row, score),
        "selected_snapshot_ids": selected,
        "source_snapshot_count": _shadow_int(row.get("source_snapshot_count")),
        "materialization_generation_sequence": _shadow_int(row.get("materialization_generation_sequence")),
        "lifecycle_status": str(row.get("status") or "ready").strip() or "ready",
        "materialization_generation_key": str(row.get("materialization_generation_key") or ""),
        "explicit_baseline_inclusion": bool(incumbent_snapshot_id and incumbent_snapshot_id in selected),
    }


def _shadow_sort_key(descriptor: Mapping[str, Any]) -> list[Any]:
    metrics = dict(descriptor.get("metrics") or {})
    return [
        _shadow_float(descriptor.get("completeness_score")),
        _shadow_int(metrics.get("effective_lane_total")),
        _shadow_int(metrics.get("candidate_count")),
        _shadow_int(metrics.get("profile_detail_count")),
    ]


def _shadow_candidate_descriptor(
    *, incumbent_descriptor: Mapping[str, Any], candidate_descriptor: Mapping[str, Any]
) -> dict[str, Any]:
    """Assemble the AI-visible candidate_descriptor (design §2.2). Pre-S4 the
    per-shard coverage evidence is not yet recorded (§2.3: payload-snapshot
    capture + lineage backfill land in S4), so ``shards`` is empty — itself the
    documented weak-evidence signal — and ``request_population_match`` is a
    best-effort selected-id superset check."""
    incumbent_selected = set(incumbent_descriptor.get("selected_snapshot_ids") or [])
    candidate_selected = set(candidate_descriptor.get("selected_snapshot_ids") or [])
    return {
        "incumbent": dict(incumbent_descriptor),
        "candidate": dict(candidate_descriptor),
        "coverage_evidence": {
            "shards": [],
            "request_population_match": {
                "candidate_covers_incumbent_shards": bool(incumbent_selected)
                and incumbent_selected <= candidate_selected,
                "new_shards": [],
                "dropped_shards": [],
            },
        },
        "prior_snapshot_comparison": {
            "candidate_sort_key": _shadow_sort_key(candidate_descriptor),
            "incumbent_sort_key": _shadow_sort_key(incumbent_descriptor),
            # Pre-S4: no per-snapshot simulate/placeholder provenance signal is
            # recorded on the registry row, so this is conservatively False. When
            # S4 lands the provenance capture, this becomes the real flag.
            "simulate_or_placeholder_provenance": False,
        },
    }


def _shadow_ladder_comparison(
    *, ladder_promote: bool, ladder_reason: str, ai_promote: bool, contested: bool, engaged: bool
) -> dict[str, Any]:
    """Divergence digest: the ladder's ACTUAL promote decision vs the AI's
    effective decision (design §7 S3 — the "AI decision ≠ ladder decision"
    counter the S5 flip consumes as free before/after evidence).

    CORRECTED 2026-07-25, RE-CORRECTED the same day (design §7.1 finding 3).

    The ORIGINAL docstring said "under ruling ④ the AI can only be MORE
    conservative, so ``ai_more_permissive`` should never fire on a contested
    decision". That is wrong as a matter of code, independently of any corpus:
    ruling ④'s "more conservative" holds relative to **guard + validators** (a
    failure never flips authority), NOT relative to the **ladder**, whose
    completeness threshold family S5 retires. ``ai_more_permissive`` is
    therefore the expected, load-bearing signal — it counts the authority flips
    the S5 conjunction would newly permit — and the scripted-client replay does
    fire it (design §7.1 records the counts and their provenance).

    The FIRST correction then over-reached in the other direction by asserting
    that "a SCRIPTED judge is more permissive than any plausible real one by
    construction". Nothing about the scripted client constrains a real model's
    raw verdict, and this batch has zero real-model evidence. What IS provable —
    and is the claim to rely on — is a CEILING, not a comparison of models:

        the scripted judge's only two reject arms
        (``prior_snapshot_comparison.simulate_or_placeholder_provenance`` and
        ``candidate.metrics.effective_lane_total < incumbent``) are exactly the
        predicates the retained battery re-checks (V_PROV / V_COMP), so under
        the S5 conjunction ``promote ⟺ guard-pass AND AI-approve AND
        validators-pass`` a scripted promote survives iff the guard-∧-battery
        floor admits it. The scripted ``ai_more_permissive`` count therefore
        equals that floor's admission count, which NO judge — scripted or real —
        can exceed.

    Read ``ai_more_permissive`` as an UPPER BOUND on the authority churn the S5
    conjunction permits, i.e. a measurement of FLOOR STRENGTH. It says nothing
    about how a real model would behave inside that bound."""
    agreement = bool(ladder_promote) == bool(ai_promote)
    if not engaged:
        divergence = "not_engaged"
    elif agreement:
        divergence = "agree"
    elif ladder_promote and not ai_promote:
        divergence = "ai_more_conservative"
    else:
        divergence = "ai_more_permissive"
    return {
        "contested": bool(contested),
        "ladder_promote": bool(ladder_promote),
        "ladder_reason": str(ladder_reason or ""),
        "ai_promote": bool(ai_promote),
        "agreement": agreement,
        "divergence": divergence,
    }


def record_organization_promote_shadow(
    model_client: Any,
    *,
    existing_authoritative: Mapping[str, Any] | None,
    candidate_record: Mapping[str, Any],
    ladder_decision: Mapping[str, Any],
    completeness_regression_tolerance: float | None = None,
) -> dict[str, Any] | None:
    """One S3 shadow record per promote decision — records, NEVER changes the
    live promote decision or which row becomes authoritative.

    Called from the ``upsert_organization_asset_registry_with_guard`` seam AFTER
    the ladder decision and the store write, with the SAME (post-coverage)
    ``candidate_record`` that was upserted and the ``existing_authoritative`` row.
    Both are read-only copies here; nothing mutates them and this never touches
    the store, so authority is byte-identical with the shadow on or off (S3 hard
    rule).

    Non-invocation (returns None — no model call, no record):
      * no judge-capable client (default simulate/replay ``OfflineModelClient`` /
        no client — condition (a), design §3.1);
      * a judge-capable client that is NOT declared billing-free while the
        dedicated opt-in ``SOURCING_WS7_PROMOTE_SHADOW_ALLOW_REAL_MODEL`` is
        unset — the SAFETY gate (see
        :func:`promote_shadow_model_calls_permitted`). This seam runs inside the
        authoritative write path, so a real provider client must never be
        callable here by mere threading; the opt-in is OFF by default.

    Otherwise returns the shadow record. Engagement (OQ4):
      * a CONTESTED decision (a non-pre-branch ladder reason) drives the S2 helper
        with the full descriptor + read-only guard-predicted verdict + incumbent
        (one model call).
      * a NON-CONTESTED decision (a deterministic pre-branch: no-incumbent /
        same-snapshot-refresh / lifecycle-not-promotable) records a SKIP shadow
        (engaged=False, ``skip_reason``) WITHOUT a model call.

    Any exception is caught and returned AS the record
    (``shadow_status="shadow_error"``) — the S3 hard rule is that the shadow path
    can never affect the live promote decision or the registry write.
    """
    try:
        if not model_client_supports_promote_judgment(model_client):
            return None
        # SAFETY gate (2026-07-25): capability is not permission. Without the
        # dedicated opt-in a non-billing-free client never reaches the model.
        if not promote_shadow_model_calls_permitted(model_client):
            return None

        ladder = dict(ladder_decision or {})
        ladder_reason = str(ladder.get("reason") or "")
        ladder_promote = bool(ladder.get("promote"))
        contested = ladder_reason not in _DETERMINISTIC_PREBRANCH_REASONS
        deterministic_promote = ladder_reason in _DETERMINISTIC_PROMOTE_REASONS

        incumbent_row = dict(existing_authoritative or {})
        candidate_row = dict(candidate_record or {})

        if not contested:
            # OQ4: deterministic pre-branch — no model call, no descriptor build.
            result = judge_and_validate_promotion(
                model_client,
                candidate_descriptor={},
                guard_predicted_verdict={"refused": False, "reason": ""},
                incumbent_descriptor={},
                contested=False,
                deterministic_promote=deterministic_promote,
            )
            guard_verdict: dict[str, Any] | None = None
        else:
            incumbent_snapshot_id = str(incumbent_row.get("snapshot_id") or "")
            incumbent_descriptor = _snapshot_descriptor_from_registry_row(incumbent_row)
            candidate_snapshot = _snapshot_descriptor_from_registry_row(
                candidate_row, incumbent_snapshot_id=incumbent_snapshot_id
            )
            # The read-only guard-predicted verdict (mirror of storage.py:8670-8681);
            # the model never sees it — it flows to the S1 battery as a parameter.
            guard_verdict = predict_lineage_guard_refusal(
                incoming_snapshot_id=candidate_snapshot["snapshot_id"],
                incoming_generation_key=candidate_snapshot["materialization_generation_key"],
                incoming_generation_sequence=candidate_snapshot["materialization_generation_sequence"],
                incoming_selected_snapshot_ids=candidate_snapshot["selected_snapshot_ids"],
                incumbent_snapshot_id=incumbent_descriptor["snapshot_id"],
                incumbent_generation_key=incumbent_descriptor["materialization_generation_key"],
                incumbent_generation_sequence=incumbent_descriptor["materialization_generation_sequence"],
                incumbent_selected_snapshot_ids=incumbent_descriptor["selected_snapshot_ids"],
            )
            candidate_descriptor = _shadow_candidate_descriptor(
                incumbent_descriptor=incumbent_descriptor, candidate_descriptor=candidate_snapshot
            )
            helper_kwargs: dict[str, Any] = {}
            if completeness_regression_tolerance is not None:
                helper_kwargs["completeness_regression_tolerance"] = completeness_regression_tolerance
            result = judge_and_validate_promotion(
                model_client,
                candidate_descriptor=candidate_descriptor,
                guard_predicted_verdict=guard_verdict,
                incumbent_descriptor=incumbent_descriptor,
                contested=True,
                **helper_kwargs,
            )

        engaged = bool(result["engaged"])
        ai_promote = str(result["status"]) == STATUS_PROMOTED
        decision = result.get("decision")
        audit = result.get("audit")
        return {
            "kind": SHADOW_RECORD_KIND,
            "mode": "shadow",
            "engaged": engaged,
            "contested": bool(result["contested"]),
            "skip_reason": str(result.get("skip_reason") or ""),
            "decision_id": str(result["decision_id"]),
            "ai_status": str(result["status"]),
            "decision": dict(decision) if isinstance(decision, Mapping) else None,
            "audit": dict(audit) if isinstance(audit, Mapping) else None,
            "guard_predicted_verdict": dict(guard_verdict) if isinstance(guard_verdict, Mapping) else None,
            "ladder_comparison": _shadow_ladder_comparison(
                ladder_promote=ladder_promote,
                ladder_reason=ladder_reason,
                ai_promote=ai_promote,
                contested=contested,
                engaged=engaged,
            ),
        }
    except Exception as exc:  # noqa: BLE001 — S3 hard rule: shadow failure NEVER affects the live promote decision or the registry write
        return {
            "kind": SHADOW_RECORD_KIND,
            "mode": "shadow",
            "shadow_status": SHADOW_STATUS_ERROR,
            "engaged": False,
            "shadow_error": " ".join(str(exc or "").strip().split())[:MAX_JUDGE_ERROR_LENGTH],
            "shadow_error_type": type(exc).__name__,
        }


# Re-export the validator-rejection reason helper so an S3/S5 caller mapping a
# late (apply-time) validator failure names the same F5 fallback_reason string.
__all__ = [
    "STATUS_PROMOTED",
    "STATUS_KEPT_INCUMBENT",
    "SKIP_REASON_NOT_CONTESTED",
    "SHADOW_RECORD_KEY",
    "SHADOW_RECORD_KIND",
    "SHADOW_STATUS_ERROR",
    "build_promote_judge_input_payload",
    "build_keep_incumbent_audit",
    "stale_input_keep_incumbent_audit",
    "judge_and_validate_promotion",
    "model_client_supports_promote_judgment",
    "promote_shadow_model_calls_permitted",
    "record_organization_promote_shadow",
    "fallback_reason_for_validator",
]
