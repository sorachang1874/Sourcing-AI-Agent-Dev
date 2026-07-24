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
    ModelClient,
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


# Re-export the validator-rejection reason helper so an S3/S5 caller mapping a
# late (apply-time) validator failure names the same F5 fallback_reason string.
__all__ = [
    "STATUS_PROMOTED",
    "STATUS_KEPT_INCUMBENT",
    "SKIP_REASON_NOT_CONTESTED",
    "build_promote_judge_input_payload",
    "build_keep_incumbent_audit",
    "stale_input_keep_incumbent_audit",
    "judge_and_validate_promotion",
    "fallback_reason_for_validator",
]
