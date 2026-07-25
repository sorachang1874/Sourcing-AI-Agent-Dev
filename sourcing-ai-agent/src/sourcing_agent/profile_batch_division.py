"""Divider orchestration helper (WS7/W7.2 slices S2+S3, ADDITIVE ONLY).

Spec: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §2 (model invocation surface), §3
(ruling-④ failure classes F1-F6), OQ1/OQ5/OQ6/OQ7/OQ8 RATIFIED 2026-07-23.
This module is the thin seam between the ModelClient divider method
(`divide_profile_prefetch_batches`, model_provider.py) and the S1 contract
(`profile_batch_division_contract`): it builds the OQ1 input payload (index
ranges + digests only — NEVER raw URLs), calls the model ONCE per wave mint
(OQ6; idempotency/locking is the enrichment mint seam's scheduler-lock duty),
assembles the caller-owned envelope fields (division_id, membership_sha256,
provenance passthrough), and validates through the single-sourced S1 battery.
Slice S3 adds `record_profile_prefetch_division_shadow` — the SHADOW hook the
enrichment.py mint seam calls to RECORD a division proposal beside the
ladder-built plan; it never produces `dispatch_item_specs` and dispatch never
reads its output (the flip is slice S5).
Slice S4 extends the R6 durable wave identity with `refill_plan_division_id`
(design §4.3/OQ6): `shadow_plan_division_id` threads the shadow proposal's id
into the plan-record write (so the hook now runs right BEFORE
`_record_profile_prefetch_batch_plan_items`), `inherited_division_wave_identity`
carries the id through the scalar R6 claim, and
`inherit_heterogeneous_division_windows` is the pure-function per-batch
inheritance for AI divisions — structurally unreachable until the S5 flip
passes a division into the plan.

Responsibility split (design §1.2 read with §2.3): the model authors ONLY the
``batches`` list; every other ai_batch_division.v1 field is caller-authored
fact (wave identity, membership hash over the caller's inventory keys, the
client's own call provenance). Assembling that envelope is therefore not
"repairing" model output — the raw batches pass through byte-identical and any
defect in them surfaces as the auditable F4/F5 fallback, never as a fix-up.

Engagement policy (OQ5): the divider engages only when the eligible
(non-retry_wait) ready set exceeds 300 urls. At or below the threshold the
helper returns an immediate structural fallback record WITHOUT calling the
model — deliberate policy non-engagement, not a ruling-④ failure class, so it
carries ``skip_reason`` instead of an F-audit.

SHADOW SAFETY GATE (S3, added 2026-07-25 — design §5.1). The S3 shadow hook
runs SYNCHRONOUSLY inside the enrichment mint seam while the scheduler lock is
held. Its exposure is NOT the authoritative write path (dispatch never reads the
record, and the ladder-built plan is byte-identical with the shadow on or off) —
the exposure is a BILLED provider call plus up to a 20 s judge timeout inside a
lock-held scheduling hot spot, once per wave mint above the OQ5 threshold. That
is real enough to gate identically to the promote seam, so the hook requires
capability (`model_client_supports_batch_division`) AND permission
(`divider_shadow_model_calls_permitted`): a client whose class declares
`ws7_shadow_billing_free = True` is always permitted; anything else needs the
dedicated opt-in `SOURCING_WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL`, OFF by default.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Collection, Mapping, Sequence
from typing import Any

from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .model_provider import (
    PROFILE_BATCH_DIVIDER_CIRCUIT_ERROR_PREFIX,
    PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_RAW_PREVIEW_KEY,
    WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV,
    DeterministicModelClient,
    ModelClient,
    ws7_shadow_model_calls_permitted,
)
from .profile_batch_division_contract import (
    ACTOR_SLOT_URL_TARGET,
    AI_DIVIDER_MAX_BATCH_COUNT,
    AI_DIVIDER_MIN_BATCH_COUNT,
    DEFAULT_ACTOR_GLOBAL_INFLIGHT,
    DIVISION_SOURCE_AI_DIVIDER,
    DIVISION_SOURCE_RULE_LADDER_FALLBACK,
    FALLBACK_REASON_CALL_FAILED,
    FALLBACK_REASON_CIRCUIT_OPEN,
    FALLBACK_REASON_INPUT_STALE,
    FALLBACK_REASON_INVALID_OUTPUT,
    FALLBACK_REASON_MODEL_UNAVAILABLE,
    MAX_ACTOR_WAVE_ROUNDS,
    MAX_BATCH_COUNT_PER_WAVE,
    MAX_DIVIDER_ERROR_LENGTH,
    PROVIDER_ENVELOPE_MAX_URLS,
    SCHEMA_FAILURE_VALIDATOR_ID,
    SCHEMA_ID_V1,
    TINY_BATCH_LEGAL_REASON_CODES,
    VALIDATOR_RESULT_STATUS_FAIL,
    VALIDATOR_RESULT_STATUS_PASS,
    VALIDATOR_V5_WORKER_BUDGET,
    VALIDATOR_V6_WAVE_MINT_ONLY,
    BatchDivisionContractError,
    compute_membership_sha256,
    fallback_reason_for_validator,
    normalize_fallback_audit,
    validate_ai_batch_division,
    validate_v5_worker_budget,
    validate_v6_wave_mint_only,
)
from .runtime_tuning import resolved_harvest_profile_actor_global_inflight

# OQ5 RATIFIED 2026-07-23: engage the AI divider only above the single-envelope
# band — exactly the provider envelope cap (>300 urls), where the ladder starts
# multi-batch splitting and the ruling-① [4, 8] band is meaningful.
AI_DIVIDER_ENGAGEMENT_THRESHOLD_URLS = PROVIDER_ENVELOPE_MAX_URLS

DIVISION_PROPOSAL_STATUS_PROPOSED = "proposed"
DIVISION_PROPOSAL_STATUS_FALLBACK = "fallback"
SKIP_REASON_BELOW_ENGAGEMENT_THRESHOLD = "ready_set_at_or_below_engagement_threshold"

# Wire-shape rule for the model's raw output (S2 invocation-surface contract,
# design §2.3 first paragraph): the model authors exactly one top-level key.
_MODEL_OUTPUT_REQUIRED_KEY = "batches"


def _inventory_url_keys(inventory: Sequence[Mapping[str, Any] | str]) -> list[str]:
    """Extract the canonically ordered normalized url keys from the inventory.

    Entries are either the url key itself or a mapping carrying ``url_key``
    (plus optional descriptor fields: source_shards, queue_state,
    attempt_count, last_failure_class, priority).
    """
    keys: list[str] = []
    for position, entry in enumerate(inventory):
        if isinstance(entry, str):
            key = entry.strip()
        elif isinstance(entry, Mapping):
            key = str(entry.get("url_key") or "").strip()
        else:
            raise ValueError(f"inventory[{position}] must be a url key or a mapping with url_key")
        if not key:
            raise ValueError(f"inventory[{position}] has an empty url_key")
        keys.append(key)
    return keys


def _entry_descriptor(entry: Mapping[str, Any] | str) -> tuple[str, tuple[str, ...], str]:
    if not isinstance(entry, Mapping):
        return ("", (), "")
    shards = tuple(sorted(str(item) for item in (entry.get("source_shards") or []) if str(item).strip()))
    return (
        str(entry.get("queue_state") or ""),
        shards,
        str(entry.get("last_failure_class") or ""),
    )


def _inventory_groups(inventory: Sequence[Mapping[str, Any] | str]) -> list[dict[str, Any]]:
    """Collapse the inventory to contiguous per-(state, shards, failure) runs.

    Design §2.2: the model answers in index ranges either way, so membership
    exactness is preserved while a 3,000-url set never round-trips through the
    model verbatim.
    """
    groups: list[dict[str, Any]] = []
    current_key: tuple[str, tuple[str, ...], str] | None = None
    for index, entry in enumerate(inventory):
        descriptor = _entry_descriptor(entry)
        attempt_count = 0
        if isinstance(entry, Mapping):
            try:
                attempt_count = max(0, int(entry.get("attempt_count") or 0))
            except (TypeError, ValueError):
                attempt_count = 0
        if descriptor == current_key and groups:
            groups[-1]["index_range"][1] = index
            groups[-1]["count"] += 1
            groups[-1]["attempt_count_max"] = max(groups[-1]["attempt_count_max"], attempt_count)
            continue
        current_key = descriptor
        queue_state, shards, last_failure_class = descriptor
        groups.append(
            {
                "index_range": [index, index],
                "count": 1,
                "queue_state": queue_state,
                "source_shards": list(shards),
                "last_failure_class": last_failure_class,
                "attempt_count_max": attempt_count,
            }
        )
    return groups


def build_divider_input_payload(
    *,
    inventory: Sequence[Mapping[str, Any] | str],
    division_id: str,
    retry_wait_indices: Collection[int],
    actor_global_inflight: int,
    failure_history: Mapping[str, Any] | None = None,
    prior_round_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the OQ1 divider input payload (design §2.2).

    Full input by ruling: inventory descriptor (sizes + ordering digest +
    aggregate groups — no URL echo), url-level failure-history summary,
    prior-round context, and budget parameters incl. the validator constants
    the acceptance battery will hold the answer to.
    """
    url_keys = _inventory_url_keys(inventory)
    size = len(url_keys)
    retry_set = sorted({int(index) for index in retry_wait_indices if 0 <= int(index) < size})
    ordering_digest = hashlib.sha256("\n".join(url_keys).encode("utf-8")).hexdigest()
    return {
        "task": "profile_prefetch_batch_division",
        "output_contract_schema_id": SCHEMA_ID_V1,
        "division_id": str(division_id),
        "inventory": {
            "size": size,
            "ordering_digest_sha256": ordering_digest,
            "groups": _inventory_groups(inventory),
        },
        "retry_wait": {"count": len(retry_set), "indices": retry_set},
        "failure_history": dict(failure_history or {}),
        "prior_round_context": dict(prior_round_context or {}),
        "budget": {
            "actor_global_inflight": int(actor_global_inflight),
            "provider_envelope_max_urls": PROVIDER_ENVELOPE_MAX_URLS,
            "max_batch_count_per_wave": MAX_BATCH_COUNT_PER_WAVE,
            "ai_batch_count_band": [AI_DIVIDER_MIN_BATCH_COUNT, AI_DIVIDER_MAX_BATCH_COUNT],
            "max_actor_wave_rounds": MAX_ACTOR_WAVE_ROUNDS,
            "min_non_tiny_batch_size": ACTOR_SLOT_URL_TARGET,
            "legal_tiny_reason_codes": sorted(TINY_BATCH_LEGAL_REASON_CODES),
        },
    }


def division_membership_sha256(
    batches: Sequence[Mapping[str, Any]],
    url_keys: Sequence[str],
) -> str:
    """The design-§1.2 membership hash for a batches list over the inventory.

    Out-of-bounds indices map to a stable placeholder token instead of raising:
    hashing must never pre-empt the single-sourced S1 validation (V3 rejects
    such a division right after, and the hash of a rejected division is never
    consumed). Structurally unreadable ranges hash the raw batches JSON so the
    envelope still carries a well-formed 64-hex identity into strict parsing.
    """
    try:
        member_keys: list[list[str]] = []
        for batch in batches:
            batch_keys: list[str] = []
            for raw_range in batch["member_index_ranges"]:
                start, end = int(raw_range[0]), int(raw_range[1])
                for index in range(start, end + 1):
                    if 0 <= index < len(url_keys):
                        batch_keys.append(url_keys[index])
                    else:
                        batch_keys.append(f"__out_of_bounds_{index}__")
            member_keys.append(batch_keys)
        return compute_membership_sha256(member_keys)
    except (KeyError, IndexError, TypeError, ValueError, OverflowError):
        canonical = repr(batches).encode("utf-8", errors="replace")
        return hashlib.sha256(canonical).hexdigest()


def build_fallback_audit(
    fallback_reason: str,
    *,
    divider_error: str = "",
    validator_results: Sequence[Mapping[str, Any]] = (),
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build + strictly normalize one ruling-④ fallback audit payload (design §3).

    Single builder for every failure class F1-F6 (F6 `divider_input_stale` is
    exported for the S3/S5 apply path — within one synchronous propose call the
    input cannot go stale). A structurally invalid client ``provenance`` is
    dropped to null with an explicit note appended to ``divider_error`` — the
    audit itself must never fail to record.
    """
    truncated_error = " ".join(str(divider_error or "").strip().split())[:MAX_DIVIDER_ERROR_LENGTH]
    payload: dict[str, Any] = {
        "division_source": DIVISION_SOURCE_RULE_LADDER_FALLBACK,
        "fallback_reason": str(fallback_reason),
        "divider_error": truncated_error,
        "validator_results": [dict(entry) for entry in validator_results],
        "provenance": dict(provenance) if isinstance(provenance, Mapping) else None,
    }
    try:
        return normalize_fallback_audit(payload).to_payload()
    except BatchDivisionContractError:
        if payload["provenance"] is None:
            raise
        note = "; provenance dropped: client payload failed strict audit shape"
        payload["provenance"] = None
        payload["divider_error"] = (truncated_error + note)[:MAX_DIVIDER_ERROR_LENGTH]
        return normalize_fallback_audit(payload).to_payload()


def stale_input_fallback_audit(
    *,
    recorded_membership_sha256: str,
    current_membership_sha256: str,
    provenance: Mapping[str, Any] | None = None,
    validator_results: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """F6 audit for the apply path (design §3): membership hash mismatch at apply."""
    return build_fallback_audit(
        FALLBACK_REASON_INPUT_STALE,
        divider_error=(
            "ready set changed between input snapshot and apply: recorded membership "
            f"{recorded_membership_sha256} != current {current_membership_sha256}"
        ),
        validator_results=validator_results,
        provenance=provenance,
    )


def _proposal(
    *,
    status: str,
    engaged: bool,
    division: dict[str, Any] | None,
    fallback_audit: dict[str, Any] | None,
    division_id: str,
    eligible_member_count: int,
    engagement_threshold_urls: int,
    skip_reason: str = "",
    validator_results: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return {
        "status": status,
        "engaged": engaged,
        "division": division,
        "fallback_audit": fallback_audit,
        "division_id": division_id,
        "eligible_member_count": eligible_member_count,
        "engagement_threshold_urls": engagement_threshold_urls,
        "skip_reason": skip_reason,
        "validator_results": [dict(entry) for entry in validator_results],
    }


def propose_and_validate_division(
    model_client: ModelClient,
    *,
    inventory: Sequence[Mapping[str, Any] | str],
    failure_history: Mapping[str, Any] | None = None,
    prior_round_context: Mapping[str, Any] | None = None,
    retry_wait_indices: Collection[int] = (),
    actor_global_inflight: int = DEFAULT_ACTOR_GLOBAL_INFLIGHT,
    engagement_threshold_urls: int = AI_DIVIDER_ENGAGEMENT_THRESHOLD_URLS,
) -> dict[str, Any]:
    """One divider proposal per wave mint (OQ5/OQ6): call once, validate via S1.

    Returns ``{"status": "proposed"|"fallback", "engaged", "division",
    "fallback_audit", "division_id", ...}``. ``status="proposed"`` carries the
    validated ai_batch_division.v1 payload (battery results filled in);
    ``status="fallback"`` carries either the OQ5 non-engagement skip record
    (``engaged=False``, no model call, no F-audit) or the ruling-④ F1-F5 audit
    mapped from the failure:

    - F1 `divider_model_unavailable`: structural ``{}`` response
      (DeterministicModelClient / OfflineModelClient / opt-in absent).
    - F2 `divider_circuit_open`: error string with the shared circuit prefix.
    - F3 `divider_call_failed`: any other transport/timeout error string.
    - F4 `divider_invalid_output`: non-JSON output, wire-shape violation
      (anything but exactly ``{"batches": [...]}``), or S1 strict-parse
      rejection (``validator_id="schema"``).
    - F5 `divider_validator_rejected:<validator>`: any V1-V10 battery FAIL.

    F6 is apply-time (see ``stale_input_fallback_audit``); the caller applying
    a recorded division re-checks membership against the live ready set.
    """
    provider = model_client.provider_name()
    url_keys = _inventory_url_keys(inventory)
    size = len(url_keys)
    retry_set = {int(index) for index in retry_wait_indices if 0 <= int(index) < size}
    eligible_member_count = size - len(retry_set)
    threshold = int(engagement_threshold_urls)
    if eligible_member_count <= threshold:
        return _proposal(
            status=DIVISION_PROPOSAL_STATUS_FALLBACK,
            engaged=False,
            division=None,
            fallback_audit=None,
            division_id="",
            eligible_member_count=eligible_member_count,
            engagement_threshold_urls=threshold,
            skip_reason=SKIP_REASON_BELOW_ENGAGEMENT_THRESHOLD,
        )

    division_id = uuid.uuid4().hex
    payload = build_divider_input_payload(
        inventory=inventory,
        division_id=division_id,
        retry_wait_indices=retry_set,
        actor_global_inflight=actor_global_inflight,
        failure_history=failure_history,
        prior_round_context=prior_round_context,
    )
    response = model_client.divide_profile_prefetch_batches(payload)

    def _fallback(
        fallback_reason: str,
        *,
        divider_error: str,
        validator_results: Sequence[Mapping[str, Any]] = (),
        provenance: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        return _proposal(
            status=DIVISION_PROPOSAL_STATUS_FALLBACK,
            engaged=True,
            division=None,
            fallback_audit=build_fallback_audit(
                fallback_reason,
                divider_error=divider_error,
                validator_results=validator_results,
                provenance=provenance,
            ),
            division_id=division_id,
            eligible_member_count=eligible_member_count,
            engagement_threshold_urls=threshold,
            validator_results=validator_results,
        )

    if not isinstance(response, Mapping) or not response:
        return _fallback(
            FALLBACK_REASON_MODEL_UNAVAILABLE,
            divider_error=f"model client returned no division (provider: {provider})",
        )
    error_text = str(response.get(PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY) or "")
    if error_text:
        reason = (
            FALLBACK_REASON_CIRCUIT_OPEN
            if error_text.startswith(PROFILE_BATCH_DIVIDER_CIRCUIT_ERROR_PREFIX)
            else FALLBACK_REASON_CALL_FAILED
        )
        return _fallback(reason, divider_error=error_text)

    raw_provenance = response.get(PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY)
    provenance = dict(raw_provenance) if isinstance(raw_provenance, Mapping) else None
    raw_preview = str(response.get(PROFILE_BATCH_DIVIDER_RESPONSE_RAW_PREVIEW_KEY) or "")
    division_raw = response.get(PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY)
    if not isinstance(division_raw, Mapping) or not division_raw:
        return _fallback(
            FALLBACK_REASON_INVALID_OUTPUT,
            divider_error=("model produced no JSON object output" + (f"; raw: {raw_preview}" if raw_preview else "")),
            provenance=provenance,
        )
    unexpected_keys = sorted(set(str(key) for key in division_raw.keys()) - {_MODEL_OUTPUT_REQUIRED_KEY})
    raw_batches = division_raw.get(_MODEL_OUTPUT_REQUIRED_KEY)
    if unexpected_keys or not isinstance(raw_batches, Sequence) or isinstance(raw_batches, (str, bytes)):
        return _fallback(
            FALLBACK_REASON_INVALID_OUTPUT,
            divider_error=(
                'model output must be exactly {"batches": [...]}; '
                f"unexpected keys {unexpected_keys}, batches type {type(raw_batches).__name__}"
            ),
            provenance=provenance,
        )

    batches = list(raw_batches)
    envelope: dict[str, Any] = {
        "schema_id": SCHEMA_ID_V1,
        "division_id": division_id,
        "division_source": DIVISION_SOURCE_AI_DIVIDER,
        "batch_count": len(batches),
        "batches": batches,
        "membership_sha256": division_membership_sha256(
            [batch for batch in batches if isinstance(batch, Mapping)], url_keys
        ),
        "provenance": provenance,
        "validator_results": [],
        "fallback": None,
    }
    result = validate_ai_batch_division(
        envelope,
        inventory_size=size,
        retry_wait_indices=retry_set,
        actor_global_inflight=actor_global_inflight,
    )
    if result["valid"]:
        division_payload = dict(result["normalized"])
        division_payload["validator_results"] = list(result["validator_results"])
        return _proposal(
            status=DIVISION_PROPOSAL_STATUS_PROPOSED,
            engaged=True,
            division=division_payload,
            fallback_audit=None,
            division_id=division_id,
            eligible_member_count=eligible_member_count,
            engagement_threshold_urls=threshold,
            validator_results=result["validator_results"],
        )
    first_failure = result["failures"][0]
    failure_reason = str(first_failure.get("reason") or "")
    if first_failure.get("validator_id") == SCHEMA_FAILURE_VALIDATOR_ID:
        return _fallback(
            FALLBACK_REASON_INVALID_OUTPUT,
            divider_error=failure_reason,
            validator_results=result["validator_results"],
            provenance=provenance,
        )
    return _fallback(
        fallback_reason_for_validator(str(first_failure["validator_id"])),
        divider_error=failure_reason,
        validator_results=result["validator_results"],
        provenance=provenance,
    )


# ---------------------------------------------------------------------------
# W7.2 slice S3 — SHADOW integration (records, NEVER drives dispatch).
#
# Design §5.2/§7 S3 + discrepancy D1: the shadow record lives on the
# `refill_plan_items` activity surface next to
# `_record_profile_prefetch_batch_plan_items` (enrichment.py mint seam), NEVER
# inside the oracle-pinned plan record (`ProfilePrefetchBatchPlan.to_record()`
# whole-dict compare, test_fetch_profile_batch_characterization.py:142-192).
# The additive plan-record key + schema_version bump are the S5 flip's job.
# ---------------------------------------------------------------------------

SHADOW_RECORD_KIND = "profile_prefetch_ai_batch_division_shadow"
SHADOW_STATUS_ERROR = "shadow_error"
# Ladder literal for an R6 durable-wave-inherited window (enrichment.py:1211);
# such a window belongs to an in-flight wave, which is never (re-)divided
# (design §4.5 / OQ6).
DURABLE_WAVE_BATCH_SIZE_REASON = "durable_refill_wave_batch_size"
# S4 registry contract field (design §4.3, LANDED 2026-07-23, migration 0015):
# minted at the plan-record moment from the shadow proposal's division id and
# cleared in lockstep with the scalar wave identity, so V6's live set is
# non-empty exactly while a divided wave is in flight.
REGISTRY_DIVISION_ID_FIELD = "refill_plan_division_id"
RETRY_WAIT_QUEUE_STATE = "retry_wait"
# The registry queue states that mean "this item belongs to a live durable
# wave" — the same set the scalar R6 inheritance scan uses
# (enrichment.py `_apply_durable_refill_wave_dispatch_window`).
DURABLE_WAVE_QUEUE_STATES = frozenset(
    {"deferred_budget", "deferred_coalescing", "dispatch_reserved", "dispatch_claimed"}
)
# Additive dispatch-window key the scalar R6 claim carries the division id under
# (absent when the durable wave has no division id — every pre-S4 row and every
# ladder-only wave — so the oracle's R6 goldens stay byte-identical).
DURABLE_WAVE_DIVISION_ID_WINDOW_KEY = "durable_refill_wave_division_id"


def model_client_supports_batch_division(model_client: Any) -> bool:
    """True when the client overrides the deterministic F1 divider stub.

    Structural capability check for the S3 condition (a) "scripted/live model
    client available": `DeterministicModelClient.divide_profile_prefetch_batches`
    (and its `OfflineModelClient` inheritance) is the structural
    "divider unavailable" marker (design §2.1 / discrepancy D2) — calling it on
    every wave would only mint F1 audit noise, so the shadow hook does not
    engage at all for such clients.
    """
    if model_client is None:
        return False
    method = getattr(type(model_client), "divide_profile_prefetch_batches", None)
    if method is None:
        return False
    return method is not DeterministicModelClient.divide_profile_prefetch_batches


def divider_shadow_model_calls_permitted(model_client: Any) -> bool:
    """SAFETY gate — may the mint-seam shadow actually CALL this client? (2026-07-25)

    Mirror of ``organization_promote_judgment.promote_shadow_model_calls_permitted``.
    Capability is not permission: ``model_client_supports_batch_division`` is
    True for the REAL provider clients too, and this hook runs synchronously
    inside the enrichment mint seam under the scheduler lock. So a client that
    does not declare ``ws7_shadow_billing_free = True`` additionally requires the
    dedicated opt-in env ``SOURCING_WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL``, which
    is OFF by default and is deliberately not one of the three live-provider gate
    vars.

    EXPOSURE NOTE (differs from the promote seam — recorded honestly rather than
    equated): the divider shadow is on the REFILL/mint path, not the
    authoritative write path. Nothing it returns can change dispatch, and the
    ladder plan is byte-identical with it on or off. What an ungated real client
    WOULD have cost is a billed provider call plus up to the 20 s divider timeout
    per wave mint >300 eligible urls, taken while the scheduler lock is held —
    a latency/cost exposure on a scheduling hot spot rather than a correctness
    exposure on authority. Gated identically anyway: an optional record-only path
    must never be the thing that starts billing.
    """

    return ws7_shadow_model_calls_permitted(model_client, allow_real_model_env=WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL_ENV)


def _shadow_item_url_key(item: Any) -> str:
    return str(getattr(item, "registry_key", "") or "").strip() or str(getattr(item, "profile_url", "") or "").strip()


def _shadow_apply_time_validator_results(
    division: Any,
    *,
    worker_budget: Mapping[str, Any],
    registry_entries: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """V5/V6 apply-time wiring at the mint seam (S1 left both `skipped`).

    V5: the post-R5 dispatched batch count a real apply would produce is
    `min(batch_count, available_new_worker_count)` (surplus defers — exactly
    the R5 split the oracle pins); recorded as a structural tripwire, the same
    check the S5 apply path will run. V6: evaluated over the division ids
    recorded on the dispatch set's registry items; pre-S4 no writer exists so
    the live set is empty by construction (recorded honestly in the note).
    """
    if not isinstance(division, Mapping):
        return []
    available = max(0, int(dict(worker_budget or {}).get("available_new_worker_count") or 0))
    batch_count = max(0, int(division.get("batch_count") or 0))
    simulated_dispatched = min(batch_count, available)
    v5_failure = validate_v5_worker_budget(simulated_dispatched, available_new_worker_count=available)
    live_division_ids = {
        str(dict(entry or {}).get(REGISTRY_DIVISION_ID_FIELD) or "").strip()
        for entry in dict(registry_entries or {}).values()
    } - {""}
    v6_failure = validate_v6_wave_mint_only(str(division.get("division_id") or ""), live_division_ids=live_division_ids)
    results: list[dict[str, Any]] = []
    for validator_id, failure, note in (
        (
            VALIDATOR_V5_WORKER_BUDGET,
            v5_failure,
            (
                f"shadow-simulated R5 apply: min(batch_count={batch_count}, "
                f"available_new_worker_count={available}) = {simulated_dispatched} dispatched, surplus defers"
            ),
        ),
        (
            VALIDATOR_V6_WAVE_MINT_ONLY,
            v6_failure,
            (
                f"live division ids from registry field {REGISTRY_DIVISION_ID_FIELD!r}: "
                f"{sorted(live_division_ids) or 'none recorded (no live durable wave carries a division id)'}"
            ),
        ),
    ):
        entry: dict[str, Any] = {
            "validator": validator_id,
            "status": VALIDATOR_RESULT_STATUS_FAIL if failure else VALIDATOR_RESULT_STATUS_PASS,
            "reason": str(failure) if failure else note,
        }
        results.append(entry)
    return results


def _shadow_ladder_comparison(plan: Any, division: Any) -> dict[str, Any]:
    """Divergence digest: the ladder's ACTUAL division vs the shadow proposal.

    The ladder side is the dispatched (post-R5) partition plus deferred/tail
    counts; the AI side covers the whole eligible set, so
    `dispatched_membership_identical` is only True when the AI division equals
    the fully-dispatched ladder partition — the before/after evidence counter
    the S5 flip decision consumes (design §5.2).
    """
    dispatched_key_chunks = [
        [_shadow_item_url_key(item) for item in list(chunk or [])]
        for _, chunk in list(getattr(plan, "dispatch_item_specs", None) or [])
    ]
    ladder_membership = compute_membership_sha256(dispatched_key_chunks)
    payload: dict[str, Any] = {
        "ladder_dispatched_batch_count": len(dispatched_key_chunks),
        "ladder_dispatched_batch_sizes": [len(chunk) for chunk in dispatched_key_chunks],
        "ladder_deferred_item_count": len(list(getattr(plan, "deferred_items", None) or [])),
        "ladder_tail_coalescing_item_count": len(list(getattr(plan, "tail_coalescing_items", None) or [])),
        "ladder_dispatched_membership_sha256": ladder_membership,
        "ai_batch_count": None,
        "ai_batch_sizes": None,
        "ai_membership_sha256": None,
        "batch_count_delta": None,
        "dispatched_membership_identical": None,
    }
    if isinstance(division, Mapping):
        ai_batch_count = max(0, int(division.get("batch_count") or 0))
        ai_membership = str(division.get("membership_sha256") or "")
        payload["ai_batch_count"] = ai_batch_count
        payload["ai_batch_sizes"] = [
            max(0, int(dict(batch or {}).get("member_count") or 0)) for batch in list(division.get("batches") or [])
        ]
        payload["ai_membership_sha256"] = ai_membership
        payload["batch_count_delta"] = ai_batch_count - len(dispatched_key_chunks)
        payload["dispatched_membership_identical"] = bool(ai_membership) and ai_membership == ladder_membership
    return payload


def record_profile_prefetch_division_shadow(
    model_client: Any,
    *,
    plan: Any,
    registry_entries: Mapping[str, Mapping[str, Any]] | None = None,
    runtime_tuning_context: Mapping[str, Any] | None = None,
    wave_mint_provider_submit: bool = True,
) -> dict[str, Any] | None:
    """One S3 shadow record per wave mint — records, NEVER drives dispatch.

    Called from the enrichment.py mint seam (inside the scheduler lock, right
    BEFORE ``_record_profile_prefetch_batch_plan_items`` since S4, so the
    proposal's division id can ride the same plan-record write) with the LADDER-built
    ``ProfilePrefetchBatchPlan`` (duck-typed; this module never imports
    enrichment). The plan is read-only input: nothing here mutates
    ``dispatch_item_specs``/``dispatch_specs`` and the caller only attaches the
    returned record to the ``refill_plan_items`` activity surface, which
    dispatch never reads.

    Non-invocation (returns None, no model call, no record):
      * no divider-capable client (default simulate/replay OfflineModelClient —
        condition (a); discrepancy D2)
      * a divider-capable client that is NOT declared billing-free while the
        dedicated opt-in ``SOURCING_WS7_DIVIDER_SHADOW_ALLOW_REAL_MODEL`` is
        unset — the SAFETY gate
        (:func:`divider_shadow_model_calls_permitted`)
      * completion fast path / deferred-submit callback
        (``wave_mint_provider_submit=False`` — ruling ② + OQ6: the divider runs
        once per wave mint, never on the <1 s completion tick)
      * retry-isolated wave (V8: the retry wave is never AI-divided, §4.2)
      * R6 durable-wave-inherited window (in-flight wave, §4.5)
      * empty plan (nothing to divide)

    Otherwise returns the shadow record: the OQ5 engagement outcome from
    ``propose_and_validate_division`` (validated envelope OR ruling-④ fallback
    audit OR the ≤300 skip record), V5/V6 apply-time results wired from the
    seam's worker budget + registry ids, and the ladder-divergence digest.
    Any exception is caught and returned AS the record
    (``shadow_status="shadow_error"``) — the S3 hard rule is that the shadow
    path can never affect dispatch.
    """
    try:
        if not model_client_supports_batch_division(model_client):
            return None
        # SAFETY gate (2026-07-25): capability is not permission. Without the
        # dedicated opt-in a non-billing-free client never reaches the model.
        if not divider_shadow_model_calls_permitted(model_client):
            return None
        if not wave_mint_provider_submit:
            return None
        queue_items = list(getattr(plan, "queue_items", None) or [])
        if not queue_items:
            return None
        retry_wait_indices = [
            index
            for index, item in enumerate(queue_items)
            if str(getattr(item, "queue_state", "") or "").strip() == RETRY_WAIT_QUEUE_STATE
        ]
        if len(retry_wait_indices) == len(queue_items):
            return None
        dispatch_window = dict(getattr(plan, "dispatch_window", None) or {})
        if str(dispatch_window.get("batch_size_reason") or "").strip() == DURABLE_WAVE_BATCH_SIZE_REASON:
            return None
        entries = {str(key): dict(value or {}) for key, value in dict(registry_entries or {}).items()}
        inventory: list[dict[str, Any]] = []
        failure_class_counts: dict[str, int] = {}
        attempted_item_count = 0
        max_attempt_count = 0
        for item in queue_items:
            url_key = _shadow_item_url_key(item)
            entry = entries.get(url_key) or {}
            try:
                attempt_count = max(0, int(entry.get("last_refill_attempt_count") or 0))
            except (TypeError, ValueError):
                attempt_count = 0
            failure_class = str(
                entry.get("last_refill_deferred_reason") or entry.get("refill_terminal_status") or ""
            ).strip()
            if attempt_count > 0:
                attempted_item_count += 1
                max_attempt_count = max(max_attempt_count, attempt_count)
            if failure_class:
                failure_class_counts[failure_class] = failure_class_counts.get(failure_class, 0) + 1
            inventory.append(
                {
                    "url_key": url_key,
                    "source_shards": [str(shard) for shard in list(getattr(item, "source_shards", None) or [])],
                    "queue_state": str(getattr(item, "queue_state", "") or "").strip(),
                    "attempt_count": attempt_count,
                    "last_failure_class": failure_class,
                    "priority": bool(getattr(item, "priority", False)),
                }
            )
        failure_history = {
            "retry_wait_item_count": len(retry_wait_indices),
            "attempted_item_count": attempted_item_count,
            "max_attempt_count": max_attempt_count,
            "failure_class_counts": dict(sorted(failure_class_counts.items())),
        }
        prior_round_context = {
            "plan_reason": str(getattr(plan, "plan_reason", "") or ""),
            "window_batch_size_reason": str(dispatch_window.get("batch_size_reason") or ""),
            "recorded_wave_batch_size": max(
                [max(0, int(getattr(item, "refill_plan_batch_size", 0) or 0)) for item in queue_items] or [0]
            ),
            "recorded_wave_batch_count": max(
                [max(0, int(getattr(item, "refill_plan_batch_count", 0) or 0)) for item in queue_items] or [0]
            ),
            "recorded_wave_window_url_count": max(
                [max(0, int(getattr(item, "refill_plan_window_url_count", 0) or 0)) for item in queue_items] or [0]
            ),
        }
        actor_global_inflight = resolved_harvest_profile_actor_global_inflight(dict(runtime_tuning_context or {}))
        proposal = propose_and_validate_division(
            model_client,
            inventory=inventory,
            failure_history=failure_history,
            prior_round_context=prior_round_context,
            retry_wait_indices=retry_wait_indices,
            actor_global_inflight=actor_global_inflight,
        )
        division = proposal.get("division")
        return {
            "kind": SHADOW_RECORD_KIND,
            "mode": "shadow",
            "shadow_status": str(proposal.get("status") or ""),
            "engaged": bool(proposal.get("engaged")),
            "skip_reason": str(proposal.get("skip_reason") or ""),
            "division_id": str(proposal.get("division_id") or ""),
            "eligible_member_count": int(proposal.get("eligible_member_count") or 0),
            "engagement_threshold_urls": int(proposal.get("engagement_threshold_urls") or 0),
            "actor_global_inflight": int(actor_global_inflight),
            "division": dict(division) if isinstance(division, Mapping) else None,
            "fallback_audit": (
                dict(proposal["fallback_audit"]) if isinstance(proposal.get("fallback_audit"), Mapping) else None
            ),
            "apply_time_validator_results": _shadow_apply_time_validator_results(
                division,
                worker_budget=dict(getattr(plan, "worker_budget", None) or {}),
                registry_entries=entries,
            ),
            "ladder_comparison": _shadow_ladder_comparison(plan, division),
        }
    except Exception as exc:  # noqa: BLE001 — S3 hard rule: shadow failure NEVER affects dispatch
        return {
            "kind": SHADOW_RECORD_KIND,
            "mode": "shadow",
            "shadow_status": SHADOW_STATUS_ERROR,
            "engaged": False,
            "shadow_error": " ".join(str(exc or "").strip().split())[:MAX_DIVIDER_ERROR_LENGTH],
            "shadow_error_type": type(exc).__name__,
        }


# ---------------------------------------------------------------------------
# W7.2 slice S4 — R6 durable wave-identity extension by `division_id`.
#
# Design §4.3 (the honest gap, resolved here) + OQ6: R6's scalar wave identity
# (refill_plan_batch_size/batch_count/window_url_count, all-items-same-size)
# cannot represent a heterogeneous AI division. S4 adds the identity carrier:
# `refill_plan_division_id` is minted at the plan-record moment (the shadow
# proposal's id pre-flip), persisted per registry item beside the scalars
# (migration 0015), inherited through the scalar R6 claim, and consumed by V6.
# The heterogeneous per-batch inheritance is a pure function here — reachable
# only when a future S5 caller passes an AI division into the plan.
# ---------------------------------------------------------------------------


def shadow_plan_division_id(shadow_record: Mapping[str, Any] | None) -> str:
    """The division id the plan-record moment persists as wave identity (S4).

    Pre-flip rule: only a VALIDATED shadow proposal (``shadow_status ==
    "proposed"``) links the shadow division to the actual ladder-built wave —
    that linkage is the divergence-measurement key. Fallback/skip/error shadow
    records return "" so rejected-proposal ids never pollute the registry (a
    persisted id means "an accepted division is riding this wave", which is
    exactly what V6's in-flight set and the S5 identity claim consume).
    """
    if not isinstance(shadow_record, Mapping):
        return ""
    if str(shadow_record.get("shadow_status") or "") != DIVISION_PROPOSAL_STATUS_PROPOSED:
        return ""
    return str(shadow_record.get("division_id") or "").strip()


def live_division_ids_for_dispatch(
    *,
    dispatch_urls: Sequence[str],
    registry_entries: Mapping[str, Mapping[str, Any]],
) -> set[str]:
    """Division ids recorded on the dispatch set's live durable-wave items.

    Mirrors the scalar R6 scan exactly: only entries whose
    ``refill_queue_state`` is in ``DURABLE_WAVE_QUEUE_STATES`` contribute, so a
    terminalized or re-queued item's stale id can never claim a wave.
    """
    ids: set[str] = set()
    entries = dict(registry_entries or {})
    for profile_url in list(dispatch_urls or []):
        entry = dict(entries.get(normalize_linkedin_profile_url_key(profile_url)) or {})
        if str(entry.get("refill_queue_state") or "").strip() not in DURABLE_WAVE_QUEUE_STATES:
            continue
        division_id = str(entry.get(REGISTRY_DIVISION_ID_FIELD) or "").strip()
        if division_id:
            ids.add(division_id)
    return ids


def inherited_division_wave_identity(
    *,
    dispatch_urls: Sequence[str],
    registry_entries: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    """The division-id fragment the scalar R6 claim merges into the window (S4).

    Called by enrichment's ``_apply_durable_refill_wave_dispatch_window`` ONLY
    inside its claim branch, so the id inherits exactly when the scalar
    identity inherits. Returns ``{}`` when no live item carries a division id —
    every pre-S4 row and every ladder-only wave — keeping the claimed window
    byte-identical to today (the oracle's R6 goldens). With multiple distinct
    ids on one merged dispatch set the lexicographically-first id is carried
    (deterministic); the conflict itself is V6's domain at proposal time.
    """
    ids = live_division_ids_for_dispatch(dispatch_urls=dispatch_urls, registry_entries=registry_entries)
    if not ids:
        return {}
    return {DURABLE_WAVE_DIVISION_ID_WINDOW_KEY: sorted(ids)[0]}


def inherit_heterogeneous_division_windows(
    *,
    division: Mapping[str, Any],
    inventory_url_keys: Sequence[str],
    dispatch_url_keys: Sequence[str],
) -> dict[str, Any]:
    """Per-batch R6 inheritance for a heterogeneous AI division (pure, S5-only).

    Design §4.3: when a dispatch set carries a live ``division_id``, the
    recorded division claims the window WHOLESALE — no re-split, no re-call.
    Because AI batches are heterogeneous, the claim is expressed as per-batch
    windows keyed by (``division_id``, ``batch_index`` ordinal): each remaining
    dispatch member is routed back to the batch that owns it in the recorded
    division (membership expanded from ``member_index_ranges`` over the
    canonical inventory ordering the division was minted against).

    ``claimed`` is True only when the division id is non-empty, at least one
    dispatch member remains, and EVERY dispatch member maps into exactly one
    recorded batch; any unassigned member (ready-set drift since mint) makes
    the claim fail closed — the caller falls back to a fresh mint with the F6
    ``divider_input_stale`` audit. Structurally unreachable pre-flip: no
    production caller passes an AI division (pinned by the S4 suite).
    """
    division_payload = dict(division or {})
    division_id = str(division_payload.get("division_id") or "").strip()
    position_by_key: dict[str, int] = {}
    for position, url_key in enumerate(inventory_url_keys):
        position_by_key.setdefault(str(url_key or "").strip(), position)
    batch_index_by_position: dict[int, int] = {}
    batch_by_index: dict[int, dict[str, Any]] = {}
    for batch in list(division_payload.get("batches") or []):
        batch_payload = dict(batch or {})
        try:
            batch_index = int(batch_payload.get("batch_index") or 0)
        except (TypeError, ValueError):
            continue
        if batch_index <= 0:
            continue
        batch_by_index[batch_index] = batch_payload
        for raw_range in list(batch_payload.get("member_index_ranges") or []):
            try:
                start, end = int(raw_range[0]), int(raw_range[1])
            except (IndexError, TypeError, ValueError):
                continue
            for position in range(start, end + 1):
                batch_index_by_position.setdefault(position, batch_index)
    remaining_by_batch: dict[int, list[str]] = {}
    unassigned_url_keys: list[str] = []
    for url_key in [str(key or "").strip() for key in list(dispatch_url_keys or [])]:
        member_position: int | None = position_by_key.get(url_key)
        owning_batch_index: int | None = (
            batch_index_by_position.get(member_position) if member_position is not None else None
        )
        if owning_batch_index is None:
            unassigned_url_keys.append(url_key)
            continue
        remaining_by_batch.setdefault(owning_batch_index, []).append(url_key)
    batch_windows = [
        {
            "division_id": division_id,
            "batch_index": batch_index,
            "recorded_member_count": max(0, int(batch_by_index[batch_index].get("member_count") or 0)),
            "remaining_member_count": len(member_url_keys),
            "member_url_keys": list(member_url_keys),
            "reason": str(batch_by_index[batch_index].get("reason") or ""),
            "reason_code": str(batch_by_index[batch_index].get("reason_code") or ""),
        }
        for batch_index, member_url_keys in sorted(remaining_by_batch.items())
    ]
    claimed = bool(division_id) and bool(batch_windows) and not unassigned_url_keys
    return {
        "division_id": division_id,
        "claimed": claimed,
        "batch_windows": batch_windows,
        "unassigned_url_keys": unassigned_url_keys,
    }
