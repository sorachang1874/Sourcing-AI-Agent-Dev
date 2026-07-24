"""Divider orchestration helper (WS7/W7.2 slice S2, ADDITIVE ONLY).

Spec: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §2 (model invocation surface), §3
(ruling-④ failure classes F1-F6), OQ1/OQ5/OQ6/OQ7/OQ8 RATIFIED 2026-07-23.
This module is the thin seam between the ModelClient divider method
(`divide_profile_prefetch_batches`, model_provider.py) and the S1 contract
(`profile_batch_division_contract`): it builds the OQ1 input payload (index
ranges + digests only — NEVER raw URLs), calls the model ONCE per wave mint
(OQ6; idempotency/locking is the S3 mint site's duty), assembles the
caller-owned envelope fields (division_id, membership_sha256, provenance
passthrough), and validates through the single-sourced S1 battery. It performs
NO enrichment.py integration and NO dispatch — shadow recording is slice S3.

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
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Collection, Mapping, Sequence
from typing import Any

from .model_provider import (
    PROFILE_BATCH_DIVIDER_CIRCUIT_ERROR_PREFIX,
    PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_RAW_PREVIEW_KEY,
    ModelClient,
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
    BatchDivisionContractError,
    compute_membership_sha256,
    fallback_reason_for_validator,
    normalize_fallback_audit,
    validate_ai_batch_division,
)

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
