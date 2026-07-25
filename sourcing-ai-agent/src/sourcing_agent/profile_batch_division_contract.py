"""Divider-output contract `sourcing.profile_prefetch.ai_batch_division.v1` (WS7/W7.2 S1).

Spec: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §1.2 (schema), §1.3 (validator battery
V1-V10), §3 (failure classes F1-F6 + fallback audit shape). OQ1-OQ8 RATIFIED
2026-07-23; slice S1 is ADDITIVE ONLY — this module is standalone (no store, no
model client, no enrichment import) and everything here is a pure function:
retry_wait membership, inventory size, and budget context arrive as explicit
inputs. Oracle: tests/test_fetch_profile_batch_characterization.py pins the
current rule ladder each validator formalizes; per-validator docstrings cite the
exact pins from the design's §1.3 table.

Version policy (fail-closed): payloads are accepted by EXACT schema-id lookup in
``_SCHEMA_NORMALIZERS``. An unknown or missing ``schema_id`` — including a future
``...v2`` — is rejected outright; there is no auto-upgrade path. Repairing or
partially accepting a bad division is deliberately unsupported (design §2.3:
repaired output would launder an unauditable model error into a dispatch shape).

Strict-parse vs battery split (design §2.3 / §1.3): normalization enforces
structure — key allowlists, types, lengths, ordinal continuity, range shape,
count consistency, hash formats, enums with a single deliberate exception. The
exception: batch ``reason`` presence and ``reason_code`` vocabulary membership
are enforced by validator V7 (the R7 audit row), not by normalization, so a
reason-less division surfaces as the auditable ``V7_reason_audit`` battery
failure the design table assigns rather than as an opaque parse error.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable

SCHEMA_ID_V1 = "sourcing.profile_prefetch.ai_batch_division.v1"

DIVISION_SOURCE_AI_DIVIDER = "ai_divider"
DIVISION_SOURCE_RULE_LADDER_FALLBACK = "rule_ladder_fallback"
DIVISION_SOURCE_VALUES = frozenset({DIVISION_SOURCE_AI_DIVIDER, DIVISION_SOURCE_RULE_LADDER_FALLBACK})

# Acceptance constants — standalone mirrors of the constant-ladder defaults the
# oracle pins (ConstantLadderBaselineTest, tests/
# test_fetch_profile_batch_characterization.py:202-211; source band
# enrichment.py:465-503). Deliberately NOT imported from enrichment: the ladder
# constants are env-overridable runtime tuning, while these are the RATIFIED
# acceptance bounds (ruling ① + OQ3) the battery holds AI output to.
PROVIDER_ENVELOPE_MAX_URLS = 300
MAX_BATCH_COUNT_PER_WAVE = 8
ACTOR_SLOT_URL_TARGET = 50
AI_DIVIDER_MIN_BATCH_COUNT = 4
AI_DIVIDER_MAX_BATCH_COUNT = 8
# OQ3 RATIFIED 2026-07-23: the "1-2 rounds of HarvestAPI actors" directive as a
# machine check, bound to the CONFIGURED inflight (default 4,
# runtime_tuning.py:354-361), never the unverified ~8 provider ceiling (design
# discrepancy D4).
MAX_ACTOR_WAVE_ROUNDS = 2
DEFAULT_ACTOR_GLOBAL_INFLIGHT = 4

MAX_REASON_LENGTH = 240
MAX_DIVIDER_ERROR_LENGTH = 500
MAX_DIVISION_ID_LENGTH = 64

# R7 vocabulary (design §1.2): AI batches carry the new "ai_division" code;
# fallback records keep the ladder's original enum values VERBATIM. Ladder
# values transcribed from the oracle-pinned surfaces: sizer R1.a-d golden grid
# (test :231-259), roster split (:791), durable-wave inheritance (:431), and
# the small-batch envelope reason fn + tiny-legality set
# (enrichment.py:1926-1963).
AI_DIVISION_REASON_CODE = "ai_division"
LADDER_BATCH_SIZE_REASON_CODES = frozenset(
    {
        "no_ready_urls",
        "actor_slot_item_packing",
        "single_durable_unit_ready_set",
        "bounded_ready_set_balanced_provider_envelopes",
        "large_ready_set_provider_envelope_target",
        "large_ready_set_provider_envelope_cap",
        "roster_actor_slot_fill",
        "durable_refill_wave_batch_size",
        "final_tail_unproven",
        "normal_batch_under_target_with_deferred_backlog",
    }
)
# The R7-legal tiny reasons, verbatim from
# ``_profile_batch_envelope_allowed_tiny_reason`` (enrichment.py:1955-1962).
# "ai_division" is intentionally NOT tiny-legal: the AI is free to pack >=50
# shapes and must borrow a ladder-legal tiny code to justify a sub-50 batch (V4).
TINY_BATCH_LEGAL_REASON_CODES = frozenset(
    {
        "cache_filtered_actor_slot",
        "final_tail",
        "queue_quiescent_final_tail",
        "retry_isolation",
        "urgent_user_visible",
        "low_volume_company",
        "prior_batch_backpressure_ordinal_gate",
    }
)
REASON_CODE_VALUES = (
    frozenset({AI_DIVISION_REASON_CODE}) | LADDER_BATCH_SIZE_REASON_CODES | TINY_BATCH_LEGAL_REASON_CODES
)

VALIDATOR_V1_PROVIDER_ENVELOPE = "V1_provider_envelope"
VALIDATOR_V2_BATCH_CEILING = "V2_batch_ceiling"
VALIDATOR_V3_EXACT_PARTITION = "V3_exact_partition"
VALIDATOR_V4_TINY_BATCH_REASON = "V4_tiny_batch_reason"
VALIDATOR_V5_WORKER_BUDGET = "V5_worker_budget"
VALIDATOR_V6_WAVE_MINT_ONLY = "V6_wave_mint_only"
VALIDATOR_V7_REASON_AUDIT = "V7_reason_audit"
VALIDATOR_V8_RETRY_ISOLATION = "V8_retry_isolation"
VALIDATOR_V9_ROUND_BUDGET = "V9_round_budget"
VALIDATOR_V10_BATCH_COUNT_BAND = "V10_batch_count_band"
VALIDATOR_IDS = (
    VALIDATOR_V1_PROVIDER_ENVELOPE,
    VALIDATOR_V2_BATCH_CEILING,
    VALIDATOR_V3_EXACT_PARTITION,
    VALIDATOR_V4_TINY_BATCH_REASON,
    VALIDATOR_V5_WORKER_BUDGET,
    VALIDATOR_V6_WAVE_MINT_ONLY,
    VALIDATOR_V7_REASON_AUDIT,
    VALIDATOR_V8_RETRY_ISOLATION,
    VALIDATOR_V9_ROUND_BUDGET,
    VALIDATOR_V10_BATCH_COUNT_BAND,
)

# failures[].validator_id for strict-parse (F4-class) rejections, distinct from
# every battery id so callers can map schema failures to `divider_invalid_output`
# and battery failures to `divider_validator_rejected:<validator>` (design §3).
SCHEMA_FAILURE_VALIDATOR_ID = "schema"

VALIDATOR_RESULT_STATUS_PASS = "pass"
VALIDATOR_RESULT_STATUS_FAIL = "fail"
VALIDATOR_RESULT_STATUS_SKIPPED = "skipped"
VALIDATOR_RESULT_STATUS_VALUES = frozenset(
    {VALIDATOR_RESULT_STATUS_PASS, VALIDATOR_RESULT_STATUS_FAIL, VALIDATOR_RESULT_STATUS_SKIPPED}
)

# Ruling-④ failure classes F1-F6 (design §3 table, `fallback_reason` column).
FALLBACK_REASON_MODEL_UNAVAILABLE = "divider_model_unavailable"  # F1
FALLBACK_REASON_CIRCUIT_OPEN = "divider_circuit_open"  # F2
FALLBACK_REASON_CALL_FAILED = "divider_call_failed"  # F3
FALLBACK_REASON_INVALID_OUTPUT = "divider_invalid_output"  # F4
FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX = "divider_validator_rejected:"  # F5
FALLBACK_REASON_INPUT_STALE = "divider_input_stale"  # F6
FALLBACK_REASON_FIXED_VALUES = frozenset(
    {
        FALLBACK_REASON_MODEL_UNAVAILABLE,
        FALLBACK_REASON_CIRCUIT_OPEN,
        FALLBACK_REASON_CALL_FAILED,
        FALLBACK_REASON_INVALID_OUTPUT,
        FALLBACK_REASON_INPUT_STALE,
    }
)

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_DIVISION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_id",
        "division_id",
        "division_source",
        "batch_count",
        "batches",
        "membership_sha256",
        "provenance",
        "validator_results",
        "fallback",
    }
)
_BATCH_KEYS = frozenset({"batch_index", "member_index_ranges", "member_count", "reason", "reason_code"})
_PROVENANCE_KEYS = frozenset(
    {
        "model_provider",
        "requested_model",
        "response_model",
        "prompt_sha256",
        "input_snapshot_sha256",
        "usage",
        "latency_ms",
    }
)
_USAGE_KEYS = frozenset({"input_tokens", "output_tokens"})
_VALIDATOR_RESULT_REQUIRED_KEYS = frozenset({"validator", "status"})
_VALIDATOR_RESULT_KEYS = _VALIDATOR_RESULT_REQUIRED_KEYS | frozenset({"reason"})
_FALLBACK_KEYS = frozenset({"division_source", "fallback_reason", "divider_error", "validator_results", "provenance"})


class BatchDivisionContractError(ValueError):
    """Fail-closed strict-parse error for the divider-output contract (F4 class)."""


def fallback_reason_for_validator(validator_id: str) -> str:
    """The F5 `fallback_reason` string for a battery rejection (design §3)."""
    if validator_id not in VALIDATOR_IDS:
        raise BatchDivisionContractError(f"unknown validator id: {validator_id!r}")
    return f"{FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX}{validator_id}"


def is_valid_fallback_reason(value: str) -> bool:
    """True for the fixed F1-F4/F6 values and well-formed F5 `...:<validator>` forms."""
    text = str(value or "")
    if text in FALLBACK_REASON_FIXED_VALUES:
        return True
    if text.startswith(FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX):
        return text[len(FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX) :] in VALIDATOR_IDS
    return False


def compute_membership_sha256(batch_member_keys: Sequence[Sequence[str]]) -> str:
    """Hash over the ordered normalized url-key partition (design §1.2).

    Canonical form: the batch-ordered list of member-key lists, canonical-JSON
    encoded (sorted object keys are irrelevant here; ORDER of batches and of
    members within a batch is significant and preserved). This is the identity
    the F6 stale-input check compares at apply time.
    """
    canonical = json.dumps(
        [[str(key) for key in batch] for batch in batch_member_keys],
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DivisionBatch:
    """One divided batch: 1-based ordinal + inclusive index ranges + R7 audit."""

    batch_index: int
    member_index_ranges: tuple[tuple[int, int], ...]
    member_count: int
    reason: str
    reason_code: str

    def member_indices(self) -> list[int]:
        """Expand the inclusive ranges in order (duplicates preserved for V3)."""
        indices: list[int] = []
        for start, end in self.member_index_ranges:
            indices.extend(range(start, end + 1))
        return indices

    def to_payload(self) -> dict[str, Any]:
        return {
            "batch_index": self.batch_index,
            "member_index_ranges": [[start, end] for start, end in self.member_index_ranges],
            "member_count": self.member_count,
            "reason": self.reason,
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True)
class DivisionProvenance:
    """Model provenance fields (design §1.2 `provenance` object)."""

    model_provider: str
    requested_model: str
    response_model: str
    prompt_sha256: str
    input_snapshot_sha256: str
    input_tokens: int
    output_tokens: int
    latency_ms: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "model_provider": self.model_provider,
            "requested_model": self.requested_model,
            "response_model": self.response_model,
            "prompt_sha256": self.prompt_sha256,
            "input_snapshot_sha256": self.input_snapshot_sha256,
            "usage": {"input_tokens": self.input_tokens, "output_tokens": self.output_tokens},
            "latency_ms": self.latency_ms,
        }


@dataclass(frozen=True)
class ValidatorResultRecord:
    """One `validator_results` entry; `reason` is optional audit detail."""

    validator: str
    status: str
    reason: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"validator": self.validator, "status": self.status}
        if self.reason:
            payload["reason"] = self.reason
        return payload


@dataclass(frozen=True)
class FallbackAudit:
    """Ruling-④ fallback audit object (design §3): reason + truncated error +
    whatever validator results ran + provenance when a call was actually made."""

    division_source: str
    fallback_reason: str
    divider_error: str
    validator_results: tuple[ValidatorResultRecord, ...]
    provenance: DivisionProvenance | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "division_source": self.division_source,
            "fallback_reason": self.fallback_reason,
            "divider_error": self.divider_error,
            "validator_results": [entry.to_payload() for entry in self.validator_results],
            "provenance": self.provenance.to_payload() if self.provenance is not None else None,
        }


@dataclass(frozen=True)
class BatchDivision:
    """The normalized `ai_batch_division.v1` division record (design §1.2)."""

    schema_id: str
    division_id: str
    division_source: str
    batch_count: int
    batches: tuple[DivisionBatch, ...]
    membership_sha256: str
    provenance: DivisionProvenance | None
    validator_results: tuple[ValidatorResultRecord, ...]
    fallback: FallbackAudit | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "division_id": self.division_id,
            "division_source": self.division_source,
            "batch_count": self.batch_count,
            "batches": [batch.to_payload() for batch in self.batches],
            "membership_sha256": self.membership_sha256,
            "provenance": self.provenance.to_payload() if self.provenance is not None else None,
            "validator_results": [entry.to_payload() for entry in self.validator_results],
            "fallback": self.fallback.to_payload() if self.fallback is not None else None,
        }


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BatchDivisionContractError(f"{label} must be an object, got {type(value).__name__}")
    return value


def _require_allowed_keys(payload: Mapping[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = sorted(set(str(key) for key in payload.keys()) - allowed)
    if unknown:
        raise BatchDivisionContractError(f"{label} carries unknown keys {unknown} (strict allowlist)")


def _require_keys(payload: Mapping[str, Any], required: Iterable[str], label: str) -> None:
    missing = sorted(key for key in required if key not in payload)
    if missing:
        raise BatchDivisionContractError(f"{label} is missing required keys {missing}")


def _require_str(value: Any, label: str, *, max_length: int | None = None) -> str:
    if not isinstance(value, str):
        raise BatchDivisionContractError(f"{label} must be a string, got {type(value).__name__}")
    if max_length is not None and len(value) > max_length:
        raise BatchDivisionContractError(f"{label} exceeds {max_length} chars (got {len(value)})")
    return value


def _require_int(value: Any, label: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BatchDivisionContractError(f"{label} must be an integer, got {type(value).__name__}")
    if minimum is not None and value < minimum:
        raise BatchDivisionContractError(f"{label} must be >= {minimum}, got {value}")
    return value


def _require_sha256(value: Any, label: str) -> str:
    text = _require_str(value, label)
    if not _SHA256_HEX_RE.fullmatch(text):
        raise BatchDivisionContractError(f"{label} must be 64 lowercase hex chars")
    return text


def _normalize_batch(value: Any, position: int) -> DivisionBatch:
    label = f"batches[{position}]"
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _BATCH_KEYS, label)
    _require_keys(payload, _BATCH_KEYS, label)
    batch_index = _require_int(payload["batch_index"], f"{label}.batch_index", minimum=1)
    if batch_index != position + 1:
        raise BatchDivisionContractError(
            f"{label}.batch_index must be the 1-based ordinal {position + 1} "
            f"(mirrors dispatch_item_specs ordinals), got {batch_index}"
        )
    raw_ranges = payload["member_index_ranges"]
    if not isinstance(raw_ranges, Sequence) or isinstance(raw_ranges, (str, bytes)):
        raise BatchDivisionContractError(f"{label}.member_index_ranges must be a list of [start, end] pairs")
    ranges: list[tuple[int, int]] = []
    for range_position, raw_range in enumerate(raw_ranges):
        range_label = f"{label}.member_index_ranges[{range_position}]"
        if not isinstance(raw_range, Sequence) or isinstance(raw_range, (str, bytes)) or len(raw_range) != 2:
            raise BatchDivisionContractError(f"{range_label} must be a [start, end] pair")
        start = _require_int(raw_range[0], f"{range_label}[0]", minimum=0)
        end = _require_int(raw_range[1], f"{range_label}[1]", minimum=0)
        if end < start:
            raise BatchDivisionContractError(f"{range_label} is inverted: end {end} < start {start}")
        ranges.append((start, end))
    member_count = _require_int(payload["member_count"], f"{label}.member_count", minimum=0)
    expanded_size = sum(end - start + 1 for start, end in ranges)
    if member_count != expanded_size:
        raise BatchDivisionContractError(f"{label}.member_count {member_count} != expanded range size {expanded_size}")
    # reason presence and reason_code vocabulary are V7's domain (module
    # docstring: strict-parse vs battery split); only structure is checked here.
    reason = _require_str(payload["reason"], f"{label}.reason", max_length=MAX_REASON_LENGTH)
    reason_code = _require_str(payload["reason_code"], f"{label}.reason_code", max_length=MAX_REASON_LENGTH)
    return DivisionBatch(
        batch_index=batch_index,
        member_index_ranges=tuple(ranges),
        member_count=member_count,
        reason=reason,
        reason_code=reason_code,
    )


def _normalize_provenance(value: Any, label: str) -> DivisionProvenance:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _PROVENANCE_KEYS, label)
    _require_keys(payload, _PROVENANCE_KEYS, label)
    usage = _require_mapping(payload["usage"], f"{label}.usage")
    _require_allowed_keys(usage, _USAGE_KEYS, f"{label}.usage")
    _require_keys(usage, _USAGE_KEYS, f"{label}.usage")
    return DivisionProvenance(
        model_provider=_require_str(payload["model_provider"], f"{label}.model_provider"),
        requested_model=_require_str(payload["requested_model"], f"{label}.requested_model"),
        response_model=_require_str(payload["response_model"], f"{label}.response_model"),
        prompt_sha256=_require_sha256(payload["prompt_sha256"], f"{label}.prompt_sha256"),
        input_snapshot_sha256=_require_sha256(payload["input_snapshot_sha256"], f"{label}.input_snapshot_sha256"),
        input_tokens=_require_int(usage["input_tokens"], f"{label}.usage.input_tokens", minimum=0),
        output_tokens=_require_int(usage["output_tokens"], f"{label}.usage.output_tokens", minimum=0),
        latency_ms=_require_int(payload["latency_ms"], f"{label}.latency_ms", minimum=0),
    )


def _normalize_validator_results(value: Any, label: str) -> tuple[ValidatorResultRecord, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise BatchDivisionContractError(f"{label} must be a list of validator result objects")
    results: list[ValidatorResultRecord] = []
    for position, raw_entry in enumerate(value):
        entry_label = f"{label}[{position}]"
        entry = _require_mapping(raw_entry, entry_label)
        _require_allowed_keys(entry, _VALIDATOR_RESULT_KEYS, entry_label)
        _require_keys(entry, _VALIDATOR_RESULT_REQUIRED_KEYS, entry_label)
        status = _require_str(entry["status"], f"{entry_label}.status")
        if status not in VALIDATOR_RESULT_STATUS_VALUES:
            raise BatchDivisionContractError(
                f"{entry_label}.status must be one of {sorted(VALIDATOR_RESULT_STATUS_VALUES)}, got {status!r}"
            )
        results.append(
            ValidatorResultRecord(
                validator=_require_str(entry["validator"], f"{entry_label}.validator"),
                status=status,
                reason=_require_str(entry.get("reason", ""), f"{entry_label}.reason"),
            )
        )
    return tuple(results)


def normalize_fallback_audit(value: Any, label: str = "fallback") -> FallbackAudit:
    """Strictly normalize a ruling-④ fallback audit object (design §3 shape).

    Public for the shadow-slice activity surface (S3), where the audit is stored
    standalone next to the plan-items record; on the plan record post-flip it
    appears as ``ai_batch_division.fallback``.
    """
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _FALLBACK_KEYS, label)
    _require_keys(payload, ("division_source", "fallback_reason", "divider_error", "validator_results"), label)
    division_source = _require_str(payload["division_source"], f"{label}.division_source")
    if division_source != DIVISION_SOURCE_RULE_LADDER_FALLBACK:
        raise BatchDivisionContractError(
            f"{label}.division_source must be {DIVISION_SOURCE_RULE_LADDER_FALLBACK!r}, got {division_source!r}"
        )
    fallback_reason = _require_str(payload["fallback_reason"], f"{label}.fallback_reason")
    if not is_valid_fallback_reason(fallback_reason):
        raise BatchDivisionContractError(f"{label}.fallback_reason {fallback_reason!r} is not a ruling-④ F1-F6 value")
    divider_error = _require_str(
        payload["divider_error"], f"{label}.divider_error", max_length=MAX_DIVIDER_ERROR_LENGTH
    )
    raw_provenance = payload.get("provenance")
    provenance = _normalize_provenance(raw_provenance, f"{label}.provenance") if raw_provenance is not None else None
    return FallbackAudit(
        division_source=division_source,
        fallback_reason=fallback_reason,
        divider_error=divider_error,
        validator_results=_normalize_validator_results(payload["validator_results"], f"{label}.validator_results"),
        provenance=provenance,
    )


def _normalize_v1(payload: Mapping[str, Any]) -> BatchDivision:
    _require_allowed_keys(payload, _TOP_LEVEL_KEYS, "division")
    _require_keys(payload, _TOP_LEVEL_KEYS, "division")
    division_id = _require_str(payload["division_id"], "division_id", max_length=MAX_DIVISION_ID_LENGTH)
    if not _DIVISION_ID_RE.fullmatch(division_id):
        raise BatchDivisionContractError("division_id must be 1-64 chars of [A-Za-z0-9_-] (ULID-compatible identity)")
    division_source = _require_str(payload["division_source"], "division_source")
    if division_source not in DIVISION_SOURCE_VALUES:
        raise BatchDivisionContractError(
            f"division_source must be one of {sorted(DIVISION_SOURCE_VALUES)}, got {division_source!r}"
        )
    raw_batches = payload["batches"]
    if not isinstance(raw_batches, Sequence) or isinstance(raw_batches, (str, bytes)):
        raise BatchDivisionContractError("batches must be a list of batch objects")
    batches = tuple(_normalize_batch(raw_batch, position) for position, raw_batch in enumerate(raw_batches))
    batch_count = _require_int(payload["batch_count"], "batch_count", minimum=0)
    if batch_count != len(batches):
        raise BatchDivisionContractError(f"batch_count {batch_count} != len(batches) {len(batches)}")
    raw_provenance = payload["provenance"]
    provenance = _normalize_provenance(raw_provenance, "provenance") if raw_provenance is not None else None
    raw_fallback = payload["fallback"]
    fallback = normalize_fallback_audit(raw_fallback, "fallback") if raw_fallback is not None else None
    if division_source == DIVISION_SOURCE_AI_DIVIDER:
        if fallback is not None:
            raise BatchDivisionContractError("ai_divider divisions must carry fallback=null")
        if provenance is None:
            raise BatchDivisionContractError("ai_divider divisions must carry model provenance")
        if not batches:
            raise BatchDivisionContractError("ai_divider divisions must carry at least one batch")
    else:
        if fallback is None:
            raise BatchDivisionContractError(
                "rule_ladder_fallback divisions must carry the ruling-④ fallback audit object"
            )
    return BatchDivision(
        schema_id=SCHEMA_ID_V1,
        division_id=division_id,
        division_source=division_source,
        batch_count=batch_count,
        batches=batches,
        membership_sha256=_require_sha256(payload["membership_sha256"], "membership_sha256"),
        provenance=provenance,
        validator_results=_normalize_validator_results(payload["validator_results"], "validator_results"),
        fallback=fallback,
    )


_SCHEMA_NORMALIZERS: dict[str, Callable[[Mapping[str, Any]], BatchDivision]] = {
    SCHEMA_ID_V1: _normalize_v1,
}


def normalize_ai_batch_division(payload: Any) -> BatchDivision:
    """Strict, fail-closed normalization by EXACT schema-id lookup (no upgrade)."""
    mapping = _require_mapping(payload, "division payload")
    schema_id = mapping.get("schema_id")
    normalizer = _SCHEMA_NORMALIZERS.get(schema_id) if isinstance(schema_id, str) else None
    if normalizer is None:
        raise BatchDivisionContractError(
            f"unknown schema_id {schema_id!r}: exact-version lookup accepts only "
            f"{sorted(_SCHEMA_NORMALIZERS)} (fail-closed, no auto-upgrade)"
        )
    return normalizer(mapping)


# ---------------------------------------------------------------------------
# Validator battery V1-V10 (design §1.3) — pure functions, one per design row.
# Each returns None on pass or a human-auditable failure reason on FAIL; any
# FAIL maps to ruling-④ fallback class F5 with
# ``fallback_reason_for_validator(<id>)``.
# ---------------------------------------------------------------------------


def validate_v1_provider_envelope(
    batches: Sequence[DivisionBatch],
    *,
    provider_envelope_max_urls: int = PROVIDER_ENVELOPE_MAX_URLS,
) -> str | None:
    """V1 — every batch <= 300 members (provider envelope cap).

    Demoted ladder rule: ``PROVIDER_ENVELOPE_MAX_URLS`` constant band
    (enrichment.py:465-503). Oracle pins: ConstantLadderBaselineTest
    (test_fetch_profile_batch_characterization.py:202-211) and the R1.d golden
    grid rows (:255-258, fixed 300 envelope above the 8-cap threshold).
    """
    oversized = [
        f"batch {batch.batch_index} has {batch.member_count} members"
        for batch in batches
        if batch.member_count > provider_envelope_max_urls
    ]
    if oversized:
        return f"provider envelope exceeded (> {provider_envelope_max_urls}): " + "; ".join(oversized)
    return None


def validate_v2_batch_ceiling(
    batch_count: int,
    *,
    max_batch_count: int = MAX_BATCH_COUNT_PER_WAVE,
) -> str | None:
    """V2 — at most 8 batches per wave.

    Demoted ladder rule: ``MAX_BATCH_COUNT_FOR_LARGE_READY_SET`` (same constant
    band; sizer R1.d enrichment.py:677-702). Oracle pin:
    ``test_large_3000_hits_the_8_cap_and_defers_the_surplus_wave``
    (test_fetch_profile_batch_characterization.py:665-713).
    """
    if batch_count > max_batch_count:
        return f"batch_count {batch_count} exceeds the per-wave ceiling {max_batch_count}"
    return None


def validate_v3_exact_partition(
    batches: Sequence[DivisionBatch],
    *,
    inventory_size: int,
    retry_wait_indices: Collection[int] = frozenset(),
) -> str | None:
    """V3 — exact partition: batches cover the ready set with no duplicate,
    omitted, or invented member.

    Demoted ladder rule: implicit in today's contiguous slicing
    (``_actor_slot_chunk_strings`` enrichment.py:1491). Oracle pin: the
    ``_assert_specs`` membership pins on every plan golden
    (test_fetch_profile_batch_characterization.py:502-507).

    Interpretation note (design table read with the V8 row + the retry-isolation
    oracle pin :803-835): retry_wait indices are EXCLUDED from the coverage
    target here — the retry wave is never AI-divided, so a retry_wait member
    appearing inside a batch is V8's failure, not a V3 "invented member"; V3
    checks that the NON-retry ready indices [0, inventory_size) are covered
    exactly once and that no index lies outside the inventory bounds.
    """
    if inventory_size < 0:
        return f"inventory_size must be >= 0, got {inventory_size}"
    retry_set = {int(index) for index in retry_wait_indices}
    seen: set[int] = set()
    duplicates: set[int] = set()
    invented: set[int] = set()
    for batch in batches:
        for index in batch.member_indices():
            if index < 0 or index >= inventory_size:
                invented.add(index)
                continue
            if index in retry_set:
                continue  # V8's domain
            if index in seen:
                duplicates.add(index)
            seen.add(index)
    problems: list[str] = []
    if invented:
        problems.append(f"invented members outside inventory [0, {inventory_size}): {sorted(invented)[:10]}")
    if duplicates:
        problems.append(f"duplicate members: {sorted(duplicates)[:10]}")
    missing = set(range(inventory_size)) - retry_set - seen
    if missing:
        problems.append(f"omitted members: {sorted(missing)[:10]}")
    if problems:
        return "not an exact partition of the ready set: " + "; ".join(problems)
    return None


def validate_v4_tiny_batch_reason(
    batches: Sequence[DivisionBatch],
    *,
    min_non_tiny_batch_size: int = ACTOR_SLOT_URL_TARGET,
    legal_tiny_reason_codes: frozenset[str] = TINY_BATCH_LEGAL_REASON_CODES,
) -> str | None:
    """V4 — a batch under 50 members is legal only with an R7-legal tiny reason.

    Demoted ladder rule: the R4 sub-50 tail guard (enrichment.py:1467-1490) and
    the tiny-legality set (``_profile_batch_envelope_allowed_tiny_reason``
    :1955-1962). Oracle pins: sub-50 tail goldens
    (test_fetch_profile_batch_characterization.py:532-575) and the retry tiny
    golden (:803-835).
    """
    illegal = [
        f"batch {batch.batch_index} ({batch.member_count} members, reason_code {batch.reason_code!r})"
        for batch in batches
        if batch.member_count < min_non_tiny_batch_size and batch.reason_code not in legal_tiny_reason_codes
    ]
    if illegal:
        return f"sub-{min_non_tiny_batch_size} batches without an R7-legal tiny reason_code: " + "; ".join(illegal)
    return None


def validate_v5_worker_budget(
    dispatched_batch_count: int,
    *,
    available_new_worker_count: int,
) -> str | None:
    """V5 — dispatched specs never overrun the worker budget; surplus defers.

    Demoted ladder rule: R5 wave bounding
    (``_split_profile_prefetch_dispatch_specs`` enrichment.py:1062). Oracle
    pins: DispatchSpecSplitCharacterizationTest
    (test_fetch_profile_batch_characterization.py:470-489) and the 3000-case
    deferred-600 golden (:713).

    Apply-time validator: the immediate-dispatch batch count is produced by the
    surviving R5 split at apply (S3/S5), so the top-level S1 battery marks this
    row "skipped" — a division whose batch_count exceeds the budget is still
    valid (the surplus defers, exactly as the oracle pins).
    """
    if available_new_worker_count < 0:
        return f"available_new_worker_count must be >= 0, got {available_new_worker_count}"
    if dispatched_batch_count > available_new_worker_count:
        return (
            f"dispatched batch count {dispatched_batch_count} overruns the worker "
            f"budget {available_new_worker_count} (surplus must defer, never dispatch)"
        )
    return None


def validate_v6_wave_mint_only(
    division_id: str,
    *,
    live_division_ids: Collection[str],
) -> str | None:
    """V6 — an in-flight durable wave is never re-divided; division happens at
    wave mint only.

    Demoted ladder rule: R6 durable-wave inheritance
    (``_apply_durable_refill_wave_dispatch_window`` enrichment.py:1162). Oracle
    pin: DurableWaveInheritanceCharacterizationTest
    (test_fetch_profile_batch_characterization.py:387-467, incl. the
    append/retry/empty never-inherit variants :462-467).

    Apply-time validator: ``live_division_ids`` are the division ids already
    recorded on the dispatch set's registry items (the §4.3 R6 extension). A new
    division is acceptable only when no other division id is live; re-recording
    the SAME id is the idempotent re-tick case and passes.
    """
    live = {str(value) for value in live_division_ids if str(value)}
    foreign = sorted(live - {str(division_id)})
    if foreign:
        return (
            f"dispatch set already carries live division id(s) {foreign}: an in-flight "
            "wave is never re-divided (division happens at wave mint only)"
        )
    return None


def validate_v7_reason_audit(
    batches: Sequence[DivisionBatch],
    *,
    reason_code_values: frozenset[str] = REASON_CODE_VALUES,
) -> str | None:
    """V7 — every batch carries an explainable reason and an R7-auditable
    reason_code from the pinned vocabulary (+ the "ai_division" extension).

    Demoted ladder rule: the R7 ``batch_size_reason`` audit enum
    (window/record fields enrichment.py:1306-1317). Oracle pins: sizer golden
    grid reasons (test_fetch_profile_batch_characterization.py:231-259) and the
    record ``batch_size_reason`` pins throughout the plan goldens.
    """
    problems: list[str] = []
    for batch in batches:
        if not batch.reason.strip():
            problems.append(f"batch {batch.batch_index} has no reason")
        if batch.reason_code not in reason_code_values:
            problems.append(f"batch {batch.batch_index} reason_code {batch.reason_code!r} is not R7-auditable")
    if problems:
        return "reason audit failed: " + "; ".join(problems)
    return None


def validate_v8_retry_isolation(
    batches: Sequence[DivisionBatch],
    *,
    retry_wait_indices: Collection[int],
) -> str | None:
    """V8 — retry_wait items are never mixed into a normal-wave division; the
    retry wave is not AI-divided at all (design §4.2).

    Demoted ladder rule: retry isolation (``_profile_refill_retry_gate``
    enrichment.py:3900-4020; plan retry branch :1348-1350/:1536-1537). Oracle
    pin: ``test_retry_wait_items_dispatch_as_isolated_wave_even_when_tiny``
    (test_fetch_profile_batch_characterization.py:803-835).
    """
    retry_set = {int(index) for index in retry_wait_indices}
    if not retry_set:
        return None
    mixed: list[str] = []
    for batch in batches:
        hits = sorted(retry_set.intersection(batch.member_indices()))
        if hits:
            mixed.append(f"batch {batch.batch_index} contains retry_wait members {hits[:10]}")
    if mixed:
        return "retry_wait members mixed into a normal-wave division: " + "; ".join(mixed)
    return None


def validate_v9_round_budget(
    batch_count: int,
    *,
    actor_global_inflight: int,
    max_rounds: int = MAX_ACTOR_WAVE_ROUNDS,
) -> str | None:
    """V9 — ``ceil(batch_count / actor_global_inflight) <= 2``: the "1-2 rounds
    of HarvestAPI actors" budget as a machine check.

    New rule (formalizes operator directive 2; recon §4.3). OQ3 RATIFIED
    2026-07-23: bound to the CONFIGURED inflight
    (``resolved_harvest_profile_actor_global_inflight`` runtime_tuning.py:354-361,
    default 4; fast_smoke=2), never the folklore ~8 provider ceiling (design
    discrepancy D4). No oracle pin exists yet — this validator IS the pin's
    origin (design §1.3 row V9).
    """
    if actor_global_inflight < 1:
        return f"actor_global_inflight must be >= 1, got {actor_global_inflight} (fail-closed)"
    rounds = math.ceil(batch_count / actor_global_inflight)
    if rounds > max_rounds:
        return (
            f"batch_count {batch_count} needs {rounds} actor rounds at inflight "
            f"{actor_global_inflight} (budget: <= {max_rounds} rounds)"
        )
    return None


def validate_v10_batch_count_band(
    batch_count: int,
    *,
    division_source: str,
    min_batch_count: int = AI_DIVIDER_MIN_BATCH_COUNT,
    max_batch_count: int = AI_DIVIDER_MAX_BATCH_COUNT,
) -> str | None:
    """V10 — ``batch_count in [4, 8]`` when the AI divider is engaged.

    New rule (ruling-① batch-count band; design §1.3 row V10, no oracle pin —
    added with the validator). Applies to ``ai_divider`` divisions only: fallback
    records reproduce ladder output verbatim, whose pre-R5 chunk counts legally
    leave the band (the oracle's 3000-case emits 10 chunks before R5 defers).
    The OQ5-ratified engagement threshold (divider engaged only for >300-url
    ready sets) is call-site policy at the mint site, not re-checked here.
    """
    if division_source != DIVISION_SOURCE_AI_DIVIDER:
        return None
    if batch_count < min_batch_count or batch_count > max_batch_count:
        return (
            f"ai_divider batch_count {batch_count} is outside the ruling-① band [{min_batch_count}, {max_batch_count}]"
        )
    return None


def validate_ai_batch_division(
    payload: Any,
    *,
    inventory_size: int,
    retry_wait_indices: Collection[int],
    actor_global_inflight: int,
) -> dict[str, Any]:
    """Normalize + run the acceptance battery over one division payload.

    Returns ``{"valid", "failures": [{"validator_id", "reason"}...],
    "normalized", "validator_results"}``. Fail-closed on unknown
    ``schema_version``/``schema_id`` (exact-version lookup, no auto-upgrade):
    such payloads report a single ``validator_id="schema"`` failure with
    ``normalized=None`` (F4 class).

    Battery scope: the V1-V10 battery is the acceptance gate for ``ai_divider``
    divisions (design §1.3: "validators run on every AI division before it is
    applied"). ``rule_ladder_fallback`` payloads are ruling-④ audit records —
    they ARE the ladder, whose oracle-pinned shapes legally violate the AI band —
    so only the strict schema (incl. the §3 audit shape) is enforced and every
    battery row reports "skipped". V5/V6 consume apply-time inputs (post-R5
    dispatch count, live wave division ids) outside this S1 signature; they are
    exported pure functions for the apply path and report "skipped" here.
    """
    retry_set = frozenset(int(index) for index in retry_wait_indices)
    try:
        division = normalize_ai_batch_division(payload)
    except BatchDivisionContractError as error:
        return {
            "valid": False,
            "failures": [{"validator_id": SCHEMA_FAILURE_VALIDATOR_ID, "reason": str(error)}],
            "normalized": None,
            "validator_results": [],
        }

    skipped_reason = ""
    if division.division_source == DIVISION_SOURCE_RULE_LADDER_FALLBACK:
        skipped_reason = "fallback audit record: the battery gates ai_divider divisions only"
        outcomes: list[tuple[str, str | None, str]] = [
            (validator_id, None, skipped_reason) for validator_id in VALIDATOR_IDS
        ]
    else:
        apply_time_reason = "apply-time input (post-R5 dispatch count / live wave ids) not in S1 scope"
        outcomes = [
            (VALIDATOR_V1_PROVIDER_ENVELOPE, validate_v1_provider_envelope(division.batches), ""),
            (VALIDATOR_V2_BATCH_CEILING, validate_v2_batch_ceiling(division.batch_count), ""),
            (
                VALIDATOR_V3_EXACT_PARTITION,
                validate_v3_exact_partition(
                    division.batches, inventory_size=inventory_size, retry_wait_indices=retry_set
                ),
                "",
            ),
            (VALIDATOR_V4_TINY_BATCH_REASON, validate_v4_tiny_batch_reason(division.batches), ""),
            (VALIDATOR_V5_WORKER_BUDGET, None, apply_time_reason),
            (VALIDATOR_V6_WAVE_MINT_ONLY, None, apply_time_reason),
            (VALIDATOR_V7_REASON_AUDIT, validate_v7_reason_audit(division.batches), ""),
            (
                VALIDATOR_V8_RETRY_ISOLATION,
                validate_v8_retry_isolation(division.batches, retry_wait_indices=retry_set),
                "",
            ),
            (
                VALIDATOR_V9_ROUND_BUDGET,
                validate_v9_round_budget(division.batch_count, actor_global_inflight=actor_global_inflight),
                "",
            ),
            (
                VALIDATOR_V10_BATCH_COUNT_BAND,
                validate_v10_batch_count_band(division.batch_count, division_source=division.division_source),
                "",
            ),
        ]

    failures: list[dict[str, str]] = []
    validator_results: list[dict[str, str]] = []
    for validator_id, failure_reason, skip_reason in outcomes:
        if skip_reason:
            entry = {
                "validator": validator_id,
                "status": VALIDATOR_RESULT_STATUS_SKIPPED,
                "reason": skip_reason,
            }
        elif failure_reason is not None:
            entry = {
                "validator": validator_id,
                "status": VALIDATOR_RESULT_STATUS_FAIL,
                "reason": failure_reason,
            }
            failures.append({"validator_id": validator_id, "reason": failure_reason})
        else:
            entry = {"validator": validator_id, "status": VALIDATOR_RESULT_STATUS_PASS}
        validator_results.append(entry)

    return {
        "valid": not failures,
        "failures": failures,
        "normalized": division.to_payload(),
        "validator_results": validator_results,
    }
