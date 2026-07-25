"""Promote-decision contract `sourcing.organization_asset.ai_promote_decision.v1` (WS7/W7.3 S1).

Spec: docs/WS7_AI_PROMOTE_DESIGN.md §2 (schema), §5 (validators-from-rules
V_LINEAGE / V_COMP / V_GEN / V_PROV / V_LIFECYCLE), §4 (failure classes F1-F6 +
keep-incumbent audit shape). OQ1-OQ8 RATIFIED 2026-07-24; slice S1 is ADDITIVE
ONLY — this module is standalone (no store, no model client, no integration into
the live evaluate/promote path) and everything here is a pure function: the
guard's predicted verdict and the incumbent descriptor arrive as EXPLICIT input
parameters, never as model-authored payload fields. Oracle:
tests/test_organization_promote_characterization.py (S0, landed 7f929fc) pins the
current deterministic ladder each validator formalizes; per-validator docstrings
cite the exact S0 pins.

Structural fail-closed properties (mirrors the proven divider S1,
profile_batch_division_contract.py):

* EXACT-VERSION LOOKUP. Payloads are accepted only by exact ``schema_id`` match
  in ``_SCHEMA_NORMALIZERS``. An unknown or missing ``schema_id`` — including a
  future ``...v2`` — is rejected outright; there is NO auto-upgrade path.
  Repairing or partially accepting a bad decision is deliberately unsupported
  (design §3.3: a repaired decision would launder an unauditable model error
  into an authoritative-snapshot flip).

* THE MODEL AUTHORS ONLY ``{decision, reason, reason_code}`` (design §2.2, mirror
  of divider D7). The caller-authored ``candidate_descriptor`` (coverage/lineage
  evidence, completeness signals, generation, prior-snapshot comparison) and
  ``provenance`` are facts the mint-side helper contributes; the model never
  emits them.

* THE MODEL CAN NEITHER SEE NOR SET THE LINEAGE-GUARD VERDICT (design §2.2/§5,
  ruling ③). This is enforced STRUCTURALLY: there is no schema slot anywhere —
  top-level, ``candidate_descriptor``, or any nested object — for a guard
  verdict, and the strict key allowlists reject any attempt to inject one (e.g.
  an ``authoritative_promotion_refused`` or ``guard_verdict`` key). The guard's
  read-only predicted verdict flows ONLY as the ``guard_predicted_verdict``
  parameter of :func:`validate_v_lineage` / :func:`validate_ai_promote_decision`.

The conjunction (design §2.1) — ``promote ⟺ guard-pass AND AI-approve AND
all-validators-pass`` — is enforced by :func:`validate_ai_promote_decision`: the
guard pass is folded into V_LINEAGE (a guard refusal is a V_LINEAGE failure), the
AI approval is ``decision == "promote"``, and any validator failure downgrades a
would-be promote to an effective reject with an F5 keep-incumbent audit. The AI
can only be MORE conservative than the rules — it can turn a rule-ladder promote
into a reject, never the reverse (ruling ④: a failure never flips authority).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Callable

SCHEMA_ID_V1 = "sourcing.organization_asset.ai_promote_decision.v1"

# decision_source (design §2.2): the AI judge is the only source that authors a
# contested verdict; the other three are recorded by the mint-side helper for
# the deterministic pre-branches (§1.1), the guard pre-filter refusal, and the
# ruling-④ keep-incumbent fallback (§4). The validator battery gates ai_judge
# records only — the rest are audit records whose reason strings stay verbatim.
DECISION_SOURCE_AI_JUDGE = "ai_judge"
DECISION_SOURCE_DETERMINISTIC_PREBRANCH = "deterministic_prebranch"
DECISION_SOURCE_GUARD_REFUSED = "guard_refused"
DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT = "fallback_keep_incumbent"
DECISION_SOURCE_VALUES = frozenset(
    {
        DECISION_SOURCE_AI_JUDGE,
        DECISION_SOURCE_DETERMINISTIC_PREBRANCH,
        DECISION_SOURCE_GUARD_REFUSED,
        DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
    }
)

DECISION_PROMOTE = "promote"
DECISION_REJECT = "reject"
DECISION_VALUES = frozenset({DECISION_PROMOTE, DECISION_REJECT})

# reason_code controlled vocabularies (design §2.2/§4). Only contested AI
# decisions use the new vocabulary; deterministic pre-branch and guard-refusal
# records keep the CURRENT enum values verbatim (asset_reuse_planning.py
# :1235/:1244/:1227 pre-branch reasons; storage.py:8675/8681 guard reasons —
# pinned as-is by the S0 oracle, which must NOT drift). The fallback record
# carries a single keep-incumbent marker.
AI_DECISION_REASON_CODES = frozenset(
    {
        # promote-direction codes (the AI judges a genuinely wider/richer candidate)
        "ai_coverage_superset",
        "ai_material_coverage_gain",
        "ai_quality_recovery",
        # reject-direction codes (the AI is more conservative than the ladder)
        "ai_coverage_not_superset",
        "ai_completeness_regression_risk",
        "ai_generation_regression_risk",
        "ai_provenance_suspect",
        "ai_no_material_gain",
    }
)
# Verbatim pre-branch reasons (asset_reuse_planning.py:1227/1235/1244), pinned by
# PromoteDecisionPreBranchCharacterizationTest (S0).
DETERMINISTIC_PREBRANCH_REASON_CODES = frozenset(
    {"no_existing_authoritative", "same_snapshot_refresh", "lifecycle_state_not_promotable"}
)
# Verbatim storage-guard refusal reasons (storage.py:8675/8681), pinned
# PERMANENT-HARD by StorageLineageGuardRefusalShapeCharacterizationTest (S0).
GUARD_REFUSAL_REASON_CODES = frozenset({"stale_generation_sequence_replay", "source_snapshot_coverage_regression"})
FALLBACK_KEEP_INCUMBENT_REASON_CODES = frozenset({"keep_incumbent_judge_fallback"})
REASON_CODE_VALUES = (
    AI_DECISION_REASON_CODES
    | DETERMINISTIC_PREBRANCH_REASON_CODES
    | GUARD_REFUSAL_REASON_CODES
    | FALLBACK_KEEP_INCUMBENT_REASON_CODES
)
# The reason_code subset a given decision_source is allowed to carry. Enforced by
# strict normalization (design §3.3: reason_code membership is part of the F4
# strict validation, unlike the divider where it was validator V7).
_REASON_CODES_BY_SOURCE: dict[str, frozenset[str]] = {
    DECISION_SOURCE_AI_JUDGE: AI_DECISION_REASON_CODES,
    DECISION_SOURCE_DETERMINISTIC_PREBRANCH: DETERMINISTIC_PREBRANCH_REASON_CODES,
    DECISION_SOURCE_GUARD_REFUSED: GUARD_REFUSAL_REASON_CODES,
    DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT: FALLBACK_KEEP_INCUMBENT_REASON_CODES,
}
# decision_sources constrained to reject-only (a keep-incumbent outcome never
# promotes). ai_judge and deterministic_prebranch may promote.
_REJECT_ONLY_SOURCES = frozenset({DECISION_SOURCE_GUARD_REFUSED, DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT})

# Validator ids (design §5 rows). Kept close to the §4 audit-shape example
# (``V_LINEAGE_prefilter`` / ``V_COMP_regression_floor``).
VALIDATOR_V_LINEAGE = "V_LINEAGE_prefilter"
VALIDATOR_V_COMP = "V_COMP_regression_floor"
VALIDATOR_V_GEN = "V_GEN_generation_monotonic"
VALIDATOR_V_PROV = "V_PROV_provenance_sanity"
VALIDATOR_V_LIFECYCLE = "V_LIFECYCLE_promotable"
VALIDATOR_IDS = (
    VALIDATOR_V_LINEAGE,
    VALIDATOR_V_COMP,
    VALIDATOR_V_GEN,
    VALIDATOR_V_PROV,
    VALIDATOR_V_LIFECYCLE,
)

# failures[].validator_id for strict-parse (F4-class) rejections, distinct from
# every battery id so callers map schema failures to `judge_invalid_output` and
# battery failures to `judge_validator_rejected:<validator>` (design §4).
SCHEMA_FAILURE_VALIDATOR_ID = "schema"

VALIDATOR_RESULT_STATUS_PASS = "pass"
VALIDATOR_RESULT_STATUS_FAIL = "fail"
VALIDATOR_RESULT_STATUS_SKIPPED = "skipped"
VALIDATOR_RESULT_STATUS_VALUES = frozenset(
    {VALIDATOR_RESULT_STATUS_PASS, VALIDATOR_RESULT_STATUS_FAIL, VALIDATOR_RESULT_STATUS_SKIPPED}
)

# Ruling-④ failure classes F1-F6 (design §4 table, `fallback_reason` column).
FALLBACK_REASON_MODEL_UNAVAILABLE = "judge_model_unavailable"  # F1
FALLBACK_REASON_CIRCUIT_OPEN = "judge_circuit_open"  # F2
FALLBACK_REASON_CALL_FAILED = "judge_call_failed"  # F3
FALLBACK_REASON_INVALID_OUTPUT = "judge_invalid_output"  # F4
FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX = "judge_validator_rejected:"  # F5
FALLBACK_REASON_INPUT_STALE = "judge_input_stale"  # F6
FALLBACK_REASON_FIXED_VALUES = frozenset(
    {
        FALLBACK_REASON_MODEL_UNAVAILABLE,
        FALLBACK_REASON_CIRCUIT_OPEN,
        FALLBACK_REASON_CALL_FAILED,
        FALLBACK_REASON_INVALID_OUTPUT,
        FALLBACK_REASON_INPUT_STALE,
    }
)

# V_COMP coarse non-regression floor (design §5 + OQ2): retire the six
# subsumption thresholds + four promote branches to AI judgment, but keep a
# coarse floor so the AI's freedom is "within guard-pass and non-regression". The
# tolerance mirrors asset_reuse_planning.py:152
# (_ORGANIZATION_ASSET_PROMOTION_MAX_COMPLETENESS_SCORE_REGRESSION = 1.0).
MAX_COMPLETENESS_SCORE_REGRESSION = 1.0

# Lifecycle statuses that are NOT promotable — standalone mirror of
# asset_reuse_planning.py:154
# (_ORGANIZATION_ASSET_REGISTRY_NON_PROMOTABLE_STATUSES). Deliberately NOT
# imported: the planning constant is the ladder's runtime value, this is the
# RATIFIED acceptance set the battery holds AI output to (mirrors the divider's
# standalone-constants stance).
NON_PROMOTABLE_LIFECYCLE_STATUSES = frozenset({"draft", "partial", "superseded", "archived", "empty"})

MAX_REASON_LENGTH = 240
MAX_JUDGE_ERROR_LENGTH = 500
MAX_DECISION_ID_LENGTH = 64

_SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_DECISION_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_id",
        "decision_id",
        "decision_source",
        "decision",
        "reason",
        "reason_code",
        "candidate_descriptor",
        "provenance",
        "validator_results",
        "fallback",
    }
)
_CANDIDATE_DESCRIPTOR_KEYS = frozenset({"incumbent", "candidate", "coverage_evidence", "prior_snapshot_comparison"})
# Snapshot descriptor: required core + two optional candidate-specific keys.
# NOTE the absence of ANY guard-verdict key — the allowlist STRUCTURALLY forbids
# the model (or caller) from placing the lineage-guard verdict in the descriptor.
_SNAPSHOT_DESCRIPTOR_REQUIRED_KEYS = frozenset(
    {
        "snapshot_id",
        "metrics",
        "completeness_score",
        "completeness_band",
        "selected_snapshot_ids",
        "source_snapshot_count",
        "materialization_generation_sequence",
        "lifecycle_status",
    }
)
_SNAPSHOT_DESCRIPTOR_OPTIONAL_KEYS = frozenset({"materialization_generation_key", "explicit_baseline_inclusion"})
_SNAPSHOT_DESCRIPTOR_KEYS = _SNAPSHOT_DESCRIPTOR_REQUIRED_KEYS | _SNAPSHOT_DESCRIPTOR_OPTIONAL_KEYS
_DESCRIPTOR_METRICS_KEYS = frozenset(
    {
        "candidate_count",
        "evidence_count",
        "profile_detail_count",
        "missing_linkedin_count",
        "profile_completion_backlog_count",
        "effective_lane_total",
    }
)
_COVERAGE_EVIDENCE_KEYS = frozenset({"shards", "request_population_match"})
_COVERAGE_SHARD_KEYS = frozenset(
    {
        "shard_id",
        "lane",
        "search_query",
        "locations",
        "function_ids",
        "job_titles",
        "seniority",
        "query_family",
        "result_count",
        "estimated_total_count",
        "provider_cap_hit",
        "payload_snapshot_sha256",
    }
)
_REQUEST_POPULATION_MATCH_KEYS = frozenset({"candidate_covers_incumbent_shards", "new_shards", "dropped_shards"})
_PRIOR_SNAPSHOT_COMPARISON_KEYS = frozenset(
    {"candidate_sort_key", "incumbent_sort_key", "simulate_or_placeholder_provenance"}
)
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
_FALLBACK_KEYS = frozenset({"decision_source", "fallback_reason", "judge_error", "validator_results", "provenance"})
# The read-only guard-predicted verdict shape (produced by
# predict_lineage_guard_refusal, mirroring storage.py:8683-8695). Passed as a
# parameter, NEVER a payload field.
_GUARD_VERDICT_REQUIRED_KEYS = frozenset({"refused", "reason"})
_GUARD_VERDICT_KEYS = _GUARD_VERDICT_REQUIRED_KEYS | frozenset(
    {
        "incoming_snapshot_id",
        "incoming_generation_key",
        "incoming_generation_sequence",
        "incoming_selected_snapshot_ids",
        "blocking_snapshot_id",
        "blocking_generation_key",
        "blocking_generation_sequence",
        "blocking_selected_snapshot_ids",
    }
)


class PromoteDecisionContractError(ValueError):
    """Fail-closed strict-parse error for the promote-decision contract (F4 class)."""


def fallback_reason_for_validator(validator_id: str) -> str:
    """The F5 `fallback_reason` string for a battery rejection (design §4)."""
    if validator_id not in VALIDATOR_IDS:
        raise PromoteDecisionContractError(f"unknown validator id: {validator_id!r}")
    return f"{FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX}{validator_id}"


def is_valid_fallback_reason(value: str) -> bool:
    """True for the fixed F1-F4/F6 values and well-formed F5 `...:<validator>` forms."""
    text = str(value or "")
    if text in FALLBACK_REASON_FIXED_VALUES:
        return True
    if text.startswith(FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX):
        return text[len(FALLBACK_REASON_VALIDATOR_REJECTED_PREFIX) :] in VALIDATOR_IDS
    return False


# ---------------------------------------------------------------------------
# Normalized dataclasses.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DescriptorMetrics:
    """The metric pair `evaluate` reads today (asset_reuse_planning.py:1254-1269)."""

    candidate_count: int
    evidence_count: int
    profile_detail_count: int
    missing_linkedin_count: int
    profile_completion_backlog_count: int
    effective_lane_total: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_count": self.candidate_count,
            "evidence_count": self.evidence_count,
            "profile_detail_count": self.profile_detail_count,
            "missing_linkedin_count": self.missing_linkedin_count,
            "profile_completion_backlog_count": self.profile_completion_backlog_count,
            "effective_lane_total": self.effective_lane_total,
        }


@dataclass(frozen=True)
class SnapshotDescriptor:
    """One snapshot's coverage/lineage/completeness face — what the AI SEES for a
    contested candidate (design §2.2). Carries NO guard verdict and NO store
    internals."""

    snapshot_id: str
    metrics: DescriptorMetrics
    completeness_score: float
    completeness_band: str
    selected_snapshot_ids: tuple[str, ...]
    source_snapshot_count: int
    materialization_generation_sequence: int
    lifecycle_status: str
    materialization_generation_key: str = ""
    explicit_baseline_inclusion: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "metrics": self.metrics.to_payload(),
            "completeness_score": self.completeness_score,
            "completeness_band": self.completeness_band,
            "selected_snapshot_ids": list(self.selected_snapshot_ids),
            "source_snapshot_count": self.source_snapshot_count,
            "materialization_generation_sequence": self.materialization_generation_sequence,
            "lifecycle_status": self.lifecycle_status,
            "materialization_generation_key": self.materialization_generation_key,
            "explicit_baseline_inclusion": self.explicit_baseline_inclusion,
        }


@dataclass(frozen=True)
class CoverageShard:
    """One shard's request-population evidence (design §2.2 coverage_evidence).

    ``payload_snapshot_sha256`` is nullable per §2.3: a shard with no captured
    provider payload is weaker evidence, not a structural error."""

    shard_id: str
    lane: str
    search_query: str
    locations: tuple[str, ...]
    function_ids: tuple[str, ...]
    job_titles: tuple[str, ...]
    seniority: tuple[str, ...]
    query_family: str
    result_count: int
    estimated_total_count: int
    provider_cap_hit: bool
    payload_snapshot_sha256: str | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "lane": self.lane,
            "search_query": self.search_query,
            "locations": list(self.locations),
            "function_ids": list(self.function_ids),
            "job_titles": list(self.job_titles),
            "seniority": list(self.seniority),
            "query_family": self.query_family,
            "result_count": self.result_count,
            "estimated_total_count": self.estimated_total_count,
            "provider_cap_hit": self.provider_cap_hit,
            "payload_snapshot_sha256": self.payload_snapshot_sha256,
        }


@dataclass(frozen=True)
class RequestPopulationMatch:
    """Per-shard request-population match summary (design §2.2)."""

    candidate_covers_incumbent_shards: bool
    new_shards: tuple[str, ...]
    dropped_shards: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_covers_incumbent_shards": self.candidate_covers_incumbent_shards,
            "new_shards": list(self.new_shards),
            "dropped_shards": list(self.dropped_shards),
        }


@dataclass(frozen=True)
class CoverageEvidence:
    """The recon §2.4 per-shard request-population evidence surface."""

    shards: tuple[CoverageShard, ...]
    request_population_match: RequestPopulationMatch

    def to_payload(self) -> dict[str, Any]:
        return {
            "shards": [shard.to_payload() for shard in self.shards],
            "request_population_match": self.request_population_match.to_payload(),
        }


@dataclass(frozen=True)
class PriorSnapshotComparison:
    """Opaque sort keys + the incident-#2 simulate/placeholder provenance flag."""

    candidate_sort_key: tuple[Any, ...]
    incumbent_sort_key: tuple[Any, ...]
    simulate_or_placeholder_provenance: bool

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_sort_key": list(self.candidate_sort_key),
            "incumbent_sort_key": list(self.incumbent_sort_key),
            "simulate_or_placeholder_provenance": self.simulate_or_placeholder_provenance,
        }


@dataclass(frozen=True)
class CandidateDescriptor:
    """WHAT THE AI SEES (design §2.2): incumbent + candidate faces, coverage
    evidence, prior-snapshot comparison. No guard verdict, no store internals."""

    incumbent: SnapshotDescriptor
    candidate: SnapshotDescriptor
    coverage_evidence: CoverageEvidence
    prior_snapshot_comparison: PriorSnapshotComparison

    def to_payload(self) -> dict[str, Any]:
        return {
            "incumbent": self.incumbent.to_payload(),
            "candidate": self.candidate.to_payload(),
            "coverage_evidence": self.coverage_evidence.to_payload(),
            "prior_snapshot_comparison": self.prior_snapshot_comparison.to_payload(),
        }


@dataclass(frozen=True)
class PromoteProvenance:
    """Model provenance fields (design §2.2 `provenance` object)."""

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
class PromoteFallbackAudit:
    """Ruling-④ keep-incumbent audit object (design §4): reason + truncated judge
    error + whatever validator results ran + provenance when a call was made."""

    decision_source: str
    fallback_reason: str
    judge_error: str
    validator_results: tuple[ValidatorResultRecord, ...]
    provenance: PromoteProvenance | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "decision_source": self.decision_source,
            "fallback_reason": self.fallback_reason,
            "judge_error": self.judge_error,
            "validator_results": [entry.to_payload() for entry in self.validator_results],
            "provenance": self.provenance.to_payload() if self.provenance is not None else None,
        }


@dataclass(frozen=True)
class PromoteDecision:
    """The normalized `ai_promote_decision.v1` record (design §2.2)."""

    schema_id: str
    decision_id: str
    decision_source: str
    decision: str
    reason: str
    reason_code: str
    candidate_descriptor: CandidateDescriptor
    provenance: PromoteProvenance | None
    validator_results: tuple[ValidatorResultRecord, ...]
    fallback: PromoteFallbackAudit | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "decision_id": self.decision_id,
            "decision_source": self.decision_source,
            "decision": self.decision,
            "reason": self.reason,
            "reason_code": self.reason_code,
            "candidate_descriptor": self.candidate_descriptor.to_payload(),
            "provenance": self.provenance.to_payload() if self.provenance is not None else None,
            "validator_results": [entry.to_payload() for entry in self.validator_results],
            "fallback": self.fallback.to_payload() if self.fallback is not None else None,
        }


# ---------------------------------------------------------------------------
# Strict, fail-closed normalization primitives (mirror the divider contract).
# ---------------------------------------------------------------------------


def _require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PromoteDecisionContractError(f"{label} must be an object, got {type(value).__name__}")
    return value


def _require_allowed_keys(payload: Mapping[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = sorted(set(str(key) for key in payload.keys()) - allowed)
    if unknown:
        raise PromoteDecisionContractError(f"{label} carries unknown keys {unknown} (strict allowlist)")


def _require_keys(payload: Mapping[str, Any], required: Iterable[str], label: str) -> None:
    missing = sorted(key for key in required if key not in payload)
    if missing:
        raise PromoteDecisionContractError(f"{label} is missing required keys {missing}")


def _require_str(value: Any, label: str, *, max_length: int | None = None) -> str:
    if not isinstance(value, str):
        raise PromoteDecisionContractError(f"{label} must be a string, got {type(value).__name__}")
    if max_length is not None and len(value) > max_length:
        raise PromoteDecisionContractError(f"{label} exceeds {max_length} chars (got {len(value)})")
    return value


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise PromoteDecisionContractError(f"{label} must be a boolean, got {type(value).__name__}")
    return value


def _require_int(value: Any, label: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PromoteDecisionContractError(f"{label} must be an integer, got {type(value).__name__}")
    if minimum is not None and value < minimum:
        raise PromoteDecisionContractError(f"{label} must be >= {minimum}, got {value}")
    return value


def _require_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PromoteDecisionContractError(f"{label} must be a number, got {type(value).__name__}")
    return float(value)


def _require_str_list(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise PromoteDecisionContractError(f"{label} must be a list of strings")
    return tuple(_require_str(item, f"{label}[{position}]") for position, item in enumerate(value))


def _require_sha256(value: Any, label: str) -> str:
    text = _require_str(value, label)
    if not _SHA256_HEX_RE.fullmatch(text):
        raise PromoteDecisionContractError(f"{label} must be 64 lowercase hex chars")
    return text


def _normalize_metrics(value: Any, label: str) -> DescriptorMetrics:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _DESCRIPTOR_METRICS_KEYS, label)
    _require_keys(payload, _DESCRIPTOR_METRICS_KEYS, label)
    return DescriptorMetrics(
        candidate_count=_require_int(payload["candidate_count"], f"{label}.candidate_count", minimum=0),
        evidence_count=_require_int(payload["evidence_count"], f"{label}.evidence_count", minimum=0),
        profile_detail_count=_require_int(payload["profile_detail_count"], f"{label}.profile_detail_count", minimum=0),
        missing_linkedin_count=_require_int(
            payload["missing_linkedin_count"], f"{label}.missing_linkedin_count", minimum=0
        ),
        profile_completion_backlog_count=_require_int(
            payload["profile_completion_backlog_count"], f"{label}.profile_completion_backlog_count", minimum=0
        ),
        effective_lane_total=_require_int(payload["effective_lane_total"], f"{label}.effective_lane_total", minimum=0),
    )


def normalize_snapshot_descriptor(value: Any, label: str = "descriptor") -> SnapshotDescriptor:
    """Strictly normalize one snapshot face (incumbent or candidate)."""
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _SNAPSHOT_DESCRIPTOR_KEYS, label)
    _require_keys(payload, _SNAPSHOT_DESCRIPTOR_REQUIRED_KEYS, label)
    return SnapshotDescriptor(
        snapshot_id=_require_str(payload["snapshot_id"], f"{label}.snapshot_id"),
        metrics=_normalize_metrics(payload["metrics"], f"{label}.metrics"),
        completeness_score=_require_number(payload["completeness_score"], f"{label}.completeness_score"),
        completeness_band=_require_str(payload["completeness_band"], f"{label}.completeness_band"),
        selected_snapshot_ids=_require_str_list(payload["selected_snapshot_ids"], f"{label}.selected_snapshot_ids"),
        source_snapshot_count=_require_int(
            payload["source_snapshot_count"], f"{label}.source_snapshot_count", minimum=0
        ),
        materialization_generation_sequence=_require_int(
            payload["materialization_generation_sequence"], f"{label}.materialization_generation_sequence", minimum=0
        ),
        lifecycle_status=_require_str(payload["lifecycle_status"], f"{label}.lifecycle_status"),
        materialization_generation_key=_require_str(
            payload.get("materialization_generation_key", ""), f"{label}.materialization_generation_key"
        ),
        explicit_baseline_inclusion=_require_bool(
            payload.get("explicit_baseline_inclusion", False), f"{label}.explicit_baseline_inclusion"
        ),
    )


def _normalize_coverage_shard(value: Any, position: int) -> CoverageShard:
    label = f"shards[{position}]"
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _COVERAGE_SHARD_KEYS, label)
    _require_keys(payload, _COVERAGE_SHARD_KEYS, label)
    raw_hash = payload["payload_snapshot_sha256"]
    payload_snapshot_sha256 = (
        None if raw_hash is None else _require_sha256(raw_hash, f"{label}.payload_snapshot_sha256")
    )
    return CoverageShard(
        shard_id=_require_str(payload["shard_id"], f"{label}.shard_id"),
        lane=_require_str(payload["lane"], f"{label}.lane"),
        search_query=_require_str(payload["search_query"], f"{label}.search_query"),
        locations=_require_str_list(payload["locations"], f"{label}.locations"),
        function_ids=_require_str_list(payload["function_ids"], f"{label}.function_ids"),
        job_titles=_require_str_list(payload["job_titles"], f"{label}.job_titles"),
        seniority=_require_str_list(payload["seniority"], f"{label}.seniority"),
        query_family=_require_str(payload["query_family"], f"{label}.query_family"),
        result_count=_require_int(payload["result_count"], f"{label}.result_count", minimum=0),
        estimated_total_count=_require_int(
            payload["estimated_total_count"], f"{label}.estimated_total_count", minimum=0
        ),
        provider_cap_hit=_require_bool(payload["provider_cap_hit"], f"{label}.provider_cap_hit"),
        payload_snapshot_sha256=payload_snapshot_sha256,
    )


def _normalize_request_population_match(value: Any, label: str) -> RequestPopulationMatch:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _REQUEST_POPULATION_MATCH_KEYS, label)
    _require_keys(payload, _REQUEST_POPULATION_MATCH_KEYS, label)
    return RequestPopulationMatch(
        candidate_covers_incumbent_shards=_require_bool(
            payload["candidate_covers_incumbent_shards"], f"{label}.candidate_covers_incumbent_shards"
        ),
        new_shards=_require_str_list(payload["new_shards"], f"{label}.new_shards"),
        dropped_shards=_require_str_list(payload["dropped_shards"], f"{label}.dropped_shards"),
    )


def _normalize_coverage_evidence(value: Any, label: str) -> CoverageEvidence:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _COVERAGE_EVIDENCE_KEYS, label)
    _require_keys(payload, _COVERAGE_EVIDENCE_KEYS, label)
    raw_shards = payload["shards"]
    if not isinstance(raw_shards, Sequence) or isinstance(raw_shards, (str, bytes)):
        raise PromoteDecisionContractError(f"{label}.shards must be a list of shard objects")
    return CoverageEvidence(
        shards=tuple(_normalize_coverage_shard(raw, position) for position, raw in enumerate(raw_shards)),
        request_population_match=_normalize_request_population_match(
            payload["request_population_match"], f"{label}.request_population_match"
        ),
    )


def _normalize_opaque_sort_key(value: Any, label: str) -> tuple[Any, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise PromoteDecisionContractError(f"{label} must be a list")
    return tuple(value)


def _normalize_prior_snapshot_comparison(value: Any, label: str) -> PriorSnapshotComparison:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _PRIOR_SNAPSHOT_COMPARISON_KEYS, label)
    _require_keys(payload, _PRIOR_SNAPSHOT_COMPARISON_KEYS, label)
    return PriorSnapshotComparison(
        candidate_sort_key=_normalize_opaque_sort_key(payload["candidate_sort_key"], f"{label}.candidate_sort_key"),
        incumbent_sort_key=_normalize_opaque_sort_key(payload["incumbent_sort_key"], f"{label}.incumbent_sort_key"),
        simulate_or_placeholder_provenance=_require_bool(
            payload["simulate_or_placeholder_provenance"], f"{label}.simulate_or_placeholder_provenance"
        ),
    )


def normalize_candidate_descriptor(value: Any, label: str = "candidate_descriptor") -> CandidateDescriptor:
    """Strictly normalize the AI-visible descriptor (design §2.2)."""
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _CANDIDATE_DESCRIPTOR_KEYS, label)
    _require_keys(payload, _CANDIDATE_DESCRIPTOR_KEYS, label)
    return CandidateDescriptor(
        incumbent=normalize_snapshot_descriptor(payload["incumbent"], f"{label}.incumbent"),
        candidate=normalize_snapshot_descriptor(payload["candidate"], f"{label}.candidate"),
        coverage_evidence=_normalize_coverage_evidence(payload["coverage_evidence"], f"{label}.coverage_evidence"),
        prior_snapshot_comparison=_normalize_prior_snapshot_comparison(
            payload["prior_snapshot_comparison"], f"{label}.prior_snapshot_comparison"
        ),
    )


def _normalize_provenance(value: Any, label: str) -> PromoteProvenance:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _PROVENANCE_KEYS, label)
    _require_keys(payload, _PROVENANCE_KEYS, label)
    usage = _require_mapping(payload["usage"], f"{label}.usage")
    _require_allowed_keys(usage, _USAGE_KEYS, f"{label}.usage")
    _require_keys(usage, _USAGE_KEYS, f"{label}.usage")
    return PromoteProvenance(
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
        raise PromoteDecisionContractError(f"{label} must be a list of validator result objects")
    results: list[ValidatorResultRecord] = []
    for position, raw_entry in enumerate(value):
        entry_label = f"{label}[{position}]"
        entry = _require_mapping(raw_entry, entry_label)
        _require_allowed_keys(entry, _VALIDATOR_RESULT_KEYS, entry_label)
        _require_keys(entry, _VALIDATOR_RESULT_REQUIRED_KEYS, entry_label)
        status = _require_str(entry["status"], f"{entry_label}.status")
        if status not in VALIDATOR_RESULT_STATUS_VALUES:
            raise PromoteDecisionContractError(
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


def normalize_fallback_audit(value: Any, label: str = "fallback") -> PromoteFallbackAudit:
    """Strictly normalize a ruling-④ keep-incumbent audit object (design §4 shape).

    Public for the shadow-slice audit surface (S3), where the fallback is stored
    at ``metadata.ai_promote_decision.fallback``."""
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _FALLBACK_KEYS, label)
    _require_keys(payload, ("decision_source", "fallback_reason", "judge_error", "validator_results"), label)
    decision_source = _require_str(payload["decision_source"], f"{label}.decision_source")
    if decision_source != DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT:
        raise PromoteDecisionContractError(
            f"{label}.decision_source must be {DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT!r}, got {decision_source!r}"
        )
    fallback_reason = _require_str(payload["fallback_reason"], f"{label}.fallback_reason")
    if not is_valid_fallback_reason(fallback_reason):
        raise PromoteDecisionContractError(f"{label}.fallback_reason {fallback_reason!r} is not a ruling-④ F1-F6 value")
    judge_error = _require_str(payload["judge_error"], f"{label}.judge_error", max_length=MAX_JUDGE_ERROR_LENGTH)
    raw_provenance = payload.get("provenance")
    provenance = _normalize_provenance(raw_provenance, f"{label}.provenance") if raw_provenance is not None else None
    return PromoteFallbackAudit(
        decision_source=decision_source,
        fallback_reason=fallback_reason,
        judge_error=judge_error,
        validator_results=_normalize_validator_results(payload["validator_results"], f"{label}.validator_results"),
        provenance=provenance,
    )


def _normalize_v1(payload: Mapping[str, Any]) -> PromoteDecision:
    _require_allowed_keys(payload, _TOP_LEVEL_KEYS, "decision")
    _require_keys(payload, _TOP_LEVEL_KEYS, "decision")
    decision_id = _require_str(payload["decision_id"], "decision_id", max_length=MAX_DECISION_ID_LENGTH)
    if not _DECISION_ID_RE.fullmatch(decision_id):
        raise PromoteDecisionContractError("decision_id must be 1-64 chars of [A-Za-z0-9_-] (ULID-compatible identity)")
    decision_source = _require_str(payload["decision_source"], "decision_source")
    if decision_source not in DECISION_SOURCE_VALUES:
        raise PromoteDecisionContractError(
            f"decision_source must be one of {sorted(DECISION_SOURCE_VALUES)}, got {decision_source!r}"
        )
    decision = _require_str(payload["decision"], "decision")
    if decision not in DECISION_VALUES:
        raise PromoteDecisionContractError(f"decision must be one of {sorted(DECISION_VALUES)}, got {decision!r}")
    if decision == DECISION_PROMOTE and decision_source in _REJECT_ONLY_SOURCES:
        raise PromoteDecisionContractError(f"decision_source {decision_source!r} may never carry a promote decision")
    reason = _require_str(payload["reason"], "reason", max_length=MAX_REASON_LENGTH)
    if not reason.strip():
        raise PromoteDecisionContractError("reason must be non-empty (design §3.3)")
    reason_code = _require_str(payload["reason_code"], "reason_code")
    allowed_codes = _REASON_CODES_BY_SOURCE[decision_source]
    if reason_code not in allowed_codes:
        raise PromoteDecisionContractError(
            f"reason_code {reason_code!r} is not in the {decision_source} vocabulary {sorted(allowed_codes)}"
        )
    candidate_descriptor = normalize_candidate_descriptor(payload["candidate_descriptor"], "candidate_descriptor")
    raw_provenance = payload["provenance"]
    provenance = _normalize_provenance(raw_provenance, "provenance") if raw_provenance is not None else None
    raw_fallback = payload["fallback"]
    fallback = normalize_fallback_audit(raw_fallback, "fallback") if raw_fallback is not None else None
    if decision_source == DECISION_SOURCE_AI_JUDGE:
        if provenance is None:
            raise PromoteDecisionContractError("ai_judge decisions must carry model provenance")
        if fallback is not None:
            raise PromoteDecisionContractError("ai_judge decisions must carry fallback=null")
    elif decision_source == DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT:
        if fallback is None:
            raise PromoteDecisionContractError(
                "fallback_keep_incumbent decisions must carry the ruling-④ keep-incumbent audit object"
            )
    else:  # deterministic_prebranch / guard_refused
        if fallback is not None:
            raise PromoteDecisionContractError(f"{decision_source} decisions must carry fallback=null")
    return PromoteDecision(
        schema_id=SCHEMA_ID_V1,
        decision_id=decision_id,
        decision_source=decision_source,
        decision=decision,
        reason=reason,
        reason_code=reason_code,
        candidate_descriptor=candidate_descriptor,
        provenance=provenance,
        validator_results=_normalize_validator_results(payload["validator_results"], "validator_results"),
        fallback=fallback,
    )


_SCHEMA_NORMALIZERS: dict[str, Callable[[Mapping[str, Any]], PromoteDecision]] = {
    SCHEMA_ID_V1: _normalize_v1,
}


def normalize_ai_promote_decision(payload: Any) -> PromoteDecision:
    """Strict, fail-closed normalization by EXACT schema-id lookup (no upgrade)."""
    mapping = _require_mapping(payload, "decision payload")
    schema_id = mapping.get("schema_id")
    normalizer = _SCHEMA_NORMALIZERS.get(schema_id) if isinstance(schema_id, str) else None
    if normalizer is None:
        raise PromoteDecisionContractError(
            f"unknown schema_id {schema_id!r}: exact-version lookup accepts only "
            f"{sorted(_SCHEMA_NORMALIZERS)} (fail-closed, no auto-upgrade)"
        )
    return normalizer(mapping)


# ---------------------------------------------------------------------------
# The read-only lineage-guard verdict predictor (mirror of storage.py:8670-8681).
# The AI never sees this; the mint-side helper computes it and passes it to
# V_LINEAGE as a parameter.
# ---------------------------------------------------------------------------


def predict_lineage_guard_refusal(
    *,
    incoming_snapshot_id: str,
    incoming_generation_key: str,
    incoming_generation_sequence: int,
    incoming_selected_snapshot_ids: Iterable[str],
    incumbent_snapshot_id: str,
    incumbent_generation_key: str,
    incumbent_generation_sequence: int,
    incumbent_selected_snapshot_ids: Iterable[str],
) -> dict[str, Any]:
    """Read-only mirror of the storage lineage guard's two-refusal predicate
    (storage.py:8670-8681), returning the ``authoritative_promotion_refused``
    shape (storage.py:8683-8695) so the mint helper can compute the guard verdict
    WITHOUT the in-transaction row read.

    S0 pin: StorageLineageGuardRefusalShapeCharacterizationTest
    (tests/test_organization_promote_characterization.py) — the two refusal
    shapes are PERMANENT-HARD and this predictor must reproduce them exactly. The
    storage guard stays the FINAL in-transaction backstop (design §2.1); this is
    only the early pre-filter so the AI never judges a guard-refused candidate.
    """
    incoming_key = str(incoming_generation_key or "").strip()
    incumbent_key = str(incumbent_generation_key or "").strip()
    incoming_ids = {str(value or "").strip() for value in incoming_selected_snapshot_ids if str(value or "").strip()}
    incumbent_ids = {str(value or "").strip() for value in incumbent_selected_snapshot_ids if str(value or "").strip()}
    reason = ""
    if (
        incoming_key
        and incoming_key == incumbent_key
        and int(incoming_generation_sequence) < int(incumbent_generation_sequence)
    ):
        reason = "stale_generation_sequence_replay"
    elif incoming_ids and incumbent_ids and incoming_ids < incumbent_ids:
        reason = "source_snapshot_coverage_regression"
    if not reason:
        return {"refused": False, "reason": ""}
    return {
        "refused": True,
        "reason": reason,
        "incoming_snapshot_id": str(incoming_snapshot_id or ""),
        "incoming_generation_key": incoming_key,
        "incoming_generation_sequence": int(incoming_generation_sequence),
        "incoming_selected_snapshot_ids": sorted(incoming_ids),
        "blocking_snapshot_id": str(incumbent_snapshot_id or ""),
        "blocking_generation_key": incumbent_key,
        "blocking_generation_sequence": int(incumbent_generation_sequence),
        "blocking_selected_snapshot_ids": sorted(incumbent_ids),
    }


def _normalize_guard_predicted_verdict(value: Any, label: str = "guard_predicted_verdict") -> Mapping[str, Any]:
    payload = _require_mapping(value, label)
    _require_allowed_keys(payload, _GUARD_VERDICT_KEYS, label)
    _require_keys(payload, _GUARD_VERDICT_REQUIRED_KEYS, label)
    _require_bool(payload["refused"], f"{label}.refused")
    _require_str(payload["reason"], f"{label}.reason")
    return payload


# ---------------------------------------------------------------------------
# Validator battery (design §5) — pure functions, one per row. Each returns None
# on pass or a human-auditable failure reason on FAIL; any FAIL maps to ruling-④
# fallback class F5 via ``fallback_reason_for_validator(<id>)``.
# ---------------------------------------------------------------------------


def validate_v_lineage(*, guard_predicted_verdict: Mapping[str, Any]) -> str | None:
    """V_LINEAGE — a promote can NEVER override a guard refusal (ruling ③).

    Retained rule: the storage lineage guard (design §1.4). The guard's read-only
    predicted verdict arrives as an INPUT parameter — the AI never authors or
    sees it (there is no schema slot for it, see the module docstring). The AI can
    only be MORE conservative: a guard refusal is a hard V_LINEAGE failure that no
    AI-approve can lift. The storage guard (storage.py:8633) stays the final
    in-transaction backstop.

    S0 pin: StorageLineageGuardRefusalShapeCharacterizationTest
    (tests/test_organization_promote_characterization.py) — the PERMANENT-HARD
    two-refusal shapes (stale_generation_sequence_replay,
    source_snapshot_coverage_regression) this pre-filter mirrors.
    """
    verdict = _normalize_guard_predicted_verdict(guard_predicted_verdict)
    if bool(verdict["refused"]):
        reason = str(verdict["reason"]) or "unspecified"
        return f"lineage guard would refuse this candidate ({reason}): promote can never override a guard refusal"
    return None


def validate_v_comp(
    *,
    candidate: SnapshotDescriptor,
    incumbent: SnapshotDescriptor,
    completeness_regression_tolerance: float = MAX_COMPLETENESS_SCORE_REGRESSION,
) -> str | None:
    """V_COMP — coarse completeness/coverage non-regression floor (design §5 + OQ2).

    Retained rule (coarse, per OQ2): the six subsumption thresholds + four promote
    branches retire to AI judgment, but the AI may NEVER promote a candidate that
    (a) regresses completeness_score beyond the tolerance, or (b) is strictly
    narrower in effective lane coverage than the incumbent. An AI-approve of a
    regressing candidate still FAILS here.

    Anchors: ``_ORGANIZATION_ASSET_PROMOTION_MAX_COMPLETENESS_SCORE_REGRESSION``
    (asset_reuse_planning.py:152, the score tolerance) and the
    ``coverage_materially_higher`` shape (:1311) as the meaning of "not a
    regression". S0 pins: ``test_stable_quality_branch_score_gap_edge_is_1_0``
    (the 1.0 score-gap edge) and
    ``test_count_098_relaxation_is_neutralized_candidate_must_reach_existing``
    (coverage-shortfall reject). Interpretation note (design §5 ambiguity, OQ2 at
    S5): this is a COARSE floor, deliberately weaker than the ladder's six
    subsumption checks — the AI owns the finer judgment above this floor.
    """
    score_gap = incumbent.completeness_score - candidate.completeness_score
    if score_gap > completeness_regression_tolerance:
        return (
            f"completeness_score regresses {score_gap:.4g} (incumbent {incumbent.completeness_score:.4g} - "
            f"candidate {candidate.completeness_score:.4g}) beyond the tolerance {completeness_regression_tolerance:.4g}"
        )
    if candidate.metrics.effective_lane_total < incumbent.metrics.effective_lane_total:
        return (
            f"effective lane coverage is strictly narrower: candidate {candidate.metrics.effective_lane_total} "
            f"< incumbent {incumbent.metrics.effective_lane_total}"
        )
    return None


def validate_v_gen(*, candidate: SnapshotDescriptor, incumbent: SnapshotDescriptor) -> str | None:
    """V_GEN — generation monotonicity: never make an authoritative generation
    sequence go backward within a lineage (design §5).

    Retained rule: the generation sequence guard (storage.py:8670-8675,
    ``stale_generation_sequence_replay``) restated as a plan-time validator. A
    promote whose candidate shares the incumbent's
    ``materialization_generation_key`` but carries a LOWER sequence is a stale
    replay and FAILS. Cross-lineage candidates (different generation key) are not
    monotonicity-comparable — their coverage relationship is V_LINEAGE's domain —
    so V_GEN passes them here.

    S0 pin: ``test_stale_generation_sequence_replay_refusal_shape`` (the same-key
    lower-sequence refusal). Interpretation note (design §5 says "candidate's
    generation ≥ incumbent's"): the guard only refuses SAME-key lower-sequence, so
    V_GEN mirrors that scope rather than comparing sequences across unrelated
    lineages.
    """
    if (
        candidate.materialization_generation_key
        and candidate.materialization_generation_key == incumbent.materialization_generation_key
        and candidate.materialization_generation_sequence < incumbent.materialization_generation_sequence
    ):
        return (
            f"generation sequence regresses within lineage {candidate.materialization_generation_key!r}: "
            f"candidate {candidate.materialization_generation_sequence} "
            f"< incumbent {incumbent.materialization_generation_sequence}"
        )
    return None


def validate_v_prov(
    *,
    candidate_descriptor: CandidateDescriptor,
    provenance: PromoteProvenance | None,
) -> str | None:
    """V_PROV — provenance sanity (design §5): reject simulate/placeholder-tainted
    candidates, require lineage evidence, and require the decision to carry model
    provenance.

    Retained lessons (incidents #1/#2, design §1.6): the Google simulate
    40-placeholder incident (a placeholder-tainted snapshot stayed authoritative)
    means simulate/placeholder provenance is a HARD disqualifier — an AI-approve
    of such a candidate still FAILS. "Lineage evidence present" = the candidate
    carries at least one ``selected_snapshot_ids`` entry. "Provenance completeness"
    = the decision carries the required model provenance fields (design §2.2).

    No direct S0 pin exists (the S0 oracle pins the threshold ladder, not the
    simulate-provenance flag); this validator IS the origin of that pin, mirroring
    the divider's V9/V10 new-rule stance. Cross-ref: recon §2.3 (the simulate
    40-placeholder incident).
    """
    if candidate_descriptor.prior_snapshot_comparison.simulate_or_placeholder_provenance:
        return "candidate carries simulate/placeholder provenance (incident #2): a hard promote disqualifier"
    if not candidate_descriptor.candidate.selected_snapshot_ids:
        return "candidate carries no selected_snapshot_ids: lineage evidence absent"
    if provenance is None:
        return "decision carries no model provenance (design §2.2 requires it for an ai_judge promote)"
    missing = [
        field
        for field, value in (
            ("model_provider", provenance.model_provider),
            ("requested_model", provenance.requested_model),
            ("response_model", provenance.response_model),
            ("prompt_sha256", provenance.prompt_sha256),
            ("input_snapshot_sha256", provenance.input_snapshot_sha256),
        )
        if not str(value or "").strip()
    ]
    if missing:
        return f"provenance is missing required fields: {missing}"
    return None


def validate_v_lifecycle(*, candidate: SnapshotDescriptor) -> str | None:
    """V_LIFECYCLE — the candidate lifecycle must be promotable (design §5).

    Retained rule: the §1.1 pre-branch (asset_reuse_planning.py:1224, via
    ``organization_asset_lifecycle_promotable``). A candidate whose normalized
    lifecycle status is in the non-promotable set (draft/partial/superseded/
    archived/empty) FAILS.

    S0 pin: ``test_lifecycle_not_promotable_rejects_with_thin_record``
    (the archived-candidate pre-branch reject).
    """
    status = str(candidate.lifecycle_status or "").strip().lower().replace("-", "_")
    if status in {"complete", "completed"}:
        status = "ready"
    if status in NON_PROMOTABLE_LIFECYCLE_STATUSES:
        return f"candidate lifecycle status {status!r} is not promotable"
    return None


def build_keep_incumbent_fallback(
    *,
    fallback_reason: str,
    judge_error: str = "",
    validator_results: Sequence[ValidatorResultRecord] = (),
    provenance: PromoteProvenance | None = None,
) -> PromoteFallbackAudit:
    """Assemble a ruling-④ keep-incumbent audit (design §4). Fail-closed on an
    unknown ``fallback_reason``."""
    if not is_valid_fallback_reason(fallback_reason):
        raise PromoteDecisionContractError(f"fallback_reason {fallback_reason!r} is not a ruling-④ F1-F6 value")
    return PromoteFallbackAudit(
        decision_source=DECISION_SOURCE_FALLBACK_KEEP_INCUMBENT,
        fallback_reason=fallback_reason,
        judge_error=str(judge_error or "")[:MAX_JUDGE_ERROR_LENGTH],
        validator_results=tuple(validator_results),
        provenance=provenance,
    )


def validate_ai_promote_decision(
    payload: Any,
    *,
    guard_predicted_verdict: Mapping[str, Any],
    incumbent_descriptor: Any,
    completeness_regression_tolerance: float = MAX_COMPLETENESS_SCORE_REGRESSION,
) -> dict[str, Any]:
    """Normalize + run the §5 battery, enforcing the design §2.1 conjunction.

    ``promote ⟺ guard-pass AND AI-approve AND all-validators-pass``. The guard
    pass is folded into V_LINEAGE (the guard's read-only predicted verdict arrives
    as ``guard_predicted_verdict`` — the model never authors it); AI-approve is
    ``decision == "promote"``. A would-be promote that fails ANY validator is
    downgraded to an EFFECTIVE REJECT with an F5 keep-incumbent audit — the AI can
    only be more conservative than the rules, never less (ruling ④: a failure
    never flips authority).

    Returns ``{"valid", "failures": [{"validator_id", "reason"}...], "normalized",
    "validator_results", "effective_decision", "fallback"}``:

    * ``valid`` — True unless the record is schema-invalid OR the AI authored a
      promote that a hard floor (guard/validator) rejects. An honest reject is
      always valid.
    * ``effective_decision`` — ``"promote"`` only when the AI approved AND every
      floor passed; otherwise ``"reject"`` (keep the incumbent).
    * ``fallback`` — the F5 keep-incumbent audit payload when a would-be promote
      was blocked by a floor; ``None`` otherwise.

    Fail-closed on unknown ``schema_id`` (exact-version lookup, no auto-upgrade):
    a single ``validator_id="schema"`` failure, ``normalized=None``,
    ``effective_decision="reject"`` (F4 class). Battery scope: the §5 battery gates
    ``ai_judge`` records only; deterministic pre-branch / guard-refusal /
    keep-incumbent audit records are well-formed audit rows whose verdicts are not
    re-judged — every battery row reports ``skipped``.
    """
    try:
        decision = normalize_ai_promote_decision(payload)
    except PromoteDecisionContractError as error:
        return {
            "valid": False,
            "failures": [{"validator_id": SCHEMA_FAILURE_VALIDATOR_ID, "reason": str(error)}],
            "normalized": None,
            "validator_results": [],
            "effective_decision": DECISION_REJECT,
            "fallback": None,
        }

    if decision.decision_source != DECISION_SOURCE_AI_JUDGE:
        skip_reason = "audit record: the §5 battery gates ai_judge decisions only"
        validator_results = [
            {"validator": validator_id, "status": VALIDATOR_RESULT_STATUS_SKIPPED, "reason": skip_reason}
            for validator_id in VALIDATOR_IDS
        ]
        return {
            "valid": True,
            "failures": [],
            "normalized": decision.to_payload(),
            "validator_results": validator_results,
            "effective_decision": decision.decision,
            "fallback": decision.fallback.to_payload() if decision.fallback is not None else None,
        }

    incumbent = normalize_snapshot_descriptor(incumbent_descriptor, "incumbent_descriptor")
    candidate = decision.candidate_descriptor.candidate
    outcomes: list[tuple[str, str | None]] = [
        (VALIDATOR_V_LINEAGE, validate_v_lineage(guard_predicted_verdict=guard_predicted_verdict)),
        (
            VALIDATOR_V_COMP,
            validate_v_comp(
                candidate=candidate,
                incumbent=incumbent,
                completeness_regression_tolerance=completeness_regression_tolerance,
            ),
        ),
        (VALIDATOR_V_GEN, validate_v_gen(candidate=candidate, incumbent=incumbent)),
        (
            VALIDATOR_V_PROV,
            validate_v_prov(candidate_descriptor=decision.candidate_descriptor, provenance=decision.provenance),
        ),
        (VALIDATOR_V_LIFECYCLE, validate_v_lifecycle(candidate=candidate)),
    ]

    failures: list[dict[str, str]] = []
    validator_results = []
    for validator_id, failure_reason in outcomes:
        if failure_reason is not None:
            validator_results.append(
                {"validator": validator_id, "status": VALIDATOR_RESULT_STATUS_FAIL, "reason": failure_reason}
            )
            failures.append({"validator_id": validator_id, "reason": failure_reason})
        else:
            validator_results.append({"validator": validator_id, "status": VALIDATOR_RESULT_STATUS_PASS})

    ai_promote = decision.decision == DECISION_PROMOTE
    all_pass = not failures
    effective_decision = DECISION_PROMOTE if (ai_promote and all_pass) else DECISION_REJECT
    # A promote is admissible only when every floor passes; an honest reject is
    # always valid (ruling ④: no-promotion is always a safe state).
    valid = (not ai_promote) or all_pass

    fallback_payload: dict[str, Any] | None = None
    if ai_promote and failures:
        # F5: the first failing floor names the keep-incumbent fallback reason.
        blocking = failures[0]["validator_id"]
        fallback_payload = build_keep_incumbent_fallback(
            fallback_reason=fallback_reason_for_validator(blocking),
            judge_error="",
            validator_results=tuple(
                ValidatorResultRecord(
                    validator=str(entry["validator"]),
                    status=str(entry["status"]),
                    reason=str(entry.get("reason", "")),
                )
                for entry in validator_results
            ),
            provenance=decision.provenance,
        ).to_payload()

    return {
        "valid": valid,
        "failures": failures,
        "normalized": decision.to_payload(),
        "validator_results": validator_results,
        "effective_decision": effective_decision,
        "fallback": fallback_payload,
    }
