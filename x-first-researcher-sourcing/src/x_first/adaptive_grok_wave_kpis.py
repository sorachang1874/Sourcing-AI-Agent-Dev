from __future__ import annotations

import hashlib
import json
import os
import stat
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from x_first import adaptive_grok_wave_runner as runner
from x_first.native_x_evidence_contract import SUPPORT_DIMENSIONS, TEMPORAL_STATES
from x_first.recall_pool_schema import MiniDraft202012Error, assert_schema_valid

KPI_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.kpi_summary.v1"
RESULT_SCHEMA_FILE = "x.grok.adaptive_recall_wave.result.v2.schema.json"
SUPPORTED_RECEIPT_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.operator_receipt.v3"

EVIDENCE_KINDS = ("bio", "mention", "post", "thread")
THREAD_RELATIONS = ("not_applicable", "self_post", "reply", "quote", "thread_root", "thread_reply")
POST_LIKE_RELATIONS = frozenset({"self_post", "quote", "thread_root"})
REPLY_LIKE_RELATIONS = frozenset({"reply", "thread_reply"})
NATIVE_X_TOOLS = ("x_keyword_search", "x_semantic_search", "x_thread_fetch", "x_user_search")
DIMENSIONS = tuple(sorted(SUPPORT_DIMENSIONS))
STATE_LABELS = ("current", "historical", "ambiguous", "unsupported")
PROPOSAL_LABELS = (*STATE_LABELS, "conflict", "none")
MAX_PRIVATE_JSON_BYTES = 67_108_864


class AdaptiveWaveKPIError(ValueError):
    """Raised when a source cannot support a replayable aggregate KPI summary."""


def _reject_constant(value: str) -> None:
    raise AdaptiveWaveKPIError(f"non_finite_json_number:{value}")


def _closed_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise AdaptiveWaveKPIError("duplicate_json_key")
        result[key] = value
    return result


def _read_owner_only_json(path: Path, *, maximum_bytes: int = MAX_PRIVATE_JSON_BYTES) -> tuple[Any, bytes]:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AdaptiveWaveKPIError("private_json_unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_size < 1
            or opened.st_size > maximum_bytes
        ):
            raise AdaptiveWaveKPIError("private_json_metadata_invalid")
        chunks: list[bytes] = []
        remaining = opened.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                raise AdaptiveWaveKPIError("private_json_truncated")
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise AdaptiveWaveKPIError("private_json_grew")
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size) != (opened.st_dev, opened.st_ino, opened.st_size):
            raise AdaptiveWaveKPIError("private_json_changed")
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    try:
        payload = json.loads(
            raw,
            object_pairs_hook=_closed_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise AdaptiveWaveKPIError("private_json_invalid") from exc
    return payload, raw


def _ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _coverage(numerator: int, denominator: int) -> dict[str, int | float | None]:
    return {
        "covered_candidates": numerator,
        "denominator_candidates": denominator,
        "coverage_rate": _ratio(numerator, denominator),
    }


def _empty_matrix(labels: Sequence[str]) -> dict[str, dict[str, int]]:
    return {row: {column: 0 for column in labels} for row in labels}


def _resolve_proposal(values: set[str]) -> str:
    if not values:
        return "none"
    if len(values) > 1:
        return "conflict"
    return next(iter(values))


def _delta(model_reported: int, ledger: int) -> dict[str, int]:
    return {
        "model_reported": model_reported,
        "ledger": ledger,
        "delta_from_ledger": model_reported - ledger,
    }


def _post_url_count(candidates: Sequence[Mapping[str, Any]]) -> int:
    count = 0
    for candidate in candidates:
        for evidence in candidate["evidence"]:
            if runner._is_post_url(evidence.get("url")):
                count += 1
    return count


def _validate_pair_bindings(result: Mapping[str, Any], receipt: Mapping[str, Any], sanitized_raw: bytes) -> None:
    if result.get("schema_version") is not None:
        raise AdaptiveWaveKPIError("result_schema_version_field_unexpected")
    try:
        assert_schema_valid(result, RESULT_SCHEMA_FILE)
    except MiniDraft202012Error as exc:
        raise AdaptiveWaveKPIError("result_schema_invalid") from exc
    receipt_errors = runner.validate_operator_receipt(receipt)
    if receipt_errors:
        raise AdaptiveWaveKPIError("receipt_contract_invalid")
    if (
        receipt.get("schema_version") != SUPPORTED_RECEIPT_SCHEMA_VERSION
        or receipt.get("execution_mode") != "live"
        or receipt.get("status") != "completed"
        or receipt.get("session_proof", {}).get("status") != "verified"
        or receipt.get("reconciliation", {}).get("tool_fact_source") != "session_transcript_verified"
    ):
        raise AdaptiveWaveKPIError("completed_verified_live_receipt_required")
    if hashlib.sha256(sanitized_raw).hexdigest() != receipt.get("artifacts", {}).get("sanitized_output_sha256"):
        raise AdaptiveWaveKPIError("sanitized_result_hash_mismatch")

    candidates = result["candidates"]
    evidence_count = sum(len(candidate["evidence"]) for candidate in candidates)
    receipt_reconciliation = receipt["reconciliation"]
    actual_counts = {
        "candidate_count": len(candidates),
        "evidence_count": evidence_count,
        "post_url_count": _post_url_count(candidates),
    }
    if any(receipt_reconciliation[key] != value for key, value in actual_counts.items()):
        raise AdaptiveWaveKPIError("result_receipt_count_mismatch")
    result_handles = sorted(candidate["handle"].casefold() for candidate in candidates)
    coverage_handles = [row["handle_key"] for row in receipt_reconciliation["candidate_surface_coverage"]]
    if coverage_handles != result_handles:
        raise AdaptiveWaveKPIError("result_receipt_surface_population_mismatch")

    local = result["local_reconciliation"]
    provenance = result["native_x_tool_provenance"]
    proof = receipt["session_proof"]
    if (
        result["counts"]["candidates_retained"] != len(candidates)
        or local["candidate_records_validated"] != len(candidates)
        or local["evidence_items_validated"] != evidence_count
        or local["post_urls_structurally_validated"] != actual_counts["post_url_count"]
        or local["tool_calls_completed"] != proof["completed_tool_calls"]
        or provenance["tool_calls_reported"] != proof["completed_tool_calls"]
        or local["tool_counts"] != proof["tool_counts"]
    ):
        raise AdaptiveWaveKPIError("result_receipt_reconciliation_mismatch")


def _summarize_validated_pair(
    result: Mapping[str, Any],
    receipt: Mapping[str, Any],
    *,
    validation_basis: str,
) -> dict[str, Any]:
    candidates = result["candidates"]
    candidate_count = len(candidates)
    proof = receipt["session_proof"]
    completed_calls = proof["completed_tool_calls"]
    elapsed_ms = receipt["process"]["elapsed_ms"]

    evidence_counts_by_kind: Counter[str] = Counter()
    evidence_counts_by_relation: Counter[str] = Counter()
    candidates_with_post = 0
    candidates_with_reply = 0
    post_only = 0
    reply_only = 0
    both = 0
    neither = 0
    state_matrix = _empty_matrix(STATE_LABELS)
    proposal_matrix = _empty_matrix(PROPOSAL_LABELS)
    proposal_alignment = {
        dimension: {"matches": 0, "differs": 0, "conflict": 0, "none": 0}
        for dimension in DIMENSIONS
    }
    stable_id_count = 0
    bio_count = 0
    both_profile_fields = 0

    for candidate in candidates:
        evidence_rows = candidate["evidence"]
        has_post = False
        has_reply = False
        proposal_values = {dimension: set() for dimension in DIMENSIONS}
        for evidence in evidence_rows:
            evidence_counts_by_kind[evidence["kind"]] += 1
            relation = evidence["thread_relation"] or "not_applicable"
            evidence_counts_by_relation[relation] += 1
            has_post = has_post or relation in POST_LIKE_RELATIONS
            has_reply = has_reply or relation in REPLY_LIKE_RELATIONS
            for claim in evidence["supports"]:
                dimension = claim["dimension"]
                asserted_value = claim["asserted_value"]
                if dimension in proposal_values and asserted_value in TEMPORAL_STATES:
                    proposal_values[dimension].add(asserted_value)
        candidates_with_post += has_post
        candidates_with_reply += has_reply
        both += has_post and has_reply
        post_only += has_post and not has_reply
        reply_only += has_reply and not has_post
        neither += not has_post and not has_reply

        lab_state = candidate["target_lab_affiliation_state"]
        pretraining_state = candidate["pretraining_experience_state"]
        state_matrix[lab_state][pretraining_state] += 1
        proposal_resolution = {
            dimension: _resolve_proposal(proposal_values[dimension]) for dimension in DIMENSIONS
        }
        proposal_matrix[proposal_resolution["target_lab_affiliation_state"]][
            proposal_resolution["pretraining_experience_state"]
        ] += 1
        for dimension, resolution in proposal_resolution.items():
            if resolution == "none":
                alignment = "none"
            elif resolution == "conflict":
                alignment = "conflict"
            elif resolution == candidate[dimension]:
                alignment = "matches"
            else:
                alignment = "differs"
            proposal_alignment[dimension][alignment] += 1

        has_stable_id = candidate["platform_user_id"] is not None
        has_bio = candidate["bio_excerpt"] is not None
        stable_id_count += has_stable_id
        bio_count += has_bio
        both_profile_fields += has_stable_id and has_bio

    coverage_rows = receipt["reconciliation"]["candidate_surface_coverage"]
    attempted_post = sum(row["authored_post"]["attempted"] for row in coverage_rows)
    attempted_reply = sum(row["authored_reply"]["attempted"] for row in coverage_rows)
    attempted_both = sum(
        row["authored_post"]["attempted"] and row["authored_reply"]["attempted"] for row in coverage_rows
    )
    attempted_neither = sum(
        not row["authored_post"]["attempted"] and not row["authored_reply"]["attempted"]
        for row in coverage_rows
    )

    model_counts = result["counts"]
    local = result["local_reconciliation"]
    ledger = receipt["reconciliation"]
    discrepancies = {
        "candidate_count": {
            "model_result_count": model_counts["candidates_retained"],
            "model_local_validated": local["candidate_records_validated"],
            "ledger": ledger["candidate_count"],
            "result_delta_from_ledger": model_counts["candidates_retained"] - ledger["candidate_count"],
            "local_delta_from_ledger": local["candidate_records_validated"] - ledger["candidate_count"],
        },
        "evidence_count": _delta(local["evidence_items_validated"], ledger["evidence_count"]),
        "post_url_count": _delta(local["post_urls_structurally_validated"], ledger["post_url_count"]),
        "completed_native_x_calls": {
            "model_provenance_reported": result["native_x_tool_provenance"]["tool_calls_reported"],
            "model_local_reported": local["tool_calls_completed"],
            "ledger": completed_calls,
            "provenance_delta_from_ledger": (
                result["native_x_tool_provenance"]["tool_calls_reported"] - completed_calls
            ),
            "local_delta_from_ledger": local["tool_calls_completed"] - completed_calls,
        },
        "tool_counts": {
            tool: _delta(local["tool_counts"].get(tool, 0), proof["tool_counts"].get(tool, 0))
            for tool in NATIVE_X_TOOLS
        },
    }
    numeric_deltas = (
        discrepancies["candidate_count"]["result_delta_from_ledger"],
        discrepancies["candidate_count"]["local_delta_from_ledger"],
        discrepancies["evidence_count"]["delta_from_ledger"],
        discrepancies["post_url_count"]["delta_from_ledger"],
        discrepancies["completed_native_x_calls"]["provenance_delta_from_ledger"],
        discrepancies["completed_native_x_calls"]["local_delta_from_ledger"],
        *(row["delta_from_ledger"] for row in discrepancies["tool_counts"].values()),
    )
    discrepancies["any_discrepancy"] = any(numeric_deltas)

    return {
        "schema_version": KPI_SCHEMA_VERSION,
        "validation_basis": validation_basis,
        "execution": {
            "receipt_status": receipt["status"],
            "result_status": result["status"],
            "elapsed_ms": elapsed_ms,
        },
        "throughput": {
            "candidate_count": candidate_count,
            "completed_native_x_calls": completed_calls,
            "unique_candidates_per_completed_native_x_call": _ratio(candidate_count, completed_calls),
            "wall_time_ms_per_candidate": (
                round(elapsed_ms / candidate_count, 6)
                if isinstance(elapsed_ms, int) and candidate_count
                else None
            ),
        },
        "evidence": {
            "total_count": sum(evidence_counts_by_kind.values()),
            "counts_by_kind": {kind: evidence_counts_by_kind[kind] for kind in EVIDENCE_KINDS},
            "counts_by_thread_relation": {
                relation: evidence_counts_by_relation[relation] for relation in THREAD_RELATIONS
            },
            "surface_definition": {
                "post_like_thread_relations": sorted(POST_LIKE_RELATIONS),
                "reply_like_thread_relations": sorted(REPLY_LIKE_RELATIONS),
            },
            "candidates_with_post_evidence": candidates_with_post,
            "candidates_with_reply_evidence": candidates_with_reply,
            "candidate_surface_overlap": {
                "post_only": post_only,
                "reply_only": reply_only,
                "both": both,
                "neither": neither,
            },
        },
        "authored_surface_attempt_coverage": {
            "authored_post": _coverage(attempted_post, candidate_count),
            "authored_reply": _coverage(attempted_reply, candidate_count),
            "both_attempted": _coverage(attempted_both, candidate_count),
            "neither_attempted": _coverage(attempted_neither, candidate_count),
        },
        "candidate_state_matrix": {
            "row_dimension": "target_lab_affiliation_state",
            "column_dimension": "pretraining_experience_state",
            "counts": state_matrix,
        },
        "model_evidence_proposal_matrix": {
            "row_dimension": "target_lab_affiliation_state",
            "column_dimension": "pretraining_experience_state",
            "counts": proposal_matrix,
            "model_state_alignment_by_dimension": proposal_alignment,
        },
        "profile_coverage": {
            "stable_platform_user_id": _coverage(stable_id_count, candidate_count),
            "bio_excerpt": _coverage(bio_count, candidate_count),
            "both_stable_id_and_bio": _coverage(both_profile_fields, candidate_count),
        },
        "model_reported_vs_ledger_discrepancies": discrepancies,
    }


def analyze_sanitized_pair(
    result: Mapping[str, Any],
    receipt: Mapping[str, Any],
    *,
    sanitized_raw: bytes,
) -> dict[str, Any]:
    """Summarize a pair without candidate fields; this does not claim full bundle replay.

    Pair mode rechecks the closed result/receipt contracts, the receipt-bound sanitized
    hash, counts, transcript-derived call totals, and candidate surface population. Use
    ``analyze_operator_bundle`` when approval, command, and raw-session replay are needed.
    """

    _validate_pair_bindings(result, receipt, sanitized_raw)
    return _summarize_validated_pair(
        result,
        receipt,
        validation_basis="receipt_result_contracts_and_hash_reconciled",
    )


def analyze_sanitized_pair_paths(result_path: Path, receipt_path: Path) -> dict[str, Any]:
    result, result_raw = _read_owner_only_json(result_path)
    receipt, _ = _read_owner_only_json(receipt_path)
    if not isinstance(result, Mapping) or not isinstance(receipt, Mapping):
        raise AdaptiveWaveKPIError("pair_object_invalid")
    return analyze_sanitized_pair(result, receipt, sanitized_raw=result_raw)


def analyze_operator_bundle(
    run_root: Path,
    *,
    approval_root: Path = runner.DEFAULT_APPROVAL_ROOT,
) -> dict[str, Any]:
    """Replay the canonical validator, then emit a de-identified aggregate summary."""

    bundle_errors = runner.validate_operator_bundle(run_root, approval_root=approval_root)
    if bundle_errors:
        raise AdaptiveWaveKPIError("operator_bundle_replay_invalid")
    result, result_raw = _read_owner_only_json(run_root / "sanitized.json")
    receipt, _ = _read_owner_only_json(run_root / "operator-receipt.json")
    if not isinstance(result, Mapping) or not isinstance(receipt, Mapping):
        raise AdaptiveWaveKPIError("bundle_object_invalid")
    _validate_pair_bindings(result, receipt, result_raw)
    return _summarize_validated_pair(result, receipt, validation_basis="operator_bundle_replayed")


__all__ = [
    "AdaptiveWaveKPIError",
    "KPI_SCHEMA_VERSION",
    "analyze_operator_bundle",
    "analyze_sanitized_pair",
    "analyze_sanitized_pair_paths",
]
