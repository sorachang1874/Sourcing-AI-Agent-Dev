"""Fixture-first contract helpers for compact Grok/X discovery.

The compact lane retains only lead identities, coarse independent temporal
proposals, and canonical X source references.  Query text, tool-call counts,
and model-authored aggregate counts are deliberately outside this result
contract.  Aggregate metrics are recomputed here by the operator.
"""

from __future__ import annotations

import copy
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

RESULT_KEYS = frozenset(
    {"status", "strategy_id", "leads", "coverage_cells", "uncovered_cells", "limitations"}
)
LEAD_KEYS = frozenset(
    {
        "handle",
        "profile_url",
        "platform_user_id",
        "target_lab_affiliation_state",
        "pretraining_experience_state",
        "source_status",
        "source_refs",
        "reason_codes",
    }
)
SOURCE_REF_KEYS = frozenset({"surface", "url", "subject_handle", "author_handle", "support_dimensions"})

STATUSES = frozenset({"X_DISCOVERY_OK", "X_DISCOVERY_PARTIAL", "X_DISCOVERY_BLOCKED"})
TEMPORAL_STATES = ("current", "historical", "ambiguous")
SOURCE_STATUS = "model_mediated_unverified"
SOURCE_SURFACES = (
    "profile",
    "bio",
    "self_post",
    "reply",
    "quote",
    "mention",
    "official_post",
    "thread",
)
PROFILE_SURFACES = frozenset({"profile", "bio"})
STATUS_SURFACES = frozenset(set(SOURCE_SURFACES) - PROFILE_SURFACES)
SUPPORT_DIMENSIONS = ("lab_affiliation", "pretraining_relevance")
COVERAGE_CELLS = (
    "official_account_profiles",
    "official_account_posts",
    "official_account_mentions",
    "candidate_profiles",
    "candidate_authored_posts",
    "candidate_replies",
    "candidate_quotes_and_threads",
    "research_artifact_author_graph",
)
LIMITATION_CODES = (
    "native_x_search_incomplete",
    "profile_hydration_incomplete",
    "thread_hydration_incomplete",
    "result_truncated",
    "execution_deadline_reached",
    "transport_failure",
    "model_output_repaired",
)
MODEL_OWNED_LIMITATION_CODES = (
    "native_x_search_incomplete",
    "profile_hydration_incomplete",
    "thread_hydration_incomplete",
)
OPERATOR_OWNED_LIMITATION_CODES = (
    "result_truncated",
    "execution_deadline_reached",
    "transport_failure",
    "model_output_repaired",
)
REASON_CODES = tuple(
    [f"lab_affiliation_{state}_signal" for state in TEMPORAL_STATES]
    + [f"pretraining_relevance_{state}_signal" for state in TEMPORAL_STATES]
)

_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{0,19}")
_PROFILE_URL_RE = re.compile(r"https://x\.com/(?P<handle>[A-Za-z0-9_]{1,15})")
_STATUS_URL_RE = re.compile(
    r"https://x\.com/(?P<author>[A-Za-z0-9_]{1,15})/status/(?P<post_id>[1-9][0-9]{0,19})"
)
_STRATEGY_ID_RE = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*")
_RESERVED_OR_PLACEHOLDER_HANDLES = frozenset(
    {
        "account",
        "compose",
        "example",
        "example_user",
        "explore",
        "home",
        "i",
        "login",
        "messages",
        "notifications",
        "placeholder",
        "privacy",
        "search",
        "settings",
        "signup",
        "test",
        "test_user",
        "tos",
        "unknown",
        "unknown_user",
        "user",
    }
)


class CompactDiscoveryContractError(ValueError):
    """Raised when a compact discovery payload violates the runtime contract."""


@dataclass(frozen=True)
class CompactDiscoveryMergeSummary:
    """Candidate-text-free operator metrics for a compact-result union."""

    input_result_count: int
    input_lead_count: int
    unique_lead_count: int
    overlapping_handle_count: int
    platform_user_id_conflict_count: int
    lab_affiliation_state_conflict_count: int
    pretraining_experience_state_conflict_count: int
    input_source_ref_count: int
    merged_source_ref_count: int


@dataclass(frozen=True)
class MergedCompactDiscovery:
    """A validated compact result plus non-serialized merge diagnostics."""

    result: dict[str, Any]
    summary: CompactDiscoveryMergeSummary


@dataclass(frozen=True)
class CompactDiscoveryOperatorProjection:
    """A receipt-owned execution projection plus candidate-free audit facts."""

    result: dict[str, Any]
    removed_model_operator_limitations: tuple[str, ...]
    added_operator_limitations: tuple[str, ...]


def _valid_handle(value: Any) -> bool:
    return (
        isinstance(value, str)
        and _HANDLE_RE.fullmatch(value) is not None
        and value.casefold() not in _RESERVED_OR_PLACEHOLDER_HANDLES
    )


def _valid_closed_array(value: Any, allowed: Sequence[str]) -> bool:
    return (
        isinstance(value, list)
        and all(isinstance(item, str) and item in allowed for item in value)
        and len(value) == len(set(value))
    )


def canonical_profile_url(handle: str) -> str:
    """Render a canonical profile URL using the payload handle's display case."""

    if not _valid_handle(handle):
        raise CompactDiscoveryContractError("profile_handle_invalid")
    return f"https://x.com/{handle}"


def _expected_reason_codes(lead: Mapping[str, Any]) -> frozenset[str]:
    return frozenset(
        {
            f"lab_affiliation_{lead.get('target_lab_affiliation_state')}_signal",
            f"pretraining_relevance_{lead.get('pretraining_experience_state')}_signal",
        }
    )


def _validate_source_ref(
    source_ref: Any,
    *,
    lead_handle: str,
    profile_url: str,
) -> tuple[list[str], set[str], tuple[str, str] | None]:
    errors: list[str] = []
    if not isinstance(source_ref, dict) or set(source_ref) != SOURCE_REF_KEYS:
        return ["source_ref_shape_invalid"], set(), None

    surface = source_ref.get("surface")
    url = source_ref.get("url")
    subject = source_ref.get("subject_handle")
    author = source_ref.get("author_handle")
    dimensions = source_ref.get("support_dimensions")
    if surface not in SOURCE_SURFACES:
        errors.append("source_ref_surface_invalid")
    if not _valid_handle(subject) or subject.casefold() != lead_handle.casefold():
        errors.append("source_ref_subject_binding_invalid")
    if not _valid_handle(author):
        errors.append("source_ref_author_invalid")
    author_key = author.casefold() if isinstance(author, str) else ""
    if not _valid_closed_array(dimensions, SUPPORT_DIMENSIONS) or not dimensions:
        errors.append("source_ref_support_dimensions_invalid")
        supported: set[str] = set()
    else:
        supported = set(dimensions)

    if surface in PROFILE_SURFACES:
        match = _PROFILE_URL_RE.fullmatch(url) if isinstance(url, str) else None
        if (
            match is None
            or match.group("handle").casefold() != lead_handle.casefold()
            or author_key != lead_handle.casefold()
            or not isinstance(url, str)
            or url.casefold() != profile_url.casefold()
        ):
            errors.append("source_ref_profile_binding_invalid")
    elif surface in STATUS_SURFACES:
        match = _STATUS_URL_RE.fullmatch(url) if isinstance(url, str) else None
        if match is None or match.group("author").casefold() != author_key:
            errors.append("source_ref_status_binding_invalid")
        if surface == "self_post" and author_key != lead_handle.casefold():
            errors.append("source_ref_self_post_binding_invalid")
    else:
        errors.append("source_ref_url_invalid")

    signature = (surface, url.casefold()) if isinstance(surface, str) and isinstance(url, str) else None
    return errors, supported, signature


def validate_compact_discovery_result(result: Any) -> list[str]:
    """Return stable contract errors for a compact discovery result.

    This validator intentionally performs cross-field checks that JSON Schema
    cannot express: case-insensitive lead uniqueness, canonical URL-to-handle
    binding, source-subject binding, dimension coverage, closed coverage-cell
    partitioning, and status coherence.
    """

    if not isinstance(result, dict) or set(result) != RESULT_KEYS:
        return ["result_shape_invalid"]

    errors: list[str] = []
    status = result.get("status")
    if status not in STATUSES:
        errors.append("status_invalid")
    strategy_id = result.get("strategy_id")
    if (
        not isinstance(strategy_id, str)
        or len(strategy_id) > 128
        or _STRATEGY_ID_RE.fullmatch(strategy_id) is None
    ):
        errors.append("strategy_id_invalid")

    coverage = result.get("coverage_cells")
    uncovered = result.get("uncovered_cells")
    limitations = result.get("limitations")
    coverage_valid = _valid_closed_array(coverage, COVERAGE_CELLS)
    uncovered_valid = _valid_closed_array(uncovered, COVERAGE_CELLS)
    if not coverage_valid:
        errors.append("coverage_cells_invalid")
    if not uncovered_valid:
        errors.append("uncovered_cells_invalid")
    if not _valid_closed_array(limitations, LIMITATION_CODES):
        errors.append("limitations_invalid")
        limitations = []
    if coverage_valid and uncovered_valid:
        covered_set = set(coverage)
        uncovered_set = set(uncovered)
        if covered_set & uncovered_set:
            errors.append("coverage_cells_overlap")
        if covered_set | uncovered_set != set(COVERAGE_CELLS):
            errors.append("coverage_partition_invalid")
        if status == "X_DISCOVERY_OK" and (uncovered_set or limitations):
            errors.append("status_coverage_invalid")
        if status == "X_DISCOVERY_PARTIAL" and not uncovered_set and not limitations:
            errors.append("status_coverage_invalid")

    leads = result.get("leads")
    if not isinstance(leads, list):
        errors.append("leads_invalid")
        leads = []
    if status == "X_DISCOVERY_BLOCKED" and (leads or not limitations):
        errors.append("blocked_status_invalid")

    seen_handles: set[str] = set()
    for lead_index, lead in enumerate(leads):
        prefix = f"lead:{lead_index}"
        if not isinstance(lead, dict) or set(lead) != LEAD_KEYS:
            errors.append(f"{prefix}:shape_invalid")
            continue

        handle = lead.get("handle")
        if not _valid_handle(handle):
            errors.append(f"{prefix}:handle_invalid")
            continue
        handle_key = handle.casefold()
        if handle_key in seen_handles:
            errors.append(f"{prefix}:handle_casefold_duplicate")
        seen_handles.add(handle_key)

        profile_url = lead.get("profile_url")
        profile_match = _PROFILE_URL_RE.fullmatch(profile_url) if isinstance(profile_url, str) else None
        if profile_match is None or profile_match.group("handle").casefold() != handle.casefold():
            errors.append(f"{prefix}:profile_url_binding_invalid")
        platform_user_id = lead.get("platform_user_id")
        if platform_user_id is not None and (
            not isinstance(platform_user_id, str) or _PLATFORM_USER_ID_RE.fullmatch(platform_user_id) is None
        ):
            errors.append(f"{prefix}:platform_user_id_invalid")

        lab_state = lead.get("target_lab_affiliation_state")
        pretraining_state = lead.get("pretraining_experience_state")
        if lab_state not in TEMPORAL_STATES:
            errors.append(f"{prefix}:lab_state_invalid")
        if pretraining_state not in TEMPORAL_STATES:
            errors.append(f"{prefix}:pretraining_state_invalid")
        if lead.get("source_status") != SOURCE_STATUS:
            errors.append(f"{prefix}:source_status_invalid")

        reason_codes = lead.get("reason_codes")
        if (
            not _valid_closed_array(reason_codes, REASON_CODES)
            or frozenset(reason_codes) != _expected_reason_codes(lead)
        ):
            errors.append(f"{prefix}:reason_codes_invalid")

        source_refs = lead.get("source_refs")
        if not isinstance(source_refs, list) or not source_refs:
            errors.append(f"{prefix}:source_refs_invalid")
            continue
        supported_dimensions: set[str] = set()
        source_signatures: set[tuple[str, str]] = set()
        for source_index, source_ref in enumerate(source_refs):
            source_errors, dimensions, signature = _validate_source_ref(
                source_ref,
                lead_handle=handle,
                profile_url=profile_url,
            )
            errors.extend(f"{prefix}:source_ref:{source_index}:{error}" for error in source_errors)
            supported_dimensions.update(dimensions)
            if signature is not None:
                if signature in source_signatures:
                    errors.append(f"{prefix}:source_ref:{source_index}:duplicate")
                source_signatures.add(signature)
        required_dimensions: set[str] = set()
        if lab_state != "ambiguous":
            required_dimensions.add("lab_affiliation")
        if pretraining_state != "ambiguous":
            required_dimensions.add("pretraining_relevance")
        if not supported_dimensions or not required_dimensions.issubset(supported_dimensions):
            errors.append(f"{prefix}:source_dimension_coverage_invalid")

    return errors


def assert_compact_discovery_result(result: Any) -> None:
    """Raise :class:`CompactDiscoveryContractError` for an invalid result."""

    errors = validate_compact_discovery_result(result)
    if errors:
        raise CompactDiscoveryContractError(";".join(errors))


def project_compact_execution_limitations(
    result: Mapping[str, Any],
    *,
    result_truncated: bool,
    execution_deadline_reached: bool,
    transport_failure: bool,
    model_output_repaired: bool,
) -> CompactDiscoveryOperatorProjection:
    """Replace model-authored technical claims with operator-observed facts.

    Grok may assess whether its X, profile, or thread exploration was
    incomplete.  It cannot observe the supervising process receipt reliably,
    so truncation, deadline, transport, and repair limitations are owned by the
    operator.  The projection is non-mutating and recomputes status from the
    resulting coverage and limitation state.
    """

    assert_compact_discovery_result(result)
    operator_facts = {
        "result_truncated": result_truncated,
        "execution_deadline_reached": execution_deadline_reached,
        "transport_failure": transport_failure,
        "model_output_repaired": model_output_repaired,
    }
    invalid_fact = next(
        (code for code, observed in operator_facts.items() if type(observed) is not bool),
        None,
    )
    if invalid_fact is not None:
        raise CompactDiscoveryContractError(f"operator_fact_invalid:{invalid_fact}")

    model_limitations = set(result["limitations"])
    removed = tuple(
        code for code in OPERATOR_OWNED_LIMITATION_CODES if code in model_limitations
    )
    added = tuple(
        code for code in OPERATOR_OWNED_LIMITATION_CODES if operator_facts[code]
    )
    retained_model_limitations = {
        code for code in model_limitations if code in MODEL_OWNED_LIMITATION_CODES
    }
    projected_limitations = [
        code
        for code in LIMITATION_CODES
        if code in retained_model_limitations or code in added
    ]

    projected = copy.deepcopy(result)
    projected["limitations"] = projected_limitations
    hard_execution_failure = execution_deadline_reached or transport_failure
    if not projected["leads"] and hard_execution_failure:
        projected["status"] = "X_DISCOVERY_BLOCKED"
    elif projected["uncovered_cells"] or projected_limitations:
        projected["status"] = "X_DISCOVERY_PARTIAL"
    else:
        projected["status"] = "X_DISCOVERY_OK"

    assert_compact_discovery_result(projected)
    return CompactDiscoveryOperatorProjection(
        result=projected,
        removed_model_operator_limitations=removed,
        added_operator_limitations=added,
    )


def summarize_compact_discovery(result: Mapping[str, Any]) -> dict[str, Any]:
    """Mechanically summarize a validated result without returning lead data."""

    assert_compact_discovery_result(result)
    surface_counts: Counter[str] = Counter()
    dimension_counts: Counter[str] = Counter()
    state_matrix = {
        lab_state: {pretraining_state: 0 for pretraining_state in TEMPORAL_STATES}
        for lab_state in TEMPORAL_STATES
    }
    source_ref_count = 0
    for lead in result["leads"]:
        state_matrix[lead["target_lab_affiliation_state"]][lead["pretraining_experience_state"]] += 1
        for source_ref in lead["source_refs"]:
            source_ref_count += 1
            surface_counts[source_ref["surface"]] += 1
            dimension_counts.update(source_ref["support_dimensions"])
    return {
        "unique_lead_count": len(result["leads"]),
        "source_refs": {
            "total": source_ref_count,
            "by_surface": {surface: surface_counts[surface] for surface in SOURCE_SURFACES},
            "by_support_dimension": {
                dimension: dimension_counts[dimension] for dimension in SUPPORT_DIMENSIONS
            },
        },
        "state_matrix": state_matrix,
    }


def compare_lead_sets(left_handles: Sequence[str], right_handles: Sequence[str]) -> dict[str, int | float]:
    """Return case-insensitive aggregate overlap metrics for two handle sets."""

    if any(not _valid_handle(handle) for handle in (*left_handles, *right_handles)):
        raise CompactDiscoveryContractError("comparison_handle_invalid")
    left = {handle.casefold() for handle in left_handles}
    right = {handle.casefold() for handle in right_handles}
    union = left | right
    intersection = left & right
    return {
        "left_unique_leads": len(left),
        "right_unique_leads": len(right),
        "union": len(union),
        "intersection": len(intersection),
        "left_only": len(left - right),
        "right_only": len(right - left),
        "jaccard": round(len(intersection) / len(union), 6) if union else 1.0,
    }


def compare_compact_discovery_results(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> dict[str, int | float]:
    """Validate two results and compare their operator-derived lead sets."""

    assert_compact_discovery_result(left)
    assert_compact_discovery_result(right)
    return compare_lead_sets(
        [lead["handle"] for lead in left["leads"]],
        [lead["handle"] for lead in right["leads"]],
    )


def _merge_temporal_states(states: Sequence[str]) -> str:
    concrete_states = {state for state in states if state != "ambiguous"}
    if len(concrete_states) == 1:
        return next(iter(concrete_states))
    return "ambiguous"


def _has_concrete_temporal_conflict(states: Sequence[str]) -> bool:
    return {"current", "historical"}.issubset(states)


def _canonical_support_dimensions(dimensions: Sequence[str]) -> list[str]:
    return [dimension for dimension in SUPPORT_DIMENSIONS if dimension in dimensions]


def _source_ref_key(source_ref: Mapping[str, Any]) -> tuple[str, str, str, str, tuple[str, ...]]:
    return (
        source_ref["surface"],
        source_ref["url"].casefold(),
        source_ref["subject_handle"].casefold(),
        source_ref["author_handle"].casefold(),
        tuple(source_ref["support_dimensions"]),
    )


def _source_ref_representation_key(source_ref: Mapping[str, Any]) -> tuple[str, ...]:
    return (
        source_ref["surface"],
        source_ref["url"],
        source_ref["subject_handle"],
        source_ref["author_handle"],
        *source_ref["support_dimensions"],
    )


def _merge_source_refs(
    source_refs: Sequence[Mapping[str, Any]],
    *,
    canonical_handle: str,
) -> list[dict[str, Any]]:
    """Return a deterministic union without dropping any evidence surface.

    The requested full identity key includes support dimensions.  The compact
    result validator additionally treats one surface/URL pair as one evidence
    record, so dimension variants for the same record are consolidated into a
    single reference with the closed dimension union.
    """

    deduplicated: dict[tuple[str, str, str, str, tuple[str, ...]], dict[str, Any]] = {}
    for source_ref in source_refs:
        normalized = {
            "surface": source_ref["surface"],
            "url": source_ref["url"],
            "subject_handle": canonical_handle,
            "author_handle": source_ref["author_handle"],
            "support_dimensions": _canonical_support_dimensions(
                source_ref["support_dimensions"]
            ),
        }
        key = _source_ref_key(normalized)
        existing = deduplicated.get(key)
        if existing is None or _source_ref_representation_key(
            normalized
        ) < _source_ref_representation_key(existing):
            deduplicated[key] = normalized

    by_contract_signature: dict[tuple[str, str], dict[str, Any]] = {}
    for source_ref in deduplicated.values():
        signature = (source_ref["surface"], source_ref["url"].casefold())
        existing = by_contract_signature.get(signature)
        if existing is None:
            by_contract_signature[signature] = source_ref
            continue

        dimensions = set(existing["support_dimensions"])
        dimensions.update(source_ref["support_dimensions"])
        representative = min(
            (existing, source_ref),
            key=_source_ref_representation_key,
        )
        by_contract_signature[signature] = {
            **representative,
            "support_dimensions": _canonical_support_dimensions(tuple(dimensions)),
        }

    return sorted(by_contract_signature.values(), key=_source_ref_key)


def merge_compact_discovery_results(
    results: Sequence[Mapping[str, Any]],
    *,
    strategy_id: str = "operator.compact-discovery-union-v1",
) -> MergedCompactDiscovery:
    """Validate and deterministically union any number of compact shards.

    Handle identity is case-insensitive.  Conflicting non-null platform user
    IDs are never selected arbitrarily: the merged field becomes ``None`` and
    the aggregate conflict count is exposed only in the operator summary.
    There are deliberately no lead or source-reference business caps.
    """

    if isinstance(results, (str, bytes)):
        raise CompactDiscoveryContractError("merge_results_invalid")
    inputs = tuple(results)
    if not inputs:
        raise CompactDiscoveryContractError("merge_results_empty")

    input_errors: list[str] = []
    for result_index, result in enumerate(inputs):
        input_errors.extend(
            f"merge_input:{result_index}:{error}"
            for error in validate_compact_discovery_result(result)
        )
    if input_errors:
        raise CompactDiscoveryContractError(";".join(input_errors))

    leads_by_handle: dict[str, list[Mapping[str, Any]]] = {}
    input_lead_count = 0
    input_source_ref_count = 0
    for result in inputs:
        for lead in result["leads"]:
            input_lead_count += 1
            input_source_ref_count += len(lead["source_refs"])
            leads_by_handle.setdefault(lead["handle"].casefold(), []).append(lead)

    merged_leads: list[dict[str, Any]] = []
    platform_user_id_conflict_count = 0
    lab_affiliation_state_conflict_count = 0
    pretraining_experience_state_conflict_count = 0
    for handle_key in sorted(leads_by_handle):
        lead_group = leads_by_handle[handle_key]
        canonical_handle = min(lead["handle"] for lead in lead_group)
        platform_user_ids = {
            lead["platform_user_id"]
            for lead in lead_group
            if lead["platform_user_id"] is not None
        }
        if len(platform_user_ids) > 1:
            platform_user_id = None
            platform_user_id_conflict_count += 1
        else:
            platform_user_id = next(iter(platform_user_ids), None)

        lab_states = [lead["target_lab_affiliation_state"] for lead in lead_group]
        pretraining_states = [lead["pretraining_experience_state"] for lead in lead_group]
        if _has_concrete_temporal_conflict(lab_states):
            lab_affiliation_state_conflict_count += 1
        if _has_concrete_temporal_conflict(pretraining_states):
            pretraining_experience_state_conflict_count += 1
        lab_state = _merge_temporal_states(lab_states)
        pretraining_state = _merge_temporal_states(pretraining_states)
        merged_source_refs = _merge_source_refs(
            [source_ref for lead in lead_group for source_ref in lead["source_refs"]],
            canonical_handle=canonical_handle,
        )
        merged_leads.append(
            {
                "handle": canonical_handle,
                "profile_url": canonical_profile_url(canonical_handle),
                "platform_user_id": platform_user_id,
                "target_lab_affiliation_state": lab_state,
                "pretraining_experience_state": pretraining_state,
                "source_status": SOURCE_STATUS,
                "source_refs": merged_source_refs,
                "reason_codes": [
                    f"lab_affiliation_{lab_state}_signal",
                    f"pretraining_relevance_{pretraining_state}_signal",
                ],
            }
        )

    covered_set = {
        coverage_cell for result in inputs for coverage_cell in result["coverage_cells"]
    }
    coverage_cells = [cell for cell in COVERAGE_CELLS if cell in covered_set]
    uncovered_cells = [cell for cell in COVERAGE_CELLS if cell not in covered_set]
    limitation_set = {
        limitation for result in inputs for limitation in result["limitations"]
    }
    limitations = [code for code in LIMITATION_CODES if code in limitation_set]
    if all(result["status"] == "X_DISCOVERY_BLOCKED" for result in inputs):
        status = "X_DISCOVERY_BLOCKED"
    elif uncovered_cells or limitations:
        status = "X_DISCOVERY_PARTIAL"
    else:
        status = "X_DISCOVERY_OK"

    merged_result = {
        "status": status,
        "strategy_id": strategy_id,
        "leads": merged_leads,
        "coverage_cells": coverage_cells,
        "uncovered_cells": uncovered_cells,
        "limitations": limitations,
    }
    assert_compact_discovery_result(merged_result)
    summary = CompactDiscoveryMergeSummary(
        input_result_count=len(inputs),
        input_lead_count=input_lead_count,
        unique_lead_count=len(merged_leads),
        overlapping_handle_count=sum(
            len(lead_group) > 1 for lead_group in leads_by_handle.values()
        ),
        platform_user_id_conflict_count=platform_user_id_conflict_count,
        lab_affiliation_state_conflict_count=lab_affiliation_state_conflict_count,
        pretraining_experience_state_conflict_count=pretraining_experience_state_conflict_count,
        input_source_ref_count=input_source_ref_count,
        merged_source_ref_count=sum(len(lead["source_refs"]) for lead in merged_leads),
    )
    return MergedCompactDiscovery(result=merged_result, summary=summary)
