"""Contract helpers for receipt-bound compact Grok/X discovery.

The model proposes compact account/evidence records.  The operator owns
campaign binding, execution facts, identity reconciliation, and per-shard
membership.  A raw model mapping cannot enter the union: it must first be
projected through a typed execution receipt.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

CONTRACT_VERSION = "x.grok.compact_discovery.result.v1"
RESULT_KEYS = frozenset(
    {
        "contract_version",
        "campaign_id",
        "target_descriptor_id",
        "target_descriptor_sha256",
        "prompt_policy_sha256",
        "result_kind",
        "shard_id",
        "input_shards",
        "status",
        "strategy_id",
        "leads",
        "coverage_cells",
        "uncovered_cells",
        "limitations",
    }
)
INPUT_SHARD_KEYS = frozenset({"shard_id", "projected_result_sha256"})
LEAD_KEYS = frozenset(
    {
        "handle",
        "profile_url",
        "platform_user_id",
        "identity_status",
        "handle_history_proposals",
        "origin_shard_ids",
        "target_lab_affiliation_state",
        "pretraining_experience_state",
        "source_status",
        "source_refs",
        "reason_codes",
    }
)
HANDLE_HISTORY_KEYS = frozenset({"handle", "profile_url", "source_status", "origin_shard_ids"})
SOURCE_REF_KEYS = frozenset(
    {
        "surface",
        "url",
        "subject_handle",
        "author_handle",
        "support_dimensions",
        "origin_shard_ids",
    }
)

STATUSES = frozenset({"X_DISCOVERY_OK", "X_DISCOVERY_PARTIAL", "X_DISCOVERY_BLOCKED"})
RESULT_KINDS = frozenset({"shard", "union"})
IDENTITY_STATUSES = (
    "stable_platform_id",
    "provisional_handle",
    "quarantined_handle_reuse",
)
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
_IDENTIFIER_RE = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SESSION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
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
class CompactDiscoveryExecutionReceipt:
    """Operator-owned facts binding one terminal to one discovery session."""

    receipt_version: str
    campaign_id: str
    target_descriptor_id: str
    target_descriptor_sha256: str
    prompt_policy_sha256: str
    shard_id: str
    session_id: str
    transcript_sha256: str
    terminal_sha256: str
    terminal_selected_after_last_tool_completion: bool
    result_truncated: bool
    execution_deadline_reached: bool
    transport_failure: bool
    model_output_repaired: bool


@dataclass(frozen=True)
class CompactDiscoveryOperatorProjection:
    """Receipt-bound result that is eligible to enter the operator union."""

    raw_terminal: dict[str, Any]
    result: dict[str, Any]
    receipt: CompactDiscoveryExecutionReceipt
    receipt_sha256: str
    terminal_sha256: str
    projected_result_sha256: str
    removed_model_operator_limitations: tuple[str, ...]
    added_operator_limitations: tuple[str, ...]


@dataclass(frozen=True)
class CompactDiscoveryMergeSummary:
    """Candidate-text-free operator metrics for a compact-result union."""

    input_result_count: int
    input_lead_count: int
    unique_lead_count: int
    overlapping_handle_count: int
    platform_user_id_conflict_count: int
    renamed_stable_identity_count: int
    quarantined_handle_reuse_identity_count: int
    provisional_identity_count: int
    lab_affiliation_state_conflict_count: int
    pretraining_experience_state_conflict_count: int
    input_source_ref_count: int
    merged_source_ref_count: int


@dataclass(frozen=True)
class MergedCompactDiscovery:
    """A validated compact union plus non-serialized diagnostics."""

    result: dict[str, Any]
    summary: CompactDiscoveryMergeSummary


def canonical_json_sha256(value: Any) -> str:
    """Hash one JSON-compatible value with the repository canonical encoding."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _valid_identifier(value: Any) -> bool:
    return isinstance(value, str) and len(value) <= 128 and _IDENTIFIER_RE.fullmatch(value) is not None


def _valid_sha256(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None


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


def _valid_origin_ids(value: Any, allowed_ids: set[str]) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(_valid_identifier(item) and item in allowed_ids for item in value)
        and len(value) == len(set(value))
        and value == sorted(value)
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
    lead_handles: set[str],
    allowed_origin_ids: set[str],
) -> tuple[list[str], set[str], tuple[str, str] | None, set[str]]:
    errors: list[str] = []
    if not isinstance(source_ref, dict) or set(source_ref) != SOURCE_REF_KEYS:
        return ["source_ref_shape_invalid"], set(), None, set()

    surface = source_ref.get("surface")
    url = source_ref.get("url")
    subject = source_ref.get("subject_handle")
    author = source_ref.get("author_handle")
    dimensions = source_ref.get("support_dimensions")
    origins = source_ref.get("origin_shard_ids")
    if surface not in SOURCE_SURFACES:
        errors.append("source_ref_surface_invalid")
    if not _valid_handle(subject) or subject.casefold() not in lead_handles:
        errors.append("source_ref_subject_binding_invalid")
    if not _valid_handle(author):
        errors.append("source_ref_author_invalid")
    author_key = author.casefold() if isinstance(author, str) else ""
    if not _valid_closed_array(dimensions, SUPPORT_DIMENSIONS) or not dimensions:
        errors.append("source_ref_support_dimensions_invalid")
        supported: set[str] = set()
    else:
        supported = set(dimensions)
    if not _valid_origin_ids(origins, allowed_origin_ids):
        errors.append("source_ref_origin_binding_invalid")
        origin_set: set[str] = set()
    else:
        origin_set = set(origins)

    if surface in PROFILE_SURFACES:
        match = _PROFILE_URL_RE.fullmatch(url) if isinstance(url, str) else None
        if (
            match is None
            or match.group("handle").casefold() not in lead_handles
            or author_key != match.group("handle").casefold()
        ):
            errors.append("source_ref_profile_binding_invalid")
    elif surface in STATUS_SURFACES:
        match = _STATUS_URL_RE.fullmatch(url) if isinstance(url, str) else None
        if match is None or match.group("author").casefold() != author_key:
            errors.append("source_ref_status_binding_invalid")
        if surface == "self_post" and author_key != str(subject).casefold():
            errors.append("source_ref_self_post_binding_invalid")
    else:
        errors.append("source_ref_url_invalid")

    signature = (surface, url.casefold()) if isinstance(surface, str) and isinstance(url, str) else None
    return errors, supported, signature, origin_set


def _validate_compact_discovery_result(
    result: Any,
    *,
    enforce_status_coherence: bool,
) -> list[str]:
    if not isinstance(result, dict) or set(result) != RESULT_KEYS:
        return ["result_shape_invalid"]

    errors: list[str] = []
    if result.get("contract_version") != CONTRACT_VERSION:
        errors.append("contract_version_invalid")
    for field in ("campaign_id", "target_descriptor_id", "shard_id", "strategy_id"):
        if not _valid_identifier(result.get(field)):
            errors.append(f"{field}_invalid")
    for field in ("target_descriptor_sha256", "prompt_policy_sha256"):
        if not _valid_sha256(result.get(field)):
            errors.append(f"{field}_invalid")

    result_kind = result.get("result_kind")
    if result_kind not in RESULT_KINDS:
        errors.append("result_kind_invalid")
    input_shards = result.get("input_shards")
    input_shard_ids: set[str] = set()
    if not isinstance(input_shards, list):
        errors.append("input_shards_invalid")
        input_shards = []
    for index, binding in enumerate(input_shards):
        if not isinstance(binding, dict) or set(binding) != INPUT_SHARD_KEYS:
            errors.append(f"input_shard:{index}:shape_invalid")
            continue
        shard_id = binding.get("shard_id")
        if not _valid_identifier(shard_id):
            errors.append(f"input_shard:{index}:shard_id_invalid")
        elif shard_id in input_shard_ids:
            errors.append(f"input_shard:{index}:shard_id_duplicate")
        else:
            input_shard_ids.add(shard_id)
        if not _valid_sha256(binding.get("projected_result_sha256")):
            errors.append(f"input_shard:{index}:digest_invalid")
    if isinstance(input_shards, list) and input_shards != sorted(
        input_shards,
        key=lambda item: item.get("shard_id", "") if isinstance(item, dict) else "",
    ):
        errors.append("input_shards_order_invalid")
    shard_id_value = result.get("shard_id")
    if result_kind == "shard":
        if input_shards:
            errors.append("shard_input_shards_must_be_empty")
        allowed_origin_ids = {shard_id_value} if _valid_identifier(shard_id_value) else set()
    else:
        if not input_shards:
            errors.append("union_input_shards_empty")
        if shard_id_value in input_shard_ids:
            errors.append("union_id_collides_with_input_shard")
        allowed_origin_ids = input_shard_ids

    status = result.get("status")
    if status not in STATUSES:
        errors.append("status_invalid")
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
        if enforce_status_coherence:
            if status == "X_DISCOVERY_OK" and (uncovered_set or limitations):
                errors.append("status_coverage_invalid")
            if status == "X_DISCOVERY_PARTIAL" and not uncovered_set and not limitations:
                errors.append("status_coverage_invalid")

    leads = result.get("leads")
    if not isinstance(leads, list):
        errors.append("leads_invalid")
        leads = []
    if enforce_status_coherence and status == "X_DISCOVERY_BLOCKED" and (leads or not limitations):
        errors.append("blocked_status_invalid")

    stable_ids: dict[str, int] = {}
    provisional_handles: dict[str, int] = {}
    handle_owners: dict[str, list[tuple[int, str | None, str]]] = {}
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
        profile_url = lead.get("profile_url")
        profile_match = _PROFILE_URL_RE.fullmatch(profile_url) if isinstance(profile_url, str) else None
        if profile_match is None or profile_match.group("handle").casefold() != handle_key:
            errors.append(f"{prefix}:profile_url_binding_invalid")

        platform_user_id = lead.get("platform_user_id")
        if platform_user_id is not None and (
            not isinstance(platform_user_id, str) or _PLATFORM_USER_ID_RE.fullmatch(platform_user_id) is None
        ):
            errors.append(f"{prefix}:platform_user_id_invalid")
        identity_status = lead.get("identity_status")
        if identity_status not in IDENTITY_STATUSES:
            errors.append(f"{prefix}:identity_status_invalid")
        if platform_user_id is None and identity_status != "provisional_handle":
            errors.append(f"{prefix}:provisional_identity_status_invalid")
        if platform_user_id is not None and identity_status == "provisional_handle":
            errors.append(f"{prefix}:stable_identity_status_invalid")
        if isinstance(platform_user_id, str):
            if platform_user_id in stable_ids:
                errors.append(f"{prefix}:platform_user_id_duplicate")
            else:
                stable_ids[platform_user_id] = lead_index
        elif handle_key in provisional_handles:
            errors.append(f"{prefix}:provisional_handle_duplicate")
        else:
            provisional_handles[handle_key] = lead_index

        history = lead.get("handle_history_proposals")
        history_handles: set[str] = set()
        history_origins: set[str] = set()
        if not isinstance(history, list) or not history:
            errors.append(f"{prefix}:handle_history_invalid")
            history = []
        for history_index, proposal in enumerate(history):
            history_prefix = f"{prefix}:handle_history:{history_index}"
            if not isinstance(proposal, dict) or set(proposal) != HANDLE_HISTORY_KEYS:
                errors.append(f"{history_prefix}:shape_invalid")
                continue
            proposal_handle = proposal.get("handle")
            proposal_url = proposal.get("profile_url")
            match = _PROFILE_URL_RE.fullmatch(proposal_url) if isinstance(proposal_url, str) else None
            if not _valid_handle(proposal_handle):
                errors.append(f"{history_prefix}:handle_invalid")
                continue
            proposal_key = proposal_handle.casefold()
            if proposal_key in history_handles:
                errors.append(f"{history_prefix}:handle_duplicate")
            history_handles.add(proposal_key)
            if match is None or match.group("handle").casefold() != proposal_key:
                errors.append(f"{history_prefix}:profile_url_binding_invalid")
            if proposal.get("source_status") != SOURCE_STATUS:
                errors.append(f"{history_prefix}:source_status_invalid")
            origins = proposal.get("origin_shard_ids")
            if not _valid_origin_ids(origins, allowed_origin_ids):
                errors.append(f"{history_prefix}:origin_binding_invalid")
            else:
                history_origins.update(origins)
        if handle_key not in history_handles:
            errors.append(f"{prefix}:canonical_handle_missing_from_history")

        lead_origins = lead.get("origin_shard_ids")
        if not _valid_origin_ids(lead_origins, allowed_origin_ids):
            errors.append(f"{prefix}:origin_binding_invalid")
            lead_origin_set: set[str] = set()
        else:
            lead_origin_set = set(lead_origins)

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
            source_refs = []
        supported_dimensions: set[str] = set()
        source_signatures: set[tuple[str, str]] = set()
        source_origins: set[str] = set()
        for source_index, source_ref in enumerate(source_refs):
            source_errors, dimensions, signature, origins = _validate_source_ref(
                source_ref,
                lead_handles=history_handles,
                allowed_origin_ids=allowed_origin_ids,
            )
            errors.extend(f"{prefix}:source_ref:{source_index}:{error}" for error in source_errors)
            supported_dimensions.update(dimensions)
            source_origins.update(origins)
            if signature is not None:
                if signature in source_signatures:
                    errors.append(f"{prefix}:source_ref:{source_index}:duplicate")
                source_signatures.add(signature)
        if not set(SUPPORT_DIMENSIONS).issubset(supported_dimensions):
            errors.append(f"{prefix}:source_dimension_coverage_invalid")
        if lead_origin_set and lead_origin_set != source_origins | history_origins:
            errors.append(f"{prefix}:origin_membership_mismatch")

        for proposal_handle in history_handles or {handle_key}:
            handle_owners.setdefault(proposal_handle, []).append(
                (lead_index, platform_user_id if isinstance(platform_user_id, str) else None, str(identity_status))
            )

    for owners in handle_owners.values():
        stable_owner_ids = {owner_id for _, owner_id, _ in owners if owner_id is not None}
        if len(stable_owner_ids) > 1 and any(
            owner_id is not None and status != "quarantined_handle_reuse"
            for _, owner_id, status in owners
        ):
            errors.append("handle_reuse_not_quarantined")

    return list(dict.fromkeys(errors))


def validate_compact_discovery_result(result: Any) -> list[str]:
    """Return full runtime errors, including post-projection status coherence."""

    return _validate_compact_discovery_result(result, enforce_status_coherence=True)


def assert_compact_discovery_result(result: Any) -> None:
    errors = validate_compact_discovery_result(result)
    if errors:
        raise CompactDiscoveryContractError(";".join(errors))


def _validate_compact_receipt(
    result: Mapping[str, Any],
    receipt: CompactDiscoveryExecutionReceipt,
) -> list[str]:
    if not isinstance(receipt, CompactDiscoveryExecutionReceipt):
        return ["operator_receipt_type_invalid"]
    errors: list[str] = []
    if receipt.receipt_version != "x.grok.compact_discovery.execution_receipt.v1":
        errors.append("operator_receipt_version_invalid")
    for field in ("campaign_id", "target_descriptor_id", "shard_id"):
        value = getattr(receipt, field)
        if not _valid_identifier(value):
            errors.append(f"operator_receipt_{field}_invalid")
        if value != result.get(field):
            errors.append(f"operator_receipt_{field}_mismatch")
    for field in ("target_descriptor_sha256", "prompt_policy_sha256"):
        value = getattr(receipt, field)
        if not _valid_sha256(value):
            errors.append(f"operator_receipt_{field}_invalid")
        if value != result.get(field):
            errors.append(f"operator_receipt_{field}_mismatch")
    if not isinstance(receipt.session_id, str) or _SESSION_ID_RE.fullmatch(receipt.session_id) is None:
        errors.append("operator_receipt_session_id_invalid")
    if not _valid_sha256(receipt.transcript_sha256):
        errors.append("operator_receipt_transcript_sha256_invalid")
    if not _valid_sha256(receipt.terminal_sha256):
        errors.append("operator_receipt_terminal_sha256_invalid")
    elif receipt.terminal_sha256 != canonical_json_sha256(result):
        errors.append("operator_receipt_terminal_sha256_mismatch")
    if receipt.terminal_selected_after_last_tool_completion is not True:
        errors.append("operator_receipt_terminal_order_unproven")
    for field in (
        "result_truncated",
        "execution_deadline_reached",
        "transport_failure",
        "model_output_repaired",
    ):
        if type(getattr(receipt, field)) is not bool:
            errors.append(f"operator_receipt_{field}_invalid")
    return errors


def project_compact_execution_limitations(
    result: Mapping[str, Any],
    *,
    receipt: CompactDiscoveryExecutionReceipt,
) -> CompactDiscoveryOperatorProjection:
    """Normalize operator-owned claims using a terminal-bound receipt.

    Pre-projection validation intentionally ignores model-authored status
    coherence.  The model's false timeout/truncation claims are removed first;
    the projected object then passes the complete validator.
    """

    structural_errors = _validate_compact_discovery_result(
        result,
        enforce_status_coherence=False,
    )
    if structural_errors:
        raise CompactDiscoveryContractError(";".join(structural_errors))
    receipt_errors = _validate_compact_receipt(result, receipt)
    if receipt_errors:
        raise CompactDiscoveryContractError(";".join(receipt_errors))
    if result.get("result_kind") != "shard":
        raise CompactDiscoveryContractError("projection_requires_shard_result")

    operator_facts = {
        "result_truncated": receipt.result_truncated,
        "execution_deadline_reached": receipt.execution_deadline_reached,
        "transport_failure": receipt.transport_failure,
        "model_output_repaired": receipt.model_output_repaired,
    }
    model_limitations = set(result["limitations"])
    removed = tuple(code for code in OPERATOR_OWNED_LIMITATION_CODES if code in model_limitations)
    added = tuple(code for code in OPERATOR_OWNED_LIMITATION_CODES if operator_facts[code])
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
    hard_execution_failure = receipt.execution_deadline_reached or receipt.transport_failure
    if not projected["leads"] and hard_execution_failure:
        projected["status"] = "X_DISCOVERY_BLOCKED"
    elif projected["uncovered_cells"] or projected_limitations:
        projected["status"] = "X_DISCOVERY_PARTIAL"
    else:
        projected["status"] = "X_DISCOVERY_OK"
    assert_compact_discovery_result(projected)
    return CompactDiscoveryOperatorProjection(
        raw_terminal=copy.deepcopy(dict(result)),
        result=projected,
        receipt=receipt,
        receipt_sha256=canonical_json_sha256(asdict(receipt)),
        terminal_sha256=receipt.terminal_sha256,
        projected_result_sha256=canonical_json_sha256(projected),
        removed_model_operator_limitations=removed,
        added_operator_limitations=added,
    )


def _assert_projection(projection: Any) -> CompactDiscoveryOperatorProjection:
    if not isinstance(projection, CompactDiscoveryOperatorProjection):
        raise CompactDiscoveryContractError("merge_projection_required")
    assert_compact_discovery_result(projection.result)
    try:
        recomputed = project_compact_execution_limitations(
            projection.raw_terminal,
            receipt=projection.receipt,
        )
    except CompactDiscoveryContractError as exc:
        raise CompactDiscoveryContractError(f"merge_projection_revalidation_failed:{exc}") from exc
    if recomputed != projection:
        if projection.receipt_sha256 != recomputed.receipt_sha256:
            raise CompactDiscoveryContractError("merge_projection_receipt_digest_mismatch")
        if projection.terminal_sha256 != recomputed.terminal_sha256:
            raise CompactDiscoveryContractError("merge_projection_terminal_digest_mismatch")
        if projection.projected_result_sha256 != recomputed.projected_result_sha256:
            raise CompactDiscoveryContractError("merge_projection_result_digest_mismatch")
        raise CompactDiscoveryContractError("merge_projection_content_mismatch")
    return projection


def summarize_compact_discovery(result: Mapping[str, Any]) -> dict[str, Any]:
    """Mechanically summarize a validated result without returning lead data."""

    assert_compact_discovery_result(result)
    surface_counts: Counter[str] = Counter()
    dimension_counts: Counter[str] = Counter()
    identity_counts: Counter[str] = Counter()
    state_matrix = {
        lab_state: {pretraining_state: 0 for pretraining_state in TEMPORAL_STATES}
        for lab_state in TEMPORAL_STATES
    }
    source_ref_count = 0
    for lead in result["leads"]:
        identity_counts[lead["identity_status"]] += 1
        state_matrix[lead["target_lab_affiliation_state"]][lead["pretraining_experience_state"]] += 1
        for source_ref in lead["source_refs"]:
            source_ref_count += 1
            surface_counts[source_ref["surface"]] += 1
            dimension_counts.update(source_ref["support_dimensions"])
    return {
        "unique_lead_count": len(result["leads"]),
        "identity_status_counts": {
            status: identity_counts[status] for status in IDENTITY_STATUSES
        },
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
    """Return case-insensitive aggregate overlap metrics for provisional handle sets."""

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
    """Compare same-campaign diagnostics; platform identity is handled by union."""

    assert_compact_discovery_result(left)
    assert_compact_discovery_result(right)
    binding_fields = (
        "campaign_id",
        "target_descriptor_id",
        "target_descriptor_sha256",
        "prompt_policy_sha256",
    )
    if any(left[field] != right[field] for field in binding_fields):
        raise CompactDiscoveryContractError("comparison_campaign_binding_mismatch")
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


def _merge_history(
    leads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    by_handle: dict[str, dict[str, Any]] = {}
    for lead in leads:
        for proposal in lead["handle_history_proposals"]:
            key = proposal["handle"].casefold()
            existing = by_handle.get(key)
            if existing is None:
                by_handle[key] = copy.deepcopy(proposal)
            else:
                existing["origin_shard_ids"] = sorted(
                    set(existing["origin_shard_ids"]) | set(proposal["origin_shard_ids"])
                )
                if proposal["handle"] < existing["handle"]:
                    existing["handle"] = proposal["handle"]
                    existing["profile_url"] = canonical_profile_url(proposal["handle"])
    return sorted(by_handle.values(), key=lambda proposal: proposal["handle"].casefold())


def _merge_source_refs(source_refs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_signature: dict[tuple[str, str], dict[str, Any]] = {}
    for source_ref in source_refs:
        signature = (source_ref["surface"], source_ref["url"].casefold())
        existing = by_signature.get(signature)
        if existing is None:
            by_signature[signature] = copy.deepcopy(dict(source_ref))
            continue
        existing["support_dimensions"] = _canonical_support_dimensions(
            tuple(set(existing["support_dimensions"]) | set(source_ref["support_dimensions"]))
        )
        existing["origin_shard_ids"] = sorted(
            set(existing["origin_shard_ids"]) | set(source_ref["origin_shard_ids"])
        )
        representation = (
            source_ref["subject_handle"],
            source_ref["author_handle"],
            source_ref["url"],
        )
        existing_representation = (
            existing["subject_handle"],
            existing["author_handle"],
            existing["url"],
        )
        if representation < existing_representation:
            existing.update(
                {
                    "url": source_ref["url"],
                    "subject_handle": source_ref["subject_handle"],
                    "author_handle": source_ref["author_handle"],
                }
            )
    return sorted(
        by_signature.values(),
        key=lambda item: (
            item["surface"],
            item["url"].casefold(),
            item["subject_handle"].casefold(),
            item["author_handle"].casefold(),
        ),
    )


def merge_compact_discovery_results(
    projections: Sequence[CompactDiscoveryOperatorProjection],
    *,
    union_id: str = "operator.compact-discovery-union-v1",
    strategy_id: str = "operator.compact-discovery-union-v1",
) -> MergedCompactDiscovery:
    """Union receipt-projected, binding-compatible shards by platform identity.

    Non-null platform ids are the primary account identity.  A stable id seen
    under multiple handles becomes one lead with reversible handle-history
    proposals.  The same handle observed under multiple stable ids yields
    separate quarantined leads; their evidence is never combined.  Null ids
    remain explicit provisional-handle identities and never weaken a stable-id
    fence.
    """

    if isinstance(projections, (str, bytes)):
        raise CompactDiscoveryContractError("merge_results_invalid")
    inputs = tuple(projections)
    if not inputs:
        raise CompactDiscoveryContractError("merge_results_empty")
    if not _valid_identifier(union_id) or not _valid_identifier(strategy_id):
        raise CompactDiscoveryContractError("merge_output_identity_invalid")

    checked = tuple(_assert_projection(projection) for projection in inputs)
    results = tuple(projection.result for projection in checked)
    if any(result["result_kind"] != "shard" for result in results):
        raise CompactDiscoveryContractError("merge_input_must_be_shard")
    binding_fields = (
        "campaign_id",
        "target_descriptor_id",
        "target_descriptor_sha256",
        "prompt_policy_sha256",
    )
    first = results[0]
    for result_index, result in enumerate(results[1:], start=1):
        for field in binding_fields:
            if result[field] != first[field]:
                raise CompactDiscoveryContractError(
                    f"merge_input:{result_index}:campaign_binding_mismatch:{field}"
                )
    shard_ids = [result["shard_id"] for result in results]
    if len(set(shard_ids)) != len(shard_ids):
        raise CompactDiscoveryContractError("merge_shard_id_duplicate")
    if union_id in set(shard_ids):
        raise CompactDiscoveryContractError("merge_union_id_collides_with_shard")

    identity_groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    handle_occurrences: Counter[str] = Counter()
    stable_ids_by_alias: dict[str, set[str]] = {}
    input_lead_count = 0
    input_source_ref_count = 0
    for result in results:
        for lead in result["leads"]:
            input_lead_count += 1
            input_source_ref_count += len(lead["source_refs"])
            handle_occurrences[lead["handle"].casefold()] += 1
            platform_id = lead["platform_user_id"]
            if platform_id is None:
                key = ("provisional", lead["handle"].casefold())
            else:
                key = ("platform", platform_id)
                for proposal in lead["handle_history_proposals"]:
                    stable_ids_by_alias.setdefault(proposal["handle"].casefold(), set()).add(platform_id)
            identity_groups.setdefault(key, []).append(lead)

    reused_aliases = {
        handle for handle, platform_ids in stable_ids_by_alias.items() if len(platform_ids) > 1
    }
    merged_leads: list[dict[str, Any]] = []
    renamed_stable_identity_count = 0
    quarantined_handle_reuse_identity_count = 0
    provisional_identity_count = 0
    lab_conflicts = 0
    pretraining_conflicts = 0
    for identity_key in sorted(identity_groups):
        lead_group = identity_groups[identity_key]
        history = _merge_history(lead_group)
        canonical_handle = min(proposal["handle"] for proposal in history)
        platform_user_id = None if identity_key[0] == "provisional" else identity_key[1]
        alias_keys = {proposal["handle"].casefold() for proposal in history}
        if platform_user_id is None:
            identity_status = "provisional_handle"
            provisional_identity_count += 1
        elif alias_keys & reused_aliases:
            identity_status = "quarantined_handle_reuse"
            quarantined_handle_reuse_identity_count += 1
        else:
            identity_status = "stable_platform_id"
            if len(alias_keys) > 1:
                renamed_stable_identity_count += 1

        lab_states = [lead["target_lab_affiliation_state"] for lead in lead_group]
        pretraining_states = [lead["pretraining_experience_state"] for lead in lead_group]
        if _has_concrete_temporal_conflict(lab_states):
            lab_conflicts += 1
        if _has_concrete_temporal_conflict(pretraining_states):
            pretraining_conflicts += 1
        lab_state = _merge_temporal_states(lab_states)
        pretraining_state = _merge_temporal_states(pretraining_states)
        source_refs = _merge_source_refs(
            [source_ref for lead in lead_group for source_ref in lead["source_refs"]]
        )
        origins = sorted(
            {origin for lead in lead_group for origin in lead["origin_shard_ids"]}
        )
        merged_leads.append(
            {
                "handle": canonical_handle,
                "profile_url": canonical_profile_url(canonical_handle),
                "platform_user_id": platform_user_id,
                "identity_status": identity_status,
                "handle_history_proposals": history,
                "origin_shard_ids": origins,
                "target_lab_affiliation_state": lab_state,
                "pretraining_experience_state": pretraining_state,
                "source_status": SOURCE_STATUS,
                "source_refs": source_refs,
                "reason_codes": [
                    f"lab_affiliation_{lab_state}_signal",
                    f"pretraining_relevance_{pretraining_state}_signal",
                ],
            }
        )

    covered_set = {cell for result in results for cell in result["coverage_cells"]}
    coverage_cells = [cell for cell in COVERAGE_CELLS if cell in covered_set]
    uncovered_cells = [cell for cell in COVERAGE_CELLS if cell not in covered_set]
    limitation_set = {code for result in results for code in result["limitations"]}
    limitations = [code for code in LIMITATION_CODES if code in limitation_set]
    if all(result["status"] == "X_DISCOVERY_BLOCKED" for result in results):
        status = "X_DISCOVERY_BLOCKED"
    elif uncovered_cells or limitations:
        status = "X_DISCOVERY_PARTIAL"
    else:
        status = "X_DISCOVERY_OK"
    input_shards = sorted(
        (
            {
                "shard_id": projection.result["shard_id"],
                "projected_result_sha256": projection.projected_result_sha256,
            }
            for projection in checked
        ),
        key=lambda item: item["shard_id"],
    )
    merged_result = {
        "contract_version": CONTRACT_VERSION,
        "campaign_id": first["campaign_id"],
        "target_descriptor_id": first["target_descriptor_id"],
        "target_descriptor_sha256": first["target_descriptor_sha256"],
        "prompt_policy_sha256": first["prompt_policy_sha256"],
        "result_kind": "union",
        "shard_id": union_id,
        "input_shards": input_shards,
        "status": status,
        "strategy_id": strategy_id,
        "leads": sorted(
            merged_leads,
            key=lambda lead: (
                lead["platform_user_id"] is None,
                lead["platform_user_id"] or "",
                lead["handle"].casefold(),
            ),
        ),
        "coverage_cells": coverage_cells,
        "uncovered_cells": uncovered_cells,
        "limitations": limitations,
    }
    assert_compact_discovery_result(merged_result)
    summary = CompactDiscoveryMergeSummary(
        input_result_count=len(results),
        input_lead_count=input_lead_count,
        unique_lead_count=len(merged_leads),
        overlapping_handle_count=sum(count > 1 for count in handle_occurrences.values()),
        platform_user_id_conflict_count=len(reused_aliases),
        renamed_stable_identity_count=renamed_stable_identity_count,
        quarantined_handle_reuse_identity_count=quarantined_handle_reuse_identity_count,
        provisional_identity_count=provisional_identity_count,
        lab_affiliation_state_conflict_count=lab_conflicts,
        pretraining_experience_state_conflict_count=pretraining_conflicts,
        input_source_ref_count=input_source_ref_count,
        merged_source_ref_count=sum(len(lead["source_refs"]) for lead in merged_leads),
    )
    return MergedCompactDiscovery(result=merged_result, summary=summary)
