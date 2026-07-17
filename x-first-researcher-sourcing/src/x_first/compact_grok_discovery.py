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

from x_first.grok_operator_session_replay import (
    RAW_SESSION_SHAPE_REGISTRY_VERSION,
    FrozenRawSessionArtifact,
    GrokOperatorExecutionFacts,
    GrokOperatorSessionPrecommit,
    GrokOperatorSessionReplay,
    GrokOperatorSessionReplayError,
    ReplayedNativeToolCompletion,
    operator_execution_facts_sha256,
    replay_grok_operator_session,
    session_precommit_sha256,
    thaw_raw_session_artifacts,
)

CONTRACT_VERSION = "x.grok.compact_discovery.result.v1"
MERGE_ENVELOPE_VERSION = "x.grok.compact_discovery.merge_envelope.v1"
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
        "identity_resolution_sidecars",
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
        "lookup_handle",
        "identity_conflicts",
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
IDENTITY_RESOLUTION_SIDECAR_KEYS = frozenset(
    {
        "handle",
        "candidate_platform_user_ids",
        "provisional_lead_sha256s",
        "provisional_source_ref_sha256s",
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
IDENTITY_CONFLICT_CODES = (
    "same_handle_multiple_stable_ids_unresolved",
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
COMPACT_ALLOWED_NATIVE_X_TOOLS = frozenset(
    {"x_keyword_search", "x_semantic_search", "x_thread_fetch"}
)
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
_EXACT_HANDLE_QUERY_RE = re.compile(r"\s*@[A-Za-z0-9_]{1,15}\s*")
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
    request_id: str
    model_id: str
    transcript_sha256: str
    session_precommit_sha256: str
    raw_session_shape_registry_version: str
    operator_execution_facts_sha256: str
    system_prompt_sha256: str
    prompt_context_sha256: str
    user_prompt_sha256: str
    raw_session_artifact_sha256s: tuple[tuple[str, str], ...]
    terminal_sha256: str
    terminal_start_byte_offset: int
    terminal_end_byte_offset_exclusive: int
    terminal_start_update_index: int
    terminal_end_update_index: int
    final_assistant_update_index: int
    last_native_tool_update_index: int | None
    session_event_count: int
    started_tool_call_count: int
    completed_tool_call_count: int
    tool_completions: tuple[ReplayedNativeToolCompletion, ...]
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
    session_precommit: GrokOperatorSessionPrecommit
    operator_execution_facts: GrokOperatorExecutionFacts
    raw_session_artifacts: tuple[FrozenRawSessionArtifact, ...]
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
    absorbed_provisional_observation_count: int
    unresolved_identity_sidecar_count: int
    lab_affiliation_state_conflict_count: int
    pretraining_experience_state_conflict_count: int
    input_source_ref_count: int
    merged_source_ref_count: int


@dataclass(frozen=True)
class MergedCompactDiscovery:
    """Replay-bound compact union, merge controls, and diagnostics.

    ``input_projections`` retain the exact operator projections needed to
    replay every raw Grok session before this envelope can cross into profile
    hydration.  The cached digests are diagnostics, never authority: replay
    reconstructs the complete result and summary and compares both values.
    """

    merge_envelope_version: str
    input_projections: tuple[CompactDiscoveryOperatorProjection, ...]
    union_id: str
    strategy_id: str
    resolved_lookup_handles: tuple[tuple[str, str], ...]
    result: dict[str, Any]
    summary: CompactDiscoveryMergeSummary
    result_sha256: str
    summary_sha256: str


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

    identity_resolution_sidecars = result.get("identity_resolution_sidecars")
    if not isinstance(identity_resolution_sidecars, list):
        errors.append("identity_resolution_sidecars_invalid")
        identity_resolution_sidecars = []
    if result_kind == "shard" and identity_resolution_sidecars:
        errors.append("shard_identity_resolution_sidecars_must_be_empty")
    if identity_resolution_sidecars != sorted(
        identity_resolution_sidecars,
        key=lambda item: item.get("handle", "").casefold()
        if isinstance(item, dict) and isinstance(item.get("handle"), str)
        else "",
    ):
        errors.append("identity_resolution_sidecars_order_invalid")

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
    lookup_handle_owners: dict[str, int] = {}
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

        lookup_handle = lead.get("lookup_handle")
        identity_conflicts = lead.get("identity_conflicts")
        conflicts_valid = _valid_closed_array(identity_conflicts, IDENTITY_CONFLICT_CODES)
        if not conflicts_valid:
            errors.append(f"{prefix}:identity_conflicts_invalid")
            identity_conflicts = []
        if result_kind == "shard":
            if lookup_handle is not None:
                errors.append(f"{prefix}:shard_lookup_handle_must_be_null")
            if identity_conflicts:
                errors.append(f"{prefix}:shard_identity_conflicts_must_be_empty")

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
        if result_kind == "union":
            lookup_is_unresolved = (
                identity_status == "quarantined_handle_reuse"
                or "same_handle_multiple_stable_ids_unresolved" in identity_conflicts
            )
            if lookup_is_unresolved:
                if lookup_handle is not None:
                    errors.append(f"{prefix}:quarantined_lookup_handle_must_be_null")
            elif not _valid_handle(lookup_handle):
                errors.append(f"{prefix}:lookup_handle_invalid")
            elif lookup_handle.casefold() not in history_handles:
                errors.append(f"{prefix}:lookup_handle_not_in_history")
            else:
                lookup_key = lookup_handle.casefold()
                if lookup_key in lookup_handle_owners:
                    errors.append(f"{prefix}:lookup_handle_casefold_duplicate")
                else:
                    lookup_handle_owners[lookup_key] = lead_index
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

    seen_sidecar_handles: set[str] = set()
    for sidecar_index, sidecar in enumerate(identity_resolution_sidecars):
        prefix = f"identity_resolution_sidecar:{sidecar_index}"
        if not isinstance(sidecar, dict) or set(sidecar) != IDENTITY_RESOLUTION_SIDECAR_KEYS:
            errors.append(f"{prefix}:shape_invalid")
            continue
        handle = sidecar.get("handle")
        if not _valid_handle(handle):
            errors.append(f"{prefix}:handle_invalid")
            continue
        handle_key = handle.casefold()
        if handle_key in seen_sidecar_handles:
            errors.append(f"{prefix}:handle_duplicate")
        seen_sidecar_handles.add(handle_key)
        platform_ids = sidecar.get("candidate_platform_user_ids")
        if (
            not isinstance(platform_ids, list)
            or not platform_ids
            or platform_ids != sorted(platform_ids)
            or len(platform_ids) != len(set(platform_ids))
            or any(
                not isinstance(value, str)
                or _PLATFORM_USER_ID_RE.fullmatch(value) is None
                for value in platform_ids
            )
        ):
            errors.append(f"{prefix}:candidate_platform_user_ids_invalid")
        else:
            observed_platform_ids = {
                owner_id
                for _, owner_id, _ in handle_owners.get(handle_key, [])
                if owner_id is not None
            }
            if set(platform_ids) != observed_platform_ids:
                errors.append(f"{prefix}:candidate_platform_user_ids_mismatch")
        if not _valid_origin_ids(sidecar.get("origin_shard_ids"), allowed_origin_ids):
            errors.append(f"{prefix}:origin_binding_invalid")
        for field in ("provisional_lead_sha256s", "provisional_source_ref_sha256s"):
            digests = sidecar.get(field)
            if (
                not isinstance(digests, list)
                or not digests
                or digests != sorted(digests)
                or len(digests) != len(set(digests))
                or any(not _valid_sha256(value) for value in digests)
            ):
                errors.append(f"{prefix}:{field}_invalid")

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
    if receipt.receipt_version != "x.grok.compact_discovery.execution_receipt.v3":
        errors.append("operator_receipt_version_invalid")
    if (
        receipt.raw_session_shape_registry_version
        != RAW_SESSION_SHAPE_REGISTRY_VERSION
    ):
        errors.append("operator_receipt_raw_session_shape_registry_version_invalid")
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
    for field in ("request_id", "model_id"):
        value = getattr(receipt, field)
        if not isinstance(value, str) or _SESSION_ID_RE.fullmatch(value) is None:
            errors.append(f"operator_receipt_{field}_invalid")
    for field in (
        "transcript_sha256",
        "session_precommit_sha256",
        "operator_execution_facts_sha256",
    ):
        if not _valid_sha256(getattr(receipt, field)):
            errors.append(f"operator_receipt_{field}_invalid")
    for field in ("system_prompt_sha256", "prompt_context_sha256", "user_prompt_sha256"):
        if not _valid_sha256(getattr(receipt, field)):
            errors.append(f"operator_receipt_{field}_invalid")
    if (
        not isinstance(receipt.raw_session_artifact_sha256s, tuple)
        or not receipt.raw_session_artifact_sha256s
        or any(
            not isinstance(binding, tuple)
            or len(binding) != 2
            or not isinstance(binding[0], str)
            or not _valid_sha256(binding[1])
            for binding in receipt.raw_session_artifact_sha256s
        )
    ):
        errors.append("operator_receipt_raw_session_artifacts_invalid")
    if not _valid_sha256(receipt.terminal_sha256):
        errors.append("operator_receipt_terminal_sha256_invalid")
    elif receipt.terminal_sha256 != canonical_json_sha256(result):
        errors.append("operator_receipt_terminal_sha256_mismatch")
    integer_fields = (
        "terminal_start_byte_offset",
        "terminal_end_byte_offset_exclusive",
        "terminal_start_update_index",
        "terminal_end_update_index",
        "final_assistant_update_index",
        "session_event_count",
        "started_tool_call_count",
        "completed_tool_call_count",
    )
    if any(type(getattr(receipt, field)) is not int for field in integer_fields):
        errors.append("operator_receipt_replay_indices_invalid")
    elif (
        receipt.terminal_start_byte_offset < 0
        or receipt.terminal_end_byte_offset_exclusive <= receipt.terminal_start_byte_offset
        or receipt.terminal_start_update_index > receipt.terminal_end_update_index
        or receipt.terminal_end_update_index > receipt.final_assistant_update_index
        or receipt.session_event_count <= 0
        or receipt.started_tool_call_count < 0
        or receipt.completed_tool_call_count < 0
        or receipt.started_tool_call_count != receipt.completed_tool_call_count
    ):
        errors.append("operator_receipt_replay_indices_invalid")
    if receipt.last_native_tool_update_index is not None and (
        type(receipt.last_native_tool_update_index) is not int
        or receipt.last_native_tool_update_index >= receipt.terminal_start_update_index
    ):
        errors.append("operator_receipt_terminal_order_unproven")
    if not isinstance(receipt.tool_completions, tuple) or any(
        not isinstance(item, ReplayedNativeToolCompletion)
        for item in receipt.tool_completions
    ):
        errors.append("operator_receipt_tool_completions_invalid")
    elif len(receipt.tool_completions) != receipt.completed_tool_call_count:
        errors.append("operator_receipt_tool_completion_count_mismatch")
    for field in (
        "result_truncated",
        "execution_deadline_reached",
        "transport_failure",
        "model_output_repaired",
    ):
        if type(getattr(receipt, field)) is not bool:
            errors.append(f"operator_receipt_{field}_invalid")
    if (
        receipt.completed_tool_call_count == 0
        and receipt.execution_deadline_reached is False
        and receipt.transport_failure is False
    ):
        errors.append("operator_receipt_native_tool_call_empty")
    return errors


def _receipt_from_replay(
    result: Mapping[str, Any],
    replay: GrokOperatorSessionReplay,
    *,
    operator_execution_facts: GrokOperatorExecutionFacts,
) -> CompactDiscoveryExecutionReceipt:
    return CompactDiscoveryExecutionReceipt(
        receipt_version="x.grok.compact_discovery.execution_receipt.v3",
        campaign_id=result["campaign_id"],
        target_descriptor_id=result["target_descriptor_id"],
        target_descriptor_sha256=result["target_descriptor_sha256"],
        prompt_policy_sha256=result["prompt_policy_sha256"],
        shard_id=result["shard_id"],
        session_id=replay.session_id,
        request_id=replay.request_id,
        model_id=replay.model_id,
        transcript_sha256=replay.transcript_sha256,
        session_precommit_sha256=replay.session_precommit_sha256,
        raw_session_shape_registry_version=replay.raw_session_shape_registry_version,
        operator_execution_facts_sha256=operator_execution_facts_sha256(
            operator_execution_facts
        ),
        system_prompt_sha256=replay.system_prompt_sha256,
        prompt_context_sha256=replay.prompt_context_sha256,
        user_prompt_sha256=replay.user_prompt_sha256,
        raw_session_artifact_sha256s=replay.source_artifact_sha256s,
        terminal_sha256=canonical_json_sha256(result),
        terminal_start_byte_offset=replay.terminal_start_byte_offset,
        terminal_end_byte_offset_exclusive=replay.terminal_end_byte_offset_exclusive,
        terminal_start_update_index=replay.terminal_start_update_index,
        terminal_end_update_index=replay.terminal_end_update_index,
        final_assistant_update_index=replay.final_assistant_update_index,
        last_native_tool_update_index=replay.last_native_tool_update_index,
        session_event_count=replay.session_event_count,
        started_tool_call_count=len(replay.tool_completions),
        completed_tool_call_count=len(replay.tool_completions),
        tool_completions=replay.tool_completions,
        result_truncated=operator_execution_facts.result_truncated,
        execution_deadline_reached=operator_execution_facts.execution_deadline_reached,
        transport_failure=operator_execution_facts.transport_failure,
        model_output_repaired=operator_execution_facts.model_output_repaired,
    )


def _assert_compact_native_call_policy(replay: GrokOperatorSessionReplay) -> None:
    """Reject mechanically identifiable person-only discovery lookups."""

    for completion in replay.tool_completions:
        if (
            completion.tool_name in {"x_keyword_search", "x_semantic_search"}
            and isinstance(completion.query, str)
            and _EXACT_HANDLE_QUERY_RE.fullmatch(completion.query) is not None
        ):
            raise CompactDiscoveryContractError(
                "operator_raw_session_invalid:compact_exact_handle_query_forbidden"
            )


def _project_compact_execution_limitations(
    result: Mapping[str, Any],
    *,
    receipt: CompactDiscoveryExecutionReceipt,
    session_precommit: GrokOperatorSessionPrecommit,
    operator_execution_facts: GrokOperatorExecutionFacts,
    raw_session_artifacts: tuple[FrozenRawSessionArtifact, ...],
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
    try:
        if receipt.session_precommit_sha256 != session_precommit_sha256(
            session_precommit
        ):
            receipt_errors.append("operator_receipt_session_precommit_mismatch")
        if receipt.operator_execution_facts_sha256 != operator_execution_facts_sha256(
            operator_execution_facts
        ):
            receipt_errors.append("operator_receipt_execution_facts_mismatch")
    except GrokOperatorSessionReplayError as exc:
        receipt_errors.append(str(exc))
    if receipt_errors:
        raise CompactDiscoveryContractError(";".join(receipt_errors))
    if result.get("result_kind") != "shard":
        raise CompactDiscoveryContractError("projection_requires_shard_result")

    operator_facts = {
        "result_truncated": operator_execution_facts.result_truncated,
        "execution_deadline_reached": operator_execution_facts.execution_deadline_reached,
        "transport_failure": operator_execution_facts.transport_failure,
        "model_output_repaired": operator_execution_facts.model_output_repaired,
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
        session_precommit=session_precommit,
        operator_execution_facts=operator_execution_facts,
        raw_session_artifacts=raw_session_artifacts,
        receipt_sha256=canonical_json_sha256(asdict(receipt)),
        terminal_sha256=receipt.terminal_sha256,
        projected_result_sha256=canonical_json_sha256(projected),
        removed_model_operator_limitations=removed,
        added_operator_limitations=added,
    )


def project_compact_execution_limitations(
    result: Mapping[str, Any],
    *,
    receipt: CompactDiscoveryExecutionReceipt,
    session_precommit: GrokOperatorSessionPrecommit,
    operator_execution_facts: GrokOperatorExecutionFacts,
    raw_session_files: Mapping[str, bytes] | None = None,
) -> CompactDiscoveryOperatorProjection:
    """Replay raw bytes before accepting a receipt supplied at this boundary."""

    if raw_session_files is None:
        raise CompactDiscoveryContractError("operator_raw_session_required")
    try:
        replay = replay_grok_operator_session(
            raw_session_files,
            session_precommit=session_precommit,
            allowed_tool_names=COMPACT_ALLOWED_NATIVE_X_TOOLS,
            expected_terminal=result,
            terminal_validator=lambda value: _validate_compact_discovery_result(
                value, enforce_status_coherence=False
            ),
        )
    except GrokOperatorSessionReplayError as exc:
        raise CompactDiscoveryContractError(f"operator_raw_session_invalid:{exc}") from exc
    _assert_compact_native_call_policy(replay)
    expected_receipt = _receipt_from_replay(
        result,
        replay,
        operator_execution_facts=operator_execution_facts,
    )
    if receipt != expected_receipt:
        raise CompactDiscoveryContractError("operator_receipt_replay_mismatch")
    return _project_compact_execution_limitations(
        result,
        receipt=receipt,
        session_precommit=session_precommit,
        operator_execution_facts=operator_execution_facts,
        raw_session_artifacts=replay.source_artifacts,
    )


def build_compact_operator_projection(
    raw_session_files: Mapping[str, bytes],
    *,
    session_precommit: GrokOperatorSessionPrecommit,
    result_truncated: bool = False,
    execution_deadline_reached: bool = False,
    transport_failure: bool = False,
    model_output_repaired: bool = False,
) -> CompactDiscoveryOperatorProjection:
    """Operator-owned builder deriving terminal, receipt, and projection."""

    for value in (
        result_truncated,
        execution_deadline_reached,
        transport_failure,
        model_output_repaired,
    ):
        if type(value) is not bool:
            raise CompactDiscoveryContractError("operator_execution_fact_invalid")
    try:
        operator_execution_facts = GrokOperatorExecutionFacts(
            result_truncated=result_truncated,
            execution_deadline_reached=execution_deadline_reached,
            transport_failure=transport_failure,
            model_output_repaired=model_output_repaired,
        )
        replay = replay_grok_operator_session(
            raw_session_files,
            session_precommit=session_precommit,
            allowed_tool_names=COMPACT_ALLOWED_NATIVE_X_TOOLS,
            terminal_validator=lambda value: _validate_compact_discovery_result(
                value, enforce_status_coherence=False
            ),
        )
    except GrokOperatorSessionReplayError as exc:
        raise CompactDiscoveryContractError(f"operator_raw_session_invalid:{exc}") from exc
    _assert_compact_native_call_policy(replay)
    result = replay.terminal
    receipt = _receipt_from_replay(
        result,
        replay,
        operator_execution_facts=operator_execution_facts,
    )
    return _project_compact_execution_limitations(
        result,
        receipt=receipt,
        session_precommit=session_precommit,
        operator_execution_facts=operator_execution_facts,
        raw_session_artifacts=replay.source_artifacts,
    )


def _assert_projection(projection: Any) -> CompactDiscoveryOperatorProjection:
    if not isinstance(projection, CompactDiscoveryOperatorProjection):
        raise CompactDiscoveryContractError("merge_projection_required")
    if not isinstance(projection.result, dict) or not isinstance(projection.raw_terminal, dict):
        raise CompactDiscoveryContractError("merge_projection_result_type_invalid")
    if not isinstance(projection.receipt, CompactDiscoveryExecutionReceipt):
        raise CompactDiscoveryContractError("merge_projection_receipt_type_invalid")
    if not isinstance(projection.session_precommit, GrokOperatorSessionPrecommit):
        raise CompactDiscoveryContractError("merge_projection_session_precommit_type_invalid")
    if not isinstance(projection.operator_execution_facts, GrokOperatorExecutionFacts):
        raise CompactDiscoveryContractError("merge_projection_execution_facts_type_invalid")
    assert_compact_discovery_result(projection.result)
    try:
        raw_session_files = thaw_raw_session_artifacts(projection.raw_session_artifacts)
        recomputed = project_compact_execution_limitations(
            projection.raw_terminal,
            receipt=projection.receipt,
            session_precommit=projection.session_precommit,
            operator_execution_facts=projection.operator_execution_facts,
            raw_session_files=raw_session_files,
        )
    except (CompactDiscoveryContractError, GrokOperatorSessionReplayError) as exc:
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
    resolved_lookup_handles: Mapping[str, str] | None = None,
) -> MergedCompactDiscovery:
    """Union receipt-projected, binding-compatible shards by platform identity.

    Non-null platform ids are the primary account identity.  A stable id seen
    under multiple handles becomes one lead with reversible handle-history
    proposals.  The same handle observed under multiple stable ids yields
    separate quarantined leads; their evidence is never combined.  Null ids
    remain explicit provisional-handle identities and never weaken a stable-id
    fence.  A stable identity observed under more than one alias requires an
    explicit operator-resolved lookup handle keyed by platform user id.
    """

    if isinstance(projections, (str, bytes)):
        raise CompactDiscoveryContractError("merge_results_invalid")
    inputs = tuple(projections)
    if not inputs:
        raise CompactDiscoveryContractError("merge_results_empty")
    if not _valid_identifier(union_id) or not _valid_identifier(strategy_id):
        raise CompactDiscoveryContractError("merge_output_identity_invalid")
    if resolved_lookup_handles is None:
        resolved_lookup_handles = {}
    if not isinstance(resolved_lookup_handles, Mapping) or any(
        not isinstance(key, str) or not _valid_handle(value)
        for key, value in resolved_lookup_handles.items()
    ):
        raise CompactDiscoveryContractError("merge_resolved_lookup_handles_invalid")
    resolved_lookup_handles = dict(resolved_lookup_handles)

    checked = tuple(
        sorted(
            (
                copy.deepcopy(_assert_projection(projection))
                for projection in inputs
            ),
            key=lambda projection: projection.result["shard_id"],
        )
    )
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
    absorbed_provisional_observation_count = 0
    unresolved_sidecar_inputs: dict[
        str, tuple[list[Mapping[str, Any]], set[str]]
    ] = {}
    for identity_key in tuple(identity_groups):
        if identity_key[0] != "provisional":
            continue
        alias_key = identity_key[1]
        stable_ids = stable_ids_by_alias.get(alias_key, set())
        if stable_ids:
            provisional_leads = identity_groups.pop(identity_key)
            unresolved_sidecar_inputs[alias_key] = (
                provisional_leads,
                set(stable_ids),
            )
            if len(stable_ids) == 1:
                # Preserve the existing non-double-count diagnostic while the
                # observation remains unresolved and evidence-only.  It must
                # never enter the stable lead or produce another hydration
                # lookup until an explicit identity-resolution contract exists.
                absorbed_provisional_observation_count += len(provisional_leads)
    merged_leads: list[dict[str, Any]] = []
    renamed_stable_identity_count = 0
    quarantined_handle_reuse_identity_count = 0
    provisional_identity_count = 0
    lab_conflicts = 0
    pretraining_conflicts = 0
    consumed_lookup_resolution_ids: set[str] = set()
    for identity_key in sorted(identity_groups):
        lead_group = identity_groups[identity_key]
        history = _merge_history(lead_group)
        canonical_handle = min(proposal["handle"] for proposal in history)
        platform_user_id = None if identity_key[0] == "provisional" else identity_key[1]
        alias_keys = {proposal["handle"].casefold() for proposal in history}
        identity_conflicts: list[str] = []
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

        lookup_handle: str | None
        if (
            identity_status == "quarantined_handle_reuse"
        ):
            lookup_handle = None
        elif platform_user_id is not None and len(alias_keys) > 1:
            lookup_handle = resolved_lookup_handles.get(platform_user_id)
            if lookup_handle is None:
                raise CompactDiscoveryContractError(
                    f"merge_resolved_lookup_handle_required:{platform_user_id}"
                )
            if lookup_handle.casefold() not in alias_keys:
                raise CompactDiscoveryContractError(
                    f"merge_resolved_lookup_handle_not_in_history:{platform_user_id}"
                )
            consumed_lookup_resolution_ids.add(platform_user_id)
        else:
            lookup_handle = history[0]["handle"]
            if platform_user_id is not None and platform_user_id in resolved_lookup_handles:
                if resolved_lookup_handles[platform_user_id].casefold() not in alias_keys:
                    raise CompactDiscoveryContractError(
                        f"merge_resolved_lookup_handle_not_in_history:{platform_user_id}"
                    )
                lookup_handle = resolved_lookup_handles[platform_user_id]
                consumed_lookup_resolution_ids.add(platform_user_id)

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
                "lookup_handle": lookup_handle,
                "identity_conflicts": identity_conflicts,
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

    unused_resolution_ids = set(resolved_lookup_handles) - consumed_lookup_resolution_ids
    if unused_resolution_ids:
        raise CompactDiscoveryContractError("merge_resolved_lookup_handle_unused")

    identity_resolution_sidecars = []
    for alias_key, (provisional_leads, stable_ids) in sorted(
        unresolved_sidecar_inputs.items()
    ):
        identity_resolution_sidecars.append(
            {
                "handle": min(lead["handle"] for lead in provisional_leads),
                "candidate_platform_user_ids": sorted(stable_ids),
                "provisional_lead_sha256s": sorted(
                    {canonical_json_sha256(lead) for lead in provisional_leads}
                ),
                "provisional_source_ref_sha256s": sorted(
                    {
                        canonical_json_sha256(source_ref)
                        for lead in provisional_leads
                        for source_ref in lead["source_refs"]
                    }
                ),
                "origin_shard_ids": sorted(
                    {
                        origin
                        for lead in provisional_leads
                        for origin in lead["origin_shard_ids"]
                    }
                ),
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
        "identity_resolution_sidecars": identity_resolution_sidecars,
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
        absorbed_provisional_observation_count=absorbed_provisional_observation_count,
        unresolved_identity_sidecar_count=len(identity_resolution_sidecars),
        lab_affiliation_state_conflict_count=lab_conflicts,
        pretraining_experience_state_conflict_count=pretraining_conflicts,
        input_source_ref_count=input_source_ref_count,
        merged_source_ref_count=sum(len(lead["source_refs"]) for lead in merged_leads),
    )
    return MergedCompactDiscovery(
        merge_envelope_version=MERGE_ENVELOPE_VERSION,
        input_projections=checked,
        union_id=union_id,
        strategy_id=strategy_id,
        resolved_lookup_handles=tuple(sorted(resolved_lookup_handles.items())),
        result=merged_result,
        summary=summary,
        result_sha256=canonical_json_sha256(merged_result),
        summary_sha256=canonical_json_sha256(asdict(summary)),
    )


def replay_merged_compact_discovery(value: Any) -> MergedCompactDiscovery:
    """Replay every retained shard and reconstruct an exact merge envelope.

    This is the normal handoff gate for downstream hydration.  A JSON union,
    even one that is schema-valid and internally digest-consistent, carries no
    replay authority and is deliberately rejected here.
    """

    if not isinstance(value, MergedCompactDiscovery):
        raise CompactDiscoveryContractError("merge_envelope_required")
    if value.merge_envelope_version != MERGE_ENVELOPE_VERSION:
        raise CompactDiscoveryContractError("merge_envelope_version_invalid")
    if not isinstance(value.input_projections, tuple) or not value.input_projections:
        raise CompactDiscoveryContractError("merge_envelope_projections_invalid")
    if not isinstance(value.resolved_lookup_handles, tuple) or any(
        not isinstance(item, tuple) or len(item) != 2
        for item in value.resolved_lookup_handles
    ):
        raise CompactDiscoveryContractError("merge_envelope_lookup_controls_invalid")
    if any(
        not isinstance(key, str) or not _valid_handle(handle)
        for key, handle in value.resolved_lookup_handles
    ):
        raise CompactDiscoveryContractError("merge_envelope_lookup_controls_invalid")
    if value.resolved_lookup_handles != tuple(sorted(value.resolved_lookup_handles)):
        raise CompactDiscoveryContractError("merge_envelope_lookup_controls_noncanonical")
    if len({key for key, _value in value.resolved_lookup_handles}) != len(
        value.resolved_lookup_handles
    ):
        raise CompactDiscoveryContractError("merge_envelope_lookup_controls_duplicate")
    try:
        recomputed = merge_compact_discovery_results(
            value.input_projections,
            union_id=value.union_id,
            strategy_id=value.strategy_id,
            resolved_lookup_handles=dict(value.resolved_lookup_handles),
        )
    except (CompactDiscoveryContractError, TypeError, ValueError) as exc:
        raise CompactDiscoveryContractError("merge_envelope_replay_failed") from exc
    if value.input_projections != recomputed.input_projections:
        raise CompactDiscoveryContractError("merge_envelope_projection_set_mismatch")
    if value.result != recomputed.result:
        raise CompactDiscoveryContractError("merge_envelope_result_mismatch")
    if value.summary != recomputed.summary:
        raise CompactDiscoveryContractError("merge_envelope_summary_mismatch")
    if value.result_sha256 != canonical_json_sha256(value.result):
        raise CompactDiscoveryContractError("merge_envelope_result_digest_invalid")
    if value.summary_sha256 != canonical_json_sha256(asdict(value.summary)):
        raise CompactDiscoveryContractError("merge_envelope_summary_digest_invalid")
    if value != recomputed:
        raise CompactDiscoveryContractError("merge_envelope_content_mismatch")
    return recomputed
