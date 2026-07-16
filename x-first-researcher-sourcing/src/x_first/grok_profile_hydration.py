"""Receipt-bound Grok/X profile hydration contracts.

The model result is only a model-mediated observation.  Acceptance requires a
typed operator projection whose receipt binds the campaign, exact input set,
raw terminal digest, transcript, session, and paired native-tool lifecycle.
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
from urllib.parse import urlsplit

from x_first.compact_grok_discovery import validate_compact_discovery_result
from x_first.grok_operator_session_replay import (
    RAW_SESSION_SHAPE_REGISTRY_VERSION,
    FrozenRawSessionArtifact,
    GrokOperatorExecutionFacts,
    GrokOperatorSessionPrecommit,
    GrokOperatorSessionReplay,
    GrokOperatorSessionReplayError,
    operator_execution_facts_sha256,
    replay_grok_operator_session,
    session_precommit_sha256,
    thaw_raw_session_artifacts,
)
from x_first.recall_pool_schema import load_contract_schema, schema_errors

CONTRACT_VERSION = "x.grok.profile_hydration.result.v1"
SCHEMA_FILENAME = "x.grok.profile_hydration.result.v1.schema.json"
RESULT_KEYS = frozenset(
    {
        "contract_version",
        "campaign_id",
        "target_descriptor_id",
        "target_descriptor_sha256",
        "prompt_policy_sha256",
        "discovery_union_sha256",
        "input_set_sha256",
        "run_id",
        "batch_id",
        "status",
        "records",
        "limitations",
    }
)
RECORD_KEYS = frozenset(
    {
        "input_handle",
        "lookup_status",
        "matched_handle",
        "platform_user_id",
        "display_name",
        "bio",
        "location",
        "external_urls",
        "professional_category",
        "affiliations",
        "verification",
        "org_affiliation_signals",
        "source_status",
        "missing_fields",
        "limitations",
    }
)
AFFILIATION_KEYS = frozenset(
    {"organization_name", "organization_handle", "temporal_state", "evidence_source"}
)
VERIFICATION_KEYS = frozenset(
    {"account_verified", "organization_affiliation_badge_observed"}
)

STATUSES = (
    "X_PROFILE_HYDRATION_OK",
    "X_PROFILE_HYDRATION_PARTIAL",
    "X_PROFILE_HYDRATION_BLOCKED",
)
LOOKUP_STATUSES = ("matched", "not_found", "blocked", "error")
TEMPORAL_STATES = ("current", "historical", "ambiguous")
AFFILIATION_EVIDENCE_SOURCES = (
    "bio_explicit",
    "profile_field",
    "profile_affiliation_badge",
)
SOURCE_STATUS = "model_mediated_unverified"
HYDRATION_FIELDS = (
    "platform_user_id",
    "display_name",
    "bio",
    "location",
    "external_urls",
    "professional_category",
    "affiliations",
    "verification",
    "org_affiliation_signals",
)
BATCH_LIMITATIONS = (
    "native_x_lookup_incomplete",
    "execution_deadline_reached",
    "transport_failure",
    "result_truncated",
    "model_output_repaired",
)
MODEL_OWNED_BATCH_LIMITATIONS = ("native_x_lookup_incomplete",)
OPERATOR_OWNED_BATCH_LIMITATIONS = (
    "execution_deadline_reached",
    "transport_failure",
    "result_truncated",
    "model_output_repaired",
)
RECORD_LIMITATIONS = (
    "profile_fields_not_exposed",
    "ambiguous_profile_match",
    "lookup_not_found",
    "lookup_blocked",
    "lookup_error",
    "model_output_repaired",
)
LOOKUP_FAILURE_LIMITATION = {
    "not_found": "lookup_not_found",
    "blocked": "lookup_blocked",
    "error": "lookup_error",
}
ORTHOGONAL_UNMATCHED_LIMITATIONS = frozenset({"ambiguous_profile_match", "model_output_repaired"})

_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_IDENTIFIER_RE = re.compile(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{0,19}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_SESSION_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
_CALL_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,255}")
_LEAD_IDENTITY_RE = re.compile(
    r"(?:platform:[1-9][0-9]{0,19}|provisional:[a-z0-9_]{1,15})"
)


class ProfileHydrationContractError(ValueError):
    """Raised when profile-hydration evidence cannot preserve the contract."""


@dataclass(frozen=True)
class ProfileHydrationToolCompletion:
    """One start/completion-paired native call from the session ledger."""

    call_id: str
    provider_call_id: str
    tool_name: str
    query: str
    arguments_json: str
    started_ledger_sequence: int
    completed_ledger_sequence: int
    start_event_sha256: str
    completion_event_sha256: str
    completion_status: str = "completed"


@dataclass(frozen=True)
class ProfileHydrationExecutionReceipt:
    """Operator-owned execution and transcript reconciliation evidence."""

    receipt_version: str
    campaign_id: str
    target_descriptor_id: str
    target_descriptor_sha256: str
    prompt_policy_sha256: str
    discovery_union_sha256: str
    input_set_sha256: str
    run_id: str
    batch_id: str
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
    result_truncated: bool
    execution_deadline_reached: bool
    transport_failure: bool
    model_output_repaired: bool
    tool_completions: tuple[ProfileHydrationToolCompletion, ...]


@dataclass(frozen=True)
class ProfileHydrationBatchExpectation:
    """Owner-supplied campaign and exact-input binding for evaluation."""

    campaign_id: str
    target_descriptor_id: str
    target_descriptor_sha256: str
    prompt_policy_sha256: str
    discovery_union_sha256: str
    run_id: str
    batch_id: str
    input_identities: tuple[ProfileHydrationIdentityExpectation, ...]


@dataclass(frozen=True)
class ProfileHydrationIdentityExpectation:
    """Closed union-to-lookup identity tuple owned by the operator."""

    lead_identity: str
    lookup_handle: str
    expected_platform_user_id: str | None
    identity_state: str


@dataclass(frozen=True)
class ProfileHydrationOperatorProjection:
    """Receipt-bound normalized result eligible for batch evaluation."""

    raw_terminal: dict[str, Any]
    result: dict[str, Any]
    receipt: ProfileHydrationExecutionReceipt
    session_precommit: GrokOperatorSessionPrecommit
    operator_execution_facts: GrokOperatorExecutionFacts
    raw_session_artifacts: tuple[FrozenRawSessionArtifact, ...]
    receipt_sha256: str
    terminal_sha256: str
    projected_result_sha256: str
    removed_model_operator_limitations: tuple[str, ...]
    added_operator_limitations: tuple[str, ...]
    removed_model_record_repair_count: int
    added_operator_record_repair_count: int


def canonical_json_sha256(value: Any) -> str:
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
    return isinstance(value, str) and _HANDLE_RE.fullmatch(value) is not None


def _valid_closed_array(value: Any, allowed: Sequence[str]) -> bool:
    return (
        isinstance(value, list)
        and all(isinstance(item, str) and item in allowed for item in value)
        and len(value) == len(set(value))
    )


def _identity_expectation_errors(value: Any) -> list[str]:
    if not isinstance(value, ProfileHydrationIdentityExpectation):
        return ["expectation_identity_type_invalid"]
    errors: list[str] = []
    if (
        not isinstance(value.lead_identity, str)
        or _LEAD_IDENTITY_RE.fullmatch(value.lead_identity) is None
    ):
        errors.append("expectation_lead_identity_invalid")
    if not _valid_handle(value.lookup_handle):
        errors.append("expectation_lookup_handle_invalid")
    if value.identity_state == "stable_platform_id":
        if (
            not isinstance(value.expected_platform_user_id, str)
            or _PLATFORM_USER_ID_RE.fullmatch(value.expected_platform_user_id) is None
            or value.lead_identity != f"platform:{value.expected_platform_user_id}"
        ):
            errors.append("expectation_stable_identity_binding_invalid")
    elif value.identity_state == "provisional_handle":
        if (
            value.expected_platform_user_id is not None
            or not _valid_handle(value.lookup_handle)
            or value.lead_identity != f"provisional:{value.lookup_handle.casefold()}"
        ):
            errors.append("expectation_provisional_identity_binding_invalid")
    else:
        errors.append("expectation_identity_state_invalid")
    return errors


def profile_identity_input_set_sha256(
    identities: Sequence[ProfileHydrationIdentityExpectation],
) -> str:
    """Hash the closed, order-independent discovery-to-lookup identity set."""

    if isinstance(identities, (str, bytes)) or not isinstance(identities, Sequence):
        raise ProfileHydrationContractError("expected_identities_invalid")
    values = tuple(identities)
    if not values:
        raise ProfileHydrationContractError("expected_identities_invalid")
    errors = [error for value in values for error in _identity_expectation_errors(value)]
    handles = [
        value.lookup_handle.casefold()
        for value in values
        if isinstance(value, ProfileHydrationIdentityExpectation)
    ]
    lead_ids = [
        value.lead_identity
        for value in values
        if isinstance(value, ProfileHydrationIdentityExpectation)
    ]
    if (
        errors
        or len(handles) != len(values)
        or len(handles) != len(set(handles))
        or len(lead_ids) != len(set(lead_ids))
    ):
        raise ProfileHydrationContractError("expected_identities_invalid")
    rows = sorted(
        (
            {
                "lead_identity": value.lead_identity,
                "lookup_handle": value.lookup_handle.casefold(),
                "expected_platform_user_id": value.expected_platform_user_id,
                "identity_state": value.identity_state,
            }
            for value in values
        ),
        key=lambda row: (row["lookup_handle"], row["lead_identity"]),
    )
    return canonical_json_sha256(rows)


def profile_input_set_sha256(handles: Sequence[str]) -> str:
    """Legacy handle-only digest is closed; callers must carry identity tuples."""

    raise ProfileHydrationContractError("handle_only_input_set_retired")


def build_profile_hydration_expectation_from_union(
    discovery_union: Mapping[str, Any],
    *,
    run_id: str,
    batch_id: str,
) -> ProfileHydrationBatchExpectation:
    """Build the only normal hydration input from an exact compact union."""

    errors = validate_compact_discovery_result(discovery_union)
    if errors:
        raise ProfileHydrationContractError("discovery_union_invalid")
    if discovery_union.get("result_kind") != "union":
        raise ProfileHydrationContractError("discovery_union_required")
    if not _valid_identifier(run_id) or not _valid_identifier(batch_id):
        raise ProfileHydrationContractError("hydration_run_identity_invalid")
    identities: list[ProfileHydrationIdentityExpectation] = []
    for lead in discovery_union["leads"]:
        lookup_handle = lead["lookup_handle"]
        identity_status = lead["identity_status"]
        if lookup_handle is None or identity_status == "quarantined_handle_reuse":
            continue
        platform_user_id = lead["platform_user_id"]
        if platform_user_id is None:
            identity = ProfileHydrationIdentityExpectation(
                lead_identity=f"provisional:{lookup_handle.casefold()}",
                lookup_handle=lookup_handle,
                expected_platform_user_id=None,
                identity_state="provisional_handle",
            )
        else:
            identity = ProfileHydrationIdentityExpectation(
                lead_identity=f"platform:{platform_user_id}",
                lookup_handle=lookup_handle,
                expected_platform_user_id=platform_user_id,
                identity_state="stable_platform_id",
            )
        identities.append(identity)
    if not identities:
        raise ProfileHydrationContractError("discovery_union_has_no_resolved_lookup_inputs")
    profile_identity_input_set_sha256(identities)
    return ProfileHydrationBatchExpectation(
        campaign_id=discovery_union["campaign_id"],
        target_descriptor_id=discovery_union["target_descriptor_id"],
        target_descriptor_sha256=discovery_union["target_descriptor_sha256"],
        prompt_policy_sha256=discovery_union["prompt_policy_sha256"],
        discovery_union_sha256=canonical_json_sha256(discovery_union),
        run_id=run_id,
        batch_id=batch_id,
        input_identities=tuple(identities),
    )


def _valid_external_url(value: Any) -> bool:
    if not isinstance(value, str) or len(value) > 2048 or any(character.isspace() for character in value):
        return False
    parsed = urlsplit(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _validate_affiliations(
    affiliations: Any,
    *,
    record_index: int,
) -> tuple[list[str], set[str]]:
    prefix = f"record:{record_index}:affiliations"
    if affiliations is None:
        return [], set()
    if not isinstance(affiliations, list):
        return [f"{prefix}_invalid"], set()
    errors: list[str] = []
    evidence_sources: set[str] = set()
    signatures: set[tuple[str, str, str, str]] = set()
    for affiliation_index, affiliation in enumerate(affiliations):
        item_prefix = f"{prefix}:{affiliation_index}"
        if not isinstance(affiliation, dict) or set(affiliation) != AFFILIATION_KEYS:
            errors.append(f"{item_prefix}:shape_invalid")
            continue
        name = affiliation.get("organization_name")
        handle = affiliation.get("organization_handle")
        temporal_state = affiliation.get("temporal_state")
        evidence_source = affiliation.get("evidence_source")
        if not isinstance(name, str) or not name or len(name) > 256:
            errors.append(f"{item_prefix}:organization_name_invalid")
        if handle is not None and not _valid_handle(handle):
            errors.append(f"{item_prefix}:organization_handle_invalid")
        if temporal_state not in TEMPORAL_STATES:
            errors.append(f"{item_prefix}:temporal_state_invalid")
        if evidence_source not in AFFILIATION_EVIDENCE_SOURCES:
            errors.append(f"{item_prefix}:evidence_source_invalid")
        else:
            evidence_sources.add(evidence_source)
        if (
            isinstance(name, str)
            and (handle is None or isinstance(handle, str))
            and isinstance(temporal_state, str)
            and isinstance(evidence_source, str)
        ):
            signature = (
                name.casefold(),
                handle.casefold() if isinstance(handle, str) else "",
                temporal_state,
                evidence_source,
            )
            if signature in signatures:
                errors.append(f"{item_prefix}:duplicate")
            signatures.add(signature)
    return errors, evidence_sources


def _validate_profile_hydration_result(
    result: Any,
    *,
    enforce_status_coherence: bool,
    enforce_repair_ownership: bool,
) -> list[str]:
    schema = load_contract_schema(SCHEMA_FILENAME)
    errors = [f"schema:{error}" for error in schema_errors(result, schema)]
    if not isinstance(result, dict) or set(result) != RESULT_KEYS:
        errors.append("result_shape_invalid")
        return list(dict.fromkeys(errors))
    if result.get("contract_version") != CONTRACT_VERSION:
        errors.append("contract_version_invalid")
    for field in ("campaign_id", "target_descriptor_id", "run_id", "batch_id"):
        if not _valid_identifier(result.get(field)):
            errors.append(f"{field}_invalid")
    for field in (
        "target_descriptor_sha256",
        "prompt_policy_sha256",
        "discovery_union_sha256",
        "input_set_sha256",
    ):
        if not _valid_sha256(result.get(field)):
            errors.append(f"{field}_invalid")
    status = result.get("status")
    if status not in STATUSES:
        errors.append("status_invalid")
    limitations = result.get("limitations")
    if not _valid_closed_array(limitations, BATCH_LIMITATIONS):
        errors.append("limitations_invalid")
        limitations = []
    batch_repaired = "model_output_repaired" in limitations

    records = result.get("records")
    if not isinstance(records, list):
        errors.append("records_invalid")
        records = []
    seen_input_handles: set[str] = set()
    seen_platform_user_ids: set[str] = set()
    lookup_statuses: list[str] = []
    for record_index, record in enumerate(records):
        prefix = f"record:{record_index}"
        if not isinstance(record, dict) or set(record) != RECORD_KEYS:
            errors.append(f"{prefix}:shape_invalid")
            continue
        input_handle = record.get("input_handle")
        if not _valid_handle(input_handle):
            errors.append(f"{prefix}:input_handle_invalid")
            input_key = None
        else:
            input_key = input_handle.casefold()
            if input_key in seen_input_handles:
                errors.append(f"{prefix}:input_handle_casefold_duplicate")
            seen_input_handles.add(input_key)
        lookup_status = record.get("lookup_status")
        if lookup_status not in LOOKUP_STATUSES:
            errors.append(f"{prefix}:lookup_status_invalid")
        else:
            lookup_statuses.append(lookup_status)
        matched_handle = record.get("matched_handle")
        if lookup_status == "matched":
            if not _valid_handle(matched_handle) or input_key is None or matched_handle.casefold() != input_key:
                errors.append(f"{prefix}:matched_handle_binding_invalid")
        elif matched_handle is not None:
            errors.append(f"{prefix}:unmatched_handle_must_be_null")

        platform_user_id = record.get("platform_user_id")
        if platform_user_id is not None and (
            not isinstance(platform_user_id, str) or _PLATFORM_USER_ID_RE.fullmatch(platform_user_id) is None
        ):
            errors.append(f"{prefix}:platform_user_id_invalid")
        elif isinstance(platform_user_id, str):
            if platform_user_id in seen_platform_user_ids:
                errors.append(f"{prefix}:platform_user_id_duplicate")
            seen_platform_user_ids.add(platform_user_id)

        external_urls = record.get("external_urls")
        if external_urls is not None:
            if not isinstance(external_urls, list) or not all(_valid_external_url(url) for url in external_urls):
                errors.append(f"{prefix}:external_urls_invalid")
            elif len({url.casefold() for url in external_urls}) != len(external_urls):
                errors.append(f"{prefix}:external_urls_casefold_duplicate")

        affiliation_errors, evidence_sources = _validate_affiliations(
            record.get("affiliations"),
            record_index=record_index,
        )
        errors.extend(affiliation_errors)
        signals = record.get("org_affiliation_signals")
        if signals is not None and not _valid_closed_array(signals, AFFILIATION_EVIDENCE_SOURCES):
            errors.append(f"{prefix}:org_affiliation_signals_invalid")
        if record.get("affiliations") is None:
            if signals is not None:
                errors.append(f"{prefix}:org_affiliation_signals_orphaned")
        elif isinstance(signals, list) and set(signals) != evidence_sources:
            errors.append(f"{prefix}:org_affiliation_signals_mismatch")
        elif signals is None:
            errors.append(f"{prefix}:org_affiliation_signals_mismatch")

        verification = record.get("verification")
        badge_signal = isinstance(signals, list) and "profile_affiliation_badge" in signals
        if verification is not None:
            if not isinstance(verification, dict) or set(verification) != VERIFICATION_KEYS:
                errors.append(f"{prefix}:verification_invalid")
            else:
                account_verified = verification.get("account_verified")
                badge_observed = verification.get("organization_affiliation_badge_observed")
                if account_verified is not None and not isinstance(account_verified, bool):
                    errors.append(f"{prefix}:verification_invalid")
                if badge_observed is not None and not isinstance(badge_observed, bool):
                    errors.append(f"{prefix}:verification_invalid")
                if badge_signal != (badge_observed is True):
                    errors.append(f"{prefix}:verification_badge_signal_mismatch")
        elif badge_signal:
            errors.append(f"{prefix}:verification_badge_signal_mismatch")
        if record.get("source_status") != SOURCE_STATUS:
            errors.append(f"{prefix}:source_status_invalid")

        missing_fields = record.get("missing_fields")
        if not _valid_closed_array(missing_fields, HYDRATION_FIELDS):
            errors.append(f"{prefix}:missing_fields_invalid")
        else:
            expected_missing = {field for field in HYDRATION_FIELDS if record.get(field) is None}
            if set(missing_fields) != expected_missing:
                errors.append(f"{prefix}:missing_fields_mismatch")

        record_limitations = record.get("limitations")
        if not _valid_closed_array(record_limitations, RECORD_LIMITATIONS):
            errors.append(f"{prefix}:limitations_invalid")
            record_limitations = []
        record_limitation_set = set(record_limitations)
        lookup_failure_codes = set(LOOKUP_FAILURE_LIMITATION.values())
        if lookup_status == "matched":
            if lookup_failure_codes & record_limitation_set:
                errors.append(f"{prefix}:matched_lookup_limitation_invalid")
            missing_profile_fields = isinstance(missing_fields, list) and bool(missing_fields)
            if missing_profile_fields != ("profile_fields_not_exposed" in record_limitation_set):
                errors.append(f"{prefix}:profile_fields_limitation_mismatch")
        elif lookup_status in LOOKUP_FAILURE_LIMITATION:
            expected_limitation = LOOKUP_FAILURE_LIMITATION[lookup_status]
            actual_lookup_codes = lookup_failure_codes & record_limitation_set
            if actual_lookup_codes != {expected_limitation}:
                errors.append(f"{prefix}:lookup_limitation_mismatch")
            unexpected_orthogonal = record_limitation_set - {
                expected_limitation,
                *ORTHOGONAL_UNMATCHED_LIMITATIONS,
            }
            if unexpected_orthogonal:
                errors.append(f"{prefix}:unmatched_limitation_invalid")
            if any(record.get(field) is not None for field in HYDRATION_FIELDS):
                errors.append(f"{prefix}:unmatched_profile_fields_must_be_null")
        if enforce_repair_ownership and (
            ("model_output_repaired" in record_limitation_set) != batch_repaired
        ):
            errors.append(f"{prefix}:record_repair_ownership_mismatch")

    if enforce_status_coherence:
        if status == "X_PROFILE_HYDRATION_OK" and (
            not records or any(item != "matched" for item in lookup_statuses) or limitations
        ):
            errors.append("status_coherence_invalid")
        if status == "X_PROFILE_HYDRATION_PARTIAL" and (
            not limitations and (not lookup_statuses or all(item == "matched" for item in lookup_statuses))
        ):
            errors.append("status_coherence_invalid")
        if status == "X_PROFILE_HYDRATION_BLOCKED" and (
            not limitations or any(item != "blocked" for item in lookup_statuses)
        ):
            errors.append("status_coherence_invalid")
    return list(dict.fromkeys(errors))


def validate_profile_hydration_result(result: Any) -> list[str]:
    """Return schema, cross-field, status, and operator-ownership errors."""

    return _validate_profile_hydration_result(
        result,
        enforce_status_coherence=True,
        enforce_repair_ownership=True,
    )


def _validate_execution_receipt(
    result: Mapping[str, Any],
    receipt: ProfileHydrationExecutionReceipt,
) -> list[str]:
    if not isinstance(receipt, ProfileHydrationExecutionReceipt):
        return ["operator_receipt_type_invalid"]
    errors: list[str] = []
    if receipt.receipt_version != "x.grok.profile_hydration.execution_receipt.v3":
        errors.append("operator_receipt_version_invalid")
    if (
        receipt.raw_session_shape_registry_version
        != RAW_SESSION_SHAPE_REGISTRY_VERSION
    ):
        errors.append("operator_receipt_raw_session_shape_registry_version_invalid")
    for field in ("campaign_id", "target_descriptor_id", "run_id", "batch_id"):
        value = getattr(receipt, field)
        if not _valid_identifier(value):
            errors.append(f"operator_receipt_{field}_invalid")
        if value != result.get(field):
            errors.append(f"operator_receipt_{field}_mismatch")
    for field in (
        "target_descriptor_sha256",
        "prompt_policy_sha256",
        "discovery_union_sha256",
        "input_set_sha256",
    ):
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
    )
    if any(type(getattr(receipt, field)) is not int for field in integer_fields):
        errors.append("operator_receipt_replay_indices_invalid")
    elif (
        receipt.terminal_start_byte_offset < 0
        or receipt.terminal_end_byte_offset_exclusive <= receipt.terminal_start_byte_offset
        or receipt.terminal_start_update_index > receipt.terminal_end_update_index
        or receipt.terminal_end_update_index > receipt.final_assistant_update_index
        or receipt.session_event_count <= 0
    ):
        errors.append("operator_receipt_replay_indices_invalid")
    if receipt.last_native_tool_update_index is not None and (
        type(receipt.last_native_tool_update_index) is not int
        or receipt.last_native_tool_update_index >= receipt.terminal_start_update_index
    ):
        errors.append("operator_receipt_terminal_order_invalid")
    for field in (
        "result_truncated",
        "execution_deadline_reached",
        "transport_failure",
        "model_output_repaired",
    ):
        if type(getattr(receipt, field)) is not bool:
            errors.append(f"operator_receipt_{field}_invalid")
    if not isinstance(receipt.tool_completions, tuple):
        errors.append("operator_receipt_tool_completions_invalid")
        completions: tuple[Any, ...] = ()
    else:
        completions = receipt.tool_completions
    call_ids: set[str] = set()
    update_indices: set[int] = set()
    event_digests: set[str] = set()
    previous_start = 0
    for index, completion in enumerate(completions):
        prefix = f"operator_receipt_call:{index}"
        if not isinstance(completion, ProfileHydrationToolCompletion):
            errors.append(f"{prefix}:type_invalid")
            continue
        if not isinstance(completion.call_id, str) or _CALL_ID_RE.fullmatch(completion.call_id) is None:
            errors.append(f"{prefix}:call_id_invalid")
        elif completion.call_id in call_ids:
            errors.append(f"{prefix}:call_id_duplicate")
        else:
            call_ids.add(completion.call_id)
        if (
            not isinstance(completion.provider_call_id, str)
            or _CALL_ID_RE.fullmatch(completion.provider_call_id) is None
        ):
            errors.append(f"{prefix}:provider_call_id_invalid")
        if completion.completion_status != "completed":
            errors.append(f"{prefix}:completion_status_invalid")
        if completion.tool_name != "x_user_search":
            errors.append(f"{prefix}:unexpected_tool")
        if not _valid_handle(completion.query):
            errors.append(f"{prefix}:query_not_bare_handle")
        if not isinstance(completion.arguments_json, str):
            errors.append(f"{prefix}:arguments_json_invalid")
        else:
            try:
                arguments = json.loads(completion.arguments_json)
            except (TypeError, ValueError):
                arguments = None
            if (
                not isinstance(arguments, dict)
                or arguments.get("query") != completion.query
                or json.dumps(
                    arguments,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                != completion.arguments_json
            ):
                errors.append(f"{prefix}:arguments_json_invalid")
        for digest_field in ("start_event_sha256", "completion_event_sha256"):
            digest = getattr(completion, digest_field)
            if not _valid_sha256(digest):
                errors.append(f"{prefix}:{digest_field}_invalid")
            elif digest in event_digests:
                errors.append(f"{prefix}:event_digest_duplicate")
            else:
                event_digests.add(digest)
        start = completion.started_ledger_sequence
        end = completion.completed_ledger_sequence
        sequences_valid = (
            type(start) is int
            and type(end) is int
            and start >= 0
            and end > start
        )
        if not sequences_valid:
            errors.append(f"{prefix}:lifecycle_sequence_invalid")
        elif end >= receipt.terminal_start_update_index:
            errors.append(f"{prefix}:terminal_order_invalid")
        if sequences_valid:
            if start in update_indices or end in update_indices:
                errors.append(f"{prefix}:update_index_duplicate")
            update_indices.update((start, end))
        if type(start) is int and start < previous_start:
            errors.append(f"{prefix}:ledger_order_invalid")
        if type(start) is int:
            previous_start = start
    return list(dict.fromkeys(errors))


def _receipt_from_replay(
    result: Mapping[str, Any],
    replay: GrokOperatorSessionReplay,
    *,
    operator_execution_facts: GrokOperatorExecutionFacts,
) -> ProfileHydrationExecutionReceipt:
    completions = tuple(
        ProfileHydrationToolCompletion(
            call_id=item.call_id,
            provider_call_id=item.provider_call_id,
            tool_name=item.tool_name,
            query=item.query if isinstance(item.query, str) else "",
            arguments_json=item.arguments_json,
            started_ledger_sequence=item.started_update_index,
            completed_ledger_sequence=item.completed_update_index,
            start_event_sha256=item.start_event_sha256,
            completion_event_sha256=item.completion_event_sha256,
        )
        for item in replay.tool_completions
    )
    return ProfileHydrationExecutionReceipt(
        receipt_version="x.grok.profile_hydration.execution_receipt.v3",
        campaign_id=result["campaign_id"],
        target_descriptor_id=result["target_descriptor_id"],
        target_descriptor_sha256=result["target_descriptor_sha256"],
        prompt_policy_sha256=result["prompt_policy_sha256"],
        discovery_union_sha256=result["discovery_union_sha256"],
        input_set_sha256=result["input_set_sha256"],
        run_id=result["run_id"],
        batch_id=result["batch_id"],
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
        result_truncated=operator_execution_facts.result_truncated,
        execution_deadline_reached=operator_execution_facts.execution_deadline_reached,
        transport_failure=operator_execution_facts.transport_failure,
        model_output_repaired=operator_execution_facts.model_output_repaired,
        tool_completions=completions,
    )


def _project_profile_execution_limitations(
    result: Mapping[str, Any],
    *,
    receipt: ProfileHydrationExecutionReceipt,
    session_precommit: GrokOperatorSessionPrecommit,
    operator_execution_facts: GrokOperatorExecutionFacts,
    raw_session_artifacts: tuple[FrozenRawSessionArtifact, ...],
) -> ProfileHydrationOperatorProjection:
    """Replace technical and record-repair claims using a bound receipt."""

    structural_errors = _validate_profile_hydration_result(
        result,
        enforce_status_coherence=False,
        enforce_repair_ownership=False,
    )
    if structural_errors:
        raise ProfileHydrationContractError(";".join(structural_errors))
    receipt_errors = _validate_execution_receipt(result, receipt)
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
        raise ProfileHydrationContractError(";".join(receipt_errors))

    operator_facts = {
        "execution_deadline_reached": operator_execution_facts.execution_deadline_reached,
        "transport_failure": operator_execution_facts.transport_failure,
        "result_truncated": operator_execution_facts.result_truncated,
        "model_output_repaired": operator_execution_facts.model_output_repaired,
    }
    model_limitations = set(result["limitations"])
    removed = tuple(code for code in OPERATOR_OWNED_BATCH_LIMITATIONS if code in model_limitations)
    added = tuple(code for code in OPERATOR_OWNED_BATCH_LIMITATIONS if operator_facts[code])
    retained = {code for code in model_limitations if code in MODEL_OWNED_BATCH_LIMITATIONS}
    projected_limitations = [
        code for code in BATCH_LIMITATIONS if code in retained or code in added
    ]

    projected = copy.deepcopy(result)
    projected["limitations"] = projected_limitations
    removed_record_repairs = 0
    added_record_repairs = 0
    for record in projected["records"]:
        record_limitations = [
            code for code in record["limitations"] if code != "model_output_repaired"
        ]
        if "model_output_repaired" in record["limitations"]:
            removed_record_repairs += 1
        if operator_execution_facts.model_output_repaired:
            record_limitations.append("model_output_repaired")
            added_record_repairs += 1
        record["limitations"] = [
            code for code in RECORD_LIMITATIONS if code in set(record_limitations)
        ]

    lookup_statuses = [record["lookup_status"] for record in projected["records"]]
    hard_execution_failure = (
        operator_execution_facts.execution_deadline_reached
        or operator_execution_facts.transport_failure
    )
    if hard_execution_failure and (
        not lookup_statuses or all(status == "blocked" for status in lookup_statuses)
    ):
        projected["status"] = "X_PROFILE_HYDRATION_BLOCKED"
    elif projected_limitations or any(status != "matched" for status in lookup_statuses):
        projected["status"] = "X_PROFILE_HYDRATION_PARTIAL"
    else:
        projected["status"] = "X_PROFILE_HYDRATION_OK"
    projected_errors = validate_profile_hydration_result(projected)
    if projected_errors:
        raise ProfileHydrationContractError(";".join(projected_errors))
    return ProfileHydrationOperatorProjection(
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
        removed_model_record_repair_count=removed_record_repairs,
        added_operator_record_repair_count=added_record_repairs,
    )


def project_profile_execution_limitations(
    result: Mapping[str, Any],
    *,
    receipt: ProfileHydrationExecutionReceipt,
    session_precommit: GrokOperatorSessionPrecommit,
    operator_execution_facts: GrokOperatorExecutionFacts,
    raw_session_files: Mapping[str, bytes] | None = None,
) -> ProfileHydrationOperatorProjection:
    """Replay exact raw sources before accepting a supplied receipt."""

    if raw_session_files is None:
        raise ProfileHydrationContractError("operator_raw_session_required")
    try:
        replay = replay_grok_operator_session(
            raw_session_files,
            session_precommit=session_precommit,
            allowed_tool_names=frozenset({"x_user_search"}),
            expected_terminal=result,
            terminal_validator=lambda value: _validate_profile_hydration_result(
                value,
                enforce_status_coherence=False,
                enforce_repair_ownership=False,
            ),
        )
    except GrokOperatorSessionReplayError as exc:
        raise ProfileHydrationContractError(f"operator_raw_session_invalid:{exc}") from exc
    expected_receipt = _receipt_from_replay(
        result,
        replay,
        operator_execution_facts=operator_execution_facts,
    )
    if receipt != expected_receipt:
        raise ProfileHydrationContractError("operator_receipt_replay_mismatch")
    return _project_profile_execution_limitations(
        result,
        receipt=receipt,
        session_precommit=session_precommit,
        operator_execution_facts=operator_execution_facts,
        raw_session_artifacts=replay.source_artifacts,
    )


def build_profile_hydration_operator_projection(
    raw_session_files: Mapping[str, bytes],
    *,
    session_precommit: GrokOperatorSessionPrecommit,
    result_truncated: bool = False,
    execution_deadline_reached: bool = False,
    transport_failure: bool = False,
    model_output_repaired: bool = False,
) -> ProfileHydrationOperatorProjection:
    """Operator-owned builder deriving a profile projection from raw bytes."""

    for value in (
        result_truncated,
        execution_deadline_reached,
        transport_failure,
        model_output_repaired,
    ):
        if type(value) is not bool:
            raise ProfileHydrationContractError("operator_execution_fact_invalid")
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
            allowed_tool_names=frozenset({"x_user_search"}),
            terminal_validator=lambda value: _validate_profile_hydration_result(
                value,
                enforce_status_coherence=False,
                enforce_repair_ownership=False,
            ),
        )
    except GrokOperatorSessionReplayError as exc:
        raise ProfileHydrationContractError(f"operator_raw_session_invalid:{exc}") from exc
    result = replay.terminal
    receipt = _receipt_from_replay(
        result,
        replay,
        operator_execution_facts=operator_execution_facts,
    )
    return _project_profile_execution_limitations(
        result,
        receipt=receipt,
        session_precommit=session_precommit,
        operator_execution_facts=operator_execution_facts,
        raw_session_artifacts=replay.source_artifacts,
    )


def _assert_projection(projection: Any) -> ProfileHydrationOperatorProjection:
    if not isinstance(projection, ProfileHydrationOperatorProjection):
        raise ProfileHydrationContractError("hydration_projection_required")
    if not isinstance(projection.result, dict) or not isinstance(projection.raw_terminal, dict):
        raise ProfileHydrationContractError("hydration_projection_result_type_invalid")
    if not isinstance(projection.receipt, ProfileHydrationExecutionReceipt):
        raise ProfileHydrationContractError("hydration_projection_receipt_type_invalid")
    if not isinstance(projection.session_precommit, GrokOperatorSessionPrecommit):
        raise ProfileHydrationContractError(
            "hydration_projection_session_precommit_type_invalid"
        )
    if not isinstance(projection.operator_execution_facts, GrokOperatorExecutionFacts):
        raise ProfileHydrationContractError(
            "hydration_projection_execution_facts_type_invalid"
        )
    errors = validate_profile_hydration_result(projection.result)
    if errors:
        raise ProfileHydrationContractError(";".join(errors))
    try:
        raw_session_files = thaw_raw_session_artifacts(projection.raw_session_artifacts)
        recomputed = project_profile_execution_limitations(
            projection.raw_terminal,
            receipt=projection.receipt,
            session_precommit=projection.session_precommit,
            operator_execution_facts=projection.operator_execution_facts,
            raw_session_files=raw_session_files,
        )
    except (ProfileHydrationContractError, GrokOperatorSessionReplayError) as exc:
        raise ProfileHydrationContractError(f"hydration_projection_revalidation_failed:{exc}") from exc
    if recomputed != projection:
        if projection.receipt_sha256 != recomputed.receipt_sha256:
            raise ProfileHydrationContractError("hydration_projection_receipt_digest_mismatch")
        if projection.terminal_sha256 != recomputed.terminal_sha256:
            raise ProfileHydrationContractError("hydration_projection_terminal_digest_mismatch")
        if projection.projected_result_sha256 != recomputed.projected_result_sha256:
            raise ProfileHydrationContractError("hydration_projection_result_digest_mismatch")
        raise ProfileHydrationContractError("hydration_projection_content_mismatch")
    return projection


def _field_coverage(records: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int | float]]:
    matched_records = [record for record in records if record.get("lookup_status") == "matched"]
    denominator = len(matched_records)
    coverage: dict[str, dict[str, int | float]] = {}
    for field in HYDRATION_FIELDS:
        observed = sum(record.get(field) is not None for record in matched_records)
        populated = sum(
            record.get(field) is not None
            and (not isinstance(record.get(field), list) or bool(record.get(field)))
            for record in matched_records
        )
        coverage[field] = {
            "observed_count": observed,
            "populated_count": populated,
            "matched_record_coverage_rate": round(observed / denominator, 6) if denominator else 0.0,
        }
    return coverage


_CANDIDATE_FREE_PROJECTION_ERROR_CODES = (
    "hydration_projection_required",
    "hydration_projection_result_type_invalid",
    "hydration_projection_receipt_type_invalid",
    "hydration_projection_session_precommit_type_invalid",
    "hydration_projection_execution_facts_type_invalid",
    "hydration_projection_receipt_digest_mismatch",
    "hydration_projection_terminal_digest_mismatch",
    "hydration_projection_result_digest_mismatch",
    "hydration_projection_content_mismatch",
    "hydration_projection_revalidation_failed",
)


def _candidate_free_projection_error(exc: Exception) -> str:
    """Map internal validation details to a closed candidate-free error code."""

    message = str(exc)
    for code in _CANDIDATE_FREE_PROJECTION_ERROR_CODES:
        if message == code or message.startswith(f"{code}:"):
            return code
    return "hydration_projection_invalid"


def evaluate_profile_hydration_batch(
    projection: ProfileHydrationOperatorProjection,
    expectation: ProfileHydrationBatchExpectation,
) -> dict[str, Any]:
    """Reconcile one projected result with its completed native-call receipt.

    Raw result mappings and caller-supplied call-shaped dictionaries are not
    accepted.  The returned object contains candidate-free counts and errors.
    """

    errors: list[str] = []
    checked: ProfileHydrationOperatorProjection | None = None
    try:
        checked = _assert_projection(projection)
        result: Mapping[str, Any] = checked.result
        completions = checked.receipt.tool_completions
    except (ProfileHydrationContractError, TypeError, AttributeError, ValueError) as exc:
        result = {}
        completions = ()
        errors.append(_candidate_free_projection_error(exc))

    expected_values: list[ProfileHydrationIdentityExpectation]
    expected_by_handle: dict[str, ProfileHydrationIdentityExpectation] = {}
    if isinstance(expectation, ProfileHydrationBatchExpectation):
        if not isinstance(expectation.input_identities, tuple):
            expected_values = []
            errors.append("expectation_identities_type_invalid")
        else:
            expected_values = [
                item
                for item in expectation.input_identities
                if isinstance(item, ProfileHydrationIdentityExpectation)
            ]
            if len(expected_values) != len(expectation.input_identities):
                errors.append("expectation_identity_type_invalid")
        binding_fields = (
            "campaign_id",
            "target_descriptor_id",
            "target_descriptor_sha256",
            "prompt_policy_sha256",
            "discovery_union_sha256",
            "run_id",
            "batch_id",
        )
        for field in binding_fields:
            expected_value = getattr(expectation, field)
            if field in {"campaign_id", "target_descriptor_id", "run_id", "batch_id"}:
                valid = _valid_identifier(expected_value)
            else:
                valid = _valid_sha256(expected_value)
            if not valid:
                errors.append(f"expectation_{field}_invalid")
            if result.get(field) != expected_value:
                errors.append(f"result_expectation_{field}_mismatch")
            if checked is not None and getattr(checked.receipt, field) != expected_value:
                errors.append(f"receipt_expectation_{field}_mismatch")
    else:
        expected_values = []
        errors.append("hydration_expectation_required")
    expected_counts: Counter[str] = Counter()
    lead_identity_counts: Counter[str] = Counter()
    for index, identity in enumerate(expected_values):
        identity_errors = _identity_expectation_errors(identity)
        if identity_errors:
            errors.extend(f"expected_identity:{index}:{error}" for error in identity_errors)
            continue
        handle_key = identity.lookup_handle.casefold()
        expected_counts[handle_key] += 1
        lead_identity_counts[identity.lead_identity] += 1
        expected_by_handle.setdefault(handle_key, identity)
    expected_keys = set(expected_counts)
    expected_duplicate_count = sum(count - 1 for count in expected_counts.values() if count > 1)
    expected_lead_identity_duplicate_count = sum(
        count - 1 for count in lead_identity_counts.values() if count > 1
    )
    if not expected_keys:
        errors.append("expected_identities_empty")
    if expected_duplicate_count:
        errors.append("expected_lookup_handles_casefold_duplicate")
    if expected_lead_identity_duplicate_count:
        errors.append("expected_lead_identity_duplicate")
    if expected_keys and not expected_duplicate_count and not expected_lead_identity_duplicate_count:
        try:
            expected_digest = profile_identity_input_set_sha256(expected_values)
        except ProfileHydrationContractError:
            expected_digest = None
            errors.append("expected_identities_invalid")
        if expected_digest is not None and result.get("input_set_sha256") != expected_digest:
            errors.append("result_input_set_digest_mismatch")
        if (
            expected_digest is not None
            and checked is not None
            and checked.receipt.input_set_sha256 != expected_digest
        ):
            errors.append("receipt_input_set_digest_mismatch")

    raw_records = result.get("records", []) if isinstance(result, Mapping) else []
    records = [record for record in raw_records if isinstance(record, dict)] if isinstance(raw_records, list) else []
    record_counts: Counter[str] = Counter(
        record["input_handle"].casefold() for record in records if _valid_handle(record.get("input_handle"))
    )
    record_duplicate_count = sum(count - 1 for count in record_counts.values() if count > 1)
    missing_record_count = len(expected_keys - set(record_counts))
    extra_record_count = sum(count for handle, count in record_counts.items() if handle not in expected_keys)
    if record_duplicate_count:
        errors.append("records_input_duplicate")
    if missing_record_count:
        errors.append("records_input_missing")
    if extra_record_count:
        errors.append("records_input_extra")
    if any(record_counts.get(handle, 0) != 1 for handle in expected_keys):
        errors.append("records_expected_bijection_invalid")

    query_counts: Counter[str] = Counter()
    valid_user_search_count = 0
    unexpected_tool_count = 0
    malformed_call_count = 0
    extra_query_count = 0
    for call_index, completion in enumerate(completions):
        if not isinstance(completion, ProfileHydrationToolCompletion):
            errors.append(f"tool_call:{call_index}:shape_invalid")
            malformed_call_count += 1
            continue
        if completion.tool_name != "x_user_search":
            errors.append(f"tool_call:{call_index}:unexpected_tool")
            unexpected_tool_count += 1
            continue
        query = completion.query
        if not _valid_handle(query):
            errors.append(f"tool_call:{call_index}:query_not_bare_handle")
            malformed_call_count += 1
            continue
        valid_user_search_count += 1
        query_key = query.casefold()
        query_counts[query_key] += 1
        if query_key not in expected_keys:
            extra_query_count += 1
    duplicate_query_count = sum(
        count - 1 for handle, count in query_counts.items() if handle in expected_keys and count > 1
    )
    missing_query_count = len(expected_keys - set(query_counts))
    matched_query_count = sum(query_counts.get(handle, 0) == 1 for handle in expected_keys)
    if unexpected_tool_count:
        errors.append("native_tool_family_invalid")
    if malformed_call_count:
        errors.append("native_tool_call_shape_invalid")
    if duplicate_query_count:
        errors.append("native_tool_query_duplicate")
    if missing_query_count:
        errors.append("native_tool_query_missing")
    if extra_query_count:
        errors.append("native_tool_query_extra")
    if len(completions) != len(expected_keys):
        errors.append("native_tool_call_count_mismatch")

    lookup_counts = Counter(
        record.get("lookup_status") for record in records if record.get("lookup_status") in LOOKUP_STATUSES
    )
    affiliation_temporal_counts: Counter[str] = Counter()
    affiliation_source_counts: Counter[str] = Counter()
    verification_counts: Counter[str] = Counter()
    stable_platform_id_missing_count = 0
    platform_id_mismatch_quarantine_count = 0
    for record in records:
        input_handle = record.get("input_handle")
        expectation_row = (
            expected_by_handle.get(input_handle.casefold())
            if _valid_handle(input_handle)
            else None
        )
        if (
            expectation_row is not None
            and expectation_row.identity_state == "stable_platform_id"
            and record.get("lookup_status") == "matched"
        ):
            returned_platform_id = record.get("platform_user_id")
            if returned_platform_id is None:
                errors.append("stable_identity_platform_user_id_missing")
                stable_platform_id_missing_count += 1
            elif returned_platform_id != expectation_row.expected_platform_user_id:
                errors.append("stable_identity_platform_user_id_mismatch")
                platform_id_mismatch_quarantine_count += 1
        affiliations = record.get("affiliations")
        if isinstance(affiliations, list):
            for affiliation in affiliations:
                if isinstance(affiliation, Mapping):
                    temporal_state = affiliation.get("temporal_state")
                    evidence_source = affiliation.get("evidence_source")
                    if temporal_state in TEMPORAL_STATES:
                        affiliation_temporal_counts[temporal_state] += 1
                    if evidence_source in AFFILIATION_EVIDENCE_SOURCES:
                        affiliation_source_counts[evidence_source] += 1
        verification = record.get("verification")
        if isinstance(verification, Mapping):
            value = verification.get("account_verified")
            state = "verified" if value is True else "not_verified" if value is False else "unknown"
            verification_counts[state] += 1

    unique_errors = list(dict.fromkeys(errors))
    return {
        "status": "invalid" if unique_errors else "valid",
        "errors": unique_errors,
        "aggregate": {
            "input_record_reconciliation": {
                "requested_input_count": len(expected_values),
                "expected_input_count": len(expected_keys),
                "result_record_count": len(raw_records) if isinstance(raw_records, list) else 0,
                "expected_duplicate_count": expected_duplicate_count,
                "record_duplicate_count": record_duplicate_count,
                "missing_record_count": missing_record_count,
                "extra_record_count": extra_record_count,
                "stable_platform_id_missing_count": stable_platform_id_missing_count,
                "platform_id_mismatch_quarantine_count": platform_id_mismatch_quarantine_count,
            },
            "call_reconciliation": {
                "native_tool_call_count": len(completions),
                "valid_x_user_search_count": valid_user_search_count,
                "matched_query_count": matched_query_count,
                "duplicate_query_count": duplicate_query_count,
                "missing_query_count": missing_query_count,
                "extra_query_count": extra_query_count,
                "unexpected_tool_count": unexpected_tool_count,
                "malformed_call_count": malformed_call_count,
            },
            "lookup_status_counts": {status: lookup_counts.get(status, 0) for status in LOOKUP_STATUSES},
            "field_coverage": _field_coverage(records),
            "affiliation_temporal_state_counts": {
                state: affiliation_temporal_counts.get(state, 0) for state in TEMPORAL_STATES
            },
            "affiliation_evidence_source_counts": {
                source: affiliation_source_counts.get(source, 0)
                for source in AFFILIATION_EVIDENCE_SOURCES
            },
            "verification_counts": {
                state: verification_counts.get(state, 0)
                for state in ("verified", "not_verified", "unknown")
            },
        },
    }
