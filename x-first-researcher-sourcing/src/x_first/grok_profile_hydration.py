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


class ProfileHydrationContractError(ValueError):
    """Raised when profile-hydration evidence cannot preserve the contract."""


@dataclass(frozen=True)
class ProfileHydrationToolCompletion:
    """One start/completion-paired native call from the session ledger."""

    call_id: str
    tool_name: str
    query: str
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
    transcript_sha256: str
    terminal_sha256: str
    terminal_ledger_sequence: int
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
    input_handles: tuple[str, ...]


@dataclass(frozen=True)
class ProfileHydrationOperatorProjection:
    """Receipt-bound normalized result eligible for batch evaluation."""

    raw_terminal: dict[str, Any]
    result: dict[str, Any]
    receipt: ProfileHydrationExecutionReceipt
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


def profile_input_set_sha256(handles: Sequence[str]) -> str:
    """Hash a case-insensitive, order-independent hydration input set."""

    if isinstance(handles, (str, bytes)):
        raise ProfileHydrationContractError("expected_handles_invalid")
    normalized = [handle.casefold() for handle in handles if _valid_handle(handle)]
    if len(normalized) != len(handles) or not normalized or len(normalized) != len(set(normalized)):
        raise ProfileHydrationContractError("expected_handles_invalid")
    return canonical_json_sha256(sorted(normalized))


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
    if receipt.receipt_version != "x.grok.profile_hydration.execution_receipt.v1":
        errors.append("operator_receipt_version_invalid")
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
    if not _valid_sha256(receipt.transcript_sha256):
        errors.append("operator_receipt_transcript_sha256_invalid")
    if not _valid_sha256(receipt.terminal_sha256):
        errors.append("operator_receipt_terminal_sha256_invalid")
    elif receipt.terminal_sha256 != canonical_json_sha256(result):
        errors.append("operator_receipt_terminal_sha256_mismatch")
    terminal_sequence_valid = (
        type(receipt.terminal_ledger_sequence) is int
        and receipt.terminal_ledger_sequence > 0
    )
    if not terminal_sequence_valid:
        errors.append("operator_receipt_terminal_sequence_invalid")
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
    ledger_sequences: set[int] = set()
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
        if completion.completion_status != "completed":
            errors.append(f"{prefix}:completion_status_invalid")
        if completion.tool_name != "x_user_search":
            errors.append(f"{prefix}:unexpected_tool")
        if not _valid_handle(completion.query):
            errors.append(f"{prefix}:query_not_bare_handle")
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
            and start > 0
            and end > start
        )
        if not sequences_valid:
            errors.append(f"{prefix}:lifecycle_sequence_invalid")
        elif terminal_sequence_valid and end >= receipt.terminal_ledger_sequence:
            errors.append(f"{prefix}:terminal_order_invalid")
        if sequences_valid:
            if start in ledger_sequences or end in ledger_sequences:
                errors.append(f"{prefix}:ledger_sequence_duplicate")
            ledger_sequences.update((start, end))
        if type(start) is int and start < previous_start:
            errors.append(f"{prefix}:ledger_order_invalid")
        if type(start) is int:
            previous_start = start
    return list(dict.fromkeys(errors))


def project_profile_execution_limitations(
    result: Mapping[str, Any],
    *,
    receipt: ProfileHydrationExecutionReceipt,
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
    if receipt_errors:
        raise ProfileHydrationContractError(";".join(receipt_errors))

    operator_facts = {
        "execution_deadline_reached": receipt.execution_deadline_reached,
        "transport_failure": receipt.transport_failure,
        "result_truncated": receipt.result_truncated,
        "model_output_repaired": receipt.model_output_repaired,
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
        if receipt.model_output_repaired:
            record_limitations.append("model_output_repaired")
            added_record_repairs += 1
        record["limitations"] = [
            code for code in RECORD_LIMITATIONS if code in set(record_limitations)
        ]

    lookup_statuses = [record["lookup_status"] for record in projected["records"]]
    hard_execution_failure = receipt.execution_deadline_reached or receipt.transport_failure
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
        receipt_sha256=canonical_json_sha256(asdict(receipt)),
        terminal_sha256=receipt.terminal_sha256,
        projected_result_sha256=canonical_json_sha256(projected),
        removed_model_operator_limitations=removed,
        added_operator_limitations=added,
        removed_model_record_repair_count=removed_record_repairs,
        added_operator_record_repair_count=added_record_repairs,
    )


def _assert_projection(projection: Any) -> ProfileHydrationOperatorProjection:
    if not isinstance(projection, ProfileHydrationOperatorProjection):
        raise ProfileHydrationContractError("hydration_projection_required")
    errors = validate_profile_hydration_result(projection.result)
    if errors:
        raise ProfileHydrationContractError(";".join(errors))
    try:
        recomputed = project_profile_execution_limitations(
            projection.raw_terminal,
            receipt=projection.receipt,
        )
    except ProfileHydrationContractError as exc:
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


def evaluate_profile_hydration_batch(
    projection: ProfileHydrationOperatorProjection,
    expectation: ProfileHydrationBatchExpectation,
) -> dict[str, Any]:
    """Reconcile one projected result with its completed native-call receipt.

    Raw result mappings and caller-supplied call-shaped dictionaries are not
    accepted.  The returned object contains candidate-free counts and errors.
    """

    errors: list[str] = []
    try:
        checked = _assert_projection(projection)
        result: Mapping[str, Any] = checked.result
        completions = checked.receipt.tool_completions
    except ProfileHydrationContractError as exc:
        result = projection.result if isinstance(projection, ProfileHydrationOperatorProjection) else {}
        completions = ()
        errors.extend(str(exc).split(";"))

    expected_values: list[Any]
    if isinstance(expectation, ProfileHydrationBatchExpectation):
        expected_values = list(expectation.input_handles)
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
            if isinstance(projection, ProfileHydrationOperatorProjection) and (
                getattr(projection.receipt, field) != expected_value
            ):
                errors.append(f"receipt_expectation_{field}_mismatch")
    else:
        expected_values = []
        errors.append("hydration_expectation_required")
    expected_counts: Counter[str] = Counter()
    for index, handle in enumerate(expected_values):
        if not _valid_handle(handle):
            errors.append(f"expected_handle:{index}:invalid")
            continue
        expected_counts[handle.casefold()] += 1
    expected_keys = set(expected_counts)
    expected_duplicate_count = sum(count - 1 for count in expected_counts.values() if count > 1)
    if not expected_keys:
        errors.append("expected_handles_empty")
    if expected_duplicate_count:
        errors.append("expected_handles_casefold_duplicate")
    if expected_keys and not expected_duplicate_count:
        expected_digest = canonical_json_sha256(sorted(expected_keys))
        if result.get("input_set_sha256") != expected_digest:
            errors.append("result_input_set_digest_mismatch")
        if isinstance(projection, ProfileHydrationOperatorProjection) and (
            projection.receipt.input_set_sha256 != expected_digest
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
    for record in records:
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
