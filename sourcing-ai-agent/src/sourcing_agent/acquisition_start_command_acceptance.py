"""Closed identity contract for acquisition-start command acceptance.

The command-acceptance Operation event, Action/Operation result references, and
Agent terminal owner all share this exact value.  Keeping its structural rules
in one pure module prevents readers from re-deriving a weaker variant.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION = "acquisition_start_command_acceptance.v1"
ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION = (
    "acquisition_start_command_acceptance_owner_result_ref.v1"
)
ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS = (
    "schema_version",
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "action_id",
    "operation_run_id",
    "workflow_run_id",
    "workflow_command_id",
    "terminal_winner_id",
    "terminal_winner_sequence_number",
    "command_source_event_id",
    "command_source_event_sequence_number",
    "command_source_event_contract_digest",
    "confirmation_receipt_ref",
    "parent_budget_envelope_ref",
    "start_snapshot_digest",
    "root_command_payload_digest",
    "result_occurrence_ref",
)


class AcquisitionStartCommandAcceptanceError(ValueError):
    pass


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AcquisitionStartCommandAcceptanceError("acquisition start command acceptance JSON invalid") from exc


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _identity_text(value: object, *, field: str) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 1024
        or any(character in "\r\n\x00" for character in value)
    ):
        raise AcquisitionStartCommandAcceptanceError(f"acquisition start command acceptance {field} invalid")
    return value


def _sha256(value: object, *, field: str) -> str:
    normalized = _identity_text(value, field=field)
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise AcquisitionStartCommandAcceptanceError(f"acquisition start command acceptance {field} invalid")
    return normalized


def _closed_mapping(value: object, *, fields: set[str], field: str) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise AcquisitionStartCommandAcceptanceError(f"acquisition start command acceptance {field} invalid")
    return dict(value)


@dataclass(frozen=True, slots=True)
class AcquisitionStartCommandAcceptanceOwnerRef:
    """One structurally exact 18-field command-acceptance owner reference."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        if type(self._record) is not dict or set(self._record) != set(
            ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS
        ):
            raise AcquisitionStartCommandAcceptanceError("acquisition start command acceptance owner ref invalid")
        record = {field: self._record[field] for field in ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS}
        for field in (
            "schema_version",
            "runtime_namespace",
            "provider_mode",
            "workspace_id",
            "action_id",
            "operation_run_id",
            "workflow_run_id",
            "workflow_command_id",
            "terminal_winner_id",
            "command_source_event_id",
        ):
            _identity_text(record.get(field), field=field)
        if record["schema_version"] != ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION:
            raise AcquisitionStartCommandAcceptanceError("acquisition start command acceptance schema version invalid")
        if record.get("terminal_winner_sequence_number") != 1 or record.get(
            "command_source_event_sequence_number"
        ) != 2:
            raise AcquisitionStartCommandAcceptanceError("acquisition start command acceptance topology invalid")
        for field in (
            "command_source_event_contract_digest",
            "start_snapshot_digest",
            "root_command_payload_digest",
        ):
            _sha256(record.get(field), field=field)
        receipt_ref = _closed_mapping(
            record.get("confirmation_receipt_ref"),
            fields={"receipt_id", "receipt_digest"},
            field="confirmation_receipt_ref",
        )
        _identity_text(receipt_ref["receipt_id"], field="confirmation_receipt_ref.receipt_id")
        _sha256(receipt_ref["receipt_digest"], field="confirmation_receipt_ref.receipt_digest")
        occurrence_ref = _closed_mapping(
            record.get("result_occurrence_ref"),
            fields={"result_slot_id", "slot_generation", "logical_occurrence_digest"},
            field="result_occurrence_ref",
        )
        _identity_text(occurrence_ref["result_slot_id"], field="result_occurrence_ref.result_slot_id")
        if type(occurrence_ref["slot_generation"]) is not int or occurrence_ref["slot_generation"] <= 0:
            raise AcquisitionStartCommandAcceptanceError(
                "acquisition start command acceptance result_occurrence_ref.slot_generation invalid"
            )
        _sha256(
            occurrence_ref["logical_occurrence_digest"],
            field="result_occurrence_ref.logical_occurrence_digest",
        )
        # The detailed budget-owner value object validates this nested record at
        # both producer and physical-reader boundaries.  This contract still
        # requires a plain nonempty mapping so aliases cannot erase the field.
        if type(record.get("parent_budget_envelope_ref")) is not dict or not record["parent_budget_envelope_ref"]:
            raise AcquisitionStartCommandAcceptanceError(
                "acquisition start command acceptance parent_budget_envelope_ref invalid"
            )
        object.__setattr__(self, "_record", record)

    @property
    def digest(self) -> str:
        return _sha256_json(self._record)

    def to_record(self) -> dict[str, Any]:
        return deepcopy(dict(self._record))


@dataclass(frozen=True, slots=True)
class AcquisitionStartCommandAcceptanceEvent:
    owner_result_ref: AcquisitionStartCommandAcceptanceOwnerRef
    owner_result_digest: str

    def __post_init__(self) -> None:
        if type(self.owner_result_ref) is not AcquisitionStartCommandAcceptanceOwnerRef:
            raise AcquisitionStartCommandAcceptanceError("acquisition start command acceptance owner ref invalid")
        normalized_digest = _sha256(self.owner_result_digest, field="owner_result_digest")
        if normalized_digest != self.owner_result_ref.digest:
            raise AcquisitionStartCommandAcceptanceError("acquisition start command acceptance digest mismatch")

    @classmethod
    def from_payload(cls, payload: object) -> AcquisitionStartCommandAcceptanceEvent:
        record = _closed_mapping(
            payload,
            fields={"owner_result_ref", "owner_result_digest"},
            field="event payload",
        )
        return cls(
            owner_result_ref=AcquisitionStartCommandAcceptanceOwnerRef(record["owner_result_ref"]),
            owner_result_digest=_sha256(record["owner_result_digest"], field="owner_result_digest"),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "owner_result_ref": self.owner_result_ref.to_record(),
            "owner_result_digest": self.owner_result_digest,
        }


__all__ = [
    "ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS",
    "ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION",
    "ACQUISITION_START_COMMAND_ACCEPTANCE_SCHEMA_VERSION",
    "AcquisitionStartCommandAcceptanceError",
    "AcquisitionStartCommandAcceptanceEvent",
    "AcquisitionStartCommandAcceptanceOwnerRef",
]
