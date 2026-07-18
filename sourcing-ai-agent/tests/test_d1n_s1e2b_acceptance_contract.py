from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest

from sourcing_agent.acquisition_start_command_acceptance import (
    ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION,
    AcquisitionStartCommandAcceptanceError,
    AcquisitionStartCommandAcceptanceEvent,
    AcquisitionStartCommandAcceptanceOwnerRef,
)


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _owner_ref_record() -> dict[str, object]:
    return {
        "schema_version": ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_SCHEMA_VERSION,
        "runtime_namespace": "runtime:test",
        "provider_mode": "scripted",
        "workspace_id": "workspace-1",
        "action_id": "action-1",
        "operation_run_id": "operation-1",
        "workflow_run_id": "workflow-1",
        "workflow_command_id": "command-1",
        "terminal_winner_id": "event-1",
        "terminal_winner_sequence_number": 1,
        "command_source_event_id": "event-2",
        "command_source_event_sequence_number": 2,
        "command_source_event_contract_digest": "1" * 64,
        "confirmation_receipt_ref": {
            "receipt_id": "receipt-1",
            "receipt_digest": "2" * 64,
        },
        "parent_budget_envelope_ref": {"schema_version": "test-budget-ref.v1"},
        "start_snapshot_digest": "3" * 64,
        "root_command_payload_digest": "4" * 64,
        "result_occurrence_ref": {
            "result_slot_id": "slot-1",
            "slot_generation": 1,
            "logical_occurrence_digest": "5" * 64,
        },
    }


@pytest.mark.parametrize(
    ("field", "alias"),
    (
        ("terminal_winner_sequence_number", True),
        ("terminal_winner_sequence_number", 1.0),
        ("terminal_winner_sequence_number", "1"),
        ("command_source_event_sequence_number", True),
        ("command_source_event_sequence_number", 2.0),
        ("command_source_event_sequence_number", "2"),
    ),
)
def test_owner_ref_rejects_non_integer_sequence_aliases(field: str, alias: object) -> None:
    owner_ref = _owner_ref_record()
    owner_ref[field] = alias

    with pytest.raises(AcquisitionStartCommandAcceptanceError, match="topology invalid"):
        AcquisitionStartCommandAcceptanceOwnerRef(owner_ref)


@pytest.mark.parametrize(
    ("field", "alias"),
    (
        ("terminal_winner_sequence_number", True),
        ("terminal_winner_sequence_number", 1.0),
        ("terminal_winner_sequence_number", "1"),
        ("command_source_event_sequence_number", True),
        ("command_source_event_sequence_number", 2.0),
        ("command_source_event_sequence_number", "2"),
    ),
)
def test_recomputed_alias_digest_does_not_make_event_acceptable(field: str, alias: object) -> None:
    owner_ref = _owner_ref_record()
    owner_ref[field] = alias
    payload = {
        "owner_result_ref": owner_ref,
        "owner_result_digest": _canonical_digest(owner_ref),
    }

    with pytest.raises(AcquisitionStartCommandAcceptanceError, match="topology invalid"):
        AcquisitionStartCommandAcceptanceEvent.from_payload(payload)


def test_exact_integer_sequences_preserve_digest_and_round_trip_semantics() -> None:
    owner_ref_record = _owner_ref_record()
    owner_ref = AcquisitionStartCommandAcceptanceOwnerRef(owner_ref_record)
    owner_digest = _canonical_digest(owner_ref_record)

    assert owner_ref.digest == owner_digest
    assert owner_ref.to_record() == owner_ref_record

    event = AcquisitionStartCommandAcceptanceEvent.from_payload(
        {
            "owner_result_ref": deepcopy(owner_ref_record),
            "owner_result_digest": owner_digest,
        }
    )

    assert event.to_payload() == {
        "owner_result_ref": owner_ref_record,
        "owner_result_digest": owner_digest,
    }
