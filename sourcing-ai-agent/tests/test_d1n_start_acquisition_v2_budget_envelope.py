from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from types import MappingProxyType
from typing import Any

import pytest

from sourcing_agent.acquisition_start_v2 import (
    ACQUISITION_PARENT_BUDGET_FIELDS,
    ACQUISITION_START_V2_PARENT_BUDGET_INVALID,
    AcquisitionConfirmationReceipt,
    AcquisitionParentBudgetEnvelopeRef,
    AcquisitionStartV2Error,
    build_acquisition_parent_budget_envelope_ref,
)
from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_registry import AgentToolOwnerPin
from tests.test_d1n_start_acquisition_v2 import _bind, _receipt

_ENVELOPE_REF_FIELDS = (
    "owner_id",
    "owner_revision",
    "owner_contract_digest",
    "confirmation_receipt_id",
    "confirmation_receipt_digest",
    "budget_digest",
)


def _digest(value: Any) -> str:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _receipt_and_owner() -> tuple[AcquisitionConfirmationReceipt, AgentToolOwnerPin]:
    repository, bound = _bind()
    receipt = _receipt(repository, bound)
    owner = START_ACQUISITION_RUN_TOOL_SPEC.budget.budget_owner
    assert type(owner) is AgentToolOwnerPin
    return receipt, owner


def _rehashed_receipt_with_budget(budget: Any) -> AcquisitionConfirmationReceipt:
    receipt, _ = _receipt_and_owner()
    record = receipt.to_record()
    record["budget"] = budget
    record["receipt_digest"] = _digest({key: value for key, value in record.items() if key != "receipt_digest"})
    return AcquisitionConfirmationReceipt(record)


def test_parent_budget_envelope_ref_exact_copies_registered_owner_and_receipt() -> None:
    receipt, owner = _receipt_and_owner()
    receipt_record = receipt.to_record()
    canonical_budget = {
        field: receipt_record["budget"][field] for field in ACQUISITION_PARENT_BUDGET_FIELDS
    }

    envelope = build_acquisition_parent_budget_envelope_ref(receipt, owner)
    replay = build_acquisition_parent_budget_envelope_ref(receipt, owner)
    record = envelope.to_record()

    assert isinstance(envelope, AcquisitionParentBudgetEnvelopeRef)
    assert tuple(ACQUISITION_PARENT_BUDGET_FIELDS) == (
        "max_provider_calls",
        "max_provider_items",
        "max_output_candidates",
        "max_cost_micro_usd",
        "max_elapsed_seconds",
    )
    assert tuple(canonical_budget) == ACQUISITION_PARENT_BUDGET_FIELDS
    assert tuple(record) == _ENVELOPE_REF_FIELDS
    assert record == {
        **owner.to_fingerprint_record(),
        "confirmation_receipt_id": receipt.receipt_id,
        "confirmation_receipt_digest": receipt.receipt_digest,
        "budget_digest": _digest(canonical_budget),
    }
    assert replay.to_record() == record
    assert receipt.to_record() == receipt_record
    assert (
        envelope.owner_id,
        envelope.owner_revision,
        envelope.owner_contract_digest,
        envelope.confirmation_receipt_id,
        envelope.confirmation_receipt_digest,
        envelope.budget_digest,
    ) == tuple(record[field] for field in _ENVELOPE_REF_FIELDS)


def test_parent_budget_envelope_ref_is_deeply_immutable_and_defensive() -> None:
    receipt, owner = _receipt_and_owner()
    envelope = build_acquisition_parent_budget_envelope_ref(receipt, owner)
    assert isinstance(envelope._record, MappingProxyType)

    with pytest.raises(TypeError):
        envelope._record["budget_digest"] = "f" * 64  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        envelope._record = {}  # type: ignore[misc]

    exported = envelope.to_record()
    exported["budget_digest"] = "f" * 64
    assert envelope.budget_digest != exported["budget_digest"]


def test_builder_requires_typed_receipt_and_accepts_reconstructed_current_owner() -> None:
    receipt, owner = _receipt_and_owner()

    exact_reconstructed_owner = AgentToolOwnerPin(**owner.to_fingerprint_record())
    assert build_acquisition_parent_budget_envelope_ref(
        receipt,
        exact_reconstructed_owner,
    ).to_record() == build_acquisition_parent_budget_envelope_ref(receipt, owner).to_record()

    for raw_receipt, raw_owner in (
        (receipt.to_record(), owner),
        (receipt, owner.to_fingerprint_record()),
    ):
        with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_PARENT_BUDGET_INVALID):
            build_acquisition_parent_budget_envelope_ref(  # type: ignore[arg-type]
                raw_receipt,
                raw_owner,
            )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("owner_id", "acquisition.parent_budget_reservation.rotated"),
        ("owner_revision", "acquisition_parent_budget_v2"),
        ("owner_contract_digest", "f" * 64),
    ],
)
def test_builder_rejects_each_noncurrent_registered_owner_pin(field: str, replacement: str) -> None:
    receipt, owner = _receipt_and_owner()
    stale_record = owner.to_fingerprint_record()
    stale_record[field] = replacement
    stale_owner = AgentToolOwnerPin(**stale_record)

    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_PARENT_BUDGET_INVALID):
        build_acquisition_parent_budget_envelope_ref(receipt, stale_owner)


@pytest.mark.parametrize("tampered_field", ["budget", "receipt_digest"])
def test_builder_revalidates_receipt_digest_after_in_memory_tamper(tampered_field: str) -> None:
    receipt, owner = _receipt_and_owner()
    tampered_record = receipt.to_record()
    if tampered_field == "budget":
        tampered_record["budget"]["max_provider_calls"] += 1
    else:
        tampered_record["receipt_digest"] = "f" * 64
    object.__setattr__(receipt, "_record", tampered_record)

    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_PARENT_BUDGET_INVALID):
        build_acquisition_parent_budget_envelope_ref(receipt, owner)


@pytest.mark.parametrize(
    "budget",
    [
        {"max_provider_calls": 4},
        {
            "max_provider_calls": True,
            "max_provider_items": 20,
            "max_output_candidates": 10,
            "max_cost_micro_usd": 2_000_000,
            "max_elapsed_seconds": 900,
        },
        {
            "max_provider_calls": 4,
            "max_provider_items": 20,
            "max_output_candidates": 21,
            "max_cost_micro_usd": 2_000_000,
            "max_elapsed_seconds": 900,
        },
    ],
)
def test_builder_rejects_self_consistent_historical_budget_shape_drift(budget: dict[str, Any]) -> None:
    receipt = _rehashed_receipt_with_budget(budget)
    owner = START_ACQUISITION_RUN_TOOL_SPEC.budget.budget_owner
    assert type(owner) is AgentToolOwnerPin

    with pytest.raises(AcquisitionStartV2Error, match=ACQUISITION_START_V2_PARENT_BUDGET_INVALID):
        build_acquisition_parent_budget_envelope_ref(receipt, owner)
