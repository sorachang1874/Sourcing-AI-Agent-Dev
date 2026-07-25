from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sourcing_agent.control_plane_live_postgres import _PRIMARY_KEY_COLUMNS
from sourcing_agent.control_plane_repository import Column, Kind, TableDescriptor
from sourcing_agent.model_tool_runtime import (
    MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION,
    ModelInvocationEnvelopeError,
    ModelInvocationEnvelopeV1,
)
from sourcing_agent.model_usage import ModelUsage
from sourcing_agent.repositories.model_invocation_envelopes import (
    MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST,
    MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION,
    MODEL_INVOCATION_ENVELOPE_TABLE,
    ModelInvocationEnvelopeCollisionError,
    ModelInvocationEnvelopeRepository,
    ModelInvocationEnvelopeRepositoryError,
)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _envelope(mode: str = "scripted") -> ModelInvocationEnvelopeV1:
    return ModelInvocationEnvelopeV1(
        schema_version=MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION,
        route_id="product.model.route.v1",
        route_revision=_digest("route-v1"),
        provider="openai_compatible",
        api_style="chat_completions",
        requested_model="gpt-test",
        response_model="gpt-test",
        effective_model="gpt-test",
        model_identity_provenance="provider_response",
        effective_route_snapshot_ref=("route-snapshot:test:v1" if mode == "live" else None),
        effective_route_snapshot_digest=_digest("route-snapshot-v1"),
        circuit_identity="product.model.route.v1:test",
        runtime_namespace="test:d0f",
        provider_mode=mode,  # type: ignore[arg-type]
        workspace_id="workspace-a",
        actor_id="actor-a",
        permission_scope="agent:test",
        prompt_policy_version="prompt-v1",
        permission_scope_revision="permission-v1",
        outbound_policy_revision="outbound-v1",
        model_safe_schema_revision="model-safe-v1",
        operation_run_id="operation-a",
        turn_id="turn-a",
        step_id="step-a",
        workflow_command_id="command-a",
        activity_run_id="activity-a",
        activity_attempt_id="attempt-a",
        provider_call_id="provider-call-a",
        terminal_reason="end_turn",
        usage=ModelUsage(input_tokens=10, output_tokens=5, total_tokens=15),
        usage_status="reported",
        fallback_status="not_used",
        circuit_state="closed",
        evidence_bundle_hash=None,
        canonical_result_digest=_digest("result-a"),
        result_artifact_ref="artifact:test:a",
        result_artifact_digest=_digest("artifact-a"),
        cost_exposure_ref="cost-exposure:test:a",
        canonical_request_digest=_digest("request-a"),
    )


def _pfx(mode: str = "scripted") -> dict[str, object]:
    return {
        "runtime_namespace": "test:d0f",
        "provider_mode": mode,
        "workspace_id": "workspace-a",
        "scope_digest": _digest("scope-a"),
        "coordination_plan_review_id": 17,
    }


class _FakeAdapter:
    def __init__(self, *, authoritative: bool = True) -> None:
        self.authoritative = authoritative
        self.insert_calls: list[dict[str, Any]] = []
        self.get_calls: list[dict[str, Any]] = []
        self.purge_calls: list[dict[str, Any]] = []
        self.row: dict[str, Any] | None = None

    def should_prefer_read(self, table_name: str) -> bool:
        return self.authoritative and table_name == MODEL_INVOCATION_ENVELOPE_TABLE

    def is_authoritative(self, table_name: str) -> bool:
        return self.should_prefer_read(table_name)

    def insert_model_invocation_envelope(self, *, table_name: str, row: dict[str, Any]) -> dict[str, Any]:
        self.insert_calls.append({"table_name": table_name, "row": dict(row)})
        if self.row is None:
            created_at = datetime(2026, 7, 15, 1, 2, 3, tzinfo=timezone.utc)
            self.row = {
                **row,
                "retention_state": "retained",
                "retained_until": created_at + timedelta(days=30),
                "created_at": created_at,
                "purged_at": None,
                "state_version": 0,
            }
        return dict(self.row)

    def get_model_invocation_envelope(self, **kwargs: Any) -> dict[str, Any] | None:
        self.get_calls.append(dict(kwargs))
        return dict(self.row) if self.row is not None else None

    def purge_expired_model_invocation_envelopes(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.purge_calls.append(dict(kwargs))
        return []


def test_canonical_json_roundtrip_is_exact_and_rejects_noncanonical_bytes() -> None:
    envelope = _envelope()
    canonical = envelope.to_canonical_json()

    assert ModelInvocationEnvelopeV1.from_canonical_json(canonical) == envelope
    with pytest.raises(ModelInvocationEnvelopeError, match="canonical_json_not_canonical"):
        ModelInvocationEnvelopeV1.from_canonical_json(" " + canonical)
    with pytest.raises(ModelInvocationEnvelopeError, match="canonical_json_not_object"):
        ModelInvocationEnvelopeV1.from_canonical_json("[]")


@pytest.mark.parametrize("mode", ["live", "simulate", "scripted"])
def test_repository_persists_all_three_modes_with_owner_issued_full_pfx_ref(mode: str) -> None:
    adapter = _FakeAdapter()
    repository = ModelInvocationEnvelopeRepository(adapter)
    envelope = _envelope(mode)

    persisted = repository.persist(**_pfx(mode), envelope=envelope)  # type: ignore[arg-type]

    reference = persisted["model_invocation_envelope_ref"]
    pfx_payload = {"schema": "model_invocation_envelope_pfx_v1", **_pfx(mode)}
    expected_pfx_digest = hashlib.sha256(
        json.dumps(
            pfx_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    assert reference == f"mie:v1:{expected_pfx_digest}:{envelope.envelope_digest}"
    assert persisted["envelope"] == envelope
    assert adapter.insert_calls[0]["row"]["envelope_record_json"] == envelope.to_canonical_json()
    assert adapter.insert_calls[0]["row"]["retention_policy_version"] == (
        MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION
    )


@pytest.mark.parametrize(
    ("pfx_patch", "envelope_patch", "error"),
    [
        ({"provider_mode": "replay"}, {}, "provider_mode_ineligible"),
        (
            {},
            {
                "operation_run_id": None,
                "turn_id": None,
                "step_id": None,
                "workflow_command_id": None,
                "activity_run_id": None,
                "activity_attempt_id": None,
            },
            "durable_causality_required",
        ),
        ({}, {"cost_exposure_ref": None}, "cost_exposure_ref_required"),
        (
            {"provider_mode": "live"},
            {"provider_mode": "live", "effective_route_snapshot_ref": None},
            "live_route_snapshot_ref_required",
        ),
        ({"workspace_id": "workspace-b"}, {}, "pfx_mismatch:workspace_id"),
    ],
)
def test_repository_rejects_ineligible_or_incomplete_evidence_before_any_db_call(
    pfx_patch: dict[str, object],
    envelope_patch: dict[str, object],
    error: str,
) -> None:
    adapter = _FakeAdapter()
    repository = ModelInvocationEnvelopeRepository(adapter)
    pfx = {**_pfx(), **pfx_patch}
    envelope = replace(_envelope(), **envelope_patch)

    with pytest.raises(ModelInvocationEnvelopeRepositoryError, match=error):
        repository.persist(**pfx, envelope=envelope)  # type: ignore[arg-type]
    assert adapter.insert_calls == []


def test_lookup_requires_full_pfx_ref_and_digest_and_isolation_mismatch_is_zero_db_read() -> None:
    adapter = _FakeAdapter()
    repository = ModelInvocationEnvelopeRepository(adapter)
    persisted = repository.persist(**_pfx(), envelope=_envelope())  # type: ignore[arg-type]
    adapter.get_calls.clear()

    found = repository.get(
        **_pfx(),  # type: ignore[arg-type]
        model_invocation_envelope_ref=persisted["model_invocation_envelope_ref"],
        envelope_digest=persisted["envelope_digest"],
    )
    assert found is not None and found["envelope"] == _envelope()
    assert set(adapter.get_calls[0]) >= {
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "scope_digest",
        "coordination_plan_review_id",
        "model_invocation_envelope_ref",
        "envelope_digest",
    }

    with pytest.raises(ModelInvocationEnvelopeRepositoryError, match="lookup_identity_mismatch"):
        repository.get(
            **{**_pfx(), "workspace_id": "workspace-b"},  # type: ignore[arg-type]
            model_invocation_envelope_ref=persisted["model_invocation_envelope_ref"],
            envelope_digest=persisted["envelope_digest"],
        )
    assert len(adapter.get_calls) == 1


def test_repository_detects_canonical_storage_tamper_and_sqlite_authority_fails_closed() -> None:
    adapter = _FakeAdapter()
    repository = ModelInvocationEnvelopeRepository(adapter)
    repository.persist(**_pfx(), envelope=_envelope())  # type: ignore[arg-type]
    assert adapter.row is not None
    adapter.row["envelope_record_json"] = replace(
        _envelope(), canonical_result_digest=_digest("tampered-result")
    ).to_canonical_json()

    with pytest.raises(ModelInvocationEnvelopeCollisionError, match="digest_record"):
        repository.get(
            **_pfx(),  # type: ignore[arg-type]
            model_invocation_envelope_ref=adapter.row["model_invocation_envelope_ref"],
            envelope_digest=adapter.row["envelope_digest"],
        )
    adapter.row["envelope_record_json"] = _envelope().to_canonical_json()
    adapter.row["state_version"] = False
    with pytest.raises(ModelInvocationEnvelopeCollisionError, match="state_version_type"):
        repository.get(
            **_pfx(),  # type: ignore[arg-type]
            model_invocation_envelope_ref=adapter.row["model_invocation_envelope_ref"],
            envelope_digest=adapter.row["envelope_digest"],
        )

    sqlite_like = _FakeAdapter(authoritative=False)
    with pytest.raises(ModelInvocationEnvelopeRepositoryError, match="postgres_only"):
        ModelInvocationEnvelopeRepository(sqlite_like).persist(**_pfx(), envelope=_envelope())  # type: ignore[arg-type]
    assert sqlite_like.insert_calls == []


def test_timestamptz_codec_is_nullable_timezone_aware_and_utc_exact() -> None:
    descriptor = TableDescriptor(
        table="timestamp_probe",
        columns=(Column("occurred_at", Kind.TIMESTAMPTZ),),
        pk=("occurred_at",),
    )
    offset_value = datetime.fromisoformat("2026-07-15T09:30:00+08:00")

    assert descriptor.to_columns({"occurred_at": None}) == {"occurred_at": None}
    assert descriptor.from_row({"occurred_at": None}) == {"occurred_at": None}
    assert descriptor.to_columns({"occurred_at": offset_value})["occurred_at"] == datetime(
        2026, 7, 15, 1, 30, tzinfo=timezone.utc
    )
    assert descriptor.from_row({"occurred_at": "2026-07-15T01:30:00Z"})["occurred_at"] == datetime(
        2026, 7, 15, 1, 30, tzinfo=timezone.utc
    )
    with pytest.raises(ValueError, match="requires timezone"):
        descriptor.to_columns({"occurred_at": datetime(2026, 7, 15, 1, 30)})
    with pytest.raises(ValueError, match="naive TIMESTAMPTZ"):
        descriptor.from_row({"occurred_at": "2026-07-15T01:30:00"})
    with pytest.raises(TypeError, match="requires datetime"):
        descriptor.to_columns({"occurred_at": "2026-07-15T01:30:00Z"})
    with pytest.raises(TypeError, match="invalid TIMESTAMPTZ value type"):
        descriptor.from_row({"occurred_at": 17})


def test_physical_manifest_and_specialized_storage_surface_forbid_generic_upsert_registration() -> None:
    assert len(MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST) == 15
    assert tuple(row[0] for row in MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST) == (
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "scope_digest",
        "coordination_plan_review_id",
        "model_invocation_envelope_ref",
        "envelope_schema_version",
        "envelope_digest",
        "envelope_record_json",
        "retention_policy_version",
        "retention_state",
        "retained_until",
        "created_at",
        "purged_at",
        "state_version",
    )
    assert MODEL_INVOCATION_ENVELOPE_TABLE not in _PRIMARY_KEY_COLUMNS

    from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter

    insert_source = inspect.getsource(LiveControlPlanePostgresAdapter.insert_model_invocation_envelope)
    lookup_source = inspect.getsource(LiveControlPlanePostgresAdapter.get_model_invocation_envelope)
    assert "ON CONFLICT (" not in insert_source
    assert "FOR UPDATE" in insert_source
    assert "_acquire_transaction_lock" in insert_source
    assert all(
        field_name in lookup_source
        for field_name in (
            "runtime_namespace",
            "provider_mode",
            "workspace_id",
            "scope_digest",
            "coordination_plan_review_id",
            "model_invocation_envelope_ref",
            "envelope_digest",
        )
    )
