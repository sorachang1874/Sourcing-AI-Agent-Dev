"""PG-only durable owner for canonical ``ModelInvocationEnvelopeV1`` evidence.

The repository owns reference issuance, exact persistence/replay, scoped lookup,
and the one v1 retention transition. It does not own transport, cost exposure,
logical result-slot acceptance/consumption, or AgentAction effects.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from ..model_tool_runtime import (
    MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION,
    ModelInvocationEnvelopeError,
    ModelInvocationEnvelopeV1,
)

MODEL_INVOCATION_ENVELOPE_TABLE = "model_invocation_envelopes"
MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION = "model_invocation_retention_30d_v1"
MODEL_INVOCATION_ENVELOPE_RETENTION_DAYS = 30
MODEL_INVOCATION_ENVELOPE_PFX_FIELDS = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
)
MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST = (
    ("runtime_namespace", "TEXT", False, None),
    ("provider_mode", "TEXT", False, None),
    ("workspace_id", "TEXT", False, None),
    ("scope_digest", "TEXT", False, None),
    ("coordination_plan_review_id", "BIGINT", False, None),
    ("model_invocation_envelope_ref", "TEXT", False, None),
    ("envelope_schema_version", "TEXT", False, None),
    ("envelope_digest", "TEXT", False, None),
    ("envelope_record_json", "TEXT", True, None),
    ("retention_policy_version", "TEXT", False, None),
    ("retention_state", "TEXT", False, "retained"),
    ("retained_until", "TIMESTAMPTZ", False, None),
    ("created_at", "TIMESTAMPTZ", False, "transaction_timestamp()"),
    ("purged_at", "TIMESTAMPTZ", True, None),
    ("state_version", "BIGINT", False, 0),
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ELIGIBLE_PROVIDER_MODES = frozenset({"live", "simulate", "scripted"})


class ModelInvocationEnvelopeRepositoryError(RuntimeError):
    """Base error for durable envelope ownership failures."""


class ModelInvocationEnvelopeCollisionError(ModelInvocationEnvelopeRepositoryError):
    """An immutable identity resolved to different canonical evidence."""


class ModelInvocationEnvelopePurgedError(ModelInvocationEnvelopeRepositoryError):
    """A tombstoned envelope cannot be reinserted or revived."""


def _required_exact_text(field_name: str, value: object) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_invalid_{field_name}")
    return value


def _required_sha256(field_name: str, value: object) -> str:
    normalized = _required_exact_text(field_name, value)
    if _SHA256_RE.fullmatch(normalized) is None:
        raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_invalid_{field_name}_sha256")
    return normalized


def _required_positive_bigint(field_name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > 2**63 - 1:
        raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_invalid_{field_name}")
    return value


def _utc_datetime(field_name: str, value: object, *, nullable: bool = False) -> datetime | None:
    if value is None and nullable:
        return None
    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif type(value) is str and value:
        candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_invalid_{field_name}") from exc
    else:
        raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_invalid_{field_name}")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_invalid_{field_name}")
    return parsed.astimezone(timezone.utc)


def _canonical_pfx_digest(pfx: dict[str, object]) -> str:
    payload = {
        "schema": "model_invocation_envelope_pfx_v1",
        **{field_name: pfx[field_name] for field_name in MODEL_INVOCATION_ENVELOPE_PFX_FIELDS},
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class ModelInvocationEnvelopeRepository:
    """The sole durable issuer/persistence owner for v1 model envelopes."""

    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    def _require_pg_authority(self) -> None:
        prefer = getattr(self._adapter, "should_prefer_read", None)
        authoritative = getattr(self._adapter, "is_authoritative", None)
        if not callable(prefer) or not callable(authoritative):
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_postgres_only")
        try:
            is_pg_owner = bool(prefer(MODEL_INVOCATION_ENVELOPE_TABLE)) and bool(
                authoritative(MODEL_INVOCATION_ENVELOPE_TABLE)
            )
        except Exception as exc:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_postgres_only") from exc
        if not is_pg_owner:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_postgres_only")

    @staticmethod
    def _normalize_pfx(
        *,
        runtime_namespace: object,
        provider_mode: object,
        workspace_id: object,
        scope_digest: object,
        coordination_plan_review_id: object,
    ) -> dict[str, object]:
        normalized_mode = _required_exact_text("provider_mode", provider_mode)
        if normalized_mode not in _ELIGIBLE_PROVIDER_MODES:
            raise ModelInvocationEnvelopeRepositoryError(
                f"model_invocation_envelope_provider_mode_ineligible:{normalized_mode}"
            )
        return {
            "runtime_namespace": _required_exact_text("runtime_namespace", runtime_namespace),
            "provider_mode": normalized_mode,
            "workspace_id": _required_exact_text("workspace_id", workspace_id),
            "scope_digest": _required_sha256("scope_digest", scope_digest),
            "coordination_plan_review_id": _required_positive_bigint(
                "coordination_plan_review_id", coordination_plan_review_id
            ),
        }

    @staticmethod
    def _issued_ref(pfx: dict[str, object], envelope_digest: str) -> str:
        return f"mie:v1:{_canonical_pfx_digest(pfx)}:{envelope_digest}"

    @staticmethod
    def _validate_envelope_for_persistence(
        envelope: ModelInvocationEnvelopeV1,
        *,
        pfx: dict[str, object],
    ) -> str:
        if type(envelope) is not ModelInvocationEnvelopeV1:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_type_invalid")
        for field_name in ("runtime_namespace", "provider_mode", "workspace_id"):
            if getattr(envelope, field_name) != pfx[field_name]:
                raise ModelInvocationEnvelopeRepositoryError(f"model_invocation_envelope_pfx_mismatch:{field_name}")
        causality = (
            envelope.operation_run_id,
            envelope.turn_id,
            envelope.step_id,
            envelope.workflow_command_id,
            envelope.activity_run_id,
            envelope.activity_attempt_id,
        )
        if any(value is None for value in causality):
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_durable_causality_required")
        if envelope.cost_exposure_ref is None:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_cost_exposure_ref_required")
        if envelope.provider_mode == "live" and envelope.effective_route_snapshot_ref is None:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_live_route_snapshot_ref_required")
        canonical_json = envelope.to_canonical_json()
        try:
            round_tripped = ModelInvocationEnvelopeV1.from_canonical_json(canonical_json)
        except ModelInvocationEnvelopeError as exc:
            raise ModelInvocationEnvelopeRepositoryError(
                "model_invocation_envelope_canonical_roundtrip_failed"
            ) from exc
        if round_tripped != envelope:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_canonical_roundtrip_mismatch")
        return canonical_json

    def _validate_stored_row(
        self,
        raw_row: object,
        *,
        expected_pfx: dict[str, object] | None = None,
        expected_ref: str | None = None,
        expected_digest: str | None = None,
        expected_envelope: ModelInvocationEnvelopeV1 | None = None,
    ) -> dict[str, Any]:
        if not isinstance(raw_row, dict):
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_storage_row_invalid")
        row = dict(raw_row)
        if set(row) != {item[0] for item in MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST}:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_storage_row_keyset_invalid")
        pfx = self._normalize_pfx(
            **{field_name: row.get(field_name) for field_name in MODEL_INVOCATION_ENVELOPE_PFX_FIELDS}
        )
        if expected_pfx is not None and pfx != expected_pfx:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:pfx")
        digest = _required_sha256("envelope_digest", row.get("envelope_digest"))
        reference = _required_exact_text("ref", row.get("model_invocation_envelope_ref"))
        if reference != self._issued_ref(pfx, digest):
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:ref")
        if expected_ref is not None and reference != expected_ref:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:ref")
        if expected_digest is not None and digest != expected_digest:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:digest")
        if row.get("envelope_schema_version") != MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:schema_version")
        if row.get("retention_policy_version") != MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:retention_policy")
        created_at = _utc_datetime("created_at", row.get("created_at"))
        retained_until = _utc_datetime("retained_until", row.get("retained_until"))
        assert created_at is not None and retained_until is not None
        if retained_until != created_at + timedelta(days=MODEL_INVOCATION_ENVELOPE_RETENTION_DAYS):
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:retention_deadline")

        state = row.get("retention_state")
        record_json = row.get("envelope_record_json")
        purged_at = _utc_datetime("purged_at", row.get("purged_at"), nullable=True)
        version = row.get("state_version")
        if type(version) is not int:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:state_version_type")
        parsed_envelope: ModelInvocationEnvelopeV1 | None = None
        if state == "retained":
            if type(record_json) is not str or not record_json or purged_at is not None or version != 0:
                raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:retained_state")
            try:
                parsed_envelope = ModelInvocationEnvelopeV1.from_canonical_json(record_json)
            except ModelInvocationEnvelopeError as exc:
                raise ModelInvocationEnvelopeCollisionError(
                    "model_invocation_envelope_collision:canonical_record"
                ) from exc
            if parsed_envelope.envelope_digest != digest:
                raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:digest_record")
            if expected_envelope is not None and parsed_envelope != expected_envelope:
                raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:envelope")
        elif state == "purged_tombstone":
            if record_json is not None or purged_at is None or purged_at < retained_until or version != 1:
                raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:tombstone_state")
            if expected_envelope is not None:
                raise ModelInvocationEnvelopePurgedError("model_invocation_envelope_purged_tombstone")
        else:
            raise ModelInvocationEnvelopeCollisionError("model_invocation_envelope_collision:retention_state")
        return {**row, "envelope": parsed_envelope}

    def persist(
        self,
        *,
        runtime_namespace: str,
        provider_mode: str,
        workspace_id: str,
        scope_digest: str,
        coordination_plan_review_id: int,
        envelope: ModelInvocationEnvelopeV1,
    ) -> dict[str, Any]:
        """Insert once or return an exact retained replay; never revive a tombstone."""

        pfx = self._normalize_pfx(
            runtime_namespace=runtime_namespace,
            provider_mode=provider_mode,
            workspace_id=workspace_id,
            scope_digest=scope_digest,
            coordination_plan_review_id=coordination_plan_review_id,
        )
        canonical_json = self._validate_envelope_for_persistence(envelope, pfx=pfx)
        reference = self._issued_ref(pfx, envelope.envelope_digest)
        self._require_pg_authority()
        try:
            row = self._adapter.insert_model_invocation_envelope(
                table_name=MODEL_INVOCATION_ENVELOPE_TABLE,
                row={
                    **pfx,
                    "model_invocation_envelope_ref": reference,
                    "envelope_schema_version": envelope.schema_version,
                    "envelope_digest": envelope.envelope_digest,
                    "envelope_record_json": canonical_json,
                    "retention_policy_version": MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION,
                },
            )
        except ValueError as exc:
            if str(exc).startswith("model_invocation_envelope_collision:"):
                raise ModelInvocationEnvelopeCollisionError(str(exc)) from exc
            raise
        if row is None:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_insert_unconfirmed")
        return self._validate_stored_row(
            row,
            expected_pfx=pfx,
            expected_ref=reference,
            expected_digest=envelope.envelope_digest,
            expected_envelope=envelope,
        )

    def get(
        self,
        *,
        runtime_namespace: str,
        provider_mode: str,
        workspace_id: str,
        scope_digest: str,
        coordination_plan_review_id: int,
        model_invocation_envelope_ref: str,
        envelope_digest: str,
    ) -> dict[str, Any] | None:
        """Lookup requires the complete PFX plus both owner-issued identities."""

        pfx = self._normalize_pfx(
            runtime_namespace=runtime_namespace,
            provider_mode=provider_mode,
            workspace_id=workspace_id,
            scope_digest=scope_digest,
            coordination_plan_review_id=coordination_plan_review_id,
        )
        digest = _required_sha256("envelope_digest", envelope_digest)
        reference = _required_exact_text("ref", model_invocation_envelope_ref)
        if reference != self._issued_ref(pfx, digest):
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_lookup_identity_mismatch")
        self._require_pg_authority()
        row = self._adapter.get_model_invocation_envelope(
            table_name=MODEL_INVOCATION_ENVELOPE_TABLE,
            **pfx,
            model_invocation_envelope_ref=reference,
            envelope_digest=digest,
        )
        if row is None:
            return None
        return self._validate_stored_row(
            row,
            expected_pfx=pfx,
            expected_ref=reference,
            expected_digest=digest,
        )

    def purge_expired(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Apply the retained-to-tombstone DB-clock CAS for the fixed v1 policy."""

        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1 or limit > 1000:
            raise ModelInvocationEnvelopeRepositoryError("model_invocation_envelope_purge_limit_invalid")
        self._require_pg_authority()
        rows = self._adapter.purge_expired_model_invocation_envelopes(
            table_name=MODEL_INVOCATION_ENVELOPE_TABLE,
            retention_policy_version=MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION,
            limit=limit,
        )
        return [self._validate_stored_row(row) for row in rows]


__all__ = [
    "MODEL_INVOCATION_ENVELOPE_COLUMN_MANIFEST",
    "MODEL_INVOCATION_ENVELOPE_PFX_FIELDS",
    "MODEL_INVOCATION_ENVELOPE_RETENTION_DAYS",
    "MODEL_INVOCATION_ENVELOPE_RETENTION_POLICY_VERSION",
    "MODEL_INVOCATION_ENVELOPE_TABLE",
    "ModelInvocationEnvelopeCollisionError",
    "ModelInvocationEnvelopePurgedError",
    "ModelInvocationEnvelopeRepository",
    "ModelInvocationEnvelopeRepositoryError",
]
