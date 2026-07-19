"""Versioned progressed-child contract shared by root completion and inspection.

A root command progresses when its completion owner commits, in one
transaction, the child command row, the child ``CommandPlanRequested`` plan
event, and the root ``downstream_command_ids`` edge.  The write-side owner
(``complete_acquisition_root_command``), the succeeded-replay validator, and
the read-side inspection verifier must all validate the exact same immutable
child/event/causality identity, so the registered completion contracts, the
field sets, and the derivations live here exactly once
(``PROGRESSED_CHILD_CONTRACT_VERSION``).

Only the explicitly mutable child lifecycle fields may change after that
commit; every other child/event/causality field is exact-compared with
type-strict contract equality (``json_contract_equal``), so ``1`` can never
alias ``true`` or ``1.0`` inside a persisted identity.
"""

from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha1
from typing import Any

from .json_contract import json_contract_equal

PROGRESSED_CHILD_CONTRACT_VERSION = "progressed_workflow_child_contract_v1"

# Registered parent-root -> progressed-child completion contracts.  The
# child plan event actor/source are pinned per contract: the owner path
# rejects a drifted caller spec and the read-side verifiers reject a drifted
# persisted event, so a semantically foreign child can never pass as the
# registered progression of its root.
PROGRESSED_CHILD_COMPLETION_CONTRACTS: dict[str, dict[str, Any]] = {
    "acquisition_root": {
        "parent_command_type": "acquisition.run.create",
        "parent_owner": "acquisition_run_writer",
        "child_command_type": "acquisition.intent.resolve",
        "child_owner": "acquisition_planner",
        "child_plan_event_actor": "acquisition_run_create_owner",
        "child_plan_event_source": "acquisition_run_create.command_owner",
        "reason_prefix": "acquisition_root",
        "child_reason_prefix": "acquisition_root_intent_child",
        "committed_reason": "acquisition_root_intent_child_committed",
        "entity_delta_count": 0,
        "entity_delta_type": "",
    },
    "company_public_web_source": {
        "parent_command_type": "company.public_web.source.collect",
        "parent_owner": "company_public_web_owner",
        "child_command_type": "company.public_web.assets.materialize",
        "child_owner": "company_public_web_owner",
        "child_plan_event_actor": "company_public_web_phase_planner",
        "child_plan_event_source": "company_public_web_source_collect_owner",
        "reason_prefix": "company_public_web_source",
        "child_reason_prefix": "company_public_web_source_materialize_child",
        "committed_reason": "company_public_web_source_materialize_child_committed",
        "entity_delta_count": 1,
        "entity_delta_type": "company_public_web_run",
    },
}

# The only child fields allowed to change after the completion commit: the
# child may be claimed, run, released, retried, completed, and may itself
# progress.  ``created_at``/``updated_at`` are row metadata: ``updated_at``
# moves with every lifecycle write, and ``created_at`` is not re-derivable at
# replay, so neither participates in the compared identity.
PROGRESSED_CHILD_MUTABLE_LIFECYCLE_FIELDS = frozenset(
    {
        "status",
        "attempt",
        "lease_owner",
        "lease_expires_at",
        "heartbeat_at",
        "last_error",
        "result",
        "not_before_at",
        "downstream_command_ids",
        "updated_at",
    }
)

_CHILD_IDENTITY_SCALAR_FIELDS = (
    "command_id",
    "workflow_run_id",
    "operation_id",
    "command_type",
    "owner",
    "stage_id",
    "causal_group_id",
    "parent_command_id",
    "source_event_id",
    "source_event_type",
    "no_op_reason",
    "readiness_effect",
    "causality_schema_version",
    "idempotency_key",
    "schema_version",
)

# Payload-causality scalar fields that must mirror the child's own columns
# exactly (``schema_version`` mirrors ``causality_schema_version``).
_CHILD_CAUSALITY_SCALAR_MIRROR = (
    "workflow_run_id",
    "operation_id",
    "stage_id",
    "causal_group_id",
    "parent_command_id",
    "source_event_id",
    "source_event_type",
    "command_type",
    "owner",
    "idempotency_key",
    "no_op_reason",
    "readiness_effect",
)

_CHILD_CAUSALITY_JSON_MIRROR = (
    "input_artifact_refs",
    "output_artifact_refs",
    "produced_entity_counts",
)


def progressed_child_completion_contract(name: str) -> dict[str, Any] | None:
    """Return the registered completion contract for ``name``, or ``None``."""

    contract = PROGRESSED_CHILD_COMPLETION_CONTRACTS.get(str(name or "").strip())
    return dict(contract) if contract is not None else None


def progressed_child_completion_contract_for(
    *,
    parent_command_type: str,
    parent_owner: str,
) -> tuple[str, dict[str, Any]] | None:
    """Resolve the registered completion contract for one parent type/owner pair."""

    normalized_type = str(parent_command_type or "").strip()
    normalized_owner = str(parent_owner or "").strip()
    for name, contract in PROGRESSED_CHILD_COMPLETION_CONTRACTS.items():
        if (
            str(contract.get("parent_command_type") or "") == normalized_type
            and str(contract.get("parent_owner") or "") == normalized_owner
        ):
            return name, dict(contract)
    return None


def progressed_child_plan_event_idempotency_key(child_idempotency_key: str) -> str:
    """The one registered plan-event idempotency derivation for a child."""

    return f"{str(child_idempotency_key or '').strip()}:plan"


def progressed_child_plan_event_id(workflow_run_id: str, sequence_number: int, idempotency_key: str) -> str:
    """The one deterministic plan-event id for a run/sequence/idempotency triple."""

    return "evt_" + sha1(f"{workflow_run_id}:{sequence_number}:{idempotency_key}".encode("utf-8")).hexdigest()[:24]


def _normalized_scalar(value: Any) -> str:
    return str(value or "").strip()


def canonical_progressed_child_identity(child: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the immutable identity of one decoded progressed-child row.

    ``child`` carries scalar columns plus materialized JSON fields
    (``input_artifact_refs``/``output_artifact_refs``/``produced_entity_counts``/
    ``payload``/``artifact_refs``/``retry_policy``).  The payload causality must
    mirror every causality column exactly (type-strict for JSON fields):
    ``downstream_command_ids`` is the one causality payload member excluded
    because the column is mutable lifecycle.  Returns ``None`` when the row is
    outside the contract instead of normalizing it.
    """

    payload = child.get("payload")
    if not isinstance(payload, Mapping):
        return None
    causality = payload.get("causality")
    if not isinstance(causality, Mapping):
        return None
    identity = {field: _normalized_scalar(child.get(field)) for field in _CHILD_IDENTITY_SCALAR_FIELDS}
    for field in _CHILD_CAUSALITY_SCALAR_MIRROR:
        if _normalized_scalar(causality.get(field)) != identity[field]:
            return None
    if _normalized_scalar(causality.get("schema_version")) != identity["causality_schema_version"]:
        return None
    for field in _CHILD_CAUSALITY_JSON_MIRROR:
        value = child.get(field)
        if not json_contract_equal(causality.get(field), value):
            return None
        identity[field] = value
    max_attempts = child.get("max_attempts")
    retry_policy = child.get("retry_policy")
    artifact_refs = child.get("artifact_refs")
    if not isinstance(retry_policy, Mapping) or not isinstance(artifact_refs, list):
        return None
    try:
        identity["max_attempts"] = int(max_attempts or 0)
    except (TypeError, ValueError):
        return None
    identity["payload"] = dict(payload)
    identity["artifact_refs"] = list(artifact_refs)
    identity["retry_policy"] = dict(retry_policy)
    return identity


def expected_progressed_child_row(
    *,
    contract: Mapping[str, Any],
    parent_command_id: str,
    workflow_run_id: str,
    operation_id: str,
    child_command: Mapping[str, Any],
    child_causality: Mapping[str, Any],
) -> dict[str, Any]:
    """Materialize the decoded expected progressed-child row from one completion spec.

    ``child_command`` is the owner spec (``command_id``/``idempotency_key``/
    ``payload``/``artifact_refs``/``max_attempts``/``retry_policy``);
    ``child_causality`` is the complete causality payload including the
    committed ``source_event_id``.  The result feeds
    ``canonical_progressed_child_identity`` so the expected and persisted
    sides are compared through the exact same canonicalization.
    """

    causality = dict(child_causality or {})
    child_payload = {**dict(child_command.get("payload") or {}), "causality": causality}
    return {
        "command_id": _normalized_scalar(child_command.get("command_id")),
        "workflow_run_id": _normalized_scalar(workflow_run_id),
        "operation_id": _normalized_scalar(operation_id),
        "command_type": _normalized_scalar(contract.get("child_command_type")),
        "owner": _normalized_scalar(contract.get("child_owner")),
        "stage_id": _normalized_scalar(causality.get("stage_id")),
        "causal_group_id": _normalized_scalar(causality.get("causal_group_id")),
        "parent_command_id": _normalized_scalar(parent_command_id),
        "source_event_id": _normalized_scalar(causality.get("source_event_id")),
        "source_event_type": _normalized_scalar(causality.get("source_event_type")),
        "input_artifact_refs": list(causality.get("input_artifact_refs") or []),
        "output_artifact_refs": list(causality.get("output_artifact_refs") or []),
        "produced_entity_counts": dict(causality.get("produced_entity_counts") or {}),
        "no_op_reason": _normalized_scalar(causality.get("no_op_reason")),
        "readiness_effect": _normalized_scalar(causality.get("readiness_effect")),
        "causality_schema_version": _normalized_scalar(causality.get("schema_version") or "command_causality_v1"),
        "idempotency_key": _normalized_scalar(child_command.get("idempotency_key")),
        "payload": child_payload,
        "artifact_refs": list(child_command.get("artifact_refs") or []),
        "max_attempts": max(1, int(child_command.get("max_attempts") or 3)),
        "retry_policy": dict(child_command.get("retry_policy") or {}),
        "schema_version": "workflow_command_v1",
    }


def progressed_child_plan_event_violation(
    *,
    contract: Mapping[str, Any],
    parent_command_id: str,
    parent_source_sequence: int,
    child_identity: Mapping[str, Any],
    event: Mapping[str, Any],
) -> str:
    """Return ``""`` when ``event`` is the exact registered plan event of the child.

    ``event`` carries the decoded scalar columns plus ``payload`` (dict) and
    ``artifact_refs`` (list).  Every immutable event field is exact-compared:
    run/operation/command binding, empty activity attempt, family/type/schema,
    the registered ``<child-idempotency>:plan`` idempotency key, the pinned
    per-contract actor/source, a positive sequence ordered after the parent's
    own source event, the deterministic ``evt_<sha1>`` id, empty artifact
    refs, and the full plan payload (child type/idempotency/parent/stage/
    causal group/max_attempts/retry policy plus the child payload without its
    causality member).  Any drift returns a short reason token.
    """

    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        return "event_payload_invalid"
    workflow_run_id = _normalized_scalar(child_identity.get("workflow_run_id"))
    idempotency_key = _normalized_scalar(child_identity.get("idempotency_key"))
    plan_idempotency_key = progressed_child_plan_event_idempotency_key(idempotency_key)
    parent_sequence = max(0, int(parent_source_sequence or 0))
    sequence = event.get("sequence_number")
    if parent_sequence <= 0 or type(sequence) is not int or sequence <= parent_sequence:
        return "event_sequence_invalid"
    if (
        _normalized_scalar(event.get("event_id"))
        != progressed_child_plan_event_id(workflow_run_id, sequence, plan_idempotency_key)
        or _normalized_scalar(event.get("idempotency_key")) != plan_idempotency_key
    ):
        return "event_idempotency_mismatch"
    if (
        _normalized_scalar(event.get("workflow_run_id")) != workflow_run_id
        or _normalized_scalar(event.get("operation_id")) != _normalized_scalar(child_identity.get("operation_id"))
        or _normalized_scalar(event.get("command_id")) != _normalized_scalar(parent_command_id)
        or _normalized_scalar(event.get("activity_attempt_id")) != ""
        or _normalized_scalar(event.get("event_family")) != "workflow_event"
        or _normalized_scalar(event.get("event_type")) != "CommandPlanRequested"
        or _normalized_scalar(event.get("schema_version")) != "workflow_event_v1"
    ):
        return "event_column_mismatch"
    if _normalized_scalar(event.get("actor")) != _normalized_scalar(
        contract.get("child_plan_event_actor")
    ) or _normalized_scalar(event.get("source")) != _normalized_scalar(contract.get("child_plan_event_source")):
        return "event_actor_source_mismatch"
    if not json_contract_equal(list(event.get("artifact_refs") or []), []):
        return "event_artifact_refs_mismatch"
    stage_id = _normalized_scalar(payload.get("stage_id") or payload.get("stage_key"))
    if (
        _normalized_scalar(payload.get("command_type")) != _normalized_scalar(child_identity.get("command_type"))
        or _normalized_scalar(payload.get("idempotency_key")) != idempotency_key
        or _normalized_scalar(payload.get("parent_command_id")) != _normalized_scalar(parent_command_id)
        or _normalized_scalar(payload.get("causal_group_id"))
        != _normalized_scalar(child_identity.get("causal_group_id"))
        or stage_id != _normalized_scalar(child_identity.get("stage_id"))
    ):
        return "event_payload_identity_mismatch"
    try:
        event_max_attempts = int(payload.get("max_attempts") or 0)
    except (TypeError, ValueError):
        return "event_payload_identity_mismatch"
    if event_max_attempts != int(child_identity.get("max_attempts") or 0) or not json_contract_equal(
        payload.get("retry_policy"), child_identity.get("retry_policy")
    ):
        return "event_payload_identity_mismatch"
    child_payload = child_identity.get("payload")
    expected_inner_payload = (
        {key: value for key, value in dict(child_payload).items() if key != "causality"}
        if isinstance(child_payload, Mapping)
        else None
    )
    if expected_inner_payload is None or not json_contract_equal(payload.get("payload"), expected_inner_payload):
        return "event_payload_payload_mismatch"
    return ""
