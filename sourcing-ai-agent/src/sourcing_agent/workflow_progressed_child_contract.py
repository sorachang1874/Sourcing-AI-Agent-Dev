"""Versioned progressed-child contract shared by root completion and inspection.

A root command progresses when its completion owner commits, in one
transaction, the child command row, the child ``CommandPlanRequested`` plan
event, and the root ``downstream_command_ids`` edge.  The write-side owner
(``complete_acquisition_root_command``), the succeeded-replay validators, and
the read-side inspection verifier must all validate the exact same immutable
child/event/causality identity, so the registered completion contracts, the
registered pure builders, the field sets, and the derivations live here
exactly once (``PROGRESSED_CHILD_CONTRACT_VERSION``).

Every committed child causality payload and child plan-event payload carries
an immutable contract pin (name/version/digest over the registered contract):
a persisted row whose pin is missing, unknown, or from another contract
version fails closed instead of being silently reinterpreted by future code.

Only the explicitly mutable child lifecycle fields may change after that
commit; every other child/event/causality field is exact-compared with
type-strict contract equality (``json_contract_equal``) against the complete
expected row reconstructed from the parent command through the registered
pure builder — padded strings, foreign stage/group/readiness/artifact values,
foreign workflow types, and missing/extra payload keys never pass.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .durable_runtime import (
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    command_causality_for,
    command_id_for,
    default_stage_id_for_command_type,
)
from .json_contract import json_contract_equal

PROGRESSED_CHILD_CONTRACT_VERSION = "progressed_workflow_child_contract_v2"

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

ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION = "acquisition_root_command_payload.v2"


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


def progressed_child_completion_contract_for_child(child_command_type: str) -> tuple[str, dict[str, Any]] | None:
    """Resolve the registered completion contract by its unique child type."""

    normalized_type = str(child_command_type or "").strip()
    for name, contract in PROGRESSED_CHILD_COMPLETION_CONTRACTS.items():
        if str(contract.get("child_command_type") or "") == normalized_type:
            return name, dict(contract)
    return None


def progressed_child_contract_pin(name: str) -> dict[str, str]:
    """Return the immutable contract pin (name/version/digest) for one contract.

    The digest binds the complete registered contract entry under the current
    ``PROGRESSED_CHILD_CONTRACT_VERSION``: any registry change without a
    version bump, or a persisted pin minted under another name/version, fails
    exact comparison instead of being reinterpreted.
    """

    normalized_name = str(name or "").strip()
    contract = PROGRESSED_CHILD_COMPLETION_CONTRACTS.get(normalized_name)
    if contract is None:
        return {}
    canonical = json.dumps(
        {"contract": contract, "name": normalized_name, "version": PROGRESSED_CHILD_CONTRACT_VERSION},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "name": normalized_name,
        "version": PROGRESSED_CHILD_CONTRACT_VERSION,
        "digest": hashlib.sha256(canonical).hexdigest(),
    }


def progressed_child_plan_event_idempotency_key(child_idempotency_key: str) -> str:
    """The one registered plan-event idempotency derivation for a child."""

    return f"{str(child_idempotency_key or '').strip()}:plan"


def progressed_child_plan_event_id(workflow_run_id: str, sequence_number: int, idempotency_key: str) -> str:
    """The one deterministic plan-event id for a run/sequence/idempotency triple."""

    return (
        "evt_" + hashlib.sha1(f"{workflow_run_id}:{sequence_number}:{idempotency_key}".encode("utf-8")).hexdigest()[:24]
    )


def _exact_text(value: Any) -> str:
    """Comparison text: builders normalize at mint time; verifiers never trim."""

    return str(value or "")


def start_v2_root_workflow_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize the start-v2 root owner workflow payload from one root payload."""

    root = dict(payload or {})
    if str(root.get("schema_version") or "").strip() != ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION:
        return {}
    snapshot = dict(root.get("start_snapshot") or {})
    preview = dict(snapshot.get("preview") or {})
    company = dict(preview.get("company_target") or {})
    effective = dict(preview.get("effective_request") or {})
    manifest = dict(preview.get("provider_planning_manifest") or {})
    target_company = str(company.get("canonical_name") or company.get("canonical_company_id") or "").strip()
    if not target_company:
        return {}
    lane_queries: list[str] = []
    for lane in list(manifest.get("lanes") or []):
        provider_payload = dict(dict(lane or {}).get("provider_payload") or {})
        query_text = str(provider_payload.get("query_text") or "").strip()
        if query_text and query_text not in lane_queries:
            lane_queries.append(query_text)
    return {
        "schema_version": "acquisition_start_v2_root_owner_compat_payload.v1",
        "target_company": target_company,
        "canonical_company_id": str(company.get("canonical_company_id") or "").strip(),
        "provider_company_labels": list(company.get("provider_company_labels") or []),
        "query": " | ".join(lane_queries),
        "cohort_selection": dict(effective.get("cohort_selection") or {}),
        "source_preferences": list(effective.get("source_preferences") or []),
        "coverage_intent": str(effective.get("coverage_intent") or "").strip(),
        "thematic_constraints": list(effective.get("thematic_constraints") or []),
        "provider_mode_intent": str(effective.get("provider_mode_intent") or "").strip(),
        "budget": dict(effective.get("budget") or {}),
        "provider_planning_manifest_ref": {
            "schema_version": str(manifest.get("schema_version") or "").strip(),
            "manifest_digest": str(manifest.get("manifest_digest") or "").strip(),
            "physical_query_digest": str(manifest.get("physical_query_digest") or "").strip(),
        },
        "preview_ref": {
            "preview_id": str(preview.get("preview_id") or "").strip(),
            "preview_revision": int(preview.get("preview_revision") or 0),
            "preview_digest": str(preview.get("preview_digest") or "").strip(),
        },
        "start_snapshot_digest": str(root.get("start_snapshot_digest") or "").strip(),
        "confirmation_receipt_ref": dict(root.get("confirmation_receipt_ref") or {}),
        "start_action_id": str(root.get("action_id") or "").strip(),
    }


def _mint_progressed_child_pin(
    *,
    parent_command_type: str,
    parent_owner: str,
    child_causality: dict[str, Any],
    plan_event_payload: dict[str, Any],
) -> None:
    """Mint the contract pin into a causality payload and plan-event payload.

    Pins are minted only for registered parent/child pairs; unregistered
    pairs (for example a reducer-planned phase child outside the registry)
    stay unpinned and never pass a contract-verifying path.
    """

    resolved = progressed_child_completion_contract_for(
        parent_command_type=parent_command_type,
        parent_owner=parent_owner,
    )
    if resolved is None:
        return
    contract_name, contract = resolved
    if _exact_text(child_causality.get("command_type")) != _exact_text(contract.get("child_command_type")):
        return
    pin = progressed_child_contract_pin(contract_name)
    child_causality["progressed_child_contract"] = pin
    plan_event_payload["progressed_child_contract"] = pin


def build_acquisition_root_intent_plan(
    command: Mapping[str, Any],
    *,
    claim_attempt: int,
) -> dict[str, Any]:
    """The registered pure builder for the ``acquisition_root`` completion.

    Reconstructs the complete expected plan event, child command, child
    causality, and root terminal result from one locked root command; every
    verifier compares persisted rows against this single reconstruction.
    """

    root = dict(command or {})
    payload = dict(root.get("payload") or {})
    workflow_payload = dict(payload.get("workflow_payload") or {})
    if not workflow_payload:
        workflow_payload = start_v2_root_workflow_payload(payload)
    workflow_run_id = str(root.get("workflow_run_id") or "").strip()
    operation_id = str(root.get("operation_id") or "").strip()
    root_command_id = str(root.get("command_id") or "").strip()
    target_company = str(payload.get("target_company") or workflow_payload.get("target_company") or "").strip()
    query_text = str(
        payload.get("query") or workflow_payload.get("query") or workflow_payload.get("raw_user_request") or ""
    ).strip()
    plan_review_id = str(payload.get("plan_review_id") or workflow_payload.get("plan_review_id") or "").strip()
    normalized_attempt = max(0, int(claim_attempt or 0))
    if (
        not workflow_run_id
        or not operation_id
        or not root_command_id
        or normalized_attempt <= 0
        or (not plan_review_id and not target_company and not query_text)
    ):
        return {}
    child_idempotency_key = f"{ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE}:parent:{root_command_id}"
    child_command_id = command_id_for(workflow_run_id, child_idempotency_key)
    root_causality = dict(payload.get("causality") or {})
    causal_group_id = (
        str(root_causality.get("causal_group_id") or "").strip()
        or str(root.get("causal_group_id") or "").strip()
        or root_command_id
    )
    child_payload = {
        "workflow_payload": workflow_payload,
        "target_company": target_company,
        "query": query_text,
        "plan_review_id": plan_review_id,
        "intent_count": 1,
        "query_count": 1 if query_text else 0,
        "parent_command_id": root_command_id,
        "causal_group_id": causal_group_id,
        "operation_run_id": operation_id,
        "action_id": str(payload.get("action_id") or "").strip(),
        "source": "acquisition_run_create.command_owner",
        "migration_phase": "W11b_acquisition_intent_resolve",
        "normal_path_executes_queue_workflow_inline": False,
    }
    stage_id = default_stage_id_for_command_type(ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE)
    child_owner = DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE)
    plan_event_payload = {
        "workflow_type": "agent_callable_acquisition",
        "stage_key": stage_id,
        "stage_id": stage_id,
        "command_type": ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
        "idempotency_key": child_idempotency_key,
        "parent_command_id": root_command_id,
        "causal_group_id": causal_group_id,
        "payload": child_payload,
        "max_attempts": 3,
        "retry_policy": {
            "kind": "acquisition_intent_resolve",
            "retry_delay_seconds": 10,
        },
    }
    source_event_template = {
        "event_id": "",
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_id,
        "command_id": root_command_id,
        "event_type": "CommandPlanRequested",
        "payload": plan_event_payload,
    }
    child_causality = command_causality_for(
        workflow_run_id=workflow_run_id,
        operation_id=operation_id,
        stage_id=stage_id,
        command_type=ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
        owner=child_owner,
        idempotency_key=child_idempotency_key,
        source_event=source_event_template,
        command_payload=child_payload,
        artifact_refs=(),
    ).to_payload()
    _mint_progressed_child_pin(
        parent_command_type=str(root.get("command_type") or "").strip(),
        parent_owner=str(root.get("owner") or "").strip(),
        child_causality=child_causality,
        plan_event_payload=plan_event_payload,
    )
    child_ref = {
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_id,
        "command_id": child_command_id,
        "command_type": ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
        "owner": child_owner,
        "idempotency_key": child_idempotency_key,
        "parent_command_id": root_command_id,
    }
    root_result = {
        "status": "ready_for_downstream_commands",
        "reason": "acquisition_run_root_recorded",
        "operation_completion_deferred": True,
        "module_state_mutated": False,
        "normal_path_executes_queue_workflow_inline": False,
        "queue_workflow_called": False,
        "target_company": target_company,
        "query": query_text,
        "plan_review_id": plan_review_id,
        "workflow_payload": workflow_payload,
        "downstream_command_required": True,
        "downstream_command_count": 1,
        "downstream_command_ids": [child_command_id],
        "downstream_command_ref": child_ref,
        "downstream_command_types": [ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE],
        "next_phase": "W11b_acquisition_intent_plan_commands",
        "created_or_reused_job_id": "",
        "legacy_job_shell_created": False,
        "completed_claim_attempt": normalized_attempt,
        "migration_phase": "W11a_acquisition_run_create_root",
        "terminal_result_schema_version": "acquisition_root_terminal_result_v1",
        "contract": "w11a_acquisition_run_create_root_owner_v2",
    }
    return {
        "plan_event": {
            "workflow_run_id": workflow_run_id,
            "operation_id": operation_id,
            "command_id": root_command_id,
            "event_family": "workflow_event",
            "event_type": "CommandPlanRequested",
            "idempotency_key": progressed_child_plan_event_idempotency_key(child_idempotency_key),
            "actor": "acquisition_run_create_owner",
            "source": "acquisition_run_create.command_owner",
            "payload": plan_event_payload,
            "artifact_refs": [],
        },
        "child_command": {
            **child_ref,
            "payload": child_payload,
            "artifact_refs": [],
            "not_before_at": "",
            "max_attempts": 3,
            "retry_policy": {
                "kind": "acquisition_intent_resolve",
                "retry_delay_seconds": 10,
            },
        },
        "child_causality": child_causality,
        "root_result": root_result,
    }


def build_company_public_web_phase_plan(
    *,
    parent_command: dict[str, Any],
    command_type: str,
    command_payload: dict[str, Any],
    idempotency_suffix: str,
    source: str = "company_public_web_refresh_owner",
) -> dict[str, Any]:
    """The registered pure builder for Company Public Web phase children.

    Reconstructs the complete expected plan event, child command, and child
    causality for one phase child of a Company Public Web parent; the
    contract pin is minted for the registered
    ``company_public_web_source`` pair.
    """

    normalized_type = str(command_type or "").strip()
    if normalized_type not in {
        COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
        COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    }:
        return {}
    parent_payload = dict(parent_command or {})
    parent_command_id = str(parent_payload.get("command_id") or "").strip()
    workflow_run_id = str(parent_payload.get("workflow_run_id") or "").strip()
    operation_id = (
        str(parent_payload.get("operation_id") or "").strip()
        or str(dict(parent_payload.get("payload") or {}).get("operation_id") or "").strip()
    )
    if not workflow_run_id or not operation_id or not parent_command_id:
        return {}
    phase_payload = {key: value for key, value in dict(command_payload or {}).items() if key != "causality"}
    payload = {
        **phase_payload,
        "operation_id": operation_id,
        "parent_command_id": parent_command_id,
        "causal_group_id": parent_command_id
        or str(parent_payload.get("idempotency_key") or "").strip()
        or str(idempotency_suffix or "").strip(),
        "source": str(source or "company_public_web_refresh_owner").strip(),
        "migration_phase": "W11_company_public_web_phase_command",
    }
    idempotency_key = (
        f"{normalized_type}:{parent_command_id or workflow_run_id}:"
        f"{hashlib.sha1(str(idempotency_suffix or payload).encode('utf-8')).hexdigest()[:24]}"
    )
    stage_id = default_stage_id_for_command_type(normalized_type)
    child_owner = DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(normalized_type)
    retry_policy = {"kind": "company_public_web_phase", "retry_delay_seconds": 30}
    plan_event_payload = {
        "workflow_type": "company_public_web_refresh",
        "stage_key": stage_id,
        "command_type": normalized_type,
        "idempotency_key": idempotency_key,
        "parent_command_id": parent_command_id,
        "causal_group_id": payload["causal_group_id"],
        "payload": payload,
        "max_attempts": 3,
        "retry_policy": retry_policy,
    }
    source_event_template = {
        "event_id": "",
        "workflow_run_id": workflow_run_id,
        "operation_id": operation_id,
        "command_id": parent_command_id,
        "event_type": "CommandPlanRequested",
        "payload": plan_event_payload,
    }
    child_causality = command_causality_for(
        workflow_run_id=workflow_run_id,
        operation_id=operation_id,
        stage_id=stage_id,
        command_type=normalized_type,
        owner=child_owner,
        idempotency_key=idempotency_key,
        source_event=source_event_template,
        command_payload=payload,
        artifact_refs=(),
    ).to_payload()
    _mint_progressed_child_pin(
        parent_command_type=str(parent_payload.get("command_type") or "").strip(),
        parent_owner=str(parent_payload.get("owner") or "").strip(),
        child_causality=child_causality,
        plan_event_payload=plan_event_payload,
    )
    return {
        "plan_event": {
            "workflow_run_id": workflow_run_id,
            "operation_id": operation_id,
            "command_id": parent_command_id,
            "event_family": "workflow_event",
            "event_type": "CommandPlanRequested",
            "idempotency_key": progressed_child_plan_event_idempotency_key(idempotency_key),
            "actor": "company_public_web_phase_planner",
            "source": str(source or "company_public_web_refresh_owner").strip(),
            "payload": plan_event_payload,
            "artifact_refs": [],
        },
        "child_command": {
            "workflow_run_id": workflow_run_id,
            "operation_id": operation_id,
            "command_id": command_id_for(workflow_run_id, idempotency_key),
            "command_type": normalized_type,
            "owner": child_owner,
            "idempotency_key": idempotency_key,
            "parent_command_id": parent_command_id,
            "payload": payload,
            "artifact_refs": [],
            "not_before_at": "",
            "max_attempts": 3,
            "retry_policy": retry_policy,
        },
        "child_causality": child_causality,
    }


def canonical_progressed_child_identity(child: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the immutable identity of one decoded progressed-child row.

    ``child`` carries scalar columns plus materialized JSON fields
    (``input_artifact_refs``/``output_artifact_refs``/``produced_entity_counts``/
    ``payload``/``artifact_refs``/``retry_policy``).  The payload causality must
    mirror every causality column exactly (type-strict for JSON fields) and
    carry a current contract pin for a registered contract;
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
    pin = causality.get("progressed_child_contract")
    if not isinstance(pin, Mapping):
        return None
    contract_name = _exact_text(pin.get("name"))
    contract = progressed_child_completion_contract(contract_name)
    if contract is None or not json_contract_equal(dict(pin), progressed_child_contract_pin(contract_name)):
        return None
    identity = {field: _exact_text(child.get(field)) for field in _CHILD_IDENTITY_SCALAR_FIELDS}
    for field in _CHILD_CAUSALITY_SCALAR_MIRROR:
        if _exact_text(causality.get(field)) != identity[field]:
            return None
    if _exact_text(causality.get("schema_version")) != identity["causality_schema_version"]:
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
    identity["contract_name"] = contract_name
    identity["progressed_child_contract"] = dict(pin)
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
    committed ``source_event_id`` and the contract pin.  The result feeds
    ``canonical_progressed_child_identity`` so the expected and persisted
    sides are compared through the exact same canonicalization.
    """

    causality = dict(child_causality or {})
    child_payload = {**dict(child_command.get("payload") or {}), "causality": causality}
    return {
        "command_id": _exact_text(child_command.get("command_id")),
        "workflow_run_id": _exact_text(workflow_run_id),
        "operation_id": _exact_text(operation_id),
        "command_type": _exact_text(contract.get("child_command_type")),
        "owner": _exact_text(contract.get("child_owner")),
        "stage_id": _exact_text(causality.get("stage_id")),
        "causal_group_id": _exact_text(causality.get("causal_group_id")),
        "parent_command_id": _exact_text(parent_command_id),
        "source_event_id": _exact_text(causality.get("source_event_id")),
        "source_event_type": _exact_text(causality.get("source_event_type")),
        "input_artifact_refs": list(causality.get("input_artifact_refs") or []),
        "output_artifact_refs": list(causality.get("output_artifact_refs") or []),
        "produced_entity_counts": dict(causality.get("produced_entity_counts") or {}),
        "no_op_reason": _exact_text(causality.get("no_op_reason")),
        "readiness_effect": _exact_text(causality.get("readiness_effect")),
        "causality_schema_version": _exact_text(causality.get("schema_version") or "command_causality_v1"),
        "idempotency_key": _exact_text(child_command.get("idempotency_key")),
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
    expected_payload: Mapping[str, Any] | None,
) -> str:
    """Return ``""`` when ``event`` is the exact registered plan event of the child.

    ``event`` carries the decoded scalar columns plus ``payload`` (dict) and
    ``artifact_refs`` (list); ``expected_payload`` is the complete expected
    plan payload reconstructed through the registered pure builder and is
    exact-compared as a whole (foreign workflow types, substituted stage keys,
    and missing/extra keys never pass).  Every immutable event field is
    exact-compared: run/operation/command binding, empty activity attempt,
    family/type/schema, the registered ``<child-idempotency>:plan`` idempotency
    key, the pinned per-contract actor/source, a positive sequence ordered
    after the parent's own source event, and the deterministic ``evt_<sha1>``
    id.  Any drift returns a short reason token.
    """

    payload = event.get("payload")
    if not isinstance(payload, Mapping) or not isinstance(expected_payload, Mapping):
        return "event_payload_invalid"
    workflow_run_id = _exact_text(child_identity.get("workflow_run_id"))
    idempotency_key = _exact_text(child_identity.get("idempotency_key"))
    plan_idempotency_key = progressed_child_plan_event_idempotency_key(idempotency_key)
    parent_sequence = max(0, int(parent_source_sequence or 0))
    sequence = event.get("sequence_number")
    if parent_sequence <= 0 or type(sequence) is not int or sequence <= parent_sequence:
        return "event_sequence_invalid"
    if (
        _exact_text(event.get("event_id"))
        != progressed_child_plan_event_id(workflow_run_id, sequence, plan_idempotency_key)
        or _exact_text(event.get("idempotency_key")) != plan_idempotency_key
    ):
        return "event_idempotency_mismatch"
    if (
        _exact_text(event.get("workflow_run_id")) != workflow_run_id
        or _exact_text(event.get("operation_id")) != _exact_text(child_identity.get("operation_id"))
        or _exact_text(event.get("command_id")) != _exact_text(parent_command_id)
        or _exact_text(event.get("activity_attempt_id")) != ""
        or _exact_text(event.get("event_family")) != "workflow_event"
        or _exact_text(event.get("event_type")) != "CommandPlanRequested"
        or _exact_text(event.get("schema_version")) != "workflow_event_v1"
    ):
        return "event_column_mismatch"
    if _exact_text(event.get("actor")) != _exact_text(contract.get("child_plan_event_actor")) or _exact_text(
        event.get("source")
    ) != _exact_text(contract.get("child_plan_event_source")):
        return "event_actor_source_mismatch"
    if not json_contract_equal(list(event.get("artifact_refs") or []), []):
        return "event_artifact_refs_mismatch"
    if not json_contract_equal(dict(payload), dict(expected_payload)):
        return "event_payload_mismatch"
    return ""
