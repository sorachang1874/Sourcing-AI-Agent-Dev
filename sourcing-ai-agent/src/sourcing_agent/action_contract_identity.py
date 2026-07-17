"""Canonical fingerprints for versioned Operation action contracts.

The production ``ActionRegistry`` owns stable action, command, display, and
control semantics.  A request-schema successor may change only the versioned
request surface while retaining those base semantics.  This module combines
both owners into one deterministic digest without mutating the registry or
selecting an active version.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from .agent_contract_activation import ActionContractPin
from .operation_runtime import DEFAULT_ACTION_REGISTRY, ActionRegistry, ActionRequestSpec

ACTION_CONTRACT_FINGERPRINT_SCHEMA_VERSION = "operation_action_contract_fingerprint_v1"
ACTION_CONTRACT_MANIFEST_SCHEMA_VERSION = "operation_action_contract_manifest_v1"
ACTION_CONTRACT_FINGERPRINT_OWNER = "operation_runtime.versioned_action_contract_fingerprint"
ACTION_CONTRACT_FINGERPRINT_OWNER_REVISION = "versioned_action_contract_fingerprint_v1"

_BASE_CONTRACT_FIELDS = (
    "owner_module",
    "operation_type",
    "dispatch_adapter",
    "approval_policy",
    "budget_required",
    "description",
    "display_label",
    "display_category",
    "allowed_workflow_command_types",
    "default_workflow_command_type",
)


class ActionContractIdentityError(ValueError):
    """Raised when a versioned action fingerprint cannot be proven exact."""


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ActionContractIdentityError("action_contract_not_canonical_json") from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _request_contract_record(spec: ActionRequestSpec) -> dict[str, Any]:
    if spec.request_schema is None:
        return {
            "status": "schema_less",
            "schema_version": "",
            "schema_digest": "",
            "identity_target_fields": [],
            "target_ref_field_aliases": [],
        }
    return {
        "status": "schema_defined",
        "schema_version": spec.request_schema_version,
        "schema_digest": spec.request_schema_digest,
        "identity_target_fields": list(spec.request_identity_target_fields),
        "target_ref_field_aliases": [
            {"target_field": target_field, "aliases": list(aliases)}
            for target_field, aliases in spec.target_ref_field_aliases
        ],
    }


def _require_versioned_spec_matches_base(
    registry: ActionRegistry,
    spec: ActionRequestSpec,
) -> ActionRequestSpec:
    if not isinstance(registry, ActionRegistry) or not isinstance(spec, ActionRequestSpec):
        raise ActionContractIdentityError("action_contract_owner_invalid")
    try:
        base = registry.spec_for(spec.action_type)
    except KeyError as exc:
        raise ActionContractIdentityError("action_contract_action_missing") from exc
    drifted = [
        field_name for field_name in _BASE_CONTRACT_FIELDS if getattr(base, field_name) != getattr(spec, field_name)
    ]
    if drifted:
        raise ActionContractIdentityError("action_contract_base_semantics_mismatch:" + ",".join(drifted))
    return base


def build_action_contract_fingerprint(
    registry: ActionRegistry,
    spec: ActionRequestSpec,
) -> dict[str, Any]:
    """Build the complete versioned action contract record.

    ``spec`` may be the registered version or a request-schema successor.  All
    non-request fields must remain object-equal to the registered base action.
    """

    _require_versioned_spec_matches_base(registry, spec)
    try:
        registry_record = registry.to_record(include_command_contracts=True)[spec.action_type]
    except (KeyError, TypeError, ValueError) as exc:
        raise ActionContractIdentityError("action_contract_registry_record_missing") from exc
    record = {
        "schema_version": ACTION_CONTRACT_FINGERPRINT_SCHEMA_VERSION,
        "fingerprint_owner": ACTION_CONTRACT_FINGERPRINT_OWNER,
        "fingerprint_owner_revision": ACTION_CONTRACT_FINGERPRINT_OWNER_REVISION,
        "action_type": spec.action_type,
        "owner_module": spec.owner_module,
        "operation_type": spec.operation_type,
        "dispatch_adapter": spec.dispatch_adapter,
        "request": _request_contract_record(spec),
        "approval_policy": spec.approval_policy,
        "budget_required": spec.budget_required,
        "description": spec.description,
        "display_contract": dict(registry_record["display_contract"]),
        "allowed_workflow_command_types": list(spec.allowed_workflow_command_types),
        "default_workflow_command_type": spec.default_workflow_command_type,
        "workflow_command_exposure_gate": registry_record["workflow_command_exposure_gate"],
        "workflow_command_exposure_status": registry_record["workflow_command_exposure_status"],
        "allowed_workflow_command_contracts": list(registry_record.get("allowed_workflow_command_contracts") or []),
        "workflow_command_control_summary": dict(registry_record.get("workflow_command_control_summary") or {}),
        "default_workflow_command_contract": (
            dict(registry_record["default_workflow_command_contract"])
            if isinstance(registry_record.get("default_workflow_command_contract"), Mapping)
            else None
        ),
    }
    # Round-trip once so no MappingProxy/tuple/custom value can silently enter
    # a persisted fingerprint surface.
    return json.loads(_canonical_json(record))


def action_contract_fingerprint_digest(record: Mapping[str, Any]) -> str:
    if not isinstance(record, Mapping):
        raise ActionContractIdentityError("action_contract_fingerprint_invalid")
    candidate = dict(record)
    if (
        candidate.get("schema_version") != ACTION_CONTRACT_FINGERPRINT_SCHEMA_VERSION
        or candidate.get("fingerprint_owner") != ACTION_CONTRACT_FINGERPRINT_OWNER
        or candidate.get("fingerprint_owner_revision") != ACTION_CONTRACT_FINGERPRINT_OWNER_REVISION
    ):
        raise ActionContractIdentityError("action_contract_fingerprint_invalid")
    return _sha256_json(candidate)


def action_contract_digest(registry: ActionRegistry, spec: ActionRequestSpec) -> str:
    return action_contract_fingerprint_digest(build_action_contract_fingerprint(registry, spec))


def build_action_contract_pin(registry: ActionRegistry, spec: ActionRequestSpec) -> ActionContractPin:
    if spec.request_schema is None:
        raise ActionContractIdentityError("action_contract_request_schema_missing")
    return ActionContractPin(
        action_type=spec.action_type,
        request_schema_version=spec.request_schema_version,
        request_schema_digest=spec.request_schema_digest,
        action_contract_digest=action_contract_digest(registry, spec),
    )


def production_action_contract_manifest(
    registry: ActionRegistry = DEFAULT_ACTION_REGISTRY,
) -> Mapping[str, Any]:
    """Fingerprint every registered action, including visible schema-less debt."""

    if not isinstance(registry, ActionRegistry):
        raise ActionContractIdentityError("action_contract_owner_invalid")
    rows: list[dict[str, Any]] = []
    for action_type in sorted(registry.to_record(include_command_contracts=False)):
        spec = registry.spec_for(action_type)
        fingerprint = build_action_contract_fingerprint(registry, spec)
        rows.append(
            {
                "action_type": action_type,
                "request_status": fingerprint["request"]["status"],
                "action_contract_digest": action_contract_fingerprint_digest(fingerprint),
                "fingerprint": fingerprint,
            }
        )
    schema_defined_count = sum(row["request_status"] == "schema_defined" for row in rows)
    manifest: dict[str, Any] = {
        "schema_version": ACTION_CONTRACT_MANIFEST_SCHEMA_VERSION,
        "fingerprint_owner": ACTION_CONTRACT_FINGERPRINT_OWNER,
        "fingerprint_owner_revision": ACTION_CONTRACT_FINGERPRINT_OWNER_REVISION,
        "action_count": len(rows),
        "schema_defined_count": schema_defined_count,
        "schema_less_count": len(rows) - schema_defined_count,
        "actions": rows,
    }
    manifest["manifest_digest"] = _sha256_json(manifest)
    return _freeze_json(manifest)


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


__all__ = [
    "ACTION_CONTRACT_FINGERPRINT_OWNER",
    "ACTION_CONTRACT_FINGERPRINT_OWNER_REVISION",
    "ACTION_CONTRACT_FINGERPRINT_SCHEMA_VERSION",
    "ACTION_CONTRACT_MANIFEST_SCHEMA_VERSION",
    "ActionContractIdentityError",
    "action_contract_digest",
    "action_contract_fingerprint_digest",
    "build_action_contract_fingerprint",
    "build_action_contract_pin",
    "production_action_contract_manifest",
]
