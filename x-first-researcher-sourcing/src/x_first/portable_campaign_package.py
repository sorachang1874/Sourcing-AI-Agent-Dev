"""Provider-free package and semantic receipt publisher for fixture campaigns."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from x_first.recall_pool_schema import assert_schema_valid, contract_schema_sha256
from x_first.research_orchestration import (
    CATALOG_SCHEMA_FILE,
    PLAN_SCHEMA_FILE,
    POLICY_SCHEMA_FILE,
    REQUEST_SCHEMA_FILE,
    RESULT_SCHEMA_FILE,
    build_campaign_plan,
    canonical_sha256,
    validate_campaign_request,
    validate_campaign_result,
    validate_policy,
    validate_scope_catalog,
)
from x_first.selected_subject_adapter import (
    BINDING_SCHEMA_FILE,
    SELECTION_SCHEMA_FILE,
    validate_request_binding,
    validate_subject_selection,
)

PACKAGE_MANIFEST_SCHEMA_VERSION = "x.portable.research_campaign.package_manifest.v1"
SEMANTIC_RECEIPT_SCHEMA_VERSION = (
    "x.portable.research_campaign.semantic_validation_receipt.v1"
)
PACKAGE_MANIFEST_SCHEMA_FILE = "x.portable.research_campaign.package_manifest.v1.schema.json"
SEMANTIC_RECEIPT_SCHEMA_FILE = (
    "x.portable.research_campaign.semantic_validation_receipt.v1.schema.json"
)
VALIDATOR_ID = "x_first.portable_campaign.semantic_validator"
VALIDATOR_REVISION = "x_first.portable_campaign.semantic_validator.v1"

_ROLES = ("selection", "policy", "catalog", "request", "binding", "plan", "result")
_SCHEMA_FILES = {
    "selection": SELECTION_SCHEMA_FILE,
    "policy": POLICY_SCHEMA_FILE,
    "catalog": CATALOG_SCHEMA_FILE,
    "request": REQUEST_SCHEMA_FILE,
    "binding": BINDING_SCHEMA_FILE,
    "plan": PLAN_SCHEMA_FILE,
    "result": RESULT_SCHEMA_FILE,
}
_DECLARED_HASH_FIELDS = {
    "selection": "artifact_sha256",
    "policy": None,
    "catalog": "catalog_sha256",
    "request": "request_sha256",
    "binding": "binding_sha256",
    "plan": "plan_sha256",
    "result": "result_sha256",
}
_CHECKS = [
    "selection_semantics_valid",
    "request_binding_semantics_valid",
    "policy_semantics_valid",
    "catalog_semantics_valid",
    "request_semantics_valid",
    "canonical_plan_valid",
    "result_semantics_valid",
    "fixture_execution_only",
]
_MANIFEST_AUTHORITY = {
    "provider_calls_allowed": False,
    "model_calls_allowed": False,
    "product_writes_allowed": False,
    "live_authority": False,
}
_RECEIPT_AUTHORITY = {
    "live_authority": False,
    "promotion_authorized": False,
    "product_writes_allowed": False,
}


class PortableCampaignPackageError(ValueError):
    """Stable fail-closed package publication error."""


def _content_sha256(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _validate_timestamp(value: str) -> None:
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except (TypeError, ValueError) as exc:
        raise PortableCampaignPackageError("portable_package_validated_at_invalid") from exc


def validator_revision_sha256() -> str:
    source_root = Path(__file__).resolve().parent
    implementation_sources = (
        source_root / "research_orchestration.py",
        source_root / "selected_subject_adapter.py",
        source_root / "portable_campaign_package.py",
        source_root / "recall_pool_schema.py",
    )
    return canonical_sha256(
        {
            "validator_id": VALIDATOR_ID,
            "validator_revision": VALIDATOR_REVISION,
            "artifact_contract_schema_sha256s": {
                role: contract_schema_sha256(_SCHEMA_FILES[role]) for role in _ROLES
            },
            "package_manifest_contract_schema_sha256": contract_schema_sha256(
                PACKAGE_MANIFEST_SCHEMA_FILE
            ),
            "semantic_receipt_contract_schema_sha256": contract_schema_sha256(
                SEMANTIC_RECEIPT_SCHEMA_FILE
            ),
            "validator_implementation_source_sha256s": {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in implementation_sources
            },
        }
    )


def _validate_fixture_execution(result: Mapping[str, Any]) -> tuple[int, int]:
    attempt_count = 0
    for field in ("surface_attempts", "semantic_recall_attempts", "optional_channel_attempts"):
        for row in result[field]:
            if (
                row["source_status"] != "fixture_synthetic"
                or not row["receipt_ref"].startswith("fixture://")
            ):
                raise PortableCampaignPackageError("portable_package_not_fixture_only")
            attempt_count += 1
    for row in result["handle_resolution_attempts"]:
        receipt = row["retrieval_receipt"]
        if (
            row["source_status"] != "fixture_synthetic"
            or receipt["source_status"] != "fixture_synthetic"
            or receipt["receipt_locator"] is not None
        ):
            raise PortableCampaignPackageError("portable_package_not_fixture_only")
        attempt_count += 1
    evidence_count = 0
    for observation in result["observations"]:
        if (
            observation["source_status"] != "fixture_synthetic"
            or not observation["receipt_ref"].startswith("fixture://")
        ):
            raise PortableCampaignPackageError("portable_package_not_fixture_only")
        evidence_count += 1
    for evidence in result["handle_resolution_evidence"]:
        receipt = evidence["retrieval_receipt"]
        if (
            evidence["source_status"] != "fixture_synthetic"
            or receipt["source_status"] != "fixture_synthetic"
            or receipt["receipt_locator"] is not None
        ):
            raise PortableCampaignPackageError("portable_package_not_fixture_only")
        evidence_count += 1
    for evidence in result["optional_channel_evidence"]:
        if (
            evidence["source_status"] != "fixture_synthetic"
            or not evidence["receipt_ref"].startswith("fixture://")
        ):
            raise PortableCampaignPackageError("portable_package_not_fixture_only")
        evidence_count += 1
    return attempt_count, evidence_count


def _descriptor(role: str, payload: Mapping[str, Any]) -> dict[str, str]:
    declared_field = _DECLARED_HASH_FIELDS[role]
    declared_hash = (
        canonical_sha256(payload)
        if declared_field is None
        else str(payload[declared_field])
    )
    if declared_field is not None and declared_hash != _content_sha256(payload, declared_field):
        raise PortableCampaignPackageError(f"portable_package_{role}_hash_invalid")
    return {
        "schema_version": str(payload["schema_version"]),
        "contract_schema_sha256": contract_schema_sha256(_SCHEMA_FILES[role]),
        "payload_sha256": canonical_sha256(payload),
        "declared_content_sha256": declared_hash,
    }


def build_fixture_simulate_package(
    *,
    selection: Mapping[str, Any],
    policy: Mapping[str, Any],
    catalog: Mapping[str, Any],
    request: Mapping[str, Any],
    binding: Mapping[str, Any],
    plan: Mapping[str, Any],
    result: Mapping[str, Any],
    validated_at: str,
) -> dict[str, Any]:
    """Publish a receipt only after every semantic validator succeeds."""

    _validate_timestamp(validated_at)
    validate_subject_selection(selection)
    validate_policy(policy)
    validate_scope_catalog(catalog, policy=policy)
    validate_campaign_request(request, catalog=catalog, policy=policy)
    validate_request_binding(binding, selection=selection, request=request)
    expected_plan = build_campaign_plan(request=request, catalog=catalog, policy=policy)
    if plan != expected_plan:
        raise PortableCampaignPackageError("portable_package_plan_not_canonical")
    validate_campaign_result(
        result,
        request=request,
        plan=plan,
        catalog=catalog,
        policy=policy,
    )
    simulated_attempt_count, simulated_evidence_count = _validate_fixture_execution(result)
    artifacts = {
        "selection": copy.deepcopy(dict(selection)),
        "policy": copy.deepcopy(dict(policy)),
        "catalog": copy.deepcopy(dict(catalog)),
        "request": copy.deepcopy(dict(request)),
        "binding": copy.deepcopy(dict(binding)),
        "plan": copy.deepcopy(dict(plan)),
        "result": copy.deepcopy(dict(result)),
    }
    descriptors = {role: _descriptor(role, artifacts[role]) for role in _ROLES}
    package_id = f"package_{canonical_sha256({role: descriptors[role]['payload_sha256'] for role in _ROLES})[:24]}"
    manifest: dict[str, Any] = {
        "schema_version": PACKAGE_MANIFEST_SCHEMA_VERSION,
        "package_id": package_id,
        "package_mode": "fixture_simulate",
        "campaign_id": request["campaign_id"],
        "selection_id": selection["selection_id"],
        "artifacts": descriptors,
        "authority": dict(_MANIFEST_AUTHORITY),
        "manifest_sha256": "",
    }
    manifest["manifest_sha256"] = _content_sha256(manifest, "manifest_sha256")
    assert_schema_valid(manifest, PACKAGE_MANIFEST_SCHEMA_FILE)
    receipt: dict[str, Any] = {
        "schema_version": SEMANTIC_RECEIPT_SCHEMA_VERSION,
        "receipt_id": f"receipt_{manifest['manifest_sha256'][:24]}",
        "package_id": package_id,
        "manifest_sha256": manifest["manifest_sha256"],
        "campaign_id": request["campaign_id"],
        "selection_id": selection["selection_id"],
        "validator": {
            "validator_id": VALIDATOR_ID,
            "validator_revision": VALIDATOR_REVISION,
            "validator_revision_sha256": validator_revision_sha256(),
        },
        "validation_mode": "fixture_simulate",
        "validation_status": "passed",
        "validated_at": validated_at,
        "checks": list(_CHECKS),
        "artifact_payload_sha256s": {
            role: descriptors[role]["payload_sha256"] for role in _ROLES
        },
        "effects": {
            "simulated_external_attempt_count": simulated_attempt_count,
            "simulated_external_evidence_count": simulated_evidence_count,
            "provider_call_count": 0,
            "model_call_count": 0,
            "product_write_count": 0,
        },
        "authority": dict(_RECEIPT_AUTHORITY),
        "receipt_sha256": "",
    }
    receipt["receipt_sha256"] = _content_sha256(receipt, "receipt_sha256")
    assert_schema_valid(receipt, SEMANTIC_RECEIPT_SCHEMA_FILE)
    return {
        "manifest": manifest,
        "semantic_validation_receipt": receipt,
        "artifacts": artifacts,
    }


__all__ = [
    "PACKAGE_MANIFEST_SCHEMA_FILE",
    "PACKAGE_MANIFEST_SCHEMA_VERSION",
    "PortableCampaignPackageError",
    "SEMANTIC_RECEIPT_SCHEMA_FILE",
    "SEMANTIC_RECEIPT_SCHEMA_VERSION",
    "VALIDATOR_ID",
    "VALIDATOR_REVISION",
    "build_fixture_simulate_package",
    "validator_revision_sha256",
]
