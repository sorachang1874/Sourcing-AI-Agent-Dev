"""Hash-verified, fixture-only package boundary for portable X research.

The X-First producer remains responsible for deep campaign semantics.  This
consumer accepts its receipt only when a server-owned pin authenticates the
exact manifest, receipt, and validator revision.  None of those trust values
may be taken from an Agent or artifact payload.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PACKAGE_MANIFEST_SCHEMA_VERSION = "x.portable.research_campaign.package_manifest.v1"
SEMANTIC_RECEIPT_SCHEMA_VERSION = (
    "x.portable.research_campaign.semantic_validation_receipt.v1"
)
PACKAGE_MANIFEST_CONTRACT_SCHEMA_SHA256 = (
    "9c02c58623bba58567e7b0a9d5590888855143dd5b1bb2ef6a9bb07068b9a8c4"
)
SEMANTIC_RECEIPT_CONTRACT_SCHEMA_SHA256 = (
    "48dc2727c13fa1e23b98f7311cd318e3524f2ad7d6c2be700901d4fc0815b438"
)

_MANIFEST_SCHEMA_PATH = Path(
    "contracts/external/x_first/x.portable.research_campaign.package_manifest.v1.schema.json"
)
_RECEIPT_SCHEMA_PATH = Path(
    "contracts/external/x_first/"
    "x.portable.research_campaign.semantic_validation_receipt.v1.schema.json"
)
_FIXTURE_REGISTRY_PATH = Path("configs/x_first_fixture_semantic_validation_registry.v1.json")
FIXTURE_REGISTRY_SHA256 = "a4901921f7a142c85cd9a392fa1fc974df4e3c45ba8cf9ddc141abb527fba7c9"
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_IDENTIFIER_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_TIMESTAMP_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_ROLES = ("selection", "policy", "catalog", "request", "binding", "plan", "result")
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
_FALSE_CAMPAIGN_AUTHORITY = {
    "provider_calls_allowed": False,
    "product_writes_allowed": False,
    "canonical_person_merge_allowed": False,
    "outreach_allowed": False,
}
_FALSE_RESULT_AUTHORITY = {
    "product_writes_allowed": False,
    "canonical_person_write_allowed": False,
    "automatic_cross_source_merge_allowed": False,
    "outreach_allowed": False,
}


@dataclass(frozen=True, slots=True)
class _ArtifactContract:
    schema_version: str
    contract_schema_sha256: str
    declared_hash_field: str | None
    schema_path: Path


_ARTIFACT_CONTRACTS = {
    "selection": _ArtifactContract(
        "sourcing.x_first.subject_selection.v1",
        "86a5924d6b551b6af61cfccd9e94a426bdd046db4545e922dc8ae3bc5483b754",
        "artifact_sha256",
        Path("contracts/external/x_first/sourcing.x_first.subject_selection.v1.schema.json"),
    ),
    "policy": _ArtifactContract(
        "x.research_orchestration.policy.v1",
        "6a19698b1ca829453ce1a9c87fbfb0df694c0da3e9e047375d028f1c441b2575",
        None,
        Path("contracts/external/x_first/x.research_orchestration.policy.v1.schema.json"),
    ),
    "catalog": _ArtifactContract(
        "x.research_scope.catalog.v1",
        "9399fa128bbe9c5420e5c5e1988858b1201a9f24222435dcc0a775a922170bf9",
        "catalog_sha256",
        Path("contracts/external/x_first/x.research_scope.catalog.v1.schema.json"),
    ),
    "request": _ArtifactContract(
        "x.portable.research_campaign.request.v1",
        "b756a031a5cfca273ffc6b97b0d65dacc24811a0487a354a641c731122ce4c3b",
        "request_sha256",
        Path("contracts/external/x_first/x.portable.research_campaign.request.v1.schema.json"),
    ),
    "binding": _ArtifactContract(
        "x.portable.selected_subject.request_binding.v1",
        "ef2f6dc742658a5b6507f7320bc2ed5530612e97097eadf1c3e4a46405835c1d",
        "binding_sha256",
        Path("contracts/external/x_first/x.portable.selected_subject.request_binding.v1.schema.json"),
    ),
    "plan": _ArtifactContract(
        "x.portable.research_campaign.plan.v1",
        "f0dc7a78a8287f51f4d2a0543634079391d3816891c52f09cf4c87f402327543",
        "plan_sha256",
        Path("contracts/external/x_first/x.portable.research_campaign.plan.v1.schema.json"),
    ),
    "result": _ArtifactContract(
        "x.portable.research_campaign.result.v1",
        "31dbfc3d9f31a026df9fbe1d66fad38d00f91ff47f071533837adaa4f4f31403",
        "result_sha256",
        Path("contracts/external/x_first/x.portable.research_campaign.result.v1.schema.json"),
    ),
}


class XFirstPortablePackageError(ValueError):
    """Stable fail-closed package error."""


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _content_sha256(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _sha256(value: Any, error: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise XFirstPortablePackageError(error)
    return value


def _identifier(value: Any, error: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise XFirstPortablePackageError(error)
    return value


def _timestamp(value: Any, error: str) -> str:
    if not isinstance(value, str) or _TIMESTAMP_RE.fullmatch(value) is None:
        raise XFirstPortablePackageError(error)
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise XFirstPortablePackageError(error) from exc
    return value


def _pinned_local_schema(path: Path, expected: str, error: str) -> None:
    try:
        actual = hashlib.sha256((project_root() / path).read_bytes()).hexdigest()
    except OSError as exc:
        raise XFirstPortablePackageError(error) from exc
    if actual != expected:
        raise XFirstPortablePackageError(error)


@dataclass(frozen=True, slots=True)
class _TrustedSemanticValidationPin:
    """Capability minted only from the product-owned pinned fixture registry."""

    fixture_id: str
    manifest_sha256: str
    receipt_sha256: str
    validator_revision_sha256: str

    def __post_init__(self) -> None:
        _identifier(self.fixture_id, "x_first_trusted_pin_fixture_id_invalid")
        _sha256(self.manifest_sha256, "x_first_trusted_pin_manifest_invalid")
        _sha256(self.receipt_sha256, "x_first_trusted_pin_receipt_invalid")
        _sha256(self.validator_revision_sha256, "x_first_trusted_pin_revision_invalid")


@dataclass(frozen=True, slots=True)
class ExpectedSelectionSnapshot:
    workspace_ref: str
    projection_ref: str
    membership_revision: str
    selection_artifact_sha256: str

    def __post_init__(self) -> None:
        for value in (self.workspace_ref, self.projection_ref, self.membership_revision):
            if not isinstance(value, str) or not value or len(value) > 256:
                raise XFirstPortablePackageError("x_first_expected_selection_snapshot_invalid")
        _sha256(
            self.selection_artifact_sha256,
            "x_first_expected_selection_snapshot_invalid",
        )


_CAPABILITY_SEAL = object()


@dataclass(frozen=True, slots=True)
class _ValidatedPortableCampaignPackage:
    """Sealed capability backed only by immutable canonical JSON snapshots.

    The decoded mappings returned by the public properties are disposable
    copies.  A caller may mutate those copies, but cannot change the package
    state later consumed by the preview or fake owner.
    """

    _manifest_json: bytes
    _semantic_validation_receipt_json: bytes
    _artifacts_json: bytes
    trusted_fixture_id: str
    _seal: object

    def __post_init__(self) -> None:
        if self._seal is not _CAPABILITY_SEAL:
            raise XFirstPortablePackageError("x_first_validated_package_capability_invalid")
        for snapshot in (
            self._manifest_json,
            self._semantic_validation_receipt_json,
            self._artifacts_json,
        ):
            if not isinstance(snapshot, bytes):
                raise XFirstPortablePackageError(
                    "x_first_validated_package_snapshot_invalid"
                )

    @staticmethod
    def _decode_snapshot(snapshot: bytes) -> dict[str, Any]:
        decoded = json.loads(snapshot)
        if not isinstance(decoded, dict):
            raise XFirstPortablePackageError(
                "x_first_validated_package_snapshot_invalid"
            )
        return decoded

    @property
    def manifest(self) -> dict[str, Any]:
        return self._decode_snapshot(self._manifest_json)

    @property
    def semantic_validation_receipt(self) -> dict[str, Any]:
        return self._decode_snapshot(self._semantic_validation_receipt_json)

    @property
    def artifacts(self) -> dict[str, Any]:
        return self._decode_snapshot(self._artifacts_json)


def require_validated_package_capability(value: Any) -> _ValidatedPortableCampaignPackage:
    if type(value) is not _ValidatedPortableCampaignPackage or value._seal is not _CAPABILITY_SEAL:
        raise XFirstPortablePackageError("x_first_validated_package_capability_required")
    return value


def _load_trusted_fixture_pin(fixture_id: str) -> _TrustedSemanticValidationPin:
    _identifier(fixture_id, "x_first_fixture_id_invalid")
    path = project_root() / _FIXTURE_REGISTRY_PATH
    try:
        payload = path.read_bytes()
        registry = json.loads(payload)
    except (OSError, json.JSONDecodeError) as exc:
        raise XFirstPortablePackageError("x_first_fixture_registry_unavailable") from exc
    if hashlib.sha256(payload).hexdigest() != FIXTURE_REGISTRY_SHA256:
        raise XFirstPortablePackageError("x_first_fixture_registry_digest_mismatch")
    if (
        not isinstance(registry, Mapping)
        or set(registry) != {"schema_version", "registry_version", "fixtures", "authority"}
        or registry["schema_version"]
        != "sourcing.x_first.fixture_semantic_validation_registry.v1"
        or registry["registry_version"] != "x_first_fixture_semantic_validation_registry.v1"
        or registry["authority"]
        != {
            "caller_supplied_pin_allowed": False,
            "live_authority": False,
            "product_writes_allowed": False,
        }
        or not isinstance(registry["fixtures"], list)
    ):
        raise XFirstPortablePackageError("x_first_fixture_registry_invalid")
    matches = [row for row in registry["fixtures"] if isinstance(row, Mapping) and row.get("fixture_id") == fixture_id]
    if len(matches) != 1 or set(matches[0]) != {
        "fixture_id",
        "manifest_sha256",
        "receipt_sha256",
        "validator_revision_sha256",
    }:
        raise XFirstPortablePackageError("x_first_fixture_pin_missing_or_ambiguous")
    row = matches[0]
    return _TrustedSemanticValidationPin(
        fixture_id=row["fixture_id"],
        manifest_sha256=row["manifest_sha256"],
        receipt_sha256=row["receipt_sha256"],
        validator_revision_sha256=row["validator_revision_sha256"],
    )


def _normalize_package(value: Any) -> dict[str, Any]:
    try:
        normalized = json.loads(canonical_json(value))
    except (TypeError, ValueError) as exc:
        raise XFirstPortablePackageError("x_first_portable_package_not_json") from exc
    if not isinstance(normalized, dict):
        raise XFirstPortablePackageError("x_first_portable_package_not_object")
    return normalized


def _validate_manifest(manifest: Any) -> dict[str, Any]:
    if not isinstance(manifest, Mapping):
        raise XFirstPortablePackageError("x_first_package_manifest_not_object")
    record = dict(manifest)
    if set(record) != {
        "schema_version",
        "package_id",
        "package_mode",
        "campaign_id",
        "selection_id",
        "artifacts",
        "authority",
        "manifest_sha256",
    }:
        raise XFirstPortablePackageError("x_first_package_manifest_shape_invalid")
    if (
        record["schema_version"] != PACKAGE_MANIFEST_SCHEMA_VERSION
        or record["package_mode"] != "fixture_simulate"
        or record["authority"] != _MANIFEST_AUTHORITY
        or record["manifest_sha256"] != _content_sha256(record, "manifest_sha256")
    ):
        raise XFirstPortablePackageError("x_first_package_manifest_invalid")
    _identifier(record["package_id"], "x_first_package_manifest_invalid")
    _identifier(record["campaign_id"], "x_first_package_manifest_invalid")
    _identifier(record["selection_id"], "x_first_package_manifest_invalid")
    descriptors = record["artifacts"]
    if not isinstance(descriptors, Mapping) or set(descriptors) != set(_ROLES):
        raise XFirstPortablePackageError("x_first_package_manifest_roles_invalid")
    for descriptor in descriptors.values():
        if not isinstance(descriptor, Mapping) or set(descriptor) != {
            "schema_version",
            "contract_schema_sha256",
            "payload_sha256",
            "declared_content_sha256",
        }:
            raise XFirstPortablePackageError("x_first_package_manifest_descriptor_invalid")
        for field in (
            "contract_schema_sha256",
            "payload_sha256",
            "declared_content_sha256",
        ):
            _sha256(descriptor[field], "x_first_package_manifest_descriptor_invalid")
    return record


def _validate_receipt(
    receipt: Any,
    *,
    manifest: Mapping[str, Any],
    trusted_pin: _TrustedSemanticValidationPin,
) -> dict[str, Any]:
    if not isinstance(receipt, Mapping):
        raise XFirstPortablePackageError("x_first_semantic_receipt_not_object")
    record = dict(receipt)
    if set(record) != {
        "schema_version",
        "receipt_id",
        "package_id",
        "manifest_sha256",
        "campaign_id",
        "selection_id",
        "validator",
        "validation_mode",
        "validation_status",
        "validated_at",
        "checks",
        "artifact_payload_sha256s",
        "effects",
        "authority",
        "receipt_sha256",
    }:
        raise XFirstPortablePackageError("x_first_semantic_receipt_shape_invalid")
    validator = record["validator"]
    effects = record["effects"]
    if (
        not isinstance(validator, Mapping)
        or set(validator) != {"validator_id", "validator_revision", "validator_revision_sha256"}
        or validator["validator_id"] != "x_first.portable_campaign.semantic_validator"
        or not isinstance(validator["validator_revision"], str)
        or not validator["validator_revision"]
        or len(validator["validator_revision"]) > 128
        or record["schema_version"] != SEMANTIC_RECEIPT_SCHEMA_VERSION
        or record["package_id"] != manifest["package_id"]
        or record["manifest_sha256"] != manifest["manifest_sha256"]
        or record["campaign_id"] != manifest["campaign_id"]
        or record["selection_id"] != manifest["selection_id"]
        or record["validation_mode"] != "fixture_simulate"
        or record["validation_status"] != "passed"
        or record["checks"] != _CHECKS
        or record["authority"] != _RECEIPT_AUTHORITY
        or not isinstance(effects, Mapping)
        or set(effects)
        != {
            "simulated_external_attempt_count",
            "simulated_external_evidence_count",
            "provider_call_count",
            "model_call_count",
            "product_write_count",
        }
        or isinstance(effects["simulated_external_attempt_count"], bool)
        or not isinstance(effects["simulated_external_attempt_count"], int)
        or effects["simulated_external_attempt_count"] < 0
        or isinstance(effects["simulated_external_evidence_count"], bool)
        or not isinstance(effects["simulated_external_evidence_count"], int)
        or effects["simulated_external_evidence_count"] < 0
        or effects["provider_call_count"] != 0
        or effects["model_call_count"] != 0
        or effects["product_write_count"] != 0
        or record["receipt_sha256"] != _content_sha256(record, "receipt_sha256")
    ):
        raise XFirstPortablePackageError("x_first_semantic_receipt_invalid")
    _identifier(record["receipt_id"], "x_first_semantic_receipt_invalid")
    _timestamp(record["validated_at"], "x_first_semantic_receipt_invalid")
    _sha256(validator["validator_revision_sha256"], "x_first_semantic_receipt_invalid")
    payload_hashes = record["artifact_payload_sha256s"]
    if (
        not isinstance(payload_hashes, Mapping)
        or set(payload_hashes) != set(_ROLES)
        or dict(payload_hashes)
        != {
            role: manifest["artifacts"][role]["payload_sha256"] for role in _ROLES
        }
    ):
        raise XFirstPortablePackageError("x_first_semantic_receipt_artifacts_invalid")
    if (
        trusted_pin.manifest_sha256 != manifest["manifest_sha256"]
        or trusted_pin.receipt_sha256 != record["receipt_sha256"]
        or trusted_pin.validator_revision_sha256 != validator["validator_revision_sha256"]
    ):
        raise XFirstPortablePackageError("x_first_semantic_receipt_untrusted")
    return record


def _declared_content_sha256(role: str, payload: Mapping[str, Any]) -> str:
    contract = _ARTIFACT_CONTRACTS[role]
    if contract.declared_hash_field is None:
        return canonical_sha256(payload)
    declared = payload.get(contract.declared_hash_field)
    if declared != _content_sha256(payload, contract.declared_hash_field):
        raise XFirstPortablePackageError(f"x_first_package_{role}_content_hash_invalid")
    return _sha256(declared, f"x_first_package_{role}_content_hash_invalid")


def _validate_fixture_execution(result: Mapping[str, Any]) -> None:
    for field in ("surface_attempts", "semantic_recall_attempts", "optional_channel_attempts"):
        rows = result.get(field)
        if not isinstance(rows, list):
            raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
        for row in rows:
            if (
                not isinstance(row, Mapping)
                or row.get("source_status") != "fixture_synthetic"
                or not str(row.get("receipt_ref") or "").startswith("fixture://")
            ):
                raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    observations = result.get("observations")
    if not isinstance(observations, list):
        raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    for row in observations:
        if (
            not isinstance(row, Mapping)
            or row.get("source_status") != "fixture_synthetic"
            or not str(row.get("receipt_ref") or "").startswith("fixture://")
        ):
            raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    handle_evidence = result.get("handle_resolution_evidence")
    if not isinstance(handle_evidence, list):
        raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    for row in handle_evidence:
        receipt = row.get("retrieval_receipt") if isinstance(row, Mapping) else None
        if (
            not isinstance(row, Mapping)
            or row.get("source_status") != "fixture_synthetic"
            or not isinstance(receipt, Mapping)
            or receipt.get("source_status") != "fixture_synthetic"
            or receipt.get("receipt_locator") is not None
        ):
            raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    resolution_attempts = result.get("handle_resolution_attempts")
    if not isinstance(resolution_attempts, list):
        raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    for row in resolution_attempts:
        receipt = row.get("retrieval_receipt") if isinstance(row, Mapping) else None
        if (
            not isinstance(row, Mapping)
            or row.get("source_status") != "fixture_synthetic"
            or not isinstance(receipt, Mapping)
            or receipt.get("source_status") != "fixture_synthetic"
            or receipt.get("receipt_locator") is not None
        ):
            raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    optional_evidence = result.get("optional_channel_evidence")
    if not isinstance(optional_evidence, list):
        raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")
    for row in optional_evidence:
        if (
            not isinstance(row, Mapping)
            or row.get("source_status") != "fixture_synthetic"
            or not str(row.get("receipt_ref") or "").startswith("fixture://")
        ):
            raise XFirstPortablePackageError("x_first_package_fixture_execution_invalid")


def validate_x_first_portable_package(
    portable_package: Mapping[str, Any],
    *,
    fixture_id: str,
    expected_snapshot: ExpectedSelectionSnapshot,
) -> _ValidatedPortableCampaignPackage:
    """Validate one exact fixture package and its out-of-band trust binding."""

    if type(expected_snapshot) is not ExpectedSelectionSnapshot:
        raise XFirstPortablePackageError("x_first_expected_selection_snapshot_required")
    trusted_pin = _load_trusted_fixture_pin(fixture_id)
    _pinned_local_schema(
        _MANIFEST_SCHEMA_PATH,
        PACKAGE_MANIFEST_CONTRACT_SCHEMA_SHA256,
        "x_first_package_manifest_local_schema_mismatch",
    )
    _pinned_local_schema(
        _RECEIPT_SCHEMA_PATH,
        SEMANTIC_RECEIPT_CONTRACT_SCHEMA_SHA256,
        "x_first_semantic_receipt_local_schema_mismatch",
    )
    for role, contract in _ARTIFACT_CONTRACTS.items():
        _pinned_local_schema(
            contract.schema_path,
            contract.contract_schema_sha256,
            f"x_first_package_{role}_local_schema_mismatch",
        )
    package = _normalize_package(portable_package)
    if set(package) != {"manifest", "semantic_validation_receipt", "artifacts"}:
        raise XFirstPortablePackageError("x_first_portable_package_shape_invalid")
    manifest = _validate_manifest(package["manifest"])
    artifacts = package["artifacts"]
    if not isinstance(artifacts, Mapping) or set(artifacts) != set(_ROLES):
        raise XFirstPortablePackageError("x_first_portable_package_roles_invalid")
    normalized_artifacts: dict[str, Mapping[str, Any]] = {}
    for role in _ROLES:
        payload = artifacts[role]
        descriptor = manifest["artifacts"][role]
        contract = _ARTIFACT_CONTRACTS[role]
        if not isinstance(payload, Mapping):
            raise XFirstPortablePackageError(f"x_first_package_{role}_not_object")
        if (
            payload.get("schema_version") != contract.schema_version
            or descriptor["schema_version"] != contract.schema_version
            or descriptor["contract_schema_sha256"] != contract.contract_schema_sha256
            or descriptor["payload_sha256"] != canonical_sha256(payload)
            or descriptor["declared_content_sha256"]
            != _declared_content_sha256(role, payload)
        ):
            raise XFirstPortablePackageError(f"x_first_package_{role}_descriptor_invalid")
        normalized_artifacts[role] = dict(payload)

    selection = normalized_artifacts["selection"]
    snapshot = selection.get("snapshot")
    if (
        not isinstance(snapshot, Mapping)
        or snapshot.get("workspace_ref") != expected_snapshot.workspace_ref
        or snapshot.get("projection_ref") != expected_snapshot.projection_ref
        or snapshot.get("membership_revision") != expected_snapshot.membership_revision
        or selection.get("artifact_sha256") != expected_snapshot.selection_artifact_sha256
        or manifest["selection_id"] != selection.get("selection_id")
    ):
        raise XFirstPortablePackageError("x_first_package_selection_snapshot_rebound")

    # Local import avoids a top-level cycle: the adapter imports this validator
    # only when constructing a preview.
    from sourcing_agent.x_first_portable_adapter import (
        validate_request_binding_artifacts,
        validate_subject_selection_artifact,
    )

    validate_subject_selection_artifact(selection)
    validate_request_binding_artifacts(
        selection=selection,
        request_binding=normalized_artifacts["binding"],
        portable_request=normalized_artifacts["request"],
    )
    request = normalized_artifacts["request"]
    policy = normalized_artifacts["policy"]
    catalog = normalized_artifacts["catalog"]
    binding = normalized_artifacts["binding"]
    plan = normalized_artifacts["plan"]
    result = normalized_artifacts["result"]
    if (
        request.get("authority") != _FALSE_CAMPAIGN_AUTHORITY
        or binding.get("authority") != _FALSE_CAMPAIGN_AUTHORITY
        or result.get("authority") != _FALSE_RESULT_AUTHORITY
        or manifest["campaign_id"] != request.get("campaign_id")
        or binding.get("campaign_id") != request.get("campaign_id")
        or plan.get("campaign_id") != request.get("campaign_id")
        or result.get("campaign_id") != request.get("campaign_id")
        or plan.get("request_sha256") != request.get("request_sha256")
        or result.get("request_sha256") != request.get("request_sha256")
        or plan.get("catalog_sha256") != catalog.get("catalog_sha256")
        or result.get("catalog_sha256") != catalog.get("catalog_sha256")
        or plan.get("policy_sha256") != canonical_sha256(policy)
        or result.get("plan_sha256") != plan.get("plan_sha256")
    ):
        raise XFirstPortablePackageError("x_first_package_binding_graph_invalid")
    _validate_fixture_execution(result)
    receipt = _validate_receipt(
        package["semantic_validation_receipt"],
        manifest=manifest,
        trusted_pin=trusted_pin,
    )
    expected_attempt_count = sum(
        len(result[field])
        for field in (
            "surface_attempts",
            "semantic_recall_attempts",
            "optional_channel_attempts",
            "handle_resolution_attempts",
        )
    )
    expected_evidence_count = sum(
        len(result[field])
        for field in ("observations", "handle_resolution_evidence", "optional_channel_evidence")
    )
    if (
        receipt["effects"]["simulated_external_attempt_count"] != expected_attempt_count
        or receipt["effects"]["simulated_external_evidence_count"]
        != expected_evidence_count
    ):
        raise XFirstPortablePackageError("x_first_semantic_receipt_effect_counts_invalid")
    return _ValidatedPortableCampaignPackage(
        _manifest_json=canonical_json(manifest).encode("utf-8"),
        _semantic_validation_receipt_json=canonical_json(receipt).encode("utf-8"),
        _artifacts_json=canonical_json(normalized_artifacts).encode("utf-8"),
        trusted_fixture_id=trusted_pin.fixture_id,
        _seal=_CAPABILITY_SEAL,
    )


__all__ = [
    "ExpectedSelectionSnapshot",
    "PACKAGE_MANIFEST_CONTRACT_SCHEMA_SHA256",
    "PACKAGE_MANIFEST_SCHEMA_VERSION",
    "SEMANTIC_RECEIPT_CONTRACT_SCHEMA_SHA256",
    "SEMANTIC_RECEIPT_SCHEMA_VERSION",
    "XFirstPortablePackageError",
    "canonical_json",
    "canonical_sha256",
    "require_validated_package_capability",
    "validate_x_first_portable_package",
]
