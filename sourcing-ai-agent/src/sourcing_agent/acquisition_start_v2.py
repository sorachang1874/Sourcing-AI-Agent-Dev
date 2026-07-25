"""Pure D1n V2 contracts for starting an acquisition from an exact preview.

This module is an integration leaf.  It defines the closed v2 request, exact
preview-owner preflight, immutable start snapshot, human confirmation receipt,
root-command payload, and model-safe result schema.  It intentionally does not
register the request/result, expose a served tool, write an action/run/command,
or call a provider/model/network transport.

The future PG integration owner must invoke ``AcquisitionStartV2OwnerBinder``
before the first write and must invoke ``confirm_exact_action`` inside the
approval UoW before the OperationRun, command, or budget writes.  The reader
protocol is deliberately owner-scoped so missing and foreign previews have the
same externally observable result.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any, Literal, Protocol, TypeAlias, cast

from .acquisition_plan_preview import (
    ACQUISITION_PLAN_PREVIEW_RESULT_SPEC,
    MAX_PREVIEW_COST_MICRO_USD,
    MAX_PREVIEW_ELAPSED_SECONDS,
    MAX_PREVIEW_OUTPUT_CANDIDATES,
    MAX_PREVIEW_PROVIDER_ITEMS,
    AcquisitionPlanPreview,
    AcquisitionPlanPreviewError,
)
from .action_request_schema import DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER
from .action_result_schema import (
    ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION,
    ACTION_RESULT_VALIDATOR_OWNER,
    ActionResultActionOwner,
    ActionResultSchemaError,
    ActionResultSpec,
    ActionResultValueRole,
)
from .agent_tool_registry import AgentToolOwnerPin
from .cohort_provider_compiler import MAX_COHORT_PROVIDER_LANES
from .model_tool_runtime import InternalToolValidatorSpec, ModelToolSchemaError, ToolSpec

ACQUISITION_START_ACTION_TYPE = "start_acquisition_run"
ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION = "acquisition_root_request_v2"
ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION = "acquisition_start_snapshot.v2"
ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION = "acquisition_confirmation_receipt.v1"
ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION = "acquisition_root_command_payload.v2"
ACQUISITION_START_V2_ROOT_COMMAND_TYPE = "acquisition.run.create"
ACQUISITION_START_V2_RESULT_SCHEMA_VERSION = "acquisition_start_result_v2"
ACQUISITION_START_V2_RESULT_SERIALIZER_OWNER = "acquisition.start_result_serializer_v2"
ACQUISITION_START_V2_RESULT_SERIALIZER_REVISION = "acquisition_start_result_serializer_v2"

ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT = "acquisition_start_preview_not_found_or_conflict"
ACQUISITION_START_V2_REQUEST_INVALID = "acquisition_start_v2_request_invalid"
ACQUISITION_START_V2_APPROVAL_INVALID = "acquisition_start_v2_approval_invalid"
ACQUISITION_START_V2_RECEIPT_INVALID = "acquisition_confirmation_receipt_invalid"
ACQUISITION_START_V2_PARENT_BUDGET_INVALID = "acquisition_start_v2_parent_budget_envelope_invalid"
ACQUISITION_START_V2_COMMAND_INVALID = "acquisition_start_v2_root_command_invalid"

ACQUISITION_PARENT_BUDGET_FIELDS = (
    "max_provider_calls",
    "max_provider_items",
    "max_output_candidates",
    "max_cost_micro_usd",
    "max_elapsed_seconds",
)
_ACQUISITION_PARENT_BUDGET_ENVELOPE_REF_FIELDS = (
    "owner_id",
    "owner_revision",
    "owner_contract_digest",
    "confirmation_receipt_id",
    "confirmation_receipt_digest",
    "budget_digest",
)

MAX_START_V2_REVISION = 9_223_372_036_854_775_807
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,199}")
_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_REASON_PATTERN = re.compile(r"[a-z0-9][a-z0-9_]{0,79}")
_UTC_TIMESTAMP_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_NONEMPTY_TEXT_PATTERN = r"^(?s:.*\S.*)$"
_ID_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$"
_VERSION_SCHEMA_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
_SHA256_SCHEMA_PATTERN = r"^[0-9a-f]{64}$"
_REASON_SCHEMA_PATTERN = r"^[a-z0-9][a-z0-9_]{0,79}$"

ApprovalActorKind: TypeAlias = Literal["authenticated_user", "open_operator"]


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2Error(ValueError):
    """Stable fail-closed error raised by the pure V2 contract."""

    code: str
    detail: str = ""

    def __str__(self) -> str:
        return self.code


def _copy_json_tree(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _copy_json_tree(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_copy_json_tree(child) for child in value]
    return value


def _closed_object(
    properties: Mapping[str, Any],
    *,
    required: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    copied = {str(key): _copy_json_tree(value) for key, value in properties.items()}
    return {
        "type": "object",
        "properties": copied,
        "required": list(copied) if required is None else list(required),
        "additionalProperties": False,
    }


def _string_schema(*, maximum: int, minimum: int = 1, pattern: str = "") -> dict[str, Any]:
    result: dict[str, Any] = {"type": "string", "minLength": minimum, "maxLength": maximum}
    if pattern:
        result["pattern"] = pattern
    return result


def _id_schema() -> dict[str, Any]:
    return _string_schema(maximum=200, pattern=_ID_SCHEMA_PATTERN)


def _version_schema() -> dict[str, Any]:
    return _string_schema(maximum=128, pattern=_VERSION_SCHEMA_PATTERN)


def _sha256_schema() -> dict[str, Any]:
    return _string_schema(maximum=64, minimum=64, pattern=_SHA256_SCHEMA_PATTERN)


def _preview_reference_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "preview_id": _id_schema(),
            "preview_revision": {"type": "integer", "minimum": 1, "maximum": MAX_START_V2_REVISION},
            "preview_digest": _sha256_schema(),
        }
    )


_START_V2_REFERENCE_VALIDATOR = InternalToolValidatorSpec(
    name="start_acquisition_run:v2:preview_reference",
    description="Validate the only caller-owned fields accepted by start-acquisition v2.",
    input_schema=_preview_reference_schema(),
    schema_version=ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
)


def _result_variant_schema(variant: str) -> dict[str, Any]:
    if variant == "success":
        return _closed_object(
            {
                "variant": {"type": "string", "const": "success"},
                "status": {"type": "string", "const": "accepted"},
                "action_id": _id_schema(),
                "operation_run_id": _id_schema(),
                "workflow_command_id": _id_schema(),
                "preview_id": _id_schema(),
                "preview_revision": {"type": "integer", "minimum": 1, "maximum": MAX_START_V2_REVISION},
                "preview_digest": _sha256_schema(),
                "confirmation_receipt_id": _id_schema(),
                "confirmation_receipt_digest": _sha256_schema(),
            }
        )
    if variant == "deferred":
        return _closed_object(
            {
                "variant": {"type": "string", "const": "deferred"},
                "status": {"type": "string", "const": "deferred"},
                "reason": _string_schema(maximum=80, pattern=_REASON_SCHEMA_PATTERN),
                "retryable": {"type": "boolean"},
            }
        )
    return _closed_object(
        {
            "variant": {"type": "string", "const": "error"},
            "status": {"type": "string", "const": "failed"},
            "reason": _string_schema(maximum=80, pattern=_REASON_SCHEMA_PATTERN),
            "retryable": {"type": "boolean", "const": False},
        }
    )


def _schema_field_paths(schema: Mapping[str, Any], *, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if schema.get("type") == "object":
        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            for name, child in properties.items():
                if type(name) is not str or not isinstance(child, Mapping):
                    continue
                token = name.replace("~", "~0").replace("/", "~1")
                path = f"{prefix}/{token}"
                paths.add(path)
                paths.update(_schema_field_paths(child, prefix=path))
    elif schema.get("type") == "array" and isinstance(schema.get("items"), Mapping):
        paths.update(_schema_field_paths(cast(Mapping[str, Any], schema["items"]), prefix=f"{prefix}/*"))
    return paths


def _schema_string_paths(schema: Mapping[str, Any], *, prefix: str = "") -> set[str]:
    if schema.get("type") == "string":
        return {prefix}
    paths: set[str] = set()
    if schema.get("type") == "object":
        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            for name, child in properties.items():
                if type(name) is not str or not isinstance(child, Mapping):
                    continue
                token = name.replace("~", "~0").replace("/", "~1")
                paths.update(_schema_string_paths(child, prefix=f"{prefix}/{token}"))
    elif schema.get("type") == "array" and isinstance(schema.get("items"), Mapping):
        paths.update(_schema_string_paths(cast(Mapping[str, Any], schema["items"]), prefix=f"{prefix}/*"))
    return paths


_RESULT_VARIANT_SCHEMAS = {variant: _result_variant_schema(variant) for variant in ("success", "deferred", "error")}
_RESULT_FIELD_PROVENANCE = {
    variant: {
        path: "server_derived" if path in {"/variant", "/status"} else "owner_state"
        for path in sorted(_schema_field_paths(schema))
    }
    for variant, schema in _RESULT_VARIANT_SCHEMAS.items()
}
_RESULT_FIELD_VALUE_ROLES: dict[str, dict[str, ActionResultValueRole]] = {
    variant: {
        path: cast(ActionResultValueRole, "control" if path in {"/variant", "/status"} else "identifier")
        for path in sorted(_schema_string_paths(schema))
    }
    for variant, schema in _RESULT_VARIANT_SCHEMAS.items()
}

ACQUISITION_START_V2_RESULT_SPEC = ActionResultSpec(
    tool_name=ACQUISITION_START_ACTION_TYPE,
    tool_kind="action",
    owner_binding=ActionResultActionOwner(action_type=ACQUISITION_START_ACTION_TYPE),
    result_schema_version=ACQUISITION_START_V2_RESULT_SCHEMA_VERSION,
    serializer_owner=ACQUISITION_START_V2_RESULT_SERIALIZER_OWNER,
    serializer_revision=ACQUISITION_START_V2_RESULT_SERIALIZER_REVISION,
    serializer_contract={
        "schema_version": "acquisition_start_result_serializer_contract_v2",
        "owner_output": "closed_terminal_variant",
        "success_source": "exact_start_command_acceptance",
        "deferred_error_source": "typed_owner_result",
    },
    validator_owner=ACTION_RESULT_VALIDATOR_OWNER,
    variant_schemas=_RESULT_VARIANT_SCHEMAS,
    field_provenance=_RESULT_FIELD_PROVENANCE,
    field_value_roles=_RESULT_FIELD_VALUE_ROLES,
    max_serialized_bytes=8 * 1024,
    max_items=128,
    max_depth=4,
    artifact_ref_schemes=(),
)


def _preview_record_schema() -> dict[str, Any]:
    success_schema = ACQUISITION_PLAN_PREVIEW_RESULT_SPEC.variant_schemas["success"]
    properties = success_schema.get("properties")
    if not isinstance(properties, Mapping) or not isinstance(properties.get("preview"), Mapping):
        raise RuntimeError("acquisition plan preview result spec does not expose a preview schema")
    return cast(dict[str, Any], _copy_json_tree(properties["preview"]))


def _request_pin_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {
                "type": "string",
                "const": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
            },
            "schema_digest": _sha256_schema(),
        }
    )


def _result_pin_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {"type": "string", "const": ACQUISITION_START_V2_RESULT_SCHEMA_VERSION},
            "schema_digest": _sha256_schema(),
            "serializer_owner": {
                "type": "string",
                "const": ACQUISITION_START_V2_RESULT_SERIALIZER_OWNER,
            },
            "serializer_revision": {
                "type": "string",
                "const": ACQUISITION_START_V2_RESULT_SERIALIZER_REVISION,
            },
            "serializer_contract_digest": _sha256_schema(),
            "interpretation_contract_version": {
                "type": "string",
                "const": ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION,
            },
            "interpretation_contract_digest": _sha256_schema(),
        }
    )


def _tool_pin_schema() -> dict[str, Any]:
    return _closed_object({"tool_spec_version": _version_schema(), "tool_spec_digest": _sha256_schema()})


def _start_snapshot_schema() -> dict[str, Any]:
    return _closed_object(
        {
            "schema_version": {
                "type": "string",
                "const": ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION,
            },
            "preview": _preview_record_schema(),
            "request_pins": _request_pin_schema(),
            "result_pins": _result_pin_schema(),
            "tool_pins": _tool_pin_schema(),
            "snapshot_digest": _sha256_schema(),
        }
    )


def _build_start_request_schema() -> dict[str, Any]:
    return DEFAULT_ACTION_REQUEST_SCHEMA_BUILDER.build(
        input_properties=_preview_reference_schema()["properties"],
        input_required=("preview_id", "preview_revision", "preview_digest"),
        target_properties={
            "workspace_id": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_PATTERN),
            "requester_id": _string_schema(maximum=200, pattern=_NONEMPTY_TEXT_PATTERN),
            "start_snapshot": _start_snapshot_schema(),
        },
        target_required=("workspace_id", "requester_id", "start_snapshot"),
    )


ACQUISITION_START_V2_REQUEST_TOOL_SPEC = ToolSpec(
    name=ACQUISITION_START_ACTION_TYPE,
    description="Start an acquisition only from an exact immutable plan preview.",
    input_schema=_build_start_request_schema(),
    schema_version=ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
    approval_policy="required",
    budget_required=True,
)
ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST = ACQUISITION_START_V2_REQUEST_TOOL_SPEC.input_schema_digest


def acquisition_start_v2_request_schema() -> dict[str, Any]:
    """Return a defensive mutable copy of the complete persisted request schema."""

    return cast(dict[str, Any], _thaw_json(ACQUISITION_START_V2_REQUEST_TOOL_SPEC.input_schema))


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2ToolPins:
    """Exact F3-owned tool identity supplied by the future integration owner."""

    tool_spec_version: str
    tool_spec_digest: str

    def __post_init__(self) -> None:
        _require_version(self.tool_spec_version, field="tool_spec_version")
        _require_sha256(self.tool_spec_digest, field="tool_spec_digest")

    def to_record(self) -> dict[str, str]:
        return {
            "tool_spec_version": self.tool_spec_version,
            "tool_spec_digest": self.tool_spec_digest,
        }


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2BindContext:
    """Transport-derived owner scope; never accepted from model arguments."""

    workspace_id: str
    requester_id: str

    def __post_init__(self) -> None:
        _require_owner_identity(self.workspace_id, field="workspace_id")
        _require_owner_identity(self.requester_id, field="requester_id")


class AcquisitionPlanPreviewOwnerReader(Protocol):
    """Owner-scoped immutable preview reader implemented by the future PG UoW."""

    def get_acquisition_plan_preview(
        self,
        preview_id: str,
        *,
        workspace_id: str,
        requester_id: str,
        preview_revision: int,
        preview_digest: str,
    ) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2Snapshot:
    """Deeply immutable exact-copy snapshot stored on action/run/command."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        record = _strict_mapping_copy(self._record, code=ACQUISITION_START_V2_REQUEST_INVALID)
        try:
            validator = InternalToolValidatorSpec(
                name="start_acquisition_run:v2:snapshot",
                description="Validate one exact acquisition-start snapshot.",
                input_schema=_start_snapshot_schema(),
                schema_version=ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION,
            )
            validated = validator.validate_input(record)
        except ModelToolSchemaError as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID) from exc
        preview = _validated_preview(validated.get("preview"))
        _validate_preview_start_pins(preview)
        request_pins = dict(validated.get("request_pins") or {})
        expected_request_pins = {
            "schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
            "schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
        }
        if request_pins != expected_request_pins:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        result_pins = dict(validated.get("result_pins") or {})
        if result_pins != _result_pins_record():
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        AcquisitionStartV2ToolPins(**dict(validated.get("tool_pins") or {}))
        expected_digest = _sha256_json({key: value for key, value in validated.items() if key != "snapshot_digest"})
        if validated.get("snapshot_digest") != expected_digest:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        object.__setattr__(self, "_record", _freeze_json(validated))

    @property
    def snapshot_digest(self) -> str:
        return str(self._record["snapshot_digest"])

    @property
    def preview(self) -> AcquisitionPlanPreview:
        return _validated_preview(self._record["preview"])

    @property
    def tool_pins(self) -> AcquisitionStartV2ToolPins:
        pins = cast(Mapping[str, Any], self._record["tool_pins"])
        return AcquisitionStartV2ToolPins(
            tool_spec_version=str(pins["tool_spec_version"]),
            tool_spec_digest=str(pins["tool_spec_digest"]),
        )

    def to_record(self) -> dict[str, Any]:
        return cast(dict[str, Any], _thaw_json(self._record))

    @classmethod
    def build(
        cls,
        preview: AcquisitionPlanPreview,
        *,
        tool_pins: AcquisitionStartV2ToolPins,
    ) -> AcquisitionStartV2Snapshot:
        if not isinstance(preview, AcquisitionPlanPreview) or not isinstance(tool_pins, AcquisitionStartV2ToolPins):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        record: dict[str, Any] = {
            "schema_version": ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION,
            "preview": preview.to_record(),
            "request_pins": {
                "schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
                "schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
            },
            "result_pins": _result_pins_record(),
            "tool_pins": tool_pins.to_record(),
        }
        record["snapshot_digest"] = _sha256_json(record)
        return cls(record)


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2BoundRequest:
    """Canonical persisted request minted after exact owner preflight."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        record = _strict_mapping_copy(self._record, code=ACQUISITION_START_V2_REQUEST_INVALID)
        try:
            validated = ACQUISITION_START_V2_REQUEST_TOOL_SPEC.validate_input(record)
        except ModelToolSchemaError as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID) from exc
        target = dict(validated.get("target_ref") or {})
        snapshot = AcquisitionStartV2Snapshot(cast(Mapping[str, Any], target.get("start_snapshot") or {}))
        preview = snapshot.preview.to_record()
        input_payload = dict(validated.get("input_payload") or {})
        if (
            input_payload
            != {
                "preview_id": preview["preview_id"],
                "preview_revision": preview["preview_revision"],
                "preview_digest": preview["preview_digest"],
            }
            or target.get("workspace_id") != preview["workspace_id"]
            or target.get("requester_id") != preview["requester_id"]
        ):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        object.__setattr__(self, "_record", _freeze_json(validated))

    @property
    def input_payload(self) -> Mapping[str, Any]:
        return cast(Mapping[str, Any], self._record["input_payload"])

    @property
    def target_ref(self) -> Mapping[str, Any]:
        return cast(Mapping[str, Any], self._record["target_ref"])

    @property
    def snapshot(self) -> AcquisitionStartV2Snapshot:
        return AcquisitionStartV2Snapshot(cast(Mapping[str, Any], self.target_ref["start_snapshot"]))

    def to_record(self) -> dict[str, Any]:
        return cast(dict[str, Any], _thaw_json(self._record))


class AcquisitionStartV2OwnerBinder:
    """Reload and bind one preview before any action/run/command/budget write."""

    def __init__(self, preview_reader: AcquisitionPlanPreviewOwnerReader) -> None:
        if not hasattr(preview_reader, "get_acquisition_plan_preview"):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        self._preview_reader = preview_reader

    def bind(
        self,
        *,
        input_payload: Mapping[str, Any],
        context: AcquisitionStartV2BindContext,
        tool_pins: AcquisitionStartV2ToolPins,
        now: datetime,
    ) -> AcquisitionStartV2BoundRequest:
        if not isinstance(context, AcquisitionStartV2BindContext) or not isinstance(
            tool_pins, AcquisitionStartV2ToolPins
        ):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        try:
            candidate_input = _strict_mapping_copy(input_payload, code=ACQUISITION_START_V2_REQUEST_INVALID)
            validated_input = _START_V2_REFERENCE_VALIDATOR.validate_input(candidate_input)
        except (AcquisitionStartV2Error, ModelToolSchemaError) as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID) from exc
        preview_id = str(validated_input["preview_id"])
        try:
            raw_preview = self._preview_reader.get_acquisition_plan_preview(
                preview_id,
                workspace_id=context.workspace_id,
                requester_id=context.requester_id,
                preview_revision=int(validated_input["preview_revision"]),
                preview_digest=str(validated_input["preview_digest"]),
            )
        except Exception as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT) from exc
        if raw_preview is None:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)
        try:
            preview = _validated_preview(raw_preview)
            preview_record = preview.to_record()
            canonical_now = _require_aware_datetime(now)
            created_at = _parse_utc_timestamp(str(preview_record["created_at"]))
            expires_at = _parse_utc_timestamp(str(preview_record["expires_at"]))
            _validate_preview_start_pins(preview)
            if (
                preview_record["workspace_id"] != context.workspace_id
                or preview_record["requester_id"] != context.requester_id
                or validated_input
                != {
                    "preview_id": preview.preview_id,
                    "preview_revision": preview.preview_revision,
                    "preview_digest": preview.preview_digest,
                }
                or canonical_now < created_at
                or canonical_now >= expires_at
            ):
                raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)
            snapshot = AcquisitionStartV2Snapshot.build(preview, tool_pins=tool_pins)
            return AcquisitionStartV2BoundRequest(
                {
                    "input_payload": dict(validated_input),
                    "target_ref": {
                        "workspace_id": context.workspace_id,
                        "requester_id": context.requester_id,
                        "start_snapshot": snapshot.to_record(),
                    },
                }
            )
        except AcquisitionStartV2Error as exc:
            if exc.code == ACQUISITION_START_V2_REQUEST_INVALID:
                raise
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT) from exc
        except (AcquisitionPlanPreviewError, KeyError, TypeError, ValueError) as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT) from exc

    def confirm_exact_action(
        self,
        *,
        persisted_action: Mapping[str, Any],
        context: AcquisitionStartV2BindContext,
        approval_actor_id: str,
        approval_actor_kind: str,
        receipt_id: str,
        approval_policy_revision: str,
        approved_at: str,
    ) -> AcquisitionConfirmationReceipt:
        """Re-read the preview and compare the exact pending action before UoW writes."""

        actor_id = _require_owner_identity(approval_actor_id, field="approval_actor_id")
        if approval_actor_kind not in {"authenticated_user", "open_operator"}:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_APPROVAL_INVALID)
        _require_identifier(receipt_id, field="receipt_id")
        _require_version(approval_policy_revision, field="approval_policy_revision")
        approved_time = _parse_utc_timestamp(approved_at)
        try:
            action = _strict_mapping_copy(
                persisted_action,
                code=ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
            )
            if set(action) != {
                "action_id",
                "action_type",
                "workspace_id",
                "requester_id",
                "request_schema_version",
                "request_schema_digest",
                "request",
                "state",
            }:
                raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)
            action_id = _require_identifier(action["action_id"], field="action_id")
            if (
                action["action_type"] != ACQUISITION_START_ACTION_TYPE
                or action["workspace_id"] != context.workspace_id
                or action["requester_id"] != context.requester_id
                or action["request_schema_version"] != ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
                or action["request_schema_digest"] != ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
                or action["state"] != "pending_approval"
            ):
                raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)
            persisted_request = AcquisitionStartV2BoundRequest(cast(Mapping[str, Any], action["request"]))
            rebound = self.bind(
                input_payload=persisted_request.input_payload,
                context=context,
                tool_pins=persisted_request.snapshot.tool_pins,
                now=approved_time,
            )
            if rebound.to_record() != persisted_request.to_record():
                raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)
            return AcquisitionConfirmationReceipt.build(
                action_id=action_id,
                snapshot=rebound.snapshot,
                approval_actor_id=actor_id,
                approval_actor_kind=cast(ApprovalActorKind, approval_actor_kind),
                receipt_id=receipt_id,
                approval_policy_revision=approval_policy_revision,
                approved_at=approved_at,
            )
        except AcquisitionStartV2Error as exc:
            if exc.code in {
                ACQUISITION_START_V2_APPROVAL_INVALID,
                ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT,
            }:
                raise
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT) from exc
        except (KeyError, TypeError, ValueError) as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT) from exc


@dataclass(frozen=True, slots=True)
class AcquisitionConfirmationReceipt:
    """Immutable value persisted as the exact human ``ActionApproved`` receipt."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        record = _strict_mapping_copy(self._record, code=ACQUISITION_START_V2_RECEIPT_INVALID)
        expected_fields = {
            "schema_version",
            "receipt_id",
            "action_id",
            "approval_actor_id",
            "approval_actor_kind",
            "workspace_id",
            "requester_id",
            "preview_ref",
            "effective_request_digest",
            "company_identity",
            "cohort_identity",
            "provider_manifest_identity",
            "budget",
            "start_snapshot_digest",
            "request_pins",
            "result_pins",
            "tool_pins",
            "approval_policy_revision",
            "approved_at",
            "receipt_digest",
        }
        if (
            set(record) != expected_fields
            or record.get("schema_version") != ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION
        ):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID)
        for field_name in ("receipt_id", "action_id"):
            _require_identifier(record.get(field_name), field=field_name)
        for field_name in ("approval_actor_id", "workspace_id", "requester_id"):
            _require_owner_identity(record.get(field_name), field=field_name)
        if record.get("approval_actor_kind") not in {"authenticated_user", "open_operator"}:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID)
        _require_version(record.get("approval_policy_revision"), field="approval_policy_revision")
        _parse_utc_timestamp(str(record.get("approved_at") or ""))
        for digest_field in ("effective_request_digest", "start_snapshot_digest", "receipt_digest"):
            _require_sha256(record.get(digest_field), field=digest_field)
        preview_ref = _validate_preview_reference(record.get("preview_ref"))
        company = _strict_mapping_copy(record.get("company_identity"), code=ACQUISITION_START_V2_RECEIPT_INVALID)
        cohort = _strict_mapping_copy(record.get("cohort_identity"), code=ACQUISITION_START_V2_RECEIPT_INVALID)
        manifest = _strict_mapping_copy(
            record.get("provider_manifest_identity"), code=ACQUISITION_START_V2_RECEIPT_INVALID
        )
        if (
            set(company)
            != {
                "canonical_company_id",
                "company_registry_revision",
                "company_registry_digest",
                "company_target_digest",
            }
            or set(cohort)
            != {
                "cohort_selection",
                "cohort_selection_registry_version",
                "cohort_selection_registry_digest",
                "cohort_selection_digest",
            }
            or set(manifest) != {"schema_version", "manifest_digest", "physical_query_digest"}
        ):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID)
        _require_identifier(company["canonical_company_id"], field="canonical_company_id")
        _require_version(company["company_registry_revision"], field="company_registry_revision")
        _require_version(cohort["cohort_selection_registry_version"], field="cohort_selection_registry_version")
        _require_version(manifest["schema_version"], field="manifest_schema_version")
        for value in (
            company["company_registry_digest"],
            company["company_target_digest"],
            cohort["cohort_selection_registry_digest"],
            cohort["cohort_selection_digest"],
            manifest["manifest_digest"],
            manifest["physical_query_digest"],
        ):
            _require_sha256(value, field="receipt_identity_digest")
        if (
            dict(record.get("request_pins") or {})
            != {
                "schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
                "schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
            }
            or dict(record.get("result_pins") or {}) != _result_pins_record()
        ):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID)
        AcquisitionStartV2ToolPins(**dict(record.get("tool_pins") or {}))
        if preview_ref["preview_digest"] == record["receipt_digest"]:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID)
        expected_digest = _sha256_json({key: value for key, value in record.items() if key != "receipt_digest"})
        if record["receipt_digest"] != expected_digest:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID)
        object.__setattr__(self, "_record", _freeze_json(record))

    @property
    def receipt_id(self) -> str:
        return str(self._record["receipt_id"])

    @property
    def receipt_digest(self) -> str:
        return str(self._record["receipt_digest"])

    @property
    def action_id(self) -> str:
        return str(self._record["action_id"])

    @property
    def start_snapshot_digest(self) -> str:
        return str(self._record["start_snapshot_digest"])

    def to_record(self) -> dict[str, Any]:
        return cast(dict[str, Any], _thaw_json(self._record))

    @classmethod
    def build(
        cls,
        *,
        action_id: str,
        snapshot: AcquisitionStartV2Snapshot,
        approval_actor_id: str,
        approval_actor_kind: ApprovalActorKind,
        receipt_id: str,
        approval_policy_revision: str,
        approved_at: str,
    ) -> AcquisitionConfirmationReceipt:
        _require_identifier(action_id, field="action_id")
        _require_identifier(receipt_id, field="receipt_id")
        _require_owner_identity(approval_actor_id, field="approval_actor_id")
        if approval_actor_kind not in {"authenticated_user", "open_operator"}:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_APPROVAL_INVALID)
        _require_version(approval_policy_revision, field="approval_policy_revision")
        _parse_utc_timestamp(approved_at)
        preview = snapshot.preview.to_record()
        effective = dict(preview["effective_request"])
        company = dict(preview["company_target"])
        manifest = dict(preview["provider_planning_manifest"])
        record: dict[str, Any] = {
            "schema_version": ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION,
            "receipt_id": receipt_id,
            "action_id": action_id,
            "approval_actor_id": approval_actor_id,
            "approval_actor_kind": approval_actor_kind,
            "workspace_id": preview["workspace_id"],
            "requester_id": preview["requester_id"],
            "preview_ref": {
                "preview_id": preview["preview_id"],
                "preview_revision": preview["preview_revision"],
                "preview_digest": preview["preview_digest"],
            },
            "effective_request_digest": preview["effective_request_digest"],
            "company_identity": {
                "canonical_company_id": company["canonical_company_id"],
                "company_registry_revision": company["company_registry_revision"],
                "company_registry_digest": company["company_registry_digest"],
                "company_target_digest": company["company_target_digest"],
            },
            "cohort_identity": {
                "cohort_selection": effective["cohort_selection"],
                "cohort_selection_registry_version": effective["cohort_selection_registry_version"],
                "cohort_selection_registry_digest": effective["cohort_selection_registry_digest"],
                "cohort_selection_digest": effective["cohort_selection_digest"],
            },
            "provider_manifest_identity": {
                "schema_version": manifest["schema_version"],
                "manifest_digest": manifest["manifest_digest"],
                "physical_query_digest": manifest["physical_query_digest"],
            },
            "budget": effective["budget"],
            "start_snapshot_digest": snapshot.snapshot_digest,
            "request_pins": {
                "schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
                "schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
            },
            "result_pins": _result_pins_record(),
            "tool_pins": snapshot.tool_pins.to_record(),
            "approval_policy_revision": approval_policy_revision,
            "approved_at": approved_at,
        }
        record["receipt_digest"] = _sha256_json(record)
        return cls(record)


@dataclass(frozen=True, slots=True)
class AcquisitionParentBudgetEnvelopeRef:
    """Immutable reference to the receipt-owned acquisition budget envelope.

    The reference deliberately carries no independently writable budget values.
    The five values remain owned by the exact ``ActionApproved`` receipt; this
    value pins their canonical digest and the registered budget-owner
    fingerprint for downstream equality checks.
    """

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        try:
            record = _strict_mapping_copy(
                self._record,
                code=ACQUISITION_START_V2_PARENT_BUDGET_INVALID,
            )
            if set(record) != set(_ACQUISITION_PARENT_BUDGET_ENVELOPE_REF_FIELDS):
                raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)
            _require_identifier(record.get("owner_id"), field="owner_id")
            _require_version(record.get("owner_revision"), field="owner_revision")
            _require_identifier(record.get("confirmation_receipt_id"), field="confirmation_receipt_id")
            for field in (
                "owner_contract_digest",
                "confirmation_receipt_digest",
                "budget_digest",
            ):
                _require_sha256(record.get(field), field=field)
        except (AcquisitionStartV2Error, KeyError, TypeError, ValueError) as exc:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID) from exc
        canonical_record = {
            field: str(record[field]) for field in _ACQUISITION_PARENT_BUDGET_ENVELOPE_REF_FIELDS
        }
        object.__setattr__(self, "_record", _freeze_json(canonical_record))

    @property
    def owner_id(self) -> str:
        return str(self._record["owner_id"])

    @property
    def owner_revision(self) -> str:
        return str(self._record["owner_revision"])

    @property
    def owner_contract_digest(self) -> str:
        return str(self._record["owner_contract_digest"])

    @property
    def confirmation_receipt_id(self) -> str:
        return str(self._record["confirmation_receipt_id"])

    @property
    def confirmation_receipt_digest(self) -> str:
        return str(self._record["confirmation_receipt_digest"])

    @property
    def budget_digest(self) -> str:
        return str(self._record["budget_digest"])

    def to_record(self) -> dict[str, str]:
        return cast(dict[str, str], _thaw_json(self._record))


def build_acquisition_parent_budget_envelope_ref(
    receipt: AcquisitionConfirmationReceipt,
    registered_owner_pin: AgentToolOwnerPin,
) -> AcquisitionParentBudgetEnvelopeRef:
    """Build the only legal start-v2 parent-budget reference.

    Both arguments are typed owner outputs.  Raw dictionaries, subclasses, and
    caller-selected owner fingerprints fail closed.  The registered Agent tool
    remains the current source of truth for the exact budget-owner fingerprint.
    """

    try:
        if type(receipt) is not AcquisitionConfirmationReceipt or type(registered_owner_pin) is not AgentToolOwnerPin:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)

        # Imported lazily because the canary registry consumes this pure leaf
        # while constructing the current tool declaration.
        from .agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC

        current_owner_pin = START_ACQUISITION_RUN_TOOL_SPEC.budget.budget_owner
        if type(current_owner_pin) is not AgentToolOwnerPin:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)
        owner_fingerprint = registered_owner_pin.to_fingerprint_record()
        if owner_fingerprint != current_owner_pin.to_fingerprint_record():
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)

        receipt_record = receipt.to_record()
        revalidated_receipt = AcquisitionConfirmationReceipt(receipt_record)
        if revalidated_receipt.to_record() != receipt_record:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)
        expected_receipt_digest = _sha256_json(
            {key: value for key, value in receipt_record.items() if key != "receipt_digest"}
        )
        if receipt.receipt_digest != expected_receipt_digest:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)

        budget = _canonical_acquisition_parent_budget(receipt_record.get("budget"))
        return AcquisitionParentBudgetEnvelopeRef(
            {
                **owner_fingerprint,
                "confirmation_receipt_id": revalidated_receipt.receipt_id,
                "confirmation_receipt_digest": expected_receipt_digest,
                "budget_digest": _sha256_json(budget),
            }
        )
    except (AcquisitionStartV2Error, KeyError, TypeError, ValueError) as exc:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID) from exc


@dataclass(frozen=True, slots=True)
class AcquisitionStartV2RootCommandPayload:
    """Immutable v2 root-command payload with no free-text interpretation seam."""

    _record: Mapping[str, Any]

    def __post_init__(self) -> None:
        record = _strict_mapping_copy(self._record, code=ACQUISITION_START_V2_COMMAND_INVALID)
        if (
            set(record)
            != {
                "schema_version",
                "command_type",
                "action_id",
                "operation_run_id",
                "workflow_run_id",
                "confirmation_receipt_ref",
                "start_snapshot",
                "start_snapshot_digest",
                "payload_digest",
            }
            or record.get("schema_version") != ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION
            or record.get("command_type") != ACQUISITION_START_V2_ROOT_COMMAND_TYPE
        ):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
        for field_name in ("action_id", "operation_run_id", "workflow_run_id"):
            _require_identifier(record.get(field_name), field=field_name)
        receipt_ref = _strict_mapping_copy(
            record.get("confirmation_receipt_ref"), code=ACQUISITION_START_V2_COMMAND_INVALID
        )
        if set(receipt_ref) != {"receipt_id", "receipt_digest"}:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
        _require_identifier(receipt_ref.get("receipt_id"), field="receipt_id")
        _require_sha256(receipt_ref.get("receipt_digest"), field="receipt_digest")
        snapshot = AcquisitionStartV2Snapshot(cast(Mapping[str, Any], record.get("start_snapshot") or {}))
        if record.get("start_snapshot_digest") != snapshot.snapshot_digest:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
        _require_sha256(record.get("payload_digest"), field="payload_digest")
        expected_digest = _sha256_json({key: value for key, value in record.items() if key != "payload_digest"})
        if record["payload_digest"] != expected_digest:
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
        object.__setattr__(self, "_record", _freeze_json(record))

    def to_record(self) -> dict[str, Any]:
        return cast(dict[str, Any], _thaw_json(self._record))


def build_acquisition_start_v2_root_command_payload(
    *,
    snapshot: AcquisitionStartV2Snapshot,
    receipt: AcquisitionConfirmationReceipt,
    operation_run_id: str,
    workflow_run_id: str,
) -> AcquisitionStartV2RootCommandPayload:
    """Exact-copy the approved snapshot and receipt ref into the v2 root command."""

    if not isinstance(snapshot, AcquisitionStartV2Snapshot) or not isinstance(receipt, AcquisitionConfirmationReceipt):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
    _require_identifier(operation_run_id, field="operation_run_id")
    _require_identifier(workflow_run_id, field="workflow_run_id")
    if not _receipt_exactly_matches_snapshot(receipt, snapshot):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
    record: dict[str, Any] = {
        "schema_version": ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION,
        "command_type": ACQUISITION_START_V2_ROOT_COMMAND_TYPE,
        "action_id": receipt.action_id,
        "operation_run_id": operation_run_id,
        "workflow_run_id": workflow_run_id,
        "confirmation_receipt_ref": {
            "receipt_id": receipt.receipt_id,
            "receipt_digest": receipt.receipt_digest,
        },
        "start_snapshot": snapshot.to_record(),
        "start_snapshot_digest": snapshot.snapshot_digest,
    }
    record["payload_digest"] = _sha256_json(record)
    return AcquisitionStartV2RootCommandPayload(record)


def acquisition_start_v2_persisted_action_record(
    *,
    action_id: str,
    bound_request: AcquisitionStartV2BoundRequest,
) -> dict[str, Any]:
    """Build the exact pending-action comparison shape expected by approval."""

    _require_identifier(action_id, field="action_id")
    if not isinstance(bound_request, AcquisitionStartV2BoundRequest):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
    preview = bound_request.snapshot.preview.to_record()
    return {
        "action_id": action_id,
        "action_type": ACQUISITION_START_ACTION_TYPE,
        "workspace_id": preview["workspace_id"],
        "requester_id": preview["requester_id"],
        "request_schema_version": ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION,
        "request_schema_digest": ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST,
        "request": bound_request.to_record(),
        "state": "pending_approval",
    }


def acquisition_start_v2_success_result(
    *,
    action_id: str,
    operation_run_id: str,
    workflow_command_id: str,
    snapshot: AcquisitionStartV2Snapshot,
    receipt: AcquisitionConfirmationReceipt,
) -> dict[str, Any]:
    if receipt.action_id != action_id or not _receipt_exactly_matches_snapshot(receipt, snapshot):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_COMMAND_INVALID)
    preview = snapshot.preview
    return {
        "variant": "success",
        "status": "accepted",
        "action_id": action_id,
        "operation_run_id": operation_run_id,
        "workflow_command_id": workflow_command_id,
        "preview_id": preview.preview_id,
        "preview_revision": preview.preview_revision,
        "preview_digest": preview.preview_digest,
        "confirmation_receipt_id": receipt.receipt_id,
        "confirmation_receipt_digest": receipt.receipt_digest,
    }


def acquisition_start_v2_deferred_result(*, reason: str, retryable: bool) -> dict[str, Any]:
    _require_reason(reason)
    return {"variant": "deferred", "status": "deferred", "reason": reason, "retryable": retryable}


def acquisition_start_v2_error_result(*, reason: str) -> dict[str, Any]:
    _require_reason(reason)
    return {"variant": "error", "status": "failed", "reason": reason, "retryable": False}


def serialize_acquisition_start_v2_result(owner_output: dict[str, Any]) -> str:
    try:
        return ACQUISITION_START_V2_RESULT_SPEC.serialize(owner_output)
    except ActionResultSchemaError:
        raise


def _result_pins_record() -> dict[str, Any]:
    return {
        "schema_version": ACQUISITION_START_V2_RESULT_SPEC.result_schema_version,
        "schema_digest": ACQUISITION_START_V2_RESULT_SPEC.result_schema_digest,
        "serializer_owner": ACQUISITION_START_V2_RESULT_SPEC.serializer_owner,
        "serializer_revision": ACQUISITION_START_V2_RESULT_SPEC.serializer_revision,
        "serializer_contract_digest": ACQUISITION_START_V2_RESULT_SPEC.serializer_contract_digest,
        "interpretation_contract_version": ACQUISITION_START_V2_RESULT_SPEC.interpretation_contract_version,
        "interpretation_contract_digest": ACQUISITION_START_V2_RESULT_SPEC.interpretation_contract_digest,
    }


def _receipt_exactly_matches_snapshot(
    receipt: AcquisitionConfirmationReceipt,
    snapshot: AcquisitionStartV2Snapshot,
) -> bool:
    record = receipt.to_record()
    try:
        expected = AcquisitionConfirmationReceipt.build(
            action_id=str(record["action_id"]),
            snapshot=snapshot,
            approval_actor_id=str(record["approval_actor_id"]),
            approval_actor_kind=cast(ApprovalActorKind, record["approval_actor_kind"]),
            receipt_id=str(record["receipt_id"]),
            approval_policy_revision=str(record["approval_policy_revision"]),
            approved_at=str(record["approved_at"]),
        )
    except (AcquisitionStartV2Error, KeyError, TypeError, ValueError):
        return False
    return expected.to_record() == record


def _validate_preview_start_pins(preview: AcquisitionPlanPreview) -> None:
    schema_pins = dict(preview.to_record().get("schema_pins") or {})
    if (
        schema_pins.get("intended_start_request_schema_version") != ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION
        or schema_pins.get("intended_start_request_schema_digest") != ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST
    ):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)


def _validated_preview(value: Any) -> AcquisitionPlanPreview:
    if isinstance(value, AcquisitionPlanPreview):
        return value
    if not isinstance(value, Mapping):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT)
    return AcquisitionPlanPreview(value)


def _validate_preview_reference(value: Any) -> dict[str, Any]:
    try:
        record = _strict_mapping_copy(value, code=ACQUISITION_START_V2_RECEIPT_INVALID)
        validated = _START_V2_REFERENCE_VALIDATOR.validate_input(record)
        return dict(validated)
    except (AcquisitionStartV2Error, ModelToolSchemaError) as exc:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_RECEIPT_INVALID) from exc


def _require_identifier(value: Any, *, field: str) -> str:
    if type(value) is not str or _ID_PATTERN.fullmatch(value) is None:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, field)
    return value


def _require_version(value: Any, *, field: str) -> str:
    if type(value) is not str or _VERSION_PATTERN.fullmatch(value) is None:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, field)
    return value


def _require_sha256(value: Any, *, field: str) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, field)
    return value


def _require_owner_identity(value: Any, *, field: str) -> str:
    if (
        type(value) is not str
        or not 1 <= len(value) <= 200
        or value != value.strip()
        or any(
            character.isspace()
            or ord(character) <= 0x1F
            or 0x7F <= ord(character) <= 0x9F
            or 0xD800 <= ord(character) <= 0xDFFF
            for character in value
        )
    ):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, field)
    return value


def _require_reason(value: Any) -> str:
    if type(value) is not str or _REASON_PATTERN.fullmatch(value) is None:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "reason")
    return value


def _canonical_acquisition_parent_budget(value: Any) -> dict[str, int]:
    record = _strict_mapping_copy(value, code=ACQUISITION_START_V2_PARENT_BUDGET_INVALID)
    if set(record) != set(ACQUISITION_PARENT_BUDGET_FIELDS) or any(
        type(record.get(field)) is not int for field in ACQUISITION_PARENT_BUDGET_FIELDS
    ):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)
    budget = {field: int(record[field]) for field in ACQUISITION_PARENT_BUDGET_FIELDS}
    if (
        not 1 <= budget["max_provider_calls"] <= MAX_COHORT_PROVIDER_LANES
        or not 1 <= budget["max_provider_items"] <= MAX_PREVIEW_PROVIDER_ITEMS
        or not 1 <= budget["max_output_candidates"] <= MAX_PREVIEW_OUTPUT_CANDIDATES
        or not 0 <= budget["max_cost_micro_usd"] <= MAX_PREVIEW_COST_MICRO_USD
        or not 1 <= budget["max_elapsed_seconds"] <= MAX_PREVIEW_ELAPSED_SECONDS
        or budget["max_output_candidates"] > budget["max_provider_items"]
    ):
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_PARENT_BUDGET_INVALID)
    return budget


def _parse_utc_timestamp(value: str) -> datetime:
    if type(value) is not str or _UTC_TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "timestamp")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "timestamp") from exc


def _require_aware_datetime(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID, "now")
    return value.astimezone(timezone.utc)


def _strict_mapping_copy(value: Any, *, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise AcquisitionStartV2Error(code)
    try:
        copied = _thaw_json(value)
        if not isinstance(copied, dict):
            raise AcquisitionStartV2Error(code)
        json.dumps(copied, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return copied
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AcquisitionStartV2Error(code) from exc


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            _thaw_json(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID) from exc


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _freeze_json(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze_json(child) for child in value)
    return value


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise AcquisitionStartV2Error(ACQUISITION_START_V2_REQUEST_INVALID)
        return {key: _thaw_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(child) for child in value]
    return value


__all__ = [
    "ACQUISITION_CONFIRMATION_RECEIPT_SCHEMA_VERSION",
    "ACQUISITION_PARENT_BUDGET_FIELDS",
    "ACQUISITION_START_ACTION_TYPE",
    "ACQUISITION_START_V2_APPROVAL_INVALID",
    "ACQUISITION_START_V2_COMMAND_INVALID",
    "ACQUISITION_START_V2_PARENT_BUDGET_INVALID",
    "ACQUISITION_START_V2_PREVIEW_NOT_FOUND_OR_CONFLICT",
    "ACQUISITION_START_V2_REQUEST_INVALID",
    "ACQUISITION_START_V2_REQUEST_SCHEMA_DIGEST",
    "ACQUISITION_START_V2_REQUEST_SCHEMA_VERSION",
    "ACQUISITION_START_V2_REQUEST_TOOL_SPEC",
    "ACQUISITION_START_V2_RESULT_SCHEMA_VERSION",
    "ACQUISITION_START_V2_RESULT_SPEC",
    "ACQUISITION_START_V2_ROOT_COMMAND_PAYLOAD_SCHEMA_VERSION",
    "ACQUISITION_START_V2_ROOT_COMMAND_TYPE",
    "ACQUISITION_START_V2_SNAPSHOT_SCHEMA_VERSION",
    "AcquisitionConfirmationReceipt",
    "AcquisitionParentBudgetEnvelopeRef",
    "AcquisitionPlanPreviewOwnerReader",
    "AcquisitionStartV2BindContext",
    "AcquisitionStartV2BoundRequest",
    "AcquisitionStartV2Error",
    "AcquisitionStartV2OwnerBinder",
    "AcquisitionStartV2RootCommandPayload",
    "AcquisitionStartV2Snapshot",
    "AcquisitionStartV2ToolPins",
    "acquisition_start_v2_deferred_result",
    "acquisition_start_v2_error_result",
    "acquisition_start_v2_persisted_action_record",
    "acquisition_start_v2_request_schema",
    "acquisition_start_v2_success_result",
    "build_acquisition_parent_budget_envelope_ref",
    "build_acquisition_start_v2_root_command_payload",
    "serialize_acquisition_start_v2_result",
]
