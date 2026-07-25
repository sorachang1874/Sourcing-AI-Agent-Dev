"""Pure fixture owner for the selected-person X package boundary.

This is a service-simulation seam, not an Agent tool registration or durable
owner.  It performs no provider/model call and no product write.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sourcing_agent.x_first_portable_adapter import (
    XFirstPortableAdapterError,
    build_verification_import_preview,
    canonical_sha256,
    validate_verification_import_preview,
)
from sourcing_agent.x_first_portable_package import (
    ExpectedSelectionSnapshot,
    XFirstPortablePackageError,
    canonical_json,
    validate_x_first_portable_package,
)

SIMULATE_OWNER_RESULT_SCHEMA_VERSION = "sourcing.x_first.simulate_owner_result.v1"
SIMULATE_OWNER_RESULT_CONTRACT_SCHEMA_SHA256 = (
    "51e954f745e801a0f3010054e8371c2ff915e8e9e0475ce890c65c801dcc9df3"
)
_SCHEMA_PATH = Path(
    "contracts/external/x_first/sourcing.x_first.simulate_owner_result.v1.schema.json"
)
_STATE_TRACE = [
    "received",
    "validating_package",
    "package_validated",
    "building_preview",
    "preview_ready",
]
_EFFECTS = {
    "provider_call_count": 0,
    "model_call_count": 0,
    "product_write_count": 0,
    "canonical_person_merge_count": 0,
    "outreach_count": 0,
}
_AUTHORITY = {
    "live_authority": False,
    "promotion_authorized": False,
    "product_writes_allowed": False,
}
_ERROR_CODE_RE = re.compile(r"[a-z0-9_]{1,128}")


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(char in "0123456789abcdef" for char in value)
    )


class XFirstSimulateOwnerError(ValueError):
    """Stable failure from the effect-free fixture owner."""


@dataclass(frozen=True, slots=True)
class XFirstSimulateOwnerRun:
    owner_result: Mapping[str, Any]
    import_preview: Mapping[str, Any] | None


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _content_sha256(value: Mapping[str, Any], field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _validate_local_schema() -> None:
    try:
        actual = hashlib.sha256((_project_root() / _SCHEMA_PATH).read_bytes()).hexdigest()
    except OSError as exc:
        raise XFirstSimulateOwnerError("x_first_simulate_owner_schema_unavailable") from exc
    if actual != SIMULATE_OWNER_RESULT_CONTRACT_SCHEMA_SHA256:
        raise XFirstSimulateOwnerError("x_first_simulate_owner_schema_digest_mismatch")


def validate_x_first_simulate_owner_result(value: Any) -> None:
    _validate_local_schema()
    if not isinstance(value, Mapping):
        raise XFirstSimulateOwnerError("x_first_simulate_owner_result_not_object")
    record = dict(value)
    if set(record) != {
        "schema_version",
        "simulation_state",
        "state_trace",
        "input_package_sha256",
        "manifest_sha256",
        "semantic_validation_receipt_sha256",
        "selection_artifact_sha256",
        "portable_request_sha256",
        "portable_result_sha256",
        "import_preview_sha256",
        "error_code",
        "subject_terminal_counts",
        "effects",
        "served",
        "authority",
        "owner_result_sha256",
    }:
        raise XFirstSimulateOwnerError("x_first_simulate_owner_result_shape_invalid")
    counts = record["subject_terminal_counts"]
    if (
        record["schema_version"] != SIMULATE_OWNER_RESULT_SCHEMA_VERSION
        or record["simulation_state"] not in {"preview_ready", "rejected"}
        or record["effects"] != _EFFECTS
        or record["served"] is not False
        or record["authority"] != _AUTHORITY
        or not isinstance(counts, Mapping)
        or set(counts)
        != {
            "selected",
            "analyzed",
            "research_in_progress",
            "handle_resolution_required",
            "no_verified_account",
            "failed",
        }
        or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts.values())
        or counts["selected"]
        != counts["analyzed"]
        + counts["research_in_progress"]
        + counts["handle_resolution_required"]
        + counts["no_verified_account"]
        + counts["failed"]
        or record["owner_result_sha256"] != _content_sha256(record, "owner_result_sha256")
    ):
        raise XFirstSimulateOwnerError("x_first_simulate_owner_result_invalid")
    for field in ("input_package_sha256", "owner_result_sha256"):
        if not _is_sha256(record[field]):
            raise XFirstSimulateOwnerError("x_first_simulate_owner_result_invalid")
    if record["simulation_state"] == "preview_ready":
        if (
            record["state_trace"] != _STATE_TRACE
            or record["error_code"] is not None
            or counts["selected"] < 1
            or any(
                not _is_sha256(record[field])
                for field in (
                    "manifest_sha256",
                    "semantic_validation_receipt_sha256",
                    "selection_artifact_sha256",
                    "portable_request_sha256",
                    "portable_result_sha256",
                    "import_preview_sha256",
                )
            )
        ):
            raise XFirstSimulateOwnerError("x_first_simulate_owner_result_invalid")
    elif (
        record["state_trace"] != ["received", "validating_package", "rejected"]
        or not isinstance(record["error_code"], str)
        or _ERROR_CODE_RE.fullmatch(record["error_code"]) is None
        or any(counts.values())
        or any(
            record[field] is not None
            for field in (
                "manifest_sha256",
                "semantic_validation_receipt_sha256",
                "selection_artifact_sha256",
                "portable_request_sha256",
                "portable_result_sha256",
                "import_preview_sha256",
            )
        )
    ):
        raise XFirstSimulateOwnerError("x_first_simulate_owner_result_invalid")


def _input_package_sha256(portable_package: Mapping[str, Any]) -> str:
    try:
        return hashlib.sha256(canonical_json(portable_package).encode("utf-8")).hexdigest()
    except (TypeError, ValueError):
        return hashlib.sha256(b"x_first_non_json_package").hexdigest()


def _rejected_run(*, input_package_sha256: str, error_code: str) -> XFirstSimulateOwnerRun:
    record: dict[str, Any] = {
        "schema_version": SIMULATE_OWNER_RESULT_SCHEMA_VERSION,
        "simulation_state": "rejected",
        "state_trace": ["received", "validating_package", "rejected"],
        "input_package_sha256": input_package_sha256,
        "manifest_sha256": None,
        "semantic_validation_receipt_sha256": None,
        "selection_artifact_sha256": None,
        "portable_request_sha256": None,
        "portable_result_sha256": None,
        "import_preview_sha256": None,
        "error_code": error_code[:128],
        "subject_terminal_counts": {
            "selected": 0,
            "analyzed": 0,
            "research_in_progress": 0,
            "handle_resolution_required": 0,
            "no_verified_account": 0,
            "failed": 0,
        },
        "effects": dict(_EFFECTS),
        "served": False,
        "authority": dict(_AUTHORITY),
        "owner_result_sha256": "",
    }
    record["owner_result_sha256"] = _content_sha256(record, "owner_result_sha256")
    validate_x_first_simulate_owner_result(record)
    return XFirstSimulateOwnerRun(owner_result=record, import_preview=None)


def run_x_first_package_simulation(
    portable_package: Mapping[str, Any],
    *,
    fixture_id: str,
    expected_snapshot: ExpectedSelectionSnapshot,
) -> XFirstSimulateOwnerRun:
    """Validate, preview, and terminalize one effect-free fixture package."""

    input_package_sha256 = _input_package_sha256(portable_package)
    try:
        validated = validate_x_first_portable_package(
            portable_package,
            fixture_id=fixture_id,
            expected_snapshot=expected_snapshot,
        )
        preview = build_verification_import_preview(validated_package=validated)
    except (XFirstPortablePackageError, XFirstPortableAdapterError) as exc:
        return _rejected_run(input_package_sha256=input_package_sha256, error_code=str(exc))
    validate_verification_import_preview(preview)
    terminal_counts = {
        "selected": len(preview["subjects"]),
        "analyzed": sum(row["terminal_state"] == "analyzed" for row in preview["subjects"]),
        "research_in_progress": sum(
            row["terminal_state"] == "research_in_progress" for row in preview["subjects"]
        ),
        "handle_resolution_required": sum(
            row["terminal_state"] == "handle_resolution_required" for row in preview["subjects"]
        ),
        "no_verified_account": sum(
            row["terminal_state"] == "no_verified_account" for row in preview["subjects"]
        ),
        "failed": sum(row["terminal_state"] == "failed" for row in preview["subjects"]),
    }
    artifacts = validated.artifacts
    record: dict[str, Any] = {
        "schema_version": SIMULATE_OWNER_RESULT_SCHEMA_VERSION,
        "simulation_state": "preview_ready",
        "state_trace": list(_STATE_TRACE),
        "input_package_sha256": input_package_sha256,
        "manifest_sha256": validated.manifest["manifest_sha256"],
        "semantic_validation_receipt_sha256": validated.semantic_validation_receipt[
            "receipt_sha256"
        ],
        "selection_artifact_sha256": artifacts["selection"]["artifact_sha256"],
        "portable_request_sha256": artifacts["request"]["request_sha256"],
        "portable_result_sha256": artifacts["result"]["result_sha256"],
        "import_preview_sha256": preview["preview_sha256"],
        "error_code": None,
        "subject_terminal_counts": terminal_counts,
        "effects": dict(_EFFECTS),
        "served": False,
        "authority": dict(_AUTHORITY),
        "owner_result_sha256": "",
    }
    record["owner_result_sha256"] = _content_sha256(record, "owner_result_sha256")
    validate_x_first_simulate_owner_result(record)
    return XFirstSimulateOwnerRun(owner_result=record, import_preview=preview)


__all__ = [
    "SIMULATE_OWNER_RESULT_CONTRACT_SCHEMA_SHA256",
    "SIMULATE_OWNER_RESULT_SCHEMA_VERSION",
    "XFirstSimulateOwnerError",
    "XFirstSimulateOwnerRun",
    "run_x_first_package_simulation",
    "validate_x_first_simulate_owner_result",
]
