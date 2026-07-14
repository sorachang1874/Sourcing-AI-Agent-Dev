"""Fail-closed boundary between adaptive Grok runs and recall campaigns.

The current adaptive transcript proves tool invocation and terminal model
causality, but it does not retain the native-X payload returned by each tool
completion.  This bridge therefore emits only a typed blocked assessment.  It
never constructs a campaign wave, copies candidates, or upgrades model output
to source-bound evidence.
"""

from __future__ import annotations

import stat
from pathlib import Path
from typing import Any

from x_first.adaptive_grok_wave_runner import (
    DEFAULT_APPROVAL_ROOT,
    AdaptiveWaveValidationError,
    _read_regular_owned_bounded,
    _run_lease,
    bytes_sha256,
    canonical_json,
    canonical_sha256,
    strict_json_loads,
    validate_operator_bundle,
    validate_operator_receipt,
    validate_request,
)
from x_first.recall_pool_schema import assert_schema_valid, contract_schema_sha256

BRIDGE_SCHEMA_VERSION = "x.grok.adaptive_recall_wave.campaign_bridge.v1"
BRIDGE_SCHEMA_FILE = "x.grok.adaptive_recall_wave.campaign_bridge.v1.schema.json"
CAMPAIGN_ADMISSION = "blocked"
SOURCE_PAYLOAD_STATUS = "replay_unavailable"
REASON_CODE = "native_x_source_payload_not_replayable"

ZERO_AUTHORITY = {
    "campaign_write_authorized": False,
    "canonical_identity_write_authorized": False,
    "live_provider_call_authorized": False,
    "outreach_authorized": False,
    "product_write_authorized": False,
    "source_bound_evidence_claim_authorized": False,
}


class AdaptiveCampaignBridgeError(AdaptiveWaveValidationError):
    """Stable fail-closed error for adaptive-to-campaign admission."""


def _run_root_identity(run_root: Path) -> tuple[int, int]:
    try:
        metadata = run_root.lstat()
    except OSError as exc:
        raise AdaptiveCampaignBridgeError("adaptive_run_root_invalid") from exc
    if run_root.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise AdaptiveCampaignBridgeError("adaptive_run_root_invalid")
    return metadata.st_dev, metadata.st_ino


def _canonical_private_json(path: Path, *, maximum_bytes: int) -> tuple[dict[str, Any], bytes]:
    raw = _read_regular_owned_bounded(path, maximum_bytes=maximum_bytes, required_mode=0o600)
    try:
        payload = strict_json_loads(raw)
    except (UnicodeError, ValueError) as exc:
        raise AdaptiveCampaignBridgeError("bridge_artifact_json_invalid") from exc
    if not isinstance(payload, dict) or raw != (canonical_json(payload) + "\n").encode():
        raise AdaptiveCampaignBridgeError("bridge_artifact_not_canonical")
    return payload, raw


def build_blocked_campaign_bridge(
    run_root: Path,
    *,
    approval_root: Path = DEFAULT_APPROVAL_ROOT,
) -> dict[str, Any]:
    """Replay a private live bundle and emit a source-payload blocker.

    A successful return means only that the blocker itself is bound to a valid
    completed adaptive run.  It never means that campaign admission is safe.
    """

    # Coordinate with the only module-owned run writer/recovery owner for the
    # complete snapshot. Root identity plus exact end-of-read artifact hashes
    # also reject a same-path directory replacement during the lease.
    try:
        with _run_lease(run_root, create=False):
            initial_identity = _run_root_identity(run_root)
            artifact = _build_blocked_campaign_bridge_locked(
                run_root,
                approval_root=approval_root,
            )
            if _run_root_identity(run_root) != initial_identity:
                raise AdaptiveCampaignBridgeError("adaptive_bundle_changed_during_campaign_bridge")
            return artifact
    except AdaptiveWaveValidationError as exc:
        if isinstance(exc, AdaptiveCampaignBridgeError):
            raise
        raise AdaptiveCampaignBridgeError("adaptive_bundle_lease_unavailable") from exc


def _build_blocked_campaign_bridge_locked(
    run_root: Path,
    *,
    approval_root: Path,
) -> dict[str, Any]:
    bundle_errors = validate_operator_bundle(run_root, approval_root=approval_root)
    if bundle_errors:
        raise AdaptiveCampaignBridgeError("adaptive_bundle_invalid_for_campaign_bridge")

    request, request_raw = _canonical_private_json(
        run_root / "operator-request.json",
        maximum_bytes=4_194_304,
    )
    receipt, receipt_raw = _canonical_private_json(
        run_root / "operator-receipt.json",
        maximum_bytes=4_194_304,
    )
    if validate_request(request) or validate_operator_receipt(receipt):
        raise AdaptiveCampaignBridgeError("adaptive_bundle_changed_during_campaign_bridge")
    session_proof = receipt.get("session_proof")
    if (
        receipt.get("execution_mode") != "live"
        or receipt.get("status") != "completed"
        or not isinstance(session_proof, dict)
        or session_proof.get("status") != "verified"
    ):
        raise AdaptiveCampaignBridgeError("adaptive_source_not_live_completed")

    limits = request.get("technical_limits")
    if not isinstance(limits, dict):
        raise AdaptiveCampaignBridgeError("adaptive_source_limits_unavailable")
    max_json_bytes = limits.get("max_json_bytes")
    max_session_updates_bytes = limits.get("max_session_updates_bytes")
    if type(max_json_bytes) is not int or type(max_session_updates_bytes) is not int:
        raise AdaptiveCampaignBridgeError("adaptive_source_limits_unavailable")

    _, sanitized_raw = _canonical_private_json(
        run_root / "sanitized.json",
        maximum_bytes=max_json_bytes,
    )
    transcript_raw = _read_regular_owned_bounded(
        run_root / "session-updates.jsonl",
        maximum_bytes=max_session_updates_bytes,
        required_mode=0o600,
    )
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, dict) or not isinstance(session_proof, dict):
        raise AdaptiveCampaignBridgeError("adaptive_source_binding_unavailable")
    sanitized_sha = bytes_sha256(sanitized_raw)
    transcript_sha = bytes_sha256(transcript_raw)
    if (
        sanitized_sha != artifacts.get("sanitized_output_sha256")
        or transcript_sha != artifacts.get("session_updates_sha256")
        or transcript_sha != session_proof.get("updates_sha256")
        or canonical_sha256(request) != receipt.get("request_sha256")
    ):
        raise AdaptiveCampaignBridgeError("adaptive_source_binding_mismatch")

    artifact = {
        "schema_version": BRIDGE_SCHEMA_VERSION,
        "campaign_admission": CAMPAIGN_ADMISSION,
        "reason_code": REASON_CODE,
        "source_payload_status": SOURCE_PAYLOAD_STATUS,
        "source_payload_binding": {
            "captured_payload_count": 0,
            "captured_payload_sha256s": [],
        },
        "source_binding": {
            "run_id": receipt["run_id"],
            "request_id": receipt["request_id"],
            "execution_mode": "live",
            "terminal_status": "completed",
            "session_proof_status": "verified",
            "effective_model_id": session_proof["effective_model_id"],
            "operator_request_artifact_sha256": bytes_sha256(request_raw),
            "operator_request_canonical_sha256": canonical_sha256(request),
            "operator_receipt_artifact_sha256": bytes_sha256(receipt_raw),
            "sanitized_result_artifact_sha256": sanitized_sha,
            "session_transcript_artifact_sha256": transcript_sha,
            "bridge_schema_sha256": contract_schema_sha256(BRIDGE_SCHEMA_FILE),
        },
        "wave_input": None,
        "authority": dict(ZERO_AUTHORITY),
    }
    try:
        assert_schema_valid(artifact, BRIDGE_SCHEMA_FILE)
    except ValueError as exc:
        raise AdaptiveCampaignBridgeError("generated_campaign_bridge_invalid") from exc

    # Re-read every source used by the bridge and compare exact hashes before
    # the final full replay. This rejects A->B->A path swaps, not only an
    # invalid intermediate bundle.
    current_request, current_request_raw = _canonical_private_json(
        run_root / "operator-request.json",
        maximum_bytes=4_194_304,
    )
    _, current_receipt_raw = _canonical_private_json(
        run_root / "operator-receipt.json",
        maximum_bytes=4_194_304,
    )
    _, current_sanitized_raw = _canonical_private_json(
        run_root / "sanitized.json",
        maximum_bytes=max_json_bytes,
    )
    current_transcript_raw = _read_regular_owned_bounded(
        run_root / "session-updates.jsonl",
        maximum_bytes=max_session_updates_bytes,
        required_mode=0o600,
    )
    source_binding = artifact["source_binding"]
    if (
        bytes_sha256(current_request_raw) != source_binding["operator_request_artifact_sha256"]
        or canonical_sha256(current_request) != source_binding["operator_request_canonical_sha256"]
        or bytes_sha256(current_receipt_raw) != source_binding["operator_receipt_artifact_sha256"]
        or bytes_sha256(current_sanitized_raw) != source_binding["sanitized_result_artifact_sha256"]
        or bytes_sha256(current_transcript_raw) != source_binding["session_transcript_artifact_sha256"]
    ):
        raise AdaptiveCampaignBridgeError("adaptive_bundle_changed_during_campaign_bridge")
    if validate_operator_bundle(run_root, approval_root=approval_root):
        raise AdaptiveCampaignBridgeError("adaptive_bundle_changed_during_campaign_bridge")
    return artifact
