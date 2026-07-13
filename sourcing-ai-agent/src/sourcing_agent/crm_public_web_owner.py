"""CRM Public Web domain owner extracted from ``SourcingOrchestrator`` (Phase 3).

Planners, lifecycle (cancel/resume), queue-batch/phase command execution,
drains, owner-facing read/action APIs, promotion, and archive export for the
CRM Public Web cluster.  Bodies are moved verbatim from ``orchestrator.py``;
the only body edit is kernel-wrapper calls ``self._x(...)`` ->
``self._kernel._x(...)``.  ``SourcingOrchestrator`` keeps signature-identical
delegating wrappers for every moved method, and injects its shared spine
helpers as bound callables stored under the same attribute names so moved
bodies stay verbatim.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import uuid
import zipfile

from collections import (
    Counter,
    defaultdict,
)
from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .crm_public_web_runtime import (
    CRM_PUBLIC_WEB_EXECUTION_BACKEND,
    CRM_PUBLIC_WEB_JOB_TYPE,
    CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND,
    PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES,
    PUBLIC_WEB_TERMINAL_STATUSES,
    PUBLIC_WEB_WORKER_LANE,
    build_crm_public_web_batch_idempotency_key,
    cancel_crm_public_web_run,
    execute_crm_public_web_run_once,
    public_web_options_from_record,
    public_web_signal_identity_key,
    public_web_worker_key,
    start_crm_public_web_batch,
    sync_crm_public_web_batch_summary,
)
from .async_task_contract import (
    async_task_accepted,
    async_task_artifact,
    async_task_status,
)
from .domain import JobRequest
from .durable_runtime import (
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES,
    CRM_PUBLIC_WEB_PHASE_OWNER,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
    crm_public_web_queue_batch_idempotency_key,
    crm_public_web_run_phase_idempotency_key,
    default_readiness_effect_for_command_type,
    export_crm_public_web_generate_idempotency_key,
    workflow_command_expected_run_statuses,
    workflow_command_migration_step_id,
    workflow_command_product_label_zh,
)
from .linkedin_url_normalization import normalize_linkedin_profile_url_key
from .person_asset_writer import PersonAssetWriter
from .public_web_runtime_core import utc_compact_timestamp
from .storage import _json_safe_payload as _storage_json_safe_payload

from .command_kernel import CommandKernel


# NOTE: the helpers below duplicate small module-level helpers in
# ``orchestrator.py`` (which imports this module — importing them back from
# orchestrator would create a cycle).  The bodies are copied verbatim; several
# other ``sourcing_agent`` modules already carry the same local copies.
_CHINA_TIME_ZONE = ZoneInfo("Asia/Shanghai")

def _china_now_iso() -> str:
    return datetime.now(_CHINA_TIME_ZONE).isoformat(timespec="seconds")

def _china_local_filename_timestamp() -> str:
    return datetime.now(_CHINA_TIME_ZONE).strftime("%Y%m%dT%H%M")

def _build_csv_bytes(rows: list[dict[str, str]], fieldnames: list[str]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({field_name: str(row.get(field_name) or "") for field_name in fieldnames})
    return output.getvalue().encode("utf-8-sig")

def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(value)

def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default

def _dedupe_texts(values: Any) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in list(values or []):
        text = " ".join(str(item or "").split()).strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result

def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default

def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _target_candidate_archive_name_component(value: str, *, fallback: str) -> str:
    collapsed = " ".join(str(value or "").split()).strip()
    if not collapsed:
        return fallback
    safe_chars = [character if character.isalnum() or character in {"-", "_"} else "_" for character in collapsed]
    normalized = "".join(safe_chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    normalized = normalized.strip("._-")
    return normalized[:96] or fallback


def _public_web_target_candidate_summary(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "record_id": str(record.get("id") or record.get("record_id") or ""),
        "candidate_id": str(record.get("candidate_id") or ""),
        "candidate_name": str(record.get("candidate_name") or ""),
        "headline": str(record.get("headline") or ""),
        "current_company": str(record.get("current_company") or ""),
        "linkedin_url": str(record.get("linkedin_url") or ""),
        "primary_email": str(record.get("primary_email") or ""),
        "updated_at": str(record.get("updated_at") or ""),
    }


def _public_web_run_detail(run: dict[str, Any]) -> dict[str, Any]:
    analysis_checkpoint = dict(run.get("analysis_checkpoint") or {})
    query_manifest = [dict(item) for item in list(run.get("query_manifest") or []) if isinstance(item, dict)]
    return {
        "run_id": str(run.get("run_id") or ""),
        "batch_id": str(run.get("batch_id") or ""),
        "record_id": str(run.get("record_id") or ""),
        "candidate_id": str(run.get("candidate_id") or ""),
        "candidate_name": str(run.get("candidate_name") or ""),
        "current_company": str(run.get("current_company") or ""),
        "linkedin_url_key": str(run.get("linkedin_url_key") or ""),
        "person_identity_key": str(run.get("person_identity_key") or ""),
        "status": str(run.get("status") or ""),
        "phase": str(run.get("phase") or ""),
        "source_families": list(run.get("source_families") or []),
        "query_count": len(query_manifest),
        "summary": _public_web_model_safe_summary(dict(run.get("summary") or {})),
        "phase_command_display_line": str(run.get("phase_command_display_line") or ""),
        "run_control_state": _storage_json_safe_payload(dict(run.get("run_control_state") or {})),
        "run_display_contract": _storage_json_safe_payload(dict(run.get("run_display_contract") or {})),
        "analysis": {
            "stage": str(analysis_checkpoint.get("stage") or ""),
            "status": str(analysis_checkpoint.get("status") or ""),
            "ai_adjudication_status": str(analysis_checkpoint.get("ai_adjudication_status") or ""),
            "phase_metrics": _storage_json_safe_payload(dict(analysis_checkpoint.get("phase_metrics") or {})),
        },
        "started_at": str(run.get("started_at") or ""),
        "completed_at": str(run.get("completed_at") or ""),
        "updated_at": str(run.get("updated_at") or ""),
    }


def _public_web_asset_detail(asset: dict[str, Any]) -> dict[str, Any]:
    return {
        "asset_id": str(asset.get("asset_id") or ""),
        "person_identity_key": str(asset.get("person_identity_key") or ""),
        "linkedin_url_key": str(asset.get("linkedin_url_key") or ""),
        "latest_run_id": str(asset.get("latest_run_id") or ""),
        "target_candidate_record_id": str(asset.get("target_candidate_record_id") or ""),
        "candidate_name": str(asset.get("candidate_name") or ""),
        "current_company": str(asset.get("current_company") or ""),
        "status": str(asset.get("status") or ""),
        "summary": _public_web_model_safe_summary(dict(asset.get("summary") or {})),
        "source_run_ids": list(asset.get("source_run_ids") or []),
        "updated_at": str(asset.get("updated_at") or ""),
    }


def _public_web_signal_detail(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _public_web_model_safe_signal_metadata(dict(row.get("metadata") or {}))
    return {
        "signal_id": str(row.get("signal_id") or ""),
        "run_id": str(row.get("run_id") or ""),
        "asset_id": str(row.get("asset_id") or ""),
        "person_identity_key": str(row.get("person_identity_key") or ""),
        "record_id": str(row.get("record_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "candidate_name": str(row.get("candidate_name") or ""),
        "current_company": str(row.get("current_company") or ""),
        "linkedin_url_key": str(row.get("linkedin_url_key") or ""),
        "signal_kind": str(row.get("signal_kind") or ""),
        "signal_type": str(row.get("signal_type") or ""),
        "email_type": str(row.get("email_type") or ""),
        "value": str(row.get("value") or ""),
        "normalized_value": str(row.get("normalized_value") or ""),
        "url": str(row.get("url") or ""),
        "source_url": str(row.get("source_url") or ""),
        "source_domain": str(row.get("source_domain") or ""),
        "source_family": str(row.get("source_family") or ""),
        "source_title": str(row.get("source_title") or ""),
        "confidence_label": str(row.get("confidence_label") or ""),
        "confidence_score": _coerce_public_web_float(row.get("confidence_score")),
        "identity_match_label": str(row.get("identity_match_label") or ""),
        "identity_match_score": _coerce_public_web_float(row.get("identity_match_score")),
        "publishable": bool(row.get("publishable")),
        "promotion_status": str(row.get("promotion_status") or ""),
        "suppression_reason": str(row.get("suppression_reason") or ""),
        "evidence_excerpt": str(row.get("evidence_excerpt") or ""),
        "artifact_refs": _public_web_model_safe_artifact_refs(dict(row.get("artifact_refs") or {})),
        "model_provider": str(row.get("model_provider") or ""),
        "model_version": str(row.get("model_version") or ""),
        "link_shape_warnings": list(metadata.get("link_shape_warnings") or []),
        "clean_profile_link": bool(metadata.get("clean_profile_link", True)),
        "metadata": metadata,
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
    }


def _latest_public_web_promotions_by_signal(promotions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for promotion in promotions:
        signal_id = str(promotion.get("signal_id") or "").strip()
        if not signal_id or signal_id in latest:
            continue
        latest[signal_id] = dict(promotion)
    return latest


def _public_web_signal_stable_identity_key(row: dict[str, Any]) -> str:
    return public_web_signal_identity_key(
        person_identity_key=str(row.get("person_identity_key") or ""),
        record_id=str(row.get("record_id") or row.get("crm_record_id") or ""),
        signal_kind=str(row.get("signal_kind") or ""),
        signal_type=str(row.get("signal_type") or row.get("email_type") or ""),
        normalized_value=str(row.get("normalized_value") or ""),
        value=str(row.get("value") or ""),
        url=str(row.get("url") or ""),
        source_url=str(row.get("source_url") or ""),
    )


def _latest_public_web_promotions_by_identity(promotions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for promotion in promotions:
        identity_key = _public_web_signal_stable_identity_key(promotion)
        if not identity_key or identity_key in latest:
            continue
        latest[identity_key] = dict(promotion)
    return latest


def _public_web_signal_with_promotion(
    signal: dict[str, Any],
    promotion: dict[str, Any] | None,
    *,
    promotion_match_basis: str = "",
) -> dict[str, Any]:
    if not promotion:
        return signal
    updated = dict(signal)
    promotion_signal_id = str(promotion.get("signal_id") or "")
    updated["promotion_id"] = str(promotion.get("promotion_id") or "")
    updated["promotion_status"] = str(promotion.get("promotion_status") or signal.get("promotion_status") or "")
    updated["promotion_action"] = str(promotion.get("action") or "")
    updated["promotion_match_basis"] = promotion_match_basis or "signal_id"
    updated["promotion_source_signal_id"] = promotion_signal_id
    updated["promoted_field"] = str(promotion.get("promoted_field") or "")
    updated["promoted_value"] = str(promotion.get("new_value") or "")
    updated["previous_value"] = str(promotion.get("previous_value") or "")
    updated["promoted_by"] = str(promotion.get("operator") or "")
    updated["promoted_at"] = str(promotion.get("created_at") or "")
    updated["promotion_note"] = str(promotion.get("note") or "")
    metadata = dict(promotion.get("metadata") or {})
    updated["promotion_override_reason"] = str(metadata.get("override_reason") or "")
    updated["promotion_override_validation_reason"] = str(metadata.get("override_validation_reason") or "")
    updated["promotion_requires_manual_override"] = bool(metadata.get("promotion_requires_manual_override"))
    if promotion_signal_id and promotion_signal_id != str(signal.get("signal_id") or ""):
        updated["promotion_signal_identity_inherited"] = True
    return updated


def _public_web_promotion_detail(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _storage_json_safe_payload(dict(row.get("metadata") or {}))
    return {
        "promotion_id": str(row.get("promotion_id") or ""),
        "signal_id": str(row.get("signal_id") or ""),
        "run_id": str(row.get("run_id") or ""),
        "asset_id": str(row.get("asset_id") or ""),
        "person_identity_key": str(row.get("person_identity_key") or ""),
        "record_id": str(row.get("record_id") or ""),
        "candidate_id": str(row.get("candidate_id") or ""),
        "candidate_name": str(row.get("candidate_name") or ""),
        "current_company": str(row.get("current_company") or ""),
        "linkedin_url_key": str(row.get("linkedin_url_key") or ""),
        "signal_kind": str(row.get("signal_kind") or ""),
        "signal_type": str(row.get("signal_type") or ""),
        "email_type": str(row.get("email_type") or ""),
        "value": str(row.get("value") or ""),
        "normalized_value": str(row.get("normalized_value") or ""),
        "url": str(row.get("url") or ""),
        "source_url": str(row.get("source_url") or ""),
        "source_domain": str(row.get("source_domain") or ""),
        "source_family": str(row.get("source_family") or ""),
        "source_title": str(row.get("source_title") or ""),
        "confidence_label": str(row.get("confidence_label") or ""),
        "confidence_score": _coerce_public_web_float(row.get("confidence_score")),
        "identity_match_label": str(row.get("identity_match_label") or ""),
        "identity_match_score": _coerce_public_web_float(row.get("identity_match_score")),
        "publishable": bool(row.get("publishable")),
        "clean_profile_link": bool(row.get("clean_profile_link")),
        "link_shape_warnings": list(row.get("link_shape_warnings") or []),
        "action": str(row.get("action") or ""),
        "promotion_status": str(row.get("promotion_status") or ""),
        "promoted_field": str(row.get("promoted_field") or ""),
        "previous_value": str(row.get("previous_value") or ""),
        "new_value": str(row.get("new_value") or ""),
        "operator": str(row.get("operator") or ""),
        "note": str(row.get("note") or ""),
        "override_reason": str(metadata.get("override_reason") or ""),
        "override_validation_reason": str(metadata.get("override_validation_reason") or ""),
        "requires_manual_override": bool(metadata.get("promotion_requires_manual_override")),
        "evidence_excerpt": str(row.get("evidence_excerpt") or ""),
        "metadata": metadata,
        "created_at": str(row.get("created_at") or ""),
        "updated_at": str(row.get("updated_at") or ""),
    }


def _public_web_promotion_summary(promotions: list[dict[str, Any]]) -> dict[str, Any]:
    promoted = [item for item in promotions if str(item.get("action") or "") == "promote"]
    rejected = [item for item in promotions if str(item.get("action") or "") == "reject"]
    override_promotions = [
        item for item in promotions if bool(dict(item.get("metadata") or {}).get("promotion_requires_manual_override"))
    ]
    return {
        "promotion_count": len(promotions),
        "promoted_count": len(promoted),
        "rejected_count": len(rejected),
        "override_count": len(override_promotions),
        "promoted_email_count": len(
            [item for item in promoted if str(item.get("signal_kind") or "") == "email_candidate"]
        ),
        "promoted_link_count": len([item for item in promoted if str(item.get("signal_kind") or "") == "profile_link"]),
    }


def _coerce_public_web_record_ids(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw_values = [str(item or "") for item in value]
    else:
        raw_values = str(value or "").replace(";", ",").split(",")
    record_ids: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        record_id = str(raw_value or "").strip()
        if not record_id or record_id in seen:
            continue
        seen.add(record_id)
        record_ids.append(record_id)
    return record_ids


def _public_web_signal_is_rejected(signal: dict[str, Any]) -> bool:
    promotion_status = str(signal.get("promotion_status") or "").strip()
    promotion_action = str(signal.get("promotion_action") or signal.get("action") or "").strip()
    return promotion_status in {"manually_rejected", "rejected", "suppressed"} or promotion_action == "reject"


def _public_web_export_promotions_for_signals(
    promotions: list[dict[str, Any]],
    exported_signals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    exported_signal_ids = {
        str(signal.get("signal_id") or "").strip()
        for signal in exported_signals
        if str(signal.get("signal_id") or "").strip()
    }
    exported_promotion_ids = {
        str(signal.get("promotion_id") or "").strip()
        for signal in exported_signals
        if str(signal.get("promotion_id") or "").strip()
    }
    export_promotions = []
    for promotion in promotions:
        if not isinstance(promotion, dict):
            continue
        if str(promotion.get("action") or "").strip() != "promote":
            continue
        if str(promotion.get("promotion_status") or "").strip() != "manually_promoted":
            continue
        promotion_id = str(promotion.get("promotion_id") or "").strip()
        signal_id = str(promotion.get("signal_id") or "").strip()
        promotion_matches = promotion_id and promotion_id in exported_promotion_ids
        signal_matches = signal_id and signal_id in exported_signal_ids
        if promotion_matches or signal_matches:
            export_promotions.append(dict(promotion))
    return sorted(
        export_promotions,
        key=lambda item: (str(item.get("promotion_id") or ""), str(item.get("signal_id") or "")),
    )


def _public_web_exportable_signals(
    signals: list[dict[str, Any]],
    *,
    include_publishable_unpromoted: bool,
) -> list[dict[str, Any]]:
    exportable: list[dict[str, Any]] = []
    seen: set[str] = set()
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        signal_id = str(signal.get("signal_id") or "").strip()
        if signal_id and signal_id in seen:
            continue
        promotion_status = str(signal.get("promotion_status") or "").strip()
        if _public_web_signal_is_rejected(signal):
            continue
        promoted = promotion_status == "manually_promoted"
        suppressed = bool(str(signal.get("suppression_reason") or "").strip())
        ai_publishable = include_publishable_unpromoted and bool(signal.get("publishable")) and not suppressed
        if not promoted and not ai_publishable:
            continue
        row = dict(signal)
        row["export_status"] = "manual_promoted" if promoted else "ai_publishable_unpromoted"
        exportable.append(row)
        if signal_id:
            seen.add(signal_id)
    return exportable


def _public_web_export_signal_from_promotion(promotion: dict[str, Any]) -> dict[str, Any]:
    signal_kind = str(promotion.get("signal_kind") or "").strip()
    signal_type = str(promotion.get("signal_type") or "").strip()
    value = str(
        promotion.get("new_value")
        or promotion.get("normalized_value")
        or promotion.get("value")
        or promotion.get("url")
        or ""
    ).strip()
    url = str(promotion.get("url") or "").strip()
    if signal_kind == "profile_link" and not url:
        url = value
    normalized_value = value.lower() if signal_kind == "email_candidate" else value
    signal_id = str(promotion.get("signal_id") or "").strip()
    promotion_id = str(promotion.get("promotion_id") or "").strip()
    row = dict(promotion)
    row.update(
        {
            "signal_id": signal_id or (f"promotion:{promotion_id}" if promotion_id else ""),
            "signal_kind": signal_kind,
            "signal_type": signal_type,
            "email_type": str(promotion.get("email_type") or "").strip(),
            "value": value,
            "normalized_value": normalized_value,
            "url": url,
            "source_url": str(promotion.get("source_url") or url or "").strip(),
            "source_domain": str(promotion.get("source_domain") or "").strip(),
            "source_family": str(promotion.get("source_family") or "").strip(),
            "source_title": str(promotion.get("source_title") or "").strip(),
            "confidence_label": str(promotion.get("confidence_label") or "").strip(),
            "confidence_score": promotion.get("confidence_score"),
            "identity_match_label": str(promotion.get("identity_match_label") or "").strip(),
            "identity_match_score": promotion.get("identity_match_score"),
            "publishable": bool(promotion.get("publishable")),
            "promotion_status": "manually_promoted",
            "promotion_id": promotion_id,
            "export_status": "manual_promoted",
            "evidence_excerpt": str(promotion.get("evidence_excerpt") or "").strip(),
        }
    )
    return row


def _public_web_exportable_promotion_signals(promotions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    exportable = []
    for promotion in promotions:
        if not isinstance(promotion, dict):
            continue
        if str(promotion.get("action") or "").strip() != "promote":
            continue
        if str(promotion.get("promotion_status") or "").strip() != "manually_promoted":
            continue
        row = _public_web_export_signal_from_promotion(dict(promotion))
        if _public_web_signal_promoted_value(row):
            exportable.append(row)
    return exportable


def _public_web_export_signal_identity(signal: dict[str, Any]) -> str:
    promotion_id = str(signal.get("promotion_id") or "").strip()
    if promotion_id:
        return f"promotion:{promotion_id}"
    signal_id = str(signal.get("signal_id") or "").strip()
    if signal_id:
        return f"signal:{signal_id}"
    return "|".join(
        [
            str(signal.get("signal_kind") or "").strip(),
            str(signal.get("signal_type") or signal.get("email_type") or "").strip(),
            _public_web_signal_promoted_value(signal),
            str(signal.get("source_url") or "").strip(),
        ]
    )


def _merge_public_web_export_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for signal in signals:
        if not isinstance(signal, dict):
            continue
        identity = _public_web_export_signal_identity(signal)
        if identity and identity in seen:
            continue
        merged.append(dict(signal))
        if identity:
            seen.add(identity)
    return merged


def _public_web_run_status_is_terminal(status: str) -> bool:
    normalized = str(status or "").strip()
    return normalized in set(PUBLIC_WEB_TERMINAL_STATUSES) or normalized == "canceled"


def _crm_public_web_retry_nonce(
    *,
    source_run_ids: list[str],
    reason: str,
    workspace_id: str,
    requested_by: str,
) -> str:
    normalized_source_run_ids = sorted(str(run_id or "").strip() for run_id in source_run_ids if str(run_id or "").strip())
    payload = {
        "source_run_ids": normalized_source_run_ids,
        "reason": str(reason or "").strip(),
        "workspace_id": str(workspace_id or "default").strip() or "default",
        "requested_by": str(requested_by or "operator").strip() or "operator",
    }
    digest = hashlib.sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:24]
    return f"retry-{digest}"


def _public_web_export_record_status(
    *,
    detail: dict[str, Any],
    exported_signals: list[dict[str, Any]],
    exported_promotion_signal_count: int = 0,
) -> dict[str, str]:
    detail_status = str(detail.get("status") or "").strip() or "unknown"
    latest_run = dict(detail.get("latest_run") or {})
    latest_run_status = str(latest_run.get("status") or "").strip()
    if detail_status != "ok":
        return {"status": "skipped", "skip_reason": "no_public_web_result"}
    if not exported_signals:
        return {"status": "skipped", "skip_reason": "no_exportable_signals"}
    if latest_run_status and not _public_web_run_status_is_terminal(latest_run_status):
        if exported_promotion_signal_count <= 0:
            return {"status": "skipped", "skip_reason": "public_web_run_not_terminal"}
        return {
            "status": "exported",
            "skip_reason": "",
            "export_basis": "durable_manual_promotion",
            "latest_run_terminal": "false",
        }
    return {"status": "exported", "skip_reason": ""}


def _best_public_web_export_signal(signals: list[dict[str, Any]]) -> dict[str, Any]:
    if not signals:
        return {}
    return dict(
        sorted(
            (dict(signal) for signal in signals if isinstance(signal, dict)),
            key=lambda signal: (
                0 if str(signal.get("promotion_status") or "") == "manually_promoted" else 1,
                -_coerce_public_web_float(signal.get("confidence_score")),
                -_coerce_public_web_float(signal.get("identity_match_score")),
                str(signal.get("source_url") or signal.get("url") or ""),
            ),
        )[0]
    )


def _public_web_candidate_summary_csv_row(
    *,
    record: dict[str, Any],
    detail: dict[str, Any],
    exported_signals: list[dict[str, Any]],
    export_mode: str,
    record_export_status: dict[str, Any] | None = None,
) -> dict[str, str]:
    export_status = dict(record_export_status or {})
    exported_email = _best_public_web_export_signal(
        [signal for signal in exported_signals if str(signal.get("signal_kind") or "") == "email_candidate"]
    )
    exported_links = [signal for signal in exported_signals if str(signal.get("signal_kind") or "") == "profile_link"]
    links_by_type: dict[str, list[str]] = defaultdict(list)
    for signal in exported_links:
        signal_type = str(signal.get("signal_type") or "other").strip() or "other"
        url = _public_web_signal_promoted_value(signal)
        if url and url not in links_by_type[signal_type]:
            links_by_type[signal_type].append(url)
    return {
        "record_id": str(record.get("id") or ""),
        "candidate_name": str(record.get("candidate_name") or ""),
        "current_company": str(record.get("current_company") or ""),
        "linkedin_url": str(record.get("linkedin_url") or ""),
        "promoted_primary_email": _public_web_signal_promoted_value(exported_email),
        "email_type": str(exported_email.get("email_type") or ""),
        "email_source_url": str(exported_email.get("source_url") or ""),
        "email_confidence": str(exported_email.get("confidence_label") or ""),
        "homepage_url": " | ".join(links_by_type.get("homepage", []) or links_by_type.get("personal_homepage", [])),
        "github_url": " | ".join(links_by_type.get("github_url", []) or links_by_type.get("github", [])),
        "x_url": " | ".join(links_by_type.get("x_url", []) or links_by_type.get("twitter_url", [])),
        "substack_url": " | ".join(links_by_type.get("substack_url", []) or links_by_type.get("substack", [])),
        "scholar_url": " | ".join(links_by_type.get("google_scholar_url", []) or links_by_type.get("scholar", [])),
        "other_profile_links": " | ".join(
            url
            for signal_type, urls in sorted(links_by_type.items())
            if signal_type
            not in {
                "homepage",
                "personal_homepage",
                "github",
                "github_url",
                "x_url",
                "twitter_url",
                "substack",
                "substack_url",
                "google_scholar_url",
                "scholar",
            }
            for url in urls
        ),
        "exported_signal_count": str(len(exported_signals)),
        "promotion_count": str(dict(detail.get("promotion_summary") or {}).get("promotion_count") or 0),
        "export_mode": export_mode,
        "public_web_status": str(detail.get("status") or ""),
        "latest_run_status": str(dict(detail.get("latest_run") or {}).get("status") or ""),
        "export_record_status": str(export_status.get("status") or ""),
        "export_skip_reason": str(export_status.get("skip_reason") or ""),
    }


def _public_web_signal_csv_row(*, record: dict[str, Any], signal: dict[str, Any]) -> dict[str, str]:
    return {
        "record_id": str(record.get("id") or signal.get("record_id") or ""),
        "candidate_name": str(record.get("candidate_name") or signal.get("candidate_name") or ""),
        "signal_id": str(signal.get("signal_id") or ""),
        "signal_kind": str(signal.get("signal_kind") or ""),
        "signal_type": str(signal.get("signal_type") or ""),
        "email_type": str(signal.get("email_type") or ""),
        "value": str(signal.get("normalized_value") or signal.get("value") or ""),
        "url": str(signal.get("url") or ""),
        "source_url": str(signal.get("source_url") or ""),
        "source_domain": str(signal.get("source_domain") or ""),
        "source_family": str(signal.get("source_family") or ""),
        "confidence_label": str(signal.get("confidence_label") or ""),
        "confidence_score": str(signal.get("confidence_score") or ""),
        "identity_match_label": str(signal.get("identity_match_label") or ""),
        "identity_match_score": str(signal.get("identity_match_score") or ""),
        "promotion_status": str(signal.get("promotion_status") or ""),
        "promotion_id": str(signal.get("promotion_id") or ""),
        "export_status": str(signal.get("export_status") or ""),
        "evidence_excerpt": str(signal.get("evidence_excerpt") or ""),
    }


def _public_web_evidence_csv_row(*, record: dict[str, Any], evidence: dict[str, Any]) -> dict[str, str]:
    return {
        "record_id": str(record.get("id") or ""),
        "candidate_name": str(record.get("candidate_name") or ""),
        "source_url": str(evidence.get("source_url") or ""),
        "source_domain": str(evidence.get("source_domain") or ""),
        "source_family": str(evidence.get("source_family") or ""),
        "source_title": str(evidence.get("source_title") or ""),
        "signal_ids": " | ".join(str(item or "") for item in list(evidence.get("signal_ids") or []) if str(item or "")),
        "signal_kinds": " | ".join(
            str(item or "") for item in list(evidence.get("signal_kinds") or []) if str(item or "")
        ),
        "signal_types": " | ".join(
            str(item or "") for item in list(evidence.get("signal_types") or []) if str(item or "")
        ),
        "identity_match_labels": " | ".join(
            str(item or "") for item in list(evidence.get("identity_match_labels") or []) if str(item or "")
        ),
        "max_confidence_score": str(evidence.get("max_confidence_score") or ""),
    }


def _public_web_promotion_csv_row(*, record: dict[str, Any], promotion: dict[str, Any]) -> dict[str, str]:
    metadata = dict(promotion.get("metadata") or {})
    return {
        "record_id": str(record.get("id") or promotion.get("record_id") or ""),
        "candidate_name": str(record.get("candidate_name") or promotion.get("candidate_name") or ""),
        "promotion_id": str(promotion.get("promotion_id") or ""),
        "signal_id": str(promotion.get("signal_id") or ""),
        "action": str(promotion.get("action") or ""),
        "promotion_status": str(promotion.get("promotion_status") or ""),
        "signal_kind": str(promotion.get("signal_kind") or ""),
        "signal_type": str(promotion.get("signal_type") or ""),
        "email_type": str(promotion.get("email_type") or ""),
        "new_value": str(promotion.get("new_value") or ""),
        "previous_value": str(promotion.get("previous_value") or ""),
        "source_url": str(promotion.get("source_url") or ""),
        "source_domain": str(promotion.get("source_domain") or ""),
        "confidence_label": str(promotion.get("confidence_label") or ""),
        "identity_match_label": str(promotion.get("identity_match_label") or ""),
        "operator": str(promotion.get("operator") or ""),
        "created_at": str(promotion.get("created_at") or ""),
        "note": str(promotion.get("note") or ""),
        "override_reason": str(metadata.get("override_reason") or promotion.get("override_reason") or ""),
        "override_validation_reason": str(
            metadata.get("override_validation_reason") or promotion.get("override_validation_reason") or ""
        ),
    }


def _public_web_signal_promoted_value(signal: dict[str, Any]) -> str:
    if str(signal.get("signal_kind") or "") == "email_candidate":
        return str(signal.get("normalized_value") or signal.get("value") or "").strip().lower()
    return str(signal.get("url") or signal.get("normalized_value") or signal.get("value") or "").strip()


def _public_web_signal_assertion_type(signal: dict[str, Any]) -> str:
    signal_kind = str(signal.get("signal_kind") or "").strip()
    signal_type = str(signal.get("signal_type") or "").strip()
    if signal_kind == "email_candidate":
        return "primary_email"
    mapping = {
        "personal_homepage": "homepage_url",
        "homepage": "homepage_url",
        "github_url": "github_url",
        "github": "github_url",
        "x_url": "x_url",
        "twitter_url": "x_url",
        "substack_url": "substack_url",
        "substack": "substack_url",
        "scholar_url": "scholar_url",
        "google_scholar_url": "scholar_url",
        "linkedin_url": "linkedin_url",
    }
    return mapping.get(signal_type, "profile_link_url" if signal_kind == "profile_link" else "")


def _validate_public_web_signal_promotable(signal: dict[str, Any], *, allow_unpublishable: bool = False) -> str:
    signal_kind = str(signal.get("signal_kind") or "").strip()
    promoted_value = _public_web_signal_promoted_value(signal)
    if signal_kind == "email_candidate" and not _public_web_value_is_probable_email(promoted_value):
        return "invalid_email_candidate"
    if not allow_unpublishable:
        if not bool(signal.get("publishable")):
            return "signal_not_publishable"
        if str(signal.get("suppression_reason") or "").strip():
            return "signal_suppressed"
        if signal_kind == "profile_link" and not bool(signal.get("clean_profile_link", True)):
            return "link_shape_not_clean"
    return ""


def _public_web_value_is_probable_email(value: str) -> bool:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 254:
        return False
    return bool(re.fullmatch(r"[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+", normalized))


def _public_web_model_safe_artifact_refs(refs: dict[str, Any]) -> dict[str, str]:
    allowed_keys = {
        "candidate_summary_path",
        "entry_links_path",
        "signals_path",
        "analysis_path",
        "evidence_slice_path",
    }
    return {
        key: str(value or "").strip()
        for key, value in dict(refs or {}).items()
        if key in allowed_keys and str(value or "").strip()
    }


def _public_web_model_safe_summary(summary: dict[str, Any]) -> dict[str, Any]:
    blocked_keys = {
        "artifact_root",
        "artifact_path",
        "raw_path",
        "raw_payload",
        "raw_html",
        "raw_pdf",
        "document_fetch_payload_path",
        "adjudication_payload_path",
        "search_checkpoint",
        "analysis_checkpoint",
    }
    return _public_web_strip_blocked_summary_fields(_storage_json_safe_payload(dict(summary or {})), blocked_keys)


_PUBLIC_WEB_MATERIALIZED_SIGNAL_METRIC_KEYS = (
    "signal_materialized_count",
    "email_signal_materialized_count",
    "profile_link_signal_materialized_count",
)


def _public_web_materialized_signal_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    signal_rows = [dict(row) for row in list(rows or []) if isinstance(row, dict)]
    by_kind = Counter(str(row.get("signal_kind") or "").strip() for row in signal_rows)
    return {
        "signal_materialized_count": len(signal_rows),
        "email_signal_materialized_count": int(by_kind.get("email_candidate") or 0),
        "profile_link_signal_materialized_count": int(by_kind.get("profile_link") or 0),
    }


def _public_web_strip_blocked_summary_fields(value: Any, blocked_keys: set[str]) -> Any:
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key or "")
            if normalized_key in blocked_keys:
                continue
            if normalized_key.endswith("_raw_path") or normalized_key.endswith("_payload_path"):
                continue
            sanitized[normalized_key] = _public_web_strip_blocked_summary_fields(item, blocked_keys)
        return sanitized
    if isinstance(value, list):
        return [_public_web_strip_blocked_summary_fields(item, blocked_keys) for item in value]
    return value


def _public_web_model_safe_signal_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    allowed_keys = {
        "query_id",
        "query_text",
        "provider_name",
        "result_rank",
        "fetchable",
        "reasons",
        "adjudication",
        "link_shape_warnings",
        "clean_profile_link",
    }
    return _storage_json_safe_payload(
        {key: value for key, value in dict(metadata or {}).items() if key in allowed_keys}
    )


def _group_public_web_signals(
    *,
    email_candidates: list[dict[str, Any]],
    profile_links: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "email_candidates_by_type": _group_public_web_items(email_candidates, key="email_type"),
        "profile_links_by_type": _group_public_web_items(profile_links, key="signal_type"),
        "suppressed_email_candidates": [
            item for item in email_candidates if str(item.get("suppression_reason") or "").strip()
        ],
    }


def _group_public_web_items(items: list[dict[str, Any]], *, key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        group_key = str(item.get(key) or "unknown").strip() or "unknown"
        grouped.setdefault(group_key, []).append(item)
    return grouped


def _public_web_evidence_links(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for signal in signals:
        source_url = str(signal.get("source_url") or signal.get("url") or "").strip()
        if not source_url:
            continue
        current = by_url.setdefault(
            source_url,
            {
                "source_url": source_url,
                "source_domain": str(signal.get("source_domain") or ""),
                "source_family": str(signal.get("source_family") or ""),
                "source_title": str(signal.get("source_title") or ""),
                "signal_ids": [],
                "signal_kinds": [],
                "signal_types": [],
                "identity_match_labels": [],
                "max_confidence_score": 0.0,
            },
        )
        for field_name, value in (
            ("signal_ids", signal.get("signal_id")),
            ("signal_kinds", signal.get("signal_kind")),
            ("signal_types", signal.get("signal_type")),
            ("identity_match_labels", signal.get("identity_match_label")),
        ):
            normalized_value = str(value or "").strip()
            if normalized_value and normalized_value not in current[field_name]:
                current[field_name].append(normalized_value)
        current["max_confidence_score"] = max(
            _coerce_public_web_float(current.get("max_confidence_score")),
            _coerce_public_web_float(signal.get("confidence_score")),
        )
    return sorted(
        by_url.values(),
        key=lambda item: (
            -_coerce_public_web_float(item.get("max_confidence_score")),
            str(item.get("source_domain") or ""),
            str(item.get("source_url") or ""),
        ),
    )


def _coerce_public_web_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _legacy_target_public_web_orchestrator_disabled_result(
    *,
    operation: str,
    canonical_endpoint: str = "/api/crm/records/public-web-search",
    record_id: str = "",
) -> dict[str, Any]:
    payload = {
        "status": "retired",
        "reason": "legacy_target_public_web_orchestrator_disabled",
        "operation": str(operation or "").strip(),
        "canonical_endpoint": canonical_endpoint,
        "migration_override_env": "SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS",
        "migration_override_status": "removed",
        "retirement_mode": "permanent_hard_disable",
        "report_visible": True,
        "normal_path": False,
        "legacy_bridge_used": False,
        "read_contract": {
            "source": "crm_records+person_public_web_assets",
            "fallback_used": False,
            "fail_closed": True,
            "legacy_target_candidates_used": False,
            "public_web_storage_bridge": "",
            "bridge_removal_phase": "retired",
        },
    }
    if str(record_id or "").strip():
        payload["record_id"] = str(record_id or "").strip()
    return payload


class CrmPublicWebOwner:
    """Domain owner for CRM Public Web planners, lifecycle, drains, and export."""

    def __init__(
        self,
        *,
        store: Any,
        command_kernel: CommandKernel,
        durable_runtime_writer: Any,
        runtime_dir: Path | str,
        acquisition_engine: Any,
        agent_runtime: Any,
        crm_writer: Any,
        person_asset_writer: Any,
        enqueue_person_search_index: Callable[..., Any],
        public_crm_record_payload: Callable[..., dict[str, Any]],
        sync_operation_run_from_workflow_command_control: Callable[..., dict[str, Any]],
        workflow_command_downstream_commands: Callable[..., list[dict[str, Any]]],
        workflow_command_waiting_prerequisite: Callable[..., dict[str, Any]],
        export_command_cancelled_owner_response: Callable[..., dict[str, Any]],
        publish_export_artifact_if_command_active: Callable[..., dict[str, Any]],
    ) -> None:
        self.store = store
        self._kernel = command_kernel
        self.durable_runtime_writer = durable_runtime_writer
        self.runtime_dir = runtime_dir
        self.acquisition_engine = acquisition_engine
        self.agent_runtime = agent_runtime
        self.crm_writer = crm_writer
        self.person_asset_writer = person_asset_writer
        # Injected cross-domain/spine callables, stored under the same names the
        # moved bodies already use so the bodies stay verbatim.
        self._enqueue_projection_person_search_index_for_person = enqueue_person_search_index
        self._public_crm_record_payload = public_crm_record_payload
        self._sync_operation_run_from_workflow_command_control = sync_operation_run_from_workflow_command_control
        self._workflow_command_downstream_commands = workflow_command_downstream_commands
        self._workflow_command_waiting_prerequisite = workflow_command_waiting_prerequisite
        self._export_command_cancelled_owner_response = export_command_cancelled_owner_response
        self._publish_export_artifact_if_command_active = publish_export_artifact_if_command_active

    def _crm_public_web_workflow_run_id(self, batch_id: str) -> str:
        normalized_batch_id = str(batch_id or "").strip()
        if not normalized_batch_id:
            return ""
        digest = hashlib.sha1(f"crm_public_web:{normalized_batch_id}".encode("utf-8")).hexdigest()[:24]
        return f"wf_crm_public_web_{digest}"

    def _crm_public_web_operation_id(self, batch_id: str) -> str:
        normalized_batch_id = str(batch_id or "").strip()
        if not normalized_batch_id:
            return ""
        digest = hashlib.sha1(f"crm_public_web_operation:{normalized_batch_id}".encode("utf-8")).hexdigest()[:24]
        return f"op_crm_public_web_{digest}"

    def _plan_crm_public_web_queue_batch_command(
        self,
        *,
        batch: dict[str, Any],
        runs: list[dict[str, Any]],
        job_payload: dict[str, Any],
        request_payload: dict[str, Any],
        source: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        batch_payload = dict(batch or {})
        batch_id = str(batch_payload.get("batch_id") or "").strip()
        workspace_id = str(batch_payload.get("workspace_id") or request_payload.get("workspace_id") or "default").strip() or "default"
        run_payloads = [dict(run) for run in list(runs or []) if isinstance(run, dict)]
        run_ids = _dedupe_texts(str(run.get("run_id") or "") for run in run_payloads)
        if not batch_id or not run_ids:
            return {}
        workflow_run_id = self._crm_public_web_workflow_run_id(batch_id)
        operation_id = self._crm_public_web_operation_id(batch_id)
        command_idempotency_key = crm_public_web_queue_batch_idempotency_key(
            workspace_id=workspace_id,
            batch_id=batch_id,
            run_ids=run_ids,
            queue_scope="crm_public_web_start",
        )
        if not workflow_run_id or not operation_id or not command_idempotency_key:
            return {}
        record_ids = _dedupe_texts(
            str(run.get("crm_record_id") or run.get("record_id") or "") for run in run_payloads
        )
        materialization_metadata = {
            "command_payload_storage": "workflow_commands",
            "write_owner": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            "public_web_storage_owner": "crm_public_web_v1",
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            "migration_phase": "W7_crm_public_web_queue_batch",
        }
        try:
            self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:crm_public_web_queue_batch_started",
                actor="crm_public_web_queue_batch_planner",
                source=str(source or "crm_public_web_start").strip(),
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_queue_batch",
                    "batch_id": batch_id,
                    "workspace_id": workspace_id,
                    "migration_phase": "W7_crm_public_web_queue_batch",
                },
            )
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{command_idempotency_key}:plan",
                actor="crm_public_web_queue_batch_planner",
                source=str(source or "crm_public_web_start").strip(),
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_queue_batch",
                    "command_type": CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
                    "idempotency_key": command_idempotency_key,
                    "payload": {
                        "batch_id": batch_id,
                        "workspace_id": workspace_id,
                        "job_payload": dict(job_payload or {}),
                        "request_payload": dict(request_payload or {}),
                        "run_ids": run_ids,
                        "record_ids": record_ids,
                        "run_count": len(run_ids),
                        "record_count": len(record_ids),
                        "runs": run_payloads,
                        "materialization_metadata": materialization_metadata,
                        "migration_phase": "W7_crm_public_web_queue_batch",
                        "source": str(source or "crm_public_web_start").strip(),
                        "reason": str(reason or "crm_public_web_queue_batch").strip(),
                    },
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "crm_public_web_queue_batch",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=command_idempotency_key,
        )

    def _plan_crm_public_web_start_queue_batch_command(
        self,
        *,
        record_ids: list[str],
        request_payload: dict[str, Any],
        source: str = "crm_public_web_start",
    ) -> dict[str, Any]:
        workspace_id = str(request_payload.get("workspace_id") or "default").strip() or "default"
        normalized_record_ids = _dedupe_texts(str(record_id or "").strip() for record_id in list(record_ids or []))
        if not normalized_record_ids:
            return {}
        normalized_request_payload = {
            **dict(request_payload or {}),
            "workspace_id": workspace_id,
            "record_ids": normalized_record_ids,
            "crm_record_ids": normalized_record_ids,
        }
        force_refresh = bool(normalized_request_payload.get("force_refresh"))
        caller_refresh_nonce = str(
            normalized_request_payload.get("refresh_nonce") or normalized_request_payload.get("nonce") or ""
        ).strip()
        refresh_nonce = caller_refresh_nonce
        if force_refresh and not refresh_nonce:
            refresh_nonce = f"force-{utc_compact_timestamp()}-{uuid.uuid4().hex[:12]}"
        if refresh_nonce:
            normalized_request_payload["refresh_nonce"] = refresh_nonce
            normalized_request_payload["nonce"] = refresh_nonce
        request_options = public_web_options_from_record(normalized_request_payload)
        request_intent_key = build_crm_public_web_batch_idempotency_key(
            workspace_id=workspace_id,
            requested_record_ids=normalized_record_ids,
            options=request_options,
            force_refresh=force_refresh,
            nonce=refresh_nonce,
        )
        digest = hashlib.sha1(
            json.dumps(
                {
                    "workspace_id": workspace_id,
                    "record_ids": normalized_record_ids,
                    "request_intent_key": request_intent_key,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        workflow_run_id = f"wf_crm_public_web_start_{digest}"
        operation_id = f"op_crm_public_web_start_{digest}"
        command_idempotency_key = f"{CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE}:start:{digest}"
        materialization_metadata = {
            "command_payload_storage": "workflow_commands",
            "write_owner": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            "public_web_storage_owner": "crm_public_web_v1",
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            "migration_phase": "W7_crm_public_web_queue_batch",
            "request_intent_key": request_intent_key,
        }
        command_payload = {
            "operation_planning_mode": "create_crm_public_web_batch_from_operation",
            "workspace_id": workspace_id,
            "record_ids": normalized_record_ids,
            "crm_record_ids": normalized_record_ids,
            "request_intent_key": request_intent_key,
            "request_payload": {
                **normalized_request_payload,
                "requested_by": str(normalized_request_payload.get("requested_by") or "api").strip() or "api",
                "metadata": {
                    **dict(normalized_request_payload.get("metadata") or {}),
                    "api_start_command_owner": "crm_public_web_queue_batch",
                    "request_intent_key": request_intent_key,
                },
            },
            "materialization_metadata": materialization_metadata,
            "migration_phase": "W7_crm_public_web_queue_batch",
            "source": str(source or "crm_public_web_start").strip() or "crm_public_web_start",
            "reason": "crm_public_web_start_queue_batch",
        }
        try:
            self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:crm_public_web_start_started",
                actor="crm_public_web_start_planner",
                source=str(source or "crm_public_web_start").strip() or "crm_public_web_start",
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_queue_batch",
                    "workspace_id": workspace_id,
                    "record_ids": normalized_record_ids,
                    "request_intent_key": request_intent_key,
                    "migration_phase": "W7_crm_public_web_queue_batch",
                },
            )
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{command_idempotency_key}:plan",
                actor="crm_public_web_start_planner",
                source=str(source or "crm_public_web_start").strip() or "crm_public_web_start",
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_queue_batch",
                    "command_type": CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
                    "idempotency_key": command_idempotency_key,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "crm_public_web_queue_batch",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=command_idempotency_key,
        )

    def _plan_crm_public_web_operation_queue_batch_command(
        self,
        *,
        operation_run: dict[str, Any],
        action: dict[str, Any],
        record_ids: list[str],
        request_payload: dict[str, Any],
        actor: str = "operation_runtime",
    ) -> dict[str, Any]:
        operation_run_id = str(operation_run.get("operation_run_id") or "").strip()
        action_id = str(action.get("action_id") or "").strip()
        workspace_id = str(operation_run.get("workspace_id") or request_payload.get("workspace_id") or "default").strip()
        workspace_id = workspace_id or "default"
        normalized_record_ids = _dedupe_texts(str(record_id or "").strip() for record_id in list(record_ids or []))
        if not operation_run_id or not action_id or not normalized_record_ids:
            return {}
        digest = hashlib.sha1(
            json.dumps(
                {
                    "operation_run_id": operation_run_id,
                    "action_id": action_id,
                    "workspace_id": workspace_id,
                    "record_ids": normalized_record_ids,
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        workflow_run_id = f"wf_crm_public_web_op_{digest}"
        command_idempotency_key = f"{CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE}:operation:{digest}"
        materialization_metadata = {
            "command_payload_storage": "workflow_commands",
            "write_owner": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            "public_web_storage_owner": "crm_public_web_v1",
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            "migration_phase": "W9_operation_crm_public_web_queue_batch",
        }
        command_payload = {
            "operation_planning_mode": "create_crm_public_web_batch_from_operation",
            "operation_run_id": operation_run_id,
            "action_id": action_id,
            "workspace_id": workspace_id,
            "record_ids": normalized_record_ids,
            "crm_record_ids": normalized_record_ids,
            "request_payload": {
                **dict(request_payload or {}),
                "workspace_id": workspace_id,
                "crm_record_ids": normalized_record_ids,
                "record_ids": normalized_record_ids,
                "requested_by": str(actor or "operation_runtime").strip() or "operation_runtime",
                "metadata": {
                    **dict(dict(request_payload or {}).get("metadata") or {}),
                    "operation_run_id": operation_run_id,
                    "action_id": action_id,
                    "operation_adapter": "w9_enrich_person_public_web_v1",
                },
            },
            "materialization_metadata": materialization_metadata,
            "migration_phase": "W9_operation_crm_public_web_queue_batch",
            "source": "operation_run_dispatch",
            "reason": "enrich_person_public_web_operation",
        }
        try:
            self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_run_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:crm_public_web_operation_started",
                actor="operation_crm_public_web_planner",
                source="operation_run_dispatch",
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_queue_batch",
                    "operation_run_id": operation_run_id,
                    "action_id": action_id,
                    "workspace_id": workspace_id,
                    "migration_phase": "W9_operation_crm_public_web_queue_batch",
                },
            )
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_run_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{command_idempotency_key}:plan",
                actor="operation_crm_public_web_planner",
                source="operation_run_dispatch",
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_queue_batch",
                    "command_type": CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
                    "idempotency_key": command_idempotency_key,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "crm_public_web_queue_batch",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=command_idempotency_key,
        )

    @staticmethod
    def _crm_public_web_phase_command_migration_phase(command_type: str) -> str:
        normalized = str(command_type or "").strip()
        return workflow_command_migration_step_id(normalized) or "W7f_crm_public_web_phase_command"

    def _plan_crm_public_web_run_phase_command(
        self,
        *,
        workflow_run_id: str,
        operation_id: str,
        batch_id: str,
        run_id: str,
        workspace_id: str,
        command_type: str,
        parent_command: dict[str, Any] | None = None,
        request_payload: dict[str, Any] | None = None,
        source: str = "crm_public_web_phase_planner",
    ) -> dict[str, Any]:
        normalized_type = str(command_type or "").strip()
        if normalized_type not in set(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES):
            return {}
        normalized_run_id = str(run_id or "").strip()
        normalized_batch_id = str(batch_id or "").strip()
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_workflow_run_id = str(workflow_run_id or "").strip()
        if not normalized_workflow_run_id or not normalized_batch_id or not normalized_run_id:
            return {}
        idempotency_key = crm_public_web_run_phase_idempotency_key(
            workspace_id=normalized_workspace_id,
            batch_id=normalized_batch_id,
            run_id=normalized_run_id,
            phase_command_type=normalized_type,
        )
        if not idempotency_key:
            return {}
        parent_payload = dict(parent_command or {})
        parent_causality = dict(dict(parent_payload.get("payload") or {}).get("causality") or {})
        parent_command_id = str(parent_payload.get("command_id") or "").strip()
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or parent_command_id
            or str(parent_payload.get("idempotency_key") or "").strip()
            or idempotency_key
        )
        run = self.store.get_crm_public_web_run(run_id=normalized_run_id) or {}
        crm_record_id = str(run.get("crm_record_id") or run.get("record_id") or "").strip()
        command_payload = {
            "batch_id": normalized_batch_id,
            "workspace_id": normalized_workspace_id,
            "run_id": normalized_run_id,
            "run_ids": [normalized_run_id],
            "run_count": 1,
            "crm_record_id": crm_record_id,
            "crm_record_ids": [crm_record_id] if crm_record_id else [],
            "record_count": 1 if crm_record_id else 0,
            "phase_command_type": normalized_type,
            "request_payload": dict(request_payload or {}),
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "source": str(source or "crm_public_web_phase_planner").strip(),
            "migration_phase": self._crm_public_web_phase_command_migration_phase(normalized_type),
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=normalized_workflow_run_id,
                operation_id=str(operation_id or parent_payload.get("operation_id") or "").strip(),
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor="crm_public_web_phase_planner",
                source=str(source or "crm_public_web_phase_planner").strip(),
                payload={
                    "workflow_type": "crm_public_web_search",
                    "stage_key": "crm_public_web_run_phase",
                    "stage_id": self._crm_public_web_phase_command_migration_phase(normalized_type),
                    "command_type": normalized_type,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 6,
                    "retry_policy": {
                        "kind": "crm_public_web_run_phase",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=normalized_workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_initial_crm_public_web_run_phase_commands(
        self,
        *,
        parent_command: dict[str, Any],
        batch_id: str,
        workspace_id: str,
        runs: list[dict[str, Any]],
        request_payload: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        parent_payload = dict(parent_command or {})
        workflow_run_id = str(parent_payload.get("workflow_run_id") or "").strip()
        operation_id = str(parent_payload.get("operation_id") or "").strip()
        planned: list[dict[str, Any]] = []
        for run in list(runs or []):
            run_payload = dict(run or {})
            run_id = str(run_payload.get("run_id") or "").strip()
            if not run_id:
                continue
            if str(run_payload.get("status") or "").strip() in {
                "completed",
                "completed_with_errors",
                "needs_review",
                "failed",
                "cancelled",
            }:
                continue
            command = self._plan_crm_public_web_run_phase_command(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                batch_id=batch_id,
                run_id=run_id,
                workspace_id=workspace_id,
                command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
                parent_command=parent_payload,
                request_payload=request_payload,
                source="crm_public_web_queue_batch_owner",
            )
            if command:
                planned.append(command)
        return planned

    def _crm_public_web_phase_command_status_record(self, command: dict[str, Any]) -> dict[str, Any]:
        payload = dict(command or {})
        command_payload = dict(payload.get("payload") or {})
        command_id = str(payload.get("command_id") or "").strip()
        command_type = str(payload.get("command_type") or "").strip()
        owner = str(payload.get("owner") or "").strip()
        record = {
            "command_id": command_id,
            "command_type": command_type,
            "owner": owner,
            "workflow_run_id": str(payload.get("workflow_run_id") or "").strip(),
            "operation_id": str(payload.get("operation_id") or "").strip(),
            "batch_id": str(command_payload.get("batch_id") or "").strip(),
            "run_id": str(command_payload.get("run_id") or "").strip(),
            "crm_record_id": str(command_payload.get("crm_record_id") or "").strip(),
            "status": str(payload.get("status") or "").strip(),
            "attempt": _coerce_int(payload.get("attempt"), 0),
            "max_attempts": _coerce_int(payload.get("max_attempts"), 0),
            "stage_id": str(payload.get("stage_id") or "").strip(),
            "causal_group_id": str(payload.get("causal_group_id") or command_payload.get("causal_group_id") or "").strip(),
            "parent_command_id": str(
                payload.get("parent_command_id") or command_payload.get("parent_command_id") or ""
            ).strip(),
            "source_event_id": str(payload.get("source_event_id") or "").strip(),
            "readiness_effect": str(payload.get("readiness_effect") or "").strip(),
            "last_error": str(payload.get("last_error") or "").strip(),
            "created_at": str(payload.get("created_at") or "").strip(),
            "updated_at": str(payload.get("updated_at") or "").strip(),
            "control_api": {
                "detail": f"/api/workflow/commands/{command_id}" if command_id else "",
                "cancel": f"/api/workflow/commands/{command_id}/cancel" if command_id else "",
                "retry": f"/api/workflow/commands/{command_id}/retry" if command_id else "",
                "resume": f"/api/workflow/commands/{command_id}/resume" if command_id else "",
            },
            "control_state": self._kernel._workflow_command_control_state_record(
                command_status=str(payload.get("status") or "").strip(),
                command_type=command_type,
                owner=owner,
            ),
            "display_contract": self._kernel._workflow_command_display_contract_record(
                command_type=command_type,
                owner=owner,
            ),
            "activity_detail_source": "/api/workflow/commands/{command_id}",
        }
        return {key: value for key, value in record.items() if value not in ("", [], {})}

    def _crm_public_web_run_control_state_record(self, run: dict[str, Any]) -> dict[str, Any]:
        payload = dict(run or {})
        status = str(payload.get("status") or "").strip()
        can_cancel = bool(status and status not in set(PUBLIC_WEB_TERMINAL_STATUSES))
        can_retry = bool(status in set(PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES))
        disabled_reasons: dict[str, str] = {}
        if not can_cancel:
            disabled_reasons["cancel"] = (
                "public_web_run_terminal" if status in set(PUBLIC_WEB_TERMINAL_STATUSES) else "public_web_run_status_missing"
            )
        if not can_retry:
            disabled_reasons["retry"] = (
                "public_web_run_not_retryable" if status else "public_web_run_status_missing"
            )
        return {
            "schema_version": "crm_public_web_run_control_state_v1",
            "source_of_truth": "crm_public_web_owner.run_control_state",
            "owner": "crm_public_web_owner",
            "run_id": str(payload.get("run_id") or "").strip(),
            "run_status": status,
            "can_cancel": can_cancel,
            "can_retry": can_retry,
            "allowed_actions": [
                action
                for action, allowed in (
                    ("cancel", can_cancel),
                    ("retry", can_retry),
                )
                if allowed
            ],
            "disabled_reasons": disabled_reasons,
            "fallback_status": "fail_closed",
        }

    def _crm_public_web_run_display_contract_record(self, run: dict[str, Any]) -> dict[str, Any]:
        payload = dict(run or {})
        return {
            "schema_version": "crm_public_web_run_display_contract_v1",
            "source_of_truth": "crm_public_web_owner.run_display_contract",
            "owner": "crm_public_web_owner",
            "display_label": "Public Web Search",
            "display_category": "crm_public_web",
            "description": "Candidate-level Public Web search and review run.",
            "run_id": str(payload.get("run_id") or "").strip(),
            "run_status": str(payload.get("status") or "").strip(),
            "fallback_status": "fail_closed",
        }

    def _crm_public_web_phase_command_display_line(self, phase_commands: dict[str, Any]) -> str:
        summary = dict(phase_commands or {})
        phase_order = [
            str(item or "").strip()
            for item in list(summary.get("phase_order") or [])
            if str(item or "").strip()
        ]
        phase_total = len(phase_order)
        if not phase_total:
            return ""
        completed_count = _coerce_int(summary.get("completed_phase_count"), 0)
        current = dict(summary.get("current_command") or {})
        control_state = dict(current.get("control_state") or {})
        command_type = str(current.get("command_type") or "").strip()
        display_label = str(workflow_command_product_label_zh(command_type) or "").strip()
        command_status = str(control_state.get("command_status") or current.get("status") or "").strip()
        if not display_label:
            return ""
        command_status_label = {
            "queued": "已排队",
            "claimed": "已领取",
            "running": "执行中",
            "retry_wait": "等待重试",
            "succeeded": "已完成",
            "failed": "失败",
            "failed_terminal": "终态失败",
            "cancelled": "已取消",
        }.get(command_status, command_status)
        return (
            f"Public Web 阶段 {completed_count}/{phase_total} · "
            f"当前 {display_label}"
            f"{f'（{command_status_label}）' if command_status_label else ''}"
        )

    def _crm_public_web_run_api_record(
        self,
        run: dict[str, Any],
        *,
        phase_commands: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = dict(run or {})
        command_summary = dict(phase_commands or payload.get("phase_commands") or {})
        if command_summary:
            payload["phase_commands"] = command_summary
        payload["phase_command_display_line"] = self._crm_public_web_phase_command_display_line(command_summary)
        payload["run_control_state"] = self._crm_public_web_run_control_state_record(payload)
        payload["run_display_contract"] = self._crm_public_web_run_display_contract_record(payload)
        return payload

    def _crm_public_web_run_api_records(
        self,
        runs: list[dict[str, Any]],
        *,
        phase_commands_by_run_id: dict[str, dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        command_summaries = dict(phase_commands_by_run_id or {})
        return [
            self._crm_public_web_run_api_record(
                dict(run),
                phase_commands=command_summaries.get(str(dict(run).get("run_id") or "").strip(), {}),
            )
            for run in list(runs or [])
        ]

    def _crm_public_web_workflow_run_ids_for_batch(self, batch_id: str) -> list[str]:
        normalized_batch_id = str(batch_id or "").strip()
        if not normalized_batch_id:
            return []
        candidates: list[str] = []
        batch = self.store.get_crm_public_web_batch(batch_id=normalized_batch_id)
        metadata = dict(dict(batch or {}).get("metadata") or {})
        for value in (
            metadata.get("workflow_run_id"),
            metadata.get("phase_command_workflow_run_id"),
            self._crm_public_web_workflow_run_id(normalized_batch_id),
        ):
            normalized = str(value or "").strip()
            if normalized and normalized not in candidates:
                candidates.append(normalized)
        return candidates

    def _crm_public_web_phase_command_summaries_for_runs(
        self,
        runs: list[dict[str, Any]],
        *,
        workspace_id: str = "default",
    ) -> dict[str, dict[str, Any]]:
        run_payloads = [dict(run or {}) for run in list(runs or []) if isinstance(run, dict)]
        run_ids = {
            str(run.get("run_id") or "").strip()
            for run in run_payloads
            if str(run.get("run_id") or "").strip()
        }
        if not run_ids:
            return {}
        batch_ids = _dedupe_texts(str(run.get("batch_id") or "").strip() for run in run_payloads)
        workflow_run_ids_by_batch: dict[str, list[str]] = {}
        for run in run_payloads:
            batch_id = str(run.get("batch_id") or "").strip()
            if not batch_id:
                continue
            checkpoint = dict(run.get("analysis_checkpoint") or {})
            for value in (
                checkpoint.get("phase_command_workflow_run_id"),
                checkpoint.get("workflow_run_id"),
            ):
                workflow_run_id = str(value or "").strip()
                if workflow_run_id and workflow_run_id not in workflow_run_ids_by_batch.setdefault(batch_id, []):
                    workflow_run_ids_by_batch[batch_id].append(workflow_run_id)
        commands: list[dict[str, Any]] = []
        seen_command_ids: set[str] = set()
        for batch_id in batch_ids:
            workflow_run_ids = [
                *workflow_run_ids_by_batch.get(batch_id, []),
                *self._crm_public_web_workflow_run_ids_for_batch(batch_id),
            ]
            for workflow_run_id in _dedupe_texts(workflow_run_ids):
                for command in self.store.list_workflow_commands(
                    workflow_run_id=workflow_run_id,
                    owner=CRM_PUBLIC_WEB_PHASE_OWNER,
                    limit=1000,
                ):
                    command_id = str(command.get("command_id") or "").strip()
                    if not command_id or command_id in seen_command_ids:
                        continue
                    command_payload = dict(command.get("payload") or {})
                    if str(command_payload.get("run_id") or "").strip() not in run_ids:
                        continue
                    if str(command.get("command_type") or "").strip() not in set(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES):
                        continue
                    seen_command_ids.add(command_id)
                    commands.append(dict(command))
        phase_order = {command_type: index for index, command_type in enumerate(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES)}
        commands.sort(
            key=lambda item: (
                str(dict(item.get("payload") or {}).get("run_id") or ""),
                phase_order.get(str(item.get("command_type") or ""), 999),
                str(item.get("created_at") or ""),
                str(item.get("command_id") or ""),
            )
        )
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for command in commands:
            run_id = str(dict(command.get("payload") or {}).get("run_id") or "").strip()
            if run_id:
                grouped[run_id].append(self._crm_public_web_phase_command_status_record(command))

        def summary_for(run_id: str) -> dict[str, Any]:
            items = grouped.get(run_id, [])
            by_phase = {
                str(item.get("command_type") or ""): item
                for item in items
                if str(item.get("command_type") or "").strip()
            }
            phase_count = len(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES)
            completed_phase_count = sum(
                1
                for command_type in CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES
                if str(dict(by_phase.get(command_type) or {}).get("status") or "").strip() == "succeeded"
            )
            current = next(
                (
                    item
                    for item in items
                    if str(item.get("status") or "").strip()
                    not in {"succeeded", "failed", "cancelled", "skipped"}
                ),
                items[-1] if items else {},
            )
            summary = {
                "contract": "crm_public_web_phase_command_status_v1",
                "source": "workflow_commands",
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "workspace_id": str(workspace_id or "default").strip() or "default",
                "run_id": run_id,
                "fallback_used": False,
                "module_state_mutated": False,
                "phase_order": list(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES),
                "phase_count": phase_count,
                "completed_phase_count": completed_phase_count,
                "command_count": phase_count,
                "materialized_command_count": len(items),
                "commands": items,
                "by_phase": by_phase,
                "current_command": current,
            }
            summary["display_line"] = self._crm_public_web_phase_command_display_line(summary)
            return summary

        return {run_id: summary_for(run_id) for run_id in sorted(run_ids)}

    def _cancel_running_crm_public_web_phase_command(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        command_type = str(command_payload.get("command_type") or "").strip()
        body = dict(command_payload.get("payload") or {})
        run_id = str(body.get("run_id") or "").strip()
        batch_id = str(body.get("batch_id") or "").strip()
        workspace_id = str(body.get("workspace_id") or "default").strip() or "default"
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "cancelled_by_owner_specific_command_control").strip()
        if not run_id:
            return {
                "status": "invalid",
                "reason": "crm_public_web_running_cancel_missing_run_id",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        cancel_result = cancel_crm_public_web_run(
            store=self.store,
            run_id=run_id,
            reason=reason,
            operator=actor,
        )
        cancel_status = str(cancel_result.get("status") or "").strip()
        if cancel_status not in {"cancelled", "skipped"}:
            return {
                "status": cancel_status or "failed",
                "reason": str(cancel_result.get("reason") or "crm_public_web_running_cancel_failed"),
                "cancel_result": cancel_result,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        run = dict(cancel_result.get("run") or {})
        if run:
            self._interrupt_crm_public_web_workers_for_run(
                run,
                reason=reason,
                operator=actor,
            )
        activity_rows = self.store.list_workflow_activity_runs(
            command_id=command_id,
            activity_type=command_type,
            limit=1,
        )
        activity = dict(activity_rows[0]) if activity_rows else {}
        attempts = (
            self.store.list_workflow_activity_attempts(
                activity_run_id=str(activity.get("activity_run_id") or ""),
                limit=1,
            )
            if activity
            else []
        )
        attempt = dict(attempts[0]) if attempts else {}
        if not activity or not attempt:
            activity, attempt = self._kernel._start_workflow_command_activity_attempt(
                command_payload,
                activity_type=command_type,
                owner=CRM_PUBLIC_WEB_PHASE_OWNER,
                phase="owner_specific_cancel",
                lease_owner=actor,
                provider="crm_public_web_runtime",
                provider_request_ref=run_id,
                input_payload={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "phase_command_type": command_type,
                    "control_action": "cancel",
                },
                entity_counts={"crm_public_web_run_count": 1},
                metadata={
                    "owner_specific_control": True,
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                },
                attempt_suffix="owner_specific_cancel",
            )
        final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
            activity=activity,
            attempt=attempt,
            status="cancelled",
            phase="cancelled",
            output={
                "batch_id": batch_id,
                "run_id": run_id,
                "workspace_id": workspace_id,
                "phase_command_type": command_type,
                "cancel_result": cancel_result,
            },
            entity_counts={"crm_public_web_run_count": 1},
            error={"reason": reason, "control_action": "cancel"},
            metadata={
                "owner_specific_control": True,
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
            attempt_status="cancelled",
        )
        delta = self._record_crm_public_web_phase_entity_delta(
            command=command_payload,
            activity=final_activity or activity,
            attempt=final_attempt or attempt,
            command_type=command_type,
            run_id=run_id,
            batch_id=batch_id,
            workspace_id=workspace_id,
            run_status="cancelled",
            worker_status="cancelled",
            delta_status="cancelled",
            reason="crm_public_web_run_cancelled_by_command_control",
            phase_result={"cancel_result": cancel_result},
        )
        updated = self.store.cancel_workflow_command(
            command_id,
            reason=reason,
            actor=actor,
            result={
                "control_source": "api.workflow_command_owner_specific_cancel",
                "control_action": "cancel",
                "owner_specific_control": True,
                "run_id": run_id,
                "batch_id": batch_id,
                "activity_run_id": str((final_activity or activity).get("activity_run_id") or "").strip(),
                "activity_attempt_id": str((final_attempt or attempt).get("attempt_id") or "").strip(),
                "entity_delta_id": str(delta.get("delta_id") or "").strip(),
                "cancel_result_status": cancel_status,
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
            from_statuses=("claimed", "running"),
        )
        if not updated:
            return {
                "status": "invalid",
                "reason": "workflow_command_owner_specific_cancel_not_applied",
                "workflow_command": self._kernel._workflow_command_api_record(
                    self.store.get_workflow_command(command_id) or command_payload
                ),
                **self._kernel._workflow_command_control_response_policy_records(
                    self.store.get_workflow_command(command_id) or command_payload
                ),
                "cancel_result": cancel_result,
                "module_state_mutated": True,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated,
            control_action="cancel",
            actor=actor,
            source="api.workflow_command_owner_specific_cancel",
        )
        return {
            "status": "cancelled",
            "workflow_command": self._kernel._workflow_command_api_record(updated),
            "operation_sync": operation_sync,
            **self._kernel._workflow_command_control_response_policy_records(updated),
            "cancel_result": cancel_result,
            "workflow_activity": final_activity or activity,
            "workflow_activity_attempt": final_attempt or attempt,
            "workflow_entity_delta": delta,
            "module_state_mutated": True,
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    def _cancel_running_crm_public_web_queue_batch_before_phase_commands(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        body = dict(command_payload.get("payload") or {})
        result_payload = dict(command_payload.get("result") or {})
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "cancelled_before_crm_public_web_phase_commands").strip()
        force = _coerce_bool(payload.get("force"), False)
        downstream = self._workflow_command_downstream_commands(command_payload)
        if downstream:
            return {
                "status": "invalid",
                "reason": "crm_public_web_queue_batch_cancel_blocked_after_phase_planned",
                "downstream_command_count": len(downstream),
                "downstream_command_ids": [
                    str(row.get("command_id") or "").strip()
                    for row in downstream
                    if str(row.get("command_id") or "").strip()
                ],
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        if self._kernel._workflow_command_lease_active(command_payload) and not force:
            return {
                "status": "invalid",
                "reason": "workflow_command_running_cancel_requires_expired_lease_or_force",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        workspace_id = str(body.get("workspace_id") or result_payload.get("workspace_id") or "default").strip() or "default"
        batch_id = str(body.get("batch_id") or result_payload.get("batch_id") or "").strip()
        run_ids = [
            str(item or "").strip()
            for item in list(body.get("run_ids") or result_payload.get("run_ids") or [])
            if str(item or "").strip()
        ]
        batch = self.store.get_crm_public_web_batch(batch_id=batch_id) if batch_id else None
        if batch is not None:
            batch_workspace_id = str(batch.get("workspace_id") or "default").strip() or "default"
            if batch_workspace_id != workspace_id:
                return {
                    "status": "invalid",
                    "reason": "public_web_batch_workspace_mismatch",
                    "workspace_id": workspace_id,
                    "batch_id": batch_id,
                    "batch_workspace_id": batch_workspace_id,
                    "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                    **self._kernel._workflow_command_control_response_policy_records(command_payload),
                    "module_state_mutated": False,
                    "owner_specific_control": True,
                    "contract": "w11_workflow_command_owner_specific_control_v1",
                }
        runs: list[dict[str, Any]] = []
        if batch_id:
            runs.extend(self.store.list_crm_public_web_runs(batch_id=batch_id, workspace_id=workspace_id, limit=500))
        for run_id in run_ids:
            run = self.store.get_crm_public_web_run(run_id=run_id)
            if run and str(run.get("run_id") or "").strip() not in {
                str(existing.get("run_id") or "").strip() for existing in runs
            }:
                runs.append(run)
        validation = self._validate_crm_public_web_runs([dict(run) for run in runs], workspace_id=workspace_id)
        if isinstance(validation, dict):
            return {
                **validation,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        cancelled_runs: list[dict[str, Any]] = []
        for run in runs:
            run_id = str(run.get("run_id") or "").strip()
            if not run_id:
                continue
            cancel_result = cancel_crm_public_web_run(
                store=self.store,
                run_id=run_id,
                reason=reason,
                operator=actor,
            )
            if str(cancel_result.get("status") or "").strip() in {"cancelled", "skipped"}:
                cancelled_run = dict(cancel_result.get("run") or {})
                if cancelled_run:
                    cancelled_runs.append(cancelled_run)
        cancelled_batch: dict[str, Any] = {}
        if batch:
            metadata = dict(batch.get("metadata") or {})
            metadata.update(
                {
                    "cancelled_by": actor,
                    "cancel_reason": reason,
                    "control_source": "api.workflow_command_owner_specific_cancel",
                    "phase_command_planned": False,
                }
            )
            summary = dict(batch.get("summary") or {})
            summary.update(
                {
                    "cancelled_run_count": len(cancelled_runs),
                    "phase_command_planned": False,
                }
            )
            cancelled_batch = self.store.upsert_crm_public_web_batch(
                {
                    **batch,
                    "status": "cancelled",
                    "summary": summary,
                    "metadata": metadata,
                }
            )
        updated = self.store.cancel_workflow_command(
            command_id,
            reason=reason,
            actor=actor,
            result={
                "control_source": "api.workflow_command_owner_specific_cancel",
                "control_action": "cancel",
                "owner_specific_control": True,
                "cancel_boundary": "crm_public_web_queue_batch_before_phase_commands",
                "batch_id": batch_id,
                "batch_cancelled": bool(cancelled_batch),
                "run_cancelled_count": len(cancelled_runs),
                "phase_command_planned": False,
                "force": force,
            },
            from_statuses=("claimed", "running"),
        )
        if not updated:
            latest = self.store.get_workflow_command(command_id) or command_payload
            return {
                "status": "invalid",
                "reason": "workflow_command_owner_specific_cancel_not_applied",
                "workflow_command": self._kernel._workflow_command_api_record(latest),
                **self._kernel._workflow_command_control_response_policy_records(latest),
                "crm_public_web_batch": cancelled_batch or batch or {},
                "crm_public_web_runs": cancelled_runs,
                "module_state_mutated": bool(cancelled_batch or cancelled_runs),
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated,
            control_action="cancel",
            actor=actor,
            source="api.workflow_command_owner_specific_cancel",
        )
        return {
            "status": "cancelled",
            "workflow_command": self._kernel._workflow_command_api_record(updated),
            "operation_sync": operation_sync,
            "crm_public_web_batch": cancelled_batch or batch or {},
            "crm_public_web_runs": cancelled_runs,
            **self._kernel._workflow_command_control_response_policy_records(updated),
            "module_state_mutated": bool(cancelled_batch or cancelled_runs),
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    def _resume_running_crm_public_web_phase_command(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        command_type = str(command_payload.get("command_type") or "").strip()
        body = dict(command_payload.get("payload") or {})
        run_id = str(body.get("run_id") or "").strip()
        batch_id = str(body.get("batch_id") or "").strip()
        workspace_id = str(body.get("workspace_id") or "default").strip() or "default"
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "resumed_by_owner_specific_command_control").strip()
        force = _coerce_bool(payload.get("force"), False)
        if not run_id:
            return {
                "status": "invalid",
                "reason": "crm_public_web_running_resume_missing_run_id",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        if self._kernel._workflow_command_lease_active(command_payload) and not force:
            return {
                "status": "invalid",
                "reason": "workflow_command_running_resume_requires_expired_lease_or_force",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        activity_rows = self.store.list_workflow_activity_runs(
            command_id=command_id,
            activity_type=command_type,
            limit=1,
        )
        activity = dict(activity_rows[0]) if activity_rows else {}
        attempts = (
            self.store.list_workflow_activity_attempts(
                activity_run_id=str(activity.get("activity_run_id") or ""),
                limit=1,
            )
            if activity
            else []
        )
        attempt = dict(attempts[0]) if attempts else {}
        if not activity or not attempt:
            activity, attempt = self._kernel._start_workflow_command_activity_attempt(
                command_payload,
                activity_type=command_type,
                owner=CRM_PUBLIC_WEB_PHASE_OWNER,
                phase="owner_specific_resume",
                lease_owner=actor,
                provider="crm_public_web_runtime",
                provider_request_ref=run_id,
                input_payload={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "phase_command_type": command_type,
                    "control_action": "resume",
                    "force": force,
                },
                entity_counts={"crm_public_web_run_count": 1},
                metadata={
                    "owner_specific_control": True,
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                },
                attempt_suffix="owner_specific_resume",
            )
        final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
            activity=activity,
            attempt=attempt,
            status="queued",
            phase="owner_specific_resume_queued",
            output={
                "batch_id": batch_id,
                "run_id": run_id,
                "workspace_id": workspace_id,
                "phase_command_type": command_type,
                "control_action": "resume",
                "force": force,
                "reason": reason,
            },
            entity_counts={"crm_public_web_run_count": 1},
            metadata={
                "owner_specific_control": True,
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                "resume_mode": "owner_specific_requeue",
            },
            attempt_status="succeeded",
        )
        delta = self._record_crm_public_web_phase_entity_delta(
            command=command_payload,
            activity=final_activity or activity,
            attempt=final_attempt or attempt,
            command_type=command_type,
            run_id=run_id,
            batch_id=batch_id,
            workspace_id=workspace_id,
            run_status="resume_queued",
            worker_status="resume_queued",
            delta_status="queued",
            reason="crm_public_web_phase_command_resumed_by_command_control",
            phase_result={"control_action": "resume", "force": force, "reason": reason},
        )
        updated = self.store.mark_workflow_command_partial_progress(
            command_id,
            result={
                **dict(command_payload.get("result") or {}),
                "control_source": "api.workflow_command_owner_specific_resume",
                "control_action": "resume",
                "owner_specific_control": True,
                "run_id": run_id,
                "batch_id": batch_id,
                "activity_run_id": str((final_activity or activity).get("activity_run_id") or "").strip(),
                "activity_attempt_id": str((final_attempt or attempt).get("attempt_id") or "").strip(),
                "entity_delta_id": str(delta.get("delta_id") or "").strip(),
                "resume_mode": "owner_specific_requeue",
                "force": force,
                "reason": reason,
            },
        )
        if not updated:
            return {
                "status": "invalid",
                "reason": "workflow_command_owner_specific_resume_not_applied",
                "workflow_command": self._kernel._workflow_command_api_record(
                    self.store.get_workflow_command(command_id) or command_payload
                ),
                **self._kernel._workflow_command_control_response_policy_records(
                    self.store.get_workflow_command(command_id) or command_payload
                ),
                "module_state_mutated": True,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated,
            control_action="resume",
            actor=actor,
            source="api.workflow_command_owner_specific_resume",
        )
        return {
            "status": "queued",
            "workflow_command": self._kernel._workflow_command_api_record(updated),
            "operation_sync": operation_sync,
            **self._kernel._workflow_command_control_response_policy_records(updated),
            "workflow_activity": final_activity or activity,
            "workflow_activity_attempt": final_attempt or attempt,
            "workflow_entity_delta": delta,
            "module_state_mutated": True,
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    def _dispatch_person_public_web_enrichment_operation(
        self,
        *,
        operation_run: dict[str, Any],
        action: dict[str, Any],
        actor: str,
    ) -> dict[str, Any]:
        if str(action.get("approval_policy") or "").strip() == "required" and str(
            action.get("approval_status") or ""
        ).strip() != "approved":
            return {
                "status": "approval_required",
                "action": action,
                "operation_run": operation_run,
                "module_state_mutated": False,
                "contract": "w9_operation_run_dispatch_v1",
            }
        if str(operation_run.get("status") or "").strip() in {"completed", "failed", "cancelled"}:
            return {
                "status": "invalid",
                "reason": f"terminal operation cannot be dispatched: {operation_run.get('status')}",
                "operation_run": operation_run,
                "action": action,
                "module_state_mutated": False,
                "contract": "w9_operation_run_dispatch_v1",
            }
        target_ref = dict(action.get("target_ref") or {})
        input_payload = dict(action.get("input") or {})
        workspace_id = str(operation_run.get("workspace_id") or target_ref.get("workspace_id") or "default").strip()
        workspace_id = workspace_id or "default"
        record_ids = _coerce_public_web_record_ids(
            input_payload.get("crm_record_ids")
            or input_payload.get("record_ids")
            or input_payload.get("crm_record_id")
            or target_ref.get("crm_record_ids")
            or target_ref.get("record_ids")
            or target_ref.get("crm_record_id")
            or target_ref.get("record_id")
        )
        person_identity_key = str(
            input_payload.get("person_identity_key") or target_ref.get("person_identity_key") or ""
        ).strip()
        if person_identity_key and not record_ids:
            record = self.store.get_crm_record_by_person_identity(person_identity_key, workspace_id=workspace_id)
            if record:
                record_ids = [str(record.get("crm_record_id") or "").strip()]
        record_ids = _dedupe_texts(record_id for record_id in record_ids if record_id)
        if not record_ids:
            return {
                "status": "invalid",
                "reason": "enrich_person_public_web requires crm_record_id or person_identity_key with CRM record",
                "operation_run": operation_run,
                "action": action,
                "module_state_mutated": False,
                "contract": "w9_operation_run_dispatch_v1",
            }
        missing_record_ids = [
            record_id
            for record_id in record_ids
            if not self.store.get_crm_record(record_id)
            or str((self.store.get_crm_record(record_id) or {}).get("workspace_id") or "default").strip()
            != workspace_id
        ]
        if missing_record_ids:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "missing_record_ids": missing_record_ids,
                "operation_run": operation_run,
                "action": action,
                "module_state_mutated": False,
                "contract": "w9_operation_run_dispatch_v1",
            }
        command = self._plan_crm_public_web_operation_queue_batch_command(
            operation_run=operation_run,
            action=action,
            record_ids=record_ids,
            request_payload={
                **input_payload,
                "workspace_id": workspace_id,
                "crm_record_ids": record_ids,
                "record_ids": record_ids,
            },
            actor=actor,
        )
        if not command:
            return {
                "status": "failed",
                "reason": "crm_public_web_operation_command_enqueue_failed",
                "operation_run": operation_run,
                "action": action,
                "module_state_mutated": False,
                "contract": "w9_operation_run_dispatch_v1",
            }
        workflow_ref = {
            "workflow_run_id": str(command.get("workflow_run_id") or ""),
            "command_id": str(command.get("command_id") or ""),
            "command_type": str(command.get("command_type") or ""),
            "owner": str(command.get("owner") or ""),
        }
        next_operation = self.store.repos.workflow_runtime.update_operation_state(
            str(operation_run.get("operation_run_id") or ""),
            status="planned",
            progress_patch={"phase": "workflow_command_planned", "crm_record_ids": record_ids, **workflow_ref},
            workflow_ref_patch=workflow_ref,
            result_ref_patch={"crm_record_ids": record_ids},
            metadata_patch={"last_planned_command_id": workflow_ref["command_id"], "dispatch_actor": actor},
        )
        self.store.repos.workflow_runtime.update_action_state(
            str(action.get("action_id") or ""),
            status="planned",
            metadata_patch={"last_operation_run_id": operation_run.get("operation_run_id")},
        )
        event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=workspace_id,
            event_stream_id=str(operation_run.get("operation_run_id") or ""),
            operation_run_id=str(operation_run.get("operation_run_id") or ""),
            action_id=str(action.get("action_id") or ""),
            event_family="operation_event",
            event_type="OperationCommandPlanned",
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:OperationCommandPlanned:{workflow_ref['command_id']}"
            ),
            actor=actor,
            source="api.operation_run_dispatch",
            payload={**workflow_ref, "crm_record_ids": record_ids, "module_state_mutated": False},
        )
        return {
            "status": "planned",
            "action": self.store.repos.workflow_runtime.get_action(str(action.get("action_id") or "")) or action,
            "operation_run": next_operation,
            "workflow_command": self._kernel._workflow_command_api_record(command),
            "events": [event],
            "module_state_mutated": False,
            "contract": "w9_operation_run_dispatch_v1",
        }

    def _require_crm_public_web_body_workspace_id(
        self,
        payload: dict[str, Any] | None,
        *,
        operation: str,
    ) -> str | dict[str, Any]:
        workspace_id = str(dict(payload or {}).get("workspace_id") or "").strip()
        if not workspace_id:
            return {
                "status": "invalid",
                "reason": "crm_public_web_workspace_id_required",
                "operation": operation,
                "workspace_id_required": True,
            }
        return workspace_id

    def start_crm_record_public_web_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="start")
        if isinstance(workspace_result, dict):
            return self._with_crm_public_web_contract(workspace_result, operation="start")
        workspace_id = workspace_result
        record_ids_result = self._prepare_crm_public_web_record_ids(
            normalized.get("crm_record_ids") or normalized.get("record_ids") or normalized.get("record_id"),
            require_non_empty=True,
            workspace_id=workspace_id,
        )
        if isinstance(record_ids_result, dict):
            return record_ids_result
        normalized["record_ids"] = record_ids_result
        normalized["crm_record_ids"] = record_ids_result
        normalized["workspace_id"] = workspace_id
        command = self._plan_crm_public_web_start_queue_batch_command(
            record_ids=record_ids_result,
            request_payload=normalized,
            source="crm_public_web_start",
        )
        if not command:
            return self._with_crm_public_web_contract(
                {
                    "status": "failed",
                    "reason": "crm_public_web_queue_batch_command_enqueue_failed",
                    "batch": {},
                    "runs": [],
                    "summary": {},
                    "job": {},
                    "public_web_owner_sync": {
                        "status": "owned",
                        "owner": "crm_public_web_v1",
                        "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                        "operation": "start",
                    },
                },
                operation="start",
            )
        worker_summary = self._drain_crm_public_web_queue_batch_commands(
            {
                "workflow_run_id": str(command.get("workflow_run_id") or ""),
                "command_limit": 1,
                "source": "crm_public_web_start_inline_owner_drain",
            }
        )
        refreshed_command = self.store.get_workflow_command(str(command.get("command_id") or "")) or command
        command_payload = dict(refreshed_command.get("payload") or {})
        command_result = dict(refreshed_command.get("result") or {})
        batch_id = str(command_payload.get("batch_id") or command_result.get("batch_id") or "").strip()
        batch = self.store.get_crm_public_web_batch(batch_id=batch_id) if batch_id else None
        refreshed_runs = (
            self.store.list_crm_public_web_runs(
                batch_id=batch_id,
                workspace_id=workspace_id,
            )
            if batch_id
            else []
        )
        refreshed_runs = self._crm_public_web_runs_with_materialized_signal_metrics(refreshed_runs)
        phase_commands_by_run_id = self._crm_public_web_phase_command_summaries_for_runs(
            [dict(run) for run in refreshed_runs],
            workspace_id=workspace_id,
        )
        refreshed_runs = self._crm_public_web_run_api_records(
            [dict(run) for run in refreshed_runs],
            phase_commands_by_run_id=phase_commands_by_run_id,
        )
        sync_result = (
            sync_crm_public_web_batch_summary(
                self.store,
                batch_id,
                workspace_id=workspace_id,
            )
            if batch_id
            else {"batch": {}, "summary": {}}
        )
        if not batch_id or batch is None:
            command_status = str(refreshed_command.get("status") or "").strip()
            reason = (
                str(worker_summary.get("reason") or "").strip()
                or "crm_public_web_queue_batch_pending_owner"
            )
            failed_without_owner = reason == "crm_public_web_queue_batch_command_owner_disabled"
            return self._with_crm_public_web_contract(
                {
                    "status": (
                        "failed"
                        if failed_without_owner or command_status in {"failed", "failed_terminal", "cancelled"}
                        else "queued"
                    ),
                    "reason": reason,
                    "batch": {},
                    "runs": [],
                    "summary": {},
                    "worker_summary": worker_summary,
                    "workflow_command": self._kernel._workflow_command_observation(
                        refreshed_command,
                        migration_phase="W7_crm_public_web_queue_batch",
                    ),
                    "job": {},
                    "public_web_owner_sync": {
                        "status": "owned",
                        "owner": "crm_public_web_v1",
                        "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                        "operation": "start",
                    },
                },
                operation="start",
            )
        job_payload = dict(command_payload.get("job_payload") or {})
        result = {
            "status": str(command_payload.get("operation_planning_status") or "queued"),
            "batch": dict(sync_result.get("batch") or batch),
            "runs": refreshed_runs,
            "phase_commands_by_run_id": phase_commands_by_run_id,
            "summary": dict(sync_result.get("summary") or {}),
            "worker_summary": worker_summary,
            "workflow_command": self._kernel._workflow_command_observation(
                refreshed_command,
                migration_phase="W7_crm_public_web_queue_batch",
            ),
            "job": job_payload,
            "public_web_owner_sync": {
                "status": "owned",
                "owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                "operation": "start",
            },
        }
        return self._with_crm_public_web_contract(result, operation="start")

    def list_crm_record_public_web_searches(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="poll")
        if isinstance(workspace_result, dict):
            return self._with_crm_public_web_contract(workspace_result, operation="poll", read=True)
        workspace_id = workspace_result
        record_id_input = normalized.get("crm_record_ids") or normalized.get("record_ids") or normalized.get("record_id")
        record_ids = _coerce_public_web_record_ids(record_id_input)
        if not record_ids and not str(normalized.get("batch_id") or "").strip():
            return self._with_crm_public_web_contract(
                {
                    "status": "ok",
                    "batches": [],
                    "runs": [],
                    "reason": "crm_record_ids_required_for_crm_public_web_poll",
                },
                operation="poll",
                read=True,
            )
        if record_ids:
            record_ids_result = self._prepare_crm_public_web_record_ids(
                record_ids,
                require_non_empty=True,
                workspace_id=workspace_id,
            )
            if isinstance(record_ids_result, dict):
                return record_ids_result
            normalized["record_ids"] = record_ids_result
            normalized.pop("record_id", None)
        result = self._list_crm_public_web_searches_from_owner(normalized, workspace_id=workspace_id)
        return self._with_crm_public_web_contract(result, operation="poll", read=True)

    def _crm_public_web_runs_with_materialized_signal_metrics(
        self,
        runs: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> list[dict[str, Any]]:
        return [self._crm_public_web_run_with_materialized_signal_metrics(dict(run)) for run in list(runs or [])]

    def _crm_public_web_run_with_materialized_signal_metrics(
        self,
        run: dict[str, Any],
        *,
        signal_rows: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    ) -> dict[str, Any]:
        enriched = dict(run or {})
        summary = dict(enriched.get("summary") or {})
        phase_metrics = dict(summary.get("phase_metrics") or {})
        status = str(enriched.get("status") or summary.get("status") or "").strip()
        has_all_materialized_metrics = all(
            key in phase_metrics for key in _PUBLIC_WEB_MATERIALIZED_SIGNAL_METRIC_KEYS
        )
        terminal_run = status in PUBLIC_WEB_TERMINAL_STATUSES
        if signal_rows is None and has_all_materialized_metrics:
            return enriched

        rows: list[dict[str, Any]]
        if signal_rows is None:
            if terminal_run:
                run_id = str(enriched.get("run_id") or "").strip()
                rows = (
                    self.store.list_person_public_web_signals(
                        run_id=run_id,
                        record_id=str(enriched.get("crm_record_id") or enriched.get("record_id") or "").strip(),
                        limit=5000,
                    )
                    if run_id
                    else []
                )
            else:
                rows = []
        else:
            rows = [dict(row or {}) for row in list(signal_rows or []) if isinstance(row, dict)]

        materialized_counts = _public_web_materialized_signal_counts(rows)
        summary["phase_metrics"] = {**phase_metrics, **materialized_counts}
        enriched["summary"] = summary
        analysis_checkpoint = dict(enriched.get("analysis_checkpoint") or {})
        analysis_phase_metrics = dict(analysis_checkpoint.get("phase_metrics") or {})
        analysis_checkpoint["phase_metrics"] = {**analysis_phase_metrics, **materialized_counts}
        for key, value in materialized_counts.items():
            analysis_checkpoint.setdefault(key, value)
        enriched["analysis_checkpoint"] = analysis_checkpoint
        return enriched

    def cancel_crm_record_public_web_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        validation = self._validate_crm_public_web_action_payload(payload)
        if isinstance(validation, dict):
            return self._with_crm_public_web_contract(validation, operation="cancel")
        result = self._cancel_crm_public_web_search_from_owner(dict(payload or {}))
        return self._with_crm_public_web_contract(result, operation="cancel")

    def retry_crm_record_public_web_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        validation = self._validate_crm_public_web_action_payload(payload)
        if isinstance(validation, dict):
            return self._with_crm_public_web_contract(validation, operation="retry")
        result = self._retry_crm_public_web_search_from_owner(dict(payload or {}))
        return self._with_crm_public_web_contract(result, operation="retry")

    def get_crm_record_public_web_search_detail(self, crm_record_id: str) -> dict[str, Any]:
        record_result = self._prepare_single_crm_public_web_record_id(crm_record_id)
        if isinstance(record_result, dict):
            return record_result
        result = self._get_crm_record_public_web_search_detail_from_owner(record_result)
        return self._with_crm_public_web_contract(result, operation="detail", read=True)

    def list_crm_record_public_web_promotions(self, crm_record_id: str) -> dict[str, Any]:
        record_result = self._prepare_single_crm_public_web_record_id(crm_record_id)
        if isinstance(record_result, dict):
            return record_result
        result = self._list_crm_record_public_web_promotions_from_owner(record_result)
        return self._with_crm_public_web_contract(result, operation="promotion_list", read=True)

    def promote_crm_record_public_web_signal(self, crm_record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        record_result = self._prepare_single_crm_public_web_record_id(crm_record_id)
        if isinstance(record_result, dict):
            return record_result
        result = self._promote_crm_public_web_signal_from_owner(record_result, dict(payload or {}))
        return self._with_crm_public_web_contract(result, operation="promotion")

    def export_crm_record_public_web_archive(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="export")
        if isinstance(workspace_result, dict):
            return self._with_crm_public_web_contract(workspace_result, operation="export", read=True)
        workspace_id = workspace_result
        record_ids_result = self._prepare_crm_public_web_record_ids(
            normalized.get("crm_record_ids") or normalized.get("record_ids") or normalized.get("record_id"),
            require_non_empty=True,
            workspace_id=workspace_id,
        )
        if isinstance(record_ids_result, dict):
            return record_ids_result
        normalized["record_ids"] = record_ids_result
        normalized["crm_record_ids"] = record_ids_result
        normalized["workspace_id"] = workspace_id
        command = self._plan_crm_public_web_export_generate_command(normalized, workspace_id=workspace_id)
        if not command:
            result = {
                "status": "failed",
                "reason": "crm_public_web_export_command_enqueue_failed",
                "command_type": EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                "owner": EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
                "public_web_storage_owner": "crm_public_web_v1",
                "read_contract": {
                    "source": "workflow_commands",
                    "fallback_used": False,
                    "fail_closed": True,
                },
            }
        else:
            # C1.4 (substrate-unify): submit the export as a durable task and return
            # 202 (the worker CRM export drain builds the archive off the request
            # thread). _plan already committed durable events, so the recovery worker
            # is signaled internally. An idempotent hit on an already-succeeded
            # command replays its artifact handle.
            command_id = str(command.get("command_id") or "")
            domain_status = str(command.get("status") or "").strip()
            if domain_status == "succeeded":
                result = self._crm_public_web_export_task_status_envelope(command)
            else:
                result = async_task_accepted(
                    task_id=command_id,
                    task_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                    idempotency_key=str(command.get("idempotency_key") or ""),
                    domain_status=domain_status,
                )
        return self._with_crm_public_web_contract(result, operation="export", read=True)

    def _crm_public_web_export_artifact_headers(self, result: dict[str, Any]) -> dict[str, str]:
        """The X-Sourcing-* download headers for a CRM public-web export artifact —
        byte-for-byte the set the old synchronous /api/crm/records/public-web-export
        emitted (the Canonical-Public-Web-Owner is owner-identity, not a result field)."""
        return {
            "X-Sourcing-Export-Record-Count": str(int(result.get("record_count") or 0)),
            "X-Sourcing-Exported-Record-Count": str(int(result.get("exported_record_count") or 0)),
            "X-Sourcing-Exported-Signal-Count": str(int(result.get("exported_signal_count") or 0)),
            "X-Sourcing-No-Public-Web-Result-Count": str(int(result.get("no_public_web_result_count") or 0)),
            "X-Sourcing-No-Exportable-Signal-Count": str(int(result.get("no_exportable_signal_count") or 0)),
            "X-Sourcing-Non-Terminal-Run-Count": str(int(result.get("non_terminal_run_count") or 0)),
            "X-Sourcing-Canonical-Public-Web-Owner": "crm_records",
        }

    def _crm_public_web_export_task_status_envelope(self, command: dict[str, Any]) -> dict[str, Any]:
        """Project a durable CRM export command into the unified async-task poll body
        (used for an idempotent submit replay; the generic poll endpoint on the
        orchestrator builds the same shape command-type-aware)."""
        command_id = str(command.get("command_id") or "")
        domain_status = str(command.get("status") or "").strip()
        result = dict(command.get("result") or {})
        artifact = None
        if domain_status == "succeeded":
            artifact = async_task_artifact(
                handle=f"/api/exports/{command_id}/artifact",
                content_type=str(result.get("content_type") or "application/zip"),
                filename=str(result.get("filename") or "crm-public-web-export.zip"),
                byte_size=int(result.get("byte_size") or 0),
                headers=self._crm_public_web_export_artifact_headers(result),
            )
        error = None
        if domain_status in {"failed", "failed_terminal"}:
            error = {"reason": str(result.get("reason") or "crm_public_web_export_failed")}
        return async_task_status(
            task_id=command_id,
            task_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            domain_status=domain_status,
            error=error,
            artifact=artifact,
            idempotency_key=str(command.get("idempotency_key") or ""),
        )

    def _crm_public_web_export_workflow_run_id(
        self,
        *,
        workspace_id: str,
        crm_record_ids: list[str],
        export_mode: str,
        export_contract_version: str = CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
        export_input_watermark_hash: str = "",
    ) -> str:
        scope_hash = hashlib.sha1(
            json.dumps(
                {
                    "workspace_id": str(workspace_id or "default").strip() or "default",
                    "crm_record_ids": sorted({str(item or "").strip() for item in crm_record_ids if str(item or "").strip()}),
                    "export_mode": str(export_mode or "promoted_only").strip() or "promoted_only",
                    "export_contract_version": str(export_contract_version or "").strip(),
                    "export_input_watermark_hash": str(export_input_watermark_hash or "").strip(),
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"wf_crm_public_web_export_{scope_hash}"

    def _crm_public_web_export_operation_id(
        self,
        *,
        workspace_id: str,
        crm_record_ids: list[str],
        export_mode: str,
        export_contract_version: str = CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
        export_input_watermark_hash: str = "",
    ) -> str:
        scope_hash = hashlib.sha1(
            json.dumps(
                {
                    "workspace_id": str(workspace_id or "default").strip() or "default",
                    "crm_record_ids": sorted({str(item or "").strip() for item in crm_record_ids if str(item or "").strip()}),
                    "export_mode": str(export_mode or "promoted_only").strip() or "promoted_only",
                    "export_contract_version": str(export_contract_version or "").strip(),
                    "export_input_watermark_hash": str(export_input_watermark_hash or "").strip(),
                },
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"op_crm_public_web_export_{scope_hash}"

    def _normalize_crm_public_web_export_command_payload(
        self,
        payload: dict[str, Any],
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        normalized = dict(payload or {})
        crm_record_ids = _coerce_public_web_record_ids(
            normalized.get("crm_record_ids") or normalized.get("record_ids") or normalized.get("record_id")
        )
        export_mode = str(normalized.get("mode") or normalized.get("public_web_export_mode") or "promoted_only").strip()
        if export_mode not in {"promoted_only", "promoted_and_publishable"}:
            export_mode = "promoted_only"
        return {
            "workspace_id": str(workspace_id or normalized.get("workspace_id") or "default").strip() or "default",
            "crm_record_ids": list(dict.fromkeys(crm_record_ids)),
            "record_ids": list(dict.fromkeys(crm_record_ids)),
            "mode": export_mode,
            "public_web_export_mode": export_mode,
            "export_scope": "crm_public_web_records",
            "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
        }

    def _crm_public_web_export_record_input_snapshot(
        self,
        record: dict[str, Any],
        *,
        export_mode: str,
    ) -> dict[str, Any]:
        record_payload = dict(record or {})
        record_id = str(record_payload.get("id") or record_payload.get("crm_record_id") or "").strip()
        detail = self._get_crm_record_public_web_search_detail_from_owner(record_id)
        if detail.get("status") != "ok":
            detail = {
                "status": str(detail.get("status") or "not_found"),
                "record_id": record_id,
                "crm_record_id": record_id,
                "signals": [],
                "email_candidates": [],
                "profile_links": [],
                "evidence_links": [],
                "promotions": [],
                "latest_run": None,
                "phase_commands": {},
                "person_asset": None,
                "promotion_summary": {},
                "public_web_storage_owner": "crm_public_web_v1",
            }
        raw_promotions = [dict(item) for item in list(detail.get("promotions") or []) if isinstance(item, dict)]
        latest_run = dict(detail.get("latest_run") or {})
        latest_run_status = str(latest_run.get("status") or "").strip()
        include_publishable_unpromoted = export_mode == "promoted_and_publishable"
        exported_run_signals = (
            _public_web_exportable_signals(
                list(detail.get("signals") or []),
                include_publishable_unpromoted=include_publishable_unpromoted,
            )
            if not latest_run_status or _public_web_run_status_is_terminal(latest_run_status)
            else []
        )
        exported_promotion_signals = _public_web_exportable_promotion_signals(raw_promotions)
        exported_signals = _merge_public_web_export_signals([*exported_run_signals, *exported_promotion_signals])
        record_export_status = _public_web_export_record_status(
            detail=dict(detail),
            exported_signals=exported_signals,
            exported_promotion_signal_count=len(exported_promotion_signals),
        )
        record_status = str(record_export_status.get("status") or "")
        skip_reason = str(record_export_status.get("skip_reason") or "")
        exported_evidence_links = _public_web_evidence_links(exported_signals)
        record_promotions = _public_web_export_promotions_for_signals(
            raw_promotions,
            exported_signals,
        )
        phase_commands = dict(detail.get("phase_commands") or latest_run.get("phase_commands") or {})
        summary_payload = {
            "snapshot_contract": "crm_public_web_export_record_input_v1",
            "record_id": record_id,
            "crm_record_id": record_id,
            "target_candidate": _public_web_target_candidate_summary(record_payload),
            "crm_record": {
                key: record_payload.get(key)
                for key in (
                    "crm_record_id",
                    "person_identity_key",
                    "candidate_identity_key",
                    "stage",
                    "follow_up_status",
                    "source_projection_id",
                    "source_collection_id",
                )
                if record_payload.get(key) not in (None, "", [], {})
            },
            "latest_run": latest_run,
            "phase_commands": phase_commands,
            "person_asset": detail.get("person_asset"),
            "promotion_summary": detail.get("promotion_summary") or {},
            "export_record_status": record_status,
            "export_skip_reason": skip_reason,
            "exported_signals": exported_signals,
            "evidence_links": exported_evidence_links,
            "promotions": record_promotions,
            "public_web_storage_owner": "crm_public_web_v1",
        }
        snapshot_digest = hashlib.sha1(
            json.dumps(_storage_json_safe_payload(summary_payload), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return {
            "detail": detail,
            "latest_run": latest_run,
            "phase_commands": phase_commands,
            "person_asset": detail.get("person_asset"),
            "exported_signals": exported_signals,
            "exported_evidence_links": exported_evidence_links,
            "record_promotions": record_promotions,
            "record_export_status": record_export_status,
            "record_status": record_status,
            "skip_reason": skip_reason,
            "input_snapshot_digest": snapshot_digest,
            "input_snapshot_summary": {
                "snapshot_contract": "crm_public_web_export_record_input_v1",
                "digest": snapshot_digest,
                "detail_status": str(detail.get("status") or ""),
                "latest_run_id": str(latest_run.get("run_id") or ""),
                "latest_run_status": str(latest_run.get("status") or ""),
                "latest_run_updated_at": str(latest_run.get("updated_at") or ""),
                "phase_command_count": _coerce_int(dict(phase_commands or {}).get("command_count"), 0),
                "phase_commands_digest": hashlib.sha1(
                    json.dumps(_storage_json_safe_payload(phase_commands), ensure_ascii=False, sort_keys=True).encode(
                        "utf-8"
                    )
                ).hexdigest(),
                "person_asset_id": str(dict(detail.get("person_asset") or {}).get("asset_id") or ""),
                "person_asset_updated_at": str(dict(detail.get("person_asset") or {}).get("updated_at") or ""),
                "exported_signal_count": len(exported_signals),
                "exported_evidence_link_count": len(exported_evidence_links),
                "promotion_count": len(record_promotions),
                "export_record_status": record_status,
                "export_skip_reason": skip_reason,
            },
        }

    def _crm_public_web_export_input_watermark(
        self,
        *,
        workspace_id: str,
        crm_record_ids: list[str],
        export_mode: str,
        include_record_inputs: bool = False,
    ) -> dict[str, Any]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_record_ids = sorted(
            {str(record_id or "").strip() for record_id in list(crm_record_ids or []) if str(record_id or "").strip()}
        )
        records: list[dict[str, Any]] = []
        record_export_inputs_by_id: dict[str, dict[str, Any]] = {}
        for record_id in normalized_record_ids:
            crm_record = self.store.get_crm_record(record_id) or {}
            engagement_id = str(crm_record.get("current_engagement_id") or "").strip()
            engagement = self.store.get_crm_engagement(engagement_id) if engagement_id else None
            latest_runs = self.store.list_crm_public_web_runs(
                crm_record_id=record_id,
                workspace_id=normalized_workspace_id,
                limit=1,
            )
            latest_run = dict(latest_runs[0] if latest_runs else {})
            latest_run_id = str(latest_run.get("run_id") or "").strip()
            signals = self.store.list_person_public_web_signals(
                run_id=latest_run_id,
                record_id=record_id,
                limit=5000,
            ) if latest_run_id else []
            promotions = self.store.list_crm_public_web_promotions(
                crm_record_id=record_id,
                workspace_id=normalized_workspace_id,
                limit=5000,
            )
            person_identity_key = str(crm_record.get("person_identity_key") or "").strip()
            assertions = (
                self.store.list_person_assertions(person_identity_key=person_identity_key, limit=500)
                if person_identity_key
                else []
            )
            record_payload = self._public_crm_record_payload(crm_record) if crm_record else {"crm_record_id": record_id}
            export_input = self._crm_public_web_export_record_input_snapshot(record_payload, export_mode=export_mode)
            if include_record_inputs:
                record_export_inputs_by_id[record_id] = export_input
            records.append(
                {
                    "crm_record_id": record_id,
                    "record_updated_at": str(crm_record.get("updated_at") or ""),
                    "engagement_id": engagement_id,
                    "engagement_updated_at": str(dict(engagement or {}).get("updated_at") or ""),
                    "person_identity_key": person_identity_key,
                    "latest_run": {
                        "run_id": latest_run_id,
                        "status": str(latest_run.get("status") or ""),
                        "phase": str(latest_run.get("phase") or ""),
                        "updated_at": str(latest_run.get("updated_at") or ""),
                    },
                    "signals": {
                        "count": len(signals),
                        "max_updated_at": max((str(item.get("updated_at") or "") for item in signals), default=""),
                        "digest": hashlib.sha1(
                            json.dumps(
                                [
                                    {
                                        "signal_id": str(item.get("signal_id") or ""),
                                        "run_id": str(item.get("run_id") or ""),
                                        "signal_kind": str(item.get("signal_kind") or ""),
                                        "signal_type": str(item.get("signal_type") or ""),
                                        "normalized_value": str(item.get("normalized_value") or ""),
                                        "value": str(item.get("value") or ""),
                                        "url": str(item.get("url") or ""),
                                        "source_url": str(item.get("source_url") or ""),
                                        "promotion_status": str(item.get("promotion_status") or ""),
                                        "publishable": bool(item.get("publishable")),
                                        "updated_at": str(item.get("updated_at") or ""),
                                    }
                                    for item in sorted(signals, key=lambda row: str(row.get("signal_id") or ""))
                                ],
                                ensure_ascii=False,
                                sort_keys=True,
                            ).encode("utf-8")
                        ).hexdigest(),
                    },
                    "promotions": {
                        "count": len(promotions),
                        "max_updated_at": max((str(item.get("updated_at") or "") for item in promotions), default=""),
                        "digest": hashlib.sha1(
                            json.dumps(
                                [
                                    {
                                        "promotion_id": str(item.get("promotion_id") or ""),
                                        "signal_id": str(item.get("signal_id") or ""),
                                        "run_id": str(item.get("run_id") or ""),
                                        "action": str(item.get("action") or ""),
                                        "promotion_status": str(item.get("promotion_status") or ""),
                                        "new_value": str(item.get("new_value") or ""),
                                        "updated_at": str(item.get("updated_at") or ""),
                                    }
                                    for item in sorted(promotions, key=lambda row: str(row.get("promotion_id") or ""))
                                ],
                                ensure_ascii=False,
                                sort_keys=True,
                            ).encode("utf-8")
                        ).hexdigest(),
                    },
                    "assertions": {
                        "count": len(assertions),
                        "max_updated_at": max((str(item.get("updated_at") or "") for item in assertions), default=""),
                        "digest": hashlib.sha1(
                            json.dumps(
                                [
                                    {
                                        "assertion_id": str(item.get("assertion_id") or ""),
                                        "assertion_type": str(item.get("assertion_type") or ""),
                                        "normalized_value": str(item.get("normalized_value") or ""),
                                        "verification_status": str(item.get("verification_status") or ""),
                                        "source_run_id": str(item.get("source_run_id") or ""),
                                        "updated_at": str(item.get("updated_at") or ""),
                                    }
                                    for item in sorted(assertions, key=lambda row: str(row.get("assertion_id") or ""))
                                ],
                                ensure_ascii=False,
                                sort_keys=True,
                            ).encode("utf-8")
                        ).hexdigest(),
                    },
                    "export_input_snapshot": dict(export_input.get("input_snapshot_summary") or {}),
                }
            )
        watermark: dict[str, Any] = {
            "watermark_version": "crm_public_web_export_input_watermark_v1",
            "workspace_id": normalized_workspace_id,
            "crm_record_ids": normalized_record_ids,
            "export_mode": str(export_mode or "promoted_only").strip() or "promoted_only",
            "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            "records": records,
        }
        watermark["watermark_hash"] = hashlib.sha1(
            json.dumps(_storage_json_safe_payload(watermark), ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        if include_record_inputs:
            watermark["_record_export_inputs"] = record_export_inputs_by_id
        return watermark

    def _plan_crm_public_web_export_generate_command(
        self,
        payload: dict[str, Any],
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        command_payload = self._normalize_crm_public_web_export_command_payload(payload, workspace_id=workspace_id)
        crm_record_ids = [str(item or "").strip() for item in list(command_payload.get("crm_record_ids") or []) if str(item or "").strip()]
        normalized_workspace_id = str(command_payload.get("workspace_id") or "default").strip() or "default"
        export_mode = str(command_payload.get("mode") or "promoted_only").strip() or "promoted_only"
        if not crm_record_ids:
            return {}
        export_input_watermark = self._crm_public_web_export_input_watermark(
            workspace_id=normalized_workspace_id,
            crm_record_ids=crm_record_ids,
            export_mode=export_mode,
        )
        export_input_watermark_hash = str(export_input_watermark.get("watermark_hash") or "").strip()
        workflow_run_id = self._crm_public_web_export_workflow_run_id(
            workspace_id=normalized_workspace_id,
            crm_record_ids=crm_record_ids,
            export_mode=export_mode,
            export_contract_version=CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            export_input_watermark_hash=export_input_watermark_hash,
        )
        operation_id = self._crm_public_web_export_operation_id(
            workspace_id=normalized_workspace_id,
            crm_record_ids=crm_record_ids,
            export_mode=export_mode,
            export_contract_version=CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            export_input_watermark_hash=export_input_watermark_hash,
        )
        idempotency_key = export_crm_public_web_generate_idempotency_key(
            workspace_id=normalized_workspace_id,
            crm_record_ids=crm_record_ids,
            export_mode=export_mode,
            export_contract_version=CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            export_input_watermark_hash=export_input_watermark_hash,
        )
        if not workflow_run_id or not operation_id or not idempotency_key:
            return {}
        command_payload["export_input_watermark"] = export_input_watermark
        command_payload["export_input_watermark_hash"] = export_input_watermark_hash
        command_payload["crm_record_count"] = len(crm_record_ids)
        command_payload["materialization_metadata"] = {
            "command_payload_storage": "workflow_commands",
            "write_owner": EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
            "migration_phase": "W7_crm_public_web_export_command_owner",
            "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            "export_input_watermark_hash": export_input_watermark_hash,
        }
        try:
            self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="WorkflowStarted",
                idempotency_key=f"{workflow_run_id}:crm_public_web_export_started",
                actor="crm_public_web_export_planner",
                source="crm_public_web_export",
                payload={
                    "workflow_type": "crm_public_web_export",
                    "stage_key": "crm_public_web_export",
                    "workspace_id": normalized_workspace_id,
                    "record_count": len(crm_record_ids),
                    "export_input_watermark_hash": export_input_watermark_hash,
                    "migration_phase": "W7_crm_public_web_export_command_owner",
                },
            )
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor="crm_public_web_export_planner",
                source="crm_public_web_export",
                payload={
                    "workflow_type": "crm_public_web_export",
                    "stage_key": "crm_public_web_export",
                    "command_type": EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "crm_public_web_export_generate",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _crm_public_web_export_artifact_path(self, command_id: str, filename: str) -> Path:
        safe_command_id = _target_candidate_archive_name_component(str(command_id or ""), fallback="command")
        safe_filename = _target_candidate_archive_name_component(str(filename or ""), fallback="crm-public-web-export.zip")
        if not safe_filename.endswith(".zip"):
            safe_filename = f"{safe_filename}.zip"
        return Path(self.runtime_dir) / "exports" / "crm_public_web" / safe_command_id / safe_filename

    def _crm_public_web_export_payload_from_artifact(
        self,
        *,
        command: dict[str, Any],
        artifact_path: str,
        filename: str,
    ) -> dict[str, Any]:
        path = Path(str(artifact_path or "")).expanduser()
        if not path.exists() or not path.is_file():
            return {
                "status": "failed",
                "reason": "crm_public_web_export_artifact_missing",
                "artifact_path": str(path),
                "public_web_storage_owner": "crm_public_web_v1",
                "workflow_command": self._kernel._workflow_command_observation(
                    command,
                    migration_phase="W7_crm_public_web_export_command_owner",
                ),
            }
        result = dict(command.get("result") or {})
        return {
            "status": "ok",
            "filename": str(filename or result.get("filename") or path.name),
            "content_type": "application/zip",
            "body": path.read_bytes(),
            "record_count": int(result.get("record_count") or 0),
            "export_mode": str(result.get("export_mode") or dict(command.get("payload") or {}).get("mode") or "promoted_only"),
            "export_contract_version": str(
                result.get("export_contract_version")
                or dict(command.get("payload") or {}).get("export_contract_version")
                or ""
            ),
            "export_input_watermark_hash": str(
                result.get("export_input_watermark_hash")
                or dict(command.get("payload") or {}).get("export_input_watermark_hash")
                or ""
            ),
            "export_input_watermark": dict(
                result.get("export_input_watermark")
                or dict(command.get("payload") or {}).get("export_input_watermark")
                or {}
            ),
            "exported_signal_count": int(result.get("exported_signal_count") or 0),
            "exported_record_count": int(result.get("exported_record_count") or 0),
            "no_public_web_result_count": int(result.get("no_public_web_result_count") or 0),
            "no_exportable_signal_count": int(result.get("no_exportable_signal_count") or 0),
            "non_terminal_run_count": int(result.get("non_terminal_run_count") or 0),
            "artifact_path": str(path),
            "public_web_storage_owner": "crm_public_web_v1",
            "workflow_command": self._kernel._workflow_command_observation(
                command,
                migration_phase="W7_crm_public_web_export_command_owner",
            ),
            "read_contract": {
                "source": "workflow_commands+crm_public_web_assets+person_assertions",
                "fallback_used": False,
                "fail_closed": True,
                "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                "export_input_watermark_hash": str(
                    result.get("export_input_watermark_hash")
                    or dict(command.get("payload") or {}).get("export_input_watermark_hash")
                    or ""
                ),
            },
        }

    def _crm_public_web_export_command_has_current_contract(self, command: dict[str, Any]) -> bool:
        return not self._crm_public_web_export_command_contract_failure(command)

    def _crm_public_web_export_command_contract_failure(self, command: dict[str, Any]) -> dict[str, Any]:
        payload = dict(command.get("payload") or {})
        result = dict(command.get("result") or {})
        version = str(result.get("export_contract_version") or payload.get("export_contract_version") or "").strip()
        if version != CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION:
            return {
                "reason": "crm_public_web_export_contract_version_stale",
                "expected_export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                "actual_export_contract_version": version,
            }
        payload_watermark_hash = str(payload.get("export_input_watermark_hash") or "").strip()
        result_watermark_hash = str(result.get("export_input_watermark_hash") or "").strip()
        command_watermark_hash = result_watermark_hash or payload_watermark_hash
        if not command_watermark_hash:
            return {
                "reason": "crm_public_web_export_input_watermark_missing",
                "expected_export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            }
        crm_record_ids = [
            str(record_id or "").strip()
            for record_id in list(payload.get("crm_record_ids") or payload.get("record_ids") or [])
            if str(record_id or "").strip()
        ]
        if not crm_record_ids:
            return {
                "reason": "crm_public_web_export_input_watermark_scope_missing",
                "export_input_watermark_hash": command_watermark_hash,
            }
        current_watermark = self._crm_public_web_export_input_watermark(
            workspace_id=str(payload.get("workspace_id") or "default").strip() or "default",
            crm_record_ids=crm_record_ids,
            export_mode=str(payload.get("mode") or payload.get("public_web_export_mode") or "promoted_only").strip()
            or "promoted_only",
        )
        current_watermark_hash = str(current_watermark.get("watermark_hash") or "").strip()
        if command_watermark_hash != current_watermark_hash:
            return {
                "reason": "crm_public_web_export_input_watermark_stale",
                "export_input_watermark_hash": command_watermark_hash,
                "current_export_input_watermark_hash": current_watermark_hash,
                "current_export_input_watermark": current_watermark,
            }
        return {}

    def _run_crm_public_web_export_generate_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "crm_public_web_export_command_id_missing"}
        if str(command_payload.get("status") or "").strip() == "succeeded":
            contract_failure = self._crm_public_web_export_command_contract_failure(command_payload)
            if contract_failure:
                return {
                    "status": "failed",
                    **contract_failure,
                    "public_web_storage_owner": "crm_public_web_v1",
                    "workflow_command": self._kernel._workflow_command_observation(
                        command_payload,
                        migration_phase="W7_crm_public_web_export_command_owner",
                    ),
                    "read_contract": {
                        "source": "workflow_commands",
                        "fallback_used": False,
                        "fail_closed": True,
                        "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                    },
                }
            result_payload = dict(command_payload.get("result") or {})
            return self._crm_public_web_export_payload_from_artifact(
                command=command_payload,
                artifact_path=str(result_payload.get("artifact_path") or "").strip(),
                filename=str(result_payload.get("filename") or ""),
            )
        lease_owner = f"{EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER}-{uuid.uuid4().hex[:8]}"
        # Idempotent CRM export build -> safe to reclaim an expired-lease 'claimed' row
        # (mirrors the projection export _run + the CRM drain's reclaim_claimed=True).
        claimed = self.store.claim_workflow_command(
            command_id, lease_owner=lease_owner, lease_seconds=600, reclaim_claimed=True
        )
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            if str(latest.get("status") or "").strip() == "succeeded":
                contract_failure = self._crm_public_web_export_command_contract_failure(latest)
                if contract_failure:
                    return {
                        "status": "failed",
                        **contract_failure,
                        "public_web_storage_owner": "crm_public_web_v1",
                        "workflow_command": self._kernel._workflow_command_observation(
                            latest,
                            migration_phase="W7_crm_public_web_export_command_owner",
                        ),
                        "read_contract": {
                            "source": "workflow_commands",
                            "fallback_used": False,
                            "fail_closed": True,
                            "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                        },
                    }
                result_payload = dict(latest.get("result") or {})
                return self._crm_public_web_export_payload_from_artifact(
                    command=latest,
                    artifact_path=str(result_payload.get("artifact_path") or "").strip(),
                    filename=str(result_payload.get("filename") or ""),
                )
            return {
                "status": "failed",
                "reason": "crm_public_web_export_command_not_claimed",
                "public_web_storage_owner": "crm_public_web_v1",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W7_crm_public_web_export_command_owner",
                ),
            }
        running_command = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner)
        latest_command = running_command or self.store.get_workflow_command(command_id) or claimed
        contract_failure = self._crm_public_web_export_command_contract_failure(latest_command)
        if contract_failure:
            reason = str(contract_failure.get("reason") or "crm_public_web_export_contract_failure")
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=reason,
                retryable=False,
            )
            return {
                "status": "failed",
                **contract_failure,
                "public_web_storage_owner": "crm_public_web_v1",
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_crm_public_web_export_command_owner",
                ),
                "read_contract": {
                    "source": "workflow_commands",
                    "fallback_used": False,
                    "fail_closed": True,
                    "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                },
            }
        latest_payload = dict(latest_command.get("payload") or {})
        crm_record_ids = [
            str(record_id or "").strip()
            for record_id in list(latest_payload.get("crm_record_ids") or latest_payload.get("record_ids") or [])
            if str(record_id or "").strip()
        ]
        workspace_id = str(latest_payload.get("workspace_id") or "default").strip() or "default"
        export_input_watermark = dict(latest_payload.get("export_input_watermark") or {})
        export_input_watermark_hash = str(
            latest_payload.get("export_input_watermark_hash")
            or export_input_watermark.get("watermark_hash")
            or ""
        ).strip()
        activity, attempt = self._kernel._start_workflow_command_activity_attempt(
            latest_command,
            activity_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            owner=EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
            phase="crm_public_web_export_generate",
            lease_owner=lease_owner,
            provider="crm_public_web_exporter",
            provider_request_ref=",".join(crm_record_ids) or command_id,
            input_payload=latest_payload,
            entity_counts={
                "crm_record_count": len(crm_record_ids),
            },
            metadata={
                "export_mode": str(latest_payload.get("mode") or "promoted_only").strip() or "promoted_only",
                "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                "export_input_watermark_hash": export_input_watermark_hash,
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
            attempt_suffix="crm_public_web_export_generate",
        )
        try:
            result = self._export_crm_public_web_archive_from_owner(
                latest_payload,
                workspace_id=workspace_id,
            )
        except Exception as exc:
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="failed",
                phase="failed",
                output={"workspace_id": workspace_id, "reason": "crm_public_web_export_generate_failed"},
                entity_counts={"crm_record_count": len(crm_record_ids)},
                error={"message": str(exc), "reason": "crm_public_web_export_generate_failed"},
                attempt_status="failed",
            )
            self._kernel._record_command_activity_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                entity_type="crm_public_web_export",
                entity_key=hashlib.sha1(",".join(crm_record_ids).encode("utf-8")).hexdigest()[:24] or command_id,
                delta_kind=default_readiness_effect_for_command_type(EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE),
                status="failed",
                reason="crm_public_web_export_generate_failed",
                source_ref={
                    "workspace_id": workspace_id,
                    "crm_record_ids": crm_record_ids,
                    "command_type": EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                },
                entity_payload={"error": str(exc)},
                metadata={"owner": EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER},
                idempotency_scope="crm_public_web_export_generate",
            )
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=f"crm_public_web_export_generate_failed:{exc}",
                retryable=True,
                retry_delay_seconds=10,
            )
            return {
                "status": "failed",
                "reason": "crm_public_web_export_generate_failed",
                "error": str(exc),
                "public_web_storage_owner": "crm_public_web_v1",
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_crm_public_web_export_command_owner",
                ),
            }
        if str(result.get("status") or "") != "ok":
            reason = str(result.get("reason") or result.get("status") or "crm_public_web_export_not_ok")
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="failed",
                phase="failed",
                output={**dict(result or {}), "workspace_id": workspace_id},
                entity_counts={"crm_record_count": len(crm_record_ids)},
                error={"reason": reason},
                attempt_status="failed",
            )
            self._kernel._record_command_activity_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                entity_type="crm_public_web_export",
                entity_key=hashlib.sha1(",".join(crm_record_ids).encode("utf-8")).hexdigest()[:24] or command_id,
                delta_kind=default_readiness_effect_for_command_type(EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE),
                status="failed",
                reason=reason,
                source_ref={
                    "workspace_id": workspace_id,
                    "crm_record_ids": crm_record_ids,
                    "command_type": EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                },
                entity_payload=dict(result or {}),
                metadata={"owner": EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER},
                idempotency_scope="crm_public_web_export_generate",
            )
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=reason,
                retryable=False,
            )
            result["workflow_command"] = self._kernel._workflow_command_observation(
                failed or latest_command,
                migration_phase="W7_crm_public_web_export_command_owner",
            )
            result["public_web_storage_owner"] = "crm_public_web_v1"
            return result
        artifact_path = self._crm_public_web_export_artifact_path(command_id, str(result.get("filename") or "crm-public-web.zip"))
        publish_result = self._publish_export_artifact_if_command_active(
            command_id=command_id,
            artifact_path=artifact_path,
            body=bytes(result.get("body") or b""),
        )
        if str(publish_result.get("status") or "") == "cancelled":
            return self._export_command_cancelled_owner_response(
                latest_command,
                reason=str(publish_result.get("reason") or "crm_public_web_export_cancelled_before_artifact_publish"),
            )
        final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
            activity=activity,
            attempt=attempt,
            status="succeeded",
            phase="crm_public_web_export_generated",
            output={
                "workspace_id": workspace_id,
                "filename": str(result.get("filename") or artifact_path.name),
                "artifact_path": str(artifact_path),
                "record_count": int(result.get("record_count") or 0),
                "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                "export_input_watermark_hash": export_input_watermark_hash,
                "exported_signal_count": int(result.get("exported_signal_count") or 0),
                "exported_record_count": int(result.get("exported_record_count") or 0),
                "no_public_web_result_count": int(result.get("no_public_web_result_count") or 0),
                "no_exportable_signal_count": int(result.get("no_exportable_signal_count") or 0),
                "non_terminal_run_count": int(result.get("non_terminal_run_count") or 0),
            },
            entity_counts={
                "crm_record_count": len(crm_record_ids),
                "record_count": int(result.get("record_count") or 0),
                "exported_signal_count": int(result.get("exported_signal_count") or 0),
                "exported_record_count": int(result.get("exported_record_count") or 0),
                "no_public_web_result_count": int(result.get("no_public_web_result_count") or 0),
                "no_exportable_signal_count": int(result.get("no_exportable_signal_count") or 0),
                "non_terminal_run_count": int(result.get("non_terminal_run_count") or 0),
            },
            artifact_refs=[str(artifact_path)],
            metadata={
                "content_type": str(result.get("content_type") or "application/zip"),
                "public_web_storage_owner": "crm_public_web_v1",
                "export_input_watermark_hash": export_input_watermark_hash,
            },
            attempt_status="succeeded",
        )
        export_delta = self._kernel._record_command_activity_entity_delta(
            command=latest_command,
            activity=final_activity or activity,
            attempt=final_attempt or attempt,
            entity_type="crm_public_web_export",
            entity_key=hashlib.sha1(",".join(crm_record_ids).encode("utf-8")).hexdigest()[:24] or command_id,
            delta_kind=default_readiness_effect_for_command_type(EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE),
            status="recorded",
            reason="crm_public_web_export_generated",
            source_ref={
                "workspace_id": workspace_id,
                "crm_record_ids": crm_record_ids,
                "command_type": EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            },
            entity_payload={
                "filename": str(result.get("filename") or artifact_path.name),
                "artifact_path": str(artifact_path),
                "record_count": int(result.get("record_count") or 0),
                "exported_signal_count": int(result.get("exported_signal_count") or 0),
                "exported_record_count": int(result.get("exported_record_count") or 0),
                "export_input_watermark_hash": export_input_watermark_hash,
            },
            artifact_refs=[str(artifact_path)],
            metadata={"owner": EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER, "public_web_storage_owner": "crm_public_web_v1"},
            idempotency_scope="crm_public_web_export_generate",
        )
        succeeded = self.store.mark_workflow_command_succeeded(
            command_id,
            result={
                "filename": str(result.get("filename") or artifact_path.name),
                "artifact_path": str(artifact_path),
                "activity_run_id": str((final_activity or activity).get("activity_run_id") or "").strip(),
                "activity_attempt_id": str((final_attempt or attempt).get("attempt_id") or "").strip(),
                "entity_delta_id": str(export_delta.get("delta_id") or "").strip(),
                "record_count": int(result.get("record_count") or 0),
                "export_mode": str(result.get("export_mode") or ""),
                "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                "export_input_watermark_hash": export_input_watermark_hash,
                "export_input_watermark": export_input_watermark,
                "exported_signal_count": int(result.get("exported_signal_count") or 0),
                "exported_record_count": int(result.get("exported_record_count") or 0),
                "no_public_web_result_count": int(result.get("no_public_web_result_count") or 0),
                "no_exportable_signal_count": int(result.get("no_exportable_signal_count") or 0),
                "non_terminal_run_count": int(result.get("non_terminal_run_count") or 0),
                "content_type": str(result.get("content_type") or "application/zip"),
                "migration_phase": "W7_crm_public_web_export_command_owner",
            },
        )
        if not succeeded and self._kernel._workflow_command_is_cancelled(command_id):
            try:
                artifact_path.unlink(missing_ok=True)
            except OSError:
                pass
            return self._export_command_cancelled_owner_response(
                latest_command,
                reason="crm_public_web_export_cancelled_before_command_success",
            )
        result["artifact_path"] = str(artifact_path)
        result["public_web_storage_owner"] = "crm_public_web_v1"
        result["workflow_command"] = self._kernel._workflow_command_observation(
            succeeded or latest_command,
            migration_phase="W7_crm_public_web_export_command_owner",
        )
        result["read_contract"] = {
            "source": "workflow_commands+crm_public_web_assets+person_assertions",
            "fallback_used": False,
            "fail_closed": True,
            "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            "export_input_watermark_hash": export_input_watermark_hash,
        }
        return result

    def start_target_candidate_public_web_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _legacy_target_public_web_orchestrator_disabled_result(operation="start")

    def cancel_target_candidate_public_web_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _legacy_target_public_web_orchestrator_disabled_result(operation="cancel")

    def retry_target_candidate_public_web_search(self, payload: dict[str, Any]) -> dict[str, Any]:
        return _legacy_target_public_web_orchestrator_disabled_result(operation="retry")

    def list_target_candidate_public_web_searches(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        return _legacy_target_public_web_orchestrator_disabled_result(operation="poll")

    def _cancel_crm_public_web_search_from_owner(self, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_crm_public_web_action_run_ids(
            payload,
            action_name="cancel",
            allow_terminal=False,
        )
        if isinstance(resolved, dict):
            return resolved
        run_ids, reason = resolved
        workspace_id = str(payload.get("workspace_id") or "").strip()
        operator = str(payload.get("operator") or payload.get("requested_by") or "operator").strip() or "operator"
        cancelled_runs: list[dict[str, Any]] = []
        skipped_runs: list[dict[str, Any]] = []
        interrupted_workers: list[dict[str, Any]] = []
        for run_id in run_ids:
            result = cancel_crm_public_web_run(
                store=self.store,
                run_id=run_id,
                reason=reason,
                operator=operator,
            )
            if result.get("status") == "cancelled":
                run = dict(result.get("run") or {})
                cancelled_runs.append(run)
                interrupted_workers.extend(
                    self._interrupt_crm_public_web_workers_for_run(
                        run,
                        reason=reason,
                        operator=operator,
                    )
                )
            elif result.get("status") == "skipped":
                skipped_runs.append(dict(result.get("run") or {}))
            else:
                return result
        batch_ids = sorted(
            {
                str(run.get("batch_id") or "").strip()
                for run in [*cancelled_runs, *skipped_runs]
                if str(run.get("batch_id") or "").strip()
            }
        )
        batches = [
            dict(
                (
                    sync_crm_public_web_batch_summary(
                        self.store,
                        batch_id,
                        workspace_id=workspace_id,
                    ).get("batch")
                    or {}
                )
            )
            for batch_id in batch_ids
        ]
        refreshed_runs = [
            self.store.get_crm_public_web_run(run_id=str(run.get("run_id") or "")) or run
            for run in [*cancelled_runs, *skipped_runs]
            if str(run.get("run_id") or "")
        ]
        return {
            "status": "cancelled" if cancelled_runs else "skipped",
            "runs": refreshed_runs,
            "batches": batches,
            "worker_summary": {
                "interrupted_worker_count": len(interrupted_workers),
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in interrupted_workers],
            },
            "reason": reason,
        }

    def _retry_crm_public_web_search_from_owner(self, payload: dict[str, Any]) -> dict[str, Any]:
        resolved = self._resolve_crm_public_web_action_run_ids(
            payload,
            action_name="retry",
            allow_terminal=True,
        )
        if isinstance(resolved, dict):
            return resolved
        run_ids, reason = resolved
        source_runs: list[dict[str, Any]] = []
        record_ids: list[str] = []
        skipped_runs: list[dict[str, Any]] = []
        for run_id in run_ids:
            run = self.store.get_crm_public_web_run(run_id=run_id)
            if run is None:
                return {"status": "not_found", "reason": "public_web_run_not_found", "run_id": run_id}
            run_status = str(run.get("status") or "").strip().lower()
            if run_status not in PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES:
                skipped_runs.append(run)
                continue
            record_id = str(run.get("crm_record_id") or run.get("record_id") or "").strip()
            if not record_id:
                skipped_runs.append(run)
                continue
            source_runs.append(run)
            record_ids.append(record_id)
        if not record_ids:
            return {
                "status": "invalid",
                "reason": "no retryable terminal Public Web runs matched",
                "skipped_runs": skipped_runs,
            }
        explicit_nonce = str(payload.get("refresh_nonce") or payload.get("nonce") or "").strip()
        requested_by = str(payload.get("operator") or payload.get("requested_by") or "operator").strip() or "operator"
        workspace_id = str(payload.get("workspace_id") or "").strip()
        source_run_ids = [str(run.get("run_id") or "").strip() for run in source_runs if str(run.get("run_id") or "").strip()]
        nonce = explicit_nonce or _crm_public_web_retry_nonce(
            source_run_ids=source_run_ids,
            reason=reason,
            workspace_id=workspace_id,
            requested_by=requested_by,
        )
        retry_payload = {
            "crm_record_ids": record_ids,
            "record_ids": record_ids,
            "workspace_id": workspace_id,
            "options": dict(source_runs[0].get("options") or {}),
            "force_refresh": True,
            "refresh_nonce": nonce,
            "requested_by": requested_by,
            "metadata": {
                "action": "retry",
                "reason": reason,
                "source_run_ids": source_run_ids,
                "retry_idempotency_key": nonce,
                "explicit_retry_nonce": bool(explicit_nonce),
            },
        }
        result = self.start_crm_record_public_web_search(retry_payload)
        result["status"] = "retried" if result.get("status") in {"queued", "joined"} else result.get("status")
        result["source_runs"] = source_runs
        result["skipped_runs"] = skipped_runs
        result["reason"] = reason
        return result

    def _prepare_single_crm_public_web_record_id(self, crm_record_id: str) -> str | dict[str, Any]:
        normalized_record_id = str(crm_record_id or "").strip()
        if not normalized_record_id:
            return {"status": "invalid", "reason": "crm_record_id is required"}
        if not self.store.get_crm_record(normalized_record_id):
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "missing_record_ids": [normalized_record_id],
            }
        return normalized_record_id

    def _prepare_crm_public_web_record_ids(
        self,
        value: Any,
        *,
        require_non_empty: bool,
        workspace_id: str = "default",
    ) -> list[str] | dict[str, Any]:
        record_ids = _coerce_public_web_record_ids(value)
        if require_non_empty and not record_ids:
            return {"status": "invalid", "reason": "crm_record_ids are required"}
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        missing_record_ids: list[str] = []
        prepared: list[str] = []
        for record_id in record_ids:
            crm_record = self.store.get_crm_record(record_id)
            if not crm_record or str(crm_record.get("workspace_id") or "default").strip() != normalized_workspace_id:
                missing_record_ids.append(record_id)
                continue
            prepared.append(record_id)
        if missing_record_ids:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "missing_record_ids": missing_record_ids,
            }
        return prepared

    def _list_crm_public_web_searches_from_owner(
        self,
        payload: dict[str, Any],
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        query = dict(payload or {})
        normalized_workspace_id = str(workspace_id or query.get("workspace_id") or "default").strip() or "default"
        batch_id = str(query.get("batch_id") or "").strip()
        record_id = str(query.get("record_id") or "").strip()
        record_ids = _coerce_public_web_record_ids(query.get("record_ids") or query.get("crm_record_ids") or query.get("record_id"))
        status = str(query.get("status") or "").strip()
        limit = min(max(_coerce_int(query.get("limit"), 100), 1), 1000)
        batches = []
        if batch_id:
            batch = self.store.get_crm_public_web_batch(batch_id=batch_id)
            if batch is not None:
                batch_workspace_id = str(batch.get("workspace_id") or "default").strip() or "default"
                if batch_workspace_id != normalized_workspace_id:
                    return {
                        "status": "invalid",
                        "reason": "public_web_batch_workspace_mismatch",
                        "workspace_id": normalized_workspace_id,
                        "batch_id": batch_id,
                        "batch_workspace_id": batch_workspace_id,
                        "batches": [],
                        "runs": [],
                    }
            batches = [batch] if batch is not None else []
        elif not record_id and not record_ids:
            batches = self.store.list_crm_public_web_batches(
                status=status,
                workspace_id=normalized_workspace_id,
                limit=limit,
            )
        if record_ids and not batch_id:
            runs = self.store.list_latest_crm_public_web_runs_by_record_ids(
                record_ids,
                workspace_id=normalized_workspace_id,
                status=status,
                limit=limit,
            )
        else:
            runs = self.store.list_crm_public_web_runs(
                batch_id=batch_id,
                crm_record_id=record_id,
                workspace_id=normalized_workspace_id,
                status=status,
                limit=limit,
            )
        phase_commands_by_run_id: dict[str, dict[str, Any]] = {}
        if _coerce_bool(query.get("include_phase_commands"), True):
            phase_commands_by_run_id = self._crm_public_web_phase_command_summaries_for_runs(
                [dict(run) for run in runs],
                workspace_id=normalized_workspace_id,
            )
            runs = [
                {
                    **dict(run),
                    "phase_commands": phase_commands_by_run_id.get(str(dict(run).get("run_id") or "").strip(), {}),
                }
                for run in runs
            ]
        runs = self._crm_public_web_runs_with_materialized_signal_metrics(runs)
        runs = self._crm_public_web_run_api_records(
            [dict(run) for run in runs],
            phase_commands_by_run_id=phase_commands_by_run_id,
        )
        return {
            "status": "ok",
            "batches": batches,
            "runs": runs,
            "phase_commands_by_run_id": phase_commands_by_run_id,
            "public_web_storage_owner": "crm_public_web_v1",
        }

    def _get_crm_record_public_web_search_detail_from_owner(self, crm_record_id: str) -> dict[str, Any]:
        normalized_record_id = str(crm_record_id or "").strip()
        if not normalized_record_id:
            return {"status": "invalid", "reason": "crm_record_id is required"}
        crm_record = self.store.get_crm_record(normalized_record_id)
        if not crm_record:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "record_id": normalized_record_id,
                "crm_record_id": normalized_record_id,
            }
        record = self._public_crm_record_payload(crm_record)
        runs = self.store.list_crm_public_web_runs(
            crm_record_id=normalized_record_id,
            workspace_id=str(crm_record.get("workspace_id") or "default"),
            limit=10,
        )
        latest_run = dict(runs[0]) if runs else {}
        linkedin_url_key = str(latest_run.get("linkedin_url_key") or "").strip() or normalize_linkedin_profile_url_key(
            str(record.get("linkedin_url") or "")
        )
        person_identity_key = str(latest_run.get("person_identity_key") or record.get("person_identity_key") or "").strip()
        if not person_identity_key and linkedin_url_key:
            person_identity_key = f"linkedin:{linkedin_url_key}"
        asset = None
        if person_identity_key:
            asset = self.store.get_person_public_web_asset(person_identity_key=person_identity_key)
        if asset is None and linkedin_url_key:
            asset = self.store.get_person_public_web_asset(linkedin_url_key=linkedin_url_key)
        promotions = self.store.list_crm_public_web_promotions(
            crm_record_id=normalized_record_id,
            workspace_id=str(crm_record.get("workspace_id") or "default"),
            limit=1000,
        )
        if not latest_run:
            if promotions:
                return {
                    "status": "ok",
                    "record_id": normalized_record_id,
                    "crm_record_id": normalized_record_id,
                    "target_candidate": _public_web_target_candidate_summary(record),
                    "crm_record": record,
                    "latest_run": None,
                    "phase_commands": {},
                    "person_asset": _public_web_asset_detail(dict(asset or {})) if asset is not None else None,
                    "signals": [],
                    "email_candidates": [],
                    "profile_links": [],
                    "grouped_signals": _group_public_web_signals(email_candidates=[], profile_links=[]),
                    "evidence_links": [],
                    "promotions": [_public_web_promotion_detail(item) for item in promotions],
                    "promotion_summary": _public_web_promotion_summary(promotions),
                    "public_web_storage_owner": "crm_public_web_v1",
                    "raw_asset_policy": {
                        "raw_assets_included_by_default": False,
                        "default_export_surface": "model_safe_summary_signals_and_evidence_links",
                    },
                }
            return {
                "status": "not_found",
                "reason": "public_web_search_not_found",
                "record_id": normalized_record_id,
                "crm_record_id": normalized_record_id,
            }
        latest_run_record_id = str(latest_run.get("crm_record_id") or latest_run.get("record_id") or "").strip()
        if latest_run_record_id and latest_run_record_id != normalized_record_id:
            return {
                "status": "not_found",
                "reason": "public_web_search_not_found",
                "record_id": normalized_record_id,
                "crm_record_id": normalized_record_id,
            }
        signal_run_id = str(latest_run.get("run_id") or "").strip()
        if signal_run_id:
            signal_rows = self.store.list_person_public_web_signals(
                run_id=signal_run_id,
                record_id=normalized_record_id,
                limit=1000,
            )
        else:
            signal_rows = []
        latest_run = self._crm_public_web_run_with_materialized_signal_metrics(
            latest_run,
            signal_rows=signal_rows,
        )
        promotions_by_signal = _latest_public_web_promotions_by_signal(promotions)
        promotions_by_identity = _latest_public_web_promotions_by_identity(promotions)
        signals = []
        for row in signal_rows:
            signal = _public_web_signal_detail(row)
            promotion = promotions_by_signal.get(str(signal.get("signal_id") or ""))
            promotion_match_basis = "signal_id" if promotion else ""
            if promotion is None:
                promotion = promotions_by_identity.get(_public_web_signal_stable_identity_key(signal))
                promotion_match_basis = "stable_signal_identity" if promotion else ""
            signals.append(
                _public_web_signal_with_promotion(
                    signal,
                    promotion,
                    promotion_match_basis=promotion_match_basis,
                )
            )
        email_candidates = [signal for signal in signals if signal.get("signal_kind") == "email_candidate"]
        profile_links = [signal for signal in signals if signal.get("signal_kind") == "profile_link"]
        phase_commands = self._crm_public_web_phase_command_summaries_for_runs(
            [latest_run],
            workspace_id=str(crm_record.get("workspace_id") or "default"),
        ).get(signal_run_id or str(latest_run.get("run_id") or ""), {})
        latest_run_api = self._crm_public_web_run_api_record(latest_run, phase_commands=phase_commands) if latest_run else {}
        latest_run_detail = _public_web_run_detail(latest_run_api) if latest_run_api else None
        if latest_run_detail is not None:
            latest_run_detail["phase_commands"] = phase_commands
        return {
            "status": "ok",
            "record_id": normalized_record_id,
            "crm_record_id": normalized_record_id,
            "target_candidate": _public_web_target_candidate_summary(record),
            "crm_record": record,
            "latest_run": latest_run_detail,
            "phase_commands": phase_commands,
            "person_asset": _public_web_asset_detail(dict(asset or {})) if asset is not None else None,
            "signals": signals,
            "email_candidates": email_candidates,
            "profile_links": profile_links,
            "grouped_signals": _group_public_web_signals(
                email_candidates=email_candidates,
                profile_links=profile_links,
            ),
            "evidence_links": _public_web_evidence_links(signals),
            "promotions": [_public_web_promotion_detail(item) for item in promotions],
            "promotion_summary": _public_web_promotion_summary(promotions),
            "public_web_storage_owner": "crm_public_web_v1",
            "raw_asset_policy": {
                "raw_assets_included_by_default": False,
                "default_export_surface": "model_safe_summary_signals_and_evidence_links",
            },
        }

    def _list_crm_record_public_web_promotions_from_owner(self, crm_record_id: str) -> dict[str, Any]:
        normalized_record_id = str(crm_record_id or "").strip()
        if not normalized_record_id:
            return {"status": "invalid", "reason": "crm_record_id is required"}
        crm_record = self.store.get_crm_record(normalized_record_id)
        if not crm_record:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "record_id": normalized_record_id,
                "crm_record_id": normalized_record_id,
            }
        promotions = self.store.list_crm_public_web_promotions(
            crm_record_id=normalized_record_id,
            workspace_id=str(crm_record.get("workspace_id") or "default"),
            limit=1000,
        )
        return {
            "status": "ok",
            "record_id": normalized_record_id,
            "crm_record_id": normalized_record_id,
            "promotions": [_public_web_promotion_detail(item) for item in promotions],
            "promotion_summary": _public_web_promotion_summary(promotions),
            "public_web_storage_owner": "crm_public_web_v1",
        }

    def _promote_crm_public_web_signal_from_owner(self, crm_record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_record_id = str(crm_record_id or "").strip()
        if not normalized_record_id:
            return {"status": "invalid", "reason": "crm_record_id is required"}
        crm_record = self.store.get_crm_record(normalized_record_id)
        if not crm_record:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "record_id": normalized_record_id,
                "crm_record_id": normalized_record_id,
            }
        record = self._public_crm_record_payload(crm_record)
        workspace_id = str(crm_record.get("workspace_id") or "default").strip() or "default"
        signal_id = str(payload.get("signal_id") or "").strip()
        if not signal_id:
            return {"status": "invalid", "reason": "signal_id is required"}
        signal_row = self.store.get_person_public_web_signal(signal_id=signal_id)
        if signal_row is None:
            return {"status": "not_found", "reason": "public_web_signal_not_found", "signal_id": signal_id}
        signal_run_id = str(signal_row.get("run_id") or "").strip()
        signal_run = self.store.get_crm_public_web_run(run_id=signal_run_id) if signal_run_id else None
        if signal_run is None:
            return {
                "status": "invalid",
                "reason": "public_web_signal_not_owned_by_crm_public_web_run",
                "signal_id": signal_id,
                "run_id": signal_run_id,
                "crm_record_id": normalized_record_id,
            }
        signal_run_record_id = str(signal_run.get("crm_record_id") or signal_run.get("record_id") or "").strip()
        if signal_run_record_id != normalized_record_id:
            return {
                "status": "invalid",
                "reason": "signal_record_mismatch",
                "record_id": normalized_record_id,
                "signal_record_id": signal_run_record_id,
            }
        signal_run_workspace_id = str(signal_run.get("workspace_id") or "default").strip() or "default"
        if signal_run_workspace_id != workspace_id:
            return {
                "status": "invalid",
                "reason": "public_web_signal_not_latest_run",
                "record_id": normalized_record_id,
                "signal_run_id": signal_run_id,
                "latest_run_id": "",
                "workspace_id": workspace_id,
                "signal_workspace_id": signal_run_workspace_id,
            }
        signal_record_id = str(signal_row.get("record_id") or "").strip()
        if signal_record_id and signal_record_id != normalized_record_id:
            return {
                "status": "invalid",
                "reason": "signal_record_mismatch",
                "record_id": normalized_record_id,
                "signal_record_id": signal_record_id,
            }
        latest_runs = self.store.list_crm_public_web_runs(
            crm_record_id=normalized_record_id,
            workspace_id=workspace_id,
            limit=1,
        )
        latest_run_id = str(latest_runs[0].get("run_id") or "").strip() if latest_runs else ""
        if not latest_run_id or signal_run_id != latest_run_id:
            return {
                "status": "invalid",
                "reason": "public_web_signal_not_latest_run",
                "record_id": normalized_record_id,
                "signal_run_id": signal_run_id,
                "latest_run_id": latest_run_id,
            }
        signal = _public_web_signal_detail(signal_row)
        action = str(payload.get("action") or "promote").strip().lower()
        if action not in {"promote", "reject"}:
            return {"status": "invalid", "reason": "action must be promote or reject"}
        signal_kind = str(signal.get("signal_kind") or "").strip()
        if signal_kind not in {"email_candidate", "profile_link"}:
            return {"status": "invalid", "reason": "unsupported_signal_kind", "signal_kind": signal_kind}
        allow_unpublishable = bool(payload.get("allow_unpublishable") or payload.get("override"))
        override_reason = str(payload.get("override_reason") or payload.get("reason") or "").strip()
        override_validation_reason = ""
        if action == "promote":
            validation_error = _validate_public_web_signal_promotable(signal, allow_unpublishable=False)
            if validation_error:
                override_validation_reason = validation_error
                if not allow_unpublishable:
                    return {
                        "status": "invalid",
                        "reason": validation_error,
                        "signal": signal,
                    }
                if not override_reason:
                    return {
                        "status": "invalid",
                        "reason": "override_reason_required",
                        "validation_reason": validation_error,
                        "signal": signal,
                    }
                hard_validation_error = _validate_public_web_signal_promotable(signal, allow_unpublishable=True)
                if hard_validation_error:
                    return {
                        "status": "invalid",
                        "reason": hard_validation_error,
                        "signal": signal,
                    }
        promoted_value = _public_web_signal_promoted_value(signal)
        if action == "promote" and not promoted_value:
            return {"status": "invalid", "reason": "promoted_value_missing", "signal": signal}
        previous_primary_email = str(record.get("primary_email") or "").strip()
        promoted_field = (
            "primary_email"
            if signal_kind == "email_candidate" and action == "promote"
            else ("public_web_profile_link" if signal_kind == "profile_link" and action == "promote" else "")
        )
        promotion_id = (
            str(payload.get("promotion_id") or "").strip() or f"crm-public-web-promotion-{uuid.uuid4().hex[:16]}"
        )
        promotion_payload = {
            "promotion_id": promotion_id,
            "signal_id": signal_id,
            "run_id": signal_run_id,
            "asset_id": str(signal.get("asset_id") or ""),
            "person_identity_key": str(signal.get("person_identity_key") or record.get("person_identity_key") or ""),
            "crm_record_id": normalized_record_id,
            "workspace_id": workspace_id,
            "candidate_id": str(signal.get("candidate_id") or record.get("candidate_id") or ""),
            "candidate_name": str(signal.get("candidate_name") or record.get("candidate_name") or ""),
            "current_company": str(signal.get("current_company") or record.get("current_company") or ""),
            "linkedin_url_key": str(signal.get("linkedin_url_key") or ""),
            "signal_kind": signal_kind,
            "signal_type": str(signal.get("signal_type") or ""),
            "email_type": str(signal.get("email_type") or ""),
            "value": str(signal.get("value") or ""),
            "normalized_value": str(signal.get("normalized_value") or promoted_value),
            "url": str(signal.get("url") or ""),
            "source_url": str(signal.get("source_url") or ""),
            "source_domain": str(signal.get("source_domain") or ""),
            "source_family": str(signal.get("source_family") or ""),
            "source_title": str(signal.get("source_title") or ""),
            "confidence_label": str(signal.get("confidence_label") or ""),
            "confidence_score": _coerce_public_web_float(signal.get("confidence_score")),
            "identity_match_label": str(signal.get("identity_match_label") or ""),
            "identity_match_score": _coerce_public_web_float(signal.get("identity_match_score")),
            "publishable": bool(signal.get("publishable")),
            "clean_profile_link": bool(signal.get("clean_profile_link", True)),
            "link_shape_warnings": list(signal.get("link_shape_warnings") or []),
            "action": action,
            "promotion_status": "manually_promoted" if action == "promote" else "manually_rejected",
            "promoted_field": promoted_field,
            "previous_value": previous_primary_email if promoted_field == "primary_email" else "",
            "new_value": promoted_value,
            "operator": str(payload.get("operator") or payload.get("requested_by") or "operator").strip() or "operator",
            "note": str(payload.get("note") or override_reason).strip(),
            "evidence_excerpt": str(signal.get("evidence_excerpt") or ""),
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            "source_target_promotion_id": "",
            "metadata": {
                "raw_assets_included": False,
                "allow_unpublishable": allow_unpublishable,
                "override_reason": override_reason,
                "override_validation_reason": override_validation_reason,
                "promotion_requires_manual_override": bool(override_validation_reason),
                "source": "crm_public_web_search",
                "owner": "crm_public_web_v1",
            },
        }
        promotion = self.store.upsert_crm_public_web_promotion(promotion_payload)
        assertion: dict[str, Any] = {}
        assertion_event: dict[str, Any] = {}
        projection_index_rebuild: dict[str, Any] = {}
        if action == "promote":
            assertion_type = _public_web_signal_assertion_type(signal)
            person_identity_key = str(
                signal.get("person_identity_key") or promotion.get("person_identity_key") or record.get("person_identity_key") or ""
            ).strip()
            if assertion_type and person_identity_key:
                assertion = self.person_asset_writer.record_assertion(
                    {
                        "assertion_id": (
                            "crm_public_web_"
                            + _target_candidate_archive_name_component(
                                str(promotion.get("promotion_id") or promotion_id),
                                fallback="promotion",
                            )
                        ),
                        "person_identity_key": person_identity_key,
                        "assertion_type": assertion_type,
                        "value": promoted_value,
                        "normalized_value": promoted_value.lower() if assertion_type == "primary_email" else promoted_value,
                        "authority": "operator_confirmed",
                        "verification_status": "active",
                        "source_run_id": signal_run_id,
                        "confidence_score": _coerce_public_web_float(signal.get("confidence_score")),
                        "metadata": {
                            "source": "crm_public_web_promotion",
                            "promotion_id": str(promotion.get("promotion_id") or promotion_id),
                            "signal_id": signal_id,
                            "source_url": str(signal.get("source_url") or ""),
                            "source_domain": str(signal.get("source_domain") or ""),
                            "source_family": str(signal.get("source_family") or ""),
                            "export_policy": "default_human_promoted_assertion",
                        },
                    }
                )
                assertion_event = self.crm_writer.record_person_assertion_linked(
                    crm_record_id=normalized_record_id,
                    engagement_id=str(crm_record.get("current_engagement_id") or ""),
                    person_identity_key=person_identity_key,
                    assertion_id=str(assertion.get("assertion_id") or ""),
                    assertion_type=assertion_type,
                    verification_status="active",
                    actor_type="user",
                    actor_id=str(payload.get("operator") or payload.get("requested_by") or "operator").strip()
                    or "operator",
                    idempotency_key=f"crm:public-web-promotion:{str(promotion.get('promotion_id') or promotion_id)}",
                    source_run_id=signal_run_id,
                    reason="crm_public_web_signal_promoted",
                    payload={
                        "promotion_id": str(promotion.get("promotion_id") or promotion_id),
                        "signal_id": signal_id,
                        "crm_record_id": normalized_record_id,
                    },
                )
                projection_index_rebuild = self._enqueue_projection_person_search_index_for_person(
                    person_identity_key=person_identity_key,
                    job_id=signal_run_id,
                    reason="crm_public_web_person_assertion_promoted",
                )
        refreshed_record = self._public_crm_record_payload(self.store.get_crm_record(normalized_record_id) or crm_record)
        detail = self._get_crm_record_public_web_search_detail_from_owner(normalized_record_id)
        return {
            "status": "promoted" if action == "promote" else "rejected",
            "record_id": normalized_record_id,
            "crm_record_id": normalized_record_id,
            "signal": _public_web_signal_with_promotion(signal, promotion),
            "promotion": _public_web_promotion_detail(promotion),
            "person_assertion": assertion,
            "crm_event": assertion_event,
            "projection_person_search_index_rebuild": projection_index_rebuild,
            "crm_record": refreshed_record,
            "target_candidate": _public_web_target_candidate_summary(refreshed_record),
            "detail": detail if detail.get("status") == "ok" else None,
            "public_web_storage_owner": "crm_public_web_v1",
        }

    def _export_crm_public_web_archive_from_owner(
        self,
        payload: dict[str, Any],
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        query = dict(payload or {})
        requested_record_ids = _coerce_public_web_record_ids(
            query.get("crm_record_ids") or query.get("record_ids") or query.get("record_id")
        )
        if not requested_record_ids:
            return {"status": "invalid", "reason": "crm_record_ids are required"}
        export_mode = str(query.get("mode") or query.get("public_web_export_mode") or "promoted_only").strip()
        if export_mode not in {"promoted_only", "promoted_and_publishable"}:
            return {
                "status": "invalid",
                "reason": "mode must be promoted_only or promoted_and_publishable",
            }
        normalized_workspace_id = str(workspace_id or query.get("workspace_id") or "default").strip() or "default"
        records: list[dict[str, Any]] = []
        missing_record_ids: list[str] = []
        seen_record_ids: set[str] = set()
        for record_id in requested_record_ids:
            if record_id in seen_record_ids:
                continue
            seen_record_ids.add(record_id)
            crm_record = self.store.get_crm_record(record_id)
            if not crm_record or str(crm_record.get("workspace_id") or "default").strip() != normalized_workspace_id:
                missing_record_ids.append(record_id)
                continue
            records.append(self._public_crm_record_payload(dict(crm_record)))
        if missing_record_ids:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found",
                "missing_record_ids": missing_record_ids,
            }
        if not records:
            return {
                "status": "not_found",
                "reason": "no CRM records matched the requested Public Web export scope",
            }

        export_input_watermark = dict(query.get("export_input_watermark") or {})
        export_input_watermark_hash = str(
            query.get("export_input_watermark_hash") or export_input_watermark.get("watermark_hash") or ""
        ).strip()
        current_export_input_watermark = self._crm_public_web_export_input_watermark(
            workspace_id=normalized_workspace_id,
            crm_record_ids=[str(record.get("id") or record.get("crm_record_id") or "").strip() for record in records],
            export_mode=export_mode,
            include_record_inputs=True,
        )
        record_export_inputs_by_id = dict(current_export_input_watermark.pop("_record_export_inputs", {}) or {})
        current_export_input_watermark_hash = str(current_export_input_watermark.get("watermark_hash") or "").strip()
        if export_input_watermark_hash and export_input_watermark_hash != current_export_input_watermark_hash:
            return {
                "status": "failed",
                "reason": "crm_public_web_export_input_watermark_stale",
                "export_input_watermark_hash": export_input_watermark_hash,
                "current_export_input_watermark_hash": current_export_input_watermark_hash,
                "current_export_input_watermark": current_export_input_watermark,
                "public_web_storage_owner": "crm_public_web_v1",
                "read_contract": {
                    "source": "crm_public_web_assets+person_assertions",
                    "fallback_used": False,
                    "fail_closed": True,
                    "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                },
            }
        if not export_input_watermark_hash:
            export_input_watermark = current_export_input_watermark
            export_input_watermark_hash = current_export_input_watermark_hash
        elif not export_input_watermark:
            export_input_watermark = current_export_input_watermark
        ordered_record_export_inputs: list[dict[str, Any]] = []
        missing_export_input_record_ids: list[str] = []
        for record in records:
            record_id = str(record.get("id") or record.get("crm_record_id") or "").strip()
            export_input = dict(record_export_inputs_by_id.get(record_id) or {})
            if not export_input:
                missing_export_input_record_ids.append(record_id)
                continue
            ordered_record_export_inputs.append(export_input)
        if missing_export_input_record_ids:
            return {
                "status": "failed",
                "reason": "crm_public_web_export_input_snapshot_missing",
                "missing_record_ids": missing_export_input_record_ids,
                "export_input_watermark_hash": export_input_watermark_hash,
                "current_export_input_watermark_hash": current_export_input_watermark_hash,
                "current_export_input_watermark": current_export_input_watermark,
                "public_web_storage_owner": "crm_public_web_v1",
                "read_contract": {
                    "source": "crm_public_web_assets+person_assertions",
                    "fallback_used": False,
                    "fail_closed": True,
                    "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                },
            }
        base_name = f"crm-public-web-{_china_local_filename_timestamp()}"
        manifest_records: list[dict[str, Any]] = []
        summary_rows: list[dict[str, str]] = []
        signal_rows: list[dict[str, str]] = []
        evidence_rows: list[dict[str, str]] = []
        promotion_rows: list[dict[str, str]] = []
        exported_record_count = 0
        no_public_web_result_count = 0
        no_exportable_signal_count = 0
        non_terminal_run_count = 0
        archive_buffer = io.BytesIO()
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for ordinal, (record, export_input) in enumerate(zip(records, ordered_record_export_inputs), start=1):
                record_id = str(record.get("id") or record.get("crm_record_id") or "").strip()
                detail = dict(export_input.get("detail") or {})
                exported_signals = list(export_input.get("exported_signals") or [])
                exported_evidence_links = list(export_input.get("exported_evidence_links") or [])
                record_promotions = [dict(item) for item in list(export_input.get("record_promotions") or [])]
                record_export_status = dict(export_input.get("record_export_status") or {})
                record_status = str(export_input.get("record_status") or record_export_status.get("status") or "")
                skip_reason = str(export_input.get("skip_reason") or record_export_status.get("skip_reason") or "")
                if record_status == "exported":
                    exported_record_count += 1
                elif skip_reason == "no_public_web_result":
                    no_public_web_result_count += 1
                elif skip_reason == "public_web_run_not_terminal":
                    non_terminal_run_count += 1
                else:
                    no_exportable_signal_count += 1
                promotion_rows.extend(
                    _public_web_promotion_csv_row(record=record, promotion=dict(item)) for item in record_promotions
                )
                signal_rows.extend(
                    _public_web_signal_csv_row(record=record, signal=dict(signal)) for signal in exported_signals
                )
                evidence_rows.extend(
                    _public_web_evidence_csv_row(record=record, evidence=dict(evidence))
                    for evidence in exported_evidence_links
                )
                summary_rows.append(
                    _public_web_candidate_summary_csv_row(
                        record=record,
                        detail=dict(detail),
                        exported_signals=exported_signals,
                        export_mode=export_mode,
                        record_export_status=record_export_status,
                    )
                )
                record_slug = _target_candidate_archive_name_component(
                    str(record.get("candidate_name") or record.get("display_name") or ""),
                    fallback=f"crm-record-{ordinal}",
                )
                record_id_component = _target_candidate_archive_name_component(
                    record_id,
                    fallback=f"record-{ordinal}",
                )
                archive.writestr(
                    f"{base_name}/crm_records/{record_slug}__{record_id_component}/public_web_summary.json",
                    json.dumps(
                        {
                            "generated_at": _china_now_iso(),
                            "export_mode": export_mode,
                            "record_id": record_id,
                            "crm_record_id": record_id,
                            "target_candidate": _public_web_target_candidate_summary(record),
                            "crm_record": {
                                key: record.get(key)
                                for key in (
                                    "crm_record_id",
                                    "person_identity_key",
                                    "candidate_identity_key",
                                    "stage",
                                    "follow_up_status",
                                    "source_projection_id",
                                    "source_collection_id",
                                )
                                if record.get(key) not in (None, "", [], {})
                            },
                            "latest_run": detail.get("latest_run"),
                            "person_asset": detail.get("person_asset"),
                            "promotion_summary": detail.get("promotion_summary") or {},
                            "export_record_status": record_status,
                            "export_skip_reason": skip_reason,
                            "exported_signals": exported_signals,
                            "evidence_links": exported_evidence_links,
                            "public_web_storage_owner": "crm_public_web_v1",
                            "raw_asset_policy": {
                                "raw_assets_included": False,
                                "excluded_raw_asset_types": ["raw_html", "raw_pdf", "raw_search_payload"],
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    ).encode("utf-8"),
                )
                latest_run = dict(detail.get("latest_run") or {})
                manifest_records.append(
                    {
                        "record_id": record_id,
                        "crm_record_id": record_id,
                        "candidate_id": str(record.get("candidate_id") or ""),
                        "candidate_name": str(record.get("candidate_name") or record.get("display_name") or ""),
                        "person_identity_key": str(record.get("person_identity_key") or ""),
                        "latest_run_id": str(latest_run.get("run_id") or ""),
                        "latest_run_status": str(latest_run.get("status") or ""),
                        "detail_status": str(detail.get("status") or ""),
                        "export_record_status": record_status,
                        "export_skip_reason": skip_reason,
                        "exported_signal_count": len(exported_signals),
                        "exported_evidence_link_count": len(exported_evidence_links),
                        "promotion_summary": detail.get("promotion_summary") or {},
                    }
                )
            archive.writestr(
                f"{base_name}/public_web_summary.csv",
                _build_csv_bytes(
                    summary_rows,
                    [
                        "record_id",
                        "candidate_name",
                        "current_company",
                        "linkedin_url",
                        "promoted_primary_email",
                        "email_type",
                        "email_source_url",
                        "email_confidence",
                        "homepage_url",
                        "github_url",
                        "x_url",
                        "substack_url",
                        "scholar_url",
                        "other_profile_links",
                        "exported_signal_count",
                        "promotion_count",
                        "export_mode",
                        "public_web_status",
                        "latest_run_status",
                        "export_record_status",
                        "export_skip_reason",
                    ],
                ),
            )
            archive.writestr(
                f"{base_name}/public_web_signals.csv",
                _build_csv_bytes(
                    signal_rows,
                    [
                        "record_id",
                        "candidate_name",
                        "signal_id",
                        "signal_kind",
                        "signal_type",
                        "email_type",
                        "value",
                        "url",
                        "source_url",
                        "source_domain",
                        "source_family",
                        "confidence_label",
                        "confidence_score",
                        "identity_match_label",
                        "identity_match_score",
                        "promotion_status",
                        "promotion_id",
                        "export_status",
                        "evidence_excerpt",
                    ],
                ),
            )
            archive.writestr(
                f"{base_name}/public_web_evidence_links.csv",
                _build_csv_bytes(
                    evidence_rows,
                    [
                        "record_id",
                        "candidate_name",
                        "source_url",
                        "source_domain",
                        "source_family",
                        "source_title",
                        "signal_ids",
                        "signal_kinds",
                        "signal_types",
                        "identity_match_labels",
                        "max_confidence_score",
                    ],
                ),
            )
            archive.writestr(
                f"{base_name}/public_web_promotions.csv",
                _build_csv_bytes(
                    promotion_rows,
                    [
                        "record_id",
                        "candidate_name",
                        "promotion_id",
                        "signal_id",
                        "action",
                        "promotion_status",
                        "signal_kind",
                        "signal_type",
                        "email_type",
                        "new_value",
                        "previous_value",
                        "source_url",
                        "source_domain",
                        "confidence_label",
                        "identity_match_label",
                        "operator",
                        "created_at",
                        "note",
                        "override_reason",
                        "override_validation_reason",
                    ],
                ),
            )
            archive.writestr(
                f"{base_name}/public_web_manifest.json",
                json.dumps(
                    {
                        "generated_at": _china_now_iso(),
                        "record_count": len(records),
                        "requested_crm_record_ids": requested_record_ids,
                        "workspace_id": normalized_workspace_id,
                        "exported_record_count": exported_record_count,
                        "no_public_web_result_count": no_public_web_result_count,
                        "no_exportable_signal_count": no_exportable_signal_count,
                        "non_terminal_run_count": non_terminal_run_count,
                        "export_mode": export_mode,
                        "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
                        "export_input_watermark_hash": export_input_watermark_hash,
                        "export_input_watermark": export_input_watermark,
                        "default_export_surface": "manual_promotions"
                        if export_mode == "promoted_only"
                        else "manual_promotions_and_ai_publishable_signals",
                        "public_web_storage_owner": "crm_public_web_v1",
                        "raw_assets_included": False,
                        "excluded_raw_asset_types": ["raw_html", "raw_pdf", "raw_search_payload"],
                        "files": [
                            "public_web_summary.csv",
                            "public_web_signals.csv",
                            "public_web_evidence_links.csv",
                            "public_web_promotions.csv",
                        ],
                        "records": manifest_records,
                    },
                    ensure_ascii=False,
                    indent=2,
                ).encode("utf-8"),
            )
        return {
            "status": "ok",
            "filename": f"{base_name}.zip",
            "content_type": "application/zip",
            "body": archive_buffer.getvalue(),
            "record_count": len(records),
            "export_mode": export_mode,
            "export_contract_version": CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
            "export_input_watermark_hash": export_input_watermark_hash,
            "export_input_watermark": export_input_watermark,
            "exported_signal_count": len(signal_rows),
            "exported_record_count": exported_record_count,
            "no_public_web_result_count": no_public_web_result_count,
            "no_exportable_signal_count": no_exportable_signal_count,
            "non_terminal_run_count": non_terminal_run_count,
            "public_web_storage_owner": "crm_public_web_v1",
        }

    def _validate_crm_public_web_action_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        normalized = dict(payload or {})
        workspace_result = self._require_crm_public_web_body_workspace_id(normalized, operation="action")
        if isinstance(workspace_result, dict):
            return workspace_result
        workspace_id = workspace_result
        record_ids = _coerce_public_web_record_ids(
            normalized.get("crm_record_ids") or normalized.get("record_ids") or normalized.get("record_id")
        )
        if record_ids:
            record_ids_result = self._prepare_crm_public_web_record_ids(
                record_ids,
                require_non_empty=True,
                workspace_id=workspace_id,
            )
            if isinstance(record_ids_result, dict):
                return record_ids_result
        run_ids = [
            str(item or "").strip()
            for item in list(normalized.get("run_ids") or normalized.get("public_web_run_ids") or [])
            if str(item or "").strip()
        ]
        run_id = str(normalized.get("run_id") or "").strip()
        if run_id:
            run_ids.append(run_id)
        runs: list[dict[str, Any]] = []
        for item in sorted(set(run_ids)):
            run = self.store.get_crm_public_web_run(run_id=item)
            if run is None:
                return {"status": "not_found", "reason": "public_web_run_not_found", "run_id": item}
            runs.append(dict(run))
        batch_id = str(normalized.get("batch_id") or "").strip()
        if batch_id:
            batch = self.store.get_crm_public_web_batch(batch_id=batch_id)
            if batch is not None:
                batch_workspace_id = str(batch.get("workspace_id") or "default").strip() or "default"
                if batch_workspace_id != workspace_id:
                    return {
                        "status": "invalid",
                        "reason": "public_web_batch_workspace_mismatch",
                        "workspace_id": workspace_id,
                        "batch_id": batch_id,
                        "batch_workspace_id": batch_workspace_id,
                    }
            runs.extend(
                dict(run)
                for run in self.store.list_crm_public_web_runs(
                    batch_id=batch_id,
                    workspace_id=workspace_id,
                    limit=1000,
                )
            )
        if runs:
            validation = self._validate_crm_public_web_runs(runs, workspace_id=workspace_id)
            if isinstance(validation, dict):
                return validation
        return None

    def _validate_crm_public_web_runs(
        self,
        runs: list[dict[str, Any]],
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any] | None:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        missing_crm_record_ids: list[str] = []
        mismatched_run_ids: list[str] = []
        mismatched_record_ids: list[str] = []
        for run in runs:
            record_id = str(run.get("crm_record_id") or run.get("record_id") or "").strip()
            run_workspace_id = str(run.get("workspace_id") or "default").strip() or "default"
            if run_workspace_id != normalized_workspace_id:
                run_id = str(run.get("run_id") or "").strip()
                if run_id:
                    mismatched_run_ids.append(run_id)
                continue
            if not record_id:
                continue
            crm_record = self.store.get_crm_record(record_id)
            if crm_record is None:
                missing_crm_record_ids.append(record_id)
                continue
            record_workspace_id = str(crm_record.get("workspace_id") or "default").strip() or "default"
            if record_workspace_id != normalized_workspace_id:
                mismatched_record_ids.append(record_id)
        if mismatched_run_ids or mismatched_record_ids:
            return {
                "status": "invalid",
                "reason": "public_web_run_workspace_mismatch",
                "workspace_id": normalized_workspace_id,
                "run_ids": sorted(set(mismatched_run_ids)),
                "record_ids": sorted(set(mismatched_record_ids)),
            }
        if missing_crm_record_ids:
            return {
                "status": "invalid",
                "reason": "public_web_run_not_owned_by_crm_record",
                "record_ids": sorted(set(missing_crm_record_ids)),
            }
        return None

    def _with_crm_public_web_contract(
        self,
        result: dict[str, Any],
        *,
        operation: str,
        read: bool = False,
    ) -> dict[str, Any]:
        payload = dict(result or {})
        if isinstance(payload.get("runs"), list):
            phase_commands_by_run_id = dict(payload.get("phase_commands_by_run_id") or {})
            payload["runs"] = self._crm_public_web_run_api_records(
                [dict(run) for run in list(payload.get("runs") or []) if isinstance(run, dict)],
                phase_commands_by_run_id=phase_commands_by_run_id,
            )
        contract_key = "read_contract" if read else "write_contract"
        existing = dict(payload.get(contract_key) or {})
        payload[contract_key] = {
            **existing,
            "owner": "CRMWriter+PublicWebService",
            "source": "crm_records",
            "fallback_used": False,
            "fail_closed": True,
            "operation": operation,
            "legacy_target_candidates_used": False,
            "public_web_storage_owner": "crm_public_web_v1",
            "public_web_execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            "public_web_storage_bridge": "",
            "legacy_target_candidate_state_owner": False,
            "bridge_removal_phase": "retired",
        }
        return payload

    def _resolve_crm_public_web_action_run_ids(
        self,
        payload: dict[str, Any],
        *,
        action_name: str,
        allow_terminal: bool,
    ) -> tuple[list[str], str] | dict[str, Any]:
        query = dict(payload or {})
        reason = str(query.get("reason") or f"{action_name}_requested_by_operator").strip()
        workspace_result = self._require_crm_public_web_body_workspace_id(query, operation=action_name)
        if isinstance(workspace_result, dict):
            return workspace_result
        workspace_id = workspace_result
        run_ids = [
            str(item or "").strip()
            for item in list(query.get("run_ids") or query.get("public_web_run_ids") or [])
            if str(item or "").strip()
        ]
        run_id = str(query.get("run_id") or "").strip()
        if run_id:
            run_ids.append(run_id)
        batch_id = str(query.get("batch_id") or "").strip()
        record_ids = _coerce_public_web_record_ids(
            query.get("crm_record_ids") or query.get("record_ids") or query.get("record_id")
        )
        if batch_id:
            batch = self.store.get_crm_public_web_batch(batch_id=batch_id)
            if batch is not None:
                batch_workspace_id = str(batch.get("workspace_id") or "default").strip() or "default"
                if batch_workspace_id != workspace_id:
                    return {
                        "status": "invalid",
                        "reason": "public_web_batch_workspace_mismatch",
                        "workspace_id": workspace_id,
                        "batch_id": batch_id,
                        "batch_workspace_id": batch_workspace_id,
                    }
            run_ids.extend(
                str(run.get("run_id") or "").strip()
                for run in self.store.list_crm_public_web_runs(
                    batch_id=batch_id,
                    workspace_id=workspace_id,
                    limit=1000,
                )
                if str(run.get("run_id") or "").strip()
            )
        for record_id in record_ids:
            run_ids.extend(
                str(run.get("run_id") or "").strip()
                for run in self.store.list_crm_public_web_runs(
                    crm_record_id=record_id,
                    workspace_id=workspace_id,
                    limit=1,
                )
                if str(run.get("run_id") or "").strip()
            )
        normalized_run_ids: list[str] = []
        seen_run_ids: set[str] = set()
        for item in run_ids:
            if item in seen_run_ids:
                continue
            seen_run_ids.add(item)
            normalized_run_ids.append(item)
        if not normalized_run_ids:
            return {"status": "invalid", "reason": f"run_id, batch_id, or record_ids are required for {action_name}"}
        if not allow_terminal:
            filtered_run_ids: list[str] = []
            skipped: list[dict[str, Any]] = []
            for item in normalized_run_ids:
                run = self.store.get_crm_public_web_run(run_id=item)
                if run is None:
                    return {"status": "not_found", "reason": "public_web_run_not_found", "run_id": item}
                if str(run.get("status") or "") in {
                    "completed",
                    "completed_with_errors",
                    "needs_review",
                    "failed",
                    "cancelled",
                }:
                    skipped.append(run)
                    continue
                filtered_run_ids.append(item)
            if not filtered_run_ids:
                return {"status": "skipped", "reason": "all selected Public Web runs are already terminal", "runs": skipped}
            normalized_run_ids = filtered_run_ids
        return normalized_run_ids, reason

    def _interrupt_crm_public_web_workers_for_run(
        self,
        run: dict[str, Any],
        *,
        reason: str,
        operator: str,
    ) -> list[dict[str, Any]]:
        run_id = str(run.get("run_id") or "").strip()
        if not run_id:
            return []
        worker_key = str(run.get("worker_key") or "").strip() or public_web_worker_key(run_id)
        job_id = f"crm-public-web-{str(run.get('batch_id') or '').strip()}"
        workers = [
            worker
            for worker in self.store.list_agent_workers(job_id=job_id, lane_id=PUBLIC_WEB_WORKER_LANE)
            if str(worker.get("worker_key") or "") == worker_key
        ]
        updated_workers: list[dict[str, Any]] = []
        for worker in workers:
            worker_status = str(worker.get("status") or "").strip().lower()
            if worker_status in {"completed", "failed", "cancelled", "canceled", "interrupted"}:
                continue
            worker_id = int(worker.get("worker_id") or 0)
            if worker_id <= 0:
                continue
            self.agent_runtime.interrupt_worker(worker_id)
            checkpoint = dict(worker.get("checkpoint") or {})
            checkpoint.update(
                {
                    "stage": "cancelled",
                    "status": "cancelled",
                    "run_id": run_id,
                    "batch_id": str(run.get("batch_id") or ""),
                    "reason": reason,
                    "operator": operator,
                }
            )
            completed = self.store.complete_agent_worker(
                worker_id,
                status="cancelled",
                checkpoint_payload=checkpoint,
                output_payload={
                    "run_id": run_id,
                    "run_status": "cancelled",
                    "reason": reason,
                    "operator": operator,
                },
            )
            if completed is not None:
                updated_workers.append(completed)
        return updated_workers

    def get_target_candidate_public_web_search_detail(self, record_id: str) -> dict[str, Any]:
        normalized_record_id = str(record_id or "").strip()
        return {
            **_legacy_target_public_web_orchestrator_disabled_result(
                operation="detail",
                canonical_endpoint="/api/crm/records/{crm_record_id}/public-web-search",
                record_id=normalized_record_id,
            ),
            "target_candidate": _public_web_target_candidate_summary({}),
            "latest_run": None,
            "person_asset": None,
            "signals": [],
            "email_candidates": [],
            "profile_links": [],
            "grouped_signals": {"email_candidates": [], "profile_links": []},
            "evidence_links": [],
            "promotions": [],
            "promotion_summary": {},
            "raw_asset_policy": {
                "raw_assets_included_by_default": False,
                "default_export_surface": "retired",
            },
        }

    def list_target_candidate_public_web_promotions(self, record_id: str) -> dict[str, Any]:
        normalized_record_id = str(record_id or "").strip()
        return {
            **_legacy_target_public_web_orchestrator_disabled_result(
                operation="promotion_list",
                canonical_endpoint="/api/crm/records/{crm_record_id}/public-web-promotions",
                record_id=normalized_record_id,
            ),
            "promotions": [],
            "promotion_summary": {},
        }

    def promote_target_candidate_public_web_signal(self, record_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_record_id = str(record_id or "").strip()
        return {
            **_legacy_target_public_web_orchestrator_disabled_result(
                operation="promotion",
                canonical_endpoint="/api/crm/records/{crm_record_id}/public-web-promotions",
                record_id=normalized_record_id,
            ),
            "promotion": None,
            "signal": None,
            "person_assertion": None,
            "crm_event": None,
            "projection_person_search_index_rebuild": None,
            "target_candidate": _public_web_target_candidate_summary({}),
            "detail": None,
        }

    def _ensure_crm_public_web_job(
        self,
        *,
        batch: dict[str, Any],
        runs: list[dict[str, Any]],
        request_payload: dict[str, Any],
    ) -> dict[str, Any]:
        batch_id = str(batch.get("batch_id") or "").strip()
        job_id = f"crm-public-web-{batch_id}" if batch_id else f"crm-public-web-{uuid.uuid4().hex[:12]}"
        request = self._crm_public_web_job_request(batch=batch, request_payload=request_payload)
        plan_payload = {
            "workflow_kind": CRM_PUBLIC_WEB_JOB_TYPE,
            "batch_id": batch_id,
            "run_count": len(runs),
            "scheduler_lane_limits": {PUBLIC_WEB_WORKER_LANE: 2},
            "acquisition_strategy": {"cost_policy": {"worker_retry_limit": 1}},
            "default_workflow_stage": "not_enabled",
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
        }
        self.store.save_job(
            job_id,
            CRM_PUBLIC_WEB_JOB_TYPE,
            "running",
            "public_web_search",
            request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "batch_id": batch_id,
                "run_count": len(runs),
                "status": str(batch.get("status") or "queued"),
                "public_web_storage_owner": "crm_public_web_v1",
                "public_web_execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
            artifact_path=str(dict(batch.get("metadata") or {}).get("artifact_root") or ""),
            idempotency_key=str(batch.get("idempotency_key") or ""),
        )
        return {
            "job_id": job_id,
            "request": request.to_record(),
            "plan": plan_payload,
        }

    def _run_crm_public_web_queue_batch_command(
        self,
        command: dict[str, Any],
        *,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "skipped", "reason": "command_id_missing"}
        lease_owner = f"{CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(
            command_id,
            lease_owner=lease_owner,
            lease_seconds=max(30, int(lease_seconds or 300)),
        )
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            latest_status = str(latest.get("status") or "").strip()
            self._kernel._sync_operation_run_from_workflow_command(
                latest,
                actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                source="crm_public_web.queue_batch_owner",
            )
            return {
                "status": "skipped",
                "reason": "crm_public_web_queue_batch_command_not_claimed",
                "command_status": latest_status,
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W7_crm_public_web_queue_batch",
                ),
            }
        self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner)
        latest_command = self.store.get_workflow_command(command_id) or claimed
        payload = dict(latest_command.get("payload") or {})
        batch_id = str(payload.get("batch_id") or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        if not batch_id and str(payload.get("operation_planning_mode") or "").strip() == (
            "create_crm_public_web_batch_from_operation"
        ):
            record_ids = _coerce_public_web_record_ids(
                payload.get("crm_record_ids") or payload.get("record_ids") or payload.get("record_id")
            )
            missing_record_ids: list[str] = []
            records: list[dict[str, Any]] = []
            for record_id in record_ids:
                record = self.store.get_crm_record(record_id)
                if not record or str(record.get("workspace_id") or "default").strip() != workspace_id:
                    missing_record_ids.append(record_id)
                    continue
                records.append(self._public_crm_record_payload(record))
            if missing_record_ids or not records:
                failed = self.store.mark_workflow_command_failed(
                    command_id,
                    error_text="crm_public_web_operation_crm_record_not_found",
                    retryable=False,
                )
                self._kernel._sync_operation_run_from_workflow_command(
                    failed or latest_command,
                    actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                    source="crm_public_web.queue_batch_owner",
                )
                return {
                    "status": "failed",
                    "reason": "crm_public_web_operation_crm_record_not_found",
                    "missing_record_ids": missing_record_ids,
                    "workflow_command": self._kernel._workflow_command_observation(
                        failed or latest_command,
                        migration_phase="W9_operation_crm_public_web_queue_batch",
                    ),
                }
            request_payload = {
                **dict(payload.get("request_payload") or {}),
                "workspace_id": workspace_id,
                "crm_record_ids": record_ids,
                "record_ids": record_ids,
            }
            batch_result = start_crm_public_web_batch(
                store=self.store,
                crm_records=records,
                runtime_dir=self.runtime_dir,
                payload=request_payload,
            )
            batch = dict(batch_result.get("batch") or {})
            runs = [dict(run) for run in list(batch_result.get("runs") or []) if isinstance(run, dict)]
            batch_id = str(batch.get("batch_id") or "").strip()
            if not batch_id or not runs:
                failed = self.store.mark_workflow_command_failed(
                    command_id,
                    error_text="crm_public_web_operation_batch_planning_failed",
                    retryable=True,
                    retry_delay_seconds=10,
                )
                self._kernel._sync_operation_run_from_workflow_command(
                    failed or latest_command,
                    actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                    source="crm_public_web.queue_batch_owner",
                )
                return {
                    "status": "failed",
                    "reason": "crm_public_web_operation_batch_planning_failed",
                    "workflow_command": self._kernel._workflow_command_observation(
                        failed or latest_command,
                        migration_phase="W9_operation_crm_public_web_queue_batch",
                    ),
                }
            job_payload = self._ensure_crm_public_web_job(
                batch=batch,
                runs=runs,
                request_payload=request_payload,
            )
            payload = {
                **payload,
                "batch_id": batch_id,
                "job_payload": job_payload,
                "request_payload": request_payload,
                "run_ids": [str(run.get("run_id") or "") for run in runs if str(run.get("run_id") or "")],
                "record_ids": record_ids,
                "runs": runs,
                "operation_planning_status": str(batch_result.get("status") or "queued"),
            }
            self.store.update_workflow_command_payload(command_id, payload=payload)
            latest_command = self.store.get_workflow_command(command_id) or latest_command
        if not batch_id:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text="crm_public_web_queue_batch_missing_batch_id",
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                source="crm_public_web.queue_batch_owner",
            )
            return {
                "status": "failed",
                "reason": "crm_public_web_queue_batch_missing_batch_id",
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_crm_public_web_queue_batch",
                ),
            }
        batch = self.store.get_crm_public_web_batch(batch_id=batch_id)
        if not batch:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text="crm_public_web_batch_not_found",
                retryable=True,
                retry_delay_seconds=10,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                source="crm_public_web.queue_batch_owner",
            )
            return {
                "status": "deferred",
                "reason": "crm_public_web_batch_not_found",
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_crm_public_web_queue_batch",
                ),
            }
        batch_workspace_id = str(batch.get("workspace_id") or "default").strip() or "default"
        if batch_workspace_id != workspace_id:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text="crm_public_web_batch_workspace_mismatch",
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                source="crm_public_web.queue_batch_owner",
            )
            return {
                "status": "failed",
                "reason": "crm_public_web_batch_workspace_mismatch",
                "workspace_id": workspace_id,
                "batch_id": batch_id,
                "batch_workspace_id": batch_workspace_id,
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_crm_public_web_queue_batch",
                ),
            }
        runs = self.store.list_crm_public_web_runs(batch_id=batch_id, workspace_id=workspace_id)
        if not runs:
            runs = [dict(run) for run in list(payload.get("runs") or []) if isinstance(run, dict)]
        if not runs:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text="crm_public_web_runs_not_found",
                retryable=True,
                retry_delay_seconds=10,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                source="crm_public_web.queue_batch_owner",
            )
            return {
                "status": "deferred",
                "reason": "crm_public_web_runs_not_found",
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest_command,
                    migration_phase="W7_crm_public_web_queue_batch",
                ),
            }
        job_payload = dict(payload.get("job_payload") or {})
        if not job_payload:
            job_payload = self._ensure_crm_public_web_job(
                batch=dict(batch),
                runs=[dict(run) for run in runs],
                request_payload=dict(payload.get("request_payload") or {}),
            )
        phase_owner_link = {
            "workflow_run_id": str(latest_command.get("workflow_run_id") or "").strip(),
            "operation_id": str(latest_command.get("operation_id") or "").strip(),
            "queue_command_id": command_id,
            "phase_command_owner": CRM_PUBLIC_WEB_PHASE_OWNER,
            "phase_command_contract": "crm_public_web_phase_command_status_v1",
        }
        batch = self.store.upsert_crm_public_web_batch(
            {
                **dict(batch),
                "metadata": {
                    **dict(dict(batch).get("metadata") or {}),
                    **phase_owner_link,
                    "phase_command_workflow_run_id": phase_owner_link["workflow_run_id"],
                },
            }
        )
        linked_runs: list[dict[str, Any]] = []
        for run in runs:
            run_payload = dict(run)
            run_id = str(run_payload.get("run_id") or "").strip()
            if not run_id:
                continue
            linked = self.store.update_crm_public_web_run(
                run_id,
                {
                    "analysis_checkpoint": {
                        **dict(run_payload.get("analysis_checkpoint") or {}),
                        **phase_owner_link,
                        "phase_command_workflow_run_id": phase_owner_link["workflow_run_id"],
                    }
                },
            )
            linked_runs.append(dict(linked or run_payload))
        if linked_runs:
            runs = linked_runs
        phase_commands = self._plan_initial_crm_public_web_run_phase_commands(
            parent_command=latest_command,
            batch_id=batch_id,
            workspace_id=workspace_id,
            runs=[dict(run) for run in runs],
            request_payload=dict(payload.get("request_payload") or {}),
        )
        phase_command_ids = [
            str(command.get("command_id") or "").strip()
            for command in phase_commands
            if str(command.get("command_id") or "").strip()
        ]
        terminal_reused_count = len(
            [
                run
                for run in runs
                if str(dict(run).get("status") or "").strip()
                in {"completed", "completed_with_errors", "needs_review", "failed", "cancelled"}
            ]
        )
        worker_summary = {
            "queued_worker_count": 0,
            "reused_terminal_run_count": terminal_reused_count,
            "worker_ids": [],
            "queued_phase_command_count": len(phase_command_ids),
            "phase_command_ids": phase_command_ids,
            "phase_command_types": [CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE] if phase_command_ids else [],
            "lane_id": PUBLIC_WEB_WORKER_LANE,
            "recovery_kind": CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND,
            "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
        }
        sync_result = sync_crm_public_web_batch_summary(
            self.store,
            batch_id,
            workspace_id=workspace_id,
        )
        succeeded = self.store.mark_workflow_command_succeeded(
            command_id,
            result={
                "worker_summary": worker_summary,
                "sync_result": sync_result,
                "batch_id": batch_id,
                "workspace_id": workspace_id,
                "run_count": len(runs),
                "queued_worker_count": 0,
                "queued_phase_command_count": len(phase_command_ids),
                "reused_terminal_run_count": _coerce_int(worker_summary.get("reused_terminal_run_count"), 0),
                "worker_ids": [],
                "phase_command_ids": phase_command_ids,
                "operation_completion_deferred": bool(
                    str(latest_command.get("operation_id") or "").strip() and phase_command_ids
                ),
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
        )
        self._kernel._sync_operation_run_from_workflow_command(
            succeeded or latest_command,
            actor=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            source="crm_public_web.queue_batch_owner",
        )
        return {
            "status": "completed",
            "reason": "crm_public_web_queue_batch_command_owner",
            "batch_id": batch_id,
            "workspace_id": workspace_id,
            "command_count": 1,
            "executed_command_count": 1,
            "claimed_count": 1,
            "completed_count": 1,
            "failed_count": 0,
            "queued_worker_count": 0,
            "queued_phase_command_count": len(phase_command_ids),
            "reused_terminal_run_count": _coerce_int(worker_summary.get("reused_terminal_run_count"), 0),
            "worker_ids": [],
            "phase_command_ids": phase_command_ids,
            "worker_summary": worker_summary,
            "sync_result": sync_result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or latest_command,
                migration_phase="W7_crm_public_web_queue_batch",
            ),
            "legacy_bridge_used": False,
            "migration_phase": "W7_crm_public_web_queue_batch",
        }

    def _drain_crm_public_web_queue_batch_commands(
        self,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = dict(payload or {})
        if not _env_bool("CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_OWNER_ENABLED", True):
            return {
                "status": "skipped",
                "reason": "crm_public_web_queue_batch_command_owner_disabled",
                "command_owner_env": "CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_OWNER_ENABLED",
                "owner": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
                "command_type": CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
                "command_count": 0,
                "executed_command_count": 0,
                "claimed_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "queued_worker_count": 0,
                "legacy_bridge_used": False,
                "report_visible": True,
                "migration_phase": "W7_crm_public_web_queue_batch",
            }
        workflow_run_id = str(payload.get("workflow_run_id") or "").strip()
        batch_id = str(payload.get("batch_id") or "").strip()
        if not workflow_run_id and batch_id:
            workflow_run_id = self._crm_public_web_workflow_run_id(batch_id)
        limit = max(1, _coerce_int(payload.get("command_limit") or payload.get("crm_public_web_queue_batch_command_limit"), 20))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            limit=limit,
        )
        results: list[dict[str, Any]] = []
        executed_command_count = 0
        claimed_count = 0
        completed_count = 0
        failed_count = 0
        queued_worker_count = 0
        queued_phase_command_count = 0
        reused_terminal_run_count = 0
        worker_ids: list[int] = []
        for command in ready_commands:
            result = self._run_crm_public_web_queue_batch_command(
                dict(command),
                lease_seconds=max(30, _env_int("CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_LEASE_SECONDS", 300)),
            )
            results.append(result)
            if str(result.get("status") or "") in {"completed", "deferred", "failed"}:
                executed_command_count += 1
            claimed_count += _coerce_int(result.get("claimed_count"), 0)
            completed_count += _coerce_int(result.get("completed_count"), 0)
            failed_count += _coerce_int(result.get("failed_count"), 0)
            queued_worker_count += _coerce_int(result.get("queued_worker_count"), 0)
            queued_phase_command_count += _coerce_int(result.get("queued_phase_command_count"), 0)
            reused_terminal_run_count += _coerce_int(result.get("reused_terminal_run_count"), 0)
            for worker_id_value in list(result.get("worker_ids") or []):
                worker_id = _coerce_int(worker_id_value, 0)
                if worker_id > 0 and worker_id not in worker_ids:
                    worker_ids.append(worker_id)
        return {
            "status": "active" if executed_command_count > 0 or claimed_count > 0 else "idle",
            "reason": (
                "crm_public_web_queue_batch_command_owner"
                if ready_commands
                else "no_ready_crm_public_web_queue_batch_commands"
            ),
            "workflow_run_id": workflow_run_id,
            "command_count": len(ready_commands),
            "executed_command_count": executed_command_count,
            "claimed_count": claimed_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_worker_count": queued_worker_count,
            "queued_phase_command_count": queued_phase_command_count,
            "reused_terminal_run_count": reused_terminal_run_count,
            "worker_ids": worker_ids,
            "lane_id": PUBLIC_WEB_WORKER_LANE,
            "recovery_kind": CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND,
            "owner": CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            "command_type": CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            "legacy_bridge_used": False,
            "migration_phase": "W7_crm_public_web_queue_batch",
            "items": results,
            "results": results,
        }

    @staticmethod
    def _next_crm_public_web_phase_command_type(run_status: str) -> str:
        normalized_status = str(run_status or "").strip()
        if normalized_status in {"queued", "search_submitted", "searching"}:
            return CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE
        if normalized_status in {"entry_links_ready", "fetching"}:
            return CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE
        if normalized_status in {"documents_fetched", "analyzing"}:
            return CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE
        if normalized_status == "adjudication_completed":
            return CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE
        if normalized_status == "analysis_completed":
            return CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE
        return ""

    @staticmethod
    def _crm_public_web_phase_status_rank(run_status: str) -> int:
        normalized_status = str(run_status or "").strip()
        return {
            "queued": 0,
            "search_submitted": 1,
            "searching": 1,
            "entry_links_ready": 2,
            "fetching": 3,
            "documents_fetched": 4,
            "analyzing": 5,
            "adjudication_completed": 6,
            "analysis_completed": 7,
            "completed": 8,
            "completed_with_errors": 8,
            "failed": 8,
            "cancelled": 8,
            "retired": 8,
        }.get(normalized_status, -1)

    @staticmethod
    def _crm_public_web_phase_expected_statuses(command_type: str) -> tuple[str, ...]:
        normalized_type = str(command_type or "").strip()
        return workflow_command_expected_run_statuses(normalized_type)

    @classmethod
    def _crm_public_web_phase_status_relation(cls, command_type: str, run_status: str) -> str:
        normalized_status = str(run_status or "").strip()
        if normalized_status in {"completed", "completed_with_errors", "failed", "cancelled", "retired"}:
            return "terminal"
        expected_statuses = cls._crm_public_web_phase_expected_statuses(command_type)
        if not expected_statuses:
            return "unknown_command_type"
        if normalized_status in set(expected_statuses):
            return "ready"
        current_rank = cls._crm_public_web_phase_status_rank(normalized_status)
        expected_ranks = [
            cls._crm_public_web_phase_status_rank(status)
            for status in expected_statuses
            if cls._crm_public_web_phase_status_rank(status) >= 0
        ]
        if current_rank < 0 or not expected_ranks:
            return "unknown_run_status"
        if current_rank < min(expected_ranks):
            return "prerequisite_not_met"
        if current_rank > max(expected_ranks):
            return "already_advanced"
        return "status_mismatch"

    @staticmethod
    def _crm_public_web_phase_entity_type(command_type: str) -> str:
        return "crm_public_web_run"

    def _record_crm_public_web_phase_entity_delta(
        self,
        *,
        command: dict[str, Any],
        activity: dict[str, Any],
        attempt: dict[str, Any],
        command_type: str,
        run_id: str,
        batch_id: str,
        workspace_id: str,
        run_status: str,
        worker_status: str,
        delta_status: str,
        reason: str,
        phase_result: dict[str, Any] | None = None,
        downstream_command_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        if not activity:
            return {}
        normalized_command_type = str(command_type or "").strip()
        normalized_run_id = str(run_id or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        if not workflow_run_id or not normalized_command_type or not normalized_run_id:
            return {}
        result_payload = dict(phase_result or {})
        summary = dict(result_payload.get("summary") or {})
        phase_metrics = dict(summary.get("phase_metrics") or {})
        artifact_refs: list[str] = []
        artifact_root = str(summary.get("artifact_root") or result_payload.get("artifact_root") or "").strip()
        if artifact_root:
            artifact_refs.append(artifact_root)
        readiness_effect = default_readiness_effect_for_command_type(normalized_command_type)
        normalized_status = str(delta_status or "recorded").strip() or "recorded"
        return self.store.upsert_workflow_entity_delta(
            {
                "workspace_id": str(workspace_id or "default").strip() or "default",
                "workflow_run_id": workflow_run_id,
                "operation_run_id": str(command_payload.get("operation_id") or "").strip(),
                "command_id": str(command_payload.get("command_id") or "").strip(),
                "activity_run_id": str(activity.get("activity_run_id") or "").strip(),
                "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                "entity_type": self._crm_public_web_phase_entity_type(normalized_command_type),
                "entity_key": normalized_run_id,
                "delta_kind": readiness_effect or normalized_command_type,
                "status": normalized_status,
                "reason": str(reason or "").strip(),
                "source_ref": {
                    "batch_id": str(batch_id or "").strip(),
                    "run_id": normalized_run_id,
                    "command_type": normalized_command_type,
                    "run_status": str(run_status or "").strip(),
                    "worker_status": str(worker_status or "").strip(),
                    "downstream_command_ids": [
                        str(command_id or "").strip()
                        for command_id in list(downstream_command_ids or [])
                        if str(command_id or "").strip()
                    ],
                },
                "entity_payload": {
                    "run_id": normalized_run_id,
                    "batch_id": str(batch_id or "").strip(),
                    "run_status": str(run_status or "").strip(),
                    "worker_status": str(worker_status or "").strip(),
                    "phase_metrics": phase_metrics,
                },
                "projection_effect": {
                    "entered_projection": False,
                    "reason": "crm_public_web_signal_materialization_not_projection_membership",
                },
                "artifact_refs": artifact_refs,
                "metadata": {
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                },
                "idempotency_key": (
                    f"crm_public_web_phase_delta:{normalized_command_type}:"
                    f"{str(command_payload.get('command_id') or '').strip()}:{normalized_run_id}"
                ),
            }
        )

    def _record_crm_public_web_signal_entity_deltas(
        self,
        *,
        command: dict[str, Any],
        activity: dict[str, Any],
        attempt: dict[str, Any],
        run_id: str,
        batch_id: str,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return []
        signals = self.store.list_person_public_web_signals(run_id=normalized_run_id, limit=1000)
        deltas: list[dict[str, Any]] = []
        for signal in signals:
            signal_payload = dict(signal or {})
            signal_id = str(signal_payload.get("signal_id") or "").strip()
            if not signal_id:
                continue
            artifact_refs_payload = signal_payload.get("artifact_refs") or {}
            artifact_refs: list[str] = []
            if isinstance(artifact_refs_payload, dict):
                for value in artifact_refs_payload.values():
                    if isinstance(value, str) and value.strip():
                        artifact_refs.append(value.strip())
                    elif isinstance(value, (list, tuple)):
                        artifact_refs.extend(str(item or "").strip() for item in value if str(item or "").strip())
            source_url = str(signal_payload.get("source_url") or signal_payload.get("url") or "").strip()
            if source_url:
                artifact_refs.append(source_url)
            delta = self._kernel._record_command_activity_entity_delta(
                command=command,
                activity=activity,
                attempt=attempt,
                entity_type="public_web_signal",
                entity_key=signal_id,
                delta_kind="crm_public_web_signal_materialized",
                status="recorded",
                reason="crm_public_web_signals_materialized",
                source_ref={
                    "batch_id": str(batch_id or "").strip(),
                    "run_id": normalized_run_id,
                    "signal_id": signal_id,
                    "signal_kind": str(signal_payload.get("signal_kind") or "").strip(),
                    "source_url": source_url,
                    "command_type": CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                },
                entity_payload={
                    "signal_id": signal_id,
                    "run_id": normalized_run_id,
                    "record_id": str(signal_payload.get("record_id") or "").strip(),
                    "person_identity_key": str(signal_payload.get("person_identity_key") or "").strip(),
                    "signal_kind": str(signal_payload.get("signal_kind") or "").strip(),
                    "signal_type": str(signal_payload.get("signal_type") or "").strip(),
                    "value": str(signal_payload.get("value") or "").strip(),
                    "normalized_value": str(signal_payload.get("normalized_value") or "").strip(),
                    "confidence_label": str(signal_payload.get("confidence_label") or "").strip(),
                    "confidence_score": signal_payload.get("confidence_score"),
                    "identity_match_label": str(signal_payload.get("identity_match_label") or "").strip(),
                    "publishable": bool(signal_payload.get("publishable")),
                    "promotion_status": str(signal_payload.get("promotion_status") or "").strip(),
                },
                projection_effect={
                    "entered_projection": False,
                    "candidate_count_delta": 0,
                    "reason": "public_web_signal_materialized_not_projection_membership",
                },
                artifact_refs=artifact_refs,
                metadata={
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                    "signal_entity_delta_contract": "crm_public_web_signal_delta_v1",
                },
                idempotency_scope="crm_public_web_signal_materialized",
            )
            if delta:
                deltas.append(delta)
        return deltas

    def _sync_crm_public_web_signals_to_person_asset_layer(
        self,
        *,
        command: dict[str, Any],
        activity: dict[str, Any],
        attempt: dict[str, Any],
        run_id: str,
        batch_id: str,
        workspace_id: str,
        signals: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    ) -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        signal_rows = [dict(item or {}) for item in list(signals or []) if isinstance(item, dict)]
        if not normalized_run_id and signal_rows:
            normalized_run_id = str(signal_rows[0].get("run_id") or "").strip()
        if not normalized_run_id:
            return {
                "status": "invalid",
                "reason": "run_id_required",
                "asset_ids": [],
                "evidence_ids": [],
                "entity_delta_ids": [],
            }
        if not signal_rows:
            signal_rows = self.store.list_person_public_web_signals(run_id=normalized_run_id, limit=1000)
        writer = PersonAssetWriter(self.store, writer_id="crm_public_web_signal_materialize_v1")
        asset_ids: list[str] = []
        evidence_ids: list[str] = []
        entity_delta_ids: list[str] = []
        skipped_count = 0
        for signal in signal_rows:
            signal_payload = dict(signal or {})
            signal_id = str(signal_payload.get("signal_id") or "").strip()
            person_identity_key = str(signal_payload.get("person_identity_key") or "").strip()
            normalized_value = str(signal_payload.get("normalized_value") or signal_payload.get("value") or "").strip()
            source_url = str(signal_payload.get("source_url") or signal_payload.get("url") or "").strip()
            if not signal_id or not person_identity_key or (not normalized_value and not source_url):
                skipped_count += 1
                continue
            signal_run_id = str(signal_payload.get("run_id") or normalized_run_id).strip() or normalized_run_id
            signal_token = hashlib.sha1(
                "|".join([signal_run_id, signal_id, person_identity_key, normalized_value, source_url]).encode("utf-8")
            ).hexdigest()[:24]
            asset_id = f"person-public-web-signal-asset-{signal_token}"
            evidence_id = f"person-public-web-evidence-{signal_token}"
            artifact_refs = dict(signal_payload.get("artifact_refs") or {})
            asset = writer.record_asset(
                {
                    "asset_id": asset_id,
                    "person_identity_key": person_identity_key,
                    "asset_type": "public_web_signal",
                    "source_kind": "crm_public_web_signal",
                    "source_run_id": signal_run_id,
                    "content_ref": source_url or normalized_value,
                    "source_url": source_url,
                    "visibility_scope": "internal_evidence",
                    "status": "available",
                    "metadata": {
                        "workspace_id": str(workspace_id or "default").strip() or "default",
                        "batch_id": str(batch_id or "").strip(),
                        "run_id": signal_run_id,
                        "signal_id": signal_id,
                        "record_id": str(signal_payload.get("record_id") or "").strip(),
                        "candidate_id": str(signal_payload.get("candidate_id") or "").strip(),
                        "signal_kind": str(signal_payload.get("signal_kind") or "").strip(),
                        "signal_type": str(signal_payload.get("signal_type") or "").strip(),
                        "public_web_storage_owner": "crm_public_web_v1",
                        "source_command_id": str(dict(command or {}).get("command_id") or "").strip(),
                        "activity_run_id": str(activity.get("activity_run_id") or "").strip(),
                        "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                    },
                }
            )
            evidence = writer.record_evidence(
                {
                    "evidence_id": evidence_id,
                    "person_identity_key": person_identity_key,
                    "asset_id": str(asset.get("asset_id") or asset_id),
                    "evidence_type": str(
                        signal_payload.get("signal_kind")
                        or signal_payload.get("signal_type")
                        or "public_web_signal"
                    ).strip()
                    or "public_web_signal",
                    "value": str(signal_payload.get("value") or normalized_value).strip(),
                    "normalized_value": normalized_value,
                    "source_url": source_url,
                    "source_domain": str(signal_payload.get("source_domain") or "").strip(),
                    "confidence_score": signal_payload.get("confidence_score"),
                    "identity_match_score": signal_payload.get("identity_match_score"),
                    "publishable": bool(signal_payload.get("publishable")),
                    "evidence_excerpt": str(signal_payload.get("evidence_excerpt") or "").strip(),
                    "artifact_refs": artifact_refs,
                    "status": "observed",
                    "metadata": {
                        "workspace_id": str(workspace_id or "default").strip() or "default",
                        "batch_id": str(batch_id or "").strip(),
                        "run_id": signal_run_id,
                        "signal_id": signal_id,
                        "record_id": str(signal_payload.get("record_id") or "").strip(),
                        "candidate_id": str(signal_payload.get("candidate_id") or "").strip(),
                        "confidence_label": str(signal_payload.get("confidence_label") or "").strip(),
                        "identity_match_label": str(signal_payload.get("identity_match_label") or "").strip(),
                        "promotion_status": str(signal_payload.get("promotion_status") or "").strip(),
                        "model_provider": str(signal_payload.get("model_provider") or "").strip(),
                        "model_version": str(signal_payload.get("model_version") or "").strip(),
                        "public_web_storage_owner": "crm_public_web_v1",
                        "source_command_id": str(dict(command or {}).get("command_id") or "").strip(),
                        "activity_run_id": str(activity.get("activity_run_id") or "").strip(),
                        "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                    },
                }
            )
            asset_id = str(asset.get("asset_id") or asset_id).strip()
            evidence_id = str(evidence.get("evidence_id") or evidence_id).strip()
            if asset_id:
                asset_ids.append(asset_id)
                asset_delta = self._kernel._record_command_activity_entity_delta(
                    command=command,
                    activity=activity,
                    attempt=attempt,
                    entity_type="person_asset",
                    entity_key=asset_id,
                    delta_kind="crm_public_web_signal_asset_synced",
                    status="recorded",
                    reason="crm_public_web_signal_synced_to_person_asset",
                    source_ref={
                        "batch_id": str(batch_id or "").strip(),
                        "run_id": signal_run_id,
                        "signal_id": signal_id,
                        "source_url": source_url,
                        "command_type": CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                    },
                    entity_payload={
                        "asset_id": asset_id,
                        "asset_type": "public_web_signal",
                        "person_identity_key": person_identity_key,
                    },
                    projection_effect={"entered_projection": False, "person_asset_layer_refreshed": True},
                    artifact_refs=[source_url] if source_url else [],
                    metadata={
                        "person_asset_sync_contract": "crm_public_web_signal_person_asset_sync_v1",
                        "public_web_storage_owner": "crm_public_web_v1",
                    },
                    idempotency_scope="crm_public_web_signal_person_asset_synced",
                )
                if str(asset_delta.get("delta_id") or "").strip():
                    entity_delta_ids.append(str(asset_delta.get("delta_id") or "").strip())
            if evidence_id:
                evidence_ids.append(evidence_id)
                evidence_delta = self._kernel._record_command_activity_entity_delta(
                    command=command,
                    activity=activity,
                    attempt=attempt,
                    entity_type="person_evidence",
                    entity_key=evidence_id,
                    delta_kind="crm_public_web_signal_evidence_synced",
                    status="recorded",
                    reason="crm_public_web_signal_synced_to_person_evidence",
                    source_ref={
                        "batch_id": str(batch_id or "").strip(),
                        "run_id": signal_run_id,
                        "signal_id": signal_id,
                        "source_url": source_url,
                        "command_type": CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                    },
                    entity_payload={
                        "evidence_id": evidence_id,
                        "asset_id": asset_id,
                        "person_identity_key": person_identity_key,
                        "evidence_type": str(evidence.get("evidence_type") or "").strip(),
                        "normalized_value": normalized_value,
                        "publishable": bool(signal_payload.get("publishable")),
                    },
                    projection_effect={"entered_projection": False, "person_evidence_layer_refreshed": True},
                    artifact_refs=[source_url] if source_url else [],
                    metadata={
                        "person_asset_sync_contract": "crm_public_web_signal_person_asset_sync_v1",
                        "public_web_storage_owner": "crm_public_web_v1",
                    },
                    idempotency_scope="crm_public_web_signal_person_evidence_synced",
                )
                if str(evidence_delta.get("delta_id") or "").strip():
                    entity_delta_ids.append(str(evidence_delta.get("delta_id") or "").strip())
        return {
            "status": "synced",
            "run_id": normalized_run_id,
            "asset_count": len(asset_ids),
            "evidence_count": len(evidence_ids),
            "entity_delta_count": len(entity_delta_ids),
            "skipped_count": skipped_count,
            "asset_ids": asset_ids,
            "evidence_ids": evidence_ids,
            "entity_delta_ids": entity_delta_ids,
            "contract": "crm_public_web_signal_person_asset_sync_v1",
        }

    def _record_crm_public_web_document_entity_deltas(
        self,
        *,
        command: dict[str, Any],
        activity: dict[str, Any],
        attempt: dict[str, Any],
        run_id: str,
        batch_id: str,
        workspace_id: str,
    ) -> list[dict[str, Any]]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return []
        run = self.store.get_crm_public_web_run(run_id=normalized_run_id) or {}
        analysis_checkpoint = dict(run.get("analysis_checkpoint") or {})
        document_fetch_payload_path = str(analysis_checkpoint.get("document_fetch_payload_path") or "").strip()
        if not document_fetch_payload_path:
            return []
        payload_path = Path(document_fetch_payload_path).expanduser()
        try:
            document_fetch_payload = json.loads(payload_path.read_text(encoding="utf-8"))
        except Exception:
            return []
        documents = [
            dict(item)
            for item in list(dict(document_fetch_payload or {}).get("fetched_documents") or [])
            if isinstance(item, dict)
        ]
        deltas: list[dict[str, Any]] = []
        for index, document in enumerate(documents, start=1):
            source_url = str(document.get("source_url") or document.get("url") or "").strip()
            final_url = str(document.get("final_url") or "").strip()
            title = str(document.get("title") or "").strip()
            document_key_seed = "|".join(
                [
                    normalized_run_id,
                    source_url,
                    final_url,
                    title,
                    str(index),
                ]
            )
            document_key = "public_web_document:" + hashlib.sha1(
                document_key_seed.encode("utf-8")
            ).hexdigest()[:24]
            artifact_refs = [str(payload_path)]
            for url_value in (source_url, final_url):
                if url_value:
                    artifact_refs.append(url_value)
            delta_status = "not_applied" if document.get("error") else "recorded"
            delta = self._kernel._record_command_activity_entity_delta(
                command=command,
                activity=activity,
                attempt=attempt,
                entity_type="public_web_document",
                entity_key=document_key,
                delta_kind="crm_public_web_document_fetched",
                status=delta_status,
                reason="crm_public_web_documents_fetched" if delta_status == "recorded" else "crm_public_web_document_fetch_error",
                source_ref={
                    "batch_id": str(batch_id or "").strip(),
                    "run_id": normalized_run_id,
                    "document_fetch_payload_path": str(payload_path),
                    "source_url": source_url,
                    "final_url": final_url,
                    "command_type": CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                },
                entity_payload={
                    "run_id": normalized_run_id,
                    "source_url": source_url,
                    "final_url": final_url,
                    "title": title,
                    "source_family": str(document.get("source_family") or "").strip(),
                    "source_domain": str(document.get("source_domain") or "").strip(),
                    "content_type": str(document.get("content_type") or "").strip(),
                    "status_code": document.get("status_code"),
                    "error": str(document.get("error") or "").strip(),
                },
                projection_effect={
                    "entered_projection": False,
                    "candidate_count_delta": 0,
                    "reason": "public_web_document_fetched_not_projection_membership",
                },
                artifact_refs=artifact_refs,
                metadata={
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                    "document_entity_delta_contract": "crm_public_web_document_delta_v1",
                },
                idempotency_scope="crm_public_web_document_fetched",
            )
            if delta:
                deltas.append(delta)
        return deltas

    def _run_crm_public_web_phase_command(
        self,
        command: dict[str, Any],
        *,
        lease_seconds: int = 300,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        command_type = str(command_payload.get("command_type") or "").strip()
        migration_phase = self._crm_public_web_phase_command_migration_phase(command_type)
        if not command_id:
            return {"status": "skipped", "reason": "command_id_missing"}
        if command_type not in set(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES):
            return {"status": "skipped", "reason": "not_crm_public_web_phase_command", "command_type": command_type}
        lease_owner = f"{CRM_PUBLIC_WEB_PHASE_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(
            command_id,
            lease_owner=lease_owner,
            lease_seconds=max(30, int(lease_seconds or 300)),
        )
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            self._kernel._sync_operation_run_from_workflow_command(
                latest,
                actor=CRM_PUBLIC_WEB_PHASE_OWNER,
                source="crm_public_web.phase_owner",
            )
            return {
                "status": "skipped",
                "reason": "crm_public_web_phase_command_not_claimed",
                "command_status": str(latest.get("status") or "").strip(),
                "workflow_command": self._kernel._workflow_command_observation(latest, migration_phase=migration_phase),
            }
        self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner)
        latest_command = self.store.get_workflow_command(command_id) or claimed
        payload = dict(latest_command.get("payload") or {})
        run_id = str(payload.get("run_id") or "").strip()
        batch_id = str(payload.get("batch_id") or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        if not run_id:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text="crm_public_web_phase_missing_run_id",
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_PHASE_OWNER,
                source="crm_public_web.phase_owner",
            )
            return {
                "status": "failed",
                "reason": "crm_public_web_phase_missing_run_id",
                "workflow_command": self._kernel._workflow_command_observation(failed or latest_command, migration_phase=migration_phase),
            }
        activity, attempt = self._kernel._start_workflow_command_activity_attempt(
            latest_command,
            activity_type=command_type,
            owner=CRM_PUBLIC_WEB_PHASE_OWNER,
            phase=migration_phase,
            lease_owner=lease_owner,
            provider="crm_public_web_runtime",
            provider_request_ref=run_id,
            input_payload={
                "batch_id": batch_id,
                "run_id": run_id,
                "workspace_id": workspace_id,
                "phase_command_type": command_type,
                "request_payload": dict(payload.get("request_payload") or {}),
            },
            entity_counts={"crm_public_web_run_count": 1},
            metadata={
                "batch_id": batch_id,
                "run_id": run_id,
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
            attempt_suffix=command_type,
        )
        current_run = self.store.get_crm_public_web_run(run_id=run_id) or {}
        current_run_status = str(current_run.get("status") or "").strip()
        current_batch_id = str(current_run.get("batch_id") or "").strip()
        if current_batch_id and current_batch_id != batch_id:
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="succeeded",
                phase="stale_batch_superseded",
                output={
                    "batch_id": batch_id,
                    "current_batch_id": current_batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "run_status": current_run_status,
                    "phase_command_type": command_type,
                    "no_op_reason": "crm_public_web_phase_stale_batch_superseded",
                },
                entity_counts={"crm_public_web_run_count": 1},
                metadata={
                    "no_op_reason": "crm_public_web_phase_stale_batch_superseded",
                    "current_batch_id": current_batch_id,
                },
                attempt_status="succeeded",
            )
            self._record_crm_public_web_phase_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                command_type=command_type,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
                run_status=current_run_status,
                worker_status="completed",
                delta_status="not_applied",
                reason="crm_public_web_phase_stale_batch_superseded",
                phase_result={"current_batch_id": current_batch_id},
            )
            succeeded = self.store.mark_workflow_command_succeeded(
                command_id,
                result={
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "current_batch_id": current_batch_id,
                    "workspace_id": workspace_id,
                    "run_status": current_run_status,
                    "worker_status": "completed",
                    "phase_command_type": command_type,
                    "no_op_reason": "crm_public_web_phase_stale_batch_superseded",
                    "terminal_run": True,
                    "downstream_command_ids": [],
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                },
            )
            return {
                "status": "completed",
                "reason": "crm_public_web_phase_stale_batch_superseded",
                "run_id": run_id,
                "batch_id": batch_id,
                "current_batch_id": current_batch_id,
                "run_status": current_run_status,
                "command_count": 1,
                "executed_command_count": 1,
                "claimed_count": 1,
                "completed_count": 1,
                "failed_count": 0,
                "no_op_reason": "crm_public_web_phase_stale_batch_superseded",
                "workflow_command": self._kernel._workflow_command_observation(succeeded or latest_command, migration_phase=migration_phase),
                "legacy_bridge_used": False,
                "migration_phase": migration_phase,
            }
        current_run_record_id = str(current_run.get("crm_record_id") or current_run.get("record_id") or "").strip()
        if current_run_record_id:
            latest_runs_for_record = self.store.list_crm_public_web_runs(
                crm_record_id=current_run_record_id,
                workspace_id=workspace_id,
                limit=1,
            )
            latest_run_id_for_record = (
                str(latest_runs_for_record[0].get("run_id") or "").strip() if latest_runs_for_record else ""
            )
            if latest_run_id_for_record and latest_run_id_for_record != run_id:
                final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                    activity=activity,
                    attempt=attempt,
                    status="succeeded",
                    phase="stale_run_superseded",
                    output={
                        "batch_id": batch_id,
                        "run_id": run_id,
                        "latest_run_id": latest_run_id_for_record,
                        "workspace_id": workspace_id,
                        "crm_record_id": current_run_record_id,
                        "run_status": current_run_status,
                        "phase_command_type": command_type,
                        "no_op_reason": "crm_public_web_phase_stale_run_superseded",
                    },
                    entity_counts={"crm_public_web_run_count": 1},
                    metadata={
                        "no_op_reason": "crm_public_web_phase_stale_run_superseded",
                        "latest_run_id": latest_run_id_for_record,
                    },
                    attempt_status="succeeded",
                )
                self._record_crm_public_web_phase_entity_delta(
                    command=latest_command,
                    activity=final_activity or activity,
                    attempt=final_attempt or attempt,
                    command_type=command_type,
                    run_id=run_id,
                    batch_id=batch_id,
                    workspace_id=workspace_id,
                    run_status=current_run_status,
                    worker_status="completed",
                    delta_status="not_applied",
                    reason="crm_public_web_phase_stale_run_superseded",
                    phase_result={"latest_run_id": latest_run_id_for_record},
                )
                succeeded = self.store.mark_workflow_command_succeeded(
                    command_id,
                    result={
                        "run_id": run_id,
                        "batch_id": batch_id,
                        "latest_run_id": latest_run_id_for_record,
                        "workspace_id": workspace_id,
                        "crm_record_id": current_run_record_id,
                        "run_status": current_run_status,
                        "worker_status": "completed",
                        "phase_command_type": command_type,
                        "no_op_reason": "crm_public_web_phase_stale_run_superseded",
                        "terminal_run": True,
                        "downstream_command_ids": [],
                        "public_web_storage_owner": "crm_public_web_v1",
                        "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                    },
                )
                return {
                    "status": "completed",
                    "reason": "crm_public_web_phase_stale_run_superseded",
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "latest_run_id": latest_run_id_for_record,
                    "run_status": current_run_status,
                    "command_count": 1,
                    "executed_command_count": 1,
                    "claimed_count": 1,
                    "completed_count": 1,
                    "failed_count": 0,
                    "no_op_reason": "crm_public_web_phase_stale_run_superseded",
                    "workflow_command": self._kernel._workflow_command_observation(succeeded or latest_command, migration_phase=migration_phase),
                    "legacy_bridge_used": False,
                    "migration_phase": migration_phase,
                }
        status_relation = self._crm_public_web_phase_status_relation(command_type, current_run_status)
        if status_relation in {"already_advanced", "terminal"}:
            next_command_type = "" if status_relation == "terminal" else self._next_crm_public_web_phase_command_type(current_run_status)
            next_command: dict[str, Any] = {}
            if next_command_type and next_command_type != command_type:
                next_command = self._plan_crm_public_web_run_phase_command(
                    workflow_run_id=str(latest_command.get("workflow_run_id") or ""),
                    operation_id=str(latest_command.get("operation_id") or ""),
                    batch_id=batch_id,
                    run_id=run_id,
                    workspace_id=workspace_id,
                    command_type=next_command_type,
                    parent_command=latest_command,
                    request_payload=dict(payload.get("request_payload") or {}),
                    source="crm_public_web_phase_owner_status_guard",
                )
            downstream_ids = [
                str(next_command.get("command_id") or "").strip()
            ] if str(next_command.get("command_id") or "").strip() else []
            no_op_reason = (
                "crm_public_web_phase_terminal_already_reached"
                if status_relation == "terminal"
                else "crm_public_web_phase_status_already_advanced"
            )
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="succeeded",
                phase=status_relation,
                output={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "run_status": current_run_status,
                    "phase_command_type": command_type,
                    "next_command_type": next_command_type,
                    "downstream_command_ids": downstream_ids,
                    "no_op_reason": no_op_reason,
                },
                entity_counts={"crm_public_web_run_count": 1},
                metadata={"no_op_reason": no_op_reason, "next_command_type": next_command_type},
                attempt_status="succeeded",
            )
            self._record_crm_public_web_phase_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                command_type=command_type,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
                run_status=current_run_status,
                worker_status="running" if downstream_ids else "completed",
                delta_status="not_applied",
                reason=no_op_reason,
                phase_result={"next_command_type": next_command_type, "downstream_command_ids": downstream_ids},
                downstream_command_ids=downstream_ids,
            )
            succeeded = self.store.mark_workflow_command_succeeded(
                command_id,
                result={
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "workspace_id": workspace_id,
                    "run_status": current_run_status,
                    "worker_status": "running" if downstream_ids else "completed",
                    "phase_command_type": command_type,
                    "next_command_type": next_command_type,
                    "downstream_command_ids": downstream_ids,
                    "no_op_reason": no_op_reason,
                    "terminal_run": not bool(downstream_ids),
                    "operation_completion_deferred": bool(
                        str(latest_command.get("operation_id") or "").strip() and downstream_ids
                    ),
                    "public_web_storage_owner": "crm_public_web_v1",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                },
            )
            if not downstream_ids:
                self._kernel._sync_operation_run_from_workflow_command(
                    succeeded or latest_command,
                    actor=CRM_PUBLIC_WEB_PHASE_OWNER,
                    source="crm_public_web.phase_owner_status_guard",
                )
            return {
                "status": "completed",
                "reason": no_op_reason,
                "run_id": run_id,
                "batch_id": batch_id,
                "run_status": current_run_status,
                "next_command_type": next_command_type,
                "downstream_command_ids": downstream_ids,
                "command_count": 1,
                "executed_command_count": 1,
                "claimed_count": 1,
                "completed_count": 1,
                "failed_count": 0,
                "no_op_reason": no_op_reason,
                "workflow_command": self._kernel._workflow_command_observation(succeeded or latest_command, migration_phase=migration_phase),
                "legacy_bridge_used": False,
                "migration_phase": migration_phase,
            }
        if status_relation in {"prerequisite_not_met", "status_mismatch", "unknown_run_status"}:
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="retry_wait",
                phase=status_relation,
                output={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "run_status": current_run_status,
                    "phase_command_type": command_type,
                    "expected_statuses": list(self._crm_public_web_phase_expected_statuses(command_type)),
                    "reason": "crm_public_web_phase_prerequisite_not_met",
                },
                entity_counts={"crm_public_web_run_count": 1},
                metadata={
                    "wait_reason": "crm_public_web_phase_prerequisite_not_met",
                    "status_relation": status_relation,
                },
                attempt_status="retry_wait",
            )
            self._record_crm_public_web_phase_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                command_type=command_type,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
                run_status=current_run_status,
                worker_status="running",
                delta_status="not_applied",
                reason="crm_public_web_phase_prerequisite_not_met",
                phase_result={"status_relation": status_relation},
            )
            waiting = self._workflow_command_waiting_prerequisite(
                command_id,
                retry_delay_seconds=10,
                from_statuses=("running", "claimed"),
                result={
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "workspace_id": workspace_id,
                    "run_status": current_run_status,
                    "phase_command_type": command_type,
                    "expected_statuses": list(self._crm_public_web_phase_expected_statuses(command_type)),
                    "reason": "crm_public_web_phase_prerequisite_not_met",
                    "status_relation": status_relation,
                },
            )
            return {
                "status": "deferred",
                "reason": "crm_public_web_phase_prerequisite_not_met",
                "run_id": run_id,
                "batch_id": batch_id,
                "run_status": current_run_status,
                "status_relation": status_relation,
                "command_count": 1,
                "executed_command_count": 1,
                "claimed_count": 1,
                "completed_count": 0,
                "failed_count": 0,
                "workflow_command": self._kernel._workflow_command_observation(waiting or latest_command, migration_phase=migration_phase),
                "legacy_bridge_used": False,
                "migration_phase": migration_phase,
            }
        try:
            phase_result = execute_crm_public_web_run_once(
                store=self.store,
                search_provider=self.acquisition_engine.search_provider,
                model_client=getattr(self.acquisition_engine, "model_client", None),
                runtime_dir=self.runtime_dir,
                run_id=run_id,
                worker=None,
                phase_command_type=command_type,
            )
        except Exception as exc:
            self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="failed",
                phase="failed",
                output={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "phase_command_type": command_type,
                },
                entity_counts={"crm_public_web_run_count": 1},
                error={"message": str(exc), "reason": "crm_public_web_phase_command_failed"},
                metadata={"error_reason": "crm_public_web_phase_command_failed"},
                attempt_status="failed",
            )
            self._record_crm_public_web_phase_entity_delta(
                command=latest_command,
                activity=activity,
                attempt=attempt,
                command_type=command_type,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
                run_status="failed",
                worker_status="failed",
                delta_status="failed",
                reason="crm_public_web_phase_command_failed",
                phase_result={"error": str(exc)},
            )
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=f"crm_public_web_phase_command_failed:{exc}",
                retryable=True,
                retry_delay_seconds=10,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_PHASE_OWNER,
                source="crm_public_web.phase_owner",
            )
            return {
                "status": "failed",
                "reason": "crm_public_web_phase_command_failed",
                "error": str(exc),
                "workflow_command": self._kernel._workflow_command_observation(failed or latest_command, migration_phase=migration_phase),
            }
        run_status = str(phase_result.get("run_status") or "").strip()
        worker_status = str(phase_result.get("worker_status") or "").strip()
        if command_type == CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE and run_status in {
            "queued",
            "search_submitted",
            "searching",
        }:
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="retry_wait",
                phase="remote_search_not_ready",
                output={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "run_status": run_status,
                    "worker_status": worker_status,
                    "phase_result": phase_result,
                },
                entity_counts={"crm_public_web_run_count": 1},
                metadata={"wait_reason": "crm_public_web_remote_search_not_ready"},
                attempt_status="retry_wait",
            )
            self._record_crm_public_web_phase_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                command_type=command_type,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
                run_status=run_status,
                worker_status=worker_status,
                delta_status="not_applied",
                reason="crm_public_web_remote_search_not_ready",
                phase_result=phase_result,
            )
            waiting = self._workflow_command_waiting_prerequisite(
                command_id,
                retry_delay_seconds=10,
                from_statuses=("running", "claimed"),
                result={
                    "run_id": run_id,
                    "batch_id": batch_id,
                    "workspace_id": workspace_id,
                    "run_status": run_status,
                    "worker_status": worker_status,
                    "reason": "crm_public_web_remote_search_not_ready",
                },
            )
            return {
                "status": "deferred",
                "reason": "crm_public_web_remote_search_not_ready",
                "run_id": run_id,
                "batch_id": batch_id,
                "run_status": run_status,
                "workflow_command": self._kernel._workflow_command_observation(waiting or latest_command, migration_phase=migration_phase),
            }
        if run_status in {"failed", "cancelled"}:
            final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
                activity=activity,
                attempt=attempt,
                status="failed",
                phase=run_status,
                output={
                    "batch_id": batch_id,
                    "run_id": run_id,
                    "workspace_id": workspace_id,
                    "run_status": run_status,
                    "worker_status": worker_status,
                    "phase_result": phase_result,
                },
                entity_counts={"crm_public_web_run_count": 1},
                error={"reason": f"crm_public_web_run_{run_status}"},
                metadata={"terminal_run_status": run_status},
                attempt_status="failed",
            )
            self._record_crm_public_web_phase_entity_delta(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                command_type=command_type,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
                run_status=run_status,
                worker_status=worker_status,
                delta_status="failed",
                reason=f"crm_public_web_run_{run_status}",
                phase_result=phase_result,
            )
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=f"crm_public_web_run_{run_status}",
                retryable=run_status in set(PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES),
                retry_delay_seconds=10,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest_command,
                actor=CRM_PUBLIC_WEB_PHASE_OWNER,
                source="crm_public_web.phase_owner",
            )
            return {
                "status": "failed",
                "reason": f"crm_public_web_run_{run_status}",
                "run_id": run_id,
                "batch_id": batch_id,
                "run_status": run_status,
                "workflow_command": self._kernel._workflow_command_observation(failed or latest_command, migration_phase=migration_phase),
            }
        next_command_type = self._next_crm_public_web_phase_command_type(run_status)
        next_command: dict[str, Any] = {}
        if next_command_type:
            next_command = self._plan_crm_public_web_run_phase_command(
                workflow_run_id=str(latest_command.get("workflow_run_id") or ""),
                operation_id=str(latest_command.get("operation_id") or ""),
                batch_id=batch_id,
                run_id=run_id,
                workspace_id=workspace_id,
                command_type=next_command_type,
                parent_command=latest_command,
                request_payload=dict(payload.get("request_payload") or {}),
                source="crm_public_web_phase_owner",
            )
        downstream_ids = [
            str(next_command.get("command_id") or "").strip()
        ] if str(next_command.get("command_id") or "").strip() else []
        phase_metrics = dict(dict(dict(phase_result or {}).get("summary") or {}).get("phase_metrics") or {})
        final_activity, final_attempt = self._kernel._finish_workflow_command_activity_attempt(
            activity=activity,
            attempt=attempt,
            status="succeeded",
            phase=run_status or migration_phase,
            output={
                "batch_id": batch_id,
                "run_id": run_id,
                "workspace_id": workspace_id,
                "run_status": run_status,
                "worker_status": worker_status,
                "phase_result": phase_result,
                "downstream_command_ids": downstream_ids,
                "next_command_type": next_command_type,
            },
            entity_counts={
                "crm_public_web_run_count": 1,
                **phase_metrics,
            },
            metadata={
                "terminal_run": not bool(next_command_type),
                "next_command_type": next_command_type,
            },
            attempt_status="succeeded",
        )
        phase_delta = self._record_crm_public_web_phase_entity_delta(
            command=latest_command,
            activity=final_activity or activity,
            attempt=final_attempt or attempt,
            command_type=command_type,
            run_id=run_id,
            batch_id=batch_id,
            workspace_id=workspace_id,
            run_status=run_status,
            worker_status=worker_status,
            delta_status="recorded",
            reason="crm_public_web_phase_completed",
            phase_result=phase_result,
            downstream_command_ids=downstream_ids,
        )
        signal_deltas: list[dict[str, Any]] = []
        person_asset_sync: dict[str, Any] = {}
        if command_type == CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE:
            signal_deltas = self._record_crm_public_web_signal_entity_deltas(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
            )
            person_asset_sync = self._sync_crm_public_web_signals_to_person_asset_layer(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
            )
        document_deltas: list[dict[str, Any]] = []
        if command_type == CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE:
            document_deltas = self._record_crm_public_web_document_entity_deltas(
                command=latest_command,
                activity=final_activity or activity,
                attempt=final_attempt or attempt,
                run_id=run_id,
                batch_id=batch_id,
                workspace_id=workspace_id,
            )
        succeeded = self.store.mark_workflow_command_succeeded(
            command_id,
            result={
                "run_id": run_id,
                "batch_id": batch_id,
                "workspace_id": workspace_id,
                "run_status": run_status,
                "worker_status": worker_status,
                "phase_result": phase_result,
                "activity_run_id": str((final_activity or activity).get("activity_run_id") or "").strip(),
                "activity_attempt_id": str((final_attempt or attempt).get("attempt_id") or "").strip(),
                "entity_delta_id": str(phase_delta.get("delta_id") or "").strip(),
                "signal_entity_delta_ids": [
                    str(delta.get("delta_id") or "").strip()
                    for delta in signal_deltas
                    if str(delta.get("delta_id") or "").strip()
                ],
                "signal_entity_delta_count": len(signal_deltas),
                "person_asset_sync": person_asset_sync,
                "person_asset_sync_asset_count": int(person_asset_sync.get("asset_count") or 0),
                "person_asset_sync_evidence_count": int(person_asset_sync.get("evidence_count") or 0),
                "person_asset_sync_entity_delta_count": int(person_asset_sync.get("entity_delta_count") or 0),
                "document_entity_delta_ids": [
                    str(delta.get("delta_id") or "").strip()
                    for delta in document_deltas
                    if str(delta.get("delta_id") or "").strip()
                ],
                "document_entity_delta_count": len(document_deltas),
                "downstream_command_ids": downstream_ids,
                "next_command_type": next_command_type,
                "terminal_run": not bool(next_command_type),
                "operation_completion_deferred": bool(
                    str(latest_command.get("operation_id") or "").strip() and next_command_type
                ),
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
            },
        )
        if not next_command_type:
            self._kernel._sync_operation_run_from_workflow_command(
                succeeded or latest_command,
                actor=CRM_PUBLIC_WEB_PHASE_OWNER,
                source="crm_public_web.phase_owner",
            )
        return {
            "status": "completed",
            "reason": "crm_public_web_phase_command_owner",
            "run_id": run_id,
            "batch_id": batch_id,
            "workspace_id": workspace_id,
            "run_status": run_status,
            "worker_status": worker_status,
            "next_command_type": next_command_type,
            "downstream_command_ids": downstream_ids,
            "command_count": 1,
            "executed_command_count": 1,
            "claimed_count": 1,
            "completed_count": 1,
            "failed_count": 0,
            "signal_entity_delta_count": len(signal_deltas),
            "person_asset_sync_asset_count": int(person_asset_sync.get("asset_count") or 0),
            "person_asset_sync_evidence_count": int(person_asset_sync.get("evidence_count") or 0),
            "person_asset_sync_entity_delta_count": int(person_asset_sync.get("entity_delta_count") or 0),
            "document_entity_delta_count": len(document_deltas),
            "workflow_command": self._kernel._workflow_command_observation(succeeded or latest_command, migration_phase=migration_phase),
            "legacy_bridge_used": False,
            "migration_phase": migration_phase,
        }

    def _drain_crm_public_web_phase_commands(
        self,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = dict(payload or {})
        workflow_run_id = str(payload.get("workflow_run_id") or "").strip()
        batch_id = str(payload.get("batch_id") or "").strip()
        if not workflow_run_id and batch_id:
            workflow_run_id = self._crm_public_web_workflow_run_id(batch_id)
        limit = max(1, _coerce_int(payload.get("command_limit") or payload.get("crm_public_web_phase_command_limit"), 20))
        ready_commands: list[dict[str, Any]] = []
        remaining = limit
        for command_type in CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES:
            if remaining <= 0:
                break
            commands = self.store.list_ready_workflow_commands(
                workflow_run_id=workflow_run_id,
                owner=CRM_PUBLIC_WEB_PHASE_OWNER,
                command_type=command_type,
                limit=remaining,
            )
            ready_commands.extend(dict(command) for command in commands)
            remaining = limit - len(ready_commands)
        results: list[dict[str, Any]] = []
        executed_command_count = 0
        claimed_count = 0
        completed_count = 0
        failed_count = 0
        deferred_count = 0
        lease_seconds = max(30, _env_int("CRM_PUBLIC_WEB_PHASE_COMMAND_LEASE_SECONDS", 300))

        def record_result(result: dict[str, Any]) -> None:
            nonlocal executed_command_count, claimed_count, completed_count, failed_count, deferred_count
            results.append(result)
            if str(result.get("status") or "") in {"completed", "deferred", "failed"}:
                executed_command_count += 1
            claimed_count += _coerce_int(result.get("claimed_count"), 0)
            completed_count += _coerce_int(result.get("completed_count"), 0)
            failed_count += _coerce_int(result.get("failed_count"), 0)
            if str(result.get("status") or "") == "deferred":
                deferred_count += 1

        adjudication_commands = [
            dict(command)
            for command in ready_commands
            if str(command.get("command_type") or "").strip() == CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE
        ]
        adjudication_command_ids = {
            str(command.get("command_id") or "").strip()
            for command in adjudication_commands
            if str(command.get("command_id") or "").strip()
        }
        max_adjudication_workers = max(
            1,
            min(
                len(adjudication_commands) or 1,
                _coerce_int(
                    payload.get("adjudication_command_concurrency")
                    or payload.get("crm_public_web_adjudication_command_concurrency")
                    or os.getenv("CRM_PUBLIC_WEB_ADJUDICATION_COMMAND_CONCURRENCY")
                    or 2,
                    2,
                ),
            ),
        )
        if adjudication_commands and max_adjudication_workers > 1:
            with ThreadPoolExecutor(
                max_workers=max_adjudication_workers,
                thread_name_prefix="crm-public-web-adjudicate",
            ) as executor:
                futures = [
                    executor.submit(
                        self._run_crm_public_web_phase_command,
                        command,
                        lease_seconds=lease_seconds,
                    )
                    for command in adjudication_commands
                ]
                for future in as_completed(futures):
                    record_result(future.result())

        for command in ready_commands:
            command_id = str(command.get("command_id") or "").strip()
            if command_id and command_id in adjudication_command_ids and max_adjudication_workers > 1:
                continue
            result = self._run_crm_public_web_phase_command(
                dict(command),
                lease_seconds=lease_seconds,
            )
            record_result(result)
        return {
            "status": "active" if executed_command_count > 0 or claimed_count > 0 else "idle",
            "reason": "crm_public_web_phase_command_owner" if ready_commands else "no_ready_crm_public_web_phase_commands",
            "workflow_run_id": workflow_run_id,
            "command_count": len(ready_commands),
            "executed_command_count": executed_command_count,
            "claimed_count": claimed_count,
            "completed_count": completed_count,
            "failed_count": failed_count,
            "deferred_count": deferred_count,
            "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
            "command_types": list(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES),
            "parallel_adjudication_command_count": len(adjudication_commands) if max_adjudication_workers > 1 else 0,
            "adjudication_command_concurrency": max_adjudication_workers,
            "legacy_bridge_used": False,
            "migration_phase": "W7f_crm_public_web_phase_commands",
            "items": results,
            "results": results,
        }

    @staticmethod
    def _crm_public_web_job_request(
        *,
        batch: dict[str, Any],
        request_payload: dict[str, Any],
    ) -> JobRequest:
        return JobRequest(
            raw_user_request="CRM Public Web Search",
            query="CRM Public Web Search",
            target_company="crm_records",
            target_scope="crm_public_web_search",
            analysis_stage_mode="single_stage",
            execution_preferences={
                "public_web_search": {
                    "batch_id": str(batch.get("batch_id") or ""),
                    "record_ids": list(
                        batch.get("requested_crm_record_ids")
                        or batch.get("requested_record_ids")
                        or request_payload.get("record_ids")
                        or []
                    ),
                    "default_workflow_stage": "not_enabled",
                    "execution_backend": CRM_PUBLIC_WEB_EXECUTION_BACKEND,
                }
            },
        )

    def export_target_candidate_public_web_archive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {
            **_legacy_target_public_web_orchestrator_disabled_result(
                operation="export",
                canonical_endpoint="/api/crm/records/public-web-export",
            ),
            "filename": "target-candidate-public-web-retired.json",
            "content_type": "application/json",
            "body": b"",
            "record_count": 0,
            "exported_record_count": 0,
            "exported_signal_count": 0,
            "exported_evidence_count": 0,
            "exported_promotion_count": 0,
            "no_public_web_result_count": 0,
            "no_exportable_signal_count": 0,
            "non_terminal_run_count": 0,
            "raw_asset_policy": {
                "raw_assets_included_by_default": False,
                "default_export_surface": "retired",
            },
        }
