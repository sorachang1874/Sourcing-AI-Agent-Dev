from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha1
from typing import Any

from .company_registry import normalize_company_key

ASSET_LIFECYCLE_STATES = {"draft", "partial", "canonical", "superseded", "archived", "empty"}
PROMOTABLE_ASSET_LIFECYCLE_STATES = {"canonical"}


def normalize_asset_lifecycle_status(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in ASSET_LIFECYCLE_STATES:
        return normalized
    return "draft"


def default_asset_pointer_key(
    *,
    company_key: str,
    scope_kind: str = "company",
    scope_key: str = "",
    asset_kind: str = "company_asset",
) -> str:
    normalized_company_key = normalize_company_key(company_key)
    normalized_scope_kind = str(scope_kind or "company").strip().lower() or "company"
    normalized_scope_key = str(scope_key or normalized_company_key).strip().lower() or normalized_company_key
    normalized_asset_kind = str(asset_kind or "company_asset").strip().lower() or "company_asset"
    return "/".join(
        [
            normalized_company_key,
            normalized_scope_kind,
            normalized_scope_key,
            normalized_asset_kind,
        ]
    )


def build_default_asset_pointer(
    *,
    company_key: str,
    snapshot_id: str,
    scope_kind: str = "company",
    scope_key: str = "",
    asset_kind: str = "company_asset",
    lifecycle_status: str = "canonical",
    coverage_proof: dict[str, Any] | None = None,
    previous_snapshot_id: str = "",
    promoted_by_job_id: str = "",
    promoted_at: str = "",
) -> dict[str, Any]:
    normalized_company_key = normalize_company_key(company_key)
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_status = normalize_asset_lifecycle_status(lifecycle_status)
    normalized_scope_kind = str(scope_kind or "company").strip().lower() or "company"
    normalized_scope_key = str(scope_key or normalized_company_key).strip().lower() or normalized_company_key
    normalized_asset_kind = str(asset_kind or "company_asset").strip().lower() or "company_asset"
    timestamp = str(promoted_at or "").strip() or datetime.now(timezone.utc).isoformat()
    return {
        "pointer_key": default_asset_pointer_key(
            company_key=normalized_company_key,
            scope_kind=normalized_scope_kind,
            scope_key=normalized_scope_key,
            asset_kind=normalized_asset_kind,
        ),
        "company_key": normalized_company_key,
        "scope_kind": normalized_scope_kind,
        "scope_key": normalized_scope_key,
        "asset_kind": normalized_asset_kind,
        "snapshot_id": normalized_snapshot_id,
        "lifecycle_status": normalized_status,
        "coverage_proof": dict(coverage_proof or {}),
        "previous_snapshot_id": str(previous_snapshot_id or "").strip(),
        "promoted_by_job_id": str(promoted_by_job_id or "").strip(),
        "promoted_at": timestamp,
    }


def normalize_default_asset_pointer_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    normalized = dict(payload or {})
    company_key = normalize_company_key(str(normalized.get("company_key") or normalized.get("target_company") or ""))
    scope_kind = str(normalized.get("scope_kind") or "company").strip().lower() or "company"
    scope_key = str(normalized.get("scope_key") or company_key).strip().lower() or company_key
    asset_kind = str(normalized.get("asset_kind") or "company_asset").strip().lower() or "company_asset"
    pointer_key = str(normalized.get("pointer_key") or "").strip() or default_asset_pointer_key(
        company_key=company_key,
        scope_kind=scope_kind,
        scope_key=scope_key,
        asset_kind=asset_kind,
    )
    return {
        "pointer_key": pointer_key,
        "company_key": company_key,
        "scope_kind": scope_kind,
        "scope_key": scope_key,
        "asset_kind": asset_kind,
        "snapshot_id": str(normalized.get("snapshot_id") or "").strip(),
        "lifecycle_status": normalize_asset_lifecycle_status(normalized.get("lifecycle_status") or "canonical"),
        "coverage_proof": dict(normalized.get("coverage_proof") or {}),
        "previous_snapshot_id": str(normalized.get("previous_snapshot_id") or "").strip(),
        "promoted_by_job_id": str(normalized.get("promoted_by_job_id") or "").strip(),
        "promoted_at": str(normalized.get("promoted_at") or "").strip(),
        "metadata": dict(normalized.get("metadata") or {}),
    }


def default_asset_pointer_history_id(
    *,
    pointer_key: str,
    snapshot_id: str,
    event_type: str,
    occurred_at: str,
) -> str:
    digest = sha1(
        "|".join(
            [
                str(pointer_key or "").strip(),
                str(snapshot_id or "").strip(),
                str(event_type or "").strip(),
                str(occurred_at or "").strip(),
            ]
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"asset-pointer-history::{digest}"


def build_canonical_asset_replacement_plan(
    *,
    current_pointer: dict[str, Any] | None,
    company_key: str,
    snapshot_id: str,
    scope_kind: str = "company",
    scope_key: str = "",
    asset_kind: str = "company_asset",
    lifecycle_status: str = "canonical",
    coverage_proof: dict[str, Any] | None = None,
    promoted_by_job_id: str = "",
    retain_latest_canonical_count: int = 12,
) -> dict[str, Any]:
    normalized_status = normalize_asset_lifecycle_status(lifecycle_status)
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_snapshot_id:
        return {
            "status": "rejected",
            "reason": "missing_snapshot_id",
        }
    pointer_key = default_asset_pointer_key(
        company_key=company_key,
        scope_kind=scope_kind,
        scope_key=scope_key,
        asset_kind=asset_kind,
    )
    if normalized_status not in PROMOTABLE_ASSET_LIFECYCLE_STATES:
        return {
            "status": "rejected",
            "reason": "lifecycle_status_not_promotable",
            "lifecycle_status": normalized_status,
            "pointer_key": pointer_key,
        }
    current = dict(current_pointer or {})
    current_snapshot_id = str(current.get("snapshot_id") or "").strip()
    if current_snapshot_id == normalized_snapshot_id and normalize_asset_lifecycle_status(
        current.get("lifecycle_status")
    ) == "canonical":
        return {
            "status": "noop",
            "reason": "snapshot_already_default",
            "default_pointer": current,
            "pointer_key": pointer_key,
        }
    default_pointer = build_default_asset_pointer(
        company_key=company_key,
        snapshot_id=normalized_snapshot_id,
        scope_kind=scope_kind,
        scope_key=scope_key,
        asset_kind=asset_kind,
        lifecycle_status="canonical",
        coverage_proof=coverage_proof,
        previous_snapshot_id=current_snapshot_id,
        promoted_by_job_id=promoted_by_job_id,
    )
    superseded_pointer: dict[str, Any] = {}
    if current_snapshot_id:
        superseded_pointer = {
            **current,
            "pointer_key": str(current.get("pointer_key") or pointer_key),
            "lifecycle_status": "superseded",
            "superseded_by_snapshot_id": normalized_snapshot_id,
            "default_pointer_replaced": True,
        }
    retain_snapshot_ids = [normalized_snapshot_id]
    if current_snapshot_id and current_snapshot_id not in retain_snapshot_ids:
        retain_snapshot_ids.append(current_snapshot_id)
    return {
        "status": "promoted",
        "pointer_key": pointer_key,
        "default_pointer": default_pointer,
        "superseded_pointer": superseded_pointer,
        "historical_retention": {
            "retain_latest_canonical_count": max(1, int(retain_latest_canonical_count or 1)),
            "retain_snapshot_ids": retain_snapshot_ids,
        },
    }
