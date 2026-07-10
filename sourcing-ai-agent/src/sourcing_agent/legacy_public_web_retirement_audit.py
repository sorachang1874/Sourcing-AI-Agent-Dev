from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .legacy_public_web_storage import (
    list_legacy_target_public_web_batches,
    list_legacy_target_public_web_promotions,
    list_legacy_target_public_web_runs,
)

CONTRACT_VERSION = "legacy_public_web_retirement_audit_v1"


def audit_legacy_public_web_retirement(
    *,
    store: Any,
    workspace_id: str = "default",
    row_limit: int = 10000,
    sample_limit: int = 25,
) -> dict[str, Any]:
    """Build a read-only W7e pre-delete audit for retired Public Web state.

    The audit intentionally checks global legacy target-candidate Public Web
    rows before code/table deletion. Company-asset overview coverage is reported
    as context only; it is not a deletion blocker for legacy Public Web runtime.
    """

    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_row_limit = max(1, int(row_limit or 10000))
    normalized_sample_limit = max(1, int(sample_limit or 25))

    legacy_batches = _list_rows(
        list_legacy_target_public_web_batches,
        store,
        limit=normalized_row_limit,
    )
    legacy_runs = _list_rows(
        list_legacy_target_public_web_runs,
        store,
        limit=normalized_row_limit,
    )
    legacy_promotions = _list_rows(
        list_legacy_target_public_web_promotions,
        store,
        limit=normalized_row_limit,
    )

    crm_batches = _list_rows(
        store.list_crm_public_web_batches,
        workspace_id=normalized_workspace_id,
        limit=normalized_row_limit,
    )
    crm_runs = _list_rows(
        store.list_crm_public_web_runs,
        workspace_id=normalized_workspace_id,
        limit=normalized_row_limit,
    )
    crm_promotions = _list_rows(
        store.list_crm_public_web_promotions,
        workspace_id=normalized_workspace_id,
        limit=normalized_row_limit,
    )
    crm_records = _list_rows(
        store.list_crm_records,
        workspace_id=normalized_workspace_id,
        limit=normalized_row_limit,
    )
    collection_pointers = _list_rows(
        store.repos.serving_projection.list_authoritative_pointers,
        state="active",
        limit=normalized_row_limit,
    )

    legacy_count = len(legacy_batches) + len(legacy_runs) + len(legacy_promotions)
    legacy_limited = any(
        len(rows) >= normalized_row_limit
        for rows in (legacy_batches, legacy_runs, legacy_promotions)
    )
    deletion_blockers: list[dict[str, Any]] = []
    if legacy_count > 0:
        deletion_blockers.append(
            {
                "blocker": "legacy_target_candidate_public_web_rows_present",
                "severity": "blocking",
                "row_count": legacy_count,
                "required_action": (
                    "migrate valuable promotions/signals into CRM Public Web/PersonAssertion, "
                    "or write a reviewed cold-backup manifest before deleting legacy helpers/tables"
                ),
            }
        )
    if legacy_limited:
        deletion_blockers.append(
            {
                "blocker": "legacy_target_candidate_public_web_audit_limited",
                "severity": "blocking",
                "row_limit": normalized_row_limit,
                "required_action": "rerun the audit with a larger --row-limit before approving deletion",
            }
        )

    deletion_allowed = not deletion_blockers
    status = "ready_for_physical_deletion" if deletion_allowed else "blocked"

    return {
        "contract_version": CONTRACT_VERSION,
        "status": status,
        "read_only": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workspace_id": normalized_workspace_id,
        "row_limit": normalized_row_limit,
        "sample_limit": normalized_sample_limit,
        "deletion_allowed": deletion_allowed,
        "deletion_blockers": deletion_blockers,
        "summary": {
            "legacy_target_candidate_public_web_row_count": legacy_count,
            "legacy_target_candidate_public_web_batch_count": len(legacy_batches),
            "legacy_target_candidate_public_web_run_count": len(legacy_runs),
            "legacy_target_candidate_public_web_promotion_count": len(legacy_promotions),
            "legacy_target_candidate_public_web_limited": legacy_limited,
            "crm_public_web_batch_count": len(crm_batches),
            "crm_public_web_run_count": len(crm_runs),
            "crm_public_web_promotion_count": len(crm_promotions),
            "crm_record_count": len(crm_records),
            "collection_authoritative_pointer_count": len(collection_pointers),
            "company_asset_overview_is_deletion_blocker": False,
        },
        "legacy_target_candidate_public_web": {
            "batches": _surface_summary(
                legacy_batches,
                id_key="batch_id",
                sample_limit=normalized_sample_limit,
            ),
            "runs": _surface_summary(
                legacy_runs,
                id_key="run_id",
                sample_limit=normalized_sample_limit,
                extra_keys=("record_id", "candidate_name", "current_company", "linkedin_url_key", "batch_id"),
            ),
            "promotions": _surface_summary(
                legacy_promotions,
                id_key="promotion_id",
                sample_limit=normalized_sample_limit,
                extra_keys=("record_id", "run_id", "signal_id", "action", "signal_type", "normalized_value"),
            ),
        },
        "crm_public_web": {
            "batches": _surface_summary(
                crm_batches,
                id_key="batch_id",
                sample_limit=normalized_sample_limit,
            ),
            "runs": _surface_summary(
                crm_runs,
                id_key="run_id",
                sample_limit=normalized_sample_limit,
                extra_keys=("crm_record_id", "candidate_name", "current_company", "linkedin_url_key", "batch_id"),
            ),
            "promotions": _surface_summary(
                crm_promotions,
                id_key="promotion_id",
                sample_limit=normalized_sample_limit,
                extra_keys=("crm_record_id", "run_id", "signal_id", "action", "signal_type", "normalized_value"),
            ),
        },
        "crm_records": {
            "count": len(crm_records),
            "sample": [
                _pick_keys(row, ("crm_record_id", "person_identity_key", "display_name", "source_collection_id", "stage", "updated_at"))
                for row in crm_records[:normalized_sample_limit]
            ],
        },
        "collections": {
            "count": len(collection_pointers),
            "ids": [
                str(row.get("collection_id") or "").strip()
                for row in collection_pointers[:normalized_sample_limit]
                if str(row.get("collection_id") or "").strip()
            ],
            "sample": [
                _pick_keys(row, ("collection_id", "active_projection_id", "active_collection_version", "state", "updated_at"))
                for row in collection_pointers[:normalized_sample_limit]
            ],
            "contract_note": (
                "Collection authoritative pointer coverage controls the local asset Overview UX. "
                "It does not block legacy target-candidate Public Web runtime deletion."
            ),
        },
        "deletion_gate": {
            "normal_path_owner": "crm_public_web_v1",
            "legacy_owner": "target_candidate_public_web_v1",
            "legacy_rows_present": legacy_count > 0,
            "legacy_audit_limited": legacy_limited,
            "requires_migration_or_cold_backup": legacy_count > 0 or legacy_limited,
            "company_asset_overview_required_before_deletion": False,
        },
    }


def _list_rows(func: Any, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    rows = func(*args, **kwargs)
    return [dict(row) for row in list(rows or []) if isinstance(row, dict)]


def _surface_summary(
    rows: list[dict[str, Any]],
    *,
    id_key: str,
    sample_limit: int,
    extra_keys: tuple[str, ...] = (),
) -> dict[str, Any]:
    status_counts = Counter(str(row.get("status") or "").strip() or "unknown" for row in rows)
    company_counts = Counter(
        str(row.get("current_company") or "").strip()
        for row in rows
        if str(row.get("current_company") or "").strip()
    )
    keys = (id_key, "status", "phase", "created_at", "updated_at", *extra_keys)
    return {
        "count": len(rows),
        "status_counts": dict(sorted(status_counts.items())),
        "company_counts": dict(sorted(company_counts.items())),
        "sample": [_pick_keys(row, keys) for row in rows[:sample_limit]],
    }


def _pick_keys(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in keys
        if key in row and row.get(key) not in (None, "", [], {})
    }
