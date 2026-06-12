from __future__ import annotations

from typing import Any

from .company_registry import normalize_company_key
from .storage import ControlPlaneStore

_REUSABLE_SHARD_LANES = {"company_employees", "profile_search"}
_REUSABLE_SHARD_STATUSES = {"completed", "completed_with_cap", "skipped_high_overlap"}


def normalize_authoritative_source_provenance(
    *,
    store: ControlPlaneStore,
    company: str,
    asset_view: str = "canonical_merged",
    apply: bool = False,
) -> dict[str, Any]:
    """Normalize selected source snapshots on an authoritative registry row.

    `selected_snapshot_ids` is consumed by planner/audit code as source provenance,
    so it should not keep arbitrary historical snapshots. The durable contract is:
    keep the serving snapshot, plus selected source snapshots that have reusable
    acquisition-shard registry proof. Older selected ids without shard proof are
    retained in metadata for auditability, but no longer masquerade as reusable
    planner inputs.
    """

    normalized_company = _normalize_text(company)
    normalized_asset_view = _normalize_text(asset_view) or "canonical_merged"
    if not normalized_company:
        raise ValueError("company is required")

    row = store.get_authoritative_organization_asset_registry(
        target_company=normalized_company,
        asset_view=normalized_asset_view,
    )
    if not row:
        return {
            "status": "blocked",
            "applied": False,
            "reason": "authoritative_registry_row_missing",
            "target_company": normalized_company,
            "asset_view": normalized_asset_view,
        }

    row_payload = dict(row)
    summary = dict(row_payload.get("summary") or {})
    selection = dict(row_payload.get("source_snapshot_selection") or summary.get("source_snapshot_selection") or {})
    serving_snapshot_id = _normalize_text(
        selection.get("serving_snapshot_id")
        or dict(selection.get("serving_generation_repair") or {}).get("repair_snapshot_id")
        or row_payload.get("snapshot_id")
    )
    selected_ids = _dedupe_strings(
        row_payload.get("selected_snapshot_ids")
        or selection.get("selected_snapshot_ids")
        or summary.get("selected_snapshot_ids")
        or [row_payload.get("snapshot_id")]
    )
    if serving_snapshot_id and serving_snapshot_id.lower() not in {item.lower() for item in selected_ids}:
        selected_ids = [serving_snapshot_id, *selected_ids]

    reusable_source_ids, shard_summary = _selected_ids_with_reusable_shards(
        store=store,
        target_company=normalized_company,
        selected_snapshot_ids=selected_ids,
    )
    serving_ids = _dedupe_strings([row_payload.get("snapshot_id"), serving_snapshot_id])
    serving_lookup = {item.lower() for item in serving_ids}
    reusable_lookup = {item.lower() for item in reusable_source_ids}
    kept_ids = _dedupe_strings([*serving_ids, *reusable_source_ids])
    dropped_ids = [
        snapshot_id
        for snapshot_id in selected_ids
        if snapshot_id.lower() not in serving_lookup and snapshot_id.lower() not in reusable_lookup
    ]

    previous_ids = _dedupe_strings(row_payload.get("selected_snapshot_ids") or selected_ids)
    changed = previous_ids != kept_ids or _normalize_text(selection.get("serving_snapshot_id")) != serving_snapshot_id
    result = {
        "status": "dry_run" if changed and not apply else ("applied" if changed else "no_change"),
        "applied": bool(apply and changed),
        "target_company": normalized_company,
        "asset_view": normalized_asset_view,
        "snapshot_id": _normalize_text(row_payload.get("snapshot_id")),
        "serving_snapshot_id": serving_snapshot_id,
        "previous_selected_snapshot_ids": previous_ids,
        "normalized_selected_snapshot_ids": kept_ids,
        "reusable_source_snapshot_ids": reusable_source_ids,
        "dropped_snapshot_ids_without_reusable_shard_rows": dropped_ids,
        "shard_summary": shard_summary,
    }
    if not changed or not apply:
        return result

    normalized_selection = {
        **selection,
        "source_snapshot_contract_version": max(_safe_int(selection.get("source_snapshot_contract_version")), 2),
        "serving_snapshot_id": serving_snapshot_id,
        "selected_snapshot_ids": kept_ids,
        "reusable_source_snapshot_ids": reusable_source_ids,
        "archived_source_snapshot_ids_without_shard_registry_rows": dropped_ids,
        "provenance_normalized_by": "normalize_authoritative_source_provenance",
    }
    normalized_summary = dict(summary)
    normalized_summary["selected_snapshot_ids"] = kept_ids
    normalized_summary["source_snapshot_selection"] = normalized_selection
    normalized_summary["source_snapshot_count"] = len(kept_ids)
    row_payload["source_snapshot_selection"] = normalized_selection
    row_payload["selected_snapshot_ids"] = kept_ids
    row_payload["source_snapshot_count"] = len(kept_ids)
    row_payload["summary"] = normalized_summary
    row_payload.setdefault("company_key", normalize_company_key(normalized_company))
    persisted = store.upsert_organization_asset_registry(row_payload, authoritative=True)
    result["persisted"] = {
        "registry_id": persisted.get("registry_id"),
        "snapshot_id": persisted.get("snapshot_id"),
        "selected_snapshot_ids": list(persisted.get("selected_snapshot_ids") or []),
    }
    return result


def _selected_ids_with_reusable_shards(
    *,
    store: ControlPlaneStore,
    target_company: str,
    selected_snapshot_ids: list[str],
) -> tuple[list[str], dict[str, Any]]:
    rows = store.list_acquisition_shard_registry(
        target_company=target_company,
        snapshot_ids=selected_snapshot_ids,
        statuses=sorted(_REUSABLE_SHARD_STATUSES),
        limit=max(1000, len(selected_snapshot_ids) * 100),
    )
    reusable_ids: list[str] = []
    seen: set[str] = set()
    row_counts_by_snapshot: dict[str, int] = {}
    query_counts_by_snapshot: dict[str, int] = {}
    query_seen_by_snapshot: dict[str, set[str]] = {}
    selected_lookup = {item.lower(): item for item in selected_snapshot_ids}
    for row in list(rows or []):
        lane = _normalize_text(row.get("lane")).lower()
        if lane not in _REUSABLE_SHARD_LANES:
            continue
        snapshot_id = _normalize_text(row.get("snapshot_id"))
        canonical_snapshot_id = selected_lookup.get(snapshot_id.lower())
        if not canonical_snapshot_id:
            continue
        row_counts_by_snapshot[canonical_snapshot_id] = row_counts_by_snapshot.get(canonical_snapshot_id, 0) + 1
        query = _normalize_text(row.get("search_query") or row.get("shard_title") or row.get("shard_id")).lower()
        if query:
            query_seen_by_snapshot.setdefault(canonical_snapshot_id, set()).add(query)
        if canonical_snapshot_id.lower() not in seen:
            seen.add(canonical_snapshot_id.lower())
            reusable_ids.append(canonical_snapshot_id)
    for snapshot_id, queries in query_seen_by_snapshot.items():
        query_counts_by_snapshot[snapshot_id] = len(queries)
    return reusable_ids, {
        "reusable_snapshot_count": len(reusable_ids),
        "row_counts_by_snapshot": dict(sorted(row_counts_by_snapshot.items())),
        "query_counts_by_snapshot": dict(sorted(query_counts_by_snapshot.items())),
    }


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _dedupe_strings(values: Any) -> list[str]:
    if isinstance(values, str):
        values = [values]
    result: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        text = _normalize_text(value)
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
