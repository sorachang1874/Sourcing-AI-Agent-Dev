from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .asset_logger import AssetLogger
from .domain import normalize_name_token
from .seed_discovery import SearchSeedSnapshot, normalize_search_seed_employment_scope


def dedupe_search_seed_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in list(entries or []):
        if not isinstance(entry, dict):
            continue
        merge_key = _search_seed_entry_merge_key(entry)
        if not merge_key or merge_key in seen:
            continue
        seen.add(merge_key)
        deduped.append(dict(entry))
    return deduped


def dedupe_search_seed_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in list(records or []):
        if not isinstance(record, dict):
            continue
        merge_key = json.dumps(record, ensure_ascii=False, sort_keys=True)
        if merge_key in seen:
            continue
        seen.add(merge_key)
        deduped.append(dict(record))
    return deduped


def infer_search_seed_summary_employment_scope(summary: dict[str, Any] | None) -> str:
    summary = dict(summary or {})
    hinted_scope = normalize_search_seed_employment_scope(
        summary.get("employment_scope") or summary.get("employment_status")
    )
    if hinted_scope != "all":
        return hinted_scope
    filter_hints = dict(
        summary.get("effective_filter_hints")
        or summary.get("filter_hints")
        or summary.get("requested_filter_hints")
        or {}
    )
    if list(filter_hints.get("past_companies") or []):
        return "former"
    if list(filter_hints.get("current_companies") or []):
        return "current"
    strategy_type = str(summary.get("strategy_type") or "").strip().lower()
    if strategy_type == "former_employee_search":
        return "former"
    if strategy_type == "scoped_search_roster":
        return "current"
    return "all"


def search_seed_lane_name_from_payload(payload: dict[str, Any] | None) -> str:
    payload = dict(payload or {})
    hinted_scope = normalize_search_seed_employment_scope(
        payload.get("employment_scope") or payload.get("employment_status")
    )
    if hinted_scope != "all":
        return hinted_scope
    effective_filter_hints = dict(payload.get("effective_filter_hints") or payload.get("filter_hints") or {})
    if list(effective_filter_hints.get("past_companies") or []):
        return "former"
    if list(effective_filter_hints.get("current_companies") or []):
        return "current"
    return "all"


def resolve_search_seed_lane_entries(snapshot: SearchSeedSnapshot) -> dict[str, list[dict[str, Any]]]:
    resolved: dict[str, list[dict[str, Any]]] = {
        str(lane).strip(): [dict(item) for item in list(entries or []) if isinstance(item, dict)]
        for lane, entries in dict(snapshot.lane_entries or {}).items()
        if str(lane).strip()
    }
    if resolved:
        return resolved
    inferred_lane = ""
    if len(dict(snapshot.lane_payloads or {})) == 1:
        inferred_lane = next(iter(dict(snapshot.lane_payloads or {}).keys()), "")
    elif snapshot.summary_payload:
        inferred_lane = search_seed_lane_name_from_payload(snapshot.summary_payload)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for entry in list(snapshot.entries or []):
        if not isinstance(entry, dict):
            continue
        lane = normalize_search_seed_employment_scope(entry.get("employment_status") or inferred_lane)
        grouped.setdefault(lane, []).append(dict(entry))
    if grouped:
        return grouped
    if inferred_lane:
        return {inferred_lane: [dict(item) for item in list(snapshot.entries or []) if isinstance(item, dict)]}
    return {}


def persist_search_seed_snapshot(snapshot: SearchSeedSnapshot) -> SearchSeedSnapshot:
    logger = AssetLogger(snapshot.snapshot_dir)
    summary_path = snapshot.summary_path
    entries_path = (
        snapshot.entries_path if isinstance(snapshot.entries_path, Path) else summary_path.parent / "entries.json"
    )
    summary_payload = dict(snapshot.summary_payload or {})
    lane_entries_map = resolve_search_seed_lane_entries(snapshot)
    lane_payloads = {
        str(lane).strip(): dict(payload or {})
        for lane, payload in dict(snapshot.lane_payloads or {}).items()
        if str(lane).strip()
    }
    if not lane_payloads and lane_entries_map:
        inferred_lane = next(iter(lane_entries_map.keys()), "all")
        lane_payloads[inferred_lane] = {
            "lane": "profile_search",
            "employment_scope": inferred_lane,
            "employment_status": inferred_lane,
            "strategy_type": str(summary_payload.get("strategy_type") or ""),
            "query_summaries": list(snapshot.query_summaries or []),
            "requested_filter_hints": dict(summary_payload.get("requested_filter_hints") or {}),
            "effective_filter_hints": dict(summary_payload.get("effective_filter_hints") or {}),
        }
    normalized_lane_payloads: dict[str, dict[str, Any]] = {}
    normalized_lane_entries: dict[str, list[dict[str, Any]]] = {}
    logger.write_json(
        entries_path,
        list(snapshot.entries or []),
        asset_type="search_seed_entries",
        source_kind="search_seed_discovery",
        is_raw_asset=False,
        model_safe=True,
    )
    lane_summaries: dict[str, dict[str, Any]] = {}
    for lane_name in sorted({*lane_entries_map.keys(), *lane_payloads.keys()}):
        lane_key = normalize_search_seed_employment_scope(lane_name)
        lane_dir = summary_path.parent / lane_key
        lane_summary_path = lane_dir / "summary.json"
        lane_entries_path = lane_dir / "entries.json"
        lane_entries = dedupe_search_seed_entries(
            list(lane_entries_map.get(lane_name) or lane_entries_map.get(lane_key) or [])
        )
        lane_payload = dict(lane_payloads.get(lane_name) or lane_payloads.get(lane_key) or {})
        lane_query_summaries = dedupe_search_seed_records(
            list(lane_payload.get("query_summaries") or [])
            or [
                dict(item)
                for item in list(snapshot.query_summaries or [])
                if normalize_search_seed_employment_scope(
                    dict(item).get("employment_scope") or dict(item).get("employment_status") or lane_key
                )
                == lane_key
            ]
        )
        if not lane_entries and lane_key == "all":
            lane_entries = dedupe_search_seed_entries(list(snapshot.entries or []))
        logger.write_json(
            lane_entries_path,
            lane_entries,
            asset_type="search_seed_entries",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        normalized_lane_payload = {
            **lane_payload,
            "snapshot_id": snapshot.snapshot_id,
            "target_company": snapshot.target_company,
            "company_identity": snapshot.company_identity.to_record(),
            "lane": str(lane_payload.get("lane") or "profile_search"),
            "employment_scope": lane_key,
            "employment_status": lane_key,
            "query_summaries": lane_query_summaries,
            "entry_count": len(lane_entries),
            "accounts_used": list(snapshot.accounts_used or []),
            "errors": list(snapshot.errors or []),
            "stop_reason": snapshot.stop_reason,
            "summary_path": str(lane_summary_path),
            "entries_path": str(lane_entries_path),
            "queued_query_count": sum(
                1 for item in lane_query_summaries if str(item.get("status") or "").strip().lower() == "queued"
            ),
        }
        logger.write_json(
            lane_summary_path,
            normalized_lane_payload,
            asset_type="search_seed_summary",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        normalized_lane_payloads[lane_key] = normalized_lane_payload
        normalized_lane_entries[lane_key] = lane_entries
        lane_summaries[lane_key] = {
            "lane": str(normalized_lane_payload.get("lane") or "profile_search"),
            "employment_scope": lane_key,
            "strategy_type": str(normalized_lane_payload.get("strategy_type") or ""),
            "summary_path": str(lane_summary_path),
            "entries_path": str(lane_entries_path),
            "entry_count": len(lane_entries),
            "query_count": len(lane_query_summaries),
        }

    merged_requested_filter_hints = merge_search_seed_filter_hints(
        summary_payload.get("requested_filter_hints"),
        *[payload.get("requested_filter_hints") for payload in normalized_lane_payloads.values()],
    )
    merged_effective_filter_hints = merge_search_seed_filter_hints(
        summary_payload.get("effective_filter_hints"),
        *[
            payload.get("effective_filter_hints") or payload.get("filter_hints")
            for payload in normalized_lane_payloads.values()
        ],
    )
    merged_query_summaries = dedupe_search_seed_records(
        [
            *list(snapshot.query_summaries or []),
            *[
                dict(item)
                for payload in normalized_lane_payloads.values()
                for item in list(payload.get("query_summaries") or [])
                if isinstance(item, dict)
            ],
        ]
    )
    summary_payload.update(
        {
            "snapshot_id": snapshot.snapshot_id,
            "target_company": snapshot.target_company,
            "company_identity": snapshot.company_identity.to_record(),
            "entry_count": len(snapshot.entries),
            "query_summaries": merged_query_summaries,
            "accounts_used": list(snapshot.accounts_used or []),
            "errors": list(snapshot.errors or []),
            "stop_reason": snapshot.stop_reason,
            "queued_query_count": sum(
                1 for item in merged_query_summaries if str(item.get("status") or "").strip().lower() == "queued"
            ),
            "lane_summaries": lane_summaries,
        }
    )
    if merged_requested_filter_hints:
        summary_payload["requested_filter_hints"] = merged_requested_filter_hints
    if merged_effective_filter_hints:
        summary_payload["effective_filter_hints"] = merged_effective_filter_hints
    logger.write_json(
        summary_path,
        summary_payload,
        asset_type="search_seed_summary",
        source_kind="search_seed_discovery",
        is_raw_asset=False,
        model_safe=True,
    )
    return SearchSeedSnapshot(
        snapshot_id=snapshot.snapshot_id,
        target_company=snapshot.target_company,
        company_identity=snapshot.company_identity,
        snapshot_dir=snapshot.snapshot_dir,
        entries=list(snapshot.entries or []),
        query_summaries=list(snapshot.query_summaries or []),
        accounts_used=list(snapshot.accounts_used or []),
        errors=list(snapshot.errors or []),
        stop_reason=snapshot.stop_reason,
        summary_path=summary_path,
        entries_path=entries_path,
        summary_payload=summary_payload,
        lane_payloads=normalized_lane_payloads,
        lane_entries=normalized_lane_entries,
    )


def merge_search_seed_snapshots(
    existing: SearchSeedSnapshot | None,
    incoming: SearchSeedSnapshot | None,
) -> SearchSeedSnapshot | None:
    if not isinstance(existing, SearchSeedSnapshot):
        if isinstance(incoming, SearchSeedSnapshot):
            return persist_search_seed_snapshot(incoming)
        return None
    if not isinstance(incoming, SearchSeedSnapshot):
        return persist_search_seed_snapshot(existing)

    merged = SearchSeedSnapshot(
        snapshot_id=incoming.snapshot_id or existing.snapshot_id,
        target_company=incoming.target_company or existing.target_company,
        company_identity=incoming.company_identity,
        snapshot_dir=incoming.snapshot_dir,
        entries=dedupe_search_seed_entries([*list(existing.entries or []), *list(incoming.entries or [])]),
        query_summaries=dedupe_search_seed_records(
            [*list(existing.query_summaries or []), *list(incoming.query_summaries or [])]
        ),
        accounts_used=list(dict.fromkeys([*list(existing.accounts_used or []), *list(incoming.accounts_used or [])])),
        errors=list(dict.fromkeys([*list(existing.errors or []), *list(incoming.errors or [])])),
        stop_reason=str(incoming.stop_reason or existing.stop_reason or "").strip(),
        summary_path=incoming.summary_path,
        entries_path=incoming.entries_path if isinstance(incoming.entries_path, Path) else existing.entries_path,
        summary_payload={
            **dict(existing.summary_payload or {}),
            **dict(incoming.summary_payload or {}),
        },
        lane_payloads={
            lane: _merge_search_seed_lane_payload(
                existing_payload=dict(existing.lane_payloads or {}).get(lane),
                incoming_payload=dict(incoming.lane_payloads or {}).get(lane),
                merged_entries=dedupe_search_seed_entries(
                    [
                        *list(dict(existing.lane_entries or {}).get(lane) or []),
                        *list(dict(incoming.lane_entries or {}).get(lane) or []),
                    ]
                ),
                accounts_used=list(
                    dict.fromkeys([*list(existing.accounts_used or []), *list(incoming.accounts_used or [])])
                ),
                errors=list(dict.fromkeys([*list(existing.errors or []), *list(incoming.errors or [])])),
                stop_reason=str(incoming.stop_reason or existing.stop_reason or "").strip(),
            )
            for lane in sorted(
                {
                    *dict(existing.lane_payloads or {}).keys(),
                    *dict(incoming.lane_payloads or {}).keys(),
                    *resolve_search_seed_lane_entries(existing).keys(),
                    *resolve_search_seed_lane_entries(incoming).keys(),
                }
            )
        },
        lane_entries={
            lane: dedupe_search_seed_entries(
                [
                    *list(resolve_search_seed_lane_entries(existing).get(lane) or []),
                    *list(resolve_search_seed_lane_entries(incoming).get(lane) or []),
                ]
            )
            for lane in sorted(
                {
                    *resolve_search_seed_lane_entries(existing).keys(),
                    *resolve_search_seed_lane_entries(incoming).keys(),
                    *dict(existing.lane_payloads or {}).keys(),
                    *dict(incoming.lane_payloads or {}).keys(),
                }
            )
        },
    )
    return persist_search_seed_snapshot(merged)


def load_search_seed_lane_summaries(
    snapshot_dir: Path,
    *,
    auto_backfill: bool = True,
) -> list[dict[str, Any]]:
    discovery_dir = snapshot_dir / "search_seed_discovery"
    lane_summaries = _load_materialized_lane_summaries(discovery_dir)
    if lane_summaries:
        return lane_summaries
    if auto_backfill:
        backfill_search_seed_lane_assets(snapshot_dir)
        lane_summaries = _load_materialized_lane_summaries(discovery_dir)
        if lane_summaries:
            return lane_summaries
    aggregate_summary_payload = _load_json(discovery_dir / "summary.json")
    if not isinstance(aggregate_summary_payload, dict) or not aggregate_summary_payload:
        return []
    return _build_lane_payloads_from_aggregate_summary(aggregate_summary_payload)


def backfill_search_seed_lane_assets(snapshot_dir: Path) -> dict[str, Any]:
    discovery_dir = snapshot_dir / "search_seed_discovery"
    aggregate_summary_path = discovery_dir / "summary.json"
    aggregate_entries_path = discovery_dir / "entries.json"
    existing_lane_summaries = _load_materialized_lane_summaries(discovery_dir)
    if existing_lane_summaries:
        return {
            "status": "already_materialized",
            "snapshot_dir": str(snapshot_dir),
            "lane_keys": [
                str(item.get("employment_scope") or infer_search_seed_summary_employment_scope(item))
                for item in existing_lane_summaries
            ],
            "lane_count": len(existing_lane_summaries),
        }
    aggregate_summary_payload = _load_json(aggregate_summary_path)
    if not isinstance(aggregate_summary_payload, dict) or not aggregate_summary_payload:
        return {
            "status": "missing_aggregate_summary",
            "snapshot_dir": str(snapshot_dir),
            "lane_keys": [],
            "lane_count": 0,
        }
    aggregate_summary: dict[str, Any] = dict(aggregate_summary_payload)
    lane_payloads = _build_lane_payloads_from_aggregate_summary(aggregate_summary)
    if not lane_payloads:
        return {
            "status": "aggregate_summary_empty",
            "snapshot_dir": str(snapshot_dir),
            "lane_keys": [],
            "lane_count": 0,
        }
    aggregate_entries_payload = _load_json(aggregate_entries_path)
    entry_records = (
        [dict(item) for item in aggregate_entries_payload if isinstance(item, dict)]
        if isinstance(aggregate_entries_payload, list)
        else []
    )
    lane_keys = [
        normalize_search_seed_employment_scope(
            lane_payload.get("employment_scope") or lane_payload.get("employment_status")
        )
        for lane_payload in lane_payloads
    ]
    lane_entry_map = _partition_aggregate_entries_by_lane(
        entry_records,
        lane_keys=lane_keys,
        default_lane=infer_search_seed_summary_employment_scope(aggregate_summary),
    )
    lane_summaries: dict[str, dict[str, Any]] = {}
    logger = AssetLogger(snapshot_dir)
    for lane_payload in lane_payloads:
        lane_key = normalize_search_seed_employment_scope(
            lane_payload.get("employment_scope") or lane_payload.get("employment_status")
        )
        lane_dir = discovery_dir / lane_key
        lane_summary_path = lane_dir / "summary.json"
        lane_entries_path = lane_dir / "entries.json"
        lane_entries = dedupe_search_seed_entries(list(lane_entry_map.get(lane_key) or []))
        normalized_payload = {
            **lane_payload,
            "employment_scope": lane_key,
            "employment_status": lane_key,
            "summary_path": str(lane_summary_path),
            "entries_path": str(lane_entries_path),
            "entry_count": len(lane_entries),
            "query_summaries": dedupe_search_seed_records(list(lane_payload.get("query_summaries") or [])),
        }
        logger.write_json(
            lane_entries_path,
            lane_entries,
            asset_type="search_seed_entries",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        logger.write_json(
            lane_summary_path,
            normalized_payload,
            asset_type="search_seed_summary",
            source_kind="search_seed_discovery",
            is_raw_asset=False,
            model_safe=True,
        )
        lane_summaries[lane_key] = {
            "lane": str(normalized_payload.get("lane") or "profile_search"),
            "employment_scope": lane_key,
            "strategy_type": str(normalized_payload.get("strategy_type") or ""),
            "summary_path": str(lane_summary_path),
            "entries_path": str(lane_entries_path),
            "entry_count": len(lane_entries),
            "query_count": len(list(normalized_payload.get("query_summaries") or [])),
            "backfilled_from_aggregate": True,
        }
    aggregate_summary["lane_summaries"] = lane_summaries
    logger.write_json(
        aggregate_summary_path,
        aggregate_summary,
        asset_type="search_seed_summary",
        source_kind="search_seed_discovery",
        is_raw_asset=False,
        model_safe=True,
    )
    return {
        "status": "backfilled",
        "snapshot_dir": str(snapshot_dir),
        "lane_keys": sorted(lane_summaries.keys()),
        "lane_count": len(lane_summaries),
        "entry_counts": {
            lane_key: int(summary.get("entry_count") or 0) for lane_key, summary in lane_summaries.items()
        },
    }


def merge_search_seed_filter_hints(*payloads: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for payload in payloads:
        for key, value in dict(payload or {}).items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            if isinstance(value, list):
                existing = list(merged.get(normalized_key) or [])
                merged[normalized_key] = list(dict.fromkeys([*existing, *list(value)]))
            elif normalized_key not in merged or merged.get(normalized_key) in (None, "", {}):
                merged[normalized_key] = value
    return merged


def _build_lane_payloads_from_aggregate_summary(aggregate_summary: dict[str, Any]) -> list[dict[str, Any]]:
    aggregate_summary = dict(aggregate_summary or {})
    query_summaries = [
        dict(item) for item in list(aggregate_summary.get("query_summaries") or []) if isinstance(item, dict)
    ]
    if not query_summaries:
        return [aggregate_summary] if aggregate_summary else []
    grouped: dict[str, dict[str, Any]] = {}
    top_level_scope = infer_search_seed_summary_employment_scope(aggregate_summary)
    top_level_strategy = str(aggregate_summary.get("strategy_type") or "").strip()
    top_level_filter_hints = dict(
        aggregate_summary.get("effective_filter_hints")
        or aggregate_summary.get("filter_hints")
        or aggregate_summary.get("requested_filter_hints")
        or {}
    )
    for item in query_summaries:
        employment_scope = normalize_search_seed_employment_scope(
            item.get("employment_scope") or item.get("employment_status") or top_level_scope
        )
        group = grouped.setdefault(
            employment_scope,
            {
                **aggregate_summary,
                "employment_scope": employment_scope,
                "employment_status": employment_scope,
                "strategy_type": top_level_strategy
                or ("former_employee_search" if employment_scope == "former" else "scoped_search_roster"),
                "query_summaries": [],
            },
        )
        item_filter_hints = dict(item.get("filter_hints") or top_level_filter_hints)
        group["query_summaries"].append(
            {
                **dict(item),
                "employment_scope": employment_scope,
                "employment_status": employment_scope,
                "strategy_type": str(
                    item.get("strategy_type")
                    or group.get("strategy_type")
                    or ("former_employee_search" if employment_scope == "former" else "scoped_search_roster")
                ),
                "filter_hints": item_filter_hints,
            }
        )
        if item_filter_hints and not dict(group.get("effective_filter_hints") or {}):
            group["effective_filter_hints"] = item_filter_hints
    return list(grouped.values()) or [aggregate_summary]


def _load_materialized_lane_summaries(discovery_dir: Path) -> list[dict[str, Any]]:
    lane_summaries: list[dict[str, Any]] = []
    for lane_summary_path in sorted(discovery_dir.glob("*/summary.json")):
        if lane_summary_path.parent == discovery_dir:
            continue
        lane_summary = _load_json(lane_summary_path)
        if isinstance(lane_summary, dict) and lane_summary:
            lane_summaries.append(lane_summary)
    return lane_summaries


def _load_json(path: Path) -> dict[str, Any] | list[Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(payload, (dict, list)):
        return payload
    return {}


def _merge_search_seed_lane_payload(
    *,
    existing_payload: dict[str, Any] | None,
    incoming_payload: dict[str, Any] | None,
    merged_entries: list[dict[str, Any]],
    accounts_used: list[str],
    errors: list[str],
    stop_reason: str,
) -> dict[str, Any]:
    existing_payload = dict(existing_payload or {})
    incoming_payload = dict(incoming_payload or {})
    query_summaries = dedupe_search_seed_records(
        [
            *list(existing_payload.get("query_summaries") or []),
            *list(incoming_payload.get("query_summaries") or []),
        ]
    )
    requested_filter_hints = merge_search_seed_filter_hints(
        existing_payload.get("requested_filter_hints"),
        incoming_payload.get("requested_filter_hints"),
    )
    effective_filter_hints = merge_search_seed_filter_hints(
        existing_payload.get("effective_filter_hints"),
        incoming_payload.get("effective_filter_hints"),
        existing_payload.get("filter_hints"),
        incoming_payload.get("filter_hints"),
    )
    merged_payload = {
        **existing_payload,
        **incoming_payload,
        "query_summaries": query_summaries,
        "accounts_used": list(accounts_used),
        "errors": list(errors),
        "stop_reason": stop_reason,
        "entry_count": len(merged_entries),
    }
    if requested_filter_hints:
        merged_payload["requested_filter_hints"] = requested_filter_hints
    if effective_filter_hints:
        merged_payload["effective_filter_hints"] = effective_filter_hints
        merged_payload["filter_hints"] = effective_filter_hints
    merged_payload["queued_query_count"] = sum(
        1 for item in query_summaries if str(item.get("status") or "").strip().lower() == "queued"
    )
    return merged_payload


def _partition_aggregate_entries_by_lane(
    entries: list[dict[str, Any]],
    *,
    lane_keys: list[str],
    default_lane: str,
) -> dict[str, list[dict[str, Any]]]:
    normalized_lanes = [
        lane_key
        for lane_key in [normalize_search_seed_employment_scope(item) for item in list(lane_keys or [])]
        if lane_key
    ]
    unique_lane_keys = list(dict.fromkeys(normalized_lanes or [default_lane]))
    if len(unique_lane_keys) == 1:
        lane_key = unique_lane_keys[0]
        return {lane_key: dedupe_search_seed_entries(entries)}
    grouped: dict[str, list[dict[str, Any]]] = {lane_key: [] for lane_key in unique_lane_keys}
    fallback_lane = normalize_search_seed_employment_scope(default_lane)
    for entry in list(entries or []):
        if not isinstance(entry, dict):
            continue
        lane_key = normalize_search_seed_employment_scope(entry.get("employment_status") or fallback_lane)
        if lane_key not in grouped:
            lane_key = fallback_lane if fallback_lane in grouped else unique_lane_keys[0]
        grouped.setdefault(lane_key, []).append(dict(entry))
    return {
        lane_key: dedupe_search_seed_entries(lane_entries) for lane_key, lane_entries in grouped.items()
    }


def _search_seed_entry_merge_key(entry: dict[str, Any]) -> str:
    seed_key = str(entry.get("seed_key") or "").strip()
    if seed_key:
        return seed_key
    full_name = normalize_name_token(str(entry.get("full_name") or ""))
    profile_url = (
        str(entry.get("profile_url") or entry.get("linkedin_url") or entry.get("source_query") or "").strip().lower()
    )
    return "|".join([full_name, profile_url])
