"""Track B ② — linkedin_profile_registry domain repository (pilot domain, full surface).

This module is the PUBLIC API for the linkedin profile registry domain (owner-ratified 2026-06-21:
repositories replace the ``ControlPlaneStore`` facade; callers migrate directly). It owns:

- The typed ``TableDescriptor``s for the domain's tables. The registry descriptor is the single
  declarative source of truth for the row<->dict mapping: it replaced the hand-written
  ``_linkedin_profile_registry_from_row`` (50 lines of per-field coercion + JSON parsing) and the parallel
  ``_linkedin_profile_registry_row_payload`` column builder, proven byte-equivalent. The
  ``source_shards_json`` / ``source_jobs_json`` TEXT columns map to the public ``source_shards`` /
  ``source_jobs`` string lists; the roadmap-③ schema migration flips these to ``jsonb`` by changing only
  the column ``Kind`` here.
- ``LinkedinProfileRegistryRepository`` — every read/write method of the domain, ported 1:1 from the
  retired ``ControlPlaneStore`` methods (②.0). Bodies are verbatim ports: authority guards, fail-closed
  raises, and row-absent sentinels (``None`` / ``{}`` / ``[]`` / ``0``) are preserved exactly.

Conflict policy is REPLACE_ALL: the registry computes its merged row in Python
(``_compose_effective_payload``) before writing, so the adapter's plain
``ON CONFLICT (profile_url_key) DO UPDATE SET <all-non-pk> = excluded`` upsert is correct. All writes
route through the adapter primitives (``upsert_row`` / ``bulk_upsert_rows`` /
``insert_row_with_generated_id`` / ``delete_rows``) — no literal SQL in this package (the pg-onconflict
guard's literal-SQL scan does not cover ``repositories/``).

The events and aliases tables are intentionally read as raw adapter dict rows (no descriptor) — that is
the pre-migration behavior; converting them is deferred to the jsonb round (③).
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from hashlib import sha1
from typing import Any

from ..control_plane_repository import Column, Kind, Repository, TableDescriptor
from ..control_plane_time import is_sqlite_timestamp_expired, utc_now_timestamp
from ..linkedin_url_normalization import (
    normalize_linkedin_profile_url_key,
    normalize_linkedin_profile_url_list,
)

LINKEDIN_PROFILE_REGISTRY = TableDescriptor(
    table="linkedin_profile_registry",
    pk=("profile_url_key",),
    columns=(
        Column("profile_url_key"),
        Column("profile_url"),
        Column("raw_linkedin_url"),
        Column("sanity_linkedin_url"),
        Column("status", default="queued"),
        Column("retry_count", Kind.INT),
        Column("last_error"),
        Column("last_run_id"),
        Column("last_dataset_id"),
        Column("last_snapshot_dir"),
        Column("last_raw_path"),
        Column("first_queued_at"),
        Column("last_queued_at"),
        Column("last_fetched_at"),
        Column("last_failed_at"),
        Column("source_shards_json", Kind.JSON_STR_LIST, field="source_shards"),
        Column("source_jobs_json", Kind.JSON_STR_LIST, field="source_jobs"),
        Column("refill_queue_state"),
        Column("last_refill_trigger_kind"),
        Column("last_refill_plan_reason"),
        Column("last_refill_deferred_reason"),
        Column("last_refill_planned_at"),
        Column("refill_not_before_at"),
        Column("refill_plan_batch_size", Kind.INT),
        Column("refill_plan_batch_count", Kind.INT),
        Column("refill_plan_window_url_count", Kind.INT),
        Column("last_refill_attempt_count", Kind.INT),
        Column("refill_owner_worker_id", Kind.INT),
        Column("refill_owner_run_id"),
        Column("refill_owner_dataset_id"),
        Column("refill_owner_payload_hash"),
        Column("refill_terminal_status"),
        Column("refill_terminal_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


LINKEDIN_PROFILE_REGISTRY_LEASES = TableDescriptor(
    table="linkedin_profile_registry_leases",
    pk=("profile_url_key",),
    columns=(
        Column("profile_url_key"),
        Column("lease_owner"),
        Column("lease_token"),
        Column("lease_expires_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
    derived=(
        # `expired` is computed from lease_expires_at, not a stored column — mirrors the former lease mapper.
        ("expired", lambda mapped: is_sqlite_timestamp_expired(str(mapped.get("lease_expires_at") or ""))),
    ),
)


LINKEDIN_PROFILE_REGISTRY_BACKFILL_RUNS = TableDescriptor(
    table="linkedin_profile_registry_backfill_runs",
    pk=("run_key",),
    columns=(
        Column("run_key"),
        Column("scope_company"),
        Column("scope_snapshot_id"),
        Column("checkpoint_json", Kind.JSON, field="checkpoint"),
        Column("summary_json", Kind.JSON, field="summary"),
        Column("status", default="running"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


def linkedin_profile_max_retry_attempts(default: int = 1) -> int:
    """Number of retry submissions allowed after the initial profile fetch failure."""

    raw = str(os.getenv("SOURCING_LINKEDIN_PROFILE_MAX_RETRY_ATTEMPTS") or "").strip()
    if not raw:
        return max(0, default)
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return max(0, default)


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in list(values or []):
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _normalize_registry_label_list(values: list[str] | None) -> list[str]:
    return _dedupe_preserve_order([str(item or "").strip() for item in list(values or []) if str(item or "").strip()])


def _merge_registry_label_lists(existing: list[str], incoming: list[str]) -> list[str]:
    return _dedupe_preserve_order([*list(existing or []), *list(incoming or [])])


def _percentile(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    sorted_values = sorted(max(0, int(value)) for value in values)
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (max(0.0, min(100.0, float(percentile))) / 100.0) * (len(sorted_values) - 1)
    lower = int(rank)
    upper = min(len(sorted_values) - 1, lower + 1)
    if lower == upper:
        return sorted_values[lower]
    weight = rank - lower
    interpolated = (sorted_values[lower] * (1.0 - weight)) + (sorted_values[upper] * weight)
    return int(round(interpolated))


def _summarize_registry_rows_for_scope(
    rows: list[dict[str, Any]],
    *,
    source_job: str,
    snapshot_dir: str,
) -> dict[str, Any]:
    normalized_source_job = str(source_job or "").strip()
    normalized_snapshot_dir = str(snapshot_dir or "").strip()
    requested_count = 0
    fetched_count = 0
    fetched_missing_raw_path_count = 0
    unrecoverable_count = 0
    open_count = 0
    terminal_queue_state_leak_count = 0
    open_state_counts: dict[str, int] = {}
    for row in list(rows or []):
        payload = dict(row or {})
        if normalized_snapshot_dir and str(payload.get("last_snapshot_dir") or "").strip() != normalized_snapshot_dir:
            continue
        source_jobs = {
            str(item or "").strip()
            for item in list(payload.get("source_jobs") or [])
            if str(item or "").strip()
        }
        if normalized_source_job and normalized_source_job not in source_jobs:
            continue
        requested_count += 1
        status_value = str(payload.get("status") or "").strip().lower()
        refill_state = str(payload.get("refill_queue_state") or "").strip().lower()
        if refill_state and status_value in {"fetched", "unrecoverable"}:
            terminal_queue_state_leak_count += 1
        if status_value == "fetched":
            if str(payload.get("last_raw_path") or "").strip():
                fetched_count += 1
            else:
                fetched_missing_raw_path_count += 1
                open_count += 1
                open_state_counts["fetched_missing_raw_path"] = (
                    int(open_state_counts.get("fetched_missing_raw_path") or 0) + 1
                )
            continue
        if status_value == "unrecoverable":
            unrecoverable_count += 1
            continue
        open_count += 1
        state_key = refill_state or status_value or "unknown"
        open_state_counts[state_key] = int(open_state_counts.get(state_key) or 0) + 1
    terminal_count = fetched_count + unrecoverable_count
    all_requested_terminal = (
        requested_count > 0
        and terminal_count == requested_count
        and open_count == 0
        and terminal_queue_state_leak_count == 0
    )
    return {
        "requested_url_count": requested_count,
        "terminal_url_count": terminal_count,
        "fetched_url_count": fetched_count,
        "fetched_missing_raw_path_count": fetched_missing_raw_path_count,
        "unrecoverable_url_count": unrecoverable_count,
        "open_url_count": open_count,
        "open_state_counts": open_state_counts,
        "terminal_queue_state_leak_count": terminal_queue_state_leak_count,
        "all_requested_terminal": all_requested_terminal,
        "source_job": normalized_source_job,
        "snapshot_dir": normalized_snapshot_dir,
    }


def _normalize_backfill_entry(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    normalized = dict(payload or {})
    profile_url = str(normalized.get("profile_url") or "").strip()
    profile_url_key = normalize_linkedin_profile_url_key(profile_url)
    raw_linkedin_url = str(normalized.get("raw_linkedin_url") or "").strip()
    sanity_linkedin_url = str(normalized.get("sanity_linkedin_url") or "").strip()
    alias_urls = normalize_linkedin_profile_url_list(
        [
            *list(normalized.get("alias_urls") or []),
            profile_url,
            raw_linkedin_url,
            sanity_linkedin_url,
        ]
    )
    alias_keys = _dedupe_preserve_order(
        [normalize_linkedin_profile_url_key(alias_url) for alias_url in alias_urls if str(alias_url or "").strip()]
    )
    normalized_profile_key = profile_url_key or (alias_keys[0] if alias_keys else "")
    normalized_status = str(normalized.get("status") or "").strip()
    if normalized_status not in {"fetched", "failed_retryable", "unrecoverable"}:
        normalized_status = "failed_retryable"
    if not normalized_profile_key:
        return None
    retryable = bool(normalized.get("retryable")) and normalized_status not in {"fetched", "unrecoverable"}
    return {
        "profile_url": profile_url or normalized_profile_key,
        "profile_url_key": normalized_profile_key,
        "status": normalized_status,
        "error": str(normalized.get("error") or "").strip(),
        "retryable": retryable,
        "raw_path": str(normalized.get("raw_path") or "").strip(),
        "source_shards": _normalize_registry_label_list(list(normalized.get("source_shards") or [])),
        "source_jobs": _normalize_registry_label_list(list(normalized.get("source_jobs") or [])),
        "alias_urls": alias_urls,
        "alias_keys": alias_keys,
        "raw_linkedin_url": raw_linkedin_url,
        "sanity_linkedin_url": sanity_linkedin_url,
        "snapshot_dir": str(normalized.get("snapshot_dir") or "").strip(),
        "run_id": str(normalized.get("run_id") or normalized.get("last_run_id") or "").strip(),
        "dataset_id": str(normalized.get("dataset_id") or normalized.get("last_dataset_id") or "").strip(),
    }


class LinkedinProfileRegistryRepository(Repository):
    """Typed repository for the linkedin profile registry domain (full ②.0 surface)."""

    descriptor = LINKEDIN_PROFILE_REGISTRY

    # --- row mappers (descriptor-backed) ---

    def _registry_from_row(self, row: Any) -> dict[str, Any]:
        return LINKEDIN_PROFILE_REGISTRY.from_row(row)

    def _lease_from_row(self, row: Any) -> dict[str, Any]:
        return LINKEDIN_PROFILE_REGISTRY_LEASES.from_row(row)

    def _backfill_from_row(self, row: Any) -> dict[str, Any]:
        return LINKEDIN_PROFILE_REGISTRY_BACKFILL_RUNS.from_row(row)

    # --- alias resolution ---

    def _resolve_key(self, profile_url_key: str) -> str:
        normalized_key = str(profile_url_key or "").strip()
        if not normalized_key:
            return ""
        if self._should_prefer_read("linkedin_profile_registry_aliases"):
            current_key = normalized_key
            seen: set[str] = set()
            while current_key and current_key not in seen:
                seen.add(current_key)
                try:
                    row = self._adapter.select_one(
                        "linkedin_profile_registry_aliases",
                        where_sql="alias_url_key = %s",
                        params=[current_key],
                    )
                except Exception:
                    row = None
                if row is None:
                    break
                mapped_key = str(dict(row).get("profile_url_key") or "").strip()
                if not mapped_key or mapped_key == current_key:
                    break
                current_key = mapped_key
            return current_key or normalized_key
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_aliases", method_name="_resolve_key"
        )
        return normalized_key  # unreachable — the invariant raise above never returns

    def _resolve_keys_bulk(
        self,
        profile_url_keys: list[str] | tuple[str, ...],
    ) -> dict[str, str]:
        normalized_keys = _dedupe_preserve_order(
            [str(profile_url_key or "").strip() for profile_url_key in list(profile_url_keys or [])]
        )
        normalized_keys = [key for key in normalized_keys if key]
        if not normalized_keys:
            return {}
        if self._should_prefer_read("linkedin_profile_registry_aliases"):
            resolved_by_original = {key: key for key in normalized_keys}
            unresolved_keys = list(normalized_keys)
            seen_alias_keys: set[str] = set()
            # Preserve the single-key resolver's alias-chain semantics while keeping
            # each chain level to one PG round trip for refill batch lease hot paths.
            for _ in range(8):
                query_keys = [
                    key
                    for key in _dedupe_preserve_order(unresolved_keys)
                    if key and key not in seen_alias_keys
                ]
                if not query_keys:
                    break
                seen_alias_keys.update(query_keys)
                placeholders = ", ".join("%s" for _ in query_keys)
                try:
                    alias_rows = self._select_rows(
                        "linkedin_profile_registry_aliases",
                        row_builder=lambda row: dict(row),
                        where_sql=f"alias_url_key IN ({placeholders})",
                        params=query_keys,
                        limit=0,
                    )
                except Exception:
                    alias_rows = []
                alias_map = {
                    str(dict(row).get("alias_url_key") or "").strip(): str(
                        dict(row).get("profile_url_key") or ""
                    ).strip()
                    for row in list(alias_rows or [])
                    if str(dict(row).get("alias_url_key") or "").strip()
                    and str(dict(row).get("profile_url_key") or "").strip()
                }
                if not alias_map:
                    break
                next_unresolved: list[str] = []
                for original_key, current_key in list(resolved_by_original.items()):
                    mapped_key = str(alias_map.get(current_key) or "").strip()
                    if not mapped_key or mapped_key == current_key:
                        continue
                    resolved_by_original[original_key] = mapped_key
                    if mapped_key not in seen_alias_keys:
                        next_unresolved.append(mapped_key)
                unresolved_keys = next_unresolved
            return resolved_by_original
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_aliases", method_name="_resolve_keys_bulk"
        )
        return {}  # unreachable — the invariant raise above never returns

    def _list_alias_urls(self, canonical_key: str) -> list[str]:
        normalized_canonical_key = str(canonical_key or "").strip()
        if not normalized_canonical_key:
            return []
        if self._should_prefer_read("linkedin_profile_registry_aliases"):
            alias_rows = self._select_rows(
                "linkedin_profile_registry_aliases",
                row_builder=lambda row: dict(row),
                where_sql="profile_url_key = %s",
                params=[normalized_canonical_key],
                order_by_sql="updated_at DESC",
                limit=0,
            )
            if alias_rows or self._strict_authoritative("linkedin_profile_registry_aliases"):
                aliases: list[str] = []
                for row in alias_rows:
                    alias_url = str(dict(row).get("alias_url") or "").strip()
                    if alias_url and alias_url not in aliases:
                        aliases.append(alias_url)
                return aliases
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_aliases", method_name="_list_alias_urls"
        )
        return []  # unreachable — the invariant raise above never returns

    # --- persisted-row payload builder (descriptor to_columns + now-defaults) ---

    def _row_payload(
        self,
        *,
        profile_url_key: str,
        profile_url: str,
        raw_linkedin_url: str,
        sanity_linkedin_url: str,
        status: str,
        retry_count: int,
        last_error: str,
        last_run_id: str,
        last_dataset_id: str,
        last_snapshot_dir: str,
        last_raw_path: str,
        first_queued_at: str,
        last_queued_at: str,
        last_fetched_at: str,
        last_failed_at: str,
        source_shards: list[str],
        source_jobs: list[str],
        refill_queue_state: str = "",
        last_refill_trigger_kind: str = "",
        last_refill_plan_reason: str = "",
        last_refill_deferred_reason: str = "",
        last_refill_planned_at: str = "",
        refill_not_before_at: str = "",
        refill_plan_batch_size: int = 0,
        refill_plan_batch_count: int = 0,
        refill_plan_window_url_count: int = 0,
        last_refill_attempt_count: int = 0,
        refill_owner_worker_id: int = 0,
        refill_owner_run_id: str = "",
        refill_owner_dataset_id: str = "",
        refill_owner_payload_hash: str = "",
        refill_terminal_status: str = "",
        refill_terminal_at: str = "",
        created_at: str = "",
        updated_at: str = "",
    ) -> dict[str, Any]:
        now = utc_now_timestamp()
        # Write normalization (strip text / non-negative ints / status default "queued") is declared once
        # in the typed descriptor's to_columns. created_at/updated_at default to now here.
        return LINKEDIN_PROFILE_REGISTRY.to_columns(
            {
                "profile_url_key": profile_url_key,
                "profile_url": profile_url,
                "raw_linkedin_url": raw_linkedin_url,
                "sanity_linkedin_url": sanity_linkedin_url,
                "status": status,
                "retry_count": retry_count,
                "last_error": last_error,
                "last_run_id": last_run_id,
                "last_dataset_id": last_dataset_id,
                "last_snapshot_dir": last_snapshot_dir,
                "last_raw_path": last_raw_path,
                "first_queued_at": first_queued_at,
                "last_queued_at": last_queued_at,
                "last_fetched_at": last_fetched_at,
                "last_failed_at": last_failed_at,
                "source_shards": source_shards,
                "source_jobs": source_jobs,
                "refill_queue_state": refill_queue_state,
                "last_refill_trigger_kind": last_refill_trigger_kind,
                "last_refill_plan_reason": last_refill_plan_reason,
                "last_refill_deferred_reason": last_refill_deferred_reason,
                "last_refill_planned_at": last_refill_planned_at,
                "refill_not_before_at": refill_not_before_at,
                "refill_plan_batch_size": refill_plan_batch_size,
                "refill_plan_batch_count": refill_plan_batch_count,
                "refill_plan_window_url_count": refill_plan_window_url_count,
                "last_refill_attempt_count": last_refill_attempt_count,
                "refill_owner_worker_id": refill_owner_worker_id,
                "refill_owner_run_id": refill_owner_run_id,
                "refill_owner_dataset_id": refill_owner_dataset_id,
                "refill_owner_payload_hash": refill_owner_payload_hash,
                "refill_terminal_status": refill_terminal_status,
                "refill_terminal_at": refill_terminal_at,
                "created_at": created_at or now,
                "updated_at": updated_at or now,
            }
        )

    # --- reads ---

    def get(self, profile_url: str) -> dict[str, Any] | None:
        key = normalize_linkedin_profile_url_key(profile_url)
        if not key:
            return None
        resolved_key = self._resolve_key(key)
        payload = self._select_row(
            "linkedin_profile_registry",
            row_builder=self._registry_from_row,
            where_sql="profile_url_key = %s",
            params=[resolved_key],
        )
        if payload is None:
            return None
        payload["alias_urls"] = self._list_alias_urls(resolved_key)
        return payload

    def get_bulk(self, profile_urls: list[str]) -> dict[str, dict[str, Any]]:
        keys = _dedupe_preserve_order(
            [normalize_linkedin_profile_url_key(profile_url) for profile_url in list(profile_urls or [])]
        )
        if not keys:
            return {}
        # The PG branch always resolves canonical_keys whenever any input key is present and returns
        # `resolved` (the alias-table routing is unchanged from the store-era implementation).
        alias_map: dict[str, str] = {}
        if self._should_prefer_read("linkedin_profile_registry_aliases"):
            placeholders = ", ".join("%s" for _ in keys)
            alias_rows = self._select_rows(
                "linkedin_profile_registry_aliases",
                row_builder=lambda row: dict(row),
                where_sql=f"alias_url_key IN ({placeholders})",
                params=keys,
                limit=0,
            )
            alias_map = {
                str(dict(row).get("alias_url_key") or "").strip(): str(dict(row).get("profile_url_key") or "").strip()
                for row in alias_rows
                if str(dict(row).get("alias_url_key") or "").strip()
                and str(dict(row).get("profile_url_key") or "").strip()
            }
        canonical_keys = _dedupe_preserve_order(
            [str(alias_map.get(key) or key).strip() for key in keys if str(alias_map.get(key) or key).strip()]
        )
        if not canonical_keys:
            return {}
        placeholders = ", ".join("%s" for _ in canonical_keys)
        rows = self._select_rows(
            "linkedin_profile_registry",
            row_builder=self._registry_from_row,
            where_sql=f"profile_url_key IN ({placeholders})",
            params=canonical_keys,
            limit=0,
        )
        alias_rows_all = self._select_rows(
            "linkedin_profile_registry_aliases",
            row_builder=lambda row: dict(row),
            where_sql=f"profile_url_key IN ({placeholders})",
            params=canonical_keys,
            order_by_sql="updated_at DESC",
            limit=0,
        )
        aliases_by_canonical: dict[str, list[str]] = {}
        for alias_row in alias_rows_all:
            canonical_key = str(dict(alias_row).get("profile_url_key") or "").strip()
            alias_url = str(dict(alias_row).get("alias_url") or "").strip()
            if not canonical_key or not alias_url:
                continue
            aliases_by_canonical.setdefault(canonical_key, [])
            if alias_url not in aliases_by_canonical[canonical_key]:
                aliases_by_canonical[canonical_key].append(alias_url)
        payload_by_canonical: dict[str, dict[str, Any]] = {}
        for row in rows:
            canonical_key = str(dict(row).get("profile_url_key") or "").strip()
            if not canonical_key:
                continue
            payload = dict(row)
            payload["alias_urls"] = list(aliases_by_canonical.get(canonical_key) or [])
            payload_by_canonical[canonical_key] = payload
        resolved: dict[str, dict[str, Any]] = dict(payload_by_canonical)
        for key in keys:
            canonical_key = str(alias_map.get(key) or key).strip()
            payload = payload_by_canonical.get(canonical_key)
            if payload is not None:
                resolved[key] = dict(payload)
        return resolved

    def summarize_scope(
        self,
        *,
        source_job: str,
        snapshot_dir: str,
    ) -> dict[str, Any]:
        """Return the terminal-state proof for one job-owned snapshot profile wave.

        The profile registry is the scheduler source of truth. Stage recovery
        must therefore only restore LinkedIn Stage 1 from candidate documents
        when this job/snapshot registry scope has no open provider work left.
        """

        normalized_source_job = str(source_job or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        if not normalized_source_job or not normalized_snapshot_dir:
            return {
                "requested_url_count": 0,
                "terminal_url_count": 0,
                "fetched_url_count": 0,
                "fetched_missing_raw_path_count": 0,
                "unrecoverable_url_count": 0,
                "open_url_count": 0,
                "open_state_counts": {},
                "terminal_queue_state_leak_count": 0,
                "all_requested_terminal": False,
                "source_job": normalized_source_job,
                "snapshot_dir": normalized_snapshot_dir,
            }

        rows = self._select_rows(
            "linkedin_profile_registry",
            row_builder=self._registry_from_row,
            where_sql="last_snapshot_dir = %s",
            params=[normalized_snapshot_dir],
            order_by_sql="updated_at ASC",
            limit=0,
        )
        return _summarize_registry_rows_for_scope(
            rows,
            source_job=normalized_source_job,
            snapshot_dir=normalized_snapshot_dir,
        )

    def get_aliases(self, profile_url: str) -> list[str]:
        key = normalize_linkedin_profile_url_key(profile_url)
        if not key:
            return []
        if self._should_prefer_read("linkedin_profile_registry_aliases"):
            canonical_key = self._resolve_key(key)
            aliases = self._list_alias_urls(canonical_key)
            if aliases or self._strict_authoritative("linkedin_profile_registry_aliases"):
                return aliases
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_aliases", method_name="get_aliases"
        )
        return []  # unreachable — the invariant raise above never returns

    def upsert_aliases(
        self,
        profile_url: str,
        alias_urls: list[str],
        *,
        alias_kind: str = "observed",
    ) -> int:
        normalized_profile_url = str(profile_url or "").strip()
        profile_key = normalize_linkedin_profile_url_key(normalized_profile_url)
        normalized_alias_urls = normalize_linkedin_profile_url_list(alias_urls)
        if not profile_key and not normalized_alias_urls:
            return 0
        if self._should_prefer_read("linkedin_profile_registry_aliases"):
            canonical_key = self._resolve_key(profile_key) if profile_key else ""
            if not canonical_key and normalized_alias_urls:
                canonical_key = normalize_linkedin_profile_url_key(normalized_alias_urls[0])
            if not canonical_key:
                return 0
            existing = self._select_row(
                "linkedin_profile_registry",
                row_builder=self._registry_from_row,
                where_sql="profile_url_key = %s",
                params=[canonical_key],
            )
            canonical_profile_url = str(dict(existing or {}).get("profile_url") or "").strip()
            if not canonical_profile_url:
                canonical_profile_url = normalized_profile_url or canonical_key
                payload = self._row_payload(
                    profile_url_key=canonical_key,
                    profile_url=canonical_profile_url,
                    raw_linkedin_url="",
                    sanity_linkedin_url="",
                    status="queued",
                    retry_count=0,
                    last_error="",
                    last_run_id="",
                    last_dataset_id="",
                    last_snapshot_dir="",
                    last_raw_path="",
                    first_queued_at="",
                    last_queued_at="",
                    last_fetched_at="",
                    last_failed_at="",
                    source_shards=[],
                    source_jobs=[],
                    created_at=utc_now_timestamp(),
                    updated_at=utc_now_timestamp(),
                )
                self._write_row("linkedin_profile_registry", payload)
            all_alias_urls = normalize_linkedin_profile_url_list([canonical_profile_url, *normalized_alias_urls])
            upserted = 0
            now = utc_now_timestamp()
            for alias_url in all_alias_urls:
                alias_key = normalize_linkedin_profile_url_key(alias_url)
                if not alias_key:
                    continue
                if self._write_row(
                    "linkedin_profile_registry_aliases",
                    {
                        "alias_url_key": alias_key,
                        "profile_url_key": canonical_key,
                        "alias_url": alias_url,
                        "alias_kind": str(alias_kind or "observed").strip() or "observed",
                        "created_at": now,
                        "updated_at": now,
                    },
                ):
                    upserted += 1
            return upserted
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_aliases", method_name="upsert_aliases"
        )
        return 0  # unreachable — the invariant raise above never returns

    # --- lease family ---

    def acquire_lease(
        self,
        profile_url: str,
        *,
        lease_owner: str,
        lease_seconds: int = 240,
        lease_token: str = "",
    ) -> dict[str, Any]:
        normalized_owner = str(lease_owner or "").strip()
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key or not normalized_owner:
            return {
                "profile_url_key": normalized_key,
                "acquired": False,
                "lease_owner": "",
                "lease_token": "",
                "lease_expires_at": "",
            }
        normalized_token = (
            str(lease_token or "").strip()
            or sha1(f"{normalized_key}:{normalized_owner}:{utc_now_timestamp()}".encode("utf-8")).hexdigest()[:16]
        )
        ttl_seconds = max(5, int(lease_seconds or 0))
        if self._should_prefer_read("linkedin_profile_registry_leases"):
            canonical_key = self._resolve_key(normalized_key)
            native_acquire = getattr(self._adapter, "acquire_linkedin_profile_registry_lease", None)
            if callable(native_acquire):
                try:
                    native_payload = native_acquire(
                        canonical_key,
                        lease_owner=normalized_owner,
                        lease_seconds=ttl_seconds,
                        lease_token=normalized_token,
                    )
                except Exception as exc:
                    if self._strict_authoritative("linkedin_profile_registry_leases"):
                        self._raise_write_failure(
                            table_name="linkedin_profile_registry_leases",
                            method_name="acquire_linkedin_profile_registry_lease",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
                    native_payload = None
                if native_payload is not None:
                    lease_payload = self._lease_from_row(native_payload)
                    acquired = (
                        str(lease_payload.get("lease_owner") or "") == normalized_owner
                        and str(lease_payload.get("lease_token") or "") == normalized_token
                        and not bool(lease_payload.get("expired"))
                    )
                    return {
                        **lease_payload,
                        "acquired": acquired,
                        "contended": bool(
                            not acquired
                            and lease_payload
                            and str(lease_payload.get("lease_owner") or "").strip()
                        ),
                    }
            existing = self._select_row(
                "linkedin_profile_registry_leases",
                row_builder=self._lease_from_row,
                where_sql="profile_url_key = %s",
                params=[canonical_key],
            )
            if (
                existing
                and not bool(existing.get("expired"))
                and str(existing.get("lease_owner") or "") not in {"", normalized_owner}
                and str(existing.get("lease_token") or "") not in {"", normalized_token}
            ):
                return {
                    **existing,
                    "acquired": False,
                    "contended": True,
                }
            now = utc_now_timestamp()
            self._write_row(
                "linkedin_profile_registry_leases",
                {
                    "profile_url_key": canonical_key,
                    "lease_owner": normalized_owner,
                    "lease_token": normalized_token,
                    "lease_expires_at": (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "created_at": str(dict(existing or {}).get("created_at") or now),
                    "updated_at": now,
                },
            )
            lease_payload = self.get_lease(profile_url) or {}
            if lease_payload:
                return {
                    **lease_payload,
                    "acquired": True,
                    "contended": False,
                }
            if self._strict_authoritative("linkedin_profile_registry_leases"):
                return {
                    **dict(existing or {}),
                    "acquired": False,
                    "contended": bool(existing),
                }
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_leases", method_name="acquire_lease"
        )
        return {}  # unreachable — the invariant raise above never returns

    def acquire_leases(
        self,
        profile_urls: list[str] | tuple[str, ...],
        *,
        lease_owner: str,
        lease_seconds: int = 240,
        lease_token: str = "",
    ) -> dict[str, Any]:
        normalized_owner = str(lease_owner or "").strip()
        normalized_urls: list[str] = []
        for profile_url in list(profile_urls or []):
            normalized_profile_url = str(profile_url or "").strip()
            if normalized_profile_url and normalized_profile_url not in normalized_urls:
                normalized_urls.append(normalized_profile_url)
        if not normalized_urls or not normalized_owner:
            return {
                "acquired": False,
                "lease_owner": normalized_owner,
                "lease_token": "",
                "requested_urls": normalized_urls,
                "acquired_urls": [],
                "contended_urls": normalized_urls,
                "leases_by_url": {},
            }
        normalized_token = (
            str(lease_token or "").strip()
            or sha1(
                f"{','.join(normalize_linkedin_profile_url_key(url) for url in normalized_urls)}:{normalized_owner}:{utc_now_timestamp()}".encode(
                    "utf-8"
                )
            ).hexdigest()[:16]
        )
        ttl_seconds = max(5, int(lease_seconds or 0))
        if self._should_prefer_read("linkedin_profile_registry_leases"):
            canonical_keys_by_normalized_key = self._resolve_keys_bulk(
                [normalize_linkedin_profile_url_key(profile_url) for profile_url in normalized_urls]
            )
            canonical_keys_by_url = {
                profile_url: str(
                    canonical_keys_by_normalized_key.get(normalize_linkedin_profile_url_key(profile_url))
                    or normalize_linkedin_profile_url_key(profile_url)
                ).strip()
                for profile_url in normalized_urls
            }
            canonical_keys = _dedupe_preserve_order(
                [key for key in canonical_keys_by_url.values() if str(key or "").strip()]
            )
            native_acquire = getattr(self._adapter, "acquire_linkedin_profile_registry_leases", None)
            if callable(native_acquire) and canonical_keys:
                try:
                    native_rows = native_acquire(
                        canonical_keys,
                        lease_owner=normalized_owner,
                        lease_seconds=ttl_seconds,
                        lease_token=normalized_token,
                    )
                except Exception as exc:
                    if self._strict_authoritative("linkedin_profile_registry_leases"):
                        self._raise_write_failure(
                            table_name="linkedin_profile_registry_leases",
                            method_name="acquire_linkedin_profile_registry_leases",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
                    native_rows = None
                if native_rows is not None:
                    rows_by_key = {
                        str(dict(row or {}).get("profile_url_key") or "").strip(): self._lease_from_row(row)
                        for row in list(native_rows or [])
                        if str(dict(row or {}).get("profile_url_key") or "").strip()
                    }
                    return self._build_batch_lease_payload(
                        normalized_urls=normalized_urls,
                        canonical_keys_by_url=canonical_keys_by_url,
                        rows_by_key=rows_by_key,
                        lease_owner=normalized_owner,
                        lease_token=normalized_token,
                    )
            if self._strict_authoritative("linkedin_profile_registry_leases"):
                lease_payloads: list[dict[str, Any]] = []
                for profile_url in normalized_urls:
                    lease_payloads.append(
                        self.acquire_lease(
                            profile_url,
                            lease_owner=normalized_owner,
                            lease_seconds=ttl_seconds,
                            lease_token=normalized_token,
                        )
                    )
                return self._build_batch_lease_payload(
                    normalized_urls=normalized_urls,
                    canonical_keys_by_url={
                        profile_url: str(payload.get("profile_url_key") or "")
                        for profile_url, payload in zip(normalized_urls, lease_payloads)
                    },
                    rows_by_key={
                        str(payload.get("profile_url_key") or ""): dict(payload)
                        for payload in lease_payloads
                        if str(payload.get("profile_url_key") or "")
                    },
                    lease_owner=normalized_owner,
                    lease_token=normalized_token,
                )
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_leases", method_name="acquire_leases"
        )
        return {}  # unreachable — the invariant raise above never returns

    def get_lease(self, profile_url: str) -> dict[str, Any] | None:
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key:
            return None
        # Parity with the historical store surface: a missing lease row is the empty-dict sentinel from
        # _lease_from_row(None), never None for a valid URL.
        canonical_key = self._resolve_key(normalized_key)
        payload = self._select_row(
            "linkedin_profile_registry_leases",
            row_builder=self._lease_from_row,
            where_sql="profile_url_key = %s",
            params=[canonical_key],
        )
        if payload is not None:
            return payload
        return self._lease_from_row(None)

    def release_lease(
        self,
        profile_url: str,
        *,
        lease_owner: str = "",
        lease_token: str = "",
    ) -> bool:
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key:
            return False
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if self._should_prefer_read("linkedin_profile_registry_leases"):
            canonical_key = self._resolve_key(normalized_key)
            clauses = ["profile_url_key = %s"]
            params: list[Any] = [canonical_key]
            if normalized_owner:
                clauses.append("lease_owner = %s")
                params.append(normalized_owner)
            if normalized_token:
                clauses.append("lease_token = %s")
                params.append(normalized_token)
            try:
                deleted_count = self._adapter.delete_rows(
                    table_name="linkedin_profile_registry_leases",
                    where_sql=" AND ".join(clauses),
                    params=params,
                )
            except Exception as exc:
                if self._strict_authoritative("linkedin_profile_registry_leases"):
                    self._raise_write_failure(
                        table_name="linkedin_profile_registry_leases",
                        method_name="delete_rows",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
                deleted_count = 0
            if deleted_count or self._strict_authoritative("linkedin_profile_registry_leases"):
                return bool(deleted_count)
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_leases", method_name="release_lease"
        )
        return False  # unreachable — the invariant raise above never returns

    def release_leases(
        self,
        profile_urls: list[str] | tuple[str, ...],
        *,
        lease_owner: str = "",
        lease_token: str = "",
    ) -> int:
        normalized_urls: list[str] = []
        for profile_url in list(profile_urls or []):
            normalized_profile_url = str(profile_url or "").strip()
            if normalized_profile_url and normalized_profile_url not in normalized_urls:
                normalized_urls.append(normalized_profile_url)
        if not normalized_urls:
            return 0
        normalized_owner = str(lease_owner or "").strip()
        normalized_token = str(lease_token or "").strip()
        if self._should_prefer_read("linkedin_profile_registry_leases"):
            canonical_keys_by_normalized_key = self._resolve_keys_bulk(
                [normalize_linkedin_profile_url_key(profile_url) for profile_url in normalized_urls]
            )
            canonical_keys = _dedupe_preserve_order(
                [
                    str(
                        canonical_keys_by_normalized_key.get(normalize_linkedin_profile_url_key(profile_url))
                        or normalize_linkedin_profile_url_key(profile_url)
                    ).strip()
                    for profile_url in normalized_urls
                    if normalize_linkedin_profile_url_key(profile_url)
                ]
            )
            native_release = getattr(self._adapter, "release_linkedin_profile_registry_leases", None)
            if callable(native_release) and canonical_keys:
                try:
                    deleted_count = int(
                        native_release(
                            canonical_keys,
                            lease_owner=normalized_owner,
                            lease_token=normalized_token,
                        )
                        or 0
                    )
                except Exception as exc:
                    if self._strict_authoritative("linkedin_profile_registry_leases"):
                        self._raise_write_failure(
                            table_name="linkedin_profile_registry_leases",
                            method_name="release_linkedin_profile_registry_leases",
                            reason=f"{type(exc).__name__}: {exc}",
                            error=exc,
                        )
                    deleted_count = 0
                if deleted_count or self._strict_authoritative("linkedin_profile_registry_leases"):
                    return deleted_count
            if self._strict_authoritative("linkedin_profile_registry_leases"):
                return sum(
                    1
                    for profile_url in normalized_urls
                    if self.release_lease(
                        profile_url,
                        lease_owner=normalized_owner,
                        lease_token=normalized_token,
                    )
                )
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_leases", method_name="release_leases"
        )
        return 0  # unreachable — the invariant raise above never returns

    def _build_batch_lease_payload(
        self,
        *,
        normalized_urls: list[str],
        canonical_keys_by_url: dict[str, str],
        rows_by_key: dict[str, dict[str, Any]],
        lease_owner: str,
        lease_token: str,
    ) -> dict[str, Any]:
        acquired_urls: list[str] = []
        contended_urls: list[str] = []
        leases_by_url: dict[str, dict[str, Any]] = {}
        for profile_url in normalized_urls:
            canonical_key = str(canonical_keys_by_url.get(profile_url) or "").strip()
            lease_payload = dict(rows_by_key.get(canonical_key) or {})
            acquired = (
                bool(lease_payload)
                and str(lease_payload.get("lease_owner") or "") == str(lease_owner or "")
                and str(lease_payload.get("lease_token") or "") == str(lease_token or "")
                and not bool(lease_payload.get("expired"))
            )
            lease_result = {
                **lease_payload,
                "profile_url_key": canonical_key or str(lease_payload.get("profile_url_key") or ""),
                "acquired": acquired,
                "contended": bool(
                    not acquired
                    and lease_payload
                    and str(lease_payload.get("lease_owner") or "").strip()
                ),
            }
            leases_by_url[profile_url] = lease_result
            if acquired:
                acquired_urls.append(profile_url)
            else:
                contended_urls.append(profile_url)
        return {
            "acquired": len(acquired_urls) == len(normalized_urls),
            "lease_owner": str(lease_owner or ""),
            "lease_token": str(lease_token or ""),
            "requested_urls": list(normalized_urls),
            "acquired_urls": acquired_urls,
            "contended_urls": contended_urls,
            "leases_by_url": leases_by_url,
            "acquired_count": len(acquired_urls),
            "contended_count": len(contended_urls),
        }

    # --- events + metrics ---

    def record_event(
        self,
        profile_url: str,
        *,
        event_type: str,
        event_status: str = "",
        detail: str = "",
        run_id: str = "",
        dataset_id: str = "",
        metadata: dict[str, Any] | None = None,
        duration_ms: int | None = None,
    ) -> None:
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        if not normalized_key:
            return
        if self._should_prefer_read("linkedin_profile_registry_events"):
            canonical_key = self._resolve_key(normalized_key)
            now = utc_now_timestamp()
            try:
                self._adapter.insert_row_with_generated_id(
                    table_name="linkedin_profile_registry_events",
                    row={
                        "profile_url_key": canonical_key,
                        "event_type": str(event_type or "").strip(),
                        "event_status": str(event_status or "").strip(),
                        "detail": str(detail or "").strip(),
                        "run_id": str(run_id or "").strip(),
                        "dataset_id": str(dataset_id or "").strip(),
                        "metadata_json": json.dumps(metadata or {}, ensure_ascii=False),
                        "duration_ms": int(duration_ms) if duration_ms is not None else None,
                        "created_at": now,
                    },
                )
            except Exception as exc:
                if self._strict_authoritative("linkedin_profile_registry_events"):
                    self._raise_write_failure(
                        table_name="linkedin_profile_registry_events",
                        method_name="insert_row_with_generated_id",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
            if self._strict_authoritative("linkedin_profile_registry_events"):
                return
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_events", method_name="record_event"
        )

    def get_metrics(self, *, lookback_hours: int = 24) -> dict[str, Any]:
        lookback = max(0, int(lookback_hours or 0))
        if self._should_prefer_read("linkedin_profile_registry_events"):
            if lookback > 0:
                pg_rows = self._select_rows(
                    "linkedin_profile_registry_events",
                    row_builder=lambda row: dict(row),
                    where_sql="created_at >= %s",
                    params=[
                        (datetime.now(timezone.utc) - timedelta(hours=lookback)).strftime("%Y-%m-%d %H:%M:%S")
                    ],
                    order_by_sql="event_id ASC",
                    limit=0,
                )
            else:
                pg_rows = self._select_rows(
                    "linkedin_profile_registry_events",
                    row_builder=lambda row: dict(row),
                    order_by_sql="event_id ASC",
                    limit=0,
                )
            pg_registry_rows = self._select_rows(
                "linkedin_profile_registry",
                row_builder=self._registry_from_row,
                limit=0,
            )
            if pg_rows or pg_registry_rows or self._strict_authoritative("linkedin_profile_registry_events"):
                rows = pg_rows
                status_counts: dict[str, int] = {}
                for registry_row in pg_registry_rows:
                    status = str(dict(registry_row).get("status") or "").strip()
                    if not status:
                        continue
                    status_counts[status] = int(status_counts.get(status) or 0) + 1
                status_row = [{"status": status, "count": count} for status, count in status_counts.items()]
            else:
                self._raise_postgres_only_invariant(
                    table_name="linkedin_profile_registry_events", method_name="get_metrics"
                )
                return {}  # unreachable — the invariant raise above never returns
        else:
            self._raise_postgres_only_invariant(
                table_name="linkedin_profile_registry_events", method_name="get_metrics"
            )
            return {}  # unreachable — the invariant raise above never returns
        total_events = len(rows)
        lookup_attempts = 0
        cache_hits = 0
        live_fetch_requests = 0
        duplicate_skips = 0
        live_fetch_success = 0
        live_fetch_failed = 0
        retry_success = 0
        retry_failures = 0
        unrecoverable_failures = 0
        queue_durations_ms: list[int] = []
        for row in rows:
            event_type = str(row["event_type"] or "").strip()
            event_status = str(row["event_status"] or "").strip().lower()
            if event_type == "lookup_attempt":
                lookup_attempts += 1
            if event_type in {"cache_hit_registry", "cache_hit_local_raw", "cache_hit_lease_wait"}:
                cache_hits += 1
            if event_type == "live_fetch_requested":
                live_fetch_requests += 1
            if event_type in {"duplicate_fetch_blocked", "lease_contended_skip"}:
                duplicate_skips += 1
            metadata: dict[str, Any] = {}
            try:
                metadata = dict(json.loads(row["metadata_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                metadata = {}
            retry_before = int(metadata.get("retry_count_before") or 0)
            if event_type == "live_fetch_success":
                live_fetch_success += 1
                if retry_before > 0:
                    retry_success += 1
                duration_ms = row["duration_ms"]
                if duration_ms is not None:
                    try:
                        queue_durations_ms.append(max(0, int(duration_ms)))
                    except (TypeError, ValueError):
                        pass
            elif event_type == "live_fetch_failed":
                live_fetch_failed += 1
                if retry_before > 0:
                    retry_failures += 1
                if event_status == "unrecoverable":
                    unrecoverable_failures += 1
        queue_duration_p50 = _percentile(queue_durations_ms, 50)
        queue_duration_p95 = _percentile(queue_durations_ms, 95)
        retry_attempts = retry_success + retry_failures
        terminal_attempts = live_fetch_success + live_fetch_failed
        registry_status_counts = {
            str(row["status"] or "").strip(): int(row["count"] or 0)
            for row in status_row
            if str(row["status"] or "").strip()
        }
        return {
            "window_hours": lookback,
            "event_count": total_events,
            "lookup_attempts": lookup_attempts,
            "cache_hits": cache_hits,
            "cache_hit_rate": (cache_hits / lookup_attempts) if lookup_attempts else 0.0,
            "live_fetch_requests": live_fetch_requests,
            "duplicate_fetch_skips": duplicate_skips,
            "duplicate_request_rate": (duplicate_skips / (live_fetch_requests + duplicate_skips))
            if (live_fetch_requests + duplicate_skips)
            else 0.0,
            "live_fetch_success": live_fetch_success,
            "live_fetch_failed": live_fetch_failed,
            "queued_duration_ms_p50": queue_duration_p50,
            "queued_duration_ms_p95": queue_duration_p95,
            "retry_success_count": retry_success,
            "retry_failure_count": retry_failures,
            "retry_success_rate": (retry_success / retry_attempts) if retry_attempts else 0.0,
            "unrecoverable_failures": unrecoverable_failures,
            "unrecoverable_ratio": (unrecoverable_failures / terminal_attempts) if terminal_attempts else 0.0,
            "registry_status_counts": registry_status_counts,
        }

    # --- backfill runs ---

    def get_backfill_run(self, run_key: str) -> dict[str, Any] | None:
        normalized_run_key = str(run_key or "").strip()
        if not normalized_run_key:
            return None
        if self._should_prefer_read("linkedin_profile_registry_backfill_runs"):
            payload = self._select_row(
                "linkedin_profile_registry_backfill_runs",
                row_builder=self._backfill_from_row,
                where_sql="run_key = %s",
                params=[normalized_run_key],
            )
            if payload is not None:
                return payload
            if self._strict_authoritative("linkedin_profile_registry_backfill_runs"):
                return None
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_backfill_runs", method_name="get_backfill_run"
        )
        return None  # unreachable — the invariant raise above never returns

    def upsert_backfill_run(
        self,
        run_key: str,
        *,
        scope_company: str = "",
        scope_snapshot_id: str = "",
        checkpoint: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
        status: str = "running",
    ) -> dict[str, Any] | None:
        normalized_run_key = str(run_key or "").strip()
        if not normalized_run_key:
            return None
        checkpoint_payload = checkpoint or {}
        summary_payload = summary or {}
        if self._should_prefer_read("linkedin_profile_registry_backfill_runs"):
            existing = self.get_backfill_run(normalized_run_key) or {}
            row_payload = LINKEDIN_PROFILE_REGISTRY_BACKFILL_RUNS.to_columns(
                {
                    "run_key": normalized_run_key,
                    "scope_company": scope_company,
                    "scope_snapshot_id": scope_snapshot_id,
                    "checkpoint": checkpoint_payload,
                    "summary": summary_payload,
                    "status": status,
                    "created_at": str(dict(existing).get("created_at") or utc_now_timestamp()),
                    "updated_at": utc_now_timestamp(),
                }
            )
            self._write_row("linkedin_profile_registry_backfill_runs", row_payload)
            return self.get_backfill_run(normalized_run_key)
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry_backfill_runs", method_name="upsert_backfill_run"
        )
        return None  # unreachable — the invariant raise above never returns

    # --- status transitions ---

    def mark_queued(
        self,
        profile_url: str,
        *,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_registry(
            profile_url=profile_url,
            status="queued",
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            run_id=run_id,
            dataset_id=dataset_id,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=True,
        )

    def mark_queued_many(
        self,
        profile_urls: list[str] | tuple[str, ...] | set[str],
        *,
        source_shards: list[str] | None = None,
        source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
        raw_linkedin_urls_by_url: dict[str, str] | None = None,
        sanity_linkedin_urls_by_url: dict[str, str] | None = None,
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any]:
        normalized_urls = normalize_linkedin_profile_url_list(list(profile_urls or []))
        if not normalized_urls:
            return {"status": "skipped", "reason": "no_profile_urls", "queued_count": 0, "requested_count": 0}
        normalized_source_shards = _normalize_registry_label_list(source_shards)
        normalized_source_jobs = _normalize_registry_label_list(source_jobs)
        source_shards_map = {
            str(url or "").strip(): _normalize_registry_label_list(list(values or []))
            for url, values in dict(source_shards_by_url or {}).items()
            if str(url or "").strip()
        }
        alias_map = {
            str(url or "").strip(): normalize_linkedin_profile_url_list(list(values or []))
            for url, values in dict(alias_urls_by_url or {}).items()
            if str(url or "").strip()
        }
        raw_url_map = {
            str(url or "").strip(): str(value or "").strip()
            for url, value in dict(raw_linkedin_urls_by_url or {}).items()
            if str(url or "").strip()
        }
        sanity_url_map = {
            str(url or "").strip(): str(value or "").strip()
            for url, value in dict(sanity_linkedin_urls_by_url or {}).items()
            if str(url or "").strip()
        }
        normalized_run_id = str(run_id or "").strip()
        normalized_dataset_id = str(dataset_id or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        now_timestamp = utc_now_timestamp()

        def _source_shards_for(url: str) -> list[str]:
            return source_shards_map.get(str(url or "").strip()) or normalized_source_shards

        if self._should_prefer_read("linkedin_profile_registry"):
            try:
                requested_keys = _dedupe_preserve_order(
                    [normalize_linkedin_profile_url_key(profile_url) for profile_url in normalized_urls]
                )
                canonical_keys_by_key = self._resolve_keys_bulk(requested_keys)
                canonical_keys = _dedupe_preserve_order(
                    [
                        str(canonical_keys_by_key.get(key) or key).strip()
                        for key in requested_keys
                        if str(canonical_keys_by_key.get(key) or key).strip()
                    ]
                )
                existing_rows_by_key: dict[str, dict[str, Any]] = {}
                aliases_by_canonical: dict[str, list[str]] = {}
                if canonical_keys:
                    placeholders = ", ".join("%s" for _ in canonical_keys)
                    existing_rows = self._select_rows(
                        "linkedin_profile_registry",
                        row_builder=self._registry_from_row,
                        where_sql=f"profile_url_key IN ({placeholders})",
                        params=canonical_keys,
                        limit=0,
                    )
                    alias_rows = self._select_rows(
                        "linkedin_profile_registry_aliases",
                        row_builder=lambda row: dict(row),
                        where_sql=f"profile_url_key IN ({placeholders})",
                        params=canonical_keys,
                        order_by_sql="updated_at DESC",
                        limit=0,
                    )
                    for alias_row in alias_rows:
                        canonical_key = str(dict(alias_row).get("profile_url_key") or "").strip()
                        alias_url = str(dict(alias_row).get("alias_url") or "").strip()
                        if not canonical_key or not alias_url:
                            continue
                        aliases = aliases_by_canonical.setdefault(canonical_key, [])
                        if alias_url not in aliases:
                            aliases.append(alias_url)
                    for existing_row in existing_rows:
                        payload = dict(existing_row or {})
                        canonical_key = str(payload.get("profile_url_key") or "").strip()
                        if not canonical_key:
                            continue
                        payload["alias_urls"] = list(aliases_by_canonical.get(canonical_key) or [])
                        existing_rows_by_key[canonical_key] = payload

                effective_by_key: dict[str, dict[str, Any]] = {}
                modified_keys: set[str] = set()
                for profile_url in normalized_urls:
                    normalized_key = normalize_linkedin_profile_url_key(profile_url)
                    canonical_key = str(canonical_keys_by_key.get(normalized_key) or normalized_key).strip()
                    if not canonical_key:
                        continue
                    existing_payload = dict(
                        effective_by_key.get(canonical_key)
                        or existing_rows_by_key.get(canonical_key)
                        or {}
                    )
                    effective_payload = self._compose_effective_payload(
                        existing_payload=existing_payload,
                        normalized_status="queued",
                        normalized_profile_url=profile_url,
                        normalized_raw_linkedin_url=raw_url_map.get(profile_url, ""),
                        normalized_sanity_linkedin_url=sanity_url_map.get(profile_url, ""),
                        normalized_alias_urls=[profile_url, *list(alias_map.get(profile_url) or [])],
                        normalized_run_id=normalized_run_id,
                        normalized_dataset_id=normalized_dataset_id,
                        normalized_snapshot_dir=normalized_snapshot_dir,
                        normalized_raw_path="",
                        normalized_source_shards=_source_shards_for(profile_url),
                        normalized_source_jobs=normalized_source_jobs,
                        retry_count=None,
                        increment_retry=False,
                        last_error=None,
                        preserve_unrecoverable=True,
                        now_timestamp=now_timestamp,
                    )
                    effective_by_key[canonical_key] = effective_payload
                    modified_keys.add(canonical_key)

                registry_rows_to_upsert: list[dict[str, Any]] = []
                alias_rows_to_upsert: list[dict[str, Any]] = []
                alias_written_keys: set[str] = set()
                for canonical_key in canonical_keys:
                    if canonical_key not in modified_keys:
                        continue
                    effective_payload = dict(effective_by_key.get(canonical_key) or {})
                    profile_url = str(effective_payload.get("profile_url") or canonical_key).strip()
                    registry_rows_to_upsert.append(
                        self._effective_payload_row(canonical_key, profile_url, effective_payload, now_timestamp)
                    )
                    for alias_url in normalize_linkedin_profile_url_list(
                        [profile_url, *list(effective_payload.get("alias_urls") or [])]
                    ):
                        alias_key = normalize_linkedin_profile_url_key(alias_url)
                        if not alias_key or alias_key in alias_written_keys:
                            continue
                        alias_written_keys.add(alias_key)
                        alias_rows_to_upsert.append(
                            {
                                "alias_url_key": alias_key,
                                "profile_url_key": canonical_key,
                                "alias_url": alias_url,
                                "alias_kind": "observed",
                                "created_at": str(effective_payload.get("created_at") or now_timestamp),
                                "updated_at": now_timestamp,
                            }
                        )
                if registry_rows_to_upsert:
                    self._adapter.bulk_upsert_rows(
                        "linkedin_profile_registry",
                        registry_rows_to_upsert,
                    )
                if alias_rows_to_upsert:
                    self._adapter.bulk_upsert_rows(
                        "linkedin_profile_registry_aliases",
                        alias_rows_to_upsert,
                    )
                queued_count = len(registry_rows_to_upsert)
                return {
                    "status": "queued" if queued_count else "skipped",
                    "reason": "" if queued_count else "no_valid_profile_urls",
                    "queued_count": queued_count,
                    "requested_count": len(normalized_urls),
                    "write_mode": "bulk_postgres",
                }
            except Exception as exc:
                if self._strict_authoritative("linkedin_profile_registry"):
                    self._raise_write_failure(
                        table_name="linkedin_profile_registry",
                        method_name="mark_queued_many",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )

        queued_count = 0
        for profile_url in normalized_urls:
            payload = self._upsert_registry(
                profile_url=profile_url,
                status="queued",
                source_shards=_source_shards_for(profile_url),
                source_jobs=normalized_source_jobs,
                alias_urls=list(alias_map.get(profile_url) or []),
                raw_linkedin_url=raw_url_map.get(profile_url, ""),
                sanity_linkedin_url=sanity_url_map.get(profile_url, ""),
                run_id=normalized_run_id,
                dataset_id=normalized_dataset_id,
                snapshot_dir=normalized_snapshot_dir,
                preserve_unrecoverable=True,
            )
            if payload is not None:
                queued_count += 1
        return {
            "status": "queued" if queued_count else "skipped",
            "reason": "" if queued_count else "no_valid_profile_urls",
            "queued_count": queued_count,
            "requested_count": len(normalized_urls),
            "write_mode": "per_row_loop",
        }

    def mark_fetched(
        self,
        profile_url: str,
        *,
        raw_path: str,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_registry(
            profile_url=profile_url,
            status="fetched",
            retry_count=0,
            last_error="",
            raw_path=raw_path,
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            run_id=run_id,
            dataset_id=dataset_id,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=False,
        )

    def mark_failed(
        self,
        profile_url: str,
        *,
        error: str,
        retryable: bool = True,
        retry_delay_seconds: int = 30,
        max_retry_attempts: int | None = None,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        retry_count_before = 0
        if retryable:
            existing = self.get(profile_url) or {}
            retry_count_before = max(0, int(dict(existing).get("retry_count") or 0))
        retry_attempt_budget = (
            linkedin_profile_max_retry_attempts()
            if max_retry_attempts is None
            else max(0, int(max_retry_attempts or 0))
        )
        retry_queue_allowed = bool(retryable) and retry_count_before < retry_attempt_budget
        failed = self._upsert_registry(
            profile_url=profile_url,
            status="failed_retryable" if retry_queue_allowed else "unrecoverable",
            increment_retry=retryable,
            last_error=str(error or "").strip(),
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            run_id=run_id,
            dataset_id=dataset_id,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=not retry_queue_allowed,
        )
        if not retry_queue_allowed or failed is None:
            return failed
        failed_payload = dict(failed or {})
        retry_source_jobs = [
            str(item or "").strip()
            for item in list(failed_payload.get("source_jobs") or source_jobs or [])
            if str(item or "").strip()
        ]
        retry_snapshot_dir = str(failed_payload.get("last_snapshot_dir") or snapshot_dir or "").strip()
        if not retry_source_jobs or not retry_snapshot_dir:
            return failed
        retry_not_before_at = (
            datetime.now(timezone.utc).replace(microsecond=0)
            + timedelta(seconds=max(1, int(retry_delay_seconds or 30)))
        ).strftime("%Y-%m-%d %H:%M:%S")
        retry_profile_url = str(failed_payload.get("profile_url") or profile_url).strip()
        self.record_refill_plan_items(
            deferred_profile_urls=[retry_profile_url],
            source_shards_by_url={
                retry_profile_url: list(failed_payload.get("source_shards") or source_shards or [])
            },
            source_jobs=retry_source_jobs,
            snapshot_dir=retry_snapshot_dir,
            trigger_kind="profile_retry",
            plan_reason="profile_retry_wait",
            deferred_reason=str(error or "profile_retryable_failure").strip() or "profile_retryable_failure",
            deferred_queue_state="retry_wait",
            refill_not_before_at=retry_not_before_at,
        )
        return self.get(retry_profile_url) or failed

    def mark_deferred_for_coalescing(
        self,
        profile_url: str,
        *,
        reason: str,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        snapshot_dir: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_registry(
            profile_url=profile_url,
            status="deferred_coalescing",
            last_error=str(reason or "").strip(),
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            snapshot_dir=snapshot_dir,
            preserve_unrecoverable=True,
        )

    def upsert_sources(
        self,
        profile_url: str,
        *,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
    ) -> dict[str, Any] | None:
        return self._upsert_registry(
            profile_url=profile_url,
            status="",
            source_shards=source_shards,
            source_jobs=source_jobs,
            alias_urls=alias_urls,
            raw_linkedin_url=raw_linkedin_url,
            sanity_linkedin_url=sanity_linkedin_url,
            preserve_unrecoverable=True,
        )

    # --- refill queue ---

    def record_refill_plan_items(
        self,
        *,
        active_profile_urls: list[str] | tuple[str, ...] | set[str] | None = None,
        deferred_profile_urls: list[str] | tuple[str, ...] | set[str] | None = None,
        source_shards_by_url: dict[str, list[str]] | dict[str, set[str]] | None = None,
        source_jobs: list[str] | None = None,
        snapshot_dir: str = "",
        trigger_kind: str = "profile_prefetch_refill",
        plan_reason: str = "",
        active_queue_state: str = "planned_dispatch",
        active_reason: str = "",
        active_refill_not_before_at: str = "",
        active_owner_worker_id: int = 0,
        active_owner_run_id: str = "",
        active_owner_dataset_id: str = "",
        active_owner_payload_hash: str = "",
        deferred_reason: str = "",
        deferred_queue_state: str = "deferred_budget",
        refill_not_before_at: str = "",
        refill_plan_batch_size: int = 0,
        refill_plan_batch_count: int = 0,
        refill_plan_window_url_count: int = 0,
    ) -> dict[str, Any]:
        active_urls = normalize_linkedin_profile_url_list(list(active_profile_urls or []))
        deferred_urls = normalize_linkedin_profile_url_list(list(deferred_profile_urls or []))
        active_keys = {normalize_linkedin_profile_url_key(url) for url in active_urls}
        deferred_urls = [
            url for url in deferred_urls if normalize_linkedin_profile_url_key(url) not in active_keys
        ]
        if not active_urls and not deferred_urls:
            return {"status": "skipped", "reason": "no_refill_items", "active_item_count": 0, "deferred_item_count": 0}
        normalized_source_jobs = _normalize_registry_label_list(source_jobs)
        source_shards_map = {
            str(url or "").strip(): _normalize_registry_label_list(list(values or []))
            for url, values in dict(source_shards_by_url or {}).items()
            if str(url or "").strip()
        }
        planned_at = utc_now_timestamp()
        normalized_refill_not_before_at = str(refill_not_before_at or "").strip()
        normalized_active_refill_not_before_at = str(
            active_refill_not_before_at or refill_not_before_at or ""
        ).strip()
        normalized_active_owner_worker_id = max(0, int(active_owner_worker_id or 0))
        normalized_active_owner_run_id = str(active_owner_run_id or "").strip()
        normalized_active_owner_dataset_id = str(active_owner_dataset_id or "").strip()
        normalized_active_owner_payload_hash = str(active_owner_payload_hash or "").strip()
        normalized_active_queue_state = str(active_queue_state or "planned_dispatch").strip()
        if normalized_active_queue_state not in {
            "planned_dispatch",
            "dispatch_reserved",
            "dispatch_claimed",
            "deferred_budget",
        }:
            raise ValueError(
                "active_queue_state must be one of: planned_dispatch, dispatch_reserved, dispatch_claimed, deferred_budget"
            )
        normalized_deferred_queue_state = str(deferred_queue_state or "deferred_budget").strip()
        if normalized_deferred_queue_state not in {
            "deferred_budget",
            "deferred_coalescing",
            "retry_wait",
            "dispatch_reserved",
            "dispatch_claimed",
            "planned_dispatch",
        }:
            raise ValueError(
                "deferred_queue_state must be one of: deferred_budget, deferred_coalescing, retry_wait, dispatch_reserved, dispatch_claimed, planned_dispatch"
            )
        normalized_refill_plan_batch_size = max(0, int(refill_plan_batch_size or 0))
        normalized_refill_plan_batch_count = max(0, int(refill_plan_batch_count or 0))
        normalized_refill_plan_window_url_count = max(0, int(refill_plan_window_url_count or 0))

        def _source_shards_for(url: str) -> list[str]:
            return source_shards_map.get(str(url or "").strip()) or []

        terminal_skipped_active_count = 0
        terminal_skipped_deferred_count = 0

        if self._should_prefer_read("linkedin_profile_registry"):
            try:
                specs: list[dict[str, Any]] = []

                def _append_specs(urls: list[str], *, kind: str, queue_state: str, reason: str, not_before_at: str) -> None:
                    for profile_url in urls:
                        normalized_key = normalize_linkedin_profile_url_key(profile_url)
                        if not normalized_key:
                            continue
                        specs.append(
                            {
                                "url": profile_url,
                                "key": normalized_key,
                                "kind": kind,
                                "queue_state": queue_state,
                                "reason": reason,
                                "not_before_at": str(not_before_at or "").strip(),
                            }
                        )

                resolved_active_reason = str(active_reason or plan_reason or "ready_to_dispatch").strip()
                resolved_deferred_reason = str(deferred_reason or plan_reason or "worker_budget_deferred").strip()
                _append_specs(
                    active_urls,
                    kind="active",
                    queue_state=normalized_active_queue_state,
                    reason="" if normalized_active_queue_state == "planned_dispatch" else resolved_active_reason,
                    not_before_at=normalized_active_refill_not_before_at,
                )
                _append_specs(
                    deferred_urls,
                    kind="deferred",
                    queue_state=normalized_deferred_queue_state,
                    reason=resolved_deferred_reason,
                    not_before_at=normalized_refill_not_before_at,
                )
                spec_keys = _dedupe_preserve_order([str(spec.get("key") or "") for spec in specs])
                canonical_keys_by_key = self._resolve_keys_bulk(spec_keys)
                canonical_keys = _dedupe_preserve_order(
                    [
                        str(canonical_keys_by_key.get(key) or key).strip()
                        for key in spec_keys
                        if str(canonical_keys_by_key.get(key) or key).strip()
                    ]
                )
                existing_rows_by_key: dict[str, dict[str, Any]] = {}
                aliases_by_canonical: dict[str, list[str]] = {}
                if canonical_keys:
                    placeholders = ", ".join("%s" for _ in canonical_keys)
                    existing_rows = self._select_rows(
                        "linkedin_profile_registry",
                        row_builder=self._registry_from_row,
                        where_sql=f"profile_url_key IN ({placeholders})",
                        params=canonical_keys,
                        limit=0,
                    )
                    alias_rows = self._select_rows(
                        "linkedin_profile_registry_aliases",
                        row_builder=lambda row: dict(row),
                        where_sql=f"profile_url_key IN ({placeholders})",
                        params=canonical_keys,
                        order_by_sql="updated_at DESC",
                        limit=0,
                    )
                    for alias_row in alias_rows:
                        canonical_key = str(dict(alias_row).get("profile_url_key") or "").strip()
                        alias_url = str(dict(alias_row).get("alias_url") or "").strip()
                        if not canonical_key or not alias_url:
                            continue
                        aliases = aliases_by_canonical.setdefault(canonical_key, [])
                        if alias_url not in aliases:
                            aliases.append(alias_url)
                    for existing_row in existing_rows:
                        payload = dict(existing_row or {})
                        canonical_key = str(payload.get("profile_url_key") or "").strip()
                        if not canonical_key:
                            continue
                        payload["alias_urls"] = list(aliases_by_canonical.get(canonical_key) or [])
                        existing_rows_by_key[canonical_key] = payload

                effective_by_key: dict[str, dict[str, Any]] = {}
                profile_url_by_key: dict[str, str] = {}
                modified_keys: set[str] = set()
                recorded_active_count = 0
                recorded_deferred_count = 0
                for spec in specs:
                    profile_url = str(spec.get("url") or "").strip()
                    normalized_key = str(spec.get("key") or "").strip()
                    canonical_key = str(canonical_keys_by_key.get(normalized_key) or normalized_key).strip()
                    if not canonical_key:
                        continue
                    existing_payload = dict(
                        effective_by_key.get(canonical_key)
                        or existing_rows_by_key.get(canonical_key)
                        or {}
                    )
                    effective_payload = self._compose_effective_payload(
                        existing_payload=existing_payload,
                        normalized_status="",
                        normalized_profile_url=profile_url,
                        normalized_raw_linkedin_url="",
                        normalized_sanity_linkedin_url="",
                        normalized_alias_urls=[profile_url],
                        normalized_run_id="",
                        normalized_dataset_id="",
                        normalized_snapshot_dir=snapshot_dir,
                        normalized_raw_path="",
                        normalized_source_shards=_source_shards_for(profile_url),
                        normalized_source_jobs=normalized_source_jobs,
                        retry_count=None,
                        increment_retry=False,
                        last_error=None,
                        preserve_unrecoverable=True,
                        now_timestamp=planned_at,
                    )
                    current_status = str(effective_payload.get("status") or "").strip().lower()
                    if current_status in {"fetched", "unrecoverable"}:
                        if spec.get("kind") == "active":
                            terminal_skipped_active_count += 1
                        else:
                            terminal_skipped_deferred_count += 1
                        effective_by_key[canonical_key] = effective_payload
                        # Parity with the per-row store-era path: the composed
                        # payload (merged source_jobs/aliases, terminal status
                        # preserved) must still be written — terminal rows keep
                        # accumulating job scope even though their queue state
                        # is untouched. Skipping the upsert under-counts
                        # terminal rows in per-job scope summaries.
                        profile_url_by_key[canonical_key] = profile_url
                        modified_keys.add(canonical_key)
                        continue
                    queue_state = str(spec.get("queue_state") or "").strip()
                    current_refill_queue_state = str(effective_payload.get("refill_queue_state") or "").strip()
                    current_owner = (
                        max(0, int(effective_payload.get("refill_owner_worker_id") or 0)),
                        str(effective_payload.get("refill_owner_run_id") or "").strip(),
                        str(effective_payload.get("refill_owner_dataset_id") or "").strip(),
                        str(effective_payload.get("refill_owner_payload_hash") or "").strip(),
                    )
                    next_owner = (
                        normalized_active_owner_worker_id,
                        normalized_active_owner_run_id,
                        normalized_active_owner_dataset_id,
                        normalized_active_owner_payload_hash,
                    )
                    owner_changed = any(next_owner) and next_owner != current_owner
                    attempt_count = max(0, int(effective_payload.get("last_refill_attempt_count") or 0)) + (
                        1
                        if queue_state == "planned_dispatch"
                        and (current_refill_queue_state != "planned_dispatch" or owner_changed)
                        else 0
                    )
                    owner_worker_id = max(0, int(effective_payload.get("refill_owner_worker_id") or 0))
                    owner_run_id = str(effective_payload.get("refill_owner_run_id") or "").strip()
                    owner_dataset_id = str(effective_payload.get("refill_owner_dataset_id") or "").strip()
                    owner_payload_hash = str(effective_payload.get("refill_owner_payload_hash") or "").strip()
                    terminal_status = str(effective_payload.get("refill_terminal_status") or "").strip()
                    terminal_at = str(effective_payload.get("refill_terminal_at") or "").strip()
                    if queue_state == "planned_dispatch":
                        if normalized_active_owner_worker_id > 0:
                            owner_worker_id = normalized_active_owner_worker_id
                        if normalized_active_owner_run_id:
                            owner_run_id = normalized_active_owner_run_id
                        if normalized_active_owner_dataset_id:
                            owner_dataset_id = normalized_active_owner_dataset_id
                        if normalized_active_owner_payload_hash:
                            owner_payload_hash = normalized_active_owner_payload_hash
                        terminal_status = ""
                        terminal_at = ""
                    elif queue_state in {"dispatch_reserved", "dispatch_claimed"}:
                        owner_worker_id = normalized_active_owner_worker_id
                        owner_run_id = normalized_active_owner_run_id
                        owner_dataset_id = normalized_active_owner_dataset_id
                        owner_payload_hash = normalized_active_owner_payload_hash
                        terminal_status = ""
                        terminal_at = ""
                    elif queue_state in {
                        "deferred_budget",
                        "deferred_coalescing",
                    }:
                        owner_worker_id = 0
                        owner_run_id = ""
                        owner_dataset_id = ""
                        owner_payload_hash = ""
                        terminal_status = ""
                        terminal_at = ""
                    effective_payload.update(
                        {
                            "status": "deferred_coalescing"
                            if queue_state == "deferred_coalescing"
                            else str(effective_payload.get("status") or "queued"),
                            "refill_queue_state": queue_state,
                            "last_refill_trigger_kind": str(trigger_kind or "").strip(),
                            "last_refill_plan_reason": str(plan_reason or "").strip(),
                            "last_refill_deferred_reason": str(spec.get("reason") or "").strip(),
                            "last_refill_planned_at": planned_at,
                            "refill_not_before_at": ""
                            if queue_state == "planned_dispatch"
                            else str(spec.get("not_before_at") or "").strip(),
                            "refill_plan_batch_size": normalized_refill_plan_batch_size,
                            "refill_plan_batch_count": normalized_refill_plan_batch_count,
                            "refill_plan_window_url_count": normalized_refill_plan_window_url_count,
                            "last_refill_attempt_count": attempt_count,
                            "refill_owner_worker_id": owner_worker_id,
                            "refill_owner_run_id": owner_run_id,
                            "refill_owner_dataset_id": owner_dataset_id,
                            "refill_owner_payload_hash": owner_payload_hash,
                            "refill_terminal_status": terminal_status,
                            "refill_terminal_at": terminal_at,
                            "updated_at": planned_at,
                        }
                    )
                    effective_by_key[canonical_key] = effective_payload
                    profile_url_by_key[canonical_key] = profile_url
                    modified_keys.add(canonical_key)
                    if spec.get("kind") == "active":
                        recorded_active_count += 1
                    else:
                        recorded_deferred_count += 1

                registry_rows_to_upsert: list[dict[str, Any]] = []
                alias_rows_to_upsert: list[dict[str, Any]] = []
                alias_written_keys: set[str] = set()
                for canonical_key in canonical_keys:
                    if canonical_key not in modified_keys:
                        continue
                    effective_payload = dict(effective_by_key.get(canonical_key) or {})
                    profile_url = str(
                        effective_payload.get("profile_url") or profile_url_by_key.get(canonical_key) or canonical_key
                    ).strip()
                    registry_rows_to_upsert.append(
                        self._effective_payload_row(canonical_key, profile_url, effective_payload, planned_at)
                    )
                    for alias_url in normalize_linkedin_profile_url_list(
                        [
                            profile_url,
                            *list(effective_payload.get("alias_urls") or []),
                        ]
                    ):
                        alias_key = normalize_linkedin_profile_url_key(alias_url)
                        if not alias_key or alias_key in alias_written_keys:
                            continue
                        alias_written_keys.add(alias_key)
                        alias_rows_to_upsert.append(
                            {
                                "alias_url_key": alias_key,
                                "profile_url_key": canonical_key,
                                "alias_url": alias_url,
                                "alias_kind": "observed",
                                "created_at": str(effective_payload.get("created_at") or planned_at),
                                "updated_at": planned_at,
                            }
                        )
                if registry_rows_to_upsert:
                    self._adapter.bulk_upsert_rows(
                        "linkedin_profile_registry",
                        registry_rows_to_upsert,
                    )
                if alias_rows_to_upsert:
                    self._adapter.bulk_upsert_rows(
                        "linkedin_profile_registry_aliases",
                        alias_rows_to_upsert,
                    )
                recorded_item_count = recorded_active_count + recorded_deferred_count
                terminal_skipped_count = terminal_skipped_active_count + terminal_skipped_deferred_count
                return {
                    "status": "recorded" if recorded_item_count > 0 else "skipped",
                    "reason": "" if recorded_item_count > 0 else "terminal_items_already_closed",
                    "item_store": "linkedin_profile_registry",
                    "active_item_count": recorded_active_count,
                    "deferred_item_count": recorded_deferred_count,
                    "requested_active_item_count": len(active_urls),
                    "requested_deferred_item_count": len(deferred_urls),
                    "terminal_skipped_item_count": terminal_skipped_count,
                    "terminal_skipped_active_item_count": terminal_skipped_active_count,
                    "terminal_skipped_deferred_item_count": terminal_skipped_deferred_count,
                    "trigger_kind": str(trigger_kind or "").strip(),
                    "plan_reason": str(plan_reason or "").strip(),
                    "active_queue_state": normalized_active_queue_state,
                    "active_reason": resolved_active_reason,
                    "active_refill_not_before_at": normalized_active_refill_not_before_at,
                    "active_owner_worker_id": normalized_active_owner_worker_id,
                    "active_owner_run_id": normalized_active_owner_run_id,
                    "active_owner_dataset_id": normalized_active_owner_dataset_id,
                    "active_owner_payload_hash": normalized_active_owner_payload_hash,
                    "deferred_reason": resolved_deferred_reason,
                    "deferred_queue_state": normalized_deferred_queue_state,
                    "planned_at": planned_at,
                    "refill_not_before_at": normalized_refill_not_before_at,
                }
            except Exception as exc:
                if self._strict_authoritative("linkedin_profile_registry"):
                    self._raise_write_failure(
                        table_name="linkedin_profile_registry",
                        method_name="record_refill_plan_items",
                        reason=f"{type(exc).__name__}: {exc}",
                        error=exc,
                    )
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry", method_name="record_refill_plan_items"
        )
        return {}  # unreachable — the invariant raise above never returns

    def list_refill_queue_items(
        self,
        *,
        states: list[str] | tuple[str, ...] | set[str] | None = None,
        source_job: str = "",
        snapshot_dir: str = "",
        limit: int = 100,
        ready_only: bool = True,
    ) -> list[dict[str, Any]]:
        normalized_states = [
            str(item or "").strip()
            for item in list(states or ["deferred_budget"])
            if str(item or "").strip()
        ]
        if not normalized_states:
            return []
        normalized_limit = max(1, min(10000, int(limit or 100)))
        normalized_source_job = str(source_job or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        normalized_ready_only = bool(ready_only)
        ready_clause_postgres = "(coalesce(refill_not_before_at, '') = '' OR refill_not_before_at <= %s)"
        provider_owned_states = {"planned_dispatch"}
        include_provider_owned_states = any(state in provider_owned_states for state in normalized_states)
        if normalized_ready_only and include_provider_owned_states:
            return []
        now_timestamp = utc_now_timestamp()
        placeholders = ", ".join("%s" for _ in normalized_states)
        where_sql = f"refill_queue_state IN ({placeholders})"
        params: list[Any] = [*normalized_states]
        if normalized_ready_only:
            where_sql = f"{where_sql} AND {ready_clause_postgres}"
            params.append(now_timestamp)
        rows = self._select_rows(
            "linkedin_profile_registry",
            row_builder=self._registry_from_row,
            where_sql=where_sql,
            params=params,
            order_by_sql="refill_not_before_at ASC, last_refill_planned_at ASC, updated_at ASC",
            limit=normalized_limit * 3,
        )
        return self._filter_refill_queue_rows(
            rows,
            source_job=normalized_source_job,
            snapshot_dir=normalized_snapshot_dir,
            limit=normalized_limit,
        )

    def list_refill_queue_groups(
        self,
        *,
        states: list[str] | tuple[str, ...] | set[str] | None = None,
        source_job: str = "",
        limit: int = 20,
        item_limit_per_group: int = 200,
    ) -> list[dict[str, Any]]:
        """Return durable refill work grouped by the workflow scope that owns it.

        The registry row remains the canonical item store; this helper only groups
        rows so daemon ticks can wake a job/snapshot refill without reconstructing
        queue state from artifacts.
        """

        normalized_states = [
            str(item or "").strip()
            for item in list(states or ["deferred_budget"])
            if str(item or "").strip()
        ]
        if not normalized_states:
            return []
        normalized_limit = max(1, min(100, int(limit or 20)))
        normalized_item_limit = max(1, min(10000, int(item_limit_per_group or 200)))
        normalized_source_job = str(source_job or "").strip()
        row_limit = max(normalized_limit * normalized_item_limit, normalized_item_limit)
        ready_clause_postgres = "(coalesce(refill_not_before_at, '') = '' OR refill_not_before_at <= %s)"
        now_timestamp = utc_now_timestamp()
        placeholders = ", ".join("%s" for _ in normalized_states)
        rows = self._select_rows(
            "linkedin_profile_registry",
            row_builder=self._registry_from_row,
            where_sql=f"refill_queue_state IN ({placeholders}) AND {ready_clause_postgres}",
            params=[*normalized_states, now_timestamp],
            order_by_sql="refill_not_before_at ASC, last_refill_planned_at ASC, updated_at ASC",
            limit=row_limit,
        )
        return self._group_refill_queue_rows(
            rows,
            source_job=normalized_source_job,
            group_limit=normalized_limit,
            item_limit_per_group=normalized_item_limit,
        )

    @staticmethod
    def _filter_refill_queue_rows(
        rows: list[dict[str, Any]],
        *,
        source_job: str,
        snapshot_dir: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        filtered: list[dict[str, Any]] = []
        normalized_limit = max(1, int(limit or 1))
        for row in list(rows or []):
            payload = dict(row or {})
            if source_job and source_job not in {str(item or "").strip() for item in list(payload.get("source_jobs") or [])}:
                continue
            if snapshot_dir and str(payload.get("last_snapshot_dir") or "").strip() != snapshot_dir:
                continue
            filtered.append(payload)
            if len(filtered) >= normalized_limit:
                break
        return filtered

    @staticmethod
    def _group_refill_queue_rows(
        rows: list[dict[str, Any]],
        *,
        source_job: str,
        group_limit: int,
        item_limit_per_group: int,
    ) -> list[dict[str, Any]]:
        groups: list[dict[str, Any]] = []
        groups_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        normalized_group_limit = max(1, int(group_limit or 1))
        normalized_item_limit = max(1, int(item_limit_per_group or 1))
        normalized_source_job = str(source_job or "").strip()
        for row in list(rows or []):
            payload = dict(row or {})
            row_source_jobs = [
                str(item or "").strip()
                for item in list(payload.get("source_jobs") or [])
                if str(item or "").strip()
            ]
            snapshot_dir = str(payload.get("last_snapshot_dir") or "").strip()
            if not snapshot_dir:
                continue
            candidate_jobs = [normalized_source_job] if normalized_source_job else row_source_jobs
            for job_id in candidate_jobs:
                if not job_id:
                    continue
                if row_source_jobs and job_id not in row_source_jobs:
                    continue
                key = (job_id, snapshot_dir)
                group = groups_by_key.get(key)
                if group is None:
                    if len(groups) >= normalized_group_limit:
                        continue
                    group = {
                        "source_job": job_id,
                        "snapshot_dir": snapshot_dir,
                        "states": {},
                        "item_count": 0,
                        "items": [],
                    }
                    groups_by_key[key] = group
                    groups.append(group)
                if int(group.get("item_count") or 0) >= normalized_item_limit:
                    continue
                items = list(group.get("items") or [])
                items.append(payload)
                group["items"] = items
                group["item_count"] = len(items)
                state = str(payload.get("refill_queue_state") or "").strip()
                states = dict(group.get("states") or {})
                if state:
                    states[state] = int(states.get(state) or 0) + 1
                group["states"] = states
        return groups

    # --- read-merge-write core ---

    def _compose_effective_payload(
        self,
        *,
        existing_payload: dict[str, Any],
        normalized_status: str,
        normalized_profile_url: str,
        normalized_raw_linkedin_url: str,
        normalized_sanity_linkedin_url: str,
        normalized_alias_urls: list[str],
        normalized_run_id: str,
        normalized_dataset_id: str,
        normalized_snapshot_dir: str,
        normalized_raw_path: str,
        normalized_source_shards: list[str],
        normalized_source_jobs: list[str],
        retry_count: int | None,
        increment_retry: bool,
        last_error: str | None,
        preserve_unrecoverable: bool,
        now_timestamp: str,
    ) -> dict[str, Any]:
        existing = dict(existing_payload or {})
        existing_status = str(existing.get("status") or "").strip()
        merged_source_shards = _merge_registry_label_lists(
            list(existing.get("source_shards") or []),
            normalized_source_shards,
        )
        merged_source_jobs = _merge_registry_label_lists(
            list(existing.get("source_jobs") or []),
            normalized_source_jobs,
        )
        effective_status = normalized_status or existing_status or "queued"
        existing_raw_path = str(existing.get("last_raw_path") or "").strip()
        if preserve_unrecoverable and existing_status == "unrecoverable" and effective_status != "fetched":
            effective_status = "unrecoverable"
        if normalized_status == "unrecoverable":
            effective_status = "unrecoverable"
        if normalized_status == "fetched":
            effective_status = "fetched"
        if normalized_status in {"queued", "deferred_coalescing"} and existing_status == "fetched" and existing_raw_path:
            effective_status = "fetched"
        if retry_count is not None:
            effective_retry_count = max(0, int(retry_count))
        else:
            effective_retry_count = max(0, int(existing.get("retry_count") or 0))
            if increment_retry:
                effective_retry_count += 1
        effective_last_error = str(last_error) if last_error is not None else str(existing.get("last_error") or "")
        if normalized_status == "fetched":
            effective_last_error = ""
        effective_profile_url = normalized_profile_url or str(existing.get("profile_url") or "")
        effective_raw_linkedin_url = normalized_raw_linkedin_url or str(existing.get("raw_linkedin_url") or "")
        effective_sanity_linkedin_url = normalized_sanity_linkedin_url or str(existing.get("sanity_linkedin_url") or "")
        effective_run_id = normalized_run_id or str(existing.get("last_run_id") or "")
        effective_dataset_id = normalized_dataset_id or str(existing.get("last_dataset_id") or "")
        effective_snapshot_dir = normalized_snapshot_dir or str(existing.get("last_snapshot_dir") or "")
        effective_raw_path = normalized_raw_path or str(existing.get("last_raw_path") or "")
        effective_first_queued_at = str(existing.get("first_queued_at") or "")
        effective_last_queued_at = str(existing.get("last_queued_at") or "")
        effective_last_fetched_at = str(existing.get("last_fetched_at") or "")
        effective_last_failed_at = str(existing.get("last_failed_at") or "")
        if normalized_status == "queued":
            if not effective_first_queued_at:
                effective_first_queued_at = now_timestamp
            effective_last_queued_at = now_timestamp
        if normalized_status == "fetched":
            effective_last_fetched_at = now_timestamp
        if normalized_status in {"failed_retryable", "unrecoverable"}:
            effective_last_failed_at = now_timestamp
        effective_alias_urls = normalize_linkedin_profile_url_list(
            [
                *list(existing.get("alias_urls") or []),
                *normalized_alias_urls,
                effective_profile_url,
                effective_raw_linkedin_url,
                effective_sanity_linkedin_url,
            ]
        )
        effective_refill_queue_state = str(existing.get("refill_queue_state") or "")
        effective_refill_not_before_at = str(existing.get("refill_not_before_at") or "")
        effective_refill_plan_batch_size = max(0, int(existing.get("refill_plan_batch_size") or 0))
        effective_refill_plan_batch_count = max(0, int(existing.get("refill_plan_batch_count") or 0))
        effective_refill_plan_window_url_count = max(
            0,
            int(existing.get("refill_plan_window_url_count") or 0),
        )
        effective_refill_terminal_status = str(existing.get("refill_terminal_status") or "")
        effective_refill_terminal_at = str(existing.get("refill_terminal_at") or "")
        if effective_status in {"fetched", "unrecoverable"}:
            effective_refill_queue_state = ""
            effective_refill_not_before_at = ""
            effective_refill_plan_batch_size = 0
            effective_refill_plan_batch_count = 0
            effective_refill_plan_window_url_count = 0
            effective_refill_terminal_status = (
                effective_refill_terminal_status
                or ("completed" if effective_status == "fetched" else "terminal_failed")
            )
            effective_refill_terminal_at = effective_refill_terminal_at or now_timestamp
        elif normalized_status == "failed_retryable":
            effective_refill_terminal_status = "retryable_failed"
            effective_refill_terminal_at = now_timestamp
        if normalized_status == "failed_retryable" and (not merged_source_jobs or not effective_snapshot_dir):
            effective_refill_queue_state = ""
            effective_refill_not_before_at = ""
        return {
            "profile_url": effective_profile_url,
            "raw_linkedin_url": effective_raw_linkedin_url,
            "sanity_linkedin_url": effective_sanity_linkedin_url,
            "status": effective_status,
            "retry_count": effective_retry_count,
            "last_error": effective_last_error,
            "last_run_id": effective_run_id,
            "last_dataset_id": effective_dataset_id,
            "last_snapshot_dir": effective_snapshot_dir,
            "last_raw_path": effective_raw_path,
            "first_queued_at": effective_first_queued_at,
            "last_queued_at": effective_last_queued_at,
            "last_fetched_at": effective_last_fetched_at,
            "last_failed_at": effective_last_failed_at,
            "source_shards": merged_source_shards,
            "source_jobs": merged_source_jobs,
            "refill_queue_state": effective_refill_queue_state,
            "last_refill_trigger_kind": str(existing.get("last_refill_trigger_kind") or ""),
            "last_refill_plan_reason": str(existing.get("last_refill_plan_reason") or ""),
            "last_refill_deferred_reason": str(existing.get("last_refill_deferred_reason") or ""),
            "last_refill_planned_at": str(existing.get("last_refill_planned_at") or ""),
            "refill_not_before_at": effective_refill_not_before_at,
            "refill_plan_batch_size": effective_refill_plan_batch_size,
            "refill_plan_batch_count": effective_refill_plan_batch_count,
            "refill_plan_window_url_count": effective_refill_plan_window_url_count,
            "last_refill_attempt_count": max(0, int(existing.get("last_refill_attempt_count") or 0)),
            "refill_owner_worker_id": max(0, int(existing.get("refill_owner_worker_id") or 0)),
            "refill_owner_run_id": str(existing.get("refill_owner_run_id") or ""),
            "refill_owner_dataset_id": str(existing.get("refill_owner_dataset_id") or ""),
            "refill_owner_payload_hash": str(existing.get("refill_owner_payload_hash") or ""),
            "refill_terminal_status": effective_refill_terminal_status,
            "refill_terminal_at": effective_refill_terminal_at,
            "alias_urls": effective_alias_urls,
            "created_at": str(existing.get("created_at") or now_timestamp),
            "updated_at": now_timestamp,
        }

    def _effective_payload_row(
        self,
        canonical_key: str,
        profile_url: str,
        effective_payload: dict[str, Any],
        now_timestamp: str,
    ) -> dict[str, Any]:
        """Persisted-row payload for a composed effective payload (shared by every bulk write path)."""

        return self._row_payload(
            profile_url_key=canonical_key,
            profile_url=profile_url,
            raw_linkedin_url=str(effective_payload.get("raw_linkedin_url") or ""),
            sanity_linkedin_url=str(effective_payload.get("sanity_linkedin_url") or ""),
            status=str(effective_payload.get("status") or "queued"),
            retry_count=int(effective_payload.get("retry_count") or 0),
            last_error=str(effective_payload.get("last_error") or ""),
            last_run_id=str(effective_payload.get("last_run_id") or ""),
            last_dataset_id=str(effective_payload.get("last_dataset_id") or ""),
            last_snapshot_dir=str(effective_payload.get("last_snapshot_dir") or ""),
            last_raw_path=str(effective_payload.get("last_raw_path") or ""),
            first_queued_at=str(effective_payload.get("first_queued_at") or ""),
            last_queued_at=str(effective_payload.get("last_queued_at") or ""),
            last_fetched_at=str(effective_payload.get("last_fetched_at") or ""),
            last_failed_at=str(effective_payload.get("last_failed_at") or ""),
            source_shards=list(effective_payload.get("source_shards") or []),
            source_jobs=list(effective_payload.get("source_jobs") or []),
            refill_queue_state=str(effective_payload.get("refill_queue_state") or ""),
            last_refill_trigger_kind=str(effective_payload.get("last_refill_trigger_kind") or ""),
            last_refill_plan_reason=str(effective_payload.get("last_refill_plan_reason") or ""),
            last_refill_deferred_reason=str(effective_payload.get("last_refill_deferred_reason") or ""),
            last_refill_planned_at=str(effective_payload.get("last_refill_planned_at") or ""),
            refill_not_before_at=str(effective_payload.get("refill_not_before_at") or ""),
            refill_plan_batch_size=int(effective_payload.get("refill_plan_batch_size") or 0),
            refill_plan_batch_count=int(effective_payload.get("refill_plan_batch_count") or 0),
            refill_plan_window_url_count=int(effective_payload.get("refill_plan_window_url_count") or 0),
            last_refill_attempt_count=int(effective_payload.get("last_refill_attempt_count") or 0),
            refill_owner_worker_id=int(effective_payload.get("refill_owner_worker_id") or 0),
            refill_owner_run_id=str(effective_payload.get("refill_owner_run_id") or ""),
            refill_owner_dataset_id=str(effective_payload.get("refill_owner_dataset_id") or ""),
            refill_owner_payload_hash=str(effective_payload.get("refill_owner_payload_hash") or ""),
            refill_terminal_status=str(effective_payload.get("refill_terminal_status") or ""),
            refill_terminal_at=str(effective_payload.get("refill_terminal_at") or ""),
            created_at=str(effective_payload.get("created_at") or now_timestamp),
            updated_at=str(effective_payload.get("updated_at") or now_timestamp),
        )

    # --- backfill batch ---

    def backfill_batch(
        self,
        entries: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        normalized_entries = [
            _normalize_backfill_entry(item)
            for item in list(entries or [])
        ]
        normalized_entries = [item for item in normalized_entries if item is not None]
        if not normalized_entries:
            return 0
        if self._should_prefer_read("linkedin_profile_registry"):
            return self._backfill_batch_postgres(normalized_entries)
        processed = 0
        for entry in normalized_entries:
            if str(entry.get("status") or "") == "fetched":
                self.mark_fetched(
                    str(entry.get("profile_url") or ""),
                    raw_path=str(entry.get("raw_path") or ""),
                    source_shards=list(entry.get("source_shards") or []),
                    source_jobs=list(entry.get("source_jobs") or []),
                    alias_urls=list(entry.get("alias_urls") or []),
                    raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                    sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                    run_id=str(entry.get("run_id") or ""),
                    dataset_id=str(entry.get("dataset_id") or ""),
                    snapshot_dir=str(entry.get("snapshot_dir") or ""),
                )
            else:
                self.mark_failed(
                    str(entry.get("profile_url") or ""),
                    error=str(entry.get("error") or ""),
                    retryable=bool(entry.get("retryable")),
                    source_shards=list(entry.get("source_shards") or []),
                    source_jobs=list(entry.get("source_jobs") or []),
                    alias_urls=list(entry.get("alias_urls") or []),
                    raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                    sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                    run_id=str(entry.get("run_id") or ""),
                    dataset_id=str(entry.get("dataset_id") or ""),
                    snapshot_dir=str(entry.get("snapshot_dir") or ""),
                )
            processed += 1
        return processed

    def _backfill_batch_postgres(
        self,
        entries: list[dict[str, Any]],
    ) -> int:
        grouped_entries: dict[str, list[dict[str, Any]]] = {}
        alias_to_group: dict[str, str] = {}
        group_order: list[str] = []
        for entry in entries:
            alias_keys = [str(item).strip() for item in list(entry.get("alias_keys") or []) if str(item).strip()]
            related_groups = _dedupe_preserve_order(
                [alias_to_group.get(alias_key, "") for alias_key in alias_keys if alias_to_group.get(alias_key, "")]
            )
            if related_groups:
                primary_group = related_groups[0]
                if primary_group not in grouped_entries:
                    grouped_entries[primary_group] = []
                    group_order.append(primary_group)
                grouped_entries[primary_group].append(entry)
                for other_group in related_groups[1:]:
                    if other_group == primary_group or other_group not in grouped_entries:
                        continue
                    grouped_entries[primary_group].extend(grouped_entries.pop(other_group))
                    if other_group in group_order:
                        group_order.remove(other_group)
                for alias_key in alias_keys:
                    alias_to_group[alias_key] = primary_group
                for merged_entry in grouped_entries[primary_group]:
                    for alias_key in list(merged_entry.get("alias_keys") or []):
                        if str(alias_key or "").strip():
                            alias_to_group[str(alias_key).strip()] = primary_group
                continue
            primary_group = str(entry.get("profile_url_key") or "") or alias_keys[0]
            grouped_entries[primary_group] = [entry]
            group_order.append(primary_group)
            for alias_key in alias_keys:
                alias_to_group[alias_key] = primary_group

        all_alias_keys = _dedupe_preserve_order(
            [
                str(alias_key).strip()
                for group_key in group_order
                for entry in grouped_entries.get(group_key, [])
                for alias_key in list(entry.get("alias_keys") or [])
                if str(alias_key).strip()
            ]
        )
        alias_rows: list[dict[str, Any]] = []
        alias_rows_by_profile: dict[str, list[str]] = {}
        existing_alias_map: dict[str, str] = {}
        if all_alias_keys:
            where_sql = "alias_url_key IN (" + ", ".join(["%s"] * len(all_alias_keys)) + ")"
            alias_rows = self._adapter.select_many(
                "linkedin_profile_registry_aliases",
                where_sql=where_sql,
                params=all_alias_keys,
                order_by_sql="updated_at DESC",
                limit=max(100, len(all_alias_keys) * 2),
            )
            for row in alias_rows:
                alias_key = str(row.get("alias_url_key") or "").strip()
                canonical_key = str(row.get("profile_url_key") or "").strip()
                alias_url = str(row.get("alias_url") or "").strip()
                if alias_key and canonical_key and alias_key not in existing_alias_map:
                    existing_alias_map[alias_key] = canonical_key
                if canonical_key and alias_url:
                    aliases = alias_rows_by_profile.setdefault(canonical_key, [])
                    if alias_url not in aliases:
                        aliases.append(alias_url)

        canonical_keys = _dedupe_preserve_order(
            [
                next(
                    (
                        existing_alias_map.get(alias_key, "")
                        for entry in grouped_entries.get(group_key, [])
                        for alias_key in list(entry.get("alias_keys") or [])
                        if existing_alias_map.get(alias_key, "")
                    ),
                    str(grouped_entries.get(group_key, [{}])[0].get("profile_url_key") or ""),
                )
                for group_key in group_order
            ]
        )
        existing_rows_by_key: dict[str, dict[str, Any]] = {}
        if canonical_keys:
            where_sql = "profile_url_key IN (" + ", ".join(["%s"] * len(canonical_keys)) + ")"
            existing_rows = self._adapter.select_many(
                "linkedin_profile_registry",
                where_sql=where_sql,
                params=canonical_keys,
                limit=max(100, len(canonical_keys) * 2),
            )
            for row in existing_rows:
                payload = self._registry_from_row(row)
                canonical_key = str(payload.get("profile_url_key") or "").strip()
                if not canonical_key:
                    continue
                payload["alias_urls"] = list(alias_rows_by_profile.get(canonical_key, []))
                existing_rows_by_key[canonical_key] = payload

        registry_rows_to_upsert: list[dict[str, Any]] = []
        alias_rows_to_upsert: list[dict[str, Any]] = []
        alias_written_keys: set[str] = set()
        for group_key in group_order:
            batch_entries = list(grouped_entries.get(group_key, []))
            if not batch_entries:
                continue
            canonical_key = next(
                (
                    existing_alias_map.get(alias_key, "")
                    for entry in batch_entries
                    for alias_key in list(entry.get("alias_keys") or [])
                    if existing_alias_map.get(alias_key, "")
                ),
                str(batch_entries[0].get("profile_url_key") or ""),
            )
            if not canonical_key:
                continue
            effective_payload = dict(existing_rows_by_key.get(canonical_key) or {})
            for entry in batch_entries:
                entry_status = str(entry.get("status") or "")
                entry_retryable = bool(entry.get("retryable"))
                retry_count_before = max(0, int(effective_payload.get("retry_count") or 0))
                retry_attempt_budget = linkedin_profile_max_retry_attempts()
                retry_queue_allowed = (
                    entry_status == "failed_retryable"
                    and entry_retryable
                    and retry_count_before < retry_attempt_budget
                )
                effective_status = (
                    "unrecoverable"
                    if entry_status == "failed_retryable" and entry_retryable and not retry_queue_allowed
                    else entry_status
                )
                now_timestamp = utc_now_timestamp()
                effective_payload = self._compose_effective_payload(
                    existing_payload=effective_payload,
                    normalized_status=effective_status,
                    normalized_profile_url=str(entry.get("profile_url") or ""),
                    normalized_raw_linkedin_url=str(entry.get("raw_linkedin_url") or ""),
                    normalized_sanity_linkedin_url=str(entry.get("sanity_linkedin_url") or ""),
                    normalized_alias_urls=list(entry.get("alias_urls") or []),
                    normalized_run_id=str(entry.get("run_id") or ""),
                    normalized_dataset_id=str(entry.get("dataset_id") or ""),
                    normalized_snapshot_dir=str(entry.get("snapshot_dir") or ""),
                    normalized_raw_path=str(entry.get("raw_path") or ""),
                    normalized_source_shards=list(entry.get("source_shards") or []),
                    normalized_source_jobs=list(entry.get("source_jobs") or []),
                    retry_count=0 if entry_status == "fetched" else None,
                    increment_retry=entry_retryable,
                    last_error=str(entry.get("error") or ""),
                    preserve_unrecoverable=not entry_retryable,
                    now_timestamp=now_timestamp,
                )
                retry_source_jobs = [
                    str(item or "").strip()
                    for item in list(effective_payload.get("source_jobs") or [])
                    if str(item or "").strip()
                ]
                retry_snapshot_dir = str(effective_payload.get("last_snapshot_dir") or "").strip()
                if retry_queue_allowed and retry_source_jobs and retry_snapshot_dir:
                    retry_not_before_at = (
                        datetime.now(timezone.utc).replace(microsecond=0)
                        + timedelta(seconds=30)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    effective_payload.update(
                        {
                            "status": "failed_retryable",
                            "refill_queue_state": "retry_wait",
                            "last_refill_trigger_kind": "profile_retry",
                            "last_refill_plan_reason": "profile_retry_wait",
                            "last_refill_deferred_reason": str(
                                entry.get("error") or "profile_retryable_failure"
                            ).strip()
                            or "profile_retryable_failure",
                            "last_refill_planned_at": now_timestamp,
                            "refill_not_before_at": retry_not_before_at,
                            "refill_plan_batch_size": 0,
                            "refill_plan_batch_count": 0,
                            "refill_plan_window_url_count": 0,
                            "refill_owner_worker_id": 0,
                            "refill_owner_run_id": "",
                            "refill_owner_dataset_id": "",
                            "refill_owner_payload_hash": "",
                            "refill_terminal_status": "retryable_failed",
                            "refill_terminal_at": now_timestamp,
                            "updated_at": now_timestamp,
                        }
                    )
            registry_rows_to_upsert.append(
                self._effective_payload_row(
                    canonical_key,
                    str(effective_payload.get("profile_url") or canonical_key),
                    effective_payload,
                    utc_now_timestamp(),
                )
            )
            for alias_url in normalize_linkedin_profile_url_list(
                [
                    str(effective_payload.get("profile_url") or canonical_key),
                    *list(effective_payload.get("alias_urls") or []),
                ]
            ):
                alias_key = normalize_linkedin_profile_url_key(alias_url)
                if not alias_key or alias_key in alias_written_keys:
                    continue
                alias_written_keys.add(alias_key)
                alias_rows_to_upsert.append(
                    {
                        "alias_url_key": alias_key,
                        "profile_url_key": canonical_key,
                        "alias_url": alias_url,
                        "alias_kind": "observed",
                        "created_at": str(effective_payload.get("created_at") or utc_now_timestamp()),
                        "updated_at": str(effective_payload.get("updated_at") or utc_now_timestamp()),
                    }
                )

        if registry_rows_to_upsert:
            self._adapter.bulk_upsert_rows("linkedin_profile_registry", registry_rows_to_upsert)
        if alias_rows_to_upsert:
            self._adapter.bulk_upsert_rows("linkedin_profile_registry_aliases", alias_rows_to_upsert)
        return len(entries)

    def _upsert_registry(
        self,
        *,
        profile_url: str,
        status: str,
        source_shards: list[str] | None = None,
        source_jobs: list[str] | None = None,
        alias_urls: list[str] | None = None,
        raw_linkedin_url: str = "",
        sanity_linkedin_url: str = "",
        alias_kind: str = "observed",
        run_id: str = "",
        dataset_id: str = "",
        snapshot_dir: str = "",
        raw_path: str = "",
        retry_count: int | None = None,
        increment_retry: bool = False,
        last_error: str | None = None,
        preserve_unrecoverable: bool = True,
    ) -> dict[str, Any] | None:
        normalized_key = normalize_linkedin_profile_url_key(profile_url)
        normalized_profile_url = str(profile_url or "").strip()
        if not normalized_key:
            return None
        normalized_raw_linkedin_url = str(raw_linkedin_url or "").strip()
        normalized_sanity_linkedin_url = str(sanity_linkedin_url or "").strip()
        normalized_alias_urls = normalize_linkedin_profile_url_list(
            [
                *list(alias_urls or []),
                normalized_profile_url,
                normalized_raw_linkedin_url,
                normalized_sanity_linkedin_url,
            ]
        )
        normalized_status = str(status or "").strip()
        normalized_run_id = str(run_id or "").strip()
        normalized_dataset_id = str(dataset_id or "").strip()
        normalized_snapshot_dir = str(snapshot_dir or "").strip()
        normalized_raw_path = str(raw_path or "").strip()
        normalized_source_shards = _normalize_registry_label_list(source_shards)
        normalized_source_jobs = _normalize_registry_label_list(source_jobs)
        now_timestamp = utc_now_timestamp()
        if self._should_prefer_read("linkedin_profile_registry"):
            canonical_key = self._resolve_key(normalized_key)
            existing_payload = self.get(normalized_key) or {}
            effective_payload = self._compose_effective_payload(
                existing_payload=existing_payload,
                normalized_status=normalized_status,
                normalized_profile_url=normalized_profile_url,
                normalized_raw_linkedin_url=normalized_raw_linkedin_url,
                normalized_sanity_linkedin_url=normalized_sanity_linkedin_url,
                normalized_alias_urls=normalized_alias_urls,
                normalized_run_id=normalized_run_id,
                normalized_dataset_id=normalized_dataset_id,
                normalized_snapshot_dir=normalized_snapshot_dir,
                normalized_raw_path=normalized_raw_path,
                normalized_source_shards=normalized_source_shards,
                normalized_source_jobs=normalized_source_jobs,
                retry_count=retry_count,
                increment_retry=increment_retry,
                last_error=last_error,
                preserve_unrecoverable=preserve_unrecoverable,
                now_timestamp=now_timestamp,
            )

            row_payload = self._effective_payload_row(
                canonical_key,
                str(effective_payload.get("profile_url") or ""),
                effective_payload,
                now_timestamp,
            )
            self._write_row("linkedin_profile_registry", row_payload)
            canonical_profile_url = str(effective_payload.get("profile_url") or canonical_key).strip()
            self.upsert_aliases(
                canonical_profile_url,
                list(effective_payload.get("alias_urls") or normalized_alias_urls),
                alias_kind=alias_kind,
            )
            return self.get(canonical_profile_url or normalized_key)
        self._raise_postgres_only_invariant(
            table_name="linkedin_profile_registry", method_name="_upsert_registry"
        )
        return None  # unreachable — the invariant raise above never returns
