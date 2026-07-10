"""Track B 2.2 - manual-review queue repository (verbatim port of the store methods).

This module is the PUBLIC data API for ``manual_review_items``.  It preserves the seven retired
``ControlPlaneStore`` operations exactly, including fail-closed authority checks, read-after-write race
behavior, cross-job supersession, snapshot cleanup, and the historical ``[]``/``0``/``None`` sentinels.

The row mapper intentionally remains hand-written.  Nullable scalar columns pass ``None`` through and
the JSON columns use direct ``json.loads`` calls whose malformed-input behavior differs from the current
``TableDescriptor`` kinds.  Descriptor migration therefore remains deferred until the typed/nullable
column work in Track B ③.
"""

from __future__ import annotations

import json
from typing import Any

from ..asset_paths import extract_company_snapshot_ref
from ..control_plane_repository import Repository
from ..control_plane_time import utc_now_timestamp


def _prepare_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for item in items:
        normalized = _normalize_item_payload(item)
        key = _scope_key(normalized)
        if not key:
            continue
        deduped[key] = normalized
    return list(deduped.values())


def _normalize_item_payload(item: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(item or {})
    candidate = dict(normalized.get("candidate") or {})
    evidence = list(normalized.get("evidence") or [])
    metadata = dict(normalized.get("metadata") or {})
    snapshot_id = _infer_snapshot_id({"candidate": candidate, "evidence": evidence, "metadata": metadata})
    if snapshot_id:
        metadata["snapshot_id"] = snapshot_id
    normalized["candidate"] = candidate
    normalized["evidence"] = evidence
    normalized["metadata"] = metadata
    return normalized


def _scope_key(payload: dict[str, Any], *, include_snapshot: bool = True) -> tuple[Any, ...]:
    target_company = str(payload.get("target_company") or "").strip().lower()
    candidate_id = str(
        payload.get("candidate_id") or (payload.get("candidate") or {}).get("candidate_id") or ""
    ).strip()
    review_type = str(payload.get("review_type") or "").strip()
    if not target_company or not candidate_id or not review_type:
        return ()
    if not include_snapshot:
        return (target_company, candidate_id, review_type)
    snapshot_id = _infer_snapshot_id(payload)
    return (target_company, candidate_id, review_type, snapshot_id)


def _infer_snapshot_id(payload: dict[str, Any]) -> str:
    metadata = dict(payload.get("metadata") or {})
    candidate = dict(payload.get("candidate") or {})
    evidence = list(payload.get("evidence") or [])
    for value in [
        metadata.get("snapshot_id"),
        metadata.get("source_snapshot_id"),
        dict(candidate.get("metadata") or {}).get("snapshot_id"),
    ]:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    for value in [
        candidate.get("source_path"),
        candidate.get("metadata", {}).get("source_path") if isinstance(candidate.get("metadata"), dict) else "",
    ]:
        snapshot_id = _snapshot_id_from_path(str(value or ""))
        if snapshot_id:
            return snapshot_id
    for item in evidence:
        item_metadata = dict(item.get("metadata") or {})
        snapshot_id = str(item_metadata.get("snapshot_id") or "").strip()
        if snapshot_id:
            return snapshot_id
        snapshot_id = _snapshot_id_from_path(str(item.get("source_path") or ""))
        if snapshot_id:
            return snapshot_id
    return ""


def _snapshot_id_from_path(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    snapshot_ref = extract_company_snapshot_ref(normalized)
    return str(snapshot_ref[1] if snapshot_ref is not None else "")


def _append_review_note(existing_notes: Any, message: str) -> str:
    normalized_message = str(message or "").strip()
    normalized_existing = str(existing_notes or "").strip()
    if not normalized_message:
        return normalized_existing
    if not normalized_existing:
        return normalized_message
    if normalized_message in normalized_existing:
        return normalized_existing
    return f"{normalized_existing} | {normalized_message}"


class ManualReviewRepository(Repository):
    """PG-only repository for the manual-review queue."""

    def replace_items(self, job_id: str, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        normalized_items = _prepare_items(items)
        scope_keys = {_scope_key(item) for item in normalized_items if _scope_key(item)}
        legacy_scope_keys = {
            _scope_key(item, include_snapshot=False)
            for item in normalized_items
            if _scope_key(item, include_snapshot=False)
        }
        if self._should_prefer_read("manual_review_items"):
            self._call_native_write(
                "delete_rows",
                table_name="manual_review_items",
                where_sql="job_id = %s",
                params=[job_id],
            )
            if legacy_scope_keys:
                existing_rows = self._select_rows(
                    "manual_review_items",
                    row_builder=self._item_from_row,
                    where_sql="status = %s",
                    params=["open"],
                    order_by_sql="updated_at DESC, review_item_id DESC",
                    limit=0,
                )
                for existing_row in existing_rows:
                    if str(existing_row.get("job_id") or "") == job_id:
                        continue
                    row_scope = _scope_key(existing_row)
                    row_legacy_scope = _scope_key(existing_row, include_snapshot=False)
                    if row_scope not in scope_keys and row_legacy_scope not in legacy_scope_keys:
                        continue
                    self._call_native_write(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=int(existing_row.get("review_item_id") or 0),
                        row={
                            "status": "superseded",
                            "review_notes": _append_review_note(
                                existing_row.get("review_notes"),
                                "Superseded by a newer manual-review queue item.",
                            ),
                            "updated_at": utc_now_timestamp(),
                        },
                    )
            for item in normalized_items:
                now = utc_now_timestamp()
                self._call_native_write(
                    "insert_row_with_generated_id",
                    table_name="manual_review_items",
                    row={
                        "job_id": job_id,
                        "candidate_id": str(item.get("candidate_id") or ""),
                        "target_company": str(item.get("target_company") or ""),
                        "review_type": str(item.get("review_type") or ""),
                        "priority": str(item.get("priority") or "medium"),
                        "status": str(item.get("status") or "open"),
                        "summary": str(item.get("summary") or ""),
                        "candidate_json": json.dumps(item.get("candidate") or {}, ensure_ascii=False),
                        "evidence_json": json.dumps(item.get("evidence") or [], ensure_ascii=False),
                        "metadata_json": json.dumps(item.get("metadata") or {}, ensure_ascii=False),
                        "reviewed_by": "",
                        "review_notes": "",
                        "reviewed_at": "",
                        "created_at": now,
                        "updated_at": now,
                    },
                )
            return self.list_items(job_id=job_id, status="", limit=max(len(normalized_items), 1))
        raise RuntimeError(
            "postgres-only invariant violated for manual_review_items in replace_manual_review_items: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def list_items(
        self,
        *,
        target_company: str = "",
        status: str = "open",
        job_id: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        postgres_rows = self._select_rows(
            "manual_review_items",
            row_builder=self._item_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["status = %s"] if status else []),
                    *(["job_id = %s"] if job_id else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([status] if status else []),
                *([job_id] if job_id else []),
            ],
            order_by_sql="updated_at DESC, review_item_id DESC",
            limit=limit,
        )
        if postgres_rows:
            return postgres_rows
        return []

    def count_items(
        self,
        *,
        target_company: str = "",
        status: str = "open",
        job_id: str = "",
    ) -> int:
        postgres_rows = self._select_rows(
            "manual_review_items",
            row_builder=self._item_from_row,
            where_sql=" AND ".join(
                [
                    *(["lower(target_company) = lower(%s)"] if target_company else []),
                    *(["status = %s"] if status else []),
                    *(["job_id = %s"] if job_id else []),
                ]
            ),
            params=[
                *([target_company] if target_company else []),
                *([status] if status else []),
                *([job_id] if job_id else []),
            ],
            order_by_sql="review_item_id DESC",
            limit=0,
        )
        if postgres_rows:
            return len(postgres_rows)
        return 0

    def cleanup_items(
        self,
        *,
        target_company: str = "",
        snapshot_id: str = "",
    ) -> dict[str, Any]:
        if self._should_prefer_read("manual_review_items"):
            rows = self._select_rows(
                "manual_review_items",
                row_builder=self._item_from_row,
                where_sql=" AND ".join(["lower(target_company) = lower(%s)"] if target_company else []),
                params=[target_company] if target_company else [],
                order_by_sql="updated_at DESC, review_item_id DESC",
                limit=0,
            )
            metadata_updated = 0
            superseded_count = 0
            out_of_scope_count = 0
            latest_open_by_scope: dict[tuple[str, str, str], int] = {}
            for row in rows:
                payload = dict(row)
                metadata = dict(payload.get("metadata") or {})
                inferred_snapshot_id = _infer_snapshot_id(payload)
                review_item_id = int(payload.get("review_item_id") or 0)
                if inferred_snapshot_id and str(metadata.get("snapshot_id") or "").strip() != inferred_snapshot_id:
                    metadata["snapshot_id"] = inferred_snapshot_id
                    self._call_native_write(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=review_item_id,
                        row={
                            "metadata_json": json.dumps(metadata, ensure_ascii=False),
                            "updated_at": utc_now_timestamp(),
                        },
                    )
                    payload["metadata"] = metadata
                    metadata_updated += 1

                row_snapshot_id = str(metadata.get("snapshot_id") or "").strip()
                if str(payload.get("status") or "") != "open":
                    continue
                if snapshot_id and row_snapshot_id and row_snapshot_id != snapshot_id:
                    self._call_native_write(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=review_item_id,
                        row={
                            "status": "out_of_scope",
                            "review_notes": _append_review_note(
                                payload.get("review_notes"),
                                "Marked out of scope for the active snapshot.",
                            ),
                            "updated_at": utc_now_timestamp(),
                        },
                    )
                    out_of_scope_count += 1
                    continue

                scope_key = _scope_key(payload, include_snapshot=False)
                if not scope_key:
                    continue
                if scope_key in latest_open_by_scope:
                    self._call_native_write(
                        "update_row_returning",
                        table_name="manual_review_items",
                        id_column="review_item_id",
                        id_value=review_item_id,
                        row={
                            "status": "superseded",
                            "review_notes": _append_review_note(
                                payload.get("review_notes"),
                                "Superseded by a newer manual-review queue item.",
                            ),
                            "updated_at": utc_now_timestamp(),
                        },
                    )
                    superseded_count += 1
                    continue
                latest_open_by_scope[scope_key] = review_item_id
            return {
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "metadata_updated_count": metadata_updated,
                "superseded_count": superseded_count,
                "out_of_scope_count": out_of_scope_count,
            }
        raise RuntimeError(
            "postgres-only invariant violated for manual_review_items in cleanup_manual_review_items: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def review_item(
        self,
        *,
        review_item_id: int,
        action: str,
        reviewer: str = "",
        notes: str = "",
        candidate_payload: dict[str, Any] | None = None,
        evidence_payload: list[dict[str, Any]] | None = None,
        metadata_merge: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self._select_row(
            "manual_review_items",
            row_builder=self._item_from_row,
            where_sql="review_item_id = %s",
            params=[review_item_id],
        )
        if existing is None:
            existing = self.get_item(review_item_id)
        if existing is None:
            return None
        normalized_action = str(action or "").strip().lower()
        if normalized_action in {"open", "reopen", "pending"}:
            status = "open"
        elif normalized_action in {"resolve", "resolved", "approve", "approved"}:
            status = "resolved"
        elif normalized_action in {"dismiss", "dismissed", "reject", "rejected"}:
            status = "dismissed"
        elif normalized_action in {"escalate", "escalated"}:
            status = "escalated"
        else:
            status = existing.get("status") or "open"
        merged_metadata = dict(existing.get("metadata") or {})
        merged_metadata.update(metadata_merge or {})
        stored_candidate = (
            candidate_payload
            if isinstance(candidate_payload, dict) and candidate_payload
            else existing.get("candidate") or {}
        )
        stored_evidence = (
            evidence_payload
            if isinstance(evidence_payload, list) and evidence_payload
            else existing.get("evidence") or []
        )
        if self._should_prefer_read("manual_review_items"):
            row = self._call_native_write(
                "update_row_returning",
                table_name="manual_review_items",
                id_column="review_item_id",
                id_value=review_item_id,
                row={
                    "status": status,
                    "summary": str(existing.get("summary") or ""),
                    "candidate_json": json.dumps(stored_candidate, ensure_ascii=False),
                    "evidence_json": json.dumps(stored_evidence, ensure_ascii=False),
                    "metadata_json": json.dumps(merged_metadata, ensure_ascii=False),
                    "reviewed_by": reviewer,
                    "review_notes": notes,
                    "reviewed_at": utc_now_timestamp(),
                    "updated_at": utc_now_timestamp(),
                },
            )
            if row is not None:
                return self._item_from_row(row)
            if self._strict_authoritative("manual_review_items"):
                # Track B B4.1b: PG is authoritative. The native update returns None only when the row
                # vanished between the read and the update; re-read PG for the current row rather than
                # fall through to the dead SQLite shadow.
                return self.get_item(review_item_id)
        raise RuntimeError(
            "postgres-only invariant violated for manual_review_items in review_manual_review_item: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def get_item(self, review_item_id: int) -> dict[str, Any] | None:
        if review_item_id <= 0:
            return None
        postgres_row = self._select_row(
            "manual_review_items",
            row_builder=self._item_from_row,
            where_sql="review_item_id = %s",
            params=[review_item_id],
        )
        if postgres_row is not None:
            return postgres_row
        if self._strict_authoritative("manual_review_items"):
            return None
        raise RuntimeError(
            "postgres-only invariant violated for manual_review_items in get_manual_review_item: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def merge_item_metadata(
        self,
        review_item_id: int,
        metadata_merge: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        existing = self.get_item(review_item_id)
        if existing is None:
            return None
        merged_metadata = dict(existing.get("metadata") or {})
        merged_metadata.update(dict(metadata_merge or {}))
        if self._should_prefer_read("manual_review_items"):
            row = self._call_native_write(
                "update_row_returning",
                table_name="manual_review_items",
                id_column="review_item_id",
                id_value=review_item_id,
                row={
                    "metadata_json": json.dumps(merged_metadata, ensure_ascii=False),
                    "updated_at": utc_now_timestamp(),
                },
            )
            if row is not None:
                return self._item_from_row(row)
            if self._strict_authoritative("manual_review_items"):
                return None
        raise RuntimeError(
            "postgres-only invariant violated for manual_review_items in merge_manual_review_item_metadata: should_prefer_read "
            "returned False; legacy SQLite tail retired (B4)"
        )

    def _item_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "review_item_id": row["review_item_id"],
            "job_id": row["job_id"],
            "candidate_id": row["candidate_id"],
            "target_company": row["target_company"],
            "review_type": row["review_type"],
            "priority": row["priority"],
            "status": row["status"],
            "summary": row["summary"],
            "candidate": json.loads(row["candidate_json"] or "{}"),
            "evidence": json.loads(row["evidence_json"] or "[]"),
            "metadata": json.loads(row["metadata_json"] or "{}"),
            "reviewed_by": row["reviewed_by"],
            "review_notes": row["review_notes"],
            "reviewed_at": row["reviewed_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
