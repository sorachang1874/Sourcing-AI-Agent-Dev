"""Track B B4.2 — serving-projection control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.
"""

from __future__ import annotations

import builtins
import json
from typing import Any
from uuid import uuid4

from ..control_plane_repository import Column, Kind, Repository, TableDescriptor
from ..control_plane_serde import json_safe_payload
from ..control_plane_time import utc_now_timestamp

SERVING_PROJECTIONS = TableDescriptor(
    table="serving_projections",
    pk=("projection_id",),
    columns=(
        Column("projection_id"),
        Column("projection_type"),
        Column("collection_id"),
        Column("source_run_id"),
        Column("projection_version", default="serving_projection_v1", read_default="serving_projection_v1"),
        Column("state"),
        Column("scope_label"),
        Column("scope_spec_json", Kind.JSON, field="scope_spec"),
        Column("candidate_identity_manifest_ref"),
        Column("source_collection_version"),
        Column("raw_profile_index_watermark"),
        Column("evidence_index_watermark"),
        Column("counts_json", Kind.JSON, field="counts"),
        Column("readiness_json", Kind.JSON, field="readiness"),
        Column("provenance_json", Kind.JSON, field="provenance"),
        Column("manual_overlay_version"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("published_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


PROJECTION_PERSON_SEARCH_INDEX = TableDescriptor(
    table="projection_person_search_index",
    pk=("projection_id", "candidate_identity_key"),
    columns=(
        Column("projection_id"),
        Column("candidate_identity_key"),
        Column("person_identity_key"),
        Column("indexed_text"),
        Column("raw_profile_terms_json", Kind.JSON_LIST, field="raw_profile_terms"),
        Column("evidence_terms_json", Kind.JSON_LIST, field="evidence_terms"),
        Column("assertion_terms_json", Kind.JSON_LIST, field="assertion_terms"),
        Column("indexed_field_sources_json", Kind.JSON, field="indexed_field_sources"),
        Column("raw_profile_index_watermark"),
        Column("evidence_index_watermark"),
        Column("count_scope"),
        Column("profile_fetched_at"),
        Column("profile_indexed_at"),
        Column("evidence_indexed_at"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


RUN_PROJECTION_LINKS = TableDescriptor(
    table="run_projection_links",
    pk=("run_id", "link_type"),
    columns=(
        Column("run_id"),
        Column("projection_id"),
        Column("link_type"),
        Column("projection_type"),
        Column("collection_id"),
        Column("state"),
        Column("created_by"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


COLLECTION_AUTHORITATIVE_POINTERS = TableDescriptor(
    table="collection_authoritative_pointers",
    pk=("collection_id",),
    columns=(
        Column("collection_id"),
        Column("active_projection_id"),
        Column("active_collection_version"),
        Column("previous_projection_id"),
        Column("state"),
        Column("writer_id"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("published_at"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


# Read-path descriptors keyed by the ControlPlaneStore mapper method they replace.
FROM_ROW_DESCRIPTORS = {
    "_projection_person_search_index_from_row": PROJECTION_PERSON_SEARCH_INDEX,
}


_SERVING_PROJECTION_TYPES = {
    "run_scope_projection",
    "collection_authoritative_projection",
}
_SERVING_PROJECTION_STATES = {
    "draft",
    "building",
    "serving",
    "degraded",
    "failed",
    "archived",
}


def _loads_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _normalize_json_object_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(json_safe_payload(value))
    return _loads_json_dict(value)


def _row_value(row: Any, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _normalize_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _build_projection_id(value: Any = "") -> str:
    normalized = str(value or "").strip()
    if normalized:
        return normalized
    return f"proj_{uuid4().hex}"


def _normalize_projection_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _SERVING_PROJECTION_TYPES:
        return normalized
    return "run_scope_projection"


def _normalize_projection_state(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in _SERVING_PROJECTION_STATES:
        return normalized
    return "draft"


class ServingProjectionRepository(Repository):
    """PG-only repository for projection catalog, links, pointers, and manifest shards."""

    def _projection_from_row(self, row: Any) -> dict[str, Any]:
        return SERVING_PROJECTIONS.from_row(row)

    def _run_link_from_row(self, row: Any) -> dict[str, Any]:
        return RUN_PROJECTION_LINKS.from_row(row)

    def _authoritative_pointer_from_row(self, row: Any) -> dict[str, Any]:
        return COLLECTION_AUTHORITATIVE_POINTERS.from_row(row)

    def _manifest_shard_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        return {
            "shard_id": str(_row_value(row, "shard_id") or ""),
            "projection_id": str(_row_value(row, "projection_id") or ""),
            "shard_kind": str(_row_value(row, "shard_kind") or ""),
            "shard_index": _normalize_non_negative_int(_row_value(row, "shard_index")),
            "manifest_ref": str(_row_value(row, "manifest_ref") or ""),
            "row_count": _normalize_non_negative_int(_row_value(row, "row_count")),
            "content_signature": str(_row_value(row, "content_signature") or ""),
            "metadata": _loads_json_dict(_row_value(row, "metadata_json")),
            "created_at": str(_row_value(row, "created_at") or ""),
            "updated_at": str(_row_value(row, "updated_at") or ""),
        }

    def upsert(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        projection_id = _build_projection_id(normalized.get("projection_id") or normalized.get("id"))
        projection_type = _normalize_projection_type(normalized.get("projection_type"))
        state = _normalize_projection_state(normalized.get("state"))
        now = utc_now_timestamp()
        existing = self.get(projection_id)
        row_payload = SERVING_PROJECTIONS.to_columns(
            {
                **normalized,
                "projection_id": projection_id,
                "projection_type": projection_type,
                "state": state,
                "source_run_id": normalized.get("source_run_id") or normalized.get("run_id"),
                "scope_spec": _normalize_json_object_payload(
                    normalized.get("scope_spec") or normalized.get("scope_spec_json")
                ),
                "counts": _normalize_json_object_payload(normalized.get("counts") or normalized.get("counts_json")),
                "readiness": _normalize_json_object_payload(
                    normalized.get("readiness") or normalized.get("readiness_json")
                ),
                "provenance": _normalize_json_object_payload(
                    normalized.get("provenance") or normalized.get("provenance_json")
                ),
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        if self._write_row("serving_projections", row_payload):
            return self.get(projection_id)
        self._raise_write_failure(
            table_name="serving_projections",
            method_name="upsert_serving_projection",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get(self, projection_id: str) -> dict[str, Any]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return {}
        postgres_row = self._select_row(
            "serving_projections",
            row_builder=self._projection_from_row,
            where_sql="projection_id = %s",
            params=[normalized_projection_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list(
        self,
        *,
        collection_id: str = "",
        source_run_id: str = "",
        projection_type: str = "",
        state: str = "",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if str(collection_id or "").strip():
            clauses.append("collection_id = ?")
            params.append(str(collection_id or "").strip())
        if str(source_run_id or "").strip():
            clauses.append("source_run_id = ?")
            params.append(str(source_run_id or "").strip())
        if str(projection_type or "").strip():
            clauses.append("projection_type = ?")
            params.append(_normalize_projection_type(projection_type))
        if str(state or "").strip():
            clauses.append("state = ?")
            params.append(_normalize_projection_state(state))
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_rows(
            "serving_projections",
            row_builder=self._projection_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="updated_at DESC, projection_id DESC",
            limit=max(1, int(limit or 100)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_run_link(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        run_id = str(normalized.get("run_id") or normalized.get("job_id") or "").strip()
        projection_id = str(normalized.get("projection_id") or "").strip()
        if not run_id or not projection_id:
            return {}
        link_type = str(normalized.get("link_type") or "result").strip() or "result"
        existing = self.get_run_link(run_id, link_type=link_type)
        now = utc_now_timestamp()
        row_payload = RUN_PROJECTION_LINKS.to_columns(
            {
                **normalized,
                "run_id": run_id,
                "projection_id": projection_id,
                "link_type": link_type,
                "projection_type": _normalize_projection_type(normalized.get("projection_type")),
                "state": str(normalized.get("state") or "active").strip().lower() or "active",
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        if self._write_row("run_projection_links", row_payload):
            return self.get_run_link(run_id, link_type=link_type)
        self._raise_write_failure(
            table_name="run_projection_links",
            method_name="upsert_run_projection_link",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_run_link(self, run_id: str, *, link_type: str = "result") -> dict[str, Any]:
        normalized_run_id = str(run_id or "").strip()
        normalized_link_type = str(link_type or "result").strip() or "result"
        if not normalized_run_id:
            return {}
        postgres_row = self._select_row(
            "run_projection_links",
            row_builder=self._run_link_from_row,
            where_sql="run_id = %s AND link_type = %s",
            params=[normalized_run_id, normalized_link_type],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_run_links(self, run_id: str, *, limit: int = 20) -> builtins.list[dict[str, Any]]:
        normalized_run_id = str(run_id or "").strip()
        if not normalized_run_id:
            return []
        postgres_rows = self._select_rows(
            "run_projection_links",
            row_builder=self._run_link_from_row,
            where_sql="run_id = %s",
            params=[normalized_run_id],
            order_by_sql="updated_at DESC, link_type ASC",
            limit=max(1, int(limit or 20)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_authoritative_pointer(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        collection_id = str(normalized.get("collection_id") or "").strip()
        active_projection_id = str(
            normalized.get("active_projection_id") or normalized.get("projection_id") or ""
        ).strip()
        if not collection_id or not active_projection_id:
            return {}
        existing = self.get_authoritative_pointer(collection_id)
        previous_projection_id = str(
            normalized.get("previous_projection_id")
            or (
                (existing or {}).get("active_projection_id")
                if str((existing or {}).get("active_projection_id") or "") != active_projection_id
                else (existing or {}).get("previous_projection_id")
            )
            or ""
        ).strip()
        now = utc_now_timestamp()
        row_payload = COLLECTION_AUTHORITATIVE_POINTERS.to_columns(
            {
                **normalized,
                "collection_id": collection_id,
                "active_projection_id": active_projection_id,
                "previous_projection_id": previous_projection_id,
                "state": str(normalized.get("state") or "active").strip().lower() or "active",
                "metadata": _normalize_json_object_payload(
                    normalized.get("metadata") or normalized.get("metadata_json")
                ),
                "published_at": str(normalized.get("published_at") or now).strip(),
                "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
                "updated_at": now,
            }
        )
        if self._write_row("collection_authoritative_pointers", row_payload):
            return self.get_authoritative_pointer(collection_id)
        self._raise_write_failure(
            table_name="collection_authoritative_pointers",
            method_name="upsert_collection_authoritative_pointer",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_authoritative_pointer(self, collection_id: str) -> dict[str, Any]:
        normalized_collection_id = str(collection_id or "").strip()
        if not normalized_collection_id:
            return {}
        postgres_row = self._select_row(
            "collection_authoritative_pointers",
            row_builder=self._authoritative_pointer_from_row,
            where_sql="collection_id = %s",
            params=[normalized_collection_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_authoritative_pointers(
        self,
        *,
        state: str = "active",
        limit: int = 250,
    ) -> builtins.list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        normalized_state = str(state or "").strip().lower()
        if normalized_state:
            clauses.append("state = ?")
            params.append(normalized_state)
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_rows(
            "collection_authoritative_pointers",
            row_builder=self._authoritative_pointer_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="updated_at DESC, collection_id ASC",
            limit=max(1, int(limit or 250)),
        )
        if postgres_rows:
            return postgres_rows
        return []

    def upsert_manifest_shard(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        projection_id = str(normalized.get("projection_id") or "").strip()
        if not projection_id:
            return {}
        shard_kind = str(normalized.get("shard_kind") or "candidate_identity_manifest").strip()
        shard_index = _normalize_non_negative_int(normalized.get("shard_index"))
        manifest_ref = str(normalized.get("manifest_ref") or "").strip()
        explicit_shard_id = str(normalized.get("shard_id") or "").strip()
        shard_id = explicit_shard_id or f"{projection_id}::{shard_kind}::{shard_index}"
        now = utc_now_timestamp()
        existing = self.get_manifest_shard(shard_id)
        row_payload = {
            "shard_id": shard_id,
            "projection_id": projection_id,
            "shard_kind": shard_kind,
            "shard_index": shard_index,
            "manifest_ref": manifest_ref,
            "row_count": _normalize_non_negative_int(normalized.get("row_count")),
            "content_signature": str(normalized.get("content_signature") or "").strip(),
            "metadata_json": json.dumps(
                _normalize_json_object_payload(normalized.get("metadata") or normalized.get("metadata_json")),
                ensure_ascii=False,
            ),
            "created_at": str((existing or {}).get("created_at") or normalized.get("created_at") or now),
            "updated_at": now,
        }
        if self._write_row("projection_manifest_shards", row_payload):
            return self.get_manifest_shard(shard_id)
        self._raise_write_failure(
            table_name="projection_manifest_shards",
            method_name="upsert_projection_manifest_shard",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def get_manifest_shard(self, shard_id: str) -> dict[str, Any]:
        normalized_shard_id = str(shard_id or "").strip()
        if not normalized_shard_id:
            return {}
        postgres_row = self._select_row(
            "projection_manifest_shards",
            row_builder=self._manifest_shard_from_row,
            where_sql="shard_id = %s",
            params=[normalized_shard_id],
        )
        if postgres_row is not None:
            return postgres_row
        return {}

    def list_manifest_shards(
        self,
        projection_id: str,
        *,
        shard_kind: str = "",
        limit: int = 1000,
    ) -> builtins.list[dict[str, Any]]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return []
        clauses = ["projection_id = ?"]
        params: builtins.list[Any] = [normalized_projection_id]
        if str(shard_kind or "").strip():
            clauses.append("shard_kind = ?")
            params.append(str(shard_kind or "").strip())
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_rows(
            "projection_manifest_shards",
            row_builder=self._manifest_shard_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="shard_kind ASC, shard_index ASC, shard_id ASC",
            limit=max(1, int(limit or 1000)),
        )
        if postgres_rows:
            return postgres_rows
        return []
