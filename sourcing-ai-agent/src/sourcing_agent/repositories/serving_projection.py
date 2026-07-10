"""Track B B4.2 / ②.3 — serving-projection control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.

Manifest shards and projection members retain bespoke mappers because their historical read contract
clamps malformed/negative integers and tolerates broad mapping access failures. Member writes also own
identity resolution and readiness aggregation here; callers use ``store.repos.serving_projection``
directly and no ``ControlPlaneStore`` member facade remains.
"""

from __future__ import annotations

import builtins
import json
from typing import Any
from uuid import uuid4

from ..control_plane_repository import Column, Kind, Repository, TableDescriptor
from ..control_plane_serde import json_safe_payload
from ..control_plane_time import utc_now_timestamp
from ..person_identity import (
    build_person_summary_view,
    resolve_candidate_identity_key,
    resolve_person_identity_key,
    resolve_profile_url_key,
)

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


def _normalize_employment_scope(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"current", "former", "all"}:
        return normalized
    if normalized in {"past", "previous", "ex"}:
        return "former"
    if normalized:
        return normalized
    return ""


def _member_readiness_counts(members: builtins.list[dict[str, Any]]) -> dict[str, int]:
    row_count = 0
    profile_ready_count = 0
    card_ready_count = 0
    profile_required_count = 0
    for member in members:
        if not isinstance(member, dict):
            continue
        row_count += 1
        profile_readiness = str(member.get("profile_readiness") or "").strip().lower()
        card_readiness = str(member.get("card_readiness") or "").strip().lower()
        projection_metrics = dict(member.get("projection_metrics") or {})
        public_summary = dict(member.get("public_summary") or {})
        profile_required = bool(
            projection_metrics.get("profile_required")
            or projection_metrics.get("needs_profile_completion")
            or public_summary.get("needs_profile_completion")
            or profile_readiness not in {"", "not_required", "skipped"}
        )
        if profile_required:
            profile_required_count += 1
        if profile_readiness in {"ready", "complete", "completed", "fetched", "available"}:
            profile_ready_count += 1
        if card_readiness in {"ready", "complete", "completed", "materialized", "display_ready"}:
            card_ready_count += 1
    return {
        "row_count": row_count,
        "profile_required_count": profile_required_count,
        "profile_ready_count": profile_ready_count,
        "card_ready_count": card_ready_count,
    }


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
    """PG-only repository for projection catalog, members, links, pointers, and manifest shards."""

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

    def _member_from_row(self, row: Any) -> dict[str, Any]:
        if row is None:
            return {}
        return {
            "projection_id": str(_row_value(row, "projection_id") or ""),
            "candidate_identity_key": str(_row_value(row, "candidate_identity_key") or ""),
            "person_identity_key": str(_row_value(row, "person_identity_key") or ""),
            "profile_url_key": str(_row_value(row, "profile_url_key") or ""),
            "candidate_id": str(_row_value(row, "candidate_id") or ""),
            "rank_index": _normalize_non_negative_int(_row_value(row, "rank_index")),
            "rank_key": str(_row_value(row, "rank_key") or ""),
            "lane": str(_row_value(row, "lane") or ""),
            "employment_scope": str(_row_value(row, "employment_scope") or ""),
            "source_shard_key": str(_row_value(row, "source_shard_key") or ""),
            "source_run_id": str(_row_value(row, "source_run_id") or ""),
            "row_readiness": str(_row_value(row, "row_readiness") or ""),
            "profile_readiness": str(_row_value(row, "profile_readiness") or ""),
            "card_readiness": str(_row_value(row, "card_readiness") or ""),
            "visibility_state": str(_row_value(row, "visibility_state") or ""),
            "public_summary": _loads_json_dict(_row_value(row, "public_summary_json")),
            "projection_metrics": _loads_json_dict(_row_value(row, "projection_metrics_json")),
            "crm_overlay_summary": _loads_json_dict(_row_value(row, "crm_overlay_summary_json")),
            "provenance": _loads_json_dict(_row_value(row, "provenance_json")),
            "metadata": _loads_json_dict(_row_value(row, "metadata_json")),
            "published_at": str(_row_value(row, "published_at") or ""),
            "created_at": str(_row_value(row, "created_at") or ""),
            "updated_at": str(_row_value(row, "updated_at") or ""),
        }

    def _member_row_payload(
        self,
        projection_id: str,
        member: dict[str, Any],
        *,
        existing: dict[str, Any] | None = None,
        now: str = "",
    ) -> dict[str, Any]:
        normalized_member = dict(member or {})
        public_summary = _normalize_json_object_payload(
            normalized_member.get("public_summary") or normalized_member.get("public_summary_json")
        )
        profile_url_key = resolve_profile_url_key(
            normalized_member.get("profile_url_key"),
            public_summary.get("profile_url_key"),
            normalized_member.get("linkedin_url"),
            public_summary.get("linkedin_url"),
            public_summary.get("profile_url"),
        )
        candidate_id = str(normalized_member.get("candidate_id") or public_summary.get("candidate_id") or "").strip()
        person_identity_key = resolve_person_identity_key(
            person_identity_key=str(normalized_member.get("person_identity_key") or ""),
            profile_url_key=profile_url_key,
            linkedin_url=str(normalized_member.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            candidate_identity_key=str(normalized_member.get("candidate_identity_key") or ""),
            candidate_id=candidate_id,
        )
        candidate_identity_key = resolve_candidate_identity_key(
            candidate_identity_key=str(normalized_member.get("candidate_identity_key") or ""),
            person_identity_key=person_identity_key,
            profile_url_key=profile_url_key,
            linkedin_url=str(normalized_member.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
            candidate_id=candidate_id,
        )
        public_summary = {
            **public_summary,
            **build_person_summary_view(
                public_summary,
                candidate_id=candidate_id,
                profile_url_key=profile_url_key,
                person_identity_key=person_identity_key,
                source_projection_id=projection_id,
                source_run_id=str(normalized_member.get("source_run_id") or ""),
            ),
        }
        timestamp = str(now or utc_now_timestamp())
        return {
            "projection_id": projection_id,
            "candidate_identity_key": candidate_identity_key,
            "person_identity_key": person_identity_key or candidate_identity_key,
            "profile_url_key": profile_url_key,
            "candidate_id": candidate_id,
            "rank_index": _normalize_non_negative_int(normalized_member.get("rank_index")),
            "rank_key": str(normalized_member.get("rank_key") or "").strip(),
            "lane": str(normalized_member.get("lane") or "").strip(),
            "employment_scope": _normalize_employment_scope(normalized_member.get("employment_scope")),
            "source_shard_key": str(normalized_member.get("source_shard_key") or "").strip(),
            "source_run_id": str(normalized_member.get("source_run_id") or "").strip(),
            "row_readiness": str(normalized_member.get("row_readiness") or "ready").strip() or "ready",
            "profile_readiness": str(normalized_member.get("profile_readiness") or "unknown").strip() or "unknown",
            "card_readiness": str(normalized_member.get("card_readiness") or "unknown").strip() or "unknown",
            "visibility_state": str(normalized_member.get("visibility_state") or "visible").strip() or "visible",
            "public_summary_json": json.dumps(public_summary, ensure_ascii=False),
            "projection_metrics_json": json.dumps(
                _normalize_json_object_payload(
                    normalized_member.get("projection_metrics") or normalized_member.get("projection_metrics_json")
                ),
                ensure_ascii=False,
            ),
            "crm_overlay_summary_json": json.dumps(
                _normalize_json_object_payload(
                    normalized_member.get("crm_overlay_summary") or normalized_member.get("crm_overlay_summary_json")
                ),
                ensure_ascii=False,
            ),
            "provenance_json": json.dumps(
                _normalize_json_object_payload(
                    normalized_member.get("provenance") or normalized_member.get("provenance_json")
                ),
                ensure_ascii=False,
            ),
            "metadata_json": json.dumps(
                _normalize_json_object_payload(
                    normalized_member.get("metadata") or normalized_member.get("metadata_json")
                ),
                ensure_ascii=False,
            ),
            "published_at": str(normalized_member.get("published_at") or "").strip(),
            "created_at": str((existing or {}).get("created_at") or normalized_member.get("created_at") or timestamp),
            "updated_at": timestamp,
        }

    def upsert_members(
        self,
        projection_id: str,
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return 0
        row_payloads = self._member_row_payloads(normalized_projection_id, members)
        if not row_payloads:
            return 0
        if self._should_prefer_read("serving_projection_members"):
            return int(
                self._call_native_write(
                    "bulk_upsert_rows",
                    table_name="serving_projection_members",
                    rows=row_payloads,
                    transaction_lock_key=f"serving_projection_publication:{normalized_projection_id}",
                )
                or 0
            )
        self._raise_postgres_only_invariant(
            table_name="serving_projection_members",
            method_name="upsert_serving_projection_members",
        )

    def _member_row_payloads(
        self,
        projection_id: str,
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> builtins.list[dict[str, Any]]:
        normalized_by_key = self._normalized_member_inputs(members)
        if not normalized_by_key:
            return []
        existing_by_key = {
            str(item.get("candidate_identity_key") or "").strip(): item
            for item in self.list_members_by_identity_keys(projection_id, builtins.list(normalized_by_key))
            if str(item.get("candidate_identity_key") or "").strip()
        }
        return self._member_row_payloads_from_existing(
            projection_id,
            normalized_by_key,
            existing_by_key=existing_by_key,
            now=utc_now_timestamp(),
        )

    @staticmethod
    def _normalized_member_inputs(
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> dict[str, dict[str, Any]]:
        normalized_by_key: dict[str, dict[str, Any]] = {}
        for member in builtins.list(members or []):
            if not isinstance(member, dict):
                continue
            public_summary = _normalize_json_object_payload(
                member.get("public_summary") or member.get("public_summary_json")
            )
            profile_url_key = resolve_profile_url_key(
                member.get("profile_url_key"),
                public_summary.get("profile_url_key"),
                member.get("linkedin_url"),
                public_summary.get("linkedin_url"),
                public_summary.get("profile_url"),
            )
            candidate_identity_key = resolve_candidate_identity_key(
                candidate_identity_key=str(member.get("candidate_identity_key") or ""),
                person_identity_key=str(member.get("person_identity_key") or ""),
                profile_url_key=profile_url_key,
                linkedin_url=str(member.get("linkedin_url") or public_summary.get("linkedin_url") or ""),
                candidate_id=str(member.get("candidate_id") or public_summary.get("candidate_id") or ""),
            )
            if not candidate_identity_key:
                continue
            normalized_by_key[candidate_identity_key] = dict(member)
        return normalized_by_key

    def _member_row_payloads_from_existing(
        self,
        projection_id: str,
        normalized_by_key: dict[str, dict[str, Any]],
        *,
        existing_by_key: dict[str, dict[str, Any]],
        now: str,
    ) -> builtins.list[dict[str, Any]]:
        return [
            self._member_row_payload(
                projection_id,
                member,
                existing=existing_by_key.get(candidate_identity_key),
                now=now,
            )
            for candidate_identity_key, member in normalized_by_key.items()
        ]

    def replace_members(
        self,
        projection_id: str,
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> int:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return 0
        if self._should_prefer_read("serving_projection_members"):
            row_payloads = self._member_row_payloads(normalized_projection_id, members)
            return int(
                self._call_native_write(
                    "replace_rows",
                    table_name="serving_projection_members",
                    where_sql="projection_id = %s",
                    params=[normalized_projection_id],
                    rows=row_payloads,
                    transaction_lock_key=f"serving_projection_publication:{normalized_projection_id}",
                )
                or 0
            )
        self._raise_postgres_only_invariant(
            table_name="serving_projection_members",
            method_name="replace_serving_projection_members",
        )

    def list_members(
        self,
        projection_id: str,
        *,
        limit: int = 100,
        offset: int = 0,
        visible_only: bool = True,
    ) -> builtins.list[dict[str, Any]]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return []
        normalized_limit = max(1, int(limit or 100))
        normalized_offset = max(0, int(offset or 0))
        clauses = ["projection_id = ?"]
        params: builtins.list[Any] = [normalized_projection_id]
        if visible_only:
            clauses.append("visibility_state = ?")
            params.append("visible")
        where_sqlite = " AND ".join(clauses)
        postgres_rows = self._select_rows(
            "serving_projection_members",
            row_builder=self._member_from_row,
            where_sql=where_sqlite.replace("?", "%s"),
            params=params,
            order_by_sql="rank_index ASC, candidate_identity_key ASC",
            limit=normalized_limit,
            offset=normalized_offset,
        )
        if not postgres_rows:
            return []
        return postgres_rows

    def list_members_by_identity_keys(
        self,
        projection_id: str,
        candidate_identity_keys: builtins.list[str] | tuple[str, ...] | set[str],
    ) -> builtins.list[dict[str, Any]]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return []
        normalized_keys = [
            str(key or "").strip() for key in dict.fromkeys(candidate_identity_keys or []) if str(key or "").strip()
        ]
        if not normalized_keys:
            return []

        rows: builtins.list[dict[str, Any]] = []
        chunk_size = 500
        for offset in range(0, len(normalized_keys), chunk_size):
            chunk = normalized_keys[offset : offset + chunk_size]
            placeholders = ", ".join("%s" for _ in chunk)
            rows.extend(
                self._select_rows(
                    "serving_projection_members",
                    row_builder=self._member_from_row,
                    where_sql=f"projection_id = %s AND candidate_identity_key IN ({placeholders})",
                    params=[normalized_projection_id, *chunk],
                    order_by_sql="rank_index ASC, candidate_identity_key ASC",
                    limit=0,
                )
            )
        return rows

    def list_members_by_person_identity(
        self,
        person_identity_key: str,
        *,
        limit: int = 100,
    ) -> builtins.list[dict[str, Any]]:
        normalized_person_key = str(person_identity_key or "").strip()
        if not normalized_person_key:
            return []
        normalized_limit = max(1, int(limit or 100))
        postgres_rows = self._select_rows(
            "serving_projection_members",
            row_builder=self._member_from_row,
            where_sql="person_identity_key = %s",
            params=[normalized_person_key],
            order_by_sql="updated_at DESC, projection_id ASC, rank_index ASC",
            limit=normalized_limit,
        )
        if not postgres_rows:
            return []
        return postgres_rows

    def count_members_by_readiness(
        self,
        projection_id: str,
        *,
        visible_only: bool = True,
    ) -> dict[str, int]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return {}
        clauses = ["projection_id = ?"]
        params: builtins.list[Any] = [normalized_projection_id]
        if visible_only:
            clauses.append("visibility_state = ?")
            params.append("visible")
        where_sqlite = " AND ".join(clauses)
        if not self._should_prefer_read("serving_projection_members"):
            return {}
        try:
            rows = self._adapter.select_many(
                "serving_projection_members",
                where_sql=where_sqlite.replace("?", "%s"),
                params=params,
                order_by_sql="profile_readiness ASC, card_readiness ASC",
                limit=0,
            )
            return _member_readiness_counts([self._member_from_row(row) for row in rows])
        except Exception as exc:
            if self._strict_authoritative("serving_projection_members"):
                self._raise_read_failure(
                    table_name="serving_projection_members",
                    method_name="select_many",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return {}

    def get_member(self, projection_id: str, candidate_identity_key: str) -> dict[str, Any]:
        normalized_projection_id = str(projection_id or "").strip()
        normalized_candidate_key = str(candidate_identity_key or "").strip()
        if not normalized_projection_id or not normalized_candidate_key:
            return {}
        postgres_row = self._select_row(
            "serving_projection_members",
            row_builder=self._member_from_row,
            where_sql="projection_id = %s AND candidate_identity_key = %s",
            params=[normalized_projection_id, normalized_candidate_key],
        )
        if postgres_row is None:
            return {}
        return postgres_row

    def count_members(self, projection_id: str, *, visible_only: bool = True) -> int:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return 0
        clauses = ["projection_id = ?"]
        params: builtins.list[Any] = [normalized_projection_id]
        if visible_only:
            clauses.append("visibility_state = ?")
            params.append("visible")
        where_sqlite = " AND ".join(clauses)
        if not self._should_prefer_read("serving_projection_members"):
            return 0
        try:
            count_rows = getattr(self._adapter, "count_rows", None)
            if callable(count_rows):
                return int(
                    count_rows(
                        "serving_projection_members",
                        where_sql=where_sqlite.replace("?", "%s"),
                        params=params,
                    )
                    or 0
                )
        except Exception as exc:
            if self._strict_authoritative("serving_projection_members"):
                self._raise_read_failure(
                    table_name="serving_projection_members",
                    method_name="count_rows",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
        try:
            rows = self._adapter.select_many(
                "serving_projection_members",
                where_sql=where_sqlite.replace("?", "%s"),
                params=params,
                limit=0,
            )
            return len(rows)
        except Exception as exc:
            if self._strict_authoritative("serving_projection_members"):
                self._raise_read_failure(
                    table_name="serving_projection_members",
                    method_name="select_many",
                    reason=f"{type(exc).__name__}: {exc}",
                    error=exc,
                )
            return 0

    def _projection_row_payload(
        self,
        payload: dict[str, Any],
        *,
        selected_projection_id: str = "",
        existing: dict[str, Any] | None = None,
        now: str = "",
    ) -> tuple[str, dict[str, Any]]:
        normalized = dict(payload or {})
        projection_id = str(selected_projection_id or "").strip() or _build_projection_id(
            normalized.get("projection_id") or normalized.get("id")
        )
        projection_type = _normalize_projection_type(normalized.get("projection_type"))
        state = _normalize_projection_state(normalized.get("state"))
        effective_now = str(now or utc_now_timestamp())
        existing_row = self.get(projection_id) if existing is None else dict(existing)
        return projection_id, SERVING_PROJECTIONS.to_columns(
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
                "created_at": str(existing_row.get("created_at") or normalized.get("created_at") or effective_now),
                "updated_at": effective_now,
            }
        )

    def upsert(self, payload: dict[str, Any]) -> dict[str, Any]:
        projection_id, row_payload = self._projection_row_payload(payload)
        if self._write_row("serving_projections", row_payload):
            return self.get(projection_id)
        self._raise_write_failure(
            table_name="serving_projections",
            method_name="upsert_serving_projection",
            reason="postgres-only: write returned no confirmation; legacy SQLite mirror tail retired (B4)",
        )

    def upsert_with_replaced_members(
        self,
        payload: dict[str, Any],
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        """Atomically persist projection metadata and its complete member scope."""

        if not self._should_prefer_read("serving_projections"):
            self._raise_postgres_only_invariant(
                table_name="serving_projections",
                method_name="upsert_serving_projection_with_replaced_members",
            )
        if not self._should_prefer_read("serving_projection_members"):
            self._raise_postgres_only_invariant(
                table_name="serving_projection_members",
                method_name="upsert_serving_projection_with_replaced_members",
            )
        projection_id, projection_row = self._projection_row_payload(payload)
        member_rows = self._member_row_payloads(projection_id, members)
        result = self._call_native_write(
            "upsert_row_and_replace_rows",
            table_name="serving_projections",
            row=projection_row,
            replace_table_name="serving_projection_members",
            replace_where_sql="projection_id = %s",
            replace_params=[projection_id],
            replace_rows=member_rows,
            transaction_lock_key=f"serving_projection_publication:{projection_id}",
        )
        if not isinstance(result, dict):
            self._raise_write_failure(
                table_name="serving_projections",
                method_name="upsert_serving_projection_with_replaced_members",
                reason="postgres-only: atomic publication returned no confirmation",
            )
        projection = self.get(projection_id)
        if not projection:
            self._raise_read_failure(
                table_name="serving_projections",
                method_name="upsert_serving_projection_with_replaced_members",
                reason="atomic publication committed but projection read-back returned no row",
            )
        return {
            "projection": projection,
            "member_count": int(result.get("replaced_count") or 0),
        }

    def upsert_with_members(
        self,
        payload: dict[str, Any],
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        """Atomically persist projection metadata and merge incremental members."""

        if not self._should_prefer_read("serving_projections"):
            self._raise_postgres_only_invariant(
                table_name="serving_projections",
                method_name="upsert_serving_projection_with_members",
            )
        if not self._should_prefer_read("serving_projection_members"):
            self._raise_postgres_only_invariant(
                table_name="serving_projection_members",
                method_name="upsert_serving_projection_with_members",
            )
        projection_id, projection_row = self._projection_row_payload(payload)
        member_rows = self._member_row_payloads(projection_id, members)
        result = self._call_native_write(
            "upsert_row_and_upsert_rows",
            table_name="serving_projections",
            row=projection_row,
            upsert_table_name="serving_projection_members",
            upsert_rows=member_rows,
            transaction_lock_key=f"serving_projection_publication:{projection_id}",
        )
        if not isinstance(result, dict):
            self._raise_write_failure(
                table_name="serving_projections",
                method_name="upsert_serving_projection_with_members",
                reason="postgres-only: atomic incremental publication returned no confirmation",
            )
        projection = self.get(projection_id)
        if not projection:
            self._raise_read_failure(
                table_name="serving_projections",
                method_name="upsert_serving_projection_with_members",
                reason="atomic incremental publication committed but projection read-back returned no row",
            )
        return {
            "projection": projection,
            "member_count": int(result.get("child_upserted_count") or 0),
        }

    def publish_run_scope_projection(
        self,
        *,
        projection_payload: dict[str, Any],
        run_link_payload: dict[str, Any],
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
        replace_members: bool,
    ) -> dict[str, Any]:
        normalized_projection = dict(projection_payload or {})
        normalized_link = dict(run_link_payload or {})
        run_id = str(
            normalized_link.get("run_id")
            or normalized_projection.get("source_run_id")
            or normalized_projection.get("run_id")
            or ""
        ).strip()
        if not run_id:
            raise ValueError("run_id is required for run-scope projection publication")
        normalized_link["run_id"] = run_id
        normalized_link["link_type"] = "result"
        return self._publish_projection_with_route(
            scope_kind="run_scope",
            scope_key=run_id,
            active_collection_version="",
            projection_payload=normalized_projection,
            routing_payload=normalized_link,
            members=members,
            replace_members=replace_members,
        )

    def publish_collection_authoritative_projection(
        self,
        *,
        projection_payload: dict[str, Any],
        pointer_payload: dict[str, Any],
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
        replace_members: bool,
    ) -> dict[str, Any]:
        normalized_projection = dict(projection_payload or {})
        normalized_pointer = dict(pointer_payload or {})
        collection_id = str(
            normalized_pointer.get("collection_id") or normalized_projection.get("collection_id") or ""
        ).strip()
        active_collection_version = str(
            normalized_pointer.get("active_collection_version")
            or normalized_projection.get("source_collection_version")
            or ""
        ).strip()
        if not collection_id or not active_collection_version:
            raise ValueError(
                "collection_id and active_collection_version are required for authoritative projection publication"
            )
        normalized_pointer["collection_id"] = collection_id
        normalized_pointer["active_collection_version"] = active_collection_version
        return self._publish_projection_with_route(
            scope_kind="collection_authoritative",
            scope_key=collection_id,
            active_collection_version=active_collection_version,
            projection_payload=normalized_projection,
            routing_payload=normalized_pointer,
            members=members,
            replace_members=replace_members,
        )

    def _publish_projection_with_route(
        self,
        *,
        scope_kind: str,
        scope_key: str,
        active_collection_version: str,
        projection_payload: dict[str, Any],
        routing_payload: dict[str, Any],
        members: builtins.list[dict[str, Any]] | tuple[dict[str, Any], ...],
        replace_members: bool,
    ) -> dict[str, Any]:
        normalized_members = self._normalized_member_inputs(members)
        explicit_projection_id = str(
            projection_payload.get("projection_id") or projection_payload.get("id") or ""
        ).strip()

        def build_payload(
            *,
            selected_projection_id: str,
            existing_projection: dict[str, Any],
            existing_members_by_key: dict[str, dict[str, Any]],
            existing_route: dict[str, Any],
            publication_now: str,
        ) -> dict[str, Any]:
            effective_projection_payload = dict(projection_payload)
            if scope_kind == "run_scope" and not str(effective_projection_payload.get("collection_id") or "").strip():
                effective_projection_payload["collection_id"] = str(existing_route.get("collection_id") or "").strip()
            _, projection_row = self._projection_row_payload(
                effective_projection_payload,
                selected_projection_id=selected_projection_id,
                existing=existing_projection,
                now=publication_now,
            )
            member_rows = self._member_row_payloads_from_existing(
                selected_projection_id,
                normalized_members,
                existing_by_key=existing_members_by_key,
                now=publication_now,
            )
            if scope_kind == "run_scope":
                routing_row = self._run_link_row_payload(
                    {
                        **routing_payload,
                        "projection_id": selected_projection_id,
                        "collection_id": projection_row.get("collection_id") or "",
                    },
                    existing=existing_route,
                    now=publication_now,
                )
            else:
                routing_row = self._authoritative_pointer_row_payload(
                    {
                        **routing_payload,
                        "active_projection_id": selected_projection_id,
                    },
                    existing=existing_route,
                    now=publication_now,
                )
            return {
                "projection_row": projection_row,
                "member_rows": member_rows,
                "routing_row": routing_row,
            }

        result = self._call_native_write(
            "publish_serving_projection",
            table_name="serving_projections",
            scope_kind=scope_kind,
            scope_key=scope_key,
            explicit_projection_id=explicit_projection_id,
            active_collection_version=active_collection_version,
            replace_members=bool(replace_members),
            member_identity_keys=builtins.list(normalized_members),
            projection_id_factory=_build_projection_id,
            payload_builder=build_payload,
        )
        if not isinstance(result, dict):
            self._raise_write_failure(
                table_name="serving_projections",
                method_name="publish_serving_projection",
                reason="postgres-only: atomic serving publication returned no confirmation",
            )
        projection = self._projection_from_row(result.get("projection_row"))
        if scope_kind == "run_scope":
            routing = self._run_link_from_row(result.get("routing_row"))
            return {
                "projection": projection,
                "link": routing,
                "member_count": int(result.get("member_count") or 0),
                "identity_source": str(result.get("identity_source") or ""),
            }
        routing = self._authoritative_pointer_from_row(result.get("routing_row"))
        return {
            "projection": projection,
            "pointer": routing,
            "member_count": int(result.get("member_count") or 0),
            "identity_source": str(result.get("identity_source") or ""),
        }

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

    @staticmethod
    def _run_link_row_payload(
        payload: dict[str, Any],
        *,
        existing: dict[str, Any],
        now: str,
    ) -> dict[str, Any]:
        normalized = dict(payload or {})
        run_id = str(normalized.get("run_id") or normalized.get("job_id") or "").strip()
        projection_id = str(normalized.get("projection_id") or "").strip()
        if not run_id or not projection_id:
            return {}
        link_type = str(normalized.get("link_type") or "result").strip() or "result"
        return RUN_PROJECTION_LINKS.to_columns(
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

    def upsert_run_link(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        run_id = str(normalized.get("run_id") or normalized.get("job_id") or "").strip()
        projection_id = str(normalized.get("projection_id") or "").strip()
        if not run_id or not projection_id:
            return {}
        link_type = str(normalized.get("link_type") or "result").strip() or "result"
        row_payload = self._run_link_row_payload(
            normalized,
            existing=self.get_run_link(run_id, link_type=link_type),
            now=utc_now_timestamp(),
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

    @staticmethod
    def _authoritative_pointer_row_payload(
        payload: dict[str, Any],
        *,
        existing: dict[str, Any],
        now: str,
    ) -> dict[str, Any]:
        normalized = dict(payload or {})
        collection_id = str(normalized.get("collection_id") or "").strip()
        active_projection_id = str(
            normalized.get("active_projection_id") or normalized.get("projection_id") or ""
        ).strip()
        if not collection_id or not active_projection_id:
            return {}
        previous_projection_id = str(
            normalized.get("previous_projection_id")
            or (
                (existing or {}).get("active_projection_id")
                if str((existing or {}).get("active_projection_id") or "") != active_projection_id
                else (existing or {}).get("previous_projection_id")
            )
            or ""
        ).strip()
        return COLLECTION_AUTHORITATIVE_POINTERS.to_columns(
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

    def upsert_authoritative_pointer(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(payload or {})
        collection_id = str(normalized.get("collection_id") or "").strip()
        active_projection_id = str(
            normalized.get("active_projection_id") or normalized.get("projection_id") or ""
        ).strip()
        if not collection_id or not active_projection_id:
            return {}
        row_payload = self._authoritative_pointer_row_payload(
            normalized,
            existing=self.get_authoritative_pointer(collection_id),
            now=utc_now_timestamp(),
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
