from __future__ import annotations

from typing import Any

from .public_candidate_facets import (
    candidate_page_filter_corpus,
    public_facet_counts_from_records,
)
from .storage import ControlPlaneStore


class PersonAssetWriter:
    """Owner-facing facade for person assets, evidence, and assertions."""

    def __init__(self, store: ControlPlaneStore, *, writer_id: str = "person_asset_writer_v1") -> None:
        self.store = store
        self.writer_id = str(writer_id or "person_asset_writer_v1").strip() or "person_asset_writer_v1"

    def record_asset(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = {**dict(payload.get("metadata") or {}), "writer_id": self.writer_id}
        return self.store.upsert_person_asset({**dict(payload or {}), "metadata": metadata})

    def record_evidence(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = {**dict(payload.get("metadata") or {}), "writer_id": self.writer_id}
        return self.store.upsert_person_evidence({**dict(payload or {}), "metadata": metadata})

    def record_assertion(self, payload: dict[str, Any]) -> dict[str, Any]:
        metadata = {**dict(payload.get("metadata") or {}), "writer_id": self.writer_id}
        return self.store.upsert_person_assertion({**dict(payload or {}), "metadata": metadata})

    def rebuild_raw_profile_index_for_person(
        self,
        *,
        person_identity_key: str,
        public_summary: dict[str, Any] | None = None,
        raw_profile_index_watermark: str = "",
    ) -> dict[str, Any]:
        normalized_person_key = str(person_identity_key or "").strip()
        if not normalized_person_key:
            return {"status": "invalid", "reason": "person_identity_key_required"}
        assets = self.store.list_person_assets(person_identity_key=normalized_person_key, limit=500)
        raw_profile_assets = [
            dict(asset)
            for asset in assets
            if str(asset.get("asset_type") or "") in {"raw_profile", "linkedin_raw_profile", "profile_json"}
        ]
        raw_profile_terms = _projection_raw_profile_terms(dict(public_summary or {}), raw_profile_assets)
        indexed = self.store.upsert_raw_profile_index(
            {
                "person_identity_key": normalized_person_key,
                "indexed_text": " ".join(raw_profile_terms),
                "raw_profile_terms": raw_profile_terms,
                "source_asset_ids": [
                    str(asset.get("asset_id") or "").strip()
                    for asset in raw_profile_assets
                    if str(asset.get("asset_id") or "").strip()
                ],
                "indexed_field_sources": {
                    "public_summary": bool(public_summary),
                    "raw_profile_asset_count": len(raw_profile_assets),
                },
                "raw_profile_index_watermark": str(raw_profile_index_watermark or "").strip(),
                "profile_fetched_at": _latest_non_empty(
                    [str(asset.get("fetched_at") or "") for asset in raw_profile_assets]
                    + [str(dict(public_summary or {}).get("profile_fetched_at") or "")]
                ),
                "profile_indexed_at": _latest_non_empty(str(asset.get("updated_at") or "") for asset in raw_profile_assets),
                "metadata": {"writer_id": self.writer_id},
            }
        )
        return {
            "status": "indexed",
            "person_identity_key": normalized_person_key,
            "term_count": len(raw_profile_terms),
            "source_asset_count": len(raw_profile_assets),
            "index": indexed,
        }

    def rebuild_candidate_evidence_index_for_person(
        self,
        *,
        person_identity_key: str,
        evidence_index_watermark: str = "",
    ) -> dict[str, Any]:
        normalized_person_key = str(person_identity_key or "").strip()
        if not normalized_person_key:
            return {"status": "invalid", "reason": "person_identity_key_required"}
        evidence = self.store.list_person_evidence(person_identity_key=normalized_person_key, limit=1000)
        assertions = self.store.list_person_assertions(person_identity_key=normalized_person_key, limit=1000)
        evidence_terms = _projection_evidence_terms(evidence)
        assertion_terms = _projection_assertion_terms(assertions)
        indexed = self.store.upsert_candidate_evidence_index(
            {
                "person_identity_key": normalized_person_key,
                "indexed_text": " ".join([*evidence_terms, *assertion_terms]),
                "evidence_terms": evidence_terms,
                "assertion_terms": assertion_terms,
                "source_evidence_ids": [
                    str(item.get("evidence_id") or "").strip()
                    for item in evidence
                    if str(item.get("evidence_id") or "").strip()
                ],
                "source_assertion_ids": [
                    str(item.get("assertion_id") or "").strip()
                    for item in assertions
                    if str(item.get("assertion_id") or "").strip()
                ],
                "indexed_field_sources": {
                    "person_evidence_count": len(evidence),
                    "person_assertion_count": len(assertions),
                },
                "evidence_index_watermark": str(evidence_index_watermark or "").strip(),
                "evidence_indexed_at": _latest_non_empty(
                    [str(item.get("updated_at") or "") for item in evidence]
                    + [str(item.get("updated_at") or "") for item in assertions]
                ),
                "metadata": {"writer_id": self.writer_id},
            }
        )
        return {
            "status": "indexed",
            "person_identity_key": normalized_person_key,
            "term_count": len(evidence_terms) + len(assertion_terms),
            "source_evidence_count": len(evidence),
            "source_assertion_count": len(assertions),
            "index": indexed,
        }

    def rebuild_person_indexes(
        self,
        *,
        person_identity_keys: list[str] | tuple[str, ...],
        public_summary_by_person: dict[str, dict[str, Any]] | None = None,
        raw_profile_index_watermark: str = "",
        evidence_index_watermark: str = "",
        max_people: int = 100_000,
    ) -> dict[str, Any]:
        normalized_people: list[str] = []
        seen: set[str] = set()
        for item in list(person_identity_keys or []):
            person_key = str(item or "").strip()
            if not person_key or person_key in seen:
                continue
            seen.add(person_key)
            normalized_people.append(person_key)
        resolved_max_people = max(1, int(max_people or 100_000))
        truncated = len(normalized_people) > resolved_max_people
        selected_people = normalized_people[:resolved_max_people]
        public_summaries = dict(public_summary_by_person or {})
        raw_indexed_count = 0
        evidence_indexed_count = 0
        for person_key in selected_people:
            raw_result = self.rebuild_raw_profile_index_for_person(
                person_identity_key=person_key,
                public_summary=dict(public_summaries.get(person_key) or {}),
                raw_profile_index_watermark=raw_profile_index_watermark,
            )
            evidence_result = self.rebuild_candidate_evidence_index_for_person(
                person_identity_key=person_key,
                evidence_index_watermark=evidence_index_watermark,
            )
            if str(raw_result.get("status") or "") == "indexed":
                raw_indexed_count += 1
            if str(evidence_result.get("status") or "") == "indexed":
                evidence_indexed_count += 1
        return {
            "status": "indexed",
            "person_count": len(selected_people),
            "raw_profile_indexed_count": raw_indexed_count,
            "candidate_evidence_indexed_count": evidence_indexed_count,
            "truncated": truncated,
            "read_contract": {
                "source": "raw_profile_index+candidate_evidence_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def rebuild_person_indexes_for_projection(
        self,
        *,
        projection_id: str,
        raw_profile_index_watermark: str = "",
        evidence_index_watermark: str = "",
        member_page_size: int = 1000,
        max_members: int = 100_000,
    ) -> dict[str, Any]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return {"status": "invalid", "reason": "projection_id_required"}
        resolved_page_size = max(1, min(5000, int(member_page_size or 1000)))
        resolved_max_members = max(1, int(max_members or 100_000))
        processed_count = 0
        raw_indexed_count = 0
        evidence_indexed_count = 0
        offset = 0
        while processed_count < resolved_max_members:
            page_limit = min(resolved_page_size, resolved_max_members - processed_count)
            members = self.store.list_serving_projection_members(
                normalized_projection_id,
                offset=offset,
                limit=page_limit,
                visible_only=True,
            )
            if not members:
                break
            person_keys: list[str] = []
            public_summary_by_person: dict[str, dict[str, Any]] = {}
            for member in members:
                public_summary = dict(member.get("public_summary") or {})
                person_key = str(member.get("person_identity_key") or public_summary.get("person_identity_key") or "").strip()
                if not person_key:
                    continue
                person_keys.append(person_key)
                public_summary_by_person.setdefault(person_key, public_summary)
            result = self.rebuild_person_indexes(
                person_identity_keys=person_keys,
                public_summary_by_person=public_summary_by_person,
                raw_profile_index_watermark=raw_profile_index_watermark,
                evidence_index_watermark=evidence_index_watermark,
                max_people=len(person_keys) or 1,
            )
            raw_indexed_count += int(result.get("raw_profile_indexed_count") or 0)
            evidence_indexed_count += int(result.get("candidate_evidence_indexed_count") or 0)
            processed_count += len(members)
            if len(members) < page_limit:
                break
            offset += len(members)
        return {
            "status": "indexed",
            "projection_id": normalized_projection_id,
            "processed_member_count": processed_count,
            "raw_profile_indexed_count": raw_indexed_count,
            "candidate_evidence_indexed_count": evidence_indexed_count,
            "read_contract": {
                "source": "serving_projection_members->raw_profile_index+candidate_evidence_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def rebuild_projection_person_search_index(
        self,
        *,
        projection_id: str,
        count_scope: str = "index_partial",
        raw_profile_index_watermark: str = "",
        evidence_index_watermark: str = "",
        member_page_size: int = 1000,
        max_members: int = 100_000,
        rebuild_person_indexes: bool = True,
    ) -> dict[str, Any]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return {"status": "invalid", "reason": "projection_id_required", "indexed_count": 0}
        resolved_page_size = max(1, min(5000, int(member_page_size or 1000)))
        resolved_max_members = max(1, int(max_members or 100_000))
        indexed_count = 0
        raw_indexed_count = 0
        evidence_indexed_count = 0
        offset = 0
        last_page: dict[str, Any] = {}
        while True:
            page = self.rebuild_projection_person_search_index_page(
                projection_id=normalized_projection_id,
                count_scope=count_scope,
                raw_profile_index_watermark=raw_profile_index_watermark,
                evidence_index_watermark=evidence_index_watermark,
                member_page_size=resolved_page_size,
                max_members=resolved_max_members,
                offset=offset,
                reset_index=(offset == 0),
                rebuild_person_indexes=rebuild_person_indexes,
            )
            last_page = page
            if str(page.get("status") or "") != "indexed":
                return page
            indexed_count += int(page.get("indexed_count") or 0)
            person_index = dict(page.get("person_index") or {})
            raw_indexed_count += int(person_index.get("raw_profile_indexed_count") or 0)
            evidence_indexed_count += int(person_index.get("candidate_evidence_indexed_count") or 0)
            if bool(page.get("completed")):
                break
            next_offset = int(page.get("next_offset") or 0)
            if next_offset <= offset:
                break
            offset = next_offset
        total_member_count = int(last_page.get("total_member_count") or 0)
        truncated = bool(last_page.get("truncated"))
        return {
            "status": "indexed",
            "projection_id": normalized_projection_id,
            "indexed_count": indexed_count,
            "total_member_count": total_member_count,
            "truncated": truncated,
            "member_page_size": resolved_page_size,
            "max_members": resolved_max_members,
            "completed": bool(last_page.get("completed", True)),
            "next_offset": int(last_page.get("next_offset") or indexed_count),
            "person_index": {
                "status": "indexed" if bool(rebuild_person_indexes) else "skipped",
                "reason": "" if bool(rebuild_person_indexes) else "projection_index_consumes_existing_person_indexes",
                "person_count": indexed_count,
                "raw_profile_indexed_count": raw_indexed_count,
                "candidate_evidence_indexed_count": evidence_indexed_count,
                "truncated": truncated,
            },
            "read_contract": {
                "source": "projection_person_search_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def rebuild_projection_person_search_index_page(
        self,
        *,
        projection_id: str,
        count_scope: str = "index_partial",
        raw_profile_index_watermark: str = "",
        evidence_index_watermark: str = "",
        member_page_size: int = 100,
        max_members: int = 100_000,
        offset: int = 0,
        reset_index: bool = False,
        rebuild_person_indexes: bool = True,
    ) -> dict[str, Any]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return {"status": "invalid", "reason": "projection_id_required", "indexed_count": 0}
        total_member_count = self.store.count_serving_projection_members(normalized_projection_id, visible_only=True)
        resolved_page_size = max(1, min(5000, int(member_page_size or 100)))
        resolved_max_members = max(1, int(max_members or 100_000))
        resolved_offset = max(0, int(offset or 0))
        effective_member_limit = min(total_member_count, resolved_max_members)
        truncated = total_member_count > resolved_max_members
        requested_count_scope = str(count_scope or "index_partial").strip() or "index_partial"
        final_count_scope = "index_partial" if truncated else requested_count_scope
        if bool(reset_index) and resolved_offset == 0:
            self.store.delete_projection_person_search_index(normalized_projection_id)
            self._mark_projection_person_search_index_partial(
                projection_id=normalized_projection_id,
                total_member_count=total_member_count,
                truncated=truncated,
            )
        if resolved_offset >= effective_member_limit:
            self._finalize_projection_person_search_index(
                projection_id=normalized_projection_id,
                count_scope=final_count_scope,
                raw_profile_index_watermark=raw_profile_index_watermark,
                evidence_index_watermark=evidence_index_watermark,
                total_member_count=total_member_count,
                truncated=truncated,
            )
            return {
                "status": "indexed",
                "projection_id": normalized_projection_id,
                "indexed_count": 0,
                "processed_member_count": 0,
                "next_offset": resolved_offset,
                "completed": True,
                "total_member_count": total_member_count,
                "truncated": truncated,
                "member_page_size": resolved_page_size,
                "max_members": resolved_max_members,
                "person_index": {"status": "indexed", "person_count": 0},
                "read_contract": {
                    "source": "projection_person_search_index",
                    "fallback_used": False,
                    "fail_closed": True,
                },
            }
        page_limit = min(resolved_page_size, effective_member_limit - resolved_offset)
        members = self.store.list_serving_projection_members(
            normalized_projection_id,
            offset=resolved_offset,
            limit=page_limit,
            visible_only=True,
        )
        next_offset = resolved_offset + len(members)
        completed = next_offset >= effective_member_limit or len(members) < page_limit
        row_count_scope = final_count_scope if completed and resolved_offset == 0 else "index_partial"
        person_keys: list[str] = []
        public_summary_by_person: dict[str, dict[str, Any]] = {}
        for member in members:
            public_summary = dict(member.get("public_summary") or {})
            person_key = str(member.get("person_identity_key") or public_summary.get("person_identity_key") or "").strip()
            if not person_key:
                continue
            person_keys.append(person_key)
            public_summary_by_person.setdefault(person_key, public_summary)
        person_index_result = (
            self.rebuild_person_indexes(
                person_identity_keys=person_keys,
                public_summary_by_person=public_summary_by_person,
                raw_profile_index_watermark=raw_profile_index_watermark,
                evidence_index_watermark=evidence_index_watermark,
                max_people=len(person_keys) or 1,
            )
            if bool(rebuild_person_indexes)
            else {
                "status": "skipped",
                "reason": "projection_index_consumes_existing_person_indexes",
                "person_count": len(person_keys),
                "raw_profile_indexed_count": 0,
                "candidate_evidence_indexed_count": 0,
            }
        )
        raw_profile_index_by_person = self.store.list_raw_profile_indexes(person_keys)
        candidate_evidence_index_by_person = self.store.list_candidate_evidence_indexes(person_keys)
        rows = [
            self._projection_search_index_row(
                member,
                count_scope=row_count_scope,
                raw_profile_index_watermark=raw_profile_index_watermark,
                evidence_index_watermark=evidence_index_watermark,
                raw_profile_index_by_person=raw_profile_index_by_person,
                candidate_evidence_index_by_person=candidate_evidence_index_by_person,
            )
            for member in members
        ]
        batch_result = self.store.upsert_projection_person_search_index_rows(normalized_projection_id, rows)
        indexed_count = int(batch_result.get("indexed_count") or 0)
        if completed:
            if row_count_scope != final_count_scope:
                self.store.update_projection_person_search_index_scope(
                    normalized_projection_id,
                    count_scope=final_count_scope,
                    raw_profile_index_watermark=raw_profile_index_watermark,
                    evidence_index_watermark=evidence_index_watermark,
                )
            self._finalize_projection_person_search_index(
                projection_id=normalized_projection_id,
                count_scope=final_count_scope,
                raw_profile_index_watermark=raw_profile_index_watermark,
                evidence_index_watermark=evidence_index_watermark,
                total_member_count=total_member_count,
                truncated=truncated,
            )
        return {
            "status": "indexed",
            "projection_id": normalized_projection_id,
            "indexed_count": indexed_count,
            "processed_member_count": len(members),
            "next_offset": next_offset,
            "completed": completed,
            "total_member_count": total_member_count,
            "truncated": truncated,
            "member_page_size": resolved_page_size,
            "max_members": resolved_max_members,
            "person_index": person_index_result,
            "read_contract": {
                "source": "projection_person_search_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def _mark_projection_person_search_index_partial(
        self,
        *,
        projection_id: str,
        total_member_count: int,
        truncated: bool,
    ) -> None:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return
        projection = self.store.repos.serving_projection.get(normalized_projection_id)
        if projection:
            readiness = {
                **dict(projection.get("readiness") or {}),
                "index_count_scope": "index_partial",
            }
            self.store.repos.serving_projection.upsert(
                {
                    **projection,
                    "readiness": readiness,
                    "metadata": {
                        **dict(projection.get("metadata") or {}),
                        "search_index_writer_id": self.writer_id,
                        "search_indexed_member_count": self.store.count_projection_person_search_index(normalized_projection_id),
                        "search_index_total_member_count": total_member_count,
                        "search_index_truncated": truncated,
                        "search_index_build_status": "partial",
                    },
                }
            )

    def _finalize_projection_person_search_index(
        self,
        *,
        projection_id: str,
        count_scope: str,
        raw_profile_index_watermark: str,
        evidence_index_watermark: str,
        total_member_count: int,
        truncated: bool,
    ) -> None:
        projection = self.store.repos.serving_projection.get(projection_id)
        if not projection:
            return
        index_summary = self.store.get_projection_person_search_index_summary(projection_id)
        readiness_payload = dict(index_summary.get("index_filter_readiness") or {})
        readiness = {
            **dict(projection.get("readiness") or {}),
            "index_count_scope": str(count_scope or "index_partial").strip() or "index_partial",
            "profile_indexed_at": str(readiness_payload.get("profile_indexed_at") or "").strip(),
            "evidence_indexed_at": str(readiness_payload.get("evidence_indexed_at") or "").strip(),
        }
        indexed_count = self.store.count_projection_person_search_index(projection_id)
        self.store.repos.serving_projection.upsert(
            {
                **projection,
                "raw_profile_index_watermark": str(raw_profile_index_watermark or "").strip()
                or str(projection.get("raw_profile_index_watermark") or "").strip(),
                "evidence_index_watermark": str(evidence_index_watermark or "").strip()
                or str(projection.get("evidence_index_watermark") or "").strip(),
                "readiness": readiness,
                "metadata": {
                    **dict(projection.get("metadata") or {}),
                    "search_index_writer_id": self.writer_id,
                    "search_indexed_member_count": indexed_count,
                    "search_index_total_member_count": total_member_count,
                    "search_index_truncated": truncated,
                    "search_index_build_status": "completed",
                },
            }
        )
        self.rebuild_projection_public_facet_counts(
            projection_id=projection_id,
            count_scope=count_scope,
            total_indexed_count=indexed_count,
        )

    def rebuild_projection_public_facet_counts(
        self,
        *,
        projection_id: str,
        count_scope: str = "exact_projection",
        page_size: int = 1000,
        max_rows: int = 100_000,
        total_indexed_count: int | None = None,
    ) -> dict[str, Any]:
        """Publish canonical projection facet counts from persisted index records.

        Projection pages must not scan raw assets, old job overlays, or frontend
        row fragments to build global filters. The search index owns public
        filter records, so its completion is the bounded background point where
        exact projection facet counts become available.
        """

        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return {"status": "invalid", "reason": "projection_id_required", "facet_record_count": 0}
        projection = self.store.repos.serving_projection.get(normalized_projection_id)
        if not projection:
            return {
                "status": "invalid",
                "reason": "projection_missing",
                "projection_id": normalized_projection_id,
                "facet_record_count": 0,
            }
        indexed_count = (
            max(0, int(total_indexed_count))
            if total_indexed_count is not None
            else self.store.count_projection_person_search_index(normalized_projection_id)
        )
        if indexed_count <= 0:
            return {
                "status": "unavailable",
                "reason": "projection_person_search_index_missing",
                "projection_id": normalized_projection_id,
                "facet_record_count": 0,
            }
        resolved_page_size = max(1, min(5000, int(page_size or 1000)))
        resolved_max_rows = max(1, int(max_rows or 100_000))
        effective_limit = min(indexed_count, resolved_max_rows)
        records: list[dict[str, Any]] = []
        missing_filter_record_count = 0
        offset = 0
        while offset < effective_limit:
            page_limit = min(resolved_page_size, effective_limit - offset)
            page = self.store.list_projection_person_search_index_rows(
                normalized_projection_id,
                offset=offset,
                limit=page_limit,
            )
            if not page:
                break
            for row in page:
                filter_record = dict(dict(row.get("metadata") or {}).get("filter_record") or {})
                if filter_record:
                    records.append(filter_record)
                else:
                    missing_filter_record_count += 1
            offset += len(page)
            if len(page) < page_limit:
                break
        if not records:
            return {
                "status": "unavailable",
                "reason": "projection_person_search_index_filter_record_missing",
                "projection_id": normalized_projection_id,
                "facet_record_count": 0,
                "missing_filter_record_count": missing_filter_record_count,
            }
        truncated = indexed_count > len(records) + missing_filter_record_count or indexed_count > resolved_max_rows
        effective_count_scope = (
            "index_partial"
            if truncated or missing_filter_record_count > 0
            else (str(count_scope or "exact_projection").strip() or "exact_projection")
        )
        public_facet_counts = public_facet_counts_from_records(records)
        public_facet_counts.update(
            {
                "count_scope": effective_count_scope,
                "source": "projection_person_search_index",
                "indexed_count": indexed_count,
                "facet_record_count": len(records),
                "missing_filter_record_count": missing_filter_record_count,
                "truncated": truncated,
            }
        )
        latest_projection = self.store.repos.serving_projection.get(normalized_projection_id) or projection
        counts = {
            **dict(latest_projection.get("counts") or {}),
            "public_facet_counts": public_facet_counts,
            "facet_count_scope": effective_count_scope,
            "facet_build_status": "partial" if effective_count_scope == "index_partial" else "completed",
        }
        self.store.repos.serving_projection.upsert(
            {
                **latest_projection,
                "counts": counts,
                "metadata": {
                    **dict(latest_projection.get("metadata") or {}),
                    "public_facet_counts_writer_id": self.writer_id,
                    "public_facet_counts_source": "projection_person_search_index",
                    "public_facet_counts_build_status": counts["facet_build_status"],
                    "public_facet_counts_record_count": len(records),
                    "public_facet_counts_missing_filter_record_count": missing_filter_record_count,
                    "public_facet_counts_truncated": truncated,
                },
            }
        )
        return {
            "status": "published",
            "projection_id": normalized_projection_id,
            "count_scope": effective_count_scope,
            "facet_record_count": len(records),
            "indexed_count": indexed_count,
            "missing_filter_record_count": missing_filter_record_count,
            "truncated": truncated,
            "read_contract": {
                "source": "projection_person_search_index",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def _projection_search_index_row(
        self,
        member: dict[str, Any],
        *,
        count_scope: str,
        raw_profile_index_watermark: str,
        evidence_index_watermark: str,
        raw_profile_index_by_person: dict[str, dict[str, Any]] | None = None,
        candidate_evidence_index_by_person: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        public_summary = dict(member.get("public_summary") or {})
        projection_metrics = dict(member.get("projection_metrics") or {})
        member_metadata = dict(member.get("metadata") or {})
        person_key = str(member.get("person_identity_key") or public_summary.get("person_identity_key") or "").strip()
        raw_profile_index = (
            dict((raw_profile_index_by_person or {}).get(person_key) or {})
            if raw_profile_index_by_person is not None
            else (self.store.get_raw_profile_index(person_key) if person_key else {})
        )
        candidate_evidence_index = (
            dict((candidate_evidence_index_by_person or {}).get(person_key) or {})
            if candidate_evidence_index_by_person is not None
            else (self.store.get_candidate_evidence_index(person_key) if person_key else {})
        )
        raw_profile_terms = _non_empty_terms(
            [*_projection_raw_profile_terms(public_summary, []), *list(raw_profile_index.get("raw_profile_terms") or [])]
        )
        evidence_terms = _non_empty_terms(list(candidate_evidence_index.get("evidence_terms") or []))
        assertion_terms = _non_empty_terms(list(candidate_evidence_index.get("assertion_terms") or []))
        indexed_text = " ".join(
            [
                candidate_page_filter_corpus(
                    {
                        **public_summary,
                        "metadata": {
                            **dict(public_summary.get("metadata") or {}),
                            **dict(member.get("metadata") or {}),
                        },
                    }
                ),
                " ".join(raw_profile_terms),
                " ".join(evidence_terms),
                " ".join(assertion_terms),
            ]
        )
        return {
            "projection_id": str(member.get("projection_id") or "").strip(),
            "candidate_identity_key": str(member.get("candidate_identity_key") or "").strip(),
            "person_identity_key": person_key,
            "indexed_text": indexed_text,
            "raw_profile_terms": raw_profile_terms,
            "evidence_terms": evidence_terms,
            "assertion_terms": assertion_terms,
            "indexed_field_sources": {
                "public_summary": True,
                "raw_profile_index": bool(raw_profile_index),
                "candidate_evidence_index": bool(candidate_evidence_index),
                "raw_profile_source_asset_count": len(list(raw_profile_index.get("source_asset_ids") or [])),
                "evidence_source_count": len(list(candidate_evidence_index.get("source_evidence_ids") or [])),
                "assertion_source_count": len(list(candidate_evidence_index.get("source_assertion_ids") or [])),
            },
            "raw_profile_index_watermark": str(
                raw_profile_index_watermark or raw_profile_index.get("raw_profile_index_watermark") or ""
            ).strip(),
            "evidence_index_watermark": str(
                evidence_index_watermark or candidate_evidence_index.get("evidence_index_watermark") or ""
            ).strip(),
            "count_scope": str(count_scope or "index_partial").strip() or "index_partial",
            "profile_fetched_at": str(public_summary.get("profile_fetched_at") or "").strip(),
            "profile_indexed_at": str(raw_profile_index.get("profile_indexed_at") or ""),
            "evidence_indexed_at": str(candidate_evidence_index.get("evidence_indexed_at") or ""),
            "metadata": {
                "writer_id": self.writer_id,
                "raw_profile_index_present": bool(raw_profile_index),
                "candidate_evidence_index_present": bool(candidate_evidence_index),
                "filter_record": _projection_filter_record(
                    member=member,
                    public_summary=public_summary,
                    projection_metrics=projection_metrics,
                    member_metadata=member_metadata,
                ),
            },
        }


def _projection_raw_profile_terms(public_summary: dict[str, Any], assets: list[dict[str, Any]]) -> list[str]:
    terms: list[str] = []
    for key in (
        "display_name",
        "name",
        "headline",
        "summary",
        "current_company",
        "title",
        "location",
        "skills",
        "experience",
        "experience_lines",
        "education",
        "education_lines",
    ):
        value = public_summary.get(key)
        if isinstance(value, list):
            terms.extend(str(item or "") for item in value)
        else:
            terms.append(str(value or ""))
    for asset in assets:
        if str(asset.get("asset_type") or "") in {"raw_profile", "linkedin_raw_profile", "profile_json"}:
            metadata = dict(asset.get("metadata") or {})
            terms.extend(str(item or "") for item in list(metadata.get("indexed_terms") or []))
    return _non_empty_terms(terms)


def _projection_evidence_terms(evidence: list[dict[str, Any]]) -> list[str]:
    terms: list[str] = []
    for item in evidence:
        terms.extend(
            [
                str(item.get("evidence_type") or ""),
                str(item.get("value") or ""),
                str(item.get("normalized_value") or ""),
                str(item.get("source_domain") or ""),
                str(item.get("evidence_excerpt") or ""),
            ]
        )
    return _non_empty_terms(terms)


def _projection_assertion_terms(assertions: list[dict[str, Any]]) -> list[str]:
    terms: list[str] = []
    for assertion in assertions:
        terms.extend(
            [
                str(assertion.get("assertion_type") or ""),
                str(assertion.get("value") or ""),
                str(assertion.get("normalized_value") or ""),
                str(assertion.get("authority") or ""),
                str(assertion.get("verification_status") or ""),
            ]
        )
    return _non_empty_terms(terms)


def _non_empty_terms(values: list[str]) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        terms.append(normalized)
    return terms


def _latest_non_empty(values: Any) -> str:
    normalized = sorted(str(value or "").strip() for value in values if str(value or "").strip())
    return normalized[-1] if normalized else ""


def _projection_filter_record(
    *,
    member: dict[str, Any],
    public_summary: dict[str, Any],
    projection_metrics: dict[str, Any],
    member_metadata: dict[str, Any],
) -> dict[str, Any]:
    """Persist public filter inputs so projection filters do not scan raw assets."""

    record = {
        **public_summary,
        "candidate_id": str(member.get("candidate_id") or public_summary.get("candidate_id") or "").strip(),
        "id": str(member.get("candidate_id") or public_summary.get("candidate_id") or "").strip(),
        "employment_status": str(
            member.get("employment_scope")
            or public_summary.get("employment_status")
            or member_metadata.get("employment_status")
            or ""
        ).strip(),
        "row_readiness": str(member.get("row_readiness") or "").strip(),
        "profile_readiness": str(member.get("profile_readiness") or "").strip(),
        "card_readiness": str(member.get("card_readiness") or "").strip(),
        "needs_profile_completion": bool(
            public_summary.get("needs_profile_completion")
            or projection_metrics.get("needs_profile_completion")
        ),
        "low_profile_richness": bool(
            public_summary.get("low_profile_richness")
            or projection_metrics.get("low_profile_richness")
        ),
        "metadata": {
            **dict(public_summary.get("metadata") or {}),
            **member_metadata,
        },
    }
    return record
