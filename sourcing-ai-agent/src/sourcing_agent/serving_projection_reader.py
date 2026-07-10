from __future__ import annotations

import os
from typing import Any

from .control_plane_repository import ControlPlaneAuthoritativeReadError
from .media_asset_owner import media_asset_frontend_url
from .public_candidate_facets import (
    apply_candidate_page_filter,
    candidate_page_filter_active,
    candidate_page_filter_contract,
    candidate_page_filter_signature,
    normalize_candidate_page_filter,
    public_facet_summary_from_counts,
)
from .storage import ControlPlaneStore

_SERVABLE_PROJECTION_STATES = {"serving", "building", "degraded"}
_VISIBLE_MEMBER_STATE = "visible"
_RESTRICTED_PUBLIC_ROW_KEYS = {
    "contact",
    "contacts",
    "crm_notes",
    "debug",
    "debug_payload",
    "email",
    "evidence",
    "evidence_records",
    "internal_evidence",
    "notes",
    "phone",
    "primary_email",
    "primary_email_metadata",
    "raw",
    "raw_html",
    "raw_payload",
    "raw_profile",
    "raw_profile_json",
    "restricted_contact",
}


class ServingProjectionReader:
    """Fail-closed public reader for canonical ServingProjection records."""

    def __init__(self, store: ControlPlaneStore) -> None:
        self.store = store

    def get_projection(self, projection_id: str) -> dict[str, Any]:
        normalized_projection_id = str(projection_id or "").strip()
        if not normalized_projection_id:
            return self._projection_error("invalid_projection_id", projection_id=projection_id)
        projection = self.store.repos.serving_projection.get(normalized_projection_id)
        if not projection:
            return self._projection_error("projection_not_found", projection_id=normalized_projection_id)
        if str(projection.get("state") or "").strip().lower() not in _SERVABLE_PROJECTION_STATES:
            return self._projection_error(
                "projection_not_servable",
                projection_id=normalized_projection_id,
                projection=projection,
            )
        try:
            visible_count = self.store.repos.serving_projection.count_members(
                normalized_projection_id,
                visible_only=True,
            )
            readiness_counts = self.store.repos.serving_projection.count_members_by_readiness(
                normalized_projection_id,
                visible_only=True,
            )
        except ControlPlaneAuthoritativeReadError:
            return self._projection_error(
                "projection_members_unavailable",
                projection_id=normalized_projection_id,
                projection=projection,
            )
        return {
            "status": "ready",
            "projection": self._public_projection_payload(
                projection,
                visible_count=visible_count,
                readiness_counts=readiness_counts,
            ),
        }

    def get_projection_candidates(
        self,
        projection_id: str,
        *,
        offset: int = 0,
        limit: int = 120,
        candidate_filter: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        projection_payload = self.get_projection(projection_id)
        if str(projection_payload.get("status") or "") != "ready":
            return projection_payload
        projection = dict(projection_payload.get("projection") or {})
        normalized_projection_id = str(projection.get("projection_id") or "").strip()
        normalized_offset = max(0, int(offset or 0))
        normalized_limit = min(max(1, int(limit or 120)), 250)
        normalized_filter = normalize_candidate_page_filter(candidate_filter)
        filter_signature = candidate_page_filter_signature(normalized_filter)
        filter_active = candidate_page_filter_active(normalized_filter)
        total_count = int(projection.get("visible_member_count") or 0)
        try:
            if filter_active:
                filtered = self._filtered_projection_members(
                    normalized_projection_id,
                    candidate_filter=normalized_filter,
                    offset=normalized_offset,
                    limit=normalized_limit,
                )
                if str(filtered.get("status") or "ready") != "ready":
                    filter_contract = self._projection_filter_contract(
                        projection=projection,
                        applied_filter=normalized_filter,
                        filter_signature=filter_signature,
                        source=str(filtered.get("filter_source") or "projection_person_search_index"),
                        row_filter_scope="projection_membership",
                        fallback_used=False,
                    )
                    return {
                        "status": "not_ready",
                        "reason": str(filtered.get("reason") or "projection_person_search_index_unavailable"),
                        "projection": projection,
                        "candidate_count": total_count,
                        "total_candidates": total_count,
                        "filtered_candidate_count": 0,
                        "offset": normalized_offset,
                        "limit": 0,
                        "has_more": False,
                        "next_offset": None,
                        "candidates": [],
                        "facet_summary": self._projection_facet_summary(projection),
                        "index_filter_readiness": dict(filtered.get("index_filter_readiness") or {})
                        or self._index_filter_readiness_payload(projection),
                        "applied_filter": normalized_filter,
                        "filter_signature": filter_signature,
                        "filter_contract": filter_contract,
                        "field_visibility": projection.get("field_visibility") or self._field_visibility_payload(),
                        "read_contract": {
                            "source": "projection_person_search_index",
                            "fallback_used": False,
                            "fail_closed": True,
                        },
                    }
                filtered_count = int(filtered["filtered_count"])
                members = list(filtered["members"])
                filter_source = str(filtered.get("filter_source") or "serving_projection_members_scan")
                index_filter_readiness = dict(filtered.get("index_filter_readiness") or {})
                filter_fallback_used = bool(filtered.get("filter_fallback_used"))
                filter_fallback_reason = str(filtered.get("filter_fallback_reason") or "").strip()
            else:
                filtered_count = total_count
                members = self.store.repos.serving_projection.list_members(
                    normalized_projection_id,
                    offset=normalized_offset,
                    limit=normalized_limit,
                    visible_only=True,
                )
                filter_source = "serving_projection_members"
                index_filter_readiness = self._index_filter_readiness_payload(projection)
                filter_fallback_used = False
                filter_fallback_reason = ""
        except ControlPlaneAuthoritativeReadError:
            return self._projection_error(
                "projection_members_unavailable",
                projection_id=normalized_projection_id,
                projection=projection,
            )
        crm_overlays_by_person = self._crm_overlays_for_members(members)
        if crm_overlays_by_person:
            members = [
                {
                    **member,
                    "crm_overlay_summary": {
                        **dict(member.get("crm_overlay_summary") or {}),
                        **crm_overlays_by_person.get(str(member.get("person_identity_key") or "").strip(), {}),
                    },
                }
                for member in members
            ]
        media_summaries_by_person = self._media_summaries_for_members(members)
        if media_summaries_by_person:
            unavailable_media_summary = self._media_summary_payload([])
            members = [
                {
                    **member,
                    "media_summary": media_summaries_by_person.get(
                        str(member.get("person_identity_key") or "").strip(),
                        unavailable_media_summary,
                    ),
                }
                for member in members
            ]
        rows = [self._public_member_payload(member) for member in members]
        next_offset = normalized_offset + len(rows)
        filter_contract = self._projection_filter_contract(
            projection=projection,
            applied_filter=normalized_filter,
            filter_signature=filter_signature,
            source=filter_source,
            row_filter_scope="projection_membership",
            fallback_used=filter_fallback_used,
            fallback_reason=filter_fallback_reason,
        )
        return {
            "status": "ready",
            "projection": projection,
            "candidate_count": total_count,
            "total_candidates": total_count,
            "filtered_candidate_count": filtered_count,
            "offset": normalized_offset,
            "limit": len(rows),
            "has_more": next_offset < filtered_count,
            "next_offset": next_offset if next_offset < filtered_count else None,
            "candidates": rows,
            "facet_summary": self._projection_facet_summary(projection),
            "index_filter_readiness": index_filter_readiness or self._index_filter_readiness_payload(projection),
            "applied_filter": normalized_filter,
            "filter_signature": filter_signature,
            "filter_contract": filter_contract,
            "field_visibility": projection.get("field_visibility") or self._field_visibility_payload(),
            "read_contract": {
                "source": "serving_projection_members",
                "fallback_used": filter_fallback_used,
                "fail_closed": True,
            },
        }

    def get_projection_person_detail(
        self,
        projection_id: str,
        candidate_identity_key: str,
        *,
        workspace_id: str = "default",
    ) -> dict[str, Any]:
        projection_payload = self.get_projection(projection_id)
        if str(projection_payload.get("status") or "") != "ready":
            return projection_payload
        normalized_projection_id = str(projection_id or "").strip()
        normalized_candidate_key = str(candidate_identity_key or "").strip()
        if not normalized_candidate_key:
            return self._projection_error("candidate_identity_key_required", projection_id=normalized_projection_id)
        try:
            member = self.store.repos.serving_projection.get_member(
                normalized_projection_id,
                normalized_candidate_key,
                visible_only=True,
            )
        except ControlPlaneAuthoritativeReadError:
            return self._projection_error(
                "projection_members_unavailable",
                projection_id=normalized_projection_id,
                projection=dict(projection_payload.get("projection") or {}),
            )
        if not member:
            return self._projection_error("projection_member_not_found", projection_id=normalized_projection_id)
        person_key = str(member.get("person_identity_key") or "").strip()
        assets = self.store.list_person_assets(person_identity_key=person_key, limit=50) if person_key else []
        assertions = self.store.list_person_assertions(person_identity_key=person_key, limit=100) if person_key else []
        evidence = self.store.list_person_evidence(person_identity_key=person_key, limit=100) if person_key else []
        crm_record = (
            self.store.get_crm_record_by_person_identity(
                person_key,
                workspace_id=str(workspace_id or "default").strip() or "default",
            )
            if person_key
            else {}
        )
        return {
            "status": "ready",
            "projection_id": normalized_projection_id,
            "candidate_identity_key": normalized_candidate_key,
            "person_identity_key": person_key,
            "public_summary": _scrub_public_projection_payload(dict(member.get("public_summary") or {})),
            "projection_metrics": _scrub_public_projection_payload(dict(member.get("projection_metrics") or {})),
            "membership": self._public_member_payload(member),
            "crm_overlay_summary": self._compact_crm_overlay_summary(crm_record),
            "media_summary": self._media_summary_payload(assets),
            "assertions": [self._public_assertion_payload(item) for item in assertions],
            "assets": [self._public_asset_payload(item) for item in assets],
            "evidence_summary": self._evidence_summary_payload(evidence),
            "field_visibility": {
                "included": ["public_summary", "projection_metrics", "crm_overlay_summary", "assertion_summary"],
                "excluded": ["raw_profile", "raw_payload", "crm_notes", "restricted_contact_value_by_default"],
            },
            "read_contract": {
                "source": "serving_projection_members+person_assets+crm_records",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def search_projection_person_index(
        self,
        projection_id: str,
        *,
        search_keyword: str,
        offset: int = 0,
        limit: int = 120,
    ) -> dict[str, Any]:
        projection_payload = self.get_projection(projection_id)
        if str(projection_payload.get("status") or "") != "ready":
            return projection_payload
        projection = dict(projection_payload.get("projection") or {})
        normalized_projection_id = str(projection.get("projection_id") or "").strip()
        normalized_offset = max(0, int(offset or 0))
        normalized_limit = min(max(1, int(limit or 120)), 250)
        try:
            search_result = self.store.repos.serving_projection.search_person_index(
                normalized_projection_id,
                search_keyword=search_keyword,
                offset=normalized_offset,
                limit=normalized_limit,
            )
        except ControlPlaneAuthoritativeReadError:
            search_result = {
                "status": "unavailable",
                "reason": "projection_person_search_index_unavailable",
            }
        if str(search_result.get("status") or "") != "ready":
            return {
                "status": "not_ready",
                "reason": str(search_result.get("reason") or "projection_person_search_index_unavailable"),
                "projection": projection,
                "candidate_count": int(projection.get("visible_member_count") or 0),
                "filtered_candidate_count": 0,
                "offset": normalized_offset,
                "limit": 0,
                "has_more": False,
                "next_offset": None,
                "candidates": [],
                "index_filter_readiness": dict(search_result.get("index_filter_readiness") or {}),
                "read_contract": {
                    "source": "projection_person_search_index",
                    "fallback_used": False,
                    "fail_closed": True,
                },
            }
        try:
            members = self._hydrate_index_page_members(
                normalized_projection_id,
                index_result=search_result,
                requested_offset=normalized_offset,
                requested_limit=normalized_limit,
            )
        except ControlPlaneAuthoritativeReadError:
            return self._projection_error(
                "projection_members_unavailable",
                projection_id=normalized_projection_id,
                projection=projection,
            )
        if members is None:
            return {
                "status": "not_ready",
                "reason": "projection_person_search_index_unavailable",
                "projection": projection,
                "candidate_count": int(projection.get("visible_member_count") or 0),
                "filtered_candidate_count": 0,
                "offset": normalized_offset,
                "limit": 0,
                "has_more": False,
                "next_offset": None,
                "candidates": [],
                "index_filter_readiness": dict(search_result.get("index_filter_readiness") or {}),
                "read_contract": {
                    "source": "projection_person_search_index",
                    "fallback_used": False,
                    "fail_closed": True,
                },
            }
        crm_overlays_by_person = self._crm_overlays_for_members(members)
        if crm_overlays_by_person:
            members = [
                {
                    **member,
                    "crm_overlay_summary": {
                        **dict(member.get("crm_overlay_summary") or {}),
                        **crm_overlays_by_person.get(str(member.get("person_identity_key") or "").strip(), {}),
                    },
                }
                for member in members
            ]
        return {
            "status": "ready",
            "projection": projection,
            "candidate_count": int(projection.get("visible_member_count") or 0),
            "total_candidates": int(projection.get("visible_member_count") or 0),
            "filtered_candidate_count": int(search_result.get("matched_count") or 0),
            "offset": int(search_result.get("offset") or 0),
            "limit": len(members),
            "has_more": bool(search_result.get("has_more")),
            "next_offset": search_result.get("next_offset"),
            "search_keyword": str(search_keyword or "").strip(),
            "candidates": [self._public_member_payload(member) for member in members],
            "facet_summary": self._projection_facet_summary(projection),
            "index_filter_readiness": dict(search_result.get("index_filter_readiness") or {}),
            "field_visibility": projection.get("field_visibility") or self._field_visibility_payload(),
            "read_contract": {
                "source": "projection_person_search_index+serving_projection_members",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def get_person_summary(
        self,
        person_identity_key: str,
        *,
        workspace_id: str = "default",
        limit: int = 25,
    ) -> dict[str, Any]:
        normalized_person_key = str(person_identity_key or "").strip()
        if not normalized_person_key:
            return {"status": "invalid", "reason": "person_identity_key_required"}
        try:
            members = self.store.repos.serving_projection.list_members_by_person_identity(
                normalized_person_key,
                limit=max(1, int(limit or 25)),
            )
        except ControlPlaneAuthoritativeReadError:
            return self._projection_error("projection_members_unavailable")
        if not members:
            return {
                "status": "not_found",
                "reason": "person_summary_not_found",
                "person_identity_key": normalized_person_key,
                "read_contract": {
                    "source": "serving_projection_members",
                    "fallback_used": False,
                    "fail_closed": True,
                },
            }
        latest_member = dict(members[0])
        crm_record = self.store.get_crm_record_by_person_identity(
            normalized_person_key,
            workspace_id=str(workspace_id or "default").strip() or "default",
        )
        assertions = self.store.list_person_assertions(person_identity_key=normalized_person_key, limit=100)
        assets = self.store.list_person_assets(person_identity_key=normalized_person_key, limit=50)
        evidence = self.store.list_person_evidence(person_identity_key=normalized_person_key, limit=100)
        return {
            "status": "ready",
            "person_identity_key": normalized_person_key,
            "public_summary": _scrub_public_projection_payload(dict(latest_member.get("public_summary") or {})),
            "projection_membership_count": len(members),
            "projection_memberships": [
                {
                    "projection_id": str(member.get("projection_id") or ""),
                    "candidate_identity_key": str(member.get("candidate_identity_key") or ""),
                    "source_run_id": str(member.get("source_run_id") or ""),
                    "visibility_state": str(member.get("visibility_state") or ""),
                    "updated_at": str(member.get("updated_at") or ""),
                }
                for member in members
            ],
            "crm_overlay_summary": self._compact_crm_overlay_summary(crm_record),
            "media_summary": self._media_summary_payload(assets),
            "assertion_summary": self._assertion_summary_payload(assertions),
            "asset_summary": self._asset_summary_payload(assets),
            "evidence_summary": self._evidence_summary_payload(evidence),
            "read_contract": {
                "source": "serving_projection_members+person_assets+crm_records",
                "fallback_used": False,
                "fail_closed": True,
            },
        }

    def _crm_overlays_for_members(self, members: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        person_keys = [
            str(member.get("person_identity_key") or "").strip()
            for member in list(members or [])
            if str(member.get("person_identity_key") or "").strip()
        ]
        if not person_keys or not hasattr(self.store, "list_crm_records_by_person_identity_keys"):
            return {}
        records_by_person = self.store.list_crm_records_by_person_identity_keys(person_keys)
        overlays: dict[str, dict[str, Any]] = {}
        for person_key, record in records_by_person.items():
            if not record:
                continue
            metadata = dict(record.get("metadata") or {})
            overlays[person_key] = {
                "in_crm": True,
                "crm_record_id": str(record.get("crm_record_id") or "").strip(),
                "lifecycle_status": str(record.get("lifecycle_status") or "").strip(),
                "visibility_status": str(record.get("visibility_status") or "").strip(),
                "stage": str(metadata.get("current_stage") or "").strip(),
                "owner_user_id": str(record.get("owner_user_id") or "").strip(),
                "updated_at": str(record.get("updated_at") or "").strip(),
            }
        return overlays

    def _media_summaries_for_members(self, members: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        person_keys = [
            str(member.get("person_identity_key") or "").strip()
            for member in list(members or [])
            if str(member.get("person_identity_key") or "").strip()
        ]
        if not person_keys:
            return {}
        deduped_keys = list(dict.fromkeys(person_keys))
        assets = self.store.list_person_assets_for_person_keys(
            deduped_keys,
            asset_type="avatar_media",
            limit=max(50, len(deduped_keys) * 3),
        )
        assets_by_person: dict[str, list[dict[str, Any]]] = {person_key: [] for person_key in deduped_keys}
        for asset in assets:
            person_key = str(asset.get("person_identity_key") or "").strip()
            if not person_key or person_key not in assets_by_person:
                continue
            assets_by_person[person_key].append(dict(asset or {}))
        return {
            person_key: self._media_summary_payload(person_assets)
            for person_key, person_assets in assets_by_person.items()
        }

    def _filtered_projection_members(
        self,
        projection_id: str,
        *,
        candidate_filter: dict[str, Any],
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        indexed_result = self._indexed_filter_projection_members(
            projection_id,
            candidate_filter=candidate_filter,
            offset=offset,
            limit=limit,
        )
        if indexed_result and (
            str(indexed_result.get("status") or "ready") == "ready"
            or not _legacy_projection_filter_scan_fallback_enabled()
        ):
            return indexed_result
        if not _legacy_projection_filter_scan_fallback_enabled():
            return {
                "status": "not_ready",
                "reason": "projection_person_search_index_unavailable",
                "members": [],
                "filtered_count": 0,
                "filter_source": "projection_person_search_index",
                "filter_fallback_used": False,
                "index_filter_readiness": self._index_filter_readiness_payload(
                    self.store.repos.serving_projection.get(projection_id)
                ),
            }
        page_size = 1000
        scan_offset = 0
        filtered_count = 0
        selected: list[dict[str, Any]] = []
        while True:
            page = self.store.repos.serving_projection.list_members(
                projection_id,
                offset=scan_offset,
                limit=page_size,
                visible_only=True,
            )
            if not page:
                break
            filtered_page = apply_candidate_page_filter(
                candidates=[self._member_filter_record(member) for member in page],
                candidate_filter=candidate_filter,
                review_status_lookup={},
            )
            for member in filtered_page:
                if filtered_count >= offset and len(selected) < limit:
                    selected.append(member)
                filtered_count += 1
            if len(page) < page_size:
                break
            scan_offset += len(page)
        return {
            "status": "ready",
            "members": selected,
            "filtered_count": filtered_count,
            "filter_source": "serving_projection_members_scan_legacy_cutover",
            "filter_fallback_used": True,
            "filter_fallback_reason": "projection_person_search_index_unavailable",
            "index_filter_readiness": self._index_filter_readiness_payload(
                self.store.repos.serving_projection.get(projection_id)
            ),
        }

    def _indexed_filter_projection_members(
        self,
        projection_id: str,
        *,
        candidate_filter: dict[str, Any],
        offset: int,
        limit: int,
    ) -> dict[str, Any]:
        if _candidate_filter_is_keyword_only(candidate_filter):
            search_keyword = str(dict(candidate_filter or {}).get("search_keyword") or "").strip()
            if not search_keyword:
                return {}
            try:
                result = self.store.repos.serving_projection.search_person_index(
                    projection_id,
                    search_keyword=search_keyword,
                    offset=offset,
                    limit=limit,
                )
            except ControlPlaneAuthoritativeReadError:
                return {
                    "status": "not_ready",
                    "reason": "projection_person_search_index_unavailable",
                    "members": [],
                    "filtered_count": 0,
                    "filter_source": "projection_person_search_index",
                    "index_filter_readiness": {},
                }
        else:
            try:
                result = self.store.repos.serving_projection.filter_person_search_index(
                    projection_id,
                    candidate_filter=candidate_filter,
                    offset=offset,
                    limit=limit,
                )
            except ControlPlaneAuthoritativeReadError:
                return {
                    "status": "not_ready",
                    "reason": "projection_person_search_index_unavailable",
                    "members": [],
                    "filtered_count": 0,
                    "filter_source": "projection_person_search_index",
                    "index_filter_readiness": {},
                }
        if str(result.get("status") or "") != "ready":
            return {
                "status": "not_ready",
                "reason": "projection_person_search_index_unavailable",
                "members": [],
                "filtered_count": 0,
                "filter_source": "projection_person_search_index",
                "index_filter_readiness": dict(result.get("index_filter_readiness") or {})
                or self._index_filter_readiness_payload(self.store.repos.serving_projection.get(projection_id)),
            }
        members = self._hydrate_index_page_members(
            projection_id,
            index_result=result,
            requested_offset=offset,
            requested_limit=limit,
        )
        if members is None:
            return {
                "status": "not_ready",
                "reason": "projection_person_search_index_unavailable",
                "members": [],
                "filtered_count": 0,
                "filter_source": "projection_person_search_index",
                "index_filter_readiness": dict(result.get("index_filter_readiness") or {}),
            }
        return {
            "status": "ready",
            "members": members,
            "filtered_count": int(result.get("matched_count") or 0),
            "filter_source": "projection_person_search_index",
            "index_filter_readiness": dict(result.get("index_filter_readiness") or {}),
        }

    def _hydrate_index_page_members(
        self,
        projection_id: str,
        *,
        index_result: dict[str, Any],
        requested_offset: int,
        requested_limit: int,
    ) -> list[dict[str, Any]] | None:
        raw_keys = list(index_result.get("candidate_identity_keys") or [])
        keys = [str(key or "").strip() for key in raw_keys]
        matched_count = _public_count(index_result.get("matched_count"))
        normalized_offset = max(0, int(requested_offset or 0))
        normalized_limit = min(max(1, int(requested_limit or 1)), 250)
        expected_page_count = min(normalized_limit, max(0, matched_count - normalized_offset))
        if (
            _public_count(index_result.get("offset")) != normalized_offset
            or any(not key for key in keys)
            or len(keys) != len(set(keys))
            or len(keys) != expected_page_count
        ):
            return None
        if not keys:
            return []
        members = self.store.repos.serving_projection.list_members_by_identity_keys(
            projection_id,
            keys,
            visible_only=True,
        )
        members_by_key: dict[str, dict[str, Any]] = {}
        for member in members:
            candidate_key = str(member.get("candidate_identity_key") or "").strip()
            if (
                not candidate_key
                or candidate_key not in keys
                or candidate_key in members_by_key
                or str(member.get("projection_id") or "").strip() != projection_id
                or str(member.get("visibility_state") or "").strip() != _VISIBLE_MEMBER_STATE
            ):
                return None
            members_by_key[candidate_key] = member
        if set(members_by_key) != set(keys):
            return None
        return [members_by_key[key] for key in keys]

    @staticmethod
    def _field_visibility_payload() -> dict[str, Any]:
        return {
            "included": ["public_summary", "projection_metrics", "crm_overlay_summary"],
            "excluded": ["restricted_contact", "internal_evidence", "raw_profile", "debug"],
        }

    def _public_projection_payload(
        self,
        projection: dict[str, Any],
        *,
        visible_count: int,
        readiness_counts: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        payload = dict(projection or {})
        counts = dict(payload.get("counts") or {})
        stored_member_count = max(
            _public_count(counts.get("member_count")),
            _public_count(counts.get("result_count")),
            _public_count(counts.get("candidate_count")),
            _public_count(counts.get("visible_member_count")),
            visible_count,
        )
        if visible_count >= 0:
            # Public projection APIs serve visible membership only. Persisted
            # projection counts may include hidden/manual-excluded rows for
            # audit, but they must not leak into board denominators.
            counts["result_count"] = visible_count
            counts["candidate_count"] = visible_count
            counts["visible_member_count"] = visible_count
            if stored_member_count > visible_count:
                counts["member_count"] = stored_member_count
            counts.setdefault("count_scope", "exact_projection")
        readiness = dict(payload.get("readiness") or {})
        normalized_readiness_counts = dict(readiness_counts or {})
        if normalized_readiness_counts:
            # Public projection readiness is derived from visible members at
            # read time. Persisted readiness mirrors can lag after event-time
            # board-visible updates, so they are diagnostics rather than the
            # public reader's source of truth.
            readiness["row_count"] = int(normalized_readiness_counts.get("row_count") or visible_count or 0)
            readiness["profile_required_count"] = int(normalized_readiness_counts.get("profile_required_count") or 0)
            readiness["profile_ready_count"] = int(normalized_readiness_counts.get("profile_ready_count") or 0)
            readiness["card_ready_count"] = int(normalized_readiness_counts.get("card_ready_count") or 0)
            readiness["count_scope"] = "exact_projection"
            counts["profile_fetch_required_count"] = readiness.get("profile_required_count") or 0
            counts["profile_fetched_count"] = readiness.get("profile_ready_count") or 0
            counts["card_materialized_count"] = readiness.get("card_ready_count") or 0
            counts["row_shell_count"] = readiness.get("row_count") or visible_count or 0
        return {
            "projection_id": str(payload.get("projection_id") or "").strip(),
            "projection_type": str(payload.get("projection_type") or "").strip(),
            "collection_id": str(payload.get("collection_id") or "").strip(),
            "source_run_id": str(payload.get("source_run_id") or "").strip(),
            "projection_version": str(payload.get("projection_version") or "").strip(),
            "state": str(payload.get("state") or "").strip(),
            "scope_label": str(payload.get("scope_label") or "").strip(),
            "scope_spec": dict(payload.get("scope_spec") or {}),
            "counts": counts,
            "readiness": readiness,
            "provenance": dict(payload.get("provenance") or {}),
            "manual_overlay_version": str(payload.get("manual_overlay_version") or "").strip(),
            "raw_profile_index_watermark": str(payload.get("raw_profile_index_watermark") or "").strip(),
            "evidence_index_watermark": str(payload.get("evidence_index_watermark") or "").strip(),
            "visible_member_count": visible_count,
            "field_visibility": self._field_visibility_payload(),
            "index_filter_readiness": self._index_filter_readiness_payload(
                {**payload, "counts": counts, "readiness": readiness}
            ),
            "read_contract": {
                "source": "serving_projection_members",
                "fallback_used": False,
                "fail_closed": True,
            },
            "published_at": str(payload.get("published_at") or "").strip(),
            "created_at": str(payload.get("created_at") or "").strip(),
            "updated_at": str(payload.get("updated_at") or "").strip(),
        }

    def _projection_facet_summary(self, projection: dict[str, Any]) -> dict[str, Any]:
        counts = dict(projection.get("counts") or {})
        public_facet_counts = dict(counts.get("public_facet_counts") or {})
        if not public_facet_counts:
            return {
                "status": "unavailable",
                "count_scope": "unavailable",
                "reason": "projection_facet_build_product_missing",
            }
        summary = public_facet_summary_from_counts(public_facet_counts)
        if not summary:
            return {
                "status": "unavailable",
                "count_scope": "unavailable",
                "reason": "projection_facet_counts_empty",
            }
        visible_count = _public_count(projection.get("visible_member_count") or counts.get("visible_member_count"))
        facet_candidate_count = _public_count(
            summary.get("candidate_count") or public_facet_counts.get("candidate_count")
        )
        if visible_count > 0 and facet_candidate_count != visible_count:
            return {
                "status": "unavailable",
                "count_scope": "unavailable",
                "reason": "projection_facet_count_mismatch_visible_members",
                "expected_candidate_count": visible_count,
                "actual_candidate_count": facet_candidate_count,
            }
        summary["status"] = "complete"
        summary["count_scope"] = str(
            public_facet_counts.get("count_scope") or counts.get("count_scope") or "exact_projection"
        )
        return summary

    def _projection_filter_contract(
        self,
        *,
        projection: dict[str, Any],
        applied_filter: dict[str, Any],
        filter_signature: str,
        source: str,
        row_filter_scope: str,
        fallback_used: bool,
        fallback_reason: str = "",
    ) -> dict[str, Any]:
        """Build the public filter contract from canonical projection state.

        Generic job candidate pages default to ``global_full_population``.
        Projection pages must not inherit that default: their facet counts are
        either exact products of ``counts.public_facet_counts`` or explicitly
        unavailable until that projection build product exists.
        """

        contract = candidate_page_filter_contract(
            applied_filter=applied_filter,
            filter_signature=filter_signature,
            source=source,
            row_filter_scope=row_filter_scope,
        )
        contract.update(self._projection_filter_count_contract(projection))
        contract["fallback_used"] = bool(fallback_used)
        if fallback_reason:
            contract["fallback_reason"] = fallback_reason
        return contract

    def _projection_filter_count_contract(self, projection: dict[str, Any]) -> dict[str, Any]:
        projection_id = str(dict(projection or {}).get("projection_id") or "").strip()
        facet_summary = self._projection_facet_summary(projection)
        if str(facet_summary.get("status") or "").strip() == "complete":
            return {
                "facet_count_scope": str(facet_summary.get("count_scope") or "exact_projection").strip()
                or "exact_projection",
                "facet_summary_source": "serving_projection_public_facet_counts",
                "facet_summary_projection_id": projection_id,
            }
        return {
            "facet_count_scope": "unavailable",
            "facet_unavailable_reason": str(
                facet_summary.get("reason") or "projection_facet_build_product_missing"
            ).strip()
            or "projection_facet_build_product_missing",
            "facet_summary_projection_id": projection_id,
        }

    def projection_facet_readiness_contract(self, projection: dict[str, Any]) -> dict[str, Any]:
        """Public owner adapter for projection facet/filter readiness.

        Job/run endpoints may expose projection-backed board runtime state, but
        they must not re-derive facet readiness from legacy overlays or page
        windows. Keep the source-of-truth logic in this reader and let job
        endpoints consume this bounded contract.
        """

        facet_summary = self._projection_facet_summary(projection)
        filter_contract = self._projection_filter_count_contract(projection)
        return {
            "facet_summary": facet_summary,
            "filter_count_contract": filter_contract,
        }

    @staticmethod
    def _index_filter_readiness_payload(projection: dict[str, Any]) -> dict[str, Any]:
        payload = dict(projection or {})
        counts = dict(payload.get("counts") or {})
        readiness = dict(payload.get("readiness") or {})
        raw_watermark = str(payload.get("raw_profile_index_watermark") or "").strip()
        evidence_watermark = str(payload.get("evidence_index_watermark") or "").strip()
        count_scope = str(
            readiness.get("index_count_scope")
            or counts.get("index_count_scope")
            or ("exact_projection" if raw_watermark or evidence_watermark else "unavailable")
        ).strip()
        return {
            "raw_profile_index_watermark": raw_watermark,
            "evidence_index_watermark": evidence_watermark,
            "count_scope": count_scope or "unavailable",
            "profile_fetched_at": str(readiness.get("profile_fetched_at") or "").strip(),
            "profile_indexed_at": str(readiness.get("profile_indexed_at") or "").strip(),
            "freshness_timezone": "Asia/Shanghai",
        }

    @staticmethod
    def _compact_crm_overlay_summary(record: dict[str, Any]) -> dict[str, Any]:
        if not record:
            return {"in_crm": False}
        metadata = dict(record.get("metadata") or {})
        return {
            "in_crm": True,
            "crm_record_id": str(record.get("crm_record_id") or "").strip(),
            "lifecycle_status": str(record.get("lifecycle_status") or "").strip(),
            "visibility_status": str(record.get("visibility_status") or "").strip(),
            "stage": str(metadata.get("current_stage") or "").strip(),
            "owner_user_id": str(record.get("owner_user_id") or "").strip(),
            "updated_at": str(record.get("updated_at") or "").strip(),
        }

    @staticmethod
    def _public_assertion_payload(assertion: dict[str, Any]) -> dict[str, Any]:
        metadata = dict(assertion.get("metadata") or {})
        assertion_type = str(assertion.get("assertion_type") or "").strip()
        restricted_contact = assertion_type in {"primary_email", "email", "phone"}
        payload = {
            "assertion_id": str(assertion.get("assertion_id") or "").strip(),
            "assertion_type": assertion_type,
            "authority": str(assertion.get("authority") or "").strip(),
            "verification_status": str(assertion.get("verification_status") or "").strip(),
            "source_evidence_id": str(assertion.get("source_evidence_id") or "").strip(),
            "source_crm_event_id": str(assertion.get("source_crm_event_id") or "").strip(),
            "source_run_id": str(assertion.get("source_run_id") or "").strip(),
            "confidence_score": assertion.get("confidence_score"),
            "export_policy": str(metadata.get("export_policy") or "").strip(),
            "updated_at": str(assertion.get("updated_at") or "").strip(),
        }
        if not restricted_contact or str(assertion.get("verification_status") or "") == "active":
            payload["value"] = str(assertion.get("value") or "").strip()
            payload["normalized_value"] = str(assertion.get("normalized_value") or "").strip()
        else:
            payload["value_redacted"] = True
        return payload

    @staticmethod
    def _public_asset_payload(asset: dict[str, Any]) -> dict[str, Any]:
        visibility_scope = str(asset.get("visibility_scope") or "").strip()
        payload = {
            "asset_id": str(asset.get("asset_id") or "").strip(),
            "asset_type": str(asset.get("asset_type") or "").strip(),
            "source_kind": str(asset.get("source_kind") or "").strip(),
            "source_run_id": str(asset.get("source_run_id") or "").strip(),
            "source_projection_id": str(asset.get("source_projection_id") or "").strip(),
            "visibility_scope": visibility_scope,
            "status": str(asset.get("status") or "").strip(),
            "fetched_at": str(asset.get("fetched_at") or "").strip(),
            "updated_at": str(asset.get("updated_at") or "").strip(),
        }
        if visibility_scope == "public_summary":
            payload["content_ref"] = str(asset.get("content_ref") or "").strip()
            payload["source_url"] = str(asset.get("source_url") or "").strip()
        return payload

    @staticmethod
    def _assertion_summary_payload(assertions: list[dict[str, Any]]) -> dict[str, Any]:
        counts: dict[str, int] = {}
        active_types: list[str] = []
        needs_review_types: list[str] = []
        for assertion in assertions:
            assertion_type = str(assertion.get("assertion_type") or "").strip()
            if not assertion_type:
                continue
            counts[assertion_type] = counts.get(assertion_type, 0) + 1
            status = str(assertion.get("verification_status") or "").strip()
            if status == "active" and assertion_type not in active_types:
                active_types.append(assertion_type)
            if status == "needs_review" and assertion_type not in needs_review_types:
                needs_review_types.append(assertion_type)
        return {
            "assertion_count": len(assertions),
            "assertion_type_counts": counts,
            "active_types": active_types,
            "needs_review_types": needs_review_types,
        }

    @staticmethod
    def _asset_summary_payload(assets: list[dict[str, Any]]) -> dict[str, Any]:
        counts: dict[str, int] = {}
        public_asset_ids: list[str] = []
        for asset in assets:
            asset_type = str(asset.get("asset_type") or "").strip()
            if asset_type:
                counts[asset_type] = counts.get(asset_type, 0) + 1
            if str(asset.get("visibility_scope") or "") == "public_summary":
                public_asset_ids.append(str(asset.get("asset_id") or ""))
        return {
            "asset_count": len(assets),
            "asset_type_counts": counts,
            "public_summary_asset_ids": [item for item in public_asset_ids if item],
        }

    @staticmethod
    def _media_summary_payload(assets: list[dict[str, Any]]) -> dict[str, Any]:
        avatar_asset: dict[str, Any] = {}
        for asset in assets:
            candidate = dict(asset or {})
            if str(candidate.get("asset_type") or "").strip() != "avatar_media":
                continue
            if str(candidate.get("visibility_scope") or "").strip() != "public_summary":
                continue
            if str(candidate.get("status") or "").strip() != "available":
                continue
            if not str(candidate.get("content_ref") or "").strip():
                continue
            avatar_asset = candidate
            break
        if avatar_asset:
            avatar_url = media_asset_frontend_url(avatar_asset) or str(avatar_asset.get("content_ref") or "").strip()
            return {
                "avatar_status": "available",
                "avatar_asset_id": str(avatar_asset.get("asset_id") or "").strip(),
                "avatar_url": avatar_url,
                "avatar_source_url": str(avatar_asset.get("source_url") or "").strip(),
                "media_contract": {
                    "source": "PersonAsset.avatar_media",
                    "fallback_source": "",
                    "fallback_used": False,
                    "fail_closed": True,
                    "owner": "PersonAssetWriter",
                    "served_by": "media_asset_read_contract_v1" if avatar_url.startswith("/api/media/assets/") else "",
                },
            }
        return {
            "avatar_status": "avatar_unavailable",
            "avatar_asset_id": "",
            "avatar_url": "",
            "avatar_source_url": "",
            "media_contract": {
                "source": "PersonAsset.avatar_media",
                "fallback_source": "provider_avatar_url_metadata",
                "fallback_used": False,
                "fail_closed": True,
                "reason": "avatar_media_asset_not_available",
                "future_owner": "person_media_asset_owner",
            },
        }

    @staticmethod
    def _evidence_summary_payload(evidence: list[dict[str, Any]]) -> dict[str, Any]:
        counts: dict[str, int] = {}
        publishable_count = 0
        for item in evidence:
            evidence_type = str(item.get("evidence_type") or "").strip()
            if evidence_type:
                counts[evidence_type] = counts.get(evidence_type, 0) + 1
            if bool(item.get("publishable")):
                publishable_count += 1
        return {
            "evidence_count": len(evidence),
            "publishable_evidence_count": publishable_count,
            "evidence_type_counts": counts,
            "count_scope": "exact_person" if evidence else "unavailable",
        }

    @staticmethod
    def _public_member_payload(member: dict[str, Any]) -> dict[str, Any]:
        payload = dict(member or {})
        public_summary = _scrub_public_projection_payload(dict(payload.get("public_summary") or {}))
        projection_metrics = _scrub_public_projection_payload(dict(payload.get("projection_metrics") or {}))
        crm_overlay_summary = _scrub_public_projection_payload(dict(payload.get("crm_overlay_summary") or {}))
        media_summary = _scrub_public_projection_payload(dict(payload.get("media_summary") or {}))
        row = {
            "projection_id": str(payload.get("projection_id") or "").strip(),
            "candidate_identity_key": str(payload.get("candidate_identity_key") or "").strip(),
            "person_identity_key": str(payload.get("person_identity_key") or "").strip(),
            "profile_url_key": str(payload.get("profile_url_key") or "").strip(),
            "candidate_id": str(payload.get("candidate_id") or "").strip(),
            "rank_index": int(payload.get("rank_index") or 0),
            "rank_key": str(payload.get("rank_key") or "").strip(),
            "lane": str(payload.get("lane") or "").strip(),
            "employment_scope": str(payload.get("employment_scope") or "").strip(),
            "source_shard_key": str(payload.get("source_shard_key") or "").strip(),
            "source_run_id": str(payload.get("source_run_id") or "").strip(),
            "row_readiness": str(payload.get("row_readiness") or "").strip(),
            "profile_readiness": str(payload.get("profile_readiness") or "").strip(),
            "card_readiness": str(payload.get("card_readiness") or "").strip(),
            "visibility_state": str(payload.get("visibility_state") or _VISIBLE_MEMBER_STATE).strip(),
            "public_summary": public_summary,
            "projection_metrics": projection_metrics,
            "crm_overlay_summary": crm_overlay_summary,
            "published_at": str(payload.get("published_at") or "").strip(),
            "updated_at": str(payload.get("updated_at") or "").strip(),
        }
        if media_summary:
            row["media_summary"] = media_summary
        return row

    @staticmethod
    def _member_filter_record(member: dict[str, Any]) -> dict[str, Any]:
        payload = dict(member or {})
        public_summary = _scrub_public_projection_payload(dict(payload.get("public_summary") or {}))
        projection_metrics = _scrub_public_projection_payload(dict(payload.get("projection_metrics") or {}))
        crm_overlay_summary = _scrub_public_projection_payload(dict(payload.get("crm_overlay_summary") or {}))
        candidate_id = (
            str(payload.get("candidate_id") or "").strip()
            or str(public_summary.get("candidate_id") or public_summary.get("id") or "").strip()
        )
        return {
            **public_summary,
            "candidate_id": candidate_id,
            "id": candidate_id,
            "employment_status": str(
                payload.get("employment_scope")
                or public_summary.get("employment_status")
                or public_summary.get("status")
                or ""
            ).strip(),
            "source_dataset": str(
                payload.get("source_shard_key") or payload.get("lane") or public_summary.get("source_dataset") or ""
            ).strip(),
            "has_profile_detail": public_summary.get("has_profile_detail")
            or projection_metrics.get("has_profile_detail"),
            "needs_profile_completion": public_summary.get("needs_profile_completion")
            or projection_metrics.get("needs_profile_completion"),
            "low_profile_richness": public_summary.get("low_profile_richness")
            or projection_metrics.get("low_profile_richness"),
            "crm_overlay_summary": crm_overlay_summary,
            "projection_id": str(payload.get("projection_id") or "").strip(),
            "candidate_identity_key": str(payload.get("candidate_identity_key") or "").strip(),
            "person_identity_key": str(payload.get("person_identity_key") or "").strip(),
            "profile_url_key": str(payload.get("profile_url_key") or "").strip(),
            "rank_index": int(payload.get("rank_index") or 0),
            "rank_key": str(payload.get("rank_key") or "").strip(),
            "lane": str(payload.get("lane") or "").strip(),
            "source_shard_key": str(payload.get("source_shard_key") or "").strip(),
            "source_run_id": str(payload.get("source_run_id") or "").strip(),
            "row_readiness": str(payload.get("row_readiness") or "").strip(),
            "profile_readiness": str(payload.get("profile_readiness") or "").strip(),
            "card_readiness": str(payload.get("card_readiness") or "").strip(),
            "visibility_state": str(payload.get("visibility_state") or _VISIBLE_MEMBER_STATE).strip(),
            "public_summary": public_summary,
            "projection_metrics": projection_metrics,
            "published_at": str(payload.get("published_at") or "").strip(),
            "updated_at": str(payload.get("updated_at") or "").strip(),
        }

    @staticmethod
    def _projection_error(
        reason: str, *, projection_id: str = "", projection: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "status": "not_ready",
            "reason": reason,
            "projection_id": str(projection_id or "").strip(),
            "read_contract": {
                "source": "serving_projection_members",
                "fallback_used": False,
                "fail_closed": True,
            },
        }
        if projection:
            payload["projection_state"] = str(dict(projection).get("state") or "").strip()
        return payload


def _candidate_filter_is_keyword_only(candidate_filter: dict[str, Any]) -> bool:
    source = dict(candidate_filter or {})
    if not str(source.get("search_keyword") or "").strip():
        return False
    for key in (
        "recall_buckets",
        "employment_statuses",
        "locations",
        "function_buckets",
        "layer_includes",
        "layer_excludes",
        "audit_statuses",
    ):
        values = [str(item or "").strip() for item in list(source.get(key) or []) if str(item or "").strip()]
        if key == "recall_buckets":
            values = [item for item in values if item != "all"]
        elif key == "employment_statuses":
            values = [item for item in values if item not in {"current", "former"}]
        elif key == "locations":
            values = [item for item in values if item not in {"us", "other", "unknown"}]
        elif key == "function_buckets":
            values = [
                item
                for item in values
                if item not in {"research", "engineering", "product_management", "other", "unknown"}
            ]
        elif key == "layer_includes":
            values = [item for item in values if item != "layer_0"]
        elif key == "audit_statuses":
            values = [
                item
                for item in values
                if item
                not in {
                    "no_review_needed",
                    "needs_review",
                    "needs_profile_completion",
                    "low_profile_richness",
                    "verified_keep",
                    "verified_exclude",
                }
            ]
        if values:
            return False
    return True


def _legacy_projection_filter_scan_fallback_enabled() -> bool:
    return str(os.getenv("SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _scrub_public_projection_payload(value: Any) -> Any:
    if isinstance(value, dict):
        scrubbed: dict[str, Any] = {}
        for key, item in value.items():
            normalized_key = str(key or "").strip()
            if not normalized_key:
                continue
            if normalized_key.lower() in _RESTRICTED_PUBLIC_ROW_KEYS:
                continue
            scrubbed[normalized_key] = _scrub_public_projection_payload(item)
        return scrubbed
    if isinstance(value, list):
        return [_scrub_public_projection_payload(item) for item in value]
    return value


def _public_count(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0
