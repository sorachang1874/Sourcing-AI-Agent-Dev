from __future__ import annotations

from typing import Any

from .filter_projection_publication_owner import (
    FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE,
    FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
    FilterProjectionPublicationFoundation,
    reject_filter_projection_foundation_member_carriers,
    reject_filter_projection_foundation_metadata,
    strip_filter_projection_foundation_carriers,
    validate_filter_projection_publication_foundation,
)
from .storage import ControlPlaneStore


class ServingProjectionWriter:
    """Owner-facing writer facade for canonical serving projection foundation records."""

    def __init__(self, store: ControlPlaneStore, *, writer_id: str = "serving_projection_writer_v1") -> None:
        self.store = store
        self.writer_id = str(writer_id or "serving_projection_writer_v1").strip() or "serving_projection_writer_v1"

    def publish_run_scope_projection(
        self,
        *,
        run_id: str,
        collection_id: str = "",
        projection_id: str = "",
        members: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        replace_members: bool = False,
        scope_label: str = "",
        scope_spec: dict[str, Any] | None = None,
        counts: dict[str, Any] | None = None,
        readiness: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        state: str = "serving",
    ) -> dict[str, Any]:
        normalized_run_id = _require_non_empty(run_id, "run_id")
        reject_filter_projection_foundation_metadata(metadata)
        reject_filter_projection_foundation_member_carriers(members)
        projection_payload = {
            "projection_id": str(projection_id or "").strip(),
            "projection_type": "run_scope_projection",
            "collection_id": str(collection_id or "").strip(),
            "source_run_id": normalized_run_id,
            "state": state,
            "scope_label": str(scope_label or "").strip(),
            "scope_spec": dict(scope_spec or {}),
            "counts": _counts_with_member_floor(counts or {}, members),
            "readiness": dict(readiness or {}),
            "provenance": dict(provenance or {}),
            "metadata": _metadata_with_writer(metadata or {}, self.writer_id),
        }
        return self.store.repos.serving_projection.publish_run_scope_projection(
            projection_payload=projection_payload,
            run_link_payload={
                "run_id": normalized_run_id,
                "projection_type": "run_scope_projection",
                "collection_id": str(collection_id or "").strip(),
                "created_by": self.writer_id,
                "metadata": {
                    "writer_id": self.writer_id,
                    "projection_state": state,
                },
            },
            members=members,
            replace_members=replace_members,
        )

    def publish_filter_projection_foundation_run_scope_projection(
        self,
        *,
        foundation: FilterProjectionPublicationFoundation,
        collection_id: str = "",
        projection_id: str = "",
        scope_label: str = "",
        scope_spec: dict[str, Any] | None = None,
        counts: dict[str, Any] | None = None,
        readiness: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Exercise the atomic UoW with a non-product foundation carrier."""

        validate_filter_projection_publication_foundation(foundation)
        reject_filter_projection_foundation_metadata(metadata)
        members = foundation.members
        projection_payload = {
            "projection_id": str(projection_id or "").strip(),
            "projection_type": "run_scope_projection",
            "collection_id": str(collection_id or "").strip(),
            "source_run_id": foundation.source_run_id,
            "state": FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE,
            "scope_label": str(scope_label or "").strip(),
            "scope_spec": dict(scope_spec or {}),
            "counts": _counts_with_member_floor(counts or {}, members),
            "readiness": dict(readiness or {}),
            "provenance": dict(provenance or {}),
            "metadata": _metadata_with_writer(metadata or {}, self.writer_id),
        }
        return self.store.repos.serving_projection.publish_filter_projection_foundation_run_scope_projection(
            foundation=foundation,
            projection_payload=projection_payload,
            run_link_payload={
                "run_id": foundation.source_run_id,
                "projection_type": "run_scope_projection",
                "collection_id": str(collection_id or "").strip(),
                "created_by": self.writer_id,
                "metadata": {
                    "writer_id": self.writer_id,
                    "projection_state": FILTER_PROJECTION_FOUNDATION_PROJECTION_STATE,
                    "route_type": FILTER_PROJECTION_FOUNDATION_ROUTE_TYPE,
                },
            },
        )

    def publish_collection_authoritative_projection(
        self,
        *,
        collection_id: str,
        active_collection_version: str,
        projection_id: str = "",
        members: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        replace_members: bool = False,
        scope_label: str = "",
        scope_spec: dict[str, Any] | None = None,
        counts: dict[str, Any] | None = None,
        readiness: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        state: str = "serving",
    ) -> dict[str, Any]:
        normalized_collection_id = _require_non_empty(collection_id, "collection_id")
        normalized_version = _require_non_empty(active_collection_version, "active_collection_version")
        stripped_metadata, stripped_members = strip_filter_projection_foundation_carriers(
            metadata=metadata,
            members=members,
        )
        projection_payload = {
            "projection_id": str(projection_id or "").strip(),
            "projection_type": "collection_authoritative_projection",
            "collection_id": normalized_collection_id,
            "source_collection_version": normalized_version,
            "state": state,
            "scope_label": str(scope_label or "").strip(),
            "scope_spec": dict(scope_spec or {}),
            "counts": _counts_with_member_floor(counts or {}, stripped_members),
            "readiness": dict(readiness or {}),
            "provenance": dict(provenance or {}),
            "metadata": _metadata_with_writer(stripped_metadata, self.writer_id),
        }
        return self.store.repos.serving_projection.publish_collection_authoritative_projection(
            projection_payload=projection_payload,
            pointer_payload={
                "collection_id": normalized_collection_id,
                "active_collection_version": normalized_version,
                "writer_id": self.writer_id,
                "metadata": {
                    "writer_id": self.writer_id,
                    "projection_state": state,
                },
            },
            members=stripped_members,
            replace_members=replace_members,
        )


def _require_non_empty(value: Any, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _metadata_with_writer(metadata: dict[str, Any], writer_id: str) -> dict[str, Any]:
    payload = dict(metadata or {})
    payload.setdefault("writer_id", writer_id)
    return payload


def _counts_with_member_floor(
    counts: dict[str, Any],
    members: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> dict[str, Any]:
    payload = dict(counts or {})
    member_count = len(
        {
            str(
                dict(member or {}).get("candidate_identity_key")
                or dict(member or {}).get("person_identity_key")
                or dict(member or {}).get("profile_url_key")
                or dict(member or {}).get("candidate_id")
                or ""
            ).strip()
            for member in list(members or [])
            if isinstance(member, dict)
            and str(
                member.get("candidate_identity_key")
                or member.get("person_identity_key")
                or member.get("profile_url_key")
                or member.get("candidate_id")
                or ""
            ).strip()
        }
    )
    if member_count and int(payload.get("result_count") or payload.get("candidate_count") or 0) <= 0:
        payload["result_count"] = member_count
        payload.setdefault("count_scope", "exact_projection")
    return payload
