from __future__ import annotations

from typing import Any

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
        existing_link = self.store.repos.serving_projection.get_run_link(normalized_run_id)
        effective_projection_id = str(projection_id or existing_link.get("projection_id") or "").strip()
        projection_payload = {
            "projection_id": effective_projection_id,
            "projection_type": "run_scope_projection",
            "collection_id": str(collection_id or existing_link.get("collection_id") or "").strip(),
            "source_run_id": normalized_run_id,
            "state": state,
            "scope_label": str(scope_label or "").strip(),
            "scope_spec": dict(scope_spec or {}),
            "counts": _counts_with_member_floor(counts or {}, members),
            "readiness": dict(readiness or {}),
            "provenance": dict(provenance or {}),
            "metadata": _metadata_with_writer(metadata or {}, self.writer_id),
        }
        projection = self.store.repos.serving_projection.upsert(projection_payload)
        persisted_projection_id = _require_non_empty(projection.get("projection_id"), "projection_id")
        if replace_members:
            member_count = self.store.repos.serving_projection.replace_members(persisted_projection_id, members)
        else:
            member_count = self.store.repos.serving_projection.upsert_members(persisted_projection_id, members)
        link = self.store.repos.serving_projection.upsert_run_link(
            {
                "run_id": normalized_run_id,
                "projection_id": persisted_projection_id,
                "projection_type": "run_scope_projection",
                "collection_id": projection.get("collection_id") or "",
                "created_by": self.writer_id,
                "metadata": {
                    "writer_id": self.writer_id,
                    "projection_state": projection.get("state") or "",
                },
            }
        )
        return {
            "projection": projection,
            "link": link,
            "member_count": member_count,
        }

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
        existing_pointer = self.store.repos.serving_projection.get_authoritative_pointer(normalized_collection_id)
        effective_projection_id = str(projection_id or "").strip()
        if (
            not effective_projection_id
            and str(existing_pointer.get("active_collection_version") or "") == normalized_version
        ):
            effective_projection_id = str(existing_pointer.get("active_projection_id") or "").strip()
        projection = self.store.repos.serving_projection.upsert(
            {
                "projection_id": effective_projection_id,
                "projection_type": "collection_authoritative_projection",
                "collection_id": normalized_collection_id,
                "state": state,
                "scope_label": str(scope_label or "").strip(),
                "scope_spec": dict(scope_spec or {}),
                "counts": _counts_with_member_floor(counts or {}, members),
                "readiness": dict(readiness or {}),
                "provenance": dict(provenance or {}),
                "metadata": _metadata_with_writer(metadata or {}, self.writer_id),
            }
        )
        persisted_projection_id = _require_non_empty(projection.get("projection_id"), "projection_id")
        if replace_members:
            member_count = self.store.repos.serving_projection.replace_members(persisted_projection_id, members)
        else:
            member_count = self.store.repos.serving_projection.upsert_members(persisted_projection_id, members)
        pointer = self.store.repos.serving_projection.upsert_authoritative_pointer(
            {
                "collection_id": normalized_collection_id,
                "active_projection_id": persisted_projection_id,
                "active_collection_version": normalized_version,
                "writer_id": self.writer_id,
                "metadata": {
                    "writer_id": self.writer_id,
                    "projection_state": projection.get("state") or "",
                },
            }
        )
        return {
            "projection": projection,
            "pointer": pointer,
            "member_count": member_count,
        }


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
