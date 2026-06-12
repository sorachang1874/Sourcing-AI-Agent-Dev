from __future__ import annotations

from hashlib import sha1
from typing import Any

from .person_identity import build_person_summary_view, resolve_person_identity_key
from .storage import ControlPlaneStore

_CRM_STAGE_CATEGORIES = {
    "new": "open",
    "researching": "open",
    "outreach_ready": "open",
    "contacted_waiting": "waiting",
    "responded": "open",
    "interview_completed": "terminal_success",
    "accepted": "terminal_success",
    "rejected": "terminal_loss",
    "do_not_contact": "blocked",
    "archived": "archived",
}

_TARGET_FOLLOW_UP_TO_CRM_STAGE = {
    "pending_outreach": "outreach_ready",
    "contacted_waiting": "contacted_waiting",
    "interview_completed": "interview_completed",
    "accepted": "accepted",
    "rejected": "rejected",
}


class CRMWriter:
    """Owner-facing CRM writer for person-first CRM state."""

    def __init__(self, store: ControlPlaneStore, *, writer_id: str = "crm_writer_v1") -> None:
        self.store = store
        self.writer_id = str(writer_id or "crm_writer_v1").strip() or "crm_writer_v1"

    def add_projection_member_to_crm(
        self,
        *,
        projection_id: str,
        candidate_identity_key: str,
        workspace_id: str = "default",
        actor_type: str = "user",
        actor_id: str = "",
        idempotency_key: str = "",
        pipeline_id: str = "default_sourcing",
        stage: str = "new",
        source_reason: str = "selected_from_projection",
    ) -> dict[str, Any]:
        normalized_projection_id = _require_non_empty(projection_id, "projection_id")
        normalized_candidate_key = _require_non_empty(candidate_identity_key, "candidate_identity_key")
        projection = self.store.get_serving_projection(normalized_projection_id)
        if not projection:
            return {
                "status": "not_found",
                "reason": "projection_not_found",
                "projection_id": normalized_projection_id,
            }
        member = self.store.get_serving_projection_member(normalized_projection_id, normalized_candidate_key)
        if not member:
            return {
                "status": "not_found",
                "reason": "projection_member_not_found",
                "projection_id": normalized_projection_id,
                "candidate_identity_key": normalized_candidate_key,
            }
        return self.add_person_to_crm(
            person_identity_key=str(member.get("person_identity_key") or ""),
            candidate_identity_key=str(member.get("candidate_identity_key") or normalized_candidate_key),
            collection_id=str(projection.get("collection_id") or ""),
            source_projection_id=normalized_projection_id,
            source_run_id=str(member.get("source_run_id") or projection.get("source_run_id") or ""),
            source_collection_id=str(projection.get("collection_id") or ""),
            public_summary=dict(member.get("public_summary") or {}),
            workspace_id=workspace_id,
            actor_type=actor_type,
            actor_id=actor_id,
            idempotency_key=idempotency_key
            or f"crm:add:{workspace_id}:{normalized_projection_id}:{normalized_candidate_key}",
            pipeline_id=pipeline_id,
            stage=stage,
            source_reason=source_reason,
        )

    def add_person_to_crm(
        self,
        *,
        person_identity_key: str,
        candidate_identity_key: str = "",
        collection_id: str = "",
        source_projection_id: str = "",
        source_run_id: str = "",
        source_collection_id: str = "",
        public_summary: dict[str, Any] | None = None,
        workspace_id: str = "default",
        actor_type: str = "user",
        actor_id: str = "",
        idempotency_key: str = "",
        pipeline_id: str = "default_sourcing",
        stage: str = "new",
        source_reason: str = "selected",
    ) -> dict[str, Any]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        summary = build_person_summary_view(
            dict(public_summary or {}),
            person_identity_key=person_identity_key,
            source_projection_id=source_projection_id,
            source_run_id=source_run_id,
        )
        normalized_person_key = resolve_person_identity_key(
            person_identity_key=person_identity_key,
            profile_url_key=str(summary.get("profile_url_key") or ""),
            linkedin_url=str(summary.get("linkedin_url") or ""),
            candidate_identity_key=candidate_identity_key,
            candidate_id=str(summary.get("candidate_id") or ""),
        )
        if not normalized_person_key:
            return {"status": "invalid", "reason": "person_identity_key_required"}
        effective_idempotency_key = (
            str(idempotency_key or "").strip()
            or f"crm:add:{normalized_workspace_id}:{normalized_person_key}:{source_projection_id}"
        )
        existing_event = self.store.get_crm_event_by_idempotency(
            effective_idempotency_key,
            workspace_id=normalized_workspace_id,
        )
        if existing_event:
            existing_record = self.store.get_crm_record_by_person_identity(
                normalized_person_key,
                workspace_id=normalized_workspace_id,
            )
            current_engagement = self.store.get_crm_engagement(
                str(existing_record.get("current_engagement_id") or "")
            )
            return {
                "status": "idempotent",
                "crm_record": existing_record,
                "crm_engagement": current_engagement,
                "crm_event": existing_event,
            }
        existing = self.store.get_crm_record_by_person_identity(
            normalized_person_key,
            workspace_id=normalized_workspace_id,
        )
        record = self.store.upsert_crm_record(
            {
                "crm_record_id": existing.get("crm_record_id") or "",
                "workspace_id": normalized_workspace_id,
                "person_identity_key": normalized_person_key,
                "candidate_identity_key": candidate_identity_key,
                "collection_id": collection_id or source_collection_id,
                "display_name_cache": str(summary.get("display_name") or summary.get("name") or ""),
                "headline_cache": str(summary.get("headline") or ""),
                "primary_company_cache": str(summary.get("current_company") or ""),
                "avatar_asset_id": str(summary.get("avatar_asset_id") or ""),
                "lifecycle_status": "active",
                "visibility_status": "normal",
                "source_projection_id": source_projection_id,
                "source_run_id": source_run_id,
                "source_collection_id": source_collection_id or collection_id,
                "source_reason": source_reason,
                "metadata": {
                    **dict(existing.get("metadata") or {}),
                    "writer_id": self.writer_id,
                    "current_stage": stage,
                    "candidate_id": str(summary.get("candidate_id") or ""),
                    "avatar_url_cache": str(summary.get("avatar_url") or ""),
                    "linkedin_url_cache": str(summary.get("linkedin_url") or ""),
                },
            }
        )
        engagement = self.store.upsert_crm_engagement(
            {
                "crm_record_id": record.get("crm_record_id") or "",
                "pipeline_id": pipeline_id,
                "stage": stage,
                "source_projection_id": source_projection_id,
                "source_run_id": source_run_id,
                "source_selection_reason": source_reason,
                "created_by_actor": str(actor_type or "user").strip() or "user",
                "metadata": {
                    "writer_id": self.writer_id,
                },
            }
        )
        event = self.store.append_crm_event(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": record.get("crm_record_id") or "",
                "engagement_id": engagement.get("engagement_id") or "",
                "person_identity_key": normalized_person_key,
                "event_type": "crm_record_created",
                "actor_type": str(actor_type or "user").strip() or "user",
                "actor_id": str(actor_id or "").strip(),
                "idempotency_key": effective_idempotency_key,
                "payload": {
                    "writer_id": self.writer_id,
                    "source_projection_id": source_projection_id,
                    "source_run_id": source_run_id,
                    "source_reason": source_reason,
                    "candidate_identity_key": candidate_identity_key,
                },
                "metadata": {"writer_id": self.writer_id},
            }
        )
        if engagement.get("engagement_id") and record.get("current_engagement_id") != engagement.get("engagement_id"):
            record = self.store.upsert_crm_record(
                {
                    **record,
                    "current_engagement_id": engagement.get("engagement_id") or "",
                    "metadata": {
                        **dict(record.get("metadata") or {}),
                        "current_engagement_id_set_by": self.writer_id,
                        "last_event_id": event.get("event_id") or "",
                    },
                }
            )
        return {
            "status": "upserted",
            "crm_record": record,
            "crm_engagement": engagement,
            "crm_event": event,
        }

    def update_crm_record(
        self,
        *,
        crm_record_id: str,
        workspace_id: str = "default",
        actor_type: str = "user",
        actor_id: str = "",
        idempotency_key: str = "",
        stage: str = "",
        follow_up_status: str = "",
        quality_score: Any = None,
        quality_score_present: bool = False,
        comment: str = "",
        comment_present: bool = False,
        display_patch: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_record_id = _require_non_empty(crm_record_id, "crm_record_id")
        record = self.store.get_crm_record(normalized_record_id)
        if not record:
            return {"status": "not_found", "reason": "crm_record_not_found", "crm_record_id": normalized_record_id}
        if str(record.get("workspace_id") or "default").strip() != normalized_workspace_id:
            return {
                "status": "not_found",
                "reason": "crm_record_not_found_in_workspace",
                "crm_record_id": normalized_record_id,
                "workspace_id": normalized_workspace_id,
            }

        current_engagement = self.store.get_crm_engagement(str(record.get("current_engagement_id") or ""))
        current_record_metadata = dict(record.get("metadata") or {})
        current_engagement_metadata = dict(current_engagement.get("metadata") or {})
        next_stage = _normalize_crm_stage(
            stage or _crm_stage_from_target_follow_up(follow_up_status) or current_engagement.get("stage") or current_record_metadata.get("current_stage")
        )
        parsed_quality_score = (
            _coerce_quality_score(quality_score)
            if quality_score_present
            else current_engagement.get("quality_score")
        )
        next_comment = str(comment or "").strip() if comment_present else str(current_engagement_metadata.get("comment") or "")
        patch = dict(display_patch or {})
        next_record_metadata = {
            **current_record_metadata,
            "writer_id": self.writer_id,
            "current_stage": next_stage,
            "last_update_actor_type": str(actor_type or "user").strip() or "user",
        }
        next_engagement_metadata = {
            **current_engagement_metadata,
            "writer_id": self.writer_id,
            "comment": next_comment,
            "last_update_actor_type": str(actor_type or "user").strip() or "user",
        }
        for metadata_key, payload_key in (
            ("avatar_url_cache", "avatar_url"),
            ("linkedin_url_cache", "linkedin_url"),
            ("candidate_id", "candidate_id"),
        ):
            patch_value = str(patch.get(payload_key) or "").strip()
            if patch_value:
                next_record_metadata[metadata_key] = patch_value

        updated_record = self.store.upsert_crm_record(
            {
                **record,
                "display_name_cache": _patch_non_empty(
                    patch.get("candidate_name"),
                    patch.get("display_name"),
                    record.get("display_name_cache"),
                ),
                "headline_cache": _patch_non_empty(patch.get("headline"), record.get("headline_cache")),
                "primary_company_cache": _patch_non_empty(
                    patch.get("current_company"),
                    record.get("primary_company_cache"),
                ),
                "current_engagement_id": str(
                    current_engagement.get("engagement_id")
                    or record.get("current_engagement_id")
                    or ""
                ),
                "metadata": next_record_metadata,
            }
        )
        updated_engagement = self.store.upsert_crm_engagement(
            {
                **current_engagement,
                "crm_record_id": normalized_record_id,
                "pipeline_id": str(current_engagement.get("pipeline_id") or "default_sourcing").strip()
                or "default_sourcing",
                "stage": next_stage,
                "stage_category": _crm_stage_category(next_stage),
                "quality_score": parsed_quality_score,
                "source_projection_id": str(
                    current_engagement.get("source_projection_id")
                    or updated_record.get("source_projection_id")
                    or ""
                ),
                "source_run_id": str(
                    current_engagement.get("source_run_id")
                    or updated_record.get("source_run_id")
                    or ""
                ),
                "source_selection_reason": str(
                    current_engagement.get("source_selection_reason")
                    or updated_record.get("source_reason")
                    or ""
                ),
                "created_by_actor": str(
                    current_engagement.get("created_by_actor")
                    or actor_type
                    or "user"
                ).strip()
                or "user",
                "metadata": next_engagement_metadata,
            }
        )
        if not str(updated_record.get("current_engagement_id") or "").strip() and updated_engagement.get("engagement_id"):
            updated_record = self.store.upsert_crm_record(
                {
                    **updated_record,
                    "current_engagement_id": str(updated_engagement.get("engagement_id") or ""),
                    "metadata": {
                        **dict(updated_record.get("metadata") or {}),
                        "current_engagement_id_set_by": self.writer_id,
                    },
                }
            )
        event = self.store.append_crm_event(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "engagement_id": str(updated_engagement.get("engagement_id") or ""),
                "person_identity_key": str(updated_record.get("person_identity_key") or ""),
                "event_type": "crm_engagement_updated",
                "actor_type": str(actor_type or "user").strip() or "user",
                "actor_id": str(actor_id or "").strip(),
                "idempotency_key": str(idempotency_key or "").strip(),
                "payload": {
                    "writer_id": self.writer_id,
                    "stage": next_stage,
                    "quality_score": parsed_quality_score,
                    "comment_updated": comment_present,
                    "display_patch_keys": sorted(str(key) for key in patch),
                },
                "metadata": {"writer_id": self.writer_id},
            }
        )
        return {
            "status": "updated",
            "crm_record": updated_record,
            "crm_engagement": updated_engagement,
            "crm_event": event,
            "write_contract": {
                "owner": "CRMWriter",
                "source": "crm_records+crm_engagements+crm_events",
                "legacy_target_candidates_written": False,
            },
        }

    def record_person_assertion_linked(
        self,
        *,
        workspace_id: str = "default",
        crm_record_id: str = "",
        engagement_id: str = "",
        person_identity_key: str,
        assertion_id: str,
        assertion_type: str,
        verification_status: str,
        actor_type: str = "system",
        actor_id: str = "",
        idempotency_key: str = "",
        source_projection_id: str = "",
        source_run_id: str = "",
        reason: str = "",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_person_key = _require_non_empty(person_identity_key, "person_identity_key")
        normalized_assertion_id = _require_non_empty(assertion_id, "assertion_id")
        event_payload = {
            **dict(payload or {}),
            "writer_id": self.writer_id,
            "assertion_id": normalized_assertion_id,
            "assertion_type": str(assertion_type or "").strip(),
            "verification_status": str(verification_status or "").strip(),
            "source_projection_id": str(source_projection_id or "").strip(),
            "source_run_id": str(source_run_id or "").strip(),
            "reason": str(reason or "").strip(),
        }
        return self.store.append_crm_event(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": str(crm_record_id or "").strip(),
                "engagement_id": str(engagement_id or "").strip(),
                "person_identity_key": normalized_person_key,
                "event_type": "person_assertion_linked",
                "actor_type": str(actor_type or "system").strip() or "system",
                "actor_id": str(actor_id or self.writer_id).strip() or self.writer_id,
                "idempotency_key": str(idempotency_key or f"crm:assertion-linked:{normalized_assertion_id}").strip(),
                "payload": event_payload,
                "metadata": {"writer_id": self.writer_id},
            }
        )

    def add_crm_note(
        self,
        *,
        crm_record_id: str,
        note: str,
        workspace_id: str = "default",
        actor_type: str = "user",
        actor_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_record_id = _require_non_empty(crm_record_id, "crm_record_id")
        normalized_note = _require_non_empty(note, "note")
        record = self.store.get_crm_record(normalized_record_id)
        if not record or str(record.get("workspace_id") or "default").strip() != normalized_workspace_id:
            return {"status": "not_found", "reason": "crm_record_not_found", "crm_record_id": normalized_record_id}
        effective_idempotency_key = str(idempotency_key or "").strip() or (
            f"crm:note:{normalized_workspace_id}:{normalized_record_id}:"
            f"{sha1(normalized_note.encode('utf-8')).hexdigest()[:16]}"
        )
        event = self.store.append_crm_event(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "engagement_id": str(record.get("current_engagement_id") or ""),
                "person_identity_key": str(record.get("person_identity_key") or ""),
                "event_type": "crm_note_added",
                "actor_type": str(actor_type or "user").strip() or "user",
                "actor_id": str(actor_id or "").strip(),
                "idempotency_key": effective_idempotency_key,
                "payload": {"writer_id": self.writer_id, "note": normalized_note},
                "metadata": {"writer_id": self.writer_id},
            }
        )
        return {
            "status": "created",
            "crm_record": record,
            "crm_event": event,
            "write_contract": {
                "owner": "CRMWriter",
                "source": "crm_events",
                "legacy_target_candidates_written": False,
            },
        }

    def create_crm_task(
        self,
        *,
        crm_record_id: str,
        title: str,
        workspace_id: str = "default",
        description: str = "",
        due_at: str = "",
        actor_type: str = "user",
        actor_id: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_record_id = _require_non_empty(crm_record_id, "crm_record_id")
        normalized_title = _require_non_empty(title, "title")
        record = self.store.get_crm_record(normalized_record_id)
        if not record or str(record.get("workspace_id") or "default").strip() != normalized_workspace_id:
            return {"status": "not_found", "reason": "crm_record_not_found", "crm_record_id": normalized_record_id}
        task_seed = str(idempotency_key or "").strip() or (
            f"{normalized_workspace_id}:{normalized_record_id}:{normalized_title}:{str(due_at or '').strip()}"
        )
        task_id = "crmtask_" + sha1(task_seed.encode("utf-8")).hexdigest()[:24]
        effective_idempotency_key = str(idempotency_key or f"crm:task:{task_id}").strip()
        event = self.store.append_crm_event(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "engagement_id": str(record.get("current_engagement_id") or ""),
                "person_identity_key": str(record.get("person_identity_key") or ""),
                "event_type": "crm_task_created",
                "actor_type": str(actor_type or "user").strip() or "user",
                "actor_id": str(actor_id or "").strip(),
                "idempotency_key": effective_idempotency_key,
                "payload": {
                    "writer_id": self.writer_id,
                    "task_id": task_id,
                    "title": normalized_title,
                    "description": str(description or "").strip(),
                    "due_at": str(due_at or "").strip(),
                    "task_status": "open",
                },
                "metadata": {"writer_id": self.writer_id},
            }
        )
        task = self.store.upsert_crm_task(
            {
                "task_id": task_id,
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "engagement_id": str(record.get("current_engagement_id") or ""),
                "person_identity_key": str(record.get("person_identity_key") or ""),
                "title": normalized_title,
                "description": str(description or "").strip(),
                "status": "open",
                "priority": "normal",
                "due_at": str(due_at or "").strip(),
                "created_by_actor": str(actor_type or "user").strip() or "user",
                "created_by_actor_id": str(actor_id or "").strip(),
                "source_event_id": str(event.get("event_id") or ""),
                "idempotency_key": effective_idempotency_key,
                "metadata": {"writer_id": self.writer_id},
            }
        )
        return {
            "status": "created",
            "crm_record": record,
            "crm_task": task,
            "crm_event": event,
            "write_contract": {
                "owner": "CRMWriter",
                "source": "crm_tasks+crm_events",
                "legacy_target_candidates_written": False,
            },
        }


def _require_non_empty(value: Any, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _crm_stage_from_target_follow_up(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return _TARGET_FOLLOW_UP_TO_CRM_STAGE.get(normalized, "")


def _normalize_crm_stage(value: Any) -> str:
    normalized = str(value or "new").strip().lower() or "new"
    if normalized in _TARGET_FOLLOW_UP_TO_CRM_STAGE:
        return _TARGET_FOLLOW_UP_TO_CRM_STAGE[normalized]
    if normalized in _CRM_STAGE_CATEGORIES:
        return normalized
    return "new"


def _crm_stage_category(value: Any) -> str:
    return _CRM_STAGE_CATEGORIES.get(_normalize_crm_stage(value), "open")


def _coerce_quality_score(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed < 0 or parsed > 100:
        return None
    return parsed


def _patch_non_empty(*values: Any) -> str:
    for value in values:
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    return ""
