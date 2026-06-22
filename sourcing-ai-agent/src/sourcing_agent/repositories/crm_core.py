"""Track B B4.2 — CRM record / event / task control-plane tables.

Declarative read-path TableDescriptors replacing the hand-written `_*_from_row` mappers in
ControlPlaneStore (byte-equivalence verified by scripts/_descriptor_equiv_harness.py). The `*_json`
TEXT columns map to suffix-stripped public names via `field=`; the B4.2 schema migration flips them
to jsonb by changing only the column Kind here.
"""

from __future__ import annotations

from ..control_plane_repository import Column, Kind, TableDescriptor

CRM_RECORDS = TableDescriptor(
    table="crm_records",
    pk=("crm_record_id",),
    columns=(
        Column("crm_record_id"),
        Column("workspace_id", read_default="default"),
        Column("person_identity_key"),
        Column("candidate_identity_key"),
        Column("collection_id"),
        Column("display_name_cache"),
        Column("headline_cache"),
        Column("primary_company_cache"),
        Column("avatar_asset_id"),
        Column("lifecycle_status", read_default="active"),
        Column("visibility_status", read_default="normal"),
        Column("owner_user_id"),
        Column("source_projection_id"),
        Column("source_run_id"),
        Column("source_collection_id"),
        Column("source_reason"),
        Column("current_engagement_id"),
        Column("crm_version", Kind.INT, read_default="1"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


CRM_EVENTS = TableDescriptor(
    table="crm_events",
    pk=("event_id",),
    columns=(
        Column("event_id"),
        Column("workspace_id", read_default="default"),
        Column("crm_record_id"),
        Column("engagement_id"),
        Column("person_identity_key"),
        Column("event_type"),
        Column("actor_type"),
        Column("actor_id"),
        Column("idempotency_key"),
        Column("payload_json", Kind.JSON, field="payload"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("occurred_at"),
        Column("created_at"),
    ),
)


CRM_TASKS = TableDescriptor(
    table="crm_tasks",
    pk=("task_id",),
    columns=(
        Column("task_id"),
        Column("workspace_id", read_default="default"),
        Column("crm_record_id"),
        Column("engagement_id"),
        Column("person_identity_key"),
        Column("title"),
        Column("description"),
        Column("status", read_default="open"),
        Column("priority", read_default="normal"),
        Column("due_at"),
        Column("completed_at"),
        Column("created_by_actor"),
        Column("created_by_actor_id"),
        Column("source_event_id"),
        Column("idempotency_key"),
        Column("metadata_json", Kind.JSON, field="metadata"),
        Column("created_at"),
        Column("updated_at"),
    ),
)


# Read-path descriptors keyed by the ControlPlaneStore mapper method they replace.
FROM_ROW_DESCRIPTORS = {
    "_crm_record_from_row": CRM_RECORDS,
    "_crm_event_from_row": CRM_EVENTS,
    "_crm_task_from_row": CRM_TASKS,
}
