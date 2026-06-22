"""Track B B4.2 — linkedin_profile_registry domain repository (pilot).

The ``linkedin_profile_registry`` table descriptor is the single declarative source of truth for the
table's row<->dict mapping: it replaces the inherited hand-written ``_linkedin_profile_registry_from_row``
(50 lines of per-field coercion + JSON parsing) and the parallel ``_linkedin_profile_registry_row_payload``
column builder. The ``source_shards_json`` / ``source_jobs_json`` TEXT columns map to the public
``source_shards`` / ``source_jobs`` string lists; the B4.2 schema migration flips these to ``jsonb`` by
changing only the column ``Kind`` here.

Conflict policy is REPLACE_ALL: the registry computes its merged row in Python
(``_compose_linkedin_profile_registry_effective_payload``) before writing, so a plain
``ON CONFLICT (profile_url_key) DO UPDATE SET <all-non-pk> = excluded`` is the correct upsert.
"""

from __future__ import annotations

from typing import Any

from ..control_plane_repository import Column, Kind, Repository, TableDescriptor

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


class LinkedinProfileRegistryRepository(Repository):
    """Typed repository for the linkedin_profile_registry table (pilot scope: base row read/upsert)."""

    descriptor = LINKEDIN_PROFILE_REGISTRY

    def get_by_key(self, profile_url_key: str) -> dict[str, Any] | None:
        return self.get(where_sql="profile_url_key = %s", params=[profile_url_key])
