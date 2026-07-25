"""
Fail-closed migration/audit entrypoint for ``job_result_lifecycle`` rows.

The lifecycle contract is now event-time writer owned. Public reads and
maintenance commands must not reconstruct lifecycle state from mixed job
summary, result-view, asset-population, Stage 1, registry, or candidate-state
fallbacks.

This module only migrates lifecycle payloads that already exist as serialized
canonical evidence on historical ``job_result_views.metadata`` or
``jobs.summary``. Jobs without that evidence are reported as repair-required;
they are not synthesized into validated rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .orchestrator import SourcingOrchestrator

_TEXT_FIELDS = {
    "view_id",
    "company_key",
    "target_company",
    "workflow_kind",
    "phase",
    "phase_status",
    "state",
    "baseline_snapshot_id",
    "current_snapshot_id",
    "served_snapshot_id",
    "served_generation_key",
    "serving_projection_id",
    "serving_projection_phase",
    "delta_profile_progress_reason",
    "background_snapshot_materialization_status",
    "outreach_layering_status",
    "source_validation_status",
    "projection_source_snapshot_id",
}

_INT_FIELDS = {
    "baseline_candidate_count",
    "expected_candidate_count",
    "served_candidate_count",
    "delta_profile_required_count",
    "delta_profile_fetched_count",
    "delta_profile_applied_count",
    "delta_profile_materialized_count",
    "delta_profile_board_visible_count",
    "stage1_current_search_returned_count",
    "stage1_former_search_returned_count",
    "stage1_all_search_returned_count",
    "stage1_deduped_candidate_count",
    "stage1_deduped_profile_url_count",
    "stage1_profile_fetch_required_count",
    "stage1_profile_fetched_count",
    "last_event_id",
}


@dataclass
class BackfillStatistics:
    """Statistics for the lifecycle migration/audit operation."""

    total_jobs: int = 0
    jobs_with_lifecycle: int = 0
    jobs_backfilled: int = 0
    jobs_skipped: int = 0
    jobs_repair_required: int = 0
    errors: int = 0
    repair_required_samples: list[dict[str, str]] = field(default_factory=list)
    migrated_sources: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_jobs": self.total_jobs,
            "jobs_with_lifecycle": self.jobs_with_lifecycle,
            "jobs_backfilled": self.jobs_backfilled,
            "jobs_skipped": self.jobs_skipped,
            "jobs_repair_required": self.jobs_repair_required,
            "jobs_legacy_projection_retired": self.jobs_repair_required,
            "errors": self.errors,
            "repair_required_samples": list(self.repair_required_samples),
            "migrated_sources": dict(self.migrated_sources),
        }


ProgressCallback = Callable[[str, BackfillStatistics], None]


def _ensure_job_result_lifecycle_schema(orchestrator: SourcingOrchestrator) -> dict[str, Any]:
    """Ensure the lifecycle table exists before the migration's first read."""

    store = orchestrator.store
    mode = str(getattr(store, "control_plane_postgres_live_mode", lambda: "disabled")() or "disabled")
    if mode == "disabled":
        return {"status": "skipped", "reason": "postgres_disabled"}
    # B4.3f: the SQLite-shadow-sourced sync is retired. job_result_lifecycle is part of the
    # versioned-migration baseline, so the adapter's bootstrap is the schema guarantee.
    try:
        adapter = getattr(store, "_control_plane_postgres", None)
        ensure_bootstrapped = getattr(adapter, "ensure_bootstrapped", None)
        if callable(ensure_bootstrapped):
            ensure_bootstrapped()
    except Exception as exc:
        return {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    return {"status": "completed", "mechanism": "migration_runner_bootstrap"}


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _serialized_lifecycle_payload(job: dict[str, Any], result_view: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    view_metadata = dict(dict(result_view or {}).get("metadata") or {})
    view_lifecycle = dict(view_metadata.get("result_view_lifecycle") or {})
    if view_lifecycle:
        return "job_result_view.metadata.result_view_lifecycle", view_lifecycle
    job_summary = dict(dict(job or {}).get("summary") or {})
    summary_lifecycle = dict(job_summary.get("result_view_lifecycle") or {})
    if summary_lifecycle:
        return "jobs.summary.result_view_lifecycle", summary_lifecycle
    return "", {}


def _canonical_fields_from_serialized_payload(
    *,
    job: dict[str, Any],
    result_view: dict[str, Any],
    source: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    lifecycle = dict(payload or {})
    state = str(lifecycle.get("state") or lifecycle.get("phase") or "").strip()
    served_snapshot_id = str(lifecycle.get("served_snapshot_id") or "").strip()
    current_snapshot_id = str(lifecycle.get("current_snapshot_id") or "").strip()
    baseline_snapshot_id = str(lifecycle.get("baseline_snapshot_id") or "").strip()
    if not (state or served_snapshot_id or current_snapshot_id or baseline_snapshot_id):
        return {}

    fields: dict[str, Any] = {}
    for field_name in _TEXT_FIELDS:
        if field_name in lifecycle:
            fields[field_name] = str(lifecycle.get(field_name) or "").strip()
    for field_name in _INT_FIELDS:
        if field_name in lifecycle:
            fields[field_name] = _coerce_int(lifecycle.get(field_name))

    fields["source_validation_status"] = "validated"
    fields["state"] = state or str(fields.get("state") or fields.get("phase") or "unavailable")
    fields["phase"] = str(fields.get("phase") or fields["state"])
    fields["view_id"] = str(fields.get("view_id") or dict(result_view or {}).get("view_id") or "")
    fields["target_company"] = str(
        fields.get("target_company") or dict(job or {}).get("target_company") or dict(result_view or {}).get("target_company") or ""
    )
    fields["company_key"] = str(fields.get("company_key") or dict(job or {}).get("company_key") or "")
    fields["workflow_kind"] = str(fields.get("workflow_kind") or dict(job or {}).get("job_type") or "")
    fields["delta_profile_progress_applicable"] = lifecycle.get("delta_profile_progress_applicable") is not False

    metadata = dict(lifecycle.get("metadata") or {})
    metadata["migration_source"] = source
    metadata["legacy_projection_retired"] = True
    fields["metadata"] = metadata
    return fields


def _record_repair_required(stats: BackfillStatistics, *, job_id: str, reason: str) -> None:
    stats.jobs_skipped += 1
    stats.jobs_repair_required += 1
    if len(stats.repair_required_samples) < 25:
        stats.repair_required_samples.append({"job_id": str(job_id or ""), "reason": reason})


def backfill_job_result_lifecycle(
    *,
    orchestrator: SourcingOrchestrator,
    dry_run: bool = False,
    progress_callback: ProgressCallback | None = None,
    batch_size: int = 100,
) -> dict[str, Any]:
    """
    Migrate existing serialized canonical lifecycle evidence into the lifecycle table.

    Jobs without validated rows and without serialized lifecycle evidence are
    reported as repair-required. The command deliberately does not rebuild
    lifecycle rows from mixed fallback sources.
    """

    stats = BackfillStatistics()
    store = orchestrator.store
    schema_preflight = _ensure_job_result_lifecycle_schema(orchestrator)
    if str(schema_preflight.get("status") or "").strip().lower() == "failed":
        return {**stats.to_dict(), "schema_preflight": schema_preflight}

    def _emit_progress(job_id: str) -> None:
        if progress_callback:
            progress_callback(job_id, stats)

    all_jobs = store.list_jobs(limit=100000)
    stats.total_jobs = len(all_jobs)

    for index, job in enumerate(all_jobs, start=1):
        job_id = str(job.get("job_id") or "").strip()
        if not job_id:
            stats.errors += 1
            if index % batch_size == 0:
                _emit_progress(job_id)
            continue

        existing_row = store.get_job_result_lifecycle(job_id)
        if existing_row and str(existing_row.get("source_validation_status") or "") == "validated":
            stats.jobs_with_lifecycle += 1
            stats.jobs_skipped += 1
            if index % batch_size == 0:
                _emit_progress(job_id)
            continue

        try:
            result_view = store.get_job_result_view(job_id=job_id) or {}
            source, lifecycle_payload = _serialized_lifecycle_payload(job, result_view)
            if not lifecycle_payload:
                _record_repair_required(
                    stats,
                    job_id=job_id,
                    reason="serialized_job_result_lifecycle_missing",
                )
                if index % batch_size == 0:
                    _emit_progress(job_id)
                continue

            fields = _canonical_fields_from_serialized_payload(
                job=job,
                result_view=result_view,
                source=source,
                payload=lifecycle_payload,
            )
            if not fields:
                _record_repair_required(
                    stats,
                    job_id=job_id,
                    reason="serialized_job_result_lifecycle_invalid",
                )
                if index % batch_size == 0:
                    _emit_progress(job_id)
                continue

            if not dry_run:
                store.upsert_job_result_lifecycle(job_id=job_id, fields=fields)
            stats.jobs_backfilled += 1
            stats.migrated_sources[source] = int(stats.migrated_sources.get(source, 0)) + 1
        except Exception as exc:
            stats.errors += 1
            if progress_callback:
                progress_callback(f"{job_id} (error: {exc})", stats)

        if index % batch_size == 0:
            _emit_progress(job_id)

    _emit_progress("backfill_complete")

    return {**stats.to_dict(), "schema_preflight": schema_preflight}
