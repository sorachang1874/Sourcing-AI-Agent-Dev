"""Backfill durable full-snapshot materialization items for legacy jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from .orchestrator import SourcingOrchestrator


@dataclass
class SnapshotMaterializationBackfillStats:
    total_jobs: int = 0
    eligible_jobs: int = 0
    existing_items: int = 0
    items_enqueued: int = 0
    jobs_skipped: int = 0
    errors: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_jobs": self.total_jobs,
            "eligible_jobs": self.eligible_jobs,
            "existing_items": self.existing_items,
            "items_enqueued": self.items_enqueued,
            "jobs_skipped": self.jobs_skipped,
            "errors": self.errors,
        }


SnapshotMaterializationBackfillProgress = Callable[[str, SnapshotMaterializationBackfillStats], None]


def backfill_snapshot_full_materialization_items(
    *,
    orchestrator: SourcingOrchestrator,
    dry_run: bool = True,
    limit: int = 100000,
    progress_callback: SnapshotMaterializationBackfillProgress | None = None,
    batch_size: int = 100,
) -> dict[str, Any]:
    """Create durable queue items for legacy scheduled full-materialization jobs.

    The migration is intentionally dry-run by default. It scans completed workflow
    jobs whose summaries still require `background_snapshot_materialization` and
    enqueues the same deterministic `snapshot_full_materialization` item used by
    new workflow-completion scheduling.
    """

    stats = SnapshotMaterializationBackfillStats()
    jobs = orchestrator.store.list_jobs(
        job_type="workflow",
        statuses=["completed"],
        limit=max(1, int(limit or 100000)),
    )
    stats.total_jobs = len(jobs)

    def _emit_progress(job_id: str) -> None:
        if progress_callback:
            progress_callback(job_id, stats)

    for index, job in enumerate(jobs, start=1):
        job_id = str(dict(job).get("job_id") or "").strip()
        try:
            summary = dict(dict(job).get("summary") or {})
            if not job_id or not orchestrator._snapshot_materialization_requires_background_reconcile(
                summary_payload=summary
            ):
                stats.jobs_skipped += 1
                continue
            stats.eligible_jobs += 1
            snapshot_id = str(dict(summary.get("background_snapshot_materialization") or {}).get("snapshot_id") or "")
            item_id = orchestrator._snapshot_full_materialization_item_id(job_id=job_id, snapshot_id=snapshot_id)
            existing = orchestrator.store.get_job_materialization_item(item_id)
            if existing:
                stats.existing_items += 1
                continue
            if not dry_run:
                item = orchestrator._enqueue_snapshot_full_materialization_item(
                    job_id=job_id,
                    source="snapshot_full_materialization_backfill",
                    summary_payload=summary,
                    metadata={"enqueue_event": "snapshot_full_materialization_backfill"},
                )
                if item and str(item.get("item_kind") or "") == "snapshot_full_materialization":
                    stats.items_enqueued += 1
                else:
                    stats.errors += 1
            else:
                stats.items_enqueued += 1
        except Exception:
            stats.errors += 1
        finally:
            if index % max(1, int(batch_size or 100)) == 0:
                _emit_progress(job_id)

    _emit_progress("snapshot_full_materialization_backfill_complete")
    return {
        **stats.to_dict(),
        "dry_run": bool(dry_run),
        "limit": max(1, int(limit or 100000)),
    }
