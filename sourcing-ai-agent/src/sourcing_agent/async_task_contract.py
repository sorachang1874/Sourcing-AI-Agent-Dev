"""Unified async-task contract shape for heavy serving operations.

Single transport contract for every heavy op (plan compile, retrieval, export,
refine-compile, future agent-turn): ``submit -> 202 + handle -> poll/stream ->
artifact``. Ratified 2026-06-15 (Track C, decision c). See
``docs/SERVING_EXECUTION_NORTH_STAR.md`` and ``docs/TRACK_C_C1_HEAVY_OPS_DESIGN.md``
section 7.

This module is the *shape*, not new storage. Heavy ops ride the existing
command/event/outbox substrate (``durable_runtime``); these builders project a
heterogeneous domain record (a workflow job, a typed command) into one
consistent outer envelope so the frontend uses a single poll/download path and
SSE (C4) can tail the same spine uniformly.

The outer lifecycle is normalized to ``queued -> running -> {succeeded, failed,
cancelled}`` (+ ``expired``). The richer domain status (e.g. ``joined_existing_job``,
``failed_terminal``, ``superseded``) is never dropped: it is preserved verbatim
under ``domain_status`` so nothing the existing API exposed is hidden.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------- lifecycle
TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCEEDED = "succeeded"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"
TASK_STATUS_EXPIRED = "expired"

# Ordered, the canonical normalized vocabulary the contract promises.
TASK_STATUSES: tuple[str, ...] = (
    TASK_STATUS_QUEUED,
    TASK_STATUS_RUNNING,
    TASK_STATUS_SUCCEEDED,
    TASK_STATUS_FAILED,
    TASK_STATUS_CANCELLED,
    TASK_STATUS_EXPIRED,
)
TERMINAL_TASK_STATUSES: frozenset[str] = frozenset(
    {TASK_STATUS_SUCCEEDED, TASK_STATUS_FAILED, TASK_STATUS_CANCELLED, TASK_STATUS_EXPIRED}
)

# Explicit, total mapping from the existing domain vocabularies (durable_runtime
# workflow status + command status + the workflow-submission outcomes) onto the
# normalized lifecycle. Keep this exhaustive: tests assert it covers
# TERMINAL_WORKFLOW_STATUSES and TERMINAL_COMMAND_STATUSES so drift is caught.
_DOMAIN_STATUS_MAP: dict[str, str] = {
    # not-yet-running
    "pending": TASK_STATUS_QUEUED,
    "queued": TASK_STATUS_QUEUED,
    "not_before": TASK_STATUS_QUEUED,
    # in-flight
    "running": TASK_STATUS_RUNNING,
    "in_progress": TASK_STATUS_RUNNING,
    "claimed": TASK_STATUS_RUNNING,
    "joined_existing_job": TASK_STATUS_RUNNING,
    "active": TASK_STATUS_RUNNING,
    # succeeded
    "succeeded": TASK_STATUS_SUCCEEDED,
    "completed": TASK_STATUS_SUCCEEDED,
    "reused_completed_job": TASK_STATUS_SUCCEEDED,
    "ok": TASK_STATUS_SUCCEEDED,
    # failed
    "failed": TASK_STATUS_FAILED,
    "failed_terminal": TASK_STATUS_FAILED,
    "error": TASK_STATUS_FAILED,
    "invalid": TASK_STATUS_FAILED,
    # cancelled (superseded = replaced by a newer task; terminal-not-success to the client)
    "cancelled": TASK_STATUS_CANCELLED,
    "canceled": TASK_STATUS_CANCELLED,
    "superseded": TASK_STATUS_CANCELLED,
    "expired": TASK_STATUS_EXPIRED,
}


def normalize_task_status(domain_status: Any) -> str:
    """Map a domain status onto the normalized task lifecycle.

    Unknown statuses default to ``running`` (non-terminal) so a client keeps
    polling rather than prematurely declaring a task done; the raw value is
    always preserved in the envelope under ``domain_status``.
    """

    normalized = str(domain_status or "").strip().lower()
    if not normalized:
        return TASK_STATUS_RUNNING
    return _DOMAIN_STATUS_MAP.get(normalized, TASK_STATUS_RUNNING)


def is_terminal_task_status(status: Any) -> bool:
    return normalize_task_status(status) in TERMINAL_TASK_STATUSES


# ----------------------------------------------------------------- builders
def async_task_artifact(
    *,
    handle: str,
    content_type: str,
    filename: str,
    byte_size: int = 0,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """An artifact reference returned by a completed task's poll body.

    ``handle`` is a relative download URL (e.g. ``/api/exports/{command_id}/artifact``);
    the download endpoint streams the bytes and replicates ``headers`` (notably the
    ``X-Sourcing-*`` export headers). Results are referenced, never inlined into the
    poll body, so polls stay cheap.
    """

    return {
        "handle": str(handle or "").strip(),
        "content_type": str(content_type or "application/octet-stream").strip()
        or "application/octet-stream",
        "filename": str(filename or "download.bin").strip() or "download.bin",
        "byte_size": max(0, int(byte_size or 0)),
        "headers": {str(k): str(v) for k, v in dict(headers or {}).items()},
    }


def async_task_accepted(
    *,
    task_id: str,
    task_type: str,
    idempotency_key: str = "",
    domain_status: str = TASK_STATUS_QUEUED,
    **domain: Any,
) -> dict[str, Any]:
    """The submit acknowledgement body (HTTP 202).

    ``task_id`` is the durable handle (a command_id or job_id on the existing
    substrate). ``idempotency_key`` is both the in-flight lock and the result
    lookup key — a duplicate in-flight submission returns the same handle, an
    already-succeeded one replays its artifact. Extra ``domain`` keys (e.g.
    ``dispatch``, ``history_id``) are preserved alongside the envelope.
    """

    envelope: dict[str, Any] = {
        "task_id": str(task_id or "").strip(),
        "task_type": str(task_type or "").strip(),
        "status": normalize_task_status(domain_status),
        "domain_status": str(domain_status or "").strip(),
        "idempotency_key": str(idempotency_key or "").strip(),
    }
    for key, value in domain.items():
        envelope.setdefault(key, value)
    return envelope


def async_task_status(
    *,
    task_id: str,
    task_type: str,
    domain_status: str,
    error: dict[str, Any] | None = None,
    artifact: dict[str, Any] | None = None,
    **domain: Any,
) -> dict[str, Any]:
    """The poll body for an in-flight or completed task.

    ``status`` is the normalized lifecycle value; ``domain_status`` preserves the
    raw substrate status. ``error`` is populated only for ``failed``; ``artifact``
    only for a ``succeeded`` task that produced a downloadable result.
    """

    envelope: dict[str, Any] = {
        "task_id": str(task_id or "").strip(),
        "task_type": str(task_type or "").strip(),
        "status": normalize_task_status(domain_status),
        "domain_status": str(domain_status or "").strip(),
        "error": dict(error) if error else None,
        "artifact": dict(artifact) if artifact else None,
    }
    for key, value in domain.items():
        envelope.setdefault(key, value)
    return envelope
