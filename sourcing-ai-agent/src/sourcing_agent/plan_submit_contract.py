"""Current Plan-submit compatibility contract for Track C C1b.

This module deliberately describes the *legacy bridge* that remains in place
until the owner-gated C1e cutover: ``POST /api/plan/submit`` returns HTTP 200
with top-level ``status=pending`` while one process-local hydration owner moves
``metadata.plan_generation`` through queued/running/terminal states.

It is not a durable task implementation.  Keeping the bridge constants,
generation record builder, compiler version, and request fingerprint in one
module makes the later C1e deletion mechanical and prevents callers from
inventing parallel lifecycle semantics in the meantime.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

LEGACY_PLAN_SUBMIT_ROUTE = "/api/plan/submit"
LEGACY_PLAN_SUBMIT_HTTP_STATUS = 200
LEGACY_PLAN_SUBMIT_RESPONSE_STATUS = "pending"

LEGACY_PLAN_HYDRATION_OWNER_METHOD = "submit_plan_workflow"
LEGACY_PLAN_HYDRATION_QUEUE_METHOD = "_queue_plan_hydration"
LEGACY_PLAN_HYDRATION_RUN_METHOD = "_run_plan_hydration"

PLAN_COMPILER_CONTRACT_VERSION = "legacy_plan_compile_v1"

PLAN_GENERATION_QUEUED = "queued"
PLAN_GENERATION_RUNNING = "running"
PLAN_GENERATION_COMPLETED = "completed"
PLAN_GENERATION_FAILED = "failed"
PLAN_GENERATION_STATUSES = frozenset(
    {
        PLAN_GENERATION_QUEUED,
        PLAN_GENERATION_RUNNING,
        PLAN_GENERATION_COMPLETED,
        PLAN_GENERATION_FAILED,
    }
)
PLAN_GENERATION_TERMINAL_STATUSES = frozenset({PLAN_GENERATION_COMPLETED, PLAN_GENERATION_FAILED})

PLAN_SUBMIT_OWNER_UNAVAILABLE_STATUS = "failed"
PLAN_SUBMIT_OWNER_UNAVAILABLE_REASON = "plan_submit_owner_unavailable"
PLAN_SUBMIT_OWNER_UNAVAILABLE_HTTP_STATUS = 503

_PLAN_HYDRATION_SIGNATURE_IGNORED_KEYS = frozenset(
    {
        "history_id",
        "frontend_history_id",
        "plan_request_id",
        "request_id",
        "idempotency_key",
        "queued_at",
        "submitted_at",
        "started_at",
        "completed_at",
    }
)


def build_plan_generation(
    *,
    status: str,
    request_id: str,
    queued_at: str,
    started_at: str | None = None,
    completed_at: str | None = None,
    **details: Any,
) -> dict[str, Any]:
    """Build the one legacy history-projection lifecycle record.

    ``started_at``/``completed_at`` are included when explicitly supplied,
    including the legacy empty-string value used when hydration fails before
    acquiring its execution slot.  This preserves today's bridge shape while
    centralizing the allowed state vocabulary.
    """

    normalized_status = str(status or "").strip()
    if normalized_status not in PLAN_GENERATION_STATUSES:
        raise ValueError(f"unsupported plan generation status: {normalized_status or '<missing>'}")
    normalized_request_id = str(request_id or "").strip()
    normalized_queued_at = str(queued_at or "").strip()
    if not normalized_request_id:
        raise ValueError("plan generation request_id is required")
    if not normalized_queued_at:
        raise ValueError("plan generation queued_at is required")

    generation: dict[str, Any] = {
        "status": normalized_status,
        "request_id": normalized_request_id,
        "queued_at": normalized_queued_at,
        "submitted_at": normalized_queued_at,
        "compiler_contract_version": PLAN_COMPILER_CONTRACT_VERSION,
    }
    if started_at is not None:
        generation["started_at"] = str(started_at or "").strip()
    if completed_at is not None:
        generation["completed_at"] = str(completed_at or "").strip()
    generation.update(details)
    return generation


def _normalize_plan_hydration_signature_value(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: dict[str, Any] = {}
        for raw_key, raw_value in sorted(value.items(), key=lambda item: str(item[0])):
            key = str(raw_key or "").strip()
            if not key or key in _PLAN_HYDRATION_SIGNATURE_IGNORED_KEYS:
                continue
            normalized[key] = _normalize_plan_hydration_signature_value(raw_value)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_plan_hydration_signature_value(item) for item in value]
    if isinstance(value, set):
        return sorted(_normalize_plan_hydration_signature_value(item) for item in value)
    if isinstance(value, Path):
        return str(value)
    return value


def canonical_plan_compile_request(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Return the current process-local compile/coalescing request projection.

    Consumer/history identity and transport timestamps are intentionally
    excluded. Trusted requester/tenant fields and every semantic request field
    remain included.  List order is preserved because C1b characterizes the
    existing compiler rather than changing request semantics ahead of owner
    decisions.
    """

    normalized = _normalize_plan_hydration_signature_value(dict(payload or {}))
    return dict(normalized or {})


def plan_hydration_request_signature(payload: dict[str, Any] | None) -> str:
    """Hash the canonical request together with its explicit compiler version."""

    encoded = json.dumps(
        {
            "compiler_contract_version": PLAN_COMPILER_CONTRACT_VERSION,
            "request": canonical_plan_compile_request(payload),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
