from __future__ import annotations

from collections.abc import Iterable
from typing import Any

COMPLETION_POLICY_VERSION = "linkedin_completion_policy_v1"

TERMINAL_MATERIALIZATION_ITEM_STATUSES = frozenset(
    {
        "completed",
        "failed",
        "skipped",
        "cancelled",
        "canceled",
        "superseded",
        "exhausted",
    }
)

LINKEDIN_COMPLETION_POLICY_KEYS = (
    "stage1_candidate_set_terminal",
    "stage1_preview_allowed",
    "profile_fetch_terminal",
    "local_apply_terminal",
    "board_visible_terminal",
    "serving_finalized",
    "post_result_layering_ready",
    "collection_merge_terminal",
)


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _policy(status: str, reason: str = "", **evidence: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": str(status or "").strip() or "unknown",
    }
    normalized_reason = str(reason or "").strip()
    if normalized_reason:
        payload["reason"] = normalized_reason
    for key, value in evidence.items():
        if value not in (None, "", [], {}):
            payload[key] = value
    return payload


def _active_materialization_items(
    materialization_items: Iterable[dict[str, Any]],
    *,
    blocking_item_kinds: Iterable[str],
    terminal_item_statuses: Iterable[str] = TERMINAL_MATERIALIZATION_ITEM_STATUSES,
) -> list[dict[str, Any]]:
    blocking_kinds = {str(kind or "").strip() for kind in blocking_item_kinds if str(kind or "").strip()}
    terminal_statuses = {str(status or "").strip().lower() for status in terminal_item_statuses}
    return [
        dict(item)
        for item in list(materialization_items or [])
        if str(dict(item).get("status") or "").strip().lower() not in terminal_statuses
        and str(dict(item).get("item_kind") or "").strip() in blocking_kinds
    ]


def _materialization_blocked_policy_key(active_items: list[dict[str, Any]]) -> str:
    kinds = {str(item.get("item_kind") or "").strip() for item in active_items}
    if "local_apply_closure" in kinds:
        return "local_apply_terminal"
    if "board_visible_delta_apply" in kinds:
        return "board_visible_terminal"
    if "search_seed_discovery_query" in kinds:
        return "stage1_candidate_set_terminal"
    return "local_apply_terminal"


def _serving_finalized_policy(workflow_current_state: dict[str, Any]) -> dict[str, Any]:
    completion_proofs = dict(dict(workflow_current_state or {}).get("completion_proofs") or {})
    serving_finalized = dict(completion_proofs.get("serving_finalized") or {})
    if str(serving_finalized.get("status") or "").strip() == "proved":
        return _policy(
            "proved",
            "serving_finalized_proof_recorded",
            sequence_number=_coerce_int(serving_finalized.get("sequence_number"), 0),
            event_id=str(serving_finalized.get("event_id") or ""),
        )
    return _policy("pending", "serving_finalized_proof_missing")


def evaluate_linkedin_completion_policies(
    *,
    job_id: str,
    workflow_owner_blocker: dict[str, Any] | None = None,
    active_worker_count: int = 0,
    blocking_worker_count: int = 0,
    blocked_task: str = "",
    lifecycle: dict[str, Any] | None = None,
    patch_projection: dict[str, Any] | None = None,
    materialization_items: Iterable[dict[str, Any]] = (),
    blocking_materialization_item_kinds: Iterable[str] = (),
    linkedin_stage_1_progress: dict[str, Any] | None = None,
    workflow_current_state: dict[str, Any] | None = None,
    workflow_run_absent: bool = False,
) -> dict[str, Any]:
    """Evaluate explicit LinkedIn workflow completion policies.

    This module is intentionally side-effect free. Callers must collect durable
    observations and then pass them in; policy evaluation must not read DBs,
    repair state, or publish projections.
    """

    normalized_job_id = str(job_id or "").strip()
    policies = {
        key: _policy("unknown", "not_evaluated")
        for key in LINKEDIN_COMPLETION_POLICY_KEYS
    }
    first_blocker: dict[str, Any] = {}

    def _block(policy_key: str, blocker: dict[str, Any]) -> None:
        nonlocal first_blocker
        blocker_payload = dict(blocker or {})
        reason = str(blocker_payload.get("reason") or "").strip()
        policies[policy_key] = _policy(
            "blocked",
            reason,
            **{key: value for key, value in blocker_payload.items() if key != "reason"},
        )
        if not first_blocker:
            first_blocker = blocker_payload

    if not normalized_job_id:
        _block("stage1_candidate_set_terminal", {"reason": "job_id_missing"})
    elif workflow_owner_blocker:
        _block("serving_finalized", dict(workflow_owner_blocker))

    if not first_blocker and (_coerce_int(active_worker_count, 0) > 0 or _coerce_int(blocking_worker_count, 0) > 0):
        _block(
            "profile_fetch_terminal",
            {
                "reason": "active_workers_present",
                "active_worker_count": _coerce_int(active_worker_count, 0),
                "blocking_worker_count": _coerce_int(blocking_worker_count, 0),
                "blocked_task": str(blocked_task or "").strip(),
            },
        )

    lifecycle_payload = dict(lifecycle or {})
    patch_payload = dict(patch_projection or {})
    delta_required_count = _coerce_int(lifecycle_payload.get("delta_profile_required_count"), 0)
    delta_fetched_count = _coerce_int(lifecycle_payload.get("delta_profile_fetched_count"), 0)
    delta_board_visible_count = _coerce_int(lifecycle_payload.get("delta_profile_board_visible_count"), 0)
    patch_board_visible_count = _coerce_int(patch_payload.get("delta_profile_board_visible_count"), 0)
    if patch_board_visible_count > 0:
        if delta_required_count > 0:
            patch_board_visible_count = min(patch_board_visible_count, delta_required_count)
        delta_board_visible_count = max(delta_board_visible_count, patch_board_visible_count)
        delta_fetched_count = max(delta_fetched_count, delta_board_visible_count)

    if not first_blocker and lifecycle_payload:
        if delta_required_count > 0 and delta_fetched_count < delta_required_count:
            _block(
                "profile_fetch_terminal",
                {
                    "reason": "delta_profile_fetch_incomplete",
                    "delta_profile_required_count": delta_required_count,
                    "delta_profile_fetched_count": delta_fetched_count,
                },
            )
        elif delta_required_count > 0 and delta_board_visible_count < delta_required_count:
            _block(
                "board_visible_terminal",
                {
                    "reason": "delta_profile_board_visible_incomplete",
                    "delta_profile_required_count": delta_required_count,
                    "delta_profile_board_visible_count": delta_board_visible_count,
                },
            )

    active_items = _active_materialization_items(
        materialization_items,
        blocking_item_kinds=blocking_materialization_item_kinds,
    )
    if not first_blocker and active_items:
        _block(
            _materialization_blocked_policy_key(active_items),
            {
                "reason": "active_materialization_items_present",
                "active_materialization_item_count": len(active_items),
                "active_materialization_item_ids": [
                    str(item.get("item_id") or "")
                    for item in active_items[:10]
                    if str(item.get("item_id") or "").strip()
                ],
            },
        )

    linkedin_progress = dict(linkedin_stage_1_progress or {})
    outstanding_profile_tail_count = sum(
        max(0, _coerce_int(linkedin_progress.get(key), 0))
        for key in (
            "profile_pending_count",
            "profile_queued_count",
            "profile_retryable_count",
        )
    )
    if not first_blocker and outstanding_profile_tail_count > 0:
        _block(
            "profile_fetch_terminal",
            {
                "reason": "outstanding_profile_tail_present",
                "outstanding_profile_tail_count": outstanding_profile_tail_count,
                "linkedin_stage_1_progress": {
                    key: _coerce_int(linkedin_progress.get(key), 0)
                    for key in (
                        "profile_required_count",
                        "profile_fetched_count",
                        "profile_materialized_count",
                        "profile_pending_count",
                        "profile_queued_count",
                        "profile_retryable_count",
                        "profile_unrecoverable_count",
                    )
                },
            },
        )

    if policies["profile_fetch_terminal"]["status"] == "unknown":
        if delta_required_count > 0:
            policies["profile_fetch_terminal"] = _policy(
                "proved",
                "delta_profile_fetch_terminal",
                delta_profile_required_count=delta_required_count,
                delta_profile_fetched_count=delta_fetched_count,
            )
        elif outstanding_profile_tail_count <= 0:
            policies["profile_fetch_terminal"] = _policy("not_applicable", "no_required_delta_profiles")

    if policies["board_visible_terminal"]["status"] == "unknown":
        if delta_required_count > 0:
            policies["board_visible_terminal"] = _policy(
                "proved",
                "delta_profile_board_visible_terminal",
                delta_profile_required_count=delta_required_count,
                delta_profile_board_visible_count=delta_board_visible_count,
            )
        else:
            policies["board_visible_terminal"] = _policy("not_applicable", "no_required_delta_profiles")

    if policies["local_apply_terminal"]["status"] == "unknown":
        policies["local_apply_terminal"] = _policy("proved", "no_active_local_apply_materialization_items")

    if policies["stage1_candidate_set_terminal"]["status"] == "unknown":
        if lifecycle_payload:
            policies["stage1_candidate_set_terminal"] = _policy(
                "proved",
                "lifecycle_candidate_set_evidence_present",
                stage1_deduped_candidate_count=_coerce_int(
                    lifecycle_payload.get("stage1_deduped_candidate_count"),
                    0,
                ),
            )
        else:
            policies["stage1_candidate_set_terminal"] = _policy("unknown", "lifecycle_missing")

    if policies["stage1_preview_allowed"]["status"] == "unknown":
        policies["stage1_preview_allowed"] = _policy("not_applicable", "not_checked_by_completion_gate")

    serving_finalized_policy = _serving_finalized_policy(dict(workflow_current_state or {}))
    if policies["serving_finalized"]["status"] == "unknown":
        if workflow_run_absent and serving_finalized_policy["status"] != "proved":
            # Owner-approved exemption (2026-06-12): legacy/recovered jobs that
            # have no durable workflow run at all can never produce a
            # serving_finalized proof; requiring one would block them forever.
            # This applies ONLY to confirmed row absence — lookup failures must
            # stay fail-closed (callers signal those separately and must not
            # set workflow_run_absent on exceptions).
            policies["serving_finalized"] = _policy(
                "not_applicable",
                "legacy_job_without_durable_workflow_run",
            )
        elif not first_blocker and serving_finalized_policy["status"] != "proved":
            _block(
                "serving_finalized",
                {
                    "reason": str(serving_finalized_policy.get("reason") or "serving_finalized_proof_missing"),
                    "policy_status": str(serving_finalized_policy.get("status") or ""),
                },
            )
        else:
            policies["serving_finalized"] = serving_finalized_policy
    policies["post_result_layering_ready"] = _policy("not_applicable", "not_blocking_run_result_serving")
    policies["collection_merge_terminal"] = _policy("not_applicable", "background_collection_merge_not_blocking_run")

    return {
        "status": "blocked" if first_blocker else "proved",
        "job_id": normalized_job_id,
        "policy_version": COMPLETION_POLICY_VERSION,
        "first_blocker": first_blocker,
        "policies": policies,
    }


def completion_policy_first_blocker(evaluation: dict[str, Any] | None) -> dict[str, Any]:
    return dict(dict(evaluation or {}).get("first_blocker") or {})
