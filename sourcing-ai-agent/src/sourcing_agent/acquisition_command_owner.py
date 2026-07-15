"""Acquisition command-layer owner extracted from ``SourcingOrchestrator`` (Phase 3).

The typed acquisition command band: plan/execute/run/drain handlers for the
eight acquisition command types (``acquisition.run.create``,
``acquisition.intent.resolve``, ``acquisition.plan.build``,
``acquisition.plan_review.request``, ``acquisition.plan.commit``,
``acquisition.probe.submit``, ``acquisition.probe.collect``,
``acquisition.scale.plan``), their owner-specific running-cancel handlers
(scale-plan-before-discovery, plan-commit-before-probe, plan-review-request),
the acquisition-run lookup used only by those cancels, the plan-review session
helpers, the operation-run sync emitters for plan-ready / plan-review-requested
/ plan-committed / run-phase-advanced, and the decomposition downstream
command-type listing. Bodies are moved verbatim from ``orchestrator.py``; the
only body edit is kernel-wrapper calls ``self._x(...)`` -> ``self._kernel._x(...)``.
``SourcingOrchestrator`` keeps signature-identical delegating wrappers for
every moved method, and injects its shared spine helpers as bound callables
stored under the same attribute names so moved bodies stay verbatim.

Deliberate stay-behinds on the orchestrator (Phase 4 territory): the worker
recovery spine that invokes the drains, the generic
``_cancel_running_orchestration_before_downstream`` cancel (shared with
company-public-web refresh), ``_upsert_acquisition_run_phase`` (also called by
the stage-1 discovery and projection-admission executors and injected into
``ProfileFetchOwner``), the discovery-lane read-model APIs, the stage-1
``linkedin.discovery_query.run`` lifecycle, and the legacy hosted-acquisition
resume/workflow spine.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Mapping
from typing import Any, Callable

from .command_kernel import CommandKernel
from .durable_runtime import (
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    ACQUISITION_INTENT_RESOLVE_OWNER,
    ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
    ACQUISITION_PLAN_BUILD_OWNER,
    ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
    ACQUISITION_PLAN_COMMIT_OWNER,
    ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
    ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
    ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
    ACQUISITION_PROBE_OWNER,
    ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_OWNER,
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_OWNER,
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
    default_stage_id_for_command_type,
)

# NOTE: the helpers below duplicate small module-level helpers in
# ``orchestrator.py`` (which imports this module — importing them back from
# orchestrator would create a cycle).  The bodies are copied verbatim; several
# other ``sourcing_agent`` modules already carry the same local copies.


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(value)


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _plan_review_session_api_summary(session: dict[str, Any] | None) -> dict[str, Any]:
    payload = dict(session or {})
    if not payload:
        return {}
    return {
        "review_id": int(payload.get("review_id") or 0),
        "target_company": str(payload.get("target_company") or ""),
        "status": str(payload.get("status") or ""),
        "risk_level": str(payload.get("risk_level") or ""),
        "required_before_execution": bool(payload.get("required_before_execution")),
        "reviewer": str(payload.get("reviewer") or ""),
        "approved_at": str(payload.get("approved_at") or ""),
        "created_at": str(payload.get("created_at") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
    }


class AcquisitionCommandOwner:
    """Owner of the typed acquisition command band (Phase 3 extraction)."""

    def __init__(
        self,
        *,
        store: Any,
        command_kernel: CommandKernel,
        durable_runtime_writer: Any,
        upsert_acquisition_run_phase: Callable[..., dict[str, Any]],
        sync_operation_run_from_workflow_command_control: Callable[..., dict[str, Any]],
        revalidate_operation_action_target: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> None:
        self.store = store
        self._kernel = command_kernel
        self.durable_runtime_writer = durable_runtime_writer
        # Injected cross-domain/spine callables, stored under the same names the
        # moved bodies already use so the bodies stay verbatim.
        # ``_upsert_acquisition_run_phase`` stays on the orchestrator: the
        # stage-1 discovery and projection-admission executors also call it and
        # ``ProfileFetchOwner`` already receives it as an injected callable.
        self._upsert_acquisition_run_phase = upsert_acquisition_run_phase
        self._sync_operation_run_from_workflow_command_control = sync_operation_run_from_workflow_command_control
        self._revalidate_operation_action_target = revalidate_operation_action_target

    def _preflight_acquisition_root_operation_action_command(
        self,
        command: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Require exact Operation/action authority for every executable root."""

        return self._revalidate_operation_action_target(dict(command))

    def _acquisition_root_preflight_failure(
        self,
        command: Mapping[str, Any],
        *,
        reason: str,
        mark_failed: bool,
    ) -> dict[str, Any]:
        command_record = dict(command)
        command_id = str(command_record.get("command_id") or "").strip()
        failed: dict[str, Any] | None = None
        if mark_failed and command_id:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=reason,
                retryable=False,
            )
        observed = failed or command_record
        return {
            "status": "failed",
            "reason": reason,
            "workflow_command": self._kernel._workflow_command_observation(
                observed,
                migration_phase="W11a_acquisition_run_create_root",
            ),
        }

    def _cancel_acquisition_owner_command_uow(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
        cancel_kind: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        default_reason = {
            "acquisition_plan_commit_before_probe": "cancelled_before_probe_command_planned",
            "acquisition_scale_plan_before_discovery": "cancelled_before_discovery_command_planned",
        }[cancel_kind]
        reason = str(payload.get("reason") or default_reason).strip()
        force = _coerce_bool(payload.get("force"), False)
        result = self.store.repos.workflow_runtime.cancel_acquisition_owner_command(
            str(command_payload.get("command_id") or "").strip(),
            cancel_kind=cancel_kind,
            actor=actor,
            reason=reason,
            force=force,
        )
        outcome = str(result.get("outcome") or "conflict").strip() or "conflict"
        latest = dict(result.get("workflow_command") or command_payload)
        response_base = {
            "workflow_command": self._kernel._workflow_command_api_record(latest),
            **self._kernel._workflow_command_control_response_policy_records(latest),
            "uow_outcome": outcome,
            "module_state_mutated": bool(result.get("module_state_mutated")),
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }
        if outcome == "not_found":
            return {"status": "not_found", "reason": "workflow_command_not_found", **response_base}
        if outcome not in {"applied", "repaired", "already_applied"}:
            invalid = {
                "status": "invalid",
                "reason": str(result.get("reason") or "workflow_command_owner_specific_cancel_not_applied").strip(),
                **response_base,
            }
            if result.get("downstream_command_ids"):
                invalid.update(
                    {
                        "downstream_command_count": len(result["downstream_command_ids"]),
                        "downstream_command_ids": list(result["downstream_command_ids"]),
                    }
                )
            if cancel_kind == "acquisition_scale_plan_before_discovery":
                invalid.update(
                    {
                        "activity_attempt_count": int(result.get("activity_attempt_count") or 0),
                        "entity_delta_count": int(result.get("entity_delta_count") or 0),
                        "lane_downstream_command_ids": list(result.get("lane_downstream_command_ids") or []),
                    }
                )
            return invalid
        operation_sync = {}
        if outcome != "already_applied":
            operation_sync = self._sync_operation_run_from_workflow_command_control(
                latest,
                control_action="cancel",
                actor=actor,
                source="api.workflow_command_owner_specific_cancel",
            )
        response = {
            "status": "cancelled",
            "operation_sync": operation_sync,
            "acquisition_run": dict(result.get("acquisition_run") or {}),
            **response_base,
        }
        if cancel_kind == "acquisition_scale_plan_before_discovery":
            response.update(
                {
                    "workflow_activity_runs": list(result.get("workflow_activity_runs") or []),
                    "acquisition_discovery_lanes": list(result.get("acquisition_discovery_lanes") or []),
                }
            )
        return response

    def _cancel_running_acquisition_scale_plan_before_discovery(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._cancel_acquisition_owner_command_uow(
            command,
            payload=payload,
            cancel_kind="acquisition_scale_plan_before_discovery",
        )

    def _cancel_running_acquisition_plan_commit_before_probe(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self._cancel_acquisition_owner_command_uow(
            command,
            payload=payload,
            cancel_kind="acquisition_plan_commit_before_probe",
        )

    def _cancel_running_acquisition_plan_review_request_command(
        self,
        command: dict[str, Any],
        *,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        body = dict(command_payload.get("payload") or {})
        acquisition_plan = dict(body.get("acquisition_plan") or {})
        plan_id = str(acquisition_plan.get("plan_id") or body.get("plan_id") or "").strip()
        target_company = str(acquisition_plan.get("target_company") or body.get("target_company") or "").strip()
        actor = str(payload.get("actor") or payload.get("operator") or "api").strip() or "api"
        reason = str(payload.get("reason") or "cancelled_by_owner_specific_command_control").strip()
        review_session = self._find_existing_acquisition_plan_review_session(
            target_company=target_company,
            plan_id=plan_id,
        )
        if str((review_session or {}).get("status") or "").strip() == "approved":
            return {
                "status": "invalid",
                "reason": "acquisition_plan_review_request_cancel_blocked_after_approval",
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
                "plan_review_session": _plan_review_session_api_summary(review_session),
                **self._kernel._workflow_command_control_response_policy_records(command_payload),
                "module_state_mutated": False,
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        cancelled_review: dict[str, Any] = {}
        if review_session:
            cancelled_review = (
                self.store.review_plan_session(
                    review_id=int(review_session.get("review_id") or 0),
                    status="cancelled",
                    reviewer=actor,
                    notes=reason,
                    decision_payload={
                        "status": "cancelled",
                        "reason": reason,
                        "control_source": "api.workflow_command_owner_specific_cancel",
                        "command_id": command_id,
                    },
                )
                or {}
            )
        updated = self.store.cancel_workflow_command(
            command_id,
            reason=reason,
            actor=actor,
            result={
                "control_source": "api.workflow_command_owner_specific_cancel",
                "control_action": "cancel",
                "owner_specific_control": True,
                "plan_id": plan_id,
                "target_company": target_company,
                "plan_review_session_cancelled": bool(cancelled_review),
                "plan_review_id": int((cancelled_review or review_session or {}).get("review_id") or 0),
                "downstream_commit_planned": False,
            },
            from_statuses=("claimed", "running"),
        )
        if not updated:
            latest = self.store.get_workflow_command(command_id) or command_payload
            return {
                "status": "invalid",
                "reason": "workflow_command_owner_specific_cancel_not_applied",
                "workflow_command": self._kernel._workflow_command_api_record(latest),
                "plan_review_session": _plan_review_session_api_summary(cancelled_review or review_session),
                **self._kernel._workflow_command_control_response_policy_records(latest),
                "module_state_mutated": bool(cancelled_review),
                "owner_specific_control": True,
                "contract": "w11_workflow_command_owner_specific_control_v1",
            }
        operation_sync = self._sync_operation_run_from_workflow_command_control(
            updated,
            control_action="cancel",
            actor=actor,
            source="api.workflow_command_owner_specific_cancel",
        )
        return {
            "status": "cancelled",
            "workflow_command": self._kernel._workflow_command_api_record(updated),
            "operation_sync": operation_sync,
            "plan_review_session": _plan_review_session_api_summary(cancelled_review or review_session),
            **self._kernel._workflow_command_control_response_policy_records(updated),
            "module_state_mutated": bool(cancelled_review),
            "owner_specific_control": True,
            "contract": "w11_workflow_command_owner_specific_control_v1",
        }

    @staticmethod
    def _acquisition_decomposition_downstream_command_types() -> list[str]:
        return [
            ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
            ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
            ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
            ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
            ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
            ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
            ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
            LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
            LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
            LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
            LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
            LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
            PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
            PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
            PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
            PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
            PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
            COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
            SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
        ]

    def _plan_acquisition_intent_resolve_command(
        self,
        *,
        parent_command: dict[str, Any],
        workflow_payload: dict[str, Any],
        target_company: str,
        query_text: str,
        plan_review_id: str,
    ) -> dict[str, Any]:
        parent = dict(parent_command or {})
        workflow_run_id = str(parent.get("workflow_run_id") or "").strip()
        operation_id = str(parent.get("operation_id") or "").strip()
        parent_command_id = str(parent.get("command_id") or "").strip()
        if not workflow_run_id or not operation_id or not parent_command_id:
            return {}
        idempotency_key = f"{ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE}:parent:{parent_command_id}"
        parent_payload = dict(parent.get("payload") or {})
        parent_causality = dict(parent_payload.get("causality") or {})
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent.get("causal_group_id") or "").strip()
            or parent_command_id
        )
        command_payload = {
            "workflow_payload": dict(workflow_payload or {}),
            "target_company": str(target_company or "").strip(),
            "query": str(query_text or "").strip(),
            "plan_review_id": str(plan_review_id or "").strip(),
            "intent_count": 1,
            "query_count": 1 if str(query_text or "").strip() else 0,
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "action_id": str(parent_payload.get("action_id") or "").strip(),
            "source": "acquisition_run_create.command_owner",
            "migration_phase": "W11b_acquisition_intent_resolve",
            "normal_path_executes_queue_workflow_inline": False,
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor="acquisition_run_create_owner",
                source="acquisition_run_create.command_owner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "acquisition_intent_resolve",
                    "stage_id": "acquisition_intent_resolve",
                    "command_type": ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "acquisition_intent_resolve",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_acquisition_plan_build_command(
        self,
        *,
        parent_command: dict[str, Any],
        resolved_intent: dict[str, Any],
    ) -> dict[str, Any]:
        parent = dict(parent_command or {})
        workflow_run_id = str(parent.get("workflow_run_id") or "").strip()
        operation_id = str(parent.get("operation_id") or "").strip()
        parent_command_id = str(parent.get("command_id") or "").strip()
        if not workflow_run_id or not operation_id or not parent_command_id:
            return {}
        idempotency_key = f"{ACQUISITION_PLAN_BUILD_COMMAND_TYPE}:parent:{parent_command_id}"
        parent_payload = dict(parent.get("payload") or {})
        parent_causality = dict(parent_payload.get("causality") or {})
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent.get("causal_group_id") or "").strip()
            or parent_command_id
        )
        command_payload = {
            "resolved_intent": dict(resolved_intent or {}),
            "target_company": str(dict(resolved_intent or {}).get("target_company") or "").strip(),
            "query": str(dict(resolved_intent or {}).get("query") or "").strip(),
            "plan_review_id": str(dict(resolved_intent or {}).get("plan_review_id") or "").strip(),
            "plan_count": 1,
            "query_count": 1 if str(dict(resolved_intent or {}).get("query") or "").strip() else 0,
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "action_id": str(parent_payload.get("action_id") or "").strip(),
            "source": "acquisition_intent_resolve.command_owner",
            "migration_phase": "W11b_acquisition_plan_build",
            "normal_path_executes_queue_workflow_inline": False,
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor="acquisition_intent_resolve_owner",
                source="acquisition_intent_resolve.command_owner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "acquisition_plan_build",
                    "stage_id": "acquisition_plan_build",
                    "command_type": ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "acquisition_plan_build",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_acquisition_plan_review_request_command(
        self,
        *,
        parent_command: dict[str, Any],
        acquisition_plan: dict[str, Any],
    ) -> dict[str, Any]:
        parent = dict(parent_command or {})
        workflow_run_id = str(parent.get("workflow_run_id") or "").strip()
        operation_id = str(parent.get("operation_id") or "").strip()
        parent_command_id = str(parent.get("command_id") or "").strip()
        plan_payload = dict(acquisition_plan or {})
        plan_id = str(plan_payload.get("plan_id") or "").strip()
        if not workflow_run_id or not operation_id or not parent_command_id or not plan_id:
            return {}
        idempotency_key = f"{ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE}:plan:{plan_id}:parent:{parent_command_id}"
        parent_payload = dict(parent.get("payload") or {})
        parent_causality = dict(parent_payload.get("causality") or {})
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent.get("causal_group_id") or "").strip()
            or parent_command_id
        )
        command_payload = {
            "acquisition_plan": plan_payload,
            "plan_id": plan_id,
            "target_company": str(plan_payload.get("target_company") or "").strip(),
            "query": str(plan_payload.get("query") or "").strip(),
            "plan_review_id": str(plan_payload.get("plan_review_id") or "").strip(),
            "plan_review_count": 1,
            "plan_count": 1,
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "action_id": str(parent_payload.get("action_id") or "").strip(),
            "source": "acquisition_plan_build.command_owner",
            "migration_phase": "W11b_acquisition_plan_review_request",
            "normal_path_executes_queue_workflow_inline": False,
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor="acquisition_plan_build_owner",
                source="acquisition_plan_build.command_owner",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "acquisition_plan_review_request",
                    "stage_id": "acquisition_plan_review_request",
                    "command_type": ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "acquisition_plan_review_request",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _execute_acquisition_intent_resolve_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        workflow_payload = dict(payload.get("workflow_payload") or {})
        target_company = str(payload.get("target_company") or workflow_payload.get("target_company") or "").strip()
        query_text = str(
            payload.get("query") or workflow_payload.get("query") or workflow_payload.get("raw_user_request") or ""
        ).strip()
        plan_review_id = str(payload.get("plan_review_id") or workflow_payload.get("plan_review_id") or "").strip()
        if not plan_review_id and not target_company and not query_text:
            return {
                "status": "invalid",
                "reason": "acquisition_intent_resolve_payload_missing_scope",
                "operation_completion_deferred": False,
            }
        resolved_intent = {
            "target_company": target_company,
            "query": query_text,
            "plan_review_id": plan_review_id,
            "workspace_id": str(
                payload.get("workspace_id") or workflow_payload.get("workspace_id") or "default"
            ).strip()
            or "default",
            "runtime_execution_mode": str(workflow_payload.get("runtime_execution_mode") or "operation_command").strip()
            or "operation_command",
            "source_workflow_payload": workflow_payload,
            "resolution_mode": "deterministic_payload_normalization",
            "model_used": "",
            "provider_called": False,
            "queue_workflow_called": False,
            "legacy_job_shell_created": False,
        }
        downstream_command = self._plan_acquisition_plan_build_command(
            parent_command=command_payload,
            resolved_intent=resolved_intent,
        )
        if not downstream_command:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_build_command_enqueue_failed",
                "operation_completion_deferred": False,
            }
        return {
            "status": "ready_for_plan_build",
            "reason": "acquisition_intent_resolved",
            "operation_completion_deferred": True,
            "module_state_mutated": False,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "resolved_intent": resolved_intent,
            "downstream_command_required": True,
            "downstream_command_count": 1,
            "downstream_command_ids": [str(downstream_command.get("command_id") or "").strip()],
            "downstream_commands": [
                self._kernel._workflow_command_observation(
                    downstream_command,
                    migration_phase="W11b_acquisition_plan_build",
                )
            ],
            "downstream_command_types": [ACQUISITION_PLAN_BUILD_COMMAND_TYPE],
            "next_phase": "W11b_acquisition_plan_build",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11b_acquisition_intent_resolve",
            "contract": "w11b_acquisition_intent_resolve_owner_v1",
        }

    def _run_acquisition_intent_resolve_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "acquisition_intent_resolve_command_id_missing"}
        if str(command_payload.get("status") or "").strip() == "succeeded":
            self._kernel._sync_operation_run_from_workflow_command(
                command_payload,
                actor=ACQUISITION_INTENT_RESOLVE_OWNER,
                source="acquisition_intent_resolve.command_owner",
            )
            return {
                "status": "completed",
                "reason": "acquisition_intent_resolve_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase="W11b_acquisition_intent_resolve",
                ),
            }
        lease_owner = f"{ACQUISITION_INTENT_RESOLVE_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            return {
                "status": "queued",
                "reason": "acquisition_intent_resolve_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11b_acquisition_intent_resolve",
                ),
            }
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed
        result = self._execute_acquisition_intent_resolve_command_payload(running, lease_owner=lease_owner)
        result_status = str(result.get("status") or "").strip()
        if result_status not in {"ready_for_plan_build"}:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=str(result.get("reason") or "acquisition_intent_resolve_invalid"),
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or running,
                actor=ACQUISITION_INTENT_RESOLVE_OWNER,
                source="acquisition_intent_resolve.command_owner",
            )
            return {
                "status": "failed",
                "reason": str(result.get("reason") or "acquisition_intent_resolve_invalid"),
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or running,
                    migration_phase="W11b_acquisition_intent_resolve",
                ),
            }
        succeeded = self.store.mark_workflow_command_succeeded(command_id, result=result)
        self._kernel._sync_operation_run_from_workflow_command(
            succeeded or running,
            actor=ACQUISITION_INTENT_RESOLVE_OWNER,
            source="acquisition_intent_resolve.command_owner",
        )
        return {
            "status": "completed",
            "reason": "acquisition_intent_resolved",
            "result": result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or running,
                migration_phase="W11b_acquisition_intent_resolve",
            ),
        }

    def _drain_acquisition_intent_resolve_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_intent_resolve_command_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=ACQUISITION_INTENT_RESOLVE_OWNER,
            command_type=ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
            limit=limit,
        )
        results = [self._run_acquisition_intent_resolve_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_intent_resolve_commands_drained"
            if results
            else "no_ready_acquisition_intent_resolve_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11b_acquisition_intent_resolve",
            "items": results,
        }

    def _build_acquisition_plan_from_resolved_intent(
        self,
        *,
        command: dict[str, Any],
        resolved_intent: dict[str, Any],
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        command_id = str(command_payload.get("command_id") or "").strip()
        intent = dict(resolved_intent or {})
        target_company = str(intent.get("target_company") or "").strip()
        query_text = str(intent.get("query") or "").strip()
        plan_review_id = str(intent.get("plan_review_id") or "").strip()
        plan_seed = f"{workflow_run_id}:{command_id}:{target_company}:{query_text}:{plan_review_id}"
        plan_id = "acqplan_" + hashlib.sha1(plan_seed.encode("utf-8")).hexdigest()[:24]
        stages = [
            {
                "stage_key": "plan_review",
                "command_type": ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
                "owner": ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
                "requires_human_review": True,
            },
            {
                "stage_key": "plan_commit",
                "command_type": ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
                "owner": ACQUISITION_PLAN_COMMIT_OWNER,
                "requires_human_review": False,
            },
            {
                "stage_key": "probe",
                "command_types": [ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE, ACQUISITION_PROBE_COLLECT_COMMAND_TYPE],
                "owner": ACQUISITION_PROBE_OWNER,
                "completion_policy": "probe_terminal_before_scale",
            },
            {
                "stage_key": "scale",
                "command_types": [ACQUISITION_SCALE_PLAN_COMMAND_TYPE, LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE],
                "owner": ACQUISITION_SCALE_PLAN_OWNER,
                "completion_policy": "scale_plan_then_lane_terminal",
            },
            {
                "stage_key": "profile",
                "command_types": [
                    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                ],
                "owner": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
                "completion_policy": "profile_fetch_terminal_recorded",
            },
            {
                "stage_key": "post_profile_materialization",
                "command_types": [
                    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
                    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
                    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
                    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
                    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
                    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
                ],
                "owner": "typed_materialization_owners",
                "completion_policy": "serving_projection_finalized_before_collection_merge",
            },
        ]
        return {
            "plan_id": plan_id,
            "workflow_run_id": workflow_run_id,
            "source_command_id": command_id,
            "target_company": target_company,
            "query": query_text,
            "plan_review_id": plan_review_id,
            "status": "ready_for_review",
            "plan_mode": "typed_staged_acquisition",
            "requires_plan_review": True,
            "provider_called": False,
            "model_called": False,
            "queue_workflow_called": False,
            "legacy_job_shell_created": False,
            "stages": stages,
            "contract": "w11b_acquisition_plan_build_result_v1",
        }

    def _sync_acquisition_plan_ready_from_workflow_command(
        self,
        command: dict[str, Any] | None,
        *,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        latest_command = self.store.get_workflow_command(command_id) if command_id else {}
        if latest_command:
            command_payload = latest_command
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        if not operation_run_id:
            payload = dict(command_payload.get("payload") or {})
            payload_causality = dict(payload.get("causality") or {})
            operation_run_id = str(
                payload.get("operation_id")
                or payload.get("operation_run_id")
                or payload_causality.get("operation_id")
                or ""
            ).strip()
        if not operation_run_id:
            return {"status": "skipped", "reason": "operation_id_missing"}
        operation_run = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {
                "status": "skipped",
                "reason": "operation_run_not_found",
                "operation_run_id": operation_run_id,
            }
        command_status = str(command_payload.get("status") or "").strip()
        if command_status != "succeeded":
            return {
                "status": "skipped",
                "reason": "workflow_command_not_plan_ready",
                "command_status": command_status,
                "operation_run_id": operation_run_id,
            }
        command_result = dict(command_payload.get("result") or {})
        acquisition_plan = dict(command_result.get("acquisition_plan") or {})
        workflow_ref = {
            "workflow_run_id": str(command_payload.get("workflow_run_id") or ""),
            "command_id": command_id,
            "command_type": str(command_payload.get("command_type") or ""),
            "owner": str(command_payload.get("owner") or ""),
        }
        operation_patch = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="running",
            progress_patch={
                "phase": "acquisition_plan_ready_for_review",
                "command_status": command_status,
                "command_attempt": int(command_payload.get("attempt") or 0),
                "plan_id": str(acquisition_plan.get("plan_id") or ""),
                "requires_plan_review": bool(acquisition_plan.get("requires_plan_review")),
                **workflow_ref,
            },
            workflow_ref_patch=workflow_ref,
            result_ref_patch={
                "workflow_command": {**workflow_ref, "status": command_status},
                "workflow_command_result": command_result,
                "acquisition_plan": acquisition_plan,
            },
            metadata_patch={
                "last_command_terminal_status": command_status,
                "last_command_sync_source": source,
                "awaiting_plan_review": True,
            },
        )
        action_id = str(operation_patch.get("action_id") or operation_run.get("action_id") or "").strip()
        if action_id:
            self.store.repos.workflow_runtime.update_action_state(
                action_id,
                status="running",
                result_ref_patch={
                    "operation_run_id": operation_run_id,
                    "workflow_command": {**workflow_ref, "status": command_status},
                    "acquisition_plan": acquisition_plan,
                },
                metadata_patch={
                    "last_operation_command_status": command_status,
                    "awaiting_plan_review": True,
                },
            )
        event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(
                operation_patch.get("workspace_id") or operation_run.get("workspace_id") or "default"
            ).strip()
            or "default",
            event_stream_id=operation_run_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationAcquisitionPlanReady",
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:OperationAcquisitionPlanReady:{command_id}:"
                f"{int(command_payload.get('attempt') or 0)}"
            ),
            actor=actor,
            source=source,
            payload={
                **workflow_ref,
                "command_status": command_status,
                "acquisition_plan": acquisition_plan,
                "module_state_mutated": False,
                "requires_plan_review": bool(acquisition_plan.get("requires_plan_review")),
            },
        )
        return self._kernel._workflow_command_operation_sync_api_record(
            {
                "status": "running",
                "operation_run": operation_patch,
                "event": event,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
            }
        )

    def _execute_acquisition_plan_build_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        resolved_intent = dict(payload.get("resolved_intent") or {})
        target_company = str(resolved_intent.get("target_company") or payload.get("target_company") or "").strip()
        query_text = str(resolved_intent.get("query") or payload.get("query") or "").strip()
        plan_review_id = str(resolved_intent.get("plan_review_id") or payload.get("plan_review_id") or "").strip()
        if not plan_review_id and not target_company and not query_text:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_build_payload_missing_scope",
                "operation_completion_deferred": False,
            }
        if not resolved_intent:
            resolved_intent = {
                "target_company": target_company,
                "query": query_text,
                "plan_review_id": plan_review_id,
                "resolution_mode": "payload_passthrough",
            }
        acquisition_plan = self._build_acquisition_plan_from_resolved_intent(
            command=command_payload,
            resolved_intent=resolved_intent,
        )
        downstream_command = self._plan_acquisition_plan_review_request_command(
            parent_command=command_payload,
            acquisition_plan=acquisition_plan,
        )
        if not downstream_command:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_review_request_command_enqueue_failed",
                "operation_completion_deferred": False,
            }
        return {
            "status": "plan_ready_for_review",
            "reason": "acquisition_plan_built",
            "operation_completion_deferred": True,
            "human_review_required": True,
            "module_state_mutated": False,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "model_called": False,
            "legacy_job_shell_created": False,
            "acquisition_plan": acquisition_plan,
            "downstream_command_required": True,
            "downstream_command_count": 1,
            "downstream_command_ids": [str(downstream_command.get("command_id") or "").strip()],
            "downstream_commands": [
                self._kernel._workflow_command_observation(
                    downstream_command,
                    migration_phase="W11b_acquisition_plan_review_request",
                )
            ],
            "downstream_command_types": [
                ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
                ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
            ],
            "next_phase": "W11b_acquisition_plan_review_request",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11b_acquisition_plan_build",
            "contract": "w11b_acquisition_plan_build_owner_v1",
        }

    def _run_acquisition_plan_build_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "acquisition_plan_build_command_id_missing"}
        if str(command_payload.get("status") or "").strip() == "succeeded":
            self._sync_acquisition_plan_ready_from_workflow_command(
                command_payload,
                actor=ACQUISITION_PLAN_BUILD_OWNER,
                source="acquisition_plan_build.command_owner",
            )
            return {
                "status": "completed",
                "reason": "acquisition_plan_build_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase="W11b_acquisition_plan_build",
                ),
            }
        lease_owner = f"{ACQUISITION_PLAN_BUILD_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            if str(latest.get("status") or "").strip() == "succeeded":
                self._sync_acquisition_plan_ready_from_workflow_command(
                    latest,
                    actor=ACQUISITION_PLAN_BUILD_OWNER,
                    source="acquisition_plan_build.command_owner",
                )
                return {
                    "status": "completed",
                    "reason": "acquisition_plan_build_command_already_succeeded",
                    "workflow_command": self._kernel._workflow_command_observation(
                        latest,
                        migration_phase="W11b_acquisition_plan_build",
                    ),
                }
            return {
                "status": "queued",
                "reason": "acquisition_plan_build_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11b_acquisition_plan_build",
                ),
            }
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed
        result = self._execute_acquisition_plan_build_command_payload(running, lease_owner=lease_owner)
        result_status = str(result.get("status") or "").strip()
        if result_status not in {"plan_ready_for_review"}:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=str(result.get("reason") or "acquisition_plan_build_invalid"),
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or running,
                actor=ACQUISITION_PLAN_BUILD_OWNER,
                source="acquisition_plan_build.command_owner",
            )
            return {
                "status": "failed",
                "reason": str(result.get("reason") or "acquisition_plan_build_invalid"),
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or running,
                    migration_phase="W11b_acquisition_plan_build",
                ),
            }
        succeeded = self.store.mark_workflow_command_succeeded(command_id, result=result)
        self._sync_acquisition_plan_ready_from_workflow_command(
            succeeded or running,
            actor=ACQUISITION_PLAN_BUILD_OWNER,
            source="acquisition_plan_build.command_owner",
        )
        return {
            "status": "completed",
            "reason": "acquisition_plan_built",
            "result": result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or running,
                migration_phase="W11b_acquisition_plan_build",
            ),
        }

    def _drain_acquisition_plan_build_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_plan_build_command_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=ACQUISITION_PLAN_BUILD_OWNER,
            command_type=ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
            limit=limit,
        )
        results = [self._run_acquisition_plan_build_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_plan_build_commands_drained"
            if results
            else "no_ready_acquisition_plan_build_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11b_acquisition_plan_build",
            "items": results,
        }

    @staticmethod
    def _acquisition_plan_review_request_payload(acquisition_plan: dict[str, Any]) -> dict[str, Any]:
        plan_payload = dict(acquisition_plan or {})
        return {
            "target_company": str(plan_payload.get("target_company") or "").strip(),
            "query": str(plan_payload.get("query") or "").strip(),
            "plan_id": str(plan_payload.get("plan_id") or "").strip(),
            "workflow_run_id": str(plan_payload.get("workflow_run_id") or "").strip(),
            "source_command_id": str(plan_payload.get("source_command_id") or "").strip(),
            "request_source": ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
        }

    @staticmethod
    def _acquisition_plan_review_gate_payload(acquisition_plan: dict[str, Any]) -> dict[str, Any]:
        plan_payload = dict(acquisition_plan or {})
        return {
            "required_before_execution": True,
            "risk_level": "medium",
            "review_reason": "typed_acquisition_plan_requires_review_before_provider_work",
            "source": ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
            "plan_id": str(plan_payload.get("plan_id") or "").strip(),
            "downstream_stage_count": len(list(plan_payload.get("stages") or [])),
        }

    def _find_existing_acquisition_plan_review_session(
        self,
        *,
        target_company: str,
        plan_id: str,
    ) -> dict[str, Any]:
        normalized_company = str(target_company or "").strip()
        normalized_plan_id = str(plan_id or "").strip()
        if not normalized_company or not normalized_plan_id:
            return {}
        for status in ("pending", "ready", "approved"):
            try:
                sessions = self.store.list_plan_review_sessions(
                    target_company=normalized_company,
                    status=status,
                    limit=100,
                )
            except Exception:
                sessions = []
            for session in sessions:
                request_payload = dict(session.get("request") or {})
                plan_payload = dict(session.get("plan") or {})
                if (
                    str(request_payload.get("plan_id") or "").strip() == normalized_plan_id
                    or str(plan_payload.get("plan_id") or "").strip() == normalized_plan_id
                ):
                    return dict(session)
        return {}

    def _create_or_get_acquisition_plan_review_session(
        self,
        *,
        acquisition_plan: dict[str, Any],
        command: dict[str, Any],
    ) -> dict[str, Any]:
        plan_payload = dict(acquisition_plan or {})
        command_payload = dict(command or {})
        target_company = str(plan_payload.get("target_company") or "").strip()
        plan_id = str(plan_payload.get("plan_id") or "").strip()
        existing = self._find_existing_acquisition_plan_review_session(
            target_company=target_company,
            plan_id=plan_id,
        )
        if existing:
            return {
                "status": "reused",
                "plan_review_session": existing,
                "created": False,
            }
        request_payload = self._acquisition_plan_review_request_payload(plan_payload)
        gate_payload = self._acquisition_plan_review_gate_payload(plan_payload)
        execution_bundle = {
            "request": request_payload,
            "plan": plan_payload,
            "source": ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
            "workflow_run_id": str(plan_payload.get("workflow_run_id") or "").strip(),
            "operation_id": str(command_payload.get("operation_id") or "").strip(),
            "plan_review_request_command_id": str(command_payload.get("command_id") or "").strip(),
            "plan_review_request_parent_command_id": str(command_payload.get("parent_command_id") or "").strip(),
            "causal_group_id": str(command_payload.get("causal_group_id") or "").strip(),
            "plan_id": plan_id,
            "provider_called": False,
            "queue_workflow_called": False,
        }
        session = self.store.create_plan_review_session(
            target_company=target_company,
            request_payload=request_payload,
            plan_payload=plan_payload,
            gate_payload=gate_payload,
            execution_bundle_payload=execution_bundle,
        )
        return {
            "status": "created",
            "plan_review_session": dict(session or {}),
            "created": True,
        }

    def _sync_acquisition_plan_review_requested_from_workflow_command(
        self,
        command: dict[str, Any] | None,
        *,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        latest_command = self.store.get_workflow_command(command_id) if command_id else {}
        if latest_command:
            command_payload = latest_command
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        if not operation_run_id:
            operation_run_id = str(dict(command_payload.get("payload") or {}).get("operation_id") or "").strip()
        if not operation_run_id:
            return {"status": "skipped", "reason": "operation_id_missing"}
        operation_run = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {
                "status": "skipped",
                "reason": "operation_run_not_found",
                "operation_run_id": operation_run_id,
            }
        command_status = str(command_payload.get("status") or "").strip()
        if command_status != "succeeded":
            return {
                "status": "skipped",
                "reason": "workflow_command_not_plan_review_requested",
                "command_status": command_status,
                "operation_run_id": operation_run_id,
            }
        command_result = dict(command_payload.get("result") or {})
        review_session = dict(command_result.get("plan_review_session") or {})
        workflow_ref = {
            "workflow_run_id": str(command_payload.get("workflow_run_id") or ""),
            "command_id": command_id,
            "command_type": str(command_payload.get("command_type") or ""),
            "owner": str(command_payload.get("owner") or ""),
        }
        operation_patch = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="running",
            progress_patch={
                "phase": "acquisition_plan_review_requested",
                "command_status": command_status,
                "command_attempt": int(command_payload.get("attempt") or 0),
                "plan_review_id": int(review_session.get("review_id") or 0),
                "plan_review_status": str(review_session.get("status") or ""),
                **workflow_ref,
            },
            workflow_ref_patch=workflow_ref,
            result_ref_patch={
                "workflow_command": {**workflow_ref, "status": command_status},
                "workflow_command_result": command_result,
                "plan_review_session": _plan_review_session_api_summary(review_session),
            },
            metadata_patch={
                "last_command_terminal_status": command_status,
                "last_command_sync_source": source,
                "awaiting_plan_review": True,
                "plan_review_id": int(review_session.get("review_id") or 0),
            },
        )
        action_id = str(operation_patch.get("action_id") or operation_run.get("action_id") or "").strip()
        if action_id:
            self.store.repos.workflow_runtime.update_action_state(
                action_id,
                status="running",
                result_ref_patch={
                    "operation_run_id": operation_run_id,
                    "workflow_command": {**workflow_ref, "status": command_status},
                    "plan_review_session": _plan_review_session_api_summary(review_session),
                },
                metadata_patch={
                    "last_operation_command_status": command_status,
                    "awaiting_plan_review": True,
                    "plan_review_id": int(review_session.get("review_id") or 0),
                },
            )
        event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(
                operation_patch.get("workspace_id") or operation_run.get("workspace_id") or "default"
            ).strip()
            or "default",
            event_stream_id=operation_run_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationAcquisitionPlanReviewRequested",
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:OperationAcquisitionPlanReviewRequested:{command_id}:"
                f"{int(command_payload.get('attempt') or 0)}"
            ),
            actor=actor,
            source=source,
            payload={
                **workflow_ref,
                "command_status": command_status,
                "plan_review_session": _plan_review_session_api_summary(review_session),
                "module_state_mutated": False,
                "requires_plan_review": True,
            },
        )
        return self._kernel._workflow_command_operation_sync_api_record(
            {
                "status": "running",
                "operation_run": operation_patch,
                "event": event,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
            }
        )

    def _execute_acquisition_plan_review_request_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        acquisition_plan = dict(payload.get("acquisition_plan") or {})
        plan_id = str(acquisition_plan.get("plan_id") or payload.get("plan_id") or "").strip()
        target_company = str(acquisition_plan.get("target_company") or payload.get("target_company") or "").strip()
        if not plan_id or not target_company:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_review_request_payload_missing_plan",
                "operation_completion_deferred": False,
            }
        review_result = self._create_or_get_acquisition_plan_review_session(
            acquisition_plan=acquisition_plan,
            command=command_payload,
        )
        review_session = dict(review_result.get("plan_review_session") or {})
        if not int(review_session.get("review_id") or 0):
            return {
                "status": "invalid",
                "reason": "acquisition_plan_review_session_create_failed",
                "operation_completion_deferred": False,
            }
        return {
            "status": "plan_review_requested",
            "reason": "acquisition_plan_review_requested",
            "operation_completion_deferred": True,
            "human_review_required": True,
            "module_state_mutated": True,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "model_called": False,
            "legacy_job_shell_created": False,
            "plan_id": plan_id,
            "plan_review_session": _plan_review_session_api_summary(review_session),
            "plan_review_created": bool(review_result.get("created")),
            "downstream_command_required": True,
            "downstream_command_count": 0,
            "downstream_command_ids": [],
            "downstream_command_types": [ACQUISITION_PLAN_COMMIT_COMMAND_TYPE],
            "next_phase": "W11b_acquisition_plan_commit_after_review",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11b_acquisition_plan_review_request",
            "contract": "w11b_acquisition_plan_review_request_owner_v1",
        }

    def _run_acquisition_plan_review_request_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "acquisition_plan_review_request_command_id_missing"}
        if str(command_payload.get("status") or "").strip() == "succeeded":
            self._sync_acquisition_plan_review_requested_from_workflow_command(
                command_payload,
                actor=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
                source="acquisition_plan_review_request.command_owner",
            )
            return {
                "status": "completed",
                "reason": "acquisition_plan_review_request_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase="W11b_acquisition_plan_review_request",
                ),
            }
        lease_owner = f"{ACQUISITION_PLAN_REVIEW_REQUEST_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            if str(latest.get("status") or "").strip() == "succeeded":
                self._sync_acquisition_plan_review_requested_from_workflow_command(
                    latest,
                    actor=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
                    source="acquisition_plan_review_request.command_owner",
                )
                return {
                    "status": "completed",
                    "reason": "acquisition_plan_review_request_command_already_succeeded",
                    "workflow_command": self._kernel._workflow_command_observation(
                        latest,
                        migration_phase="W11b_acquisition_plan_review_request",
                    ),
                }
            return {
                "status": "queued",
                "reason": "acquisition_plan_review_request_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11b_acquisition_plan_review_request",
                ),
            }
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed
        result = self._execute_acquisition_plan_review_request_command_payload(running, lease_owner=lease_owner)
        result_status = str(result.get("status") or "").strip()
        if result_status not in {"plan_review_requested"}:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=str(result.get("reason") or "acquisition_plan_review_request_invalid"),
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or running,
                actor=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
                source="acquisition_plan_review_request.command_owner",
            )
            return {
                "status": "failed",
                "reason": str(result.get("reason") or "acquisition_plan_review_request_invalid"),
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or running,
                    migration_phase="W11b_acquisition_plan_review_request",
                ),
            }
        succeeded = self.store.mark_workflow_command_succeeded(command_id, result=result)
        self._sync_acquisition_plan_review_requested_from_workflow_command(
            succeeded or running,
            actor=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
            source="acquisition_plan_review_request.command_owner",
        )
        return {
            "status": "completed",
            "reason": "acquisition_plan_review_requested",
            "result": result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or running,
                migration_phase="W11b_acquisition_plan_review_request",
            ),
        }

    def _drain_acquisition_plan_review_request_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_plan_review_request_command_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
            command_type=ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
            limit=limit,
        )
        results = [self._run_acquisition_plan_review_request_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_plan_review_request_commands_drained"
            if results
            else "no_ready_acquisition_plan_review_request_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11b_acquisition_plan_review_request",
            "items": results,
        }

    def _plan_acquisition_plan_commit_command_from_review(
        self,
        *,
        review_session: dict[str, Any],
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        session = dict(review_session or {})
        review_id = int(session.get("review_id") or 0)
        if review_id <= 0 or str(session.get("status") or "").strip() not in {"approved", "ready"}:
            return {}
        execution_bundle = dict(session.get("execution_bundle") or {})
        plan_payload = dict(execution_bundle.get("plan") or session.get("plan") or {})
        request_payload = dict(execution_bundle.get("request") or session.get("request") or {})
        workflow_run_id = str(
            execution_bundle.get("workflow_run_id") or plan_payload.get("workflow_run_id") or ""
        ).strip()
        operation_id = str(execution_bundle.get("operation_id") or "").strip()
        parent_command_id = str(
            execution_bundle.get("plan_review_request_command_id")
            or execution_bundle.get("plan_review_request_parent_command_id")
            or plan_payload.get("source_command_id")
            or ""
        ).strip()
        if not workflow_run_id or not operation_id:
            return {}
        idempotency_key = f"{ACQUISITION_PLAN_COMMIT_COMMAND_TYPE}:review:{review_id}"
        causal_group_id = str(
            execution_bundle.get("causal_group_id") or parent_command_id or f"review:{review_id}"
        ).strip()
        command_payload = {
            "review_id": review_id,
            "plan_review_session": _plan_review_session_api_summary(session),
            "request": request_payload,
            "plan": plan_payload,
            "execution_bundle": execution_bundle,
            "plan_id": str(plan_payload.get("plan_id") or request_payload.get("plan_id") or "").strip(),
            "target_company": str(session.get("target_company") or request_payload.get("target_company") or "").strip(),
            "plan_review_count": 1,
            "plan_count": 1,
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "source": source,
            "migration_phase": "W11b_acquisition_plan_commit",
            "normal_path_executes_queue_workflow_inline": False,
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor=actor or "plan_review_api",
                source=source or "plan_review.approved",
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": "acquisition_plan_commit",
                    "stage_id": "acquisition_plan_commit",
                    "command_type": ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": "acquisition_plan_commit",
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_acquisition_run_phase_command(
        self,
        *,
        parent_command: dict[str, Any],
        acquisition_run: dict[str, Any],
        command_type: str,
        stage_key: str,
        source: str,
        migration_phase: str,
        retry_kind: str,
        extra_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        parent = dict(parent_command or {})
        run_payload = dict(acquisition_run or {})
        workflow_run_id = str(parent.get("workflow_run_id") or run_payload.get("workflow_run_id") or "").strip()
        operation_id = str(parent.get("operation_id") or run_payload.get("operation_run_id") or "").strip()
        parent_command_id = str(parent.get("command_id") or "").strip()
        acquisition_run_id = str(run_payload.get("acquisition_run_id") or "").strip()
        if not workflow_run_id or not operation_id or not parent_command_id or not acquisition_run_id:
            return {}
        try:
            owner = DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(command_type)
        except KeyError:
            return {}
        parent_payload = dict(parent.get("payload") or {})
        parent_causality = dict(parent_payload.get("causality") or {})
        causal_group_id = (
            str(parent_causality.get("causal_group_id") or "").strip()
            or str(parent.get("causal_group_id") or "").strip()
            or str(dict(run_payload.get("metadata") or {}).get("causal_group_id") or "").strip()
            or parent_command_id
        )
        idempotency_key = f"{command_type}:acquisition_run:{acquisition_run_id}:parent:{parent_command_id}"
        query_text = str(run_payload.get("query") or "").strip()
        command_payload = {
            "acquisition_run_id": acquisition_run_id,
            "acquisition_run": run_payload,
            "target_company": str(run_payload.get("target_company") or "").strip(),
            "query": query_text,
            "plan_id": str(run_payload.get("plan_id") or "").strip(),
            "plan_review_id": int(run_payload.get("plan_review_id") or 0),
            "acquisition_run_count": 1,
            "query_count": 1 if query_text else 0,
            "parent_command_id": parent_command_id,
            "causal_group_id": causal_group_id,
            "operation_run_id": operation_id,
            "source": source,
            "migration_phase": migration_phase,
            "normal_path_executes_queue_workflow_inline": False,
            "legacy_job_shell_created": False,
            "provider_called": False,
            **dict(extra_payload or {}),
        }
        try:
            apply_result = self.durable_runtime_writer.append_event_and_reduce(
                workflow_run_id=workflow_run_id,
                operation_id=operation_id,
                command_id=parent_command_id,
                event_family="workflow_event",
                event_type="CommandPlanRequested",
                idempotency_key=f"{idempotency_key}:plan",
                actor=owner,
                source=source,
                payload={
                    "workflow_type": "agent_callable_acquisition",
                    "stage_key": stage_key,
                    "stage_id": default_stage_id_for_command_type(command_type) or stage_key,
                    "command_type": command_type,
                    "idempotency_key": idempotency_key,
                    "parent_command_id": parent_command_id,
                    "causal_group_id": causal_group_id,
                    "payload": command_payload,
                    "max_attempts": 3,
                    "retry_policy": {
                        "kind": retry_kind,
                        "retry_delay_seconds": 10,
                    },
                },
            )
        except Exception:
            return {}
        return self._kernel._workflow_command_from_apply_result_or_store(
            apply_result=apply_result,
            workflow_run_id=workflow_run_id,
            idempotency_key=idempotency_key,
        )

    def _plan_acquisition_probe_submit_command(
        self,
        *,
        parent_command: dict[str, Any],
        acquisition_run: dict[str, Any],
    ) -> dict[str, Any]:
        return self._plan_acquisition_run_phase_command(
            parent_command=parent_command,
            acquisition_run=acquisition_run,
            command_type=ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
            stage_key="acquisition_probe_submit",
            source="acquisition_plan_commit.command_owner",
            migration_phase="W11c_acquisition_probe_submit",
            retry_kind="acquisition_probe_submit",
        )

    def _plan_acquisition_probe_collect_command(
        self,
        *,
        parent_command: dict[str, Any],
        acquisition_run: dict[str, Any],
    ) -> dict[str, Any]:
        return self._plan_acquisition_run_phase_command(
            parent_command=parent_command,
            acquisition_run=acquisition_run,
            command_type=ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
            stage_key="acquisition_probe_collect",
            source="acquisition_probe_submit.command_owner",
            migration_phase="W11c_acquisition_probe_collect",
            retry_kind="acquisition_probe_collect",
            extra_payload={"probe_result_count": 1},
        )

    def _plan_acquisition_scale_plan_command(
        self,
        *,
        parent_command: dict[str, Any],
        acquisition_run: dict[str, Any],
        probe_result: dict[str, Any],
    ) -> dict[str, Any]:
        return self._plan_acquisition_run_phase_command(
            parent_command=parent_command,
            acquisition_run=acquisition_run,
            command_type=ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
            stage_key="acquisition_scale_plan",
            source="acquisition_probe_collect.command_owner",
            migration_phase="W11c_acquisition_scale_plan",
            retry_kind="acquisition_scale_plan",
            extra_payload={
                "probe_result": dict(probe_result or {}),
                "probe_result_count": 1,
                "lane_count": 1,
            },
        )

    def _sync_acquisition_plan_committed_from_workflow_command(
        self,
        command: dict[str, Any] | None,
        *,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        latest_command = self.store.get_workflow_command(command_id) if command_id else {}
        if latest_command:
            command_payload = latest_command
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        if not operation_run_id:
            operation_run_id = str(dict(command_payload.get("payload") or {}).get("operation_id") or "").strip()
        if not operation_run_id:
            return {"status": "skipped", "reason": "operation_id_missing"}
        operation_run = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {
                "status": "skipped",
                "reason": "operation_run_not_found",
                "operation_run_id": operation_run_id,
            }
        command_status = str(command_payload.get("status") or "").strip()
        if command_status != "succeeded":
            return {
                "status": "skipped",
                "reason": "workflow_command_not_plan_committed",
                "command_status": command_status,
                "operation_run_id": operation_run_id,
            }
        command_result = dict(command_payload.get("result") or {})
        review_session = dict(command_result.get("plan_review_session") or {})
        acquisition_run = dict(command_result.get("acquisition_run") or {})
        acquisition_run_id = str(
            command_result.get("acquisition_run_id") or acquisition_run.get("acquisition_run_id") or ""
        ).strip()
        workflow_ref = {
            "workflow_run_id": str(command_payload.get("workflow_run_id") or ""),
            "command_id": command_id,
            "command_type": str(command_payload.get("command_type") or ""),
            "owner": str(command_payload.get("owner") or ""),
        }
        operation_patch = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="running",
            progress_patch={
                "phase": "acquisition_plan_committed_pending_probe",
                "command_status": command_status,
                "command_attempt": int(command_payload.get("attempt") or 0),
                "plan_review_id": int(review_session.get("review_id") or 0),
                "acquisition_run_id": acquisition_run_id,
                **workflow_ref,
            },
            workflow_ref_patch=workflow_ref,
            result_ref_patch={
                "workflow_command": {**workflow_ref, "status": command_status},
                "workflow_command_result": command_result,
                "plan_review_session": _plan_review_session_api_summary(review_session),
                "acquisition_run": acquisition_run,
            },
            metadata_patch={
                "last_command_terminal_status": command_status,
                "last_command_sync_source": source,
                "awaiting_probe_commands": True,
                "awaiting_plan_review": False,
            },
        )
        action_id = str(operation_patch.get("action_id") or operation_run.get("action_id") or "").strip()
        if action_id:
            self.store.repos.workflow_runtime.update_action_state(
                action_id,
                status="running",
                result_ref_patch={
                    "operation_run_id": operation_run_id,
                    "workflow_command": {**workflow_ref, "status": command_status},
                    "plan_review_session": _plan_review_session_api_summary(review_session),
                    "acquisition_run": acquisition_run,
                },
                metadata_patch={
                    "last_operation_command_status": command_status,
                    "awaiting_probe_commands": True,
                    "awaiting_plan_review": False,
                },
            )
        event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(
                operation_patch.get("workspace_id") or operation_run.get("workspace_id") or "default"
            ).strip()
            or "default",
            event_stream_id=operation_run_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationAcquisitionPlanCommitted",
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:OperationAcquisitionPlanCommitted:{command_id}:"
                f"{int(command_payload.get('attempt') or 0)}"
            ),
            actor=actor,
            source=source,
            payload={
                **workflow_ref,
                "command_status": command_status,
                "plan_review_session": _plan_review_session_api_summary(review_session),
                "acquisition_run_id": acquisition_run_id,
                "module_state_mutated": bool(acquisition_run_id),
                "next_phase": "W11c_acquisition_probe_scale",
            },
        )
        return self._kernel._workflow_command_operation_sync_api_record(
            {
                "status": "running",
                "operation_run": operation_patch,
                "event": event,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
            }
        )

    def _execute_acquisition_plan_commit_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        review_id = int(payload.get("review_id") or 0)
        review_session = self.store.get_plan_review_session(review_id) if review_id > 0 else None
        if review_session is None:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_commit_review_session_missing",
                "operation_completion_deferred": False,
            }
        if str(review_session.get("status") or "").strip() not in {"approved", "ready"}:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_commit_review_not_approved",
                "operation_completion_deferred": False,
            }
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(command_payload.get("operation_id") or payload.get("operation_run_id") or "").strip()
        if not workflow_run_id or not operation_run_id:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_commit_missing_workflow_or_operation_id",
                "operation_completion_deferred": False,
            }
        execution_bundle = dict(payload.get("execution_bundle") or review_session.get("execution_bundle") or {})
        request_payload = dict(
            payload.get("request") or execution_bundle.get("request") or review_session.get("request") or {}
        )
        plan_payload = dict(payload.get("plan") or execution_bundle.get("plan") or review_session.get("plan") or {})
        target_company = str(
            payload.get("target_company")
            or review_session.get("target_company")
            or request_payload.get("target_company")
            or plan_payload.get("target_company")
            or ""
        ).strip()
        query_text = str(
            request_payload.get("query") or request_payload.get("raw_user_request") or plan_payload.get("query") or ""
        ).strip()
        plan_id = str(
            payload.get("plan_id") or plan_payload.get("plan_id") or request_payload.get("plan_id") or ""
        ).strip()
        operation_run = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        workspace_id = (
            str(
                (operation_run or {}).get("workspace_id")
                or payload.get("workspace_id")
                or request_payload.get("tenant_id")
                or "default"
            ).strip()
            or "default"
        )
        acquisition_run_id = (
            "acqrun_"
            + hashlib.sha1(f"{workflow_run_id}:{operation_run_id}:{review_id}:{plan_id}".encode("utf-8")).hexdigest()[
                :24
            ]
        )
        acquisition_run = self.store.repos.workflow_runtime.upsert_acquisition_run(
            {
                "acquisition_run_id": acquisition_run_id,
                "workspace_id": workspace_id,
                "operation_run_id": operation_run_id,
                "workflow_run_id": workflow_run_id,
                "plan_id": plan_id,
                "plan_review_id": review_id,
                "target_company": target_company,
                "query": query_text,
                "status": "committed_pending_probe",
                "current_phase": "probe_pending",
                "request": request_payload,
                "plan": plan_payload,
                "execution_bundle": execution_bundle,
                "idempotency_key": f"acquisition_run:plan_commit:{command_payload.get('command_id') or review_id}",
                "metadata": {
                    "source_command_id": str(command_payload.get("command_id") or "").strip(),
                    "source_command_type": str(command_payload.get("command_type") or "").strip(),
                    "causal_group_id": str(
                        command_payload.get("causal_group_id") or payload.get("causal_group_id") or ""
                    ).strip(),
                    "owner": ACQUISITION_PLAN_COMMIT_OWNER,
                    "normal_path_executes_queue_workflow_inline": False,
                    "legacy_job_shell_created": False,
                    "next_phase": "W11c_acquisition_probe_scale",
                },
            }
        )
        if not acquisition_run:
            return {
                "status": "invalid",
                "reason": "acquisition_plan_commit_failed_to_materialize_acquisition_run",
                "operation_completion_deferred": False,
            }
        downstream_command = self._plan_acquisition_probe_submit_command(
            parent_command=command_payload,
            acquisition_run=acquisition_run,
        )
        if not downstream_command:
            return {
                "status": "invalid",
                "reason": "acquisition_probe_submit_command_enqueue_failed",
                "operation_completion_deferred": False,
            }
        return {
            "status": "plan_committed",
            "reason": "acquisition_plan_committed",
            "operation_completion_deferred": True,
            "human_review_required": False,
            "module_state_mutated": True,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "model_called": False,
            "legacy_job_shell_created": False,
            "acquisition_run": acquisition_run,
            "acquisition_run_id": acquisition_run["acquisition_run_id"],
            "plan_review_session": _plan_review_session_api_summary(review_session),
            "downstream_command_required": True,
            "downstream_command_count": 1,
            "downstream_command_ids": [str(downstream_command.get("command_id") or "").strip()],
            "downstream_commands": [
                self._kernel._workflow_command_observation(
                    downstream_command,
                    migration_phase="W11c_acquisition_probe_submit",
                )
            ],
            "downstream_command_types": [
                ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
                ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
                ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
            ],
            "next_phase": "W11c_acquisition_probe_scale",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11b_acquisition_plan_commit",
            "contract": "w11b_acquisition_plan_commit_owner_v1",
        }

    def _run_acquisition_plan_commit_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "acquisition_plan_commit_command_id_missing"}
        if str(command_payload.get("status") or "").strip() == "succeeded":
            self._sync_acquisition_plan_committed_from_workflow_command(
                command_payload,
                actor=ACQUISITION_PLAN_COMMIT_OWNER,
                source="acquisition_plan_commit.command_owner",
            )
            return {
                "status": "completed",
                "reason": "acquisition_plan_commit_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase="W11b_acquisition_plan_commit",
                ),
            }
        lease_owner = f"{ACQUISITION_PLAN_COMMIT_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            if str(latest.get("status") or "").strip() == "succeeded":
                self._sync_acquisition_plan_committed_from_workflow_command(
                    latest,
                    actor=ACQUISITION_PLAN_COMMIT_OWNER,
                    source="acquisition_plan_commit.command_owner",
                )
                return {
                    "status": "completed",
                    "reason": "acquisition_plan_commit_command_already_succeeded",
                    "workflow_command": self._kernel._workflow_command_observation(
                        latest,
                        migration_phase="W11b_acquisition_plan_commit",
                    ),
                }
            return {
                "status": "queued",
                "reason": "acquisition_plan_commit_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11b_acquisition_plan_commit",
                ),
            }
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed
        result = self._execute_acquisition_plan_commit_command_payload(running, lease_owner=lease_owner)
        result_status = str(result.get("status") or "").strip()
        if result_status not in {"plan_committed"}:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=str(result.get("reason") or "acquisition_plan_commit_invalid"),
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or running,
                actor=ACQUISITION_PLAN_COMMIT_OWNER,
                source="acquisition_plan_commit.command_owner",
            )
            return {
                "status": "failed",
                "reason": str(result.get("reason") or "acquisition_plan_commit_invalid"),
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or running,
                    migration_phase="W11b_acquisition_plan_commit",
                ),
            }
        succeeded = self.store.mark_workflow_command_succeeded(command_id, result=result)
        self._sync_acquisition_plan_committed_from_workflow_command(
            succeeded or running,
            actor=ACQUISITION_PLAN_COMMIT_OWNER,
            source="acquisition_plan_commit.command_owner",
        )
        return {
            "status": "completed",
            "reason": "acquisition_plan_committed",
            "result": result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or running,
                migration_phase="W11b_acquisition_plan_commit",
            ),
        }

    def _drain_acquisition_plan_commit_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_plan_commit_command_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=ACQUISITION_PLAN_COMMIT_OWNER,
            command_type=ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
            limit=limit,
        )
        results = [self._run_acquisition_plan_commit_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_plan_commit_commands_drained"
            if results
            else "no_ready_acquisition_plan_commit_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11b_acquisition_plan_commit",
            "items": results,
        }

    def _sync_acquisition_run_phase_from_workflow_command(
        self,
        command: dict[str, Any] | None,
        *,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        latest_command = self.store.get_workflow_command(command_id) if command_id else {}
        if latest_command:
            command_payload = latest_command
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        if not operation_run_id:
            operation_run_id = str(dict(command_payload.get("payload") or {}).get("operation_id") or "").strip()
        if not operation_run_id:
            return {"status": "skipped", "reason": "operation_id_missing"}
        operation_run = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {
                "status": "skipped",
                "reason": "operation_run_not_found",
                "operation_run_id": operation_run_id,
            }
        command_status = str(command_payload.get("status") or "").strip()
        if command_status != "succeeded":
            return {
                "status": "skipped",
                "reason": "workflow_command_not_acquisition_run_phase_terminal",
                "command_status": command_status,
                "operation_run_id": operation_run_id,
            }
        command_result = dict(command_payload.get("result") or {})
        acquisition_run = dict(command_result.get("acquisition_run") or {})
        acquisition_run_id = str(
            command_result.get("acquisition_run_id") or acquisition_run.get("acquisition_run_id") or ""
        ).strip()
        operation_phase = str(command_result.get("operation_phase") or "acquisition_run_phase_advanced").strip()
        workflow_ref = {
            "workflow_run_id": str(command_payload.get("workflow_run_id") or ""),
            "command_id": command_id,
            "command_type": str(command_payload.get("command_type") or ""),
            "owner": str(command_payload.get("owner") or ""),
        }
        operation_patch = self.store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status="running",
            progress_patch={
                "phase": operation_phase,
                "command_status": command_status,
                "command_attempt": int(command_payload.get("attempt") or 0),
                "acquisition_run_id": acquisition_run_id,
                **workflow_ref,
            },
            workflow_ref_patch=workflow_ref,
            result_ref_patch={
                "workflow_command": {**workflow_ref, "status": command_status},
                "workflow_command_result": command_result,
                "acquisition_run": acquisition_run,
            },
            metadata_patch={
                "last_command_terminal_status": command_status,
                "last_command_sync_source": source,
                "awaiting_probe_commands": operation_phase
                in {
                    "acquisition_probe_submitted",
                    "acquisition_probe_collected_pending_scale",
                },
                "awaiting_discovery_commands": operation_phase == "acquisition_scale_planned_pending_discovery",
            },
        )
        action_id = str(operation_patch.get("action_id") or operation_run.get("action_id") or "").strip()
        if action_id:
            self.store.repos.workflow_runtime.update_action_state(
                action_id,
                status="running",
                result_ref_patch={
                    "operation_run_id": operation_run_id,
                    "workflow_command": {**workflow_ref, "status": command_status},
                    "acquisition_run": acquisition_run,
                },
                metadata_patch={
                    "last_operation_command_status": command_status,
                    "awaiting_probe_commands": operation_phase
                    in {
                        "acquisition_probe_submitted",
                        "acquisition_probe_collected_pending_scale",
                    },
                    "awaiting_discovery_commands": operation_phase == "acquisition_scale_planned_pending_discovery",
                },
            )
        event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(
                operation_patch.get("workspace_id") or operation_run.get("workspace_id") or "default"
            ).strip()
            or "default",
            event_stream_id=operation_run_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationAcquisitionRunPhaseAdvanced",
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:OperationAcquisitionRunPhaseAdvanced:{command_id}:"
                f"{int(command_payload.get('attempt') or 0)}"
            ),
            actor=actor,
            source=source,
            payload={
                **workflow_ref,
                "command_status": command_status,
                "acquisition_run_id": acquisition_run_id,
                "operation_phase": operation_phase,
                "module_state_mutated": True,
            },
        )
        return self._kernel._workflow_command_operation_sync_api_record(
            {
                "status": "running",
                "operation_run": operation_patch,
                "event": event,
                "workflow_command": self._kernel._workflow_command_api_record(command_payload),
            }
        )

    def _execute_acquisition_probe_submit_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        acquisition_run_id = str(payload.get("acquisition_run_id") or "").strip()
        acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id)
        if not acquisition_run:
            return {
                "status": "invalid",
                "reason": "acquisition_probe_submit_run_missing",
                "operation_completion_deferred": False,
            }
        updated_run = self._upsert_acquisition_run_phase(
            acquisition_run=acquisition_run,
            command=command_payload,
            status="probe_submitted",
            current_phase="probe_submitted",
            metadata_patch={
                "probe_submit_owner": ACQUISITION_PROBE_OWNER,
                "probe_submit_completed_by": str(lease_owner or "").strip(),
            },
        )
        downstream_command = self._plan_acquisition_probe_collect_command(
            parent_command=command_payload,
            acquisition_run=updated_run,
        )
        if not downstream_command:
            return {
                "status": "invalid",
                "reason": "acquisition_probe_collect_command_enqueue_failed",
                "operation_completion_deferred": False,
            }
        return {
            "status": "probe_submitted",
            "reason": "acquisition_probe_submitted",
            "operation_completion_deferred": True,
            "operation_phase": "acquisition_probe_submitted",
            "module_state_mutated": True,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "model_called": False,
            "legacy_job_shell_created": False,
            "acquisition_run": updated_run,
            "acquisition_run_id": updated_run["acquisition_run_id"],
            "downstream_command_required": True,
            "downstream_command_count": 1,
            "downstream_command_ids": [str(downstream_command.get("command_id") or "").strip()],
            "downstream_commands": [
                self._kernel._workflow_command_observation(
                    downstream_command,
                    migration_phase="W11c_acquisition_probe_collect",
                )
            ],
            "downstream_command_types": [ACQUISITION_PROBE_COLLECT_COMMAND_TYPE],
            "next_phase": "W11c_acquisition_probe_collect",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11c_acquisition_probe_submit",
            "contract": "w11c_acquisition_probe_submit_owner_v1",
        }

    def _execute_acquisition_probe_collect_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        acquisition_run_id = str(payload.get("acquisition_run_id") or "").strip()
        acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id)
        if not acquisition_run:
            return {
                "status": "invalid",
                "reason": "acquisition_probe_collect_run_missing",
                "operation_completion_deferred": False,
            }
        probe_result = {
            "probe_result_id": "probe_" + hashlib.sha1(acquisition_run_id.encode("utf-8")).hexdigest()[:24],
            "mode": "operation_native_probe_contract",
            "provider_called": False,
            "candidate_count": 0,
            "requires_provider_backed_discovery_owner": True,
            "source_command_id": str(command_payload.get("command_id") or "").strip(),
        }
        updated_run = self._upsert_acquisition_run_phase(
            acquisition_run=acquisition_run,
            command=command_payload,
            status="probe_collected",
            current_phase="probe_collected",
            metadata_patch={
                "probe_collect_owner": ACQUISITION_PROBE_OWNER,
                "probe_result": probe_result,
                "probe_collect_completed_by": str(lease_owner or "").strip(),
            },
        )
        downstream_command = self._plan_acquisition_scale_plan_command(
            parent_command=command_payload,
            acquisition_run=updated_run,
            probe_result=probe_result,
        )
        if not downstream_command:
            return {
                "status": "invalid",
                "reason": "acquisition_scale_plan_command_enqueue_failed",
                "operation_completion_deferred": False,
            }
        return {
            "status": "probe_collected",
            "reason": "acquisition_probe_collected",
            "operation_completion_deferred": True,
            "operation_phase": "acquisition_probe_collected_pending_scale",
            "module_state_mutated": True,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "model_called": False,
            "legacy_job_shell_created": False,
            "acquisition_run": updated_run,
            "acquisition_run_id": updated_run["acquisition_run_id"],
            "probe_result": probe_result,
            "probe_result_count": 1,
            "downstream_command_required": True,
            "downstream_command_count": 1,
            "downstream_command_ids": [str(downstream_command.get("command_id") or "").strip()],
            "downstream_commands": [
                self._kernel._workflow_command_observation(
                    downstream_command,
                    migration_phase="W11c_acquisition_scale_plan",
                )
            ],
            "downstream_command_types": [ACQUISITION_SCALE_PLAN_COMMAND_TYPE],
            "next_phase": "W11c_acquisition_scale_plan",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11c_acquisition_probe_collect",
            "contract": "w11c_acquisition_probe_collect_owner_v1",
        }

    def _execute_acquisition_scale_plan_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        acquisition_run_id = str(payload.get("acquisition_run_id") or "").strip()
        acquisition_run = self.store.repos.workflow_runtime.get_acquisition_run(acquisition_run_id)
        if not acquisition_run:
            return {
                "status": "invalid",
                "reason": "acquisition_scale_plan_run_missing",
                "operation_completion_deferred": False,
            }
        query_text = str(acquisition_run.get("query") or payload.get("query") or "").strip()
        workflow_run_id = str(
            command_payload.get("workflow_run_id") or acquisition_run.get("workflow_run_id") or ""
        ).strip()
        operation_run_id = str(
            command_payload.get("operation_id") or acquisition_run.get("operation_run_id") or ""
        ).strip()
        source_command_id = str(command_payload.get("command_id") or "").strip()
        target_company = str(acquisition_run.get("target_company") or "").strip()
        activity_idempotency_key = (
            "workflow_activity:"
            + hashlib.sha1(
                f"{workflow_run_id}:{acquisition_run_id}:{source_command_id}:linkedin.discovery_query.run:{query_text}".encode(
                    "utf-8"
                )
            ).hexdigest()[:24]
        )
        activity_run = self.store.repos.workflow_runtime.upsert_activity_run(
            {
                "workspace_id": str(acquisition_run.get("workspace_id") or "default").strip() or "default",
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "acquisition_run_id": acquisition_run_id,
                "command_id": source_command_id,
                "activity_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
                "status": "planned_pending_owner",
                "phase": "discovery_query_planned",
                "idempotency_key": activity_idempotency_key,
                "provider_ref": {},
                "input": {
                    "query": query_text,
                    "target_company": target_company,
                    "acquisition_run_id": acquisition_run_id,
                    "source_command_id": source_command_id,
                },
                "output": {},
                "artifact_refs": [],
                "entity_counts": {
                    "candidate_count": 0,
                    "profile_url_count": 0,
                },
                "metadata": {
                    "attempt_envelope_table": "workflow_activity_attempts",
                    "attempt_count": 0,
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "operation_native_owner_required": True,
                    "normal_path_executes_legacy_discovery_owner": False,
                    "migration_phase": "W11d_activity_boundary",
                },
            }
        )
        activity_run_id = str(activity_run.get("activity_run_id") or "").strip()
        if not activity_run_id:
            return {
                "status": "invalid",
                "reason": "acquisition_scale_plan_activity_boundary_missing",
                "operation_completion_deferred": False,
            }
        lane_plan = {
            "lane_id": "lane_" + hashlib.sha1(f"{acquisition_run_id}:{query_text}".encode("utf-8")).hexdigest()[:16],
            "query": query_text,
            "target_company": target_company,
            "command_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            "owner": LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
            "status": "planned_pending_owner",
            "phase": "discovery_query_planned",
            "activity_run_id": activity_run_id,
            "source_command_id": source_command_id,
            "provider_ref": {},
            "artifact_refs": [],
            "entity_counts": {"candidate_count": 0, "profile_url_count": 0},
            "downstream_command_ids": [],
            "requires_legacy_job_shell": False,
            "normal_path_executes_legacy_discovery_owner": False,
            "operation_native_owner_required": True,
        }
        discovery_lane = self.store.repos.workflow_runtime.upsert_discovery_lane(
            {
                "workspace_id": str(acquisition_run.get("workspace_id") or "default").strip() or "default",
                "acquisition_run_id": acquisition_run_id,
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "source_command_id": source_command_id,
                "activity_run_id": activity_run_id,
                "target_company": target_company,
                "query": query_text,
                "provider": "linkedin",
                "status": "planned_pending_owner",
                "phase": "discovery_query_planned",
                "lane_plan": lane_plan,
                "provider_ref": {},
                "artifact_refs": [],
                "entity_counts": {"candidate_count": 0, "profile_url_count": 0},
                "downstream_command_ids": [],
                "idempotency_key": f"acquisition_discovery_lane:{lane_plan['lane_id']}",
                "metadata": {
                    "source_command_id": source_command_id,
                    "activity_run_id": activity_run_id,
                    "provider_called": False,
                    "legacy_job_shell_created": False,
                    "operation_native_owner_required": True,
                    "migration_phase": "W11d_activity_boundary",
                },
            }
        )
        if not discovery_lane:
            return {
                "status": "invalid",
                "reason": "acquisition_scale_plan_discovery_lane_missing",
                "operation_completion_deferred": False,
            }
        updated_run = self._upsert_acquisition_run_phase(
            acquisition_run=acquisition_run,
            command=command_payload,
            status="scale_planned_pending_discovery",
            current_phase="discovery_pending",
            metadata_patch={
                "scale_plan_owner": ACQUISITION_SCALE_PLAN_OWNER,
                "discovery_lane_plan": lane_plan,
                "discovery_lane": discovery_lane,
                "workflow_activity_run": activity_run,
                "activity_boundary": "workflow_activity_runs",
                "attempt_boundary": "workflow_activity_attempts",
                "scale_plan_completed_by": str(lease_owner or "").strip(),
            },
        )
        return {
            "status": "scale_planned",
            "reason": "acquisition_scale_planned",
            "operation_completion_deferred": True,
            "operation_phase": "acquisition_scale_planned_pending_discovery",
            "module_state_mutated": True,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "provider_called": False,
            "model_called": False,
            "legacy_job_shell_created": False,
            "acquisition_run": updated_run,
            "acquisition_run_id": updated_run["acquisition_run_id"],
            "discovery_lane_plan": lane_plan,
            "discovery_lane": discovery_lane,
            "workflow_activity_run": activity_run,
            "lane_count": 1,
            "activity_run_count": 1 if activity_run else 0,
            "attempt_envelope_table": "workflow_activity_attempts",
            "downstream_command_required": True,
            "downstream_command_count": 0,
            "downstream_command_ids": [],
            "downstream_command_types": [LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE],
            "next_phase": "W11d_operation_native_discovery_query_run",
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11d_activity_boundary_scale_plan",
            "contract": "w11d_acquisition_scale_plan_activity_boundary_v1",
        }

    def _run_acquisition_run_phase_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        command_type = str(command_payload.get("command_type") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "acquisition_run_phase_command_id_missing"}
        executor_by_type = {
            ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE: self._execute_acquisition_probe_submit_command_payload,
            ACQUISITION_PROBE_COLLECT_COMMAND_TYPE: self._execute_acquisition_probe_collect_command_payload,
            ACQUISITION_SCALE_PLAN_COMMAND_TYPE: self._execute_acquisition_scale_plan_command_payload,
        }
        executor = executor_by_type.get(command_type)
        if executor is None:
            return {"status": "failed", "reason": "unsupported_acquisition_run_phase_command_type"}
        try:
            owner = DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(command_type)
        except KeyError:
            return {"status": "failed", "reason": "unknown_acquisition_run_phase_command_owner"}
        migration_phase_by_type = {
            ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE: "W11c_acquisition_probe_submit",
            ACQUISITION_PROBE_COLLECT_COMMAND_TYPE: "W11c_acquisition_probe_collect",
            ACQUISITION_SCALE_PLAN_COMMAND_TYPE: "W11c_acquisition_scale_plan",
        }
        migration_phase = migration_phase_by_type.get(command_type, "W11c_acquisition_run_phase")
        if str(command_payload.get("status") or "").strip() == "succeeded":
            self._sync_acquisition_run_phase_from_workflow_command(
                command_payload,
                actor=owner,
                source=f"{command_type}.command_owner",
            )
            return {
                "status": "completed",
                "reason": "acquisition_run_phase_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase=migration_phase,
                ),
            }
        lease_owner = f"{owner}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            if str(latest.get("status") or "").strip() == "succeeded":
                self._sync_acquisition_run_phase_from_workflow_command(
                    latest,
                    actor=owner,
                    source=f"{command_type}.command_owner",
                )
                return {
                    "status": "completed",
                    "reason": "acquisition_run_phase_command_already_succeeded",
                    "workflow_command": self._kernel._workflow_command_observation(
                        latest,
                        migration_phase=migration_phase,
                    ),
                }
            return {
                "status": "queued",
                "reason": "acquisition_run_phase_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase=migration_phase,
                ),
            }
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner) or claimed
        result = executor(running, lease_owner=lease_owner)
        result_status = str(result.get("status") or "").strip()
        if result_status not in {"probe_submitted", "probe_collected", "scale_planned"}:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=str(result.get("reason") or "acquisition_run_phase_invalid"),
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or running,
                actor=owner,
                source=f"{command_type}.command_owner",
            )
            return {
                "status": "failed",
                "reason": str(result.get("reason") or "acquisition_run_phase_invalid"),
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or running,
                    migration_phase=migration_phase,
                ),
            }
        succeeded = self.store.mark_workflow_command_succeeded(command_id, result=result)
        self._sync_acquisition_run_phase_from_workflow_command(
            succeeded or running,
            actor=owner,
            source=f"{command_type}.command_owner",
        )
        return {
            "status": "completed",
            "reason": str(result.get("reason") or "acquisition_run_phase_completed"),
            "result": result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or running,
                migration_phase=migration_phase,
            ),
        }

    def _drain_acquisition_probe_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_probe_command_limit"), 10))
        ready_commands: list[dict[str, Any]] = []
        for command_type in (ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE, ACQUISITION_PROBE_COLLECT_COMMAND_TYPE):
            remaining = max(0, limit - len(ready_commands))
            if remaining <= 0:
                break
            ready_commands.extend(
                self.store.list_ready_workflow_commands(
                    workflow_run_id=workflow_run_id,
                    owner=ACQUISITION_PROBE_OWNER,
                    command_type=command_type,
                    limit=remaining,
                )
            )
        results = [self._run_acquisition_run_phase_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_probe_commands_drained" if results else "no_ready_acquisition_probe_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11c_acquisition_probe",
            "items": results,
        }

    def _drain_acquisition_scale_plan_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_scale_plan_command_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=ACQUISITION_SCALE_PLAN_OWNER,
            command_type=ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
            limit=limit,
        )
        results = [self._run_acquisition_run_phase_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_scale_plan_commands_drained"
            if results
            else "no_ready_acquisition_scale_plan_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11c_acquisition_scale_plan",
            "items": results,
        }

    def _execute_acquisition_run_create_command_payload(
        self,
        command: dict[str, Any],
        *,
        lease_owner: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        workflow_payload = dict(payload.get("workflow_payload") or {})
        target_company = str(payload.get("target_company") or workflow_payload.get("target_company") or "").strip()
        query_text = str(
            payload.get("query") or workflow_payload.get("query") or workflow_payload.get("raw_user_request") or ""
        ).strip()
        plan_review_id = str(payload.get("plan_review_id") or workflow_payload.get("plan_review_id") or "").strip()
        if not plan_review_id and not target_company and not query_text:
            return {
                "status": "invalid",
                "reason": "acquisition_run_create_payload_missing_scope",
                "operation_completion_deferred": False,
            }
        downstream_command = self._plan_acquisition_intent_resolve_command(
            parent_command=command_payload,
            workflow_payload=workflow_payload,
            target_company=target_company,
            query_text=query_text,
            plan_review_id=plan_review_id,
        )
        if not downstream_command:
            return {
                "status": "invalid",
                "reason": "acquisition_intent_resolve_command_enqueue_failed",
                "operation_completion_deferred": False,
            }
        downstream_types = self._acquisition_decomposition_downstream_command_types()
        return {
            "status": "ready_for_downstream_commands",
            "reason": "acquisition_run_root_recorded",
            "operation_completion_deferred": True,
            "module_state_mutated": False,
            "normal_path_executes_queue_workflow_inline": False,
            "queue_workflow_called": False,
            "target_company": target_company,
            "query": query_text,
            "plan_review_id": plan_review_id,
            "workflow_payload": workflow_payload,
            "downstream_command_required": True,
            "downstream_command_count": 1,
            "downstream_command_ids": [str(downstream_command.get("command_id") or "").strip()],
            "downstream_commands": [
                self._kernel._workflow_command_observation(
                    downstream_command,
                    migration_phase="W11b_acquisition_intent_resolve",
                )
            ],
            "downstream_command_types": downstream_types,
            "next_phase": "W11b_acquisition_intent_plan_commands",
            "created_or_reused_job_id": "",
            "legacy_job_shell_created": False,
            "completed_by": str(lease_owner or "").strip(),
            "migration_phase": "W11a_acquisition_run_create_root",
            "contract": "w11a_acquisition_run_create_root_owner_v1",
        }

    def _run_acquisition_run_create_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        if not command_id:
            return {"status": "failed", "reason": "acquisition_run_create_command_id_missing"}
        latest_initial = self.store.get_workflow_command(command_id) or command_payload
        command_payload = latest_initial
        if str(command_payload.get("status") or "").strip() == "succeeded":
            terminal_preflight = self._preflight_acquisition_root_operation_action_command(command_payload)
            if str(terminal_preflight.get("status") or "") != "ready":
                return self._acquisition_root_preflight_failure(
                    command_payload,
                    reason=str(terminal_preflight.get("reason") or "acquisition_root_command_target_conflict").strip(),
                    mark_failed=False,
                )
            self._kernel._sync_operation_run_from_workflow_command(
                command_payload,
                actor=ACQUISITION_RUN_CREATE_OWNER,
                source="acquisition_run_create.command_owner",
            )
            return {
                "status": "completed",
                "reason": "acquisition_run_create_command_already_succeeded",
                "workflow_command": self._kernel._workflow_command_observation(
                    command_payload,
                    migration_phase="W11a_acquisition_run_create_root",
                ),
            }
        lease_owner = f"{ACQUISITION_RUN_CREATE_OWNER}-{uuid.uuid4().hex[:8]}"
        claimed = self.store.claim_workflow_command(command_id, lease_owner=lease_owner, lease_seconds=300)
        if not claimed:
            latest = self.store.get_workflow_command(command_id) or command_payload
            if str(latest.get("status") or "").strip() == "succeeded":
                latest_preflight = self._preflight_acquisition_root_operation_action_command(latest)
                if str(latest_preflight.get("status") or "") != "ready":
                    return self._acquisition_root_preflight_failure(
                        latest,
                        reason=str(
                            latest_preflight.get("reason") or "acquisition_root_command_target_conflict"
                        ).strip(),
                        mark_failed=False,
                    )
                self._kernel._sync_operation_run_from_workflow_command(
                    latest,
                    actor=ACQUISITION_RUN_CREATE_OWNER,
                    source="acquisition_run_create.command_owner",
                )
                return {
                    "status": "completed",
                    "reason": "acquisition_run_create_command_already_succeeded",
                    "workflow_command": self._kernel._workflow_command_observation(
                        latest,
                        migration_phase="W11a_acquisition_run_create_root",
                    ),
                }
            return {
                "status": "queued",
                "reason": "acquisition_run_create_command_not_claimed",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11a_acquisition_run_create_root",
                ),
            }
        running = self.store.mark_workflow_command_running(command_id, lease_owner=lease_owner)
        if not running:
            latest = self.store.get_workflow_command(command_id) or claimed
            return {
                "status": "queued",
                "reason": "acquisition_run_create_command_running_transition_not_applied",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11a_acquisition_run_create_root",
                ),
            }
        latest = self.store.get_workflow_command(command_id) or running
        if (
            str(latest.get("status") or "").strip() != "running"
            or str(latest.get("lease_owner") or "").strip() != lease_owner
        ):
            return {
                "status": "queued",
                "reason": "acquisition_run_create_command_live_lease_lost",
                "workflow_command": self._kernel._workflow_command_observation(
                    latest,
                    migration_phase="W11a_acquisition_run_create_root",
                ),
            }
        execution_preflight = self._preflight_acquisition_root_operation_action_command(latest)
        if str(execution_preflight.get("status") or "") != "ready":
            return self._acquisition_root_preflight_failure(
                latest,
                reason=str(execution_preflight.get("reason") or "acquisition_root_command_target_conflict").strip(),
                mark_failed=True,
            )
        result = self._execute_acquisition_run_create_command_payload(latest, lease_owner=lease_owner)
        result_status = str(result.get("status") or "").strip()
        if result_status not in {"ready_for_downstream_commands"}:
            failed = self.store.mark_workflow_command_failed(
                command_id,
                error_text=str(result.get("reason") or "acquisition_run_create_invalid"),
                retryable=False,
            )
            self._kernel._sync_operation_run_from_workflow_command(
                failed or latest,
                actor=ACQUISITION_RUN_CREATE_OWNER,
                source="acquisition_run_create.command_owner",
            )
            return {
                "status": "failed",
                "reason": str(result.get("reason") or "acquisition_run_create_invalid"),
                "workflow_command": self._kernel._workflow_command_observation(
                    failed or latest,
                    migration_phase="W11a_acquisition_run_create_root",
                ),
            }
        succeeded = self.store.mark_workflow_command_succeeded(command_id, result=result)
        self._kernel._sync_operation_run_from_workflow_command(
            succeeded or latest,
            actor=ACQUISITION_RUN_CREATE_OWNER,
            source="acquisition_run_create.command_owner",
        )
        return {
            "status": "completed",
            "reason": "acquisition_run_create_root_recorded",
            "result": result,
            "workflow_command": self._kernel._workflow_command_observation(
                succeeded or latest,
                migration_phase="W11a_acquisition_run_create_root",
            ),
        }

    def _drain_acquisition_run_create_commands(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        normalized = dict(payload or {})
        workflow_run_id = str(normalized.get("workflow_run_id") or "").strip()
        limit = max(1, _coerce_int(normalized.get("acquisition_run_create_command_limit"), 10))
        ready_commands = self.store.list_ready_workflow_commands(
            workflow_run_id=workflow_run_id,
            owner=ACQUISITION_RUN_CREATE_OWNER,
            command_type=ACQUISITION_RUN_CREATE_COMMAND_TYPE,
            limit=limit,
        )
        results = [self._run_acquisition_run_create_command(command) for command in ready_commands]
        completed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "completed")
        failed_count = sum(1 for result in results if str(dict(result).get("status") or "") == "failed")
        queued_count = sum(1 for result in results if str(dict(result).get("status") or "") == "queued")
        return {
            "status": "completed" if results else "idle",
            "reason": "acquisition_run_create_commands_drained"
            if results
            else "no_ready_acquisition_run_create_commands",
            "command_count": len(ready_commands),
            "executed_command_count": len(results),
            "completed_count": completed_count,
            "failed_count": failed_count,
            "queued_count": queued_count,
            "legacy_bridge_used": False,
            "migration_phase": "W11a_acquisition_run_create_root",
            "items": results,
        }
