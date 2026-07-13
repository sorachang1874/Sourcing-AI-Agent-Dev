"""Command-execution kernel extracted from ``SourcingOrchestrator`` (Phase 1).

Store-only workflow-command helpers: every method here depends only on the
control-plane store (``self._store``), pure module helpers, and imported pure
functions/constants — no locks, no file IO, no cross-domain orchestrator
methods.  Bodies are moved verbatim from ``orchestrator.py``; the only edit is
``self.store`` -> ``self._store``.  ``SourcingOrchestrator`` keeps one-line
delegating wrappers with identical signatures.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from .durable_runtime import (
    workflow_command_activity_spine_policy,
    workflow_command_control_policy,
    workflow_command_control_state,
    workflow_command_display_contract,
)
from .operation_runtime import (
    DEFAULT_ACTION_REGISTRY,
    WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE,
    WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED,
)


# NOTE: the three helpers below duplicate module-level helpers in
# ``orchestrator.py`` (which imports this module — importing them back from
# orchestrator would create a cycle).  The bodies are copied verbatim; several
# other ``sourcing_agent`` modules already carry the same local copies.
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def _parse_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


class CommandKernel:
    """Store-only execution kernel for workflow-command bookkeeping."""

    def __init__(self, store: Any) -> None:
        self._store = store

    def _workflow_command_observation(
        self,
        command: dict[str, Any] | None,
        *,
        migration_phase: str,
    ) -> dict[str, Any]:
        payload = dict(command or {})
        record = {
            key: payload.get(key)
            for key in (
                "workflow_run_id",
                "operation_id",
                "command_id",
                "command_type",
                "owner",
                "stage_id",
                "causal_group_id",
                "parent_command_id",
                "source_event_id",
                "source_event_type",
                "input_artifact_refs",
                "output_artifact_refs",
                "produced_entity_counts",
                "no_op_reason",
                "readiness_effect",
                "downstream_command_ids",
                "causality_schema_version",
                "idempotency_key",
                "status",
                "attempt",
                "last_error",
            )
            if payload.get(key) not in (None, "", [], {})
        }
        if record:
            record.update(self._workflow_command_agent_exposure_record(str(record.get("command_type") or "")))
            record["migration_phase"] = str(migration_phase or "").strip() or "durable_runtime_command_owner"
            record["normal_path"] = True
        return record

    def _workflow_command_from_apply_result_or_store(
        self,
        *,
        apply_result: Any,
        workflow_run_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        normalized_idempotency_key = str(idempotency_key or "").strip()
        if not normalized_idempotency_key:
            return {}
        for command_payload in list(getattr(apply_result, "commands", ()) or ()):
            command = dict(command_payload or {})
            if str(command.get("idempotency_key") or "").strip() == normalized_idempotency_key:
                return command
        for existing_command in self._store.list_workflow_commands(
            workflow_run_id=str(workflow_run_id or "").strip(),
            limit=0,
        ):
            command = dict(existing_command or {})
            if str(command.get("idempotency_key") or "").strip() == normalized_idempotency_key:
                return command
        return {}

    @staticmethod
    def _command_owned_item_result(
        *,
        status: str,
        reason: str,
        item_id: str = "",
        metadata: dict[str, Any] | None = None,
        serving_projection_id: str = "",
        result_view_id: str = "",
        result_patch_id: str = "",
        last_error: str = "",
    ) -> dict[str, Any]:
        return {
            "item_id": str(item_id or "").strip(),
            "status": str(status or "").strip(),
            "phase": str(status or "").strip(),
            "reason": str(reason or "").strip(),
            "serving_projection_id": str(serving_projection_id or "").strip(),
            "result_view_id": str(result_view_id or "").strip(),
            "result_patch_id": str(result_patch_id or "").strip(),
            "last_error": str(last_error or "").strip(),
            "metadata": dict(metadata or {}),
            "command_owned_payload": True,
        }

    def _start_workflow_command_activity_attempt(
        self,
        command: dict[str, Any],
        *,
        activity_type: str,
        owner: str,
        phase: str,
        lease_owner: str,
        provider: str,
        provider_request_ref: str,
        input_payload: dict[str, Any],
        entity_counts: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        attempt_suffix: str = "",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        command_payload = dict(command or {})
        payload = dict(command_payload.get("payload") or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        operation_run_id = str(command_payload.get("operation_id") or payload.get("operation_run_id") or "").strip()
        if not command_id or not workflow_run_id:
            return {}, {}
        normalized_activity_type = str(activity_type or "").strip()
        normalized_owner = str(owner or "").strip()
        normalized_phase = str(phase or "").strip()
        workspace_id = str(payload.get("workspace_id") or "default").strip() or "default"
        activity = self._store.upsert_workflow_activity_run(
            {
                "workspace_id": workspace_id,
                "workflow_run_id": workflow_run_id,
                "operation_run_id": operation_run_id,
                "command_id": command_id,
                "activity_type": normalized_activity_type,
                "owner": normalized_owner,
                "status": "running",
                "phase": normalized_phase,
                "idempotency_key": f"workflow_activity:{normalized_activity_type}:{command_id}",
                "input": dict(input_payload or {}),
                "output": {},
                "artifact_refs": [],
                "entity_counts": dict(entity_counts or {}),
                "metadata": {
                    **dict(metadata or {}),
                    "lease_owner": str(lease_owner or "").strip(),
                    "workflow_command_id": command_id,
                    "workflow_command_type": str(command_payload.get("command_type") or "").strip(),
                    "workflow_command_owner": str(command_payload.get("owner") or "").strip(),
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                },
            }
        )
        activity_run_id = str(activity.get("activity_run_id") or "").strip()
        attempt_number = max(1, _coerce_int(command_payload.get("attempt"), 1))
        normalized_suffix = str(attempt_suffix or normalized_phase or "attempt").strip()
        attempt_key = hashlib.sha1(
            f"{normalized_activity_type}:{normalized_suffix}:{attempt_number}".encode("utf-8")
        ).hexdigest()[:24]
        attempt = self._store.upsert_workflow_activity_attempt(
            {
                "workspace_id": workspace_id,
                "activity_run_id": activity_run_id,
                "workflow_run_id": workflow_run_id,
                "command_id": command_id,
                "attempt_number": attempt_number,
                "status": "running",
                "provider": str(provider or normalized_owner).strip(),
                "provider_request_ref": str(provider_request_ref or f"{normalized_activity_type}:{command_id}").strip(),
                "started_at": _utc_now_iso(),
                "input": dict(input_payload or {}),
                "output": {},
                "artifact_refs": [],
                "error": {},
                "idempotency_key": f"workflow_activity_attempt:{command_id}:{attempt_key}",
                "metadata": {
                    "lease_owner": str(lease_owner or "").strip(),
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                },
            }
        )
        return activity, attempt

    def _finish_workflow_command_activity_attempt(
        self,
        *,
        activity: dict[str, Any],
        attempt: dict[str, Any],
        status: str,
        phase: str,
        output: dict[str, Any],
        entity_counts: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        artifact_refs: list[str] | tuple[str, ...] | None = None,
        attempt_status: str = "",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not activity or not attempt:
            return {}, {}
        normalized_artifact_refs = [
            str(ref or "").strip()
            for ref in list(artifact_refs or [])
            if str(ref or "").strip()
        ]
        normalized_status = str(status or "").strip() or "succeeded"
        normalized_attempt_status = str(attempt_status or normalized_status).strip() or normalized_status
        completed_at = (
            _utc_now_iso()
            if normalized_attempt_status in {"succeeded", "failed", "retry_wait", "cancelled"}
            else ""
        )
        final_attempt = self._store.upsert_workflow_activity_attempt(
            {
                **attempt,
                "status": normalized_attempt_status,
                "completed_at": completed_at,
                "output": dict(output or {}),
                "error": dict(error or {}),
                "artifact_refs": normalized_artifact_refs or list(attempt.get("artifact_refs") or []),
            }
        )
        final_activity = self._store.upsert_workflow_activity_run(
            {
                **activity,
                "status": normalized_status,
                "phase": str(phase or normalized_status).strip(),
                "output": {
                    **dict(activity.get("output") or {}),
                    **dict(output or {}),
                    "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                },
                "entity_counts": {
                    **dict(activity.get("entity_counts") or {}),
                    **dict(entity_counts or {}),
                },
                "artifact_refs": normalized_artifact_refs or list(activity.get("artifact_refs") or []),
                "metadata": {
                    **dict(activity.get("metadata") or {}),
                    **dict(metadata or {}),
                    "latest_attempt_id": str(final_attempt.get("attempt_id") or "").strip(),
                },
            }
        )
        return final_activity, final_attempt

    def _workflow_command_control_policy_record(
        self,
        *,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_control_policy(command_type=command_type, owner=owner).to_record()

    def _workflow_command_control_state_record(
        self,
        *,
        command_status: str,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_control_state(
            command_status=command_status,
            command_type=command_type,
            owner=owner,
        ).to_record()

    def _workflow_command_activity_spine_policy_record(
        self,
        *,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_activity_spine_policy(command_type=command_type, owner=owner).to_record()

    def _workflow_command_display_contract_record(
        self,
        *,
        command_type: str,
        owner: str = "",
    ) -> dict[str, Any]:
        return workflow_command_display_contract(command_type=command_type, owner=owner).to_record()

    def _workflow_command_agent_exposure_record(self, command_type: str) -> dict[str, Any]:
        normalized_type = str(command_type or "").strip()
        action_registry = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
        action_allowlisted_commands = {
            str(allowed_command_type or "").strip()
            for action_record in action_registry.values()
            for allowed_command_type in list(action_record.get("allowed_workflow_command_types") or [])
            if str(allowed_command_type or "").strip()
        }
        return {
            "agent_exposure_gate": WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE,
            "agent_exposure_status": (
                WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED
                if normalized_type in action_allowlisted_commands
                else "not_action_registry_allowlisted"
            ),
        }

    def _workflow_command_api_record(self, command: dict[str, Any]) -> dict[str, Any]:
        record = dict(command or {})
        command_type = str(record.get("command_type") or "").strip()
        owner = str(record.get("owner") or "").strip()
        record.update(self._workflow_command_agent_exposure_record(command_type))
        record["display_contract"] = self._workflow_command_display_contract_record(
            command_type=command_type,
            owner=owner,
        )
        record["control_policy"] = self._workflow_command_control_policy_record(
            command_type=command_type,
            owner=owner,
        )
        record["control_state"] = self._workflow_command_control_state_record(
            command_status=str(record.get("status") or "").strip(),
            command_type=command_type,
            owner=owner,
        )
        record["activity_spine_policy"] = self._workflow_command_activity_spine_policy_record(
            command_type=command_type,
            owner=owner,
        )
        return record

    def _workflow_command_control_response_policy_records(
        self,
        command: dict[str, Any],
    ) -> dict[str, Any]:
        command_type = str((command or {}).get("command_type") or "").strip()
        owner = str((command or {}).get("owner") or "").strip()
        command_status = str((command or {}).get("status") or "").strip()
        return {
            "control_policy": self._workflow_command_control_policy_record(
                command_type=command_type,
                owner=owner,
            ),
            "control_state": self._workflow_command_control_state_record(
                command_status=command_status,
                command_type=command_type,
                owner=owner,
            ),
            "display_contract": self._workflow_command_display_contract_record(
                command_type=command_type,
                owner=owner,
            ),
            "activity_spine_policy": self._workflow_command_activity_spine_policy_record(
                command_type=command_type,
                owner=owner,
            ),
        }

    def _workflow_command_lease_active(self, command: dict[str, Any]) -> bool:
        command_payload = dict(command or {})
        lease_owner = str(command_payload.get("lease_owner") or "").strip()
        lease_expires_at = str(command_payload.get("lease_expires_at") or "").strip()
        if not lease_owner or not lease_expires_at:
            return False
        parsed = _parse_timestamp(lease_expires_at)
        return bool(parsed and parsed > datetime.now(timezone.utc))

    def _workflow_command_is_cancelled(self, command_id: str) -> bool:
        normalized_command_id = str(command_id or "").strip()
        if not normalized_command_id:
            return False
        current = self._store.get_workflow_command(normalized_command_id) or {}
        return str(current.get("status") or "").strip().lower() in {"cancelled", "canceled"}

    def _sync_operation_run_from_workflow_command(
        self,
        command: dict[str, Any] | None,
        *,
        actor: str,
        source: str,
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        command_id = str(command_payload.get("command_id") or "").strip()
        latest_command = self._store.get_workflow_command(command_id) if command_id else {}
        if latest_command:
            command_payload = latest_command
        operation_run_id = str(command_payload.get("operation_id") or "").strip()
        if not operation_run_id:
            operation_run_id = str(dict(command_payload.get("payload") or {}).get("operation_id") or "").strip()
        if not operation_run_id:
            return {"status": "skipped", "reason": "operation_id_missing"}
        operation_run = self._store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation_run:
            return {
                "status": "skipped",
                "reason": "operation_run_not_found",
                "operation_run_id": operation_run_id,
            }
        command_status = str(command_payload.get("status") or "").strip()
        if command_status == "succeeded":
            command_result = dict(command_payload.get("result") or {})
            result_operation_phase = str(command_result.get("operation_phase") or "").strip()
            if bool(command_result.get("operation_completion_deferred")):
                next_status = "running"
                event_type = "OperationCommandDownstreamQueued"
                phase = result_operation_phase or "workflow_command_downstream_queued"
                action_status = "running"
            else:
                next_status = "completed"
                event_type = "OperationCommandSucceeded"
                phase = result_operation_phase or "workflow_command_succeeded"
                action_status = "completed"
        elif command_status == "failed_terminal":
            next_status = "failed"
            event_type = "OperationCommandFailed"
            phase = "workflow_command_failed"
            action_status = "failed"
        elif command_status == "retry_wait":
            next_status = "planned"
            event_type = "OperationCommandRetryWaiting"
            phase = "workflow_command_retry_wait"
            action_status = "planned"
        else:
            return {
                "status": "skipped",
                "reason": "workflow_command_not_syncable",
                "command_status": command_status,
                "operation_run_id": operation_run_id,
            }
        workflow_ref = {
            "workflow_run_id": str(command_payload.get("workflow_run_id") or ""),
            "command_id": command_id,
            "command_type": str(command_payload.get("command_type") or ""),
            "owner": str(command_payload.get("owner") or ""),
        }
        command_result = dict(command_payload.get("result") or {})
        operation_patch = self._store.repos.workflow_runtime.update_operation_state(
            operation_run_id,
            status=next_status,
            progress_patch={
                "phase": phase,
                "command_status": command_status,
                "command_attempt": int(command_payload.get("attempt") or 0),
                **workflow_ref,
            },
            workflow_ref_patch=workflow_ref,
            result_ref_patch={
                "workflow_command": {**workflow_ref, "status": command_status},
                "workflow_command_result": command_result,
                "workflow_command_last_error": str(command_payload.get("last_error") or "").strip(),
            },
            metadata_patch={
                "last_command_terminal_status": command_status,
                "last_command_sync_source": source,
            },
        )
        action_id = str(operation_patch.get("action_id") or operation_run.get("action_id") or "").strip()
        if action_id:
            self._store.repos.workflow_runtime.update_action_state(
                action_id,
                status=action_status,
                result_ref_patch={
                    "operation_run_id": operation_run_id,
                    "workflow_command": {**workflow_ref, "status": command_status},
                },
                metadata_patch={"last_operation_command_status": command_status},
            )
        event = self._store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(operation_patch.get("workspace_id") or operation_run.get("workspace_id") or "default").strip()
            or "default",
            event_stream_id=operation_run_id,
            operation_run_id=operation_run_id,
            action_id=action_id,
            event_family="operation_event",
            event_type=event_type,
            idempotency_key=(
                f"{operation_run.get('idempotency_key')}:{event_type}:{command_id}:"
                f"{command_status}:{int(command_payload.get('attempt') or 0)}"
            ),
            actor=actor,
            source=source,
            payload={
                **workflow_ref,
                "command_status": command_status,
                "command_result": command_result,
                "last_error": str(command_payload.get("last_error") or "").strip(),
                "module_state_mutated": False,
            },
        )
        return {
            "status": next_status,
            "operation_run": operation_patch,
            "event": event,
            "workflow_command": command_payload,
        }

    def _record_command_activity_entity_delta(
        self,
        *,
        command: dict[str, Any],
        activity: dict[str, Any],
        attempt: dict[str, Any],
        entity_type: str,
        entity_key: str,
        delta_kind: str,
        status: str,
        reason: str,
        source_ref: dict[str, Any] | None = None,
        entity_payload: dict[str, Any] | None = None,
        projection_effect: dict[str, Any] | None = None,
        artifact_refs: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_scope: str = "",
    ) -> dict[str, Any]:
        command_payload = dict(command or {})
        if not activity:
            return {}
        workflow_run_id = str(command_payload.get("workflow_run_id") or "").strip()
        command_id = str(command_payload.get("command_id") or "").strip()
        normalized_entity_type = str(entity_type or "").strip()
        normalized_delta_kind = str(delta_kind or "").strip()
        normalized_entity_key = str(entity_key or "").strip()
        if not workflow_run_id or not command_id or not normalized_entity_type or not normalized_delta_kind:
            return {}
        return self._store.upsert_workflow_entity_delta(
            {
                "workspace_id": str(dict(command_payload.get("payload") or {}).get("workspace_id") or "default").strip()
                or "default",
                "workflow_run_id": workflow_run_id,
                "operation_run_id": str(command_payload.get("operation_id") or "").strip(),
                "command_id": command_id,
                "activity_run_id": str(activity.get("activity_run_id") or "").strip(),
                "attempt_id": str(attempt.get("attempt_id") or "").strip(),
                "entity_type": normalized_entity_type,
                "entity_key": normalized_entity_key,
                "delta_kind": normalized_delta_kind,
                "status": str(status or "recorded").strip() or "recorded",
                "reason": str(reason or "").strip(),
                "source_ref": dict(source_ref or {}),
                "entity_payload": dict(entity_payload or {}),
                "projection_effect": dict(projection_effect or {"entered_projection": False}),
                "artifact_refs": [
                    str(ref or "").strip()
                    for ref in list(artifact_refs or [])
                    if str(ref or "").strip()
                ],
                "metadata": {
                    **dict(metadata or {}),
                    "activity_spine_contract": "command_activity_attempt_entity_delta_v1",
                },
                "idempotency_key": (
                    f"workflow_command_entity_delta:{idempotency_scope or normalized_delta_kind}:"
                    f"{command_id}:{normalized_entity_type}:{normalized_entity_key}"
                ),
            }
        )

    def _append_completed_workflow_reconcile_event(
        self,
        job_id: str,
        *,
        phase: str,
        reconcile_kind: str,
        status: str = "running",
        detail: str = "",
        snapshot_id: str = "",
        workers: list[dict[str, Any]] | None = None,
        worker_ids: list[int] | None = None,
        payload: dict[str, Any] | None = None,
        materialize_call: bool = False,
        materialize_signature: str = "",
        marker_backfill_count: int = 0,
        lease_acquired: bool | None = None,
        skip_reason: str = "",
    ) -> dict[str, Any]:
        normalized_job_id = str(job_id or "").strip()
        if not normalized_job_id:
            return {}
        resolved_worker_ids: list[int] = []
        seen_worker_ids: set[int] = set()
        for value in list(worker_ids or []):
            try:
                worker_id = int(value or 0)
            except (TypeError, ValueError):
                continue
            if worker_id > 0 and worker_id not in seen_worker_ids:
                seen_worker_ids.add(worker_id)
                resolved_worker_ids.append(worker_id)
        for worker in list(workers or []):
            try:
                worker_id = int(dict(worker or {}).get("worker_id") or 0)
            except (TypeError, ValueError):
                continue
            if worker_id > 0 and worker_id not in seen_worker_ids:
                seen_worker_ids.add(worker_id)
                resolved_worker_ids.append(worker_id)
        extra_payload = dict(payload or {})
        sync_result = dict(extra_payload.get("sync_result") or {})
        resolved_snapshot_id = str(snapshot_id or extra_payload.get("snapshot_id") or "").strip()
        resolved_reconcile_kind = str(reconcile_kind or extra_payload.get("reconcile_kind") or "unknown").strip()
        resolved_phase = str(phase or extra_payload.get("phase") or "unknown").strip()
        resolved_materialize_signature = str(materialize_signature or "").strip()
        if materialize_call and not resolved_materialize_signature:
            materialize_reason = str(sync_result.get("reason") or extra_payload.get("reason") or resolved_phase).strip()
            materialize_worker_ids = ",".join(str(item) for item in sorted(resolved_worker_ids))
            resolved_materialize_signature = "|".join(
                item
                for item in (
                    resolved_snapshot_id,
                    resolved_reconcile_kind,
                    materialize_worker_ids,
                    materialize_reason,
                )
                if item
            )
        event_payload = dict(extra_payload)
        event_payload.update(
            {
                "event_family": "completed_workflow_reconcile",
                "phase": resolved_phase,
                "reconcile_kind": resolved_reconcile_kind,
                "snapshot_id": resolved_snapshot_id,
                "worker_ids": resolved_worker_ids,
                "worker_count": len(resolved_worker_ids),
                "materialize_call": bool(materialize_call),
                "materialize_signature": resolved_materialize_signature,
                "marker_backfill_count": max(0, int(marker_backfill_count or 0)),
            }
        )
        if lease_acquired is not None:
            event_payload["lease_acquired"] = bool(lease_acquired)
        if skip_reason:
            event_payload["skip_reason"] = str(skip_reason or "").strip()
        if sync_result:
            event_payload["sync_status"] = str(sync_result.get("status") or "").strip()
            event_payload["sync_reason"] = str(sync_result.get("reason") or "").strip()
            event_payload["sync_result"] = {key: value for key, value in sync_result.items() if key != "state_updates"}
        resolved_detail = str(detail or "").strip()
        if not resolved_detail:
            resolved_detail = f"Completed workflow reconcile {resolved_phase}."
        self._store.append_job_event(
            normalized_job_id,
            "completed",
            str(status or "running").strip() or "running",
            resolved_detail,
            payload=event_payload,
        )
        return event_payload
