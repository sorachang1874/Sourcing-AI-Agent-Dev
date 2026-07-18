"""Result-preparation and acceptance UoWs for acquisition-start v2.

This module is the bounded S1e2c bridge between the exact start-create owner
bundle and the generic Agent result-slot state machine.  It performs no live
provider/model I/O and creates no runtime-outbox row.  The only non-result-slot
mutation is the start-specific release of the dormant root WorkflowCommand after
the exact command-acceptance result is accepted.
"""

from __future__ import annotations

import time
from typing import Any

from .acquisition_start_v2 import (
    AcquisitionConfirmationReceipt,
    acquisition_start_v2_success_result,
    serialize_acquisition_start_v2_result,
)
from .acquisition_start_v2_create_postgres import (
    _ACTION_JSON_FIELDS,
    _COMMAND_JSON_FIELDS,
    _CURRENT_STATE_JSON_FIELDS,
    _OPERATION_JSON_FIELDS,
    _RESULT_ACCEPTANCE_HOLD_UNTIL,
    _WINNER_OWNER_REF_SCHEMA_VERSION,
    _assert_event_set_exact,
    _assert_row_exact,
    _canonical_create_rows,
    _workflow_run_id,
)
from .acquisition_start_v2_postgres import (
    _bind_locked_preview,
    _canonical_submit_rows,
    _utc_second_iso,
    revalidate_acquisition_start_v2_occurrence,
)
from .agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from .operation_runtime import operation_run_id_for

ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND = "acquisition_start_command_acceptance_v1"

_RESULT_REQUIRED_TABLES = (
    "operation_events",
    "operation_runs",
    "agent_actions",
    "agent_tool_result_slots",
    "acquisition_plan_previews",
    "workflow_events",
    "workflow_commands",
    "workflow_current_state",
    "agent_tool_result_attempts",
    "agent_tool_result_journal",
)


def _read_dependencies(adapter: Any, table_names: tuple[str, ...]) -> bool:
    return all(adapter.should_prefer_read(table_name) for table_name in table_names)


def _runtime_dependencies(adapter: Any, table_names: tuple[str, ...]) -> bool:
    authoritative = getattr(adapter, "is_authoritative", None)
    if not callable(authoritative):
        return False
    for table_name in table_names:
        if not adapter.should_prefer_read(table_name) or not authoritative(table_name):
            return False
        adapter._ensure_table_write_schema(table_name)
    return True


def _deadline_seconds(lock_timeout_seconds: float) -> tuple[float, float]:
    import math

    if isinstance(lock_timeout_seconds, bool):
        raise ValueError("acquisition start result lock timeout must be finite and positive")
    timeout_seconds = float(lock_timeout_seconds)
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("acquisition start result lock timeout must be finite and positive")
    return timeout_seconds, time.monotonic() + timeout_seconds


def _refresh_transaction_deadline(cursor: Any, *, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise RuntimeError("acquisition start result deadline exhausted")
    milliseconds = max(1, int(remaining * 1000))
    cursor.execute(
        "SELECT set_config('lock_timeout', %s, true), set_config('statement_timeout', %s, true)",
        (f"{milliseconds}ms", f"{milliseconds}ms"),
    )


def _with_retry_dependencies() -> tuple[Any, Any, Any, int, float]:
    from .control_plane_live_postgres import (
        _CONTROL_PLANE_POSTGRES_MAX_RETRIES,
        _SESSION_ADVISORY_LOCK_POLL_SECONDS,
        ControlPlaneAdvisoryLockBusy,
        _control_plane_postgres_retry_delay_seconds,
        _is_retryable_postgres_exception,
    )

    return (
        ControlPlaneAdvisoryLockBusy,
        _is_retryable_postgres_exception,
        _control_plane_postgres_retry_delay_seconds,
        _CONTROL_PLANE_POSTGRES_MAX_RETRIES,
        _SESSION_ADVISORY_LOCK_POLL_SECONDS,
    )


def start_acquisition_result_lock_groups(
    *,
    occurrence: AgentToolOccurrence,
    operation_run_id: str,
    workflow_run_id: str,
    workflow_command_id: str,
    command_key: str,
) -> tuple[tuple[str, ...], ...]:
    binding = revalidate_acquisition_start_v2_occurrence(occurrence)

    def _sorted(*values: str) -> tuple[str, ...]:
        return tuple(sorted(set(values), key=lambda value: value.encode("utf-8")))

    return (
        _sorted(
            f"operation_runs:id:{operation_run_id}",
            f"operation_runs:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        ),
        _sorted(
            f"agent_actions:id:{binding.action_id}",
            f"agent_actions:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        ),
        _sorted(
            f"agent_tool_result_slots:id:{occurrence.result_slot_id}",
            f"agent_tool_result_slots:occurrence:{occurrence.logical_occurrence_digest}",
        ),
        _sorted(
            f"acquisition_plan_previews:id:{binding.preview_id}",
            f"acquisition_plan_previews:revision:{binding.preview_revision}",
        ),
        _sorted(
            f"workflow_commands:id:{workflow_command_id}",
            f"workflow_commands:idempotency:{workflow_run_id}:{command_key}",
        ),
        _sorted(f"workflow_current_state:{workflow_run_id}"),
        _sorted(
            f"operation_events:{binding.action_id}",
            f"operation_events:{operation_run_id}",
            f"workflow_events:{workflow_run_id}",
        ),
    )


def _json_payload(row: dict[str, Any], field: str) -> dict[str, Any]:
    from .control_plane_live_postgres import _json_load_dict

    value = _json_load_dict(row.get(field))
    if not isinstance(value, dict):
        raise ValueError(f"acquisition start result {field} invalid")
    return value


def _load_one(cursor: Any, sql: str, params: tuple[Any, ...]) -> dict[str, Any]:
    from .control_plane_live_postgres import _fetch_one_dict_row

    cursor.execute(sql, params)
    return _fetch_one_dict_row(cursor, cursor.fetchone()) or {}


def _discover_start_result_lock_identity(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
) -> tuple[str, str, str, str]:
    binding = revalidate_acquisition_start_v2_occurrence(occurrence)
    operation_run_id = operation_run_id_for(
        action_id=binding.action_id,
        operation_type="acquisition_run",
        idempotency_key=binding.start_idempotency,
    )
    workflow_run_id = _workflow_run_id(operation_run_id)
    action = _load_one(cursor, "SELECT * FROM agent_actions WHERE action_id = %s", (binding.action_id,))
    receipt_event = _load_one(
        cursor,
        "SELECT * FROM operation_events WHERE event_stream_id = %s AND sequence_number = 2",
        (binding.action_id,),
    )
    if not action or not receipt_event:
        raise ValueError("acquisition start result exact owner not found")
    receipt = AcquisitionConfirmationReceipt(_json_payload(receipt_event, "payload_json"))
    submitted_at = _utc_second_iso(action.get("created_at"))
    submitted_action, _approval_required_event = _canonical_submit_rows(binding, submitted_at=submitted_at)
    expected = _canonical_create_rows(
        binding=binding,
        receipt=receipt,
        submitted_action=submitted_action,
    )
    return operation_run_id, workflow_run_id, str(expected["workflow_command_id"]), str(expected["command_key"])


def _command_key_from_terminal_owner_ref(terminal: AgentToolTerminalResult) -> str:
    receipt_ref = terminal.owner_result_ref.get("confirmation_receipt_ref")
    receipt_digest = ""
    if isinstance(receipt_ref, dict):
        receipt_digest = str(receipt_ref.get("receipt_digest") or "")
    if not receipt_digest:
        raise ValueError("acquisition start result terminal receipt digest missing")
    return f"acquisition.run.create:start-v2:{receipt_digest}"


def load_start_acquisition_result_base_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult | None = None,
) -> dict[str, Any]:
    binding = revalidate_acquisition_start_v2_occurrence(occurrence)
    operation_run_id = operation_run_id_for(
        action_id=binding.action_id,
        operation_type="acquisition_run",
        idempotency_key=binding.start_idempotency,
    )
    workflow_run_id = _workflow_run_id(operation_run_id)

    action = _load_one(cursor, "SELECT * FROM agent_actions WHERE action_id = %s FOR UPDATE", (binding.action_id,))
    cursor.execute(
        "SELECT * FROM acquisition_plan_previews WHERE preview_id = %s OR "
        "(workspace_id = %s AND requester_id = %s AND preview_revision = %s AND preview_digest = %s) "
        "ORDER BY preview_id FOR UPDATE",
        (
            binding.preview_id,
            binding.occurrence.workspace_id,
            binding.occurrence.actor_id,
            binding.preview_revision,
            binding.preview_digest,
        ),
    )
    from .control_plane_live_postgres import _fetch_all_dict_rows

    preview_candidates = _fetch_all_dict_rows(cursor)
    if len(preview_candidates) != 1:
        raise ValueError("acquisition start result preview exact owner not found")
    preview = preview_candidates[0]
    cursor.execute(
        "SELECT * FROM agent_tool_result_slots WHERE result_slot_id = %s "
        "OR logical_occurrence_digest = %s ORDER BY result_slot_id FOR UPDATE",
        (binding.occurrence.result_slot_id, binding.occurrence.logical_occurrence_digest),
    )

    slot_candidates = _fetch_all_dict_rows(cursor)
    if len(slot_candidates) != 1:
        raise ValueError("acquisition start result slot exact owner not found")
    slot = slot_candidates[0]
    from .agent_tool_result_postgres import _assert_exact_slot

    _assert_exact_slot(slot, binding.occurrence)
    submitted_at = _utc_second_iso(action.get("created_at"))
    _bind_locked_preview(binding=binding, preview_row=preview, submitted_at=submitted_at)
    operation = _load_one(
        cursor,
        "SELECT * FROM operation_runs WHERE operation_run_id = %s FOR UPDATE",
        (operation_run_id,),
    )
    receipt_event = _load_one(
        cursor,
        "SELECT * FROM operation_events WHERE event_stream_id = %s AND sequence_number = 2 FOR UPDATE",
        (binding.action_id,),
    )
    planned_event = _load_one(
        cursor,
        "SELECT * FROM operation_events WHERE event_stream_id = %s AND sequence_number = 1 FOR UPDATE",
        (operation_run_id,),
    )
    if not all((action, preview, operation, receipt_event, planned_event)):
        raise ValueError("acquisition start result exact owner not found")
    receipt = AcquisitionConfirmationReceipt(_json_payload(receipt_event, "payload_json"))
    submitted_at = _utc_second_iso(action.get("created_at"))
    submitted_action, approval_required_event = _canonical_submit_rows(binding, submitted_at=submitted_at)
    expected = _canonical_create_rows(
        binding=binding,
        receipt=receipt,
        submitted_action=submitted_action,
    )
    workflow_command_id = str(expected["workflow_command_id"])
    if terminal is not None and (
        terminal.action_id != binding.action_id
        or terminal.operation_run_id != operation_run_id
        or terminal.workflow_command_id != workflow_command_id
        or terminal.owner_target_id != workflow_command_id
        or terminal.terminal_winner_id != str(expected["planned_event"]["event_id"])
    ):
        raise ValueError("acquisition start result terminal owner link mismatch")

    workflow_command = _load_one(
        cursor,
        "SELECT * FROM workflow_commands WHERE command_id = %s FOR UPDATE",
        (workflow_command_id,),
    )
    current_state = _load_one(
        cursor,
        "SELECT * FROM workflow_current_state WHERE workflow_run_id = %s FOR UPDATE",
        (workflow_run_id,),
    )
    cursor.execute(
        "SELECT * FROM workflow_events WHERE workflow_run_id = %s AND sequence_number IN (1, 2) "
        "ORDER BY sequence_number FOR UPDATE",
        (workflow_run_id,),
    )
    workflow_events = _fetch_all_dict_rows(cursor)
    if not workflow_command or not current_state or len(workflow_events) != 2:
        raise ValueError("acquisition start result exact workflow owner not found")
    return {
        "binding": binding,
        "expected": expected,
        "action": action,
        "preview": preview,
        "slot": slot,
        "operation_run": operation,
        "receipt_event": receipt_event,
        "workflow_events": workflow_events,
        "workflow_command": workflow_command,
        "workflow_current_state": current_state,
        "planned_event": planned_event,
        "receipt": receipt,
    }


def assert_exact_start_acquisition_result_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, Any],
) -> None:
    _assert_start_owner(base_owner, terminal=terminal, released=None)


def _assert_start_owner(
    base_owner: dict[str, Any],
    *,
    terminal: AgentToolTerminalResult,
    released: bool | None,
) -> None:
    expected = dict(base_owner["expected"])
    expected_command = dict(expected["workflow_command"])
    actual_command = dict(base_owner["workflow_command"])
    actual_hold = str(actual_command.get("not_before_at") or "")
    if released is True and actual_hold:
        raise ValueError("acquisition start result command release missing")
    if released is False and actual_hold != _RESULT_ACCEPTANCE_HOLD_UNTIL:
        raise ValueError("acquisition start result command hold missing")
    if released is None and actual_hold not in {"", _RESULT_ACCEPTANCE_HOLD_UNTIL}:
        raise ValueError("acquisition start result command hold collision")
    expected_command["not_before_at"] = actual_hold
    if actual_hold == "":
        expected_command["updated_at"] = actual_command.get("updated_at")
    _assert_row_exact("Action", base_owner["action"], expected["action"], json_fields=_ACTION_JSON_FIELDS)
    _assert_row_exact(
        "OperationRun",
        base_owner["operation_run"],
        expected["operation_run"],
        json_fields=_OPERATION_JSON_FIELDS,
    )
    _assert_row_exact(
        "WorkflowCommand",
        actual_command,
        expected_command,
        json_fields=_COMMAND_JSON_FIELDS,
    )
    _assert_row_exact(
        "workflow current state",
        base_owner["workflow_current_state"],
        expected["workflow_current_state"],
        json_fields=_CURRENT_STATE_JSON_FIELDS,
    )
    _assert_event_set_exact(
        "operation events",
        [base_owner["receipt_event"], base_owner["planned_event"]],
        [expected["receipt_event"], expected["planned_event"]],
        workflow=False,
    )
    _assert_event_set_exact(
        "workflow events",
        base_owner["workflow_events"],
        expected["workflow_events"],
        workflow=True,
    )
    owner_result_ref = dict(expected["owner_result_ref"])
    if owner_result_ref.get("schema_version") != _WINNER_OWNER_REF_SCHEMA_VERSION:
        raise ValueError("acquisition start result owner-ref schema mismatch")
    if (
        terminal.owner_target_kind != ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND
        or terminal.owner_target_id != str(expected["workflow_command_id"])
        or terminal.owner_target_revision != 1
        or terminal.owner_target_generation != 0
        or terminal.owner_target_revision_token
        or terminal.terminal_winner_id != str(expected["planned_event"]["event_id"])
        or terminal.owner_result_ref != owner_result_ref
        or terminal.owner_result_digest != str(expected["owner_result_digest"])
    ):
        raise ValueError("acquisition start result terminal exact-owner mismatch")
    expected_terminal = _canonical_start_terminal_result_for_attempt(
        terminal=terminal,
        base_owner=base_owner,
    )
    if (
        terminal.serialized_result_json != expected_terminal.serialized_result_json
        or terminal.serialized_result_digest != expected_terminal.serialized_result_digest
        or terminal.tool_result_message_digest != expected_terminal.tool_result_message_digest
        or terminal.is_error
    ):
        raise ValueError("acquisition start result serializer output mismatch")


def _canonical_start_terminal_result_for_attempt(
    *,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, Any],
) -> AgentToolTerminalResult:
    expected = dict(base_owner["expected"])
    binding = base_owner["binding"]
    receipt = base_owner["receipt"]
    serialized = acquisition_start_v2_success_result(
        action_id=binding.action_id,
        operation_run_id=str(expected["operation_run_id"]),
        workflow_command_id=str(expected["workflow_command_id"]),
        snapshot=binding.bound_request.snapshot,
        receipt=receipt,
    )
    serialized = __import__("json").loads(serialize_acquisition_start_v2_result(serialized))
    return AgentToolTerminalResult.from_serialized_result(
        result_attempt_id=terminal.result_attempt_id,
        provider_call_id=terminal.provider_call_id,
        tool_call_id=terminal.tool_call_id,
        action_id=binding.action_id,
        operation_run_id=str(expected["operation_run_id"]),
        workflow_command_id=str(expected["workflow_command_id"]),
        owner_target_kind=ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND,
        owner_target_id=str(expected["workflow_command_id"]),
        owner_target_revision=1,
        owner_target_generation=0,
        terminal_winner_id=str(expected["planned_event"]["event_id"]),
        owner_result_ref=dict(expected["owner_result_ref"]),
        owner_result_digest=str(expected["owner_result_digest"]),
        serialized_result=serialized,
        is_error=False,
    )


def terminal_from_locked_start_acquisition_owner(
    *,
    occurrence: AgentToolOccurrence,
    result_attempt_id: str,
    provider_call_id: str,
    tool_call_id: str,
    base_owner: dict[str, Any],
) -> AgentToolTerminalResult:
    terminal = _canonical_start_terminal_result_for_attempt(
        terminal=AgentToolTerminalResult.from_serialized_result(
            result_attempt_id=result_attempt_id,
            provider_call_id=provider_call_id,
            tool_call_id=tool_call_id,
            action_id=str(base_owner["binding"].action_id),
            operation_run_id=str(base_owner["expected"]["operation_run_id"]),
            workflow_command_id=str(base_owner["expected"]["workflow_command_id"]),
            owner_target_kind=ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND,
            owner_target_id=str(base_owner["expected"]["workflow_command_id"]),
            owner_target_revision=1,
            owner_target_generation=0,
            terminal_winner_id=str(base_owner["expected"]["planned_event"]["event_id"]),
            owner_result_ref=dict(base_owner["expected"]["owner_result_ref"]),
            owner_result_digest=str(base_owner["expected"]["owner_result_digest"]),
            serialized_result={"variant": "success"},
            is_error=False,
        ),
        base_owner=base_owner,
    )
    terminal.validate_for_occurrence(occurrence)
    _assert_start_owner(base_owner, terminal=terminal, released=None)
    return terminal


def prepare_start_acquisition_tool_result(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    result_attempt_id: str,
    provider_call_id: str,
    tool_call_id: str,
    lock_timeout_seconds: float = 5.0,
) -> AgentToolTerminalResult | None:
    if not isinstance(occurrence, AgentToolOccurrence):
        raise ValueError("prepare start result requires exact occurrence")
    occurrence = revalidate_acquisition_start_v2_occurrence(occurrence).occurrence
    if not _read_dependencies(adapter, _RESULT_REQUIRED_TABLES[:-2]):
        return None
    timeout_seconds, deadline = _deadline_seconds(lock_timeout_seconds)
    retry_attempt = 0
    binding = revalidate_acquisition_start_v2_occurrence(occurrence)
    operation_run_id = operation_run_id_for(
        action_id=binding.action_id,
        operation_type="acquisition_run",
        idempotency_key=binding.start_idempotency,
    )
    workflow_run_id = _workflow_run_id(operation_run_id)
    last_busy_key = f"operation_events:{operation_run_id}"
    (
        busy_error,
        is_retryable,
        retry_delay,
        max_retries,
        poll_seconds,
    ) = _with_retry_dependencies()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds)
        connection = adapter._connect_with_timeout(remaining)
        try:
            with connection.cursor() as cursor:
                _refresh_transaction_deadline(cursor, deadline=deadline)
                operation_run_id, workflow_run_id, workflow_command_id, command_key = _discover_start_result_lock_identity(
                    cursor,
                    occurrence=occurrence,
                )
                for lock_group in start_acquisition_result_lock_groups(
                    occurrence=occurrence,
                    operation_run_id=operation_run_id,
                    workflow_run_id=workflow_run_id,
                    workflow_command_id=workflow_command_id,
                    command_key=command_key,
                ):
                    for lock_key in lock_group:
                        _refresh_transaction_deadline(cursor, deadline=deadline)
                        last_busy_key = lock_key
                        if not adapter._try_acquire_transaction_lock(cursor, lock_key):
                            raise RuntimeError("acquisition start result lock acquisition raced")
                _refresh_transaction_deadline(cursor, deadline=deadline)
                base_owner = load_start_acquisition_result_base_owner(
                    cursor,
                    occurrence=occurrence,
                    terminal=None,
                )
                terminal = terminal_from_locked_start_acquisition_owner(
                    occurrence=occurrence,
                    result_attempt_id=result_attempt_id,
                    provider_call_id=provider_call_id,
                    tool_call_id=tool_call_id,
                    base_owner=base_owner,
                )
            connection.commit()
            return terminal
        except RuntimeError as exc:
            if str(exc) != "acquisition start result lock acquisition raced":
                connection.rollback()
                raise
            connection.rollback()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise busy_error(lock_key=last_busy_key, timeout_seconds=timeout_seconds) from exc
            time.sleep(min(poll_seconds, remaining))
        except Exception as exc:
            connection.rollback()
            retry_attempt += 1
            if is_retryable(exc) and retry_attempt < max_retries and deadline > time.monotonic():
                time.sleep(min(retry_delay(retry_attempt), deadline - time.monotonic()))
            else:
                raise
        finally:
            connection.close()


def _release_start_command_hold(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, Any],
) -> dict[str, Any]:
    from .control_plane_live_postgres import _fetch_one_dict_row

    _assert_start_owner(base_owner, terminal=terminal, released=False)
    cursor.execute(
        "UPDATE workflow_commands SET not_before_at = '', updated_at = transaction_timestamp() "
        "WHERE command_id = %s AND status = 'queued' AND not_before_at = %s RETURNING *",
        (terminal.workflow_command_id, _RESULT_ACCEPTANCE_HOLD_UNTIL),
    )
    command = _fetch_one_dict_row(cursor, cursor.fetchone()) or {}
    if not command:
        raise ValueError("acquisition start result command release CAS lost")
    return {"released_workflow_command": command}


def _assert_start_command_released(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, Any],
) -> dict[str, Any]:
    del cursor, occurrence
    expected = dict(base_owner["expected"])
    actual_command = dict(base_owner["workflow_command"])
    if str(actual_command.get("not_before_at") or ""):
        raise ValueError("acquisition start result command release missing")
    owner_result_ref = dict(expected["owner_result_ref"])
    if (
        terminal.owner_target_kind != ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND
        or terminal.owner_target_id != str(expected["workflow_command_id"])
        or terminal.owner_target_revision != 1
        or terminal.owner_target_generation != 0
        or terminal.owner_target_revision_token
        or terminal.terminal_winner_id != str(expected["planned_event"]["event_id"])
        or terminal.owner_result_ref != owner_result_ref
        or terminal.owner_result_digest != str(expected["owner_result_digest"])
    ):
        raise ValueError("acquisition start result accepted replay owner mismatch")
    expected_terminal = _canonical_start_terminal_result_for_attempt(
        terminal=terminal,
        base_owner=base_owner,
    )
    if (
        terminal.serialized_result_json != expected_terminal.serialized_result_json
        or terminal.serialized_result_digest != expected_terminal.serialized_result_digest
        or terminal.tool_result_message_digest != expected_terminal.tool_result_message_digest
        or terminal.is_error
    ):
        raise ValueError("acquisition start result accepted replay serializer mismatch")
    _assert_event_set_exact(
        "operation events",
        [base_owner["receipt_event"], base_owner["planned_event"]],
        [expected["receipt_event"], expected["planned_event"]],
        workflow=False,
    )
    return {"released_workflow_command": dict(base_owner["workflow_command"])}


def _assert_start_late_quarantine_owner(
    cursor: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    base_owner: dict[str, Any],
) -> None:
    del cursor, occurrence
    expected = dict(base_owner["expected"])
    owner_result_ref = dict(expected["owner_result_ref"])
    if (
        terminal.owner_target_kind != ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND
        or terminal.owner_target_id != str(expected["workflow_command_id"])
        or terminal.owner_target_revision != 1
        or terminal.owner_target_generation != 0
        or terminal.owner_target_revision_token
        or terminal.terminal_winner_id != str(expected["planned_event"]["event_id"])
        or terminal.owner_result_ref != owner_result_ref
        or terminal.owner_result_digest != str(expected["owner_result_digest"])
    ):
        raise ValueError("acquisition start result late quarantine owner mismatch")
    expected_terminal = _canonical_start_terminal_result_for_attempt(
        terminal=terminal,
        base_owner=base_owner,
    )
    if (
        terminal.serialized_result_json != expected_terminal.serialized_result_json
        or terminal.serialized_result_digest != expected_terminal.serialized_result_digest
        or terminal.tool_result_message_digest != expected_terminal.tool_result_message_digest
        or terminal.is_error
    ):
        raise ValueError("acquisition start result late quarantine serializer mismatch")
    _assert_event_set_exact(
        "operation events",
        [base_owner["receipt_event"], base_owner["planned_event"]],
        [expected["receipt_event"], expected["planned_event"]],
        workflow=False,
    )


def accept_start_acquisition_tool_result_uow(
    adapter: Any,
    *,
    occurrence: AgentToolOccurrence,
    terminal: AgentToolTerminalResult,
    attempted_slot_generation: int,
    lock_timeout_seconds: float = 5.0,
    fault_injection_point: str = "",
) -> dict[str, Any] | None:
    from .agent_tool_result_postgres import _accept_exact_agent_tool_result_uow
    from .service_daemon import request_service_wakeup

    if not isinstance(occurrence, AgentToolOccurrence) or not isinstance(terminal, AgentToolTerminalResult):
        raise ValueError("accept start result requires exact occurrence and terminal result")
    occurrence = revalidate_acquisition_start_v2_occurrence(occurrence).occurrence
    terminal = terminal.revalidated()
    terminal.validate_for_occurrence(occurrence)
    binding = revalidate_acquisition_start_v2_occurrence(occurrence)
    operation_run_id = operation_run_id_for(
        action_id=binding.action_id,
        operation_type="acquisition_run",
        idempotency_key=binding.start_idempotency,
    )
    workflow_run_id = _workflow_run_id(operation_run_id)
    if (
        terminal.owner_target_kind != ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND
        or terminal.action_id != binding.action_id
        or terminal.operation_run_id != operation_run_id
    ):
        raise ValueError("accept start result requires start_acquisition_run command acceptance owner")
    result = _accept_exact_agent_tool_result_uow(
        adapter,
        occurrence=occurrence,
        terminal=terminal,
        attempted_slot_generation=attempted_slot_generation,
        required_tables=_RESULT_REQUIRED_TABLES,
        lock_groups=start_acquisition_result_lock_groups(
            occurrence=occurrence,
            operation_run_id=operation_run_id,
            workflow_run_id=workflow_run_id,
            workflow_command_id=terminal.workflow_command_id,
            command_key=_command_key_from_terminal_owner_ref(terminal),
        ),
        load_base_owner=load_start_acquisition_result_base_owner,
        assert_locked_owner=assert_exact_start_acquisition_result_owner,
        apply_acceptance_effect=_release_start_command_hold,
        assert_accepted_replay_effect=_assert_start_command_released,
        assert_late_quarantine_owner=_assert_start_late_quarantine_owner,
        lock_timeout_seconds=lock_timeout_seconds,
        fault_injection_point=fault_injection_point,
    )
    if not result:
        return result
    if result.get("outcome") in {"accepted", "replayed"} and result.get("released_workflow_command"):
        try:
            wake = request_service_wakeup(
                adapter.runtime_dir,
                "worker-recovery-daemon",
                reason="durable_runtime_event",
                requested_by="agent_start_v2_result_accept_uow",
                callback_payload={"source": "durable_runtime_event"},
            )
        except Exception:
            wake = None
        if wake:
            result = {**result, "recovery_wakeup": wake}
    return result


__all__ = [
    "ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_TARGET_KIND",
    "accept_start_acquisition_tool_result_uow",
    "assert_exact_start_acquisition_result_owner",
    "load_start_acquisition_result_base_owner",
    "prepare_start_acquisition_tool_result",
    "start_acquisition_result_lock_groups",
    "terminal_from_locked_start_acquisition_owner",
]
