from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence, AgentToolTerminalResult
from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.repositories.workflow_runtime import WorkflowRuntimeRepository


def _occurrence() -> AgentToolOccurrence:
    return AgentToolOccurrence.from_tool_spec(
        result_slot_id="slot_start_create_delegate_1",
        slot_generation=1,
        workspace_id="workspace_1",
        actor_id="requester_1",
        runtime_namespace="isolated_local_canary",
        provider_mode="simulate",
        turn_id="turn_start_create_delegate_1",
        step_id="step_start_create_delegate_1",
        tool_spec=START_ACQUISITION_RUN_TOOL_SPEC,
        canonical_args={},
        occurrence_ordinal=1,
    )


class _NativeAdapter:
    mode = "postgres_only"

    def __init__(self, result: dict[str, Any] | None) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    def should_prefer_read(self, table_name: str) -> bool:
        return bool(table_name)

    def is_authoritative(self, table_name: str) -> bool:
        return bool(table_name)

    def create_acquisition_start_v2_uow(self, **kwargs: Any) -> dict[str, Any] | None:
        self.calls.append(dict(kwargs))
        return self.result

    def prepare_start_acquisition_tool_result(self, **kwargs: Any) -> Any:
        self.calls.append(dict(kwargs))
        return self.result

    def accept_start_acquisition_tool_result_uow(self, **kwargs: Any) -> dict[str, Any] | None:
        self.calls.append(dict(kwargs))
        return self.result


def _terminal() -> AgentToolTerminalResult:
    return AgentToolTerminalResult.from_serialized_result(
        result_attempt_id="attempt_start_create_delegate_1",
        provider_call_id="provider_start_create_delegate_1",
        tool_call_id="tool_start_create_delegate_1",
        action_id="action_1",
        operation_run_id="operation_1",
        workflow_command_id="cmd_1",
        owner_target_kind="acquisition_start_command_acceptance_v1",
        owner_target_id="cmd_1",
        owner_target_revision=1,
        owner_target_generation=0,
        terminal_winner_id="opevt_winner_1",
        owner_result_ref={"schema_version": "acquisition_start_command_acceptance_owner_result_ref.v1"},
        owner_result_digest="a" * 64,
        serialized_result={"variant": "success", "status": "accepted"},
        is_error=False,
    )


def test_live_adapter_delegates_create_to_specialized_owner_without_import_cycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    adapter = LiveControlPlanePostgresAdapter(runtime_dir=tmp_path, dsn="", mode="disabled")
    occurrence = object()
    expected = {"outcome": "created"}
    calls: list[dict[str, Any]] = []
    owner_module = ModuleType("sourcing_agent.acquisition_start_v2_create_postgres")

    def create_owner(
        delegated_adapter: LiveControlPlanePostgresAdapter,
        *,
        occurrence: Any,
        approval_actor_id: str,
        approval_actor_kind: str,
        approval_policy_revision: str,
        lock_timeout_seconds: float,
        fault_injection_point: str,
    ) -> dict[str, Any]:
        calls.append(
            {
                "adapter": delegated_adapter,
                "occurrence": occurrence,
                "approval_actor_id": approval_actor_id,
                "approval_actor_kind": approval_actor_kind,
                "approval_policy_revision": approval_policy_revision,
                "lock_timeout_seconds": lock_timeout_seconds,
                "fault_injection_point": fault_injection_point,
            }
        )
        return expected

    owner_module.create_acquisition_start_v2_uow = create_owner  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, owner_module.__name__, owner_module)

    result = adapter.create_acquisition_start_v2_uow(
        occurrence=occurrence,
        approval_actor_id="human_1",
        approval_actor_kind="authenticated_user",
        approval_policy_revision="approval_policy.v1",
        lock_timeout_seconds=2.5,
        fault_injection_point="after_workflow_command_write",
    )

    assert result is expected
    assert calls == [
        {
            "adapter": adapter,
            "occurrence": occurrence,
            "approval_actor_id": "human_1",
            "approval_actor_kind": "authenticated_user",
            "approval_policy_revision": "approval_policy.v1",
            "lock_timeout_seconds": 2.5,
            "fault_injection_point": "after_workflow_command_write",
        }
    ]


def test_live_adapter_rejects_an_alternate_table_before_create_owner_import(tmp_path: Any) -> None:
    adapter = LiveControlPlanePostgresAdapter(runtime_dir=tmp_path, dsn="", mode="disabled")

    with pytest.raises(
        ValueError,
        match="create_acquisition_start_v2_uow requires table_name=agent_actions",
    ):
        adapter.create_acquisition_start_v2_uow(
            table_name="operation_runs",
            occurrence=object(),
            approval_actor_id="human_1",
            approval_actor_kind="authenticated_user",
            approval_policy_revision="approval_policy.v1",
        )


def test_live_adapter_delegates_start_result_prepare_and_accept_without_import_cycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    adapter = LiveControlPlanePostgresAdapter(runtime_dir=tmp_path, dsn="", mode="disabled")
    occurrence = object()
    terminal = object()
    expected_terminal = object()
    expected_accept = {"outcome": "accepted"}
    calls: list[dict[str, Any]] = []
    owner_module = ModuleType("sourcing_agent.acquisition_start_v2_result_postgres")

    def prepare_owner(delegated_adapter: Any, **kwargs: Any) -> Any:
        calls.append({"method": "prepare", "adapter": delegated_adapter, **kwargs})
        return expected_terminal

    def accept_owner(delegated_adapter: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append({"method": "accept", "adapter": delegated_adapter, **kwargs})
        return expected_accept

    owner_module.prepare_start_acquisition_tool_result = prepare_owner  # type: ignore[attr-defined]
    owner_module.accept_start_acquisition_tool_result_uow = accept_owner  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, owner_module.__name__, owner_module)

    prepared = adapter.prepare_start_acquisition_tool_result(
        occurrence=occurrence,
        result_attempt_id="attempt_1",
        provider_call_id="provider_1",
        tool_call_id="tool_1",
        lock_timeout_seconds=2.5,
    )
    accepted = adapter.accept_start_acquisition_tool_result_uow(
        occurrence=occurrence,
        terminal=terminal,
        attempted_slot_generation=3,
        lock_timeout_seconds=4.5,
        fault_injection_point="after_journal_write",
    )

    assert prepared is expected_terminal
    assert accepted is expected_accept
    assert calls == [
        {
            "method": "prepare",
            "adapter": adapter,
            "occurrence": occurrence,
            "result_attempt_id": "attempt_1",
            "provider_call_id": "provider_1",
            "tool_call_id": "tool_1",
            "lock_timeout_seconds": 2.5,
        },
        {
            "method": "accept",
            "adapter": adapter,
            "occurrence": occurrence,
            "terminal": terminal,
            "attempted_slot_generation": 3,
            "lock_timeout_seconds": 4.5,
            "fault_injection_point": "after_journal_write",
        },
    ]


def test_repository_requires_create_authorities_and_maps_the_complete_owner_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    occurrence = _occurrence()
    confirmation_receipt = {
        "schema_version": "acquisition_confirmation_receipt.v1",
        "receipt_id": "opevt_receipt_1",
        "receipt_digest": "a" * 64,
    }
    owner_result_ref = {
        "schema_version": "acquisition_start_command_acceptance_owner_result_ref.v1",
        "workflow_command_id": "cmd_1",
    }
    adapter = _NativeAdapter(
        {
            "outcome": "created",
            "replayed": False,
            "action": {
                "action_id": "action_1",
                "workspace_id": "workspace_1",
                "status": "queued",
                "approval_status": "approved",
            },
            "operation_run": {
                "operation_run_id": "operation_1",
                "workspace_id": "workspace_1",
                "action_id": "action_1",
                "status": "queued",
            },
            "receipt_event": {
                "event_id": "opevt_receipt_1",
                "event_stream_id": "action_1",
                "event_type": "ActionApproved",
                "sequence_number": 2,
                "payload_json": confirmation_receipt,
            },
            "workflow_events": [
                {
                    "event_id": "evt_started_1",
                    "workflow_run_id": "workflow_1",
                    "event_type": "WorkflowStarted",
                    "sequence_number": 1,
                    "payload_json": {"stage_key": "acquisition_run_create"},
                    "artifact_refs_json": [],
                },
                {
                    "event_id": "evt_plan_1",
                    "workflow_run_id": "workflow_1",
                    "event_type": "CommandPlanRequested",
                    "sequence_number": 2,
                    "payload_json": {"command_type": "acquisition.run.create"},
                    "artifact_refs_json": [],
                },
            ],
            "workflow_command": {
                "command_id": "cmd_1",
                "workflow_run_id": "workflow_1",
                "command_type": "acquisition.run.create",
                "status": "queued",
                "payload_json": {"schema_version": "acquisition_start_root_command.v1"},
                "artifact_refs_json": [],
            },
            "workflow_current_state": {
                "workflow_run_id": "workflow_1",
                "operation_id": "operation_1",
                "status": "running",
                "last_processed_sequence_number": 2,
                "active_command_counts_json": {
                    "acquisition_run_writer": {"acquisition.run.create": 1}
                },
            },
            "planned_event": {
                "event_id": "opevt_planned_1",
                "event_stream_id": "operation_1",
                "event_type": "OperationCommandPlanned",
                "sequence_number": 1,
                "payload_json": {
                    "owner_result_ref": owner_result_ref,
                    "owner_result_digest": "b" * 64,
                },
            },
            "confirmation_receipt": confirmation_receipt,
            "owner_result_ref": owner_result_ref,
            "owner_result_digest": "b" * 64,
        }
    )
    repository = WorkflowRuntimeRepository(adapter)
    required_tables: list[str] = []
    monkeypatch.setattr(
        repository,
        "_require_postgres_for_durable_runtime",
        required_tables.append,
    )

    result = repository.create_acquisition_start_v2_uow(
        occurrence=occurrence,
        approval_actor_id="human_1",
        approval_actor_kind="authenticated_user",
        approval_policy_revision="approval_policy.v1",
        lock_timeout_seconds=3.25,
    )

    assert required_tables == [
        "operation_runs",
        "agent_actions",
        "operation_events",
        "agent_tool_result_slots",
        "acquisition_plan_previews",
        "workflow_events",
        "workflow_commands",
        "workflow_current_state",
    ]
    assert adapter.calls == [
        {
            "table_name": "agent_actions",
            "occurrence": occurrence,
            "approval_actor_id": "human_1",
            "approval_actor_kind": "authenticated_user",
            "approval_policy_revision": "approval_policy.v1",
            "lock_timeout_seconds": 3.25,
        }
    ]
    assert result["outcome"] == "created"
    assert result["replayed"] is False
    assert result["action"]["action_id"] == "action_1"
    assert result["action"]["approval_status"] == "approved"
    assert result["operation_run"]["operation_run_id"] == "operation_1"
    assert result["receipt_event"]["payload"] == confirmation_receipt
    assert [event["event_type"] for event in result["workflow_events"]] == [
        "WorkflowStarted",
        "CommandPlanRequested",
    ]
    assert result["workflow_command"]["payload"] == {
        "schema_version": "acquisition_start_root_command.v1"
    }
    assert result["workflow_current_state"]["active_command_counts"] == {
        "acquisition_run_writer": {"acquisition.run.create": 1}
    }
    assert result["planned_event"]["payload"]["owner_result_ref"] == owner_result_ref
    assert result["confirmation_receipt"] == confirmation_receipt
    assert result["owner_result_ref"] == owner_result_ref
    assert result["owner_result_digest"] == "b" * 64


def test_repository_maps_start_result_prepare_and_accept_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    occurrence = _occurrence()
    terminal = _terminal()
    adapter = _NativeAdapter(
        {
            "outcome": "accepted",
            "replayed": False,
            "slot": {
                "result_slot_id": occurrence.result_slot_id,
                "status": "accepted",
            },
            "attempt": {
                "result_attempt_id": terminal.result_attempt_id,
                "disposition": "accepted",
            },
            "journal": {
                "journal_id": "tooljournal_1",
                "result_attempt_id": terminal.result_attempt_id,
            },
            "released_workflow_command": {
                "command_id": "cmd_1",
                "workflow_run_id": "workflow_1",
                "not_before_at": "",
            },
            "recovery_wakeup": {"status": "requested"},
        }
    )
    repository = WorkflowRuntimeRepository(adapter)
    required_tables: list[str] = []
    monkeypatch.setattr(
        repository,
        "_require_postgres_for_durable_runtime",
        required_tables.append,
    )

    adapter.result = terminal
    prepared = repository.prepare_start_acquisition_tool_result(
        occurrence=occurrence,
        result_attempt_id=terminal.result_attempt_id,
        provider_call_id=terminal.provider_call_id,
        tool_call_id=terminal.tool_call_id,
        lock_timeout_seconds=2.0,
    )
    adapter.result = {
        "outcome": "accepted",
        "replayed": False,
        "slot": {"result_slot_id": occurrence.result_slot_id, "status": "accepted"},
        "attempt": {"result_attempt_id": terminal.result_attempt_id, "disposition": "accepted"},
        "journal": {"journal_id": "tooljournal_1", "result_attempt_id": terminal.result_attempt_id},
        "released_workflow_command": {"command_id": "cmd_1", "workflow_run_id": "workflow_1", "not_before_at": ""},
        "recovery_wakeup": {"status": "requested"},
    }
    accepted = repository.accept_start_acquisition_tool_result_uow(
        occurrence=occurrence,
        terminal=terminal,
        attempted_slot_generation=occurrence.slot_generation,
        lock_timeout_seconds=3.0,
    )

    assert prepared is terminal
    assert required_tables == [
        "operation_events",
        "operation_runs",
        "agent_actions",
        "acquisition_plan_previews",
        "workflow_events",
        "workflow_commands",
        "workflow_current_state",
        "agent_tool_result_slots",
        "agent_tool_result_attempts",
        "agent_tool_result_journal",
        "workflow_commands",
    ]
    assert accepted["outcome"] == "accepted"
    assert accepted["slot"]["status"] == "accepted"
    assert accepted["attempt"]["disposition"] == "accepted"
    assert accepted["released_workflow_command"]["not_before_at"] == ""
    assert accepted["recovery_wakeup"] == {"status": "requested"}


def test_repository_rejects_wrong_occurrence_type_before_create_native_write() -> None:
    adapter = _NativeAdapter({"outcome": "created"})
    repository = WorkflowRuntimeRepository(adapter)

    with pytest.raises(ValueError, match="requires AgentToolOccurrence"):
        repository.create_acquisition_start_v2_uow(
            occurrence=object(),
            approval_actor_id="human_1",
            approval_actor_kind="authenticated_user",
            approval_policy_revision="approval_policy.v1",
        )

    assert adapter.calls == []


def test_repository_fails_closed_when_create_owner_returns_no_bundle() -> None:
    adapter = _NativeAdapter(None)
    repository = WorkflowRuntimeRepository(adapter)

    with pytest.raises(
        RuntimeError,
        match="Postgres authoritative write failed for agent_actions.*native writer returned no bundle",
    ):
        repository.create_acquisition_start_v2_uow(
            occurrence=_occurrence(),
            approval_actor_id="human_1",
            approval_actor_kind="authenticated_user",
            approval_policy_revision="approval_policy.v1",
        )

    assert len(adapter.calls) == 1
