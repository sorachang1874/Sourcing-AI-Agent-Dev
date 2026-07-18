from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

import pytest

from sourcing_agent.agent_canary_registry import START_ACQUISITION_RUN_TOOL_SPEC
from sourcing_agent.agent_tool_result_slot import AgentToolOccurrence
from sourcing_agent.control_plane_live_postgres import LiveControlPlanePostgresAdapter
from sourcing_agent.repositories.workflow_runtime import WorkflowRuntimeRepository


def _occurrence() -> AgentToolOccurrence:
    return AgentToolOccurrence.from_tool_spec(
        result_slot_id="slot_start_delegate_1",
        slot_generation=1,
        workspace_id="workspace_1",
        actor_id="requester_1",
        runtime_namespace="isolated_local_canary",
        provider_mode="simulate",
        turn_id="turn_start_delegate_1",
        step_id="step_start_delegate_1",
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

    def submit_acquisition_start_v2_action_uow(self, **kwargs: Any) -> dict[str, Any] | None:
        self.calls.append(dict(kwargs))
        return self.result


def test_live_adapter_delegates_to_specialized_owner_without_import_cycle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    adapter = LiveControlPlanePostgresAdapter(runtime_dir=tmp_path, dsn="", mode="disabled")
    occurrence = object()
    expected = {"outcome": "submitted"}
    calls: list[dict[str, Any]] = []
    owner_module = ModuleType("sourcing_agent.acquisition_start_v2_postgres")

    def submit_owner(
        delegated_adapter: LiveControlPlanePostgresAdapter,
        *,
        occurrence: Any,
        lock_timeout_seconds: float,
        fault_injection_point: str,
    ) -> dict[str, Any]:
        calls.append(
            {
                "adapter": delegated_adapter,
                "occurrence": occurrence,
                "lock_timeout_seconds": lock_timeout_seconds,
                "fault_injection_point": fault_injection_point,
            }
        )
        return expected

    owner_module.submit_acquisition_start_v2_action_uow = submit_owner  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, owner_module.__name__, owner_module)

    result = adapter.submit_acquisition_start_v2_action_uow(
        occurrence=occurrence,
        lock_timeout_seconds=2.5,
        fault_injection_point="after_action_write",
    )

    assert result is expected
    assert calls == [
        {
            "adapter": adapter,
            "occurrence": occurrence,
            "lock_timeout_seconds": 2.5,
            "fault_injection_point": "after_action_write",
        }
    ]


def test_live_adapter_rejects_an_alternate_table_before_owner_import(tmp_path: Any) -> None:
    adapter = LiveControlPlanePostgresAdapter(runtime_dir=tmp_path, dsn="", mode="disabled")

    with pytest.raises(
        ValueError,
        match="submit_acquisition_start_v2_action_uow requires table_name=agent_actions",
    ):
        adapter.submit_acquisition_start_v2_action_uow(
            table_name="operation_runs",
            occurrence=object(),
        )


def test_repository_requires_exact_occurrence_and_maps_the_owner_bundle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    occurrence = _occurrence()
    adapter = _NativeAdapter(
        {
            "outcome": "submitted",
            "replayed": False,
            "action": {
                "action_id": "action_1",
                "workspace_id": "workspace_1",
                "status": "approval_required",
            },
            "event": {
                "event_id": "opevt_1",
                "workspace_id": "workspace_1",
                "event_stream_id": "action_1",
                "action_id": "action_1",
                "event_type": "ActionApprovalRequired",
                "sequence_number": 1,
            },
        }
    )
    repository = WorkflowRuntimeRepository(adapter)
    required_tables: list[str] = []
    monkeypatch.setattr(
        repository,
        "_require_postgres_for_durable_runtime",
        required_tables.append,
    )

    result = repository.submit_acquisition_start_v2_action_uow(
        occurrence=occurrence,
        lock_timeout_seconds=3.25,
    )

    assert required_tables == [
        "agent_actions",
        "operation_events",
        "agent_tool_result_slots",
        "acquisition_plan_previews",
    ]
    assert adapter.calls == [
        {
            "table_name": "agent_actions",
            "occurrence": occurrence,
            "lock_timeout_seconds": 3.25,
        }
    ]
    assert result["outcome"] == "submitted"
    assert result["replayed"] is False
    assert result["action"]["action_id"] == "action_1"
    assert result["action"]["status"] == "approval_required"
    assert result["event"]["event_id"] == "opevt_1"
    assert result["event"]["event_type"] == "ActionApprovalRequired"
    assert result["event"]["sequence_number"] == 1


def test_repository_rejects_wrong_occurrence_type_before_native_write() -> None:
    adapter = _NativeAdapter({"outcome": "submitted"})
    repository = WorkflowRuntimeRepository(adapter)

    with pytest.raises(ValueError, match="requires AgentToolOccurrence"):
        repository.submit_acquisition_start_v2_action_uow(occurrence=object())

    assert adapter.calls == []


def test_repository_fails_closed_when_the_authoritative_owner_returns_no_bundle() -> None:
    adapter = _NativeAdapter(None)
    repository = WorkflowRuntimeRepository(adapter)

    with pytest.raises(
        RuntimeError,
        match="Postgres authoritative write failed for agent_actions.*native writer returned no bundle",
    ):
        repository.submit_acquisition_start_v2_action_uow(occurrence=_occurrence())

    assert len(adapter.calls) == 1
