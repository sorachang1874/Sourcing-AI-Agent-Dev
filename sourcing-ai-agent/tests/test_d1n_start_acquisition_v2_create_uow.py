from __future__ import annotations

from dataclasses import replace

import pytest

from sourcing_agent import acquisition_start_v2_create_postgres as create_pg
from sourcing_agent import acquisition_start_v2_result_postgres as result_pg
from sourcing_agent.agent_tool_result_slot import AgentToolResultSlotError
from tests.test_d1n_s1e1_start_authority_owner_decision import _build_executable_contract

_APPROVAL = {
    "approval_actor_id": "human_1",
    "approval_actor_kind": "authenticated_user",
    "approval_policy_revision": "acquisition_confirmation_policy_v1",
}


class _NoAccessAdapter:
    """Proves closed-input rejection happens before dependency or PG access."""

    def __init__(self) -> None:
        self.accesses: list[tuple[str, object]] = []

    def should_prefer_read(self, table_name: str) -> bool:
        self.accesses.append(("should_prefer_read", table_name))
        raise AssertionError("adapter dependency access was not expected")

    def is_authoritative(self, table_name: str) -> bool:
        self.accesses.append(("is_authoritative", table_name))
        raise AssertionError("adapter dependency access was not expected")

    def _connect_with_timeout(self, timeout: float):
        self.accesses.append(("connect", timeout))
        raise AssertionError("PG access was not expected")


@pytest.mark.parametrize("provider_mode", ["live", "replay"])
def test_create_rejects_disallowed_modes_before_dependency_or_pg_access(provider_mode: str) -> None:
    occurrence = replace(_build_executable_contract()["occurrence"], provider_mode=provider_mode)
    adapter = _NoAccessAdapter()

    with pytest.raises(AgentToolResultSlotError, match="runtime_mode_not_allowed"):
        create_pg.create_acquisition_start_v2_uow(adapter, occurrence=occurrence, **_APPROVAL)

    assert adapter.accesses == []


def test_create_rejects_disallowed_namespace_before_dependency_or_pg_access() -> None:
    occurrence = replace(_build_executable_contract()["occurrence"], runtime_namespace="hosted")
    adapter = _NoAccessAdapter()

    with pytest.raises(AgentToolResultSlotError, match="runtime_mode_not_allowed"):
        create_pg.create_acquisition_start_v2_uow(adapter, occurrence=occurrence, **_APPROVAL)

    assert adapter.accesses == []


@pytest.mark.parametrize(
    ("override", "error"),
    [
        ({"approval_actor_id": ""}, "approval_actor_id"),
        ({"approval_actor_id": "x" * 201}, "approval_actor_id"),
        ({"approval_actor_id": "human\x7f1"}, "approval_actor_id"),
        ({"approval_actor_id": "human" + chr(0xD800)}, "approval_actor_id"),
        ({"approval_actor_kind": "service"}, "approval_actor_kind"),
        ({"approval_policy_revision": ""}, "approval_policy_revision"),
        ({"approval_policy_revision": "bad revision"}, "approval_policy_revision"),
        ({"approval_policy_revision": "v" + ("1" * 128)}, "approval_policy_revision"),
    ],
)
def test_create_rejects_nonhuman_or_unversioned_approval_before_dependency_or_pg_access(
    override: dict[str, str],
    error: str,
) -> None:
    adapter = _NoAccessAdapter()
    approval = {**_APPROVAL, **override}

    with pytest.raises((AgentToolResultSlotError, ValueError), match=error):
        create_pg.create_acquisition_start_v2_uow(
            adapter,
            occurrence=_build_executable_contract()["occurrence"],
            **approval,
        )

    assert adapter.accesses == []


def test_create_and_result_share_ratified_lock_topology() -> None:
    occurrence = _build_executable_contract()["occurrence"]
    binding = create_pg.revalidate_acquisition_start_v2_occurrence(occurrence)
    operation_run_id = "op_start_lock_topology"
    workflow_run_id = "workflow_start_lock_topology"
    workflow_command_id = "command_start_lock_topology"
    command_key = "acquisition.run.create:start-v2:receipt_lock_topology"

    create_groups = create_pg._advisory_lock_groups(  # noqa: SLF001
        binding=binding,
        operation_run_id=operation_run_id,
        workflow_run_id=workflow_run_id,
        workflow_command_id=workflow_command_id,
        command_key=command_key,
    )
    result_groups = result_pg.start_acquisition_result_lock_groups(
        occurrence=occurrence,
        operation_run_id=operation_run_id,
        workflow_run_id=workflow_run_id,
        workflow_command_id=workflow_command_id,
        command_key=command_key,
    )

    assert create_groups == result_groups
    flattened = [key for group in create_groups for key in group]
    assert flattened == [
        f"operation_runs:id:{operation_run_id}",
        f"operation_runs:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        f"agent_actions:id:{binding.action_id}",
        f"agent_actions:idempotency:{occurrence.workspace_id}:{binding.start_idempotency}",
        f"agent_tool_result_slots:id:{occurrence.result_slot_id}",
        f"agent_tool_result_slots:occurrence:{occurrence.logical_occurrence_digest}",
        f"acquisition_plan_previews:id:{binding.preview_id}",
        f"acquisition_plan_previews:revision:{binding.preview_revision}",
        f"workflow_commands:id:{workflow_command_id}",
        f"workflow_commands:idempotency:{workflow_run_id}:{command_key}",
        f"workflow_current_state:{workflow_run_id}",
        f"operation_events:{binding.action_id}",
        f"operation_events:{operation_run_id}",
        f"workflow_events:{workflow_run_id}",
    ]
    assert all(list(group) == sorted(group, key=lambda value: value.encode("utf-8")) for group in create_groups)


def test_create_fault_surface_is_closed() -> None:
    occurrence = _build_executable_contract()["occurrence"]
    adapter = _NoAccessAdapter()

    with pytest.raises((AgentToolResultSlotError, ValueError), match="fault"):
        create_pg.create_acquisition_start_v2_uow(
            adapter,
            occurrence=occurrence,
            fault_injection_point="unknown_boundary",
            **_APPROVAL,
        )

    assert adapter.accesses == []
