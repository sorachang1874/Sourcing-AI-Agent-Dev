from __future__ import annotations

import ast
import inspect
import textwrap
from dataclasses import replace
from typing import Any, Callable

import pytest

import sourcing_agent.orchestrator as orchestrator_module
from sourcing_agent.operation_runtime import (
    ACTION_DISPATCH_ADAPTERS,
    ACTION_EXPORT_CANDIDATES,
    ACTION_EXTERNAL_INTAKE,
    ACTION_PLAN_ACQUISITION,
    ACTION_PROMOTE_PERSON_ASSERTION,
    DEFAULT_ACTION_REGISTRY,
    DISPATCH_ADAPTER_EXPORT,
    ActionRegistry,
    ActionSpec,
)
from sourcing_agent.orchestrator import SourcingOrchestrator


class _DispatchProbe:
    @staticmethod
    def _operation_run_control_response_record(record: dict[str, Any]) -> dict[str, Any]:
        return record

    @staticmethod
    def _dispatch_projection_read_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "projection_read"}

    @staticmethod
    def _dispatch_person_public_web_enrichment_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "person_public_web"}

    @staticmethod
    def _dispatch_export_candidates_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "export"}

    @staticmethod
    def _dispatch_agent_callable_workflow_command_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "agent_callable_workflow_command"}

    @staticmethod
    def _dispatch_crm_writer_operation(**_: Any) -> dict[str, Any]:
        return {"adapter": "crm_writer"}

    def _operation_dispatch_adapter_bindings(self) -> dict[str, Callable[..., dict[str, Any]]]:
        return SourcingOrchestrator._operation_dispatch_adapter_bindings(self)

    def __getattr__(self, name: str) -> Any:
        raise AssertionError(f"dispatch attempted arbitrary attribute lookup: {name}")


class _MissingExportBindingProbe(_DispatchProbe):
    def _operation_dispatch_adapter_bindings(self) -> dict[str, Callable[..., dict[str, Any]]]:
        bindings = super()._operation_dispatch_adapter_bindings()
        bindings.pop(DISPATCH_ADAPTER_EXPORT)
        return bindings


def _dispatch(probe: Any, action_type: str) -> dict[str, Any]:
    return SourcingOrchestrator._dispatch_operation_run_from_records(
        probe,
        operation_run={"operation_run_id": f"operation-{action_type}"},
        action={"action_id": f"action-{action_type}", "action_type": action_type},
        actor="d1b-test",
    )


def _valid_spec(*, action_type: str, dispatch_adapter: str) -> ActionSpec:
    return ActionSpec(
        action_type=action_type,
        owner_module="test_owner",
        operation_type="test_operation",
        dispatch_adapter=dispatch_adapter,
        description="D1b registry validation fixture.",
        display_label="D1b fixture",
        display_category="test",
    )


def _registered_action_literals(tree: ast.AST, registered_action_types: set[str]) -> set[str]:
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in registered_action_types
    }


def _action_type_control_flow_nodes(tree: ast.AST) -> list[ast.AST]:
    predicate_nodes: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.If, ast.IfExp, ast.While)):
            predicate_nodes.append(node.test)
        elif isinstance(node, ast.Match):
            predicate_nodes.append(node.subject)
        elif isinstance(node, ast.comprehension):
            predicate_nodes.extend(node.ifs)
    return [
        predicate
        for predicate in predicate_nodes
        if any(
            isinstance(descendant, ast.Name) and descendant.id == "action_type" for descendant in ast.walk(predicate)
        )
    ]


def test_dispatch_adapter_registry_is_closed_and_normalized() -> None:
    with pytest.raises(ValueError, match="unregistered dispatch adapter"):
        ActionRegistry(
            {
                "unknown_adapter": _valid_spec(
                    action_type="unknown_adapter",
                    dispatch_adapter="dynamic.module.method",
                )
            }
        )

    with pytest.raises(ValueError, match="dispatch_adapter must be normalized"):
        ActionRegistry(
            {
                "unnormalized_adapter": _valid_spec(
                    action_type="unnormalized_adapter",
                    dispatch_adapter=f" {DISPATCH_ADAPTER_EXPORT} ",
                )
            }
        )


def test_dispatch_bindings_are_explicit_total_and_do_not_use_getattr_or_action_branches() -> None:
    probe = _DispatchProbe()
    bindings = probe._operation_dispatch_adapter_bindings()
    assert set(bindings) == ACTION_DISPATCH_ADAPTERS
    assert len(set(bindings.values())) == len(ACTION_DISPATCH_ADAPTERS)

    dispatch_source = textwrap.dedent(inspect.getsource(SourcingOrchestrator._dispatch_operation_run_from_records))
    binding_source = textwrap.dedent(inspect.getsource(SourcingOrchestrator._operation_dispatch_adapter_bindings))
    dispatch_tree = ast.parse(dispatch_source)
    registered_action_types = set(DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False))
    assert not {
        node.id for node in ast.walk(dispatch_tree) if isinstance(node, ast.Name) and node.id.startswith("ACTION_")
    }
    assert not _registered_action_literals(dispatch_tree, registered_action_types)
    assert not _action_type_control_flow_nodes(dispatch_tree)
    assert not [
        node
        for tree in (dispatch_tree, ast.parse(binding_source))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr"
    ]

    literal_branch_mutation = dispatch_source.replace(
        '    action_type = str(action.get("action_type") or "").strip()\n',
        '    action_type = str(action.get("action_type") or "").strip()\n'
        '    if action_type == "search_projection":\n'
        "        return {}\n",
        1,
    )
    assert literal_branch_mutation != dispatch_source
    literal_branch_tree = ast.parse(literal_branch_mutation)
    assert _registered_action_literals(literal_branch_tree, registered_action_types) == {"search_projection"}
    assert _action_type_control_flow_nodes(literal_branch_tree)


@pytest.mark.parametrize(
    "action_type",
    sorted(DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)),
)
def test_every_action_follows_registry_adapter_mutation_without_action_type_branch(
    action_type: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_adapter = DEFAULT_ACTION_REGISTRY.spec_for(action_type).dispatch_adapter
    replacement_adapter = next(adapter for adapter in sorted(ACTION_DISPATCH_ADAPTERS) if adapter != original_adapter)
    mutated_specs = {
        registered_action_type: replace(
            DEFAULT_ACTION_REGISTRY.spec_for(registered_action_type),
            dispatch_adapter=(
                replacement_adapter
                if registered_action_type == action_type
                else DEFAULT_ACTION_REGISTRY.spec_for(registered_action_type).dispatch_adapter
            ),
        )
        for registered_action_type in DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    }
    monkeypatch.setattr(orchestrator_module, "DEFAULT_ACTION_REGISTRY", ActionRegistry(mutated_specs))

    assert _dispatch(_DispatchProbe(), action_type) == {"adapter": replacement_adapter}


def test_missing_bound_adapter_and_unregistered_action_fail_closed() -> None:
    result = _dispatch(_MissingExportBindingProbe(), ACTION_EXPORT_CANDIDATES)
    assert result["status"] == "unsupported"
    assert result["reason"] == "operation action 'export_candidates' has no W9b owner adapter"
    assert result["module_state_mutated"] is False

    unknown = _dispatch(_DispatchProbe(), "unregistered_action")
    assert unknown["status"] == "unsupported"
    assert unknown["reason"] == "operation action 'unregistered_action' has no W9b owner adapter"
    assert unknown["module_state_mutated"] is False


def test_empty_adapter_actions_stay_unsupported_and_adapter_alone_never_serves_a_tool() -> None:
    records = DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False)
    assert {
        action_type for action_type in records if not DEFAULT_ACTION_REGISTRY.spec_for(action_type).dispatch_adapter
    } == {
        ACTION_PLAN_ACQUISITION,
        ACTION_PROMOTE_PERSON_ASSERTION,
        ACTION_EXTERNAL_INTAKE,
    }
    for action_type in (ACTION_PLAN_ACQUISITION, ACTION_PROMOTE_PERSON_ASSERTION, ACTION_EXTERNAL_INTAKE):
        result = _dispatch(_DispatchProbe(), action_type)
        assert result["status"] == "unsupported"
        assert result["reason"] == f"operation action {action_type!r} has no W9b owner adapter"

    for record in records.values():
        assert "request_schema" not in record
        assert "dispatch_adapter" not in record
        assert "model_safe_result_schema" not in record
        assert "agent_tool_enabled" not in record
        assert "served_tool_status" not in record
