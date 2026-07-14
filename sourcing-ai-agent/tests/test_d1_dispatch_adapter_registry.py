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


def _action_constant_references(tree: ast.AST) -> set[str]:
    references: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id.startswith("ACTION_"):
            references.add(node.id)
        elif isinstance(node, ast.Attribute) and node.attr.startswith("ACTION_"):
            references.add(node.attr)
    return references


def _reads_action_type(node: ast.AST) -> bool:
    for descendant in ast.walk(node):
        if isinstance(descendant, ast.Name) and descendant.id == "action_type":
            return True
        if (
            isinstance(descendant, ast.Call)
            and isinstance(descendant.func, ast.Attribute)
            and isinstance(descendant.func.value, ast.Name)
            and descendant.func.value.id == "action"
            and descendant.func.attr == "get"
            and descendant.args
            and isinstance(descendant.args[0], ast.Constant)
            and descendant.args[0].value == "action_type"
        ):
            return True
        if (
            isinstance(descendant, ast.Subscript)
            and isinstance(descendant.value, ast.Name)
            and descendant.value.id == "action"
            and isinstance(descendant.slice, ast.Constant)
            and descendant.slice.value == "action_type"
        ):
            return True
    return False


def _action_type_control_flow_nodes(tree: ast.AST) -> list[ast.AST]:
    predicate_nodes: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.If, ast.IfExp, ast.While)):
            predicate_nodes.append(node.test)
        elif isinstance(node, ast.Match):
            predicate_nodes.append(node.subject)
        elif isinstance(node, ast.comprehension):
            predicate_nodes.extend(node.ifs)
    return [predicate for predicate in predicate_nodes if _reads_action_type(predicate)]


_DYNAMIC_ATTRIBUTE_LOOKUP_SYMBOLS = frozenset(
    {
        "__getattribute__",
        "attrgetter",
        "getattr",
        "getattr_static",
        "vars",
    }
)


def _assigned_names(target: ast.AST) -> set[str]:
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.List, ast.Tuple)):
        return {name for item in target.elts for name in _assigned_names(item)}
    return set()


def _dynamic_attribute_lookup_reference(node: ast.AST, aliases: set[str]) -> bool:
    if isinstance(node, ast.Name):
        return node.id in aliases or node.id in _DYNAMIC_ATTRIBUTE_LOOKUP_SYMBOLS
    return isinstance(node, ast.Attribute) and (
        node.attr in _DYNAMIC_ATTRIBUTE_LOOKUP_SYMBOLS or node.attr == "__dict__"
    )


def _dynamic_attribute_lookup_aliases(
    trees: tuple[ast.AST, ...],
) -> set[str]:
    lookup_aliases = set(_DYNAMIC_ATTRIBUTE_LOOKUP_SYMBOLS)
    nodes = [node for tree in trees for node in ast.walk(tree)]

    for node in nodes:
        if not isinstance(node, ast.ImportFrom):
            continue
        for imported in node.names:
            imported_name = imported.name.rsplit(".", 1)[-1]
            bound_name = imported.asname or imported_name
            if imported_name in _DYNAMIC_ATTRIBUTE_LOOKUP_SYMBOLS:
                lookup_aliases.add(bound_name)

    changed = True
    while changed:
        changed = False
        for node in nodes:
            targets: set[str] = set()
            value: ast.AST | None = None
            if isinstance(node, ast.Assign):
                targets = {name for target in node.targets for name in _assigned_names(target)}
                value = node.value
            elif isinstance(node, ast.AnnAssign):
                targets = _assigned_names(node.target)
                value = node.value
            elif isinstance(node, ast.NamedExpr):
                targets = _assigned_names(node.target)
                value = node.value
            if not targets or value is None:
                continue
            if _dynamic_attribute_lookup_reference(value, lookup_aliases):
                previous_size = len(lookup_aliases)
                lookup_aliases.update(targets)
                changed = changed or len(lookup_aliases) != previous_size
    return lookup_aliases


def _dynamic_attribute_lookup_nodes(
    *trees: ast.AST,
    alias_trees: tuple[ast.AST, ...] = (),
) -> list[ast.AST]:
    lookup_aliases = _dynamic_attribute_lookup_aliases((*alias_trees, *trees))
    matches: list[ast.AST] = []
    for tree in trees:
        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in lookup_aliases:
                matches.append(node)
            elif isinstance(node, ast.Attribute) and (
                node.attr in _DYNAMIC_ATTRIBUTE_LOOKUP_SYMBOLS or node.attr == "__dict__"
            ):
                matches.append(node)
    return matches


def _dispatch_handler_assignment_values(tree: ast.AST) -> list[ast.AST]:
    values: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            "dispatch_handler" in _assigned_names(target) for target in node.targets
        ):
            values.append(node.value)
        elif (
            isinstance(node, ast.AnnAssign)
            and node.value is not None
            and "dispatch_handler" in _assigned_names(node.target)
        ):
            values.append(node.value)
        elif isinstance(node, ast.NamedExpr) and "dispatch_handler" in _assigned_names(node.target):
            values.append(node.value)
    return values


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
    binding_tree = ast.parse(binding_source)
    orchestrator_tree = ast.parse(inspect.getsource(orchestrator_module))
    module_alias_tree = ast.Module(
        body=[
            node
            for node in orchestrator_tree.body
            if isinstance(node, (ast.AnnAssign, ast.Assign, ast.Import, ast.ImportFrom))
        ],
        type_ignores=[],
    )
    registered_action_types = set(DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False))
    assert not _action_constant_references(dispatch_tree)
    assert not _registered_action_literals(dispatch_tree, registered_action_types)
    assert not _action_type_control_flow_nodes(dispatch_tree)
    assert not _dynamic_attribute_lookup_nodes(dispatch_tree, binding_tree, alias_trees=(module_alias_tree,))
    dispatch_handler_values = _dispatch_handler_assignment_values(dispatch_tree)
    assert len(dispatch_handler_values) == 1
    assert ast.unparse(dispatch_handler_values[0]) == (
        "self._operation_dispatch_adapter_bindings().get(dispatch_adapter)"
    )

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

    qualified_action_mutation = dispatch_source.replace(
        '    action_type = str(action.get("action_type") or "").strip()\n',
        '    action_type = str(action.get("action_type") or "").strip()\n'
        "    hidden_action = orchestrator_module.ACTION_ADD_CRM_NOTE\n",
        1,
    )
    assert qualified_action_mutation != dispatch_source
    assert _action_constant_references(ast.parse(qualified_action_mutation)) == {"ACTION_ADD_CRM_NOTE"}

    raw_action_control_mutation = dispatch_source.replace(
        '    action_type = str(action.get("action_type") or "").strip()\n',
        '    action_type = str(action.get("action_type") or "").strip()\n'
        '    if action.get("action_type"):\n'
        "        return {}\n",
        1,
    )
    assert raw_action_control_mutation != dispatch_source
    assert _action_type_control_flow_nodes(ast.parse(raw_action_control_mutation))

    handler_lookup_line = "    dispatch_handler = self._operation_dispatch_adapter_bindings().get(dispatch_adapter)\n"
    dynamic_lookup_mutations = {
        "qualified_builtin": "    dispatch_handler = builtins.getattr(self, dispatch_adapter)\n",
        "assignment_alias": (
            "    resolve_handler = getattr\n    dispatch_handler = resolve_handler(self, dispatch_adapter)\n"
        ),
        "local_import_alias": (
            "    from builtins import getattr as resolve_handler\n"
            "    dispatch_handler = resolve_handler(self, dispatch_adapter)\n"
        ),
        "dunder_getattribute": "    dispatch_handler = self.__getattribute__(dispatch_adapter)\n",
        "vars_subscript": "    dispatch_handler = vars(self)[dispatch_adapter]\n",
        "vars_namespace_alias": (
            "    attribute_namespace = vars(self)\n    dispatch_handler = attribute_namespace[dispatch_adapter]\n"
        ),
    }
    for mutation_name, replacement in dynamic_lookup_mutations.items():
        mutation = dispatch_source.replace(handler_lookup_line, replacement, 1)
        assert mutation != dispatch_source, mutation_name
        assert _dynamic_attribute_lookup_nodes(ast.parse(mutation), binding_tree), mutation_name

    module_alias_mutation = dispatch_source.replace(
        handler_lookup_line,
        "    dispatch_handler = imported_handler_lookup(self, dispatch_adapter)\n",
        1,
    )
    imported_alias_tree = ast.parse("from builtins import getattr as imported_handler_lookup\n")
    assert module_alias_mutation != dispatch_source
    assert _dynamic_attribute_lookup_nodes(
        ast.parse(module_alias_mutation),
        binding_tree,
        alias_trees=(imported_alias_tree,),
    )

    ordinary_mapping_lookup = ast.parse(
        "def select_handler(bindings, dispatch_adapter):\n    return bindings.get(dispatch_adapter)\n"
    )
    assert not _dynamic_attribute_lookup_nodes(ordinary_mapping_lookup)


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
