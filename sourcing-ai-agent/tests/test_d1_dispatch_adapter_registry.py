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


_EXPECTED_DISPATCH_SELECTOR_SOURCE = """
def _dispatch_operation_run_from_records(
    self,
    *,
    operation_run: dict[str, Any],
    action: dict[str, Any],
    actor: str,
) -> dict[str, Any]:
    action_type = str(action.get("action_type") or "").strip()
    try:
        action_spec = DEFAULT_ACTION_REGISTRY.spec_for(action_type)
    except KeyError:
        action_spec = None
    dispatch_adapter = str(action_spec.dispatch_adapter or "").strip() if action_spec is not None else ""
    dispatch_handler = self._operation_dispatch_adapter_bindings().get(dispatch_adapter)
    if dispatch_handler is not None:
        return self._operation_run_control_response_record(
            dispatch_handler(
                operation_run=operation_run,
                action=action,
                actor=actor,
            )
        )
    return self._operation_run_control_response_record(
        {
            "status": "unsupported",
            "reason": f"operation action {action_type!r} has no W9b owner adapter",
            "operation_run": operation_run,
            "action": action,
            "module_state_mutated": False,
            "contract": "w9_operation_run_dispatch_v1",
        }
    )
"""

_EXPECTED_DISPATCH_BINDING_SOURCE = """
def _operation_dispatch_adapter_bindings(self) -> dict[str, Callable[..., dict[str, Any]]]:
    return {
        DISPATCH_ADAPTER_PROJECTION_READ: self._dispatch_projection_read_operation,
        DISPATCH_ADAPTER_PERSON_PUBLIC_WEB: self._dispatch_person_public_web_enrichment_operation,
        DISPATCH_ADAPTER_EXPORT: self._dispatch_export_candidates_operation,
        DISPATCH_ADAPTER_AGENT_CALLABLE_WORKFLOW_COMMAND: (
            self._dispatch_agent_callable_workflow_command_operation
        ),
        DISPATCH_ADAPTER_CRM_WRITER: self._dispatch_crm_writer_operation,
    }
"""

_EXPECTED_SELECTOR_BODY_DEPENDENCIES = frozenset(
    {
        "DEFAULT_ACTION_REGISTRY",
        "KeyError",
        "action",
        "action_spec",
        "action_type",
        "actor",
        "dispatch_adapter",
        "dispatch_handler",
        "operation_run",
        "self",
        "str",
    }
)
_EXPECTED_BINDING_BODY_DEPENDENCIES = frozenset(
    {
        "DISPATCH_ADAPTER_AGENT_CALLABLE_WORKFLOW_COMMAND",
        "DISPATCH_ADAPTER_CRM_WRITER",
        "DISPATCH_ADAPTER_EXPORT",
        "DISPATCH_ADAPTER_PERSON_PUBLIC_WEB",
        "DISPATCH_ADAPTER_PROJECTION_READ",
        "self",
    }
)


def _named_function(tree: ast.AST, function_name: str) -> ast.FunctionDef | None:
    matches = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == function_name]
    return matches[0] if len(matches) == 1 else None


def _matches_canonical_function_shape(
    tree: ast.AST,
    *,
    function_name: str,
    expected_source: str,
) -> bool:
    actual = _named_function(tree, function_name)
    expected = _named_function(ast.parse(textwrap.dedent(expected_source)), function_name)
    if actual is None or expected is None:
        return False
    return ast.dump(actual, include_attributes=False) == ast.dump(expected, include_attributes=False)


def _function_body_dependencies(tree: ast.AST, function_name: str) -> frozenset[str]:
    function = _named_function(tree, function_name)
    if function is None:
        return frozenset()
    body_tree = ast.Module(body=function.body, type_ignores=[])
    return frozenset(
        node.id for node in ast.walk(body_tree) if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load)
    )


def _selector_is_canonical(tree: ast.AST) -> bool:
    return (
        _matches_canonical_function_shape(
            tree,
            function_name="_dispatch_operation_run_from_records",
            expected_source=_EXPECTED_DISPATCH_SELECTOR_SOURCE,
        )
        and _function_body_dependencies(tree, "_dispatch_operation_run_from_records")
        == _EXPECTED_SELECTOR_BODY_DEPENDENCIES
    )


def _binding_is_canonical(tree: ast.AST) -> bool:
    return (
        _matches_canonical_function_shape(
            tree,
            function_name="_operation_dispatch_adapter_bindings",
            expected_source=_EXPECTED_DISPATCH_BINDING_SOURCE,
        )
        and _function_body_dependencies(tree, "_operation_dispatch_adapter_bindings")
        == _EXPECTED_BINDING_BODY_DEPENDENCIES
    )


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
    assert _selector_is_canonical(dispatch_tree)
    assert _binding_is_canonical(binding_tree)
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
    assert not _selector_is_canonical(literal_branch_tree)
    assert _registered_action_literals(literal_branch_tree, registered_action_types) == {"search_projection"}
    assert _action_type_control_flow_nodes(literal_branch_tree)

    qualified_action_mutation = dispatch_source.replace(
        '    action_type = str(action.get("action_type") or "").strip()\n',
        '    action_type = str(action.get("action_type") or "").strip()\n'
        "    hidden_action = orchestrator_module.ACTION_ADD_CRM_NOTE\n",
        1,
    )
    assert qualified_action_mutation != dispatch_source
    qualified_action_tree = ast.parse(qualified_action_mutation)
    assert not _selector_is_canonical(qualified_action_tree)
    assert _action_constant_references(qualified_action_tree) == {"ACTION_ADD_CRM_NOTE"}

    raw_action_control_mutation = dispatch_source.replace(
        '    action_type = str(action.get("action_type") or "").strip()\n',
        '    action_type = str(action.get("action_type") or "").strip()\n'
        '    if action.get("action_type"):\n'
        "        return {}\n",
        1,
    )
    assert raw_action_control_mutation != dispatch_source
    raw_action_control_tree = ast.parse(raw_action_control_mutation)
    assert not _selector_is_canonical(raw_action_control_tree)
    assert _action_type_control_flow_nodes(raw_action_control_tree)

    action_alias_mutations = {
        "module_and_local_alias": (
            "    routed_action_type = action_type\n"
            "    if routed_action_type == _D1B_HIDDEN_ACTION:\n"
            "        operation_run = {**operation_run}\n"
        ),
        "multi_hop_local_alias": (
            "    first_action_alias = action_type\n"
            "    second_action_alias = first_action_alias\n"
            "    if second_action_alias == _D1B_HIDDEN_ACTION:\n"
            "        operation_run = {**operation_run}\n"
        ),
    }
    for mutation_name, insertion in action_alias_mutations.items():
        mutation = dispatch_source.replace(
            '    action_type = str(action.get("action_type") or "").strip()\n',
            '    action_type = str(action.get("action_type") or "").strip()\n' + insertion,
            1,
        )
        assert mutation != dispatch_source, mutation_name
        mutation_tree = ast.parse(mutation)
        assert not _selector_is_canonical(mutation_tree), mutation_name
        assert not _action_constant_references(mutation_tree), mutation_name
        assert not _registered_action_literals(mutation_tree, registered_action_types), mutation_name
        assert not _action_type_control_flow_nodes(mutation_tree), mutation_name
        assert not _function_body_dependencies(mutation_tree, "_dispatch_operation_run_from_records").issubset(
            _EXPECTED_SELECTOR_BODY_DEPENDENCIES
        ), mutation_name

    handler_lookup_line = "    dispatch_handler = self._operation_dispatch_adapter_bindings().get(dispatch_adapter)\n"
    dynamic_lookup_mutations = {
        "qualified_builtin": "    dispatch_handler = builtins.getattr(self, dispatch_adapter)\n",
        "assignment_alias": (
            "    resolve_handler = getattr\n    dispatch_handler = resolve_handler(self, dispatch_adapter)\n"
        ),
        "assignment_alias_chain": (
            "    first_resolver = getattr\n"
            "    second_resolver = first_resolver\n"
            "    dispatch_handler = second_resolver(self, dispatch_adapter)\n"
        ),
        "local_import_alias": (
            "    from builtins import getattr as resolve_handler\n"
            "    dispatch_handler = resolve_handler(self, dispatch_adapter)\n"
        ),
        "dunder_getattribute": "    dispatch_handler = self.__getattribute__(dispatch_adapter)\n",
        "qualified_attrgetter": "    dispatch_handler = operator.attrgetter(dispatch_adapter)(self)\n",
        "qualified_getattr_static": ("    dispatch_handler = inspect.getattr_static(self, dispatch_adapter)\n"),
        "vars_subscript": "    dispatch_handler = vars(self)[dispatch_adapter]\n",
        "vars_namespace_alias": (
            "    attribute_namespace = vars(self)\n    dispatch_handler = attribute_namespace[dispatch_adapter]\n"
        ),
        "dunder_dict_subscript": "    dispatch_handler = self.__dict__[dispatch_adapter]\n",
    }
    for mutation_name, replacement in dynamic_lookup_mutations.items():
        mutation = dispatch_source.replace(handler_lookup_line, replacement, 1)
        assert mutation != dispatch_source, mutation_name
        mutation_tree = ast.parse(mutation)
        assert not _selector_is_canonical(mutation_tree), mutation_name
        assert _dynamic_attribute_lookup_nodes(mutation_tree, binding_tree), mutation_name

    module_alias_mutation = dispatch_source.replace(
        handler_lookup_line,
        "    dispatch_handler = imported_handler_lookup(self, dispatch_adapter)\n",
        1,
    )
    imported_alias_tree = ast.parse("from builtins import getattr as imported_handler_lookup\n")
    assert module_alias_mutation != dispatch_source
    module_alias_mutation_tree = ast.parse(module_alias_mutation)
    assert not _selector_is_canonical(module_alias_mutation_tree)
    assert _dynamic_attribute_lookup_nodes(
        module_alias_mutation_tree,
        binding_tree,
        alias_trees=(imported_alias_tree,),
    )

    binding_method_references = {
        "self._dispatch_projection_read_operation": 'hidden_resolver(self, "_dispatch_projection_read_operation")',
        "self._dispatch_person_public_web_enrichment_operation": (
            'hidden_resolver(self, "_dispatch_person_public_web_enrichment_operation")'
        ),
        "self._dispatch_export_candidates_operation": (
            'hidden_resolver(self, "_dispatch_export_candidates_operation")'
        ),
        "self._dispatch_agent_callable_workflow_command_operation": (
            'hidden_resolver(self, "_dispatch_agent_callable_workflow_command_operation")'
        ),
        "self._dispatch_crm_writer_operation": 'hidden_resolver(self, "_dispatch_crm_writer_operation")',
    }
    helper_binding_source = binding_source
    for direct_reference, hidden_reference in binding_method_references.items():
        helper_binding_source = helper_binding_source.replace(direct_reference, hidden_reference)
    assert helper_binding_source != binding_source
    helper_binding_tree = ast.parse(helper_binding_source)
    assert not _binding_is_canonical(helper_binding_tree)
    assert _function_body_dependencies(
        helper_binding_tree, "_operation_dispatch_adapter_bindings"
    ) - _EXPECTED_BINDING_BODY_DEPENDENCIES == {"hidden_resolver"}

    helper_indirection_modules = {
        "top_level_getattr_helper": (
            "def hidden_resolver(instance, name):\n    return getattr(instance, name)\n\n" + helper_binding_source
        ),
        "top_level_helper_chain": (
            "def first_resolver(instance, name):\n"
            "    return getattr(instance, name)\n\n"
            "def hidden_resolver(instance, name):\n"
            "    return first_resolver(instance, name)\n\n" + helper_binding_source
        ),
        "module_callable_alias": ("hidden_resolver = getattr\n\n" + helper_binding_source),
    }
    for mutation_name, module_source in helper_indirection_modules.items():
        module_tree = ast.parse(module_source)
        assert not _binding_is_canonical(module_tree), mutation_name
        assert not _function_body_dependencies(module_tree, "_operation_dispatch_adapter_bindings").issubset(
            _EXPECTED_BINDING_BODY_DEPENDENCIES
        ), mutation_name

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
