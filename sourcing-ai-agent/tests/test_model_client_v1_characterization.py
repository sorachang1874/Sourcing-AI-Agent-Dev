from __future__ import annotations

import ast
import json
from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import sourcing_agent.model_provider as model_provider_module
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import (
    CRM_PUBLIC_WEB_PRODUCT_MODEL,
    OpenAICompatibleChatModelClient,
    OpenAIModelCallResult,
    OpenAIModelUsage,
    QwenResponsesModelClient,
    ScriptedLivePlanningModelClient,
    _reset_model_provider_circuits_for_tests,
)
from sourcing_agent.settings import ModelProviderSettings, QwenSettings

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MODEL_PROVIDER_PATH = SOURCE_ROOT / "model_provider.py"


PROTOCOL_METHOD_CONTRACTS = {
    "summarize": ("sync", "(self, request: JobRequest, matches: list[dict], total_matches: int) -> str", ()),
    "normalize_request": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "normalize_spreadsheet_contacts": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "normalize_review_instruction": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "normalize_refinement_instruction": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "interpret_intent": ("sync", "(self, request: JobRequest, draft_plan: dict[str, Any]) -> str", ()),
    "draft_intent_brief": (
        "sync",
        "(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]",
        (),
    ),
    "plan_search_strategy": (
        "sync",
        "(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]",
        (),
    ),
    "analyze_page_asset": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "analyze_public_web_candidate_signals": (
        "sync",
        "(self, payload: dict[str, Any]) -> dict[str, Any]",
        (),
    ),
    "judge_company_equivalence": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "judge_profile_membership": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "synthesize_manual_review": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "evaluate_outreach_profile": ("sync", "(self, payload: dict[str, Any]) -> dict[str, Any]", ()),
    "provider_name": ("sync", "(self) -> str", ()),
    "supports_outreach_ai_verification": ("sync", "(self) -> bool", ()),
    "healthcheck": ("sync", "(self) -> dict[str, Any]", ()),
}


CONCRETE_PROTOCOL_OVERRIDES = {
    "DeterministicModelClient": frozenset(PROTOCOL_METHOD_CONTRACTS),
    "OfflineModelClient": frozenset({"provider_name", "healthcheck"}),
    "ScriptedLivePlanningModelClient": frozenset(
        {
            "normalize_request",
            "normalize_review_instruction",
            "normalize_refinement_instruction",
            "interpret_intent",
            "draft_intent_brief",
            "plan_search_strategy",
            "provider_name",
            "supports_outreach_ai_verification",
            "healthcheck",
        }
    ),
    "QwenResponsesModelClient": frozenset(PROTOCOL_METHOD_CONTRACTS),
    "OpenAICompatibleChatModelClient": frozenset(PROTOCOL_METHOD_CONTRACTS),
}


SCRIPTED_LIVE_DELEGATED_METHODS = frozenset(
    {
        "normalize_request",
        "normalize_review_instruction",
        "normalize_refinement_instruction",
        "interpret_intent",
        "draft_intent_brief",
        "plan_search_strategy",
    }
)


MODEL_CLIENT_CONSUMER_MODULES = frozenset(
    {
        "acquisition.py",
        "asset_reuse_audit.py",
        "cli.py",
        "company_asset_completion.py",
        "company_asset_supplement.py",
        "connectors.py",
        "criteria_evolution.py",
        "crm_public_web_owner.py",
        "document_extraction.py",
        "enrichment.py",
        "excel_intake.py",
        "excel_intake_owner.py",
        "exploratory_enrichment.py",
        "manual_review_resolution.py",
        "manual_review_synthesis.py",
        "orchestrator.py",
        "outreach_layering.py",
        "planning.py",
        "post_acquisition_refinement.py",
        "public_web_runtime_core.py",
        "public_web_search.py",
        "review_plan_instructions.py",
        "search_planning.py",
        "seed_discovery.py",
        "snapshot_materializer.py",
    }
)


MODEL_CLIENT_CALL_POINTS = Counter(
    {
        ("connectors.py", "_resolve_company_identity_from_observed_candidates", "judge_company_equivalence"): 1,
        ("criteria_evolution.py", "recompile_after_feedback", "provider_name"): 1,
        ("document_extraction.py", "analyze_remote_document", "analyze_page_asset"): 1,
        ("enrichment.py", "_ai_company_equivalence_matches", "judge_company_equivalence"): 1,
        ("enrichment.py", "_review_profile_membership", "judge_profile_membership"): 1,
        ("excel_intake.py", "prepare_contacts", "normalize_spreadsheet_contacts"): 1,
        ("manual_review_synthesis.py", "compile_manual_review_synthesis", "provider_name"): 1,
        ("manual_review_synthesis.py", "compile_manual_review_synthesis", "synthesize_manual_review"): 1,
        ("orchestrator.py", "healthcheck_model", "healthcheck"): 1,
        ("orchestrator.py", "_prepare_request_payload_with_diagnostics", "normalize_request"): 1,
        ("orchestrator.py", "_run_outreach_layering_after_acquisition", "supports_outreach_ai_verification"): 1,
        ("orchestrator.py", "_execute_retrieval", "provider_name"): 1,
        ("orchestrator.py", "_execute_retrieval", "summarize"): 2,
        ("orchestrator.py", "_persist_criteria_artifacts", "provider_name"): 1,
        ("outreach_layering.py", "_evaluate_candidate_with_model", "evaluate_outreach_profile"): 1,
        ("planning.py", "build_sourcing_plan", "interpret_intent"): 2,
        ("planning.py", "_build_intent_brief", "draft_intent_brief"): 1,
        ("post_acquisition_refinement.py", "compile_refinement_patch_from_instruction", "provider_name"): 1,
        (
            "post_acquisition_refinement.py",
            "compile_refinement_patch_from_instruction",
            "normalize_refinement_instruction",
        ): 1,
        ("public_web_search.py", "run_public_web_candidate_adjudication", "provider_name"): 2,
        (
            "public_web_search.py",
            "run_public_web_candidate_adjudication",
            "analyze_public_web_candidate_signals",
        ): 1,
        ("review_plan_instructions.py", "compile_review_payload_from_instruction", "provider_name"): 1,
        (
            "review_plan_instructions.py",
            "compile_review_payload_from_instruction",
            "normalize_review_instruction",
        ): 1,
        ("search_planning.py", "compile_search_strategy", "provider_name"): 1,
        ("search_planning.py", "compile_search_strategy", "plan_search_strategy"): 1,
        ("seed_discovery.py", "_analyze_public_media_results", "analyze_page_asset"): 1,
    }
)


def _class_node(tree: ast.Module, class_name: str) -> ast.ClassDef:
    matches = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(matches) == 1, f"expected exactly one class {class_name}, found {len(matches)}"
    return matches[0]


def _annotation(annotation: ast.expr | None) -> str:
    return ast.unparse(annotation) if annotation is not None else ""


def _argument(arg: ast.arg, default: ast.expr | None) -> str:
    rendered = arg.arg
    if arg.annotation is not None:
        rendered += f": {_annotation(arg.annotation)}"
    if default is not None:
        rendered += f" = {ast.unparse(default)}"
    return rendered


def _canonical_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    positional = [*node.args.posonlyargs, *node.args.args]
    defaults: list[ast.expr | None] = [None] * (len(positional) - len(node.args.defaults)) + list(node.args.defaults)
    parts = [_argument(arg, default) for arg, default in zip(positional, defaults, strict=True)]
    if node.args.posonlyargs:
        parts.insert(len(node.args.posonlyargs), "/")
    if node.args.vararg is not None:
        parts.append(f"*{_argument(node.args.vararg, None)}")
    elif node.args.kwonlyargs:
        parts.append("*")
    parts.extend(
        _argument(arg, default) for arg, default in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True)
    )
    if node.args.kwarg is not None:
        parts.append(f"**{_argument(node.args.kwarg, None)}")
    return f"({', '.join(parts)}) -> {_annotation(node.returns)}"


def _canonical_method_contract(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
) -> tuple[str, str, tuple[str, ...]]:
    return (
        "async" if isinstance(node, ast.AsyncFunctionDef) else "sync",
        _canonical_signature(node),
        tuple(ast.unparse(decorator) for decorator in node.decorator_list),
    )


def _protocol_surface(tree: ast.Module) -> dict[str, tuple[str, str, tuple[str, ...]]]:
    protocol = _class_node(tree, "ModelClient")
    return {
        node.name: _canonical_method_contract(node)
        for node in protocol.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_")
    }


def _protocol_override_contracts(
    tree: ast.Module,
    class_name: str,
) -> dict[str, tuple[str, str, tuple[str, ...]]]:
    class_definition = _class_node(tree, class_name)
    return {
        node.name: _canonical_method_contract(node)
        for node in class_definition.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in PROTOCOL_METHOD_CONTRACTS
    }


def _symbol_tail(node: ast.AST | None) -> str:
    dotted = _dotted_name(node)
    return dotted.rsplit(".", maxsplit=1)[-1] if dotted else ""


def _concrete_client_population(tree: ast.Module) -> frozenset[str]:
    protocol_methods = set(_protocol_surface(tree))
    class_definitions = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    concrete = {
        name
        for name, class_definition in class_definitions.items()
        if name != "ModelClient"
        and protocol_methods
        <= {node.name for node in class_definition.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
    }
    while True:
        inherited = {
            name
            for name, class_definition in class_definitions.items()
            if name != "ModelClient" and any(_symbol_tail(base) in concrete for base in class_definition.bases)
        }
        expanded = concrete | inherited
        if expanded == concrete:
            return frozenset(concrete)
        concrete = expanded


def _factory_return_population(tree: ast.Module) -> frozenset[str]:
    factory = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "build_model_client"
    )
    return frozenset(
        _symbol_tail(node.value.func)
        for node in _scope_nodes(factory)
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Call)
    )


def _delegated_return_target(tree: ast.Module, method_name: str) -> str:
    class_definition = _class_node(tree, "ScriptedLivePlanningModelClient")
    method = next(
        node
        for node in class_definition.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method_name
    )
    assert len(method.body) == 1 and isinstance(method.body[0], ast.Return)
    returned = method.body[0].value
    assert isinstance(returned, ast.Call) and isinstance(returned.func, ast.Attribute)
    delegate = returned.func.value
    assert isinstance(delegate, ast.Attribute) and isinstance(delegate.value, ast.Name)
    assert delegate.value.id == "self" and delegate.attr == "delegate"
    return returned.func.attr


def _direct_delegate_return_targets(tree: ast.Module) -> dict[str, str]:
    class_definition = _class_node(tree, "ScriptedLivePlanningModelClient")
    targets: dict[str, str] = {}
    for method in class_definition.body:
        if not isinstance(method, (ast.FunctionDef, ast.AsyncFunctionDef)) or method.name.startswith("_"):
            continue
        if len(method.body) != 1 or not isinstance(method.body[0], ast.Return):
            continue
        returned = method.body[0].value
        if not isinstance(returned, ast.Call) or not isinstance(returned.func, ast.Attribute):
            continue
        delegate = returned.func.value
        if (
            isinstance(delegate, ast.Attribute)
            and isinstance(delegate.value, ast.Name)
            and delegate.value.id == "self"
            and delegate.attr == "delegate"
        ):
            targets[method.name] = returned.func.attr
    return targets


def _dotted_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
    ):
        prefix = _dotted_name(node.args[0])
        return f"{prefix}.{node.args[1].value}" if prefix else node.args[1].value
    return ""


def _model_provider_imports(
    tree: ast.Module,
    concrete: frozenset[str],
) -> tuple[dict[str, str], frozenset[str]]:
    imported_symbols: dict[str, str] = {}
    module_aliases: set[str] = set()
    recognized = {"ModelClient", "build_model_client", *concrete}
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and str(node.module or "").endswith("model_provider"):
            for imported in node.names:
                if imported.name in recognized:
                    imported_symbols[imported.asname or imported.name] = imported.name
        elif isinstance(node, ast.Import):
            for imported in node.names:
                if imported.name.endswith("model_provider"):
                    module_aliases.add(imported.asname or imported.name.split(".")[-1])
    return imported_symbols, frozenset(module_aliases)


def _resolved_symbol(
    node: ast.AST | None,
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
) -> str:
    if isinstance(node, ast.Name):
        return imported_symbols.get(node.id, node.id)
    if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id in module_aliases:
        return node.attr
    return _symbol_tail(node)


def _local_model_protocols(tree: ast.Module, protocol_methods: frozenset[str]) -> frozenset[str]:
    protocols: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or not any(_symbol_tail(base) == "Protocol" for base in node.bases):
            continue
        public_methods = {
            item.name
            for item in node.body
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and not item.name.startswith("_")
        }
        if public_methods and public_methods <= protocol_methods:
            protocols.add(node.name)
    return frozenset(protocols)


def _annotation_mentions_model(
    annotation: ast.expr | None,
    *,
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    model_types: frozenset[str],
) -> bool:
    if annotation is None:
        return False
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            annotation = ast.parse(annotation.value, mode="eval").body
        except SyntaxError:
            return False
    return any(
        isinstance(node, (ast.Name, ast.Attribute))
        and _resolved_symbol(node, imported_symbols, module_aliases) in model_types
        for node in ast.walk(annotation)
    )


def _local_model_helpers(
    tree: ast.Module,
    *,
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    concrete: frozenset[str],
    model_types: frozenset[str],
) -> frozenset[str]:
    helpers: set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if _annotation_mentions_model(
            node.returns,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            model_types=model_types,
        ) or any(
            isinstance(item, ast.Return)
            and _is_constructor_or_factory(
                item.value,
                imported_symbols=imported_symbols,
                module_aliases=module_aliases,
                concrete=concrete,
            )
            for item in _scope_nodes(node)
        ):
            helpers.add(node.name)
    return frozenset(helpers)


def _scope_nodes(root: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.AST]:
    nodes: list[ast.AST] = []
    pending = list(ast.iter_child_nodes(root))
    while pending:
        node = pending.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        nodes.append(node)
        pending.extend(ast.iter_child_nodes(node))
    return nodes


def _target_keys(node: ast.AST | None) -> tuple[str, ...]:
    if isinstance(node, (ast.Name, ast.Attribute)):
        dotted = _dotted_name(node)
        return (dotted,) if dotted else ()
    if isinstance(node, (ast.Tuple, ast.List)):
        return tuple(key for item in node.elts for key in _target_keys(item))
    return ()


def _is_constructor_or_factory(
    node: ast.AST | None,
    *,
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    concrete: frozenset[str],
) -> bool:
    return isinstance(node, ast.Call) and _resolved_symbol(node.func, imported_symbols, module_aliases) in {
        "build_model_client",
        *concrete,
    }


def _is_model_expression(
    node: ast.AST | None,
    *,
    receivers: set[str],
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    concrete: frozenset[str],
) -> bool:
    if isinstance(node, (ast.Name, ast.Attribute)) and _dotted_name(node) in receivers:
        return True
    if _is_constructor_or_factory(
        node,
        imported_symbols=imported_symbols,
        module_aliases=module_aliases,
        concrete=concrete,
    ):
        return True
    if isinstance(node, ast.IfExp):
        return _is_model_expression(
            node.body,
            receivers=receivers,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            concrete=concrete,
        ) or _is_model_expression(
            node.orelse,
            receivers=receivers,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            concrete=concrete,
        )
    if isinstance(node, ast.BoolOp):
        return any(
            _is_model_expression(
                value,
                receivers=receivers,
                imported_symbols=imported_symbols,
                module_aliases=module_aliases,
                concrete=concrete,
            )
            for value in node.values
        )
    return isinstance(node, ast.NamedExpr) and _is_model_expression(
        node.value,
        receivers=receivers,
        imported_symbols=imported_symbols,
        module_aliases=module_aliases,
        concrete=concrete,
    )


def _method_reference(
    node: ast.AST | None,
    *,
    receivers: set[str],
    callable_aliases: dict[str, str],
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    concrete: frozenset[str],
    protocol_methods: frozenset[str],
) -> str:
    if isinstance(node, (ast.Name, ast.Attribute)):
        alias = callable_aliases.get(_dotted_name(node), "")
        if alias:
            return alias
    if (
        isinstance(node, ast.Attribute)
        and node.attr in protocol_methods
        and _is_model_expression(
            node.value,
            receivers=receivers,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            concrete=concrete,
        )
    ):
        return node.attr
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "getattr"
        and len(node.args) >= 2
        and isinstance(node.args[1], ast.Constant)
        and isinstance(node.args[1].value, str)
        and node.args[1].value in protocol_methods
        and _is_model_expression(
            node.args[0],
            receivers=receivers,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            concrete=concrete,
        )
    ):
        return node.args[1].value
    return ""


def _class_model_receivers(
    tree: ast.Module,
    *,
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    concrete: frozenset[str],
    model_types: frozenset[str],
) -> dict[str, frozenset[str]]:
    result: dict[str, frozenset[str]] = {}
    for class_definition in (node for node in tree.body if isinstance(node, ast.ClassDef)):
        attributes: set[str] = set()
        for method in (
            node for node in class_definition.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        ):
            receivers = {
                arg.arg
                for arg in [*method.args.posonlyargs, *method.args.args, *method.args.kwonlyargs]
                if _annotation_mentions_model(
                    arg.annotation,
                    imported_symbols=imported_symbols,
                    module_aliases=module_aliases,
                    model_types=model_types,
                )
            }
            scope = _scope_nodes(method)
            changed = True
            while changed:
                changed = False
                for node in scope:
                    value: ast.AST | None = None
                    targets: tuple[str, ...] = ()
                    annotation: ast.expr | None = None
                    if isinstance(node, ast.Assign):
                        value = node.value
                        targets = tuple(key for target in node.targets for key in _target_keys(target))
                    elif isinstance(node, ast.AnnAssign):
                        value = node.value
                        targets = _target_keys(node.target)
                        annotation = node.annotation
                    if not targets:
                        continue
                    is_model = _annotation_mentions_model(
                        annotation,
                        imported_symbols=imported_symbols,
                        module_aliases=module_aliases,
                        model_types=model_types,
                    ) or _is_model_expression(
                        value,
                        receivers=receivers,
                        imported_symbols=imported_symbols,
                        module_aliases=module_aliases,
                        concrete=concrete,
                    )
                    if is_model:
                        before = len(receivers)
                        receivers.update(targets)
                        changed |= len(receivers) != before
            attributes.update(key for key in receivers if key.startswith("self."))
        result[class_definition.name] = frozenset(attributes)
    return result


def _function_call_points(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    *,
    class_name: str,
    class_receivers: dict[str, frozenset[str]],
    imported_symbols: dict[str, str],
    module_aliases: frozenset[str],
    concrete: frozenset[str],
    model_types: frozenset[str],
    protocol_methods: frozenset[str],
) -> Counter[str]:
    receivers = set(class_receivers.get(class_name, ()))
    receivers.update(
        arg.arg
        for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
        if _annotation_mentions_model(
            arg.annotation,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            model_types=model_types,
        )
    )
    callable_aliases: dict[str, str] = {}
    scope = _scope_nodes(node)
    changed = True
    while changed:
        changed = False
        for item in scope:
            value: ast.AST | None = None
            targets: tuple[str, ...] = ()
            annotation: ast.expr | None = None
            if isinstance(item, ast.Assign):
                value = item.value
                targets = tuple(key for target in item.targets for key in _target_keys(target))
            elif isinstance(item, ast.AnnAssign):
                value = item.value
                targets = _target_keys(item.target)
                annotation = item.annotation
            elif isinstance(item, ast.NamedExpr):
                value = item.value
                targets = _target_keys(item.target)
            if not targets:
                continue
            if _annotation_mentions_model(
                annotation,
                imported_symbols=imported_symbols,
                module_aliases=module_aliases,
                model_types=model_types,
            ) or _is_model_expression(
                value,
                receivers=receivers,
                imported_symbols=imported_symbols,
                module_aliases=module_aliases,
                concrete=concrete,
            ):
                before = len(receivers)
                receivers.update(targets)
                changed |= len(receivers) != before
            method = _method_reference(
                value,
                receivers=receivers,
                callable_aliases=callable_aliases,
                imported_symbols=imported_symbols,
                module_aliases=module_aliases,
                concrete=concrete,
                protocol_methods=protocol_methods,
            )
            if method:
                before = len(callable_aliases)
                callable_aliases.update({target: method for target in targets})
                changed |= len(callable_aliases) != before

    calls: Counter[str] = Counter()
    for item in scope:
        if not isinstance(item, ast.Call):
            continue
        method = _method_reference(
            item.func,
            receivers=receivers,
            callable_aliases=callable_aliases,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            concrete=concrete,
            protocol_methods=protocol_methods,
        )
        if method:
            calls[method] += 1
    return calls


def _has_non_null_model_handoff(tree: ast.Module) -> bool:
    return any(
        isinstance(node, ast.Call)
        and any(
            keyword.arg == "model_client"
            and not (isinstance(keyword.value, ast.Constant) and keyword.value.value is None)
            for keyword in node.keywords
        )
        for node in ast.walk(tree)
    )


def _analyze_module(
    tree: ast.Module,
    *,
    module_name: str,
    concrete: frozenset[str],
    protocol_methods: frozenset[str],
) -> tuple[bool, Counter[tuple[str, str, str]]]:
    imported_symbols, module_aliases = _model_provider_imports(tree, concrete)
    local_protocols = _local_model_protocols(tree, protocol_methods)
    model_types = frozenset({"ModelClient", *concrete, *local_protocols})
    model_factories = concrete | _local_model_helpers(
        tree,
        imported_symbols=imported_symbols,
        module_aliases=module_aliases,
        concrete=concrete,
        model_types=model_types,
    )
    class_receivers = _class_model_receivers(
        tree,
        imported_symbols=imported_symbols,
        module_aliases=module_aliases,
        concrete=model_factories,
        model_types=model_types,
    )
    calls: Counter[tuple[str, str, str]] = Counter()
    for top_level in tree.body:
        if isinstance(top_level, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function_calls = _function_call_points(
                top_level,
                class_name="",
                class_receivers=class_receivers,
                imported_symbols=imported_symbols,
                module_aliases=module_aliases,
                concrete=model_factories,
                model_types=model_types,
                protocol_methods=protocol_methods,
            )
            calls.update((module_name, top_level.name, method) for method in function_calls.elements())
        elif isinstance(top_level, ast.ClassDef):
            for method in (
                node for node in top_level.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            ):
                function_calls = _function_call_points(
                    method,
                    class_name=top_level.name,
                    class_receivers=class_receivers,
                    imported_symbols=imported_symbols,
                    module_aliases=module_aliases,
                    concrete=model_factories,
                    model_types=model_types,
                    protocol_methods=protocol_methods,
                )
                calls.update((module_name, method.name, facade_method) for facade_method in function_calls.elements())
    constructs_client = any(
        _is_constructor_or_factory(
            node,
            imported_symbols=imported_symbols,
            module_aliases=module_aliases,
            concrete=model_factories,
        )
        for node in ast.walk(tree)
    )
    imports_facade = any(
        symbol in {"ModelClient", "build_model_client", *concrete} for symbol in imported_symbols.values()
    )
    is_consumer = bool(
        imports_facade or local_protocols or constructs_client or calls or _has_non_null_model_handoff(tree)
    )
    return is_consumer, calls


def _consumer_inventory() -> tuple[frozenset[str], Counter[tuple[str, str, str]]]:
    provider_tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))
    concrete = _concrete_client_population(provider_tree)
    protocol_methods = frozenset(_protocol_surface(provider_tree))
    modules: set[str] = set()
    calls: Counter[tuple[str, str, str]] = Counter()
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        if path == MODEL_PROVIDER_PATH or "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_name = path.relative_to(SOURCE_ROOT).as_posix()
        is_consumer, module_calls = _analyze_module(
            tree,
            module_name=module_name,
            concrete=concrete,
            protocol_methods=protocol_methods,
        )
        if is_consumer:
            modules.add(module_name)
        calls.update(module_calls)
    return frozenset(modules), calls


class _RequestsResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


class _UrlOpenResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _UrlOpenResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self.body


@pytest.fixture(autouse=True)
def _reset_model_circuits() -> None:
    _reset_model_provider_circuits_for_tests()
    yield
    _reset_model_provider_circuits_for_tests()


def test_model_client_protocol_surface_matches_v1_golden() -> None:
    tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))

    assert _protocol_surface(tree) == PROTOCOL_METHOD_CONTRACTS
    assert len(PROTOCOL_METHOD_CONTRACTS) == 17


def test_concrete_clients_and_factory_returns_preserve_complete_protocol_surface() -> None:
    tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))
    concrete = _concrete_client_population(tree)

    assert concrete == frozenset(CONCRETE_PROTOCOL_OVERRIDES)
    assert _factory_return_population(tree) == concrete
    for class_name in concrete:
        client_class = getattr(model_provider_module, class_name)
        override_contracts = _protocol_override_contracts(tree, class_name)
        expected_overrides = CONCRETE_PROTOCOL_OVERRIDES[class_name]
        assert frozenset(override_contracts) == expected_overrides
        assert override_contracts == {name: PROTOCOL_METHOD_CONTRACTS[name] for name in expected_overrides}
        assert {name for name in PROTOCOL_METHOD_CONTRACTS if callable(getattr(client_class, name, None))} == set(
            PROTOCOL_METHOD_CONTRACTS
        )


def test_factory_return_population_fails_closed_on_unknown_direct_return_mutation() -> None:
    source = MODEL_PROVIDER_PATH.read_text(encoding="utf-8")
    assert "return DeterministicModelClient()" in source
    mutated_tree = ast.parse(
        source.replace("return DeterministicModelClient()", "return object()", 1),
        filename=str(MODEL_PROVIDER_PATH),
    )
    concrete = _concrete_client_population(mutated_tree)
    factory_returns = _factory_return_population(mutated_tree)

    assert "object" in factory_returns
    assert factory_returns != concrete


def test_scripted_live_client_delegates_only_the_six_front_door_planning_methods() -> None:
    tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))

    delegated = {
        method_name: _delegated_return_target(tree, method_name) for method_name in SCRIPTED_LIVE_DELEGATED_METHODS
    }

    assert delegated == {method_name: method_name for method_name in SCRIPTED_LIVE_DELEGATED_METHODS}
    assert _direct_delegate_return_targets(tree) == delegated
    assert SCRIPTED_LIVE_DELEGATED_METHODS < CONCRETE_PROTOCOL_OVERRIDES["ScriptedLivePlanningModelClient"]


class _SpyModelDelegate:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def __getattr__(self, method_name: str):
        if method_name not in PROTOCOL_METHOD_CONTRACTS:
            raise AttributeError(method_name)

        def record(*_args: object, **_kwargs: object) -> Any:
            self.calls.append(method_name)
            if method_name == "provider_name":
                return "spy_delegate"
            if method_name == "supports_outreach_ai_verification":
                return True
            if method_name in {"summarize", "interpret_intent"}:
                return f"spy_{method_name}"
            return {"spy_method": method_name}

        return record


def _scripted_live_runtime_delegate_graph() -> dict[str, tuple[str, ...]]:
    delegate = _SpyModelDelegate()
    client = ScriptedLivePlanningModelClient(delegate, mode="scripted")
    request = JobRequest(raw_user_request="Find researchers", target_company="Example Lab")
    calls = {
        "summarize": lambda: client.summarize(
            request,
            [{"display_name": "A", "score": 1}],
            1,
        ),
        "normalize_request": lambda: client.normalize_request({}),
        "normalize_spreadsheet_contacts": lambda: client.normalize_spreadsheet_contacts({}),
        "normalize_review_instruction": lambda: client.normalize_review_instruction({}),
        "normalize_refinement_instruction": lambda: client.normalize_refinement_instruction({}),
        "interpret_intent": lambda: client.interpret_intent(request, {}),
        "draft_intent_brief": lambda: client.draft_intent_brief(request, {}),
        "plan_search_strategy": lambda: client.plan_search_strategy(request, {}),
        "analyze_page_asset": lambda: client.analyze_page_asset({}),
        "analyze_public_web_candidate_signals": lambda: client.analyze_public_web_candidate_signals({}),
        "judge_company_equivalence": lambda: client.judge_company_equivalence({}),
        "judge_profile_membership": lambda: client.judge_profile_membership({}),
        "synthesize_manual_review": lambda: client.synthesize_manual_review({}),
        "evaluate_outreach_profile": lambda: client.evaluate_outreach_profile({}),
        "provider_name": client.provider_name,
        "supports_outreach_ai_verification": client.supports_outreach_ai_verification,
        "healthcheck": client.healthcheck,
    }
    assert set(calls) == set(PROTOCOL_METHOD_CONTRACTS)
    graph: dict[str, tuple[str, ...]] = {}
    for method_name, invoke in calls.items():
        start = len(delegate.calls)
        invoke()
        graph[method_name] = tuple(delegate.calls[start:])
    return graph


def test_scripted_live_runtime_spy_proves_the_complete_17_method_call_graph() -> None:
    expected = {
        method_name: ((method_name,) if method_name in SCRIPTED_LIVE_DELEGATED_METHODS else ())
        for method_name in PROTOCOL_METHOD_CONTRACTS
    }
    expected["healthcheck"] = ("provider_name",)

    graph = _scripted_live_runtime_delegate_graph()

    assert graph == expected
    assert Counter(method for methods in graph.values() for method in methods) == Counter(
        {**{method: 1 for method in SCRIPTED_LIVE_DELEGATED_METHODS}, "provider_name": 1}
    )


def test_model_client_consumer_inventory_and_call_points_match_v1_golden() -> None:
    modules, calls = _consumer_inventory()

    assert modules == MODEL_CLIENT_CONSUMER_MODULES
    assert calls == MODEL_CLIENT_CALL_POINTS
    assert len(modules) == 25
    assert sum(calls.values()) == 29
    assert {method_name for _, _, method_name in calls} == set(PROTOCOL_METHOD_CONTRACTS)


def test_openai_chat_completions_wire_shape_remains_v1() -> None:
    settings = ModelProviderSettings(
        enabled=True,
        provider_name="characterization_chat",
        api_key="sk-synthetic",
        base_url="https://model-characterization.test/v1",
        model="gpt-characterization",
        api_style="openai_chat_completions",
        timeout_seconds=17,
    )
    client = OpenAICompatibleChatModelClient(settings)
    messages = [{"role": "system", "content": "System"}, {"role": "user", "content": "User"}]
    response = _RequestsResponse(
        {
            "id": "chat-characterization-1",
            "model": settings.model,
            "choices": [{"message": {"content": "OK"}}],
        }
    )

    with patch.object(model_provider_module.requests, "post", return_value=response) as post:
        result = client._call_chat_completions_result(messages, max_tokens=7)

    assert result.text == "OK"
    post.assert_called_once()
    assert post.call_args.args == ("https://model-characterization.test/v1/chat/completions",)
    assert post.call_args.kwargs == {
        "timeout": 17,
        "headers": {
            "Authorization": "Bearer sk-synthetic",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        "json": {
            "model": "gpt-characterization",
            "messages": messages,
            "max_tokens": 32,
            "temperature": 0,
        },
    }


def test_openai_responses_wire_shape_and_role_flattening_remain_v1() -> None:
    settings = ModelProviderSettings(
        enabled=True,
        provider_name="characterization_responses",
        api_key="sk-synthetic",
        base_url="https://model-characterization.test/v1",
        model="gpt-characterization",
        api_style="openai_responses",
        timeout_seconds=19,
    )
    client = OpenAICompatibleChatModelClient(settings)
    messages = [{"role": "system", "content": " System "}, {"role": "user", "content": " User "}]
    response = _RequestsResponse(
        {
            "id": "responses-characterization-1",
            "model": settings.model,
            "output_text": "OK",
        }
    )

    with patch.object(model_provider_module.requests, "post", return_value=response) as post:
        result = client._call_responses_api_result(messages, max_tokens=65)

    assert result.text == "OK"
    post.assert_called_once()
    assert post.call_args.args == ("https://model-characterization.test/v1/responses",)
    assert post.call_args.kwargs == {
        "timeout": 19,
        "headers": {
            "Authorization": "Bearer sk-synthetic",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        },
        "json": {
            "model": "gpt-characterization",
            "input": "system: System\n\nuser: User",
            "max_output_tokens": 65,
            "temperature": 0,
        },
    }


@pytest.mark.parametrize(
    ("max_tokens", "expected_payload"),
    [
        (None, {"model": "qwen-characterization", "input": "Synthetic input"}),
        (
            7,
            {
                "model": "qwen-characterization",
                "input": "Synthetic input",
                "max_output_tokens": 32,
            },
        ),
        (
            65,
            {
                "model": "qwen-characterization",
                "input": "Synthetic input",
                "max_output_tokens": 65,
            },
        ),
    ],
)
def test_qwen_responses_wire_shape_and_urllib_transport_remain_v1(
    max_tokens: int | None,
    expected_payload: dict[str, Any],
) -> None:
    client = QwenResponsesModelClient(
        QwenSettings(
            enabled=True,
            api_key="sk-synthetic",
            base_url="https://qwen-characterization.test/v1",
            model="qwen-characterization",
            timeout_seconds=23,
        )
    )

    with (
        patch.object(
            model_provider_module.request,
            "urlopen",
            return_value=_UrlOpenResponse(
                {"model": "provider-response-identity-is-not-checked-v1", "output_text": "QWEN_OK"}
            ),
        ) as urlopen,
        patch.object(model_provider_module.requests, "post") as requests_post,
    ):
        result = client._call_responses_api("Synthetic input", max_tokens=max_tokens)

    assert result == "QWEN_OK"
    requests_post.assert_not_called()
    urlopen.assert_called_once()
    request_object = urlopen.call_args.args[0]
    assert request_object.full_url == "https://qwen-characterization.test/v1/responses"
    assert request_object.get_method() == "POST"
    assert {key.lower(): value for key, value in request_object.header_items()} == {
        "authorization": "Bearer sk-synthetic",
        "content-type": "application/json",
    }
    assert json.loads(request_object.data.decode("utf-8")) == expected_payload
    assert "temperature" not in expected_payload
    assert urlopen.call_args.kwargs == {"timeout": 23}


def test_unknown_openai_api_style_rejects_before_any_transport() -> None:
    client = OpenAICompatibleChatModelClient(
        ModelProviderSettings(
            enabled=True,
            provider_name="characterization_unknown",
            api_key="sk-synthetic",
            base_url="https://model-characterization.test/v1",
            model="gpt-characterization",
            api_style="unknown_style",
        )
    )

    with (
        patch.object(model_provider_module.requests, "post") as requests_post,
        patch.object(model_provider_module.request, "urlopen") as urlopen,
        pytest.raises(RuntimeError, match="Unsupported OpenAI-compatible api_style"),
    ):
        client._call_prompt_result([{"role": "user", "content": "No transport"}], max_tokens=32)

    requests_post.assert_not_called()
    urlopen.assert_not_called()


def test_generic_response_identity_and_crm_product_model_lock_remain_separate_boundaries() -> None:
    assert model_provider_module._openai_model_identity_failure(
        requested_model="gpt-characterization",
        response_model="gpt-characterization",
    ) == ("", "")
    assert (
        model_provider_module._openai_model_identity_failure(
            requested_model="gpt-characterization",
            response_model="",
        )[0]
        == "model_identity_missing"
    )
    assert (
        model_provider_module._openai_model_identity_failure(
            requested_model="gpt-characterization",
            response_model="gpt-other",
        )[0]
        == "model_identity_mismatch"
    )

    class _CharacterizedClient(OpenAICompatibleChatModelClient):
        def __init__(self, settings: ModelProviderSettings) -> None:
            super().__init__(settings)
            self.prompt_calls = 0

        def _call_prompt_result(
            self,
            messages: list[dict[str, str]],
            *,
            max_tokens: int,
        ) -> OpenAIModelCallResult:
            del messages, max_tokens
            self.prompt_calls += 1
            return OpenAIModelCallResult(
                text='{"target_company":"Anthropic"}',
                requested_model=self.settings.model,
                response_model=self.settings.model,
                usage=OpenAIModelUsage(),
            )

    client = _CharacterizedClient(
        ModelProviderSettings(
            enabled=True,
            provider_name="characterization_identity",
            api_key="sk-synthetic",
            base_url="https://model-characterization.test/v1",
            model="gpt-characterization-not-crm",
            api_style="openai_responses",
        )
    )

    public_web_result = client.analyze_public_web_candidate_signals({})

    assert CRM_PUBLIC_WEB_PRODUCT_MODEL != client.settings.model
    assert client.prompt_calls == 0
    assert public_web_result["fallback_used"] is True
    assert public_web_result["fallback_reason"] == "model_configuration_mismatch"
    assert client.normalize_request({"raw_user_request": "Find Anthropic researchers"}) == {
        "target_company": "Anthropic"
    }
    assert client.prompt_calls == 1


def test_characterization_helpers_detect_semantic_protocol_and_call_mutations() -> None:
    protocol_tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))
    protocol = _class_node(protocol_tree, "ModelClient")
    protocol.body = [
        node
        for node in protocol.body
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) or node.name != "healthcheck"
    ]
    assert _protocol_surface(protocol_tree) != PROTOCOL_METHOD_CONTRACTS

    async_method = ast.parse("async def healthcheck(self) -> dict[str, Any]: ...").body[0]
    decorated_method = ast.parse("@staticmethod\ndef healthcheck(self) -> dict[str, Any]: ...").body[0]
    assert isinstance(async_method, ast.AsyncFunctionDef)
    assert isinstance(decorated_method, ast.FunctionDef)
    assert _canonical_method_contract(async_method) != PROTOCOL_METHOD_CONTRACTS["healthcheck"]
    assert _canonical_method_contract(decorated_method) != PROTOCOL_METHOD_CONTRACTS["healthcheck"]

    provider_tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))
    concrete = _concrete_client_population(provider_tree)
    protocol_methods = frozenset(_protocol_surface(provider_tree))
    source = """
from sourcing_agent.model_provider import DeterministicModelClient, ModelClient

def helper(model_client: ModelClient):
    client = model_client
    direct = client.healthcheck()
    callable_alias = client.healthcheck
    aliased = callable_alias()
    dynamic_alias = getattr(client, "provider_name")
    dynamic = dynamic_alias()
    return direct, aliased, dynamic

def direct_concrete():
    return DeterministicModelClient().healthcheck()

def build_helper():
    return DeterministicModelClient()

def helper_receiver():
    return build_helper().provider_name()
"""

    def analyze(candidate: str) -> Counter[tuple[str, str, str]]:
        _, calls = _analyze_module(
            ast.parse(candidate),
            module_name="synthetic.py",
            concrete=concrete,
            protocol_methods=protocol_methods,
        )
        return calls

    baseline = analyze(source)
    assert baseline == Counter(
        {
            ("synthetic.py", "helper", "healthcheck"): 2,
            ("synthetic.py", "helper", "provider_name"): 1,
            ("synthetic.py", "direct_concrete", "healthcheck"): 1,
            ("synthetic.py", "helper_receiver", "provider_name"): 1,
        }
    )
    assert analyze(source.replace("direct = client.healthcheck()", "direct = client.unknown_method()")) != baseline
    assert analyze(source.replace('getattr(client, "provider_name")', 'getattr(client, "unknown_method")')) != baseline
    assert analyze(source.replace("DeterministicModelClient().healthcheck()", "object().healthcheck()")) != baseline
    assert analyze(source.replace("return DeterministicModelClient()\n", "return object()\n")) != baseline


def test_scripted_runtime_spy_detects_a_new_delegate_side_effect_mutation() -> None:
    original = ScriptedLivePlanningModelClient.analyze_page_asset

    def mutated(self: ScriptedLivePlanningModelClient, payload: dict[str, Any]) -> dict[str, Any]:
        self.delegate.judge_company_equivalence(payload)
        return original(self, payload)

    with patch.object(ScriptedLivePlanningModelClient, "analyze_page_asset", mutated):
        graph = _scripted_live_runtime_delegate_graph()

    assert graph["analyze_page_asset"] == ("judge_company_equivalence",)
