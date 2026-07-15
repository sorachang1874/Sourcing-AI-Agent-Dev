from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from sourcing_agent.operation_runtime import (
    ACTION_FILTER_PROJECTION,
    ACTION_SEARCH_PROJECTION,
    DEFAULT_ACTION_REGISTRY,
    DISPATCH_ADAPTER_PROJECTION_READ,
)
from sourcing_agent.repositories.serving_projection import (
    COLLECTION_AUTHORITATIVE_POINTERS,
    RUN_PROJECTION_LINKS,
    SERVING_PROJECTIONS,
)
from sourcing_agent.serving_projection_writer import ServingProjectionWriter

REPO_ROOT = Path(__file__).resolve().parents[1]
API_PATH = REPO_ROOT / "src" / "sourcing_agent" / "api.py"
ORCHESTRATOR_PATH = REPO_ROOT / "src" / "sourcing_agent" / "orchestrator.py"

PROJECTION_ACTIONS = (ACTION_SEARCH_PROJECTION, ACTION_FILTER_PROJECTION)
PROJECTION_OWNER_COLUMN_CANDIDATES = frozenset(
    {
        "workspace_id",
        "tenant_id",
        "owner_user_id",
        "access_scope",
        "visibility_scope",
    }
)


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    matches = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name]
    assert len(matches) == 1, f"expected one function named {name}, found {len(matches)}"
    return matches[0]


def _class_method(tree: ast.Module, class_name: str, method_name: str) -> ast.FunctionDef:
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name]
    assert len(classes) == 1, f"expected one class named {class_name}, found {len(classes)}"
    matches = [node for node in classes[0].body if isinstance(node, ast.FunctionDef) and node.name == method_name]
    assert len(matches) == 1, f"expected one {class_name}.{method_name}, found {len(matches)}"
    return matches[0]


def _calls_named(node: ast.AST, name: str) -> list[ast.Call]:
    return [
        candidate
        for candidate in ast.walk(node)
        if isinstance(candidate, ast.Call)
        and (
            (isinstance(candidate.func, ast.Name) and candidate.func.id == name)
            or (isinstance(candidate.func, ast.Attribute) and candidate.func.attr == name)
        )
    ]


@pytest.mark.parametrize(
    "descriptor",
    [SERVING_PROJECTIONS, RUN_PROJECTION_LINKS, COLLECTION_AUTHORITATIVE_POINTERS],
    ids=lambda descriptor: descriptor.table,
)
def test_projection_catalog_has_no_physical_workspace_or_access_scope_owner(descriptor: object) -> None:
    column_names = {column.name for column in descriptor.columns}  # type: ignore[attr-defined]
    assert column_names.isdisjoint(PROJECTION_OWNER_COLUMN_CANDIDATES)


@pytest.mark.parametrize(
    "method_name",
    ["publish_run_scope_projection", "publish_collection_authoritative_projection"],
)
def test_projection_writer_cannot_persist_a_workspace_or_access_scope_owner(method_name: str) -> None:
    parameters = set(inspect.signature(getattr(ServingProjectionWriter, method_name)).parameters)
    assert parameters.isdisjoint(PROJECTION_OWNER_COLUMN_CANDIDATES)


def test_generic_operation_submit_does_not_have_an_authenticated_projection_bind_context() -> None:
    api_tree = ast.parse(API_PATH.read_text(encoding="utf-8"), filename=str(API_PATH))
    handler = _function(api_tree, "post_operation_actions")
    identity_calls = _calls_named(handler, "_apply_server_identity")
    assert len(identity_calls) == 1
    identity_keywords = {keyword.arg for keyword in identity_calls[0].keywords}
    assert "workspace" not in identity_keywords
    assert "tenant" not in identity_keywords

    orchestrator_tree = ast.parse(
        ORCHESTRATOR_PATH.read_text(encoding="utf-8"),
        filename=str(ORCHESTRATOR_PATH),
    )
    submit_method = _class_method(orchestrator_tree, "SourcingOrchestrator", "submit_operation_action")
    writer_calls = _calls_named(submit_method, "submit_action")
    assert len(writer_calls) == 1
    writer_keywords = {keyword.arg: keyword.value for keyword in writer_calls[0].keywords}
    assert "owner_bound_target_ref" not in writer_keywords
    assert ast.unparse(writer_keywords["workspace_id"]) == (
        "str(payload.get('workspace_id') or 'default').strip() or 'default'"
    )


@pytest.mark.parametrize("action_type", PROJECTION_ACTIONS)
def test_projection_actions_remain_schema_less_until_projection_scope_owner_is_decided(action_type: str) -> None:
    spec = DEFAULT_ACTION_REGISTRY.spec_for(action_type)
    assert spec.dispatch_adapter == DISPATCH_ADAPTER_PROJECTION_READ
    assert spec.request_schema is None
    assert spec.request_schema_version == ""
    assert spec.request_schema_digest == ""
    assert spec.has_request_schema is False
