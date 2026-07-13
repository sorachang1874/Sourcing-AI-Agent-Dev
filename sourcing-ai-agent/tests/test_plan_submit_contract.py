"""C1b contract/preflight tests for the legacy Plan hydration bridge."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent.plan_submit_contract import (
    LEGACY_PLAN_HYDRATION_OWNER_METHOD,
    LEGACY_PLAN_HYDRATION_QUEUE_METHOD,
    LEGACY_PLAN_HYDRATION_RUN_METHOD,
    LEGACY_PLAN_SUBMIT_HTTP_STATUS,
    LEGACY_PLAN_SUBMIT_RESPONSE_STATUS,
    PLAN_COMPILER_CONTRACT_VERSION,
    PLAN_GENERATION_COMPLETED,
    PLAN_GENERATION_FAILED,
    PLAN_GENERATION_QUEUED,
    PLAN_GENERATION_RUNNING,
    PLAN_GENERATION_TERMINAL_STATUSES,
    PLAN_SUBMIT_IDENTITY_PROVENANCE_SERVER,
    authenticated_plan_history_metadata_owned,
    build_plan_generation,
    build_plan_submit_identity_metadata,
    canonical_plan_compile_request,
    frontend_history_record_is_plan,
    plan_hydration_request_signature,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src" / "sourcing_agent"


class _CallInventory(ast.NodeVisitor):
    def __init__(self) -> None:
        self.function_stack: list[str] = []
        self.if_stack: list[str] = []
        self.attribute_calls: list[dict[str, Any]] = []
        self.hydration_thread_creators: list[dict[str, Any]] = []
        self.hydration_executor_targets: list[dict[str, Any]] = []
        self._alias_scopes: list[dict[str, str]] = [{}]

    def _bind_alias(self, name: str, target: str) -> None:
        normalized_name = str(name or "").strip()
        normalized_target = str(target or "").strip()
        if normalized_name and normalized_target:
            self._alias_scopes[-1][normalized_name] = normalized_target

    def _lookup_alias(self, name: str) -> str:
        for scope in reversed(self._alias_scopes):
            if name in scope:
                return scope[name]
        return name

    def _resolve_callable(self, node: ast.AST | None) -> str:
        if isinstance(node, ast.Attribute):
            return node.attr
        if isinstance(node, ast.Name):
            return self._lookup_alias(node.id)
        if isinstance(node, ast.Call):
            called = self._resolve_callable(node.func)
            if called == "partial" and node.args:
                return self._resolve_callable(node.args[0])
            if called == "getattr" and len(node.args) >= 2 and isinstance(node.args[1], ast.Constant):
                return str(node.args[1].value or "")
        return ""

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._bind_alias(alias.asname or alias.name.split(".")[0], alias.name.split(".")[-1])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self._bind_alias(alias.asname or alias.name, alias.name)

    def visit_Assign(self, node: ast.Assign) -> None:
        resolved = self._resolve_callable(node.value)
        for target in node.targets:
            if isinstance(target, ast.Name):
                self._bind_alias(target.id, resolved)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if isinstance(node.target, ast.Name):
            self._bind_alias(node.target.id, self._resolve_callable(node.value))
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self._alias_scopes.append({})
        self.generic_visit(node)
        self._alias_scopes.pop()
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self._alias_scopes.append({})
        self.generic_visit(node)
        self._alias_scopes.pop()
        self.function_stack.pop()

    def visit_If(self, node: ast.If) -> None:
        self.if_stack.append(ast.unparse(node.test))
        for statement in node.body:
            self.visit(statement)
        self.if_stack.pop()
        for statement in node.orelse:
            self.visit(statement)

    def visit_Call(self, node: ast.Call) -> None:
        called = self._resolve_callable(node.func)
        if called in {
            LEGACY_PLAN_HYDRATION_QUEUE_METHOD,
            LEGACY_PLAN_HYDRATION_RUN_METHOD,
            "plan_workflow",
        }:
            record = {
                "attribute": called,
                "function": self.function_stack[-1] if self.function_stack else "<module>",
                "if_tests": tuple(self.if_stack),
                "lineno": node.lineno,
                "kind": "direct_or_alias_call",
            }
            self.attribute_calls.append(record)

        if called in {"Thread", "start_new_thread"}:
            target = next(
                (keyword.value for keyword in node.keywords if keyword.arg == "target"),
                node.args[0] if called == "start_new_thread" and node.args else None,
            )
            target_name = self._resolve_callable(target)
            if target_name == LEGACY_PLAN_HYDRATION_RUN_METHOD:
                self.hydration_thread_creators.append(
                    {
                        "attribute": called,
                        "function": self.function_stack[-1] if self.function_stack else "<module>",
                        "if_tests": tuple(self.if_stack),
                        "lineno": node.lineno,
                        "kind": "thread_target",
                    }
                )
            if target_name == "plan_workflow":
                self.attribute_calls.append(
                    {
                        "attribute": target_name,
                        "function": self.function_stack[-1] if self.function_stack else "<module>",
                        "if_tests": tuple(self.if_stack),
                        "lineno": node.lineno,
                        "kind": "thread_target",
                    }
                )

        if called in {"submit", "map", "apply", "apply_async"}:
            target = node.args[0] if node.args else next(
                (keyword.value for keyword in node.keywords if keyword.arg in {"fn", "func"}),
                None,
            )
            target_name = self._resolve_callable(target)
            target_record = {
                "attribute": target_name,
                "function": self.function_stack[-1] if self.function_stack else "<module>",
                "if_tests": tuple(self.if_stack),
                "lineno": node.lineno,
                "kind": "executor_target",
            }
            if target_name == LEGACY_PLAN_HYDRATION_RUN_METHOD:
                self.hydration_executor_targets.append(target_record)
            if target_name == "plan_workflow":
                self.attribute_calls.append(target_record)
        self.generic_visit(node)


def _inventory(path: Path) -> _CallInventory:
    inventory = _CallInventory()
    inventory.visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
    return inventory


def _inventory_source(source: str) -> _CallInventory:
    inventory = _CallInventory()
    inventory.visit(ast.parse(source))
    return inventory


def test_legacy_submit_contract_is_explicit_and_terminal_set_is_pinned() -> None:
    assert LEGACY_PLAN_SUBMIT_HTTP_STATUS == 200
    assert LEGACY_PLAN_SUBMIT_RESPONSE_STATUS == "pending"
    assert PLAN_COMPILER_CONTRACT_VERSION == "legacy_plan_compile_v1"
    assert PLAN_GENERATION_TERMINAL_STATUSES == {
        PLAN_GENERATION_COMPLETED,
        PLAN_GENERATION_FAILED,
    }


def test_plan_generation_builder_preserves_one_lifecycle_shape() -> None:
    queued = build_plan_generation(
        status=PLAN_GENERATION_QUEUED,
        request_id="request-1",
        queued_at="2026-07-14T00:00:00+00:00",
    )
    assert queued == {
        "status": "queued",
        "request_id": "request-1",
        "queued_at": "2026-07-14T00:00:00+00:00",
        "submitted_at": "2026-07-14T00:00:00+00:00",
        "compiler_contract_version": "legacy_plan_compile_v1",
    }

    running = build_plan_generation(
        status=PLAN_GENERATION_RUNNING,
        request_id="request-1",
        queued_at="2026-07-14T00:00:00+00:00",
        started_at="2026-07-14T00:00:01+00:00",
        request_signature="signature-1",
    )
    assert running["submitted_at"] == running["queued_at"]
    assert running["started_at"] == "2026-07-14T00:00:01+00:00"
    assert running["request_signature"] == "signature-1"

    with pytest.raises(ValueError, match="unsupported plan generation status"):
        build_plan_generation(
            status="future_state",
            request_id="request-1",
            queued_at="2026-07-14T00:00:00+00:00",
        )


def test_authenticated_plan_submit_identity_proof_is_total_and_fail_closed() -> None:
    assert build_plan_submit_identity_metadata(
        provenance=PLAN_SUBMIT_IDENTITY_PROVENANCE_SERVER,
        requester_id="alice",
        tenant_id="user-alice",
    ) == {
        "provenance": "authenticated_request_state_v1",
        "requester_id": "alice",
        "tenant_id": "user-alice",
    }
    assert not build_plan_submit_identity_metadata(
        provenance="client_claim",
        requester_id="alice",
        tenant_id="user-alice",
    )
    assert not build_plan_submit_identity_metadata(
        provenance=PLAN_SUBMIT_IDENTITY_PROVENANCE_SERVER,
        requester_id="alice",
        tenant_id="",
    )
    link = {
        "phase": "plan",
        "metadata": {
            "plan_submit_identity": {
                "provenance": PLAN_SUBMIT_IDENTITY_PROVENANCE_SERVER,
                "requester_id": "alice",
                "tenant_id": "user-alice",
            }
        },
    }
    assert frontend_history_record_is_plan(link)
    assert authenticated_plan_history_metadata_owned(
        link,
        requester_id="alice",
        tenant_id="user-alice",
    )
    assert not authenticated_plan_history_metadata_owned(
        link,
        requester_id="bob",
        tenant_id="user-bob",
    )
    assert not authenticated_plan_history_metadata_owned(
        {"phase": "results", "metadata": link["metadata"]},
        requester_id="alice",
        tenant_id="user-alice",
    )


def test_plan_compile_signature_excludes_consumer_transport_identity_only() -> None:
    first = {
        "raw_user_request": "Find OpenAI reasoning researchers",
        "history_id": "history-1",
        "request_id": "request-1",
        "queued_at": "2026-07-14T00:00:00+00:00",
        "requester_id": "user-1",
        "tenant_id": "tenant-1",
        "keywords": ["reasoning", "pretraining"],
    }
    second = {
        **first,
        "history_id": "history-2",
        "request_id": "request-2",
        "queued_at": "2026-07-14T00:01:00+00:00",
    }
    assert canonical_plan_compile_request(first) == canonical_plan_compile_request(second)
    assert plan_hydration_request_signature(first) == plan_hydration_request_signature(second)
    assert plan_hydration_request_signature(first) != plan_hydration_request_signature(
        {**second, "tenant_id": "tenant-2"}
    )
    # C1b characterizes current compiler semantics: list order remains meaningful.
    assert plan_hydration_request_signature(first) != plan_hydration_request_signature(
        {**second, "keywords": ["pretraining", "reasoning"]}
    )


def test_source_ratchet_has_one_submit_owned_hydration_thread_and_no_api_compile_fallback() -> None:
    inventories = {
        str(path.relative_to(SOURCE_ROOT)): _inventory(path)
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
    }

    queue_callers = [
        (path, call["function"])
        for path, inventory in inventories.items()
        for call in inventory.attribute_calls
        if call["attribute"] == LEGACY_PLAN_HYDRATION_QUEUE_METHOD
    ]
    assert queue_callers == [("orchestrator.py", LEGACY_PLAN_HYDRATION_OWNER_METHOD)]

    thread_creators = [
        (path, creator["function"])
        for path, inventory in inventories.items()
        for creator in inventory.hydration_thread_creators
    ]
    assert thread_creators == [("orchestrator.py", LEGACY_PLAN_HYDRATION_QUEUE_METHOD)]

    executor_targets = [
        (path, target["function"])
        for path, inventory in inventories.items()
        for target in inventory.hydration_executor_targets
    ]
    assert executor_targets == []

    compile_callers = [
        (path, call["function"], call["if_tests"])
        for path, inventory in inventories.items()
        for call in inventory.attribute_calls
        if call["attribute"] == "plan_workflow"
    ]
    assert len(compile_callers) == 2
    hydration_call = next(call for call in compile_callers if call[0] == "orchestrator.py")
    assert hydration_call[1] == LEGACY_PLAN_HYDRATION_RUN_METHOD

    cli_call = next(call for call in compile_callers if call[0] == "cli.py")
    assert cli_call[1] == "main"
    assert "args.command == 'plan'" in cli_call[2]
    assert not any(path == "api.py" for path, _function, _if_tests in compile_callers)


def test_source_ratchet_detects_callable_alias_thread_alias_and_executor_bypasses() -> None:
    inventory = _inventory_source(
        """
import threading as thread_runtime

def bypass(self, executor):
    compile_alias = self.plan_workflow
    compile_alias({})
    run_alias = self._run_plan_hydration
    thread_alias = thread_runtime.Thread
    thread_alias(target=run_alias)
    submit_alias = executor.submit
    submit_alias(run_alias)
    submit_alias(compile_alias, {})
"""
    )

    compile_uses = [
        call for call in inventory.attribute_calls if call["attribute"] == "plan_workflow"
    ]
    assert {call["kind"] for call in compile_uses} == {"direct_or_alias_call", "executor_target"}
    assert len(inventory.hydration_thread_creators) == 1
    assert len(inventory.hydration_executor_targets) == 1
