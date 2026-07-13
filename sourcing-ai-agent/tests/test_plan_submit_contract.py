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
    build_plan_generation,
    canonical_plan_compile_request,
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

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_If(self, node: ast.If) -> None:
        self.if_stack.append(ast.unparse(node.test))
        for statement in node.body:
            self.visit(statement)
        self.if_stack.pop()
        for statement in node.orelse:
            self.visit(statement)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute):
            record = {
                "attribute": node.func.attr,
                "function": self.function_stack[-1] if self.function_stack else "<module>",
                "if_tests": tuple(self.if_stack),
                "lineno": node.lineno,
            }
            self.attribute_calls.append(record)
            if node.func.attr == "Thread":
                target = next(
                    (keyword.value for keyword in node.keywords if keyword.arg == "target"),
                    None,
                )
                if isinstance(target, ast.Attribute) and target.attr == LEGACY_PLAN_HYDRATION_RUN_METHOD:
                    self.hydration_thread_creators.append(record)
        self.generic_visit(node)


def _inventory(path: Path) -> _CallInventory:
    inventory = _CallInventory()
    inventory.visit(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
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
