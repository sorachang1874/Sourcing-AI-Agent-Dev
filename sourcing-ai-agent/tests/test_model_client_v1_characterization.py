from __future__ import annotations

import ast
import json
from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

import sourcing_agent.model_provider as model_provider_module
from sourcing_agent.model_provider import (
    CRM_PUBLIC_WEB_PRODUCT_MODEL,
    DeterministicModelClient,
    OfflineModelClient,
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


PROTOCOL_SIGNATURES = {
    "summarize": "(self, request: JobRequest, matches: list[dict], total_matches: int) -> str",
    "normalize_request": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "normalize_spreadsheet_contacts": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "normalize_review_instruction": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "normalize_refinement_instruction": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "interpret_intent": "(self, request: JobRequest, draft_plan: dict[str, Any]) -> str",
    "draft_intent_brief": "(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]",
    "plan_search_strategy": "(self, request: JobRequest, draft_payload: dict[str, Any]) -> dict[str, Any]",
    "analyze_page_asset": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "analyze_public_web_candidate_signals": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "judge_company_equivalence": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "judge_profile_membership": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "synthesize_manual_review": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "evaluate_outreach_profile": "(self, payload: dict[str, Any]) -> dict[str, Any]",
    "provider_name": "(self) -> str",
    "supports_outreach_ai_verification": "(self) -> bool",
    "healthcheck": "(self) -> dict[str, Any]",
}


CONCRETE_PROTOCOL_OVERRIDES = {
    "DeterministicModelClient": frozenset(PROTOCOL_SIGNATURES),
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
    "QwenResponsesModelClient": frozenset(PROTOCOL_SIGNATURES),
    "OpenAICompatibleChatModelClient": frozenset(PROTOCOL_SIGNATURES),
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
        ("orchestrator.py", "_execute_retrieval", "summarize"): 1,
        ("orchestrator.py", "_persist_criteria_artifacts", "provider_name"): 1,
        ("outreach_layering.py", "_evaluate_candidate_with_model", "evaluate_outreach_profile"): 1,
        ("planning.py", "build_sourcing_plan", "interpret_intent"): 1,
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


CONCRETE_CLIENTS = {
    "DeterministicModelClient": DeterministicModelClient,
    "OfflineModelClient": OfflineModelClient,
    "ScriptedLivePlanningModelClient": ScriptedLivePlanningModelClient,
    "QwenResponsesModelClient": QwenResponsesModelClient,
    "OpenAICompatibleChatModelClient": OpenAICompatibleChatModelClient,
}


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


def _protocol_surface(tree: ast.Module) -> dict[str, str]:
    protocol = _class_node(tree, "ModelClient")
    return {
        node.name: _canonical_signature(node)
        for node in protocol.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.name.startswith("_")
    }


def _protocol_override_signatures(tree: ast.Module, class_name: str) -> dict[str, str]:
    class_definition = _class_node(tree, class_name)
    return {
        node.name: _canonical_signature(node)
        for node in class_definition.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in PROTOCOL_SIGNATURES
    }


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


def _mentions_model_client(tree: ast.Module) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "model_client":
            return True
        if isinstance(node, ast.Attribute) and node.attr == "model_client":
            return True
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value == "model_client"
        ):
            return True
    return False


class _ModelClientCallVisitor(ast.NodeVisitor):
    def __init__(self, module_name: str) -> None:
        self.module_name = module_name
        self.function_stack: list[str] = []
        self.call_points: Counter[tuple[str, str, str]] = Counter()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.function_stack.append(node.name)
        self.generic_visit(node)
        self.function_stack.pop()

    def visit_Call(self, node: ast.Call) -> None:
        target = _dotted_name(node.func).split(".")
        if len(target) >= 2 and target[-2] == "model_client":
            owner = self.function_stack[-1] if self.function_stack else "<module>"
            self.call_points[(self.module_name, owner, target[-1])] += 1
        self.generic_visit(node)


def _consumer_inventory() -> tuple[frozenset[str], Counter[tuple[str, str, str]]]:
    modules: set[str] = set()
    calls: Counter[tuple[str, str, str]] = Counter()
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        if path == MODEL_PROVIDER_PATH or "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        module_name = path.relative_to(SOURCE_ROOT).as_posix()
        if _mentions_model_client(tree):
            modules.add(module_name)
        visitor = _ModelClientCallVisitor(module_name)
        visitor.visit(tree)
        calls.update(visitor.call_points)
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

    assert _protocol_surface(tree) == PROTOCOL_SIGNATURES
    assert len(PROTOCOL_SIGNATURES) == 17


def test_concrete_clients_preserve_complete_protocol_surface_and_override_boundaries() -> None:
    tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))

    for class_name, client_class in CONCRETE_CLIENTS.items():
        override_signatures = _protocol_override_signatures(tree, class_name)
        expected_overrides = CONCRETE_PROTOCOL_OVERRIDES[class_name]
        assert frozenset(override_signatures) == expected_overrides
        assert override_signatures == {name: PROTOCOL_SIGNATURES[name] for name in expected_overrides}
        assert {name for name in PROTOCOL_SIGNATURES if callable(getattr(client_class, name, None))} == set(
            PROTOCOL_SIGNATURES
        )


def test_scripted_live_client_delegates_only_the_six_front_door_planning_methods() -> None:
    tree = ast.parse(MODEL_PROVIDER_PATH.read_text(encoding="utf-8"), filename=str(MODEL_PROVIDER_PATH))

    delegated = {
        method_name: _delegated_return_target(tree, method_name) for method_name in SCRIPTED_LIVE_DELEGATED_METHODS
    }

    assert delegated == {method_name: method_name for method_name in SCRIPTED_LIVE_DELEGATED_METHODS}
    assert _direct_delegate_return_targets(tree) == delegated
    assert SCRIPTED_LIVE_DELEGATED_METHODS < CONCRETE_PROTOCOL_OVERRIDES["ScriptedLivePlanningModelClient"]


def test_model_client_consumer_inventory_and_call_points_match_v1_golden() -> None:
    modules, calls = _consumer_inventory()

    assert modules == MODEL_CLIENT_CONSUMER_MODULES
    assert calls == MODEL_CLIENT_CALL_POINTS
    assert sum(calls.values()) == 27
    assert {method_name for _, _, method_name in calls} == set(PROTOCOL_SIGNATURES)


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
    assert post.call_args.kwargs["timeout"] == 17
    assert post.call_args.kwargs["json"] == {
        "model": "gpt-characterization",
        "messages": messages,
        "max_tokens": 32,
        "temperature": 0,
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
    assert post.call_args.kwargs["timeout"] == 19
    assert post.call_args.kwargs["json"] == {
        "model": "gpt-characterization",
        "input": "system: System\n\nuser: User",
        "max_output_tokens": 65,
        "temperature": 0,
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
    assert _protocol_surface(protocol_tree) != PROTOCOL_SIGNATURES

    call_tree = ast.parse(
        """
def run(model_client):
    return model_client.healthcheck()
"""
    )
    visitor = _ModelClientCallVisitor("synthetic.py")
    visitor.visit(call_tree)
    assert visitor.call_points == Counter({("synthetic.py", "run", "healthcheck"): 1})

    call = next(node for node in ast.walk(call_tree) if isinstance(node, ast.Call))
    assert isinstance(call.func, ast.Attribute)
    call.func.attr = "unknown_method"
    mutated = _ModelClientCallVisitor("synthetic.py")
    mutated.visit(call_tree)
    assert mutated.call_points != visitor.call_points
