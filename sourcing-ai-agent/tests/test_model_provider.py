import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
from unittest.mock import patch

import requests

import sourcing_agent.model_provider as model_provider_module
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import (
    CRM_PUBLIC_WEB_PRODUCT_MODEL,
    DeterministicModelClient,
    OfflineModelClient,
    OpenAICompatibleChatModelClient,
    OpenAIModelCallResult,
    OpenAIModelUsage,
    QwenResponsesModelClient,
    ScriptedLivePlanningModelClient,
    _build_public_web_signal_adjudication_prompt,
    _build_request_normalization_system_prompt,
    _extract_openai_chat_text,
    _extract_openai_models,
    _normalize_public_web_signal_adjudication,
    _record_model_provider_failure,
    _reset_model_provider_circuits_for_tests,
    build_model_client,
    get_outreach_layer_prompt_template,
)
from sourcing_agent.settings import ModelProviderSettings, QwenSettings

# Explicit live opt-in for tests that assert live-client selection (fail-closed default).
_LIVE_CONFIRMED_ENV = {
    "SOURCING_EXTERNAL_PROVIDER_MODE": "live",
    "SOURCING_LIVE_PROVIDER_CONFIRM": "1",
    "SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS": "1",
}


class ModelProviderTest(unittest.TestCase):
    def setUp(self) -> None:
        _reset_model_provider_circuits_for_tests()

    def tearDown(self) -> None:
        _reset_model_provider_circuits_for_tests()

    def test_extract_openai_chat_text_from_string_content(self) -> None:
        payload = {
            "choices": [
                {
                    "message": {
                        "content": "CLAUDE_OK",
                    }
                }
            ]
        }
        self.assertEqual(_extract_openai_chat_text(payload), "CLAUDE_OK")

    def test_extract_openai_models_supports_data_and_models_keys(self) -> None:
        self.assertEqual(
            _extract_openai_models({"data": [{"id": "claude-sonnet-4-6"}, {"id": "claude-opus-4-6"}]}),
            ["claude-sonnet-4-6", "claude-opus-4-6"],
        )
        self.assertEqual(
            _extract_openai_models({"models": [{"name": "claude-sonnet-4-6"}]}),
            ["claude-sonnet-4-6"],
        )

    def test_build_model_client_prefers_configured_model_provider_when_enabled(self) -> None:
        # A live LLM client now requires explicit live mode + dual-confirm (fail-closed default).
        with patch.dict("os.environ", _LIVE_CONFIRMED_ENV, clear=False):
            model_client = build_model_client(
                ModelProviderSettings(
                    enabled=True,
                    provider_name="relay",
                    api_key="sk-test",
                    base_url="https://tb.keeps.cc/v1",
                    model="claude-sonnet-4-6",
                ),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )
        self.assertIsInstance(model_client, OpenAICompatibleChatModelClient)

    def test_build_model_client_uses_qwen_when_no_configured_model_provider(self) -> None:
        with patch.dict("os.environ", _LIVE_CONFIRMED_ENV, clear=False):
            model_client = build_model_client(
                ModelProviderSettings(enabled=False),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )
        self.assertIsInstance(model_client, QwenResponsesModelClient)

    def test_build_model_client_fails_closed_to_deterministic_without_live_confirm(self) -> None:
        # Regression guard: in the default (non-live) mode, a configured live model
        # client must NOT be built — no accidental billed LLM calls.
        with patch.dict("os.environ", {}, clear=True):
            model_client = build_model_client(
                ModelProviderSettings(enabled=True, api_key="sk-test", base_url="https://x/v1", model="m"),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )
        self.assertNotIsInstance(model_client, OpenAICompatibleChatModelClient)
        self.assertNotIsInstance(model_client, QwenResponsesModelClient)

    def test_public_web_signal_prompt_and_normalizer_preserve_user_visible_signal_contract(self) -> None:
        prompt = _build_public_web_signal_adjudication_prompt()
        self.assertIn("user_visible_signal", prompt)
        self.assertIn("review_queue_reason", prompt)

        normalized = _normalize_public_web_signal_adjudication(
            {
                "summary": "reviewed",
                "link_assessments": [
                    {
                        "url": "https://x.com/jbowocky",
                        "signal_type": "x_url",
                        "identity_match_label": "needs_review",
                        "identity_match_score": 0.45,
                        "confidence_label": "low",
                        "user_visible_signal": True,
                        "review_queue_reason": "Plausible owned X profile; useful for human review.",
                        "rationale": "Weak but plausible identity evidence.",
                    }
                ],
            },
            fallback={"summary": "fallback", "academic_summary": {}},
        )

        [assessment] = normalized["link_assessments"]
        self.assertTrue(assessment["user_visible_signal"])
        self.assertEqual(assessment["review_queue_reason"], "Plausible owned X profile; useful for human review.")

    def test_openai_compatible_healthcheck_requires_chat_completion(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.5"}]}

            def _call_chat_completions_result(  # noqa: ARG002
                self,
                messages: list[dict[str, str]],
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                raise RuntimeError("OpenAI-compatible HTTP 401: auth_unavailable")

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="chshapi_openai_compatible",
                api_key="sk-test",
                base_url="https://api.chshapi.org/v1",
                model="gpt-5.5",
            )
        )

        health = client.healthcheck()

        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["models_status"], "ready")
        self.assertEqual(health["chat_status"], "degraded")
        self.assertIn("401", health["error"])

    def test_openai_compatible_responses_style_healthcheck_uses_responses_api(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def __init__(self, settings: ModelProviderSettings) -> None:
                super().__init__(settings)
                self.responses_calls = 0

            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.6-sol"}]}

            def _call_responses_api_result(  # noqa: ARG002
                self,
                messages: list[dict[str, str]],
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                self.responses_calls += 1
                return OpenAIModelCallResult(
                    text="MODEL_OK",
                    requested_model=self.settings.model,
                    response_model="gpt-5.6-sol",
                    usage=OpenAIModelUsage(input_tokens=8, output_tokens=2, total_tokens=10),
                )

            def _call_chat_completions_result(  # noqa: ARG002
                self,
                messages: list[dict[str, str]],
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                raise AssertionError("responses-style provider must not call chat completions")

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.6-sol",
                api_style="openai_responses",
            )
        )

        health = client.healthcheck()

        self.assertEqual(health["status"], "ready")
        self.assertEqual(health["chat_status"], "ready")
        self.assertEqual(health["requested_model"], "gpt-5.6-sol")
        self.assertEqual(health["effective_model"], "gpt-5.6-sol")
        self.assertEqual(health["model_identity_provenance"], "provider_response")
        self.assertEqual(health["model_usage"]["total_tokens"], 10)
        self.assertEqual(client.responses_calls, 1)

    def test_openai_healthcheck_singleflight_shares_first_paid_probe_across_clients(self) -> None:
        prompt_calls = 0
        prompt_calls_lock = Lock()

        class _Client(OpenAICompatibleChatModelClient):
            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.6-sol"}]}

            def _call_prompt_result(  # noqa: ANN001, ARG002
                self,
                messages,
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                nonlocal prompt_calls
                with prompt_calls_lock:
                    prompt_calls += 1
                return OpenAIModelCallResult(
                    text="MODEL_OK",
                    requested_model=self.settings.model,
                    response_model=self.settings.model,
                    usage=OpenAIModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
                )

        settings = ModelProviderSettings(
            enabled=True,
            provider_name="sharedchat_openai_compatible",
            api_key="sk-test",
            base_url="https://singleflight.test/codex",
            model="gpt-5.6-sol",
            api_style="openai_responses",
        )
        clients = [_Client(settings), _Client(settings)]
        claim_start = Barrier(2)
        claim_complete = Barrier(2)
        original_claim = model_provider_module._claim_model_provider_healthcheck_flight

        def _synchronized_claim(key):  # noqa: ANN001, ANN202
            claim_start.wait(timeout=5)
            claimed = original_claim(key)
            claim_complete.wait(timeout=5)
            return claimed

        with (
            patch.object(
                model_provider_module,
                "_claim_model_provider_healthcheck_flight",
                side_effect=_synchronized_claim,
            ),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            futures = [executor.submit(client.healthcheck) for client in clients]
            health_results = [future.result(timeout=5) for future in futures]

        self.assertEqual(prompt_calls, 1)
        self.assertTrue(all(result["status"] == "ready" for result in health_results))
        self.assertTrue(all(result["effective_model"] == "gpt-5.6-sol" for result in health_results))
        self.assertTrue(all(client._healthcheck_cache is not None for client in clients))

    def test_openai_responses_product_model_is_sent_exactly(self) -> None:
        class _Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "output_text": "MODEL_OK",
                    "model": "gpt-5.6-sol",
                    "usage": {
                        "input_tokens": 11,
                        "output_tokens": 3,
                        "total_tokens": 14,
                        "input_tokens_details": {"cached_tokens": 4},
                        "output_tokens_details": {"reasoning_tokens": 2},
                        "unbounded_provider_detail": "ignored",
                    },
                }

        client = OpenAICompatibleChatModelClient(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.6-sol",
                api_style="openai_responses",
            )
        )

        with patch("sourcing_agent.model_provider.requests.post", return_value=_Response()) as post:
            call_result = client._call_responses_api_result(
                [{"role": "user", "content": "Reply with exactly: MODEL_OK"}],
                max_tokens=32,
            )
            text_result = client._call_responses_api(
                [{"role": "user", "content": "Reply with exactly: MODEL_OK"}],
                max_tokens=32,
            )

        self.assertEqual(call_result.text, "MODEL_OK")
        self.assertEqual(text_result, "MODEL_OK")
        self.assertEqual(call_result.requested_model, "gpt-5.6-sol")
        self.assertEqual(call_result.response_model, "gpt-5.6-sol")
        self.assertEqual(call_result.effective_model, "gpt-5.6-sol")
        self.assertEqual(call_result.model_identity_provenance, "provider_response")
        self.assertEqual(
            call_result.usage.to_record(),
            {
                "input_tokens": 11,
                "output_tokens": 3,
                "total_tokens": 14,
                "cached_input_tokens": 4,
                "reasoning_output_tokens": 2,
            },
        )
        self.assertEqual(post.call_count, 2)
        self.assertEqual(post.call_args.kwargs["json"]["model"], "gpt-5.6-sol")
        self.assertEqual(post.call_args.kwargs["json"]["temperature"], 0)

    def test_openai_chat_result_preserves_response_model_and_bounded_usage(self) -> None:
        class _Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "choices": [{"message": {"content": "MODEL_OK"}}],
                    "model": "gpt-5.6-sol",
                    "usage": {
                        "prompt_tokens": -3,
                        "completion_tokens": 5,
                        "total_tokens": 2_000_000_000,
                        "prompt_tokens_details": {"cached_tokens": 2},
                        "completion_tokens_details": {"reasoning_tokens": 1},
                    },
                }

        client = OpenAICompatibleChatModelClient(
            ModelProviderSettings(
                enabled=True,
                provider_name="openai_compatible",
                api_key="sk-test",
                base_url="https://example.test/v1",
                model="gpt-5.6-sol",
            )
        )

        with patch("sourcing_agent.model_provider.requests.post", return_value=_Response()) as post:
            result = client._call_chat_completions_result(
                [{"role": "user", "content": "Reply with exactly: MODEL_OK"}],
                max_tokens=32,
            )

        self.assertEqual(result.text, "MODEL_OK")
        self.assertEqual(result.effective_model, "gpt-5.6-sol")
        self.assertEqual(
            result.usage.to_record(),
            {
                "input_tokens": 0,
                "output_tokens": 5,
                "total_tokens": 1_000_000_000,
                "cached_input_tokens": 2,
                "reasoning_output_tokens": 1,
            },
        )
        self.assertEqual(post.call_args.kwargs["json"]["model"], "gpt-5.6-sol")

    def test_openai_healthcheck_fails_closed_when_response_model_is_missing(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def __init__(self, settings: ModelProviderSettings) -> None:
                super().__init__(settings)
                self.prompt_calls = 0

            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.6-sol"}]}

            def _call_prompt_result(self, messages, *, max_tokens: int) -> OpenAIModelCallResult:  # noqa: ANN001, ARG002
                self.prompt_calls += 1
                return OpenAIModelCallResult(
                    text="MODEL_OK",
                    requested_model=self.settings.model,
                    response_model="",
                    usage=OpenAIModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
                )

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.6-sol",
                api_style="openai_responses",
            )
        )

        health = client.healthcheck()
        circuit_health = client.healthcheck()

        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["chat_status"], "model_identity_missing")
        self.assertEqual(health["requested_model"], "gpt-5.6-sol")
        self.assertNotIn("response_model", health)
        self.assertNotIn("effective_model", health)
        self.assertNotIn("model_identity_provenance", health)
        self.assertIn("model_response_identity_missing", health["error"])
        self.assertEqual(client._healthcheck_cache["status"], "degraded")
        self.assertEqual(circuit_health["chat_status"], "circuit_open")
        self.assertEqual(client.prompt_calls, 1)

    def test_openai_healthcheck_fails_closed_when_response_model_mismatches(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def __init__(self, settings: ModelProviderSettings) -> None:
                super().__init__(settings)
                self.prompt_calls = 0

            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.6-sol"}]}

            def _call_prompt_result(self, messages, *, max_tokens: int) -> OpenAIModelCallResult:  # noqa: ANN001, ARG002
                self.prompt_calls += 1
                return OpenAIModelCallResult(
                    text="MODEL_OK",
                    requested_model=self.settings.model,
                    response_model="gpt-5.5",
                    usage=OpenAIModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
                )

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.6-sol",
                api_style="openai_responses",
            )
        )

        health = client.healthcheck()
        circuit_health = client.healthcheck()

        self.assertEqual(health["status"], "degraded")
        self.assertEqual(health["chat_status"], "model_identity_mismatch")
        self.assertEqual(health["requested_model"], "gpt-5.6-sol")
        self.assertEqual(health["response_model"], "gpt-5.5")
        self.assertEqual(health["effective_model"], "gpt-5.5")
        self.assertEqual(health["model_identity_provenance"], "provider_response")
        self.assertIn("model_response_identity_mismatch", health["error"])
        self.assertEqual(client._healthcheck_cache["status"], "degraded")
        self.assertEqual(circuit_health["chat_status"], "circuit_open")
        self.assertEqual(client.prompt_calls, 1)

    def test_openai_healthcheck_nonready_inventory_or_response_opens_circuit(self) -> None:
        for scenario in ("model_missing", "unexpected_response"):
            with self.subTest(scenario=scenario):
                _reset_model_provider_circuits_for_tests()

                class _Client(OpenAICompatibleChatModelClient):
                    def __init__(self, settings: ModelProviderSettings) -> None:
                        super().__init__(settings)
                        self.prompt_calls = 0

                    def _list_models(self) -> dict:
                        listed_model = "gpt-5.5" if scenario == "model_missing" else self.settings.model
                        return {"data": [{"id": listed_model}]}

                    def _call_prompt_result(  # noqa: ANN001, ARG002
                        self,
                        messages,
                        *,
                        max_tokens: int,
                    ) -> OpenAIModelCallResult:
                        self.prompt_calls += 1
                        return OpenAIModelCallResult(
                            text="NOT_OK" if scenario == "unexpected_response" else "MODEL_OK",
                            requested_model=self.settings.model,
                            response_model=self.settings.model,
                            usage=OpenAIModelUsage(input_tokens=4, output_tokens=2, total_tokens=6),
                        )

                client = _Client(
                    ModelProviderSettings(
                        enabled=True,
                        provider_name=f"sharedchat_{scenario}",
                        api_key="sk-test",
                        base_url=f"https://{scenario}.test/codex",
                        model="gpt-5.6-sol",
                        api_style="openai_responses",
                    )
                )

                first = client.healthcheck()
                second = client.healthcheck()

                self.assertEqual(first["status"], "degraded")
                self.assertEqual(client._healthcheck_cache["status"], "degraded")
                self.assertEqual(second["chat_status"], "circuit_open")
                self.assertEqual(client.prompt_calls, 1)

    def test_openai_business_json_paths_reject_mismatched_model_identity(self) -> None:
        self._assert_openai_business_json_paths_reject_model_identity("gpt-5.5")

    def test_openai_business_json_paths_reject_missing_model_identity(self) -> None:
        self._assert_openai_business_json_paths_reject_model_identity("")

    def test_openai_legacy_string_apis_reject_mismatched_model_identity(self) -> None:
        for operation in ("prompt", "chat_completions", "responses"):
            with self.subTest(operation=operation):
                _reset_model_provider_circuits_for_tests()

                class _Client(OpenAICompatibleChatModelClient):
                    def _mismatched_result(self) -> OpenAIModelCallResult:
                        return OpenAIModelCallResult(
                            text="UNTRUSTED_MODEL_TEXT",
                            requested_model=self.settings.model,
                            response_model="gpt-5.5",
                            usage=OpenAIModelUsage(input_tokens=2, output_tokens=1, total_tokens=3),
                        )

                    def _call_prompt_result(  # noqa: ANN001, ARG002
                        self,
                        messages,
                        *,
                        max_tokens: int,
                    ) -> OpenAIModelCallResult:
                        return self._mismatched_result()

                    def _call_chat_completions_result(  # noqa: ANN001, ARG002
                        self,
                        messages,
                        *,
                        max_tokens: int,
                    ) -> OpenAIModelCallResult:
                        return self._mismatched_result()

                    def _call_responses_api_result(  # noqa: ANN001, ARG002
                        self,
                        messages,
                        *,
                        max_tokens: int,
                    ) -> OpenAIModelCallResult:
                        return self._mismatched_result()

                client = _Client(
                    ModelProviderSettings(
                        enabled=True,
                        provider_name=f"sharedchat_legacy_{operation}",
                        api_key="sk-test",
                        base_url=f"https://legacy-{operation}.test/codex",
                        model="gpt-5.6-sol",
                        api_style="openai_responses",
                    )
                )
                messages = [{"role": "user", "content": "Return trusted text."}]

                with self.assertRaisesRegex(RuntimeError, "model_response_identity_mismatch"):
                    if operation == "prompt":
                        client._call_prompt(messages, max_tokens=32)
                    elif operation == "chat_completions":
                        client._call_chat_completions(messages, max_tokens=32)
                    else:
                        client._call_responses_api(messages, max_tokens=32)

                circuit_health = client.healthcheck()
                self.assertEqual(circuit_health["chat_status"], "circuit_open")
                self.assertIn("model_response_identity_mismatch", circuit_health["error"])

    def _assert_openai_business_json_paths_reject_model_identity(self, response_model: str) -> None:
        for operation in ("refinement", "planning"):
            with self.subTest(operation=operation, response_model=response_model or "missing"):
                _reset_model_provider_circuits_for_tests()

                class _Client(OpenAICompatibleChatModelClient):
                    def __init__(self, settings: ModelProviderSettings) -> None:
                        super().__init__(settings)
                        self.prompt_calls = 0

                    def _call_prompt_result(  # noqa: ANN001, ARG002
                        self,
                        messages,
                        *,
                        max_tokens: int,
                    ) -> OpenAIModelCallResult:
                        self.prompt_calls += 1
                        return OpenAIModelCallResult(
                            text=json.dumps(
                                {
                                    "patch": {"keywords": ["untrusted-provider-refinement"]},
                                    "planner_mode": "untrusted-provider-planning",
                                    "objective": "This provider JSON must not be accepted.",
                                    "query_bundles": [],
                                    "follow_up_rules": [],
                                    "review_triggers": [],
                                }
                            ),
                            requested_model=self.settings.model,
                            response_model=response_model,
                            usage=OpenAIModelUsage(input_tokens=10, output_tokens=4, total_tokens=14),
                        )

                client = _Client(
                    ModelProviderSettings(
                        enabled=True,
                        provider_name=f"sharedchat_business_{operation}_{response_model or 'missing'}",
                        api_key="sk-test",
                        base_url=f"https://business-{operation}-{response_model or 'missing'}.test/codex",
                        model="gpt-5.6-sol",
                        api_style="openai_responses",
                    )
                )

                if operation == "refinement":
                    result = client.normalize_refinement_instruction({"instruction": "只看 Agent 方向"})
                    self.assertEqual(result, {})
                else:
                    result = client.plan_search_strategy(
                        JobRequest(target_company="OpenAI", query="OpenAI Agent researchers"),
                        {"draft_search_strategy": {"query_bundles": []}},
                    )
                    self.assertEqual(result["planner_mode"], "deterministic")
                    self.assertNotEqual(result["planner_mode"], "untrusted-provider-planning")

                circuit_health = client.healthcheck()
                self.assertEqual(circuit_health["chat_status"], "circuit_open")
                self.assertIn(
                    "model_response_identity_missing" if not response_model else "model_response_identity_mismatch",
                    circuit_health["error"],
                )
                self.assertEqual(client.prompt_calls, 1)

    def test_openai_compatible_healthcheck_failure_opens_uncached_circuit(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def __init__(self, settings: ModelProviderSettings) -> None:
                super().__init__(settings)
                self.chat_calls = 0

            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.5"}]}

            def _call_chat_completions_result(  # noqa: ARG002
                self,
                messages: list[dict[str, str]],
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                self.chat_calls += 1
                raise RuntimeError("OpenAI-compatible HTTP 503: auth_unavailable")

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="chshapi_openai_compatible",
                api_key="sk-test",
                base_url="https://api.chshapi.org/v1",
                model="gpt-5.5",
            )
        )

        first = client.healthcheck()
        second = client.healthcheck()

        self.assertEqual(first["chat_status"], "degraded")
        self.assertEqual(second["chat_status"], "circuit_open")
        self.assertNotIn("cache_hit", second)
        self.assertIn("model_provider_circuit_open", second["error"])
        self.assertEqual(client.chat_calls, 1)

    def test_openai_compatible_chat_failure_opens_circuit_for_followup_calls(self) -> None:
        class _Response:
            status_code = 503
            text = '{"error":{"message":"auth_unavailable: no auth available"}}'

            def raise_for_status(self) -> None:
                raise requests.HTTPError(response=self)

            def json(self) -> dict:
                return {}

        client = OpenAICompatibleChatModelClient(
            ModelProviderSettings(
                enabled=True,
                provider_name="chshapi_openai_compatible",
                api_key="sk-test",
                base_url="https://api.chshapi.org/v1",
                model=CRM_PUBLIC_WEB_PRODUCT_MODEL,
            )
        )
        payload = {
            "candidate": {"candidate_name": "Jackie Bow", "current_company": "Anthropic"},
            "entry_links": [{"url": "https://github.com/jbow", "title": "Jackie Bow"}],
            "email_candidates": [],
            "evidence_slices": [],
        }
        call_count = 0

        def _post(*args, **kwargs):  # noqa: ANN002, ANN003
            nonlocal call_count
            call_count += 1
            return _Response()

        with patch("sourcing_agent.model_provider.requests.post", side_effect=_post):
            first = client.analyze_public_web_candidate_signals(payload)
            second = client.analyze_public_web_candidate_signals(payload)

        self.assertTrue(first["fallback_used"])
        self.assertEqual(first["fallback_reason"], "model_call_failed")
        self.assertIn("503", first["model_error"])
        self.assertTrue(second["fallback_used"])
        self.assertEqual(second["fallback_reason"], "model_call_failed")
        self.assertIn("model_provider_circuit_open", second["model_error"])
        self.assertEqual(call_count, 1)

    def test_openai_healthcheck_cache_does_not_mask_open_circuit(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.5"}]}

            def _call_prompt_result(self, messages, *, max_tokens: int) -> OpenAIModelCallResult:  # noqa: ANN001, ARG002
                return OpenAIModelCallResult(
                    text="MODEL_OK",
                    requested_model=self.settings.model,
                    response_model=self.settings.model,
                    usage=OpenAIModelUsage(),
                )

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.5",
            )
        )

        first = client.healthcheck()
        self.assertEqual(first["status"], "ready")

        _record_model_provider_failure(client._circuit_key(), "OpenAI-compatible HTTP 502: bad gateway")
        second = client.healthcheck()

        self.assertEqual(second["status"], "degraded")
        self.assertEqual(second["chat_status"], "circuit_open")
        self.assertNotIn("cache_hit", second)
        self.assertIn("model_provider_circuit_open", second["error"])

    def test_openai_public_web_model_call_error_is_fail_visible(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def _call_prompt_result(  # noqa: ANN001, ARG002
                self,
                messages,
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                raise RuntimeError("OpenAI-compatible HTTP 401: auth_unavailable")

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="chshapi_openai_compatible",
                api_key="sk-test",
                base_url="https://api.chshapi.org/v1",
                model=CRM_PUBLIC_WEB_PRODUCT_MODEL,
            )
        )

        result = client.analyze_public_web_candidate_signals(
            {
                "candidate": {"candidate_name": "Jackie Bow", "current_company": "Anthropic"},
                "entry_links": [{"url": "https://github.com/jbow", "title": "Jackie Bow"}],
                "email_candidates": [],
                "evidence_slices": [],
            }
        )

        self.assertEqual(result["provider"], "chshapi_openai_compatible")
        self.assertEqual(result["model"], CRM_PUBLIC_WEB_PRODUCT_MODEL)
        self.assertEqual(result["model_version"], CRM_PUBLIC_WEB_PRODUCT_MODEL)
        self.assertEqual(result["requested_model"], CRM_PUBLIC_WEB_PRODUCT_MODEL)
        self.assertNotIn("effective_model", result)
        self.assertNotIn("model_identity_provenance", result)
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["fallback_reason"], "model_call_failed")
        self.assertIn("401", result["model_error"])

    def test_openai_public_web_rejects_wrong_product_model_before_transport_without_blocking_other_calls(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def __init__(self, settings: ModelProviderSettings) -> None:
                super().__init__(settings)
                self.prompt_calls = 0

            def _call_prompt_result(  # noqa: ANN001, ARG002
                self,
                messages,
                *,
                max_tokens: int,
            ) -> OpenAIModelCallResult:
                self.prompt_calls += 1
                return OpenAIModelCallResult(
                    text='{"target_company":"Anthropic"}',
                    requested_model=self.settings.model,
                    response_model=self.settings.model,
                    usage=OpenAIModelUsage(),
                )

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.5",
                api_style="openai_responses",
            )
        )

        result = client.analyze_public_web_candidate_signals(
            {
                "candidate": {"candidate_name": "Jackie Bow", "current_company": "Anthropic"},
                "entry_links": [{"url": "https://github.com/jbow", "title": "Jackie Bow"}],
                "email_candidates": [],
                "evidence_slices": [],
            }
        )

        self.assertEqual(client.prompt_calls, 0)
        self.assertEqual(result["provider"], "sharedchat_openai_compatible")
        self.assertEqual(result["model"], "gpt-5.5")
        self.assertEqual(result["model_version"], "gpt-5.5")
        self.assertEqual(result["requested_model"], "gpt-5.5")
        self.assertNotIn("response_model", result)
        self.assertNotIn("effective_model", result)
        self.assertNotIn("model_identity_provenance", result)
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["fallback_reason"], "model_configuration_mismatch")
        self.assertEqual(
            result["model_error"],
            "crm_public_web_product_model_mismatch: expected_model=gpt-5.6-sol requested_model=gpt-5.5",
        )

        self.assertEqual(
            client.normalize_request({"raw_user_request": "Find Anthropic researchers"}),
            {"target_company": "Anthropic"},
        )
        self.assertEqual(client.prompt_calls, 1)

    def test_openai_public_web_adjudication_records_provider_response_identity_and_usage(self) -> None:
        class _Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "output_text": json.dumps(
                        {
                            "summary": "Provider-reviewed evidence.",
                            "link_assessments": [],
                            "email_assessments": [],
                        }
                    ),
                    "model": "gpt-5.6-sol",
                    "usage": {"input_tokens": 120, "output_tokens": 24, "total_tokens": 144},
                }

        client = OpenAICompatibleChatModelClient(
            ModelProviderSettings(
                enabled=True,
                provider_name="sharedchat_openai_compatible",
                api_key="sk-test",
                base_url="https://new.sharedchat.cc/codex",
                model="gpt-5.6-sol",
                api_style="openai_responses",
            )
        )

        with patch("sourcing_agent.model_provider.requests.post", return_value=_Response()):
            result = client.analyze_public_web_candidate_signals(
                {
                    "candidate": {"candidate_name": "Jackie Bow", "current_company": "Anthropic"},
                    "entry_links": [{"url": "https://github.com/jbow", "title": "Jackie Bow"}],
                    "email_candidates": [],
                    "evidence_slices": [],
                }
            )

        self.assertFalse(result["fallback_used"])
        self.assertEqual(result["requested_model"], "gpt-5.6-sol")
        self.assertEqual(result["response_model"], "gpt-5.6-sol")
        self.assertEqual(result["effective_model"], "gpt-5.6-sol")
        self.assertEqual(result["model_identity_provenance"], "provider_response")
        self.assertEqual(result["model"], "gpt-5.6-sol")
        self.assertEqual(result["model_version"], "gpt-5.6-sol")
        self.assertEqual(
            result["model_usage"],
            {"input_tokens": 120, "output_tokens": 24, "total_tokens": 144},
        )

    def test_openai_public_web_adjudication_rejects_unproven_or_mismatched_model_identity(self) -> None:
        for response_model, expected_reason in (
            ("", "model_identity_missing"),
            ("gpt-5.5", "model_identity_mismatch"),
        ):
            with self.subTest(response_model=response_model or "missing"):
                _reset_model_provider_circuits_for_tests()

                class _Client(OpenAICompatibleChatModelClient):
                    def __init__(self, settings: ModelProviderSettings) -> None:
                        super().__init__(settings)
                        self.prompt_calls = 0

                    def _call_prompt_result(  # noqa: ANN001, ARG002
                        self,
                        messages,
                        *,
                        max_tokens: int,
                    ) -> OpenAIModelCallResult:
                        self.prompt_calls += 1
                        return OpenAIModelCallResult(
                            text=json.dumps(
                                {
                                    "summary": "Provider result must not be used.",
                                    "link_assessments": [],
                                    "email_assessments": [],
                                }
                            ),
                            requested_model=self.settings.model,
                            response_model=response_model,
                            usage=OpenAIModelUsage(input_tokens=10, output_tokens=2, total_tokens=12),
                        )

                client = _Client(
                    ModelProviderSettings(
                        enabled=True,
                        provider_name=f"sharedchat_{expected_reason}",
                        api_key="sk-test",
                        base_url=f"https://{expected_reason}.test/codex",
                        model="gpt-5.6-sol",
                        api_style="openai_responses",
                    )
                )
                payload = {
                    "candidate": {"candidate_name": "Jackie Bow", "current_company": "Anthropic"},
                    "entry_links": [{"url": "https://github.com/jbow", "title": "Jackie Bow"}],
                    "email_candidates": [],
                    "evidence_slices": [],
                }

                first = client.analyze_public_web_candidate_signals(payload)
                circuit_health = client.healthcheck()

                self.assertEqual(first["summary"], "Deterministic public-web signal adjudication fallback.")
                self.assertTrue(first["fallback_used"])
                self.assertEqual(first["fallback_reason"], expected_reason)
                self.assertIn(
                    f"model_response_identity_{expected_reason.removeprefix('model_identity_')}", first["model_error"]
                )
                self.assertEqual(first["model"], "gpt-5.6-sol")
                self.assertEqual(first["model_version"], "gpt-5.6-sol")
                self.assertEqual(circuit_health["chat_status"], "circuit_open")
                self.assertIn("model_provider_circuit_open", circuit_health["error"])
                self.assertEqual(client.prompt_calls, 1)

    def test_qwen_public_web_is_rejected_before_prompt_without_blocking_other_calls(self) -> None:
        class _Client(QwenResponsesModelClient):
            def __init__(self, settings: QwenSettings) -> None:
                super().__init__(settings)
                self.prompt_calls = 0

            def _run_text_prompt(  # noqa: ARG002
                self,
                system_prompt: str,
                user_prompt: str,
                *,
                max_tokens: int | None = None,
            ) -> str:
                self.prompt_calls += 1
                return '{"target_company":"Anthropic"}'

        client = _Client(QwenSettings(enabled=True, api_key="sk-qwen", model="qwen3.5-plus-2026-04-20"))

        result = client.analyze_public_web_candidate_signals(
            {
                "candidate": {"candidate_name": "Yuwei Qin", "current_company": "Anthropic"},
                "entry_links": [],
                "email_candidates": [{"normalized_value": "yuwei@example.com"}],
                "evidence_slices": [],
            }
        )

        self.assertEqual(client.prompt_calls, 0)
        self.assertEqual(result["provider"], "qwen")
        self.assertEqual(result["model"], "qwen3.5-plus-2026-04-20")
        self.assertEqual(result["model_version"], "qwen3.5-plus-2026-04-20")
        self.assertEqual(result["requested_model"], "qwen3.5-plus-2026-04-20")
        self.assertNotIn("response_model", result)
        self.assertNotIn("effective_model", result)
        self.assertNotIn("model_identity_provenance", result)
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["fallback_reason"], "model_configuration_mismatch")
        self.assertEqual(
            result["model_error"],
            (
                "crm_public_web_product_model_mismatch: expected_model=gpt-5.6-sol "
                "requested_model=qwen3.5-plus-2026-04-20"
            ),
        )

        self.assertEqual(
            client.normalize_request({"raw_user_request": "Find Anthropic researchers"}),
            {"target_company": "Anthropic"},
        )
        self.assertEqual(client.prompt_calls, 1)

    def test_request_normalization_prompt_lists_agent_as_ai_direction(self) -> None:
        prompt = _build_request_normalization_system_prompt()

        self.assertIn("Coding, Agent, Math", prompt)
        self.assertIn("Coding, Agent, or Math", prompt)

    def test_build_model_client_falls_back_to_deterministic(self) -> None:
        model_client = build_model_client(
            ModelProviderSettings(enabled=False),
            QwenSettings(enabled=False),
        )
        self.assertIsInstance(model_client, DeterministicModelClient)

    def test_build_model_client_uses_offline_provider_in_simulate_mode(self) -> None:
        with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}):
            model_client = build_model_client(
                ModelProviderSettings(
                    enabled=True,
                    provider_name="relay",
                    api_key="sk-test",
                    base_url="https://tb.keeps.cc/v1",
                    model="claude-sonnet-4-6",
                ),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )
        self.assertIsInstance(model_client, OfflineModelClient)
        self.assertEqual(model_client.provider_name(), "offline_model")
        self.assertEqual(model_client.healthcheck()["provider_mode"], "simulate")

    def test_build_model_client_uses_offline_provider_in_replay_mode(self) -> None:
        with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "replay"}):
            model_client = build_model_client(
                ModelProviderSettings(
                    enabled=True,
                    provider_name="relay",
                    api_key="sk-test",
                    base_url="https://tb.keeps.cc/v1",
                    model="claude-sonnet-4-6",
                ),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )
        self.assertIsInstance(model_client, OfflineModelClient)
        self.assertEqual(model_client.healthcheck()["provider_mode"], "replay")

    def test_build_model_client_uses_offline_provider_in_scripted_mode(self) -> None:
        with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}):
            model_client = build_model_client(
                ModelProviderSettings(
                    enabled=True,
                    provider_name="relay",
                    api_key="sk-test",
                    base_url="https://tb.keeps.cc/v1",
                    model="claude-sonnet-4-6",
                ),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )
        self.assertIsInstance(model_client, OfflineModelClient)
        self.assertEqual(model_client.healthcheck()["provider_mode"], "scripted")

    def test_build_model_client_allows_planning_only_live_model_in_scripted_mode(self) -> None:
        with patch.dict(
            "os.environ",
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_LIVE_MODEL_PLANNING": "1",
            },
        ):
            model_client = build_model_client(
                ModelProviderSettings(
                    enabled=False,
                    provider_name="relay",
                    api_key="",
                    base_url="",
                    model="",
                ),
                QwenSettings(enabled=True, api_key="sk-qwen"),
            )

        self.assertIsInstance(model_client, ScriptedLivePlanningModelClient)
        self.assertEqual(model_client.provider_name(), "scripted_live_planning_model")
        self.assertEqual(model_client.healthcheck()["delegate_provider"], "qwen")
        self.assertFalse(model_client.supports_outreach_ai_verification())

    def test_scripted_live_model_planning_does_not_enable_non_planning_calls(self) -> None:
        class Delegate(DeterministicModelClient):
            def provider_name(self) -> str:
                return "delegate"

            def normalize_request(self, payload: dict) -> dict:
                return {"target_company": "OpenAI"}

            def evaluate_outreach_profile(self, payload: dict) -> dict:
                return {"final_layer": 3}

        model_client = ScriptedLivePlanningModelClient(Delegate(), mode="scripted")

        self.assertEqual(model_client.normalize_request({}), {"target_company": "OpenAI"})
        self.assertEqual(model_client.evaluate_outreach_profile({}), {})
        self.assertFalse(model_client.supports_outreach_ai_verification())

    def test_outreach_ai_capability_flags(self) -> None:
        self.assertFalse(DeterministicModelClient().supports_outreach_ai_verification())
        self.assertTrue(
            OpenAICompatibleChatModelClient(
                ModelProviderSettings(
                    enabled=True,
                    provider_name="relay",
                    api_key="sk-test",
                    base_url="https://tb.keeps.cc/v1",
                    model="claude-sonnet-4-6",
                )
            ).supports_outreach_ai_verification()
        )
        self.assertTrue(
            QwenResponsesModelClient(
                QwenSettings(
                    enabled=True,
                    api_key="sk-qwen",
                )
            ).supports_outreach_ai_verification()
        )

    def test_outreach_layer_prompt_template_includes_comprehensive_signals(self) -> None:
        template = get_outreach_layer_prompt_template()
        self.assertEqual(
            template.get("version"),
            "outreach_layering_v3_explicit_greater_china_scope",
        )
        system_prompt = str(template.get("system_prompt") or "")
        self.assertIn("name, education history, work history, and language signals", system_prompt)
        self.assertIn("请综合候选人的姓名、教育经历、工作经历、语言能力等公开信息", system_prompt)
        self.assertIn("broader Greater China region experience signal", system_prompt)
        self.assertIn("Layer 2 is broader than Mainland China", system_prompt)
        self.assertEqual(
            template.get("required_output_keys"),
            ["final_layer", "confidence_label", "evidence_clues", "rationale"],
        )

    def test_openai_normalize_request_includes_supported_rewrite_policies_in_payload(self) -> None:
        captured: dict[str, object] = {}

        class _Client(OpenAICompatibleChatModelClient):
            def _safe_text_prompt(self, system_prompt: str, user_prompt: str, max_tokens: int = 700) -> str:  # noqa: ARG002
                captured["system_prompt"] = system_prompt
                captured["payload"] = json.loads(user_prompt)
                return "{}"

        client = _Client(
            ModelProviderSettings(
                enabled=True,
                provider_name="relay",
                api_key="sk-test",
                base_url="https://tb.keeps.cc/v1",
                model="claude-sonnet-4-6",
            )
        )
        client.normalize_request({"raw_user_request": "帮我找华人研究员"})

        prompt_payload = dict(captured.get("payload") or {})
        supported = list(prompt_payload.get("supported_rewrite_policies") or [])
        self.assertTrue(
            any(item.get("rewrite_id") == "greater_china_outreach" for item in supported if isinstance(item, dict))
        )
        system_prompt = str(captured.get("system_prompt") or "")
        self.assertIn("four orthogonal dimensions", system_prompt)
        self.assertNotIn("keyword_priority_only", system_prompt)
        self.assertNotIn("large_org_keyword_probe_mode", system_prompt)
        self.assertIn("provider_people_search_query_strategy", system_prompt)
        self.assertIn("acquisition_strategy_override is only the base roster strategy axis", system_prompt)
        self.assertIn("per-function shard queries", system_prompt)
        self.assertIn("prefer categories=['researcher','engineer']", system_prompt)
        multimodal = next(
            item
            for item in supported
            if isinstance(item, dict) and item.get("rewrite_id") == "multimodal_project_focus"
        )
        self.assertEqual(multimodal.get("request_patch", {}).get("keywords"), ["multimodal"])

    def test_request_normalization_prompt_keeps_explicit_thematic_boundary(self) -> None:
        system_prompt = _build_request_normalization_system_prompt()

        self.assertIn(
            "Do not expand a single direction into sibling, parent, child, or adjacent directions", system_prompt
        )
        self.assertIn("if the request says Multimodal", system_prompt)
        self.assertIn("do not add Text, Vision, vision-language, or video generation", system_prompt)

    def test_qwen_normalize_refinement_instruction_includes_supported_rewrite_policies_in_payload(self) -> None:
        captured: dict[str, object] = {}

        class _Client(QwenResponsesModelClient):
            def _safe_text_prompt(self, system_prompt: str, user_prompt: str) -> str:  # noqa: ARG002
                captured["system_prompt"] = system_prompt
                captured["payload"] = json.loads(user_prompt)
                return "{}"

        client = _Client(QwenSettings(enabled=True, api_key="sk-qwen"))
        client.normalize_refinement_instruction({"instruction": "只看华人"})

        prompt_payload = dict(captured.get("payload") or {})
        supported = list(prompt_payload.get("supported_rewrite_policies") or [])
        self.assertTrue(
            any(item.get("rewrite_id") == "greater_china_outreach" for item in supported if isinstance(item, dict))
        )

    def test_qwen_normalize_review_instruction_prompt_includes_keyword_first_axes(self) -> None:
        captured: dict[str, object] = {}

        class _Client(QwenResponsesModelClient):
            def _safe_text_prompt(self, system_prompt: str, user_prompt: str) -> str:  # noqa: ARG002
                captured["system_prompt"] = system_prompt
                captured["payload"] = json.loads(user_prompt)
                return "{}"

        client = _Client(QwenSettings(enabled=True, api_key="sk-qwen"))
        client.normalize_review_instruction(
            {
                "instruction": "只用 search API 做 acquisition，不要 company-employees，former 也要，多 query 并集。",
                "editable_fields": [
                    "use_company_employees_lane",
                    "run_former_search_seed",
                    "provider_people_search_query_strategy",
                ],
            }
        )

        system_prompt = str(captured.get("system_prompt") or "")
        self.assertIn("four orthogonal control axes", system_prompt)
        self.assertNotIn("keyword_priority_only", system_prompt)
        self.assertNotIn("large_org_keyword_probe_mode", system_prompt)
        self.assertIn("use_company_employees_lane=false", system_prompt)
        self.assertIn("provider_people_search_query_strategy=all_queries_union", system_prompt)

    def test_deterministic_normalize_spreadsheet_contacts_maps_common_headers(self) -> None:
        client = DeterministicModelClient()
        result = client.normalize_spreadsheet_contacts(
            {
                "filename": "contacts.xlsx",
                "sheets": [
                    {
                        "sheet_name": "Deepmind List",
                        "headers": ["Name", "Company", "Title", "Linkedin", "Email"],
                        "sample_rows": [
                            {
                                "Name": "Piaoyang Cui",
                                "Company": "Google Deepmind",
                                "Title": "Senior Staff",
                                "Linkedin": "https://www.linkedin.com/in/piaoyang/",
                            }
                        ],
                    }
                ],
            }
        )
        self.assertTrue(result["contacts_detected"])
        selected = list(result.get("selected_sheets") or [])
        self.assertEqual(len(selected), 1)
        mapping = dict(selected[0].get("column_mapping") or {})
        self.assertEqual(mapping["name"], "Name")
        self.assertEqual(mapping["company"], "Company")
        self.assertEqual(mapping["title"], "Title")
        self.assertEqual(mapping["linkedin_url"], "Linkedin")
