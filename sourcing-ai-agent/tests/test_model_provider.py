import json
import unittest
from unittest.mock import patch

import requests

from sourcing_agent.model_provider import (
    DeterministicModelClient,
    OfflineModelClient,
    OpenAICompatibleChatModelClient,
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
        model_client = build_model_client(
            ModelProviderSettings(enabled=False),
            QwenSettings(enabled=True, api_key="sk-qwen"),
        )
        self.assertIsInstance(model_client, QwenResponsesModelClient)

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

            def _call_chat_completions(self, messages: list[dict[str, str]], *, max_tokens: int) -> str:  # noqa: ARG002
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
                return {"data": [{"id": "gpt-5.5"}]}

            def _call_responses_api(self, messages: list[dict[str, str]], *, max_tokens: int) -> str:  # noqa: ARG002
                self.responses_calls += 1
                return "MODEL_OK"

            def _call_chat_completions(self, messages: list[dict[str, str]], *, max_tokens: int) -> str:  # noqa: ARG002
                raise AssertionError("responses-style provider must not call chat completions")

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

        health = client.healthcheck()

        self.assertEqual(health["status"], "ready")
        self.assertEqual(health["chat_status"], "ready")
        self.assertEqual(client.responses_calls, 1)

    def test_openai_compatible_healthcheck_failure_opens_uncached_circuit(self) -> None:
        class _Client(OpenAICompatibleChatModelClient):
            def __init__(self, settings: ModelProviderSettings) -> None:
                super().__init__(settings)
                self.chat_calls = 0

            def _list_models(self) -> dict:
                return {"data": [{"id": "gpt-5.5"}]}

            def _call_chat_completions(self, messages: list[dict[str, str]], *, max_tokens: int) -> str:  # noqa: ARG002
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
                model="gpt-5.5",
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

            def _call_prompt(self, messages, *, max_tokens: int) -> str:  # noqa: ANN001, ARG002
                return "MODEL_OK"

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
            def _run_text_prompt(self, system_prompt: str, user_prompt: str, *, max_tokens: int) -> str:  # noqa: ARG002
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

        result = client.analyze_public_web_candidate_signals(
            {
                "candidate": {"candidate_name": "Jackie Bow", "current_company": "Anthropic"},
                "entry_links": [{"url": "https://github.com/jbow", "title": "Jackie Bow"}],
                "email_candidates": [],
                "evidence_slices": [],
            }
        )

        self.assertEqual(result["provider"], "chshapi_openai_compatible")
        self.assertEqual(result["model"], "gpt-5.5")
        self.assertEqual(result["model_version"], "gpt-5.5")
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["fallback_reason"], "model_call_failed")
        self.assertIn("401", result["model_error"])

    def test_qwen_public_web_model_call_error_is_fail_visible(self) -> None:
        class _Client(QwenResponsesModelClient):
            def _run_text_prompt(self, system_prompt: str, user_prompt: str, *, max_tokens: int | None = None) -> str:  # noqa: ARG002
                raise RuntimeError("Qwen HTTP 401: invalid api key")

        client = _Client(QwenSettings(enabled=True, api_key="sk-qwen", model="qwen3.5-plus-2026-04-20"))

        result = client.analyze_public_web_candidate_signals(
            {
                "candidate": {"candidate_name": "Yuwei Qin", "current_company": "Anthropic"},
                "entry_links": [],
                "email_candidates": [{"normalized_value": "yuwei@example.com"}],
                "evidence_slices": [],
            }
        )

        self.assertEqual(result["provider"], "qwen")
        self.assertEqual(result["model"], "qwen3.5-plus-2026-04-20")
        self.assertTrue(result["fallback_used"])
        self.assertEqual(result["fallback_reason"], "model_call_failed")
        self.assertIn("401", result["model_error"])

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
        self.assertTrue(any(item.get("rewrite_id") == "greater_china_outreach" for item in supported if isinstance(item, dict)))
        system_prompt = str(captured.get("system_prompt") or "")
        self.assertIn("four orthogonal dimensions", system_prompt)
        self.assertIn("keyword_priority_only", system_prompt)
        self.assertIn("provider_people_search_query_strategy", system_prompt)
        self.assertIn("acquisition_strategy_override is only the base roster strategy axis", system_prompt)
        self.assertIn("prefer categories=['researcher','engineer']", system_prompt)
        multimodal = next(
            item for item in supported
            if isinstance(item, dict) and item.get("rewrite_id") == "multimodal_project_focus"
        )
        self.assertEqual(multimodal.get("request_patch", {}).get("keywords"), ["multimodal"])

    def test_request_normalization_prompt_keeps_explicit_thematic_boundary(self) -> None:
        system_prompt = _build_request_normalization_system_prompt()

        self.assertIn("Do not expand a single direction into sibling, parent, child, or adjacent directions", system_prompt)
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
        self.assertTrue(any(item.get("rewrite_id") == "greater_china_outreach" for item in supported if isinstance(item, dict)))

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
                "instruction": "只用 search API 做 keyword-first acquisition，不要 company-employees，former 也要，多 query 并集。",
                "editable_fields": [
                    "keyword_priority_only",
                    "use_company_employees_lane",
                    "run_former_search_seed",
                    "provider_people_search_query_strategy",
                ],
            }
        )

        system_prompt = str(captured.get("system_prompt") or "")
        self.assertIn("four orthogonal control axes", system_prompt)
        self.assertIn("keyword_priority_only", system_prompt)
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
