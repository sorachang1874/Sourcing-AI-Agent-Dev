import unittest

from sourcing_agent.model_provider import (
    DeterministicModelClient,
    OpenAICompatibleChatModelClient,
    QwenResponsesModelClient,
    _extract_openai_chat_text,
    _extract_openai_models,
    build_model_client,
    get_outreach_layer_prompt_template,
)
from sourcing_agent.settings import ModelProviderSettings, QwenSettings


class ModelProviderTest(unittest.TestCase):
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

    def test_build_model_client_prefers_generic_provider(self) -> None:
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

    def test_build_model_client_falls_back_to_deterministic(self) -> None:
        model_client = build_model_client(
            ModelProviderSettings(enabled=False),
            QwenSettings(enabled=False),
        )
        self.assertIsInstance(model_client, DeterministicModelClient)

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
