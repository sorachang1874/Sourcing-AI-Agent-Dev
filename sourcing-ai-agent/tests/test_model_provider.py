import unittest

from sourcing_agent.model_provider import (
    DeterministicModelClient,
    OpenAICompatibleChatModelClient,
    _extract_openai_chat_text,
    _extract_openai_models,
    build_model_client,
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
