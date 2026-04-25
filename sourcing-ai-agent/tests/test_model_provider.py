import json
import unittest
from unittest.mock import patch

from sourcing_agent.model_provider import (
    DeterministicModelClient,
    OfflineModelClient,
    OpenAICompatibleChatModelClient,
    QwenResponsesModelClient,
    _build_request_normalization_system_prompt,
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

    def test_build_model_client_prefers_qwen_when_enabled(self) -> None:
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
        self.assertIsInstance(model_client, QwenResponsesModelClient)

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
