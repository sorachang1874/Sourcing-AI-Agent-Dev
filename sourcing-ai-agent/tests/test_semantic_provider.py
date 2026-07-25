import unittest
from unittest.mock import patch

from sourcing_agent.semantic_provider import LocalSemanticProvider, OfflineSemanticProvider, build_semantic_provider
from sourcing_agent.settings import SemanticProviderSettings


class SemanticProviderTest(unittest.TestCase):
    def test_local_provider_health_and_rerank(self) -> None:
        provider = LocalSemanticProvider()
        self.assertEqual(provider.healthcheck()["status"], "ready")
        ranked = provider.rerank("gemini post train", ["inference systems", "post training reinforcement learning"], top_n=2)
        self.assertEqual(len(ranked), 2)
        self.assertGreaterEqual(ranked[0]["relevance_score"], ranked[1]["relevance_score"])

    def test_local_provider_scores_media_records(self) -> None:
        provider = LocalSemanticProvider()
        scored = provider.score_media_records(
            "Gemini interview",
            [
                {"title": "Podcast with Gemini team", "snippet": "Interview about post-training", "url": "https://youtube.com/a"},
                {"title": "Unrelated cooking video", "snippet": "Recipe discussion", "url": "https://youtube.com/b"},
            ],
            top_n=2,
        )
        self.assertEqual(scored[0]["record"]["url"], "https://youtube.com/a")

    def test_build_semantic_provider_uses_offline_provider_in_simulate_mode(self) -> None:
        with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}):
            provider = build_semantic_provider(
                SemanticProviderSettings(
                    enabled=True,
                    api_key="sk-test",
                )
            )
        self.assertIsInstance(provider, OfflineSemanticProvider)
        self.assertEqual(provider.healthcheck()["provider_mode"], "simulate")
        ranked = provider.rerank("multimodal video", ["video generation", "finance"], top_n=2)
        self.assertEqual(len(ranked), 2)

    def test_build_semantic_provider_uses_offline_provider_in_replay_mode(self) -> None:
        with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "replay"}):
            provider = build_semantic_provider(
                SemanticProviderSettings(
                    enabled=True,
                    api_key="sk-test",
                )
            )
        self.assertIsInstance(provider, OfflineSemanticProvider)
        self.assertEqual(provider.healthcheck()["provider_mode"], "replay")

    def test_build_semantic_provider_uses_offline_provider_in_scripted_mode(self) -> None:
        with patch.dict("os.environ", {"SOURCING_EXTERNAL_PROVIDER_MODE": "scripted"}):
            provider = build_semantic_provider(
                SemanticProviderSettings(
                    enabled=True,
                    api_key="sk-test",
                )
            )
        self.assertIsInstance(provider, OfflineSemanticProvider)
        self.assertEqual(provider.healthcheck()["provider_mode"], "scripted")
