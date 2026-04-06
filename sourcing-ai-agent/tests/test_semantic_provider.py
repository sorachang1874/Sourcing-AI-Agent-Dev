import unittest

from sourcing_agent.semantic_provider import LocalSemanticProvider


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
