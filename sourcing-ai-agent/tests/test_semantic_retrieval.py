import unittest

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.scoring import score_candidates
from sourcing_agent.semantic_retrieval import rank_semantic_candidates


class SemanticRetrievalTest(unittest.TestCase):
    def test_semantic_hit_recovers_post_train_variant(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_post_train",
                name_en="Taylor Trainer",
                display_name="Taylor Trainer",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Applied Scientist",
                team="Gemini",
                focus_areas="post training reinforcement learning",
                notes="Works on reward modeling and post training systems.",
            ),
            Candidate(
                candidate_id="cand_inference",
                name_en="Inference Person",
                display_name="Inference Person",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Software Engineer",
                focus_areas="inference serving systems",
                notes="Owns low-latency serving stack.",
            ),
        ]
        request = JobRequest(
            raw_user_request="给我 Gemini Team 的 post-train researcher",
            query="post-train researcher",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        without_semantic = score_candidates(candidates, request)
        self.assertEqual(len(without_semantic), 0)

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["role", "team", "focus_areas", "notes"],
            limit=5,
        )
        self.assertIn("cand_post_train", semantic_hits)
        self.assertGreater(semantic_hits["cand_post_train"]["semantic_score"], 0.0)

        with_semantic = score_candidates(candidates, request, semantic_hits=semantic_hits)
        self.assertEqual(len(with_semantic), 1)
        self.assertEqual(with_semantic[0].candidate.candidate_id, "cand_post_train")
        self.assertGreater(with_semantic[0].semantic_score, 0.0)
