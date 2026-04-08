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

    def test_roster_baseline_boilerplate_notes_do_not_create_false_matches(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_roster_only",
                name_en="Boilerplate Person",
                display_name="Boilerplate Person",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Member of Technical Staff",
                notes="LinkedIn company roster baseline. Location: Platform City. Source account: platform-seed.",
            ),
            Candidate(
                candidate_id="cand_platform",
                name_en="Platform Person",
                display_name="Platform Person",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Software Engineer",
                focus_areas="platform engineering for training systems",
            ),
        ]
        request = JobRequest(
            raw_user_request="Find Google platform engineers",
            query="platform",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        scored = score_candidates(candidates, request)
        self.assertEqual([item.candidate.candidate_id for item in scored], ["cand_platform"])

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["focus_areas", "notes"],
            limit=5,
        )
        self.assertIn("cand_platform", semantic_hits)
        self.assertNotIn("cand_roster_only", semantic_hits)

    def test_derived_facets_support_ops_queries(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_ops",
                name_en="Morgan Ops",
                display_name="Morgan Ops",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Chief of Staff",
                notes="Supports research leadership operations.",
            ),
            Candidate(
                candidate_id="cand_research",
                name_en="Riley Research",
                display_name="Riley Research",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Research Scientist",
                focus_areas="reasoning research",
            ),
        ]
        request = JobRequest(
            raw_user_request="Find Google ops talent",
            query="ops",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        scored = score_candidates(candidates, request)
        self.assertEqual([item.candidate.candidate_id for item in scored], ["cand_ops"])

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["role", "derived_facets", "notes"],
            limit=5,
        )
        self.assertIn("cand_ops", semantic_hits)
        self.assertNotIn("cand_research", semantic_hits)

    def test_must_have_facet_filters_out_note_only_leakage(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_recruiter",
                name_en="Taylor Recruiter",
                display_name="Taylor Recruiter",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Technical Recruiter",
                team="Talent",
                focus_areas="technical recruiting",
            ),
            Candidate(
                candidate_id="cand_engineer",
                name_en="Jordan Engineer",
                display_name="Jordan Engineer",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Software Engineer",
                focus_areas="training systems",
                notes="Enjoys helping with recruiting and community outreach.",
            ),
        ]
        request = JobRequest(
            raw_user_request="Find Google recruiting talent",
            query="recruiting",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_facets=["recruiting"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        scored = score_candidates(candidates, request)
        self.assertEqual([item.candidate.candidate_id for item in scored], ["cand_recruiter"])

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["role", "team", "derived_facets", "notes"],
            limit=5,
        )
        self.assertIn("cand_recruiter", semantic_hits)
        self.assertNotIn("cand_engineer", semantic_hits)

    def test_primary_role_bucket_filter_excludes_leadership_with_secondary_infra_signal(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_founder",
                name_en="Founding Infra Leader",
                display_name="Founding Infra Leader",
                category="employee",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                employment_status="current",
                role="Co-founder and CTO",
                focus_areas="infrastructure and distributed systems",
            ),
            Candidate(
                candidate_id="cand_infra",
                name_en="Infra Engineer",
                display_name="Infra Engineer",
                category="employee",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                employment_status="current",
                role="Infrastructure Engineer",
                focus_areas="gpu cluster runtime platform",
            ),
        ]
        facet_only_request = JobRequest(
            raw_user_request="Find TML infra systems people",
            query="infra systems",
            target_company="Thinking Machines Lab",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_facets=["infra_systems"],
            top_k=5,
            semantic_rerank_limit=5,
        )
        primary_bucket_request = JobRequest(
            raw_user_request="Find TML infra systems people",
            query="infra systems",
            target_company="Thinking Machines Lab",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_facets=["infra_systems"],
            must_have_primary_role_buckets=["infra_systems"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        facet_only_scored = score_candidates(candidates, facet_only_request)
        self.assertCountEqual(
            [item.candidate.candidate_id for item in facet_only_scored],
            ["cand_founder", "cand_infra"],
        )

        scored = score_candidates(candidates, primary_bucket_request)
        self.assertEqual([item.candidate.candidate_id for item in scored], ["cand_infra"])

    def test_primary_role_bucket_query_excludes_notes_from_lexical_and_semantic_matches(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_infra_notes",
                name_en="Infra Notes Person",
                display_name="Infra Notes Person",
                category="employee",
                target_company="Thinking Machines Lab",
                organization="Thinking Machines Lab",
                employment_status="current",
                role="Infrastructure Engineer",
                focus_areas="cluster runtime platform",
                notes="Loves infra and systems work across the stack.",
            ),
        ]
        request = JobRequest(
            raw_user_request="Find TML infra systems people",
            query="infra systems",
            target_company="Thinking Machines Lab",
            categories=["employee"],
            employment_statuses=["current"],
            must_have_facets=["infra_systems"],
            must_have_primary_role_buckets=["infra_systems"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        scored = score_candidates(candidates, request)
        self.assertEqual(len(scored), 1)
        self.assertTrue(all(item["field"] != "notes" for item in scored[0].matched_fields))

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["role", "focus_areas", "derived_facets", "notes"],
            limit=5,
        )
        self.assertIn("cand_infra_notes", semantic_hits)
        self.assertTrue(
            all(item["field"] != "notes" for item in semantic_hits["cand_infra_notes"]["matched_fields"])
        )
