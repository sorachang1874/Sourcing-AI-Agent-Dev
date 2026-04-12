import unittest

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.scoring import score_candidates
from sourcing_agent.semantic_retrieval import rank_semantic_candidates


class _FakeRemoteSemanticProvider:
    def __init__(self) -> None:
        self.embed_calls = 0
        self.rerank_calls = 0

    def provider_name(self) -> str:
        return "fake_remote"

    def healthcheck(self) -> dict:
        return {"provider": "fake_remote", "status": "ready"}

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        self.embed_calls += 1
        return [[1.0] for _ in texts]

    def rerank(self, query: str, documents: list[str], *, top_n: int) -> list[dict]:
        self.rerank_calls += 1
        return [
            {
                "index": 0,
                "document": documents[0] if documents else "",
                "relevance_score": 0.9,
            }
        ]

    def score_media_records(self, query: str, records: list[dict], *, top_n: int) -> list[dict]:
        return []


class SemanticRetrievalTest(unittest.TestCase):
    def test_semantic_retrieval_prefers_intent_view_over_conflicting_flat_fields(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_gemini_pm",
                name_en="Gemini PM",
                display_name="Gemini PM",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Product Manager",
                team="Gemini",
                focus_areas="Gemini product strategy",
            )
        ]
        request = JobRequest.from_payload(
            {
                "raw_user_request": "找 Gemini 的产品经理",
                "query": "Gemini product manager",
                "target_company": "WrongCo",
                "semantic_rerank_limit": 5,
                "intent_axes": {
                    "population_boundary": {
                        "categories": ["employee"],
                        "employment_statuses": ["current", "former"],
                    },
                    "scope_boundary": {
                        "target_company": "Google",
                        "organization_keywords": ["Google DeepMind", "Gemini"],
                    },
                    "thematic_constraints": {
                        "must_have_primary_role_buckets": ["product_management"],
                        "keywords": ["Gemini"],
                    },
                },
            }
        )

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["role", "team", "focus_areas"],
            limit=5,
        )
        self.assertIn("cand_gemini_pm", semantic_hits)

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

    def test_semantic_notes_field_uses_profile_metadata_summary(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_profile_meta",
                name_en="Profile Meta Person",
                display_name="Profile Meta Person",
                category="employee",
                target_company="Google",
                organization="Google DeepMind",
                employment_status="current",
                role="Software Engineer",
                metadata={
                    "summary": "Builds infrastructure platforms for multimodal training and cluster scheduling.",
                    "skills": ["Distributed Systems", "Kubernetes"],
                },
            )
        ]
        request = JobRequest(
            raw_user_request="Find Google infra people",
            query="infra kubernetes",
            target_company="Google",
            categories=["employee"],
            employment_statuses=["current"],
            top_k=5,
            semantic_rerank_limit=5,
        )

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["notes"],
            limit=5,
        )
        self.assertIn("cand_profile_meta", semantic_hits)

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

    def test_role_like_categories_filter_on_facets_not_membership_category(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_researcher",
                name_en="Research Person",
                display_name="Research Person",
                category="employee",
                target_company="Humans&",
                organization="Humans&",
                employment_status="current",
                role="Research Scientist",
                focus_areas="coding agents",
            ),
            Candidate(
                candidate_id="cand_recruiter",
                name_en="Recruiting Person",
                display_name="Recruiting Person",
                category="employee",
                target_company="Humans&",
                organization="Humans&",
                employment_status="current",
                role="Technical Recruiter",
                focus_areas="talent acquisition",
            ),
            Candidate(
                candidate_id="cand_former_researcher",
                name_en="Former Researcher",
                display_name="Former Researcher",
                category="former_employee",
                target_company="Humans&",
                organization="Humans&",
                employment_status="former",
                role="Research Engineer",
                focus_areas="code generation",
            ),
        ]
        request = JobRequest(
            raw_user_request="我想了解 Humans& 的 Coding 方向的 Researcher。",
            target_company="Humans&",
            categories=["employee", "researcher"],
            employment_statuses=["current"],
        )

        scored = score_candidates(candidates, request)
        self.assertEqual([item.candidate.candidate_id for item in scored], ["cand_researcher"])

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

    def test_remote_semantic_provider_is_skipped_for_low_cost_requests(self) -> None:
        candidates = [
            Candidate(
                candidate_id="cand_reasoning",
                name_en="Reasoning Person",
                display_name="Reasoning Person",
                category="employee",
                target_company="OpenAI",
                organization="OpenAI",
                employment_status="current",
                role="Research Scientist",
                focus_areas="reasoning and reinforcement learning",
            ),
        ]
        request = JobRequest(
            raw_user_request="Find OpenAI reasoning researchers",
            query="reasoning researchers",
            target_company="OpenAI",
            categories=["employee"],
            employment_statuses=["current"],
            semantic_rerank_limit=5,
        )
        fake_provider = _FakeRemoteSemanticProvider()

        semantic_hits = rank_semantic_candidates(
            candidates,
            request,
            semantic_fields=["role", "focus_areas"],
            limit=5,
            semantic_provider=fake_provider,
        )

        self.assertIn("cand_reasoning", semantic_hits)
        self.assertEqual(fake_provider.embed_calls, 0)
        self.assertEqual(fake_provider.rerank_calls, 0)
