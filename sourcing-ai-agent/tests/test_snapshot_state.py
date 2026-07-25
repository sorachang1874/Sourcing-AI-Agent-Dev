import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.snapshot_state import load_candidate_document_state


class SnapshotStateTest(unittest.TestCase):
    def test_candidate_documents_loader_accepts_additive_serving_fields(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            candidate_doc_path = Path(tempdir) / "candidate_documents.json"
            candidate_doc_path.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "candidate_id": "openai-1",
                                "name_en": "OpenAI Reasoning",
                                "display_name": "OpenAI Reasoning",
                                "category": "employee",
                                "target_company": "OpenAI",
                                "employment_status": "current",
                                "linkedin_url": "https://www.linkedin.com/in/openai-reasoning/",
                                "headline": "Research Scientist at OpenAI",
                                "experience_lines": ["OpenAI, Research Scientist"],
                                "education_lines": ["MS, Stanford University"],
                                "has_profile_detail": True,
                                "needs_profile_completion": False,
                                "profile_capture_kind": "seeded_reference_profile",
                                "metadata": {"profile_url": "https://www.linkedin.com/in/openai-reasoning/"},
                            }
                        ],
                        "evidence": [
                            {
                                "candidate_id": "openai-1",
                                "source_type": "linkedin_profile",
                                "title": "OpenAI Reasoning",
                                "url": "https://www.linkedin.com/in/openai-reasoning/",
                                "rank": 1,
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            state = load_candidate_document_state(candidate_doc_path)

        candidates = list(state.get("candidates") or [])
        evidence = list(state.get("evidence") or [])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].candidate_id, "openai-1")
        self.assertEqual(candidates[0].metadata["headline"], "Research Scientist at OpenAI")
        self.assertEqual(candidates[0].metadata["experience_lines"], ["OpenAI, Research Scientist"])
        self.assertTrue(candidates[0].metadata["has_profile_detail"])
        self.assertEqual(len(evidence), 1)
        self.assertTrue(evidence[0].evidence_id)
        self.assertEqual(evidence[0].metadata["rank"], 1)

    def test_candidate_documents_loader_restores_linkedin_stage_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            candidate_doc_path = Path(tempdir) / "candidate_documents.json"
            candidate_doc_path.write_text(
                json.dumps(
                    {
                        "acquisition_stage": {
                            "phase": "linkedin_stage_1",
                            "task_type": "enrich_linkedin_profiles",
                        },
                        "enrichment_scope": "linkedin_stage_1",
                        "enrichment_summary": {
                            "profile_prefetch": {
                                "status": "completed",
                                "requested_url_count": 1,
                                "registry_terminal_summary": {
                                    "requested_url_count": 1,
                                    "terminal_url_count": 1,
                                    "open_url_count": 0,
                                    "all_requested_terminal": True,
                                },
                            }
                        },
                        "candidates": [
                            {
                                "candidate_id": "lovable-1",
                                "name_en": "Lovable Engineer",
                                "display_name": "Lovable Engineer",
                                "category": "employee",
                                "target_company": "Lovable",
                                "employment_status": "current",
                                "linkedin_url": "https://www.linkedin.com/in/lovable-engineer/",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state = load_candidate_document_state(candidate_doc_path)

        self.assertTrue(state["linkedin_stage_completed"])
        self.assertEqual(state["linkedin_stage_candidate_doc_path"], candidate_doc_path)

    def test_candidate_documents_loader_does_not_restore_open_linkedin_stage_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            candidate_doc_path = Path(tempdir) / "candidate_documents.json"
            candidate_doc_path.write_text(
                json.dumps(
                    {
                        "acquisition_stage": {
                            "phase": "linkedin_stage_1",
                            "task_type": "enrich_linkedin_profiles",
                        },
                        "enrichment_scope": "linkedin_stage_1",
                        "enrichment_summary": {
                            "profile_prefetch": {
                                "status": "queued",
                                "requested_url_count": 25,
                                "registry_terminal_summary": {
                                    "requested_url_count": 25,
                                    "terminal_url_count": 0,
                                    "open_url_count": 25,
                                    "all_requested_terminal": False,
                                },
                            }
                        },
                        "candidates": [
                            {
                                "candidate_id": "lovable-open-stage",
                                "name_en": "Lovable Open Stage",
                                "display_name": "Lovable Open Stage",
                                "category": "employee",
                                "target_company": "Lovable",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state = load_candidate_document_state(candidate_doc_path)

        self.assertNotIn("linkedin_stage_completed", state)
        self.assertNotIn("linkedin_stage_candidate_doc_path", state)

    def test_candidate_documents_loader_does_not_mark_stage_for_plain_candidate_docs(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            candidate_doc_path = Path(tempdir) / "candidate_documents.json"
            candidate_doc_path.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "candidate_id": "plain-1",
                                "name_en": "Plain Candidate",
                                "display_name": "Plain Candidate",
                                "category": "employee",
                                "target_company": "Plain Co",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            state = load_candidate_document_state(candidate_doc_path)

        self.assertNotIn("linkedin_stage_completed", state)
        self.assertNotIn("linkedin_stage_candidate_doc_path", state)


if __name__ == "__main__":
    unittest.main()
