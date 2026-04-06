import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.agent_runtime import AgentRuntimeCoordinator
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.document_extraction import AnalyzedDocumentAsset, empty_signal_bundle
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import AppSettings, HarvestActorSettings, HarvestSettings, QwenSettings, SemanticProviderSettings
from sourcing_agent.storage import SQLiteStore


def _build_catalog(root: Path) -> AssetCatalog:
    return AssetCatalog(
        project_root=root,
        dev_root=root,
        anthropic_root=root,
        anthropic_workbook=root / "anthropic.xlsx",
        anthropic_readme=root / "README.md",
        anthropic_progress=root / "PROGRESS.md",
        legacy_api_accounts=root / "api_accounts.json",
        legacy_company_ids=root / "company_ids.json",
        anthropic_publications=root / "publications.json",
        scholar_scan_results=root / "scholar.json",
        investor_members_json=root / "investor.json",
        employee_scan_skill=root / "employee_skill.md",
        investor_scan_skill=root / "investor_skill.md",
        onepager_skill=root / "onepager_skill.md",
    )


class ManualReviewResolutionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.catalog = _build_catalog(self.root)
        self.store = SQLiteStore(self.root / "test.db")
        self.settings = AppSettings(
            project_root=self.root,
            runtime_dir=self.root,
            secrets_file=self.root / "providers.local.json",
            jobs_dir=self.root / "jobs",
            company_assets_dir=self.root / "company_assets",
            db_path=self.root / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.agent_runtime = AgentRuntimeCoordinator(self.store)
        self.acquisition_engine = AcquisitionEngine(
            self.catalog,
            self.settings,
            self.store,
            self.model_client,
            worker_runtime=self.agent_runtime,
        )
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=self.root / "jobs",
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
            agent_runtime=self.agent_runtime,
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_ad_hoc_manual_review_resolution_upserts_candidate_and_evidence(self) -> None:
        payload = {
            "action": "resolved",
            "reviewer": "tester",
            "notes": "Manual review confirmed the LinkedIn profile.",
            "target_company": "Thinking Machines Lab",
            "candidate": {
                "candidate_id": "lead_kevin",
                "name_en": "Kevin Lu",
                "display_name": "Kevin Lu",
                "category": "lead",
                "target_company": "Thinking Machines Lab",
                "organization": "Thinking Machines Lab",
                "role": "Publication author lead",
                "source_dataset": "thinkingmachinesai_official_feed",
                "source_path": str(self.root / "publication_leads.json"),
                "metadata": {"publication_url": "https://thinkingmachines.ai/blog/on-policy-distillation/"},
            },
            "candidate_patch": {
                "category": "employee",
                "employment_status": "current",
                "role": "Research Engineer",
            },
            "fetch_assets": False,
            "source_links": [
                {
                    "label": "LinkedIn",
                    "url": "https://www.linkedin.com/in/kzl/",
                    "source_type": "manual_review_link",
                    "notes": "Found via Google search during manual review.",
                }
            ],
        }

        result = self.orchestrator.review_manual_review_item(payload)
        self.assertEqual(result["status"], "applied_without_queue")
        candidate = self.store.get_candidate("lead_kevin")
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.category, "employee")
        self.assertEqual(candidate.employment_status, "current")
        self.assertEqual(candidate.linkedin_url, "https://www.linkedin.com/in/kzl/")
        evidence = self.store.list_evidence("lead_kevin")
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]["url"], "https://www.linkedin.com/in/kzl/")
        artifact_root = Path(result["resolution"]["artifact_root"])
        self.assertTrue((artifact_root / "resolution_input.json").exists())
        self.assertTrue((artifact_root / "sources" / "source_01.json").exists())

    def test_manual_review_resolution_structures_resume_signals_into_candidate(self) -> None:
        signal_bundle = empty_signal_bundle()
        signal_bundle["resume_urls"] = ["https://horace.io/files/horace.pdf"]
        signal_bundle["validated_summaries"] = ["Horace He current profile mentions Thinking Machines Lab."]
        signal_bundle["role_signals"] = ["compiler", "research"]
        signal_bundle["education_signals"] = [
            {"school": "MIT", "degree": "BS", "field": "Computer Science", "date_range": "2016-2020"}
        ]
        signal_bundle["work_history_signals"] = [
            {"title": "Research Engineer", "organization": "Thinking Machines Lab", "date_range": "2025-Present"}
        ]
        signal_bundle["affiliation_signals"] = [
            {"organization": "Thinking Machines Lab", "relation": "explicit_current_affiliation", "evidence": "Research Engineer at Thinking Machines Lab"}
        ]

        payload = {
            "action": "resolved",
            "reviewer": "tester",
            "target_company": "Thinking Machines Lab",
            "candidate": {
                "candidate_id": "lead_horace",
                "name_en": "Horace He",
                "display_name": "Horace He",
                "category": "lead",
                "target_company": "Thinking Machines Lab",
                "organization": "",
                "source_dataset": "thinkingmachinesai_official_feed",
                "source_path": str(self.root / "publication_leads.json"),
                "metadata": {},
            },
            "source_links": [
                {
                    "label": "Resume",
                    "url": "https://horace.io/files/horace.pdf",
                    "source_type": "manual_review_resume",
                }
            ],
        }

        fake_asset = AnalyzedDocumentAsset(
            source_url="https://horace.io/files/horace.pdf",
            final_url="https://horace.io/files/horace.pdf",
            content_type="application/pdf",
            document_type="pdf_resume",
            raw_path=str(self.root / "raw.pdf"),
            extracted_text_path=str(self.root / "raw.txt"),
            analysis_input_path=str(self.root / "input.json"),
            analysis_path=str(self.root / "analysis.json"),
            signals=signal_bundle,
            analysis={
                "summary": "Horace He resume confirms Thinking Machines Lab affiliation.",
                "role_signals": ["compiler", "research"],
                "education_signals": signal_bundle["education_signals"],
                "work_history_signals": signal_bundle["work_history_signals"],
                "affiliation_signals": signal_bundle["affiliation_signals"],
                "recommended_links": {"resume_url": "https://horace.io/files/horace.pdf"},
                "document_type": "pdf_resume",
            },
        )

        with patch("sourcing_agent.manual_review_resolution.analyze_remote_document", return_value=fake_asset):
            result = self.orchestrator.review_manual_review_item(payload)

        self.assertEqual(result["status"], "applied_without_queue")
        candidate = self.store.get_candidate("lead_horace")
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertIn("MIT", candidate.education)
        self.assertIn("Thinking Machines Lab", candidate.work_history)
        self.assertEqual(candidate.organization, "Thinking Machines Lab")
        self.assertEqual(candidate.media_url, "https://horace.io/files/horace.pdf")


if __name__ == "__main__":
    unittest.main()
