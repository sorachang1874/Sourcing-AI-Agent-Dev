import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile
from xml.sax.saxutils import escape

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.excel_intake import ExcelIntakeService
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.settings import AppSettings, HarvestActorSettings, HarvestSettings, QwenSettings, SemanticProviderSettings
from sourcing_agent.storage import SQLiteStore


def _column_ref(index: int) -> str:
    value = index + 1
    result = ""
    while value:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _write_inline_workbook(path: Path, *, sheet_name: str, headers: list[str], rows: list[list[str]]) -> None:
    def _cell_xml(row_index: int, column_index: int, value: str) -> str:
        ref = f"{_column_ref(column_index)}{row_index}"
        return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value or ""))}</t></is></c>'

    sheet_rows = [headers, *rows]
    row_xml = []
    for row_index, row in enumerate(sheet_rows, start=1):
        cells = "".join(_cell_xml(row_index, column_index, value) for column_index, value in enumerate(row))
        row_xml.append(f'<row r="{row_index}">{cells}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        "<sheetData>"
        + "".join(row_xml)
        + "</sheetData></worksheet>"
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        f'<sheets><sheet name="{escape(sheet_name)}" sheetId="1" r:id="rId1"/></sheets>'
        "</workbook>"
    )
    workbook_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
        'Target="worksheets/sheet1.xml"/>'
        "</Relationships>"
    )
    root_rels_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    content_types_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        "</Types>"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("[Content_Types].xml", content_types_xml)
        archive.writestr("_rels/.rels", root_rels_xml)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)


class ExcelIntakeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        root = Path(self.tempdir.name)
        self.store = SQLiteStore(root / "test.db")
        self.settings = AppSettings(
            project_root=root,
            runtime_dir=root,
            secrets_file=root / "providers.local.json",
            jobs_dir=root / "jobs",
            company_assets_dir=root / "company_assets",
            db_path=root / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(
                profile_scraper=HarvestActorSettings(enabled=False),
                profile_search=HarvestActorSettings(enabled=False),
            ),
        )
        self.service = ExcelIntakeService(
            runtime_dir=root,
            store=self.store,
            settings=self.settings,
            model_client=DeterministicModelClient(),
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_ingest_excel_contacts_handles_local_exact_hit_and_local_manual_review(self) -> None:
        alice = Candidate(
            candidate_id="alice-1",
            name_en="Alice Zhang",
            display_name="Alice Zhang",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Infra Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-zhang/",
        )
        bobby = Candidate(
            candidate_id="bobby-1",
            name_en="Bobby Example",
            display_name="Bobby Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/bobby-example/",
        )
        self.store.upsert_candidate(alice)
        self.store.upsert_candidate(bobby)

        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title", "Linkedin"],
            rows=[
                ["Alice Zhang", "Reflection AI", "Infra Engineer", "https://www.linkedin.com/in/alice-zhang/"],
                ["Bob Example", "Reflection AI", "Infra Engineer", ""],
            ],
        )

        result = self.service.ingest_contacts({"file_path": str(workbook_path)})
        statuses = {item["row_key"]: item["status"] for item in result["results"]}
        self.assertEqual(statuses["Contacts#1"], "local_exact_hit")
        self.assertEqual(statuses["Contacts#2"], "manual_review_local")
        self.assertEqual(result["summary"]["local_exact_hit_count"], 1)
        self.assertEqual(result["summary"]["manual_review_local_count"], 1)

    def test_ingest_excel_contacts_can_fetch_via_search_and_persist_candidate(self) -> None:
        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title"],
            rows=[
                ["Dana New", "Reflection AI", "Infra Engineer"],
            ],
        )
        fetched_payload = {
            "https://www.linkedin.com/in/dana-new/": {
                "raw_path": str(Path(self.tempdir.name) / "harvest_profiles" / "dana-new.json"),
                "parsed": {
                    "full_name": "Dana New",
                    "headline": "Infra Engineer at Reflection AI",
                    "profile_url": "https://www.linkedin.com/in/dana-new/",
                    "current_company": "Reflection AI",
                    "summary": "Works on infrastructure systems.",
                    "location": "San Francisco, California, United States",
                    "education": [{"school": "Stanford University", "degree": "MS", "field": "CS"}],
                    "experience": [{"title": "Infra Engineer", "companyName": "Reflection AI"}],
                    "languages": ["English"],
                    "skills": ["Infrastructure", "Distributed Systems"],
                    "public_identifier": "dana-new",
                },
            }
        }
        with (
            patch.object(
                self.service,
                "_search_contact",
                return_value={
                    "status": "completed",
                    "attempts": [],
                    "ranked_candidates": [
                        {
                            "full_name": "Dana New",
                            "profile_url": "https://www.linkedin.com/in/dana-new/",
                            "headline": "Infra Engineer at Reflection AI",
                            "current_company": "Reflection AI",
                            "match_score": 96.0,
                        }
                    ],
                    "manual_review_candidates": [],
                    "auto_fetch_profile_url": "https://www.linkedin.com/in/dana-new/",
                },
            ),
            patch.object(self.service, "_fetch_profiles", return_value=(fetched_payload, [])),
        ):
            result = self.service.ingest_contacts({"file_path": str(workbook_path)})

        self.assertEqual(result["summary"]["fetched_via_search_count"], 1)
        self.assertEqual(result["summary"]["persisted_candidate_count"], 1)
        stored_candidates = self.store.list_candidates()
        self.assertEqual(len(stored_candidates), 1)
        self.assertEqual(stored_candidates[0].name_en, "Dana New")
        self.assertIn("Stanford University", stored_candidates[0].education)
        self.assertIn("Reflection AI", stored_candidates[0].work_history)

    def test_ingest_excel_contacts_prefers_direct_linkedin_fetch_over_local_near_match(self) -> None:
        noisy_candidate = Candidate(
            candidate_id="noise-1",
            name_en="Shun Yao",
            display_name="Shun Yao",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Research Scientist",
            linkedin_url="https://www.linkedin.com/in/shun-yao/",
        )
        self.store.upsert_candidate(noisy_candidate)

        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title", "Linkedin"],
            rows=[
                ["Shunyu Yao", "Google", "Research Scientist", "https://www.linkedin.com/in/shunyu-yao/"],
            ],
        )

        fetched_payload = {
            "https://www.linkedin.com/in/shunyu-yao/": {
                "raw_path": str(Path(self.tempdir.name) / "harvest_profiles" / "shunyu-yao.json"),
                "parsed": {
                    "full_name": "Shunyu Yao",
                    "headline": "Research Scientist at Google",
                    "profile_url": "https://www.linkedin.com/in/shunyu-yao/",
                    "current_company": "Google",
                    "summary": "Works on multimodal systems.",
                    "location": "Mountain View, California, United States",
                    "education": [{"school": "Tsinghua University", "degree": "BS", "field": "Computer Science"}],
                    "experience": [{"title": "Research Scientist", "companyName": "Google"}],
                    "languages": ["English"],
                    "skills": ["Multimodal", "Machine Learning"],
                    "public_identifier": "shunyu-yao",
                },
            }
        }

        with (
            patch.object(self.service, "_fetch_profiles", return_value=(fetched_payload, [])) as mocked_fetch,
            patch.object(self.service, "_search_contact") as mocked_search,
        ):
            result = self.service.ingest_contacts({"file_path": str(workbook_path)})

        self.assertEqual(result["results"][0]["status"], "fetched_direct_linkedin")
        self.assertEqual(result["summary"]["fetched_direct_linkedin_count"], 1)
        self.assertEqual(result["summary"]["manual_review_local_count"], 0)
        mocked_fetch.assert_called_once()
        mocked_search.assert_not_called()

    def test_ingest_excel_contacts_enriches_thin_local_exact_hit_with_uploaded_linkedin(self) -> None:
        thin_exact = Candidate(
            candidate_id="piaoyang-1",
            name_en="Piaoyang Cui",
            display_name="Piaoyang Cui",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Senior Staff Research Engineer at Google DeepMind",
            team="Research",
            focus_areas="Senior Staff Research Engineer at Google DeepMind",
            notes="LinkedIn company roster baseline. Location: United States. Source account: harvest_company_employees.",
            linkedin_url="https://www.linkedin.com/in/ACwAAAc1QCkBmjgoVxfvxHNsV8i8dTdant5jGEM",
            source_dataset="google_linkedin_company_people",
            metadata={"source_account_id": "harvest_company_employees"},
        )
        self.store.upsert_candidate(thin_exact)

        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title", "Linkedin"],
            rows=[
                ["Piaoyang Cui", "Google Deepmind", "Senior Staff", "https://www.linkedin.com/in/piaoyang/"],
            ],
        )

        fetched_payload = {
            "https://www.linkedin.com/in/piaoyang/": {
                "raw_path": str(Path(self.tempdir.name) / "harvest_profiles" / "piaoyang.json"),
                "parsed": {
                    "full_name": "Piaoyang Cui",
                    "headline": "Senior Staff Research Engineer at Google DeepMind",
                    "profile_url": "https://www.linkedin.com/in/piaoyang/",
                    "current_company": "Google DeepMind",
                    "summary": "Works on multimodal generation systems.",
                    "location": "Mountain View, California, United States",
                    "education": [{"school": "Tsinghua University", "degree": "PhD", "field": "Computer Science"}],
                    "experience": [{"title": "Senior Staff Research Engineer", "companyName": "Google DeepMind"}],
                    "languages": ["English", "Chinese"],
                    "skills": ["Multimodal", "Generative Models"],
                    "public_identifier": "piaoyang",
                },
            }
        }

        with (
            patch.object(self.service, "_fetch_profiles", return_value=(fetched_payload, [])) as mocked_fetch,
            patch.object(self.service, "_search_contact") as mocked_search,
        ):
            result = self.service.ingest_contacts({"file_path": str(workbook_path)})

        self.assertEqual(result["results"][0]["status"], "fetched_direct_linkedin")
        self.assertTrue(result["results"][0]["enriched_local_exact_hit"])
        self.assertEqual(result["summary"]["local_exact_hit_count"], 0)
        self.assertEqual(result["summary"]["fetched_direct_linkedin_count"], 1)
        mocked_fetch.assert_called_once()
        mocked_search.assert_not_called()

        stored_candidates = self.store.list_candidates()
        self.assertEqual(len(stored_candidates), 1)
        self.assertEqual(stored_candidates[0].candidate_id, "piaoyang-1")
        self.assertEqual(stored_candidates[0].category, "employee")
        self.assertEqual(stored_candidates[0].linkedin_url.rstrip("/"), "https://www.linkedin.com/in/piaoyang")
        self.assertIn("Tsinghua University", stored_candidates[0].education)
        self.assertIn("Google DeepMind", stored_candidates[0].work_history)
        self.assertIn("multimodal generation systems", stored_candidates[0].notes.lower())

    def test_ingest_excel_contacts_repairs_stale_target_company_mismatch_on_local_exact_hit(self) -> None:
        stale_candidate = Candidate(
            candidate_id="piaoyang-2",
            name_en="Piaoyang Cui",
            display_name="Piaoyang Cui",
            category="non_member",
            target_company="Google",
            organization="Google DeepMind",
            employment_status="current",
            role="Agent Research",
            linkedin_url="https://www.linkedin.com/in/piaoyang/",
            education="Master of Science, Columbia University",
            work_history="Google DeepMind, Senior Staff Research Engineer",
            source_dataset="excel_intake",
            metadata={
                "public_identifier": "piaoyang",
                "headline": "Agent Research",
                "target_company_mismatch": True,
            },
        )
        self.store.upsert_candidate(stale_candidate)

        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title", "Linkedin"],
            rows=[
                ["Piaoyang Cui", "Google Deepmind", "Senior Staff", "https://www.linkedin.com/in/piaoyang/"],
            ],
        )

        result = self.service.ingest_contacts({"file_path": str(workbook_path)})

        self.assertEqual(result["results"][0]["status"], "local_exact_hit")
        self.assertTrue(result["results"][0]["repaired_local_exact_candidate"])
        stored_candidates = self.store.list_candidates()
        self.assertEqual(len(stored_candidates), 1)
        self.assertEqual(stored_candidates[0].category, "employee")
        self.assertFalse(bool((stored_candidates[0].metadata or {}).get("target_company_mismatch")))

    def test_ingest_excel_contacts_matches_existing_candidate_by_linkedin_slug_and_email(self) -> None:
        existing = Candidate(
            candidate_id="candidate-1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Infra Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            metadata={
                "public_identifier": "alice-example",
                "excel_uploaded_email": "alice@example.com",
            },
        )
        self.store.upsert_candidate(existing)

        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title", "Linkedin", "Email"],
            rows=[
                ["Alice Example", "Reflection AI", "Infra Engineer", "https://www.linkedin.com/in/alice-example/?trk=public_profile", ""],
                ["Alice Example", "Reflection AI", "Infra Engineer", "", "alice@example.com"],
            ],
        )

        result = self.service.ingest_contacts({"file_path": str(workbook_path)})

        statuses = {item["row_key"]: item["status"] for item in result["results"]}
        reasons = {item["row_key"]: item.get("match_reason") for item in result["results"]}
        self.assertEqual(statuses["Contacts#1"], "local_exact_hit")
        self.assertEqual(statuses["Contacts#2"], "local_exact_hit")
        self.assertEqual(reasons["Contacts#1"], "linkedin_url")
        self.assertEqual(reasons["Contacts#2"], "email")

    def test_continue_excel_intake_can_resolve_manual_review_local_selection(self) -> None:
        candidate = Candidate(
            candidate_id="bobby-1",
            name_en="Bobby Example",
            display_name="Bobby Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/bobby-example/",
        )
        self.store.upsert_candidate(candidate)

        workbook_path = Path(self.tempdir.name) / "contacts.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title"],
            rows=[
                ["Bob Example", "Reflection AI", "Infra Engineer"],
            ],
        )

        initial = self.service.ingest_contacts({"file_path": str(workbook_path)})
        self.assertEqual(initial["results"][0]["status"], "manual_review_local")

        continued = self.service.continue_review(
            {
                "intake_id": str(initial["intake_id"]),
                "decisions": [
                    {
                        "row_key": "Contacts#1",
                        "action": "select_local_candidate",
                        "selected_candidate_id": "bobby-1",
                    }
                ],
            }
        )

        self.assertEqual(continued["status"], "completed")
        self.assertEqual(continued["results"][0]["status"], "resolved_manual_review_local")
        self.assertEqual(continued["results"][0]["matched_candidate"]["candidate_id"], "bobby-1")

    def test_job_request_defaults_disable_semantic_rerank(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的 Infra 成员",
                "target_company": "Reflection AI",
            }
        )
        self.assertEqual(request.semantic_rerank_limit, 0)
