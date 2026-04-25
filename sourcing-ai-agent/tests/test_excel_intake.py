import base64
import json
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch
from xml.sax.saxutils import escape

from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.excel_intake import (
    ExcelIntakeService,
    build_excel_intake_throughput_plan,
    _build_local_candidate_inventory,
    _find_exact_local_linkedin_match,
    group_contacts_by_company_hints,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
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
        "<sheetData>" + "".join(row_xml) + "</sheetData></worksheet>"
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

    def _seed_snapshot(self, *, company_key: str, canonical_name: str, snapshot_id: str) -> Path:
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity_payload = {
            "requested_name": canonical_name,
            "canonical_name": canonical_name,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "linkedin_company_url": f"https://www.linkedin.com/company/{company_key}/",
        }
        (snapshot_dir / "identity.json").write_text(json.dumps(identity_payload, ensure_ascii=False, indent=2))
        (snapshot_dir.parent / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": identity_payload}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False)
        )
        return snapshot_dir

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
        self.assertEqual(result["throughput_plan"]["total_rows"], 2)
        self.assertEqual(result["throughput_plan"]["company_group_count"], 1)

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

    def test_prepare_contacts_accepts_base64_upload_without_file_path(self) -> None:
        workbook_path = Path(self.tempdir.name) / "contacts-upload.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title"],
            rows=[
                ["Ada Import", "OpenAI", "Research Engineer"],
            ],
        )

        prepared = self.service.prepare_contacts(
            {
                "file_path": "",
                "filename": "contacts-upload.xlsx",
                "file_content_base64": base64.b64encode(workbook_path.read_bytes()).decode("ascii"),
            },
            intake_dir=Path(self.tempdir.name) / "upload-intake",
        )

        self.assertEqual(int(dict(prepared["workbook"]).get("detected_contact_row_count") or 0), 1)
        contacts = list(prepared.get("contacts") or [])
        self.assertEqual(len(contacts), 1)
        self.assertEqual(str(contacts[0].get("name") or ""), "Ada Import")

    def test_excel_intake_throughput_plan_groups_companies_and_batches_profile_fetch(self) -> None:
        plan = build_excel_intake_throughput_plan(
            [
                {
                    "row_key": "Contacts#1",
                    "name": "A",
                    "company": "OpenAI & Meta",
                    "linkedin_url": "https://www.linkedin.com/in/a/",
                },
                {
                    "row_key": "Contacts#2",
                    "name": "B",
                    "company": "Anthropic",
                    "linkedin_url": "",
                },
            ],
            runtime_context={"harvest_prefetch_submit_workers": 3},
            target_candidates_enabled=True,
            export_enabled=True,
        )

        self.assertEqual(plan["total_rows"], 2)
        self.assertEqual(plan["company_group_count"], 3)
        self.assertEqual(plan["direct_linkedin_count"], 1)
        self.assertEqual(plan["search_required_count"], 1)
        self.assertEqual(plan["profile_fetch"]["batch_count"], 1)
        self.assertEqual(plan["profile_fetch"]["dispatch_granularity"], "profile_fetch_batch")
        self.assertTrue(plan["target_candidates_linkage"]["enabled"])
        self.assertEqual(plan["export_linkage"]["recommended_formats"], ["csv", "profile_bundle"])

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

    def test_local_candidate_inventory_prefers_hot_cache_materialized_snapshot_over_canonical_raw_snapshot(
        self,
    ) -> None:
        canonical_snapshot_dir = Path(self.tempdir.name) / "company_assets" / "reflectionai" / "20260418T120000"
        hot_cache_root = Path(self.tempdir.name) / "hot_cache_company_assets"
        hot_cache_snapshot_dir = hot_cache_root / "reflectionai" / "20260418T120000"
        canonical_snapshot_dir.mkdir(parents=True, exist_ok=True)
        (hot_cache_snapshot_dir / "normalized_artifacts").mkdir(parents=True, exist_ok=True)

        linkedin_url = "https://www.linkedin.com/in/reflection-hot-cache/"
        (canonical_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "canonical-1",
                            "name_en": "Casey Reflection",
                            "display_name": "Casey Reflection",
                            "target_company": "Reflection AI",
                            "organization": "Reflection AI",
                            "employment_status": "current",
                            "role": "Research Engineer",
                            "linkedin_url": linkedin_url,
                            "source_dataset": "canonical_snapshot",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (hot_cache_snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [
                        {
                            "candidate_id": "hot-cache-1",
                            "name_en": "Casey Reflection",
                            "display_name": "Casey Reflection",
                            "target_company": "Reflection AI",
                            "organization": "Reflection AI",
                            "employment_status": "current",
                            "role": "Research Engineer",
                            "linkedin_url": linkedin_url,
                            "education": "Stanford University",
                            "work_history": "Reflection AI",
                            "metadata": {"headline": "Research Engineer at Reflection AI"},
                            "source_dataset": "hot_cache_materialized",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with patch.dict(os.environ, {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root)}, clear=False):
            inventory = _build_local_candidate_inventory(Path(self.tempdir.name), self.store)

        candidate = inventory["linkedin_index"][linkedin_url.rstrip("/")]
        self.assertEqual(candidate.candidate_id, "hot-cache-1")
        self.assertIn("Stanford University", candidate.education)
        self.assertEqual(candidate.source_dataset, "hot_cache_materialized")

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
                [
                    "Alice Example",
                    "Reflection AI",
                    "Infra Engineer",
                    "https://www.linkedin.com/in/alice-example/?trk=public_profile",
                    "",
                ],
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

    def test_ingest_excel_contacts_can_attach_local_exact_hit_to_snapshot(self) -> None:
        self._seed_snapshot(company_key="reflectionai", canonical_name="Reflection AI", snapshot_id="snapshot-1")
        existing = Candidate(
            candidate_id="candidate-attach-1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Infra Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            metadata={"public_identifier": "alice-example"},
        )
        self.store.upsert_candidate(existing)

        workbook_path = Path(self.tempdir.name) / "attach.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title", "Linkedin"],
            rows=[
                ["Alice Example", "Reflection AI", "Infra Engineer", "https://www.linkedin.com/in/alice-example/"],
            ],
        )

        result = self.service.ingest_contacts(
            {
                "file_path": str(workbook_path),
                "target_company": "Reflection AI",
                "snapshot_id": "snapshot-1",
                "attach_to_snapshot": True,
                "build_artifacts": True,
            }
        )

        self.assertEqual(result["attachment_summary"]["status"], "completed")
        self.assertEqual(result["attachment_summary"]["snapshot_id"], "snapshot-1")
        snapshot_payload = json.loads(
            (
                Path(self.tempdir.name) / "company_assets" / "reflectionai" / "snapshot-1" / "candidate_documents.json"
            ).read_text()
        )
        self.assertEqual(len(snapshot_payload["candidates"]), 1)
        self.assertEqual(snapshot_payload["candidates"][0]["candidate_id"], "candidate-attach-1")
        registry_row = self.store.get_authoritative_organization_asset_registry(target_company="Reflection AI")
        self.assertEqual(registry_row["candidate_count"], 1)

    def test_continue_excel_intake_review_reuses_attachment_config_and_merges_selected_local_candidate(self) -> None:
        self._seed_snapshot(company_key="reflectionai", canonical_name="Reflection AI", snapshot_id="snapshot-2")
        selected_candidate = Candidate(
            candidate_id="candidate-review-1",
            name_en="Bobby Example",
            display_name="Bobby Example",
            category="employee",
            target_company="Reflection AI",
            organization="Reflection AI",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/bobby-example/",
        )
        self.store.upsert_candidate(selected_candidate)

        workbook_path = Path(self.tempdir.name) / "review-attach.xlsx"
        _write_inline_workbook(
            workbook_path,
            sheet_name="Contacts",
            headers=["Name", "Company", "Title"],
            rows=[
                ["Bob Example", "Reflection AI", "Infra Engineer"],
            ],
        )

        initial = self.service.ingest_contacts(
            {
                "file_path": str(workbook_path),
                "target_company": "Reflection AI",
                "snapshot_id": "snapshot-2",
                "attach_to_snapshot": True,
                "build_artifacts": True,
            }
        )
        self.assertEqual(initial["results"][0]["status"], "manual_review_local")
        self.assertEqual(initial["attachment_summary"]["status"], "skipped")

        continued = self.service.continue_review(
            {
                "intake_id": str(initial["intake_id"]),
                "decisions": [
                    {
                        "row_key": "Contacts#1",
                        "action": "select_local_candidate",
                        "selected_candidate_id": "candidate-review-1",
                    }
                ],
            }
        )

        self.assertEqual(continued["attachment_summary"]["status"], "completed")
        self.assertEqual(continued["attachment_summary"]["snapshot_id"], "snapshot-2")
        snapshot_payload = json.loads(
            (
                Path(self.tempdir.name) / "company_assets" / "reflectionai" / "snapshot-2" / "candidate_documents.json"
            ).read_text()
        )
        self.assertEqual(len(snapshot_payload["candidates"]), 1)
        self.assertEqual(snapshot_payload["candidates"][0]["candidate_id"], "candidate-review-1")

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

    def test_group_contacts_by_company_hints_splits_compound_company_cells(self) -> None:
        grouped = group_contacts_by_company_hints(
            [
                {
                    "row_key": "Contacts#1",
                    "name": "Barry Dong",
                    "company": "OpenAI & Meta",
                    "title": "Researcher",
                },
                {
                    "row_key": "Contacts#2",
                    "name": "Ada Import",
                    "company": "Anthropic",
                    "title": "Engineer",
                },
            ]
        )

        groups = {item["company"]: item for item in grouped["groups"]}
        self.assertEqual(set(groups.keys()), {"Anthropic", "Meta", "OpenAI"})
        self.assertEqual(groups["OpenAI"]["row_count"], 1)
        self.assertEqual(groups["Meta"]["row_count"], 1)
        self.assertEqual(groups["Anthropic"]["row_count"], 1)

    def test_exact_local_linkedin_match_respects_route_company(self) -> None:
        candidate = Candidate(
            candidate_id="barry-meta",
            name_en="Barry Dong",
            display_name="Barry Dong",
            category="employee",
            target_company="Meta",
            organization="Meta",
            employment_status="current",
            role="Researcher",
            linkedin_url="https://www.linkedin.com/in/barry-dong-525a10149/",
        )
        self.store.upsert_candidate(candidate)
        inventory = _build_local_candidate_inventory(Path(self.tempdir.name), self.store)

        self.assertIsNone(
            _find_exact_local_linkedin_match(
                {
                    "name": "Barry Dong",
                    "company": "OpenAI",
                    "linkedin_url": "https://www.linkedin.com/in/barry-dong-525a10149/",
                },
                inventory,
            )
        )
        self.assertIsNotNone(
            _find_exact_local_linkedin_match(
                {
                    "name": "Barry Dong",
                    "company": "Meta",
                    "linkedin_url": "https://www.linkedin.com/in/barry-dong-525a10149/",
                },
                inventory,
            )
        )

    def test_ingest_excel_contacts_routes_profile_to_former_company_when_current_company_differs(self) -> None:
        fetched_payload = {
            "https://www.linkedin.com/in/barry-dong-525a10149/": {
                "raw_path": str(Path(self.tempdir.name) / "harvest_profiles" / "barry-dong.json"),
                "parsed": {
                    "full_name": "Barry Dong",
                    "headline": "Researcher at Meta",
                    "profile_url": "https://www.linkedin.com/in/barry-dong-525a10149/",
                    "current_company": "Meta",
                    "summary": "Works on reasoning systems.",
                    "location": "San Francisco, California, United States",
                    "education": [{"school": "Fudan University", "degree": "BS", "field": "CS"}],
                    "experience": [
                        {"title": "Researcher", "companyName": "Meta"},
                        {"title": "Researcher", "companyName": "OpenAI"},
                    ],
                    "languages": ["English"],
                    "skills": ["Reasoning"],
                    "public_identifier": "barry-dong-525a10149",
                },
            }
        }
        with patch.object(self.service, "_fetch_profiles", return_value=(fetched_payload, [])):
            result = self.service.ingest_contacts(
                {
                    "target_company": "OpenAI",
                    "attach_to_snapshot": False,
                    "prepared_contact_batch": {
                        "workbook": {
                            "source_path": str(Path(self.tempdir.name) / "contacts.xlsx"),
                            "sheet_count": 1,
                            "sheet_names": ["Contacts"],
                            "detected_contact_row_count": 1,
                        },
                        "schema_inference": {},
                        "contacts": [
                            {
                                "row_key": "Contacts#1",
                                "sheet_name": "Contacts",
                                "row_index": 1,
                                "name": "Barry Dong",
                                "company": "OpenAI",
                                "uploaded_company": "OpenAI & Meta",
                                "title": "Researcher",
                                "linkedin_url": "https://www.linkedin.com/in/barry-dong-525a10149/",
                                "email": "",
                                "source_path": str(Path(self.tempdir.name) / "contacts.xlsx#Contacts:1"),
                                "raw_row": {},
                            }
                        ],
                    },
                }
            )

        self.assertEqual(result["summary"]["fetched_direct_linkedin_count"], 1)
        stored_candidates = self.store.list_candidates()
        self.assertEqual(len(stored_candidates), 1)
        self.assertEqual(stored_candidates[0].target_company, "OpenAI")
        self.assertEqual(stored_candidates[0].employment_status, "former")
        self.assertEqual(stored_candidates[0].category, "former_employee")
        self.assertEqual(stored_candidates[0].current_destination, "Meta")
        self.assertEqual(stored_candidates[0].metadata.get("excel_route_company_membership"), "former")
        self.assertEqual(stored_candidates[0].metadata.get("excel_uploaded_company"), "OpenAI & Meta")

    def test_job_request_defaults_disable_semantic_rerank(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的 Infra 成员",
                "target_company": "Reflection AI",
            }
        )
        self.assertEqual(request.semantic_rerank_limit, 0)
