import json
import tempfile
import unittest
from hashlib import sha1
from pathlib import Path
from unittest import mock

from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.company_asset_completion import CompanyAssetCompletionManager
from sourcing_agent.company_asset_supplement import CompanyAssetSupplementManager
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, EvidenceRecord
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.settings import load_settings
from sourcing_agent.storage import SQLiteStore


class CompanyAssetSupplementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "secrets").mkdir(parents=True, exist_ok=True)
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "linkedin_slug": "acme",
                        "linkedin_company_url": "https://www.linkedin.com/company/acme/",
                        "aliases": ["acme ai"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Acme",
                    "canonical_name": "Acme",
                    "company_key": "acme",
                    "linkedin_slug": "acme",
                    "linkedin_company_url": "https://www.linkedin.com/company/acme/",
                    "aliases": ["acme ai"],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False, indent=2))
        self.store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")
        self.settings = load_settings(self.project_root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_run_former_search_seed_persists_candidates_and_summary(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        search_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_summary_path.write_text(json.dumps({"ok": True}, ensure_ascii=False, indent=2))
        fake_snapshot = SearchSeedSnapshot(
            snapshot_id="20260406T120000",
            target_company="Acme",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[
                {
                    "full_name": "Former Example",
                    "headline": "Research Engineer at NewCo",
                    "location": "San Francisco",
                    "source_type": "harvest_profile_search",
                    "source_query": "__past_company_only__",
                    "profile_url": "https://www.linkedin.com/in/former-example/",
                    "slug": "former-example",
                    "employment_status": "former",
                    "target_company": "Acme",
                    "metadata": {"current_company": "NewCo"},
                }
            ],
            query_summaries=[{"query": "__past_company_only__", "mode": "harvest_profile_search", "seed_entry_count": 1}],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=search_summary_path,
        )

        with mock.patch("sourcing_agent.company_asset_supplement.SearchSeedAcquirer.discover", return_value=fake_snapshot):
            result = manager.run_former_search_seed(
                target_company="Acme",
                snapshot_id="20260406T120000",
                limit=80,
                pages=4,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["inserted_candidate_count"], 1)
        self.assertEqual(result["requested_limit"], 80)
        self.assertEqual(result["requested_pages"], 4)
        stored = self.store.list_candidates_for_company("Acme")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].display_name, "Former Example")
        self.assertTrue(Path(result["summary_path"]).exists())

    def test_run_former_search_seed_inherits_snapshot_locations(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (harvest_dir / "harvest_company_employees_raw.json").write_text(
            json.dumps(
                {
                    "segmented": True,
                    "shards": [
                        {"company_filters": {"locations": ["United States"], "function_ids": ["8"]}},
                        {"company_filters": {"locations": ["United States"], "exclude_function_ids": ["8"]}},
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        search_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_summary_path.write_text(json.dumps({"ok": True}, ensure_ascii=False, indent=2))
        fake_snapshot = SearchSeedSnapshot(
            snapshot_id="20260406T120000",
            target_company="Acme",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=search_summary_path,
        )
        captured: dict[str, object] = {}

        def _capture_discover(*args, **kwargs):
            captured["filter_hints"] = dict(kwargs.get("filter_hints") or {})
            return fake_snapshot

        with mock.patch("sourcing_agent.company_asset_supplement.SearchSeedAcquirer.discover", side_effect=_capture_discover):
            result = manager.run_former_search_seed(
                target_company="Acme",
                snapshot_id="20260406T120000",
                limit=80,
                pages=4,
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            captured["filter_hints"],
            {
                "past_companies": ["https://www.linkedin.com/company/acme/"],
                "locations": ["United States"],
            },
        )

    def test_run_former_search_seed_expands_to_probe_total_before_discovery(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        search_summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        search_summary_path.parent.mkdir(parents=True, exist_ok=True)
        search_summary_path.write_text(json.dumps({"ok": True}, ensure_ascii=False, indent=2))
        fake_snapshot = SearchSeedSnapshot(
            snapshot_id="20260406T120000",
            target_company="Acme",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[],
            query_summaries=[
                {
                    "query": "__past_company_only__",
                    "mode": "harvest_profile_search",
                    "requested_limit": 500,
                    "requested_pages": 20,
                    "effective_limit": 559,
                    "effective_pages": 23,
                    "seed_entry_count": 559,
                    "probe": {
                        "probe_performed": True,
                        "probe_query_text": "",
                        "probe_limit": 25,
                        "probe_pages": 1,
                        "requested_limit": 500,
                        "requested_pages": 20,
                        "effective_limit": 559,
                        "effective_pages": 23,
                        "provider_total_count": 559,
                        "provider_total_pages": 23,
                        "probe_returned_count": 25,
                        "probe_raw_path": "/tmp/probe.json",
                    },
                }
            ],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=search_summary_path,
        )
        captured: dict[str, object] = {}

        def _capture_discover(*args, **kwargs):
            captured["cost_policy"] = dict(kwargs.get("cost_policy") or {})
            return fake_snapshot

        with mock.patch("sourcing_agent.company_asset_supplement.SearchSeedAcquirer.discover", side_effect=_capture_discover):
            result = manager.run_former_search_seed(
                target_company="Acme",
                snapshot_id="20260406T120000",
                limit=500,
                pages=20,
            )

        self.assertEqual(result["requested_limit"], 500)
        self.assertEqual(result["requested_pages"], 20)
        self.assertEqual(result["effective_limit"], 559)
        self.assertEqual(result["effective_pages"], 23)
        self.assertEqual(result["former_search_probe"]["provider_total_count"], 559)
        self.assertEqual(captured["cost_policy"]["provider_people_search_min_expected_results"], 500)
        self.assertEqual(captured["cost_policy"]["provider_people_search_pages"], 20)

    def test_rebuild_linkedin_stage_1_snapshot_merges_roster_and_search_seed(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (harvest_dir / "harvest_company_employees_summary.json").write_text(
            json.dumps(
                {
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "linkedin_slug": "acme",
                        "linkedin_company_url": "https://www.linkedin.com/company/acme/",
                    },
                    "page_summaries": [],
                    "accounts_used": ["harvest_company_employees"],
                    "errors": [],
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (harvest_dir / "harvest_company_employees_merged.json").write_text(
            json.dumps(
                [
                    {
                        "full_name": "Alice Current",
                        "member_key": "alice-current",
                        "headline": "Infra Engineer at Acme",
                        "location": "San Francisco",
                        "linkedin_url": "https://www.linkedin.com/in/alice-current/",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        (harvest_dir / "harvest_company_employees_visible.json").write_text(
            json.dumps(
                [
                    {
                        "full_name": "Alice Current",
                        "member_key": "alice-current",
                        "headline": "Infra Engineer at Acme",
                        "location": "San Francisco",
                        "linkedin_url": "https://www.linkedin.com/in/alice-current/",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        (harvest_dir / "harvest_company_employees_headless.json").write_text(
            json.dumps([], ensure_ascii=False, indent=2)
        )
        search_dir = snapshot_dir / "search_seed_discovery"
        search_dir.mkdir(parents=True, exist_ok=True)
        (search_dir / "summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "linkedin_slug": "acme",
                        "linkedin_company_url": "https://www.linkedin.com/company/acme/",
                    },
                    "query_summaries": [{"query": "__past_company_only__", "mode": "harvest_profile_search"}],
                    "accounts_used": ["harvest_profile_search"],
                    "errors": [],
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (search_dir / "entries.json").write_text(
            json.dumps(
                [
                    {
                        "full_name": "Bob Former",
                        "headline": "Former Staff Engineer at Acme",
                        "profile_url": "https://www.linkedin.com/in/bob-former/",
                        "employment_status": "former",
                        "source_type": "harvest_profile_search",
                        "source_query": "__past_company_only__",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            )
        )

        result = manager.supplement_snapshot(
            target_company="Acme",
            snapshot_id="20260406T120000",
            rebuild_linkedin_stage_1=True,
            build_artifacts=False,
        )

        self.assertEqual(result["status"], "completed")
        rebuild = dict(result["linkedin_stage_1_rebuild"] or {})
        self.assertEqual(rebuild["status"], "completed")
        self.assertEqual(rebuild["roster_candidate_count"], 1)
        self.assertEqual(rebuild["search_seed_candidate_count"], 1)
        self.assertEqual(rebuild["candidate_count"], 2)
        payload = json.loads((snapshot_dir / "candidate_documents.linkedin_stage_1.json").read_text())
        self.assertEqual(int(payload["candidate_count"]), 2)
        self.assertEqual(
            set((payload.get("acquisition_sources") or {}).keys()),
            {"roster_snapshot", "search_seed_snapshot"},
        )
        stored = self.store.list_candidates_for_company("Acme")
        self.assertEqual(len(stored), 2)

    def test_repair_current_roster_profile_refs_restores_visible_roster_url(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        visible_path.write_text(
            json.dumps(
                [
                    {
                        "full_name": "Alice Example",
                        "member_key": "opaque-alice",
                        "linkedin_url": "https://www.linkedin.com/in/opaque-alice",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            )
        )
        candidate_id = sha1("acme|aliceexample|opaque-alice".encode("utf-8")).hexdigest()[:16]
        self.store.upsert_candidate(
            Candidate(
                candidate_id=candidate_id,
                name_en="Alice Example",
                display_name="Alice Example",
                category="employee",
                target_company="Acme",
                organization="Acme",
                employment_status="current",
                linkedin_url="https://www.linkedin.com/in/alice-example",
                source_dataset="acme_linkedin_company_people",
                metadata={
                    "profile_url": "https://www.linkedin.com/in/alice-example",
                    "more_profiles": ["https://www.linkedin.com/in/legacy-alice"],
                },
            )
        )

        result = manager.repair_current_roster_profile_refs(target_company="Acme", snapshot_id="20260406T120000")

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["repaired_count"], 1)
        updated = self.store.get_candidate(candidate_id)
        assert updated is not None
        self.assertEqual(updated.linkedin_url, "https://www.linkedin.com/in/opaque-alice")
        self.assertEqual(updated.metadata.get("profile_url"), "https://www.linkedin.com/in/opaque-alice")
        self.assertIn("https://www.linkedin.com/in/alice-example", updated.metadata.get("more_profiles") or [])

    def test_merge_candidates_into_snapshot_retargets_cross_company_candidate_and_refreshes_registry(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        cross_company_candidate = Candidate(
            candidate_id="candidate-cross-company",
            name_en="Alex Transfer",
            display_name="Alex Transfer",
            category="employee",
            target_company="OtherCo",
            organization="OtherCo",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/alex-transfer/",
        )
        cross_company_evidence = EvidenceRecord(
            evidence_id="evidence-cross-company",
            candidate_id="candidate-cross-company",
            source_type="excel_upload_row",
            title="Excel upload row",
            url="https://www.linkedin.com/in/alex-transfer/",
            summary="Imported from Excel supplement.",
            source_dataset="excel_upload_row",
            source_path="/tmp/alex-transfer.xlsx",
            metadata={"row_key": "Contacts#1"},
        )

        result = manager.merge_candidates_into_snapshot(
            target_company="Acme",
            snapshot_id="20260406T120000",
            candidates=[cross_company_candidate],
            evidence=[cross_company_evidence],
            source_kind="excel_intake",
            source_summary={"intake_id": "excel-1"},
            build_artifacts=True,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["projection_summary"]["retargeted_candidate_count"], 1)
        snapshot_payload = json.loads(
            (self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").read_text()
        )
        self.assertEqual(len(snapshot_payload["candidates"]), 1)
        merged_candidate = snapshot_payload["candidates"][0]
        self.assertEqual(merged_candidate["target_company"], "Acme")
        self.assertEqual(merged_candidate["metadata"]["supplement_source_candidate_id"], "candidate-cross-company")
        registry_row = self.store.get_authoritative_organization_asset_registry(target_company="Acme")
        self.assertEqual(registry_row["candidate_count"], 1)
        self.assertEqual(registry_row["snapshot_id"], "20260406T120000")

    def test_sync_project_local_anthropic_package_copies_selected_assets_into_repo_package(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        external_root = self.project_root / "external_anthropic"
        (external_root / "data").mkdir(parents=True, exist_ok=True)
        workbook_path = external_root / "anthropic_v1.xlsx"
        workbook_path.write_text("workbook", encoding="utf-8")
        readme_path = external_root / "README.md"
        readme_path.write_text("readme", encoding="utf-8")
        progress_path = external_root / "PROGRESS.md"
        progress_path.write_text("progress", encoding="utf-8")
        api_accounts_path = external_root / "api_accounts.json"
        api_accounts_path.write_text("{}", encoding="utf-8")
        company_ids_path = external_root / "company_ids.json"
        company_ids_path.write_text("{}", encoding="utf-8")
        publications_path = external_root / "data" / "publications_unified.json"
        publications_path.write_text("[]", encoding="utf-8")
        scholar_path = external_root / "data" / "scholar_scan_results.json"
        scholar_path.write_text("{}", encoding="utf-8")
        investor_path = external_root / "investor_chinese_members_final.json"
        investor_path.write_text("{}", encoding="utf-8")

        catalog = AssetCatalog(
            project_root=self.project_root,
            dev_root=self.project_root.parent,
            anthropic_root=external_root,
            anthropic_workbook=workbook_path,
            anthropic_readme=readme_path,
            anthropic_progress=progress_path,
            legacy_api_accounts=api_accounts_path,
            legacy_company_ids=company_ids_path,
            anthropic_publications=publications_path,
            scholar_scan_results=scholar_path,
            investor_members_json=investor_path,
            employee_scan_skill=self.project_root / "employee_skill.md",
            investor_scan_skill=self.project_root / "investor_skill.md",
            onepager_skill=self.project_root / "onepager_skill.md",
            anthropic_asset_source="external_legacy",
            anthropic_project_root=None,
            anthropic_external_root=external_root,
        )

        with mock.patch("sourcing_agent.company_asset_supplement.AssetCatalog.discover", return_value=catalog):
            result = manager.sync_project_local_anthropic_package()

        self.assertEqual(result["status"], "completed")
        project_package_root = self.project_root / "local_asset_packages" / "anthropic"
        self.assertTrue((project_package_root / "anthropic_v1.xlsx").exists())
        self.assertTrue((project_package_root / "data" / "publications_unified.json").exists())
        self.assertTrue((project_package_root / "package_manifest.json").exists())

    def test_repair_current_roster_registry_aliases_reuses_historical_harvest_profiles(self) -> None:
        manager = CompanyAssetSupplementManager(
            runtime_dir=self.runtime_dir,
            store=self.store,
            settings=self.settings,
            asset_completion_manager=CompanyAssetCompletionManager(
                runtime_dir=self.runtime_dir,
                store=self.store,
                settings=self.settings,
            ),
        )
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        harvest_dir = snapshot_dir / "harvest_company_employees"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        visible_path = harvest_dir / "harvest_company_employees_visible.json"
        raw_member_url = "https://www.linkedin.com/in/ACwAAATEST123"
        vanity_url = "https://www.linkedin.com/in/alice-example"
        visible_path.write_text(
            json.dumps(
                [
                    {
                        "full_name": "Alice Example",
                        "member_key": "opaque-alice",
                        "linkedin_url": raw_member_url,
                        "source_shard_id": "kw_infra",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            )
        )

        historical_snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260405T120000"
        historical_harvest_dir = historical_snapshot_dir / "harvest_profiles"
        historical_harvest_dir.mkdir(parents=True, exist_ok=True)
        raw_profile_path = historical_harvest_dir / "historical-alice.json"
        raw_profile_path.write_text(
            json.dumps(
                {
                    "_harvest_request": {
                        "profile_url": raw_member_url,
                    },
                    "item": {
                        "linkedinUrl": vanity_url,
                        "publicIdentifier": "alice-example",
                        "headline": "Infra Engineer at Acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        result = manager.repair_current_roster_registry_aliases(
            target_company="Acme",
            snapshot_id="20260406T120000",
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["repaired_count"], 1)
        registry_by_raw = self.store.get_linkedin_profile_registry(raw_member_url)
        assert registry_by_raw is not None
        self.assertEqual(registry_by_raw["status"], "fetched")
        self.assertEqual(registry_by_raw["profile_url"], vanity_url)
        self.assertEqual(registry_by_raw["raw_linkedin_url"], raw_member_url)
        self.assertEqual(registry_by_raw["sanity_linkedin_url"], vanity_url)
        self.assertEqual(Path(registry_by_raw["last_raw_path"]).resolve(), raw_profile_path.resolve())
        self.assertIn(raw_member_url, registry_by_raw.get("alias_urls") or [])
        self.assertIn(vanity_url, registry_by_raw.get("alias_urls") or [])
        registry_by_vanity = self.store.get_linkedin_profile_registry(vanity_url)
        assert registry_by_vanity is not None
        self.assertEqual(registry_by_vanity["profile_url"], vanity_url)
        self.assertTrue(Path(result["summary_path"]).exists())


if __name__ == "__main__":
    unittest.main()
