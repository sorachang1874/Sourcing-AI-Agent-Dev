import json
import os
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.asset_sync import AssetBundleError, AssetBundleManager
from sourcing_agent.cloud_asset_import import hydrate_cloud_generation, import_cloud_assets
from sourcing_agent.domain import Candidate, make_evidence_id
from sourcing_agent.object_storage import ObjectStorageConfig, build_object_storage_client
from sourcing_agent.request_matching import matching_request_family_signature, matching_request_signature
from sourcing_agent.storage import SQLiteStore


class CloudAssetImportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.source_project = self.root / "source_project"
        self.source_runtime = self.source_project / "runtime"
        self.source_runtime.mkdir(parents=True, exist_ok=True)
        self.target_project = self.root / "target_project"
        self.target_runtime = self.target_project / "runtime"
        self.target_runtime.mkdir(parents=True, exist_ok=True)
        self.source_manager = AssetBundleManager(self.source_project, self.source_runtime)
        self.target_manager = AssetBundleManager(self.target_project, self.target_runtime)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_import_company_snapshot_repairs_artifacts_and_registries(self) -> None:
        company_dir = self.source_runtime / "company_assets" / "acme"
        snapshot_dir = company_dir / "20260413T120000"
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260413T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "aliases": ["acme ai"],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="legacy_current_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            education="Stanford",
            work_history="Acme | ExampleCo",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "legacy_current_1",
                "legacy_snapshot",
                "Legacy root snapshot",
                "https://www.linkedin.com/in/alice-example/",
            ),
            "candidate_id": "legacy_current_1",
            "source_type": "linkedin_profile_detail",
            "title": "Legacy root snapshot",
            "url": "https://www.linkedin.com/in/alice-example/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/alice-example/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260413T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (harvest_dir / "alice.json").write_text(
            json.dumps(
                {
                    "_harvest_request": {
                        "kind": "url",
                        "value": "https://www.linkedin.com/in/alice-example/",
                        "profile_url": "https://www.linkedin.com/in/alice-example/",
                    },
                    "item": {
                        "fullName": "Alice Example",
                        "profileUrl": "https://www.linkedin.com/in/alice-example/",
                        "headline": "Infrastructure Engineer at Acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        export = self.source_manager.export_company_snapshot_bundle("Acme")
        result = import_cloud_assets(
            bundle_manager=self.target_manager,
            manifest_path=export["manifest_path"],
            conflict="error",
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["bundle_kind"], "company_snapshot")
        self.assertEqual(result["scoped_companies"], ["acme"])
        self.assertEqual(result["scoped_snapshot_id"], "20260413T120000")
        self.assertTrue(str(result.get("sync_run_id") or ""))
        self.assertEqual(str(dict(result.get("ledger") or {}).get("operation_type") or ""), "import_bundle")
        self.assertEqual(result["artifact_repair"]["status"], "completed")
        self.assertEqual(result["artifact_repair"]["repaired_snapshot_count"], 1)
        self.assertEqual(result["organization_warmup"]["status"], "completed")
        self.assertEqual(result["profile_registry_backfill"]["status"], "completed")
        self.assertEqual(result["organization_warmup"]["results"][0]["backfill"]["status"], "backfilled")

        normalized_dir = self.target_runtime / "company_assets" / "acme" / "20260413T120000" / "normalized_artifacts"
        self.assertTrue((normalized_dir / "manifest.json").exists())
        self.assertTrue((normalized_dir / "artifact_summary.json").exists())
        ledger_path = Path(result["artifact_repair"]["companies"][0]["repaired_snapshots"][0]["ledger_path"])
        self.assertTrue(ledger_path.exists())
        self.assertTrue((self.target_runtime / "company_identity_registry.json").exists())

        store = SQLiteStore(self.target_runtime / "sourcing_agent.db")
        authoritative = store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        self.assertEqual(str(authoritative.get("snapshot_id") or ""), "20260413T120000")
        self.assertTrue(str(authoritative.get("materialization_generation_key") or ""))
        execution_profile = store.get_organization_execution_profile(
            target_company="Acme", asset_view="canonical_merged"
        )
        self.assertTrue(bool(execution_profile))
        self.assertTrue(str(execution_profile.get("source_generation_key") or ""))
        profile_registry_entry = store.get_linkedin_profile_registry("https://www.linkedin.com/in/alice-example/")
        self.assertIsNotNone(profile_registry_entry)
        assert profile_registry_entry is not None
        self.assertEqual(profile_registry_entry["status"], "fetched")

    def test_import_company_snapshot_can_background_followup_refresh(self) -> None:
        company_dir = self.source_runtime / "company_assets" / "acme"
        snapshot_dir = company_dir / "20260413T120000"
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260413T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate = Candidate(
            candidate_id="legacy_current_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            source_dataset="legacy_snapshot",
        )
        evidence = {
            "evidence_id": make_evidence_id(
                "legacy_current_1",
                "legacy_snapshot",
                "Legacy root snapshot",
                "https://www.linkedin.com/in/alice-example/",
            ),
            "candidate_id": "legacy_current_1",
            "source_type": "linkedin_profile_detail",
            "title": "Legacy root snapshot",
            "url": "https://www.linkedin.com/in/alice-example/",
            "summary": "Recovered from a legacy root candidate snapshot.",
            "source_dataset": "legacy_snapshot",
            "source_path": str(snapshot_dir / "candidate_documents.json"),
            "metadata": {"profile_url": "https://www.linkedin.com/in/alice-example/"},
        }
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": "20260413T120000",
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        },
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [evidence],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (harvest_dir / "alice.json").write_text(
            json.dumps(
                {
                    "_harvest_request": {
                        "kind": "url",
                        "value": "https://www.linkedin.com/in/alice-example/",
                        "profile_url": "https://www.linkedin.com/in/alice-example/",
                    },
                    "item": {
                        "fullName": "Alice Example",
                        "profileUrl": "https://www.linkedin.com/in/alice-example/",
                        "headline": "Infrastructure Engineer at Acme",
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        export = self.source_manager.export_company_snapshot_bundle("Acme")
        with unittest.mock.patch.dict(os.environ, {"SOURCING_IMPORT_POST_REFRESH_MODE": "background"}, clear=False):
            result = import_cloud_assets(
                bundle_manager=self.target_manager,
                manifest_path=export["manifest_path"],
                conflict="error",
            )

        self.assertEqual(result["artifact_repair"]["status"], "completed")
        self.assertEqual(result["organization_warmup"]["status"], "scheduled")
        self.assertEqual(result["profile_registry_backfill"]["status"], "scheduled")
        state_path = Path(str(result["organization_warmup"]["state_path"] or ""))
        self.assertTrue(state_path.exists())

        state_payload: dict[str, object] = {}
        for _ in range(80):
            state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            if str(state_payload.get("status") or "") in {"completed", "failed"}:
                break
            time.sleep(0.05)
        self.assertEqual(str(state_payload.get("status") or ""), "completed")
        self.assertEqual(str(dict(state_payload.get("organization_warmup") or {}).get("status") or ""), "completed")
        self.assertEqual(
            str(dict(state_payload.get("profile_registry_backfill") or {}).get("status") or ""),
            "completed",
        )

        target_store = SQLiteStore(self.target_runtime / "sourcing_agent.db")
        profile_registry_entry = target_store.get_linkedin_profile_registry("https://www.linkedin.com/in/alice-example/")
        self.assertIsNotNone(profile_registry_entry)

    def test_import_sqlite_snapshot_refreshes_legacy_matching_metadata(self) -> None:
        source_store = SQLiteStore(self.source_runtime / "sourcing_agent.db")
        legacy_request = {
            "raw_user_request": "我想找 Google Gemini 的产品经理",
            "target_company": "Google",
        }
        source_store._connection.execute(
            """
            INSERT INTO jobs (
                job_id, status, stage, request_json, execution_bundle_json, matching_request_json,
                request_signature, request_family_signature, matching_request_signature, matching_request_family_signature
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-job",
                "completed",
                "completed",
                json.dumps(legacy_request, ensure_ascii=False),
                "{}",
                "{}",
                "",
                "",
                "",
                "",
            ),
        )
        source_store._connection.execute(
            """
            INSERT INTO criteria_feedback (
                job_id, candidate_id, target_company, feedback_type, subject, value, reviewer, notes, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-job",
                "cand-1",
                "Google",
                "must_have_signal",
                "gemini",
                "product manager",
                "human",
                "",
                json.dumps({"request_payload": legacy_request}, ensure_ascii=False),
            ),
        )
        source_store._connection.commit()

        export = self.source_manager.export_sqlite_snapshot()
        result = import_cloud_assets(
            bundle_manager=self.target_manager,
            manifest_path=export["manifest_path"],
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["bundle_kind"], "control_plane_snapshot")
        self.assertEqual(result["matching_metadata_refresh"]["status"], "completed")
        self.assertEqual(str(dict(result.get("ledger") or {}).get("operation_type") or ""), "import_bundle")
        self.assertGreaterEqual(result["matching_metadata_refresh"]["job_request_signature_count"], 1)
        self.assertGreaterEqual(result["matching_metadata_refresh"]["job_count"], 1)
        self.assertGreaterEqual(result["matching_metadata_refresh"]["feedback_count"], 1)

        target_store = SQLiteStore(self.target_runtime / "sourcing_agent.db")
        restored_job = target_store.get_job("legacy-job")
        self.assertIsNotNone(restored_job)
        assert restored_job is not None
        self.assertEqual(
            restored_job["matching_request_signature"],
            matching_request_signature(legacy_request),
        )
        self.assertEqual(
            restored_job["matching_request_family_signature"],
            matching_request_family_signature(legacy_request),
        )
        feedback_rows = target_store.list_criteria_feedback(target_company="Google", limit=5)
        self.assertEqual(
            str(feedback_rows[0]["metadata"].get("matching_request_signature") or ""),
            matching_request_signature(legacy_request),
        )
        self.assertEqual(
            str(feedback_rows[0]["metadata"].get("matching_request_family_signature") or ""),
            matching_request_family_signature(legacy_request),
        )

    def test_import_sqlite_snapshot_requires_explicit_restore_opt_in(self) -> None:
        sqlite_path = self.source_runtime / "sourcing_agent.db"
        sqlite_path.write_bytes(b"not-a-real-db")
        export = self.source_manager.export_sqlite_snapshot()

        with self.assertRaisesRegex(AssetBundleError, "sqlite snapshot restore is disabled by default"):
            import_cloud_assets(
                bundle_manager=self.target_manager,
                manifest_path=export["manifest_path"],
            )

    def test_import_cloud_assets_can_disable_legacy_bundle_fallback(self) -> None:
        source_store = SQLiteStore(self.source_runtime / "sourcing_agent.db")
        source_store.save_job(
            "job-1",
            "workflow",
            "completed",
            "completed",
            {"target_company": "Acme"},
        )
        export = self.source_manager.export_sqlite_snapshot()

        with self.assertRaisesRegex(AssetBundleError, "legacy bundle fallback is disabled"):
            import_cloud_assets(
                bundle_manager=self.target_manager,
                manifest_path=export["manifest_path"],
                allow_legacy_bundle_fallback=False,
            )

    def test_hydrate_cloud_generation_records_operation_ledger(self) -> None:
        canonical_root = self.root / "canonical_company_assets"
        hot_cache_root = self.root / "hot_cache_company_assets"
        snapshot_id = "20260418T120000"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        artifact_dir = snapshot_dir / "normalized_artifacts"
        (artifact_dir / "candidate_shards").mkdir(parents=True, exist_ok=True)
        (artifact_dir / "pages").mkdir(parents=True, exist_ok=True)
        (artifact_dir / "backlogs").mkdir(parents=True, exist_ok=True)
        company_identity = {
            "requested_name": "Acme",
            "canonical_name": "Acme",
            "company_key": "acme",
            "linkedin_slug": "acme",
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": company_identity}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (artifact_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "candidate_shards": [{"candidate_id": "c1", "path": "candidate_shards/c1.json"}],
                    "pages": [{"page": 1, "path": "pages/page-0001.json"}],
                    "backlogs": {
                        "manual_review": "backlogs/manual_review.json",
                        "profile_completion": "backlogs/profile_completion.json",
                    },
                    "pagination": {"page_size": 50, "page_count": 1},
                    "materialization_generation_key": "gen-acme-2",
                    "materialization_generation_sequence": 2,
                    "materialization_watermark": "2:gen-acme-2",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "materialization_generation_key": "gen-acme-2",
                    "materialization_generation_sequence": 2,
                    "materialization_watermark": "2:gen-acme-2",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "snapshot_manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "company_identity": company_identity,
                    "materialization_generation_key": "gen-acme-2",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "candidate_shards" / "c1.json").write_text(
            json.dumps({"candidate_id": "c1", "display_name": "Alice"}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "pages" / "page-0001.json").write_text(
            json.dumps({"page": 1, "candidates": [{"candidate_id": "c1"}]}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "backlogs" / "manual_review.json").write_text(
            json.dumps({"items": []}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "backlogs" / "profile_completion.json").write_text(
            json.dumps({"items": []}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "publishable_primary_emails.json").write_text(
            json.dumps({"by_candidate_id": {}, "by_profile_url_key": {}}, ensure_ascii=False, indent=2)
        )
        client = build_object_storage_client(
            ObjectStorageConfig(
                provider="filesystem", local_dir=str(self.root / "object_store"), prefix="cloud-import-tests"
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            published = self.source_manager.publish_candidate_generation(target_company="Acme", client=client)
            hydrated = hydrate_cloud_generation(
                bundle_manager=self.target_manager,
                storage_client=client,
                target_company="Acme",
                snapshot_id=snapshot_id,
                asset_view="canonical_merged",
                generation_key=str(published.get("generation_key") or ""),
            )

        self.assertEqual(hydrated["status"], "hydrated")
        self.assertEqual(str(dict(hydrated.get("ledger") or {}).get("operation_type") or ""), "hydrate_generation")
        self.assertTrue(
            (hot_cache_root / "acme" / snapshot_id / "normalized_artifacts" / "candidate_shards" / "c1.json").exists()
        )

    def test_import_cloud_assets_can_hydrate_generation_first_from_generation_index(self) -> None:
        canonical_root = self.root / "canonical_company_assets"
        hot_cache_root = self.root / "hot_cache_company_assets"
        snapshot_id = "20260418T120000"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        artifact_dir = snapshot_dir / "normalized_artifacts"
        (artifact_dir / "candidate_shards").mkdir(parents=True, exist_ok=True)
        (artifact_dir / "pages").mkdir(parents=True, exist_ok=True)
        (artifact_dir / "backlogs").mkdir(parents=True, exist_ok=True)
        company_identity = {
            "requested_name": "Acme",
            "canonical_name": "Acme",
            "company_key": "acme",
            "linkedin_slug": "acme",
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": company_identity}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (artifact_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "candidate_shards": [{"candidate_id": "c1", "path": "candidate_shards/c1.json"}],
                    "pages": [{"page": 1, "path": "pages/page-0001.json"}],
                    "backlogs": {
                        "manual_review": "backlogs/manual_review.json",
                        "profile_completion": "backlogs/profile_completion.json",
                    },
                    "pagination": {"page_size": 50, "page_count": 1},
                    "materialization_generation_key": "gen-acme-import-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-import-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "materialization_generation_key": "gen-acme-import-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-import-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "snapshot_manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "company_identity": company_identity,
                    "materialization_generation_key": "gen-acme-import-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "candidate_shards" / "c1.json").write_text(
            json.dumps({"candidate_id": "c1", "display_name": "Alice"}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "pages" / "page-0001.json").write_text(
            json.dumps({"page": 1, "candidates": [{"candidate_id": "c1"}]}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "backlogs" / "manual_review.json").write_text(
            json.dumps({"items": []}, ensure_ascii=False, indent=2)
        )
        (artifact_dir / "backlogs" / "profile_completion.json").write_text(
            json.dumps({"items": []}, ensure_ascii=False, indent=2)
        )
        client = build_object_storage_client(
            ObjectStorageConfig(
                provider="filesystem", local_dir=str(self.root / "object_store"), prefix="cloud-import-tests"
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            published = self.source_manager.publish_candidate_generation(target_company="Acme", client=client)
            imported = import_cloud_assets(
                bundle_manager=self.target_manager,
                storage_client=client,
                generation_key=str(published.get("generation_key") or ""),
                companies=["Acme"],
                snapshot_id=snapshot_id,
            )

        self.assertEqual(imported["status"], "completed")
        self.assertEqual(imported["bundle_kind"], "candidate_generation")
        self.assertEqual(str(dict(imported.get("ledger") or {}).get("operation_type") or ""), "import_generation")
        self.assertEqual(imported["organization_warmup"]["status"], "completed")
        self.assertEqual(imported["artifact_repair"]["status"], "completed")
        self.assertTrue(
            (hot_cache_root / "acme" / snapshot_id / "normalized_artifacts" / "candidate_shards" / "c1.json").exists()
        )
        target_store = SQLiteStore(self.target_runtime / "sourcing_agent.db")
        authoritative = target_store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        self.assertEqual(str(authoritative.get("snapshot_id") or ""), snapshot_id)
        self.assertEqual(
            str(authoritative.get("materialization_generation_key") or ""),
            str(published.get("generation_key") or ""),
        )

    def test_import_company_snapshot_manifest_prefers_generation_first_when_available(self) -> None:
        canonical_root = self.root / "canonical_company_assets"
        hot_cache_root = self.root / "hot_cache_company_assets"
        snapshot_id = "20260418T120000"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        artifact_dir = snapshot_dir / "normalized_artifacts"
        (artifact_dir / "candidate_shards").mkdir(parents=True, exist_ok=True)
        company_identity = {
            "requested_name": "Acme",
            "canonical_name": "Acme",
            "company_key": "acme",
            "linkedin_slug": "acme",
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": company_identity}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (artifact_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "candidate_shards": [{"candidate_id": "c1", "path": "candidate_shards/c1.json"}],
                    "pages": [],
                    "backlogs": {},
                    "materialization_generation_key": "gen-acme-bundle-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-bundle-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "materialization_generation_key": "gen-acme-bundle-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-bundle-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "snapshot_manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "company_identity": company_identity,
                    "materialization_generation_key": "gen-acme-bundle-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "candidate_shards" / "c1.json").write_text(
            json.dumps({"candidate_id": "c1", "display_name": "Alice"}, ensure_ascii=False, indent=2)
        )
        client = build_object_storage_client(
            ObjectStorageConfig(
                provider="filesystem", local_dir=str(self.root / "object_store"), prefix="cloud-import-tests"
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            published = self.source_manager.publish_candidate_generation(target_company="Acme", client=client)
            export = self.source_manager.export_company_snapshot_bundle("Acme")
            imported = import_cloud_assets(
                bundle_manager=self.target_manager,
                manifest_path=export["manifest_path"],
                storage_client=client,
                companies=["Acme"],
                snapshot_id=snapshot_id,
            )

        self.assertEqual(imported["status"], "completed")
        self.assertEqual(imported["bundle_kind"], "candidate_generation")
        self.assertEqual(imported["import_mode"], "candidate_generation")
        self.assertFalse(bool(imported["legacy_bundle_fallback_used"]))
        self.assertEqual(str(imported["requested_bundle_kind"] or ""), "company_snapshot")
        self.assertEqual(
            str(dict(imported.get("generation_first_attempt") or {}).get("generation_key") or ""),
            str(published.get("generation_key") or ""),
        )

    def test_import_company_handoff_manifest_prefers_generation_first_when_available(self) -> None:
        canonical_root = self.root / "canonical_company_assets"
        hot_cache_root = self.root / "hot_cache_company_assets"
        snapshot_id = "20260418T120000"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        artifact_dir = snapshot_dir / "normalized_artifacts"
        (artifact_dir / "candidate_shards").mkdir(parents=True, exist_ok=True)
        company_identity = {
            "requested_name": "Acme",
            "canonical_name": "Acme",
            "company_key": "acme",
            "linkedin_slug": "acme",
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": company_identity}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (artifact_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "candidate_shards": [{"candidate_id": "c1", "path": "candidate_shards/c1.json"}],
                    "pages": [],
                    "backlogs": {},
                    "materialization_generation_key": "gen-acme-handoff-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-handoff-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "materialization_generation_key": "gen-acme-handoff-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-handoff-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "candidate_shards" / "c1.json").write_text(
            json.dumps({"candidate_id": "c1", "display_name": "Alice"}, ensure_ascii=False, indent=2)
        )
        client = build_object_storage_client(
            ObjectStorageConfig(
                provider="filesystem", local_dir=str(self.root / "object_store"), prefix="cloud-import-tests"
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            published = self.source_manager.publish_candidate_generation(target_company="Acme", client=client)
            export = self.source_manager.export_company_handoff_bundle("Acme", include_sqlite=False)
            imported = import_cloud_assets(
                bundle_manager=self.target_manager,
                manifest_path=export["manifest_path"],
                storage_client=client,
                companies=["Acme"],
                snapshot_id=snapshot_id,
            )

        self.assertEqual(imported["status"], "completed")
        self.assertEqual(imported["bundle_kind"], "candidate_generation")
        self.assertEqual(imported["import_mode"], "candidate_generation")
        self.assertFalse(bool(imported["legacy_bundle_fallback_used"]))
        self.assertEqual(str(imported["requested_bundle_kind"] or ""), "company_handoff")
        self.assertEqual(
            str(dict(imported.get("generation_first_attempt") or {}).get("generation_key") or ""),
            str(published.get("generation_key") or ""),
        )

    def test_import_sqlite_snapshot_direct_request_can_resolve_generation_first(self) -> None:
        canonical_root = self.root / "canonical_company_assets"
        hot_cache_root = self.root / "hot_cache_company_assets"
        snapshot_id = "20260418T120000"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        artifact_dir = snapshot_dir / "normalized_artifacts"
        (artifact_dir / "candidate_shards").mkdir(parents=True, exist_ok=True)
        company_identity = {
            "requested_name": "Acme",
            "canonical_name": "Acme",
            "company_key": "acme",
            "linkedin_slug": "acme",
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": company_identity}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (artifact_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "candidate_shards": [{"candidate_id": "c1", "path": "candidate_shards/c1.json"}],
                    "pages": [],
                    "backlogs": {},
                    "materialization_generation_key": "gen-acme-sqlite-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-sqlite-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "artifact_summary.json").write_text(
            json.dumps(
                {
                    "target_company": "Acme",
                    "company_key": "acme",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_count": 1,
                    "materialization_generation_key": "gen-acme-sqlite-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-sqlite-1",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        (artifact_dir / "candidate_shards" / "c1.json").write_text(
            json.dumps({"candidate_id": "c1", "display_name": "Alice"}, ensure_ascii=False, indent=2)
        )
        client = build_object_storage_client(
            ObjectStorageConfig(
                provider="filesystem", local_dir=str(self.root / "object_store"), prefix="cloud-import-tests"
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            published = self.source_manager.publish_candidate_generation(target_company="Acme", client=client)
            imported = import_cloud_assets(
                bundle_manager=self.target_manager,
                storage_client=client,
                bundle_kind="sqlite_snapshot",
                bundle_id="legacy-sqlite-bundle",
                companies=["Acme"],
                snapshot_id=snapshot_id,
            )

        self.assertEqual(imported["status"], "completed")
        self.assertEqual(imported["bundle_kind"], "candidate_generation")
        self.assertEqual(imported["import_mode"], "candidate_generation")
        self.assertFalse(bool(imported["legacy_bundle_fallback_used"]))
        self.assertEqual(str(imported["requested_bundle_kind"] or ""), "sqlite_snapshot")
        self.assertEqual(str(imported["requested_bundle_id"] or ""), "legacy-sqlite-bundle")
        self.assertEqual(
            str(dict(imported.get("generation_first_attempt") or {}).get("generation_key") or ""),
            str(published.get("generation_key") or ""),
        )


if __name__ == "__main__":
    unittest.main()
