import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.asset_sync import AssetBundleManager
from sourcing_agent.object_storage import ObjectStorageConfig, build_object_storage_client
from sourcing_agent.storage import SQLiteStore


class AssetBundleManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.manager = AssetBundleManager(self.project_root, self.runtime_dir)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_export_and_restore_company_snapshot_bundle(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(json.dumps({"snapshot_id": "20260406T120000"}))
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"count": 2}))
        latest_path = self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json"
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {"canonical_name": "Acme", "aliases": ["acme"]},
                }
            )
        )
        export = self.manager.export_company_snapshot_bundle("Acme")
        manifest_path = Path(export["manifest_path"])
        manifest = json.loads(manifest_path.read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertIn("company_assets/acme/latest_snapshot.json", relpaths)
        self.assertIn("company_assets/acme/20260406T120000/manifest.json", relpaths)
        self.assertIn("company_assets/acme/20260406T120000/candidate_documents.json", relpaths)

        restore_runtime = self.project_root / "restored_runtime"
        summary = self.manager.restore_bundle(manifest_path, target_runtime_dir=restore_runtime, conflict="error")
        self.assertEqual(summary["status"], "restored")
        self.assertTrue((restore_runtime / "company_assets" / "acme" / "latest_snapshot.json").exists())
        self.assertTrue(
            (restore_runtime / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").exists()
        )

    def test_export_company_handoff_bundle_collects_related_runtime_assets(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "thinkingmachineslab" / "20260406T172703"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(json.dumps({"snapshot_id": "20260406T172703"}))
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"count": 25}))
        latest_path = self.runtime_dir / "company_assets" / "thinkingmachineslab" / "latest_snapshot.json"
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T172703",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {
                        "canonical_name": "Thinking Machines Lab",
                        "aliases": ["thinking machines", "tml"],
                        "linkedin_slug": "thinkingmachinesai",
                        "domain": "thinkingmachines.ai",
                    },
                }
            )
        )
        manual_review_path = (
            self.runtime_dir
            / "manual_review_assets"
            / "thinkingmachineslab"
            / "session_01"
            / "review_adhoc_01"
            / "resolution_input.json"
        )
        manual_review_path.parent.mkdir(parents=True, exist_ok=True)
        manual_review_path.write_text(json.dumps({"candidate": "Kevin Lu"}))
        live_test_path = self.runtime_dir / "live_tests" / "harvest_profile_batch_tml" / "batch_summary.json"
        live_test_path.parent.mkdir(parents=True, exist_ok=True)
        live_test_path.write_text(json.dumps({"count": 12}))
        job_path = self.runtime_dir / "jobs" / "job_01.json"
        job_path.parent.mkdir(parents=True, exist_ok=True)
        job_path.write_text(json.dumps({"target_company": "Thinking Machines Lab", "status": "completed"}))
        SQLiteStore(self.runtime_dir / "sourcing_agent.db")

        export = self.manager.export_company_handoff_bundle("Thinking Machines Lab", include_sqlite=True)
        manifest = json.loads(Path(export["manifest_path"]).read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertEqual(str(dict(manifest.get("metadata") or {}).get("control_plane_snapshot_role") or ""), "backup_only")
        self.assertIn("company_assets/thinkingmachineslab/latest_snapshot.json", relpaths)
        self.assertIn(
            "manual_review_assets/thinkingmachineslab/session_01/review_adhoc_01/resolution_input.json",
            relpaths,
        )
        self.assertIn("live_tests/harvest_profile_batch_tml/batch_summary.json", relpaths)
        self.assertIn("jobs/job_01.json", relpaths)
        self.assertIn("object_sync/control_plane/control_plane_snapshot.json", relpaths)

    def test_export_sqlite_snapshot_bundle(self) -> None:
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        sqlite_path.write_bytes(b"sqlite-bytes")
        export = self.manager.export_sqlite_snapshot()
        manifest = json.loads(Path(export["manifest_path"]).read_text())
        self.assertEqual(manifest["bundle_kind"], "sqlite_snapshot")
        self.assertEqual(str(dict(manifest.get("metadata") or {}).get("sqlite_role") or ""), "backup_only")
        self.assertEqual(manifest["files"][0]["runtime_relative_path"], "sourcing_agent.db")

    def test_restore_sqlite_snapshot_compat_restores_valid_control_plane_snapshot_bundle(self) -> None:
        store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")
        store.save_job(
            "job-control-plane",
            "workflow",
            "completed",
            "completed",
            {"target_company": "Acme"},
        )
        export = self.manager.export_sqlite_snapshot()

        manifest = json.loads(Path(export["manifest_path"]).read_text())
        self.assertEqual(manifest["bundle_kind"], "control_plane_snapshot")

        replacement_store = SQLiteStore(self.runtime_dir / "replacement.db")
        replacement_store.save_job(
            "job-replacement",
            "workflow",
            "running",
            "running",
            {"target_company": "Other"},
        )

        restore = self.manager.restore_sqlite_snapshot(
            export["manifest_path"],
            target_db_path=self.runtime_dir / "replacement.db",
        )

        self.assertEqual(restore["status"], "control_plane_snapshot_restored")
        self.assertTrue(bool(restore["backup_path"]))
        restored_store = SQLiteStore(self.runtime_dir / "replacement.db")
        self.assertIsNotNone(restored_store.get_job("job-control-plane"))
        self.assertIsNone(restored_store.get_job("job-replacement"))

    def test_export_company_handoff_bundle_excludes_sqlite_by_default(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(json.dumps({"snapshot_id": "20260406T120000"}))
        latest_path = self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json"
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {"canonical_name": "Acme", "aliases": ["acme"]},
                }
            )
        )
        (self.runtime_dir / "sourcing_agent.db").write_bytes(b"sqlite-bytes")

        export = self.manager.export_company_handoff_bundle("Acme")
        manifest = json.loads(Path(export["manifest_path"]).read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}

        self.assertNotIn("sourcing_agent.db", relpaths)
        self.assertFalse(bool(dict(manifest.get("metadata") or {}).get("include_sqlite")))

    def test_export_company_snapshot_bundle_from_external_canonical_root(self) -> None:
        canonical_root = self.project_root / "canonical_company_assets"
        snapshot_dir = canonical_root / "acme" / "20260406T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "linkedin_slug": "acme",
                    },
                }
            )
        )
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"count": 2}))
        latest_path = canonical_root / "acme" / "latest_snapshot.json"
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260406T120000",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {"canonical_name": "Acme", "aliases": ["acme"]},
                }
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root)},
            clear=False,
        ):
            export = self.manager.export_company_snapshot_bundle("Acme")

        manifest_path = Path(export["manifest_path"])
        manifest = json.loads(manifest_path.read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertIn("company_assets/acme/latest_snapshot.json", relpaths)
        self.assertIn("company_assets/acme/20260406T120000/manifest.json", relpaths)
        self.assertIn("company_assets/acme/20260406T120000/candidate_documents.json", relpaths)
        self.assertFalse((self.runtime_dir / "company_assets" / "acme").exists())

        restore_runtime = self.project_root / "restored_runtime"
        summary = self.manager.restore_bundle(manifest_path, target_runtime_dir=restore_runtime, conflict="error")
        self.assertEqual(summary["status"], "restored")
        self.assertTrue((restore_runtime / "company_assets" / "acme" / "latest_snapshot.json").exists())
        self.assertTrue(
            (restore_runtime / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").exists()
        )

    def test_export_company_snapshot_bundle_from_hot_cache_root(self) -> None:
        hot_cache_root = self.project_root / "hot_cache_company_assets"
        snapshot_dir = hot_cache_root / "reflectionai" / "20260418T120000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": "20260418T120000",
                    "company_identity": {
                        "requested_name": "Reflection AI",
                        "canonical_name": "Reflection AI",
                        "company_key": "reflectionai",
                        "linkedin_slug": "reflectionai",
                        "aliases": ["reflection ai"],
                    },
                }
            )
        )
        (snapshot_dir / "candidate_documents.json").write_text(json.dumps({"count": 3}))
        latest_path = hot_cache_root / "reflectionai" / "latest_snapshot.json"
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_path.write_text(
            json.dumps(
                {
                    "snapshot_id": "20260418T120000",
                    "snapshot_dir": str(snapshot_dir),
                    "company_identity": {
                        "requested_name": "Reflection AI",
                        "canonical_name": "Reflection AI",
                        "company_key": "reflectionai",
                        "aliases": ["reflection ai"],
                    },
                }
            )
        )

        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root)},
            clear=False,
        ):
            export = self.manager.export_company_snapshot_bundle("Reflection AI")

        manifest_path = Path(export["manifest_path"])
        manifest = json.loads(manifest_path.read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertIn("company_assets/reflectionai/latest_snapshot.json", relpaths)
        self.assertIn("company_assets/reflectionai/20260418T120000/manifest.json", relpaths)
        self.assertIn("company_assets/reflectionai/20260418T120000/candidate_documents.json", relpaths)

        restore_runtime = self.project_root / "restored_hot_cache_runtime"
        summary = self.manager.restore_bundle(manifest_path, target_runtime_dir=restore_runtime, conflict="error")
        self.assertEqual(summary["status"], "restored")
        self.assertTrue((restore_runtime / "company_assets" / "reflectionai" / "latest_snapshot.json").exists())
        self.assertTrue(
            (
                restore_runtime / "company_assets" / "reflectionai" / "20260418T120000" / "candidate_documents.json"
            ).exists()
        )

    def test_publish_and_hydrate_candidate_generation_tracks_generation_index(self) -> None:
        canonical_root = self.project_root / "canonical_company_assets"
        hot_cache_root = self.project_root / "hot_cache_company_assets"
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
            "aliases": ["acme ai"],
        }
        (canonical_root / "acme" / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": company_identity}, ensure_ascii=False, indent=2)
        )
        (snapshot_dir / "identity.json").write_text(json.dumps(company_identity, ensure_ascii=False, indent=2))
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": snapshot_id, "company_identity": company_identity},
                    "candidates": [{"candidate_id": "c1", "target_company": "Acme", "display_name": "Alice"}],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
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
                    "materialization_generation_key": "gen-acme-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-1",
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
                    "materialization_generation_key": "gen-acme-1",
                    "materialization_generation_sequence": 1,
                    "materialization_watermark": "1:gen-acme-1",
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
                    "materialization_generation_key": "gen-acme-1",
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
                provider="filesystem", local_dir=str(self.project_root / "object_store"), prefix="sync-tests"
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
            publish = self.manager.publish_candidate_generation(target_company="Acme", client=client)
            hydrate = self.manager.hydrate_published_generation(
                client=client,
                target_company="Acme",
                snapshot_id=snapshot_id,
                asset_view="canonical_merged",
            )
            resolved_generation = self.manager.resolve_candidate_generation(
                client=client,
                target_company="Acme",
                snapshot_id=snapshot_id,
                asset_view="canonical_merged",
            )

        self.assertEqual(publish["status"], "uploaded")
        self.assertEqual(hydrate["status"], "hydrated")
        self.assertEqual(str(resolved_generation.get("generation_key") or ""), str(publish.get("generation_key") or ""))
        self.assertEqual(str(resolved_generation.get("resolved_via") or ""), "local_generation_index")
        self.assertTrue(client.has_object("indexes/generation_index.json"))
        self.assertTrue((self.runtime_dir / "object_sync" / "generation_index.json").exists())
        self.assertTrue((hot_cache_root / "acme" / snapshot_id / "normalized_artifacts" / "manifest.json").exists())
        self.assertTrue(
            (hot_cache_root / "acme" / snapshot_id / "normalized_artifacts" / "candidate_shards" / "c1.json").exists()
        )
        self.assertGreaterEqual(int(hydrate.get("linked_file_count") or 0), 1)


if __name__ == "__main__":
    unittest.main()
