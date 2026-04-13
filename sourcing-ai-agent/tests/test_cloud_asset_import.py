import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.asset_sync import AssetBundleManager
from sourcing_agent.cloud_asset_import import import_cloud_assets
from sourcing_agent.domain import Candidate, make_evidence_id
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
        self.assertEqual(result["artifact_repair"]["status"], "completed")
        self.assertEqual(result["artifact_repair"]["repaired_snapshot_count"], 1)
        self.assertEqual(result["organization_warmup"]["status"], "completed")
        self.assertEqual(result["profile_registry_backfill"]["status"], "completed")

        normalized_dir = self.target_runtime / "company_assets" / "acme" / "20260413T120000" / "normalized_artifacts"
        self.assertTrue((normalized_dir / "materialized_candidate_documents.json").exists())
        self.assertTrue((normalized_dir / "artifact_summary.json").exists())
        self.assertTrue((normalized_dir / "organization_completeness_ledger.json").exists())

        store = SQLiteStore(self.target_runtime / "sourcing_agent.db")
        authoritative = store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        self.assertEqual(str(authoritative.get("snapshot_id") or ""), "20260413T120000")
        profile_registry_entry = store.get_linkedin_profile_registry("https://www.linkedin.com/in/alice-example/")
        self.assertIsNotNone(profile_registry_entry)
        assert profile_registry_entry is not None
        self.assertEqual(profile_registry_entry["status"], "fetched")


if __name__ == "__main__":
    unittest.main()
