import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.asset_sync import AssetBundleManager


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
        self.assertTrue((restore_runtime / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").exists())

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
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        sqlite_path.write_bytes(b"sqlite-bytes")

        export = self.manager.export_company_handoff_bundle("Thinking Machines Lab")
        manifest = json.loads(Path(export["manifest_path"]).read_text())
        relpaths = {entry["runtime_relative_path"] for entry in manifest["files"]}
        self.assertIn("company_assets/thinkingmachineslab/latest_snapshot.json", relpaths)
        self.assertIn(
            "manual_review_assets/thinkingmachineslab/session_01/review_adhoc_01/resolution_input.json",
            relpaths,
        )
        self.assertIn("live_tests/harvest_profile_batch_tml/batch_summary.json", relpaths)
        self.assertIn("jobs/job_01.json", relpaths)
        self.assertIn("sourcing_agent.db", relpaths)

    def test_export_sqlite_snapshot_bundle(self) -> None:
        sqlite_path = self.runtime_dir / "sourcing_agent.db"
        sqlite_path.write_bytes(b"sqlite-bytes")
        export = self.manager.export_sqlite_snapshot()
        manifest = json.loads(Path(export["manifest_path"]).read_text())
        self.assertEqual(manifest["bundle_kind"], "sqlite_snapshot")
        self.assertEqual(manifest["files"][0]["runtime_relative_path"], "sourcing_agent.db")


if __name__ == "__main__":
    unittest.main()
