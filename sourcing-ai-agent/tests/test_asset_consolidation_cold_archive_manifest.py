import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_consolidation_cold_archive_manifest import (
    build_asset_consolidation_cold_archive_manifest,
    render_asset_consolidation_cold_archive_manifest_markdown,
)


class AssetConsolidationColdArchiveManifestTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_snapshot(self, company_key: str, snapshot_id: str, files: dict[str, str]) -> Path:
        snapshot_dir = self.runtime_dir / "company_assets" / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        for relative_path, content in files.items():
            path = snapshot_dir / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        return snapshot_dir

    def _plan(self, candidates: list[dict[str, object]]) -> dict[str, object]:
        return {
            "contract_version": "asset_consolidation_plan_v1",
            "generated_at": "2026-05-23T00:00:00+00:00",
            "companies": [
                {
                    "company_key": "openai",
                    "target_company": "OpenAI",
                    "collection_id": "company:openai",
                    "archive_plan": {
                        "archive_candidate_count": len(candidates),
                        "archive_ready_snapshot_count": sum(
                            1 for item in candidates if item.get("decision") == "archive_ready_for_cold_backup_review"
                        ),
                        "blocked_archive_candidate_count": sum(
                            1 for item in candidates if item.get("decision") != "archive_ready_for_cold_backup_review"
                        ),
                        "candidates": candidates,
                    },
                }
            ],
        }

    def test_manifest_lists_ready_snapshot_files_without_allowing_deletion(self) -> None:
        self._write_snapshot(
            "openai",
            "snap-duplicate",
            {
                "manifest.json": json.dumps({"snapshot_id": "snap-duplicate"}),
                "candidate_documents.json": json.dumps({"candidates": [{"candidate_id": "ada"}]}),
            },
        )
        plan = self._plan(
            [
                {
                    "snapshot_id": "snap-duplicate",
                    "decision": "archive_ready_for_cold_backup_review",
                    "classification": "archive_candidate_no_increment_duplicate",
                    "reason": "overlap_subsumed_by_reference",
                    "overlap_status": "subsumed_by_reference",
                    "overlap_ratio": 1.0,
                    "unique_count": 0,
                    "candidate_count": 1,
                    "profile_detail_count": 1,
                    "deletion_blockers": [],
                }
            ]
        )

        manifest = build_asset_consolidation_cold_archive_manifest(plan=plan, runtime_dir=self.runtime_dir)

        self.assertEqual(manifest["status"], "ready_for_cold_backup_review")
        self.assertTrue(manifest["read_only"])
        self.assertFalse(manifest["deletion_allowed"])
        self.assertTrue(manifest["normal_reuse_exclusion_recommended"])
        snapshot = manifest["companies"][0]["snapshots"][0]
        self.assertEqual(snapshot["status"], "ready_for_cold_backup_review")
        self.assertEqual(snapshot["backup_key"], "openai/snap-duplicate")
        self.assertEqual(snapshot["source_root"], "company_assets")
        self.assertEqual(snapshot["file_count"], 2)
        self.assertGreater(snapshot["total_size_bytes"], 0)
        self.assertFalse(snapshot["deletion_allowed"])
        self.assertTrue(snapshot["normal_reuse_exclusion_recommended"])
        files_by_path = {item["relative_path"]: item for item in snapshot["files"]}
        self.assertEqual(
            files_by_path["manifest.json"]["sha256"],
            hashlib.sha256(json.dumps({"snapshot_id": "snap-duplicate"}).encode("utf-8")).hexdigest(),
        )
        self.assertEqual(len(snapshot["manifest_digest_sha256"]), 64)
        markdown = render_asset_consolidation_cold_archive_manifest_markdown(manifest)
        self.assertIn("# Asset Consolidation Cold Archive Manifest", markdown)
        self.assertIn("snap-duplicate", markdown)

    def test_manifest_blocks_unique_or_not_subsumed_archive_candidate(self) -> None:
        self._write_snapshot("openai", "snap-unique", {"manifest.json": "{}"})
        plan = self._plan(
            [
                {
                    "snapshot_id": "snap-unique",
                    "decision": "blocked_overlap_review",
                    "classification": "archive_candidate_no_increment_duplicate",
                    "reason": "review_unique_candidates_present",
                    "overlap_status": "review_unique_candidates_present",
                    "overlap_ratio": 0.5,
                    "unique_count": 1,
                    "candidate_count": 2,
                    "profile_detail_count": 2,
                    "deletion_blockers": [],
                }
            ]
        )

        manifest = build_asset_consolidation_cold_archive_manifest(plan=plan, runtime_dir=self.runtime_dir)

        snapshot = manifest["companies"][0]["snapshots"][0]
        self.assertEqual(manifest["status"], "blocked_no_cold_archive_ready_snapshots")
        self.assertEqual(snapshot["status"], "blocked_cold_backup_manifest")
        self.assertEqual(snapshot["source_dir"], "")
        self.assertEqual(snapshot["file_count"], 0)
        self.assertEqual(snapshot["files"], [])
        self.assertIn("archive_candidate_not_ready", snapshot["blocking_reasons"])
        self.assertIn("overlap_not_subsumed_by_reference", snapshot["blocking_reasons"])
        self.assertIn("unique_identities_present", snapshot["blocking_reasons"])
        self.assertFalse(snapshot["normal_reuse_exclusion_recommended"])

    def test_manifest_blocks_ready_candidate_when_source_dir_is_missing(self) -> None:
        plan = self._plan(
            [
                {
                    "snapshot_id": "snap-missing",
                    "decision": "archive_ready_for_cold_backup_review",
                    "classification": "archive_candidate_no_increment_duplicate",
                    "reason": "overlap_subsumed_by_reference",
                    "overlap_status": "subsumed_by_reference",
                    "overlap_ratio": 1.0,
                    "unique_count": 0,
                    "candidate_count": 1,
                    "profile_detail_count": 1,
                    "deletion_blockers": [],
                }
            ]
        )

        manifest = build_asset_consolidation_cold_archive_manifest(plan=plan, runtime_dir=self.runtime_dir)

        snapshot = manifest["companies"][0]["snapshots"][0]
        self.assertEqual(snapshot["status"], "blocked_cold_backup_manifest")
        self.assertIn("source_snapshot_dir_missing", snapshot["blocking_reasons"])
        self.assertEqual(snapshot["files"], [])
        self.assertFalse(snapshot["normal_reuse_exclusion_recommended"])

    def test_manifest_blocks_when_file_listing_is_truncated(self) -> None:
        self._write_snapshot(
            "openai",
            "snap-many-files",
            {
                "manifest.json": "{}",
                "candidate_documents.json": "{}",
            },
        )
        plan = self._plan(
            [
                {
                    "snapshot_id": "snap-many-files",
                    "decision": "archive_ready_for_cold_backup_review",
                    "classification": "archive_candidate_no_increment_duplicate",
                    "reason": "overlap_subsumed_by_reference",
                    "overlap_status": "subsumed_by_reference",
                    "overlap_ratio": 1.0,
                    "unique_count": 0,
                    "candidate_count": 1,
                    "profile_detail_count": 1,
                    "deletion_blockers": [],
                }
            ]
        )

        manifest = build_asset_consolidation_cold_archive_manifest(
            plan=plan,
            runtime_dir=self.runtime_dir,
            max_files_per_snapshot=1,
        )

        snapshot = manifest["companies"][0]["snapshots"][0]
        self.assertEqual(snapshot["status"], "blocked_cold_backup_manifest")
        self.assertTrue(snapshot["truncated"])
        self.assertIn("file_manifest_truncated", snapshot["blocking_reasons"])
        self.assertEqual(snapshot["file_count"], 1)


if __name__ == "__main__":
    unittest.main()
