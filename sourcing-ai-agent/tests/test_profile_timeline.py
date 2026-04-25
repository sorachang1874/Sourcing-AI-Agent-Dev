import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.profile_timeline import profile_snapshot_from_source_path, resolve_candidate_profile_timeline


class ProfileTimelinePathMigrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260409T101500"
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.profile_path = self.snapshot_dir / "harvest_profiles" / "alice.json"
        self.profile_path.parent.mkdir(parents=True, exist_ok=True)
        self.profile_path.write_text(
            json.dumps(
                {
                    "item": {
                        "display_name": "Alice Example",
                        "linkedin_url": "https://www.linkedin.com/in/alice-example",
                        "headline": "Research Engineer at Acme",
                        "experience": [
                            {
                                "companyName": "Acme",
                                "title": "Research Engineer",
                                "current": True,
                            }
                        ],
                        "education": [
                            {
                                "schoolName": "MIT",
                                "degreeName": "BS",
                                "fieldOfStudy": "Computer Science",
                            }
                        ],
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.legacy_linux_source_path = (
            "/home/sorachang/projects/Sourcing AI Agent Dev/"
            "sourcing-ai-agent/runtime/company_assets/acme/20260409T101500/harvest_profiles/alice.json"
        )
        self.env_patcher = mock.patch.dict(
            os.environ,
            {"SOURCING_RUNTIME_DIR": str(self.runtime_dir)},
            clear=False,
        )
        self.env_patcher.start()

    def tearDown(self) -> None:
        self.env_patcher.stop()
        self.tempdir.cleanup()

    def test_profile_snapshot_from_source_path_reads_local_profile_after_migration(self) -> None:
        result = profile_snapshot_from_source_path(self.legacy_linux_source_path)

        self.assertEqual(result["headline"], "Research Engineer at Acme")
        self.assertIn("Acme", result["experience_lines"][0])
        self.assertIn("MIT", result["education_lines"][0])
        self.assertEqual(Path(result["profile_capture_source_path"]).resolve(), self.profile_path.resolve())

    def test_resolve_candidate_profile_timeline_rewrites_legacy_source_path_to_local_runtime(self) -> None:
        result = resolve_candidate_profile_timeline(
            payload={
                "display_name": "Alice Example",
                "linkedin_url": "https://www.linkedin.com/in/alice-example",
            },
            source_path=self.legacy_linux_source_path,
        )

        self.assertEqual(result["source_kind"], "source_path")
        self.assertEqual(Path(result["source_path"]).resolve(), self.profile_path.resolve())
        self.assertEqual(Path(result["profile_capture_source_path"]).resolve(), self.profile_path.resolve())
        self.assertIn("Acme", result["experience_lines"][0])

    def test_profile_snapshot_uses_selector_index_for_multi_item_source_and_invalidates_on_file_change(self) -> None:
        roster_path = self.snapshot_dir / "harvest_company_employees" / "roster.json"
        roster_path.parent.mkdir(parents=True, exist_ok=True)
        roster_path.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "display_name": "Alice Example",
                            "linkedin_url": "https://www.linkedin.com/in/alice-example/",
                            "headline": "Research Engineer at Acme",
                        },
                        {
                            "display_name": "Bob Example",
                            "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                            "headline": "Staff Engineer at Acme",
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        first = profile_snapshot_from_source_path(
            str(roster_path),
            payload={"display_name": "Bob Example", "linkedin_url": "https://www.linkedin.com/in/bob-example/"},
        )
        self.assertEqual(first["headline"], "Staff Engineer at Acme")

        roster_path.write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "display_name": "Bob Example",
                            "linkedin_url": "https://www.linkedin.com/in/bob-example/",
                            "headline": "Principal Engineer at Acme with updated cache signature",
                        }
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        second = profile_snapshot_from_source_path(
            str(roster_path),
            payload={"display_name": "Bob Example", "linkedin_url": "https://www.linkedin.com/in/bob-example/"},
        )
        self.assertEqual(second["headline"], "Principal Engineer at Acme with updated cache signature")


if __name__ == "__main__":
    unittest.main()
