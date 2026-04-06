import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.candidate_artifacts import build_company_candidate_artifacts, materialize_company_candidate_view
from sourcing_agent.domain import Candidate, EvidenceRecord, make_evidence_id
from sourcing_agent.storage import SQLiteStore


class CandidateArtifactsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
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
                        "aliases": ["acme ai"],
                    },
                }
            )
        )
        self.store = SQLiteStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_build_company_candidate_artifacts_materializes_backlog_and_reusable_docs(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="c0",
            name_en="Snapshot Current",
            display_name="Snapshot Current",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Member of Technical Staff",
            linkedin_url="https://www.linkedin.com/in/snapshot-current",
            source_dataset="acme_roster_snapshot",
        )
        snapshot_evidence = {
            "evidence_id": make_evidence_id("c0", "acme_roster_snapshot", "Roster row", "https://www.linkedin.com/in/snapshot-current"),
            "candidate_id": "c0",
            "source_type": "company_roster",
            "title": "Roster row",
            "url": "https://www.linkedin.com/in/snapshot-current",
            "summary": "Recovered from Acme current roster snapshot.",
            "source_dataset": "acme_roster_snapshot",
            "source_path": str(self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json"),
            "metadata": {},
        }
        (self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "candidates": [snapshot_candidate.to_record()],
                    "evidence": [snapshot_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        current = Candidate(
            candidate_id="c1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice",
            education="MIT",
            work_history="Acme",
            source_dataset="acme_roster",
        )
        lead = Candidate(
            candidate_id="c2",
            name_en="Bob Lead",
            display_name="Bob Lead",
            category="lead",
            target_company="Acme",
            employment_status="",
            role="Unknown",
            source_dataset="publication_feed",
        )
        self.store.upsert_candidate(current)
        self.store.upsert_candidate(lead)
        self.store.upsert_evidence_records(
            [
                EvidenceRecord(
                    evidence_id=make_evidence_id("c1", "manual_review_link", "Homepage", "https://alice.example.com"),
                    candidate_id="c1",
                    source_type="manual_review_link",
                    title="Homepage",
                    url="https://alice.example.com",
                    summary="Manual review confirmed Alice Example.",
                    source_dataset="manual_review",
                    source_path="/tmp/manual_review.json",
                )
            ]
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 3)
        self.assertEqual(len(materialized_view["source_snapshots"]), 1)

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(result["status"], "built")
        summary = result["summary"]
        self.assertEqual(summary["candidate_count"], 3)
        self.assertEqual(summary["source_snapshot_count"], 1)
        self.assertEqual(summary["manual_review_backlog_count"], 1)
        self.assertEqual(summary["profile_completion_backlog_count"], 1)

        normalized = json.loads(Path(result["artifact_paths"]["normalized_candidates"]).read_text())
        reusable = json.loads(Path(result["artifact_paths"]["reusable_candidate_documents"]).read_text())
        backlog = json.loads(Path(result["artifact_paths"]["manual_review_backlog"]).read_text())
        profile_completion_backlog = json.loads(Path(result["artifact_paths"]["profile_completion_backlog"]).read_text())
        self.assertEqual(len(normalized), 3)
        self.assertEqual(len(reusable), 3)
        self.assertEqual(len(backlog), 1)
        self.assertEqual(len(profile_completion_backlog), 1)
        snapshot_current = next(item for item in normalized if item["candidate_id"] == "c0")
        alice = next(item for item in normalized if item["candidate_id"] == "c1")
        bob = next(item for item in normalized if item["candidate_id"] == "c2")
        self.assertTrue(snapshot_current["has_linkedin_url"])
        self.assertTrue(snapshot_current["needs_profile_completion"])
        self.assertTrue(alice["manual_review_confirmed"])
        self.assertTrue(alice["has_profile_detail"])
        self.assertTrue(bob["needs_manual_review"])
        self.assertEqual(bob["manual_review_reason"], "unresolved_lead")


if __name__ == "__main__":
    unittest.main()
