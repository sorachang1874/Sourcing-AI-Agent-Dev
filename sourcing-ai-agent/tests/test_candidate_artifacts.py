import json
from pathlib import Path
import tempfile
import unittest

from sourcing_agent.candidate_artifacts import (
    build_company_candidate_artifacts,
    load_company_snapshot_candidate_documents,
    materialize_company_candidate_view,
)
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
        roster_baseline = Candidate(
            candidate_id="c3",
            name_en="Carol Baseline",
            display_name="Carol Baseline",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Member of Technical Staff at Acme",
            work_history="Member of Technical Staff at Acme",
            notes="LinkedIn company roster baseline.",
            linkedin_url="https://www.linkedin.com/in/carol-baseline/",
            source_dataset="acme_linkedin_company_people",
        )
        self.store.upsert_candidate(current)
        self.store.upsert_candidate(lead)
        self.store.upsert_candidate(roster_baseline)
        self.store.upsert_candidate(
            Candidate(
                candidate_id="c4",
                name_en="Alice Example",
                display_name="Alice Example",
                category="employee",
                target_company="Acme",
                employment_status="current",
                role="Research Engineer",
                media_url="https://alice.example.com/about",
                source_dataset="rocketreach_seed",
            )
        )
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
                ),
                EvidenceRecord(
                    evidence_id=make_evidence_id("c4", "rocketreach_profile", "RocketReach", "https://rocketreach.co/alice"),
                    candidate_id="c4",
                    source_type="rocketreach_profile",
                    title="RocketReach",
                    url="https://www.linkedin.com/in/alice/",
                    summary="RocketReach found the same LinkedIn identity.",
                    source_dataset="rocketreach_profile",
                    source_path="/tmp/rocketreach.json",
                    metadata={"profile_url": "https://www.linkedin.com/in/alice/", "public_identifier": "alice"},
                ),
            ]
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 4)
        self.assertEqual(len(materialized_view["source_snapshots"]), 1)

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(result["status"], "built")
        summary = result["summary"]
        self.assertEqual(summary["candidate_count"], 4)
        self.assertEqual(summary["source_snapshot_count"], 1)
        self.assertEqual(summary["manual_review_backlog_count"], 1)
        self.assertEqual(summary["profile_completion_backlog_count"], 2)
        self.assertEqual(summary["explicit_profile_capture_count"], 0)

        normalized = json.loads(Path(result["artifact_paths"]["normalized_candidates"]).read_text())
        reusable = json.loads(Path(result["artifact_paths"]["reusable_candidate_documents"]).read_text())
        backlog = json.loads(Path(result["artifact_paths"]["manual_review_backlog"]).read_text())
        profile_completion_backlog = json.loads(Path(result["artifact_paths"]["profile_completion_backlog"]).read_text())
        strict_summary = result["views"]["strict_roster_only"]["summary"]
        strict_normalized = json.loads(
            Path(result["views"]["strict_roster_only"]["artifact_paths"]["normalized_candidates"]).read_text()
        )
        self.assertEqual(len(normalized), 4)
        self.assertEqual(len(reusable), 4)
        self.assertEqual(len(backlog), 1)
        self.assertEqual(len(profile_completion_backlog), 2)
        self.assertEqual(strict_summary["candidate_count"], 3)
        self.assertEqual(strict_summary["manual_review_backlog_count"], 0)
        snapshot_current = next(item for item in normalized if item["candidate_id"] == "c0")
        alice = next(item for item in normalized if item["candidate_id"] == "c1")
        bob = next(item for item in normalized if item["candidate_id"] == "c2")
        carol = next(item for item in normalized if item["candidate_id"] == "c3")
        strict_ids = {item["candidate_id"] for item in strict_normalized}
        self.assertTrue(snapshot_current["has_linkedin_url"])
        self.assertTrue(snapshot_current["needs_profile_completion"])
        self.assertTrue(alice["manual_review_confirmed"])
        self.assertTrue(alice["has_profile_detail"])
        self.assertEqual(alice["role_bucket"], "research")
        self.assertIn("engineering", alice["functional_facets"])
        self.assertEqual(alice["evidence_count"], 2)
        self.assertIn("rocketreach_profile", alice["source_datasets"])
        self.assertTrue(bob["needs_manual_review"])
        self.assertEqual(bob["manual_review_reason"], "unresolved_lead")
        self.assertFalse(carol["has_profile_detail"])
        self.assertTrue(carol["needs_profile_completion"])
        self.assertFalse(carol["has_explicit_profile_capture"])
        self.assertEqual(carol["role_bucket"], "engineering")
        self.assertNotIn("c2", strict_ids)

    def test_build_company_candidate_artifacts_surfaces_suspicious_membership_reviews(self) -> None:
        suspicious = Candidate(
            candidate_id="c5",
            name_en="Suspicious Example",
            display_name="Suspicious Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/suspicious-example/",
            source_dataset="acme_roster",
            metadata={
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
                "membership_review_rationale": "Profile content does not look like a plausible Acme employee profile.",
                "membership_review_triggers": ["suspicious_profile_content"],
                "membership_review_trigger_keywords": ["spiritual", "healer"],
            },
        )
        self.store.upsert_candidate(suspicious)
        self.store.upsert_evidence_records(
            [
                EvidenceRecord(
                    evidence_id=make_evidence_id("c5", "linkedin_profile_detail", "Profile", "https://www.linkedin.com/in/suspicious-example/"),
                    candidate_id="c5",
                    source_type="linkedin_profile_detail",
                    title="Profile",
                    url="https://www.linkedin.com/in/suspicious-example/",
                    summary="LinkedIn profile detail captured for suspicious review.",
                    source_dataset="linkedin_profile_detail",
                    source_path="/tmp/profile.json",
                ),
            ]
        )

        result = build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        normalized = json.loads(Path(result["artifact_paths"]["normalized_candidates"]).read_text())
        backlog = json.loads(Path(result["artifact_paths"]["manual_review_backlog"]).read_text())
        suspicious_normalized = next(item for item in normalized if item["candidate_id"] == "c5")
        suspicious_backlog = next(item for item in backlog if item["candidate_id"] == "c5")
        self.assertEqual(suspicious_normalized["status_bucket"], "lead")
        self.assertTrue(suspicious_normalized["needs_manual_review"])
        self.assertEqual(suspicious_normalized["manual_review_reason"], "suspicious_membership")
        self.assertEqual(suspicious_normalized["membership_review_decision"], "suspicious_member")
        self.assertEqual(suspicious_backlog["reason"], "suspicious_membership")

    def test_load_company_snapshot_candidate_documents_normalizes_investor_roles(self) -> None:
        snapshot_dir = self.runtime_dir / "company_assets" / "acme" / "20260406T120000" / "normalized_artifacts"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        strict_dir = snapshot_dir / "strict_roster_only"
        strict_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "candidates": [
                Candidate(
                    candidate_id="investor_1",
                    name_en="Ivy Investor",
                    display_name="Ivy Investor",
                    category="employee",
                    target_company="Acme",
                    organization="Acme",
                    employment_status="current",
                    role="Investor at Acme",
                    focus_areas="Investor at Acme | Seed investor",
                    source_dataset="acme_materialized",
                ).to_record(),
                Candidate(
                    candidate_id="other_1",
                    name_en="Other Company",
                    display_name="Other Company",
                    category="employee",
                    target_company="OtherCo",
                    organization="OtherCo",
                    employment_status="current",
                    role="Engineer",
                    source_dataset="other_materialized",
                ).to_record(),
            ],
            "evidence": [],
        }
        (snapshot_dir / "materialized_candidate_documents.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2))
        (strict_dir / "materialized_candidate_documents.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2))

        result = load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company="Acme",
        )
        self.assertEqual(result["snapshot_id"], "20260406T120000")
        self.assertEqual(len(result["candidates"]), 1)
        self.assertEqual(result["candidates"][0].display_name, "Ivy Investor")
        self.assertEqual(result["candidates"][0].category, "investor")
        self.assertEqual(result["candidates"][0].employment_status, "current")

        strict_result = load_company_snapshot_candidate_documents(
            runtime_dir=self.runtime_dir,
            target_company="Acme",
            view="strict_roster_only",
        )
        self.assertEqual(strict_result["asset_view"], "strict_roster_only")
        self.assertEqual(len(strict_result["candidates"]), 1)

    def test_materialized_view_canonicalizes_snapshot_and_sqlite_duplicates(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="lead_kevin",
            name_en="Kevin Example",
            display_name="Kevin Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
        )
        snapshot_evidence = {
            "evidence_id": make_evidence_id("lead_kevin", "publication_match", "Paper", "https://example.com/paper"),
            "candidate_id": "lead_kevin",
            "source_type": "publication_match",
            "title": "Paper",
            "url": "https://example.com/paper",
            "summary": "Lead from publication.",
            "source_dataset": "publication_match",
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
        self.store.upsert_candidate(
            Candidate(
                candidate_id="emp_kevin",
                name_en="Kevin Example",
                display_name="Kevin Example",
                category="employee",
                target_company="Acme",
                organization="Acme",
                employment_status="current",
                role="Research Engineer",
                linkedin_url="https://www.linkedin.com/in/kevin-example/",
                education="MIT",
                work_history="Acme",
                source_dataset="acme_linkedin_company_people",
                metadata={"public_identifier": "kevin-example"},
            )
        )
        self.store.upsert_evidence_records(
            [
                EvidenceRecord(
                    evidence_id=make_evidence_id(
                        "emp_kevin",
                        "linkedin_profile_detail",
                        "Research Engineer",
                        "https://www.linkedin.com/in/kevin-example/",
                    ),
                    candidate_id="emp_kevin",
                    source_type="linkedin_profile_detail",
                    title="Research Engineer",
                    url="https://www.linkedin.com/in/kevin-example/",
                    summary="Profile detail for Kevin Example.",
                    source_dataset="linkedin_profile_detail",
                    source_path="/tmp/kevin-example.json",
                ),
            ]
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 1)
        self.assertEqual(materialized_view["candidates"][0].candidate_id, "emp_kevin")
        self.assertEqual(materialized_view["canonicalization"]["name_merge_count"], 1)
        source_types = {item["source_type"] for item in materialized_view["evidence"]}
        self.assertIn("publication_match", source_types)
        self.assertIn("linkedin_profile_detail", source_types)

    def test_materialized_view_preserves_manual_non_member_resolution(self) -> None:
        snapshot_candidate = Candidate(
            candidate_id="seed_rabia",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="OpenAI at Acme",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
        )
        snapshot_evidence = {
            "evidence_id": make_evidence_id(
                "seed_rabia",
                "acme_search_seed_candidates",
                "Search seed",
                "https://www.linkedin.com/in/rabia-example/",
            ),
            "candidate_id": "seed_rabia",
            "source_type": "harvest_profile_search",
            "title": "Search seed",
            "url": "https://www.linkedin.com/in/rabia-example/",
            "summary": "Suspicious search-seed candidate.",
            "source_dataset": "acme_search_seed_candidates",
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
        self.store.upsert_candidate(
            Candidate(
                candidate_id="manual_rabia",
                name_en="Rabia Example",
                display_name="Rabia Example",
                category="non_member",
                target_company="Acme",
                organization="Other Org",
                employment_status="",
                role="Data Analyst",
                linkedin_url="https://www.linkedin.com/in/rabia-example/",
                source_dataset="acme_search_seed_candidates",
                source_path="/tmp/manual_review/rabia.json",
                metadata={
                    "manual_review_artifact_root": "/tmp/manual_review/rabia",
                    "manual_review_links": [{"label": "LinkedIn", "url": "https://www.linkedin.com/in/rabia-example/"}],
                    "target_company_mismatch": True,
                    "membership_review_required": False,
                    "membership_review_decision": "manual_non_member",
                },
            )
        )
        self.store.upsert_evidence_records(
            [
                EvidenceRecord(
                    evidence_id=make_evidence_id(
                        "manual_rabia",
                        "manual_review",
                        "LinkedIn",
                        "https://www.linkedin.com/in/rabia-example/",
                    ),
                    candidate_id="manual_rabia",
                    source_type="manual_review_link",
                    title="LinkedIn",
                    url="https://www.linkedin.com/in/rabia-example/",
                    summary="Manual review rejected this profile as unrelated to Acme.",
                    source_dataset="manual_review",
                    source_path="/tmp/manual_review/rabia_source.json",
                ),
            ]
        )

        materialized_view = materialize_company_candidate_view(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
        )
        self.assertEqual(len(materialized_view["candidates"]), 1)
        candidate = materialized_view["candidates"][0]
        self.assertEqual(candidate.category, "non_member")
        self.assertEqual(candidate.organization, "Other Org")
        self.assertEqual(candidate.metadata.get("membership_review_decision"), "manual_non_member")
        self.assertTrue(candidate.metadata.get("target_company_mismatch"))


if __name__ == "__main__":
    unittest.main()
