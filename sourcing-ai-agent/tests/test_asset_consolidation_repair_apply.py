import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.asset_consolidation_repair_apply import (
    apply_asset_consolidation_repair,
    render_asset_consolidation_repair_apply_markdown,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AssetConsolidationRepairApplyTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = self.make_pg_store(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _write_candidate_payload(self, company_key: str, snapshot_id: str, candidates: list[dict[str, object]]) -> Path:
        snapshot_dir = self.runtime_dir / "company_assets" / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        path = snapshot_dir / "candidate_documents.json"
        path.write_text(json.dumps({"candidates": candidates}, ensure_ascii=False), encoding="utf-8")
        return path

    def _proposal(self, *, payload_path: Path, snapshot_id: str = "snap-repair", delta: int = 0) -> dict[str, object]:
        risks = ["registry_payload_count_mismatch"] if delta else []
        return {
            "contract_version": "asset_consolidation_repair_proposal_v1",
            "generated_at": "2026-05-22T00:00:00+00:00",
            "companies": [
                {
                    "company_key": "openai",
                    "target_company": "OpenAI",
                    "collection_id": "company:openai",
                    "status": "ready_for_manual_authoritative_repair_review",
                    "missing_reference_snapshot_ids": ["snap-missing-reference"],
                    "verified_candidates": [
                        {
                            "snapshot_id": snapshot_id,
                            "verification_status": "verified_payload_available",
                            "payload_candidate_count": 2,
                            "identity_count": 2,
                            "profile_url_identity_count": 2,
                            "fallback_identity_count": 0,
                            "truncated": False,
                            "registry_payload_candidate_delta": delta,
                            "candidate_payload_path": str(payload_path),
                            "candidate_payload_source": "local_path",
                            "promotion_risks": risks,
                        }
                    ],
                    "recommended_candidate": {
                        "snapshot_id": snapshot_id,
                    },
                }
            ],
        }

    def test_dry_run_plans_projection_without_mutating_pointer_or_members(self) -> None:
        payload_path = self._write_candidate_payload(
            "openai",
            "snap-repair",
            [
                {"candidate_id": "ada", "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/"},
                {"candidate_id": "grace", "linkedin_url": "https://www.linkedin.com/in/grace-hopper/"},
            ],
        )

        report = apply_asset_consolidation_repair(
            proposal=self._proposal(payload_path=payload_path),
            runtime_dir=self.runtime_dir,
            store=self.store,
            selections={"openai": "snap-repair"},
            apply=False,
        )

        company = dict(report["companies"][0])
        self.assertEqual(report["status"], "dry_run_ready")
        self.assertTrue(report["read_only"])
        self.assertEqual(company["status"], "dry_run_ready")
        self.assertEqual(company["member_count"], 2)
        self.assertTrue(str(company["planned_projection_id"]).startswith("proj_assetrepair_"))
        self.assertEqual(self.store.get_collection_authoritative_pointer("company:openai"), {})
        self.assertEqual(self.store.get_serving_projection(str(company["planned_projection_id"])), {})
        markdown = render_asset_consolidation_repair_apply_markdown(report)
        self.assertIn("# Asset Consolidation Repair Apply Report", markdown)
        self.assertIn("snap-repair", markdown)

    def test_apply_requires_manual_review_acceptance(self) -> None:
        payload_path = self._write_candidate_payload(
            "openai",
            "snap-repair",
            [
                {"candidate_id": "ada", "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/"},
                {"candidate_id": "grace", "linkedin_url": "https://www.linkedin.com/in/grace-hopper/"},
            ],
        )

        report = apply_asset_consolidation_repair(
            proposal=self._proposal(payload_path=payload_path),
            runtime_dir=self.runtime_dir,
            store=self.store,
            selections={"openai": "snap-repair"},
            apply=True,
            manual_review_accepted=False,
        )

        company = dict(report["companies"][0])
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(company["status"], "blocked_pre_apply_gates")
        self.assertIn("manual_review_not_accepted", company["blocking_reasons"])
        self.assertEqual(self.store.get_collection_authoritative_pointer("company:openai"), {})

    def test_apply_publishes_collection_authoritative_projection_and_pointer(self) -> None:
        self.store.upsert_serving_projection(
            {
                "projection_id": "proj_previous",
                "projection_type": "collection_authoritative_projection",
                "collection_id": "company:openai",
                "state": "serving",
            }
        )
        self.store.upsert_collection_authoritative_pointer(
            {
                "collection_id": "company:openai",
                "active_projection_id": "proj_previous",
                "active_collection_version": "snap-missing-reference",
            }
        )
        payload_path = self._write_candidate_payload(
            "openai",
            "snap-repair",
            [
                {
                    "candidate_id": "ada",
                    "name": "Ada Lovelace",
                    "headline": "Computing pioneer",
                    "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/",
                    "has_profile_detail": True,
                    "experience_lines": ["1843~Present, Analytical Engine, Programmer"],
                    "education_lines": ["Private tutors, Mathematics"],
                },
                {
                    "candidate_id": "grace",
                    "display_name": "Grace Hopper",
                    "linkedin_url": "https://www.linkedin.com/in/grace-hopper/",
                },
            ],
        )

        report = apply_asset_consolidation_repair(
            proposal=self._proposal(payload_path=payload_path),
            runtime_dir=self.runtime_dir,
            store=self.store,
            selections={"openai": "snap-repair"},
            apply=True,
            manual_review_accepted=True,
            source_proposal_path="/tmp/proposal.json",
        )

        company = dict(report["companies"][0])
        projection_id = str(company["planned_projection_id"])
        pointer = self.store.get_collection_authoritative_pointer("company:openai")
        members = self.store.list_serving_projection_members(projection_id, limit=10)

        self.assertEqual(report["status"], "applied")
        self.assertFalse(report["read_only"])
        self.assertEqual(company["status"], "applied")
        self.assertTrue(company["applied"])
        self.assertEqual(pointer["active_projection_id"], projection_id)
        self.assertEqual(pointer["previous_projection_id"], "proj_previous")
        self.assertEqual(pointer["active_collection_version"], "snap-repair")
        self.assertEqual(len(members), 2)
        self.assertEqual(members[0]["candidate_identity_key"], "linkedin:https://www.linkedin.com/in/ada-lovelace")
        self.assertEqual(members[0]["public_summary"]["target_company"], "OpenAI")
        self.assertEqual(members[0]["public_summary"]["source_collection_id"], "company:openai")
        self.assertEqual(
            members[0]["public_summary"]["experience_lines"],
            ["1843~Present, Analytical Engine, Programmer"],
        )
        self.assertEqual(members[0]["public_summary"]["education_lines"], ["Private tutors, Mathematics"])
        self.assertEqual(members[0]["projection_metrics"]["repair_phase"], "W5c.3")
        projection = self.store.get_serving_projection(projection_id)
        self.assertEqual(projection["projection_type"], "collection_authoritative_projection")
        self.assertEqual(projection["scope_spec"]["repair_source"], "w5c_asset_consolidation_repair")
        self.assertEqual(projection["counts"]["result_count"], 2)
        self.assertEqual(projection["metadata"]["registry_rows_mutated"], False)

    def test_count_mismatch_candidate_is_rejected(self) -> None:
        payload_path = self._write_candidate_payload(
            "openai",
            "snap-mismatch",
            [
                {"candidate_id": "ada", "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/"},
                {"candidate_id": "grace", "linkedin_url": "https://www.linkedin.com/in/grace-hopper/"},
            ],
        )

        report = apply_asset_consolidation_repair(
            proposal=self._proposal(payload_path=payload_path, snapshot_id="snap-mismatch", delta=1),
            runtime_dir=self.runtime_dir,
            store=self.store,
            selections={"openai": "snap-mismatch"},
            apply=False,
        )

        company = dict(report["companies"][0])
        self.assertEqual(report["status"], "blocked")
        self.assertIn("candidate_registry_payload_count_mismatch", company["blocking_reasons"])
        self.assertIn("blocking_promotion_risk:registry_payload_count_mismatch", company["blocking_reasons"])

    def test_selection_must_match_proposal_company_and_candidate(self) -> None:
        payload_path = self._write_candidate_payload(
            "openai",
            "snap-repair",
            [
                {"candidate_id": "ada", "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/"},
                {"candidate_id": "grace", "linkedin_url": "https://www.linkedin.com/in/grace-hopper/"},
            ],
        )

        missing_company = apply_asset_consolidation_repair(
            proposal=self._proposal(payload_path=payload_path),
            runtime_dir=self.runtime_dir,
            store=self.store,
            selections={"anthropic": "snap-repair"},
            apply=False,
        )
        wrong_snapshot = apply_asset_consolidation_repair(
            proposal=self._proposal(payload_path=payload_path),
            runtime_dir=self.runtime_dir,
            store=self.store,
            selections={"openai": "snap-other"},
            apply=False,
        )

        self.assertEqual(missing_company["companies"][0]["status"], "blocked_company_not_found_in_proposal")
        self.assertEqual(wrong_snapshot["companies"][0]["status"], "blocked_pre_apply_gates")
        self.assertIn("candidate_snapshot_not_found_in_verified_candidates", wrong_snapshot["companies"][0]["blocking_reasons"])


if __name__ == "__main__":
    unittest.main()
