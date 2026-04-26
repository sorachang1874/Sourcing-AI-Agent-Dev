import json
import tempfile
import unittest
from pathlib import Path

from sourcing_agent.domain import Candidate
from sourcing_agent.organization_assets import load_company_snapshot_registry_summary
from sourcing_agent.storage import ControlPlaneStore


class OrganizationAssetsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_load_company_snapshot_registry_summary_uses_authoritative_source_payload_for_materialized_selection(self) -> None:
        current_snapshot_id = "20260420T010101"
        previous_snapshot_id = "20260419T235959"

        for snapshot_id, candidate_id in [
            (previous_snapshot_id, "cand_acme_prev"),
            (current_snapshot_id, "cand_acme_current"),
        ]:
            snapshot_dir = self.runtime_dir / "company_assets" / "acme" / snapshot_id
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            candidate = Candidate(
                candidate_id=candidate_id,
                name_en="Ada Builder",
                display_name="Ada Builder",
                category="employee",
                target_company="Acme",
                organization="Acme",
                employment_status="current",
                role="Research Engineer",
                linkedin_url="https://www.linkedin.com/in/ada-builder/",
                source_dataset="candidate_documents",
                source_path=str(snapshot_dir / "candidate_documents.json"),
            )
            (snapshot_dir / "identity.json").write_text(
                json.dumps(
                    {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                        "linkedin_slug": "acme",
                        "aliases": ["acme ai"],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "snapshot_id": snapshot_id,
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                            "linkedin_slug": "acme",
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "candidate_documents.json").write_text(
                json.dumps(
                    {
                        "snapshot": {
                            "source_snapshots": [
                                {
                                    "snapshot_id": snapshot_id,
                                    "candidate_count": 1,
                                    "evidence_count": 0,
                                    "source_path": str(snapshot_dir / "candidate_documents.json"),
                                }
                            ],
                            "source_snapshot_selection": {
                                "mode": "candidate_documents_fallback",
                                "selected_snapshot_ids": [snapshot_id],
                                "reason": f"seed_{snapshot_id}",
                            },
                        },
                        "candidates": [candidate.to_record()],
                        "evidence": [],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        result = load_company_snapshot_registry_summary(
            runtime_dir=self.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id=current_snapshot_id,
        )

        self.assertEqual(result["summary"]["candidate_count"], 1)
        self.assertEqual(result["summary"]["source_snapshot_selection"]["mode"], "all_history_snapshots")
        self.assertEqual(
            set(result["summary"]["selected_snapshot_ids"]),
            {previous_snapshot_id, current_snapshot_id},
        )
        self.assertEqual(result["summary"]["source_snapshot_count"], 2)
