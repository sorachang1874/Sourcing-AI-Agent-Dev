import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from sourcing_agent.candidate_artifacts import build_company_candidate_artifacts
from sourcing_agent.domain import Candidate
from sourcing_agent.runtime_rebuild import (
    rebuild_runtime_company_asset_control_plane,
    rebuild_runtime_jobs_control_plane,
)
from sourcing_agent.storage import ControlPlaneStore


class RuntimeRebuildTest(unittest.TestCase):
    def setUp(self) -> None:
        self.env_patcher = mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "",
            },
            clear=False,
        )
        self.env_patcher.start()
        self.tempdir = tempfile.TemporaryDirectory()
        self.project_root = Path(self.tempdir.name)
        self.runtime_dir = self.project_root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.snapshot_id = "20260406T120000"
        self.snapshot_dir = self.runtime_dir / "company_assets" / "acme" / self.snapshot_id
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "company_assets" / "acme" / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": self.snapshot_id,
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
            candidate_id="cand-1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example",
            source_dataset="acme_roster_snapshot",
        )
        (self.snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "company_identity": {
                            "requested_name": "Acme",
                            "canonical_name": "Acme",
                            "company_key": "acme",
                        }
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()
        self.env_patcher.stop()

    def _write_legacy_search_seed_summary(self) -> None:
        discovery_dir = self.snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        current_filter_hints = {
            "current_companies": ["https://www.linkedin.com/company/acme/"],
            "function_ids": ["24", "8"],
            "keywords": ["Math"],
        }
        former_filter_hints = {
            "past_companies": ["https://www.linkedin.com/company/acme/"],
            "function_ids": ["24", "8"],
            "keywords": ["Math"],
        }
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": self.snapshot_id,
                    "target_company": "Acme",
                    "company_identity": {
                        "requested_name": "Acme",
                        "canonical_name": "Acme",
                        "company_key": "acme",
                    },
                    "query_summaries": [
                        {
                            "query": "Math",
                            "effective_query_text": "Math",
                            "mode": "harvest_profile_search",
                            "seed_entry_count": 1,
                            "employment_scope": "current",
                            "employment_status": "current",
                            "strategy_type": "scoped_search_roster",
                            "filter_hints": current_filter_hints,
                        },
                        {
                            "query": "Math",
                            "effective_query_text": "Math",
                            "mode": "harvest_profile_search",
                            "seed_entry_count": 1,
                            "employment_scope": "former",
                            "employment_status": "former",
                            "strategy_type": "former_employee_search",
                            "filter_hints": former_filter_hints,
                        },
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (discovery_dir / "entries.json").write_text(
            json.dumps(
                [
                    {
                        "seed_key": "current_1",
                        "full_name": "Alice Example",
                        "source_type": "harvest_profile_search",
                        "employment_status": "current",
                        "profile_url": "https://www.linkedin.com/in/alice-example/",
                    },
                    {
                        "seed_key": "former_1",
                        "full_name": "Bob Example",
                        "source_type": "harvest_profile_search",
                        "employment_status": "former",
                        "profile_url": "https://www.linkedin.com/in/bob-example/",
                    },
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def test_rebuild_runtime_company_assets_registers_generation_without_eager_artifact_repair(self) -> None:
        rebuild_store = ControlPlaneStore(self.runtime_dir / "rebuild.db")

        result = rebuild_runtime_company_asset_control_plane(
            runtime_dir=self.runtime_dir,
            store=rebuild_store,
            rebuild_missing_artifacts=True,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(int(result["repair_result"].get("repair_candidate_count") or 0), 0)
        self.assertEqual(int(result["repair_result"].get("repaired_snapshot_count") or 0), 0)
        generation = rebuild_store.get_asset_materialization_generation(
            target_company="Acme",
            snapshot_id=self.snapshot_id,
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
        )
        self.assertIsNotNone(generation)

    def test_rebuild_runtime_company_assets_registers_generation_for_legacy_summary_snapshot(self) -> None:
        seed_store = ControlPlaneStore(self.runtime_dir / "seed.db")
        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=seed_store,
            target_company="Acme",
            snapshot_id=self.snapshot_id,
        )
        artifact_dir = self.snapshot_dir / "normalized_artifacts"
        (artifact_dir / "manifest.json").unlink()
        (artifact_dir / "snapshot_manifest.json").unlink()

        rebuild_store = ControlPlaneStore(self.runtime_dir / "rebuild.db")
        result = rebuild_runtime_company_asset_control_plane(
            runtime_dir=self.runtime_dir,
            store=rebuild_store,
            rebuild_missing_artifacts=False,
        )

        self.assertEqual(result["status"], "completed")
        self.assertGreaterEqual(int(result["registered_generation_count"] or 0), 1)
        generation = rebuild_store.get_asset_materialization_generation(
            target_company="Acme",
            snapshot_id=self.snapshot_id,
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
        )
        self.assertIsNotNone(generation)
        membership_summary = rebuild_store.summarize_asset_membership_index(
            generation_key=str(generation.get("generation_key") or "")
        )
        self.assertEqual(int(membership_summary.get("member_count") or 0), 1)
        authoritative = rebuild_store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        authoritative_generation_key = str(authoritative.get("materialization_generation_key") or "")
        self.assertTrue(authoritative_generation_key)
        authoritative_membership_summary = rebuild_store.summarize_asset_membership_index(
            generation_key=authoritative_generation_key
        )
        self.assertGreaterEqual(int(authoritative_membership_summary.get("member_count") or 0), 1)

    def test_rebuild_runtime_company_assets_backfills_legacy_search_seed_lanes_and_registry(self) -> None:
        self._write_legacy_search_seed_summary()
        rebuild_store = ControlPlaneStore(self.runtime_dir / "rebuild.db")

        result = rebuild_runtime_company_asset_control_plane(
            runtime_dir=self.runtime_dir,
            store=rebuild_store,
            rebuild_missing_artifacts=False,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(int(result.get("search_seed_lane_backfill_count") or 0), 1)
        self.assertEqual(int(result.get("acquisition_shard_registry_sync_count") or 0), 1)
        self.assertTrue((self.snapshot_dir / "search_seed_discovery" / "current" / "summary.json").exists())
        self.assertTrue((self.snapshot_dir / "search_seed_discovery" / "former" / "summary.json").exists())
        shard_rows = rebuild_store.list_acquisition_shard_registry(
            target_company="Acme",
            snapshot_ids=[self.snapshot_id],
            limit=20,
        )
        profile_search_rows = [
            row
            for row in shard_rows
            if str(row.get("lane") or "").strip() == "profile_search"
            and str(row.get("search_query") or "").strip() == "Math"
        ]
        self.assertEqual(len(profile_search_rows), 2)
        self.assertEqual(
            {str(row.get("employment_scope") or "").strip() for row in profile_search_rows},
            {"current", "former"},
        )

    def test_rebuild_runtime_jobs_restores_jobs_and_result_views_with_local_source_path(self) -> None:
        seed_store = ControlPlaneStore(self.runtime_dir / "seed.db")
        build_company_candidate_artifacts(
            runtime_dir=self.runtime_dir,
            store=seed_store,
            target_company="Acme",
            snapshot_id=self.snapshot_id,
        )
        rebuild_store = ControlPlaneStore(self.runtime_dir / "rebuild.db")
        rebuild_runtime_company_asset_control_plane(
            runtime_dir=self.runtime_dir,
            store=rebuild_store,
            rebuild_missing_artifacts=False,
        )
        (self.runtime_dir / "jobs").mkdir(parents=True, exist_ok=True)
        (self.runtime_dir / "jobs" / "job-1.json").write_text(
            json.dumps(
                {
                    "job_id": "job-1",
                    "status": "completed",
                    "request": {
                        "raw_user_request": "给我 Acme 做研究的人",
                        "query": "Acme research",
                        "target_company": "Acme",
                        "target_scope": "full_company_asset",
                        "asset_view": "canonical_merged",
                        "retrieval_strategy": "hybrid",
                    },
                    "plan": {"target_company": "Acme"},
                    "summary": {
                        "text": "Found 1 match for Acme.",
                        "returned_matches": 1,
                        "total_matches": 1,
                        "analysis_stage": "stage_2_final",
                        "summary_provider": "deterministic",
                        "candidate_source": {
                            "source_kind": "company_snapshot",
                            "snapshot_id": self.snapshot_id,
                            "asset_view": "canonical_merged",
                            "source_path": "/home/old/runtime/company_assets/acme/legacy.json",
                            "candidate_count": 1,
                        },
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        result = rebuild_runtime_jobs_control_plane(
            runtime_dir=self.runtime_dir,
            store=rebuild_store,
        )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["restored_job_count"], 1)
        self.assertEqual(result["restored_result_view_count"], 1)
        job = rebuild_store.get_job("job-1")
        self.assertIsNotNone(job)
        self.assertEqual(str(job.get("job_type") or ""), "retrieval")
        self.assertEqual(str(job.get("stage") or ""), "completed")
        result_view = rebuild_store.get_job_result_view(job_id="job-1")
        authoritative = rebuild_store.get_authoritative_organization_asset_registry(
            target_company="Acme",
            asset_view="canonical_merged",
        )
        self.assertIsNotNone(result_view)
        self.assertEqual(str(result_view.get("view_kind") or ""), "asset_population")
        self.assertEqual(
            str(result_view.get("materialization_generation_key") or ""),
            str(authoritative.get("materialization_generation_key") or ""),
        )
        resolved_source_path = str(result_view.get("source_path") or "")
        self.assertNotIn("/home/old/", resolved_source_path)
        self.assertIn(self.snapshot_id, resolved_source_path)
        self.assertIn("normalized_artifacts", resolved_source_path)

    def test_company_filter_does_not_match_humansand_via_requested_name_normalization(self) -> None:
        humans_snapshot = self.runtime_dir / "company_assets" / "humans" / "20260408T200003"
        humans_snapshot.mkdir(parents=True, exist_ok=True)
        (humans_snapshot / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Humans",
                    "canonical_name": "Humans",
                    "company_key": "humans",
                    "aliases": ["humans"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (humans_snapshot / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "company_identity": {
                            "requested_name": "Humans",
                            "canonical_name": "Humans",
                            "company_key": "humans",
                        }
                    },
                    "candidates": [],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        humansand_snapshot = self.runtime_dir / "company_assets" / "humansand" / "20260408T154820"
        humansand_snapshot.mkdir(parents=True, exist_ok=True)
        (humansand_snapshot / "identity.json").write_text(
            json.dumps(
                {
                    "requested_name": "Humans&",
                    "canonical_name": "humans&",
                    "company_key": "humansand",
                    "linkedin_slug": "humansand",
                    "aliases": ["humansand"],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (humansand_snapshot / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "company_identity": {
                            "requested_name": "Humans&",
                            "canonical_name": "humans&",
                            "company_key": "humansand",
                        }
                    },
                    "candidates": [],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        rebuild_store = ControlPlaneStore(self.runtime_dir / "rebuild.db")
        result = rebuild_runtime_company_asset_control_plane(
            runtime_dir=self.runtime_dir,
            store=rebuild_store,
            companies=["humans"],
            rebuild_missing_artifacts=False,
        )

        processed_snapshots = {
            (str(item.get("company_key") or ""), str(item.get("snapshot_id") or ""))
            for item in list(result.get("companies") or [])
        }
        self.assertIn(("humans", "20260408T200003"), processed_snapshots)
        self.assertNotIn(("humansand", "20260408T154820"), processed_snapshots)


if __name__ == "__main__":
    unittest.main()
