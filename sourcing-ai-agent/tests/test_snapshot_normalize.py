"""Snapshot normalization behavior — ported from the frozen test_pipeline.py god-file.

WS3 Tier 3 salvage wave 3 (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md;
R-009 disposition = salvage-then-delete): normalize-snapshot contract coverage
(historical explicit-profile-capture inheritance, large-baseline reuse for
sparse refresh, force-fresh skip of inheritance, hot-cache mirror + retrieval
index refresh, immediate candidate-artifact materialization) re-homed onto the
repo-standard PG fixture. Old->new port mapping in
docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks in the same change.
"""

import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import AcquisitionTask, Candidate, EvidenceRecord, JobRequest, make_evidence_id
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class SnapshotNormalizeTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.catalog = AssetCatalog.discover()
        self.store = self.make_pg_store(f"{self.tempdir.name}/test.db")
        self.settings = AppSettings(
            project_root=Path(self.tempdir.name),
            runtime_dir=Path(self.tempdir.name),
            secrets_file=Path(self.tempdir.name) / "providers.local.json",
            jobs_dir=Path(self.tempdir.name) / "jobs",
            company_assets_dir=Path(self.tempdir.name) / "company_assets",
            db_path=Path(self.tempdir.name) / "test.db",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        self.model_client = DeterministicModelClient()
        self.semantic_provider = LocalSemanticProvider()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=self.semantic_provider,
            acquisition_engine=self.acquisition_engine,
        )
        env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        env_patcher.start()
        self.addCleanup(env_patcher.stop)


    def test_normalize_snapshot_inherits_historical_explicit_profile_captures(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T120000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T120000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            focus_areas="Research Engineer | distributed systems",
            education="MIT",
            work_history="Acme | Example Labs",
            notes="Historical enriched snapshot.",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            source_dataset="acme_linkedin_company_people",
            source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
            metadata={
                "public_identifier": "alice-example",
                "profile_url": "https://www.linkedin.com/in/alice-example/",
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
                "membership_review_rationale": "Profile content looks implausible for the target company.",
                "membership_review_triggers": ["suspicious_profile_content"],
                "membership_review_trigger_keywords": ["healer"],
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "linkedin_profile_detail",
                    "Research Engineer",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="linkedin_profile_detail",
                title="Research Engineer",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Historical profile detail capture.",
                source_dataset="linkedin_profile_detail",
                source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
            ),
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "linkedin_profile_membership_review",
                    "Membership review",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="linkedin_profile_membership_review",
                title="Membership review",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Historical suspicious membership review.",
                source_dataset="linkedin_profile_membership_review",
                source_path=str(old_snapshot_dir / "harvest_profiles" / "alice-example.json"),
                metadata={"decision": "suspicious_member"},
            ),
        ]
        historical_lead = Candidate(
            candidate_id="cand_acme_lead",
            name_en="Historical Lead",
            display_name="Historical Lead",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Research lead from publication",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "publications" / "lead.json"),
        )
        historical_lead_evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                historical_lead.candidate_id,
                "publication_match",
                "Great Paper",
                "https://example.com/paper",
            ),
            candidate_id=historical_lead.candidate_id,
            source_type="publication_match",
            title="Great Paper",
            url="https://example.com/paper",
            summary="Historical lead that should not be dropped by normalize.",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "publications" / "lead.json"),
        )
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record(), historical_lead.to_record()],
                    "evidence": [item.to_record() for item in [*old_evidence, historical_lead_evidence]],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Member of Technical Staff at Acme",
            focus_areas="Member of Technical Staff at Acme",
            notes="LinkedIn company roster baseline. Location: San Francisco.",
            linkedin_url="https://www.linkedin.com/in/ACwAAExampleBaseline",
            source_dataset="acme_linkedin_company_people",
            source_path=str(current_snapshot_dir / "harvest_company_employees" / "visible.json"),
            metadata={
                "profile_url": "https://www.linkedin.com/in/ACwAAExampleBaseline",
                "snapshot_id": current_snapshot_dir.name,
            },
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "acme_linkedin_company_people",
                    "Roster row",
                    "https://www.linkedin.com/company/acme/",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="linkedin_company_people",
                title="Roster row",
                url="https://www.linkedin.com/company/acme/",
                summary="Current roster baseline.",
                source_dataset="acme_linkedin_company_people",
                source_path=str(current_snapshot_dir / "harvest_company_employees" / "visible.json"),
            )
        ]
        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": current_snapshot_dir.name},
                    "candidates": [baseline_candidate.to_record()],
                    "evidence": [item.to_record() for item in baseline_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [baseline_candidate],
                "evidence": baseline_evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("cand_acme_1")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.education, "MIT")
        self.assertEqual(stored_candidate.work_history, "Acme | Example Labs")
        self.assertTrue(stored_candidate.metadata.get("membership_review_required"))
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "suspicious_member")
        self.assertIsNotNone(self.store.get_candidate("cand_acme_lead"))

        stored_evidence = self.store.list_evidence("cand_acme_1")
        stored_source_types = {item["source_type"] for item in stored_evidence}
        self.assertIn("linkedin_company_people", stored_source_types)
        self.assertIn("linkedin_profile_detail", stored_source_types)
        self.assertIn("linkedin_profile_membership_review", stored_source_types)
        lead_evidence = self.store.list_evidence("cand_acme_lead")
        self.assertEqual(lead_evidence[0]["source_type"], "publication_match")

        rewritten_payload = json.loads((current_snapshot_dir / "candidate_documents.json").read_text())
        rewritten_candidate = rewritten_payload["candidates"][0]
        rewritten_source_types = {item["source_type"] for item in rewritten_payload["evidence"]}
        self.assertEqual(rewritten_candidate["education"], "MIT")
        self.assertEqual(rewritten_candidate["work_history"], "Acme | Example Labs")
        self.assertTrue(rewritten_candidate["metadata"]["membership_review_required"])
        self.assertIn("linkedin_profile_detail", rewritten_source_types)
        self.assertEqual(rewritten_payload["historical_profile_inheritance"]["matched_candidate_count"], 1)
        self.assertGreaterEqual(rewritten_payload["historical_profile_inheritance"]["inherited_evidence_count"], 2)
        self.assertEqual(rewritten_payload["historical_profile_inheritance"]["carried_forward_candidate_count"], 1)
        rewritten_candidate_ids = {item["candidate_id"] for item in rewritten_payload["candidates"]}
        self.assertIn("cand_acme_lead", rewritten_candidate_ids)


    def test_normalize_snapshot_reuses_large_historical_baseline_for_sparse_full_company_refresh(self) -> None:
        company_dir = self.settings.company_assets_dir / "bigco"
        old_snapshot_dir = company_dir / "20260406T120000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T120000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        historical_candidates: list[Candidate] = []
        for index in range(1001):
            historical_candidates.append(
                Candidate(
                    candidate_id=f"cand_bigco_{index:04d}",
                    name_en=f"Person {index:04d}",
                    display_name=f"Person {index:04d}",
                    category="employee",
                    target_company="BigCo",
                    organization="BigCo",
                    employment_status="current",
                    role="Software Engineer",
                    linkedin_url=f"https://www.linkedin.com/in/bigco-{index:04d}/",
                    source_dataset="bigco_linkedin_company_people",
                    source_path=str(old_snapshot_dir / "harvest_company_employees" / f"{index:04d}.json"),
                )
            )

        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [item.to_record() for item in historical_candidates],
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        current_seed = Candidate(
            candidate_id="seed_bigco_0001",
            name_en="Person 0001",
            display_name="Person 0001",
            category="employee",
            target_company="BigCo",
            organization="BigCo",
            employment_status="current",
            role="Software Engineer",
            linkedin_url="https://www.linkedin.com/in/bigco-0001/",
            source_dataset="bigco_scoped_search_seed",
            source_path=str(current_snapshot_dir / "search_seed_discovery" / "seed.json"),
        )
        current_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    current_seed.candidate_id,
                    "bigco_scoped_search_seed",
                    "Scoped seed",
                    current_seed.linkedin_url,
                ),
                candidate_id=current_seed.candidate_id,
                source_type="profile_search_seed",
                title="Scoped seed",
                url=current_seed.linkedin_url,
                summary="Sparse scoped seed refresh.",
                source_dataset="bigco_scoped_search_seed",
                source_path=str(current_snapshot_dir / "search_seed_discovery" / "seed.json"),
            )
        ]
        (current_snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": current_snapshot_dir.name},
                    "candidates": [current_seed.to_record()],
                    "evidence": [item.to_record() for item in current_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        identity = CompanyIdentity(
            requested_name="BigCo",
            canonical_name="BigCo",
            company_key="bigco",
            linkedin_slug="bigco",
            linkedin_company_url="https://www.linkedin.com/company/bigco/",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )

        result = self.acquisition_engine._normalize_snapshot(
            task,
            {
                "company_identity": identity,
                "snapshot_id": current_snapshot_dir.name,
                "snapshot_dir": current_snapshot_dir,
                "candidates": [current_seed],
                "evidence": current_evidence,
            },
            JobRequest(
                raw_user_request="帮我找 BigCo 做 infra 的人",
                target_company="BigCo",
                target_scope="full_company_asset",
                categories=["employee"],
            ),
        )

        self.assertEqual(result.status, "completed")
        rewritten_payload = json.loads((current_snapshot_dir / "candidate_documents.json").read_text())
        self.assertTrue(rewritten_payload["historical_profile_inheritance"]["baseline_snapshot_reused"])
        self.assertEqual(
            rewritten_payload["historical_profile_inheritance"]["baseline_snapshot_id"],
            old_snapshot_dir.name,
        )
        self.assertEqual(rewritten_payload["candidate_count"], 1001)
        self.assertEqual(self.store.candidate_count_for_company("BigCo"), 1001)


    def test_normalize_snapshot_force_fresh_run_skips_historical_profile_inheritance_at_execution_time(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / "snapshot-normalize-fresh-runtime"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Anthropic",
            canonical_name="Anthropic",
            company_key="anthropic",
            linkedin_slug="anthropic",
            linkedin_company_url="https://www.linkedin.com/company/anthropicresearch/",
        )
        candidate = Candidate(
            candidate_id="anthropic_1",
            name_en="Ada Example",
            display_name="Ada Example",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
        )
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
            metadata={"cost_policy": {"allow_historical_profile_inheritance": True}},
        )
        with (
            unittest.mock.patch(
                "sourcing_agent.acquisition._inherit_historical_profile_captures",
                side_effect=AssertionError("force_fresh_run should skip historical profile inheritance"),
            ),
            unittest.mock.patch(
                "sourcing_agent.acquisition.canonicalize_company_records",
                return_value=([candidate], [], {"merged_candidate_count": 0}),
            ),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [],
                },
                JobRequest(
                    raw_user_request="重新跑 Anthropic normalize",
                    target_company="Anthropic",
                    categories=["employee"],
                    execution_preferences={"force_fresh_run": True},
                ),
            )

        self.assertEqual(result.status, "completed")
        stored_candidate = self.store.get_candidate("anthropic_1")
        self.assertIsNotNone(stored_candidate)


    def test_normalize_snapshot_mirrors_hot_cache_and_refreshes_retrieval_index(self) -> None:
        canonical_root = Path(self.tempdir.name) / "canonical_company_assets"
        hot_cache_root = Path(self.tempdir.name) / "hot_cache_company_assets"
        snapshot_id = "snapshot-hot-cache"
        snapshot_dir = canonical_root / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_hot_cache_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
        )
        normalize_task = AcquisitionTask(
            task_id="normalize_hot_cache",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        retrieval_task = AcquisitionTask(
            task_id="retrieval_hot_cache",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Build retrieval index",
        )

        with unittest.mock.patch.dict(
            os.environ,
            {
                "SOURCING_CANONICAL_ASSETS_DIR": str(canonical_root),
                "SOURCING_HOT_CACHE_ASSETS_DIR": str(hot_cache_root),
            },
            clear=False,
        ):
            engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
            engine._write_latest_snapshot_pointer(identity, snapshot_id, snapshot_dir)
            with unittest.mock.patch(
                "sourcing_agent.acquisition.build_company_candidate_artifacts",
                return_value={"artifact_dir": "", "artifact_paths": {}, "sync_status": {}},
            ):
                normalize_result = engine._normalize_snapshot(
                    normalize_task,
                    {
                        "company_identity": identity,
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )

            self.assertEqual(normalize_result.status, "completed")
            self.assertEqual(normalize_result.payload["hot_cache_sync"]["status"], "completed")
            hot_cache_snapshot_dir = hot_cache_root / "acme" / snapshot_id
            self.assertTrue((hot_cache_root / "acme" / "latest_snapshot.json").exists())
            self.assertTrue(hot_cache_snapshot_dir.exists())
            self.assertTrue((hot_cache_snapshot_dir / "candidate_documents.json").exists())

            stale_marker = hot_cache_snapshot_dir / "stale.json"
            stale_marker.write_text("stale", encoding="utf-8")
            retrieval_result = engine._build_retrieval_index(
                retrieval_task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
            )

        self.assertEqual(retrieval_result.status, "completed")
        self.assertEqual(retrieval_result.payload["hot_cache_sync"]["status"], "completed")
        self.assertTrue((snapshot_dir / "retrieval_index_summary.json").exists())
        self.assertTrue((hot_cache_root / "acme" / snapshot_id / "retrieval_index_summary.json").exists())
        self.assertFalse((hot_cache_root / "acme" / snapshot_id / "stale.json").exists())


    def test_normalize_snapshot_materializes_candidate_artifacts_immediately(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-normalize-immediate-materialize"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_1",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
        )
        artifact_build_result = {
            "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
            "artifact_paths": {
                "materialized_candidate_documents": str(
                    snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
                ),
                "artifact_summary": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
            },
            "sync_status": {
                "overall_status": "completed",
                "organization_asset_registry_refresh": {"status": "completed"},
            },
        }
        task = AcquisitionTask(
            task_id="normalize",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        request = JobRequest(
            raw_user_request="帮我找 Acme 的人",
            target_company="Acme",
            execution_preferences={"delta_baseline_snapshot_ids": ["baseline-1", "baseline-2"]},
        )

        with unittest.mock.patch(
            "sourcing_agent.acquisition.build_company_candidate_artifacts",
            return_value=artifact_build_result,
        ) as build_artifacts:
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": snapshot_dir.name,
                    "snapshot_dir": snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [],
                },
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.payload["artifact_materialization_status"], "completed")
        self.assertEqual(result.payload["artifact_dir"], artifact_build_result["artifact_dir"])
        self.assertEqual(result.payload["artifact_paths"], artifact_build_result["artifact_paths"])
        self.assertEqual(result.payload["sync_status"], artifact_build_result["sync_status"])
        # Contract evolution adopted at port time (2026-07-22): the builder
        # now also receives snapshot_dir + the resolved company_identity
        # record (identity threading); identity content is owned by the
        # identity-resolution suites.
        build_artifacts.assert_called_once_with(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id=snapshot_dir.name,
            snapshot_dir=snapshot_dir,
            company_identity=unittest.mock.ANY,
            preferred_source_snapshot_ids=["baseline-1", "baseline-2"],
            build_profile="full",
        )



if __name__ == "__main__":
    unittest.main()
