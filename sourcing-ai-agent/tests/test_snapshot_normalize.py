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
        # WS7/W7.3 S3 wiring (2026-07-25): plus the engine's own model client, a
        # pure pass-through that only makes the record-only promote-judgment
        # SHADOW hook reachable — it changes no artifact, no ladder decision and
        # no authoritative row (the shadow suite owns that regression).
        build_artifacts.assert_called_once_with(
            runtime_dir=self.settings.runtime_dir,
            store=self.store,
            target_company="Acme",
            snapshot_id=snapshot_dir.name,
            snapshot_dir=snapshot_dir,
            company_identity=unittest.mock.ANY,
            preferred_source_snapshot_ids=["baseline-1", "baseline-2"],
            build_profile="full",
            model_client=self.model_client,
        )


    def _write_snapshot_normalized_artifacts(
        self,
        *,
        snapshot_dir: Path,
        target_company: str,
        include_strict: bool = True,
        include_serving_docs: bool = False,
    ) -> None:
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "snapshot_id": snapshot_dir.name,
            "target_company": target_company,
            "candidate_count": 1,
        }
        (normalized_dir / "manifest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        if include_serving_docs:
            serving_payload = {
                "target_company": target_company,
                "snapshot_id": snapshot_dir.name,
                "candidates": [],
                "evidence": [],
                "candidate_count": 0,
                "evidence_count": 0,
            }
            (normalized_dir / "materialized_candidate_documents.json").write_text(
                json.dumps(serving_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (normalized_dir / "reusable_candidate_documents.json").write_text(
                json.dumps(serving_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        if include_strict:
            strict_dir = normalized_dir / "strict_roster_only"
            strict_dir.mkdir(parents=True, exist_ok=True)
            (strict_dir / "manifest.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            (strict_dir / "artifact_summary.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )


    def test_normalize_snapshot_reuses_equivalent_delta_baseline_snapshot(self) -> None:
        baseline_snapshot_id = "snapshot-baseline-equivalent"
        current_snapshot_id = "snapshot-current-equivalent"
        baseline_snapshot_dir = self.settings.company_assets_dir / "xai" / baseline_snapshot_id
        baseline_snapshot_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir = self.settings.company_assets_dir / "xai" / current_snapshot_id
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        candidate = Candidate(
            candidate_id="xai_equivalent_1",
            name_en="Equivalent Candidate",
            display_name="Equivalent Candidate",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/equivalent-candidate/",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "xai_candidates",
                "Equivalent profile",
                candidate.linkedin_url,
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile",
            title="Equivalent profile",
            url=candidate.linkedin_url,
            source_dataset="xai_candidates",
            source_path=str(baseline_snapshot_dir / "candidate_documents.json"),
            summary="Equivalent baseline evidence.",
        )
        for snapshot_dir in (baseline_snapshot_dir, current_snapshot_dir):
            (snapshot_dir / "candidate_documents.json").write_text(
                json.dumps(
                    {
                        "snapshot": {"company_identity": identity.to_record()},
                        "candidates": [candidate.to_record()],
                        "evidence": [evidence.to_record()],
                        "candidate_count": 1,
                        "evidence_count": 1,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "retrieval_index_summary.json").write_text(
                json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=baseline_snapshot_dir,
            target_company="xAI",
            include_strict=True,
        )
        task = AcquisitionTask(
            task_id="normalize-equivalent-baseline",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        request = JobRequest(
            raw_user_request="给我 xAI 的所有成员",
            target_company="xAI",
            execution_preferences={"delta_baseline_snapshot_ids": [baseline_snapshot_id]},
        )

        with unittest.mock.patch(
            "sourcing_agent.acquisition.build_company_candidate_artifacts",
            side_effect=AssertionError("equivalent baseline should bypass artifact rebuild"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition._inherit_historical_profile_captures",
            side_effect=AssertionError("equivalent baseline should bypass historical inheritance"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition.canonicalize_company_records",
            side_effect=AssertionError("equivalent baseline should bypass canonicalization"),
        ), unittest.mock.patch.object(
            self.store,
            "replace_company_data",
            side_effect=AssertionError("equivalent baseline should bypass SQLite replace"),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": current_snapshot_id,
                    "snapshot_dir": current_snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.payload["artifact_materialization_status"], "reused_equivalent_snapshot")
        self.assertEqual(result.payload["reused_equivalent_snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_dir"], baseline_snapshot_dir)
        self.assertEqual(result.state_updates["candidate_doc_path"], baseline_snapshot_dir / "candidate_documents.json")
        self.assertEqual(result.state_updates["manifest_path"], baseline_snapshot_dir / "manifest.json")


    def test_normalize_snapshot_reuses_legacy_serving_baseline_without_strict_exports(self) -> None:
        baseline_snapshot_id = "snapshot-baseline-legacy-serving"
        current_snapshot_id = "snapshot-current-legacy-serving"
        baseline_snapshot_dir = self.settings.company_assets_dir / "xai" / baseline_snapshot_id
        baseline_snapshot_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir = self.settings.company_assets_dir / "xai" / current_snapshot_id
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="xAI",
            canonical_name="xAI",
            company_key="xai",
            linkedin_slug="xai",
            linkedin_company_url="https://www.linkedin.com/company/xai/",
        )
        candidate = Candidate(
            candidate_id="xai_equivalent_legacy_1",
            name_en="Equivalent Legacy Candidate",
            display_name="Equivalent Legacy Candidate",
            category="employee",
            target_company="xAI",
            organization="xAI",
            employment_status="current",
            role="Engineer",
            linkedin_url="https://www.linkedin.com/in/equivalent-legacy-candidate/",
        )
        evidence = EvidenceRecord(
            evidence_id=make_evidence_id(
                candidate.candidate_id,
                "xai_candidates",
                "Equivalent legacy profile",
                candidate.linkedin_url,
            ),
            candidate_id=candidate.candidate_id,
            source_type="linkedin_profile",
            title="Equivalent legacy profile",
            url=candidate.linkedin_url,
            source_dataset="xai_candidates",
            source_path=str(baseline_snapshot_dir / "candidate_documents.json"),
            summary="Equivalent baseline evidence.",
        )
        for snapshot_dir in (baseline_snapshot_dir, current_snapshot_dir):
            (snapshot_dir / "candidate_documents.json").write_text(
                json.dumps(
                    {
                        "snapshot": {"company_identity": identity.to_record()},
                        "candidates": [candidate.to_record()],
                        "evidence": [evidence.to_record()],
                        "candidate_count": 1,
                        "evidence_count": 1,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "manifest.json").write_text(
                json.dumps(
                    {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            (snapshot_dir / "retrieval_index_summary.json").write_text(
                json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        normalized_dir = baseline_snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        (normalized_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": baseline_snapshot_id,
                    "asset_view": "canonical_merged",
                    "candidate_shards": [],
                    "pages": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        task = AcquisitionTask(
            task_id="normalize-equivalent-legacy-serving-baseline",
            task_type="normalize_asset_snapshot",
            title="Normalize",
            description="Persist snapshot",
        )
        request = JobRequest(
            raw_user_request="给我 xAI 的所有成员",
            target_company="xAI",
            execution_preferences={"delta_baseline_snapshot_ids": [baseline_snapshot_id]},
        )

        with unittest.mock.patch(
            "sourcing_agent.acquisition.build_company_candidate_artifacts",
            side_effect=AssertionError("legacy serving baseline should bypass artifact rebuild"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition._inherit_historical_profile_captures",
            side_effect=AssertionError("legacy serving baseline should bypass historical inheritance"),
        ), unittest.mock.patch(
            "sourcing_agent.acquisition.canonicalize_company_records",
            side_effect=AssertionError("legacy serving baseline should bypass canonicalization"),
        ), unittest.mock.patch.object(
            self.store,
            "replace_company_data",
            side_effect=AssertionError("legacy serving baseline should bypass SQLite replace"),
        ):
            result = self.acquisition_engine._normalize_snapshot(
                task,
                {
                    "company_identity": identity,
                    "snapshot_id": current_snapshot_id,
                    "snapshot_dir": current_snapshot_dir,
                    "candidates": [candidate],
                    "evidence": [evidence],
                },
                request,
            )

        self.assertEqual(result.status, "completed")
        self.assertEqual(result.payload["artifact_materialization_status"], "reused_equivalent_snapshot")
        self.assertEqual(result.payload["reused_equivalent_snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_id"], baseline_snapshot_id)
        self.assertEqual(result.state_updates["snapshot_dir"], baseline_snapshot_dir)


    def test_normalize_snapshot_canonicalizes_same_name_current_candidates(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "20260407T130000"
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        lead_candidate = Candidate(
            candidate_id="lead_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
            source_path=str(snapshot_dir / "publications" / "alice.html"),
        )
        employee_candidate = Candidate(
            candidate_id="employee_alice",
            name_en="Alice Example",
            display_name="Alice Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-example/",
            work_history="Acme",
            education="MIT",
            source_dataset="acme_linkedin_company_people",
            source_path=str(snapshot_dir / "harvest_profiles" / "alice-example.json"),
            metadata={"public_identifier": "alice-example"},
        )
        evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id("lead_alice", "publication_match", "Paper", "https://example.com/paper"),
                candidate_id="lead_alice",
                source_type="publication_match",
                title="Paper",
                url="https://example.com/paper",
                summary="Lead from publication.",
                source_dataset="publication_match",
                source_path=str(snapshot_dir / "publications" / "alice.html"),
            ),
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    "employee_alice",
                    "linkedin_profile_detail",
                    "Research Engineer",
                    "https://www.linkedin.com/in/alice-example/",
                ),
                candidate_id="employee_alice",
                source_type="linkedin_profile_detail",
                title="Research Engineer",
                url="https://www.linkedin.com/in/alice-example/",
                summary="Profile detail for Alice.",
                source_dataset="linkedin_profile_detail",
                source_path=str(snapshot_dir / "harvest_profiles" / "alice-example.json"),
            ),
        ]

        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        result = self.acquisition_engine._normalize_snapshot(
            AcquisitionTask(
                task_id="normalize_canonical",
                task_type="normalize_asset_snapshot",
                title="Normalize canonical",
                description="Persist snapshot with canonical dedupe.",
            ),
            {
                "company_identity": identity,
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidates": [lead_candidate, employee_candidate],
                "evidence": evidence,
            },
        )

        self.assertEqual(result.status, "completed")
        stored = self.store.list_candidates_for_company("Acme")
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0].candidate_id, "employee_alice")
        self.assertEqual(stored[0].category, "employee")
        self.assertEqual(stored[0].linkedin_url, "https://www.linkedin.com/in/alice-example/")
        stored_evidence = self.store.list_evidence("employee_alice")
        stored_source_types = {item["source_type"] for item in stored_evidence}
        self.assertIn("publication_match", stored_source_types)
        self.assertIn("linkedin_profile_detail", stored_source_types)
        payload = json.loads((snapshot_dir / "candidate_documents.json").read_text())
        self.assertEqual(payload["canonicalization"]["canonical_candidate_count"], 1)
        self.assertEqual(payload["canonicalization"]["name_merge_count"], 1)


    def test_normalize_snapshot_inherits_manual_review_confirmed_membership(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T130000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T140000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_manual_member",
            name_en="Jeremy Example",
            display_name="Jeremy Example",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Publication author lead",
            media_url="https://jeremy.example.com/",
            source_dataset="publication_match",
            source_path=str(old_snapshot_dir / "manual_review_assets" / "jeremy" / "resolution.json"),
            metadata={
                "manual_review_artifact_root": str(old_snapshot_dir / "manual_review_assets" / "jeremy"),
                "manual_review_links": [{"label": "Homepage", "url": "https://jeremy.example.com/"}],
                "membership_review_required": False,
                "membership_review_decision": "manual_confirmed_member",
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "manual_review",
                    "Homepage",
                    "https://jeremy.example.com/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="manual_review_link",
                title="Homepage",
                url="https://jeremy.example.com/",
                summary="Manual review confirmed current membership.",
                source_dataset="manual_review",
                source_path=str(old_snapshot_dir / "manual_review_assets" / "jeremy" / "source_01.json"),
            )
        ]
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record()],
                    "evidence": [item.to_record() for item in old_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_manual_member",
            name_en="Jeremy Example",
            display_name="Jeremy Example",
            category="lead",
            target_company="Acme",
            organization="Acme",
            employment_status="",
            role="Publication author lead",
            source_dataset="publication_match",
            source_path=str(current_snapshot_dir / "publications" / "jeremy.json"),
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "publication_match",
                    "Blog post",
                    "https://example.com/jeremy",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="publication_match",
                title="Blog post",
                url="https://example.com/jeremy",
                summary="Current snapshot still only has publication lead evidence.",
                source_dataset="publication_match",
                source_path=str(current_snapshot_dir / "publications" / "jeremy.json"),
            )
        ]

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
        stored_candidate = self.store.get_candidate("cand_manual_member")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.category, "employee")
        self.assertEqual(stored_candidate.employment_status, "current")
        self.assertEqual(stored_candidate.media_url, "https://jeremy.example.com/")
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "manual_confirmed_member")
        self.assertFalse(stored_candidate.metadata.get("membership_review_required"))
        self.assertTrue(stored_candidate.metadata.get("manual_review_artifact_root"))


    def test_normalize_snapshot_inherits_manual_non_member_resolution(self) -> None:
        company_dir = self.settings.company_assets_dir / "acme"
        old_snapshot_dir = company_dir / "20260406T140000"
        old_artifact_dir = old_snapshot_dir / "normalized_artifacts"
        current_snapshot_dir = company_dir / "20260407T150000"
        old_artifact_dir.mkdir(parents=True, exist_ok=True)
        current_snapshot_dir.mkdir(parents=True, exist_ok=True)

        old_candidate = Candidate(
            candidate_id="cand_manual_non_member",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="non_member",
            target_company="Acme",
            organization="Other Org",
            employment_status="",
            role="Data Analyst",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path=str(old_snapshot_dir / "manual_review_assets" / "rabia" / "resolution.json"),
            metadata={
                "manual_review_artifact_root": str(old_snapshot_dir / "manual_review_assets" / "rabia"),
                "manual_review_links": [{"label": "LinkedIn", "url": "https://www.linkedin.com/in/rabia-example/"}],
                "target_company_mismatch": True,
                "membership_review_required": False,
                "membership_review_decision": "manual_non_member",
                "membership_review_rationale": "Manual review rejected this profile as unrelated to the target company.",
            },
        )
        old_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    old_candidate.candidate_id,
                    "manual_review",
                    "LinkedIn",
                    "https://www.linkedin.com/in/rabia-example/",
                ),
                candidate_id=old_candidate.candidate_id,
                source_type="manual_review_link",
                title="LinkedIn",
                url="https://www.linkedin.com/in/rabia-example/",
                summary="Manual review rejected this candidate as a non-member.",
                source_dataset="manual_review",
                source_path=str(old_snapshot_dir / "manual_review_assets" / "rabia" / "source_01.json"),
            )
        ]
        (old_artifact_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {"snapshot_id": old_snapshot_dir.name},
                    "candidates": [old_candidate.to_record()],
                    "evidence": [item.to_record() for item in old_evidence],
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        baseline_candidate = Candidate(
            candidate_id="cand_manual_non_member",
            name_en="Rabia Example",
            display_name="Rabia Example",
            category="former_employee",
            target_company="Acme",
            organization="Acme",
            employment_status="former",
            role="OpenAI at Acme",
            linkedin_url="https://www.linkedin.com/in/rabia-example/",
            source_dataset="acme_search_seed_candidates",
            source_path=str(current_snapshot_dir / "search_seeds" / "rabia.json"),
            metadata={
                "membership_review_required": True,
                "membership_review_reason": "suspicious_membership",
                "membership_review_decision": "suspicious_member",
            },
        )
        baseline_evidence = [
            EvidenceRecord(
                evidence_id=make_evidence_id(
                    baseline_candidate.candidate_id,
                    "acme_search_seed_candidates",
                    "Search seed",
                    "https://www.linkedin.com/in/rabia-example/",
                ),
                candidate_id=baseline_candidate.candidate_id,
                source_type="harvest_profile_search",
                title="Search seed",
                url="https://www.linkedin.com/in/rabia-example/",
                summary="Current snapshot still has the suspicious search-seed candidate.",
                source_dataset="acme_search_seed_candidates",
                source_path=str(current_snapshot_dir / "search_seeds" / "rabia.json"),
            )
        ]

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
        stored_candidate = self.store.get_candidate("cand_manual_non_member")
        self.assertIsNotNone(stored_candidate)
        assert stored_candidate is not None
        self.assertEqual(stored_candidate.category, "non_member")
        self.assertEqual(stored_candidate.organization, "Other Org")
        self.assertEqual(stored_candidate.employment_status, "")
        self.assertFalse(stored_candidate.metadata.get("membership_review_required"))
        self.assertTrue(stored_candidate.metadata.get("target_company_mismatch"))
        self.assertEqual(stored_candidate.metadata.get("membership_review_decision"), "manual_non_member")


if __name__ == "__main__":
    unittest.main()
