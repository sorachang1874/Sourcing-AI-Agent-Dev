"""Acquisition checkpoint-reusability predicates — shard-A port group G1.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/NONBAND_OWNERSHIP_R2_SHARD_A_
2026-07-22.md G1): `_acquisition_task_checkpoint_reusable` and its
per-task-type freshness/scope requirements (roster snapshot vs search-seed-
only, former/current scope state, fully-materialized normalize artifacts,
retrieval-index summary freshness, equivalent-baseline / reused-snapshot skip
gates) had zero modern pins. Ported verbatim onto the repo-standard PG
fixture with the two snapshot/artifact helpers. Old->new mapping in
docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks same-change.
"""

import json
import os
import tempfile
import time
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import AcquisitionTask, Candidate, JobRequest
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.seed_discovery import SearchSeedSnapshot
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from tests.pg_store_fixture import PGControlPlaneStoreTestMixin


class AcquisitionCheckpointReusabilityTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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
        runtime_env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        runtime_env_patcher.start()
        self.addCleanup(runtime_env_patcher.stop)

    def _write_company_snapshot_candidate_documents(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidates: list[dict[str, object]],
    ) -> tuple[Path, Path]:
        normalized_key = normalize_company_key(target_company)
        company_key = canonicalize_company_key(target_company) or normalized_key
        identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "aliases": [normalized_key] if normalized_key and normalized_key != company_key else [],
        }
        company_dir = Path(self.tempdir.name) / "company_assets" / company_key
        snapshot_dir = company_dir / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "target_company": target_company,
                        "snapshot_id": snapshot_id,
                        "company_identity": identity,
                    },
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "candidates": candidates,
                    "evidence": [],
                    "candidate_count": len(candidates),
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (company_dir / "latest_snapshot.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "company_identity": identity,
                    "target_company": target_company,
                    "company_key": company_key,
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
        return snapshot_dir, candidate_doc_path

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

    def test_full_company_roster_checkpoint_requires_roster_snapshot_not_search_seed_only(self) -> None:
        identity = CompanyIdentity(
            requested_name="Google",
            canonical_name="Google",
            company_key="google",
            linkedin_slug="google",
            linkedin_company_url="https://www.linkedin.com/company/google/",
        )
        snapshot_dir = self.settings.company_assets_dir / "google" / "snapshot-search-seed-only"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        entries_payload = [
            {
                "seed_key": "former_1",
                "full_name": "Former Seed",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/former-seed/",
            }
        ]
        entries_path.write_text(json.dumps(entries_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Google",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=entries_payload,
            query_summaries=[{"query": "Veo", "status": "completed"}],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_fallback",
            summary_path=summary_path,
            entries_path=entries_path,
        )
        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire Google roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "full_company_roster"},
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"search_seed_snapshot": search_seed_snapshot},
            )
        )

        scoped_task = AcquisitionTask(
            task_id="acquire-scoped-roster",
            task_type="acquire_full_roster",
            title="Acquire scoped roster",
            description="Acquire scoped roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "scoped_search_roster"},
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                scoped_task,
                {"search_seed_snapshot": search_seed_snapshot},
            )
        )

    def test_former_search_seed_checkpoint_requires_former_scope_state(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-former-checkpoint"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")

        current_entries = [
            {
                "seed_key": "openai_current_1",
                "full_name": "Current Seed",
                "source_type": "harvest_profile_search",
                "employment_status": "current",
                "profile_url": "https://www.linkedin.com/in/openai-current-seed/",
            }
        ]
        entries_path.write_text(json.dumps(current_entries, ensure_ascii=False, indent=2), encoding="utf-8")
        current_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=current_entries,
            query_summaries=[{"query": "Coding", "status": "completed"}],
            accounts_used=["harvest_profile_search"],
            errors=[],
            stop_reason="provider_people_search_primary",
            summary_path=summary_path,
            entries_path=entries_path,
        )
        former_task = AcquisitionTask(
            task_id="acquire-former-search-seed",
            task_type="acquire_former_search_seed",
            title="Acquire former-member LinkedIn search seeds",
            description="Acquire former-member search seeds",
            status="ready",
            blocking=True,
            metadata={"search_seed_queries": ["Coding"]},
        )

        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                former_task,
                {"search_seed_snapshot": current_seed_snapshot},
            )
        )

        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-former-current-only-candidates",
            candidates=[
                Candidate(
                    candidate_id="openai_current_only",
                    name_en="Current Only",
                    display_name="Current Only",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Engineer",
                    linkedin_url="https://www.linkedin.com/in/openai-current-only/",
                ).to_record()
            ],
        )
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                former_task,
                {
                    "candidate_doc_path": candidate_doc_path,
                    "candidates": [
                        Candidate(
                            candidate_id="openai_current_only",
                            name_en="Current Only",
                            display_name="Current Only",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url="https://www.linkedin.com/in/openai-current-only/",
                        )
                    ],
                },
            )
        )

        former_entries = [
            {
                "seed_key": "openai_former_1",
                "full_name": "Former Seed",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/openai-former-seed/",
            }
        ]
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                former_task,
                {
                    "search_seed_snapshot": SearchSeedSnapshot(
                        snapshot_id=snapshot_dir.name,
                        target_company="OpenAI",
                        company_identity=identity,
                        snapshot_dir=snapshot_dir,
                        entries=former_entries,
                        query_summaries=[{"query": "Coding", "status": "completed"}],
                        accounts_used=["harvest_profile_search"],
                        errors=[],
                        stop_reason="provider_people_search_primary",
                        summary_path=summary_path,
                        entries_path=entries_path,
                    )
                },
            )
        )

    def test_scoped_current_roster_checkpoint_requires_current_scope_state(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-current-checkpoint"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        summary_path = snapshot_dir / "search_seed_discovery" / "summary.json"
        entries_path = snapshot_dir / "search_seed_discovery" / "entries.json"
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text("{}", encoding="utf-8")
        former_entries = [
            {
                "seed_key": "openai_former_only",
                "full_name": "Former Only",
                "source_type": "harvest_profile_search",
                "employment_status": "former",
                "profile_url": "https://www.linkedin.com/in/openai-former-only/",
            }
        ]
        entries_path.write_text(json.dumps(former_entries, ensure_ascii=False, indent=2), encoding="utf-8")
        scoped_task = AcquisitionTask(
            task_id="acquire-scoped-roster",
            task_type="acquire_full_roster",
            title="Acquire current company roster",
            description="Acquire current company roster",
            status="ready",
            blocking=True,
            metadata={"strategy_type": "scoped_search_roster"},
        )

        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                scoped_task,
                {
                    "search_seed_snapshot": SearchSeedSnapshot(
                        snapshot_id=snapshot_dir.name,
                        target_company="OpenAI",
                        company_identity=identity,
                        snapshot_dir=snapshot_dir,
                        entries=former_entries,
                        query_summaries=[{"query": "Coding", "status": "completed"}],
                        accounts_used=["harvest_profile_search"],
                        errors=[],
                        stop_reason="provider_people_search_primary",
                        summary_path=summary_path,
                        entries_path=entries_path,
                    )
                },
            )
        )

        current_entries = [
            {
                "seed_key": "openai_current_only",
                "full_name": "Current Only",
                "source_type": "harvest_profile_search",
                "employment_status": "current",
                "profile_url": "https://www.linkedin.com/in/openai-current-only/",
            }
        ]
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                scoped_task,
                {
                    "search_seed_snapshot": SearchSeedSnapshot(
                        snapshot_id=snapshot_dir.name,
                        target_company="OpenAI",
                        company_identity=identity,
                        snapshot_dir=snapshot_dir,
                        entries=current_entries,
                        query_summaries=[{"query": "Coding", "status": "completed"}],
                        accounts_used=["harvest_profile_search"],
                        errors=[],
                        stop_reason="provider_people_search_primary",
                        summary_path=summary_path,
                        entries_path=entries_path,
                    )
                },
            )
        )

    def test_normalize_checkpoint_requires_fully_materialized_snapshot_artifacts(self) -> None:
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="xAI",
            snapshot_id="snapshot-normalize-checkpoint",
            candidates=[
                Candidate(
                    candidate_id="cand_xai_checkpoint",
                    name_en="Preview Candidate",
                    display_name="Preview Candidate",
                    category="employee",
                    target_company="xAI",
                    organization="xAI",
                    employment_status="current",
                    role="Engineer",
                    linkedin_url="https://www.linkedin.com/in/preview-candidate/",
                ).to_record()
            ],
        )
        task = AcquisitionTask(
            task_id="normalize-checkpoint",
            task_type="normalize_asset_snapshot",
            title="Normalize snapshot",
            description="Persist snapshot artifacts",
        )

        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="xAI",
            include_strict=False,
        )
        self.assertTrue(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "manifest_path": snapshot_dir / "manifest.json",
                },
            )
        )

        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="xAI",
            include_strict=True,
        )
        fresh_time = time.time() + 10
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (fresh_time, fresh_time))
        self.assertFalse(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": candidate_doc_path,
                    "manifest_path": snapshot_dir / "manifest.json",
                },
            )
        )

    def test_build_retrieval_index_checkpoint_requires_summary_newer_than_materialized_snapshot(self) -> None:
        snapshot_dir, _ = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-retrieval-checkpoint",
            candidates=[
                Candidate(
                    candidate_id="cand_openai_checkpoint",
                    name_en="Retriever Candidate",
                    display_name="Retriever Candidate",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/retriever-candidate/",
                ).to_record()
            ],
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
            include_strict=True,
        )
        normalized_time = time.time() + 20
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (normalized_time, normalized_time))

        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
        retrieval_index_path.write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stale_time = normalized_time - 10
        os.utime(retrieval_index_path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="build-retrieval-checkpoint",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Build retrieval index summary",
        )
        self.assertTrue(self.orchestrator._snapshot_retrieval_index_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )

        fresh_time = normalized_time + 10
        os.utime(retrieval_index_path, (fresh_time, fresh_time))
        self.assertFalse(self.orchestrator._snapshot_retrieval_index_refresh_required(snapshot_dir))
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )

    def test_equivalent_baseline_reuse_skips_normalize_asset_snapshot_checkpoint(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "openai" / "baseline-equivalent-normalize"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "OpenAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
            include_strict=True,
        )
        stale_time = time.time() - 60
        fresh_time = time.time() + 60
        os.utime(candidate_doc_path, (fresh_time, fresh_time))
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="normalize-equivalent-baseline",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Persist the authoritative snapshot artifacts",
        )
        self.assertTrue(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "snapshot_id": snapshot_dir.name,
                    "equivalent_baseline_reused": True,
                    "equivalent_baseline_snapshot_id": snapshot_dir.name,
                },
            )
        )

    def test_reused_snapshot_serving_artifacts_skip_normalize_asset_snapshot_checkpoint(self) -> None:
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-reused-serving-normalize",
            candidates=[
                Candidate(
                    candidate_id="cand_reflection_checkpoint",
                    name_en="Reflection Researcher",
                    display_name="Reflection Researcher",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Researcher",
                    linkedin_url="https://www.linkedin.com/in/reflection-checkpoint/",
                ).to_record()
            ],
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="Reflection AI",
            include_strict=True,
            include_serving_docs=True,
        )
        stale_time = time.time() - 60
        fresh_time = time.time() + 60
        os.utime(candidate_doc_path, (fresh_time, fresh_time))
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json",
            snapshot_dir / "normalized_artifacts" / "reusable_candidate_documents.json",
        ):
            os.utime(path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="normalize-reused-serving-checkpoint",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Persist the authoritative snapshot artifacts",
        )
        materialized_candidate_doc_path = snapshot_dir / "normalized_artifacts" / "materialized_candidate_documents.json"
        self.assertTrue(self.orchestrator._snapshot_materialization_refresh_required(snapshot_dir))
        self.assertTrue(
            self.orchestrator._reused_snapshot_serving_artifacts_ready(
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": materialized_candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                }
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "candidate_doc_path": materialized_candidate_doc_path,
                    "reused_snapshot_checkpoint": True,
                },
            )
        )

    def test_reused_snapshot_candidate_documents_skip_normalize_and_build_retrieval_index_checkpoints(self) -> None:
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-reused-direct-candidate-docs",
            candidates=[
                Candidate(
                    candidate_id="cand_reflection_direct_reuse",
                    name_en="Reflection Direct Reuse",
                    display_name="Reflection Direct Reuse",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/reflection-direct-reuse/",
                ).to_record()
            ],
        )
        normalize_task = AcquisitionTask(
            task_id="normalize-reused-direct-candidate-docs",
            task_type="normalize_asset_snapshot",
            title="Normalize and version the asset snapshot",
            description="Persist the authoritative snapshot artifacts",
        )
        build_index_task = AcquisitionTask(
            task_id="build-index-reused-direct-candidate-docs",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Prepare retrieval-ready artifacts",
        )

        acquisition_state = {
            "snapshot_dir": snapshot_dir,
            "candidate_doc_path": candidate_doc_path,
            "reused_snapshot_checkpoint": True,
        }
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                normalize_task,
                acquisition_state,
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                build_index_task,
                acquisition_state,
            )
        )

    def test_equivalent_baseline_reuse_skips_build_retrieval_index_checkpoint(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "openai" / "baseline-equivalent-index"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "target_company": "OpenAI",
                    "snapshot_id": snapshot_dir.name,
                    "candidates": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_snapshot_normalized_artifacts(
            snapshot_dir=snapshot_dir,
            target_company="OpenAI",
            include_strict=True,
        )
        normalized_time = time.time() + 20
        for path in (
            snapshot_dir / "normalized_artifacts" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "artifact_summary.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "manifest.json",
            snapshot_dir / "normalized_artifacts" / "strict_roster_only" / "artifact_summary.json",
        ):
            os.utime(path, (normalized_time, normalized_time))

        retrieval_index_path = snapshot_dir / "retrieval_index_summary.json"
        retrieval_index_path.write_text(
            json.dumps({"status": "built"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stale_time = normalized_time - 10
        os.utime(retrieval_index_path, (stale_time, stale_time))

        task = AcquisitionTask(
            task_id="build-equivalent-baseline-index",
            task_type="build_retrieval_index",
            title="Build retrieval index",
            description="Build retrieval index summary",
        )
        self.assertTrue(self.orchestrator._snapshot_retrieval_index_refresh_required(snapshot_dir))
        self.assertFalse(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {"snapshot_dir": snapshot_dir},
            )
        )
        self.assertTrue(
            self.orchestrator._acquisition_task_checkpoint_reusable(
                task,
                {
                    "snapshot_dir": snapshot_dir,
                    "snapshot_id": snapshot_dir.name,
                    "equivalent_baseline_reused": True,
                    "equivalent_baseline_snapshot_id": snapshot_dir.name,
                },
            )
        )


if __name__ == "__main__":
    unittest.main()
