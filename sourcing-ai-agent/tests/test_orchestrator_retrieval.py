"""Orchestrator retrieval behavior — ported from the frozen test_pipeline.py god-file.

WS3 Tier 3 salvage wave 2 (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md;
R-009 disposition = salvage-then-delete): execute_retrieval contract coverage
(deterministic summary default, asset-population default-view scoring skip,
job-result-view persistence for snapshot-backed results, stale candidate-source
override rejection) re-homed onto the repo-standard PG fixture. Old->new port
mapping in docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks in the
same change.
"""

import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.domain import Candidate, JobRequest
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


class OrchestratorRetrievalTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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


    def test_execute_retrieval_defaults_to_deterministic_summary(self) -> None:
        candidate = Candidate(
            candidate_id="acme_1",
            name_en="Alex Builder",
            display_name="Alex Builder",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infra Engineer",
            focus_areas="GPU systems",
            linkedin_url="https://www.linkedin.com/in/alex-builder/",
            source_dataset="sqlite_candidates",
        )
        self.store.upsert_candidate(candidate)

        class _ExplodingModel(DeterministicModelClient):
            def summarize(self, request, matches, total_matches):  # type: ignore[override]
                raise AssertionError("model summary should be disabled by default")

        self.orchestrator.model_client = _ExplodingModel()
        self.acquisition_engine.model_client = self.orchestrator.model_client

        planned = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "帮我找做 GPU systems 的 infra engineer",
                "target_company": "",
                "target_scope": "scoped_search",
                "categories": ["employee"],
                "employment_statuses": ["current"],
                "keywords": ["GPU systems", "infra"],
            }
        )
        request = JobRequest.from_payload(dict(planned["request"]))
        artifact = self.orchestrator._execute_retrieval(
            "job_deterministic_default",
            request,
            dict(planned["plan"]),
            job_type="workflow",
        )
        self.assertEqual(artifact["summary"]["summary_provider"], "deterministic")
        self.assertEqual(artifact["summary"]["returned_matches"], 1)


    def test_execute_retrieval_skips_ranked_scoring_for_asset_population_default_view(self) -> None:
        snapshot_id = "20260417T090000"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="anthropic_1",
            name_en="Ada Researcher",
            display_name="Ada Researcher",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/ada-researcher/",
            source_dataset="candidate_documents",
            source_path=str(snapshot_dir / "candidate_documents.json"),
            metadata={"headline": "Research Engineer at Anthropic"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [candidate.to_record()], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Anthropic 做 Pre-training 的人",
                "target_company": "Anthropic",
                "target_scope": "scoped_search",
                "employment_statuses": ["current", "former"],
                "keywords": ["Pre-train"],
                "must_have_facets": ["pre_training"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 1,
            },
        }

        with unittest.mock.patch(
            "sourcing_agent.orchestrator.score_candidates", side_effect=AssertionError("should skip ranked scoring")
        ):
            artifact = self.orchestrator._execute_retrieval(
                "job_asset_population_fast_path",
                request,
                plan,
                job_type="workflow",
                runtime_policy={"workflow_snapshot_id": snapshot_id, "analysis_stage": "stage_1_preview"},
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["summary_provider"], "asset_population_fast_path")
        self.assertTrue(artifact["summary"]["asset_population_fast_path"])
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], 1)
        self.assertEqual(artifact["summary"]["returned_matches"], 1)
        self.assertEqual(artifact["matches"], [])
        self.assertIn("Stage 1 preview is ready", artifact["summary"]["text"])


    def test_execute_retrieval_persists_job_result_view_for_snapshot_backed_results(self) -> None:
        snapshot_id = "20260418T030303"
        snapshot_dir = self.settings.company_assets_dir / "anthropic" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        candidate = Candidate(
            candidate_id="anthropic_result_view_1",
            name_en="Nina Systems",
            display_name="Nina Systems",
            category="employee",
            target_company="Anthropic",
            organization="Anthropic",
            employment_status="current",
            role="Systems Engineer",
            linkedin_url="https://www.linkedin.com/in/nina-systems/",
            source_dataset="candidate_documents",
            source_path=str(snapshot_dir / "candidate_documents.json"),
            metadata={"headline": "Systems Engineer at Anthropic"},
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps({"candidates": [candidate.to_record()], "evidence": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 Anthropic 的全部成员",
                "target_company": "Anthropic",
                "target_scope": "full_company_asset",
                "employment_statuses": ["current", "former"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Anthropic",
                "asset_view": "canonical_merged",
                "org_scale_band": "medium",
                "default_acquisition_mode": "hybrid",
                "current_lane_default": "reuse_baseline",
                "former_lane_default": "reuse_baseline",
                "source_snapshot_id": snapshot_id,
                "source_generation_key": "gen_anthropic_1",
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": False,
                "baseline_candidate_count": 1,
            },
        }

        artifact = self.orchestrator._execute_retrieval(
            "job_asset_population_result_view_persisted",
            request,
            plan,
            job_type="workflow",
            runtime_policy={"workflow_snapshot_id": snapshot_id},
            persist_job_state=True,
        )

        self.assertEqual(artifact["summary"]["candidate_source"]["source_kind"], "company_snapshot")
        result_view = self.store.get_job_result_view(job_id="job_asset_population_result_view_persisted")
        assert result_view is not None
        self.assertEqual(result_view["source_kind"], "company_snapshot")
        self.assertEqual(result_view["view_kind"], "asset_population")
        self.assertEqual(result_view["company_key"], "anthropic")
        self.assertEqual(result_view["snapshot_id"], snapshot_id)
        self.assertEqual(result_view["authoritative_snapshot_id"], snapshot_id)
        self.assertEqual(result_view["materialization_generation_key"], "gen_anthropic_1")
        self.assertTrue(str(result_view.get("request_signature") or ""))

        saved_job = self.store.get_job("job_asset_population_result_view_persisted")
        assert saved_job is not None
        saved_candidate_source = dict(dict(saved_job.get("summary") or {}).get("candidate_source") or {})
        self.assertEqual(saved_candidate_source["result_view_id"], result_view["view_id"])
        self.assertEqual(saved_candidate_source["result_view_kind"], "asset_population")
        self.assertEqual(saved_candidate_source["authoritative_snapshot_id"], snapshot_id)
        self.assertEqual(saved_candidate_source["materialization_generation_key"], "gen_anthropic_1")


    def test_execute_retrieval_ignores_stale_candidate_source_override_for_workflow_snapshot(self) -> None:
        old_snapshot_id = "snapshot-google-old-result-view"
        new_snapshot_id = "snapshot-google-gemini-delta"
        old_candidate = Candidate(
            candidate_id="cand-google-old",
            name_en="Old Result",
            display_name="Old Result",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Legacy Engineer",
            linkedin_url="https://www.linkedin.com/in/google-old/",
        )
        new_candidate = Candidate(
            candidate_id="cand-google-gemini",
            name_en="Gemini Current",
            display_name="Gemini Current",
            category="employee",
            target_company="Google",
            organization="Google",
            employment_status="current",
            role="Gemini Engineer",
            linkedin_url="https://www.linkedin.com/in/google-gemini/",
        )
        _old_snapshot_dir, old_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=old_snapshot_id,
            candidates=[old_candidate.to_record()],
        )
        _new_snapshot_dir, _new_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=new_snapshot_id,
            candidates=[new_candidate.to_record()],
        )
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找Google在Gemini组的人",
                "target_company": "Google",
                "target_scope": "full_company_asset",
                "keywords": ["Gemini"],
                "employment_statuses": ["current", "former"],
                "top_k": 10,
            }
        )
        plan = {
            "organization_execution_profile": {
                "target_company": "Google",
                "asset_view": "canonical_merged",
                "source_snapshot_id": old_snapshot_id,
            },
            "asset_reuse_plan": {
                "baseline_reuse_available": True,
                "requires_delta_acquisition": True,
                "baseline_snapshot_id": old_snapshot_id,
            },
        }
        job_id = "job_google_gemini_stale_override"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="retrieving",
            request_payload=request.to_record(),
            plan_payload=plan,
            summary_payload={"message": "Retrieving"},
        )
        self.store.upsert_job_result_view(
            job_id=job_id,
            target_company="Google",
            source_kind="company_snapshot",
            view_kind="asset_population",
            snapshot_id=old_snapshot_id,
            source_path=str(old_doc_path),
            authoritative_snapshot_id=old_snapshot_id,
            summary={"candidate_count": 1},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_build_effective_execution_semantics",
            return_value={"default_results_mode": "asset_population", "asset_population_supported": True},
        ):
            artifact = self.orchestrator._execute_retrieval(
                job_id,
                request,
                plan,
                job_type="workflow",
                runtime_policy={"workflow_snapshot_id": new_snapshot_id},
                candidate_source_override={
                    "source_kind": "company_snapshot",
                    "snapshot_id": old_snapshot_id,
                    "asset_view": "canonical_merged",
                    "source_path": str(old_doc_path),
                    "candidate_count": 1,
                    "candidates": [old_candidate],
                    "evidence_lookup": {},
                },
            )

        candidate_source = dict(artifact["summary"]["candidate_source"])
        self.assertEqual(candidate_source["snapshot_id"], new_snapshot_id)
        self.assertIn(new_snapshot_id, candidate_source["source_path"])
        self.assertNotIn(old_snapshot_id, candidate_source["source_path"])
        self.assertGreaterEqual(int(candidate_source["candidate_count"]), 1)
        result_view = self.store.get_job_result_view(job_id=job_id)
        assert result_view is not None
        self.assertEqual(result_view["snapshot_id"], new_snapshot_id)
        self.assertIn(new_snapshot_id, result_view["source_path"])
        self.assertNotIn(old_snapshot_id, result_view["source_path"])
        events = self.store.list_job_events(job_id)
        self.assertTrue(
            any(
                str(event.get("status") or "") == "recovered"
                and str(dict(event.get("payload") or {}).get("reason") or "")
                == "candidate_source_override_snapshot_mismatch"
                for event in events
            )
        )



if __name__ == "__main__":
    unittest.main()
