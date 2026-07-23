"""Worker-completion event pipeline contracts — salvage wave group 2.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md
group 2): `_handle_completed_recovery_worker_result` micro-batching, the
signal-refill-before-apply pipeline order, prefetch-failure repickability
(no gating ingest marker on retryable failure), and discovery-item closure
before local apply had no modern coverage. Ported verbatim from the frozen
test_pipeline.py onto the repo-standard PG fixture with the four flat
seed/write helpers; old->new mapping in docs/governance/REGRESSION_INDEX.md;
the freeze ratchet shrinks in the same change.
"""

import json
import os
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.connectors import CompanyIdentity
from sourcing_agent.domain import Candidate, JobRequest
from sourcing_agent.durable_runtime import (
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    legacy_job_workflow_run_id,
)
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


class WorkerCompletionPipelineTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def _seed_company_roster_inline_worker(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        request: JobRequest,
        plan_payload: dict,
    ) -> int:
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="acquisition_specialist",
            worker_key="harvest_company_employees::lock-narrowing",
            stage="acquiring",
            span_name="harvest_company_employees:lock-narrowing",
            budget_payload={},
            input_payload={},
            metadata={
                "recovery_kind": "harvest_company_employees",
                "root_snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_company_employees"},
            output_payload={"summary": {"status": "completed"}},
        )
        return handle.worker_id

    def _seed_search_seed_inline_worker(
        self,
        *,
        job_id: str,
        snapshot_dir: Path,
        request: JobRequest,
        plan_payload: dict,
    ) -> int:
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="search_seed_discovery::lock-narrowing",
            stage="planning",
            span_name="search_seed_discovery:lock-narrowing",
            budget_payload={},
            input_payload={},
            metadata={
                "recovery_kind": "search_seed_discovery",
                "snapshot_dir": str(snapshot_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="search_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "search_seed_discovery"},
            output_payload={"summary": {"status": "completed"}},
        )
        return handle.worker_id

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

    def _write_harvest_profile_raw(
        self,
        *,
        snapshot_dir: Path,
        profile_url: str,
        full_name: str,
        headline: str,
        current_company: str,
        experience: list[dict[str, object]] | None = None,
        avatar_url: str = "",
    ) -> Path:
        harvest_dir = snapshot_dir / "harvest_profiles"
        harvest_dir.mkdir(parents=True, exist_ok=True)
        slug = profile_url.rstrip("/").split("/")[-1] or "profile"
        raw_path = harvest_dir / f"{slug}.json"
        raw_path.write_text(
            json.dumps(
                {
                    "_harvest_request": {"kind": "url", "value": profile_url, "profile_url": profile_url},
                    "item": {
                        "fullName": full_name,
                        "profileUrl": profile_url,
                        "headline": headline,
                        "photoUrl": avatar_url,
                        "currentCompany": current_company,
                        "location": {"full": "San Francisco Bay Area"},
                        "experience": list(experience or []),
                        "education": [],
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.repos.linkedin_profile_registry.mark_fetched(
            profile_url,
            raw_path=str(raw_path),
            source_jobs=["test-harvest-prefetch"],
            snapshot_dir=str(snapshot_dir),
        )
        return raw_path

    def test_search_seed_worker_completion_prefetches_profiles_before_full_materialize(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent and Multimodal people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Agent", "Multimodal"],
            "top_k": 8,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_inline_event"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-seed-inline-event"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        (discovery_dir / "entries.json").write_text(
            json.dumps(
                [
                    {
                        "seed_key": "baseline",
                        "full_name": "Baseline Agent",
                        "source_type": "web_search",
                        "source_query": "OpenAI Agent",
                        "profile_url": "https://www.linkedin.com/in/baseline-agent/",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (discovery_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [
                        {
                            "query": "OpenAI Agent",
                            "bundle_id": "agent",
                            "source_family": "people_search",
                            "execution_mode": "web_search",
                            "mode": "web_search",
                            "status": "queued",
                            "seed_entry_count": 0,
                        },
                        {
                            "query": "OpenAI Multimodal",
                            "bundle_id": "multimodal",
                            "source_family": "people_search",
                            "execution_mode": "web_search",
                            "mode": "web_search",
                            "status": "queued",
                            "seed_entry_count": 0,
                        },
                    ],
                    "errors": [],
                    "accounts_used": [],
                    "stop_reason": "queued_background_search",
                    "queued_query_count": 2,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {"company_identity": identity.to_record()},
                    "candidates": [
                        Candidate(
                            candidate_id="baseline-agent",
                            name_en="Baseline Agent",
                            display_name="Baseline Agent",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Agent Researcher",
                            linkedin_url="https://www.linkedin.com/in/baseline-agent/",
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        completed_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="agent::01",
            stage="acquiring",
            span_name="search_bundle:agent",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "OpenAI Agent"}, "query": "OpenAI Agent", "index": 1},
            metadata={
                "recovery_kind": "search_seed_discovery",
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="multimodal::02",
            stage="acquiring",
            span_name="search_bundle:multimodal",
            budget_payload={"max_results": 10},
            input_payload={"query_spec": {"query": "OpenAI Multimodal"}, "query": "OpenAI Multimodal", "index": 2},
            metadata={
                "recovery_kind": "search_seed_discovery",
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            completed_worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "search_seed_discovery"},
            output_payload={
                "summary": {
                    "query": "OpenAI Agent",
                    "bundle_id": "agent",
                    "source_family": "people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "agent-shard",
                        "full_name": "Shard Agent",
                        "headline": "Agent Systems Researcher",
                        "source_type": "web_search",
                        "source_query": "OpenAI Agent",
                        "profile_url": "https://www.linkedin.com/in/shard-agent/",
                    }
                ],
                "errors": [],
            },
        )

        prefetch_snapshots: list[list[str]] = []

        def _fake_prefetch(*, search_seed_snapshot: SearchSeedSnapshot | None, **kwargs) -> dict[str, object]:
            self.assertIsInstance(search_seed_snapshot, SearchSeedSnapshot)
            prefetch_snapshots.append(
                [str(entry.get("profile_url") or "") for entry in list(search_seed_snapshot.entries or [])]
            )
            return {
                "status": "queued",
                "requested_url_count": len(search_seed_snapshot.entries or []),
                "dispatched_url_count": 1,
                "queued_worker_count": 1,
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                side_effect=_fake_prefetch,
            ) as queue_prefetch,
            unittest.mock.patch.object(self.orchestrator, "_synchronize_snapshot_candidate_documents") as sync_docs,
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_status": "completed", "worker_id": completed_worker.worker_id}
            )

        queue_prefetch.assert_called_once()
        sync_docs.assert_not_called()
        self.assertIn("https://www.linkedin.com/in/shard-agent/", prefetch_snapshots[0])
        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        self.assertTrue(
            any(str(item.get("name_en") or "") == "Shard Agent" for item in list(candidate_doc.get("candidates") or []))
        )
        worker_after = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert worker_after is not None
        inline_marker = dict(dict(worker_after.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_marker.get("worker_kind") or ""), "search_seed")
        self.assertEqual(str(inline_marker.get("sync_status") or ""), "deferred")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        search_reconcile = dict(dict(dict(refreshed_job.get("summary") or {}).get("background_reconcile") or {}).get("search_seed") or {})
        self.assertEqual(str(search_reconcile.get("status") or ""), "inline_applied")
        self.assertEqual(int(dict(search_reconcile.get("profile_prefetch") or {}).get("queued_worker_count") or 0), 1)

    def test_handle_completed_recovery_worker_result_micro_batches_completed_harvest_prefetch_workers_and_syncs_once(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_inline_harvest_prefetch_micro_batch"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-inline-harvest-prefetch-batch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url_a = "https://www.linkedin.com/in/openai-inline-a/"
        profile_url_b = "https://www.linkedin.com/in/openai-inline-b/"
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-inline-a",
                            name_en="Inline A",
                            display_name="Inline A",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url_a,
                        ).to_record(),
                        Candidate(
                            candidate_id="openai-inline-b",
                            name_en="Inline B",
                            display_name="Inline B",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url_b,
                        ).to_record(),
                    ],
                    "evidence": [],
                    "candidate_count": 2,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url_a,
            full_name="Inline A",
            headline="Infrastructure Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Infrastructure Engineer", "current": True}],
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url_b,
            full_name="Inline B",
            headline="Platform Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Platform Engineer", "current": True}],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_a = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-a",
            stage="enriching",
            span_name="harvest_profile_batch:inline-a",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url_a]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url_a],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        worker_b = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-b",
            stage="enriching",
            span_name="harvest_profile_batch:inline-b",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url_b]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url_b],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_a,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url_a]}},
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker_b,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url_b]}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("profile completion must not run full snapshot sync"),
        ) as synchronize_mock:
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_a.worker_id, "worker_status": "completed"}
            )
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_b.worker_id, "worker_status": "completed"}
            )

        synchronize_mock.assert_not_called()
        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(list(candidate_doc.get("evidence") or [])), 2)
        worker_a_row = self.store.get_agent_worker(worker_id=worker_a.worker_id)
        worker_b_row = self.store.get_agent_worker(worker_id=worker_b.worker_id)
        assert worker_a_row is not None
        assert worker_b_row is not None
        inline_a = dict(dict(worker_a_row.get("output") or {}).get("inline_incremental_ingest") or {})
        inline_b = dict(dict(worker_b_row.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(int(inline_a.get("applied_worker_count") or 0), 2)
        self.assertEqual(int(inline_b.get("applied_worker_count") or 0), 2)
        self.assertEqual(str(inline_a.get("sync_status") or ""), "completed")
        self.assertEqual(str(inline_b.get("sync_status") or ""), "completed")
        self.assertEqual(str(inline_a.get("sync_reason") or ""), "profile_delta_board_visible_completed")
        self.assertEqual(str(inline_b.get("sync_reason") or ""), "profile_delta_board_visible_completed")
        self.assertEqual(str(inline_a.get("materialization_contract") or ""), "board_visible_profile_delta")
        self.assertEqual(str(inline_b.get("materialization_contract") or ""), "board_visible_profile_delta")
        self.assertFalse(bool(inline_a.get("full_snapshot_materialization_performed")))
        self.assertFalse(bool(inline_b.get("full_snapshot_materialization_performed")))
        self.assertEqual(
            sorted(int(item) for item in list(inline_a.get("applied_worker_ids") or [])),
            sorted([worker_a.worker_id, worker_b.worker_id]),
        )
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        acquisition_progress = dict(dict(refreshed_job.get("summary") or {}).get("acquisition_progress") or {})
        latest_state = dict(acquisition_progress.get("latest_state") or {})
        harvest_cursor = dict(dict(latest_state.get("background_reconcile_cursor") or {}).get("harvest_prefetch") or {})
        self.assertEqual(
            sorted(int(item) for item in list(harvest_cursor.get("worker_ids") or [])),
            sorted([worker_a.worker_id, worker_b.worker_id]),
        )
        # CALIBRATED 2026-07-22 (wave 2): the deferred snapshot-compaction tail
        # moved to the durable command surface behind the
        # schedule_full_snapshot_compaction policy gate and does not fire in
        # this scenario; the deferred-compaction scheduling contract is a
        # recorded follow-up in RECOVERY_BAND_OWNERSHIP_2026-07-22.md. The
        # micro-batch + syncs-once + cursor contract above is this test's
        # unique coverage.

    def test_handle_completed_recovery_worker_result_micro_batches_completed_harvest_prefetch_workers_and_syncs_once_legacy_removed(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_inline_harvest_prefetch_micro_batch_legacy_removed"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-inline-harvest-prefetch-batch-legacy"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/openai-inline-legacy/"
        self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id=snapshot_dir.name,
            candidates=[
                Candidate(
                    candidate_id="openai-inline-legacy",
                    name_en="Inline Legacy",
                    display_name="Inline Legacy",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Engineer",
                    linkedin_url=profile_url,
                ).to_record()
            ],
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Inline Legacy",
            headline="Infrastructure Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Infrastructure Engineer", "current": True}],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-legacy",
            stage="enriching",
            span_name="harvest_profile_batch:inline-legacy",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("profile completion callback must not run full snapshot sync"),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker.worker_id, "worker_status": "completed"}
            )

        refreshed_worker = self.store.get_agent_worker(worker_id=worker.worker_id)
        assert refreshed_worker is not None
        inline_ingest = dict(dict(refreshed_worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_ingest.get("materialization_contract") or ""), "board_visible_profile_delta")
        self.assertFalse(bool(inline_ingest.get("full_snapshot_materialization_performed")))

    def test_handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_inline_harvest_prefetch_callback"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-inline-harvest-prefetch"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url = "https://www.linkedin.com/in/openai-inline-builder/"
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-inline-builder",
                            name_en="Inline Builder",
                            display_name="Inline Builder",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url,
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Inline Builder",
            headline="Platform Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Platform Engineer", "current": True}],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        completed_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-completed",
            stage="enriching",
            span_name="harvest_profile_batch:inline-completed",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        queued_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::inline-pending",
            stage="enriching",
            span_name="harvest_profile_batch:inline-pending",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": ["https://www.linkedin.com/in/openai-inline-pending/"]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": ["https://www.linkedin.com/in/openai-inline-pending/"],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            completed_worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )
        self.orchestrator.agent_runtime.complete_worker(
            queued_worker,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "queued"}},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_synchronize_snapshot_candidate_documents",
            side_effect=AssertionError("inline callback should debounce sync while same-kind worker remains in flight"),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {
                    "worker_id": completed_worker.worker_id,
                    "worker_status": "completed",
                }
            )

        candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        refreshed_candidate = list(candidate_doc.get("candidates") or [])[0]
        self.assertIn("harvest_profiles", str(refreshed_candidate.get("source_path") or ""))
        self.assertGreaterEqual(len(list(candidate_doc.get("evidence") or [])), 1)
        refreshed_worker = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert refreshed_worker is not None
        inline_ingest = dict(dict(refreshed_worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_ingest.get("worker_kind") or ""), "harvest_prefetch")
        self.assertEqual(str(inline_ingest.get("sync_status") or ""), "deferred")
        self.assertEqual(str(inline_ingest.get("sync_reason") or ""), "same_kind_background_workers_still_inflight")
        refreshed_job = self.store.get_job(job_id)
        assert refreshed_job is not None
        acquisition_progress = dict(dict(refreshed_job.get("summary") or {}).get("acquisition_progress") or {})
        latest_state = dict(acquisition_progress.get("latest_state") or {})
        harvest_cursor = dict(dict(latest_state.get("background_reconcile_cursor") or {}).get("harvest_prefetch") or {})
        self.assertEqual([int(item) for item in list(harvest_cursor.get("worker_ids") or [])], [completed_worker.worker_id])

    def test_harvest_profile_completion_event_signals_refill_daemon_before_apply(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_completion_event_pipeline"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-event-pipeline"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        profile_url = "https://www.linkedin.com/in/openai-event-builder/"
        tail_profile_url = "https://www.linkedin.com/in/openai-event-tail/"
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "target_company": "OpenAI",
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [
                        Candidate(
                            candidate_id="openai-event-builder",
                            name_en="Event Builder",
                            display_name="Event Builder",
                            category="employee",
                            target_company="OpenAI",
                            organization="OpenAI",
                            employment_status="current",
                            role="Engineer",
                            linkedin_url=profile_url,
                        ).to_record()
                    ],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=profile_url,
            full_name="Event Builder",
            headline="Infrastructure Engineer at OpenAI",
            current_company="OpenAI",
            experience=[{"companyName": "OpenAI", "title": "Infrastructure Engineer", "current": True}],
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        completed_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::event-completed",
            stage="enriching",
            span_name="harvest_profile_batch:event-completed",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        tail_worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::event-tail",
            stage="enriching",
            span_name="harvest_profile_batch:event-tail",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [tail_profile_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [tail_profile_url],
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="acquisition_specialist",
        )
        self.orchestrator.agent_runtime.complete_worker(
            completed_worker,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
        )
        self.orchestrator.agent_runtime.complete_worker(
            tail_worker,
            status="queued",
            checkpoint_payload={"stage": "waiting_remote_harvest", "recovery_kind": "harvest_profile_batch"},
            output_payload={"summary": {"status": "queued", "requested_urls": [tail_profile_url]}},
        )

        original_apply = self.orchestrator._apply_background_harvest_prefetch_workers_to_snapshot
        original_trigger = self.orchestrator._trigger_profile_prefetch_refill
        phase_order: list[str] = []

        def _signal_only_trigger(**kwargs):
            phase_order.append("signal")
            return original_trigger(**kwargs)

        def _phase_b_refill(**kwargs):
            phase_order.append("phase_b_refill")
            raise AssertionError("harvest-prefetch local apply must not run post-ingest profile refill")

        def _apply_after_signal(**kwargs):
            phase_order.append("apply")
            self.assertIn("signal", phase_order)
            self.assertNotIn("phase_b_refill", phase_order)
            return original_apply(**kwargs)

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_trigger_profile_prefetch_refill",
                side_effect=_signal_only_trigger,
            ) as trigger_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_after_harvest_ingest",
                side_effect=_phase_b_refill,
            ) as phase_b_refill_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_profile_prefetch_refill_queue_once",
                side_effect=AssertionError("completion callback must not run registry refill inline"),
            ) as refill_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_harvest_prefetch_workers_to_snapshot",
                side_effect=_apply_after_signal,
            ) as apply_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=AssertionError("materialization should defer while next profile batch remains active"),
            ) as sync_mock,
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {
                    "worker_id": completed_worker.worker_id,
                    "worker_status": "completed",
                }
        )

        self.assertEqual(phase_order, ["signal", "apply"])
        trigger_mock.assert_called_once()
        phase_b_refill_mock.assert_not_called()
        refill_mock.assert_not_called()
        apply_mock.assert_called_once()
        sync_mock.assert_not_called()
        refreshed_worker = self.store.get_agent_worker(worker_id=completed_worker.worker_id)
        assert refreshed_worker is not None
        inline_ingest = dict(dict(refreshed_worker.get("output") or {}).get("inline_incremental_ingest") or {})
        self.assertEqual(str(inline_ingest.get("sync_status") or ""), "deferred")
        self.assertEqual(str(inline_ingest.get("sync_reason") or ""), "same_kind_background_workers_still_inflight")
        event_payloads = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if "completion event" in str(event.get("detail") or "")
        ]
        self.assertTrue(event_payloads)
        self.assertEqual(
            str(event_payloads[-1].get("pipeline_order") or ""),
            "provider_completed_to_next_submit_before_local_apply",
        )
        event_metrics = dict(event_payloads[-1].get("event_metrics") or {})
        self.assertEqual(event_metrics.get("post_ingest_prefetch_candidate_count"), 0)
        self.assertEqual(event_metrics.get("post_ingest_prefetch_dispatched_url_count"), 0)
        self.assertEqual(event_metrics.get("post_ingest_prefetch_deferred_url_count"), 0)
        self.assertGreaterEqual(int(event_metrics.get("post_ingest_prefetch_elapsed_ms") or 0), 0)
        self.assertEqual(
            event_metrics.get("next_submit_attempt_semantics"),
            "refill_daemon_signal_only",
        )
        self.assertTrue(str(event_metrics.get("refill_daemon_signal_started_at") or ""))
        self.assertTrue(str(event_metrics.get("refill_daemon_signal_finished_at") or ""))
        refill_trigger = dict(event_payloads[-1].get("profile_refill_trigger") or {})
        self.assertEqual(refill_trigger["kind"], "profile_prefetch_refill_trigger")
        self.assertEqual(refill_trigger["trigger_kind"], "provider_completion")
        self.assertEqual(refill_trigger["trigger_reason"], "provider_completion")
        self.assertEqual(refill_trigger["trigger_source"], "worker_completion_callback")
        self.assertEqual(refill_trigger["item_store"], "linkedin_profile_registry")
        self.assertEqual(refill_trigger["requested_url_count"], 0)
        self.assertEqual(refill_trigger["dispatched_url_count"], 0)
        self.assertEqual(refill_trigger["next_submit_owner"], "profile_refill_daemon")
        self.assertTrue(refill_trigger["signal_only"])
        self.assertTrue(refill_trigger["provider_submit_deferred_to_refill_daemon"])
        self.assertFalse(refill_trigger["direct_refill_enabled"])
        self.assertTrue(refill_trigger["refill_daemon_signal_only"])

    def test_harvest_profile_completion_callback_coalesces_completed_workers_before_prefetch(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_harvest_completion_event_coalesces"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-harvest-event-coalesces"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_ids: list[int] = []
        for index in range(2):
            profile_url = f"https://www.linkedin.com/in/openai-event-coalesce-{index}/"
            handle = self.orchestrator.agent_runtime.begin_worker(
                job_id=job_id,
                request=request,
                plan_payload=plan_payload,
                runtime_mode="workflow",
                lane_id="enrichment_specialist",
                worker_key=f"harvest_profile_batch::event-coalesce-{index}",
                stage="enriching",
                span_name=f"harvest_profile_batch:event-coalesce-{index}",
                budget_payload={"requested_url_count": 1},
                input_payload={"profile_urls": [profile_url]},
                metadata={
                    "recovery_kind": "harvest_profile_batch",
                    "snapshot_dir": str(snapshot_dir),
                    "profile_urls": [profile_url],
                    "request_payload": request.to_record(),
                    "plan_payload": plan_payload,
                    "runtime_mode": "workflow",
                },
                handoff_from_lane="acquisition_specialist",
            )
            self.orchestrator.agent_runtime.complete_worker(
                handle,
                status="completed",
                checkpoint_payload={"stage": "completed", "recovery_kind": "harvest_profile_batch"},
                output_payload={"summary": {"status": "completed", "requested_urls": [profile_url]}},
            )
            worker_ids.append(handle.worker_id)

        def _fake_apply(**kwargs):
            pending_workers = list(kwargs.get("pending_workers") or [])
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [int(worker.get("worker_id") or 0) for worker in pending_workers],
                "candidate_ids": [],
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_profile_prefetch_refill_queue_once",
                side_effect=AssertionError("completion callback must not run registry refill inline"),
            ) as refill_mock,
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_harvest_prefetch_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                return_value={
                    "status": "deferred",
                    "reason": "same_kind_background_workers_still_inflight",
                },
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_ids[0], "worker_status": "completed"}
            )
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_ids[1], "worker_status": "completed"}
            )

        refill_mock.assert_not_called()
        event_payloads = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if "completion event" in str(event.get("detail") or "")
        ]
        self.assertEqual(len(event_payloads), 1)
        self.assertEqual(
            sorted(int(item) for item in list(event_payloads[0].get("worker_ids") or [])),
            sorted(worker_ids),
        )
        for worker_id in worker_ids:
            worker = self.store.get_agent_worker(worker_id=worker_id)
            assert worker is not None
            inline_ingest = dict(dict(worker.get("output") or {}).get("inline_incremental_ingest") or {})
            self.assertEqual(sorted(int(item) for item in list(inline_ingest.get("applied_worker_ids") or [])), sorted(worker_ids))

    def test_completed_company_roster_reconcile_prefetch_failure_leaves_worker_repickable(self) -> None:
        """Pass-3 contract: completed-job company_roster reconcile must skip Phase C and the
        gating ingest marker when Phase B prefetch fails retryably; the worker keeps only its
        `inline_incremental_apply` marker and is re-pickable on the next recovery tick.
        """

        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_company_roster_prefetch_failure"
        snapshot_dir = (
            self.settings.company_assets_dir / "openai" / "snapshot-completed-company-roster-prefetch-failure"
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_id = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        sync_calls = 0

        def _fake_sync(**_kwargs):
            nonlocal sync_calls
            sync_calls += 1
            return {"status": "completed"}

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                return_value={
                    "status": "applied",
                    "snapshot_id": snapshot_dir.name,
                    "worker_ids": [worker_id],
                    "candidate_ids": [],
                    "roster_snapshot": None,
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                side_effect=RuntimeError("simulated_provider_outage"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"job_id": job_id, "status": "completed", "artifact_path": str(artifact_path)},
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertEqual(
            sync_calls, 0, "Phase C sync must NOT run when Phase B prefetch failed retryably"
        )
        worker_after = self.store.get_agent_worker(worker_id=worker_id)
        assert worker_after is not None
        output = dict(worker_after.get("output") or {})
        self.assertFalse(
            dict(output.get("inline_incremental_ingest") or {}),
            "completed-job company_roster prefetch failure must NOT write the gating ingest marker",
        )
        apply_marker = dict(output.get("inline_incremental_apply") or {})
        self.assertEqual(
            str(apply_marker.get("snapshot_id") or ""),
            snapshot_dir.name,
            "apply marker persists so Phase A short-circuits on retry",
        )

    def test_completed_search_seed_reconcile_prefetch_failure_leaves_worker_repickable(self) -> None:
        """Pass-3 contract: completed-job search_seed reconcile must skip Phase C and the
        gating ingest marker when Phase B prefetch fails retryably.
        """

        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_search_seed_prefetch_failure"
        snapshot_dir = (
            self.settings.company_assets_dir / "openai" / "snapshot-completed-search-seed-prefetch-failure"
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"job_id": job_id, "summary": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {"snapshot_id": snapshot_dir.name},
                "background_reconcile": {},
            },
            artifact_path=str(artifact_path),
        )
        worker_id = self._seed_search_seed_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        sentinel_search_seed_snapshot = SearchSeedSnapshot.__new__(SearchSeedSnapshot)
        sync_calls = 0

        def _fake_sync(**_kwargs):
            nonlocal sync_calls
            sync_calls += 1
            return {"status": "completed"}

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_search_seed_workers_to_snapshot",
                return_value={
                    "status": "applied",
                    "snapshot_id": snapshot_dir.name,
                    "worker_ids": [worker_id],
                    "candidate_ids": [],
                    "search_seed_snapshot": sentinel_search_seed_snapshot,
                },
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_search_seed_snapshot",
                side_effect=RuntimeError("simulated_provider_outage"),
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"job_id": job_id, "status": "completed", "artifact_path": str(artifact_path)},
            ),
        ):
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )

        self.assertEqual(
            sync_calls, 0, "Phase C sync must NOT run when Phase B prefetch failed retryably"
        )
        worker_after = self.store.get_agent_worker(worker_id=worker_id)
        assert worker_after is not None
        output = dict(worker_after.get("output") or {})
        self.assertFalse(
            dict(output.get("inline_incremental_ingest") or {}),
            "completed-job search_seed prefetch failure must NOT write the gating ingest marker",
        )
        apply_marker = dict(output.get("inline_incremental_apply") or {})
        self.assertEqual(
            str(apply_marker.get("snapshot_id") or ""),
            snapshot_dir.name,
            "apply marker persists so Phase A short-circuits on retry",
        )

    def test_company_roster_running_job_prefetch_failure_leaves_worker_repickable(self) -> None:
        """Pass-3 contract: Phase B prefetch failure (retryable=True) short-circuits Phase C
        and does NOT write the gating `inline_incremental_ingest` marker. The next recovery
        tick re-picks the worker, Phase A short-circuits on the apply marker, Phase B retries
        and succeeds, Phase C runs.
        """

        request_payload = {
            "raw_user_request": "Find OpenAI infra people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_company_roster_prefetch_failure_recovery"
        snapshot_dir = (
            self.settings.company_assets_dir / "openai" / "snapshot-prefetch-failure-recovery"
        )
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        worker_id = self._seed_company_roster_inline_worker(
            job_id=job_id,
            snapshot_dir=snapshot_dir,
            request=request,
            plan_payload=plan_payload,
        )

        apply_call_count = 0

        def _fake_apply(**_kwargs):
            nonlocal apply_call_count
            apply_call_count += 1
            return {
                "status": "applied",
                "snapshot_id": snapshot_dir.name,
                "worker_ids": [worker_id],
                "candidate_ids": [],
                "roster_snapshot": None,
            }

        first_call = {"value": True}

        def _fake_queue(**_kwargs):
            if first_call["value"]:
                first_call["value"] = False
                raise RuntimeError("simulated_provider_outage")
            return {"status": "queued", "queued_worker_count": 0, "dispatched_url_count": 0}

        sync_call_count = 0

        def _fake_sync(**_kwargs):
            nonlocal sync_call_count
            sync_call_count += 1
            return {
                "status": "deferred",
                "reason": "same_kind_background_workers_still_inflight",
                "writer_scope": "job",
                "sync_policy": "same_kind_micro_batch_single_writer",
            }

        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_apply_background_company_roster_workers_to_snapshot",
                side_effect=_fake_apply,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_queue_background_profile_prefetch_from_available_baselines",
                side_effect=_fake_queue,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_inline_incremental_sync_for_running_job",
                side_effect=_fake_sync,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_persist_running_job_inline_reconcile_state",
                return_value=None,
            ),
        ):
            # First tick: Phase B prefetch raises with retryable=True; pass-3 contract
            # short-circuits Phase C and does NOT write the gating ingest marker.
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )
            self.assertEqual(
                apply_call_count, 1, "apply must run once on the first tick"
            )
            self.assertEqual(
                sync_call_count,
                0,
                "Phase C sync must NOT run when Phase B failed retryably",
            )

            worker = self.store.get_agent_worker(worker_id=worker_id)
            assert worker is not None
            output = dict(worker.get("output") or {})
            apply_marker = dict(output.get("inline_incremental_apply") or {})
            self.assertEqual(
                str(apply_marker.get("snapshot_id") or ""),
                snapshot_dir.name,
                "apply marker must persist after Phase B prefetch failure",
            )
            ingest_marker = dict(output.get("inline_incremental_ingest") or {})
            self.assertFalse(
                ingest_marker,
                "the gating `inline_incremental_ingest` marker must NOT be written when "
                "Phase B prefetch failed retryably; otherwise the collector would skip "
                "the worker permanently",
            )

            # Second tick: collector re-picks the worker (no ingest marker), Phase A finds
            # the apply marker and skips re-apply, Phase B succeeds, Phase C sync runs.
            self.orchestrator._handle_completed_recovery_worker_result(
                {"worker_id": worker_id, "worker_status": "completed"}
            )
            self.assertEqual(
                apply_call_count,
                1,
                "Phase A apply must short-circuit on the apply marker; do not re-apply",
            )
            self.assertEqual(
                sync_call_count, 1, "Phase C sync must run on the second tick"
            )

            worker_after = self.store.get_agent_worker(worker_id=worker_id)
            assert worker_after is not None
            ingest_marker_after = dict(
                dict(worker_after.get("output") or {}).get("inline_incremental_ingest") or {}
            )
            self.assertEqual(
                str(ingest_marker_after.get("snapshot_id") or ""),
                snapshot_dir.name,
                "after the recovery tick succeeds, the gating ingest marker is finally written",
            )

    def test_search_seed_worker_completion_event_closes_discovery_item_before_local_apply(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_search_seed_discovery_item_completion_event"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-search-seed-discovery-completion"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="acquiring",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"message": "Acquiring"},
        )
        handle = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="search_planner",
            worker_key="targeted_people_search::01",
            stage="acquiring",
            span_name="search_bundle:targeted_people_search",
            budget_payload={"max_results": 10},
            input_payload={
                "query_spec": {
                    "query": "OpenAI Agent",
                    "bundle_id": "targeted_people_search",
                    "source_family": "linkedin_people_search",
                    "execution_mode": "web_search",
                },
                "index": 1,
            },
            metadata={
                "recovery_kind": "search_seed_discovery",
                "index": 1,
                "snapshot_dir": str(snapshot_dir),
                "discovery_dir": str(discovery_dir),
                "employment_status": "current",
                "request_payload": request.to_record(),
                "plan_payload": plan_payload,
                "runtime_mode": "workflow",
            },
            handoff_from_lane="triage_planner",
        )
        self.orchestrator.agent_runtime.complete_worker(
            handle,
            status="completed",
            checkpoint_payload={"stage": "completed", "recovery_kind": "search_seed_discovery"},
            output_payload={
                "summary": {
                    "query": "OpenAI Agent",
                    "bundle_id": "targeted_people_search",
                    "source_family": "linkedin_people_search",
                    "execution_mode": "web_search",
                    "mode": "web_search",
                    "status": "completed",
                    "seed_entry_count": 1,
                },
                "entries": [
                    {
                        "seed_key": "agent-person",
                        "full_name": "Agent Person",
                        "source_type": "web_search",
                        "source_query": "OpenAI Agent",
                        "profile_url": "https://www.linkedin.com/in/agent-person/",
                    }
                ],
                "errors": [],
            },
        )

        result = self.orchestrator._enqueue_local_apply_closure_item_for_completed_worker_result(
            {"worker_id": handle.worker_id, "worker_status": "completed", "source": "unit_test"}
        )

        self.assertEqual(result["status"], "enqueued")
        discovery_items = self.store.list_job_materialization_items(
            job_id=job_id,
            item_kind="search_seed_discovery_query",
            statuses=["completed"],
        )
        self.assertEqual(len(discovery_items), 1)
        self.assertEqual(discovery_items[0]["source"], "search_seed_discovery_worker_completion_event")
        # CALIBRATED 2026-07-22 (wave 2): local-apply closures are durable
        # workflow COMMANDS now (command_payload_storage=workflow_commands via
        # the legacy-materialization adapter), not materialization items.
        commands = self.store.list_workflow_commands(
            workflow_run_id=legacy_job_workflow_run_id(job_id)
        )
        local_apply_commands = [
            command
            for command in commands
            if str(command.get("command_type") or "") == LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE
            and str(command.get("status") or "") == "queued"
        ]
        self.assertEqual(len(local_apply_commands), 1)
        materialization_metadata = dict(
            dict(local_apply_commands[0].get("payload") or {}).get("materialization_metadata") or {}
        )
        self.assertEqual(
            dict(materialization_metadata.get("search_seed_discovery_item") or {}).get("item_id"),
            discovery_items[0]["item_id"],
        )


if __name__ == "__main__":
    unittest.main()
