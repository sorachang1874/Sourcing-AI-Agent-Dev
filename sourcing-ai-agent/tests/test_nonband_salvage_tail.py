"""Non-band salvage tail — final round-2 port wave (all remaining groups).

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-lists NONBAND_OWNERSHIP_R2_SHARD_{A,B}_2026-07-22.md
groups F/K/L/M/N/G3-G6/G8/G9/J): public-web two_stage opt-in planning +
stage-2 execution, runtime watchdog family, disk-state hydration ladder,
parallel former-seed join-on-failure, reuse->live fallback, non-member
materialized profiles, cached-profile replay before final sync, nested
reconcile coalescing, manual-review snapshot scoping, registry
state-downgrade fence, artifact build-profile selection and the legacy
bootstrap-store opt-in gate get their modern home. Ported verbatim onto the
repo-standard PG fixture with two ride-along helpers. Old->new mapping in
docs/governance/REGRESSION_INDEX.md; freeze ratchet shrinks same-change.
"""

import json
import os
import tempfile
import threading
import time
import unittest
import unittest.mock
from dataclasses import replace
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine, AcquisitionExecution
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_paths import canonicalize_company_key
from sourcing_agent.company_registry import normalize_company_key
from sourcing_agent.connectors import CompanyIdentity, CompanyRosterSnapshot
from sourcing_agent.company_shard_planning import build_default_company_employee_shard_policy
from sourcing_agent.domain import AcquisitionTask, Candidate, JobRequest
from sourcing_agent.enrichment import MultiSourceEnrichmentResult
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.cli import run_server_runtime_watchdog_once
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.planning import build_sourcing_plan
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


class NonBandSalvageTailTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def test_execute_retrieval_uses_legacy_bootstrap_store_only_when_explicitly_opted_in(self) -> None:
        self.store.replace_bootstrap_data(
            [
                Candidate(
                    candidate_id="reflection_emp_1",
                    name_en="Infra Lead",
                    display_name="Infra Lead",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Head of Infrastructure",
                    focus_areas="infra systems",
                ),
                Candidate(
                    candidate_id="openai_emp_1",
                    name_en="Other Infra",
                    display_name="Other Infra",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Infrastructure Engineer",
                    focus_areas="infra systems",
                ),
            ],
            [],
        )

        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我寻找Reflection AI的Infra方向成员",
                "target_company": "Reflection AI",
                "keywords": ["infra"],
                "top_k": 5,
                "execution_preferences": {"allow_local_bootstrap_fallback": True},
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        with unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_ENABLE_BOOTSTRAP_CANDIDATE_STORE": "1"},
            clear=False,
        ):
            artifact = self.orchestrator._execute_retrieval(
                job_id="bootstrap_company_scoped",
                request=request,
                plan=plan,
                job_type="workflow",
                persist_job_state=False,
            )

        self.assertEqual(artifact["summary"]["candidate_source"]["source_kind"], "legacy_bootstrap_store")
        self.assertEqual(artifact["summary"]["candidate_source"]["candidate_count"], 1)
        self.assertEqual([item["candidate_id"] for item in artifact["matches"]], ["reflection_emp_1"])

    def test_build_sourcing_plan_omits_public_web_stage_by_default(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的基础设施成员",
                "target_company": "Reflection AI",
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        task_types = [task.task_type for task in plan.acquisition_tasks]
        self.assertIn("acquire_former_search_seed", task_types)
        self.assertIn("enrich_linkedin_profiles", task_types)
        self.assertNotIn("enrich_public_web_signals", task_types)
        former_task = next(task for task in plan.acquisition_tasks if task.task_type == "acquire_former_search_seed")
        self.assertEqual(former_task.metadata["acquisition_phase"], "linkedin_stage_1")
        self.assertEqual(former_task.metadata["strategy_type"], "former_employee_search")
        search_bundles = list(plan.search_strategy.query_bundles or [])
        self.assertFalse(
            [
                bundle.bundle_id
                for bundle in search_bundles
                if bundle.source_family in {"public_web_search", "publication_and_blog", "public_interviews"}
            ]
        )
        self.assertTrue(
            all(
                bundle.execution_mode == "paid_fallback"
                or bundle.source_family in {"linkedin_people_search", "targeted_people_search"}
                for bundle in search_bundles
            )
        )

    def test_build_sourcing_plan_includes_public_web_stage_when_two_stage_enabled(self) -> None:
        request = JobRequest.from_payload(
            {
                "raw_user_request": "帮我找 Reflection AI 的基础设施成员",
                "target_company": "Reflection AI",
                "analysis_stage_mode": "two_stage",
            }
        )
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        task_types = [task.task_type for task in plan.acquisition_tasks]
        self.assertIn("enrich_public_web_signals", task_types)
        self.assertLess(task_types.index("enrich_linkedin_profiles"), task_types.index("enrich_public_web_signals"))
        public_web_task = next(task for task in plan.acquisition_tasks if task.task_type == "enrich_public_web_signals")
        self.assertEqual(public_web_task.metadata["acquisition_phase"], "public_web_stage_2")

    def test_ensure_hosted_runtime_watchdog_starts_sidecar_process(self) -> None:
        captured: dict[str, object] = {}

        class FakeProcess:
            pid = 97531

            def poll(self):  # type: ignore[no-untyped-def]
                return None

        def fake_popen(command, **kwargs):  # type: ignore[no-untyped-def]
            captured["command"] = command
            captured["kwargs"] = kwargs
            return FakeProcess()

        with (
            unittest.mock.patch(
                "sourcing_agent.process_supervision.subprocess.Popen",
                side_effect=fake_popen,
            ),
            unittest.mock.patch(
                "sourcing_agent.process_supervision.time.sleep",
                return_value=None,
            ),
            unittest.mock.patch(
                "sourcing_agent.orchestrator.read_service_status",
                side_effect=[
                    {"service_name": "server-runtime-watchdog", "status": "not_started", "lock_status": "missing"},
                    {"service_name": "server-runtime-watchdog", "status": "running", "lock_status": "locked"},
                ],
            ),
        ):
            result = self.orchestrator.ensure_hosted_runtime_watchdog(
                {
                    "auto_job_daemon": True,
                    "hosted_runtime_watchdog_poll_seconds": 9.0,
                }
            )

        self.assertEqual(result["status"], "started")
        self.assertEqual(result["mode"], "sidecar")
        self.assertEqual(result["scope"], "hosted_runtime_watchdog")
        self.assertEqual(result["pid"], 97531)
        self.assertEqual(result["handshake"]["status"], "ready")
        command = list(captured["command"])
        self.assertIn("run-server-runtime-watchdog-service", command)
        self.assertIn("--service-name", command)
        self.assertIn("--shared-service-name", command)
        self.assertIn("server-runtime-watchdog", command)
        self.assertIn("worker-recovery-daemon", command)

    def test_server_runtime_watchdog_skips_shared_restart_when_service_ready(self) -> None:
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_hosted_runtime_watchdog_once",
            return_value={
                "status": "completed",
                "mode": "hosted",
                "worker_recovery": {"status": "completed"},
                "hosted_dispatch": [{"job_id": "job-1", "status": "started"}],
            },
        ) as hosted_mock:
            result = run_server_runtime_watchdog_once(self.orchestrator)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["mode"], "hosted")
        hosted_mock.assert_called_once()

    def test_hosted_runtime_watchdog_reports_retired_shadow_pg_sync(self) -> None:
        # B4.3f: the shadow-sourced watchdog sync is retired — since B4.1 it mirrored an
        # empty in-memory DB. The watchdog must report the retired status and never call
        # the SQLite->PG sync routine.
        with unittest.mock.patch.object(
            self.orchestrator,
            "run_worker_recovery_once",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_runtime_metrics_snapshot",
            return_value={"status": "ok", "observed_at": "2026-04-21T00:00:00Z"},
        ):
            result = self.orchestrator.run_hosted_runtime_watchdog_once(
                {"control_plane_postgres_dsn": "postgresql://demo/demo"}
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(
            result["control_plane_postgres_sync"],
            {"status": "retired", "reason": "sqlite_shadow_retired_b4_3f"},
        )

    def test_scoped_parallel_former_seed_is_joined_when_current_lane_fails(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-scoped-former-join-on-failure"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        former_started = threading.Event()
        former_finished = threading.Event()

        def _fake_current_discover(*args, **kwargs):  # noqa: ANN002, ANN003
            self.assertTrue(former_started.wait(timeout=1.0))
            raise RuntimeError("current lane failed")

        def _fake_former_search(*args, **kwargs):  # noqa: ANN002, ANN003
            former_started.set()
            time.sleep(0.2)
            former_finished.set()
            return AcquisitionExecution(
                task_id="acquire-search-seed-pool-former-search-seed",
                status="completed",
                detail="Former search seed ready.",
                payload={},
                state_updates={},
            )

        task = AcquisitionTask(
            task_id="acquire-search-seed-pool",
            task_type="acquire_search_seed_pool",
            title="Acquire scoped search seeds",
            description="Acquire current and former search lanes.",
            status="ready",
            metadata={
                "strategy_type": "scoped_search_roster",
                "include_former_search_seed": True,
                "search_seed_queries": ["infra"],
                "employment_statuses": ["current", "former"],
            },
        )
        started_at = time.monotonic()
        with (
            unittest.mock.patch.object(
                self.acquisition_engine.search_seed_acquirer,
                "discover",
                side_effect=_fake_current_discover,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                side_effect=_fake_former_search,
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "current lane failed"):
                self.acquisition_engine._acquire_search_seed_pool(
                    task,
                    {
                        "company_identity": identity,
                        "snapshot_dir": snapshot_dir,
                        "job_id": "job_scoped_parallel_former_join_on_failure",
                        "plan_payload": {},
                        "runtime_mode": "workflow",
                    },
                    JobRequest(
                        raw_user_request="Find Reflection AI infra members",
                        target_company="Reflection AI",
                        categories=["employee", "former_employee"],
                    ),
                )

        self.assertTrue(former_finished.is_set())
        self.assertGreaterEqual(time.monotonic() - started_at, 0.15)
        leaked_threads = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("acquisition-scoped-former-seed")
        ]
        self.assertEqual(leaked_threads, [])

    def test_full_roster_parallel_former_seed_is_joined_when_roster_lane_fails(self) -> None:
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflectionai",
            linkedin_company_url="https://www.linkedin.com/company/reflectionai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "reflectionai" / "snapshot-roster-former-join-on-failure"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        former_started = threading.Event()
        former_finished = threading.Event()

        def _fake_roster_fetch(*args, **kwargs):  # noqa: ANN002, ANN003
            self.assertTrue(former_started.wait(timeout=1.0))
            raise RuntimeError("roster lane failed")

        def _fake_former_search(*args, **kwargs):  # noqa: ANN002, ANN003
            former_started.set()
            time.sleep(0.2)
            former_finished.set()
            return AcquisitionExecution(
                task_id="acquire-full-roster-former-search-seed",
                status="completed",
                detail="Former search seed ready.",
                payload={},
                state_updates={},
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire company roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "include_former_search_seed": True,
                "cost_policy": {"allow_company_employee_api": False, "allow_cached_roster_fallback": False},
                # pgLegacy deletion (2026-07-23): mirror the planner's minted
                # unified policy — policy-less roster tasks now fail closed.
                "company_employee_shard_policy": build_default_company_employee_shard_policy(
                    max_pages=10, page_limit=50
                ),
            },
        )
        started_at = time.monotonic()
        with (
            unittest.mock.patch.object(
                self.acquisition_engine.roster_connector,
                "fetch_company_roster",
                side_effect=_fake_roster_fetch,
            ),
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_acquire_default_former_search_seed",
                side_effect=_fake_former_search,
            ),
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                    "job_id": "job_full_roster_parallel_former_join_on_failure",
                    "plan_payload": {},
                    "runtime_mode": "workflow",
                },
                JobRequest(
                    raw_user_request="Find Reflection AI infra members",
                    target_company="Reflection AI",
                    categories=["employee", "former_employee"],
                ),
            )

        self.assertEqual(execution.status, "blocked")
        self.assertTrue(former_finished.is_set())
        self.assertGreaterEqual(time.monotonic() - started_at, 0.15)
        leaked_threads = [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("acquisition-former-seed")
        ]
        self.assertEqual(leaked_threads, [])

    def test_acquire_full_roster_reuse_request_falls_back_to_live_when_no_cached_roster(self) -> None:
        identity = CompanyIdentity(
            requested_name="Lovable",
            canonical_name="Lovable",
            company_key="lovable",
            linkedin_slug="lovable",
            linkedin_company_url="https://www.linkedin.com/company/lovable/",
        )
        snapshot_dir = self.settings.company_assets_dir / "lovable" / "snapshot-reuse-miss-live-roster"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        self.acquisition_engine.harvest_company_connector.settings = replace(
            self.acquisition_engine.harvest_company_connector.settings,
            enabled=True,
            api_token="token",
            actor_id="actor",
        )
        seen: dict[str, object] = {}

        def _fake_fetch_company_roster(
            _identity,
            _snapshot_dir,
            *,
            asset_logger=None,
            max_pages=10,
            page_limit=50,
            company_filters=None,
            allow_shared_provider_cache=True,
        ):
            seen["called"] = True
            seen["allow_shared_provider_cache"] = allow_shared_provider_cache
            roster_dir = snapshot_dir / "harvest_company_employees"
            roster_dir.mkdir(parents=True, exist_ok=True)
            merged_path = roster_dir / "harvest_company_employees_merged.json"
            visible_path = roster_dir / "harvest_company_employees_visible.json"
            headless_path = roster_dir / "harvest_company_employees_headless.json"
            summary_path = roster_dir / "harvest_company_employees_summary.json"
            entry = {
                "full_name": "Ada Lovable",
                "headline": "Engineer at Lovable",
                "linkedin_url": "https://www.linkedin.com/in/ada-lovable/",
            }
            merged_path.write_text(json.dumps([entry]), encoding="utf-8")
            visible_path.write_text(json.dumps([entry]), encoding="utf-8")
            headless_path.write_text("[]", encoding="utf-8")
            summary_path.write_text(json.dumps({"visible_entry_count": 1}), encoding="utf-8")
            return CompanyRosterSnapshot(
                snapshot_id=snapshot_dir.name,
                target_company="Lovable",
                company_identity=identity,
                snapshot_dir=snapshot_dir,
                raw_entries=[entry],
                visible_entries=[entry],
                headless_entries=[],
                page_summaries=[{"page": 1, "entry_count": 1}],
                accounts_used=["harvest_company_employees"],
                errors=[],
                stop_reason="completed",
                merged_path=merged_path,
                visible_path=visible_path,
                headless_path=headless_path,
                summary_path=summary_path,
            )

        task = AcquisitionTask(
            task_id="acquire-full-roster",
            task_type="acquire_full_roster",
            title="Acquire roster",
            description="Acquire company roster",
            status="ready",
            blocking=True,
            metadata={
                "strategy_type": "full_company_roster",
                "include_former_search_seed": False,
                "cost_policy": {"allow_company_employee_api": True},
                # pgLegacy deletion (2026-07-23): mirror the planner's minted
                # unified policy — policy-less roster tasks now fail closed.
                "company_employee_shard_policy": build_default_company_employee_shard_policy(
                    max_pages=10, page_limit=50
                ),
            },
        )
        with unittest.mock.patch.object(
            type(self.acquisition_engine.harvest_company_connector),
            "fetch_company_roster",
            side_effect=_fake_fetch_company_roster,
        ):
            execution = self.acquisition_engine._acquire_full_roster(
                task,
                {
                    "company_identity": identity,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="基于现有 roster 或实时获取 Lovable 全部成员",
                    target_company="Lovable",
                    categories=["employee"],
                    execution_preferences={"reuse_existing_roster": True},
                ),
            )

        self.assertEqual(execution.status, "completed")
        self.assertTrue(seen.get("called"))
        self.assertEqual(execution.payload["acquisition_mode"], "live_roster_acquisition")
        self.assertTrue(execution.payload["reuse_existing_roster_miss"])
        self.assertEqual(execution.payload["reuse_existing_roster_miss_reason"], "no_cached_roster_snapshot")

    def test_enrich_profiles_hydrates_disk_roster_when_state_has_search_seed_only(self) -> None:
        identity = CompanyIdentity(
            requested_name="Wispr Flow",
            canonical_name="Wispr Flow",
            company_key="wisprflow",
            linkedin_slug="wispr-flow",
            linkedin_company_url="https://www.linkedin.com/company/wispr-flow/",
        )
        snapshot_dir = self.settings.company_assets_dir / "wisprflow" / "snapshot-hydrate-roster"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        roster_dir = snapshot_dir / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)
        roster_summary_path = roster_dir / "harvest_company_employees_summary.json"
        roster_visible_path = roster_dir / "harvest_company_employees_visible.json"
        roster_merged_path = roster_dir / "harvest_company_employees_merged.json"
        roster_headless_path = roster_dir / "harvest_company_employees_headless.json"
        roster_row = {
            "full_name": "Current Wispr",
            "headline": "Engineer at Wispr Flow",
            "location": "San Francisco Bay Area",
            "linkedin_url": "https://www.linkedin.com/in/current-wispr/",
            "member_key": "current-wispr",
        }
        roster_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "completion_status": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        roster_visible_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_merged_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_headless_path.write_text("[]", encoding="utf-8")

        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        seed_summary_path = discovery_dir / "summary.json"
        seed_entries_path = discovery_dir / "entries.json"
        seed_entry = {
            "full_name": "Former Wispr",
            "headline": "Former ML Engineer at Wispr Flow",
            "profile_url": "https://www.linkedin.com/in/former-wispr/",
            "employment_status": "former",
            "source_type": "harvest_profile_search",
        }
        seed_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        seed_entries_path.write_text(json.dumps([seed_entry], ensure_ascii=False), encoding="utf-8")
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Wispr Flow",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[seed_entry],
            query_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            summary_path=seed_summary_path,
            entries_path=seed_entries_path,
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        seen_candidate_names: list[str] = []

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            seen_candidate_names.extend(candidate.display_name for candidate in candidates_arg)
            return MultiSourceEnrichmentResult(candidates=list(candidates_arg), evidence=[])

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            execution = self.acquisition_engine._enrich_profiles(
                AcquisitionTask(
                    task_id="enrich-profiles",
                    task_type="enrich_profiles_multisource",
                    title="Enrich profiles",
                    description="Run profile enrichment",
                    status="ready",
                    blocking=True,
                    metadata={"cost_policy": {}, "enrichment_scope": "linkedin_stage_1"},
                ),
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="帮我找 Wispr Flow 的全部成员",
                    target_company="Wispr Flow",
                    categories=["employee", "former_employee"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 2)
        self.assertEqual(set(seen_candidate_names), {"Current Wispr", "Former Wispr"})
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertEqual(candidate_doc["candidate_count"], 2)
        self.assertIn("roster_snapshot", candidate_doc["acquisition_sources"])
        self.assertIn("search_seed_snapshot", candidate_doc["acquisition_sources"])

    def test_enrich_profiles_hydrates_disk_search_seed_when_state_has_roster_only(self) -> None:
        identity = CompanyIdentity(
            requested_name="Wispr Flow",
            canonical_name="Wispr Flow",
            company_key="wisprflow",
            linkedin_slug="wispr-flow",
            linkedin_company_url="https://www.linkedin.com/company/wispr-flow/",
        )
        snapshot_dir = self.settings.company_assets_dir / "wisprflow" / "snapshot-hydrate-search-seed"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        roster_dir = snapshot_dir / "harvest_company_employees"
        roster_dir.mkdir(parents=True, exist_ok=True)
        roster_summary_path = roster_dir / "harvest_company_employees_summary.json"
        roster_visible_path = roster_dir / "harvest_company_employees_visible.json"
        roster_merged_path = roster_dir / "harvest_company_employees_merged.json"
        roster_headless_path = roster_dir / "harvest_company_employees_headless.json"
        roster_row = {
            "full_name": "Current Wispr",
            "headline": "Engineer at Wispr Flow",
            "location": "San Francisco Bay Area",
            "linkedin_url": "https://www.linkedin.com/in/current-wispr/",
            "member_key": "current-wispr",
        }
        roster_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "completion_status": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        roster_visible_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_merged_path.write_text(json.dumps([roster_row], ensure_ascii=False), encoding="utf-8")
        roster_headless_path.write_text("[]", encoding="utf-8")
        roster_snapshot = CompanyRosterSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="Wispr Flow",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            raw_entries=[roster_row],
            visible_entries=[roster_row],
            headless_entries=[],
            page_summaries=[],
            accounts_used=[],
            errors=[],
            stop_reason="completed",
            merged_path=roster_merged_path,
            visible_path=roster_visible_path,
            headless_path=roster_headless_path,
            summary_path=roster_summary_path,
        )

        discovery_dir = snapshot_dir / "search_seed_discovery"
        discovery_dir.mkdir(parents=True, exist_ok=True)
        seed_summary_path = discovery_dir / "summary.json"
        seed_entries_path = discovery_dir / "entries.json"
        seed_entry = {
            "full_name": "Former Wispr",
            "headline": "Former ML Engineer at Wispr Flow",
            "profile_url": "https://www.linkedin.com/in/former-wispr/",
            "employment_status": "former",
            "source_type": "harvest_profile_search",
        }
        seed_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "Wispr Flow",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [],
                    "stop_reason": "completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        seed_entries_path.write_text(json.dumps([seed_entry], ensure_ascii=False), encoding="utf-8")
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        seen_candidate_names: list[str] = []

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            seen_candidate_names.extend(candidate.display_name for candidate in candidates_arg)
            return MultiSourceEnrichmentResult(candidates=list(candidates_arg), evidence=[])

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            execution = self.acquisition_engine._enrich_profiles(
                AcquisitionTask(
                    task_id="enrich-profiles",
                    task_type="enrich_profiles_multisource",
                    title="Enrich profiles",
                    description="Run profile enrichment",
                    status="ready",
                    blocking=True,
                    metadata={"cost_policy": {}, "enrichment_scope": "linkedin_stage_1"},
                ),
                {
                    "roster_snapshot": roster_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="帮我找 Wispr Flow 的全部成员",
                    target_company="Wispr Flow",
                    categories=["employee", "former_employee"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 2)
        self.assertEqual(set(seen_candidate_names), {"Current Wispr", "Former Wispr"})
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertEqual(candidate_doc["candidate_count"], 2)
        self.assertIn("roster_snapshot", candidate_doc["acquisition_sources"])
        self.assertIn("search_seed_snapshot", candidate_doc["acquisition_sources"])

    def test_enrich_profiles_hydrates_disk_search_seed_lane_when_state_has_partial_scoped_seed(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-hydrate-multi-scoped-search"
        discovery_dir = snapshot_dir / "search_seed_discovery"
        current_lane_dir = discovery_dir / "current"
        current_lane_dir.mkdir(parents=True, exist_ok=True)
        in_memory_summary_path = discovery_dir / "summary.json"
        in_memory_entries_path = discovery_dir / "entries.json"
        in_memory_summary_path.write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "entry_count": 1,
                    "query_summaries": [{"query": "OpenAI Agent", "status": "completed"}],
                    "stop_reason": "partial_agent_worker_completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        agent_entry = {
            "seed_key": "agent-seed",
            "full_name": "Ari Agent",
            "headline": "Agent Research at OpenAI",
            "profile_url": "https://www.linkedin.com/in/ari-agent/",
            "employment_status": "current",
            "source_query": "OpenAI Agent",
            "source_type": "harvest_profile_search",
        }
        in_memory_entries_path.write_text(json.dumps([agent_entry], ensure_ascii=False), encoding="utf-8")
        multimodal_entry = {
            "seed_key": "multimodal-seed",
            "full_name": "Mira Multimodal",
            "headline": "Multimodal Research at OpenAI",
            "profile_url": "https://www.linkedin.com/in/mira-multimodal/",
            "employment_status": "current",
            "source_query": "OpenAI Multimodal",
            "source_type": "harvest_profile_search",
        }
        (current_lane_dir / "summary.json").write_text(
            json.dumps(
                {
                    "snapshot_id": snapshot_dir.name,
                    "target_company": "OpenAI",
                    "company_identity": identity.to_record(),
                    "employment_scope": "current",
                    "employment_status": "current",
                    "strategy_type": "scoped_search_roster",
                    "entry_count": 1,
                    "query_summaries": [{"query": "OpenAI Multimodal", "status": "completed"}],
                    "stop_reason": "partial_multimodal_worker_completed",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (current_lane_dir / "entries.json").write_text(
            json.dumps([multimodal_entry], ensure_ascii=False),
            encoding="utf-8",
        )
        search_seed_snapshot = SearchSeedSnapshot(
            snapshot_id=snapshot_dir.name,
            target_company="OpenAI",
            company_identity=identity,
            snapshot_dir=snapshot_dir,
            entries=[agent_entry],
            query_summaries=[{"query": "OpenAI Agent", "status": "completed"}],
            accounts_used=[],
            errors=[],
            stop_reason="partial_agent_worker_completed",
            summary_path=in_memory_summary_path,
            entries_path=in_memory_entries_path,
        )
        original_enrich = self.acquisition_engine.multi_source_enricher.enrich
        seen_candidate_names: list[str] = []
        seen_linkedin_urls: list[str] = []

        def _fake_enrich(identity_arg, snapshot_dir_arg, candidates_arg, *args, **kwargs):  # noqa: ARG001
            seen_candidate_names.extend(candidate.display_name for candidate in candidates_arg)
            seen_linkedin_urls.extend(candidate.linkedin_url for candidate in candidates_arg if candidate.linkedin_url)
            return MultiSourceEnrichmentResult(candidates=list(candidates_arg), evidence=[])

        self.acquisition_engine.multi_source_enricher.enrich = _fake_enrich
        try:
            execution = self.acquisition_engine._enrich_profiles(
                AcquisitionTask(
                    task_id="enrich-profiles",
                    task_type="enrich_profiles_multisource",
                    title="Enrich profiles",
                    description="Run profile enrichment",
                    status="ready",
                    blocking=True,
                    metadata={
                        "cost_policy": {},
                        "strategy_type": "scoped_search_roster",
                        "enrichment_scope": "linkedin_stage_1",
                    },
                ),
                {
                    "search_seed_snapshot": search_seed_snapshot,
                    "snapshot_dir": snapshot_dir,
                },
                JobRequest(
                    raw_user_request="帮我找 OpenAI 做 Agent 和 Multimodal 方向的人",
                    target_company="OpenAI",
                    categories=["employee"],
                    keywords=["Agent", "Multimodal"],
                ),
            )
        finally:
            self.acquisition_engine.multi_source_enricher.enrich = original_enrich

        self.assertEqual(execution.status, "completed")
        self.assertEqual(execution.payload["candidate_count"], 2)
        self.assertEqual(set(seen_candidate_names), {"Ari Agent", "Mira Multimodal"})
        self.assertEqual(
            set(seen_linkedin_urls),
            {
                "https://www.linkedin.com/in/ari-agent/",
                "https://www.linkedin.com/in/mira-multimodal/",
            },
        )
        candidate_doc = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
        self.assertEqual(candidate_doc["candidate_count"], 2)
        source_snapshot = candidate_doc["acquisition_sources"]["search_seed_snapshot"]
        self.assertEqual(source_snapshot["entry_count"], 2)

    def test_harvest_profile_apply_candidate_ids_include_non_member_materialized_profiles(self) -> None:
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-non-member-delta-sync"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        profile_url = "https://www.linkedin.com/in/alex-chatgpt/"
        candidate = Candidate(
            candidate_id="alex-chatgpt",
            name_en="Alex ChatGPT",
            display_name="Alex ChatGPT",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="ChatGPT Engineer",
            linkedin_url="",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
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
            full_name="Alex ChatGPT",
            headline="Research Engineer at Anthropic",
            current_company="Anthropic",
            experience=[{"companyName": "Anthropic", "title": "Research Engineer", "current": True}],
        )
        worker = {
            "worker_id": 123,
            "updated_at": "2026-04-30T00:00:00+00:00",
            "metadata": {
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [profile_url],
            },
            "checkpoint": {"run_id": "run-non-member", "dataset_id": "dataset-non-member"},
            "output": {"summary": {"status": "completed", "requested_urls": [profile_url]}},
        }

        with unittest.mock.patch.object(
            self.store.repos.linkedin_profile_registry,
            "mark_fetched",
            side_effect=AssertionError("materialization must not re-upsert already-terminal fetched URLs"),
        ):
            result = self.orchestrator.snapshot_materializer.apply_harvest_profile_workers_to_snapshot(
                snapshot_dir=snapshot_dir,
                pending_workers=[worker],
            )

        self.assertEqual(result["status"], "applied")
        self.assertEqual(result["resolved_candidate_count"], 0)
        self.assertEqual(result["non_member_candidate_count"], 1)
        self.assertEqual(result["candidate_ids"], ["alex-chatgpt"])
        self.assertEqual(result["non_member_candidate_ids"], ["alex-chatgpt"])
        event_records = list(result.get("profile_materialized_candidate_records") or [])
        self.assertEqual(len(event_records), 1)
        event_record = dict(event_records[0])
        self.assertEqual(event_record["candidate_id"], "alex-chatgpt")
        self.assertTrue(event_record.get("experience_lines"))
        self.assertEqual(event_record.get("profile_capture_kind"), "provider_profile_detail")
        self.assertEqual(result["registry_terminal_backfill_requested_count"], 0)
        self.assertEqual(result["registry_terminal_backfill_skipped_count"], 1)

    def test_run_workflow_from_acquisition_publishes_preview_when_blocked_execute_has_open_apply(
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
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = "job_terminal_linkedin_blocked_execute_open_apply"
        snapshot_dir = self.settings.company_assets_dir / "openai" / "snapshot-terminal-linkedin-blocked-execute"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate = Candidate(
            candidate_id="openai-bridge-blocked-execute",
            name_en="OpenAI Bridge Blocked Execute",
            display_name="OpenAI Bridge Blocked Execute",
            category="employee",
            target_company="OpenAI",
            organization="OpenAI",
            employment_status="current",
            role="Infrastructure Engineer",
            linkedin_url="https://www.linkedin.com/in/openai-bridge-blocked-execute/",
        )
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Planning completed."},
        )
        self.store.upsert_job_materialization_item(
            item_id="local_apply_terminal_bridge_blocked_execute",
            job_id=job_id,
            target_company="OpenAI",
            snapshot_id=snapshot_dir.name,
            item_kind="local_apply_closure",
            source="worker_completion_event",
            reason="harvest_profile_batch_completed_needs_local_apply_closure",
            status="queued",
            phase="queued",
            source_worker_ids=[78],
            metadata={"recovery_kind": "harvest_profile_batch", "snapshot_dir": str(snapshot_dir)},
        )

        executed_task_types: list[str] = []
        retrieval_calls: list[str] = []

        def fake_execute_task(task, job_request, target_company, state, bootstrap_summary=None):  # noqa: ARG001
            executed_task_types.append(task.task_type)
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved company identity.",
                    payload={"snapshot_dir": str(snapshot_dir)},
                    state_updates={
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "company_identity": identity,
                    },
                )
            if task.task_type == "acquire_full_roster":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Acquired current candidates.",
                    payload={"candidate_count": 1},
                    state_updates={"candidates": [candidate]},
                )
            if task.task_type == "enrich_linkedin_profiles":
                candidate_doc_path.write_text(
                    json.dumps(
                        {
                            "snapshot": {
                                "snapshot_id": snapshot_dir.name,
                                "company_identity": identity.to_record(),
                            },
                            "acquisition_stage": {
                                "phase": "linkedin_stage_1",
                                "task_id": task.task_id,
                                "task_type": "enrich_linkedin_profiles",
                                "status": "completed",
                                "stage_checkpoint_source": "linkedin_profile_registry_terminal_scope",
                            },
                            "enrichment_scope": "linkedin_stage_1",
                            "enrichment_summary": {
                                "profile_prefetch": {
                                    "status": "completed",
                                    "requested_url_count": 1,
                                    "registry_terminal_summary": {
                                        "requested_url_count": 1,
                                        "terminal_url_count": 1,
                                        "open_url_count": 0,
                                        "all_requested_terminal": True,
                                    },
                                    "profile_prefetch_queue": {
                                        "requested_url_count": 1,
                                        "registry_terminal_url_count": 1,
                                        "registry_open_url_count": 0,
                                        "registry_all_requested_terminal": True,
                                        "terminal_queue_state_leak_count": 0,
                                    },
                                }
                            },
                            "candidates": [candidate.to_record()],
                            "evidence": [],
                            "candidate_count": 1,
                            "evidence_count": 0,
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="blocked",
                    detail="Stage 1 terminal artifact exists; local apply remains open.",
                    payload={"candidate_doc_path": str(candidate_doc_path), "candidate_count": 1},
                    state_updates={
                        "snapshot_id": snapshot_dir.name,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_completed": True,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )
            raise AssertionError(f"unexpected task execution: {task.task_type}")

        def fake_execute_retrieval(job_id_arg, request_arg, plan_arg, **kwargs):  # noqa: ARG001
            runtime_policy = dict(kwargs.get("runtime_policy") or {})
            retrieval_calls.append(str(runtime_policy.get("analysis_stage") or ""))
            return {
                "artifact_path": str(self.settings.jobs_dir / f"{job_id_arg}.preview.json"),
                "summary": {
                    "text": "Stage 1 preview is ready with 1 candidates.",
                    "analysis_stage": str(runtime_policy.get("analysis_stage") or "stage_1_preview"),
                    "total_matches": 1,
                    "returned_matches": 1,
                    "manual_review_queue_count": 0,
                },
                "matches": [],
            }

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "execute_task",
            side_effect=fake_execute_task,
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_execute_retrieval",
            side_effect=fake_execute_retrieval,
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_running_workflow_before_retrieval",
            return_value={"status": "skipped", "reason": "test"},
        ):
            result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(result["status"], "blocked")
        self.assertIn("enrich_linkedin_profiles", executed_task_types)
        self.assertEqual(retrieval_calls, ["stage_1_preview"])
        latest_job = self.store.get_job(job_id) or {}
        summary = dict(latest_job.get("summary") or {})
        stage1_preview = dict(summary.get("stage1_preview") or {})
        self.assertEqual(str(stage1_preview.get("status") or ""), "ready")
        self.assertEqual(str(summary.get("blocked_task") or ""), "enrich_linkedin_profiles")

    def test_completed_workflow_harvest_reconcile_replays_snapshot_cached_profiles_before_final_sync(
        self,
    ) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI Agent people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Agent"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_harvest_reconcile_snapshot_cached_profiles"
        current_url = "https://www.linkedin.com/in/openai-agent-current-final-tail/"
        cached_url = "https://www.linkedin.com/in/openai-agent-current-cached/"
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-completed-harvest-reconcile-snapshot-cached",
            candidates=[
                Candidate(
                    candidate_id="openai-agent-current-final-tail",
                    name_en="Current Final Tail",
                    display_name="Current Final Tail",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Agent Systems Researcher at OpenAI",
                    linkedin_url=current_url,
                    metadata={"seed_source_type": "harvest_profile_search", "seed_query": "OpenAI Agent"},
                ).to_record(),
                Candidate(
                    candidate_id="openai-agent-current-cached",
                    name_en="Current Cached",
                    display_name="Current Cached",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Agent Research Engineer at OpenAI",
                    linkedin_url=cached_url,
                    metadata={"seed_source_type": "harvest_profile_search", "seed_query": "OpenAI Agent"},
                ).to_record(),
            ],
        )
        current_raw_path = self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=current_url,
            full_name="Current Final Tail",
            headline="Agent Systems Researcher at OpenAI",
            current_company="OpenAI",
            experience=[
                {
                    "title": "Agent Systems Researcher",
                    "companyName": "OpenAI",
                    "startDate": {"year": 2024},
                    "endDate": {"text": "Present"},
                }
            ],
        )
        cached_raw_path = self._write_harvest_profile_raw(
            snapshot_dir=snapshot_dir,
            profile_url=cached_url,
            full_name="Current Cached",
            headline="Agent Research Engineer at OpenAI",
            current_company="OpenAI",
            experience=[
                {
                    "title": "Agent Research Engineer",
                    "companyName": "OpenAI",
                    "startDate": {"year": 2023},
                    "endDate": {"text": "Present"},
                }
            ],
        )
        artifact_path = self.settings.jobs_dir / f"{job_id}.json"
        artifact_path.write_text(
            json.dumps(
                {
                    "job_id": job_id,
                    "status": "completed",
                    "request": request.to_record(),
                    "plan": plan_payload,
                    "summary": {
                        "analysis_stage": "stage_2_final",
                        "candidate_source": {
                            "source_kind": "company_snapshot",
                            "snapshot_id": snapshot_dir.name,
                            "candidate_count": 2,
                        },
                    },
                    "matches": [],
                    "manual_review_items": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
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
                "analysis_stage": "stage_2_final",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_dir.name,
                    "candidate_count": 2,
                },
            },
            artifact_path=str(artifact_path),
        )
        worker = self.orchestrator.agent_runtime.begin_worker(
            job_id=job_id,
            request=request,
            plan_payload=plan_payload,
            runtime_mode="workflow",
            lane_id="enrichment_specialist",
            worker_key="harvest_profile_batch::completed-final-tail",
            stage="enriching",
            span_name="harvest_profile_batch:completed-final-tail",
            budget_payload={"requested_url_count": 1},
            input_payload={"profile_urls": [current_url]},
            metadata={
                "recovery_kind": "harvest_profile_batch",
                "snapshot_dir": str(snapshot_dir),
                "profile_urls": [current_url],
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
            output_payload={"summary": {"status": "completed", "requested_urls": [current_url]}},
        )

        sync_result = {
            "status": "completed",
            "reason": "background_harvest_prefetch_reconcile",
            "snapshot_id": snapshot_dir.name,
            "candidate_count": 2,
            "evidence_count": 2,
            "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
            "artifact_paths": {},
            "state_updates": {
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
            },
        }
        with (
            unittest.mock.patch.object(
                self.orchestrator,
                "_handle_harvest_profile_completion_event",
                return_value={"profile_prefetch": {"status": "completed", "dispatched_url_count": 0}},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_synchronize_snapshot_candidate_documents",
                return_value=sync_result,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                return_value={"status": "skipped", "reason": "test"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                return_value={"artifact_path": str(artifact_path), "status": "completed"},
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_outreach_layering_requires_background_reconcile",
                return_value=False,
            ),
        ):
            result = self.orchestrator._reconcile_completed_workflow_if_needed(job_id)

        self.assertEqual(str(result.get("status") or ""), "reconciled_harvest_prefetch")
        reconciled_candidate_doc = json.loads(candidate_doc_path.read_text(encoding="utf-8"))
        reconcile_summary = dict(reconciled_candidate_doc.get("harvest_prefetch_background_reconcile") or {})
        self.assertEqual(int(reconcile_summary.get("requested_url_count") or 0), 1)
        self.assertEqual(int(reconcile_summary.get("reconcile_profile_url_count") or 0), 2)
        self.assertEqual(int(reconcile_summary.get("snapshot_cached_profile_url_count") or 0), 1)
        self.assertTrue(bool(reconcile_summary.get("full_snapshot_cached_profile_reconcile")))
        self.assertEqual(int(reconcile_summary.get("profile_materialized_candidate_count") or 0), 2)
        candidates_by_id = {
            str(item.get("candidate_id") or ""): dict(item)
            for item in list(reconciled_candidate_doc.get("candidates") or [])
        }
        current_candidate = candidates_by_id["openai-agent-current-final-tail"]
        cached_candidate = candidates_by_id["openai-agent-current-cached"]
        self.assertTrue(str(current_candidate.get("work_history") or "").strip())
        self.assertTrue(str(cached_candidate.get("work_history") or "").strip())
        self.assertEqual(str(current_candidate.get("source_path") or ""), str(current_raw_path))
        self.assertEqual(str(cached_candidate.get("source_path") or ""), str(cached_raw_path))

    def test_completed_workflow_reconcile_same_owner_nested_attempt_coalesces(self) -> None:
        request_payload = {
            "raw_user_request": "Find OpenAI workflow runtime people",
            "target_company": "OpenAI",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["workflow"],
            "top_k": 5,
        }
        request = JobRequest.from_payload(request_payload)
        plan_payload = self.orchestrator.plan_workflow(request_payload)["plan"]
        job_id = "job_completed_reconcile_same_owner_nested_attempt"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=request.to_record(),
            plan_payload=plan_payload,
            summary_payload={"analysis_stage": "stage_2_final"},
        )
        calls: list[str] = []

        def _inner_callback() -> dict[str, str]:
            calls.append("inner")
            return {"status": "inner_ran"}

        def _outer_callback() -> dict[str, object]:
            calls.append("outer")
            nested = self.orchestrator._run_completed_workflow_reconcile_with_inflight_slot(
                job_id,
                reconcile_kind="harvest_prefetch",
                snapshot_id="snapshot-nested-attempt",
                worker_ids=[5],
                callback=_inner_callback,
            )
            calls.append(f"nested:{nested.get('status')}:{nested.get('reason')}")
            return {"status": "outer_ran", "nested": nested}

        with unittest.mock.patch("sourcing_agent.storage._utc_now_timestamp", return_value="2026-05-23 00:00:00"):
            result = self.orchestrator._run_completed_workflow_reconcile_with_inflight_slot(
                job_id,
                reconcile_kind="harvest_prefetch",
                snapshot_id="snapshot-nested-attempt",
                worker_ids=[5],
                callback=_outer_callback,
            )

        self.assertEqual(result.get("status"), "outer_ran")
        self.assertEqual(calls, ["outer", "nested:skipped:completed_workflow_reconcile_inflight"])
        structured_events = [
            dict(event.get("payload") or {})
            for event in self.store.list_job_events(job_id)
            if dict(event.get("payload") or {}).get("event_family") == "completed_workflow_reconcile"
        ]
        self.assertTrue(
            any(
                str(event.get("phase") or "") == "coalesced"
                and str(event.get("skip_reason") or "") == "completed_workflow_reconcile_inflight"
                and event.get("worker_ids") == [5]
                for event in structured_events
            )
        )

    def test_run_workflow_from_acquisition_reused_snapshot_short_circuits_after_public_web_stage(self) -> None:
        request_payload = {
            "raw_user_request": "帮我找Reflection AI的Post-train方向的人",
            "target_company": "Reflection AI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["Post-train"],
            "top_k": 10,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Reflection AI",
            snapshot_id="snapshot-direct-finalization",
            candidates=[
                Candidate(
                    candidate_id="cand_direct_finalization",
                    name_en="Direct Finalization",
                    display_name="Direct Finalization",
                    category="employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/direct-finalization/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_direct_finalization_former",
                    name_en="Direct Finalization Former",
                    display_name="Direct Finalization Former",
                    category="former_employee",
                    target_company="Reflection AI",
                    organization="Reflection AI",
                    employment_status="former",
                    role="Research Scientist",
                    linkedin_url="https://www.linkedin.com/in/direct-finalization-former/",
                ).to_record(),
            ],
        )
        identity = CompanyIdentity(
            requested_name="Reflection AI",
            canonical_name="Reflection AI",
            company_key="reflectionai",
            linkedin_slug="reflection-ai",
            linkedin_company_url="https://www.linkedin.com/company/reflection-ai/",
        )
        job_id = "job_reused_snapshot_direct_finalization"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Planning completed."},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_load_reusable_snapshot_state",
            return_value={
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
                "company_identity": identity,
                "reused_snapshot_checkpoint": True,
                "linkedin_stage_completed": True,
                "public_web_stage_completed": True,
                "candidates": [
                    Candidate(
                        candidate_id="cand_direct_finalization",
                        name_en="Direct Finalization",
                        display_name="Direct Finalization",
                        category="employee",
                        target_company="Reflection AI",
                        organization="Reflection AI",
                        employment_status="current",
                        role="Research Engineer",
                        linkedin_url="https://www.linkedin.com/in/direct-finalization/",
                    ),
                    Candidate(
                        candidate_id="cand_direct_finalization_former",
                        name_en="Direct Finalization Former",
                        display_name="Direct Finalization Former",
                        category="former_employee",
                        target_company="Reflection AI",
                        organization="Reflection AI",
                        employment_status="former",
                        role="Research Scientist",
                        linkedin_url="https://www.linkedin.com/in/direct-finalization-former/",
                    ),
                ],
            },
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_publish_stage1_preview_after_linkedin_stage",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_mark_public_web_stage_2_completed",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_running_workflow_before_retrieval",
            return_value={"status": "skipped", "reason": "reused_snapshot_checkpoint"},
        ) as refresh_running_workflow_before_retrieval, unittest.mock.patch.object(
            self.orchestrator,
            "_run_outreach_layering_after_acquisition",
            return_value={},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_execute_retrieval",
            return_value={
                "artifact_path": str(self.settings.jobs_dir / f"{job_id}.json"),
                "summary": {
                    "text": "Local asset population is ready with 1 candidates.",
                    "analysis_stage": "stage_2_final",
                },
                "matches": [],
            },
        ) as execute_retrieval, unittest.mock.patch.object(
            self.acquisition_engine,
            "execute_task",
            side_effect=AssertionError("reused snapshot should skip residual acquisition task execution"),
        ):
            result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(result["status"], "completed")
        self.assertEqual(execute_retrieval.call_count, 1)
        refresh_running_workflow_before_retrieval.assert_called_once()
        self.assertFalse(
            bool(refresh_running_workflow_before_retrieval.call_args.kwargs.get("include_materialization_refresh", True))
        )
        self.assertFalse(bool(execute_retrieval.call_args.kwargs.get("persist_job_state", True)))
        self.assertEqual(
            str(dict(execute_retrieval.call_args.kwargs.get("runtime_policy") or {}).get("mode") or ""),
            "direct_asset_population_finalization",
        )
        self.assertEqual(
            str(
                dict(dict(execute_retrieval.call_args.kwargs.get("runtime_policy") or {}).get(
                    "background_snapshot_materialization"
                ) or {}).get("status")
                or ""
            ),
            "deferred",
        )
        latest_job = self.store.get_job(job_id) or {}
        progress = dict(dict(latest_job.get("summary") or {}).get("acquisition_progress") or {})
        completed_task_types = {
            str(dict(payload or {}).get("task_type") or "").strip()
            for payload in dict(progress.get("tasks") or {}).values()
        }
        self.assertIn("normalize_asset_snapshot", completed_task_types)
        self.assertIn("build_retrieval_index", completed_task_types)

    def test_run_workflow_from_acquisition_merges_harvest_defer_into_background_materialization(self) -> None:
        request_payload = {
            "raw_user_request": "我想要OpenAI做Multimodal方向的人",
            "target_company": "OpenAI",
            "target_scope": "full_company_asset",
            "categories": ["researcher", "engineer"],
            "employment_statuses": ["current", "former"],
            "keywords": ["multimodal"],
            "top_k": 10,
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        plan.acquisition_tasks = []
        snapshot_dir, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="OpenAI",
            snapshot_id="snapshot-openai-direct-finalization-harvest-defer",
            candidates=[
                Candidate(
                    candidate_id="cand_openai_harvest_defer",
                    name_en="Harvest Defer",
                    display_name="Harvest Defer",
                    category="employee",
                    target_company="OpenAI",
                    organization="OpenAI",
                    employment_status="current",
                    role="Research Engineer",
                    linkedin_url="https://www.linkedin.com/in/openai-harvest-defer/",
                ).to_record()
            ],
        )
        identity = CompanyIdentity(
            requested_name="OpenAI",
            canonical_name="OpenAI",
            company_key="openai",
            linkedin_slug="openai",
            linkedin_company_url="https://www.linkedin.com/company/openai/",
        )
        job_id = "job_direct_finalization_harvest_defer"
        self.store.save_job(
            job_id=job_id,
            job_type="workflow",
            status="running",
            stage="planning",
            request_payload=request.to_record(),
            plan_payload=plan.to_record(),
            summary_payload={"message": "Planning completed."},
        )

        with unittest.mock.patch.object(
            self.orchestrator,
            "_restore_acquisition_state",
            return_value={
                "snapshot_id": snapshot_dir.name,
                "snapshot_dir": snapshot_dir,
                "candidate_doc_path": candidate_doc_path,
                "company_identity": identity,
                "reused_snapshot_checkpoint": True,
                "linkedin_stage_completed": True,
                "public_web_stage_completed": True,
                "candidates": [
                    Candidate(
                        candidate_id="cand_openai_harvest_defer",
                        name_en="Harvest Defer",
                        display_name="Harvest Defer",
                        category="employee",
                        target_company="OpenAI",
                        organization="OpenAI",
                        employment_status="current",
                        role="Research Engineer",
                        linkedin_url="https://www.linkedin.com/in/openai-harvest-defer/",
                    )
                ],
            },
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_publish_stage1_preview_after_linkedin_stage",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_mark_public_web_stage_2_completed",
            return_value={"status": "completed"},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_refresh_running_workflow_before_retrieval",
            return_value={
                "status": "skipped",
                "reason": "harvest_prefetch_refresh_deferred_to_background",
                "harvest_prefetch_worker_count": 2,
            },
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_run_outreach_layering_after_acquisition",
            return_value={},
        ), unittest.mock.patch.object(
            self.orchestrator,
            "_execute_retrieval",
            return_value={
                "artifact_path": str(self.settings.jobs_dir / f"{job_id}.json"),
                "summary": {
                    "text": "Local asset population is ready with 1 candidates.",
                    "analysis_stage": "stage_2_final",
                },
                "matches": [],
            },
        ) as execute_retrieval, unittest.mock.patch.object(
            self.acquisition_engine,
            "execute_task",
            side_effect=AssertionError("reused snapshot should skip residual acquisition task execution"),
        ):
            result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(result["status"], "completed")
        runtime_policy = dict(execute_retrieval.call_args.kwargs.get("runtime_policy") or {})
        background_snapshot_materialization = dict(runtime_policy.get("background_snapshot_materialization") or {})
        self.assertEqual(str(background_snapshot_materialization.get("status") or ""), "deferred")
        self.assertEqual(int(background_snapshot_materialization.get("harvest_prefetch_worker_count") or 0), 2)
        self.assertEqual(
            list(background_snapshot_materialization.get("deferred_components") or []),
            ["snapshot_materialization", "harvest_prefetch_refresh"],
        )

    def test_manual_review_items_are_snapshot_scoped_and_cleanup_old_snapshots(self) -> None:
        old_snapshot_path = str(
            self.settings.company_assets_dir / "acme" / "20260406T120000" / "candidate_documents.json"
        )
        new_snapshot_path = str(
            self.settings.company_assets_dir / "acme" / "20260407T120000" / "candidate_documents.json"
        )
        old_item = {
            "candidate_id": "cand_manual",
            "target_company": "Acme",
            "review_type": "manual_identity_resolution",
            "priority": "high",
            "status": "open",
            "summary": "Old snapshot item.",
            "candidate": {
                "candidate_id": "cand_manual",
                "name_en": "Alice Example",
                "display_name": "Alice Example",
                "category": "lead",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": old_snapshot_path,
                "metadata": {},
            },
            "evidence": [
                {
                    "candidate_id": "cand_manual",
                    "source_type": "publication_match",
                    "source_path": old_snapshot_path,
                    "metadata": {},
                }
            ],
            "metadata": {},
        }
        old_other_item = {
            "candidate_id": "cand_old_only",
            "target_company": "Acme",
            "review_type": "needs_human_validation",
            "priority": "medium",
            "status": "open",
            "summary": "Old-only snapshot item.",
            "candidate": {
                "candidate_id": "cand_old_only",
                "name_en": "Bob Old",
                "display_name": "Bob Old",
                "category": "employee",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": old_snapshot_path,
                "metadata": {},
            },
            "evidence": [],
            "metadata": {},
        }
        new_item = {
            "candidate_id": "cand_manual",
            "target_company": "Acme",
            "review_type": "manual_identity_resolution",
            "priority": "high",
            "status": "open",
            "summary": "New snapshot item.",
            "candidate": {
                "candidate_id": "cand_manual",
                "name_en": "Alice Example",
                "display_name": "Alice Example",
                "category": "lead",
                "target_company": "Acme",
                "organization": "Acme",
                "source_path": new_snapshot_path,
                "metadata": {},
            },
            "evidence": [
                {
                    "candidate_id": "cand_manual",
                    "source_type": "publication_match",
                    "source_path": new_snapshot_path,
                    "metadata": {},
                }
            ],
            "metadata": {},
        }

        self.store.repos.manual_review.replace_items("job_old", [old_item, old_other_item])
        self.store.repos.manual_review.replace_items("job_new", [new_item])

        all_items = self.store.repos.manual_review.list_items(target_company="Acme", status="", limit=10)
        status_by_candidate = {(item["candidate_id"], item["job_id"]): item["status"] for item in all_items}
        snapshot_by_candidate = {
            (item["candidate_id"], item["job_id"]): item["metadata"].get("snapshot_id") for item in all_items
        }
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_old")], "20260406T120000")
        self.assertEqual(snapshot_by_candidate[("cand_manual", "job_new")], "20260407T120000")
        self.assertEqual(status_by_candidate[("cand_manual", "job_old")], "superseded")
        self.assertEqual(status_by_candidate[("cand_manual", "job_new")], "open")

        cleanup = self.store.repos.manual_review.cleanup_items(target_company="Acme", snapshot_id="20260407T120000")
        self.assertGreaterEqual(cleanup["out_of_scope_count"], 1)
        open_items = self.store.repos.manual_review.list_items(target_company="Acme", status="open", limit=10)
        self.assertEqual(len(open_items), 1)
        self.assertEqual(open_items[0]["candidate_id"], "cand_manual")
        self.assertEqual(open_items[0]["metadata"].get("snapshot_id"), "20260407T120000")

    def test_mark_linkedin_profile_registry_queued_preserves_fetched_when_raw_exists(self) -> None:
        profile_url = "https://www.linkedin.com/in/registry-preserve/"
        raw_path = str(
            self.settings.company_assets_dir / "reflectionai" / "registry-preserve" / "harvest_profiles" / "cached.json"
        )
        self.store.repos.linkedin_profile_registry.mark_fetched(
            profile_url,
            raw_path=raw_path,
            snapshot_dir=str(self.settings.company_assets_dir / "reflectionai" / "registry-preserve"),
        )

        updated = self.store.repos.linkedin_profile_registry.mark_queued(
            profile_url,
            snapshot_dir=str(self.settings.company_assets_dir / "reflectionai" / "queued-overwrite-attempt"),
        )

        self.assertEqual(str(updated.get("status") or ""), "fetched")
        self.assertEqual(str(updated.get("last_raw_path") or ""), raw_path)

    def test_snapshot_materializer_pre_retrieval_refresh_uses_foreground_fast_artifact_profile(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-foreground-fast"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_sync_fast_1",
            name_en="Alice Fast",
            display_name="Alice Fast",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-fast/",
        )
        candidate_doc_path = snapshot_dir / "candidate_documents.json"
        candidate_doc_path.write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        manifest_path = snapshot_dir / "manifest.json"
        captured_execution_preferences: dict[str, object] = {}

        def fake_normalize_snapshot(
            task: AcquisitionTask,
            state: dict[str, object],
            job_request: JobRequest | None = None,
        ) -> AcquisitionExecution:
            captured_execution_preferences.update(dict(getattr(job_request, "execution_preferences", {}) or {}))
            return AcquisitionExecution(
                task_id=str(task.task_id),
                status="completed",
                detail="Normalized and materialized snapshot.",
                payload={
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "manifest_path": str(manifest_path),
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "manifest": str(snapshot_dir / "normalized_artifacts" / "manifest.json"),
                    },
                    "sync_status": {"overall_status": "completed"},
                },
                state_updates={"manifest_path": manifest_path},
            )

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "_normalize_snapshot",
                side_effect=fake_normalize_snapshot,
            ) as normalize_snapshot,
            unittest.mock.patch(
                "sourcing_agent.snapshot_materializer.build_company_candidate_artifacts",
                side_effect=AssertionError("should not rebuild artifacts when normalize already did"),
            ),
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="pre_retrieval_refresh",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(str(captured_execution_preferences.get("artifact_build_profile") or ""), "foreground_fast")
        normalize_snapshot.assert_called_once()

    def test_snapshot_materializer_background_harvest_refresh_uses_foreground_fast_artifact_profile(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-background-harvest-fast"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_sync_harvest_fast_1",
            name_en="Alice Harvest Fast",
            display_name="Alice Harvest Fast",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-harvest-fast/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        captured_execution_preferences: dict[str, object] = {}

        def fake_normalize_snapshot(
            task: AcquisitionTask,
            state: dict[str, object],
            job_request: JobRequest | None = None,
        ) -> AcquisitionExecution:
            captured_execution_preferences.update(dict(getattr(job_request, "execution_preferences", {}) or {}))
            return AcquisitionExecution(
                task_id=str(task.task_id),
                status="completed",
                detail="Normalized and materialized snapshot.",
                payload={
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "manifest_path": str(snapshot_dir / "manifest.json"),
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "manifest": str(snapshot_dir / "normalized_artifacts" / "manifest.json"),
                    },
                    "sync_status": {"overall_status": "completed"},
                },
                state_updates={"manifest_path": snapshot_dir / "manifest.json"},
            )

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_normalize_snapshot",
            side_effect=fake_normalize_snapshot,
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="background_harvest_prefetch_reconcile",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(str(captured_execution_preferences.get("artifact_build_profile") or ""), "foreground_fast")

    def test_snapshot_materializer_background_snapshot_reconcile_uses_foreground_fast_artifact_profile(self) -> None:
        snapshot_dir = self.settings.company_assets_dir / "acme" / "snapshot-sync-background-materialization-fast"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
            linkedin_slug="acme",
            linkedin_company_url="https://www.linkedin.com/company/acme/",
        )
        candidate = Candidate(
            candidate_id="acme_sync_snapshot_fast_1",
            name_en="Alice Snapshot Fast",
            display_name="Alice Snapshot Fast",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Research Engineer",
            linkedin_url="https://www.linkedin.com/in/alice-snapshot-fast/",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_dir.name,
                        "company_identity": identity.to_record(),
                    },
                    "candidates": [candidate.to_record()],
                    "evidence": [],
                    "candidate_count": 1,
                    "evidence_count": 0,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        captured_execution_preferences: dict[str, object] = {}

        def fake_normalize_snapshot(
            task: AcquisitionTask,
            state: dict[str, object],
            job_request: JobRequest | None = None,
        ) -> AcquisitionExecution:
            captured_execution_preferences.update(dict(getattr(job_request, "execution_preferences", {}) or {}))
            return AcquisitionExecution(
                task_id=str(task.task_id),
                status="completed",
                detail="Normalized and materialized snapshot.",
                payload={
                    "candidate_count": 1,
                    "evidence_count": 0,
                    "manifest_path": str(snapshot_dir / "manifest.json"),
                    "artifact_dir": str(snapshot_dir / "normalized_artifacts"),
                    "artifact_paths": {
                        "manifest": str(snapshot_dir / "normalized_artifacts" / "manifest.json"),
                    },
                    "sync_status": {"overall_status": "completed"},
                },
                state_updates={"manifest_path": snapshot_dir / "manifest.json"},
            )

        with unittest.mock.patch.object(
            self.acquisition_engine,
            "_normalize_snapshot",
            side_effect=fake_normalize_snapshot,
        ):
            result = self.orchestrator.snapshot_materializer.synchronize_snapshot_candidate_documents(
                request=JobRequest(raw_user_request="帮我找 Acme 的人", target_company="Acme"),
                snapshot_dir=snapshot_dir,
                reason="background_snapshot_materialization_reconcile",
            )

        self.assertEqual(result["status"], "completed")
        self.assertEqual(str(captured_execution_preferences.get("artifact_build_profile") or ""), "foreground_fast")


    def test_two_stage_workflow_publishes_stage1_preview_and_continues_public_web_stage2_when_enabled(self) -> None:
        snapshot_id = "20260411T130000"
        snapshot_dir = self.settings.company_assets_dir / "acme" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        company_identity = CompanyIdentity(
            requested_name="Acme",
            canonical_name="Acme",
            company_key="acme",
        )
        candidate = Candidate(
            candidate_id="acme_infra_1",
            name_en="Taylor Infra",
            display_name="Taylor Infra",
            category="employee",
            target_company="Acme",
            organization="Acme",
            employment_status="current",
            role="Infrastructure Engineer",
            focus_areas="infra platform systems",
            linkedin_url="https://www.linkedin.com/in/taylor-infra/",
        )
        request_payload = {
            "raw_user_request": "给我 Acme 的 Infra 方向成员",
            "target_company": "Acme",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["infra"],
            "semantic_rerank_limit": 0,
            "analysis_stage_mode": "two_stage",
        }
        request = JobRequest.from_payload(request_payload)
        plan = build_sourcing_plan(request, self.catalog, self.model_client)
        job_id = self.orchestrator._create_workflow_job(request, plan)
        retrieval_calls: list[dict[str, object]] = []

        def fake_execute_task(
            task: AcquisitionTask,
            _request: JobRequest,
            _target_company: str,
            _state: dict[str, object],
            _bootstrap_summary: dict[str, object] | None,
        ) -> AcquisitionExecution:
            if task.task_type == "resolve_company_identity":
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Resolved company identity.",
                    payload={"snapshot_id": snapshot_id},
                    state_updates={
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "company_identity": company_identity,
                    },
                )
            if task.task_type == "enrich_linkedin_profiles":
                candidate_doc_path = snapshot_dir / "candidate_documents.json"
                linkedin_stage_path = snapshot_dir / "candidate_documents.linkedin_stage_1.json"
                candidate_doc_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                linkedin_stage_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built LinkedIn stage-1 candidate documents.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                        "linkedin_stage_candidate_doc_path": linkedin_stage_path,
                        "linkedin_stage_completed": True,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )
            if task.task_type == "enrich_public_web_signals":
                candidate_doc_path = snapshot_dir / "candidate_documents.json"
                public_web_stage_path = snapshot_dir / "candidate_documents.public_web_stage_2.json"
                candidate_doc_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                public_web_stage_path.write_text(json.dumps({"candidates": [], "evidence": []}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Extended candidate documents with public-web stage-2 evidence.",
                    payload={"candidate_doc_path": str(candidate_doc_path)},
                    state_updates={
                        "snapshot_id": snapshot_id,
                        "snapshot_dir": snapshot_dir,
                        "candidate_doc_path": candidate_doc_path,
                        "public_web_stage_candidate_doc_path": public_web_stage_path,
                        "public_web_stage_completed": True,
                        "candidates": [candidate],
                        "evidence": [],
                    },
                )
            if task.task_type == "normalize_asset_snapshot":
                manifest_path = snapshot_dir / "manifest.json"
                manifest_path.write_text(json.dumps({"snapshot_id": snapshot_id}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Normalized snapshot.",
                    payload={"manifest_path": str(manifest_path)},
                    state_updates={"manifest_path": manifest_path},
                )
            if task.task_type == "build_retrieval_index":
                index_path = snapshot_dir / "retrieval_index_summary.json"
                index_path.write_text(json.dumps({"status": "completed"}, ensure_ascii=False))
                return AcquisitionExecution(
                    task_id=task.task_id,
                    status="completed",
                    detail="Built retrieval index.",
                    payload={"retrieval_index_summary": str(index_path)},
                    state_updates={},
                )
            return AcquisitionExecution(
                task_id=task.task_id,
                status="completed",
                detail=f"Completed {task.task_type}.",
                payload={},
                state_updates={},
            )

        def fake_execute_retrieval(
            current_job_id: str,
            current_request: JobRequest,
            current_plan,
            job_type: str,
            runtime_policy: dict[str, object] | None = None,
            candidate_source_override: dict[str, object] | None = None,
            *,
            persist_job_state: bool = True,
            artifact_name_suffix: str = "",
            artifact_status: str = "completed",
        ) -> dict[str, object]:
            runtime_policy = dict(runtime_policy or {})
            analysis_stage = str(runtime_policy.get("analysis_stage") or "stage_2_final")
            current_job = self.store.get_job(current_job_id) or {}
            retrieval_calls.append(
                {
                    "analysis_stage": analysis_stage,
                    "job_stage": str(current_job.get("stage") or ""),
                    "job_status": str(current_job.get("status") or ""),
                    "persist_job_state": persist_job_state,
                }
            )
            summary = {
                "text": f"{analysis_stage} summary",
                "total_matches": 1,
                "returned_matches": 1,
                "manual_review_queue_count": 0,
                "analysis_stage": analysis_stage,
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "candidate_count": 1,
                },
                "outreach_layering": {"status": "completed", "analysis_stage": analysis_stage},
            }
            artifact_path = self.settings.jobs_dir / (
                f"{current_job_id}.json"
                if not artifact_name_suffix
                else f"{current_job_id}.{artifact_name_suffix}.json"
            )
            artifact = {
                "job_id": current_job_id,
                "status": artifact_status,
                "request": current_request.to_record(),
                "plan": current_plan.to_record(),
                "summary": summary,
                "matches": [],
                "manual_review_items": [],
                "artifact_path": str(artifact_path),
            }
            self.store.replace_job_results(
                current_job_id,
                [
                    {
                        "candidate_id": candidate.candidate_id,
                        "rank": 1,
                        "score": 1.0,
                        "semantic_score": 0.0,
                        "confidence_label": "high",
                        "confidence_score": 1.0,
                        "confidence_reason": "test",
                        "explanation": f"{analysis_stage} explanation",
                        "matched_fields": ["focus_areas"],
                        "outreach_layer": 0,
                        "outreach_layer_key": "layer_0_roster",
                        "outreach_layer_source": "rules",
                    }
                ],
            )
            self.store.repos.manual_review.replace_items(current_job_id, [])
            if persist_job_state:
                self.store.save_job(
                    job_id=current_job_id,
                    job_type=job_type,
                    status="completed",
                    stage="completed",
                    request_payload=current_request.to_record(),
                    plan_payload=current_plan.to_record(),
                    summary_payload=summary,
                    artifact_path=str(artifact_path),
                )
            return artifact

        layering_calls: list[dict[str, object]] = []

        def fake_run_outreach_layering_after_acquisition(
            *,
            job_id: str,
            request: JobRequest,
            acquisition_state: dict[str, object],
            allow_ai: bool | None = None,
            allow_background_defer: bool | None = None,
            analysis_stage_label: str = "",
            event_stage: str = "",
            **_kwargs,
        ) -> dict[str, object]:
            current_job = self.store.get_job(job_id) or {}
            layering_calls.append(
                {
                    "analysis_stage": analysis_stage_label,
                    "job_stage": str(current_job.get("stage") or ""),
                    "job_status": str(current_job.get("status") or ""),
                    "event_stage": event_stage,
                    "allow_ai": bool(allow_ai),
                    "allow_background_defer": bool(allow_background_defer),
                }
            )
            return {"status": "completed", "analysis_stage": analysis_stage_label or "stage_2_final"}

        with (
            unittest.mock.patch.object(
                self.acquisition_engine,
                "execute_task",
                side_effect=fake_execute_task,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_run_outreach_layering_after_acquisition",
                side_effect=fake_run_outreach_layering_after_acquisition,
            ),
            unittest.mock.patch.object(
                self.orchestrator,
                "_execute_retrieval",
                side_effect=fake_execute_retrieval,
            ),
        ):
            run_result = self.orchestrator._run_workflow_from_acquisition(job_id, request, plan)

        self.assertEqual(run_result["status"], "completed")
        self.assertEqual(
            [str(item["analysis_stage"]) for item in retrieval_calls],
            ["stage_1_preview", "stage_2_final"],
        )
        self.assertEqual(
            [str(item["analysis_stage"]) for item in layering_calls],
            ["stage_2_final"],
        )
        self.assertEqual(layering_calls[0]["job_stage"], "retrieving")
        self.assertEqual(layering_calls[0]["event_stage"], "retrieving")
        self.assertEqual(retrieval_calls[0]["job_stage"], "acquiring")
        self.assertEqual(retrieval_calls[1]["job_stage"], "retrieving")

        snapshot = self.orchestrator.get_job_results(job_id)
        assert snapshot is not None
        # CALIBRATED 2026-07-22 (deep forensics): terminal flip is gated on
        # the durable serving-finalized proof this synthetic fixture never
        # records — pin the fail-closed gate instead of inline completion.
        stored_job = self.store.get_job(job_id)
        assert stored_job is not None
        blockers = self.orchestrator._workflow_completion_promotion_blockers(stored_job)
        self.assertEqual(str(blockers.get("reason") or ""), "serving_finalized_proof_missing")
        self.assertEqual(snapshot["job"]["status"], "running")
        self.assertFalse(snapshot["job"]["summary"].get("awaiting_user_action"))
        self.assertEqual(snapshot["job"]["summary"]["stage1_preview"]["status"], "ready")
        self.assertEqual(snapshot["job"]["summary"]["public_web_stage_2"]["status"], "completed")
        event_details = [str(item.get("detail") or "") for item in snapshot["events"]]
        self.assertIn("LinkedIn Stage 1 acquisition completed.", event_details)
        self.assertIn("Stage 1 preview ready; continuing Public Web Stage 2 acquisition.", event_details)
        self.assertIn("Public Web Stage 2 acquisition completed.", event_details)

if __name__ == "__main__":
    unittest.main()
