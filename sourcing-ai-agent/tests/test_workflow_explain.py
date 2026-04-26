import json
import os
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path
from urllib import request as urllib_request

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_reuse_planning import build_acquisition_shard_registry_record
from sourcing_agent.company_registry import resolve_company_alias_key
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
from sourcing_agent.storage import ControlPlaneStore


def _without_volatile_timestamps(value):
    if isinstance(value, dict):
        return {
            key: _without_volatile_timestamps(item)
            for key, item in value.items()
            if not str(key).endswith("_at")
        }
    if isinstance(value, list):
        return [_without_volatile_timestamps(item) for item in value]
    return value


class WorkflowExplainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.catalog = AssetCatalog.discover()
        self.store = ControlPlaneStore(f"{self.tempdir.name}/test.db")
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
        self._runtime_env_patcher = unittest.mock.patch.dict(
            os.environ,
            {"SOURCING_COMPANY_REGISTRY_RUNTIME_DIR": str(self.settings.runtime_dir)},
            clear=False,
        )
        self._runtime_env_patcher.start()

    def tearDown(self) -> None:
        self._runtime_env_patcher.stop()
        self.tempdir.cleanup()

    def _write_company_snapshot(self, *, target_company: str, snapshot_id: str, candidate_count: int = 3) -> str:
        company_key = resolve_company_alias_key(target_company)
        snapshot_dir = Path(self.tempdir.name) / "company_assets" / company_key / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "aliases": [target_company],
        }
        (snapshot_dir / "identity.json").write_text(json.dumps(identity, ensure_ascii=False, indent=2), encoding="utf-8")
        (snapshot_dir.parent / "latest_snapshot.json").write_text(
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
        candidates = []
        for index in range(candidate_count):
            candidates.append(
                {
                    "candidate_id": f"{company_key}-{index}",
                    "name_en": f"Candidate {index}",
                    "display_name": f"Candidate {index}",
                    "category": "employee",
                    "target_company": target_company,
                    "employment_status": "current",
                    "role": "Research Engineer",
                    "linkedin_url": f"https://www.linkedin.com/in/{company_key}-{index}/",
                    "source_dataset": f"{company_key}_snapshot",
                }
            )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(
                {
                    "snapshot": {
                        "snapshot_id": snapshot_id,
                        "company_identity": identity,
                    },
                    "candidates": candidates,
                    "evidence": [],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return company_key

    def _upsert_authoritative_registry(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        current_count: int,
        former_count: int,
        profile_detail_count: int | None = None,
        explicit_profile_capture_count: int | None = None,
        missing_linkedin_count: int = 0,
        manual_review_backlog_count: int = 0,
        profile_completion_backlog_count: int = 0,
        completeness_score: float = 90.0,
        completeness_band: str = "high",
        materialization_generation_key: str = "",
        materialization_generation_sequence: int = 0,
        materialization_watermark: str = "",
    ) -> None:
        company_key = resolve_company_alias_key(target_company)
        source_path = str(
            Path(self.tempdir.name) / "company_assets" / company_key / snapshot_id / "candidate_documents.json"
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": target_company,
                "company_key": company_key,
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": current_count + former_count,
                "evidence_count": 0,
                "profile_detail_count": (
                    current_count + former_count if profile_detail_count is None else profile_detail_count
                ),
                "explicit_profile_capture_count": (
                    current_count + former_count
                    if explicit_profile_capture_count is None
                    else explicit_profile_capture_count
                ),
                "missing_linkedin_count": missing_linkedin_count,
                "manual_review_backlog_count": manual_review_backlog_count,
                "profile_completion_backlog_count": profile_completion_backlog_count,
                "source_snapshot_count": 1,
                "standard_bundles": {"bundle_count": 1},
                "completeness_score": completeness_score,
                "completeness_band": completeness_band,
                "current_lane_coverage": {
                    "effective_candidate_count": current_count,
                    "effective_ready": True,
                },
                "former_lane_coverage": {
                    "effective_candidate_count": former_count,
                    "effective_ready": True,
                },
                "current_lane_effective_candidate_count": current_count,
                "former_lane_effective_candidate_count": former_count,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {"selected_snapshot_ids": [snapshot_id]},
                "source_path": source_path,
                "source_job_id": "",
                "materialization_generation_key": materialization_generation_key,
                "materialization_generation_sequence": materialization_generation_sequence,
                "materialization_watermark": materialization_watermark,
                "summary": {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "candidate_count": current_count + former_count,
                    "standard_bundles": {"bundle_count": 1},
                    "profile_detail_count": (
                        current_count + former_count if profile_detail_count is None else profile_detail_count
                    ),
                    "current_lane_coverage": {
                        "effective_candidate_count": current_count,
                        "effective_ready": True,
                    },
                    "former_lane_coverage": {
                        "effective_candidate_count": former_count,
                        "effective_ready": True,
                    },
                },
            },
            authoritative=True,
        )

    def _register_generation(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidate_count: int,
    ) -> dict[str, object]:
        company_key = resolve_company_alias_key(target_company)
        members = []
        for index in range(candidate_count):
            linkedin_url = f"https://www.linkedin.com/in/{company_key}-{index}/"
            members.append(
                {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "artifact_kind": "organization_asset",
                    "artifact_key": "canonical_merged",
                    "lane": "baseline",
                    "employment_scope": "current",
                    "member_key": self.store.normalize_linkedin_profile_url(linkedin_url),
                    "member_key_kind": "profile_url_key",
                    "candidate_id": f"{company_key}-{index}",
                    "profile_url_key": self.store.normalize_linkedin_profile_url(linkedin_url),
                }
            )
        return self.store.register_asset_materialization(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            artifact_kind="organization_asset",
            artifact_key="canonical_merged",
            source_path=str(
                Path(self.tempdir.name)
                / "company_assets"
                / company_key
                / snapshot_id
                / "normalized_artifacts"
                / "artifact_summary.json"
            ),
            summary={
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "candidate_count": candidate_count,
            },
            metadata={"test_case": "workflow_explain_generation"},
            members=members,
        )

    def test_explain_workflow_exposes_ingress_planning_and_dispatch(self) -> None:
        snapshot_id = "20260414T010203"
        self._write_company_snapshot(target_company="Skild AI", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="Skild AI",
            snapshot_id=snapshot_id,
            current_count=153,
            former_count=22,
        )

        explained = self.orchestrator.explain_workflow(
            {"raw_user_request": "帮我找Skild AI做Pre-train方向的人"}
        )

        self.assertEqual(explained["ingress_normalization"]["prepared_request"]["target_company"], "Skild AI")
        self.assertEqual(explained["request_preview"]["target_company"], "Skild AI")
        self.assertEqual(explained["dispatch_preview"]["strategy"], "reuse_snapshot")
        self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "reuse_baseline")
        self.assertIn("request_matching", explained["dispatch_matching_normalization"])
        self.assertGreaterEqual(float(explained["timings_ms"]["total"]), 0.0)
        self.assertIn("prepare_request", explained["timings_ms"])
        self.assertIn("build_plan", explained["timings_ms"])
        self.assertIn("build_execution_bundle", explained["timings_ms"])
        self.assertIn("timing_breakdown_ms", explained)
        prepare_breakdown = dict((explained.get("timing_breakdown_ms") or {}).get("prepare_request") or {})
        self.assertIn("llm_normalize_request", prepare_breakdown)
        self.assertIn("deterministic_signal_supplement", prepare_breakdown)
        self.assertGreaterEqual(float(prepare_breakdown.get("total") or 0.0), 0.0)

    def test_api_workflows_explain_route_returns_dry_run_payload(self) -> None:
        snapshot_id = "20260414T020304"
        self._write_company_snapshot(target_company="Reflection AI", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="Reflection AI",
            snapshot_id=snapshot_id,
            current_count=120,
            former_count=10,
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        try:
            request_payload = urllib_request.Request(
                f"http://{host}:{port}/api/workflows/explain",
                data=json.dumps(
                    {
                        "raw_user_request": "帮我找Reflection AI做Infra方向的人",
                        "skip_plan_review": True,
                    },
                    ensure_ascii=False,
                ).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
            with opener.open(request_payload, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        self.assertEqual(payload["status"], "ready")
        self.assertIn("ingress_normalization", payload)
        self.assertIn("dispatch_preview", payload)
        self.assertIn("timings_ms", payload)
        self.assertIn("timing_breakdown_ms", payload)

    def test_explain_workflow_does_not_treat_company_name_ai_suffix_as_ambiguous(self) -> None:
        for target_company, snapshot_id, query in [
            ("Reflection AI", "20260414T020401", "我想要Reflection AI做Coding方向的人"),
            ("Skild AI", "20260414T020402", "请帮我找Skild AI里做Pre-train方向的人"),
        ]:
            with self.subTest(target_company=target_company):
                self._write_company_snapshot(target_company=target_company, snapshot_id=snapshot_id)
                self._upsert_authoritative_registry(
                    target_company=target_company,
                    snapshot_id=snapshot_id,
                    current_count=120,
                    former_count=12,
                )
                explained = self.orchestrator.explain_workflow({"raw_user_request": query})
                confirmation_items = [
                    str(item).strip()
                    for item in list((explained.get("plan_review_gate") or {}).get("confirmation_items") or [])
                    if str(item).strip()
                ]
                self.assertFalse(
                    any("not confidently grounded: AI" in item for item in confirmation_items),
                    confirmation_items,
                )

    def test_explain_workflow_large_org_directional_queries_without_baseline_start_live_acquisition(self) -> None:
        cases = [
            ("给我OpenAI做Pre-train方向的人", ["Pre-train"]),
            ("给我OpenAI做Infra和Post-train方向的人", ["Post-train", "Infra"]),
        ]
        for query, expected_keywords in cases:
            with self.subTest(query=query):
                explained = self.orchestrator.explain_workflow({"raw_user_request": query, "top_k": 10})
                self.assertEqual(explained["request_preview"]["target_company"], "OpenAI")
                self.assertCountEqual(explained["request_preview"]["keywords"], expected_keywords)
                self.assertEqual(
                    explained["organization_execution_profile"]["default_acquisition_mode"],
                    "scoped_search_roster",
                )
                self.assertEqual(explained["dispatch_preview"]["strategy"], "new_job")
                self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "live_acquisition")
                self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "live_acquisition")

    def test_explain_workflow_large_org_directional_query_with_unrelated_baseline_shards_keeps_scoped_delta(self) -> None:
        snapshot_id = "20260415T010203"
        company_key = self._write_company_snapshot(target_company="OpenAI", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            current_count=153,
            former_count=44,
        )
        self.store.upsert_acquisition_shard_registry(
            build_acquisition_shard_registry_record(
                target_company="OpenAI",
                company_key=company_key,
                snapshot_id=snapshot_id,
                lane="profile_search",
                employment_scope="current",
                strategy_type="scoped_search_roster",
                shard_id="Reasoning",
                shard_title="Reasoning",
                search_query="Reasoning",
                company_filters={
                    "companies": ["https://www.linkedin.com/company/openai/"],
                    "function_ids": ["24", "8"],
                    "search_query": "Reasoning",
                },
                result_count=95,
                status="completed",
            )
        )

        explained = self.orchestrator.explain_workflow(
            {"raw_user_request": "我想要OpenAI做Pre-train方向的人", "top_k": 10}
        )

        self.assertEqual(explained["request_preview"]["target_company"], "OpenAI")
        self.assertEqual(explained["request_preview"]["keywords"], ["Pre-train"])
        self.assertEqual(
            explained["organization_execution_profile"]["default_acquisition_mode"],
            "scoped_search_roster",
        )
        self.assertEqual(explained["dispatch_preview"]["strategy"], "delta_from_snapshot")
        self.assertEqual(explained["asset_reuse_plan"]["planner_mode"], "delta_from_snapshot")
        self.assertTrue(explained["asset_reuse_plan"]["requires_delta_acquisition"])
        self.assertFalse(explained["asset_reuse_plan"]["baseline_current_embedded_query_reuse_allowed"])
        self.assertFalse(explained["asset_reuse_plan"]["baseline_former_embedded_query_reuse_allowed"])
        missing_current_queries = [
            str(item).strip()
            for item in list(explained["asset_reuse_plan"]["missing_current_profile_search_queries"] or [])
            if str(item).strip()
        ]
        self.assertTrue(any("Pre-train" in item for item in missing_current_queries), missing_current_queries)
        self.assertFalse(any(item.lower() == "research" for item in missing_current_queries), missing_current_queries)
        self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "delta_acquisition")
        self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "delta_acquisition")
        covered_current_queries = list(explained["asset_reuse_plan"]["covered_current_profile_search_queries"] or [])
        self.assertFalse(
            any(
                str((row or {}).get("search_query") or "").strip() == "Pre-train"
                and str((row or {}).get("status") or "").strip() == "embedded_baseline_reuse"
                for row in covered_current_queries
            )
        )

    def test_explain_workflow_prefers_better_non_authoritative_snapshot_when_authoritative_baseline_is_stale(self) -> None:
        old_snapshot_id = "20260422T151623"
        newer_snapshot_id = "20260422T171923"
        company_key = self._write_company_snapshot(target_company="OpenAI", snapshot_id=old_snapshot_id)
        self._write_company_snapshot(target_company="OpenAI", snapshot_id=newer_snapshot_id)
        self._upsert_authoritative_registry(
            target_company="OpenAI",
            snapshot_id=old_snapshot_id,
            current_count=252,
            former_count=80,
        )
        self.store.upsert_organization_asset_registry(
            {
                "target_company": "OpenAI",
                "company_key": company_key,
                "snapshot_id": newer_snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": False,
                "candidate_count": 404,
                "evidence_count": 0,
                "profile_detail_count": 404,
                "explicit_profile_capture_count": 404,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 11,
                "completeness_score": 88.0,
                "completeness_band": "high",
                "current_lane_coverage": {
                    "effective_candidate_count": 293,
                    "effective_ready": True,
                },
                "former_lane_coverage": {
                    "effective_candidate_count": 111,
                    "effective_ready": True,
                },
                "current_lane_effective_candidate_count": 293,
                "former_lane_effective_candidate_count": 111,
                "current_lane_effective_ready": True,
                "former_lane_effective_ready": True,
                "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
                "source_snapshot_selection": {
                    "mode": "all_history_snapshots",
                    "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
                },
                "source_path": str(
                    Path(self.tempdir.name) / "company_assets" / company_key / newer_snapshot_id / "candidate_documents.json"
                ),
                "source_job_id": "",
                "materialization_generation_key": "newer-openai-generation",
                "materialization_generation_sequence": 5,
                "materialization_watermark": "5:newer-openai",
                "summary": {
                    "target_company": "OpenAI",
                    "snapshot_id": newer_snapshot_id,
                    "candidate_count": 404,
                    "profile_detail_count": 404,
                    "source_snapshot_selection": {
                        "mode": "all_history_snapshots",
                        "selected_snapshot_ids": [old_snapshot_id, newer_snapshot_id],
                    },
                    "current_lane_coverage": {
                        "effective_candidate_count": 293,
                        "effective_ready": True,
                    },
                    "former_lane_coverage": {
                        "effective_candidate_count": 111,
                        "effective_ready": True,
                    },
                },
            },
            authoritative=False,
        )
        for employment_scope, strategy_type in (("current", "scoped_search_roster"), ("former", "former_employee_search")):
            self.store.upsert_acquisition_shard_registry(
                build_acquisition_shard_registry_record(
                    target_company="OpenAI",
                    company_key=company_key,
                    snapshot_id=newer_snapshot_id,
                    lane="profile_search",
                    employment_scope=employment_scope,
                    strategy_type=strategy_type,
                    shard_id="Coding",
                    shard_title="Coding",
                    search_query="Coding",
                    company_filters={
                        "companies": ["https://www.linkedin.com/company/openai/"],
                        "function_ids": ["24", "8"],
                        "search_query": "Coding",
                    },
                    result_count=26 if employment_scope == "former" else 47,
                    status="completed",
                )
            )

        explained = self.orchestrator.explain_workflow(
            {"raw_user_request": "我想要OpenAI做Coding方向的人", "top_k": 10}
        )

        self.assertEqual(explained["asset_reuse_plan"]["baseline_snapshot_id"], newer_snapshot_id)
        self.assertEqual(
            explained["asset_reuse_plan"]["baseline_selection_explanation"]["matched_registry_snapshot_id"],
            newer_snapshot_id,
        )
        self.assertEqual(explained["asset_reuse_plan"]["planner_mode"], "reuse_snapshot_only")
        self.assertFalse(explained["asset_reuse_plan"]["requires_delta_acquisition"])
        self.assertEqual(
            list(explained["asset_reuse_plan"]["missing_current_profile_search_queries"] or []),
            [],
        )
        self.assertEqual(
            list(explained["asset_reuse_plan"]["missing_former_profile_search_queries"] or []),
            [],
        )
        self.assertEqual(
            explained["effective_execution_semantics"]["effective_acquisition_mode"],
            "full_local_asset_reuse",
        )

    def test_explain_workflow_large_authoritative_baseline_reuses_snapshot_without_delta(self) -> None:
        snapshot_id = "20260415T010203-anthropic"
        self._write_company_snapshot(target_company="Anthropic", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            current_count=3014,
            former_count=429,
        )

        explained = self.orchestrator.explain_workflow(
            {"raw_user_request": "帮我找Anthropic里做Pre-training方向的人", "top_k": 10}
        )

        self.assertEqual(explained["organization_execution_profile"]["org_scale_band"], "large")
        self.assertEqual(explained["organization_execution_profile"]["default_acquisition_mode"], "scoped_search_roster")
        self.assertTrue(explained["asset_reuse_plan"]["baseline_directional_local_reuse_eligible"])
        self.assertEqual(explained["dispatch_preview"]["strategy"], "reuse_snapshot")
        self.assertFalse(explained["asset_reuse_plan"]["requires_delta_acquisition"])
        self.assertEqual(explained["asset_reuse_plan"]["planner_mode"], "reuse_snapshot_only")
        self.assertEqual(
            explained["effective_execution_semantics"]["effective_acquisition_mode"],
            "full_local_asset_reuse",
        )
        self.assertEqual(
            explained["effective_execution_semantics"]["default_results_mode"],
            "asset_population",
        )
        self.assertTrue(explained["effective_execution_semantics"]["asset_population_supported"])

    def test_explain_workflow_ignores_malformed_nested_intent_axes_payloads(self) -> None:
        snapshot_id = "20260415T010204"
        self._write_company_snapshot(target_company="OpenAI", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            current_count=153,
            former_count=44,
        )

        explained = self.orchestrator.explain_workflow(
            {
                "raw_user_request": "我想要OpenAI做Pre-train方向的人",
                "top_k": 10,
                "intent_axes": {
                    "population_boundary": ["current", "former"],
                    "scope_boundary": "OpenAI",
                    "fallback_policy": ["force_fresh_run"],
                    "thematic_constraints": ["Pre-train"],
                },
            }
        )

        self.assertEqual(explained["request_preview"]["target_company"], "OpenAI")
        self.assertEqual(explained["request_preview"]["keywords"], ["Pre-train"])
        self.assertEqual(
            explained["organization_execution_profile"]["default_acquisition_mode"],
            "scoped_search_roster",
        )
        self.assertEqual(explained["dispatch_preview"]["strategy"], "delta_from_snapshot")
        self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "delta_acquisition")
        self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "delta_acquisition")

    def test_plan_workflow_reuses_same_effective_request_and_dispatch_as_explain(self) -> None:
        snapshot_id = "20260415T010205"
        self._write_company_snapshot(target_company="OpenAI", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            current_count=153,
            former_count=44,
        )

        payload = {"raw_user_request": "帮我找OpenAI做Pre-train方向的人", "top_k": 10}
        explained = self.orchestrator.explain_workflow(payload)
        planned = self.orchestrator.plan_workflow(payload)

        self.assertEqual(planned["request"], explained["request"])
        self.assertEqual(planned["request_preview"], explained["request_preview"])
        self.assertEqual(
            _without_volatile_timestamps(planned["dispatch_preview"]),
            _without_volatile_timestamps(explained["dispatch_preview"]),
        )
        self.assertEqual(
            _without_volatile_timestamps(planned["asset_reuse_plan"]),
            _without_volatile_timestamps(explained["asset_reuse_plan"]),
        )
        self.assertEqual(planned["lane_preview"], explained["lane_preview"])
        self.assertEqual(
            dict(planned["plan_review_session"]).get("review_id"),
            dict(self.store.get_plan_review_session(int(planned["plan_review_session"]["review_id"]))).get("review_id"),
        )

    def test_explain_workflow_full_local_baseline_can_skip_delta_for_humansand_and_anthropic(self) -> None:
        cases = [
            ("Humans&", "20260414T040101", "我想了解Humans&里偏Coding agents方向的研究成员", "small", "full_company_roster", 520, 80),
            ("Anthropic", "20260414T040102", "帮我找Anthropic里做Pre-training方向的人", "medium", "hybrid", 880, 140),
        ]
        for target_company, snapshot_id, query, expected_scale, expected_mode, current_count, former_count in cases:
            with self.subTest(target_company=target_company):
                self._write_company_snapshot(target_company=target_company, snapshot_id=snapshot_id)
                self._upsert_authoritative_registry(
                    target_company=target_company,
                    snapshot_id=snapshot_id,
                    current_count=current_count,
                    former_count=former_count,
                )
                explained = self.orchestrator.explain_workflow({"raw_user_request": query, "top_k": 10})
                self.assertEqual(explained["organization_execution_profile"]["org_scale_band"], expected_scale)
                self.assertEqual(explained["organization_execution_profile"]["default_acquisition_mode"], expected_mode)
                self.assertEqual(explained["dispatch_preview"]["strategy"], "reuse_snapshot")
                self.assertEqual(explained["asset_reuse_plan"]["planner_mode"], "reuse_snapshot_only")
                self.assertFalse(explained["asset_reuse_plan"]["requires_delta_acquisition"])
                self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "reuse_baseline")
                self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "reuse_baseline")
                self.assertEqual(
                    explained["effective_execution_semantics"]["effective_acquisition_mode"],
                    "full_local_asset_reuse",
                )
                self.assertEqual(
                    explained["effective_execution_semantics"]["default_results_mode"],
                    "asset_population",
                )

    def test_explain_workflow_large_authoritative_baseline_low_profile_detail_still_defaults_to_population_reuse(self) -> None:
        snapshot_id = "20260416T223400-anthropic-runtime"
        self._write_company_snapshot(target_company="Anthropic", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            current_count=3014,
            former_count=429,
            profile_detail_count=15,
            explicit_profile_capture_count=15,
            completeness_score=31.24,
            completeness_band="low",
        )

        explained = self.orchestrator.explain_workflow(
            {"raw_user_request": "帮我找Anthropic里做Pre-training方向的人", "top_k": 10}
        )

        self.assertEqual(explained["dispatch_preview"]["strategy"], "reuse_snapshot")
        self.assertFalse(explained["asset_reuse_plan"]["requires_delta_acquisition"])
        self.assertTrue(explained["asset_reuse_plan"]["baseline_population_default_reuse_sufficient"])
        self.assertEqual(explained["asset_reuse_plan"]["planner_mode"], "reuse_snapshot_only")
        self.assertEqual(explained["plan"]["acquisition_strategy"]["strategy_type"], "full_company_roster")
        self.assertEqual(
            explained["effective_execution_semantics"]["effective_acquisition_mode"],
            "full_local_asset_reuse",
        )
        self.assertEqual(explained["request_preview"]["keywords"], ["Pre-train"])
        self.assertNotIn("pre_training", list(explained["request_preview"].get("must_have_facets") or []))

    def test_explain_workflow_preserves_common_directional_keywords(self) -> None:
        snapshot_id = "20260414T040103"
        self._write_company_snapshot(target_company="Anthropic", snapshot_id=snapshot_id)
        self._upsert_authoritative_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            current_count=880,
            former_count=140,
        )
        cases = [
            ("给我Anthropic做Coding方向的人", ["Coding"]),
            ("给我Anthropic做Math和Audio方向的人", ["Math", "Audio"]),
            ("给我Anthropic做Vision和Text方向的人", ["Text", "Vision"]),
        ]
        for query, expected_keywords in cases:
            with self.subTest(query=query):
                explained = self.orchestrator.explain_workflow({"raw_user_request": query, "top_k": 10})
                self.assertEqual(explained["request_preview"]["target_company"], "Anthropic")
                self.assertEqual(explained["request_preview"]["keywords"], expected_keywords)

    def test_explain_workflow_nvidia_world_model_direction_uses_large_org_scoped_search(self) -> None:
        cases = [
            ("帮我找NVDIA做世界模型方向的人", "NVIDIA"),
            ("帮我找NVIDIA做世界模型方向的人", "NVIDIA"),
        ]
        for query, expected_company in cases:
            with self.subTest(query=query):
                explained = self.orchestrator.explain_workflow({"raw_user_request": query, "top_k": 10})
                self.assertEqual(explained["request_preview"]["target_company"], expected_company)
                self.assertIn("World model", list(explained["request_preview"]["keywords"] or []))
                self.assertEqual(explained["organization_execution_profile"]["org_scale_band"], "large")
                self.assertEqual(
                    explained["organization_execution_profile"]["default_acquisition_mode"],
                    "scoped_search_roster",
                )
                self.assertEqual(explained["dispatch_preview"]["strategy"], "new_job")
                self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "live_acquisition")
                self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "live_acquisition")

    def test_explain_workflow_large_org_directional_scoped_search_matrix_uses_canonical_shards(self) -> None:
        openai_snapshot_id = "20260415T020101"
        google_snapshot_id = "20260415T020102"
        self._write_company_snapshot(target_company="OpenAI", snapshot_id=openai_snapshot_id)
        self._upsert_authoritative_registry(
            target_company="OpenAI",
            snapshot_id=openai_snapshot_id,
            current_count=153,
            former_count=44,
        )
        self._write_company_snapshot(target_company="Google", snapshot_id=google_snapshot_id)
        self._upsert_authoritative_registry(
            target_company="Google",
            snapshot_id=google_snapshot_id,
            current_count=520,
            former_count=80,
        )

        cases = [
            ("我想要OpenAI做Pre-train方向的人", "OpenAI", ["Pre-train"], "delta_from_snapshot"),
            ("我想要OpenAI做Post-train方向的人", "OpenAI", ["Post-train"], "delta_from_snapshot"),
            ("我想要OpenAI做Infra方向的人", "OpenAI", ["Infra"], "delta_from_snapshot"),
            ("我想要OpenAI做RL方向的人", "OpenAI", ["RL"], "delta_from_snapshot"),
            ("我想要OpenAI做Eval方向的人", "OpenAI", ["Eval"], "delta_from_snapshot"),
            ("我想要OpenAI做Math方向的人", "OpenAI", ["Math"], "delta_from_snapshot"),
            ("我想要OpenAI做Text方向的人", "OpenAI", ["Text"], "delta_from_snapshot"),
            ("我想要OpenAI做Audio方向的人", "OpenAI", ["Audio"], "delta_from_snapshot"),
            ("我想要OpenAI做Coding方向的人", "OpenAI", ["Coding"], "delta_from_snapshot"),
            ("帮我找Google做多模态方向的人", "Google", ["Multimodal"], "delta_from_snapshot"),
            ("帮我找Google DeepMind做多模态方向的人", "Google", ["Multimodal"], "delta_from_snapshot"),
            ("帮我找NVIDIA做世界模型方向的人", "NVIDIA", ["World model"], "new_job"),
        ]

        for query, expected_company, expected_keywords, expected_dispatch in cases:
            with self.subTest(query=query):
                explained = self.orchestrator.explain_workflow({"raw_user_request": query, "top_k": 10})
                self.assertEqual(explained["request_preview"]["target_company"], expected_company)
                self.assertEqual(explained["request_preview"]["keywords"], expected_keywords)
                self.assertEqual(
                    explained["organization_execution_profile"]["default_acquisition_mode"],
                    "scoped_search_roster",
                )
                self.assertEqual(explained["dispatch_preview"]["strategy"], expected_dispatch)
                if expected_dispatch == "delta_from_snapshot":
                    self.assertEqual(explained["asset_reuse_plan"]["planner_mode"], "delta_from_snapshot")
                    self.assertTrue(explained["asset_reuse_plan"]["requires_delta_acquisition"])
                    self.assertEqual(
                        list(explained["asset_reuse_plan"]["missing_current_profile_search_queries"] or []),
                        expected_keywords,
                    )
                    self.assertEqual(
                        list(explained["asset_reuse_plan"]["missing_former_profile_search_queries"] or []),
                        expected_keywords,
                    )
                    self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "delta_acquisition")
                    self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "delta_acquisition")
                else:
                    self.assertEqual(explained["lane_preview"]["current"]["planned_behavior"], "live_acquisition")
                    self.assertEqual(explained["lane_preview"]["former"]["planned_behavior"], "live_acquisition")

    def test_explain_workflow_exposes_generation_watermarks_and_cloud_asset_operations(self) -> None:
        snapshot_id = "20260414T030405"
        self._write_company_snapshot(target_company="OpenAI", snapshot_id=snapshot_id, candidate_count=2)
        generation = self._register_generation(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            candidate_count=2,
        )
        self._upsert_authoritative_registry(
            target_company="OpenAI",
            snapshot_id=snapshot_id,
            current_count=2,
            former_count=0,
            materialization_generation_key=str(generation.get("generation_key") or ""),
            materialization_generation_sequence=int(generation.get("generation_sequence") or 0),
            materialization_watermark=str(generation.get("generation_watermark") or ""),
        )
        self.store.record_cloud_asset_operation(
            operation_type="import_bundle",
            bundle_kind="company_snapshot",
            bundle_id="openai_bundle",
            status="completed",
            scoped_companies=["OpenAI"],
            scoped_snapshot_id=snapshot_id,
            summary={"imported_snapshot_id": snapshot_id},
        )

        explained = self.orchestrator.explain_workflow(
            {"raw_user_request": "我想要OpenAI做Reasoning方向的人"}
        )

        self.assertEqual(
            str(explained["generation_watermarks"]["baseline"]["generation_watermark"] or ""),
            str(generation.get("generation_watermark") or ""),
        )
        self.assertEqual(
            str(explained["asset_reuse_plan"]["baseline_generation_watermark"] or ""),
            str(generation.get("generation_watermark") or ""),
        )
        self.assertEqual(int(explained["cloud_asset_operations"]["count"] or 0), 1)
        self.assertEqual(
            str(explained["cloud_asset_operations"]["items"][0]["bundle_id"] or ""),
            "openai_bundle",
        )


if __name__ == "__main__":
    unittest.main()
