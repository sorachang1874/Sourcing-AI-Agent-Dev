import contextlib
import errno
import json
import os
import shutil
import tempfile
import threading
import time
import unittest
import unittest.mock
from dataclasses import dataclass
from pathlib import Path

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.asset_reuse_planning import build_acquisition_shard_registry_record
from sourcing_agent.company_registry import resolve_company_alias_key
from sourcing_agent.domain import JobRequest
from sourcing_agent.model_provider import build_model_client
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.organization_execution_profile import ensure_organization_execution_profile
from sourcing_agent.scripted_test_runtime import build_isolated_runtime_env, isolated_runtime_state_paths
from sourcing_agent.semantic_provider import build_semantic_provider
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import SQLiteStore
from sourcing_agent.workflow_explain_matrix import (
    load_explain_cases,
    run_hosted_explain_case,
    run_hosted_explain_matrix,
)
from sourcing_agent.workflow_smoke import (
    HostedWorkflowSmokeClient,
    load_smoke_cases,
    run_hosted_smoke_case,
    run_hosted_smoke_matrix,
    summarize_smoke_timings,
)

_FAST_HOSTED_SMOKE_ENV = {
    "WEB_SEARCH_READY_COOLDOWN_SECONDS": "0",
    "WEB_SEARCH_FETCH_COOLDOWN_SECONDS": "0",
    "WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS": "0",
    "WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS": "0",
    "SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS": "0",
    "SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS": "0",
    "EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS": "0",
    "EXPLORATION_FETCH_MIN_INTERVAL_SECONDS": "0",
    "DATAFORSEO_TASK_GET_BATCH_WORKERS": "4",
}


@dataclass
class HostedSmokeHarness:
    tempdir: tempfile.TemporaryDirectory
    store: SQLiteStore
    settings: AppSettings
    orchestrator: SourcingOrchestrator
    client: HostedWorkflowSmokeClient
    server: object
    thread: threading.Thread


class HostedWorkflowSmokeTest(unittest.TestCase):
    maxDiff = None

    def test_summarize_smoke_timings_aggregates_p95(self) -> None:
        summary = summarize_smoke_timings(
            [
                {"timings_ms": {"explain": 10.0, "wait_for_completion": 100.0, "total": 140.0}},
                {"timings_ms": {"explain": 20.0, "wait_for_completion": 200.0, "total": 260.0}},
                {"timings_ms": {"explain": 30.0, "wait_for_completion": 300.0, "total": 380.0}},
            ]
        )

        self.assertEqual(summary["case_count"], 3)
        self.assertEqual(summary["timings_ms"]["explain"]["avg"], 20.0)
        self.assertEqual(summary["timings_ms"]["wait_for_completion"]["max"], 300.0)
        self.assertGreater(summary["timings_ms"]["total"]["p95"], 300.0)

    def _write_runtime_identity(
        self,
        *,
        runtime_dir: Path,
        company_key: str,
        snapshot_id: str,
        requested_name: str,
        canonical_name: str,
        linkedin_slug: str,
        aliases: list[str],
        confidence: str = "high",
    ) -> None:
        identity_path = runtime_dir / "company_assets" / company_key / snapshot_id / "identity.json"
        identity_path.parent.mkdir(parents=True, exist_ok=True)
        identity_path.write_text(
            json.dumps(
                {
                    "requested_name": requested_name,
                    "canonical_name": canonical_name,
                    "company_key": company_key,
                    "linkedin_slug": linkedin_slug,
                    "aliases": aliases,
                    "confidence": confidence,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    @contextlib.contextmanager
    def _hosted_harness(
        self,
        *,
        provider_mode: str,
        scripted_scenario: str = "",
        inject_fast_runtime_env: bool = True,
    ):
        tempdir = tempfile.TemporaryDirectory()
        runtime_paths = isolated_runtime_state_paths(tempdir.name)
        extra_env = dict(_FAST_HOSTED_SMOKE_ENV) if inject_fast_runtime_env else {}
        env_payload, _runtime_env_file = build_isolated_runtime_env(
            runtime_dir=tempdir.name,
            provider_mode=provider_mode,
            scripted_scenario=scripted_scenario,
            extra_env=extra_env,
        )
        patcher = unittest.mock.patch.dict(os.environ, env_payload, clear=False)
        patcher.start()
        server = None
        thread = None
        store = None
        try:
            catalog = AssetCatalog.discover()
            settings = AppSettings(
                project_root=Path(tempdir.name),
                runtime_dir=runtime_paths["runtime_dir"],
                secrets_file=runtime_paths["secrets_file"],
                jobs_dir=runtime_paths["jobs_dir"],
                company_assets_dir=runtime_paths["company_assets_dir"],
                db_path=runtime_paths["db_path"],
                qwen=QwenSettings(enabled=False),
                semantic=SemanticProviderSettings(enabled=False),
                harvest=HarvestSettings(
                    profile_scraper=HarvestActorSettings(
                        enabled=True,
                        api_token="test-token",
                        actor_id="test-profile-scraper",
                        default_mode="short",
                        max_paid_items=2500,
                    ),
                    profile_search=HarvestActorSettings(
                        enabled=True,
                        api_token="test-token",
                        actor_id="test-profile-search",
                        default_mode="short",
                        max_paid_items=2500,
                    ),
                    company_employees=HarvestActorSettings(
                        enabled=True,
                        api_token="test-token",
                        actor_id="test-company-employees",
                        default_mode="short",
                        max_paid_items=2500,
                    ),
                ),
            )
            store = SQLiteStore(str(settings.db_path))
            model_client = build_model_client(qwen_settings=settings.qwen)
            semantic_provider = build_semantic_provider(settings.semantic)
            acquisition_engine = AcquisitionEngine(catalog, settings, store, model_client)
            orchestrator = SourcingOrchestrator(
                catalog=catalog,
                store=store,
                jobs_dir=str(settings.jobs_dir),
                model_client=model_client,
                semantic_provider=semantic_provider,
                acquisition_engine=acquisition_engine,
            )
            server = create_server(orchestrator, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            yield HostedSmokeHarness(
                tempdir=tempdir,
                store=store,
                settings=settings,
                orchestrator=orchestrator,
                client=HostedWorkflowSmokeClient(f"http://{host}:{port}"),
                server=server,
                thread=thread,
            )
        finally:
            if server is not None:
                server.shutdown()
                server.server_close()
            if thread is not None:
                thread.join(timeout=5)
            runtime_thread_prefixes = (
                "hosted-workflow-",
                "hosted-stage2-",
                "continue-stage2-",
                "progress-auto-takeover-",
                "workflow-job-lease-",
                "workflow-runtime-controls-",
                "hosted-runtime-watchdog-deferred-",
                "shared-recovery-deferred-",
                "background-outreach-layering-",
                "background-snapshot-materialization-",
                "plan-hydration-",
                "excel-intake-",
                "organization-asset-warmup",
            )
            runtime_thread_markers = (
                "worker-recovery",
                "runtime-watchdog",
                "workflow-recovery",
            )
            for _ in range(20):
                active_runtime_threads = [
                    item
                    for item in threading.enumerate()
                    if item.is_alive()
                    and (
                        any(item.name.startswith(prefix) for prefix in runtime_thread_prefixes)
                        or (
                            item.name.endswith("-thread")
                            and any(marker in item.name for marker in runtime_thread_markers)
                        )
                    )
                ]
                if not active_runtime_threads:
                    break
                for active_thread in active_runtime_threads:
                    active_thread.join(timeout=0.1)
                time.sleep(0.05)
            connection = getattr(store, "_connection", None)
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
            patcher.stop()
            tempdir_root = Path(tempdir.name)
            cleanup_error = None
            for attempt in range(8):
                try:
                    if tempdir_root.exists():
                        shutil.rmtree(tempdir_root)
                    tempdir.cleanup()
                    cleanup_error = None
                    break
                except FileNotFoundError:
                    tempdir.cleanup()
                    cleanup_error = None
                    break
                except OSError as exc:
                    if exc.errno not in {errno.ENOTEMPTY, errno.EBUSY}:
                        raise
                    cleanup_error = exc
                    time.sleep(0.1 * (attempt + 1))
            if cleanup_error is not None:
                raise cleanup_error

    def _write_candidate_documents(
        self,
        *,
        harness: HostedSmokeHarness,
        target_company: str,
        snapshot_id: str,
        claimed_candidate_count: int,
        current_count: int,
        former_count: int,
        candidate_specs: list[dict[str, str]],
    ) -> Path:
        company_key = resolve_company_alias_key(target_company)
        snapshot_dir = harness.settings.company_assets_dir / company_key / snapshot_id
        normalized_dir = snapshot_dir / "normalized_artifacts"
        normalized_dir.mkdir(parents=True, exist_ok=True)
        identity = {
            "requested_name": target_company,
            "canonical_name": target_company,
            "company_key": company_key,
            "linkedin_slug": company_key,
            "aliases": [target_company],
        }
        candidates: list[dict[str, object]] = []
        for index, spec in enumerate(candidate_specs, start=1):
            employment_status = str(spec.get("employment_status") or "current")
            slug = str(spec.get("slug") or f"{company_key}-{index}")
            focus_text = str(spec.get("focus_text") or "")
            role = str(spec.get("role") or "Research Engineer")
            candidates.append(
                {
                    "candidate_id": f"{company_key}-{index}",
                    "name_en": str(spec.get("name") or f"{target_company} Candidate {index}"),
                    "display_name": str(spec.get("name") or f"{target_company} Candidate {index}"),
                    "category": "employee",
                    "target_company": target_company,
                    "organization": target_company,
                    "employment_status": employment_status,
                    "role": role,
                    "focus_areas": focus_text,
                    "work_history": f"{target_company} | {focus_text}",
                    "notes": focus_text,
                    "linkedin_url": f"https://www.linkedin.com/in/{slug}/",
                    "source_dataset": f"{company_key}_snapshot",
                    "metadata": {
                        "profile_url": f"https://www.linkedin.com/in/{slug}/",
                        "public_identifier": slug,
                    },
                }
            )
        candidate_payload = {
            "snapshot": {
                "snapshot_id": snapshot_id,
                "company_identity": identity,
            },
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "candidates": candidates,
            "evidence": [],
            "candidate_count": claimed_candidate_count,
            "evidence_count": 0,
        }
        artifact_summary = {
            "snapshot_id": snapshot_id,
            "target_company": target_company,
            "candidate_count": claimed_candidate_count,
            "evidence_count": 0,
            "profile_detail_count": claimed_candidate_count,
            "missing_linkedin_count": 0,
            "current_lane_coverage": {
                "effective_candidate_count": current_count,
                "effective_ready": current_count > 0,
            },
            "former_lane_coverage": {
                "effective_candidate_count": former_count,
                "effective_ready": former_count > 0,
            },
        }
        (snapshot_dir / "identity.json").write_text(
            json.dumps(identity, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        (snapshot_dir.parent / "latest_snapshot.json").write_text(
            json.dumps({"snapshot_id": snapshot_id, "company_identity": identity}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (snapshot_dir / "candidate_documents.json").write_text(
            json.dumps(candidate_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (normalized_dir / "materialized_candidate_documents.json").write_text(
            json.dumps(candidate_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (normalized_dir / "artifact_summary.json").write_text(
            json.dumps(artifact_summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return snapshot_dir

    def _seed_authoritative_company_asset(
        self,
        *,
        harness: HostedSmokeHarness,
        target_company: str,
        snapshot_id: str,
        current_count: int,
        former_count: int,
        candidate_specs: list[dict[str, str]],
    ) -> None:
        claimed_candidate_count = max(current_count + former_count, len(candidate_specs))
        snapshot_dir = self._write_candidate_documents(
            harness=harness,
            target_company=target_company,
            snapshot_id=snapshot_id,
            claimed_candidate_count=claimed_candidate_count,
            current_count=current_count,
            former_count=former_count,
            candidate_specs=candidate_specs,
        )
        company_key = resolve_company_alias_key(target_company)
        summary_payload = {
            "target_company": target_company,
            "snapshot_id": snapshot_id,
            "candidate_count": claimed_candidate_count,
            "profile_detail_count": claimed_candidate_count,
            "current_lane_coverage": {
                "effective_candidate_count": current_count,
                "effective_ready": current_count > 0,
            },
            "former_lane_coverage": {
                "effective_candidate_count": former_count,
                "effective_ready": former_count > 0,
            },
        }
        harness.store.upsert_organization_asset_registry(
            {
                "target_company": target_company,
                "company_key": company_key,
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "authoritative": True,
                "candidate_count": claimed_candidate_count,
                "evidence_count": 0,
                "profile_detail_count": claimed_candidate_count,
                "explicit_profile_capture_count": claimed_candidate_count,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "completeness_score": 90.0,
                "completeness_band": "high",
                "current_lane_coverage": summary_payload["current_lane_coverage"],
                "former_lane_coverage": summary_payload["former_lane_coverage"],
                "current_lane_effective_candidate_count": current_count,
                "former_lane_effective_candidate_count": former_count,
                "current_lane_effective_ready": current_count > 0,
                "former_lane_effective_ready": former_count > 0,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {"selected_snapshot_ids": [snapshot_id]},
                "source_path": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
                "source_job_id": "",
                "summary": summary_payload,
            },
            authoritative=True,
        )
        ensure_organization_execution_profile(
            runtime_dir=harness.settings.runtime_dir,
            store=harness.store,
            target_company=target_company,
            asset_view="canonical_merged",
        )

    def _seed_reference_org_assets(self, harness: HostedSmokeHarness) -> None:
        self._seed_authoritative_company_asset(
            harness=harness,
            target_company="Skild AI",
            snapshot_id="20260414T120000",
            current_count=153,
            former_count=22,
            candidate_specs=[
                {
                    "name": "Skild Pretrain",
                    "slug": "skild-pretrain",
                    "role": "Pre-train Engineer",
                    "focus_text": "Pre-train systems, foundation models, model training infrastructure.",
                },
                {
                    "name": "Skild Systems",
                    "slug": "skild-systems",
                    "role": "Training Systems Engineer",
                    "focus_text": "Large-scale model training, distributed systems, pretraining pipelines.",
                },
            ],
        )
        self._seed_authoritative_company_asset(
            harness=harness,
            target_company="Humans&",
            snapshot_id="20260414T120100",
            current_count=520,
            former_count=80,
            candidate_specs=[
                {
                    "name": "Humans Coding",
                    "slug": "humans-coding",
                    "role": "Research Scientist",
                    "focus_text": "Coding agents, tool use, agent reliability, evaluation.",
                },
                {
                    "name": "Humans Infra",
                    "slug": "humans-infra",
                    "role": "Member of Technical Staff",
                    "focus_text": "Coding systems, agent platform, infrastructure for coding models.",
                },
            ],
        )
        self._seed_authoritative_company_asset(
            harness=harness,
            target_company="Anthropic",
            snapshot_id="20260414T120200",
            current_count=880,
            former_count=140,
            candidate_specs=[
                {
                    "name": "Anthropic Pretraining",
                    "slug": "anthropic-pretraining",
                    "role": "Research Scientist",
                    "focus_text": "Pre-training, scaling laws, language model training.",
                },
                {
                    "name": "Anthropic Safety Pretrain",
                    "slug": "anthropic-safety-pretrain",
                    "role": "Research Engineer",
                    "focus_text": "Pre-training systems, safety-aware model training, large-scale experimentation.",
                },
            ],
        )
        self._seed_authoritative_company_asset(
            harness=harness,
            target_company="OpenAI",
            snapshot_id="20260414T120300",
            current_count=1400,
            former_count=220,
            candidate_specs=[
                {
                    "name": "OpenAI Reasoning",
                    "slug": "openai-reasoning",
                    "role": "Research Scientist",
                    "focus_text": "Reasoning models, chain-of-thought, o-series model development.",
                },
                {
                    "name": "OpenAI Reasoning Engineer",
                    "slug": "openai-reasoning-eng",
                    "role": "Software Engineer",
                    "focus_text": "Reasoning systems, inference optimization, model evaluation.",
                },
            ],
        )
        openai_reasoning_snapshot_id = "20260414T120301"
        openai_reasoning_current_specs = [
            {
                "name": "OpenAI Reasoning Current",
                "slug": "openai-reasoning-current",
                "role": "Research Scientist",
                "focus_text": "Reasoning models, chain-of-thought, inference research.",
            }
        ]
        openai_reasoning_former_specs = [
            {
                "name": "OpenAI Reasoning Former",
                "slug": "openai-reasoning-former",
                "role": "Former Research Scientist",
                "employment_status": "former",
                "focus_text": "Former OpenAI reasoning and evaluation researcher.",
            }
        ]
        openai_current_row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=openai_reasoning_snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="Reasoning-current",
            shard_title="Reasoning",
            search_query="Reasoning",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Reasoning",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        openai_current_generation = self._register_materialization_generation(
            harness=harness,
            target_company="OpenAI",
            snapshot_id=openai_reasoning_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(openai_current_row.get("shard_key") or "reasoning-current"),
            candidate_specs=openai_reasoning_current_specs,
            lane="profile_search",
            employment_scope="current",
        )
        openai_current_row["materialization_generation_key"] = str(
            openai_current_generation.get("generation_key") or ""
        )
        openai_current_row["materialization_generation_sequence"] = int(
            openai_current_generation.get("generation_sequence") or 0
        )
        openai_current_row["materialization_watermark"] = str(
            openai_current_generation.get("generation_watermark") or ""
        )
        harness.store.upsert_acquisition_shard_registry(openai_current_row)

        openai_former_row = build_acquisition_shard_registry_record(
            target_company="OpenAI",
            company_key="openai",
            snapshot_id=openai_reasoning_snapshot_id,
            lane="profile_search",
            employment_scope="former",
            strategy_type="former_employee_search",
            shard_id="Reasoning-former",
            shard_title="Reasoning",
            search_query="Reasoning",
            company_filters={
                "companies": ["https://www.linkedin.com/company/openai/"],
                "function_ids": ["24", "8"],
                "search_query": "Reasoning",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        openai_former_generation = self._register_materialization_generation(
            harness=harness,
            target_company="OpenAI",
            snapshot_id=openai_reasoning_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(openai_former_row.get("shard_key") or "reasoning-former"),
            candidate_specs=openai_reasoning_former_specs,
            lane="profile_search",
            employment_scope="former",
        )
        openai_former_row["materialization_generation_key"] = str(openai_former_generation.get("generation_key") or "")
        openai_former_row["materialization_generation_sequence"] = int(
            openai_former_generation.get("generation_sequence") or 0
        )
        openai_former_row["materialization_watermark"] = str(openai_former_generation.get("generation_watermark") or "")
        harness.store.upsert_acquisition_shard_registry(openai_former_row)
        self._append_selected_snapshots(
            harness=harness,
            target_company="OpenAI",
            snapshot_ids=[openai_reasoning_snapshot_id],
        )
        self._seed_authoritative_company_asset(
            harness=harness,
            target_company="Google",
            snapshot_id="20260414T120400",
            current_count=5200,
            former_count=800,
            candidate_specs=[
                {
                    "name": "Google Veo",
                    "slug": "google-veo",
                    "role": "Research Engineer",
                    "focus_text": "Veo, multimodal generation, video generation, pre-train systems at Google DeepMind.",
                },
                {
                    "name": "Google Nano Banana",
                    "slug": "google-nano-banana",
                    "role": "Research Scientist",
                    "focus_text": "Nano Banana, multimodal generation, vision-language, pre-train workflows at Google DeepMind.",
                },
                {
                    "name": "Google Former Multimodal",
                    "slug": "google-former-multimodal",
                    "role": "Former Staff Researcher",
                    "employment_status": "former",
                    "focus_text": "Former Google multimodal and pre-train researcher from Veo-related work.",
                },
            ],
        )

    def _register_materialization_generation(
        self,
        *,
        harness: HostedSmokeHarness,
        target_company: str,
        snapshot_id: str,
        artifact_kind: str,
        artifact_key: str,
        candidate_specs: list[dict[str, str]],
        lane: str = "",
        employment_scope: str = "",
    ) -> dict[str, object]:
        company_key = resolve_company_alias_key(target_company)
        members = []
        for index, spec in enumerate(candidate_specs, start=1):
            slug = str(spec.get("slug") or f"{company_key}-{artifact_key}-{index}")
            linkedin_url = f"https://www.linkedin.com/in/{slug}/"
            members.append(
                {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "artifact_kind": artifact_kind,
                    "artifact_key": artifact_key,
                    "lane": lane,
                    "employment_scope": str(spec.get("employment_status") or employment_scope or "current"),
                    "member_key": harness.store.normalize_linkedin_profile_url(linkedin_url),
                    "member_key_kind": "profile_url_key",
                    "candidate_id": f"{company_key}-{artifact_key}-{index}",
                    "profile_url_key": harness.store.normalize_linkedin_profile_url(linkedin_url),
                    "metadata": {
                        "display_name": str(spec.get("name") or f"{target_company} Candidate {index}"),
                        "role": str(spec.get("role") or ""),
                    },
                }
            )
        return harness.store.register_asset_materialization(
            target_company=target_company,
            snapshot_id=snapshot_id,
            asset_view="canonical_merged",
            artifact_kind=artifact_kind,
            artifact_key=artifact_key,
            source_path=str(
                harness.settings.company_assets_dir
                / company_key
                / snapshot_id
                / "normalized_artifacts"
                / "artifact_summary.json"
            ),
            summary={
                "target_company": target_company,
                "snapshot_id": snapshot_id,
                "candidate_count": len(candidate_specs),
            },
            metadata={"test_case": "hosted_explain_matrix"},
            members=members,
        )

    def _append_selected_snapshots(
        self,
        *,
        harness: HostedSmokeHarness,
        target_company: str,
        snapshot_ids: list[str],
    ) -> None:
        row = harness.store.get_authoritative_organization_asset_registry(target_company=target_company)
        selected_snapshot_ids = list(row.get("selected_snapshot_ids") or [])
        for snapshot_id in snapshot_ids:
            if snapshot_id and snapshot_id not in selected_snapshot_ids:
                selected_snapshot_ids.append(snapshot_id)
        row["selected_snapshot_ids"] = selected_snapshot_ids
        row["source_snapshot_selection"] = {"selected_snapshot_ids": selected_snapshot_ids}
        row["source_snapshot_count"] = len(selected_snapshot_ids)
        summary = dict(row.get("summary") or {})
        summary["selected_snapshot_ids"] = selected_snapshot_ids
        row["summary"] = summary
        harness.store.upsert_organization_asset_registry(row, authoritative=True)
        ensure_organization_execution_profile(
            runtime_dir=harness.settings.runtime_dir,
            store=harness.store,
            target_company=target_company,
            asset_view="canonical_merged",
        )

    def _seed_explain_matrix_assets(self, harness: HostedSmokeHarness) -> None:
        self._seed_reference_org_assets(harness)
        self._write_runtime_identity(
            runtime_dir=harness.settings.runtime_dir,
            company_key="physicalintelligence",
            snapshot_id="20260415T010203",
            requested_name="Physical Intelligence",
            canonical_name="Physical Intelligence",
            linkedin_slug="physical-intelligence",
            aliases=["physical intelligence", "pi"],
        )
        self._seed_authoritative_company_asset(
            harness=harness,
            target_company="Reflection AI",
            snapshot_id="20260414T120450",
            current_count=120,
            former_count=18,
            candidate_specs=[
                {
                    "name": "Reflection Post-train",
                    "slug": "reflection-post-train",
                    "role": "Member of Technical Staff",
                    "focus_text": "Post-train, alignment, RLHF, evaluation.",
                },
                {
                    "name": "Reflection Former Post-train",
                    "slug": "reflection-former-post-train",
                    "role": "Former Research Engineer",
                    "employment_status": "former",
                    "focus_text": "Former Reflection AI post-train and eval researcher.",
                },
            ],
        )
        reflection_posttrain_snapshot_id = "20260414T120451"
        reflection_current_specs = [
            {
                "name": "Reflection Post-train Current",
                "slug": "reflection-post-train-current",
                "role": "Member of Technical Staff",
                "focus_text": "Post-train, alignment, RLHF, evaluation.",
            }
        ]
        reflection_former_specs = [
            {
                "name": "Reflection Post-train Former",
                "slug": "reflection-post-train-former",
                "role": "Former Research Engineer",
                "employment_status": "former",
                "focus_text": "Former Reflection AI post-train and eval researcher.",
            }
        ]
        reflection_current_row = build_acquisition_shard_registry_record(
            target_company="Reflection AI",
            company_key="reflectionai",
            snapshot_id=reflection_posttrain_snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="Post-train-current",
            shard_title="Post-train",
            search_query="Post-train",
            company_filters={
                "companies": ["https://www.linkedin.com/company/reflectionai/"],
                "search_query": "Post-train",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        reflection_current_generation = self._register_materialization_generation(
            harness=harness,
            target_company="Reflection AI",
            snapshot_id=reflection_posttrain_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(reflection_current_row.get("shard_key") or "post-train-current"),
            candidate_specs=reflection_current_specs,
            lane="profile_search",
            employment_scope="current",
        )
        reflection_current_row["materialization_generation_key"] = str(
            reflection_current_generation.get("generation_key") or ""
        )
        reflection_current_row["materialization_generation_sequence"] = int(
            reflection_current_generation.get("generation_sequence") or 0
        )
        reflection_current_row["materialization_watermark"] = str(
            reflection_current_generation.get("generation_watermark") or ""
        )
        harness.store.upsert_acquisition_shard_registry(reflection_current_row)

        reflection_former_row = build_acquisition_shard_registry_record(
            target_company="Reflection AI",
            company_key="reflectionai",
            snapshot_id=reflection_posttrain_snapshot_id,
            lane="profile_search",
            employment_scope="former",
            strategy_type="former_employee_search",
            shard_id="Post-train-former",
            shard_title="Post-train",
            search_query="Post-train",
            company_filters={
                "companies": ["https://www.linkedin.com/company/reflectionai/"],
                "search_query": "Post-train",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        reflection_former_generation = self._register_materialization_generation(
            harness=harness,
            target_company="Reflection AI",
            snapshot_id=reflection_posttrain_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(reflection_former_row.get("shard_key") or "post-train-former"),
            candidate_specs=reflection_former_specs,
            lane="profile_search",
            employment_scope="former",
        )
        reflection_former_row["materialization_generation_key"] = str(
            reflection_former_generation.get("generation_key") or ""
        )
        reflection_former_row["materialization_generation_sequence"] = int(
            reflection_former_generation.get("generation_sequence") or 0
        )
        reflection_former_row["materialization_watermark"] = str(
            reflection_former_generation.get("generation_watermark") or ""
        )
        harness.store.upsert_acquisition_shard_registry(reflection_former_row)
        self._append_selected_snapshots(
            harness=harness,
            target_company="Reflection AI",
            snapshot_ids=[reflection_posttrain_snapshot_id],
        )

        google_multimodal_snapshot_id = "20260414T120401"
        google_multimodal_current_specs = [
            {
                "name": "Google Multimodal Current",
                "slug": "google-multimodal-current",
                "role": "Research Engineer",
                "focus_text": "Multimodal Veo video generation systems.",
            }
        ]
        google_multimodal_former_specs = [
            {
                "name": "Google Multimodal Former",
                "slug": "google-multimodal-former",
                "role": "Former Research Scientist",
                "employment_status": "former",
                "focus_text": "Former Google DeepMind multimodal and Veo work.",
            }
        ]
        google_current_row = build_acquisition_shard_registry_record(
            target_company="Google",
            company_key="google",
            snapshot_id=google_multimodal_snapshot_id,
            lane="profile_search",
            employment_scope="current",
            strategy_type="scoped_search_roster",
            shard_id="Multimodal-current",
            shard_title="Multimodal",
            search_query="Multimodal",
            company_filters={
                "companies": [
                    "https://www.linkedin.com/company/google/",
                    "https://www.linkedin.com/company/deepmind/",
                ],
                "function_ids": ["8", "9", "19", "24"],
                "search_query": "Multimodal",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        google_current_generation = self._register_materialization_generation(
            harness=harness,
            target_company="Google",
            snapshot_id=google_multimodal_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(google_current_row.get("shard_key") or "multimodal-current"),
            candidate_specs=google_multimodal_current_specs,
            lane="profile_search",
            employment_scope="current",
        )
        google_current_row["materialization_generation_key"] = str(
            google_current_generation.get("generation_key") or ""
        )
        google_current_row["materialization_generation_sequence"] = int(
            google_current_generation.get("generation_sequence") or 0
        )
        google_current_row["materialization_watermark"] = str(
            google_current_generation.get("generation_watermark") or ""
        )
        harness.store.upsert_acquisition_shard_registry(google_current_row)

        google_former_row = build_acquisition_shard_registry_record(
            target_company="Google",
            company_key="google",
            snapshot_id=google_multimodal_snapshot_id,
            lane="profile_search",
            employment_scope="former",
            strategy_type="former_employee_search",
            shard_id="Multimodal-former",
            shard_title="Multimodal",
            search_query="Multimodal",
            company_filters={
                "companies": [
                    "https://www.linkedin.com/company/google/",
                    "https://www.linkedin.com/company/deepmind/",
                ],
                "function_ids": ["8", "9", "19", "24"],
                "search_query": "Multimodal",
            },
            result_count=1,
            estimated_total_count=1,
            status="completed",
        )
        google_former_generation = self._register_materialization_generation(
            harness=harness,
            target_company="Google",
            snapshot_id=google_multimodal_snapshot_id,
            artifact_kind="acquisition_shard_bundle",
            artifact_key=str(google_former_row.get("shard_key") or "multimodal-former"),
            candidate_specs=google_multimodal_former_specs,
            lane="profile_search",
            employment_scope="former",
        )
        google_former_row["materialization_generation_key"] = str(google_former_generation.get("generation_key") or "")
        google_former_row["materialization_generation_sequence"] = int(
            google_former_generation.get("generation_sequence") or 0
        )
        google_former_row["materialization_watermark"] = str(google_former_generation.get("generation_watermark") or "")
        harness.store.upsert_acquisition_shard_registry(google_former_row)
        self._append_selected_snapshots(
            harness=harness,
            target_company="Google",
            snapshot_ids=[google_multimodal_snapshot_id],
        )

    def _load_explain_case(self, case_name: str) -> dict[str, object]:
        cases = load_explain_cases("", {case_name})
        self.assertEqual(len(cases), 1)
        return dict(cases[0])

    def test_default_smoke_matrix_covers_reference_orgs(self) -> None:
        cases = load_smoke_cases()
        self.assertEqual(
            [str(item.get("case") or "") for item in cases],
            [
                "skild_pretrain",
                "humansand_coding",
                "anthropic_pretraining",
                "xai_full_roster",
                "xai_coding_all_members_scoped",
                "openai_reasoning",
                "google_multimodal_pretrain",
            ],
        )

    def test_default_explain_matrix_covers_reference_regressions(self) -> None:
        cases = load_explain_cases()
        self.assertEqual(
            [str(item.get("case") or "") for item in cases],
            [
                "physical_intelligence_full_roster",
                "reflection_posttrain_reuse",
                "humansand_coding_reuse",
                "anthropic_pretraining_reuse",
                "xai_full_roster_live",
                "xai_coding_all_members_scoped",
                "openai_reasoning_reuse",
                "openai_pretrain_delta",
                "google_multimodal_pretrain_delta",
                "nvidia_world_model_new_job",
            ],
        )

    def test_hosted_explain_dry_run_matrix_covers_reference_regressions(self) -> None:
        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_explain_matrix_assets(harness)
            cases = load_explain_cases()
            summaries, failures = run_hosted_explain_matrix(harness.client, cases=cases)

            self.assertFalse(failures, json.dumps(summaries, ensure_ascii=False, indent=2))
            by_case = {str(item.get("case") or ""): item for item in summaries}
            self.assertEqual(
                by_case["physical_intelligence_full_roster"]["summary"]["target_company"], "Physical Intelligence"
            )
            self.assertEqual(
                by_case["anthropic_pretraining_reuse"]["summary"]["effective_acquisition_mode"],
                "full_local_asset_reuse",
            )
            self.assertEqual(
                by_case["anthropic_pretraining_reuse"]["summary"]["default_results_mode"],
                "asset_population",
            )
            self.assertEqual(
                by_case["xai_full_roster_live"]["summary"]["plan_primary_strategy_type"], "full_company_roster"
            )
            self.assertEqual(
                by_case["xai_full_roster_live"]["summary"]["plan_company_employee_shard_strategy"],
                "adaptive_us_technical_partition",
            )
            self.assertTrue(
                by_case["xai_full_roster_live"]["summary"]["plan_company_employee_shard_policy_allow_overflow_partial"]
            )
            self.assertEqual(
                by_case["xai_coding_all_members_scoped"]["summary"]["plan_primary_strategy_type"],
                "scoped_search_roster",
            )
            self.assertEqual(by_case["openai_reasoning_reuse"]["summary"]["dispatch_strategy"], "reuse_snapshot")
            self.assertEqual(by_case["openai_pretrain_delta"]["summary"]["dispatch_strategy"], "delta_from_snapshot")
            self.assertIn(
                "Multimodal",
                list(
                    by_case["google_multimodal_pretrain_delta"]["summary"]["covered_current_profile_search_queries"]
                    or []
                ),
            )
            self.assertIn("timing_breakdown_ms", by_case["openai_pretrain_delta"]["summary"])
            self.assertIn(
                "prepare_request",
                dict(by_case["openai_pretrain_delta"]["summary"].get("timing_breakdown_ms") or {}),
            )

    def test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs(self) -> None:
        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_reference_org_assets(harness)
            cases = load_smoke_cases("", {"skild_pretrain", "humansand_coding", "openai_reasoning"})
            summaries, failures = run_hosted_smoke_matrix(
                client=harness.client,
                cases=cases,
                reviewer="hosted-simulate-smoke",
                poll_seconds=0.1,
                max_poll_seconds=40.0,
                runtime_tuning_profile="fast_smoke",
            )

            self.assertFalse(failures, json.dumps(summaries, ensure_ascii=False, indent=2))
            by_case = {str(item.get("case") or ""): item for item in summaries}
            self.assertEqual(by_case["skild_pretrain"]["explain"]["target_company"], "Skild AI")
            self.assertEqual(by_case["skild_pretrain"]["explain"]["org_scale_band"], "small")
            self.assertEqual(by_case["skild_pretrain"]["explain"]["default_acquisition_mode"], "full_company_roster")
            self.assertEqual(by_case["humansand_coding"]["explain"]["org_scale_band"], "small")
            self.assertEqual(by_case["humansand_coding"]["explain"]["default_acquisition_mode"], "full_company_roster")
            self.assertEqual(by_case["openai_reasoning"]["explain"]["org_scale_band"], "large")

            for summary in summaries:
                self.assertEqual((summary.get("final") or {}).get("job_status"), "completed")
                self.assertGreater(float(dict(summary.get("timings_ms") or {}).get("total") or 0.0), 0.0)
                self.assertIn("wait_for_completion", dict(summary.get("timings_ms") or {}))
                stage_summaries = dict((summary.get("final") or {}).get("stage_summaries") or {})
                self.assertIn("linkedin_stage_1", stage_summaries)
                self.assertIn("stage_1_preview", stage_summaries)
                self.assertIn("public_web_stage_2", stage_summaries)
                self.assertEqual(dict(stage_summaries.get("stage_1_preview") or {}).get("status"), "completed")
                self.assertEqual(dict(stage_summaries.get("stage_2_final") or {}).get("status"), "completed")

            runtime_health = harness.client.get("/api/runtime/health?force_refresh=1")
            self.assertEqual(runtime_health.get("status"), "ok")
            metrics = dict(runtime_health.get("metrics") or {})
            self.assertEqual(int(metrics.get("stale_acquiring_job_count") or 0), 0)
            self.assertEqual(int(metrics.get("stale_queue_job_count") or 0), 0)

    def test_hosted_simulate_smoke_xai_boundary_and_anthropic_asset_population(self) -> None:
        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_reference_org_assets(harness)
            explain_cases = load_explain_cases("", {"xai_coding_all_members_scoped"})
            explain_summaries, explain_failures = run_hosted_explain_matrix(harness.client, cases=explain_cases)
            self.assertFalse(explain_failures, json.dumps(explain_summaries, ensure_ascii=False, indent=2))
            explain_by_case = {str(item.get("case") or ""): item for item in explain_summaries}
            self.assertEqual(
                explain_by_case["xai_coding_all_members_scoped"]["summary"]["plan_primary_strategy_type"],
                "scoped_search_roster",
            )

            cases = load_smoke_cases("", {"anthropic_pretraining", "xai_full_roster"})
            summaries, failures = run_hosted_smoke_matrix(
                client=harness.client,
                cases=cases,
                reviewer="hosted-simulate-xai-boundary",
                poll_seconds=0.1,
                max_poll_seconds=40.0,
                runtime_tuning_profile="fast_smoke",
            )

            self.assertFalse(failures, json.dumps(summaries, ensure_ascii=False, indent=2))
            by_case = {str(item.get("case") or ""): item for item in summaries}

            self.assertEqual(
                by_case["anthropic_pretraining"]["explain"]["effective_acquisition_mode"], "full_local_asset_reuse"
            )
            self.assertEqual(by_case["anthropic_pretraining"]["final"]["default_results_mode"], "asset_population")
            self.assertTrue(by_case["anthropic_pretraining"]["final"]["asset_population_available"])
            self.assertEqual(by_case["anthropic_pretraining"]["final"]["asset_population_candidate_count"], 2)

            self.assertEqual(by_case["xai_full_roster"]["explain"]["plan_primary_strategy_type"], "full_company_roster")

            for summary in summaries:
                self.assertEqual((summary.get("final") or {}).get("job_status"), "completed")

    def test_hosted_simulate_large_org_full_roster_existing_baseline_defaults_to_asset_population(self) -> None:
        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_authoritative_company_asset(
                harness=harness,
                target_company="xAI",
                snapshot_id="20260414T121000",
                current_count=2600,
                former_count=320,
                candidate_specs=[
                    {
                        "name": "xAI Systems",
                        "slug": "xai-systems",
                        "role": "ML Systems Engineer",
                        "focus_text": "Distributed systems, training infrastructure, and inference platform.",
                    },
                    {
                        "name": "xAI Research",
                        "slug": "xai-research",
                        "role": "Research Scientist",
                        "focus_text": "Frontier-model research, reasoning, and post-training.",
                    },
                    {
                        "name": "xAI Former",
                        "slug": "xai-former",
                        "role": "Former Research Engineer",
                        "employment_status": "former",
                        "focus_text": "Former xAI research engineer focused on evaluation and data systems.",
                    },
                ],
            )
            record = run_hosted_smoke_case(
                client=harness.client,
                case_name="xai_full_roster_existing_baseline",
                payload={
                    "raw_user_request": "给我 xAI 的所有成员",
                    "top_k": 10,
                },
                reviewer="hosted-simulate-large-org-baseline",
                poll_seconds=0.1,
                max_poll_seconds=40.0,
                runtime_tuning_profile="fast_smoke",
            )

            self.assertEqual(
                (record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertEqual((record.get("explain") or {}).get("plan_primary_strategy_type"), "full_company_roster")
            self.assertEqual((record.get("final") or {}).get("default_results_mode"), "asset_population")
            self.assertTrue((record.get("final") or {}).get("asset_population_available"))
            self.assertGreaterEqual(int((record.get("final") or {}).get("asset_population_candidate_count") or 0), 1)
            self.assertNotEqual(str((record.get("explain") or {}).get("dispatch_strategy") or ""), "new_job")

    def test_authoritative_snapshot_candidate_page_ignores_non_patch_overlay_prefix_drift(self) -> None:
        with self._hosted_harness(provider_mode="simulate") as harness:
            candidate_specs = [
                {
                    "name": "xAI Systems",
                    "slug": "xai-systems",
                    "role": "ML Systems Engineer",
                    "focus_text": "Distributed systems, training infrastructure, and inference platform.",
                },
                {
                    "name": "xAI Research",
                    "slug": "xai-research",
                    "role": "Research Scientist",
                    "focus_text": "Frontier-model research, reasoning, and post-training.",
                },
                {
                    "name": "xAI Former",
                    "slug": "xai-former",
                    "role": "Former Research Engineer",
                    "employment_status": "former",
                    "focus_text": "Former xAI research engineer focused on evaluation and data systems.",
                },
            ] + [
                {
                    "name": f"xAI Candidate {index:03d}",
                    "slug": f"xai-{index:03d}",
                    "role": "Research Engineer" if index % 2 else "Research Scientist",
                    "employment_status": "former" if index % 6 == 0 else "current",
                    "focus_text": (
                        "Reasoning, Coding, inference systems, evaluation"
                        if index % 3
                        else "Pre-training, multimodal, data systems, scaling"
                    ),
                }
                for index in range(4, 321)
            ]
            snapshot_id = "20260414T121000"
            self._seed_authoritative_company_asset(
                harness=harness,
                target_company="xAI",
                snapshot_id=snapshot_id,
                current_count=2600,
                former_count=320,
                candidate_specs=candidate_specs,
            )
            company_key = resolve_company_alias_key("xAI")
            snapshot_dir = harness.settings.company_assets_dir / company_key / snapshot_id
            candidate_payload = json.loads((snapshot_dir / "candidate_documents.json").read_text(encoding="utf-8"))
            overlay_candidates = list(candidate_payload.get("candidates") or [])[3:99]
            overlay_path = harness.settings.jobs_dir / "test-xai-prefix-drift.asset_population.json"
            overlay_path.write_text(
                json.dumps(
                    {
                        "target_company": "xAI",
                        "snapshot_id": snapshot_id,
                        "asset_view": "canonical_merged",
                        "candidate_count": int(candidate_payload.get("candidate_count") or 320),
                        "candidates": overlay_candidates,
                        "evidence_lookup": {},
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            payload = harness.orchestrator._build_job_asset_population_payload(
                request=JobRequest.from_payload(
                    {
                        "raw_user_request": "给我 xAI 的所有成员",
                        "target_company": "xAI",
                        "target_scope": "full_company_asset",
                    }
                ),
                candidate_source={
                    "target_company": "xAI",
                    "source_kind": "company_snapshot",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "authoritative_snapshot_id": snapshot_id,
                    "source_path": str(snapshot_dir / "normalized_artifacts" / "artifact_summary.json"),
                    "asset_population_overlay_path": str(overlay_path),
                },
                effective_execution_semantics={
                    "asset_population_supported": True,
                    "default_results_mode": "asset_population",
                },
                offset=0,
                limit=96,
                include_candidates=True,
                load_profile_timeline=False,
            )
            first_ids = [str(item.get("candidate_id") or "") for item in list(payload.get("candidates") or [])[:3]]
            self.assertEqual(first_ids, ["xai-1", "xai-2", "xai-3"])

    def test_hosted_simulate_reuse_queries_preserve_follow_up_planning_contract(self) -> None:
        reuse_cases = [
            self._load_explain_case("reflection_posttrain_reuse"),
            self._load_explain_case("openai_reasoning_reuse"),
        ]
        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_explain_matrix_assets(harness)
            for case in reuse_cases:
                case_name = str(case.get("case") or "")
                payload = dict(case.get("payload") or {})
                expect = dict(case.get("expect") or {})
                with self.subTest(case=case_name):
                    record = run_hosted_smoke_case(
                        client=harness.client,
                        case_name=case_name,
                        payload=payload,
                        reviewer="hosted-simulate-follow-up-reuse",
                        poll_seconds=0.1,
                        max_poll_seconds=40.0,
                        runtime_tuning_profile="fast_smoke",
                    )

                    self.assertEqual(
                        (record.get("final") or {}).get("job_status"),
                        "completed",
                        json.dumps(record, ensure_ascii=False, indent=2),
                    )
                    self.assertLess(
                        float(dict(record.get("timings_ms") or {}).get("wait_for_completion") or 0.0),
                        10_000.0,
                        json.dumps(record, ensure_ascii=False, indent=2),
                    )
                    follow_up = run_hosted_explain_case(
                        harness.client,
                        case_name=f"{case_name}_follow_up",
                        payload=payload,
                    )
                    follow_up_summary = dict(follow_up.get("summary") or {})
                    self.assertEqual(
                        str(follow_up_summary.get("target_company") or ""),
                        str(expect.get("target_company") or ""),
                        json.dumps(follow_up, ensure_ascii=False, indent=2),
                    )
                    self.assertIn(
                        str(follow_up_summary.get("dispatch_strategy") or ""),
                        {"reuse_snapshot", "reuse_completed"},
                        json.dumps(follow_up, ensure_ascii=False, indent=2),
                    )
                    if str(follow_up_summary.get("dispatch_strategy") or "") == "reuse_snapshot":
                        self.assertEqual(
                            str(follow_up_summary.get("planner_mode") or ""),
                            str(expect.get("planner_mode") or ""),
                            json.dumps(follow_up, ensure_ascii=False, indent=2),
                        )
                        self.assertEqual(
                            bool(follow_up_summary.get("requires_delta_acquisition")),
                            bool(expect.get("requires_delta_acquisition")),
                            json.dumps(follow_up, ensure_ascii=False, indent=2),
                        )
                    else:
                        self.assertEqual(
                            str(follow_up_summary.get("dispatch_matched_job_status") or ""),
                            "completed",
                            json.dumps(follow_up, ensure_ascii=False, indent=2),
                        )
                        self.assertTrue(
                            str(follow_up_summary.get("dispatch_matched_job_id") or "").strip(),
                            json.dumps(follow_up, ensure_ascii=False, indent=2),
                        )

    def test_hosted_simulate_completed_history_round_trip_exposes_results_recovery(self) -> None:
        case = self._load_explain_case("openai_reasoning_reuse")
        payload = dict(case.get("payload") or {})
        history_id = "history-smoke-openai-reasoning"
        payload["history_id"] = history_id
        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_explain_matrix_assets(harness)
            record = run_hosted_smoke_case(
                client=harness.client,
                case_name="openai_reasoning_history_round_trip",
                payload=payload,
                reviewer="hosted-simulate-history-round-trip",
                poll_seconds=0.1,
                max_poll_seconds=40.0,
                runtime_tuning_profile="fast_smoke",
            )

            self.assertEqual(
                (record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            job_id = str((record.get("start") or {}).get("job_id") or "")
            self.assertTrue(job_id)
            link = harness.store.get_frontend_history_link(history_id)
            self.assertIsNotNone(link)
            assert link is not None
            self.assertEqual(str(link.get("phase") or ""), "results")
            self.assertEqual(str(link.get("job_id") or ""), job_id)

            recovered = harness.client.get(f"/api/frontend-history/{history_id}")
            self.assertEqual(recovered.get("status"), "found", json.dumps(recovered, ensure_ascii=False, indent=2))
            recovery = dict(recovered.get("recovery") or {})
            self.assertEqual(str(recovery.get("history_id") or ""), history_id)
            self.assertEqual(str(recovery.get("job_id") or ""), job_id)
            self.assertEqual(str(recovery.get("phase") or ""), "results")
            self.assertEqual(
                str(recovery.get("query_text") or ""),
                str(payload.get("raw_user_request") or ""),
            )
            self.assertEqual(str(recovery.get("target_company") or ""), "OpenAI")
            self.assertEqual(
                str(dict(recovery.get("request") or {}).get("target_company") or ""),
                "OpenAI",
            )

            results_payload = harness.client.get(f"/api/jobs/{job_id}/results?include_candidates=1")
            self.assertEqual(str(dict(results_payload.get("job") or {}).get("status") or ""), "completed")
            self.assertTrue(
                bool(dict(results_payload.get("asset_population") or {}).get("available"))
                or bool(results_payload.get("results") or [])
                or bool(results_payload.get("manual_review_items") or []),
                json.dumps(results_payload, ensure_ascii=False, indent=2),
            )

    def test_hosted_simulate_full_smoke_matrix_completes_when_enabled(self) -> None:
        enabled = str(os.getenv("SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if not enabled:
            self.skipTest("set SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1 to run the full 5-case hosted smoke matrix")

        with self._hosted_harness(provider_mode="simulate") as harness:
            self._seed_reference_org_assets(harness)
            cases = load_smoke_cases()
            summaries, failures = run_hosted_smoke_matrix(
                client=harness.client,
                cases=cases,
                reviewer="hosted-simulate-full-smoke",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
                runtime_tuning_profile="fast_smoke",
            )

            self.assertFalse(failures, json.dumps(summaries, ensure_ascii=False, indent=2))
            self.assertEqual(
                {str(item.get("case") or "") for item in summaries},
                {
                    "skild_pretrain",
                    "humansand_coding",
                    "anthropic_pretraining",
                    "xai_full_roster",
                    "xai_coding_all_members_scoped",
                    "openai_reasoning",
                    "google_multimodal_pretrain",
                },
            )
            for summary in summaries:
                self.assertEqual((summary.get("final") or {}).get("job_status"), "completed")

    def test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover(self) -> None:
        scenario_path = str(
            Path(__file__).resolve().parents[1] / "configs" / "scripted" / "google_multimodal_long_tail.json"
        )
        with self._hosted_harness(provider_mode="scripted", scripted_scenario=scenario_path) as harness:
            record = run_hosted_smoke_case(
                client=harness.client,
                case_name="google_multimodal_long_tail",
                payload={
                    "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人（包括Veo和Nano Banana相关）",
                    "top_k": 10,
                    "force_fresh_run": True,
                },
                reviewer="hosted-scripted-smoke",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
            )

            self.assertEqual(
                (record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            timeline = list(record.get("timeline") or [])
            self.assertIn("acquiring", {str(item.get("stage") or "") for item in timeline})
            acquiring_ticks = [
                int(item.get("tick") or 0) for item in timeline if str(item.get("stage") or "") == "acquiring"
            ]
            self.assertGreaterEqual(
                len(acquiring_ticks),
                2,
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertGreater(
                acquiring_ticks[-1],
                acquiring_ticks[0],
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertGreaterEqual(
                int((record.get("final") or {}).get("results_count") or 0)
                + int((record.get("final") or {}).get("manual_review_count") or 0),
                1,
            )

            job_id = str((record.get("start") or {}).get("job_id") or "")
            job_payload = harness.client.get(f"/api/jobs/{job_id}?include_details=1")
            runtime_control_events = [
                event
                for event in list(job_payload.get("events") or [])
                if str(event.get("stage") or "") == "runtime_control"
            ]
            self.assertFalse(
                any(
                    "execute-workflow" in str(event.get("detail") or "").lower()
                    or "supervise-workflow" in str(event.get("detail") or "").lower()
                    for event in runtime_control_events
                ),
                json.dumps(runtime_control_events, ensure_ascii=False, indent=2),
            )

            runtime_health = harness.client.get("/api/runtime/health?force_refresh=1")
            metrics = dict(runtime_health.get("metrics") or {})
            self.assertEqual(int(metrics.get("stale_acquiring_job_count") or 0), 0)
            self.assertEqual(int(metrics.get("stale_queue_job_count") or 0), 0)

    def test_hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider(self) -> None:
        scenario_path = str(
            Path(__file__).resolve().parents[1] / "configs" / "scripted" / "large_org_full_roster_overflow.json"
        )
        with self._hosted_harness(provider_mode="scripted", scripted_scenario=scenario_path) as harness:
            record = run_hosted_smoke_case(
                client=harness.client,
                case_name="large_org_full_roster_scripted_long_tail",
                payload={
                    "raw_user_request": "给我 xAI 的所有成员",
                    "top_k": 10,
                    "force_fresh_run": True,
                },
                reviewer="hosted-scripted-large-org-full-roster",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
            )

            self.assertEqual(
                (record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertEqual((record.get("explain") or {}).get("plan_primary_strategy_type"), "full_company_roster")
            self.assertEqual(
                (record.get("explain") or {}).get("plan_company_employee_shard_strategy"),
                "adaptive_us_technical_partition",
            )
            self.assertTrue(
                (record.get("explain") or {}).get("plan_company_employee_shard_policy_allow_overflow_partial")
            )
            behavior_guardrails = dict(dict(record.get("provider_case_report") or {}).get("behavior_guardrails") or {})
            duplicate_provider_dispatch = dict(behavior_guardrails.get("duplicate_provider_dispatch") or {})
            disabled_stage_violations = dict(behavior_guardrails.get("disabled_stage_violations") or {})
            prerequisite_gaps = dict(behavior_guardrails.get("prerequisite_gaps") or {})
            final_results_board_consistency = dict(behavior_guardrails.get("final_results_board_consistency") or {})
            self.assertGreaterEqual(int(duplicate_provider_dispatch.get("invocation_count") or 0), 1)
            self.assertFalse(duplicate_provider_dispatch.get("violation_detected"), json.dumps(record, ensure_ascii=False, indent=2))
            self.assertFalse(disabled_stage_violations.get("violation_detected"), json.dumps(record, ensure_ascii=False, indent=2))
            self.assertFalse(prerequisite_gaps.get("violation_detected"), json.dumps(record, ensure_ascii=False, indent=2))
            self.assertFalse(
                final_results_board_consistency.get("violation_detected"),
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            timeline = list(record.get("timeline") or [])
            self.assertIn("acquiring", {str(item.get("stage") or "") for item in timeline})
            self.assertTrue(list(record.get("worker_recovery") or []), json.dumps(record, ensure_ascii=False, indent=2))
            board = dict(dict(record.get("provider_case_report") or {}).get("board") or {})
            self.assertTrue(board.get("ready_nonempty"), json.dumps(record, ensure_ascii=False, indent=2))
            report_counts = dict(dict(record.get("provider_case_report") or {}).get("counts") or {})
            self.assertGreaterEqual(
                max(
                    int(report_counts.get("final_candidate_count") or 0),
                    int(report_counts.get("board_total_candidates") or 0),
                    int(board.get("candidate_page_total_candidates") or 0),
                ),
                1,
                json.dumps(record, ensure_ascii=False, indent=2),
            )

    def test_hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch(self) -> None:
        scenario_path = str(
            Path(__file__).resolve().parents[1] / "configs" / "scripted" / "large_org_profile_tail_reconcile.json"
        )
        with self._hosted_harness(provider_mode="scripted", scripted_scenario=scenario_path) as harness:
            record = run_hosted_smoke_case(
                client=harness.client,
                case_name="large_org_full_roster_profile_tail",
                payload={
                    "raw_user_request": "给我 xAI 的所有成员",
                    "top_k": 10,
                    "force_fresh_run": True,
                },
                reviewer="hosted-scripted-large-org-profile-tail",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
            )

            self.assertEqual(
                (record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertEqual((record.get("explain") or {}).get("plan_primary_strategy_type"), "full_company_roster")
            self.assertTrue(
                (record.get("explain") or {}).get("plan_company_employee_shard_policy_allow_overflow_partial"),
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertTrue(list(record.get("worker_recovery") or []), json.dumps(record, ensure_ascii=False, indent=2))

            background_reconcile = dict(((record.get("final") or {}).get("background_reconcile")) or {})
            harvest_prefetch = dict(background_reconcile.get("harvest_prefetch") or {})
            self.assertEqual(
                harvest_prefetch.get("status"), "completed", json.dumps(record, ensure_ascii=False, indent=2)
            )
            self.assertGreaterEqual(int(harvest_prefetch.get("applied_worker_count") or 0), 1)

            resume_result = dict(harvest_prefetch.get("resume_result") or {})
            profile_completion_result = dict(resume_result.get("profile_completion_result") or {})
            self.assertEqual(
                profile_completion_result.get("status"), "completed", json.dumps(record, ensure_ascii=False, indent=2)
            )
            completion_metrics = dict(profile_completion_result.get("result") or {})
            artifact_summary = dict(dict(profile_completion_result.get("artifact_result") or {}).get("summary") or {})
            self.assertGreaterEqual(
                max(
                    int(completion_metrics.get("fetched_profile_count") or 0),
                    int(artifact_summary.get("profile_detail_count") or 0),
                ),
                1,
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertGreaterEqual(
                int(artifact_summary.get("structured_experience_count") or 0),
                1,
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertGreaterEqual(
                int(artifact_summary.get("structured_education_count") or 0),
                1,
                json.dumps(record, ensure_ascii=False, indent=2),
            )

            job_id = str((record.get("start") or {}).get("job_id") or "")
            job_payload = harness.client.get(f"/api/jobs/{job_id}?include_details=1")
            event_messages = [
                str(event.get("message") or event.get("detail") or "")
                for event in list(job_payload.get("events") or [])
            ]
            self.assertTrue(
                any(
                    "Background harvest profile prefetch reconcile started after worker recovery." in message
                    for message in event_messages
                ),
                json.dumps(event_messages, ensure_ascii=False, indent=2),
            )
            self.assertTrue(
                any(
                    "Background harvest profile prefetch merged local profile detail into candidate artifacts."
                    in message
                    for message in event_messages
                ),
                json.dumps(event_messages, ensure_ascii=False, indent=2),
            )

            results_payload = harness.client.get(f"/api/jobs/{job_id}/results?include_candidates=1")
            asset_population = dict(results_payload.get("asset_population") or {})
            candidate_by_name = {
                str(item.get("display_name") or item.get("name_en") or ""): item
                for item in list(asset_population.get("candidates") or [])
                if isinstance(item, dict)
            }
            scripted_research = dict(candidate_by_name.get("Scripted xAI Research") or {})
            self.assertTrue(scripted_research, json.dumps(results_payload, ensure_ascii=False, indent=2))
            self.assertIn(
                "Research Scientist", json.dumps(scripted_research.get("experience_lines") or [], ensure_ascii=False)
            )
            self.assertIn(
                "Stanford University", json.dumps(scripted_research.get("education_lines") or [], ensure_ascii=False)
            )

    def test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch(self) -> None:
        scenario_path = str(
            Path(__file__).resolve().parents[1] / "configs" / "scripted" / "large_org_profile_tail_reconcile.json"
        )
        with self._hosted_harness(provider_mode="scripted", scripted_scenario=scenario_path) as harness:
            first_record = run_hosted_smoke_case(
                client=harness.client,
                case_name="large_org_profile_tail_first_pass",
                payload={
                    "raw_user_request": "给我 xAI 的所有成员",
                    "top_k": 10,
                    "force_fresh_run": True,
                },
                reviewer="hosted-scripted-large-org-profile-tail-first",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
            )
            self.assertEqual(
                (first_record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(first_record, ensure_ascii=False, indent=2),
            )
            first_reconcile = dict(((first_record.get("final") or {}).get("background_reconcile")) or {})
            self.assertEqual(
                dict(first_reconcile.get("harvest_prefetch") or {}).get("status"),
                "completed",
                json.dumps(first_record, ensure_ascii=False, indent=2),
            )

            registry_row = harness.store.get_authoritative_organization_asset_registry(target_company="xAI")
            self.assertTrue(
                registry_row, "authoritative xAI snapshot should exist after the first scripted reconcile run"
            )
            self.assertEqual(int(dict(registry_row or {}).get("profile_completion_backlog_count") or 0), 0)

            second_record = run_hosted_smoke_case(
                client=harness.client,
                case_name="large_org_profile_tail_second_pass",
                payload={
                    "raw_user_request": "给我 xAI 的所有成员",
                    "top_k": 10,
                },
                reviewer="hosted-scripted-large-org-profile-tail-second",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
            )
            self.assertEqual(
                (second_record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(second_record, ensure_ascii=False, indent=2),
            )
            self.assertTrue((second_record.get("final") or {}).get("asset_population_available"))
            self.assertGreaterEqual(
                int((second_record.get("final") or {}).get("asset_population_candidate_count") or 0), 1
            )
            self.assertEqual((second_record.get("explain") or {}).get("current_lane"), "reuse_baseline")
            self.assertEqual(
                str((second_record.get("explain") or {}).get("dispatch_strategy") or ""), "reuse_completed"
            )
            self.assertEqual(
                str((second_record.get("explain") or {}).get("dispatch_matched_job_status") or ""), "completed"
            )
            self.assertEqual(
                str((second_record.get("start") or {}).get("job_id") or ""),
                str((first_record.get("start") or {}).get("job_id") or ""),
            )
            self.assertEqual(
                str((second_record.get("explain") or {}).get("dispatch_matched_job_id") or ""),
                str((first_record.get("start") or {}).get("job_id") or ""),
            )
            self.assertFalse(
                list(second_record.get("worker_recovery") or []),
                json.dumps(second_record, ensure_ascii=False, indent=2),
            )
            self.assertLessEqual(
                max(int(item.get("tick") or 0) for item in list(second_record.get("timeline") or []) or [{"tick": 0}]),
                1,
                json.dumps(second_record, ensure_ascii=False, indent=2),
            )
            refreshed_registry_row = harness.store.get_authoritative_organization_asset_registry(target_company="xAI")
            self.assertEqual(int(dict(refreshed_registry_row or {}).get("profile_completion_backlog_count") or 0), 0)

    def test_hosted_scripted_long_tail_accepts_request_scoped_fast_smoke_profile_without_server_env(self) -> None:
        scenario_path = str(
            Path(__file__).resolve().parents[1] / "configs" / "scripted" / "google_multimodal_long_tail.json"
        )
        with self._hosted_harness(
            provider_mode="scripted",
            scripted_scenario=scenario_path,
            inject_fast_runtime_env=False,
        ) as harness:
            record = run_hosted_smoke_case(
                client=harness.client,
                case_name="google_multimodal_long_tail_request_scoped_fast_smoke",
                payload={
                    "raw_user_request": "帮我找Google里做多模态和Pre-train方向的人（包括Veo和Nano Banana相关）",
                    "top_k": 10,
                    "force_fresh_run": True,
                },
                reviewer="hosted-scripted-request-scoped-fast-smoke",
                poll_seconds=0.1,
                max_poll_seconds=60.0,
                runtime_tuning_profile="fast_smoke",
            )

            self.assertEqual(
                (record.get("final") or {}).get("job_status"),
                "completed",
                json.dumps(record, ensure_ascii=False, indent=2),
            )
            self.assertEqual((record.get("explain") or {}).get("runtime_tuning_profile"), "fast_smoke")
            self.assertLessEqual(
                max(int(item.get("tick") or 0) for item in list(record.get("timeline") or []) or [{"tick": 0}]),
                650,
                json.dumps(record, ensure_ascii=False, indent=2),
            )

            job_id = str((record.get("start") or {}).get("job_id") or "")
            job_payload = harness.client.get(f"/api/jobs/{job_id}?include_details=1")
            execution_preferences = dict(dict(job_payload.get("request") or {}).get("execution_preferences") or {})
            self.assertEqual(execution_preferences.get("runtime_tuning_profile"), "fast_smoke")


if __name__ == "__main__":
    unittest.main()
