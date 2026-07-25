"""queue_workflow dispatch-decision contracts — salvage wave group 4.

WS3 Tier 3 salvage (2026-07-22, master plan docs/REFACTOR_MASTER_PLAN.md; R-009
salvage-then-delete; work-list docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md
group 4): join-inflight (exact + family-signature + idempotency-first), tenant
fencing on completed-job reuse, force-fresh preservation/suppression policy,
registry-lane-coverage preference over completed query-specific snapshots,
projection-based reuse, and former-lane-missing delta requirements had only
smoke-level strategy labels as modern coverage. Ported verbatim from the frozen
test_pipeline.py onto the repo-standard PG fixture; the two snapshot/registry
helpers ride along per the salvage-file convention. Old->new mapping in
docs/governance/REGRESSION_INDEX.md; the freeze ratchet shrinks in the same
change.
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
from sourcing_agent.domain import Candidate, JobRequest
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


class QueueWorkflowDispatchTest(PGControlPlaneStoreTestMixin, unittest.TestCase):
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

    def _upsert_authoritative_org_registry(
        self,
        *,
        target_company: str,
        snapshot_id: str,
        candidate_count: int,
        source_path: str,
        current_ready: bool,
        former_ready: bool,
        current_count: int,
        former_count: int,
        source_job_id: str = "",
        materialization_generation_key: str = "",
        materialization_generation_sequence: int = 0,
        materialization_watermark: str = "",
    ) -> dict[str, object]:
        return self.store.upsert_organization_asset_registry(
            {
                "target_company": target_company,
                "company_key": normalize_company_key(target_company),
                "snapshot_id": snapshot_id,
                "asset_view": "canonical_merged",
                "status": "ready",
                "candidate_count": candidate_count,
                "evidence_count": 0,
                "profile_detail_count": candidate_count,
                "explicit_profile_capture_count": candidate_count,
                "missing_linkedin_count": 0,
                "manual_review_backlog_count": 0,
                "profile_completion_backlog_count": 0,
                "source_snapshot_count": 1,
                "standard_bundles": {"bundle_count": 1},
                "completeness_score": 100.0,
                "completeness_band": "high",
                "current_lane_coverage": {
                    "effective_candidate_count": current_count,
                    "effective_ready": current_ready,
                    "company_employees_current": {
                        "effective_candidate_count": current_count,
                        "effective_ready": current_ready,
                        "inferred_candidate_count": current_count,
                        "inferred_ready": current_ready,
                    },
                },
                "former_lane_coverage": {
                    "effective_candidate_count": former_count,
                    "effective_ready": former_ready,
                    "standard_bundle_ready_count": 1 if former_ready and former_count > 0 else 0,
                    "inferred_candidate_count": former_count,
                    "inferred_profile_detail_count": former_count,
                    "inferred_linkedin_url_count": former_count,
                    "profile_search_former": {
                        "effective_candidate_count": former_count,
                        "effective_ready": former_ready,
                        "standard_bundle_ready_count": 1 if former_ready and former_count > 0 else 0,
                        "inferred_candidate_count": former_count,
                        "inferred_profile_detail_count": former_count,
                        "inferred_linkedin_url_count": former_count,
                    },
                },
                "current_lane_effective_candidate_count": current_count,
                "former_lane_effective_candidate_count": former_count,
                "current_lane_effective_ready": current_ready,
                "former_lane_effective_ready": former_ready,
                "selected_snapshot_ids": [snapshot_id],
                "source_snapshot_selection": {"selected_snapshot_ids": [snapshot_id]},
                "source_path": source_path,
                "source_job_id": source_job_id,
                "materialization_generation_key": materialization_generation_key,
                "materialization_generation_sequence": materialization_generation_sequence,
                "materialization_watermark": materialization_watermark,
                "summary": {
                    "target_company": target_company,
                    "snapshot_id": snapshot_id,
                    "candidate_count": candidate_count,
                    "profile_detail_count": candidate_count,
                    "standard_bundles": {"bundle_count": 1},
                    "current_lane_coverage": {
                        "effective_candidate_count": current_count,
                        "effective_ready": current_ready,
                    },
                    "former_lane_coverage": {
                        "effective_candidate_count": former_count,
                        "effective_ready": former_ready,
                    },
                },
            },
            authoritative=True,
        )

    def test_queue_workflow_joins_inflight_exact_request(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "skip_plan_review": True,
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")
        second = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(second["status"], "joined_existing_job")
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(second["dispatch"]["strategy"], "join_inflight")
        dispatches = self.store.list_query_dispatches(target_company="Anthropic", limit=10)
        self.assertTrue(any(str(item.get("strategy") or "") == "join_inflight" for item in dispatches))

    def test_queue_workflow_joins_inflight_request_family_signature(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "profile_detail_limit": 200,
            "skip_plan_review": True,
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")

        second = self.orchestrator.queue_workflow(
            {
                **payload,
                "raw_user_request": "帮我找 Anthropic 做基础设施的技术成员，top10 就行。",
                "top_k": 10,
                "profile_detail_limit": 50,
            }
        )
        self.assertEqual(second["status"], "joined_existing_job")
        self.assertEqual(second["job_id"], first["job_id"])
        self.assertEqual(second["dispatch"]["strategy"], "join_inflight")
        self.assertEqual(second["dispatch"]["request_family_signature"], first["dispatch"]["request_family_signature"])
        self.assertNotEqual(second["dispatch"]["request_signature"], first["dispatch"]["request_signature"])

    def test_queue_workflow_reuses_completed_job_with_tenant_scope(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "skip_plan_review": True,
            "tenant_id": "tenant-a",
            "requester_id": "user-1",
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")
        run_result = self.orchestrator.run_queued_workflow(str(first.get("job_id") or ""))
        self.assertEqual(run_result["status"], "completed")

        same_tenant_other_user = self.orchestrator.queue_workflow({**payload, "requester_id": "user-2"})
        self.assertEqual(same_tenant_other_user["status"], "reused_completed_job")
        self.assertEqual(same_tenant_other_user["job_id"], first["job_id"])
        self.assertEqual(same_tenant_other_user["dispatch"]["strategy"], "reuse_completed")

        different_tenant = self.orchestrator.queue_workflow(
            {**payload, "tenant_id": "tenant-b", "requester_id": "user-3"}
        )
        self.assertEqual(different_tenant["status"], "queued")
        self.assertNotEqual(different_tenant["job_id"], first["job_id"])

    def test_queue_workflow_plan_review_force_fresh_does_not_reuse_completed_job(self) -> None:
        snapshot_id = "20260420T010101"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_force_fresh_registry",
                    name_en="Force Fresh Registry Candidate",
                    display_name="Force Fresh Registry Candidate",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="infrastructure",
                    linkedin_url="https://www.linkedin.com/in/force-fresh-registry/",
                ).to_record()
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
        )
        base_payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施"],
            "top_k": 3,
        }
        completed_request = JobRequest.from_payload(base_payload)
        completed_plan = self.orchestrator.plan_workflow({**base_payload, "skip_plan_review": True})["plan"]
        self.store.save_job(
            job_id="completed_force_fresh_match",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=completed_request.to_record(),
            plan_payload=completed_plan,
            summary_payload={"message": "Workflow completed."},
        )

        fresh_request = JobRequest.from_payload({**base_payload, "force_fresh_run": True})
        fresh_plan = self.orchestrator.plan_workflow({**base_payload, "force_fresh_run": True, "skip_plan_review": True})[
            "plan"
        ]
        execution_bundle = self.orchestrator._build_execution_bundle(  # noqa: SLF001
            request=fresh_request,
            plan=fresh_plan,
            effective_request=fresh_request,
            source="test_force_fresh_plan_review",
        )
        review_session = self.store.create_plan_review_session(
            target_company="Anthropic",
            request_payload=fresh_request.to_record(),
            plan_payload=fresh_plan,
            gate_payload={"required_before_execution": False, "risk_level": "low"},
            execution_bundle_payload=execution_bundle,
        )

        queued = self.orchestrator.queue_workflow({"plan_review_id": int(review_session["review_id"])})

        self.assertEqual(queued["status"], "queued")
        self.assertNotEqual(queued["job_id"], "completed_force_fresh_match")
        self.assertEqual(queued["dispatch"]["strategy"], "new_job")
        self.assertNotIn("force_fresh_run_suppressed", queued["dispatch"])

    def test_queue_workflow_reuses_collection_authoritative_projection_for_unscoped_full_asset_request(self) -> None:
        snapshot_id = "20260520T010101"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="xAI",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_xai_research",
                    name_en="xAI Researcher",
                    display_name="xAI Researcher",
                    category="employee",
                    target_company="xAI",
                    organization="xAI",
                    employment_status="current",
                    role="Research Scientist",
                    linkedin_url="https://www.linkedin.com/in/xai-researcher/",
                ).to_record()
            ],
        )
        source_job_id = "completed_xai_force_fresh_full_asset"
        source_payload = {
            "raw_user_request": "给我 xAI 的所有成员",
            "target_company": "xAI",
            "categories": ["employee", "former_employee"],
            "employment_statuses": ["current", "former"],
            "top_k": 10,
            "force_fresh_run": True,
            "skip_plan_review": True,
        }
        source_request = JobRequest.from_payload(source_payload)
        source_plan = self.orchestrator.plan_workflow(source_payload)["plan"]
        self.store.save_job(
            job_id=source_job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=source_request.to_record(),
            plan_payload=source_plan,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {
                    "source_kind": "company_snapshot",
                    "target_company": "xAI",
                    "snapshot_id": snapshot_id,
                    "asset_view": "canonical_merged",
                    "source_path": str(candidate_doc_path),
                    "candidate_count": 1,
                },
            },
        )
        member = {
            "candidate_identity_key": "linkedin:xai-researcher",
            "person_identity_key": "linkedin:xai-researcher",
            "profile_url_key": "linkedin:xai-researcher",
            "candidate_id": "cand_xai_research",
            "rank_index": 1,
            "public_summary": {"display_name": "xAI Researcher"},
            "row_readiness": "complete",
            "profile_readiness": "complete",
            "card_readiness": "complete",
        }
        run_projection = self.orchestrator.serving_projection_writer.publish_run_scope_projection(
            run_id=source_job_id,
            collection_id="company:xai",
            projection_id="proj_xai_full_asset_run",
            members=[member],
            replace_members=True,
            scope_label="给我 xAI 的所有成员",
            scope_spec={
                "target_company": "xAI",
                "target_scope": "full_company_asset",
                "asset_view": "canonical_merged",
                "keywords": [],
                "snapshot_id": snapshot_id,
            },
            counts={"result_count": 1, "candidate_count": 1, "visible_member_count": 1},
            readiness={"row": "complete", "profile": "complete", "card": "complete"},
            provenance={"source_run_id": source_job_id, "snapshot_id": snapshot_id},
            metadata={"source_path": str(candidate_doc_path)},
        )

        run_projection_reuse = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "给我 xAI 的所有成员",
                "target_company": "xAI",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "top_k": 10,
                "skip_plan_review": True,
            }
        )
        self.assertEqual(run_projection_reuse["status"], "reused_completed_job")
        self.assertEqual(run_projection_reuse["job_id"], source_job_id)
        self.assertEqual(run_projection_reuse["dispatch"]["reuse_basis"], "run_scope_projection")

        self.orchestrator.serving_projection_writer.publish_collection_authoritative_projection(
            collection_id="company:xai",
            active_collection_version="collv_xai_full_asset_1",
            projection_id="proj_xai_collection_authoritative",
            members=[member],
            replace_members=True,
            counts={"result_count": 1, "candidate_count": 1, "visible_member_count": 1},
            readiness={"row": "complete", "profile": "complete", "card": "complete"},
            provenance={
                "source_projection_id": run_projection["projection"]["projection_id"],
                "source_run_id": source_job_id,
            },
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "给我 xAI 的所有成员",
                "target_company": "xAI",
                "categories": ["employee", "former_employee"],
                "employment_statuses": ["current", "former"],
                "top_k": 10,
                "skip_plan_review": True,
            }
        )

        self.assertEqual(queued["status"], "reused_completed_job")
        self.assertEqual(queued["job_id"], source_job_id)
        self.assertEqual(queued["dispatch"]["strategy"], "reuse_completed")
        self.assertEqual(queued["dispatch"]["reuse_basis"], "collection_authoritative_projection")
        self.assertEqual(queued["dispatch"]["matched_projection_id"], "proj_xai_collection_authoritative")
        self.assertEqual(queued["dispatch"]["matched_collection_id"], "company:xai")

        scoped_request = JobRequest.from_payload(
            {
                "raw_user_request": "给我 xAI 做 agents 的成员",
                "target_company": "xAI",
                "keywords": ["agents"],
            }
        )
        scoped_context = self.orchestrator._build_query_dispatch_context(  # noqa: SLF001
            scoped_request.to_record(),
            scoped_request,
        )
        self.assertEqual(
            self.orchestrator._resolve_collection_authoritative_projection_reuse_match(  # noqa: SLF001
                scoped_request,
                scoped_context,
            ),
            {},
        )

    def test_queue_workflow_prefers_authoritative_registry_snapshot_over_completed_query_specific_snapshot(
        self,
    ) -> None:
        authoritative_snapshot_id = "20260413T010101"
        _, authoritative_candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=authoritative_snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_authoritative",
                    name_en="Authoritative Researcher",
                    display_name="Authoritative Researcher",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="pre-train systems",
                    linkedin_url="https://www.linkedin.com/in/authoritative-researcher/",
                ).to_record(),
            ],
        )
        registry_row = self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=authoritative_snapshot_id,
            candidate_count=1,
            source_path=str(authoritative_candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=0,
        )

        live_snapshot_id = "20260416T225318"
        _, live_candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=live_snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_live",
                    name_en="Live Snapshot Candidate",
                    display_name="Live Snapshot Candidate",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Research Engineer",
                    focus_areas="pre-training",
                    linkedin_url="https://www.linkedin.com/in/live-snapshot-candidate/",
                ).to_record(),
            ],
        )

        payload = {
            "raw_user_request": "帮我找 Anthropic 做 Pre-train 的人。",
            "target_company": "Anthropic",
            "target_scope": "full_company_asset",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["Pre-train"],
            "top_k": 8,
            "skip_plan_review": True,
        }
        completed_request = JobRequest.from_payload(payload)
        completed_plan = self.orchestrator.plan_workflow(payload)["plan"]
        self.store.save_job(
            job_id="completed_live_snapshot_job",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=completed_request.to_record(),
            plan_payload=completed_plan,
            summary_payload={
                "message": "Workflow completed.",
                "candidate_source": {
                    "snapshot_id": live_snapshot_id,
                    "source_kind": "company_snapshot",
                    "source_path": str(live_candidate_doc_path),
                },
            },
        )

        queued = self.orchestrator.queue_workflow(dict(payload))

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "reuse_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], authoritative_snapshot_id)
        self.assertEqual(int(queued["dispatch"]["matched_registry_id"] or 0), int(registry_row["registry_id"] or 0))
        self.assertEqual(queued["dispatch"]["reuse_basis"], "organization_asset_registry_lane_coverage")
        self.assertNotEqual(str(queued.get("job_id") or ""), "completed_live_snapshot_job")
        explanation = dict(queued["dispatch"].get("request_family_match_explanation") or {})
        self.assertTrue(bool(explanation.get("completed_match_reuse_suppressed")))
        self.assertEqual(
            explanation.get("completed_match_reuse_suppressed_reason"),
            "authoritative_registry_snapshot_preferred",
        )
        self.assertEqual(explanation.get("completed_match_snapshot_id"), live_snapshot_id)

    def test_queue_workflow_uses_idempotency_key_first(self) -> None:
        payload = {
            "raw_user_request": "帮我找 Anthropic 当前偏基础设施方向的技术成员，先获取全量资产再检索。",
            "target_company": "Anthropic",
            "categories": ["employee"],
            "employment_statuses": ["current"],
            "keywords": ["基础设施", "GPU", "预训练"],
            "top_k": 3,
            "skip_plan_review": True,
            "tenant_id": "tenant-a",
            "requester_id": "user-1",
            "idempotency_key": "req-42",
        }
        first = self.orchestrator.queue_workflow(dict(payload))
        self.assertEqual(first["status"], "queued")

        inflight_repeat = self.orchestrator.queue_workflow(
            {
                **payload,
                "keywords": ["this payload is intentionally different"],
                "query": "same idempotency should still dedupe",
            }
        )
        self.assertEqual(inflight_repeat["status"], "joined_existing_job")
        self.assertEqual(inflight_repeat["job_id"], first["job_id"])
        self.assertEqual(inflight_repeat["dispatch"]["strategy"], "join_inflight")

        run_result = self.orchestrator.run_queued_workflow(str(first.get("job_id") or ""))
        self.assertEqual(run_result["status"], "completed")

        completed_repeat = self.orchestrator.queue_workflow(
            {
                **payload,
                "keywords": ["changed again after completion"],
                "query": "completed idempotent reuse",
            }
        )
        self.assertEqual(completed_repeat["status"], "reused_completed_job")
        self.assertEqual(completed_repeat["job_id"], first["job_id"])
        self.assertEqual(completed_repeat["dispatch"]["strategy"], "reuse_completed")

    def test_queue_workflow_keeps_plan_review_force_fresh_when_effective_baseline_ready(self) -> None:
        snapshot_id = "20260413T020204"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_current_review_fresh",
                    name_en="Fresh Review Engineer",
                    display_name="Fresh Review Engineer",
                    category="employee",
                    target_company="Google",
                    organization="Google DeepMind",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="multimodal Veo fresh run",
                    linkedin_url="https://www.linkedin.com/in/google-fresh-review-engineer/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_google_former_review_fresh",
                    name_en="Former Fresh Review Researcher",
                    display_name="Former Fresh Review Researcher",
                    category="former_employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="former",
                    role="Research Scientist",
                    focus_areas="video generation fresh run",
                    linkedin_url="https://www.linkedin.com/in/google-former-fresh-review-researcher/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=2,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=1,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要 Google 公司全量成员。",
                "target_company": "Google",
            }
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)
        self.orchestrator.review_plan_session(
            {
                "review_id": review_id,
                "action": "approved",
                "reviewer": "tester",
                "decision": {
                    "force_fresh_run": True,
                },
            }
        )

        queued = self.orchestrator.queue_workflow(
            {
                "plan_review_id": review_id,
                "runtime_execution_mode": "hosted",
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "new_job")
        self.assertNotIn("force_fresh_run_suppressed", queued["dispatch"])
        dispatch_asset_reuse = dict(queued["dispatch"].get("asset_reuse_plan") or {})
        self.assertFalse(dispatch_asset_reuse.get("baseline_reuse_available"))
        self.assertEqual(dispatch_asset_reuse.get("reason"), "force_fresh_run")
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertTrue(execution_preferences.get("force_fresh_run"))
        self.assertNotIn("reuse_snapshot_id", execution_preferences)
        self.assertNotIn("delta_baseline_snapshot_id", execution_preferences)

    def test_queue_workflow_keeps_explicit_force_fresh_when_requested_at_queue_time(self) -> None:
        snapshot_id = "20260413T020303"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_google_current_2",
                    name_en="Multimodal Lead",
                    display_name="Multimodal Lead",
                    category="employee",
                    target_company="Google",
                    organization="Google",
                    employment_status="current",
                    role="Research Scientist",
                    focus_areas="multimodal Veo Nano Banana",
                    linkedin_url="https://www.linkedin.com/in/google-multimodal-lead/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Google",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
        )

        plan_result = self.orchestrator.plan_workflow(
            {
                "raw_user_request": "我想要 Google 公司全量成员，重新跑，不要高成本。",
                "target_company": "Google",
            }
        )
        self.store.save_job(
            job_id=source_job_id,
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload=dict(plan_result.get("request") or {}),
            plan_payload={},
            summary_payload={"message": "completed"},
        )
        review_id = int(plan_result["plan_review_session"]["review_id"] or 0)

        queued = self.orchestrator.queue_workflow(
            {
                "plan_review_id": review_id,
                "runtime_execution_mode": "hosted",
                "execution_preferences": {"force_fresh_run": True},
            }
        )

        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["dispatch"]["strategy"], "new_job")
        self.assertNotIn("force_fresh_run_suppressed", queued["dispatch"])
        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        self.assertTrue(execution_preferences.get("force_fresh_run"))


    def test_queue_workflow_reuses_authoritative_registry_snapshot_without_family_match(self) -> None:
        snapshot_id = "20260413T010101"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_registry_current",
                    name_en="Infra Generalist",
                    display_name="Infra Generalist",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="infrastructure systems",
                    linkedin_url="https://www.linkedin.com/in/infra-generalist/",
                ).to_record(),
                Candidate(
                    candidate_id="cand_registry_former",
                    name_en="Alignment Former",
                    display_name="Alignment Former",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="former",
                    role="Researcher",
                    focus_areas="alignment safety",
                    linkedin_url="https://www.linkedin.com/in/alignment-former/",
                ).to_record(),
            ],
        )
        registry_row = self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=2,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=True,
            current_count=1,
            former_count=1,
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 做安全和对齐方向的人。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["current", "former"],
                "keywords": ["安全", "对齐"],
                "top_k": 8,
                "skip_plan_review": True,
            }
        )

        self.assertEqual(queued["status"], "queued")
        # FLIPPED 2026-07-22 (4b forensics): under the unified query-shaped
        # strategy (WS1 Step 3) this scoped request dispatches delta on the
        # registry baseline; the test's core contract — the authoritative
        # registry is chosen WITHOUT a family match — is the reuse_basis pin.
        self.assertEqual(queued["dispatch"]["strategy"], "delta_from_snapshot")
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_id)
        self.assertEqual(int(queued["dispatch"]["matched_registry_id"] or 0), int(registry_row["registry_id"] or 0))
        self.assertEqual(queued["dispatch"]["reuse_basis"], "organization_asset_registry_lane_coverage")
        self.assertEqual(queued["dispatch"]["matched_job_id"], "")
        # Delta dispatch does not pin reuse-only; the registry preference is
        # carried by reuse_basis above.
        self.assertFalse(queued["dispatch"]["force_reuse_snapshot_only"])
        self.assertEqual(
            queued["dispatch"]["request_family_match_explanation"]["selection_mode"],
            "organization_asset_registry_lane_coverage",
        )

        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        # Delta carries the baseline binding, not the reuse-only pin.
        self.assertEqual(execution_preferences.get("delta_baseline_snapshot_id"), snapshot_id)
        # reuse_existing_roster was the reuse-only pin; delta does not set it.
        self.assertNotIn("reuse_existing_roster", execution_preferences)
        # The old tail pinned reuse-only run semantics (zero acquisition tasks
        # on run_queued_workflow); under the unified delta dispatch the run
        # legitimately executes delta acquisition — run-level delta behavior
        # is owned by the hosted smoke delta rows and the acquisition suites.

    def test_queue_workflow_registry_reuse_requires_delta_when_former_lane_missing(self) -> None:
        snapshot_id = "20260413T020202"
        _, candidate_doc_path = self._write_company_snapshot_candidate_documents(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidates=[
                Candidate(
                    candidate_id="cand_registry_only_current",
                    name_en="Current Only",
                    display_name="Current Only",
                    category="employee",
                    target_company="Anthropic",
                    organization="Anthropic",
                    employment_status="current",
                    role="Engineer",
                    focus_areas="infrastructure systems",
                    linkedin_url="https://www.linkedin.com/in/current-only/",
                ).to_record(),
            ],
        )
        self._upsert_authoritative_org_registry(
            target_company="Anthropic",
            snapshot_id=snapshot_id,
            candidate_count=1,
            source_path=str(candidate_doc_path),
            current_ready=True,
            former_ready=False,
            current_count=1,
            former_count=0,
        )

        queued = self.orchestrator.queue_workflow(
            {
                "raw_user_request": "帮我找 Anthropic 的前员工。",
                "target_company": "Anthropic",
                "categories": ["employee"],
                "employment_statuses": ["former"],
                "top_k": 6,
                "skip_plan_review": True,
            }
        )

        self.assertEqual(queued["status"], "queued")
        # FLIPPED 2026-07-22 (4b forensics): B1 unified the former lane onto
        # per-function shard delta — a missing former lane now dispatches
        # delta_from_snapshot on the registry baseline instead of a new job
        # with an acquisition-strategy override.
        self.assertEqual(queued["dispatch"]["strategy"], "delta_from_snapshot")
        # Delta binds the registry baseline snapshot instead of leaving the
        # match empty.
        self.assertEqual(queued["dispatch"]["matched_snapshot_id"], snapshot_id)
        self.assertGreater(int(queued["dispatch"]["matched_registry_id"] or 0), 0)

        queued_job = self.store.get_job(str(queued.get("job_id") or ""))
        assert queued_job is not None
        execution_preferences = dict(dict(queued_job.get("request") or {}).get("execution_preferences") or {})
        # The former override still rides the delta request (probe-verified);
        # delta additionally binds the baseline snapshot ids.
        self.assertEqual(execution_preferences.get("acquisition_strategy_override"), "former_employee_search")
        self.assertEqual(execution_preferences.get("delta_baseline_snapshot_id"), snapshot_id)

if __name__ == "__main__":
    unittest.main()
