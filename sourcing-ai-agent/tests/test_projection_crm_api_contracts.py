import json
import os
import tempfile
import threading
import unittest
import zipfile
from io import BytesIO
from pathlib import Path
from unittest import mock
from urllib import request as urllib_request
from urllib.parse import quote

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.company_asset_writer import CompanyAssetWriter
from sourcing_agent.crm_public_web_runtime import sync_crm_public_web_batch_summary
from sourcing_agent.durable_runtime import (
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_OWNER,
    MEDIA_ASSET_CACHE_COMMAND_TYPE,
    MEDIA_ASSET_OWNER,
)
from sourcing_agent.legacy_public_web_storage import (
    seed_legacy_target_public_web_promotion,
    seed_legacy_target_public_web_run,
)
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.orchestrator import SourcingOrchestrator
from sourcing_agent.person_asset_writer import PersonAssetWriter
from sourcing_agent.semantic_provider import LocalSemanticProvider
from sourcing_agent.serving_projection_writer import ServingProjectionWriter
from sourcing_agent.settings import (
    AppSettings,
    HarvestActorSettings,
    HarvestSettings,
    QwenSettings,
    SemanticProviderSettings,
)
from sourcing_agent.storage import ControlPlaneStore
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class ProjectionCrmApiContractsTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self._start_pg_durable_runtime(runtime_dir=Path(self.tempdir.name))
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
        self.catalog = AssetCatalog.discover()
        self.model_client = DeterministicModelClient()
        self.acquisition_engine = AcquisitionEngine(self.catalog, self.settings, self.store, self.model_client)
        self.orchestrator = SourcingOrchestrator(
            catalog=self.catalog,
            store=self.store,
            jobs_dir=f"{self.tempdir.name}/jobs",
            model_client=self.model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=self.acquisition_engine,
        )
        self.projection_writer = ServingProjectionWriter(self.store)
        self.person_asset_writer = PersonAssetWriter(self.store)
        self.company_asset_writer = CompanyAssetWriter(self.store)

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_collection_projection_crm_and_export_policy_api_fail_closed(self) -> None:
        self.projection_writer.publish_collection_authoritative_projection(
            collection_id="company:google",
            active_collection_version="v1",
            projection_id="proj_google_authoritative",
            members=[
                {
                    "candidate_identity_key": "linkedin:google-ada",
                    "person_identity_key": "linkedin:google-ada",
                    "candidate_id": "google-ada",
                    "public_summary": {"display_name": "Google Ada"},
                }
            ],
            replace_members=True,
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            collection_payload = _get_json(
                f"{base_url}/api/collections/{quote('company:google')}/authoritative-projection"
            )
            self.assertEqual(collection_payload["projection_id"], "proj_google_authoritative")
            self.assertFalse(collection_payload["read_contract"]["fallback_used"])

            collections_payload = _get_json(f"{base_url}/api/collections")
            self.assertEqual(collections_payload["status"], "ready")
            self.assertEqual(collections_payload["collection_count"], 1)
            self.assertEqual(collections_payload["collections"][0]["collection_id"], "company:google")
            self.assertEqual(collections_payload["collections"][0]["active_projection_id"], "proj_google_authoritative")
            self.assertEqual(collections_payload["collections"][0]["projection_url"], "/projections/proj_google_authoritative")
            overview_media = collections_payload["collections"][0]["company_media"]
            self.assertEqual(overview_media["logo_status"], "logo_unavailable")
            self.assertEqual(overview_media["placeholder"]["kind"], "initials")
            self.assertEqual(overview_media["placeholder"]["text"], "GO")
            self.assertTrue(overview_media["media_contract"]["fallback_used"])
            self.assertEqual(overview_media["media_contract"]["source"], "CompanyAsset.logo_media")
            self.assertEqual(overview_media["media_contract"]["reason"], "company_logo_media_asset_not_implemented")
            self.assertFalse(collections_payload["read_contract"]["fallback_used"])
            self.assertEqual(collections_payload["read_contract"]["result_serving_source"], "serving_projection_members")

            coverage_payload = _get_json(f"{base_url}/api/collections/{quote('company:google')}/coverage")
            self.assertEqual(coverage_payload["status"], "ready")
            self.assertEqual(coverage_payload["projection_id"], "proj_google_authoritative")
            self.assertEqual(coverage_payload["coverage"]["profile_completeness"]["count_scope"], "exact_projection")
            self.assertFalse(coverage_payload["read_contract"]["fallback_used"])
            self.assertEqual(coverage_payload["read_contract"]["result_serving_source"], "serving_projection_members")

            asset_entry_payload = _get_json(f"{base_url}/api/collections/{quote('company:google')}/asset-entry")
            self.assertEqual(asset_entry_payload["asset_entry"]["active_projection_id"], "proj_google_authoritative")
            self.assertEqual(asset_entry_payload["asset_entry"]["candidate_count"], 1)
            entry_media = asset_entry_payload["asset_entry"]["company_media"]
            self.assertEqual(entry_media["logo_status"], "logo_unavailable")
            self.assertEqual(entry_media["placeholder"]["text"], "GO")
            self.assertTrue(entry_media["media_contract"]["fallback_used"])
            self.assertFalse(asset_entry_payload["asset_entry"]["acquisition_handoff"]["available"])
            self.assertNotIn("start_new_acquisition", asset_entry_payload["asset_entry"])
            self.assertFalse(asset_entry_payload["read_contract"]["fallback_used"])

            policy_payload = _get_json(f"{base_url}/api/projections/proj_google_authoritative/export-policy")
            self.assertIn("human_promoted_assertions", policy_payload["export_policy"]["default_included_groups"])

            crm_payload = _post_json(
                f"{base_url}/api/crm/records",
                {
                    "projection_id": "proj_google_authoritative",
                    "candidate_identity_key": "linkedin:google-ada",
                    "idempotency_key": "api-add-google-ada",
                    "stage": "researching",
                },
            )
            self.assertIn(crm_payload["status"], {"upserted", "idempotent"})

            self.projection_writer.publish_collection_authoritative_projection(
                collection_id="company:openai",
                active_collection_version="v1",
                projection_id="proj_openai_authoritative",
                members=[
                    {
                        "candidate_identity_key": "linkedin:openai-grace",
                        "person_identity_key": "linkedin:openai-grace",
                        "candidate_id": "openai-grace",
                        "public_summary": {"display_name": "OpenAI Grace"},
                    }
                ],
                replace_members=True,
            )
            openai_crm_payload = _post_json(
                f"{base_url}/api/crm/records",
                {
                    "projection_id": "proj_openai_authoritative",
                    "candidate_identity_key": "linkedin:openai-grace",
                    "idempotency_key": "api-add-openai-grace",
                    "stage": "researching",
                },
            )
            self.assertIn(openai_crm_payload["status"], {"upserted", "idempotent"})

            crm_list_payload = _get_json(
                f"{base_url}/api/crm/records?source_projection_id=proj_google_authoritative"
            )
            self.assertEqual(crm_list_payload["status"], "ready")
            self.assertEqual(crm_list_payload["read_contract"]["source"], "crm_records")
            self.assertFalse(crm_list_payload["read_contract"]["legacy_target_candidates_used"])
            self.assertEqual(crm_list_payload["crm_records"][0]["stage"], "researching")
            self.assertEqual(crm_list_payload["crm_records"][0]["source"], "crm_records")
            self.assertEqual(crm_list_payload["crm_records"][0]["follow_up_status"], "pending_outreach")

            crm_collection_payload = _get_json(
                f"{base_url}/api/crm/records?source_collection_id={quote('company:google')}"
            )
            self.assertEqual(crm_collection_payload["status"], "ready")
            self.assertEqual(crm_collection_payload["source_collection_id"], "company:google")
            self.assertEqual(crm_collection_payload["record_count"], 1)
            self.assertEqual(crm_collection_payload["crm_records"][0]["source_collection_id"], "company:google")
            self.assertEqual(crm_collection_payload["crm_records"][0]["candidate_identity_key"], "linkedin:google-ada")

            openai_collection_payload = _get_json(
                f"{base_url}/api/crm/records?source_collection_id={quote('company:openai')}"
            )
            self.assertEqual(openai_collection_payload["record_count"], 1)
            self.assertEqual(openai_collection_payload["crm_records"][0]["source_collection_id"], "company:openai")
            self.assertEqual(
                openai_collection_payload["crm_records"][0]["candidate_identity_key"],
                "linkedin:openai-grace",
            )

            crm_record_id = crm_list_payload["crm_records"][0]["crm_record_id"]
            crm_update_payload = _patch_json(
                f"{base_url}/api/crm/records/{quote(crm_record_id, safe='')}",
                {
                    "follow_up_status": "contacted_waiting",
                    "quality_score": 88,
                    "comment": "Reached out after projection review.",
                },
            )
            self.assertEqual(crm_update_payload["status"], "updated")
            self.assertEqual(crm_update_payload["write_contract"]["owner"], "CRMWriter")
            self.assertFalse(crm_update_payload["write_contract"]["legacy_target_candidates_written"])
            self.assertEqual(crm_update_payload["crm_record"]["stage"], "contacted_waiting")
            self.assertEqual(crm_update_payload["crm_record"]["follow_up_status"], "contacted_waiting")
            self.assertEqual(crm_update_payload["crm_record"]["quality_score"], 88)
            self.assertEqual(crm_update_payload["crm_record"]["comment"], "Reached out after projection review.")
            engagement = self.store.get_crm_engagement(crm_update_payload["crm_record"]["current_engagement_id"])
            self.assertEqual(engagement["metadata"]["comment"], "Reached out after projection review.")

            task_result = self.orchestrator.crm_writer.create_crm_task(
                crm_record_id=crm_record_id,
                title="Review evidence",
                description="Check promoted public web signals.",
                due_at="2026-06-01T00:00:00Z",
                actor_type="unit-test",
                actor_id="projection-crm-api",
                idempotency_key="api-task-google-ada",
            )
            self.assertEqual(task_result["write_contract"]["source"], "crm_tasks+crm_events")
            crm_tasks_payload = _get_json(f"{base_url}/api/crm/records/{quote(crm_record_id, safe='')}/tasks")
            self.assertEqual(crm_tasks_payload["status"], "ready")
            self.assertEqual(crm_tasks_payload["read_contract"]["source"], "crm_tasks")
            self.assertEqual(crm_tasks_payload["read_contract"]["audit_source"], "crm_events")
            self.assertEqual(crm_tasks_payload["task_count"], 1)
            self.assertEqual(crm_tasks_payload["crm_tasks"][0]["title"], "Review evidence")
            self.assertEqual(crm_tasks_payload["crm_tasks"][0]["source"], "crm_tasks")
            self.assertTrue(crm_tasks_payload["crm_tasks"][0]["source_event_id"].startswith("crmevt_"))

            crm_state_payload = _get_json(
                f"{base_url}/api/projections/proj_google_authoritative/crm-state"
                "?candidate_identity_keys=linkedin:google-ada"
            )
            overlay = crm_state_payload["crm_overlay_by_candidate_identity_key"]["linkedin:google-ada"]
            self.assertTrue(overlay["in_crm"])
            self.assertEqual(overlay["stage"], "contacted_waiting")
            self.assertFalse(crm_state_payload["read_contract"]["auto_create"])

            self.store.upsert_target_candidate(
                {
                    "candidate_id": "legacy-api",
                    "candidate_name": "Legacy API",
                    "linkedin_url": "https://www.linkedin.com/in/legacy-api/",
                    "primary_email": "legacy@example.com",
                    "source_projection_id": "proj_google_authoritative",
                }
            )
            backfill_payload = _post_json(
                f"{base_url}/api/crm/backfill-target-candidates",
                {"source_projection_id": "proj_google_authoritative", "limit": 10},
            )
            self.assertEqual(backfill_payload["status"], "backfilled")
            self.assertGreaterEqual(backfill_payload["migrated_count"], 1)
            legacy_export_payload, legacy_export_status = _post_json_with_status(
                f"{base_url}/api/target-candidates/export",
                {"recordIds": []},
            )
            self.assertEqual(legacy_export_status, 410)
            self.assertEqual(legacy_export_payload["status"], "retired")
            self.assertEqual(legacy_export_payload["canonical_export_path"], "/api/projections/export")

            self.person_asset_writer.record_asset(
                {
                    "asset_id": "pa_google_raw",
                    "person_identity_key": "linkedin:google-ada",
                    "asset_type": "raw_profile",
                    "metadata": {"indexed_terms": ["Gemini multimodal systems"]},
                    "visibility_scope": "internal",
                }
            )
            index_payload = _post_json(
                f"{base_url}/api/projections/rebuild-person-search-index",
                {
                    "projection_id": "proj_google_authoritative",
                    "count_scope": "exact_projection",
                    "raw_profile_index_watermark": "raw-google-1",
                },
            )
            self.assertEqual(index_payload["status"], "indexed")
            self.assertEqual(index_payload["person_index"]["raw_profile_indexed_count"], 1)
            raw_evidence_backfill_payload = _post_json(
                f"{base_url}/api/persons/backfill-raw-evidence-indexes",
                {"projection_ids": ["proj_google_authoritative"], "dry_run": True},
            )
            self.assertEqual(raw_evidence_backfill_payload["status"], "dry_run")
            self.assertEqual(raw_evidence_backfill_payload["projections"][0]["status"], "would_backfill")
            search_payload = _get_json(
                f"{base_url}/api/projections/proj_google_authoritative/search?search=Gemini"
            )
            self.assertEqual(search_payload["status"], "ready")
            self.assertEqual(search_payload["filtered_candidate_count"], 1)
            self.assertEqual(search_payload["read_contract"]["source"], "projection_person_search_index+serving_projection_members")
            self.assertEqual(search_payload["index_filter_readiness"]["count_scope"], "exact_projection")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_asset_backfill_http_routes_are_explicit_migration_paths(self) -> None:
        self.store.upsert_person_public_web_signal(
            {
                "signal_id": "signal-api-public-web-backfill",
                "run_id": "run-api-public-web-backfill",
                "record_id": "crmrec-api-public-web-backfill",
                "person_identity_key": "linkedin:api-public-web-backfill",
                "signal_kind": "email_candidate",
                "signal_type": "work",
                "value": "api-backfill@example.com",
                "normalized_value": "api-backfill@example.com",
                "source_url": "https://example.com/api-backfill",
                "source_domain": "example.com",
                "confidence_label": "high",
                "confidence_score": 0.93,
                "identity_match_label": "strong",
                "identity_match_score": 0.9,
                "publishable": True,
                "promotion_status": "not_promoted",
            }
        )
        self.store.repos.serving_projection.upsert(
            {
                "projection_id": "proj-api-avatar-backfill",
                "projection_type": "collection_authoritative_projection",
                "collection_id": "company:openai",
                "source_run_id": "run-api-avatar-backfill",
                "state": "serving",
            }
        )
        self.store.upsert_serving_projection_members(
            "proj-api-avatar-backfill",
            [
                {
                    "candidate_identity_key": "linkedin:api-avatar-backfill",
                    "person_identity_key": "linkedin:api-avatar-backfill",
                    "profile_url_key": "api-avatar-backfill",
                    "rank_index": 1,
                    "public_summary": {
                        "candidate_id": "cand-api-avatar-backfill",
                        "name": "API Avatar Backfill",
                        "linkedin_url": "https://www.linkedin.com/in/api-avatar-backfill/",
                        "avatar_url": "https://provider.example.com/avatar/api-avatar.png",
                    },
                }
            ],
        )
        self.store.upsert_company_public_web_asset_run(
            {
                "run_id": "company-public-web-run-api-backfill",
                "target_company": "DeepMind",
                "company_key": "deepmind",
                "idempotency_key": "company-public-web-api-backfill",
                "status": "completed",
                "phase": "seed_url_only",
                "source_families": ["company_research"],
                "seed_urls": ["https://deepmind.google/research/"],
                "summary": {"asset_count": 1},
            }
        )
        self.store.upsert_company_public_web_asset(
            {
                "asset_id": "company-public-web-asset-api-backfill",
                "target_company": "DeepMind",
                "company_key": "deepmind",
                "latest_run_id": "company-public-web-run-api-backfill",
                "source_family": "company_research",
                "asset_kind": "web_page",
                "title": "DeepMind Research",
                "url": "https://deepmind.google/research/",
                "summary": "Research updates from DeepMind.",
                "model_safe_payload": {"title": "DeepMind Research", "summary": "Research updates from DeepMind."},
                "source_run_ids": ["company-public-web-run-api-backfill"],
                "status": "active",
            }
        )

        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            public_web_dry_run = _post_json(
                f"{base_url}/api/persons/backfill-public-web-signals",
                {"run_id": "run-api-public-web-backfill", "dry_run": True},
            )
            self.assertEqual(public_web_dry_run["status"], "dry_run")
            self.assertEqual(public_web_dry_run["eligible_signal_count"], 1)
            self.assertTrue(public_web_dry_run["migration_path"])
            self.assertFalse(public_web_dry_run["module_state_mutated"])
            self.assertFalse(public_web_dry_run["read_contract"]["fallback_used"])
            self.assertFalse(public_web_dry_run["read_contract"]["normal_reader_repair"])
            self.assertEqual(
                self.store.list_person_assets(
                    person_identity_key="linkedin:api-public-web-backfill",
                    asset_type="public_web_signal",
                ),
                [],
            )

            public_web_apply, public_web_status = _post_json_with_status(
                f"{base_url}/api/persons/backfill-public-web-signals",
                {"run_id": "run-api-public-web-backfill", "dry_run": False},
            )
            self.assertEqual(public_web_status, 400)
            self.assertEqual(public_web_apply["status"], "invalid")
            self.assertEqual(public_web_apply["reason"], "operator_review_required")
            self.assertFalse(public_web_apply["module_state_mutated"])
            self.assertTrue(public_web_apply["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(
                self.store.list_person_assets(
                    person_identity_key="linkedin:api-public-web-backfill",
                    asset_type="public_web_signal",
                ),
                [],
            )

            public_web_apply = _post_json(
                f"{base_url}/api/persons/backfill-public-web-signals",
                {"run_id": "run-api-public-web-backfill", "dry_run": False, "reviewed": True},
            )
            self.assertEqual(public_web_apply["status"], "backfilled")
            self.assertEqual(public_web_apply["asset_count"], 1)
            self.assertTrue(public_web_apply["module_state_mutated"])
            self.assertFalse(public_web_apply["read_contract"]["normal_reader_repair"])
            self.assertEqual(
                len(
                    self.store.list_person_assets(
                        person_identity_key="linkedin:api-public-web-backfill",
                        asset_type="public_web_signal",
                    )
                ),
                1,
            )

            company_fact_dry_run = _post_json(
                f"{base_url}/api/company-assets/backfill-public-web-assets",
                {"target_company": "DeepMind", "dry_run": True},
            )
            self.assertEqual(company_fact_dry_run["status"], "dry_run")
            self.assertEqual(company_fact_dry_run["eligible_asset_count"], 1)
            self.assertTrue(company_fact_dry_run["migration_path"])
            self.assertFalse(company_fact_dry_run["module_state_mutated"])
            self.assertFalse(company_fact_dry_run["read_contract"]["fallback_used"])
            self.assertFalse(company_fact_dry_run["read_contract"]["normal_reader_repair"])
            self.assertEqual(self.store.list_company_assets(company_key="deepmind", limit=10), [])

            company_fact_apply, company_fact_status = _post_json_with_status(
                f"{base_url}/api/company-assets/backfill-public-web-assets",
                {"target_company": "DeepMind", "dry_run": False},
            )
            self.assertEqual(company_fact_status, 400)
            self.assertEqual(company_fact_apply["status"], "invalid")
            self.assertEqual(company_fact_apply["reason"], "operator_review_required")
            self.assertFalse(company_fact_apply["module_state_mutated"])
            self.assertTrue(company_fact_apply["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(self.store.list_company_assets(company_key="deepmind", limit=10), [])

            company_fact_apply = _post_json(
                f"{base_url}/api/company-assets/backfill-public-web-assets",
                {"target_company": "DeepMind", "dry_run": False, "reviewed": True},
            )
            self.assertEqual(company_fact_apply["status"], "backfilled")
            self.assertEqual(company_fact_apply["synced_asset_count"], 1)
            self.assertEqual(company_fact_apply["synced_evidence_count"], 1)
            self.assertTrue(company_fact_apply["module_state_mutated"])
            self.assertFalse(company_fact_apply["read_contract"]["normal_reader_repair"])
            company_facts = self.store.list_company_assets(company_key="deepmind", limit=10)
            self.assertEqual(len(company_facts), 1)
            self.assertEqual(company_facts[0]["asset_type"], "company_research")
            self.assertEqual(company_facts[0]["source_kind"], "company_public_web_model_safe")

            avatar_dry_run = _post_json(
                f"{base_url}/api/media/backfill-person-avatars",
                {
                    "projection_id": "proj-api-avatar-backfill",
                    "workflow_run_id": "wf-api-avatar-backfill",
                    "dry_run": True,
                },
            )
            self.assertEqual(avatar_dry_run["status"], "dry_run")
            self.assertEqual(avatar_dry_run["eligible_command_count"], 1)
            self.assertTrue(avatar_dry_run["migration_path"])
            self.assertFalse(avatar_dry_run["module_state_mutated"])
            self.assertFalse(avatar_dry_run["read_contract"]["request_path_fetch"])
            self.assertFalse(avatar_dry_run["read_contract"]["normal_reader_repair"])
            self.assertEqual(self.store.list_workflow_commands(workflow_run_id="wf-api-avatar-backfill"), [])

            avatar_planned, avatar_planned_status = _post_json_with_status(
                f"{base_url}/api/media/backfill-person-avatars",
                {
                    "projection_id": "proj-api-avatar-backfill",
                    "workflow_run_id": "wf-api-avatar-backfill",
                    "dry_run": False,
                },
            )
            self.assertEqual(avatar_planned_status, 400)
            self.assertEqual(avatar_planned["status"], "invalid")
            self.assertEqual(avatar_planned["reason"], "operator_review_required")
            self.assertFalse(avatar_planned["module_state_mutated"])
            self.assertTrue(avatar_planned["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(self.store.list_workflow_commands(workflow_run_id="wf-api-avatar-backfill"), [])

            avatar_planned = _post_json(
                f"{base_url}/api/media/backfill-person-avatars",
                {
                    "projection_id": "proj-api-avatar-backfill",
                    "workflow_run_id": "wf-api-avatar-backfill",
                    "dry_run": False,
                    "reviewed": True,
                },
            )
            self.assertEqual(avatar_planned["status"], "planned")
            self.assertEqual(avatar_planned["planned_command_count"], 1)
            self.assertFalse(avatar_planned["run_now"])
            self.assertFalse(avatar_planned["read_contract"]["request_path_fetch"])
            avatar_commands = self.store.list_workflow_commands(
                workflow_run_id="wf-api-avatar-backfill",
                owner=MEDIA_ASSET_OWNER,
            )
            self.assertEqual(len(avatar_commands), 1)
            self.assertEqual(avatar_commands[0]["command_type"], MEDIA_ASSET_CACHE_COMMAND_TYPE)
            self.assertEqual(avatar_commands[0]["payload"]["entity_type"], "person")
            self.assertEqual(avatar_commands[0]["payload"]["asset_type"], "avatar_media")
            self.assertEqual(
                self.store.list_person_assets(
                    person_identity_key="linkedin:api-avatar-backfill",
                    asset_type="avatar_media",
                ),
                [],
            )

            company_logo_dry_run = _post_json(
                f"{base_url}/api/media/backfill-company-logos",
                {
                    "target_company": "OpenAI",
                    "source_url": "https://static.example.com/openai-logo.png",
                    "workflow_run_id": "wf-api-company-logo-backfill",
                    "dry_run": True,
                },
            )
            self.assertEqual(company_logo_dry_run["status"], "dry_run")
            self.assertEqual(company_logo_dry_run["eligible_command_count"], 1)
            self.assertFalse(company_logo_dry_run["module_state_mutated"])
            self.assertFalse(company_logo_dry_run["read_contract"]["request_path_fetch"])
            self.assertFalse(company_logo_dry_run["read_contract"]["normal_reader_repair"])
            self.assertFalse(company_logo_dry_run["read_contract"]["homepage_favicon_derivation_default"])
            self.assertEqual(self.store.list_workflow_commands(workflow_run_id="wf-api-company-logo-backfill"), [])

            company_logo_planned, company_logo_planned_status = _post_json_with_status(
                f"{base_url}/api/media/backfill-company-logos",
                {
                    "target_company": "OpenAI",
                    "source_url": "https://static.example.com/openai-logo.png",
                    "workflow_run_id": "wf-api-company-logo-backfill",
                    "dry_run": False,
                },
            )
            self.assertEqual(company_logo_planned_status, 400)
            self.assertEqual(company_logo_planned["status"], "invalid")
            self.assertEqual(company_logo_planned["reason"], "operator_review_required")
            self.assertFalse(company_logo_planned["module_state_mutated"])
            self.assertTrue(company_logo_planned["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(self.store.list_workflow_commands(workflow_run_id="wf-api-company-logo-backfill"), [])

            company_logo_planned = _post_json(
                f"{base_url}/api/media/backfill-company-logos",
                {
                    "target_company": "OpenAI",
                    "source_url": "https://static.example.com/openai-logo.png",
                    "workflow_run_id": "wf-api-company-logo-backfill",
                    "dry_run": False,
                    "reviewed": True,
                },
            )
            self.assertEqual(company_logo_planned["status"], "planned")
            self.assertEqual(company_logo_planned["planned_command_count"], 1)
            self.assertFalse(company_logo_planned["run_now"])
            self.assertFalse(company_logo_planned["read_contract"]["request_path_fetch"])
            company_logo_commands = self.store.list_workflow_commands(
                workflow_run_id="wf-api-company-logo-backfill",
                owner=MEDIA_ASSET_OWNER,
            )
            self.assertEqual(len(company_logo_commands), 1)
            self.assertEqual(company_logo_commands[0]["command_type"], MEDIA_ASSET_CACHE_COMMAND_TYPE)
            self.assertEqual(company_logo_commands[0]["payload"]["entity_type"], "company")
            self.assertEqual(company_logo_commands[0]["payload"]["asset_type"], "logo_media")
            self.assertEqual(company_logo_commands[0]["payload"]["company_key"], "openai")
            self.assertEqual(self.store.list_company_assets(company_key="openai", asset_type="logo_media"), [])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_legacy_job_result_endpoint_retirement_returns_projection_pointer_for_all_public_readers(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-retired",
            collection_id="company:openai",
            projection_id="proj_retired",
            members=[
                {
                    "candidate_identity_key": "linkedin:retired",
                    "person_identity_key": "linkedin:retired",
                    "public_summary": {"display_name": "Retired Ada"},
                }
            ],
            replace_members=True,
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        previous = os.environ.get("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS")
        os.environ["SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS"] = "1"
        try:
            endpoints = {
                "/api/jobs/job-retired/results": "/api/jobs/{job_id}/results",
                "/api/jobs/job-retired/dashboard": "/api/jobs/{job_id}/dashboard",
                "/api/jobs/job-retired/candidates": "/api/jobs/{job_id}/candidates",
                f"/api/jobs/job-retired/candidates/{quote('linkedin:retired')}": (
                    "/api/jobs/{job_id}/candidates/{candidate_id}"
                ),
            }
            for endpoint, legacy_endpoint in endpoints.items():
                with self.subTest(endpoint=endpoint):
                    payload, status = _get_json_with_status(f"http://{host}:{port}{endpoint}")
                    self.assertEqual(status, 410)
                    self.assertEqual(payload["status"], "retired")
                    self.assertEqual(payload["reason"], "legacy_job_result_endpoint_retired")
                    self.assertEqual(payload["projection_id"], "proj_retired")
                    self.assertEqual(payload["legacy_endpoint"], legacy_endpoint)
                    self.assertFalse(payload["read_contract"]["fallback_used"])
                    self.assertTrue(payload["read_contract"]["fail_closed"])
                    self.assertEqual(payload["projection_url"].split("?")[0], "/projections/proj_retired")
        finally:
            if previous is None:
                os.environ.pop("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS", None)
            else:
                os.environ["SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS"] = previous
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_projection_ready_legacy_job_result_endpoint_retires_by_default(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-auto-retired",
            collection_id="company:openai",
            projection_id="proj_auto_retired",
            members=[
                {
                    "candidate_identity_key": "linkedin:auto-retired",
                    "person_identity_key": "linkedin:auto-retired",
                    "public_summary": {"display_name": "Auto Retired Ada"},
                }
            ],
            replace_members=True,
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        previous_retire = os.environ.get("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS")
        previous_allow = os.environ.get("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS")
        os.environ.pop("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS", None)
        os.environ.pop("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS", None)
        try:
            payload, status = _get_json_with_status(f"http://{host}:{port}/api/jobs/job-auto-retired/results")
            self.assertEqual(status, 410)
            self.assertEqual(payload["projection_id"], "proj_auto_retired")
            self.assertTrue(payload["retirement"]["cutover_enforced"])
            self.assertTrue(payload["retirement"]["cutover_defaulted_from_projection"])
            self.assertFalse(payload["retirement"]["normal_serving_allowed"])
        finally:
            if previous_retire is None:
                os.environ.pop("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS", None)
            else:
                os.environ["SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS"] = previous_retire
            if previous_allow is None:
                os.environ.pop("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS", None)
            else:
                os.environ["SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS"] = previous_allow
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_unmigrated_legacy_job_result_endpoint_returns_migration_required_by_default(self) -> None:
        self.store.save_job(
            job_id="job-unmigrated-legacy",
            job_type="workflow",
            status="completed",
            stage="completed",
            request_payload={
                "raw_user_request": "帮我找OpenAI做Agent方向的人",
                "target_company": "OpenAI",
            },
            plan_payload={},
            summary_payload={},
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        previous_retire = os.environ.get("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS")
        previous_allow = os.environ.get("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS")
        os.environ.pop("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS", None)
        os.environ.pop("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS", None)
        try:
            payload, status = _get_json_with_status(
                f"http://{host}:{port}/api/jobs/job-unmigrated-legacy/results"
            )
            self.assertEqual(status, 410)
            self.assertEqual(payload["status"], "retired")
            self.assertEqual(payload["reason"], "legacy_job_result_endpoint_migration_required")
            self.assertEqual(payload["projection_id"], "")
            self.assertEqual(payload["projection_url"], "")
            self.assertEqual(payload["retirement"]["status"], "retired_migration_required")
            self.assertTrue(payload["retirement"]["cutover_enforced"])
            self.assertTrue(payload["retirement"]["cutover_defaulted_to_migration_required"])
            self.assertFalse(payload["retirement"]["normal_serving_allowed"])
            self.assertEqual(payload["retirement"]["missing_run_projection_links"], ["job-unmigrated-legacy"])
            self.assertFalse(payload["read_contract"]["fallback_used"])
            self.assertTrue(payload["read_contract"]["fail_closed"])
        finally:
            if previous_retire is None:
                os.environ.pop("SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS", None)
            else:
                os.environ["SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS"] = previous_retire
            if previous_allow is None:
                os.environ.pop("SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS", None)
            else:
                os.environ["SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS"] = previous_allow
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_person_detail_export_and_public_web_promotion_api_contracts(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-agent",
            collection_id="company:openai",
            projection_id="proj_openai_delta",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-ada",
                    "person_identity_key": "linkedin:openai-ada",
                    "candidate_id": "openai-ada",
                    "profile_readiness": "ready",
                    "card_readiness": "ready",
                    "public_summary": {
                        "display_name": "OpenAI Ada",
                        "headline": "Agent engineer",
                        "linkedin_url": "https://www.linkedin.com/in/openai-ada/",
                        "primary_email": "must-not-leak@example.com",
                    },
                }
            ],
            replace_members=True,
        )
        self.person_asset_writer.record_assertion(
            {
                "assertion_id": "assertion-openai-homepage",
                "person_identity_key": "linkedin:openai-ada",
                "assertion_type": "homepage_url",
                "value": "https://ada.example/",
                "authority": "operator_confirmed",
                "verification_status": "active",
                "metadata": {"export_policy": "default_human_promoted_assertion"},
            }
        )
        self.person_asset_writer.record_assertion(
            {
                "assertion_id": "assertion-openai-email-needs-review",
                "person_identity_key": "linkedin:openai-ada",
                "assertion_type": "primary_email",
                "value": "review@example.com",
                "authority": "agent_suggested",
                "verification_status": "needs_review",
            }
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            candidate_key = quote("linkedin:openai-ada", safe="")
            detail_payload = _get_json(f"{base_url}/api/projections/proj_openai_delta/persons/{candidate_key}")
            self.assertEqual(detail_payload["status"], "ready")
            self.assertEqual(detail_payload["public_summary"]["display_name"], "OpenAI Ada")
            self.assertNotIn("primary_email", detail_payload["public_summary"])
            self.assertFalse(detail_payload["read_contract"]["fallback_used"])

            person_payload = _get_json(f"{base_url}/api/persons/{quote('linkedin:openai-ada', safe='')}")
            self.assertEqual(person_payload["status"], "ready")
            self.assertEqual(person_payload["projection_membership_count"], 1)
            self.assertEqual(person_payload["assertion_summary"]["assertion_count"], 2)

            # C1.4b: export is an async task — submit -> 202 + task_id; the worker
            # export drain builds the artifact; then poll + download the handle.
            submit_payload, submit_status = _post_json_with_status(
                f"{base_url}/api/projections/export",
                {"projection_id": "proj_openai_delta"},
            )
            self.assertEqual(submit_status, 202)
            self.assertEqual(submit_payload["status"], "queued")
            export_task_id = str(submit_payload["task_id"])
            self.orchestrator._drain_export_projection_generate_commands({})
            export_poll = _get_json(f"{base_url}/api/exports/{export_task_id}")
            self.assertEqual(export_poll["status"], "succeeded")
            self.assertEqual(
                export_poll["artifact"]["handle"], f"/api/exports/{export_task_id}/artifact"
            )
            export_body, export_headers = _get_binary(
                f"{base_url}/api/exports/{export_task_id}/artifact"
            )
            self.assertEqual(export_headers["X-Sourcing-Projection-Id"], "proj_openai_delta")
            self.assertEqual(export_headers["X-Sourcing-Export-Record-Count"], "1")
            self.assertEqual(export_headers["X-Sourcing-Skipped-Assertion-Count"], "1")
            with zipfile.ZipFile(BytesIO(export_body)) as archive:
                names = archive.namelist()
                manifest_name = next(name for name in names if name.endswith("projection_export_manifest.json"))
                csv_name = next(name for name in names if name.endswith("projection_candidates.csv"))
                manifest = json.loads(archive.read(manifest_name).decode("utf-8"))
                csv_text = archive.read(csv_name).decode("utf-8-sig")
            self.assertEqual(manifest["record_count"], 1)
            self.assertEqual(manifest["skipped_assertion_count"], 1)
            self.assertIn("https://ada.example/", csv_text)
            self.assertNotIn("review@example.com", csv_text)
            export_commands = self.store.list_workflow_commands(
                owner=EXPORT_PROJECTION_GENERATE_OWNER,
                statuses=["succeeded"],
                limit=10,
            )
            self.assertEqual(len(export_commands), 1)
            self.assertEqual(export_commands[0]["command_type"], EXPORT_PROJECTION_GENERATE_COMMAND_TYPE)
            self.assertEqual(export_commands[0]["stage_id"], "projection_export")
            self.assertEqual(export_commands[0]["readiness_effect"], "projection_export_generated")
            self.assertEqual(export_commands[0]["produced_entity_counts"]["projection"], 1)
            self.assertTrue(Path(export_commands[0]["result"]["artifact_path"]).exists())

            seed_legacy_target_public_web_promotion(
                self.store,
                {
                    "promotion_id": "promotion-openai-x",
                    "signal_id": "signal-openai-x",
                    "run_id": "public-web-run-openai",
                    "person_identity_key": "linkedin:openai-ada",
                    "record_id": "target-openai-ada",
                    "candidate_id": "openai-ada",
                    "candidate_name": "OpenAI Ada",
                    "signal_kind": "profile_link",
                    "signal_type": "x_url",
                    "url": "https://x.com/openai_ada",
                    "normalized_value": "https://x.com/openai_ada",
                    "new_value": "https://x.com/openai_ada",
                    "source_url": "https://x.com/openai_ada",
                    "source_domain": "x.com",
                    "publishable": True,
                    "action": "promote",
                    "promoted_field": "public_web_profile_link",
                }
            )
            public_web_backfill = _post_json(
                f"{base_url}/api/crm/backfill-public-web-promotions",
                {"limit": 10},
            )
            self.assertEqual(public_web_backfill["status"], "backfilled")
            self.assertGreaterEqual(public_web_backfill["assertions_created"], 1)
            x_assertions = self.store.list_person_assertions(
                person_identity_key="linkedin:openai-ada",
                assertion_type="x_url",
            )
            self.assertEqual(x_assertions[0]["value"], "https://x.com/openai_ada")
            self.assertEqual(x_assertions[0]["authority"], "operator_confirmed")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_crm_record_drives_public_web_without_target_candidate_bridge_state(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-agent",
            collection_id="company:openai",
            projection_id="proj_openai_public_web",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-public-web",
                    "person_identity_key": "linkedin:https://www.linkedin.com/in/openai-public-web",
                    "candidate_id": "openai-public-web",
                    "public_summary": {
                        "display_name": "OpenAI Public Web",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-public-web/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_public_web",
                "candidate_identity_key": "linkedin:openai-public-web",
                "idempotency_key": "api-add-openai-public-web",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]

        start_result = self.orchestrator.start_crm_record_public_web_search(
            {
                "workspace_id": "default",
                "crm_record_ids": [crm_record_id],
                "options": {"source_families": ["profile_web_presence"], "fetch_content": False},
            }
        )
        bridge = self.store.get_target_candidate(crm_record_id)

        self.assertIn(start_result["status"], {"queued", "joined"})
        self.assertEqual(start_result["write_contract"]["source"], "crm_records")
        self.assertEqual(start_result["write_contract"]["public_web_storage_bridge"], "")
        self.assertFalse(start_result["write_contract"]["legacy_target_candidate_state_owner"])
        self.assertIsNone(bridge)
        self.assertEqual(start_result["workflow_command"]["command_type"], CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE)
        self.assertEqual(start_result["workflow_command"]["owner"], CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER)
        self.assertEqual(start_result["workflow_command"]["status"], "succeeded")
        self.assertEqual(start_result["worker_summary"]["owner"], CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER)
        self.assertEqual(start_result["worker_summary"]["command_type"], CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE)
        self.assertFalse(start_result["worker_summary"]["legacy_bridge_used"])
        self.assertEqual(start_result["worker_summary"]["queued_worker_count"], 0)
        self.assertEqual(start_result["worker_summary"]["queued_phase_command_count"], 1)
        self.assertEqual(
            start_result["worker_summary"]["items"][0]["worker_summary"]["phase_command_types"],
            ["crm.public_web.search.submit"],
        )
        self.assertEqual(len(start_result["worker_summary"]["items"][0]["phase_command_ids"]), 1)
        self.assertEqual(start_result["runs"][0]["record_id"], crm_record_id)
        self.assertEqual(
            start_result["runs"][0]["person_identity_key"],
            "linkedin:https://www.linkedin.com/in/openai-public-web",
        )
        owner_run = self.store.get_crm_public_web_run(run_id=start_result["runs"][0]["run_id"])
        self.assertIsNotNone(owner_run)
        self.assertEqual(owner_run["crm_record_id"], crm_record_id)
        self.assertEqual(owner_run["record_id"], crm_record_id)
        self.assertEqual(owner_run["execution_backend"], "crm_public_web_v1")
        self.assertEqual(owner_run["summary"]["owner"], "crm_public_web_v1")
        self.assertEqual(owner_run["source_target_run_id"], "")

        self.store.update_crm_public_web_run(
            start_result["runs"][0]["run_id"],
            {
                "status": "completed",
                "phase": "completed",
                "summary": {
                    **dict(start_result["runs"][0].get("summary") or {}),
                    "phase_metrics": {"signal_materialized_count": 0},
                },
            },
        )
        sync_result = sync_crm_public_web_batch_summary(self.store, start_result["batch"]["batch_id"])
        mirrored_run = self.store.get_crm_public_web_run(run_id=start_result["runs"][0]["run_id"])
        self.assertEqual(sync_result["status"], "updated")
        self.assertEqual(mirrored_run["status"], "completed")
        self.assertEqual(mirrored_run["phase"], "completed")
        self.assertEqual(mirrored_run["source_target_run_id"], "")

    def test_crm_public_web_start_fails_closed_when_queue_batch_owner_disabled(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-disabled-public-web",
            collection_id="company:openai",
            projection_id="proj_openai_disabled_public_web",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-disabled-public-web",
                    "person_identity_key": "linkedin:openai-disabled-public-web",
                    "candidate_id": "openai-disabled-public-web",
                    "public_summary": {
                        "display_name": "OpenAI Disabled Public Web",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-disabled-public-web/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_disabled_public_web",
                "candidate_identity_key": "linkedin:openai-disabled-public-web",
                "idempotency_key": "api-add-openai-disabled-public-web",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]

        with mock.patch.dict(os.environ, {"CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_OWNER_ENABLED": "0"}, clear=False):
            start_result = self.orchestrator.start_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "crm_record_ids": [crm_record_id],
                    "options": {"source_families": ["profile_web_presence"], "fetch_content": False},
                }
            )

        self.assertEqual(start_result["status"], "failed")
        self.assertEqual(start_result["reason"], "crm_public_web_queue_batch_command_owner_disabled")
        self.assertEqual(start_result["worker_summary"]["status"], "skipped")
        self.assertEqual(start_result["worker_summary"]["reason"], "crm_public_web_queue_batch_command_owner_disabled")
        self.assertEqual(start_result["workflow_command"]["command_type"], CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE)
        self.assertEqual(start_result["workflow_command"]["status"], "queued")
        self.assertEqual(self.store.list_crm_public_web_batches(workspace_id="default"), [])
        self.assertEqual(self.store.list_crm_public_web_runs(crm_record_id=crm_record_id, workspace_id="default"), [])

    def test_crm_public_web_api_routes_are_normal_path_and_fail_closed_for_legacy_ids(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-api",
            collection_id="company:openai",
            projection_id="proj_openai_public_web_api",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-public-web-api",
                    "person_identity_key": "linkedin:openai-public-web-api",
                    "candidate_id": "openai-public-web-api",
                    "public_summary": {
                        "display_name": "OpenAI Public Web API",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-public-web-api/",
                    },
                }
            ],
            replace_members=True,
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            add_payload = _post_json(
                f"{base_url}/api/crm/records",
                {
                    "projection_id": "proj_openai_public_web_api",
                    "candidate_identity_key": "linkedin:openai-public-web-api",
                    "idempotency_key": "api-add-openai-public-web-api",
                },
            )
            crm_record_id = add_payload["crm_record"]["crm_record_id"]

            missing_workspace_payload, missing_workspace_status = _post_json_with_status(
                f"{base_url}/api/crm/records/public-web-search",
                {
                    "crm_record_ids": [crm_record_id],
                    "options": {"source_families": ["profile_web_presence"], "fetch_content": False},
                },
            )
            self.assertEqual(missing_workspace_status, 400)
            self.assertEqual(missing_workspace_payload["reason"], "crm_public_web_workspace_id_required")

            start_payload = _post_json(
                f"{base_url}/api/crm/records/public-web-search",
                {
                    "workspace_id": "default",
                    "crm_record_ids": [crm_record_id],
                    "options": {"source_families": ["profile_web_presence"], "fetch_content": False},
                },
            )
            self.assertIn(start_payload["status"], {"queued", "joined"})
            self.assertEqual(start_payload["write_contract"]["source"], "crm_records")
            self.assertFalse(start_payload["write_contract"]["legacy_target_candidates_used"])
            self.assertFalse(start_payload["write_contract"]["legacy_target_candidate_state_owner"])

            poll_payload = _post_json(
                f"{base_url}/api/crm/records/public-web-search/poll",
                {"workspace_id": "default", "crm_record_ids": [crm_record_id], "limit": 25},
            )
            self.assertEqual(poll_payload["status"], "ok")
            self.assertEqual(poll_payload["read_contract"]["source"], "crm_records")
            self.assertFalse(poll_payload["read_contract"]["legacy_target_candidates_used"])
            self.assertEqual(poll_payload["runs"][0]["record_id"], crm_record_id)

            detail_payload = _get_json(
                f"{base_url}/api/crm/records/{quote(crm_record_id, safe='')}/public-web-search"
            )
            self.assertEqual(detail_payload["read_contract"]["source"], "crm_records")

            legacy_record = self.store.upsert_target_candidate(
                {
                    "record_id": "legacy-public-web-only",
                    "candidate_id": "legacy-public-web-only",
                    "candidate_name": "Legacy Only",
                    "linkedin_url": "https://www.linkedin.com/in/legacy-public-web-only/",
                }
            )
            self.assertEqual(legacy_record["id"], "legacy-public-web-only")
            legacy_start_payload, legacy_start_status = _post_json_with_status(
                f"{base_url}/api/crm/records/public-web-search",
                {"workspace_id": "default", "crm_record_ids": ["legacy-public-web-only"]},
            )
            self.assertEqual(legacy_start_status, 404)
            self.assertEqual(legacy_start_payload["reason"], "crm_record_not_found")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_legacy_target_candidate_public_web_endpoints_retire_by_default(self) -> None:
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            start_payload, start_status = _post_json_with_status(
                f"{base_url}/api/target-candidates/public-web-search",
                {"record_ids": ["legacy-target-public-web"]},
            )
            poll_payload, poll_status = _post_json_with_status(
                f"{base_url}/api/target-candidates/public-web-search/poll",
                {"record_ids": ["legacy-target-public-web"]},
            )
            list_payload, list_status = _get_json_with_status(
                f"{base_url}/api/target-candidates/public-web-search"
            )
            detail_payload, detail_status = _get_json_with_status(
                f"{base_url}/api/target-candidates/{quote('legacy-target-public-web', safe='')}/public-web-search"
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(start_status, 410)
        self.assertEqual(start_payload["reason"], "legacy_target_public_web_endpoint_retired")
        self.assertEqual(start_payload["canonical_endpoint"], "/api/crm/records/public-web-search")
        self.assertEqual(start_payload["read_contract"]["public_web_storage_bridge"], "")
        self.assertEqual(poll_status, 410)
        self.assertEqual(poll_payload["canonical_endpoint"], "/api/crm/records/public-web-search/poll")
        self.assertEqual(list_status, 410)
        self.assertEqual(list_payload["canonical_endpoint"], "/api/crm/records/public-web-search")
        self.assertEqual(detail_status, 410)
        self.assertEqual(detail_payload["canonical_endpoint"], "/api/crm/records/{crm_record_id}/public-web-search")

    def test_crm_public_web_promotion_writes_crm_owner_without_target_bridge(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-crm-public-web-promotion",
            collection_id="company:openai",
            projection_id="proj_openai_crm_public_web_promotion",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-crm-public-web-promotion",
                    "person_identity_key": "linkedin:openai-crm-public-web-promotion",
                    "candidate_id": "openai-crm-public-web-promotion",
                    "public_summary": {
                        "display_name": "OpenAI CRM Public Web Promotion",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-promotion/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_crm_public_web_promotion",
                "candidate_identity_key": "linkedin:openai-crm-public-web-promotion",
                "idempotency_key": "api-add-openai-crm-public-web-promotion",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]
        run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-promotion-run-1",
                "batch_id": "crm-public-web-promotion-batch-1",
                "crm_record_id": crm_record_id,
                "workspace_id": "default",
                "candidate_id": "openai-crm-public-web-promotion",
                "candidate_name": "OpenAI CRM Public Web Promotion",
                "current_company": "OpenAI",
                "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-promotion/",
                "linkedin_url_key": "openai-crm-public-web-promotion",
                "person_identity_key": "linkedin:openai-crm-public-web-promotion",
                "status": "completed",
                "phase": "completed",
                "summary": {"email_candidate_count": 1, "owner": "crm_public_web_v1"},
                "execution_backend": "crm_public_web_v1",
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=run["run_id"],
            signals=[
                {
                    "signal_id": "crm-public-web-promotion-signal-1",
                    "run_id": run["run_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-promotion",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-promotion",
                    "candidate_name": "OpenAI CRM Public Web Promotion",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-promotion",
                    "signal_kind": "email_candidate",
                    "signal_type": "academic",
                    "email_type": "academic",
                    "value": "crm.promotion@example.edu",
                    "normalized_value": "crm.promotion@example.edu",
                    "source_url": "https://crm-promotion.example.edu/",
                    "source_domain": "crm-promotion.example.edu",
                    "source_family": "profile_web_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.94,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.92,
                    "publishable": True,
                }
            ],
        )

        promoted = self.orchestrator.promote_crm_record_public_web_signal(
            crm_record_id,
            {
                "signal_id": "crm-public-web-promotion-signal-1",
                "action": "promote",
                "operator": "qa-user",
            },
        )

        self.assertEqual(promoted["status"], "promoted")
        self.assertEqual(promoted["write_contract"]["public_web_storage_owner"], "crm_public_web_v1")
        self.assertEqual(promoted["write_contract"]["public_web_storage_bridge"], "")
        self.assertEqual(promoted["promotion"]["record_id"], crm_record_id)
        self.assertEqual(promoted["promotion"]["metadata"]["owner"], "crm_public_web_v1")
        self.assertEqual(promoted["person_assertion"]["value"], "crm.promotion@example.edu")
        self.assertIsNone(self.store.get_target_candidate(crm_record_id))
        self.assertEqual(self.store.list_target_candidate_public_web_promotions(record_id=crm_record_id), [])
        crm_promotions = self.store.list_crm_public_web_promotions(crm_record_id=crm_record_id)
        self.assertEqual(len(crm_promotions), 1)
        self.assertEqual(crm_promotions[0]["execution_backend"], "crm_public_web_v1")

    def test_crm_public_web_promotion_rejects_stale_run_signal(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-crm-public-web-stale-promotion",
            collection_id="company:openai",
            projection_id="proj_openai_crm_public_web_stale_promotion",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-crm-public-web-stale-promotion",
                    "person_identity_key": "linkedin:openai-crm-public-web-stale-promotion",
                    "candidate_id": "openai-crm-public-web-stale-promotion",
                    "public_summary": {
                        "display_name": "OpenAI CRM Public Web Stale Promotion",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-stale-promotion/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_crm_public_web_stale_promotion",
                "candidate_identity_key": "linkedin:openai-crm-public-web-stale-promotion",
                "idempotency_key": "api-add-openai-crm-public-web-stale-promotion",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]
        common_run_payload = {
            "batch_id": "crm-public-web-stale-promotion-batch",
            "crm_record_id": crm_record_id,
            "workspace_id": "default",
            "candidate_id": "openai-crm-public-web-stale-promotion",
            "candidate_name": "OpenAI CRM Public Web Stale Promotion",
            "current_company": "OpenAI",
            "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-stale-promotion/",
            "linkedin_url_key": "openai-crm-public-web-stale-promotion",
            "person_identity_key": "linkedin:openai-crm-public-web-stale-promotion",
            "status": "completed",
            "phase": "completed",
            "summary": {"owner": "crm_public_web_v1"},
            "execution_backend": "crm_public_web_v1",
        }
        old_run = self.store.upsert_crm_public_web_run(
            {
                **common_run_payload,
                "run_id": "crm-public-web-stale-promotion-run-a",
                "idempotency_key": "crm-public-web-run:stale-promotion-a",
            }
        )
        latest_run = self.store.upsert_crm_public_web_run(
            {
                **common_run_payload,
                "run_id": "crm-public-web-stale-promotion-run-z",
                "idempotency_key": "crm-public-web-run:stale-promotion-z",
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=old_run["run_id"],
            signals=[
                {
                    "signal_id": "crm-public-web-stale-promotion-signal-old",
                    "run_id": old_run["run_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-stale-promotion",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-stale-promotion",
                    "candidate_name": "OpenAI CRM Public Web Stale Promotion",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-stale-promotion",
                    "signal_kind": "profile_link",
                    "signal_type": "github_url",
                    "value": "https://github.com/openai-crm-public-web-stale-promotion",
                    "normalized_value": "https://github.com/openai-crm-public-web-stale-promotion",
                    "url": "https://github.com/openai-crm-public-web-stale-promotion",
                    "source_url": "https://github.com/openai-crm-public-web-stale-promotion",
                    "source_domain": "github.com",
                    "source_family": "technical_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.94,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.92,
                    "publishable": True,
                    "metadata": {"clean_profile_link": True},
                }
            ],
        )

        promoted = self.orchestrator.promote_crm_record_public_web_signal(
            crm_record_id,
            {
                "signal_id": "crm-public-web-stale-promotion-signal-old",
                "action": "promote",
                "operator": "qa-user",
            },
        )

        self.assertEqual(promoted["status"], "invalid")
        self.assertEqual(promoted["reason"], "public_web_signal_not_latest_run")
        self.assertEqual(promoted["signal_run_id"], old_run["run_id"])
        self.assertEqual(promoted["latest_run_id"], latest_run["run_id"])
        self.assertEqual(self.store.list_crm_public_web_promotions(crm_record_id=crm_record_id), [])
        assertions = self.store.list_person_assertions(
            person_identity_key="linkedin:openai-crm-public-web-stale-promotion"
        )
        self.assertEqual(assertions, [])

    def test_crm_public_web_promotion_rejects_signal_without_current_workspace_latest_run(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-crm-public-web-cross-workspace-promotion",
            collection_id="company:openai",
            projection_id="proj_openai_crm_public_web_cross_workspace_promotion",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-crm-public-web-cross-workspace-promotion",
                    "person_identity_key": "linkedin:openai-crm-public-web-cross-workspace-promotion",
                    "candidate_id": "openai-crm-public-web-cross-workspace-promotion",
                    "public_summary": {
                        "display_name": "OpenAI CRM Public Web Cross Workspace Promotion",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-cross-workspace-promotion/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_crm_public_web_cross_workspace_promotion",
                "candidate_identity_key": "linkedin:openai-crm-public-web-cross-workspace-promotion",
                "idempotency_key": "api-add-openai-crm-public-web-cross-workspace-promotion",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]
        other_workspace_run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-cross-workspace-promotion-run",
                "batch_id": "crm-public-web-cross-workspace-promotion-batch",
                "crm_record_id": crm_record_id,
                "workspace_id": "other_workspace",
                "candidate_id": "openai-crm-public-web-cross-workspace-promotion",
                "candidate_name": "OpenAI CRM Public Web Cross Workspace Promotion",
                "current_company": "OpenAI",
                "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-cross-workspace-promotion/",
                "linkedin_url_key": "openai-crm-public-web-cross-workspace-promotion",
                "person_identity_key": "linkedin:openai-crm-public-web-cross-workspace-promotion",
                "status": "completed",
                "phase": "completed",
                "summary": {"owner": "crm_public_web_v1"},
                "execution_backend": "crm_public_web_v1",
                "idempotency_key": "crm-public-web-run:cross-workspace-promotion",
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=other_workspace_run["run_id"],
            signals=[
                {
                    "signal_id": "crm-public-web-cross-workspace-promotion-signal",
                    "run_id": other_workspace_run["run_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-cross-workspace-promotion",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-cross-workspace-promotion",
                    "candidate_name": "OpenAI CRM Public Web Cross Workspace Promotion",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-cross-workspace-promotion",
                    "signal_kind": "profile_link",
                    "signal_type": "github_url",
                    "value": "https://github.com/openai-crm-public-web-cross-workspace-promotion",
                    "normalized_value": "https://github.com/openai-crm-public-web-cross-workspace-promotion",
                    "url": "https://github.com/openai-crm-public-web-cross-workspace-promotion",
                    "source_url": "https://github.com/openai-crm-public-web-cross-workspace-promotion",
                    "source_domain": "github.com",
                    "source_family": "technical_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.94,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.92,
                    "publishable": True,
                    "metadata": {"clean_profile_link": True},
                }
            ],
        )

        promoted = self.orchestrator.promote_crm_record_public_web_signal(
            crm_record_id,
            {
                "signal_id": "crm-public-web-cross-workspace-promotion-signal",
                "action": "promote",
                "operator": "qa-user",
            },
        )

        self.assertEqual(promoted["status"], "invalid")
        self.assertEqual(promoted["reason"], "public_web_signal_not_latest_run")
        self.assertEqual(promoted["signal_run_id"], other_workspace_run["run_id"])
        self.assertEqual(promoted["latest_run_id"], "")
        self.assertEqual(promoted["workspace_id"], "default")
        self.assertEqual(promoted["signal_workspace_id"], "other_workspace")
        self.assertEqual(self.store.list_crm_public_web_promotions(crm_record_id=crm_record_id), [])
        assertions = self.store.list_person_assertions(
            person_identity_key="linkedin:openai-crm-public-web-cross-workspace-promotion"
        )
        self.assertEqual(assertions, [])

    def test_crm_public_web_export_reads_crm_owner_storage(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-crm-public-web-export",
            collection_id="company:openai",
            projection_id="proj_openai_crm_public_web_export",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-crm-public-web-export",
                    "person_identity_key": "linkedin:openai-crm-public-web-export",
                    "candidate_id": "openai-crm-public-web-export",
                    "public_summary": {
                        "display_name": "OpenAI CRM Public Web Export",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-export/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_crm_public_web_export",
                "candidate_identity_key": "linkedin:openai-crm-public-web-export",
                "idempotency_key": "api-add-openai-crm-public-web-export",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]
        run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-export-run-1",
                "batch_id": "crm-public-web-export-batch-1",
                "crm_record_id": crm_record_id,
                "workspace_id": "default",
                "candidate_id": "openai-crm-public-web-export",
                "candidate_name": "OpenAI CRM Public Web Export",
                "current_company": "OpenAI",
                "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-export/",
                "linkedin_url_key": "openai-crm-public-web-export",
                "person_identity_key": "linkedin:openai-crm-public-web-export",
                "status": "completed",
                "phase": "completed",
                "summary": {
                    "email_candidate_count": 1,
                    "artifact_root": "/tmp/internal-crm-public-web-export-run",
                    "document_fetch_payload_path": "/tmp/document_fetch_payload.json",
                },
                "analysis_checkpoint": {"stage": "completed", "status": "completed"},
                "execution_backend": "crm_public_web_v1",
                "metadata": {"owner": "crm_public_web_v1"},
            }
        )
        asset = self.store.upsert_person_public_web_asset(
            {
                "asset_id": "crm-public-web-export-asset-1",
                "person_identity_key": "linkedin:openai-crm-public-web-export",
                "linkedin_url_key": "openai-crm-public-web-export",
                "latest_run_id": run["run_id"],
                "summary": {
                    "email_candidate_count": 1,
                    "artifact_root": "/tmp/internal-crm-public-web-export-asset",
                    "adjudication_payload_path": "/tmp/adjudication_payload.json",
                },
                "source_run_ids": [run["run_id"]],
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=run["run_id"],
            signals=[
                {
                    "signal_id": "crm-public-web-export-email-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-export",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-export",
                    "candidate_name": "OpenAI CRM Public Web Export",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-export",
                    "signal_kind": "email_candidate",
                    "signal_type": "academic",
                    "email_type": "academic",
                    "value": "crm.public.web@example.edu",
                    "normalized_value": "crm.public.web@example.edu",
                    "source_url": "https://crm-public-web.example.edu/",
                    "source_domain": "crm-public-web.example.edu",
                    "source_family": "profile_web_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.94,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.92,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                    "evidence_excerpt": "Contact at crm.public.web@example.edu",
                    "artifact_refs": {
                        "evidence_slice_path": "/tmp/evidence.json",
                        "raw_path": "/tmp/raw.html",
                    },
                    "metadata": {"raw_payload": {"html": "<html></html>"}},
                },
                {
                    "signal_id": "crm-public-web-export-github-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-export",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-export",
                    "candidate_name": "OpenAI CRM Public Web Export",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-export",
                    "signal_kind": "profile_link",
                    "signal_type": "github_url",
                    "value": "https://github.com/crm-public-web",
                    "normalized_value": "https://github.com/crm-public-web",
                    "url": "https://github.com/crm-public-web",
                    "source_url": "https://github.com/crm-public-web",
                    "source_domain": "github.com",
                    "source_family": "technical_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.9,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.88,
                    "publishable": True,
                    "metadata": {"clean_profile_link": True, "link_shape_warnings": []},
                },
                {
                    "signal_id": "crm-public-web-export-rejected-x-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-export",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-export",
                    "candidate_name": "OpenAI CRM Public Web Export",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-export",
                    "signal_kind": "profile_link",
                    "signal_type": "x_url",
                    "value": "https://x.com/rejected-crm-public-web",
                    "normalized_value": "https://x.com/rejected-crm-public-web",
                    "url": "https://x.com/rejected-crm-public-web",
                    "source_url": "https://x.com/rejected-crm-public-web",
                    "source_domain": "x.com",
                    "source_family": "social_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.87,
                    "publishable": True,
                    "metadata": {"clean_profile_link": True, "link_shape_warnings": []},
                },
                {
                    "signal_id": "crm-public-web-export-suppressed-homepage-1",
                    "run_id": run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-export",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-export",
                    "candidate_name": "OpenAI CRM Public Web Export",
                    "current_company": "OpenAI",
                    "linkedin_url_key": "openai-crm-public-web-export",
                    "signal_kind": "profile_link",
                    "signal_type": "homepage_url",
                    "value": "https://suppressed-crm-public-web.example.com",
                    "normalized_value": "https://suppressed-crm-public-web.example.com",
                    "url": "https://suppressed-crm-public-web.example.com",
                    "source_url": "https://suppressed-crm-public-web.example.com",
                    "source_domain": "suppressed-crm-public-web.example.com",
                    "source_family": "personal_site",
                    "confidence_label": "high",
                    "confidence_score": 0.9,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.88,
                    "publishable": True,
                    "suppression_reason": "manual_or_model_suppression",
                    "metadata": {"clean_profile_link": True, "link_shape_warnings": []},
                },
            ],
        )
        baseline_export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_only"}
        )
        self.assertEqual(baseline_export["status"], "ok")
        self.assertEqual(baseline_export["exported_signal_count"], 0)
        baseline_command = baseline_export["workflow_command"]
        baseline_result = dict(
            self.store.get_workflow_command(str(baseline_command.get("command_id") or ""))["result"]
        )
        baseline_watermark_hash = str(baseline_export.get("export_input_watermark_hash") or "")
        self.assertTrue(baseline_watermark_hash)
        promoted = self.store.upsert_crm_public_web_promotion(
            {
                "promotion_id": "crm-public-web-export-promotion-1",
                "signal_id": "crm-public-web-export-email-1",
                "run_id": run["run_id"],
                "asset_id": asset["asset_id"],
                "person_identity_key": "linkedin:openai-crm-public-web-export",
                "crm_record_id": crm_record_id,
                "workspace_id": "default",
                "candidate_id": "openai-crm-public-web-export",
                "candidate_name": "OpenAI CRM Public Web Export",
                "current_company": "OpenAI",
                "linkedin_url_key": "openai-crm-public-web-export",
                "signal_kind": "email_candidate",
                "signal_type": "academic",
                "email_type": "academic",
                "value": "crm.public.web@example.edu",
                "normalized_value": "crm.public.web@example.edu",
                "source_url": "https://crm-public-web.example.edu/",
                "source_domain": "crm-public-web.example.edu",
                "source_family": "profile_web_presence",
                "confidence_label": "high",
                "confidence_score": 0.94,
                "identity_match_label": "likely_same_person",
                "identity_match_score": 0.92,
                "publishable": True,
                "action": "promote",
                "promotion_status": "manually_promoted",
                "promoted_field": "primary_email",
                "new_value": "crm.public.web@example.edu",
                "operator": "qa-user",
                "note": "CRM owner promotion",
                "execution_backend": "crm_public_web_v1",
                "metadata": {"owner": "crm_public_web_v1"},
            }
        )
        self.store.upsert_crm_public_web_promotion(
            {
                "promotion_id": "crm-public-web-export-rejection-1",
                "signal_id": "crm-public-web-export-rejected-x-1",
                "run_id": run["run_id"],
                "asset_id": asset["asset_id"],
                "person_identity_key": "linkedin:openai-crm-public-web-export",
                "crm_record_id": crm_record_id,
                "workspace_id": "default",
                "candidate_id": "openai-crm-public-web-export",
                "candidate_name": "OpenAI CRM Public Web Export",
                "current_company": "OpenAI",
                "linkedin_url_key": "openai-crm-public-web-export",
                "signal_kind": "profile_link",
                "signal_type": "x_url",
                "value": "https://x.com/rejected-crm-public-web",
                "normalized_value": "https://x.com/rejected-crm-public-web",
                "url": "https://x.com/rejected-crm-public-web",
                "source_url": "https://x.com/rejected-crm-public-web",
                "source_domain": "x.com",
                "source_family": "social_presence",
                "confidence_label": "high",
                "confidence_score": 0.91,
                "identity_match_label": "likely_same_person",
                "identity_match_score": 0.87,
                "publishable": True,
                "action": "reject",
                "promotion_status": "manually_rejected",
                "promoted_field": "",
                "new_value": "",
                "operator": "qa-user",
                "note": "Rejected by operator",
                "execution_backend": "crm_public_web_v1",
                "metadata": {"owner": "crm_public_web_v1"},
            }
        )

        detail = self.orchestrator.get_crm_record_public_web_search_detail(crm_record_id)
        self.assertEqual(detail["status"], "ok")
        self.assertEqual(detail["public_web_storage_owner"], "crm_public_web_v1")
        self.assertEqual(detail["latest_run"]["run_id"], run["run_id"])
        self.assertEqual(detail["email_candidates"][0]["promotion_status"], "manually_promoted")
        self.assertEqual(promoted["record_id"], crm_record_id)

        promoted_only_export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_only"}
        )
        self.assertEqual(promoted_only_export["status"], "ok")
        self.assertEqual(promoted_only_export["exported_signal_count"], 1)
        self.assertNotEqual(
            baseline_command["command_id"],
            promoted_only_export["workflow_command"]["command_id"],
        )
        self.assertNotEqual(
            baseline_result["artifact_path"],
            dict(
                self.store.get_workflow_command(
                    str(promoted_only_export["workflow_command"].get("command_id") or "")
                )["result"]
            )["artifact_path"],
        )
        self.assertNotEqual(baseline_watermark_hash, promoted_only_export["export_input_watermark_hash"])
        with zipfile.ZipFile(BytesIO(promoted_only_export["body"]), "r") as archive:
            promoted_only_text = "\n".join(
                archive.read(name).decode("utf-8", errors="ignore")
                for name in archive.namelist()
                if name.endswith((".csv", ".json"))
            )
        self.assertIn("crm.public.web@example.edu", promoted_only_text)
        self.assertIn("CRM owner promotion", promoted_only_text)

        export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_and_publishable"}
        )

        self.assertEqual(export["status"], "ok")
        self.assertEqual(export["public_web_storage_owner"], "crm_public_web_v1")
        self.assertEqual(export["record_count"], 1)
        self.assertEqual(export["exported_record_count"], 1)
        self.assertEqual(export["exported_signal_count"], 2)
        self.assertEqual(export["workflow_command"]["command_type"], EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE)
        self.assertEqual(export["workflow_command"]["owner"], EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER)
        self.assertEqual(export["workflow_command"]["status"], "succeeded")
        crm_export_commands = [
            command
            for command in self.store.list_workflow_commands(
                owner=EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
                statuses=["succeeded"],
                limit=10,
            )
            if command["command_type"] == EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE
        ]
        self.assertEqual(len(crm_export_commands), 3)
        publishable_command = next(
            command
            for command in crm_export_commands
            if command["command_id"] == export["workflow_command"]["command_id"]
        )
        self.assertEqual(publishable_command["stage_id"], "crm_public_web_export")
        self.assertEqual(publishable_command["readiness_effect"], "crm_public_web_export_generated")
        self.assertEqual(publishable_command["produced_entity_counts"]["crm_record"], 1)
        self.assertTrue(str(dict(publishable_command["result"]).get("artifact_path") or "").endswith(".zip"))
        self.assertEqual(
            dict(publishable_command["payload"])["export_input_watermark_hash"],
            dict(publishable_command["result"])["export_input_watermark_hash"],
        )
        with zipfile.ZipFile(BytesIO(export["body"]), "r") as archive:
            names = set(archive.namelist())
            summary_name = next(name for name in names if name.endswith("/public_web_summary.csv"))
            signals_name = next(name for name in names if name.endswith("/public_web_signals.csv"))
            promotions_name = next(name for name in names if name.endswith("/public_web_promotions.csv"))
            manifest_name = next(name for name in names if name.endswith("/public_web_manifest.json"))
            summary_csv = archive.read(summary_name).decode("utf-8-sig")
            signals_csv = archive.read(signals_name).decode("utf-8-sig")
            promotions_csv = archive.read(promotions_name).decode("utf-8-sig")
            manifest = json.loads(archive.read(manifest_name).decode("utf-8"))
            archive_text = "\n".join(
                archive.read(name).decode("utf-8", errors="ignore")
                for name in names
                if name.endswith((".csv", ".json"))
            )
        self.assertEqual(manifest["public_web_storage_owner"], "crm_public_web_v1")
        self.assertEqual(manifest["records"][0]["crm_record_id"], crm_record_id)
        self.assertIn("crm.public.web@example.edu", summary_csv)
        self.assertIn("https://github.com/crm-public-web", summary_csv)
        self.assertNotIn("https://x.com/rejected-crm-public-web", archive_text)
        self.assertNotIn("Rejected by operator", archive_text)
        self.assertNotIn("https://suppressed-crm-public-web.example.com", archive_text)
        self.assertNotIn("manual_or_model_suppression", archive_text)
        self.assertIn("manual_promoted", signals_csv)
        self.assertIn("ai_publishable_unpromoted", signals_csv)
        self.assertIn("CRM owner promotion", promotions_csv)
        self.assertNotIn("raw_path", archive_text)
        self.assertNotIn("raw_payload", archive_text)
        self.assertNotIn("artifact_root", archive_text)
        self.assertNotIn("document_fetch_payload_path", archive_text)
        self.assertNotIn("adjudication_payload_path", archive_text)
        self.assertNotIn("<html", archive_text)

        asset_changed = self.store.upsert_person_public_web_asset(
            {
                "asset_id": asset["asset_id"],
                "person_identity_key": "linkedin:openai-crm-public-web-export",
                "linkedin_url_key": "openai-crm-public-web-export",
                "latest_run_id": run["run_id"],
                "summary": {
                    "email_candidate_count": 1,
                    "asset_watermark_marker": "asset-summary-v2",
                    "artifact_root": "/tmp/internal-crm-public-web-export-asset-v2",
                },
                "source_run_ids": [run["run_id"], "crm-public-web-export-run-asset-v2"],
            }
        )
        self.assertEqual(asset_changed["asset_id"], asset["asset_id"])
        asset_changed_export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_and_publishable"}
        )
        self.assertEqual(asset_changed_export["status"], "ok")
        self.assertNotEqual(export["workflow_command"]["command_id"], asset_changed_export["workflow_command"]["command_id"])
        self.assertNotEqual(export["export_input_watermark_hash"], asset_changed_export["export_input_watermark_hash"])
        with zipfile.ZipFile(BytesIO(asset_changed_export["body"]), "r") as archive:
            asset_changed_text = "\n".join(
                archive.read(name).decode("utf-8", errors="ignore")
                for name in archive.namelist()
                if name.endswith((".csv", ".json"))
            )
        self.assertIn("asset-summary-v2", asset_changed_text)
        self.assertNotIn("artifact_root", asset_changed_text)

        phase_command = self.orchestrator._plan_crm_public_web_run_phase_command(  # noqa: SLF001
            workflow_run_id=self.orchestrator._crm_public_web_workflow_run_id(run["batch_id"]),  # noqa: SLF001
            operation_id=self.orchestrator._crm_public_web_operation_id(run["batch_id"]),  # noqa: SLF001
            batch_id=run["batch_id"],
            run_id=run["run_id"],
            workspace_id="default",
            command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
            request_payload={"reason": "phase watermark regression"},
            source="crm_public_web_export_watermark_regression",
        )
        self.assertEqual(phase_command["status"], "queued")
        phase_planned_export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_and_publishable"}
        )
        self.assertEqual(phase_planned_export["status"], "ok")
        self.assertNotEqual(
            asset_changed_export["export_input_watermark_hash"],
            phase_planned_export["export_input_watermark_hash"],
        )
        self.store.cancel_workflow_command(
            phase_command["command_id"],
            reason="phase-command-cancelled-v2",
        )
        phase_changed_export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_and_publishable"}
        )
        self.assertEqual(phase_changed_export["status"], "ok")
        self.assertNotEqual(
            phase_planned_export["workflow_command"]["command_id"],
            phase_changed_export["workflow_command"]["command_id"],
        )
        self.assertNotEqual(
            phase_planned_export["export_input_watermark_hash"],
            phase_changed_export["export_input_watermark_hash"],
        )

    def test_crm_public_web_detail_and_export_do_not_fallback_to_cross_record_person_asset_run(self) -> None:
        linkedin_url = "https://www.linkedin.com/in/crm-public-web-cross-record-asset/"
        linkedin_url_key = "crm-public-web-cross-record-asset"
        other_record = self.store.upsert_crm_record(
            {
                "crm_record_id": "crmrec-public-web-cross-record-owner",
                "workspace_id": "default",
                "person_identity_key": "linkedin:public-web-cross-record-owner",
                "candidate_identity_key": "linkedin:public-web-cross-record-owner",
                "display_name_cache": "Public Web Cross Record Owner",
                "primary_company_cache": "Anthropic",
                "metadata": {
                    "candidate_id": "public-web-cross-record-owner",
                    "linkedin_url_cache": linkedin_url,
                },
            }
        )
        no_run_record = self.store.upsert_crm_record(
            {
                "crm_record_id": "crmrec-public-web-cross-record-no-run",
                "workspace_id": "default",
                "person_identity_key": "linkedin:public-web-cross-record-no-run",
                "candidate_identity_key": "linkedin:public-web-cross-record-no-run",
                "display_name_cache": "Public Web Cross Record No Run",
                "primary_company_cache": "Anthropic",
                "metadata": {
                    "candidate_id": "public-web-cross-record-no-run",
                    "linkedin_url_cache": linkedin_url,
                },
            }
        )
        other_run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-cross-record-owner-run",
                "batch_id": "crm-public-web-cross-record-owner-batch",
                "crm_record_id": other_record["crm_record_id"],
                "workspace_id": "default",
                "candidate_id": "public-web-cross-record-owner",
                "candidate_name": "Public Web Cross Record Owner",
                "current_company": "Anthropic",
                "linkedin_url": linkedin_url,
                "linkedin_url_key": linkedin_url_key,
                "person_identity_key": "linkedin:public-web-cross-record-owner",
                "status": "completed",
                "phase": "completed",
                "execution_backend": "crm_public_web_v1",
                "metadata": {"owner": "crm_public_web_v1"},
            }
        )
        asset = self.store.upsert_person_public_web_asset(
            {
                "asset_id": "person-public-web-cross-record-owner-asset",
                "person_identity_key": "linkedin:public-web-cross-record-owner",
                "linkedin_url_key": linkedin_url_key,
                "latest_run_id": other_run["run_id"],
                "target_candidate_record_id": other_record["crm_record_id"],
                "summary": {"source": "owner_record_public_web_run"},
                "source_run_ids": [other_run["run_id"]],
            }
        )
        leaked_url = "https://github.com/cross-record-public-web-owner"
        self.store.replace_person_public_web_signals_for_run(
            run_id=other_run["run_id"],
            signals=[
                {
                    "signal_id": "crm-public-web-cross-record-owner-github",
                    "run_id": other_run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:public-web-cross-record-owner",
                    "record_id": other_record["crm_record_id"],
                    "candidate_id": "public-web-cross-record-owner",
                    "candidate_name": "Public Web Cross Record Owner",
                    "current_company": "Anthropic",
                    "linkedin_url_key": linkedin_url_key,
                    "signal_kind": "profile_link",
                    "signal_type": "github_url",
                    "value": leaked_url,
                    "normalized_value": leaked_url,
                    "url": leaked_url,
                    "source_url": leaked_url,
                    "source_domain": "github.com",
                    "source_family": "technical_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.92,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.9,
                    "publishable": True,
                    "metadata": {"clean_profile_link": True},
                }
            ],
        )

        owner_detail = self.orchestrator.get_crm_record_public_web_search_detail(other_record["crm_record_id"])
        self.assertEqual(owner_detail["status"], "ok")
        self.assertEqual(owner_detail["latest_run"]["run_id"], other_run["run_id"])
        self.assertEqual(owner_detail["profile_links"][0]["source_url"], leaked_url)

        no_run_detail = self.orchestrator.get_crm_record_public_web_search_detail(no_run_record["crm_record_id"])
        self.assertEqual(no_run_detail["status"], "not_found")
        self.assertEqual(no_run_detail["reason"], "public_web_search_not_found")
        self.assertFalse(no_run_detail["read_contract"]["fallback_used"])

        export = _drive_crm_export(self.orchestrator, 
            {
                "workspace_id": "default",
                "crm_record_ids": [no_run_record["crm_record_id"]],
                "mode": "promoted_and_publishable",
            }
        )
        self.assertEqual(export["status"], "ok")
        self.assertEqual(export["record_count"], 1)
        self.assertEqual(export["exported_record_count"], 0)
        self.assertEqual(export["exported_signal_count"], 0)
        self.assertEqual(export["no_public_web_result_count"], 1)
        with zipfile.ZipFile(BytesIO(export["body"]), "r") as archive:
            archive_text = "\n".join(
                archive.read(name).decode("utf-8", errors="ignore")
                for name in archive.namelist()
                if name.endswith((".csv", ".json"))
            )
        self.assertIn("no_public_web_result", archive_text)
        self.assertNotIn(leaked_url, archive_text)
        self.assertNotIn(other_run["run_id"], archive_text)

    def test_crm_public_web_export_rejects_stale_queued_command_before_artifact_publish(self) -> None:
        crm_record = self.store.upsert_crm_record(
            {
                "crm_record_id": "crmrec-public-web-export-stale-command",
                "workspace_id": "default",
                "person_identity_key": "linkedin:public-web-export-stale-command",
                "candidate_identity_key": "linkedin:public-web-export-stale-command",
                "display_name_cache": "Public Web Export Stale Command",
                "primary_company_cache": "OpenAI",
            }
        )
        run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-export-stale-command-run",
                "batch_id": "crm-public-web-export-stale-command-batch",
                "crm_record_id": crm_record["crm_record_id"],
                "workspace_id": "default",
                "candidate_id": "public-web-export-stale-command",
                "candidate_name": "Public Web Export Stale Command",
                "current_company": "OpenAI",
                "linkedin_url_key": "public-web-export-stale-command",
                "person_identity_key": "linkedin:public-web-export-stale-command",
                "status": "completed",
                "phase": "completed",
                "execution_backend": "crm_public_web_v1",
                "metadata": {"owner": "crm_public_web_v1"},
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=run["run_id"],
            signals=[
                {
                    "signal_id": "crm-public-web-export-stale-command-email",
                    "run_id": run["run_id"],
                    "person_identity_key": "linkedin:public-web-export-stale-command",
                    "record_id": crm_record["crm_record_id"],
                    "candidate_id": "public-web-export-stale-command",
                    "candidate_name": "Public Web Export Stale Command",
                    "linkedin_url_key": "public-web-export-stale-command",
                    "signal_kind": "email_candidate",
                    "signal_type": "academic",
                    "email_type": "academic",
                    "value": "stale.command@example.edu",
                    "normalized_value": "stale.command@example.edu",
                    "source_url": "https://stale-command.example.edu/",
                    "source_domain": "stale-command.example.edu",
                    "source_family": "profile_web_presence",
                    "confidence_label": "high",
                    "confidence_score": 0.9,
                    "identity_match_label": "likely_same_person",
                    "identity_match_score": 0.9,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                }
            ],
        )
        command = self.orchestrator._plan_crm_public_web_export_generate_command(  # noqa: SLF001
            {"crm_record_ids": [crm_record["crm_record_id"]], "mode": "promoted_only"},
            workspace_id="default",
        )
        self.assertEqual(command["status"], "queued")
        command_watermark_hash = dict(command["payload"])["export_input_watermark_hash"]

        self.store.upsert_crm_public_web_promotion(
            {
                "promotion_id": "crm-public-web-export-stale-command-promotion",
                "signal_id": "crm-public-web-export-stale-command-email",
                "run_id": run["run_id"],
                "person_identity_key": "linkedin:public-web-export-stale-command",
                "crm_record_id": crm_record["crm_record_id"],
                "workspace_id": "default",
                "candidate_id": "public-web-export-stale-command",
                "candidate_name": "Public Web Export Stale Command",
                "linkedin_url_key": "public-web-export-stale-command",
                "signal_kind": "email_candidate",
                "signal_type": "academic",
                "email_type": "academic",
                "value": "stale.command@example.edu",
                "normalized_value": "stale.command@example.edu",
                "source_url": "https://stale-command.example.edu/",
                "source_domain": "stale-command.example.edu",
                "source_family": "profile_web_presence",
                "action": "promote",
                "promotion_status": "manually_promoted",
                "promoted_field": "primary_email",
                "new_value": "stale.command@example.edu",
                "operator": "qa-user",
                "execution_backend": "crm_public_web_v1",
                "metadata": {"owner": "crm_public_web_v1"},
            }
        )

        stale_result = self.orchestrator._run_crm_public_web_export_generate_command(command)  # noqa: SLF001

        self.assertEqual(stale_result["status"], "failed")
        self.assertEqual(stale_result["reason"], "crm_public_web_export_input_watermark_stale")
        self.assertEqual(stale_result["export_input_watermark_hash"], command_watermark_hash)
        self.assertNotEqual(stale_result["current_export_input_watermark_hash"], command_watermark_hash)
        self.assertNotIn("body", stale_result)
        stored_command = self.store.get_workflow_command(command["command_id"])
        self.assertEqual(stored_command["status"], "failed_terminal")
        self.assertNotIn("artifact_path", dict(stored_command.get("result") or {}))

    def test_crm_public_web_export_api_fails_closed_for_owner_failure(self) -> None:
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            with mock.patch.object(
                self.orchestrator,
                "export_crm_record_public_web_archive",
                return_value={
                    "status": "failed",
                    "reason": "crm_public_web_export_input_watermark_stale",
                    "public_web_storage_owner": "crm_public_web_v1",
                    "read_contract": {"fail_closed": True},
                },
            ):
                payload, status = _post_json_with_status(
                    f"{base_url}/api/crm/records/public-web-export",
                    {"crm_record_ids": ["crmrec-stale-export"], "mode": "promoted_only"},
                )
        finally:
            server.shutdown()
            thread.join(timeout=2)

        self.assertEqual(status, 409)
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["reason"], "crm_public_web_export_input_watermark_stale")
        self.assertTrue(payload["read_contract"]["fail_closed"])

    def test_crm_public_web_reads_fail_closed_without_owner_projection(self) -> None:
        self.projection_writer.publish_run_scope_projection(
            run_id="job-openai-crm-public-web-fail-closed",
            collection_id="company:openai",
            projection_id="proj_openai_crm_public_web_fail_closed",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-crm-public-web-fail-closed",
                    "person_identity_key": "linkedin:openai-crm-public-web-fail-closed",
                    "candidate_id": "openai-crm-public-web-fail-closed",
                    "public_summary": {
                        "display_name": "OpenAI CRM Public Web Fail Closed",
                        "headline": "Research Engineer",
                        "current_company": "OpenAI",
                        "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-fail-closed/",
                    },
                }
            ],
            replace_members=True,
        )
        add_result = self.orchestrator.add_projection_candidate_to_crm(
            {
                "projection_id": "proj_openai_crm_public_web_fail_closed",
                "candidate_identity_key": "linkedin:openai-crm-public-web-fail-closed",
                "idempotency_key": "api-add-openai-crm-public-web-fail-closed",
            }
        )
        crm_record_id = add_result["crm_record"]["crm_record_id"]
        legacy_run = seed_legacy_target_public_web_run(
            self.store,
            {
                "run_id": "legacy-public-web-only-run-for-crm-fail-closed",
                "batch_id": "legacy-public-web-only-batch-for-crm-fail-closed",
                "record_id": crm_record_id,
                "candidate_id": "openai-crm-public-web-fail-closed",
                "candidate_name": "OpenAI CRM Public Web Fail Closed",
                "current_company": "OpenAI",
                "linkedin_url": "https://www.linkedin.com/in/openai-crm-public-web-fail-closed/",
                "linkedin_url_key": "openai-crm-public-web-fail-closed",
                "person_identity_key": "linkedin:openai-crm-public-web-fail-closed",
                "status": "completed",
                "phase": "completed",
                "summary": {"email_candidate_count": 1},
            }
        )
        asset = self.store.upsert_person_public_web_asset(
            {
                "asset_id": "legacy-public-web-only-asset-for-crm-fail-closed",
                "person_identity_key": "linkedin:openai-crm-public-web-fail-closed",
                "linkedin_url_key": "openai-crm-public-web-fail-closed",
                "latest_run_id": legacy_run["run_id"],
                "source_run_ids": [legacy_run["run_id"]],
            }
        )
        self.store.replace_person_public_web_signals_for_run(
            run_id=legacy_run["run_id"],
            signals=[
                {
                    "signal_id": "legacy-public-web-only-signal-for-crm-fail-closed",
                    "run_id": legacy_run["run_id"],
                    "asset_id": asset["asset_id"],
                    "person_identity_key": "linkedin:openai-crm-public-web-fail-closed",
                    "record_id": crm_record_id,
                    "candidate_id": "openai-crm-public-web-fail-closed",
                    "candidate_name": "OpenAI CRM Public Web Fail Closed",
                    "linkedin_url_key": "openai-crm-public-web-fail-closed",
                    "signal_kind": "email_candidate",
                    "signal_type": "academic",
                    "email_type": "academic",
                    "value": "legacy.only@example.edu",
                    "normalized_value": "legacy.only@example.edu",
                    "source_url": "https://legacy-only.example.edu/",
                    "source_family": "profile_web_presence",
                    "publishable": True,
                }
            ],
        )

        detail = self.orchestrator.get_crm_record_public_web_search_detail(crm_record_id)
        export = _drive_crm_export(self.orchestrator, 
            {"workspace_id": "default", "crm_record_ids": [crm_record_id], "mode": "promoted_and_publishable"}
        )

        self.assertEqual(detail["status"], "not_found")
        self.assertEqual(detail["reason"], "public_web_search_not_found")
        self.assertEqual(detail["read_contract"]["fallback_used"], False)
        self.assertEqual(export["status"], "ok")
        self.assertEqual(export["exported_record_count"], 0)
        self.assertEqual(export["no_public_web_result_count"], 1)
        self.assertIsNone(self.store.get_crm_public_web_run(run_id=legacy_run["run_id"]))
        self.assertIsNone(self.store.get_target_candidate(crm_record_id))

    def test_collection_company_media_uses_company_asset_logo_when_available(self) -> None:
        self.projection_writer.publish_collection_authoritative_projection(
            collection_id="company:openai",
            active_collection_version="v1",
            projection_id="proj_openai_logo_authoritative",
            members=[
                {
                    "candidate_identity_key": "linkedin:openai-logo-person",
                    "person_identity_key": "linkedin:openai-logo-person",
                    "candidate_id": "openai-logo-person",
                    "public_summary": {"display_name": "OpenAI Logo Person"},
                }
            ],
            replace_members=True,
        )
        self.company_asset_writer.record_asset(
            {
                "asset_id": "ca_openai_logo_api",
                "company_key": "openai",
                "target_company": "OpenAI",
                "asset_type": "logo_media",
                "source_kind": "operator_seed",
                "content_ref": "https://static.example.com/openai-logo.png",
                "visibility_scope": "public_summary",
            }
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            payload = _get_json(f"{base_url}/api/collections")
            media = payload["collections"][0]["company_media"]

            self.assertEqual(media["logo_status"], "available")
            self.assertEqual(media["logo_asset_id"], "ca_openai_logo_api")
            self.assertEqual(media["logo_url"], "https://static.example.com/openai-logo.png")
            self.assertFalse(media["media_contract"]["fallback_used"])
            self.assertEqual(media["media_contract"]["source"], "CompanyAsset.logo_media")
        finally:
            server.shutdown()
            thread.join(timeout=2)

    def test_company_asset_canonical_api_is_pg_only_and_fail_closed(self) -> None:
        self.company_asset_writer.record_asset(
            {
                "asset_id": "ca_openai_research_api",
                "company_key": "openai",
                "target_company": "OpenAI",
                "asset_type": "company_research",
                "source_kind": "company_public_web_model_safe",
                "content_ref": "https://openai.com/research",
                "source_url": "https://openai.com/research",
                "visibility_scope": "public_summary",
            }
        )
        self.company_asset_writer.record_evidence(
            {
                "evidence_id": "ce_openai_research_api",
                "company_key": "openai",
                "target_company": "OpenAI",
                "asset_id": "ca_openai_research_api",
                "evidence_type": "company_public_web_asset",
                "value": "Research updates from OpenAI.",
                "source_url": "https://openai.com/research",
                "source_domain": "openai.com",
            }
        )
        self.company_asset_writer.record_assertion(
            {
                "assertion_id": "cass_openai_homepage_api",
                "company_key": "openai",
                "target_company": "OpenAI",
                "assertion_type": "official_homepage_url",
                "value": "https://openai.com/",
                "verification_status": "active",
                "authority": "operator_confirmed",
            }
        )
        server = create_server(self.orchestrator, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"
        try:
            assets_payload = _get_json(f"{base_url}/api/company-assets?company_key=openai")
            evidence_payload = _get_json(f"{base_url}/api/company-assets/evidence?company_key=openai")
            assertions_payload = _get_json(f"{base_url}/api/company-assets/assertions?company_key=openai")

            self.assertEqual(assets_payload["status"], "ready")
            self.assertEqual(assets_payload["asset_count"], 1)
            self.assertEqual(assets_payload["assets"][0]["asset_id"], "ca_openai_research_api")
            self.assertFalse(assets_payload["read_contract"]["fallback_used"])
            self.assertFalse(assets_payload["read_contract"]["sqlite_fallback_allowed"])
            self.assertEqual(evidence_payload["evidence_count"], 1)
            self.assertEqual(evidence_payload["evidence"][0]["asset_id"], "ca_openai_research_api")
            self.assertEqual(assertions_payload["assertion_count"], 1)
            self.assertEqual(assertions_payload["assertions"][0]["assertion_type"], "official_homepage_url")
        finally:
            server.shutdown()
            thread.join(timeout=2)


_LOCAL_API_OPENER = urllib_request.build_opener(urllib_request.ProxyHandler({}))


def _urlopen_local_api(request: str | urllib_request.Request):
    return _LOCAL_API_OPENER.open(request, timeout=10)


def _get_json(url: str) -> dict:
    with _urlopen_local_api(url) as response:
        return json.loads(response.read().decode("utf-8"))


def _get_json_with_status(url: str) -> tuple[dict, int]:
    try:
        with _urlopen_local_api(url) as response:
            return json.loads(response.read().decode("utf-8")), int(response.status)
    except urllib_request.HTTPError as error:
        return json.loads(error.read().decode("utf-8")), int(error.code)


def _post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    request = urllib_request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with _urlopen_local_api(request) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_json_with_status(url: str, payload: dict) -> tuple[dict, int]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib_request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with _urlopen_local_api(request) as response:
            return json.loads(response.read().decode("utf-8")), int(response.status)
    except urllib_request.HTTPError as error:
        return json.loads(error.read().decode("utf-8")), int(error.code)


def _patch_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    request = urllib_request.Request(
        url,
        data=data,
        method="PATCH",
        headers={"Content-Type": "application/json"},
    )
    with _urlopen_local_api(request) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_binary(url: str, payload: dict) -> tuple[bytes, dict[str, str]]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib_request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with _urlopen_local_api(request) as response:
        return response.read(), dict(response.headers.items())


def _get_binary(url: str) -> tuple[bytes, dict[str, str]]:
    with _urlopen_local_api(url) as response:
        return response.read(), dict(response.headers.items())


def _drive_crm_export(orchestrator, payload: dict) -> dict:
    """C1.4: drive the now-async CRM public-web export end-to-end for tests —
    submit (202/queued, or idempotent succeeded) -> worker CRM export drain ->
    read the succeeded artifact result (status ok + body + counts + workflow_command,
    the same shape the old synchronous export returned). Submit-time failures
    (validation / fail-closed pre-flight) are returned as-is; a command that fails
    in the drain is returned as the poll envelope (status 'failed' + error)."""
    submitted = orchestrator.export_crm_record_public_web_archive(payload)
    status = str(submitted.get("status") or "").strip()
    if status not in {"queued", "succeeded"}:
        return submitted
    task_id = str(submitted.get("task_id") or "")
    orchestrator._drain_export_crm_public_web_generate_commands({})
    poll = orchestrator.get_export_command_status(task_id)
    if str(poll.get("status") or "") == "succeeded":
        return orchestrator.get_export_command_artifact(task_id)
    return poll


if __name__ == "__main__":
    unittest.main()
