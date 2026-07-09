import os
import base64
import json
import threading
import tempfile
import unittest
from pathlib import Path
from urllib import request as urllib_request
from unittest import mock

from sourcing_agent.acquisition import AcquisitionEngine
from sourcing_agent.api import create_server
from sourcing_agent.asset_catalog import AssetCatalog
from sourcing_agent.company_asset_writer import CompanyAssetWriter
from sourcing_agent.crm_public_web_runtime import start_crm_public_web_batch
from sourcing_agent.model_provider import DeterministicModelClient
from sourcing_agent.operation_runtime import (
    ACTION_ADD_TO_CRM,
    ACTION_ADD_CRM_NOTE,
    ACTION_CONTINUE_ACQUISITION_RUN,
    ACTION_CREATE_CRM_TASK,
    ACTION_FETCH_PROFILE_SAMPLE,
    ACTION_REFRESH_COMPANY_PUBLIC_WEB,
    ACTION_SET_CRM_STAGE,
    ACTION_START_ACQUISITION_RUN,
    ACTION_ENRICH_PERSON_PUBLIC_WEB,
    ACTION_EXPORT_CANDIDATES,
    ACTION_FILTER_PROJECTION,
    ActionRegistry,
    ActionSpec,
    DEFAULT_ACTION_REGISTRY,
    OperationRuntimeWriter,
    operation_run_control_state,
)
from sourcing_agent.durable_runtime import (
    ACTIVITY_SPINE_LEGACY_INTERNAL,
    ACTIVITY_SPINE_REQUIRED,
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
    ACQUISITION_INTENT_RESOLVE_OWNER,
    ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
    ACQUISITION_PLAN_COMMIT_OWNER,
    ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
    ACQUISITION_PLAN_BUILD_OWNER,
    ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
    ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
    ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
    ACQUISITION_PROBE_OWNER,
    ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_OWNER,
    COMPANY_ASSET_OWNER,
    COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_OWNER,
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    COLLECTION_AUTHORITATIVE_MERGE_OWNER,
    CRM_NOTE_ADD_COMMAND_TYPE,
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
    CRM_RECORD_UPDATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
    CRM_TASK_CREATE_COMMAND_TYPE,
    CRM_WRITER_OWNER,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXCEL_INTAKE_RUN_OWNER,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_OWNER,
    MEDIA_ASSET_CACHE_COMMAND_TYPE,
    MEDIA_ASSET_OWNER,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_OWNER,
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
    PROJECTION_FACET_LAYERING_BUILD_OWNER,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
    SNAPSHOT_COMPACTION_RUN_OWNER,
    workflow_command_activity_spine_policy,
)
from sourcing_agent.domain import JobRequest
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
from tests.pg_durable_runtime import PGDurableRuntimeTestMixin


class OperationRuntimeTest(PGDurableRuntimeTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.runtime_dir = Path(self.tempdir.name)
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")
        self.writer = OperationRuntimeWriter(self.store)

    def tearDown(self) -> None:
        self._stop_pg_durable_runtime()
        self.tempdir.cleanup()

    def test_operation_run_control_state_is_backend_owned(self) -> None:
        queued = operation_run_control_state(
            operation_status="queued",
            action_status="queued",
            operation_phase="queued",
        ).to_record()
        self.assertEqual(
            queued["control_source_of_truth"],
            "operation_runtime.operation_run_control_state",
        )
        self.assertEqual(queued["fallback_status"], "fail_closed")
        self.assertTrue(queued["can_dispatch"])
        self.assertTrue(queued["can_resume"])
        self.assertTrue(queued["can_cancel"])
        self.assertFalse(queued["can_retry"])
        self.assertEqual(set(queued["allowed_actions"]), {"dispatch", "resume", "cancel"})

        failed = operation_run_control_state(
            operation_status="failed",
            action_status="queued",
            operation_phase="failed",
        ).to_record()
        self.assertTrue(failed["can_retry"])
        self.assertFalse(failed["can_dispatch"])
        self.assertEqual(failed["disabled_reasons"]["dispatch"], "dispatch_requires_queued_operation")

        cancelled_action = operation_run_control_state(
            operation_status="cancelled",
            action_status="cancelled",
            operation_phase="cancelled",
        ).to_record()
        self.assertFalse(cancelled_action["can_retry"])
        self.assertEqual(cancelled_action["disabled_reasons"]["retry"], "linked_action_terminal")

    def test_action_registry_unknown_action_fails_closed(self) -> None:
        with self.assertRaisesRegex(KeyError, "unknown operation action type"):
            DEFAULT_ACTION_REGISTRY.spec_for("write_projection_directly")

        with self.assertRaisesRegex(KeyError, "unknown operation action type"):
            self.writer.submit_action(
                action_type="write_projection_directly",
                target_ref={"projection_id": "proj-a"},
                idempotency_key="unknown:proj-a",
            )

    def test_action_registry_rejects_unregistered_workflow_commands(self) -> None:
        with self.assertRaisesRegex(ValueError, "unregistered workflow command type"):
            ActionRegistry(
                {
                    "unknown_command": ActionSpec(
                        action_type="unknown_command",
                        owner_module="unknown_owner",
                        operation_type="unknown_operation",
                        allowed_workflow_command_types=("projection.write_directly",),
                        default_workflow_command_type="projection.write_directly",
                    )
                }
            )

        with self.assertRaisesRegex(ValueError, "default workflow command type must be in allowed_workflow_command_types"):
            ActionRegistry(
                {
                    "mismatched_default": ActionSpec(
                        action_type="mismatched_default",
                        owner_module="crm_writer",
                        operation_type="crm_update",
                        allowed_workflow_command_types=(CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,),
                        default_workflow_command_type=CRM_RECORD_UPDATE_COMMAND_TYPE,
                    )
                }
            )

        with self.assertRaisesRegex(ValueError, "requires display_label"):
            ActionRegistry(
                {
                    "missing_display": ActionSpec(
                        action_type="missing_display",
                        owner_module="crm_writer",
                        operation_type="crm_update",
                        description="Valid command surface with incomplete display contract.",
                        display_category="crm",
                        allowed_workflow_command_types=(CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,),
                        default_workflow_command_type=CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
                    )
                }
            )

    def test_action_registry_exposes_workflow_command_contract_for_agent_callable_actions(self) -> None:
        registry = DEFAULT_ACTION_REGISTRY.to_record()

        self.assertEqual(
            registry[ACTION_START_ACQUISITION_RUN]["default_workflow_command_type"],
            ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        )
        self.assertNotIn(
            LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            registry[ACTION_START_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_type"],
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["command_type"],
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_exposure_gate"],
            "operation_runtime.ActionRegistry.allowed_workflow_command_types",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_exposure_status"],
            "action_registry_allowlisted",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["agent_exposure_gate"],
            "operation_runtime.ActionRegistry.allowed_workflow_command_types",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["agent_exposure_status"],
            "action_registry_allowlisted",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["control_policy"][
                "fallback_status"
            ],
            "fail_closed",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["control_policy"][
                "running_control_category"
            ],
            "domain_mutation",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["control_policy"][
                "running_control_categories"
            ],
            ["domain_mutation"],
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_control_summary"]["source_of_truth"],
            "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_control_summary"][
                "running_control_maturity_counts"
            ],
            {"owner_specific_cancel_resume": 1},
        )
        self.assertFalse(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_control_summary"][
                "has_fail_closed_running_controls"
            ]
        )
        self.assertTrue(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_control_summary"][
                "has_owner_specific_running_controls"
            ]
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["workflow_command_control_summary"]["agent_ui_guidance"],
            "owner_specific_command_controls_available",
        )
        self.assertEqual(
            registry[ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"]["activity_spine_policy"][
                "requirement"
            ],
            ACTIVITY_SPINE_REQUIRED,
        )
        self.assertNotIn(
            LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
            registry[ACTION_FETCH_PROFILE_SAMPLE]["allowed_workflow_command_types"],
        )
        self.assertIn(
            LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertIn(
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertIn(
            LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertIn(
            LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertIn(
            PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertEqual(
            registry[ACTION_REFRESH_COMPANY_PUBLIC_WEB]["default_workflow_command_type"],
            COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_REFRESH_COMPANY_PUBLIC_WEB]["default_workflow_command_contract"]["owner"],
            COMPANY_PUBLIC_WEB_REFRESH_OWNER,
        )
        self.assertEqual(
            registry[ACTION_REFRESH_COMPANY_PUBLIC_WEB]["workflow_command_control_summary"][
                "running_control_category_counts"
            ],
            {"orchestration": 1},
        )
        self.assertNotIn(
            LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertNotIn(
            LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        for activity_command_type in (
            LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
            LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
            LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
            LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
            LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
            PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
            PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
            PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
            SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
        ):
            self.assertEqual(
                workflow_command_activity_spine_policy(activity_command_type).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        self.assertNotIn(
            EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
            registry[ACTION_CONTINUE_ACQUISITION_RUN]["allowed_workflow_command_types"],
        )
        self.assertEqual(
            registry[ACTION_ADD_TO_CRM]["default_workflow_command_type"],
            CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_SET_CRM_STAGE]["default_workflow_command_type"],
            CRM_RECORD_UPDATE_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_ADD_CRM_NOTE]["default_workflow_command_type"],
            CRM_NOTE_ADD_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_CREATE_CRM_TASK]["default_workflow_command_type"],
            CRM_TASK_CREATE_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_ENRICH_PERSON_PUBLIC_WEB]["default_workflow_command_type"],
            CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
        )
        self.assertIn(
            EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
            registry[ACTION_EXPORT_CANDIDATES]["allowed_workflow_command_types"],
        )
        self.assertIn(
            EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            registry[ACTION_EXPORT_CANDIDATES]["allowed_workflow_command_types"],
        )
        self.assertEqual(
            registry[ACTION_EXPORT_CANDIDATES]["default_workflow_command_type"],
            EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
        )
        self.assertEqual(
            registry[ACTION_EXPORT_CANDIDATES]["workflow_command_control_summary"][
                "running_control_maturity_counts"
            ],
            {"owner_specific_cancel_resume": 2},
        )
        self.assertEqual(
            registry[ACTION_EXPORT_CANDIDATES]["workflow_command_control_summary"]["agent_ui_guidance"],
            "owner_specific_command_controls_available",
        )
        for action_record in registry.values():
            display_contract = dict(action_record.get("display_contract") or {})
            self.assertEqual(
                display_contract["source_of_truth"],
                "operation_runtime.ActionRegistry.display_contract_for",
            )
            self.assertEqual(display_contract["fallback_status"], "fail_closed")
            self.assertTrue(display_contract["display_label"])
            self.assertTrue(display_contract["display_category"])
            self.assertTrue(display_contract["description"])
            command_types = list(action_record.get("allowed_workflow_command_types") or [])
            command_contracts = list(action_record.get("allowed_workflow_command_contracts") or [])
            self.assertEqual(
                [contract["command_type"] for contract in command_contracts],
                command_types,
            )
            control_summary = dict(action_record.get("workflow_command_control_summary") or {})
            self.assertEqual(
                control_summary["source_of_truth"],
                "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
            )
            self.assertEqual(control_summary["command_count"], len(command_contracts))
            for command_contract in command_contracts:
                self.assertEqual(command_contract["control_policy"]["fallback_status"], "fail_closed")
                self.assertEqual(command_contract["activity_spine_policy"]["fallback_status"], "fail_closed")
                self.assertNotEqual(
                    command_contract["activity_spine_policy"]["requirement"],
                    ACTIVITY_SPINE_LEGACY_INTERNAL,
                )

    def test_workflow_command_api_exposure_is_action_registry_allowlist_owned(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "agent-exposure.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, model_client),
        )
        allowed_commands = {
            str(command_type or "").strip()
            for action_record in DEFAULT_ACTION_REGISTRY.to_record(include_command_contracts=False).values()
            for command_type in list(action_record.get("allowed_workflow_command_types") or [])
            if str(command_type or "").strip()
        }
        owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()

        registry_response = orchestrator.get_workflow_command_registry_api()
        self.assertEqual(registry_response["status"], "ok")
        self.assertEqual(set(registry_response["command_registry"]), set(owner_registry))
        for command_type, registry_record in registry_response["command_registry"].items():
            expected_status = (
                "action_registry_allowlisted"
                if command_type in allowed_commands
                else "not_action_registry_allowlisted"
            )
            self.assertEqual(
                registry_record["agent_exposure_gate"],
                "operation_runtime.ActionRegistry.allowed_workflow_command_types",
            )
            self.assertEqual(registry_record["agent_exposure_status"], expected_status, command_type)

        allowlisted_command = self.store.upsert_workflow_command(
            workflow_run_id="wf-agent-exposure",
            command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            owner=owner_registry[CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE],
            idempotency_key="workflow_command:agent-exposure:allowlisted",
            payload={"workspace_id": "default"},
        )
        owner_only_command = self.store.upsert_workflow_command(
            workflow_run_id="wf-agent-exposure",
            command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
            owner=owner_registry[CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE],
            idempotency_key="workflow_command:agent-exposure:owner-only",
            payload={"workspace_id": "default"},
        )

        command_rows = orchestrator.list_workflow_commands_api(
            {"workflow_run_id": "wf-agent-exposure", "limit": 50}
        )["workflow_commands"]
        exposure_by_id = {row["command_id"]: row["agent_exposure_status"] for row in command_rows}
        self.assertEqual(
            exposure_by_id[allowlisted_command["command_id"]],
            "action_registry_allowlisted",
        )
        self.assertEqual(
            exposure_by_id[owner_only_command["command_id"]],
            "not_action_registry_allowlisted",
        )

        detail_response = orchestrator.get_workflow_command_api(owner_only_command["command_id"])
        self.assertEqual(detail_response["status"], "ok")
        self.assertEqual(
            detail_response["workflow_command"]["agent_exposure_gate"],
            "operation_runtime.ActionRegistry.allowed_workflow_command_types",
        )
        self.assertEqual(
            detail_response["workflow_command"]["agent_exposure_status"],
            "not_action_registry_allowlisted",
        )

    def test_running_plan_review_request_cancel_closes_review_checkpoint(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "plan-review-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, model_client),
        )
        plan_payload = {
            "plan_id": "plan-openai-review-cancel",
            "target_company": "OpenAI",
            "query": "site:linkedin.com/in OpenAI research engineer",
            "workflow_run_id": "wf-plan-review-cancel",
            "stages": [{"stage": "probe"}],
        }
        review_session = self.store.create_plan_review_session(
            target_company="OpenAI",
            request_payload={
                "target_company": "OpenAI",
                "plan_id": plan_payload["plan_id"],
                "request_source": "acquisition.plan_review.request",
            },
            plan_payload=plan_payload,
            gate_payload={
                "required_before_execution": True,
                "risk_level": "medium",
                "source": "acquisition.plan_review.request",
            },
            execution_bundle_payload={
                "workflow_run_id": "wf-plan-review-cancel",
                "plan_id": plan_payload["plan_id"],
            },
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-plan-review-cancel",
            command_type=ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
            owner=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
            idempotency_key="workflow_command:plan-review-cancel",
            payload={
                "workspace_id": "default",
                "acquisition_plan": plan_payload,
                "plan_id": plan_payload["plan_id"],
                "target_company": "OpenAI",
            },
        )
        claimed = self.store.claim_workflow_command(
            command["command_id"],
            lease_owner="unit-test-plan-review-owner",
            lease_seconds=300,
        )
        running = self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-plan-review-owner",
        )
        self.assertEqual(running["status"], "running")

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_plan_review"},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertTrue(response["module_state_mutated"])
        self.assertEqual(response["workflow_command"]["status"], "cancelled")
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "workflow_orchestrator.cancel_acquisition_plan_review_request",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(response["control_policy"]["running_control_gap_status"], "closed")
        cancelled_session = self.store.get_plan_review_session(int(review_session["review_id"]))
        self.assertEqual(cancelled_session["status"], "cancelled")
        self.assertEqual(cancelled_session["decision"]["control_source"], "api.workflow_command_owner_specific_cancel")
        self.assertFalse(response["workflow_command"]["result"]["downstream_commit_planned"])

    def test_running_early_orchestration_cancel_requires_no_downstream_boundary(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "early-orchestration-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-early-orchestration-cancel",
            command_type=ACQUISITION_RUN_CREATE_COMMAND_TYPE,
            owner=DEFAULT_COMMAND_OWNER_REGISTRY.to_record()[ACQUISITION_RUN_CREATE_COMMAND_TYPE],
            idempotency_key="workflow_command:early-orchestration-cancel",
            payload={
                "workspace_id": "default",
                "workflow_payload": {"target_company": "OpenAI", "query": "OpenAI research"},
            },
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-orchestrator")
        running = self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-orchestrator",
        )
        self.assertEqual(running["status"], "running")

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_before_downstream", "force": True},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertTrue(response["module_state_mutated"])
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "workflow_orchestrator.cancel_orchestration_before_downstream",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "orchestration_before_downstream")
        self.assertFalse(response["workflow_command"]["result"]["downstream_command_planned"])

    def test_running_early_orchestration_cancel_blocks_after_downstream_planned(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "early-orchestration-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        parent = self.store.upsert_workflow_command(
            workflow_run_id="wf-early-orchestration-downstream",
            command_type=ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
            owner=ACQUISITION_INTENT_RESOLVE_OWNER,
            idempotency_key="workflow_command:early-orchestration-downstream:parent",
            payload={"workspace_id": "default", "target_company": "OpenAI"},
        )
        self.store.upsert_workflow_command(
            workflow_run_id="wf-early-orchestration-downstream",
            command_type=ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
            owner=ACQUISITION_PLAN_BUILD_OWNER,
            idempotency_key="workflow_command:early-orchestration-downstream:child",
            payload={
                "workspace_id": "default",
                "target_company": "OpenAI",
                "causality": {"parent_command_id": parent["command_id"]},
            },
        )
        self.store.claim_workflow_command(parent["command_id"], lease_owner="unit-test-orchestrator")
        self.store.mark_workflow_command_running(parent["command_id"], lease_owner="unit-test-orchestrator")

        response = orchestrator.cancel_workflow_command_api(
            parent["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_after_downstream", "force": True},
        )

        self.assertEqual(response["status"], "invalid")
        self.assertEqual(response["reason"], "orchestration_running_cancel_blocked_after_downstream_planned")
        self.assertEqual(response["downstream_command_count"], 1)
        self.assertFalse(response["module_state_mutated"])
        self.assertTrue(response["owner_specific_control"])
        latest = self.store.get_workflow_command(parent["command_id"])
        self.assertEqual(latest["status"], "running")

    def test_running_company_public_web_refresh_cancel_requires_no_downstream_boundary(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-refresh-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-company-public-web-refresh-cancel",
            command_type=COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
            owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            idempotency_key="workflow_command:company-public-web-refresh-cancel",
            payload={"workspace_id": "default", "company_key": "openai", "company_name": "OpenAI"},
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-company-public-web")
        self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-company-public-web",
        )

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_company_public_web_refresh", "force": True},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "workflow_orchestrator.cancel_orchestration_before_downstream",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "orchestration_before_downstream")
        self.assertFalse(response["workflow_command"]["result"]["downstream_command_planned"])

    def test_running_acquisition_plan_commit_cancel_marks_run_cancelled_before_probe(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "plan-commit-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-plan-commit-cancel",
            operation_id="op-plan-commit-cancel",
            command_type=ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
            owner=ACQUISITION_PLAN_COMMIT_OWNER,
            idempotency_key="workflow_command:plan-commit-cancel",
            payload={"workspace_id": "default", "target_company": "OpenAI", "plan_id": "plan-commit-cancel"},
        )
        acquisition_run = self.store.upsert_acquisition_run(
            {
                "acquisition_run_id": "acqrun-plan-commit-cancel",
                "workspace_id": "default",
                "operation_run_id": "op-plan-commit-cancel",
                "workflow_run_id": "wf-plan-commit-cancel",
                "plan_id": "plan-commit-cancel",
                "plan_review_id": 7,
                "target_company": "OpenAI",
                "query": "OpenAI research",
                "status": "committed_pending_probe",
                "current_phase": "probe_pending",
                "idempotency_key": "acquisition_run:plan-commit-cancel",
                "metadata": {"source_command_id": command["command_id"]},
            }
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-plan-commit")
        self.store.mark_workflow_command_running(command["command_id"], lease_owner="unit-test-plan-commit")

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_before_probe", "force": True},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertTrue(response["module_state_mutated"])
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "workflow_orchestrator.cancel_acquisition_plan_commit_before_probe",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "acquisition_plan_commit_before_probe")
        self.assertFalse(response["workflow_command"]["result"]["downstream_command_planned"])
        self.assertTrue(response["workflow_command"]["result"]["acquisition_run_cancelled"])
        cancelled_run = self.store.get_acquisition_run(acquisition_run["acquisition_run_id"])
        self.assertEqual(cancelled_run["status"], "cancelled_before_probe")
        self.assertEqual(cancelled_run["current_phase"], "cancelled")
        self.assertEqual(cancelled_run["metadata"]["control_source"], "api.workflow_command_owner_specific_cancel")

    def test_running_acquisition_scale_plan_cancel_marks_lane_and_activity_cancelled_before_discovery(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "scale-plan-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-scale-plan-cancel",
            operation_id="op-scale-plan-cancel",
            command_type=ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
            owner=ACQUISITION_SCALE_PLAN_OWNER,
            idempotency_key="workflow_command:scale-plan-cancel",
            payload={"workspace_id": "default", "target_company": "OpenAI", "query": "OpenAI research"},
        )
        acquisition_run = self.store.upsert_acquisition_run(
            {
                "acquisition_run_id": "acqrun-scale-plan-cancel",
                "workspace_id": "default",
                "operation_run_id": "op-scale-plan-cancel",
                "workflow_run_id": "wf-scale-plan-cancel",
                "target_company": "OpenAI",
                "query": "OpenAI research",
                "status": "scale_planned_pending_discovery",
                "current_phase": "discovery_pending",
                "idempotency_key": "acquisition_run:scale-plan-cancel",
                "metadata": {"source_command_id": command["command_id"]},
            }
        )
        activity = self.store.upsert_workflow_activity_run(
            {
                "activity_run_id": "actrun-scale-plan-cancel",
                "workspace_id": "default",
                "workflow_run_id": "wf-scale-plan-cancel",
                "operation_run_id": "op-scale-plan-cancel",
                "acquisition_run_id": acquisition_run["acquisition_run_id"],
                "command_id": command["command_id"],
                "activity_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
                "status": "planned_pending_owner",
                "phase": "discovery_query_planned",
                "idempotency_key": "workflow_activity:scale-plan-cancel",
                "metadata": {"provider_called": False, "legacy_job_shell_created": False},
            }
        )
        lane = self.store.upsert_acquisition_discovery_lane(
            {
                "lane_id": "lane-scale-plan-cancel",
                "workspace_id": "default",
                "acquisition_run_id": acquisition_run["acquisition_run_id"],
                "workflow_run_id": "wf-scale-plan-cancel",
                "operation_run_id": "op-scale-plan-cancel",
                "source_command_id": command["command_id"],
                "activity_run_id": activity["activity_run_id"],
                "target_company": "OpenAI",
                "query": "OpenAI research",
                "provider": "linkedin",
                "status": "planned_pending_owner",
                "phase": "discovery_query_planned",
                "lane_plan": {"status": "planned_pending_owner", "phase": "discovery_query_planned"},
                "downstream_command_ids": [],
                "idempotency_key": "acquisition_discovery_lane:scale-plan-cancel",
                "metadata": {"provider_called": False, "legacy_job_shell_created": False},
            }
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-scale-plan")
        self.store.mark_workflow_command_running(command["command_id"], lease_owner="unit-test-scale-plan")

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_before_discovery", "force": True},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertTrue(response["module_state_mutated"])
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "workflow_orchestrator.cancel_acquisition_scale_plan_before_discovery",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "acquisition_scale_plan_before_discovery")
        self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
        self.assertEqual(response["workflow_command"]["result"]["discovery_lane_cancelled_count"], 1)
        cancelled_activity = self.store.get_workflow_activity_run(activity["activity_run_id"])
        self.assertEqual(cancelled_activity["status"], "cancelled_before_discovery")
        self.assertEqual(cancelled_activity["phase"], "cancelled")
        cancelled_lane = self.store.get_acquisition_discovery_lane(lane["lane_id"])
        self.assertEqual(cancelled_lane["status"], "cancelled_before_discovery")
        self.assertEqual(cancelled_lane["phase"], "cancelled")
        cancelled_run = self.store.get_acquisition_run(acquisition_run["acquisition_run_id"])
        self.assertEqual(cancelled_run["status"], "cancelled_before_discovery")
        self.assertEqual(cancelled_run["current_phase"], "cancelled")
        self.assertEqual(cancelled_run["metadata"]["cancelled_activity_run_count"], 1)
        self.assertEqual(cancelled_run["metadata"]["cancelled_discovery_lane_count"], 1)

    def test_running_acquisition_scale_plan_cancel_blocks_after_activity_attempt_started(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "scale-plan-cancel-attempt.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-scale-plan-cancel-attempt",
            operation_id="op-scale-plan-cancel-attempt",
            command_type=ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
            owner=ACQUISITION_SCALE_PLAN_OWNER,
            idempotency_key="workflow_command:scale-plan-cancel-attempt",
            payload={"workspace_id": "default", "target_company": "OpenAI", "query": "OpenAI research"},
        )
        acquisition_run = self.store.upsert_acquisition_run(
            {
                "acquisition_run_id": "acqrun-scale-plan-cancel-attempt",
                "workspace_id": "default",
                "operation_run_id": "op-scale-plan-cancel-attempt",
                "workflow_run_id": "wf-scale-plan-cancel-attempt",
                "target_company": "OpenAI",
                "query": "OpenAI research",
                "status": "scale_planned_pending_discovery",
                "current_phase": "discovery_pending",
                "idempotency_key": "acquisition_run:scale-plan-cancel-attempt",
                "metadata": {"source_command_id": command["command_id"]},
            }
        )
        activity = self.store.upsert_workflow_activity_run(
            {
                "activity_run_id": "actrun-scale-plan-cancel-attempt",
                "workspace_id": "default",
                "workflow_run_id": "wf-scale-plan-cancel-attempt",
                "operation_run_id": "op-scale-plan-cancel-attempt",
                "acquisition_run_id": acquisition_run["acquisition_run_id"],
                "command_id": command["command_id"],
                "activity_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
                "status": "planned_pending_owner",
                "phase": "discovery_query_planned",
                "idempotency_key": "workflow_activity:scale-plan-cancel-attempt",
            }
        )
        self.store.upsert_workflow_activity_attempt(
            {
                "activity_run_id": activity["activity_run_id"],
                "workflow_run_id": "wf-scale-plan-cancel-attempt",
                "command_id": command["command_id"],
                "attempt_number": 1,
                "status": "running",
                "provider": "linkedin",
                "idempotency_key": "workflow_activity_attempt:scale-plan-cancel-attempt",
            }
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-scale-plan")
        self.store.mark_workflow_command_running(command["command_id"], lease_owner="unit-test-scale-plan")

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "too_late", "force": True},
        )

        self.assertEqual(response["status"], "invalid")
        self.assertEqual(response["reason"], "acquisition_scale_plan_cancel_blocked_after_discovery_started")
        self.assertEqual(response["activity_attempt_count"], 1)
        self.assertFalse(response["module_state_mutated"])
        self.assertEqual(
            self.store.get_acquisition_run(acquisition_run["acquisition_run_id"])["status"],
            "scale_planned_pending_discovery",
        )

    def test_running_crm_public_web_queue_batch_cancel_marks_batch_and_runs_cancelled_before_phase_commands(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-queue-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        batch = self.store.upsert_crm_public_web_batch(
            {
                "batch_id": "crm-public-web-batch-cancel",
                "workspace_id": "default",
                "status": "queued",
                "crm_record_ids": ["crm-record-cancel"],
                "run_ids": ["crm-public-web-run-cancel"],
                "idempotency_key": "crm-public-web-batch:cancel",
                "metadata": {"phase_command_planned": False},
            }
        )
        run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-run-cancel",
                "batch_id": batch["batch_id"],
                "workspace_id": "default",
                "crm_record_id": "crm-record-cancel",
                "candidate_name": "Ada Lovelace",
                "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/",
                "status": "queued",
                "phase": "queued",
                "idempotency_key": "crm-public-web-run:cancel",
            }
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-crm-public-web-queue-cancel",
            operation_id="op-crm-public-web-queue-cancel",
            command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            owner=DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE),
            idempotency_key="workflow_command:crm-public-web-queue-cancel",
            payload={
                "workspace_id": "default",
                "batch_id": batch["batch_id"],
                "run_ids": [run["run_id"]],
            },
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-crm-public-web-queue")
        self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-crm-public-web-queue",
        )

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_before_phase_commands", "force": True},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertTrue(response["module_state_mutated"])
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "crm_public_web_owner.cancel_queue_batch_before_phase_commands",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(
            response["workflow_command"]["result"]["cancel_boundary"],
            "crm_public_web_queue_batch_before_phase_commands",
        )
        self.assertEqual(response["workflow_command"]["result"]["run_cancelled_count"], 1)
        cancelled_batch = self.store.get_crm_public_web_batch(batch_id=batch["batch_id"])
        self.assertEqual(cancelled_batch["status"], "cancelled")
        self.assertEqual(cancelled_batch["metadata"]["control_source"], "api.workflow_command_owner_specific_cancel")
        cancelled_run = self.store.get_crm_public_web_run(run_id=run["run_id"])
        self.assertEqual(cancelled_run["status"], "cancelled")

    def test_crm_public_web_force_refresh_without_nonce_creates_distinct_runs(self) -> None:
        record = {
            "crm_record_id": "crmrec-force-refresh",
            "record_id": "crmrec-force-refresh",
            "id": "crmrec-force-refresh",
            "candidate_id": "candidate-force-refresh",
            "name": "Ada Lovelace",
            "candidate_name": "Ada Lovelace",
            "current_company": "Anthropic",
            "linkedin_url": "https://www.linkedin.com/in/ada-lovelace/",
            "linkedin_url_key": "https://www.linkedin.com/in/ada-lovelace/",
            "person_identity_key": "linkedin:https://www.linkedin.com/in/ada-lovelace/",
        }

        first = start_crm_public_web_batch(
            store=self.store,
            crm_records=[record],
            runtime_dir=self.runtime_dir,
            payload={"workspace_id": "default", "force_refresh": True},
        )
        second = start_crm_public_web_batch(
            store=self.store,
            crm_records=[record],
            runtime_dir=self.runtime_dir,
            payload={"workspace_id": "default", "force_refresh": True},
        )

        first_batch = first["batch"]
        second_batch = second["batch"]
        first_run = first["runs"][0]
        second_run = second["runs"][0]
        self.assertNotEqual(first_batch["batch_id"], second_batch["batch_id"])
        self.assertNotEqual(first_run["run_id"], second_run["run_id"])
        self.assertNotEqual(first_run["idempotency_key"], second_run["idempotency_key"])
        self.assertTrue(first_batch["metadata"]["refresh_nonce"].startswith("force-"))
        self.assertTrue(second_batch["metadata"]["refresh_nonce"].startswith("force-"))
        self.assertNotEqual(first_batch["metadata"]["refresh_nonce"], second_batch["metadata"]["refresh_nonce"])

    def test_crm_public_web_api_start_does_not_write_batch_before_queue_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-api-start-owner-boundary.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(AssetCatalog.discover(), settings, api_store, DeterministicModelClient()),
        )
        try:
            api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-api-start-owner-boundary",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:api-start-owner-boundary",
                    "display_name_cache": "Owner Boundary",
                    "primary_company_cache": "Anthropic",
                    "metadata": {"linkedin_url_cache": "https://www.linkedin.com/in/owner-boundary/"},
                }
            )
            with mock.patch.object(
                orchestrator._crm_public_web_owner,
                "_drain_crm_public_web_queue_batch_commands",
                return_value={
                    "status": "skipped",
                    "reason": "crm_public_web_queue_batch_command_owner_disabled",
                    "command_count": 0,
                    "completed_count": 0,
                    "legacy_bridge_used": False,
                },
            ):
                result = orchestrator.start_crm_record_public_web_search(
                    {
                        "crm_record_ids": ["crmrec-api-start-owner-boundary"],
                        "workspace_id": "default",
                    }
                )

            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["reason"], "crm_public_web_queue_batch_command_owner_disabled")
            self.assertEqual(api_store.list_crm_public_web_batches(workspace_id="default"), [])
            self.assertEqual(
                api_store.list_crm_public_web_runs(
                    crm_record_id="crmrec-api-start-owner-boundary",
                    workspace_id="default",
                    limit=10,
                ),
                [],
            )
            queue_commands = [
                command
                for command in api_store.list_workflow_commands(limit=100)
                if command["command_type"] == CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE
            ]
            self.assertEqual(len(queue_commands), 1)
            self.assertEqual(
                queue_commands[0]["payload"]["operation_planning_mode"],
                "create_crm_public_web_batch_from_operation",
            )
            self.assertNotIn("batch_id", queue_commands[0]["payload"])
        finally:
            api_store.close()

    def test_crm_public_web_detail_returns_durable_promotions_without_latest_run(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-promotions-without-run.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(AssetCatalog.discover(), settings, api_store, DeterministicModelClient()),
        )
        try:
            api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-promoted-no-run",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:promoted-no-run",
                    "display_name_cache": "Promoted Person",
                    "primary_company_cache": "Anthropic",
                    "metadata": {"linkedin_url_cache": "https://www.linkedin.com/in/promoted-no-run/"},
                }
            )
            api_store.upsert_crm_public_web_promotion(
                {
                    "promotion_id": "promotion-promoted-no-run-scholar",
                    "signal_id": "signal-old-run-scholar",
                    "run_id": "crm-public-web-run-historical",
                    "person_identity_key": "linkedin:promoted-no-run",
                    "crm_record_id": "crmrec-promoted-no-run",
                    "workspace_id": "default",
                    "candidate_name": "Promoted Person",
                    "current_company": "Anthropic",
                    "linkedin_url_key": "https://www.linkedin.com/in/promoted-no-run/",
                    "signal_kind": "profile_link",
                    "signal_type": "scholar_url",
                    "url": "https://scholar.google.com/citations?user=promoted",
                    "source_url": "https://scholar.google.com/citations?user=promoted",
                    "source_domain": "scholar.google.com",
                    "source_family": "scholar",
                    "source_title": "Promoted Person",
                    "identity_match_label": "confirmed",
                    "confidence_label": "high",
                    "publishable": True,
                    "clean_profile_link": True,
                    "action": "promote",
                    "promotion_status": "manually_promoted",
                    "promoted_field": "scholar_url",
                    "new_value": "https://scholar.google.com/citations?user=promoted",
                    "operator": "unit-test",
                }
            )

            detail = orchestrator.get_crm_record_public_web_search_detail("crmrec-promoted-no-run")

            self.assertEqual(detail["status"], "ok")
            self.assertIsNone(detail["latest_run"])
            self.assertEqual(detail["signals"], [])
            self.assertEqual(detail["email_candidates"], [])
            self.assertEqual(detail["profile_links"], [])
            self.assertEqual(len(detail["promotions"]), 1)
            self.assertEqual(detail["promotions"][0]["promotion_status"], "manually_promoted")
        finally:
            api_store.close()

    def test_crm_public_web_retry_without_nonce_joins_existing_child_run(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-retry-idempotency.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-retry-idempotency",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:retry-idempotency",
                    "display_name_cache": "Retry Person",
                    "headline_cache": "Researcher at Anthropic",
                    "primary_company_cache": "Anthropic",
                    "metadata": {"linkedin_url_cache": "https://www.linkedin.com/in/retry-idempotency/"},
                }
            )
            source_run = api_store.upsert_crm_public_web_run(
                {
                    "run_id": "crm-public-web-run-source-retry-idempotency",
                    "batch_id": "crm-public-web-batch-source-retry-idempotency",
                    "crm_record_id": record["crm_record_id"],
                    "workspace_id": "default",
                    "candidate_name": "Retry Person",
                    "current_company": "Anthropic",
                    "linkedin_url": "https://www.linkedin.com/in/retry-idempotency/",
                    "linkedin_url_key": "https://www.linkedin.com/in/retry-idempotency/",
                    "person_identity_key": "linkedin:retry-idempotency",
                    "status": "failed",
                    "phase": "failed",
                    "source_families": ["profile_web_presence"],
                    "options": {"source_families": ["profile_web_presence"]},
                    "execution_backend": "crm_public_web_v1",
                }
            )

            first = orchestrator.retry_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "run_ids": [source_run["run_id"]],
                    "reason": "retry_requested_from_target_candidates_panel",
                    "operator": "frontend",
                }
            )
            second = orchestrator.retry_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "run_ids": [source_run["run_id"]],
                    "reason": "retry_requested_from_target_candidates_panel",
                    "operator": "frontend",
                }
            )

            self.assertEqual(first["status"], "retried")
            self.assertEqual(second["status"], "retried")
            self.assertEqual(first["batch"]["batch_id"], second["batch"]["batch_id"])
            self.assertEqual(first["runs"][0]["run_id"], second["runs"][0]["run_id"])
            self.assertEqual(first["runs"][0]["idempotency_key"], second["runs"][0]["idempotency_key"])
            self.assertRegex(first["batch"]["batch_id"], r"^crm-public-web-batch-[0-9a-f]{16}$")
            retry_nonce = first["batch"]["metadata"]["refresh_nonce"]
            self.assertTrue(retry_nonce.startswith("retry-"))
            self.assertEqual(retry_nonce, second["batch"]["metadata"]["refresh_nonce"])
            self.assertEqual(first["batch"]["metadata"]["retry_idempotency_key"], retry_nonce)
            child_runs = [
                run
                for run in api_store.list_crm_public_web_runs(
                    crm_record_id=record["crm_record_id"],
                    workspace_id="default",
                    limit=10,
                )
                if str(run.get("run_id") or "") != source_run["run_id"]
            ]
            self.assertEqual(len({run["run_id"] for run in child_runs}), 1)
            queue_commands = [
                command
                for command in api_store.list_workflow_commands(limit=100)
                if command["command_type"] == CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE
            ]
            self.assertEqual(len(queue_commands), 1)
            self.assertEqual(queue_commands[0]["result"]["batch_id"], first["batch"]["batch_id"])
        finally:
            api_store.close()

    def test_crm_public_web_action_run_ids_fail_closed_on_workspace_mismatch(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-workspace-action-guard.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-tenant-public-web",
                    "workspace_id": "tenant-a",
                    "person_identity_key": "linkedin:tenant-public-web",
                    "display_name_cache": "Tenant Person",
                    "primary_company_cache": "Anthropic",
                    "metadata": {"linkedin_url_cache": "https://www.linkedin.com/in/tenant-public-web/"},
                }
            )
            batch = api_store.upsert_crm_public_web_batch(
                {
                    "batch_id": "crm-public-web-batch-tenant",
                    "workspace_id": "tenant-a",
                    "status": "queued",
                    "crm_record_ids": [record["crm_record_id"]],
                    "run_ids": ["crm-public-web-run-tenant-queued", "crm-public-web-run-tenant-failed"],
                    "idempotency_key": "crm-public-web-batch:tenant-workspace-action",
                }
            )
            queued_run = api_store.upsert_crm_public_web_run(
                {
                    "run_id": "crm-public-web-run-tenant-queued",
                    "batch_id": batch["batch_id"],
                    "workspace_id": "tenant-a",
                    "crm_record_id": record["crm_record_id"],
                    "candidate_name": "Tenant Person",
                    "status": "queued",
                    "phase": "queued",
                    "execution_backend": "crm_public_web_v1",
                    "idempotency_key": "crm-public-web-run:tenant-queued",
                }
            )
            failed_run = api_store.upsert_crm_public_web_run(
                {
                    "run_id": "crm-public-web-run-tenant-failed",
                    "batch_id": batch["batch_id"],
                    "workspace_id": "tenant-a",
                    "crm_record_id": record["crm_record_id"],
                    "candidate_name": "Tenant Person",
                    "status": "failed",
                    "phase": "failed",
                    "execution_backend": "crm_public_web_v1",
                    "idempotency_key": "crm-public-web-run:tenant-failed",
                }
            )

            self.assertEqual(
                api_store.list_crm_public_web_runs(batch_id=batch["batch_id"], workspace_id="default"),
                [],
            )
            poll_result = orchestrator.list_crm_record_public_web_searches(
                {
                    "workspace_id": "default",
                    "batch_id": batch["batch_id"],
                }
            )
            cancel_result = orchestrator.cancel_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "batch_id": batch["batch_id"],
                    "reason": "workspace_mismatch_should_not_cancel",
                }
            )
            retry_result = orchestrator.retry_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "batch_id": batch["batch_id"],
                    "reason": "workspace_mismatch_should_not_retry",
                }
            )
            cancel_run_result = orchestrator.cancel_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "run_ids": [queued_run["run_id"]],
                    "reason": "workspace_mismatch_should_not_cancel_run",
                }
            )
            retry_run_result = orchestrator.retry_crm_record_public_web_search(
                {
                    "workspace_id": "default",
                    "run_ids": [failed_run["run_id"]],
                    "reason": "workspace_mismatch_should_not_retry_run",
                }
            )

            self.assertEqual(poll_result["status"], "invalid")
            self.assertEqual(poll_result["reason"], "public_web_batch_workspace_mismatch")
            self.assertEqual(poll_result["batch_id"], batch["batch_id"])
            self.assertEqual(cancel_result["status"], "invalid")
            self.assertEqual(cancel_result["reason"], "public_web_batch_workspace_mismatch")
            self.assertEqual(cancel_result["batch_id"], batch["batch_id"])
            self.assertEqual(retry_result["status"], "invalid")
            self.assertEqual(retry_result["reason"], "public_web_batch_workspace_mismatch")
            self.assertEqual(retry_result["batch_id"], batch["batch_id"])
            self.assertEqual(cancel_run_result["status"], "invalid")
            self.assertEqual(cancel_run_result["reason"], "public_web_run_workspace_mismatch")
            self.assertEqual(cancel_run_result["run_ids"], [queued_run["run_id"]])
            self.assertEqual(retry_run_result["status"], "invalid")
            self.assertEqual(retry_run_result["reason"], "public_web_run_workspace_mismatch")
            self.assertEqual(retry_run_result["run_ids"], [failed_run["run_id"]])
            self.assertEqual(api_store.get_crm_public_web_run(run_id=queued_run["run_id"])["status"], "queued")
            self.assertEqual(api_store.get_crm_public_web_run(run_id=failed_run["run_id"])["status"], "failed")
        finally:
            api_store.close()

    def test_crm_public_web_path_reads_use_record_workspace_and_keep_promotions_across_empty_retry(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-record-workspace-detail.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-tenant-promoted",
                    "workspace_id": "tenant-a",
                    "person_identity_key": "linkedin:tenant-promoted",
                    "display_name_cache": "Tenant Promoted",
                    "primary_company_cache": "Anthropic",
                    "metadata": {"linkedin_url_cache": "https://www.linkedin.com/in/tenant-promoted/"},
                }
            )
            old_run = api_store.upsert_crm_public_web_run(
                {
                    "run_id": "crm-public-web-run-tenant-old-scholar",
                    "batch_id": "crm-public-web-batch-tenant-old",
                    "workspace_id": "tenant-a",
                    "crm_record_id": record["crm_record_id"],
                    "candidate_name": "Tenant Promoted",
                    "person_identity_key": "linkedin:tenant-promoted",
                    "linkedin_url": "https://www.linkedin.com/in/tenant-promoted/",
                    "status": "completed",
                    "phase": "completed",
                    "execution_backend": "crm_public_web_v1",
                    "idempotency_key": "crm-public-web-run:tenant-old-scholar",
                    "created_at": "2026-04-20T00:00:00Z",
                    "completed_at": "2026-04-20T00:01:00Z",
                }
            )
            signal = api_store.upsert_person_public_web_signal(
                {
                    "signal_id": "signal-tenant-old-scholar",
                    "run_id": old_run["run_id"],
                    "person_identity_key": "linkedin:tenant-promoted",
                    "record_id": record["crm_record_id"],
                    "candidate_name": "Tenant Promoted",
                    "current_company": "Anthropic",
                    "linkedin_url_key": "https://www.linkedin.com/in/tenant-promoted/",
                    "signal_kind": "profile_link",
                    "signal_type": "scholar_url",
                    "value": "https://scholar.google.com/citations?user=tenant",
                    "normalized_value": "https://scholar.google.com/citations?user=tenant",
                    "url": "https://scholar.google.com/citations?user=tenant",
                    "source_url": "https://scholar.google.com/citations?user=tenant",
                    "source_domain": "scholar.google.com",
                    "source_family": "scholar",
                    "source_title": "Tenant Promoted",
                    "confidence_label": "high",
                    "confidence_score": 0.96,
                    "identity_match_label": "confirmed",
                    "identity_match_score": 0.98,
                    "publishable": True,
                    "promotion_status": "promotion_recommended",
                }
            )
            promote_result = orchestrator.promote_crm_record_public_web_signal(
                record["crm_record_id"],
                {"signal_id": signal["signal_id"], "action": "promote", "operator": "unit-test"},
            )
            self.assertEqual(promote_result["status"], "promoted")

            latest_empty_run = api_store.upsert_crm_public_web_run(
                {
                    "run_id": "crm-public-web-run-tenant-latest-empty",
                    "batch_id": "crm-public-web-batch-tenant-latest",
                    "workspace_id": "tenant-a",
                    "crm_record_id": record["crm_record_id"],
                    "candidate_name": "Tenant Promoted",
                    "person_identity_key": "linkedin:tenant-promoted",
                    "linkedin_url": "https://www.linkedin.com/in/tenant-promoted/",
                    "status": "completed_with_errors",
                    "phase": "completed_with_errors",
                    "execution_backend": "crm_public_web_v1",
                    "idempotency_key": "crm-public-web-run:tenant-latest-empty",
                    "created_at": "2026-04-21T00:00:00Z",
                    "completed_at": "2026-04-21T00:01:00Z",
                }
            )

            detail = orchestrator.get_crm_record_public_web_search_detail(record["crm_record_id"])

            self.assertEqual(detail["status"], "ok")
            self.assertEqual(detail["latest_run"]["run_id"], latest_empty_run["run_id"])
            self.assertEqual(detail["profile_links"], [])
            self.assertEqual(len(detail["promotions"]), 1)
            self.assertEqual(detail["promotions"][0]["new_value"], "https://scholar.google.com/citations?user=tenant")
            self.assertEqual(detail["promotion_summary"]["promoted_count"], 1)
            self.assertEqual(detail["promotion_summary"]["promoted_link_count"], 1)
        finally:
            api_store.close()

    def test_running_crm_public_web_queue_batch_cancel_blocks_after_phase_command_planned(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-queue-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        batch = self.store.upsert_crm_public_web_batch(
            {
                "batch_id": "crm-public-web-batch-cancel-blocked",
                "workspace_id": "default",
                "status": "queued",
                "crm_record_ids": ["crm-record-cancel-blocked"],
                "run_ids": ["crm-public-web-run-cancel-blocked"],
                "idempotency_key": "crm-public-web-batch:cancel-blocked",
            }
        )
        run = self.store.upsert_crm_public_web_run(
            {
                "run_id": "crm-public-web-run-cancel-blocked",
                "batch_id": batch["batch_id"],
                "workspace_id": "default",
                "crm_record_id": "crm-record-cancel-blocked",
                "candidate_name": "Grace Hopper",
                "linkedin_url": "https://www.linkedin.com/in/grace-hopper/",
                "status": "queued",
                "phase": "queued",
                "idempotency_key": "crm-public-web-run:cancel-blocked",
            }
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-crm-public-web-queue-cancel-blocked",
            operation_id="op-crm-public-web-queue-cancel-blocked",
            command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
            owner=DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE),
            idempotency_key="workflow_command:crm-public-web-queue-cancel-blocked",
            payload={
                "workspace_id": "default",
                "batch_id": batch["batch_id"],
                "run_ids": [run["run_id"]],
            },
        )
        phase_command = self.store.upsert_workflow_command(
            workflow_run_id="wf-crm-public-web-queue-cancel-blocked",
            operation_id="op-crm-public-web-queue-cancel-blocked",
            command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
            owner=DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE),
            idempotency_key="workflow_command:crm-public-web-phase-after-queue",
            payload={
                "workspace_id": "default",
                "batch_id": batch["batch_id"],
                "run_id": run["run_id"],
                "causality": {"parent_command_id": command["command_id"]},
            },
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-crm-public-web-queue")
        self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-crm-public-web-queue",
        )

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "too_late", "force": True},
        )

        self.assertEqual(response["status"], "invalid")
        self.assertEqual(response["reason"], "crm_public_web_queue_batch_cancel_blocked_after_phase_planned")
        self.assertEqual(response["downstream_command_ids"], [phase_command["command_id"]])
        self.assertFalse(response["module_state_mutated"])
        self.assertEqual(self.store.get_crm_public_web_batch(batch_id=batch["batch_id"])["status"], "queued")
        self.assertEqual(self.store.get_crm_public_web_run(run_id=run["run_id"])["status"], "queued")

    def test_running_profile_fetch_activity_cancel_marks_activity_before_cache_lookup_attempt(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-fetch-activity-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-profile-fetch-activity-cancel",
            operation_id="op-profile-fetch-activity-cancel",
            command_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
            idempotency_key="workflow_command:profile-fetch-activity-cancel",
            payload={
                "workspace_id": "default",
                "workflow_run_id": "wf-profile-fetch-activity-cancel",
                "profile_urls": ["https://www.linkedin.com/in/ada-lovelace/"],
            },
        )
        activity = self.store.upsert_workflow_activity_run(
            {
                "activity_run_id": "actrun-profile-fetch-activity-cancel",
                "workspace_id": "default",
                "workflow_run_id": "wf-profile-fetch-activity-cancel",
                "operation_run_id": "op-profile-fetch-activity-cancel",
                "command_id": command["command_id"],
                "activity_type": LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "status": "planned",
                "phase": "cache_lookup_pending",
                "idempotency_key": "workflow_activity:profile-fetch-activity-cancel",
            }
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-profile-fetch-activity")
        self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-profile-fetch-activity",
        )

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "user_cancelled_before_cache_lookup", "force": True},
        )

        self.assertEqual(response["status"], "cancelled")
        self.assertTrue(response["owner_specific_control"])
        self.assertTrue(response["module_state_mutated"])
        self.assertEqual(
            response["control_policy"]["running_cancel_delegate"],
            "linkedin_profile_activity_owner.cancel_before_cache_lookup_attempt",
        )
        self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
        self.assertEqual(
            response["workflow_command"]["result"]["cancel_boundary"],
            "profile_fetch_activity_before_cache_lookup_attempt",
        )
        self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
        cancelled_activity = self.store.get_workflow_activity_run(activity["activity_run_id"])
        self.assertEqual(cancelled_activity["status"], "cancelled_before_cache_lookup")
        self.assertEqual(cancelled_activity["phase"], "cancelled")

    def test_running_profile_fetch_activity_cancel_blocks_after_entity_delta_recorded(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-fetch-activity-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, self.store, DeterministicModelClient()),
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-profile-fetch-activity-cancel-blocked",
            operation_id="op-profile-fetch-activity-cancel-blocked",
            command_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
            idempotency_key="workflow_command:profile-fetch-activity-cancel-blocked",
            payload={
                "workspace_id": "default",
                "workflow_run_id": "wf-profile-fetch-activity-cancel-blocked",
                "profile_urls": ["https://www.linkedin.com/in/grace-hopper/"],
            },
        )
        activity = self.store.upsert_workflow_activity_run(
            {
                "activity_run_id": "actrun-profile-fetch-activity-cancel-blocked",
                "workspace_id": "default",
                "workflow_run_id": "wf-profile-fetch-activity-cancel-blocked",
                "operation_run_id": "op-profile-fetch-activity-cancel-blocked",
                "command_id": command["command_id"],
                "activity_type": LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "status": "running",
                "phase": "profile_registry_cache_lookup",
                "idempotency_key": "workflow_activity:profile-fetch-activity-cancel-blocked",
            }
        )
        self.store.upsert_workflow_entity_delta(
            {
                "workspace_id": "default",
                "workflow_run_id": "wf-profile-fetch-activity-cancel-blocked",
                "operation_run_id": "op-profile-fetch-activity-cancel-blocked",
                "command_id": command["command_id"],
                "activity_run_id": activity["activity_run_id"],
                "entity_type": "profile",
                "entity_key": "grace-hopper",
                "delta_kind": "profile_fetch_required",
                "status": "not_applied",
                "reason": "provider_owner_pending",
                "idempotency_key": "workflow_entity_delta:profile-fetch-activity-cancel-blocked",
            }
        )
        self.store.claim_workflow_command(command["command_id"], lease_owner="unit-test-profile-fetch-activity")
        self.store.mark_workflow_command_running(
            command["command_id"],
            lease_owner="unit-test-profile-fetch-activity",
        )

        response = orchestrator.cancel_workflow_command_api(
            command["command_id"],
            {"actor": "unit-test", "reason": "too_late", "force": True},
        )

        self.assertEqual(response["status"], "invalid")
        self.assertEqual(response["reason"], "profile_fetch_activity_cancel_blocked_after_cache_lookup_started")
        self.assertEqual(response["entity_delta_count"], 1)
        self.assertFalse(response["module_state_mutated"])
        self.assertEqual(self.store.get_workflow_activity_run(activity["activity_run_id"])["status"], "running")

    def test_read_only_projection_action_creates_idempotent_operation_without_module_side_effects(self) -> None:
        first = self.writer.submit_action(
            action_type=ACTION_FILTER_PROJECTION,
            workspace_id="default",
            conversation_id="conv-a",
            target_ref={"projection_id": "proj-a"},
            input_payload={"filters": {"location": ["SF"]}},
            idempotency_key="filter:proj-a:sf",
            actor="unit-test",
        )
        duplicate = self.writer.submit_action(
            action_type=ACTION_FILTER_PROJECTION,
            workspace_id="default",
            conversation_id="conv-a",
            target_ref={"projection_id": "proj-a"},
            input_payload={"filters": {"location": ["NYC"]}},
            idempotency_key="filter:proj-a:sf",
            actor="unit-test",
        )

        self.assertTrue(first.executable)
        self.assertEqual(first.action["action_id"], duplicate.action["action_id"])
        self.assertEqual(first.operation_run["operation_run_id"], duplicate.operation_run["operation_run_id"])
        self.assertEqual(first.action["input"]["filters"], {"location": ["SF"]})
        self.assertEqual(first.operation_run["owner_module"], "projection_search_service")
        self.assertEqual(first.operation_run["status"], "queued")
        self.assertEqual(
            [event["event_type"] for event in self.store.list_operation_events(first.action["action_id"])],
            ["AgentActionQueued"],
        )
        self.assertEqual(
            [event["event_type"] for event in self.store.list_operation_events(first.operation_run["operation_run_id"])],
            ["OperationRunQueued"],
        )
        self.assertEqual(self.store.list_workflow_commands(limit=0), [])

    def test_approval_required_action_does_not_create_operation_run_before_approval(self) -> None:
        result = self.writer.submit_action(
            action_type=ACTION_EXPORT_CANDIDATES,
            workspace_id="default",
            target_ref={"projection_id": "proj-sensitive"},
            input_payload={"include_crm_notes": True},
            idempotency_key="export:proj-sensitive",
            actor="unit-test",
        )

        self.assertFalse(result.executable)
        self.assertEqual(result.operation_run, {})
        self.assertEqual(result.action["approval_status"], "required")
        self.assertEqual(result.action["status"], "approval_required")
        self.assertEqual(
            [event["event_type"] for event in self.store.list_operation_events(result.action["action_id"])],
            ["ActionApprovalRequired"],
        )
        self.assertEqual(self.store.list_workflow_commands(limit=0), [])

    def test_budget_required_action_requires_explicit_budget(self) -> None:
        with self.assertRaisesRegex(ValueError, "requires explicit budget"):
            self.writer.submit_action(
                action_type=ACTION_ENRICH_PERSON_PUBLIC_WEB,
                target_ref={"person_identity_key": "person-a"},
                idempotency_key="enrich:person-a",
            )

        result = self.writer.submit_action(
            action_type=ACTION_ENRICH_PERSON_PUBLIC_WEB,
            target_ref={"person_identity_key": "person-a"},
            budget={"max_provider_calls": 3, "max_usd": 1.5},
            idempotency_key="enrich:person-a",
        )
        self.assertEqual(result.action["approval_status"], "required")
        self.assertEqual(result.action["budget"], {"max_provider_calls": 3, "max_usd": 1.5})
        self.assertEqual(result.operation_run, {})

    def test_approval_transition_creates_idempotent_operation_run_after_approval(self) -> None:
        submitted = self.writer.submit_action(
            action_type=ACTION_EXPORT_CANDIDATES,
            workspace_id="default",
            target_ref={"projection_id": "proj-approval"},
            input_payload={"include_crm_notes": True},
            idempotency_key="export:proj-approval",
        )

        first_approval = self.writer.approve_action(
            action_id=submitted.action["action_id"],
            actor="unit-test",
            approval_payload={"approved_by": "operator"},
        )
        duplicate_approval = self.writer.approve_action(
            action_id=submitted.action["action_id"],
            actor="unit-test",
            approval_payload={"approved_by": "operator"},
        )

        self.assertEqual(first_approval.action["approval_status"], "approved")
        self.assertEqual(first_approval.action["status"], "queued")
        self.assertEqual(
            first_approval.operation_run["operation_run_id"],
            duplicate_approval.operation_run["operation_run_id"],
        )
        self.assertEqual(first_approval.operation_run["operation_type"], "export")
        self.assertEqual(
            [event["event_type"] for event in self.store.list_operation_events(submitted.action["action_id"])],
            ["ActionApprovalRequired", "ActionApproved"],
        )
        self.assertEqual(
            [event["event_type"] for event in self.store.list_operation_events(first_approval.operation_run["operation_run_id"])],
            ["OperationRunQueued"],
        )
        self.assertEqual(self.store.list_workflow_commands(limit=0), [])

    def test_cancel_operation_updates_operation_and_action_without_module_side_effects(self) -> None:
        result = self.writer.submit_action(
            action_type=ACTION_FILTER_PROJECTION,
            workspace_id="default",
            target_ref={"projection_id": "proj-cancel"},
            input_payload={"filters": {"role": ["engineering"]}},
            idempotency_key="filter:proj-cancel",
        )

        cancelled = self.writer.cancel_operation(
            operation_run_id=result.operation_run["operation_run_id"],
            actor="unit-test",
            reason="operator_cancelled",
        )

        action = self.store.get_agent_action(result.action["action_id"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertEqual(cancelled["progress"]["phase"], "cancelled")
        self.assertEqual(action["status"], "cancelled")
        self.assertEqual(
            [event["event_type"] for event in self.store.list_operation_events(result.operation_run["operation_run_id"])],
            ["OperationRunQueued", "OperationCancelled"],
        )
        self.assertEqual(self.store.list_workflow_commands(limit=0), [])

    def test_list_provenance_resume_and_retry_stay_inside_operation_runtime(self) -> None:
        result = self.writer.submit_action(
            action_type=ACTION_FILTER_PROJECTION,
            workspace_id="default",
            conversation_id="conv-ops",
            target_ref={"projection_id": "proj-retry"},
            input_payload={"filters": {"role": ["ml"]}},
            idempotency_key="filter:proj-retry",
        )
        operation_run_id = result.operation_run["operation_run_id"]

        resumed = self.writer.resume_operation(
            operation_run_id=operation_run_id,
            actor="unit-test",
            reason="worker_recovered",
        )
        duplicate_resume = self.writer.resume_operation(
            operation_run_id=operation_run_id,
            actor="unit-test",
            reason="worker_recovered",
        )
        self.assertEqual(resumed["operation_run"]["operation_run_id"], operation_run_id)
        self.assertEqual(duplicate_resume["events"][0]["event_id"], resumed["events"][0]["event_id"])
        self.assertEqual(resumed["operation_run"]["progress"]["phase"], "resume_requested")

        self.store.update_operation_run_state(
            operation_run_id,
            status="failed",
            progress_patch={"phase": "failed", "reason": "owner_timeout"},
        )
        retry = self.writer.retry_operation(
            operation_run_id=operation_run_id,
            actor="unit-test",
            reason="owner_timeout",
        )
        duplicate_retry = self.writer.retry_operation(
            operation_run_id=operation_run_id,
            actor="unit-test",
            reason="owner_timeout",
        )

        retry_run = retry["operation_run"]
        self.assertNotEqual(retry_run["operation_run_id"], operation_run_id)
        self.assertEqual(retry_run["status"], "queued")
        self.assertEqual(retry_run["metadata"]["parent_operation_run_id"], operation_run_id)
        self.assertEqual(duplicate_retry["operation_run"]["operation_run_id"], retry_run["operation_run_id"])
        self.assertEqual(
            {run["operation_run_id"] for run in self.store.list_operation_runs(action_id=result.action["action_id"])},
            {retry_run["operation_run_id"], operation_run_id},
        )
        self.assertEqual(
            [action["action_id"] for action in self.store.list_agent_actions(conversation_id="conv-ops")],
            [result.action["action_id"]],
        )
        event_types = [
            event["event_type"]
            for event in self.store.list_operation_events_for_action(result.action["action_id"])
        ]
        self.assertIn("AgentActionQueued", event_types)
        self.assertIn("OperationResumeRequested", event_types)
        self.assertIn("OperationRetryRequested", event_types)
        self.assertEqual(self.store.list_workflow_commands(limit=0), [])

    def test_operation_and_acquisition_runtime_state_are_pg_only(self) -> None:
        self._stop_pg_durable_runtime()
        with mock.patch.dict(
            os.environ,
            {
                "SOURCING_CONTROL_PLANE_POSTGRES_DSN": "",
                "SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA": "",
                "SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE": "disabled",
                "SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES": "",
                "SOURCING_LOCAL_POSTGRES_ENV_FILE": str(self.runtime_dir / "missing-postgres.env"),
            },
            clear=False,
        ):
            # Track B B3: SQLite-authoritative control-plane storage is no longer supported —
            # a store with no resolved DSN / disabled mode is rejected at CONSTRUCTION (stronger
            # than the former per-durable-op fail-closed), so operation/acquisition runtime state
            # can only ever live in Postgres.
            with self.assertRaisesRegex(RuntimeError, "no longer supported"):
                ControlPlaneStore(self.runtime_dir / "blocked-operation.db")
        self._start_pg_durable_runtime(runtime_dir=self.runtime_dir)
        self.store = ControlPlaneStore(self.runtime_dir / "sourcing_agent.db")
        self.writer = OperationRuntimeWriter(self.store)

    def test_company_asset_writer_records_pg_only_asset_evidence_and_assertion(self) -> None:
        writer = CompanyAssetWriter(self.store)
        asset = writer.record_asset(
            {
                "asset_id": "ca_openai_logo",
                "company_key": "openai",
                "target_company": "OpenAI",
                "asset_type": "logo_media",
                "source_kind": "operator_seed",
                "content_ref": "https://static.example.com/openai-logo.png",
                "visibility_scope": "public_summary",
            }
        )
        evidence = writer.record_evidence(
            {
                "evidence_id": "ce_openai_homepage",
                "company_key": "openai",
                "target_company": "OpenAI",
                "asset_id": asset["asset_id"],
                "evidence_type": "official_homepage",
                "value": "https://openai.com/",
                "source_url": "https://openai.com/",
                "confidence_score": 1.0,
                "artifact_refs": {"source": "unit-test"},
            }
        )
        assertion = writer.record_assertion(
            {
                "assertion_id": "cass_openai_homepage",
                "company_key": "openai",
                "target_company": "OpenAI",
                "assertion_type": "official_homepage",
                "value": "https://openai.com/",
                "authority": "operator_confirmed",
                "verification_status": "active",
                "source_evidence_id": evidence["evidence_id"],
            }
        )

        self.assertEqual(asset["metadata"]["writer_id"], "company_asset_writer_v1")
        self.assertEqual(self.store.list_company_assets(company_key="openai")[0]["asset_id"], "ca_openai_logo")
        self.assertEqual(self.store.list_company_evidence(asset_id="ca_openai_logo")[0]["evidence_id"], "ce_openai_homepage")
        self.assertEqual(
            self.store.list_company_assertions(company_key="openai", verification_status="active")[0]["assertion_id"],
            assertion["assertion_id"],
        )

    def test_export_operation_dispatch_plans_projection_export_command_without_running_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "dispatch.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_EXPORT_CANDIDATES,
                    "target_ref": {"projection_id": "proj-dispatch"},
                    "input": {"candidate_identity_keys": ["person-a"], "limit": 10},
                    "idempotency_key": "export:proj-dispatch",
                }
            )
            approved = orchestrator.approve_operation_action_api(
                submitted["action"]["action_id"],
                {"actor": "unit-test"},
            )
            operation_run_id = approved["operation_run"]["operation_run_id"]
            self.assertEqual(
                submitted["action"]["display_contract"]["source_of_truth"],
                "operation_runtime.ActionRegistry.display_contract_for",
            )
            self.assertEqual(submitted["action"]["display_contract"]["display_label"], "Export candidates")
            self.assertEqual(approved["action"]["display_contract"]["display_category"], "export")
            self.assertEqual(approved["operation_run"]["display_contract"]["display_label"], "Export candidates")
            self.assertEqual(
                approved["operation_run"]["control_state"]["control_source_of_truth"],
                "operation_runtime.operation_run_control_state",
            )
            self.assertIn("dispatch", approved["operation_run"]["control_state"]["allowed_actions"])

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})
            duplicate = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "planned")
            self.assertEqual(dispatched["display_contract"]["display_label"], "Export candidates")
            self.assertEqual(dispatched["action"]["display_contract"]["display_label"], "Export candidates")
            self.assertEqual(
                dispatched["operation_run"]["control_state"]["operation_status"],
                "planned",
            )
            self.assertEqual(
                dispatched["control_state"],
                dispatched["operation_run"]["control_state"],
            )
            self.assertNotIn("dispatch", dispatched["operation_run"]["control_state"]["allowed_actions"])
            self.assertEqual(dispatched["workflow_command"]["command_type"], "export.projection.generate")
            self.assertEqual(dispatched["workflow_command"]["owner"], "projection_exporter")
            self.assertEqual(dispatched["workflow_command"]["operation_id"], operation_run_id)
            self.assertEqual(
                dispatched["workflow_command"]["agent_exposure_gate"],
                "operation_runtime.ActionRegistry.allowed_workflow_command_types",
            )
            self.assertEqual(
                dispatched["workflow_command"]["agent_exposure_status"],
                "action_registry_allowlisted",
            )
            self.assertEqual(
                duplicate["workflow_command"]["command_id"],
                dispatched["workflow_command"]["command_id"],
            )
            self.assertEqual(duplicate["events"][0]["event_id"], dispatched["events"][0]["event_id"])
            operation = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation["status"], "planned")
            self.assertEqual(operation["workflow_ref"]["command_id"], dispatched["workflow_command"]["command_id"])
            provenance = orchestrator.get_operation_run_provenance_api(operation_run_id)
            self.assertEqual(provenance["action"]["display_contract"]["display_label"], "Export candidates")
            self.assertEqual(
                provenance["operation_run"]["control_state"]["control_source_of_truth"],
                "operation_runtime.operation_run_control_state",
            )
            self.assertEqual(provenance["operation_run"]["display_contract"]["display_category"], "export")
            self.assertEqual(
                [command["command_id"] for command in provenance["workflow_commands"]],
                [dispatched["workflow_command"]["command_id"]],
            )
            provenance_command = provenance["workflow_commands"][0]
            self.assertEqual(provenance_command["agent_exposure_status"], "action_registry_allowlisted")
            self.assertEqual(provenance_command["control_policy"]["fallback_status"], "fail_closed")
            self.assertEqual(provenance_command["execution_summary"]["fallback_status"], "fail_closed")
            self.assertFalse(provenance_command["execution_summary"]["module_state_mutated"])
            self.assertEqual(provenance_command["execution_summary"]["activity_count"], 0)
            run_detail = orchestrator.get_operation_run_api(operation_run_id)
            self.assertEqual(run_detail["operation_run"]["display_contract"]["display_label"], "Export candidates")
            run_summary = run_detail["operation_run"]["status_summary"]
            self.assertEqual(run_summary["fallback_status"], "fail_closed")
            self.assertFalse(run_summary["module_state_mutated"])
            self.assertEqual(run_summary["workflow_command_count"], 1)
            self.assertEqual(run_summary["command_status_counts"], {"queued": 1})
            self.assertEqual(run_summary["latest_event_type"], "OperationCommandPlanned")
            self.assertEqual(
                run_summary["latest_workflow_command"]["command_id"],
                dispatched["workflow_command"]["command_id"],
            )
            run_list = orchestrator.list_operation_runs_api(
                {"action_id": approved["action"]["action_id"], "include_status_summary": True}
            )
            self.assertEqual(
                run_list["operation_runs"][0]["control_state"]["control_source_of_truth"],
                "operation_runtime.operation_run_control_state",
            )
            self.assertEqual(run_list["operation_runs"][0]["status_summary"]["workflow_command_count"], 1)
            self.assertEqual(run_list["operation_runs"][0]["display_contract"]["display_category"], "export")
            self.assertFalse(dispatched["module_state_mutated"])
        finally:
            api_store.close()

    def test_projection_export_command_owner_records_activity_spine(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "projection-export-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-projection-export-activity",
                operation_id="op-projection-export-activity",
                command_type=EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                owner="projection_exporter",
                idempotency_key="export.projection.generate:activity",
                payload={
                    "projection_id": "proj-export-activity",
                    "candidate_identity_keys": ["person-a"],
                    "limit": 10,
                    "workspace_id": "default",
                },
            )

            with mock.patch.object(
                orchestrator,
                "_build_projection_candidates_archive_payload",
                return_value={
                    "status": "ok",
                    "filename": "projection-export.zip",
                    "content_type": "application/zip",
                    "body": b"export-bytes",
                    "projection_id": "proj-export-activity",
                    "record_count": 1,
                    "exported_record_count": 1,
                    "skipped_assertion_count": 0,
                },
            ):
                result = orchestrator._run_projection_export_generate_command(command)  # noqa: SLF001

            completed_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(completed_command["status"], "succeeded")
            self.assertEqual(completed_command["result"]["activity_run_id"][:7], "actrun_")
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["exported_record_count"], 1)
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activities[0]["activity_run_id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                activity_run_id=activities[0]["activity_run_id"],
                entity_type="projection_export",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "projection_export_generated")
            self.assertEqual(deltas[0]["status"], "recorded")
        finally:
            api_store.close()

    def test_projection_export_running_cancel_prevents_artifact_publish(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "projection-export-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-projection-export-cancel",
                operation_id="op-projection-export-cancel",
                command_type=EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                owner="projection_exporter",
                idempotency_key="export.projection.generate:cancel",
                payload={
                    "projection_id": "proj-export-cancel",
                    "candidate_identity_keys": ["person-a"],
                    "limit": 10,
                    "workspace_id": "default",
                },
            )

            def _build_and_cancel(_payload: dict[str, object]) -> dict[str, object]:
                cancel_result = orchestrator.cancel_workflow_command_api(
                    command["command_id"],
                    {"actor": "unit-test", "reason": "cancel-during-export-build"},
                )
                self.assertEqual(cancel_result["status"], "cancelled")
                return {
                    "status": "ok",
                    "filename": "projection-export.zip",
                    "content_type": "application/zip",
                    "body": b"export-bytes",
                    "projection_id": "proj-export-cancel",
                    "record_count": 1,
                    "exported_record_count": 1,
                    "skipped_assertion_count": 0,
                }

            with mock.patch.object(
                orchestrator,
                "_build_projection_candidates_archive_payload",
                side_effect=_build_and_cancel,
            ):
                result = orchestrator._run_projection_export_generate_command(command)  # noqa: SLF001

            completed_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(result["status"], "cancelled")
            self.assertEqual(completed_command["status"], "cancelled")
            self.assertFalse(
                (
                    settings.runtime_dir
                    / "exports"
                    / "projection_candidates"
                    / command["command_id"]
                    / "projection-export.zip"
                ).exists()
            )
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="projection_export",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["status"], "cancelled")
        finally:
            api_store.close()

    def test_crm_public_web_export_command_owner_records_activity_spine(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-public-web-export-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = orchestrator._plan_crm_public_web_export_generate_command(  # noqa: SLF001
                {
                    "workspace_id": "default",
                    "crm_record_ids": ["crmrec-export-a"],
                    "record_ids": ["crmrec-export-a"],
                    "mode": "promoted_only",
                },
                workspace_id="default",
            )
            self.assertEqual(command["command_type"], EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE)
            self.assertEqual(command["owner"], "crm_public_web_exporter")
            self.assertTrue(command["payload"].get("export_input_watermark_hash"))

            with mock.patch.object(
                orchestrator._crm_public_web_owner,
                "_export_crm_public_web_archive_from_owner",
                return_value={
                    "status": "ok",
                    "filename": "crm-public-web.zip",
                    "content_type": "application/zip",
                    "body": b"crm-export-bytes",
                    "record_count": 1,
                    "exported_signal_count": 2,
                    "exported_record_count": 1,
                    "no_public_web_result_count": 0,
                    "no_exportable_signal_count": 0,
                    "non_terminal_run_count": 0,
                    "export_mode": "promoted_only",
                },
            ):
                result = orchestrator._run_crm_public_web_export_generate_command(command)  # noqa: SLF001

            completed_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(result["status"], "ok")
            self.assertEqual(completed_command["status"], "succeeded")
            self.assertEqual(completed_command["result"]["activity_run_id"][:7], "actrun_")
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["exported_signal_count"], 2)
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activities[0]["activity_run_id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                activity_run_id=activities[0]["activity_run_id"],
                entity_type="crm_public_web_export",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "crm_public_web_export_generated")
            self.assertEqual(deltas[0]["status"], "recorded")
        finally:
            api_store.close()

    def test_excel_intake_command_thread_records_activity_spine(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "excel-intake-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            request_payload = {
                "raw_user_request": "Import workbook",
                "query": "Import workbook",
                "target_company": "Example",
            }
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-excel-intake-activity",
                operation_id="op-excel-intake-activity",
                command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                owner="excel_intake_owner",
                idempotency_key="excel.intake.run:activity",
                payload={
                    "workspace_id": "default",
                    "job_id": "excel-job-activity",
                    "request": request_payload,
                    "filename": "contacts.xlsx",
                    "row_count": 3,
                },
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="excel-owner-test")
            command = api_store.mark_workflow_command_running(
                command["command_id"],
                lease_owner="excel-owner-test",
            )
            activity, attempt = orchestrator._start_workflow_command_activity_attempt(  # noqa: SLF001
                command,
                activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                owner="excel_intake_owner",
                phase="excel_intake_started",
                lease_owner="excel-owner-test",
                provider="excel_intake_local",
                provider_request_ref="excel-job-activity",
                input_payload={"job_id": "excel-job-activity"},
                entity_counts={"excel_intake_job_count": 1, "row_count": 3},
                metadata={"test": True},
                attempt_suffix="excel_intake_run",
            )

            def _complete_excel_job(*, job_id, request, payload):
                del request, payload
                api_store.save_job(
                    job_id=job_id,
                    job_type="excel_intake",
                    status="completed",
                    stage="completed",
                    request_payload=request_payload,
                    summary_payload={"workflow_kind": "excel_intake"},
                )

            with mock.patch.object(orchestrator, "_run_excel_intake_workflow", side_effect=_complete_excel_job):
                orchestrator._run_excel_intake_workflow_command_thread(  # noqa: SLF001
                    command_id=command["command_id"],
                    job_id="excel-job-activity",
                    request=JobRequest.from_payload(request_payload),
                    payload={"row_count": 3},
                    activity=activity,
                    attempt=attempt,
                )

            completed_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(completed_command["status"], "succeeded")
            self.assertEqual(completed_command["result"]["activity_run_id"][:7], "actrun_")
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["phase"], "excel_intake_completed")
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activities[0]["activity_run_id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                activity_run_id=activities[0]["activity_run_id"],
                entity_type="excel_intake_job",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "excel_intake_started")
            self.assertEqual(deltas[0]["reason"], "excel_intake_run_completed")
        finally:
            api_store.close()

    def test_excel_intake_running_cancel_prevents_thread_success_terminalization(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "excel-intake-running-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            request_payload = {
                "raw_user_request": "Import workbook",
                "query": "Import workbook",
                "target_company": "Example",
            }
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-excel-intake-running-cancel",
                operation_id="op-excel-intake-running-cancel",
                command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                owner="excel_intake_owner",
                idempotency_key="excel.intake.run:running-cancel",
                payload={
                    "workspace_id": "default",
                    "job_id": "excel-job-running-cancel",
                    "request": request_payload,
                    "filename": "contacts.xlsx",
                    "row_count": 3,
                },
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="excel-owner-test")
            command = api_store.mark_workflow_command_running(
                command["command_id"],
                lease_owner="excel-owner-test",
            )
            activity, attempt = orchestrator._start_workflow_command_activity_attempt(  # noqa: SLF001
                command,
                activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                owner="excel_intake_owner",
                phase="excel_intake_started",
                lease_owner="excel-owner-test",
                provider="excel_intake_local",
                provider_request_ref="excel-job-running-cancel",
                input_payload={"job_id": "excel-job-running-cancel"},
                entity_counts={"excel_intake_job_count": 1, "row_count": 3},
                metadata={"test": True},
                attempt_suffix="excel_intake_run",
            )

            def _cancel_then_attempt_completion(*, job_id, request, payload):
                del request, payload
                cancel_result = orchestrator.cancel_workflow_command_api(
                    command["command_id"],
                    {"actor": "unit-test", "reason": "cancel-during-excel-thread"},
                )
                self.assertEqual(cancel_result["status"], "cancelled")
                self.assertEqual(api_store.get_job(job_id)["status"], "cancelled")

            with mock.patch.object(orchestrator, "_run_excel_intake_workflow", side_effect=_cancel_then_attempt_completion):
                orchestrator._run_excel_intake_workflow_command_thread(  # noqa: SLF001
                    command_id=command["command_id"],
                    job_id="excel-job-running-cancel",
                    request=JobRequest.from_payload(request_payload),
                    payload={"row_count": 3, "workflow_command_id": command["command_id"]},
                    activity=activity,
                    attempt=attempt,
                )

            completed_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(completed_command["status"], "cancelled")
            self.assertEqual(api_store.get_job("excel-job-running-cancel")["status"], "cancelled")
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "cancelled")
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activities[0]["activity_run_id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "cancelled")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                activity_run_id=activities[0]["activity_run_id"],
                entity_type="excel_intake_job",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["status"], "cancelled")
            self.assertIn(
                deltas[0]["reason"],
                {"cancel-during-excel-thread", "excel_intake_cancelled_before_thread_terminal"},
            )
            command_detail = orchestrator.get_workflow_command_api(command["command_id"])
            execution_summary = command_detail["workflow_command"]["execution_summary"]
            self.assertEqual(execution_summary["fallback_status"], "fail_closed")
            self.assertFalse(execution_summary["fallback_used"])
            self.assertFalse(execution_summary["module_state_mutated"])
            self.assertEqual(execution_summary["activity_count"], 1)
            self.assertEqual(execution_summary["attempt_count"], 1)
            self.assertEqual(execution_summary["entity_delta_count"], 1)
            self.assertEqual(execution_summary["latest_activity"]["status"], "cancelled")
            self.assertEqual(execution_summary["latest_attempt"]["status"], "cancelled")
            self.assertEqual(execution_summary["latest_entity_delta"]["status"], "cancelled")
            command_list = orchestrator.list_workflow_commands_api(
                {"workflow_run_id": command["workflow_run_id"], "include_execution_summary": True}
            )
            self.assertEqual(
                command_list["workflow_commands"][0]["execution_summary"]["latest_effect_status"],
                "cancelled",
            )
        finally:
            api_store.close()

    def test_projection_filter_operation_dispatch_completes_read_only_without_workflow_command(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "projection-read.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_serving_projection(
                {
                    "projection_id": "proj-read",
                    "projection_type": "run_scope_projection",
                    "collection_id": "company:test",
                    "source_run_id": "job-read",
                    "state": "serving",
                }
            )
            api_store.upsert_serving_projection_members(
                "proj-read",
                [
                    {
                        "candidate_identity_key": "linkedin:ada",
                        "person_identity_key": "linkedin:ada",
                        "profile_url_key": "ada",
                        "rank_index": 1,
                        "public_summary": {"name": "Ada"},
                    }
                ],
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_FILTER_PROJECTION,
                    "target_ref": {"projection_id": "proj-read"},
                    "input": {"limit": 1},
                    "idempotency_key": "filter:proj-read",
                }
            )
            operation_run_id = submitted["operation_run"]["operation_run_id"]

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})
            duplicate = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "completed")
            self.assertEqual(dispatched["operation_run"]["status"], "completed")
            self.assertEqual(dispatched["operation_run"]["result_ref"]["read_status"], "ready")
            self.assertEqual(
                dispatched["operation_run"]["result_ref"]["read_result"]["candidates"][0]["public_summary"]["name"],
                "Ada",
            )
            self.assertEqual(duplicate["status"], "completed")
            self.assertEqual(duplicate["events"], [])
            self.assertEqual(api_store.list_workflow_commands(limit=0), [])
            self.assertFalse(dispatched["module_state_mutated"])

            missing = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_FILTER_PROJECTION,
                    "target_ref": {"projection_id": "proj-missing"},
                    "input": {"limit": 1},
                    "idempotency_key": "filter:proj-missing",
                }
            )
            missing_operation_id = missing["operation_run"]["operation_run_id"]
            failed = orchestrator.dispatch_operation_run_api(missing_operation_id, {"actor": "unit-test"})
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(api_store.get_agent_action(missing["action"]["action_id"])["status"], "queued")
            retried = orchestrator.retry_operation_run_api(missing_operation_id, {"actor": "unit-test"})
            self.assertEqual(retried["status"], "queued")
            self.assertNotEqual(retried["operation_run"]["operation_run_id"], missing_operation_id)
        finally:
            api_store.close()

    def test_public_web_enrichment_operation_dispatch_leaves_batch_creation_to_command_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            crm_record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-public-web-op",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:public-web-op",
                    "candidate_identity_key": "linkedin:public-web-op",
                    "display_name_cache": "Public Web Person",
                    "headline_cache": "Researcher",
                    "primary_company_cache": "Example",
                    "metadata": {
                        "linkedin_url_cache": "https://www.linkedin.com/in/public-web-op/",
                    },
                }
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_ENRICH_PERSON_PUBLIC_WEB,
                    "target_ref": {"crm_record_id": crm_record["crm_record_id"]},
                    "budget": {"max_provider_calls": 2, "max_usd": 1.0},
                    "idempotency_key": "public-web:crmrec-public-web-op",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "planned")
            self.assertEqual(dispatched["workflow_command"]["command_type"], "crm.public_web.queue_batch")
            self.assertEqual(dispatched["workflow_command"]["owner"], "crm_public_web_owner")
            self.assertEqual(dispatched["workflow_command"]["operation_id"], operation_run_id)
            self.assertEqual(api_store.list_crm_public_web_batches(workspace_id="default"), [])

            drain = orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
                {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )
            self.assertEqual(drain["status"], "active")
            self.assertEqual(drain["completed_count"], 1)
            self.assertEqual(
                api_store.get_workflow_command(dispatched["workflow_command"]["command_id"])["status"],
                "succeeded",
            )
            operation_after_owner = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_owner["status"], "running")
            self.assertEqual(
                operation_after_owner["progress"]["phase"],
                "workflow_command_downstream_queued",
            )
            self.assertEqual(
                operation_after_owner["result_ref"]["workflow_command"]["command_id"],
                dispatched["workflow_command"]["command_id"],
            )
            self.assertEqual(
                api_store.get_agent_action(approved["action"]["action_id"])["status"],
                "running",
            )
            operation_events = api_store.list_operation_events(operation_run_id)
            self.assertIn(
                "OperationCommandDownstreamQueued",
                [event["event_type"] for event in operation_events],
            )
            batches = api_store.list_crm_public_web_batches(workspace_id="default")
            self.assertEqual(len(batches), 1)
            runs = api_store.list_crm_public_web_runs(batch_id=batches[0]["batch_id"], workspace_id="default")
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["crm_record_id"], crm_record["crm_record_id"])
            phase_commands = [
                command
                for command in api_store.list_workflow_commands(
                    workflow_run_id=dispatched["workflow_command"]["workflow_run_id"],
                    limit=0,
                )
                if command["command_type"] == "crm.public_web.search.submit"
            ]
            self.assertEqual(len(phase_commands), 1)
            self.assertEqual(phase_commands[0]["owner"], "crm_public_web_owner")
            self.assertEqual(phase_commands[0]["operation_id"], operation_run_id)
            self.assertEqual(
                phase_commands[0]["parent_command_id"],
                dispatched["workflow_command"]["command_id"],
            )
            command_registry = orchestrator.get_workflow_command_registry_api()
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["owner"],
                "crm_public_web_owner",
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["agent_exposure_gate"],
                "operation_runtime.ActionRegistry.allowed_workflow_command_types",
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["agent_exposure_status"],
                "not_action_registry_allowlisted",
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.queue_batch"]["agent_exposure_status"],
                "action_registry_allowlisted",
            )
            self.assertTrue(
                command_registry["command_registry"]["crm.public_web.search.submit"]["control_policy"][
                    "running_cancel_supported"
                ]
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["control_policy"][
                    "running_control_category"
                ],
                "crm_public_web_phase",
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["control_policy"][
                    "running_control_categories"
                ],
                ["crm_public_web_phase"],
            )
            self.assertTrue(
                command_registry["command_registry"]["export.projection.generate"]["control_policy"][
                    "running_cancel_supported"
                ]
            )
            self.assertEqual(
                command_registry["command_registry"]["export.projection.generate"]["control_policy"][
                    "running_control_category"
                ],
                "export_artifact",
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["control_policy"][
                    "running_cancel_prerequisites"
                ],
                ["payload.run_id", "crm_public_web_run_exists"],
            )
            self.assertEqual(
                command_registry["command_registry"]["export.projection.generate"]["control_policy"][
                    "running_cancel_delegate"
                ],
                "projection_exporter.cancel_export_command",
            )
            self.assertEqual(
                command_registry["command_registry"]["export.projection.generate"]["control_policy"][
                    "running_cancel_prerequisites"
                ],
                ["workflow_command_status_claimed_or_running", "artifact_not_published"],
            )
            self.assertTrue(
                command_registry["command_registry"]["excel.intake.run"]["control_policy"][
                    "running_cancel_supported"
                ]
            )
            self.assertEqual(
                command_registry["command_registry"]["excel.intake.run"]["control_policy"][
                    "running_cancel_delegate"
                ],
                "excel_intake_owner.cancel_excel_intake_run_command",
            )
            self.assertTrue(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "running_cancel_supported"
                ]
            )
            self.assertEqual(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "running_cancel_delegate"
                ],
                "workflow_provider_owner.cancel_or_poll_stop_provider_attempt",
            )
            self.assertEqual(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "running_cancel_prerequisites"
                ],
                [
                    "workflow_command_status_claimed_or_running",
                    "provider_activity_attempt_absent_or_poll_cancel_quarantine",
                    "no_provider_entity_delta_recorded",
                    "no_downstream_command_planned",
                    "command_lease_expired_or_force",
                ],
            )
            self.assertEqual(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "running_control_category"
                ],
                "provider_attempt",
            )
            self.assertEqual(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "provider_after_start_control_status"
                ],
                "active",
            )
            self.assertEqual(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "provider_after_start_control_mode"
                ],
                "poll_cancel_late_result_quarantine",
            )
            self.assertEqual(
                command_registry["command_registry"]["linkedin.profile_refill.submit_batch"]["control_policy"][
                    "provider_after_start_control_blocked_reason"
                ],
                "",
            )
            self.assertEqual(
                command_registry["command_registry"]["projection.run_scope.finalize"]["control_policy"][
                    "running_cancel_delegate"
                ],
                "workflow_domain_owner.cancel_before_domain_mutation_attempt",
            )
            self.assertTrue(
                command_registry["command_registry"]["projection.run_scope.finalize"]["control_policy"][
                    "running_cancel_supported"
                ]
            )
            self.assertEqual(
                command_registry["command_registry"]["projection.run_scope.finalize"]["control_policy"][
                    "running_control_category"
                ],
                "domain_mutation",
            )
            self.assertEqual(
                command_registry["command_registry"]["acquisition.plan.build"]["control_policy"][
                    "running_cancel_delegate"
                ],
                "workflow_orchestrator.cancel_orchestration_before_downstream",
            )
            self.assertTrue(
                command_registry["command_registry"]["acquisition.plan.build"]["control_policy"][
                    "running_cancel_supported"
                ]
            )
            self.assertEqual(
                command_registry["command_registry"]["acquisition.plan.build"]["control_policy"][
                    "running_control_category"
                ],
                "orchestration",
            )
            command_list = orchestrator.list_workflow_commands_api({"operation_id": operation_run_id, "limit": 10})
            self.assertEqual(command_list["status"], "ok")
            self.assertIn(
                "crm.public_web.search.submit",
                {command["command_type"] for command in command_list["workflow_commands"]},
            )
            command_detail = orchestrator.get_workflow_command_api(phase_commands[0]["command_id"])
            self.assertEqual(command_detail["workflow_command"]["command_id"], phase_commands[0]["command_id"])
            self.assertTrue(command_detail["workflow_command"]["control_policy"]["running_cancel_supported"])
            self.assertEqual(
                command_detail["workflow_command"]["control_state"]["control_source_of_truth"],
                "durable_runtime.workflow_command_control_state",
            )
            self.assertTrue(command_detail["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(command_detail["workflow_command"]["control_state"]["cancel_mode"], "generic")
            self.assertEqual(
                command_detail["workflow_command"]["control_policy"]["running_cancel_statuses"],
                ["claimed", "running"],
            )
            with mock.patch(
                "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
                return_value={"worker_status": "completed", "run_status": "completed", "summary": {}},
            ) as execute_phase:
                phase_drain = orchestrator._drain_crm_public_web_phase_commands(  # noqa: SLF001
                    {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
                )
            self.assertEqual(execute_phase.call_args.kwargs["phase_command_type"], "crm.public_web.search.submit")
            self.assertEqual(phase_drain["status"], "active")
            self.assertEqual(phase_drain["completed_count"], 1)
            self.assertEqual(
                api_store.get_workflow_command(phase_commands[0]["command_id"])["status"],
                "succeeded",
            )
            phase_activities = api_store.list_workflow_activity_runs(
                command_id=phase_commands[0]["command_id"],
                activity_type="crm.public_web.search.submit",
            )
            self.assertEqual(len(phase_activities), 1)
            self.assertEqual(phase_activities[0]["status"], "succeeded")
            self.assertEqual(phase_activities[0]["owner"], "crm_public_web_owner")
            phase_attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=phase_activities[0]["activity_run_id"],
            )
            self.assertEqual(len(phase_attempts), 1)
            self.assertEqual(phase_attempts[0]["status"], "succeeded")
            phase_deltas = api_store.list_workflow_entity_deltas(
                command_id=phase_commands[0]["command_id"],
                activity_run_id=phase_activities[0]["activity_run_id"],
                entity_type="crm_public_web_run",
            )
            self.assertEqual(len(phase_deltas), 1)
            self.assertEqual(phase_deltas[0]["delta_kind"], "crm_public_web_search_submitted")
            self.assertEqual(phase_deltas[0]["status"], "recorded")
            operation_after_phase = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_phase["status"], "completed")
            self.assertEqual(operation_after_phase["progress"]["phase"], "workflow_command_succeeded")
            operation_events = api_store.list_operation_events(operation_run_id)
            self.assertIn("OperationCommandSucceeded", [event["event_type"] for event in operation_events])
            self.assertFalse(dispatched["module_state_mutated"])
        finally:
            api_store.close()

    def test_crm_public_web_operation_full_phase_lifecycle_uses_typed_commands(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-operation-full-phase.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            crm_record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-public-web-full-phase",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:public-web-full-phase",
                    "candidate_identity_key": "linkedin:public-web-full-phase",
                    "display_name_cache": "Full Phase Person",
                    "headline_cache": "Researcher",
                    "primary_company_cache": "Example",
                    "metadata": {
                        "linkedin_url_cache": "https://www.linkedin.com/in/public-web-full-phase/",
                    },
                }
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_ENRICH_PERSON_PUBLIC_WEB,
                    "target_ref": {"crm_record_id": crm_record["crm_record_id"]},
                    "budget": {"max_provider_calls": 3, "max_usd": 1.0},
                    "idempotency_key": "public-web-full-phase:crmrec-public-web-full-phase",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]
            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})
            workflow_run_id = dispatched["workflow_command"]["workflow_run_id"]

            queue_drain = orchestrator._drain_crm_public_web_queue_batch_commands(  # noqa: SLF001
                {"workflow_run_id": workflow_run_id, "command_limit": 1}
            )
            self.assertEqual(queue_drain["completed_count"], 1)
            batches = api_store.list_crm_public_web_batches(workspace_id="default")
            self.assertEqual(len(batches), 1)
            batch_id = batches[0]["batch_id"]
            runs = api_store.list_crm_public_web_runs(batch_id=batch_id, workspace_id="default")
            self.assertEqual(len(runs), 1)
            run_id = runs[0]["run_id"]
            self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "running")

            phase_calls: list[str] = []
            poll_calls = {"count": 0}

            def find_command(command_type: str) -> dict:
                matches = [
                    command
                    for command in api_store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=100)
                    if command["command_type"] == command_type
                ]
                self.assertEqual(len(matches), 1, f"expected one {command_type} command")
                return matches[0]

            def fake_execute_crm_public_web_phase(**kwargs: object) -> dict:
                phase = str(kwargs.get("phase_command_type") or "")
                current_run_id = str(kwargs.get("run_id") or run_id)
                phase_calls.append(phase)
                if phase == CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE:
                    api_store.update_crm_public_web_run(
                        current_run_id,
                        {"status": "search_submitted", "phase": "search_submitted"},
                    )
                    return {
                        "worker_status": "running",
                        "run_status": "search_submitted",
                        "summary": {"phase_metrics": {"submitted_search_count": 1}},
                    }
                if phase == CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE:
                    poll_calls["count"] += 1
                    if poll_calls["count"] == 1:
                        api_store.update_crm_public_web_run(
                            current_run_id,
                            {"status": "searching", "phase": "searching"},
                        )
                        return {
                            "worker_status": "running",
                            "run_status": "searching",
                            "summary": {"pending_task_count": 1},
                        }
                    api_store.update_crm_public_web_run(
                        current_run_id,
                        {"status": "entry_links_ready", "phase": "entry_links_ready"},
                    )
                    return {
                        "worker_status": "running",
                        "run_status": "entry_links_ready",
                        "summary": {"phase_metrics": {"entry_link_count": 1}},
                    }
                if phase == CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE:
                    payload_path = self.runtime_dir / "public_web" / "full_phase_document_fetch_payload.json"
                    payload_path.parent.mkdir(parents=True, exist_ok=True)
                    payload_path.write_text(
                        json.dumps(
                            {
                                "fetched_documents": [
                                    {
                                        "source_url": "https://example.com/full-phase",
                                        "final_url": "https://example.com/full-phase",
                                        "title": "Full phase profile",
                                        "source_family": "homepage",
                                        "source_domain": "example.com",
                                        "content_type": "text/html",
                                        "status_code": 200,
                                    }
                                ]
                            }
                        ),
                        encoding="utf-8",
                    )
                    api_store.update_crm_public_web_run(
                        current_run_id,
                        {
                            "status": "documents_fetched",
                            "phase": "documents_fetched",
                            "analysis_checkpoint": {"document_fetch_payload_path": str(payload_path)},
                        },
                    )
                    return {
                        "worker_status": "running",
                        "run_status": "documents_fetched",
                        "summary": {"phase_metrics": {"fetched_document_count": 1}},
                    }
                if phase == CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE:
                    api_store.update_crm_public_web_run(
                        current_run_id,
                        {"status": "adjudication_completed", "phase": "adjudication_completed"},
                    )
                    return {
                        "worker_status": "running",
                        "run_status": "adjudication_completed",
                        "summary": {"phase_metrics": {"adjudicated_document_count": 1}},
                    }
                if phase == CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE:
                    api_store.update_crm_public_web_run(
                        current_run_id,
                        {"status": "analysis_completed", "phase": "analysis_completed"},
                    )
                    return {
                        "worker_status": "running",
                        "run_status": "analysis_completed",
                        "summary": {"phase_metrics": {"model_safe_document_count": 1}},
                    }
                if phase == CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE:
                    api_store.upsert_person_public_web_signal(
                        {
                            "signal_id": "signal-public-web-full-phase-1",
                            "run_id": current_run_id,
                            "record_id": crm_record["crm_record_id"],
                            "person_identity_key": crm_record["person_identity_key"],
                            "signal_kind": "email_candidate",
                            "signal_type": "work",
                            "value": "full-phase@example.com",
                            "normalized_value": "full-phase@example.com",
                            "source_url": "https://example.com/full-phase",
                            "source_domain": "example.com",
                            "confidence_label": "high",
                            "confidence_score": 0.93,
                            "identity_match_label": "strong",
                            "identity_match_score": 0.91,
                            "publishable": True,
                            "promotion_status": "not_promoted",
                            "artifact_refs": {"model_safe": "artifact://public-web/full-phase-model-safe.json"},
                            "model_provider": "qwen",
                            "model_version": "qwen3.5-plus-2026-04-20",
                        }
                    )
                    api_store.update_crm_public_web_run(
                        current_run_id,
                        {"status": "completed", "phase": "completed"},
                    )
                    return {
                        "worker_status": "completed",
                        "run_status": "completed",
                        "summary": {"phase_metrics": {"signal_materialized_count": 1}},
                    }
                raise AssertionError(f"unexpected phase command {phase}")

            with mock.patch(
                "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
                side_effect=fake_execute_crm_public_web_phase,
            ):
                search_drain = orchestrator._drain_crm_public_web_phase_commands(  # noqa: SLF001
                    {"workflow_run_id": workflow_run_id, "command_limit": 10}
                )
                self.assertEqual(search_drain["completed_count"], 1)
                self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "running")

                poll_deferred = orchestrator._drain_crm_public_web_phase_commands(  # noqa: SLF001
                    {"workflow_run_id": workflow_run_id, "command_limit": 10}
                )
                self.assertEqual(poll_deferred["deferred_count"], 1)
                poll_command = find_command(CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE)
                self.assertEqual(poll_command["status"], "retry_wait")
                resumed = orchestrator.resume_workflow_command_api(poll_command["command_id"], {"actor": "unit-test"})
                self.assertEqual(resumed["status"], "queued")

                for _ in range(5):
                    drain = orchestrator._drain_crm_public_web_phase_commands(  # noqa: SLF001
                        {"workflow_run_id": workflow_run_id, "command_limit": 10}
                    )
                    self.assertEqual(drain["failed_count"], 0)

            self.assertEqual(
                phase_calls,
                [
                    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                ],
            )
            for command_type in {
                CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
                CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
                CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
                CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
            }:
                self.assertEqual(find_command(command_type)["status"], "succeeded")

            poll_response = orchestrator.list_crm_record_public_web_searches(
                {"workspace_id": "default", "batch_id": batch_id}
            )
            self.assertEqual(poll_response["status"], "ok")
            self.assertIn(run_id, poll_response["phase_commands_by_run_id"])
            run_phase_commands = poll_response["phase_commands_by_run_id"][run_id]
            self.assertEqual(run_phase_commands["contract"], "crm_public_web_phase_command_status_v1")
            self.assertEqual(run_phase_commands["source"], "workflow_commands")
            self.assertFalse(run_phase_commands["fallback_used"])
            self.assertFalse(run_phase_commands["module_state_mutated"])
            self.assertEqual(run_phase_commands["command_count"], 6)
            self.assertEqual(
                set(run_phase_commands["by_phase"]),
                {
                    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                },
            )
            self.assertTrue(
                run_phase_commands["by_phase"][CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE]["control_api"][
                    "retry"
                ].endswith("/retry")
            )
            self.assertEqual(
                poll_response["runs"][0]["phase_commands"]["command_count"],
                6,
            )
            record_poll_response = orchestrator.list_crm_record_public_web_searches(
                {"workspace_id": "default", "crm_record_ids": [crm_record["crm_record_id"]]}
            )
            self.assertEqual(record_poll_response["status"], "ok")
            self.assertEqual(len(record_poll_response["runs"]), 1)
            self.assertEqual(record_poll_response["runs"][0]["run_id"], run_id)
            self.assertIn(run_id, record_poll_response["phase_commands_by_run_id"])
            self.assertEqual(
                record_poll_response["runs"][0]["phase_commands"]["command_count"],
                6,
            )

            final_operation = api_store.get_operation_run(operation_run_id)
            self.assertEqual(final_operation["status"], "completed")
            self.assertEqual(final_operation["progress"]["phase"], "workflow_command_succeeded")
            phase_activities = api_store.list_workflow_activity_runs(
                workflow_run_id=workflow_run_id,
                limit=100,
            )
            self.assertEqual(
                {
                    activity["activity_type"]
                    for activity in phase_activities
                    if str(activity.get("activity_type") or "").startswith("crm.public_web.")
                },
                {
                    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
                    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                },
            )
            document_deltas = api_store.list_workflow_entity_deltas(
                workflow_run_id=workflow_run_id,
                entity_type="public_web_document",
            )
            signal_deltas = api_store.list_workflow_entity_deltas(
                workflow_run_id=workflow_run_id,
                entity_type="public_web_signal",
            )
            self.assertEqual(len(document_deltas), 1)
            self.assertEqual(len(signal_deltas), 1)
            self.assertEqual(signal_deltas[0]["entity_payload"]["normalized_value"], "full-phase@example.com")
            detail_response = orchestrator.get_crm_record_public_web_search_detail(crm_record["crm_record_id"])
            self.assertEqual(detail_response["status"], "ok")
            self.assertEqual(detail_response["phase_commands"]["command_count"], 6)
            self.assertEqual(
                detail_response["latest_run"]["phase_commands"]["by_phase"][
                    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE
                ]["status"],
                "succeeded",
            )
        finally:
            api_store.close()

    def test_crm_public_web_record_poll_latest_runs_match_detail_contract(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-record-poll-detail-parity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            records = [
                api_store.upsert_crm_record(
                    {
                        "crm_record_id": "crmrec-public-web-poll-yuwei",
                        "workspace_id": "default",
                        "person_identity_key": "linkedin:public-web-poll-yuwei",
                        "candidate_identity_key": "linkedin:public-web-poll-yuwei",
                        "display_name_cache": "Yuwei Qin",
                        "headline_cache": "Professor of Engineering",
                        "primary_company_cache": "Carnegie Mellon University",
                        "metadata": {
                            "linkedin_url_cache": "https://www.linkedin.com/in/public-web-poll-yuwei/",
                        },
                    }
                ),
                api_store.upsert_crm_record(
                    {
                        "crm_record_id": "crmrec-public-web-poll-jackie",
                        "workspace_id": "default",
                        "person_identity_key": "linkedin:public-web-poll-jackie",
                        "candidate_identity_key": "linkedin:public-web-poll-jackie",
                        "display_name_cache": "Jackie Bow",
                        "headline_cache": "Engineer",
                        "primary_company_cache": "Asuna",
                        "metadata": {
                            "linkedin_url_cache": "https://www.linkedin.com/in/public-web-poll-jackie/",
                        },
                    }
                ),
            ]
            expected_latest_by_record: dict[str, str] = {}
            for index, record in enumerate(records):
                record_id = record["crm_record_id"]
                person_identity_key = record["person_identity_key"]
                old_run_id = f"crm-public-web-run-poll-old-{index}"
                latest_run_id = f"crm-public-web-run-poll-latest-{index}"
                api_store.upsert_crm_public_web_run(
                    {
                        "run_id": old_run_id,
                        "batch_id": f"crm-public-web-batch-poll-old-{index}",
                        "workspace_id": "default",
                        "crm_record_id": record_id,
                        "person_identity_key": person_identity_key,
                        "candidate_name": record["display_name_cache"],
                        "status": "completed",
                        "phase": "completed",
                        "execution_backend": "crm_public_web_v1",
                        "idempotency_key": f"crm-public-web-run:poll-old:{index}",
                        "created_at": f"2026-04-20T00:0{index}:00Z",
                        "completed_at": f"2026-04-20T00:0{index}:30Z",
                    }
                )
                api_store.upsert_crm_public_web_run(
                    {
                        "run_id": latest_run_id,
                        "batch_id": f"crm-public-web-batch-poll-latest-{index}",
                        "workspace_id": "default",
                        "crm_record_id": record_id,
                        "person_identity_key": person_identity_key,
                        "candidate_name": record["display_name_cache"],
                        "status": "completed_with_errors",
                        "phase": "completed_with_errors",
                        "execution_backend": "crm_public_web_v1",
                        "idempotency_key": f"crm-public-web-run:poll-latest:{index}",
                        "created_at": f"2026-04-21T00:0{index}:00Z",
                        "completed_at": f"2026-04-21T00:0{index}:30Z",
                    }
                )
                expected_latest_by_record[record_id] = latest_run_id

            poll_response = orchestrator.list_crm_record_public_web_searches(
                {
                    "workspace_id": "default",
                    "crm_record_ids": [record["crm_record_id"] for record in records],
                    "limit": 100,
                }
            )

            self.assertEqual(poll_response["status"], "ok")
            latest_by_record = {
                run["crm_record_id"]: run["run_id"]
                for run in poll_response["runs"]
            }
            self.assertEqual(latest_by_record, expected_latest_by_record)
            for record in records:
                detail_response = orchestrator.get_crm_record_public_web_search_detail(record["crm_record_id"])
                self.assertEqual(detail_response["status"], "ok")
                self.assertEqual(
                    detail_response["latest_run"]["run_id"],
                    latest_by_record[record["crm_record_id"]],
                )
        finally:
            api_store.close()

    def test_crm_public_web_adjudication_phase_commands_drain_with_bounded_parallelism(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-adjudication-parallel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            workflow_run_id = "wf-crm-public-web-adjudication-parallel"
            for index in (1, 2):
                api_store.upsert_crm_public_web_run(
                    {
                        "run_id": f"run-adjudication-{index}",
                        "batch_id": "batch-adjudication-parallel",
                        "workspace_id": "default",
                        "crm_record_id": f"crmrec-adjudication-{index}",
                        "status": "documents_fetched",
                        "phase": "documents_fetched",
                        "execution_backend": "crm_public_web_v1",
                    }
                )
            api_store.upsert_crm_public_web_run(
                {
                    "run_id": "run-signal-1",
                    "batch_id": "batch-adjudication-parallel",
                    "workspace_id": "default",
                    "crm_record_id": "crmrec-signal-1",
                    "status": "analysis_completed",
                    "phase": "analysis_completed",
                    "execution_backend": "crm_public_web_v1",
                }
            )
            evidence_commands = [
                api_store.upsert_workflow_command(
                    workflow_run_id=workflow_run_id,
                    command_id=f"cmd-evidence-{index}",
                    command_type=CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                    owner="crm_public_web_owner",
                    idempotency_key=f"crm.public_web.evidence.adjudicate:run-{index}",
                    payload={
                        "workspace_id": "default",
                        "batch_id": "batch-adjudication-parallel",
                        "run_id": f"run-adjudication-{index}",
                    },
                )
                for index in (1, 2)
            ]
            signal_command = api_store.upsert_workflow_command(
                workflow_run_id=workflow_run_id,
                command_id="cmd-signal-materialize-1",
                command_type=CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                owner="crm_public_web_owner",
                idempotency_key="crm.public_web.signals.materialize:run-signal-1",
                payload={
                    "workspace_id": "default",
                    "batch_id": "batch-adjudication-parallel",
                    "run_id": "run-signal-1",
                },
            )
            self.assertEqual(len(evidence_commands), 2)
            self.assertEqual(signal_command["status"], "queued")
            adjudication_barrier = threading.Barrier(2)
            phase_calls: list[tuple[str, str, str]] = []
            phase_lock = threading.Lock()

            def fake_execute_crm_public_web_phase(**kwargs: object) -> dict[str, object]:
                phase = str(kwargs.get("phase_command_type") or "")
                run_id = str(kwargs.get("run_id") or "")
                with phase_lock:
                    phase_calls.append((phase, run_id, threading.current_thread().name))
                if phase == CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE:
                    adjudication_barrier.wait(timeout=2)
                    return {
                        "worker_status": "running",
                        "run_status": "adjudication_completed",
                        "summary": {
                            "phase_metrics": {"adjudicated_profile_link_count": 1},
                            "artifact_root": f"artifact://public-web/{run_id}",
                        },
                    }
                if phase == CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE:
                    return {
                        "worker_status": "completed",
                        "run_status": "completed",
                        "summary": {"phase_metrics": {"signal_materialized_count": 0}},
                    }
                raise AssertionError(f"unexpected phase command {phase}")

            with mock.patch(
                "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
                side_effect=fake_execute_crm_public_web_phase,
            ):
                drain = orchestrator._drain_crm_public_web_phase_commands(  # noqa: SLF001
                    {
                        "workflow_run_id": workflow_run_id,
                        "command_limit": 3,
                        "adjudication_command_concurrency": 2,
                    }
                )

            self.assertEqual(drain["status"], "active")
            self.assertEqual(drain["parallel_adjudication_command_count"], 2)
            self.assertEqual(drain["adjudication_command_concurrency"], 2)
            self.assertEqual(drain["completed_count"], 3)
            self.assertEqual(drain["failed_count"], 0)
            evidence_threads = {
                thread_name
                for phase, _run_id, thread_name in phase_calls
                if phase == CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE
            }
            self.assertEqual(len(evidence_threads), 2)
            self.assertEqual(
                {
                    api_store.get_workflow_command(str(command["command_id"]))["status"]
                    for command in [*evidence_commands, signal_command]
                },
                {"succeeded"},
            )
            self.assertEqual(
                len(
                    api_store.list_workflow_activity_runs(
                        workflow_run_id=workflow_run_id,
                        activity_type=CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                    )
                ),
                2,
            )
            self.assertEqual(
                len(
                    api_store.list_workflow_activity_runs(
                        workflow_run_id=workflow_run_id,
                        activity_type=CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                    )
                ),
                1,
            )
        finally:
            api_store.close()

    def test_workflow_command_cancel_control_updates_linked_operation(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "command-control-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            crm_record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-command-control",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:command-control",
                    "candidate_identity_key": "linkedin:command-control",
                    "display_name_cache": "Command Control Person",
                    "headline_cache": "Researcher",
                    "primary_company_cache": "Example",
                }
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_ENRICH_PERSON_PUBLIC_WEB,
                    "target_ref": {"crm_record_id": crm_record["crm_record_id"]},
                    "budget": {"max_provider_calls": 2, "max_usd": 1.0},
                    "idempotency_key": "public-web:crmrec-command-control",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-command-control-op",
                operation_id=operation_run_id,
                command_type="crm.public_web.search.submit",
                owner="crm_public_web_owner",
                idempotency_key="wf-command-control-op:search-submit",
                payload={"operation_id": operation_run_id, "crm_record_id": crm_record["crm_record_id"]},
            )

            cancelled = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "unit-test-cancel"},
            )
            running_command = api_store.upsert_workflow_command(
                workflow_run_id="wf-command-control-op",
                operation_id=operation_run_id,
                command_type="export.projection.generate",
                owner="projection_exporter",
                idempotency_key="wf-command-control-op:projection-export",
            )
            api_store.claim_workflow_command(running_command["command_id"], lease_owner="worker-a")
            api_store.mark_workflow_command_running(running_command["command_id"], lease_owner="worker-a")
            export_cancelled = orchestrator.cancel_workflow_command_api(
                running_command["command_id"],
                {"actor": "unit-test", "reason": "running"},
            )
            resumable_export_command = api_store.upsert_workflow_command(
                workflow_run_id="wf-command-control-op",
                operation_id="op-export-resume",
                command_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                owner="crm_public_web_exporter",
                idempotency_key="wf-command-control-op:crm-public-web-export-resume",
                payload={"workspace_id": "default", "crm_record_ids": [crm_record["crm_record_id"]]},
            )
            api_store.claim_workflow_command(
                resumable_export_command["command_id"],
                lease_owner="worker-b",
                lease_seconds=60,
            )
            api_store.mark_workflow_command_running(
                resumable_export_command["command_id"],
                lease_owner="worker-b",
            )
            running_export_before_resume = orchestrator.get_workflow_command_api(resumable_export_command["command_id"])
            export_resume_blocked = orchestrator.resume_workflow_command_api(
                resumable_export_command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            export_resumed = orchestrator.resume_workflow_command_api(
                resumable_export_command["command_id"],
                {"actor": "unit-test", "reason": "force-requeue", "force": True},
            )

            self.assertEqual(cancelled["status"], "cancelled")
            self.assertEqual(cancelled["workflow_command"]["status"], "cancelled")
            self.assertEqual(cancelled["control_policy"]["command_type"], "crm.public_web.search.submit")
            self.assertEqual(cancelled["activity_spine_policy"]["command_type"], "crm.public_web.search.submit")
            self.assertEqual(cancelled["activity_spine_policy"]["requirement"], "activity_attempt_entity_delta_required")
            self.assertEqual(cancelled["operation_sync"]["status"], "cancelled")
            self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "cancelled")
            self.assertEqual(api_store.get_agent_action(approved["action"]["action_id"])["status"], "cancelled")
            self.assertIn(
                "OperationCommandCancelled",
                [event["event_type"] for event in api_store.list_operation_events(operation_run_id)],
            )
            self.assertEqual(export_cancelled["status"], "cancelled")
            self.assertEqual(export_cancelled["workflow_command"]["status"], "cancelled")
            self.assertTrue(export_cancelled["control_policy"]["running_cancel_supported"])
            self.assertEqual(export_cancelled["control_policy"]["running_cancel_delegate"], "projection_exporter.cancel_export_command")
            self.assertEqual(export_cancelled["activity_spine_policy"]["command_type"], "export.projection.generate")
            self.assertEqual(
                export_cancelled["activity_spine_policy"]["requirement"],
                "activity_attempt_entity_delta_required",
            )
            self.assertEqual(
                export_cancelled["workflow_command"]["control_policy"]["generic_cancel_statuses"],
                ["queued", "retry_wait"],
            )
            self.assertEqual(api_store.get_workflow_command(running_command["command_id"])["status"], "cancelled")
            self.assertTrue(
                running_export_before_resume["workflow_command"]["control_state"]["can_resume"],
            )
            self.assertEqual(
                running_export_before_resume["workflow_command"]["control_state"]["resume_mode"],
                "owner_specific",
            )
            self.assertEqual(
                running_export_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "crm_public_web_exporter.resume_export_command",
            )
            self.assertEqual(
                export_resume_blocked["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(export_resume_blocked["status"], "invalid")
            self.assertEqual(export_resumed["status"], "queued")
            self.assertEqual(export_resumed["workflow_command"]["status"], "queued")
            self.assertEqual(
                export_resumed["workflow_command"]["result"]["resume_mode"],
                "owner_specific_requeue",
            )
            self.assertEqual(
                export_resumed["workflow_command"]["result"]["artifact_publish_policy"],
                "resume_requeues_without_publish",
            )
            self.assertEqual(
                api_store.get_workflow_command(resumable_export_command["command_id"])["status"],
                "queued",
            )
            self.assertEqual(
                export_resumed["workflow_entity_delta"]["entity_type"],
                "crm_public_web_export",
            )
            self.assertEqual(export_resumed["workflow_entity_delta"]["status"], "queued")
        finally:
            api_store.close()

    def test_crm_public_web_running_phase_command_cancel_uses_owner_specific_control(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-running-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_crm_public_web_run(
                {
                    "run_id": "run-public-web-running-cancel",
                    "batch_id": "batch-public-web-running-cancel",
                    "crm_record_id": "crmrec-public-web-running-cancel",
                    "workspace_id": "default",
                    "status": "searching",
                    "phase": "searching",
                    "execution_backend": "crm_public_web_v1",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-public-web-running-cancel",
                operation_id="op-public-web-running-cancel",
                command_type="crm.public_web.search.poll_fetch",
                owner="crm_public_web_owner",
                idempotency_key="wf-public-web-running-cancel:poll-fetch",
                payload={
                    "run_id": "run-public-web-running-cancel",
                    "batch_id": "batch-public-web-running-cancel",
                    "workspace_id": "default",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="worker-a")
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="worker-a")

            cancelled = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "unit-test-owner-cancel"},
            )

            self.assertEqual(cancelled["status"], "cancelled")
            self.assertTrue(cancelled["owner_specific_control"])
            self.assertTrue(cancelled["module_state_mutated"])
            self.assertEqual(cancelled["workflow_command"]["status"], "cancelled")
            self.assertTrue(cancelled["workflow_command"]["control_policy"]["running_cancel_supported"])
            self.assertEqual(cancelled["workflow_command"]["result"]["owner_specific_control"], True)
            self.assertEqual(
                api_store.get_crm_public_web_run(run_id="run-public-web-running-cancel")["status"],
                "cancelled",
            )
            activities = api_store.list_workflow_activity_runs(command_id=command["command_id"])
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "cancelled")
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activities[0]["activity_run_id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "cancelled")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="crm_public_web_run",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["status"], "cancelled")
            self.assertEqual(deltas[0]["reason"], "crm_public_web_run_cancelled_by_command_control")
        finally:
            api_store.close()

    def test_crm_public_web_poll_phase_defer_and_resume_uses_workflow_command_state(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-phase-control.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_crm_public_web_run(
                {
                    "run_id": "run-public-web-phase-control",
                    "batch_id": "batch-public-web-phase-control",
                    "crm_record_id": "crmrec-public-web-phase-control",
                    "workspace_id": "default",
                    "status": "searching",
                    "phase": "searching",
                    "execution_backend": "crm_public_web_v1",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-public-web-phase-control",
                command_type="crm.public_web.search.poll_fetch",
                owner="crm_public_web_owner",
                idempotency_key="wf-public-web-phase-control:poll-fetch",
                payload={
                    "run_id": "run-public-web-phase-control",
                    "batch_id": "batch-public-web-phase-control",
                    "workspace_id": "default",
                },
                max_attempts=6,
            )
            with mock.patch(
                "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
                return_value={
                    "worker_status": "running",
                    "run_status": "searching",
                    "summary": {"pending_task_count": 2},
                },
            ) as execute_phase:
                deferred = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

            waiting_command = api_store.get_workflow_command(command["command_id"])
            resumed = orchestrator.resume_workflow_command_api(command["command_id"], {"actor": "unit-test"})

            self.assertEqual(execute_phase.call_args.kwargs["phase_command_type"], "crm.public_web.search.poll_fetch")
            self.assertEqual(deferred["status"], "deferred")
            self.assertEqual(deferred["reason"], "crm_public_web_remote_search_not_ready")
            self.assertEqual(waiting_command["status"], "retry_wait")
            self.assertEqual(waiting_command["result"]["reason"], "crm_public_web_remote_search_not_ready")
            retry_activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type="crm.public_web.search.poll_fetch",
            )
            self.assertEqual(len(retry_activities), 1)
            self.assertEqual(retry_activities[0]["status"], "retry_wait")
            retry_deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                activity_run_id=retry_activities[0]["activity_run_id"],
                entity_type="crm_public_web_run",
            )
            self.assertEqual(len(retry_deltas), 1)
            self.assertEqual(retry_deltas[0]["status"], "not_applied")
            self.assertEqual(retry_deltas[0]["reason"], "crm_public_web_remote_search_not_ready")
            self.assertEqual(resumed["status"], "queued")
            self.assertEqual(resumed["workflow_command"]["status"], "queued")
            self.assertEqual(resumed["workflow_command"]["result"]["control_action"], "resume")
            self.assertEqual(resumed["control_policy"]["command_type"], "crm.public_web.search.poll_fetch")
            self.assertEqual(resumed["activity_spine_policy"]["command_type"], "crm.public_web.search.poll_fetch")
            self.assertEqual(resumed["activity_spine_policy"]["requirement"], "activity_attempt_entity_delta_required")

            running_command = api_store.upsert_workflow_command(
                workflow_run_id="wf-public-web-phase-control",
                command_type="crm.public_web.search.poll_fetch",
                owner="crm_public_web_owner",
                idempotency_key="wf-public-web-phase-control:poll-fetch-running",
                payload={
                    "run_id": "run-public-web-phase-control-running",
                    "batch_id": "batch-public-web-phase-control",
                    "workspace_id": "default",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(running_command["command_id"], lease_owner="poll-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(running_command["command_id"], lease_owner="poll-worker")
            running_before_resume = orchestrator.get_workflow_command_api(running_command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                running_command["command_id"],
                {"actor": "unit-test"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                running_command["command_id"],
                {"actor": "unit-test", "force": True, "reason": "operator-reviewed-stale-poll"},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertEqual(running_before_resume["workflow_command"]["control_state"]["resume_mode"], "owner_specific")
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "crm_public_web_owner.resume_crm_public_web_phase_command",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["result"]["control_action"], "resume")
            self.assertEqual(forced_resume["workflow_command"]["result"]["resume_mode"], "owner_specific_requeue")
            self.assertEqual(forced_resume["operation_sync"]["status"], "skipped")
            forced_deltas = api_store.list_workflow_entity_deltas(
                command_id=running_command["command_id"],
                entity_type="crm_public_web_run",
            )
            self.assertEqual(len(forced_deltas), 1)
            self.assertEqual(forced_deltas[0]["status"], "queued")
            self.assertEqual(
                forced_deltas[0]["reason"],
                "crm_public_web_phase_command_resumed_by_command_control",
            )

            documents_command = api_store.upsert_workflow_command(
                workflow_run_id="wf-public-web-phase-control",
                command_type=CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                owner="crm_public_web_owner",
                idempotency_key="wf-public-web-phase-control:documents-fetch-running",
                payload={
                    "run_id": "run-public-web-phase-control-documents",
                    "batch_id": "batch-public-web-phase-control",
                    "workspace_id": "default",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(documents_command["command_id"], lease_owner="documents-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(documents_command["command_id"], lease_owner="documents-worker")
            documents_before_resume = orchestrator.get_workflow_command_api(documents_command["command_id"])
            documents_forced_resume = orchestrator.resume_workflow_command_api(
                documents_command["command_id"],
                {"actor": "unit-test", "force": True, "reason": "operator-reviewed-stale-documents"},
            )

            self.assertEqual(
                documents_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "crm_public_web_owner.resume_crm_public_web_phase_command",
            )
            self.assertEqual(documents_forced_resume["status"], "queued")
            self.assertEqual(documents_forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                documents_forced_resume["workflow_command"]["result"]["resume_mode"],
                "owner_specific_requeue",
            )
            documents_deltas = api_store.list_workflow_entity_deltas(
                command_id=documents_command["command_id"],
                entity_type="crm_public_web_run",
            )
            self.assertEqual(len(documents_deltas), 1)
            self.assertEqual(documents_deltas[0]["status"], "queued")
            self.assertEqual(
                documents_deltas[0]["reason"],
                "crm_public_web_phase_command_resumed_by_command_control",
            )
        finally:
            api_store.close()

    def test_orchestration_running_resume_requeues_without_inline_owner_execution(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "orchestration-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-orchestration-resume",
                operation_id="op-orchestration-resume",
                command_type=ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
                owner=ACQUISITION_PLAN_BUILD_OWNER,
                idempotency_key="acquisition.plan.build:resume",
                payload={"acquisition_run_id": "acqrun-resume", "target_company": "OpenAI"},
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="planner-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="planner-worker")

            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-orchestration", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "workflow_orchestrator.resume_orchestration_command",
            )
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "workflow_orchestrator.cancel_orchestration_before_downstream",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["orchestration_resume_policy"],
                "resume_requeues_without_inline_reducer_or_owner_execution",
            )
            self.assertEqual(
                forced_resume["workflow_entity_delta"]["entity_type"],
                "workflow_orchestration_command",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertTrue(
                forced_resume["workflow_entity_delta"]["projection_effect"]["orchestration_requeued"]
            )
        finally:
            api_store.close()

    def test_provider_attempt_running_resume_requeues_without_provider_call(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "provider-attempt-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-provider-resume",
                operation_id="op-provider-resume",
                command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                idempotency_key="linkedin.profile_fetch.provider.fetch:resume",
                payload={
                    "activity_run_id": "actrun-provider-resume",
                    "provider": "harvest",
                    "provider_request_ref": "provider-ref-1",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="provider-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="provider-worker")

            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-provider-attempt", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "workflow_provider_owner.cancel_or_poll_stop_provider_attempt",
            )
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "workflow_provider_owner.resume_provider_attempt_command",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["provider_attempt_resume_policy"],
                "resume_requeues_without_provider_call",
            )
            self.assertEqual(
                forced_resume["workflow_entity_delta"]["entity_type"],
                "workflow_provider_attempt_command",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertTrue(
                forced_resume["workflow_entity_delta"]["projection_effect"]["provider_attempt_requeued"]
            )
        finally:
            api_store.close()

    def test_provider_attempt_running_cancel_marks_activity_before_provider_attempt(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "provider-attempt-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-provider-cancel",
                operation_id="op-provider-cancel",
                command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                idempotency_key="linkedin.profile_fetch.provider.fetch:cancel",
                payload={"activity_run_id": "actrun-provider-cancel", "provider": "harvest"},
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-provider-cancel",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-provider-cancel",
                    "operation_run_id": "op-provider-cancel",
                    "command_id": command["command_id"],
                    "activity_type": LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                    "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                    "status": "planned",
                    "phase": "provider_attempt_pending",
                    "idempotency_key": "workflow_activity:provider-cancel",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="provider-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="provider-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator_cancelled_before_provider_attempt", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertTrue(response["owner_specific_control"])
            self.assertEqual(
                response["control_policy"]["running_cancel_delegate"],
                "workflow_provider_owner.cancel_or_poll_stop_provider_attempt",
            )
            self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
            self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "provider_attempt_before_activity_attempt")
            self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
            cancelled_activity = api_store.get_workflow_activity_run(activity["activity_run_id"])
            self.assertEqual(cancelled_activity["status"], "cancelled_before_provider_attempt")
            self.assertEqual(cancelled_activity["phase"], "cancelled")
            self.assertEqual(api_store.list_workflow_activity_attempts(activity_run_id=activity["activity_run_id"]), [])
        finally:
            api_store.close()

    def test_provider_attempt_running_cancel_poll_stops_after_provider_attempt_started(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "provider-attempt-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-provider-cancel-blocked",
                operation_id="op-provider-cancel-blocked",
                command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                idempotency_key="linkedin.profile_fetch.provider.fetch:cancel-blocked",
                payload={"activity_run_id": "actrun-provider-cancel-blocked", "provider": "harvest"},
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-provider-cancel-blocked",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-provider-cancel-blocked",
                    "operation_run_id": "op-provider-cancel-blocked",
                    "command_id": command["command_id"],
                    "activity_type": LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                    "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                    "status": "running",
                    "phase": "provider_attempt_started",
                    "idempotency_key": "workflow_activity:provider-cancel-blocked",
                }
            )
            api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt-provider-cancel-blocked",
                    "workspace_id": "default",
                    "activity_run_id": activity["activity_run_id"],
                    "workflow_run_id": "wf-provider-cancel-blocked",
                    "command_id": command["command_id"],
                    "attempt_number": 1,
                    "status": "running",
                    "provider": "harvest",
                    "idempotency_key": "workflow_activity_attempt:provider-cancel-blocked",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="provider-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="provider-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "too_late", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertEqual(
                response["workflow_command"]["result"]["cancel_boundary"],
                "provider_attempt_after_start_poll_cancel",
            )
            self.assertEqual(
                response["workflow_command"]["result"]["provider_after_start_control_mode"],
                "poll_cancel_late_result_quarantine",
            )
            self.assertEqual(response["workflow_command"]["result"]["activity_attempt_cancelled_count"], 1)
            self.assertFalse(response["workflow_command"]["result"]["remote_cancel_attempted"])
            self.assertEqual(
                response["workflow_command"]["result"]["late_result_policy"],
                "quarantine_as_ignored_evidence",
            )
            self.assertFalse(response["module_state_mutated"])
            self.assertEqual(
                api_store.get_workflow_activity_run(activity["activity_run_id"])["status"],
                "cancelled_poll_stopped",
            )
            self.assertEqual(
                api_store.get_workflow_activity_attempt("actattempt-provider-cancel-blocked")["status"],
                "cancelled_remote_ignored",
            )
        finally:
            api_store.close()

    def test_provider_attempt_running_cancel_blocks_after_entity_delta_recorded(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "provider-attempt-cancel-delta.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-provider-cancel-delta",
                operation_id="op-provider-cancel-delta",
                command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                idempotency_key="linkedin.profile_fetch.provider.fetch:cancel-delta",
                payload={"activity_run_id": "actrun-provider-cancel-delta", "provider": "harvest"},
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-provider-cancel-delta",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-provider-cancel-delta",
                    "operation_run_id": "op-provider-cancel-delta",
                    "command_id": command["command_id"],
                    "activity_type": LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                    "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                    "status": "running",
                    "phase": "provider_attempt_started",
                    "idempotency_key": "workflow_activity:provider-cancel-delta",
                }
            )
            api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt-provider-cancel-delta",
                    "workspace_id": "default",
                    "activity_run_id": activity["activity_run_id"],
                    "workflow_run_id": "wf-provider-cancel-delta",
                    "command_id": command["command_id"],
                    "attempt_number": 1,
                    "status": "running",
                    "provider": "harvest",
                    "idempotency_key": "workflow_activity_attempt:provider-cancel-delta",
                }
            )
            api_store.upsert_workflow_entity_delta(
                {
                    "workflow_run_id": "wf-provider-cancel-delta",
                    "operation_run_id": "op-provider-cancel-delta",
                    "command_id": command["command_id"],
                    "activity_run_id": activity["activity_run_id"],
                    "attempt_id": "actattempt-provider-cancel-delta",
                    "entity_type": "profile",
                    "entity_key": "linkedin:example",
                    "delta_kind": "provider_result_recorded",
                    "status": "recorded",
                    "idempotency_key": "workflow_entity_delta:provider-cancel-delta",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="provider-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="provider-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "too_late", "force": True},
            )

            self.assertEqual(response["status"], "invalid")
            self.assertEqual(response["reason"], "provider_attempt_cancel_blocked_after_provider_entity_delta_recorded")
            self.assertEqual(response["activity_attempt_count"], 1)
            self.assertEqual(response["entity_delta_count"], 1)
            self.assertFalse(response["module_state_mutated"])
            self.assertEqual(api_store.get_workflow_activity_run(activity["activity_run_id"])["status"], "running")
            self.assertEqual(
                api_store.get_workflow_activity_attempt("actattempt-provider-cancel-delta")["status"],
                "running",
            )
        finally:
            api_store.close()

    def test_operation_native_profile_provider_partial_success_uses_bucketed_retry_wave(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "operation-native-profile-partial.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            workflow_run_id = "wf-operation-native-profile-partial"
            operation_run_id = "op-operation-native-profile-partial"
            activity_run_id = "actrun-operation-native-profile-partial"
            profile_urls = [
                "https://www.linkedin.com/in/ada-lovelace/",
                "https://www.linkedin.com/in/grace-hopper/",
            ]
            api_store.upsert_operation_run(
                operation_run_id=operation_run_id,
                action_id="action-operation-native-profile-partial",
                owner_module=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                operation_type="continue_acquisition",
                status="running",
                progress={"phase": "operation_native_profile_provider_fetch_pending"},
                idempotency_key="operation-native-profile-partial",
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": activity_run_id,
                    "workspace_id": "default",
                    "workflow_run_id": workflow_run_id,
                    "operation_run_id": operation_run_id,
                    "command_id": "cmd-profile-fetch-activity-parent",
                    "activity_type": LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                    "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                    "status": "planned_pending_provider_owner",
                    "phase": "provider_profile_fetch_pending",
                    "input": {"profile_urls": profile_urls},
                    "output": {},
                    "entity_counts": {"profile_url_count": len(profile_urls), "fetch_required_count": len(profile_urls)},
                    "idempotency_key": "workflow_activity:operation-native-profile-partial",
                }
            )
            source_delta_ids: list[str] = []
            for profile_url in profile_urls:
                delta = api_store.upsert_workflow_entity_delta(
                    {
                        "workspace_id": "default",
                        "workflow_run_id": workflow_run_id,
                        "operation_run_id": operation_run_id,
                        "command_id": "cmd-profile-fetch-activity-parent",
                        "activity_run_id": activity_run_id,
                        "entity_type": "profile",
                        "entity_key": profile_url,
                        "delta_kind": "profile_fetch_required",
                        "status": "not_applied",
                        "reason": "unit_test_fetch_required",
                        "entity_payload": {"profile_url": profile_url},
                        "projection_effect": {"entered_projection": False},
                        "idempotency_key": f"workflow_entity_delta:partial:required:{profile_url}",
                    }
                )
                source_delta_ids.append(delta["delta_id"])
            command = api_store.upsert_workflow_command(
                workflow_run_id=workflow_run_id,
                operation_id=operation_run_id,
                command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                idempotency_key="linkedin.profile_fetch.provider.fetch:partial-normal",
                payload={
                    "runtime_execution_mode": "operation_native_profile_provider_fetch",
                    "source_profile_activity_run_id": activity["activity_run_id"],
                    "operation_run_id": operation_run_id,
                    "target_company": "OpenAI",
                    "profile_urls": profile_urls,
                    "source_entity_delta_ids": source_delta_ids,
                    "provider_attempt_scope": "normal",
                    "retry_wave_index": 0,
                    "profile_retry_budget": 1,
                    "legacy_job_shell_created": False,
                    "queue_workflow_called": False,
                    "normal_path_executes_legacy_profile_refill_owner": False,
                },
                max_attempts=2,
                retry_policy={
                    "kind": "operation_native_profile_provider_fetch",
                    "retry_strategy": "bucketed_entity_retry_after_normal_wave",
                },
            )

            def _fake_fetch_profile(slug, snapshot_dir, *, asset_logger=None):
                if slug == "grace-hopper":
                    return None
                profile_dir = Path(snapshot_dir) / "profiles"
                profile_dir.mkdir(parents=True, exist_ok=True)
                raw_path = profile_dir / f"{slug}.json"
                raw_path.write_text(json.dumps({"profile": {"public_identifier": slug}}), encoding="utf-8")
                return {"raw_path": raw_path, "parsed": {"public_identifier": slug}}

            with mock.patch.object(
                orchestrator.acquisition_engine.multi_source_enricher.profile_connector,
                "fetch_profile",
                side_effect=_fake_fetch_profile,
            ) as fetch_profile:
                result = orchestrator._drain_operation_native_profile_fetch_activity_commands(  # noqa: SLF001
                    {
                        "workflow_run_id": workflow_run_id,
                        "operation_native_profile_fetch_command_limit": 5,
                    }
                )

            fetch_profile.assert_has_calls(
                [
                    mock.call("ada-lovelace", mock.ANY),
                    mock.call("grace-hopper", mock.ANY),
                ],
                any_order=False,
            )
            self.assertEqual(result["completed_count"], 1)
            self.assertEqual(result["failed_count"], 0)
            completed_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(completed_command["status"], "succeeded")
            self.assertEqual(
                completed_command["result"]["reason"],
                "operation_native_profile_provider_fetch_partial_retry_planned",
            )
            self.assertEqual(completed_command["result"]["fetched_urls"], [profile_urls[0]])
            self.assertEqual(completed_command["result"]["failed_urls"], [profile_urls[1]])
            self.assertEqual(completed_command["result"]["retry_strategy"], "bucketed_entity_retry_after_normal_wave")
            self.assertEqual(completed_command["result"]["downstream_command_count"], 2)

            commands = api_store.list_workflow_commands(workflow_run_id=workflow_run_id, limit=20)
            terminal_commands = [
                item for item in commands if item["command_type"] == LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE
            ]
            retry_commands = [
                item
                for item in commands
                if item["command_type"] == LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE
                and item["command_id"] != command["command_id"]
            ]
            self.assertEqual(len(terminal_commands), 1)
            self.assertEqual(terminal_commands[0]["payload"]["profile_urls"], [profile_urls[0]])
            self.assertEqual(terminal_commands[0]["status"], "queued")
            self.assertEqual(len(retry_commands), 1)
            self.assertEqual(retry_commands[0]["payload"]["profile_urls"], [profile_urls[1]])
            self.assertEqual(retry_commands[0]["payload"]["provider_attempt_scope"], "retry_wave")
            self.assertEqual(retry_commands[0]["payload"]["retry_wave_index"], 1)
            self.assertEqual(retry_commands[0]["max_attempts"], 1)

            activity_after = api_store.get_workflow_activity_run(activity_run_id)
            self.assertEqual(activity_after["status"], "partial_success")
            self.assertEqual(activity_after["phase"], "provider_profile_fetch_partial_retry_planned")
            self.assertEqual(activity_after["metadata"]["retry_wave_planned"], True)
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activity_run_id)
            self.assertEqual([attempt["status"] for attempt in attempts], ["partial_success"])
            deltas = api_store.list_workflow_entity_deltas(activity_run_id=activity_run_id, entity_type="profile")
            delta_kinds = {delta["delta_kind"] for delta in deltas}
            self.assertIn("profile_provider_fetched", delta_kinds)
            self.assertIn("profile_provider_retry_bucketed", delta_kinds)
            retry_delta = next(delta for delta in deltas if delta["delta_kind"] == "profile_provider_retry_bucketed")
            self.assertEqual(retry_delta["status"], "retry_wait")
            self.assertEqual(retry_delta["entity_payload"]["profile_url"], profile_urls[1])
        finally:
            api_store.close()

    def test_domain_mutation_running_resume_requeues_without_request_path_write(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "domain-mutation-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-domain-resume",
                operation_id="op-domain-resume",
                command_type=PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                owner=PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
                idempotency_key="projection.person_search_index.build:resume",
                payload={
                    "projection_id": "proj-domain-resume",
                    "collection_id": "company:openai",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="projection-index-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="projection-index-worker")

            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-domain-mutation", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "workflow_domain_owner.cancel_before_domain_mutation_attempt",
            )
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "workflow_domain_owner.resume_domain_mutation_command",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["domain_mutation_resume_policy"],
                "resume_requeues_without_request_path_domain_write",
            )
            self.assertEqual(
                forced_resume["workflow_entity_delta"]["entity_type"],
                "workflow_domain_mutation_command",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertTrue(
                forced_resume["workflow_entity_delta"]["projection_effect"]["domain_mutation_requeued"]
            )
            self.assertEqual(
                api_store.count_projection_person_search_index(projection_id="proj-domain-resume"),
                0,
            )
        finally:
            api_store.close()

    def test_domain_mutation_running_cancel_marks_activity_before_attempt(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "domain-mutation-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-domain-cancel",
                operation_id="op-domain-cancel",
                command_type=PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                owner=PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
                idempotency_key="projection.person_search_index.build:cancel",
                payload={
                    "projection_id": "proj-domain-cancel",
                    "collection_id": "company:openai",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-domain-cancel",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-domain-cancel",
                    "operation_run_id": "op-domain-cancel",
                    "command_id": command["command_id"],
                    "activity_type": PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                    "owner": PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
                    "status": "planned",
                    "phase": "projection_index_pending",
                    "idempotency_key": "workflow_activity:domain-cancel",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="projection-index-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="projection-index-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator_cancelled_before_projection_index", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertTrue(response["owner_specific_control"])
            self.assertEqual(
                response["control_policy"]["running_cancel_delegate"],
                "workflow_domain_owner.cancel_before_domain_mutation_attempt",
            )
            self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
            self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "domain_mutation_before_attempt")
            self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
            cancelled_activity = api_store.get_workflow_activity_run(activity["activity_run_id"])
            self.assertEqual(cancelled_activity["status"], "cancelled_before_domain_mutation")
            self.assertEqual(cancelled_activity["phase"], "cancelled")
            self.assertEqual(api_store.count_projection_person_search_index(projection_id="proj-domain-cancel"), 0)
        finally:
            api_store.close()

    def test_domain_mutation_running_cancel_blocks_after_attempt_started(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "domain-mutation-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-domain-cancel-blocked",
                operation_id="op-domain-cancel-blocked",
                command_type=PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                owner=PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
                idempotency_key="projection.person_search_index.build:cancel-blocked",
                payload={
                    "projection_id": "proj-domain-cancel-blocked",
                    "collection_id": "company:openai",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-domain-cancel-blocked",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-domain-cancel-blocked",
                    "operation_run_id": "op-domain-cancel-blocked",
                    "command_id": command["command_id"],
                    "activity_type": PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                    "owner": PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
                    "status": "running",
                    "phase": "projection_index_started",
                    "idempotency_key": "workflow_activity:domain-cancel-blocked",
                }
            )
            api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt-domain-cancel-blocked",
                    "workspace_id": "default",
                    "activity_run_id": activity["activity_run_id"],
                    "workflow_run_id": "wf-domain-cancel-blocked",
                    "command_id": command["command_id"],
                    "attempt_number": 1,
                    "status": "running",
                    "provider": "projection_index_owner",
                    "idempotency_key": "workflow_activity_attempt:domain-cancel-blocked",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="projection-index-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="projection-index-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "too_late", "force": True},
            )

            self.assertEqual(response["status"], "invalid")
            self.assertEqual(response["reason"], "domain_mutation_cancel_blocked_after_attempt_started")
            self.assertEqual(response["activity_attempt_count"], 1)
            self.assertFalse(response["module_state_mutated"])
            self.assertEqual(api_store.get_workflow_activity_run(activity["activity_run_id"])["status"], "running")
            self.assertEqual(
                api_store.count_projection_person_search_index(projection_id="proj-domain-cancel-blocked"),
                0,
            )
        finally:
            api_store.close()

    def test_excel_intake_running_resume_requeues_without_thread_start(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "excel-intake-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-excel-resume",
                operation_id="op-excel-resume",
                command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
                owner=EXCEL_INTAKE_RUN_OWNER,
                idempotency_key="excel.intake.run:resume",
                payload={"job_id": "excel-job-resume"},
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="excel-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="excel-worker")

            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-excel-thread", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "excel_intake_owner.resume_excel_intake_run_command",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["excel_intake_resume_policy"],
                "resume_requeues_without_request_path_thread_start",
            )
            self.assertEqual(
                forced_resume["workflow_entity_delta"]["entity_type"],
                "excel_intake_command",
            )
            self.assertTrue(
                forced_resume["workflow_entity_delta"]["projection_effect"]["excel_intake_requeued"]
            )
        finally:
            api_store.close()

    def test_crm_public_web_phase_command_noops_stale_superseded_batch(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-stale-batch.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(AssetCatalog.discover(), settings, self.store, DeterministicModelClient()),
        )
        self.store.upsert_crm_public_web_run(
            {
                "run_id": "run-public-web-stale-batch",
                "batch_id": "batch-current",
                "crm_record_id": "crmrec-stale-batch",
                "workspace_id": "default",
                "status": "queued",
                "phase": "queued",
                "execution_backend": "crm_public_web_v1",
            }
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-public-web-stale-batch",
            operation_id="op-public-web-stale-batch",
            command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
            owner="crm_public_web_owner",
            idempotency_key="wf-public-web-stale-batch:search-submit",
            payload={
                "run_id": "run-public-web-stale-batch",
                "batch_id": "batch-old",
                "workspace_id": "default",
            },
        )

        with mock.patch(
            "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
            side_effect=AssertionError("stale batch command must not execute runtime core"),
        ):
            result = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["no_op_reason"], "crm_public_web_phase_stale_batch_superseded")
        succeeded = self.store.get_workflow_command(command["command_id"])
        self.assertEqual(succeeded["status"], "succeeded")
        self.assertEqual(succeeded["result"]["current_batch_id"], "batch-current")
        self.assertEqual(succeeded["result"]["downstream_command_ids"], [])
        self.assertEqual(
            self.store.get_crm_public_web_run(run_id="run-public-web-stale-batch")["batch_id"],
            "batch-current",
        )

    def test_crm_public_web_phase_command_noops_stale_superseded_run(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-stale-run.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(AssetCatalog.discover(), settings, self.store, DeterministicModelClient()),
        )
        self.store.upsert_crm_public_web_run(
            {
                "run_id": "run-public-web-stale-run-old",
                "batch_id": "batch-stale-run-old",
                "crm_record_id": "crmrec-stale-run",
                "workspace_id": "default",
                "status": "queued",
                "phase": "queued",
                "execution_backend": "crm_public_web_v1",
                "idempotency_key": "crm-public-web-run:stale-run-old",
                "created_at": "2026-04-01T00:00:00Z",
            }
        )
        self.store.upsert_crm_public_web_run(
            {
                "run_id": "run-public-web-stale-run-new",
                "batch_id": "batch-stale-run-new",
                "crm_record_id": "crmrec-stale-run",
                "workspace_id": "default",
                "status": "queued",
                "phase": "queued",
                "execution_backend": "crm_public_web_v1",
                "idempotency_key": "crm-public-web-run:stale-run-new",
                "created_at": "2026-04-02T00:00:00Z",
            }
        )
        self.store.update_crm_public_web_run(
            "run-public-web-stale-run-old",
            {"status": "searching", "phase": "searching"},
        )
        latest = self.store.list_crm_public_web_runs(
            crm_record_id="crmrec-stale-run",
            workspace_id="default",
            limit=1,
        )
        self.assertEqual(latest[0]["run_id"], "run-public-web-stale-run-new")
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-public-web-stale-run",
            operation_id="op-public-web-stale-run",
            command_type=CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
            owner="crm_public_web_owner",
            idempotency_key="wf-public-web-stale-run:poll-fetch",
            payload={
                "run_id": "run-public-web-stale-run-old",
                "batch_id": "batch-stale-run-old",
                "workspace_id": "default",
            },
        )

        with mock.patch(
            "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
            side_effect=AssertionError("stale run command must not execute runtime core"),
        ):
            result = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["no_op_reason"], "crm_public_web_phase_stale_run_superseded")
        self.assertEqual(result["latest_run_id"], "run-public-web-stale-run-new")
        succeeded = self.store.get_workflow_command(command["command_id"])
        self.assertEqual(succeeded["status"], "succeeded")
        self.assertEqual(succeeded["result"]["no_op_reason"], "crm_public_web_phase_stale_run_superseded")
        self.assertEqual(succeeded["result"]["downstream_command_ids"], [])

    def test_crm_public_web_phase_command_advanced_state_plans_missing_downstream_without_reexecution(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-advanced-phase.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(AssetCatalog.discover(), settings, self.store, DeterministicModelClient()),
        )
        self.store.upsert_crm_public_web_run(
            {
                "run_id": "run-public-web-advanced-phase",
                "batch_id": "batch-advanced",
                "crm_record_id": "crmrec-advanced-phase",
                "workspace_id": "default",
                "status": "documents_fetched",
                "phase": "documents_fetched",
                "analysis_checkpoint": {"document_fetch_payload_path": "artifact://already-fetched.json"},
                "execution_backend": "crm_public_web_v1",
            }
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-public-web-advanced-phase",
            operation_id="op-public-web-advanced-phase",
            command_type=CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
            owner="crm_public_web_owner",
            idempotency_key="wf-public-web-advanced-phase:documents-fetch",
            payload={
                "run_id": "run-public-web-advanced-phase",
                "batch_id": "batch-advanced",
                "workspace_id": "default",
            },
        )

        with mock.patch(
            "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
            side_effect=AssertionError("advanced phase command must not reexecute runtime core"),
        ):
            result = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["no_op_reason"], "crm_public_web_phase_status_already_advanced")
        self.assertEqual(result["next_command_type"], CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE)
        self.assertEqual(len(result["downstream_command_ids"]), 1)
        downstream = self.store.get_workflow_command(result["downstream_command_ids"][0])
        self.assertEqual(downstream["command_type"], CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE)
        self.assertEqual(downstream["payload"]["batch_id"], "batch-advanced")

    def test_crm_public_web_phase_command_waits_when_prerequisite_status_not_met(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-prerequisite-wait.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=self.store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(AssetCatalog.discover(), settings, self.store, DeterministicModelClient()),
        )
        self.store.upsert_crm_public_web_run(
            {
                "run_id": "run-public-web-prerequisite-wait",
                "batch_id": "batch-prerequisite",
                "crm_record_id": "crmrec-prerequisite-wait",
                "workspace_id": "default",
                "status": "documents_fetched",
                "phase": "documents_fetched",
                "execution_backend": "crm_public_web_v1",
            }
        )
        command = self.store.upsert_workflow_command(
            workflow_run_id="wf-public-web-prerequisite-wait",
            operation_id="op-public-web-prerequisite-wait",
            command_type=CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
            owner="crm_public_web_owner",
            idempotency_key="wf-public-web-prerequisite-wait:model-safe-finalize",
            payload={
                "run_id": "run-public-web-prerequisite-wait",
                "batch_id": "batch-prerequisite",
                "workspace_id": "default",
            },
        )

        with mock.patch(
            "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
            side_effect=AssertionError("prerequisite-missing command must not execute runtime core"),
        ):
            result = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

        self.assertEqual(result["status"], "deferred")
        self.assertEqual(result["reason"], "crm_public_web_phase_prerequisite_not_met")
        self.assertEqual(result["status_relation"], "prerequisite_not_met")
        waiting = self.store.get_workflow_command(command["command_id"])
        self.assertEqual(waiting["status"], "retry_wait")
        self.assertEqual(waiting["result"]["expected_statuses"], ["adjudication_completed"])

    def test_crm_public_web_signal_materialize_phase_records_signal_entity_deltas(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-signal-deltas.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_crm_public_web_run(
                {
                    "run_id": "run-public-web-signal-deltas",
                    "batch_id": "batch-public-web-signal-deltas",
                    "workspace_id": "default",
                    "crm_record_id": "crmrec-public-web-signal-deltas",
                    "status": "analysis_completed",
                    "phase": "analysis_completed",
                    "execution_backend": "crm_public_web_v1",
                }
            )
            api_store.upsert_person_public_web_signal(
                {
                    "signal_id": "signal-public-web-materialized-1",
                    "run_id": "run-public-web-signal-deltas",
                    "record_id": "crmrec-public-web-signal-deltas",
                    "person_identity_key": "linkedin:public-web-signal-deltas",
                    "signal_kind": "email_candidate",
                    "signal_type": "work",
                    "value": "person@example.com",
                    "normalized_value": "person@example.com",
                    "source_url": "https://example.com/person",
                    "source_domain": "example.com",
                    "confidence_label": "high",
                    "confidence_score": 0.92,
                    "identity_match_label": "strong",
                    "identity_match_score": 0.9,
                    "publishable": True,
                    "promotion_status": "not_promoted",
                    "artifact_refs": {"model_safe": "artifact://public-web/model-safe.json"},
                    "model_provider": "qwen",
                    "model_version": "qwen3.5-plus-2026-04-20",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-public-web-signal-deltas",
                operation_id="op-public-web-signal-deltas",
                command_type="crm.public_web.signals.materialize",
                owner="crm_public_web_owner",
                idempotency_key="wf-public-web-signal-deltas:signals-materialize",
                payload={
                    "run_id": "run-public-web-signal-deltas",
                    "batch_id": "batch-public-web-signal-deltas",
                    "workspace_id": "default",
                },
                max_attempts=6,
            )
            with mock.patch(
                "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
                return_value={
                    "worker_status": "completed",
                    "run_status": "completed",
                    "summary": {"phase_metrics": {"signal_materialized_count": 1}},
                },
            ) as execute_phase:
                result = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

            self.assertEqual(execute_phase.call_args.kwargs["phase_command_type"], "crm.public_web.signals.materialize")
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["signal_entity_delta_count"], 1)
            self.assertEqual(result["person_asset_sync_asset_count"], 1)
            self.assertEqual(result["person_asset_sync_evidence_count"], 1)
            self.assertEqual(result["person_asset_sync_entity_delta_count"], 2)
            succeeded = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(succeeded["status"], "succeeded")
            self.assertEqual(succeeded["result"]["signal_entity_delta_count"], 1)
            self.assertEqual(succeeded["result"]["person_asset_sync_asset_count"], 1)
            self.assertEqual(succeeded["result"]["person_asset_sync_evidence_count"], 1)
            signal_deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="public_web_signal",
                entity_key="signal-public-web-materialized-1",
            )
            self.assertEqual(len(signal_deltas), 1)
            self.assertEqual(signal_deltas[0]["delta_kind"], "crm_public_web_signal_materialized")
            self.assertEqual(signal_deltas[0]["reason"], "crm_public_web_signals_materialized")
            self.assertEqual(signal_deltas[0]["entity_payload"]["normalized_value"], "person@example.com")
            self.assertEqual(
                signal_deltas[0]["projection_effect"]["reason"],
                "public_web_signal_materialized_not_projection_membership",
            )
            person_assets = api_store.list_person_assets(
                person_identity_key="linkedin:public-web-signal-deltas",
                asset_type="public_web_signal",
            )
            self.assertEqual(len(person_assets), 1)
            self.assertEqual(person_assets[0]["source_kind"], "crm_public_web_signal")
            self.assertEqual(person_assets[0]["metadata"]["writer_id"], "crm_public_web_signal_materialize_v1")
            person_evidence = api_store.list_person_evidence(
                person_identity_key="linkedin:public-web-signal-deltas",
            )
            self.assertEqual(len(person_evidence), 1)
            self.assertEqual(person_evidence[0]["evidence_type"], "email_candidate")
            self.assertEqual(person_evidence[0]["normalized_value"], "person@example.com")
            self.assertTrue(person_evidence[0]["publishable"])
            evidence_deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="person_evidence",
                entity_key=person_evidence[0]["evidence_id"],
            )
            self.assertEqual(len(evidence_deltas), 1)
            self.assertEqual(evidence_deltas[0]["delta_kind"], "crm_public_web_signal_evidence_synced")
            self.assertEqual(
                evidence_deltas[0]["metadata"]["person_asset_sync_contract"],
                "crm_public_web_signal_person_asset_sync_v1",
            )
        finally:
            api_store.close()

    def test_public_web_signal_person_asset_backfill_is_explicit_and_dry_run_first(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-signal-asset-backfill.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_person_public_web_signal(
                {
                    "signal_id": "signal-public-web-historical-1",
                    "run_id": "run-historical-public-web-signal",
                    "record_id": "crmrec-historical-public-web-signal",
                    "person_identity_key": "linkedin:historical-public-web-signal",
                    "signal_kind": "email_candidate",
                    "signal_type": "work",
                    "value": "historical@example.com",
                    "normalized_value": "historical@example.com",
                    "source_url": "https://example.com/historical-person",
                    "source_domain": "example.com",
                    "confidence_label": "high",
                    "confidence_score": 0.91,
                    "identity_match_label": "strong",
                    "identity_match_score": 0.89,
                    "publishable": True,
                    "promotion_status": "not_promoted",
                    "artifact_refs": {"model_safe": "artifact://public-web/historical-model-safe.json"},
                    "model_provider": "qwen",
                    "model_version": "qwen3.5-plus-2026-04-20",
                }
            )

            dry_run = orchestrator.backfill_public_web_signals_to_person_asset_layer(
                {
                    "run_id": "run-historical-public-web-signal",
                    "dry_run": True,
                }
            )

            self.assertEqual(dry_run["status"], "dry_run")
            self.assertEqual(dry_run["eligible_signal_count"], 1)
            self.assertFalse(dry_run["module_state_mutated"])
            self.assertFalse(dry_run["read_contract"]["normal_reader_repair"])
            self.assertFalse(dry_run["read_contract"]["fallback_used"])
            self.assertEqual(
                api_store.list_person_assets(
                    person_identity_key="linkedin:historical-public-web-signal",
                    asset_type="public_web_signal",
                ),
                [],
            )
            self.assertEqual(
                api_store.list_person_evidence(person_identity_key="linkedin:historical-public-web-signal"),
                [],
            )

            rejected = orchestrator.backfill_public_web_signals_to_person_asset_layer(
                {
                    "run_id": "run-historical-public-web-signal",
                    "dry_run": False,
                    "batch_id": "historical-public-web-signal-asset-backfill",
                }
            )

            self.assertEqual(rejected["status"], "invalid")
            self.assertEqual(rejected["reason"], "operator_review_required")
            self.assertFalse(rejected["module_state_mutated"])
            self.assertTrue(rejected["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(
                api_store.list_person_assets(
                    person_identity_key="linkedin:historical-public-web-signal",
                    asset_type="public_web_signal",
                ),
                [],
            )
            self.assertEqual(
                api_store.list_person_evidence(person_identity_key="linkedin:historical-public-web-signal"),
                [],
            )

            applied = orchestrator.backfill_public_web_signals_to_person_asset_layer(
                {
                    "run_id": "run-historical-public-web-signal",
                    "dry_run": False,
                    "reviewed": True,
                    "batch_id": "historical-public-web-signal-asset-backfill",
                }
            )

            self.assertEqual(applied["status"], "backfilled")
            self.assertTrue(applied["migration_path"])
            self.assertTrue(applied["module_state_mutated"])
            self.assertEqual(applied["asset_count"], 1)
            self.assertEqual(applied["evidence_count"], 1)
            self.assertEqual(applied["entity_delta_count"], 0)
            self.assertEqual(applied["read_contract"]["source"], "person_public_web_signals")
            self.assertEqual(applied["read_contract"]["target"], "PersonAsset.public_web_signal+PersonEvidence")
            self.assertFalse(applied["read_contract"]["normal_reader_repair"])
            person_assets = api_store.list_person_assets(
                person_identity_key="linkedin:historical-public-web-signal",
                asset_type="public_web_signal",
            )
            self.assertEqual(len(person_assets), 1)
            self.assertEqual(person_assets[0]["source_kind"], "crm_public_web_signal")
            self.assertEqual(person_assets[0]["source_run_id"], "run-historical-public-web-signal")
            self.assertEqual(person_assets[0]["metadata"]["writer_id"], "crm_public_web_signal_materialize_v1")
            self.assertEqual(person_assets[0]["metadata"]["batch_id"], "historical-public-web-signal-asset-backfill")
            person_evidence = api_store.list_person_evidence(
                person_identity_key="linkedin:historical-public-web-signal",
            )
            self.assertEqual(len(person_evidence), 1)
            self.assertEqual(person_evidence[0]["asset_id"], person_assets[0]["asset_id"])
            self.assertEqual(person_evidence[0]["evidence_type"], "email_candidate")
            self.assertEqual(person_evidence[0]["normalized_value"], "historical@example.com")
            self.assertTrue(person_evidence[0]["publishable"])
        finally:
            api_store.close()

    def test_crm_public_web_documents_fetch_phase_records_document_entity_deltas(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "public-web-document-deltas.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            payload_path = self.runtime_dir / "public_web" / "document_fetch_payload.json"
            payload_path.parent.mkdir(parents=True, exist_ok=True)
            payload_path.write_text(
                json.dumps(
                    {
                        "fetched_documents": [
                            {
                                "source_url": "https://example.com/profile",
                                "final_url": "https://example.com/profile",
                                "title": "Example profile",
                                "source_family": "homepage",
                                "source_domain": "example.com",
                                "content_type": "text/html",
                                "status_code": 200,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            api_store.upsert_crm_public_web_run(
                {
                    "run_id": "run-public-web-document-deltas",
                    "batch_id": "batch-public-web-document-deltas",
                    "crm_record_id": "crmrec-public-web-document-deltas",
                    "workspace_id": "default",
                    "status": "entry_links_ready",
                    "phase": "entry_links_ready",
                    "analysis_checkpoint": {"document_fetch_payload_path": str(payload_path)},
                    "execution_backend": "crm_public_web_v1",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-public-web-document-deltas",
                operation_id="op-public-web-document-deltas",
                command_type="crm.public_web.documents.fetch",
                owner="crm_public_web_owner",
                idempotency_key="wf-public-web-document-deltas:documents-fetch",
                payload={
                    "run_id": "run-public-web-document-deltas",
                    "batch_id": "batch-public-web-document-deltas",
                    "workspace_id": "default",
                },
                max_attempts=6,
            )
            with mock.patch(
                "sourcing_agent.crm_public_web_owner.execute_crm_public_web_run_once",
                return_value={
                    "worker_status": "running",
                    "run_status": "documents_fetched",
                    "summary": {"phase_metrics": {"fetched_document_count": 1}},
                },
            ) as execute_phase:
                result = orchestrator._run_crm_public_web_phase_command(command)  # noqa: SLF001

            self.assertEqual(execute_phase.call_args.kwargs["phase_command_type"], "crm.public_web.documents.fetch")
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["document_entity_delta_count"], 1)
            succeeded = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(succeeded["result"]["document_entity_delta_count"], 1)
            document_deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="public_web_document",
            )
            self.assertEqual(len(document_deltas), 1)
            run_deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="crm_public_web_run",
            )
            self.assertEqual(len(run_deltas), 1)
            self.assertEqual(document_deltas[0]["delta_kind"], "crm_public_web_document_fetched")
            self.assertEqual(document_deltas[0]["reason"], "crm_public_web_documents_fetched")
            self.assertEqual(document_deltas[0]["entity_payload"]["source_url"], "https://example.com/profile")
            self.assertEqual(
                document_deltas[0]["projection_effect"]["reason"],
                "public_web_document_fetched_not_projection_membership",
            )
        finally:
            api_store.close()

    def test_fetch_profile_sample_operation_plans_profile_fetch_activity_command_only(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-sample-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_FETCH_PROFILE_SAMPLE,
                    "target_ref": {"workflow_run_id": "wf_profile_sample"},
                    "input": {
                        "profile_urls": [
                            "https://www.linkedin.com/in/ada/",
                            "https://www.linkedin.com/in/ada/",
                            "https://www.linkedin.com/in/grace/",
                        ]
                    },
                    "budget": {"max_provider_calls": 2, "max_usd": 1.0},
                    "idempotency_key": "profile-sample:job-profile-sample",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "planned")
            self.assertEqual(
                dispatched["workflow_command"]["command_type"],
                LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            )
            self.assertEqual(dispatched["workflow_command"]["owner"], LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER)
            self.assertEqual(dispatched["workflow_command"]["operation_id"], operation_run_id)
            self.assertEqual(dispatched["workflow_command"]["workflow_run_id"], "wf_profile_sample")
            self.assertEqual(dispatched["workflow_command"]["payload"]["profile_url_count"], 2)
            self.assertFalse(dispatched["workflow_command"]["payload"]["legacy_job_shell_created"])
            self.assertFalse(dispatched["workflow_command"]["payload"]["queue_workflow_called"])
            self.assertFalse(
                dispatched["workflow_command"]["payload"]["normal_path_executes_legacy_profile_refill_owner"]
            )
            self.assertNotIn("job_id", dispatched["workflow_command"]["payload"])
            self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "planned")
            self.assertEqual(api_store.get_agent_action(approved["action"]["action_id"])["status"], "planned")
            self.assertFalse(dispatched["module_state_mutated"])
            self.assertEqual(api_store.list_jobs(), [])
        finally:
            api_store.close()

    def test_start_acquisition_operation_plans_root_acquisition_run_command_only(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "acquisition-command-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_START_ACQUISITION_RUN,
                    "target_ref": {"target_company": "OpenAI"},
                    "input": {
                        "query": "site:linkedin.com/in OpenAI research engineer",
                        "limit": 25,
                    },
                    "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                    "idempotency_key": "discovery:job-discovery",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "planned")
            self.assertEqual(dispatched["workflow_command"]["command_type"], ACQUISITION_RUN_CREATE_COMMAND_TYPE)
            self.assertEqual(dispatched["workflow_command"]["owner"], "acquisition_run_writer")
            self.assertEqual(dispatched["workflow_command"]["operation_id"], operation_run_id)
            self.assertEqual(dispatched["workflow_command"]["stage_id"], "acquisition_run_create")
            self.assertEqual(
                dispatched["workflow_command"]["payload"]["workflow_payload"]["target_company"],
                "OpenAI",
            )
            self.assertEqual(
                dispatched["workflow_command"]["payload"]["workflow_payload"]["query"],
                "site:linkedin.com/in OpenAI research engineer",
            )
            self.assertFalse(dispatched["workflow_command"]["payload"]["decomposition_contract"]["normal_path_executes_queue_workflow_inline"])
            self.assertEqual(api_store.list_jobs(), [])
            self.assertFalse(dispatched["module_state_mutated"])

            owner_result = orchestrator._drain_acquisition_run_create_commands(  # noqa: SLF001
                {"acquisition_run_create_command_limit": 5}
            )

            self.assertEqual(owner_result["status"], "completed")
            self.assertEqual(owner_result["completed_count"], 1)
            terminal_command = api_store.get_workflow_command(dispatched["workflow_command"]["command_id"])
            self.assertEqual(terminal_command["status"], "succeeded")
            self.assertTrue(terminal_command["result"]["operation_completion_deferred"])
            self.assertFalse(terminal_command["result"]["queue_workflow_called"])
            self.assertFalse(terminal_command["result"]["legacy_job_shell_created"])
            self.assertEqual(terminal_command["result"]["next_phase"], "W11b_acquisition_intent_plan_commands")
            self.assertEqual(terminal_command["result"]["downstream_command_count"], 1)
            self.assertEqual(len(terminal_command["result"]["downstream_command_ids"]), 1)
            downstream_command = api_store.get_workflow_command(terminal_command["result"]["downstream_command_ids"][0])
            self.assertEqual(downstream_command["command_type"], ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE)
            self.assertEqual(downstream_command["owner"], ACQUISITION_INTENT_RESOLVE_OWNER)
            self.assertEqual(downstream_command["parent_command_id"], terminal_command["command_id"])
            self.assertEqual(downstream_command["status"], "queued")

            intent_owner_result = orchestrator._drain_acquisition_intent_resolve_commands(  # noqa: SLF001
                {"acquisition_intent_resolve_command_limit": 5}
            )

            self.assertEqual(intent_owner_result["status"], "completed")
            self.assertEqual(intent_owner_result["completed_count"], 1)
            terminal_intent_command = api_store.get_workflow_command(downstream_command["command_id"])
            self.assertEqual(terminal_intent_command["status"], "succeeded")
            self.assertFalse(terminal_intent_command["result"]["provider_called"])
            self.assertFalse(terminal_intent_command["result"]["queue_workflow_called"])
            self.assertEqual(terminal_intent_command["result"]["resolved_intent"]["target_company"], "OpenAI")
            self.assertEqual(terminal_intent_command["result"]["downstream_command_count"], 1)
            plan_command = api_store.get_workflow_command(terminal_intent_command["result"]["downstream_command_ids"][0])
            self.assertEqual(plan_command["command_type"], ACQUISITION_PLAN_BUILD_COMMAND_TYPE)
            self.assertEqual(plan_command["owner"], ACQUISITION_PLAN_BUILD_OWNER)
            self.assertEqual(plan_command["parent_command_id"], terminal_intent_command["command_id"])
            self.assertEqual(plan_command["status"], "queued")

            plan_owner_result = orchestrator._drain_acquisition_plan_build_commands(  # noqa: SLF001
                {"acquisition_plan_build_command_limit": 5}
            )

            self.assertEqual(plan_owner_result["status"], "completed")
            self.assertEqual(plan_owner_result["completed_count"], 1)
            terminal_plan_command = api_store.get_workflow_command(plan_command["command_id"])
            self.assertEqual(terminal_plan_command["status"], "succeeded")
            self.assertFalse(terminal_plan_command["result"]["provider_called"])
            self.assertFalse(terminal_plan_command["result"]["queue_workflow_called"])
            self.assertFalse(terminal_plan_command["result"]["legacy_job_shell_created"])
            self.assertTrue(terminal_plan_command["result"]["human_review_required"])
            self.assertEqual(terminal_plan_command["result"]["acquisition_plan"]["status"], "ready_for_review")
            self.assertEqual(
                terminal_plan_command["result"]["acquisition_plan"]["target_company"],
                "OpenAI",
            )
            self.assertEqual(terminal_plan_command["result"]["downstream_command_count"], 1)
            review_request_command = api_store.get_workflow_command(
                terminal_plan_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(review_request_command["command_type"], ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE)
            self.assertEqual(review_request_command["owner"], ACQUISITION_PLAN_REVIEW_REQUEST_OWNER)
            self.assertEqual(review_request_command["parent_command_id"], terminal_plan_command["command_id"])
            self.assertEqual(review_request_command["status"], "queued")

            review_owner_result = orchestrator._drain_acquisition_plan_review_request_commands(  # noqa: SLF001
                {"acquisition_plan_review_request_command_limit": 5}
            )

            self.assertEqual(review_owner_result["status"], "completed")
            self.assertEqual(review_owner_result["completed_count"], 1)
            terminal_review_command = api_store.get_workflow_command(review_request_command["command_id"])
            self.assertEqual(terminal_review_command["status"], "succeeded")
            self.assertTrue(terminal_review_command["result"]["human_review_required"])
            self.assertFalse(terminal_review_command["result"]["provider_called"])
            self.assertFalse(terminal_review_command["result"]["queue_workflow_called"])
            self.assertFalse(terminal_review_command["result"]["legacy_job_shell_created"])
            self.assertGreater(terminal_review_command["result"]["plan_review_session"]["review_id"], 0)
            review_session = api_store.get_plan_review_session(
                terminal_review_command["result"]["plan_review_session"]["review_id"]
            )
            self.assertEqual(review_session["status"], "pending")
            self.assertEqual(review_session["request"]["plan_id"], terminal_plan_command["result"]["acquisition_plan"]["plan_id"])

            reviewed = orchestrator.review_plan_session(
                {
                    "review_id": review_session["review_id"],
                    "action": "approve",
                    "reviewer": "unit-test",
                    "notes": "approve typed plan",
                }
            )

            self.assertEqual(reviewed["status"], "reviewed")
            self.assertEqual(reviewed["review"]["status"], "approved")
            self.assertEqual(reviewed["workflow_command"]["command_type"], ACQUISITION_PLAN_COMMIT_COMMAND_TYPE)
            self.assertEqual(reviewed["workflow_command"]["owner"], ACQUISITION_PLAN_COMMIT_OWNER)
            self.assertEqual(reviewed["workflow_command"]["status"], "queued")

            commit_owner_result = orchestrator._drain_acquisition_plan_commit_commands(  # noqa: SLF001
                {"acquisition_plan_commit_command_limit": 5}
            )

            self.assertEqual(commit_owner_result["status"], "completed")
            self.assertEqual(commit_owner_result["completed_count"], 1)
            terminal_commit_command = api_store.get_workflow_command(reviewed["workflow_command"]["command_id"])
            self.assertEqual(terminal_commit_command["status"], "succeeded")
            self.assertEqual(terminal_commit_command["result"]["status"], "plan_committed")
            self.assertFalse(terminal_commit_command["result"]["provider_called"])
            self.assertFalse(terminal_commit_command["result"]["queue_workflow_called"])
            self.assertFalse(terminal_commit_command["result"]["legacy_job_shell_created"])
            acquisition_run = terminal_commit_command["result"]["acquisition_run"]
            self.assertEqual(acquisition_run["status"], "committed_pending_probe")
            self.assertEqual(acquisition_run["current_phase"], "probe_pending")
            self.assertEqual(acquisition_run["operation_run_id"], operation_run_id)
            self.assertEqual(acquisition_run["workflow_run_id"], terminal_commit_command["workflow_run_id"])
            self.assertEqual(acquisition_run["target_company"], "OpenAI")
            self.assertEqual(acquisition_run["plan_review_id"], review_session["review_id"])
            self.assertEqual(
                api_store.list_acquisition_runs(operation_run_id=operation_run_id)[0]["acquisition_run_id"],
                acquisition_run["acquisition_run_id"],
            )
            self.assertEqual(terminal_commit_command["result"]["downstream_command_count"], 1)
            probe_submit_command = api_store.get_workflow_command(
                terminal_commit_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(probe_submit_command["command_type"], ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE)
            self.assertEqual(probe_submit_command["owner"], ACQUISITION_PROBE_OWNER)
            self.assertEqual(probe_submit_command["parent_command_id"], terminal_commit_command["command_id"])
            self.assertEqual(probe_submit_command["payload"]["acquisition_run_id"], acquisition_run["acquisition_run_id"])
            self.assertEqual(terminal_commit_command["result"]["next_phase"], "W11c_acquisition_probe_scale")
            operation_after_owner = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_owner["status"], "running")
            self.assertEqual(operation_after_owner["progress"]["phase"], "acquisition_plan_committed_pending_probe")
            self.assertEqual(operation_after_owner["progress"]["acquisition_run_id"], acquisition_run["acquisition_run_id"])
            self.assertEqual(
                operation_after_owner["result_ref"]["acquisition_run"]["acquisition_run_id"],
                acquisition_run["acquisition_run_id"],
            )
            self.assertEqual(
                operation_after_owner["result_ref"]["plan_review_session"]["review_id"],
                terminal_review_command["result"]["plan_review_session"]["review_id"],
            )
            self.assertEqual(api_store.get_agent_action(approved["action"]["action_id"])["status"], "running")
            self.assertEqual(api_store.list_jobs(), [])

            probe_submit_result = orchestrator._drain_acquisition_probe_commands(  # noqa: SLF001
                {"acquisition_probe_command_limit": 5}
            )
            self.assertEqual(probe_submit_result["status"], "completed")
            self.assertEqual(probe_submit_result["completed_count"], 1)
            terminal_probe_submit_command = api_store.get_workflow_command(probe_submit_command["command_id"])
            self.assertEqual(terminal_probe_submit_command["status"], "succeeded")
            self.assertEqual(terminal_probe_submit_command["result"]["status"], "probe_submitted")
            self.assertFalse(terminal_probe_submit_command["result"]["provider_called"])
            self.assertFalse(terminal_probe_submit_command["result"]["queue_workflow_called"])
            self.assertFalse(terminal_probe_submit_command["result"]["legacy_job_shell_created"])
            probe_collect_command = api_store.get_workflow_command(
                terminal_probe_submit_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(probe_collect_command["command_type"], ACQUISITION_PROBE_COLLECT_COMMAND_TYPE)
            self.assertEqual(probe_collect_command["owner"], ACQUISITION_PROBE_OWNER)
            self.assertEqual(probe_collect_command["parent_command_id"], terminal_probe_submit_command["command_id"])
            acquisition_after_probe_submit = api_store.get_acquisition_run(acquisition_run["acquisition_run_id"])
            self.assertEqual(acquisition_after_probe_submit["status"], "probe_submitted")
            self.assertEqual(acquisition_after_probe_submit["current_phase"], "probe_submitted")
            operation_after_probe_submit = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_probe_submit["progress"]["phase"], "acquisition_probe_submitted")

            probe_collect_result = orchestrator._drain_acquisition_probe_commands(  # noqa: SLF001
                {"acquisition_probe_command_limit": 5}
            )
            self.assertEqual(probe_collect_result["status"], "completed")
            self.assertEqual(probe_collect_result["completed_count"], 1)
            terminal_probe_collect_command = api_store.get_workflow_command(probe_collect_command["command_id"])
            self.assertEqual(terminal_probe_collect_command["status"], "succeeded")
            self.assertEqual(terminal_probe_collect_command["result"]["status"], "probe_collected")
            self.assertTrue(terminal_probe_collect_command["result"]["probe_result"]["requires_provider_backed_discovery_owner"])
            scale_plan_command = api_store.get_workflow_command(
                terminal_probe_collect_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(scale_plan_command["command_type"], ACQUISITION_SCALE_PLAN_COMMAND_TYPE)
            self.assertEqual(scale_plan_command["owner"], ACQUISITION_SCALE_PLAN_OWNER)
            self.assertEqual(scale_plan_command["parent_command_id"], terminal_probe_collect_command["command_id"])
            acquisition_after_probe_collect = api_store.get_acquisition_run(acquisition_run["acquisition_run_id"])
            self.assertEqual(acquisition_after_probe_collect["status"], "probe_collected")
            self.assertEqual(acquisition_after_probe_collect["current_phase"], "probe_collected")
            operation_after_probe_collect = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_probe_collect["progress"]["phase"], "acquisition_probe_collected_pending_scale")

            scale_plan_result = orchestrator._drain_acquisition_scale_plan_commands(  # noqa: SLF001
                {"acquisition_scale_plan_command_limit": 5}
            )
            self.assertEqual(scale_plan_result["status"], "completed")
            self.assertEqual(scale_plan_result["completed_count"], 1)
            terminal_scale_plan_command = api_store.get_workflow_command(scale_plan_command["command_id"])
            self.assertEqual(terminal_scale_plan_command["status"], "succeeded")
            self.assertEqual(terminal_scale_plan_command["result"]["status"], "scale_planned")
            self.assertEqual(terminal_scale_plan_command["result"]["lane_count"], 1)
            self.assertEqual(terminal_scale_plan_command["result"]["activity_run_count"], 1)
            self.assertEqual(
                terminal_scale_plan_command["result"]["attempt_envelope_table"],
                "workflow_activity_attempts",
            )
            self.assertFalse(terminal_scale_plan_command["result"]["provider_called"])
            self.assertFalse(terminal_scale_plan_command["result"]["queue_workflow_called"])
            self.assertFalse(terminal_scale_plan_command["result"]["legacy_job_shell_created"])
            activity_runs = api_store.list_workflow_activity_runs(
                acquisition_run_id=acquisition_run["acquisition_run_id"],
                activity_type=LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            )
            self.assertEqual(len(activity_runs), 1)
            self.assertEqual(activity_runs[0]["status"], "planned_pending_owner")
            self.assertEqual(activity_runs[0]["owner"], LINKEDIN_DISCOVERY_QUERY_RUN_OWNER)
            self.assertEqual(activity_runs[0]["command_id"], scale_plan_command["command_id"])
            self.assertEqual(activity_runs[0]["metadata"]["attempt_envelope_table"], "workflow_activity_attempts")
            self.assertEqual(
                api_store.list_workflow_activity_attempts(activity_run_id=activity_runs[0]["activity_run_id"]),
                [],
            )
            discovery_lanes = api_store.list_acquisition_discovery_lanes(
                acquisition_run_id=acquisition_run["acquisition_run_id"],
            )
            self.assertEqual(len(discovery_lanes), 1)
            self.assertEqual(discovery_lanes[0]["status"], "planned_pending_owner")
            self.assertEqual(discovery_lanes[0]["activity_run_id"], activity_runs[0]["activity_run_id"])
            self.assertEqual(discovery_lanes[0]["source_command_id"], scale_plan_command["command_id"])
            self.assertFalse(discovery_lanes[0]["metadata"]["legacy_job_shell_created"])
            self.assertFalse(discovery_lanes[0]["downstream_command_ids"])
            acquisition_after_scale_plan = api_store.get_acquisition_run(acquisition_run["acquisition_run_id"])
            self.assertEqual(acquisition_after_scale_plan["status"], "scale_planned_pending_discovery")
            self.assertEqual(acquisition_after_scale_plan["current_phase"], "discovery_pending")
            self.assertEqual(
                acquisition_after_scale_plan["metadata"]["activity_boundary"],
                "workflow_activity_runs",
            )
            operation_after_scale_plan = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_scale_plan["progress"]["phase"], "acquisition_scale_planned_pending_discovery")

            continue_discovery = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_CONTINUE_ACQUISITION_RUN,
                    "target_ref": {
                        "workflow_run_id": terminal_scale_plan_command["workflow_run_id"],
                        "acquisition_run_id": acquisition_run["acquisition_run_id"],
                        "lane_id": discovery_lanes[0]["lane_id"],
                        "activity_run_id": activity_runs[0]["activity_run_id"],
                    },
                    "input": {
                        "command_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                        "query": discovery_lanes[0]["query"],
                    },
                    "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                    "idempotency_key": "continue:operation-native-discovery",
                }
            )
            approved_continue = orchestrator.approve_operation_action_api(
                continue_discovery["action"]["action_id"],
                {"actor": "unit-test"},
            )
            continue_dispatch = orchestrator.dispatch_operation_run_api(
                approved_continue["operation_run"]["operation_run_id"],
                {"actor": "unit-test"},
            )
            self.assertEqual(continue_dispatch["status"], "planned")
            operation_native_command = continue_dispatch["workflow_command"]
            self.assertEqual(operation_native_command["command_type"], LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE)
            self.assertEqual(operation_native_command["owner"], LINKEDIN_DISCOVERY_QUERY_RUN_OWNER)
            self.assertEqual(operation_native_command["payload"]["runtime_execution_mode"], "operation_native_discovery")
            self.assertEqual(operation_native_command["payload"]["acquisition_run_id"], acquisition_run["acquisition_run_id"])
            self.assertEqual(operation_native_command["payload"]["lane_id"], discovery_lanes[0]["lane_id"])
            self.assertEqual(operation_native_command["payload"]["activity_run_id"], activity_runs[0]["activity_run_id"])
            self.assertNotIn("job_id", operation_native_command["payload"])
            self.assertNotIn("snapshot_id", operation_native_command["payload"])

            legacy_search_seed_skip = orchestrator._run_search_seed_discovery_query_queue_once(  # noqa: SLF001
                {"search_seed_discovery_item_limit": 5}
            )
            self.assertEqual(legacy_search_seed_skip["status"], "idle")
            self.assertEqual(
                legacy_search_seed_skip["reason"],
                "operation_native_discovery_commands_require_activity_owner",
            )
            self.assertEqual(legacy_search_seed_skip["operation_native_skipped_count"], 1)
            self.assertEqual(
                api_store.get_workflow_command(operation_native_command["command_id"])["status"],
                "queued",
            )

            with mock.patch.object(
                orchestrator.acquisition_engine.search_seed_acquirer,
                "_provider_people_search_fallback",
                return_value=(
                    [
                        {
                            "full_name": "Ada Lovelace",
                            "headline": "Research engineer at OpenAI",
                            "profile_url": "https://www.linkedin.com/in/ada-lovelace/",
                            "source_type": "harvest_profile_search",
                        },
                        {
                            "full_name": "Grace Hopper",
                            "headline": "AI systems engineer at OpenAI",
                            "profile_url": "https://www.linkedin.com/in/grace-hopper/",
                            "source_type": "harvest_profile_search",
                        },
                    ],
                    [
                        {
                            "query": discovery_lanes[0]["query"],
                            "mode": "harvest_profile_search",
                            "status": "completed",
                            "seed_entry_count": 2,
                            "raw_path": "",
                        }
                    ],
                    [],
                    ["harvest_profile_search"],
                ),
            ) as provider_search:
                owner_defer = orchestrator._drain_operation_native_discovery_activity_commands(  # noqa: SLF001
                    {"operation_native_discovery_command_limit": 5}
                )
            provider_search.assert_called_once()
            self.assertEqual(owner_defer["status"], "active")
            self.assertEqual(owner_defer["executed_command_count"], 1)
            self.assertEqual(owner_defer["completed_count"], 1)
            self.assertEqual(owner_defer["waiting_count"], 0)
            self.assertEqual(owner_defer["failed_count"], 0)
            completed_command = api_store.get_workflow_command(operation_native_command["command_id"])
            self.assertEqual(completed_command["status"], "succeeded")
            self.assertEqual(completed_command["result"]["reason"], "operation_native_discovery_provider_completed")
            self.assertTrue(completed_command["result"]["provider_called"])
            self.assertFalse(completed_command["result"]["legacy_job_shell_created"])
            self.assertFalse(completed_command["result"]["queue_workflow_called"])
            self.assertEqual(completed_command["result"]["entry_count"], 2)
            self.assertEqual(completed_command["result"]["profile_url_count"], 2)
            self.assertEqual(completed_command["result"]["downstream_command_count"], 1)
            self.assertEqual(len(completed_command["result"]["downstream_command_ids"]), 1)
            profile_fetch_command = api_store.get_workflow_command(
                completed_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(profile_fetch_command["command_type"], LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE)
            self.assertEqual(profile_fetch_command["owner"], LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER)
            self.assertEqual(profile_fetch_command["parent_command_id"], completed_command["command_id"])
            self.assertEqual(profile_fetch_command["status"], "queued")
            self.assertFalse(profile_fetch_command["payload"]["legacy_job_shell_created"])
            self.assertFalse(profile_fetch_command["payload"]["queue_workflow_called"])
            self.assertFalse(profile_fetch_command["payload"]["normal_path_executes_legacy_profile_refill_owner"])
            self.assertNotIn("job_id", profile_fetch_command["payload"])
            completed_activity = api_store.get_workflow_activity_run(activity_runs[0]["activity_run_id"])
            self.assertEqual(completed_activity["status"], "succeeded")
            self.assertEqual(completed_activity["phase"], "provider_discovery_completed")
            self.assertEqual(completed_activity["entity_counts"]["candidate_count"], 2)
            completed_lane = api_store.get_acquisition_discovery_lane(discovery_lanes[0]["lane_id"])
            self.assertEqual(completed_lane["status"], "provider_discovery_completed")
            self.assertEqual(completed_lane["phase"], "provider_discovery_completed")
            self.assertEqual(completed_lane["entity_counts"]["profile_url_count"], 2)
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activity_runs[0]["activity_run_id"])
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            self.assertEqual(attempts[0]["output"]["entry_count"], 2)
            entity_deltas = api_store.list_workflow_entity_deltas(activity_run_id=activity_runs[0]["activity_run_id"])
            self.assertEqual(len(entity_deltas), 2)
            self.assertEqual({delta["entity_type"] for delta in entity_deltas}, {"candidate"})
            self.assertEqual({delta["delta_kind"] for delta in entity_deltas}, {"provider_discovered"})
            self.assertEqual({delta["status"] for delta in entity_deltas}, {"recorded"})
            self.assertTrue(all(not delta["projection_effect"]["entered_projection"] for delta in entity_deltas))
            discovery_operation = api_store.get_operation_run(approved_continue["operation_run"]["operation_run_id"])
            self.assertEqual(discovery_operation["status"], "running")
            self.assertEqual(discovery_operation["result_ref"]["workflow_command"]["status"], "succeeded")
            self.assertEqual(api_store.list_jobs(), [])

            profile_fetch_owner_result = orchestrator._drain_operation_native_profile_fetch_activity_commands(  # noqa: SLF001
                {"operation_native_profile_fetch_command_limit": 5}
            )
            self.assertEqual(profile_fetch_owner_result["status"], "active")
            self.assertEqual(profile_fetch_owner_result["executed_command_count"], 1)
            self.assertEqual(profile_fetch_owner_result["completed_count"], 1)
            completed_profile_command = api_store.get_workflow_command(profile_fetch_command["command_id"])
            self.assertEqual(completed_profile_command["status"], "succeeded")
            self.assertEqual(
                completed_profile_command["result"]["reason"],
                "operation_native_profile_fetch_activity_planned",
            )
            self.assertFalse(completed_profile_command["result"]["provider_called"])
            self.assertFalse(completed_profile_command["result"]["legacy_job_shell_created"])
            self.assertFalse(completed_profile_command["result"]["queue_workflow_called"])
            self.assertEqual(completed_profile_command["result"]["profile_url_count"], 2)
            self.assertEqual(completed_profile_command["result"]["fetch_required_count"], 2)
            self.assertTrue(completed_profile_command["result"]["provider_owner_required"])
            self.assertEqual(completed_profile_command["result"]["downstream_command_count"], 1)
            provider_fetch_command = api_store.get_workflow_command(
                completed_profile_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(provider_fetch_command["command_type"], LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE)
            self.assertEqual(provider_fetch_command["owner"], LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER)
            self.assertEqual(provider_fetch_command["parent_command_id"], completed_profile_command["command_id"])
            self.assertFalse(provider_fetch_command["payload"]["legacy_job_shell_created"])
            self.assertFalse(provider_fetch_command["payload"]["queue_workflow_called"])
            self.assertFalse(provider_fetch_command["payload"]["normal_path_executes_legacy_profile_refill_owner"])
            profile_activities = api_store.list_workflow_activity_runs(
                command_id=profile_fetch_command["command_id"],
                activity_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
            )
            self.assertEqual(len(profile_activities), 1)
            self.assertEqual(profile_activities[0]["status"], "planned_pending_provider_owner")
            self.assertEqual(profile_activities[0]["phase"], "provider_profile_fetch_pending")
            self.assertEqual(profile_activities[0]["owner"], LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER)
            self.assertEqual(profile_activities[0]["parent_activity_run_id"], activity_runs[0]["activity_run_id"])
            self.assertEqual(profile_activities[0]["entity_counts"]["profile_url_count"], 2)
            self.assertEqual(profile_activities[0]["entity_counts"]["fetch_required_count"], 2)
            profile_attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=profile_activities[0]["activity_run_id"],
            )
            self.assertEqual(len(profile_attempts), 1)
            self.assertEqual(profile_attempts[0]["status"], "succeeded")
            self.assertEqual(profile_attempts[0]["output"]["fetch_required_count"], 2)
            profile_deltas = api_store.list_workflow_entity_deltas(
                activity_run_id=profile_activities[0]["activity_run_id"],
                entity_type="profile",
            )
            self.assertEqual(len(profile_deltas), 2)
            self.assertEqual({delta["delta_kind"] for delta in profile_deltas}, {"profile_fetch_required"})
            self.assertEqual({delta["status"] for delta in profile_deltas}, {"not_applied"})
            self.assertTrue(all(not delta["projection_effect"]["entered_projection"] for delta in profile_deltas))
            profile_operation = api_store.get_operation_run(approved_continue["operation_run"]["operation_run_id"])
            self.assertEqual(profile_operation["status"], "running")
            self.assertEqual(profile_operation["progress"]["phase"], "operation_native_profile_fetch_activity_planned")
            self.assertEqual(api_store.list_jobs(), [])

            def _fake_fetch_profile(slug, snapshot_dir, *, asset_logger=None):
                profile_dir = Path(snapshot_dir) / "profiles"
                profile_dir.mkdir(parents=True, exist_ok=True)
                raw_path = profile_dir / f"{slug}.json"
                raw_path.write_text(json.dumps({"profile": {"public_identifier": slug}}), encoding="utf-8")
                return {"raw_path": raw_path, "parsed": {"public_identifier": slug}}

            with mock.patch.object(
                orchestrator.acquisition_engine.multi_source_enricher.profile_connector,
                "fetch_profile",
                side_effect=_fake_fetch_profile,
            ) as fetch_profile:
                provider_fetch_owner_result = orchestrator._drain_operation_native_profile_fetch_activity_commands(  # noqa: SLF001
                    {"operation_native_profile_fetch_command_limit": 5}
                )
            self.assertEqual(fetch_profile.call_count, 2)
            self.assertEqual(provider_fetch_owner_result["status"], "active")
            self.assertEqual(provider_fetch_owner_result["executed_command_count"], 1)
            self.assertEqual(provider_fetch_owner_result["completed_count"], 1)
            completed_provider_command = api_store.get_workflow_command(provider_fetch_command["command_id"])
            self.assertEqual(completed_provider_command["status"], "succeeded")
            self.assertEqual(
                completed_provider_command["result"]["reason"],
                "operation_native_profile_provider_fetch_completed",
            )
            self.assertTrue(completed_provider_command["result"]["provider_called"])
            self.assertFalse(completed_provider_command["result"]["legacy_job_shell_created"])
            self.assertFalse(completed_provider_command["result"]["queue_workflow_called"])
            self.assertEqual(completed_provider_command["result"]["fetched_count"], 2)
            self.assertEqual(completed_provider_command["result"]["downstream_command_count"], 1)
            terminal_admit_command = api_store.get_workflow_command(
                completed_provider_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(terminal_admit_command["command_type"], LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE)
            self.assertEqual(terminal_admit_command["owner"], LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER)
            self.assertEqual(terminal_admit_command["parent_command_id"], completed_provider_command["command_id"])
            self.assertFalse(terminal_admit_command["payload"]["legacy_job_shell_created"])
            self.assertFalse(terminal_admit_command["payload"]["queue_workflow_called"])
            self.assertFalse(terminal_admit_command["payload"]["normal_path_mutates_projection"])
            final_profile_activity = api_store.get_workflow_activity_run(profile_activities[0]["activity_run_id"])
            self.assertEqual(final_profile_activity["status"], "succeeded")
            self.assertEqual(final_profile_activity["phase"], "provider_profile_fetch_completed")
            self.assertEqual(final_profile_activity["entity_counts"]["provider_fetched_count"], 2)
            final_profile_attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=profile_activities[0]["activity_run_id"],
            )
            self.assertEqual(len(final_profile_attempts), 2)
            self.assertIn("linkedin_profile_detail", {attempt["provider"] for attempt in final_profile_attempts})
            final_profile_deltas = api_store.list_workflow_entity_deltas(
                activity_run_id=profile_activities[0]["activity_run_id"],
                entity_type="profile",
            )
            self.assertEqual(
                {delta["delta_kind"] for delta in final_profile_deltas},
                {"profile_fetch_required", "profile_provider_fetched"},
            )
            fetched_registry = api_store.repos.linkedin_profile_registry.get("https://www.linkedin.com/in/ada-lovelace/")
            self.assertEqual(fetched_registry["status"], "fetched")
            self.assertIn("operation_activities", fetched_registry["last_raw_path"])
            provider_operation = api_store.get_operation_run(approved_continue["operation_run"]["operation_run_id"])
            self.assertEqual(provider_operation["status"], "running")
            self.assertEqual(
                provider_operation["progress"]["phase"],
                "operation_native_profile_provider_fetch_completed",
            )
            self.assertEqual(api_store.list_jobs(), [])

            terminal_owner_result = orchestrator._drain_operation_native_profile_fetch_activity_commands(  # noqa: SLF001
                {"operation_native_profile_fetch_command_limit": 5}
            )
            self.assertEqual(terminal_owner_result["status"], "active")
            self.assertEqual(terminal_owner_result["executed_command_count"], 1)
            self.assertEqual(terminal_owner_result["completed_count"], 1)
            completed_terminal_command = api_store.get_workflow_command(terminal_admit_command["command_id"])
            self.assertEqual(completed_terminal_command["status"], "succeeded")
            self.assertEqual(
                completed_terminal_command["result"]["reason"],
                "operation_native_profile_terminal_admitted",
            )
            self.assertEqual(completed_terminal_command["result"]["downstream_command_count"], 1)
            projection_admission_command = api_store.get_workflow_command(
                completed_terminal_command["result"]["downstream_command_ids"][0]
            )
            self.assertEqual(
                projection_admission_command["command_type"],
                PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
            )
            self.assertEqual(projection_admission_command["owner"], PROJECTION_PROFILE_ADMISSION_APPLY_OWNER)
            self.assertEqual(projection_admission_command["parent_command_id"], completed_terminal_command["command_id"])
            self.assertFalse(projection_admission_command["payload"]["legacy_job_shell_created"])
            self.assertFalse(projection_admission_command["payload"]["queue_workflow_called"])
            self.assertEqual(
                projection_admission_command["payload"]["projection_write_owner"],
                PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
            )
            self.assertFalse(completed_terminal_command["result"]["legacy_job_shell_created"])
            self.assertFalse(completed_terminal_command["result"]["queue_workflow_called"])
            self.assertFalse(completed_terminal_command["result"]["normal_path_mutates_projection"])
            self.assertEqual(completed_terminal_command["result"]["admitted_count"], 2)
            terminal_deltas = api_store.list_workflow_entity_deltas(
                entity_type="profile",
                activity_run_id=completed_terminal_command["result"]["activity_run_id"],
            )
            self.assertEqual(len(terminal_deltas), 2)
            self.assertEqual({delta["delta_kind"] for delta in terminal_deltas}, {"profile_terminal_recorded"})
            self.assertTrue(all(not delta["projection_effect"]["entered_projection"] for delta in terminal_deltas))
            self.assertEqual(
                {delta["projection_effect"]["reason"] for delta in terminal_deltas},
                {"projection_admission_pending_owner"},
            )
            terminal_operation = api_store.get_operation_run(approved_continue["operation_run"]["operation_run_id"])
            self.assertEqual(terminal_operation["status"], "running")
            self.assertEqual(
                terminal_operation["progress"]["phase"],
                "operation_native_profile_terminal_admitted_pending_projection",
            )
            self.assertEqual(api_store.list_jobs(), [])

            projection_owner_result = orchestrator._drain_operation_native_projection_admission_commands(  # noqa: SLF001
                {"operation_native_projection_admission_command_limit": 5}
            )
            self.assertEqual(projection_owner_result["status"], "active")
            self.assertEqual(projection_owner_result["executed_command_count"], 1)
            self.assertEqual(projection_owner_result["completed_count"], 1)
            completed_projection_command = api_store.get_workflow_command(projection_admission_command["command_id"])
            self.assertEqual(completed_projection_command["status"], "succeeded")
            self.assertEqual(
                completed_projection_command["result"]["reason"],
                "operation_native_projection_admission_completed",
            )
            self.assertEqual(completed_projection_command["result"]["admitted_member_count"], 2)
            self.assertEqual(completed_projection_command["result"]["downstream_command_count"], 2)
            downstream_projection_commands = completed_projection_command["result"]["downstream_commands"]
            self.assertEqual(
                {command["command_type"] for command in downstream_projection_commands},
                {
                    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
                },
            )
            self.assertEqual(
                {command["owner"] for command in downstream_projection_commands},
                {
                    PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
                    COLLECTION_AUTHORITATIVE_MERGE_OWNER,
                },
            )
            self.assertTrue(
                all(command["workflow_run_id"] == completed_projection_command["workflow_run_id"] for command in downstream_projection_commands)
            )
            self.assertFalse(completed_projection_command["result"]["legacy_job_shell_created"])
            self.assertFalse(completed_projection_command["result"]["queue_workflow_called"])
            projection_id = completed_projection_command["result"]["projection_id"]
            self.assertTrue(projection_id)
            self.assertEqual(api_store.count_serving_projection_members(projection_id, visible_only=True), 2)
            run_projection_link = api_store.get_run_projection_link(completed_projection_command["workflow_run_id"])
            self.assertEqual(run_projection_link["projection_id"], projection_id)
            projection_member_deltas = api_store.list_workflow_entity_deltas(
                activity_run_id=completed_projection_command["result"]["activity_run_id"],
                entity_type="projection_member",
            )
            self.assertEqual(len(projection_member_deltas), 2)
            self.assertEqual(
                {delta["delta_kind"] for delta in projection_member_deltas},
                {"projection_member_admitted"},
            )
            self.assertTrue(all(delta["projection_effect"]["entered_projection"] for delta in projection_member_deltas))
            downstream_by_type = {
                command["command_type"]: api_store.get_workflow_command(command["command_id"])
                for command in downstream_projection_commands
            }
            index_owner_result = orchestrator._drain_projection_person_search_index_build_commands(  # noqa: SLF001
                {"projection_person_search_index_command_limit": 5}
            )
            self.assertEqual(index_owner_result["completed_count"], 1)
            completed_index_command = api_store.get_workflow_command(
                downstream_by_type[PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE]["command_id"]
            )
            self.assertEqual(completed_index_command["status"], "succeeded")
            self.assertEqual(completed_index_command["result"]["activity_run_id"][:7], "actrun_")
            index_activities = api_store.list_workflow_activity_runs(
                command_id=completed_index_command["command_id"],
                activity_type=PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
            )
            self.assertEqual(len(index_activities), 1)
            self.assertEqual(index_activities[0]["status"], "succeeded")
            index_deltas = api_store.list_workflow_entity_deltas(
                command_id=completed_index_command["command_id"],
                entity_type="projection_index",
            )
            self.assertEqual({delta["delta_kind"] for delta in index_deltas}, {"projection_person_search_index_built"})
            collection_owner_result = orchestrator._drain_collection_authoritative_merge_commands(  # noqa: SLF001
                {"collection_authoritative_merge_command_limit": 5}
            )
            self.assertEqual(collection_owner_result["completed_count"], 1)
            completed_collection_command = api_store.get_workflow_command(
                downstream_by_type[COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE]["command_id"]
            )
            self.assertEqual(completed_collection_command["status"], "succeeded")
            self.assertEqual(completed_collection_command["result"]["activity_run_id"][:7], "actrun_")
            collection_activities = api_store.list_workflow_activity_runs(
                command_id=completed_collection_command["command_id"],
                activity_type=COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
            )
            self.assertEqual(len(collection_activities), 1)
            self.assertEqual(collection_activities[0]["status"], "succeeded")
            collection_deltas = api_store.list_workflow_entity_deltas(
                command_id=completed_collection_command["command_id"],
                entity_type="collection_projection",
            )
            self.assertEqual({delta["delta_kind"] for delta in collection_deltas}, {"collection_authoritative_merged"})
            completed_operation = api_store.get_operation_run(approved_continue["operation_run"]["operation_run_id"])
            self.assertEqual(completed_operation["status"], "completed")
            self.assertEqual(
                completed_operation["progress"]["phase"],
                "operation_native_projection_admission_completed",
            )
            self.assertEqual(api_store.list_jobs(), [])

            legacy_continue = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_CONTINUE_ACQUISITION_RUN,
                    "target_ref": {"job_id": "job-legacy-discovery"},
                    "input": {
                        "command_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                        "snapshot_id": "snap-legacy",
                        "query": "site:linkedin.com/in OpenAI legacy",
                    },
                    "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                    "idempotency_key": "continue:legacy-discovery-rejected",
                }
            )
            approved_legacy_continue = orchestrator.approve_operation_action_api(
                legacy_continue["action"]["action_id"],
                {"actor": "unit-test"},
            )
            legacy_continue_dispatch = orchestrator.dispatch_operation_run_api(
                approved_legacy_continue["operation_run"]["operation_run_id"],
                {"actor": "unit-test"},
            )
            self.assertEqual(legacy_continue_dispatch["status"], "invalid")
            self.assertEqual(
                legacy_continue_dispatch["reason"],
                "continue_acquisition_run requires acquisition_run_id, lane_id, activity_run_id, and query",
            )

            rejected = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_START_ACQUISITION_RUN,
                    "target_ref": {"job_id": "job-discovery", "target_company": "OpenAI"},
                    "input": {
                        "command_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                        "snapshot_id": "snap-discovery",
                        "query": "site:linkedin.com/in OpenAI research engineer",
                    },
                    "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                    "idempotency_key": "discovery:job-discovery:rejected",
                }
            )
            rejected_approval = orchestrator.approve_operation_action_api(
                rejected["action"]["action_id"],
                {"actor": "unit-test"},
            )
            rejected_dispatch = orchestrator.dispatch_operation_run_api(
                rejected_approval["operation_run"]["operation_run_id"],
                {"actor": "unit-test"},
            )

            self.assertEqual(rejected_dispatch["status"], "invalid")
            self.assertEqual(rejected_dispatch["reason"], "unsupported_agent_callable_workflow_command_type")
            self.assertEqual(len(api_store.list_workflow_commands(limit=100)), 16)
        finally:
            api_store.close()

    def test_run_scope_projection_finalize_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "run-scope-finalize-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            request_payload = {
                "target_company": "OpenAI",
                "role": "AI infrastructure engineer",
                "location": "United States",
            }
            result_view = api_store.upsert_job_result_view(
                job_id="job-run-scope-finalize-activity",
                target_company="OpenAI",
                source_kind="company_snapshot",
                view_kind="asset_population",
                snapshot_id="snap-run-scope-finalize-activity",
                asset_view="canonical_merged",
                source_path=str(self.runtime_dir / "snapshots" / "candidate_documents.json"),
                summary={"candidate_count": 2, "default_results_mode": "asset_population"},
                metadata={},
            )
            command = orchestrator._plan_run_scope_projection_finalize_command(  # noqa: SLF001
                job_id="job-run-scope-finalize-activity",
                request=JobRequest.from_payload(request_payload),
                candidate_source={
                    "source_kind": "company_snapshot",
                    "target_company": "OpenAI",
                    "snapshot_id": "snap-run-scope-finalize-activity",
                    "asset_view": "canonical_merged",
                    "source_path": str(self.runtime_dir / "snapshots" / "candidate_documents.json"),
                    "candidate_count": 2,
                },
                result_view=result_view,
                reason="unit_test_activity_spine",
                replace_members=True,
            )
            self.assertEqual(command["command_type"], PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE)
            self.assertEqual(command["owner"], PROJECTION_RUN_SCOPE_FINALIZE_OWNER)
            with mock.patch.object(
                orchestrator,
                "_publish_run_scope_projection_from_result_view_owner",
                return_value={
                    "status": "published",
                    "reason": "unit_test_projection_published",
                    "projection_id": "proj-run-scope-finalize-activity",
                    "member_count": 2,
                    "link": {
                        "run_id": "job-run-scope-finalize-activity",
                        "projection_id": "proj-run-scope-finalize-activity",
                    },
                },
            ):
                finalize_result = orchestrator._run_run_scope_projection_finalize_command_once(command)  # noqa: SLF001

            self.assertEqual(finalize_result["status"], "completed")
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertTrue(stored_command["result"]["entity_delta_id"].startswith("entitydelta_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["phase"], "unit_test_projection_published")
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="run_scope_projection",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "run_scope_projection_finalized")
            self.assertEqual(deltas[0]["status"], "recorded")
            self.assertEqual(deltas[0]["entity_key"], "proj-run-scope-finalize-activity")
            self.assertTrue(deltas[0]["projection_effect"]["entered_projection"])
            self.assertEqual(deltas[0]["projection_effect"]["member_count"], 2)
            self.assertEqual(
                workflow_command_activity_spine_policy(PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_projection_facet_layering_command_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "facet-layering-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-facet-layering-activity",
                operation_id="op-facet-layering-activity",
                command_type=PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
                owner=PROJECTION_FACET_LAYERING_BUILD_OWNER,
                idempotency_key="projection.facet_layering.build:activity-test",
                payload={
                    "job_id": "job-facet-layering-activity",
                    "item_id": "item-facet-layering-activity",
                    "snapshot_id": "snap-facet-layering-activity",
                    "candidate_count": 3,
                    "materialization_metadata": {
                        "serving_projection_id": "proj-facet-layering-activity",
                        "source_kind": "serving_projection_members",
                        "candidate_count": 3,
                    },
                    "source": "unit_test",
                    "reason": "unit_test_facet_layering_activity",
                },
            )
            self.assertEqual(command["command_type"], PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE)
            with mock.patch.object(
                orchestrator,
                "_process_projection_facet_layering_build_item",
                return_value={
                    "status": "completed",
                    "reason": "projection_facet_layering_build_completed",
                    "candidate_count": 3,
                    "processed_candidate_count": 3,
                    "outreach_layering": {
                        "analysis_paths": {"summary": str(self.runtime_dir / "analysis_summary.json")},
                    },
                    "projection_layer_assignment": {
                        "projection_id": "proj-facet-layering-activity",
                        "updated_member_count": 3,
                    },
                },
            ):
                owner_result = orchestrator._run_projection_facet_layering_queue_once(  # noqa: SLF001
                    {
                        "projection_facet_layering_item_limit": 1,
                        "projection_facet_layering_phase_budget_ms": 10000,
                        "projection_facet_layering_chunk_size": 50,
                    }
                )

            self.assertEqual(owner_result["completed_count"], 1)
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertTrue(stored_command["result"]["entity_delta_id"].startswith("entitydelta_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="facet_layering",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "projection_facet_layering_built")
            self.assertEqual(deltas[0]["status"], "recorded")
            self.assertEqual(deltas[0]["entity_key"], "proj-facet-layering-activity")
            self.assertFalse(deltas[0]["projection_effect"]["entered_projection"])
            self.assertTrue(deltas[0]["projection_effect"]["facet_layering_ready"])
            self.assertEqual(
                workflow_command_activity_spine_policy(PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_snapshot_compaction_command_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "snapshot-compaction-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-snapshot-compaction-activity",
                operation_id="op-snapshot-compaction-activity",
                command_type=SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
                owner=SNAPSHOT_COMPACTION_RUN_OWNER,
                idempotency_key="snapshot.compaction.run:activity-test",
                payload={
                    "job_id": "job-snapshot-compaction-activity",
                    "snapshot_id": "snap-compaction",
                    "item_id": "item-snapshot-compaction",
                    "item_kind": "snapshot_full_materialization",
                    "causality": {
                        "source_event_id": "evt-snapshot-compaction-activity",
                        "causal_group_id": "cg-snapshot-compaction-activity",
                        "produced_entity_counts": {"snapshot": 1},
                    },
                },
            )
            with mock.patch.object(
                orchestrator,
                "_process_snapshot_full_materialization_item",
                return_value={
                    "status": "completed",
                    "reason": "snapshot_full_materialization_completed",
                    "snapshot_id": "snap-compaction",
                    "candidate_count": 17,
                    "profile_count": 13,
                },
            ):
                owner_result = orchestrator._run_snapshot_full_materialization_queue_once(  # noqa: SLF001
                    {"snapshot_full_materialization_item_limit": 1}
                )

            self.assertEqual(owner_result["completed_count"], 1)
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertTrue(stored_command["result"]["entity_delta_id"].startswith("entitydelta_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["candidate_count"], 17)
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="snapshot_compaction",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "snapshot_compaction_completed")
            self.assertEqual(deltas[0]["status"], "recorded")
            self.assertTrue(deltas[0]["projection_effect"]["snapshot_compaction_completed"])
            self.assertEqual(
                workflow_command_activity_spine_policy(SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_board_visible_patch_command_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "board-visible-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-board-visible-activity",
                operation_id="op-board-visible-activity",
                command_type=PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
                owner=PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_OWNER,
                idempotency_key="projection.board_visible_patch.publish:activity-test",
                payload={
                    "job_id": "job-board-visible-activity",
                    "item_id": "item-board-visible-activity",
                    "snapshot_id": "snap-board-visible-activity",
                    "baseline_snapshot_id": "baseline-board-visible-activity",
                    "candidate_ids": ["candidate-a", "candidate-b"],
                    "candidate_count": 2,
                    "source": "unit_test",
                    "reason": "unit_test_board_visible_activity",
                },
            )
            with mock.patch.object(
                orchestrator,
                "_process_board_visible_delta_apply_item_group",
                return_value={
                    "status": "completed",
                    "reason": "board_visible_patch_publish_completed",
                    "candidate_count": 2,
                    "candidate_ids": ["candidate-a", "candidate-b"],
                    "overlay_path": str(self.runtime_dir / "board_visible_overlay.json"),
                },
            ):
                owner_result = orchestrator._run_board_visible_patch_publish_command_group(  # noqa: SLF001
                    commands=[command],
                    lease_seconds=300,
                )

            self.assertEqual(owner_result["completed_count"], 1)
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertTrue(stored_command["result"]["entity_delta_id"].startswith("entitydelta_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="board_visible_patch",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "board_visible_patch_published")
            self.assertEqual(deltas[0]["status"], "recorded")
            self.assertEqual(deltas[0]["entity_payload"]["candidate_count"], 2)
            self.assertTrue(deltas[0]["projection_effect"]["board_visible"])
            self.assertEqual(
                workflow_command_activity_spine_policy(PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_local_profile_delta_apply_command_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "local-apply-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-local-apply-activity",
                operation_id="op-local-apply-activity",
                command_type=LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
                owner=LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_OWNER,
                idempotency_key="linkedin.local_profile_delta.apply:activity-test",
                payload={
                    "job_id": "job-local-apply-activity",
                    "item_id": "item-local-apply-activity",
                    "snapshot_id": "snap-local-apply-activity",
                    "worker_kind": "harvest_profile_batch",
                    "candidate_ids": ["candidate-a", "candidate-b"],
                    "candidate_count": 2,
                    "source_worker_ids": [101, 102],
                    "source": "unit_test",
                    "reason": "unit_test_local_apply_activity",
                },
            )
            with mock.patch.object(
                orchestrator,
                "_process_local_apply_closure_item_batch",
                return_value={
                    "status": "completed",
                    "reason": "local_profile_delta_apply_completed",
                    "candidate_count": 2,
                    "candidate_ids": ["candidate-a", "candidate-b"],
                    "profile_apply_progress": {"applied_count": 2},
                },
            ):
                owner_result = orchestrator._run_local_profile_delta_apply_command_group(  # noqa: SLF001
                    commands=[command],
                    lease_seconds=300,
                    profile_url_limit=100,
                    profile_url_budget_ms=10000,
                )

            self.assertEqual(owner_result["completed_count"], 1)
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertTrue(stored_command["result"]["entity_delta_id"].startswith("entitydelta_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="local_profile_delta",
            )
            self.assertEqual(len(deltas), 1)
            self.assertEqual(deltas[0]["delta_kind"], "local_profile_delta_applied")
            self.assertEqual(deltas[0]["status"], "recorded")
            self.assertEqual(deltas[0]["entity_payload"]["candidate_count"], 2)
            self.assertTrue(deltas[0]["projection_effect"]["local_profile_delta_applied"])
            self.assertTrue(deltas[0]["projection_effect"]["board_visible_pending"])
            self.assertEqual(
                workflow_command_activity_spine_policy(LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_profile_refill_submit_command_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-refill-submit-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            profile_urls = [
                "https://www.linkedin.com/in/ada-lovelace/",
                "https://www.linkedin.com/in/grace-hopper/",
            ]
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-profile-refill-submit-activity",
                operation_id="op-profile-refill-submit-activity",
                command_type=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
                idempotency_key="linkedin.profile_refill.submit_batch:activity-test",
                payload={
                    "job_id": "job-profile-refill-submit-activity",
                    "snapshot_dir": str(self.runtime_dir / "snap-refill"),
                    "chunk_index": 3,
                    "profile_urls": profile_urls,
                    "requested_url_count": len(profile_urls),
                    "candidate_count": len(profile_urls),
                    "causality": {
                        "source_event_id": "evt-profile-refill-submit-activity",
                        "causal_group_id": "cg-profile-refill-submit-activity",
                        "produced_entity_counts": {"profile_url_count": len(profile_urls)},
                    },
                },
            )

            with mock.patch.object(
                orchestrator.acquisition_engine.multi_source_enricher,
                "_execute_harvest_profile_batch_worker",
                return_value={
                    "worker_status": "queued",
                    "summary": {
                        "queued_urls": profile_urls,
                        "worker_id": 42,
                        "run_id": "run-profile-refill-submit",
                        "dataset_id": "dataset-profile-refill-submit",
                        "payload_hash": "payload-profile-refill-submit",
                        "summary_path": str(self.runtime_dir / "refill_summary.json"),
                    },
                },
            ):
                owner_result = (
                    orchestrator.acquisition_engine.multi_source_enricher
                    .run_linkedin_profile_refill_submit_command_once(command)
                )

            self.assertEqual(owner_result["status"], "completed")
            self.assertEqual(owner_result["dispatch_result"]["worker_status"], "queued")
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertEqual(stored_command["result"]["entity_delta_count"], 2)
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["queued_url_count"], 2)
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            self.assertEqual(attempts[0]["provider"], "harvest_profile")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="profile_refill_submit",
            )
            self.assertEqual(len(deltas), 2)
            self.assertEqual({delta["delta_kind"] for delta in deltas}, {"profile_refill_submit_queued"})
            self.assertEqual({delta["status"] for delta in deltas}, {"recorded"})
            self.assertTrue(all(delta["projection_effect"]["provider_submit_queued"] for delta in deltas))
            self.assertEqual(
                workflow_command_activity_spine_policy(
                    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE
                ).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_profile_url_terminal_record_command_records_activity_spine_evidence(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-url-terminal-activity.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            entries = [
                {
                    "profile_url": "https://www.linkedin.com/in/ada-lovelace/",
                    "status": "fetched",
                    "raw_path": str(self.runtime_dir / "profiles" / "ada.json"),
                    "source_jobs": ["job-profile-url-terminal-activity"],
                    "snapshot_dir": str(self.runtime_dir / "snap-terminal"),
                },
                {
                    "profile_url": "https://www.linkedin.com/in/grace-hopper/",
                    "status": "failed",
                    "error": "not_found",
                    "retryable": False,
                    "source_jobs": ["job-profile-url-terminal-activity"],
                    "snapshot_dir": str(self.runtime_dir / "snap-terminal"),
                },
            ]
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-profile-url-terminal-activity",
                operation_id="op-profile-url-terminal-activity",
                command_type=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
                owner=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
                idempotency_key="linkedin.profile_url_terminal.record:activity-test",
                payload={
                    "job_id": "job-profile-url-terminal-activity",
                    "snapshot_dir": str(self.runtime_dir / "snap-terminal"),
                    "snapshot_id": "snap-terminal",
                    "terminal_scope": "unit_test",
                    "entry_count": len(entries),
                    "entries": entries,
                    "causality": {
                        "source_event_id": "evt-profile-url-terminal-activity",
                        "causal_group_id": "cg-profile-url-terminal-activity",
                        "produced_entity_counts": {"profile_url_count": len(entries)},
                    },
                },
            )

            owner_result = (
                orchestrator.acquisition_engine.multi_source_enricher
                .run_linkedin_profile_url_terminal_record_command_once(command)
            )

            self.assertEqual(owner_result["status"], "completed")
            self.assertEqual(owner_result["recorded_count"], 2)
            self.assertEqual(owner_result["fetched_count"], 1)
            self.assertEqual(owner_result["failed_count"], 1)
            stored_command = api_store.get_workflow_command(command["command_id"])
            self.assertEqual(stored_command["status"], "succeeded")
            self.assertEqual(stored_command["result"]["activity_run_id"][:7], "actrun_")
            self.assertEqual(stored_command["result"]["entity_delta_count"], 2)
            activities = api_store.list_workflow_activity_runs(
                command_id=command["command_id"],
                activity_type=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["recorded_count"], 2)
            attempts = api_store.list_workflow_activity_attempts(
                activity_run_id=activities[0]["activity_run_id"],
            )
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            deltas = api_store.list_workflow_entity_deltas(
                command_id=command["command_id"],
                entity_type="profile_url_terminal_record",
            )
            self.assertEqual(len(deltas), 2)
            self.assertEqual(
                {delta["delta_kind"] for delta in deltas},
                {"profile_url_terminal_recorded"},
            )
            self.assertEqual({delta["status"] for delta in deltas}, {"recorded"})
            terminal_statuses = {
                delta["entity_payload"]["profile_url_key"]: delta["entity_payload"]["terminal_status"]
                for delta in deltas
            }
            self.assertEqual(terminal_statuses["https://www.linkedin.com/in/ada-lovelace"], "fetched")
            self.assertEqual(terminal_statuses["https://www.linkedin.com/in/grace-hopper"], "failed")
            self.assertTrue(
                next(
                    delta
                    for delta in deltas
                    if delta["entity_key"] == "https://www.linkedin.com/in/ada-lovelace"
                )[
                    "projection_effect"
                ]["profile_terminal_recorded"]
            )
            registry_rows = api_store.repos.linkedin_profile_registry.get_bulk(
                [
                    "https://www.linkedin.com/in/ada-lovelace/",
                    "https://www.linkedin.com/in/grace-hopper/",
                ]
            )
            self.assertEqual(
                registry_rows["https://www.linkedin.com/in/ada-lovelace"]["status"],
                "fetched",
            )
            self.assertTrue(
                registry_rows["https://www.linkedin.com/in/grace-hopper"]["status"].startswith("failed"),
                registry_rows["https://www.linkedin.com/in/grace-hopper"]["status"],
            )
            self.assertEqual(
                workflow_command_activity_spine_policy(
                    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE
                ).requirement,
                ACTIVITY_SPINE_REQUIRED,
            )
        finally:
            api_store.close()

    def test_continue_acquisition_rejects_commands_outside_action_registry_allowlist(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "continue-command-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_CONTINUE_ACQUISITION_RUN,
                    "target_ref": {"job_id": "job-continue"},
                    "input": {
                        "command_type": EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                        "command_payload": {"projection_id": "proj-not-acquisition"},
                    },
                    "budget": {"max_provider_calls": 1, "max_usd": 1.0},
                    "idempotency_key": "continue:reject-export-command",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "invalid")
            self.assertEqual(dispatched["reason"], "unsupported_agent_callable_workflow_command_type")
            self.assertEqual(api_store.list_workflow_commands(limit=0), [])
        finally:
            api_store.close()

    def test_add_to_crm_operation_dispatch_leaves_crm_writes_to_command_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_serving_projection(
                {
                    "projection_id": "proj-crm-op",
                    "projection_type": "run_scope_projection",
                    "collection_id": "company:test",
                    "source_run_id": "job-crm-op",
                    "state": "serving",
                }
            )
            api_store.upsert_serving_projection_members(
                "proj-crm-op",
                [
                    {
                        "candidate_identity_key": "linkedin:crm-op",
                        "person_identity_key": "linkedin:crm-op",
                        "profile_url_key": "crm-op",
                        "rank_index": 1,
                        "public_summary": {
                            "name": "CRM Operation Person",
                            "headline": "Researcher",
                            "current_company": "Example",
                        },
                    }
                ],
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_ADD_TO_CRM,
                    "target_ref": {
                        "projection_id": "proj-crm-op",
                        "candidate_identity_key": "linkedin:crm-op",
                    },
                    "idempotency_key": "add-to-crm:proj-crm-op",
                }
            )
            operation_run_id = submitted["operation_run"]["operation_run_id"]

            dispatched = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(dispatched["status"], "planned")
            self.assertEqual(dispatched["workflow_command"]["command_type"], "crm.record.add_from_projection")
            self.assertEqual(dispatched["workflow_command"]["owner"], "crm_writer")
            self.assertEqual(dispatched["workflow_command"]["operation_id"], operation_run_id)
            self.assertEqual(
                api_store.get_crm_record_by_person_identity("linkedin:crm-op", workspace_id="default"),
                {},
            )

            drain = orchestrator._drain_crm_writer_commands(  # noqa: SLF001
                {"workflow_run_id": dispatched["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )

            self.assertEqual(drain["status"], "active")
            self.assertEqual(drain["completed_count"], 1)
            record = api_store.get_crm_record_by_person_identity("linkedin:crm-op", workspace_id="default")
            self.assertEqual(record["display_name_cache"], "CRM Operation Person")
            operation_after_owner = api_store.get_operation_run(operation_run_id)
            self.assertEqual(operation_after_owner["status"], "completed")
            self.assertEqual(
                api_store.get_agent_action(submitted["action"]["action_id"])["status"],
                "completed",
            )
            self.assertIn(
                "OperationCommandSucceeded",
                [event["event_type"] for event in api_store.list_operation_events(operation_run_id)],
            )
            command_after_owner = api_store.get_workflow_command(dispatched["workflow_command"]["command_id"])
            activity_run_id = command_after_owner["result"]["activity_run_id"]
            self.assertTrue(activity_run_id.startswith("actrun_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command_after_owner["command_id"],
                activity_type=CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["crm_record_count"], 1)
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activity_run_id)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            crm_record_deltas = api_store.list_workflow_entity_deltas(
                command_id=command_after_owner["command_id"],
                activity_run_id=activity_run_id,
                entity_type="crm_record",
            )
            self.assertEqual(len(crm_record_deltas), 1)
            self.assertEqual(crm_record_deltas[0]["delta_kind"], "crm_record_added_from_projection")
            self.assertEqual(crm_record_deltas[0]["entity_key"], record["crm_record_id"])
        finally:
            api_store.close()

    def test_sensitive_crm_stage_operation_requires_approval_before_command_planning(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-approval-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-stage-op",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:stage-op",
                    "display_name_cache": "Stage Person",
                }
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_SET_CRM_STAGE,
                    "target_ref": {"crm_record_id": record["crm_record_id"]},
                    "input": {"stage": "do_not_contact"},
                    "idempotency_key": "stage:do-not-contact",
                }
            )
            operation_run_id = submitted["operation_run"]["operation_run_id"]

            blocked = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(blocked["status"], "approval_required")
            self.assertEqual(api_store.list_workflow_commands(operation_id=operation_run_id, limit=0), [])
            self.assertEqual(
                api_store.get_agent_action(submitted["action"]["action_id"])["status"],
                "approval_required",
            )

            orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            planned = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})
            self.assertEqual(planned["status"], "planned")
            self.assertEqual(planned["workflow_command"]["command_type"], "crm.record.update")

            drain = orchestrator._drain_crm_writer_commands(  # noqa: SLF001
                {"workflow_run_id": planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )

            self.assertEqual(drain["completed_count"], 1)
            updated = api_store.get_crm_record(record["crm_record_id"])
            engagement = api_store.get_crm_engagement(updated["current_engagement_id"])
            self.assertEqual(engagement["stage"], "do_not_contact")
            self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "completed")
        finally:
            api_store.close()

    def test_create_crm_task_operation_materializes_task_only_in_command_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-task-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-task-op",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:task-op",
                    "display_name_cache": "Task Person",
                }
            )
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_CREATE_CRM_TASK,
                    "target_ref": {"crm_record_id": record["crm_record_id"]},
                    "input": {
                        "title": "Follow up",
                        "description": "Check public web evidence.",
                        "due_at": "2026-06-01T00:00:00Z",
                    },
                    "idempotency_key": "crm-task:follow-up",
                }
            )
            operation_run_id = submitted["operation_run"]["operation_run_id"]

            planned = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(planned["status"], "planned")
            self.assertEqual(planned["workflow_command"]["command_type"], "crm.task.create")
            self.assertEqual(planned["workflow_command"]["owner"], "crm_writer")
            self.assertEqual(api_store.list_crm_tasks(crm_record_id=record["crm_record_id"]), [])

            drain = orchestrator._drain_crm_writer_commands(  # noqa: SLF001
                {"workflow_run_id": planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )

            self.assertEqual(drain["completed_count"], 1)
            tasks = api_store.list_crm_tasks(crm_record_id=record["crm_record_id"])
            self.assertEqual(len(tasks), 1)
            self.assertEqual(tasks[0]["title"], "Follow up")
            self.assertEqual(tasks[0]["status"], "open")
            self.assertEqual(tasks[0]["due_at"], "2026-06-01T00:00:00Z")
            self.assertEqual(tasks[0]["source_event_id"][:7], "crmevt_")
            events = api_store.list_operation_events(operation_run_id)
            self.assertIn("OperationCommandSucceeded", [event["event_type"] for event in events])
            self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "completed")
            command_after_owner = api_store.get_workflow_command(planned["workflow_command"]["command_id"])
            activity_run_id = command_after_owner["result"]["activity_run_id"]
            self.assertTrue(activity_run_id.startswith("actrun_"))
            activities = api_store.list_workflow_activity_runs(
                command_id=command_after_owner["command_id"],
                activity_type=CRM_TASK_CREATE_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["crm_task_count"], 1)
            attempts = api_store.list_workflow_activity_attempts(activity_run_id=activity_run_id)
            self.assertEqual(len(attempts), 1)
            self.assertEqual(attempts[0]["status"], "succeeded")
            task_deltas = api_store.list_workflow_entity_deltas(
                command_id=command_after_owner["command_id"],
                activity_run_id=activity_run_id,
                entity_type="crm_task",
            )
            self.assertEqual(len(task_deltas), 1)
            self.assertEqual(task_deltas[0]["delta_kind"], "crm_task_created")
            self.assertEqual(task_deltas[0]["entity_key"], tasks[0]["task_id"])
        finally:
            api_store.close()

    def test_crm_writer_running_resume_requeues_without_direct_crm_write(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-writer-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-writer-resume",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:crm-writer-resume",
                    "display_name_cache": "CRM Resume Person",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-crm-writer-resume",
                operation_id="op-crm-writer-resume",
                command_type=CRM_TASK_CREATE_COMMAND_TYPE,
                owner=CRM_WRITER_OWNER,
                idempotency_key="crm.task.create:resume",
                payload={
                    "workspace_id": "default",
                    "crm_record_ids": [record["crm_record_id"]],
                    "title": "Review resumed CRM write",
                    "description": "Should be created only by crm_writer owner drain.",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="crm-writer-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="crm-writer-worker")

            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-crm-write", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "crm_writer.cancel_before_mutation_attempt",
            )
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "crm_writer.resume_crm_writer_command",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["crm_mutation_policy"],
                "resume_requeues_without_direct_crm_write",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["entity_type"], "crm_write_command")
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertTrue(
                forced_resume["workflow_entity_delta"]["projection_effect"][
                    "crm_mutation_deferred_to_command_owner"
                ]
            )
            self.assertEqual(api_store.list_crm_tasks(crm_record_id=record["crm_record_id"]), [])
        finally:
            api_store.close()

    def test_crm_writer_running_cancel_marks_activity_before_mutation_attempt(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-writer-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-writer-cancel",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:crm-writer-cancel",
                    "display_name_cache": "CRM Cancel Person",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-crm-writer-cancel",
                operation_id="op-crm-writer-cancel",
                command_type=CRM_TASK_CREATE_COMMAND_TYPE,
                owner=CRM_WRITER_OWNER,
                idempotency_key="crm.task.create:cancel",
                payload={
                    "workspace_id": "default",
                    "crm_record_ids": [record["crm_record_id"]],
                    "title": "Review cancelled CRM write",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-crm-writer-cancel",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-crm-writer-cancel",
                    "operation_run_id": "op-crm-writer-cancel",
                    "command_id": command["command_id"],
                    "activity_type": CRM_TASK_CREATE_COMMAND_TYPE,
                    "owner": CRM_WRITER_OWNER,
                    "status": "planned",
                    "phase": "crm_writer_pending",
                    "idempotency_key": "workflow_activity:crm-writer-cancel",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="crm-writer-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="crm-writer-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator_cancelled_before_crm_mutation", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertTrue(response["owner_specific_control"])
            self.assertEqual(
                response["control_policy"]["running_cancel_delegate"],
                "crm_writer.cancel_before_mutation_attempt",
            )
            self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
            self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "crm_writer_before_mutation_attempt")
            self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
            cancelled_activity = api_store.get_workflow_activity_run(activity["activity_run_id"])
            self.assertEqual(cancelled_activity["status"], "cancelled_before_crm_mutation")
            self.assertEqual(cancelled_activity["phase"], "cancelled")
            self.assertEqual(api_store.list_crm_tasks(crm_record_id=record["crm_record_id"]), [])
        finally:
            api_store.close()

    def test_crm_writer_running_cancel_blocks_after_mutation_attempt_started(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "crm-writer-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            record = api_store.upsert_crm_record(
                {
                    "crm_record_id": "crmrec-writer-cancel-blocked",
                    "workspace_id": "default",
                    "person_identity_key": "linkedin:crm-writer-cancel-blocked",
                    "display_name_cache": "CRM Cancel Blocked Person",
                }
            )
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-crm-writer-cancel-blocked",
                operation_id="op-crm-writer-cancel-blocked",
                command_type=CRM_TASK_CREATE_COMMAND_TYPE,
                owner=CRM_WRITER_OWNER,
                idempotency_key="crm.task.create:cancel-blocked",
                payload={
                    "workspace_id": "default",
                    "crm_record_ids": [record["crm_record_id"]],
                    "title": "Review blocked CRM write",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-crm-writer-cancel-blocked",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-crm-writer-cancel-blocked",
                    "operation_run_id": "op-crm-writer-cancel-blocked",
                    "command_id": command["command_id"],
                    "activity_type": CRM_TASK_CREATE_COMMAND_TYPE,
                    "owner": CRM_WRITER_OWNER,
                    "status": "running",
                    "phase": "crm_writer_started",
                    "idempotency_key": "workflow_activity:crm-writer-cancel-blocked",
                }
            )
            api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt-crm-writer-cancel-blocked",
                    "workspace_id": "default",
                    "activity_run_id": activity["activity_run_id"],
                    "workflow_run_id": "wf-crm-writer-cancel-blocked",
                    "command_id": command["command_id"],
                    "attempt_number": 1,
                    "status": "running",
                    "provider": "crm_writer",
                    "idempotency_key": "workflow_activity_attempt:crm-writer-cancel-blocked",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="crm-writer-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="crm-writer-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "too_late", "force": True},
            )

            self.assertEqual(response["status"], "invalid")
            self.assertEqual(response["reason"], "crm_writer_cancel_blocked_after_mutation_attempt_started")
            self.assertEqual(response["activity_attempt_count"], 1)
            self.assertFalse(response["module_state_mutated"])
            self.assertEqual(api_store.get_workflow_activity_run(activity["activity_run_id"])["status"], "running")
            self.assertEqual(api_store.list_crm_tasks(crm_record_id=record["crm_record_id"]), [])
        finally:
            api_store.close()

    def test_company_public_web_operation_refreshes_company_assets_only_in_command_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-operation.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_REFRESH_COMPANY_PUBLIC_WEB,
                    "target_ref": {"target_company": "OpenAI", "company_key": "openai"},
                    "input": {
                        "collection_mode": "seed_url_only",
                        "source_families": ["homepage"],
                        "seed_urls": ["https://openai.com/"],
                        "force_refresh": "false",
                    },
                    "budget": {"max_provider_calls": 0, "max_usd": 0.0},
                    "idempotency_key": "company-public-web:openai-homepage",
                }
            )
            approved = orchestrator.approve_operation_action_api(submitted["action"]["action_id"], {"actor": "unit-test"})
            operation_run_id = approved["operation_run"]["operation_run_id"]

            planned = orchestrator.dispatch_operation_run_api(operation_run_id, {"actor": "unit-test"})

            self.assertEqual(planned["status"], "planned")
            self.assertEqual(planned["workflow_command"]["command_type"], COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE)
            self.assertEqual(planned["workflow_command"]["owner"], COMPANY_PUBLIC_WEB_REFRESH_OWNER)
            self.assertEqual(planned["workflow_command"]["operation_id"], operation_run_id)
            self.assertFalse(planned["workflow_command"]["payload"]["force_refresh"])
            self.assertEqual(api_store.list_company_public_web_asset_runs(company_key="openai"), [])
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])

            root_drain = orchestrator._drain_company_public_web_refresh_commands(  # noqa: SLF001
                {"workflow_run_id": planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )

            self.assertEqual(root_drain["completed_count"], 1)
            root_after_owner = api_store.get_workflow_command(planned["workflow_command"]["command_id"])
            self.assertEqual(root_after_owner["status"], "succeeded")
            self.assertTrue(root_after_owner["result"]["operation_completion_deferred"])
            self.assertEqual(
                root_after_owner["result"]["downstream_command_types"],
                [COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE],
            )
            self.assertEqual(api_store.list_company_public_web_asset_runs(company_key="openai"), [])
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])

            source_drain = orchestrator._drain_company_public_web_refresh_commands(  # noqa: SLF001
                {"workflow_run_id": planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )
            self.assertEqual(source_drain["completed_count"], 1)
            runs = api_store.list_company_public_web_asset_runs(company_key="openai")
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs[0]["status"], "completed")
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])

            joined_submitted = orchestrator.submit_operation_action(
                {
                    "action_type": ACTION_REFRESH_COMPANY_PUBLIC_WEB,
                    "target_ref": {"target_company": "OpenAI", "company_key": "openai"},
                    "input": {
                        "collection_mode": "seed_url_only",
                        "source_families": ["homepage"],
                        "seed_urls": ["https://openai.com/"],
                        "force_refresh": "false",
                    },
                    "budget": {"max_provider_calls": 0, "max_usd": 0.0},
                    "idempotency_key": "company-public-web:openai-homepage-joined",
                }
            )
            joined_approved = orchestrator.approve_operation_action_api(
                joined_submitted["action"]["action_id"],
                {"actor": "unit-test"},
            )
            joined_operation_run_id = joined_approved["operation_run"]["operation_run_id"]
            joined_planned = orchestrator.dispatch_operation_run_api(joined_operation_run_id, {"actor": "unit-test"})
            joined_root_drain = orchestrator._drain_company_public_web_refresh_commands(  # noqa: SLF001
                {"workflow_run_id": joined_planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )
            self.assertEqual(joined_root_drain["completed_count"], 1)
            joined_source_drain = orchestrator._drain_company_public_web_refresh_commands(  # noqa: SLF001
                {"workflow_run_id": joined_planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )
            self.assertEqual(joined_source_drain["completed_count"], 1)
            joined_root_after_owner = api_store.get_workflow_command(joined_planned["workflow_command"]["command_id"])
            joined_source_command = api_store.get_workflow_command(joined_root_after_owner["result"]["downstream_command_ids"][0])
            self.assertEqual(joined_source_command["command_type"], COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE)
            self.assertEqual(joined_source_command["status"], "succeeded")
            self.assertEqual(joined_source_command["result"]["reason"], "company_public_web_refresh_joined_existing_run")
            self.assertEqual(
                joined_source_command["result"]["company_asset_sync"]["reason"],
                "company_asset_sync_deferred_to_typed_command",
            )
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])

            materialize_drain = orchestrator._drain_company_public_web_refresh_commands(  # noqa: SLF001
                {"workflow_run_id": planned["workflow_command"]["workflow_run_id"], "command_limit": 1}
            )
            self.assertEqual(materialize_drain["completed_count"], 1)
            assets = api_store.list_company_assets(company_key="openai")
            evidence = api_store.list_company_evidence(company_key="openai")
            self.assertEqual(len(assets), 1)
            self.assertEqual(len(evidence), 1)
            self.assertEqual(assets[0]["source_kind"], "company_public_web_model_safe")
            self.assertEqual(assets[0]["content_ref"], "https://openai.com/")
            source_command_id = root_after_owner["result"]["downstream_command_ids"][0]
            source_command = api_store.get_workflow_command(source_command_id)
            self.assertEqual(source_command["command_type"], COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE)
            self.assertEqual(source_command["status"], "succeeded")
            self.assertTrue(source_command["result"]["operation_completion_deferred"])
            materialize_command_id = source_command["result"]["downstream_command_ids"][0]
            command_after_owner = api_store.get_workflow_command(materialize_command_id)
            self.assertEqual(command_after_owner["command_type"], COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE)
            self.assertEqual(command_after_owner["status"], "succeeded")
            self.assertEqual(command_after_owner["result"]["activity_spine_contract"], "command_activity_attempt_entity_delta_v1")
            activity_run_id = command_after_owner["result"]["activity_run_id"]
            activities = api_store.list_workflow_activity_runs(
                command_id=command_after_owner["command_id"],
                activity_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            self.assertEqual(activities[0]["entity_counts"]["company_asset_count"], 1)
            run_deltas = api_store.list_workflow_entity_deltas(
                command_id=command_after_owner["command_id"],
                activity_run_id=activity_run_id,
                entity_type="company_public_web_run",
            )
            asset_deltas = api_store.list_workflow_entity_deltas(
                command_id=command_after_owner["command_id"],
                activity_run_id=activity_run_id,
                entity_type="company_asset",
            )
            self.assertEqual(len(run_deltas), 1)
            self.assertEqual(len(asset_deltas), 1)
            self.assertEqual(asset_deltas[0]["delta_kind"], "company_asset_synced_from_public_web")
            self.assertEqual(api_store.get_operation_run(operation_run_id)["status"], "completed")
        finally:
            api_store.close()

    def test_company_public_web_source_collect_running_resume_requeues_without_asset_sync(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-source-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-company-public-web-source-resume",
                operation_id="op-company-public-web-source-resume",
                command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key="company.public_web.source.collect:resume",
                payload={
                    "target_company": "OpenAI",
                    "company_key": "openai",
                    "collection_mode": "seed_url_only",
                    "seed_urls": ["https://openai.com/"],
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="company-web-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="company-web-worker")
            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-source-collect", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "company_public_web_owner.cancel_or_poll_stop_source_collect",
            )
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "company_public_web_owner.resume_source_collect",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["canonical_asset_sync"],
                "deferred_to_company.public_web.assets.materialize",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["entity_type"], "company_public_web_source_collect")
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])
        finally:
            api_store.close()

    def test_company_public_web_source_collect_running_cancel_marks_activity_before_provider_attempt(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-source-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-company-public-web-source-cancel",
                operation_id="op-company-public-web-source-cancel",
                command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key="company.public_web.source.collect:cancel",
                payload={
                    "target_company": "OpenAI",
                    "company_key": "openai",
                    "collection_mode": "seed_url_only",
                    "seed_urls": ["https://openai.com/"],
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-company-public-web-source-cancel",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-company-public-web-source-cancel",
                    "operation_run_id": "op-company-public-web-source-cancel",
                    "command_id": command["command_id"],
                    "activity_type": COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
                    "owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                    "status": "planned",
                    "phase": "company_public_web_source_pending",
                    "idempotency_key": "workflow_activity:company-public-web-source-cancel",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="company-web-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="company-web-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator_cancelled_before_source_collect", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertTrue(response["owner_specific_control"])
            self.assertEqual(
                response["control_policy"]["running_cancel_delegate"],
                "company_public_web_owner.cancel_or_poll_stop_source_collect",
            )
            self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
            self.assertEqual(response["workflow_command"]["result"]["cancel_boundary"], "provider_attempt_before_activity_attempt")
            self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
            cancelled_activity = api_store.get_workflow_activity_run(activity["activity_run_id"])
            self.assertEqual(cancelled_activity["status"], "cancelled_before_provider_attempt")
            self.assertEqual(cancelled_activity["phase"], "cancelled")
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])
        finally:
            api_store.close()

    def test_company_public_web_assets_materialize_running_resume_requeues_without_asset_sync(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-materialize-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-company-public-web-materialize-resume",
                operation_id="op-company-public-web-materialize-resume",
                command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key="company.public_web.assets.materialize:resume",
                payload={
                    "target_company": "OpenAI",
                    "company_key": "openai",
                    "run_id": "cpwrun-openai-resume",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="company-web-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="company-web-worker")
            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-materialize", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "company_public_web_owner.cancel_assets_materialize_before_sync",
            )
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "company_public_web_owner.resume_assets_materialize",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["company_asset_sync_policy"],
                "resume_requeues_without_request_path_sync",
            )
            self.assertEqual(
                forced_resume["workflow_entity_delta"]["entity_type"],
                "company_public_web_assets_materialize",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])
        finally:
            api_store.close()

    def test_company_public_web_assets_materialize_running_cancel_marks_activity_before_sync(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-materialize-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-company-public-web-materialize-cancel",
                operation_id="op-company-public-web-materialize-cancel",
                command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key="company.public_web.assets.materialize:cancel",
                payload={
                    "target_company": "OpenAI",
                    "company_key": "openai",
                    "run_id": "cpwrun-openai-cancel",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-company-public-web-materialize-cancel",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-company-public-web-materialize-cancel",
                    "operation_run_id": "op-company-public-web-materialize-cancel",
                    "command_id": command["command_id"],
                    "activity_type": COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                    "owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                    "status": "planned",
                    "phase": "company_asset_sync_pending",
                    "idempotency_key": "workflow_activity:company-public-web-materialize-cancel",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="company-web-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="company-web-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator_cancelled_before_company_asset_sync", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertTrue(response["owner_specific_control"])
            self.assertEqual(
                response["control_policy"]["running_cancel_delegate"],
                "company_public_web_owner.cancel_assets_materialize_before_sync",
            )
            self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
            self.assertEqual(
                response["workflow_command"]["result"]["cancel_boundary"],
                "company_public_web_assets_materialize_before_sync",
            )
            self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
            cancelled_activity = api_store.get_workflow_activity_run(activity["activity_run_id"])
            self.assertEqual(cancelled_activity["status"], "cancelled_before_company_asset_sync")
            self.assertEqual(cancelled_activity["phase"], "cancelled")
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])
        finally:
            api_store.close()

    def test_company_public_web_assets_materialize_running_cancel_blocks_after_sync_started(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-public-web-materialize-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-company-public-web-materialize-cancel-blocked",
                operation_id="op-company-public-web-materialize-cancel-blocked",
                command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                idempotency_key="company.public_web.assets.materialize:cancel-blocked",
                payload={
                    "target_company": "OpenAI",
                    "company_key": "openai",
                    "run_id": "cpwrun-openai-cancel-blocked",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-company-public-web-materialize-cancel-blocked",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-company-public-web-materialize-cancel-blocked",
                    "operation_run_id": "op-company-public-web-materialize-cancel-blocked",
                    "command_id": command["command_id"],
                    "activity_type": COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
                    "owner": COMPANY_PUBLIC_WEB_REFRESH_OWNER,
                    "status": "running",
                    "phase": "company_public_web_assets_materialize_started",
                    "idempotency_key": "workflow_activity:company-public-web-materialize-cancel-blocked",
                }
            )
            api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt-company-public-web-materialize-cancel-blocked",
                    "workspace_id": "default",
                    "activity_run_id": activity["activity_run_id"],
                    "workflow_run_id": "wf-company-public-web-materialize-cancel-blocked",
                    "command_id": command["command_id"],
                    "attempt_number": 1,
                    "status": "running",
                    "provider": "company_public_web_assets_materialize",
                    "idempotency_key": "workflow_activity_attempt:company-public-web-materialize-cancel-blocked",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="company-web-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="company-web-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "too_late", "force": True},
            )

            self.assertEqual(response["status"], "invalid")
            self.assertEqual(
                response["reason"],
                "company_public_web_assets_materialize_cancel_blocked_after_sync_started",
            )
            self.assertEqual(response["activity_attempt_count"], 1)
            self.assertFalse(response["module_state_mutated"])
            self.assertEqual(api_store.get_workflow_activity_run(activity["activity_run_id"])["status"], "running")
            self.assertEqual(api_store.list_company_assets(company_key="openai"), [])
        finally:
            api_store.close()

    def test_media_asset_cache_command_materializes_stable_person_and_company_assets(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "media-asset-cache.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            tiny_png = (
                b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
                b"\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4"
                b"\x89\x00\x00\x00\nIDATx\x9cc`\x00\x00\x00\x02"
                b"\x00\x01\xe2!\xbc3\x00\x00\x00\x00IEND\xaeB`\x82"
            )
            payload_bytes_base64 = base64.b64encode(tiny_png).decode("ascii")
            person_command = api_store.upsert_workflow_command(
                workflow_run_id="wf_media_asset_cache",
                command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
                owner=MEDIA_ASSET_OWNER,
                idempotency_key="media-cache:person:ada",
                payload={
                    "workspace_id": "default",
                    "entity_type": "person",
                    "person_identity_key": "linkedin:ada-lovelace",
                    "asset_type": "avatar_media",
                    "payload_bytes_base64": payload_bytes_base64,
                    "content_type": "image/png",
                    "source_url": "https://provider.example/avatar/ada.png",
                    "stage_id": "media_asset_cache",
                    "causal_group_id": "media-cache:ada",
                    "source_event_id": "evt-media-person",
                    "source_event_type": "MediaAssetCacheRequested",
                    "readiness_effect": "media_asset_cached",
                },
                max_attempts=2,
                retry_policy={"kind": "media_asset_cache", "retry_delay_seconds": 5},
            )
            company_command = api_store.upsert_workflow_command(
                workflow_run_id="wf_media_asset_cache",
                command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
                owner=MEDIA_ASSET_OWNER,
                idempotency_key="media-cache:company:openai",
                payload={
                    "workspace_id": "default",
                    "entity_type": "company",
                    "company_key": "openai",
                    "target_company": "OpenAI",
                    "asset_type": "logo_media",
                    "payload_bytes_base64": payload_bytes_base64,
                    "content_type": "image/png",
                    "source_url": "https://openai.com/favicon.png",
                    "stage_id": "media_asset_cache",
                    "causal_group_id": "media-cache:openai",
                    "source_event_id": "evt-media-company",
                    "source_event_type": "MediaAssetCacheRequested",
                    "readiness_effect": "media_asset_cached",
                },
                max_attempts=2,
                retry_policy={"kind": "media_asset_cache", "retry_delay_seconds": 5},
            )

            drain = orchestrator._drain_media_asset_cache_commands(  # noqa: SLF001
                {"workflow_run_id": "wf_media_asset_cache", "command_limit": 2}
            )

            self.assertEqual(drain["completed_count"], 2)
            person_assets = api_store.list_person_assets(
                person_identity_key="linkedin:ada-lovelace",
                asset_type="avatar_media",
            )
            company_assets = api_store.list_company_assets(company_key="openai", asset_type="logo_media")
            self.assertEqual(len(person_assets), 1)
            self.assertEqual(len(company_assets), 1)
            self.assertEqual(person_assets[0]["source_kind"], "media_asset_cache")
            self.assertTrue(person_assets[0]["content_ref"].startswith("file://"))
            self.assertEqual(company_assets[0]["source_command_id"], company_command["command_id"])
            self.assertTrue(company_assets[0]["content_ref"].startswith("file://"))
            served = orchestrator.get_media_asset_content_api(person_assets[0]["asset_id"])
            self.assertEqual(served["status"], "ready")
            self.assertEqual(served["content_type"], "image/png")
            self.assertEqual(served["content"], tiny_png)
            server = create_server(orchestrator, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            host, port = server.server_address
            try:
                response = urllib_request.build_opener(urllib_request.ProxyHandler({})).open(
                    f"http://{host}:{port}/api/media/assets/{person_assets[0]['asset_id']}",
                    timeout=5,
                )
                self.assertEqual(response.status, 200)
                self.assertEqual(response.headers.get("X-Media-Asset-Contract"), "media_asset_read_contract_v1")
                self.assertEqual(response.read(), tiny_png)
            finally:
                server.shutdown()
                thread.join(timeout=2)
            person_after = api_store.get_workflow_command(person_command["command_id"])
            company_after = api_store.get_workflow_command(company_command["command_id"])
            self.assertEqual(person_after["status"], "succeeded")
            self.assertEqual(company_after["status"], "succeeded")
            self.assertEqual(person_after["result"]["activity_spine_contract"], "command_activity_attempt_entity_delta_v1")
            activities = api_store.list_workflow_activity_runs(
                command_id=person_command["command_id"],
                activity_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
            )
            self.assertEqual(len(activities), 1)
            self.assertEqual(activities[0]["status"], "succeeded")
            person_deltas = api_store.list_workflow_entity_deltas(
                command_id=person_command["command_id"],
                activity_run_id=activities[0]["activity_run_id"],
                entity_type="person_asset",
            )
            company_deltas = api_store.list_workflow_entity_deltas(
                command_id=company_command["command_id"],
                entity_type="company_asset",
            )
            self.assertEqual(len(person_deltas), 1)
            self.assertEqual(person_deltas[0]["entity_key"], person_assets[0]["asset_id"])
            self.assertEqual(len(company_deltas), 1)
            self.assertEqual(company_deltas[0]["entity_key"], company_assets[0]["asset_id"])
        finally:
            api_store.close()

    def test_media_asset_cache_running_resume_requeues_without_request_path_fetch(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "media-asset-cache-resume.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-media-cache-resume",
                operation_id="op-media-cache-resume",
                command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
                owner=MEDIA_ASSET_OWNER,
                idempotency_key="media.asset.cache:resume",
                payload={
                    "entity_type": "person",
                    "asset_type": "avatar_media",
                    "person_identity_key": "linkedin:media-cache-resume",
                    "source_url": "https://provider.example.com/avatar/resume.png",
                },
                max_attempts=6,
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="media-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="media-worker")
            running_before_resume = orchestrator.get_workflow_command_api(command["command_id"])
            active_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "active-lease"},
            )
            forced_resume = orchestrator.resume_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator-reviewed-stale-media-cache", "force": True},
            )

            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_resume"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_resume_delegate"],
                "media_asset_owner.resume_media_asset_cache",
            )
            self.assertTrue(running_before_resume["workflow_command"]["control_state"]["can_cancel"])
            self.assertEqual(
                running_before_resume["workflow_command"]["control_policy"]["running_cancel_delegate"],
                "media_asset_owner.cancel_before_fetch_upload_attempt",
            )
            self.assertEqual(active_resume["status"], "invalid")
            self.assertEqual(
                active_resume["reason"],
                "workflow_command_running_resume_requires_expired_lease_or_force",
            )
            self.assertEqual(forced_resume["status"], "queued")
            self.assertEqual(forced_resume["workflow_command"]["status"], "queued")
            self.assertEqual(
                forced_resume["workflow_command"]["result"]["asset_publish_policy"],
                "resume_requeues_without_request_path_fetch",
            )
            self.assertEqual(forced_resume["workflow_entity_delta"]["entity_type"], "media_asset_cache")
            self.assertEqual(forced_resume["workflow_entity_delta"]["status"], "queued")
            self.assertEqual(
                api_store.list_person_assets(
                    person_identity_key="linkedin:media-cache-resume",
                    asset_type="avatar_media",
                ),
                [],
            )
        finally:
            api_store.close()

    def test_media_asset_cache_running_cancel_marks_activity_before_fetch_upload_attempt(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "media-asset-cache-cancel.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-media-cache-cancel",
                operation_id="op-media-cache-cancel",
                command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
                owner=MEDIA_ASSET_OWNER,
                idempotency_key="media.asset.cache:cancel",
                payload={
                    "entity_type": "person",
                    "asset_type": "avatar_media",
                    "person_identity_key": "linkedin:media-cache-cancel",
                    "source_url": "https://provider.example.com/avatar/cancel.png",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-media-cache-cancel",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-media-cache-cancel",
                    "operation_run_id": "op-media-cache-cancel",
                    "command_id": command["command_id"],
                    "activity_type": MEDIA_ASSET_CACHE_COMMAND_TYPE,
                    "owner": MEDIA_ASSET_OWNER,
                    "status": "planned",
                    "phase": "media_fetch_pending",
                    "idempotency_key": "workflow_activity:media-cache-cancel",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="media-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="media-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "operator_cancelled_before_media_fetch", "force": True},
            )

            self.assertEqual(response["status"], "cancelled")
            self.assertTrue(response["owner_specific_control"])
            self.assertEqual(
                response["control_policy"]["running_cancel_delegate"],
                "media_asset_owner.cancel_before_fetch_upload_attempt",
            )
            self.assertEqual(response["control_policy"]["running_control_maturity"], "owner_specific_cancel_resume")
            self.assertEqual(
                response["workflow_command"]["result"]["cancel_boundary"],
                "media_asset_cache_before_fetch_upload_attempt",
            )
            self.assertEqual(response["workflow_command"]["result"]["activity_run_cancelled_count"], 1)
            cancelled_activity = api_store.get_workflow_activity_run(activity["activity_run_id"])
            self.assertEqual(cancelled_activity["status"], "cancelled_before_fetch_upload")
            self.assertEqual(cancelled_activity["phase"], "cancelled")
            self.assertEqual(
                api_store.list_person_assets(
                    person_identity_key="linkedin:media-cache-cancel",
                    asset_type="avatar_media",
                ),
                [],
            )
        finally:
            api_store.close()

    def test_media_asset_cache_running_cancel_blocks_after_fetch_upload_attempt_started(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "media-asset-cache-cancel-blocked.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=AssetCatalog.discover(),
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=DeterministicModelClient(),
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(
                AssetCatalog.discover(),
                settings,
                api_store,
                DeterministicModelClient(),
            ),
        )
        try:
            command = api_store.upsert_workflow_command(
                workflow_run_id="wf-media-cache-cancel-blocked",
                operation_id="op-media-cache-cancel-blocked",
                command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
                owner=MEDIA_ASSET_OWNER,
                idempotency_key="media.asset.cache:cancel-blocked",
                payload={
                    "entity_type": "person",
                    "asset_type": "avatar_media",
                    "person_identity_key": "linkedin:media-cache-cancel-blocked",
                    "source_url": "https://provider.example.com/avatar/cancel-blocked.png",
                },
                max_attempts=6,
            )
            activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun-media-cache-cancel-blocked",
                    "workspace_id": "default",
                    "workflow_run_id": "wf-media-cache-cancel-blocked",
                    "operation_run_id": "op-media-cache-cancel-blocked",
                    "command_id": command["command_id"],
                    "activity_type": MEDIA_ASSET_CACHE_COMMAND_TYPE,
                    "owner": MEDIA_ASSET_OWNER,
                    "status": "running",
                    "phase": "media_asset_cache_started",
                    "idempotency_key": "workflow_activity:media-cache-cancel-blocked",
                }
            )
            api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt-media-cache-cancel-blocked",
                    "workspace_id": "default",
                    "activity_run_id": activity["activity_run_id"],
                    "workflow_run_id": "wf-media-cache-cancel-blocked",
                    "command_id": command["command_id"],
                    "attempt_number": 1,
                    "status": "running",
                    "provider": "object_storage_media_cache",
                    "idempotency_key": "workflow_activity_attempt:media-cache-cancel-blocked",
                }
            )
            api_store.claim_workflow_command(command["command_id"], lease_owner="media-worker", lease_seconds=60)
            api_store.mark_workflow_command_running(command["command_id"], lease_owner="media-worker")

            response = orchestrator.cancel_workflow_command_api(
                command["command_id"],
                {"actor": "unit-test", "reason": "too_late", "force": True},
            )

            self.assertEqual(response["status"], "invalid")
            self.assertEqual(response["reason"], "media_asset_cache_cancel_blocked_after_fetch_upload_attempt_started")
            self.assertEqual(response["activity_attempt_count"], 1)
            self.assertFalse(response["module_state_mutated"])
            self.assertEqual(api_store.get_workflow_activity_run(activity["activity_run_id"])["status"], "running")
            self.assertEqual(
                api_store.list_person_assets(
                    person_identity_key="linkedin:media-cache-cancel-blocked",
                    asset_type="avatar_media",
                ),
                [],
            )
        finally:
            api_store.close()

    def test_person_avatar_media_backfill_plans_media_cache_commands_only(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "person-avatar-media-backfill.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_serving_projection(
                {
                    "projection_id": "proj-avatar-backfill",
                    "projection_type": "collection_authoritative_projection",
                    "collection_id": "company:openai",
                    "source_run_id": "asset-backfill-source",
                    "state": "serving",
                }
            )
            api_store.upsert_serving_projection_members(
                "proj-avatar-backfill",
                [
                    {
                        "candidate_identity_key": "linkedin:avatar-backfill-ada",
                        "person_identity_key": "linkedin:avatar-backfill-ada",
                        "profile_url_key": "avatar-backfill-ada",
                        "rank_index": 1,
                        "public_summary": {
                            "name": "Avatar Backfill Ada",
                            "candidate_id": "cand-avatar-backfill-ada",
                            "linkedin_url": "https://www.linkedin.com/in/avatar-backfill-ada",
                            "avatar_url": "https://provider.example.com/avatar/ada.png",
                        },
                    },
                    {
                        "candidate_identity_key": "linkedin:avatar-backfill-no-url",
                        "person_identity_key": "linkedin:avatar-backfill-no-url",
                        "profile_url_key": "avatar-backfill-no-url",
                        "rank_index": 2,
                        "public_summary": {"name": "No Avatar Url"},
                    },
                ],
            )

            dry_run = orchestrator.backfill_person_avatar_media_assets_api(
                {
                    "projection_id": "proj-avatar-backfill",
                    "dry_run": True,
                    "workflow_run_id": "wf-avatar-backfill",
                }
            )

            self.assertEqual(dry_run["status"], "dry_run")
            self.assertEqual(dry_run["eligible_command_count"], 1)
            self.assertEqual(dry_run["skipped_missing_url_count"], 1)
            self.assertFalse(dry_run["module_state_mutated"])
            self.assertFalse(dry_run["read_contract"]["normal_reader_repair"])
            self.assertFalse(dry_run["read_contract"]["request_path_fetch"])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-avatar-backfill"), [])

            rejected = orchestrator.backfill_person_avatar_media_assets_api(
                {
                    "projection_id": "proj-avatar-backfill",
                    "dry_run": False,
                    "workflow_run_id": "wf-avatar-backfill",
                }
            )

            self.assertEqual(rejected["status"], "invalid")
            self.assertEqual(rejected["reason"], "operator_review_required")
            self.assertFalse(rejected["module_state_mutated"])
            self.assertTrue(rejected["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-avatar-backfill"), [])

            planned = orchestrator.backfill_person_avatar_media_assets_api(
                {
                    "projection_id": "proj-avatar-backfill",
                    "dry_run": False,
                    "reviewed": True,
                    "workflow_run_id": "wf-avatar-backfill",
                }
            )

            self.assertEqual(planned["status"], "planned")
            self.assertEqual(planned["planned_command_count"], 1)
            self.assertTrue(planned["module_state_mutated"])
            self.assertFalse(planned["run_now"])
            self.assertFalse(planned["read_contract"]["request_path_fetch"])
            commands = api_store.list_workflow_commands(
                workflow_run_id="wf-avatar-backfill",
                owner=MEDIA_ASSET_OWNER,
            )
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["command_type"], MEDIA_ASSET_CACHE_COMMAND_TYPE)
            self.assertEqual(commands[0]["status"], "queued")
            self.assertEqual(commands[0]["payload"]["entity_type"], "person")
            self.assertEqual(commands[0]["payload"]["asset_type"], "avatar_media")
            self.assertEqual(commands[0]["payload"]["person_identity_key"], "linkedin:avatar-backfill-ada")
            self.assertEqual(commands[0]["payload"]["source_url"], "https://provider.example.com/avatar/ada.png")
            self.assertEqual(commands[0]["payload"]["source_projection_id"], "proj-avatar-backfill")
            self.assertEqual(
                api_store.list_person_assets(
                    person_identity_key="linkedin:avatar-backfill-ada",
                    asset_type="avatar_media",
                ),
                [],
            )
        finally:
            api_store.close()

    def test_company_logo_media_backfill_plans_media_cache_commands_only(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "company-logo-media-backfill.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            dry_run = orchestrator.backfill_company_logo_media_assets_api(
                {
                    "target_company": "OpenAI",
                    "source_url": "https://static.example.com/openai-logo.png",
                    "dry_run": True,
                    "workflow_run_id": "wf-company-logo-backfill",
                }
            )

            self.assertEqual(dry_run["status"], "dry_run")
            self.assertEqual(dry_run["company_key"], "openai")
            self.assertEqual(dry_run["eligible_command_count"], 1)
            self.assertFalse(dry_run["module_state_mutated"])
            self.assertFalse(dry_run["read_contract"]["normal_reader_repair"])
            self.assertFalse(dry_run["read_contract"]["request_path_fetch"])
            self.assertFalse(dry_run["read_contract"]["homepage_favicon_derivation_default"])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-company-logo-backfill"), [])

            rejected = orchestrator.backfill_company_logo_media_assets_api(
                {
                    "target_company": "OpenAI",
                    "source_url": "https://static.example.com/openai-logo.png",
                    "dry_run": False,
                    "workflow_run_id": "wf-company-logo-backfill",
                }
            )

            self.assertEqual(rejected["status"], "invalid")
            self.assertEqual(rejected["reason"], "operator_review_required")
            self.assertFalse(rejected["module_state_mutated"])
            self.assertTrue(rejected["read_contract"]["apply_requires_reviewed"])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-company-logo-backfill"), [])

            planned = orchestrator.backfill_company_logo_media_assets_api(
                {
                    "target_company": "OpenAI",
                    "source_url": "https://static.example.com/openai-logo.png",
                    "dry_run": False,
                    "reviewed": True,
                    "workflow_run_id": "wf-company-logo-backfill",
                }
            )

            self.assertEqual(planned["status"], "planned")
            self.assertEqual(planned["planned_command_count"], 1)
            self.assertFalse(planned["run_now"])
            self.assertFalse(planned["read_contract"]["request_path_fetch"])
            commands = api_store.list_workflow_commands(
                workflow_run_id="wf-company-logo-backfill",
                owner=MEDIA_ASSET_OWNER,
            )
            self.assertEqual(len(commands), 1)
            self.assertEqual(commands[0]["command_type"], MEDIA_ASSET_CACHE_COMMAND_TYPE)
            self.assertEqual(commands[0]["status"], "queued")
            self.assertEqual(commands[0]["payload"]["entity_type"], "company")
            self.assertEqual(commands[0]["payload"]["asset_type"], "logo_media")
            self.assertEqual(commands[0]["payload"]["company_key"], "openai")
            self.assertEqual(commands[0]["payload"]["target_company"], "OpenAI")
            self.assertEqual(commands[0]["payload"]["source_url"], "https://static.example.com/openai-logo.png")
            self.assertEqual(api_store.list_company_assets(company_key="openai", asset_type="logo_media"), [])
        finally:
            api_store.close()

    def test_profile_experience_company_logo_plans_evidence_and_media_cache(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-company-logo-ingest.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            profile_payload = {
                "experience": [
                    {
                        "companyName": "OpenAI",
                        "title": "Research Engineer",
                        "companyLogo": {
                            "url": "https://media.licdn.example/openai-logo.png?e=4102444800",
                            "sizes": [
                                {
                                    "url": "https://media.licdn.example/openai-logo-large.png?e=4102444800",
                                    "expiresAt": 4102444800000,
                                }
                            ],
                        },
                    }
                ]
            }

            dry_run = orchestrator.ingest_company_logo_from_profile_experience_api(
                {
                    "target_company": "OpenAI",
                    "profile_payload": profile_payload,
                    "dry_run": True,
                    "workflow_run_id": "wf-profile-logo-ingest",
                    "now_epoch": 1_800_000_000,
                }
            )

            self.assertEqual(dry_run["status"], "dry_run")
            self.assertEqual(dry_run["company_key"], "openai")
            self.assertEqual(dry_run["eligible_command_count"], 2)
            self.assertFalse(dry_run["module_state_mutated"])
            self.assertFalse(dry_run["read_contract"]["request_path_fetch"])
            self.assertEqual(api_store.list_company_evidence(company_key="openai", evidence_type="logo_url"), [])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-profile-logo-ingest"), [])

            planned = orchestrator.ingest_company_logo_from_profile_experience_api(
                {
                    "target_company": "OpenAI",
                    "profile_payload": profile_payload,
                    "dry_run": False,
                    "workflow_run_id": "wf-profile-logo-ingest",
                    "collection_id": "company:openai",
                    "source_run_id": "job-openai-fresh-profile",
                    "source_profile_path": "hot_cache/openai/profile.json",
                    "now_epoch": 1_800_000_000,
                }
            )

            self.assertEqual(planned["status"], "planned")
            self.assertEqual(planned["planned_command_count"], 2)
            self.assertEqual(planned["evidence_count"], 2)
            self.assertTrue(planned["module_state_mutated"])
            self.assertFalse(planned["read_contract"]["request_path_fetch"])
            evidence_rows = api_store.list_company_evidence(company_key="openai", evidence_type="logo_url")
            self.assertEqual(len(evidence_rows), 2)
            self.assertEqual(evidence_rows[0]["metadata"]["source_kind"], "profile_experience_company_logo")
            commands = api_store.list_workflow_commands(
                workflow_run_id="wf-profile-logo-ingest",
                owner=MEDIA_ASSET_OWNER,
            )
            self.assertEqual(len(commands), 2)
            self.assertEqual(commands[0]["command_type"], MEDIA_ASSET_CACHE_COMMAND_TYPE)
            self.assertEqual(commands[0]["payload"]["entity_type"], "company")
            self.assertEqual(commands[0]["payload"]["asset_type"], "logo_media")
            self.assertEqual(commands[0]["payload"]["company_key"], "openai")
            self.assertEqual(commands[0]["payload"]["source_kind"], "profile_experience_company_logo")
            self.assertTrue(commands[0]["payload"]["source_evidence_id"].startswith("ce_profile_logo_"))
            self.assertEqual(api_store.list_company_assets(company_key="openai", asset_type="logo_media"), [])
        finally:
            api_store.close()

    def test_profile_experience_company_logo_skips_existing_or_expired_sources(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-company-logo-skip.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            api_store.upsert_company_asset(
                {
                    "asset_id": "ca_openai_existing_logo",
                    "company_key": "openai",
                    "target_company": "OpenAI",
                    "asset_type": "logo_media",
                    "content_ref": "/api/media/assets/ca_openai_existing_logo",
                    "status": "available",
                    "metadata": {"object_key": "media/company/openai/logo.png"},
                }
            )
            profile_payload = {
                "experience": [
                    {
                        "companyName": "OpenAI",
                        "companyLogo": {"url": "https://media.licdn.example/openai-logo.png?e=4102444800"},
                    }
                ]
            }

            skipped = orchestrator.ingest_company_logo_from_profile_experience_api(
                {
                    "target_company": "OpenAI",
                    "profile_payload": profile_payload,
                    "workflow_run_id": "wf-profile-logo-skip-existing",
                    "now_epoch": 1_800_000_000,
                }
            )

            self.assertEqual(skipped["status"], "skipped")
            self.assertEqual(skipped["reason"], "stable_company_logo_already_available")
            self.assertFalse(skipped["module_state_mutated"])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-profile-logo-skip-existing"), [])

            expired = orchestrator.ingest_company_logo_from_profile_experience_api(
                {
                    "target_company": "Expired Co",
                    "profile_payload": {
                        "experience": [
                            {
                                "companyName": "Expired Co",
                                "companyLogo": {
                                    "url": "https://media.licdn.example/expired-logo.png?e=1700000000"
                                },
                            }
                        ]
                    },
                    "workflow_run_id": "wf-profile-logo-expired",
                    "now_epoch": 1_800_000_000,
                }
            )

            self.assertEqual(expired["status"], "source_discovery_required")
            self.assertEqual(expired["fallback_strategy"], "logo_source_discovery")
            self.assertEqual(expired["expired_or_insufficient_ttl_count"], 1)
            self.assertFalse(expired["module_state_mutated"])
            self.assertEqual(api_store.list_company_evidence(company_key="expiredco", evidence_type="logo_url"), [])
            self.assertEqual(api_store.list_workflow_commands(workflow_run_id="wf-profile-logo-expired"), [])
        finally:
            api_store.close()

    def test_harvest_local_apply_queues_nonblocking_profile_logo_discovery_owner(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "profile-company-logo-local-apply.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        api_store = ControlPlaneStore(settings.db_path)
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        try:
            snapshot_dir = self.runtime_dir / "hot_cache_company_assets" / "openai" / "20260528T000000"
            profile_dir = snapshot_dir / "harvest_profiles"
            profile_dir.mkdir(parents=True)
            (profile_dir / "fresh-openai-profile.json").write_text(
                json.dumps(
                    {
                        "_harvest_request": {
                            "profile_url": "https://www.linkedin.com/in/openai-logo-source/",
                        },
                        "item": {
                            "linkedinUrl": "https://www.linkedin.com/in/openai-logo-source/",
                            "experience": [
                                {
                                    "companyName": "OpenAI",
                                    "companyLogo": {
                                        "url": "https://media.licdn.example/openai-logo.png?e=4102444800"
                                    },
                                }
                            ],
                        },
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = orchestrator._plan_company_logo_profile_experience_discover_command(  # noqa: SLF001
                job={"job_id": "job-profile-logo-local-apply"},
                request=JobRequest(target_company="OpenAI"),
                snapshot_dir=snapshot_dir,
                apply_result={
                    "status": "applied",
                    "applied_profile_url_keys": ["https://www.linkedin.com/in/openai-logo-source/"],
                },
                worker_ids=[123],
            )

            self.assertEqual(result["status"], "planned")
            self.assertEqual(result["reason"], "company_logo_profile_experience_discover_command_planned")
            self.assertTrue(result["non_blocking"])
            self.assertFalse(result["request_path_fetch"])
            self.assertEqual(api_store.list_company_evidence(company_key="openai", evidence_type="logo_url"), [])
            self.assertEqual(api_store.list_workflow_commands(owner=MEDIA_ASSET_OWNER, limit=10), [])

            discover_commands = [
                command
                for command in api_store.list_workflow_commands(owner=COMPANY_ASSET_OWNER, limit=10)
                if command["command_type"] == COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE
            ]
            self.assertEqual(len(discover_commands), 1)
            self.assertEqual(discover_commands[0]["payload"]["max_profile_payload_reads"], 1)
            self.assertEqual(discover_commands[0]["payload"]["source_worker_ids"], [123])

            drain = orchestrator._drain_company_logo_profile_experience_discover_commands(  # noqa: SLF001
                {"workflow_run_id": result["workflow_run_id"], "command_limit": 1}
            )

            self.assertEqual(drain["status"], "active")
            self.assertEqual(drain["completed_count"], 1)
            self.assertEqual(drain["items"][0]["profile_payload_read_count"], 1)
            evidence_rows = api_store.list_company_evidence(company_key="openai", evidence_type="logo_url")
            self.assertEqual(len(evidence_rows), 1)
            media_commands = api_store.list_workflow_commands(
                workflow_run_id=result["workflow_run_id"],
                owner=MEDIA_ASSET_OWNER,
            )
            self.assertEqual(len(media_commands), 1)
            self.assertEqual(media_commands[0]["command_type"], MEDIA_ASSET_CACHE_COMMAND_TYPE)
            self.assertEqual(media_commands[0]["payload"]["source_kind"], "profile_experience_company_logo")
        finally:
            api_store.close()

    def test_operation_http_api_submit_query_approve_cancel(self) -> None:
        settings = AppSettings(
            project_root=self.runtime_dir,
            runtime_dir=self.runtime_dir,
            secrets_file=self.runtime_dir / "secrets.toml",
            db_path=self.runtime_dir / "api.db",
            jobs_dir=self.runtime_dir / "jobs",
            company_assets_dir=self.runtime_dir / "company_assets",
            qwen=QwenSettings(enabled=False),
            semantic=SemanticProviderSettings(enabled=False),
            harvest=HarvestSettings(profile_scraper=HarvestActorSettings(enabled=False)),
        )
        catalog = AssetCatalog.discover()
        model_client = DeterministicModelClient()
        api_store = ControlPlaneStore(settings.db_path)
        orchestrator = SourcingOrchestrator(
            catalog=catalog,
            store=api_store,
            jobs_dir=settings.jobs_dir,
            model_client=model_client,
            semantic_provider=LocalSemanticProvider(),
            acquisition_engine=AcquisitionEngine(catalog, settings, api_store, model_client),
        )
        server = create_server(orchestrator, port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        opener = urllib_request.build_opener(urllib_request.ProxyHandler({}))
        try:
            registry_req = urllib_request.Request(f"http://{host}:{port}/api/operations/action-registry", method="GET")
            with opener.open(registry_req) as response:
                registry = json.loads(response.read().decode("utf-8"))
            self.assertEqual(registry["status"], "ok")
            self.assertIn(ACTION_EXPORT_CANDIDATES, registry["action_registry"])
            continue_contracts = registry["action_registry"][ACTION_CONTINUE_ACQUISITION_RUN][
                "allowed_workflow_command_contracts"
            ]
            self.assertIn(
                LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                {contract["command_type"] for contract in continue_contracts},
            )
            self.assertEqual(
                registry["action_registry"][ACTION_FETCH_PROFILE_SAMPLE]["default_workflow_command_contract"][
                    "owner"
                ],
                LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
            )
            self.assertEqual(
                continue_contracts[0]["control_policy"]["fallback_status"],
                "fail_closed",
            )
            self.assertEqual(
                continue_contracts[0]["activity_spine_policy"]["fallback_status"],
                "fail_closed",
            )
            self.assertIn(
                continue_contracts[0]["activity_spine_policy"]["requirement"],
                {
                    "activity_attempt_entity_delta_required",
                    "activity_run_boundary_required",
                    "orchestration_downstream_activity_required",
                },
            )

            command_registry_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/command-registry",
                method="GET",
            )
            with opener.open(command_registry_req) as response:
                command_registry = json.loads(response.read().decode("utf-8"))
            self.assertEqual(command_registry["status"], "ok")
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["owner"],
                "crm_public_web_owner",
            )
            self.assertEqual(
                command_registry["command_registry"]["crm.public_web.search.submit"]["agent_exposure_status"],
                "not_action_registry_allowlisted",
            )
            self.assertTrue(
                command_registry["command_registry"]["crm.public_web.search.submit"]["activity_spine_policy"][
                    "must_write_activity_attempt"
                ]
            )
            command_list_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/commands?operation_id=missing",
                method="GET",
            )
            with opener.open(command_list_req) as response:
                command_list = json.loads(response.read().decode("utf-8"))
            self.assertEqual(command_list["workflow_commands"], [])

            api_control_command = api_store.upsert_workflow_command(
                workflow_run_id="wf-api-activity",
                command_type=LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                owner=LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
                idempotency_key="workflow_command:api-activity",
                command_id="cmd-api-activity",
                operation_id="op-api-activity",
            )
            claimed_control_command = api_store.claim_workflow_command(
                api_control_command["command_id"],
                lease_owner="unit-test",
                lease_seconds=60,
            )
            api_store.mark_workflow_command_running(
                claimed_control_command["command_id"],
                lease_owner="unit-test",
            )
            api_store.mark_workflow_command_failed(
                api_control_command["command_id"],
                error_text="api-test-retry-wait",
                retryable=True,
            )

            api_activity = api_store.upsert_workflow_activity_run(
                {
                    "activity_run_id": "actrun_api",
                    "workflow_run_id": "wf-api-activity",
                    "operation_run_id": "op-api-activity",
                    "acquisition_run_id": "acqrun_api",
                    "command_id": "cmd-api-activity",
                    "activity_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                    "owner": LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
                    "status": "waiting_owner_implementation",
                    "idempotency_key": "activity:api",
                }
            )
            api_attempt = api_store.upsert_workflow_activity_attempt(
                {
                    "attempt_id": "actattempt_api",
                    "activity_run_id": api_activity["activity_run_id"],
                    "workflow_run_id": api_activity["workflow_run_id"],
                    "command_id": "cmd-api-activity",
                    "attempt_number": 1,
                    "status": "waiting_owner_implementation",
                    "idempotency_key": "activity_attempt:api",
                }
            )
            api_delta = api_store.upsert_workflow_entity_delta(
                {
                    "delta_id": "entitydelta_api",
                    "workflow_run_id": api_activity["workflow_run_id"],
                    "operation_run_id": api_activity["operation_run_id"],
                    "command_id": "cmd-api-activity",
                    "activity_run_id": api_activity["activity_run_id"],
                    "attempt_id": api_attempt["attempt_id"],
                    "entity_type": "discovery_lane",
                    "entity_key": "lane-api",
                    "delta_kind": "no_op_waiting_prerequisite",
                    "status": "not_applied",
                    "reason": "api_test_waiting",
                    "idempotency_key": "entity_delta:api",
                }
            )
            api_lane = api_store.upsert_acquisition_discovery_lane(
                {
                    "lane_id": "lane_api",
                    "workspace_id": "default",
                    "acquisition_run_id": api_activity["acquisition_run_id"],
                    "workflow_run_id": api_activity["workflow_run_id"],
                    "operation_run_id": api_activity["operation_run_id"],
                    "source_command_id": "cmd-api-activity",
                    "activity_run_id": api_activity["activity_run_id"],
                    "target_company": "OpenAI",
                    "query": "site:linkedin.com/in OpenAI research",
                    "provider": "dataforseo",
                    "status": "planned_pending_owner",
                    "phase": "planned",
                    "idempotency_key": "acquisition_discovery_lane:api",
                }
            )
            activity_list_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/activities?operation_run_id=op-api-activity",
                method="GET",
            )
            with opener.open(activity_list_req) as response:
                activity_list = json.loads(response.read().decode("utf-8"))
            self.assertEqual(activity_list["status"], "ok")
            self.assertEqual(activity_list["workflow_activities"][0]["activity_run_id"], api_activity["activity_run_id"])
            self.assertFalse(activity_list["workflow_activities"][0]["module_state_mutated"])
            self.assertEqual(
                activity_list["workflow_activities"][0]["mutation_contract"],
                "read_only_activity_evidence",
            )
            self.assertEqual(
                activity_list["workflow_activities"][0]["control_target"]["command_id"],
                "cmd-api-activity",
            )
            self.assertEqual(
                activity_list["workflow_activities"][0]["control_target"]["control_policy"]["generic_resume_statuses"],
                ["retry_wait"],
            )
            self.assertEqual(
                activity_list["workflow_activities"][0]["control_target"]["display_contract"]["source_of_truth"],
                "durable_runtime.workflow_command_display_contract",
            )
            self.assertEqual(
                activity_list["workflow_activities"][0]["control_target"]["activity_spine_policy"]["source_of_truth"],
                "durable_runtime.workflow_command_activity_spine_policy",
            )
            self.assertEqual(
                activity_list["workflow_activities"][0]["control_target"]["control_state"]["command_status"],
                "retry_wait",
            )
            self.assertTrue(activity_list["workflow_activities"][0]["control_target"]["control_state"]["can_resume"])
            self.assertIn(
                "resume",
                activity_list["workflow_activities"][0]["control_target"]["control_state"]["allowed_actions"],
            )
            activity_detail_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/activities/{api_activity['activity_run_id']}",
                method="GET",
            )
            with opener.open(activity_detail_req) as response:
                activity_detail = json.loads(response.read().decode("utf-8"))
            self.assertEqual(activity_detail["status"], "ok")
            self.assertEqual(activity_detail["workflow_activity"]["activity_run_id"], api_activity["activity_run_id"])
            self.assertFalse(activity_detail["workflow_activity"]["module_state_mutated"])
            self.assertEqual(activity_detail["activity_attempts"][0]["attempt_id"], api_attempt["attempt_id"])
            self.assertFalse(activity_detail["activity_attempts"][0]["module_state_mutated"])
            self.assertEqual(
                activity_detail["activity_attempts"][0]["mutation_contract"],
                "read_only_activity_attempt_evidence",
            )
            self.assertEqual(
                activity_detail["activity_attempts"][0]["control_target"]["command_id"],
                "cmd-api-activity",
            )
            attempt_list_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/activity-attempts?activity_run_id={api_activity['activity_run_id']}",
                method="GET",
            )
            with opener.open(attempt_list_req) as response:
                attempt_list = json.loads(response.read().decode("utf-8"))
            self.assertEqual(attempt_list["workflow_activity_attempts"][0]["attempt_id"], api_attempt["attempt_id"])
            self.assertFalse(attempt_list["workflow_activity_attempts"][0]["module_state_mutated"])
            self.assertEqual(
                attempt_list["workflow_activity_attempts"][0]["control_target"]["command_type"],
                LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            )
            attempt_detail_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/activity-attempts/{api_attempt['attempt_id']}",
                method="GET",
            )
            with opener.open(attempt_detail_req) as response:
                attempt_detail = json.loads(response.read().decode("utf-8"))
            self.assertEqual(attempt_detail["status"], "ok")
            self.assertEqual(attempt_detail["workflow_activity_attempt"]["attempt_id"], api_attempt["attempt_id"])
            delta_list_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/entity-deltas?activity_run_id={api_activity['activity_run_id']}",
                method="GET",
            )
            with opener.open(delta_list_req) as response:
                delta_list = json.loads(response.read().decode("utf-8"))
            self.assertEqual(delta_list["workflow_entity_deltas"][0]["delta_id"], api_delta["delta_id"])
            self.assertFalse(delta_list["workflow_entity_deltas"][0]["module_state_mutated"])
            self.assertEqual(
                delta_list["workflow_entity_deltas"][0]["mutation_contract"],
                "read_only_entity_delta_evidence",
            )
            self.assertEqual(
                delta_list["workflow_entity_deltas"][0]["control_target"]["target_type"],
                "workflow_command",
            )
            delta_detail_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/entity-deltas/{api_delta['delta_id']}",
                method="GET",
            )
            with opener.open(delta_detail_req) as response:
                delta_detail = json.loads(response.read().decode("utf-8"))
            self.assertEqual(delta_detail["status"], "ok")
            self.assertEqual(delta_detail["workflow_entity_delta"]["delta_id"], api_delta["delta_id"])
            self.assertFalse(delta_detail["workflow_entity_delta"]["module_state_mutated"])
            lane_list_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/discovery-lanes?acquisition_run_id=acqrun_api",
                method="GET",
            )
            with opener.open(lane_list_req) as response:
                lane_list = json.loads(response.read().decode("utf-8"))
            self.assertEqual(lane_list["status"], "ok")
            self.assertEqual(lane_list["contract"], "w11_acquisition_discovery_lane_list_v1")
            self.assertEqual(lane_list["acquisition_discovery_lanes"][0]["lane_id"], api_lane["lane_id"])
            self.assertFalse(lane_list["acquisition_discovery_lanes"][0]["module_state_mutated"])
            self.assertEqual(
                lane_list["acquisition_discovery_lanes"][0]["control_target"]["command_id"],
                "cmd-api-activity",
            )
            self.assertEqual(
                lane_list["acquisition_discovery_lanes"][0]["control_target"]["command_type"],
                LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
            )
            self.assertEqual(
                lane_list["acquisition_discovery_lanes"][0]["control_target"]["control_state"]["command_status"],
                "retry_wait",
            )
            self.assertEqual(
                lane_list["acquisition_discovery_lanes"][0]["control_target"]["display_contract"]["source_of_truth"],
                "durable_runtime.workflow_command_display_contract",
            )
            self.assertEqual(
                lane_list["acquisition_discovery_lanes"][0]["control_target"]["activity_spine_policy"][
                    "source_of_truth"
                ],
                "durable_runtime.workflow_command_activity_spine_policy",
            )
            self.assertEqual(
                lane_list["acquisition_discovery_lanes"][0]["control_target"]["fallback_status"],
                "fail_closed",
            )
            lane_detail_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/discovery-lanes/{api_lane['lane_id']}",
                method="GET",
            )
            with opener.open(lane_detail_req) as response:
                lane_detail = json.loads(response.read().decode("utf-8"))
            self.assertEqual(lane_detail["status"], "ok")
            self.assertEqual(lane_detail["acquisition_discovery_lane"]["lane_id"], api_lane["lane_id"])
            self.assertEqual(
                lane_detail["acquisition_discovery_lane"]["mutation_contract"],
                "read_only_domain_read_model",
            )

            api_command = api_store.upsert_workflow_command(
                workflow_run_id="wf-api-command-control",
                command_type="crm.public_web.search.submit",
                owner="crm_public_web_owner",
                idempotency_key="wf-api-command-control:search-submit",
            )
            command_cancel_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/commands/{api_command['command_id']}/cancel",
                data=json.dumps({"reason": "api-test", "actor": "unit-test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(command_cancel_req) as response:
                command_cancelled = json.loads(response.read().decode("utf-8"))
            self.assertEqual(command_cancelled["status"], "cancelled")
            self.assertEqual(command_cancelled["workflow_command"]["status"], "cancelled")

            command_retry_req = urllib_request.Request(
                f"http://{host}:{port}/api/workflow/commands/{api_command['command_id']}/retry",
                data=json.dumps({"reason": "api-test", "actor": "unit-test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(command_retry_req) as response:
                command_retried = json.loads(response.read().decode("utf-8"))
            self.assertEqual(command_retried["status"], "queued")
            self.assertEqual(command_retried["workflow_command"]["status"], "queued")

            submit_payload = json.dumps(
                {
                    "action_type": ACTION_EXPORT_CANDIDATES,
                    "target_ref": {"projection_id": "proj-api"},
                    "input": {"include_crm_notes": True},
                    "idempotency_key": "api-export:proj-api",
                },
                ensure_ascii=False,
            ).encode("utf-8")
            submit_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/actions",
                data=submit_payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(submit_req) as response:
                submitted = json.loads(response.read().decode("utf-8"))
            self.assertEqual(submitted["status"], "approval_required")
            self.assertEqual(submitted["operation_run"], {})
            action_id = submitted["action"]["action_id"]

            approve_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/actions/{action_id}/approve",
                data=json.dumps({"actor": "unit-test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(approve_req) as response:
                approved = json.loads(response.read().decode("utf-8"))
            self.assertEqual(approved["status"], "queued")
            operation_run_id = approved["operation_run"]["operation_run_id"]

            get_run_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/runs/{operation_run_id}",
                method="GET",
            )
            with opener.open(get_run_req) as response:
                run_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(run_payload["operation_run"]["status"], "queued")
            self.assertEqual(run_payload["operation_run"]["status_summary"]["fallback_status"], "fail_closed")
            self.assertFalse(run_payload["operation_run"]["status_summary"]["module_state_mutated"])

            list_actions_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/actions?conversation_id=&status=queued",
                method="GET",
            )
            with opener.open(list_actions_req) as response:
                listed_actions = json.loads(response.read().decode("utf-8"))
            self.assertEqual(listed_actions["status"], "ok")
            self.assertEqual([item["action_id"] for item in listed_actions["actions"]], [action_id])

            list_runs_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/runs?action_id={action_id}&include_status_summary=true",
                method="GET",
            )
            with opener.open(list_runs_req) as response:
                listed_runs = json.loads(response.read().decode("utf-8"))
            self.assertEqual([item["operation_run_id"] for item in listed_runs["operation_runs"]], [operation_run_id])
            self.assertEqual(
                listed_runs["operation_runs"][0]["status_summary"]["fallback_status"],
                "fail_closed",
            )

            provenance_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/runs/{operation_run_id}/provenance",
                method="GET",
            )
            with opener.open(provenance_req) as response:
                provenance = json.loads(response.read().decode("utf-8"))
            self.assertEqual(provenance["status"], "ok")
            self.assertEqual(provenance["operation_run"]["operation_run_id"], operation_run_id)
            self.assertEqual(provenance["workflow_commands"], [])

            resume_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/runs/{operation_run_id}/resume",
                data=json.dumps({"reason": "api-resume"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(resume_req) as response:
                resumed = json.loads(response.read().decode("utf-8"))
            self.assertEqual(resumed["status"], "queued")
            self.assertEqual(resumed["operation_run"]["progress"]["phase"], "resume_requested")

            api_store.update_operation_run_state(
                operation_run_id,
                status="failed",
                progress_patch={"phase": "failed", "reason": "api-test"},
            )
            retry_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/runs/{operation_run_id}/retry",
                data=json.dumps({"reason": "api-test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(retry_req) as response:
                retried = json.loads(response.read().decode("utf-8"))
            self.assertEqual(retried["status"], "queued")
            operation_run_id = retried["operation_run"]["operation_run_id"]

            cancel_req = urllib_request.Request(
                f"http://{host}:{port}/api/operations/runs/{operation_run_id}/cancel",
                data=json.dumps({"reason": "api-test"}).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with opener.open(cancel_req) as response:
                cancelled = json.loads(response.read().decode("utf-8"))
            self.assertEqual(cancelled["operation_run"]["status"], "cancelled")
            self.assertFalse(cancelled["module_state_mutated"])
            self.assertEqual(api_store.list_workflow_commands(operation_id=operation_run_id, limit=0), [])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            api_store.close()


if __name__ == "__main__":
    unittest.main()
