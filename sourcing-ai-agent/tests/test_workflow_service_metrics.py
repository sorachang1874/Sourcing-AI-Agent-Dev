from sourcing_agent.durable_runtime import (
    ACTIVITY_SPINE_ORCHESTRATION,
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
    ACQUISITION_RUN_CREATE_OWNER,
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
    ACQUISITION_SCALE_PLAN_OWNER,
    COMPANY_ASSET_OWNER,
    COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE,
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    COLLECTION_AUTHORITATIVE_MERGE_OWNER,
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
    CRM_PUBLIC_WEB_PHASE_OWNER,
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
    CRM_WRITER_OWNER,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXCEL_INTAKE_RUN_OWNER,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_OWNER,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    MEDIA_ASSET_CACHE_COMMAND_TYPE,
    MEDIA_ASSET_OWNER,
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
    PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
)
import sourcing_agent.workflow_service_metrics as workflow_service_metrics
from sourcing_agent.workflow_service_metrics import build_workflow_service_metrics


def test_workflow_service_metrics_reports_command_causality_contract() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_ok",
                "workflow_run_id": "wf_ok",
                "operation_id": "op_ok",
                "command_type": "linkedin.local_profile_delta.apply",
                "owner": "profile_local_apply_owner",
                "stage_id": "local_apply",
                "causal_group_id": "cg_1",
                "parent_command_id": "cmd_profile",
                "source_event_id": "evt_apply_requested",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 12},
                "readiness_effect": "local_profile_delta_applied",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "apply:1",
                "payload": {},
            }
        ]
    )

    contract = metrics["workflow_causality_contract"]
    assert contract["report_available"] is True
    assert contract["checked_command_count"] == 1
    assert contract["violation_detected"] is False
    assert contract["causal_group_count"] == 1


def test_workflow_service_metrics_flags_missing_command_causality_contract() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_missing",
                "workflow_run_id": "wf_missing",
                "operation_id": "op_missing",
                "command_type": "projection.board_visible_patch.publish",
                "owner": "board_visible_projection_owner",
                "status": "queued",
                "idempotency_key": "board:1",
                "payload": {"candidate_ids": ["candidate-a"]},
            }
        ]
    )

    contract = metrics["workflow_causality_contract"]
    assert contract["violation_detected"] is True
    assert contract["missing_envelope_count"] == 1
    assert contract["missing_envelope_command_type_counts"] == {
        "projection.board_visible_patch.publish": 1
    }


def test_workflow_service_metrics_requires_no_op_reason_for_zero_count_commands() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_zero",
                "workflow_run_id": "wf_zero",
                "operation_id": "op_zero",
                "command_type": "linkedin.local_profile_delta.apply",
                "owner": "profile_local_apply_owner",
                "stage_id": "local_apply",
                "causal_group_id": "cg_zero",
                "source_event_id": "evt_zero",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 0},
                "readiness_effect": "local_profile_delta_applied",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "apply:zero",
                "payload": {},
            }
        ]
    )

    contract = metrics["workflow_causality_contract"]
    assert contract["violation_detected"] is True
    assert contract["no_op_contract_violation_count"] == 1


def test_workflow_service_metrics_requires_stage_readiness_and_count_fact_for_causality() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_incomplete",
                "workflow_run_id": "wf_incomplete",
                "operation_id": "op_incomplete",
                "command_type": "projection.board_visible_patch.publish",
                "owner": "board_visible_projection_owner",
                "causal_group_id": "cg_incomplete",
                "source_event_id": "evt_incomplete",
                "source_event_type": "CommandPlanRequested",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "board:incomplete",
                "payload": {},
            }
        ]
    )

    contract = metrics["workflow_causality_contract"]
    assert contract["violation_detected"] is True
    assert contract["incomplete_envelope_count"] == 1
    missing_fields = contract["incomplete_envelope_samples"][0]["missing_or_invalid_fields"]
    assert "stage_id" in missing_fields
    assert "readiness_effect" in missing_fields
    assert "produced_entity_counts_or_no_op_reason" in missing_fields


def test_workflow_service_metrics_reports_durable_command_owner_contracts() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_acquisition_run_create",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_RUN_CREATE_COMMAND_TYPE,
                "owner": ACQUISITION_RUN_CREATE_OWNER,
                "stage_id": "acquisition_run_create",
                "causal_group_id": "cg_acquisition_run_create",
                "source_event_id": "evt_acquisition_run_create",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"acquisition_run": 1},
                "readiness_effect": "acquisition_run_requested",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.run.create:1",
                "payload": {},
            },
            {
                "command_id": "cmd_excel",
                "workflow_run_id": "wf_excel",
                "operation_id": "op_excel",
                "command_type": EXCEL_INTAKE_RUN_COMMAND_TYPE,
                "owner": EXCEL_INTAKE_RUN_OWNER,
                "stage_id": "excel_intake",
                "causal_group_id": "cg_excel",
                "source_event_id": "evt_excel",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"excel_intake_job": 1, "excel_contact": 12},
                "readiness_effect": "excel_intake_started",
                "causality_schema_version": "command_causality_v1",
                "status": "running",
                "idempotency_key": "excel.intake.run:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_intent_resolve",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
                "owner": ACQUISITION_INTENT_RESOLVE_OWNER,
                "stage_id": "acquisition_intent_resolve",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_run_create",
                "source_event_id": "evt_acquisition_intent_resolve",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"acquisition_intent": 1},
                "readiness_effect": "acquisition_intent_ready_for_plan",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.intent.resolve:1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_add",
                "workflow_run_id": "wf_crm_add",
                "operation_id": "op_crm_add",
                "command_type": CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
                "owner": CRM_WRITER_OWNER,
                "stage_id": "crm_record_add_from_projection",
                "causal_group_id": "cg_crm_add",
                "source_event_id": "evt_crm_add",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"crm_record": 1},
                "readiness_effect": "crm_record_added_from_projection",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.record.add_from_projection:1",
                "payload": {},
            },
            {
                "command_id": "cmd_media_asset_cache",
                "workflow_run_id": "wf_media_asset_cache",
                "operation_id": "op_media_asset_cache",
                "command_type": MEDIA_ASSET_CACHE_COMMAND_TYPE,
                "owner": MEDIA_ASSET_OWNER,
                "stage_id": "media_asset_cache",
                "causal_group_id": "cg_media_asset_cache",
                "source_event_id": "evt_media_asset_cache",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"media_asset": 1},
                "readiness_effect": "media_asset_cached",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "media.asset.cache:person:1",
                "payload": {},
            },
            {
                "command_id": "cmd_company_logo_profile_discover",
                "workflow_run_id": "wf_company_logo_profile_discover",
                "operation_id": "op_company_logo_profile_discover",
                "command_type": COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE,
                "owner": COMPANY_ASSET_OWNER,
                "stage_id": "company_logo_profile_experience_discover",
                "causal_group_id": "cg_company_logo_profile_discover",
                "source_event_id": "evt_company_logo_profile_discover",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"company_evidence": 1},
                "readiness_effect": "company_logo_profile_evidence_planned",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "company.logo.profile_experience.discover:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_plan_build",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
                "owner": ACQUISITION_PLAN_BUILD_OWNER,
                "stage_id": "acquisition_plan_build",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_intent_resolve",
                "source_event_id": "evt_acquisition_plan_build",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"acquisition_plan": 1},
                "readiness_effect": "acquisition_plan_ready_for_review",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.plan.build:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_plan_review_request",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
                "owner": ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
                "stage_id": "acquisition_plan_review_request",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_plan_build",
                "source_event_id": "evt_acquisition_plan_review_request",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"plan_review": 1, "acquisition_plan": 1},
                "readiness_effect": "acquisition_plan_review_requested",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.plan_review.request:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_plan_commit",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
                "owner": ACQUISITION_PLAN_COMMIT_OWNER,
                "stage_id": "acquisition_plan_commit",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_plan_review_request",
                "source_event_id": "evt_acquisition_plan_commit",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"plan_review": 1, "acquisition_plan": 1},
                "readiness_effect": "acquisition_plan_committed",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.plan.commit:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_probe_submit",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
                "owner": ACQUISITION_PROBE_OWNER,
                "stage_id": "acquisition_probe_submit",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_plan_commit",
                "source_event_id": "evt_acquisition_probe_submit",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"acquisition_run": 1, "query": 1},
                "readiness_effect": "acquisition_probe_submitted",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.probe.submit:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_probe_collect",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
                "owner": ACQUISITION_PROBE_OWNER,
                "stage_id": "acquisition_probe_collect",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_probe_submit",
                "source_event_id": "evt_acquisition_probe_collect",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"acquisition_run": 1, "probe_result": 1},
                "readiness_effect": "acquisition_probe_collected",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.probe.collect:1",
                "payload": {},
            },
            {
                "command_id": "cmd_acquisition_scale_plan",
                "workflow_run_id": "wf_acquisition_run_create",
                "operation_id": "op_acquisition_run_create",
                "command_type": ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
                "owner": ACQUISITION_SCALE_PLAN_OWNER,
                "stage_id": "acquisition_scale_plan",
                "causal_group_id": "cg_acquisition_run_create",
                "parent_command_id": "cmd_acquisition_probe_collect",
                "source_event_id": "evt_acquisition_scale_plan",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"acquisition_run": 1, "discovery_lane": 1},
                "readiness_effect": "acquisition_scale_planned",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "acquisition.scale.plan:1",
                "payload": {},
            },
            {
                "command_id": "cmd_linkedin_discovery",
                "workflow_run_id": "wf_linkedin_discovery",
                "operation_id": "op_linkedin_discovery",
                "command_type": LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
                "stage_id": "stage1_candidate_discovery",
                "causal_group_id": "cg_linkedin_discovery",
                "source_event_id": "evt_linkedin_discovery",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 25},
                "readiness_effect": "stage1_discovery_lane_submitted",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "linkedin.discovery_query.run:1",
                "payload": {},
            },
            {
                "command_id": "cmd_profile_refill",
                "workflow_run_id": "wf_profile_refill",
                "operation_id": "op_profile_refill",
                "command_type": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
                "stage_id": "profile_fetch",
                "causal_group_id": "cg_profile_refill",
                "source_event_id": "evt_profile_refill",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"profile_url": 2},
                "readiness_effect": "profile_refill_submitted",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "linkedin.profile_refill.submit_batch:1",
                "payload": {},
            },
            {
                "command_id": "cmd_profile_fetch_activity",
                "workflow_run_id": "wf_profile_fetch_activity",
                "operation_id": "op_profile_fetch_activity",
                "command_type": LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "stage_id": "operation_native_profile_fetch",
                "causal_group_id": "cg_profile_fetch_activity",
                "source_event_id": "evt_profile_fetch_activity",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"profile_url": 2},
                "readiness_effect": "profile_fetch_activity_planned",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "linkedin.profile_fetch.activity.run:1",
                "payload": {},
            },
            {
                "command_id": "cmd_profile_fetch_provider",
                "workflow_run_id": "wf_profile_fetch_activity",
                "operation_id": "op_profile_fetch_activity",
                "command_type": LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "stage_id": "operation_native_profile_provider_fetch",
                "causal_group_id": "cg_profile_fetch_activity",
                "parent_command_id": "cmd_profile_fetch_activity",
                "source_event_id": "evt_profile_fetch_provider",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"profile_url": 2},
                "readiness_effect": "profile_fetch_provider_completed",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "linkedin.profile_fetch.provider.fetch:1",
                "payload": {},
            },
            {
                "command_id": "cmd_profile_terminal_admit",
                "workflow_run_id": "wf_profile_fetch_activity",
                "operation_id": "op_profile_fetch_activity",
                "command_type": LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
                "owner": LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
                "stage_id": "operation_native_profile_terminal_admission",
                "causal_group_id": "cg_profile_fetch_activity",
                "parent_command_id": "cmd_profile_fetch_provider",
                "source_event_id": "evt_profile_terminal_admit",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"profile_url": 2},
                "readiness_effect": "profile_terminal_admitted",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "linkedin.profile_terminal.admit:1",
                "payload": {},
            },
            {
                "command_id": "cmd_projection_profile_admission",
                "workflow_run_id": "wf_profile_fetch_activity",
                "operation_id": "op_profile_fetch_activity",
                "command_type": PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
                "owner": PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
                "stage_id": "operation_native_projection_admission",
                "causal_group_id": "cg_profile_fetch_activity",
                "parent_command_id": "cmd_profile_terminal_admit",
                "source_event_id": "evt_projection_profile_admission",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"profile_url": 2},
                "readiness_effect": "profile_terminal_projection_admitted",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "projection.profile_admission.apply:1",
                "payload": {},
            },
            {
                "command_id": "cmd_projection_finalize",
                "workflow_run_id": "wf_projection_finalize",
                "operation_id": "op_projection_finalize",
                "command_type": PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
                "owner": PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
                "stage_id": "serving_projection_finalization",
                "causal_group_id": "cg_projection_finalize",
                "source_event_id": "evt_projection_finalize",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"projection": 1, "candidate": 25},
                "readiness_effect": "run_scope_projection_finalized",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "projection.run_scope.finalize:1",
                "payload": {},
            },
            {
                "command_id": "cmd_collection_merge",
                "workflow_run_id": "wf_collection_merge",
                "operation_id": "op_collection_merge",
                "command_type": COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
                "owner": COLLECTION_AUTHORITATIVE_MERGE_OWNER,
                "stage_id": "collection_authoritative_merge",
                "causal_group_id": "cg_collection_merge",
                "source_event_id": "evt_collection_merge",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"projection": 1, "candidate": 25},
                "readiness_effect": "collection_authoritative_projection_merged",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "collection.authoritative.merge:1",
                "payload": {},
            },
            {
                "command_id": "cmd_projection_export",
                "workflow_run_id": "wf_projection_export",
                "operation_id": "op_projection_export",
                "command_type": EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                "owner": EXPORT_PROJECTION_GENERATE_OWNER,
                "stage_id": "projection_export",
                "causal_group_id": "cg_projection_export",
                "source_event_id": "evt_projection_export",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"projection": 1, "candidate": 25},
                "readiness_effect": "projection_export_generated",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "export.projection.generate:1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_export",
                "workflow_run_id": "wf_crm_public_web_export",
                "operation_id": "op_crm_public_web_export",
                "command_type": EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
                "owner": EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
                "stage_id": "crm_public_web_export",
                "causal_group_id": "cg_crm_public_web_export",
                "source_event_id": "evt_crm_public_web_export",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"crm_record": 3, "public_web_signal": 9},
                "readiness_effect": "crm_public_web_export_generated",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "export.crm_public_web.generate:1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_search_submit",
                "workflow_run_id": "wf_crm_public_web_batch",
                "operation_id": "op_crm_public_web_batch",
                "command_type": CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "stage_id": "crm_public_web_search_submit",
                "causal_group_id": "cg_crm_public_web_batch",
                "source_event_id": "evt_crm_public_web_search_submit",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"crm_public_web_run": 1},
                "readiness_effect": "crm_public_web_search_submitted",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.search.submit:run-1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_search_poll_fetch",
                "workflow_run_id": "wf_crm_public_web_batch",
                "operation_id": "op_crm_public_web_batch",
                "command_type": CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "stage_id": "crm_public_web_search_poll_fetch",
                "causal_group_id": "cg_crm_public_web_batch",
                "parent_command_id": "cmd_crm_public_web_search_submit",
                "source_event_id": "evt_crm_public_web_search_poll_fetch",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"crm_public_web_run": 1},
                "readiness_effect": "crm_public_web_search_results_fetched",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.search.poll_fetch:run-1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_documents_fetch",
                "workflow_run_id": "wf_crm_public_web_batch",
                "operation_id": "op_crm_public_web_batch",
                "command_type": CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "stage_id": "crm_public_web_documents_fetch",
                "causal_group_id": "cg_crm_public_web_batch",
                "parent_command_id": "cmd_crm_public_web_search_poll_fetch",
                "source_event_id": "evt_crm_public_web_documents_fetch",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"public_web_document": 3},
                "readiness_effect": "crm_public_web_documents_fetched",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.documents.fetch:run-1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_evidence_adjudicate",
                "workflow_run_id": "wf_crm_public_web_batch",
                "operation_id": "op_crm_public_web_batch",
                "command_type": CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "stage_id": "crm_public_web_evidence_adjudicate",
                "causal_group_id": "cg_crm_public_web_batch",
                "parent_command_id": "cmd_crm_public_web_documents_fetch",
                "source_event_id": "evt_crm_public_web_evidence_adjudicate",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"person_evidence": 2},
                "readiness_effect": "crm_public_web_evidence_adjudicated",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.evidence.adjudicate:run-1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_model_safe_finalize",
                "workflow_run_id": "wf_crm_public_web_batch",
                "operation_id": "op_crm_public_web_batch",
                "command_type": CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "stage_id": "crm_public_web_model_safe_finalize",
                "causal_group_id": "cg_crm_public_web_batch",
                "parent_command_id": "cmd_crm_public_web_evidence_adjudicate",
                "source_event_id": "evt_crm_public_web_model_safe_finalize",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"model_safe_result": 1},
                "readiness_effect": "crm_public_web_model_safe_finalized",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.model_safe.finalize:run-1",
                "payload": {},
            },
            {
                "command_id": "cmd_crm_public_web_signals_materialize",
                "workflow_run_id": "wf_crm_public_web_batch",
                "operation_id": "op_crm_public_web_batch",
                "command_type": CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
                "owner": CRM_PUBLIC_WEB_PHASE_OWNER,
                "stage_id": "crm_public_web_signals_materialize",
                "causal_group_id": "cg_crm_public_web_batch",
                "parent_command_id": "cmd_crm_public_web_model_safe_finalize",
                "source_event_id": "evt_crm_public_web_signals_materialize",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"public_web_signal": 2},
                "readiness_effect": "crm_public_web_signals_materialized",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.signals.materialize:run-1",
                "payload": {},
            },
        ]
    )

    contracts = metrics["durable_command_owner_contracts"]
    assert contracts["report_available"] is True
    assert contracts["checked_command_count"] == 28
    assert contracts["violation_detected"] is False
    assert contracts["control_policy_violation_count"] == 0
    assert contracts["display_contract_violation_count"] == 0
    assert contracts["activity_spine_policy_violation_count"] == 0
    assert sum(contracts["running_control_maturity_counts"].values()) == contracts["checked_command_type_count"]
    assert sum(contracts["running_control_gap_status_counts"].values()) == contracts["checked_command_type_count"]
    assert contracts["running_control_maturity_counts"]["owner_specific_cancel_resume"] == 41
    assert contracts["running_control_maturity_counts"].get("owner_specific_cancel_only", 0) == 0
    assert contracts["running_control_maturity_counts"].get("owner_specific_resume_only", 0) == 0
    assert contracts["running_control_maturity_counts"].get("fail_closed_with_upgrade_requirements", 0) == 0
    assert contracts["running_control_gap_status_counts"]["closed"] == 41
    assert contracts["running_control_gap_status_counts"].get("partial_resume_gap_reported", 0) == 0
    assert contracts["running_control_gap_status_counts"].get("partial_cancel_gap_reported", 0) == 0
    assert contracts["running_control_gap_status_counts"].get(
        "accepted_fail_closed_pending_owner_specific_control",
        0,
    ) == 0
    acquisition_create = contracts["contracts"]["acquisition_run_create"]
    assert acquisition_create["expected_owner"] == ACQUISITION_RUN_CREATE_OWNER
    assert acquisition_create["pending_count"] == 1
    assert acquisition_create["control_policy_violation_count"] == 0
    assert acquisition_create["display_contract_violation_count"] == 0
    assert acquisition_create["activity_spine_policy_violation_count"] == 0
    assert acquisition_create["activity_spine_policy"]["requirement"] == ACTIVITY_SPINE_ORCHESTRATION
    acquisition_intent = contracts["contracts"]["acquisition_intent_resolve"]
    assert acquisition_intent["expected_owner"] == ACQUISITION_INTENT_RESOLVE_OWNER
    assert acquisition_intent["pending_count"] == 1
    acquisition_plan = contracts["contracts"]["acquisition_plan_build"]
    assert acquisition_plan["expected_owner"] == ACQUISITION_PLAN_BUILD_OWNER
    assert acquisition_plan["pending_count"] == 1
    acquisition_plan_review = contracts["contracts"]["acquisition_plan_review_request"]
    assert acquisition_plan_review["expected_owner"] == ACQUISITION_PLAN_REVIEW_REQUEST_OWNER
    assert acquisition_plan_review["pending_count"] == 1
    acquisition_plan_commit = contracts["contracts"]["acquisition_plan_commit"]
    assert acquisition_plan_commit["expected_owner"] == ACQUISITION_PLAN_COMMIT_OWNER
    assert acquisition_plan_commit["pending_count"] == 1
    acquisition_probe_submit = contracts["contracts"]["acquisition_probe_submit"]
    assert acquisition_probe_submit["expected_owner"] == ACQUISITION_PROBE_OWNER
    assert acquisition_probe_submit["pending_count"] == 1
    acquisition_probe_collect = contracts["contracts"]["acquisition_probe_collect"]
    assert acquisition_probe_collect["expected_owner"] == ACQUISITION_PROBE_OWNER
    assert acquisition_probe_collect["pending_count"] == 1
    acquisition_scale_plan = contracts["contracts"]["acquisition_scale_plan"]
    assert acquisition_scale_plan["expected_owner"] == ACQUISITION_SCALE_PLAN_OWNER
    assert acquisition_scale_plan["pending_count"] == 1
    crm_add = contracts["contracts"]["crm_record_add_from_projection"]
    assert crm_add["expected_owner"] == CRM_WRITER_OWNER
    assert crm_add["succeeded_count"] == 1
    assert crm_add["activity_spine_policy"]["requirement"] == ACTIVITY_SPINE_REQUIRED
    assert crm_add["activity_spine_policy"]["must_write_activity_attempt"] is True
    media_asset_cache = contracts["contracts"]["media_asset_cache"]
    assert media_asset_cache["expected_owner"] == MEDIA_ASSET_OWNER
    assert media_asset_cache["succeeded_count"] == 1
    assert media_asset_cache["activity_spine_policy"]["requirement"] == ACTIVITY_SPINE_REQUIRED
    company_logo_profile_discover = contracts["contracts"]["company_logo_profile_experience_discover"]
    assert company_logo_profile_discover["expected_owner"] == COMPANY_ASSET_OWNER
    assert company_logo_profile_discover["succeeded_count"] == 1
    assert company_logo_profile_discover["activity_spine_policy"]["requirement"] == ACTIVITY_SPINE_REQUIRED
    assert company_logo_profile_discover["control_policy"]["running_cancel_supported"] is True
    linkedin_discovery = contracts["contracts"]["linkedin_discovery_query_run"]
    assert linkedin_discovery["expected_owner"] == LINKEDIN_DISCOVERY_QUERY_RUN_OWNER
    assert linkedin_discovery["pending_count"] == 1
    assert linkedin_discovery["activity_spine_policy"]["must_write_entity_delta"] is True
    profile_refill = contracts["contracts"]["linkedin_profile_refill_submit_batch"]
    assert profile_refill["expected_owner"] == LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER
    assert profile_refill["activity_spine_policy"]["requirement"] == ACTIVITY_SPINE_REQUIRED
    profile_fetch_activity = contracts["contracts"]["linkedin_profile_fetch_activity_run"]
    assert profile_fetch_activity["expected_owner"] == LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER
    assert profile_fetch_activity["pending_count"] == 1
    profile_fetch_provider = contracts["contracts"]["linkedin_profile_fetch_provider_fetch"]
    assert profile_fetch_provider["expected_owner"] == LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER
    assert profile_fetch_provider["pending_count"] == 1
    profile_terminal_admit = contracts["contracts"]["linkedin_profile_terminal_admit"]
    assert profile_terminal_admit["expected_owner"] == LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER
    assert profile_terminal_admit["pending_count"] == 1
    projection_profile_admission = contracts["contracts"]["projection_profile_admission_apply"]
    assert projection_profile_admission["expected_owner"] == PROJECTION_PROFILE_ADMISSION_APPLY_OWNER
    assert projection_profile_admission["pending_count"] == 1
    projection_finalize = contracts["contracts"]["projection_run_scope_finalize"]
    assert projection_finalize["expected_owner"] == PROJECTION_RUN_SCOPE_FINALIZE_OWNER
    collection_merge = contracts["contracts"]["collection_authoritative_merge"]
    assert collection_merge["expected_owner"] == COLLECTION_AUTHORITATIVE_MERGE_OWNER
    excel = contracts["contracts"]["excel_intake_run"]
    assert excel["command_type"] == EXCEL_INTAKE_RUN_COMMAND_TYPE
    assert excel["expected_owner_count"] == 1
    assert excel["pending_count"] == 1
    assert excel["control_policy"]["running_cancel_supported"] is True
    assert excel["running_control_maturity"] == "owner_specific_cancel_resume"
    assert excel["running_control_gap_status"] == "closed"
    assert excel["control_policy"]["running_cancel_delegate"] == "excel_intake_owner.cancel_excel_intake_run_command"
    assert excel["control_policy"]["running_cancel_prerequisites"] == [
        "workflow_command_status_claimed_or_running",
        "payload.job_id",
        "thread_terminal_checkpoint",
    ]
    assert excel["control_policy"]["running_resume_supported"] is True
    assert excel["control_policy"]["running_resume_delegate"] == "excel_intake_owner.resume_excel_intake_run_command"
    assert excel["control_policy"]["running_resume_prerequisites"] == [
        "workflow_command_status_claimed_or_running",
        "payload.job_id",
        "command_lease_expired_or_force",
    ]
    projection_export = contracts["contracts"]["projection_export_generate"]
    assert projection_export["succeeded_count"] == 1
    assert projection_export["running_control_maturity"] == "owner_specific_cancel_resume"
    assert projection_export["running_control_gap_status"] == "closed"
    assert projection_export["control_policy"]["running_cancel_supported"] is True
    assert projection_export["control_policy"]["running_cancel_delegate"] == "projection_exporter.cancel_export_command"
    assert projection_export["control_policy"]["running_resume_supported"] is True
    assert projection_export["control_policy"]["running_resume_delegate"] == "projection_exporter.resume_export_command"
    assert projection_export["control_policy"]["running_resume_prerequisites"] == [
        "workflow_command_status_claimed_or_running",
        "artifact_not_published",
        "command_lease_expired_or_force",
    ]
    crm_public_web_export = contracts["contracts"]["crm_public_web_export_generate"]
    assert crm_public_web_export["expected_owner"] == EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER
    assert crm_public_web_export["control_policy"]["running_cancel_supported"] is True
    assert (
        crm_public_web_export["control_policy"]["running_cancel_delegate"]
        == "crm_public_web_exporter.cancel_export_command"
    )
    assert crm_public_web_export["control_policy"]["running_resume_supported"] is True
    assert (
        crm_public_web_export["control_policy"]["running_resume_delegate"]
        == "crm_public_web_exporter.resume_export_command"
    )
    crm_public_web_phase_keys = (
        "crm_public_web_search_submit",
        "crm_public_web_search_poll_fetch",
        "crm_public_web_documents_fetch",
        "crm_public_web_evidence_adjudicate",
        "crm_public_web_model_safe_finalize",
        "crm_public_web_signals_materialize",
    )
    for key in crm_public_web_phase_keys:
        assert contracts["contracts"][key]["expected_owner"] == CRM_PUBLIC_WEB_PHASE_OWNER
        assert contracts["contracts"][key]["succeeded_count"] == 1
        assert contracts["contracts"][key]["activity_spine_policy"]["requirement"] == ACTIVITY_SPINE_REQUIRED
        assert contracts["contracts"][key]["control_policy"]["running_resume_supported"] is True
        assert (
            contracts["contracts"][key]["control_policy"]["running_resume_delegate"]
            == "crm_public_web_owner.resume_crm_public_web_phase_command"
        )
        assert contracts["contracts"][key]["control_policy"]["running_resume_prerequisites"] == [
            "payload.run_id",
            "crm_public_web_run_exists",
            "command_lease_expired_or_force",
        ]


def test_workflow_service_metrics_flags_durable_command_owner_contract_drift() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_projection_export_drift",
                "workflow_run_id": "wf_projection_export_drift",
                "operation_id": "op_projection_export_drift",
                "command_type": EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                "owner": "legacy_export_helper",
                "causal_group_id": "cg_projection_export_drift",
                "source_event_id": "evt_projection_export_drift",
                "source_event_type": "CommandPlanRequested",
                "status": "queued",
                "idempotency_key": "export.projection.generate:drift",
                "payload": {},
            }
        ]
    )

    contracts = metrics["durable_command_owner_contracts"]
    projection_export = contracts["contracts"]["projection_export_generate"]
    assert contracts["violation_detected"] is True
    assert contracts["invalid_owner_count"] == 1
    assert contracts["incomplete_causality_count"] == 1
    assert projection_export["invalid_owner_count"] == 1
    assert projection_export["incomplete_causality_count"] == 1
    missing_fields = projection_export["incomplete_causality_samples"][0]["missing_or_invalid_fields"]
    assert "stage_id" in missing_fields
    assert "owner_matches_contract" in missing_fields
    assert "produced_entity_counts_or_no_op_reason" in missing_fields


def test_workflow_service_metrics_flags_durable_command_control_policy_drift(monkeypatch) -> None:
    class _Policy:
        def __init__(self, command_type: str, owner: str) -> None:
            self.command_type = command_type
            self.owner = owner

        def to_record(self) -> dict:
            return {
                "schema_version": "workflow_command_control_policy_v1",
                "command_type": self.command_type,
                "owner": self.owner,
                "generic_control_contract": "w11_workflow_command_control_v1",
                "generic_cancel_statuses": ["queued", "retry_wait"],
                "generic_retry_statuses": ["failed_terminal", "cancelled"],
                "generic_resume_statuses": ["retry_wait"],
                "running_cancel_supported": False,
                "running_cancel_contract": "w11_workflow_command_owner_specific_control_v1",
                "running_cancel_blocked_reason": "domain_mutation_must_finish_or_retry_from_terminal_state",
                "running_cancel_upgrade_requirements": ["owner_checkpoint_before_mutation_or_terminal_retry"],
                "unsupported_running_cancel_reason": "running_command_requires_owner_specific_cancel",
                "running_resume_supported": False,
                "running_resume_contract": "w11_workflow_command_owner_specific_control_v1",
                "running_resume_blocked_reason": "owner_specific_resume_not_implemented",
                "running_resume_upgrade_requirements": [],
                "unsupported_running_resume_reason": "running_command_requires_owner_specific_resume",
                "control_source_of_truth": "durable_runtime.workflow_command_control_policy",
                "agent_callable_surface": "workflow_command_control_api",
                "fallback_status": "fail_closed",
            }

    def _fake_control_policy(command_type: str, owner: str = "") -> _Policy:
        return _Policy(command_type, owner)

    monkeypatch.setattr(
        workflow_service_metrics,
        "workflow_command_control_policy",
        _fake_control_policy,
    )
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_projection_export_policy_drift",
                "workflow_run_id": "wf_projection_export_policy_drift",
                "operation_id": "op_projection_export_policy_drift",
                "command_type": EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                "owner": EXPORT_PROJECTION_GENERATE_OWNER,
                "stage_id": "projection_export",
                "causal_group_id": "cg_projection_export_policy_drift",
                "source_event_id": "evt_projection_export_policy_drift",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"export": 1},
                "readiness_effect": "projection_export_requested",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "export.projection.generate:policy-drift",
                "payload": {},
            }
        ]
    )

    contracts = metrics["durable_command_owner_contracts"]
    projection_export = contracts["contracts"]["projection_export_generate"]
    assert contracts["violation_detected"] is True
    assert contracts["invalid_owner_count"] == 0
    assert contracts["incomplete_causality_count"] == 0
    assert contracts["control_policy_violation_count"] == 1
    assert projection_export["control_policy_violation_count"] == 1
    assert "running_resume_blocked_reason_not_placeholder" in projection_export[
        "control_policy_missing_or_invalid_fields"
    ]
    assert "running_resume_upgrade_requirements" in projection_export[
        "control_policy_missing_or_invalid_fields"
    ]


def test_workflow_service_metrics_flags_display_and_activity_policy_drift(monkeypatch) -> None:
    class _DisplayContract:
        def __init__(self, command_type: str, owner: str) -> None:
            self.command_type = command_type
            self.owner = owner

        def to_record(self) -> dict:
            return {
                "schema_version": "workflow_command_display_contract_v1",
                "command_type": self.command_type,
                "owner": self.owner,
                "display_label": "Projection export",
                "display_category": "Export",
                "description": "",
                "source_of_truth": "durable_runtime.workflow_command_display_contract",
                "fallback_status": "fail_closed",
            }

    class _ActivityPolicy:
        def __init__(self, command_type: str, owner: str) -> None:
            self.command_type = command_type
            self.owner = owner

        def to_record(self) -> dict:
            return {
                "schema_version": "w11_workflow_command_activity_spine_policy_v1",
                "command_type": self.command_type,
                "owner": self.owner,
                "requirement": "legacy_internal_pending_activity_spine",
                "must_write_activity_run": False,
                "must_write_activity_attempt": False,
                "must_write_entity_delta": False,
                "downstream_activity_required": False,
                "agent_callable": False,
                "activity_table": "workflow_activity_runs",
                "attempt_table": "workflow_activity_attempts",
                "entity_delta_table": "workflow_entity_deltas",
                "source_of_truth": "durable_runtime.workflow_command_activity_spine_policy",
                "agent_callable_surface": "operation_command_activity_api",
                "fallback_status": "fail_closed",
            }

    monkeypatch.setattr(
        workflow_service_metrics,
        "workflow_command_display_contract",
        lambda command_type, owner="": _DisplayContract(command_type, owner),
    )
    monkeypatch.setattr(
        workflow_service_metrics,
        "workflow_command_activity_spine_policy",
        lambda command_type, owner="": _ActivityPolicy(command_type, owner),
    )
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_projection_export_display_activity_drift",
                "workflow_run_id": "wf_projection_export_display_activity_drift",
                "operation_id": "op_projection_export_display_activity_drift",
                "command_type": EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                "owner": EXPORT_PROJECTION_GENERATE_OWNER,
                "stage_id": "projection_export",
                "causal_group_id": "cg_projection_export_display_activity_drift",
                "source_event_id": "evt_projection_export_display_activity_drift",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"export": 1},
                "readiness_effect": "projection_export_requested",
                "causality_schema_version": "command_causality_v1",
                "status": "queued",
                "idempotency_key": "export.projection.generate:display-activity-drift",
                "payload": {},
            }
        ]
    )

    contracts = metrics["durable_command_owner_contracts"]
    projection_export = contracts["contracts"]["projection_export_generate"]
    assert contracts["violation_detected"] is True
    assert contracts["display_contract_violation_count"] == 1
    assert contracts["activity_spine_policy_violation_count"] == 1
    assert "description" in projection_export["display_contract_missing_or_invalid_fields"]
    assert "requirement_not_legacy_internal" in projection_export[
        "activity_spine_policy_missing_or_invalid_fields"
    ]


def test_workflow_service_metrics_exposes_worker_gaps_and_user_experience_bottlenecks() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "span_id": 101,
                "lane_id": "search_seed",
                "worker_key": "agent-current-probe",
                "status": "completed",
                "metadata": {"recovery_kind": "search_seed"},
            },
            {
                "worker_id": 2,
                "span_id": 102,
                "lane_id": "search_seed",
                "worker_key": "agent-current-scale",
                "status": "completed",
                "metadata": {"recovery_kind": "search_seed"},
            },
            {
                "worker_id": 3,
                "span_id": 103,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-1",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
        ],
        trace_spans=[
            {
                "span_id": 101,
                "lane_id": "search_seed",
                "started_at": "2026-04-28T10:00:00+00:00",
                "completed_at": "2026-04-28T10:00:02+00:00",
            },
            {
                "span_id": 102,
                "lane_id": "search_seed",
                "started_at": "2026-04-28T10:00:02.100000+00:00",
                "completed_at": "2026-04-28T10:00:04+00:00",
            },
            {
                "span_id": 103,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-04-28T10:00:10+00:00",
                "completed_at": "2026-04-28T10:00:45+00:00",
            },
        ],
        workflow_wall_clock_ms={
            "final_results_to_board_ready": 2500.0,
            "final_results_to_board_nonempty": 7000.0,
            "job_to_board_visible_partial": 42000.0,
            "job_to_board_nonempty": 127000.0,
            "stage_1_preview_to_final_results": 45000.0,
        },
        stage_wall_clock_ms={"linkedin_stage_1": 120000.0},
        timings_ms={"dashboard_fetch": 40.0, "candidate_page_fetch": 60.0, "board_probe_wait": 7000.0},
        timeline=[{"observed_at_ms": 125.0, "message": "queued"}],
    )

    worker_timeline = metrics["worker_timeline"]
    assert metrics["report_available"] is True
    assert worker_timeline["worker_count"] == 3
    assert worker_timeline["duration_ms"]["max"] == 35000.0
    assert worker_timeline["handoff_gap_ms"]["global_next_worker_start_gap_ms"]["max"] == 6000.0
    assert worker_timeline["handoff_gap_ms"]["same_lane_next_worker_start_gap_ms"]["search_seed"]["max"] == 100.0
    assert worker_timeline["timeline_sample"][0]["started_at"] == "2026-04-28T10:00:00+00:00"
    assert worker_timeline["timeline_sample"][0]["next_worker_start_gap_ms"] == 100.0
    assert worker_timeline["timeline_sample"][1]["same_lane_next_worker_start_gap_ms"] is None
    assert worker_timeline["slow_workers"][0]["worker_id"] == 3

    user_experience = metrics["user_experience"]
    assert user_experience["loading_feedback_required"] is True
    assert user_experience["board_readiness_violation"] is True
    assert user_experience["long_finalization_after_preview"] is True
    assert user_experience["partial_board_visible_observed"] is True
    assert user_experience["job_to_board_visible_partial_ms"] == 42000.0
    assert user_experience["first_progress_observed_ms"] == 125.0

    bottlenecks = metrics["bottlenecks"]
    assert bottlenecks["optimization_ready"] is True
    assert {item["kind"] for item in bottlenecks["top_bottlenecks"]} >= {
        "slow_worker",
        "worker_handoff_gap",
        "board_readiness_lag",
        "post_preview_finalization_lag",
    }


def test_workflow_service_metrics_excludes_profile_provider_wait_from_finalization_lag() -> None:
    metrics = build_workflow_service_metrics(
        workflow_wall_clock_ms={
            "stage_1_preview_to_final_results": 313000.0,
            "job_to_final_results": 540000.0,
        },
        post_preview_finalization={
            "report_available": True,
            "profile_wait_excluded_from_finalization_gate_ms": 213000.0,
            "finalization_start_gate_ms": 10408.0,
            "finalization_start_gate_source": "profile_terminal_at",
        },
    )

    user_experience = metrics["user_experience"]
    assert user_experience["stage_1_preview_to_final_results_ms"] == 313000.0
    assert user_experience["raw_stage_1_preview_to_final_results_ms"] == 313000.0
    assert user_experience["finalization_lag_evaluation_ms"] == 10408.0
    assert user_experience["finalization_lag_evaluation_source"] == "profile_terminal_at"
    assert user_experience["finalization_lag_excludes_profile_provider_wait"] is True
    assert user_experience["long_finalization_after_preview"] is False
    assert "post_preview_finalization_lag" not in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_worker_handoff_gap_uses_provider_timestamps_for_remote_profile_workers() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 14,
                "span_id": 114,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-14",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-11T15:21:49.302+00:00"},
            },
            {
                "worker_id": 15,
                "span_id": 115,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-15",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-11T15:21:50.791+00:00"},
            },
        ],
        trace_spans=[
            {
                "span_id": 114,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-11T15:22:45+00:00",
                "completed_at": "2026-05-11T15:22:50+00:00",
            },
            {
                "span_id": 115,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-11T15:24:08+00:00",
                "completed_at": "2026-05-11T15:24:14+00:00",
            },
        ],
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [14],
                    "event_metrics": {"remote_completed_at": "2026-05-11T15:21:50.302+00:00"},
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [15],
                    "event_metrics": {"remote_completed_at": "2026-05-11T15:21:51.791+00:00"},
                },
            },
        ],
    )

    handoff_gap = metrics["worker_timeline"]["handoff_gap_ms"]["global_next_worker_start_gap_ms"]
    assert handoff_gap["max"] == 489.0
    assert metrics["worker_timeline"]["timeline_sample"][1]["started_at"] == "2026-05-11T15:24:08+00:00"
    assert metrics["worker_timeline"]["timeline_sample"][1]["handoff_started_at"] == "2026-05-11T15:21:50.791+00:00"


def test_worker_handoff_gap_orders_by_provider_handoff_start_not_worker_created_at() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "span_id": 101,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-1",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-11T15:00:00+00:00"},
            },
            {
                "worker_id": 2,
                "span_id": 102,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-2",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-11T15:00:01+00:00"},
            },
        ],
        trace_spans=[
            {
                "span_id": 101,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-11T15:00:00+00:00",
                "completed_at": "2026-05-11T15:01:00+00:00",
            },
            {
                "span_id": 102,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-11T15:01:00+00:00",
                "completed_at": "2026-05-11T15:01:02+00:00",
            },
        ],
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [1],
                    "event_metrics": {"remote_completed_at": "2026-05-11T15:00:05+00:00"},
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [2],
                    "event_metrics": {"remote_completed_at": "2026-05-11T15:00:06+00:00"},
                },
            },
        ],
    )

    handoff_gap = metrics["worker_timeline"]["handoff_gap_ms"]["global_next_worker_start_gap_ms"]
    assert handoff_gap["max"] == 0.0
    assert metrics["worker_timeline"]["timeline_sample"][1]["handoff_started_at"] == "2026-05-11T15:00:01+00:00"


def test_profile_scheduler_handoff_gap_ignores_upstream_acquisition_wait() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 8,
                "span_id": 108,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-current-tail",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-12T20:07:27+00:00"},
            },
            {
                "worker_id": 17,
                "span_id": 117,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-former-head",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-12T20:08:15+00:00"},
            },
        ],
        trace_spans=[
            {
                "span_id": 108,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-12T20:07:33+00:00",
                "completed_at": "2026-05-12T20:07:33+00:00",
            },
            {
                "span_id": 110,
                "lane_id": "acquisition_specialist",
                "handoff_to_lane": "acquisition_specialist",
                "started_at": "2026-05-12T20:07:25+00:00",
                "completed_at": "2026-05-12T20:08:18+00:00",
            },
            {
                "span_id": 117,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-12T20:08:20+00:00",
                "completed_at": "2026-05-12T20:08:21+00:00",
            },
        ],
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [8],
                    "event_metrics": {"remote_completed_at": "2026-05-12T20:07:30.721+00:00"},
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [17],
                    "event_metrics": {"remote_completed_at": "2026-05-12T20:08:19.373+00:00"},
                },
            },
        ],
    )

    handoff_gap = metrics["worker_timeline"]["handoff_gap_ms"]
    assert handoff_gap["global_next_worker_start_gap_ms"]["max"] == 44279.0
    assert handoff_gap["profile_scheduler_next_worker_start_gap_ms"]["count"] == 0
    assert handoff_gap["profile_scheduler_next_worker_start_gap_ms"]["max"] == 0.0
    assert handoff_gap["slow_gaps"][0]["covered_by_upstream"] is True
    assert handoff_gap["slow_gaps"][0]["covering_span_id"] == 110


def test_profile_scheduler_handoff_gap_counts_uncovered_profile_gap() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 8,
                "span_id": 108,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-current-tail",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-12T20:07:27+00:00"},
            },
            {
                "worker_id": 17,
                "span_id": 117,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-former-head",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_wait_started_at": "2026-05-12T20:08:15+00:00"},
            },
        ],
        trace_spans=[
            {
                "span_id": 108,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-12T20:07:33+00:00",
                "completed_at": "2026-05-12T20:07:33+00:00",
            },
            {
                "span_id": 117,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-12T20:08:20+00:00",
                "completed_at": "2026-05-12T20:08:21+00:00",
            },
        ],
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [8],
                    "event_metrics": {"remote_completed_at": "2026-05-12T20:07:30.721+00:00"},
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [17],
                    "event_metrics": {"remote_completed_at": "2026-05-12T20:08:19.373+00:00"},
                },
            },
        ],
    )

    handoff_gap = metrics["worker_timeline"]["handoff_gap_ms"]
    assert handoff_gap["profile_scheduler_next_worker_start_gap_ms"]["max"] == 44279.0
    assert handoff_gap["slow_gaps"][0]["covered_by_upstream"] is False


def test_profile_scheduler_handoff_gap_ignores_exhausted_dispatch_plan_boundary() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "span_id": 101,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-current-tail",
                "status": "completed",
                "metadata": {
                    "recovery_kind": "harvest_profile_batch",
                    "prefetch_batch_context": {
                        "chunk_index": 2,
                        "planned_dispatch_worker_count": 2,
                        "requested_url_count": 223,
                    },
                },
                "checkpoint": {"remote_wait_started_at": "2026-05-12T20:07:27+00:00"},
            },
            {
                "worker_id": 2,
                "span_id": 102,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-former-head",
                "status": "completed",
                "metadata": {
                    "recovery_kind": "harvest_profile_batch",
                    "prefetch_batch_context": {
                        "chunk_index": 1,
                        "planned_dispatch_worker_count": 1,
                        "requested_url_count": 297,
                    },
                },
                "checkpoint": {"remote_wait_started_at": "2026-05-12T20:08:15+00:00"},
            },
        ],
        trace_spans=[
            {
                "span_id": 101,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-12T20:07:30+00:00",
                "completed_at": "2026-05-12T20:07:31+00:00",
            },
            {
                "span_id": 102,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-12T20:08:15+00:00",
                "completed_at": "2026-05-12T20:08:16+00:00",
            },
        ],
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [1],
                    "event_metrics": {"remote_completed_at": "2026-05-12T20:07:31+00:00"},
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [2],
                    "event_metrics": {"remote_completed_at": "2026-05-12T20:08:15+00:00"},
                },
            },
        ],
    )

    handoff_gap = metrics["worker_timeline"]["handoff_gap_ms"]
    assert handoff_gap["global_next_worker_start_gap_ms"]["max"] == 44000.0
    assert handoff_gap["profile_scheduler_next_worker_start_gap_ms"]["count"] == 0
    assert handoff_gap["slow_gaps"][0]["profile_scheduler_gap"] is False
    assert handoff_gap["slow_gaps"][0]["reason"] == "profile_prefetch_dispatch_plan_exhausted"


def test_workflow_service_metrics_exposes_post_profile_completion_slos() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 10,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:04+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-07T10:00:04+00:00"},
            }
        ],
        job_events=[
            {
                "created_at": "2026-05-07T10:00:04.500000+00:00",
                "payload": {
                    "profile_prefetch": {
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "terminal_queue_state_leak_count": 0,
                            "refill_queue_state_counts": {"fetched": 2},
                        }
                    }
                },
            },
            {
                "created_at": "2026-05-07T10:00:06+00:00",
                "payload": {
                    "profile_refill_trigger": {
                        "trigger_source": "worker_completion_callback",
                        "status": "queued",
                        "elapsed_ms": 250,
                        "signal_only": True,
                        "refill_daemon_signal_only": True,
                    },
                },
            },
        ],
        result_view_lifecycle={
            "delta_profile_required_count": 2,
            "delta_profile_fetched_count": 2,
            "delta_profile_board_visible_count": 2,
        },
        materialization_items=[
            {
                "item_id": "local-apply-1",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "snapshot_id": "snap-1",
                "completed_at": "2026-05-07T10:00:05+00:00",
            },
            {
                "item_id": "board-visible-1",
                "item_kind": "board_visible_delta_apply",
                "status": "completed",
                "snapshot_id": "snap-1",
                "completed_at": "2026-05-07T10:00:07+00:00",
            },
        ],
        board_visible_patches=[
            {
                "snapshot_id": "snap-1",
                "sequence_index": 1,
                "published_at": "2026-05-07T10:00:08+00:00",
                "cumulative_candidate_count": 2,
            }
        ],
    )

    post_profile = metrics["post_profile_completion"]
    assert post_profile["report_available"] is True
    assert post_profile["url_terminal_state_recording"]["terminal_queue_state_leak_count"] == 0
    assert post_profile["event_level_callback"]["elapsed_ms"]["max"] == 250.0
    assert post_profile["profile_file_visible_to_board_patch_visible"]["elapsed_ms"]["max"] == 2000.0
    assert post_profile["all_profiles_fetched_to_all_cards_visible"]["elapsed_ms"]["max"] == 4000.0
    assert post_profile["slo_violation_detected"] is False


def test_post_profile_file_to_board_patch_ignores_non_profile_local_apply_items() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:01+00:00",
                "metadata": {"recovery_kind": "harvest_company_employees"},
            },
            {
                "worker_id": 2,
                "status": "completed",
                "updated_at": "2026-05-07T10:01:00+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-07T10:01:00+00:00"},
            },
        ],
        materialization_items=[
            {
                "item_id": "company-local-apply",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "snapshot_id": "snap-live",
                "source_worker_ids": [1],
                "created_at": "2026-05-07T10:00:01+00:00",
                "completed_at": "2026-05-07T10:00:03+00:00",
            },
            {
                "item_id": "profile-local-apply",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "snapshot_id": "snap-live",
                "source_worker_ids": [2],
                "created_at": "2026-05-07T10:01:01+00:00",
                "completed_at": "2026-05-07T10:01:05+00:00",
            },
            {
                "item_id": "profile-board-visible",
                "item_kind": "board_visible_delta_apply",
                "status": "completed",
                "snapshot_id": "snap-live",
                "completed_at": "2026-05-07T10:01:04+00:00",
            },
        ],
        board_visible_patches=[
            {
                "snapshot_id": "snap-live",
                "sequence_index": 1,
                "published_at": "2026-05-07T10:01:04+00:00",
                "cumulative_candidate_count": 50,
            }
        ],
    )

    profile_patch = metrics["post_profile_completion"]["profile_file_visible_to_board_patch_visible"]
    assert profile_patch["completed_local_apply_count"] == 1
    assert profile_patch["ignored_non_profile_local_apply_count"] == 1
    assert profile_patch["elapsed_ms"]["max"] == 0.0
    assert profile_patch["slo_violation"] is False


def test_post_profile_file_to_board_patch_ignores_zero_candidate_local_apply_items() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:01+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
            {
                "worker_id": 2,
                "status": "completed",
                "updated_at": "2026-05-07T10:01:00+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
        ],
        workflow_commands=[
            {
                "command_id": "cmd_zero_local_apply",
                "command_type": "linkedin.local_profile_delta.apply",
                "owner": "profile_local_apply_command_owner",
                "causal_group_id": "cg_zero",
                "source_event_id": "evt_zero_local",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 0},
                "no_op_reason": "zero_candidate_local_apply",
                "status": "succeeded",
                "created_at": "2026-05-07T10:00:01+00:00",
                "updated_at": "2026-05-07T10:00:05+00:00",
                "payload": {
                    "item_id": "local-zero",
                    "snapshot_id": "snap-live",
                    "source_worker_ids": [1],
                    "worker_kind": "harvest_prefetch",
                },
                "result": {"candidate_count": 0},
            },
            {
                "command_id": "cmd_profile_local_apply",
                "command_type": "linkedin.local_profile_delta.apply",
                "owner": "profile_local_apply_command_owner",
                "causal_group_id": "cg_profile",
                "source_event_id": "evt_profile_local",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 20},
                "status": "succeeded",
                "created_at": "2026-05-07T10:01:01+00:00",
                "updated_at": "2026-05-07T10:01:05+00:00",
                "payload": {
                    "item_id": "local-profile",
                    "snapshot_id": "snap-live",
                    "source_worker_ids": [2],
                    "worker_kind": "harvest_prefetch",
                },
                "result": {"candidate_count": 20},
            },
            {
                "command_id": "cmd_board_visible",
                "command_type": "projection.board_visible_patch.publish",
                "owner": "board_visible_projection_owner",
                "causal_group_id": "cg_profile",
                "parent_command_id": "cmd_profile_local_apply",
                "source_event_id": "evt_profile_board",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 20},
                "status": "succeeded",
                "created_at": "2026-05-07T10:01:05+00:00",
                "updated_at": "2026-05-07T10:01:07+00:00",
                "payload": {
                    "item_id": "board-profile",
                    "snapshot_id": "snap-live",
                    "source_worker_ids": [2],
                    "candidate_count": 20,
                },
                "result": {"candidate_count": 20},
            },
        ],
    )

    profile_patch = metrics["post_profile_completion"]["profile_file_visible_to_board_patch_visible"]
    assert profile_patch["completed_local_apply_count"] == 2
    assert profile_patch["elapsed_ms"]["max"] == 2000.0
    assert profile_patch["typed_causal_group_pair_count"] == 1
    assert profile_patch["legacy_snapshot_pair_count"] == 0
    assert profile_patch["heuristic_pairing_used"] is False
    assert profile_patch["slo_violation"] is False


def test_workflow_service_metrics_uses_full_snapshot_serving_denominator_for_live_roster() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 20,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:04+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-07T10:00:04+00:00"},
            }
        ],
        result_view_lifecycle={
            "state": "current_snapshot_serving",
            "current_snapshot_id": "snap-live",
            "served_snapshot_id": "snap-live",
            "served_candidate_count": 140,
            "delta_profile_progress_applicable": False,
            "delta_profile_progress_reason": "not_applicable_live_roster_stage1",
            "delta_profile_required_count": 0,
            "delta_profile_fetched_count": 0,
            "delta_profile_board_visible_count": 0,
            "serving_projection_phase": "current_snapshot_serving",
        },
        materialization_items=[
            {
                "item_id": "board-visible-live",
                "item_kind": "board_visible_delta_apply",
                "status": "completed",
                "snapshot_id": "snap-live",
                "completed_at": "2026-05-07T10:00:06+00:00",
            },
        ],
        board_visible_patches=[
            {
                "snapshot_id": "snap-live",
                "sequence_index": 1,
                "published_at": "2026-05-07T10:00:06+00:00",
                "served_candidate_count": 140,
                "cumulative_candidate_count": 140,
            }
        ],
    )

    all_cards = metrics["post_profile_completion"]["all_profiles_fetched_to_all_cards_visible"]
    assert all_cards["delta_profile_progress_applicable"] is False
    assert all_cards["target_visible_count"] == 140
    assert all_cards["all_cards_visible"] is True
    assert all_cards["elapsed_ms"]["max"] == 2000.0


def test_workflow_service_metrics_no_delta_complete_board_uses_public_publication_status() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 22,
                "status": "completed",
                "updated_at": "2026-05-14T10:00:00+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-14T10:00:00+00:00"},
            }
        ],
        result_view_lifecycle={
            "phase": "current_snapshot_materializing",
            "state": "current_snapshot_materializing",
            "current_snapshot_id": "snap-live",
            "served_snapshot_id": "snap-live",
            "served_candidate_count": 297,
            "expected_candidate_count": 297,
            "delta_profile_progress_applicable": False,
            "delta_profile_required_count": 0,
            "delta_profile_fetched_count": 0,
            "delta_profile_board_visible_count": 0,
        },
        board_runtime_state={
            "phase": "current_snapshot_materializing",
            "publication_status": "complete",
            "expected_candidate_count": 297,
            "served_candidate_count": 297,
            "published_candidate_count": 297,
            "display_ready_candidate_count": 297,
        },
        board_visible_patches=[
            {
                "snapshot_id": "snap-live",
                "sequence_index": 1,
                "published_at": "2026-05-14T10:00:20+00:00",
                "served_candidate_count": 297,
                "cumulative_candidate_count": 297,
            }
        ],
    )

    all_cards = metrics["post_profile_completion"]["all_profiles_fetched_to_all_cards_visible"]
    assert all_cards["delta_profile_progress_applicable"] is False
    assert all_cards["full_snapshot_visible_count"] == 297
    assert all_cards["target_visible_count"] == 297
    assert all_cards["visible_count"] == 297
    assert all_cards["all_cards_visible"] is True
    assert all_cards["all_cards_visible_at"] == "2026-05-14T10:00:20+00:00"
    assert all_cards["elapsed_ms"]["max"] == 20000.0


def test_workflow_service_metrics_flags_incomplete_board_visible_after_all_profiles_fetched() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 30,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:04+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-07T10:00:04+00:00"},
            }
        ],
        result_view_lifecycle={
            "delta_profile_progress_applicable": True,
            "delta_profile_required_count": 2,
            "delta_profile_fetched_count": 2,
            "delta_profile_board_visible_count": 1,
        },
        board_visible_patches=[
            {
                "snapshot_id": "snap-delta",
                "sequence_index": 1,
                "published_at": "2026-05-07T10:00:06+00:00",
                "delta_profile_board_visible_count": 1,
                "cumulative_candidate_count": 1,
            }
        ],
    )

    post_profile = metrics["post_profile_completion"]
    all_cards = post_profile["all_profiles_fetched_to_all_cards_visible"]
    assert all_cards["all_cards_visible"] is False
    assert all_cards["missing_visible_count"] == 1
    assert all_cards["slo_violation"] is True
    assert post_profile["slo_violation_detected"] is True


def test_post_profile_delta_visibility_timestamp_ignores_baseline_served_counts() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 40,
                "status": "completed",
                "updated_at": "2026-05-11T10:00:10+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-11T10:00:10+00:00"},
            }
        ],
        result_view_lifecycle={
            "delta_profile_progress_applicable": True,
            "served_candidate_count": 7352,
            "delta_profile_required_count": 2384,
            "delta_profile_fetched_count": 2384,
            "delta_profile_materialized_count": 2384,
            "delta_profile_board_visible_count": 2384,
        },
        board_visible_patches=[
            {
                "snapshot_id": "snap-delta",
                "sequence_index": 1,
                "published_at": "2026-05-11T10:00:01+00:00",
                "served_candidate_count": 5000,
                "cumulative_candidate_count": 100,
                "delta_profile_board_visible_count": 100,
            },
            {
                "snapshot_id": "snap-delta",
                "sequence_index": 2,
                "published_at": "2026-05-11T10:00:20+00:00",
                "served_candidate_count": 7352,
                "cumulative_candidate_count": 2384,
                "delta_profile_board_visible_count": 2384,
            },
        ],
    )

    all_cards = metrics["post_profile_completion"]["all_profiles_fetched_to_all_cards_visible"]
    assert all_cards["all_cards_visible"] is True
    assert all_cards["visible_count"] == 2384
    assert all_cards["full_snapshot_visible_count"] == 0
    assert all_cards["all_cards_visible_at"] == "2026-05-11T10:00:20+00:00"
    assert all_cards["elapsed_ms"]["max"] == 10000.0
    assert all_cards["slo_violation"] is False


def test_post_profile_delta_visibility_uses_board_runtime_publication_time_not_early_item() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 41,
                "status": "completed",
                "updated_at": "2026-05-11T10:00:10+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-11T10:00:10+00:00"},
            }
        ],
        result_view_lifecycle={
            "phase": "current_snapshot_serving",
            "current_snapshot_id": "snap-current",
            "served_snapshot_id": "snap-current",
            "serving_projection_phase": "current_snapshot_serving",
            "delta_profile_progress_applicable": True,
            "served_candidate_count": 7352,
            "delta_profile_required_count": 2384,
            "delta_profile_fetched_count": 2384,
            "delta_profile_materialized_count": 2384,
            "delta_profile_board_visible_count": 2384,
        },
        board_runtime_state={
            "phase": "current_snapshot_serving",
            "publication_status": "complete",
            "delta_profile_board_visible_count": 2384,
            "row_publication_updated_at": "2026-05-11T10:00:20+00:00",
        },
        materialization_items=[
            {
                "item_id": "early-board-visible",
                "item_kind": "board_visible_delta_apply",
                "status": "completed",
                "snapshot_id": "snap-current",
                "completed_at": "2026-05-11T10:00:01+00:00",
            }
        ],
    )

    all_cards = metrics["post_profile_completion"]["all_profiles_fetched_to_all_cards_visible"]
    assert all_cards["all_cards_visible"] is True
    assert all_cards["full_snapshot_visible_count"] == 7352
    assert all_cards["visible_count"] == 2384
    assert all_cards["all_cards_visible_at"] == "2026-05-11T10:00:20+00:00"
    assert all_cards["elapsed_ms"]["max"] == 10000.0


def test_workflow_service_metrics_exposes_recovery_phase_contract_violations() -> None:
    def phase(
        status: str = "skipped",
        *,
        elapsed_ms: int = 0,
        reason: str = "",
        counts: dict[str, int] | None = None,
    ) -> dict[str, object]:
        return {
            "phase": "placeholder",
            "owner": "test-owner",
            "max_sync_work": "bounded test work",
            "started_at": "2026-05-08T10:00:00+00:00",
            "finished_at": "2026-05-08T10:00:01+00:00",
            "elapsed_ms": elapsed_ms,
            "status": status,
            "reason": reason,
            "counts": dict(counts or {}),
        }

    phase_metrics = {
        "search_seed_discovery": phase("active", elapsed_ms=5, counts={"item_count": 1}),
        "worker_recovery": phase("completed", elapsed_ms=1200, counts={"executed_count": 2}),
        "blocked_workflow_cleanup": phase("skipped", reason="explicit_job_scope"),
        "local_apply_backlog": phase("completed", elapsed_ms=250, counts={"candidate_count": 100}),
        "event_level_materialization_followup": phase("completed", elapsed_ms=350),
        "profile_prefetch_refill": phase("completed", elapsed_ms=450),
        "profile_refill_event_level_materialization_followup": phase("completed", elapsed_ms=550),
        "workflow_resume": phase("completed", elapsed_ms=650),
        "post_projection_workflow_resume": phase("skipped", reason="no_run_scope_projection_finalize_work"),
        "post_completion_reconcile": phase("completed", elapsed_ms=750),
        "excel_intake_recovery": phase("skipped", reason="excel_intake_recovery_disabled_by_payload"),
        "board_visible_apply": phase("completed", elapsed_ms=850),
        "snapshot_full_materialization": phase("failed", elapsed_ms=100, reason="boom"),
        "explicit_job_followup_rounds": phase("skipped", reason="explicit_job_followup_rounds_zero"),
        "remote_event_followup": phase("completed", elapsed_ms=40_000),
        "post_recovery_housekeeping": phase("skipped", reason="post_recovery_housekeeping_disabled_by_payload"),
        "total": phase("completed", elapsed_ms=41_000),
    }
    for name, payload in phase_metrics.items():
        payload["phase"] = name

    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "poll_auto_recovery",
                "status": "completed",
                "daemon_status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert metrics["report_available"] is True
    assert recovery["report_available"] is True
    assert recovery["run_count"] == 1
    assert recovery["missing_phase_count"] == 0
    assert recovery["failed_phase_count"] == 1
    assert recovery["slow_phase_count"] == 1
    assert recovery["slow_total_phase_count"] == 1
    assert recovery["unexpected_enabled_phase_count"] == 2
    assert recovery["failed_phase_present"] is True
    assert recovery["slow_phase_present"] is True
    assert recovery["slow_total_phase_present"] is True
    assert recovery["unexpected_enabled_phase_present"] is True
    assert recovery["elapsed_ms"]["max"] == 40000.0
    assert recovery["total_elapsed_ms"]["max"] == 41000.0
    assert recovery["phase_candidate_count"]["max"] == 100.0
    assert recovery["phase_candidate_count_max"]["local_apply_backlog"] == 100
    assert recovery["local_apply_candidate_per_second"]["max"] == 400.0
    assert recovery["phase_elapsed_ms_max"]["remote_event_followup"] == 40000.0
    bottleneck_kinds = {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]}
    assert {
        "recovery_phase_failure",
        "recovery_phase_slow",
        "recovery_total_elapsed_slow",
        "recovery_unexpected_enabled_phase",
    } <= bottleneck_kinds


def test_workflow_service_metrics_allows_explicit_post_terminal_snapshot_drain() -> None:
    phase_names = [
        "search_seed_discovery",
        "worker_recovery",
        "blocked_workflow_cleanup",
        "local_apply_backlog",
        "event_level_materialization_followup",
        "profile_prefetch_refill",
        "profile_refill_event_level_materialization_followup",
        "workflow_resume",
        "post_projection_workflow_resume",
        "post_completion_reconcile",
        "excel_intake_recovery",
        "board_visible_apply",
        "snapshot_full_materialization",
        "explicit_job_followup_rounds",
        "remote_event_followup",
        "post_recovery_housekeeping",
        "total",
    ]
    phase_metrics = {
        name: {
            "phase": name,
            "owner": "test-owner",
            "status": "skipped",
            "elapsed_ms": 0,
            "counts": {},
        }
        for name in phase_names
    }
    phase_metrics["snapshot_full_materialization"] = {
        "phase": "snapshot_full_materialization",
        "owner": "snapshot_materialization_owner",
        "status": "active",
        "reason": "snapshot_full_materialization_command_owner",
        "elapsed_ms": 446,
        "counts": {"command_count": 1, "completed_count": 1},
    }
    phase_metrics["total"] = {
        "phase": "total",
        "owner": "worker_recovery_daemon",
        "status": "completed",
        "elapsed_ms": 500,
        "counts": {},
    }

    allowed = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "post_terminal",
                "status": "completed",
                "allow_background_snapshot_full_materialization": True,
                "materialization_wait_state": {
                    "snapshot_full_materialization_pending_count": 1,
                    "items": [{"source": "workflow_commands"}],
                },
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )["recovery_phase_metrics"]
    blocked = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "poll_auto_recovery",
                "status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )["recovery_phase_metrics"]

    assert allowed["unexpected_enabled_phase_count"] == 0
    assert allowed["unexpected_enabled_phase_present"] is False
    assert blocked["unexpected_enabled_phase_count"] == 1
    assert blocked["unexpected_enabled_phase_present"] is True
    assert blocked["unexpected_enabled_phases"][0]["phase"] == "snapshot_full_materialization"


def test_workflow_service_metrics_treats_total_recovery_elapsed_as_optimization_only() -> None:
    phase_metrics = {
        "search_seed_discovery": {"phase": "search_seed_discovery", "status": "skipped", "elapsed_ms": 0},
        "worker_recovery": {"phase": "worker_recovery", "status": "completed", "elapsed_ms": 12000},
        "blocked_workflow_cleanup": {"phase": "blocked_workflow_cleanup", "status": "skipped", "elapsed_ms": 0},
        "local_apply_backlog": {"phase": "local_apply_backlog", "status": "completed", "elapsed_ms": 1000},
        "event_level_materialization_followup": {
            "phase": "event_level_materialization_followup",
            "status": "completed",
            "elapsed_ms": 1000,
        },
        "profile_prefetch_refill": {"phase": "profile_prefetch_refill", "status": "completed", "elapsed_ms": 1000},
        "profile_refill_event_level_materialization_followup": {
            "phase": "profile_refill_event_level_materialization_followup",
            "status": "completed",
            "elapsed_ms": 26000,
        },
        "workflow_resume": {"phase": "workflow_resume", "status": "completed", "elapsed_ms": 1000},
        "post_projection_workflow_resume": {
            "phase": "post_projection_workflow_resume",
            "status": "skipped",
            "elapsed_ms": 0,
        },
        "post_completion_reconcile": {"phase": "post_completion_reconcile", "status": "completed", "elapsed_ms": 500},
        "excel_intake_recovery": {"phase": "excel_intake_recovery", "status": "skipped", "elapsed_ms": 0},
        "board_visible_apply": {"phase": "board_visible_apply", "status": "completed", "elapsed_ms": 100},
        "snapshot_full_materialization": {
            "phase": "snapshot_full_materialization",
            "status": "skipped",
            "elapsed_ms": 0,
        },
        "explicit_job_followup_rounds": {
            "phase": "explicit_job_followup_rounds",
            "status": "skipped",
            "elapsed_ms": 0,
        },
        "remote_event_followup": {"phase": "remote_event_followup", "status": "skipped", "elapsed_ms": 0},
        "post_recovery_housekeeping": {
            "phase": "post_recovery_housekeeping",
            "status": "skipped",
            "elapsed_ms": 0,
        },
        "total": {"phase": "total", "status": "completed", "elapsed_ms": 43195},
    }
    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "poll_auto_recovery",
                "status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["slow_phase_present"] is False
    assert recovery["slow_total_phase_present"] is True
    assert recovery["recovery_tick_budget_exhausted_count"] == 0
    assert recovery["elapsed_ms"]["max"] == 26000.0
    assert recovery["total_elapsed_ms"]["max"] == 43195.0
    assert recovery["slo_violation_detected"] is False
    assert recovery["optimization_attention_detected"] is True
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "recovery_total_elapsed_slow"
    }


def test_workflow_service_metrics_counts_clean_recovery_tick_budget_exhaustion_as_cooperative() -> None:
    phase_metrics = {
        name: {"phase": name, "status": "skipped", "elapsed_ms": 0}
        for name in [
            "search_seed_discovery",
            "worker_recovery",
            "blocked_workflow_cleanup",
            "local_apply_backlog",
            "event_level_materialization_followup",
            "profile_prefetch_refill",
            "profile_refill_event_level_materialization_followup",
            "workflow_resume",
            "post_projection_workflow_resume",
            "post_completion_reconcile",
            "excel_intake_recovery",
            "board_visible_apply",
            "snapshot_full_materialization",
            "explicit_job_followup_rounds",
            "remote_event_followup",
            "post_recovery_housekeeping",
            "total",
        ]
    }
    phase_metrics["worker_recovery"]["budget_exhausted"] = True
    phase_metrics["total"] = {
        "phase": "total",
        "status": "completed",
        "elapsed_ms": 30001,
        "budget_exhausted": True,
    }

    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "job_scoped_recovery",
                "status": "completed",
                "next_tick_requested": True,
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["recovery_tick_budget_exhausted_count"] == 1
    assert recovery["recovery_tick_budget_exhausted_present"] is True
    assert recovery["budget_yield_next_tick_requested_count"] == 1
    assert recovery["cooperative_budget_yield_count"] == 1
    assert recovery["cooperative_budget_yield_present"] is True
    assert recovery["budget_yield_attention_required"] is False
    assert recovery["budget_yield_contract"] == "cooperative_recovery_budget_yield"
    assert recovery["budget_yield_manual_handoff_blocking"] is False
    assert "recovery_tick_budget_exhausted" not in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_workflow_service_metrics_keeps_budget_yield_attention_without_next_tick() -> None:
    phase_metrics = {
        name: {"phase": name, "status": "skipped", "elapsed_ms": 0}
        for name in [
            "search_seed_discovery",
            "worker_recovery",
            "blocked_workflow_cleanup",
            "local_apply_backlog",
            "event_level_materialization_followup",
            "profile_prefetch_refill",
            "profile_refill_event_level_materialization_followup",
            "workflow_resume",
            "post_projection_workflow_resume",
            "post_completion_reconcile",
            "excel_intake_recovery",
            "board_visible_apply",
            "snapshot_full_materialization",
            "explicit_job_followup_rounds",
            "remote_event_followup",
            "post_recovery_housekeeping",
            "total",
        ]
    }
    phase_metrics["worker_recovery"]["budget_exhausted"] = True
    phase_metrics["total"] = {
        "phase": "total",
        "status": "completed",
        "elapsed_ms": 30001,
        "budget_exhausted": True,
    }

    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "job_scoped_recovery",
                "status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["recovery_tick_budget_exhausted_count"] == 1
    assert recovery["budget_yield_next_tick_requested_count"] == 0
    assert recovery["budget_yield_next_tick_missing_count"] == 1
    assert recovery["cooperative_budget_yield_count"] == 0
    assert recovery["budget_yield_attention_required"] is True
    assert "recovery_tick_budget_exhausted" in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_workflow_service_metrics_counts_clean_recovery_handoff_yield_as_cooperative() -> None:
    phase_metrics = {
        name: {"phase": name, "status": "skipped", "elapsed_ms": 0}
        for name in [
            "search_seed_discovery",
            "worker_recovery",
            "blocked_workflow_cleanup",
            "local_apply_backlog",
            "event_level_materialization_followup",
            "profile_prefetch_refill",
            "profile_refill_event_level_materialization_followup",
            "workflow_resume",
            "post_projection_workflow_resume",
            "post_completion_reconcile",
            "excel_intake_recovery",
            "board_visible_apply",
            "snapshot_full_materialization",
            "explicit_job_followup_rounds",
            "remote_event_followup",
            "post_recovery_housekeeping",
            "total",
        ]
    }
    phase_metrics["total"] = {
        "phase": "total",
        "status": "completed",
        "elapsed_ms": 1000,
        "durable_work_handoff_yield": True,
        "counts": {"durable_work_handoff_yield_count": 1},
    }

    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "job_scoped_recovery",
                "status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["durable_work_handoff_yield_count"] == 1
    assert recovery["durable_work_handoff_yield_present"] is True
    assert recovery["cooperative_handoff_yield_count"] == 1
    assert recovery["cooperative_handoff_yield_present"] is True
    assert recovery["handoff_yield_attention_required"] is False
    assert recovery["handoff_yield_contract"] == "cooperative_scheduling_yield"
    assert recovery["handoff_yield_manual_handoff_blocking"] is False
    assert recovery["samples"][0]["durable_work_handoff_yield"] is True
    assert "recovery_durable_work_handoff_yield" not in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_workflow_service_metrics_keeps_handoff_yield_attention_when_recovery_contract_dirty() -> None:
    phase_metrics = {
        name: {"phase": name, "status": "skipped", "elapsed_ms": 0}
        for name in [
            "search_seed_discovery",
            "worker_recovery",
            "blocked_workflow_cleanup",
            "local_apply_backlog",
            "event_level_materialization_followup",
            "profile_prefetch_refill",
            "profile_refill_event_level_materialization_followup",
            "workflow_resume",
            "post_projection_workflow_resume",
            "post_completion_reconcile",
            "excel_intake_recovery",
            "board_visible_apply",
            "snapshot_full_materialization",
            "explicit_job_followup_rounds",
            "remote_event_followup",
            "post_recovery_housekeeping",
            "total",
        ]
    }
    phase_metrics["local_apply_backlog"] = {
        "phase": "local_apply_backlog",
        "status": "failed",
        "elapsed_ms": 100,
        "reason": "boom",
    }
    phase_metrics["total"] = {
        "phase": "total",
        "status": "completed",
        "elapsed_ms": 1000,
        "durable_work_handoff_yield": True,
        "counts": {"durable_work_handoff_yield_count": 1},
    }

    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "job_scoped_recovery",
                "status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["durable_work_handoff_yield_count"] == 1
    assert recovery["cooperative_handoff_yield_count"] == 0
    assert recovery["handoff_yield_attention_required"] is True
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "recovery_durable_work_handoff_yield",
        "recovery_phase_failure",
    }


def test_workflow_service_metrics_counts_legacy_recovery_bridge_usage() -> None:
    phase_metrics = {
        name: {"phase": name, "status": "skipped", "elapsed_ms": 0}
        for name in [
            "search_seed_discovery",
            "worker_recovery",
            "blocked_workflow_cleanup",
            "local_apply_backlog",
            "event_level_materialization_followup",
            "profile_prefetch_refill",
            "profile_refill_event_level_materialization_followup",
            "workflow_resume",
            "post_projection_workflow_resume",
            "post_completion_reconcile",
            "excel_intake_recovery",
            "board_visible_apply",
            "snapshot_full_materialization",
            "explicit_job_followup_rounds",
            "remote_event_followup",
            "post_recovery_housekeeping",
            "total",
        ]
    }
    phase_metrics["local_apply_backlog"] = {
        "phase": "local_apply_backlog",
        "status": "completed",
        "elapsed_ms": 250,
        "owner": "legacy_job_materialization_items",
        "legacy_bridge_used": True,
        "reason": "migration_bridge_before_deletion",
        "counts": {"candidate_count": 12},
    }

    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "job_scoped_recovery",
                "status": "completed",
                "recovery_phase_metrics": phase_metrics,
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["legacy_bridge_used_count"] == 1
    assert recovery["legacy_bridge_used_present"] is True
    assert recovery["legacy_bridge_used_phases"] == [
        {
            "run_index": 0,
            "phase": "local_apply_backlog",
            "status": "completed",
            "reason": "migration_bridge_before_deletion",
            "elapsed_ms": 250.0,
            "owner": "legacy_job_materialization_items",
        }
    ]
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "legacy_materialization_recovery_bridge"
    }


def test_workflow_service_metrics_flags_missing_recovery_phase_metrics() -> None:
    metrics = build_workflow_service_metrics(
        worker_recovery_runs=[
            {
                "phase": "post_terminal",
                "status": "completed",
                "daemon_status": "completed",
            }
        ]
    )

    recovery = metrics["recovery_phase_metrics"]
    assert recovery["report_available"] is True
    assert recovery["run_count"] == 1
    assert recovery["runs_with_phase_metrics_count"] == 0
    assert recovery["missing_phase_present"] is True
    assert recovery["missing_phase_count"] == len(recovery["required_phase_names"])
    assert recovery["missing_phases"][0]["phase"] == "<all>"
    assert any(
        item["kind"] == "recovery_phase_metrics_missing"
        for item in metrics["bottlenecks"]["top_bottlenecks"]
    )


def test_post_profile_terminal_queue_leak_only_evaluates_terminal_window() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 10,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:10+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-07T10:00:10+00:00"},
            }
        ],
        job_events=[
            {
                "created_at": "2026-05-07T10:00:04+00:00",
                "payload": {
                    "profile_prefetch": {
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "terminal_queue_state_leak_count": 3,
                            "refill_queue_state_counts": {"planned_dispatch": 3},
                        }
                    }
                },
            },
            {
                "created_at": "2026-05-07T10:00:11+00:00",
                "payload": {
                    "profile_prefetch": {
                        "profile_prefetch_queue": {
                            "kind": "linkedin_profile_prefetch_queue",
                            "terminal_queue_state_leak_count": 0,
                            "refill_queue_state_counts": {},
                        }
                    }
                },
            },
        ],
    )

    terminal_state = metrics["post_profile_completion"]["url_terminal_state_recording"]
    assert terminal_state["queue_snapshot_count"] == 2
    assert terminal_state["terminal_queue_state_leak_count"] == 0
    assert terminal_state["slo_violation"] is False


def test_post_profile_board_patch_inside_local_apply_window_counts_as_immediate() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 10,
                "status": "completed",
                "updated_at": "2026-05-07T10:00:04+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {"remote_completed_at": "2026-05-07T10:00:04+00:00"},
            }
        ],
        materialization_items=[
            {
                "item_id": "local-apply-1",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "snapshot_id": "snap-1",
                "created_at": "2026-05-07T10:00:05+00:00",
                "completed_at": "2026-05-07T10:00:08+00:00",
            },
            {
                "item_id": "board-visible-1",
                "item_kind": "board_visible_delta_apply",
                "status": "completed",
                "snapshot_id": "snap-1",
                "completed_at": "2026-05-07T10:00:06+00:00",
            },
        ],
        board_visible_patches=[
            {
                "snapshot_id": "snap-1",
                "sequence_index": 1,
                "published_at": "2026-05-07T10:00:06+00:00",
                "cumulative_candidate_count": 1,
            }
        ],
    )

    profile_patch = metrics["post_profile_completion"]["profile_file_visible_to_board_patch_visible"]
    assert profile_patch["elapsed_ms"]["max"] == 0.0
    assert profile_patch["slo_violation"] is False


def test_workflow_service_metrics_exposes_legacy_materialization_write_contract() -> None:
    metrics = build_workflow_service_metrics(
        materialization_items=[
            {
                "item_id": "legacy-normal",
                "job_id": "job-1",
                "item_kind": "board_visible_delta_apply",
                "status": "queued",
                "metadata": {
                    "legacy_materialization_write_contract": {
                        "normal_path": True,
                        "migration_adapter": False,
                        "target_runtime_table": "workflow_commands",
                        "retirement_phase": "Phase W4",
                    }
                },
            },
            {
                "item_id": "legacy-migration",
                "job_id": "job-1",
                "item_kind": "local_apply_closure",
                "status": "queued",
                "metadata": {
                    "legacy_materialization_write_contract": {
                        "normal_path": False,
                        "migration_adapter": True,
                        "target_runtime_table": "workflow_commands",
                        "retirement_phase": "Phase W4",
                    }
                },
            },
            {
                "item_id": "legacy-missing",
                "job_id": "job-1",
                "item_kind": "snapshot_full_materialization",
                "status": "queued",
                "metadata": {},
            },
        ],
    )

    contract = metrics["legacy_materialization_write_contract"]
    assert contract["report_available"] is True
    assert contract["item_count"] == 3
    assert contract["normal_path_write_count"] == 1
    assert contract["migration_adapter_write_count"] == 1
    assert contract["missing_contract_count"] == 1
    assert contract["normal_path_write_present"] is True
    assert contract["missing_contract_present"] is True
    assert contract["normal_path_kind_counts"] == {"board_visible_delta_apply": 1}
    assert contract["migration_adapter_kind_counts"] == {"local_apply_closure": 1}
    assert contract["missing_contract_kind_counts"] == {"snapshot_full_materialization": 1}
    assert any(
        item["kind"] == "legacy_materialization_normal_write"
        for item in metrics["bottlenecks"]["top_bottlenecks"]
    )


def test_workflow_service_metrics_reports_zero_legacy_materialization_writes() -> None:
    metrics = build_workflow_service_metrics()

    contract = metrics["legacy_materialization_write_contract"]
    assert contract["report_available"] is True
    assert contract["item_count"] == 0
    assert contract["normal_path_write_count"] == 0
    assert contract["migration_adapter_write_count"] == 0
    assert contract["missing_contract_count"] == 0
    assert contract["normal_path_write_present"] is False
    assert contract["missing_contract_present"] is False


def test_remote_provider_in_flight_duplicates_do_not_count_as_actionable_lag() -> None:
    metrics = build_workflow_service_metrics(
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received_in_flight",
                "created_at": "2026-05-07T10:02:00+00:00",
                "payload": {
                    "target_worker_ids": [1],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "provider_webhook",
                        "remote_to_local_event_lag_ms": 120000,
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "created_at": "2026-05-07T10:00:05+00:00",
                "payload": {
                    "target_worker_ids": [1],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "provider_webhook",
                        "remote_to_local_event_lag_ms": 5000,
                    },
                },
            },
        ]
    )

    remote_events = metrics["remote_provider_events"]
    assert remote_events["in_flight_duplicate_count"] == 1
    assert remote_events["remote_to_local_event_lag_ms"]["max"] == 120000.0
    assert remote_events["actionable_remote_to_local_event_lag_ms"]["max"] == 5000.0
    assert remote_events["lag_violation"] is False


def test_remote_provider_received_duplicate_after_watcher_is_not_actionable_lag() -> None:
    metrics = build_workflow_service_metrics(
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "created_at": "2026-05-15T21:31:51+00:00",
                "payload": {
                    "target_worker_ids": [1],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "local_provider_event_watcher",
                        "remote_to_local_event_lag_ms": 1059,
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "created_at": "2026-05-15T21:32:20+00:00",
                "payload": {
                    "target_worker_ids": [1],
                    "event": {"run_id": "run-1", "dataset_id": "dataset-1"},
                    "event_metrics": {
                        "source": "provider_webhook",
                        "remote_to_local_event_lag_ms": 30059,
                    },
                },
            },
        ]
    )

    remote_events = metrics["remote_provider_events"]
    assert remote_events["received_count"] == 1
    assert remote_events["late_duplicate_count"] == 1
    assert remote_events["inferred_received_duplicate_count"] == 1
    assert remote_events["remote_to_local_event_lag_ms"]["max"] == 30059.0
    assert remote_events["actionable_remote_to_local_event_lag_ms"]["max"] == 1059.0
    assert remote_events["late_duplicate_remote_to_local_event_lag_ms"]["max"] == 30059.0
    assert remote_events["lag_violation"] is False


def test_workflow_service_metrics_exposes_target_public_web_guardrails() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd_crm_public_web_queue_batch_1",
                "workflow_run_id": "wf_crm_public_web_batch_1",
                "operation_id": "op_crm_public_web_batch_1",
                "command_type": "crm.public_web.queue_batch",
                "owner": "crm_public_web_owner",
                "stage_id": "crm_public_web_queue_batch",
                "causal_group_id": "crm.public_web.queue_batch:batch-1",
                "source_event_id": "evt_crm_public_web_queue_batch_1",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"crm_public_web_run": 1, "crm_record": 1},
                "readiness_effect": "crm_public_web_workers_queued",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "idempotency_key": "crm.public_web.queue_batch:batch-1",
                "payload": {"batch_id": "public-web-batch-1"},
            }
        ],
        target_candidate_public_web_batches=[
            {
                "batch_id": "public-web-batch-1",
                "status": "completed",
                "updated_at": "2026-05-04T10:00:00+00:00",
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": "crm_public_web_v1",
                "summary": {
                    "phase_metrics": {
                        "run_count": 2,
                        "metric_run_count": 1,
                        "remote_search_pending_run_count": 0,
                        "unmaterialized_signal_gap_count": 1,
                        "completed_without_materialized_signals_count": 1,
                        "terminal_with_errors_count": 0,
                        "partial_failure_count": 0,
                        "runs_with_metric_errors_count": 0,
                        "missing_phase_metric_count": 1,
                        "provider_or_fetch_failure_count": 2,
                        "local_processing_error_count": 1,
                        "slowest_phase": "document_fetch",
                        "duration_by_phase_ms_max": {
                            "search_poll": 1250.0,
                            "document_fetch": 31000.0,
                            "adjudication": 2100.0,
                            "signal_materialization": 700.0,
                        },
                        "phase_lag_risk_reasons": [
                            "terminal_signal_materialization_gap",
                            "missing_phase_metrics",
                        ],
                        "service_guardrail_violation_detected": True,
                    }
                },
            }
        ]
    )

    public_web = metrics["target_candidate_public_web"]
    assert metrics["report_available"] is True
    assert public_web["report_available"] is True
    assert public_web["batch_count"] == 1
    assert public_web["crm_storage_owner_batch_count"] == 1
    assert public_web["legacy_storage_owner_batch_count"] == 0
    assert public_web["storage_owner_counts"]["crm_public_web_v1"] == 1
    assert public_web["execution_backend_counts"]["crm_public_web_v1"] == 1
    assert public_web["execution_backend_bridge_present"] is False
    assert public_web["execution_backend_bridge_count"] == 0
    assert public_web["execution_backend_retired"] is True
    assert public_web["queue_batch_command_contract_present"] is True
    assert public_web["queue_batch_command_count"] == 1
    assert public_web["queue_batch_command_succeeded_count"] == 1
    assert public_web["queue_batch_command_pending_count"] == 0
    assert public_web["queue_batch_command_expected_owner_count"] == 1
    assert public_web["queue_batch_command_invalid_owner_count"] == 0
    assert public_web["queue_batch_command_incomplete_causality_count"] == 0
    assert public_web["queue_batch_command_status_counts"]["succeeded"] == 1
    assert public_web["queue_batch_command_owner_counts"]["crm_public_web_owner"] == 1
    assert public_web["latest_batch"]["public_web_storage_owner"] == "crm_public_web_v1"
    assert public_web["latest_batch"]["public_web_execution_backend"] == "crm_public_web_v1"
    assert public_web["run_count"] == 2
    assert public_web["metric_run_count"] == 1
    assert public_web["unmaterialized_signal_gap_count"] == 1
    assert public_web["completed_without_materialized_signals_count"] == 1
    assert public_web["missing_phase_metric_count"] == 1
    assert public_web["provider_or_fetch_failure_count"] == 2
    assert public_web["local_processing_error_count"] == 1
    assert public_web["slowest_phase_counts"]["document_fetch"] == 1
    assert public_web["duration_by_phase_ms_max"]["document_fetch"] == 31000.0
    assert public_web["service_guardrail_violation_detected"] is True
    assert public_web["risk_reason_counts"]["missing_phase_metrics"] == 1
    bottlenecks = metrics["bottlenecks"]["top_bottlenecks"]
    assert any(item["kind"] == "target_public_web_guardrail_violation" for item in bottlenecks)
    assert any(item["kind"] == "target_public_web_slowest_phase" for item in bottlenecks)


def test_workflow_service_metrics_flags_target_public_web_missing_queue_batch_command() -> None:
    metrics = build_workflow_service_metrics(
        target_candidate_public_web_batches=[
            {
                "batch_id": "public-web-batch-missing-command",
                "status": "completed",
                "updated_at": "2026-05-04T10:00:00+00:00",
                "public_web_storage_owner": "crm_public_web_v1",
                "execution_backend": "crm_public_web_v1",
                "summary": {"phase_metrics": {"run_count": 1}},
            }
        ]
    )

    public_web = metrics["target_candidate_public_web"]
    assert public_web["report_available"] is True
    assert public_web["queue_batch_command_missing"] is True
    assert public_web["queue_batch_command_contract_present"] is False
    assert public_web["queue_batch_command_count"] == 0
    assert public_web["queue_batch_command_succeeded_count"] == 0


def test_workflow_service_metrics_exposes_legacy_public_web_retirement_contract() -> None:
    metrics = build_workflow_service_metrics(
        legacy_public_web_retirement_audit={
            "contract_version": "legacy_public_web_retirement_audit_v1",
            "status": "blocked",
            "read_only": True,
            "deletion_allowed": False,
            "row_limit": 10000,
            "summary": {
                "legacy_target_candidate_public_web_row_count": 2,
                "legacy_target_candidate_public_web_batch_count": 1,
                "legacy_target_candidate_public_web_run_count": 1,
                "legacy_target_candidate_public_web_promotion_count": 0,
                "legacy_target_candidate_public_web_limited": False,
                "crm_public_web_batch_count": 3,
                "crm_public_web_run_count": 12,
                "crm_public_web_promotion_count": 4,
            },
            "deletion_blockers": [
                {"blocker": "legacy_target_candidate_public_web_rows_present"},
            ],
            "deletion_gate": {
                "normal_path_owner": "crm_public_web_v1",
                "legacy_owner": "target_candidate_public_web_v1",
                "requires_migration_or_cold_backup": True,
            },
        }
    )

    retirement = metrics["legacy_public_web_retirement"]
    assert metrics["report_available"] is True
    assert retirement["report_available"] is True
    assert retirement["contract_version"] == "legacy_public_web_retirement_audit_v1"
    assert retirement["legacy_rows_present"] is True
    assert retirement["legacy_target_candidate_public_web_row_count"] == 2
    assert retirement["crm_public_web_run_count"] == 12
    assert retirement["blocker_names"] == ["legacy_target_candidate_public_web_rows_present"]
    assert retirement["deletion_gate"]["normal_path_owner"] == "crm_public_web_v1"
    assert any(item["kind"] == "legacy_public_web_rows_present" for item in metrics["bottlenecks"]["top_bottlenecks"])


def test_workflow_service_metrics_exposes_company_public_web_collector_guardrails() -> None:
    metrics = build_workflow_service_metrics(
        company_public_web_runs=[
            {
                "run_id": "company-public-web-run-1",
                "status": "completed",
                "phase": "completed",
                "updated_at": "2026-05-04T10:00:00+00:00",
                "summary": {
                    "asset_count": 4,
                    "collector_record_count": 4,
                    "collector_source_count": 4,
                    "collector_document_fetch_count": 4,
                    "collector_document_fetch_failure_count": 0,
                    "collector_fetch_duration_ms_max": 125.5,
                    "provider_result_count": 0,
                    "raw_assets_included": False,
                    "collection_mode_counts": {"collector_bundle": 4},
                    "collector_type_counts": {
                        "rss_items": 1,
                        "arxiv_publications": 1,
                        "openreview_publications": 1,
                        "crawled_pages": 1,
                    },
                },
                "metadata": {
                    "artifact_paths": {"collector_manifest": "/tmp/collector_manifest.json"},
                },
            },
            {
                "run_id": "company-public-web-run-2",
                "status": "failed",
                "phase": "failed",
                "updated_at": "2026-05-04T10:01:00+00:00",
                "summary": {
                    "asset_count": 0,
                    "collector_record_count": 0,
                    "collector_source_count": 1,
                    "collector_document_fetch_count": 0,
                    "collector_document_fetch_failure_count": 1,
                    "collector_fetch_duration_ms_max": 0.0,
                    "raw_assets_included": False,
                    "collection_mode_counts": {"collector_bundle": 0},
                },
            },
        ]
    )

    company_public_web = metrics["company_public_web"]
    assert metrics["report_available"] is True
    assert company_public_web["report_available"] is True
    assert company_public_web["run_count"] == 2
    assert company_public_web["asset_count"] == 4
    assert company_public_web["collector_record_count"] == 4
    assert company_public_web["collector_source_count"] == 5
    assert company_public_web["collector_document_fetch_count"] == 4
    assert company_public_web["collector_document_fetch_failure_count"] == 1
    assert company_public_web["collector_fetch_duration_ms_max"] == 125.5
    assert company_public_web["failed_run_count"] == 1
    assert company_public_web["raw_assets_included_count"] == 0
    assert company_public_web["collection_mode_counts"]["collector_bundle"] == 4
    assert company_public_web["collector_type_counts"]["openreview_publications"] == 1
    assert company_public_web["service_guardrail_violation_detected"] is True
    bottlenecks = metrics["bottlenecks"]["top_bottlenecks"]
    assert any(item["kind"] == "company_public_web_guardrail_violation" for item in bottlenecks)


def test_workflow_service_metrics_exposes_profile_out_of_order_completion_coverage() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 11,
                "span_id": 111,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-current-early",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
            {
                "worker_id": 12,
                "span_id": 112,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-current-fast-later",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
            {
                "worker_id": 13,
                "span_id": 113,
                "lane_id": "enrichment_specialist",
                "worker_key": "profile-batch-former-interleaved",
                "status": "completed",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
        ],
        trace_spans=[
            {
                "span_id": 111,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-04-30T16:00:00+00:00",
                "completed_at": "2026-04-30T16:05:00+00:00",
            },
            {
                "span_id": 112,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-04-30T16:01:00+00:00",
                "completed_at": "2026-04-30T16:02:00+00:00",
            },
            {
                "span_id": 113,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-04-30T16:02:00+00:00",
                "completed_at": "2026-04-30T16:03:00+00:00",
            },
        ],
    )

    ordering = metrics["worker_timeline"]["out_of_order_completion"]
    assert ordering["profile_batch_worker_count"] == 3
    assert ordering["profile_batch_inversion_count"] == 2
    assert ordering["profile_batch_observed"] is True
    assert ordering["samples"][0]["earlier_worker_id"] == 11
    assert ordering["samples"][0]["later_worker_id"] == 12
    assert ordering["samples"][0]["completion_lead_ms"] == 180000.0


def test_workflow_service_metrics_exposes_board_visible_projection_guardrails() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "phase": "current_snapshot_materializing",
            "serving_projection_id": "/tmp/job.partial.asset_population.json",
            "serving_projection_phase": "partial_delta_overlay",
            "served_candidate_count": 1200,
            "delta_profile_required_count": 89,
            "delta_profile_fetched_count": 54,
            "delta_profile_materialized_count": 19,
            "delta_profile_board_visible_count": 19,
        },
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "candidate_ids": ["delta_1", "delta_2"],
                "cumulative_candidate_ids": ["delta_1", "delta_2"],
                "cumulative_candidate_count": 2,
                "served_candidate_count": 1132,
                "card_materialization_summary": {
                    "candidate_count": 2,
                    "display_ready_candidate_count": 0,
                    "preview_candidate_count": 2,
                    "needs_profile_completion_candidate_count": 0,
                    "low_profile_richness_candidate_count": 0,
                    "quality_fields_available": True,
                },
                "published_at": "2026-04-30T16:03:00+00:00",
            },
            {
                "patch_id": "job|snap|2",
                "sequence_index": 2,
                "candidate_ids": ["delta_3"],
                "cumulative_candidate_ids": ["delta_1", "delta_2", "delta_3"],
                "cumulative_candidate_count": 19,
                "served_candidate_count": 1200,
                "card_materialization_summary": {
                    "candidate_count": 19,
                    "display_ready_candidate_count": 8,
                    "preview_candidate_count": 11,
                    "needs_profile_completion_candidate_count": 1,
                    "low_profile_richness_candidate_count": 1,
                    "quality_fields_available": True,
                },
                "published_at": "2026-04-30T16:04:00+00:00",
            },
        ],
    )

    projection = metrics["board_visible_projection"]
    assert metrics["report_available"] is True
    assert projection["report_available"] is True
    assert projection["projection_present"] is True
    assert projection["patch_sequence_contiguous"] is True
    assert projection["patch_log_count"] == 2
    assert projection["patch_log_latest_cumulative_count"] == 19
    assert projection["patch_consumable_card_nonzero_count"] == 1
    assert projection["patch_consumable_card_distinct_count"] == 1
    assert projection["pure_shell_patch_count"] == 1
    assert projection["fetched_to_board_visible_lag_count"] == 35
    assert projection["materialization_lag_violation"] is True
    assert projection["projection_missing_for_visible_count"] is False
    assert projection["patch_log_missing_for_visible_count"] is False
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} == {
        "board_visible_materialization_lag"
    }


def test_workflow_service_metrics_reads_post_profile_typed_commands() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "status": "completed",
                "completed_at": "2026-05-23T07:33:10+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            }
        ],
        job_events=[
            {
                "stage": "retrieving",
                "status": "running",
                "created_at": "2026-05-23T07:33:14+00:00",
                "payload": {
                    "profile_prefetch": {
                        "profile_refill_trigger": {
                            "signal_only": True,
                            "trigger_source": "worker_completion_callback_enqueue",
                            "status": "queued",
                            "elapsed_ms": 4,
                            "requested_url_count": 0,
                            "dispatched_url_count": 0,
                            "queued_worker_count": 0,
                        }
                    }
                },
            }
        ],
        result_view_lifecycle={
            "phase": "current_snapshot_materializing",
            "serving_projection_id": "proj_post_profile",
            "serving_projection_phase": "partial_delta_overlay",
            "served_candidate_count": 140,
            "delta_profile_required_count": 1,
            "delta_profile_fetched_count": 1,
            "delta_profile_materialized_count": 1,
            "delta_profile_board_visible_count": 1,
        },
        workflow_commands=[
            {
                "command_id": "cmd_local_apply",
                "command_type": "linkedin.local_profile_delta.apply",
                "owner": "profile_local_apply_owner",
                "causal_group_id": "cg_post_profile_1",
                "source_event_id": "evt_local_apply",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 1},
                "readiness_effect": "local_profile_delta_applied",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "created_at": "2026-05-23T07:33:11+00:00",
                "updated_at": "2026-05-23T07:33:12+00:00",
                "payload": {
                    "item_id": "local_apply_item",
                    "snapshot_id": "snapshot_post_profile",
                    "worker_kind": "harvest_prefetch",
                    "source_worker_ids": [1],
                    "candidate_count": 1,
                },
                "result": {
                    "candidate_count": 1,
                    "completed_at": "2026-05-23T07:33:12+00:00",
                },
            },
            {
                "command_id": "cmd_board_visible",
                "command_type": "projection.board_visible_patch.publish",
                "owner": "board_visible_projection_owner",
                "causal_group_id": "cg_post_profile_1",
                "parent_command_id": "cmd_local_apply",
                "source_event_id": "evt_board_visible",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 1},
                "readiness_effect": "board_visible_patch_published",
                "causality_schema_version": "command_causality_v1",
                "status": "succeeded",
                "created_at": "2026-05-23T07:33:12+00:00",
                "updated_at": "2026-05-23T07:33:15+00:00",
                "payload": {
                    "item_id": "board_visible_item",
                    "snapshot_id": "snapshot_post_profile",
                    "candidate_count": 1,
                },
                "result": {
                    "candidate_count": 1,
                    "completed_at": "2026-05-23T07:33:15+00:00",
                },
            },
        ],
    )

    post_profile = metrics["post_profile_completion"]
    assert post_profile["event_level_callback"]["event_count"] == 1
    assert post_profile["event_level_callback"]["elapsed_ms"]["max"] == 4.0
    assert post_profile["profile_file_visible_to_board_patch_visible"]["completed_local_apply_count"] == 1
    assert post_profile["profile_file_visible_to_board_patch_visible"]["completed_board_visible_item_count"] == 1
    assert (
        post_profile["profile_file_visible_to_board_patch_visible"]["pairing_contract"]
        == "typed_causal_group_for_workflow_commands"
    )
    assert post_profile["profile_file_visible_to_board_patch_visible"]["elapsed_ms"]["max"] == 3000.0
    assert post_profile["slo_violation_detected"] is False


def test_post_profile_typed_commands_pair_by_causal_group_not_snapshot() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "status": "completed",
                "completed_at": "2026-05-23T07:33:10+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            }
        ],
        workflow_commands=[
            {
                "command_id": "cmd_local_apply",
                "command_type": "linkedin.local_profile_delta.apply",
                "owner": "profile_local_apply_owner",
                "causal_group_id": "cg_shared",
                "source_event_id": "evt_local",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 1},
                "status": "succeeded",
                "created_at": "2026-05-23T07:33:11+00:00",
                "updated_at": "2026-05-23T07:33:12+00:00",
                "payload": {
                    "item_id": "local_apply_item",
                    "snapshot_id": "snapshot_a",
                    "worker_kind": "harvest_prefetch",
                    "source_worker_ids": [1],
                },
                "result": {"candidate_count": 1, "completed_at": "2026-05-23T07:33:12+00:00"},
            },
            {
                "command_id": "cmd_wrong_snapshot_board_visible",
                "command_type": "projection.board_visible_patch.publish",
                "owner": "board_visible_projection_owner",
                "causal_group_id": "cg_other",
                "source_event_id": "evt_other_board",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 1},
                "status": "succeeded",
                "created_at": "2026-05-23T07:33:12+00:00",
                "updated_at": "2026-05-23T07:33:13+00:00",
                "payload": {"item_id": "wrong_board", "snapshot_id": "snapshot_a"},
                "result": {"candidate_count": 1, "completed_at": "2026-05-23T07:33:13+00:00"},
            },
            {
                "command_id": "cmd_board_visible",
                "command_type": "projection.board_visible_patch.publish",
                "owner": "board_visible_projection_owner",
                "causal_group_id": "cg_shared",
                "source_event_id": "evt_board",
                "source_event_type": "CommandPlanRequested",
                "produced_entity_counts": {"candidate": 1},
                "status": "succeeded",
                "created_at": "2026-05-23T07:33:12+00:00",
                "updated_at": "2026-05-23T07:33:15+00:00",
                "payload": {"item_id": "board_visible_item", "snapshot_id": "snapshot_b"},
                "result": {"candidate_count": 1, "completed_at": "2026-05-23T07:33:15+00:00"},
            },
        ],
    )

    profile_patch = metrics["post_profile_completion"]["profile_file_visible_to_board_patch_visible"]
    assert profile_patch["elapsed_ms"]["max"] == 3000.0
    assert profile_patch["unmatched_local_apply_count"] == 0
    assert profile_patch["typed_causal_group_pair_count"] == 1
    assert profile_patch["legacy_snapshot_pair_count"] == 0
    assert profile_patch["heuristic_pairing_used"] is False


def test_post_profile_legacy_materialization_slo_reports_snapshot_heuristic_pairing() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 1,
                "status": "completed",
                "updated_at": "2026-05-23T07:33:10+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            }
        ],
        materialization_items=[
            {
                "item_id": "legacy_local_apply",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "snapshot_id": "snapshot_legacy",
                "source_worker_ids": [1],
                "created_at": "2026-05-23T07:33:11+00:00",
                "completed_at": "2026-05-23T07:33:12+00:00",
            },
            {
                "item_id": "legacy_board_visible",
                "item_kind": "board_visible_delta_apply",
                "status": "completed",
                "snapshot_id": "snapshot_legacy",
                "completed_at": "2026-05-23T07:33:15+00:00",
            },
        ],
    )

    profile_patch = metrics["post_profile_completion"]["profile_file_visible_to_board_patch_visible"]
    assert profile_patch["legacy_snapshot_pair_count"] == 1
    assert profile_patch["typed_causal_group_pair_count"] == 0
    assert profile_patch["heuristic_pairing_used"] is True
    assert profile_patch["elapsed_ms"]["max"] == 3000.0


def test_workflow_service_metrics_use_visible_projection_count_for_public_completion() -> None:
    metrics = build_workflow_service_metrics(
        final_summary={
            "analysis_stage": "stage_2_final",
            "default_results_mode": "asset_population",
            "candidate_source": {
                "candidate_count": 8831,
                "unfiltered_candidate_count": 8831,
            },
        },
        result_view_lifecycle={
            "phase": "current_snapshot_serving",
            "current_snapshot_id": "snapshot_visible",
            "served_snapshot_id": "snapshot_visible",
            "serving_projection_id": "proj_visible",
            "serving_projection_phase": "current_snapshot_serving",
            "served_candidate_count": 8830,
            "expected_candidate_count": 8831,
        },
    )

    board_projection = metrics["board_visible_projection"]
    finalization = metrics["finalization_overlay"]
    assert board_projection["raw_expected_candidate_count"] == 8831
    assert board_projection["expected_candidate_count"] == 8830
    assert board_projection["public_expected_count_source"] == "serving_projection_visible_members"
    assert finalization["raw_expected_candidate_count"] == 8831
    assert finalization["expected_candidate_count"] == 8830
    assert finalization["served_complete"] is True
    assert finalization["public_expected_count_source"] == "serving_projection_visible_members"


def test_workflow_service_metrics_flags_missing_overlay_write_mode_on_delta_patch() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "phase": "current_snapshot_materializing",
            "serving_projection_id": "/tmp/job.partial.asset_population.json",
            "serving_projection_phase": "partial_delta_overlay",
            "delta_profile_required_count": 2,
            "delta_profile_fetched_count": 2,
            "delta_profile_materialized_count": 2,
            "delta_profile_board_visible_count": 2,
        },
        board_visible_patches=[
            {
                "patch_id": "job|snap|legacy",
                "sequence_index": 1,
                "patch_kind": "partial_delta_board_visible_patch",
                "patch_phase": "board_visible_delta_applied",
                "candidate_ids": ["delta_1", "delta_2"],
                "cumulative_candidate_ids": ["delta_1", "delta_2"],
                "cumulative_candidate_count": 2,
                "served_candidate_count": 1200,
                "published_at": "2026-04-30T16:03:00+00:00",
            }
        ],
    )

    projection = metrics["board_visible_projection"]
    assert projection["partial_delta_patch_count"] == 1
    assert projection["overlay_write_mode_missing_count"] == 1
    assert projection["overlay_write_mode_missing_present"] is True


def test_workflow_service_metrics_exposes_partial_overlay_fast_path_fallbacks() -> None:
    metrics = build_workflow_service_metrics(
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "candidate_count": 50,
                "cumulative_candidate_count": 50,
                "metadata": {
                    "result_view_metadata_mirror": {
                        "overlay_write_mode": "full_rebuild",
                        "overlay_fast_path_eligible": False,
                    }
                },
            },
            {
                "patch_id": "job|snap|2",
                "sequence_index": 2,
                "candidate_count": 50,
                "cumulative_candidate_count": 100,
                "metadata": {
                    "result_view_metadata_mirror": {
                        "overlay_write_mode": "incremental_partial_overlay",
                        "overlay_fast_path_eligible": True,
                        "overlay_fast_path_used": True,
                    }
                },
            },
            {
                "patch_id": "job|snap|3",
                "sequence_index": 3,
                "candidate_count": 50,
                "cumulative_candidate_count": 150,
                "metadata": {
                    "result_view_metadata_mirror": {
                        "overlay_write_mode": "full_rebuild",
                        "overlay_fast_path_eligible": True,
                        "overlay_fast_path_used": False,
                        "overlay_fallback_reason": "existing_overlay_record_unparseable",
                    }
                },
            },
        ],
    )

    overlay = metrics["board_overlay_writes"]
    assert overlay["report_available"] is True
    assert overlay["mode_counts"]["full_rebuild"] == 2
    assert overlay["incremental_partial_overlay_count"] == 1
    assert overlay["fast_path_eligible_count"] == 2
    assert overlay["fast_path_used_count"] == 1
    assert overlay["eligible_full_rebuild_fallback_count"] == 1
    assert overlay["eligible_full_rebuild_fallback_present"] is True
    assert overlay["fallback_reason_counts"] == {"existing_overlay_record_unparseable": 1}
    assert "partial_overlay_fast_path_fallback" in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_workflow_service_metrics_does_not_count_bootstrap_full_rebuild_as_fast_path_fallback() -> None:
    metrics = build_workflow_service_metrics(
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "candidate_count": 80,
                "cumulative_candidate_count": 80,
                "metadata": {
                    "result_view_metadata_mirror": {
                        "overlay_write_mode": "full_rebuild",
                        "overlay_fast_path_eligible": True,
                        "overlay_fast_path_used": True,
                        "overlay_fallback_reason": "",
                    }
                },
            },
            {
                "patch_id": "job|snap|2",
                "sequence_index": 2,
                "candidate_count": 40,
                "cumulative_candidate_count": 120,
                "metadata": {
                    "result_view_metadata_mirror": {
                        "overlay_write_mode": "incremental_partial_overlay",
                        "overlay_fast_path_eligible": True,
                        "overlay_fast_path_used": True,
                    }
                },
            },
        ],
    )

    overlay = metrics["board_overlay_writes"]
    assert overlay["full_rebuild_count"] == 1
    assert overlay["incremental_partial_overlay_count"] == 1
    assert overlay["eligible_full_rebuild_fallback_count"] == 0
    assert overlay["eligible_full_rebuild_fallback_present"] is False


def test_workflow_service_metrics_flags_finalization_full_rewrite_when_board_projection_complete() -> None:
    metrics = build_workflow_service_metrics(
        final_summary={
            "summary_provider": "asset_population_fast_path",
            "analysis_stage": "stage_2_final",
            "default_results_mode": "asset_population",
            "candidate_source": {
                "candidate_count": 8831,
                "asset_population_overlay_path": "job.asset_population.json",
            },
            "asset_population_overlay": {
                "path": "job.asset_population.json",
                "candidate_count": 8831,
                "reuse": False,
                "overlay_write_metadata": {"overlay_write_mode": "full_rebuild"},
            },
        },
        result_view_lifecycle={
            "current_snapshot_id": "snap",
            "served_snapshot_id": "snap",
            "served_candidate_count": 8831,
            "serving_projection_id": "job.asset_population.json",
            "serving_projection_phase": "current_snapshot_row_shell_overlay",
            "delta_profile_required_count": 120,
            "delta_profile_fetched_count": 120,
            "delta_profile_materialized_count": 120,
            "delta_profile_board_visible_count": 120,
        },
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "candidate_count": 120,
                "cumulative_candidate_count": 120,
                "candidate_ids": [f"cand-{index}" for index in range(120)],
            }
        ],
    )

    finalization_overlay = metrics["finalization_overlay"]
    assert finalization_overlay["report_available"] is True
    assert finalization_overlay["reuse_eligible"] is True
    assert finalization_overlay["eligible_full_rewrite_present"] is True
    assert finalization_overlay["eligible_full_rewrite_count"] == 1
    assert "finalization_overlay_reuse_missed" in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_workflow_service_metrics_accepts_finalization_serving_projection_reuse() -> None:
    metrics = build_workflow_service_metrics(
        final_summary={
            "summary_provider": "asset_population_fast_path",
            "analysis_stage": "stage_2_final",
            "candidate_source": {
                "candidate_count": 8831,
                "asset_population_overlay_path": "job.asset_population.json",
                "asset_population_overlay_reuse": {
                    "status": "reused",
                    "reason": "post_profile_board_visible_projection_complete",
                },
            },
            "asset_population_overlay": {
                "path": "job.asset_population.json",
                "candidate_count": 8831,
                "reuse": True,
                "reuse_reason": "post_profile_board_visible_projection_complete",
            },
        },
        result_view_lifecycle={
            "current_snapshot_id": "snap",
            "served_snapshot_id": "snap",
            "served_candidate_count": 8831,
            "serving_projection_id": "job.asset_population.json",
            "serving_projection_phase": "current_snapshot_row_shell_overlay",
            "delta_profile_required_count": 120,
            "delta_profile_fetched_count": 120,
            "delta_profile_materialized_count": 120,
            "delta_profile_board_visible_count": 120,
        },
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "candidate_count": 120,
                "cumulative_candidate_count": 120,
                "candidate_ids": [f"cand-{index}" for index in range(120)],
            }
        ],
    )

    finalization_overlay = metrics["finalization_overlay"]
    assert finalization_overlay["reuse_eligible"] is True
    assert finalization_overlay["reuse_used"] is True
    assert finalization_overlay["eligible_full_rewrite_present"] is False
    assert "finalization_overlay_reuse_missed" not in {
        item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]
    }


def test_workflow_service_metrics_counts_needs_completion_as_consumable_card_progress() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "serving_projection_id": "job.partial.asset_population.json",
            "serving_projection_phase": "partial_delta_overlay",
            "delta_profile_required_count": 50,
            "delta_profile_fetched_count": 50,
            "delta_profile_materialized_count": 30,
            "delta_profile_board_visible_count": 30,
        },
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "cumulative_candidate_count": 10,
                "card_materialization_summary": {
                    "candidate_count": 10,
                    "display_ready_candidate_count": 0,
                    "preview_candidate_count": 10,
                    "needs_profile_completion_candidate_count": 0,
                    "low_profile_richness_candidate_count": 0,
                    "quality_fields_available": True,
                },
            },
            {
                "patch_id": "job|snap|2",
                "sequence_index": 2,
                "cumulative_candidate_count": 20,
                "card_materialization_summary": {
                    "candidate_count": 20,
                    "display_ready_candidate_count": 0,
                    "preview_candidate_count": 8,
                    "needs_profile_completion_candidate_count": 12,
                    "low_profile_richness_candidate_count": 0,
                    "quality_fields_available": True,
                },
            },
            {
                "patch_id": "job|snap|3",
                "sequence_index": 3,
                "cumulative_candidate_count": 30,
                "card_materialization_summary": {
                    "candidate_count": 30,
                    "display_ready_candidate_count": 15,
                    "preview_candidate_count": 10,
                    "needs_profile_completion_candidate_count": 5,
                    "low_profile_richness_candidate_count": 0,
                    "quality_fields_available": True,
                },
            },
        ],
    )

    projection = metrics["board_visible_projection"]
    assert projection["patch_display_ready_nonzero_count"] == 1
    assert projection["patch_display_ready_distinct_count"] == 1
    assert projection["patch_consumable_card_nonzero_count"] == 2
    assert projection["patch_consumable_card_distinct_count"] == 2
    assert projection["patch_delta_card_visible_nonzero_count"] == 2
    assert projection["patch_delta_card_visible_distinct_count"] == 2
    assert projection["pure_shell_patch_count"] == 1


def test_workflow_service_metrics_flags_visible_count_without_projection_or_patch_log() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "delta_profile_fetched_count": 10,
            "delta_profile_board_visible_count": 5,
        },
    )

    projection = metrics["board_visible_projection"]
    assert projection["projection_missing_for_visible_count"] is True
    assert projection["patch_log_missing_for_visible_count"] is True
    assert projection["metadata_replay_dependency"] is False
    assert projection["patch_log_required_missing"] is True
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "board_visible_projection_missing"
    }


def test_workflow_service_metrics_consumable_card_count_does_not_double_count_low_richness() -> None:
    metrics = build_workflow_service_metrics(
        board_visible_patches=[
            {
                "patch_id": "job|snap|1",
                "sequence_index": 1,
                "cumulative_candidate_count": 140,
                "candidate_ids": [f"candidate-{index}" for index in range(140)],
                "card_materialization_summary": {
                    "candidate_count": 140,
                    "display_ready_candidate_count": 140,
                    "preview_candidate_count": 0,
                    "needs_profile_completion_candidate_count": 0,
                    "low_profile_richness_candidate_count": 9,
                    "quality_fields_available": True,
                },
            }
        ],
    )

    projection = metrics["board_visible_projection"]
    assert projection["patch_consumable_card_count"]["max"] == 140.0
    assert projection["patch_display_ready_candidate_count"]["max"] == 140.0


def test_workflow_service_metrics_allows_full_snapshot_serving_without_patch_log() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "state": "current_snapshot_serving",
            "current_snapshot_id": "20260503T155711",
            "served_snapshot_id": "20260503T155711",
            "served_candidate_count": 312,
            "serving_projection_id": "jrv_openai_whisper_current",
            "serving_projection_phase": "current_snapshot_serving",
            "delta_profile_required_count": 12,
            "delta_profile_fetched_count": 12,
            "delta_profile_materialized_count": 12,
            "delta_profile_board_visible_count": 12,
        },
    )

    projection = metrics["board_visible_projection"]
    assert projection["full_snapshot_serving"] is True
    assert projection["patch_log_required"] is False
    assert projection["projection_present"] is True
    assert projection["patch_log_missing_for_visible_count"] is False
    assert projection["patch_log_replay_lag"] is False
    assert projection["patch_log_required_missing"] is False
    assert projection["fetched_to_board_visible_lag_count"] == 0
    assert not any(
        str(item.get("kind") or "").startswith("board_visible")
        for item in metrics["bottlenecks"]["top_bottlenecks"]
    )


def test_workflow_service_metrics_exposes_serving_publication_gap_guardrail() -> None:
    metrics = build_workflow_service_metrics(
        serving_publication_gap={
            "status": "pending_event_time_publication",
            "reason": "running_public_read_does_not_publish_result_view",
            "job_id": "job-openai-agent-gap",
            "target_company": "OpenAI",
            "served_snapshot_id": "baseline-snapshot",
            "current_snapshot_id": "current-snapshot",
            "source_kind": "company_snapshot",
            "source_updated_at": "2026-04-30T16:00:00+00:00",
            "observed_at": "2026-04-30T16:01:00+00:00",
        },
    )

    gap = metrics["serving_publication_gap"]
    assert metrics["report_available"] is True
    assert gap["report_available"] is True
    assert gap["gap_present"] is True
    assert gap["stale_gap_present"] is True
    assert gap["age_ms"] == 60000.0
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "serving_publication_gap"
    }


def test_workflow_service_metrics_exposes_snapshot_full_materialization_queue_guardrails() -> None:
    metrics = build_workflow_service_metrics(
        materialization_items=[
            {
                "item_id": "item-queued",
                "item_kind": "snapshot_full_materialization",
                "status": "queued",
                "phase": "queued",
                "updated_at": "2026-05-02 10:00:00",
            },
            {
                "item_id": "item-retryable",
                "item_kind": "snapshot_full_materialization",
                "status": "failed_retryable",
                "phase": "retry_wait",
                "not_before_at": "2000-01-01 00:00:00",
                "last_error": "transient full materialization failure",
                "updated_at": "2026-05-02 10:01:00",
            },
            {
                "item_id": "item-stale-running",
                "item_kind": "snapshot_full_materialization",
                "status": "running",
                "phase": "applying",
                "lease_expires_at": "2000-01-01 00:00:00",
                "updated_at": "2026-05-02 10:02:00",
            },
            {
                "item_id": "item-applying",
                "item_kind": "snapshot_full_materialization",
                "status": "applying",
                "phase": "applying",
                "updated_at": "2026-05-02 10:03:00",
            },
            {
                "item_id": "item-waiting-prerequisite",
                "item_kind": "snapshot_full_materialization",
                "status": "waiting_prerequisite",
                "phase": "waiting_prerequisite",
                "updated_at": "2026-05-02 10:04:00",
            },
            {
                "item_id": "item-board-visible",
                "item_kind": "board_visible_delta_apply",
                "status": "failed_retryable",
            },
        ],
    )

    queue = metrics["snapshot_full_materialization_queue"]
    assert metrics["report_available"] is True
    assert queue["report_available"] is True
    assert queue["item_count"] == 5
    assert queue["owner"] == "snapshot_materialization_owner"
    assert queue["command_type"] == "snapshot.compaction.run"
    assert queue["scope"] == "background_snapshot_compaction"
    assert queue["readiness_effect"] == "background_artifact_compaction"
    assert queue["manual_handoff_blocking"] is False
    assert queue["background_maintenance_pending"] is False
    assert queue["queued_count"] == 2
    assert queue["waiting_prerequisite_count"] == 1
    assert queue["running_count"] == 2
    assert queue["retryable_count"] == 1
    assert queue["ready_retry_count"] == 1
    assert queue["backlog_count"] == 5
    assert queue["stale_running_count"] == 1
    assert queue["retry_backlog_present"] is True
    assert queue["stale_running_present"] is True
    assert queue["status_counts"]["failed_retryable"] == 1
    assert queue["last_error_samples"] == ["transient full materialization failure"]
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "snapshot_full_materialization_retry_backlog",
        "snapshot_full_materialization_stale_running",
    }


def test_workflow_service_metrics_counts_command_owned_snapshot_full_materialization_queue() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd-snapshot-queued",
                "command_type": "snapshot.compaction.run",
                "owner": "snapshot_materialization_owner",
                "status": "queued",
                "payload": {"job_id": "job-1", "item_id": "snapshot-item-1"},
            },
            {
                "command_id": "cmd-snapshot-retry",
                "command_type": "snapshot.compaction.run",
                "owner": "snapshot_materialization_owner",
                "status": "retry_wait",
                "not_before_at": "2000-01-01 00:00:00",
                "payload": {"job_id": "job-1", "item_id": "snapshot-item-2"},
                "result": {"status": "waiting_prerequisite"},
            },
        ],
    )

    queue = metrics["snapshot_full_materialization_queue"]
    contract = metrics["legacy_materialization_write_contract"]
    assert queue["report_available"] is True
    assert queue["item_count"] == 0
    assert queue["command_count"] == 2
    assert queue["owner"] == "snapshot_materialization_owner"
    assert queue["scope"] == "background_snapshot_compaction"
    assert queue["manual_handoff_blocking"] is False
    assert queue["background_maintenance_pending"] is False
    assert queue["command_status_counts"] == {"queued": 1, "retry_wait": 1}
    assert queue["queued_count"] == 2
    assert queue["retryable_count"] == 1
    assert queue["ready_retry_count"] == 1
    assert queue["waiting_prerequisite_count"] == 1
    assert contract["normal_path_write_count"] == 0


def test_workflow_service_metrics_marks_healthy_snapshot_compaction_as_background_maintenance() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd-snapshot-queued",
                "command_type": "snapshot.compaction.run",
                "owner": "snapshot_materialization_owner",
                "status": "queued",
                "payload": {"job_id": "job-1", "item_id": "snapshot-item-1"},
            },
            {
                "command_id": "cmd-snapshot-running",
                "command_type": "snapshot.compaction.run",
                "owner": "snapshot_materialization_owner",
                "status": "running",
                "payload": {"job_id": "job-1", "item_id": "snapshot-item-2"},
            },
        ],
    )

    queue = metrics["snapshot_full_materialization_queue"]
    assert queue["report_available"] is True
    assert queue["backlog_count"] == 2
    assert queue["unhealthy_count"] == 0
    assert queue["background_maintenance_pending"] is True
    assert queue["healthy_background_backlog"] is True
    assert queue["manual_handoff_blocking"] is False
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]}.isdisjoint(
        {
            "snapshot_full_materialization_retry_backlog",
            "snapshot_full_materialization_stale_running",
        }
    )


def test_workflow_service_metrics_exposes_provider_search_retry_queue_guardrails() -> None:
    metrics = build_workflow_service_metrics(
        materialization_items=[
            {
                "item_id": "provider-search-retry-terminal",
                "item_kind": "provider_search_retry",
                "status": "failed",
                "phase": "terminal",
                "last_error": "provider_zero_results_after_retry",
                "metadata": {
                    "provider_retry_item": {
                        "provider_retry_type": "harvest_people_search_zero_result_retry",
                        "provider": "harvest_profile_search",
                        "query": "Gemini",
                        "employment_status": "current",
                    }
                },
            },
            {
                "item_id": "provider-search-retry-ready",
                "item_kind": "provider_search_retry",
                "status": "failed_retryable",
                "phase": "retry_wait",
                "not_before_at": "2000-01-01 00:00:00",
                "last_error": "provider temporary timeout",
                "metadata": {
                    "provider_retry_item": {
                        "provider_retry_type": "harvest_people_search_zero_result_retry",
                        "provider": "harvest_profile_search",
                        "query": "Veo",
                        "employment_status": "current",
                    }
                },
            },
            {
                "item_id": "provider-search-retry-stale",
                "item_kind": "provider_search_retry",
                "status": "running",
                "phase": "applying",
                "lease_expires_at": "2000-01-01 00:00:00",
            },
            {
                "item_id": "snapshot-full",
                "item_kind": "snapshot_full_materialization",
                "status": "failed_retryable",
            },
        ],
    )

    queue = metrics["provider_search_retry_queue"]
    assert metrics["report_available"] is True
    assert queue["report_available"] is True
    assert queue["item_count"] == 3
    assert queue["terminal_failed_count"] == 1
    assert queue["retryable_count"] == 1
    assert queue["ready_retry_count"] == 1
    assert queue["stale_running_count"] == 1
    assert queue["backlog_count"] == 2
    assert queue["retry_backlog_present"] is True
    assert queue["stale_running_present"] is True
    assert queue["terminal_failure_present"] is True
    assert queue["provider_retry_type_counts"]["harvest_people_search_zero_result_retry"] == 2
    assert queue["provider_counts"]["harvest_profile_search"] == 2
    assert queue["query_samples"][0]["query"] == "Gemini"
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "provider_search_retry_backlog",
        "provider_search_retry_stale_running",
        "provider_search_retry_terminal_failure",
    }


def test_workflow_service_metrics_exposes_provider_anomaly_breakdown() -> None:
    metrics = build_workflow_service_metrics(
        materialization_items=[
            {
                "item_id": "search-seed-degraded",
                "item_kind": "search_seed_discovery_query",
                "status": "completed",
                "metadata": {
                    "query_summary": {
                        "query": "Gemini",
                        "mode": "harvest_profile_search",
                        "employment_status": "current",
                        "status": "degraded",
                        "fallback_reason": "scaled_harvest_profile_search_returned_no_rows",
                        "degraded_reason": "provider_reported_variable_or_unreliable_page_coverage_after_probe",
                        "zero_result_retry": {"result": {"retry_count": 1, "exhausted": False}},
                        "chunked_scale_fallback": {
                            "single_page_retry_count": 2,
                            "empty_page_ranges": [{"start_page": 2, "pages": 1}],
                        },
                    }
                },
            },
            {
                "item_id": "search-seed-zero-accepted",
                "item_kind": "search_seed_discovery_query",
                "status": "completed",
                "metadata": {
                    "query_summary": {
                        "query": "Infra",
                        "mode": "harvest_profile_search",
                        "employment_status": "current",
                        "status": "completed",
                        "zero_result_accepted": True,
                        "zero_result_retry": {"result": {"retry_count": 2, "exhausted": True}},
                    }
                },
            },
            {
                "item_id": "provider-search-retry-terminal",
                "item_kind": "provider_search_retry",
                "status": "failed",
                "last_error": "provider_zero_results_after_retry",
                "metadata": {
                    "provider_retry_item": {
                        "provider_retry_type": "harvest_people_search_zero_result_retry",
                        "provider": "harvest_profile_search",
                        "query": "Health",
                        "employment_status": "current",
                        "zero_result_retry": {"result": {"retry_count": 2, "exhausted": True}},
                    }
                },
            },
        ],
    )

    anomalies = metrics["provider_anomalies"]
    assert anomalies["report_available"] is True
    assert anomalies["query_summary_count"] == 3
    assert anomalies["zero_result_retry_count"] == 5
    assert anomalies["zero_result_retry_exhausted_count"] == 2
    assert anomalies["zero_result_accepted_count"] == 1
    assert anomalies["empty_scale_count"] == 1
    assert anomalies["probe_total_drift_count"] == 1
    assert anomalies["chunked_scale_fallback_count"] == 1
    assert anomalies["empty_page_range_count"] == 1
    assert anomalies["single_page_retry_count"] == 2
    assert anomalies["provider_counts"]["harvest_profile_search"] == 3
    assert anomalies["degraded_reason_counts"]["provider_reported_variable_or_unreliable_page_coverage_after_probe"] == 1
    assert anomalies["samples"][0]["query"] == "Gemini"


def test_workflow_service_metrics_reads_provider_anomaly_summary_inputs_without_legacy_items() -> None:
    metrics = build_workflow_service_metrics(
        provider_anomaly_query_summaries=[
            {
                "query": "Gemini",
                "mode": "harvest_profile_search",
                "employment_status": "current",
                "status": "degraded",
                "fallback_reason": "scaled_harvest_profile_search_returned_no_rows",
                "degraded_reason": "provider_reported_variable_or_unreliable_page_coverage_after_probe",
                "zero_result_retry": {
                    "page_chunks": [
                        {"retry_count": 2, "exhausted": True},
                        {"retry_count": 0, "exhausted": False},
                        {"retry_count": 2, "exhausted": True},
                    ]
                },
                "chunked_scale_fallback": {
                    "single_page_retry_count": 2,
                    "empty_page_ranges": [{"start_page": 3, "pages": 1}],
                },
            },
            {
                "query": "Gemini",
                "mode": "harvest_profile_search",
                "employment_status": "former",
                "status": "completed",
            },
        ],
    )

    anomalies = metrics["provider_anomalies"]
    assert anomalies["report_available"] is True
    assert anomalies["query_summary_count"] == 2
    assert anomalies["anomaly_count"] == 10
    assert anomalies["zero_result_retry_count"] == 4
    assert anomalies["zero_result_retry_exhausted_count"] == 1
    assert anomalies["empty_scale_count"] == 1
    assert anomalies["probe_total_drift_count"] == 1
    assert anomalies["empty_page_range_count"] == 1
    assert anomalies["single_page_retry_count"] == 2


def test_workflow_service_metrics_reads_legacy_provider_summary_metadata() -> None:
    metrics = build_workflow_service_metrics(
        materialization_items=[
            {
                "item_id": "legacy-search-seed-zero-accepted",
                "item_kind": "search_seed_discovery_query",
                "status": "completed",
                "metadata": {
                    "summary": {
                        "query": "Infra",
                        "mode": "harvest_profile_search",
                        "employment_status": "current",
                        "status": "completed",
                        "zero_result_accepted": True,
                        "zero_result_retry": {"probe": {"retry_count": 2, "exhausted": True}},
                    }
                },
            }
        ],
    )

    anomalies = metrics["provider_anomalies"]
    assert anomalies["report_available"] is True
    assert anomalies["query_summary_count"] == 1
    assert anomalies["zero_result_retry_count"] == 2
    assert anomalies["zero_result_retry_exhausted_count"] == 1
    assert anomalies["zero_result_accepted_count"] == 1


def test_workflow_service_metrics_zero_retry_count_does_not_fall_back_to_attempts_when_zero() -> None:
    metrics = build_workflow_service_metrics(
        materialization_items=[
            {
                "item_id": "search-seed-normal-probe",
                "item_kind": "search_seed_discovery_query",
                "status": "completed",
                "metadata": {
                    "query_summary": {
                        "query": "Infra",
                        "mode": "harvest_profile_search",
                        "status": "completed",
                        "zero_result_retry": {
                            "probe": {"attempts": 2, "retry_count": 0, "exhausted": False},
                            "result": {"attempts": 2, "retry_count": 0, "exhausted": False},
                        },
                    }
                },
            }
        ],
    )

    anomalies = metrics["provider_anomalies"]
    assert anomalies["report_available"] is True
    assert anomalies["zero_result_retry_count"] == 0
    assert anomalies["anomaly_count"] == 0


def test_workflow_service_metrics_exposes_search_seed_discovery_queue_guardrails() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 91,
                "status": "completed",
                "lane_id": "search_planner",
                "metadata": {"recovery_kind": "search_seed_discovery"},
                "output": {"summary": {"query": "OpenAI Agent site:linkedin.com/in"}},
            },
            {
                "worker_id": 92,
                "status": "completed",
                "lane_id": "search_planner",
                "metadata": {"recovery_kind": "search_seed_discovery"},
                "output": {"summary": {"query": "OpenAI ChatGPT site:linkedin.com/in"}},
            },
        ],
        materialization_items=[
            {
                "item_id": "search-seed-owned",
                "item_kind": "search_seed_discovery_query",
                "status": "running",
                "phase": "provider_owned",
                "source_worker_ids": [7, 91],
                "updated_at": "2099-01-01 00:00:00",
                "metadata": {
                    "query": "OpenAI Agent site:linkedin.com/in",
                    "employment_status": "current",
                    "provider_name": "dataforseo_google_organic",
                    "worker_id": 7,
                },
            },
            {
                "item_id": "search-seed-ownerless",
                "item_kind": "search_seed_discovery_query",
                "status": "running",
                "phase": "provider_owned",
                "updated_at": "2000-01-01 00:00:00",
                "metadata": {
                    "query": "OpenAI Whisper site:linkedin.com/in",
                    "employment_status": "former",
                    "provider_name": "dataforseo_google_organic",
                },
            },
            {
                "item_id": "provider-search-retry-terminal",
                "item_kind": "provider_search_retry",
                "status": "failed",
                "metadata": {
                    "provider_retry_item": {
                        "owner_item_id": "search-seed-exhausted",
                        "provider_retry_type": "harvest_people_search_zero_result_retry",
                    }
                },
            },
            {
                "item_id": "search-seed-retry",
                "item_kind": "search_seed_discovery_query",
                "status": "failed_retryable",
                "phase": "retry_wait",
                "not_before_at": "2000-01-01 00:00:00",
                "metadata": {
                    "query": "OpenAI ChatGPT site:linkedin.com/in",
                    "employment_status": "current",
                    "provider_name": "harvest_profile_search",
                },
            },
            {
                "item_id": "local-apply-owned",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "phase": "completed",
                "source_worker_ids": [91],
                "metadata": {"worker_kind": "search_seed"},
            },
            {
                "item_id": "search-seed-exhausted",
                "item_kind": "search_seed_discovery_query",
                "status": "exhausted",
                "phase": "exhausted",
                "metadata": {
                    "query": "OpenAI Health",
                    "employment_status": "current",
                    "provider_name": "harvest_profile_search",
                },
            },
        ],
    )

    queue = metrics["search_seed_discovery_queue"]
    assert queue["report_available"] is True
    assert queue["item_count"] == 4
    assert queue["completed_search_seed_worker_count"] == 2
    assert queue["discovery_worker_without_item_count"] == 1
    assert queue["discovery_worker_without_item_present"] is True
    assert queue["discovery_worker_without_local_apply_count"] == 1
    assert queue["discovery_worker_without_local_apply_present"] is True
    assert queue["discovery_worker_owner_gap_count"] == 1
    assert queue["discovery_worker_owner_gap_worker_ids"] == [92]
    assert queue["provider_owned_count"] == 2
    assert queue["retry_wait_count"] == 1
    assert queue["ready_retry_count"] == 1
    assert queue["exhausted_count"] == 1
    assert queue["exhausted_without_provider_retry_count"] == 0
    assert queue["item_without_worker_owner_count"] == 1
    assert queue["item_without_worker_owner_present"] is True
    assert queue["stale_provider_owned_count"] == 1
    assert queue["stale_provider_owned_present"] is True
    assert queue["provider_counts"]["dataforseo_google_organic"] == 2
    assert queue["provider_counts"]["harvest_profile_search"] == 2
    assert queue["employment_scope_counts"]["current"] == 3
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "search_seed_discovery_owner_missing",
        "search_seed_discovery_worker_owner_gap",
        "search_seed_discovery_retry_backlog",
        "search_seed_discovery_stale_provider_owned",
    }


def test_workflow_service_metrics_counts_command_owned_search_seed_discovery_queue() -> None:
    metrics = build_workflow_service_metrics(
        workflow_commands=[
            {
                "command_id": "cmd-search-seed-queued",
                "command_type": "linkedin.discovery_query.run",
                "owner": "linkedin_acquisition_owner",
                "status": "queued",
                "payload": {
                    "job_id": "job-1",
                    "item_id": "search-seed-item-1",
                    "query": "OpenAI Agent site:linkedin.com/in",
                    "employment_status": "current",
                    "materialization_metadata": {"provider_name": "harvest_profile_search"},
                },
            },
            {
                "command_id": "cmd-search-seed-running",
                "command_type": "linkedin.discovery_query.run",
                "owner": "linkedin_acquisition_owner",
                "status": "running",
                "lease_expires_at": "2099-01-01 00:00:00",
                "payload": {
                    "job_id": "job-1",
                    "item_id": "search-seed-item-2",
                    "query": "OpenAI ChatGPT site:linkedin.com/in",
                    "employment_status": "former",
                    "materialization_metadata": {"provider_name": "dataforseo_google_organic"},
                },
            },
        ],
    )

    queue = metrics["search_seed_discovery_queue"]
    contract = metrics["legacy_materialization_write_contract"]
    assert queue["report_available"] is True
    assert queue["item_count"] == 0
    assert queue["command_count"] == 2
    assert queue["command_status_counts"] == {"queued": 1, "running": 1}
    assert queue["queued_count"] == 1
    assert queue["provider_owned_count"] == 1
    assert queue["backlog_count"] == 2
    assert queue["query_samples"][0]["workflow_command_id"] == "cmd-search-seed-queued"
    assert contract["normal_path_write_count"] == 0


def test_workflow_service_metrics_exposes_remote_provider_event_lag_without_backlog() -> None:
    metrics = build_workflow_service_metrics(
        job_events=[
            {
                "created_at": "2026-04-30T16:01:57+00:00",
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [901],
                    "event": {
                        "run_id": "run-webhook-first",
                        "dataset_id": "dataset-webhook-first",
                    },
                    "event_metrics": {
                        "source": "provider_webhook",
                        "remote_to_local_event_lag_ms": 1000,
                    },
                },
            },
            {
                "created_at": "2026-04-30T16:05:23+00:00",
                "stage": "remote_provider_event",
                "status": "received_late",
                "payload": {
                    "target_worker_ids": [901],
                    "event": {
                        "run_id": "run-webhook-first",
                        "dataset_id": "dataset-webhook-first",
                    },
                    "event_metrics": {
                        "source": "local_provider_event_watcher",
                        "remote_to_local_event_lag_ms": 207000,
                    },
                },
            },
        ],
    )

    remote_events = metrics["remote_provider_events"]
    assert metrics["report_available"] is True
    assert remote_events["report_available"] is True
    assert remote_events["event_count"] == 2
    assert remote_events["received_count"] == 1
    assert remote_events["late_duplicate_count"] == 1
    assert remote_events["duplicate_event_count"] == 1
    assert remote_events["status_counts"] == {"received": 1, "received_late": 1}
    assert remote_events["source_counts"] == {
        "local_provider_event_watcher": 1,
        "provider_webhook": 1,
    }
    assert remote_events["remote_to_local_event_lag_ms"]["max"] == 207000.0
    assert remote_events["actionable_remote_to_local_event_lag_ms"]["max"] == 1000.0
    assert remote_events["late_duplicate_remote_to_local_event_lag_ms"]["max"] == 207000.0
    assert remote_events["slow_lag_count"] == 0
    assert remote_events["late_duplicate_slow_lag_count"] == 1
    assert remote_events["lag_violation"] is False
    assert remote_events["samples"][1]["source"] == "local_provider_event_watcher"
    assert "remote_provider_event_lag" not in {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]}


def test_board_visible_projection_infers_delta_visibility_from_full_snapshot_serving() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "phase": "current_snapshot_serving",
            "current_snapshot_id": "snapshot-current",
            "served_snapshot_id": "snapshot-current",
            "serving_projection_phase": "current_snapshot_serving",
            "served_candidate_count": 312,
            "delta_profile_required_count": 12,
            "delta_profile_fetched_count": 12,
            "delta_profile_materialized_count": 0,
            "delta_profile_board_visible_count": 0,
        },
    )

    board_visible = metrics["board_visible_projection"]
    assert board_visible["full_snapshot_serving"] is True
    assert board_visible["board_visible_inferred_from_full_snapshot"] is True
    assert board_visible["delta_profile_board_visible_count"] == 12
    assert board_visible["fetched_to_board_visible_lag_count"] == 0
    assert board_visible["materialization_lag_violation"] is False


def test_board_visible_projection_uses_full_snapshot_serving_as_stronger_visibility_proof() -> None:
    metrics = build_workflow_service_metrics(
        result_view_lifecycle={
            "phase": "current_snapshot_serving",
            "current_snapshot_id": "snapshot-current",
            "served_snapshot_id": "snapshot-current",
            "serving_projection_phase": "current_snapshot_serving",
            "served_candidate_count": 597,
            "delta_profile_required_count": 297,
            "delta_profile_fetched_count": 150,
            "delta_profile_materialized_count": 125,
            "delta_profile_board_visible_count": 125,
        },
    )

    board_visible = metrics["board_visible_projection"]
    assert board_visible["full_snapshot_serving"] is True
    assert board_visible["board_visible_inferred_from_full_snapshot"] is True
    assert board_visible["delta_profile_board_visible_count"] == 150
    assert board_visible["fetched_to_board_visible_lag_count"] == 0
    assert board_visible["materialization_lag_violation"] is False


def test_worker_out_of_order_completion_uses_remote_provider_completion_time() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 10,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::slow",
                "created_at": "2026-04-27 00:00:00",
                "updated_at": "2026-04-27 00:00:45",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
            {
                "worker_id": 11,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::fast",
                "created_at": "2026-04-27 00:00:01",
                "updated_at": "2026-04-27 00:00:45",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
            },
        ],
        job_events=[
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [10],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:40+00:00",
                    },
                },
            },
            {
                "stage": "remote_provider_event",
                "status": "received",
                "payload": {
                    "target_worker_ids": [11],
                    "event_metrics": {
                        "remote_completed_at": "2026-04-27T00:00:10+00:00",
                    },
                },
            },
        ],
    )

    out_of_order = metrics["worker_timeline"]["out_of_order_completion"]
    assert out_of_order["profile_batch_inversion_count"] == 1
    assert out_of_order["profile_batch_observed"] is True
    assert out_of_order["samples"][0]["earlier_completion_source"] == "remote_provider_event"
    assert out_of_order["samples"][0]["later_completion_source"] == "remote_provider_event"


def test_worker_out_of_order_completion_uses_scripted_remote_ready_time_before_terminal_marker() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 20,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::scripted-slow-anchor",
                "created_at": "2026-05-03T00:00:00+00:00",
                "updated_at": "2026-05-03T00:01:00+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "provider_mode": "scripted",
                    "scripted_remote_wait_after_submit": True,
                    "scripted_remote_ready_epoch_ms": 1_777_766_440_000,
                },
            },
            {
                "worker_id": 21,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::scripted-fast-later",
                "created_at": "2026-05-03T00:00:05+00:00",
                "updated_at": "2026-05-03T00:01:00+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "provider_mode": "scripted",
                    "scripted_remote_wait_after_submit": True,
                    "scripted_remote_ready_epoch_ms": 1_777_766_410_000,
                },
            },
        ],
    )

    out_of_order = metrics["worker_timeline"]["out_of_order_completion"]
    assert out_of_order["profile_batch_inversion_count"] == 1
    assert out_of_order["profile_batch_observed"] is True
    assert out_of_order["samples"][0]["earlier_worker_id"] == 20
    assert out_of_order["samples"][0]["later_worker_id"] == 21
    assert out_of_order["samples"][0]["earlier_completion_source"] == "scripted_remote_ready"
    assert out_of_order["samples"][0]["later_completion_source"] == "scripted_remote_ready"
    assert out_of_order["samples"][0]["completion_lead_ms"] == 30000.0
    timeline_sample = metrics["worker_timeline"]["timeline_sample"]
    assert timeline_sample[0]["scripted_remote_ready_at"] == "2026-05-03T00:00:40+00:00"


def test_worker_out_of_order_completion_uses_provider_start_before_recovery_span_start() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 40,
                "span_id": 400,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::provider-slow",
                "created_at": "2026-05-05T00:00:00+00:00",
                "updated_at": "2026-05-05T00:00:11+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "provider_mode": "scripted",
                    "scripted_remote_wait_after_submit": True,
                    "remote_wait_started_at": "2026-05-05T00:00:00+00:00",
                    "scripted_remote_ready_epoch_ms": 1_777_939_210_000,
                },
            },
            {
                "worker_id": 41,
                "span_id": 401,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::provider-fast-later",
                "created_at": "2026-05-05T00:00:04+00:00",
                "updated_at": "2026-05-05T00:00:08+00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "provider_mode": "scripted",
                    "scripted_remote_wait_after_submit": True,
                    "remote_wait_started_at": "2026-05-05T00:00:04+00:00",
                    "scripted_remote_ready_epoch_ms": 1_777_939_207_000,
                },
            },
        ],
        trace_spans=[
            {
                "span_id": 400,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-05T00:00:10+00:00",
                "completed_at": "2026-05-05T00:00:11+00:00",
            },
            {
                "span_id": 401,
                "lane_id": "enrichment_specialist",
                "started_at": "2026-05-05T00:00:07+00:00",
                "completed_at": "2026-05-05T00:00:08+00:00",
            },
        ],
    )

    out_of_order = metrics["worker_timeline"]["out_of_order_completion"]
    assert out_of_order["profile_batch_inversion_count"] == 1
    assert out_of_order["samples"][0]["earlier_worker_id"] == 40
    assert out_of_order["samples"][0]["later_worker_id"] == 41
    assert out_of_order["samples"][0]["start_gap_ms"] == 4000.0
    timeline_sample = metrics["worker_timeline"]["timeline_sample"]
    timeline_by_worker = {row["worker_id"]: row for row in timeline_sample}
    assert timeline_sample[0]["worker_id"] == 40
    assert timeline_by_worker[40]["provider_started_at"] == "2026-05-05T00:00:00+00:00"


def test_worker_out_of_order_completion_uses_worker_id_order_when_start_times_match() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 30,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::same-second-slow",
                "created_at": "2026-05-03 15:06:57",
                "updated_at": "2026-05-03 15:07:15",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "provider_mode": "scripted",
                    "scripted_remote_wait_after_submit": True,
                    "scripted_remote_ready_epoch_ms": 1_777_820_029_398,
                },
            },
            {
                "worker_id": 31,
                "status": "completed",
                "lane_id": "enrichment_specialist",
                "worker_key": "harvest_profile_batch::same-second-fast",
                "created_at": "2026-05-03 15:06:57",
                "updated_at": "2026-05-03 15:07:06",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "checkpoint": {
                    "provider_mode": "scripted",
                    "scripted_remote_wait_after_submit": True,
                    "scripted_remote_ready_epoch_ms": 1_777_820_018_438,
                },
            },
        ],
    )

    out_of_order = metrics["worker_timeline"]["out_of_order_completion"]
    assert out_of_order["profile_batch_inversion_count"] == 1
    assert out_of_order["samples"][0]["earlier_worker_id"] == 30
    assert out_of_order["samples"][0]["later_worker_id"] == 31
    assert out_of_order["samples"][0]["start_gap_ms"] == 0.0


def test_workflow_service_metrics_exposes_local_apply_backlog_guardrail() -> None:
    metrics = build_workflow_service_metrics(
        workers=[
            {
                "worker_id": 77,
                "status": "completed",
                "updated_at": "2000-01-01 00:00:00",
                "metadata": {"recovery_kind": "harvest_profile_batch"},
                "output": {
                    "inline_incremental_apply": {
                        "worker_kind": "harvest_prefetch",
                        "snapshot_id": "20260430T160000",
                        "applied_at": "2000-01-01 00:00:00",
                        "apply_status": "applied",
                    }
                },
            },
            {
                "worker_id": 78,
                "status": "completed",
                "metadata": {"recovery_kind": "search_seed_discovery"},
                "output": {
                    "inline_incremental_apply": {
                        "worker_kind": "search_seed",
                        "snapshot_id": "20260430T160000",
                        "applied_at": "2000-01-01 00:00:00",
                        "apply_status": "applied",
                    },
                    "inline_incremental_ingest": {
                        "worker_kind": "search_seed",
                        "snapshot_id": "20260430T160000",
                    },
                },
            },
        ],
        materialization_items=[
            {
                "item_id": "local-apply-closure-retry",
                "item_kind": "local_apply_closure",
                "status": "failed_retryable",
                "phase": "retry_wait",
                "not_before_at": "2000-01-01 00:00:00",
                "last_error": "profile prefetch temporarily unavailable",
            },
            {
                "item_id": "local-apply-closure-stale",
                "item_kind": "local_apply_closure",
                "status": "running",
                "phase": "applying",
                "lease_expires_at": "2000-01-01 00:00:00",
            },
            {
                "item_id": "local-apply-closure-reawakened",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "phase": "applied",
                "metadata": {
                    "failure_reason": "waiting_prerequisite_candidate_documents",
                    "reawakened_by": "search_seed_candidate_documents_projection",
                },
            },
            {
                "item_id": "local-apply-closure-fallback-completed",
                "item_kind": "local_apply_closure",
                "status": "completed",
                "phase": "applied",
                "metadata": {
                    "failure_reason": "waiting_prerequisite_candidate_documents",
                },
            },
        ],
    )

    backlog = metrics["local_apply_backlog"]
    assert metrics["report_available"] is True
    assert backlog["report_available"] is True
    assert backlog["applied_not_ingested_count"] == 1
    assert backlog["stale_applied_not_ingested_count"] == 1
    assert backlog["stale_applied_not_ingested_present"] is True
    assert backlog["by_worker_kind"]["harvest_prefetch"] == 1
    assert backlog["by_recovery_kind"]["harvest_profile_batch"] == 1
    assert backlog["closure_item_count"] == 4
    assert backlog["closure_backlog_count"] == 2
    assert backlog["closure_retryable_count"] == 1
    assert backlog["closure_ready_retry_count"] == 1
    assert backlog["closure_stale_running_count"] == 1
    assert backlog["waiting_prerequisite_history_count"] == 2
    assert backlog["waiting_prerequisite_completed_count"] == 2
    assert backlog["waiting_prerequisite_reawakened_count"] == 1
    assert backlog["waiting_prerequisite_fallback_completed_count"] == 1
    assert backlog["waiting_prerequisite_fallback_completed_present"] is True
    assert backlog["closure_error_samples"] == ["profile prefetch temporarily unavailable"]
    assert backlog["sample"][0]["worker_id"] == 77
    assert {item["kind"] for item in metrics["bottlenecks"]["top_bottlenecks"]} >= {
        "local_apply_backlog",
        "local_apply_closure_retry_backlog",
        "local_apply_closure_stale_running",
    }
