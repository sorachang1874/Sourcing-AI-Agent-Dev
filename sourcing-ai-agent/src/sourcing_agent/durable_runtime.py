from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha1
from typing import Any

TERMINAL_COMMAND_STATUSES = {"succeeded", "failed_terminal", "cancelled", "superseded"}
TERMINAL_WORKFLOW_STATUSES = {"completed", "failed", "cancelled", "superseded"}
ACQUISITION_RUN_CREATE_COMMAND_TYPE = "acquisition.run.create"
ACQUISITION_RUN_CREATE_OWNER = "acquisition_run_writer"
ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE = "acquisition.intent.resolve"
ACQUISITION_INTENT_RESOLVE_OWNER = "acquisition_planner"
ACQUISITION_PLAN_BUILD_COMMAND_TYPE = "acquisition.plan.build"
ACQUISITION_PLAN_BUILD_OWNER = "acquisition_planner"
ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE = "acquisition.plan_review.request"
ACQUISITION_PLAN_REVIEW_REQUEST_OWNER = "acquisition_planner"
ACQUISITION_PLAN_COMMIT_COMMAND_TYPE = "acquisition.plan.commit"
ACQUISITION_PLAN_COMMIT_OWNER = "acquisition_planner"
ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE = "acquisition.probe.submit"
ACQUISITION_PROBE_COLLECT_COMMAND_TYPE = "acquisition.probe.collect"
ACQUISITION_PROBE_OWNER = "acquisition_probe_owner"
ACQUISITION_SCALE_PLAN_COMMAND_TYPE = "acquisition.scale.plan"
ACQUISITION_SCALE_PLAN_OWNER = "acquisition_scale_planner"
LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE = "linkedin.profile_refill.submit_batch"
LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER = "linkedin_profile_owner"
LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE = "linkedin.profile_fetch.activity.run"
LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE = "linkedin.profile_fetch.provider.fetch"
LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER = "linkedin_profile_activity_owner"
LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE = "linkedin.profile_terminal.admit"
LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE = "linkedin.profile_url_terminal.record"
LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER = "linkedin_profile_owner"
LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE = "linkedin.discovery_query.run"
LINKEDIN_DISCOVERY_QUERY_RUN_OWNER = "linkedin_acquisition_owner"
LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE = "linkedin.local_profile_delta.apply"
LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_OWNER = "profile_local_apply_owner"
PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE = "projection.board_visible_patch.publish"
PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_OWNER = "board_visible_projection_owner"
PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE = "projection.profile_admission.apply"
PROJECTION_PROFILE_ADMISSION_APPLY_OWNER = "serving_projection_owner"
PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE = "projection.facet_layering.build"
PROJECTION_FACET_LAYERING_BUILD_OWNER = "projection_facet_layering_owner"
PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE = "projection.run_scope.finalize"
PROJECTION_RUN_SCOPE_FINALIZE_OWNER = "serving_projection_owner"
PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE = "projection.person_search_index.build"
PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER = "projection_index_owner"
COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE = "collection.authoritative.merge"
COLLECTION_AUTHORITATIVE_MERGE_OWNER = "collection_writer_owner"
SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE = "snapshot.compaction.run"
SNAPSHOT_COMPACTION_RUN_OWNER = "snapshot_materialization_owner"
CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE = "crm.public_web.queue_batch"
CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER = "crm_public_web_owner"
CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE = "crm.public_web.search.submit"
CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE = "crm.public_web.search.poll_fetch"
CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE = "crm.public_web.documents.fetch"
CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE = "crm.public_web.evidence.adjudicate"
CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE = "crm.public_web.model_safe.finalize"
CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE = "crm.public_web.signals.materialize"
CRM_PUBLIC_WEB_PHASE_OWNER = CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER
CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE = "crm.record.add_from_projection"
CRM_RECORD_UPDATE_COMMAND_TYPE = "crm.record.update"
CRM_NOTE_ADD_COMMAND_TYPE = "crm.note.add"
CRM_TASK_CREATE_COMMAND_TYPE = "crm.task.create"
CRM_WRITER_OWNER = "crm_writer"
COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE = "company.public_web.refresh"
COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE = "company.public_web.source.collect"
COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE = "company.public_web.assets.materialize"
COMPANY_PUBLIC_WEB_REFRESH_OWNER = "company_public_web_owner"
COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE = "company.logo.profile_experience.discover"
COMPANY_ASSET_OWNER = "company_asset_owner"
MEDIA_ASSET_CACHE_COMMAND_TYPE = "media.asset.cache"
MEDIA_ASSET_OWNER = "media_asset_owner"
EXCEL_INTAKE_RUN_COMMAND_TYPE = "excel.intake.run"
EXCEL_INTAKE_RUN_OWNER = "excel_intake_owner"
EXPORT_PROJECTION_GENERATE_COMMAND_TYPE = "export.projection.generate"
EXPORT_PROJECTION_GENERATE_OWNER = "projection_exporter"
EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE = "export.crm_public_web.generate"
EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER = "crm_public_web_exporter"
CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION = "crm_public_web_export_v2_stable_signal_identity"

PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE = "poll_cancel_late_result_quarantine"
PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL = "fail_closed_until_terminal"
PROVIDER_AFTER_START_CONTROL_MODE_NOT_APPLICABLE = "not_applicable"
ACTIVITY_SPINE_REQUIRED = "activity_attempt_entity_delta_required"
ACTIVITY_SPINE_ACTIVITY_BOUNDARY = "activity_run_boundary_required"
ACTIVITY_SPINE_ORCHESTRATION = "orchestration_downstream_activity_required"
ACTIVITY_SPINE_CONTROL_PLANE = "command_result_control_plane"
ACTIVITY_SPINE_LEGACY_INTERNAL = "legacy_internal_pending_activity_spine"


@dataclass(frozen=True)
class CommandTypeSpec:
    """Single source of truth for one workflow command type's static semantics.

    Every legacy per-command-type table/set in this module is derived from
    DEFAULT_COMMAND_TYPE_SPECS. Accessor behavior must stay byte-identical to
    the pre-registry literal tables (pinned by tests/test_command_type_specs.py).
    """

    command_type: str
    owner: str
    stage_id: str
    readiness_effect: str
    display_label: str
    display_category: str
    display_description: str
    activity_spine_requirement: str = ACTIVITY_SPINE_CONTROL_PLANE
    running_control_categories: tuple[str, ...] = ()
    provider_after_start_mode: str = PROVIDER_AFTER_START_CONTROL_MODE_NOT_APPLICABLE
    phase_group: str = ""
    phase_index: int | None = None
    # Metrics identifier; empty means mechanically derived from command_type
    # ("." -> "_"). Set explicitly only where history diverges from that rule.
    metric_key: str = ""
    # Product-facing Chinese label for CRM Public Web phase commands ("" elsewhere).
    product_label_zh: str = ""
    # Migration tracker step id (e.g. "W7f_crm_public_web_search_submit"; "" elsewhere).
    migration_step_id: str = ""
    # Run statuses in which this phase command is expected to execute (() elsewhere).
    expected_run_statuses: tuple[str, ...] = ()


DEFAULT_COMMAND_TYPE_SPECS: dict[str, CommandTypeSpec] = {
    ACQUISITION_RUN_CREATE_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        owner=ACQUISITION_RUN_CREATE_OWNER,
        stage_id='acquisition_run_create',
        readiness_effect='acquisition_run_requested',
        display_label='Create acquisition run',
        display_category='acquisition',
        display_description='Create the durable root for a staged acquisition operation.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
        owner=ACQUISITION_INTENT_RESOLVE_OWNER,
        stage_id='acquisition_intent_resolve',
        readiness_effect='acquisition_intent_ready_for_plan',
        display_label='Resolve acquisition intent',
        display_category='acquisition',
        display_description='Normalize the bounded acquisition request before plan building.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    ACQUISITION_PLAN_BUILD_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
        owner=ACQUISITION_PLAN_BUILD_OWNER,
        stage_id='acquisition_plan_build',
        readiness_effect='acquisition_plan_ready_for_review',
        display_label='Build acquisition plan',
        display_category='acquisition',
        display_description='Build a typed acquisition plan without provider side effects.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE,
        owner=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
        stage_id='acquisition_plan_review_request',
        readiness_effect='acquisition_plan_review_requested',
        display_label='Request plan review',
        display_category='acquisition',
        display_description='Open or reuse a human review session before execution.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    ACQUISITION_PLAN_COMMIT_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_PLAN_COMMIT_COMMAND_TYPE,
        owner=ACQUISITION_PLAN_COMMIT_OWNER,
        stage_id='acquisition_plan_commit',
        readiness_effect='acquisition_plan_committed',
        display_label='Commit reviewed plan',
        display_category='acquisition',
        display_description='Materialize the approved acquisition plan as a PG-only acquisition run.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE,
        owner=ACQUISITION_PROBE_OWNER,
        stage_id='acquisition_probe_submit',
        readiness_effect='acquisition_probe_submitted',
        display_label='Submit acquisition probe',
        display_category='acquisition',
        display_description='Submit the bounded acquisition probe.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('provider_attempt',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
    ),
    ACQUISITION_PROBE_COLLECT_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_PROBE_COLLECT_COMMAND_TYPE,
        owner=ACQUISITION_PROBE_OWNER,
        stage_id='acquisition_probe_collect',
        readiness_effect='acquisition_probe_collected',
        display_label='Collect acquisition probe',
        display_category='acquisition',
        display_description='Collect probe results and prepare scale planning.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('provider_attempt',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
    ),
    ACQUISITION_SCALE_PLAN_COMMAND_TYPE: CommandTypeSpec(
        command_type=ACQUISITION_SCALE_PLAN_COMMAND_TYPE,
        owner=ACQUISITION_SCALE_PLAN_OWNER,
        stage_id='acquisition_scale_plan',
        readiness_effect='acquisition_scale_planned',
        display_label='Plan acquisition scale',
        display_category='acquisition',
        display_description='Create discovery lanes and activity boundaries.',
        activity_spine_requirement=ACTIVITY_SPINE_ACTIVITY_BOUNDARY,
        running_control_categories=('orchestration',),
    ),
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
        owner=LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
        stage_id='stage1_candidate_discovery',
        readiness_effect='stage1_discovery_lane_submitted',
        display_label='Run LinkedIn discovery lane',
        display_category='linkedin',
        display_description='Execute one operation-native discovery lane.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('provider_attempt',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
    ),
    LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
        owner=LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_OWNER,
        stage_id='profile_fetch',
        readiness_effect='profile_refill_submitted',
        display_label='Submit profile refill batch',
        display_category='linkedin',
        display_description='Submit a bounded LinkedIn profile refill batch.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('provider_attempt',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
    ),
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
        stage_id='operation_native_profile_fetch',
        readiness_effect='profile_fetch_activity_planned',
        display_label='Plan profile fetch activity',
        display_category='linkedin',
        display_description='Resolve profile cache hits and provider fetch requirements.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
        owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
        stage_id='operation_native_profile_provider_fetch',
        readiness_effect='profile_fetch_provider_completed',
        display_label='Fetch profiles from provider',
        display_category='linkedin',
        display_description='Fetch required LinkedIn profiles through provider attempts.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('provider_attempt',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
    ),
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
        owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
        stage_id='operation_native_profile_terminal_admission',
        readiness_effect='profile_terminal_admitted',
        display_label='Admit terminal profiles',
        display_category='linkedin',
        display_description='Convert cache/provider facts into terminal profile deltas.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE,
        owner=LINKEDIN_PROFILE_URL_TERMINAL_RECORD_OWNER,
        stage_id='profile_fetch_terminal_recording',
        readiness_effect='profile_url_terminal_recorded',
        display_label='Record profile URL terminal state',
        display_category='linkedin',
        display_description='Persist terminal registry state for fetched or failed profile URLs.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE: CommandTypeSpec(
        command_type=LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
        owner=LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_OWNER,
        stage_id='local_profile_delta_apply',
        readiness_effect='local_profile_delta_applied',
        display_label='Apply local profile delta',
        display_category='linkedin',
        display_description='Apply fetched profile data into local candidate materialization.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE: CommandTypeSpec(
        command_type=PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
        owner=PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_OWNER,
        stage_id='board_visible_publication',
        readiness_effect='board_visible_patch_published',
        display_label='Publish board-visible patch',
        display_category='projection',
        display_description='Publish candidate changes to the board-visible patch stream.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE: CommandTypeSpec(
        command_type=PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
        owner=PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
        stage_id='operation_native_projection_admission',
        readiness_effect='profile_terminal_projection_admitted',
        display_label='Admit profiles to projection',
        display_category='projection',
        display_description='Apply terminal profile facts into canonical run-scope projection membership.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE: CommandTypeSpec(
        command_type=PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE,
        owner=PROJECTION_FACET_LAYERING_BUILD_OWNER,
        stage_id='post_result_layering',
        readiness_effect='projection_facet_layering_built',
        display_label='Build projection facets and layering',
        display_category='projection',
        display_description='Build canonical filter facets, layering, and related read-model metadata.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE: CommandTypeSpec(
        command_type=PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE,
        owner=PROJECTION_RUN_SCOPE_FINALIZE_OWNER,
        stage_id='serving_projection_finalization',
        readiness_effect='run_scope_projection_finalized',
        display_label='Finalize run-scope projection',
        display_category='projection',
        display_description='Publish canonical run-scope projection readiness.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE: CommandTypeSpec(
        command_type=PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
        owner=PROJECTION_PERSON_SEARCH_INDEX_BUILD_OWNER,
        stage_id='projection_index_build',
        readiness_effect='projection_person_search_index_built',
        display_label='Build projection person search index',
        display_category='projection',
        display_description='Build searchable person/profile index rows for projection readers.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE: CommandTypeSpec(
        command_type=COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
        owner=COLLECTION_AUTHORITATIVE_MERGE_OWNER,
        stage_id='collection_authoritative_merge',
        readiness_effect='collection_authoritative_projection_merged',
        display_label='Merge collection authoritative projection',
        display_category='projection',
        display_description='Merge a run-scope projection into collection-authoritative assets.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE: CommandTypeSpec(
        command_type=SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE,
        owner=SNAPSHOT_COMPACTION_RUN_OWNER,
        stage_id='snapshot_compaction',
        readiness_effect='snapshot_compaction_completed',
        display_label='Compact snapshot artifacts',
        display_category='maintenance',
        display_description='Run background snapshot/materialization compaction.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
        stage_id='crm_public_web_queue_batch',
        readiness_effect='crm_public_web_workers_queued',
        display_label='Queue CRM Public Web batch',
        display_category='crm_public_web',
        display_description='Create CRM Public Web batch/run rows and downstream phase commands.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_SEARCH_SUBMIT_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_PHASE_OWNER,
        stage_id='crm_public_web_search_submit',
        readiness_effect='crm_public_web_search_submitted',
        display_label='Submit CRM Public Web search',
        display_category='crm_public_web',
        display_description='Submit provider search for one CRM Public Web run.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('crm_public_web_phase',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
        phase_group="crm_public_web",
        phase_index=0,
        product_label_zh='提交公开搜索',
        migration_step_id='W7f_crm_public_web_search_submit',
        expected_run_statuses=('queued',),
    ),
    CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_SEARCH_POLL_FETCH_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_PHASE_OWNER,
        stage_id='crm_public_web_search_poll_fetch',
        readiness_effect='crm_public_web_search_polled_or_fetched',
        display_label='Poll CRM Public Web search',
        display_category='crm_public_web',
        display_description='Poll/fetch provider search results for one CRM Public Web run.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('crm_public_web_phase',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
        phase_group="crm_public_web",
        phase_index=1,
        product_label_zh='取回搜索结果',
        migration_step_id='W7f_crm_public_web_search_poll_fetch',
        expected_run_statuses=('search_submitted', 'searching'),
    ),
    CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_DOCUMENTS_FETCH_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_PHASE_OWNER,
        stage_id='crm_public_web_documents_fetch',
        readiness_effect='crm_public_web_documents_fetched',
        display_label='Fetch CRM Public Web documents',
        display_category='crm_public_web',
        display_description='Fetch and persist candidate Public Web documents.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('crm_public_web_phase',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL,
        phase_group="crm_public_web",
        phase_index=2,
        product_label_zh='整理页面内容',
        migration_step_id='W7f_crm_public_web_documents_fetch',
        expected_run_statuses=('entry_links_ready', 'fetching'),
    ),
    CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_EVIDENCE_ADJUDICATE_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_PHASE_OWNER,
        stage_id='crm_public_web_evidence_adjudicate',
        readiness_effect='crm_public_web_evidence_adjudicated',
        display_label='Adjudicate CRM Public Web evidence',
        display_category='crm_public_web',
        display_description='Review fetched Public Web evidence into candidate-safe signals.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('crm_public_web_phase',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL,
        phase_group="crm_public_web",
        phase_index=3,
        product_label_zh='判断候选信号',
        migration_step_id='W7f_crm_public_web_evidence_adjudicate',
        expected_run_statuses=('documents_fetched', 'analyzing'),
    ),
    CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_MODEL_SAFE_FINALIZE_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_PHASE_OWNER,
        stage_id='crm_public_web_model_safe_finalize',
        readiness_effect='crm_public_web_model_safe_finalized',
        display_label='Finalize model-safe Public Web payload',
        display_category='crm_public_web',
        display_description='Finalize model-safe Public Web artifacts for review/export.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('crm_public_web_phase',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL,
        phase_group="crm_public_web",
        phase_index=4,
        product_label_zh='生成审核候选',
        migration_step_id='W7f_crm_public_web_model_safe_finalize',
        expected_run_statuses=('adjudication_completed',),
    ),
    CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_PUBLIC_WEB_SIGNALS_MATERIALIZE_COMMAND_TYPE,
        owner=CRM_PUBLIC_WEB_PHASE_OWNER,
        stage_id='crm_public_web_signals_materialize',
        readiness_effect='crm_public_web_signals_materialized',
        display_label='Materialize CRM Public Web signals',
        display_category='crm_public_web',
        display_description='Materialize reviewed Public Web signals into person asset/evidence rows.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('crm_public_web_phase',),
        phase_group="crm_public_web",
        phase_index=5,
        product_label_zh='保存公开信息结果',
        migration_step_id='W7f_crm_public_web_signals_materialize',
        expected_run_statuses=('analysis_completed',),
    ),
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
        owner=CRM_WRITER_OWNER,
        stage_id='crm_record_add_from_projection',
        readiness_effect='crm_record_added_from_projection',
        display_label='Add person to CRM',
        display_category='crm',
        display_description='Create or link a CRM record from canonical projection/person identity.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    CRM_RECORD_UPDATE_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_RECORD_UPDATE_COMMAND_TYPE,
        owner=CRM_WRITER_OWNER,
        stage_id='crm_record_update',
        readiness_effect='crm_record_updated',
        display_label='Update CRM record',
        display_category='crm',
        display_description='Apply a bounded CRM record or engagement update.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    CRM_NOTE_ADD_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_NOTE_ADD_COMMAND_TYPE,
        owner=CRM_WRITER_OWNER,
        stage_id='crm_note_add',
        readiness_effect='crm_note_added',
        display_label='Add CRM note',
        display_category='crm',
        display_description='Append a CRM note through the CRM writer owner.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    CRM_TASK_CREATE_COMMAND_TYPE: CommandTypeSpec(
        command_type=CRM_TASK_CREATE_COMMAND_TYPE,
        owner=CRM_WRITER_OWNER,
        stage_id='crm_task_create',
        readiness_effect='crm_task_created',
        display_label='Create CRM task',
        display_category='crm',
        display_description='Create a queryable CRM follow-up task with audit event evidence.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE: CommandTypeSpec(
        command_type=COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
        owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
        stage_id='company_public_web_refresh',
        readiness_effect='company_public_web_refreshed',
        display_label='Refresh company Public Web assets',
        display_category='company_assets',
        display_description='Plan company-level Public Web source collection and asset materialization.',
        activity_spine_requirement=ACTIVITY_SPINE_ORCHESTRATION,
        running_control_categories=('orchestration',),
    ),
    COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE: CommandTypeSpec(
        command_type=COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE,
        owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
        stage_id='company_public_web_source_collect',
        readiness_effect='company_public_web_sources_collected',
        display_label='Collect company Public Web sources',
        display_category='company_assets',
        display_description='Collect company-level Public Web source rows and model-safe artifacts.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('provider_attempt',),
        provider_after_start_mode=PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
        phase_group="company_public_web",
        phase_index=0,
    ),
    COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE: CommandTypeSpec(
        command_type=COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE,
        owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
        stage_id='company_public_web_assets_materialize',
        readiness_effect='company_public_web_assets_materialized',
        display_label='Materialize company Public Web assets',
        display_category='company_assets',
        display_description='Sync collected company Public Web rows into CompanyAsset and CompanyEvidence.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
        phase_group="company_public_web",
        phase_index=1,
    ),
    COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE: CommandTypeSpec(
        command_type=COMPANY_LOGO_PROFILE_EXPERIENCE_DISCOVER_COMMAND_TYPE,
        owner=COMPANY_ASSET_OWNER,
        stage_id='company_logo_profile_experience_discover',
        readiness_effect='company_logo_profile_evidence_planned',
        display_label='Discover profile company logo',
        display_category='company_assets',
        display_description='Read one fresh profile work-experience logo candidate and plan stable media caching.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    MEDIA_ASSET_CACHE_COMMAND_TYPE: CommandTypeSpec(
        command_type=MEDIA_ASSET_CACHE_COMMAND_TYPE,
        owner=MEDIA_ASSET_OWNER,
        stage_id='media_asset_cache',
        readiness_effect='media_asset_cached',
        display_label='Cache media asset',
        display_category='media_assets',
        display_description='Fetch or store stable person/company media assets.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('domain_mutation',),
    ),
    EXCEL_INTAKE_RUN_COMMAND_TYPE: CommandTypeSpec(
        command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
        owner=EXCEL_INTAKE_RUN_OWNER,
        stage_id='excel_intake',
        readiness_effect='excel_intake_started',
        display_label='Run Excel intake',
        display_category='excel',
        display_description='Process an Excel intake job through the durable command owner.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('local_thread',),
    ),
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE: CommandTypeSpec(
        command_type=EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
        owner=EXPORT_PROJECTION_GENERATE_OWNER,
        stage_id='projection_export',
        readiness_effect='projection_export_generated',
        display_label='Export projection candidates',
        display_category='export',
        display_description='Generate a projection candidate export artifact.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('export_artifact',),
        metric_key='projection_export_generate',
    ),
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE: CommandTypeSpec(
        command_type=EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
        owner=EXPORT_CRM_PUBLIC_WEB_GENERATE_OWNER,
        stage_id='crm_public_web_export',
        readiness_effect='crm_public_web_export_generated',
        display_label='Export CRM Public Web signals',
        display_category='export',
        display_description='Generate a CRM Public Web review/export artifact.',
        activity_spine_requirement=ACTIVITY_SPINE_REQUIRED,
        running_control_categories=('export_artifact',),
        metric_key='crm_public_web_export_generate',
    ),
}


def _command_types_with_running_control_category(category: str) -> set[str]:
    return {
        command_type
        for command_type, spec in DEFAULT_COMMAND_TYPE_SPECS.items()
        if category in spec.running_control_categories
    }


def _command_types_with_activity_spine_requirement(requirement: str) -> set[str]:
    return {
        command_type
        for command_type, spec in DEFAULT_COMMAND_TYPE_SPECS.items()
        if spec.activity_spine_requirement == requirement
    }


def _phase_command_types(phase_group: str) -> tuple[str, ...]:
    members = sorted(
        (
            spec
            for spec in DEFAULT_COMMAND_TYPE_SPECS.values()
            if spec.phase_group == phase_group
        ),
        key=lambda spec: spec.phase_index if spec.phase_index is not None else 0,
    )
    return tuple(spec.command_type for spec in members)


CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES = _phase_command_types("crm_public_web")
COMPANY_PUBLIC_WEB_PHASE_COMMAND_TYPES = _phase_command_types("company_public_web")


def command_type_manifest() -> dict[str, dict]:
    """JSON-serializable manifest of every known command type's spec fields.

    Seed of the Agent tool-spec manifest: one entry per command type covering
    all CommandTypeSpec fields.
    """
    return {
        command_type: {
            "command_type": spec.command_type,
            "owner": spec.owner,
            "stage_id": spec.stage_id,
            "readiness_effect": spec.readiness_effect,
            "display_label": spec.display_label,
            "display_category": spec.display_category,
            "display_description": spec.display_description,
            "activity_spine_requirement": spec.activity_spine_requirement,
            "running_control_categories": list(spec.running_control_categories),
            "provider_after_start_mode": spec.provider_after_start_mode,
            "phase_group": spec.phase_group,
            "phase_index": spec.phase_index,
            "metric_key": spec.metric_key or spec.command_type.replace(".", "_"),
            "product_label_zh": spec.product_label_zh,
            "migration_step_id": spec.migration_step_id,
            "expected_run_statuses": list(spec.expected_run_statuses),
        }
        for command_type, spec in sorted(DEFAULT_COMMAND_TYPE_SPECS.items())
    }


@dataclass(frozen=True)
class WorkflowCommandSpec:
    command_type: str
    owner: str
    idempotency_key: str
    payload: dict[str, Any] = field(default_factory=dict)
    artifact_refs: tuple[Any, ...] = ()
    not_before_at: str = ""
    max_attempts: int = 5
    retry_policy: dict[str, Any] = field(default_factory=dict)

    @property
    def command_id_seed(self) -> str:
        return self.idempotency_key


@dataclass(frozen=True)
class CommandCausalityEnvelope:
    workflow_run_id: str
    operation_id: str
    stage_id: str
    command_type: str
    owner: str
    causal_group_id: str
    parent_command_id: str
    source_event_id: str
    source_event_type: str
    idempotency_key: str
    input_artifact_refs: tuple[Any, ...] = ()
    output_artifact_refs: tuple[Any, ...] = ()
    produced_entity_counts: dict[str, int] = field(default_factory=dict)
    no_op_reason: str = ""
    readiness_effect: str = ""
    downstream_command_ids: tuple[str, ...] = ()
    schema_version: str = "command_causality_v1"

    def to_payload(self) -> dict[str, Any]:
        return {
            "workflow_run_id": self.workflow_run_id,
            "operation_id": self.operation_id,
            "stage_id": self.stage_id,
            "command_type": self.command_type,
            "owner": self.owner,
            "causal_group_id": self.causal_group_id,
            "parent_command_id": self.parent_command_id,
            "source_event_id": self.source_event_id,
            "source_event_type": self.source_event_type,
            "idempotency_key": self.idempotency_key,
            "input_artifact_refs": list(self.input_artifact_refs),
            "output_artifact_refs": list(self.output_artifact_refs),
            "produced_entity_counts": dict(self.produced_entity_counts),
            "no_op_reason": self.no_op_reason,
            "readiness_effect": self.readiness_effect,
            "downstream_command_ids": list(self.downstream_command_ids),
            "schema_version": self.schema_version,
        }


@dataclass(frozen=True)
class RuntimeOutboxSpec:
    outbox_type: str
    idempotency_key: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReducerResult:
    status: str
    current_stage_key: str = ""
    completion_proofs: dict[str, Any] = field(default_factory=dict)
    commands: tuple[WorkflowCommandSpec, ...] = ()
    outbox: tuple[RuntimeOutboxSpec, ...] = ()
    reducer_version: str = "durable_runtime_reducer_v1"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeApplyResult:
    workflow_run_id: str
    event: dict[str, Any]
    state: dict[str, Any]
    commands: tuple[dict[str, Any], ...] = ()
    outbox: tuple[dict[str, Any], ...] = ()
    applied_event_count: int = 0


class CommandOwnerRegistry:
    def __init__(self, mapping: dict[str, str] | None = None) -> None:
        self._mapping: dict[str, str] = {}
        for command_type, owner in dict(mapping or {}).items():
            self.register(command_type, owner)

    def register(self, command_type: str, owner: str) -> None:
        normalized_type = str(command_type or "").strip()
        normalized_owner = str(owner or "").strip()
        if not normalized_type or not normalized_owner:
            raise ValueError("command_type and owner are required")
        existing_owner = self._mapping.get(normalized_type)
        if existing_owner and existing_owner != normalized_owner:
            raise ValueError(
                f"command_type {normalized_type!r} is already owned by {existing_owner!r}"
            )
        self._mapping[normalized_type] = normalized_owner

    def owner_for(self, command_type: str) -> str:
        normalized_type = str(command_type or "").strip()
        owner = self._mapping.get(normalized_type, "")
        if not owner:
            raise KeyError(f"unknown workflow command type: {normalized_type}")
        return owner

    def to_record(self) -> dict[str, str]:
        return dict(sorted(self._mapping.items()))


DEFAULT_COMMAND_OWNER_REGISTRY = CommandOwnerRegistry(
    {command_type: spec.owner for command_type, spec in DEFAULT_COMMAND_TYPE_SPECS.items()}
)


GENERIC_COMMAND_CANCEL_STATUSES = ("queued", "retry_wait")
GENERIC_COMMAND_RETRY_STATUSES = ("failed_terminal", "cancelled")
GENERIC_COMMAND_RESUME_STATUSES = ("retry_wait",)
OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES = ("claimed", "running")
WORKFLOW_COMMAND_CONTROL_CONTRACT = "w11_workflow_command_control_v1"
WORKFLOW_COMMAND_OWNER_SPECIFIC_CONTROL_CONTRACT = "w11_workflow_command_owner_specific_control_v1"
PROVIDER_AFTER_START_CONTROL_CONTRACT = "w11_provider_after_start_control_v1"
PROVIDER_AFTER_START_CONTROL_STATUS_ACTIVE = "active"
RUNNING_COMMAND_REQUIRES_OWNER_SPECIFIC_CANCEL_REASON = "running_command_requires_owner_specific_cancel"
RUNNING_COMMAND_REQUIRES_OWNER_SPECIFIC_RESUME_REASON = "running_command_requires_owner_specific_resume"
WORKFLOW_COMMAND_ACTIVITY_SPINE_CONTRACT = "w11_workflow_command_activity_spine_policy_v1"


@dataclass(frozen=True)
class WorkflowCommandControlPolicy:
    command_type: str
    owner: str
    generic_cancel_statuses: tuple[str, ...] = GENERIC_COMMAND_CANCEL_STATUSES
    generic_retry_statuses: tuple[str, ...] = GENERIC_COMMAND_RETRY_STATUSES
    generic_resume_statuses: tuple[str, ...] = GENERIC_COMMAND_RESUME_STATUSES
    running_cancel_supported: bool = False
    running_cancel_statuses: tuple[str, ...] = ()
    running_cancel_owner: str = ""
    running_cancel_delegate: str = ""
    running_cancel_prerequisites: tuple[str, ...] = ()
    running_cancel_blocked_reason: str = "owner_specific_interrupt_not_implemented"
    running_cancel_upgrade_requirements: tuple[str, ...] = ()
    running_cancel_contract: str = WORKFLOW_COMMAND_OWNER_SPECIFIC_CONTROL_CONTRACT
    unsupported_running_cancel_reason: str = RUNNING_COMMAND_REQUIRES_OWNER_SPECIFIC_CANCEL_REASON
    module_state_mutated_on_running_cancel: bool = False
    running_resume_supported: bool = False
    running_resume_statuses: tuple[str, ...] = ()
    running_resume_owner: str = ""
    running_resume_delegate: str = ""
    running_resume_prerequisites: tuple[str, ...] = ()
    running_resume_blocked_reason: str = "owner_specific_resume_not_implemented"
    running_resume_upgrade_requirements: tuple[str, ...] = ()
    running_resume_contract: str = WORKFLOW_COMMAND_OWNER_SPECIFIC_CONTROL_CONTRACT
    unsupported_running_resume_reason: str = RUNNING_COMMAND_REQUIRES_OWNER_SPECIFIC_RESUME_REASON
    module_state_mutated_on_running_resume: bool = False
    provider_after_start_control_status: str = "not_applicable"
    provider_after_start_control_mode: str = "not_applicable"
    provider_after_start_control_owner: str = ""
    provider_after_start_control_blocked_reason: str = ""
    provider_after_start_control_upgrade_requirements: tuple[str, ...] = ()
    module_state_mutated_on_provider_after_start_control: bool = False
    schema_version: str = "workflow_command_control_policy_v1"

    def to_record(self) -> dict[str, Any]:
        running_control_categories = workflow_command_running_control_categories(self.command_type)
        if self.running_cancel_supported and self.running_resume_supported:
            running_control_maturity = "owner_specific_cancel_resume"
            running_control_gap_status = "closed"
        elif self.running_cancel_supported:
            running_control_maturity = "owner_specific_cancel_only"
            running_control_gap_status = "partial_resume_gap_reported"
        elif self.running_resume_supported:
            running_control_maturity = "owner_specific_resume_only"
            running_control_gap_status = "partial_cancel_gap_reported"
        else:
            running_control_maturity = "fail_closed_with_upgrade_requirements"
            running_control_gap_status = "accepted_fail_closed_pending_owner_specific_control"
        return {
            "schema_version": self.schema_version,
            "command_type": self.command_type,
            "owner": self.owner,
            "generic_control_contract": WORKFLOW_COMMAND_CONTROL_CONTRACT,
            "running_control_category": running_control_categories[0] if len(running_control_categories) == 1 else "",
            "running_control_categories": list(running_control_categories),
            "running_control_maturity": running_control_maturity,
            "running_control_gap_status": running_control_gap_status,
            "running_control_surface": "workflow_command_control_api_only",
            "generic_cancel_statuses": list(self.generic_cancel_statuses),
            "generic_retry_statuses": list(self.generic_retry_statuses),
            "generic_resume_statuses": list(self.generic_resume_statuses),
            "running_cancel_supported": self.running_cancel_supported,
            "running_cancel_statuses": list(self.running_cancel_statuses),
            "running_cancel_owner": self.running_cancel_owner,
            "running_cancel_delegate": self.running_cancel_delegate,
            "running_cancel_prerequisites": list(self.running_cancel_prerequisites),
            "running_cancel_blocked_reason": self.running_cancel_blocked_reason,
            "running_cancel_upgrade_requirements": list(self.running_cancel_upgrade_requirements),
            "running_cancel_contract": self.running_cancel_contract,
            "unsupported_running_cancel_reason": self.unsupported_running_cancel_reason,
            "module_state_mutated_on_running_cancel": self.module_state_mutated_on_running_cancel,
            "running_resume_supported": self.running_resume_supported,
            "running_resume_statuses": list(self.running_resume_statuses),
            "running_resume_owner": self.running_resume_owner,
            "running_resume_delegate": self.running_resume_delegate,
            "running_resume_prerequisites": list(self.running_resume_prerequisites),
            "running_resume_blocked_reason": self.running_resume_blocked_reason,
            "running_resume_upgrade_requirements": list(self.running_resume_upgrade_requirements),
            "running_resume_contract": self.running_resume_contract,
            "unsupported_running_resume_reason": self.unsupported_running_resume_reason,
            "module_state_mutated_on_running_resume": self.module_state_mutated_on_running_resume,
            "provider_after_start_control_contract": PROVIDER_AFTER_START_CONTROL_CONTRACT,
            "provider_after_start_control_status": self.provider_after_start_control_status,
            "provider_after_start_control_mode": self.provider_after_start_control_mode,
            "provider_after_start_control_owner": self.provider_after_start_control_owner,
            "provider_after_start_control_blocked_reason": self.provider_after_start_control_blocked_reason,
            "provider_after_start_control_upgrade_requirements": list(
                self.provider_after_start_control_upgrade_requirements
            ),
            "module_state_mutated_on_provider_after_start_control": (
                self.module_state_mutated_on_provider_after_start_control
            ),
            "control_source_of_truth": "durable_runtime.workflow_command_control_policy",
            "agent_callable_surface": "workflow_command_control_api",
            "fallback_status": "fail_closed",
        }


@dataclass(frozen=True)
class WorkflowCommandControlState:
    command_type: str
    owner: str
    command_status: str
    can_cancel: bool = False
    can_retry: bool = False
    can_resume: bool = False
    cancel_mode: str = "unsupported"
    retry_mode: str = "unsupported"
    resume_mode: str = "unsupported"
    allowed_actions: tuple[str, ...] = ()
    disabled_reasons: dict[str, str] = field(default_factory=dict)
    running_cancel_supported: bool = False
    running_cancel_delegate: str = ""
    running_cancel_prerequisites: tuple[str, ...] = ()
    running_resume_supported: bool = False
    running_resume_delegate: str = ""
    running_resume_prerequisites: tuple[str, ...] = ()
    module_state_mutated_on_cancel: bool = False
    module_state_mutated_on_resume: bool = False
    schema_version: str = "workflow_command_control_state_v1"

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "command_type": self.command_type,
            "owner": self.owner,
            "command_status": self.command_status,
            "can_cancel": self.can_cancel,
            "can_retry": self.can_retry,
            "can_resume": self.can_resume,
            "cancel_mode": self.cancel_mode,
            "retry_mode": self.retry_mode,
            "resume_mode": self.resume_mode,
            "allowed_actions": list(self.allowed_actions),
            "disabled_reasons": dict(self.disabled_reasons),
            "running_cancel_supported": self.running_cancel_supported,
            "running_cancel_delegate": self.running_cancel_delegate,
            "running_cancel_prerequisites": list(self.running_cancel_prerequisites),
            "running_resume_supported": self.running_resume_supported,
            "running_resume_delegate": self.running_resume_delegate,
            "running_resume_prerequisites": list(self.running_resume_prerequisites),
            "module_state_mutated_on_cancel": self.module_state_mutated_on_cancel,
            "module_state_mutated_on_resume": self.module_state_mutated_on_resume,
            "control_source_of_truth": "durable_runtime.workflow_command_control_state",
            "policy_source_of_truth": "durable_runtime.workflow_command_control_policy",
            "fallback_status": "fail_closed",
        }


@dataclass(frozen=True)
class WorkflowCommandDisplayContract:
    command_type: str
    owner: str
    display_label: str
    display_category: str
    description: str = ""
    source_of_truth: str = "durable_runtime.workflow_command_display_contract"
    fallback_status: str = "fail_closed"
    schema_version: str = "workflow_command_display_contract_v1"

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "command_type": self.command_type,
            "owner": self.owner,
            "display_label": self.display_label,
            "display_category": self.display_category,
            "description": self.description,
            "source_of_truth": self.source_of_truth,
            "fallback_status": self.fallback_status,
        }


@dataclass(frozen=True)
class WorkflowCommandActivitySpinePolicy:
    command_type: str
    owner: str
    requirement: str
    must_write_activity_run: bool = False
    must_write_activity_attempt: bool = False
    must_write_entity_delta: bool = False
    downstream_activity_required: bool = False
    agent_callable: bool = False
    activity_table: str = "workflow_activity_runs"
    attempt_table: str = "workflow_activity_attempts"
    entity_delta_table: str = "workflow_entity_deltas"
    fallback_status: str = "fail_closed"
    migration_status: str = ""
    deletion_condition: str = ""
    schema_version: str = WORKFLOW_COMMAND_ACTIVITY_SPINE_CONTRACT

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "command_type": self.command_type,
            "owner": self.owner,
            "requirement": self.requirement,
            "must_write_activity_run": self.must_write_activity_run,
            "must_write_activity_attempt": self.must_write_activity_attempt,
            "must_write_entity_delta": self.must_write_entity_delta,
            "downstream_activity_required": self.downstream_activity_required,
            "agent_callable": self.agent_callable,
            "activity_table": self.activity_table,
            "attempt_table": self.attempt_table,
            "entity_delta_table": self.entity_delta_table,
            "source_of_truth": "durable_runtime.workflow_command_activity_spine_policy",
            "agent_callable_surface": "operation_command_activity_api",
            "fallback_status": self.fallback_status,
            "migration_status": self.migration_status,
            "deletion_condition": self.deletion_condition,
        }


_ACTIVITY_SPINE_REQUIRED_COMMAND_TYPES = _command_types_with_activity_spine_requirement(
    ACTIVITY_SPINE_REQUIRED
)

_ACTIVITY_SPINE_ACTIVITY_BOUNDARY_COMMAND_TYPES = _command_types_with_activity_spine_requirement(
    ACTIVITY_SPINE_ACTIVITY_BOUNDARY
)

_ACTIVITY_SPINE_ORCHESTRATION_COMMAND_TYPES = _command_types_with_activity_spine_requirement(
    ACTIVITY_SPINE_ORCHESTRATION
)

_ACTIVITY_SPINE_LEGACY_INTERNAL_COMMAND_TYPES = _command_types_with_activity_spine_requirement(
    ACTIVITY_SPINE_LEGACY_INTERNAL
)

_PROVIDER_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES = _command_types_with_running_control_category(
    "provider_attempt"
)

PROVIDER_ATTEMPT_COMMAND_TYPES = tuple(sorted(_PROVIDER_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES))

_PROVIDER_AFTER_START_POLL_CANCEL_QUARANTINE_COMMAND_TYPES = {
    command_type
    for command_type, spec in DEFAULT_COMMAND_TYPE_SPECS.items()
    if spec.provider_after_start_mode == PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE
}

_PROVIDER_AFTER_START_FAIL_CLOSED_TERMINAL_COMMAND_TYPES = {
    command_type
    for command_type, spec in DEFAULT_COMMAND_TYPE_SPECS.items()
    if spec.provider_after_start_mode == PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL
}

_ORCHESTRATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES = _command_types_with_running_control_category(
    "orchestration"
)

ORCHESTRATION_COMMAND_TYPES = tuple(sorted(_ORCHESTRATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES))

_DOMAIN_MUTATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES = _command_types_with_running_control_category(
    "domain_mutation"
)

DOMAIN_MUTATION_COMMAND_TYPES = tuple(sorted(_DOMAIN_MUTATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES))

CRM_WRITER_COMMAND_TYPES = (
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
    CRM_RECORD_UPDATE_COMMAND_TYPE,
    CRM_NOTE_ADD_COMMAND_TYPE,
    CRM_TASK_CREATE_COMMAND_TYPE,
)

_EXPORT_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES = _command_types_with_running_control_category(
    "export_artifact"
)

_LOCAL_THREAD_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES = _command_types_with_running_control_category(
    "local_thread"
)

_ARTIFACT_OR_THREAD_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES = _command_types_with_running_control_category(
    "artifact_or_thread_blocked"
)

_RUNNING_CONTROL_POLICY_CATEGORY_SETS = {
    "crm_public_web_phase": _command_types_with_running_control_category("crm_public_web_phase"),
    "provider_attempt": _PROVIDER_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES,
    "orchestration": _ORCHESTRATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES,
    "domain_mutation": _DOMAIN_MUTATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES,
    "export_artifact": _EXPORT_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES,
    "local_thread": _LOCAL_THREAD_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES,
    "artifact_or_thread_blocked": _ARTIFACT_OR_THREAD_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES,
}


def workflow_command_running_control_categories(command_type: str) -> tuple[str, ...]:
    normalized_type = str(command_type or "").strip()
    return tuple(
        category
        for category, command_types in _RUNNING_CONTROL_POLICY_CATEGORY_SETS.items()
        if normalized_type in command_types
    )

_PROVIDER_RUNNING_CANCEL_UPGRADE_REQUIREMENTS = (
    "owner_specific_provider_interrupt_or_poll_stop",
    "attempt_terminal_event_before_command_cancel",
    "idempotent_resume_or_retry_after_cancel",
)

_ORCHESTRATION_RUNNING_CANCEL_UPGRADE_REQUIREMENTS = (
    "finish_current_reducer_step_or_plan_compensating_command",
    "no_domain_side_effect_before_running_cancel",
    "operation_command_terminal_sync",
)

_DOMAIN_MUTATION_RUNNING_CANCEL_UPGRADE_REQUIREMENTS = (
    "owner_checkpoint_before_mutation_or_terminal_retry",
    "compensating_command_for_partial_mutation",
    "activity_attempt_entity_delta_cancel_evidence",
    "operation_command_terminal_sync",
)

_ARTIFACT_OR_THREAD_RUNNING_CANCEL_UPGRADE_REQUIREMENTS = (
    "durable_cancel_requested_marker",
    "owner_polling_checkpoint_before_publish_or_thread_terminal",
    "partial_artifact_cleanup_or_no_publish",
    "activity_attempt_entity_delta_cancel_evidence",
    "operation_command_terminal_sync",
)

_PROVIDER_RUNNING_RESUME_UPGRADE_REQUIREMENTS = (
    "owner_specific_provider_poll_or_attempt_reconciliation",
    "idempotent_attempt_reentry",
    "activity_attempt_entity_delta_resume_evidence",
    "operation_command_terminal_sync",
)

def _provider_after_start_control_policy_kwargs(
    *,
    command_type: str = "",
    owner: str,
) -> dict[str, Any]:
    normalized_type = str(command_type or "").strip()
    if normalized_type in _PROVIDER_AFTER_START_POLL_CANCEL_QUARANTINE_COMMAND_TYPES:
        return {
            "provider_after_start_control_status": PROVIDER_AFTER_START_CONTROL_STATUS_ACTIVE,
            "provider_after_start_control_mode": PROVIDER_AFTER_START_CONTROL_MODE_POLL_CANCEL_QUARANTINE,
            "provider_after_start_control_owner": str(owner or "").strip(),
            "provider_after_start_control_blocked_reason": "",
            "provider_after_start_control_upgrade_requirements": (),
            "module_state_mutated_on_provider_after_start_control": False,
        }
    if normalized_type in _PROVIDER_AFTER_START_FAIL_CLOSED_TERMINAL_COMMAND_TYPES:
        return {
            "provider_after_start_control_status": PROVIDER_AFTER_START_CONTROL_STATUS_ACTIVE,
            "provider_after_start_control_mode": PROVIDER_AFTER_START_CONTROL_MODE_FAIL_CLOSED_TERMINAL,
            "provider_after_start_control_owner": str(owner or "").strip(),
            "provider_after_start_control_blocked_reason": "after_start_control_deliberately_fail_closed_until_terminal",
            "provider_after_start_control_upgrade_requirements": (),
            "module_state_mutated_on_provider_after_start_control": False,
        }
    return {
        "provider_after_start_control_status": "not_applicable",
        "provider_after_start_control_mode": "not_applicable",
        "provider_after_start_control_owner": str(owner or "").strip(),
        "provider_after_start_control_blocked_reason": "",
        "provider_after_start_control_upgrade_requirements": (),
        "module_state_mutated_on_provider_after_start_control": False,
    }

_ORCHESTRATION_RUNNING_RESUME_UPGRADE_REQUIREMENTS = (
    "deterministic_reducer_reentry_checkpoint",
    "no_duplicate_downstream_command_planning",
    "operation_command_terminal_sync",
)

_DOMAIN_MUTATION_RUNNING_RESUME_UPGRADE_REQUIREMENTS = (
    "owner_checkpoint_before_mutation_reentry",
    "idempotent_domain_effect_replay",
    "activity_attempt_entity_delta_resume_evidence",
    "operation_command_terminal_sync",
)

_ARTIFACT_OR_THREAD_RUNNING_RESUME_UPGRADE_REQUIREMENTS = (
    "durable_resume_requested_marker",
    "owner_checkpoint_before_publish_or_thread_reentry",
    "activity_attempt_entity_delta_resume_evidence",
    "operation_command_terminal_sync",
)


def _running_resume_policy_kwargs_for_command_type(command_type: str) -> dict[str, Any]:
    normalized_type = str(command_type or "").strip()
    if normalized_type in _PROVIDER_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return {
            "running_resume_blocked_reason": "external_provider_attempt_requires_owner_specific_resume",
            "running_resume_upgrade_requirements": _PROVIDER_RUNNING_RESUME_UPGRADE_REQUIREMENTS,
        }
    if normalized_type in _ORCHESTRATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return {
            "running_resume_blocked_reason": "orchestration_command_must_finish_or_reenter_reducer",
            "running_resume_upgrade_requirements": _ORCHESTRATION_RUNNING_RESUME_UPGRADE_REQUIREMENTS,
        }
    if normalized_type in _DOMAIN_MUTATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return {
            "running_resume_blocked_reason": "domain_mutation_must_finish_or_resume_from_owner_checkpoint",
            "running_resume_upgrade_requirements": _DOMAIN_MUTATION_RUNNING_RESUME_UPGRADE_REQUIREMENTS,
        }
    if normalized_type in _EXPORT_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES:
        return {
            "running_resume_blocked_reason": "artifact_command_requires_owner_checkpoint_resume",
            "running_resume_upgrade_requirements": _ARTIFACT_OR_THREAD_RUNNING_RESUME_UPGRADE_REQUIREMENTS,
        }
    if normalized_type in _LOCAL_THREAD_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES:
        return {
            "running_resume_blocked_reason": "local_thread_command_requires_owner_checkpoint_resume",
            "running_resume_upgrade_requirements": _ARTIFACT_OR_THREAD_RUNNING_RESUME_UPGRADE_REQUIREMENTS,
        }
    if normalized_type in _ARTIFACT_OR_THREAD_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return {
            "running_resume_blocked_reason": "owner_has_no_safe_inflight_resume",
            "running_resume_upgrade_requirements": _ARTIFACT_OR_THREAD_RUNNING_RESUME_UPGRADE_REQUIREMENTS,
        }
    return {
        "running_resume_blocked_reason": "owner_specific_resume_requires_policy_registration",
        "running_resume_upgrade_requirements": ("register_owner_specific_resume_policy",),
    }


def default_stage_id_for_command_type(command_type: str) -> str:
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(str(command_type or "").strip())
    return spec.stage_id if spec else ""


def default_readiness_effect_for_command_type(command_type: str) -> str:
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(str(command_type or "").strip())
    return spec.readiness_effect if spec else ""


def workflow_command_metric_key(command_type: str) -> str:
    """Metrics identifier for a command type.

    Defaults to the command type with dots replaced by underscores; a spec may
    pin an explicit metric_key where the historical key diverges.
    """
    normalized = str(command_type or "").strip()
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(normalized)
    if spec is not None and spec.metric_key:
        return spec.metric_key
    return normalized.replace(".", "_")


def workflow_command_product_label_zh(command_type: str) -> str:
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(str(command_type or "").strip())
    return spec.product_label_zh if spec else ""


def workflow_command_migration_step_id(command_type: str) -> str:
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(str(command_type or "").strip())
    return spec.migration_step_id if spec else ""


def workflow_command_expected_run_statuses(command_type: str) -> tuple[str, ...]:
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(str(command_type or "").strip())
    return spec.expected_run_statuses if spec else ()


def workflow_command_display_contract(command_type: str, owner: str = "") -> WorkflowCommandDisplayContract:
    normalized_type = str(command_type or "").strip()
    spec = DEFAULT_COMMAND_TYPE_SPECS.get(normalized_type)
    normalized_owner = str(owner or (spec.owner if spec else "")).strip()
    return WorkflowCommandDisplayContract(
        command_type=normalized_type,
        owner=normalized_owner,
        display_label=spec.display_label if spec else "",
        display_category=spec.display_category if spec else "",
        description=spec.display_description if spec else "",
    )


def workflow_command_control_policy(command_type: str, owner: str = "") -> WorkflowCommandControlPolicy:
    normalized_type = str(command_type or "").strip()
    normalized_owner = str(owner or DEFAULT_COMMAND_OWNER_REGISTRY.to_record().get(normalized_type, "")).strip()
    if normalized_type in {
        ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE,
        ACQUISITION_PLAN_BUILD_COMMAND_TYPE,
        COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    }:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=normalized_owner,
            running_cancel_delegate="workflow_orchestrator.cancel_orchestration_before_downstream",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=normalized_owner,
            running_resume_delegate="workflow_orchestrator.resume_orchestration_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_downstream_command_keys",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == ACQUISITION_PLAN_COMMIT_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=ACQUISITION_PLAN_COMMIT_OWNER,
            running_cancel_delegate="workflow_orchestrator.cancel_acquisition_plan_commit_before_probe",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_probe_command_planned",
                "command_lease_expired_or_force",
                "acquisition_run_cancel_checkpoint",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=ACQUISITION_PLAN_COMMIT_OWNER,
            running_resume_delegate="workflow_orchestrator.resume_orchestration_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_downstream_command_keys",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == ACQUISITION_SCALE_PLAN_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=ACQUISITION_SCALE_PLAN_OWNER,
            running_cancel_delegate="workflow_orchestrator.cancel_acquisition_scale_plan_before_discovery",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_discovery_command_planned",
                "no_activity_attempt_started",
                "command_lease_expired_or_force",
                "activity_lane_cancel_checkpoint",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=ACQUISITION_SCALE_PLAN_OWNER,
            running_resume_delegate="workflow_orchestrator.resume_orchestration_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_downstream_command_keys",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            running_cancel_delegate="crm_public_web_owner.cancel_queue_batch_before_phase_commands",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_phase_command_planned",
                "command_lease_expired_or_force",
                "batch_run_cancel_checkpoint",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=CRM_PUBLIC_WEB_QUEUE_BATCH_OWNER,
            running_resume_delegate="workflow_orchestrator.resume_orchestration_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_downstream_command_keys",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
            running_cancel_delegate="linkedin_profile_activity_owner.cancel_before_cache_lookup_attempt",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_activity_attempt_started",
                "no_profile_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
            running_resume_delegate="workflow_domain_owner.resume_domain_mutation_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_domain_effect_key",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_owner == CRM_PUBLIC_WEB_PHASE_OWNER and normalized_type in set(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES):
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            **_provider_after_start_control_policy_kwargs(command_type=normalized_type, owner=normalized_owner),
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=CRM_PUBLIC_WEB_PHASE_OWNER,
            running_cancel_delegate="crm_public_web_owner.cancel_crm_public_web_run",
            running_cancel_prerequisites=("payload.run_id", "crm_public_web_run_exists"),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=CRM_PUBLIC_WEB_PHASE_OWNER,
            running_resume_delegate="crm_public_web_owner.resume_crm_public_web_phase_command",
            running_resume_prerequisites=(
                "payload.run_id",
                "crm_public_web_run_exists",
                "command_lease_expired_or_force",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == COMPANY_PUBLIC_WEB_SOURCE_COLLECT_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            **_provider_after_start_control_policy_kwargs(command_type=normalized_type, owner=normalized_owner),
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            running_cancel_delegate="company_public_web_owner.cancel_or_poll_stop_source_collect",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "provider_activity_attempt_absent_or_poll_cancel_quarantine",
                "no_provider_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            running_resume_delegate="company_public_web_owner.resume_source_collect",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "canonical_asset_sync_deferred",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == COMPANY_PUBLIC_WEB_ASSETS_MATERIALIZE_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            running_cancel_delegate="company_public_web_owner.cancel_assets_materialize_before_sync",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_company_asset_materialize_activity_attempt_started",
                "no_company_asset_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
            running_resume_delegate="company_public_web_owner.resume_assets_materialize",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_company_asset_sync_key",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
            running_cancel_delegate="workflow_orchestrator.cancel_acquisition_plan_review_request",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_approved_plan_review_session",
                "plan_review_session_cancel_checkpoint",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
            running_resume_delegate="workflow_orchestrator.resume_orchestration_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_downstream_command_keys",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type == MEDIA_ASSET_CACHE_COMMAND_TYPE:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=MEDIA_ASSET_OWNER,
            running_cancel_delegate="media_asset_owner.cancel_before_fetch_upload_attempt",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_media_asset_activity_attempt_started",
                "no_media_asset_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=MEDIA_ASSET_OWNER,
            running_resume_delegate="media_asset_owner.resume_media_asset_cache",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_asset_id_or_content_hash",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type in set(CRM_WRITER_COMMAND_TYPES):
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=CRM_WRITER_OWNER,
            running_cancel_delegate="crm_writer.cancel_before_mutation_attempt",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_crm_writer_activity_attempt_started",
                "no_crm_writer_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=CRM_WRITER_OWNER,
            running_resume_delegate="crm_writer.resume_crm_writer_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_crm_write_key",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type in _EXPORT_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES:
        delegate = (
            "projection_exporter.cancel_export_command"
            if normalized_type == EXPORT_PROJECTION_GENERATE_COMMAND_TYPE
            else "crm_public_web_exporter.cancel_export_command"
        )
        resume_delegate = (
            "projection_exporter.resume_export_command"
            if normalized_type == EXPORT_PROJECTION_GENERATE_COMMAND_TYPE
            else "crm_public_web_exporter.resume_export_command"
        )
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=normalized_owner,
            running_cancel_delegate=delegate,
            running_cancel_prerequisites=("workflow_command_status_claimed_or_running", "artifact_not_published"),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=normalized_owner,
            running_resume_delegate=resume_delegate,
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "artifact_not_published",
                "command_lease_expired_or_force",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type in _LOCAL_THREAD_RUNNING_CANCEL_SUPPORTED_COMMAND_TYPES:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=normalized_owner,
            running_cancel_delegate="excel_intake_owner.cancel_excel_intake_run_command",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "payload.job_id",
                "thread_terminal_checkpoint",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=normalized_owner,
            running_resume_delegate="excel_intake_owner.resume_excel_intake_run_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "payload.job_id",
                "command_lease_expired_or_force",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type in _ARTIFACT_OR_THREAD_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_blocked_reason="owner_has_no_safe_inflight_interrupt",
            running_cancel_upgrade_requirements=_ARTIFACT_OR_THREAD_RUNNING_CANCEL_UPGRADE_REQUIREMENTS,
            **_running_resume_policy_kwargs_for_command_type(normalized_type),
        )
    if normalized_type in _PROVIDER_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            **_provider_after_start_control_policy_kwargs(command_type=normalized_type, owner=normalized_owner),
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=normalized_owner,
            running_cancel_delegate="workflow_provider_owner.cancel_or_poll_stop_provider_attempt",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "provider_activity_attempt_absent_or_poll_cancel_quarantine",
                "no_provider_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=normalized_owner,
            running_resume_delegate="workflow_provider_owner.resume_provider_attempt_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_provider_request_key",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type in _ORCHESTRATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_blocked_reason="orchestration_command_must_finish_or_plan_compensating_command",
            running_cancel_upgrade_requirements=_ORCHESTRATION_RUNNING_CANCEL_UPGRADE_REQUIREMENTS,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=normalized_owner,
            running_resume_delegate="workflow_orchestrator.resume_orchestration_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_downstream_command_keys",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    if normalized_type in _DOMAIN_MUTATION_RUNNING_CANCEL_BLOCKED_COMMAND_TYPES:
        return WorkflowCommandControlPolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            running_cancel_supported=True,
            running_cancel_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_cancel_owner=normalized_owner,
            running_cancel_delegate="workflow_domain_owner.cancel_before_domain_mutation_attempt",
            running_cancel_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "no_domain_mutation_activity_attempt_started",
                "no_domain_mutation_entity_delta_recorded",
                "no_downstream_command_planned",
                "command_lease_expired_or_force",
            ),
            running_cancel_blocked_reason="",
            module_state_mutated_on_running_cancel=True,
            running_resume_supported=True,
            running_resume_statuses=OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES,
            running_resume_owner=normalized_owner,
            running_resume_delegate="workflow_domain_owner.resume_domain_mutation_command",
            running_resume_prerequisites=(
                "workflow_command_status_claimed_or_running",
                "command_lease_expired_or_force",
                "idempotent_domain_effect_key",
            ),
            running_resume_blocked_reason="",
            module_state_mutated_on_running_resume=True,
        )
    return WorkflowCommandControlPolicy(
        command_type=normalized_type,
        owner=normalized_owner,
        **_running_resume_policy_kwargs_for_command_type(normalized_type),
    )


def workflow_command_control_state(
    *,
    command_status: str,
    command_type: str,
    owner: str = "",
) -> WorkflowCommandControlState:
    normalized_status = str(command_status or "").strip()
    normalized_type = str(command_type or "").strip()
    normalized_owner = str(owner or DEFAULT_COMMAND_OWNER_REGISTRY.to_record().get(normalized_type, "")).strip()
    policy = workflow_command_control_policy(command_type=normalized_type, owner=normalized_owner)
    can_cancel = False
    can_retry = False
    can_resume = False
    cancel_mode = "unsupported"
    retry_mode = "unsupported"
    resume_mode = "unsupported"
    disabled_reasons: dict[str, str] = {}
    module_state_mutated_on_cancel = False
    module_state_mutated_on_resume = False

    if normalized_status in set(policy.generic_cancel_statuses):
        can_cancel = True
        cancel_mode = "generic"
    elif normalized_status in set(OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES):
        if policy.running_cancel_supported:
            can_cancel = True
            cancel_mode = "owner_specific"
            module_state_mutated_on_cancel = policy.module_state_mutated_on_running_cancel
        else:
            disabled_reasons["cancel"] = policy.unsupported_running_cancel_reason
            if policy.running_cancel_blocked_reason:
                disabled_reasons["running_cancel"] = policy.running_cancel_blocked_reason
    else:
        disabled_reasons["cancel"] = "command_status_not_cancelable"

    if normalized_status in set(policy.generic_retry_statuses):
        can_retry = True
        retry_mode = "generic"
    else:
        disabled_reasons["retry"] = "command_status_not_retryable"

    if normalized_status in set(policy.generic_resume_statuses):
        can_resume = True
        resume_mode = "generic"
    elif normalized_status in set(policy.running_resume_statuses):
        if policy.running_resume_supported:
            can_resume = True
            resume_mode = "owner_specific"
            module_state_mutated_on_resume = policy.module_state_mutated_on_running_resume
        else:
            disabled_reasons["resume"] = policy.unsupported_running_resume_reason
            if policy.running_resume_blocked_reason:
                disabled_reasons["running_resume"] = policy.running_resume_blocked_reason
    elif normalized_status in set(OWNER_SPECIFIC_RUNNING_CANCEL_STATUSES):
        disabled_reasons["resume"] = policy.unsupported_running_resume_reason
        if policy.running_resume_blocked_reason:
            disabled_reasons["running_resume"] = policy.running_resume_blocked_reason
    else:
        disabled_reasons["resume"] = "command_status_not_resumable"

    allowed_actions = tuple(
        action
        for action, allowed in (
            ("cancel", can_cancel),
            ("retry", can_retry),
            ("resume", can_resume),
        )
        if allowed
    )
    return WorkflowCommandControlState(
        command_type=normalized_type,
        owner=normalized_owner,
        command_status=normalized_status,
        can_cancel=can_cancel,
        can_retry=can_retry,
        can_resume=can_resume,
        cancel_mode=cancel_mode,
        retry_mode=retry_mode,
        resume_mode=resume_mode,
        allowed_actions=allowed_actions,
        disabled_reasons=disabled_reasons,
        running_cancel_supported=policy.running_cancel_supported,
        running_cancel_delegate=policy.running_cancel_delegate,
        running_cancel_prerequisites=policy.running_cancel_prerequisites,
        running_resume_supported=policy.running_resume_supported,
        running_resume_delegate=policy.running_resume_delegate,
        running_resume_prerequisites=policy.running_resume_prerequisites,
        module_state_mutated_on_cancel=module_state_mutated_on_cancel,
        module_state_mutated_on_resume=module_state_mutated_on_resume,
    )


def workflow_command_activity_spine_policy(command_type: str, owner: str = "") -> WorkflowCommandActivitySpinePolicy:
    normalized_type = str(command_type or "").strip()
    normalized_owner = str(owner or DEFAULT_COMMAND_OWNER_REGISTRY.to_record().get(normalized_type, "")).strip()
    if normalized_type in _ACTIVITY_SPINE_REQUIRED_COMMAND_TYPES:
        return WorkflowCommandActivitySpinePolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            requirement=ACTIVITY_SPINE_REQUIRED,
            must_write_activity_run=True,
            must_write_activity_attempt=True,
            must_write_entity_delta=True,
            agent_callable=True,
            migration_status="normal_path_activity_spine_required",
            deletion_condition="do_not_replace_with_command_result_only_or_domain_table_mutation",
        )
    if normalized_type in _ACTIVITY_SPINE_ACTIVITY_BOUNDARY_COMMAND_TYPES:
        return WorkflowCommandActivitySpinePolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            requirement=ACTIVITY_SPINE_ACTIVITY_BOUNDARY,
            must_write_activity_run=True,
            downstream_activity_required=True,
            agent_callable=True,
            migration_status="plans_activity_boundary_for_downstream_owner",
            deletion_condition="downstream provider work must stay command/activity owned",
        )
    if normalized_type in _ACTIVITY_SPINE_ORCHESTRATION_COMMAND_TYPES:
        return WorkflowCommandActivitySpinePolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            requirement=ACTIVITY_SPINE_ORCHESTRATION,
            downstream_activity_required=True,
            agent_callable=True,
            migration_status="orchestration_command_only",
            deletion_condition="must plan or link to downstream command/activity before side effects execute",
        )
    if normalized_type in _ACTIVITY_SPINE_LEGACY_INTERNAL_COMMAND_TYPES:
        return WorkflowCommandActivitySpinePolicy(
            command_type=normalized_type,
            owner=normalized_owner,
            requirement=ACTIVITY_SPINE_LEGACY_INTERNAL,
            agent_callable=False,
            migration_status="legacy_workflow_internal_not_agent_normal_path",
            deletion_condition="replace or wrap with command/activity/entity-delta owner before Agent exposure",
        )
    return WorkflowCommandActivitySpinePolicy(
        command_type=normalized_type,
        owner=normalized_owner,
        requirement=ACTIVITY_SPINE_CONTROL_PLANE,
        migration_status="control_plane_command_result_only",
        deletion_condition="promote to activity spine before adding provider/domain side effects",
    )


def command_id_for(workflow_run_id: str, idempotency_key: str) -> str:
    normalized_run_id = str(workflow_run_id or "").strip()
    normalized_idempotency = str(idempotency_key or "").strip()
    return "cmd_" + sha1(f"{normalized_run_id}:{normalized_idempotency}".encode("utf-8")).hexdigest()[:24]


def command_causality_for(
    *,
    workflow_run_id: str,
    operation_id: str = "",
    stage_id: str = "",
    command_type: str,
    owner: str,
    idempotency_key: str,
    source_event: dict[str, Any] | None = None,
    command_payload: dict[str, Any] | None = None,
    artifact_refs: tuple[Any, ...] | list[Any] = (),
) -> CommandCausalityEnvelope:
    payload = dict(command_payload or {})
    existing = dict(payload.get("causality") or {})
    event_payload = dict((source_event or {}).get("payload") or {})
    normalized_run_id = str(workflow_run_id or "").strip()
    normalized_operation_id = str(operation_id or existing.get("operation_id") or "").strip()
    normalized_type = str(command_type or "").strip()
    normalized_owner = str(owner or "").strip()
    normalized_idempotency = str(idempotency_key or "").strip()
    source_event_id = str(existing.get("source_event_id") or (source_event or {}).get("event_id") or "").strip()
    source_event_type = str(
        existing.get("source_event_type") or (source_event or {}).get("event_type") or ""
    ).strip()
    parent_command_id = str(
        existing.get("parent_command_id")
        or event_payload.get("parent_command_id")
        or (source_event or {}).get("command_id")
        or ""
    ).strip()
    causal_group_id = str(
        existing.get("causal_group_id")
        or event_payload.get("causal_group_id")
        or payload.get("causal_group_id")
        or parent_command_id
        or normalized_idempotency
    ).strip()
    normalized_stage_id = str(
        stage_id
        or existing.get("stage_id")
        or event_payload.get("stage_id")
        or payload.get("stage_id")
        or default_stage_id_for_command_type(normalized_type)
        or ""
    ).strip()
    produced_counts = _normalize_produced_entity_counts(
        existing.get("produced_entity_counts")
        or event_payload.get("produced_entity_counts")
        or payload.get("produced_entity_counts")
        or {}
    )
    if not produced_counts:
        produced_counts = _infer_produced_entity_counts_from_payload(payload, command_type=normalized_type)
    no_op_reason = str(
        existing.get("no_op_reason")
        or event_payload.get("no_op_reason")
        or payload.get("no_op_reason")
        or ""
    ).strip()
    if not no_op_reason and produced_counts and sum(int(value or 0) for value in produced_counts.values()) <= 0:
        no_op_reason = _infer_no_op_reason_from_payload(payload, command_type=normalized_type)
    if not no_op_reason and not produced_counts:
        no_op_reason = _infer_no_op_reason_from_payload(payload, command_type=normalized_type)
    input_refs = existing.get("input_artifact_refs") or event_payload.get("input_artifact_refs") or list(artifact_refs or ())
    output_refs = existing.get("output_artifact_refs") or event_payload.get("output_artifact_refs") or []
    downstream_ids = existing.get("downstream_command_ids") or event_payload.get("downstream_command_ids") or []
    return CommandCausalityEnvelope(
        workflow_run_id=normalized_run_id,
        operation_id=normalized_operation_id,
        stage_id=normalized_stage_id,
        command_type=normalized_type,
        owner=normalized_owner,
        causal_group_id=causal_group_id,
        parent_command_id=parent_command_id,
        source_event_id=source_event_id,
        source_event_type=source_event_type,
        idempotency_key=normalized_idempotency,
        input_artifact_refs=tuple(input_refs or ()),
        output_artifact_refs=tuple(output_refs or ()),
        produced_entity_counts=produced_counts,
        no_op_reason=no_op_reason,
        readiness_effect=str(
            existing.get("readiness_effect")
            or event_payload.get("readiness_effect")
            or payload.get("readiness_effect")
            or default_readiness_effect_for_command_type(normalized_type)
            or ""
        ).strip(),
        downstream_command_ids=tuple(str(item or "").strip() for item in list(downstream_ids or []) if str(item or "").strip()),
    )


def attach_command_causality(
    payload: dict[str, Any] | None,
    *,
    causality: CommandCausalityEnvelope,
) -> dict[str, Any]:
    next_payload = dict(payload or {})
    next_payload["causality"] = causality.to_payload()
    return next_payload


def legacy_job_workflow_run_id(job_id: str, *, workflow_type: str = "linkedin_acquisition") -> str:
    """Stable migration-era WorkflowRun id for legacy job-owned execution.

    Long term, workflow ids should be created when an OperationRun is accepted.
    During the W2 cutover we still need a deterministic id for legacy job-owned
    profile scheduler work so command/event rows can be idempotent.
    """

    normalized_job_id = str(job_id or "").strip()
    normalized_type = str(workflow_type or "linkedin_acquisition").strip() or "linkedin_acquisition"
    if not normalized_job_id:
        return ""
    digest = sha1(f"{normalized_type}:{normalized_job_id}".encode("utf-8")).hexdigest()[:24]
    return f"wf_legacy_job_{digest}"


def legacy_job_operation_id(job_id: str) -> str:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return ""
    digest = sha1(f"legacy_job_operation:{normalized_job_id}".encode("utf-8")).hexdigest()[:24]
    return f"op_legacy_job_{digest}"


def linkedin_profile_refill_submit_idempotency_key(
    *,
    job_id: str,
    snapshot_dir: str,
    profile_urls: list[str] | tuple[str, ...],
    submit_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_dir = str(snapshot_dir or "").strip()
    normalized_urls = sorted(
        {
            str(profile_url or "").strip()
            for profile_url in list(profile_urls or [])
            if str(profile_url or "").strip()
        }
    )
    if not normalized_job_id or not normalized_snapshot_dir or not normalized_urls:
        return ""
    normalized_submit_scope = str(submit_scope or "").strip()
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_dir": normalized_snapshot_dir,
                "profile_urls": normalized_urls,
                "submit_scope": normalized_submit_scope,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE}:{scope_hash}"


def linkedin_profile_url_terminal_record_idempotency_key(
    *,
    job_id: str,
    snapshot_dir: str,
    entries: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    terminal_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_dir = str(snapshot_dir or "").strip()
    normalized_entries: list[dict[str, Any]] = []
    for entry in list(entries or []):
        payload = dict(entry or {})
        profile_url = str(payload.get("profile_url") or "").strip()
        status = str(payload.get("status") or "").strip()
        if not profile_url or not status:
            continue
        normalized_entries.append(
            {
                "profile_url": profile_url,
                "status": status,
                "raw_path": str(payload.get("raw_path") or "").strip(),
                "error": str(payload.get("error") or "").strip(),
                "retryable": bool(payload.get("retryable")),
                "run_id": str(payload.get("run_id") or "").strip(),
                "dataset_id": str(payload.get("dataset_id") or "").strip(),
            }
        )
    normalized_entries.sort(
        key=lambda item: (
            str(item.get("profile_url") or ""),
            str(item.get("status") or ""),
            str(item.get("raw_path") or ""),
            str(item.get("error") or ""),
            str(item.get("run_id") or ""),
            str(item.get("dataset_id") or ""),
        )
    )
    if not normalized_job_id or not normalized_snapshot_dir or not normalized_entries:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_dir": normalized_snapshot_dir,
                "terminal_scope": str(terminal_scope or "").strip(),
                "entries": normalized_entries,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE}:{scope_hash}"


def linkedin_discovery_query_run_idempotency_key(
    *,
    job_id: str,
    snapshot_id: str,
    item_id: str,
    query: str = "",
    employment_status: str = "",
    run_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    if not normalized_job_id or not normalized_snapshot_id or not normalized_item_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_id": normalized_snapshot_id,
                "item_id": normalized_item_id,
                "query": str(query or "").strip(),
                "employment_status": str(employment_status or "").strip(),
                "run_scope": str(run_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE}:{scope_hash}"


def linkedin_local_profile_delta_apply_idempotency_key(
    *,
    job_id: str,
    snapshot_id: str,
    item_id: str,
    worker_kind: str = "",
    source_worker_ids: list[int] | tuple[int, ...] = (),
    apply_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    if not normalized_job_id or not normalized_snapshot_id or not normalized_item_id:
        return ""
    normalized_worker_ids: list[int] = []
    seen_worker_ids: set[int] = set()
    for value in list(source_worker_ids or []):
        try:
            worker_id = int(value or 0)
        except (TypeError, ValueError):
            continue
        if worker_id <= 0 or worker_id in seen_worker_ids:
            continue
        seen_worker_ids.add(worker_id)
        normalized_worker_ids.append(worker_id)
    normalized_worker_ids.sort()
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_id": normalized_snapshot_id,
                "item_id": normalized_item_id,
                "worker_kind": str(worker_kind or "").strip(),
                "source_worker_ids": normalized_worker_ids,
                "apply_scope": str(apply_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE}:{scope_hash}"


def projection_board_visible_patch_publish_idempotency_key(
    *,
    job_id: str,
    snapshot_id: str,
    item_id: str,
    candidate_ids: list[str] | tuple[str, ...] = (),
    publish_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    if not normalized_job_id or not normalized_snapshot_id or not normalized_item_id:
        return ""
    normalized_candidate_ids = sorted(
        {
            str(candidate_id or "").strip()
            for candidate_id in list(candidate_ids or [])
            if str(candidate_id or "").strip()
        }
    )
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_id": normalized_snapshot_id,
                "item_id": normalized_item_id,
                "candidate_ids": normalized_candidate_ids,
                "publish_scope": str(publish_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE}:{scope_hash}"


def projection_facet_layering_build_idempotency_key(
    *,
    job_id: str,
    snapshot_id: str,
    item_id: str,
    input_fingerprint: str = "",
    build_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    if not normalized_job_id or not normalized_snapshot_id or not normalized_item_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_id": normalized_snapshot_id,
                "item_id": normalized_item_id,
                "input_fingerprint": str(input_fingerprint or "").strip(),
                "build_scope": str(build_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE}:{scope_hash}"


def projection_person_search_index_build_idempotency_key(
    *,
    projection_id: str,
    item_id: str,
    projection_index_input_version: str,
    build_scope: str = "",
) -> str:
    normalized_projection_id = str(projection_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    normalized_input_version = str(projection_index_input_version or "").strip()
    if not normalized_projection_id or not normalized_item_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "projection_id": normalized_projection_id,
                "item_id": normalized_item_id,
                "projection_index_input_version": normalized_input_version,
                "build_scope": str(build_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE}:{scope_hash}"


def projection_run_scope_finalize_idempotency_key(
    *,
    job_id: str,
    view_id: str,
    snapshot_id: str,
    source_path: str = "",
    finalize_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_view_id = str(view_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    if not normalized_job_id or not normalized_view_id or not normalized_snapshot_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "view_id": normalized_view_id,
                "snapshot_id": normalized_snapshot_id,
                "source_path": str(source_path or "").strip(),
                "finalize_scope": str(finalize_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE}:{scope_hash}"


def snapshot_compaction_run_idempotency_key(
    *,
    job_id: str,
    snapshot_id: str,
    item_id: str,
    compaction_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    normalized_snapshot_id = str(snapshot_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    if not normalized_job_id or not normalized_snapshot_id or not normalized_item_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "snapshot_id": normalized_snapshot_id,
                "item_id": normalized_item_id,
                "compaction_scope": str(compaction_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE}:{scope_hash}"


def collection_authoritative_merge_idempotency_key(
    *,
    collection_id: str,
    source_projection_id: str,
    item_id: str,
    publication_fingerprint: str = "",
    merge_scope: str = "",
) -> str:
    normalized_collection_id = str(collection_id or "").strip()
    normalized_projection_id = str(source_projection_id or "").strip()
    normalized_item_id = str(item_id or "").strip()
    if not normalized_collection_id or not normalized_projection_id or not normalized_item_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "collection_id": normalized_collection_id,
                "source_projection_id": normalized_projection_id,
                "item_id": normalized_item_id,
                "publication_fingerprint": str(publication_fingerprint or "").strip(),
                "merge_scope": str(merge_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE}:{scope_hash}"


def crm_public_web_queue_batch_idempotency_key(
    *,
    workspace_id: str,
    batch_id: str,
    run_ids: list[str] | tuple[str, ...],
    queue_scope: str = "",
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_batch_id = str(batch_id or "").strip()
    normalized_run_ids = sorted(
        {
            str(run_id or "").strip()
            for run_id in list(run_ids or [])
            if str(run_id or "").strip()
        }
    )
    if not normalized_batch_id or not normalized_run_ids:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "batch_id": normalized_batch_id,
                "run_ids": normalized_run_ids,
                "queue_scope": str(queue_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE}:{scope_hash}"


def crm_public_web_run_phase_idempotency_key(
    *,
    workspace_id: str,
    batch_id: str,
    run_id: str,
    phase_command_type: str,
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_batch_id = str(batch_id or "").strip()
    normalized_run_id = str(run_id or "").strip()
    normalized_phase_type = str(phase_command_type or "").strip()
    if (
        not normalized_batch_id
        or not normalized_run_id
        or normalized_phase_type not in set(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES)
    ):
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "batch_id": normalized_batch_id,
                "run_id": normalized_run_id,
                "phase_command_type": normalized_phase_type,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{normalized_phase_type}:{scope_hash}"


def crm_record_add_from_projection_idempotency_key(
    *,
    workspace_id: str,
    projection_id: str,
    candidate_identity_key: str,
    pipeline_id: str = "default_sourcing",
    stage: str = "new",
    operation_scope: str = "",
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_projection_id = str(projection_id or "").strip()
    normalized_candidate_key = str(candidate_identity_key or "").strip()
    if not normalized_projection_id or not normalized_candidate_key:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "projection_id": normalized_projection_id,
                "candidate_identity_key": normalized_candidate_key,
                "pipeline_id": str(pipeline_id or "default_sourcing").strip() or "default_sourcing",
                "stage": str(stage or "new").strip() or "new",
                "operation_scope": str(operation_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE}:{scope_hash}"


def crm_record_update_idempotency_key(
    *,
    workspace_id: str,
    crm_record_id: str,
    stage: str = "",
    follow_up_status: str = "",
    quality_score: Any = None,
    comment: str = "",
    operation_scope: str = "",
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_record_id = str(crm_record_id or "").strip()
    if not normalized_record_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "stage": str(stage or "").strip(),
                "follow_up_status": str(follow_up_status or "").strip(),
                "quality_score": quality_score,
                "comment": str(comment or "").strip(),
                "operation_scope": str(operation_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{CRM_RECORD_UPDATE_COMMAND_TYPE}:{scope_hash}"


def crm_note_add_idempotency_key(
    *,
    workspace_id: str,
    crm_record_id: str,
    note: str,
    operation_scope: str = "",
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_record_id = str(crm_record_id or "").strip()
    normalized_note = str(note or "").strip()
    if not normalized_record_id or not normalized_note:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "note": normalized_note,
                "operation_scope": str(operation_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{CRM_NOTE_ADD_COMMAND_TYPE}:{scope_hash}"


def crm_task_create_idempotency_key(
    *,
    workspace_id: str,
    crm_record_id: str,
    title: str,
    due_at: str = "",
    operation_scope: str = "",
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_record_id = str(crm_record_id or "").strip()
    normalized_title = str(title or "").strip()
    if not normalized_record_id or not normalized_title:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_id": normalized_record_id,
                "title": normalized_title,
                "due_at": str(due_at or "").strip(),
                "operation_scope": str(operation_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{CRM_TASK_CREATE_COMMAND_TYPE}:{scope_hash}"


def excel_intake_run_idempotency_key(
    *,
    job_id: str,
    batch_id: str = "",
    target_company: str = "",
    prepared_contact_batch_path: str = "",
    run_scope: str = "",
) -> str:
    normalized_job_id = str(job_id or "").strip()
    if not normalized_job_id:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "job_id": normalized_job_id,
                "batch_id": str(batch_id or "").strip(),
                "target_company": str(target_company or "").strip(),
                "prepared_contact_batch_path": str(prepared_contact_batch_path or "").strip(),
                "run_scope": str(run_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{EXCEL_INTAKE_RUN_COMMAND_TYPE}:{scope_hash}"


def export_projection_generate_idempotency_key(
    *,
    projection_id: str,
    candidate_identity_keys: list[str] | tuple[str, ...] = (),
    include_llm_reviewed_unconfirmed_assertions: bool = False,
    include_crm_notes: bool = False,
    limit: int = 0,
    page_size: int = 0,
    export_scope: str = "",
) -> str:
    normalized_projection_id = str(projection_id or "").strip()
    if not normalized_projection_id:
        return ""
    normalized_keys = sorted(
        {
            str(candidate_key or "").strip()
            for candidate_key in list(candidate_identity_keys or [])
            if str(candidate_key or "").strip()
        }
    )
    scope_hash = sha1(
        json.dumps(
            {
                "projection_id": normalized_projection_id,
                "candidate_identity_keys": normalized_keys,
                "include_llm_reviewed_unconfirmed_assertions": bool(
                    include_llm_reviewed_unconfirmed_assertions
                ),
                "include_crm_notes": bool(include_crm_notes),
                "limit": max(0, int(limit or 0)),
                "page_size": max(0, int(page_size or 0)),
                "export_scope": str(export_scope or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{EXPORT_PROJECTION_GENERATE_COMMAND_TYPE}:{scope_hash}"


def export_crm_public_web_generate_idempotency_key(
    *,
    workspace_id: str,
    crm_record_ids: list[str] | tuple[str, ...] = (),
    export_mode: str = "promoted_only",
    export_contract_version: str = CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION,
    export_input_watermark_hash: str = "",
) -> str:
    normalized_workspace_id = str(workspace_id or "default").strip() or "default"
    normalized_record_ids = sorted(
        {
            str(record_id or "").strip()
            for record_id in list(crm_record_ids or [])
            if str(record_id or "").strip()
        }
    )
    if not normalized_record_ids:
        return ""
    scope_hash = sha1(
        json.dumps(
            {
                "workspace_id": normalized_workspace_id,
                "crm_record_ids": normalized_record_ids,
                "export_mode": str(export_mode or "promoted_only").strip() or "promoted_only",
                "export_contract_version": str(export_contract_version or "").strip(),
                "export_input_watermark_hash": str(export_input_watermark_hash or "").strip(),
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE}:{scope_hash}"


def summarize_workflow_command_counts(
    commands: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], dict[str, Any]]:
    active_counts: dict[str, dict[str, int]] = {}
    terminal_counts: dict[str, dict[str, int]] = {}
    for command in list(commands or []):
        owner = str(command.get("owner") or "").strip() or "unknown_owner"
        command_type = str(command.get("command_type") or "").strip() or "unknown_command"
        status = str(command.get("status") or "").strip()
        target = terminal_counts if status in TERMINAL_COMMAND_STATUSES else active_counts
        owner_counts = target.setdefault(owner, {})
        owner_counts[command_type] = int(owner_counts.get(command_type) or 0) + 1
    return active_counts, terminal_counts


def _normalize_produced_entity_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for key, raw_count in value.items():
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        try:
            count = int(raw_count or 0)
        except (TypeError, ValueError):
            continue
        normalized[normalized_key] = max(0, count)
    return normalized


_COUNT_SEARCH_NESTED_KEYS = (
    "candidate_source",
    "materialization_metadata",
    "metadata",
    "projection_payload",
    "sync_result",
    "artifact_result",
)


def _positive_int(value: Any) -> int:
    try:
        count = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(0, count)


def _count_from_key(payload: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        count = _positive_int(payload.get(key))
        if count > 0:
            return count
    for nested_key in _COUNT_SEARCH_NESTED_KEYS:
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            count = _count_from_key(nested, keys)
            if count > 0:
                return count
    return 0


def _count_from_list_key(payload: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, (list, tuple, set)):
            count = len([item for item in value if item])
            if count > 0:
                return count
    for nested_key in _COUNT_SEARCH_NESTED_KEYS:
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            count = _count_from_list_key(nested, keys)
            if count > 0:
                return count
    return 0


def _has_explicit_empty_input(payload: dict[str, Any]) -> bool:
    empty_count_keys = {
        "candidate_count",
        "profile_count",
        "profile_url_count",
        "terminal_entry_count",
        "entry_count",
        "indexed_count",
        "facet_count",
        "member_count",
        "snapshot_count",
        "query_count",
    }
    empty_list_keys = {
        "candidate_ids",
        "profile_urls",
        "entries",
        "source_worker_ids",
        "members",
        "member_ids",
    }
    for key, value in payload.items():
        if key in empty_count_keys:
            try:
                if int(value or 0) <= 0:
                    return True
            except (TypeError, ValueError):
                continue
        if key in empty_list_keys and isinstance(value, (list, tuple, set)) and not value:
            return True
        if key in _COUNT_SEARCH_NESTED_KEYS and isinstance(value, dict) and _has_explicit_empty_input(value):
            return True
    return False


def _infer_produced_entity_counts_from_payload(payload: dict[str, Any], *, command_type: str) -> dict[str, int]:
    payload_dict = dict(payload or {})
    candidates: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...]
    normalized_type = str(command_type or "").strip()
    if normalized_type == ACQUISITION_RUN_CREATE_COMMAND_TYPE:
        candidates = (
            ("acquisition_run", ("run_count",), ("job_ids", "run_ids")),
            ("query", ("query_count",), ("queries", "search_queries")),
        )
    elif normalized_type == ACQUISITION_INTENT_RESOLVE_COMMAND_TYPE:
        candidates = (
            ("acquisition_intent", ("intent_count",), ()),
            ("query", ("query_count",), ("queries", "search_queries")),
        )
    elif normalized_type == ACQUISITION_PLAN_BUILD_COMMAND_TYPE:
        candidates = (
            ("acquisition_plan", ("plan_count",), ("plan_ids",)),
            ("query", ("query_count",), ("queries", "search_queries")),
        )
    elif normalized_type == ACQUISITION_PLAN_REVIEW_REQUEST_COMMAND_TYPE:
        candidates = (
            ("plan_review", ("plan_review_count", "review_count"), ("plan_review_ids", "review_ids")),
            ("acquisition_plan", ("plan_count",), ("plan_ids",)),
        )
    elif normalized_type == ACQUISITION_PLAN_COMMIT_COMMAND_TYPE:
        candidates = (
            ("plan_review", ("plan_review_count", "review_count"), ("plan_review_ids", "review_ids")),
            ("acquisition_plan", ("plan_count",), ("plan_ids",)),
            ("acquisition_run", ("acquisition_run_count",), ("acquisition_run_id", "acquisition_run_ids")),
        )
    elif normalized_type == ACQUISITION_PROBE_SUBMIT_COMMAND_TYPE:
        candidates = (
            ("acquisition_run", ("acquisition_run_count",), ("acquisition_run_id", "acquisition_run_ids")),
            ("query", ("query_count",), ("queries", "search_queries")),
        )
    elif normalized_type == ACQUISITION_PROBE_COLLECT_COMMAND_TYPE:
        candidates = (
            ("acquisition_run", ("acquisition_run_count",), ("acquisition_run_id", "acquisition_run_ids")),
            ("probe_result", ("probe_result_count",), ("probe_result_ids",)),
        )
    elif normalized_type == ACQUISITION_SCALE_PLAN_COMMAND_TYPE:
        candidates = (
            ("acquisition_run", ("acquisition_run_count",), ("acquisition_run_id", "acquisition_run_ids")),
            ("discovery_lane", ("lane_count", "discovery_lane_count"), ("lane_ids", "queries", "search_queries")),
        )
    elif normalized_type in {
        LINKEDIN_PROFILE_REFILL_SUBMIT_BATCH_COMMAND_TYPE,
        LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
        LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    }:
        candidates = (("profile_url", ("profile_url_count", "requested_url_count"), ("profile_urls",)),)
    elif normalized_type == LINKEDIN_PROFILE_URL_TERMINAL_RECORD_COMMAND_TYPE:
        candidates = (
            ("terminal_entry", ("terminal_entry_count", "entry_count"), ("entries",)),
            ("profile_url", ("profile_url_count",), ("profile_urls",)),
        )
    elif normalized_type in {
        LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_TYPE,
        PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_TYPE,
        PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    }:
        candidates = (
            ("candidate", ("candidate_count", "member_count"), ("candidate_ids", "members", "member_ids")),
            ("profile", ("profile_count",), ("profile_urls",)),
            ("source_worker", ("source_worker_count",), ("source_worker_ids",)),
        )
    elif normalized_type == PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE:
        candidates = (
            ("candidate", ("candidate_count", "member_count"), ("candidate_ids", "members", "member_ids")),
            ("projection", ("projection_count",), ()),
        )
    elif normalized_type == COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE:
        candidates = (
            ("member", ("member_count", "candidate_count"), ("members", "member_ids", "candidate_ids")),
            ("projection", ("projection_count",), ()),
        )
    elif normalized_type == PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE:
        candidates = (
            ("indexed_person", ("indexed_count", "member_count", "candidate_count"), ("members", "member_ids", "candidate_ids")),
        )
    elif normalized_type == PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE:
        candidates = (
            ("facet", ("facet_count",), ()),
            ("candidate", ("candidate_count", "member_count"), ("candidate_ids", "members", "member_ids")),
        )
    elif normalized_type == SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE:
        candidates = (
            ("snapshot", ("snapshot_count",), ()),
            ("candidate", ("candidate_count", "member_count"), ("candidate_ids", "members", "member_ids")),
            ("source_worker", ("source_worker_count",), ("source_worker_ids",)),
        )
    elif normalized_type == LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE:
        candidates = (("query", ("query_count",), ()), ("candidate", ("candidate_count",), ("candidate_ids",)))
    elif normalized_type == CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE:
        candidates = (
            ("crm_public_web_run", ("run_count",), ("run_ids",)),
            ("crm_record", ("record_count",), ("record_ids", "crm_record_ids")),
            ("worker", ("worker_count", "planned_worker_count"), ("worker_keys",)),
        )
    elif normalized_type in set(CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES):
        candidates = (
            ("crm_public_web_run", ("run_count",), ("run_ids", "run_id")),
            ("crm_record", ("record_count",), ("record_ids", "crm_record_ids", "crm_record_id")),
            ("public_web_signal", ("signal_count", "materialized_signal_count"), ("signal_ids",)),
        )
    elif normalized_type in {
        CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
        CRM_RECORD_UPDATE_COMMAND_TYPE,
        CRM_NOTE_ADD_COMMAND_TYPE,
        CRM_TASK_CREATE_COMMAND_TYPE,
    }:
        candidates = (
            ("crm_record", ("record_count",), ("crm_record_ids", "record_ids")),
            ("candidate", ("candidate_count",), ("candidate_identity_keys", "candidate_ids")),
            ("crm_note", ("note_count",), ()),
            ("crm_task", ("task_count",), ()),
        )
    elif normalized_type == COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE:
        candidates = (
            ("company_public_web_run", ("run_count",), ("run_id", "run_ids")),
            ("company_asset", ("asset_count", "synced_asset_count"), ("company_asset_ids", "asset_ids")),
            ("company_evidence", ("evidence_count", "synced_evidence_count"), ("company_evidence_ids", "evidence_ids")),
        )
    elif normalized_type == EXCEL_INTAKE_RUN_COMMAND_TYPE:
        candidates = (
            ("excel_intake_job", ("job_count",), ("job_ids",)),
            ("excel_contact", ("row_count", "prepared_contact_count"), ("contact_ids",)),
        )
    elif normalized_type == EXPORT_PROJECTION_GENERATE_COMMAND_TYPE:
        candidates = (
            ("projection", ("projection_count",), ("projection_ids",)),
            ("candidate", ("candidate_count", "record_count"), ("candidate_identity_keys", "candidate_ids")),
        )
    elif normalized_type == EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE:
        candidates = (
            ("crm_record", ("crm_record_count", "record_count"), ("crm_record_ids", "record_ids")),
            ("public_web_signal", ("signal_count", "exported_signal_count"), ("signal_ids",)),
        )
    else:
        candidates = (
            ("candidate", ("candidate_count",), ("candidate_ids",)),
            ("profile_url", ("profile_url_count",), ("profile_urls",)),
        )
    inferred: dict[str, int] = {}
    for entity_name, count_keys, list_keys in candidates:
        count = _count_from_key(payload_dict, count_keys)
        if count <= 0:
            count = _count_from_list_key(payload_dict, list_keys)
        if count > 0:
            inferred[entity_name] = count
    if inferred:
        return inferred
    if normalized_type == PROJECTION_RUN_SCOPE_FINALIZE_COMMAND_TYPE:
        return {"projection": 1}
    if normalized_type == COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE:
        return {"collection_projection": 1}
    if normalized_type == PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE:
        return {"person_search_index": 1}
    if normalized_type == PROJECTION_FACET_LAYERING_BUILD_COMMAND_TYPE:
        return {"facet_layering": 1}
    if normalized_type == SNAPSHOT_COMPACTION_RUN_COMMAND_TYPE:
        return {"snapshot": 1}
    return inferred


def _infer_no_op_reason_from_payload(payload: dict[str, Any], *, command_type: str) -> str:
    payload_dict = dict(payload or {})
    explicit_reason = str(payload_dict.get("reason") or "").strip()
    if explicit_reason.startswith("no_"):
        return explicit_reason
    if _has_explicit_empty_input(payload_dict):
        normalized_type = str(command_type or "").strip() or "workflow_command"
        return f"{normalized_type}:empty_input"
    return ""


class DurableRuntimeWriter:
    """Reducer-owned write facade for W2 migration slices.

    Workers/callbacks should write events/results. This class is the bounded
    transaction-style owner that applies reducer output to current-state,
    typed commands, and outbox rows.
    """

    def __init__(
        self,
        store: Any,
        *,
        owner_registry: CommandOwnerRegistry = DEFAULT_COMMAND_OWNER_REGISTRY,
    ) -> None:
        self.store = store
        self.owner_registry = owner_registry

    def append_event_and_reduce(
        self,
        *,
        workflow_run_id: str,
        event_family: str,
        event_type: str,
        idempotency_key: str,
        operation_id: str = "",
        command_id: str = "",
        activity_attempt_id: str = "",
        actor: str = "",
        source: str = "",
        payload: dict[str, Any] | None = None,
        artifact_refs: list[Any] | tuple[Any, ...] | None = None,
    ) -> RuntimeApplyResult:
        event = self.store.append_workflow_event(
            workflow_run_id=workflow_run_id,
            operation_id=operation_id,
            command_id=command_id,
            activity_attempt_id=activity_attempt_id,
            event_family=event_family,
            event_type=event_type,
            idempotency_key=idempotency_key,
            actor=actor,
            source=source,
            payload=payload or {},
            artifact_refs=artifact_refs or (),
        )
        return self.reduce_and_persist(workflow_run_id=workflow_run_id, event=event)

    def reduce_and_persist(
        self,
        *,
        workflow_run_id: str,
        event: dict[str, Any] | None = None,
    ) -> RuntimeApplyResult:
        normalized_run_id = str(workflow_run_id or "").strip()
        if not normalized_run_id:
            return RuntimeApplyResult(workflow_run_id="", event={}, state={})

        current_state = self.store.get_workflow_current_state(normalized_run_id) or {}
        last_processed_sequence = int(current_state.get("last_processed_sequence_number") or 0)
        events = self.store.list_workflow_events(normalized_run_id, limit=0)
        new_events = [
            event_payload
            for event_payload in events
            if int(event_payload.get("sequence_number") or 0) > last_processed_sequence
        ]
        existing_commands = self.store.list_workflow_commands(workflow_run_id=normalized_run_id, limit=0)
        if not new_events:
            active_counts, terminal_counts = summarize_workflow_command_counts(existing_commands)
            current_active_counts = dict(current_state.get("active_command_counts") or {})
            current_terminal_counts = dict(current_state.get("terminal_command_counts") or {})
            if active_counts != current_active_counts or terminal_counts != current_terminal_counts:
                operation_id = _resolve_operation_id(current_state=current_state, events=events)
                workflow_type = _resolve_workflow_type(current_state=current_state, events=events)
                state = self.store.upsert_workflow_current_state(
                    workflow_run_id=normalized_run_id,
                    operation_id=operation_id,
                    workflow_type=workflow_type,
                    status=str(current_state.get("status") or "pending").strip() or "pending",
                    current_stage_key=str(current_state.get("current_stage_key") or "").strip(),
                    completion_proofs=dict(current_state.get("completion_proofs") or {}),
                    active_command_counts=active_counts,
                    terminal_command_counts=terminal_counts,
                    last_processed_sequence_number=last_processed_sequence,
                    reducer_version=str(current_state.get("reducer_version") or "").strip(),
                    metadata=dict(current_state.get("metadata") or {}),
                )
            else:
                state = current_state
            return RuntimeApplyResult(
                workflow_run_id=normalized_run_id,
                event=dict(event or {}),
                state=state,
                commands=tuple(existing_commands),
                outbox=(),
                applied_event_count=0,
            )

        reducer_result = reduce_workflow_events(
            current_state=current_state,
            new_events=new_events,
            existing_commands=existing_commands,
            owner_registry=self.owner_registry,
        )
        operation_id = _resolve_operation_id(current_state=current_state, events=events)
        workflow_type = _resolve_workflow_type(current_state=current_state, events=events)
        written_commands: list[dict[str, Any]] = []
        for command_spec in reducer_result.commands:
            command_operation_id = _resolve_command_operation_id(
                command_spec=command_spec,
                default_operation_id=operation_id,
            )
            written_commands.append(
                self.store.upsert_workflow_command(
                    workflow_run_id=normalized_run_id,
                    operation_id=command_operation_id,
                    command_type=command_spec.command_type,
                    owner=command_spec.owner,
                    idempotency_key=command_spec.idempotency_key,
                    payload=command_spec.payload,
                    artifact_refs=command_spec.artifact_refs,
                    not_before_at=command_spec.not_before_at,
                    max_attempts=command_spec.max_attempts,
                    retry_policy=command_spec.retry_policy,
                )
            )
        written_outbox: list[dict[str, Any]] = []
        for outbox_spec in reducer_result.outbox:
            written_outbox.append(
                self.store.enqueue_runtime_outbox(
                    workflow_run_id=normalized_run_id,
                    operation_id=operation_id,
                    outbox_type=outbox_spec.outbox_type,
                    idempotency_key=outbox_spec.idempotency_key,
                    payload=outbox_spec.payload,
                )
            )

        all_commands = self.store.list_workflow_commands(workflow_run_id=normalized_run_id, limit=0)
        active_counts, terminal_counts = summarize_workflow_command_counts(all_commands)
        max_sequence = max(int(event_payload.get("sequence_number") or 0) for event_payload in new_events)
        state = self.store.upsert_workflow_current_state(
            workflow_run_id=normalized_run_id,
            operation_id=operation_id,
            workflow_type=workflow_type,
            status=reducer_result.status,
            current_stage_key=reducer_result.current_stage_key,
            completion_proofs=reducer_result.completion_proofs,
            active_command_counts=active_counts,
            terminal_command_counts=terminal_counts,
            last_processed_sequence_number=max_sequence,
            reducer_version=reducer_result.reducer_version,
            metadata=reducer_result.metadata,
        )
        return RuntimeApplyResult(
            workflow_run_id=normalized_run_id,
            event=dict(event or {}),
            state=state,
            commands=tuple(command for command in written_commands if command),
            outbox=tuple(outbox_item for outbox_item in written_outbox if outbox_item),
            applied_event_count=len(new_events),
        )


def reduce_workflow_events(
    *,
    current_state: dict[str, Any] | None,
    new_events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    existing_commands: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    owner_registry: CommandOwnerRegistry = DEFAULT_COMMAND_OWNER_REGISTRY,
) -> ReducerResult:
    """Pure reducer skeleton for Phase W1.

    It intentionally handles only generic runtime events and one command-plan
    event. Domain reducers in W2 should compose this shape instead of workers
    inserting arbitrary commands directly.
    """

    state = dict(current_state or {})
    status = str(state.get("status") or "pending").strip() or "pending"
    current_stage_key = str(state.get("current_stage_key") or "").strip()
    completion_proofs = dict(state.get("completion_proofs") or {})
    metadata = dict(state.get("metadata") or {})
    existing_idempotency_keys = {
        str(command.get("idempotency_key") or "").strip()
        for command in list(existing_commands or [])
        if str(command.get("idempotency_key") or "").strip()
    }
    planned_commands: list[WorkflowCommandSpec] = []
    outbox: list[RuntimeOutboxSpec] = []

    for event in sorted(list(new_events or []), key=lambda item: int(item.get("sequence_number") or 0)):
        event_type = str(event.get("event_type") or "").strip()
        payload = dict(event.get("payload") or {})
        if event_type == "WorkflowStarted" and status not in TERMINAL_WORKFLOW_STATUSES:
            status = "running"
            current_stage_key = str(payload.get("stage_key") or current_stage_key or "started").strip()
            continue
        if event_type == "CompletionProofRecorded" and status not in TERMINAL_WORKFLOW_STATUSES:
            proof_key = str(payload.get("proof_key") or "").strip()
            if proof_key:
                completion_proofs[proof_key] = {
                    "status": str(payload.get("status") or "proved").strip() or "proved",
                    "event_id": str(event.get("event_id") or ""),
                    "sequence_number": int(event.get("sequence_number") or 0),
                }
            continue
        if event_type == "CommandPlanRequested" and status not in {"failed", "cancelled", "superseded"}:
            command_type = str(payload.get("command_type") or "").strip()
            idempotency_key = str(payload.get("idempotency_key") or "").strip()
            if command_type and idempotency_key and idempotency_key not in existing_idempotency_keys:
                owner = owner_registry.owner_for(command_type)
                command_payload = dict(payload.get("payload") or {})
                artifact_refs = tuple(payload.get("artifact_refs") or ())
                causality = command_causality_for(
                    workflow_run_id=str(event.get("workflow_run_id") or "").strip(),
                    operation_id=str(event.get("operation_id") or "").strip(),
                    stage_id=str(payload.get("stage_id") or "").strip(),
                    command_type=command_type,
                    owner=owner,
                    idempotency_key=idempotency_key,
                    source_event=event,
                    command_payload=command_payload,
                    artifact_refs=artifact_refs,
                )
                planned_commands.append(
                    WorkflowCommandSpec(
                        command_type=command_type,
                        owner=owner,
                        idempotency_key=idempotency_key,
                        payload=attach_command_causality(command_payload, causality=causality),
                        artifact_refs=artifact_refs,
                        not_before_at=str(payload.get("not_before_at") or "").strip(),
                        max_attempts=max(1, int(payload.get("max_attempts") or 5)),
                        retry_policy=dict(payload.get("retry_policy") or {}),
                    )
                )
                existing_idempotency_keys.add(idempotency_key)
            continue
        if event_type == "WorkflowCompleted":
            status = "completed"
            current_stage_key = str(payload.get("stage_key") or current_stage_key or "completed").strip()
            outbox.append(
                RuntimeOutboxSpec(
                    outbox_type="workflow.completed",
                    idempotency_key=f"{event.get('workflow_run_id')}:workflow.completed",
                    payload={"workflow_run_id": event.get("workflow_run_id"), "status": "completed"},
                )
            )
            continue
        if event_type == "WorkflowFailed":
            status = "failed"
            metadata["failure_reason"] = str(payload.get("reason") or "").strip()
            continue

    return ReducerResult(
        status=status,
        current_stage_key=current_stage_key,
        completion_proofs=completion_proofs,
        commands=tuple(planned_commands),
        outbox=tuple(outbox),
        metadata=metadata,
    )


def _resolve_operation_id(*, current_state: dict[str, Any], events: list[dict[str, Any]]) -> str:
    existing = str(current_state.get("operation_id") or "").strip()
    if existing:
        return existing
    for event in events:
        operation_id = str(event.get("operation_id") or "").strip()
        if operation_id:
            return operation_id
    return ""


def _resolve_command_operation_id(
    *,
    command_spec: WorkflowCommandSpec,
    default_operation_id: str = "",
) -> str:
    payload = dict(command_spec.payload or {})
    causality = dict(payload.get("causality") or {})
    return str(
        payload.get("operation_id")
        or payload.get("operation_run_id")
        or causality.get("operation_id")
        or default_operation_id
        or ""
    ).strip()


def _resolve_workflow_type(*, current_state: dict[str, Any], events: list[dict[str, Any]]) -> str:
    existing = str(current_state.get("workflow_type") or "").strip()
    if existing:
        return existing
    for event in events:
        payload = dict(event.get("payload") or {})
        workflow_type = str(payload.get("workflow_type") or "").strip()
        if workflow_type:
            return workflow_type
    return ""
