"""Characterization tests pinning per-command-type semantics in durable_runtime.

The golden snapshot below was generated from the pre-CommandTypeSpec literal
tables (owner registry, stage ids, readiness effects, display contracts,
activity spine sets, running-control category sets, provider after-start mode
sets, and the two public-web phase-order tuples). It is the safety net for the
CommandTypeSpec registry refactor: accessor behavior must stay byte-identical.

Do NOT regenerate this snapshot to make a failing test pass — a failure here
means observable command-type semantics changed.
"""

from __future__ import annotations

import json
from hashlib import sha1

import sourcing_agent.durable_runtime as dr

BOGUS_COMMAND_TYPE = "bogus.command.type"

GOLDEN_COMMAND_TYPE_SNAPSHOT = {
    "acquisition.intent.resolve": {
        "activity_spine_record_sha1": "585a14d2b769fb2ef8b763b031ee6baffcc31b56",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "0c6c6af92076e3ba15a134967b1f2af42aedf584",
        "crm_public_web_phase_index": None,
        "display": [
            "Resolve acquisition intent",
            "acquisition",
            "Normalize the bounded acquisition request before plan building."
        ],
        "owner": "acquisition_planner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "acquisition_intent_ready_for_plan",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "acquisition_intent_resolve"
    },
    "acquisition.plan.build": {
        "activity_spine_record_sha1": "57b4fdf5ac670cbf93db868b9b7ae7991162f525",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "fbac9e38584589b1f680e76db9cf133d3d29baa2",
        "crm_public_web_phase_index": None,
        "display": [
            "Build acquisition plan",
            "acquisition",
            "Build a typed acquisition plan without provider side effects."
        ],
        "owner": "acquisition_planner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "acquisition_plan_ready_for_review",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "acquisition_plan_build"
    },
    "acquisition.plan.commit": {
        "activity_spine_record_sha1": "a2bd93fb29d24243f25d2c79634739e0c0368c57",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "8bb10b5ed16b5a13d42926770399598dfa8439e5",
        "crm_public_web_phase_index": None,
        "display": [
            "Commit reviewed plan",
            "acquisition",
            "Materialize the approved acquisition plan as a PG-only acquisition run."
        ],
        "owner": "acquisition_planner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "acquisition_plan_committed",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "acquisition_plan_commit"
    },
    "acquisition.plan_review.request": {
        "activity_spine_record_sha1": "61fc75591500198e657b844488a5120bee0387cf",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "db5bcf561dd7b4f5fbf511efc5f0421178779558",
        "crm_public_web_phase_index": None,
        "display": [
            "Request plan review",
            "acquisition",
            "Open or reuse a human review session before execution."
        ],
        "owner": "acquisition_planner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "acquisition_plan_review_requested",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "acquisition_plan_review_request"
    },
    "acquisition.probe.collect": {
        "activity_spine_record_sha1": "6ac2387855ef2bebc8884ef448c8909a84f351f1",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "742a68b9268c1ff62e5299d43eb05553f11449b6",
        "crm_public_web_phase_index": None,
        "display": [
            "Collect acquisition probe",
            "acquisition",
            "Collect probe results and prepare scale planning."
        ],
        "owner": "acquisition_probe_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "acquisition_probe_collected",
        "running_control_categories": [
            "provider_attempt"
        ],
        "stage_id": "acquisition_probe_collect"
    },
    "acquisition.probe.submit": {
        "activity_spine_record_sha1": "e330b7da0be19b83517cd355ff0a26f59794afce",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "35a5217b77f434c905fbc28a506ab65a6f505b34",
        "crm_public_web_phase_index": None,
        "display": [
            "Submit acquisition probe",
            "acquisition",
            "Submit the bounded acquisition probe."
        ],
        "owner": "acquisition_probe_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "acquisition_probe_submitted",
        "running_control_categories": [
            "provider_attempt"
        ],
        "stage_id": "acquisition_probe_submit"
    },
    "acquisition.run.create": {
        "activity_spine_record_sha1": "7ee24b00df4c28b03d3782243c24a10772aaf757",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "9f9284787923fbc67b39fd000cd69bc099956fcf",
        "crm_public_web_phase_index": None,
        "display": [
            "Create acquisition run",
            "acquisition",
            "Create the durable root for a staged acquisition operation."
        ],
        "owner": "acquisition_run_writer",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "acquisition_run_requested",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "acquisition_run_create"
    },
    "acquisition.scale.plan": {
        "activity_spine_record_sha1": "640f6139defb1733a21241b0e154fc28552bdff3",
        "activity_spine_requirement": "activity_run_boundary_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "575cabfbcea05d108bf4f1490c77d56e09553731",
        "crm_public_web_phase_index": None,
        "display": [
            "Plan acquisition scale",
            "acquisition",
            "Create discovery lanes and activity boundaries."
        ],
        "owner": "acquisition_scale_planner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "acquisition_scale_planned",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "acquisition_scale_plan"
    },
    "collection.authoritative.merge": {
        "activity_spine_record_sha1": "4590e641eb37112e7b9741af4458ceb35d768653",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "6c6ac4dbcb590518251aaee6c9e4741ba7e77a29",
        "crm_public_web_phase_index": None,
        "display": [
            "Merge collection authoritative projection",
            "projection",
            "Merge a run-scope projection into collection-authoritative assets."
        ],
        "owner": "collection_writer_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "collection_authoritative_projection_merged",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "collection_authoritative_merge"
    },
    "company.logo.profile_experience.discover": {
        "activity_spine_record_sha1": "b592e44cb71ffe684b6cf5cedf398e799bc2eb5d",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "b5b613f05c48545831fb74b9fc3bad765f715877",
        "crm_public_web_phase_index": None,
        "display": [
            "Discover profile company logo",
            "company_assets",
            "Read one fresh profile work-experience logo candidate and plan stable media caching."
        ],
        "owner": "company_asset_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "company_logo_profile_evidence_planned",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "company_logo_profile_experience_discover"
    },
    "company.public_web.assets.materialize": {
        "activity_spine_record_sha1": "43770fad515e76fc3eccd7eaebcad705d128ed5d",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": 1,
        "control_policy_record_sha1": "bb1ebeff0c6474ac707565ec55c2a31fad9a645e",
        "crm_public_web_phase_index": None,
        "display": [
            "Materialize company Public Web assets",
            "company_assets",
            "Sync collected company Public Web rows into CompanyAsset and CompanyEvidence."
        ],
        "owner": "company_public_web_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "company_public_web_assets_materialized",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "company_public_web_assets_materialize"
    },
    "company.public_web.refresh": {
        "activity_spine_record_sha1": "e3b9c8d31a36fa3df6a9c899da1d56952c286d47",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "36490156f00c681d5578274439cdb354c343ea4d",
        "crm_public_web_phase_index": None,
        "display": [
            "Refresh company Public Web assets",
            "company_assets",
            "Plan company-level Public Web source collection and asset materialization."
        ],
        "owner": "company_public_web_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "company_public_web_refreshed",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "company_public_web_refresh"
    },
    "company.public_web.source.collect": {
        "activity_spine_record_sha1": "4f943e9c693ccaad1e2e77b30a1940725a93bc34",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": 0,
        "control_policy_record_sha1": "7779d5b1d70186ef2781d5cd29a9c64499f98be4",
        "crm_public_web_phase_index": None,
        "display": [
            "Collect company Public Web sources",
            "company_assets",
            "Collect company-level Public Web source rows and model-safe artifacts."
        ],
        "owner": "company_public_web_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "company_public_web_sources_collected",
        "running_control_categories": [
            "provider_attempt"
        ],
        "stage_id": "company_public_web_source_collect"
    },
    "crm.note.add": {
        "activity_spine_record_sha1": "29b91b900decefa76ed88b442e5c1ef967d1075e",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "cc268ac7b4ee9de37088ad0570316386a57153d9",
        "crm_public_web_phase_index": None,
        "display": [
            "Add CRM note",
            "crm",
            "Append a CRM note through the CRM writer owner."
        ],
        "owner": "crm_writer",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_note_added",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "crm_note_add"
    },
    "crm.public_web.documents.fetch": {
        "activity_spine_record_sha1": "144266f06e49a95b7705372692b364140964e625",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "8479594a4da04afce98396af4e9de5895a0e9d3a",
        "crm_public_web_phase_index": 2,
        "display": [
            "Fetch CRM Public Web documents",
            "crm_public_web",
            "Fetch and persist candidate Public Web documents."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "active",
            "fail_closed_until_terminal"
        ],
        "readiness_effect": "crm_public_web_documents_fetched",
        "running_control_categories": [
            "crm_public_web_phase"
        ],
        "stage_id": "crm_public_web_documents_fetch"
    },
    "crm.public_web.evidence.adjudicate": {
        "activity_spine_record_sha1": "f573ebcb2bdcc70b6998eddf50eff2e71d2b6027",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "fdcc1f22a8a5497feb76e1dbee9b3b6aa443b2e7",
        "crm_public_web_phase_index": 3,
        "display": [
            "Adjudicate CRM Public Web evidence",
            "crm_public_web",
            "Review fetched Public Web evidence into candidate-safe signals."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "active",
            "fail_closed_until_terminal"
        ],
        "readiness_effect": "crm_public_web_evidence_adjudicated",
        "running_control_categories": [
            "crm_public_web_phase"
        ],
        "stage_id": "crm_public_web_evidence_adjudicate"
    },
    "crm.public_web.model_safe.finalize": {
        "activity_spine_record_sha1": "45e50c8afbdda0885ca918e123cd64ac1048748b",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "5243b751c49245752521cd657cb8aa1d6d7d413a",
        "crm_public_web_phase_index": 4,
        "display": [
            "Finalize model-safe Public Web payload",
            "crm_public_web",
            "Finalize model-safe Public Web artifacts for review/export."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "active",
            "fail_closed_until_terminal"
        ],
        "readiness_effect": "crm_public_web_model_safe_finalized",
        "running_control_categories": [
            "crm_public_web_phase"
        ],
        "stage_id": "crm_public_web_model_safe_finalize"
    },
    "crm.public_web.queue_batch": {
        "activity_spine_record_sha1": "10e328f67c9bb50b69beca96787afe6f998a3dd7",
        "activity_spine_requirement": "orchestration_downstream_activity_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "c7e396aea545c00ad20ac9f8e8246a3288972eaa",
        "crm_public_web_phase_index": None,
        "display": [
            "Queue CRM Public Web batch",
            "crm_public_web",
            "Create CRM Public Web batch/run rows and downstream phase commands."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_public_web_workers_queued",
        "running_control_categories": [
            "orchestration"
        ],
        "stage_id": "crm_public_web_queue_batch"
    },
    "crm.public_web.search.poll_fetch": {
        "activity_spine_record_sha1": "49eb2e001a0f0ca120cf59dab09b29e4f73b113d",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "e8edcb04bc008fc19a8d6d1d41a71fbf3e4f02b8",
        "crm_public_web_phase_index": 1,
        "display": [
            "Poll CRM Public Web search",
            "crm_public_web",
            "Poll/fetch provider search results for one CRM Public Web run."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "crm_public_web_search_polled_or_fetched",
        "running_control_categories": [
            "crm_public_web_phase"
        ],
        "stage_id": "crm_public_web_search_poll_fetch"
    },
    "crm.public_web.search.submit": {
        "activity_spine_record_sha1": "5086fc65e4e8596bf7ab678eba0b498fea43a0a2",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "9c311f8ff8411943023895ab39a1e82e95cefb75",
        "crm_public_web_phase_index": 0,
        "display": [
            "Submit CRM Public Web search",
            "crm_public_web",
            "Submit provider search for one CRM Public Web run."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "crm_public_web_search_submitted",
        "running_control_categories": [
            "crm_public_web_phase"
        ],
        "stage_id": "crm_public_web_search_submit"
    },
    "crm.public_web.signals.materialize": {
        "activity_spine_record_sha1": "4d1698e3ce1c1f1875f53c2637369f3d92bcd401",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "5bb63e64abf61d8d381e90f14de66b8196a75c64",
        "crm_public_web_phase_index": 5,
        "display": [
            "Materialize CRM Public Web signals",
            "crm_public_web",
            "Materialize reviewed Public Web signals into person asset/evidence rows."
        ],
        "owner": "crm_public_web_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_public_web_signals_materialized",
        "running_control_categories": [
            "crm_public_web_phase"
        ],
        "stage_id": "crm_public_web_signals_materialize"
    },
    "crm.record.add_from_projection": {
        "activity_spine_record_sha1": "5c1ac4ba5d0a5470d78bfcc117090fee9687e075",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "b2702476ec79e28e098e12557419f5ebf5c94e92",
        "crm_public_web_phase_index": None,
        "display": [
            "Add person to CRM",
            "crm",
            "Create or link a CRM record from canonical projection/person identity."
        ],
        "owner": "crm_writer",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_record_added_from_projection",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "crm_record_add_from_projection"
    },
    "crm.record.update": {
        "activity_spine_record_sha1": "d5feca66eeaa0554d3659b3d429f409803a9f667",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "1ea8fa19c4efae391ca8ddd98e790c27f20c9f65",
        "crm_public_web_phase_index": None,
        "display": [
            "Update CRM record",
            "crm",
            "Apply a bounded CRM record or engagement update."
        ],
        "owner": "crm_writer",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_record_updated",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "crm_record_update"
    },
    "crm.task.create": {
        "activity_spine_record_sha1": "c8dc2e097270cc1fc1581ada4002e3898978bcb7",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "46c83bc2b0021e14b9a8404b5657dcbedf80b604",
        "crm_public_web_phase_index": None,
        "display": [
            "Create CRM task",
            "crm",
            "Create a queryable CRM follow-up task with audit event evidence."
        ],
        "owner": "crm_writer",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_task_created",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "crm_task_create"
    },
    "excel.intake.run": {
        "activity_spine_record_sha1": "b69808d9e74ec77b3c7a76ad6ebb13b72f4fae7d",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "566fd720a4f6816cfde226b7cb86fbbab78f860d",
        "crm_public_web_phase_index": None,
        "display": [
            "Run Excel intake",
            "excel",
            "Process an Excel intake job through the durable command owner."
        ],
        "owner": "excel_intake_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "excel_intake_started",
        "running_control_categories": [
            "local_thread"
        ],
        "stage_id": "excel_intake"
    },
    "export.crm_public_web.generate": {
        "activity_spine_record_sha1": "732945f236c29b3fc9638d3a9d2e7fcc9808e044",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "e9397aa2e2e0a8bc38f079b3cc5ffe8e8cfcc536",
        "crm_public_web_phase_index": None,
        "display": [
            "Export CRM Public Web signals",
            "export",
            "Generate a CRM Public Web review/export artifact."
        ],
        "owner": "crm_public_web_exporter",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "crm_public_web_export_generated",
        "running_control_categories": [
            "export_artifact"
        ],
        "stage_id": "crm_public_web_export"
    },
    "export.projection.generate": {
        "activity_spine_record_sha1": "9c532c5fee716adc675f716eb3401d7f3ab5c64a",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "24fca81e705767c975e0e22c7a6692da9d6446a1",
        "crm_public_web_phase_index": None,
        "display": [
            "Export projection candidates",
            "export",
            "Generate a projection candidate export artifact."
        ],
        "owner": "projection_exporter",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "projection_export_generated",
        "running_control_categories": [
            "export_artifact"
        ],
        "stage_id": "projection_export"
    },
    "linkedin.discovery_query.run": {
        "activity_spine_record_sha1": "9a810255f0fb682689f85ae4e5106c2a193ed41e",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "4073328384f74a6855d30caca4d6bf0e56b86b2e",
        "crm_public_web_phase_index": None,
        "display": [
            "Run LinkedIn discovery lane",
            "linkedin",
            "Execute one operation-native discovery lane."
        ],
        "owner": "linkedin_acquisition_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "stage1_discovery_lane_submitted",
        "running_control_categories": [
            "provider_attempt"
        ],
        "stage_id": "stage1_candidate_discovery"
    },
    "linkedin.local_profile_delta.apply": {
        "activity_spine_record_sha1": "59f7cdd8a27134b8ff629be16da03f89506d1256",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "81bbcb58b40d11b8b3e9e980717b0323eeb0b238",
        "crm_public_web_phase_index": None,
        "display": [
            "Apply local profile delta",
            "linkedin",
            "Apply fetched profile data into local candidate materialization."
        ],
        "owner": "profile_local_apply_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "local_profile_delta_applied",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "local_profile_delta_apply"
    },
    "linkedin.profile_fetch.activity.run": {
        "activity_spine_record_sha1": "d10265bb376df37a046af2bfc2a7893d34d97e95",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "bcbd5c57ffb965f5dabd0d3379f61ce8e59238b8",
        "crm_public_web_phase_index": None,
        "display": [
            "Plan profile fetch activity",
            "linkedin",
            "Resolve profile cache hits and provider fetch requirements."
        ],
        "owner": "linkedin_profile_activity_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "profile_fetch_activity_planned",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "operation_native_profile_fetch"
    },
    "linkedin.profile_fetch.provider.fetch": {
        "activity_spine_record_sha1": "6c5fc2c261c15bcfef15c6d3aad513df8f129300",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "238024f2ffb13cc6d3077111949e68861d56ea4b",
        "crm_public_web_phase_index": None,
        "display": [
            "Fetch profiles from provider",
            "linkedin",
            "Fetch required LinkedIn profiles through provider attempts."
        ],
        "owner": "linkedin_profile_activity_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "profile_fetch_provider_completed",
        "running_control_categories": [
            "provider_attempt"
        ],
        "stage_id": "operation_native_profile_provider_fetch"
    },
    "linkedin.profile_refill.submit_batch": {
        "activity_spine_record_sha1": "9cf65b88d5203d7ded4be0e253ddc8761689d183",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "635c40182d8a5c9ce20e19807f71ddc204f0b392",
        "crm_public_web_phase_index": None,
        "display": [
            "Submit profile refill batch",
            "linkedin",
            "Submit a bounded LinkedIn profile refill batch."
        ],
        "owner": "linkedin_profile_owner",
        "provider_after_start": [
            "active",
            "poll_cancel_late_result_quarantine"
        ],
        "readiness_effect": "profile_refill_submitted",
        "running_control_categories": [
            "provider_attempt"
        ],
        "stage_id": "profile_fetch"
    },
    "linkedin.profile_terminal.admit": {
        "activity_spine_record_sha1": "85fe4fc02a6bd16991538cf2c85e5bb43425d8b8",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "d9cc93b4e4c01e243b69502eda8197739751be1d",
        "crm_public_web_phase_index": None,
        "display": [
            "Admit terminal profiles",
            "linkedin",
            "Convert cache/provider facts into terminal profile deltas."
        ],
        "owner": "linkedin_profile_activity_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "profile_terminal_admitted",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "operation_native_profile_terminal_admission"
    },
    "linkedin.profile_url_terminal.record": {
        "activity_spine_record_sha1": "a3d86e6215838bac279164f176fbc8744bcffed3",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "bc08875304db1cb9bb7e63c405035ccd6fe9f360",
        "crm_public_web_phase_index": None,
        "display": [
            "Record profile URL terminal state",
            "linkedin",
            "Persist terminal registry state for fetched or failed profile URLs."
        ],
        "owner": "linkedin_profile_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "profile_url_terminal_recorded",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "profile_fetch_terminal_recording"
    },
    "media.asset.cache": {
        "activity_spine_record_sha1": "7509fecbd2126a20b5c992284965e4aa75749578",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "96bbe87712514b28cc6e8f184a5a066cd1eca5c0",
        "crm_public_web_phase_index": None,
        "display": [
            "Cache media asset",
            "media_assets",
            "Fetch or store stable person/company media assets."
        ],
        "owner": "media_asset_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "media_asset_cached",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "media_asset_cache"
    },
    "projection.board_visible_patch.publish": {
        "activity_spine_record_sha1": "05c5a6eec683cacef33c0fdfeae47f6e5b9476e0",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "472f1f79e9dd7a8914046b2ee9c65d38a25e9006",
        "crm_public_web_phase_index": None,
        "display": [
            "Publish board-visible patch",
            "projection",
            "Publish candidate changes to the board-visible patch stream."
        ],
        "owner": "board_visible_projection_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "board_visible_patch_published",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "board_visible_publication"
    },
    "projection.facet_layering.build": {
        "activity_spine_record_sha1": "1220ca511ef5f8a65408120dc4699d8b9146bb1a",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "bc164097f23f4b9011004c16491b3e2a3f50b077",
        "crm_public_web_phase_index": None,
        "display": [
            "Build projection facets and layering",
            "projection",
            "Build canonical filter facets, layering, and related read-model metadata."
        ],
        "owner": "projection_facet_layering_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "projection_facet_layering_built",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "post_result_layering"
    },
    "projection.person_search_index.build": {
        "activity_spine_record_sha1": "431a08d973deaa57646e552bf5ecbdf9265dff67",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "74d1a9817eab3b7ec086bb6cb7fb59de8f07abe1",
        "crm_public_web_phase_index": None,
        "display": [
            "Build projection person search index",
            "projection",
            "Build searchable person/profile index rows for projection readers."
        ],
        "owner": "projection_index_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "projection_person_search_index_built",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "projection_index_build"
    },
    "projection.profile_admission.apply": {
        "activity_spine_record_sha1": "acf1042616d513885a337304358f4ba844806cad",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "febe13168b0a11e55fd011a2450a28b6f3b98f64",
        "crm_public_web_phase_index": None,
        "display": [
            "Admit profiles to projection",
            "projection",
            "Apply terminal profile facts into canonical run-scope projection membership."
        ],
        "owner": "serving_projection_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "profile_terminal_projection_admitted",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "operation_native_projection_admission"
    },
    "projection.run_scope.finalize": {
        "activity_spine_record_sha1": "c919947b99de6a9659d6d3988f419fcc8e5f3fa1",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "37e4d5c55569beaa705073a6403c4fcdbdd94529",
        "crm_public_web_phase_index": None,
        "display": [
            "Finalize run-scope projection",
            "projection",
            "Publish canonical run-scope projection readiness."
        ],
        "owner": "serving_projection_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "run_scope_projection_finalized",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "serving_projection_finalization"
    },
    "snapshot.compaction.run": {
        "activity_spine_record_sha1": "f88889da49b89ccd62994f90e7a0a8548d9cfc07",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "company_public_web_phase_index": None,
        "control_policy_record_sha1": "59ac41fb801a5797ab7c6237aa08c96724de2ba8",
        "crm_public_web_phase_index": None,
        "display": [
            "Compact snapshot artifacts",
            "maintenance",
            "Run background snapshot/materialization compaction."
        ],
        "owner": "snapshot_materialization_owner",
        "provider_after_start": [
            "not_applicable",
            "not_applicable"
        ],
        "readiness_effect": "snapshot_compaction_completed",
        "running_control_categories": [
            "domain_mutation"
        ],
        "stage_id": "snapshot_compaction"
    }
}


GOLDEN_CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES = (
    "crm.public_web.search.submit",
    "crm.public_web.search.poll_fetch",
    "crm.public_web.documents.fetch",
    "crm.public_web.evidence.adjudicate",
    "crm.public_web.model_safe.finalize",
    "crm.public_web.signals.materialize",
)

GOLDEN_COMPANY_PUBLIC_WEB_PHASE_COMMAND_TYPES = (
    "company.public_web.source.collect",
    "company.public_web.assets.materialize",
)

GOLDEN_PROVIDER_ATTEMPT_COMMAND_TYPES = (
    "acquisition.probe.collect",
    "acquisition.probe.submit",
    "company.public_web.source.collect",
    "linkedin.discovery_query.run",
    "linkedin.profile_fetch.provider.fetch",
    "linkedin.profile_refill.submit_batch",
)

GOLDEN_ORCHESTRATION_COMMAND_TYPES = (
    "acquisition.intent.resolve",
    "acquisition.plan.build",
    "acquisition.plan.commit",
    "acquisition.plan_review.request",
    "acquisition.run.create",
    "acquisition.scale.plan",
    "company.public_web.refresh",
    "crm.public_web.queue_batch",
)

GOLDEN_DOMAIN_MUTATION_COMMAND_TYPES = (
    "collection.authoritative.merge",
    "company.logo.profile_experience.discover",
    "company.public_web.assets.materialize",
    "crm.note.add",
    "crm.record.add_from_projection",
    "crm.record.update",
    "crm.task.create",
    "linkedin.local_profile_delta.apply",
    "linkedin.profile_fetch.activity.run",
    "linkedin.profile_terminal.admit",
    "linkedin.profile_url_terminal.record",
    "media.asset.cache",
    "projection.board_visible_patch.publish",
    "projection.facet_layering.build",
    "projection.person_search_index.build",
    "projection.profile_admission.apply",
    "projection.run_scope.finalize",
    "snapshot.compaction.run",
)

GOLDEN_CRM_WRITER_COMMAND_TYPES = (
    "crm.record.add_from_projection",
    "crm.record.update",
    "crm.note.add",
    "crm.task.create",
)


def _record_sha1(record: dict) -> str:
    return sha1(json.dumps(record, sort_keys=True).encode("utf-8")).hexdigest()


def _observed_entry(command_type: str) -> dict:
    display = dr.workflow_command_display_contract(command_type)
    spine = dr.workflow_command_activity_spine_policy(command_type)
    policy = dr.workflow_command_control_policy(command_type)
    crm_idx = (
        dr.CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES.index(command_type)
        if command_type in dr.CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES
        else None
    )
    company_idx = (
        dr.COMPANY_PUBLIC_WEB_PHASE_COMMAND_TYPES.index(command_type)
        if command_type in dr.COMPANY_PUBLIC_WEB_PHASE_COMMAND_TYPES
        else None
    )
    return {
        "owner": dr.DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(command_type),
        "stage_id": dr.default_stage_id_for_command_type(command_type),
        "readiness_effect": dr.default_readiness_effect_for_command_type(command_type),
        "display": [display.display_label, display.display_category, display.description],
        "activity_spine_requirement": spine.requirement,
        "activity_spine_record_sha1": _record_sha1(spine.to_record()),
        "running_control_categories": list(
            dr.workflow_command_running_control_categories(command_type)
        ),
        "provider_after_start": [
            policy.provider_after_start_control_status,
            policy.provider_after_start_control_mode,
        ],
        "control_policy_record_sha1": _record_sha1(policy.to_record()),
        "crm_public_web_phase_index": crm_idx,
        "company_public_web_phase_index": company_idx,
    }


def test_known_command_type_universe_is_pinned():
    assert len(GOLDEN_COMMAND_TYPE_SNAPSHOT) == 41
    assert sorted(dr.DEFAULT_COMMAND_OWNER_REGISTRY.to_record().keys()) == sorted(
        GOLDEN_COMMAND_TYPE_SNAPSHOT.keys()
    )


def test_every_known_command_type_matches_golden_snapshot():
    for command_type, expected in GOLDEN_COMMAND_TYPE_SNAPSHOT.items():
        observed = _observed_entry(command_type)
        assert observed == expected, f"semantics drifted for {command_type}"


def test_phase_order_tuples_match_golden_order():
    assert dr.CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES == GOLDEN_CRM_PUBLIC_WEB_PHASE_COMMAND_TYPES
    assert (
        dr.COMPANY_PUBLIC_WEB_PHASE_COMMAND_TYPES
        == GOLDEN_COMPANY_PUBLIC_WEB_PHASE_COMMAND_TYPES
    )


def test_public_category_tuples_match_golden():
    assert dr.PROVIDER_ATTEMPT_COMMAND_TYPES == GOLDEN_PROVIDER_ATTEMPT_COMMAND_TYPES
    assert dr.ORCHESTRATION_COMMAND_TYPES == GOLDEN_ORCHESTRATION_COMMAND_TYPES
    assert dr.DOMAIN_MUTATION_COMMAND_TYPES == GOLDEN_DOMAIN_MUTATION_COMMAND_TYPES
    assert dr.CRM_WRITER_COMMAND_TYPES == GOLDEN_CRM_WRITER_COMMAND_TYPES


def test_unknown_command_type_owner_lookup_raises_key_error():
    try:
        dr.DEFAULT_COMMAND_OWNER_REGISTRY.owner_for(BOGUS_COMMAND_TYPE)
    except KeyError as exc:
        assert str(exc) == "'unknown workflow command type: bogus.command.type'"
    else:
        raise AssertionError("owner_for must raise KeyError for unknown command types")


def test_unknown_command_type_accessor_defaults_are_pinned():
    assert dr.default_stage_id_for_command_type(BOGUS_COMMAND_TYPE) == ""
    assert dr.default_readiness_effect_for_command_type(BOGUS_COMMAND_TYPE) == ""
    assert dr.workflow_command_running_control_categories(BOGUS_COMMAND_TYPE) == ()

    display = dr.workflow_command_display_contract(BOGUS_COMMAND_TYPE)
    assert display.to_record() == {
        "schema_version": "workflow_command_display_contract_v1",
        "command_type": BOGUS_COMMAND_TYPE,
        "owner": "",
        "display_label": "",
        "display_category": "",
        "description": "",
        "source_of_truth": "durable_runtime.workflow_command_display_contract",
        "fallback_status": "fail_closed",
    }

    spine = dr.workflow_command_activity_spine_policy(BOGUS_COMMAND_TYPE)
    assert spine.to_record() == {
        "schema_version": "w11_workflow_command_activity_spine_policy_v1",
        "command_type": BOGUS_COMMAND_TYPE,
        "owner": "",
        "requirement": "command_result_control_plane",
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
        "migration_status": "control_plane_command_result_only",
        "deletion_condition": "promote to activity spine before adding provider/domain side effects",
    }

    policy = dr.workflow_command_control_policy(BOGUS_COMMAND_TYPE)
    assert policy.owner == ""
    assert policy.running_cancel_supported is False
    assert policy.running_resume_supported is False
    assert (
        policy.running_resume_blocked_reason
        == "owner_specific_resume_requires_policy_registration"
    )
    assert policy.running_resume_upgrade_requirements == (
        "register_owner_specific_resume_policy",
    )
    assert policy.provider_after_start_control_status == "not_applicable"
    assert policy.provider_after_start_control_mode == "not_applicable"
    assert _record_sha1(policy.to_record()) == "f49da40d23b36cb1aec53654fa2b5066623d4e80"


MANIFEST_ENTRY_FIELDS = {
    "command_type",
    "owner",
    "stage_id",
    "readiness_effect",
    "display_label",
    "display_category",
    "display_description",
    "activity_spine_requirement",
    "running_control_categories",
    "provider_after_start_mode",
    "phase_group",
    "phase_index",
    "metric_key",
    "product_label_zh",
    "migration_step_id",
    "expected_run_statuses",
    # Phase 4 Step 3: owner-specific RUNNING cancel/resume handler slots.
    "cancel_handler",
    "resume_handler",
}


def test_command_type_manifest_covers_exactly_the_known_types():
    manifest = dr.command_type_manifest()
    assert sorted(manifest.keys()) == sorted(GOLDEN_COMMAND_TYPE_SNAPSHOT.keys())
    for command_type, entry in manifest.items():
        assert set(entry.keys()) == MANIFEST_ENTRY_FIELDS, command_type
        assert entry["command_type"] == command_type


def test_command_type_manifest_is_json_serializable_round_trip():
    manifest = dr.command_type_manifest()
    assert json.loads(json.dumps(manifest)) == manifest


def test_command_type_manifest_matches_golden_snapshot_fields():
    manifest = dr.command_type_manifest()
    for command_type, expected in GOLDEN_COMMAND_TYPE_SNAPSHOT.items():
        entry = manifest[command_type]
        assert entry["owner"] == expected["owner"], command_type
        assert entry["stage_id"] == expected["stage_id"], command_type
        assert entry["readiness_effect"] == expected["readiness_effect"], command_type
        assert [
            entry["display_label"],
            entry["display_category"],
            entry["display_description"],
        ] == expected["display"], command_type
        assert (
            entry["activity_spine_requirement"] == expected["activity_spine_requirement"]
        ), command_type
        assert (
            entry["running_control_categories"] == expected["running_control_categories"]
        ), command_type


def test_command_type_manifest_spot_checks():
    manifest = dr.command_type_manifest()
    assert manifest["crm.public_web.search.submit"] == {
        "command_type": "crm.public_web.search.submit",
        "owner": "crm_public_web_owner",
        "stage_id": "crm_public_web_search_submit",
        "readiness_effect": "crm_public_web_search_submitted",
        "display_label": "Submit CRM Public Web search",
        "display_category": "crm_public_web",
        "display_description": "Submit provider search for one CRM Public Web run.",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "running_control_categories": ["crm_public_web_phase"],
        "provider_after_start_mode": "poll_cancel_late_result_quarantine",
        "phase_group": "crm_public_web",
        "phase_index": 0,
        "metric_key": "crm_public_web_search_submit",
        "product_label_zh": "提交公开搜索",
        "migration_step_id": "W7f_crm_public_web_search_submit",
        "expected_run_statuses": ["queued"],
        "cancel_handler": "_cancel_running_crm_public_web_phase_command",
        "resume_handler": "_resume_running_crm_public_web_phase_command",
    }
    assert manifest["excel.intake.run"] == {
        "command_type": "excel.intake.run",
        "owner": "excel_intake_owner",
        "stage_id": "excel_intake",
        "readiness_effect": "excel_intake_started",
        "display_label": "Run Excel intake",
        "display_category": "excel",
        "display_description": "Process an Excel intake job through the durable command owner.",
        "activity_spine_requirement": "activity_attempt_entity_delta_required",
        "running_control_categories": ["local_thread"],
        "provider_after_start_mode": "not_applicable",
        "phase_group": "",
        "phase_index": None,
        "metric_key": "excel_intake_run",
        "product_label_zh": "",
        "migration_step_id": "",
        "expected_run_statuses": [],
        "cancel_handler": "_cancel_running_excel_intake_command",
        "resume_handler": "_resume_running_excel_intake_command",
    }
    assert manifest["acquisition.scale.plan"] == {
        "command_type": "acquisition.scale.plan",
        "owner": "acquisition_scale_planner",
        "stage_id": "acquisition_scale_plan",
        "readiness_effect": "acquisition_scale_planned",
        "display_label": "Plan acquisition scale",
        "display_category": "acquisition",
        "display_description": "Create discovery lanes and activity boundaries.",
        "activity_spine_requirement": "activity_run_boundary_required",
        "running_control_categories": ["orchestration"],
        "provider_after_start_mode": "not_applicable",
        "phase_group": "",
        "phase_index": None,
        "metric_key": "acquisition_scale_plan",
        "product_label_zh": "",
        "migration_step_id": "",
        "expected_run_statuses": [],
        "cancel_handler": "_cancel_running_acquisition_scale_plan_before_discovery",
        "resume_handler": "_resume_running_orchestration_command",
    }


# Golden values for the spec fields added when the per-command tables in
# workflow_service_metrics.py and orchestrator.py were collapsed onto the
# registry. Pinned from the pre-collapse literal tables; do NOT regenerate to
# make a failing test pass.
GOLDEN_IRREGULAR_METRIC_KEYS = {
    "export.projection.generate": "projection_export_generate",
    "export.crm_public_web.generate": "crm_public_web_export_generate",
}

GOLDEN_CRM_PUBLIC_WEB_PHASE_FIELDS = {
    "crm.public_web.search.submit": (
        "提交公开搜索",
        "W7f_crm_public_web_search_submit",
        ("queued",),
    ),
    "crm.public_web.search.poll_fetch": (
        "取回搜索结果",
        "W7f_crm_public_web_search_poll_fetch",
        ("search_submitted", "searching"),
    ),
    "crm.public_web.documents.fetch": (
        "整理页面内容",
        "W7f_crm_public_web_documents_fetch",
        ("entry_links_ready", "fetching"),
    ),
    "crm.public_web.evidence.adjudicate": (
        "判断候选信号",
        "W7f_crm_public_web_evidence_adjudicate",
        ("documents_fetched", "analyzing"),
    ),
    "crm.public_web.model_safe.finalize": (
        "生成审核候选",
        "W7f_crm_public_web_model_safe_finalize",
        ("adjudication_completed",),
    ),
    "crm.public_web.signals.materialize": (
        "保存公开信息结果",
        "W7f_crm_public_web_signals_materialize",
        ("analysis_completed",),
    ),
}


def test_metric_keys_match_golden_rule_and_irregulars():
    for command_type in GOLDEN_COMMAND_TYPE_SNAPSHOT:
        expected = GOLDEN_IRREGULAR_METRIC_KEYS.get(
            command_type, command_type.replace(".", "_")
        )
        assert dr.workflow_command_metric_key(command_type) == expected, command_type
    # Only the two export commands may diverge from the mechanical rule.
    irregular = {
        command_type: spec.metric_key
        for command_type, spec in dr.DEFAULT_COMMAND_TYPE_SPECS.items()
        if spec.metric_key and spec.metric_key != command_type.replace(".", "_")
    }
    assert irregular == GOLDEN_IRREGULAR_METRIC_KEYS


def test_crm_public_web_phase_fields_match_golden():
    for command_type, (label_zh, step_id, statuses) in GOLDEN_CRM_PUBLIC_WEB_PHASE_FIELDS.items():
        assert dr.workflow_command_product_label_zh(command_type) == label_zh, command_type
        assert dr.workflow_command_migration_step_id(command_type) == step_id, command_type
        assert dr.workflow_command_expected_run_statuses(command_type) == statuses, command_type
    # Every non-phase command type carries the empty defaults.
    for command_type in GOLDEN_COMMAND_TYPE_SNAPSHOT:
        if command_type in GOLDEN_CRM_PUBLIC_WEB_PHASE_FIELDS:
            continue
        assert dr.workflow_command_product_label_zh(command_type) == "", command_type
        assert dr.workflow_command_migration_step_id(command_type) == "", command_type
        assert dr.workflow_command_expected_run_statuses(command_type) == (), command_type


def test_new_spec_field_accessor_unknown_type_defaults_are_pinned():
    assert dr.workflow_command_metric_key(BOGUS_COMMAND_TYPE) == "bogus_command_type"
    assert dr.workflow_command_product_label_zh(BOGUS_COMMAND_TYPE) == ""
    assert dr.workflow_command_migration_step_id(BOGUS_COMMAND_TYPE) == ""
    assert dr.workflow_command_expected_run_statuses(BOGUS_COMMAND_TYPE) == ()


def test_cancel_resume_handler_slots_populated_for_every_command_type():
    # Phase 4 Step 3: every known command type carries an owner-specific cancel
    # and resume handler slot (no canonical-owner command type falls through to
    # the dispatcher default). The slot is a method NAME, not a bound method.
    for command_type in GOLDEN_COMMAND_TYPE_SNAPSHOT:
        cancel_name = dr.workflow_command_cancel_handler(command_type)
        resume_name = dr.workflow_command_resume_handler(command_type)
        assert cancel_name and isinstance(cancel_name, str), command_type
        assert resume_name and isinstance(resume_name, str), command_type
        assert cancel_name.startswith("_cancel_running_"), command_type
        assert resume_name.startswith("_resume_running_"), command_type


def test_cancel_resume_handler_slots_resolve_to_orchestrator_methods():
    # Name-based resolution contract: every slot name (owner-matched and
    # owner-agnostic) is a real callable on SourcingOrchestrator, so the
    # getattr(self, name) dispatch never KeyErrors/AttributeErrors at runtime.
    from sourcing_agent.orchestrator import SourcingOrchestrator

    names: set[str] = set()
    for command_type in GOLDEN_COMMAND_TYPE_SNAPSHOT:
        names.add(dr.workflow_command_cancel_handler(command_type))
        names.add(dr.workflow_command_resume_handler(command_type))
        agnostic_cancel = dr.workflow_command_cancel_owner_agnostic_handler(command_type)
        agnostic_resume = dr.workflow_command_resume_owner_agnostic_handler(command_type)
        if agnostic_cancel:
            names.add(agnostic_cancel)
        if agnostic_resume:
            names.add(agnostic_resume)
    for name in names:
        assert callable(getattr(SourcingOrchestrator, name, None)), name


def test_cancel_resume_handler_slot_accessor_unknown_type_defaults_are_pinned():
    assert dr.workflow_command_cancel_handler(BOGUS_COMMAND_TYPE) == ""
    assert dr.workflow_command_resume_handler(BOGUS_COMMAND_TYPE) == ""
    assert dr.workflow_command_cancel_owner_agnostic_handler(BOGUS_COMMAND_TYPE) == ""
    assert dr.workflow_command_resume_owner_agnostic_handler(BOGUS_COMMAND_TYPE) == ""
