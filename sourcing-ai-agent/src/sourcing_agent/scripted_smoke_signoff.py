from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_DEFAULT_MAX_JOB_TO_STAGE_1_PREVIEW_MS = 120_000.0
_DEFAULT_MAX_JOB_TO_FINAL_RESULTS_MS = 180_000.0
_DEFAULT_MAX_STAGE_1_PREVIEW_TO_FINAL_RESULTS_MS = 30_000.0
_DEFAULT_MAX_REMOTE_TO_LOCAL_MARKER_LAG_MS = 10_000.0
_DEFAULT_MAX_PROVIDER_SLOT_TO_REMOTE_WAIT_STARTED_MS = 5_000.0


def build_scripted_smoke_signoff_report(
    *,
    records: list[dict[str, Any]],
    summary: dict[str, Any] | None = None,
    expected_provider_mode: str = "scripted",
) -> dict[str, Any]:
    normalized_expected_mode = str(expected_provider_mode or "scripted").strip().lower() or "scripted"
    blocking_findings: list[dict[str, Any]] = []
    manual_review_required_findings: list[dict[str, Any]] = []
    known_acceptable_warnings: list[dict[str, Any]] = []
    passed_gates: list[dict[str, Any]] = []

    provider_invocation_count = 0
    provider_mode_failure_count = 0
    smoke_ready_count = 0
    parity_report_count = 0
    post_profile_report_count = 0
    profile_scheduler_report_count = 0
    recovery_phase_report_count = 0
    projection_cutover_report_count = 0
    legacy_artifact_coherence_report_count = 0
    legacy_materialization_write_contract_report_count = 0
    durable_command_owner_contract_report_count = 0
    legacy_public_web_retirement_report_count = 0
    target_public_web_contract_report_count = 0
    background_maintenance_report_count = 0
    background_maintenance_pending_count = 0
    cooperative_handoff_yield_report_count = 0
    cooperative_handoff_yield_count = 0
    cooperative_budget_yield_report_count = 0
    cooperative_budget_yield_count = 0

    for record_index, record_payload in enumerate(records):
        record = dict(record_payload or {})
        case_name = str(record.get("case") or f"case[{record_index}]").strip()
        evidence = {"case": case_name, "record_index": record_index}

        final = dict(record.get("final") or {})
        provider_invocations = [
            dict(item) for item in list(record.get("provider_invocations") or []) if isinstance(item, dict)
        ]
        provider_invocation_count += len(provider_invocations)
        for invocation_index, invocation in enumerate(provider_invocations):
            provider_mode = str(invocation.get("provider_mode") or "").strip().lower()
            if provider_mode != normalized_expected_mode:
                provider_mode_failure_count += 1
                blocking_findings.append(
                    _finding(
                        "provider_mode_not_scripted",
                        "blocking",
                        "A provider invocation did not use the expected no-cost provider mode.",
                        evidence={
                            **evidence,
                            "invocation_index": invocation_index,
                            "provider_mode": provider_mode or "<missing>",
                            "expected_provider_mode": normalized_expected_mode,
                            "logical_name": invocation.get("logical_name") or invocation.get("provider_name") or "",
                        },
                    )
                )

        provider_report = dict(record.get("provider_case_report") or {})
        expectations = dict(record.get("expectations") or {})
        service_metrics = dict(provider_report.get("service_metrics") or {})
        canonical_projection_proof = _canonical_projection_completion_proof(
            provider_report=provider_report,
            service_metrics=service_metrics,
        )
        raw_expectation_failures = [
            str(item)
            for item in list(record.get("expectation_failures") or final.get("expectation_failures") or [])
            if str(item).strip()
        ]
        expectation_failures = [
            item
            for item in raw_expectation_failures
            if not _is_covered_by_canonical_projection_proof(
                failure=item,
                canonical_projection_proof=canonical_projection_proof,
            )
        ]
        smoke_ready_effective = bool(final.get("smoke_ready")) or (
            not expectation_failures
            and raw_expectation_failures
            and canonical_projection_proof["complete"]
        )
        if not smoke_ready_effective:
            blocking_findings.append(
                _finding(
                    "smoke_case_not_ready",
                    "blocking",
                    "Smoke case is not ready for manual browser testing.",
                    evidence={
                        **evidence,
                        "smoke_completion_state": final.get("smoke_completion_state"),
                        "canonical_projection_proof": canonical_projection_proof,
                    },
                )
            )
        else:
            smoke_ready_count += 1

        if expectation_failures:
            blocking_findings.append(
                _finding(
                    "smoke_expectation_failure",
                    "blocking",
                    "Smoke expectation failures are blocking signoff.",
                    evidence={
                        **evidence,
                        "expectation_failures": expectation_failures[:20],
                        "covered_expectation_failures": [
                            item for item in raw_expectation_failures if item not in expectation_failures
                        ][:20],
                        "canonical_projection_proof": canonical_projection_proof,
                    },
                )
            )

        behavior_guardrails = dict(provider_report.get("behavior_guardrails") or {})
        if bool(behavior_guardrails.get("violation_detected")):
            blocking_findings.append(
                _finding(
                    "behavior_guardrail_violation",
                    "blocking",
                    "Provider behavior guardrails reported a violation.",
                    evidence={**evidence, "behavior_guardrails": behavior_guardrails},
                )
            )
        elif bool(behavior_guardrails.get("diagnostic_violation_detected")):
            manual_review_required_findings.append(
                _finding(
                    "behavior_guardrail_diagnostic",
                    "manual_review_required",
                    "Diagnostic-only behavior guardrails need human review before manual testing.",
                    evidence={**evidence, "behavior_guardrails": behavior_guardrails},
                )
            )

        parity = dict(provider_report.get("board_runtime_state_parity") or {})
        if parity:
            parity_report_count += 1
            if not bool(parity.get("consistent")):
                blocking_findings.append(
                    _finding(
                        "board_runtime_state_cross_endpoint_drift",
                        "blocking",
                        "Public endpoints expose inconsistent board_runtime_state.",
                        evidence={**evidence, "board_runtime_state_parity": parity},
                    )
                )
        else:
            blocking_findings.append(
                _finding(
                    "board_runtime_state_parity_missing",
                    "blocking",
                    "Cross-endpoint board runtime parity report is missing.",
                    evidence=evidence,
                )
            )
        board_runtime_state = dict(dict(provider_report.get("board") or {}).get("board_runtime_state") or {})
        blocking_findings.extend(
            _pre_manual_board_runtime_semantic_findings(
                case_evidence=evidence,
                board_runtime_state=board_runtime_state,
            )
        )

        workflow_wall_clock = dict(provider_report.get("workflow_wall_clock_ms") or {})
        blocking_findings.extend(
            _pre_manual_workflow_wall_clock_findings(
                case_evidence=evidence,
                workflow_wall_clock=workflow_wall_clock,
                expectations=expectations,
            )
        )

        target_public_web_metrics = dict(service_metrics.get("target_candidate_public_web") or {})
        target_public_web_action = dict(record.get("target_public_web_action") or {})
        target_public_web_expected = bool(
            target_public_web_action
            or expectations.get("require_no_target_public_web_guardrail_violation")
            or expectations.get("require_crm_public_web_storage_owner")
            or expectations.get("require_crm_public_web_queue_batch_command")
            or expectations.get("require_public_web_execution_backend_report")
            or "max_target_public_web_legacy_storage_owner_batch_count" in expectations
            or "max_target_public_web_execution_backend_bridge_count" in expectations
        )
        if target_public_web_expected:
            if not bool(target_public_web_metrics.get("report_available")):
                blocking_findings.append(
                    _finding(
                        "target_public_web_contract_report_missing",
                        "blocking",
                        "Target Public Web action ran or was required, but service metrics did not report its owner/backend contract.",
                        evidence={**evidence, "target_public_web_action": target_public_web_action},
                    )
                )
            else:
                target_public_web_contract_report_count += 1
                if _safe_int(target_public_web_metrics.get("crm_storage_owner_batch_count")) <= 0:
                    blocking_findings.append(
                        _finding(
                            "crm_public_web_storage_owner_missing",
                            "blocking",
                            "Target Public Web service metrics did not observe any crm_public_web_v1 owner batch.",
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                if _safe_int(target_public_web_metrics.get("legacy_storage_owner_batch_count")) > 0:
                    blocking_findings.append(
                        _finding(
                            "legacy_public_web_storage_owner_used",
                            "blocking",
                            "Target Public Web normal path observed legacy storage-owner batches after CRM owner cutover.",
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                if not dict(target_public_web_metrics.get("execution_backend_counts") or {}):
                    blocking_findings.append(
                        _finding(
                            "target_public_web_execution_backend_report_missing",
                            "blocking",
                            "Target Public Web service metrics did not expose execution backend counts.",
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                elif bool(target_public_web_metrics.get("execution_backend_bridge_present")):
                    blocking_findings.append(
                        _finding(
                            "target_public_web_execution_backend_bridge_present",
                            "blocking",
                            (
                                "Target Public Web normal path observed the retired target_candidate_public_web_v1 "
                                "execution backend after the CRM-owned execution cutover."
                            ),
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                if _safe_int(target_public_web_metrics.get("queue_batch_command_succeeded_count")) <= 0:
                    blocking_findings.append(
                        _finding(
                            "crm_public_web_queue_batch_command_missing",
                            "blocking",
                            (
                                "Target Public Web normal path did not expose a succeeded "
                                "crm.public_web.queue_batch command-owner proof."
                            ),
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                if _safe_int(target_public_web_metrics.get("queue_batch_command_expected_owner_count")) <= 0:
                    blocking_findings.append(
                        _finding(
                            "crm_public_web_queue_batch_owner_missing",
                            "blocking",
                            "Target Public Web queue-batch command was not owned by crm_public_web_owner.",
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                if _safe_int(target_public_web_metrics.get("queue_batch_command_invalid_owner_count")) > 0:
                    blocking_findings.append(
                        _finding(
                            "crm_public_web_queue_batch_invalid_owner",
                            "blocking",
                            "Target Public Web queue-batch command used an owner outside the command registry contract.",
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
                if _safe_int(target_public_web_metrics.get("queue_batch_command_incomplete_causality_count")) > 0:
                    blocking_findings.append(
                        _finding(
                            "crm_public_web_queue_batch_incomplete_causality",
                            "blocking",
                            "Target Public Web queue-batch command is missing typed causality envelope fields.",
                            evidence={
                                **evidence,
                                "target_candidate_public_web": target_public_web_metrics,
                            },
                        )
                    )
        projection_cutover = dict(
            provider_report.get("projection_cutover")
            or provider_report.get("canonical_projection_cutover")
            or service_metrics.get("projection_cutover")
            or {}
        )
        if projection_cutover:
            projection_cutover_report_count += 1
            if bool(projection_cutover.get("legacy_public_reader_fallback_used")):
                blocking_findings.append(
                    _finding(
                        "legacy_public_reader_fallback_used",
                        "blocking",
                        "Normal scripted serving used a legacy public-reader fallback after projection cutover.",
                        evidence={**evidence, "projection_cutover": projection_cutover},
                    )
                )
            if bool(projection_cutover.get("projection_missing")) or not bool(
                projection_cutover.get("run_projection_link_present", True)
            ):
                blocking_findings.append(
                    _finding(
                        "canonical_projection_missing",
                        "blocking",
                        "Scripted case did not publish a linked canonical run-scope projection.",
                        evidence={**evidence, "projection_cutover": projection_cutover},
                    )
                )
            if bool(projection_cutover.get("legacy_endpoint_normal_path_used")):
                blocking_findings.append(
                    _finding(
                        "legacy_endpoint_normal_path_used",
                        "blocking",
                        "Legacy job-result endpoint remained in the normal scripted result-serving path.",
                        evidence={**evidence, "projection_cutover": projection_cutover},
                    )
                )
            if bool(projection_cutover.get("legacy_target_candidate_export_normal_path_used")):
                blocking_findings.append(
                    _finding(
                        "legacy_target_candidate_export_normal_path_used",
                        "blocking",
                        (
                            "Target-candidate export was used as a normal scripted path after projection/person-first "
                            "export cutover. Projection export must be the normal path; target-candidate export may "
                            "only remain as explicit compatibility."
                        ),
                        evidence={**evidence, "projection_cutover": projection_cutover},
                    )
                )
        elif bool(expectations.get("require_projection_cutover_report")):
            blocking_findings.append(
                _finding(
                    "projection_cutover_report_missing",
                    "blocking",
                    "Projection cutover report is required but missing.",
                    evidence=evidence,
                )
            )
        artifact_coherence = dict(
            provider_report.get("legacy_artifact_coherence")
            or service_metrics.get("legacy_artifact_coherence")
            or {}
        )
        if artifact_coherence:
            legacy_artifact_coherence_report_count += 1
            if bool(artifact_coherence.get("terminal_drift_detected")) or bool(
                artifact_coherence.get("blocking_violation")
            ):
                blocking_findings.append(
                    _finding(
                        "legacy_artifact_terminal_drift",
                        "blocking",
                        (
                            "Legacy job artifact claimed terminal completion while canonical job/progress state "
                            "was still nonterminal. Normal scripted signoff must not trust stale artifact state."
                        ),
                        evidence={**evidence, "legacy_artifact_coherence": artifact_coherence},
                    )
                )
        elif bool(expectations.get("require_legacy_artifact_coherence_report")):
            blocking_findings.append(
                _finding(
                    "legacy_artifact_coherence_report_missing",
                    "blocking",
                    "Legacy artifact/canonical job status coherence report is required but missing.",
                    evidence=evidence,
                )
            )
        legacy_materialization_write_contract = dict(
            service_metrics.get("legacy_materialization_write_contract") or {}
        )
        if legacy_materialization_write_contract:
            legacy_materialization_write_contract_report_count += 1
            if bool(expectations.get("require_no_legacy_materialization_normal_writes")) and _safe_int(
                legacy_materialization_write_contract.get("normal_path_write_count")
            ) > 0:
                blocking_findings.append(
                    _finding(
                        "legacy_materialization_normal_write_used",
                        "blocking",
                        (
                            "Normal scripted execution wrote job_materialization_items after durable runtime "
                            "command-owner cutover. New work must be planned as workflow_commands; legacy rows "
                            "may remain only as explicit migration adapter/backfill evidence."
                        ),
                        evidence={
                            **evidence,
                            "legacy_materialization_write_contract": legacy_materialization_write_contract,
                        },
                    )
                )
            if bool(expectations.get("require_legacy_materialization_write_contract_report")) and _safe_int(
                legacy_materialization_write_contract.get("missing_contract_count")
            ) > 0:
                blocking_findings.append(
                    _finding(
                        "legacy_materialization_write_contract_missing",
                        "blocking",
                        (
                            "job_materialization_items rows were present without legacy_materialization_write_contract "
                            "metadata, so signoff cannot distinguish migration adapter evidence from normal-path writes."
                        ),
                        evidence={
                            **evidence,
                            "legacy_materialization_write_contract": legacy_materialization_write_contract,
                        },
                    )
                )
        elif bool(expectations.get("require_legacy_materialization_write_contract_report")):
            blocking_findings.append(
                _finding(
                    "legacy_materialization_write_contract_report_missing",
                    "blocking",
                    "Legacy materialization write contract report is required but missing.",
                    evidence=evidence,
                )
            )
        workflow_causality_contract = dict(service_metrics.get("workflow_causality_contract") or {})
        if bool(expectations.get("require_workflow_causality_contract")):
            if not bool(workflow_causality_contract.get("report_available")):
                blocking_findings.append(
                    _finding(
                        "workflow_causality_contract_report_missing",
                        "blocking",
                        "Workflow causality contract report is required but missing.",
                        evidence=evidence,
                    )
                )
            elif bool(workflow_causality_contract.get("violation_detected")):
                blocking_findings.append(
                    _finding(
                        "workflow_causality_contract_violation",
                        "blocking",
                        (
                            "Normal-path workflow_commands must carry physical typed causality columns so W6 "
                            "validates durable execution stability instead of discovering basic command/event "
                            "provenance drift."
                        ),
                        evidence={
                            **evidence,
                            "workflow_causality_contract": workflow_causality_contract,
                        },
                    )
                )
        durable_command_owner_contracts = dict(service_metrics.get("durable_command_owner_contracts") or {})
        if durable_command_owner_contracts:
            durable_command_owner_contract_report_count += 1
        if bool(expectations.get("require_durable_command_owner_contracts")):
            if not bool(durable_command_owner_contracts.get("report_available")):
                blocking_findings.append(
                    _finding(
                        "durable_command_owner_contract_report_missing",
                        "blocking",
                        "Durable command owner contract report is required but missing.",
                        evidence=evidence,
                    )
                )
            else:
                control_policy_drift_samples = _durable_command_owner_control_policy_drift_samples(
                    durable_command_owner_contracts
                )
                if bool(durable_command_owner_contracts.get("violation_detected")):
                    blocking_findings.append(
                        _finding(
                            "durable_command_owner_contract_violation",
                            "blocking",
                            (
                                "Typed workflow_commands must be owned by their registered durable owners and carry "
                                "complete physical causality before W6 validates long-chain stability."
                            ),
                            evidence={
                                **evidence,
                                "durable_command_owner_contracts": durable_command_owner_contracts,
                            },
                        )
                    )
                if control_policy_drift_samples:
                    blocking_findings.append(
                        _finding(
                            "durable_command_control_policy_contract_violation",
                            "blocking",
                            (
                                "Active durable command owner contracts must expose complete fail-closed control "
                                "policy, including explicit running cancel/resume semantics. W6 must not discover "
                                "missing or placeholder Agent command-control fields."
                            ),
                            evidence={
                                **evidence,
                                "control_policy_drift_samples": control_policy_drift_samples,
                            },
                        )
                    )
        legacy_public_web_retirement = dict(service_metrics.get("legacy_public_web_retirement") or {})
        if bool(legacy_public_web_retirement.get("report_available")):
            legacy_public_web_retirement_report_count += 1
        if bool(expectations.get("require_legacy_public_web_retirement_ready")):
            if not bool(legacy_public_web_retirement.get("report_available")):
                blocking_findings.append(
                    _finding(
                        "legacy_public_web_retirement_report_missing",
                        "blocking",
                        "Legacy target-candidate Public Web retirement audit is required before W7e deletion/signoff.",
                        evidence=evidence,
                    )
                )
            elif bool(legacy_public_web_retirement.get("legacy_rows_present")):
                blocking_findings.append(
                    _finding(
                        "legacy_public_web_rows_present",
                        "blocking",
                        (
                            "Legacy target-candidate Public Web rows remain. Migrate valuable data or cold-backup "
                            "reviewed evidence before deleting legacy helpers/tables."
                        ),
                        evidence={
                            **evidence,
                            "legacy_public_web_retirement": legacy_public_web_retirement,
                        },
                    )
                )
            elif bool(legacy_public_web_retirement.get("legacy_audit_limited")):
                blocking_findings.append(
                    _finding(
                        "legacy_public_web_retirement_audit_limited",
                        "blocking",
                        "Legacy Public Web retirement audit was row-limit truncated and cannot approve deletion.",
                        evidence={
                            **evidence,
                            "legacy_public_web_retirement": legacy_public_web_retirement,
                        },
                    )
                )
        post_profile_completion = dict(service_metrics.get("post_profile_completion") or {})
        profile_file_to_board_patch = dict(
            post_profile_completion.get("profile_file_visible_to_board_patch_visible") or {}
        )
        if bool(expectations.get("require_no_post_profile_heuristic_slo_pairing")) and bool(
            profile_file_to_board_patch.get("heuristic_pairing_used")
        ):
            blocking_findings.append(
                _finding(
                    "post_profile_heuristic_slo_pairing_used",
                    "blocking",
                    (
                        "Post-profile SLO pairing used legacy snapshot/timestamp proximity. W6 may validate "
                        "latency and stability only after normal-path profile/local-apply/board-visible work is "
                        "paired by typed workflow command causal_group_id."
                    ),
                    evidence={
                        **evidence,
                        "profile_file_visible_to_board_patch_visible": profile_file_to_board_patch,
                    },
                )
            )
        snapshot_full_materialization_queue = dict(service_metrics.get("snapshot_full_materialization_queue") or {})
        snapshot_full_background_backlog = _safe_int(snapshot_full_materialization_queue.get("backlog_count"))
        snapshot_full_background_unhealthy = bool(
            snapshot_full_materialization_queue.get("retry_backlog_present")
        ) or bool(snapshot_full_materialization_queue.get("stale_running_present"))
        snapshot_full_terminal_failed_count = _safe_int(
            snapshot_full_materialization_queue.get("terminal_failed_count")
        )
        if bool(snapshot_full_materialization_queue.get("report_available")):
            background_maintenance_report_count += 1
        if snapshot_full_background_backlog > 0 and not snapshot_full_background_unhealthy:
            background_maintenance_pending_count += 1
        if snapshot_full_background_unhealthy or snapshot_full_terminal_failed_count > 0:
            blocking_findings.append(
                _finding(
                    "snapshot_full_materialization_background_unhealthy",
                    "blocking",
                    (
                        "Background snapshot compaction has retry, stale-running, or terminal-failed work. "
                        "Healthy queued/running compaction is allowed as background maintenance, but unhealthy "
                        "compaction requires owner-level recovery before signoff."
                    ),
                    evidence={
                        **evidence,
                        "snapshot_full_materialization_queue": snapshot_full_materialization_queue,
                    },
                )
            )
        elif snapshot_full_background_backlog > 0:
            if bool(expectations.get("require_background_snapshot_full_materialization_settled")):
                finding = _finding(
                    "snapshot_full_materialization_background_pending",
                    "blocking",
                    (
                        "This case explicitly requires background snapshot compaction to settle, but "
                        "snapshot.compaction.run work remains queued or running."
                    ),
                    evidence={
                        **evidence,
                        "snapshot_full_materialization_queue": snapshot_full_materialization_queue,
                    },
                )
                blocking_findings.append(finding)
        post_profile_completion = dict(service_metrics.get("post_profile_completion") or {})
        post_profile_completion_clean = bool(post_profile_completion.get("report_available")) and not bool(
            post_profile_completion.get("slo_violation_detected")
        )
        post_preview_finalization = dict(provider_report.get("post_preview_finalization") or {})
        provider_wait_excluded_from_finalization = (
            _safe_float(post_preview_finalization.get("profile_wait_excluded_from_finalization_gate_ms")) > 0.0
        )
        finalization_start_gate_threshold_ms = (
            _safe_float(expectations.get("max_stage1_terminal_to_finalization_start_ms"))
            or _DEFAULT_MAX_STAGE_1_PREVIEW_TO_FINAL_RESULTS_MS
        )
        raw_finalization_start_gate_ms = post_preview_finalization.get("finalization_start_gate_ms")
        finalization_start_gate_observed = raw_finalization_start_gate_ms is not None
        finalization_start_gate_ms = _safe_float(raw_finalization_start_gate_ms)
        profile_wait_only_finalization_lag = bool(
            provider_wait_excluded_from_finalization
            and post_profile_completion_clean
            and finalization_start_gate_observed
            and finalization_start_gate_ms >= 0.0
            and finalization_start_gate_ms <= finalization_start_gate_threshold_ms
        )
        if bool(post_preview_finalization.get("long_post_preview_finalization")):
            reported_post_preview_threshold_ms = _safe_float(
                post_preview_finalization.get("long_post_preview_finalization_threshold_ms")
            )
            workflow_preview_to_final_ms = _safe_float(
                workflow_wall_clock.get("stage_1_preview_to_final_results")
            )
            post_preview_completed_ms = _safe_float(
                post_preview_finalization.get("preview_to_finalization_completed_ms")
            )
            effective_preview_to_final_threshold_ms = _effective_stage_preview_to_final_threshold_ms(
                expectations=expectations,
                reported_threshold_ms=reported_post_preview_threshold_ms,
            )
            finding_evidence = {
                **evidence,
                "preview_to_finalization_completed_ms": post_preview_finalization.get(
                    "preview_to_finalization_completed_ms"
                ),
                "threshold_ms": effective_preview_to_final_threshold_ms,
                "reported_threshold_ms": reported_post_preview_threshold_ms,
                "expectation": "max_stage_1_preview_to_final_results_ms",
                "workflow_stage_1_preview_to_final_results_ms": workflow_preview_to_final_ms,
                "stage_1_preview_timestamp_source": post_preview_finalization.get(
                    "stage_1_preview_timestamp_source"
                ),
                "profile_wait_excluded_from_finalization_gate_ms": post_preview_finalization.get(
                    "profile_wait_excluded_from_finalization_gate_ms"
                ),
                "finalization_start_gate_ms": post_preview_finalization.get("finalization_start_gate_ms"),
                "finalization_start_gate_threshold_ms": finalization_start_gate_threshold_ms,
            }
            if not profile_wait_only_finalization_lag:
                if not (
                    effective_preview_to_final_threshold_ms > 0.0
                    and 0.0 < workflow_preview_to_final_ms <= effective_preview_to_final_threshold_ms
                ):
                    manual_review_required_findings.append(
                        _finding(
                            "post_preview_finalization_lag_exceeds_case_slo",
                            "manual_review_required",
                            (
                                "Post-preview finalization exceeded the case-level finalization lag SLO. "
                                "This is an optimization finding, not the manual handoff gate; board-visible "
                                "readiness remains the user-visible handoff contract."
                            ),
                            evidence=finding_evidence,
                        )
                    )
                elif (
                    reported_post_preview_threshold_ms > 0.0
                    and post_preview_completed_ms > reported_post_preview_threshold_ms
                ):
                    known_acceptable_warnings.append(
                        _finding(
                            "post_preview_finalization_timestamp_fallback_longer_than_user_visible_wall_clock",
                            "known_acceptable_warning",
                            (
                                "Low-level post-preview finalization fallback exceeded the threshold, but the "
                                "user-visible Stage 1 preview to final results wall-clock stayed within the UX SLO."
                            ),
                            evidence=finding_evidence,
                        )
                    )

        user_experience = dict(service_metrics.get("user_experience") or {})
        if bool(user_experience.get("long_finalization_after_preview")):
            user_experience_thresholds = dict(user_experience.get("thresholds_ms") or {})
            reported_user_experience_threshold_ms = _safe_float(
                user_experience_thresholds.get("stage_1_preview_to_final_results")
            )
            effective_preview_to_final_threshold_ms = _effective_stage_preview_to_final_threshold_ms(
                expectations=expectations,
                reported_threshold_ms=reported_user_experience_threshold_ms,
            )
            actual_preview_to_final_ms = _safe_float(
                user_experience.get("stage_1_preview_to_final_results_ms")
            )
            finding_evidence = {
                **evidence,
                "stage_1_preview_to_final_results_ms": user_experience.get(
                    "stage_1_preview_to_final_results_ms"
                ),
                "thresholds_ms": user_experience_thresholds,
                "threshold_ms": effective_preview_to_final_threshold_ms,
                "reported_threshold_ms": reported_user_experience_threshold_ms,
                "expectation": "max_stage_1_preview_to_final_results_ms",
                "profile_wait_excluded_from_finalization_gate_ms": post_preview_finalization.get(
                    "profile_wait_excluded_from_finalization_gate_ms"
                ),
                "finalization_start_gate_ms": post_preview_finalization.get("finalization_start_gate_ms"),
                "finalization_start_gate_threshold_ms": finalization_start_gate_threshold_ms,
            }
            if actual_preview_to_final_ms > effective_preview_to_final_threshold_ms:
                if not profile_wait_only_finalization_lag:
                    manual_review_required_findings.append(
                        _finding(
                            "user_experience_finalization_lag_exceeds_case_slo",
                            "manual_review_required",
                            (
                                "Stage-1-preview-to-final-results lag exceeded the case-level finalization SLO. "
                                "This is an internal finalization optimization finding; manual handoff readiness "
                                "must be judged from board-visible/card-visible metrics."
                            ),
                            evidence=finding_evidence,
                        )
                    )
            elif (
                reported_user_experience_threshold_ms > 0.0
                and actual_preview_to_final_ms > reported_user_experience_threshold_ms
            ):
                known_acceptable_warnings.append(
                    _finding(
                        "user_experience_finalization_exceeds_default_but_within_case_slo",
                        "known_acceptable_warning",
                        (
                            "Service metrics exceeded their default finalization threshold, but the "
                            "case-level Stage-1-preview-to-final-results SLO still passed."
                        ),
                        evidence=finding_evidence,
                    )
                )

        recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})
        if recovery_phase_metrics:
            recovery_phase_report_count += 1
            if bool(expectations.get("require_no_legacy_materialization_recovery_bridge")) and _safe_int(
                recovery_phase_metrics.get("legacy_bridge_used_count")
            ) > 0:
                blocking_findings.append(
                    _finding(
                        "legacy_materialization_recovery_bridge_used",
                        "blocking",
                        (
                            "Recovery executed a legacy job_materialization_items bridge after durable runtime "
                            "command-owner cutover. Typed workflow command owners must be the normal execution path."
                        ),
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            if bool(recovery_phase_metrics.get("missing_phase_present")):
                blocking_findings.append(
                    _finding(
                        "recovery_phase_metrics_missing",
                        "blocking",
                        "Worker recovery ran without complete phase-level owner/timing evidence.",
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            if bool(recovery_phase_metrics.get("failed_phase_present")):
                blocking_findings.append(
                    _finding(
                        "recovery_phase_failed",
                        "blocking",
                        "A worker recovery phase failed and must be resolved before manual testing.",
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            if bool(recovery_phase_metrics.get("slow_phase_present")):
                blocking_findings.append(
                    _finding(
                        "recovery_phase_slow",
                        "blocking",
                        "A worker recovery phase exceeded the bounded synchronous work SLO.",
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            cooperative_budget_yield = _safe_int(
                recovery_phase_metrics.get("cooperative_budget_yield_count")
            )
            slow_total_phase_count = _safe_int(recovery_phase_metrics.get("slow_total_phase_count"))
            if cooperative_budget_yield > 0:
                cooperative_budget_yield_report_count += 1
                cooperative_budget_yield_count += cooperative_budget_yield
            if bool(recovery_phase_metrics.get("slow_total_phase_present")) and (
                slow_total_phase_count > cooperative_budget_yield
            ):
                known_acceptable_warnings.append(
                    _finding(
                        "recovery_total_elapsed_slow",
                        "known_acceptable_warning",
                        (
                            "A recovery tick exceeded the default total elapsed optimization threshold, "
                            "but no individual recovery phase exceeded the bounded synchronous-work SLO."
                        ),
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            if _safe_int(recovery_phase_metrics.get("recovery_tick_budget_exhausted_count")) > 0 and not bool(
                recovery_phase_metrics.get("cooperative_budget_yield_present")
            ):
                known_acceptable_warnings.append(
                    _finding(
                        "recovery_tick_budget_exhausted",
                        "known_acceptable_warning",
                        (
                            "A recovery tick yielded after the configured total budget; this is acceptable only "
                            "when follow-up ticks continue draining durable work and case SLOs pass."
                        ),
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            cooperative_handoff_yield = _safe_int(
                recovery_phase_metrics.get("cooperative_handoff_yield_count")
            )
            if cooperative_handoff_yield > 0:
                cooperative_handoff_yield_report_count += 1
                cooperative_handoff_yield_count += cooperative_handoff_yield
            elif _safe_int(recovery_phase_metrics.get("durable_work_handoff_yield_count")) > 0:
                known_acceptable_warnings.append(
                    _finding(
                        "recovery_durable_work_handoff_yield",
                        "known_acceptable_warning",
                        (
                            "Recovery intentionally yielded after recording terminal/provider work; this is the "
                            "expected bounded handoff as long as durable queues continue draining and case SLOs pass."
                        ),
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
            if bool(recovery_phase_metrics.get("unexpected_enabled_phase_present")):
                blocking_findings.append(
                    _finding(
                        "recovery_unexpected_enabled_phase",
                        "blocking",
                        "Job-scoped recovery ran a phase outside its ownership contract.",
                        evidence={**evidence, "recovery_phase_metrics": recovery_phase_metrics},
                    )
                )
        elif _case_has_recovery_work(record):
            blocking_findings.append(
                _finding(
                    "recovery_phase_metrics_report_missing",
                    "blocking",
                    "Worker recovery evidence is present but recovery phase metrics are missing.",
                    evidence=evidence,
                )
            )

        post_profile = dict(service_metrics.get("post_profile_completion") or {})
        if post_profile:
            post_profile_report_count += 1
            if bool(post_profile.get("slo_violation_detected")):
                blocking_findings.append(
                    _finding(
                        "post_profile_completion_slo_violation",
                        "blocking",
                        "Post-profile completion SLO report detected a violation.",
                        evidence={**evidence, "post_profile_completion": post_profile},
                    )
                )
            missing_post_profile_sections = _missing_post_profile_sections(post_profile)
            if canonical_projection_proof["complete"]:
                missing_post_profile_sections = [
                    section
                    for section in missing_post_profile_sections
                    if section != "profile_file_visible_to_board_patch_visible"
                ]
            if missing_post_profile_sections and _case_has_profile_provider_work(record):
                blocking_findings.append(
                    _finding(
                        "post_profile_completion_report_incomplete",
                        "blocking",
                        "Provider-backed profile work is present but post-profile SLO report is incomplete.",
                        evidence={**evidence, "missing_sections": missing_post_profile_sections},
                    )
                )
        elif _case_has_profile_provider_work(record):
            blocking_findings.append(
                _finding(
                    "post_profile_completion_report_missing",
                    "blocking",
                    "Provider-backed profile work is present but post-profile SLO report is missing.",
                    evidence=evidence,
                )
            )

        event_efficiency = dict(provider_report.get("event_level_efficiency") or {})
        profile_scheduler_contract = dict(event_efficiency.get("profile_scheduler_contract") or {})
        if profile_scheduler_contract:
            profile_scheduler_report_count += 1
            if bool(profile_scheduler_contract.get("violation_detected")):
                blocking_findings.append(
                    _finding(
                        "profile_scheduler_contract_violation",
                        "blocking",
                        "Profile scheduler contract reported a violation.",
                        evidence={**evidence, "profile_scheduler_contract": profile_scheduler_contract},
                    )
                )
        elif _case_has_profile_provider_work(record):
            blocking_findings.append(
                _finding(
                    "profile_scheduler_contract_report_missing",
                    "blocking",
                    "Provider-backed profile work is present but profile scheduler contract report is missing.",
                    evidence=evidence,
                )
            )
        blocking_findings.extend(
            _pre_manual_event_efficiency_findings(
                case_evidence=evidence,
                event_efficiency=event_efficiency,
                expectations=expectations,
            )
        )

        board_projection = dict(service_metrics.get("board_visible_projection") or {})
        if bool(board_projection.get("projection_missing_for_visible_count")) or bool(
            board_projection.get("patch_log_replay_lag")
        ):
            blocking_findings.append(
                _finding(
                    "board_visible_projection_not_replayable",
                    "blocking",
                    "Board-visible counts are not backed by a replayable projection/patch log.",
                    evidence={**evidence, "board_visible_projection": board_projection},
                )
            )
        if bool(board_projection.get("overlay_write_mode_missing_present")):
            blocking_findings.append(
                _finding(
                    "partial_overlay_write_mode_report_missing",
                    "blocking",
                    "Partial board-visible delta patches are present but overlay write-mode evidence is missing.",
                    evidence={**evidence, "board_visible_projection": board_projection},
                )
            )
        board_overlay_writes = dict(service_metrics.get("board_overlay_writes") or {})
        if bool(board_overlay_writes.get("eligible_full_rebuild_fallback_present")):
            blocking_findings.append(
                _finding(
                    "partial_overlay_fast_path_fallback",
                    "blocking",
                    (
                        "Partial board overlay fast path was eligible but fell back to full rebuild. "
                        "This keeps pressure runs correct but reintroduces the old O(full board) path."
                    ),
                    evidence={**evidence, "board_overlay_writes": board_overlay_writes},
                )
            )
        finalization_overlay = dict(service_metrics.get("finalization_overlay") or {})
        if bool(finalization_overlay.get("eligible_full_rewrite_present")):
            finding = _finding(
                "finalization_overlay_reuse_missed",
                "blocking",
                (
                    "Final asset-population publication rewrote the full overlay even though the "
                    "canonical board-visible projection was already complete."
                ),
                evidence={**evidence, "finalization_overlay": finalization_overlay},
            )
            if _legacy_cutover_contract_clean(service_metrics=service_metrics) and canonical_projection_proof[
                "board_projection_clean"
            ]:
                known_acceptable_warnings.append({**finding, "severity": "known_acceptable_warning"})
            else:
                blocking_findings.append(finding)
        overlay_candidate_count = _safe_int(finalization_overlay.get("overlay_candidate_count"))
        candidate_source_count = _safe_int(finalization_overlay.get("candidate_source_count"))
        if (
            bool(finalization_overlay.get("reuse_used"))
            and overlay_candidate_count > 0
            and candidate_source_count > 0
            and overlay_candidate_count != candidate_source_count
        ):
            blocking_findings.append(
                _finding(
                    "finalization_overlay_candidate_count_drift",
                    "blocking",
                    (
                        "Final asset-population publication reused the board-visible projection, "
                        "but the final summary candidate_source count does not match that serving projection."
                    ),
                    evidence={**evidence, "finalization_overlay": finalization_overlay},
                )
            )

    if not records:
        blocking_findings.append(
            _finding(
                "smoke_report_empty",
                "blocking",
                "Smoke report contains no cases.",
                evidence={},
            )
        )

    if provider_mode_failure_count == 0:
        passed_gates.append(
            _passed_gate(
                "provider_mode_no_cost",
                f"All {provider_invocation_count} provider invocations used {normalized_expected_mode}.",
            )
        )
    if smoke_ready_count == len(records) and records:
        passed_gates.append(_passed_gate("smoke_ready", f"All {smoke_ready_count} smoke cases are ready."))
    if parity_report_count > 0:
        passed_gates.append(
            _passed_gate("board_runtime_state_parity", f"{parity_report_count} parity reports were present.")
        )
    if post_profile_report_count > 0:
        passed_gates.append(
            _passed_gate("post_profile_completion_slo", f"{post_profile_report_count} post-profile reports were present.")
        )
    if recovery_phase_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "recovery_phase_metrics",
                f"{recovery_phase_report_count} recovery phase metric reports were present.",
            )
        )
    if profile_scheduler_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "profile_scheduler_contract",
                f"{profile_scheduler_report_count} profile scheduler contract reports were present.",
            )
        )
    if projection_cutover_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "projection_cutover",
                f"{projection_cutover_report_count} projection cutover reports were present.",
            )
        )
    if legacy_artifact_coherence_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "legacy_artifact_coherence",
                f"{legacy_artifact_coherence_report_count} legacy artifact coherence reports were present.",
            )
        )
    if legacy_materialization_write_contract_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "legacy_materialization_write_contract",
                (
                    f"{legacy_materialization_write_contract_report_count} legacy materialization write contract "
                    "reports were present."
                ),
            )
        )
    if durable_command_owner_contract_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "durable_command_owner_contracts",
                f"{durable_command_owner_contract_report_count} durable command owner contract reports were present.",
            )
        )
    if legacy_public_web_retirement_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "legacy_public_web_retirement",
                f"{legacy_public_web_retirement_report_count} legacy Public Web retirement reports were present.",
            )
        )
    if target_public_web_contract_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "target_public_web_contract",
                f"{target_public_web_contract_report_count} target Public Web owner/backend reports were present.",
            )
        )
    if background_maintenance_report_count > 0:
        message = (
            f"{background_maintenance_report_count} background-maintenance reports were present; "
            f"{background_maintenance_pending_count} reported healthy pending snapshot compaction."
        )
        passed_gates.append(_passed_gate("background_maintenance_snapshot_compaction", message))
    if cooperative_handoff_yield_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "recovery_cooperative_handoff_yield",
                (
                    f"{cooperative_handoff_yield_report_count} recovery reports observed "
                    f"{cooperative_handoff_yield_count} cooperative handoff yields."
                ),
            )
        )
    if cooperative_budget_yield_report_count > 0:
        passed_gates.append(
            _passed_gate(
                "recovery_cooperative_budget_yield",
                (
                    f"{cooperative_budget_yield_report_count} recovery reports observed "
                    f"{cooperative_budget_yield_count} cooperative budget yields."
                ),
            )
        )

    summary_payload = dict(summary or {})
    if summary_payload:
        passed_gates.append(_passed_gate("aggregate_summary_loaded", "Aggregate smoke summary was loaded."))

    gate_layers = _summarize_gate_layers(
        blocking_findings=blocking_findings,
        manual_review_required_findings=manual_review_required_findings,
        known_acceptable_warnings=known_acceptable_warnings,
    )

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "status": "blocked" if blocking_findings else "passed",
        "case_count": len(records),
        "gate_layers": gate_layers,
        "passed_gates": passed_gates,
        "blocking_findings": blocking_findings,
        "manual_review_required_findings": manual_review_required_findings,
        "known_acceptable_warnings": known_acceptable_warnings,
        "summary": {
            "provider_invocation_count": provider_invocation_count,
            "provider_mode_failure_count": provider_mode_failure_count,
            "smoke_ready_count": smoke_ready_count,
            "board_runtime_state_parity_report_count": parity_report_count,
            "post_profile_completion_report_count": post_profile_report_count,
            "profile_scheduler_contract_report_count": profile_scheduler_report_count,
            "recovery_phase_metrics_report_count": recovery_phase_report_count,
            "projection_cutover_report_count": projection_cutover_report_count,
            "legacy_artifact_coherence_report_count": legacy_artifact_coherence_report_count,
            "legacy_materialization_write_contract_report_count": (
                legacy_materialization_write_contract_report_count
            ),
            "durable_command_owner_contract_report_count": durable_command_owner_contract_report_count,
            "legacy_public_web_retirement_report_count": legacy_public_web_retirement_report_count,
            "target_public_web_contract_report_count": target_public_web_contract_report_count,
            "background_maintenance_report_count": background_maintenance_report_count,
            "background_maintenance_pending_count": background_maintenance_pending_count,
            "cooperative_handoff_yield_report_count": cooperative_handoff_yield_report_count,
            "cooperative_handoff_yield_count": cooperative_handoff_yield_count,
            "cooperative_budget_yield_report_count": cooperative_budget_yield_report_count,
            "cooperative_budget_yield_count": cooperative_budget_yield_count,
        },
        "aggregate_summary": summary_payload,
    }


def render_scripted_smoke_signoff_markdown(report: dict[str, Any]) -> str:
    payload = dict(report or {})
    lines = [
        "# Pre-Manual Scripted Smoke Signoff",
        "",
        f"- status: `{payload.get('status') or 'unknown'}`",
        f"- case_count: `{payload.get('case_count') or 0}`",
        f"- generated_at: `{payload.get('generated_at') or ''}`",
        "",
    ]
    gate_layers = {
        str(key): dict(value)
        for key, value in dict(payload.get("gate_layers") or {}).items()
        if isinstance(value, dict)
    }
    if gate_layers:
        lines.append("## Gate Layers")
        for layer_name, layer_payload in sorted(gate_layers.items()):
            lines.append(
                "- "
                f"`{layer_name}`: status=`{layer_payload.get('status') or 'unknown'}`, "
                f"blocking={int(layer_payload.get('blocking_count') or 0)}, "
                f"manual_review={int(layer_payload.get('manual_review_required_count') or 0)}, "
                f"warnings={int(layer_payload.get('known_acceptable_warning_count') or 0)}"
            )
        lines.append("")
    for section_key, title in (
        ("blocking_findings", "Blocking Findings"),
        ("manual_review_required_findings", "Manual Review Required Findings"),
        ("known_acceptable_warnings", "Known Acceptable Warnings"),
        ("passed_gates", "Passed Gates"),
    ):
        lines.append(f"## {title}")
        items = [dict(item) for item in list(payload.get(section_key) or []) if isinstance(item, dict)]
        if not items:
            lines.append("- none")
            lines.append("")
            continue
        for item in items:
            name = str(item.get("name") or item.get("gate") or "unnamed").strip()
            message = str(item.get("message") or "").strip()
            lines.append(f"- `{name}`: {message}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def load_scripted_smoke_records(path: str | Path) -> list[dict[str, Any]]:
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        records = payload.get("records") or payload.get("cases") or payload.get("summaries")
        if isinstance(records, list):
            return [dict(item) for item in records if isinstance(item, dict)]
    raise ValueError("smoke report must be a list, or an object with records/cases/summaries")


def load_optional_json(path: str | Path | None) -> dict[str, Any]:
    if not path or not str(path).strip():
        return {}
    payload = json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    return dict(payload) if isinstance(payload, dict) else {}


def _case_has_profile_provider_work(record: dict[str, Any]) -> bool:
    provider_report = dict(record.get("provider_case_report") or {})
    profile_completion = dict(provider_report.get("profile_completion") or {})
    workflow_benchmark = dict(provider_report.get("workflow_benchmark") or {})
    if _safe_int(profile_completion.get("fetched_profile_count")) > 0:
        return True
    if _safe_int(workflow_benchmark.get("fetched_profile_count")) > 0:
        return True
    for invocation in list(record.get("provider_invocations") or []):
        if not isinstance(invocation, dict):
            continue
        logical_name = str(invocation.get("logical_name") or invocation.get("provider_name") or "").strip()
        if "profile" in logical_name:
            return True
    return False


def _case_has_recovery_work(record: dict[str, Any]) -> bool:
    if list(record.get("worker_recovery") or []):
        return True
    if list(record.get("target_public_web_action_recovery") or []):
        return True
    remote_driver = dict(record.get("remote_provider_event_driver") or {})
    for event in list(remote_driver.get("events") or []):
        if isinstance(event, dict) and (
            _safe_int(event.get("recovery_count")) > 0
            or _safe_int(event.get("recovery_dispatch_count")) > 0
        ):
            return True
    final = dict(record.get("final") or {})
    if str(final.get("smoke_completion_state") or "").strip() in {"timeout", "timed_out", "incomplete"}:
        return True
    return False


def _effective_stage_preview_to_final_threshold_ms(
    *,
    expectations: dict[str, Any],
    reported_threshold_ms: float = 0.0,
) -> float:
    configured_threshold_ms = _safe_float(expectations.get("max_stage_1_preview_to_final_results_ms"))
    if configured_threshold_ms > 0.0:
        return configured_threshold_ms
    if reported_threshold_ms > 0.0:
        return reported_threshold_ms
    return _DEFAULT_MAX_STAGE_1_PREVIEW_TO_FINAL_RESULTS_MS


def _pre_manual_workflow_wall_clock_findings(
    *,
    case_evidence: dict[str, Any],
    workflow_wall_clock: dict[str, Any],
    expectations: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    metric_specs = (
        (
            "job_to_stage_1_preview",
            "max_job_to_stage_1_preview_ms",
            _DEFAULT_MAX_JOB_TO_STAGE_1_PREVIEW_MS,
            "workflow_job_to_stage_1_preview_too_slow_for_manual_signoff",
            "Job-to-Stage-1-preview latency is too slow for manual handoff.",
        ),
        (
            "job_to_final_results",
            "max_job_to_final_results_ms",
            _DEFAULT_MAX_JOB_TO_FINAL_RESULTS_MS,
            "workflow_job_to_final_results_too_slow_for_manual_signoff",
            "Job-to-final-results latency is too slow for manual handoff.",
        ),
        (
            "job_to_board_nonempty",
            "max_job_to_board_nonempty_ms",
            0.0,
            "workflow_job_to_board_nonempty_too_slow_for_manual_signoff",
            "Job-to-board-nonempty latency exceeded the configured handoff SLO.",
        ),
        (
            "job_to_board_visible_partial",
            "max_job_to_board_visible_partial_ms",
            0.0,
            "workflow_job_to_board_visible_partial_too_slow_for_manual_signoff",
            "Job-to-board-visible-partial latency exceeded the configured handoff SLO.",
        ),
    )
    for metric_name, expectation_name, default_threshold, finding_name, message in metric_specs:
        threshold_value, threshold_expectation = _effective_workflow_wall_clock_threshold_ms(
            metric_name=metric_name,
            expectation_name=expectation_name,
            default_threshold_ms=default_threshold,
            expectations=expectations,
        )
        if threshold_value <= 0.0:
            continue
        if metric_name not in workflow_wall_clock:
            findings.append(
                _finding(
                    f"{finding_name}_missing_metric",
                    "blocking",
                    "Configured or default pre-manual workflow wall-clock SLO is missing its report metric.",
                    evidence={
                        **case_evidence,
                        "metric": metric_name,
                        "expectation": threshold_expectation,
                        "threshold_ms": threshold_value,
                    },
                )
            )
            continue
        actual_value = _safe_float(workflow_wall_clock.get(metric_name))
        if actual_value > threshold_value:
            findings.append(
                _finding(
                    finding_name,
                    "blocking",
                    message,
                    evidence={
                        **case_evidence,
                        "metric": metric_name,
                        "actual_ms": actual_value,
                        "threshold_ms": threshold_value,
                        "expectation": threshold_expectation,
                    },
                )
            )
    return findings


def _effective_workflow_wall_clock_threshold_ms(
    *,
    metric_name: str,
    expectation_name: str,
    default_threshold_ms: float,
    expectations: dict[str, Any],
) -> tuple[float, str]:
    configured_threshold_ms = _safe_float(expectations.get(expectation_name))
    if configured_threshold_ms > 0.0:
        return configured_threshold_ms, expectation_name
    if metric_name == "job_to_final_results":
        job_to_preview_ms = _safe_float(expectations.get("max_job_to_stage_1_preview_ms"))
        preview_to_final_ms = _safe_float(expectations.get("max_stage_1_preview_to_final_results_ms"))
        if job_to_preview_ms > 0.0 and preview_to_final_ms > 0.0:
            return (
                job_to_preview_ms + preview_to_final_ms,
                "max_job_to_stage_1_preview_ms+max_stage_1_preview_to_final_results_ms",
            )
    return default_threshold_ms, expectation_name


def _pre_manual_board_runtime_semantic_findings(
    *,
    case_evidence: dict[str, Any],
    board_runtime_state: dict[str, Any],
) -> list[dict[str, Any]]:
    payload = dict(board_runtime_state or {})
    if not payload:
        return []
    profile_required = _safe_int(payload.get("profile_fetch_required_count"))
    profile_fetched = _safe_int(payload.get("profile_fetched_count"))
    if profile_required <= 0 or profile_fetched < profile_required:
        return []
    if str(payload.get("publication_status") or "").strip() != "complete":
        return []

    baseline_count = _safe_int(payload.get("baseline_candidate_count"))
    display_ready_count = _safe_int(payload.get("display_ready_candidate_count"))
    explicit_capture_count = _safe_int(payload.get("explicit_profile_capture_candidate_count"))
    delta_required = _safe_int(payload.get("delta_profile_required_count"))
    if delta_required > 0:
        required = delta_required
        card_ready = min(
            required,
            max(
                _safe_int(payload.get("delta_profile_board_visible_count")),
                _safe_int(payload.get("delta_profile_materialized_count")),
                max(0, display_ready_count - baseline_count),
                max(0, explicit_capture_count - baseline_count),
            ),
        )
    else:
        required = profile_required
        card_ready = min(required, display_ready_count)
    if card_ready >= required:
        return []
    return [
        _finding(
            "board_visible_projection_terminal_profile_card_drift",
            "blocking",
            "Board runtime state reports terminal profile fetch but card-visible progress is behind.",
            evidence={
                **case_evidence,
                "profile_fetch_required_count": profile_required,
                "profile_fetched_count": profile_fetched,
                "card_ready_count": card_ready,
                "card_ready_required_count": required,
                "display_ready_candidate_count": display_ready_count,
                "profile_fetch_status_text": payload.get("profile_fetch_status_text"),
                "card_materialization_status_text": payload.get("card_materialization_status_text"),
                "publication_status": payload.get("publication_status"),
            },
        )
    ]


def _pre_manual_event_efficiency_findings(
    *,
    case_evidence: dict[str, Any],
    event_efficiency: dict[str, Any],
    expectations: dict[str, Any],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    metric_specs = (
        (
            "remote_to_local_marker_lag_ms",
            "max_remote_to_local_marker_lag_ms",
            _DEFAULT_MAX_REMOTE_TO_LOCAL_MARKER_LAG_MS,
            "event_efficiency_remote_to_local_marker_too_slow_for_manual_signoff",
            "Remote-terminal-event-to-local-terminal-marker latency exceeded the configured handoff SLO.",
        ),
        (
            "remote_to_next_submit_start_ms",
            "max_remote_to_next_submit_start_ms",
            0.0,
            "event_efficiency_remote_to_next_submit_too_slow_for_manual_signoff",
            "Remote-completion-to-next-submit latency exceeded the configured handoff SLO.",
        ),
        (
            "local_completion_to_next_submit_start_ms",
            "max_local_to_next_submit_start_ms",
            0.0,
            "event_efficiency_local_to_next_submit_too_slow_for_manual_signoff",
            "Local-completion-to-next-submit latency exceeded the configured handoff SLO.",
        ),
        (
            "next_submit_provider_attempt_elapsed_ms",
            "max_next_submit_attempt_elapsed_ms",
            0.0,
            "event_efficiency_next_submit_attempt_too_slow_for_manual_signoff",
            "Next provider submit hot-path latency exceeded the configured handoff SLO.",
        ),
        (
            "provider_slot_to_remote_wait_started_ms",
            "max_provider_slot_to_remote_wait_started_ms",
            _DEFAULT_MAX_PROVIDER_SLOT_TO_REMOTE_WAIT_STARTED_MS,
            "event_efficiency_provider_slot_to_remote_wait_too_slow_for_manual_signoff",
            "Provider-slot-acquired-to-remote-wait-start latency exceeded the handoff SLO.",
        ),
    )
    next_submit_opportunity = dict(event_efficiency.get("next_submit_opportunity") or {})
    next_submit_metric_not_applicable = next_submit_opportunity.get("applicable") is False
    for metric_name, expectation_name, default_threshold, finding_name, message in metric_specs:
        threshold_value = _safe_float(expectations.get(expectation_name)) or default_threshold
        if threshold_value <= 0.0:
            continue
        metric_payload = dict(event_efficiency.get(metric_name) or {})
        if next_submit_metric_not_applicable and metric_name in {
            "remote_to_next_submit_start_ms",
            "local_completion_to_next_submit_start_ms",
            "next_submit_provider_attempt_elapsed_ms",
            "next_submit_attempt_elapsed_ms",
        }:
            continue
        if "max" not in metric_payload:
            findings.append(
                _finding(
                    f"{finding_name}_missing_metric",
                    "blocking",
                    "Configured pre-manual event-efficiency SLO is missing its report metric.",
                    evidence={
                        **case_evidence,
                        "metric": metric_name,
                        "expectation": expectation_name,
                        "threshold_ms": threshold_value,
                    },
                )
            )
            continue
        actual_value = _safe_float(metric_payload.get("max"))
        if actual_value > threshold_value:
            findings.append(
                _finding(
                    finding_name,
                    "blocking",
                    message,
                    evidence={
                        **case_evidence,
                        "metric": metric_name,
                        "actual_ms": actual_value,
                        "threshold_ms": threshold_value,
                        "expectation": expectation_name,
                    },
                )
            )
    return findings


def _missing_post_profile_sections(post_profile: dict[str, Any]) -> list[str]:
    payload = dict(post_profile or {})
    required_sections = (
        "url_terminal_state_recording",
        "event_level_callback",
        "profile_file_visible_to_board_patch_visible",
        "all_profiles_fetched_to_all_cards_visible",
    )
    missing: list[str] = []
    for section in required_sections:
        section_payload = dict(payload.get(section) or {})
        if not section_payload:
            missing.append(section)
            continue
        if section == "url_terminal_state_recording":
            if "terminal_queue_state_leak_count" not in section_payload:
                missing.append(section)
        else:
            elapsed = dict(section_payload.get("elapsed_ms") or {})
            if not elapsed and "elapsed_ms" not in section_payload:
                missing.append(section)
    return missing


def _canonical_projection_completion_proof(
    *,
    provider_report: dict[str, Any],
    service_metrics: dict[str, Any],
) -> dict[str, Any]:
    projection_cutover = dict(provider_report.get("projection_cutover") or {})
    board_projection = dict(service_metrics.get("board_visible_projection") or {})
    post_profile = dict(service_metrics.get("post_profile_completion") or {})
    finalization_overlay = dict(service_metrics.get("finalization_overlay") or {})

    expected_count = max(
        _safe_int(board_projection.get("expected_candidate_count")),
        _safe_int(board_projection.get("served_candidate_count")),
        _safe_int(finalization_overlay.get("expected_candidate_count")),
        _safe_int(finalization_overlay.get("served_candidate_count")),
    )
    served_count = max(
        _safe_int(board_projection.get("served_candidate_count")),
        _safe_int(finalization_overlay.get("served_candidate_count")),
    )
    delta_required = max(
        _safe_int(board_projection.get("delta_profile_required_count")),
        _safe_int(finalization_overlay.get("delta_profile_required_count")),
    )
    delta_board_visible = max(
        _safe_int(board_projection.get("delta_profile_board_visible_count")),
        _safe_int(finalization_overlay.get("delta_profile_board_visible_count")),
    )
    delta_materialized = max(
        _safe_int(board_projection.get("delta_profile_materialized_count")),
        _safe_int(finalization_overlay.get("delta_profile_materialized_count")),
    )
    canonical_reader_clean = bool(
        projection_cutover.get("report_available")
        and projection_cutover.get("run_projection_link_present", True)
        and not projection_cutover.get("projection_missing")
        and not projection_cutover.get("legacy_public_reader_fallback_used")
        and not projection_cutover.get("legacy_endpoint_normal_path_used")
    )
    board_projection_clean = bool(
        board_projection.get("report_available")
        and not board_projection.get("projection_missing_for_visible_count")
        and not board_projection.get("patch_log_replay_lag")
        and not board_projection.get("patch_log_required_missing")
        and not board_projection.get("materialization_lag_violation")
        and (not board_projection.get("patch_sequence_values") or board_projection.get("patch_sequence_contiguous", True))
        and served_count > 0
        and (expected_count <= 0 or served_count >= expected_count)
        and (delta_required <= 0 or min(delta_board_visible, delta_materialized) >= delta_required)
    )
    finalization_clean = bool(
        not finalization_overlay
        or (
            finalization_overlay.get("report_available")
            and not finalization_overlay.get("eligible_full_rewrite_present")
            and (
                not finalization_overlay.get("reuse_eligible")
                or finalization_overlay.get("reuse_used")
                or finalization_overlay.get("served_complete")
            )
        )
    )
    post_profile_clean = bool(
        post_profile.get("report_available")
        and not post_profile.get("slo_violation_detected")
        and _safe_int(
            dict(post_profile.get("url_terminal_state_recording") or {}).get("terminal_queue_state_leak_count")
        )
        == 0
        and bool(dict(post_profile.get("all_profiles_fetched_to_all_cards_visible") or {}).get("elapsed_ms"))
    )
    legacy_clean = _legacy_cutover_contract_clean(service_metrics=service_metrics)
    serving_complete = bool(
        canonical_reader_clean
        and board_projection_clean
        and post_profile_clean
        and legacy_clean
    )
    return {
        "complete": serving_complete,
        "serving_complete": serving_complete,
        "canonical_reader_clean": canonical_reader_clean,
        "board_projection_clean": board_projection_clean,
        "finalization_clean": finalization_clean,
        "post_profile_clean": post_profile_clean,
        "legacy_clean": legacy_clean,
        "expected_candidate_count": expected_count,
        "served_candidate_count": served_count,
        "delta_profile_required_count": delta_required,
        "delta_profile_board_visible_count": delta_board_visible,
        "delta_profile_materialized_count": delta_materialized,
    }


def _legacy_cutover_contract_clean(*, service_metrics: dict[str, Any]) -> bool:
    legacy_write_contract = dict(service_metrics.get("legacy_materialization_write_contract") or {})
    recovery_phase_metrics = dict(service_metrics.get("recovery_phase_metrics") or {})
    return bool(
        legacy_write_contract.get("report_available")
        and _safe_int(legacy_write_contract.get("normal_path_write_count")) == 0
        and recovery_phase_metrics.get("report_available")
        and _safe_int(recovery_phase_metrics.get("legacy_bridge_used_count")) == 0
        and not recovery_phase_metrics.get("legacy_bridge_used_present")
    )


def _is_covered_by_canonical_projection_proof(
    *,
    failure: str,
    canonical_projection_proof: dict[str, Any],
) -> bool:
    text = str(failure or "").strip()
    if not text or not bool(canonical_projection_proof.get("complete")):
        return False
    covered_fragments = (
        "post_preview_finalization_observed: materialize_completed_count=0",
        "post_preview_finalization_observed: report unavailable",
        "post_preview_finalization.stage1_terminal_to_finalization_start_ms: report unavailable",
        "service_metrics.post_profile_completion.profile_file_visible_to_board_patch_visible.elapsed_ms: metric missing",
        "materialize_completed_count: expected >=",
    )
    return any(fragment in text for fragment in covered_fragments)


def _durable_command_owner_control_policy_drift_samples(
    durable_command_owner_contracts: dict[str, Any],
) -> list[dict[str, Any]]:
    drift_samples: list[dict[str, Any]] = []
    contracts = dict(durable_command_owner_contracts.get("contracts") or {})
    for metric_key, raw_contract in sorted(contracts.items()):
        contract = dict(raw_contract or {})
        if _safe_int(contract.get("command_count")) <= 0:
            continue
        missing_or_invalid_fields = [
            f"control_policy.{field_name}"
            for field_name in _durable_command_control_policy_missing_fields(contract)
        ]
        missing_or_invalid_fields.extend(
            f"display_contract.{field_name}"
            for field_name in _durable_command_display_contract_missing_fields(contract)
        )
        missing_or_invalid_fields.extend(
            f"activity_spine_policy.{field_name}"
            for field_name in _durable_command_activity_spine_policy_missing_fields(contract)
        )
        if missing_or_invalid_fields:
            drift_samples.append(
                {
                    "metric_key": metric_key,
                    "command_type": str(contract.get("command_type") or "").strip(),
                    "expected_owner": str(contract.get("expected_owner") or "").strip(),
                    "command_count": _safe_int(contract.get("command_count")),
                    "missing_or_invalid_fields": missing_or_invalid_fields,
                }
            )
        if len(drift_samples) >= 5:
            break
    return drift_samples


def _durable_command_control_policy_missing_fields(contract: dict[str, Any]) -> list[str]:
    expected_command_type = str(contract.get("command_type") or "").strip()
    expected_owner = str(contract.get("expected_owner") or "").strip()
    policy = dict(contract.get("control_policy") or {})
    if not policy:
        return ["control_policy"]

    missing_or_invalid: list[str] = []
    required_text_fields = (
        "schema_version",
        "command_type",
        "owner",
        "generic_control_contract",
        "running_cancel_contract",
        "running_resume_contract",
        "control_source_of_truth",
        "agent_callable_surface",
        "fallback_status",
    )
    for field_name in required_text_fields:
        if not str(policy.get(field_name) or "").strip():
            missing_or_invalid.append(field_name)
    if str(policy.get("command_type") or "").strip() != expected_command_type:
        missing_or_invalid.append("command_type_matches_contract")
    if str(policy.get("owner") or "").strip() != expected_owner:
        missing_or_invalid.append("owner_matches_contract")
    if str(policy.get("fallback_status") or "").strip() != "fail_closed":
        missing_or_invalid.append("fallback_status_fail_closed")
    if (
        str(policy.get("control_source_of_truth") or "").strip()
        != "durable_runtime.workflow_command_control_policy"
    ):
        missing_or_invalid.append("control_source_of_truth")
    if str(policy.get("agent_callable_surface") or "").strip() != "workflow_command_control_api":
        missing_or_invalid.append("agent_callable_surface")

    required_list_fields = (
        "generic_cancel_statuses",
        "generic_retry_statuses",
        "generic_resume_statuses",
    )
    for field_name in required_list_fields:
        if not _non_empty_text_list(policy.get(field_name)):
            missing_or_invalid.append(field_name)

    missing_or_invalid.extend(
        _running_command_control_policy_missing_fields(
            policy,
            action="cancel",
            unsupported_reason="running_command_requires_owner_specific_cancel",
            placeholder_reason="owner_specific_interrupt_not_implemented",
        )
    )
    missing_or_invalid.extend(
        _running_command_control_policy_missing_fields(
            policy,
            action="resume",
            unsupported_reason="running_command_requires_owner_specific_resume",
            placeholder_reason="owner_specific_resume_not_implemented",
        )
    )
    return missing_or_invalid


def _durable_command_display_contract_missing_fields(contract: dict[str, Any]) -> list[str]:
    expected_command_type = str(contract.get("command_type") or "").strip()
    expected_owner = str(contract.get("expected_owner") or "").strip()
    display_contract = dict(contract.get("display_contract") or {})
    if not display_contract:
        return ["display_contract"]
    missing_or_invalid: list[str] = []
    for field_name in (
        "schema_version",
        "command_type",
        "owner",
        "display_label",
        "display_category",
        "description",
        "source_of_truth",
        "fallback_status",
    ):
        if not str(display_contract.get(field_name) or "").strip():
            missing_or_invalid.append(field_name)
    if str(display_contract.get("command_type") or "").strip() != expected_command_type:
        missing_or_invalid.append("command_type_matches_contract")
    if str(display_contract.get("owner") or "").strip() != expected_owner:
        missing_or_invalid.append("owner_matches_contract")
    if (
        str(display_contract.get("source_of_truth") or "").strip()
        != "durable_runtime.workflow_command_display_contract"
    ):
        missing_or_invalid.append("source_of_truth")
    if str(display_contract.get("fallback_status") or "").strip() != "fail_closed":
        missing_or_invalid.append("fallback_status_fail_closed")
    return missing_or_invalid


def _durable_command_activity_spine_policy_missing_fields(contract: dict[str, Any]) -> list[str]:
    expected_command_type = str(contract.get("command_type") or "").strip()
    expected_owner = str(contract.get("expected_owner") or "").strip()
    activity_spine_policy = dict(contract.get("activity_spine_policy") or {})
    if not activity_spine_policy:
        return ["activity_spine_policy"]
    missing_or_invalid: list[str] = []
    for field_name in (
        "schema_version",
        "command_type",
        "owner",
        "requirement",
        "activity_table",
        "attempt_table",
        "entity_delta_table",
        "source_of_truth",
        "agent_callable_surface",
        "fallback_status",
    ):
        if not str(activity_spine_policy.get(field_name) or "").strip():
            missing_or_invalid.append(field_name)
    if str(activity_spine_policy.get("command_type") or "").strip() != expected_command_type:
        missing_or_invalid.append("command_type_matches_contract")
    if str(activity_spine_policy.get("owner") or "").strip() != expected_owner:
        missing_or_invalid.append("owner_matches_contract")
    if (
        str(activity_spine_policy.get("source_of_truth") or "").strip()
        != "durable_runtime.workflow_command_activity_spine_policy"
    ):
        missing_or_invalid.append("source_of_truth")
    if str(activity_spine_policy.get("agent_callable_surface") or "").strip() != (
        "operation_command_activity_api"
    ):
        missing_or_invalid.append("agent_callable_surface")
    if str(activity_spine_policy.get("fallback_status") or "").strip() != "fail_closed":
        missing_or_invalid.append("fallback_status_fail_closed")
    if str(activity_spine_policy.get("requirement") or "").strip() == "legacy_internal_pending_activity_spine":
        missing_or_invalid.append("requirement_not_legacy_internal")
    for field_name in (
        "must_write_activity_run",
        "must_write_activity_attempt",
        "must_write_entity_delta",
        "downstream_activity_required",
        "agent_callable",
    ):
        if not isinstance(activity_spine_policy.get(field_name), bool):
            missing_or_invalid.append(field_name)
    return missing_or_invalid


def _running_command_control_policy_missing_fields(
    policy: dict[str, Any],
    *,
    action: str,
    unsupported_reason: str,
    placeholder_reason: str,
) -> list[str]:
    missing_or_invalid: list[str] = []
    supported_field = f"running_{action}_supported"
    statuses_field = f"running_{action}_statuses"
    owner_field = f"running_{action}_owner"
    delegate_field = f"running_{action}_delegate"
    blocked_reason_field = f"running_{action}_blocked_reason"
    upgrade_requirements_field = f"running_{action}_upgrade_requirements"
    unsupported_reason_field = f"unsupported_running_{action}_reason"
    if supported_field not in policy or not isinstance(policy.get(supported_field), bool):
        missing_or_invalid.append(supported_field)
    if str(policy.get(unsupported_reason_field) or "").strip() != unsupported_reason:
        missing_or_invalid.append(unsupported_reason_field)
    if bool(policy.get(supported_field)):
        if not _non_empty_text_list(policy.get(statuses_field)):
            missing_or_invalid.append(statuses_field)
        if not str(policy.get(owner_field) or "").strip():
            missing_or_invalid.append(owner_field)
        if not str(policy.get(delegate_field) or "").strip():
            missing_or_invalid.append(delegate_field)
        return missing_or_invalid

    blocked_reason = str(policy.get(blocked_reason_field) or "").strip()
    if not blocked_reason:
        missing_or_invalid.append(blocked_reason_field)
    elif blocked_reason == placeholder_reason:
        missing_or_invalid.append(f"{blocked_reason_field}_not_placeholder")
    if not _non_empty_text_list(policy.get(upgrade_requirements_field)):
        missing_or_invalid.append(upgrade_requirements_field)
    return missing_or_invalid


def _non_empty_text_list(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return any(str(item or "").strip() for item in value)


def _finding(name: str, severity: str, message: str, *, evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name,
        "severity": severity,
        "gate_layer": _classify_gate_layer(name, severity),
        "message": message,
        "evidence": evidence,
    }


def _passed_gate(name: str, message: str) -> dict[str, Any]:
    return {"gate": name, "message": message}


def _classify_gate_layer(name: str, severity: str) -> str:
    normalized = str(name or "").strip().lower()
    if "finalization" in normalized or normalized in {
        "recovery_durable_work_handoff_yield",
        "recovery_total_elapsed_slow",
        "user_experience_finalization_exceeds_default_but_within_case_slo",
    }:
        return "optimization"
    if (
        "workflow_job_to_" in normalized
        or "event_efficiency_" in normalized
        or "provider_slot" in normalized
    ):
        return "manual_handoff"
    if (
        "recovery_phase" in normalized
        or "profile_scheduler" in normalized
        or "post_profile_completion" in normalized
        or "board_visible_projection" in normalized
    ):
        return "pressure"
    if "missing" in normalized or "report_incomplete" in normalized or "parity" in normalized:
        return "report_integrity"
    if str(severity or "").strip() == "known_acceptable_warning":
        return "optimization"
    return "contract_integrity"


def _summarize_gate_layers(
    *,
    blocking_findings: list[dict[str, Any]],
    manual_review_required_findings: list[dict[str, Any]],
    known_acceptable_warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    layer_names = (
        "manual_handoff",
        "pressure",
        "optimization",
        "report_integrity",
        "contract_integrity",
    )
    layers: dict[str, dict[str, Any]] = {
        layer_name: {
            "blocking_count": 0,
            "manual_review_required_count": 0,
            "known_acceptable_warning_count": 0,
            "status": "passed",
        }
        for layer_name in layer_names
    }
    for section_name, findings in (
        ("blocking_count", blocking_findings),
        ("manual_review_required_count", manual_review_required_findings),
        ("known_acceptable_warning_count", known_acceptable_warnings),
    ):
        for finding in findings:
            layer_name = str(finding.get("gate_layer") or "contract_integrity").strip() or "contract_integrity"
            if layer_name not in layers:
                layers[layer_name] = {
                    "blocking_count": 0,
                    "manual_review_required_count": 0,
                    "known_acceptable_warning_count": 0,
                    "status": "passed",
                }
            layers[layer_name][section_name] = int(layers[layer_name].get(section_name) or 0) + 1
    for layer_payload in layers.values():
        if int(layer_payload.get("blocking_count") or 0) > 0:
            layer_payload["status"] = "blocked"
        elif int(layer_payload.get("manual_review_required_count") or 0) > 0:
            layer_payload["status"] = "manual_review_required"
        elif int(layer_payload.get("known_acceptable_warning_count") or 0) > 0:
            layer_payload["status"] = "warning"
    return layers


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
