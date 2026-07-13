from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha1
from typing import Any

from sourcing_agent.durable_runtime import (
    ACQUISITION_RUN_CREATE_COMMAND_TYPE,
    ACTIVITY_SPINE_LEGACY_INTERNAL,
    COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
    COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
    CRM_NOTE_ADD_COMMAND_TYPE,
    CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
    CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
    CRM_RECORD_UPDATE_COMMAND_TYPE,
    CRM_TASK_CREATE_COMMAND_TYPE,
    DEFAULT_COMMAND_OWNER_REGISTRY,
    EXCEL_INTAKE_RUN_COMMAND_TYPE,
    EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
    EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
    LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
    LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
    LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
    PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
    PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
    default_readiness_effect_for_command_type,
    default_stage_id_for_command_type,
    workflow_command_activity_spine_policy,
    workflow_command_control_policy,
    workflow_command_display_contract,
)

ACTION_PLAN_ACQUISITION = "plan_acquisition"
ACTION_START_ACQUISITION_RUN = "start_acquisition_run"
ACTION_FETCH_PROFILE_SAMPLE = "fetch_profile_sample"
ACTION_CONTINUE_ACQUISITION_RUN = "continue_acquisition_run"
ACTION_SEARCH_PROJECTION = "search_projection"
ACTION_FILTER_PROJECTION = "filter_projection"
ACTION_ADD_TO_CRM = "add_to_crm"
ACTION_SET_CRM_STAGE = "set_crm_stage"
ACTION_ADD_CRM_NOTE = "add_crm_note"
ACTION_CREATE_CRM_TASK = "create_crm_task"
ACTION_ENRICH_PERSON_PUBLIC_WEB = "enrich_person_public_web"
ACTION_REFRESH_COMPANY_PUBLIC_WEB = "refresh_company_public_web_assets"
ACTION_PROMOTE_PERSON_ASSERTION = "promote_person_assertion"
ACTION_EXPORT_CANDIDATES = "export_candidates"
ACTION_EXTERNAL_INTAKE = "external_intake"

APPROVAL_NOT_REQUIRED = "not_required"
APPROVAL_REQUIRED = "required"
OPERATION_RUN_TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
OPERATION_ACTION_TERMINAL_STATUSES = {"completed", "failed", "cancelled", "rejected"}
WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE = "operation_runtime.ActionRegistry.allowed_workflow_command_types"
WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED = "action_registry_allowlisted"


@dataclass(frozen=True)
class ActionSpec:
    action_type: str
    owner_module: str
    operation_type: str
    approval_policy: str = APPROVAL_NOT_REQUIRED
    budget_required: bool = False
    description: str = ""
    display_label: str = ""
    display_category: str = ""
    allowed_workflow_command_types: tuple[str, ...] = ()
    default_workflow_command_type: str = ""

    @property
    def requires_approval(self) -> bool:
        return self.approval_policy == APPROVAL_REQUIRED


@dataclass(frozen=True)
class OperationActionDisplayContract:
    action_type: str
    owner_module: str
    operation_type: str
    display_label: str
    display_category: str
    description: str = ""
    source_of_truth: str = "operation_runtime.ActionRegistry.display_contract_for"
    fallback_status: str = "fail_closed"
    schema_version: str = "operation_action_display_contract_v1"

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "action_type": self.action_type,
            "owner_module": self.owner_module,
            "operation_type": self.operation_type,
            "display_label": self.display_label,
            "display_category": self.display_category,
            "description": self.description,
            "source_of_truth": self.source_of_truth,
            "fallback_status": self.fallback_status,
        }


class ActionRegistry:
    def __init__(self, mapping: dict[str, ActionSpec] | None = None) -> None:
        self._mapping: dict[str, ActionSpec] = {}
        for action_type, spec in dict(mapping or {}).items():
            self.register(action_type, spec)

    def register(self, action_type: str, spec: ActionSpec) -> None:
        normalized_type = str(action_type or "").strip()
        if not normalized_type:
            raise ValueError("action_type is required")
        if normalized_type != spec.action_type:
            raise ValueError("action_type must match ActionSpec.action_type")
        if not spec.owner_module or not spec.operation_type:
            raise ValueError("owner_module and operation_type are required")
        command_types = tuple(str(command_type or "").strip() for command_type in spec.allowed_workflow_command_types)
        if len(set(command_types)) != len(command_types):
            raise ValueError(f"action_type {normalized_type!r} has duplicate workflow command types")
        owner_registry = DEFAULT_COMMAND_OWNER_REGISTRY.to_record()
        for command_type in command_types:
            if not command_type:
                raise ValueError(f"action_type {normalized_type!r} has an empty workflow command type")
            owner = owner_registry.get(command_type, "")
            if not owner:
                raise ValueError(
                    f"action_type {normalized_type!r} references unregistered workflow command type: {command_type}"
                )
            activity_policy = workflow_command_activity_spine_policy(command_type=command_type, owner=owner)
            if activity_policy.requirement == ACTIVITY_SPINE_LEGACY_INTERNAL or not activity_policy.agent_callable:
                raise ValueError(
                    f"action_type {normalized_type!r} cannot expose legacy/internal workflow command type: "
                    f"{command_type}"
                )
        default_command_type = str(spec.default_workflow_command_type or "").strip()
        if default_command_type and default_command_type not in command_types:
            raise ValueError(
                f"action_type {normalized_type!r} default workflow command type must be in allowed_workflow_command_types"
            )
        if not str(spec.display_label or "").strip():
            raise ValueError(f"action_type {normalized_type!r} requires display_label")
        if not str(spec.display_category or "").strip():
            raise ValueError(f"action_type {normalized_type!r} requires display_category")
        if not str(spec.description or "").strip():
            raise ValueError(f"action_type {normalized_type!r} requires description")
        existing = self._mapping.get(normalized_type)
        if existing and existing != spec:
            raise ValueError(f"action_type {normalized_type!r} is already registered")
        self._mapping[normalized_type] = spec

    def spec_for(self, action_type: str) -> ActionSpec:
        normalized_type = str(action_type or "").strip()
        spec = self._mapping.get(normalized_type)
        if spec is None:
            raise KeyError(f"unknown operation action type: {normalized_type}")
        return spec

    def _workflow_command_contract_record(self, command_type: str) -> dict[str, Any]:
        normalized_type = str(command_type or "").strip()
        owner = DEFAULT_COMMAND_OWNER_REGISTRY.to_record().get(normalized_type, "")
        return {
            "command_type": normalized_type,
            "owner": owner,
            "agent_exposure_status": WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED,
            "agent_exposure_gate": WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE,
            "stage_id": default_stage_id_for_command_type(normalized_type),
            "readiness_effect": default_readiness_effect_for_command_type(normalized_type),
            "display_contract": workflow_command_display_contract(
                command_type=normalized_type,
                owner=owner,
            ).to_record(),
            "control_policy": workflow_command_control_policy(command_type=normalized_type, owner=owner).to_record(),
            "activity_spine_policy": workflow_command_activity_spine_policy(
                command_type=normalized_type,
                owner=owner,
            ).to_record(),
        }

    def _workflow_command_control_summary(
        self,
        command_contracts: list[dict[str, Any]],
        *,
        default_workflow_command_type: str = "",
    ) -> dict[str, Any]:
        maturity_counts: dict[str, int] = {}
        gap_counts: dict[str, int] = {}
        category_counts: dict[str, int] = {}
        default_maturity = ""
        default_gap_status = ""
        for contract in command_contracts:
            command_type = str(contract.get("command_type") or "").strip()
            policy = dict(contract.get("control_policy") or {})
            maturity = str(policy.get("running_control_maturity") or "unknown").strip() or "unknown"
            gap_status = str(policy.get("running_control_gap_status") or "unknown").strip() or "unknown"
            category = str(policy.get("running_control_category") or "unknown").strip() or "unknown"
            maturity_counts[maturity] = int(maturity_counts.get(maturity, 0)) + 1
            gap_counts[gap_status] = int(gap_counts.get(gap_status, 0)) + 1
            category_counts[category] = int(category_counts.get(category, 0)) + 1
            if command_type == str(default_workflow_command_type or "").strip():
                default_maturity = maturity
                default_gap_status = gap_status
        command_count = len(command_contracts)
        fail_closed_count = int(maturity_counts.get("fail_closed_with_upgrade_requirements", 0))
        owner_specific_count = (
            int(maturity_counts.get("owner_specific_cancel_resume", 0))
            + int(maturity_counts.get("owner_specific_cancel_only", 0))
            + int(maturity_counts.get("owner_specific_resume_only", 0))
        )
        return {
            "source_of_truth": "operation_runtime.ActionRegistry.allowed_workflow_command_contracts",
            "fallback_status": "fail_closed",
            "command_count": command_count,
            "running_control_maturity_counts": dict(sorted(maturity_counts.items())),
            "running_control_gap_status_counts": dict(sorted(gap_counts.items())),
            "running_control_category_counts": dict(sorted(category_counts.items())),
            "has_fail_closed_running_controls": fail_closed_count > 0,
            "has_owner_specific_running_controls": owner_specific_count > 0,
            "default_workflow_command_type": str(default_workflow_command_type or "").strip(),
            "default_running_control_maturity": default_maturity,
            "default_running_control_gap_status": default_gap_status,
            "agent_ui_guidance": (
                "show_explicit_running_control_gaps"
                if fail_closed_count
                else "owner_specific_command_controls_available"
                if owner_specific_count
                else "no_workflow_command_surface"
            ),
        }

    def display_contract_for(self, action_type: str) -> OperationActionDisplayContract:
        spec = self.spec_for(action_type)
        return OperationActionDisplayContract(
            action_type=spec.action_type,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            display_label=spec.display_label,
            display_category=spec.display_category,
            description=spec.description,
        )

    def to_record(self, *, include_command_contracts: bool = True) -> dict[str, dict[str, Any]]:
        records: dict[str, dict[str, Any]] = {}
        for action_type, spec in sorted(self._mapping.items()):
            record: dict[str, Any] = {
                "owner_module": spec.owner_module,
                "operation_type": spec.operation_type,
                "approval_policy": spec.approval_policy,
                "budget_required": spec.budget_required,
                "description": spec.description,
                "display_contract": self.display_contract_for(action_type).to_record(),
                "allowed_workflow_command_types": list(spec.allowed_workflow_command_types),
                "default_workflow_command_type": spec.default_workflow_command_type,
                "workflow_command_exposure_gate": WORKFLOW_COMMAND_EXPOSURE_GATE_SOURCE,
                "workflow_command_exposure_status": (
                    WORKFLOW_COMMAND_EXPOSURE_STATUS_ALLOWLISTED
                    if spec.allowed_workflow_command_types
                    else "no_workflow_command_surface"
                ),
            }
            if include_command_contracts:
                command_contracts = [
                    self._workflow_command_contract_record(command_type)
                    for command_type in list(spec.allowed_workflow_command_types)
                ]
                record["allowed_workflow_command_contracts"] = command_contracts
                record["workflow_command_control_summary"] = self._workflow_command_control_summary(
                    command_contracts,
                    default_workflow_command_type=spec.default_workflow_command_type,
                )
                if spec.default_workflow_command_type:
                    record["default_workflow_command_contract"] = next(
                        (
                            contract
                            for contract in command_contracts
                            if str(contract.get("command_type") or "").strip() == spec.default_workflow_command_type
                        ),
                        self._workflow_command_contract_record(spec.default_workflow_command_type),
                    )
            records[action_type] = record
        return records


DEFAULT_ACTION_REGISTRY = ActionRegistry(
    {
        ACTION_PLAN_ACQUISITION: ActionSpec(
            action_type=ACTION_PLAN_ACQUISITION,
            owner_module="planner",
            operation_type="acquisition_plan",
            description="Draft an acquisition plan without executing provider or workflow side effects.",
            display_label="Plan acquisition",
            display_category="acquisition",
        ),
        ACTION_START_ACQUISITION_RUN: ActionSpec(
            action_type=ACTION_START_ACQUISITION_RUN,
            owner_module="acquisition_run_writer",
            operation_type="acquisition_run",
            approval_policy=APPROVAL_REQUIRED,
            budget_required=True,
            description="Create the durable root for a reviewed acquisition run.",
            display_label="Start acquisition run",
            display_category="acquisition",
            allowed_workflow_command_types=(ACQUISITION_RUN_CREATE_COMMAND_TYPE,),
            default_workflow_command_type=ACQUISITION_RUN_CREATE_COMMAND_TYPE,
        ),
        ACTION_FETCH_PROFILE_SAMPLE: ActionSpec(
            action_type=ACTION_FETCH_PROFILE_SAMPLE,
            owner_module="profile_scheduler",
            operation_type="profile_sample",
            approval_policy=APPROVAL_REQUIRED,
            budget_required=True,
            description="Fetch or plan a bounded sample of LinkedIn profiles for review.",
            display_label="Fetch profile sample",
            display_category="profile",
            allowed_workflow_command_types=(LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,),
            default_workflow_command_type=LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
        ),
        ACTION_CONTINUE_ACQUISITION_RUN: ActionSpec(
            action_type=ACTION_CONTINUE_ACQUISITION_RUN,
            owner_module="acquisition_run_writer",
            operation_type="acquisition_run",
            approval_policy=APPROVAL_REQUIRED,
            budget_required=True,
            description="Continue an existing acquisition run through an explicitly allowed typed command.",
            display_label="Continue acquisition run",
            display_category="acquisition",
            allowed_workflow_command_types=(
                LINKEDIN_DISCOVERY_QUERY_RUN_COMMAND_TYPE,
                LINKEDIN_PROFILE_FETCH_ACTIVITY_RUN_COMMAND_TYPE,
                LINKEDIN_PROFILE_FETCH_PROVIDER_COMMAND_TYPE,
                LINKEDIN_PROFILE_TERMINAL_ADMIT_COMMAND_TYPE,
                PROJECTION_PROFILE_ADMISSION_APPLY_COMMAND_TYPE,
                PROJECTION_PERSON_SEARCH_INDEX_BUILD_COMMAND_TYPE,
                COLLECTION_AUTHORITATIVE_MERGE_COMMAND_TYPE,
            ),
        ),
        ACTION_SEARCH_PROJECTION: ActionSpec(
            action_type=ACTION_SEARCH_PROJECTION,
            owner_module="projection_search_service",
            operation_type="projection_search",
            description="Run a read-only search over a canonical serving projection.",
            display_label="Search projection",
            display_category="projection",
        ),
        ACTION_FILTER_PROJECTION: ActionSpec(
            action_type=ACTION_FILTER_PROJECTION,
            owner_module="projection_search_service",
            operation_type="projection_filter",
            description="Apply read-only filters against a canonical serving projection.",
            display_label="Filter projection",
            display_category="projection",
        ),
        ACTION_ADD_TO_CRM: ActionSpec(
            action_type=ACTION_ADD_TO_CRM,
            owner_module="crm_writer",
            operation_type="crm_update",
            description="Add selected projection people to the person-first CRM through the CRM writer owner.",
            display_label="Add to CRM",
            display_category="crm",
            allowed_workflow_command_types=(CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,),
            default_workflow_command_type=CRM_RECORD_ADD_FROM_PROJECTION_COMMAND_TYPE,
        ),
        ACTION_SET_CRM_STAGE: ActionSpec(
            action_type=ACTION_SET_CRM_STAGE,
            owner_module="crm_writer",
            operation_type="crm_update",
            description="Update CRM stage or record flags through the CRM writer owner.",
            display_label="Set CRM stage",
            display_category="crm",
            allowed_workflow_command_types=(CRM_RECORD_UPDATE_COMMAND_TYPE,),
            default_workflow_command_type=CRM_RECORD_UPDATE_COMMAND_TYPE,
        ),
        ACTION_ADD_CRM_NOTE: ActionSpec(
            action_type=ACTION_ADD_CRM_NOTE,
            owner_module="crm_writer",
            operation_type="crm_update",
            description="Append a CRM note as an auditable CRM event.",
            display_label="Add CRM note",
            display_category="crm",
            allowed_workflow_command_types=(CRM_NOTE_ADD_COMMAND_TYPE,),
            default_workflow_command_type=CRM_NOTE_ADD_COMMAND_TYPE,
        ),
        ACTION_CREATE_CRM_TASK: ActionSpec(
            action_type=ACTION_CREATE_CRM_TASK,
            owner_module="crm_writer",
            operation_type="crm_update",
            description="Create a CRM follow-up task backed by CRM task current state and audit events.",
            display_label="Create CRM task",
            display_category="crm",
            allowed_workflow_command_types=(CRM_TASK_CREATE_COMMAND_TYPE,),
            default_workflow_command_type=CRM_TASK_CREATE_COMMAND_TYPE,
        ),
        ACTION_ENRICH_PERSON_PUBLIC_WEB: ActionSpec(
            action_type=ACTION_ENRICH_PERSON_PUBLIC_WEB,
            owner_module="person_evidence_ingestion",
            operation_type="person_enrichment",
            approval_policy=APPROVAL_REQUIRED,
            budget_required=True,
            description="Queue CRM-owned Public Web enrichment for selected people.",
            display_label="Enrich person public web",
            display_category="public_web",
            allowed_workflow_command_types=(CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,),
            default_workflow_command_type=CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_TYPE,
        ),
        ACTION_REFRESH_COMPANY_PUBLIC_WEB: ActionSpec(
            action_type=ACTION_REFRESH_COMPANY_PUBLIC_WEB,
            owner_module="company_public_web_owner",
            operation_type="company_enrichment",
            approval_policy=APPROVAL_REQUIRED,
            budget_required=True,
            description="Refresh company-level Public Web assets through the company Public Web owner.",
            display_label="Refresh company public web",
            display_category="public_web",
            allowed_workflow_command_types=(COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,),
            default_workflow_command_type=COMPANY_PUBLIC_WEB_REFRESH_COMMAND_TYPE,
        ),
        ACTION_PROMOTE_PERSON_ASSERTION: ActionSpec(
            action_type=ACTION_PROMOTE_PERSON_ASSERTION,
            owner_module="person_assertion_writer",
            operation_type="person_assertion_promotion",
            description="Promote reviewed evidence into a selected PersonAssertion.",
            display_label="Promote person assertion",
            display_category="person_asset",
        ),
        ACTION_EXPORT_CANDIDATES: ActionSpec(
            action_type=ACTION_EXPORT_CANDIDATES,
            owner_module="export_service",
            operation_type="export",
            approval_policy=APPROVAL_REQUIRED,
            description="Generate an export artifact through the appropriate export command owner.",
            display_label="Export candidates",
            display_category="export",
            allowed_workflow_command_types=(
                EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
                EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE,
            ),
            default_workflow_command_type=EXPORT_PROJECTION_GENERATE_COMMAND_TYPE,
        ),
        ACTION_EXTERNAL_INTAKE: ActionSpec(
            action_type=ACTION_EXTERNAL_INTAKE,
            owner_module="intake_service",
            operation_type="external_intake",
            description="Ingest an external file through the Excel intake command owner.",
            display_label="Run external intake",
            display_category="intake",
            allowed_workflow_command_types=(EXCEL_INTAKE_RUN_COMMAND_TYPE,),
            default_workflow_command_type=EXCEL_INTAKE_RUN_COMMAND_TYPE,
        ),
    }
)


@dataclass(frozen=True)
class OperationSubmissionResult:
    action: dict[str, Any]
    operation_run: dict[str, Any] = field(default_factory=dict)
    events: tuple[dict[str, Any], ...] = ()

    @property
    def executable(self) -> bool:
        return bool(self.operation_run) and self.action.get("status") != "approval_required"


class OperationRuntimeStateConflict(RuntimeError):
    def __init__(self, reason: str, record: dict[str, Any]) -> None:
        self.reason = str(reason or "operation_runtime_state_conflict").strip()
        self.record = dict(record or {})
        super().__init__(self.reason)


@dataclass(frozen=True)
class OperationRunControlState:
    operation_status: str
    action_status: str = ""
    operation_phase: str = ""
    can_dispatch: bool = False
    can_cancel: bool = False
    can_retry: bool = False
    can_resume: bool = False
    allowed_actions: tuple[str, ...] = ()
    disabled_reasons: dict[str, str] = field(default_factory=dict)
    control_source_of_truth: str = "operation_runtime.operation_run_control_state"
    fallback_status: str = "fail_closed"
    module_state_mutated_on_control: bool = False
    schema_version: str = "operation_run_control_state_v1"

    def to_record(self) -> dict[str, Any]:
        return {
            "operation_status": self.operation_status,
            "action_status": self.action_status,
            "operation_phase": self.operation_phase,
            "can_dispatch": self.can_dispatch,
            "can_cancel": self.can_cancel,
            "can_retry": self.can_retry,
            "can_resume": self.can_resume,
            "allowed_actions": list(self.allowed_actions),
            "disabled_reasons": dict(self.disabled_reasons),
            "control_source_of_truth": self.control_source_of_truth,
            "fallback_status": self.fallback_status,
            "module_state_mutated_on_control": self.module_state_mutated_on_control,
            "schema_version": self.schema_version,
        }


def operation_action_retry_eligibility(
    *,
    operation_status: str,
    operation_run_id: str = "",
    action_status: str = "",
    action_approval_status: str = "",
    action_retry_operation_run_id: str = "",
    requested_retry_operation_run_id: str = "",
) -> tuple[bool, str]:
    normalized_operation_status = str(operation_status or "").strip()
    normalized_operation_run_id = str(operation_run_id or "").strip()
    normalized_action_status = str(action_status or "").strip()
    normalized_approval_status = str(action_approval_status or "").strip()
    normalized_existing_retry_id = str(action_retry_operation_run_id or "").strip()
    normalized_requested_retry_id = str(requested_retry_operation_run_id or "").strip()
    if normalized_operation_status == "completed":
        return False, "completed_operation_cannot_retry"
    if normalized_operation_status not in {"failed", "cancelled"}:
        return False, "retry_requires_failed_or_cancelled_operation"
    if normalized_approval_status == "rejected":
        return False, "linked_action_rejected"
    if not normalized_action_status:
        return False, "linked_action_missing"
    if normalized_existing_retry_id and normalized_existing_retry_id not in {
        normalized_operation_run_id,
        normalized_requested_retry_id,
    }:
        return False, "linked_action_retry_already_planned"
    if normalized_action_status == "queued":
        return True, ""
    if normalized_action_status == normalized_operation_status:
        return True, ""
    return False, "linked_action_not_retryable"


def operation_run_control_state(
    *,
    operation_status: str,
    operation_run_id: str = "",
    action_status: str = "",
    action_approval_status: str = "",
    action_retry_operation_run_id: str = "",
    operation_phase: str = "",
) -> OperationRunControlState:
    normalized_status = str(operation_status or "").strip() or "unknown"
    normalized_action_status = str(action_status or "").strip()
    normalized_phase = str(operation_phase or "").strip()
    linked_action_present = bool(normalized_action_status)
    operation_terminal = normalized_status in OPERATION_RUN_TERMINAL_STATUSES
    action_terminal = normalized_action_status in OPERATION_ACTION_TERMINAL_STATUSES
    can_retry, retry_disabled_reason = operation_action_retry_eligibility(
        operation_status=normalized_status,
        operation_run_id=operation_run_id,
        action_status=normalized_action_status,
        action_approval_status=action_approval_status,
        action_retry_operation_run_id=action_retry_operation_run_id,
    )

    can_dispatch = linked_action_present and normalized_status == "queued" and not action_terminal
    can_cancel = linked_action_present and not operation_terminal and not action_terminal
    can_resume = linked_action_present and not operation_terminal and not action_terminal

    allowed_actions = tuple(
        action
        for action, allowed in (
            ("dispatch", can_dispatch),
            ("resume", can_resume),
            ("retry", can_retry),
            ("cancel", can_cancel),
        )
        if allowed
    )
    disabled_reasons: dict[str, str] = {}
    if not can_dispatch:
        disabled_reasons["dispatch"] = (
            "linked_action_missing"
            if not linked_action_present
            else ("linked_action_terminal" if action_terminal else "dispatch_requires_queued_operation")
        )
    if not can_resume:
        disabled_reasons["resume"] = (
            "linked_action_missing"
            if not linked_action_present
            else ("linked_action_terminal" if action_terminal else "terminal_operation_cannot_resume")
        )
    if not can_retry:
        disabled_reasons["retry"] = retry_disabled_reason
    if not can_cancel:
        disabled_reasons["cancel"] = (
            "linked_action_missing"
            if not linked_action_present
            else ("linked_action_terminal" if action_terminal else "terminal_operation_cannot_cancel")
        )

    return OperationRunControlState(
        operation_status=normalized_status,
        action_status=normalized_action_status,
        operation_phase=normalized_phase,
        can_dispatch=can_dispatch,
        can_cancel=can_cancel,
        can_retry=can_retry,
        can_resume=can_resume,
        allowed_actions=allowed_actions,
        disabled_reasons=disabled_reasons,
    )


def operation_action_id(*, workspace_id: str, action_type: str, idempotency_key: str) -> str:
    seed = (
        f"{str(workspace_id or 'default').strip() or 'default'}:"
        f"{str(action_type or '').strip()}:{str(idempotency_key or '').strip()}"
    )
    return "act_" + sha1(seed.encode("utf-8")).hexdigest()[:24]


def operation_run_id_for(*, action_id: str, operation_type: str, idempotency_key: str) -> str:
    seed = f"{str(action_id or '').strip()}:{str(operation_type or '').strip()}:{str(idempotency_key or '').strip()}"
    return "oprun_" + sha1(seed.encode("utf-8")).hexdigest()[:24]


def operation_retry_run_id_for(*, parent_operation_run_id: str, idempotency_key: str) -> str:
    seed = f"{str(parent_operation_run_id or '').strip()}:retry:{str(idempotency_key or '').strip()}"
    return "oprun_" + sha1(seed.encode("utf-8")).hexdigest()[:24]


def default_action_idempotency_key(
    *,
    workspace_id: str,
    action_type: str,
    target_ref: dict[str, Any] | None = None,
    input_payload: dict[str, Any] | None = None,
) -> str:
    payload = {
        "workspace_id": str(workspace_id or "default").strip() or "default",
        "action_type": str(action_type or "").strip(),
        "target_ref": target_ref or {},
        "input": input_payload or {},
    }
    digest = sha1(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:24]
    return f"{payload['action_type']}:{digest}"


class OperationRuntimeWriter:
    """Persists user/Agent intent without mutating module-owned domain tables."""

    def __init__(self, store: Any, *, action_registry: ActionRegistry | None = None) -> None:
        self.store = store
        self.action_registry = action_registry or DEFAULT_ACTION_REGISTRY

    def submit_action(
        self,
        *,
        action_type: str,
        workspace_id: str = "default",
        conversation_id: str = "",
        target_ref: dict[str, Any] | None = None,
        input_payload: dict[str, Any] | None = None,
        budget: dict[str, Any] | None = None,
        idempotency_key: str = "",
        actor: str = "operation_runtime",
        source: str = "operation_runtime",
        metadata: dict[str, Any] | None = None,
    ) -> OperationSubmissionResult:
        spec = self.action_registry.spec_for(action_type)
        normalized_workspace_id = str(workspace_id or "default").strip() or "default"
        normalized_idempotency = str(idempotency_key or "").strip() or default_action_idempotency_key(
            workspace_id=normalized_workspace_id,
            action_type=spec.action_type,
            target_ref=target_ref or {},
            input_payload=input_payload or {},
        )
        budget_payload = dict(budget or {})
        if spec.budget_required and not budget_payload:
            raise ValueError(f"action_type {spec.action_type!r} requires explicit budget")
        action_id = operation_action_id(
            workspace_id=normalized_workspace_id,
            action_type=spec.action_type,
            idempotency_key=normalized_idempotency,
        )
        approval_status = APPROVAL_REQUIRED if spec.requires_approval else APPROVAL_NOT_REQUIRED
        action_status = "approval_required" if spec.requires_approval else "queued"
        action = self.store.repos.workflow_runtime.upsert_action(
            action_id=action_id,
            workspace_id=normalized_workspace_id,
            conversation_id=conversation_id,
            action_type=spec.action_type,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            target_ref=target_ref or {},
            input_payload=input_payload or {},
            approval_status=approval_status,
            approval_policy=spec.approval_policy,
            budget=budget_payload,
            idempotency_key=normalized_idempotency,
            status=action_status,
            metadata={
                "operation_runtime_contract": "w8_operation_action_v1",
                "budget_required": spec.budget_required,
                **dict(metadata or {}),
            },
        )
        event_type = "ActionApprovalRequired" if spec.requires_approval else "AgentActionQueued"
        action_event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=normalized_workspace_id,
            event_stream_id=action_id,
            action_id=action_id,
            event_family="operation_event",
            event_type=event_type,
            idempotency_key=f"{normalized_idempotency}:{event_type}",
            actor=actor,
            source=source,
            payload={
                "action_type": spec.action_type,
                "owner_module": spec.owner_module,
                "operation_type": spec.operation_type,
                "approval_policy": spec.approval_policy,
                "executable": not spec.requires_approval,
            },
        )
        if spec.requires_approval:
            return OperationSubmissionResult(action=action, events=(action_event,))
        operation_id = operation_run_id_for(
            action_id=action_id,
            operation_type=spec.operation_type,
            idempotency_key=normalized_idempotency,
        )
        operation_run = self.store.repos.workflow_runtime.upsert_operation(
            operation_run_id=operation_id,
            workspace_id=normalized_workspace_id,
            action_id=action_id,
            owner_module=spec.owner_module,
            operation_type=spec.operation_type,
            status="queued",
            progress={"phase": "queued"},
            workflow_ref={},
            cost_budget=budget_payload,
            idempotency_key=normalized_idempotency,
            metadata={
                "operation_runtime_contract": "w8_operation_run_v1",
                "source_action_type": spec.action_type,
            },
        )
        operation_event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=normalized_workspace_id,
            event_stream_id=operation_id,
            operation_run_id=operation_id,
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationRunQueued",
            idempotency_key=f"{normalized_idempotency}:OperationRunQueued",
            actor=actor,
            source=source,
            payload={
                "action_type": spec.action_type,
                "owner_module": spec.owner_module,
                "operation_type": spec.operation_type,
                "module_state_mutated": False,
            },
        )
        return OperationSubmissionResult(
            action=action,
            operation_run=operation_run,
            events=(action_event, operation_event),
        )

    def approve_action(
        self,
        *,
        action_id: str,
        actor: str = "operation_runtime",
        source: str = "operation_runtime",
        approval_payload: dict[str, Any] | None = None,
    ) -> OperationSubmissionResult:
        action = self.store.repos.workflow_runtime.get_action(action_id)
        if not action:
            raise KeyError(f"agent action not found: {action_id}")
        if action.get("approval_status") == "rejected":
            raise ValueError("rejected action cannot be approved")
        if action.get("status") in {"cancelled", "completed", "failed"}:
            raise ValueError(f"terminal action cannot be approved: {action.get('status')}")
        workspace_id = str(action.get("workspace_id") or "default").strip() or "default"
        idempotency_key = str(action.get("idempotency_key") or "").strip()
        action = self.store.repos.workflow_runtime.update_action_state(
            str(action.get("action_id") or ""),
            status="queued",
            approval_status="approved",
            metadata_patch={"approval": dict(approval_payload or {}), "approval_actor": actor},
        )
        if (
            str(action.get("status") or "").strip() != "queued"
            or str(action.get("approval_status") or "").strip() != "approved"
        ):
            raise OperationRuntimeStateConflict("operation_action_approval_conflict", action)
        approval_event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=workspace_id,
            event_stream_id=str(action.get("action_id") or ""),
            action_id=str(action.get("action_id") or ""),
            event_family="operation_event",
            event_type="ActionApproved",
            idempotency_key=f"{idempotency_key}:ActionApproved",
            actor=actor,
            source=source,
            payload={"action_type": action.get("action_type"), "owner_module": action.get("owner_module")},
        )
        operation_id = operation_run_id_for(
            action_id=str(action.get("action_id") or ""),
            operation_type=str(action.get("operation_type") or ""),
            idempotency_key=idempotency_key,
        )
        operation_run = self.store.repos.workflow_runtime.upsert_operation(
            operation_run_id=operation_id,
            workspace_id=workspace_id,
            action_id=str(action.get("action_id") or ""),
            owner_module=str(action.get("owner_module") or ""),
            operation_type=str(action.get("operation_type") or ""),
            status="queued",
            progress={"phase": "queued_after_approval"},
            workflow_ref={},
            cost_budget=dict(action.get("budget") or {}),
            idempotency_key=idempotency_key,
            metadata={
                "operation_runtime_contract": "w9_operation_approval_v1",
                "source_action_type": action.get("action_type"),
            },
        )
        operation_event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=workspace_id,
            event_stream_id=operation_id,
            operation_run_id=operation_id,
            action_id=str(action.get("action_id") or ""),
            event_family="operation_event",
            event_type="OperationRunQueued",
            idempotency_key=f"{idempotency_key}:OperationRunQueued",
            actor=actor,
            source=source,
            payload={
                "action_type": action.get("action_type"),
                "owner_module": action.get("owner_module"),
                "operation_type": action.get("operation_type"),
                "approved": True,
                "module_state_mutated": False,
            },
        )
        return OperationSubmissionResult(
            action=action,
            operation_run=operation_run,
            events=(approval_event, operation_event),
        )

    def reject_action(
        self,
        *,
        action_id: str,
        actor: str = "operation_runtime",
        source: str = "operation_runtime",
        reason: str = "",
    ) -> dict[str, Any]:
        action = self.store.repos.workflow_runtime.get_action(action_id)
        if not action:
            raise KeyError(f"agent action not found: {action_id}")
        action_status = str(action.get("status") or "").strip()
        approval_status = str(action.get("approval_status") or "").strip()
        if action_status in OPERATION_ACTION_TERMINAL_STATUSES and not (
            action_status == "cancelled" and approval_status == "rejected"
        ):
            return action
        control_result = self.store.repos.workflow_runtime.reject_action_with_event(
            str(action.get("action_id") or ""),
            expected_status=action_status,
            workspace_id=str(action.get("workspace_id") or "default").strip() or "default",
            metadata_patch={"rejection_reason": str(reason or "").strip()},
            event_idempotency_key=f"{action.get('idempotency_key')}:ActionRejected",
            actor=actor,
            source=source,
            event_payload={"reason": str(reason or "").strip(), "module_state_mutated": False},
        )
        next_action = dict(control_result.get("action") or {})
        if str(control_result.get("outcome") or "").strip() == "not_found" or not next_action:
            raise KeyError(f"agent action not found: {action_id}")
        return next_action

    def cancel_operation(
        self,
        *,
        operation_run_id: str,
        actor: str = "operation_runtime",
        source: str = "operation_runtime",
        reason: str = "",
    ) -> dict[str, Any]:
        operation = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation:
            raise KeyError(f"operation run not found: {operation_run_id}")
        operation_status = str(operation.get("status") or "").strip()
        if operation_status in OPERATION_RUN_TERMINAL_STATUSES and operation_status != "cancelled":
            return operation
        action_id = str(operation.get("action_id") or "")
        control_result = self.store.repos.workflow_runtime.cancel_operation_with_event(
            str(operation.get("operation_run_id") or ""),
            expected_status=operation_status,
            action_id=action_id,
            workspace_id=str(operation.get("workspace_id") or "default").strip() or "default",
            progress_patch={"phase": "cancelled", "reason": str(reason or "").strip()},
            result_ref_patch={},
            metadata_patch={"cancelled_by": actor},
            linked_action_metadata_patch={
                "cancelled_operation_run_id": operation.get("operation_run_id"),
            },
            event_idempotency_key=f"{operation.get('idempotency_key')}:OperationCancelled",
            actor=actor,
            source=source,
            event_payload={"reason": str(reason or "").strip(), "module_state_mutated": False},
        )
        next_operation = dict(control_result.get("operation") or {})
        if str(control_result.get("outcome") or "").strip() == "not_found" or not next_operation:
            raise KeyError(f"operation run not found: {operation_run_id}")
        return next_operation

    def retry_operation(
        self,
        *,
        operation_run_id: str,
        actor: str = "operation_runtime",
        source: str = "operation_runtime",
        reason: str = "",
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        operation = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation:
            raise KeyError(f"operation run not found: {operation_run_id}")
        status = str(operation.get("status") or "").strip()
        if status == "completed":
            raise ValueError("completed operation cannot be retried")
        if status not in {"failed", "cancelled"}:
            raise ValueError(f"operation retry requires failed or cancelled status, got {status!r}")
        action_id = str(operation.get("action_id") or "").strip()
        action = self.store.repos.workflow_runtime.get_action(action_id) if action_id else {}
        action_status = str(action.get("status") or "").strip()
        action_approval_status = str(action.get("approval_status") or "").strip()
        workspace_id = str(operation.get("workspace_id") or "default").strip() or "default"
        retry_key = str(idempotency_key or "").strip() or f"{operation.get('idempotency_key')}:retry"
        parent_operation_run_id = str(operation.get("operation_run_id") or "").strip()
        retry_run_id = operation_retry_run_id_for(
            parent_operation_run_id=parent_operation_run_id,
            idempotency_key=retry_key,
        )
        persisted_retry_key = f"operation_retry:{parent_operation_run_id}:{retry_run_id}"
        action_workspace_id = str(action.get("workspace_id") or "default").strip() or "default"
        if not action:
            raise ValueError("linked_action_missing")
        if action and action_workspace_id != workspace_id:
            raise OperationRuntimeStateConflict("operation_run_retry_workspace_conflict", action)
        existing_retry_run = self.store.repos.workflow_runtime.get_operation(retry_run_id)
        if existing_retry_run:
            retry_metadata = dict(existing_retry_run.get("metadata") or {})
            if (
                str(existing_retry_run.get("operation_run_id") or "").strip() != retry_run_id
                or (str(existing_retry_run.get("workspace_id") or "default").strip() or "default") != workspace_id
                or str(existing_retry_run.get("action_id") or "").strip() != action_id
                or str(existing_retry_run.get("owner_module") or "").strip()
                != str(operation.get("owner_module") or "").strip()
                or str(existing_retry_run.get("operation_type") or "").strip()
                != str(operation.get("operation_type") or "").strip()
                or str(existing_retry_run.get("idempotency_key") or "").strip() != persisted_retry_key
                or str(retry_metadata.get("parent_operation_run_id") or "").strip() != parent_operation_run_id
            ):
                raise OperationRuntimeStateConflict(
                    "operation_run_retry_identity_conflict",
                    existing_retry_run,
                )
            expected_events = (
                (
                    parent_operation_run_id,
                    f"{persisted_retry_key}:OperationRetryRequested",
                    "OperationRetryRequested",
                    parent_operation_run_id,
                ),
                (
                    retry_run_id,
                    f"{persisted_retry_key}:OperationRunQueued",
                    "OperationRunQueued",
                    retry_run_id,
                ),
            )
            replay_events: list[dict[str, Any]] = []
            for stream_id, event_key, event_type, event_operation_run_id in expected_events:
                matching_events = [
                    event
                    for event in self.store.repos.workflow_runtime.list_operation_events(stream_id)
                    if str(event.get("idempotency_key") or "").strip() == event_key
                ]
                if not matching_events:
                    continue
                event = matching_events[0]
                if (
                    len(matching_events) != 1
                    or str(event.get("event_stream_id") or "").strip() != stream_id
                    or str(event.get("event_type") or "").strip() != event_type
                    or str(event.get("operation_run_id") or "").strip() != event_operation_run_id
                    or str(event.get("action_id") or "").strip() != action_id
                    or (str(event.get("workspace_id") or "default").strip() or "default") != workspace_id
                ):
                    raise OperationRuntimeStateConflict(
                        "operation_run_retry_event_identity_conflict",
                        event,
                    )
                replay_events.append(event)
            return {
                "parent_operation_run": operation,
                "operation_run": existing_retry_run,
                "events": replay_events,
            }
        action_retry_run_id = str(dict(action.get("metadata") or {}).get("retry_operation_run_id") or "").strip()
        retry_allowed, retry_disabled_reason = operation_action_retry_eligibility(
            operation_status=status,
            operation_run_id=parent_operation_run_id,
            action_status=action_status,
            action_approval_status=action_approval_status,
            action_retry_operation_run_id=action_retry_run_id,
            requested_retry_operation_run_id=retry_run_id,
        )
        if not retry_allowed:
            if retry_disabled_reason == "linked_action_retry_already_planned":
                raise OperationRuntimeStateConflict("operation_run_retry_conflict", action)
            raise ValueError(retry_disabled_reason)
        if action_id:
            action = self.store.repos.workflow_runtime.requeue_action_for_operation_retry(
                action_id,
                workspace_id=workspace_id,
                expected_status=action_status,
                parent_operation_run_id=parent_operation_run_id,
                retry_operation_run_id=retry_run_id,
            )
            if (
                str(action.get("status") or "").strip() != "queued"
                or (str(action.get("workspace_id") or "default").strip() or "default") != workspace_id
                or str(dict(action.get("metadata") or {}).get("retry_operation_run_id") or "").strip() != retry_run_id
            ):
                raise OperationRuntimeStateConflict("operation_run_retry_conflict", action)
        retry_run = self.store.repos.workflow_runtime.upsert_operation(
            operation_run_id=retry_run_id,
            workspace_id=workspace_id,
            action_id=action_id,
            owner_module=str(operation.get("owner_module") or ""),
            operation_type=str(operation.get("operation_type") or ""),
            status="queued",
            progress={
                "phase": "queued_retry",
                "retry_of_operation_run_id": operation.get("operation_run_id"),
                "reason": str(reason or "").strip(),
            },
            workflow_ref={},
            cost_budget=dict(operation.get("cost_budget") or {}),
            idempotency_key=persisted_retry_key,
            metadata={
                "operation_runtime_contract": "w9_operation_retry_v1",
                "parent_operation_run_id": operation.get("operation_run_id"),
                "retry_requested_by": actor,
            },
        )
        retry_metadata = dict(retry_run.get("metadata") or {})
        if (
            str(retry_run.get("operation_run_id") or "").strip() != retry_run_id
            or (str(retry_run.get("workspace_id") or "default").strip() or "default") != workspace_id
            or str(retry_run.get("action_id") or "").strip() != action_id
            or str(retry_run.get("status") or "").strip() != "queued"
            or str(retry_metadata.get("parent_operation_run_id") or "").strip() != parent_operation_run_id
        ):
            raise OperationRuntimeStateConflict("operation_run_retry_identity_conflict", retry_run)
        retry_requested_event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=workspace_id,
            event_stream_id=str(operation.get("operation_run_id") or ""),
            operation_run_id=str(operation.get("operation_run_id") or ""),
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationRetryRequested",
            idempotency_key=f"{persisted_retry_key}:OperationRetryRequested",
            actor=actor,
            source=source,
            payload={
                "retry_operation_run_id": retry_run.get("operation_run_id"),
                "reason": str(reason or "").strip(),
                "module_state_mutated": False,
            },
        )
        queued_event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=workspace_id,
            event_stream_id=str(retry_run.get("operation_run_id") or ""),
            operation_run_id=str(retry_run.get("operation_run_id") or ""),
            action_id=action_id,
            event_family="operation_event",
            event_type="OperationRunQueued",
            idempotency_key=f"{persisted_retry_key}:OperationRunQueued",
            actor=actor,
            source=source,
            payload={
                "retry_of_operation_run_id": operation.get("operation_run_id"),
                "owner_module": operation.get("owner_module"),
                "operation_type": operation.get("operation_type"),
                "module_state_mutated": False,
            },
        )
        return {
            "parent_operation_run": operation,
            "operation_run": retry_run,
            "events": [retry_requested_event, queued_event],
        }

    def resume_operation(
        self,
        *,
        operation_run_id: str,
        actor: str = "operation_runtime",
        source: str = "operation_runtime",
        reason: str = "",
    ) -> dict[str, Any]:
        operation = self.store.repos.workflow_runtime.get_operation(operation_run_id)
        if not operation:
            raise KeyError(f"operation run not found: {operation_run_id}")
        status = str(operation.get("status") or "").strip()
        if status in {"completed", "failed", "cancelled"}:
            raise ValueError(f"terminal operation cannot be resumed: {status}")
        next_operation = self.store.repos.workflow_runtime.update_operation_state(
            str(operation.get("operation_run_id") or ""),
            status="queued",
            progress_patch={"phase": "resume_requested", "reason": str(reason or "").strip()},
            metadata_patch={"resume_requested_by": actor},
        )
        if (
            str(next_operation.get("status") or "").strip() != "queued"
            or str(dict(next_operation.get("progress") or {}).get("phase") or "").strip() != "resume_requested"
        ):
            raise OperationRuntimeStateConflict("operation_run_resume_conflict", next_operation)
        event = self.store.repos.workflow_runtime.append_operation_event(
            workspace_id=str(next_operation.get("workspace_id") or "default").strip() or "default",
            event_stream_id=str(next_operation.get("operation_run_id") or ""),
            operation_run_id=str(next_operation.get("operation_run_id") or ""),
            action_id=str(next_operation.get("action_id") or ""),
            event_family="operation_event",
            event_type="OperationResumeRequested",
            idempotency_key=f"{next_operation.get('idempotency_key')}:OperationResumeRequested",
            actor=actor,
            source=source,
            payload={"reason": str(reason or "").strip(), "module_state_mutated": False},
        )
        return {"operation_run": next_operation, "events": [event]}
