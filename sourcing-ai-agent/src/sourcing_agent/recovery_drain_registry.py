"""Recovery-tick domain-drain binding registry.

``SourcingOrchestrator.run_worker_recovery_once`` historically named every
domain command-owner drain directly inside a uniform flag-gated block. This
module turns those drain BINDINGS into data so the recovery tick looks drains
up through a registry instead of naming domain methods. It deliberately does
NOT model recovery phases, ordering semantics, budgets, or lease handling —
those stay in the recovery tick (Phase-4 reserved core); a binding only
carries what the audited uniform block varied per drain.

Two CRM Public Web drain phases (``crm_public_web_queue_batch`` and
``crm_public_web_phase_commands``) remain named calls inside the recovery tick
because tests/test_crm_public_web_runtime_boundary.py pins their literal
callback source text; every other drain in the uniform block is bound here in
the exact pre-registry call order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .durable_runtime import (
    ACQUISITION_INTENT_RESOLVE_OWNER,
    ACQUISITION_PLAN_BUILD_OWNER,
    ACQUISITION_PLAN_COMMIT_OWNER,
    ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
    ACQUISITION_PROBE_OWNER,
    ACQUISITION_RUN_CREATE_OWNER,
    ACQUISITION_SCALE_PLAN_OWNER,
    COMPANY_ASSET_OWNER,
    COMPANY_PUBLIC_WEB_REFRESH_OWNER,
    CRM_WRITER_OWNER,
    EXPORT_PROJECTION_GENERATE_OWNER,
    LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
    LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
    MEDIA_ASSET_OWNER,
    PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
)


@dataclass(frozen=True)
class RecoveryDrainBinding:
    """One audited drain row of the recovery tick's uniform drain block.

    ``phase`` doubles as the ``recovery_phase_metrics`` key and (when
    ``include_in_result`` is true) the recovery-summary key. ``drain_method``
    is resolved against the orchestrator with ``getattr`` at call time so
    instance/class patches and the signature-identical facade wrappers keep
    intercepting drain calls exactly as before the rebind.
    """

    phase: str
    owner: str
    payload_flag: str
    disabled_reason: str
    drain_method: str
    max_sync_work: str
    skipped_max_sync_work: str
    # Audited non-uniformity: crm_writer_command_owner is executed and metered
    # but its result is absent from both the tick-level phase-budget scan and
    # the returned recovery summary.
    include_in_phase_budget_scan: bool = True
    include_in_result: bool = True


DEFAULT_RECOVERY_DRAIN_BINDINGS: tuple[RecoveryDrainBinding, ...] = (
    RecoveryDrainBinding(
        phase="company_public_web_refresh_command_owner",
        owner=COMPANY_PUBLIC_WEB_REFRESH_OWNER,
        payload_flag="company_public_web_refresh_command_owner_enabled",
        disabled_reason="company_public_web_refresh_command_owner_disabled_by_payload",
        drain_method="_drain_company_public_web_refresh_commands",
        max_sync_work="claim and execute ready company.public_web.refresh workflow_commands only",
        skipped_max_sync_work="no company Public Web refresh command work",
    ),
    RecoveryDrainBinding(
        phase="company_logo_profile_experience_discover_command_owner",
        owner=COMPANY_ASSET_OWNER,
        payload_flag="company_logo_profile_experience_discover_command_owner_enabled",
        disabled_reason="company_logo_profile_experience_discover_command_owner_disabled_by_payload",
        drain_method="_drain_company_logo_profile_experience_discover_commands",
        max_sync_work="claim ready company.logo.profile_experience.discover commands; read at most one profile per command",
        skipped_max_sync_work="no profile-experience company logo discovery command work",
    ),
    RecoveryDrainBinding(
        phase="media_asset_cache_command_owner",
        owner=MEDIA_ASSET_OWNER,
        payload_flag="media_asset_cache_command_owner_enabled",
        disabled_reason="media_asset_cache_command_owner_disabled_by_payload",
        drain_method="_drain_media_asset_cache_commands",
        max_sync_work="claim and execute ready media.asset.cache workflow_commands only",
        skipped_max_sync_work="no media asset cache command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_run_create_command_owner",
        owner=ACQUISITION_RUN_CREATE_OWNER,
        payload_flag="acquisition_run_create_command_owner_enabled",
        disabled_reason="acquisition_run_create_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_run_create_commands",
        max_sync_work="claim and execute ready acquisition.run.create workflow_commands only",
        skipped_max_sync_work="no acquisition run create command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_intent_resolve_command_owner",
        owner=ACQUISITION_INTENT_RESOLVE_OWNER,
        payload_flag="acquisition_intent_resolve_command_owner_enabled",
        disabled_reason="acquisition_intent_resolve_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_intent_resolve_commands",
        max_sync_work="claim and execute ready acquisition.intent.resolve workflow_commands only",
        skipped_max_sync_work="no acquisition intent resolve command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_plan_build_command_owner",
        owner=ACQUISITION_PLAN_BUILD_OWNER,
        payload_flag="acquisition_plan_build_command_owner_enabled",
        disabled_reason="acquisition_plan_build_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_plan_build_commands",
        max_sync_work="claim and execute ready acquisition.plan.build workflow_commands only",
        skipped_max_sync_work="no acquisition plan build command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_plan_review_request_command_owner",
        owner=ACQUISITION_PLAN_REVIEW_REQUEST_OWNER,
        payload_flag="acquisition_plan_review_request_command_owner_enabled",
        disabled_reason="acquisition_plan_review_request_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_plan_review_request_commands",
        max_sync_work="claim and execute ready acquisition.plan_review.request workflow_commands only",
        skipped_max_sync_work="no acquisition plan review request command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_plan_commit_command_owner",
        owner=ACQUISITION_PLAN_COMMIT_OWNER,
        payload_flag="acquisition_plan_commit_command_owner_enabled",
        disabled_reason="acquisition_plan_commit_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_plan_commit_commands",
        max_sync_work="claim and execute ready acquisition.plan.commit workflow_commands only",
        skipped_max_sync_work="no acquisition plan commit command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_probe_command_owner",
        owner=ACQUISITION_PROBE_OWNER,
        payload_flag="acquisition_probe_command_owner_enabled",
        disabled_reason="acquisition_probe_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_probe_commands",
        max_sync_work="claim and execute ready acquisition.probe.* workflow_commands only",
        skipped_max_sync_work="no acquisition probe command work",
    ),
    RecoveryDrainBinding(
        phase="acquisition_scale_plan_command_owner",
        owner=ACQUISITION_SCALE_PLAN_OWNER,
        payload_flag="acquisition_scale_plan_command_owner_enabled",
        disabled_reason="acquisition_scale_plan_command_owner_disabled_by_payload",
        drain_method="_drain_acquisition_scale_plan_commands",
        max_sync_work="claim and execute ready acquisition.scale.plan workflow_commands only",
        skipped_max_sync_work="no acquisition scale plan command work",
    ),
    RecoveryDrainBinding(
        phase="operation_native_discovery_activity_owner",
        owner=LINKEDIN_DISCOVERY_QUERY_RUN_OWNER,
        payload_flag="operation_native_discovery_activity_owner_enabled",
        disabled_reason="operation_native_discovery_activity_owner_disabled_by_payload",
        drain_method="_drain_operation_native_discovery_activity_commands",
        max_sync_work="claim operation-native discovery workflow_commands and update Activity/Attempt/EntityDelta state only",
        skipped_max_sync_work="no operation-native discovery activity command work",
    ),
    RecoveryDrainBinding(
        phase="operation_native_profile_fetch_activity_owner",
        owner=LINKEDIN_PROFILE_FETCH_ACTIVITY_OWNER,
        payload_flag="operation_native_profile_fetch_activity_owner_enabled",
        disabled_reason="operation_native_profile_fetch_activity_owner_disabled_by_payload",
        drain_method="_drain_operation_native_profile_fetch_activity_commands",
        max_sync_work=(
            "claim operation-native profile-fetch workflow_commands and update "
            "Activity/Attempt/EntityDelta state only; no legacy job shell"
        ),
        skipped_max_sync_work="no operation-native profile-fetch activity command work",
    ),
    RecoveryDrainBinding(
        phase="operation_native_projection_admission_owner",
        owner=PROJECTION_PROFILE_ADMISSION_APPLY_OWNER,
        payload_flag="operation_native_projection_admission_owner_enabled",
        disabled_reason="operation_native_projection_admission_owner_disabled_by_payload",
        drain_method="_drain_operation_native_projection_admission_commands",
        max_sync_work=(
            "claim operation-native projection-admission workflow_commands and write "
            "canonical serving projection membership only through serving_projection_owner"
        ),
        skipped_max_sync_work="no operation-native projection-admission command work",
    ),
    RecoveryDrainBinding(
        phase="export_projection_generate_command_owner",
        owner=EXPORT_PROJECTION_GENERATE_OWNER,
        payload_flag="export_projection_generate_command_owner_enabled",
        disabled_reason="export_projection_generate_command_owner_disabled_by_payload",
        drain_method="_drain_export_projection_generate_commands",
        max_sync_work="claim and execute ready export.projection.generate workflow_commands only",
        skipped_max_sync_work="no projection export generate command work",
    ),
    RecoveryDrainBinding(
        phase="crm_writer_command_owner",
        owner=CRM_WRITER_OWNER,
        payload_flag="crm_writer_command_owner_enabled",
        disabled_reason="crm_writer_command_owner_disabled_by_payload",
        drain_method="_drain_crm_writer_commands",
        max_sync_work="claim and execute ready CRM writer workflow_commands only",
        skipped_max_sync_work="no CRM writer command work",
        include_in_phase_budget_scan=False,
        include_in_result=False,
    ),
)


def build_recovery_drain_registry(
    bindings: Iterable[RecoveryDrainBinding],
    *,
    drain_host: object | None = None,
) -> tuple[RecoveryDrainBinding, ...]:
    """Validate drain bindings and freeze them into the registry tuple.

    Registration must fail loudly: empty identity fields, duplicate phase
    keys, duplicate payload flags, and (when ``drain_host`` is provided) drain
    methods that do not resolve to a callable on the host all raise instead of
    silently dropping or shadowing a drain.
    """

    registry: list[RecoveryDrainBinding] = []
    seen_phases: set[str] = set()
    seen_flags: set[str] = set()
    for binding in bindings:
        if not isinstance(binding, RecoveryDrainBinding):
            raise TypeError(f"recovery drain registry entries must be RecoveryDrainBinding, got {type(binding).__name__}")
        for field_name in ("phase", "owner", "payload_flag", "disabled_reason", "drain_method"):
            if not str(getattr(binding, field_name) or "").strip():
                raise ValueError(f"recovery drain binding {binding.phase!r} has an empty {field_name}")
        if binding.phase in seen_phases:
            raise ValueError(f"duplicate recovery drain phase {binding.phase!r}")
        if binding.payload_flag in seen_flags:
            raise ValueError(f"duplicate recovery drain payload flag {binding.payload_flag!r}")
        seen_phases.add(binding.phase)
        seen_flags.add(binding.payload_flag)
        if drain_host is not None and not callable(getattr(drain_host, binding.drain_method, None)):
            raise ValueError(
                f"recovery drain binding {binding.phase!r} references missing drain method {binding.drain_method!r}"
            )
        registry.append(binding)
    if not registry:
        raise ValueError("recovery drain registry must not be empty")
    return tuple(registry)
