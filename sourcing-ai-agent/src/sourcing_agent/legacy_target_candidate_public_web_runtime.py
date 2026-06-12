"""Migration-only legacy target-candidate Public Web runtime boundary.

Normal product paths must use CRM Public Web (`crm_public_web_runtime.py`).
This module exists only so explicit historical migration/test code can observe
the retired target-candidate helper envelopes without reintroducing them as
ordinary imports throughout the service. It must not provide an execution
override after W7e.
"""

from __future__ import annotations

from .public_web_runtime_core import (
    LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS_ENV,
    LEGACY_TARGET_PUBLIC_WEB_MIGRATION_PHASE,
    PUBLIC_WEB_JOB_TYPE,
    PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES,
    PUBLIC_WEB_WORKER_LANE,
    PUBLIC_WEB_WORKER_RECOVERY_KIND,
    TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND,
    TARGET_PUBLIC_WEB_OWNER,
    cancel_target_candidate_public_web_run,
    execute_target_candidate_public_web_run_once,
    execute_target_candidate_public_web_run_to_local_idle,
    legacy_target_public_web_execution_enabled,
    public_web_worker_key,
    start_target_candidate_public_web_batch,
    sync_public_web_batch_summary,
)

__all__ = [
    "LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS_ENV",
    "LEGACY_TARGET_PUBLIC_WEB_MIGRATION_PHASE",
    "PUBLIC_WEB_JOB_TYPE",
    "PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES",
    "PUBLIC_WEB_WORKER_LANE",
    "PUBLIC_WEB_WORKER_RECOVERY_KIND",
    "TARGET_CANDIDATE_PUBLIC_WEB_EXECUTION_BACKEND",
    "TARGET_PUBLIC_WEB_OWNER",
    "cancel_target_candidate_public_web_run",
    "execute_target_candidate_public_web_run_once",
    "execute_target_candidate_public_web_run_to_local_idle",
    "legacy_target_public_web_execution_enabled",
    "public_web_worker_key",
    "start_target_candidate_public_web_batch",
    "sync_public_web_batch_summary",
]
