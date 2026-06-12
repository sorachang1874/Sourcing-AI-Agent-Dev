"""CRM-owned Public Web runtime boundary.

Normal product paths should import CRM Public Web constants and entrypoints
from this module. The lower-level implementation lives in
`public_web_runtime_core.py`; target-candidate Public Web is a retired
compatibility facade and must not be a normal execution owner.
"""

from __future__ import annotations

from .public_web_runtime_core import (
    CRM_PUBLIC_WEB_EXECUTION_BACKEND,
    CRM_PUBLIC_WEB_JOB_TYPE,
    CRM_PUBLIC_WEB_OWNER,
    CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND,
    PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES,
    PUBLIC_WEB_TERMINAL_STATUSES,
    PUBLIC_WEB_WORKER_LANE,
    build_crm_public_web_batch_idempotency_key,
    cancel_crm_public_web_run,
    execute_crm_public_web_run_once,
    execute_crm_public_web_run_to_local_idle,
    public_web_signal_id_for_identity,
    public_web_signal_identity_key,
    public_web_options_from_record,
    public_web_worker_key,
    start_crm_public_web_batch,
    sync_crm_public_web_batch_summary,
)

__all__ = [
    "CRM_PUBLIC_WEB_EXECUTION_BACKEND",
    "CRM_PUBLIC_WEB_JOB_TYPE",
    "CRM_PUBLIC_WEB_OWNER",
    "CRM_PUBLIC_WEB_WORKER_RECOVERY_KIND",
    "PUBLIC_WEB_RETRYABLE_TERMINAL_STATUSES",
    "PUBLIC_WEB_TERMINAL_STATUSES",
    "PUBLIC_WEB_WORKER_LANE",
    "build_crm_public_web_batch_idempotency_key",
    "cancel_crm_public_web_run",
    "execute_crm_public_web_run_once",
    "execute_crm_public_web_run_to_local_idle",
    "public_web_signal_id_for_identity",
    "public_web_signal_identity_key",
    "public_web_options_from_record",
    "public_web_worker_key",
    "start_crm_public_web_batch",
    "sync_crm_public_web_batch_summary",
]
