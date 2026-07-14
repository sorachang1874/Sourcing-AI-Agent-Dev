"""Pure authenticated request-owner predicates shared by canonical owners."""

from __future__ import annotations

from typing import Any


def exact_job_owner_matches(
    job: dict[str, Any] | None,
    *,
    expected_requester_id: str = "",
    expected_tenant_id: str = "",
) -> bool:
    """Return whether a job matches an authenticated exact-owner fence.

    Blank expectations are the explicit pre-auth/open compatibility path.
    Authenticated callers provide both values; legacy/blank stored owners never
    match that path.
    """
    requester = str(expected_requester_id or "").strip()
    tenant = str(expected_tenant_id or "").strip()
    if not requester and not tenant:
        return job is not None
    if not requester or not tenant or job is None:
        return False
    return str(job.get("requester_id") or "").strip() == requester and str(
        job.get("tenant_id") or ""
    ).strip() == tenant


def exact_crm_owner_matches(
    record: dict[str, Any] | None,
    *,
    expected_workspace_id: str = "",
    expected_owner_user_id: str = "",
) -> bool:
    """Return whether a CRM record matches an authenticated exact-owner fence."""
    workspace = str(expected_workspace_id or "").strip()
    owner = str(expected_owner_user_id or "").strip()
    if not workspace and not owner:
        return record is not None
    if not workspace or not owner or record is None:
        return False
    if str(record.get("workspace_id") or "").strip() != workspace:
        return False
    stored_owner = str(record.get("owner_user_id") or "").strip()
    return not stored_owner or stored_owner == owner
