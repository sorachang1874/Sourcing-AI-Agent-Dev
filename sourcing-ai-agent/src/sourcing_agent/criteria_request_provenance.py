"""Canonical request provenance for criteria-domain writes."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .cohort_selection import (
    CohortSelectionValidationError,
    prepare_external_criteria_request_payload,
)
from .domain import JobRequest
from .request_matching import request_signature_context
from .request_ownership import exact_job_owner_matches

_CRITERIA_JOB_REFERENCE_FIELDS = ("job_id", "baseline_job_id", "source_job_id")


def prepare_criteria_write_payload(
    payload: dict[str, Any],
    *,
    job_lookup: Callable[[str], dict[str, Any] | None],
    expected_requester_id: str = "",
    expected_tenant_id: str = "",
    bind_referenced_job: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Prepare one criteria mutation from canonical server provenance.

    External request aliases are validated before any domain write. When a job
    is referenced, every explicit id is exact-owner checked and the selected
    source job is re-read while its stored request is bound. A missing request
    target may be filled only from the same job's stored plan. Caller-authored
    request-family signatures may be exact replays, but freshly derived server
    values always replace them.
    """

    try:
        prepared = prepare_external_criteria_request_payload(payload)
    except CohortSelectionValidationError as exc:
        return {}, exc.to_result()

    if bind_referenced_job:
        for job_id in _distinct_referenced_job_ids(prepared):
            if not exact_job_owner_matches(
                job_lookup(job_id),
                expected_requester_id=expected_requester_id,
                expected_tenant_id=expected_tenant_id,
            ):
                return {}, {"status": "not_found", "reason": "job_not_found"}

    caller_request_present = "request_payload" in prepared
    caller_request = dict(prepared.get("request_payload") or {})
    try:
        caller_request_record = JobRequest.from_payload(caller_request).to_record() if caller_request else {}
    except CohortSelectionValidationError as exc:
        return {}, exc.to_result()

    source_job: dict[str, Any] | None = None
    if bind_referenced_job:
        source_job_id = next(iter(_distinct_referenced_job_ids(prepared)), "")
        if source_job_id:
            source_job = job_lookup(source_job_id)
            if not exact_job_owner_matches(
                source_job,
                expected_requester_id=expected_requester_id,
                expected_tenant_id=expected_tenant_id,
            ):
                return {}, {"status": "not_found", "reason": "job_not_found"}

    trusted_request: dict[str, Any] = caller_request_record
    trusted_target_company = str(trusted_request.get("target_company") or "").strip()
    if source_job is not None:
        stored_context = _stored_job_request_context(source_job)
        if stored_context is None:
            return {}, {
                "status": "invalid",
                "reason": "criteria_stored_job_provenance_invalid",
            }
        trusted_request, trusted_target_company = stored_context
        if caller_request_present and caller_request_record != trusted_request:
            return {}, {
                "status": "invalid",
                "reason": "criteria_job_request_conflict",
                "field": "request_payload",
            }

    caller_target_company = str(prepared.get("target_company") or "").strip()
    if (
        caller_target_company
        and trusted_target_company
        and caller_target_company.casefold() != trusted_target_company.casefold()
    ):
        return {}, {
            "status": "invalid",
            "reason": "criteria_job_request_conflict" if source_job is not None else "criteria_request_alias_conflict",
            "field": "target_company",
        }
    if not trusted_target_company:
        trusted_target_company = caller_target_company
    if trusted_request and trusted_target_company and not str(trusted_request.get("target_company") or "").strip():
        try:
            trusted_request = JobRequest.from_payload(
                {**trusted_request, "target_company": trusted_target_company}
            ).to_record()
        except CohortSelectionValidationError as exc:
            return {}, exc.to_result()

    signature_context = request_signature_context(trusted_request) if trusted_request else {}
    expected_metadata: dict[str, Any] = {
        "request_payload": trusted_request,
        "request_matching": dict(signature_context.get("request_matching") or {}),
        "request_signature": str(signature_context.get("request_signature") or ""),
        "request_family_signature": str(signature_context.get("request_family_signature") or ""),
        "matching_request_signature": str(signature_context.get("matching_request_signature") or ""),
        "matching_request_family_signature": str(signature_context.get("matching_request_family_signature") or ""),
        "target_company": trusted_target_company,
    }
    metadata = dict(prepared.get("metadata") or {})
    provenance_conflict = _caller_provenance_conflict(
        prepared=prepared,
        metadata=metadata,
        expected_metadata=expected_metadata,
    )
    if provenance_conflict is not None:
        return {}, provenance_conflict

    for key in expected_metadata:
        prepared.pop(key, None)
    if trusted_request:
        prepared["request_payload"] = trusted_request
    else:
        prepared.pop("request_payload", None)
    if trusted_target_company:
        prepared["target_company"] = trusted_target_company

    for key, expected_value in expected_metadata.items():
        if expected_value not in (None, "", {}, []):
            metadata[key] = expected_value
        else:
            metadata.pop(key, None)
    if metadata or "metadata" in prepared:
        prepared["metadata"] = metadata
    return prepared, {"status": "ready"}


def _distinct_referenced_job_ids(payload: dict[str, Any]) -> tuple[str, ...]:
    job_ids: list[str] = []
    for field in _CRITERIA_JOB_REFERENCE_FIELDS:
        raw_job_id = payload.get(field)
        job_id = str(raw_job_id or "").strip()
        if job_id and job_id not in job_ids:
            job_ids.append(job_id)
    return tuple(job_ids)


def _stored_job_request_context(job: dict[str, Any]) -> tuple[dict[str, Any], str] | None:
    raw_stored_request = job.get("request") or {}
    raw_stored_plan = job.get("plan") or {}
    if not isinstance(raw_stored_request, dict) or not isinstance(raw_stored_plan, dict):
        return None
    try:
        trusted_request = JobRequest.from_payload(dict(raw_stored_request)).to_record() if raw_stored_request else {}
    except CohortSelectionValidationError:
        return None

    request_target_company = str(trusted_request.get("target_company") or "").strip()
    plan_target_company = str(raw_stored_plan.get("target_company") or "").strip()
    if (
        request_target_company
        and plan_target_company
        and request_target_company.casefold() != plan_target_company.casefold()
    ):
        return None
    trusted_target_company = request_target_company or plan_target_company
    if trusted_target_company and not request_target_company:
        try:
            trusted_request = JobRequest.from_payload(
                {**trusted_request, "target_company": trusted_target_company}
            ).to_record()
        except CohortSelectionValidationError:
            return None
    return trusted_request, trusted_target_company


def _caller_provenance_conflict(
    *,
    prepared: dict[str, Any],
    metadata: dict[str, Any],
    expected_metadata: dict[str, Any],
) -> dict[str, Any] | None:
    for key, expected_value in expected_metadata.items():
        if key == "request_payload":
            # The external request alias owner already validated and removed
            # this metadata copy before stored-job binding.
            continue
        for container, field_path in (
            (metadata, f"metadata.{key}"),
            (prepared, key),
        ):
            if key not in container:
                continue
            supplied_value = container.get(key)
            if supplied_value in (None, "", {}, []):
                continue
            if key == "request_matching":
                matches = isinstance(supplied_value, dict) and dict(supplied_value) == expected_value
            else:
                matches = str(supplied_value).strip() == str(expected_value).strip()
            if not matches:
                return {
                    "status": "invalid",
                    "reason": "criteria_request_provenance_conflict",
                    "field": field_path,
                }
    return None
