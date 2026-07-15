"""Focused contract for canonical criteria request provenance."""

from __future__ import annotations

import pytest

from sourcing_agent.criteria_request_provenance import prepare_criteria_write_payload
from sourcing_agent.domain import JobRequest


def _cohort_request(role: str, *, source: str = "user_explicit") -> dict[str, object]:
    return JobRequest.from_payload(
        {
            "target_company": "OpenAI",
            "cohort_selection": {
                "schema_version": "cohort_selection.v1",
                "role_bucket_ids": [role],
                "employment_statuses": ["current"],
                "role_match": "any",
                "source": source,
            },
        }
    ).to_record()


def _job(request: dict[str, object], *, requester: str = "alice", tenant: str = "user-alice") -> dict[str, object]:
    return {
        "job_id": "job-1",
        "requester_id": requester,
        "tenant_id": tenant,
        "request": request,
        "plan": {"target_company": "OpenAI"},
    }


@pytest.mark.parametrize(
    ("caller_patch", "expected_reason"),
    (
        ({"request_payload": _cohort_request("engineering")}, "criteria_job_request_conflict"),
        (
            {"metadata": {"matching_request_family_signature": "caller-forged"}},
            "criteria_request_provenance_conflict",
        ),
    ),
)
def test_owned_job_rejects_conflicting_caller_provenance(
    caller_patch: dict[str, object],
    expected_reason: str,
) -> None:
    stored_request = _cohort_request("research")

    prepared, result = prepare_criteria_write_payload(
        {"job_id": "job-1", **caller_patch},
        job_lookup=lambda _job_id: _job(stored_request),
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert prepared == {}
    assert result["status"] == "invalid"
    assert result["reason"] == expected_reason


@pytest.mark.parametrize("job_reference_field", ["job_id", "baseline_job_id", "source_job_id"])
def test_owned_job_emits_only_stored_request_and_derived_signatures(job_reference_field: str) -> None:
    stored_request = _cohort_request("research")

    prepared, result = prepare_criteria_write_payload(
        {job_reference_field: "job-1", "metadata": {"note": "keep"}},
        job_lookup=lambda _job_id: _job(stored_request),
        expected_requester_id="alice",
        expected_tenant_id="user-alice",
    )

    assert result == {"status": "ready"}
    assert prepared["request_payload"]["cohort_selection"]["role_bucket_ids"] == ["research"]
    assert prepared["metadata"]["request_payload"] == prepared["request_payload"]
    assert prepared["metadata"]["request_signature"]
    assert prepared["metadata"]["matching_request_family_signature"]
    assert prepared["metadata"]["note"] == "keep"


def test_no_ref_and_open_mode_keep_legacy_positive_paths() -> None:
    no_ref, no_ref_result = prepare_criteria_write_payload(
        {"target_company": "OpenAI"},
        job_lookup=lambda _job_id: (_ for _ in ()).throw(AssertionError("unexpected job lookup")),
    )
    assert no_ref_result == {"status": "ready"}
    assert no_ref["target_company"] == "OpenAI"
    assert "request_payload" not in no_ref

    stored_inferred = _cohort_request("research", source="inferred")
    open_mode, open_result = prepare_criteria_write_payload(
        {"job_id": "job-1"},
        job_lookup=lambda _job_id: _job(stored_inferred, requester="", tenant=""),
    )
    assert open_result == {"status": "ready"}
    assert open_mode["request_payload"]["cohort_selection"]["source"] == "inferred"
