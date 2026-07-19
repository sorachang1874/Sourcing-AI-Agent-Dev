"""FF-SCHEMA tests for the Cohort execution contract family module.

Every expectation is recomputed from the canonical decision manifest
(``filter_projection_lineage_fixed_forward_decision_v1.json``); the manifest is
the only design source.  Collision checks scan tracked Git content at the
pinned packet base commit, so they are stable both before and after this lane
lands and after later waves adopt the module.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent.agent_runtime_namespace_ref import (
    AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST,
    mint_agent_runtime_namespace_ref,
)
from sourcing_agent.cohort_execution_contract import (
    COHORT_CANDIDATE_MEMBER_V1_CONTRACT_DIGEST,
    COHORT_CANDIDATE_MEMBER_V1_ORDERED_FIELDS,
    COHORT_CANDIDATE_MEMBER_V1_OWNER,
    COHORT_CANDIDATE_MEMBER_V1_SCHEMA,
    COHORT_CANDIDATE_MEMBER_V1_SCHEMA_VERSION,
    COHORT_CANDIDATE_SET_V1_CONTRACT_DIGEST,
    COHORT_CANDIDATE_SET_V1_ORDERED_FIELDS,
    COHORT_CANDIDATE_SET_V1_OWNER,
    COHORT_CANDIDATE_SET_V1_SCHEMA,
    COHORT_CANDIDATE_SET_V1_SCHEMA_VERSION,
    COHORT_EXECUTION_CAPABILITY_V2_CONTRACT_DIGEST,
    COHORT_EXECUTION_CAPABILITY_V2_ORDERED_FIELDS,
    COHORT_EXECUTION_CAPABILITY_V2_OWNER,
    COHORT_EXECUTION_CAPABILITY_V2_SCHEMA,
    COHORT_EXECUTION_CAPABILITY_V2_SCHEMA_VERSION,
    COHORT_EXECUTION_COMMIT_V1_CONTRACT_DIGEST,
    COHORT_EXECUTION_COMMIT_V1_ORDERED_FIELDS,
    COHORT_EXECUTION_COMMIT_V1_OWNER,
    COHORT_EXECUTION_COMMIT_V1_SCHEMA,
    COHORT_EXECUTION_COMMIT_V1_SCHEMA_VERSION,
    COHORT_EXECUTION_ENVELOPE_V1_CONTRACT_DIGEST,
    COHORT_EXECUTION_ENVELOPE_V1_ORDERED_FIELDS,
    COHORT_EXECUTION_ENVELOPE_V1_OWNER,
    COHORT_EXECUTION_ENVELOPE_V1_SCHEMA,
    COHORT_EXECUTION_ENVELOPE_V1_SCHEMA_VERSION,
    COHORT_EXECUTION_LANE_RESULT_V2_CONTRACT_DIGEST,
    COHORT_EXECUTION_LANE_RESULT_V2_ORDERED_FIELDS,
    COHORT_EXECUTION_LANE_RESULT_V2_OWNER,
    COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA,
    COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA_VERSION,
    COHORT_EXECUTION_RESULT_V2_CONTRACT_DIGEST,
    COHORT_EXECUTION_RESULT_V2_ORDERED_FIELDS,
    COHORT_EXECUTION_RESULT_V2_OWNER,
    COHORT_EXECUTION_RESULT_V2_SCHEMA,
    COHORT_EXECUTION_RESULT_V2_SCHEMA_VERSION,
    CONTRACT_SCHEMAS,
    REJECTED_COHORT_LITERALS,
    RETAINED_COHORT_HISTORY_LITERALS,
    RETAINED_LOOKUP_FORBIDDEN,
    CohortExecutionContractError,
    compute_cohort_candidate_member_digest,
    compute_cohort_candidate_set_digest,
    compute_cohort_execution_capability_digest,
    compute_cohort_execution_commit_digest,
    compute_cohort_execution_envelope_digest,
    compute_cohort_execution_lane_result_digest,
    compute_cohort_execution_result_digest,
    parse_cohort_candidate_member,
    parse_cohort_candidate_set,
    parse_cohort_execution_capability,
    parse_cohort_execution_commit,
    parse_cohort_execution_envelope,
    parse_cohort_execution_lane_result,
    parse_cohort_execution_result,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "contracts"
    / "filter_projection_lineage_fixed_forward_decision_v1.json"
)
# Pinned packet base commit (session packet s1f0c-ff-schema-v1).
BASE_COMMIT = "a546c4d4181ee8ffe952935c3d50c67daf143927"
CANDIDATE_BLOBS = {
    "docs/modules/serving-product/contracts/filter_projection_lineage_fixed_forward_decision_v1.json",
    "docs/modules/serving-product/decisions/TRACK_D_D1N_S1F0C_LINEAGE_FIXED_FORWARD_DECISION.md",
    "tests/test_d1n_s1f0c_lineage_fixed_forward_decision.py",
}

ADOPTED = {
    "cohort_execution_capability.v2": (
        COHORT_EXECUTION_CAPABILITY_V2_SCHEMA_VERSION,
        COHORT_EXECUTION_CAPABILITY_V2_OWNER,
        COHORT_EXECUTION_CAPABILITY_V2_ORDERED_FIELDS,
        COHORT_EXECUTION_CAPABILITY_V2_SCHEMA,
        COHORT_EXECUTION_CAPABILITY_V2_CONTRACT_DIGEST,
    ),
    "cohort_execution_envelope.v1": (
        COHORT_EXECUTION_ENVELOPE_V1_SCHEMA_VERSION,
        COHORT_EXECUTION_ENVELOPE_V1_OWNER,
        COHORT_EXECUTION_ENVELOPE_V1_ORDERED_FIELDS,
        COHORT_EXECUTION_ENVELOPE_V1_SCHEMA,
        COHORT_EXECUTION_ENVELOPE_V1_CONTRACT_DIGEST,
    ),
    "cohort_execution_lane_result.v2": (
        COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA_VERSION,
        COHORT_EXECUTION_LANE_RESULT_V2_OWNER,
        COHORT_EXECUTION_LANE_RESULT_V2_ORDERED_FIELDS,
        COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA,
        COHORT_EXECUTION_LANE_RESULT_V2_CONTRACT_DIGEST,
    ),
    "cohort_candidate_member.v1": (
        COHORT_CANDIDATE_MEMBER_V1_SCHEMA_VERSION,
        COHORT_CANDIDATE_MEMBER_V1_OWNER,
        COHORT_CANDIDATE_MEMBER_V1_ORDERED_FIELDS,
        COHORT_CANDIDATE_MEMBER_V1_SCHEMA,
        COHORT_CANDIDATE_MEMBER_V1_CONTRACT_DIGEST,
    ),
    "cohort_candidate_set.v1": (
        COHORT_CANDIDATE_SET_V1_SCHEMA_VERSION,
        COHORT_CANDIDATE_SET_V1_OWNER,
        COHORT_CANDIDATE_SET_V1_ORDERED_FIELDS,
        COHORT_CANDIDATE_SET_V1_SCHEMA,
        COHORT_CANDIDATE_SET_V1_CONTRACT_DIGEST,
    ),
    "cohort_execution_result.v2": (
        COHORT_EXECUTION_RESULT_V2_SCHEMA_VERSION,
        COHORT_EXECUTION_RESULT_V2_OWNER,
        COHORT_EXECUTION_RESULT_V2_ORDERED_FIELDS,
        COHORT_EXECUTION_RESULT_V2_SCHEMA,
        COHORT_EXECUTION_RESULT_V2_CONTRACT_DIGEST,
    ),
    "cohort_execution_commit.v1": (
        COHORT_EXECUTION_COMMIT_V1_SCHEMA_VERSION,
        COHORT_EXECUTION_COMMIT_V1_OWNER,
        COHORT_EXECUTION_COMMIT_V1_ORDERED_FIELDS,
        COHORT_EXECUTION_COMMIT_V1_SCHEMA,
        COHORT_EXECUTION_COMMIT_V1_CONTRACT_DIGEST,
    ),
}


def _manifest() -> dict[str, Any]:
    value = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert type(value) is dict
    return value


def _manifest_rows() -> dict[str, Any]:
    return {row["literal"]: row for row in _manifest()["contract_digests"]}


def _sha256_canonical(value: Any) -> str:
    blob = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _git_grep_paths_at_base(needle: str) -> list[str]:
    excludes = [f":(exclude,literal){path}" for path in sorted(CANDIDATE_BLOBS)]
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "grep", "-F", "-l", "-e", needle, BASE_COMMIT, "--", ".", *excludes],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 1:
        return []
    assert result.returncode == 0, result.stderr
    return sorted(line.split(":", 1)[1] for line in result.stdout.splitlines() if line)


def _digest(seed: str) -> str:
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _namespace_ref() -> dict[str, Any]:
    return mint_agent_runtime_namespace_ref(
        namespace_ref_id="ns-1",
        workspace_id="ws-1",
        provider_mode="simulate",
        policy_revision="policy-rev-1",
        generation=1,
    )


def _capability() -> dict[str, Any]:
    record = {
        "schema_version": "cohort_execution_capability.v2",
        "owner": "cohort_runtime",
        "workspace_id": "ws-1",
        "acquisition_run_id": "run-1",
        "planning_manifest_digest": _digest("planning-manifest"),
        "cohort_selection_digest": _digest("cohort-selection"),
        "provider_mode": "simulate",
        "runtime_namespace_ref": _namespace_ref(),
        "policy_revision": "policy-rev-1",
        "max_provider_calls": 2,
        "max_provider_items": 250,
        "max_output_candidates": 250,
        "role_proof_verifier_id": "verifier-1",
        "role_proof_verifier_revision": "verifier-rev-1",
        "cost_policy_revision": "cost-rev-1",
        "retry_policy_revision": "retry-rev-1",
        "circuit_policy_revision": "circuit-rev-1",
        "execution_generation": 1,
    }
    record["capability_digest"] = compute_cohort_execution_capability_digest(record)
    return record


def _lane_result(ordinal: int, *, lane_id: str | None = None) -> dict[str, Any]:
    lane = lane_id or f"lane-{ordinal}"
    record = {
        "schema_version": "cohort_execution_lane_result.v2",
        "ordinal": ordinal,
        "lane_id": lane,
        "planned_lane_digest": _digest(f"planned-{lane}"),
        "provider_exposure_id": f"exposure-{ordinal}",
        "provider_call_id": f"call-{ordinal}",
        "provider_response_digest": _digest(f"response-{lane}"),
        "evidence_ref": f"evidence-{ordinal}",
        "evidence_digest": _digest(f"evidence-{lane}"),
        "raw_occurrence_count": 3,
        "accepted_occurrence_count": 2,
        "rejected_occurrence_count": 1,
        "truncated_occurrence_count": 0,
        "ordered_accepted_occurrence_digests": [_digest(f"occurrence-{lane}-0"), _digest(f"occurrence-{lane}-1")],
    }
    record["lane_result_digest"] = compute_cohort_execution_lane_result_digest(record)
    return record


def _envelope() -> dict[str, Any]:
    capability = _capability()
    record = {
        "schema_version": "cohort_execution_envelope.v1",
        "owner": "cohort_runtime",
        "workspace_id": "ws-1",
        "acquisition_run_id": "run-1",
        "planning_manifest_digest": capability["planning_manifest_digest"],
        "capability": capability,
        "capability_digest": capability["capability_digest"],
        "provider_mode": "simulate",
        "runtime_namespace_ref": capability["runtime_namespace_ref"],
        "ordered_planned_lane_refs": [
            {"ordinal": 0, "lane_id": "lane-0", "lane_digest": _digest("planned-lane-0")},
            {"ordinal": 1, "lane_id": "lane-1", "lane_digest": _digest("planned-lane-1")},
        ],
        "execution_generation": 1,
    }
    record["envelope_digest"] = compute_cohort_execution_envelope_digest(record)
    return record


def _member(*, with_url: bool = True) -> dict[str, Any]:
    record = {
        "schema_version": "cohort_candidate_member.v1",
        "candidate_identity_key": "candidate-1",
        "display_name": "Candidate One",
        "headline": "Staff Engineer",
        "ordered_lane_memberships": [
            {
                "lane_id": "lane-0",
                "planned_lane_digest": _digest("planned-lane-0"),
                "lane_result_digest": _digest("result-lane-0"),
                "employment_status": "current",
                "role_bucket_id": "engineering",
            }
        ],
    }
    if with_url:
        record["public_profile_url"] = "https://example.com/profile/candidate-1"
    record["member_digest"] = compute_cohort_candidate_member_digest(record)
    return record


def _candidate_set() -> dict[str, Any]:
    members = [_member(with_url=False), _member()]
    digests = [member["member_digest"] for member in members]
    record = {
        "schema_version": "cohort_candidate_set.v1",
        "workspace_id": "ws-1",
        "acquisition_run_id": "run-1",
        "execution_generation": 1,
        "planning_manifest_digest": _digest("planning-manifest"),
        "member_count": len(digests),
        "ordered_member_digests": digests,
    }
    record["candidate_set_digest"] = compute_cohort_candidate_set_digest(record)
    return record


def _execution_result() -> dict[str, Any]:
    capability = _capability()
    envelope = _envelope()
    lane_results = [_lane_result(0), _lane_result(1)]
    candidate_set = _candidate_set()
    record = {
        "schema_version": "cohort_execution_result.v2",
        "owner": "cohort_runtime",
        "workspace_id": "ws-1",
        "acquisition_run_id": "run-1",
        "planning_manifest_digest": capability["planning_manifest_digest"],
        "capability_digest": capability["capability_digest"],
        "execution_envelope_digest": envelope["envelope_digest"],
        "execution_attempt_id": "attempt-1",
        "execution_generation": 1,
        "ordered_lane_results": lane_results,
        "lane_coverage_status": "complete",
        "planned_lane_count": 2,
        "executed_lane_count": 2,
        "raw_occurrence_count": 6,
        "accepted_occurrence_count": 4,
        "unique_candidate_count": 2,
        "truncated_count": 0,
        "rejected_unverified_count": 2,
        "missing_required_lane_count": 0,
        "candidate_set_digest": candidate_set["candidate_set_digest"],
    }
    record["result_digest"] = compute_cohort_execution_result_digest(record)
    return record


def _commit() -> dict[str, Any]:
    result = _execution_result()
    envelope = _envelope()
    record = {
        "schema_version": "cohort_execution_commit.v1",
        "execution_commit_id": "commit-1",
        "workspace_id": "ws-1",
        "acquisition_run_id": "run-1",
        "operation_run_id": "op-1",
        "workflow_run_id": "wf-1",
        "execution_attempt_id": "attempt-1",
        "execution_generation": 1,
        "start_authority_carrier_digest": _digest("carrier"),
        "execution_authority_digest": _digest("authority"),
        "planning_manifest_digest": result["planning_manifest_digest"],
        "capability_digest": result["capability_digest"],
        "execution_envelope_digest": envelope["envelope_digest"],
        "execution_result_json": result,
        "execution_result_digest": result["result_digest"],
        "candidate_set_digest": result["candidate_set_digest"],
        "candidate_count": 2,
        "commit_contract_digest": COHORT_EXECUTION_COMMIT_V1_CONTRACT_DIGEST,
    }
    record["commit_digest"] = compute_cohort_execution_commit_digest(record)
    return record


def _expect_invalid(parser: Any, record: Any) -> None:
    with pytest.raises(CohortExecutionContractError):
        parser(record)


def test_constants_match_manifest_exactly() -> None:
    rows = _manifest_rows()
    assert set(ADOPTED) <= set(rows)
    for literal, (version, owner, _ordered, _schema, _digest_constant) in ADOPTED.items():
        assert version == literal
        assert rows[literal]["literal"] == literal
        assert owner == rows[literal]["contract_owner"], literal
    assert COHORT_EXECUTION_COMMIT_V1_OWNER == "cohort_provider_runtime"
    # Wherever the manifest pins a constant owner field inside the record, the
    # module owner constant equals it exactly; capability/envelope/result carry
    # it, while lane result, member, candidate set, and commit do not.
    owner_field_literals = {
        literal
        for literal in ADOPTED
        if any(field["name"] == "owner" for field in rows[literal]["schema"]["fields"])
    }
    assert owner_field_literals == {
        "cohort_execution_capability.v2",
        "cohort_execution_envelope.v1",
        "cohort_execution_result.v2",
    }
    for literal in owner_field_literals:
        owner_descriptor = next(
            field for field in rows[literal]["schema"]["fields"] if field["name"] == "owner"
        )
        assert owner_descriptor["constant"] == ADOPTED[literal][1], literal


def test_schema_objects_and_digests_match_manifest() -> None:
    rows = _manifest_rows()
    for literal, (_version, _owner, _ordered, schema, digest_constant) in ADOPTED.items():
        row = rows[literal]
        assert json.loads(json.dumps(schema)) == row["schema"], literal
        assert _sha256_canonical(row["schema"]) == row["contract_digest"], literal
        assert digest_constant == row["contract_digest"], literal


def test_manifest_module_field_order_consistency() -> None:
    rows = _manifest_rows()
    for literal, (_version, _owner, ordered, schema, _digest_constant) in ADOPTED.items():
        row = rows[literal]
        assert list(ordered) == row["ordered_fields"], literal
        assert [field["name"] for field in schema["fields"]] == [
            field["name"] for field in row["schema"]["fields"]
        ], literal
        assert [field["name"] for field in schema["fields"]] == row["ordered_fields"], literal
    assert set(CONTRACT_SCHEMAS) == set(ADOPTED)
    for literal, schema in CONTRACT_SCHEMAS.items():
        assert schema is ADOPTED[literal][3]


def test_namespace_ref_pin_is_exact_version_and_digest() -> None:
    rows = _manifest_rows()
    namespace_row = rows["agent_runtime_namespace_ref.v1"]
    assert AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST == namespace_row["contract_digest"]
    for schema in (COHORT_EXECUTION_CAPABILITY_V2_SCHEMA, COHORT_EXECUTION_ENVELOPE_V1_SCHEMA):
        descriptor = next(field for field in schema["fields"] if field["name"] == "runtime_namespace_ref")
        assert descriptor["ref"] == "agent_runtime_namespace_ref.v1"
        assert descriptor["ref_digest"] == AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST


def test_parse_round_trips() -> None:
    fixtures = [
        (parse_cohort_execution_capability, _capability()),
        (parse_cohort_execution_envelope, _envelope()),
        (parse_cohort_execution_lane_result, _lane_result(0)),
        (parse_cohort_candidate_member, _member()),
        (parse_cohort_candidate_member, _member(with_url=False)),
        (parse_cohort_candidate_set, _candidate_set()),
        (parse_cohort_execution_result, _execution_result()),
        (parse_cohort_execution_commit, _commit()),
    ]
    for parser, record in fixtures:
        parsed = parser(record)
        assert parsed == record
        assert parser(deepcopy(parsed)) == record
        assert json.dumps(parsed, sort_keys=True) == json.dumps(record, sort_keys=True)


def test_digest_helpers_match_independent_recomputation() -> None:
    cases = [
        (compute_cohort_execution_capability_digest, _capability(), "capability_digest"),
        (compute_cohort_execution_envelope_digest, _envelope(), "envelope_digest"),
        (compute_cohort_execution_lane_result_digest, _lane_result(0), "lane_result_digest"),
        (compute_cohort_candidate_member_digest, _member(), "member_digest"),
        (compute_cohort_candidate_set_digest, _candidate_set(), "candidate_set_digest"),
        (compute_cohort_execution_result_digest, _execution_result(), "result_digest"),
        (compute_cohort_execution_commit_digest, _commit(), "commit_digest"),
    ]
    for helper, record, digest_field in cases:
        covered = {key: value for key, value in record.items() if key != digest_field}
        assert helper(record) == _sha256_canonical(covered) == record[digest_field]
        tampered = {**record, "workspace_id": "ws-2"}
        assert helper(tampered) != record[digest_field]


def test_capability_fail_closed() -> None:
    capability = _capability()
    hostile = [
        {**capability, "unexpected": "field"},
        {key: value for key, value in capability.items() if key != "workspace_id"},
        {**capability, "schema_version": "cohort_execution_capability.v1"},  # retained v1 is never re-minted
        {**capability, "owner": "acquisition_planner"},
        {**capability, "provider_mode": "live"},
        {**capability, "max_provider_calls": 0},
        {**capability, "max_provider_items": True},
        {**capability, "max_output_candidates": 1001},
        {**capability, "max_output_candidates": 1000.0},
        {**capability, "execution_generation": 0},
        {**capability, "planning_manifest_digest": "not-a-digest"},
        {**capability, "capability_digest": "0" * 64},
        {**capability, "policy_revision": "policy-rev-2"},  # copied digest is never proof
        {**capability, "runtime_namespace_ref": {**capability["runtime_namespace_ref"], "ref_digest": "0" * 64}},
        {**capability, "runtime_namespace_ref": {"namespace_ref_id": "ns-1"}},
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_execution_capability, record)
    boundary = {**capability, "max_output_candidates": 1000}
    boundary["capability_digest"] = compute_cohort_execution_capability_digest(boundary)
    assert parse_cohort_execution_capability(boundary) == boundary


def test_envelope_fail_closed() -> None:
    envelope = _envelope()
    lane_ref = envelope["ordered_planned_lane_refs"][0]
    hostile = [
        {**envelope, "unexpected": "field"},
        {key: value for key, value in envelope.items() if key != "capability"},
        {**envelope, "schema_version": "cohort_provider_execution_manifest.v2"},  # rejected literal never adopted
        {**envelope, "provider_mode": "live"},
        {**envelope, "capability_digest": "0" * 64},  # must equal the nested capability record digest
        {**envelope, "envelope_digest": "0" * 64},
        {**envelope, "execution_generation": 1.5},
        {**envelope, "ordered_planned_lane_refs": [{**lane_ref, "extra": "key"}, *envelope["ordered_planned_lane_refs"][1:]]},
        {**envelope, "ordered_planned_lane_refs": [{**lane_ref, "ordinal": -1}, *envelope["ordered_planned_lane_refs"][1:]]},
        {**envelope, "ordered_planned_lane_refs": [{**lane_ref, "lane_digest": "bad"}, *envelope["ordered_planned_lane_refs"][1:]]},
        {**envelope, "ordered_planned_lane_refs": "lane-0"},
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_execution_envelope, record)


def test_lane_result_fail_closed() -> None:
    lane = _lane_result(0)
    hostile = [
        {**lane, "unexpected": "field"},
        {key: value for key, value in lane.items() if key != "ordinal"},
        {**lane, "schema_version": "cohort_execution_lane_result.v1"},
        {**lane, "ordinal": -1},
        {**lane, "ordinal": True},
        {**lane, "lane_id": ""},
        {**lane, "planned_lane_digest": "bad"},
        {**lane, "evidence_ref": ""},
        {**lane, "raw_occurrence_count": -1},
        {**lane, "accepted_occurrence_count": 2.0},
        {**lane, "ordered_accepted_occurrence_digests": ["bad"]},
        {**lane, "ordered_accepted_occurrence_digests": "digest"},
        {**lane, "lane_result_digest": "0" * 64},
        {**lane, "rejected_occurrence_count": 2},  # copied digest is never proof
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_execution_lane_result, record)
    empty = {**lane, "ordered_accepted_occurrence_digests": [], "accepted_occurrence_count": 0}
    empty["lane_result_digest"] = compute_cohort_execution_lane_result_digest(empty)
    assert parse_cohort_execution_lane_result(empty) == empty


def test_candidate_member_fail_closed() -> None:
    member = _member()
    membership = member["ordered_lane_memberships"][0]
    hostile = [
        {**member, "unexpected": "field"},
        {key: value for key, value in member.items() if key != "candidate_identity_key"},
        {**member, "candidate_identity_key": ""},
        {**member, "public_profile_url": ""},  # empty-string alias forbidden
        {**member, "public_profile_url": "http://example.com/x"},
        {**member, "public_profile_url": "https://example.com/a b"},
        {**member, "ordered_lane_memberships": [{**membership, "extra": "key"}]},
        {**member, "ordered_lane_memberships": [{**membership, "employment_status": ""}]},
        {**member, "ordered_lane_memberships": [{**membership, "lane_result_digest": "bad"}]},
        {**member, "member_digest": "0" * 64},
        {**member, "headline": "Principal"},  # copied digest is never proof
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_candidate_member, record)
    # absent public_profile_url is the closed variant; it parses cleanly.
    without_url = _member(with_url=False)
    assert "public_profile_url" not in without_url
    assert parse_cohort_candidate_member(without_url) == without_url


def test_candidate_set_fail_closed() -> None:
    candidate_set = _candidate_set()
    hostile = [
        {**candidate_set, "unexpected": "field"},
        {**candidate_set, "execution_result_digest": "0" * 64},  # normative anti-cycle: never in the header
        {key: value for key, value in candidate_set.items() if key != "candidate_set_digest"},
        {**candidate_set, "member_count": 1001},
        {**candidate_set, "member_count": -1},
        {**candidate_set, "member_count": 1},  # must equal len(ordered_member_digests)
        {**candidate_set, "execution_generation": 0},
        {**candidate_set, "ordered_member_digests": ["bad", _digest("x")]},
        {**candidate_set, "candidate_set_digest": "0" * 64},
        {**candidate_set, "workspace_id": "ws-2"},  # copied digest is never proof
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_candidate_set, record)
    # empty headers are structurally valid; product publication bounds are a terminal concern.
    empty = {
        **candidate_set,
        "member_count": 0,
        "ordered_member_digests": [],
    }
    empty["candidate_set_digest"] = compute_cohort_candidate_set_digest(empty)
    assert parse_cohort_candidate_set(empty) == empty


def test_execution_result_fail_closed() -> None:
    result = _execution_result()
    swapped = deepcopy(result)
    swapped["ordered_lane_results"] = [result["ordered_lane_results"][1], result["ordered_lane_results"][0]]
    swapped["result_digest"] = compute_cohort_execution_result_digest(swapped)
    missing_lane = deepcopy(result)
    missing_lane["ordered_lane_results"] = [result["ordered_lane_results"][0]]
    missing_lane["executed_lane_count"] = 1
    missing_lane["result_digest"] = compute_cohort_execution_result_digest(missing_lane)
    hostile = [
        {**result, "unexpected": "field"},
        {key: value for key, value in result.items() if key != "lane_coverage_status"},
        {**result, "schema_version": "cohort_execution_result.v1"},  # retained v1 is never re-minted
        {**result, "lane_coverage_status": "partial"},  # exact constant
        {**result, "planned_lane_count": 3},  # planned == executed == len(ordered_lane_results)
        {**result, "unique_candidate_count": 1001},
        {**result, "missing_required_lane_count": -1},
        {**result, "result_digest": "0" * 64},
        {**result, "capability_digest": "bad"},
        swapped,  # ordinal mapping is the validated planning-v2 order, never completion order
        missing_lane,  # lane completeness is exact, never a missing-count substitution
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_execution_result, record)


def test_execution_commit_fail_closed() -> None:
    commit = _commit()
    hostile = [
        {**commit, "unexpected": "field"},
        {**commit, "owner": "cohort_provider_runtime"},  # the commit record carries no owner field
        {key: value for key, value in commit.items() if key != "commit_contract_digest"},
        {**commit, "commit_contract_digest": "0" * 64},  # constant self-binding to the contract digest
        {**commit, "execution_result_digest": "0" * 64},  # must equal the nested result digest
        {**commit, "commit_digest": "0" * 64},
        {**commit, "candidate_count": 1001},
        {**commit, "candidate_count": -1},
        {**commit, "execution_generation": 0},
        {**commit, "start_authority_carrier_digest": "bad"},
        {**commit, "execution_result_json": {**commit["execution_result_json"], "result_digest": "0" * 64}},
    ]
    for record in hostile:
        _expect_invalid(parse_cohort_execution_commit, record)


def test_retained_v1_history_never_retyped() -> None:
    manifest = _manifest()
    gates = manifest["retention_replay_migration_gates"]
    retained = set(gates["retained_literals"])
    assert set(RETAINED_COHORT_HISTORY_LITERALS) <= retained
    assert set(REJECTED_COHORT_LITERALS) == {"cohort_provider_execution_manifest.v2"}
    assert "cohort_provider_execution_manifest.v2" in gates["rejected_literals"]
    assert list(RETAINED_LOOKUP_FORBIDDEN) == gates["lookup_forbidden"]
    # The module adopts exactly the seven new literals and never retypes retained v1 history.
    assert not (set(RETAINED_COHORT_HISTORY_LITERALS) & set(CONTRACT_SCHEMAS))
    assert not (set(REJECTED_COHORT_LITERALS) & set(CONTRACT_SCHEMAS))
    # Retained literals stay byte-referenced at the base commit; adopted literals had zero collisions.
    for literal in RETAINED_COHORT_HISTORY_LITERALS:
        assert _git_grep_paths_at_base(literal) != [], literal
    for literal in ADOPTED:
        assert _git_grep_paths_at_base(literal) == [], literal


def test_base_commit_is_ancestor_of_head() -> None:
    merge_base = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "merge-base", "--is-ancestor", BASE_COMMIT, "HEAD"],
        check=False,
    )
    assert merge_base.returncode == 0, "pinned packet base must be an ancestor of HEAD"
